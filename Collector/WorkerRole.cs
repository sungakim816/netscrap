using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using System.Security.Cryptography;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Text;
using HtmlAgilityPack;
using System.Linq;
using MVCWebRole.Models;


enum STATES { START, STOP, IDLE };

namespace Collector
{
    public class WorkerRole : RoleEntryPoint
    {
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly ManualResetEvent runCompleteEvent = new ManualResetEvent(false);

        private CloudStorageAccount storageAccount;

        private CloudTableClient tableClient;
        private CloudTable websitePageTable;
        private CloudTable roleStatusTable;

        private CloudQueueClient queueClient;
        private CloudQueue urlQueue;
        private CloudQueue seedUrlQueue;
        private CloudQueue commandQueue;

        private PerformanceCounter cpuCounter;

        private string currentSeedUrl;
        private readonly HashAlgorithm algorithm = SHA256.Create();
        private Dictionary<string, string> urlList;
        private Robots robotTxtParser;
        private short instanceCount;
        private string instanceId;
        private HtmlDocument htmlDoc;
        private HtmlWeb webGet;
        private string currentStateDescription;
        private byte currentStateNumber;
        private RoleStatus workerStatus;
        private readonly string[] States = { "RUNNING", "STOPPED", "IDLE" };

        public override void Run()
        {
            Trace.TraceInformation("Url Collector Worker is running");
            try
            {
                this.RunAsync(this.cancellationTokenSource.Token).Wait();
            }
            finally
            {
                this.runCompleteEvent.Set();
            }
        }
        public override bool OnStart()
        {
            // Set the maximum number of concurrent connections
            ServicePointManager.DefaultConnectionLimit = 12;
            // For information on handling configuration changes
            // see the MSDN topic at https://go.microsoft.com/fwlink/?LinkId=166357.
            bool result = base.OnStart();
            Trace.TraceInformation("Url Collector Worker has been started");
            Initialize();
            return result;
        }

        private void Initialize()
        {
            // storageAccount = CloudStorageAccount.DevelopmentStorageAccount; // for local development
            storageAccount = CloudStorageAccount.Parse(RoleEnvironment.GetConfigurationSettingValue("StorageConnectionString"));
            // iniatilize instance count
            instanceCount = (short)RoleEnvironment.CurrentRoleInstance.Role.Instances.Count;
            // Initialize Runtime Queue List
            urlList = new Dictionary<string, string>();
            // Initialize Table (WebsitePage Table)
            tableClient = storageAccount.CreateCloudTableClient();
            websitePageTable = tableClient.GetTableReference("websitepage");
            roleStatusTable = tableClient.GetTableReference("rolestatus");
            // Initialize Queue (UrlToCrawl Queue)
            queueClient = storageAccount.CreateCloudQueueClient();
            urlQueue = queueClient.GetQueueReference("urlstocrawl");
            // Initialize Queue (SeedUrls Queue)
            seedUrlQueue = queueClient.GetQueueReference("seedurls");
            // Initialize Queue (Command queue)
            commandQueue = queueClient.GetQueueReference("command");
            currentSeedUrl = string.Empty;
            // Create robots.txt parser
            robotTxtParser = new Robots();
            // create if queues and tables does not exists
            websitePageTable.CreateIfNotExists();
            roleStatusTable.CreateIfNotExists();
            urlQueue.CreateIfNotExists();
            seedUrlQueue.CreateIfNotExists();
            commandQueue.CreateIfNotExists();
            Trace.TraceInformation("Url Collector Worker has been Initialize");
            webGet = new HtmlWeb();
            htmlDoc = new HtmlDocument();
            // instance id
            instanceId = RoleEnvironment.CurrentRoleInstance.Id;
            // workerStatus class
            workerStatus = new RoleStatus(this.GetType().Namespace, instanceId);
            // cpu counter
            cpuCounter = new PerformanceCounter("Processor", "% Processor Time", "_Total", true);
            // initial state stop
            currentStateNumber = (byte)STATES.STOP;
            currentStateDescription = States[currentStateNumber];
            // clear role status table
            ClearRoleStatusTableContent(this.GetType().Namespace);
        }

        private void ClearRoleStatusTableContent(string partitionKey)
        {
            TableQuery<RoleStatus> query = new TableQuery<RoleStatus>()
                .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey))
                .Select(new List<string> { "PartitionKey", "RowKey" });
            var entities = roleStatusTable.ExecuteQuery(query);
            TableBatchOperation deleteOldeRecords = new TableBatchOperation();
            foreach (var entity in entities)
            {
                deleteOldeRecords.Delete(entity);
            }
            if (deleteOldeRecords.Any())
            {
                roleStatusTable.ExecuteBatchAsync(deleteOldeRecords);
            }
        }

        private async Task ReadCommandQueue()
        {
            string currentCommand = string.Empty;
            try
            {
                CloudQueueMessage command = await commandQueue.PeekMessageAsync();
                currentCommand = command.AsString;
            }
            catch (Exception)
            {
                // command queue is empty
            }
            // start command is issued
            if (currentCommand.ToLower().Equals("start"))
            {
                currentStateNumber = (byte)STATES.START;
            }
            else
            {
                currentStateNumber = (byte)STATES.STOP;
            }
        }

        private async Task UpdateWorkerStatus()
        {
            workerStatus.CPU = Convert.ToInt64(cpuCounter.NextValue());
            long workingMemory = GC.GetTotalMemory(false);
            long totalMemory = Environment.WorkingSet;
            workerStatus.WorkingMem = workingMemory;
            workerStatus.TotalMem = totalMemory;
            workerStatus.State = currentStateDescription;
            workerStatus.CurrentWorkingUrl = currentSeedUrl;
            TableOperation insertOrReplace = TableOperation.InsertOrReplace(workerStatus);
            await roleStatusTable.ExecuteAsync(insertOrReplace);
        }

        private async Task UpdateInternalState()
        {
            int? seedUrlQueueCount = await CountQueueMessages(seedUrlQueue);
            if (currentStateNumber == (byte)STATES.START)
            {
                if (!seedUrlQueueCount.HasValue || seedUrlQueueCount.Value == 0)
                {
                    currentStateNumber = (byte)STATES.IDLE;
                }
            }
            else
            {
                currentStateNumber = (byte)STATES.STOP;

            }
            currentStateDescription = States[currentStateNumber];
        }

        private async Task<int?> CountQueueMessages(CloudQueue queue)
        {
            await queue.FetchAttributesAsync();
            return queue.ApproximateMessageCount;
        }

        public override void OnStop()
        {
            this.cancellationTokenSource.Cancel();
            this.runCompleteEvent.WaitOne();
            base.OnStop();
        }

        private async Task RunAsync(CancellationToken cancellationToken)
        {
            // TODO: Replace the following with your own logic.
            while (!cancellationToken.IsCancellationRequested)
            {
                await ReadCommandQueue();
                await UpdateInternalState();
                await UpdateWorkerStatus();

                switch (currentStateNumber)
                {
                    case (byte)STATES.START:
                        // get seed url from the Seed Url Queue
                        currentSeedUrl = await GetSeedUrlFromQueue();
                        // parse robots.txt
                        robotTxtParser.SeedUrl = currentSeedUrl;
                        robotTxtParser.ParseRobotsTxtFile();
                        if(robotTxtParser.GetDisallowedUrlRegex().Any())
                        {
                            foreach (var regex in robotTxtParser.GetDisallowedUrlRegex())
                            {
                                Trace.TraceInformation("Disallowed Regex:" + regex + "\n");
                            }
                        } else
                        {
                            Trace.TraceInformation("Empty");
                        }
 
                        // set current working url of the collector to current seed url value
                        workerStatus.CurrentWorkingUrl = currentSeedUrl;
                        // collect urls using sitemap
                        await UpdateWorkerStatus();
                        await CollectUrlsToCrawlThroughSiteMaps(currentSeedUrl);
                        // collect url from starting from the homepage
                        await CollectUrlStartFromPage(currentSeedUrl, false);
                        // Reset
                        Reset();
                        // sleep for 0.5s
                        Thread.Sleep(500);
                        break;
                    case (byte)STATES.STOP:
                        // wait 10s before Reading again
                        Thread.Sleep(10000);
                        break;
                    case (byte)STATES.IDLE:
                        // wait 5s before Reading again
                        Thread.Sleep(5000);
                        break;
                }
            }
        }

        private void Reset()
        {
            // clear url runtime queue
            urlList.Clear();
            // Empty seed url
            currentSeedUrl = string.Empty;
        }

        private async Task<string> GetSeedUrlFromQueue()
        {
            string url;
            try
            {
                CloudQueueMessage seedUrlObject = await seedUrlQueue.GetMessageAsync();
                url = seedUrlObject.AsString;
                // if all the role instances read the message, delete it from the queue,
                if (seedUrlObject.DequeueCount >= instanceCount)
                {
                    await seedUrlQueue.DeleteMessageAsync(seedUrlObject);
                }
                if (!url.Contains("https://") && !url.Contains("http://"))
                {
                    url = "https://" + url.Trim();
                }
            }
            catch (Exception)
            {
                url = currentSeedUrl;
            }
            return url.Trim(' ').TrimEnd('/');
        }

        // Collect urls using sitemaps
        private async Task CollectUrlsToCrawlThroughSiteMaps(string target)
        {
            if (string.IsNullOrEmpty(target) || string.IsNullOrWhiteSpace(target))
            {
                return;
            }
            if (robotTxtParser.GetSiteMaps().Count == 0)
            {
                return;
            }
            // initialize a XmlDocument Parser
            XmlDocument document = new XmlDocument();
            foreach (string sitemapUrl in robotTxtParser.GetSiteMaps())
            {
                // check command queue
                await ReadCommandQueue();
                if (currentStateNumber == (byte)STATES.STOP)
                {
                    return;
                }
                document.Load(sitemapUrl);
                // check if site map url is a sitemapindex (collection of other sitemaps)
                if (document.DocumentElement.Name.ToLower() == "sitemapindex")
                {
                    await CollectUrlThroughSitemapIndex(document);
                } // if site map url is a actual sitemap (parent node: urlset, collection of actual urls of pages of the target seed url)
                else if (document.DocumentElement.Name.ToLower() == "urlset")
                {
                    await CollectUrlsToCrawlFromSitemap(sitemapUrl);
                }
            }
        }

        private async Task CollectUrlThroughSitemapIndex(XmlDocument document)
        {
            XmlNodeList siteMapNodes = document.GetElementsByTagName("sitemap");
            foreach (XmlNode node in siteMapNodes)
            {
                // check command queue
                await ReadCommandQueue();
                if (currentStateNumber == (byte)STATES.STOP)
                {
                    return;
                }
                string url = node["loc"].InnerText;
                await CollectUrlsToCrawlFromSitemap(url);
            }
        }

        // use this method if 'link' is a link to an actual sitemap
        private async Task CollectUrlsToCrawlFromSitemap(string link)
        {
            XmlDocument document = new XmlDocument();
            try
            {
                document.Load(link);
            }
            catch (Exception)
            { }
            if (document.DocumentElement.Name.ToLower() != "urlset")
            {
                return;
            }
            XmlNodeList urls = document.GetElementsByTagName("url");
            foreach (XmlNode url in urls)
            {
                // check command queue
                await ReadCommandQueue();
                if (currentStateNumber == (byte)STATES.STOP)
                {
                    return;
                }
                string urlString = url["loc"].InnerText;
                string key = Generate256HashCode(url["loc"].InnerText);
                if (urlList.ContainsKey(key))
                {
                    Trace.TraceInformation("URL " + urlString + " Already Exists in the Runtime URL Queue");
                    continue;
                }
                // Insert to the queue (runtime and azure queue)
                urlList.Add(key, urlString);
                if (robotTxtParser.IsURLAllowed(urlString))
                {
                    CloudQueueMessage urlToCrawl;
                    urlToCrawl = new CloudQueueMessage(urlString);
                    await urlQueue.AddMessageAsync(urlToCrawl);
                }
            }
        }

        // recursive method for collecting url starting from the homepage of the target website
        private async Task CollectUrlStartFromPage(string link, bool last)
        {
            if (string.IsNullOrEmpty(link) || string.IsNullOrWhiteSpace(link))
            {
                return;
            }
            try
            {
                htmlDoc = webGet.Load(link);
            } catch (Exception ex)
            {
                WebsitePage page = new WebsitePage(link, "ERROR", "ERROR")
                {
                    ErrorDetails = ex.Message,
                    ErrorTag = "Error Link"
                };
                TableOperation insertOrReplace = TableOperation.InsertOrReplace(page);
                await websitePageTable.ExecuteAsync(insertOrReplace);
                return;
            }
            var linkedPages = htmlDoc.DocumentNode.Descendants("a")
                .Select(a => a.GetAttributeValue("href", null))
                .Where(u => (!string.IsNullOrEmpty(u) && u.StartsWith("/")));
            foreach (string page in linkedPages)
            {
                // check command queue
                await ReadCommandQueue();
                if (currentStateNumber == (byte)STATES.STOP)
                {
                    return;
                }
                bool isLast = false;
                string a = page.TrimStart('/');
                a = currentSeedUrl + "/" + a;
                string key = Generate256HashCode(a);
                if (!robotTxtParser.IsURLAllowed(a))
                {
                    // skip disallowed urls
                    continue;
                }
                if (urlList.ContainsKey(key))
                {
                    // skip urls already saved
                    continue;
                }
                // add to runtime url list
                urlList.Add(key, a);
                await urlQueue.AddMessageAsync(new CloudQueueMessage(a + " "));
                Trace.TraceInformation("Added: " + a + " to the Queue");
                if (page.Equals(linkedPages.LastOrDefault()))
                {
                    isLast = true;
                }
                await CollectUrlStartFromPage(a, isLast);
            }
            if (last)
            {
                return;
            }
        }

        private string Generate256HashCode(string s)
        {
            byte[] data = algorithm.ComputeHash(Encoding.UTF8.GetBytes(s));
            StringBuilder sBuilder = new StringBuilder();
            // Loop through each byte of the hashed data 
            // and format each one as a hexadecimal string.
            for (int i = 0; i < data.Length; i++)
            {
                sBuilder.Append(data[i].ToString("x2"));
            }
            // Return the hexadecimal string.
            return sBuilder.ToString();
        }
    }
}
