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

        private string seedUrl;
        private readonly HashAlgorithm algorithm = SHA256.Create();
        private Dictionary<string, string> urlList;
        private Robots robotTxtParser;
        private short instanceCount;
        private string instanceId;
        private HtmlDocument htmlDoc;
        private HtmlWeb webGet;
        private string currentState;
        private RoleStatus status;

        private readonly string[] States = { "Running", "Idle", "Stopped" };
        private readonly byte kRunning = 0;
        private readonly byte kIdle = 1;
        private readonly byte kStopped = 2;

        private bool IsStop;
        private bool IsIdle;
        private bool IsRunning;



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
            seedUrl = string.Empty;
            // Create robots.txt parser
            robotTxtParser = new Robots();
            Trace.TraceInformation("Url Collector Worker: Creating queue/s, table/s, if they don't exist.");
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
            // status class
            status = new RoleStatus(this.GetType().Namespace, instanceId);
            // cpu counter
            cpuCounter = new PerformanceCounter("Processor", "% Processor Time", "_Total", true);
            // initial state stop
            currentState = States[kStopped];
            IsStop = true;
            IsIdle = false;
            IsRunning = false;
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
                string.Format("Command Queue: ", currentCommand);
            }
            catch (Exception)
            {
                Trace.TraceInformation(string.Format("Command Queue currently empty, Current State: {0}", currentState));
            }
            // start command is issued
            if (currentCommand.ToLower().Equals("start"))
            {
                IsStop = false;
                IsRunning = true;
                IsIdle = false;
            }
            else
            {
                IsStop = true;
                IsRunning = false;
                IsIdle = false;
            }
        }

        private async Task UpdateRoleStatus()
        {
            status.CPU = Convert.ToInt64(cpuCounter.NextValue());
            long workingMemory = GC.GetTotalMemory(false);
            long totalMemory = Environment.WorkingSet;
            status.WorkingMem = workingMemory;
            status.TotalMem = totalMemory;
            status.State = currentState;
            status.CurrentWorkingUrl = seedUrl;
            TableOperation insertOrReplace = TableOperation.InsertOrReplace(status);
            await roleStatusTable.ExecuteAsync(insertOrReplace);
        }

        private async Task UpdateState()
        {

            int? urlQueueCount = await CountQueueMessages(seedUrlQueue);
            // if seed url queue count has no value or value == 0 and state is running
            if ((!urlQueueCount.HasValue || urlQueueCount.Value == 0) && IsRunning)
            {
                IsRunning = false;
                IsStop = false;
                IsIdle = true;
                currentState = States[kIdle];
            }
            else if ((urlQueueCount.HasValue && urlQueueCount.Value > 0) && IsRunning)
            {
                IsStop = false;
                IsIdle = false;
                currentState = States[kRunning];
            }
            else if (IsStop)
            {
                IsIdle = false;
                IsRunning = false;
                currentState = States[kStopped];
            }
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
                await UpdateState();
                await UpdateRoleStatus();
                if (IsStop)
                {
                    Trace.TraceInformation("Collector " + instanceId + "Stopped");
                    // wait n seconds before Reading againg
                    Thread.Sleep(10000);
                    continue;
                }
                else if (IsIdle)
                {
                    Trace.TraceInformation("Collector" + instanceId + "Idle");
                    // wait n seconds before Reading againg
                    Thread.Sleep(5000);
                    continue;
                }
                // get seed url from the Seed Url Queue
                seedUrl = await GetSeedUrlFromQueue();
                // parse robots.txt
                robotTxtParser.SeedUrl = seedUrl;
                robotTxtParser.ParseRobotsTxtFile();
                // set current working url of the collector to seed url value
                status.CurrentWorkingUrl = seedUrl;
                // collect urls using sitemap
                await UpdateRoleStatus();
                await CollectUrlsToCrawlThroughSiteMaps(seedUrl);
                // collect url from starting from the homepage
                await CollectUrlStartFromPage(seedUrl, false);
                // Reset
                Reset();
                // sleep for one second
                Thread.Sleep(1000);

            }
        }

        private void Reset()
        {
            // clear url runtime queue
            urlList.Clear();
            // Empty seed url
            seedUrl = string.Empty;
            IsIdle = true;
            IsRunning = false;
            IsStop = false;
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
                Trace.TraceInformation("Seed Url Queue is Currently Empty, Current Url: " + seedUrl);
                url = seedUrl;

            }
            return url.Trim(' ').TrimEnd('/');
        }

        // recursive method for collecting urls starting from the home page
        private async Task CollectUrlStartFromPage(string link, bool last)
        {
            await ReadCommandQueue();
            if (IsStop)
            {
                return;
            }
            if (string.IsNullOrEmpty(link) || string.IsNullOrWhiteSpace(link))
            {
                return;
            }
            try
            {
                htmlDoc = webGet.Load(link);
            }
            catch (WebException)
            {
                webGet.BrowserTimeout = TimeSpan.FromSeconds(30);
                ServicePointManager.SecurityProtocol |= SecurityProtocolType.Tls12;
                htmlDoc = webGet.Load(link);
            }
            catch (Exception ex)
            {
                Trace.TraceInformation("Error link: " + link);
                WebsitePage page = new WebsitePage(link, "ERROR", "ERROR")
                {
                    Error = ex.Message
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
                bool isLast = false;
                string a = page.TrimStart('/');
                a = seedUrl + "/" + a;
                string key = Generate256HashCode(a);
                if (!robotTxtParser.IsURLAllowed(a))
                {
                    Trace.TraceInformation("URL is Disallowed");
                    continue;
                }
                if (urlList.ContainsKey(key))
                {
                    Trace.TraceInformation(a + " already exist on the queue");
                    continue;
                }
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

        // Collect urls using sitemaps
        private async Task CollectUrlsToCrawlThroughSiteMaps(string target)
        {
            // stop command is issued
            if (IsStop)
            {
                return;
            }
            if (string.IsNullOrEmpty(target) || string.IsNullOrWhiteSpace(target))
            {
                return;
            }
            if (robotTxtParser.GetSiteMaps().Count == 0)
            {
                Trace.TraceInformation("SITEMAPS IS EMPTY FOR " + robotTxtParser.SeedUrl);
                return;
            }
            // initialize a XmlDocument Parser
            XmlDocument document = new XmlDocument();
            foreach (string sitemapUrl in robotTxtParser.GetSiteMaps())
            {
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
            await ReadCommandQueue();
            if (IsStop)
            {
                return;
            }
            XmlNodeList siteMapNodes = document.GetElementsByTagName("sitemap");
            foreach (XmlNode node in siteMapNodes)
            {
                string url = node["loc"].InnerText;
                await CollectUrlsToCrawlFromSitemap(url);
            }
        }

        // use this method if 'link' is a link to an actual sitemap
        private async Task CollectUrlsToCrawlFromSitemap(string link)
        {
            await ReadCommandQueue();
            if (IsStop)
            {
                return;
            }
            XmlDocument document = new XmlDocument();
            try
            {
                document.Load(link);
            }
            catch (Exception ex)
            {
                Trace.TraceInformation(ex.Message);
            }

            if (document.DocumentElement.Name.ToLower() != "urlset")
            {
                return;
            }
            XmlNodeList urls = document.GetElementsByTagName("url");
            foreach (XmlNode url in urls)
            {
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
