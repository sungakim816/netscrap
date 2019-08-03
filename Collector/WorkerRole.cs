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
        private CloudTable roleStatusTable;
        private CloudTable errorTable;

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
            Trace.TraceInformation("Collector Worker is running");
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
            RoleEnvironment.TraceSource.Switch.Level = SourceLevels.Information;
            bool result = base.OnStart();
            Trace.TraceInformation("Collector Worker has been started");
            Initialize();
            return result;
        }

        private void Initialize()
        {
            storageAccount = CloudStorageAccount.Parse(RoleEnvironment.GetConfigurationSettingValue("StorageConnectionString"));
            // iniatilize instance count
            instanceCount = (short)RoleEnvironment.CurrentRoleInstance.Role.Instances.Count;
            // Initialize Runtime Queue List
            urlList = new Dictionary<string, string>();
            // Initialize Table (WebsitePage Table)
            tableClient = storageAccount.CreateCloudTableClient();
            roleStatusTable = tableClient.GetTableReference("RoleStatus");
            errorTable = tableClient.GetTableReference("ErrorTable");
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
            roleStatusTable.CreateIfNotExistsAsync();
            urlQueue.CreateIfNotExistsAsync();
            seedUrlQueue.CreateIfNotExistsAsync();
            commandQueue.CreateIfNotExistsAsync();
            errorTable.CreateIfNotExistsAsync();
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
            { }
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
                        currentSeedUrl = await GetSeedUrlFromQueue(); // get seed url from the Seed Url Queue
                        robotTxtParser.SeedUrl = currentSeedUrl; // parse robots.txt
                        robotTxtParser.ParseRobotsTxtFile();
                        workerStatus.CurrentWorkingUrl = currentSeedUrl;  // set current working url of the collector to current seed url value
                        await UpdateWorkerStatus();
                        await CollectUrlsToCrawlThroughSiteMaps(currentSeedUrl); // collect urls using sitemap                        
                        await CollectUrlStartFromPage(currentSeedUrl, false); // collect url from starting from the homepage                    
                        Reset(); // Reset
                        Thread.Sleep(500); // sleep for 0.5s
                        break;
                    case (byte)STATES.STOP:                        
                        Reset();
                        Thread.Sleep(10000); // wait 10s before Reading again
                        break;
                    case (byte)STATES.IDLE:               
                        Reset();
                        Thread.Sleep(5000); // wait 5s before Reading again
                        break;
                }
            }
        }

        private void Reset()
        {            
            urlList.Clear(); // clear url runtime queue
            currentSeedUrl = string.Empty;  // Empty seed url
        }

        private async Task<string> GetSeedUrlFromQueue()
        {
            string url;
            try
            {
                CloudQueueMessage seedUrlObject = await seedUrlQueue.GetMessageAsync();
                url = seedUrlObject.AsString;           
                if (seedUrlObject.DequeueCount >= instanceCount)  // if all the role instances read the message, delete it from the queue,
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
     
        private async Task CollectUrlsToCrawlThroughSiteMaps(string target)  // Collect urls using sitemaps
        {
            if (string.IsNullOrEmpty(target) || string.IsNullOrWhiteSpace(target))
            {
                return;
            }
            if (robotTxtParser.GetSiteMaps().Count == 0)
            {
                return;
            }        
            XmlDocument document = new XmlDocument();  // initialize a XmlDocument Parser
            foreach (string sitemapUrl in robotTxtParser.GetSiteMaps())
            {           
                await ReadCommandQueue(); // check command queue
                if (currentStateNumber == (byte)STATES.STOP)
                {
                    return;
                }
                try
                {
                    document.Load(sitemapUrl);
                }
                catch (Exception)
                {
                    ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
                    try
                    {
                        document.Load(sitemapUrl);
                    }
                    catch (Exception ex)
                    {                         
                        await PushErrorPageObject(sitemapUrl, "REQUEST ERROR", ex.Message); // push an error
                        continue;
                    }
                }               
                if (document.DocumentElement.Name.ToLower() == "sitemapindex") // check if site map url is a sitemapindex (collection of other sitemaps)
                {
                    await CollectUrlThroughSitemapIndex(document);
                }  
                else if (document.DocumentElement.Name.ToLower() == "urlset") // if site map url is a actual sitemap (parent node: urlset, collection of actual urls of pages of the target seed url)
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
    
        private async Task CollectUrlsToCrawlFromSitemap(string link) // use this method if 'link' is a link to an actual sitemap
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
                await ReadCommandQueue(); // check command queue
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
                urlList.Add(key, urlString); // Insert to the queue (runtime and azure queue)
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
            }
            catch (Exception)
            {
                ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
                try
                {
                    htmlDoc = webGet.Load(link);
                }
                catch (Exception ex)
                {
                    await PushErrorPageObject(link, "REQUEST ERROR", ex.Message);
                    return;
                }

            }

            var links = htmlDoc.DocumentNode.Descendants("a")
                .Select(a => a.GetAttributeValue("href", null))
                .Where(u => (!string.IsNullOrEmpty(u) && u.StartsWith("/")));
            foreach (string page in links)
            {                 
                await ReadCommandQueue(); // check command queue
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
                    continue; // skip disallowed urls
                }
                if (urlList.ContainsKey(key))
                {                   
                    continue; // skip urls already saved
                }
                // add to runtime url list
                urlList.Add(key, a);
                await urlQueue.AddMessageAsync(new CloudQueueMessage(a));
                if (page.Equals(links.LastOrDefault()))
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
            for (int i = 0; i < data.Length; i++)   // Loop through each byte of the hashed data and format each one as a hexadecimal string
            {
                sBuilder.Append(data[i].ToString("x2"));
            }            
            return sBuilder.ToString(); // Return the hexadecimal string.
        }

        private async Task PushErrorPageObject(string url, string errorTag, string errorDetails)
        {
            WebsitePage page = new WebsitePage
            {
                PartitionKey = errorTag,
                RowKey = Generate256HashCode(url),
                Url = url,
                ErrorDetails = errorDetails,
                ErrorTag = errorTag,
                Title = errorTag
            };
            TableOperation insertOrMerge = TableOperation.InsertOrMerge(page);
            await errorTable.ExecuteAsync(insertOrMerge);
        }

        private async Task PushErrorPageObject(WebsitePage page)
        {
            TableOperation insertOrMerge = TableOperation.InsertOrMerge(page);
            await errorTable.ExecuteAsync(insertOrMerge);
        }
    }
}
