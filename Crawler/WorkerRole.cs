using System;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Table;
using HtmlAgilityPack;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using MVCWebRole.Models;
using Nager.PublicSuffix;

enum STATES { START, STOP, IDLE };

namespace Crawler
{
    public class WorkerRole : RoleEntryPoint
    {
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly ManualResetEvent runCompleteEvent = new ManualResetEvent(false);

        private CloudStorageAccount storageAccount;
        private CloudTableClient tableClient;
        private CloudTable websitepageTable;
        private CloudTable websitepagepartitionKeyTable;
        private CloudTable roleStatusTable;
        private CloudQueueClient queueClient;
        private CloudQueue urlQueue;
        private CloudQueue commandQueue;
        private HtmlDocument htmlDoc;
        HtmlWeb webGet;

        private PerformanceCounter cpuCounter;

        // Container for Website pages to be inserted to the Table
        private Dictionary<string, WebsitePage> container;
        private Dictionary<string, string> partionkeys;
        private TableBatchOperation batchInsert;
        private RoleStatus workerStatus;
        private readonly HashAlgorithm algorithm = SHA256.Create();
        private readonly int minimumWebSiteCount = 30;
        private string instanceId;
        private readonly string[] States = { "RUNNING", "STOPPED", "IDLE" };
        private string currenStateDescription;
        private byte currentStateNumber;
        private DomainParser domainParser;
        // current working url, for status report purpose, role status table
        private string currentWorkingUrl;

        private void Initialize()
        {
            // storageAccount = CloudStorageAccount.DevelopmentStorageAccount; // for local development
            storageAccount = CloudStorageAccount.Parse(RoleEnvironment.GetConfigurationSettingValue("StorageConnectionString"));
            tableClient = storageAccount.CreateCloudTableClient();
            queueClient = storageAccount.CreateCloudQueueClient();

            // Initialize all tables and queues
            // Create if not exist
            websitepageTable = tableClient.GetTableReference("websitepage");
            websitepagepartitionKeyTable = tableClient.GetTableReference("websitepagepartitionkey");
            roleStatusTable = tableClient.GetTableReference("rolestatus");
            websitepageTable.CreateIfNotExists();
            websitepagepartitionKeyTable.CreateIfNotExists();
            roleStatusTable.CreateIfNotExists();
            urlQueue = queueClient.GetQueueReference("urlstocrawl");
            commandQueue = queueClient.GetQueueReference("command");
            urlQueue.CreateIfNotExists();
            commandQueue.CreateIfNotExists();
            // html document parser
            htmlDoc = new HtmlDocument()
            {
                OptionFixNestedTags = true
            };
            // domain parser
            domainParser = new DomainParser(new WebTldRuleProvider());
            // downloader
            webGet = new HtmlWeb();
            container = new Dictionary<string, WebsitePage>();
            partionkeys = new Dictionary<string, string>();
            batchInsert = new TableBatchOperation();
            instanceId = RoleEnvironment.CurrentRoleInstance.Id;

            currentStateNumber = (byte)STATES.STOP;
            currenStateDescription = States[currentStateNumber];
            currentWorkingUrl = string.Empty;

            // role workerStatus
            workerStatus = new RoleStatus(this.GetType().Namespace, instanceId);
            // cpu counter
            cpuCounter = new PerformanceCounter("Processor", "% Processor Time", "_Total", true);
            // clear role workerStatus table
            ClearRoleStatusTableContent(this.GetType().Namespace);
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
                // no command is found from the table, retain the current state
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

        private async Task UpdateWorkerStatus()
        {
            workerStatus.CPU = Convert.ToInt64(cpuCounter.NextValue());
            long workingMemory = GC.GetTotalMemory(false);
            long totalMemory = Environment.WorkingSet;
            workerStatus.WorkingMem = workingMemory;
            workerStatus.TotalMem = totalMemory;
            workerStatus.State = currenStateDescription;
            workerStatus.CurrentWorkingUrl = currentWorkingUrl;
            TableOperation insertOrMerge = TableOperation.InsertOrMerge(workerStatus);
            await roleStatusTable.ExecuteAsync(insertOrMerge);
        }

        private async Task UpdateInternalState()
        {
            int? urlQueueCount = await CountQueueMessages(urlQueue);
            // state: start/run
            if (currentStateNumber == (byte)STATES.START)
            {
                // url queue count has no value or value == 0  
                if (!urlQueueCount.HasValue || urlQueueCount.Value == 0)
                {
                    currentStateNumber = (byte)STATES.IDLE;

                }
                else
                {
                    currenStateDescription = States[currentStateNumber];
                }
                // state: stop/halt    
            }
            else if (currentStateNumber == (byte)STATES.STOP)
            {
                currentStateNumber = (byte)STATES.STOP;
                currentWorkingUrl = string.Empty;
            }
            currenStateDescription = States[currentStateNumber];
        }

        // Generic Method For Counting Queue Message Content
        private async Task<int?> CountQueueMessages(CloudQueue queue)
        {
            await queue.FetchAttributesAsync();
            return queue.ApproximateMessageCount;
        }

        // main run method
        public override void Run()
        {
            Trace.TraceInformation("Crawler Worker is running");
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
            Trace.TraceInformation("Crawler Worker has been started");
            Initialize();
            Trace.TraceInformation("Crawler Worker has Been Initialize");
            return result;
        }

        public override void OnStop()
        {
            Trace.TraceInformation("Crawler Worker is stopping");
            this.cancellationTokenSource.Cancel();
            this.runCompleteEvent.WaitOne();
            base.OnStop();
            Trace.TraceInformation("Crawler Worker has stopped");
        }

        private async Task RunAsync(CancellationToken cancellationToken)
        {
            // TODO: Replace the following with your own logic.
            while (!cancellationToken.IsCancellationRequested)
            {
                await ReadCommandQueue();
                await UpdateInternalState();
                await UpdateWorkerStatus();
                // main state handler
                switch (currentStateNumber)
                {
                    case (byte)STATES.START:
                        {
                            CloudQueueMessage retrieveUrl = null;
                            string url = string.Empty;
                            try
                            {
                                // get a url to crawl from the queue
                                retrieveUrl = urlQueue.GetMessage();
                                url = retrieveUrl.AsString;
                            }
                            catch (Exception)
                            {
                                // retrieveUrl is null, which means url queue is empty
                            }
                            await Crawl(url);
                            // delete queue message
                            if (retrieveUrl != null)
                            {
                                try
                                {
                                    await urlQueue.DeleteMessageAsync(retrieveUrl);
                                }
                                catch (Exception)
                                {
                                    Trace.TraceInformation("Another Process deleted this queue message");
                                }
                            }
                            // wait 0.5s before Reading again
                            Thread.Sleep(500);
                        }
                        break;
                    case (byte)STATES.STOP:
                        if (container.Any())
                        {
                            await BatchPushToDatabase(1);
                        }
                        currentWorkingUrl = string.Empty;
                        // wait 10s before Reading again
                        Thread.Sleep(10000);
                        break;
                    case (byte)STATES.IDLE:
                        currentWorkingUrl = string.Empty;
                        // wait 5s before Reading again
                        Thread.Sleep(5000);
                        break;
                }
            }
        }

        // method for actual crawling and saving to the database
        private async Task Crawl(string url)
        {
            // if url is empty, exit immediately 
            if (string.IsNullOrEmpty(url) || string.IsNullOrWhiteSpace(url))
            {
                return;
            }
            currentWorkingUrl = url;
            // check if container has the website page already
            string key = Generate256HashCode(url);
            if (container.ContainsKey(key))
            {
                return;
            }
            // download html page
            try
            {
                htmlDoc = webGet.Load(url);
            }
            catch (Exception ex)
            {
                // insert to the table in case of an error
                WebsitePage errorPage = new WebsitePage(url, "REQUEST ERROR", "REQUEST ERROR")
                {
                    ErrorTag = "REQUEST ERROR"
                };
                errorPage.ErrorDetails = ex.Message;
                errorPage.Domain = domainParser.Get(url).Domain;
                errorPage.SubDomain = domainParser.Get(url).SubDomain;
                TableOperation insertOrReplace = TableOperation.InsertOrReplace(errorPage);
                await websitepageTable.ExecuteAsync(insertOrReplace);
                // exit method immediately
                return;
            }
            // parse html page for content
            string content = "Content";
            string title = "Title";
            HtmlNode titleNode;
            // page title

            titleNode = htmlDoc.DocumentNode.SelectSingleNode("//h1");
            if(titleNode != null && !string.IsNullOrEmpty(titleNode.InnerText))
            {
                title = titleNode.InnerText;
            } else
            {
                titleNode = htmlDoc.DocumentNode.SelectSingleNode("//title");
                if (titleNode != null && !string.IsNullOrEmpty(titleNode.InnerText))
                {
                    title = titleNode.InnerText;
                }
            }
            // body content
            try
            {
                IEnumerable<string> words = htmlDoc.DocumentNode?
                    .SelectNodes("//body//p//text()")?
                    .Select(x => HtmlEntity.DeEntitize(x.InnerText.Trim()))
                    .Where(x => !string.IsNullOrWhiteSpace(x))
                    .Take(255);
                content = string.Join(" ", words.Where(x => !string.IsNullOrWhiteSpace(x)));
                string[] contentArray = content.Split(' ');
                content = string.Empty;
                foreach (string c in contentArray.Where(x => !string.IsNullOrWhiteSpace(x)))
                {
                    content += c;
                    content += " ";
                }
            }
            catch (Exception)
            {
                // retain default value content = "Content"
            }

            WebsitePage page = new WebsitePage(url, title, content);
            // check for parsing error/s
            if (htmlDoc.ParseErrors.Any())
            {
                page.ErrorTag = "Html Parsing Error";
                string errors = string.Empty;
                foreach (var error in htmlDoc.ParseErrors)
                {
                    errors += (error.Reason + " " + error.SourceText + ".");
                }
                page.ErrorDetails = errors;
            }
            // check publish date/last modified date in the url
            DateTime current = DateTime.UtcNow;
            DateTime? date = null;
            try
            {
                // get the publish stored in meta data Note: cnn.com only!
                var pubDate = htmlDoc.DocumentNode
                    .SelectSingleNode("//meta[@name='pubdate']")
                    .GetAttributeValue("content", string.Empty);
                date = Convert.ToDateTime(pubDate);
                if (date.Value.Year == current.Year && (current.Month - date.Value.Month <= 3))
                {
                    Trace.TraceInformation("Added: " + url);
                    page.PublishDate = date;
                    container.Add(key, page);
                }
            }
            catch (Exception)
            {
                // if publish date is not present, add it immediately to the container
                container.Add(key, page);
            }
            var domainName = domainParser.Get(url);
            page.Domain = domainName.Domain;
            page.SubDomain = domainName.SubDomain;
            // collect partition keys
            if (!partionkeys.ContainsKey(page.PartitionKey))
            {
                partionkeys.Add(page.PartitionKey, page.PartitionKey);
                // insert to the websitepagepartionkey table
                TableOperation insertOrReplacePartionkey = TableOperation.InsertOrReplace(new WebsitePagePartitionKey(page.PartitionKey));
                await websitepagepartitionKeyTable.ExecuteAsync(insertOrReplacePartionkey);
            }
            // wait for n website page entities before pushing to the Table
            await BatchPushToDatabase(minimumWebSiteCount);
        }

        private async Task BatchPushToDatabase(int min)
        {
            if (!(container.Count >= min))
            {
                return;
            }
            foreach (string partitionKey in partionkeys.Values)
            {
                Trace.TraceInformation("Batch InsertOrReplace Initiated..");
                foreach (var p in container.Values.Where(c => c.PartitionKey.Equals(partitionKey)))
                {
                    batchInsert.InsertOrReplace(p);
                }
                if (batchInsert.Count() != 0)
                {
                    await websitepageTable.ExecuteBatchAsync(batchInsert);
                }
                batchInsert.Clear();
            }
            container.Clear();
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
