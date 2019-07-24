using System;
using System.Configuration;
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
using System.Text.RegularExpressions;
using MVCWebRole.Models;

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
        private RoleStatus status;
        private readonly HashAlgorithm algorithm = SHA256.Create();
        private readonly int minimumWebSiteCount = 30;
        private string instanceId;
        private readonly string[] States = { "Running", "Idle", "Stopped" };
        private readonly byte kRunning = 0;
        private readonly byte kIdle = 1;
        private readonly byte kStopped = 2;
        private string currentState;
        private bool IsStop;
        private bool IsIdle;
        private bool IsRunning;

        private string currentWorkingUrl;

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

            // create if not exist
            urlQueue = queueClient.GetQueueReference("urlstocrawl");
            commandQueue = queueClient.GetQueueReference("command");
            urlQueue.CreateIfNotExists();
            commandQueue.CreateIfNotExists();
            // parser
            htmlDoc = new HtmlDocument()
            {
                OptionFixNestedTags = true
            };
            // downloader
            webGet = new HtmlWeb();
            container = new Dictionary<string, WebsitePage>();
            partionkeys = new Dictionary<string, string>();
            batchInsert = new TableBatchOperation();
            instanceId = RoleEnvironment.CurrentRoleInstance.Id;
            IsStop = true;
            IsIdle = false;
            IsRunning = false;
            currentState = States[kStopped];
            currentWorkingUrl = string.Empty;

            status = new RoleStatus(this.GetType().Namespace, instanceId);
            // cpu counter
            cpuCounter = new PerformanceCounter("Processor", "% Processor Time", "_Total", true);
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

        private async Task UpdateRoleStatus()
        {
            status.CPU = Convert.ToInt64(cpuCounter.NextValue());
            long workingMemory = GC.GetTotalMemory(false);
            long totalMemory = Environment.WorkingSet;
            status.WorkingMem = workingMemory;
            status.TotalMem = totalMemory;
            status.State = currentState;
            status.CurrentWorkingUrl = currentWorkingUrl;
            TableOperation insertOrReplace = TableOperation.InsertOrReplace(status);
            await roleStatusTable.ExecuteAsync(insertOrReplace);
        }

        private async Task UpdateState()
        {
            int? urlQueueCount = await CountQueueMessages(urlQueue);
            // if url queue count has no value or value == 0 and state is running
            // set to idle
            if ((!urlQueueCount.HasValue || urlQueueCount.Value == 0) && IsRunning)
            {
                IsRunning = false;
                IsStop = false;
                IsIdle = true;
                currentState = States[kIdle];
                // set to running
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
                currentWorkingUrl = string.Empty;
            }
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
            Trace.TraceInformation("Process ID: " + instanceId);
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
                await UpdateState();
                await UpdateRoleStatus();
                if (IsStop)
                {
                    Trace.TraceInformation("Crawler " + instanceId + "Stopped");
                    // wait n seconds before Reading againg
                    if (container.Any())
                    {
                        await BatchPushToDatabase(1);
                    }
                    currentWorkingUrl = string.Empty;
                    Thread.Sleep(10000);
                    continue;
                }
                else if (IsIdle)
                {
                    Trace.TraceInformation("Crawler " + instanceId + "Idle");
                    // wait n seconds before Reading againg
                    currentWorkingUrl = string.Empty;
                    Thread.Sleep(5000);
                    continue;
                }
                await Crawl();
                Thread.Sleep(500);
            }
        }

        // method for actual crawling and saving to the database
        private async Task Crawl()
        {
            // get message from the url to crawl queue
            CloudQueueMessage retrieveUrl = urlQueue.GetMessage();
            if (retrieveUrl == null)
            {
                if (container.Any())
                {
                    await BatchPushToDatabase(1);
                }
                return;
            }
            Trace.TraceInformation("Crawling");
            string url = retrieveUrl.AsString;
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
                    Error = ex.Message
                };
                TableOperation insertOrReplace = TableOperation.InsertOrReplace(errorPage);
                await websitepageTable.ExecuteAsync(insertOrReplace);
                return;
            }
            // parse html page for content
            string content = "Content";
            string title = "Title";
            HtmlNode titleNode;
            try
            {
                titleNode = htmlDoc.DocumentNode.SelectSingleNode("//h1");
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
                title = titleNode.InnerText.Trim();
            }
            catch (Exception)
            {
                titleNode = htmlDoc.DocumentNode.SelectSingleNode("//title");
                if (titleNode != null)
                {
                    title = titleNode.InnerText.Trim();
                    content = title;
                }
            }
            WebsitePage page = new WebsitePage(url, title, content);
            // check for parsing error/s
            if (htmlDoc.ParseErrors.Any())
            {
                string errors = string.Empty;
                foreach (var error in htmlDoc.ParseErrors)
                {
                    errors += (error.Reason + ";\n");
                }
                page.Error = errors;
            }
            // check publish date/last modified date in the url
            DateTime current = DateTime.UtcNow;
            DateTime? date = null;
            try
            {
                // get the publish date using the url itself 
                Uri uri = new Uri(url);
                string strDate = string.Empty;
                foreach (string s in uri.Segments.Where(x => Regex.IsMatch(x.Trim('/'), @"^\d+$")))
                {
                    strDate += s;
                }
                strDate = strDate.Trim('/');
                date = Convert.ToDateTime(strDate);
                if (date.Value.Year == current.Year && (current.Month - date.Value.Month <= 2))
                {
                    Trace.TraceInformation("Added: " + url);
                    page.PublishDate = date;
                    container.Add(key, page);
                }
            }
            catch (Exception)
            {
                container.Add(key, page);
            }
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
            // delete queue message
            try
            {
                await urlQueue.DeleteMessageAsync(retrieveUrl);
            }
            catch (Exception)
            {
                Trace.TraceInformation("Message was Deleted by the other Process.");
            }
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
