using HtmlAgilityPack;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Table;
using MVCWebRole.Models;
using Nager.PublicSuffix;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

enum STATES { START, STOP, IDLE };

namespace Crawler
{
    public class WorkerRole : RoleEntryPoint
    {
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly ManualResetEvent runCompleteEvent = new ManualResetEvent(false);

        private CloudStorageAccount storageAccount;
        private CloudTableClient tableClient;
        private CloudBlobClient blobClient;
        private CloudBlobContainer netscrapContainer;
        private CloudBlob stopwordsBlob;

        private CloudTable websitePageMasterTable;
        private CloudTable domainTable;
        private CloudTable errorTable;
        private CloudTable roleStatusTable;
        private CloudQueueClient queueClient;
        private CloudQueue urlQueue;
        private CloudQueue commandQueue;
        private HtmlDocument htmlDoc;
        private char[] disallowedCharacters;
        HtmlWeb webGet;

        private PerformanceCounter cpuCounter;

        // Container for Website pages to be inserted to the Table
        private List<WebsitePage> container;
        private RoleStatus workerStatus;
        private readonly HashAlgorithm algorithm = SHA256.Create();
        private readonly int minimumWebSiteCount = 50;
        private string instanceId;
        private readonly string[] States = { "RUNNING", "STOPPED", "IDLE" };
        private string currenStateDescription;
        private byte currentStateNumber;
        // current working url, for status report purpose, role status table
        private string currentWorkingUrl;
        private Uri uri;
        private Dictionary<string, string> domainDictionary;
        private readonly DomainParser domainParser = new DomainParser(new WebTldRuleProvider());
        private List<string> stopwords;

        private void Initialize()
        {
            storageAccount = CloudStorageAccount.Parse(RoleEnvironment.GetConfigurationSettingValue("StorageConnectionString"));
            tableClient = storageAccount.CreateCloudTableClient();
            queueClient = storageAccount.CreateCloudQueueClient();
            blobClient = storageAccount.CreateCloudBlobClient();
            stopwords = new List<string>();
            domainDictionary = new Dictionary<string, string>();
            // Initialize all tables and queues and blobs
            // tables
            domainTable = tableClient.GetTableReference("DomainTable");
            websitePageMasterTable = tableClient.GetTableReference("WebsitePageMasterTable");
            errorTable = tableClient.GetTableReference("ErrorTable");
            roleStatusTable = tableClient.GetTableReference("RoleStatus");
            // queues
            urlQueue = queueClient.GetQueueReference("urlstocrawl");
            commandQueue = queueClient.GetQueueReference("command");
            // blobs
            netscrapContainer = blobClient.GetContainerReference("netscrap");
            // Create if not exist
            // tables
            domainTable.CreateIfNotExists();
            roleStatusTable.CreateIfNotExists();
            errorTable.CreateIfNotExistsAsync();
            websitePageMasterTable.CreateIfNotExists();
            // queues
            urlQueue.CreateIfNotExists();
            commandQueue.CreateIfNotExists();
            // blobs
            netscrapContainer.CreateIfNotExistsAsync();
            // stopwords blob
            stopwordsBlob = netscrapContainer.GetBlockBlobReference("stopwords.csv");
            StreamReader streamReader = new StreamReader(stopwordsBlob.OpenRead());
            string line;
            // stop word lists
            while ((line = streamReader.ReadLine()) != null)
            {
                stopwords.Add(line.ToLower());
            }
            // html document parser
            htmlDoc = new HtmlDocument()
            {
                OptionFixNestedTags = true
            };
            disallowedCharacters = new[] { '?', ',', ':', ';' };
            // downloader
            webGet = new HtmlWeb();
            container = new List<WebsitePage>();
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

                                }
                            }
                            // wait 0.5s before Reading again
                            Thread.Sleep(500);
                        }
                        break;
                    case (byte)STATES.STOP:
                        currentWorkingUrl = string.Empty;
                        // insert remaining website page objects to database
                        if (container.Any())
                        {
                            await BatchInsertToDatabase(1);
                        }
                        // wait 10s before Reading again
                        Thread.Sleep(10000);
                        break;
                    case (byte)STATES.IDLE:
                        currentWorkingUrl = string.Empty;
                        // insert remaining website page objects to database
                        if (container.Any())
                        {
                            await BatchInsertToDatabase(1);
                        }
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
            WebsitePage MainWebsitePageObject = new WebsitePage();
            currentWorkingUrl = url;
            // download html page
            try
            {
                htmlDoc = webGet.Load(url);
            }
            catch (WebException)
            {
                ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
                try
                {
                    htmlDoc = webGet.Load(url);
                }
                catch (Exception ex)
                {
                    MainWebsitePageObject.Url = url;
                    MainWebsitePageObject.ErrorTag = "REQUEST ERROR";
                    MainWebsitePageObject.PartitionKey = "REQUEST ERROR";
                    MainWebsitePageObject.RowKey = Generate256HashCode(url);
                    MainWebsitePageObject.Title = "REQUEST ERROR";
                    MainWebsitePageObject.ErrorDetails = ex.Message;
                    await PushErrorPageObject(MainWebsitePageObject);
                    // exit method immediately
                    return;
                }

            }
            // PARSE HTML FOR CONTENT
            // get domain name
            uri = new Uri(url);
            string urlDomain = domainParser.Get(uri.Authority).Domain;
            // parse page title
            string title = "Title";
            HtmlNode titleNode;
            titleNode = htmlDoc.DocumentNode.SelectSingleNode("//title");
            if (titleNode != null && !string.IsNullOrEmpty(titleNode.InnerText))
            {
                title = HtmlEntity.DeEntitize(titleNode.InnerText.Trim());
            }
            else
            {
                titleNode = htmlDoc.DocumentNode.SelectSingleNode("//h1");
                if (titleNode != null && !string.IsNullOrEmpty(titleNode.InnerText))
                {
                    title = HtmlEntity.DeEntitize(titleNode.InnerText.Trim());
                }
            }
            MainWebsitePageObject.Title = title;
            MainWebsitePageObject.Url = url;
            MainWebsitePageObject.PartitionKey = urlDomain;
            MainWebsitePageObject.RowKey = Generate256HashCode(url);
            // parse body content
            string content = "Content Preview Not Available";
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
                content = content.TrimEnd(' ');
            }
            catch (Exception)
            { /* retain default value content = "Content"*/ }

            MainWebsitePageObject.Content = content;
            // check publish date (in meta data tags if available)
            DateTime current = DateTime.UtcNow;
            DateTime? publishDate;
            try
            {
                // get the publish stored in meta data eg. (cnn.com, espn.com)
                string pubDate = htmlDoc.DocumentNode
                    .SelectSingleNode("//meta[@name='pubdate']")
                    .GetAttributeValue("content", string.Empty);
                publishDate = Convert.ToDateTime(pubDate);
                if (!(publishDate.Value.Year == current.Year && (current.Month - publishDate.Value.Month <= 3)))
                {
                    // article is too old, it will not be added to the database
                    Trace.TraceInformation("Article is too old: " + url);
                    return;
                }
            }
            catch (Exception)
            {
                publishDate = null;
            }
            MainWebsitePageObject.PublishDate = publishDate;
            if (urlDomain.Contains("bleacherreport") || urlDomain.Contains("espn"))  // check if site domain is 'bleacherreport' or 'espn'
            {
                string keywords = htmlDoc.DocumentNode
                    .SelectSingleNode("//meta[@name='keywords']")
                    .GetAttributeValue("content", string.Empty);
                if (!keywords.ToLower().Contains("nba")) // check if page is not about nba
                {
                    return;  // exit method immediately
                }
            }
            // check for any errors
            string errorTag = string.Empty;
            string errorDetails = string.Empty;
            if (htmlDoc.ParseErrors.Any())
            {
                errorTag = "HTML PARSING ERROR"; // set ErrorTag
                string errors = string.Empty;
                foreach (var error in htmlDoc.ParseErrors)
                {
                    errors += error.Reason + ";";
                }
                MainWebsitePageObject.ErrorTag = errorTag;
                MainWebsitePageObject.ErrorDetails = errorDetails;
                await PushErrorPageObject(MainWebsitePageObject); // push to ErrorTable
            }

            // if all tests passed,          
            if (!domainDictionary.ContainsKey(urlDomain))  // add domain name to the 'domain table', and 'domainDictionary'
            {
                domainDictionary.Add(urlDomain, urlDomain);
                DomainObject domainObject = new DomainObject(urlDomain);
                TableOperation insertOrMerge = TableOperation.InsertOrMerge(domainObject);
                await domainTable.ExecuteAsync(insertOrMerge);
            }
            CloudTable table = tableClient.GetTableReference(urlDomain);  // create a table using url domain as a name
            await table.CreateIfNotExistsAsync(); // create if does not exist
            var titleKeywords = title  // split the title in to keywords
                .ToLower()
                .Split(' ')
                .Where(s => !stopwords.Contains(s.Trim()))
                .Select(s => s.Trim())
                .Where(s => s.Length >= 2);
            foreach (string word in titleKeywords) // create website page object, use keywords as partition key and add to container
            {
                string keyword = word.ToLower(); // lower the keyword
                keyword = new String(keyword.ToCharArray().Where(c => !disallowedCharacters.Contains(c)).ToArray());
                keyword = keyword.Replace(" ", "");
                if (keyword.IndexOf("'s") >= 0)
                {
                    keyword = keyword.Remove(keyword.IndexOf("'s"), 2).Trim();  // remove "'s"
                }
                WebsitePage page = new WebsitePage(keyword, url) // keyword = partition key, url = rowkey,(hashed)
                {
                    Title = title,
                };
                container.Add(page); // add page to the container
            }
            TableOperation insertOperation = TableOperation.InsertOrMerge(MainWebsitePageObject);  // insert main website page object to the database
            await websitePageMasterTable.ExecuteAsync(insertOperation);
            await BatchInsertToDatabase(minimumWebSiteCount);  // batch insert operation
        }


        private async Task PushErrorPageObject(WebsitePage page)
        {
            TableOperation insertOrMerge = TableOperation.InsertOrMerge(page);
            await errorTable.ExecuteAsync(insertOrMerge);
        }

        private async Task BatchInsertToDatabase(int min)
        {
            if (!(container.Count >= min))
            {
                return;
            }
            Trace.TraceInformation("Batch Insert Operation Initiated");
            IEnumerable<string> tableNames = container.Select(page => page.Domain).Distinct(); // get all distinct table names from all website page objects
            foreach (var tableName in tableNames)
            {
                await BatchInsertViaTableName(tableName, container);
            }
            container.Clear();
        }

        private async Task BatchInsertViaTableName(string tName, List<WebsitePage> list)
        {
            CloudTable table = tableClient.GetTableReference(tName);
            await table.CreateIfNotExistsAsync();  // create if not exist
            var pages = list.Where(page => page.Domain.Equals(tName));   // get all websitepage objects with similar tablename (domain)         
            var partitionkeys = pages.Select(page => page.PartitionKey).Distinct();  // get all distinct partitionkeys, from the list (keywords) distinct       
            TableBatchOperation batchInsertOperation = new TableBatchOperation();   // create a table batch operation object     
            foreach (string key in partitionkeys)  // iterate partitionkeys
            {
                Dictionary<string, string> rowkeys = new Dictionary<string, string>(); // iterate through websitepage object lists with similar partition keys
                foreach (WebsitePage page in pages.Where(p => p.PartitionKey.Equals(key)))
                {
                    if (!rowkeys.ContainsKey(page.RowKey)) // check duplicate items,
                    {
                        batchInsertOperation.InsertOrMerge(page);
                        rowkeys.Add(page.RowKey, page.RowKey);
                    }
                }
                await table.ExecuteBatchAsync(batchInsertOperation);
                batchInsertOperation.Clear();
            }
        }

        private string Generate256HashCode(string s) // used for hashing string
        {
            byte[] data = algorithm.ComputeHash(Encoding.UTF8.GetBytes(s));
            StringBuilder sBuilder = new StringBuilder();
            for (int i = 0; i < data.Length; i++) // Loop through each byte of the hashed data and format each one as a hexadecimal string
            {
                sBuilder.Append(data[i].ToString("x2"));
            }
            return sBuilder.ToString();  // Return the hexadecimal string.
        }
    }
}
