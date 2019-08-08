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
        private CloudQueue indexedCountQueue;
        private HtmlDocument htmlDoc;
        private char[] disallowedCharacters;
        HtmlWeb webGet;

        private PerformanceCounter cpuCounter;

        private List<WebsitePage> container; // Container for Website pages to be inserted to the Table (needed for batch operation)
        private RoleStatus workerStatus; // object worker status reporting
        private readonly HashAlgorithm algorithm = SHA256.Create();
        private readonly int minimumWebSiteCount = 50; // minimun website count before pushing to the database
        private string instanceId;
        private readonly string[] States = { "RUNNING", "STOPPED", "IDLE" };
        private string currenStateDescription;
        private byte currentStateNumber;
        // current working url, for status report purposes, role status table
        private string currentWorkingUrl;
        private Uri uri;
        private Dictionary<string, string> domainDictionary;
        private readonly DomainParser domainParser = new DomainParser(new WebTldRuleProvider()); // domain parser
        private List<string> stopwords;

        /// <summary>
        /// Method for Initializing everything
        /// </summary>
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
            indexedCountQueue = queueClient.GetQueueReference("indexedcount");
            // blobs
            netscrapContainer = blobClient.GetContainerReference("netscrap");
            // Create if not exist
            // tables
            domainTable.CreateIfNotExists();
            roleStatusTable.CreateIfNotExists();
            errorTable.CreateIfNotExistsAsync();
            websitePageMasterTable.CreateIfNotExists();
            urlQueue.CreateIfNotExists(); // queues
            commandQueue.CreateIfNotExists();
            bool isCreated = indexedCountQueue.CreateIfNotExists();
            if(isCreated) // if just created initialize value to zero
            {
                indexedCountQueue.AddMessageAsync(new CloudQueueMessage("0"));
            }
            netscrapContainer.CreateIfNotExistsAsync(); // blobs        
            stopwordsBlob = netscrapContainer.GetBlockBlobReference("stopwords.csv");  // stopwords blob
            StreamReader streamReader = new StreamReader(stopwordsBlob.OpenRead());
            string line;
            // stop word lists
            while ((line = streamReader.ReadLine()) != null)
            {
                stopwords.Add(line.ToLower());
            }
            htmlDoc = new HtmlDocument()
            {
                OptionFixNestedTags = true
            };  // html document parser
            disallowedCharacters = new[] { '?', ',', ':', ';', '!', '&', '(', ')', '"' };
            webGet = new HtmlWeb(); // downloader
            container = new List<WebsitePage>();
            instanceId = RoleEnvironment.CurrentRoleInstance.Id;
            currentStateNumber = (byte)STATES.STOP;
            currenStateDescription = States[currentStateNumber];
            currentWorkingUrl = string.Empty;
            workerStatus = new RoleStatus(this.GetType().Namespace, instanceId);  // role workerStatus      
            cpuCounter = new PerformanceCounter("Processor", "% Processor Time", "_Total", true);  // cpu counter           
            ClearRoleStatusTableContent(this.GetType().Namespace); // clear role workerStatus table
        }

        /// <summary>
        /// Method for Reading the Command Queue
        /// </summary>
        /// <returns></returns>
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

        /// <summary>
        /// Method for Clearing Role Status table
        /// </summary>
        /// <param name="partitionKey">Namespace of the WorkerRole (eg. Crawler)</param>
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

        /// <summary>
        /// Method for Updating worker status
        /// </summary>
        /// <returns></returns>
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

        /// <summary>
        /// Method for updating internal state (Role Status reporting purposes)
        /// </summary>
        /// <returns></returns>
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

        /// <summary>
        /// Generic method for counting queue message
        /// </summary>
        /// <param name="queue"></param>
        /// <returns></returns>
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
            RoleEnvironment.TraceSource.Switch.Level = SourceLevels.Information;
            bool result = base.OnStart();
            Trace.TraceInformation("Crawler Worker has been started");
            Initialize();
            Trace.TraceInformation("Crawler Worker has Been Initialize");
            return result;
        }

        /// <summary>
        /// Build-in Method for stopping the whole process
        /// </summary>
        public override void OnStop()
        {
            Trace.TraceInformation("Crawler Worker is stopping");
            this.cancellationTokenSource.Cancel();
            this.runCompleteEvent.WaitOne();
            base.OnStop();
            Trace.TraceInformation("Crawler Worker has stopped");
        }

        /// <summary>
        /// Build-in method for executing methods during 'OnRun' State
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        private async Task RunAsync(CancellationToken cancellationToken)
        {
            // TODO: Replace the following with your own logic.
            while (!cancellationToken.IsCancellationRequested)
            {
                await ReadCommandQueue();
                await UpdateInternalState();
                await UpdateWorkerStatus();
                switch (currentStateNumber)  // main state handler
                {
                    case (byte)STATES.START:
                        {
                            CloudQueueMessage retrieveUrl = null;
                            string url;
                            try
                            {
                                retrieveUrl = urlQueue.GetMessage();
                                url = retrieveUrl.AsString;
                            }
                            catch (Exception)
                            {
                                // retrieveUrl is null, which means url queue is empty
                                url = string.Empty;
                            }
                            await Crawl(url);
                            try
                            {
                                await urlQueue.DeleteMessageAsync(retrieveUrl);
                            }
                            catch (Exception)
                            { /* Other Process deleted the message already */}
                            Thread.Sleep(500); // wait 0.5s before Reading again
                        }
                        break;
                    case (byte)STATES.STOP:
                        currentWorkingUrl = string.Empty;
                        if (container.Any()) // insert remaining website page objects to database
                        {
                            await BatchInsertToDatabase(1);
                        }
                        Thread.Sleep(10000); // wait 10s before Reading again
                        break;
                    case (byte)STATES.IDLE:
                        currentWorkingUrl = string.Empty;
                        if (container.Any())  // insert remaining website page objects to database
                        {
                            await BatchInsertToDatabase(1);
                        }
                        Thread.Sleep(5000); // wait 5s before Reading again
                        break;
                }
            }
        }

        /// <summary>
        /// Method for Crawling and parsing Html page
        /// </summary>
        /// <param name="url"></param>
        /// <returns></returns>
        private async Task Crawl(string url)
        {
            if (string.IsNullOrEmpty(url) || string.IsNullOrWhiteSpace(url))  // if url is empty, exit immediately 
            {
                return;
            }
            WebsitePage MainWebsitePageObject = new WebsitePage(); // instantiate Main Website Page Object
            currentWorkingUrl = url;
            try
            {
                htmlDoc = webGet.Load(url); // download html page
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
            uri = new Uri(url);  // get domain name
            string urlDomain = domainParser.Get(uri.Authority).Domain;  // get domain name
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
            DateTime current = DateTime.UtcNow;
            DateTime? publishDate; // check publish date (in meta data tags if available)
            try
            {
                // get the publish stored in meta data eg. (cnn.com, espn.com)
                string pubDate = htmlDoc.DocumentNode
                    .SelectSingleNode("//meta[@name='pubdate']")
                    .GetAttributeValue("content", string.Empty);
                publishDate = Convert.ToDateTime(pubDate);
                if (!(publishDate.Value.Year == current.Year && (current.Month - publishDate.Value.Month <= 3)))
                {
                    Trace.TraceInformation("Article is too old: " + url); // article is too old, it will not be added to the database
                    return;
                }
            }
            catch (Exception)
            {
                publishDate = null;
            }
            MainWebsitePageObject.PublishDate = publishDate;
            if (urlDomain.Contains("bleacherreport"))  // check if site domain is 'bleacherreport'
            {
                string keywords = htmlDoc.DocumentNode
                    .SelectSingleNode("//meta[@name='keywords']")
                    .GetAttributeValue("content", string.Empty);
                if (!keywords.ToLower().Contains("nba")) // check if page is not about nba
                {
                    return;  // exit method immediately this will not be added to the database
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
                TableOperation insertOrMerge = TableOperation.InsertOrMerge(domainObject); // Table operation insertOrMerge
                await domainTable.ExecuteAsync(insertOrMerge);
            }
            CloudTable table = tableClient.GetTableReference(urlDomain);  // create a table using url domain as a name
            await table.CreateIfNotExistsAsync(); // create if does not exist
            var titleKeywords = GetValidKeywords(title);
            foreach (string word in titleKeywords) // create website page object, use keywords as partition key and add to container
            {
                WebsitePage page = new WebsitePage(word, url) // keyword = partition key, url = rowkey,(hashed)
                {
                    Title = title,
                };
                container.Add(page); // add page to the container
            }
            bool exists = await Exists(websitePageMasterTable, MainWebsitePageObject.PartitionKey, MainWebsitePageObject.RowKey);
            if (!exists) // push if does not exists
            {
                TableOperation insertOperation = TableOperation.InsertOrMerge(MainWebsitePageObject);  // insert main website page object to the database 
                await websitePageMasterTable.ExecuteAsync(insertOperation); // try insert to the database
                await UpdateIndexedCount();
            }          
            await BatchInsertToDatabase(minimumWebSiteCount);  // batch insert operation
        }

        /// <summary>
        /// Method to Update Indexed Website Count Queue
        /// </summary>
        /// <returns></returns>
        private async Task UpdateIndexedCount()
        {
            bool isDone = false;
            while (!isDone)
            {
                try
                {
                    CloudQueueMessage indexedCount = await indexedCountQueue.GetMessageAsync(); // get message queue
                    if (indexedCount == null)
                    {
                        Thread.Sleep(100); // sleep for 500 ms before reading again
                    }
                    else // replace the value
                    {
                        await indexedCountQueue.ClearAsync(); 
                        var count = Convert.ToInt64(indexedCount.AsString) + 1;
                        await indexedCountQueue.AddMessageAsync(new CloudQueueMessage(count.ToString()));
                        isDone = true; // eixt the loop
                    }
                }
                catch (Exception)
                {
                    continue;
                }
            }
        }

        /// <summary>
        /// Method for generating valid keywords from an input string
        /// </summary>
        /// <param name="query"></param>
        /// <returns name="filteredKeywords"></returns>
        private List<string> GetValidKeywords(string query)
        {
            if (string.IsNullOrEmpty(query) || string.IsNullOrWhiteSpace(query))
            {
                return new List<string>();
            }
            IEnumerable<string> keywords = query.ToLower().Split(' ').AsEnumerable(); // split 'query' into 'keywords'
            keywords = keywords
                .Where(k => k.Length >= 2 && !stopwords.Contains(k.Trim()) && k.Any(c => char.IsLetter(c)))
                .Select(k => k.Trim('\'').Trim('"').Trim()); // remove unnecessary characters
            List<string> filteredKeywords = new List<string>(); // create an empty list for filtered keywords
            foreach (var word in keywords)
            {
                string keyword = word;
                keyword = new string(keyword.ToCharArray().Where(c => !disallowedCharacters.Contains(c)).ToArray()); // check for disallowed characters
                if (keyword.IndexOf("'s") >= 0)
                {
                    keyword = keyword.Remove(keyword.IndexOf("'s"), 2);  // remove 's
                }
                keyword = keyword.Trim('\'').Trim('"').Trim('?').TrimEnd('.'); // remove unncessary characters again
                if (keyword.Length < 2 || stopwords.Contains(keyword)) // check for length and for 'stopwords'
                {
                    continue;
                }
                filteredKeywords.Add(keyword); // add to the list
            }
            return filteredKeywords;
        }

        /// <summary>
        /// Method for Inserting Error Page Object
        /// </summary>
        /// <param name="page"></param>
        /// <returns></returns>
        private async Task PushErrorPageObject(WebsitePage page)
        {
            TableOperation insertOrMerge = TableOperation.InsertOrMerge(page);
            await errorTable.ExecuteAsync(insertOrMerge);
        }

        /// <summary>
        /// Actual method For pushing new entries to the database
        /// </summary>
        /// <param name="min"></param>
        /// <returns></returns>
        private async Task BatchInsertToDatabase(int min)
        {
            if (!(container.Count >= min)) // check if there's enough element to push to the database
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

        /// <summary>
        /// Batch Insert Operation for optimization purposes, 
        /// </summary>
        /// <param name="tName">Name of the table</param>
        /// <param name="list">List of Object</param>
        /// <returns></returns>
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

        /// <summary>
        /// Return hashed version of a string
        /// </summary>
        /// <param name="s"></param>
        /// <returns name="sBuilder.ToString()"></returns>
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

        /// <summary>
        /// Method for checking if Website Page object Already exists in the database
        /// </summary>
        /// <param name="table"></param>
        /// <param name="partitionKey"></param>
        /// <param name="rowKey"></param>
        /// <returns>bool</returns>
        private async Task<bool> Exists(CloudTable table, string partitionKey, string rowKey)
        {
            TableOperation retrieve = TableOperation.Retrieve(partitionKey, rowKey);
            var result = await table.ExecuteAsync(retrieve);
            if(result.Result != null)
            {
                return true;
            }
            return false;
        }
    }
}
