using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Table;
using MVCWebRole.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Web;
using System.Web.Mvc;
using PagedList;
using System.Diagnostics;

namespace MVCWebRole.Controllers
{
    public class DashboardController : Controller
    {
        private readonly CloudStorageAccount storageAccount;
        private readonly CloudQueueClient queueClient;
        private readonly CloudTableClient tableClient;

        public DashboardController()
        {
            storageAccount = CloudStorageAccount.Parse(
                RoleEnvironment.GetConfigurationSettingValue("StorageConnectionString"));
            queueClient = storageAccount.CreateCloudQueueClient();
            tableClient = storageAccount.CreateCloudTableClient();
        }

        // GET: Dashboard
        [HttpGet]
        public ActionResult Index()
        {
            return View();
        }

        /// <summary>
        /// Show Details of a specific crawled page using partitionkey and rowkey
        /// </summary>
        /// <param name="partitionkey"></param>
        /// <param name="rowkey"></param>
        /// <returns></returns>
        [HttpGet]
        [Route("Dashboard/ShowIndexedDetails")]
        [Route("Dashboard/Show/Latest/Details/{partitionkey}/{rowkey}")]
        public ActionResult ShowIndexedDetails(string partitionkey, string rowkey)
        {
            if (rowkey == null || partitionkey == null)
            {
                return View();
            }
            if (!(partitionkey.Any() || rowkey.Any()))
            {
                return View();
            }
            CloudTable websitePageMasterTable = tableClient.GetTableReference("WebsitePageMasterTable");
            websitePageMasterTable.CreateIfNotExistsAsync();

            TableQuery<WebsitePage> query = new TableQuery<WebsitePage>().Where(
                TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionkey),
                    TableOperators.And,
                    TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowkey)
                    )
                ).Take(1);
            var queryResult = websitePageMasterTable.ExecuteQuery(query);
            WebsitePage result = queryResult.FirstOrDefault();
            return View(result);
        }

        /// <summary>
        /// Method for showing 'N' Latest Indexed Websites
        /// </summary>
        /// <param name="count">N</param>
        /// <returns></returns>
        [HttpGet]
        [Route("Dashboard/ShowLatestIndexed")]
        [Route("Dashboard/Show/Latest/{count:regex(^[1-9]{0,3}$)}")]
        public async Task<ActionResult> ShowLatestIndexed(int? count)
        {
            count = count ?? 10;
            CloudTable websitePageMasterTable = tableClient
                .GetTableReference("WebsitePageMasterTable");
            await websitePageMasterTable.CreateIfNotExistsAsync();
            // TableContinuationToken continuationToken = null;
            TableQuery<WebsitePage> rangeQuery = new TableQuery<WebsitePage>()
                .Select(new string[] { "PartitionKey", "RowKey", "DateCrawled", "Title", "SubDomain" })
                .Take(count.Value);
            var results = new List<WebsitePage>();
            results.AddRange(websitePageMasterTable.ExecuteQuery(rangeQuery));

            //int c = 0;
            //do
            //{
            //    TableQuerySegment<WebsitePage> rangeResult = await websitePageMasterTable
            //       .ExecuteQuerySegmentedAsync(rangeQuery, continuationToken);
            //    results.AddRange(rangeResult.OrderByDescending(x => x.DateCrawled).Take(10));
            //    continuationToken = rangeResult.ContinuationToken;
            //    c++;
            //    Trace.TraceInformation(c + ":Kim Sunga");
            //} while (continuationToken != null);
            List<WebsitePage> websitePages = new List<WebsitePage>();
            websitePages.AddRange(results.OrderByDescending(x => x.DateCrawled).Take(count.Value));
            return View(websitePages);
        }

        /// <summary>
        /// Method for Counting messages in URL Queue 
        /// </summary>
        /// <returns></returns>
        public async Task<int?> UrlQueueCount()
        {
            CloudQueue urlQueue = queueClient.GetQueueReference("urlstocrawl");
            await urlQueue.CreateIfNotExistsAsync();
            return await CountQueueMessagesAsync(urlQueue);
        }

        /// <summary>
        /// Method for Counting Indexed Websites
        /// </summary>
        /// <returns></returns>
        public async Task<int?> IndexedWebsiteCount()
        {
            CloudTable websitePageMasterTable = tableClient
                .GetTableReference("WebsitePageMasterTable");
            CloudTable domainTable = tableClient.GetTableReference("DomainTable");
            await websitePageMasterTable.CreateIfNotExistsAsync();
            await domainTable.CreateIfNotExistsAsync();

            var keys = domainTable
                .ExecuteQuery(new TableQuery<DomainObject>()
                .Select(new string[] { "PartitionKey" }))
                .Select(x => x.PartitionKey);
            return await CountTableRows(websitePageMasterTable, keys);
        }

        /// <summary>
        /// Generic Method for counting entities on any Table
        /// </summary>
        /// <param name="table"></param>
        /// <param name="partitionKey"></param>
        /// <returns></returns>
        protected async Task<int?> GetCountOfEntitiesInPartition(CloudTable table, string partitionKey)
        {
            TableQuery<DynamicTableEntity> tableQuery = new TableQuery<DynamicTableEntity>()
                .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey))
                .Select(new string[] { "PartitionKey" });
            TableContinuationToken continuationToken = null;
            int total = 0;
            do
            {
                var tableQueryResult = await table.ExecuteQuerySegmentedAsync(tableQuery, continuationToken);
                continuationToken = tableQueryResult.ContinuationToken;
                total += tableQueryResult.Count();
            } while (continuationToken != null);
            return total;
        }

        /// <summary>
        /// Generic Method for counting rows of any Tables
        /// </summary>
        /// <param name="table"></param>
        /// <param name="partitionKeys"></param>
        /// <returns></returns>
        private async Task<int?> CountTableRows(CloudTable table, IEnumerable<string> partitionKeys)
        {
            int? total = 0;
            foreach (string key in partitionKeys)
            {
                total += await GetCountOfEntitiesInPartition(table, key);
            }
            return total;
        }

        /// <summary>
        /// Generic Method for counting messages on any Queue
        /// </summary>
        /// <param name="queue"></param>
        /// <returns></returns>
        private async Task<int?> CountQueueMessagesAsync(CloudQueue queue)
        {
            await queue.FetchAttributesAsync();
            return queue.ApproximateMessageCount;
        }

        private bool IsUrlValid(string url)
        {
            if (!url.Contains("https://") && !url.Contains("http://"))
            {
                url = "https://" + url.Trim();
            }
            return Uri.TryCreate(url, UriKind.Absolute, out _);
        }

        /// <summary>
        /// Method for adding Seed URL (Target Webiste)
        /// </summary>
        /// <param name="url"></param>
        /// <returns></returns>
        [HttpGet]
        [Route("Dashboard/AddSeedUrl")]
        [Route("Dashboard/Seed/{url}")]
        public async Task<ActionResult> AddSeedUrl(string url)
        {

            if (string.IsNullOrEmpty(url) || string.IsNullOrWhiteSpace(url))
            {
                return View(false);
            }
            if (!IsUrlValid(url))
            {
                return View(false);
            }
            bool response;
            CloudQueue seedUrlQueue = queueClient.GetQueueReference("seedurls");
            await seedUrlQueue.CreateIfNotExistsAsync();
            try
            {
                CloudQueueMessage seedUrl = new CloudQueueMessage(url);
                await seedUrlQueue.AddMessageAsync(seedUrl);
                response = true;
            }
            catch (Exception)
            {
                response = false;
            }
            return View(response);
        }


        /// <summary>
        /// Method to start all crawlers
        /// </summary>
        /// <returns></returns>
        public async Task<ActionResult> StartCrawler()
        {
            return View(await CommandCrawler("start"));
        }

        /// <summary>
        /// Method for stopping all crawlers
        /// </summary>
        /// <returns></returns>
        public async Task<ActionResult> StopCrawler()
        {
            return View(await CommandCrawler("stop"));
        }

        /// <summary>
        /// Method to issue a command to the crawlers
        /// </summary>
        /// <param name="command"></param>
        /// <returns></returns>
        private async Task<bool?> CommandCrawler(string command)
        {
            CloudQueueMessage message = new CloudQueueMessage(command);
            bool response;
            CloudQueue commandQueue = queueClient.GetQueueReference("command");
            await commandQueue.CreateIfNotExistsAsync();
            try
            {
                await commandQueue.ClearAsync();
                await commandQueue.AddMessageAsync(message);
                response = true;
            }
            catch (Exception)
            {
                response = false;
            }
            return response;
        }

        /// <summary>
        /// Method to Clear URL Queue Contents
        /// </summary>
        /// <returns></returns>
        [HttpPost]
        public async Task<ActionResult> ClearUrlQueue()
        {
            bool response;
            CloudQueue urlQueue = queueClient.GetQueueReference("urlstocrawl");
            await urlQueue.CreateIfNotExistsAsync();
            try
            {
                await urlQueue.ClearAsync();
                response = true;
            }
            catch (Exception)
            {
                response = false;
            }
            return View(response);
        }

        /// <summary>
        /// Method to Delete all Indexed URLs
        /// </summary>
        /// <returns></returns>
        [HttpPost]
        public async Task<ActionResult> ClearIndexedUrls()
        {
            bool response;
            CloudTable websitePageMasterTable = tableClient.GetTableReference("WebsitePageMasterTable");
            CloudTable errorTable = tableClient.GetTableReference("ErrorTable");
            CloudTable domainTable = tableClient.GetTableReference("DomainTable");
            try
            {
                await websitePageMasterTable.DeleteIfExistsAsync(); // delete websitePageMaster Table
                await websitePageMasterTable.CreateIfNotExistsAsync(); // recreate the table
                await errorTable.DeleteIfExistsAsync(); // delete error table
                await errorTable.CreateIfNotExistsAsync(); // recreate
                List<string> tableNames = domainTable
                .ExecuteQuery(new TableQuery<DomainObject>()
                .Select(new string[] { "PartitionKey" }))
                .Select(x => x.PartitionKey)
                .ToList();
                foreach (string tableName in tableNames)
                {
                    CloudTable table = tableClient.GetTableReference(tableName);
                    await table.DeleteIfExistsAsync();  // delete domain tables
                    await table.CreateIfNotExistsAsync(); // re create
                }
                response = true;
            }
            catch (Exception)
            {
                response = false;
            }
            return View(response);
        }

        /// <summary>
        /// Method to clear everything, Indexed and Queue
        /// </summary>
        /// <returns></returns>
        [HttpPost]
        public async Task<ActionResult> ClearAll()
        {
            bool response;
            try
            {
                await ClearUrlQueue();
                await ClearIndexedUrls();
                response = true;  // set response true if successful
            }
            catch (Exception)
            {
                response = false;  // false if something went wrong
            }

            return View(response);
        }

        /// <summary>
        /// Method to display worker role statuses
        /// </summary>
        /// <returns></returns>
        [HttpGet]
        public async Task<ActionResult> WorkerStatus()
        {
            CloudTable roleStatusTable = tableClient.GetTableReference("RoleStatus");
            await roleStatusTable.CreateIfNotExistsAsync();
            var workerList = roleStatusTable.ExecuteQuery(new TableQuery<RoleStatus>());
            return View(workerList);
        }

        /// <summary>
        /// Method to Display Webpages with errors
        /// </summary>
        /// <param name="pageNumber"></param>
        /// <returns></returns>
        [HttpGet]
        [Route("Dashboard/ErrorList/")]
        [Route("Dashboard/Errors/{pageNumber:regex(^[1-9]{0,3}$)}")]
        public async Task<ActionResult> ErrorList(int? pageNumber)
        {
            // count, results per page
            int pageSize = 15; // items per pages
            pageNumber = pageNumber.HasValue ? pageNumber : 1;
            CloudTable errorTable = tableClient.GetTableReference("ErrorTable");
            await errorTable.CreateIfNotExistsAsync();
            TableQuery<WebsitePage> rangeQuery = new TableQuery<WebsitePage>();
            TableContinuationToken continuationToken = null;
            List<WebsitePage> results = new List<WebsitePage>();
            TableQuerySegment<WebsitePage> segmentResult;
            do
            {
                segmentResult = await errorTable
                    .ExecuteQuerySegmentedAsync(rangeQuery, continuationToken);
                results.AddRange(segmentResult);
                segmentResult.Results.Clear();
                continuationToken = segmentResult.ContinuationToken;
            } while (continuationToken != null);
            List<WebsitePage> websitePages = new List<WebsitePage>();
            websitePages.AddRange(results.OrderByDescending(r => r.DateCrawled).Take(150));
            results.Clear();
            return PartialView(websitePages.ToPagedList((int)pageNumber, pageSize));
        }

        /// <summary>
        /// Method to show most popular user queries/searches
        /// </summary>
        /// <param name="pageNumber"></param>
        /// <returns></returns>
        [HttpGet]
        [Route("Dashboard/PopularSearch/")]
        [Route("Dashboard/Popular/Search/{pageNumber:regex(^[1-9]{0,3}$)}")]
        public async Task<ActionResult> PopularSearch(int? pageNumber)
        {
            int pageSize = 15;
            pageNumber = pageNumber.HasValue ? pageNumber : 1;
            CloudTable websitePageMasterTable = tableClient.GetTableReference("WebsitePageMasterTable");
            await websitePageMasterTable.CreateIfNotExistsAsync();
            TableQuery<WebsitePage> rangeQuery = new TableQuery<WebsitePage>()
                .Where(TableQuery.GenerateFilterConditionForInt("Clicks", QueryComparisons.GreaterThan, 0)).
                Select(new string[] { "Title", "Domain", "Clicks", "RowKey", "Url" });
            List<WebsitePage> results = new List<WebsitePage>();
            TableContinuationToken continuationToken = null;
            TableQuerySegment<WebsitePage> segmentResult;
            do
            {
                segmentResult = await websitePageMasterTable
                    .ExecuteQuerySegmentedAsync(rangeQuery, continuationToken);
                results.AddRange(segmentResult);
                segmentResult.Results.Clear();
                continuationToken = segmentResult.ContinuationToken;
            } while (continuationToken != null);
            List<WebsitePage> websitePages = new List<WebsitePage>();
            websitePages.AddRange(results.OrderByDescending(r => r.Clicks).Take(150));
            results.Clear();
            return View(websitePages.ToPagedList((int)pageNumber, pageSize));
        }

        /// <summary>
        /// Method for showing Crawled Domains
        /// </summary>
        /// <returns></returns>
        [HttpGet]
        [Route("Dashboard/CrawledDomain")]
        public async Task<ActionResult> CrawledDomain()
        {
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudTable domainTable = tableClient.GetTableReference("DomainTable");
            await domainTable.CreateIfNotExistsAsync();
            var crawledDomain = domainTable.ExecuteQuery(new TableQuery<DomainObject>());
            return View(crawledDomain);
        }
    }
}