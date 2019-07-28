﻿using Microsoft.WindowsAzure.ServiceRuntime;
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

namespace MVCWebRole.Controllers
{
    public class DashboardController : Controller
    {
        private readonly CloudStorageAccount storageAccount;

        private readonly CloudQueue commandQueue;
        private readonly CloudQueue urlQueue;
        private readonly CloudQueue seedUrlQueue;
        private readonly CloudQueueClient queueClient;

        private readonly CloudTable websitePageTable;
        private readonly CloudTable websitePagePartitionKeyTable;
        private readonly CloudTable roleStatusTable;
        private readonly CloudTableClient tableClient;

        public DashboardController()
        {
            storageAccount = CloudStorageAccount.Parse(RoleEnvironment.GetConfigurationSettingValue("StorageConnectionString"));
            queueClient = storageAccount.CreateCloudQueueClient();
            tableClient = storageAccount.CreateCloudTableClient();

            commandQueue = queueClient.GetQueueReference("command");
            urlQueue = queueClient.GetQueueReference("urlstocrawl");
            seedUrlQueue = queueClient.GetQueueReference("seedurls");

            websitePageTable = tableClient.GetTableReference("websitepage");
            websitePagePartitionKeyTable = tableClient.GetTableReference("websitepagepartitionkey");
            roleStatusTable = tableClient.GetTableReference("rolestatus");
            commandQueue.CreateIfNotExistsAsync();
            websitePageTable.CreateIfNotExistsAsync();
            urlQueue.CreateIfNotExistsAsync();
            seedUrlQueue.CreateIfNotExistsAsync();
            roleStatusTable.CreateIfNotExists();
            websitePagePartitionKeyTable.CreateIfNotExistsAsync();
        }

        // GET: Dashboard
        [HttpGet]
        // [OutputCache(Duration = 15)]
        public ActionResult Index()
        {
            return View();
        }

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
            TableQuery<WebsitePage> query = new TableQuery<WebsitePage>().Where(
                TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionkey),
                    TableOperators.And,
                    TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowkey)
                    )
                ).Take(1);
            var queryResult = websitePageTable.ExecuteQuery(query);
            WebsitePage result = queryResult.FirstOrDefault();
            return View(result);
        }

        [HttpGet]
        [Route("Dashboard/ShowLatestIndexed")]
        [Route("Dashboard/Show/Latest/{count:regex(^[1-9]{0,3}$)}")]
        public ActionResult ShowLatestIndexed(int? count)
        {
            if (!count.HasValue)
            {
                count = 10;
            }
            TableQuery<WebsitePage> rangeQuery = new TableQuery<WebsitePage>()
                .Select(new List<string> { "Timestamp", "Title", "PartitionKey", "RowKey", "Domain", "SubDomain" });
            var websitepages = websitePageTable.ExecuteQuery(rangeQuery);
            websitepages = websitepages.OrderByDescending(x => x.Timestamp).Take(count.Value);
            return View(websitepages);
        }

        public async Task<int?> UrlQueueCount()
        {
            return await CountQueueMessagesAsync(urlQueue);
        }

        public async Task<int?> IndexedWebsiteCount()
        {
            var keys = websitePagePartitionKeyTable
                .ExecuteQuery(new TableQuery<WebsitePagePartitionKey>()
                .Select(new List<string> { "PartitionKey" }))
                .Select(x => x.PartitionKey)
                .ToList();
            return await CountTableRows(websitePageTable, keys);
        }

        protected async Task<int?> GetCountOfEntitiesInPartition(CloudTable table, string partitionKey)
        {
            TableQuery<DynamicTableEntity> tableQuery = new TableQuery<DynamicTableEntity>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey)).Select(new string[] { "PartitionKey" });
            string resolver(string pk, string rk, DateTimeOffset ts, IDictionary<string, EntityProperty> props, string etag) => props.ContainsKey("PartitionKey") ? props["PartitionKey"].StringValue : null;
            List<string> entities = new List<string>();
            TableContinuationToken continuationToken = null;
            do
            {
                TableQuerySegment<string> tableQueryResult = await table.ExecuteQuerySegmentedAsync(tableQuery, resolver, continuationToken);
                continuationToken = tableQueryResult.ContinuationToken;
                entities.AddRange(tableQueryResult.Results);
            } while (continuationToken != null);
            return entities.Count;
        }

        private async Task<int?> CountTableRows(CloudTable table, List<string> partitionKeys)
        {
            int? total = 0;
            foreach (string key in partitionKeys)
            {
                total += await GetCountOfEntitiesInPartition(table, key);
            }
            return total;
        }

        // generic method to count queue messages
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

        public async Task<ActionResult> StartCrawler()
        {
            return View(await CommandCrawler("start"));
        }

        public async Task<ActionResult> StopCrawler()
        {
            return View(await CommandCrawler("stop"));
        }

        private async Task<bool?> CommandCrawler(string command)
        {
            CloudQueueMessage message = new CloudQueueMessage(command);
            bool response;
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

        [HttpPost]
        public async Task<ActionResult> ClearUrlQueue()
        {
            bool response;
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

        [HttpPost]
        public async Task<ActionResult> ClearIndexedUrls()
        {
            bool response;
            try
            {
                await websitePageTable.DeleteIfExistsAsync();
                await websitePageTable.CreateIfNotExistsAsync();
                response = true;
            }
            catch (Exception)
            {
                response = false;
            }
            return View(response);
        }

        [HttpPost]
        public async Task<ActionResult> ClearAll()
        {
            bool response;
            try
            {
                // set response true if successful
                await ClearUrlQueue();
                await ClearIndexedUrls();
                response = true;
            }
            catch (Exception)
            {
                // false if something went wrong
                response = false;
            }

            return View(response);
        }

        [HttpGet]
        public ActionResult WorkerStatus()
        {
            var workerList = roleStatusTable.ExecuteQuery(new TableQuery<RoleStatus>());
            return View(workerList);
        }

        [HttpGet]
        [Route("Dashboard/ErrorList/")]
        [Route("Dashboard/Errors/{pageNumber:regex(^[1-9]{0,3}$)}")]
        public ActionResult ErrorList(int? pageNumber)
        {
            // count, results per page
            int pageSize = 10; // items per pages
            if (!pageNumber.HasValue)
            {
                pageNumber = 1;
            }
            TableQuery<WebsitePage> rangeQuery = new TableQuery<WebsitePage>()
                .Where(TableQuery.GenerateFilterCondition("ErrorTag", QueryComparisons.NotEqual, string.Empty))
                .Select(new List<string> { "Timestamp", "Title", "PartitionKey", "RowKey", "Domain", "SubDomain", "ErrorTag", "ErrorDetails", "Url" });
            var websitepages = websitePageTable.ExecuteQuery(rangeQuery);
            websitepages = websitepages.OrderByDescending(x => x.Timestamp);
            return PartialView(websitepages.ToPagedList((int)pageNumber, pageSize));
        }
    }
}