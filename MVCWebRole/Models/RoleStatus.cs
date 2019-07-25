using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace MVCWebRole.Models
{

    public class Memory
    {
        public Memory()
        {
            Available = 0;
            Use = 0;
        }

        public Int64 Available { get; set; }
        public Int64 Use { get; set; }
    }

    public class RoleStatus : TableEntity
    {
        public RoleStatus()
        {
            Initialize();
        }
        public string CurrentWorkingUrl { get; set; }

        public Int64 WorkingMem { get; set; }
        public Int64 TotalMem { get; set; }

        public Int64 CPU { get; set; }

        public string State { get; set; }
        public RoleStatus(string role, string processId)
        {
            this.PartitionKey = role;
            this.RowKey = processId;
            Initialize();
        }

        private void Initialize()
        {
            State = "N/A";
            TotalMem = 0L;
            WorkingMem = 0L;
            CPU = 0L;
        }
    }
}