using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Web;

namespace MVCWebRole.Models
{
    public class DomainObject : TableEntity
    {
        private readonly HashAlgorithm algorithm = SHA256.Create();
        public DomainObject()
        {

        }

        public DomainObject(string domain)
        {
            this.PartitionKey = domain;
            this.RowKey = Generate256HashCode(domain);
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