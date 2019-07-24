﻿using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Web;

namespace MVCWebRole.Models
{
    public class WebsitePage : TableEntity
    {
        private readonly HashAlgorithm algorithm = SHA256.Create();

        [Required]
        public string Title { get; set; }

        public string Content { get; set; }

        [Required]
        public string Url { get; set; }

        [Timestamp]
        public DateTime? PublishDate { get; set; }

        [Timestamp]
        public DateTime DateCrawled { get; set; }

        public WebsitePage() { }

        public string Error { get; set; }

        public WebsitePage(string url, string title, DateTime publishDate)
        {
            Initialize(url);
            this.Url = url;
            this.PublishDate = publishDate;
            this.Title = title;
        }

        public WebsitePage(string url, string title, string content)
        {
            Initialize(url);
            this.Url = url;
            this.PublishDate = null;
            this.Title = title;
            this.Content = content;
        }

        private void Initialize(string url)
        {
            string partitionKey = new Uri(url).Authority.ToString();
            if (partitionKey.StartsWith("www."))
            {
                partitionKey = partitionKey.Remove(0, 4);
            }
            this.PartitionKey = partitionKey;
            this.RowKey = Generate256HashCode(url.Trim());
            this.Error = string.Empty;
            this.DateCrawled = DateTime.Now.ToLocalTime();
        }

        private string Generate256HashCode(string s)
        {
            byte[] data = algorithm.ComputeHash(Encoding.UTF8.GetBytes(s.Trim()));
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