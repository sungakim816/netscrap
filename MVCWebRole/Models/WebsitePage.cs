using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.ComponentModel.DataAnnotations;
using System.Security.Cryptography;
using System.Text;
using Nager.PublicSuffix;
using System.Web;

namespace MVCWebRole.Models
{
    public class WebsitePage : TableEntity
    {
        private readonly HashAlgorithm algorithm = SHA256.Create();
        private readonly DomainParser domainParser = new DomainParser(new WebTldRuleProvider());
        private string url;

        [Required]
        public string Title { get; set; }

        public string Content { get; set; }

        public string Domain { get; set; }

        public string SubDomain { get; set; }

        [Required]
        public string Url
        {
            get
            {
                return url;
            }
            set
            {
                url = value;
                Uri uri = new Uri(url);
                Domain = domainParser.Get(uri.Authority).Domain;
                SubDomain = domainParser.Get(uri.Authority).SubDomain;
            }
        }

        [Timestamp]
        public DateTime? PublishDate { get; set; }

        public WebsitePage()
        {
            this.PartitionKey = "website";
            this.RowKey = Guid.NewGuid().ToString();
            this.Domain = string.Empty;
            this.SubDomain = string.Empty;
            this.ErrorDetails = string.Empty;
            this.ErrorTag = string.Empty;
            this.Url = string.Empty;
            this.PublishDate = null;
            this.Title = "Title";
            this.Content = "Content Preview Not Available";
        }

        public string ErrorTag { get; set; }

        public string ErrorDetails { get; set; }

        public WebsitePage(string partitionKey, string rowKey)
        {
            this.PartitionKey = HttpUtility.UrlEncode(partitionKey);
            Initialize(rowKey);
        }

        private void Initialize(string url)
        {
            this.RowKey = Generate256HashCode(url.Trim());
            this.Url = url;
            Uri uri = new Uri(url);
            this.Domain = domainParser.Get(uri.Authority).Domain;
            this.SubDomain = domainParser.Get(uri.Authority).SubDomain;
            this.ErrorTag = string.Empty;
            this.ErrorDetails = string.Empty;
            this.Title = "Title";
            this.Content = "Content Preview Not Available";
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