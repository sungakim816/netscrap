using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Collector
{
    class Robots
    {
        public string SeedUrl { get; set; }

        public string RobotAgent { get; set; }

        public string ProxyServer { get; set; }

        private WebProxy proxy;
        private string content;
        private string lastError;
        private string domainRegex;
        private List<string> disallowedUrlsRegex;
        private List<string> disallowedUrlsRaw;
        private List<string> siteMaps;
        private List<string> allowedSites;

        public Robots(string url)
        {
            Initialize();
            SeedUrl = url;
        }

        public Robots()
        {
            Initialize();
        }

        private void Initialize()
        {
            RobotAgent = "Net Scrap";
            ProxyServer = string.Empty;
            domainRegex = string.Empty;
            disallowedUrlsRegex = new List<string>();
            disallowedUrlsRaw = new List<string>();
            siteMaps = new List<string>();
            allowedSites = new List<string>();
            content = string.Empty;
            lastError = string.Empty;
        }

        public List<string> GetDisallowedUrlRegex()
        {
            return disallowedUrlsRegex;
        }

        public List<string> GetDisallowedUrlRaw()
        {
            return disallowedUrlsRaw;
        }

        public List<string> GetSiteMaps()
        {
            return siteMaps;
        }

        public List<string> GetAllowedSites()
        {
            return allowedSites;
        }

        public string GetURLContent(string url)
        {
            // reset global variables
            content = string.Empty;
            lastError = string.Empty;
            WebClient client = new WebClient();

            // if we are going through a proxy server 
            if (!string.IsNullOrEmpty(ProxyServer))
            {
                if (proxy == null)
                {
                    proxy = new WebProxy(ProxyServer, true);
                    client.Proxy = proxy;
                }
            }

            client.Headers["User-Agent"] = RobotAgent;
            client.Encoding = Encoding.UTF8;
            try
            {
                content = client.DownloadString(url);
            }
            catch (Exception)
            {
                ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
                try
                {
                    content = client.DownloadString(url);
                }
                catch (Exception ex)
                {
                    lastError = "Error: " + ex.ToString();
                }
            }
            finally
            {
                client.Dispose();
            }

            if (!string.IsNullOrEmpty(lastError))
            {
                Console.WriteLine(lastError);
            }
            return content;
        }

        public bool IsURLAllowed(string url)
        {
            if (disallowedUrlsRegex.Count == 0 || disallowedUrlsRaw.Count == 0)
            {
                return true;
            }
            if (url.Contains("robots.txt"))
            {
                return false;
            }
            // check if the url contain the main domain
            Match result = Regex.Match(url, domainRegex);
            if (!result.Success)
            {
                return false;
            }
            // check for disallowed urls
            foreach (string blockedUrlRegex in disallowedUrlsRegex)
            {
                string pattern = blockedUrlRegex;
                result = Regex.Match(url, pattern);
                if (result.Success)
                {
                    // found a Disallow command
                    return false;
                }
            }

            foreach (string blockedurlRaw in disallowedUrlsRaw)
            {
                if (url.Contains(blockedurlRaw))
                {
                    return false;
                }
            }
            return true;
        }

        // parse robots.txt and change change seed url if specified using the second argument;
        public void ParseRobotsTxtFile(string url, bool changeSeedUrl)
        {
            if (changeSeedUrl)
            {
                SeedUrl = url;
            }
            ParseRobotsTxtFile(url);
        }

        public void ParseRobotsTxtFile()
        {
            ParseRobotsTxtFile(SeedUrl);
        }

        // generate url regex
        private string GenerateUrlRegex(string raw, string domainAuthority)
        {
            // remove * and $ at the end of the string
            raw = raw.Trim('*');
            raw = raw.TrimEnd('$');
            // replace * found within (in the middle) the string with (.*)?
            // create a char array from the raw string
            char[] rawArray = raw.ToCharArray();
            string regex = string.Empty;
            foreach (char c in rawArray)
            {
                if (c == '*')
                {
                    regex += @"(.*)?";
                }
                else
                {
                    regex += c;
                }
            }
            if (regex.EndsWith("/"))
            {
                regex = raw.Substring(0, raw.Length - 1);
                regex += @"\/";
            }
            string finalPattern = @"^(?:http(s)?\/\/)?(?:www.)?(.*)?(" + @domainAuthority + @"\" + regex + @")+(.*)$";
            return finalPattern;
        }

        public void ParseRobotsTxtFile(string url)
        {
            siteMaps.Clear();
            disallowedUrlsRaw.Clear();
            disallowedUrlsRegex.Clear();
            if (string.IsNullOrEmpty(url) || string.IsNullOrWhiteSpace(url))
            {
                return;
            }
            Uri currentUrl;
            try
            {
                currentUrl = new Uri(url);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return;
            }
            string RobotsTxtFile = "https://" + currentUrl.Authority + "/robots.txt";
            string fileContents = GetURLContent(RobotsTxtFile);
            if (string.IsNullOrEmpty(fileContents))
            {
                return;
            }
            domainRegex = @"^(?:http(s)?\/\/)?(?:www.)?(.*)?(" + @currentUrl.Authority + @")+(.*)$";
            string[] fileLines = fileContents.Split(Environment.NewLine.ToCharArray(), StringSplitOptions.RemoveEmptyEntries);
            bool ApplyToBot = false;
            foreach (string line in fileLines)
            {
                RobotCommand CommandLine = new RobotCommand(line);
                switch (CommandLine.Command)
                {
                    case "COMMENT":   //then comment - ignore
                        break;
                    case "user-agent":   // User-Agent
                        if ((CommandLine.UserAgent.IndexOf("*") >= 0) || (CommandLine.UserAgent.IndexOf(RobotAgent) >= 0))
                        {
                            // these rules apply to our useragent so on next line if its a DISALLOW we will add the URL
                            // to our array of URls we cannot access
                            ApplyToBot = true;
                        }
                        else
                        {
                            ApplyToBot = false;
                        }
                        break;
                    case "disallow":   // Disallow
                        if (ApplyToBot)
                        {
                            // Only add to blocked if the URL is not blank as 
                            // Disallow: 
                            // means to allow access to all URLs
                            if (CommandLine.Url.Length > 0)
                            {

                                string raw = CommandLine.Url;
                                // add to raw
                                disallowedUrlsRaw.Add(raw);
                                // generate a regex from raw 
                                string regexPattern = GenerateUrlRegex(raw, currentUrl.Authority);
                                disallowedUrlsRegex.Add(regexPattern);
                            }
                            else
                            {
                                Console.WriteLine("ALLOW ALL URLS - BLANK");
                            }
                        }
                        else
                        {
                            Console.WriteLine("DISALLOW " + CommandLine.Url + " for another user-agent");
                        }
                        break;
                    case "allow":   // Allow - only used by Google
                        allowedSites.Add(CommandLine.Url);
                        break;
                    case "sitemap": // Sitemap - points to Google sitemap
                        siteMaps.Add(CommandLine.Url);
                        break;
                    default:
                        // empty/unknown/error
                        Console.WriteLine("# Unrecognised robots.txt entry [" + line + "]");
                        break;
                }
            }
        }

    }

    /*
 * This class will take a line from a Robots.txt
 * and parse it to return a command either
 * COMMENT - commented out line
 * USER-AGENT - the name of the useragent to apply the rule to
 * DISALLOW - command to disallow an agent to a Uri
 * ALLOW - Only used by Google - reverse of DISALLOW
 * and also a URL
 */

    class RobotCommand
    {

        /*
         * Will convert a robots.txt line [command]:[rule] into either
         * command = user-agent AND useragent = Googlebot
         * OR
         * command = DISALLOW AND URL = /search
         */
        public string Command { get; }
        public string Url { get; } = string.Empty;
        public string UserAgent { get; } = string.Empty;
        public RobotCommand(string commandline)
        {
            int PosOfComment = commandline.IndexOf('#');
            if (PosOfComment == 0)
            {
                // whole line is a comment
                Command = "COMMENT";
            }
            else
            {
                // there is a comment on the line so remove it
                if (PosOfComment >= 0)
                {
                    commandline = commandline.Substring(0, PosOfComment);
                }
                // now if we have an instruction
                if (commandline.Length > 0)
                {
                    /* 
                     * split our line on : e.g turn User-agent: GoogleBot
                     * into _Command = User-agent and _Url = GoogleBot                         
                     */
                    string[] lineArray = commandline.Split(':');
                    Command = lineArray[0].Trim().ToLower();
                    if (lineArray.Length > 1)
                    {
                        // set appropriate property depending on command type
                        if (Command == "user-agent")
                        {
                            UserAgent = lineArray[1].Trim();
                        }
                        else
                        {
                            Url = lineArray[1].Trim();
                            // if the URL is a full URL e.g sitemaps then it will contain
                            // a : so add to URL
                            if (lineArray.Length > 2)
                            {
                                Url += ":" + lineArray[2].Trim();
                            }
                        }
                    }
                }
            }
        }
    }
}
