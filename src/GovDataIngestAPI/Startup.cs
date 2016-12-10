using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.IO;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.ComponentModel;
using System.Collections;
using Newtonsoft.Json;
using System.Threading;
using System.Net.Http;
using System.Text;
using Newtonsoft.Json.Linq;
using System.Net.Http.Headers;

namespace GovDataIngestAPI
{
    public class Startup
    {
        FileSystemWatcher fsWatcher = new FileSystemWatcher(@"C:\Users\mihsar\Desktop\gov", "*.csv");

        private readonly int maxRowsFromCsv = 80;

        private double waterMark = 0;

        public static string jsonData = string.Empty;

        private object syncObj = new object();

        private readonly int updateMinutes = 5;
        private readonly int updateInMilliseconds;

        public Startup(IHostingEnvironment env)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(env.ContentRootPath)
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                .AddJsonFile($"appsettings.{env.EnvironmentName}.json", optional: true)
                .AddEnvironmentVariables();
            Configuration = builder.Build();

            updateInMilliseconds = updateMinutes * 1000 * 60;

            fsWatcher.NotifyFilter = NotifyFilters.LastWrite;
            fsWatcher.Changed += new FileSystemEventHandler(FsWatcher_Changed);
            fsWatcher.EnableRaisingEvents = true;

            var checkUpdatesWorker = new BackgroundWorker();
            checkUpdatesWorker.DoWork += new DoWorkEventHandler(checkUpdatesWorker_DoWork);
            checkUpdatesWorker.RunWorkerAsync();
        }

        private void FsWatcher_Changed(object sender, FileSystemEventArgs e)
        {
            var bgWorker = new BackgroundWorker();
            bgWorker.DoWork += new DoWorkEventHandler(bgWorker_DoWork);
            bgWorker.RunWorkerAsync(e);
        }

        private void bgWorker_DoWork(object sender, DoWorkEventArgs e)
        {
            lock(syncObj)
            {
                var fsEvent = e.Argument as FileSystemEventArgs;

                if(fsEvent.ChangeType == WatcherChangeTypes.Changed)
                {
                    try
                    {
                        fsWatcher.EnableRaisingEvents = false;

                        bool fileBusy = true;
                        var list = new List<string>();

                        while(fileBusy)
                        {
                            try
                            {
                                list = File.ReadAllLines(fsEvent.FullPath).ToList();

                                fileBusy = false;
                            }
                            catch(IOException)
                            {
                                // file is busy, retry
                            }

                            Thread.Sleep(500);
                        }

                        var csv = list.Select(l => l.Split(',')).ToList();

                        var headers = csv.FirstOrDefault();
                        var dicts = csv.Skip(1)
                            .Take(maxRowsFromCsv)
                            .Select(row => Enumerable.Zip(headers, row, Tuple.Create)
                            .ToDictionary(p => p.Item1, p => p.Item2))
                            .ToArray();

                        jsonData = JsonConvert.SerializeObject(dicts);

                        Task task = UploadNewData(jsonData);
                        task.Start();
                        task.Wait();
                    }
                    catch(Exception ex)
                    {
                        System.Diagnostics.Debug.WriteLine(ex.Message);
                    }
                    finally
                    {
                        fsWatcher.EnableRaisingEvents = true;
                    }
                }
            }
        }

        private void checkUpdatesWorker_DoWork(object sender, DoWorkEventArgs e)
        {
            Thread.Sleep(updateInMilliseconds);

            var task = new Task(GetNewData);
            task.Start();
            task.Wait();
        }

        public async Task<string> UploadNewData(string json)
        {
            using(var client = new HttpClient())
            {
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Token", "15879911b32d0ab138cfdbcd65123af5dfc0eb4c");

                var content = new StringContent(json, Encoding.UTF8, "application/json");
                var response = await client.PostAsync("http://52.59.217.156:8000/entries_bulk/", content);

                System.Diagnostics.Debug.WriteLine(response.Content.ReadAsStringAsync().ToString());

                return response.Content.ReadAsStringAsync().ToString();
            }
        }

        public async void GetNewData()
        {
            using(var client = new HttpClient())
            {
                string json = await client.GetStringAsync(string.Format("http://52.59.217.156:8000/entries/?limit=500&offset={0}", waterMark));

                var jObj = JObject.Parse(json);

                lock(syncObj)
                {
                    jsonData = jObj["results"].ToString();
                }
            }
        }

        public IConfigurationRoot Configuration { get; }

        public void ConfigureServices(IServiceCollection services)
        {
            services.AddMvc();
        }

        public void Configure(IApplicationBuilder app, IHostingEnvironment env, ILoggerFactory loggerFactory)
        {
            loggerFactory.AddConsole(Configuration.GetSection("Logging"));
            loggerFactory.AddDebug();

            app.UseMvc();
        }

    }
}
