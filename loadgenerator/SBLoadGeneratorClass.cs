using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
//using Microsoft.Azure.ServiceBus;
using Azure.Messaging.ServiceBus;

namespace LoadGeneratorDotnetCore
{
    class SBLoadGeneratorClass : LoadGenerateeClass
    {
        private string EntityPath;
        private ServiceBusClient SendClient;
        
        public SBLoadGeneratorClass(
            string connectionString, string entityPath) : base(connectionString)
        {
            var opt = new ServiceBusClientOptions();
            opt.RetryOptions.MaxRetries = 0;
            opt.RetryOptions.Mode = ServiceBusRetryMode.Fixed;
            this.SendClient = new Azure.Messaging.ServiceBus.ServiceBusClient(connectionString, opt);

            EntityPath = entityPath;
        }

        public override Task GenerateBatchAndSend(int batchSize, bool dryRun, CancellationToken cancellationToken, Func<byte[]> loadGenerator)
        {
            var batchOfMessages = new List<ServiceBusMessage>();
            for (int i = 0; i < batchSize && !cancellationToken.IsCancellationRequested; i++)
            {
                batchOfMessages.Add(new ServiceBusMessage(loadGenerator()));
            }
            if (cancellationToken.IsCancellationRequested)
            {
                var tcs = new TaskCompletionSource<int>();
                tcs.TrySetCanceled();
                return tcs.Task;
            }
            if (!dryRun)
            {
                var sender = this.SendClient.CreateSender(EntityPath);
                var t = sender.SendMessagesAsync(batchOfMessages);
                t.ContinueWith((x) =>
                {
                    if (x.Exception != null)
                    {
                        Console.WriteLine(x.Exception);
                    }
                    sender.CloseAsync().Wait();
                });
                return t;
            }
            else
            {
                return Task.CompletedTask;
            }
        }
    }
}