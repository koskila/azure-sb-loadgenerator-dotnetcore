using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;

namespace LoadGeneratorDotnetCore
{
    class EHLoadGeneratorClass : LoadGenerateeClass
    {
        private string EntityPath;
        private Azure.Messaging.EventHubs.Producer.EventHubProducerClient SendClient;

        public EHLoadGeneratorClass(
            string connectionString, string entityPath) : base(connectionString)
        {
            this.EntityPath = entityPath;

            // Successfully parsed the supplied connection string but need to ensure that for Event Hubs either
            // ...;EntityPath=... exists either in the conn string or in executionOptions
            if (connectionString.IndexOf(";EntityPath=") < 0)
            {
                throw new Exception("Please specify event hub name");
            }

            SendClient = new Azure.Messaging.EventHubs.Producer.EventHubProducerClient(connectionString);
        }
        public override Task GenerateBatchAndSend(int batchSize, bool dryRun, CancellationToken cancellationToken, Func<byte[]> loadGenerator)
        {
            List<EventData> batchOfMessages = new List<EventData>();
            for (int i = 0; i < batchSize && !cancellationToken.IsCancellationRequested; i++)
            {
                batchOfMessages.Add(new EventData(loadGenerator()));
            }
            if (cancellationToken.IsCancellationRequested)
            {
                var tcs = new TaskCompletionSource<int>();
                tcs.TrySetCanceled();
                return tcs.Task;
            }
            if (!dryRun)
            {
                return this.SendClient.SendAsync(batchOfMessages);
            }
            else
            {
                return Task.CompletedTask;
            }
        }
    }
}