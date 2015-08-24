﻿using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Logging;
using Raven.Client.Connection;
using Raven.Client.Connection.Request;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

using Raven.Abstractions;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;

namespace Raven.Client.FileSystem.Connection
{
    /// <summary>
    /// Replication and failover management on the client side
    /// </summary>
    public class FilesReplicationInformer : ReplicationInformerBase<IAsyncFilesCommands>, IFilesReplicationInformer
    {
		private readonly object replicationLock = new object();

		private bool firstTime = true;

		private DateTime lastReplicationUpdate = DateTime.MinValue;

		private Task refreshReplicationInformationTask;

        public FilesReplicationInformer(FilesConvention conventions, HttpJsonRequestFactory requestFactory)
            : base(conventions, requestFactory)
        {
        }

	    public Task UpdateReplicationInformationIfNeeded(IAsyncFilesCommands commands)
		{
			return UpdateReplicationInformationIfNeededInternal(commands);
		}

		private Task UpdateReplicationInformationIfNeededInternal(IAsyncFilesCommands commands)
		{
			if (Conventions.FailoverBehavior == FailoverBehavior.FailImmediately)
				return new CompletedTask();

			if (lastReplicationUpdate.AddMinutes(5) > SystemTime.UtcNow)
				return new CompletedTask();

			var serverClient = (IAsyncFilesCommandsImpl)commands;
			lock (replicationLock)
			{
				if (firstTime)
				{
					var serverHash = ServerHash.GetServerHash(serverClient.ServerUrl);
					var document = ReplicationInformerLocalCache.TryLoadReplicationInformationFromLocalCache(serverHash);
					if (IsInvalidDestinationsDocument(document) == false)
					{
						UpdateReplicationInformationFromDocument(document);
					}
				}

				firstTime = false;

				if (lastReplicationUpdate.AddMinutes(5) > SystemTime.UtcNow)
					return new CompletedTask();

				var taskCopy = refreshReplicationInformationTask;
				if (taskCopy != null)
					return taskCopy;

				return refreshReplicationInformationTask = Task.Factory.StartNew(() => RefreshReplicationInformation(commands))
					.ContinueWith(task =>
					{
						if (task.Exception != null)
						{
							Log.ErrorException("Failed to refresh replication information", task.Exception);
						}
						refreshReplicationInformationTask = null;
					});
			}
		}

        /// <summary>
        /// Refreshes the replication information.
        /// Expert use only.
        /// </summary>
        public override void RefreshReplicationInformation(IAsyncFilesCommands commands)
        {            
            lock (this)
            {
                var serverClient = (IAsyncFilesCommandsImpl)commands;
				var urlForFilename = serverClient.UrlFor();
                var serverHash = ServerHash.GetServerHash(urlForFilename);
                JsonDocument document = null;

                try
                {
                    var config = serverClient.Configuration.GetKeyAsync<RavenJObject>(SynchronizationConstants.RavenSynchronizationDestinations).Result;
                    FailureCounters.FailureCounts[urlForFilename] = new FailureCounter(); // we just hit the master, so we can reset its failure count

					if (config != null)
					{
						var destinationsArray = config.Value<RavenJArray>("Destinations");
						if (destinationsArray != null)
							document = new JsonDocument { DataAsJson = new RavenJObject() { { "Destinations", destinationsArray } } };
					}
				}
				catch (Exception e)
				{
					Log.ErrorException("Could not contact master for new replication information", e);
					document = ReplicationInformerLocalCache.TryLoadReplicationInformationFromLocalCache(serverHash);
				}

                if (document == null)
                    return;

                ReplicationInformerLocalCache.TrySavingReplicationInformationToLocalCache(serverHash, document);

                UpdateReplicationInformationFromDocument(document);
            }
        }

	    public override void ClearReplicationInformationLocalCache(IAsyncFilesCommands client)
	    {
			var serverClient = (IAsyncFilesCommandsImpl)client;
			var urlForFilename = serverClient.UrlFor();
			var serverHash = ServerHash.GetServerHash(urlForFilename);
			ReplicationInformerLocalCache.ClearReplicationInformationFromLocalCache(serverHash);
	    }


		public override void Dispose()
		{
			base.Dispose();

			var replicationInformationTaskCopy = refreshReplicationInformationTask;
			if (replicationInformationTaskCopy != null)
				replicationInformationTaskCopy.Wait();
		}

	    protected override void UpdateReplicationInformationFromDocument(JsonDocument document)
        {
            var destinations = document.DataAsJson.Value<RavenJArray>("Destinations").Select(x => JsonConvert.DeserializeObject<SynchronizationDestination>(x.ToString()));
            ReplicationDestinations = destinations.Select(x =>
            {
                ICredentials credentials = null;
                if (string.IsNullOrEmpty(x.Username) == false)
                {
                    credentials = string.IsNullOrEmpty(x.Domain)
                                      ? new NetworkCredential(x.Username, x.Password)
                                      : new NetworkCredential(x.Username, x.Password, x.Domain);
                }

                return new OperationMetadata(x.Url, new OperationCredentials(x.ApiKey, credentials), null);
            })
                // filter out replication destination that don't have the url setup, we don't know how to reach them
                // so we might as well ignore them. Probably private replication destination (using connection string names only)
                .Where(x => x != null)
                .ToList();
            foreach (var replicationDestination in ReplicationDestinations)
            {
                FailureCounter value;
				if (FailureCounters.FailureCounts.TryGetValue(replicationDestination.Url, out value))
                    continue;
				FailureCounters.FailureCounts[replicationDestination.Url] = new FailureCounter();
            }

        }

        protected override string GetServerCheckUrl(string baseUrl)
        {
            return baseUrl + "/config?name=" + Uri.EscapeDataString(SynchronizationConstants.RavenSynchronizationDestinations) + "&check-server-reachable";
        }
    }
}
