using System;
using Raven.Abstractions;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Data;
using Raven.Database.Config;
using Voron;

namespace Raven.Database.Counters.Storage
{
	public class MaintananceActions : IMaintananceActions
	{
		private readonly CounterStorage counterStorage;

		public MaintananceActions(CounterStorage counterStorage)
		{
			this.counterStorage = counterStorage;
		}

		public void StartBackupOperation(CounterStorageDocument counterStorageDocument, string backupSourceDirectory, string backupDestinationDirectory, bool incrementalBackup)
		{
			var backupOperation = new BackupOperation(counterStorage, counterStorageDocument, counterStorage.Environment, backupSourceDirectory, backupDestinationDirectory, incrementalBackup);
			backupOperation.SetBackupStatus(new BackupStatus
			{
				Started = SystemTime.UtcNow,
				IsRunning = true,
			});
			backupOperation.Execute();
		}

		public void Restore(FilesystemRestoreRequest restoreRequest, Action<string> output)
		{
			throw new NotImplementedException();
		}

		public void Compact(InMemoryRavenConfiguration configuration, Action<string> output)
		{
			throw new NotImplementedException();
		}


		public void Dispose()
		{
			throw new NotImplementedException();
		}

		public Guid Id { get; private set; }
    }
}