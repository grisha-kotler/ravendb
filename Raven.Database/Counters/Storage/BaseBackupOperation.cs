using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Server.Abstractions;
using Voron;

namespace Raven.Database.Counters.Storage
{
	public abstract class BaseBackupOperation<TResourceStore, TDocument>
		where TResourceStore: IResourceStore
		where TDocument : IResourceDocument
	{
		protected static readonly ILog Log = LogManager.GetCurrentClassLogger();
		protected readonly TResourceStore ResourceStore;
		protected readonly TDocument Document;
		protected readonly StorageEnvironment StorageEnvironment;
		protected readonly string BackupSourceDirectory;
		protected readonly string BackupDestinationDirectory;
		protected readonly bool IncrementalBackup;


		protected BaseBackupOperation(TResourceStore resourceStore, TDocument document, StorageEnvironment storageEnvironment,
			string backupSourceDirectory, string backupDestinationDirectory, bool incrementalBackup)
		{
			ResourceStore = resourceStore;
			Document = document;
			StorageEnvironment = storageEnvironment;
			BackupSourceDirectory = backupSourceDirectory;
			BackupDestinationDirectory = backupDestinationDirectory;
			IncrementalBackup = incrementalBackup;
		}

		protected abstract bool BackupAlreadyExists { get; }
		internal abstract void PrepareForIncrementalBackup();
		internal abstract BackupStatus GetBackupStatus();
		internal abstract void CompleteBackup();
		internal abstract void SetBackupStatus(BackupStatus backupStatus);
		internal abstract void DeleteBackupStatus();
	}
}