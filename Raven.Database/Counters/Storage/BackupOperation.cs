using System;
using System.IO;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Extensions;
using Raven.Database.Storage;
using Raven.Json.Linq;
using Voron;
using Voron.Impl.Backup;

namespace Raven.Database.Counters.Storage
{
	public class BackupOperation : BaseBackupOperation<CounterStorage, CounterStorageDocument>
	{
		private readonly string backupFilename;
		
		public BackupOperation(CounterStorage counterStorage, CounterStorageDocument counterStorageDocument, StorageEnvironment env,
			string backupSourceDirectory, string backupDestinationDirectory, bool incrementalBackup)
			: base(counterStorage, counterStorageDocument, env, backupSourceDirectory, backupDestinationDirectory, incrementalBackup)
		{
			backupFilename = Document.Id + ".Voron.Backup";

			if (incrementalBackup)
				PrepareForIncrementalBackup();
		}

		internal override sealed void PrepareForIncrementalBackup()
		{
			if (Directory.Exists(BackupDestinationDirectory) == false)
				Directory.CreateDirectory(BackupDestinationDirectory);

			var incrementalBackupState = Path.Combine(BackupDestinationDirectory, Constants.IncrementalBackupState);
			if (File.Exists(incrementalBackupState))
			{
				var state = RavenJObject.Parse(File.ReadAllText(incrementalBackupState)).JsonDeserialization<IncrementalBackupState>();

				if (state.ResourceId != ResourceStore.ServerId)
					throw new InvalidOperationException(string.Format("Can't perform an incremental backup to a given folder because it already contains incremental backup data of different counter storage. Existing incremental data origins from '{0}' counter storage.", state.ResourceName));
			}
			else
			{
				var state = new IncrementalBackupState
				{
					ResourceId = ResourceStore.ServerId,
					ResourceName = Document.Id
				};

				File.WriteAllText(incrementalBackupState, RavenJObject.FromObject(state).ToString());
			}
		}

		public void Execute()
		{
			try
			{
				Log.Info("Starting backup of '{0}' to '{1}'", BackupSourceDirectory, BackupDestinationDirectory);
				UpdateBackupStatus(
					string.Format("Started backup process. Backing up data to directory = '{0}'",
								  BackupDestinationDirectory), null, BackupStatus.BackupMessageSeverity.Informational);

				UpdateBackupStatus("Executing data backup..", null, BackupStatus.BackupMessageSeverity.Informational);

				if (IncrementalBackup)
				{
					var backupDestinationIncrementalDirectory = DirectoryForIncrementalBackup();
					EnsureBackupDestinationExists(backupDestinationIncrementalDirectory);

					BackupMethods.Incremental.ToFile(StorageEnvironment, Path.Combine(backupDestinationIncrementalDirectory, backupFilename),
						infoNotify: s => UpdateBackupStatus(s, null, BackupStatus.BackupMessageSeverity.Informational));
				}
				else if (BackupAlreadyExists)
				{
					throw new InvalidOperationException("Denying request to perform a full backup to an existing backup folder! Try doing an incremental backup instead.");
				}
				else
				{
					EnsureBackupDestinationExists();
					BackupMethods.Full.ToFile(StorageEnvironment, Path.Combine(BackupDestinationDirectory, backupFilename),
						infoNotify: s => UpdateBackupStatus(s, null, BackupStatus.BackupMessageSeverity.Informational));
				}

				if (Document != null)
					File.WriteAllText(Path.Combine(BackupDestinationDirectory, Constants.Counter.BackupDocumentFileName), RavenJObject.FromObject(Document).ToString());
			}
			catch (AggregateException e)
			{
				var ne = e.ExtractSingleInnerException();
				Log.ErrorException("Failed to complete backup", ne);
				UpdateBackupStatus("Failed to complete backup because: " + ne.Message, ne.ExceptionToString(null), BackupStatus.BackupMessageSeverity.Error);
			}
			catch (Exception e)
			{
				Log.ErrorException("Failed to complete backup", e);
				UpdateBackupStatus("Failed to complete backup because: " + e.Message, e.ExceptionToString(null), BackupStatus.BackupMessageSeverity.Error);
			}
			finally
			{
				CompleteBackup();
			}
		}

		private string DirectoryForIncrementalBackup()
		{
			while (true)
			{
				var incrementalTag = SystemTime.UtcNow.ToString("Inc yyyy-MM-dd HH-mm-ss");
				var backupDirectory = Path.Combine(BackupDestinationDirectory, incrementalTag);

				if (Directory.Exists(backupDirectory) == false)
				{
					return backupDirectory;
				}
				Thread.Sleep(100); // wait until the second changes, should only even happen in tests
			}
		}

		internal override void CompleteBackup()
		{
			try
			{
				Log.Info("Backup completed");
				var backupStatus = GetBackupStatus();
				if (backupStatus == null)
					return;

				backupStatus.IsRunning = false;
				backupStatus.Completed = SystemTime.UtcNow;
				SetBackupStatus(backupStatus);
			}
			catch (Exception e)
			{
				Log.WarnException("Failed to update completed backup status, will try deleting backup status", e);
				try
				{
					DeleteBackupStatus();
				}
				catch (Exception ex)
				{
					Log.WarnException("Failed to remove backup status", ex);
				}
			}
		}

		private void UpdateBackupStatus(string newMsg, string details, BackupStatus.BackupMessageSeverity severity)
		{
			try
			{
				Log.Info(newMsg);
				var backupStatus = GetBackupStatus();
				if (backupStatus == null)
					return;

				backupStatus.Messages.Add(new BackupStatus.BackupMessage
				{
					Message = newMsg,
					Timestamp = SystemTime.UtcNow,
					Severity = severity,
					Details = details
				});
				SetBackupStatus(backupStatus);
			}
			catch (Exception e)
			{
				Log.WarnException("Failed to update backup status", e);
			}
		}

		internal override BackupStatus GetBackupStatus()
		{
			using (var reader = ResourceStore.CreateReader())
			{
				return reader.GetBackupStatus();
			}
		}

		internal override void SetBackupStatus(BackupStatus backupStatus)
		{
			using (var writer = ResourceStore.CreateWriter())
			{
				writer.SaveBackupStatus(backupStatus);
				writer.Commit();
			}
		}

		internal override void DeleteBackupStatus()
		{
			using (var writer = ResourceStore.CreateWriter())
			{
				writer.DeleteBackupStatus();
				writer.Commit();
			}
		}

		private void EnsureBackupDestinationExists(string backupDestination = null)
		{
			var path = backupDestination ?? BackupDestinationDirectory;
			if (Directory.Exists(path))
			{
				var writeTestFile = Path.Combine(path, "write-permission-test");
				try
				{
					File.Create(writeTestFile).Dispose();
				}
				catch (UnauthorizedAccessException)
				{
					throw new UnauthorizedAccessException(string.Format("You don't have write access to the path {0}", path));
				}
				IOExtensions.DeleteFile(writeTestFile);
			}
			else
				Directory.CreateDirectory(path); // will throw UnauthorizedAccessException if a user doesn't have write permission
		}

		protected override bool BackupAlreadyExists
		{
			get { return Directory.Exists(BackupDestinationDirectory) && File.Exists(Path.Combine(BackupDestinationDirectory.Trim(), backupFilename)); }
		}
	}
}
