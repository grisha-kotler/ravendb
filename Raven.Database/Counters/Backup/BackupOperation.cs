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

namespace Raven.Database.Counters.Backup
{
	public class BackupOperation
	{
		private static readonly ILog Log = LogManager.GetCurrentClassLogger();
		private readonly CounterStorage storage;
		private readonly string backupDestinationDirectory;
		private readonly StorageEnvironment env;
		private readonly bool incrementalBackup;
		private readonly CounterStorageDocument counterStorageDocument;

		private readonly string backupFilename;
		private readonly string backupSourceDirectory;

		public BackupOperation(CounterStorage storage, string backupSourceDirectory, string backupDestinationDirectory, StorageEnvironment env, bool incrementalBackup, CounterStorageDocument counterStorageDocument)
		{
			this.storage = storage;
			this.backupDestinationDirectory = backupDestinationDirectory;
			this.env = env;
			this.incrementalBackup = incrementalBackup;
			this.counterStorageDocument = counterStorageDocument;
			this.backupSourceDirectory = backupSourceDirectory;
			backupFilename = counterStorageDocument.Id + ".Voron.Backup";

			if (incrementalBackup)
				PrepareForIncrementalBackup();
		}

		public void Execute()
		{
			try
			{
				Log.Info("Starting backup of '{0}' to '{1}'", backupSourceDirectory, backupDestinationDirectory);
				UpdateBackupStatus(
					string.Format("Started backup process. Backing up data to directory = '{0}'",
								  backupDestinationDirectory), null, BackupStatus.BackupMessageSeverity.Informational);

				UpdateBackupStatus("Executing data backup..", null, BackupStatus.BackupMessageSeverity.Informational);

				if (incrementalBackup)
				{
					var backupDestinationIncrementalDirectory = DirectoryForIncrementalBackup();
					EnsureBackupDestinationExists(backupDestinationIncrementalDirectory);

					BackupMethods.Incremental.ToFile(env, Path.Combine(backupDestinationIncrementalDirectory, backupFilename),
						infoNotify: s => UpdateBackupStatus(s, null, BackupStatus.BackupMessageSeverity.Informational));
				}
				else if (Directory.Exists(backupDestinationDirectory))
				{
					throw new InvalidOperationException("Denying request to perform a full backup to an existing backup folder! Try doing an incremental backup instead.");
				}
				else
				{
					EnsureBackupDestinationExists();
					BackupMethods.Full.ToFile(env, Path.Combine(backupDestinationDirectory, backupFilename),
						infoNotify: s => UpdateBackupStatus(s, null, BackupStatus.BackupMessageSeverity.Informational));
				}

				if (counterStorageDocument != null)
					File.WriteAllText(Path.Combine(backupDestinationDirectory, Constants.Counter.BackupDocumentFileName), RavenJObject.FromObject(counterStorageDocument).ToString());
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

		private void PrepareForIncrementalBackup()
		{
			if (Directory.Exists(backupDestinationDirectory) == false)
				Directory.CreateDirectory(backupDestinationDirectory);

			var incrementalBackupState = Path.Combine(backupDestinationDirectory, Constants.IncrementalBackupState);

			if (File.Exists(incrementalBackupState))
			{
				var state = RavenJObject.Parse(File.ReadAllText(incrementalBackupState)).JsonDeserialization<IncrementalBackupState>();

				if (state.ResourceId != storage.ServerId)
					throw new InvalidOperationException(string.Format("Can't perform an incremental backup to a given folder because it already contains incremental backup data of different database. Existing incremental data origins from '{0}' database.", state.ResourceName));
			}
			else
			{
				var state = new IncrementalBackupState()
				{
					ResourceId = storage.ServerId,
					ResourceName = counterStorageDocument.Id
				};

				File.WriteAllText(incrementalBackupState, RavenJObject.FromObject(state).ToString());
			}
		}

		private string DirectoryForIncrementalBackup()
		{
			while (true)
			{
				var incrementalTag = SystemTime.UtcNow.ToString("Inc yyyy-MM-dd HH-mm-ss");
				var backupDirectory = Path.Combine(backupDestinationDirectory, incrementalTag);

				if (Directory.Exists(backupDirectory) == false)
				{
					return backupDirectory;
				}
				Thread.Sleep(100); // wait until the second changes, should only even happen in tests
			}
		}

		private void CompleteBackup()
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

		private BackupStatus GetBackupStatus()
		{
			using (var reader = storage.CreateReader())
			{
				return reader.GetBackupStatus();
			}
		}

		private void SetBackupStatus(BackupStatus backupStatus)
		{
			using (var writer = storage.CreateWriter())
			{
				writer.SaveBackupStatus(backupStatus);
				writer.Commit();
			}
		}

		private void DeleteBackupStatus()
		{
			using (var writer = storage.CreateWriter())
			{
				writer.DeleteBackupStatus();
				writer.Commit();
			}
		}

		private void EnsureBackupDestinationExists(string backupDestination = null)
		{
			var path = backupDestination ?? backupDestinationDirectory;
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

		public bool BackupAlreadyExists
		{
			get { return Directory.Exists(backupDestinationDirectory) && File.Exists(Path.Combine(backupDestinationDirectory.Trim(), backupFilename)); }
		}
	}
}
