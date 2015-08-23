﻿using System;
using System.IO;
using System.Linq;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Logging;
using Raven.Database.Extensions;
using Voron;
using Voron.Impl.Backup;

namespace Raven.Database.Counters.Storage
{
	public class RestoreOperation
	{
		private static readonly ILog log = LogManager.GetCurrentClassLogger();
		private readonly Action<string> output;
		private readonly string backupPath;
		private readonly string restoreToPath;
		private readonly string backupFilename;

		public RestoreOperation(CounterRestoreRequest restoreRequest, Action<string> output)
		{
			backupPath = restoreRequest.BackupLocation;
			restoreToPath = restoreRequest.RestoreToLocation.ToFullPath();
			this.output = output;
			backupFilename = string.Format("{0}.Voron.Backup", restoreRequest.Id);
		}

		private bool IsValidBackup()
		{
			return File.Exists(GetBackupFilenamePath());
		}

		private string GetBackupFilenamePath()
		{
			var directories = Directory.GetDirectories(backupPath, "Inc*")
				.OrderByDescending(dir => dir)
				.ToList();

			var backupFilenamePath = Path.Combine(directories.Count == 0 ? backupPath : directories.First(), backupFilename);
			return backupFilenamePath;
		}

		private void ValidateRestorePreconditionsAndReturnLogsPath()
		{
			if (IsValidBackup() == false)
			{
				output("Error: " + backupPath + " doesn't look like a valid backup");
				output("Error: Restore Canceled");
				throw new InvalidOperationException(backupPath + " doesn't look like a valid backup");
			}

			if (Directory.Exists(restoreToPath) && Directory.GetFileSystemEntries(restoreToPath).Length > 0)
			{
				output("Error: Counter already exists, cannot restore to an existing counter.");
				output("Error: Restore Canceled");
				throw new IOException("Counter already exists, cannot restore to an existing counter.");
			}

			if (Directory.Exists(restoreToPath) == false)
				Directory.CreateDirectory(restoreToPath);
		}

		public void Execute()
		{
			ValidateRestorePreconditionsAndReturnLogsPath();

			try
			{
				var backupFilenamePath = GetBackupFilenamePath();

				if (Directory.GetDirectories(backupPath, "Inc*").Any() == false)
					BackupMethods.Full.Restore(backupFilenamePath, restoreToPath);
				else
				{
					using (var options = StorageEnvironmentOptions.ForPath(restoreToPath))
					{
						var backupPaths = Directory.GetDirectories(backupPath, "Inc*")
							.OrderBy(dir => dir)
							.Select(dir => Path.Combine(dir, backupFilename))
							.ToList();
						BackupMethods.Incremental.Restore(options, backupPaths);
					}
				}

			}
			catch (Exception e)
			{
				output("Restore Operation: Failure! Could not restore database!");
				output(e.ToString());
				log.WarnException("Could not complete restore", e);
				throw;
			}
		}
	}
}
