using System;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Data;
using Raven.Database.Config;
using Voron;

namespace Raven.Database.Counters.Storage
{
    public interface IMaintananceActions : IDisposable
    {
        Guid Id { get; }

		void StartBackupOperation(CounterStorageDocument counterStorageDocument, string backupSourceDirectory, string backupDestinationDirectory, bool incrementalBackup);

        void Restore(FilesystemRestoreRequest restoreRequest, Action<string> output);

        void Compact(InMemoryRavenConfiguration configuration, Action<string> output);
    }
}