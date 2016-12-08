﻿using System;
using System.Collections.Generic;
using System.IO;
using Raven.Client.Data;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Replication;
using Raven.Client.Data.Indexes;
using Raven.Client.Indexing;
using Raven.Client.Smuggler;
using Raven.Json.Linq;
using Raven.Server.Alerts;
using Raven.Server.Commercial;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes.Debugging;
using Raven.Server.Documents.Versioning;
using Raven.Server.Documents.SqlReplication;
using Raven.Server.Documents.PeriodicExport;
using Sparrow.Json;
using TypeScripter;
using TypeScripter.TypeScript;
using Voron.Data.BTrees;
using Voron.Debugging;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

namespace TypingsGenerator
{
    public class Program
    {

        public const string TargetDirectory = "../../src/Raven.Studio/typings/server/";
        public static void Main(string[] args)
        {
            Directory.CreateDirectory(TargetDirectory);

            var scripter = new Scripter()
                .UsingFormatter(new TsFormatter
                {
                    EnumsAsString = true
                });

            scripter
                .WithTypeMapping(TsPrimitive.String, typeof(Guid))
                .WithTypeMapping(TsPrimitive.String, typeof(TimeSpan))
                .WithTypeMapping(new TsInterface(new TsName("Array")), typeof(HashSet<>))
                .WithTypeMapping(new TsInterface(new TsName("Array")), typeof(List<>))
                .WithTypeMapping(TsPrimitive.Any, typeof(RavenJObject))
                .WithTypeMapping(TsPrimitive.Any, typeof(RavenJValue))
                .WithTypeMapping(TsPrimitive.Any, typeof(TreePage))
                .WithTypeMapping(TsPrimitive.String, typeof(DateTime))
                .WithTypeMapping(new TsArray(TsPrimitive.Any, 1), typeof(RavenJArray))
                .WithTypeMapping(TsPrimitive.Any, typeof(RavenJToken))
                .WithTypeMapping(TsPrimitive.Any, typeof(BlittableJsonReaderObject));

            scripter = ConfigureTypes(scripter);
            Directory.Delete(TargetDirectory, true);
            Directory.CreateDirectory(TargetDirectory);
            scripter
                .SaveToDirectory(TargetDirectory);
        }

        private static Scripter ConfigureTypes(Scripter scripter)
        {
            var ignoredTypes = new HashSet<Type>
            {
                typeof(IEquatable<>)
            };


            scripter.UsingTypeFilter(type => ignoredTypes.Contains(type) == false);
            scripter.UsingTypeReader(new TypeReaderWithIgnoreMethods());

            scripter.AddType(typeof(DatabaseDocument));
            scripter.AddType(typeof(DatabaseStatistics));
            scripter.AddType(typeof(IndexDefinition));

            // notifications
            scripter.AddType(typeof(OperationStatusChangeNotification));
            scripter.AddType(typeof(DeterminateProgress));
            scripter.AddType(typeof(IndeterminateProgress));
            scripter.AddType(typeof(OperationExceptionResult));
            scripter.AddType(typeof(DocumentChangeNotification));
            scripter.AddType(typeof(IndexChangeNotification));
            scripter.AddType(typeof(TransformerChangeNotification));
            scripter.AddType(typeof(DatabaseOperations.PendingOperation));

            // alerts
            scripter.AddType(typeof(Alert));
            scripter.AddType(typeof(GlobalAlertNotification));
            scripter.AddType(typeof(AlertNotification));
            
            // indexes
            scripter.AddType(typeof(IndexStats));
            scripter.AddType(typeof(IndexingStatus));
            scripter.AddType(typeof(IndexPerformanceStats));
            scripter.AddType(typeof(IndexDefinition));


            // transformers
            scripter.AddType(typeof(TransformerDefinition));


            // patch
            scripter.AddType(typeof(PatchRequest));

            scripter.AddType(typeof(ResourcesInfo));


            // smuggler
            scripter.AddType(typeof(DatabaseSmugglerOptions));

            // versioning
            scripter.AddType(typeof(VersioningConfiguration));

            // replication 
            scripter.AddType(typeof(ReplicationDocument<>));

            // sql replication 
            scripter.AddType(typeof(SqlConnections));
            scripter.AddType(typeof(SqlReplicationConfiguration));
            scripter.AddType(typeof(SqlReplicationStatistics));
            scripter.AddType(typeof(SimulateSqlReplication));


            // periodic export
            scripter.AddType(typeof(PeriodicExportConfiguration));

            // storage report
            scripter.AddType(typeof(StorageReport));

            // map reduce visualizer
            scripter.AddType(typeof(ReduceTree));

            // license 
            scripter.AddType(typeof(License));
            scripter.AddType(typeof(UserRegistrationInfo));
            scripter.AddType(typeof(LicenseStatus));

            return scripter;
        }
    }
}
