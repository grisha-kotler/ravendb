import viewModelBase = require("viewmodels/viewModelBase");
import replicationsSetup = require("models/database/replication/replicationsSetup");
import replicationConfig = require("models/database/replication/replicationConfig")
import replicationDestination = require("models/database/replication/replicationDestination");
import collection = require("models/database/documents/collection");
import getDatabaseStatsCommand = require("commands/resources/getDatabaseStatsCommand");
import getReplicationsCommand = require("commands/database/replication/getReplicationsCommand");
import updateServerPrefixHiLoCommand = require("commands/database/documents/updateServerPrefixHiLoCommand");
import saveReplicationDocumentCommand = require("commands/database/replication/saveReplicationDocumentCommand");
import saveAutomaticConflictResolutionDocument = require("commands/database/replication/saveAutomaticConflictResolutionDocument");
import getServerPrefixForHiLoCommand = require("commands/database/documents/getServerPrefixForHiLoCommand");
import replicateAllIndexesCommand = require("commands/database/replication/replicateAllIndexesCommand");
import replicateAllTransformersCommand = require("commands/database/replication/replicateAllTransformersCommand");
import deleteLocalReplicationsSetupCommand = require("commands/database/globalConfig/deleteLocalReplicationsSetupCommand");
import replicateIndexesCommand = require("commands/database/replication/replicateIndexesCommand");
import replicateTransformersCommand = require("commands/database/replication/replicateTransformersCommand");
import getEffectiveConflictResolutionCommand = require("commands/database/globalConfig/getEffectiveConflictResolutionCommand");
import getCollectionsCommand = require("commands/database/documents/getCollectionsCommand");
import appUrl = require("common/appUrl");
import database = require("models/resources/database");
import enableReplicationCommand = require("commands/database/replication/enableReplicationCommand");

class replications extends viewModelBase {

    replicationEnabled = ko.observable<boolean>(false);

    prefixForHilo = ko.observable<string>("");
    replicationConfig = ko.observable<replicationConfig>(new replicationConfig({ DocumentConflictResolution: "None", AttachmentConflictResolution: "None" }));
    replicationsSetup = ko.observable<replicationsSetup>(new replicationsSetup({ MergedDocument: { Destinations: [], Source: null } }));
    globalClientFailoverBehaviour = ko.observable<string>(null);
    globalReplicationConfig = ko.observable<replicationConfig>();
    collections = ko.observableArray<collection>();    

    serverPrefixForHiLoDirtyFlag = new ko.DirtyFlag([]);
    replicationConfigDirtyFlag = new ko.DirtyFlag([]);
    replicationsSetupDirtyFlag = new ko.DirtyFlag([]);

    isServerPrefixForHiLoSaveEnabled: KnockoutComputed<boolean>;
    isConfigSaveEnabled: KnockoutComputed<boolean>;
    isSetupSaveEnabled: KnockoutComputed<boolean>;
    isReplicateIndexesToAllEnabled: KnockoutComputed<boolean>;

    usingGlobal = ko.observable<boolean>(false);
    hasGlobalValues = ko.observable<boolean>(false);

    readFromAllAllowWriteToSecondaries = ko.computed(() => {
        var behaviour = this.replicationsSetup().clientFailoverBehaviour();
        if (behaviour == null) {
            return false;
        }
        var tokens = behaviour.split(",");
        return tokens.contains("ReadFromAllServers") && tokens.contains("AllowReadsFromSecondariesAndWritesToSecondaries");
    });

    globalReadFromAllAllowWriteToSecondaries = ko.computed(() => {
        var behaviour = this.globalClientFailoverBehaviour();
        if (behaviour == null) {
            return false;
        }
        var tokens = behaviour.split(",");
        return tokens.contains("ReadFromAllServers") && tokens.contains("AllowReadsFromSecondariesAndWritesToSecondaries");
    });

    canActivate(args: any): JQueryPromise<any> {
        var deferred = $.Deferred();
        var db = this.activeDatabase();
        if (db) {
            if (db.activeBundles.contains("Replication")) {
                this.replicationEnabled(true);
                $.when(this.fetchServerPrefixForHiLoCommand(db), this.fetchAutomaticConflictResolution(db), this.fetchReplications(db))
                    .done(() => deferred.resolve({ can: true }))
                    .fail(() => deferred.resolve({ redirect: appUrl.forSettings(db) }));
            } else {
                this.replicationEnabled(false);
                deferred.resolve({ can: true });
            }
            
        }
        return deferred;
    }

    activate(args) {
        super.activate(args);
        this.updateHelpLink("7K1KES");

        this.serverPrefixForHiLoDirtyFlag = new ko.DirtyFlag([this.prefixForHilo]);
        this.isServerPrefixForHiLoSaveEnabled = ko.computed(() => this.serverPrefixForHiLoDirtyFlag().isDirty());
        this.replicationConfigDirtyFlag = new ko.DirtyFlag([this.replicationConfig]);
        this.isConfigSaveEnabled = ko.computed(() => this.replicationConfigDirtyFlag().isDirty());

        var replicationSetupDirtyFlagItems = [this.replicationsSetup, this.replicationsSetup().destinations(), this.replicationConfig, this.replicationsSetup().clientFailoverBehaviour,this.usingGlobal];

        $.each(this.replicationsSetup().destinations(), (i, dest) =>
        {
            replicationSetupDirtyFlagItems.push(<any>dest.sourceCollections);
            dest.sourceCollections.subscribe(array => {
                if (array.length > 0)
                    dest.ignoredClient(true);
                else {
                    dest.ignoredClient(false);
                }
            });
        });
        this.replicationsSetupDirtyFlag = new ko.DirtyFlag(replicationSetupDirtyFlagItems);
        
        this.isSetupSaveEnabled = ko.computed(() => this.replicationsSetupDirtyFlag().isDirty());

        this.isReplicateIndexesToAllEnabled = ko.computed(() => this.replicationsSetup().destinations().length > 0);
        var combinedFlag = ko.computed(() => {
            var rc = this.replicationConfigDirtyFlag().isDirty();
            var rs = this.replicationsSetupDirtyFlag().isDirty();
            var sp = this.serverPrefixForHiLoDirtyFlag().isDirty();
            return rc || rs || sp;
        });
        this.dirtyFlag = new ko.DirtyFlag([combinedFlag, this.usingGlobal]);

        var db = this.activeDatabase();
        this.fetchCollections(db).done(results => {
            this.collections(results);
        });
    }

    private fetchServerPrefixForHiLoCommand(db): JQueryPromise<any> {
        var deferred = $.Deferred();
        new getServerPrefixForHiLoCommand(db)
            .execute()
            .done((result) => this.prefixForHilo(result))
            .always(() => deferred.resolve({ can: true }));
        return deferred;
    }

    fetchAutomaticConflictResolution(db): JQueryPromise<any> {
        var deferred = $.Deferred();
        new getEffectiveConflictResolutionCommand(db)
            .execute()
            .done((repConfig: configurationDocumentDto<replicationConfigDto>) => {
                this.replicationConfig(new replicationConfig(repConfig.MergedDocument));
                if (repConfig.GlobalDocument) {
                    this.globalReplicationConfig(new replicationConfig(repConfig.GlobalDocument));
                }
            })
            .always(() => deferred.resolve({ can: true }));
        return deferred;
    }

    fetchReplications(db): JQueryPromise<any> {
        var deferred = $.Deferred();
        new getReplicationsCommand(db)
            .execute()
            .done((repSetup: configurationDocumentDto<replicationsDto>) => {
                this.replicationsSetup(new replicationsSetup(repSetup));
                this.usingGlobal(repSetup.GlobalExists && !repSetup.LocalExists);
                this.hasGlobalValues(repSetup.GlobalExists);
                if (repSetup.GlobalDocument && repSetup.GlobalDocument.ClientConfiguration) {
                    this.globalClientFailoverBehaviour(repSetup.GlobalDocument.ClientConfiguration.FailoverBehavior);
                }
            })
            .always(() => deferred.resolve({ can: true }));
        return deferred;
    }

    public onReplicateToCollectionClick(destination: replicationDestination, collectionName: string) {
        if (destination.sourceCollections.indexOf(collectionName) < 0) {
            destination.sourceCollections.push(collectionName);
        } else {
            destination.sourceCollections.remove(collectionName);
        }
    }

    createNewDestination() {
        var db = this.activeDatabase();
        this.replicationsSetup().destinations.unshift(replicationDestination.empty(db.name));
    }

    removeDestination(repl: replicationDestination) {
        this.replicationsSetup().destinations.remove(repl);
    }

    saveChanges() {
        if (this.usingGlobal()) {
            new deleteLocalReplicationsSetupCommand(this.activeDatabase())
                .execute();
        } else {
        if (this.isConfigSaveEnabled())
            this.saveAutomaticConflictResolutionSettings();
        if (this.isSetupSaveEnabled()) {
            if (this.replicationsSetup().source()) {
                this.saveReplicationSetup();
            } else {
                var db = this.activeDatabase();
                if (db) {
                    new getDatabaseStatsCommand(db)
                        .execute()
                        .done(result=> {
                            this.prepareAndSaveReplicationSetup(result.DatabaseId);
                        });
                }
            }
        }
    }
    }

    private prepareAndSaveReplicationSetup(source: string) {
        this.replicationsSetup().source(source);
        this.saveReplicationSetup();
    }

    private saveReplicationSetup() {
        var db = this.activeDatabase();
        if (db) {
            new saveReplicationDocumentCommand(this.replicationsSetup().toDto(), db)
                .execute()
                .done(() => this.replicationsSetupDirtyFlag().reset());
        }
    }

    private fetchCollections(db: database): JQueryPromise<Array<collection>> {
        return new getCollectionsCommand(db, this.collections()).execute();
    }

    sendReplicateCommand(destination: replicationDestination,parentClass: replications) {        
        var db = parentClass.activeDatabase();
        if (db) {
            new replicateIndexesCommand(db, destination).execute();
            new replicateTransformersCommand(db, destination).execute();
        } else {
            alert("No database selected! This error should not be seen."); //precaution to ease debugging - in case something bad happens
    }
    }

    sendReplicateAllCommand() {
        var db = this.activeDatabase();
        if (db) {
            new replicateAllIndexesCommand(db).execute();
            new replicateAllTransformersCommand(db).execute();
        } else {
            alert("No database selected! This error should not be seen."); //precaution to ease debugging - in case something bad happens
        }

    }

    saveServerPrefixForHiLo() {
        var db = this.activeDatabase();
        if (db) {
            new updateServerPrefixHiLoCommand(this.prefixForHilo(), db)
                .execute()
                .done(() => {
                    this.serverPrefixForHiLoDirtyFlag().reset();
                    this.dirtyFlag().reset();
                });
        }
    }

    saveAutomaticConflictResolutionSettings() {
        var db = this.activeDatabase();
        if (db) {
            new saveAutomaticConflictResolutionDocument(this.replicationConfig().toDto(), db)
                .execute()
                .done(() => {
                    this.replicationConfigDirtyFlag().reset();
                    this.dirtyFlag().reset();
                });
        }
    }

    override(value: boolean, destination: replicationDestination) {
        destination.hasLocal(value);
        if (!destination.hasLocal()) {
            destination.copyFromGlobal();
        }
    }

    useLocal() {
        this.usingGlobal(false);
    }

    useGlobal() {
        this.usingGlobal(true);
        if (this.globalReplicationConfig()) {
            this.replicationConfig().attachmentConflictResolution(this.globalReplicationConfig().attachmentConflictResolution());
            this.replicationConfig().documentConflictResolution(this.globalReplicationConfig().documentConflictResolution());    
        }
        
        this.replicationsSetup().copyFromParent(this.globalClientFailoverBehaviour());
    }

    enableReplication() {
        new enableReplicationCommand(this.activeDatabase())
            .execute()
            .done((bundles) => {
                var db = this.activeDatabase();
                db.activeBundles(bundles);
                this.replicationEnabled(true);
                this.fetchServerPrefixForHiLoCommand(db);
                this.fetchAutomaticConflictResolution(db);
                this.fetchReplications(db);
                this.fetchCollections(db).done(results => {
                    this.collections(results);
                });
            });
    }
}

export = replications; 