import viewModelBase = require("viewmodels/viewModelBase");
import shell = require("viewmodels/shell");
import database = require("models/resources/database");
import resource = require("models/resources/resource");
import fileSystem = require("models/filesystem/filesystem");
import counterStorage = require("models/counter/counterStorage");
import startDbCompactCommand = require("commands/maintenance/startCompactCommand");
import startFsCompactCommand = require("commands/filesystem/startCompactCommand");
import startCsCompactCommand = require("commands/counter/startCompactCommand");

class resourceCompact {
    resourceName = ko.observable<string>('');
    
    resourcesNames: KnockoutComputed<string[]>;
    searchResults: KnockoutComputed<string[]>;
    nameCustomValidityError: KnockoutComputed<string>;

    compactStatusMessages = ko.observableArray<string>();
    compactStatusLastUpdate = ko.observable<string>();

    keepDown = ko.observable<boolean>(false);

    constructor(private parent: compact, private type: string, private resources: KnockoutObservableArray<resource>) {
        this.resourcesNames = ko.computed(() => resources().map((rs: resource) => rs.name));

        this.searchResults = ko.computed(() => {
            var newResourceName = this.resourceName();
            return this.resourcesNames().filter((name) => name.toLowerCase().indexOf(newResourceName.toLowerCase()) > -1);
        });

        this.nameCustomValidityError = ko.computed(() => {
            var errorMessage: string = '';
            var newResourceName = this.resourceName();
            var foundRs = this.resources().first((rs: resource) => newResourceName === rs.name);

            if (!foundRs && newResourceName.length > 0) {
				var typeName = this.resources()[0].fullTypeName;
                errorMessage = typeName + " name doesn't exist!";
            }

            return errorMessage;
        });
    }

    toggleKeepDown() {
        this.keepDown.toggle();
        if (this.keepDown()) {
            var logsPre = document.getElementById(this.type + 'CompactLogPre');
            logsPre.scrollTop = logsPre.scrollHeight;
        }
    }

    updateCompactStatus(newCompactStatus: compactStatusDto) {
        this.compactStatusMessages(newCompactStatus.Messages);
        this.compactStatusLastUpdate(newCompactStatus.LastProgressMessage);
        if (this.keepDown()) {
            var logsPre = document.getElementById(this.type + "CompactLogPre");
            logsPre.scrollTop = logsPre.scrollHeight;
        }
        this.parent.isBusy(newCompactStatus.State === "Running");
    }

}
class compact extends viewModelBase {
    private dbCompactOptions = new resourceCompact(this, database.type, shell.databases);
    private fsCompactOptions = new resourceCompact(this, fileSystem.type, shell.fileSystems);
    private csCompactOptions = new resourceCompact(this, counterStorage.type, shell.fileSystems);

    isBusy = ko.observable<boolean>();

    canActivate(args): any {
        return true;
    }

    activate(args) {
        super.activate(args);
        this.updateHelpLink('7HZGOE');
    }

    compositionComplete() {
        super.compositionComplete();
        $('form :input[name="databaseName"]').on("keypress", (e) => e.which !== 13);
        $('form :input[name="filesystemName"]').on("keypress", (e) => e.which !== 13);
    }

    startDbCompact() {
        this.isBusy(true);
        var self = this;

        new startDbCompactCommand(this.dbCompactOptions.resourceName(), self.dbCompactOptions.updateCompactStatus.bind(self.dbCompactOptions))
            .execute();
    }

    startFsCompact() {
        this.isBusy(true);
        var self = this;

        new startFsCompactCommand(this.fsCompactOptions.resourceName(), self.fsCompactOptions.updateCompactStatus.bind(self.fsCompactOptions))
            .execute();
    }
	
	startCsCompact() {
        this.isBusy(true);
        var self = this;

        new startCsCompactCommand(this.csCompactOptions.resourceName(), self.csCompactOptions.updateCompactStatus.bind(self.csCompactOptions))
            .execute();
    }
}

export = compact;