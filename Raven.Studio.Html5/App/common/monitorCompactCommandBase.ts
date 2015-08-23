import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import appUrl = require("common/appUrl");

class monitorCompactCommandBase extends commandBase {
    private db: database = appUrl.getSystemDatabase();

    constructor(private parentPromise: JQueryDeferred<any>, private resourceName: string, private updateCompactStatus: (compactStatusDto) => void, private compactDocumentName) {
        super();
    }

    execute(): JQueryPromise<any> {
        new getDocumentWithMetadataCommand("Raven/FileSystem/Compact/Status/" + this.resourceName, this.db)
            .execute()
            .fail((response: JQueryXHR) => {
                setTimeout(() => this.execute(), 1000);
            })
            .done((compactStatus: compactStatusDto) => {
                this.updateCompactStatus(compactStatus);

                if (compactStatus.State == "Running") {
                    setTimeout(() => this.execute(), 1000);
                } else {
                    if (compactStatus.State == "Completed") {
                        this.reportSuccess("File system was successfully compacted!");
                        this.parentPromise.resolve();
                    } else {
                        this.reportError("File system wasn't compacted!");
                        this.parentPromise.reject();
                    }
                }
            });
        return this.parentPromise;
    }
}

export = monitorCompactCommandBase;