import commandBase = require("commands/commandBase");
import counterStorage = require("models/counter/counterStorage");

class getCounterBackupStatus extends commandBase {

    constructor(private cs: counterStorage) {
        super();
    }

    execute(): JQueryPromise<string> {
        var url = "/backup-status";
        return this.query(url, null, this.cs);
    }
}

export = getCounterBackupStatus;