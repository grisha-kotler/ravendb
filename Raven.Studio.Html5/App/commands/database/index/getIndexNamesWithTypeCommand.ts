import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getIndexNamesWithTypeCommand extends commandBase {
    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<indexDataDto[]> {
        var url = "/indexes/?namesWithTypeOnly=true";
        return this.query(url, null, this.db);
    }
}

export = getIndexNamesWithTypeCommand;
