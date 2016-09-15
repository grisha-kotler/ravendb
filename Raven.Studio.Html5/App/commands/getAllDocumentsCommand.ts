import commandBase = require("commands/commandBase");
import database = require("models/database");
import pagedResultSet = require("common/pagedResultSet");
import document = require("models/document");

/*
 * getAllDocumentsCommand is a specialized command that fetches all the documents in a specified database.
*/
class getAllDocumentsCommand extends commandBase {

    constructor(private ownerDatabase: database, private skip: number, private take: number) {
        super();
    }

    execute(): JQueryPromise<pagedResultSet> {

        // Getting all documents requires a 2 step process:
        // 1. Get the database total doc count.
        // 2. Fetch /docs to get the actual documents.

        var docsTask = this.fetchDocs();
        var doneTask = $.Deferred();
        docsTask.done((docsResult: document[]) => doneTask.resolve(new pagedResultSet(docsResult, this.ownerDatabase.itemCount())));
        docsTask.fail(xhr => doneTask.reject(xhr));
        return doneTask;
    }

    private fetchDocs(): JQueryPromise<document[]> {
        var args = {
            start: this.skip,
            pageSize: this.take
        };

        var docSelector = (docs: documentDto[]) => docs.map(d => new document(d));
        return this.query("/docs", args, this.ownerDatabase, docSelector);
    }

    private fetchTotalResultCount(): JQueryPromise<number> {
        var args = {
            query: "",
            start: 0,
            pageSize: 0
        };

        var url = "/indexes/Raven/DocumentsByEntityName";
        var countSelector = (dto: collectionInfoDto) => dto.TotalResults;
        return this.query(url, args, this.ownerDatabase, countSelector);
    }
}

export = getAllDocumentsCommand;
