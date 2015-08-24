import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import d3 = require("d3/d3");

class getFilteredOutIndexStatsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<filteredOutIndexStatDto[]> {
        var url = "/debug/filtered-out-indexes";
        var parser = d3.time.format.iso;

        return this.query<filteredOutIndexStatDto[]>(url, null, this.db, result => {
            result.map(item => {
                item.TimestampParsed = parser.parse(item.Timestamp);
            });
            return result;
        });
    }

}

export = getFilteredOutIndexStatsCommand;
