import monitorCompactCommandBase = require("common/monitorCompactCommandBase");

class monitorCompactCommand extends monitorCompactCommandBase {
	constructor(parentPromise: JQueryDeferred<any>, dbName: string, updateCompactStatus: (compactStatusDto) => void) {
        super(parentPromise, dbName, updateCompactStatus, "Raven/Counters/Compact/Status/");
    }
}

export = monitorCompactCommand;