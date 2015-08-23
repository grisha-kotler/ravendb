import monitorCompactCommandBase = require("common/monitorCompactCommandBase");

class monitorCompactCommand extends monitorCompactCommandBase {
    constructor(parentPromise: JQueryDeferred<any>, fsName: string, updateCompactStatus: (compactStatusDto) => void) {
        super(parentPromise, fsName, updateCompactStatus, "Raven/FileSystem/Compact/Status/");
    }
}

export = monitorCompactCommand;