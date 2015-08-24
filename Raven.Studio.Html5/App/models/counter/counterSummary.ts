class counterSummary implements documentBase {
	static groupNameField = "Group Name";
	static counterNameField = "Counter Name";
    Total: number; 

    constructor(dto: counterSummaryDto) {
	    this[counterSummary.groupNameField] = dto.GroupName;
	    this[counterSummary.counterNameField] = dto.CounterName;
        this.Total = dto.Total;
    }

    getEntityName() {
        return this[counterSummary.groupNameField];
    }

    getDocumentPropertyNames(): Array<string> {
	    return [counterSummary.groupNameField, counterSummary.counterNameField, "Total"];
    }

    getId() {
        return this[counterSummary.counterNameField];
    }

    getUrl() {
        return this.getId();
    }

	getGroupName() {
		return this[counterSummary.groupNameField];
	}

	getCounterName() {
		return this[counterSummary.counterNameField];
	}
} 

export = counterSummary;