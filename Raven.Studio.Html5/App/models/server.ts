﻿class server {
    name = ko.observable('');
    posCount  = ko.observable(0);
    negCount = ko.observable(0);

    constructor(dto: any/*serverDto*/) {
        this.name(dto.Name);
        this.posCount(dto.Positive);
        this.negCount(dto.Negative);
    }
} 

export = server;