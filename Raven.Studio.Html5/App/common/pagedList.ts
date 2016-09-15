/// <reference path="../../Scripts/typings/jquery/jquery.d.ts" />
/// <reference path="../../Scripts/typings/knockout/knockout.d.ts" />

import pagedResultSet = require("common/pagedResultSet");

class pagedList {

    totalResultCount = ko.observable(0);
    private items = [];
    isFetching = false;
    queuedFetch: Array<{ skip: number; take: number; task: JQueryDeferred<pagedResultSet> }> = [];
    collectionName = "";
    currentItemIndex = ko.observable(0);
    plus = 0;

    constructor(private fetcher: (skip: number, take: number) => JQueryPromise<pagedResultSet>) {
        if (!fetcher) {
            throw new Error("Fetcher must be specified.");
        }
    }

    clear() {
        this.queuedFetch.forEach(x => x.task.reject("data is being reloaded"));
        this.queuedFetch = [];

        while (this.items.length > 0) {
            this.items.pop();
        }
    }

    itemCount(): number {
        return this.items.length;
    }

    fetch(skip: number, take: number): JQueryPromise<pagedResultSet> {
        var cachedItemsSlice = this.getCachedSliceOrNull(skip, take);
        if (cachedItemsSlice) {
            // We've already fetched these items. Just return them immediately.
            var deferred = $.Deferred<pagedResultSet>();
            var results = new pagedResultSet(cachedItemsSlice, this.totalResultCount());
            deferred.resolve(results);
            return deferred;
        }

        if (this.isFetching) {
            var queuedFetch = { skip: skip, take: take, task: $.Deferred() };
            this.queuedFetch.push(queuedFetch);
            return queuedFetch.task;
        }

        //else {
        // We haven't fetched some of the items. Fetch them now from remote.
        this.isFetching = true;
        this.plus++;
        var self = this;
        var remoteFetch = this.fetcher(skip, take)
            .done((resultSet: pagedResultSet) => {
                self.totalResultCount(resultSet.totalResultCount);
                resultSet.items.forEach((r, i) => self.items[i + skip] = r);
            })
            .fail(() => {
                var x = 4;
            })
            .always(() => {
                self.plus--;
                self.isFetching = false;
                self.runQueuedFetch();
            });
        return remoteFetch;
        //}
    }

    getCachedSliceOrNull(skip: number, take: number): any[] {
        for (var i = skip; i < skip + take; i++) {
            if (!this.items[i]) {
                return null;
            }
        }

        return this.items.slice(skip, skip + take);
    }

    getNthItem(nth: number): JQueryPromise<any> {
        var deferred = $.Deferred();
        var cachedItemArray = this.getCachedSliceOrNull(nth, 1);
        if (cachedItemArray) {
            deferred.resolve(cachedItemArray[0]);
        } else {
            this.fetch(nth, 1)
                .done((result: pagedResultSet) => {
                    deferred.resolve(result.items[0]);
                })
                .fail(error => deferred.reject(error));
        }
        return deferred;
    }

    getCachedItemsAt(indices: number[]): any[] {
        return indices
            .filter(index => this.items[index])
            .map(validIndex => this.items[validIndex]);
    }

    getCachedIndices(indices: number[]): number[] {
        return indices.filter(index => this.items[index]);
    }

    getAllCachedItems(): any[] {
        return this.items;
    }

    runQueuedFetch() {
        if (this.queuedFetch.length === 0) {
            return;
        }

        var queuedFetch = this.queuedFetch.pop();
        var queuedSkip = queuedFetch.skip;
        var queuedTake = queuedFetch.take;
        var queuedTask = queuedFetch.task;
        var fetchTask = this.fetch(queuedSkip, queuedTake);
        fetchTask.done(results => queuedTask.resolve(results));
        fetchTask.fail(error => queuedTask.reject(error));
    }

    invalidateCache() {
        this.items.length = 0;
    }

    indexOf(item: any) {
        return this.items.indexOf(item);
    }

    hasIds(): boolean {
        return this.items && this.items.length > 0 && this.items[0] && this.items[0].getId && this.items[0].getId();
    }
}

export = pagedList;
