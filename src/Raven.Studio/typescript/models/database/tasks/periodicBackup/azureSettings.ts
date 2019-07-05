﻿import backupSettings = require("models/database/tasks/periodicBackup/backupSettings");
import jsonUtil = require("common/jsonUtil");

class azureSettings extends backupSettings {
    storageContainer = ko.observable<string>();
    remoteFolderName = ko.observable<string>();
    accountName = ko.observable<string>();
    accountKey = ko.observable<string>();
    tempAccessToken = ko.observable<string>();

    constructor(dto: Raven.Client.Documents.Operations.Backups.AzureSettings) {
        super(dto, "Azure");

        this.storageContainer(dto.StorageContainer);
        this.remoteFolderName(dto.RemoteFolderName);
        this.accountName(dto.AccountName);
        this.accountKey(dto.AccountKey);
        this.tempAccessToken(dto.TempAccessToken);

        this.initValidation();

        this.dirtyFlag = new ko.DirtyFlag([
            this.enabled,
            this.storageContainer,
            this.remoteFolderName,
            this.accountName,
            this.accountKey
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    initValidation() {
        /* Container name must:
            - start with a letter or number, and can contain only letters, numbers, and the dash (-) character.
            - every dash (-) character must be immediately preceded and followed by a letter or number; 
            - consecutive dashes are not permitted in container names.
            - all letters in a container name must be lowercase.
            - container names must be from 3 through 63 characters long.
        */
        const allowedCharactersRegExp = /^[a-z0-9-]+$/;
        const twoDashesRegExp = /--/;
        this.storageContainer.extend({
            validation: [
                {
                    validator: (storageContainer: string) => this.validate(() =>
                        storageContainer && storageContainer.length >= 3 && storageContainer.length <= 63),
                    message: "Container name should be between 3 and 63 characters long"
                },
                {
                    validator: (storageContainer: string) => this.validate(() =>
                        allowedCharactersRegExp.test(storageContainer)),
                    message: "Allowed characters lowercase characters, numbers and dashes"
                },
                {
                    validator: (storageContainer: string) => this.validate(() =>
                        storageContainer && storageContainer[0] !== "-" && storageContainer[storageContainer.length - 1] !== "-"),
                    message: "Container name must start and end with a letter or number"
                },
                {
                    validator: (storageContainer: string) => this.validate(() =>
                        !twoDashesRegExp.test(storageContainer)),
                    message: "Consecutive dashes are not permitted in container names"
                }
            ]
        });

        this.accountName.extend({
            required: {
                onlyIf: () => this.enabled()
            }
        });

        this.accountKey.extend({
            required: {
                onlyIf: () => this.enabled()
            }
        });

        this.validationGroup = ko.validatedObservable({
            storageContainer: this.storageContainer,
            accountName: this.accountName,
            accountKey: this.accountKey
        });
    }

    toDto(): Raven.Client.Documents.Operations.Backups.AzureSettings {
        const dto = super.toDto() as Raven.Client.Documents.Operations.Backups.AzureSettings;
        dto.StorageContainer = this.storageContainer();
        dto.RemoteFolderName = this.remoteFolderName();
        dto.AccountName = this.accountName();
        dto.AccountKey = this.accountKey();
        dto.TempAccessToken = this.tempAccessToken();
        return dto;
    }

    static empty(): azureSettings {
        return new azureSettings({
            Disabled: true,
            StorageContainer: null,
            RemoteFolderName: null,
            AccountName: null,
            AccountKey: null,
            TempAccessToken: null,
            GetBackupConfigurationScript: null
        });
    }
}

export = azureSettings;
