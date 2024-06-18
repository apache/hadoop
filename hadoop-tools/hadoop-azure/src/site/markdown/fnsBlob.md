# ABFS Driver for FNS Accounts

### Note: FNS-BLOB Support is being built and not yet ready for usage.

## Background
ABFS driver is recommended to be used only with HNS Enabled ADLS Gen-2 accounts
for big data analytics because of being more performant and scalable.

However, to enable users of legacy WASB Driver to migrate to ABFS driver without
needing them to upgrade their general purpose V2 accounts (HNS-Disabled), Support
for FNS accounts is being added to ABFS driver.
Refer to [WASB Deprication](./wasb.html) for more details.

## Azure Service Endpoints Used by ABFS Driver
Azure Services offers two set of endpoints for interacting with storage accounts:
1. [Azure Blob Storage](https://learn.microsoft.com/en-us/rest/api/storageservices/blob-service-rest-api) referred as Blob Endpoint
2. [Azure Data Lake Storage](https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/operation-groups) referred as DFS Endpoint

ABFS Driver by default is designed to work with DFS Endpoint only which primarily
supports HNS Enabled Accounts only. However, azure services does not recommended
to interact with FNS accounts using DFS Endpoint as the behavior is not determined.

To enable ABFS Driver to work with FNS Accounts, Support for Blob Endpoint is being added.
ABFS Driver will only allow FNS Accounts to be accessed using Blob Endpoint.
HNS Enabled accounts will still use DFS Endpoint which continues to be the
recommended stack based on performance and feature capabilities.

## Configuring ABFS Driver for FNS Accounts
Following configurations will be introduced to configure ABFS Driver for FNS Accounts:
1. Account Type: Must be set to `false` to indicate FNS Account
    ```xml
    <property>
      <name>fs.azure.account.hns.enabled</name>
      <value>false</value>
      <description>Type of account used with ABFS Driver</description>
    </property>
    ```

2. Account Url: It is either configured using following config or the path url used.
   Both must be blob endpoint url for FNS Account
    ```xml
    <property>
      <name>fs.defaultFS</name>
      <value>https://ACCOUNT_NAME.blob.core.windows.net</value>
      <description>Account URL for FNS Account</description>
    </property>
    ```
2. Service Type for FNS Accounts: This will allow an override to choose service
   type in cases where any local DNS resolution is set for the account and driver is
   unable to detect the intended endpoint from above configured URL. If this is set
   to blob for HNS Enabled Accounts, FS init will fail with InvalidConfiguration error.
    ```xml
   <property>
        <name>fs.azure.fns.account.service.type</name>
        <value>BLOB</value>
        <description>Service Type for FNS Account</description>
    </property>
    ```