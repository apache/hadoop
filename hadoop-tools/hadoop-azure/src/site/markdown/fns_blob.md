<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# ABFS Driver for Namespace Disabled Accounts (FNS: Flat Namespace)

### Note: FNS-BLOB Support is being built and not yet ready for usage.

## Background
The ABFS driver is recommended to be used only with HNS Enabled ADLS Gen-2 accounts
for big data analytics because of being more performant and scalable.

However, to enable users of legacy WASB Driver to migrate to ABFS driver without
needing them to upgrade their general purpose V2 accounts (HNS-Disabled), Support
for FNS accounts is being added to ABFS driver.
Refer to [WASB Deprication](./wasb.html) for more details.

## Azure Service Endpoints Used by ABFS Driver
Azure Services offers two set of endpoints for interacting with storage accounts:
1. [Azure Blob Storage](https://learn.microsoft.com/en-us/rest/api/storageservices/blob-service-rest-api) referred as Blob Endpoint
2. [Azure Data Lake Storage](https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/operation-groups) referred as DFS Endpoint

The ABFS Driver by default is designed to work with DFS Endpoint only which primarily
supports HNS Enabled Accounts only.

To enable ABFS Driver to work with FNS Accounts, Support for Blob Endpoint is being added.
This is because Azure services do not recommend using DFS Endpoint for FNS Accounts.
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
    </property>
    ```

2. Account Url: It is the URL used to initialize the file system. It is either passed
directly to file system or configured as default uri using "fs.DefaultFS" configuration.
In both the cases the URL used must be the blob endpoint url of the account.
    ```xml
    <property>
      <name>fs.defaultFS</name>
      <value>https://ACCOUNT_NAME.blob.core.windows.net</value>
    </property>
    ```
3. Service Type for FNS Accounts: This will allow an override to choose service
type specially in cases where any local DNS resolution is set for the account and driver is
unable to detect the intended endpoint from above configured URL. If this is set
to blob for HNS Enabled Accounts, FS init will fail with InvalidConfiguration error.
    ```xml
   <property>
        <name>fs.azure.fns.account.service.type</name>
        <value>BLOB</value>
    </property>
    ```

4. Service Type for Ingress Operations: This will allow an override to choose service
type only for Ingress Related Operations like [Create](https://learn.microsoft.com/en-us/rest/api/storageservices/put-blob?tabs=microsoft-entra-id),
[Append](https://learn.microsoft.com/en-us/rest/api/storageservices/put-block?tabs=microsoft-entra-id)
and [Flush](https://learn.microsoft.com/en-us/rest/api/storageservices/put-block-list?tabs=microsoft-entra-id). All other operations will still use the
configured service type.
    ```xml
   <property>
        <name>fs.azure.fns.account.service.type</name>
        <value>BLOB</value>
    </property>
    ```