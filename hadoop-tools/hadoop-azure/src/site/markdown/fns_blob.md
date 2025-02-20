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

## Background
The ABFS driver is recommended to be used only with HNS Enabled ADLS Gen-2 accounts
for big data analytics because of being more performant and scalable.

However, to enable users of legacy WASB Driver to migrate to ABFS driver without
needing them to upgrade their general purpose V2 accounts (HNS-Disabled), Support
for FNS accounts is being added to ABFS driver.
Refer to [WASB Deprication](./wasb.html) for more details.

## Azure Service Endpoints Used by ABFS Driver
Azure Services offers two set of endpoints for interacting with storage accounts:
1. [Azure Blob Storage](./blobEndpoint.md) referred as Blob Endpoint
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
type only for Ingress Related Operations like [Create](./blobEndpoint.html#put-blob),
[Append](./blobEndpoint.html#put-block),
and [Flush](./blobEndpoint.html#put-block-list). All other operations will still use the
configured service type.
    ```xml
   <property>
        <name>fs.azure.fns.account.service.type</name>
        <value>BLOB</value>
    </property>
    ```

5. Auth type supported over FNS Blob: SharedKey, OAuth and fixed SAS.
    ```xml
   <property>
        <name>fs.azure.account.auth.type</name>
        <value>SharedKey</value>
    </property>
    ```
    - How to configure Shared Key
      auth: [Shared Key](./abfs.md#a-nameshared-key-autha-default-shared-key)
    - How to configure
      OAuth: [OAuth](./abfs.md#a-nameoauth-client-credentialsa-oauth-20-client-credentials)
    - How to configure fixed
      SAS: [Fixed SAS](./abfs.md#using-accountservice-sas-with-abfs)

   OAuth is recommended auth type as it is more secure and flexible.

### <a name="renamedeleteoptions"></a> Rename delete configs

The following configs are related to rename and delete operations.

- `fs.azure.blob.copy.progress.wait.millis`: Blob copy API is an async API, this
  configuration defines polling duration for checking copy status. The default
  value is 1 sec i.e. 1000 ms.

- `fs.azure.blob.copy.max.wait.millis`: Maximum time to wait for a blob copy
  operation to complete. The default value is 5 minutes.

- `fs.azure.blob.atomic.rename.lease.refresh.duration`: Blob rename lease
  refresh
  duration in milliseconds. This setting ensures that the lease on the blob is
  periodically refreshed during a rename operation to prevent other operations
  from interfering.
  The default value is 60 seconds.

- `fs.azure.blob.dir.list.producer.queue.max.size`: Maximum number of blob
  entries
  enqueued in memory for rename or delete orchestration. The default value is 2
  times the default value of list max results, which is 5000, making the current
  value 10000.

- `fs.azure.blob.dir.list.consumer.max.lag`: It sets a limit on how much blob
  information can be waiting to be processed (consumer lag) during a blob
  listing
  operation. If the amount of unprocessed blob information exceeds this limit,
  the
  producer will pause until the consumer catches up and the lag becomes
  manageable. The default value is equal to the value of default value of list
  max
  results which is 5000 currently.

- `fs.azure.blob.dir.rename.max.thread`: Maximum number of threads per blob
  rename
  orchestration. The default value is 5.

- `fs.azure.blob.dir.delete.max.thread`: Maximum number of thread per
  blob-delete
  orchestration. The default value currently is 5.

## Features currently not supported

1. **User Delegation SAS** feature is currently not supported but we
   plan to bring support for it in the future.
   Jira to track this
   workitem : https://issues.apache.org/jira/browse/HADOOP-19406.


2. **Context Provider Key (CPK)** support is currently not available. It refers to the ability to use a
customer-provided encryption key to encrypt and decrypt data in Azure Blob
Storage. This feature allows users to manage their own encryption keys,
providing an additional layer of security and control over their data.

## Ask all about ABFS Driver

For any queries related to onboard to FNS Blob or anything related to ABFS
Driver in general, kindly reach out to us at **askabfs@microsoft.com**. 