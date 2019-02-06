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

# Hadoop Azure Support: ABFS  — Azure Data Lake Storage Gen2

<!-- MACRO{toc|fromDepth=1|toDepth=3} -->

## Introduction

The `hadoop-azure` module provides support for the Azure Data Lake Storage Gen2
storage layer through the "abfs" connector

To make it part of Apache Hadoop's default classpath, simply make sure that
`HADOOP_OPTIONAL_TOOLS` in `hadoop-env.sh` has `hadoop-azure` in the list.

## Features

* Read and write data stored in an Azure Blob Storage account.
* *Fully Consistent* view of the storage across all clients.
* Can read data written through the wasb: connector.
* Present a hierarchical file system view by implementing the standard Hadoop
  [`FileSystem`](../api/org/apache/hadoop/fs/FileSystem.html) interface.
* Supports configuration of multiple Azure Blob Storage accounts.
* Can act as a source or destination of data in Hadoop MapReduce, Apache Hive, Apache Spark
* Tested at scale on both Linux and Windows.
* Can be used as a replacement for HDFS on Hadoop clusters deployed in Azure infrastructure.



## Limitations

* File last access time is not tracked.


## Technical notes

### Security

### Consistency and Concurrency

*TODO*: complete/review

The abfs client has a fully consistent view of the store, which has complete Create Read Update and Delete consistency for data and metadata.
(Compare and contrast with S3 which only offers Create consistency; S3Guard adds CRUD to metadata, but not the underlying data).

### Performance

*TODO*: check these.

* File Rename: `O(1)`.
* Directory Rename: `O(files)`.
* Directory Delete: `O(files)`.

## Configuring ABFS

Any configuration can be specified generally (or as the default when accessing all accounts) or can be tied to s a specific account.
For example, an OAuth identity can be configured for use regardless of which account is accessed with the property
"fs.azure.account.oauth2.client.id"
or you can configure an identity to be used only for a specific storage account with
"fs.azure.account.oauth2.client.id.\<account\_name\>.dfs.core.windows.net".

Note that it doesn't make sense to do this with some properties, like shared keys that are inherently account-specific.

## Testing ABFS

See the relevant section in [Testing Azure](testing_azure.html).

## References

* [A closer look at Azure Data Lake Storage Gen2](https://azure.microsoft.com/en-gb/blog/a-closer-look-at-azure-data-lake-storage-gen2/);
MSDN Article from June 28, 2018.
