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

### <a name="flushconfigoptions"></a> Flush Options

#### <a name="abfsflushconfigoptions"></a> 1. Azure Blob File System Flush Options
Config `fs.azure.enable.flush` provides an option to render ABFS flush APIs -
 HFlush() and HSync() to be no-op. By default, this
config will be set to true.

Both the APIs will ensure that data is persisted.

#### <a name="outputstreamflushconfigoptions"></a> 2. OutputStream Flush Options
Config `fs.azure.disable.outputstream.flush` provides an option to render
OutputStream Flush() API to be a no-op in AbfsOutputStream. By default, this
config will be set to true.

Hflush() being the only documented API that can provide persistent data
transfer, Flush() also attempting to persist buffered data will lead to
performance issues.


## Testing ABFS

### <a name="perfoptions"></a> Perf Options

#### <a name="abfstracklatencyoptions"></a> 1. HTTP Request Tracking Options
If you set `fs.azure.abfs.latency.track` to `true`, the module starts tracking the
performance metrics of ABFS HTTP traffic. To obtain these numbers on your machine
or cluster, you will also need to enable debug logging for the `AbfsPerfTracker`
class in your `log4j` config. A typical perf log line appears like:

```
h=KARMA t=2019-10-25T20:21:14.518Z a=abfstest01.dfs.core.windows.net
c=abfs-testcontainer-84828169-6488-4a62-a875-1e674275a29f cr=delete ce=deletePath
r=Succeeded l=32 ls=32 lc=1 s=200 e= ci=95121dae-70a8-4187-b067-614091034558
ri=97effdcf-201f-0097-2d71-8bae00000000 ct=0 st=0 rt=0 bs=0 br=0 m=DELETE
u=https%3A%2F%2Fabfstest01.dfs.core.windows.net%2Ftestcontainer%2Ftest%3Ftimeout%3D90%26recursive%3Dtrue
```

The fields have the following definitions:

`h`: host name
`t`: time when this request was logged
`a`: Azure storage account name
`c`: container name
`cr`: name of the caller method
`ce`: name of the callee method
`r`: result (Succeeded/Failed)
`l`: latency (time spent in callee)
`ls`: latency sum (aggregate time spent in caller; logged when there are multiple
callees; logged with the last callee)
`lc`: latency count (number of callees; logged when there are multiple callees;
logged with the last callee)
`s`: HTTP Status code
`e`: Error code
`ci`: client request ID
`ri`: server request ID
`ct`: connection time in milliseconds
`st`: sending time in milliseconds
`rt`: receiving time in milliseconds
`bs`: bytes sent
`br`: bytes received
`m`: HTTP method (GET, PUT etc)
`u`: Encoded HTTP URL

Note that these performance numbers are also sent back to the ADLS Gen 2 API endpoints
in the `x-ms-abfs-client-latency` HTTP headers in subsequent requests. Azure uses these
settings to track their end-to-end latency.

## <a name="troubleshooting"></a> Troubleshooting

The problems associated with the connector usually come down to, in order

1. Classpath.
1. Network setup (proxy etc.).
1. Authentication and Authorization.
1. Anything else.

If you log `org.apache.hadoop.fs.azurebfs.services` at `DEBUG` then you will
see more details about any request which is failing.

One useful tool for debugging connectivity is the [cloudstore storediag utility](https://github.com/steveloughran/cloudstore/releases).

This validates the classpath, the settings, then tries to work with the filesystem.

```bash
bin/hadoop jar cloudstore-0.1-SNAPSHOT.jar storediag abfs://container@account.dfs.core.windows.net/
```

1. If the `storediag` command cannot work with an abfs store, nothing else is likely to.
1. If the `storediag` store does successfully work, that does not guarantee that the classpath
or configuration on the rest of the cluster is also going to work, especially
in distributed applications. But it is at least a start.

### `ClassNotFoundException: org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem`

The `hadoop-azure` JAR is not on the classpah.

```
java.lang.RuntimeException: java.lang.ClassNotFoundException:
    Class org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem not found
  at org.apache.hadoop.conf.Configuration.getClass(Configuration.java:2625)
  at org.apache.hadoop.fs.FileSystem.getFileSystemClass(FileSystem.java:3290)
  at org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:3322)
  at org.apache.hadoop.fs.FileSystem.access$200(FileSystem.java:136)
  at org.apache.hadoop.fs.FileSystem$Cache.getInternal(FileSystem.java:3373)
  at org.apache.hadoop.fs.FileSystem$Cache.get(FileSystem.java:3341)
  at org.apache.hadoop.fs.FileSystem.get(FileSystem.java:491)
  at org.apache.hadoop.fs.Path.getFileSystem(Path.java:361)
Caused by: java.lang.ClassNotFoundException:
    Class org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem not found
  at org.apache.hadoop.conf.Configuration.getClassByName(Configuration.java:2529)
  at org.apache.hadoop.conf.Configuration.getClass(Configuration.java:2623)
  ... 16 more
```

Tip: if this is happening on the command line, you can turn on debug logging
of the hadoop scripts:

```bash
export HADOOP_SHELL_SCRIPT_DEBUG=true
```

If this is happening on an application running within the cluster, it means
the cluster (somehow) needs to be configured so that the `hadoop-azure`
module and dependencies are on the classpath of deployed applications.

### `ClassNotFoundException: com.microsoft.azure.storage.StorageErrorCode`

The `azure-storage` JAR is not on the classpath.

See the relevant section in [Testing Azure](testing_azure.html).

## References

* [A closer look at Azure Data Lake Storage Gen2](https://azure.microsoft.com/en-gb/blog/a-closer-look-at-azure-data-lake-storage-gen2/);
MSDN Article from June 28, 2018.
