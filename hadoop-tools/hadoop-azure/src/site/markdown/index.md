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

# Hadoop Azure Support: Azure Blob Storage

<!-- MACRO{toc|fromDepth=1|toDepth=3} -->

## Introduction

The hadoop-azure module provides support for integration with
[Azure Blob Storage](http://azure.microsoft.com/en-us/documentation/services/storage/).
The built jar file, named hadoop-azure.jar, also declares transitive dependencies
on the additional artifacts it requires, notably the
[Azure Storage SDK for Java](https://github.com/Azure/azure-storage-java).

To make it part of Apache Hadoop's default classpath, simply make sure that
HADOOP_OPTIONAL_TOOLS in hadoop-env.sh has 'hadoop-azure' in the list.

## Features

* Read and write data stored in an Azure Blob Storage account.
* Present a hierarchical file system view by implementing the standard Hadoop
  [`FileSystem`](../api/org/apache/hadoop/fs/FileSystem.html) interface.
* Supports configuration of multiple Azure Blob Storage accounts.
* Supports both block blobs (suitable for most use cases, such as MapReduce) and
  page blobs (suitable for continuous write use cases, such as an HBase
  write-ahead log).
* Reference file system paths using URLs using the `wasb` scheme.
* Also reference file system paths using URLs with the `wasbs` scheme for SSL
  encrypted access.
* Can act as a source of data in a MapReduce job, or a sink.
* Tested on both Linux and Windows.
* Tested at scale.

## Limitations

* File owner and group are persisted, but the permissions model is not enforced.
  Authorization occurs at the level of the entire Azure Blob Storage account.
* File last access time is not tracked.

## Usage

### Concepts

The Azure Blob Storage data model presents 3 core concepts:

* **Storage Account**: All access is done through a storage account.
* **Container**: A container is a grouping of multiple blobs.  A storage account
  may have multiple containers.  In Hadoop, an entire file system hierarchy is
  stored in a single container.  It is also possible to configure multiple
  containers, effectively presenting multiple file systems that can be referenced
  using distinct URLs.
* **Blob**: A file of any type and size.  In Hadoop, files are stored in blobs.
  The internal implementation also uses blobs to persist the file system
  hierarchy and other metadata.

### Configuring Credentials

Usage of Azure Blob Storage requires configuration of credentials.  Typically
this is set in core-site.xml.  The configuration property name is of the form
`fs.azure.account.key.<account name>.blob.core.windows.net` and the value is the
access key.  **The access key is a secret that protects access to your storage
account.  Do not share the access key (or the core-site.xml file) with an
untrusted party.**

For example:

```xml
<property>
  <name>fs.azure.account.key.youraccount.blob.core.windows.net</name>
  <value>YOUR ACCESS KEY</value>
</property>
```
In many Hadoop clusters, the core-site.xml file is world-readable. It is possible to
protect the access key within a credential provider as well. This provides an encrypted
file format along with protection with file permissions.

#### Protecting the Azure Credentials for WASB with Credential Providers

To protect these credentials from prying eyes, it is recommended that you use
the credential provider framework to securely store them and access them
through configuration. The following describes its use for Azure credentials
in WASB FileSystem.

For additional reading on the credential provider API see:
[Credential Provider API](../hadoop-project-dist/hadoop-common/CredentialProviderAPI.html).

##### End to End Steps for Distcp and WASB with Credential Providers

###### provision

```bash
% hadoop credential create fs.azure.account.key.youraccount.blob.core.windows.net -value 123
    -provider localjceks://file/home/lmccay/wasb.jceks
```

###### configure core-site.xml or command line system property

```xml
<property>
  <name>hadoop.security.credential.provider.path</name>
  <value>localjceks://file/home/lmccay/wasb.jceks</value>
  <description>Path to interrogate for protected credentials.</description>
</property>
```

###### distcp

```bash
% hadoop distcp
    [-D hadoop.security.credential.provider.path=localjceks://file/home/lmccay/wasb.jceks]
    hdfs://hostname:9001/user/lmccay/007020615 wasb://yourcontainer@youraccount.blob.core.windows.net/testDir/
```

NOTE: You may optionally add the provider path property to the distcp command line instead of
added job specific configuration to a generic core-site.xml. The square brackets above illustrate
this capability.

#### Protecting the Azure Credentials for WASB within an Encrypted File

In addition to using the credential provider framework to protect your credentials, it's
also possible to configure it in encrypted form.  An additional configuration property
specifies an external program to be invoked by Hadoop processes to decrypt the
key.  The encrypted key value is passed to this external program as a command
line argument:

```xml
<property>
  <name>fs.azure.account.keyprovider.youraccount</name>
  <value>org.apache.hadoop.fs.azure.ShellDecryptionKeyProvider</value>
</property>

<property>
  <name>fs.azure.account.key.youraccount.blob.core.windows.net</name>
  <value>YOUR ENCRYPTED ACCESS KEY</value>
</property>

<property>
  <name>fs.azure.shellkeyprovider.script</name>
  <value>PATH TO DECRYPTION PROGRAM</value>
</property>

```

### Block Blob with Compaction Support and Configuration

Block blobs are the default kind of blob and are good for most big-data use
cases. However, block blobs have strict limit of 50,000 blocks per blob.
To prevent reaching the limit WASB, by default, does not upload new block to
the service after every `hflush()` or `hsync()`.

For most of the cases, combining data from multiple `write()` calls in
blocks of 4Mb is a good optimization. But, in others cases, like HBase log files,
every call to `hflush()` or `hsync()` must upload the data to the service.

Block blobs with compaction upload the data to the cloud service after every
`hflush()`/`hsync()`. To mitigate the limit of 50000 blocks, `hflush()
`/`hsync()` runs once compaction process, if number of blocks in the blob
is above 32,000.

Block compaction search and replaces a sequence of small blocks with one big
block. That means there is associated cost with block compaction: reading
small blocks back to the client and writing it again as one big block.

In order to have the files you create be block blobs with block compaction
enabled, the client must set the configuration variable
`fs.azure.block.blob.with.compaction.dir` to a comma-separated list of
folder names.

For example:

```xml
<property>
  <name>fs.azure.block.blob.with.compaction.dir</name>
  <value>/hbase/WALs,/data/myblobfiles</value>
</property>
```

### Page Blob Support and Configuration

The Azure Blob Storage interface for Hadoop supports two kinds of blobs,
[block blobs and page blobs](http://msdn.microsoft.com/en-us/library/azure/ee691964.aspx).
Block blobs are the default kind of blob and are good for most big-data use
cases, like input data for Hive, Pig, analytical map-reduce jobs etc.  Page blob
handling in hadoop-azure was introduced to support HBase log files.  Page blobs
can be written any number of times, whereas block blobs can only be appended to
50,000 times before you run out of blocks and your writes will fail.  That won't
work for HBase logs, so page blob support was introduced to overcome this
limitation.

Page blobs can be up to 1TB in size, larger than the maximum 200GB size for block
blobs.
You should stick to block blobs for most usage, and page blobs are only tested in context of HBase write-ahead logs.

In order to have the files you create be page blobs, you must set the
configuration variable `fs.azure.page.blob.dir` to a comma-separated list of
folder names.

For example:

```xml
<property>
  <name>fs.azure.page.blob.dir</name>
  <value>/hbase/WALs,/hbase/oldWALs,/data/mypageblobfiles</value>
</property>
```

You can set this to simply / to make all files page blobs.

The configuration option `fs.azure.page.blob.size` is the default initial
size for a page blob. It must be 128MB or greater, and no more than 1TB,
specified as an integer number of bytes.

The configuration option `fs.azure.page.blob.extension.size` is the page blob
extension size.  This defines the amount to extend a page blob if it starts to
get full.  It must be 128MB or greater, specified as an integer number of bytes.

### Custom User-Agent
WASB passes User-Agent header to the Azure back-end. The default value
contains WASB version, Java Runtime version, Azure Client library version, and the
value of the configuration option `fs.azure.user.agent.prefix`. Customized User-Agent
header enables better troubleshooting and analysis by Azure service.

```xml
<property>
    <name>fs.azure.user.agent.prefix</name>
    <value>Identifier</value>
</property>
```

### Atomic Folder Rename

Azure storage stores files as a flat key/value store without formal support
for folders.  The hadoop-azure file system layer simulates folders on top
of Azure storage.  By default, folder rename in the hadoop-azure file system
layer is not atomic.  That means that a failure during a folder rename
could, for example, leave some folders in the original directory and
some in the new one.

HBase depends on atomic folder rename.  Hence, a configuration setting was
introduced called `fs.azure.atomic.rename.dir` that allows you to specify a
comma-separated list of directories to receive special treatment so that
folder rename is made atomic.  The default value of this setting is just
`/hbase`.  Redo will be applied to finish a folder rename that fails. A file
`<folderName>-renamePending.json` may appear temporarily and is the record of
the intention of the rename operation, to allow redo in event of a failure.

For example:

```xml
<property>
  <name>fs.azure.atomic.rename.dir</name>
  <value>/hbase,/data</value>
</property>
```

### Accessing wasb URLs

After credentials are configured in core-site.xml, any Hadoop component may
reference files in that Azure Blob Storage account by using URLs of the following
format:

    wasb[s]://<containername>@<accountname>.blob.core.windows.net/<path>

The schemes `wasb` and `wasbs` identify a URL on a file system backed by Azure
Blob Storage.  `wasb` utilizes unencrypted HTTP access for all interaction with
the Azure Blob Storage API.  `wasbs` utilizes SSL encrypted HTTPS access.

For example, the following
[FileSystem Shell](../hadoop-project-dist/hadoop-common/FileSystemShell.html)
commands demonstrate access to a storage account named `youraccount` and a
container named `yourcontainer`.

```bash
% hadoop fs -mkdir wasb://yourcontainer@youraccount.blob.core.windows.net/testDir

% hadoop fs -put testFile wasb://yourcontainer@youraccount.blob.core.windows.net/testDir/testFile

% hadoop fs -cat wasbs://yourcontainer@youraccount.blob.core.windows.net/testDir/testFile
test file content
```

It's also possible to configure `fs.defaultFS` to use a `wasb` or `wasbs` URL.
This causes all bare paths, such as `/testDir/testFile` to resolve automatically
to that file system.

### Append API Support and Configuration

The Azure Blob Storage interface for Hadoop has optional support for Append API for
single writer by setting the configuration `fs.azure.enable.append.support` to true.

For Example:

```xml
<property>
  <name>fs.azure.enable.append.support</name>
  <value>true</value>
</property>
```

It must be noted Append support in Azure Blob Storage interface DIFFERS FROM HDFS SEMANTICS. Append
support does not enforce single writer internally but requires applications to guarantee this semantic.
It becomes a responsibility of the application either to ensure single-threaded handling for a particular
file path, or rely on some external locking mechanism of its own.  Failure to do so will result in
unexpected behavior.

### Multithread Support

Rename and Delete blob operations on directories with large number of files and sub directories currently is very slow as these operations are done one blob at a time serially. These files and sub folders can be deleted or renamed parallel. Following configurations can be used to enable threads to do parallel processing

To enable 10 threads for Delete operation. Set configuration value to 0 or 1 to disable threads. The default behavior is threads disabled.

```xml
<property>
  <name>fs.azure.delete.threads</name>
  <value>10</value>
</property>
```

To enable 20 threads for Rename operation. Set configuration value to 0 or 1 to disable threads. The default behavior is threads disabled.

```xml
<property>
  <name>fs.azure.rename.threads</name>
  <value>20</value>
</property>
```

### WASB Secure mode and configuration

WASB can operate in secure mode where the Storage access keys required to communicate with Azure storage does not have to
be in the same address space as the process using WASB. In this mode all interactions with Azure storage is performed using
SAS uris. There are two sub modes within the Secure mode, one is remote SAS key mode where the SAS keys are generated from
a remote process and local mode where SAS keys are generated within WASB. By default the SAS Key mode is expected to run in
Romote mode, however for testing purposes the local mode can be enabled to generate SAS keys in the same process as WASB.

To enable Secure mode following property needs to be set to true.

```xml
<property>
  <name>fs.azure.secure.mode</name>
  <value>true</value>
</property>
```

To enable SAS key generation locally following property needs to be set to true.

```xml
<property>
  <name>fs.azure.local.sas.key.mode</name>
  <value>true</value>
</property>
```

To use the remote SAS key generation mode, comma separated external REST services are expected to provided required SAS keys.
Following property can used to provide the end point to use for remote SAS Key generation:

```xml
<property>
  <name>fs.azure.cred.service.urls</name>
  <value>{URL}</value>
</property>
```

The remote service is expected to provide support for two REST calls ```{URL}/GET_CONTAINER_SAS``` and ```{URL}/GET_RELATIVE_BLOB_SAS```, for generating
container and relative blob sas keys. An example requests

```{URL}/GET_CONTAINER_SAS?storage_account=<account_name>&container=<container>&sas_expiry=<expiry period>&delegation_token=<delegation token>```
```{URL}/GET_CONTAINER_SAS?storage_account=<account_name>&container=<container>&relative_path=<relative path>&sas_expiry=<expiry period>&delegation_token=<delegation token>```

The service is expected to return a response in JSON format:

```json
{
  "responseCode" : 0 or non-zero <int>,
  "responseMessage" : relavant message on failure <String>,
  "sasKey" : Requested SAS Key <String>
}
```

### Authorization Support in WASB

Authorization support can be enabled in WASB using the following configuration:

```xml
<property>
  <name>fs.azure.authorization</name>
  <value>true</value>
</property>
```

The current implementation of authorization relies on the presence of an external service that can enforce
the authorization. The service is expected to be running on comma separated URLs provided by the following config.

```xml
<property>
  <name>fs.azure.authorization.remote.service.urls</name>
  <value>{URL}</value>
</property>
```

The remote service is expected to provide support for the following REST call: ```{URL}/CHECK_AUTHORIZATION```
An example request:
  ```{URL}/CHECK_AUTHORIZATION?wasb_absolute_path=<absolute_path>&operation_type=<operation type>&delegation_token=<delegation token>```

The service is expected to return a response in JSON format:

```json
{
    "responseCode" : 0 or non-zero <int>,
    "responseMessage" : relevant message on failure <String>,
    "authorizationResult" : true/false <boolean>
}
```

### Delegation token support in WASB

Delegation token support support can be enabled in WASB using the following configuration:

```xml
<property>
  <name>fs.azure.enable.kerberos.support</name>
  <value>true</value>
</property>
```

The current implementation of delegation token implementation relies on the presence of an external service instances that can generate and manage delegation tokens. The service is expected to be running on comma separated URLs provided by the following config.

```xml
<property>
  <name>fs.azure.delegation.token.service.urls</name>
  <value>{URL}</value>
</property>
```

The remote service is expected to provide support for the following REST call: ```{URL}?op=GETDELEGATIONTOKEN```, ```{URL}?op=RENEWDELEGATIONTOKEN``` and ```{URL}?op=CANCELDELEGATIONTOKEN```
An example request:
  ```{URL}?op=GETDELEGATIONTOKEN&renewer=<renewer>```
  ```{URL}?op=RENEWDELEGATIONTOKEN&token=<delegation token>```
  ```{URL}?op=CANCELDELEGATIONTOKEN&token=<delegation token>```

The service is expected to return a response in JSON format for GETDELEGATIONTOKEN request:

```json
{
    "Token" : {
        "urlString": URL string of delegation token.
    }
}
```
### chown behaviour when authorization is enabled in WASB

When authorization is enabled, only the users listed in the following configuration
are allowed to change the owning user of files/folders in WASB. The configuration
value takes a comma seperated list of user names who are allowed to perform chown.

```xml
<property>
  <name>fs.azure.chown.allowed.userlist</name>
  <value>user1,user2</value>
</property>
```

Caching of both SAS keys and Authorization responses can be enabled using the following setting:
The cache settings are applicable only when fs.azure.authorization is enabled.
The cache is maintained at a filesystem object level.
```
    <property>
      <name>fs.azure.authorization.caching.enable</name>
      <value>true</value>
    </property>
```

The maximum number of entries that that cache can hold can be customized using the following setting:
```
    <property>
      <name>fs.azure.authorization.caching.maxentries</name>
      <value>512</value>
    </property>
```

 The validity of an authorization cache-entry can be controlled using the following setting:
 Setting the value to zero disables authorization-caching.
 If the key is not specified, a default expiry duration of 5m takes effect.
 ```
    <property>
      <name>fs.azure.authorization.cacheentry.expiry.period</name>
      <value>5m</value>
    </property>
```

 The validity of a SASKey cache-entry can be controlled using the following setting.
 Setting the value to zero disables SASKey-caching.
 If the key is not specified, the default expiry duration specified in the sas-key request takes effect.
 ```
    <property>
      <name>fs.azure.saskey.cacheentry.expiry.period</name>
      <value>90d</value>
    </property>
```

 Use container saskey for access to all blobs within the container.
 Blob-specific saskeys are not used when this setting is enabled.
 This setting provides better performance compared to blob-specific saskeys.
 ```
    <property>
      <name>fs.azure.saskey.usecontainersaskeyforallaccess</name>
      <value>true</value>
    </property>
```

## Further Reading

* [Testing the Azure WASB client](testing_azure.html).
* MSDN article, [Understanding Block Blobs, Append Blobs, and Page Blobs](https://docs.microsoft.com/en-us/rest/api/storageservices/understanding-block-blobs--append-blobs--and-page-blobs)
