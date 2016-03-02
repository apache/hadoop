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

* [Introduction](#Introduction)
* [Features](#Features)
* [Limitations](#Limitations)
* [Usage](#Usage)
    * [Concepts](#Concepts)
    * [Configuring Credentials](#Configuring_Credentials)
    * [Page Blob Support and Configuration](#Page_Blob_Support_and_Configuration)
    * [Atomic Folder Rename](#Atomic_Folder_Rename)
    * [Accessing wasb URLs](#Accessing_wasb_URLs)
    * [Append API Support and Configuration](#Append_API_Support_and_Configuration)
* [Testing the hadoop-azure Module](#Testing_the_hadoop-azure_Module)

## <a name="Introduction" />Introduction

The hadoop-azure module provides support for integration with
[Azure Blob Storage](http://azure.microsoft.com/en-us/documentation/services/storage/).
The built jar file, named hadoop-azure.jar, also declares transitive dependencies
on the additional artifacts it requires, notably the
[Azure Storage SDK for Java](https://github.com/Azure/azure-storage-java).

To make it part of Apache Hadoop's default classpath, simply make sure that
HADOOP_OPTIONAL_TOOLS in hadoop-env.sh has 'hadoop-azure' in the list.

## <a name="Features" />Features

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

## <a name="Limitations" />Limitations

* File owner and group are persisted, but the permissions model is not enforced.
  Authorization occurs at the level of the entire Azure Blob Storage account.
* File last access time is not tracked.

## <a name="Usage" />Usage

### <a name="Concepts" />Concepts

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

### <a name="Configuring_Credentials" />Configuring Credentials

Usage of Azure Blob Storage requires configuration of credentials.  Typically
this is set in core-site.xml.  The configuration property name is of the form
`fs.azure.account.key.<account name>.blob.core.windows.net` and the value is the
access key.  **The access key is a secret that protects access to your storage
account.  Do not share the access key (or the core-site.xml file) with an
untrusted party.**

For example:

    <property>
      <name>fs.azure.account.key.youraccount.blob.core.windows.net</name>
      <value>YOUR ACCESS KEY</value>
    </property>

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

```
% hadoop credential create fs.azure.account.key.youraccount.blob.core.windows.net -value 123
    -provider localjceks://file/home/lmccay/wasb.jceks
```

###### configure core-site.xml or command line system property

```
<property>
  <name>hadoop.security.credential.provider.path</name>
  <value>localjceks://file/home/lmccay/wasb.jceks</value>
  <description>Path to interrogate for protected credentials.</description>
</property>
```

###### distcp

```
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

### <a name="Page_Blob_Support_and_Configuration" />Page Blob Support and Configuration

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

    <property>
      <name>fs.azure.page.blob.dir</name>
      <value>/hbase/WALs,/hbase/oldWALs,/data/mypageblobfiles</value>
    </property>

You can set this to simply / to make all files page blobs.

The configuration option `fs.azure.page.blob.size` is the default initial
size for a page blob. It must be 128MB or greater, and no more than 1TB,
specified as an integer number of bytes.

The configuration option `fs.azure.page.blob.extension.size` is the page blob
extension size.  This defines the amount to extend a page blob if it starts to
get full.  It must be 128MB or greater, specified as an integer number of bytes.

### <a name="Atomic_Folder_Rename" />Atomic Folder Rename

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

    <property>
      <name>fs.azure.atomic.rename.dir</name>
      <value>/hbase,/data</value>
    </property>

### <a name="Accessing_wasb_URLs" />Accessing wasb URLs

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

    > hadoop fs -mkdir wasb://yourcontainer@youraccount.blob.core.windows.net/testDir

    > hadoop fs -put testFile wasb://yourcontainer@youraccount.blob.core.windows.net/testDir/testFile

    > hadoop fs -cat wasbs://yourcontainer@youraccount.blob.core.windows.net/testDir/testFile
    test file content

It's also possible to configure `fs.defaultFS` to use a `wasb` or `wasbs` URL.
This causes all bare paths, such as `/testDir/testFile` to resolve automatically
to that file system.

### <a name="Append_API_Support_and_Configuration" />Append API Support and Configuration

The Azure Blob Storage interface for Hadoop has optional support for Append API for
single writer by setting the configuration `fs.azure.enable.append.support` to true.

For Example:

    <property>
      <name>fs.azure.enable.append.support</name>
      <value>true</value>
    </property>

It must be noted Append support in Azure Blob Storage interface DIFFERS FROM HDFS SEMANTICS. Append
support does not enforce single writer internally but requires applications to guarantee this semantic.
It becomes a responsibility of the application either to ensure single-threaded handling for a particular
file path, or rely on some external locking mechanism of its own.  Failure to do so will result in
unexpected behavior.

## <a name="Testing_the_hadoop-azure_Module" />Testing the hadoop-azure Module

The hadoop-azure module includes a full suite of unit tests.  Most of the tests
will run without additional configuration by running `mvn test`.  This includes
tests against mocked storage, which is an in-memory emulation of Azure Storage.

A selection of tests can run against the
[Azure Storage Emulator](http://msdn.microsoft.com/en-us/library/azure/hh403989.aspx)
which is a high-fidelity emulation of live Azure Storage.  The emulator is
sufficient for high-confidence testing.  The emulator is a Windows executable
that runs on a local machine.

To use the emulator, install Azure SDK 2.3 and start the storage emulator.  Then,
edit `src/test/resources/azure-test.xml` and add the following property:

    <property>
      <name>fs.azure.test.emulator</name>
      <value>true</value>
    </property>

There is a known issue when running tests with the emulator.  You may see the
following failure message:

    com.microsoft.windowsazure.storage.StorageException: The value for one of the HTTP headers is not in the correct format.

To resolve this, restart the Azure Emulator.  Ensure it v3.2 or later.

It's also possible to run tests against a live Azure Storage account by saving a
file to `src/test/resources/azure-auth-keys.xml` and setting
`fs.azure.test.account.name` to the name of the storage account.

For example:

    <?xml version="1.0"?>
    <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
    <configuration>
      <property>
        <name>fs.azure.account.key.youraccount.blob.core.windows.net</name>
        <value>YOUR ACCESS KEY</value>
      </property>

      <property>
        <name>fs.azure.test.account.name</name>
        <value>youraccount</value>
      </property>
    </configuration>

To run contract tests add live Azure Storage account by saving a
file to `src/test/resources/azure-auth-keys.xml`.
For example:

    <?xml version="1.0"?>
    <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
    <configuration>
      <property>
        <name>fs.contract.test.fs.wasb</name>
        <value>wasb://{CONTAINERNAME}@{ACCOUNTNAME}.blob.core.windows.net</value>
        <description>The name of the azure file system for testing.</description>
      </property>

      <property>
        <name>fs.azure.account.key.{ACCOUNTNAME}.blob.core.windows.net</name>
        <value>{ACCOUNTKEY}</value>
      </property>
    </configuration>

DO NOT ADD azure-auth-keys.xml TO REVISION CONTROL.  The keys to your Azure
Storage account are a secret and must not be shared.
