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
* [Testing the hadoop-azure Module](#Testing_the_hadoop-azure_Module)

## <a name="Introduction" />Introduction

The hadoop-azure module provides support for integration with
[Azure Blob Storage](http://azure.microsoft.com/en-us/documentation/services/storage/).
The built jar file, named hadoop-azure.jar, also declares transitive dependencies
on the additional artifacts it requires, notably the
[Azure Storage SDK for Java](https://github.com/Azure/azure-storage-java).

## <a name="Features" />Features

* Read and write data stored in an Azure Blob Storage account.
* Present a hierarchical file system view by implementing the standard Hadoop
  [`FileSystem`](../api/org/apache/hadoop/fs/FileSystem.html) interface.
* Supports configuration of multiple Azure Blob Storage accounts.
* Supports both page blobs (suitable for most use cases, such as MapReduce) and
  block blobs (suitable for continuous write use cases, such as an HBase
  write-ahead log).
* Reference file system paths using URLs using the `wasb` scheme.
* Also reference file system paths using URLs with the `wasbs` scheme for SSL
  encrypted access.
* Can act as a source of data in a MapReduce job, or a sink.
* Tested on both Linux and Windows.
* Tested at scale.

## <a name="Limitations" />Limitations

* The append operation is not implemented.
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

In many Hadoop clusters, the core-site.xml file is world-readable.  If it's
undesirable for the access key to be visible in core-site.xml, then it's also
possible to configure it in encrypted form.  An additional configuration property
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

Page blobs can be used for other purposes beyond just HBase log files though.
Page blobs can be up to 1TB in size, larger than the maximum 200GB size for block
blobs.

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

It's also possible to run tests against a live Azure Storage account by adding
credentials to `src/test/resources/azure-test.xml` and setting
`fs.azure.test.account.name` to the name of the storage account.

For example:

    <property>
      <name>fs.azure.account.key.youraccount.blob.core.windows.net</name>
      <value>YOUR ACCESS KEY</value>
    </property>

    <property>
      <name>fs.azure.test.account.name</name>
      <value>youraccount</value>
    </property>
