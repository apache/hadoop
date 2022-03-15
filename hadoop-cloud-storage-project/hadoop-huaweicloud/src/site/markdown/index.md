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

# OBSA: HuaweiCloud OBS Adapter for Hadoop Support

<!-- MACRO{toc|fromDepth=1|toDepth=3} -->

## Introduction

The `hadoop-huaweicloud` module provides support for integration with the
[HuaweiCloud Object Storage Service (OBS)](https://www.huaweicloud.com/en-us/product/obs.html).
This support comes via the JAR file `hadoop-huaweicloud.jar`.

## Features

* Read and write data stored in a HuaweiCloud OBS account.
* Reference file system paths using URLs using the `obs` scheme.
* Present a hierarchical file system view by implementing the standard Hadoop `FileSystem` interface.
* Support multipart upload for a large file.
* Can act as a source of data in a MapReduce job, or a sink.
* Uses HuaweiCloud OBS’s Java SDK with support for latest OBS features and authentication schemes.
* Tested for scale.

## Limitations

Partial or no support for the following operations :

* Symbolic link operations.
* Proxy users.
* File truncate.
* File concat.
* File checksum.
* File replication factor.
* Extended Attributes(XAttrs) operations.
* Snapshot operations.
* Storage policy.
* Quota.
* POSIX ACL.
* Delegation token operations.

##  Getting Started

### Packages

OBSA depends upon two JARs, alongside `hadoop-common` and its dependencies.

* `hadoop-huaweicloud` JAR.
* `esdk-obs-java` JAR.

The versions of `hadoop-common` and `hadoop-huaweicloud` must be identical.

To import the libraries into a Maven build, add `hadoop-huaweicloud` JAR to the
build dependencies; it will pull in a compatible `esdk-obs-java` JAR.

The `hadoop-huaweicloud` JAR *does not* declare any dependencies other than that
dependencies unique to it, the OBS SDK JAR. This is simplify excluding/tuning
Hadoop dependency JARs in downstream applications. The `hadoop-client` or
`hadoop-common` dependency must be declared.


```xml
<properties>
 <!-- Your exact Hadoop version here-->
  <hadoop.version>3.4.0</hadoop.version>
</properties>

<dependencies>
  <dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-client</artifactId>
    <version>${hadoop.version}</version>
  </dependency>
  <dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-huaweicloud</artifactId>
    <version>${hadoop.version}</version>
  </dependency>
</dependencies>
```
### Accessing OBS URLs
Before access a URL, OBS implementation classes of Filesystem/AbstractFileSystem and
a region endpoint where a bucket is located shoud be configured as follows:
```xml
<property>
  <name>fs.obs.impl</name>
  <value>org.apache.hadoop.fs.obs.OBSFileSystem</value>
  <description>The OBS implementation class of the Filesystem.</description>
</property>

<property>
  <name>fs.AbstractFileSystem.obs.impl</name>
  <value>org.apache.hadoop.fs.obs.OBS</value>
  <description>The OBS implementation class of the AbstractFileSystem.</description>
</property>

<property>
  <name>fs.obs.endpoint</name>
  <value>obs.region.myhuaweicloud.com</value>
  <description>OBS region endpoint where a bucket is located.</description>
</property>
```

OBS URLs can then be accessed as follows:

```
obs://<bucket_name>/path
```
The scheme `obs` identifies a URL on a Hadoop-compatible file system `OBSFileSystem`
backed by HuaweiCloud OBS.
For example, the following
[FileSystem Shell](../hadoop-project-dist/hadoop-common/FileSystemShell.html)
commands demonstrate access to a bucket named `mybucket`.
```bash
hadoop fs -mkdir obs://mybucket/testDir

hadoop fs -put testFile obs://mybucket/testDir/testFile

hadoop fs -cat obs://mybucket/testDir/testFile
test file content
```

For details on how to create a bucket, see
[**Help Center > Object Storage Service > Getting Started> Basic Operation Procedure**](https://support.huaweicloud.com/intl/en-us/qs-obs/obs_qs_0003.html)

### Authenticating with OBS
Except when interacting with public OBS buckets, the OBSA client
needs the credentials needed to interact with buckets.
The client supports multiple authentication mechanisms. The simplest authentication mechanisms is
to provide OBS access key and secret key as follows.
```xml
<property>
  <name>fs.obs.access.key</name>
  <description>OBS access key.
   Omit for provider-based authentication.</description>
</property>

<property>
  <name>fs.obs.secret.key</name>
  <description>OBS secret key.
   Omit for provider-based authentication.</description>
</property>
```

**Do not share access key, secret key, and session token. They must be kept secret.**

Custom implementations
of `com.obs.services.IObsCredentialsProvider` (see [**Creating an Instance of ObsClient**](https://support.huaweicloud.com/intl/en-us/sdk-java-devg-obs/en-us_topic_0142815570.html)) or
`org.apache.hadoop.fs.obs.BasicSessionCredential` may also be used for authentication.

```xml
<property>
  <name>fs.obs.security.provider</name>
  <description>
    Class name of security provider class which implements
    com.obs.services.IObsCredentialsProvider, which will
    be used to construct an OBS client instance as an input parameter.
  </description>
</property>

<property>
  <name>fs.obs.credentials.provider</name>
  <description>
    lass nameCof credential provider class which implements
    org.apache.hadoop.fs.obs.BasicSessionCredential,
    which must override three APIs: getOBSAccessKeyId(),
    getOBSSecretKey(), and getSessionToken().
  </description>
</property>
```

## General OBSA Client Configuration

All OBSA client options are configured with options with the prefix `fs.obs.`.

```xml
<property>
  <name>fs.obs.connection.ssl.enabled</name>
  <value>false</value>
  <description>Enable or disable SSL connections to OBS.</description>
</property>

<property>
  <name>fs.obs.connection.maximum</name>
  <value>1000</value>
  <description>Maximum number of simultaneous connections to OBS.</description>
</property>

<property>
  <name>fs.obs.connection.establish.timeout</name>
  <value>120000</value>
  <description>Socket connection setup timeout in milliseconds.</description>
</property>

<property>
  <name>fs.obs.connection.timeout</name>
  <value>120000</value>
  <description>Socket connection timeout in milliseconds.</description>
</property>

<property>
  <name>fs.obs.idle.connection.time</name>
  <value>30000</value>
  <description>Socket idle connection time.</description>
</property>

<property>
  <name>fs.obs.max.idle.connections</name>
  <value>1000</value>
  <description>Maximum number of socket idle connections.</description>
</property>

<property>
  <name>fs.obs.socket.send.buffer</name>
  <value>256 * 1024</value>
  <description>Socket send buffer to be used in OBS SDK. Represented in bytes.</description>
</property>

<property>
  <name>fs.obs.socket.recv.buffer</name>
  <value>256 * 1024</value>
  <description>Socket receive buffer to be used in OBS SDK. Represented in bytes.</description>
</property>

<property>
  <name>fs.obs.threads.keepalivetime</name>
  <value>60</value>
  <description>Number of seconds a thread can be idle before being
    terminated in thread pool.</description>
</property>

<property>
  <name>fs.obs.threads.max</name>
  <value>20</value>
  <description> Maximum number of concurrent active (part)uploads,
    which each use a thread from thread pool.</description>
</property>

<property>
  <name>fs.obs.max.total.tasks</name>
  <value>20</value>
  <description>Number of (part)uploads allowed to the queue before
    blocking additional uploads.</description>
</property>

<property>
  <name>fs.obs.delete.threads.max</name>
  <value>20</value>
  <description>Max number of delete threads.</description>
</property>

<property>
  <name>fs.obs.multipart.size</name>
  <value>104857600</value>
  <description>Part size for multipart upload.
  </description>
</property>

<property>
  <name>fs.obs.multiobjectdelete.maximum</name>
  <value>1000</value>
  <description>Max number of objects in one multi-object delete call.
  </description>
</property>

<property>
  <name>fs.obs.fast.upload.buffer</name>
  <value>disk</value>
  <description>Which buffer to use. Default is `disk`, value may be
    `disk` | `array` | `bytebuffer`.
  </description>
</property>

<property>
  <name>fs.obs.buffer.dir</name>
  <value>dir1,dir2,dir3</value>
  <description>Comma separated list of directories that will be used to buffer file
    uploads to. This option takes effect only when the option 'fs.obs.fast.upload.buffer'
    is set to 'disk'.
  </description>
</property>

<property>
  <name>fs.obs.fast.upload.active.blocks</name>
  <value>4</value>
  <description>Maximum number of blocks a single output stream can have active
    (uploading, or queued to the central FileSystem instance's pool of queued
    operations).
  </description>
</property>

<property>
  <name>fs.obs.readahead.range</name>
  <value>1024 * 1024</value>
  <description>Bytes to read ahead during a seek() before closing and
  re-opening the OBS HTTP connection. </description>
</property>

<property>
  <name>fs.obs.read.transform.enable</name>
  <value>true</value>
  <description>Flag indicating if socket connections can be reused by
    position read. Set `false` only for HBase.</description>
</property>

<property>
  <name>fs.obs.list.threads.core</name>
  <value>30</value>
  <description>Number of core list threads.</description>
</property>

<property>
  <name>fs.obs.list.threads.max</name>
  <value>60</value>
  <description>Maximum number of list threads.</description>
</property>

<property>
  <name>fs.obs.list.workqueue.capacity</name>
  <value>1024</value>
  <value>Capacity of list work queue.</value>
</property>

<property>
  <name>fs.obs.list.parallel.factor</name>
  <value>30</value>
  <description>List parallel factor.</description>
</property>

<property>
  <name>fs.obs.trash.enable</name>
  <value>false</value>
  <description>Switch for the fast delete.</description>
</property>

<property>
  <name>fs.obs.trash.dir</name>
  <description>The fast delete recycle directory.</description>
</property>

<property>
  <name>fs.obs.block.size</name>
  <value>128 * 1024 * 1024</value>
  <description>Default block size for OBS FileSystem.
  </description>
</property>
```

## Testing the hadoop-huaweicloud Module
The `hadoop-huaweicloud` module includes a full suite of unit tests.
Most of the tests will run against the HuaweiCloud OBS. To run these
tests, please create `src/test/resources/auth-keys.xml` with OBS account
information mentioned in the above sections and the following properties.

```xml
<property>
    <name>fs.contract.test.fs.obs</name>
    <value>obs://obsfilesystem-bucket</value>
</property>
```