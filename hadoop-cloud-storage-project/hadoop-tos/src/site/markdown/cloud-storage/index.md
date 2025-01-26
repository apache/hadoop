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

# Integration of VolcanoEngine TOS in Hadoop

## Overview

TOS is the object storage service of Volcano Engine, which is a cloud vendor launched by ByteDance.
Hadoop-tos is a connector between computing systems and underlying storage. For systems like
Hadoop MR, Hive(both mr and tez) and Spark, hadoop-tos helps them use TOS as the underlying storage
system instead of HDFS.

## Quick Start

In quick start, we will use hadoop shell command to access a tos bucket.

### Requirements

1. A Volcano Engine account. Use the account to create a TOS bucket.
2. A dev environment that can access TOS. E.g. a local server or a volcano engine cloud server.
3. Install hadoop to the dev environment. Hadoop is installed at `$HADOOP_HOME`.

### Usage
* Compile hadoop-tos bundle tar. The hadoop-tos bundle is not packaged in hadoop final tar file.
So we have to compile it manually. Download the hadoop project, and build it with command below.
```bash
mvn package -DskipTests -pl org.apache.hadoop:hadoop-tos
```

* Un-tar the hadoop-tos tar file. The bundle tar file is placed at
`$HADOOP_HOME/hadoop-cloud-storage-project/hadoop-tos/target/hadoop-tos-bundle-{VERSION}.tar.gz`.
```bash
tar xvf hadoop-tos-bundle-{VERSION}.tar.gz
```

* Copy hadoop-tos related jars to hdfs lib path. The hdfs lib path is
`$HADOOP_HOME/share/hadoop/hdfs/lib`. The jars above include dependencies of hadoop-tos module. If we
already have these dependencies, copy `hadoop-tos-{VERSION}.jar` should be enough. Remember copying
to all hadoop nodes.
```bash
cp hadoop-tos-bundle-{VERSION}/lib/* $HADOOP_HOME/share/hadoop/hdfs/lib/
```

* Configure properties below.

```xml
<properties>
  <property>
    <name>fs.defaultFS</name>
    <value>tos://{your_bucket_name}/</value>
    <description>
      The name of the default file system. Make it your tos bucket.
    </description>
  </property>

  <property>
    <name>fs.tos.endpoint</name>
    <value></value>
    <description>
      Object storage endpoint to connect to, which should include both region and object domain name.
      e.g. 'fs.tos.endpoint'='tos-cn-beijing.volces.com'.
    </description>
  </property>

  <property>
    <name>fs.tos.impl</name>
    <value>org.apache.hadoop.fs.tosfs.TosFileSystem</value>
    <description>
      The implementation class of the tos FileSystem.
    </description>
  </property>

  <property>
    <name>fs.AbstractFileSystem.tos.impl</name>
    <value>org.apache.hadoop.fs.tosfs.TosFS</value>
    <description>
      The implementation class of the tos AbstractFileSystem.
    </description>
  </property>

  <property>
    <name>fs.tos.access-key-id</name>
    <value></value>
    <description>
      The access key of volcano engine's user or role.
    </description>
  </property>

  <property>
    <name>fs.tos.secret-access-key</name>
    <value></value>
    <description>
      The secret key of the access key specified by 'fs.tos.access-key-id'.
    </description>
  </property>
</properties>
```

* Use hadoop shell command to access TOS.

```bash
# 1. List root dir.
hadoop fs -ls /

# 2. Make directory.
hadoop fs -mkdir /hadoop-tos

# 3. Write and read.
echo "hello tos." > hello.txt
hadoop fs -put hello.txt /hadoop-tos/
hadoop fs -cat /hadoop-tos/hello.txt

# 4. Delete file and directory.
hadoop fs -rm -r /hadoop-tos/
```

## Introduction

This is a brief introduction of hadoop-tos design and basic functions. The following contents are
based on flat mode by default. The differences between hierarchy mode will be explained at the end
of each section.

### TOS

TOS is the object storage service of Volcano Engine. It is similar to ASW S3, Azure Blob Storage
and Aliyun OSS, and has some unique features, such as object fast copy, object fast rename,
CRC32C checksum etc. Learn more details about TOS from: https://www.volcengine.com/product/TOS.

TOS has 2 modes: the flat mode and the hierarchy mode. In flat mode, there are no directories,
all objects are files indexed by the object names. User can use 'slash' in the object name to
logically divide objects into different "directories", though the "directories divided by the slash"
are not real. Cleanup a logic directory is to clean all the files with "directory path" as prefix.

In hierarchy mode, there are directories and files. A directory object is the object whose name
ends with slash. All objects start with the directory object name are the directory object's
consecutive objects, and together they form a directory tree. A directory object can't contain
any data. Delete or rename a directory object will clean or rename all objects under the
directory tree atomically.

TOS has some distinctive features that are very useful in bigdata scenarios.

1. The fast copy feature enables users to duplicate objects without copying data, even huge objects
   could be copied within tens of milliseconds.
2. The fast rename feature enables users to rename one object with a new name without copying data.
3. TOS supports CRC32C checksum. It enables user to compare files' checksums between HDFS and TOS.

### Directory and file

This section illustrates how hadoop-tos transforms TOS to a hadoop FileSystem. TOS requires object's
name must not start with slash, must not contain consecutive slash and must not be empty. Here is
the transformation rules.

* Object name is divided by slash to form hierarchy.
* An object whose name ends with slash is a directory.
* An object whose name doesn't end with slash is a file.
* A file's parents are directories, no matter whether the parent exists or not.

For example, supposing we have 2 objects "user/table/" and "user/table/part-0". The first object
is mapped to "/user/table" in hadoop and is a directory. The second object is mapped to
"/user/table/part-0" as a file. The non-existent object "user/" is mapped to "/user" as a directory
because it's the parent of file "/user/table/part-0".

| Object name       | Object existence | FileSystem path    | FileSystem Type |
|-------------------|------------------|--------------------|-----------------|
| user/table/       | yes              | /user/table        | Directory       |
| user/table/part-0 | yes              | /user/table/part-0 | File            |
| user/             | no               | /user              | Directory       |

The FileSystem requirements above are not enforced rules in flat mode, users can construct
cases violating the requirements above. For example, creating a file with its parent is a file. In
hierarchy mode, the requirements are enforced rules controlled by TOS service, so there won't be
semantic violations.

### List, Rename and Delete

List, rename and delete are costly operations in flat mode. Since the namespace is flat, to list
a directory, the client needs to scan all objects with directory as the prefix and filter with
delimiter. For rename and delete directory, the client needs to first list the directory to get all
objects and then rename or delete objects one by one. So they are not atomic operations and costs a
lot comparing to hdfs.

The idiosyncrasies of hierarchy mode is supporting directory. So it can list very fast and
support atomic rename and delete directory. Rename or delete failure in flat mode may leave
the bucket in an inconsistent state, the hierarchy mode won't have this problem.

### Read and write file

The read behaviour in hadoop-tos is very like reading an HDFS file. The challenge is how to keep the
input stream consistent with object. If the object is changed after we open the file, the input
stream should fail. This is implemented by saving the file checksum when open file. If the
file is changed while reading, the input stream will compare the checksum and trigger an exception.

The write behaviour in hadoop-tos is slightly different from hdfs. Firstly, the append interface
is not supported. Secondly, the file is not visible until it is successfully closed. Finally,
when 2 clients try to write one file, the last client to close the file will override the previous
one.

Both read and write has many performance optimizations. E.g. range read, connection reuse, local
write buffer, put for small files, multipart-upload for big files etc.

### Permissions

TOS permission model is different from hadoop filesystem permission model. TOS supports permissions
based on IAM, Bucket Policy, Bucket and Object ACL, while hadoop filesystem permission model uses
mode and acl. There is no way to mapped tos permission to hadoop filesystem permission, so we have
to use fake permissions in TosFileSystem and TosFS. Users can read and change the filesystem
permissions, they can only be seen but not effective. Permission control eventually depends on TOS
permission model.

### Times

Hadoop-tos supports last modified time and doesn't support access time. For files, the last modified
time is the object's modified time. For directories, if the directory object doesn't exist, the last
modified time is the current system time. If the directory object exists, the last modified time is
the object's modify time when `getFileStatus` and current system time when `listStatus`.

### File checksum

TOS supports CRC64ECMA checksum by default, it is mapped to Hadoop FileChecksum. We can
retrieve it by calling `FileSystem#getFileChecksum`.
To be compatible with HDFS, TOS provides optional CRC32C checksum. When we distcp
between HDFS and TOS, we can rely on distcp checksum mechanisms to keep data consistent.
To use CRC32C, configure keys below.
```xml
<configuration>
   <property>
      <name>fs.tos.checksum.enabled</name>
      <value>true</value>
   </property>
   <property>
      <name>fs.tos.checksum-algorithm</name>
      <value>COMPOSITE-CRC32C</value>
   </property>
   <property>
      <name>fs.tos.checksum-type</name>
      <value>CRC32C</value>
   </property>
</configuration>
```

### Credential

TOS client uses access key id and secret access key to authenticate with tos service. There are 2
ways to configure them. First is adding to hadoop configuration, such as adding to core-site.xml or
configuring through `-D` parameter. The second is setting environment variable, hadoop-tos will
search for environment variables automatically.

To configure ak, sk in hadoop configuration, using the key below.

```xml

<configuration>
  <!--Set global ak, sk for all buckets.-->
  <property>
    <name>fs.tos.access-key-id</name>
    <value></value>
    <description>
      The accessKey key to access the tos object storage.
    </description>
  </property>
  <property>
    <name>fs.tos.secret-access-key</name>
    <value></value>
    <description>
      The secret access key to access the object storage.
    </description>
  </property>
  <property>
    <name>fs.tos.session-token</name>
    <value></value>
    <description>
      The session token to access the object storage.
    </description>
  </property>

  <!--Set ak, sk for specified bucket. It has higher priority then the global keys.-->
  <property>
    <name>fs.tos.bucket.{bucket_name}.access-key-id</name>
    <value></value>
    <description>
      The access key to access the object storage for the configured bucket.
    </description>
  </property>
  <property>
    <name>fs.tos.bucket.{bucket_name}.secret-access-key</name>
    <value></value>
    <description>
      The secret access key to access the object storage for the configured bucket.
    </description>
  </property>
  <property>
    <name>fs.tos.bucket.{bucket_name}.session-token</name>
    <value></value>
    <description>
      The session token to access the object storage for the configured bucket.
    </description>
  </property>
</configuration>
```

The ak, sk in environment variables have the top priority and automatically fall back to hadoop
configuration if not found. The priority could be changed
by `fs.tos.credential.provider.custom.classes`.

### Committer

Hadoop-tos provides MapReduce job committer for better performance. By default, hadoop uses FileOutputCommitter
which will rename files many times. First when tasks commit, files will be renamed to a output path.
Then when job commits, the files will be renamed from output path to the final path. When using hdfs,
rename is not a problem because it only changes meta. But in TOS, by default the rename is implemented
as copy and delete and costs a lot.

TOS committer is an optimized implementation for object storage. When task commits, it won't complete
multipart-upload for the files, instead it write pending set files including all the information to
complete multipart-upload. Then when job commits, it reads all the pending files and completes all
multipart-uploads.

An alternative way is turning on TOS renameObject switch and still use FileOutputFormat. Objects are
renamed with only meta change. The performance is slightly slower than hadoop-tos committer, because
when using TOS rename objects, each file is committed first and then renamed to the final path. There
is 1 commit request and 2 rename request to TOS. But with hadoop-tos committer, the object is
postponing committed and there is no rename request overhead.

To enable hadoop-tos committer, configure the key value below.
```xml
<configurations>
   <!-- mapreduce v1 -->
   <property>
      <name>mapred.output.committer.class</name>
      <value>org.apache.hadoop.fs.tosfs.Committer</value>
   </property>
   <!-- mapreduce v2 -->
   <property>
      <name>mapreduce.outputcommitter.factory.scheme.tos</name>
      <value>org.apache.hadoop.fs.tosfs.commit.CommitterFactory</value>
   </property>
</configurations>
```

To enable tos objectRename, first turn on the object rename switch on tos, then configure the key
value below.
```xml
<configurations>
   <property>
      <name>fs.tos.rename.enabled</name>
      <value>true</value>
   </property>
</configurations>
```

## Properties Summary

| properties                                | description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | default value                                                                                                                                     | required |
|-------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| fs.tos.access-key-id                      | The accessKey key to access the tos object storage                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | NONE                                                                                                                                              | YES      |
| fs.tos.secret-access-key                  | The secret access key to access the object storage                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | NONE                                                                                                                                              | YES      |
| fs.tos.session-token                      | The session token to access the object storage                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | NONE                                                                                                                                              | NO       |
| fs.%s.endpoint                            | Object storage endpoint to connect to, which should include both region and object domain name.                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | NONE                                                                                                                                              | NO       |
| fs.%s.region                              | The region of the object storage, e.g. fs.tos.region. Parsing template "fs.%s.endpoint" to know the region.                                                                                                                                                                                                                                                                                                                                                                                                                                                            | NONE                                                                                                                                              | NO       |
| fs.tos.bucket.%s.access-key-id            | The access key to access the object storage for the configured bucket, where %s is the bucket name.                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | NONE                                                                                                                                              | NO       |
| fs.tos.bucket.%s.secret-access-key        | The secret access key to access the object storage for the configured bucket, where %s is the bucket name                                                                                                                                                                                                                                                                                                                                                                                                                                                              | NONE                                                                                                                                              | NO       |
| fs.tos.bucket.%s.session-token            | The session token to access the object storage for the configured bucket, where %s is the bucketname                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | NONE                                                                                                                                              | NO       |
| fs.tos.credentials.provider               | Default credentials provider chain that looks for credentials in this order: SimpleCredentialsProvider,EnvironmentCredentialsProvider                                                                                                                                                                                                                                                                                                                                                                                                                                  | org.apache.hadoop.fs.tosfs.object.tos.auth.DefaultCredentialsProviderChain                                                                        | NO       |
| fs.tos.credential.provider.custom.classes | User customized credential provider classes, separate provider class name with comma if there are multiple providers.                                                                                                                                                                                                                                                                                                                                                                                                                                                  | org.apache.hadoop.fs.tosfs.object.tos.auth.EnvironmentCredentialsProvider,org.apache.hadoop.fs.tosfs.object.tos.auth.SimpleCredentialsProvider    | NO       |
| fs.tos.credentials.provider               | Default credentials provider chain that looks for credentials in this order: SimpleCredentialsProvider,EnvironmentCredentialsProvider.                                                                                                                                                                                                                                                                                                                                                                                                                                 | org.apache.hadoop.fs.tosfs.object.tos.auth.DefaultCredentialsProviderChain                                                                        | NONE     |
| fs.tos.credential.provider.custom.classes | User customized credential provider classes, separate provider class name with comma if there are multiple providers.                                                                                                                                                                                                                                                                                                                                                                                                                                                  | org.apache.hadoop.fs.tosfs.object.tos.auth.EnvironmentCredentialsProvider,org.apache.hadoop.fs.tosfs.object.tos.auth.SimpleCredentialsProvider    | NONE     |
| fs.tos.http.maxConnections                | The maximum number of connections to the TOS service that a client can create.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | 1024                                                                                                                                              | NONE     |
| fs.tos.http.idleConnectionTimeMills       | The time that a connection thread can be in idle state, larger than which the thread will be terminated.                                                                                                                                                                                                                                                                                                                                                                                                                                                               | 60000                                                                                                                                             | NONE     |
| fs.tos.http.connectTimeoutMills           | The connect timeout that the tos client tries to connect to the TOS service.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           | 10000                                                                                                                                             | NONE     |
| fs.tos.http.readTimeoutMills              | The reading timeout when reading data from tos. Note that it is configured for the tos client sdk, not hadoop-tos.                                                                                                                                                                                                                                                                                                                                                                                                                                                     | 30000                                                                                                                                             | NONE     |
| fs.tos.http.writeTimeoutMills             | The writing timeout when uploading data to tos. Note that it is configured for the tos client sdk, not hadoop-tos.                                                                                                                                                                                                                                                                                                                                                                                                                                                     | 30000                                                                                                                                             | NONE     |
| fs.tos.http.enableVerifySSL               | Enables SSL connections to TOS or not.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | true                                                                                                                                              | NONE     |
| fs.tos.http.dnsCacheTimeMinutes           | The timeout (in minutes) of the dns cache used in tos client.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | 0                                                                                                                                                 | NONE     |
| fs.tos.rmr.server.enabled                 | Used for directory bucket, whether enable recursive delete capability in TOS server, which will atomic delete all objects under given dir(inclusive), otherwise the client will list all sub objects, and then send batch delete request to TOS to delete dir.                                                                                                                                                                                                                                                                                                         | false                                                                                                                                             | NONE     |
| fs.tos.rmr.client.enabled                 | If fs.tos.rmr.client.enabled is true, client will list all objects under the given dir and delete them by batch. Set value with true will use the recursive delete capability of TOS SDK, otherwise will delete object one by one via preorder tree walk.                                                                                                                                                                                                                                                                                                              | true                                                                                                                                              | NONE     |
| fs.tos.user.agent.prefix                  | The prefix will be used as the product name in TOS SDK. The final user agent pattern is '{prefix}/TOS_FS/{hadoop tos version}'.                                                                                                                                                                                                                                                                                                                                                                                                                                        | HADOOP-TOS                                                                                                                                        | NONE     |
| fs.tos.max-drain-bytes                    | The threshold indicates whether reuse the socket connection to optimize read performance during closing tos object inputstream of get object. If the remaining bytes is less than max drain bytes during closing the inputstream, will just skip the bytes instead of closing the socket connection.                                                                                                                                                                                                                                                                   | 1024 * 1024L                                                                                                                                      | NONE     |
| fs.tos.client.disable.cache               | Whether disable the tos http client cache in the current JVM.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | false                                                                                                                                             | NONE     |
| fs.tos.batch.delete.objects-count         | The batch size when deleting the objects in batches.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | 1000                                                                                                                                              | NONE     |
| fs.tos.batch.delete.max-retries           | The maximum retry times when deleting objects in batches failed.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | 20                                                                                                                                                | NONE     |
| fs.tos.batch.delete.retry-codes           | The codes from TOS deleteMultiObjects response, client will resend the batch delete request to delete the failed keys again if the response only contains these codes, otherwise won't send request anymore.                                                                                                                                                                                                                                                                                                                                                           | ExceedAccountQPSLimit,ExceedAccountRateLimit,ExceedBucketQPSLimit,ExceedBucketRateLimit,InternalError,ServiceUnavailable,SlowDown,TooManyRequests | NONE     |
| fs.tos.batch.delete.retry.interval        | The retry interval (in milliseconds) when deleting objects in batches failed.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | 1000                                                                                                                                              | NONE     |
| fs.tos.list.objects-count                 | The batch size of listing object per request for the given object storage, such as listing a directory, searching for all objects whose path starts with the directory path, and returning them as a list.                                                                                                                                                                                                                                                                                                                                                             | 1000                                                                                                                                              | NONE     |
| fs.tos.request.max.retry.times            | The maximum retry times of sending request via TOS client, client will resend the request if got retryable exceptions, e.g. SocketException, UnknownHostException, SSLException, InterruptedException, SocketTimeoutException, or got TOO_MANY_REQUESTS, INTERNAL_SERVER_ERROR http codes.                                                                                                                                                                                                                                                                             | 20                                                                                                                                                | NONE     |
| fs.tos.fast-fail-409-error-codes          | The fast-fail error codes means the error cannot be solved by retrying the request. TOS client won't retry the request if receiving a 409 http status code and if the error code is in the configured non-retryable error code list.                                                                                                                                                                                                                                                                                                                                   | 0026-00000013,0026-00000020,0026-00000021,0026-00000025,0026-00000026,0026-00000027 ,0017-00000208,0017-00000209                                  | NONE     |
| fs.tos.inputstream.max.retry.times        | The maximum retry times of reading object content via TOS client, client will resend the request to create a new input stream if getting unexpected end of stream error during reading the input stream.                                                                                                                                                                                                                                                                                                                                                               | 5                                                                                                                                                 | NONE     |
| fs.tos.crc.check.enable                   | Enable the crc check when uploading files to tos or not.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | true                                                                                                                                              | NONE     |
| fs.tos.get-file-status.enabled            | Whether enable tos getFileStatus API or not, which returns the object info directly in one RPC request, otherwise, might need to send three RPC requests to get object info. For example, there is a key 'a/b/c' exists in TOS, and we want to get object status of 'a/b', the GetFileStatus('a/b') will return the prefix 'a/b/' as a directory object directly. If this property is disabled, we need to head('a/b') at first, and then head('a/b/'), and last call list('a/b/', limit=1) to get object info. Using GetFileStatus API can reduce the RPC call times. | true                                                                                                                                              | NONE     |
| fs.tos.checksum-algorithm                 | The key indicates the name of the tos checksum algorithm. Specify the algorithm name to compare checksums between different storage systems. For example to compare checksums between hdfs and tos, we need to configure the algorithm name to COMPOSITE-CRC32C.                                                                                                                                                                                                                                                                                                       | TOS-CHECKSUM                                                                                                                                      | NONE     |
| fs.tos.checksum-type                      | The key indicates how to retrieve file checksum from tos, error will be thrown if the configured checksum type is not supported by tos. The supported checksum types are: CRC32C, CRC64ECMA.                                                                                                                                                                                                                                                                                                                                                                           | CRC64ECMA                                                                                                                                         | NONE     |
| fs.objectstorage.%s.impl                  | The object storage implementation for the defined scheme. For example, we can delegate the scheme 'abc' to TOS (or other object storage),and access the TOS object storage as 'abc://bucket/path/to/key'                                                                                                                                                                                                                                                                                                                                                               | NONE                                                                                                                                              | NO       |
| fs.%s.delete.batch-size                   | The batch size of deleting multiple objects per request for the given object storage. e.g. fs.tos.delete.batch-size                                                                                                                                                                                                                                                                                                                                                                                                                                                    | 250                                                                                                                                               | NONE     |
| fs.%s.multipart.size                      | The multipart upload part size of the given object storage, e.g. fs.tos.multipart.size.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | 8388608                                                                                                                                           | NONE     |
| fs.%s.multipart.copy-threshold            | The threshold (larger than this value) to enable multipart upload during copying objects in the given object storage. If the copied data size is less than threshold, will copy data via executing copyObject instead of uploadPartCopy. E.g. fs.tos.multipart.copy-threshold                                                                                                                                                                                                                                                                                          | 5242880                                                                                                                                           | NONE     |
| fs.%s.multipart.threshold                 | The threshold which control whether enable multipart upload during writing data to the given object storage, if the write data size is less than threshold, will write data via simple put instead of multipart upload. E.g. fs.tos.multipart.threshold.                                                                                                                                                                                                                                                                                                               | 10485760                                                                                                                                          | NONE     |
| fs.%s.multipart.staging-buffer-size       | The max byte size which will buffer the staging data in-memory before flushing to the staging file. It will decrease the random write in local staging disk dramatically if writing plenty of small files.                                                                                                                                                                                                                                                                                                                                                             | 4096                                                                                                                                              | NONE     |
| fs.%s.multipart.staging-dir               | The multipart upload part staging dir(s) of the given object storage. e.g. fs.tos.multipart.staging-dir. Separate the staging dirs with comma if there are many staging dir paths.                                                                                                                                                                                                                                                                                                                                                                                     | ${java.io.tmpdir}/multipart-staging-dir                                                                                                           | NONE     |
| fs.%s.missed.parent.dir.async-create      | True to create the missed parent dir asynchronously during deleting or renaming a file or dir.                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | true                                                                                                                                              | NONE     |
| fs.%s.rename.enabled                      | Whether using rename semantic of object storage during rename files, otherwise using copy + delete. Please ensure that the object storage support and enable rename semantic and before enable it, and also ensure grant rename permission to the requester. If you are using TOS, you have to send putBucketRename request before sending rename request, otherwise MethodNotAllowed exception will be thrown.                                                                                                                                                        | false                                                                                                                                             | NONE     |
| fs.%s.task.thread-pool-size               | The range size when open object storage input stream. Value must be positive. The size of thread pool used for running tasks in parallel for the given object fs, e.g. delete objects, copy files. the key example: fs.tos.task.thread-pool-size.                                                                                                                                                                                                                                                                                                                      | Long.MAX_VALUE                                                                                                                                    | NONE     |
| fs.%s.multipart.thread-pool-size          | The size of thread pool used for uploading multipart in parallel for the given object storage, e.g. fs.tos.multipart.thread-pool-size                                                                                                                                                                                                                                                                                                                                                                                                                                  | Max value of 2 and available processors.                                                                                                          | NONE     |
| fs.%s.checksum.enabled                    | The toggle indicates whether enable checksum during getting file status for the given object. E.g. fs.tos.checksum.enabled                                                                                                                                                                                                                                                                                                                                                                                                                                             | Max value of 2 and available processors.                                                                                                          | NONE     |
| fs.filestore.checksum-algorithm           | The key indicates the name of the filestore checksum algorithm. Specify the algorithm name to satisfy different storage systems. For example, the hdfs style name is COMPOSITE-CRC32 and COMPOSITE-CRC32C.                                                                                                                                                                                                                                                                                                                                                             | TOS-CHECKSUM                                                                                                                                      | NO       |
| fs.filestore.checksum-type                | The key indicates how to retrieve file checksum from filestore, error will be thrown if the configured checksum type is not supported. The supported checksum type is: MD5.                                                                                                                                                                                                                                                                                                                                                                                            | MD5                                                                                                                                               | NO       |

## Running unit tests in hadoop-tos module

Unit tests need to connect to tos service. Setting the 6 environment variables below to run unit
tests.

```bash
export TOS_ACCESS_KEY_ID={YOUR_ACCESS_KEY}
export TOS_SECRET_ACCESS_KEY={YOUR_SECRET_ACCESS_KEY}
export TOS_ENDPOINT={TOS_SERVICE_ENDPOINT}
export FILE_STORAGE_ROOT=/tmp/local_dev/
export TOS_BUCKET={YOUR_BUCKET_NAME}
export TOS_UNIT_TEST_ENABLED=true
```

Then cd to hadoop project root directory, and run the test command below.

```bash
mvn -Dtest=org.apache.hadoop.fs.tosfs.** test -pl org.apache.hadoop:hadoop-tos
```