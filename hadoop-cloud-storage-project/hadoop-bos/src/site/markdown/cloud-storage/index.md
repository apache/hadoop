<!--
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

# Integration of Baidu BOS in Hadoop

## Overview

BOS (Baidu Object Storage) is the object storage service provided by Baidu Cloud. Hadoop-BOS is a connector between computing systems and underlying storage. For systems like Hadoop MR, Hive, Spark and Alluxio, hadoop-bos helps them use BOS as the underlying storage system instead of HDFS.

## Quick Start

In quick start, we will use hadoop shell command to access a BOS bucket.

### Requirements

1. A Baidu Cloud account. Use the account to create a BOS bucket.
2. A dev environment that can access BOS. E.g. a local server or a Baidu Cloud cloud server.
3. Install hadoop to the dev environment. Hadoop is installed at `$HADOOP_HOME`.

### Usage

* The `hadoop-bos` connector jar is included in the Hadoop cloud storage distribution
(`hadoop-cloud-storage-dist`) and can be found in the release package at
`$HADOOP_HOME/share/hadoop/common/lib/hadoop-bos-{VERSION}.jar`.

Note: By default, only the `hadoop-bos` jar itself is included in the release tarball.
The BOS SDK dependencies are excluded to reduce distribution size. To build a distribution
with full BOS dependencies included, add `-Dhadoop-bos-package` to the build command.

If you are building from source or need to build the module separately, use:
```bash
mvn package -DskipTests -pl org.apache.hadoop:hadoop-bos
```

The bundle jar file will be placed at
`hadoop-cloud-storage-project/hadoop-bos/target/hadoop-bos-{VERSION}.jar`.

* If the jar is not already present in `$HADOOP_HOME/share/hadoop/common/lib/`,
copy it there and add it to the classpath. Remember to copy to all hadoop nodes.
```bash
cp hadoop-bos-{VERSION}.jar $HADOOP_HOME/share/hadoop/common/lib/
```

* Configure properties below.

```xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>bos://{your_bucket_name}/</value>
    <description>
      The name of the default file system. Make it your BOS bucket.
    </description>
  </property>

  <property>
    <name>fs.bos.endpoint</name>
    <value></value>
    <description>
      Object storage endpoint to connect to, e.g. 'fs.bos.endpoint'='bj.bcebos.com'.
    </description>
  </property>

  <property>
    <name>fs.bos.impl</name>
    <value>org.apache.hadoop.fs.bos.BaiduBosFileSystem</value>
    <description>
      The implementation class of the BOS FileSystem.
    </description>
  </property>

  <property>
    <name>fs.AbstractFileSystem.bos.impl</name>
    <value>org.apache.hadoop.fs.bos.BOS</value>
    <description>
      The implementation class of the BOS AbstractFileSystem.
    </description>
  </property>

  <property>
    <name>fs.bos.access.key</name>
    <value></value>
    <description>
      The access key of Baidu Cloud's user or role.
    </description>
  </property>

  <property>
    <name>fs.bos.secret.access.key</name>
    <value></value>
    <description>
      The secret key of the access key specified by 'fs.bos.access.key'.
    </description>
  </property>
</configuration>
```

* Use hadoop shell command to access BOS.

```bash
# 1. List root dir.
hadoop fs -ls /

# 2. Make directory.
hadoop fs -mkdir /hadoop-bos

# 3. Write and read.
echo "hello BOS." > hello.txt
hadoop fs -put hello.txt /hadoop-bos/
hadoop fs -cat /hadoop-bos/hello.txt

# 4. Delete file and directory.
hadoop fs -rm -r /hadoop-bos/
```

## Introduction

This is a brief introduction of hadoop-bos design and basic functions. The following contents are
based on flat mode by default. The differences between hierarchy mode will be explained at the end
of each section.

### BOS

BOS is the object storage service of Baidu Cloud. It is similar to AWS S3, Azure Blob Storage
and Aliyun OSS. BOS has 2 modes: the flat mode and the hierarchy mode. In flat mode, there are no directories,
all objects are files indexed by the object names. User can use 'slash' in the object name to
logically divide objects into different "directories", though the "directories divided by the slash"
are not real. Cleanup a logic directory is to clean all the files with "directory path" as prefix.

In hierarchy mode, there are directories and files. A directory object is the object whose name
ends with slash. All objects start with the directory object name are the directory object's
consecutive objects, and together they form a directory tree. A directory object can't contain
any data. Delete or rename a directory object will clean or rename all objects under the
directory tree atomically.

### Directory and file

This section illustrates how hadoop-bos transforms BOS to a hadoop FileSystem. BOS requires object's
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
hierarchy mode, the requirements are enforced rules controlled by BOS service, so there won't be
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

The read behaviour in hadoop-bos is very like reading an HDFS file. The challenge is how to keep the
input stream consistent with object. If the object is changed after we open the file, the input
stream should fail. This is implemented by saving the file checksum when open file. If the
file is changed while reading, the input stream will compare the checksum and trigger an exception.

The write behaviour in hadoop-bos is slightly different from hdfs. Firstly, the append interface
is not supported. Secondly, the file is not visible until it is successfully closed. Finally,
when 2 clients try to write one file, the last client to close the file will override the previous
one.

Both read and write has many performance optimizations. E.g. range read, connection reuse, local
write buffer, put for small files, multipart-upload for big files etc.

### Permissions

BOS permission model is different from hadoop filesystem permission model. BOS supports permissions
based on IAM, Bucket Policy, Bucket and Object ACL, while hadoop filesystem permission model uses
mode and acl. There is no way to map BOS permission to hadoop filesystem permission, so we have
to use fake permissions in BaiduBosFileSystem and BOS. Users can read and change the filesystem
permissions, they can only be seen but not effective. Permission control eventually depends on BOS
permission model.

### Times

Hadoop-bos supports last modified time and doesn't support access time. For files, the last modified
time is the object's modified time. For directories, if the directory object doesn't exist, the last
modified time is the current system time. If the directory object exists, the last modified time is
the object's modify time when `getFileStatus` and current system time when `listStatus`.

### File checksum

BOS supports CRC32 checksum. We can retrieve it by calling `FileSystem#getFileChecksum`.
To be compatible with HDFS, BOS provides optional CRC32C checksum. When we distcp
between HDFS and BOS, we can rely on distcp checksum mechanisms to keep data consistent.

### Credential

BOS client uses access key id and secret access key to authenticate with BOS service. There are 2
ways to configure them. First is adding to hadoop configuration, such as adding to core-site.xml or
configuring through `-D` parameter. The second is setting environment variable, hadoop-bos will
search for environment variables automatically.

To configure ak, sk in hadoop configuration, using the key below.

```xml
<configuration>
  <!--Set global ak, sk for all buckets.-->
  <property>
    <name>fs.bos.access.key</name>
    <value></value>
    <description>
      The accessKey key to access the BOS object storage.
    </description>
  </property>
  <property>
    <name>fs.bos.secret.access.key</name>
    <value></value>
    <description>
      The secret access key to access the object storage.
    </description>
  </property>
  <property>
    <name>fs.bos.session.token.key</name>
    <value></value>
    <description>
      The session token to access the object storage.
    </description>
  </property>

  <!--Set ak, sk for specified bucket. It has higher priority then the global keys.-->
  <property>
    <name>fs.bos.bucket.{bucket_name}.access.key</name>
    <value></value>
    <description>
      The access key to access the object storage for the configured bucket.
    </description>
  </property>
  <property>
    <name>fs.bos.bucket.{bucket_name}.secret.access.key</name>
    <value></value>
    <description>
      The secret access key to access the object storage for the configured bucket.
    </description>
  </property>
  <property>
    <name>fs.bos.bucket.{bucket_name}.session.token.key</name>
    <value></value>
    <description>
      The session token to access the object storage for the configured bucket.
    </description>
  </property>
</configuration>
```

The ak, sk in environment variables have the top priority and automatically fall back to hadoop
configuration if not found.

## Properties Summary

| properties                                | description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | default value                                                                                                                                     | required |
|-------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| fs.bos.access.key                         | The accessKey key to access the BOS object storage                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | NONE                                                                                                                                              | YES      |
| fs.bos.secret.access.key                  | The secret access key to access the object storage                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | NONE                                                                                                                                              | YES      |
| fs.bos.session.token.key                 | The session token to access the object storage                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | NONE                                                                                                                                              | NO       |
| fs.bos.endpoint                          | Object storage endpoint to connect to, e.g. bj.bcebos.com, gz.bcebos.com                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | NONE                                                                                                                                              | NO       |
| fs.bos.bucket.%s.access.key              | The access key to access the object storage for the configured bucket, where %s is the bucket name.                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | NONE                                                                                                                                              | NO       |
| fs.bos.bucket.%s.secret.access.key        | The secret access key to access the object storage for the configured bucket, where %s is the bucket name                                                                                                                                                                                                                                                                                                                                                                                                                                                              | NONE                                                                                                                                              | NO       |
| fs.bos.bucket.%s.session.token.key        | The session token to access the object storage for the configured bucket, where %s is the bucketname                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | NONE                                                                                                                                              | NO       |
| fs.bos.credentials.provider              | Credentials provider class for BOS authentication. Available providers: ConfigurationCredentialsProvider, EnvironmentVariableCredentialsProvider, HadoopCredentialsProvider                                                                                                                                                                                                                                                                                                                                                                                                          | org.apache.hadoop.fs.bos.credentials.ConfigurationCredentialsProvider                                                                                   | NO       |
| fs.bos.multipart.uploads.block.size      | The size of each part for multipart uploads (in bytes).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | 16777216 (16MB)                                                                                                                                  | NO       |
| fs.bos.multipart.uploads.concurrent.size  | The number of concurrent threads for multipart uploads.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | 10                                                                                                                                                | NO       |
| fs.bos.copy.large.file.threshold          | The threshold (in bytes) for using multipart upload during copy operations.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | 5368709120 (5GB)                                                                                                                               | NO       |
| fs.bos.max.connections                   | The maximum number of connections to the BOS service that a client can create.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | 50                                                                                                                                                | NO       |
| fs.bos.readahead.size                   | The size of readahead buffer (in bytes).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | 4194304 (4MB)                                                                                                                                     | NO       |
| fs.bos.read.buffer.size                 | The size of read buffer (in bytes).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | 131072 (128KB)                                                                                                                                    | NO       |
| fs.bos.threads.max.num                 | The maximum number of threads for BOS operations.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | 64                                                                                                                                                | NO       |
| fs.bos.bucket.hierarchy                 | Whether the bucket uses hierarchical namespace mode.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | NONE                                                                                                                                              | NO       |

## Running unit tests in hadoop-bos module

Unit tests need to connect to BOS service. Setting the environment variables below to run unit
tests.

```bash
export FS_BOS_ACCESS_KEY=YOUR_ACCESS_KEY
export FS_BOS_SECRET_ACCESS_KEY=YOUR_SECRET_ACCESS_KEY
export FS_BOS_ENDPOINT=bj.bcebos.com
```

Then cd to hadoop project root directory, and run the test command below.

```bash
mvn -Dtest=org.apache.hadoop.fs.bos.** test -pl org.apache.hadoop:hadoop-bos
```

## Testing the hadoop-bos Module

To test BOS filesystem, the following configuration file is needed:

### `contract-test-options.xml`

All configurations related to support contract tests need to be specified in `contract-test-options.xml`. Here is an example of `contract-test-options.xml`.

```xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>fs.contract.test.fs.bos</name>
        <value>bos://your-bucket-name/test</value>
    </property>
    <property>
        <name>fs.bos.endpoint</name>
        <value>bj.bcebos.com</value>
    </property>
    <property>
        <name>fs.bos.credentials.provider</name>
        <value>org.apache.hadoop.fs.bos.credentials.EnvironmentVariableCredentialsProvider</value>
    </property>
</configuration>
```

If the option `fs.contract.test.fs.bos` is not defined in the file, all contract tests will be skipped.

## Features and Limitations

### Supported Features

- File create, read, delete
- Directory operations (mkdir, delete)
- Rename (via copy + delete)
- Random access (seek)
- Content summary
- Checksum verification
- Hierarchical namespace buckets (atomic rename/delete)

### Unsupported Features

- Append to files
- File concatenation
- File truncation
- Symbolic links
- Extended attributes (xattr)
- Full Unix permissions (limited support in hierarchy mode)
- hflush/hsync operations

## Performance Notes

Object Storage is not a file system and it has some limitations:

1. Object storage is a key-value storage and it does not support hierarchical directory naturally. Usually, using the directory separatory in object key to simulate the hierarchical directory, such as "/hadoop/data/words.dat".

2. BOS Object storage can not support the object's append operation currently. It means that you can not append content to the end of an existing object(file).

3. Both `delete` and `rename` operations are non-atomic in flat mode, which means that the operations are interrupted, the operation result may be inconsistent state. In hierarchy mode, these operations are atomic.

4. Object storages have different authorization models:
   - Directory permissions are reported as 777.
   - File permissions are reported as 666.
   - File owner is reported as the local current user.
   - File group is also reported as the local current user.

5. Supports multipart uploads for a large file (up to 5TB), but the number of parts is limited.

6. The number of files listed each time is limited to 1000.

## Getting Help

- Check Hadoop FileSystem specification: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/filesystem/
- Review Baidu BOS documentation: https://cloud.baidu.com/doc/BOS/index.html
- Check project README.md
- File issues on project issue tracker