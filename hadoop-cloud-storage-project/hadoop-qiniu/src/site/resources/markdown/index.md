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

# Hadoop-Qiniu module: Integration with Qiniu Kodo Storage Services

<!-- MACRO{toc|fromDepth=0|toDepth=5} -->

## Documents

* [Testing](./test.html)
* [Config](./config.html)
* [Private Cloud](./private-cloud.html)
* [Design](./design.html)

* [Testing zh](./test_zh.html)
* [Config zh](./config_zh.html)
* [Private Cloud zh](./private-cloud_zh.html)
* [Design zh](./design_zh.html)

## Overview

Qiniu Kodo is a self-developed unstructured data storage management platform by Qiniu Cloud Storage that supports center and edge storage.

The `hadoop-qiniu` module provides support for Qiniu integration with
[Qiniu Kodo Storage Service](https://www.qiniu.com/products/kodo).
The generated JAR file, `hadoop-qiniu.jar` also declares a transitive
dependency on all external artifacts which are needed for this support — enabling
downstream applications to easily use this support.

To make it part of Apache Hadoop's default classpath, simply make sure
that HADOOP_OPTIONAL_TOOLS in hadoop-env.sh has 'hadoop-qiniu' in the list.


### Features

+ Support Hadoop MapReduce and Spark read and write operations on Kodo
+ Implemented the Hadoop file system interface, and provided the same user experience as HDFS by simulating the hierarchical directory structure
+ Support large file multipart upload, up to 10TB single file
+ Use Kodo's batch api, with high-performance file system operation capabilities
+ Support file block-level cache in memory and block-level cache in disk, improve file read performance

### Warning

#### Warning #1: Object storage is not a file system
Since the object storage is not a file system, there are some restrictions on the use:
1. Object storage is a key-value storage, which does not support hierarchical directory structure, so it is necessary to use path separator to simulate hierarchical directory structure
2. Do not track the modification time and access time of the directory
3. Kodo object storage does not support file append writing, so data cannot be appended to the end of an existing file
4. Do not track the modification time of the file, so in the file system, the modification time of the file is the creation time of the file
5. The delete and rename operations are not atomic, which means that if the operation is interrupted unexpectedly, the file system may be in an inconsistent state
6. Object storage does not support unix-like permission management, so in the file system, the following rules need to be provided:
   + Directory permission is 715
   + File permission is 666
   + File owner report the local current username
   + File owner group report the local current username
7. The maximum number of files per list request is 1000
8. Support large file multipart upload, but the number of parts is limited to 10000. Since the maximum size of a single part is 1GB, the maximum single file is 10TB

#### Warning #2: Directory last access time is not tracked
Features of Hadoop relying on this can have unexpected behaviour. E.g. the AggregatedLogDeletionService of YARN will not remove the appropriate log files.

#### Warning #3: Your Qiniu credentials are very, very valuable
Your Qiniu credentials not only pay for services, they offer read and write access to the data. Anyone with the credentials can not only read your datasets —they can delete them.

## Quick Start

### Configuration

#### hadoop-env.sh

Open the file `$HADOOP_HOME/etc/hadoop/hadoop-env.sh` and add the following configuration:

```shell
export HADOOP_OPTIONAL_TOOLS="hadoop-qiniu"
```

#### core-site.xml

Modify file `$HADOOP_HOME/etc/hadoop/core-site.xml` to add user configuration and class information for Kodo.
In public cloud environments, only the following configuration is usually required to work properly:

```xml

<configuration>
    <property>
        <name>fs.qiniu.auth.accessKey</name>
        <value>XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX</value>
        <description>Qiniu Access Key</description>
    </property>

    <property>
        <name>fs.qiniu.auth.secretKey</name>
        <value>XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX</value>
        <description>Qiniu Secret Key</description>
    </property>

    <property>
        <name>fs.kodo.impl</name>
        <value>org.apache.hadoop.fs.qiniu.kodo.QiniuKodoFileSystem</value>
    </property>
    <property>
        <name>fs.AbstractFileSystem.kodo.impl</name>
        <value>org.apache.hadoop.fs.qiniu.kodo.QiniuKodo</value>
    </property>
    <property>
        <name>fs.defaultFS</name>
        <value>kodo://example-bucket-name/</value>
        <description>hadoop default fs</description>
    </property>

</configuration>

```

If you need more configuration, you can refer to the yml file: [config.yml](config.md) and convert the configuration
items described by the yml hierarchy into xml configuration items by yourself, and supplement the namespace prefix
`fs.qiniu`

For example, for proxy configuration:

```yml
# proxy configuration
proxy:
  enable: true
  hostname: '127.0.0.1'
  port: 8080
```

The corresponding xml configuration is as follows:

```xml

<configuration>
    <property>
        <name>fs.qiniu.proxy.enable</name>
        <value>true</value>
    </property>
    <property>
        <name>fs.qiniu.proxy.hostname</name>
        <value>127.0.0.1</value>
    </property>
    <property>
        <name>fs.qiniu.proxy.port</name>
        <value>8080</value>
    </property>
</configuration>
```

### Run mapreduce example program wordcount

#### put command

```shell
mkdir testDir
touch testDir/input.txt
echo "a b c d ee a b s" > testDir/input.txt
hadoop fs -put testDir kodo:///testDir

```

#### ls command

```shell
hadoop fs -ls -R kodo://example-bucket/
drwx--xr-x   - root root          0 1970-01-01 08:00 kodo://example-bucket/user
drwx--xr-x   - root root          0 1970-01-01 08:00 kodo://example-bucket/user/root
drwx--xr-x   - root root          0 1970-01-01 08:00 kodo://example-bucket/testDir
-rw-rw-rw-   0 root root         17 2023-01-18 15:54 kodo://example-bucket/testDir/input.txt
```

#### get command

```shell
$ hadoop fs -get kodo://testDir testDir1
$ ls -l -R testDir1
total 8
-rw-r--r--  1 root  staff  17 Jan 18 15:57 input.txt
```

#### Run wordcount example

```shell
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-{version}.jar wordcount kodo://example-bucket/testDir/input.txt kodo://example-bucket/testDir/output
```

If the program runs successfully, the following information will be printed:

```text
2023-01-18 16:00:49,228 INFO mapreduce.Job: Counters: 35
	File System Counters
		FILE: Number of bytes read=564062
		FILE: Number of bytes written=1899311
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		KODO: Number of bytes read=34
		KODO: Number of bytes written=25
		KODO: Number of read operations=3
		KODO: Number of large read operations=0
		KODO: Number of write operations=0
	Map-Reduce Framework
		Map input records=1
		Map output records=8
		Map output bytes=49
		Map output materialized bytes=55
		Input split bytes=102
		Combine input records=8
		Combine output records=6
		Reduce input groups=6
		Reduce shuffle bytes=55
		Reduce input records=6
		Reduce output records=6
		Spilled Records=12
		Shuffled Maps =1
		Failed Shuffles=0
		Merged Map outputs=1
		GC time elapsed (ms)=31
		Total committed heap usage (bytes)=538968064
	Shuffle Errors
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
	File Input Format Counters 
		Bytes Read=17
	File Output Format Counters 
		Bytes Written=25
```

```text
$ hadoop fs -ls -R kodo://example-bucket/
drwx--xr-x   - root root          0 1970-01-01 08:00 kodo://example-bucket/user
drwx--xr-x   - root root          0 1970-01-01 08:00 kodo://example-bucket/user/root
drwx--xr-x   - root root          0 1970-01-01 08:00 kodo://example-bucket/testDir
-rw-rw-rw-   0 root root         17 2023-01-18 15:54 kodo://example-bucket/testDir/input.txt
drwx--xr-x   - root root          0 1970-01-01 08:00 kodo://example-bucket/testDir/output
-rw-rw-rw-   0 root root          0 2023-01-18 16:00 kodo://example-bucket/testDir/output/_SUCCESS
-rw-rw-rw-   0 root root         25 2023-01-18 16:00 kodo://example-bucket/testDir/output/part-r-00000
```

#### cat command

```text
$ hadoop fs -cat kodo://example-bucket/testDir/output/part-r-00000
a	2
b	2
c	1
d	1
ee	1
s	1
```

## Testing the hadoop-qiniu Module

Please refer to the [test](test.md) document for details
