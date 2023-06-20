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

# Hadoop-Aliyun module: Integration with Aliyun Web Services

<!-- MACRO{toc|fromDepth=0|toDepth=5} -->

## Overview

The `hadoop-aliyun` module provides support for Aliyun integration with
[Aliyun Object Storage Service (Aliyun OSS)](https://www.aliyun.com/product/oss).
The generated JAR file, `hadoop-aliyun.jar` also declares a transitive
dependency on all external artifacts which are needed for this support — enabling
downstream applications to easily use this support.

To make it part of Apache Hadoop's default classpath, simply make sure
that HADOOP_OPTIONAL_TOOLS in hadoop-env.sh has 'hadoop-aliyun' in the list.

### Features

* Read and write data stored in Aliyun OSS.
* Present a hierarchical file system view by implementing the standard Hadoop
[`FileSystem`](../api/org/apache/hadoop/fs/FileSystem.html) interface.
* Can act as a source of data in a MapReduce job, or a sink.

### Warning #1: Object Stores are not filesystems.

Aliyun OSS is an example of "an object store". In order to achieve scalability
and especially high availability, Aliyun OSS has relaxed some of the constraints
which classic "POSIX" filesystems promise.



Specifically

1. Atomic operations: `delete()` and `rename()` are implemented by recursive
file-by-file operations. They take time at least proportional to the number of files,
during which time partial updates may be visible. `delete()` and `rename()`
can not guarantee atomicity. If the operations are interrupted, the filesystem
is left in an intermediate state.
2. File owner and group are persisted, but the permissions model is not enforced.
Authorization occurs at the level of the entire Aliyun account via
[Aliyun Resource Access Management (Aliyun RAM)](https://www.aliyun.com/product/ram).
3. Directory last access time is not tracked.
4. The append operation is not supported.

### Warning #2: Directory last access time is not tracked,
Features of Hadoop relying on this can have unexpected behaviour. E.g. the
AggregatedLogDeletionService of YARN will not remove the appropriate log files.

### Warning #3: Your Aliyun credentials are valuable

Your Aliyun credentials not only pay for services, they offer read and write
access to the data. Anyone with the account can not only read your datasets
— they can delete them.

Do not inadvertently share these credentials through means such as
1. Checking in to SCM any configuration files containing the secrets.
2. Logging them to a console, as they invariably end up being seen.
3. Defining filesystem URIs with the credentials in the URL, such as
`oss://accessKeyId:accessKeySecret@directory/file`. They will end up in
logs and error messages.
4. Including the secrets in bug reports.

If you do any of these: change your credentials immediately!

### Warning #4: The Aliyun OSS client provided by Aliyun E-MapReduce are different from this implementation

Specifically: on Aliyun E-MapReduce, `oss://` is also supported but with
a different implementation. If you are using Aliyun E-MapReduce,
follow these instructions —and be aware that all issues related to Aliyun
OSS integration in E-MapReduce can only be addressed by Aliyun themselves:
please raise your issues with them.

## OSS

### Authentication properties

    <property>
      <name>fs.oss.accessKeyId</name>
      <description>Aliyun access key ID</description>
    </property>

    <property>
      <name>fs.oss.accessKeySecret</name>
      <description>Aliyun access key secret</description>
    </property>

    <property>
      <name>fs.oss.credentials.provider</name>
      <description>
        Class name of a credentials provider that implements
        com.aliyun.oss.common.auth.CredentialsProvider. Omit if using access/secret keys
        or another authentication mechanism. The specified class must provide an
        accessible constructor accepting java.net.URI and
        org.apache.hadoop.conf.Configuration, or an accessible default constructor.
      </description>
    </property>

### Other properties

    <property>
      <name>fs.AbstractFileSystem.oss.impl</name>
      <value>org.apache.hadoop.fs.aliyun.oss.OSS</value>
      <description>The implementation class of the OSS AbstractFileSystem.
        If you want to use OSS as YARN’s resource storage dir via the
        fs.defaultFS configuration property in Hadoop’s core-site.xml,
        you should add this configuration to Hadoop's core-site.xml
      </description>
    </property>

    <property>
      <name>fs.oss.endpoint</name>
      <description>Aliyun OSS endpoint to connect to. An up-to-date list is
        provided in the Aliyun OSS Documentation.
       </description>
    </property>

    <property>
       <name>fs.oss.impl</name>
       <value>org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem</value>
    </property>

    <property>
      <name>fs.oss.proxy.host</name>
      <description>Hostname of the (optinal) proxy server for Aliyun OSS connection</description>
    </property>

    <property>
      <name>fs.oss.proxy.port</name>
      <description>Proxy server port</description>
    </property>

    <property>
      <name>fs.oss.proxy.username</name>
      <description>Username for authenticating with proxy server</description>
    </property>

    <property>
      <name>fs.oss.proxy.password</name>
      <description>Password for authenticating with proxy server.</description>
    </property>

    <property>
      <name>fs.oss.proxy.domain</name>
      <description>Domain for authenticating with proxy server.</description>
    </property>

    <property>
      <name>fs.oss.proxy.workstation</name>
      <description>Workstation for authenticating with proxy server.</description>
    </property>

    <property>
      <name>fs.oss.attempts.maximum</name>
      <value>10</value>
      <description>How many times we should retry commands on transient errors.</description>
    </property>

    <property>
      <name>fs.oss.connection.establish.timeout</name>
      <value>50000</value>
      <description>Connection setup timeout in milliseconds.</description>
    </property>

    <property>
      <name>fs.oss.connection.timeout</name>
      <value>200000</value>
      <description>Socket connection timeout in milliseconds.</description>
    </property>

    <property>
      <name>fs.oss.paging.maximum</name>
      <value>1000</value>
      <description>How many keys to request from Aliyun OSS when doing directory listings at a time.
      </description>
    </property>

    <property>
      <name>fs.oss.multipart.upload.size</name>
      <value>10485760</value>
      <description>Size of each of multipart pieces in bytes.</description>
    </property>

    <property>
      <name>fs.oss.upload.active.blocks</name>
      <value>4</value>
      <description>Active(Concurrent) upload blocks when uploading a file.</description>
    </property>

    <property>
      <name>fs.oss.multipart.download.threads</name>
      <value>10</value>
      <description>The maximum number of threads allowed in the pool for multipart download and upload.</description>
    </property>

    <property>
      <name>fs.oss.multipart.download.ahead.part.max.number</name>
      <value>4</value>
      <description>The maximum number of read ahead parts when reading a file.</description>
    </property>

    <property>
      <name>fs.oss.max.total.tasks</name>
      <value>128</value>
      <description>The maximum queue number for multipart download and upload.</description>
    </property>

    <property>
      <name>fs.oss.max.copy.threads</name>
      <value>25</value>
      <description>The maximum number of threads allowed in the pool for copy operations.</description>
    </property>

    <property>
      <name>fs.oss.max.copy.tasks.per.dir</name>
      <value>5</value>
      <description>The maximum number of concurrent tasks allowed when copying a directory.</description>
    </property>

    <property>
      <name>fs.oss.multipart.upload.threshold</name>
      <value>20971520</value>
      <description>Minimum size in bytes before we start a multipart uploads or copy.
        Notice: This property is deprecated and will be removed in further version.
      </description>
    </property>

    <property>
      <name>fs.oss.multipart.download.size</name>
      <value>524288/value>
      <description>Size in bytes in each request from ALiyun OSS.</description>
    </property>

    <property>
      <name>fs.oss.list.version</name>
      <value>2</value>
      <description>Select which version of the OSS SDK's List Objects API to use.
        Currently support 2(default) and 1(older API).
      </description>
    </property>

    <property>
      <name>fs.oss.fast.upload.buffer</name>
      <value>disk</value>
      <description>
        The buffering mechanism to use.
        Values: disk, array, bytebuffer, array_disk, bytebuffer_disk.

        "disk" will use the directories listed in fs.oss.buffer.dir as
        the location(s) to save data prior to being uploaded.

        "array" uses arrays in the JVM heap

        "bytebuffer" uses off-heap memory within the JVM.

        Both "array" and "bytebuffer" will consume memory in a single stream up to the number
        of blocks set by:

            fs.oss.multipart.upload.size * fs.oss.upload.active.blocks.

        If using either of these mechanisms, keep this value low

        The total number of threads performing work across all threads is set by
        fs.oss.multipart.download.threads(Currently fast upload shares the same thread tool with download.
        The thread pool size is specified in "fs.oss.multipart.download.threads"),
        with fs.oss.max.total.tasks values setting the number of queued work items.

        "array_disk" and "bytebuffer_disk" support fallback to disk.
      </description>
    </property>

    <property>
      <name>fs.oss.fast.upload.memory.limit</name>
      <value>1073741824</value>
      <description>
        Memory limit of "array_disk" and "bytebuffer_disk" upload buffers.
        Will fallback to disk buffers if used memory reaches the limit.
      </description>
    </property>

    <property>
      <name>fs.oss.buffer.dir</name>
      <value>${env.LOCAL_DIRS:-${hadoop.tmp.dir}}/oss</value>
      <description>Comma separated list of directories to buffer
        OSS data before uploading to Aliyun OSS.
        Yarn container path will be used as default value on yarn applications,
        otherwise fall back to hadoop.tmp.dir
      </description>
    </property>

    <property>
      <name>fs.oss.acl.default</name>
      <value></vaule>
      <description>Set a canned ACL for bucket. Value may be private, public-read, public-read-write.
      </description>
    </property>

    <property>
      <name>fs.oss.server-side-encryption-algorithm</name>
      <value></vaule>
      <description>Specify a server-side encryption algorithm for oss: file system.
         Unset by default, and the only other currently allowable value is AES256.
      </description>
    </property>

    <property>
      <name>fs.oss.connection.maximum</name>
      <value>32</value>
      <description>Number of simultaneous connections to oss.</description>
    </property>

    <property>
      <name>fs.oss.connection.secure.enabled</name>
      <value>true</value>
      <description>Connect to oss over ssl or not, true by default.</description>
    </property>

## Testing the hadoop-aliyun Module

To test `oss://` filesystem client, two files which pass in authentication
details to the test runner are needed.

1. `auth-keys.xml`
2. `core-site.xml`

Those two configuration files must be put into
`hadoop-tools/hadoop-aliyun/src/test/resources`.

### `core-site.xml`

This file pre-exists and sources the configurations created in `auth-keys.xml`.

For most cases, no modification is needed, unless a specific, non-default property
needs to be set during the testing.

### `auth-keys.xml`

This file triggers the testing of Aliyun OSS module. Without this file,
*none of the tests in this module will be executed*

It contains the access key Id/secret and proxy information that are needed to
connect to Aliyun OSS, and an OSS bucket URL should be also provided.

1. `test.fs.oss.name` : the URL of the bucket for Aliyun OSS tests

The contents of the bucket will be cleaned during the testing process, so
do not use the bucket for any purpose other than testing.

### Run Hadoop contract tests
Create file `contract-test-options.xml` under `/test/resources`. If a
specific file `fs.contract.test.fs.oss` test path is not defined, those
tests will be skipped. Credentials are also needed to run any of those
tests, they can be copied from `auth-keys.xml` or through direct
XInclude inclusion. Here is an example of `contract-test-options.xml`:

    <?xml version="1.0"?>
    <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
    <configuration>

    <include xmlns="http://www.w3.org/2001/XInclude"
    href="auth-keys.xml"/>

      <property>
        <name>fs.contract.test.fs.oss</name>
        <value>oss://spark-tests</value>
      </property>

      <property>
        <name>fs.oss.impl</name>
        <value>org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem</value>
      </property>

      <property>
        <name>fs.oss.endpoint</name>
        <value>oss-cn-hangzhou.aliyuncs.com</value>
      </property>

      <property>
        <name>fs.oss.buffer.dir</name>
        <value>/tmp/oss</value>
      </property>

      <property>
        <name>fs.oss.multipart.download.size</name>
        <value>102400</value>
      </property>
    </configuration>
