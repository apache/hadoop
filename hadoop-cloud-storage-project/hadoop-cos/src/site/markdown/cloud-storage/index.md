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

# Integeration of Tencent COS in Hadoop

## Introduction

[Tencent COS](https://intl.cloud.tencent.com/product/cos) is a famous object storage system provided by Tencent Corp. Hadoop-COS is a client that makes the upper computing systems based on HDFS be able to use the COS as its underlying storage system. The big data-processing systems that have been identified for support are: Hadoop MR, Spark, Alluxio and etc. In addition, Druid also can use COS as its deep storage by configuring HDFS-Load-Plugin integerating with HADOOP-COS.


## Features

- Support Hadoop MapReduce and Spark write data into COS and read from it directly.

- Implements the interfaces of the Hadoop file system and provides the pseudo-hierarchical directory structure same as HDFS.

- Supports multipart uploads for a large file. Single file supports up to 19TB

- High performance and high availability. The performance difference between Hadoop-COS and HDFS is not more than 30%.


> Notes:

> Object Storage is not a file system and it has some limitations:

> 1. Object storage is a key-value storage and it does not support hierarchical directory naturally. Usually, using the directory separatory in object key to simulate the hierarchical directory, such as "/hadoop/data/words.dat".

> 2. COS Object storage can not support the object's append operation currently. It means that you can not append content to the end of an existing object(file).

> 3. Both `delete` and `rename` operations are non-atomic, which means that the operations are interrupted, the operation result may be inconsistent state.

> 4. Object storages have different authorization models:

>    - Directory permissions are reported as 777.

>    - File permissions are reported as 666.

>    - File owner is reported as the local current user.

>    - File group is also reported as the local current user.

> 5. Supports multipart uploads for a large file(up to 40TB), but the number of part is limited as 10000.

> 6. The number of files listed each time is limited to 1000.


## Quick Start

### Concepts

- **Bucket**: A container for storing data in COS. Its name is made up of user-defined bucketname and user appid.

- **Appid**: Unique resource identifier for the user dimension.

- **SecretId**: ID used to authenticate the user

- **SecretKey**: Key used to authenticate the user

- **Region**: The region where a bucket locates.

- **CosN**: Hadoop-COS uses `cosn` as its URI scheme, so CosN is often used to refer to Hadoop-COS.


### Usage

#### System Requirements

Linux kernel 2.6+


#### Dependencies

- cos_api (version 5.4.10 or later )
- cos-java-sdk (version 2.0.6 recommended)
- joda-time (version 2.9.9 recommended)
- httpClient (version 4.5.1 or later recommended)
- Jackson: jackson-core, jackson-databind, jackson-annotations (version 2.9.8 or later)
- bcprov-jdk15on (version 1.59 recommended)


#### Configure Properties

##### URI and Region Properties

If you plan to use COS as the default file system for Hadoop or other big data systems, you need to configure `fs.defaultFS` as the URI of Hadoop-COS in core-site.xml. Hadoop-COS uses `cosn` as its URI scheme, and the bucket as its URI host. At the same time, you need to explicitly set `fs.cosn.userinfo.region` to indicate the region your bucket locates.

**NOTE**:

- For Hadoop-COS, `fs.defaultFS` is an option. If you are only temporarily using the COS as a data source for Hadoop, you do not need to set the property, just specify the full URI when you use it. For example: `hadoop fs -ls cosn://testBucket-125236746/testDir/test.txt`.

- `fs.cosn.userinfo.region` is an required property for Hadoop-COS. The reason is that Hadoop-COS must know the region of the using bucket in order to accurately construct a URL to access it.

- COS supports multi-region storage, and different regions have different access domains by default. It is recommended to choose the nearest storage region according to your own business scenarios, so as to improve the object upload and download speed. You can find the available region from [https://intl.cloud.tencent.com/document/product/436/6224](https://intl.cloud.tencent.com/document/product/436/6224)

The following is an example for the configuration format:

```xml
    <property>
        <name>fs.defaultFS</name>
        <value>cosn://<bucket-appid></value>
        <description>
            Optional: If you don't want to use CosN as the default file system, you don't need to configure it.
        </description>
    </property>

    <property>
        <name>fs.cosn.bucket.region</name>
        <value>ap-xxx</value>
        <description>The region where the bucket is located</description>
    </property>

```


##### User Authentication Properties

Each user needs to properly configure the credentials ( User's secreteId and secretKey ) properly to access the object stored in COS. These credentials can be obtained from the official console provided by Tencent Cloud.

```xml
    <property>
        <name>fs.cosn.credentials.provider</name>
        <value>org.apache.hadoop.fs.auth.SimpleCredentialsProvider</value>
        <description>

            This option allows the user to specify how to get the credentials.
            Comma-separated class names of credential provider classes which implement
            com.qcloud.cos.auth.COSCredentialsProvider:

            1.org.apache.hadoop.fs.auth.SimpleCredentialsProvider: Obtain the secret id and secret key from fs.cosn.userinfo.secretId and fs.cosn.userinfo.secretKey in core-site.xml
            2.org.apache.hadoop.fs.auth.EnvironmentVariableCredentialsProvider: Obtain the secret id and secret key from system environment variables named COS_SECRET_ID and COS_SECRET_KEY

            If unspecified, the default order of credential providers is:
            1. org.apache.hadoop.fs.auth.SimpleCredentialsProvider
            2. org.apache.hadoop.fs.auth.EnvironmentVariableCredentialsProvider

        </description>
    </property>

    <property>
        <name>fs.cosn.userinfo.secretId</name>
        <value>xxxxxxxxxxxxxxxxxxxxxxxxx</value>
        <description>Tencent Cloud Secret Id </description>
    </property>

    <property>
        <name>fs.cosn.userinfo.secretKey</name>
        <value>xxxxxxxxxxxxxxxxxxxxxxxx</value>
        <description>Tencent Cloud Secret Key</description>
    </property>

```


##### Integration Properties

You need to explicitly specify the A and B options in order for Hadoop to properly integrate the COS as the underlying file system

Only correctly set `fs.cosn.impl` and `fs.AbstractFileSystem.cosn.impl` to enable Hadoop to integrate COS as its underlying file system. `fs.cosn.impl` must be set as `org.apache.hadoop.fs.cos.CosFileSystem` and `fs.AbstractFileSystem.cosn.impl` must be set as `org.apache.hadoop.fs.cos.CosN`.

```xml
    <property>
        <name>fs.cosn.impl</name>
        <value>org.apache.hadoop.fs.cosn.CosNFileSystem</value>
        <description>The implementation class of the CosN Filesystem</description>
    </property>

    <property>
        <name>fs.AbstractFileSystem.cosn.impl</name>
        <value>org.apache.hadoop.fs.cos.CosN</value>
        <description>The implementation class of the CosN AbstractFileSystem.</description>
    </property>

```

##### Other Runtime Properties

Hadoop-COS provides rich runtime properties to set, and most of these do not require custom values because a well-run default value provided for them.

**It is important to note that**:

- Hadoop-COS will generate some temporary files and consumes some disk space. All temporary files would be placed in the directory specified by option `fs.cosn.tmp.dir` (Default: /tmp/hadoop_cos);

- The default block size is 8MB and it means that you can only upload a single file up to 78GB into the COS blob storage system. That is mainly due to the fact that the multipart-upload can only support up to 10,000 blocks. For this reason, if needing to support larger single files, you must increase the block size accordingly by setting the property `fs.cosn.block.size`. For example, the size of the largest single file is 1TB, the block size is at least greater than or equal to (1 \* 1024 \* 1024 \* 1024 \* 1024)/10000 = 109951163. Currently, the maximum support file is 19TB (block size: 2147483648)

```xml
    <property>
        <name>fs.cosn.tmp.dir</name>
        <value>/tmp/hadoop_cos</value>
        <description>Temporary files would be placed here.</description>
    </property>

    <property>
        <name>fs.cosn.buffer.size</name>
        <value>33554432</value>
        <description>The total size of the buffer pool.</description>
    </property>

    <property>
        <name>fs.cosn.block.size</name>
        <value>8388608</value>
        <description>
        Block size to use cosn filesysten, which is the part size for MultipartUpload. Considering the COS supports up to 10000 blocks, user should estimate the maximum size of a single file. For example, 8MB part size can allow  writing a 78GB single file.
        </description>
    </property>

    <property>
        <name>fs.cosn.maxRetries</name>
        <value>3</value>
        <description>
      The maximum number of retries for reading or writing files to COS, before throwing a failure to the application.
        </description>
    </property>

    <property>
        <name>fs.cosn.retry.interval.seconds</name>
        <value>3</value>
        <description>The number of seconds to sleep between each COS retry.</description>
    </property>

```


##### Properties Summary

| properties | description | default value | required |
|:----------:|:-----------|:-------------:|:--------:|
| fs.defaultFS | Configure the default file system used by Hadoop.| None | NO |
| fs.cosn.credentials.provider | This option allows the user to specify how to get the credentials. Comma-separated class names of credential provider classes which implement com.qcloud.cos.auth.COSCredentialsProvider: <br/> 1. org.apache.hadoop.fs.cos.auth.SimpleCredentialsProvider: Obtain the secret id and secret key from `fs.cosn.userinfo.secretId` and `fs.cosn.userinfo.secretKey` in core-site.xml; <br/> 2. org.apache.hadoop.fs.auth.EnvironmentVariableCredentialsProvider: Obtain the secret id and secret key from system environment variables named `COSN_SECRET_ID` and `COSN_SECRET_KEY`. <br/> <br/> If unspecified, the default order of credential providers is: <br/> 1. org.apache.hadoop.fs.auth.SimpleCredentialsProvider; <br/> 2. org.apache.hadoop.fs.auth.EnvironmentVariableCredentialsProvider. | None | NO |
| fs.cosn.userinfo.secretId/secretKey | The API key information of your account | None | YES |
| fs.cosn.bucket.region | The region where the bucket is located. | None | YES |
| fs.cosn.impl | The implementation class of the CosN filesystem. | None | YES |
| fs.AbstractFileSystem.cosn.impl | The implementation class of the CosN AbstractFileSystem. | None | YES |
| fs.cosn.tmp.dir | Temporary files generated by cosn would be stored here during the program running. | /tmp/hadoop_cos | NO |
| fs.cosn.buffer.size | The total size of the buffer pool. Require greater than or equal to block size. | 33554432 | NO |
| fs.cosn.block.size | The size of file block. Considering the limitation that each file can be divided into a maximum of 10,000 to upload, the option must be set according to the maximum size of used single file. For example, 8MB part size can allow  writing a 78GB single file. | 8388608 | NO |
| fs.cosn.upload_thread_pool | Number of threads used for concurrent uploads when files are streamed to COS. | CPU core number * 3 | NO |
| fs.cosn.read.ahead.block.size | The size of each read-ahead block. | 524288 (512KB) | NO |
| fs.cosn.read.ahead.queue.size | The length of readahead queue. | 10 | NO |
| fs.cosn.maxRetries | The maxium number of retries for reading or writing files to COS, before throwing a failure to the application. | 3 | NO |
| fs.cosn.retry.interval.seconds | The number of seconds to sleep between each retry | 3 | NO |


#### Command Usage

Command format: `hadoop fs -ls -R cosn://bucket-appid/<path>` or `hadoop fs -ls -R /<path>`, the latter requires the defaultFs option to be set as `cosn`.


#### Example

Use CosN as the underlying file system to run the WordCount routine:

```shell
bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-x.x.x.jar wordcount cosn://example/mr/input.txt cosn://example/mr/output
```

If setting CosN as the default file system for Hadoop, you can run it as follows:

```shell
bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-x.x.x.jar wordcount /mr/input.txt /mr/output
```

## Testing the hadoop-cos Module

To test CosN filesystem, the following two files which pass in authentication details to the test runner are needed.

1. auth-keys.xml
2. core-site.xml

These two files need to be created under the `hadoop-cloud-storage-project/hadoop-cos/src/test/resource` directory.


### `auth-key.xml`

COS credentials can specified in `auth-key.xml`. At the same time, it is also a trigger for the CosN filesystem tests.
COS bucket URL should be provided by specifying the option: `test.fs.cosn.name`.

An example of the `auth-keys.xml` is as follow:

```xml
<configuration>
    <property>
        <name>test.fs.cosn.name</name>
        <value>cosn://testbucket-12xxxxxx</value>
    </property>
    <property>
        <name>fs.cosn.bucket.region</name>
        <value>ap-xxx</value>
        <description>The region where the bucket is located</description>
    </property>
    <property>
        <name>fs.cosn.userinfo.secretId</name>
        <value>AKIDXXXXXXXXXXXXXXXXXXXX</value>
    </property>
    <property>
        <name>fs.cosn.userinfo.secretKey</name>
        <value>xxxxxxxxxxxxxxxxxxxxxxxxx</value>
    </property>
</configuration>


```

Without this file, all tests in this module will be skipped.

### `core-site.xml`

This file pre-exists and sources the configurations created in auth-keys.xml.
For most cases, no modification is needed, unless a specific, non-default property needs to be set during the testing.

### `contract-test-options.xml`

All configurations related to support contract tests need to be specified in `contract-test-options.xml`. Here is an example of `contract-test-options.xml`.

```xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <include xmlns="http://www.w3.org/2001/XInclude"
             href="auth-keys.xml"/>
    <property>
        <name>fs.contract.test.fs.cosn</name>
        <value>cosn://testbucket-12xxxxxx</value>
    </property>

    <property>
        <name>fs.cosn.bucket.region</name>
        <value>ap-xxx</value>
        <description>The region where the bucket is located</description>
    </property>

</configuration>

```

If the option `fs.contract.test.fs.cosn` not definded in the file, all contract tests will be skipped.

## Other issues

### Performance Loss

The IO performance of COS is lower than HDFS in principle, even on virtual clusters running on Tencent CVM.

The main reason can be attributed to the following points:

- HDFS replicates data for faster query.

- HDFS is significantly faster for many “metadata” operations: listing the contents of a directory, calling getFileStatus() on path, creating or deleting directories.

- HDFS stores the data on the local hard disks, avoiding network traffic if the code can be executed on that host. But access to the object storing in COS requires access to network almost each time. It is a critical point in damaging IO performance. Hadoop-COS also do a lot of optimization work for it, such as the pre-read queue, the upload buffer pool, the concurrent upload thread pool, etc.

- File IO performing many seek calls/positioned read calls will also encounter performance problems due to the size of the HTTP requests made. Despite the pre-read cache optimizations, a large number of random reads can still cause frequent network requests.

- On HDFS, both the `rename` and `mv` for a directory or a file are an atomic and O(1)-level operation, but in COS, the operation need to combine `copy` and `delete` sequentially. Therefore, performing rename and move operations on a COS object is not only low performance, but also difficult to guarantee data consistency.

At present, using the COS blob storage system through Hadoop-COS occurs about 20% ~ 25% performance loss compared to HDFS. But, the cost of using COS is lower than HDFS, which includes both storage and maintenance costs.
