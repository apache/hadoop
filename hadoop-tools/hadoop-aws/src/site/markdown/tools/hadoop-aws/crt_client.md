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

# Using the S3 CRT client

This document explains usage of the CRT client.

The [AWS CRT-based S3 client](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/crt-based-s3-client.html)
is built on top of the AWS Common Runtime (CRT), is an alternative S3 asynchronous client. It can provide higher
transfer throughput from S3 due to its enhanced connection pool management. More information
can be found
[here](https://aws.amazon.com/blogs/developer/introducing-crt-based-s3-client-and-the-s3-transfer-manager-in-the-aws-sdk-for-java-2-x/).

When making multiple parallel GET requests, using the CRT ensures load is evenly distributed across S3 front-end. 
This can be useful for all three input streams available with versions >= 3.4.2, as:
* The Analytics accelerator will make parallel GETs for columns it predicts will be required on a Parquet file open.
* The Prefetch input stream can make up to 8 parallel 8MB GETs by default.
* The Classic input stream will make async parallel GETs for column reads when using VectoredIO.

Since the CRT client is an async client, when enabled in S3A, it will currently be used wherever
* When reading data using the Analytics stream.
* When copying files between buckets using the Transfer manager, for example, on a rename.
* When copying from the local file system to S3.

This is because all other operations in S3A currently use the S3 Sync client, and so cannot be replaced by an
asynchronous client. The move to an async client is tracked in
[HADOOP-18877](https://issues.apache.org/jira/browse/HADOOP-18877).

## Enabling the CRT Client
The CRT client can be enabled as follows:

```xml
    <property>
        <name>fs.s3a.crt.enabled</name>
        <value>true</value>
    </property>
```

## Limitations

Using the CRT client currently comes with the following limitations:

* The CRT client has limited options for configurations, this means only certain connection level configurations are
applied:
  * `fs.s3a.connection.maximum`
  * `fs.s3a.connection.establish.timeout`
* No request level timeouts.
* No support for configuring custom signers, signers registered via `fs.s3a.custom.signers`, will not be set.
* No support for execution interceptors, so auditing is not supported.
* Multipart uploads cannot be disabled, so copy requests which use the transfer manager will be split into 8MB. The
transfer manager is used when:
  * the file size to copy is >= value of `fs.s3a.multipart.threshold`. Default value of this configuration is 128MB.
  * multipart copying is not explicitly disabled using `fs.s3a.multipart.uploads.enabled`
* The CRT client is written in C, and used with the SDK via Java bindings. This means on failures, the entire stack
trace is not surfaced up and can make debugging issues challenging. 