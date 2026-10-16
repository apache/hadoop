
<!---
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
-->
# Apache Hadoop  3.5.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-19019](https://issues.apache.org/jira/browse/HADOOP-19019) | *Major* | **Parallel Maven Build Support for Apache Hadoop**

Support to parallel building with maven command, such as \`mvn -T 8 clean package -DskipTests\`.


---

* [HADOOP-18830](https://issues.apache.org/jira/browse/HADOOP-18830) | *Major* | **S3A: Cut S3 Select**

S3 Select is no longer supported through the S3A connector


---

* [HADOOP-18993](https://issues.apache.org/jira/browse/HADOOP-18993) | *Minor* | **S3A: Add option fs.s3a.classloader.isolation (#6301)**

If the user wants to load custom implementations of AWS Credential Providers through user provided jars can set {{fs.s3a.extensions.isolated.classloader}} to {{false}}.


---

* [HADOOP-19084](https://issues.apache.org/jira/browse/HADOOP-19084) | *Blocker* | **prune dependency exports of hadoop-\* modules**

maven/ivy imports of hadoop-common are less likely to end up with log4j versions on their classpath.


---

* [HADOOP-19101](https://issues.apache.org/jira/browse/HADOOP-19101) | *Blocker* | **Vectored Read into off-heap buffer broken in fallback implementation**

PositionedReadable.readVectored() will read incorrect data when reading from hdfs, azure abfs and other stores when given a direct buffer allocator. 

For cross-version compatibility, use on-heap buffer allocators only


---

* [HADOOP-19107](https://issues.apache.org/jira/browse/HADOOP-19107) | *Major* | **Drop support for HBase v1 timeline service & upgrade HBase v2**

Drop support for Hbase V1 as the back end of the YARN Application Timeline service, which becomes HBase 2 only. The supported version HBase version is 2.5.8

This does not have any effect on HBase deployments themselves


---

* [HADOOP-19152](https://issues.apache.org/jira/browse/HADOOP-19152) | *Major* | **Do not hard code security providers.**

Added a new conf "hadoop.security.crypto.jce.provider.auto-add" (default: true) to enable/disable auto-adding BouncyCastleProvider.  This change also avoid statically loading the BouncyCastleProvider class.


---

* [HDFS-17534](https://issues.apache.org/jira/browse/HDFS-17534) | *Major* | **RBF: Support leader follower mode for multiple subclusters**

Adds new Mount Table mode: LEADER\_FOLLOWER for RBF


---

* [HADOOP-19120](https://issues.apache.org/jira/browse/HADOOP-19120) | *Major* | **[ABFS]: ApacheHttpClient adaptation as network library**

Apache httpclient 4.5.x is a new implementation of http connections; this supports a large configurable pool of connections along with the ability to limit their lifespan.

The networking library can be chosen using the configuration
option fs.azure.networking.library

The supported values are
- JDK\_HTTP\_URL\_CONNECTION : Use JDK networking library  [Default]
- APACHE\_HTTP\_CLIENT : Use Apache HttpClient

Important: when the networking library is switched to
the Apache http client, the apache httpcore and httpclient must be on the classpath.


---

* [HADOOP-18487](https://issues.apache.org/jira/browse/HADOOP-18487) | *Major* | **Make protobuf 2.5 an optional runtime dependency.**

hadoop modules no longer export protobuf-2.5.0 as a dependency, and it is omitted from the hadoop distribution directory. Applications which use the library must declare an explicit dependency.

Hadoop uses a shaded version of protobuf3 internally, and does not use the 2.5.0 JAR except when compiling compatible classes. It is still included in the binary distributions when the yarn timeline server is built with hbase 1


---

* [HADOOP-19221](https://issues.apache.org/jira/browse/HADOOP-19221) | *Major* | **S3A: Unable to recover from failure of multipart block upload attempt "Status Code: 400; Error Code: RequestTimeout"**

S3A upload operations can now recover from failures where the store returns a 500 error. There is an option to control whether or not the S3A client itself attempts to retry on a 50x error other than 503 throttling events (which are independently processed as before). Option: fs.s3a.retry.http.5xx.errors . Default: true


---

* [HADOOP-19165](https://issues.apache.org/jira/browse/HADOOP-19165) | *Major* | **Explore dropping protobuf 2.5.0 from the distro**

protobuf-2.5.jar is no longer bundled into a distribution or exported as a transient dependency.


---

* [HADOOP-15760](https://issues.apache.org/jira/browse/HADOOP-15760) | *Major* | **Upgrade commons-collections to commons-collections4**

Hadoop has upgraded to commons-collections4-4.4.  This MUST be on the classpath to create a Configuration() object.  The hadoop-commons dependency exports has been updated appropriately. If dependencies are configured manually, these MUST be updated.
Applications which require the older "commons-collections" binaries on their classpath may have to explicitly add them.


---

* [HADOOP-19315](https://issues.apache.org/jira/browse/HADOOP-19315) | *Major* | **Bump avro from 1.9.2 to 1.11.4**

Backwards-incompatible upgrade for security reasons. All field access is now via setter/getter methods.  To marshal Serializable objects  the packages they are in must be declared in the system property "{{org.apache.avro.SERIALIZABLE\_PACKAGES}}"

This upgrade does break our compatibility policy -but this is a critical security issue. Everyone using an avro version before 1.11.4 MUST upgrade.


---

* [YARN-10058](https://issues.apache.org/jira/browse/YARN-10058) | *Major* | **Capacity Scheduler dispatcher hang when async thread crash**

**WARNING: No release note provided for this change.**


---

* [HDFS-17384](https://issues.apache.org/jira/browse/HDFS-17384) | *Major* | **HDFS NameNode Fine-Grained Locking Phase I**

NN fine-grained locking (FGL) implementation aims to alleviate the bottleneck by allowing concurrency of disjoint write operations. This is the phase one works.
See [HDFS-17384](https://issues.apache.org/jira/browse/HDFS-17384) for more information on this feature.


---

* [HDFS-17531](https://issues.apache.org/jira/browse/HDFS-17531) | *Major* | **RBF: Asynchronous router RPC**

Asynchronous router RPC is designed to address the performance bottlenecks of synchronous router RPC in scenarios with high concurrency and multiple named services.
By introducing an asynchronous processing mechanism, it optimizes the request-handling process, enhances the system's concurrency capacity and resource utilization efficiency.

See the Apache JIRA ticket [HDFS-17531](https://issues.apache.org/jira/browse/HDFS-17531) for more information on this feature.
Further improvements are in the Apache JIRA ticket [HDFS-17716](https://issues.apache.org/jira/browse/HDFS-17716).


---

* [HADOOP-19225](https://issues.apache.org/jira/browse/HADOOP-19225) | *Major* | **Upgrade Jetty to 9.4.57.v20241219 due to CVE-2024-8184 and other CVEs**

Jetty has been upgraded to address CVE-2024-22201, CVE-2023-44487, CVE-2024-8184, CVE-2024-13009


---

* [HADOOP-15224](https://issues.apache.org/jira/browse/HADOOP-15224) | *Minor* | **S3A: Add option to set checksum on S3 objects**

The option  fs.s3a.create.checksum.algorithm allows checksums to be set on file upload; It supports the following values:     'CRC32', 'CRC32C', 'SHA1', and 'SHA256'. A future release will also support "none".


---

* [HDFS-16644](https://issues.apache.org/jira/browse/HDFS-16644) | *Major* | **java.io.IOException Invalid token in javax.security.sasl.qop**

A Hadoop 2.10.1 or 2.10.2 client connecting to a Hadoop 3.4.0~3.4.1, 3.3.0~3.3.6, 3.2.1~3.2.4 (any version with HDFS-13541) cluster could cause the DataNode to disconnect any subsequent client connections due to an incompatible binary protocol change.

HDFS-16644 provides a partial fix: a Hadoop 3 cluster will reject Hadoop 2.10.1/2.10.2 clients, but it will not fail other subsequent client connections.

For Hadoop 2 cluster wishing to upgrade to Hadoop 3 in a rolling fashion, the workaround is to perform a two-step upgrade: upgrade to an earlier Hadoop 3 version without HDFS-13541, and then upgrade again to the newer Hadoop 3 version. Or revert HDFS-13541 from your version and rebuild.


---

* [HADOOP-19256](https://issues.apache.org/jira/browse/HADOOP-19256) | *Major* | **S3A: Support S3 Conditional Writes**

S3A client now uses S3 conditional overwrite PUT requests to perform overwrite protection checks at end of PUT request (stream close()). This saves a HEAD request on file creation, and actually delivers an atomic creation. It may not be supported on third party stores: set fs.s3a.create.conditional.enabled to false to revert to the old behavior. Consult the third-party-stores documentation for details.


---

* [HADOOP-19485](https://issues.apache.org/jira/browse/HADOOP-19485) | *Major* | **S3A: Upgrade AWS V2 SDK to 2.29.52**

AWS SDK 2.30.0 and later are (currently) incompatible with third party stores. Accordingly, this release is kept at a 2.29 version. See HADOOP-19490. There may now be some problems using the AWS4SignerType and S3.


---

* [HADOOP-19557](https://issues.apache.org/jira/browse/HADOOP-19557) | *Critical* | **S3A: S3ABlockOutputStream to never log/reject hflush(): calls**

S3A output streams no longer log warnings on use of hflush()/raise exceptions if fs.s3a.downgrade.syncable.exceptions = false. hsync() is reported with a warning/rejected, as appropriate. That method us absolutely unsupported when writing to S3


---

* [HADOOP-18296](https://issues.apache.org/jira/browse/HADOOP-18296) | *Minor* | **Memory fragmentation in ChecksumFileSystem Vectored IO implementation.**

Option "fs.file.checksum.verify" disables checksum
verification in local FS, so sliced subsets of larger buffers are
never returned. 

Stream capability  "fs.capability.vectoredio.sliced" is true
if a filesystem knows that it is returning slices of a larger buffer.
This is false if a filesystem doesn't, or against the local
FS in releases which lack this feature.


---

* [HADOOP-19343](https://issues.apache.org/jira/browse/HADOOP-19343) | *Major* | **Add native support for GCS connector**

Apache Hadoop now includes a FileSystem implementation backed by Google Cloud Storage (GCS), accessible through the gs:// URI scheme.


---

* [HADOOP-19680](https://issues.apache.org/jira/browse/HADOOP-19680) | *Critical* | **Update non-thirdparty Guava version to 32.0.1**

Previously, Hadoop has set the guava dependency version to 27.0-jre.
It is now set to 32.0.1-jre .
Note that this only applies to the un-relocated guava library, the Hadoop thirdparty guava library was already at 32.0.1-jre.


---

* [HADOOP-19719](https://issues.apache.org/jira/browse/HADOOP-19719) | *Major* | **Upgrade to wildfly version with support for openssl 3**

Wildfly SSL binding are now compatible with OpenSSL 3.0 as well as 1.1. However, as the native libraries were built with GLIBC 2.34+, these do not work on RHEL8. For deployment on those systems, stick with the JVM ssl support


---

* [HADOOP-19654](https://issues.apache.org/jira/browse/HADOOP-19654) | *Major* | **Upgrade AWS SDK to 2.35.4**

Checksum calculation in the AWS SDK has changed and may be incompatible with third party stores. Consult the third-party documentation for details on restoring compatibility with the options "fs.s3a.md5.header.enabled" and "fs.s3a.checksum.calculation.enabled"


---

* [HADOOP-19696](https://issues.apache.org/jira/browse/HADOOP-19696) | *Major* | **hadoop binary distribution to move cloud connectors to hadoop common/lib**

The hadoop-\* modules needed to communicate with cloud stores (hadoop-azure, hadoop-aws etc) are now in share/hadoop/tools/lib, and will be automatically available on the command line provided all their dependencies are in the same directory. For hadoop-aws, that means aws sdk bundle.jar 2.35.4 or later; for hadoop-azure everything should work out the box.


---

* [YARN-11886](https://issues.apache.org/jira/browse/YARN-11886) | *Major* | **Introduce Capacity Scheduler UI**

Introduced a new modern UI for monitoring the Capacity Scheduler and dynamically editing its configuration.


---

* [HADOOP-19747](https://issues.apache.org/jira/browse/HADOOP-19747) | *Major* | **switch lz4-java to at.yawk.lz4 version due to CVE**

The hadoop Lz4Compressor  instantiates a compressor via a call to  LZ4Factory.fastestInstance().safeDecompressor() and so is not directly vulnerable to CVE-2025-12183


---

* [YARN-11888](https://issues.apache.org/jira/browse/YARN-11888) | *Major* | **Serve the Capacity Scheduler UI**

The new Capacity Scheduler configuration UI is now served under rm-host:port/scheduler-ui


---

* [YARN-11916](https://issues.apache.org/jira/browse/YARN-11916) | *Minor* | **FileSystemTimelineReaderImpl vulnerable to race conditions**

People testing timeline server integration through the FileSystemTimelineReaderImpl implementation of ATSv2 need to add hadoop-yarn-server-timelineservice-test.jar to their testing classpath


---

* [HADOOP-19778](https://issues.apache.org/jira/browse/HADOOP-19778) | *Major* | **Remove Deprecated WASB Code from Hadoop**

WASB Driver Client Is removed from official hadoop releases starting from hadoop-3.5.0.

Moving ahead ABFS Driver will be the only official Hadoop connector for both types of Accounts that is "Heirarchichal Namespace Enabled (HNS)" and "Heirarchichal Namespace Disabled (FNS)"

For more details refer to: https://github.com/apache/hadoop/blob/trunk/hadoop-tools/hadoop-azure/src/site/markdown/wasb.md


---

* [YARN-11885](https://issues.apache.org/jira/browse/YARN-11885) | *Major* | **[Umbrella] YARN Capacity Scheduler UI**

Introduced a modern UI for updating and managing the YARN Capacity Scheduler configuration.



