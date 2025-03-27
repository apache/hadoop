
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
# Apache Hadoop  3.4.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-16054](https://issues.apache.org/jira/browse/HADOOP-16054) | *Major* | **Update Dockerfile to use Bionic**

The build image has been upgraded to Bionic.


---

* [HDFS-15281](https://issues.apache.org/jira/browse/HDFS-15281) | *Major* | **ZKFC ignores dfs.namenode.rpc-bind-host and uses dfs.namenode.rpc-address to bind to host address**

ZKFC binds host address to "dfs.namenode.servicerpc-bind-host", if configured. Otherwise, it binds to "dfs.namenode.rpc-bind-host". If neither of those is configured, ZKFC binds itself to NameNode RPC server address (effectively "dfs.namenode.rpc-address").


---

* [HADOOP-17010](https://issues.apache.org/jira/browse/HADOOP-17010) | *Major* | **Add queue capacity weights support in FairCallQueue**

When FairCallQueue is enabled, user can specify capacity allocation among all sub-queues via configuration “ipc.\<port\>.callqueue.capacity.weights”. The value of this config is a comma-separated list of positive integers, each of which specifies the weight associated with the sub-queue at that index. This list length should be IPC scheduler priority levels, defined by "scheduler.priority.levels". 

By default, each sub-queue is associated with weight 1, i.e., all sub-queues are allocated with the same capacity.


---

* [HADOOP-17024](https://issues.apache.org/jira/browse/HADOOP-17024) | *Major* | **ListStatus on ViewFS root (ls "/") should list the linkFallBack root (configured target root).**

ViewFS#listStatus on root("/") considers listing from fallbackLink if available. If the same directory name is present in configured mount path as well as in fallback link, then only the configured mount path will be listed in the returned result.


---

* [HDFS-15288](https://issues.apache.org/jira/browse/HDFS-15288) | *Major* | **Add Available Space Rack Fault Tolerant BPP**

Added a new BlockPlacementPolicy: "AvailableSpaceRackFaultTolerantBlockPlacementPolicy" which uses the same optimization logic as the AvailableSpaceBlockPlacementPolicy along with spreading the replicas across maximum number of racks, similar to BlockPlacementPolicyRackFaultTolerant.
The BPP can be configured by setting the blockplacement policy class as org.apache.hadoop.hdfs.server.blockmanagement.AvailableSpaceRackFaultTolerantBlockPlacementPolicy


---

* [HDFS-13183](https://issues.apache.org/jira/browse/HDFS-13183) | *Major* | **Standby NameNode process getBlocks request to reduce Active load**

Enable balancer to redirect getBlocks request to a Standby Namenode, thus reducing the performance impact of balancer to the Active NameNode.

The feature is disabled by default. To enable it, configure the hdfs-site.xml of balancer: 
dfs.ha.allow.stale.reads = true.


---

* [HADOOP-17079](https://issues.apache.org/jira/browse/HADOOP-17079) | *Major* | **Optimize UGI#getGroups by adding UGI#getGroupsSet**

Added a UserGroupMapping#getGroupsSet() API and deprecate UserGroupMapping#getGroups.

The UserGroupMapping#getGroups() can be expensive as it involves Set-\>List conversion. For user with large group membership (i.e., \> 1000 groups), we recommend using getGroupSet to avoid the conversion and fast membership look up.


---

* [HDFS-15385](https://issues.apache.org/jira/browse/HDFS-15385) | *Critical* | **Upgrade boost library to 1.72**

Boost 1.72 is required when building native code.


---

* [HADOOP-17091](https://issues.apache.org/jira/browse/HADOOP-17091) | *Major* | **[JDK11] Fix Javadoc errors**

<!-- markdown -->
* Upgraded to Yetus master to change javadoc goals.
* Changed javadoc goals from `javadoc:javadoc` to `process-sources javadoc:javadoc-no-fork`.
* Javadoc with JDK11 fails in some modules. Ignored them for now and we will fix them later.


---

* [HADOOP-17215](https://issues.apache.org/jira/browse/HADOOP-17215) | *Major* | **ABFS: Support for conditional overwrite**

ABFS: Support for conditional overwrite.


---

* [HDFS-15025](https://issues.apache.org/jira/browse/HDFS-15025) | *Major* | **Applying NVDIMM storage media to HDFS**

Add a new storage type NVDIMM and a new storage policy ALL\_NVDIMM for HDFS. The NVDIMM storage type is for non-volatile random-access memory storage medias whose data survives when DataNode restarts.


---

* [HDFS-15098](https://issues.apache.org/jira/browse/HDFS-15098) | *Major* | **Add SM4 encryption method for HDFS**

New encryption codec "SM4/CTR/NoPadding" is added. Requires openssl version \>=1.1.1 for native implementation.


---

* [YARN-9809](https://issues.apache.org/jira/browse/YARN-9809) | *Major* | **NMs should supply a health status when registering with RM**

Improved node registration with node health status.


---

* [HADOOP-17125](https://issues.apache.org/jira/browse/HADOOP-17125) | *Major* | **Using snappy-java in SnappyCodec**

The SnappyCodec uses the snappy-java compression library, rather than explicitly referencing native binaries.  It contains the native libraries for many operating systems and instruction sets, falling back to a pure java implementation. It does requires the snappy-java.jar is on the classpath. It can be found in hadoop-common/lib, and has already been present as part of the avro dependencies


---

* [HDFS-15253](https://issues.apache.org/jira/browse/HDFS-15253) | *Major* | **Set default throttle value on dfs.image.transfer.bandwidthPerSec**

The configuration dfs.image.transfer.bandwidthPerSec which defines the maximum bandwidth available for fsimage transfer is changed from 0 (meaning no throttle at all) to 50MB/s.


---

* [HADOOP-17292](https://issues.apache.org/jira/browse/HADOOP-17292) | *Major* | **Using lz4-java in Lz4Codec**

The Hadoop's LZ4 compression codec now depends on lz4-java. The native LZ4 is performed by the encapsulated JNI and it is no longer necessary to install and configure the lz4 system package.

The lz4-java is declared in provided scope. Applications that wish to use lz4 codec must declare dependency on lz4-java explicitly.


---

* [HDFS-15380](https://issues.apache.org/jira/browse/HDFS-15380) | *Major* | **RBF: Could not fetch real remote IP in RouterWebHdfsMethods**

**WARNING: No release note provided for this change.**


---

* [HDFS-15719](https://issues.apache.org/jira/browse/HDFS-15719) | *Critical* | **[Hadoop 3] Both NameNodes can crash simultaneously due to the short JN socket timeout**

The default value of the configuration hadoop.http.idle\_timeout.ms (how long does Jetty disconnect an idle connection) is changed from 10000 to 60000. 
This property is inlined during compile time, so an application that references this property must be recompiled in order for it to take effect.


---

* [HADOOP-16492](https://issues.apache.org/jira/browse/HADOOP-16492) | *Major* | **Support HuaweiCloud Object Storage as a Hadoop Backend File System**

Added support for HuaweiCloud OBS (https://www.huaweicloud.com/en-us/product/obs.html) to Hadoop file system, just like what we do before for S3, ADLS, OSS, etc. With simple configuration, Hadoop applications can read/write data from OBS without any code change.


---

* [HDFS-15767](https://issues.apache.org/jira/browse/HDFS-15767) | *Major* | **RBF: Router federation rename of directory.**

Added support for rename across namespaces for RBF through DistCp. By default the option is turned off, needs to be explicitly turned on by setting dfs.federation.router.federation.rename.option to DISTCP along with the configurations required for running DistCp. In general the client timeout should also be high enough to use this functionality to ensure that the client doesn't timeout.


---

* [HADOOP-17424](https://issues.apache.org/jira/browse/HADOOP-17424) | *Major* | **Replace HTrace with No-Op tracer**

Dependency on HTrace and TraceAdmin protocol/utility were removed. Tracing functionality is no-op until alternative tracer implementation is added.


---

* [HDFS-15683](https://issues.apache.org/jira/browse/HDFS-15683) | *Major* | **Allow configuring DISK/ARCHIVE capacity for individual volumes**

Add a new configuration "dfs.datanode.same-disk-tiering.capacity-ratio.percentage" to allow admins to configure capacity for individual volumes on the same mount.


---

* [HDFS-15814](https://issues.apache.org/jira/browse/HDFS-15814) | *Major* | **Make some parameters configurable for DataNodeDiskMetrics**

**WARNING: No release note provided for this change.**


---

* [HADOOP-16748](https://issues.apache.org/jira/browse/HADOOP-16748) | *Major* | **Migrate to Python 3 and upgrade Yetus to 0.13.0**

<!-- markdown -->
- Upgraded Yetus to 0.13.0.
- Removed determine-flaky-tests-hadoop.py.
- Temporarily disabled shelldocs check in the Jenkins jobs due to YETUS-1099.


---

* [HADOOP-17514](https://issues.apache.org/jira/browse/HADOOP-17514) | *Minor* | **Remove trace subcommand from hadoop CLI**

\`trace\` subcommand of hadoop CLI was removed as a follow-up of removal of TraceAdmin protocol.


---

* [HADOOP-17482](https://issues.apache.org/jira/browse/HADOOP-17482) | *Minor* | **Remove Commons Logger from FileSystem Class**

The protected commons-logging LOG has been removed from the FileSystem class. Any FileSystem implementation which referenced this for logging will no longer link. Fix: move these implementations to using their own, private SLF4J log (or other logging API)


---

* [HADOOP-17531](https://issues.apache.org/jira/browse/HADOOP-17531) | *Critical* | **DistCp: Reduce memory usage on copying huge directories**

Added a -useiterator option in distcp which uses listStatusIterator for building the listing. Primarily to reduce memory usage at client for building listing.


---

* [HADOOP-16870](https://issues.apache.org/jira/browse/HADOOP-16870) | *Major* | **Use spotbugs-maven-plugin instead of findbugs-maven-plugin**

Removed findbugs from the hadoop build images and added spotbugs instead.
Upgraded SpotBugs to 4.2.2 and spotbugs-maven-plugin to 4.2.0.


---

* [HADOOP-17222](https://issues.apache.org/jira/browse/HADOOP-17222) | *Major* | ** Create socket address leveraging URI cache**

DFS client can use the newly added URI cache when creating socket address for read operations. By default it is disabled. When enabled, creating socket address will use cached URI object based on host:port to reduce the frequency of URI object creation.

To enable it, set the following config key to true:
\<property\>
  \<name\>dfs.client.read.uri.cache.enabled\</name\>
  \<value\>true\</value\>
\</property\>


---

* [HADOOP-16524](https://issues.apache.org/jira/browse/HADOOP-16524) | *Major* | **Automatic keystore reloading for HttpServer2**

Adds auto-reload of keystore.

Adds below new config (default 10 seconds):

 ssl.{0}.stores.reload.interval

The refresh interval used to check if either of the truststore or keystore certificate file has changed.


---

* [HDFS-15942](https://issues.apache.org/jira/browse/HDFS-15942) | *Major* | **Increase Quota initialization threads**

The default quota initialization thread count during the NameNode startup process (dfs.namenode.quota.init-threads) is increased from 4 to 12.


---

* [HADOOP-17524](https://issues.apache.org/jira/browse/HADOOP-17524) | *Major* | **Remove EventCounter and Log counters from JVM Metrics**

Removed log counter from JVMMetrics because this feature strongly depends on Log4J 1.x API.


---

* [HDFS-15975](https://issues.apache.org/jira/browse/HDFS-15975) | *Major* | **Use LongAdder instead of AtomicLong**

This JIRA changes public fields in DFSHedgedReadMetrics. If you are using the public member variables of DFSHedgedReadMetrics, you need to use them through the public API.


---

* [HADOOP-17650](https://issues.apache.org/jira/browse/HADOOP-17650) | *Major* | **Fails to build using Maven 3.8.1**

In order to resolve build issues with Maven 3.8.1, we have to bump SolrJ to latest version 8.8.2 as of now. Solr is used by YARN application catalog. Hence, we would recommend upgrading Solr cluster accordingly before upgrading entire Hadoop cluster to 3.4.0 if the YARN application catalog service is used.


---

* [YARN-9279](https://issues.apache.org/jira/browse/YARN-9279) | *Major* | **Remove the old hamlet package**

org.apache.hadoop.yarn.webapp.hamlet package was removed. Use org.apache.hadoop.yarn.webapp.hamlet2 instead.


---

* [YARN-10820](https://issues.apache.org/jira/browse/YARN-10820) | *Major* | **Make GetClusterNodesRequestPBImpl thread safe**

Added syncronization so that the "yarn node list" command does not fail intermittently


---

* [HDFS-16265](https://issues.apache.org/jira/browse/HDFS-16265) | *Blocker* | **Refactor HDFS tool tests for better reuse**

**WARNING: No release note provided for this change.**


---

* [HADOOP-17956](https://issues.apache.org/jira/browse/HADOOP-17956) | *Major* | **Replace all default Charset usage with UTF-8**

All of the default charset usages have been replaced to UTF-8. If the default charset of your environment is not UTF-8, the behavior can be different.


---

* [HDFS-16278](https://issues.apache.org/jira/browse/HDFS-16278) | *Major* | **Make HDFS snapshot tools cross platform**

**WARNING: No release note provided for this change.**


---

* [HDFS-16285](https://issues.apache.org/jira/browse/HDFS-16285) | *Major* | **Make HDFS ownership tools cross platform**

**WARNING: No release note provided for this change.**


---

* [YARN-8234](https://issues.apache.org/jira/browse/YARN-8234) | *Critical* | **Improve RM system metrics publisher's performance by pushing events to timeline server in batch**

When Timeline Service V1 or V1.5 is used, if "yarn.resourcemanager.system-metrics-publisher.timeline-server-v1.enable-batch" is set to true, ResourceManager sends timeline events in batch. The default value is false. If this functionality is enabled, the maximum number that events published in batch is configured by "yarn.resourcemanager.system-metrics-publisher.timeline-server-v1.batch-size". The default value is 1000. The interval of publishing events can be configured by "yarn.resourcemanager.system-metrics-publisher.timeline-server-v1.interval-seconds". By default, it is set to 60 seconds.


---

* [HDFS-16419](https://issues.apache.org/jira/browse/HDFS-16419) | *Major* | **Make HDFS data transfer tools cross platform**

**WARNING: No release note provided for this change.**


---

* [HADOOP-17526](https://issues.apache.org/jira/browse/HADOOP-17526) | *Major* | **Use Slf4jRequestLog for HttpRequestLog**

Use jetty's Slf4jRequestLog for http request log.

As a side effect, we remove the dummy HttpRequestLogAppender, just make use of DailyRollingFileAppender. But the DRFA in log4j1 lacks the ability of specifying the max retain files, so there is no retainDays config any more.

Will add the above ability back after we switch to log4j2's RollingFileAppender.


---

* [HDFS-15382](https://issues.apache.org/jira/browse/HDFS-15382) | *Major* | **Split one FsDatasetImpl lock to volume grain locks.**

Throughput is one of the core performance evaluation for DataNode instance. However it does not reach the best performance especially for Federation deploy all the time although there are different improvement, because of the global coarse-grain lock. These series issues (include HDFS-16534, HDFS-16511, HDFS-15382 and HDFS-16429.) try to split the global coarse-grain lock to fine-grain lock which is double level lock for blockpool and volume, to improve the throughput and avoid lock impacts between blockpools and volumes.


---

* [HADOOP-13386](https://issues.apache.org/jira/browse/HADOOP-13386) | *Major* | **Upgrade Avro to 1.9.2**

Java classes generated from previous versions of avro will need to be recompiled


---

* [HDFS-16511](https://issues.apache.org/jira/browse/HDFS-16511) | *Major* | **Improve lock type for ReplicaMap under fine-grain lock mode.**

**WARNING: No release note provided for this change.**


---

* [HADOOP-18188](https://issues.apache.org/jira/browse/HADOOP-18188) | *Major* | **Support touch command for directory**

Before this improvement, "hadoop fs -touch" command threw PathIsDirectoryException for directory. Now the command supports directory and will not throw that exception.


---

* [HDFS-16534](https://issues.apache.org/jira/browse/HDFS-16534) | *Major* | **Split datanode block pool locks to volume grain.**

**WARNING: No release note provided for this change.**


---

* [HADOOP-18088](https://issues.apache.org/jira/browse/HADOOP-18088) | *Major* | **Replace log4j 1.x with reload4j**

log4j 1 was replaced with reload4j which is fork of log4j 1.2.17 with the goal of fixing pressing security issues.

If you are depending on the hadoop artifacts in your build were explicitly excluding log4 artifacts, and now want to exclude the reload4j files, you will need to update your exclusion lists
\<exclusion\>
  \<groupId\>org.slf4j\</groupId\>
  \<artifactId\>slf4j-reload4j\</artifactId\>
\</exclusion\>
\<exclusion\>
  \<groupId\>ch.qos.reload4j\</groupId\>
  \<artifactId\>reload4j\</artifactId\>
\</exclusion\>


---

* [HADOOP-15983](https://issues.apache.org/jira/browse/HADOOP-15983) | *Major* | **Use jersey-json that is built to use jackson2**

Use modified jersey-json 1.20 in https://github.com/pjfanning/jersey-1.x/tree/v1.20 that uses Jackson 2.x. By this change, Jackson 1.x dependency has been removed from Hadoop.
downstream applications which explicitly exclude jersey from transitive dependencies must now exclude com.github.pjfanning:jersey-json


---

* [HADOOP-18219](https://issues.apache.org/jira/browse/HADOOP-18219) | *Blocker* | **Fix shadedclient test failure**

**WARNING: No release note provided for this change.**


---

* [HDFS-16453](https://issues.apache.org/jira/browse/HDFS-16453) | *Major* | **Upgrade okhttp from 2.7.5 to 4.9.3**

okhttp has been updated to address CVE-2021-0341


---

* [HDFS-16595](https://issues.apache.org/jira/browse/HDFS-16595) | *Major* | **Slow peer metrics - add median, mad and upper latency limits**

Namenode metrics that represent Slownode Json now include three important factors (median, median absolute deviation, upper latency limit) that can help user determine how urgently a given slownode requires manual intervention.


---

* [HADOOP-18237](https://issues.apache.org/jira/browse/HADOOP-18237) | *Major* | **Upgrade Apache Xerces Java to 2.12.2**

Apache Xerces has been updated  to 2.12.2 to fix CVE-2022-23437


---

* [HADOOP-18332](https://issues.apache.org/jira/browse/HADOOP-18332) | *Major* | **Remove rs-api dependency by downgrading jackson to 2.12.7**

Downgrades Jackson from 2.13.2 to 2.12.7 to fix class conflicts in downstream projects. This version of jackson does contain the fix for CVE-2020-36518.


---

* [HADOOP-18079](https://issues.apache.org/jira/browse/HADOOP-18079) | *Major* | **Upgrade Netty to 4.1.77.Final**

Netty has been updated to address CVE-2019-20444, CVE-2019-20445 and CVE-2022-24823


---

* [HADOOP-18344](https://issues.apache.org/jira/browse/HADOOP-18344) | *Major* | **AWS SDK update to 1.12.262 to address jackson  CVE-2018-7489 and AWS CVE-2022-31159**

The AWS SDK has been updated to 1.12.262 to address jackson CVE-2018-7489


---

* [HADOOP-18382](https://issues.apache.org/jira/browse/HADOOP-18382) | *Minor* | **Upgrade AWS SDK to V2 - Prerequisites**

In preparation for an (incompatible but necessary) move to the AWS SDK v2, some uses of internal/deprecated uses of AWS classes/interfaces are logged as warnings, though only once during the life of a JVM. Set the log "org.apache.hadoop.fs.s3a.SDKV2Upgrade" to only log at INFO to hide these.


---

* [HADOOP-18442](https://issues.apache.org/jira/browse/HADOOP-18442) | *Major* | **Remove the hadoop-openstack module**

The swift:// connector for openstack support has been removed. It had fundamental problems (swift's handling of files \> 4GB). A subset of the S3 protocol is now exported by almost all object store services -please use that through the s3a connector instead. The hadoop-openstack jar remains, only now it is empty of code. This is to ensure that projects which declare the JAR a dependency will still have successful builds.


---

* [HADOOP-17563](https://issues.apache.org/jira/browse/HADOOP-17563) | *Major* | **Update Bouncy Castle to 1.68 or later**

bouncy castle 1.68+ is a multirelease JAR containing java classes compiled for different target JREs. older versions of asm.jar and maven shade plugin may have problems with these. fix: upgrade the dependencies


---

* [HADOOP-18528](https://issues.apache.org/jira/browse/HADOOP-18528) | *Major* | **Disable abfs prefetching by default**

ABFS block prefetching has been disabled to avoid HADOOP-18521 and buffer sharing on multithreaded processes (Hive, Spark etc). This will have little/no performance impact on queries against Parquet or ORC data, but can slow down sequential stream processing, including CSV files -however, the read data will be correct.
It may slow down distcp downloads, where the race condition does not arise. For maximum distcp performance re-enable the readahead by setting fs.abfs.enable.readahead to true.


---

* [HADOOP-18621](https://issues.apache.org/jira/browse/HADOOP-18621) | *Critical* | **CryptoOutputStream::close leak when encrypted zones + quota exceptions**

**WARNING: No release note provided for this change.**


---

* [HADOOP-18215](https://issues.apache.org/jira/browse/HADOOP-18215) | *Minor* | **Enhance WritableName to be able to return aliases for classes that use serializers**

If you have a SequenceFile with an old key or value class which has been renamed, you can use WritableName.addName to add an alias class. This functionality previously existed, but only worked for classes which extend Writable. It now works for any class, notably key or value classes which use io.serializations.


---

* [HADOOP-18649](https://issues.apache.org/jira/browse/HADOOP-18649) | *Major* | **CLA and CRLA appenders to be replaced with RFA**

ContainerLogAppender and ContainerRollingLogAppender both have quite similar functionality as RollingFileAppender. Both are marked as IS.Unstable.

Before migrating to log4j2, replacing them with RollingFileAppender. Any downstreamers using it should do the same.


---

* [HADOOP-18654](https://issues.apache.org/jira/browse/HADOOP-18654) | *Major* | **Remove unused custom appender TaskLogAppender**

TaskLogAppender is IA.Private and IS.Unstable.
Removing it before migrating to log4j2 as it is no longer used within Hadoop. Any downstreamers using it should use RollingFileAppender instead.


---

* [HADOOP-18631](https://issues.apache.org/jira/browse/HADOOP-18631) | *Major* | **Migrate Async appenders to log4j properties**

1. Migrating Async appenders from code to log4j properties for namenode audit logger as well as datanode/namenode metric loggers
2. Provided sample log4j properties on how we can configure AsyncAppender to wrap RFA for the loggers
3. Incompatible change as three hdfs-site configs are no longer in use, they are to be replaced with log4j properties. Removed them and added a log to indicate that they are now replaced with log4j properties. The configs:
   - dfs.namenode.audit.log.async
   - dfs.namenode.audit.log.async.blocking
   - dfs.namenode.audit.log.async.buffer.size
4. Namenode audit logger as well as datanode/namenode metric loggers now use SLF4J logger rather than log4j logger directly


---

* [HADOOP-18329](https://issues.apache.org/jira/browse/HADOOP-18329) | *Major* | **Add support for IBM Semeru OE JRE 11.0.15.0 and greater**

Support has been added for IBM Semeru Runtimes, where due to vendor name based logic and changes in the java.vendor property, failures could occur on java 11 runtimes 11.0.15.0 and above.


---

* [HADOOP-18687](https://issues.apache.org/jira/browse/HADOOP-18687) | *Major* | **Remove unnecessary dependency on json-smart**

json-smart is no longer dependency of the hadoop-auth module (it not required) so is not exported transitively as a dependency or included in hadoop releases. If application code requires this on the classpath, a version must be added to the classpath explicitly -you get to choose which one


---

* [HADOOP-18752](https://issues.apache.org/jira/browse/HADOOP-18752) | *Major* | **Change fs.s3a.directory.marker.retention to "keep"**

The s3a connector no longer deletes directory markers by default, which speeds up write operations, reduces iO throttling and saves money. this can cause problems with older hadoop releases trying to write to the same bucket. (Hadoop 3.3.0; Hadoop 3.2.x before 3.2.2, and all previous releases). Set "fs.s3a.directory.marker.retention" to "delete" for backwards compatibility


---

* [MAPREDUCE-7432](https://issues.apache.org/jira/browse/MAPREDUCE-7432) | *Major* | **Make Manifest Committer the default for abfs and gcs**

By default, the mapreduce manifest committer is used for jobs working with abfs and gcs.. Hadoop mapreduce jobs will pick this up automatically; for Spark it is a bit complicated: read the docs  to see the steps required.


---

* [HADOOP-18820](https://issues.apache.org/jira/browse/HADOOP-18820) | *Major* | **AWS SDK v2: make the v1 bridging support optional**

The v1 aws-sdk-bundle JAR has been removed; it only required for third party applications or for use of v1 SDK AWSCredentialsProvider classes. There is automatic migration of the standard providers from the v1 to v2 classes, so this is only of issue for third-party providers or if very esoteric classes in the V1 SDK are used. Consult the aws\_sdk\_upgrade document for details


---

* [HADOOP-18073](https://issues.apache.org/jira/browse/HADOOP-18073) | *Major* | **S3A: Upgrade AWS SDK to V2**

The S3A connector now uses the V2 AWS SDK. 
This is a significant change at the source code level.
Any applications using the internal extension/override points in
the filesystem connector are likely to break.
Consult the document aws\_sdk\_upgrade for the full details.


---

* [HADOOP-18876](https://issues.apache.org/jira/browse/HADOOP-18876) | *Major* | **ABFS: Change default from disk to bytebuffer for fs.azure.data.blocks.buffer**

The default value for fs.azure.data.blocks.buffer is changed from "disk" to "bytebuffer"

This will speed up writing to azure storage, at the risk of running out of memory
-especially if there are many threads writing to abfs at the same time and the
upload bandwidth is limited.

If jobs do run out of memory writing to abfs, change the option back to "disk"


---

* [HADOOP-18948](https://issues.apache.org/jira/browse/HADOOP-18948) | *Minor* | **S3A. Add option fs.s3a.directory.operations.purge.uploads to purge on rename/delete**

S3A directory delete and rename will optionally abort all pending uploads
under the to-be-deleted paths when
fs.s3a.directory.operations.purge.upload is true
It is off by default.


---

* [HADOOP-18996](https://issues.apache.org/jira/browse/HADOOP-18996) | *Major* | **S3A to provide full support for S3 Express One Zone**

Hadoop S3A connector has explicit awareness of and support for S3Express storage.A filesystem can now be probed for inconsistent directoriy listings through fs.hasPathCapability(path, "fs.capability.directory.listing.inconsistent"). If true, then treewalking code SHOULD NOT report a failure if, when walking into a subdirectory, a list/getFileStatus on that directory raises a FileNotFoundException.


---

* [YARN-5597](https://issues.apache.org/jira/browse/YARN-5597) | *Major* | **YARN Federation improvements**

We have enhanced the YARN Federation functionality for improved usability. The enhanced features are as follows:
1. YARN Router now boasts a full implementation of all interfaces including the ApplicationClientProtocol, ResourceManagerAdministrationProtocol, and RMWebServiceProtocol.
2. YARN Router support for application cleanup and automatic offline mechanisms for subCluster.
3. Code improvements were undertaken for the Router and AMRMProxy, along with enhancements to previously pending functionalities.
4. Audit logs and Metrics for Router received upgrades.
5. A boost in cluster security features was achieved, with the inclusion of Kerberos support.
6. The page function of the router has been enhanced.
7. A set of commands has been added to the Router side for operating on SubClusters and Policies.


---

* [HADOOP-18830](https://issues.apache.org/jira/browse/HADOOP-18830) | *Major* | **S3A: Cut S3 Select**

S3 Select is no longer supported through the S3A connector


---

* [HADOOP-18993](https://issues.apache.org/jira/browse/HADOOP-18993) | *Minor* | **S3A: Add option fs.s3a.classloader.isolation (#6301)**

If the user wants to load custom implementations of AWS Credential Providers through user provided jars can set {{fs.s3a.extensions.isolated.classloader}} to {{false}}.


---

* [HADOOP-19084](https://issues.apache.org/jira/browse/HADOOP-19084) | *Blocker* | **prune dependency exports of hadoop-\* modules**

maven/ivy imports of hadoop-common are less likely to end up with log4j versions on their classpath.



