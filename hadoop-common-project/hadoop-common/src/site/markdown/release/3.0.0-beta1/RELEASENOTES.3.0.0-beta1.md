
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
# Apache Hadoop  3.0.0-beta1 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-14535](https://issues.apache.org/jira/browse/HADOOP-14535) | *Major* | **wasb: implement high-performance random access and seek of block blobs**

Random access and seek improvements for the wasb:// (Azure) file system.


---

* [YARN-6798](https://issues.apache.org/jira/browse/YARN-6798) | *Major* | **Fix NM startup failure with old state store due to version mismatch**

<!-- markdown -->

This fixes the LevelDB state store for the NodeManager.  As of this patch, the state store versions now correspond to the following table.

* Previous Patch: YARN-5049
  * LevelDB Key: queued
  * Hadoop Versions: 2.9.0, 3.0.0-alpha1
  * Corresponding LevelDB Version: 1.2
* Previous Patch: YARN-6127
  * LevelDB Key: AMRMProxy/NextMasterKey
  * Hadoop Versions: 2.9.0, 3.0.0-alpha4
  * Corresponding LevelDB Version: 1.1


---

* [HADOOP-14539](https://issues.apache.org/jira/browse/HADOOP-14539) | *Major* | **Move commons logging APIs over to slf4j in hadoop-common**

In Hadoop common, fatal log level is changed to error because slf4j API does not support fatal log level.


---

* [HADOOP-14518](https://issues.apache.org/jira/browse/HADOOP-14518) | *Minor* | **Customize User-Agent header sent in HTTP/HTTPS requests by WASB.**

WASB now includes the current Apache Hadoop version in the User-Agent string passed to Azure Blob service. Users also may include optional additional information to identify their application. See the documentation of configuration property fs.wasb.user.agent.id for further details.


---

* [HADOOP-11875](https://issues.apache.org/jira/browse/HADOOP-11875) | *Major* | **[JDK9] Add a second copy of Hamlet without \_ as a one-character identifier**

Added org.apache.hadoop.yarn.webapp.hamlet2 package without \_ as a one-character identifier. Please use this package instead of org.apache.hadoop.yarn.webapp.hamlet.


---

* [HDFS-12206](https://issues.apache.org/jira/browse/HDFS-12206) | *Major* | **Rename the split EC / replicated block metrics**

The metrics and MBeans introduced in HDFS-10999 have been renamed for brevity and clarity.


---

* [HADOOP-13595](https://issues.apache.org/jira/browse/HADOOP-13595) | *Blocker* | **Rework hadoop\_usage to be broken up by clients/daemons/etc.**

This patch changes how usage output is generated to now require a sub-command type.  This allows users to see who the intended audience for  a command is or it is a daemon.


---

* [HDFS-6984](https://issues.apache.org/jira/browse/HDFS-6984) | *Major* | **Serialize FileStatus via protobuf**

FileStatus and FsPermission Writable serialization is deprecated and its implementation (incompatibly) replaced with protocol buffers. The FsPermissionProto record moved from hdfs.proto to acl.proto. HdfsFileStatus is now a subtype of FileStatus. FsPermissionExtension with its associated flags for ACLs, encryption, and erasure coding has been deprecated; users should query these attributes on the FileStatus object directly. The FsPermission instance in AclStatus no longer retains or reports these extended attributes (likely unused).


---

* [HADOOP-14722](https://issues.apache.org/jira/browse/HADOOP-14722) | *Major* | **Azure: BlockBlobInputStream position incorrect after seek**

Bug fix to Azure Filesystem related to HADOOP-14535.


---

* [YARN-6961](https://issues.apache.org/jira/browse/YARN-6961) | *Minor* | **Remove commons-logging dependency from hadoop-yarn-server-applicationhistoryservice module**

commons-logging dependency was removed from hadoop-yarn-server-applicationhistoryservice. If you rely on the transitive commons-logging dependency, please define the dependency explicitly.


---

* [HADOOP-14680](https://issues.apache.org/jira/browse/HADOOP-14680) | *Minor* | **Azure: IndexOutOfBoundsException in BlockBlobInputStream**

Bug fix to Azure Filesystem related to HADOOP-14535


---

* [HDFS-10326](https://issues.apache.org/jira/browse/HDFS-10326) | *Major* | **Disable setting tcp socket send/receive buffers for write pipelines**

The size of the TCP socket buffers are no longer hardcoded by default. Instead the OS now will automatically tune the size for the buffer.


---

* [HDFS-11957](https://issues.apache.org/jira/browse/HDFS-11957) | *Major* | **Enable POSIX ACL inheritance by default**

<!-- markdown -->
HDFS-6962 introduced POSIX ACL inheritance feature but it is disable by
default. Now enable the feature by default. Please be aware any code
expecting the old ACL inheritance behavior will have to be updated.
Please see the HDFS Permissions Guide for further details.


---

* [MAPREDUCE-6870](https://issues.apache.org/jira/browse/MAPREDUCE-6870) | *Major* | **Add configuration for MR job to finish when all reducers are complete (even with unfinished mappers)**

Enables mapreduce.job.finish-when-all-reducers-done by default. With this enabled, a MapReduce job will complete as soon as all of its reducers are complete, even if some mappers are still running. This can occur if a mapper was relaunched after node failure but the relaunched task's output is not actually needed. Previously the job would wait for all mappers to complete.


---

* [HADOOP-14260](https://issues.apache.org/jira/browse/HADOOP-14260) | *Major* | **Configuration.dumpConfiguration should redact sensitive information**

<!-- markdown -->
Configuration.dumpConfiguration no longer prints out the clear text values for the sensitive keys listed in `hadoop.security.sensitive-config-keys`. Callers can override the default list of sensitive keys either to redact more keys or print the clear text values for a few extra keys for debugging purpose.


---

* [HDFS-12221](https://issues.apache.org/jira/browse/HDFS-12221) | *Major* | **Replace xerces in XmlEditsVisitor**

New patch with changes in maven dependency. (removed apache xcerces as dependency)


---

* [HADOOP-14726](https://issues.apache.org/jira/browse/HADOOP-14726) | *Minor* | **Mark FileStatus::isDir as final**

The deprecated FileStatus::isDir method has been marked as final. FileSystems should override FileStatus::isDirectory


---

* [HADOOP-14660](https://issues.apache.org/jira/browse/HADOOP-14660) | *Major* | **wasb: improve throughput by 34% when account limit exceeded**

Up to 34% throughput improvement for the wasb:// (Azure) file system when fs.azure.selfthrottling.enable is false fs.azure.autothrottling.enable is true.


---

* [HADOOP-14769](https://issues.apache.org/jira/browse/HADOOP-14769) | *Major* | **WASB: delete recursive should not fail if a file is deleted**

Recursive directory delete improvement for the wasb filesystem.


---

* [YARN-6959](https://issues.apache.org/jira/browse/YARN-6959) | *Major* | **RM may allocate wrong AM Container for new attempt**

ResourceManager will now record ResourceRequests from different attempts into different objects.


---

* [HDFS-12303](https://issues.apache.org/jira/browse/HDFS-12303) | *Blocker* | **Change default EC cell size to 1MB for better performance**

The cell size of the provided HDFS erasure coding policies has been changed from 64k to 1024k for better performance. The policy names have all been changed accordingly, i.e. RS-6-3.1024k.


---

* [HDFS-12258](https://issues.apache.org/jira/browse/HDFS-12258) | *Major* | **ec -listPolicies should list all policies in system, no matter it's enabled or disabled**

<!-- markdown -->

`hdfs ec -listPolicies` now lists enabled, disabled, and removed policies, rather than just enabled policies.


---

* [MAPREDUCE-6892](https://issues.apache.org/jira/browse/MAPREDUCE-6892) | *Major* | **Issues with the count of failed/killed tasks in the jhist file**

This adds some new job counters, the number of failed MAP/REDUCE tasks and the number of killed MAP/REDUCE tasks.


---

* [YARN-5355](https://issues.apache.org/jira/browse/YARN-5355) | *Critical* | **YARN Timeline Service v.2: alpha 2**

We are releasing the alpha2 version of a major revision of YARN Timeline Service: v.2. YARN Timeline Service v.2 addresses two major challenges: improving scalability and reliability of Timeline Service, and enhancing usability by introducing flows and aggregation.

YARN Timeline Service v.2 alpha1 was introduced in 3.0.0-alpha1 via YARN-2928.

YARN Timeline Service v.2 alpha2 is now being provided so that users and developers can test it and provide feedback and suggestions for making it a ready replacement for Timeline Service v.1.x. Security is provided via Kerberos Authentication and delegation tokens. There is also a simple read level authorization provided via whitelists.

Some of the notable improvements since alpha-1 are:
- Security via Kerberos Authentication and delegation tokens
- Read side simple authorization via whitelist
- Client configurable entity sort ordering
- New REST APIs for apps, app attempts, containers, fetching metrics by timerange, pagination, sub-app entities
- Support for storing sub-application entities (entities that exist outside the scope of an application)
- Configurable TTLs (time-to-live) for tables, configurable table prefixes, configurable hbase cluster
- Flow level aggregations done as dynamic (table level) coprocessors
- Uses latest stable HBase release 1.2.6

More details are available in the [YARN Timeline Service v.2](./hadoop-yarn/hadoop-yarn-site/TimelineServiceV2.html) documentation.


---

* [HADOOP-14364](https://issues.apache.org/jira/browse/HADOOP-14364) | *Major* | **refresh changelog/release notes with newer Apache Yetus build**

Additionally, this patch updates maven-site-plugin to 3.6 and doxia-module-markdown to 1.8-SNAPSHOT to work around problems with unknown schemas in URLs in markdown formatted content.


---

* [HADOOP-13345](https://issues.apache.org/jira/browse/HADOOP-13345) | *Major* | **S3Guard: Improved Consistency for S3A**

S3Guard (pronounced see-guard) is a new feature for the S3A connector to Amazon S3, which uses DynamoDB for a high performance and consistent metadata repository. Essentially: S3Guard caches directory information, so your S3A clients get faster lookups and resilience to inconsistency between S3 list operations and the status of objects. When files are created, with S3Guard, they'll always be found. 

S3Guard does not address update consistency: if a file is updated, while the directory information will be updated, calling open() on the path may still return the old data. Similarly, deleted objects may also potentially be opened.

Please consult the S3Guard documentation in the Amazon S3 section of our documentation.

Note: part of this update includes moving to a new version of the AWS SDK 1.11, one which includes the Dynamo DB client and its a shaded version of Jackson 2. The large aws-sdk-bundle JAR is needed to use the S3A client with or without S3Guard enabled. The good news: because Jackson is shaded, there will be no conflict between any Jackson version used in your application and that which the AWS SDK needs.


---

* [HADOOP-14414](https://issues.apache.org/jira/browse/HADOOP-14414) | *Minor* | **Calling maven-site-plugin directly for docs profile is unnecessary**

maven-site-plugin is no longer called directly at package phase.


---

* [HDFS-12300](https://issues.apache.org/jira/browse/HDFS-12300) | *Major* | **Audit-log delegation token related operations**

<!-- markdown -->

NameNode now audit-logs `getDelegationToken`, `renewDelegationToken`, `cancelDelegationToken`.


---

* [HDFS-12218](https://issues.apache.org/jira/browse/HDFS-12218) | *Blocker* | **Rename split EC / replicated block metrics in BlockManager**

This renames ClientProtocol#getECBlockGroupsStats to ClientProtocol#getEcBlockGroupStats and ClientProtocol#getBlockStats to ClientProtocol#getReplicatedBlockStats. The return-type classes have also been similarly renamed. Their fields have also been renamed to drop the "stats" suffix.

Additionally, ECBlockGroupStats#pendingDeletionBlockGroups has been renamed to ECBlockGroupStats#pendingDeletionBlocks, to clarify that this is the number of blocks, not block groups, pending deletion. The corresponding NameNode metric has also been renamed to PendingDeletionECBlocks.


---

* [HADOOP-13421](https://issues.apache.org/jira/browse/HADOOP-13421) | *Minor* | **Switch to v2 of the S3 List Objects API in S3A**

S3A now defaults to using the "v2" S3 list API, which speeds up large-scale path listings. Non-AWS S3 implementations may not support this API: consult the S3A documentation on how to revert to the v1 API.


---

* [HADOOP-14847](https://issues.apache.org/jira/browse/HADOOP-14847) | *Blocker* | **Remove Guava Supplier and change to java Supplier in AMRMClient and AMRMClientAysnc**

AMRMClient#waitFor and AMRMClientAsync#waitFor now take java.util.function.Supplier as an argument, rather than com.google.common.base.Supplier.


---

* [HADOOP-14520](https://issues.apache.org/jira/browse/HADOOP-14520) | *Major* | **WASB: Block compaction for Azure Block Blobs**

Block Compaction for Azure Block Blobs. When the number of blocks in a block blob is above 32000, the process of compaction replaces a sequence of small blocks with with one big block.


---

* [HDFS-12412](https://issues.apache.org/jira/browse/HDFS-12412) | *Major* | **Change ErasureCodingWorker.stripedReadPool to cached thread pool**

Changed {{stripedReadPool}} to unbounded cachedThreadPool.  User should combine {{dfs.datanode.ec.reconstruction.stripedblock.threads}} and {{dfs.namenode.replication.max-streams}} to tune recovery performance.


---

* [HDFS-12414](https://issues.apache.org/jira/browse/HDFS-12414) | *Major* | **Ensure to use CLI command to enable/disable erasure coding policy**

dfs.namenode.ec.policies.enabled was removed in order to ensure there is only one approach to enable/disable erasure coding policies to avoid sync up.


---

* [HDFS-12438](https://issues.apache.org/jira/browse/HDFS-12438) | *Major* | **Rename dfs.datanode.ec.reconstruction.stripedblock.threads.size to dfs.datanode.ec.reconstruction.threads**

<!-- markdown -->

Config key `dfs.datanode.ec.reconstruction.stripedblock.threads.size` has been renamed to `dfs.datanode.ec.reconstruction.threads`.


---

* [HADOOP-14738](https://issues.apache.org/jira/browse/HADOOP-14738) | *Blocker* | **Remove S3N and obsolete bits of S3A; rework docs**

\* The s3n:// client has been removed. Please upgrade to the s3a:// client.
\* The s3a's original output stream has been removed, the "fast" output stream is the sole option available. There is no need to explicitly enable this, and trying to disable it (fs.s3a.fast.upload=false) will have no effect.


---

* [HDFS-7859](https://issues.apache.org/jira/browse/HDFS-7859) | *Major* | **Erasure Coding: Persist erasure coding policies in NameNode**

Persist all built-in erasure coding policies and user defined erasure coding policies into NameNode fsImage and editlog reliably, so that all erasure coding policies remain consistent after NameNode restart.


---

* [HDFS-12395](https://issues.apache.org/jira/browse/HDFS-12395) | *Major* | **Support erasure coding policy operations in namenode edit log**

See RN in HDFS-7859 as part of the work.


---

* [HADOOP-14670](https://issues.apache.org/jira/browse/HADOOP-14670) | *Major* | **Increase minimum cmake version for all platforms**

CMake v3.1.0 is now the minimum version required to build Apache Hadoop's native components.


---

* [HDFS-11799](https://issues.apache.org/jira/browse/HDFS-11799) | *Major* | **Introduce a config to allow setting up write pipeline with fewer nodes than replication factor**

Added new configuration "dfs.client.block.write.replace-datanode-on-failure.min-replication".
     
    The minimum number of replications that are needed to not to fail
      the write pipeline if new datanodes can not be found to replace
      failed datanodes (could be due to network failure) in the write pipeline.
      If the number of the remaining datanodes in the write pipeline is greater
      than or equal to this property value, continue writing to the remaining nodes.
      Otherwise throw exception.

      If this is set to 0, an exception will be thrown, when a replacement
      can not be found.


---

* [HDFS-12447](https://issues.apache.org/jira/browse/HDFS-12447) | *Major* | **Rename AddECPolicyResponse to AddErasureCodingPolicyResponse**

HdfsAdmin#addErasureCodingPolicies now returns an AddErasureCodingPolicyResponse[] rather than AddECPolicyResponse[]. The corresponding RPC definition and PB message have also been renamed.


---

* [HDFS-7337](https://issues.apache.org/jira/browse/HDFS-7337) | *Critical* | **Configurable and pluggable erasure codec and policy**

This allows users to:
\* develop and plugin their own erasure codec and coders. The plugin will be loaded automatically from hadoop jars, the corresponding codec and coder will be registered for runtime use.
\* define their own erasure coding policies thru an xml file and CLI command. The added policies will be persisted into fsimage.


---

* [YARN-2915](https://issues.apache.org/jira/browse/YARN-2915) | *Major* | **Enable YARN RM scale out via federation using multiple RM's**

A federation-based approach to transparently scale a single YARN cluster to tens of thousands of nodes, by federating multiple YARN standalone clusters (sub-clusters). The applications running in this federated environment will see a single massive YARN cluster and will be able to schedule tasks on any node of the federated cluster. Under the hood, the federation system will negotiate with sub-clusters ResourceManagers and provide resources to the application. The goal is to allow an individual job to “span” sub-clusters seamlessly.


---

* [YARN-5220](https://issues.apache.org/jira/browse/YARN-5220) | *Major* | **Scheduling of OPPORTUNISTIC containers through YARN RM**

This extends the centralized YARN RM in to enable the scheduling of OPPORTUNISTIC containers in a centralized fashion.
This way, users can use OPPORTUNISTIC containers to improve the cluster's utilization, without needing to enable distributed scheduling.

