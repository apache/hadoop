
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
# Apache Hadoop  3.2.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-14667](https://issues.apache.org/jira/browse/HADOOP-14667) | *Major* | **Flexible Visual Studio support**

<!-- markdown -->

This change updates the Microsoft Windows build directions to be more flexible with regards to Visual Studio compiler versions:

* Any version of Visual Studio 2010 Pro or higher may be used.
* MSBuild Solution files are converted to the version of VS at build time
* Example command file to set command paths prior to using maven so that conversion works

Additionally, Snappy and ISA-L that use bin as the location of the DLL will now be recognized without having to set their respective lib paths if the prefix is set.

Note to contributors:

It is very important that solutions for any patches remain at the VS 2010-level.


---

* [YARN-6257](https://issues.apache.org/jira/browse/YARN-6257) | *Minor* | **CapacityScheduler REST API produces incorrect JSON - JSON object operationsInfo contains duplicate keys**

The response format of resource manager's REST API endpoint "ws/v1/cluster/scheduler" is optimized, where the "operationInfos" field is converted from a map to list. This change makes the content to be more JSON parser friendly.


---

* [HADOOP-15146](https://issues.apache.org/jira/browse/HADOOP-15146) | *Minor* | **Remove DataOutputByteBuffer**

This jira helped to remove the private and unused class DataOutputByteBuffer.


---

* [MAPREDUCE-7069](https://issues.apache.org/jira/browse/MAPREDUCE-7069) | *Major* | **Add ability to specify user environment variables individually**

Environment variables for MapReduce tasks can now be specified as separate properties, e.g.:
mapreduce.map.env.VARNAME=value
mapreduce.reduce.env.VARNAME=value
yarn.app.mapreduce.am.env.VARNAME=value
yarn.app.mapreduce.am.admin.user.env.VARNAME=value
This form of specifying environment variables is useful when the value of an environment variable contains commas.


---

* [HADOOP-15446](https://issues.apache.org/jira/browse/HADOOP-15446) | *Major* | **WASB: PageBlobInputStream.skip breaks HBASE replication**

WASB: Bug fix to support non-sequential page blob reads.  Required for HBASE replication.


---

* [HDFS-13589](https://issues.apache.org/jira/browse/HDFS-13589) | *Major* | **Add dfsAdmin command to query if "upgrade" is finalized**

New command is added to dfsadmin.
hdfs dfsadmin [-upgrade [query \| finalize]
1. -upgrade query gives the upgradeStatus 
2. -upgrade finalize is equivalent to -finalizeUpgrade.


---

* [YARN-8191](https://issues.apache.org/jira/browse/YARN-8191) | *Major* | **Fair scheduler: queue deletion without RM restart**

To support Queue deletion with RM restart feature, AllocationFileLoaderService constructor signature was changed. YARN-8390 corrected this and made as a compatible change.


---

* [HADOOP-15477](https://issues.apache.org/jira/browse/HADOOP-15477) | *Trivial* | **Make unjar in RunJar overrideable**

<!-- markdown -->
If `HADOOP_CLIENT_SKIP_UNJAR` environment variable is set to true, Apache Hadoop RunJar skips unjar the provided jar.


---

* [HADOOP-15506](https://issues.apache.org/jira/browse/HADOOP-15506) | *Minor* | **Upgrade Azure Storage Sdk version to 7.0.0 and update corresponding code blocks**

WASB: Fix Spark process hang at shutdown due to use of non-daemon threads by updating Azure Storage Java SDK to 7.0


---

* [HDFS-13174](https://issues.apache.org/jira/browse/HDFS-13174) | *Major* | **hdfs mover -p /path times out after 20 min**

Mover could have fail after 20+ minutes if a block move was enqueued for this long, between two DataNodes due to an internal constant that was introduced for Balancer, but affected Mover as well.
The internal constant can be configured with the dfs.balancer.max-iteration-time parameter after the patch, and affects only the Balancer. Default is 20 minutes.


---

* [HADOOP-15495](https://issues.apache.org/jira/browse/HADOOP-15495) | *Major* | **Upgrade common-lang version to 3.7 in hadoop-common-project and hadoop-tools**

commons-lang version 2.6 was removed from Apache Hadoop. If you are using commons-lang 2.6 as transitive dependency of Hadoop, you need to add the dependency directly. Note: this also means it is absent from share/hadoop/common/lib/


---

* [HDFS-13322](https://issues.apache.org/jira/browse/HDFS-13322) | *Minor* | **fuse dfs - uid persists when switching between ticket caches**

FUSE lib now recognize the change of the Kerberos ticket cache path if it was changed between two file system access in the same local user session via the KRB5CCNAME environment variable.


---

* [HADOOP-15638](https://issues.apache.org/jira/browse/HADOOP-15638) | *Major* | **KMS Accept Queue Size default changed from 500 to 128 in Hadoop 3.x**

Restore the KMS accept queue size to 500 in Hadoop 3.x, making it the same as in Hadoop 2.x.


---

* [HADOOP-15669](https://issues.apache.org/jira/browse/HADOOP-15669) | *Major* | **ABFS: Improve HTTPS Performance**

ABFS: Improved HTTPS performance


---

* [HADOOP-15660](https://issues.apache.org/jira/browse/HADOOP-15660) | *Major* | **ABFS: Add support for OAuth**

ABFS: Support for OAuth


---

* [HDFS-13806](https://issues.apache.org/jira/browse/HDFS-13806) | *Minor* | **EC: No error message for unsetting EC policy of the directory inherits the erasure coding policy from an ancestor directory**

After this change, attempt to unsetErasureCodingPolicy() on a directory without EC policy explicitly set on it, will get NoECPolicySetException.


---

* [HADOOP-14833](https://issues.apache.org/jira/browse/HADOOP-14833) | *Major* | **Remove s3a user:secret authentication**

The S3A connector no longer supports username and secrets in URLs of the form \`s3a://key:secret@bucket/\`. It is near-impossible to stop those secrets being logged —which is why a warning has been printed since Hadoop 2.8 whenever such a URL was used.

Fix: use a more secure mechanism to pass down the secrets.


---

* [YARN-3409](https://issues.apache.org/jira/browse/YARN-3409) | *Major* | **Support Node Attribute functionality**

With this feature task, Node Attributes is supported in YARN which will help user's to effectively use resources and assign to applications based on characteristics of each node's in the cluster.


---

* [HADOOP-15684](https://issues.apache.org/jira/browse/HADOOP-15684) | *Critical* | **triggerActiveLogRoll stuck on dead name node, when ConnectTimeoutException happens.**

When a namenode A sends request RollEditLog to a remote NN, either the remote NN is standby or IO Exception happens, A should continue to try next NN, instead of getting stuck on the problematic one.  This Patch is based on trunk.


---

* [HDFS-10285](https://issues.apache.org/jira/browse/HDFS-10285) | *Major* | **Storage Policy Satisfier in HDFS**

StoragePolicySatisfier(SPS) allows users to track and satisfy the storage policy requirement of a given file/directory in HDFS. User can specify a file/directory path by invoking “hdfs storagepolicies -satisfyStoragePolicy -path \<path\>” command or via HdfsAdmin#satisfyStoragePolicy(path) API. For the blocks which has storage policy mismatches, it moves the replicas to a different storage type in order to fulfill the storage policy requirement. Since API calls goes to NN for tracking the invoked satisfier path(iNodes), administrator need to enable dfs.storage.policy.satisfier.mode’ config at NN to allow these operations. It can be enabled by setting ‘dfs.storage.policy.satisfier.mode’ to ‘external’ in hdfs-site.xml. The configs can be disabled dynamically without restarting Namenode. SPS should be started outside Namenode using "hdfs --daemon start sps". If administrator is looking to run Mover tool explicitly, then he/she should make sure to disable SPS first and then run Mover. See the "Storage Policy Satisfier (SPS)" section in the Archival Storage guide for detailed usage.


---

* [HADOOP-15407](https://issues.apache.org/jira/browse/HADOOP-15407) | *Blocker* | **Support Windows Azure Storage - Blob file system in Hadoop**

The abfs connector in the hadoop-azure module supports Microsoft Azure Datalake (Gen 2), which at the time of writing (September 2018) was in preview, soon to go GA. As with all cloud connectors, corner-cases will inevitably surface. If you encounter one, please file a bug report.


---

* [HADOOP-14445](https://issues.apache.org/jira/browse/HADOOP-14445) | *Major* | **Use DelegationTokenIssuer to create KMS delegation tokens that can authenticate to all KMS instances**

<!-- markdown -->

This patch improves the KMS delegation token issuing and authentication logic, to enable tokens to authenticate with a set of KMS servers. The change is backport compatible, in that it keeps the existing authentication logic as a fall back.

Historically, KMS delegation tokens have ip:port as service, making KMS clients only able to use the token to authenticate with the KMS server specified as ip:port, even though the token is shared among all KMS servers at server-side. After this patch, newly created tokens will have the KMS URL as service.

A `DelegationTokenIssuer` interface is introduced for token creation.


---

* [HDFS-14053](https://issues.apache.org/jira/browse/HDFS-14053) | *Major* | **Provide ability for NN to re-replicate based on topology changes.**

A new option (-replicate) is added to fsck command to re-trigger the replication for mis-replicated blocks. This option should be used instead of previous workaround of increasing and then decreasing replication factor (using hadoop fs -setrep command).


---

* [HADOOP-15996](https://issues.apache.org/jira/browse/HADOOP-15996) | *Major* | **Plugin interface to support more complex usernames in Hadoop**

This patch enables "Hadoop" and "MIT" as options for "hadoop.security.auth\_to\_local.mechanism" and defaults to 'hadoop'. This should be backward compatible with pre-HADOOP-12751.

This is basically HADOOP-12751 plus configurable + extended tests.



