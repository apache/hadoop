
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
# Apache Hadoop 2.10.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [YARN-8200](https://issues.apache.org/jira/browse/YARN-8200) | *Major* | **Backport resource types/GPU features to branch-3.0/branch-2**

The generic resource types feature allows admins to configure custom resource types outside of memory and CPU. Users can request these resource types which YARN will take into account for resource scheduling.

This also adds GPU as a native resource type, built on top of the generic resource types feature. It adds support for GPU resource discovery, GPU scheduling and GPU isolation.


---

* [HDFS-12943](https://issues.apache.org/jira/browse/HDFS-12943) | *Major* | **Consistent Reads from Standby Node**

Observer is a new type of a NameNode in addition to Active and Standby Nodes in HA settings. An Observer Node maintains a replica of the namespace same as a Standby Node. It additionally allows execution of clients read requests.

To ensure read-after-write consistency within a single client, a state ID is introduced in RPC headers. The Observer responds to the client request only after its own state has caught up with the client’s state ID, which it previously received from the Active NameNode.

Clients can explicitly invoke a new client protocol call msync(), which ensures that subsequent reads by this client from an Observer are consistent.

A new client-side ObserverReadProxyProvider is introduced to provide automatic switching between Active and Observer NameNodes for submitting respectively write and read requests.


---

* [HDFS-13541](https://issues.apache.org/jira/browse/HDFS-13541) | *Major* | **NameNode Port based selective encryption**

This feature allows HDFS to selectively enforce encryption for both RPC (NameNode) and data transfer (DataNode). With this feature enabled, NameNode can listen on multiple ports, and different ports can have different security configurations. Depending on which NameNode port clients connect to, the RPC calls and the following data transfer will enforce security configuration corresponding to this NameNode port. This can help when there is requirement to enforce different security policies depending on the location where the clients are connecting from.

This can be enabled by setting `hadoop.security.saslproperties.resolver.class` configuration to `org.apache.hadoop.security.IngressPortBasedResolver`, and add the additional NameNode auxiliary ports by setting `dfs.namenode.rpc-address.auxiliary-ports`, and set the security individual ports by configuring `ingress.port.sasl.configured.ports`.


---

* [HDFS-14403](https://issues.apache.org/jira/browse/HDFS-14403) | *Major* | **Cost-Based RPC FairCallQueue**

This adds an extension to the IPC FairCallQueue which allows for the consideration of the *cost* of a user's operations when deciding how they should be prioritized, as opposed to the number of operations. This can be helpful for protecting the NameNode from clients which submit very expensive operations (e.g. large listStatus operations or recursive getContentSummary operations).

This can be enabled by setting the `ipc.<port>.costprovder.impl` configuration to `org.apache.hadoop.ipc.WeightedTimeCostProvider`.


---

* [HDFS-12883](https://issues.apache.org/jira/browse/HDFS-12883) | *Major* | **RBF: Document Router and State Store metrics**

This JIRA makes following change:
Change Router metrics context from 'router' to 'dfs'.


---

* [HDFS-12895](https://issues.apache.org/jira/browse/HDFS-12895) | *Major* | **RBF: Add ACL support for mount table**

Mount tables support ACL, The users won't be able to modify their own entries (we are assuming these old (no-permissions before) mount table with owner:superuser, group:supergroup, permission:755 as the default permissions).  The fix way is login as superuser to modify these mount table entries.


---

* [HADOOP-15027](https://issues.apache.org/jira/browse/HADOOP-15027) | *Major* | **AliyunOSS: Support multi-thread pre-read to improve sequential read from Hadoop to Aliyun OSS performance**

Support multi-thread pre-read in AliyunOSSInputStream to improve the sequential read performance from Hadoop to Aliyun OSS.


---

* [MAPREDUCE-7029](https://issues.apache.org/jira/browse/MAPREDUCE-7029) | *Minor* | **FileOutputCommitter is slow on filesystems lacking recursive delete**

MapReduce jobs that output to filesystems without direct support for recursive delete can set mapreduce.fileoutputcommitter.task.cleanup.enabled=true to have each task delete their intermediate work directory rather than waiting for the ApplicationMaster to clean up at the end of the job. This can significantly speed up the cleanup phase for large jobs on such filesystems.


---

* [HDFS-12528](https://issues.apache.org/jira/browse/HDFS-12528) | *Major* | **Add an option to not disable short-circuit reads on failures**

Added an option to not disables short-circuit reads on failures, by setting dfs.domain.socket.disable.interval.seconds to 0.


---

* [HDFS-13083](https://issues.apache.org/jira/browse/HDFS-13083) | *Major* | **RBF: Fix doc error setting up client**

Fix the document error of setting up HFDS Router Federation


---

* [HDFS-13099](https://issues.apache.org/jira/browse/HDFS-13099) | *Minor* | **RBF: Use the ZooKeeper as the default State Store**

Change default State Store from local file to ZooKeeper. This will require additional zk address to be configured.


---

* [YARN-7919](https://issues.apache.org/jira/browse/YARN-7919) | *Major* | **Refactor timelineservice-hbase module into submodules**

HBase integration module was mixed up with for hbase-server and hbase-client dependencies. This JIRA split into sub modules such that hbase-client dependent modules and hbase-server dependent modules are separated. This allows to make conditional compilation with different version of Hbase.


---

* [HDFS-13492](https://issues.apache.org/jira/browse/HDFS-13492) | *Major* | **Limit httpfs binds to certain IP addresses in branch-2**

Use environment variable HTTPFS\_HTTP\_HOSTNAME to limit the IP addresses httpfs server binds to. Default: httpfs server binds to all IP addresses on the host.


---

* [HADOOP-15446](https://issues.apache.org/jira/browse/HADOOP-15446) | *Major* | **WASB: PageBlobInputStream.skip breaks HBASE replication**

WASB: Bug fix to support non-sequential page blob reads.  Required for HBASE replication.


---

* [HADOOP-15478](https://issues.apache.org/jira/browse/HADOOP-15478) | *Major* | **WASB: hflush() and hsync() regression**

WASB: Bug fix for recent regression in hflush() and hsync().


---

* [HADOOP-15506](https://issues.apache.org/jira/browse/HADOOP-15506) | *Minor* | **Upgrade Azure Storage Sdk version to 7.0.0 and update corresponding code blocks**

WASB: Fix Spark process hang at shutdown due to use of non-daemon threads by updating Azure Storage Java SDK to 7.0


---

* [HDFS-13553](https://issues.apache.org/jira/browse/HDFS-13553) | *Major* | **RBF: Support global quota**

Federation supports and controls global quota at mount table level.

In a federated environment, a folder can be spread across multiple subclusters. Router aggregates quota that queried from these subclusters  and uses that for the quota-verification.


---

* [HADOOP-15547](https://issues.apache.org/jira/browse/HADOOP-15547) | *Major* | **WASB: improve listStatus performance**

WASB: listStatus 10x performance improvement for listing 700,000 files


---

* [HADOOP-16055](https://issues.apache.org/jira/browse/HADOOP-16055) | *Blocker* | **Upgrade AWS SDK to 1.11.271 in branch-2**

This change was required to address license compatibility issues with the JSON parser in the older AWS SDKs.

A consequence of this, where needed, the applied patch contains HADOOP-12705 Upgrade Jackson 2.2.3 to 2.7.8.


---

* [HADOOP-16053](https://issues.apache.org/jira/browse/HADOOP-16053) | *Major* | **Backport HADOOP-14816 to branch-2**

This patch changed the default build and test environment from Ubuntu "Trusty" 14.04 to Ubuntu "Xenial" 16.04.


---

* [HDFS-14617](https://issues.apache.org/jira/browse/HDFS-14617) | *Major* | **Improve fsimage load time by writing sub-sections to the fsimage index**

This change allows the inode and inode directory sections of the fsimage to be loaded in parallel. Tests on large images have shown this change to reduce the image load time to about 50% of the pre-change run time.

It works by writing sub-section entries to the image index, effectively splitting each image section into many sub-sections which can be processed in parallel. By default 12 sub-sections per image section are created when the image is saved, and 4 threads are used to load the image at startup.

This is disabled by default for any image with more than 1M inodes (dfs.image.parallel.inode.threshold) and can be enabled by setting dfs.image.parallel.load to true. When the feature is enabled, the next HDFS checkpoint will write the image sub-sections and subsequent namenode restarts can load the image in parallel.

A image with the parallel sections can be read even if the feature is disabled, but HDFS versions without this Jira cannot load an image with parallel sections. OIV can process a parallel enabled image without issues.

Key configuration parameters are:

dfs.image.parallel.load=false - enable or disable the feature

dfs.image.parallel.target.sections = 12 - The target number of subsections. Aim for 2 to 3 times the number of dfs.image.parallel.threads.

dfs.image.parallel.inode.threshold = 1000000 - Only save and load in parallel if the image has more than this number of inodes.

dfs.image.parallel.threads = 4 - The number of threads used to load the image. Testing has shown 4 to be optimal, but this may depends on the environment


---

* [HDFS-14771](https://issues.apache.org/jira/browse/HDFS-14771) | *Major* | **Backport HDFS-14617 to branch-2 (Improve fsimage load time by writing sub-sections to the fsimage index)**

This change allows the inode and inode directory sections of the fsimage to be loaded in parallel. Tests on large images have shown this change to reduce the image load time to about 50% of the pre-change run time.

It works by writing sub-section entries to the image index, effectively splitting each image section into many sub-sections which can be processed in parallel. By default 12 sub-sections per image section are created when the image is saved, and 4 threads are used to load the image at startup.

This is disabled by default for any image with more than 1M inodes (dfs.image.parallel.inode.threshold) and can be enabled by setting dfs.image.parallel.load to true. When the feature is enabled, the next HDFS checkpoint will write the image sub-sections and subsequent namenode restarts can load the image in parallel.

A image with the parallel sections can be read even if the feature is disabled, but HDFS versions without this Jira cannot load an image with parallel sections. OIV can process a parallel enabled image without issues.

Key configuration parameters are:

dfs.image.parallel.load=false - enable or disable the feature

dfs.image.parallel.target.sections = 12 - The target number of subsections. Aim for 2 to 3 times the number of dfs.image.parallel.threads.

dfs.image.parallel.inode.threshold = 1000000 - Only save and load in parallel if the image has more than this number of inodes.

dfs.image.parallel.threads = 4 - The number of threads used to load the image. Testing has shown 4 to be optimal, but this may depends on the environment.

UPGRADE WARN:
1. It can upgrade smoothly from 2.10 to 3.\* if not enable this feature ever.
2. Only path to do upgrade from 2.10 to 3.3 currently when enable fsimage parallel loading feature.
3. If someone want to upgrade 2.10 to 3.\*(3.1.\*/3.2.\*) prior release, please make sure that save at least one fsimage file after disable this feature. It relies on change configuration parameter(dfs.image.parallel.load=false) first and restart namenode before upgrade operation.
