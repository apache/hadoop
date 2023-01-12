
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
# Apache Hadoop  3.2.2 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-16460](https://issues.apache.org/jira/browse/HADOOP-16460) | *Major* | **ABFS: fix for Sever Name Indication (SNI)**

ABFS: Bug fix to support Server Name Indication (SNI).


---

* [HDFS-14890](https://issues.apache.org/jira/browse/HDFS-14890) | *Blocker* | **Setting permissions on name directory fails on non posix compliant filesystems**

- Fixed namenode/journal startup on Windows.


---

* [HDFS-14905](https://issues.apache.org/jira/browse/HDFS-14905) | *Major* | **Backport HDFS persistent memory read cache support to branch-3.2**

Non-volatile storage class memory (SCM, also known as persistent memory) is supported in HDFS cache. To enable SCM cache, user just needs to configure SCM volume for property “dfs.datanode.cache.pmem.dirs” in hdfs-site.xml. And all HDFS cache directives keep unchanged. There are two implementations for HDFS SCM Cache, one is pure java code implementation and the other is native PMDK based implementation. The latter implementation can bring user better performance gain in cache write and cache read. If PMDK native libs could be loaded, it will use PMDK based implementation otherwise it will fallback to java code implementation. To enable PMDK based implementation, user should install PMDK library by referring to the official site http://pmem.io/. Then, build Hadoop with PMDK support by referring to "PMDK library build options" section in \`BUILDING.txt\` in the source code. If multiple SCM volumes are configured, a round-robin policy is used to select an available volume for caching a block. Consistent with DRAM cache, SCM cache also has no cache eviction mechanism. When DataNode receives a data read request from a client, if the corresponding block is cached into SCM, DataNode will instantiate an InputStream with the block location path on SCM (pure java implementation) or cache address on SCM (PMDK based implementation). Once the InputStream is created, DataNode will send the cached data to the client. Please refer "Centralized Cache Management" guide for more details.


---

* [HDFS-12943](https://issues.apache.org/jira/browse/HDFS-12943) | *Major* | **Consistent Reads from Standby Node**

Observer is a new type of a NameNode in addition to Active and Standby Nodes in HA settings. An Observer Node maintains a replica of the namespace same as a Standby Node. It additionally allows execution of clients read requests.

To ensure read-after-write consistency within a single client, a state ID is introduced in RPC headers. The Observer responds to the client request only after its own state has caught up with the client’s state ID, which it previously received from the Active NameNode.

Clients can explicitly invoke a new client protocol call msync(), which ensures that subsequent reads by this client from an Observer are consistent.

A new client-side ObserverReadProxyProvider is introduced to provide automatic switching between Active and Observer NameNodes for submitting respectively write and read requests.


---

* [HADOOP-16771](https://issues.apache.org/jira/browse/HADOOP-16771) | *Major* | **Update checkstyle to 8.26 and maven-checkstyle-plugin to 3.1.0**

Updated checkstyle to 8.26 and updated maven-checkstyle-plugin to 3.1.0.


---

* [HDFS-15281](https://issues.apache.org/jira/browse/HDFS-15281) | *Major* | **ZKFC ignores dfs.namenode.rpc-bind-host and uses dfs.namenode.rpc-address to bind to host address**

ZKFC binds host address to "dfs.namenode.servicerpc-bind-host", if configured. Otherwise, it binds to "dfs.namenode.rpc-bind-host". If neither of those is configured, ZKFC binds itself to NameNode RPC server address (effectively "dfs.namenode.rpc-address").


---

* [HADOOP-17024](https://issues.apache.org/jira/browse/HADOOP-17024) | *Major* | **ListStatus on ViewFS root (ls "/") should list the linkFallBack root (configured target root).**

ViewFS#listStatus on root("/") considers listing from fallbackLink if available. If the same directory name is present in configured mount path as well as in fallback link, then only the configured mount path will be listed in the returned result.


---

* [YARN-9809](https://issues.apache.org/jira/browse/YARN-9809) | *Major* | **NMs should supply a health status when registering with RM**

Improved node registration with node health status.



