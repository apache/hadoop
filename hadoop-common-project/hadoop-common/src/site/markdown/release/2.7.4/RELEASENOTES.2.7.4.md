
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
# Apache Hadoop  2.7.4 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-7851](https://issues.apache.org/jira/browse/HADOOP-7851) | *Major* | **Configuration.getClasses() never returns the default value.**

Fixed Configuration.getClasses() API to return the default value if the key is not set.


---

* [HDFS-9503](https://issues.apache.org/jira/browse/HDFS-9503) | *Major* | **Replace -namenode option with -fs for NNThroughputBenchmark**

The patch replaces -namenode option with -fs for specifying the remote name node against which the benchmark is running. Before this patch, if '-namenode' was not given, the benchmark would run in standalone mode, ignoring the 'fs.defaultFS' in config file even if it's remote. With this patch, the benchmark, as other tools, will rely on the 'fs.defaultFS' config, which is overridable by -fs command option, to run standalone mode or remote mode.


---

* [HADOOP-12668](https://issues.apache.org/jira/browse/HADOOP-12668) | *Critical* | **Support excluding weak Ciphers in HttpServer2 through ssl-server.xml**

The Code Changes include following:
- Modified DFSUtil.java in Apache HDFS project for supplying new parameter ssl.server.exclude.cipher.list
- Modified HttpServer2.java in Apache Hadoop-common project to work with new parameter and exclude ciphers using jetty setExcludeCihers method.
- Modfied associated test classes to owrk with existing code and also cover the newfunctionality in junit


---

* [HDFS-9412](https://issues.apache.org/jira/browse/HDFS-9412) | *Major* | **getBlocks occupies FSLock and takes too long to complete**

Skip blocks with size below dfs.balancer.getBlocks.min-block-size (default 10MB) when a balancer asks for a list of blocks.


---

* [HDFS-9902](https://issues.apache.org/jira/browse/HDFS-9902) | *Major* | **Support different values of dfs.datanode.du.reserved per storage type**

Reserved space can be configured independently for different storage types for clusters with heterogeneous storage. The 'dfs.datanode.du.reserved' property name can be suffixed with a storage types (i.e. one of ssd, disk, archival or ram\_disk). e.g. reserved space for RAM\_DISK storage can be configured using the property 'dfs.datanode.du.reserved.ram\_disk'. If specific storage type reservation is not configured then the value specified by 'dfs.datanode.du.reserved' will be used for all volumes.


---

* [HDFS-7933](https://issues.apache.org/jira/browse/HDFS-7933) | *Major* | **fsck should also report decommissioning replicas.**

The output of hdfs fsck now also contains information about decommissioning replicas.


---

* [HDFS-8312](https://issues.apache.org/jira/browse/HDFS-8312) | *Critical* | **Trash does not descent into child directories to check for permissions**

Permissions are now checked when moving a file to Trash.


---

* [HDFS-10609](https://issues.apache.org/jira/browse/HDFS-10609) | *Major* | **Uncaught InvalidEncryptionKeyException during pipeline recovery may abort downstream applications**

If pipeline recovery fails due to expired encryption key, attempt to refresh the key and retry.


---

* [HADOOP-10597](https://issues.apache.org/jira/browse/HADOOP-10597) | *Major* | **RPC Server signals backoff to clients when all request queues are full**

This change introduces a new configuration key used by RPC server to decide whether to send backoff signal to RPC Client when RPC call queue is full. When the feature is enabled, RPC server will no longer block on the processing of RPC requests when RPC call queue is full. It helps to improve quality of service when the service is under heavy load. The configuration key is in the format of "ipc.#port#.backoff.enable" where #port# is the port number that RPC server listens on. For example, if you want to enable the feature for the RPC server that listens on 8020, set ipc.8020.backoff.enable to true.


---

* [HDFS-11056](https://issues.apache.org/jira/browse/HDFS-11056) | *Major* | **Concurrent append and read operations lead to checksum error**

Load last partial chunk checksum properly into memory when converting a finalized/temporary replica to rbw replica. This ensures concurrent reader reads the correct checksum that matches the data before the update.


---

* [HADOOP-13812](https://issues.apache.org/jira/browse/HADOOP-13812) | *Blocker* | **Upgrade Tomcat to 6.0.48**

Tomcat 6.0.46 starts to filter weak ciphers. Some old SSL clients may be affected. It is recommended to upgrade the SSL client. Run the SSL client against https://www.howsmyssl.com/a/check to find out its TLS version and cipher suites.


---

* [HDFS-11229](https://issues.apache.org/jira/browse/HDFS-11229) | *Blocker* | **HDFS-11056 failed to close meta file**

The fix for HDFS-11056 reads meta file to load last partial chunk checksum when a block is converted from finalized/temporary to rbw. However, it did not close the file explicitly, which may cause number of open files reaching system limit. This jira fixes it by closing the file explicitly after the meta file is read.


---

* [HDFS-11160](https://issues.apache.org/jira/browse/HDFS-11160) | *Major* | **VolumeScanner reports write-in-progress replicas as corrupt incorrectly**

Fixed a race condition that caused VolumeScanner to recognize a good replica as a bad one if the replica is also being written concurrently.


---

* [HADOOP-13119](https://issues.apache.org/jira/browse/HADOOP-13119) | *Major* | **Add ability to secure log servlet using proxy users**

**WARNING: No release note provided for this change.**


---

* [HADOOP-14138](https://issues.apache.org/jira/browse/HADOOP-14138) | *Critical* | **Remove S3A ref from META-INF service discovery, rely on existing core-default entry**

The classpath implementing the s3a filesystem is now defined in core-default.xml. Attempting to instantiate an S3A filesystem instance using a Configuration instance which has not included the default resorts will fail. Applications should not be doing this anyway, as it will lose other critical  configuration options needed by the filesystem.


---

* [HDFS-11499](https://issues.apache.org/jira/browse/HDFS-11499) | *Major* | **Decommissioning stuck because of failing recovery**

Allow a block to complete if the number of replicas on live nodes, decommissioning nodes and nodes in maintenance mode satisfies minimum replication factor.
The fix prevents block recovery failure if replica of last block is being decommissioned. Vice versa, the decommissioning will be stuck, waiting for the last block to be completed. In addition, file close() operation will not fail due to last block being decommissioned.


---

* [HDFS-8818](https://issues.apache.org/jira/browse/HDFS-8818) | *Major* | **Allow Balancer to run faster**

Add a new conf "dfs.balancer.max-size-to-move" so that Balancer.MAX\_SIZE\_TO\_MOVE becomes configurable.
