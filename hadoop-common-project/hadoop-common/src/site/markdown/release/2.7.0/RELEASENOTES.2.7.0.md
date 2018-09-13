
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
# Apache Hadoop  2.7.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HDFS-6252](https://issues.apache.org/jira/browse/HDFS-6252) | *Minor* | **Phase out the old web UI in HDFS**

**WARNING: No release note provided for this change.**


---

* [HDFS-1362](https://issues.apache.org/jira/browse/HDFS-1362) | *Major* | **Provide volume management functionality for DataNode**

Based on the reconfiguration framework provided by HADOOP-7001, enable reconfigure the dfs.datanode.data.dir and add new volumes into service.


---

* [HADOOP-8989](https://issues.apache.org/jira/browse/HADOOP-8989) | *Major* | **hadoop fs -find feature**

New fs -find command


---

* [HADOOP-11311](https://issues.apache.org/jira/browse/HADOOP-11311) | *Major* | **Restrict uppercase key names from being created with JCEKS**

Keys with uppercase names can no longer be created when using the JavaKeyStoreProvider to resolve ambiguity about case-sensitivity in the KeyStore spec.


---

* [HDFS-7210](https://issues.apache.org/jira/browse/HDFS-7210) | *Major* | **Avoid two separate RPC's namenode.append() and namenode.getFileInfo() for an append call from DFSClient**

**WARNING: No release note provided for this change.**


---

* [HADOOP-10530](https://issues.apache.org/jira/browse/HADOOP-10530) | *Blocker* | **Make hadoop trunk build on Java7+ only**

**WARNING: No release note provided for this change.**


---

* [MAPREDUCE-5420](https://issues.apache.org/jira/browse/MAPREDUCE-5420) | *Major* | **Remove mapreduce.task.tmp.dir from mapred-default.xml**

**WARNING: No release note provided for this change.**


---

* [HADOOP-9629](https://issues.apache.org/jira/browse/HADOOP-9629) | *Major* | **Support Windows Azure Storage - Blob as a file system in Hadoop**

Hadoop now supports integration with Azure Storage as an alternative Hadoop Compatible File System.


---

* [HADOOP-11385](https://issues.apache.org/jira/browse/HADOOP-11385) | *Critical* | **Prevent cross site scripting attack on JMXJSONServlet**

**WARNING: No release note provided for this change.**


---

* [HADOOP-11446](https://issues.apache.org/jira/browse/HADOOP-11446) | *Major* | **S3AOutputStream should use shared thread pool to avoid OutOfMemoryError**

The following parameters are introduced in this JIRA:
fs.s3a.threads.max:    the maximum number of threads to allow in the pool used by TransferManager
fs.s3a.threads.core:    the number of threads to keep in the pool used by TransferManager
fs.s3a.threads.keepalivetime:  when the number of threads is greater than the core, this is the maximum time that excess idle threads will wait for new tasks before terminating
fs.s3a.max.total.tasks:    the maximum number of tasks that the LinkedBlockingQueue can hold


---

* [HADOOP-11464](https://issues.apache.org/jira/browse/HADOOP-11464) | *Major* | **Reinstate support for launching Hadoop processes on Windows using Cygwin.**

We have reinstated support for launching Hadoop processes on Windows by using Cygwin to run the shell scripts.  All processes still must have access to the native components: hadoop.dll and winutils.exe.


---

* [HDFS-3689](https://issues.apache.org/jira/browse/HDFS-3689) | *Major* | **Add support for variable length block**

1. HDFS now can choose to append data to a new block instead of end of the last partial block. Users can pass {{CreateFlag.APPEND}} and  {{CreateFlag.NEW\_BLOCK}} to the {{append}} API to indicate this requirement.
2. HDFS now allows users to pass {{SyncFlag.END\_BLOCK}} to the {{hsync}} API to finish the current block and write remaining data to a new block.


---

* [HADOOP-11498](https://issues.apache.org/jira/browse/HADOOP-11498) | *Major* | **Bump the version of HTrace to 3.1.0-incubating**

**WARNING: No release note provided for this change.**


---

* [HADOOP-10181](https://issues.apache.org/jira/browse/HADOOP-10181) | *Minor* | **GangliaContext does not work with multicast ganglia setup**

Hadoop metrics sent to Ganglia over multicast now support optional configuration of socket TTL.  The default TTL is 1, which preserves the behavior of prior Hadoop versions.  Clusters that span multiple subnets/VLANs will likely want to increase this.


---

* [HDFS-6651](https://issues.apache.org/jira/browse/HDFS-6651) | *Critical* | **Deletion failure can leak inodes permanently**

**WARNING: No release note provided for this change.**


---

* [HADOOP-11492](https://issues.apache.org/jira/browse/HADOOP-11492) | *Major* | **Bump up curator version to 2.7.1**

<!-- markdown -->
Apache Curator version change: Apache Hadoop has updated the version of Apache Curator used from 2.6.0 to 2.7.1. This change should be binary and source compatible for the majority of downstream users. Notable exceptions:

* Binary incompatible change: org.apache.curator.utils.PathUtils.validatePath(String) changed return types. Downstream users of this method will need to recompile.
* Source incompatible change: org.apache.curator.framework.recipes.shared.SharedCountReader added a method to its interface definition. Downstream users with custom implementations of this interface can continue without binary compatibility problems but will need to modify their source code to recompile.
* Source incompatible change: org.apache.curator.framework.recipes.shared.SharedValueReader added a method to its interface definition. Downstream users with custom implementations of this interface can continue without binary compatibility problems but will need to modify their source code to recompile.

Downstream users are reminded that while the Hadoop community will attempt to avoid egregious incompatible dependency changes, there is currently no policy around when Hadoop's exposed dependencies will change across versions (ref http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/Compatibility.html#Java_Classpath ).


---

* [HDFS-7270](https://issues.apache.org/jira/browse/HDFS-7270) | *Major* | **Add congestion signaling capability to DataNode write protocol**

Introduced a new configuration dfs.pipeline.ecn. When the configuration is turned on, DataNodes will signal in the writing pipelines when they are overloaded. The client can back off based on this congestion signal to avoid overloading the system.


---

* [HDFS-6133](https://issues.apache.org/jira/browse/HDFS-6133) | *Major* | **Add a feature for replica pinning so that a pinned replica will not be moved by Balancer/Mover.**

Add a feature for replica pinning so that when a replica is pinned in a datanode, it will not be moved by Balancer/Mover. The replica pinning feature can be enabled/disabled by "dfs.datanode.block-pinning.enabled", where the default is false.


---

* [HDFS-7584](https://issues.apache.org/jira/browse/HDFS-7584) | *Major* | **Enable Quota Support for Storage Types**

1. Introduced quota by storage type as a hard limit on the amount of space usage allowed for different storage types (SSD, DISK, ARCHIVE) under the target directory.
2. Added {{SetQuotaByStorageType}} API and {{-storagetype}} option for  {{hdfs dfsadmin -setSpaceQuota/-clrSpaceQuota}} commands to allow set/clear quota by storage type under the target directory.


---

* [HDFS-7806](https://issues.apache.org/jira/browse/HDFS-7806) | *Minor* | **Refactor: move StorageType from hadoop-hdfs to hadoop-common**

This fix moves the public class StorageType from the package org.apache.hadoop.hdfs to org.apache.hadoop.fs.


---

* [YARN-3217](https://issues.apache.org/jira/browse/YARN-3217) | *Major* | **Remove httpclient dependency from hadoop-yarn-server-web-proxy**

Removed commons-httpclient dependency from hadoop-yarn-server-web-proxy module.


---

* [HADOOP-9922](https://issues.apache.org/jira/browse/HADOOP-9922) | *Major* | **hadoop windows native build will fail in 32 bit machine**

The Hadoop Common native components now support 32-bit build targets on Windows.


---

* [HDFS-7774](https://issues.apache.org/jira/browse/HDFS-7774) | *Critical* | **Unresolved symbols error while compiling HDFS on Windows 7/32 bit**

LibHDFS now supports 32-bit build targets on Windows.


---

* [MAPREDUCE-5583](https://issues.apache.org/jira/browse/MAPREDUCE-5583) | *Major* | **Ability to limit running map and reduce tasks**

<!-- markdown -->
This introduces two new MR2 job configs, mentioned below, which allow users to control the maximum simultaneously-running tasks of the submitted job, across the cluster:

* mapreduce.job.running.map.limit (default: 0, for no limit)
* mapreduce.job.running.reduce.limit (default: 0, for no limit)

This is controllable at a per-job level.


---

* [HDFS-1522](https://issues.apache.org/jira/browse/HDFS-1522) | *Major* | **Merge Block.BLOCK\_FILE\_PREFIX and DataStorage.BLOCK\_FILE\_PREFIX into one constant**

This merges Block.BLOCK\_FILE\_PREFIX and DataStorage.BLOCK\_FILE\_PREFIX into one constant. Hard-coded
literals of "blk\_" in various files are also updated to use the same constant.


---

* [HDFS-7411](https://issues.apache.org/jira/browse/HDFS-7411) | *Major* | **Refactor and improve decommissioning logic into DecommissionManager**

This change introduces a new configuration key used to throttle decommissioning work, "dfs.namenode.decommission.blocks.per.interval". This new key overrides and deprecates the previous related configuration key "dfs.namenode.decommission.nodes.per.interval". The new key is intended to result in more predictable pause times while scanning decommissioning nodes.


---

* [YARN-3154](https://issues.apache.org/jira/browse/YARN-3154) | *Blocker* | **Should not upload partial logs for MR jobs or other "short-running' applications**

Applications which made use of the LogAggregationContext in their application will need to revisit this code in order to make sure that their logs continue to get rolled out.


---

* [HADOOP-9329](https://issues.apache.org/jira/browse/HADOOP-9329) | *Trivial* | **document native build dependencies in BUILDING.txt**

Added a section to BUILDING.txt on how to install required / optional packages on a clean install of Ubuntu 14.04 LTS Desktop.

Went through the CMakeLists.txt files in the repo and added the following optional library dependencies - Snappy, Bzip2, Linux FUSE and Jansson.

Updated the required packages / version numbers from the trunk branch version of BUILDING.txt.


---

* [HADOOP-11801](https://issues.apache.org/jira/browse/HADOOP-11801) | *Minor* | **Update BUILDING.txt for Ubuntu**

ProtocolBuffer is packaged in Ubuntu



