
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
# Apache Hadoop  1.1.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-5464](https://issues.apache.org/jira/browse/HADOOP-5464) | *Major* | **DFSClient does not treat write timeout of 0 properly**

Zero values for dfs.socket.timeout and dfs.datanode.socket.write.timeout are now respected. Previously zero values for these parameters resulted in a 5 second timeout.


---

* [MAPREDUCE-1906](https://issues.apache.org/jira/browse/MAPREDUCE-1906) | *Major* | **Lower default minimum heartbeat interval for tasktracker \> Jobtracker**

The default minimum heartbeat interval has been dropped from 3 seconds to 300ms to increase scheduling throughput on small clusters. Users may tune mapreduce.jobtracker.heartbeats.in.second to adjust this value.


---

* [HADOOP-6995](https://issues.apache.org/jira/browse/HADOOP-6995) | *Minor* | **Allow wildcards to be used in ProxyUsers configurations**

When configuring proxy users and hosts, the special wildcard value "\*" may be specified to match any host or any user.


---

* [MAPREDUCE-2517](https://issues.apache.org/jira/browse/MAPREDUCE-2517) | *Major* | **Porting Gridmix v3 system tests into trunk branch.**

Adds system tests to Gridmix. These system tests cover various features like job types (load and sleep), user resolvers (round-robin, submitter-user, echo) and  submission modes (stress, replay and serial).


---

* [MAPREDUCE-3008](https://issues.apache.org/jira/browse/MAPREDUCE-3008) | *Major* | **[Gridmix] Improve cumulative CPU usage emulation for short running tasks**

Improves cumulative CPU emulation for short running tasks.


---

* [MAPREDUCE-3118](https://issues.apache.org/jira/browse/MAPREDUCE-3118) | *Major* | **Backport Gridmix and Rumen features from trunk to Hadoop 0.20 security branch**

Backports latest features from trunk to 0.20.206 branch.


---

* [HDFS-2465](https://issues.apache.org/jira/browse/HDFS-2465) | *Major* | **Add HDFS support for fadvise readahead and drop-behind**

HDFS now has the ability to use posix\_fadvise and sync\_data\_range syscalls to manage the OS buffer cache. This support is currently considered experimental, and may be enabled by configuring the following keys:
dfs.datanode.drop.cache.behind.writes - set to true to drop data out of the buffer cache after writing
dfs.datanode.drop.cache.behind.reads - set to true to drop data out of the buffer cache when performing sequential reads
dfs.datanode.sync.behind.writes - set to true to trigger dirty page writeback immediately after writing data
dfs.datanode.readahead.bytes - set to a non-zero value to trigger readahead for sequential reads


---

* [MAPREDUCE-3597](https://issues.apache.org/jira/browse/MAPREDUCE-3597) | *Major* | **Provide a way to access other info of history file from Rumentool**

Rumen now provides {{Parsed\*}} objects. These objects provide extra information that are not provided by {{Logged\*}} objects.


---

* [HDFS-2741](https://issues.apache.org/jira/browse/HDFS-2741) | *Minor* | **dfs.datanode.max.xcievers missing in 0.20.205.0**

Document and raise the maximum allowed transfer threads on a DataNode to 4096. This helps Apache HBase in particular.


---

* [HDFS-3044](https://issues.apache.org/jira/browse/HDFS-3044) | *Major* | **fsck move should be non-destructive by default**

The fsck "move" option is no longer destructive. It copies the accessible blocks of corrupt files to lost and found as before, but no longer deletes the corrupt files after copying the blocks. The original, destructive behavior can be enabled by specifying both the "move" and "delete" options.


---

* [HADOOP-8154](https://issues.apache.org/jira/browse/HADOOP-8154) | *Major* | **DNS#getIPs shouldn't silently return the local host IP for bogus interface names**

**WARNING: No release note provided for this change.**


---

* [MAPREDUCE-4087](https://issues.apache.org/jira/browse/MAPREDUCE-4087) | *Major* | **[Gridmix] GenerateDistCacheData job of Gridmix can become slow in some cases**

Fixes the issue of GenerateDistCacheData  job slowness.


---

* [HDFS-3055](https://issues.apache.org/jira/browse/HDFS-3055) | *Minor* | **Implement recovery mode for branch-1**

This is a new feature.  It is documented in hdfs\_user\_guide.xml.


---

* [HDFS-3094](https://issues.apache.org/jira/browse/HDFS-3094) | *Major* | **add -nonInteractive and -force option to namenode -format command**

The 'namenode -format' command now supports the flags '-nonInteractive' and '-force' to improve usefulness without user input.


---

* [HADOOP-8314](https://issues.apache.org/jira/browse/HADOOP-8314) | *Major* | **HttpServer#hasAdminAccess should return false if authorization is enabled but user is not authenticated**

**WARNING: No release note provided for this change.**


---

* [HADOOP-8230](https://issues.apache.org/jira/browse/HADOOP-8230) | *Major* | **Enable sync by default and disable append**

Append is not supported in Hadoop 1.x. Please upgrade to 2.x if you need append. If you enabled dfs.support.append for HBase, you're OK, as durable sync (why HBase required dfs.support.append) is now enabled by default. If you really need the previous functionality, to turn on the append functionality set the flag "dfs.support.broken.append" to true.


---

* [HDFS-3522](https://issues.apache.org/jira/browse/HDFS-3522) | *Major* | **If NN is in safemode, it should throw SafeModeException when getBlockLocations has zero locations**

getBlockLocations(), and hence open() for read, will now throw SafeModeException if the NameNode is still in safe mode and there are no replicas reported yet for one of the blocks in the file.


---

* [HDFS-3518](https://issues.apache.org/jira/browse/HDFS-3518) | *Major* | **Provide API to check HDFS operational state**

Add a utility method HdfsUtils.isHealthy(uri) for checking if the given HDFS is healthy.


---

* [HADOOP-8365](https://issues.apache.org/jira/browse/HADOOP-8365) | *Blocker* | **Add flag to disable durable sync**

This patch enables durable sync by default. Installation where HBase was not used, that used to run without setting "dfs.support.append" or setting it to false explicitly in the configuration, must add a new flag "dfs.durable.sync" and set it to false to preserve the previous semantics.


---

* [HADOOP-8552](https://issues.apache.org/jira/browse/HADOOP-8552) | *Major* | **Conflict: Same security.log.file for multiple users.**

**WARNING: No release note provided for this change.**


---

* [HDFS-2617](https://issues.apache.org/jira/browse/HDFS-2617) | *Major* | **Replaced Kerberized SSL for image transfer and fsck with SPNEGO-based solution**

Due to the requirement that KSSL use weak encryption types for Kerberos tickets, HTTP authentication to the NameNode will now use SPNEGO by default. This will require users of previous branch-1 releases with security enabled to modify their configurations and create new Kerberos principals in order to use SPNEGO. The old behavior of using KSSL can optionally be enabled by setting the configuration option "hadoop.security.use-weak-http-crypto" to "true".


---

* [HDFS-3814](https://issues.apache.org/jira/browse/HDFS-3814) | *Major* | **Make the replication monitor multipliers configurable in 1.x**

This change adds two new configuration parameters.
# {{dfs.namenode.invalidate.work.pct.per.iteration}} for controlling deletion rate of blocks.
# {{dfs.namenode.replication.work.multiplier.per.iteration}} for controlling replication rate. This in turn allows controlling the time it takes for decommissioning.

Please see hdfs-default.xml for detailed description.


---

* [HDFS-3703](https://issues.apache.org/jira/browse/HDFS-3703) | *Major* | **Decrease the datanode failure detection time**

This jira adds a new DataNode state called "stale" at the NameNode. DataNodes are marked as stale if it does not send heartbeat message to NameNode within the timeout configured using the configuration parameter "dfs.namenode.stale.datanode.interval" in seconds (default value is 30 seconds). NameNode picks a stale datanode as the last target to read from when returning block locations for reads.

This feature is by default turned \* off \*. To turn on the feature, set the HDFS configuration "dfs.namenode.check.stale.datanode" to true.


---

* [MAPREDUCE-4673](https://issues.apache.org/jira/browse/MAPREDUCE-4673) | *Major* | **make TestRawHistoryFile and TestJobHistoryServer more robust**

Fixed TestRawHistoryFile and TestJobHistoryServer to not write to /tmp.


---

* [MAPREDUCE-4675](https://issues.apache.org/jira/browse/MAPREDUCE-4675) | *Major* | **TestKillSubProcesses fails as the process is still alive after the job is done**

Fixed a race condition caused in TestKillSubProcesses caused due to a recent commit.


---

* [MAPREDUCE-4698](https://issues.apache.org/jira/browse/MAPREDUCE-4698) | *Minor* | **TestJobHistoryConfig throws Exception in testJobHistoryLogging**

Optionally call initialize/initializeFileSystem in JobTracker::startTracker() to allow for proper initialization when offerService is not being called.



