
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
# Apache Hadoop  2.0.2-alpha Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-8794](https://issues.apache.org/jira/browse/HADOOP-8794) | *Major* | **Modifiy bin/hadoop to point to HADOOP\_YARN\_HOME**

**WARNING: No release note provided for this incompatible change.**


---

* [HADOOP-8710](https://issues.apache.org/jira/browse/HADOOP-8710) | *Major* | **Remove ability for users to easily run the trash emptier**

The trash emptier may no longer be run using "hadoop org.apache.hadoop.fs.Trash". The trash emptier runs on the NameNode (if configured). Old trash checkpoints may be deleted using "hadoop fs -expunge".


---

* [HADOOP-8703](https://issues.apache.org/jira/browse/HADOOP-8703) | *Major* | **distcpV2: turn CRC checking off for 0 byte size**

distcp skips CRC on 0 byte files.


---

* [HADOOP-8689](https://issues.apache.org/jira/browse/HADOOP-8689) | *Major* | **Make trash a server side configuration option**

If fs.trash.interval is configured on the server then the client's value for this configuration is ignored.


---

* [HADOOP-8551](https://issues.apache.org/jira/browse/HADOOP-8551) | *Major* | **fs -mkdir creates parent directories without the -p option**

FsShell's "mkdir" no longer implicitly creates all non-existent parent directories.  The command adopts the posix compliant behavior of requiring the "-p" flag to auto-create parent directories.


---

* [HADOOP-8533](https://issues.apache.org/jira/browse/HADOOP-8533) | *Major* | **Remove Parallel Call in IPC**

Merged the change to branch-2


---

* [HADOOP-8458](https://issues.apache.org/jira/browse/HADOOP-8458) | *Major* | **Add management hook to AuthenticationHandler to enable delegation token operations support**

**WARNING: No release note provided for this incompatible change.**


---

* [HADOOP-8388](https://issues.apache.org/jira/browse/HADOOP-8388) | *Minor* | **Remove unused BlockLocation serialization**

**WARNING: No release note provided for this incompatible change.**


---

* [HADOOP-8368](https://issues.apache.org/jira/browse/HADOOP-8368) | *Minor* | **Use CMake rather than autotools to build native code**

**WARNING: No release note provided for this incompatible change.**


---

* [HADOOP-8327](https://issues.apache.org/jira/browse/HADOOP-8327) | *Major* | **distcpv2 and distcpv1 jars should not coexist**

Resolve sporadic distcp issue due to having two DistCp classes (v1 & v2) in the classpath.


---

* [HADOOP-7703](https://issues.apache.org/jira/browse/HADOOP-7703) | *Major* | **WebAppContext should also be stopped and cleared**

Improved excpetion handling of shutting down web server. (Devaraj K via Eric Yang)


---

* [HDFS-3755](https://issues.apache.org/jira/browse/HDFS-3755) | *Major* | **Creating an already-open-for-write file with overwrite=true fails**

This is an incompatible change: Before this change, if a file is already open for write by one client, and another client calls fs.create() with overwrite=true, an AlreadyBeingCreatedException is thrown.  After this change, the file will be deleted and the new file will be created successfully.


---

* [HDFS-3697](https://issues.apache.org/jira/browse/HDFS-3697) | *Minor* | **Enable fadvise readahead by default**

The datanode now performs 4MB readahead by default when reading data from its disks, if the native libraries are present. This has been shown to improve performance in many workloads. The feature may be disabled by setting dfs.datanode.readahead.bytes to "0".


---

* [HDFS-3675](https://issues.apache.org/jira/browse/HDFS-3675) | *Minor* | **libhdfs: follow documented return codes**

**WARNING: No release note provided for this incompatible change.**


---

* [HDFS-3522](https://issues.apache.org/jira/browse/HDFS-3522) | *Major* | **If NN is in safemode, it should throw SafeModeException when getBlockLocations has zero locations**

getBlockLocations(), and hence open() for read, will now throw SafeModeException if the NameNode is still in safe mode and there are no replicas reported yet for one of the blocks in the file.


---

* [HDFS-3518](https://issues.apache.org/jira/browse/HDFS-3518) | *Major* | **Provide API to check HDFS operational state**

Add a utility method HdfsUtils.isHealthy(uri) for checking if the given HDFS is healthy.


---

* [HDFS-3475](https://issues.apache.org/jira/browse/HDFS-3475) | *Trivial* | **Make the replication and invalidation rates configurable**

This change adds two new configuration parameters. 
# {{dfs.namenode.invalidate.work.pct.per.iteration}} for controlling deletion rate of blocks. 
# {{dfs.namenode.replication.work.multiplier.per.iteration}} for controlling replication rate. This in turn allows controlling the time it takes for decommissioning. 

Please see hdfs-default.xml for detailed description.


---

* [HDFS-3446](https://issues.apache.org/jira/browse/HDFS-3446) | *Major* | **HostsFileReader silently ignores bad includes/excludes**

HDFS no longer silently ignores missing or unreadable host files specified by dfs.hosts or dfs.hosts.exclude. In order to specify that no hosts should be included or excluded, administrators should either refrain from setting the relevant config properties, or create an empty file in order to represent an empty list.


---

* [HDFS-3318](https://issues.apache.org/jira/browse/HDFS-3318) | *Blocker* | **Hftp hangs on transfers \>2GB**

**WARNING: No release note provided for this incompatible change.**


---

* [HDFS-3110](https://issues.apache.org/jira/browse/HDFS-3110) | *Major* | **libhdfs implementation of direct read API**

libhdfs is enhanced to read directly into user-supplied buffers when possible, reducing the number of memory copies.


---

* [HDFS-2793](https://issues.apache.org/jira/browse/HDFS-2793) | *Major* | **Add an admin command to trigger an edit log roll**

Introduced a new command, "hdfs dfsadmin -rollEdits" which requests that the active NameNode roll its edit log. This can be useful for administrators manually backing up log segments.


---

* [HDFS-2727](https://issues.apache.org/jira/browse/HDFS-2727) | *Minor* | **libhdfs should get the default block size from the server**

libhdfs now uses the server block size configuration rather than the deprecated dfs.block.size client configuration.


---

* [HDFS-2686](https://issues.apache.org/jira/browse/HDFS-2686) | *Major* | **Remove DistributedUpgrade related code**

This jira removes functionality that has not been used/applicable since release 0.17. The incompatibility introduced by this change will not affect any HDFS users.


---

* [HDFS-2617](https://issues.apache.org/jira/browse/HDFS-2617) | *Major* | **Replaced Kerberized SSL for image transfer and fsck with SPNEGO-based solution**

Due to the requirement that KSSL use weak encryption types for Kerberos tickets, HTTP authentication to the NameNode will now use SPNEGO by default. This will require users of previous branch-1 releases with security enabled to modify their configurations and create new Kerberos principals in order to use SPNEGO. The old behavior of using KSSL can optionally be enabled by setting the configuration option "hadoop.security.use-weak-http-crypto" to "true".


---

* [MAPREDUCE-4629](https://issues.apache.org/jira/browse/MAPREDUCE-4629) | *Major* | **Remove JobHistory.DEBUG\_MODE**

**WARNING: No release note provided for this incompatible change.**


---

* [MAPREDUCE-4311](https://issues.apache.org/jira/browse/MAPREDUCE-4311) | *Major* | **Capacity scheduler.xml does not accept decimal values for capacity and maximum-capacity settings**

**WARNING: No release note provided for this incompatible change.**


---

* [MAPREDUCE-4072](https://issues.apache.org/jira/browse/MAPREDUCE-4072) | *Major* | **User set java.library.path seems to overwrite default creating problems native lib loading**

-Djava.library.path in mapred.child.java.opts can cause issues with native libraries.  LD\_LIBRARY\_PATH through mapred.child.env should be used instead.


---

* [MAPREDUCE-4017](https://issues.apache.org/jira/browse/MAPREDUCE-4017) | *Trivial* | **Add jobname to jobsummary log**

The Job Summary log may contain commas in values that are escaped by a '\' character.  This was true before, but is more likely to be exposed now.


---

* [MAPREDUCE-3940](https://issues.apache.org/jira/browse/MAPREDUCE-3940) | *Major* | **ContainerTokens should have an expiry interval**

ContainerTokens now have an expiry interval so that stale tokens cannot be used for launching containers.


---

* [MAPREDUCE-3873](https://issues.apache.org/jira/browse/MAPREDUCE-3873) | *Minor* | **Nodemanager is not getting decommisioned if the absolute ip is given in exclude file.**

Fixed NodeManagers' decommissioning at RM to accept IP addresses also.


---

* [MAPREDUCE-3812](https://issues.apache.org/jira/browse/MAPREDUCE-3812) | *Major* | **Lower default allocation sizes, fix allocation configurations and document them**

Removes two sets of previously available config properties:

1. ( yarn.scheduler.fifo.minimum-allocation-mb and yarn.scheduler.fifo.maximum-allocation-mb ) and,
2. ( yarn.scheduler.capacity.minimum-allocation-mb and yarn.scheduler.capacity.maximum-allocation-mb )

In favor of two new, generically named properties:

1. yarn.scheduler.minimum-allocation-mb - This acts as the floor value of memory resource requests for containers.
2. yarn.scheduler.maximum-allocation-mb - This acts as the ceiling value of memory resource requests for containers.

Both these properties need to be set at the ResourceManager (RM) to take effect, as the RM is where the scheduler resides.

Also changes the default minimum and maximums to 128 MB and 10 GB respectively.


---

* [MAPREDUCE-3543](https://issues.apache.org/jira/browse/MAPREDUCE-3543) | *Critical* | **Mavenize Gridmix.**

Note that to apply this you should first run the script - ./MAPREDUCE-3543v3.sh svn, then apply the patch.

If this is merged to more then trunk, the version inside of hadoop-tools/hadoop-gridmix/pom.xml will need to be udpated accordingly.


---

* [MAPREDUCE-3348](https://issues.apache.org/jira/browse/MAPREDUCE-3348) | *Major* | **mapred job -status fails to give info even if the job is present in History**

Fixed a bug in MR client to redirect to JobHistoryServer correctly when RM forgets the app.



