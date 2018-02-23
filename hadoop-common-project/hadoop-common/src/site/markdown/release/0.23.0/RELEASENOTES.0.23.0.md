
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
# Apache Hadoop  0.23.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-6683](https://issues.apache.org/jira/browse/HADOOP-6683) | *Minor* | **the first optimization: ZlibCompressor does not fully utilize the buffer**

Improve the buffer utilization of ZlibCompressor to avoid invoking a JNI per write request.


---

* [HADOOP-7023](https://issues.apache.org/jira/browse/HADOOP-7023) | *Major* | **Add listCorruptFileBlocks to FileSystem**

Add a new API listCorruptFileBlocks to FIleContext that returns a list of files that have corrupt blocks.


---

* [HADOOP-7059](https://issues.apache.org/jira/browse/HADOOP-7059) | *Major* | **Remove "unused" warning in native code**

Adds \_\_attribute\_\_ ((unused))


---

* [HDFS-1526](https://issues.apache.org/jira/browse/HDFS-1526) | *Major* | **Dfs client name for a map/reduce task should have some randomness**

Make a client name has this format: DFSClient\_applicationid\_randomint\_threadid, where applicationid = mapred.task.id or else = "NONMAPREDUCE".


---

* [HDFS-1560](https://issues.apache.org/jira/browse/HDFS-1560) | *Minor* | **dfs.data.dir permissions should default to 700**

The permissions on datanode data directories (configured by dfs.datanode.data.dir.perm) now default to 0700. Upon startup, the datanode will automatically change the permissions to match the configured value.


---

* [MAPREDUCE-1906](https://issues.apache.org/jira/browse/MAPREDUCE-1906) | *Major* | **Lower default minimum heartbeat interval for tasktracker \> Jobtracker**

The default minimum heartbeat interval has been dropped from 3 seconds to 300ms to increase scheduling throughput on small clusters. Users may tune mapreduce.jobtracker.heartbeats.in.second to adjust this value.


---

* [MAPREDUCE-2207](https://issues.apache.org/jira/browse/MAPREDUCE-2207) | *Major* | **Task-cleanup task should not be scheduled on the node that the task just failed**

Task-cleanup task should not be scheduled on the node that the task just failed


---

* [HDFS-1536](https://issues.apache.org/jira/browse/HDFS-1536) | *Major* | **Improve HDFS WebUI**

On web UI, missing block number now becomes accurate and under-replicated blocks do not include missing blocks.


---

* [HADOOP-7089](https://issues.apache.org/jira/browse/HADOOP-7089) | *Minor* | **Fix link resolution logic in hadoop-config.sh**

Updates hadoop-config.sh to always resolve symlinks when determining HADOOP\_HOME. Bash built-ins or POSIX:2001 compliant cmds are now required.


---

* [HDFS-1547](https://issues.apache.org/jira/browse/HDFS-1547) | *Major* | **Improve decommission mechanism**

Summary of changes to the decommissioning process:
# After nodes are decommissioned, they are not shutdown. The decommissioned nodes are not used for writes. For reads, the decommissioned nodes are given as the last location to read from.
# Number of live and dead decommissioned nodes are displayed in the namenode webUI.
# Decommissioned nodes free capacity is not count towards the the cluster free capacity.


---

* [HDFS-1448](https://issues.apache.org/jira/browse/HDFS-1448) | *Major* | **Create multi-format parser for edits logs file, support binary and XML formats initially**

Offline edits viewer feature adds oev tool to hdfs script. Oev makes it possible to convert edits logs to/from native binary and XML formats. It uses the same framework as Offline image viewer.

Example usage:

$HADOOP\_HOME/bin/hdfs oev -i edits -o output.xml


---

* [HADOOP-6864](https://issues.apache.org/jira/browse/HADOOP-6864) | *Major* | **Provide a JNI-based implementation of ShellBasedUnixGroupsNetgroupMapping (implementation of GroupMappingServiceProvider)**

**WARNING: No release note provided for this change.**


---

* [HADOOP-6904](https://issues.apache.org/jira/browse/HADOOP-6904) | *Major* | **A baby step towards inter-version RPC communications**

**WARNING: No release note provided for this change.**


---

* [HADOOP-6436](https://issues.apache.org/jira/browse/HADOOP-6436) | *Major* | **Remove auto-generated native build files**

The native build run when from trunk now requires autotools, libtool and openssl dev libraries.


---

* [MAPREDUCE-2260](https://issues.apache.org/jira/browse/MAPREDUCE-2260) | *Major* | **Remove auto-generated native build files**

The native build run when from trunk now requires autotools, libtool and openssl dev libraries.


---

* [HDFS-1582](https://issues.apache.org/jira/browse/HDFS-1582) | *Major* | **Remove auto-generated native build files**

The native build run when from trunk now requires autotools, libtool and openssl dev libraries.


---

* [HADOOP-6432](https://issues.apache.org/jira/browse/HADOOP-6432) | *Major* | **Statistics support in FileContext**

**WARNING: No release note provided for this change.**


---

* [HADOOP-7136](https://issues.apache.org/jira/browse/HADOOP-7136) | *Major* | **Remove failmon contrib**

Failmon removed from contrib codebase.


---

* [MAPREDUCE-2254](https://issues.apache.org/jira/browse/MAPREDUCE-2254) | *Major* | **Allow setting of end-of-record delimiter for TextInputFormat**

TextInputFormat may now split lines with delimiters other than newline, by specifying a configuration parameter "textinputformat.record.delimiter"


---

* [HADOOP-7153](https://issues.apache.org/jira/browse/HADOOP-7153) | *Minor* | **MapWritable violates contract of Map interface for equals() and hashCode()**

MapWritable now implements equals() and hashCode() based on the map contents rather than object identity in order to correctly implement the Map interface.


---

* [MAPREDUCE-1996](https://issues.apache.org/jira/browse/MAPREDUCE-1996) | *Trivial* | **API: Reducer.reduce() method detail misstatement**

Fix a misleading documentation note about the usage of Reporter objects in Reducers.


---

* [MAPREDUCE-1159](https://issues.apache.org/jira/browse/MAPREDUCE-1159) | *Trivial* | **Limit Job name on jobtracker.jsp to be 80 char long**

Job names on jobtracker.jsp should be 80 characters long at most.


---

* [HDFS-1626](https://issues.apache.org/jira/browse/HDFS-1626) | *Minor* | **Make BLOCK\_INVALIDATE\_LIMIT configurable**

Added a new configuration property dfs.block.invalidate.limit for FSNamesystem.blockInvalidateLimit.


---

* [MAPREDUCE-2225](https://issues.apache.org/jira/browse/MAPREDUCE-2225) | *Blocker* | **MultipleOutputs should not require the use of 'Writable'**

MultipleOutputs should not require the use/check of 'Writable' interfaces in key and value classes.


---

* [MAPREDUCE-1811](https://issues.apache.org/jira/browse/MAPREDUCE-1811) | *Minor* | **Job.monitorAndPrintJob() should print status of the job at completion**

Print the resultant status of a Job on completion instead of simply saying 'Complete'.


---

* [MAPREDUCE-993](https://issues.apache.org/jira/browse/MAPREDUCE-993) | *Minor* | **bin/hadoop job -events \<jobid\> \<from-event-#\> \<#-of-events\> help message is confusing**

Added a helpful description message to the \`mapred job -events\` command.


---

* [MAPREDUCE-1242](https://issues.apache.org/jira/browse/MAPREDUCE-1242) | *Trivial* | **Chain APIs error misleading**

Fix a misleading exception message in case the Chained Mappers have mismatch in input/output Key/Value pairs between them.


---

* [HADOOP-7133](https://issues.apache.org/jira/browse/HADOOP-7133) | *Major* | **CLONE to COMMON - HDFS-1445 Batch the calls in DataStorage to FileUtil.createHardLink(), so we call it once per directory instead of once per file**

This is the COMMON portion of a fix requiring coordinated change of COMMON and HDFS.  Please see HDFS-1445 for HDFS portion and release note.


---

* [HDFS-1703](https://issues.apache.org/jira/browse/HDFS-1703) | *Minor* | **HDFS federation: Improve start/stop scripts and add script to decommission datanodes**

The masters file is no longer used to indicate which hosts to start the 2NN on. The 2NN is now started on hosts when dfs.namenode.secondary.http-address is configured with a non-wildcard IP.


---

* [HDFS-1675](https://issues.apache.org/jira/browse/HDFS-1675) | *Major* | **Transfer RBW between datanodes**

Added a new stage TRANSFER\_RBW to DataTransferProtocol


---

* [HDFS-1596](https://issues.apache.org/jira/browse/HDFS-1596) | *Major* | **Move secondary namenode checkpoint configs from core-default.xml to hdfs-default.xml**

Removed references to the older fs.checkpoint.\* properties that resided in core-site.xml


---

* [HADOOP-7117](https://issues.apache.org/jira/browse/HADOOP-7117) | *Major* | **Move secondary namenode checkpoint configs from core-default.xml to hdfs-default.xml**

Removed references to the older fs.checkpoint.\* properties that resided in core-site.xml


---

* [HDFS-1445](https://issues.apache.org/jira/browse/HDFS-1445) | *Major* | **Batch the calls in DataStorage to FileUtil.createHardLink(), so we call it once per directory instead of once per file**

Batch hardlinking during "upgrade" snapshots, cutting time from aprx 8 minutes per volume to aprx 8 seconds.  Validated in both Linux and Windows.  Depends on prior integration with patch for HADOOP-7133.


---

* [HADOOP-6949](https://issues.apache.org/jira/browse/HADOOP-6949) | *Major* | **Reduces RPC packet size for primitive arrays, especially long[], which is used at block reporting**

Increments the RPC protocol version in org.apache.hadoop.ipc.Server from 4 to 5.
Introduces ArrayPrimitiveWritable for a much more efficient wire format to transmit arrays of primitives over RPC. ObjectWritable uses the new writable for array of primitives for RPC and continues to use existing format for on-disk data.


---

* [HADOOP-7193](https://issues.apache.org/jira/browse/HADOOP-7193) | *Minor* | **Help message is wrong for touchz command.**

Updated the help for the touchz command.


---

* [HDFS-1120](https://issues.apache.org/jira/browse/HDFS-1120) | *Major* | **Make DataNode's block-to-device placement policy pluggable**

Make the DataNode's block-volume choosing policy pluggable.


---

* [HDFS-1761](https://issues.apache.org/jira/browse/HDFS-1761) | *Major* | **Add a new DataTransferProtocol operation, Op.TRANSFER\_BLOCK, instead of using RPC**

Add a new DataTransferProtocol operation, Op.TRANSFER\_BLOCK, for transferring RBW/Finalized with acknowledgement and without using RPC.


---

* [MAPREDUCE-2405](https://issues.apache.org/jira/browse/MAPREDUCE-2405) | *Major* | **MR-279: Implement uber-AppMaster (in-cluster LocalJobRunner for MRv2)**

An efficient implementation of small jobs by running all tasks in the MR ApplicationMaster JVM, there-by affecting lower latency.


---

* [HDFS-1606](https://issues.apache.org/jira/browse/HDFS-1606) | *Major* | **Provide a stronger data guarantee in the write pipeline**

Added two configuration properties, dfs.client.block.write.replace-datanode-on-failure.enable and dfs.client.block.write.replace-datanode-on-failure.policy.  Added a new feature to replace datanode on failure in DataTransferProtocol.  Added getAdditionalDatanode(..) in ClientProtocol.


---

* [HDFS-1070](https://issues.apache.org/jira/browse/HDFS-1070) | *Major* | **Speedup NameNode image loading and saving by storing local file names**

This changes the fsimage format to be
root directory-1 directory-2 ... directoy-n.
Each directory stores all its children in the following format:
Directory\_full\_path\_name num\_of\_children child-1 ... child-n.
Each inode stores only the last component of its path name into fsimage.
This change requires an upgrade at deployment.


---

* [HDFS-1594](https://issues.apache.org/jira/browse/HDFS-1594) | *Major* | **When the disk becomes full Namenode is getting shutdown and not able to recover**

Implemented a daemon thread to monitor the disk usage for periodically and if the disk usage reaches the threshold value, put the name node into Safe mode so that no modification to file system will occur. Once the disk usage reaches below the threshold, name node will be put out of the safe mode. Here threshold value and interval to check the disk usage are configurable.


---

* [HDFS-1843](https://issues.apache.org/jira/browse/HDFS-1843) | *Minor* | **Discover file not found early for file append**

I have committed this. Thanks to Bharath!


---

* [MAPREDUCE-1461](https://issues.apache.org/jira/browse/MAPREDUCE-1461) | *Major* | **Feature to instruct rumen-folder utility to skip jobs worth of specific duration**

Added a ''-starts-after' option to Rumen's Folder utility. The time duration specified after the '-starts-after' option is an offset with respect to the submit time of the first job in the input trace. Jobs in the input trace having a submit time (relative to the first job's submit time) lesser than the specified offset will be ignored.


---

* [MAPREDUCE-2153](https://issues.apache.org/jira/browse/MAPREDUCE-2153) | *Major* | **Bring in more job configuration properties in to the trace file**

Adds job configuration parameters to the job trace. The configuration parameters are stored under the 'jobProperties' field as key-value pairs.


---

* [MAPREDUCE-2417](https://issues.apache.org/jira/browse/MAPREDUCE-2417) | *Major* | **In Gridmix, in RoundRobinUserResolver mode, the testing/proxy users are not associated with unique users in a trace**

Fixes Gridmix in RoundRobinUserResolver mode to map testing/proxy users to unique users in a trace.


---

* [MAPREDUCE-2416](https://issues.apache.org/jira/browse/MAPREDUCE-2416) | *Major* | **In Gridmix, in RoundRobinUserResolver, the list of groups for a user obtained from users-list-file is incorrect**

Removes the restriction of specifying group names in users-list file for Gridmix in RoundRobinUserResolver mode.


---

* [MAPREDUCE-2434](https://issues.apache.org/jira/browse/MAPREDUCE-2434) | *Major* | **MR-279: ResourceManager metrics**

I just committed this. Thanks Luke!


---

* [MAPREDUCE-1978](https://issues.apache.org/jira/browse/MAPREDUCE-1978) | *Major* | **[Rumen] TraceBuilder should provide recursive input folder scanning**

Adds -recursive option to TraceBuilder for scanning the input directories recursively.


---

* [HADOOP-7227](https://issues.apache.org/jira/browse/HADOOP-7227) | *Major* | **Remove protocol version check at proxy creation in Hadoop RPC.**

1. Protocol version check is removed from proxy creation, instead version check is performed at server in every rpc call.
2. This change is backward incompatible because format of the rpc messages is changed to include client version, client method hash and rpc version.
3. rpc version is introduced which should change when the format of rpc messages is changed.


---

* [MAPREDUCE-2474](https://issues.apache.org/jira/browse/MAPREDUCE-2474) | *Minor* | **Add docs to the new API Partitioner on how to access Job Configuration data**

Improve the Partitioner interface's docs to help fetch Job Configuration objects.


---

* [HADOOP-6919](https://issues.apache.org/jira/browse/HADOOP-6919) | *Major* | **Metrics2: metrics framework**

New metrics2 framework for Hadoop.


---

* [HDFS-1826](https://issues.apache.org/jira/browse/HDFS-1826) | *Major* | **NameNode should save image to name directories in parallel during upgrade**

I've committed this. Thanks, Matt!


---

* [MAPREDUCE-2478](https://issues.apache.org/jira/browse/MAPREDUCE-2478) | *Major* | **MR 279: Improve history server**

Looks great. I just committed this. Thanks Siddharth!


---

* [HADOOP-7257](https://issues.apache.org/jira/browse/HADOOP-7257) | *Major* | **A client side mount table to give per-application/per-job file system view**

viewfs - client-side mount table.


---

* [HADOOP-6920](https://issues.apache.org/jira/browse/HADOOP-6920) | *Major* | **Metrics2: metrics instrumentation**

Metrics names are standardized to use CapitalizedCamelCase. Some examples of this is:
# Metrics names using "\_" is changed to new naming scheme. Eg: bytes\_written changes to BytesWritten.
# All metrics names start with capitals. Example: threadsBlocked changes to ThreadsBlocked.


---

* [HADOOP-6921](https://issues.apache.org/jira/browse/HADOOP-6921) | *Major* | **metrics2: metrics plugins**

Metrics names are standardized to CapitalizedCamelCase. See release note of HADOOP-6918 and HADOOP-6920.


---

* [HDFS-1814](https://issues.apache.org/jira/browse/HDFS-1814) | *Major* | **HDFS portion of HADOOP-7214 - Hadoop /usr/bin/groups equivalent**

Introduces a new command, "hdfs groups", which displays what groups are associated with a user as seen by the NameNode.


---

* [MAPREDUCE-2473](https://issues.apache.org/jira/browse/MAPREDUCE-2473) | *Major* | **MR portion of HADOOP-7214 - Hadoop /usr/bin/groups equivalent**

Introduces a new command, "mapred groups", which displays what groups are associated with a user as seen by the JobTracker.


---

* [HDFS-1917](https://issues.apache.org/jira/browse/HDFS-1917) | *Major* | **Clean up duplication of dependent jar files**

Remove packaging of duplicated third party jar files


---

* [HDFS-1117](https://issues.apache.org/jira/browse/HDFS-1117) | *Major* | **HDFS portion of HADOOP-6728 (ovehaul metrics framework)**

Metrics names are standardized to use CapitalizedCamelCase. Some examples:
# Metrics names using "\_" is changed to new naming scheme. Eg: bytes\_written changes to BytesWritten.
# All metrics names start with capitals. Example: threadsBlocked changes to ThreadsBlocked.


---

* [HDFS-1945](https://issues.apache.org/jira/browse/HDFS-1945) | *Major* | **Removed deprecated fields in DataTransferProtocol**

Removed the deprecated fields in DataTransferProtocol.


---

* [HADOOP-7286](https://issues.apache.org/jira/browse/HADOOP-7286) | *Major* | **Refactor FsShell's du/dus/df**

The "Found X items" header on the output of the "du" command has been removed to more closely match unix. The displayed paths now correspond to the command line arguments instead of always being a fully qualified URI. For example, the output will have relative paths if the command line arguments are relative paths.


---

* [HDFS-1939](https://issues.apache.org/jira/browse/HDFS-1939) | *Major* | **ivy: test conf should not extend common conf**

\* Removed duplicated jars in test class path.


---

* [MAPREDUCE-2483](https://issues.apache.org/jira/browse/MAPREDUCE-2483) | *Major* | **Clean up duplication of dependent jar files**

Removed duplicated hadoop-common library dependencies.


---

* [MAPREDUCE-2407](https://issues.apache.org/jira/browse/MAPREDUCE-2407) | *Major* | **Make Gridmix emulate usage of Distributed Cache files**

Makes Gridmix emulate HDFS based distributed cache files and local file system based distributed cache files.


---

* [MAPREDUCE-2492](https://issues.apache.org/jira/browse/MAPREDUCE-2492) | *Major* | **[MAPREDUCE] The new MapReduce API should make available task's progress to the task**

Map and Reduce task can access the attempt's overall progress via TaskAttemptContext.


---

* [HADOOP-7322](https://issues.apache.org/jira/browse/HADOOP-7322) | *Minor* | **Adding a util method in FileUtil for JDK File.listFiles**

Use of this new utility method avoids null result from File.listFiles(), and consequent NPEs.


---

* [MAPREDUCE-2137](https://issues.apache.org/jira/browse/MAPREDUCE-2137) | *Major* | **Mapping between Gridmix jobs and the corresponding original MR jobs is needed**

New configuration properties gridmix.job.original-job-id and gridmix.job.original-job-name in the configuration of simulated job are exposed/documented to gridmix user for mapping between original cluster's jobs and simulated jobs.


---

* [MAPREDUCE-2408](https://issues.apache.org/jira/browse/MAPREDUCE-2408) | *Major* | **Make Gridmix emulate usage of data compression**

Emulates the MapReduce compression feature in Gridmix. By default, compression emulation is turned on. Compression emulation can be disabled by setting 'gridmix.compression-emulation.enable' to 'false'.  Use 'gridmix.compression-emulation.map-input.decompression-ratio', 'gridmix.compression-emulation.map-output.compression-ratio' and 'gridmix.compression-emulation.reduce-output.compression-ratio' to configure the compression ratios at map input, map output and reduce output side respectively. Currently, compression ratios in the range [0.07, 0.68] are supported. Gridmix auto detects whether map-input, map output and reduce output should emulate compression based on original job's compression related configuration parameters.


---

* [MAPREDUCE-2517](https://issues.apache.org/jira/browse/MAPREDUCE-2517) | *Major* | **Porting Gridmix v3 system tests into trunk branch.**

Adds system tests to Gridmix. These system tests cover various features like job types (load and sleep), user resolvers (round-robin, submitter-user, echo) and  submission modes (stress, replay and serial).


---

* [HADOOP-6255](https://issues.apache.org/jira/browse/HADOOP-6255) | *Major* | **Create an rpm integration project**

Added RPM/DEB packages to build system.


---

* [HDFS-1963](https://issues.apache.org/jira/browse/HDFS-1963) | *Major* | **HDFS rpm integration project**

Create HDFS RPM package


---

* [MAPREDUCE-2521](https://issues.apache.org/jira/browse/MAPREDUCE-2521) | *Major* | **Mapreduce RPM integration project**

Created rpm and debian packages for MapReduce.


---

* [MAPREDUCE-2455](https://issues.apache.org/jira/browse/MAPREDUCE-2455) | *Major* | **Remove deprecated JobTracker.State in favour of JobTrackerStatus**

**WARNING: No release note provided for this change.**


---

* [MAPREDUCE-2544](https://issues.apache.org/jira/browse/MAPREDUCE-2544) | *Major* | **Gridmix compression emulation system tests.**

Adds system tests for testing the compression emulation feature of Gridmix.


---

* [HDFS-1636](https://issues.apache.org/jira/browse/HDFS-1636) | *Minor* | **If dfs.name.dir points to an empty dir, namenode format shouldn't require confirmation**

If dfs.name.dir points to an empty dir, namenode -format no longer requires confirmation.


---

* [HDFS-1966](https://issues.apache.org/jira/browse/HDFS-1966) | *Major* | **Encapsulate individual DataTransferProtocol op header**

Added header classes for individual DataTransferProtocol op headers.


---

* [MAPREDUCE-2469](https://issues.apache.org/jira/browse/MAPREDUCE-2469) | *Major* | **Task counters should also report the total heap usage of the task**

Task attempt's total heap usage gets recorded and published via counters as COMMITTED\_HEAP\_BYTES.


---

* [MAPREDUCE-2543](https://issues.apache.org/jira/browse/MAPREDUCE-2543) | *Major* | **[Gridmix] Add support for HighRam jobs**

Adds High-Ram feature emulation in Gridmix.


---

* [HADOOP-7331](https://issues.apache.org/jira/browse/HADOOP-7331) | *Trivial* | **Make hadoop-daemon.sh to return 1 if daemon processes did not get started**

hadoop-daemon.sh now returns a non-zero exit code if it detects that the daemon was not still running after 3 seconds.


---

* [MAPREDUCE-2554](https://issues.apache.org/jira/browse/MAPREDUCE-2554) | *Major* | **Gridmix distributed cache emulation system tests.**

Adds distributed cache related system tests to Gridmix.


---

* [MAPREDUCE-2529](https://issues.apache.org/jira/browse/MAPREDUCE-2529) | *Major* | **Recognize Jetty bug 1342 and handle it**

Added 2 new config parameters:

mapreduce.reduce.shuffle.catch.exception.stack.regex
mapreduce.reduce.shuffle.catch.exception.message.regex


---

* [MAPREDUCE-2104](https://issues.apache.org/jira/browse/MAPREDUCE-2104) | *Major* | **Rumen TraceBuilder Does Not Emit CPU/Memory Usage Details in Traces**

Adds cpu, physical memory, virtual memory and heap usages to TraceBuilder's output.


---

* [HADOOP-5647](https://issues.apache.org/jira/browse/HADOOP-5647) | *Major* | **TestJobHistory fails if /tmp/\_logs is not writable to. Testcase should not depend on /tmp**

Removed dependency of testcase on /tmp and made it to use test.build.data directory instead.


---

* [MAPREDUCE-587](https://issues.apache.org/jira/browse/MAPREDUCE-587) | *Minor* | **Stream test TestStreamingExitStatus fails with Out of Memory**

Fixed the streaming test TestStreamingExitStatus's failure due to an OutOfMemory error by reducing the testcase's io.sort.mb.


---

* [HDFS-2058](https://issues.apache.org/jira/browse/HDFS-2058) | *Major* | **DataTransfer Protocol using protobufs**

**WARNING: No release note provided for this change.**


---

* [MAPREDUCE-2430](https://issues.apache.org/jira/browse/MAPREDUCE-2430) | *Major* | **Remove mrunit contrib**

MRUnit is now available as a separate Apache project.


---

* [HADOOP-7374](https://issues.apache.org/jira/browse/HADOOP-7374) | *Major* | **Don't add tools.jar to the classpath when running Hadoop**

The scripts that run Hadoop no longer automatically add tools.jar from the JDK to the classpath (if it is present). If your job depends on tools.jar in the JDK you will need to add this dependency in your job.


---

* [HDFS-2066](https://issues.apache.org/jira/browse/HDFS-2066) | *Major* | **Create a package and individual class files for DataTransferProtocol**

**WARNING: No release note provided for this change.**


---

* [MAPREDUCE-2106](https://issues.apache.org/jira/browse/MAPREDUCE-2106) | *Major* | **Emulate CPU Usage of Tasks in GridMix3**

Adds cumulative cpu usage emulation to Gridmix


---

* [MAPREDUCE-2563](https://issues.apache.org/jira/browse/MAPREDUCE-2563) | *Major* | **Gridmix high ram jobs emulation system tests.**

Adds system tests to test the High-Ram feature in Gridmix.


---

* [MAPREDUCE-2107](https://issues.apache.org/jira/browse/MAPREDUCE-2107) | *Major* | **Emulate Memory Usage of Tasks in GridMix3**

Adds total heap usage emulation to Gridmix. Also, Gridmix can configure the simulated task's JVM heap options with max heap options obtained from the original task (via Rumen). Use 'gridmix.task.jvm-options.enable' to disable the task max heap options configuration.


---

* [HADOOP-7379](https://issues.apache.org/jira/browse/HADOOP-7379) | *Major* | **Add ability to include Protobufs in ObjectWritable**

Protocol buffer-generated types may now be used as arguments or return values for Hadoop RPC.


---

* [HDFS-2055](https://issues.apache.org/jira/browse/HDFS-2055) | *Major* | **Add hflush support to libhdfs**

Add hdfsHFlush to libhdfs.


---

* [HDFS-2087](https://issues.apache.org/jira/browse/HDFS-2087) | *Major* | **Add methods to DataTransferProtocol interface**

Declare methods in DataTransferProtocol interface, and change Sender and Receiver to implement the interface.


---

* [HDFS-1321](https://issues.apache.org/jira/browse/HDFS-1321) | *Minor* | **If service port and main port are the same, there is no clear log message explaining the issue.**

Added a check to match the sure RPC and HTTP Port's on the NameNode were not set to the same value, otherwise an IOException is throw with the appropriate message.


---

* [HDFS-1723](https://issues.apache.org/jira/browse/HDFS-1723) | *Minor* | **quota errors messages should use the same scale**

Updated the Quota exceptions to now use human readable output.


---

* [HDFS-2107](https://issues.apache.org/jira/browse/HDFS-2107) | *Major* | **Move block management code to a package**

Moved block management codes to a new package org.apache.hadoop.hdfs.server.blockmanagement.


---

* [MAPREDUCE-2596](https://issues.apache.org/jira/browse/MAPREDUCE-2596) | *Major* | **Gridmix should notify job failures**

Gridmix now prints a summary information after every run. It summarizes the runs w.r.t input trace details, input data statistics, cli arguments, data-gen runtime, simulation runtimes etc and also the cluster w.r.t map slots, reduce slots, jobtracker-address, hdfs-address etc.


---

* [MAPREDUCE-2606](https://issues.apache.org/jira/browse/MAPREDUCE-2606) | *Major* | **Remove IsolationRunner**

IsolationRunner is no longer maintained. See MAPREDUCE-2637 for its replacement.


---

* [HADOOP-7305](https://issues.apache.org/jira/browse/HADOOP-7305) | *Minor* | **Eclipse project files are incomplete**

Added missing library during creation of the eclipse project files.


---

* [HADOOP-2081](https://issues.apache.org/jira/browse/HADOOP-2081) | *Major* | **Configuration getInt, getLong, and getFloat replace invalid numbers with the default value**

Invalid configuration values now result in a number format exception rather than the default value being used.


---

* [HADOOP-6385](https://issues.apache.org/jira/browse/HADOOP-6385) | *Minor* | **dfs does not support -rmdir (was HDFS-639)**

The "rm" family of FsShell commands now supports -rmdir and -f options.


---

* [HDFS-2210](https://issues.apache.org/jira/browse/HDFS-2210) | *Major* | **Remove hdfsproxy**

The hdfsproxy contrib component is no longer supported.


---

* [HDFS-1073](https://issues.apache.org/jira/browse/HDFS-1073) | *Major* | **Simpler model for Namenode's fs Image and edit Logs**

The NameNode's storage layout for its name directories has been reorganized to be more robust. Each edit now has a unique transaction ID, and each file is associated with a transaction ID (for checkpoints) or a range of transaction IDs (for edit logs).


---

* [HDFS-1381](https://issues.apache.org/jira/browse/HDFS-1381) | *Major* | **HDFS javadocs hard-code references to dfs.namenode.name.dir and dfs.datanode.data.dir parameters**

Updated the JavaDocs to appropriately represent the new Configuration Keys that are used in the code. The docs did not match the code.


---

* [HDFS-2202](https://issues.apache.org/jira/browse/HDFS-2202) | *Major* | **Changes to balancer bandwidth should not require datanode restart.**

New dfsadmin command added: [-setBalancerBandwidth \<bandwidth\>] where bandwidth is max network bandwidth in bytes per second that the balancer is allowed to use on each datanode during balacing.

This is an incompatible change in 0.23.  The versions of ClientProtocol and DatanodeProtocol are changed.


---

* [MAPREDUCE-2494](https://issues.apache.org/jira/browse/MAPREDUCE-2494) | *Major* | **Make the distributed cache delete entires using LRU priority**

Added config option mapreduce.tasktracker.cache.local.keep.pct to the TaskTracker.  It is the target percentage of the local distributed cache that should be kept in between garbage collection runs.  In practice it will delete unused distributed cache entries in LRU order until the size of the cache is less than mapreduce.tasktracker.cache.local.keep.pct of the maximum cache size.  This is a floating point value between 0.0 and 1.0.  The default is 0.95.


---

* [MAPREDUCE-2037](https://issues.apache.org/jira/browse/MAPREDUCE-2037) | *Major* | **Capturing interim progress times, CPU usage, and memory usage, when tasks reach certain progress thresholds**

Capture intermediate task resource consumption information:
\* Time taken so far
\* CPU load [either at the time the data are taken, or exponentially smoothed]
\* Memory load [also either at the time the data are taken, or exponentially smoothed]

This would be taken at intervals that depend on the task progress plateaus. For example, reducers have three progress ranges - [0-1/3], (1/3-2/3], and (2/3-3/3] - where fundamentally different activities happen. Mappers have different boundaries that are not symmetrically placed [0-9/10], (9/10-1]. Data capture boundaries should coincide with activity boundaries. For the state information capture [CPU and memory] we should average over the covered interval.


---

* [MAPREDUCE-901](https://issues.apache.org/jira/browse/MAPREDUCE-901) | *Major* | **Move Framework Counters into a TaskMetric structure**

Efficient implementation of MapReduce framework counters.


---

* [MAPREDUCE-1738](https://issues.apache.org/jira/browse/MAPREDUCE-1738) | *Major* | **MapReduce portion of HADOOP-6728 (ovehaul metrics framework)**

**WARNING: No release note provided for this change.**


---

* [MAPREDUCE-279](https://issues.apache.org/jira/browse/MAPREDUCE-279) | *Major* | **Map-Reduce 2.0**

MapReduce has undergone a complete re-haul in hadoop-0.23 and we now have, what we call, MapReduce 2.0 (MRv2).

The fundamental idea of MRv2 is to split up the two major functionalities of the JobTracker, resource management and job scheduling/monitoring, into separate daemons. The idea is to have a global ResourceManager (RM) and per-application ApplicationMaster (AM).  An application is either a single job in the classical sense of Map-Reduce jobs or a DAG of jobs. The ResourceManager and per-node slave, the NodeManager (NM), form the data-computation framework. The ResourceManager is the ultimate authority that arbitrates resources among all the applications in the system. The per-application ApplicationMaster is, in effect, a framework specific library and is tasked with negotiating resources from the ResourceManager and working with the NodeManager(s) to execute and monitor the tasks.

The ResourceManager has two main components:
\* Scheduler (S)
\* ApplicationsManager (ASM)

The Scheduler is responsible for allocating resources to the various running applications subject to familiar constraints of capacities, queues etc. The Scheduler is pure scheduler in the sense that it performs no monitoring or tracking of status for the application. Also, it offers no guarantees on restarting failed tasks either due to application failure or hardware failures. The Scheduler performs its scheduling function based the resource requirements of the applications; it does so based on the abstract notion of a Resource Container which incorporates elements such as memory, cpu, disk, network etc.

The Scheduler has a pluggable policy plug-in, which is responsible for partitioning the cluster resources among the various queues, applications etc. The current Map-Reduce schedulers such as the CapacityScheduler and the FairScheduler would be some examples of the plug-in.

The CapacityScheduler supports hierarchical queues to allow for more predictable sharing of cluster resources.
The ApplicationsManager is responsible for accepting job-submissions, negotiating the first container for executing the application specific ApplicationMaster and provides the service for restarting the ApplicationMaster container on failure.

The NodeManager is the per-machine framework agent who is responsible for launching the applications' containers, monitoring their resource usage (cpu, memory, disk, network) and reporting the same to the Scheduler.

The per-application ApplicationMaster has the responsibility of negotiating appropriate resource containers from the Scheduler, tracking their status and monitoring for progress.


---

* [HADOOP-7264](https://issues.apache.org/jira/browse/HADOOP-7264) | *Major* | **Bump avro version to at least 1.4.1**

**WARNING: No release note provided for this change.**


---

* [MAPREDUCE-2649](https://issues.apache.org/jira/browse/MAPREDUCE-2649) | *Major* | **MR279: Fate of finished Applications on RM**

New config added:

   // the maximum number of completed applications the RM keeps \<name\>yarn.server.resourcemanager.expire.applications.completed.max\</name\>


---

* [MAPREDUCE-2846](https://issues.apache.org/jira/browse/MAPREDUCE-2846) | *Blocker* | **a small % of all tasks fail with DefaultTaskController**

Fixed a race condition in writing the log index file that caused tasks to 'fail'.


---

* [HADOOP-7547](https://issues.apache.org/jira/browse/HADOOP-7547) | *Minor* | **Fix the warning in writable classes.[ WritableComparable is a raw type. References to generic type WritableComparable\<T\> should be parameterized  ]**

**WARNING: No release note provided for this change.**


---

* [HDFS-1620](https://issues.apache.org/jira/browse/HDFS-1620) | *Minor* | **Rename HdfsConstants -\> HdfsServerConstants, FSConstants -\> HdfsConstants**

Rename HdfsConstants interface to HdfsServerConstants, FSConstants interface to HdfsConstants


---

* [HADOOP-7507](https://issues.apache.org/jira/browse/HADOOP-7507) | *Major* | **jvm metrics all use the same namespace**

JVM metrics published to Ganglia now include the process name as part of the gmetric name.


---

* [HADOOP-7119](https://issues.apache.org/jira/browse/HADOOP-7119) | *Major* | **add Kerberos HTTP SPNEGO authentication support to Hadoop JT/NN/DN/TT web-consoles**

Adding support for Kerberos HTTP SPNEGO authentication to the Hadoop web-consoles


---

* [HDFS-2338](https://issues.apache.org/jira/browse/HDFS-2338) | *Major* | **Configuration option to enable/disable webhdfs.**

Added a conf property dfs.webhdfs.enabled for enabling/disabling webhdfs.


---

* [HDFS-2318](https://issues.apache.org/jira/browse/HDFS-2318) | *Major* | **Provide authentication to webhdfs using SPNEGO**

Added two new conf properties dfs.web.authentication.kerberos.principal and dfs.web.authentication.kerberos.keytab for the SPNEGO servlet filter.


---

* [MAPREDUCE-3042](https://issues.apache.org/jira/browse/MAPREDUCE-3042) | *Major* | **YARN RM fails to start**

Simple typo fix to allow ResourceManager to start instead of fail


---

* [MAPREDUCE-2930](https://issues.apache.org/jira/browse/MAPREDUCE-2930) | *Major* | **Generate state graph from the State Machine Definition**

Generate state graph from State Machine Definition


---

* [MAPREDUCE-3081](https://issues.apache.org/jira/browse/MAPREDUCE-3081) | *Major* | **Change the name format for hadoop core and vaidya jar to be hadoop-{core/vaidya}-{version}.jar in vaidya.sh**

contrib/vaidya/bin/vaidya.sh script fixed to use appropriate jars and classpath


---

* [MAPREDUCE-3041](https://issues.apache.org/jira/browse/MAPREDUCE-3041) | *Blocker* | **Enhance YARN Client-RM protocol to provide access to information such as cluster's Min/Max Resource capabilities similar to that of AM-RM protocol**

**WARNING: No release note provided for this change.**


---

* [HADOOP-7668](https://issues.apache.org/jira/browse/HADOOP-7668) | *Minor* | **Add a NetUtils method that can tell if an InetAddress belongs to local host**

closing again


---

* [HDFS-2355](https://issues.apache.org/jira/browse/HDFS-2355) | *Major* | **Federation: enable using the same configuration file across all the nodes in the cluster.**

This change allows when running multiple namenodes on different hosts, sharing the same configuration file across all the nodes in the cluster (Datanodes, NamNode, BackupNode, SecondaryNameNode), without the need to define dfs.federation.nameservice.id parameter.


---

* [MAPREDUCE-2719](https://issues.apache.org/jira/browse/MAPREDUCE-2719) | *Major* | **MR-279: Write a shell command application**

Adding a simple, DistributedShell application as an alternate framework to MapReduce and to act as an illustrative example for porting applications to YARN.


---

* [HADOOP-7691](https://issues.apache.org/jira/browse/HADOOP-7691) | *Major* | **hadoop deb pkg should take a diff group id**

Fixed conflict uid for install packages. (Eric Yang)


---

* [HADOOP-7603](https://issues.apache.org/jira/browse/HADOOP-7603) | *Major* | **Set default hdfs, mapred uid, and hadoop group gid for RPM packages**

Set hdfs uid, mapred uid, and hadoop gid to fixed numbers (201, 202, and 123, respectively).


---

* [HADOOP-7684](https://issues.apache.org/jira/browse/HADOOP-7684) | *Major* | **jobhistory server and secondarynamenode should have init.d script**

Added init.d script for jobhistory server and secondary namenode. (Eric Yang)


---

* [MAPREDUCE-3112](https://issues.apache.org/jira/browse/MAPREDUCE-3112) | *Major* | **Calling hadoop cli inside mapreduce job leads to errors**

Removed inheritance of certain server environment variables (HADOOP\_OPTS and HADOOP\_ROOT\_LOGGER) in task attempt process.


---

* [HADOOP-7715](https://issues.apache.org/jira/browse/HADOOP-7715) | *Major* | **see log4j Error when running mr jobs and certain dfs calls**

Removed unnecessary security logger configuration. (Eric Yang)


---

* [HADOOP-7711](https://issues.apache.org/jira/browse/HADOOP-7711) | *Major* | **hadoop-env.sh generated from templates has duplicate info**

Fixed recursive sourcing of HADOOP\_OPTS environment variables (Arpit Gupta via Eric Yang)


---

* [HADOOP-7681](https://issues.apache.org/jira/browse/HADOOP-7681) | *Minor* | **log4j.properties is missing properties for security audit and hdfs audit should be changed to info**

HADOOP-7681. Fixed security and hdfs audit log4j properties
(Arpit Gupta via Eric Yang)


---

* [HADOOP-7708](https://issues.apache.org/jira/browse/HADOOP-7708) | *Critical* | **config generator does not update the properties file if on exists already**

Fixed hadoop-setup-conf.sh to handle config file consistently.  (Eric Yang)


---

* [HADOOP-7707](https://issues.apache.org/jira/browse/HADOOP-7707) | *Major* | **improve config generator to allow users to specify proxy user, turn append on or off, turn webhdfs on or off**

Added toggle for dfs.support.append, webhdfs and hadoop proxy user to setup config script. (Arpit Gupta via Eric Yang)


---

* [HADOOP-7720](https://issues.apache.org/jira/browse/HADOOP-7720) | *Major* | **improve the hadoop-setup-conf.sh to read in the hbase user and setup the configs**

Added parameter for HBase user to setup config script. (Arpit Gupta via Eric Yang)


---

* [MAPREDUCE-2702](https://issues.apache.org/jira/browse/MAPREDUCE-2702) | *Blocker* | **[MR-279] OutputCommitter changes for MR Application Master recovery**

Enhance OutputCommitter and FileOutputCommitter to allow for recover of tasks across job restart.


---

* [HADOOP-7724](https://issues.apache.org/jira/browse/HADOOP-7724) | *Major* | **hadoop-setup-conf.sh should put proxy user info into the core-site.xml**

Fixed hadoop-setup-conf.sh to put proxy user in core-site.xml.  (Arpit Gupta via Eric Yang)


---

* [MAPREDUCE-3157](https://issues.apache.org/jira/browse/MAPREDUCE-3157) | *Major* | **Rumen TraceBuilder is skipping analyzing 0.20 history files**

Fixes TraceBuilder to handle 0.20 history file names also.


---

* [MAPREDUCE-3166](https://issues.apache.org/jira/browse/MAPREDUCE-3166) | *Major* | **Make Rumen use job history api instead of relying on current history file name format**

Makes Rumen use job history api instead of relying on current history file name format.


---

* [MAPREDUCE-2789](https://issues.apache.org/jira/browse/MAPREDUCE-2789) | *Major* | **[MR:279] Update the scheduling info on CLI**

"mapred/job -list" now contains map/reduce, container, and resource information.


---

* [MAPREDUCE-2764](https://issues.apache.org/jira/browse/MAPREDUCE-2764) | *Major* | **Fix renewal of dfs delegation tokens**

Generalizes token renewal and canceling to a common interface and provides a plugin interface for adding renewers for new kinds of tokens. Hftp changed to store the tokens as HFTP and renew them over http.


---

* [HADOOP-7655](https://issues.apache.org/jira/browse/HADOOP-7655) | *Major* | **provide a small validation script that smoke tests the installed cluster**

Committed to trunk and v23, since code reviewed by Eric.


---

* [MAPREDUCE-2858](https://issues.apache.org/jira/browse/MAPREDUCE-2858) | *Blocker* | **MRv2 WebApp Security**

A new server has been added to yarn.  It is a web proxy that sits in front of the AM web UI.  The server is controlled by the yarn.web-proxy.address config.  If that config is set, and it points to an address that is different then the RM web interface then a separate proxy server needs to be launched.

This can be done by running

yarn-daemon.sh start proxyserver

If a separate proxy server is needed other configs also may need to be set, if security is enabled.
yarn.web-proxy.principal
yarn.web-proxy.keytab

The proxy server is stateless and should be able to support a VIP or other load balancing sitting in front of multiple instances of this server.


---

* [MAPREDUCE-3205](https://issues.apache.org/jira/browse/MAPREDUCE-3205) | *Blocker* | **MR2 memory limits should be pmem, not vmem**

Resource limits are now expressed and enforced in terms of physical memory, rather than virtual memory. The virtual memory limit is set as a configurable multiple of the physical limit. The NodeManager's memory usage is now configured in units of MB rather than GB.


---

* [HDFS-1869](https://issues.apache.org/jira/browse/HDFS-1869) | *Major* | **mkdirs should use the supplied permission for all of the created directories**

A multi-level mkdir is now POSIX compliant.  Instead of creating intermediate directories with the permissions of the parent directory, intermediate directories are created with permission bits of rwxrwxrwx (0777) as modified by the current umask, plus write and search permission for the owner.


---

* [HADOOP-7728](https://issues.apache.org/jira/browse/HADOOP-7728) | *Major* | **hadoop-setup-conf.sh should be modified to enable task memory manager**

Enable task memory management to be configurable via hadoop config setup script.


---

* [MAPREDUCE-3186](https://issues.apache.org/jira/browse/MAPREDUCE-3186) | *Blocker* | **User jobs are getting hanged if the Resource manager process goes down and comes up while job is getting executed.**

New Yarn configuration property:

Name: yarn.app.mapreduce.am.scheduler.connection.retries
Description: Number of times AM should retry to contact RM if connection is lost.


---

* [MAPREDUCE-2736](https://issues.apache.org/jira/browse/MAPREDUCE-2736) | *Major* | **Remove unused contrib components dependent on MR1**

The pre-MR2 MapReduce implementation (JobTracker, TaskTracer, etc) and contrib components are no longer supported. This implementation is currently supported in the 0.20.20x releases.


---

* [HADOOP-7740](https://issues.apache.org/jira/browse/HADOOP-7740) | *Minor* | **security audit logger is not on by default, fix the log4j properties to enable the logger**

Fixed security audit logger configuration. (Arpit Gupta via Eric Yang)


---

* [HDFS-2465](https://issues.apache.org/jira/browse/HDFS-2465) | *Major* | **Add HDFS support for fadvise readahead and drop-behind**

HDFS now has the ability to use posix\_fadvise and sync\_data\_range syscalls to manage the OS buffer cache. This support is currently considered experimental, and may be enabled by configuring the following keys:
dfs.datanode.drop.cache.behind.writes - set to true to drop data out of the buffer cache after writing
dfs.datanode.drop.cache.behind.reads - set to true to drop data out of the buffer cache when performing sequential reads
dfs.datanode.sync.behind.writes - set to true to trigger dirty page writeback immediately after writing data
dfs.datanode.readahead.bytes - set to a non-zero value to trigger readahead for sequential reads


---

* [MAPREDUCE-3241](https://issues.apache.org/jira/browse/MAPREDUCE-3241) | *Major* | **(Rumen)TraceBuilder throws IllegalArgumentException**

Rumen is fixed to ignore the AMRestartedEvent.


---

* [MAPREDUCE-3317](https://issues.apache.org/jira/browse/MAPREDUCE-3317) | *Major* | **Rumen TraceBuilder is emiting null as hostname**

Fixes Rumen to get correct hostName that includes rackName in attempt info.



