
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
# Apache Hadoop Changelog

## Release 1.2.0 - 2013-05-13

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-8164](https://issues.apache.org/jira/browse/HADOOP-8164) | Handle paths using back slash as path separator for windows only |  Major | fs | Suresh Srinivas | Daryn Sharp |
| [HDFS-4350](https://issues.apache.org/jira/browse/HDFS-4350) | Make enabling of stale marking on read and write paths independent |  Major | . | Andrew Wang | Andrew Wang |
| [HDFS-4122](https://issues.apache.org/jira/browse/HDFS-4122) | Cleanup HDFS logs and reduce the size of logged messages |  Major | datanode, hdfs-client, namenode | Suresh Srinivas | Suresh Srinivas |
| [MAPREDUCE-4737](https://issues.apache.org/jira/browse/MAPREDUCE-4737) |  Hadoop does not close output file / does not call Mapper.cleanup if exception in map |  Major | . | Daniel Dai | Arun C Murthy |
| [MAPREDUCE-4629](https://issues.apache.org/jira/browse/MAPREDUCE-4629) | Remove JobHistory.DEBUG\_MODE |  Major | . | Karthik Kambatla | Karthik Kambatla |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-9090](https://issues.apache.org/jira/browse/HADOOP-9090) | Support on-demand publish of metrics |  Minor | metrics | Mostafa Elhemali | Mostafa Elhemali |
| [HADOOP-8988](https://issues.apache.org/jira/browse/HADOOP-8988) | Backport HADOOP-8343 to branch-1 |  Major | conf | Jing Zhao | Jing Zhao |
| [HADOOP-8820](https://issues.apache.org/jira/browse/HADOOP-8820) | Backport HADOOP-8469 and HADOOP-8470: add "NodeGroup" layer in new NetworkTopology (also known as NetworkTopologyWithNodeGroup) |  Major | net | Junping Du | Junping Du |
| [HADOOP-8023](https://issues.apache.org/jira/browse/HADOOP-8023) | Add unset() method to Configuration |  Critical | conf | Alejandro Abdelnur | Alejandro Abdelnur |
| [HDFS-4776](https://issues.apache.org/jira/browse/HDFS-4776) | Backport SecondaryNameNode web ui to branch-1 |  Minor | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-4774](https://issues.apache.org/jira/browse/HDFS-4774) | Backport HDFS-4525 'Provide an API for knowing whether file is closed or not' to branch-1 |  Major | hdfs-client, namenode | Ted Yu | Ted Yu |
| [HDFS-4597](https://issues.apache.org/jira/browse/HDFS-4597) | Backport WebHDFS concat to branch-1 |  Major | webhdfs | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-4219](https://issues.apache.org/jira/browse/HDFS-4219) | Port slive to branch-1 |  Major | . | Arpit Gupta | Arpit Gupta |
| [HDFS-3942](https://issues.apache.org/jira/browse/HDFS-3942) | Backport HDFS-3495: Update balancer policy for Network Topology with additional 'NodeGroup' layer |  Major | balancer & mover | Junping Du | Junping Du |
| [HDFS-3941](https://issues.apache.org/jira/browse/HDFS-3941) | Backport HDFS-3498 and HDFS3601: update replica placement policy for new added "NodeGroup" layer topology |  Major | namenode | Junping Du | Junping Du |
| [HDFS-3601](https://issues.apache.org/jira/browse/HDFS-3601) | Implementation of ReplicaPlacementPolicyNodeGroup to support 4-layer network topology |  Major | namenode | Junping Du | Junping Du |
| [HDFS-3515](https://issues.apache.org/jira/browse/HDFS-3515) | Port HDFS-1457 to branch-1 |  Major | namenode | Eli Collins | Eli Collins |
| [HDFS-3495](https://issues.apache.org/jira/browse/HDFS-3495) | Update Balancer to support new NetworkTopology with NodeGroup |  Major | balancer & mover | Junping Du | Junping Du |
| [MAPREDUCE-5129](https://issues.apache.org/jira/browse/MAPREDUCE-5129) | Add tag info to JH files |  Minor | . | Billie Rinaldi | Billie Rinaldi |
| [MAPREDUCE-5081](https://issues.apache.org/jira/browse/MAPREDUCE-5081) | Backport DistCpV2 and the related JIRAs to branch-1 |  Major | distcp | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [MAPREDUCE-4824](https://issues.apache.org/jira/browse/MAPREDUCE-4824) | Provide a mechanism for jobs to indicate they should not be recovered on restart |  Major | mrv1 | Tom White | Tom White |
| [MAPREDUCE-4660](https://issues.apache.org/jira/browse/MAPREDUCE-4660) | Update task placement policy for NetworkTopology with 'NodeGroup' layer |  Major | jobtracker, mrv1, scheduler | Junping Du | Junping Du |
| [MAPREDUCE-4355](https://issues.apache.org/jira/browse/MAPREDUCE-4355) | Add RunningJob.getJobStatus() |  Major | mrv1, mrv2 | Karthik Kambatla | Karthik Kambatla |
| [MAPREDUCE-3678](https://issues.apache.org/jira/browse/MAPREDUCE-3678) | The Map tasks logs should have the value of input split it processed |  Major | mrv1, mrv2 | Bejoy KS | Harsh J |
| [MAPREDUCE-987](https://issues.apache.org/jira/browse/MAPREDUCE-987) | Exposing MiniDFS and MiniMR clusters as a single process command-line |  Minor | build, test | Philip Zeyliger | Ahmed Radwan |
| [MAPREDUCE-461](https://issues.apache.org/jira/browse/MAPREDUCE-461) | Enable ServicePlugins for the JobTracker |  Minor | . | Fredrik Hedberg | Fredrik Hedberg |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-9434](https://issues.apache.org/jira/browse/HADOOP-9434) | Backport HADOOP-9267 to branch-1 |  Minor | bin | Yu Li | Yu Li |
| [HADOOP-9379](https://issues.apache.org/jira/browse/HADOOP-9379) | capture the ulimit info after printing the log to the console |  Trivial | . | Arpit Gupta | Arpit Gupta |
| [HADOOP-9253](https://issues.apache.org/jira/browse/HADOOP-9253) | Capture ulimit info in the logs at service start time |  Major | . | Arpit Gupta | Arpit Gupta |
| [HADOOP-9071](https://issues.apache.org/jira/browse/HADOOP-9071) | configure ivy log levels for resolve/retrieve |  Major | build | Giridharan Kesavan | Giridharan Kesavan |
| [HADOOP-8971](https://issues.apache.org/jira/browse/HADOOP-8971) | Backport: hadoop.util.PureJavaCrc32 cache hit-ratio is low for static data (HADOOP-8926) |  Major | util | Gopal V | Gopal V |
| [HADOOP-8968](https://issues.apache.org/jira/browse/HADOOP-8968) | Add a flag to completely disable the worker version check |  Major | . | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-8951](https://issues.apache.org/jira/browse/HADOOP-8951) | RunJar to fail with user-comprehensible error message if jar missing |  Minor | util | Steve Loughran | Steve Loughran |
| [HADOOP-8931](https://issues.apache.org/jira/browse/HADOOP-8931) | Add Java version to startup message |  Trivial | . | Eli Collins | Eli Collins |
| [HADOOP-8711](https://issues.apache.org/jira/browse/HADOOP-8711) | provide an option for IPC server users to avoid printing stack information for certain exceptions |  Major | ipc | Brandon Li | Brandon Li |
| [HADOOP-7688](https://issues.apache.org/jira/browse/HADOOP-7688) | When a servlet filter throws an exception in init(..), the Jetty server failed silently. |  Major | . | Tsz Wo Nicholas Sze | Uma Maheswara Rao G |
| [HADOOP-7096](https://issues.apache.org/jira/browse/HADOOP-7096) | Allow setting of end-of-record delimiter for TextInputFormat |  Major | . | Ahmed Radwan | Ahmed Radwan |
| [HDFS-4651](https://issues.apache.org/jira/browse/HDFS-4651) | Offline Image Viewer backport to branch-1 |  Major | tools | Chris Nauroth | Chris Nauroth |
| [HDFS-4635](https://issues.apache.org/jira/browse/HDFS-4635) | Move BlockManager#computeCapacity to LightWeightGSet |  Major | namenode | Suresh Srinivas | Suresh Srinivas |
| [HDFS-4551](https://issues.apache.org/jira/browse/HDFS-4551) | Change WebHDFS buffersize behavior to improve default performance |  Major | webhdfs | Mark Wagner | Mark Wagner |
| [HDFS-4320](https://issues.apache.org/jira/browse/HDFS-4320) | Add a separate configuration for namenode rpc address instead of only using fs.default.name |  Major | datanode, namenode | Mostafa Elhemali | Mostafa Elhemali |
| [HDFS-4062](https://issues.apache.org/jira/browse/HDFS-4062) | In branch-1, FSNameSystem#invalidateWorkForOneNode and FSNameSystem#computeReplicationWorkForBlock should print logs outside of the namesystem lock |  Minor | . | Jing Zhao | Jing Zhao |
| [HDFS-4057](https://issues.apache.org/jira/browse/HDFS-4057) | NameNode.namesystem should be private. Use getNamesystem() instead. |  Minor | namenode | Brandon Li | Brandon Li |
| [HDFS-3940](https://issues.apache.org/jira/browse/HDFS-3940) | Add Gset#clear method and clear the block map when namenode is shutdown |  Minor | . | Eli Collins | Suresh Srinivas |
| [HDFS-3838](https://issues.apache.org/jira/browse/HDFS-3838) | fix the typo in FSEditLog.java:  isToterationEnabled should be isTolerationEnabled |  Trivial | namenode | Brandon Li | Brandon Li |
| [HDFS-3819](https://issues.apache.org/jira/browse/HDFS-3819) | Should check whether invalidate work percentage default value is not greater than 1.0f |  Minor | . | Jing Zhao | Jing Zhao |
| [HDFS-3817](https://issues.apache.org/jira/browse/HDFS-3817) | avoid printing stack information for SafeModeException |  Major | namenode | Brandon Li | Brandon Li |
| [HDFS-3697](https://issues.apache.org/jira/browse/HDFS-3697) | Enable fadvise readahead by default |  Minor | datanode, performance | Todd Lipcon | Todd Lipcon |
| [HDFS-3647](https://issues.apache.org/jira/browse/HDFS-3647) | Backport HDFS-2868 (Add number of active transfer threads to the DataNode status) to branch-1 |  Major | datanode | Steve Hoffman | Harsh J |
| [HDFS-3604](https://issues.apache.org/jira/browse/HDFS-3604) | Add dfs.webhdfs.enabled to hdfs-default.xml |  Minor | . | Eli Collins | Eli Collins |
| [HDFS-3596](https://issues.apache.org/jira/browse/HDFS-3596) | Improve FSEditLog pre-allocation in branch-1 |  Minor | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3521](https://issues.apache.org/jira/browse/HDFS-3521) | Allow namenode to tolerate edit log corruption |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-3498](https://issues.apache.org/jira/browse/HDFS-3498) | Make Replica Removal Policy pluggable and ReplicaPlacementPolicyDefault extensible for reusing code in subclass |  Major | namenode | Junping Du | Junping Du |
| [HDFS-3479](https://issues.apache.org/jira/browse/HDFS-3479) | backport HDFS-3335 (check for edit log corruption at the end of the log) to branch-1 |  Major | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3163](https://issues.apache.org/jira/browse/HDFS-3163) | TestHDFSCLI.testAll fails if the user name is not all lowercase |  Trivial | test | Brandon Li | Brandon Li |
| [HDFS-2533](https://issues.apache.org/jira/browse/HDFS-2533) | Remove needless synchronization on FSDataSet.getBlockFile |  Minor | datanode, performance | Todd Lipcon | Todd Lipcon |
| [HDFS-1957](https://issues.apache.org/jira/browse/HDFS-1957) | Documentation for HFTP |  Minor | documentation | Ari Rabkin | Ari Rabkin |
| [HDFS-385](https://issues.apache.org/jira/browse/HDFS-385) | Design a pluggable interface to place replicas of blocks in HDFS |  Major | . | dhruba borthakur | dhruba borthakur |
| [MAPREDUCE-4907](https://issues.apache.org/jira/browse/MAPREDUCE-4907) | TrackerDistributedCacheManager issues too many getFileStatus calls |  Major | mrv1, tasktracker | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-4845](https://issues.apache.org/jira/browse/MAPREDUCE-4845) | ClusterStatus.getMaxMemory() and getUsedMemory() exist in MR1 but not MR2 |  Major | client | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-4837](https://issues.apache.org/jira/browse/MAPREDUCE-4837) | Add webservices for jobtracker |  Major | . | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-4703](https://issues.apache.org/jira/browse/MAPREDUCE-4703) | Add the ability to start the MiniMRClientCluster using the configurations used before it is being stopped. |  Major | mrv1, mrv2, test | Ahmed Radwan | Ahmed Radwan |
| [MAPREDUCE-4565](https://issues.apache.org/jira/browse/MAPREDUCE-4565) | Backport MR-2855 to branch-1: ResourceBundle lookup during counter name resolution takes a lot of time |  Major | . | Karthik Kambatla | Karthik Kambatla |
| [MAPREDUCE-4556](https://issues.apache.org/jira/browse/MAPREDUCE-4556) | FairScheduler: PoolSchedulable#updateDemand() has potential redundant computation |  Minor | contrib/fair-share | Karthik Kambatla | Karthik Kambatla |
| [MAPREDUCE-4499](https://issues.apache.org/jira/browse/MAPREDUCE-4499) | Looking for speculative tasks is very expensive in 1.x |  Major | mrv1, performance | Nathan Roberts | Koji Noguchi |
| [MAPREDUCE-4464](https://issues.apache.org/jira/browse/MAPREDUCE-4464) | Reduce tasks failing with NullPointerException in ConcurrentHashMap.get() |  Minor | task | Clint Heath | Clint Heath |
| [MAPREDUCE-4415](https://issues.apache.org/jira/browse/MAPREDUCE-4415) | Backport the Job.getInstance methods from MAPREDUCE-1505 to branch-1 |  Major | mrv1 | Harsh J | Harsh J |
| [MAPREDUCE-4408](https://issues.apache.org/jira/browse/MAPREDUCE-4408) | allow jobs to set a JAR that is in the distributed cached |  Major | mrv1, mrv2 | Alejandro Abdelnur | Robert Kanter |
| [MAPREDUCE-2931](https://issues.apache.org/jira/browse/MAPREDUCE-2931) | CLONE - LocalJobRunner should support parallel mapper execution |  Major | . | Forest Tan | Sandy Ryza |
| [MAPREDUCE-2770](https://issues.apache.org/jira/browse/MAPREDUCE-2770) | Improve hadoop.job.history.location doc in mapred-default.xml |  Trivial | documentation | Eli Collins | Sandy Ryza |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-9544](https://issues.apache.org/jira/browse/HADOOP-9544) | backport UTF8 encoding fixes to branch-1 |  Major | io | Chris Nauroth | Chris Nauroth |
| [HADOOP-9543](https://issues.apache.org/jira/browse/HADOOP-9543) | TestFsShellReturnCode may fail in branch-1 |  Minor | test | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-9537](https://issues.apache.org/jira/browse/HADOOP-9537) | Backport AIX patches to branch-1 |  Major | security | Arpit Agarwal | Arpit Agarwal |
| [HADOOP-9502](https://issues.apache.org/jira/browse/HADOOP-9502) | chmod does not return error exit codes for some exceptions |  Minor | fs | Ramya Sunil | Tsz Wo Nicholas Sze |
| [HADOOP-9492](https://issues.apache.org/jira/browse/HADOOP-9492) | Fix the typo in testConf.xml to make it consistent with FileUtil#copy() |  Trivial | test | Jing Zhao | Jing Zhao |
| [HADOOP-9473](https://issues.apache.org/jira/browse/HADOOP-9473) | typo in FileUtil copy() method |  Trivial | fs | Glen Mazza |  |
| [HADOOP-9467](https://issues.apache.org/jira/browse/HADOOP-9467) | Metrics2 record filtering (.record.filter.include/exclude) does not filter by name |  Major | metrics | Chris Nauroth | Chris Nauroth |
| [HADOOP-9458](https://issues.apache.org/jira/browse/HADOOP-9458) | In branch-1, RPC.getProxy(..) may call proxy.getProtocolVersion(..) without retry |  Critical | ipc | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-9451](https://issues.apache.org/jira/browse/HADOOP-9451) | Node with one topology layer should be handled as fault topology when NodeGroup layer is enabled |  Major | net | Junping Du | Junping Du |
| [HADOOP-9375](https://issues.apache.org/jira/browse/HADOOP-9375) | Port HADOOP-7290 to branch-1 to fix TestUserGroupInformation failure |  Trivial | test | Xiaobo Peng | Suresh Srinivas |
| [HADOOP-9369](https://issues.apache.org/jira/browse/HADOOP-9369) | DNS#reverseDns() can return hostname with . appended at the end |  Major | net | Karthik Kambatla | Karthik Kambatla |
| [HADOOP-9349](https://issues.apache.org/jira/browse/HADOOP-9349) | Confusing output when running hadoop version from one hadoop installation when HADOOP\_HOME points to another |  Major | tools | Sandy Ryza | Sandy Ryza |
| [HADOOP-9191](https://issues.apache.org/jira/browse/HADOOP-9191) | TestAccessControlList and TestJobHistoryConfig fail with JDK7 |  Major | test | Arpit Agarwal | Arpit Agarwal |
| [HADOOP-9179](https://issues.apache.org/jira/browse/HADOOP-9179) | TestFileSystem fails with open JDK7 |  Major | . | Brandon Li | Brandon Li |
| [HADOOP-9154](https://issues.apache.org/jira/browse/HADOOP-9154) | SortedMapWritable#putAll() doesn't add key/value classes to the map |  Major | io | Karthik Kambatla | Karthik Kambatla |
| [HADOOP-9124](https://issues.apache.org/jira/browse/HADOOP-9124) | SortedMapWritable violates contract of Map interface for equals() and hashCode() |  Minor | io | Patrick Hunt | Surenkumar Nihalani |
| [HADOOP-9099](https://issues.apache.org/jira/browse/HADOOP-9099) | NetUtils.normalizeHostName fails on domains where UnknownHost resolves to an IP address |  Minor | test | Ivan Mitic | Ivan Mitic |
| [HADOOP-9098](https://issues.apache.org/jira/browse/HADOOP-9098) | Add missing license headers |  Blocker | build | Tom White | Arpit Agarwal |
| [HADOOP-9095](https://issues.apache.org/jira/browse/HADOOP-9095) | TestNNThroughputBenchmark fails in branch-1 |  Minor | net | Tsz Wo Nicholas Sze | Jing Zhao |
| [HADOOP-9036](https://issues.apache.org/jira/browse/HADOOP-9036) | TestSinkQueue.testConcurrentConsumers fails intermittently (Backports HADOOP-7292) |  Major | . | Ivan Mitic | Suresh Srinivas |
| [HADOOP-8963](https://issues.apache.org/jira/browse/HADOOP-8963) | CopyFromLocal doesn't always create user directory |  Trivial | . | Billie Rinaldi | Arpit Gupta |
| [HADOOP-8917](https://issues.apache.org/jira/browse/HADOOP-8917) | add LOCALE.US to toLowerCase in SecurityUtil.replacePattern |  Major | . | Arpit Gupta | Arpit Gupta |
| [HADOOP-8900](https://issues.apache.org/jira/browse/HADOOP-8900) | BuiltInGzipDecompressor throws IOException - stored gzip size doesn't match decompressed size |  Major | . | Slavik Krassovsky | Andy Isaacson |
| [HADOOP-8861](https://issues.apache.org/jira/browse/HADOOP-8861) | FSDataOutputStream.sync should call flush() if the underlying wrapped stream is not Syncable |  Major | fs | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-8832](https://issues.apache.org/jira/browse/HADOOP-8832) | backport serviceplugin to branch-1 |  Major | . | Brandon Li | Brandon Li |
| [HADOOP-8819](https://issues.apache.org/jira/browse/HADOOP-8819) | Should use && instead of  & in a few places in FTPFileSystem,FTPInputStream,S3InputStream,ViewFileSystem,ViewFs |  Major | fs | Brandon Li | Brandon Li |
| [HADOOP-8791](https://issues.apache.org/jira/browse/HADOOP-8791) | rm "Only deletes non empty directory and files." |  Major | documentation | Bertrand Dechoux | Jing Zhao |
| [HADOOP-8786](https://issues.apache.org/jira/browse/HADOOP-8786) | HttpServer continues to start even if AuthenticationFilter fails to init |  Major | . | Todd Lipcon | Todd Lipcon |
| [HADOOP-8781](https://issues.apache.org/jira/browse/HADOOP-8781) | hadoop-config.sh should add JAVA\_LIBRARY\_PATH to LD\_LIBRARY\_PATH |  Major | scripts | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-8767](https://issues.apache.org/jira/browse/HADOOP-8767) | secondary namenode on slave machines |  Minor | bin | giovanni delussu | giovanni delussu |
| [HADOOP-8613](https://issues.apache.org/jira/browse/HADOOP-8613) | AbstractDelegationTokenIdentifier#getUser() should set token auth type |  Critical | . | Daryn Sharp | Daryn Sharp |
| [HADOOP-8612](https://issues.apache.org/jira/browse/HADOOP-8612) | Backport HADOOP-8599 to branch-1 (Non empty response when read beyond eof) |  Major | fs | Matt Foley | Eli Collins |
| [HADOOP-8611](https://issues.apache.org/jira/browse/HADOOP-8611) | Allow fall-back to the shell-based implementation when JNI-based users-group mapping fails |  Major | security | Kihwal Lee | Robert Parker |
| [HADOOP-8606](https://issues.apache.org/jira/browse/HADOOP-8606) | FileSystem.get may return the wrong filesystem |  Major | fs | Daryn Sharp | Daryn Sharp |
| [HADOOP-8587](https://issues.apache.org/jira/browse/HADOOP-8587) | HarFileSystem access of harMetaCache isn't threadsafe |  Minor | fs | Eli Collins | Eli Collins |
| [HADOOP-8586](https://issues.apache.org/jira/browse/HADOOP-8586) | Fixup a bunch of SPNEGO misspellings |  Major | . | Eli Collins | Eli Collins |
| [HADOOP-8580](https://issues.apache.org/jira/browse/HADOOP-8580) | ant compile-native fails with automake version 1.11.3 |  Major | . | Eugene Koontz |  |
| [HADOOP-8512](https://issues.apache.org/jira/browse/HADOOP-8512) | AuthenticatedURL should reset the Token when the server returns other than OK on authentication |  Minor | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-8460](https://issues.apache.org/jira/browse/HADOOP-8460) | Document proper setting of HADOOP\_PID\_DIR and HADOOP\_SECURE\_DN\_PID\_DIR |  Major | documentation | Robert Joseph Evans | Robert Joseph Evans |
| [HADOOP-8423](https://issues.apache.org/jira/browse/HADOOP-8423) | MapFile.Reader.get() crashes jvm or throws EOFException on Snappy or LZO block-compressed data |  Major | io | Jason B | Todd Lipcon |
| [HADOOP-8386](https://issues.apache.org/jira/browse/HADOOP-8386) | hadoop script doesn't work if 'cd' prints to stdout (default behavior in Ubuntu) |  Major | scripts | Christopher Berner | Christopher Berner |
| [HADOOP-8355](https://issues.apache.org/jira/browse/HADOOP-8355) | SPNEGO filter throws/logs exception when authentication fails |  Minor | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-8249](https://issues.apache.org/jira/browse/HADOOP-8249) | invalid hadoop-auth cookies should trigger authentication if info is avail before returning HTTP 401 |  Major | security | bc Wong | Alejandro Abdelnur |
| [HADOOP-7868](https://issues.apache.org/jira/browse/HADOOP-7868) | Hadoop native fails to compile when default linker option is -Wl,--as-needed |  Major | native | James Page | Trevor Robinson |
| [HADOOP-7836](https://issues.apache.org/jira/browse/HADOOP-7836) | TestSaslRPC#testDigestAuthMethodHostBasedToken fails with hostname localhost.localdomain |  Minor | ipc, test | Eli Collins | Daryn Sharp |
| [HADOOP-7827](https://issues.apache.org/jira/browse/HADOOP-7827) | jsp pages missing DOCTYPE |  Trivial | . | Dave Vronay | Dave Vronay |
| [HADOOP-7698](https://issues.apache.org/jira/browse/HADOOP-7698) | jsvc target fails on x86\_64 |  Critical | build | Daryn Sharp | Daryn Sharp |
| [HADOOP-7101](https://issues.apache.org/jira/browse/HADOOP-7101) | UserGroupInformation.getCurrentUser() fails when called from non-Hadoop JAAS context |  Blocker | security | Todd Lipcon | Todd Lipcon |
| [HADOOP-6496](https://issues.apache.org/jira/browse/HADOOP-6496) | HttpServer sends wrong content-type for CSS files (and others) |  Minor | . | Lars Francke | Ivan Mitic |
| [HDFS-4715](https://issues.apache.org/jira/browse/HDFS-4715) | Backport HDFS-3577 and other related WebHDFS issue to branch-1 |  Major | webhdfs | Tsz Wo Nicholas Sze | Mark Wagner |
| [HDFS-4558](https://issues.apache.org/jira/browse/HDFS-4558) | start balancer failed with NPE |  Critical | balancer & mover | Wenwu Peng | Junping Du |
| [HDFS-4544](https://issues.apache.org/jira/browse/HDFS-4544) | Error in deleting blocks should not do check disk, for all types of errors |  Major | . | Amareshwari Sriramadasu | Arpit Agarwal |
| [HDFS-4519](https://issues.apache.org/jira/browse/HDFS-4519) | Support override of jsvc binary and log file locations when launching secure datanode. |  Major | datanode, scripts | Chris Nauroth | Chris Nauroth |
| [HDFS-4518](https://issues.apache.org/jira/browse/HDFS-4518) | Finer grained metrics for HDFS capacity |  Major | . | Arpit Agarwal | Arpit Agarwal |
| [HDFS-4479](https://issues.apache.org/jira/browse/HDFS-4479) | logSync() with the FSNamesystem lock held in commitBlockSynchronization |  Major | . | Jing Zhao | Jing Zhao |
| [HDFS-4466](https://issues.apache.org/jira/browse/HDFS-4466) | Remove the deadlock from AbstractDelegationTokenSecretManager |  Major | namenode, security | Brandon Li | Brandon Li |
| [HDFS-4444](https://issues.apache.org/jira/browse/HDFS-4444) | Add space between total transaction time and number of transactions in FSEditLog#printStatistics |  Trivial | . | Stephen Chu | Stephen Chu |
| [HDFS-4413](https://issues.apache.org/jira/browse/HDFS-4413) | Secondary namenode won't start if HDFS isn't the default file system |  Major | namenode | Mostafa Elhemali | Mostafa Elhemali |
| [HDFS-4358](https://issues.apache.org/jira/browse/HDFS-4358) | TestCheckpoint failure with JDK7 |  Major | test | Arpit Agarwal | Arpit Agarwal |
| [HDFS-4355](https://issues.apache.org/jira/browse/HDFS-4355) | TestNameNodeMetrics.testCorruptBlock fails with open JDK7 |  Major | test | Brandon Li | Brandon Li |
| [HDFS-4351](https://issues.apache.org/jira/browse/HDFS-4351) | Fix BlockPlacementPolicyDefault#chooseTarget when avoiding stale nodes |  Major | namenode | Andrew Wang | Andrew Wang |
| [HDFS-4337](https://issues.apache.org/jira/browse/HDFS-4337) | Backport HDFS-4240 to branch-1: Make sure nodes are avoided to place replica if some replica are already under the same nodegroup. |  Major | namenode | Junping Du | meng gong |
| [HDFS-4240](https://issues.apache.org/jira/browse/HDFS-4240) | In nodegroup-aware case, make sure nodes are avoided to place replica if some replica are already under the same nodegroup |  Major | namenode | Junping Du | Junping Du |
| [HDFS-4222](https://issues.apache.org/jira/browse/HDFS-4222) | NN is unresponsive and loses heartbeats of DNs when Hadoop is configured to use LDAP and LDAP has issues |  Minor | namenode | Xiaobo Peng | Xiaobo Peng |
| [HDFS-4207](https://issues.apache.org/jira/browse/HDFS-4207) | All hadoop fs operations fail if the default fs is down even if a different file system is specified in the command |  Minor | hdfs-client | Steve Loughran | Jing Zhao |
| [HDFS-4180](https://issues.apache.org/jira/browse/HDFS-4180) | TestFileCreation fails in branch-1 but not branch-1.1 |  Minor | test | Tsz Wo Nicholas Sze | Jing Zhao |
| [HDFS-4168](https://issues.apache.org/jira/browse/HDFS-4168) | TestDFSUpgradeFromImage fails in branch-1 |  Major | namenode | Tsz Wo Nicholas Sze | Jing Zhao |
| [HDFS-4072](https://issues.apache.org/jira/browse/HDFS-4072) | On file deletion remove corresponding blocks pending replication |  Minor | namenode | Jing Zhao | Jing Zhao |
| [HDFS-3963](https://issues.apache.org/jira/browse/HDFS-3963) | backport namenode/datanode serviceplugin to branch-1 |  Major | . | Brandon Li | Brandon Li |
| [HDFS-3961](https://issues.apache.org/jira/browse/HDFS-3961) | FSEditLog preallocate() needs to reset the position of PREALLOCATE\_BUFFER when more than 1MB size is needed |  Major | . | Jing Zhao | Jing Zhao |
| [HDFS-3754](https://issues.apache.org/jira/browse/HDFS-3754) | BlockSender doesn't shutdown ReadaheadPool threads |  Major | datanode | Eli Collins | Eli Collins |
| [HDFS-3698](https://issues.apache.org/jira/browse/HDFS-3698) | TestHftpFileSystem is failing in branch-1 due to changed default secure port |  Major | security | Aaron T. Myers | Aaron T. Myers |
| [HDFS-3679](https://issues.apache.org/jira/browse/HDFS-3679) | fuse\_dfs notrash option sets usetrash |  Minor | fuse-dfs | Conrad Meyer | Conrad Meyer |
| [HDFS-3628](https://issues.apache.org/jira/browse/HDFS-3628) | The dfsadmin -setBalancerBandwidth command on branch-1 does not check for superuser privileges |  Blocker | datanode, namenode | Harsh J | Harsh J |
| [HDFS-3595](https://issues.apache.org/jira/browse/HDFS-3595) | TestEditLogLoading fails in branch-1 |  Major | namenode | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3540](https://issues.apache.org/jira/browse/HDFS-3540) | Further improvement on recovery mode and edit log toleration in branch-1 |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-3499](https://issues.apache.org/jira/browse/HDFS-3499) | Make NetworkTopology support user specified topology class |  Major | datanode | Junping Du | Junping Du |
| [HDFS-3402](https://issues.apache.org/jira/browse/HDFS-3402) | Fix hdfs scripts for secure datanodes |  Minor | scripts, security | Benoy Antony | Benoy Antony |
| [HDFS-2827](https://issues.apache.org/jira/browse/HDFS-2827) | Cannot save namespace after renaming a directory above a file with an open lease |  Major | namenode | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-2757](https://issues.apache.org/jira/browse/HDFS-2757) | Cannot read a local block that's being written to when using the local read short circuit |  Major | . | Jean-Daniel Cryans | Jean-Daniel Cryans |
| [MAPREDUCE-5202](https://issues.apache.org/jira/browse/MAPREDUCE-5202) | Revert MAPREDUCE-4397 to avoid using incorrect config files |  Major | . | Owen O'Malley | Owen O'Malley |
| [MAPREDUCE-5198](https://issues.apache.org/jira/browse/MAPREDUCE-5198) | Race condition in cleanup during task tracker renint with LinuxTaskController |  Major | tasktracker | Arpit Gupta | Arpit Gupta |
| [MAPREDUCE-5169](https://issues.apache.org/jira/browse/MAPREDUCE-5169) | Job recovery fails if job tracker is restarted after the job is submitted but before its initialized |  Major | . | Arpit Gupta | Arun C Murthy |
| [MAPREDUCE-5166](https://issues.apache.org/jira/browse/MAPREDUCE-5166) | ConcurrentModificationException in LocalJobRunner |  Blocker | . | Gunther Hagleitner | Sandy Ryza |
| [MAPREDUCE-5158](https://issues.apache.org/jira/browse/MAPREDUCE-5158) | Cleanup required when mapreduce.job.restart.recover is set to false |  Major | jobtracker | Yesha Vora | Mayank Bansal |
| [MAPREDUCE-5154](https://issues.apache.org/jira/browse/MAPREDUCE-5154) | staging directory deletion fails because delegation tokens have been cancelled |  Major | jobtracker | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-5131](https://issues.apache.org/jira/browse/MAPREDUCE-5131) | Provide better handling of job status related apis during JT restart |  Major | . | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-5066](https://issues.apache.org/jira/browse/MAPREDUCE-5066) | JobTracker should set a timeout when calling into job.end.notification.url |  Major | . | Ivan Mitic | Ivan Mitic |
| [MAPREDUCE-5049](https://issues.apache.org/jira/browse/MAPREDUCE-5049) | CombineFileInputFormat counts all compressed files non-splitable |  Major | . | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-5035](https://issues.apache.org/jira/browse/MAPREDUCE-5035) | Update MR1 memory configuration docs |  Major | mrv1 | Tom White | Tom White |
| [MAPREDUCE-5028](https://issues.apache.org/jira/browse/MAPREDUCE-5028) | Maps fail when io.sort.mb is set to high value |  Critical | . | Karthik Kambatla | Karthik Kambatla |
| [MAPREDUCE-5008](https://issues.apache.org/jira/browse/MAPREDUCE-5008) | Merger progress miscounts with respect to EOF\_MARKER |  Major | . | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-4970](https://issues.apache.org/jira/browse/MAPREDUCE-4970) | Child tasks (try to) create security audit log files |  Major | . | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-4969](https://issues.apache.org/jira/browse/MAPREDUCE-4969) | TestKeyValueTextInputFormat test fails with Open JDK 7 |  Major | test | Arpit Agarwal | Arpit Agarwal |
| [MAPREDUCE-4967](https://issues.apache.org/jira/browse/MAPREDUCE-4967) | TestJvmReuse fails on assertion |  Major | tasktracker, test | Chris Nauroth | Karthik Kambatla |
| [MAPREDUCE-4963](https://issues.apache.org/jira/browse/MAPREDUCE-4963) | StatisticsCollector improperly keeps track of "Last Day" and "Last Hour" statistics for new TaskTrackers |  Major | mrv1 | Robert Kanter | Robert Kanter |
| [MAPREDUCE-4962](https://issues.apache.org/jira/browse/MAPREDUCE-4962) | jobdetails.jsp uses display name instead of real name to get counters |  Major | jobtracker, mrv1 | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-4933](https://issues.apache.org/jira/browse/MAPREDUCE-4933) | MR1 final merge asks for length of file it just wrote before flushing it |  Major | mrv1, task | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-4930](https://issues.apache.org/jira/browse/MAPREDUCE-4930) | Backport MAPREDUCE-4678 and MAPREDUCE-4925 to branch-1 |  Major | examples | Karthik Kambatla | Karthik Kambatla |
| [MAPREDUCE-4929](https://issues.apache.org/jira/browse/MAPREDUCE-4929) | mapreduce.task.timeout is ignored |  Major | mrv1 | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-4924](https://issues.apache.org/jira/browse/MAPREDUCE-4924) | flakey test: org.apache.hadoop.mapred.TestClusterMRNotification.testMR |  Trivial | mrv1 | Robert Kanter | Robert Kanter |
| [MAPREDUCE-4923](https://issues.apache.org/jira/browse/MAPREDUCE-4923) | Add toString method to TaggedInputSplit |  Minor | mrv1, mrv2, task | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-4916](https://issues.apache.org/jira/browse/MAPREDUCE-4916) | TestTrackerDistributedCacheManager is flaky due to other badly written tests in branch-1 |  Major | . | Arun C Murthy | Xuan Gong |
| [MAPREDUCE-4915](https://issues.apache.org/jira/browse/MAPREDUCE-4915) | TestShuffleExceptionCount fails with open JDK7 |  Major | test | Brandon Li | Brandon Li |
| [MAPREDUCE-4914](https://issues.apache.org/jira/browse/MAPREDUCE-4914) | TestMiniMRDFSSort fails with openJDK7 |  Major | test | Brandon Li | Brandon Li |
| [MAPREDUCE-4909](https://issues.apache.org/jira/browse/MAPREDUCE-4909) | TestKeyValueTextInputFormat fails with Open JDK 7 on Windows |  Major | test | Arpit Agarwal | Arpit Agarwal |
| [MAPREDUCE-4904](https://issues.apache.org/jira/browse/MAPREDUCE-4904) | TestMultipleLevelCaching failed in branch-1 |  Major | test | meng gong | Junping Du |
| [MAPREDUCE-4860](https://issues.apache.org/jira/browse/MAPREDUCE-4860) | DelegationTokenRenewal attempts to renew token even after a job is removed |  Major | security | Karthik Kambatla | Karthik Kambatla |
| [MAPREDUCE-4850](https://issues.apache.org/jira/browse/MAPREDUCE-4850) | Job recovery may fail if staging directory has been deleted |  Major | mrv1 | Tom White | Tom White |
| [MAPREDUCE-4843](https://issues.apache.org/jira/browse/MAPREDUCE-4843) | When using DefaultTaskController, JobLocalizer not thread safe |  Critical | tasktracker | zhaoyunjiong | Karthik Kambatla |
| [MAPREDUCE-4806](https://issues.apache.org/jira/browse/MAPREDUCE-4806) | Cleanup: Some (5) private methods in JobTracker.RecoveryManager are not used anymore after MAPREDUCE-3837 |  Major | mrv1 | Karthik Kambatla | Karthik Kambatla |
| [MAPREDUCE-4778](https://issues.apache.org/jira/browse/MAPREDUCE-4778) | Fair scheduler event log is only written if directory exists on HDFS |  Major | jobtracker, scheduler | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-4765](https://issues.apache.org/jira/browse/MAPREDUCE-4765) | Restarting the JobTracker programmatically can cause DelegationTokenRenewal to throw an exception |  Minor | jobtracker, mrv1 | Robert Kanter | Robert Kanter |
| [MAPREDUCE-4706](https://issues.apache.org/jira/browse/MAPREDUCE-4706) | FairScheduler#dump(): Computing of # running maps and reduces is commented out |  Critical | contrib/fair-share | Karthik Kambatla | Karthik Kambatla |
| [MAPREDUCE-4662](https://issues.apache.org/jira/browse/MAPREDUCE-4662) | JobHistoryFilesManager thread pool never expands |  Major | jobhistoryserver | Thomas Graves | Kihwal Lee |
| [MAPREDUCE-4652](https://issues.apache.org/jira/browse/MAPREDUCE-4652) | ValueAggregatorJob sets the wrong job jar |  Major | examples, mrv1 | Ahmed Radwan | Ahmed Radwan |
| [MAPREDUCE-4643](https://issues.apache.org/jira/browse/MAPREDUCE-4643) | Make job-history cleanup-period configurable |  Major | jobhistoryserver | Karthik Kambatla | Sandy Ryza |
| [MAPREDUCE-4595](https://issues.apache.org/jira/browse/MAPREDUCE-4595) | TestLostTracker failing - possibly due to a race in JobHistory.JobHistoryFilesManager#run() |  Critical | . | Karthik Kambatla | Karthik Kambatla |
| [MAPREDUCE-4576](https://issues.apache.org/jira/browse/MAPREDUCE-4576) | Large dist cache can block tasktracker heartbeat |  Major | . | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-4572](https://issues.apache.org/jira/browse/MAPREDUCE-4572) | Can not access user logs - Jetty is not configured by default to serve aliases/symlinks |  Major | tasktracker, webapps | Ahmed Radwan | Ahmed Radwan |
| [MAPREDUCE-4463](https://issues.apache.org/jira/browse/MAPREDUCE-4463) | JobTracker recovery fails with HDFS permission issue |  Blocker | mrv1 | Tom White | Tom White |
| [MAPREDUCE-4451](https://issues.apache.org/jira/browse/MAPREDUCE-4451) | fairscheduler fail to init job with kerberos authentication configured |  Major | contrib/fair-share | Erik.fang | Erik.fang |
| [MAPREDUCE-4434](https://issues.apache.org/jira/browse/MAPREDUCE-4434) | Backport MR-2779 (JobSplitWriter.java can't handle large job.split file) to branch-1 |  Major | mrv1 | Karthik Kambatla | Karthik Kambatla |
| [MAPREDUCE-4385](https://issues.apache.org/jira/browse/MAPREDUCE-4385) | FairScheduler.maxTasksToAssign() should check for fairscheduler.assignmultiple.maps \< TaskTracker.availableSlots |  Major | . | Karthik Kambatla | Karthik Kambatla |
| [MAPREDUCE-4359](https://issues.apache.org/jira/browse/MAPREDUCE-4359) | Potential deadlock in Counters |  Major | . | Todd Lipcon | Tom White |
| [MAPREDUCE-4317](https://issues.apache.org/jira/browse/MAPREDUCE-4317) | Job view ACL checks are too permissive |  Major | mrv1 | Harsh J | Karthik Kambatla |
| [MAPREDUCE-4315](https://issues.apache.org/jira/browse/MAPREDUCE-4315) | jobhistory.jsp throws 500 when a .txt file is found in /done |  Major | jobhistoryserver | Alexander Alten-Lorenz | Sandy Ryza |
| [MAPREDUCE-4278](https://issues.apache.org/jira/browse/MAPREDUCE-4278) | cannot run two local jobs in parallel from the same gateway. |  Major | . | Araceli Henley | Sandy Ryza |
| [MAPREDUCE-4195](https://issues.apache.org/jira/browse/MAPREDUCE-4195) | With invalid queueName request param, jobqueue\_details.jsp shows NPE |  Critical | jobtracker | Gera Shegalov |  |
| [MAPREDUCE-4036](https://issues.apache.org/jira/browse/MAPREDUCE-4036) | Streaming TestUlimit fails on CentOS 6 |  Major | test | Alejandro Abdelnur | Alejandro Abdelnur |
| [MAPREDUCE-3993](https://issues.apache.org/jira/browse/MAPREDUCE-3993) | Graceful handling of codec errors during decompression |  Major | mrv1, mrv2 | Todd Lipcon | Karthik Kambatla |
| [MAPREDUCE-3727](https://issues.apache.org/jira/browse/MAPREDUCE-3727) | jobtoken location property in jobconf refers to wrong jobtoken file |  Critical | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [MAPREDUCE-2289](https://issues.apache.org/jira/browse/MAPREDUCE-2289) | Permissions race can make getStagingDir fail on local filesystem |  Major | job submission | Todd Lipcon | Ahmed Radwan |
| [MAPREDUCE-2264](https://issues.apache.org/jira/browse/MAPREDUCE-2264) | Job status exceeds 100% in some cases |  Major | jobtracker | Adam Kramer | Devaraj K |
| [MAPREDUCE-2217](https://issues.apache.org/jira/browse/MAPREDUCE-2217) | The expire launching task should cover the UNASSIGNED task |  Major | jobtracker | Scott Chen | Karthik Kambatla |
| [MAPREDUCE-1806](https://issues.apache.org/jira/browse/MAPREDUCE-1806) | CombineFileInputFormat does not work with paths not on default FS |  Major | harchive | Paul Yang | Gera Shegalov |
| [MAPREDUCE-1684](https://issues.apache.org/jira/browse/MAPREDUCE-1684) | ClusterStatus can be cached in CapacityTaskScheduler.assignTasks() |  Major | capacity-sched | Amareshwari Sriramadasu | Koji Noguchi |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-9175](https://issues.apache.org/jira/browse/HADOOP-9175) | TestWritableName fails with Open JDK 7 |  Major | test | Arpit Agarwal | Arpit Agarwal |
| [HADOOP-9174](https://issues.apache.org/jira/browse/HADOOP-9174) | TestSecurityUtil fails on Open JDK 7 |  Major | test | Arpit Agarwal | Arpit Agarwal |
| [HDFS-4256](https://issues.apache.org/jira/browse/HDFS-4256) | Backport concatenation of files into a single file to branch-1 |  Major | namenode | Suresh Srinivas | Sanjay Radia |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-8817](https://issues.apache.org/jira/browse/HADOOP-8817) | Backport Network Topology Extension for Virtualization (HADOOP-8468) to branch-1 |  Major | . | Junping Du | Junping Du |
| [HADOOP-8470](https://issues.apache.org/jira/browse/HADOOP-8470) | Implementation of 4-layer subclass of NetworkTopology (NetworkTopologyWithNodeGroup) |  Major | . | Junping Du | Junping Du |
| [HADOOP-8469](https://issues.apache.org/jira/browse/HADOOP-8469) | Make NetworkTopology class pluggable |  Major | . | Junping Du | Junping Du |
| [HADOOP-7754](https://issues.apache.org/jira/browse/HADOOP-7754) | Expose file descriptors from Hadoop-wrapped local FileSystems |  Major | native, performance | Todd Lipcon | Todd Lipcon |
| [HDFS-4071](https://issues.apache.org/jira/browse/HDFS-4071) | Add number of stale DataNodes to metrics for Branch-1 |  Minor | datanode, namenode | Jing Zhao | Jing Zhao |
| [HDFS-3912](https://issues.apache.org/jira/browse/HDFS-3912) | Detecting and avoiding stale datanodes for writing |  Major | . | Jing Zhao | Jing Zhao |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


