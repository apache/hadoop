
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

## Release 0.23.0 - 2011-11-11

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-1526](https://issues.apache.org/jira/browse/HDFS-1526) | Dfs client name for a map/reduce task should have some randomness |  Major | hdfs-client | Hairong Kuang | Hairong Kuang |
| [HDFS-1560](https://issues.apache.org/jira/browse/HDFS-1560) | dfs.data.dir permissions should default to 700 |  Minor | datanode | Todd Lipcon | Todd Lipcon |
| [HDFS-1536](https://issues.apache.org/jira/browse/HDFS-1536) | Improve HDFS WebUI |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-6864](https://issues.apache.org/jira/browse/HADOOP-6864) | Provide a JNI-based implementation of ShellBasedUnixGroupsNetgroupMapping (implementation of GroupMappingServiceProvider) |  Major | security | Erik Steffl | Boris Shkolnik |
| [HADOOP-6904](https://issues.apache.org/jira/browse/HADOOP-6904) | A baby step towards inter-version RPC communications |  Major | ipc | Hairong Kuang | Hairong Kuang |
| [HADOOP-6432](https://issues.apache.org/jira/browse/HADOOP-6432) | Statistics support in FileContext |  Major | . | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [HADOOP-7136](https://issues.apache.org/jira/browse/HADOOP-7136) | Remove failmon contrib |  Major | . | Nigel Daley | Nigel Daley |
| [HADOOP-7153](https://issues.apache.org/jira/browse/HADOOP-7153) | MapWritable violates contract of Map interface for equals() and hashCode() |  Minor | io | Nicholas Telford | Nicholas Telford |
| [HDFS-1703](https://issues.apache.org/jira/browse/HDFS-1703) | HDFS federation: Improve start/stop scripts and add script to decommission datanodes |  Minor | scripts | Tanping Wang | Tanping Wang |
| [HDFS-1675](https://issues.apache.org/jira/browse/HDFS-1675) | Transfer RBW between datanodes |  Major | datanode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-6949](https://issues.apache.org/jira/browse/HADOOP-6949) | Reduces RPC packet size for primitive arrays, especially long[], which is used at block reporting |  Major | io | Navis | Matt Foley |
| [HDFS-1761](https://issues.apache.org/jira/browse/HDFS-1761) | Add a new DataTransferProtocol operation, Op.TRANSFER\_BLOCK, instead of using RPC |  Major | datanode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-1606](https://issues.apache.org/jira/browse/HDFS-1606) | Provide a stronger data guarantee in the write pipeline |  Major | datanode, hdfs-client, namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-7227](https://issues.apache.org/jira/browse/HADOOP-7227) | Remove protocol version check at proxy creation in Hadoop RPC. |  Major | ipc | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [HADOOP-6920](https://issues.apache.org/jira/browse/HADOOP-6920) | Metrics2: metrics instrumentation |  Major | . | Luke Lu | Luke Lu |
| [HADOOP-6921](https://issues.apache.org/jira/browse/HADOOP-6921) | metrics2: metrics plugins |  Major | . | Luke Lu | Luke Lu |
| [HDFS-1117](https://issues.apache.org/jira/browse/HDFS-1117) | HDFS portion of HADOOP-6728 (ovehaul metrics framework) |  Major | . | Luke Lu | Luke Lu |
| [HDFS-1945](https://issues.apache.org/jira/browse/HDFS-1945) | Removed deprecated fields in DataTransferProtocol |  Major | datanode, hdfs-client | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-7286](https://issues.apache.org/jira/browse/HADOOP-7286) | Refactor FsShell's du/dus/df |  Major | fs | Daryn Sharp | Daryn Sharp |
| [HADOOP-6255](https://issues.apache.org/jira/browse/HADOOP-6255) | Create an rpm integration project |  Major | . | Owen O'Malley | Eric Yang |
| [HDFS-1963](https://issues.apache.org/jira/browse/HDFS-1963) | HDFS rpm integration project |  Major | build | Eric Yang | Eric Yang |
| [MAPREDUCE-2455](https://issues.apache.org/jira/browse/MAPREDUCE-2455) | Remove deprecated JobTracker.State in favour of JobTrackerStatus |  Major | build, client | Tom White | Tom White |
| [HDFS-1966](https://issues.apache.org/jira/browse/HDFS-1966) | Encapsulate individual DataTransferProtocol op header |  Major | datanode, hdfs-client | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-7331](https://issues.apache.org/jira/browse/HADOOP-7331) | Make hadoop-daemon.sh to return 1 if daemon processes did not get started |  Trivial | scripts | Tanping Wang | Tanping Wang |
| [HDFS-2058](https://issues.apache.org/jira/browse/HDFS-2058) | DataTransfer Protocol using protobufs |  Major | . | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2430](https://issues.apache.org/jira/browse/MAPREDUCE-2430) | Remove mrunit contrib |  Major | . | Nigel Daley | Nigel Daley |
| [HADOOP-7374](https://issues.apache.org/jira/browse/HADOOP-7374) | Don't add tools.jar to the classpath when running Hadoop |  Major | scripts | Eli Collins | Eli Collins |
| [HDFS-2066](https://issues.apache.org/jira/browse/HDFS-2066) | Create a package and individual class files for DataTransferProtocol |  Major | datanode, hdfs-client, namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-2087](https://issues.apache.org/jira/browse/HDFS-2087) | Add methods to DataTransferProtocol interface |  Major | datanode, hdfs-client | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-1723](https://issues.apache.org/jira/browse/HDFS-1723) | quota errors messages should use the same scale |  Minor | . | Allen Wittenauer | Jim Plush |
| [HDFS-2107](https://issues.apache.org/jira/browse/HDFS-2107) | Move block management code to a package |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [MAPREDUCE-2606](https://issues.apache.org/jira/browse/MAPREDUCE-2606) | Remove IsolationRunner |  Major | . | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-2081](https://issues.apache.org/jira/browse/HADOOP-2081) | Configuration getInt, getLong, and getFloat replace invalid numbers with the default value |  Major | conf | Owen O'Malley | Harsh J |
| [HDFS-2210](https://issues.apache.org/jira/browse/HDFS-2210) | Remove hdfsproxy |  Major | contrib/hdfsproxy | Eli Collins | Eli Collins |
| [HDFS-1073](https://issues.apache.org/jira/browse/HDFS-1073) | Simpler model for Namenode's fs Image and edit Logs |  Major | . | Sanjay Radia | Todd Lipcon |
| [HDFS-2202](https://issues.apache.org/jira/browse/HDFS-2202) | Changes to balancer bandwidth should not require datanode restart. |  Major | balancer & mover, datanode | Eric Payne | Eric Payne |
| [MAPREDUCE-1738](https://issues.apache.org/jira/browse/MAPREDUCE-1738) | MapReduce portion of HADOOP-6728 (ovehaul metrics framework) |  Major | . | Luke Lu | Luke Lu |
| [HADOOP-7264](https://issues.apache.org/jira/browse/HADOOP-7264) | Bump avro version to at least 1.4.1 |  Major | io | Luke Lu | Luke Lu |
| [HADOOP-7547](https://issues.apache.org/jira/browse/HADOOP-7547) | Fix the warning in writable classes.[ WritableComparable is a raw type. References to generic type WritableComparable\<T\> should be parameterized  ] |  Minor | io | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-7507](https://issues.apache.org/jira/browse/HADOOP-7507) | jvm metrics all use the same namespace |  Major | metrics | Jeff Bean | Alejandro Abdelnur |
| [MAPREDUCE-3041](https://issues.apache.org/jira/browse/MAPREDUCE-3041) | Enhance YARN Client-RM protocol to provide access to information such as cluster's Min/Max Resource capabilities similar to that of AM-RM protocol |  Blocker | mrv2 | Hitesh Shah | Hitesh Shah |
| [MAPREDUCE-3205](https://issues.apache.org/jira/browse/MAPREDUCE-3205) | MR2 memory limits should be pmem, not vmem |  Blocker | mrv2, nodemanager | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2736](https://issues.apache.org/jira/browse/MAPREDUCE-2736) | Remove unused contrib components dependent on MR1 |  Major | jobtracker, tasktracker | Eli Collins | Eli Collins |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-1448](https://issues.apache.org/jira/browse/HDFS-1448) | Create multi-format parser for edits logs file, support binary and XML formats initially |  Major | tools | Erik Steffl | Erik Steffl |
| [MAPREDUCE-2438](https://issues.apache.org/jira/browse/MAPREDUCE-2438) | MR-279: WebApp for Job History |  Major | mrv2 | Mahadev konar | Krishna Ramachandran |
| [HDFS-1751](https://issues.apache.org/jira/browse/HDFS-1751) | Intrinsic limits for HDFS files, directories |  Major | datanode | Daryn Sharp | Daryn Sharp |
| [HDFS-1052](https://issues.apache.org/jira/browse/HDFS-1052) | HDFS scalability with multiple namenodes |  Major | namenode | Suresh Srinivas | Suresh Srinivas |
| [MAPREDUCE-2434](https://issues.apache.org/jira/browse/MAPREDUCE-2434) | MR-279: ResourceManager metrics |  Major | mrv2 | Luke Lu | Luke Lu |
| [HDFS-1873](https://issues.apache.org/jira/browse/HDFS-1873) | Federation Cluster Management Web Console |  Major | . | Tanping Wang | Tanping Wang |
| [HADOOP-7257](https://issues.apache.org/jira/browse/HADOOP-7257) | A client side mount table to give per-application/per-job file system view |  Major | . | Sanjay Radia | Sanjay Radia |
| [HADOOP-7214](https://issues.apache.org/jira/browse/HADOOP-7214) | Hadoop /usr/bin/groups equivalent |  Major | . | Aaron T. Myers | Aaron T. Myers |
| [HDFS-1814](https://issues.apache.org/jira/browse/HDFS-1814) | HDFS portion of HADOOP-7214 - Hadoop /usr/bin/groups equivalent |  Major | hdfs-client, namenode | Aaron T. Myers | Aaron T. Myers |
| [MAPREDUCE-2473](https://issues.apache.org/jira/browse/MAPREDUCE-2473) | MR portion of HADOOP-7214 - Hadoop /usr/bin/groups equivalent |  Major | jobtracker | Aaron T. Myers | Aaron T. Myers |
| [MAPREDUCE-461](https://issues.apache.org/jira/browse/MAPREDUCE-461) | Enable ServicePlugins for the JobTracker |  Minor | . | Fredrik Hedberg | Fredrik Hedberg |
| [MAPREDUCE-2407](https://issues.apache.org/jira/browse/MAPREDUCE-2407) | Make Gridmix emulate usage of Distributed Cache files |  Major | contrib/gridmix | Ravi Gummadi | Ravi Gummadi |
| [MAPREDUCE-2533](https://issues.apache.org/jira/browse/MAPREDUCE-2533) | MR-279: Metrics for reserved resource in ResourceManager |  Major | mrv2 | Luke Lu | Luke Lu |
| [MAPREDUCE-2527](https://issues.apache.org/jira/browse/MAPREDUCE-2527) | MR-279: Metrics for MRAppMaster |  Major | mrv2 | Luke Lu | Luke Lu |
| [MAPREDUCE-2532](https://issues.apache.org/jira/browse/MAPREDUCE-2532) | MR-279: Metrics for NodeManager |  Major | mrv2 | Luke Lu | Luke Lu |
| [MAPREDUCE-2408](https://issues.apache.org/jira/browse/MAPREDUCE-2408) | Make Gridmix emulate usage of data compression |  Major | contrib/gridmix | Ravi Gummadi | Amar Kamat |
| [MAPREDUCE-2521](https://issues.apache.org/jira/browse/MAPREDUCE-2521) | Mapreduce RPM integration project |  Major | build | Eric Yang | Eric Yang |
| [MAPREDUCE-2543](https://issues.apache.org/jira/browse/MAPREDUCE-2543) | [Gridmix] Add support for HighRam jobs |  Major | contrib/gridmix | Amar Kamat | Amar Kamat |
| [HADOOP-7144](https://issues.apache.org/jira/browse/HADOOP-7144) | Expose JMX with something like JMXProxyServlet |  Major | . | Luke Lu | Robert Joseph Evans |
| [HDFS-2055](https://issues.apache.org/jira/browse/HDFS-2055) | Add hflush support to libhdfs |  Major | libhdfs | Travis Crawford | Travis Crawford |
| [HDFS-2083](https://issues.apache.org/jira/browse/HDFS-2083) | Adopt JMXJsonServlet into HDFS in order to query statistics |  Major | . | Tanping Wang | Tanping Wang |
| [HADOOP-7206](https://issues.apache.org/jira/browse/HADOOP-7206) | Integrate Snappy compression |  Major | . | Eli Collins | Alejandro Abdelnur |
| [MAPREDUCE-2323](https://issues.apache.org/jira/browse/MAPREDUCE-2323) | Add metrics to the fair scheduler |  Major | contrib/fair-share | Todd Lipcon | Todd Lipcon |
| [HADOOP-7443](https://issues.apache.org/jira/browse/HADOOP-7443) | Add CRC32C as another DataChecksum implementation |  Major | io, util | Todd Lipcon | Todd Lipcon |
| [HADOOP-6385](https://issues.apache.org/jira/browse/HADOOP-6385) | dfs does not support -rmdir (was HDFS-639) |  Minor | fs | Scott Phillips | Daryn Sharp |
| [MAPREDUCE-2037](https://issues.apache.org/jira/browse/MAPREDUCE-2037) | Capturing interim progress times, CPU usage, and memory usage, when tasks reach certain progress thresholds |  Major | . | Dick King | Dick King |
| [HADOOP-7493](https://issues.apache.org/jira/browse/HADOOP-7493) | [HDFS-362] Provide ShortWritable class in hadoop. |  Major | io | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-1330](https://issues.apache.org/jira/browse/HDFS-1330) | Make RPCs to DataNodes timeout |  Major | datanode | Hairong Kuang | John George |
| [HADOOP-7594](https://issues.apache.org/jira/browse/HADOOP-7594) | Support HTTP REST in HttpServer |  Major | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-7119](https://issues.apache.org/jira/browse/HADOOP-7119) | add Kerberos HTTP SPNEGO authentication support to Hadoop JT/NN/DN/TT web-consoles |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [MAPREDUCE-2719](https://issues.apache.org/jira/browse/MAPREDUCE-2719) | MR-279: Write a shell command application |  Major | mrv2 | Sharad Agarwal | Hitesh Shah |
| [HADOOP-6889](https://issues.apache.org/jira/browse/HADOOP-6889) | Make RPC to have an option to timeout |  Major | ipc | Hairong Kuang | John George |
| [HADOOP-7705](https://issues.apache.org/jira/browse/HADOOP-7705) | Add a log4j back end that can push out JSON data, one per line |  Minor | util | Steve Loughran | Steve Loughran |
| [HDFS-2471](https://issues.apache.org/jira/browse/HDFS-2471) | Add Federation feature, configuration and tools documentation |  Major | documentation | Suresh Srinivas | Suresh Srinivas |
| [MAPREDUCE-1938](https://issues.apache.org/jira/browse/MAPREDUCE-1938) | Ability for having user's classes take precedence over the system classes for tasks' classpath |  Blocker | job submission, task, tasktracker | Devaraj Das | Krishna Ramachandran |
| [MAPREDUCE-2692](https://issues.apache.org/jira/browse/MAPREDUCE-2692) | Ensure AM Restart and Recovery-on-restart is complete |  Major | mrv2 | Amol Kekre | Sharad Agarwal |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-7042](https://issues.apache.org/jira/browse/HADOOP-7042) | Update test-patch.sh to include failed test names and move test-patch.properties |  Minor | test | Nigel Daley | Nigel Daley |
| [HDFS-1510](https://issues.apache.org/jira/browse/HDFS-1510) | Add test-patch.properties required by test-patch.sh |  Minor | . | Nigel Daley | Nigel Daley |
| [HDFS-1513](https://issues.apache.org/jira/browse/HDFS-1513) | Fix a number of warnings |  Minor | . | Eli Collins | Eli Collins |
| [HDFS-1481](https://issues.apache.org/jira/browse/HDFS-1481) | NameNode should validate fsimage before rolling |  Major | namenode | Hairong Kuang | Hairong Kuang |
| [HADOOP-7023](https://issues.apache.org/jira/browse/HADOOP-7023) | Add listCorruptFileBlocks to FileSystem |  Major | . | Patrick Kling | Patrick Kling |
| [HDFS-1458](https://issues.apache.org/jira/browse/HDFS-1458) | Improve checkpoint performance by avoiding unnecessary image downloads |  Major | namenode | Hairong Kuang | Hairong Kuang |
| [HADOOP-6764](https://issues.apache.org/jira/browse/HADOOP-6764) | Add number of reader threads and queue length as configuration parameters in RPC.getServer |  Major | ipc | Dmytro Molkov | Dmytro Molkov |
| [HADOOP-7049](https://issues.apache.org/jira/browse/HADOOP-7049) | TestReconfiguration should be junit v4 |  Trivial | conf | Patrick Kling | Patrick Kling |
| [HDFS-1518](https://issues.apache.org/jira/browse/HDFS-1518) | Wrong description in FSNamesystem's javadoc |  Minor | namenode | Jingguo Yao | Jingguo Yao |
| [MAPREDUCE-1752](https://issues.apache.org/jira/browse/MAPREDUCE-1752) | Implement getFileBlockLocations in HarFilesystem |  Major | harchive | Dmytro Molkov | Dmytro Molkov |
| [MAPREDUCE-2155](https://issues.apache.org/jira/browse/MAPREDUCE-2155) | RaidNode should optionally dispatch map reduce jobs to fix corrupt blocks (instead of fixing locally) |  Major | contrib/raid | Patrick Kling | Patrick Kling |
| [MAPREDUCE-1783](https://issues.apache.org/jira/browse/MAPREDUCE-1783) | Task Initialization should be delayed till when a job can be run |  Major | contrib/fair-share | Ramkumar Vadali | Ramkumar Vadali |
| [MAPREDUCE-2156](https://issues.apache.org/jira/browse/MAPREDUCE-2156) | Raid-aware FSCK |  Major | contrib/raid | Patrick Kling | Patrick Kling |
| [HDFS-1506](https://issues.apache.org/jira/browse/HDFS-1506) | Refactor fsimage loading code |  Major | namenode | Hairong Kuang | Hairong Kuang |
| [HADOOP-7060](https://issues.apache.org/jira/browse/HADOOP-7060) | A more elegant FileSystem#listCorruptFileBlocks API |  Major | fs | Hairong Kuang | Patrick Kling |
| [HADOOP-7058](https://issues.apache.org/jira/browse/HADOOP-7058) | Expose number of bytes in FSOutputSummer buffer to implementatins |  Trivial | . | Todd Lipcon | Todd Lipcon |
| [HADOOP-7061](https://issues.apache.org/jira/browse/HADOOP-7061) | unprecise javadoc for CompressionCodec |  Minor | io | Jingguo Yao | Jingguo Yao |
| [MAPREDUCE-1831](https://issues.apache.org/jira/browse/MAPREDUCE-1831) | BlockPlacement policy for RAID |  Major | contrib/raid | Scott Chen | Scott Chen |
| [HADOOP-7059](https://issues.apache.org/jira/browse/HADOOP-7059) | Remove "unused" warning in native code |  Major | native | Noah Watkins | Noah Watkins |
| [HDFS-1476](https://issues.apache.org/jira/browse/HDFS-1476) | listCorruptFileBlocks should be functional while the name node is still in safe mode |  Major | namenode | Patrick Kling | Patrick Kling |
| [HDFS-1534](https://issues.apache.org/jira/browse/HDFS-1534) | Fix some incorrect logs in FSDirectory |  Minor | namenode | Eli Collins | Eli Collins |
| [HADOOP-7078](https://issues.apache.org/jira/browse/HADOOP-7078) | Add better javadocs for RawComparator interface |  Trivial | . | Todd Lipcon | Harsh J |
| [HDFS-1509](https://issues.apache.org/jira/browse/HDFS-1509) | Resync discarded directories in fs.name.dir during saveNamespace command |  Major | namenode | dhruba borthakur | dhruba borthakur |
| [MAPREDUCE-1906](https://issues.apache.org/jira/browse/MAPREDUCE-1906) | Lower default minimum heartbeat interval for tasktracker \> Jobtracker |  Major | jobtracker, performance, tasktracker | Scott Carey | Todd Lipcon |
| [HADOOP-6578](https://issues.apache.org/jira/browse/HADOOP-6578) | Configuration should trim whitespace around a lot of value types |  Minor | conf | Todd Lipcon | Michele Catasta |
| [HDFS-1539](https://issues.apache.org/jira/browse/HDFS-1539) | prevent data loss when a cluster suffers a power loss |  Major | datanode, hdfs-client, namenode | dhruba borthakur | dhruba borthakur |
| [MAPREDUCE-2207](https://issues.apache.org/jira/browse/MAPREDUCE-2207) | Task-cleanup task should not be scheduled on the node that the task just failed |  Major | jobtracker | Scott Chen | Liyin Liang |
| [MAPREDUCE-2248](https://issues.apache.org/jira/browse/MAPREDUCE-2248) | DistributedRaidFileSystem should unraid only the corrupt block |  Major | . | Ramkumar Vadali | Ramkumar Vadali |
| [HDFS-1547](https://issues.apache.org/jira/browse/HDFS-1547) | Improve decommission mechanism |  Major | namenode | Suresh Srinivas | Suresh Srinivas |
| [HDFS-1588](https://issues.apache.org/jira/browse/HDFS-1588) | Add dfs.hosts.exclude to DFSConfigKeys and use constant in stead of hardcoded string |  Major | . | Erik Steffl | Erik Steffl |
| [MAPREDUCE-2250](https://issues.apache.org/jira/browse/MAPREDUCE-2250) | Fix logging in raid code. |  Trivial | contrib/raid | Ramkumar Vadali | Ramkumar Vadali |
| [HDFS-1335](https://issues.apache.org/jira/browse/HDFS-1335) | HDFS side of HADOOP-6904: first step towards inter-version communications between dfs client and NameNode |  Major | hdfs-client, namenode | Hairong Kuang | Hairong Kuang |
| [MAPREDUCE-2263](https://issues.apache.org/jira/browse/MAPREDUCE-2263) | MapReduce side of HADOOP-6904 |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-6436](https://issues.apache.org/jira/browse/HADOOP-6436) | Remove auto-generated native build files |  Major | . | Eli Collins | Roman Shaposhnik |
| [MAPREDUCE-2260](https://issues.apache.org/jira/browse/MAPREDUCE-2260) | Remove auto-generated native build files |  Major | build | Roman Shaposhnik | Roman Shaposhnik |
| [HDFS-1582](https://issues.apache.org/jira/browse/HDFS-1582) | Remove auto-generated native build files |  Major | libhdfs | Roman Shaposhnik | Roman Shaposhnik |
| [MAPREDUCE-2026](https://issues.apache.org/jira/browse/MAPREDUCE-2026) | JobTracker.getJobCounters() should not hold JobTracker lock while calling JobInProgress.getCounters() |  Major | . | Scott Chen | Joydeep Sen Sarma |
| [MAPREDUCE-1706](https://issues.apache.org/jira/browse/MAPREDUCE-1706) | Log RAID recoveries on HDFS |  Major | contrib/raid | Rodrigo Schmidt | Scott Chen |
| [HDFS-1601](https://issues.apache.org/jira/browse/HDFS-1601) | Pipeline ACKs are sent as lots of tiny TCP packets |  Major | datanode | Todd Lipcon | Todd Lipcon |
| [HADOOP-7096](https://issues.apache.org/jira/browse/HADOOP-7096) | Allow setting of end-of-record delimiter for TextInputFormat |  Major | . | Ahmed Radwan | Ahmed Radwan |
| [HDFS-560](https://issues.apache.org/jira/browse/HDFS-560) | Proposed enhancements/tuning to hadoop-hdfs/build.xml |  Minor | build | Steve Loughran | Steve Loughran |
| [HADOOP-7048](https://issues.apache.org/jira/browse/HADOOP-7048) | Wrong description of Block-Compressed SequenceFile Format in SequenceFile's javadoc |  Minor | io | Jingguo Yao | Jingguo Yao |
| [HDFS-1628](https://issues.apache.org/jira/browse/HDFS-1628) | AccessControlException should display the full path |  Minor | namenode | Ramya Sunil | John George |
| [HADOOP-6376](https://issues.apache.org/jira/browse/HADOOP-6376) | slaves file to have a header specifying the format of conf/slaves file |  Minor | conf | Karthik K | Karthik K |
| [MAPREDUCE-2254](https://issues.apache.org/jira/browse/MAPREDUCE-2254) | Allow setting of end-of-record delimiter for TextInputFormat |  Major | . | Ahmed Radwan | Ahmed Radwan |
| [MAPREDUCE-2203](https://issues.apache.org/jira/browse/MAPREDUCE-2203) | Wong javadoc for TaskRunner's appendJobJarClasspaths method |  Trivial | . | Jingguo Yao | Jingguo Yao |
| [MAPREDUCE-1159](https://issues.apache.org/jira/browse/MAPREDUCE-1159) | Limit Job name on jobtracker.jsp to be 80 char long |  Trivial | . | Zheng Shao | Harsh J |
| [HADOOP-7112](https://issues.apache.org/jira/browse/HADOOP-7112) | Issue a warning when GenericOptionsParser libjars are not on local filesystem |  Major | conf, filecache | Tom White | Tom White |
| [MAPREDUCE-2206](https://issues.apache.org/jira/browse/MAPREDUCE-2206) | The task-cleanup tasks should be optional |  Major | jobtracker | Scott Chen | Scott Chen |
| [HDFS-1626](https://issues.apache.org/jira/browse/HDFS-1626) | Make BLOCK\_INVALIDATE\_LIMIT configurable |  Minor | namenode | Arun C Murthy | Tsz Wo Nicholas Sze |
| [MAPREDUCE-2225](https://issues.apache.org/jira/browse/MAPREDUCE-2225) | MultipleOutputs should not require the use of 'Writable' |  Blocker | job submission | Harsh J | Harsh J |
| [HADOOP-7114](https://issues.apache.org/jira/browse/HADOOP-7114) | FsShell should dump all exceptions at DEBUG level |  Minor | fs | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2302](https://issues.apache.org/jira/browse/MAPREDUCE-2302) | Add static factory methods in GaloisField |  Major | contrib/raid | Scott Chen | Scott Chen |
| [MAPREDUCE-2351](https://issues.apache.org/jira/browse/MAPREDUCE-2351) | mapred.job.tracker.history.completed.location should support an arbitrary filesystem URI |  Major | . | Tom White | Tom White |
| [MAPREDUCE-2239](https://issues.apache.org/jira/browse/MAPREDUCE-2239) | BlockPlacementPolicyRaid should call getBlockLocations only when necessary |  Major | contrib/raid | Scott Chen | Scott Chen |
| [HADOOP-7131](https://issues.apache.org/jira/browse/HADOOP-7131) | set() and toString Methods of the org.apache.hadoop.io.Text class does not include the root exception, in the wrapping RuntimeException. |  Minor | io | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-7159](https://issues.apache.org/jira/browse/HADOOP-7159) | RPC server should log the client hostname when read exception happened |  Trivial | ipc | Scott Chen | Scott Chen |
| [HADOOP-7133](https://issues.apache.org/jira/browse/HADOOP-7133) | CLONE to COMMON - HDFS-1445 Batch the calls in DataStorage to FileUtil.createHardLink(), so we call it once per directory instead of once per file |  Major | util | Matt Foley | Matt Foley |
| [HADOOP-7177](https://issues.apache.org/jira/browse/HADOOP-7177) | CodecPool should report which compressor it is using |  Trivial | native | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-7167](https://issues.apache.org/jira/browse/HADOOP-7167) | Allow using a file to exclude certain tests from build |  Minor | . | Todd Lipcon | Todd Lipcon |
| [HDFS-1731](https://issues.apache.org/jira/browse/HDFS-1731) | Allow using a file to exclude certain tests from build |  Minor | . | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2367](https://issues.apache.org/jira/browse/MAPREDUCE-2367) | Allow using a file to exclude certain tests from build |  Minor | . | Todd Lipcon | Todd Lipcon |
| [HDFS-1757](https://issues.apache.org/jira/browse/HDFS-1757) | Don't compile fuse-dfs by default |  Major | fuse-dfs | Eli Collins | Eli Collins |
| [HDFS-1596](https://issues.apache.org/jira/browse/HDFS-1596) | Move secondary namenode checkpoint configs from core-default.xml to hdfs-default.xml |  Major | documentation, namenode | Patrick Angeles | Harsh J |
| [HADOOP-7117](https://issues.apache.org/jira/browse/HADOOP-7117) | Move secondary namenode checkpoint configs from core-default.xml to hdfs-default.xml |  Major | conf | Patrick Angeles | Harsh J |
| [HADOOP-7180](https://issues.apache.org/jira/browse/HADOOP-7180) | Improve CommandFormat |  Minor | fs | Daryn Sharp | Daryn Sharp |
| [HDFS-1763](https://issues.apache.org/jira/browse/HDFS-1763) | Replace hard-coded option strings with variables from DFSConfigKeys |  Minor | . | Eli Collins | Eli Collins |
| [HDFS-1583](https://issues.apache.org/jira/browse/HDFS-1583) | Improve backup-node sync performance by wrapping RPC parameters |  Major | namenode | Liyin Liang | Liyin Liang |
| [HADOOP-7193](https://issues.apache.org/jira/browse/HADOOP-7193) | Help message is wrong for touchz command. |  Minor | fs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [MAPREDUCE-2403](https://issues.apache.org/jira/browse/MAPREDUCE-2403) | MR-279: Improve job history event handling in AM to log to HDFS |  Major | mrv2 | Mahadev konar | Krishna Ramachandran |
| [HDFS-1785](https://issues.apache.org/jira/browse/HDFS-1785) | Cleanup BlockReceiver and DataXceiver |  Major | datanode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-1120](https://issues.apache.org/jira/browse/HDFS-1120) | Make DataNode's block-to-device placement policy pluggable |  Major | datanode | Jeff Hammerbacher | Harsh J |
| [HDFS-1789](https://issues.apache.org/jira/browse/HDFS-1789) | Refactor frequently used codes from DFSOutputStream, BlockReceiver and DataXceiver |  Minor | datanode, hdfs-client | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [MAPREDUCE-2414](https://issues.apache.org/jira/browse/MAPREDUCE-2414) | MR-279: Use generic interfaces for protocols |  Major | mrv2 | Arun C Murthy | Siddharth Seth |
| [HADOOP-7202](https://issues.apache.org/jira/browse/HADOOP-7202) | Improve Command base class |  Major | . | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-2405](https://issues.apache.org/jira/browse/MAPREDUCE-2405) | MR-279: Implement uber-AppMaster (in-cluster LocalJobRunner for MRv2) |  Major | mrv2 | Mahadev konar | Greg Roelofs |
| [HDFS-1817](https://issues.apache.org/jira/browse/HDFS-1817) | Split TestFiDataTransferProtocol.java into two files |  Trivial | test | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-1630](https://issues.apache.org/jira/browse/HDFS-1630) | Checksum fsedits |  Major | namenode | Hairong Kuang | Hairong Kuang |
| [HADOOP-6994](https://issues.apache.org/jira/browse/HADOOP-6994) | Api to get delegation token in AbstractFileSystem |  Major | . | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [HDFS-1442](https://issues.apache.org/jira/browse/HDFS-1442) | Api to get delegation token in Hdfs |  Major | . | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [MAPREDUCE-2432](https://issues.apache.org/jira/browse/MAPREDUCE-2432) | MR-279: Install sanitized poms for downstream sanity |  Major | mrv2 | Luke Lu | Luke Lu |
| [HDFS-1833](https://issues.apache.org/jira/browse/HDFS-1833) | Refactor BlockReceiver |  Minor | datanode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-1831](https://issues.apache.org/jira/browse/HDFS-1831) | HDFS equivalent of HADOOP-7223 changes to handle FileContext createFlag combinations |  Major | namenode | Suresh Srinivas | Suresh Srinivas |
| [MAPREDUCE-2424](https://issues.apache.org/jira/browse/MAPREDUCE-2424) | MR-279: counters/UI/etc. for uber-AppMaster (in-cluster LocalJobRunner for MRv2) |  Major | mrv2 | Greg Roelofs | Greg Roelofs |
| [HADOOP-7014](https://issues.apache.org/jira/browse/HADOOP-7014) | Generalize CLITest structure and interfaces to facilitate upstream adoption (e.g. for web testing) |  Major | test | Konstantin Boudnik | Konstantin Boudnik |
| [HDFS-1486](https://issues.apache.org/jira/browse/HDFS-1486) | Generalize CLITest structure and interfaces to facilitate upstream adoption (e.g. for web testing) |  Major | test | Konstantin Boudnik | Konstantin Boudnik |
| [MAPREDUCE-2202](https://issues.apache.org/jira/browse/MAPREDUCE-2202) | Generalize CLITest structure and interfaces to facilitate upstream adoption (e.g. for web or system testing) |  Major | . | Konstantin Boudnik | Konstantin Boudnik |
| [HDFS-1840](https://issues.apache.org/jira/browse/HDFS-1840) | Terminate LeaseChecker when all writing files are closed. |  Major | hdfs-client | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-7233](https://issues.apache.org/jira/browse/HADOOP-7233) | Refactor FsShell's ls |  Major | fs | Daryn Sharp | Daryn Sharp |
| [HADOOP-7241](https://issues.apache.org/jira/browse/HADOOP-7241) | fix typo of command 'hadoop fs -help tail' |  Minor | fs, test | Wei Yongjun | Wei Yongjun |
| [HDFS-1861](https://issues.apache.org/jira/browse/HDFS-1861) | Rename dfs.datanode.max.xcievers and bump its default value |  Major | datanode | Eli Collins | Eli Collins |
| [HDFS-1843](https://issues.apache.org/jira/browse/HDFS-1843) | Discover file not found early for file append |  Minor | . | Bharath Mundlapudi | Bharath Mundlapudi |
| [HADOOP-7235](https://issues.apache.org/jira/browse/HADOOP-7235) | Refactor FsShell's tail |  Major | . | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-1461](https://issues.apache.org/jira/browse/MAPREDUCE-1461) | Feature to instruct rumen-folder utility to skip jobs worth of specific duration |  Major | tools/rumen | Rajesh Balamohan | Rajesh Balamohan |
| [MAPREDUCE-2153](https://issues.apache.org/jira/browse/MAPREDUCE-2153) | Bring in more job configuration properties in to the trace file |  Major | tools/rumen | Ravi Gummadi | Rajesh Balamohan |
| [HDFS-1846](https://issues.apache.org/jira/browse/HDFS-1846) | Don't fill preallocated portion of edits log with 0x00 |  Major | namenode | Aaron T. Myers | Aaron T. Myers |
| [HDFS-1741](https://issues.apache.org/jira/browse/HDFS-1741) | Provide a minimal pom file to allow integration of HDFS into Sonar analysis |  Major | build | Konstantin Boudnik | Konstantin Boudnik |
| [MAPREDUCE-2462](https://issues.apache.org/jira/browse/MAPREDUCE-2462) | MR 279: Write job conf along with JobHistory, other minor improvements |  Minor | mrv2 | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-1978](https://issues.apache.org/jira/browse/MAPREDUCE-1978) | [Rumen] TraceBuilder should provide recursive input folder scanning |  Major | tools/rumen | Amar Kamat | Ravi Gummadi |
| [HDFS-1870](https://issues.apache.org/jira/browse/HDFS-1870) | Refactor DFSClient.LeaseChecker |  Minor | hdfs-client | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-1773](https://issues.apache.org/jira/browse/HDFS-1773) | Remove a datanode from cluster if include list is not empty and this datanode is removed from both include and exclude lists |  Minor | namenode | Tanping Wang | Tanping Wang |
| [HADOOP-7236](https://issues.apache.org/jira/browse/HADOOP-7236) | Refactor FsShell's mkdir |  Major | fs | Daryn Sharp | Daryn Sharp |
| [HADOOP-7250](https://issues.apache.org/jira/browse/HADOOP-7250) | Refactor FsShell's setrep |  Major | fs | Daryn Sharp | Daryn Sharp |
| [HDFS-1890](https://issues.apache.org/jira/browse/HDFS-1890) | A few improvements on the LeaseRenewer.pendingCreates map |  Minor | hdfs-client | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [MAPREDUCE-2474](https://issues.apache.org/jira/browse/MAPREDUCE-2474) | Add docs to the new API Partitioner on how to access Job Configuration data |  Minor | documentation | Harsh J | Harsh J |
| [HADOOP-7249](https://issues.apache.org/jira/browse/HADOOP-7249) | Refactor FsShell's chmod/chown/chgrp |  Major | fs | Daryn Sharp | Daryn Sharp |
| [HADOOP-7251](https://issues.apache.org/jira/browse/HADOOP-7251) | Refactor FsShell's getmerge |  Major | fs | Daryn Sharp | Daryn Sharp |
| [HADOOP-7265](https://issues.apache.org/jira/browse/HADOOP-7265) | Keep track of relative paths |  Major | fs | Daryn Sharp | Daryn Sharp |
| [HADOOP-7238](https://issues.apache.org/jira/browse/HADOOP-7238) | Refactor FsShell's cat & text |  Major | fs | Daryn Sharp | Daryn Sharp |
| [HDFS-1865](https://issues.apache.org/jira/browse/HDFS-1865) | Share LeaseChecker thread among DFSClients |  Major | hdfs-client | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-1906](https://issues.apache.org/jira/browse/HDFS-1906) | Remove logging exception stack trace when one of the datanode targets to read from is not reachable |  Minor | hdfs-client | Suresh Srinivas | Suresh Srinivas |
| [MAPREDUCE-2478](https://issues.apache.org/jira/browse/MAPREDUCE-2478) | MR 279: Improve history server |  Major | mrv2 | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-2456](https://issues.apache.org/jira/browse/MAPREDUCE-2456) | Show the reducer taskid and map/reduce tasktrackers for "Failed fetch notification #\_ for task attempt..." log messages |  Trivial | jobtracker | Jeffrey Naisbitt | Jeffrey Naisbitt |
| [HADOOP-7271](https://issues.apache.org/jira/browse/HADOOP-7271) | Standardize error messages |  Major | fs | Daryn Sharp | Daryn Sharp |
| [HADOOP-7275](https://issues.apache.org/jira/browse/HADOOP-7275) | Refactor FsShell's stat |  Major | fs | Daryn Sharp | Daryn Sharp |
| [HADOOP-7237](https://issues.apache.org/jira/browse/HADOOP-7237) | Refactor FsShell's touchz |  Major | fs | Daryn Sharp | Daryn Sharp |
| [HADOOP-7267](https://issues.apache.org/jira/browse/HADOOP-7267) | Refactor FsShell's rm/rmr/expunge |  Major | fs | Daryn Sharp | Daryn Sharp |
| [HADOOP-6887](https://issues.apache.org/jira/browse/HADOOP-6887) | Need a separate metrics per garbage collector |  Major | metrics | Bharath Mundlapudi | Luke Lu |
| [HADOOP-7285](https://issues.apache.org/jira/browse/HADOOP-7285) | Refactor FsShell's test |  Major | fs | Daryn Sharp | Daryn Sharp |
| [HDFS-1899](https://issues.apache.org/jira/browse/HDFS-1899) | GenericTestUtils.formatNamenode is misplaced |  Major | . | Todd Lipcon | Ted Yu |
| [HADOOP-7289](https://issues.apache.org/jira/browse/HADOOP-7289) | ivy: test conf should not extend common conf |  Major | build | Tsz Wo Nicholas Sze | Eric Yang |
| [HDFS-1573](https://issues.apache.org/jira/browse/HDFS-1573) | LeaseChecker thread name trace not that useful |  Trivial | hdfs-client | Todd Lipcon | Todd Lipcon |
| [HDFS-1939](https://issues.apache.org/jira/browse/HDFS-1939) | ivy: test conf should not extend common conf |  Major | build | Tsz Wo Nicholas Sze | Eric Yang |
| [HDFS-1332](https://issues.apache.org/jira/browse/HDFS-1332) | When unable to place replicas, BlockPlacementPolicy should log reasons nodes were excluded |  Minor | namenode | Todd Lipcon | Ted Yu |
| [HADOOP-7205](https://issues.apache.org/jira/browse/HADOOP-7205) | automatically determine JAVA\_HOME on OS X |  Trivial | . | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-2381](https://issues.apache.org/jira/browse/MAPREDUCE-2381) | JobTracker instrumentation not consistent about error handling |  Major | . | Philip Zeyliger | Philip Zeyliger |
| [HADOOP-7301](https://issues.apache.org/jira/browse/HADOOP-7301) | FSDataInputStream should expose a getWrappedStream method |  Major | . | Jonathan Hsieh | Jonathan Hsieh |
| [MAPREDUCE-2449](https://issues.apache.org/jira/browse/MAPREDUCE-2449) | Allow for command line arguments when performing "Run on Hadoop" action. |  Minor | contrib/eclipse-plugin | Jeff Zemerick | Jeff Zemerick |
| [HDFS-1958](https://issues.apache.org/jira/browse/HDFS-1958) | Format confirmation prompt should be more lenient of its input |  Major | namenode | Todd Lipcon | Todd Lipcon |
| [HADOOP-7306](https://issues.apache.org/jira/browse/HADOOP-7306) | Start metrics system even if config files are missing |  Major | metrics | Luke Lu | Luke Lu |
| [MAPREDUCE-2459](https://issues.apache.org/jira/browse/MAPREDUCE-2459) | Cache HAR filesystem metadata |  Major | harchive | Mac Yang | Mac Yang |
| [MAPREDUCE-2490](https://issues.apache.org/jira/browse/MAPREDUCE-2490) | Log blacklist debug count |  Trivial | jobtracker | Jonathan Eagles | Jonathan Eagles |
| [HDFS-1959](https://issues.apache.org/jira/browse/HDFS-1959) | Better error message for missing namenode directory |  Minor | . | Eli Collins | Eli Collins |
| [MAPREDUCE-2492](https://issues.apache.org/jira/browse/MAPREDUCE-2492) | [MAPREDUCE] The new MapReduce API should make available task's progress to the task |  Major | task | Amar Kamat | Amar Kamat |
| [MAPREDUCE-2495](https://issues.apache.org/jira/browse/MAPREDUCE-2495) | The distributed cache cleanup thread has no monitoring to check to see if it has died for some reason |  Minor | distributed-cache | Robert Joseph Evans | Robert Joseph Evans |
| [HADOOP-7329](https://issues.apache.org/jira/browse/HADOOP-7329) | incomplete help message  is displayed for df -h option |  Minor | fs | XieXianshan | XieXianshan |
| [HADOOP-7320](https://issues.apache.org/jira/browse/HADOOP-7320) | Refactor FsShell's copy & move commands |  Major | . | Daryn Sharp | Daryn Sharp |
| [HDFS-1996](https://issues.apache.org/jira/browse/HDFS-1996) | ivy: hdfs test jar should be independent to common test jar |  Major | build | Tsz Wo Nicholas Sze | Eric Yang |
| [HADOOP-7333](https://issues.apache.org/jira/browse/HADOOP-7333) | Performance improvement in PureJavaCrc32 |  Minor | performance, util | Eric Caspole | Eric Caspole |
| [HADOOP-7337](https://issues.apache.org/jira/browse/HADOOP-7337) | Annotate PureJavaCrc32 as a public API |  Minor | util | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [MAPREDUCE-2551](https://issues.apache.org/jira/browse/MAPREDUCE-2551) | MR 279: Implement JobSummaryLog |  Major | mrv2 | Siddharth Seth | Siddharth Seth |
| [HDFS-1636](https://issues.apache.org/jira/browse/HDFS-1636) | If dfs.name.dir points to an empty dir, namenode format shouldn't require confirmation |  Minor | namenode | Todd Lipcon | Harsh J |
| [HDFS-2024](https://issues.apache.org/jira/browse/HDFS-2024) | Eclipse format HDFS Junit test hdfs/TestWriteRead.java |  Trivial | test | CW Chung | CW Chung |
| [MAPREDUCE-2469](https://issues.apache.org/jira/browse/MAPREDUCE-2469) | Task counters should also report the total heap usage of the task |  Major | task | Amar Kamat | Amar Kamat |
| [HDFS-1995](https://issues.apache.org/jira/browse/HDFS-1995) | Minor modification to both dfsclusterhealth and dfshealth pages for Web UI |  Minor | . | Tanping Wang | Tanping Wang |
| [HADOOP-7316](https://issues.apache.org/jira/browse/HADOOP-7316) | Add public javadocs to FSDataInputStream and FSDataOutputStream |  Major | documentation | Jonathan Hsieh | Eli Collins |
| [HDFS-2029](https://issues.apache.org/jira/browse/HDFS-2029) | Improve TestWriteRead |  Trivial | test | Tsz Wo Nicholas Sze | John George |
| [HDFS-2040](https://issues.apache.org/jira/browse/HDFS-2040) | Only build libhdfs if a flag is passed |  Minor | . | Eli Collins | Eli Collins |
| [HDFS-2003](https://issues.apache.org/jira/browse/HDFS-2003) | Separate FSEditLog reading logic from editLog memory state building logic |  Major | . | Ivan Kelly | Ivan Kelly |
| [MAPREDUCE-2580](https://issues.apache.org/jira/browse/MAPREDUCE-2580) | MR 279: RM UI should redirect finished jobs to History UI |  Minor | mrv2 | Siddharth Seth | Siddharth Seth |
| [HDFS-2056](https://issues.apache.org/jira/browse/HDFS-2056) | Update fetchdt usage |  Minor | documentation, tools | Tanping Wang | Tanping Wang |
| [HADOOP-1886](https://issues.apache.org/jira/browse/HADOOP-1886) | Undocumented parameters in FilesSystem |  Trivial | fs | Konstantin Shvachko | Frank Conrad |
| [MAPREDUCE-1624](https://issues.apache.org/jira/browse/MAPREDUCE-1624) | Document the job credentials and associated details to do with delegation tokens (on the client side) |  Major | documentation | Devaraj Das | Devaraj Das |
| [HADOOP-7375](https://issues.apache.org/jira/browse/HADOOP-7375) | Add resolvePath method to FileContext |  Major | . | Sanjay Radia | Sanjay Radia |
| [HDFS-2046](https://issues.apache.org/jira/browse/HDFS-2046) | Force entropy to come from non-true random for tests |  Major | build, test | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2106](https://issues.apache.org/jira/browse/MAPREDUCE-2106) | Emulate CPU Usage of Tasks in GridMix3 |  Major | contrib/gridmix | Ranjit Mathew | Amar Kamat |
| [HADOOP-7384](https://issues.apache.org/jira/browse/HADOOP-7384) | Allow test-patch to be more flexible about patch format |  Major | . | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2105](https://issues.apache.org/jira/browse/MAPREDUCE-2105) | Simulate Load Incrementally and Adaptively in GridMix3 |  Major | contrib/gridmix | Ranjit Mathew | Amar Kamat |
| [MAPREDUCE-2107](https://issues.apache.org/jira/browse/MAPREDUCE-2107) | Emulate Memory Usage of Tasks in GridMix3 |  Major | contrib/gridmix | Ranjit Mathew | Amar Kamat |
| [MAPREDUCE-1702](https://issues.apache.org/jira/browse/MAPREDUCE-1702) | CPU/Memory emulation for GridMix3 |  Minor | contrib/gridmix | Jaideep |  |
| [MAPREDUCE-2326](https://issues.apache.org/jira/browse/MAPREDUCE-2326) | Port gridmix changes from hadoop-0.20.100 to trunk |  Major | . | Arun C Murthy |  |
| [HADOOP-7379](https://issues.apache.org/jira/browse/HADOOP-7379) | Add ability to include Protobufs in ObjectWritable |  Major | io, ipc | Todd Lipcon | Todd Lipcon |
| [HDFS-2073](https://issues.apache.org/jira/browse/HDFS-2073) | Namenode is missing @Override annotations |  Minor | namenode | Suresh Srinivas | Suresh Srinivas |
| [HADOOP-6929](https://issues.apache.org/jira/browse/HADOOP-6929) | RPC should have a way to pass Security information other than protocol annotations |  Major | ipc, security | Sharad Agarwal | Sharad Agarwal |
| [HDFS-420](https://issues.apache.org/jira/browse/HDFS-420) | Fuse-dfs should cache fs handles |  Major | fuse-dfs | Dima Brodsky | Brian Bockelman |
| [HDFS-1568](https://issues.apache.org/jira/browse/HDFS-1568) | Improve DataXceiver error logging |  Minor | datanode | Todd Lipcon | Joey Echeverria |
| [MAPREDUCE-2611](https://issues.apache.org/jira/browse/MAPREDUCE-2611) | MR 279: Metrics, finishTimes, etc in JobHistory |  Major | mrv2 | Siddharth Seth |  |
| [HADOOP-7392](https://issues.apache.org/jira/browse/HADOOP-7392) | Implement capability of querying individual property of a mbean using JMXProxyServlet |  Major | . | Tanping Wang | Tanping Wang |
| [HDFS-2110](https://issues.apache.org/jira/browse/HDFS-2110) | Some StreamFile and ByteRangeInputStream cleanup |  Minor | namenode | Eli Collins | Eli Collins |
| [MAPREDUCE-2624](https://issues.apache.org/jira/browse/MAPREDUCE-2624) | Update RAID for HDFS-2107 |  Major | contrib/raid | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-2118](https://issues.apache.org/jira/browse/HDFS-2118) | Couple dfs data dir improvements |  Minor | datanode | Eli Collins | Eli Collins |
| [MAPREDUCE-2501](https://issues.apache.org/jira/browse/MAPREDUCE-2501) | MR-279: Attach sources in builds |  Major | mrv2 | Luke Lu | Luke Lu |
| [HADOOP-7448](https://issues.apache.org/jira/browse/HADOOP-7448) | merge for MR-279: HttpServer /stacks servlet should use plain text content type |  Major | . | Matt Foley | Matt Foley |
| [HADOOP-7451](https://issues.apache.org/jira/browse/HADOOP-7451) | merge for MR-279: Generalize StringUtils#join |  Major | . | Matt Foley | Matt Foley |
| [HADOOP-7449](https://issues.apache.org/jira/browse/HADOOP-7449) | merge for MR-279: add Data(In,Out)putByteBuffer to work with ByteBuffer similar to Data(In,Out)putBuffer for byte[] |  Major | . | Matt Foley | Matt Foley |
| [MAPREDUCE-2249](https://issues.apache.org/jira/browse/MAPREDUCE-2249) | Better to check the reflexive property of the object while overriding equals method of it |  Major | . | Bhallamudi Venkata Siva Kamesh | Devaraj K |
| [MAPREDUCE-2596](https://issues.apache.org/jira/browse/MAPREDUCE-2596) | Gridmix should notify job failures |  Major | benchmarks, contrib/gridmix | Arun C Murthy | Amar Kamat |
| [HADOOP-7361](https://issues.apache.org/jira/browse/HADOOP-7361) | Provide overwrite option (-overwrite/-f) in put and copyFromLocal command line options |  Minor | fs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-2054](https://issues.apache.org/jira/browse/HDFS-2054) | BlockSender.sendChunk() prints ERROR for connection closures encountered  during transferToFully() |  Minor | datanode | Kihwal Lee | Kihwal Lee |
| [HADOOP-7430](https://issues.apache.org/jira/browse/HADOOP-7430) | Improve error message when moving to trash fails due to quota issue |  Minor | fs | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-2680](https://issues.apache.org/jira/browse/MAPREDUCE-2680) | Enhance job-client cli to show queue information for running jobs |  Minor | . | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-2679](https://issues.apache.org/jira/browse/MAPREDUCE-2679) | MR-279: Merge MR-279 related minor patches into trunk |  Trivial | . | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-2682](https://issues.apache.org/jira/browse/MAPREDUCE-2682) | Add a -classpath option to bin/mapred |  Trivial | . | Arun C Murthy | Vinod Kumar Vavilapalli |
| [HADOOP-7444](https://issues.apache.org/jira/browse/HADOOP-7444) | Add Checksum API to verify and calculate checksums "in bulk" |  Major | . | Todd Lipcon | Todd Lipcon |
| [HADOOP-7305](https://issues.apache.org/jira/browse/HADOOP-7305) | Eclipse project files are incomplete |  Minor | build | Niels Basjes | Niels Basjes |
| [HDFS-2143](https://issues.apache.org/jira/browse/HDFS-2143) | Federation: we should link to the live nodes and dead nodes to cluster web console |  Major | . | Ravi Prakash | Ravi Prakash |
| [HADOOP-7434](https://issues.apache.org/jira/browse/HADOOP-7434) | Display error when using "daemonlog -setlevel" with illegal level |  Minor | . | 严金双 | 严金双 |
| [HDFS-2157](https://issues.apache.org/jira/browse/HDFS-2157) | Improve header comment in o.a.h.hdfs.server.namenode.NameNode |  Major | documentation, namenode | Aaron T. Myers | Aaron T. Myers |
| [MAPREDUCE-2623](https://issues.apache.org/jira/browse/MAPREDUCE-2623) | Update ClusterMapReduceTestCase to use MiniDFSCluster.Builder |  Minor | test | Jim Plush | Harsh J |
| [HDFS-2161](https://issues.apache.org/jira/browse/HDFS-2161) | Move utilities to DFSUtil |  Minor | balancer & mover, datanode, hdfs-client, namenode, security | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-7314](https://issues.apache.org/jira/browse/HADOOP-7314) | Add support for throwing UnknownHostException when a host doesn't resolve |  Major | . | Jeffrey Naisbitt | Jeffrey Naisbitt |
| [HDFS-1774](https://issues.apache.org/jira/browse/HDFS-1774) | Small optimization to FSDataset |  Minor | datanode | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-7460](https://issues.apache.org/jira/browse/HADOOP-7460) | Support for pluggable Trash policies |  Major | fs | dhruba borthakur | Usman Masood |
| [HDFS-1739](https://issues.apache.org/jira/browse/HDFS-1739) | When DataNode throws DiskOutOfSpaceException, it will be helpfull to the user if we log the available volume size and configured block size. |  Minor | datanode | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-2116](https://issues.apache.org/jira/browse/HDFS-2116) | Cleanup TestStreamFile and TestByteRangeInputStream |  Minor | test | Eli Collins | Plamen Jeliazkov |
| [HADOOP-7463](https://issues.apache.org/jira/browse/HADOOP-7463) | Adding a configuration parameter to SecurityInfo interface. |  Minor | . | Mahadev konar | Mahadev konar |
| [HADOOP-7438](https://issues.apache.org/jira/browse/HADOOP-7438) | Using the hadoop-deamon.sh script to start nodes leads to a depricated warning |  Major | . | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-2602](https://issues.apache.org/jira/browse/MAPREDUCE-2602) | Allow setting of end-of-record delimiter for TextInputFormat (for the old API) |  Major | . | Ahmed Radwan | Ahmed Radwan |
| [HDFS-2144](https://issues.apache.org/jira/browse/HDFS-2144) | If SNN shuts down during initialization it does not log the cause |  Major | namenode | Ravi Prakash | Ravi Prakash |
| [HDFS-2180](https://issues.apache.org/jira/browse/HDFS-2180) | Refactor NameNode HTTP server into new class |  Major | . | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2664](https://issues.apache.org/jira/browse/MAPREDUCE-2664) | MR 279: Implement JobCounters for MRv2 + Fix for Map Data Locality |  Major | mrv2 | Siddharth Seth | Siddharth Seth |
| [HDFS-2198](https://issues.apache.org/jira/browse/HDFS-2198) | Remove hardcoded configuration keys |  Minor | datanode, hdfs-client, namenode | Suresh Srinivas | Suresh Srinivas |
| [HDFS-2212](https://issues.apache.org/jira/browse/HDFS-2212) | Refactor double-buffering code out of EditLogOutputStreams |  Major | namenode | Todd Lipcon | Todd Lipcon |
| [HADOOP-7474](https://issues.apache.org/jira/browse/HADOOP-7474) | Refactor ClientCache out of WritableRpcEngine. |  Major | . | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [MAPREDUCE-2707](https://issues.apache.org/jira/browse/MAPREDUCE-2707) | ProtoOverHadoopRpcEngine without using TunnelProtocol over WritableRpc |  Major | . | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [HADOOP-7491](https://issues.apache.org/jira/browse/HADOOP-7491) | hadoop command should respect HADOOP\_OPTS when given a class name |  Major | scripts | Eli Collins | Eli Collins |
| [MAPREDUCE-2243](https://issues.apache.org/jira/browse/MAPREDUCE-2243) | Close all the file streams propely in a finally block to avoid their leakage. |  Minor | jobtracker, tasktracker | Bhallamudi Venkata Siva Kamesh | Devaraj K |
| [MAPREDUCE-2494](https://issues.apache.org/jira/browse/MAPREDUCE-2494) | Make the distributed cache delete entires using LRU priority |  Major | distributed-cache | Robert Joseph Evans | Robert Joseph Evans |
| [HDFS-2187](https://issues.apache.org/jira/browse/HDFS-2187) | HDFS-1580: Make EditLogInputStream act like an iterator over FSEditLogOps |  Major | . | Ivan Kelly | Ivan Kelly |
| [HADOOP-7445](https://issues.apache.org/jira/browse/HADOOP-7445) | Implement bulk checksum verification using efficient native code |  Major | native, util | Todd Lipcon | Todd Lipcon |
| [HDFS-2226](https://issues.apache.org/jira/browse/HDFS-2226) | Clean up counting of operations in FSEditLogLoader |  Trivial | namenode | Todd Lipcon | Todd Lipcon |
| [HDFS-2225](https://issues.apache.org/jira/browse/HDFS-2225) | HDFS-2018 Part 1 : Refactor file management so its not in classes which should be generic |  Major | . | Ivan Kelly | Ivan Kelly |
| [HDFS-2238](https://issues.apache.org/jira/browse/HDFS-2238) | NamenodeFsck.toString() uses StringBuilder with + operator |  Minor | namenode | Tsz Wo Nicholas Sze | Uma Maheswara Rao G |
| [HDFS-2230](https://issues.apache.org/jira/browse/HDFS-2230) | hdfs it not resolving the latest common test jars published post common mavenization |  Major | build | Giridharan Kesavan | Giridharan Kesavan |
| [HDFS-2227](https://issues.apache.org/jira/browse/HDFS-2227) | HDFS-2018 Part 2 :  getRemoteEditLogManifest should pull it's information from FileJournalManager |  Major | . | Ivan Kelly | Ivan Kelly |
| [HADOOP-7472](https://issues.apache.org/jira/browse/HADOOP-7472) | RPC client should deal with the IP address changes |  Minor | ipc | Kihwal Lee | Kihwal Lee |
| [HDFS-2241](https://issues.apache.org/jira/browse/HDFS-2241) | Remove implementing FSConstants interface just to access the constants defined in the interface |  Major | . | Suresh Srinivas | Suresh Srinivas |
| [HADOOP-7531](https://issues.apache.org/jira/browse/HADOOP-7531) | Add servlet util methods for handling paths in requests |  Major | util | Eli Collins | Eli Collins |
| [MAPREDUCE-901](https://issues.apache.org/jira/browse/MAPREDUCE-901) | Move Framework Counters into a TaskMetric structure |  Major | task | Owen O'Malley | Luke Lu |
| [HDFS-2260](https://issues.apache.org/jira/browse/HDFS-2260) | Refactor BlockReader into an interface and implementation |  Major | hdfs-client | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-279](https://issues.apache.org/jira/browse/MAPREDUCE-279) | Map-Reduce 2.0 |  Major | mrv2 | Arun C Murthy |  |
| [HADOOP-7555](https://issues.apache.org/jira/browse/HADOOP-7555) | Add a eclipse-generated files to .gitignore |  Trivial | build | Aaron T. Myers | Aaron T. Myers |
| [MAPREDUCE-2805](https://issues.apache.org/jira/browse/MAPREDUCE-2805) | Update RAID for HDFS-2241 |  Minor | contrib/raid | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [MAPREDUCE-2701](https://issues.apache.org/jira/browse/MAPREDUCE-2701) | MR-279: app/Job.java needs UGI for the user that launched it |  Major | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-2848](https://issues.apache.org/jira/browse/MAPREDUCE-2848) | Upgrade avro to 1.5.2 |  Major | . | Luke Lu | Luke Lu |
| [HDFS-2273](https://issues.apache.org/jira/browse/HDFS-2273) | Refactor BlockManager.recentInvalidateSets to a new class |  Minor | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-2286](https://issues.apache.org/jira/browse/HDFS-2286) | DataXceiverServer logs AsynchronousCloseException at shutdown |  Trivial | datanode | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2893](https://issues.apache.org/jira/browse/MAPREDUCE-2893) | Removing duplicate service provider in hadoop-mapreduce-client-jobclient |  Trivial | client | Liang-Chi Hsieh | Liang-Chi Hsieh |
| [HADOOP-7595](https://issues.apache.org/jira/browse/HADOOP-7595) | Upgrade dependency to Avro 1.5.3 |  Major | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-7552](https://issues.apache.org/jira/browse/HADOOP-7552) | FileUtil#fullyDelete doesn't throw IOE but lists it in the throws clause |  Minor | fs | Eli Collins | Eli Collins |
| [HDFS-1620](https://issues.apache.org/jira/browse/HDFS-1620) | Rename HdfsConstants -\> HdfsServerConstants, FSConstants -\> HdfsConstants |  Minor | . | Tsz Wo Nicholas Sze | Harsh J |
| [HADOOP-7612](https://issues.apache.org/jira/browse/HADOOP-7612) | Change test-patch to run tests for all nested modules |  Major | build | Tom White | Tom White |
| [MAPREDUCE-2864](https://issues.apache.org/jira/browse/MAPREDUCE-2864) | Renaming of configuration property names in yarn |  Major | jobhistoryserver, mrv2, nodemanager, resourcemanager | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-2691](https://issues.apache.org/jira/browse/MAPREDUCE-2691) | Finish up the cleanup of distributed cache file resources and related tests. |  Major | mrv2 | Amol Kekre | Siddharth Seth |
| [MAPREDUCE-2675](https://issues.apache.org/jira/browse/MAPREDUCE-2675) | MR-279: JobHistory Server main page needs to be reformatted |  Major | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [HDFS-2197](https://issues.apache.org/jira/browse/HDFS-2197) | Refactor RPC call implementations out of NameNode class |  Major | namenode | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2676](https://issues.apache.org/jira/browse/MAPREDUCE-2676) | MR-279: JobHistory Job page needs reformatted |  Major | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-2966](https://issues.apache.org/jira/browse/MAPREDUCE-2966) | Add ShutDown hooks for MRV2 processes |  Major | applicationmaster, jobhistoryserver, nodemanager, resourcemanager | Abhijit Suresh Shingate | Abhijit Suresh Shingate |
| [MAPREDUCE-2672](https://issues.apache.org/jira/browse/MAPREDUCE-2672) | MR-279: JobHistory Server needs Analysis this job |  Major | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-2726](https://issues.apache.org/jira/browse/MAPREDUCE-2726) | MR-279: Add the jobFile to the web UI |  Blocker | mrv2 | Jeffrey Naisbitt | Jeffrey Naisbitt |
| [MAPREDUCE-2930](https://issues.apache.org/jira/browse/MAPREDUCE-2930) | Generate state graph from the State Machine Definition |  Major | mrv2 | Sharad Agarwal | Binglin Chang |
| [MAPREDUCE-2880](https://issues.apache.org/jira/browse/MAPREDUCE-2880) | Fix classpath construction for MRv2 |  Blocker | mrv2 | Luke Lu | Arun C Murthy |
| [HADOOP-7457](https://issues.apache.org/jira/browse/HADOOP-7457) | Remove out-of-date Chinese language documentation |  Blocker | documentation | Jakob Homan | Jakob Homan |
| [MAPREDUCE-1207](https://issues.apache.org/jira/browse/MAPREDUCE-1207) | Allow admins to set java options for map/reduce tasks |  Blocker | client, mrv2 | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-2961](https://issues.apache.org/jira/browse/MAPREDUCE-2961) | Increase the default threadpool size for container launching in the application master. |  Blocker | mrv2 | Mahadev konar | Vinod Kumar Vavilapalli |
| [MAPREDUCE-2990](https://issues.apache.org/jira/browse/MAPREDUCE-2990) | Health Report on Resource Manager UI is null if the NM's are all healthy. |  Blocker | mrv2 | Mahadev konar | Subroto Sanyal |
| [MAPREDUCE-3090](https://issues.apache.org/jira/browse/MAPREDUCE-3090) | Change MR AM to use ApplicationAttemptId rather than \<applicationId, startCount\> everywhere |  Major | applicationmaster, mrv2 | Arun C Murthy | Arun C Murthy |
| [HADOOP-7668](https://issues.apache.org/jira/browse/HADOOP-7668) | Add a NetUtils method that can tell if an InetAddress belongs to local host |  Minor | util | Suresh Srinivas | Steve Loughran |
| [HDFS-2355](https://issues.apache.org/jira/browse/HDFS-2355) | Federation: enable using the same configuration file across all the nodes in the cluster. |  Major | namenode | Suresh Srinivas | Suresh Srinivas |
| [HDFS-2371](https://issues.apache.org/jira/browse/HDFS-2371) | Refactor BlockSender.java for better readability |  Major | datanode | Suresh Srinivas | Suresh Srinivas |
| [MAPREDUCE-3001](https://issues.apache.org/jira/browse/MAPREDUCE-3001) | Map Reduce JobHistory and AppMaster UI should have ability to display task specific counters. |  Blocker | jobhistoryserver, mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-3113](https://issues.apache.org/jira/browse/MAPREDUCE-3113) | the scripts yarn-daemon.sh and yarn are not working properly |  Minor | mrv2 | XieXianshan | XieXianshan |
| [HADOOP-7710](https://issues.apache.org/jira/browse/HADOOP-7710) | create a script to setup application in order to create root directories for application such hbase, hcat, hive etc |  Major | . | Arpit Gupta | Arpit Gupta |
| [HADOOP-7707](https://issues.apache.org/jira/browse/HADOOP-7707) | improve config generator to allow users to specify proxy user, turn append on or off, turn webhdfs on or off |  Major | conf | Arpit Gupta | Arpit Gupta |
| [HADOOP-7720](https://issues.apache.org/jira/browse/HADOOP-7720) | improve the hadoop-setup-conf.sh to read in the hbase user and setup the configs |  Major | conf | Arpit Gupta | Arpit Gupta |
| [HDFS-2209](https://issues.apache.org/jira/browse/HDFS-2209) | Make MiniDFS easier to embed in other apps |  Minor | test | Steve Loughran | Steve Loughran |
| [MAPREDUCE-3014](https://issues.apache.org/jira/browse/MAPREDUCE-3014) | Rename and invert logic of '-cbuild' profile to 'native' and off by default |  Major | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-7624](https://issues.apache.org/jira/browse/HADOOP-7624) | Set things up for a top level hadoop-tools module |  Major | build | Vinod Kumar Vavilapalli | Alejandro Abdelnur |
| [HDFS-2294](https://issues.apache.org/jira/browse/HDFS-2294) | Download of commons-daemon TAR should not be under target |  Major | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [MAPREDUCE-2802](https://issues.apache.org/jira/browse/MAPREDUCE-2802) | [MR-279] Jobhistory filenames should have jobID to help in better parsing |  Critical | mrv2 | Ramya Sunil | Jonathan Eagles |
| [HADOOP-7627](https://issues.apache.org/jira/browse/HADOOP-7627) | Improve MetricsAsserts to give more understandable output on failure |  Minor | metrics, test | Todd Lipcon | Todd Lipcon |
| [HDFS-2205](https://issues.apache.org/jira/browse/HDFS-2205) | Log message for failed connection to datanode is not followed by a success message. |  Major | hdfs-client | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-3154](https://issues.apache.org/jira/browse/MAPREDUCE-3154) | Validate the Jobs Output Specification as the first statement in JobSubmitter.submitJobInternal(Job, Cluster) method |  Major | client, mrv2 | Abhijit Suresh Shingate | Abhijit Suresh Shingate |
| [MAPREDUCE-3161](https://issues.apache.org/jira/browse/MAPREDUCE-3161) | Improve javadoc and fix some typos in MR2 code |  Minor | mrv2 | Todd Lipcon | Todd Lipcon |
| [HADOOP-7642](https://issues.apache.org/jira/browse/HADOOP-7642) | create hadoop-dist module where TAR stitching would happen |  Major | build | Alejandro Abdelnur | Tom White |
| [MAPREDUCE-3171](https://issues.apache.org/jira/browse/MAPREDUCE-3171) | normalize nodemanager native code compilation with common/hdfs native |  Major | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-7709](https://issues.apache.org/jira/browse/HADOOP-7709) | Running a set of methods in a Single Test Class |  Major | . | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-3133](https://issues.apache.org/jira/browse/MAPREDUCE-3133) | Running a set of methods in a Single Test Class |  Major | build | Jonathan Eagles | Jonathan Eagles |
| [HDFS-2401](https://issues.apache.org/jira/browse/HDFS-2401) | Running a set of methods in a Single Test Class |  Major | build | Jonathan Eagles | Jonathan Eagles |
| [HADOOP-7737](https://issues.apache.org/jira/browse/HADOOP-7737) | normalize hadoop-mapreduce & hadoop-dist dist/tar build with common/hdfs |  Major | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-7743](https://issues.apache.org/jira/browse/HADOOP-7743) | Add Maven profile to create a full source tarball |  Major | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-7509](https://issues.apache.org/jira/browse/HADOOP-7509) | Improve message when Authentication is required |  Trivial | . | Ravi Prakash | Ravi Prakash |
| [HADOOP-7655](https://issues.apache.org/jira/browse/HADOOP-7655) | provide a small validation script that smoke tests the installed cluster |  Major | . | Arpit Gupta | Arpit Gupta |
| [MAPREDUCE-3187](https://issues.apache.org/jira/browse/MAPREDUCE-3187) | Add names for various unnamed threads in MR2 |  Minor | mrv2 | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-3190](https://issues.apache.org/jira/browse/MAPREDUCE-3190) | bin/yarn should barf early if HADOOP\_COMMON\_HOME or HADOOP\_HDFS\_HOME are not set |  Major | mrv2 | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2890](https://issues.apache.org/jira/browse/MAPREDUCE-2890) | Documentation for MRv2 |  Blocker | documentation, mrv2 | Arun C Murthy |  |
| [MAPREDUCE-2894](https://issues.apache.org/jira/browse/MAPREDUCE-2894) | Improvements to YARN apis |  Blocker | mrv2 | Arun C Murthy |  |
| [MAPREDUCE-3189](https://issues.apache.org/jira/browse/MAPREDUCE-3189) | Add link decoration back to MR2's CSS |  Major | mrv2 | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-3162](https://issues.apache.org/jira/browse/MAPREDUCE-3162) | Separate application-init and container-init event types in NM's ApplicationImpl FSM |  Minor | mrv2, nodemanager | Todd Lipcon | Todd Lipcon |
| [HADOOP-7749](https://issues.apache.org/jira/browse/HADOOP-7749) | Add NetUtils call which provides more help in exception messages |  Minor | util | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2629](https://issues.apache.org/jira/browse/MAPREDUCE-2629) | Class loading quirk prevents inner class method compilation |  Minor | task | Eric Caspole | Eric Caspole |
| [MAPREDUCE-3239](https://issues.apache.org/jira/browse/MAPREDUCE-3239) | Use new createSocketAddr API in MRv2 to give better error messages on misconfig |  Minor | mrv2 | Todd Lipcon | Todd Lipcon |
| [HDFS-2485](https://issues.apache.org/jira/browse/HDFS-2485) | Improve code layout and constants in UnderReplicatedBlocks |  Trivial | datanode | Steve Loughran | Steve Loughran |
| [HADOOP-7772](https://issues.apache.org/jira/browse/HADOOP-7772) | javadoc the topology classes |  Trivial | . | Steve Loughran | Steve Loughran |
| [HDFS-2507](https://issues.apache.org/jira/browse/HDFS-2507) | HA: Allow saveNamespace operations to be canceled |  Major | namenode | Todd Lipcon | Todd Lipcon |
| [HDFS-2500](https://issues.apache.org/jira/browse/HDFS-2500) | Avoid file system operations in BPOfferService thread while processing deletes |  Major | datanode | Todd Lipcon | Todd Lipcon |
| [HADOOP-7360](https://issues.apache.org/jira/browse/HADOOP-7360) | FsShell does not preserve relative paths with globs |  Major | fs | Daryn Sharp | Kihwal Lee |
| [HDFS-2465](https://issues.apache.org/jira/browse/HDFS-2465) | Add HDFS support for fadvise readahead and drop-behind |  Major | datanode, performance | Todd Lipcon | Todd Lipcon |
| [HADOOP-7446](https://issues.apache.org/jira/browse/HADOOP-7446) | Implement CRC32C native code using SSE4.2 instructions |  Major | native, performance | Todd Lipcon | Todd Lipcon |
| [HADOOP-7763](https://issues.apache.org/jira/browse/HADOOP-7763) | Add top-level navigation to APT docs |  Major | documentation | Tom White | Tom White |
| [MAPREDUCE-3275](https://issues.apache.org/jira/browse/MAPREDUCE-3275) | Add docs for WebAppProxy |  Critical | documentation, mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [HDFS-2512](https://issues.apache.org/jira/browse/HDFS-2512) | Add textual error message to data transfer protocol responses |  Major | datanode, hdfs-client | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-3322](https://issues.apache.org/jira/browse/MAPREDUCE-3322) | Create a better index.html for maven docs |  Major | documentation, mrv2 | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-3237](https://issues.apache.org/jira/browse/MAPREDUCE-3237) | Move LocalJobRunner to hadoop-mapreduce-client-core module |  Major | client | Tom White | Tom White |
| [HDFS-2521](https://issues.apache.org/jira/browse/HDFS-2521) | Remove custom checksum headers from data transfer protocol |  Major | datanode, hdfs-client | Todd Lipcon | Todd Lipcon |
| [HADOOP-7785](https://issues.apache.org/jira/browse/HADOOP-7785) | Add equals, hashcode, toString to DataChecksum |  Major | io, util | Todd Lipcon | Todd Lipcon |
| [HADOOP-7789](https://issues.apache.org/jira/browse/HADOOP-7789) | Minor edits to top-level site |  Major | . | Arun C Murthy | Arun C Murthy |
| [HADOOP-7664](https://issues.apache.org/jira/browse/HADOOP-7664) | o.a.h.conf.Configuration complains of overriding final parameter even if the value with which its attempting to override is the same. |  Minor | conf | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-2399](https://issues.apache.org/jira/browse/MAPREDUCE-2399) | The embedded web framework for MAPREDUCE-279 |  Major | . | Arun C Murthy | Luke Lu |
| [HADOOP-7328](https://issues.apache.org/jira/browse/HADOOP-7328) | When a serializer class is missing, return null, not throw an NPE. |  Major | io | Harsh J | Harsh J |
| [HDFS-1937](https://issues.apache.org/jira/browse/HDFS-1937) | Umbrella JIRA for improving DataTransferProtocol |  Major | datanode, hdfs-client | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-1378](https://issues.apache.org/jira/browse/HDFS-1378) | Edit log replay should track and report file offsets in case of errors |  Major | namenode | Todd Lipcon | Colin P. McCabe |
| [HADOOP-7209](https://issues.apache.org/jira/browse/HADOOP-7209) | Extensions to FsShell |  Major | . | Olga Natkovich | Daryn Sharp |
| [HADOOP-8619](https://issues.apache.org/jira/browse/HADOOP-8619) | WritableComparator must implement no-arg constructor |  Major | io | Radim Kolar | Chris Douglas |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-7015](https://issues.apache.org/jira/browse/HADOOP-7015) | RawLocalFileSystem#listStatus does not deal with a  directory whose entries are changing ( e.g. in a multi-thread or multi-process environment) |  Minor | . | Sanjay Radia | Sanjay Radia |
| [MAPREDUCE-2172](https://issues.apache.org/jira/browse/MAPREDUCE-2172) | test-patch.properties contains incorrect/version-dependent values of OK\_FINDBUGS\_WARNINGS and OK\_RELEASEAUDIT\_WARNINGS |  Major | . | Patrick Kling | Nigel Daley |
| [HADOOP-7045](https://issues.apache.org/jira/browse/HADOOP-7045) | TestDU fails on systems with local file systems with extended attributes |  Minor | fs | Eli Collins | Eli Collins |
| [HDFS-1001](https://issues.apache.org/jira/browse/HDFS-1001) | DataXceiver and BlockReader disagree on when to send/recv CHECKSUM\_OK |  Minor | datanode | bc Wong | bc Wong |
| [HDFS-1467](https://issues.apache.org/jira/browse/HDFS-1467) | Append pipeline never succeeds with more than one replica |  Blocker | datanode | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2199](https://issues.apache.org/jira/browse/MAPREDUCE-2199) | build is broken 0.22 branch creation |  Major | build | Konstantin Boudnik | Konstantin Boudnik |
| [HDFS-1516](https://issues.apache.org/jira/browse/HDFS-1516) | mvn-install is broken after 0.22 branch creation |  Major | build | Konstantin Boudnik | Konstantin Boudnik |
| [HADOOP-7052](https://issues.apache.org/jira/browse/HADOOP-7052) | misspelling of threshold in conf/log4j.properties |  Major | conf | Jingguo Yao | Jingguo Yao |
| [HADOOP-7053](https://issues.apache.org/jira/browse/HADOOP-7053) | wrong FSNamesystem Audit logging setting in conf/log4j.properties |  Minor | conf | Jingguo Yao | Jingguo Yao |
| [HDFS-1503](https://issues.apache.org/jira/browse/HDFS-1503) | TestSaveNamespace fails |  Minor | test | Eli Collins | Todd Lipcon |
| [HADOOP-7057](https://issues.apache.org/jira/browse/HADOOP-7057) | IOUtils.readFully and IOUtils.skipFully have typo in exception creation's message |  Minor | util | Konstantin Boudnik | Konstantin Boudnik |
| [HDFS-1524](https://issues.apache.org/jira/browse/HDFS-1524) | Image loader should make sure to read every byte in image file |  Blocker | namenode | Hairong Kuang | Hairong Kuang |
| [HDFS-1523](https://issues.apache.org/jira/browse/HDFS-1523) | TestLargeBlock is failing on trunk |  Major | test | Konstantin Boudnik | Konstantin Boudnik |
| [HADOOP-6939](https://issues.apache.org/jira/browse/HADOOP-6939) | Inconsistent lock ordering in AbstractDelegationTokenSecretManager |  Minor | . | Todd Lipcon | Todd Lipcon |
| [HDFS-1502](https://issues.apache.org/jira/browse/HDFS-1502) | TestBlockRecovery triggers NPE in assert |  Minor | . | Eli Collins | Hairong Kuang |
| [HDFS-1533](https://issues.apache.org/jira/browse/HDFS-1533) | A more elegant FileSystem#listCorruptFileBlocks API (HDFS portion) |  Major | hdfs-client | Patrick Kling | Patrick Kling |
| [MAPREDUCE-2215](https://issues.apache.org/jira/browse/MAPREDUCE-2215) | A more elegant FileSystem#listCorruptFileBlocks API (RAID changes) |  Major | contrib/raid | Patrick Kling | Patrick Kling |
| [MAPREDUCE-1334](https://issues.apache.org/jira/browse/MAPREDUCE-1334) | contrib/index - test - TestIndexUpdater fails due to an additional presence of file \_SUCCESS in hdfs |  Major | contrib/index | Karthik K | Karthik K |
| [HDFS-1377](https://issues.apache.org/jira/browse/HDFS-1377) | Quota bug for partial blocks allows quotas to be violated |  Blocker | namenode | Eli Collins | Eli Collins |
| [HDFS-1360](https://issues.apache.org/jira/browse/HDFS-1360) | TestBlockRecovery should bind ephemeral ports |  Minor | test | Todd Lipcon | Todd Lipcon |
| [HDFS-1206](https://issues.apache.org/jira/browse/HDFS-1206) | TestFiHFlush fails intermittently |  Major | test | Tsz Wo Nicholas Sze | Konstantin Boudnik |
| [HDFS-1511](https://issues.apache.org/jira/browse/HDFS-1511) | 98 Release Audit warnings on trunk and branch-0.22 |  Blocker | . | Nigel Daley | Jakob Homan |
| [HDFS-1551](https://issues.apache.org/jira/browse/HDFS-1551) | fix the pom template's version |  Major | build | Giridharan Kesavan | Giridharan Kesavan |
| [HADOOP-7071](https://issues.apache.org/jira/browse/HADOOP-7071) | test-patch.sh has bad ps arg |  Minor | build | Nigel Daley | Nigel Daley |
| [HDFS-1540](https://issues.apache.org/jira/browse/HDFS-1540) | Make Datanode handle errors to namenode.register call more elegantly |  Major | datanode | dhruba borthakur | dhruba borthakur |
| [HDFS-1463](https://issues.apache.org/jira/browse/HDFS-1463) | accessTime updates should not occur in safeMode |  Major | namenode | dhruba borthakur | dhruba borthakur |
| [HADOOP-7101](https://issues.apache.org/jira/browse/HADOOP-7101) | UserGroupInformation.getCurrentUser() fails when called from non-Hadoop JAAS context |  Blocker | security | Todd Lipcon | Todd Lipcon |
| [HADOOP-7089](https://issues.apache.org/jira/browse/HADOOP-7089) | Fix link resolution logic in hadoop-config.sh |  Minor | scripts | Eli Collins | Eli Collins |
| [HDFS-1585](https://issues.apache.org/jira/browse/HDFS-1585) | HDFS-1547 broke MR build |  Blocker | test | Todd Lipcon | Todd Lipcon |
| [HADOOP-7046](https://issues.apache.org/jira/browse/HADOOP-7046) | 1 Findbugs warning on trunk and branch-0.22 |  Blocker | security | Nigel Daley | Po Cheung |
| [MAPREDUCE-2271](https://issues.apache.org/jira/browse/MAPREDUCE-2271) | TestSetupTaskScheduling failing in trunk |  Blocker | jobtracker | Todd Lipcon | Liyin Liang |
| [HADOOP-7120](https://issues.apache.org/jira/browse/HADOOP-7120) | 200 new Findbugs warnings |  Major | test | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-1598](https://issues.apache.org/jira/browse/HDFS-1598) | ListPathsServlet excludes .\*.crc files |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [MAPREDUCE-2290](https://issues.apache.org/jira/browse/MAPREDUCE-2290) | TestTaskCommit missing getProtocolSignature override |  Major | test | Eli Collins | Eli Collins |
| [HDFS-863](https://issues.apache.org/jira/browse/HDFS-863) | Potential deadlock in TestOverReplicatedBlocks |  Major | test | Todd Lipcon | Ken Goodhope |
| [HDFS-1600](https://issues.apache.org/jira/browse/HDFS-1600) | editsStored.xml cause release audit warning |  Major | build, test | Tsz Wo Nicholas Sze | Todd Lipcon |
| [MAPREDUCE-2311](https://issues.apache.org/jira/browse/MAPREDUCE-2311) | TestFairScheduler failing on trunk |  Blocker | contrib/fair-share | Todd Lipcon | Scott Chen |
| [HDFS-1602](https://issues.apache.org/jira/browse/HDFS-1602) | NameNode storage failed replica restoration is broken |  Major | namenode | Konstantin Boudnik | Boris Shkolnik |
| [HADOOP-7151](https://issues.apache.org/jira/browse/HADOOP-7151) | Document need for stable hashCode() in WritableComparable |  Minor | . | Dmitriy V. Ryaboy | Dmitriy V. Ryaboy |
| [HDFS-1612](https://issues.apache.org/jira/browse/HDFS-1612) | HDFS Design Documentation is outdated |  Minor | documentation | Joe Crobak | Joe Crobak |
| [MAPREDUCE-1996](https://issues.apache.org/jira/browse/MAPREDUCE-1996) | API: Reducer.reduce() method detail misstatement |  Trivial | documentation | Glynn Durham | Harsh J |
| [MAPREDUCE-2074](https://issues.apache.org/jira/browse/MAPREDUCE-2074) | Task should fail when symlink creation fail |  Minor | distributed-cache | Koji Noguchi | Priyo Mustafi |
| [HDFS-1625](https://issues.apache.org/jira/browse/HDFS-1625) | TestDataNodeMXBean fails if disk space usage changes during test run |  Minor | test | Todd Lipcon | Tsz Wo Nicholas Sze |
| [MAPREDUCE-1811](https://issues.apache.org/jira/browse/MAPREDUCE-1811) | Job.monitorAndPrintJob() should print status of the job at completion |  Minor | client | Amareshwari Sriramadasu | Harsh J |
| [MAPREDUCE-993](https://issues.apache.org/jira/browse/MAPREDUCE-993) | bin/hadoop job -events \<jobid\> \<from-event-#\> \<#-of-events\> help message is confusing |  Minor | jobtracker | Iyappan Srinivasan | Harsh J |
| [HADOOP-6754](https://issues.apache.org/jira/browse/HADOOP-6754) | DefaultCodec.createOutputStream() leaks memory |  Major | io | Aaron Kimball | Aaron Kimball |
| [HDFS-1691](https://issues.apache.org/jira/browse/HDFS-1691) | double static declaration in Configuration.addDefaultResource("hdfs-default.xml"); |  Minor | tools | Alexey Diomin | Alexey Diomin |
| [MAPREDUCE-1242](https://issues.apache.org/jira/browse/MAPREDUCE-1242) | Chain APIs error misleading |  Trivial | . | Amogh Vasekar | Harsh J |
| [HADOOP-7098](https://issues.apache.org/jira/browse/HADOOP-7098) | tasktracker property not set in conf/hadoop-env.sh |  Major | conf | Bernd Fondermann | Bernd Fondermann |
| [HADOOP-7162](https://issues.apache.org/jira/browse/HADOOP-7162) | FsShell: call srcFs.listStatus(src) twice |  Minor | fs | Alexey Diomin | Alexey Diomin |
| [HDFS-1665](https://issues.apache.org/jira/browse/HDFS-1665) | Balancer sleeps inadequately |  Minor | balancer & mover | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-1728](https://issues.apache.org/jira/browse/HDFS-1728) | SecondaryNameNode.checkpointSize is in byte but not MB. |  Minor | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-6912](https://issues.apache.org/jira/browse/HADOOP-6912) | Guard against NPE when calling UGI.isLoginKeytabBased() |  Major | security | Kan Zhang | Kan Zhang |
| [HDFS-1748](https://issues.apache.org/jira/browse/HDFS-1748) | Balancer utilization classification is incomplete |  Major | balancer & mover | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [MAPREDUCE-2379](https://issues.apache.org/jira/browse/MAPREDUCE-2379) | Distributed cache sizing configurations are missing from mapred-default.xml |  Major | distributed-cache, documentation | Todd Lipcon | Todd Lipcon |
| [HADOOP-7175](https://issues.apache.org/jira/browse/HADOOP-7175) | Add isEnabled() to Trash |  Major | fs | Daryn Sharp | Daryn Sharp |
| [HDFS-1750](https://issues.apache.org/jira/browse/HDFS-1750) | fs -ls hftp://file not working |  Major | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-7174](https://issues.apache.org/jira/browse/HADOOP-7174) | null is displayed in the console,if the src path is invalid while doing copyToLocal operation from commandLine |  Minor | fs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-7187](https://issues.apache.org/jira/browse/HADOOP-7187) | Socket Leak in org.apache.hadoop.metrics.ganglia.GangliaContext |  Major | metrics | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-7194](https://issues.apache.org/jira/browse/HADOOP-7194) | Potential Resource leak in IOUtils.java |  Major | io | Devaraj K | Devaraj K |
| [HDFS-1797](https://issues.apache.org/jira/browse/HDFS-1797) | New findbugs warning introduced by HDFS-1120 |  Major | . | Todd Lipcon | Todd Lipcon |
| [HDFS-1781](https://issues.apache.org/jira/browse/HDFS-1781) | jsvc executable delivered into wrong package... |  Major | scripts | John George | John George |
| [HDFS-1782](https://issues.apache.org/jira/browse/HDFS-1782) | FSNamesystem.startFileInternal(..) throws NullPointerException |  Major | namenode | John George | John George |
| [HADOOP-7210](https://issues.apache.org/jira/browse/HADOOP-7210) | Chown command is not working from FSShell. |  Major | fs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-1611](https://issues.apache.org/jira/browse/HDFS-1611) | Some logical issues need to address. |  Minor | hdfs-client, namenode | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-1189](https://issues.apache.org/jira/browse/HDFS-1189) | Quota counts missed between clear quota and set quota |  Major | namenode | Kang Xiao | John George |
| [MAPREDUCE-2307](https://issues.apache.org/jira/browse/MAPREDUCE-2307) | Exception thrown in Jobtracker logs, when the Scheduler configured is FairScheduler. |  Minor | contrib/fair-share | Devaraj K | Devaraj K |
| [MAPREDUCE-2395](https://issues.apache.org/jira/browse/MAPREDUCE-2395) | TestBlockFixer timing out on trunk |  Critical | contrib/raid | Todd Lipcon | Ramkumar Vadali |
| [HDFS-1818](https://issues.apache.org/jira/browse/HDFS-1818) | TestHDFSCLI is failing on trunk |  Major | test | Aaron T. Myers | Aaron T. Myers |
| [HADOOP-7216](https://issues.apache.org/jira/browse/HADOOP-7216) | HADOOP-7202 broke TestDFSShell in HDFS |  Major | test | Aaron T. Myers | Daryn Sharp |
| [HDFS-1760](https://issues.apache.org/jira/browse/HDFS-1760) | problems with getFullPathName |  Major | namenode | Daryn Sharp | Daryn Sharp |
| [HDFS-1821](https://issues.apache.org/jira/browse/HDFS-1821) | FileContext.createSymlink with kerberos enabled sets wrong owner |  Major | . | John George | John George |
| [MAPREDUCE-2439](https://issues.apache.org/jira/browse/MAPREDUCE-2439) | MR-279: Fix YarnRemoteException to give more details. |  Major | mrv2 | Mahadev konar | Siddharth Seth |
| [HADOOP-7223](https://issues.apache.org/jira/browse/HADOOP-7223) | FileContext createFlag combinations during create are not clearly defined |  Major | fs | Suresh Srinivas | Suresh Srinivas |
| [HADOOP-7171](https://issues.apache.org/jira/browse/HADOOP-7171) | Support UGI in FileContext API |  Major | security | Owen O'Malley | Jitendra Nath Pandey |
| [MAPREDUCE-2440](https://issues.apache.org/jira/browse/MAPREDUCE-2440) | MR-279: Name clashes in TypeConverter |  Major | mrv2 | Luke Lu | Luke Lu |
| [HADOOP-7231](https://issues.apache.org/jira/browse/HADOOP-7231) | Fix synopsis for -count |  Major | util | Daryn Sharp | Daryn Sharp |
| [HDFS-1824](https://issues.apache.org/jira/browse/HDFS-1824) | delay instantiation of file system object until it is needed (linked to HADOOP-7207) |  Major | . | Boris Shkolnik | Boris Shkolnik |
| [HADOOP-7207](https://issues.apache.org/jira/browse/HADOOP-7207) | fs member of FSShell is not really needed |  Major | . | Boris Shkolnik | Boris Shkolnik |
| [HDFS-1806](https://issues.apache.org/jira/browse/HDFS-1806) | TestBlockReport.blockReport\_08() and \_09() are timing-dependent and likely to fail on fast servers |  Major | datanode, namenode | Matt Foley | Matt Foley |
| [HDFS-1845](https://issues.apache.org/jira/browse/HDFS-1845) | symlink comes up as directory after namenode restart |  Major | . | John George | John George |
| [HADOOP-7172](https://issues.apache.org/jira/browse/HADOOP-7172) | SecureIO should not check owner on non-secure clusters that have no native support |  Critical | io, security | Todd Lipcon | Todd Lipcon |
| [HDFS-1594](https://issues.apache.org/jira/browse/HDFS-1594) | When the disk becomes full Namenode is getting shutdown and not able to recover |  Major | namenode | Devaraj K | Aaron T. Myers |
| [MAPREDUCE-2317](https://issues.apache.org/jira/browse/MAPREDUCE-2317) | HadoopArchives throwing NullPointerException while creating hadoop archives (.har files) |  Minor | harchive | Devaraj K | Devaraj K |
| [HDFS-1823](https://issues.apache.org/jira/browse/HDFS-1823) | start-dfs.sh script fails if HADOOP\_HOME is not set |  Blocker | scripts | Tom White | Tom White |
| [MAPREDUCE-2428](https://issues.apache.org/jira/browse/MAPREDUCE-2428) | start-mapred.sh script fails if HADOOP\_HOME is not set |  Blocker | . | Tom White | Tom White |
| [HDFS-1808](https://issues.apache.org/jira/browse/HDFS-1808) | TestBalancer waits forever, errs without giving information |  Major | datanode, namenode | Matt Foley | Matt Foley |
| [HDFS-1829](https://issues.apache.org/jira/browse/HDFS-1829) | TestNodeCount waits forever, errs without giving information |  Major | namenode | Matt Foley | Matt Foley |
| [HDFS-1822](https://issues.apache.org/jira/browse/HDFS-1822) | Editlog opcodes overlap between 20 security and later releases |  Blocker | namenode | Suresh Srinivas | Suresh Srinivas |
| [MAPREDUCE-2417](https://issues.apache.org/jira/browse/MAPREDUCE-2417) | In Gridmix, in RoundRobinUserResolver mode, the testing/proxy users are not associated with unique users in a trace |  Major | contrib/gridmix | Ravi Gummadi | Ravi Gummadi |
| [MAPREDUCE-2433](https://issues.apache.org/jira/browse/MAPREDUCE-2433) | MR-279: YARNApplicationConstants hard code app master jar version |  Blocker | mrv2 | Luke Lu | Mahadev konar |
| [MAPREDUCE-2458](https://issues.apache.org/jira/browse/MAPREDUCE-2458) | MR-279: Rename sanitized pom.xml in build directory to work around IDE bug |  Major | mrv2 | Luke Lu | Luke Lu |
| [MAPREDUCE-2416](https://issues.apache.org/jira/browse/MAPREDUCE-2416) | In Gridmix, in RoundRobinUserResolver, the list of groups for a user obtained from users-list-file is incorrect |  Major | contrib/gridmix | Ravi Gummadi | Ravi Gummadi |
| [HDFS-1871](https://issues.apache.org/jira/browse/HDFS-1871) | Tests using MiniDFSCluster fail to compile due to HDFS-1052 changes |  Major | test | Suresh Srinivas | Suresh Srinivas |
| [MAPREDUCE-2460](https://issues.apache.org/jira/browse/MAPREDUCE-2460) | TestFairSchedulerSystem failing on Hudson |  Blocker | . | Todd Lipcon | Todd Lipcon |
| [HDFS-1876](https://issues.apache.org/jira/browse/HDFS-1876) | One MiniDFSCluster ignores numDataNodes parameter |  Blocker | . | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2466](https://issues.apache.org/jira/browse/MAPREDUCE-2466) | TestFileInputFormat.testLocality failing after federation merge |  Blocker | . | Todd Lipcon | Todd Lipcon |
| [HDFS-1888](https://issues.apache.org/jira/browse/HDFS-1888) | MiniDFSCluster#corruptBlockOnDatanodes() access must be public for MapReduce contrib raid |  Major | . | Suresh Srinivas | Suresh Srinivas |
| [HDFS-1889](https://issues.apache.org/jira/browse/HDFS-1889) | incorrect path in start/stop dfs script |  Major | . | John George | John George |
| [HDFS-1898](https://issues.apache.org/jira/browse/HDFS-1898) | Tests failing on trunk due to use of NameNode.format |  Critical | . | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2475](https://issues.apache.org/jira/browse/MAPREDUCE-2475) | Disable IPV6 for junit tests |  Major | test | Suresh Srinivas | Suresh Srinivas |
| [HADOOP-7261](https://issues.apache.org/jira/browse/HADOOP-7261) | Disable IPV6 for junit tests |  Major | test | Suresh Srinivas | Suresh Srinivas |
| [HDFS-1827](https://issues.apache.org/jira/browse/HDFS-1827) | TestBlockReplacement waits forever, errs without giving information |  Major | namenode | Matt Foley | Matt Foley |
| [MAPREDUCE-2451](https://issues.apache.org/jira/browse/MAPREDUCE-2451) | Log the reason string of healthcheck script |  Trivial | jobtracker | Thomas Graves | Thomas Graves |
| [HADOOP-7268](https://issues.apache.org/jira/browse/HADOOP-7268) | FileContext.getLocalFSFileContext() behavior needs to be fixed w.r.t tokens |  Major | fs, security | Devaraj Das | Jitendra Nath Pandey |
| [MAPREDUCE-2480](https://issues.apache.org/jira/browse/MAPREDUCE-2480) | MR-279: mr app should not depend on hard-coded version of shuffle |  Major | mrv2 | Luke Lu | Luke Lu |
| [HDFS-1908](https://issues.apache.org/jira/browse/HDFS-1908) | DataTransferTestUtil$CountdownDoosAction.run(..) throws NullPointerException |  Minor | test | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [MAPREDUCE-2467](https://issues.apache.org/jira/browse/MAPREDUCE-2467) | HDFS-1052 changes break the raid contrib module in MapReduce |  Major | contrib/raid | Suresh Srinivas | Suresh Srinivas |
| [HADOOP-7055](https://issues.apache.org/jira/browse/HADOOP-7055) | Update of commons logging libraries causes EventCounter to count logging events incorrectly |  Major | metrics | Jingguo Yao | Jingguo Yao |
| [HDFS-1627](https://issues.apache.org/jira/browse/HDFS-1627) | Fix NullPointerException in Secondary NameNode |  Major | namenode | Hairong Kuang | Hairong Kuang |
| [HDFS-1917](https://issues.apache.org/jira/browse/HDFS-1917) | Clean up duplication of dependent jar files |  Major | build | Eric Yang | Eric Yang |
| [HDFS-1938](https://issues.apache.org/jira/browse/HDFS-1938) |  Reference ivy-hdfs.classpath not found. |  Minor | build | Tsz Wo Nicholas Sze | Eric Yang |
| [HDFS-1835](https://issues.apache.org/jira/browse/HDFS-1835) | DataNode.setNewStorageID pulls entropy from /dev/random |  Major | datanode | John Carrino | John Carrino |
| [HADOOP-7292](https://issues.apache.org/jira/browse/HADOOP-7292) | Metrics 2 TestSinkQueue is racy |  Minor | metrics | Luke Lu | Luke Lu |
| [HDFS-1914](https://issues.apache.org/jira/browse/HDFS-1914) | Federation: namenode storage directory must be configurable specific to a namenode |  Major | namenode | Suresh Srinivas | Suresh Srinivas |
| [HDFS-1881](https://issues.apache.org/jira/browse/HDFS-1881) | Federation: after taking snapshot the current directory of datanode is empty |  Major | datanode | Tanping Wang | Tanping Wang |
| [HADOOP-7282](https://issues.apache.org/jira/browse/HADOOP-7282) | getRemoteIp could return null in cases where the call is ongoing but the ip went away. |  Major | ipc | John George | John George |
| [MAPREDUCE-2497](https://issues.apache.org/jira/browse/MAPREDUCE-2497) | missing spaces in error messages |  Trivial | . | Robert Henry | Eli Collins |
| [MAPREDUCE-2500](https://issues.apache.org/jira/browse/MAPREDUCE-2500) | MR 279: PB factories are not thread safe |  Major | mrv2 | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-2504](https://issues.apache.org/jira/browse/MAPREDUCE-2504) | MR 279: race in JobHistoryEventHandler stop |  Major | mrv2 | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-2509](https://issues.apache.org/jira/browse/MAPREDUCE-2509) | MR-279: Fix NPE in UI for pending attempts |  Major | mrv2 | Luke Lu | Luke Lu |
| [HDFS-1505](https://issues.apache.org/jira/browse/HDFS-1505) | saveNamespace appears to succeed even if all directories fail to save |  Blocker | . | Todd Lipcon | Aaron T. Myers |
| [HDFS-1927](https://issues.apache.org/jira/browse/HDFS-1927) | audit logs could ignore certain xsactions and also could contain "ip=null" |  Major | namenode | John George | John George |
| [MAPREDUCE-2258](https://issues.apache.org/jira/browse/MAPREDUCE-2258) | IFile reader closes stream and compressor in wrong order |  Major | task | Todd Lipcon | Todd Lipcon |
| [HDFS-1953](https://issues.apache.org/jira/browse/HDFS-1953) | Change name node mxbean name in cluster web console |  Minor | . | Tanping Wang | Tanping Wang |
| [MAPREDUCE-2483](https://issues.apache.org/jira/browse/MAPREDUCE-2483) | Clean up duplication of dependent jar files |  Major | build | Eric Yang | Eric Yang |
| [HDFS-1905](https://issues.apache.org/jira/browse/HDFS-1905) | Improve the usability of namenode -format |  Minor | namenode | Bharath Mundlapudi | Bharath Mundlapudi |
| [HDFS-1371](https://issues.apache.org/jira/browse/HDFS-1371) | One bad node can incorrectly flag many files as corrupt |  Major | hdfs-client, namenode | Koji Noguchi | Tanping Wang |
| [MAPREDUCE-2518](https://issues.apache.org/jira/browse/MAPREDUCE-2518) | missing t flag in distcp help message '-p[rbugp]' |  Major | distcp | Wei Yongjun | Wei Yongjun |
| [MAPREDUCE-2514](https://issues.apache.org/jira/browse/MAPREDUCE-2514) | ReinitTrackerAction class name misspelled RenitTrackerAction in task tracker log |  Trivial | tasktracker | Jonathan Eagles | Jonathan Eagles |
| [HDFS-1921](https://issues.apache.org/jira/browse/HDFS-1921) | Save namespace can cause NN to be unable to come up on restart |  Blocker | . | Aaron T. Myers | Matt Foley |
| [HADOOP-6508](https://issues.apache.org/jira/browse/HADOOP-6508) | Incorrect values for metrics with CompositeContext |  Major | metrics | Amareshwari Sriramadasu | Luke Lu |
| [HADOOP-7287](https://issues.apache.org/jira/browse/HADOOP-7287) | Configuration deprecation mechanism doesn't work properly for GenericOptionsParser/Tools |  Blocker | conf | Todd Lipcon | Aaron T. Myers |
| [HADOOP-7259](https://issues.apache.org/jira/browse/HADOOP-7259) | contrib modules should include build.properties from parent. |  Major | build | Owen O'Malley | Owen O'Malley |
| [HADOOP-7215](https://issues.apache.org/jira/browse/HADOOP-7215) | RPC clients must connect over a network interface corresponding to the host name in the client's kerberos principal key |  Blocker | security | Suresh Srinivas | Suresh Srinivas |
| [HADOOP-7258](https://issues.apache.org/jira/browse/HADOOP-7258) | Gzip codec should not return null decompressors |  Major | . | Owen O'Malley | Owen O'Malley |
| [MAPREDUCE-2534](https://issues.apache.org/jira/browse/MAPREDUCE-2534) | MR-279: Fix CI breaking hard coded version in jobclient pom |  Major | mrv2 | Luke Lu | Luke Lu |
| [MAPREDUCE-2470](https://issues.apache.org/jira/browse/MAPREDUCE-2470) | Receiving NPE occasionally on RunningJob.getCounters() call |  Major | client | Aaron Baff | Robert Joseph Evans |
| [HADOOP-7322](https://issues.apache.org/jira/browse/HADOOP-7322) | Adding a util method in FileUtil for JDK File.listFiles |  Minor | . | Bharath Mundlapudi | Bharath Mundlapudi |
| [HDFS-1964](https://issues.apache.org/jira/browse/HDFS-1964) | Incorrect HTML unescaping in DatanodeJspHelper.java |  Major | . | Aaron T. Myers | Aaron T. Myers |
| [HDFS-1999](https://issues.apache.org/jira/browse/HDFS-1999) | Tests use deprecated configs |  Major | test | Aaron T. Myers | Aaron T. Myers |
| [HDFS-1592](https://issues.apache.org/jira/browse/HDFS-1592) | Datanode startup doesn't honor volumes.tolerated |  Major | . | Bharath Mundlapudi | Bharath Mundlapudi |
| [HDFS-1812](https://issues.apache.org/jira/browse/HDFS-1812) | Address the cleanup issues in TestHDFSCLI.java |  Minor | test | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [MAPREDUCE-2137](https://issues.apache.org/jira/browse/MAPREDUCE-2137) | Mapping between Gridmix jobs and the corresponding original MR jobs is needed |  Major | contrib/gridmix | Ravi Gummadi | Ravi Gummadi |
| [HDFS-1920](https://issues.apache.org/jira/browse/HDFS-1920) | libhdfs does not build for ARM processors |  Major | libhdfs | Trevor Robinson | Trevor Robinson |
| [HADOOP-7276](https://issues.apache.org/jira/browse/HADOOP-7276) | Hadoop native builds fail on ARM due to -m32 |  Major | native | Trevor Robinson | Trevor Robinson |
| [HDFS-1727](https://issues.apache.org/jira/browse/HDFS-1727) | fsck command can display command usage if user passes any illegal argument |  Minor | . | Uma Maheswara Rao G | sravankorumilli |
| [HADOOP-7208](https://issues.apache.org/jira/browse/HADOOP-7208) | equals() and hashCode() implementation need to change in StandardSocketFactory |  Major | . | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-7336](https://issues.apache.org/jira/browse/HADOOP-7336) | TestFileContextResolveAfs will fail with default test.build.data property. |  Minor | . | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [MAPREDUCE-2552](https://issues.apache.org/jira/browse/MAPREDUCE-2552) | MR 279: NPE when requesting attemptids for completed jobs |  Minor | mrv2 | Siddharth Seth | Siddharth Seth |
| [HDFS-2021](https://issues.apache.org/jira/browse/HDFS-2021) | TestWriteRead failed with inconsistent visible length of a file |  Major | datanode | CW Chung | John George |
| [MAPREDUCE-2556](https://issues.apache.org/jira/browse/MAPREDUCE-2556) | MR 279: NodeStatus.getNodeHealthStatus().setBlah broken |  Major | mrv2 | Siddharth Seth | Siddharth Seth |
| [HDFS-2020](https://issues.apache.org/jira/browse/HDFS-2020) | TestDFSUpgradeFromImage fails |  Major | datanode, test | Suresh Srinivas | Suresh Srinivas |
| [HDFS-1936](https://issues.apache.org/jira/browse/HDFS-1936) | Updating the layout version from HDFS-1822 causes upgrade problems. |  Blocker | namenode | Suresh Srinivas | Suresh Srinivas |
| [HDFS-2022](https://issues.apache.org/jira/browse/HDFS-2022) | ant binary should build libhdfs |  Major | build | Eli Collins | Eric Yang |
| [HDFS-2014](https://issues.apache.org/jira/browse/HDFS-2014) | bin/hdfs no longer works from a source checkout |  Critical | scripts | Todd Lipcon | Eric Yang |
| [MAPREDUCE-2537](https://issues.apache.org/jira/browse/MAPREDUCE-2537) | MR-279: The RM writes its log to yarn-mapred-resourcemanager-\<RM\_Host\>.out |  Minor | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [HDFS-1986](https://issues.apache.org/jira/browse/HDFS-1986) | Add an option for user to return http or https ports regardless of security is on/off in DFSUtil.getInfoServer() |  Minor | tools | Tanping Wang | Tanping Wang |
| [HDFS-1934](https://issues.apache.org/jira/browse/HDFS-1934) | Fix NullPointerException when File.listFiles() API returns null |  Major | . | Bharath Mundlapudi | Bharath Mundlapudi |
| [HDFS-1907](https://issues.apache.org/jira/browse/HDFS-1907) | BlockMissingException upon concurrent read and write: reader was doing file position read while writer is doing write without hflush |  Major | hdfs-client | CW Chung | John George |
| [HADOOP-7284](https://issues.apache.org/jira/browse/HADOOP-7284) | Trash and shell's rm does not work for viewfs |  Major | viewfs | Sanjay Radia | Sanjay Radia |
| [HADOOP-7342](https://issues.apache.org/jira/browse/HADOOP-7342) | Add an utility API in FileUtil for JDK File.list |  Minor | . | Bharath Mundlapudi | Bharath Mundlapudi |
| [HDFS-2019](https://issues.apache.org/jira/browse/HDFS-2019) | Fix all the places where Java method File.list is used with FileUtil.list API |  Minor | datanode | Bharath Mundlapudi | Bharath Mundlapudi |
| [MAPREDUCE-2529](https://issues.apache.org/jira/browse/MAPREDUCE-2529) | Recognize Jetty bug 1342 and handle it |  Major | tasktracker | Thomas Graves | Thomas Graves |
| [HADOOP-7341](https://issues.apache.org/jira/browse/HADOOP-7341) | Fix option parsing in CommandFormat |  Major | fs | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-2104](https://issues.apache.org/jira/browse/MAPREDUCE-2104) | Rumen TraceBuilder Does Not Emit CPU/Memory Usage Details in Traces |  Major | tools/rumen | Ranjit Mathew | Amar Kamat |
| [HADOOP-7353](https://issues.apache.org/jira/browse/HADOOP-7353) | Cleanup FsShell and prevent masking of RTE stacktraces |  Major | fs | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-2185](https://issues.apache.org/jira/browse/MAPREDUCE-2185) | Infinite loop at creating splits using CombineFileInputFormat |  Major | job submission | Hairong Kuang | Ramkumar Vadali |
| [HDFS-1149](https://issues.apache.org/jira/browse/HDFS-1149) | Lease reassignment is not persisted to edit log |  Major | namenode | Todd Lipcon | Aaron T. Myers |
| [HDFS-1998](https://issues.apache.org/jira/browse/HDFS-1998) | make refresh-namodenodes.sh refreshing all namenodes |  Minor | scripts | Tanping Wang | Tanping Wang |
| [HDFS-1875](https://issues.apache.org/jira/browse/HDFS-1875) | MiniDFSCluster hard-codes dfs.datanode.address to localhost |  Major | test | Eric Payne | Eric Payne |
| [MAPREDUCE-2559](https://issues.apache.org/jira/browse/MAPREDUCE-2559) | ant binary fails due to missing c++ lib dir |  Major | build | Eric Yang | Eric Yang |
| [HADOOP-5647](https://issues.apache.org/jira/browse/HADOOP-5647) | TestJobHistory fails if /tmp/\_logs is not writable to. Testcase should not depend on /tmp |  Major | test | Ravi Gummadi | Ravi Gummadi |
| [MAPREDUCE-2566](https://issues.apache.org/jira/browse/MAPREDUCE-2566) | MR 279: YarnConfiguration should reloadConfiguration if instantiated with a non YarnConfiguration object |  Major | mrv2 | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-2573](https://issues.apache.org/jira/browse/MAPREDUCE-2573) | New findbugs warning after MAPREDUCE-2494 |  Major | . | Todd Lipcon | Robert Joseph Evans |
| [HDFS-2030](https://issues.apache.org/jira/browse/HDFS-2030) | Fix the usability of namenode upgrade command |  Minor | . | Bharath Mundlapudi | Bharath Mundlapudi |
| [MAPREDUCE-2581](https://issues.apache.org/jira/browse/MAPREDUCE-2581) | Spelling errors in log messages (MapTask) |  Trivial | . | Dave Syer | Tim Sell |
| [MAPREDUCE-2582](https://issues.apache.org/jira/browse/MAPREDUCE-2582) | MR 279: Cleanup JobHistory event generation |  Major | mrv2 | Siddharth Seth | Siddharth Seth |
| [HDFS-2041](https://issues.apache.org/jira/browse/HDFS-2041) | Some mtimes and atimes are lost when edit logs are replayed |  Major | . | Todd Lipcon | Todd Lipcon |
| [HDFS-1786](https://issues.apache.org/jira/browse/HDFS-1786) | Some cli test cases expect a "null" message |  Minor | test | Tsz Wo Nicholas Sze | Uma Maheswara Rao G |
| [HDFS-2061](https://issues.apache.org/jira/browse/HDFS-2061) | two minor bugs in BlockManager block report processing |  Minor | namenode | Matt Foley | Matt Foley |
| [MAPREDUCE-587](https://issues.apache.org/jira/browse/MAPREDUCE-587) | Stream test TestStreamingExitStatus fails with Out of Memory |  Minor | contrib/streaming | Steve Loughran | Amar Kamat |
| [HADOOP-7383](https://issues.apache.org/jira/browse/HADOOP-7383) | HDFS needs to export protobuf library dependency in pom |  Blocker | build | Todd Lipcon | Todd Lipcon |
| [HDFS-2067](https://issues.apache.org/jira/browse/HDFS-2067) | Bump DATA\_TRANSFER\_VERSION in trunk for protobufs |  Major | datanode, hdfs-client | Todd Lipcon | Tsz Wo Nicholas Sze |
| [MAPREDUCE-2588](https://issues.apache.org/jira/browse/MAPREDUCE-2588) | Raid is not compile after DataTransferProtocol refactoring |  Major | contrib/raid | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-7389](https://issues.apache.org/jira/browse/HADOOP-7389) | Use of TestingGroups by tests causes subsequent tests to fail |  Major | test | Aaron T. Myers | Aaron T. Myers |
| [MAPREDUCE-2569](https://issues.apache.org/jira/browse/MAPREDUCE-2569) | MR-279: Restarting resource manager with root capacity not equal to 100 percent should result in error |  Minor | mrv2 | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-2595](https://issues.apache.org/jira/browse/MAPREDUCE-2595) | MR279: update yarn INSTALL doc |  Minor | . | Thomas Graves | Thomas Graves |
| [HADOOP-7377](https://issues.apache.org/jira/browse/HADOOP-7377) | Fix command name handling affecting DFSAdmin |  Major | fs | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-2598](https://issues.apache.org/jira/browse/MAPREDUCE-2598) | MR 279: miscellaneous UI, NPE fixes for JobHistory, UI |  Minor | mrv2 | Siddharth Seth | Siddharth Seth |
| [HDFS-1952](https://issues.apache.org/jira/browse/HDFS-1952) | FSEditLog.open() appears to succeed even if all EDITS directories fail |  Major | . | Matt Foley | Andrew |
| [MAPREDUCE-2576](https://issues.apache.org/jira/browse/MAPREDUCE-2576) | Typo in comment in SimulatorLaunchTaskAction.java |  Trivial | . | Sherry Chen | Tim Sell |
| [HDFS-1656](https://issues.apache.org/jira/browse/HDFS-1656) | getDelegationToken in HftpFileSystem should renew TGT if needed. |  Major | . | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [HDFS-1692](https://issues.apache.org/jira/browse/HDFS-1692) | In secure mode, Datanode process doesn't exit when disks fail. |  Major | datanode | Bharath Mundlapudi | Bharath Mundlapudi |
| [HDFS-1734](https://issues.apache.org/jira/browse/HDFS-1734) | 'Chunk size to view' option is not working in Name Node UI. |  Major | namenode | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-7402](https://issues.apache.org/jira/browse/HADOOP-7402) | TestConfiguration doesn't clean up after itself |  Trivial | test | Aaron T. Myers | Aaron T. Myers |
| [MAPREDUCE-2603](https://issues.apache.org/jira/browse/MAPREDUCE-2603) | Gridmix system tests are failing due to high ram emulation enable by default for normal mr jobs in the trace which exceeds the solt capacity. |  Major | contrib/gridmix | Vinay Kumar Thota | Vinay Kumar Thota |
| [MAPREDUCE-2615](https://issues.apache.org/jira/browse/MAPREDUCE-2615) | MR 279: KillJob should go through AM whenever possible |  Major | mrv2 | Siddharth Seth | Siddharth Seth |
| [HDFS-2086](https://issues.apache.org/jira/browse/HDFS-2086) | If the include hosts list contains host name, after restarting namenode, datanodes registrant is denied |  Major | namenode | Tanping Wang | Tanping Wang |
| [HDFS-2092](https://issues.apache.org/jira/browse/HDFS-2092) | Create a light inner conf class in DFSClient |  Major | hdfs-client | Bharath Mundlapudi | Bharath Mundlapudi |
| [HADOOP-7385](https://issues.apache.org/jira/browse/HADOOP-7385) | Remove StringUtils.stringifyException(ie) in logger functions |  Minor | . | Bharath Mundlapudi | Bharath Mundlapudi |
| [HDFS-2082](https://issues.apache.org/jira/browse/HDFS-2082) | SecondaryNameNode web interface doesn't show the right info |  Major | . | Aaron T. Myers | Aaron T. Myers |
| [HDFS-1321](https://issues.apache.org/jira/browse/HDFS-1321) | If service port and main port are the same, there is no clear log message explaining the issue. |  Minor | namenode | gary murry | Jim Plush |
| [MAPREDUCE-2618](https://issues.apache.org/jira/browse/MAPREDUCE-2618) | MR-279: 0 map, 0 reduce job fails with Null Pointer Exception |  Major | mrv2 | Jeffrey Naisbitt | Jeffrey Naisbitt |
| [MAPREDUCE-2625](https://issues.apache.org/jira/browse/MAPREDUCE-2625) | MR-279: Add Node Manager Version to NM info page |  Minor | mrv2 | Jonathan Eagles | Jonathan Eagles |
| [HADOOP-7428](https://issues.apache.org/jira/browse/HADOOP-7428) | IPC connection is orphaned with null 'out' member |  Major | ipc | Todd Lipcon | Todd Lipcon |
| [HDFS-1955](https://issues.apache.org/jira/browse/HDFS-1955) | FSImage.doUpgrade() was made too fault-tolerant by HDFS-1826 |  Major | namenode | Matt Foley | Matt Foley |
| [HDFS-2011](https://issues.apache.org/jira/browse/HDFS-2011) | Removal and restoration of storage directories on checkpointing failure doesn't work properly |  Major | namenode | Ravi Prakash | Ravi Prakash |
| [HDFS-2109](https://issues.apache.org/jira/browse/HDFS-2109) | Store uMask as member variable to DFSClient.Conf |  Major | hdfs-client | Bharath Mundlapudi | Bharath Mundlapudi |
| [HADOOP-7437](https://issues.apache.org/jira/browse/HADOOP-7437) | IOUtils.copybytes will suppress the stream closure exceptions. |  Major | io | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-1990](https://issues.apache.org/jira/browse/HDFS-1990) | Resource leaks in HDFS |  Minor | datanode | ramkrishna.s.vasudevan | Uma Maheswara Rao G |
| [HADOOP-7090](https://issues.apache.org/jira/browse/HADOOP-7090) | Possible resource leaks in hadoop core code |  Major | fs/s3, io | Gokul | Uma Maheswara Rao G |
| [HADOOP-7440](https://issues.apache.org/jira/browse/HADOOP-7440) | HttpServer.getParameterValues throws NPE for missing parameters |  Major | . | Todd Lipcon | Todd Lipcon |
| [HADOOP-7442](https://issues.apache.org/jira/browse/HADOOP-7442) | Docs in core-default.xml still reference deprecated config "topology.script.file.name" |  Major | conf, documentation | Aaron T. Myers | Aaron T. Myers |
| [HADOOP-7419](https://issues.apache.org/jira/browse/HADOOP-7419) | new hadoop-config.sh doesn't manage classpath for HADOOP\_CONF\_DIR correctly |  Major | . | Todd Lipcon | Bing Zheng |
| [HADOOP-7327](https://issues.apache.org/jira/browse/HADOOP-7327) | FileSystem.listStatus() throws NullPointerException instead of IOException upon access permission failure |  Minor | fs | Matt Foley | Matt Foley |
| [MAPREDUCE-2620](https://issues.apache.org/jira/browse/MAPREDUCE-2620) | Update RAID for HDFS-2087 |  Major | contrib/raid | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-2034](https://issues.apache.org/jira/browse/HDFS-2034) | length in getBlockRange becomes -ve when reading only from currently being written blk |  Minor | hdfs-client | John George | John George |
| [MAPREDUCE-2628](https://issues.apache.org/jira/browse/MAPREDUCE-2628) | MR-279: Add compiled on date to NM and RM info/about page |  Minor | mrv2 | Jonathan Eagles | Jonathan Eagles |
| [HDFS-2132](https://issues.apache.org/jira/browse/HDFS-2132) | Potential resource leak in EditLogFileOutputStream.close |  Major | . | Aaron T. Myers | Aaron T. Myers |
| [HADOOP-7324](https://issues.apache.org/jira/browse/HADOOP-7324) | Ganglia plugins for metrics v2 |  Blocker | metrics | Luke Lu | Priyo Mustafi |
| [MAPREDUCE-2587](https://issues.apache.org/jira/browse/MAPREDUCE-2587) | MR279: Fix RM version in the cluster-\>about page |  Minor | . | Thomas Graves | Thomas Graves |
| [MAPREDUCE-2663](https://issues.apache.org/jira/browse/MAPREDUCE-2663) | MR-279: Refactoring StateMachineFactory inner classes |  Minor | mrv2 | Ahmed Radwan | Ahmed Radwan |
| [MAPREDUCE-2678](https://issues.apache.org/jira/browse/MAPREDUCE-2678) | MR-279: minimum-user-limit-percent no longer honored |  Major | capacity-sched | Jeffrey Naisbitt | Jeffrey Naisbitt |
| [MAPREDUCE-2630](https://issues.apache.org/jira/browse/MAPREDUCE-2630) | MR-279: refreshQueues leads to NPEs when used w/FifoScheduler |  Minor | mrv2 | Josh Wills | Josh Wills |
| [MAPREDUCE-2644](https://issues.apache.org/jira/browse/MAPREDUCE-2644) | NodeManager fails to create containers when NM\_LOG\_DIR is not explicitly set in the Configuration |  Major | mrv2 | Josh Wills | Josh Wills |
| [MAPREDUCE-2670](https://issues.apache.org/jira/browse/MAPREDUCE-2670) | Fixing spelling mistake in FairSchedulerServlet.java |  Trivial | . | Eli Collins | Eli Collins |
| [MAPREDUCE-2365](https://issues.apache.org/jira/browse/MAPREDUCE-2365) | Add counters for FileInputFormat (BYTES\_READ) and FileOutputFormat (BYTES\_WRITTEN) |  Major | . | Owen O'Malley | Siddharth Seth |
| [HDFS-2153](https://issues.apache.org/jira/browse/HDFS-2153) | DFSClientAdapter should be put under test |  Minor | test | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-7471](https://issues.apache.org/jira/browse/HADOOP-7471) | the saveVersion.sh script sometimes fails to extract SVN URL |  Major | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [MAPREDUCE-2661](https://issues.apache.org/jira/browse/MAPREDUCE-2661) | MR-279: Accessing MapTaskImpl from TaskImpl |  Minor | mrv2 | Ahmed Radwan | Ahmed Radwan |
| [MAPREDUCE-2710](https://issues.apache.org/jira/browse/MAPREDUCE-2710) | Update DFSClient.stringifyToken(..) in JobSubmitter.printTokens(..) for HDFS-2161 |  Major | client | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-7111](https://issues.apache.org/jira/browse/HADOOP-7111) | Several TFile tests failing when native libraries are present |  Critical | io | Todd Lipcon | Aaron T. Myers |
| [HDFS-2114](https://issues.apache.org/jira/browse/HDFS-2114) | re-commission of a decommissioned node does not delete excess replica |  Major | . | John George | John George |
| [MAPREDUCE-2409](https://issues.apache.org/jira/browse/MAPREDUCE-2409) | Distributed Cache does not differentiate between file /archive for files with the same path |  Major | distributed-cache | Siddharth Seth | Siddharth Seth |
| [HDFS-2156](https://issues.apache.org/jira/browse/HDFS-2156) | rpm should only require the same major version as common |  Major | . | Owen O'Malley | Eric Yang |
| [MAPREDUCE-2575](https://issues.apache.org/jira/browse/MAPREDUCE-2575) | TestMiniMRDFSCaching fails if test.build.dir is set to something other than build/test |  Major | test | Thomas Graves | Thomas Graves |
| [MAPREDUCE-2667](https://issues.apache.org/jira/browse/MAPREDUCE-2667) | MR279: mapred job -kill leaves application in RUNNING state |  Major | mrv2 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-2732](https://issues.apache.org/jira/browse/MAPREDUCE-2732) | Some tests using FSNamesystem.LOG cannot be compiled |  Major | test | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [MAPREDUCE-2463](https://issues.apache.org/jira/browse/MAPREDUCE-2463) | Job History files are not moving to done folder when job history location is hdfs location |  Major | jobtracker | Devaraj K | Devaraj K |
| [MAPREDUCE-2127](https://issues.apache.org/jira/browse/MAPREDUCE-2127) | mapreduce trunk builds are failing on hudson |  Major | build, pipes | Giridharan Kesavan | Bruno Mahé |
| [HADOOP-7356](https://issues.apache.org/jira/browse/HADOOP-7356) | RPM packages broke bin/hadoop script for hadoop 0.20.205 |  Blocker | . | Eric Yang | Eric Yang |
| [HDFS-1381](https://issues.apache.org/jira/browse/HDFS-1381) | HDFS javadocs hard-code references to dfs.namenode.name.dir and dfs.datanode.data.dir parameters |  Major | test | Jakob Homan | Jim Plush |
| [HADOOP-7178](https://issues.apache.org/jira/browse/HADOOP-7178) | FileSystem should have an option to control the .crc file creations at Local. |  Major | fs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [MAPREDUCE-2740](https://issues.apache.org/jira/browse/MAPREDUCE-2740) | MultipleOutputs in new API creates needless TaskAttemptContexts |  Major | . | Todd Lipcon | Todd Lipcon |
| [HDFS-2053](https://issues.apache.org/jira/browse/HDFS-2053) | Bug in INodeDirectory#computeContentSummary warning |  Minor | namenode | Michael Noll | Michael Noll |
| [MAPREDUCE-2760](https://issues.apache.org/jira/browse/MAPREDUCE-2760) | mapreduce.jobtracker.split.metainfo.maxsize typoed in mapred-default.xml |  Minor | documentation | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2773](https://issues.apache.org/jira/browse/MAPREDUCE-2773) | [MR-279] server.api.records.NodeHealthStatus renamed but not updated in client NodeHealthStatus.java |  Minor | mrv2 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-2772](https://issues.apache.org/jira/browse/MAPREDUCE-2772) | MR-279: mrv2 no longer compiles against trunk after common mavenization. |  Major | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-2705](https://issues.apache.org/jira/browse/MAPREDUCE-2705) | tasks localized and launched serially by TaskLauncher - causing other tasks to be delayed |  Major | tasktracker | Thomas Graves | Thomas Graves |
| [MAPREDUCE-2776](https://issues.apache.org/jira/browse/MAPREDUCE-2776) | MR 279: Fix some of the yarn findbug warnings |  Major | mrv2 | Siddharth Seth | Siddharth Seth |
| [HADOOP-7520](https://issues.apache.org/jira/browse/HADOOP-7520) | hadoop-main fails to deploy |  Major | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-7523](https://issues.apache.org/jira/browse/HADOOP-7523) | Test org.apache.hadoop.fs.TestFilterFileSystem fails due to java.lang.NoSuchMethodException |  Blocker | test | John Lee | John Lee |
| [MAPREDUCE-2797](https://issues.apache.org/jira/browse/MAPREDUCE-2797) | Some java files cannot be compiled |  Major | contrib/raid, test | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [MAPREDUCE-2689](https://issues.apache.org/jira/browse/MAPREDUCE-2689) | InvalidStateTransisiton when AM is not assigned to a job |  Major | mrv2 | Ramya Sunil |  |
| [MAPREDUCE-2706](https://issues.apache.org/jira/browse/MAPREDUCE-2706) | MR-279: Submit jobs beyond the max jobs per queue limit no longer gets logged |  Major | mrv2 | Jeffrey Naisbitt | Jeffrey Naisbitt |
| [MAPREDUCE-2781](https://issues.apache.org/jira/browse/MAPREDUCE-2781) | mr279 RM application finishtime not set |  Minor | mrv2 | Thomas Graves | Thomas Graves |
| [HDFS-2245](https://issues.apache.org/jira/browse/HDFS-2245) | BlockManager.chooseTarget(..) throws NPE |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [MAPREDUCE-2808](https://issues.apache.org/jira/browse/MAPREDUCE-2808) | pull MAPREDUCE-2797 into mr279 branch |  Minor | mrv2 | Thomas Graves | Thomas Graves |
| [HADOOP-7499](https://issues.apache.org/jira/browse/HADOOP-7499) | Add method for doing a sanity check on hostnames in NetUtils |  Major | util | Jeffrey Naisbitt | Jeffrey Naisbitt |
| [MAPREDUCE-2489](https://issues.apache.org/jira/browse/MAPREDUCE-2489) | Jobsplits with random hostnames can make the queue unusable |  Major | jobtracker | Jeffrey Naisbitt | Jeffrey Naisbitt |
| [HADOOP-7357](https://issues.apache.org/jira/browse/HADOOP-7357) | hadoop.io.compress.TestCodec#main() should exit with non-zero exit code if test failed |  Trivial | test | Philip Zeyliger | Philip Zeyliger |
| [HDFS-2229](https://issues.apache.org/jira/browse/HDFS-2229) | Deadlock in NameNode |  Blocker | namenode | Vinod Kumar Vavilapalli | Tsz Wo Nicholas Sze |
| [HADOOP-6622](https://issues.apache.org/jira/browse/HADOOP-6622) | Token should not print the password in toString. |  Major | security | Jitendra Nath Pandey | Eli Collins |
| [HDFS-2235](https://issues.apache.org/jira/browse/HDFS-2235) | Encode servlet paths |  Major | namenode | Eli Collins | Eli Collins |
| [HDFS-2186](https://issues.apache.org/jira/browse/HDFS-2186) | DN volume failures on startup are not counted |  Major | datanode | Eli Collins | Eli Collins |
| [HDFS-2240](https://issues.apache.org/jira/browse/HDFS-2240) | Possible deadlock between LeaseRenewer and its factory |  Critical | hdfs-client | Todd Lipcon | Tsz Wo Nicholas Sze |
| [HADOOP-7529](https://issues.apache.org/jira/browse/HADOOP-7529) | Possible deadlock in metrics2 |  Critical | metrics | Todd Lipcon | Luke Lu |
| [HDFS-73](https://issues.apache.org/jira/browse/HDFS-73) | DFSOutputStream does not close all the sockets |  Blocker | hdfs-client | Raghu Angadi | Uma Maheswara Rao G |
| [HDFS-1776](https://issues.apache.org/jira/browse/HDFS-1776) | Bug in Concat code |  Major | . | Dmytro Molkov | Bharath Mundlapudi |
| [MAPREDUCE-2541](https://issues.apache.org/jira/browse/MAPREDUCE-2541) | Race Condition in IndexCache(readIndexFileToCache,removeMap) causes value of totalMemoryUsed corrupt, which may cause TaskTracker continue throw Exception |  Critical | tasktracker | Binglin Chang | Binglin Chang |
| [MAPREDUCE-2839](https://issues.apache.org/jira/browse/MAPREDUCE-2839) | MR Jobs fail on a secure cluster with viewfs |  Major | . | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-2727](https://issues.apache.org/jira/browse/MAPREDUCE-2727) | MR-279: SleepJob throws divide by zero exception when count = 0 |  Major | mrv2 | Jeffrey Naisbitt | Jeffrey Naisbitt |
| [HADOOP-7545](https://issues.apache.org/jira/browse/HADOOP-7545) | common -tests jar should not include properties and configs |  Critical | build, test | Todd Lipcon | Todd Lipcon |
| [HADOOP-7536](https://issues.apache.org/jira/browse/HADOOP-7536) | Correct the dependency version regressions introduced in HADOOP-6671 |  Major | build | Kihwal Lee | Alejandro Abdelnur |
| [HDFS-1257](https://issues.apache.org/jira/browse/HDFS-1257) | Race condition on FSNamesystem#recentInvalidateSets introduced by HADOOP-5124 |  Major | namenode | Ramkumar Vadali | Eric Payne |
| [MAPREDUCE-2854](https://issues.apache.org/jira/browse/MAPREDUCE-2854) | update INSTALL with config necessary run mapred on yarn |  Major | . | Thomas Graves | Thomas Graves |
| [MAPREDUCE-2860](https://issues.apache.org/jira/browse/MAPREDUCE-2860) | Fix log4j logging in the maven test cases. |  Major | mrv2 | Mahadev konar | Mahadev konar |
| [MAPREDUCE-2867](https://issues.apache.org/jira/browse/MAPREDUCE-2867) | Remove Unused TestApplicaitonCleanup in resourcemanager/applicationsmanager. |  Major | . | Mahadev konar | Mahadev konar |
| [HADOOP-7566](https://issues.apache.org/jira/browse/HADOOP-7566) | MR tests are failing  webapps/hdfs not found in CLASSPATH |  Major | . | Mahadev konar | Alejandro Abdelnur |
| [MAPREDUCE-2649](https://issues.apache.org/jira/browse/MAPREDUCE-2649) | MR279: Fate of finished Applications on RM |  Major | mrv2 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-2868](https://issues.apache.org/jira/browse/MAPREDUCE-2868) | ant build broken in hadoop-mapreduce dir |  Major | build | Thomas Graves | Mahadev konar |
| [HDFS-2267](https://issues.apache.org/jira/browse/HDFS-2267) | DataXceiver thread name incorrect while waiting on op during keepalive |  Trivial | datanode | Todd Lipcon | Todd Lipcon |
| [HDFS-1480](https://issues.apache.org/jira/browse/HDFS-1480) | All replicas of a block can end up on the same rack when some datanodes are decommissioning. |  Major | namenode | T Meyarivan | Todd Lipcon |
| [MAPREDUCE-2859](https://issues.apache.org/jira/browse/MAPREDUCE-2859) | mapreduce trunk is broken with eclipse plugin contrib |  Major | . | Giridharan Kesavan | Giridharan Kesavan |
| [MAPREDUCE-2846](https://issues.apache.org/jira/browse/MAPREDUCE-2846) | a small % of all tasks fail with DefaultTaskController |  Blocker | task, task-controller, tasktracker | Allen Wittenauer | Owen O'Malley |
| [HADOOP-7563](https://issues.apache.org/jira/browse/HADOOP-7563) | hadoop-config.sh setup CLASSPATH, HADOOP\_HDFS\_HOME and HADOOP\_MAPRED\_HOME incorrectly |  Major | scripts | Eric Yang | Eric Yang |
| [MAPREDUCE-2877](https://issues.apache.org/jira/browse/MAPREDUCE-2877) | Add missing Apache license header in some files in MR and also add the rat plugin to the poms. |  Major | . | Mahadev konar | Mahadev konar |
| [MAPREDUCE-2879](https://issues.apache.org/jira/browse/MAPREDUCE-2879) | Change mrv2 version to be 0.23.0-SNAPSHOT |  Major | . | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-2796](https://issues.apache.org/jira/browse/MAPREDUCE-2796) | [MR-279] Start time for all the apps is set to 0 |  Major | mrv2 | Ramya Sunil | Devaraj K |
| [HADOOP-7578](https://issues.apache.org/jira/browse/HADOOP-7578) | Fix test-patch to be able to run on MR patches. |  Major | . | Mahadev konar | Mahadev konar |
| [MAPREDUCE-517](https://issues.apache.org/jira/browse/MAPREDUCE-517) | The capacity-scheduler should assign multiple tasks per heartbeat |  Critical | . | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-2429](https://issues.apache.org/jira/browse/MAPREDUCE-2429) | Check jvmid during task status report |  Major | tasktracker | Arun C Murthy | Siddharth Seth |
| [MAPREDUCE-2881](https://issues.apache.org/jira/browse/MAPREDUCE-2881) | mapreduce ant compilation fails "java.lang.IllegalStateException: impossible to get artifacts" |  Major | build | Giridharan Kesavan | Giridharan Kesavan |
| [MAPREDUCE-2885](https://issues.apache.org/jira/browse/MAPREDUCE-2885) | mapred-config.sh doesn't look for $HADOOP\_COMMON\_HOME/libexec/hadoop-config.sh |  Blocker | . | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-2550](https://issues.apache.org/jira/browse/MAPREDUCE-2550) | bin/mapred no longer works from a source checkout |  Blocker | build | Eric Yang | Eric Yang |
| [HDFS-2289](https://issues.apache.org/jira/browse/HDFS-2289) | jsvc isn't part of the artifact |  Blocker | . | Arun C Murthy | Alejandro Abdelnur |
| [HADOOP-7589](https://issues.apache.org/jira/browse/HADOOP-7589) | Prefer mvn test -DskipTests over mvn compile in test-patch.sh |  Major | build | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-2904](https://issues.apache.org/jira/browse/MAPREDUCE-2904) | HDFS jars added incorrectly to yarn classpath |  Major | . | Sharad Agarwal | Sharad Agarwal |
| [MAPREDUCE-2737](https://issues.apache.org/jira/browse/MAPREDUCE-2737) | Update the progress of jobs on client side |  Major | mrv2 | Ramya Sunil | Siddharth Seth |
| [MAPREDUCE-2886](https://issues.apache.org/jira/browse/MAPREDUCE-2886) | Fix Javadoc warnings in MapReduce. |  Critical | mrv2 | Mahadev konar | Mahadev konar |
| [HADOOP-7576](https://issues.apache.org/jira/browse/HADOOP-7576) | Fix findbugs warnings in Hadoop Auth (Alfredo) |  Major | security | Tom White | Tsz Wo Nicholas Sze |
| [HADOOP-7593](https://issues.apache.org/jira/browse/HADOOP-7593) | AssertionError in TestHttpServer.testMaxThreads() |  Major | test | Tsz Wo Nicholas Sze | Uma Maheswara Rao G |
| [MAPREDUCE-2916](https://issues.apache.org/jira/browse/MAPREDUCE-2916) | Ivy build for MRv1 fails with bad organization for common daemon. |  Major | . | Mahadev konar | Mahadev konar |
| [MAPREDUCE-2652](https://issues.apache.org/jira/browse/MAPREDUCE-2652) | MR-279: Cannot run multiple NMs on a single node |  Major | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-2756](https://issues.apache.org/jira/browse/MAPREDUCE-2756) | JobControl can drop jobs if an error occurs |  Minor | client, mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-2735](https://issues.apache.org/jira/browse/MAPREDUCE-2735) | MR279: finished applications should be added to an application summary log |  Major | mrv2 | Thomas Graves | Thomas Graves |
| [HDFS-2258](https://issues.apache.org/jira/browse/HDFS-2258) | TestLeaseRecovery2 fails as lease hard limit is not reset to default |  Major | namenode, test | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-7604](https://issues.apache.org/jira/browse/HADOOP-7604) | Hadoop Auth examples pom in 0.23 point to 0.24 versions. |  Critical | . | Mahadev konar | Mahadev konar |
| [HADOOP-7606](https://issues.apache.org/jira/browse/HADOOP-7606) | Upgrade Jackson to version 1.7.1 to match the version required by Jersey |  Major | test | Aaron T. Myers | Alejandro Abdelnur |
| [MAPREDUCE-2716](https://issues.apache.org/jira/browse/MAPREDUCE-2716) | MR279: MRReliabilityTest job fails because of missing job-file. |  Major | mrv2 | Jeffrey Naisbitt | Jeffrey Naisbitt |
| [MAPREDUCE-2697](https://issues.apache.org/jira/browse/MAPREDUCE-2697) | Enhance CS to cap concurrently running jobs |  Major | mrv2 | Arun C Murthy | Arun C Murthy |
| [HADOOP-7580](https://issues.apache.org/jira/browse/HADOOP-7580) | Add a version of getLocalPathForWrite to LocalDirAllocator which doesn't create dirs |  Major | . | Siddharth Seth | Siddharth Seth |
| [HDFS-2314](https://issues.apache.org/jira/browse/HDFS-2314) | MRV1 test compilation broken after HDFS-2197 |  Major | test | Vinod Kumar Vavilapalli | Todd Lipcon |
| [MAPREDUCE-2687](https://issues.apache.org/jira/browse/MAPREDUCE-2687) | Non superusers unable to launch apps in both secure and non-secure cluster |  Blocker | mrv2 | Ramya Sunil | Mahadev konar |
| [MAPREDUCE-2767](https://issues.apache.org/jira/browse/MAPREDUCE-2767) | Remove Linux task-controller from 0.22 branch |  Blocker | security | Milind Bhandarkar | Milind Bhandarkar |
| [MAPREDUCE-2917](https://issues.apache.org/jira/browse/MAPREDUCE-2917) | Corner case in container reservations |  Major | mrv2, resourcemanager | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-2800](https://issues.apache.org/jira/browse/MAPREDUCE-2800) | clockSplits, cpuUsages, vMemKbytes, physMemKbytes is set to -1 in jhist files |  Major | mrv2 | Ramya Sunil | Siddharth Seth |
| [MAPREDUCE-2774](https://issues.apache.org/jira/browse/MAPREDUCE-2774) | [MR-279] Add a startup msg while starting RM/NM |  Minor | mrv2 | Ramya Sunil | Venu Gopala Rao |
| [MAPREDUCE-2882](https://issues.apache.org/jira/browse/MAPREDUCE-2882) | TestLineRecordReader depends on ant jars |  Minor | test | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2655](https://issues.apache.org/jira/browse/MAPREDUCE-2655) | MR279: Audit logs for YARN |  Major | mrv2 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-2938](https://issues.apache.org/jira/browse/MAPREDUCE-2938) | Missing log stmt for app submission fail CS |  Trivial | mrv2, scheduler | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-2948](https://issues.apache.org/jira/browse/MAPREDUCE-2948) | Hadoop streaming test failure, post MR-2767 |  Major | contrib/streaming | Milind Bhandarkar | Mahadev konar |
| [HDFS-2232](https://issues.apache.org/jira/browse/HDFS-2232) | TestHDFSCLI fails on 0.22 branch |  Blocker | test | Konstantin Shvachko | Plamen Jeliazkov |
| [MAPREDUCE-2908](https://issues.apache.org/jira/browse/MAPREDUCE-2908) | Fix findbugs warnings in Map Reduce. |  Critical | mrv2 | Mahadev konar | Vinod Kumar Vavilapalli |
| [MAPREDUCE-2947](https://issues.apache.org/jira/browse/MAPREDUCE-2947) | Sort fails on YARN+MR with lots of task failures |  Major | mrv2 | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-2844](https://issues.apache.org/jira/browse/MAPREDUCE-2844) | [MR-279] Incorrect node ID info |  Trivial | mrv2 | Ramya Sunil | Ravi Teja Ch N V |
| [MAPREDUCE-2677](https://issues.apache.org/jira/browse/MAPREDUCE-2677) | MR-279: 404 error while accessing pages from history server |  Major | mrv2 | Ramya Sunil | Robert Joseph Evans |
| [HADOOP-7598](https://issues.apache.org/jira/browse/HADOOP-7598) | smart-apply-patch.sh does not handle patching from a sub directory correctly. |  Major | build | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-2690](https://issues.apache.org/jira/browse/MAPREDUCE-2690) | Construct the web page for default scheduler |  Major | mrv2 | Ramya Sunil | Eric Payne |
| [MAPREDUCE-2937](https://issues.apache.org/jira/browse/MAPREDUCE-2937) | Errors in Application failures are not shown in the client trace. |  Critical | mrv2 | Mahadev konar | Mahadev konar |
| [MAPREDUCE-2953](https://issues.apache.org/jira/browse/MAPREDUCE-2953) | JobClient fails due to a race in RM, removes staged files and in turn crashes MR AM |  Major | mrv2, resourcemanager | Vinod Kumar Vavilapalli | Thomas Graves |
| [MAPREDUCE-2958](https://issues.apache.org/jira/browse/MAPREDUCE-2958) | mapred-default.xml not merged from mr279 |  Critical | mrv2 | Thomas Graves | Arun C Murthy |
| [MAPREDUCE-2963](https://issues.apache.org/jira/browse/MAPREDUCE-2963) | TestMRJobs hangs waiting to connect to history server. |  Critical | . | Mahadev konar | Siddharth Seth |
| [MAPREDUCE-2711](https://issues.apache.org/jira/browse/MAPREDUCE-2711) | TestBlockPlacementPolicyRaid cannot be compiled |  Major | contrib/raid | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [MAPREDUCE-2954](https://issues.apache.org/jira/browse/MAPREDUCE-2954) | Deadlock in NM with threads racing for ApplicationAttemptId |  Critical | mrv2 | Vinod Kumar Vavilapalli | Siddharth Seth |
| [MAPREDUCE-2975](https://issues.apache.org/jira/browse/MAPREDUCE-2975) | ResourceManager Delegate is not getting initialized with yarn-site.xml as default configuration. |  Blocker | . | Mahadev konar | Mahadev konar |
| [MAPREDUCE-2971](https://issues.apache.org/jira/browse/MAPREDUCE-2971) | ant build mapreduce fails  protected access  jc.displayJobList(jobs); |  Blocker | mrv2 | Thomas Graves | Thomas Graves |
| [HADOOP-7610](https://issues.apache.org/jira/browse/HADOOP-7610) | /etc/profile.d does not exist on Debian |  Major | scripts | Eric Yang | Eric Yang |
| [MAPREDUCE-2749](https://issues.apache.org/jira/browse/MAPREDUCE-2749) | [MR-279] NM registers with RM even before it starts various servers |  Major | mrv2 | Vinod Kumar Vavilapalli | Thomas Graves |
| [MAPREDUCE-2979](https://issues.apache.org/jira/browse/MAPREDUCE-2979) | Remove ClientProtocolProvider configuration under mapreduce-client-core |  Major | mrv2 | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-2936](https://issues.apache.org/jira/browse/MAPREDUCE-2936) | Contrib Raid compilation broken after HDFS-1620 |  Major | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [HADOOP-7568](https://issues.apache.org/jira/browse/HADOOP-7568) | SequenceFile should not print into stdout |  Major | io | Konstantin Shvachko | Plamen Jeliazkov |
| [MAPREDUCE-2985](https://issues.apache.org/jira/browse/MAPREDUCE-2985) | findbugs error in ResourceLocalizationService.handle(LocalizationEvent) |  Major | mrv2 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-2874](https://issues.apache.org/jira/browse/MAPREDUCE-2874) | ApplicationId printed in 2 different formats and has 2 different toString routines that are used |  Major | mrv2 | Thomas Graves | Eric Payne |
| [HADOOP-7599](https://issues.apache.org/jira/browse/HADOOP-7599) | Improve hadoop setup conf script to setup secure Hadoop cluster |  Major | scripts | Eric Yang | Eric Yang |
| [HADOOP-7626](https://issues.apache.org/jira/browse/HADOOP-7626) | Allow overwrite of HADOOP\_CLASSPATH and HADOOP\_OPTS |  Major | scripts | Eric Yang | Eric Yang |
| [HDFS-2323](https://issues.apache.org/jira/browse/HDFS-2323) | start-dfs.sh script fails for tarball install |  Major | . | Tom White | Tom White |
| [MAPREDUCE-2995](https://issues.apache.org/jira/browse/MAPREDUCE-2995) | MR AM crashes when a container-launch hangs on a faulty NM |  Major | mrv2 | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-2997](https://issues.apache.org/jira/browse/MAPREDUCE-2997) | MR task fails before launch itself with an NPE in ContainerLauncher |  Major | applicationmaster, mrv2 | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [HADOOP-7629](https://issues.apache.org/jira/browse/HADOOP-7629) | regression with MAPREDUCE-2289 - setPermission passed immutable FsPermission (rpc failure) |  Major | . | Patrick Hunt | Todd Lipcon |
| [MAPREDUCE-2949](https://issues.apache.org/jira/browse/MAPREDUCE-2949) | NodeManager in a inconsistent state if a service startup fails. |  Major | mrv2, nodemanager | Ravi Teja Ch N V | Ravi Teja Ch N V |
| [MAPREDUCE-3005](https://issues.apache.org/jira/browse/MAPREDUCE-3005) | MR app hangs because of a NPE in ResourceManager |  Major | mrv2 | Vinod Kumar Vavilapalli | Arun C Murthy |
| [MAPREDUCE-2991](https://issues.apache.org/jira/browse/MAPREDUCE-2991) | queueinfo.jsp fails to show queue status if any Capacity scheduler queue name has dash/hiphen in it. |  Major | scheduler | Priyo Mustafi | Priyo Mustafi |
| [HDFS-2331](https://issues.apache.org/jira/browse/HDFS-2331) | Hdfs compilation fails |  Major | hdfs-client | Abhijit Suresh Shingate | Abhijit Suresh Shingate |
| [HDFS-2333](https://issues.apache.org/jira/browse/HDFS-2333) | HDFS-2284 introduced 2 findbugs warnings on trunk |  Major | . | Ivan Kelly | Tsz Wo Nicholas Sze |
| [MAPREDUCE-2994](https://issues.apache.org/jira/browse/MAPREDUCE-2994) | Parse Error is coming for App ID when we click application link on the RM UI |  Major | mrv2, resourcemanager | Devaraj K | Devaraj K |
| [HADOOP-7608](https://issues.apache.org/jira/browse/HADOOP-7608) | SnappyCodec check for Hadoop native lib is wrong |  Major | io | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-7637](https://issues.apache.org/jira/browse/HADOOP-7637) | Fair scheduler configuration file is not bundled in RPM |  Major | build | Eric Yang | Eric Yang |
| [MAPREDUCE-2987](https://issues.apache.org/jira/browse/MAPREDUCE-2987) | RM UI display logged in user as null |  Major | mrv2 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-1788](https://issues.apache.org/jira/browse/MAPREDUCE-1788) | o.a.h.mapreduce.Job shouldn't make a copy of the JobConf |  Major | client | Arun C Murthy | Arun C Murthy |
| [HADOOP-7631](https://issues.apache.org/jira/browse/HADOOP-7631) | In mapred-site.xml, stream.tmpdir is mapped to ${mapred.temp.dir} which is undeclared. |  Major | conf | Ramya Sunil | Eric Yang |
| [MAPREDUCE-3006](https://issues.apache.org/jira/browse/MAPREDUCE-3006) | MapReduce AM exits prematurely before completely writing and closing the JobHistory file |  Major | applicationmaster, mrv2 | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-2965](https://issues.apache.org/jira/browse/MAPREDUCE-2965) | Streamline hashCode(), equals(), compareTo() and toString() for all IDs |  Blocker | mrv2 | Vinod Kumar Vavilapalli | Siddharth Seth |
| [MAPREDUCE-2925](https://issues.apache.org/jira/browse/MAPREDUCE-2925) | job -status \<JOB\_ID\> is giving continuously info message for completed jobs on the console |  Major | mrv2 | Devaraj K | Devaraj K |
| [MAPREDUCE-3030](https://issues.apache.org/jira/browse/MAPREDUCE-3030) | RM is not processing heartbeat and continuously giving the message 'Node not found rebooting' |  Blocker | mrv2, resourcemanager | Devaraj K | Devaraj K |
| [MAPREDUCE-3042](https://issues.apache.org/jira/browse/MAPREDUCE-3042) | YARN RM fails to start |  Major | mrv2, resourcemanager | Chris Riccomini | Chris Riccomini |
| [MAPREDUCE-3038](https://issues.apache.org/jira/browse/MAPREDUCE-3038) | job history server not starting because conf() missing HsController |  Blocker | mrv2 | Thomas Graves | Jeffrey Naisbitt |
| [MAPREDUCE-3004](https://issues.apache.org/jira/browse/MAPREDUCE-3004) | sort example fails in shuffle/reduce stage as it assumes a local job by default |  Minor | mrv2 | Hitesh Shah | Hitesh Shah |
| [MAPREDUCE-3017](https://issues.apache.org/jira/browse/MAPREDUCE-3017) | The Web UI shows FINISHED for killed/successful/failed jobs. |  Blocker | mrv2 | Mahadev konar | Mahadev konar |
| [MAPREDUCE-3040](https://issues.apache.org/jira/browse/MAPREDUCE-3040) | TestMRJobs, TestMRJobsWithHistoryService, TestMROldApiJobs fail |  Major | mrv2 | Thomas Graves | Arun C Murthy |
| [HDFS-2347](https://issues.apache.org/jira/browse/HDFS-2347) | checkpointTxnCount's comment still saying about editlog size |  Trivial | namenode | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-7639](https://issues.apache.org/jira/browse/HADOOP-7639) | yarn ui not properly filtered in HttpServer |  Major | . | Thomas Graves | Thomas Graves |
| [HDFS-2344](https://issues.apache.org/jira/browse/HDFS-2344) | Fix the TestOfflineEditsViewer test failure in 0.23 branch |  Major | test | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [MAPREDUCE-2763](https://issues.apache.org/jira/browse/MAPREDUCE-2763) | IllegalArgumentException while using the dist cache |  Major | mrv2 | Ramya Sunil |  |
| [HADOOP-7630](https://issues.apache.org/jira/browse/HADOOP-7630) | hadoop-metrics2.properties should have a property \*.period set to a default value foe metrics |  Major | conf | Arpit Gupta | Eric Yang |
| [HADOOP-7633](https://issues.apache.org/jira/browse/HADOOP-7633) | log4j.properties should be added to the hadoop conf on deploy |  Major | conf | Arpit Gupta | Eric Yang |
| [MAPREDUCE-3018](https://issues.apache.org/jira/browse/MAPREDUCE-3018) | Streaming jobs with -file option fail to run. |  Blocker | mrv2 | Mahadev konar | Mahadev konar |
| [MAPREDUCE-3036](https://issues.apache.org/jira/browse/MAPREDUCE-3036) | Some of the Resource Manager memory metrics go negative. |  Blocker | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-2998](https://issues.apache.org/jira/browse/MAPREDUCE-2998) | Failing to contact Am/History for jobs: java.io.EOFException in DataInputStream |  Critical | mrv2 | Jeffrey Naisbitt | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3023](https://issues.apache.org/jira/browse/MAPREDUCE-3023) | Queue state is not being translated properly (is always assumed to be running) |  Major | mrv2 | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-2970](https://issues.apache.org/jira/browse/MAPREDUCE-2970) | Null Pointer Exception while submitting a Job, If mapreduce.framework.name property is not set. |  Major | job submission, mrv2 | Venu Gopala Rao | Venu Gopala Rao |
| [HADOOP-7575](https://issues.apache.org/jira/browse/HADOOP-7575) | Support fully qualified paths as part of LocalDirAllocator |  Minor | fs | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-3062](https://issues.apache.org/jira/browse/MAPREDUCE-3062) | YARN NM/RM fail to start |  Major | mrv2, nodemanager, resourcemanager | Chris Riccomini | Chris Riccomini |
| [MAPREDUCE-3066](https://issues.apache.org/jira/browse/MAPREDUCE-3066) | YARN NM fails to start |  Major | mrv2, nodemanager | Chris Riccomini | Chris Riccomini |
| [MAPREDUCE-3044](https://issues.apache.org/jira/browse/MAPREDUCE-3044) | Pipes jobs stuck without making progress |  Blocker | mrv2 | Ramya Sunil | Mahadev konar |
| [MAPREDUCE-3048](https://issues.apache.org/jira/browse/MAPREDUCE-3048) | Fix test-patch to run tests via "mvn clean install test" |  Major | build | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-2754](https://issues.apache.org/jira/browse/MAPREDUCE-2754) | MR-279: AM logs are incorrectly going to stderr and error messages going incorrectly to stdout |  Blocker | mrv2 | Ramya Sunil | Ravi Teja Ch N V |
| [MAPREDUCE-3073](https://issues.apache.org/jira/browse/MAPREDUCE-3073) | Build failure for MRv1 caused due to changes to MRConstants. |  Blocker | . | Mahadev konar | Mahadev konar |
| [HADOOP-7671](https://issues.apache.org/jira/browse/HADOOP-7671) | Add license headers to hadoop-common/src/main/packages/templates/conf/ |  Major | . | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-3055](https://issues.apache.org/jira/browse/MAPREDUCE-3055) | Simplify parameter passing to Application Master from Client. SImplify approach to pass info such  appId, ClusterTimestamp and failcount required by App Master. |  Minor | mrv2 | Hitesh Shah | Vinod Kumar Vavilapalli |
| [HDFS-2290](https://issues.apache.org/jira/browse/HDFS-2290) | Block with corrupt replica is not getting replicated |  Major | namenode | Konstantin Shvachko | Benoy Antony |
| [HADOOP-7663](https://issues.apache.org/jira/browse/HADOOP-7663) | TestHDFSTrash failing on 22 |  Major | test | Mayank Bansal | Mayank Bansal |
| [MAPREDUCE-3053](https://issues.apache.org/jira/browse/MAPREDUCE-3053) | YARN Protobuf RPC Failures in RM |  Major | mrv2, resourcemanager | Chris Riccomini | Vinod Kumar Vavilapalli |
| [MAPREDUCE-2952](https://issues.apache.org/jira/browse/MAPREDUCE-2952) | Application failure diagnostics are not consumed in a couple of cases |  Blocker | mrv2, resourcemanager | Vinod Kumar Vavilapalli | Arun C Murthy |
| [MAPREDUCE-2646](https://issues.apache.org/jira/browse/MAPREDUCE-2646) | MR-279: AM with same sized maps and reduces hangs in presence of failing maps |  Critical | applicationmaster, mrv2 | Sharad Agarwal | Sharad Agarwal |
| [MAPREDUCE-3031](https://issues.apache.org/jira/browse/MAPREDUCE-3031) | Job Client goes into infinite loop when we kill AM |  Blocker | mrv2 | Karam Singh | Siddharth Seth |
| [MAPREDUCE-2984](https://issues.apache.org/jira/browse/MAPREDUCE-2984) | Throwing NullPointerException when we open the container page |  Minor | mrv2, nodemanager | Devaraj K | Devaraj K |
| [MAPREDUCE-3071](https://issues.apache.org/jira/browse/MAPREDUCE-3071) | app master configuration web UI link under the Job menu opens up application menu |  Major | mrv2 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-3067](https://issues.apache.org/jira/browse/MAPREDUCE-3067) | Container exit status not set properly to launched process's exit code on successful completion of process |  Blocker | mrv2 | Hitesh Shah | Hitesh Shah |
| [MAPREDUCE-3092](https://issues.apache.org/jira/browse/MAPREDUCE-3092) | Remove JOB\_ID\_COMPARATOR usage in JobHistory.java |  Minor | mrv2 | Devaraj K | Devaraj K |
| [MAPREDUCE-2999](https://issues.apache.org/jira/browse/MAPREDUCE-2999) | hadoop.http.filter.initializers not working properly on yarn UI |  Critical | mrv2 | Thomas Graves | Thomas Graves |
| [MAPREDUCE-3081](https://issues.apache.org/jira/browse/MAPREDUCE-3081) | Change the name format for hadoop core and vaidya jar to be hadoop-{core/vaidya}-{version}.jar in vaidya.sh |  Major | contrib/vaidya | vitthal (Suhas) Gogate |  |
| [MAPREDUCE-3095](https://issues.apache.org/jira/browse/MAPREDUCE-3095) | fairscheduler ivy including wrong version for hdfs |  Major | mrv2 | John George | John George |
| [MAPREDUCE-3054](https://issues.apache.org/jira/browse/MAPREDUCE-3054) | Unable to kill submitted jobs |  Blocker | mrv2 | Siddharth Seth | Mahadev konar |
| [MAPREDUCE-3021](https://issues.apache.org/jira/browse/MAPREDUCE-3021) | all yarn webapps use same base name of "yarn/" |  Major | mrv2 | Thomas Graves | Thomas Graves |
| [HDFS-2368](https://issues.apache.org/jira/browse/HDFS-2368) | defaults created for web keytab and principal, these properties should not have defaults |  Major | . | Arpit Gupta | Tsz Wo Nicholas Sze |
| [HDFS-2361](https://issues.apache.org/jira/browse/HDFS-2361) | hftp is broken |  Critical | namenode | Rajit Saha | Jitendra Nath Pandey |
| [MAPREDUCE-2843](https://issues.apache.org/jira/browse/MAPREDUCE-2843) | [MR-279] Node entries on the RM UI are not sortable |  Major | mrv2 | Ramya Sunil | Abhijit Suresh Shingate |
| [MAPREDUCE-3110](https://issues.apache.org/jira/browse/MAPREDUCE-3110) | TestRPC.testUnknownCall() is failing |  Major | mrv2, test | Devaraj K | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3078](https://issues.apache.org/jira/browse/MAPREDUCE-3078) | Application's progress isn't updated from AM to RM. |  Blocker | applicationmaster, mrv2, resourcemanager | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [HADOOP-7662](https://issues.apache.org/jira/browse/HADOOP-7662) | logs servlet should use pathspec of /\* |  Major | . | Thomas Graves | Thomas Graves |
| [MAPREDUCE-3114](https://issues.apache.org/jira/browse/MAPREDUCE-3114) | Invalid ApplicationMaster URL in Applications Page |  Major | mrv2 | Subroto Sanyal | Subroto Sanyal |
| [MAPREDUCE-3064](https://issues.apache.org/jira/browse/MAPREDUCE-3064) | 27 unit test failures with  Invalid "mapreduce.jobtracker.address" configuration value for JobTracker: "local" |  Blocker | . | Thomas Graves | Venu Gopala Rao |
| [MAPREDUCE-2791](https://issues.apache.org/jira/browse/MAPREDUCE-2791) | [MR-279] Missing/incorrect info on job -status CLI |  Blocker | mrv2 | Ramya Sunil | Devaraj K |
| [MAPREDUCE-2996](https://issues.apache.org/jira/browse/MAPREDUCE-2996) | Log uberized information into JobHistory and use the same via CompletedJob |  Blocker | jobhistoryserver, mrv2 | Vinod Kumar Vavilapalli | Jonathan Eagles |
| [MAPREDUCE-2779](https://issues.apache.org/jira/browse/MAPREDUCE-2779) | JobSplitWriter.java can't handle large job.split file |  Major | job submission | Ming Ma | Ming Ma |
| [MAPREDUCE-3050](https://issues.apache.org/jira/browse/MAPREDUCE-3050) | YarnScheduler needs to expose Resource Usage Information |  Blocker | mrv2, resourcemanager | Robert Joseph Evans | Robert Joseph Evans |
| [HADOOP-7691](https://issues.apache.org/jira/browse/HADOOP-7691) | hadoop deb pkg should take a diff group id |  Major | . | Giridharan Kesavan | Eric Yang |
| [HADOOP-7603](https://issues.apache.org/jira/browse/HADOOP-7603) | Set default hdfs, mapred uid, and hadoop group gid for RPM packages |  Major | . | Eric Yang | Eric Yang |
| [HADOOP-7658](https://issues.apache.org/jira/browse/HADOOP-7658) | to fix hadoop config template |  Major | . | Giridharan Kesavan | Eric Yang |
| [MAPREDUCE-3138](https://issues.apache.org/jira/browse/MAPREDUCE-3138) | Allow for applications to deal with MAPREDUCE-954 |  Blocker | client, mrv2 | Arun C Murthy | Owen O'Malley |
| [HADOOP-7684](https://issues.apache.org/jira/browse/HADOOP-7684) | jobhistory server and secondarynamenode should have init.d script |  Major | scripts | Eric Yang | Eric Yang |
| [MAPREDUCE-3112](https://issues.apache.org/jira/browse/MAPREDUCE-3112) | Calling hadoop cli inside mapreduce job leads to errors |  Major | contrib/streaming | Eric Yang | Eric Yang |
| [HADOOP-7715](https://issues.apache.org/jira/browse/HADOOP-7715) | see log4j Error when running mr jobs and certain dfs calls |  Major | conf | Arpit Gupta | Eric Yang |
| [MAPREDUCE-3056](https://issues.apache.org/jira/browse/MAPREDUCE-3056) | Jobs are failing when those are submitted by other users |  Blocker | applicationmaster, mrv2 | Devaraj K | Devaraj K |
| [HADOOP-7711](https://issues.apache.org/jira/browse/HADOOP-7711) | hadoop-env.sh generated from templates has duplicate info |  Major | conf | Arpit Gupta | Arpit Gupta |
| [HADOOP-7681](https://issues.apache.org/jira/browse/HADOOP-7681) | log4j.properties is missing properties for security audit and hdfs audit should be changed to info |  Minor | conf | Arpit Gupta | Arpit Gupta |
| [HADOOP-7708](https://issues.apache.org/jira/browse/HADOOP-7708) | config generator does not update the properties file if on exists already |  Critical | conf | Arpit Gupta | Eric Yang |
| [MAPREDUCE-2531](https://issues.apache.org/jira/browse/MAPREDUCE-2531) | org.apache.hadoop.mapred.jobcontrol.getAssignedJobID throw class cast exception |  Blocker | client | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-2907](https://issues.apache.org/jira/browse/MAPREDUCE-2907) | ResourceManager logs filled with [INFO] debug messages from org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ParentQueue |  Major | mrv2, resourcemanager | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-2738](https://issues.apache.org/jira/browse/MAPREDUCE-2738) | Missing cluster level stats on the RM UI |  Blocker | mrv2 | Ramya Sunil | Robert Joseph Evans |
| [MAPREDUCE-2913](https://issues.apache.org/jira/browse/MAPREDUCE-2913) | TestMRJobs.testFailingMapper does not assert the correct thing. |  Critical | mrv2, test | Robert Joseph Evans | Jonathan Eagles |
| [HDFS-2409](https://issues.apache.org/jira/browse/HDFS-2409) | \_HOST in dfs.web.authentication.kerberos.principal. |  Major | . | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [HADOOP-7721](https://issues.apache.org/jira/browse/HADOOP-7721) | dfs.web.authentication.kerberos.principal expects the full hostname and does not replace \_HOST with the hostname |  Major | . | Arpit Gupta | Jitendra Nath Pandey |
| [HADOOP-7724](https://issues.apache.org/jira/browse/HADOOP-7724) | hadoop-setup-conf.sh should put proxy user info into the core-site.xml |  Major | . | Giridharan Kesavan | Arpit Gupta |
| [MAPREDUCE-2794](https://issues.apache.org/jira/browse/MAPREDUCE-2794) | [MR-279] Incorrect metrics value for AvailableGB per queue per user |  Blocker | mrv2 | Ramya Sunil | John George |
| [MAPREDUCE-2783](https://issues.apache.org/jira/browse/MAPREDUCE-2783) | mr279 job history handling after killing application |  Critical | mrv2 | Thomas Graves | Eric Payne |
| [MAPREDUCE-2751](https://issues.apache.org/jira/browse/MAPREDUCE-2751) | [MR-279] Lot of local files left on NM after the app finish. |  Blocker | mrv2 | Vinod Kumar Vavilapalli | Siddharth Seth |
| [HDFS-2322](https://issues.apache.org/jira/browse/HDFS-2322) | the build fails in Windows because commons-daemon TAR cannot be fetched |  Major | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [HDFS-2412](https://issues.apache.org/jira/browse/HDFS-2412) | Add backwards-compatibility layer for FSConstants |  Blocker | . | Todd Lipcon | Todd Lipcon |
| [HDFS-2414](https://issues.apache.org/jira/browse/HDFS-2414) | TestDFSRollback fails intermittently |  Critical | namenode, test | Robert Joseph Evans | Todd Lipcon |
| [MAPREDUCE-2876](https://issues.apache.org/jira/browse/MAPREDUCE-2876) | ContainerAllocationExpirer appears to use the incorrect configs |  Critical | mrv2 | Robert Joseph Evans | Anupam Seth |
| [MAPREDUCE-3153](https://issues.apache.org/jira/browse/MAPREDUCE-3153) | TestFileOutputCommitter.testFailAbort() is failing on trunk on Jenkins |  Major | mrv2, test | Vinod Kumar Vavilapalli | Mahadev konar |
| [MAPREDUCE-3123](https://issues.apache.org/jira/browse/MAPREDUCE-3123) | Symbolic links with special chars causing container/task.sh to fail |  Blocker | mrv2 | Thomas Graves | Hitesh Shah |
| [MAPREDUCE-3033](https://issues.apache.org/jira/browse/MAPREDUCE-3033) | JobClient requires mapreduce.jobtracker.address config even when mapreduce.framework.name is set to yarn |  Blocker | job submission, mrv2 | Karam Singh | Hitesh Shah |
| [MAPREDUCE-3158](https://issues.apache.org/jira/browse/MAPREDUCE-3158) | Fix trunk build failures |  Major | mrv2 | Hitesh Shah | Hitesh Shah |
| [HDFS-2422](https://issues.apache.org/jira/browse/HDFS-2422) | The NN should tolerate the same number of low-resource volumes as failed volumes |  Major | namenode | Jeff Bean | Aaron T. Myers |
| [MAPREDUCE-3167](https://issues.apache.org/jira/browse/MAPREDUCE-3167) | container-executor is not being packaged with the assembly target. |  Minor | mrv2 | Mahadev konar | Mahadev konar |
| [MAPREDUCE-3020](https://issues.apache.org/jira/browse/MAPREDUCE-3020) | Node link in reduce task attempt page is not working [Job History Page] |  Major | jobhistoryserver | chackaravarthy | chackaravarthy |
| [MAPREDUCE-2668](https://issues.apache.org/jira/browse/MAPREDUCE-2668) | MR-279: APPLICATION\_STOP is never sent to AuxServices |  Blocker | mrv2 | Robert Joseph Evans | Thomas Graves |
| [MAPREDUCE-3126](https://issues.apache.org/jira/browse/MAPREDUCE-3126) | mr job stuck because reducers using all slots and mapper isn't scheduled |  Blocker | mrv2 | Thomas Graves | Arun C Murthy |
| [MAPREDUCE-3140](https://issues.apache.org/jira/browse/MAPREDUCE-3140) | Invalid JobHistory URL for failed applications |  Major | mrv2 | Bhallamudi Venkata Siva Kamesh | Subroto Sanyal |
| [MAPREDUCE-3125](https://issues.apache.org/jira/browse/MAPREDUCE-3125) | app master web UI shows reduce task progress 100% even though reducers not complete and state running/scheduled |  Critical | mrv2 | Thomas Graves | Hitesh Shah |
| [MAPREDUCE-3157](https://issues.apache.org/jira/browse/MAPREDUCE-3157) | Rumen TraceBuilder is skipping analyzing 0.20 history files |  Major | tools/rumen | Ravi Gummadi | Ravi Gummadi |
| [MAPREDUCE-3166](https://issues.apache.org/jira/browse/MAPREDUCE-3166) | Make Rumen use job history api instead of relying on current history file name format |  Major | tools/rumen | Ravi Gummadi | Ravi Gummadi |
| [MAPREDUCE-2789](https://issues.apache.org/jira/browse/MAPREDUCE-2789) | [MR:279] Update the scheduling info on CLI |  Major | mrv2 | Ramya Sunil | Eric Payne |
| [HADOOP-7745](https://issues.apache.org/jira/browse/HADOOP-7745) | I switched variable names in HADOOP-7509 |  Major | . | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-3183](https://issues.apache.org/jira/browse/MAPREDUCE-3183) | hadoop-assemblies/src/main/resources/assemblies/hadoop-mapreduce-dist.xml missing license header |  Trivial | build | Hitesh Shah | Hitesh Shah |
| [MAPREDUCE-3170](https://issues.apache.org/jira/browse/MAPREDUCE-3170) | Trunk nightly commit builds are failing. |  Critical | build, mrv1, mrv2 | Mahadev konar | Hitesh Shah |
| [MAPREDUCE-3124](https://issues.apache.org/jira/browse/MAPREDUCE-3124) | mapper failed with failed to load native libs |  Blocker | mrv2 | Thomas Graves | John George |
| [MAPREDUCE-2764](https://issues.apache.org/jira/browse/MAPREDUCE-2764) | Fix renewal of dfs delegation tokens |  Major | . | Daryn Sharp | Owen O'Malley |
| [MAPREDUCE-3059](https://issues.apache.org/jira/browse/MAPREDUCE-3059) | QueueMetrics do not have metrics for aggregate containers-allocated and aggregate containers-released |  Blocker | mrv2 | Karam Singh | Devaraj K |
| [MAPREDUCE-3057](https://issues.apache.org/jira/browse/MAPREDUCE-3057) | Job History Server goes of OutOfMemory with 1200 Jobs and Heap Size set to 10 GB |  Blocker | jobhistoryserver, mrv2 | Karam Singh | Eric Payne |
| [MAPREDUCE-3192](https://issues.apache.org/jira/browse/MAPREDUCE-3192) | Fix Javadoc warning in JobClient.java and Cluster.java |  Major | . | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [MAPREDUCE-2840](https://issues.apache.org/jira/browse/MAPREDUCE-2840) | mr279 TestUberAM.testSleepJob test fails |  Minor | mrv2 | Thomas Graves | Jonathan Eagles |
| [MAPREDUCE-3032](https://issues.apache.org/jira/browse/MAPREDUCE-3032) | JobHistory doesn't have error information from failed tasks |  Blocker | applicationmaster, mrv2 | Vinod Kumar Vavilapalli | Devaraj K |
| [MAPREDUCE-3196](https://issues.apache.org/jira/browse/MAPREDUCE-3196) | TestLinuxContainerExecutorWithMocks fails on Mac OSX |  Major | mrv2 | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-3068](https://issues.apache.org/jira/browse/MAPREDUCE-3068) | Should set MALLOC\_ARENA\_MAX for all YARN daemons and AMs/Containers |  Blocker | mrv2 | Vinod Kumar Vavilapalli | Chris Riccomini |
| [MAPREDUCE-3197](https://issues.apache.org/jira/browse/MAPREDUCE-3197) | TestMRClientService failing on building clean checkout of branch 0.23 |  Major | mrv2 | Anupam Seth | Mahadev konar |
| [MAPREDUCE-2762](https://issues.apache.org/jira/browse/MAPREDUCE-2762) | [MR-279] - Cleanup staging dir after job completion |  Blocker | mrv2 | Ramya Sunil | Mahadev konar |
| [MAPREDUCE-3165](https://issues.apache.org/jira/browse/MAPREDUCE-3165) | Ensure logging option is set on child command line |  Blocker | applicationmaster, mrv2 | Arun C Murthy | Todd Lipcon |
| [HDFS-2467](https://issues.apache.org/jira/browse/HDFS-2467) | HftpFileSystem uses incorrect compare for finding delegation tokens |  Major | . | Owen O'Malley | Owen O'Malley |
| [MAPREDUCE-3203](https://issues.apache.org/jira/browse/MAPREDUCE-3203) | Fix some javac warnings in MRAppMaster. |  Major | mrv2 | Mahadev konar | Mahadev konar |
| [HADOOP-7755](https://issues.apache.org/jira/browse/HADOOP-7755) | Detect MapReduce PreCommit Trunk builds silently failing when running test-patch.sh |  Blocker | build | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-3003](https://issues.apache.org/jira/browse/MAPREDUCE-3003) | Publish MR JARs to Maven snapshot repository |  Major | build | Tom White | Alejandro Abdelnur |
| [MAPREDUCE-3176](https://issues.apache.org/jira/browse/MAPREDUCE-3176) | ant mapreduce tests are timing out |  Blocker | mrv2, test | Ravi Prakash | Hitesh Shah |
| [MAPREDUCE-3199](https://issues.apache.org/jira/browse/MAPREDUCE-3199) | TestJobMonitorAndPrint is broken on trunk |  Major | mrv2, test | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3181](https://issues.apache.org/jira/browse/MAPREDUCE-3181) | Terasort fails with Kerberos exception on secure cluster |  Blocker | mrv2 | Anupam Seth | Arun C Murthy |
| [MAPREDUCE-2788](https://issues.apache.org/jira/browse/MAPREDUCE-2788) | Normalize requests in FifoScheduler.allocate to prevent NPEs later |  Major | mrv2 | Ahmed Radwan | Ahmed Radwan |
| [MAPREDUCE-2693](https://issues.apache.org/jira/browse/MAPREDUCE-2693) | NPE in AM causes it to lose containers which are never returned back to RM |  Critical | mrv2 | Amol Kekre | Hitesh Shah |
| [MAPREDUCE-3208](https://issues.apache.org/jira/browse/MAPREDUCE-3208) | NPE while flushing TaskLogAppender |  Minor | mrv2 | liangzhaowang | liangzhaowang |
| [MAPREDUCE-3212](https://issues.apache.org/jira/browse/MAPREDUCE-3212) | Message displays while executing yarn command should be proper |  Minor | mrv2 | Bhallamudi Venkata Siva Kamesh | Bhallamudi Venkata Siva Kamesh |
| [HADOOP-7744](https://issues.apache.org/jira/browse/HADOOP-7744) | Incorrect exit code for hadoop-core-test tests when exception thrown |  Major | test | Jonathan Eagles | Jonathan Eagles |
| [HDFS-2445](https://issues.apache.org/jira/browse/HDFS-2445) | Incorrect exit code for hadoop-hdfs-test tests when exception thrown |  Major | test | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-3179](https://issues.apache.org/jira/browse/MAPREDUCE-3179) | Incorrect exit code for hadoop-mapreduce-test tests when exception thrown |  Major | mrv2, test | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-3188](https://issues.apache.org/jira/browse/MAPREDUCE-3188) | Lots of errors in logs when daemon startup fails |  Major | mrv2 | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-3204](https://issues.apache.org/jira/browse/MAPREDUCE-3204) | mvn site:site fails on MapReduce |  Major | build | Suresh Srinivas | Alejandro Abdelnur |
| [MAPREDUCE-3226](https://issues.apache.org/jira/browse/MAPREDUCE-3226) | Few reduce tasks hanging in a gridmix-run |  Blocker | mrv2, task | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [HDFS-2452](https://issues.apache.org/jira/browse/HDFS-2452) | OutOfMemoryError in DataXceiverServer takes down the DataNode |  Major | datanode | Konstantin Shvachko | Uma Maheswara Rao G |
| [MAPREDUCE-3163](https://issues.apache.org/jira/browse/MAPREDUCE-3163) | JobClient spews errors when killing MR2 job |  Blocker | job submission, mrv2 | Todd Lipcon | Mahadev konar |
| [MAPREDUCE-3070](https://issues.apache.org/jira/browse/MAPREDUCE-3070) | NM not able to register with RM after NM restart |  Blocker | mrv2, nodemanager | Ravi Teja Ch N V | Devaraj K |
| [MAPREDUCE-3242](https://issues.apache.org/jira/browse/MAPREDUCE-3242) | Trunk compilation broken with bad interaction from MAPREDUCE-3070 and MAPREDUCE-3239. |  Major | mrv2 | Mahadev konar | Mahadev konar |
| [MAPREDUCE-3058](https://issues.apache.org/jira/browse/MAPREDUCE-3058) | Sometimes task keeps on running while its Syslog says that it is shutdown |  Critical | contrib/gridmix, mrv2 | Karam Singh | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3087](https://issues.apache.org/jira/browse/MAPREDUCE-3087) | CLASSPATH not the same after MAPREDUCE-2880 |  Critical | mrv2 | Ravi Prakash | Ravi Prakash |
| [HDFS-2491](https://issues.apache.org/jira/browse/HDFS-2491) | TestBalancer can fail when datanode utilization and avgUtilization is exactly same. |  Major | . | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [MAPREDUCE-3028](https://issues.apache.org/jira/browse/MAPREDUCE-3028) | Support job end notification in .next /0.23 |  Blocker | mrv2 | Mohammad Kamrul Islam | Ravi Prakash |
| [MAPREDUCE-3252](https://issues.apache.org/jira/browse/MAPREDUCE-3252) | MR2: Map tasks rewrite data once even if output fits in sort buffer |  Critical | mrv2, task | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-3248](https://issues.apache.org/jira/browse/MAPREDUCE-3248) | Log4j logs from unit tests are lost |  Blocker | test | Arun C Murthy | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3159](https://issues.apache.org/jira/browse/MAPREDUCE-3159) | DefaultContainerExecutor removes appcache dir on every localization |  Blocker | mrv2 | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2821](https://issues.apache.org/jira/browse/MAPREDUCE-2821) | [MR-279] Missing fields in job summary logs |  Blocker | mrv2 | Ramya Sunil | Mahadev konar |
| [MAPREDUCE-3261](https://issues.apache.org/jira/browse/MAPREDUCE-3261) | AM unable to release containers |  Major | applicationmaster | Chris Riccomini |  |
| [MAPREDUCE-3253](https://issues.apache.org/jira/browse/MAPREDUCE-3253) | ContextFactory throw NoSuchFieldException |  Blocker | mrv2 | Daniel Dai | Arun C Murthy |
| [MAPREDUCE-3263](https://issues.apache.org/jira/browse/MAPREDUCE-3263) | compile-mapred-test target fails |  Blocker | build, mrv2 | Ramya Sunil | Hitesh Shah |
| [MAPREDUCE-3269](https://issues.apache.org/jira/browse/MAPREDUCE-3269) | Jobsummary logs not being moved to a separate file |  Blocker | mrv2 | Ramya Sunil | Mahadev konar |
| [HADOOP-7764](https://issues.apache.org/jira/browse/HADOOP-7764) | Allow both ACL list and global path spec filters to HttpServer |  Blocker | . | Jonathan Eagles | Jonathan Eagles |
| [HADOOP-7766](https://issues.apache.org/jira/browse/HADOOP-7766) | The auth to local mappings are not being respected, with webhdfs and security enabled. |  Major | . | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [HDFS-2411](https://issues.apache.org/jira/browse/HDFS-2411) | with webhdfs enabled in secure mode the auth to local mappings are not being respected. |  Major | webhdfs | Arpit Gupta | Jitendra Nath Pandey |
| [HDFS-1869](https://issues.apache.org/jira/browse/HDFS-1869) | mkdirs should use the supplied permission for all of the created directories |  Major | namenode | Daryn Sharp | Daryn Sharp |
| [HADOOP-7768](https://issues.apache.org/jira/browse/HADOOP-7768) | PreCommit-HADOOP-Build is failing on hadoop-auth-examples |  Blocker | build | Jonathan Eagles | Tom White |
| [MAPREDUCE-3254](https://issues.apache.org/jira/browse/MAPREDUCE-3254) | Streaming jobs failing with PipeMapRunner ClassNotFoundException |  Blocker | contrib/streaming, mrv2 | Ramya Sunil | Arun C Murthy |
| [MAPREDUCE-3264](https://issues.apache.org/jira/browse/MAPREDUCE-3264) | mapreduce.job.user.name needs to be set automatically |  Blocker | mrv2 | Todd Lipcon | Arun C Murthy |
| [MAPREDUCE-3279](https://issues.apache.org/jira/browse/MAPREDUCE-3279) | TestJobHistoryParsing broken |  Major | mrv2 | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-3259](https://issues.apache.org/jira/browse/MAPREDUCE-3259) | ContainerLocalizer should get the proper java.library.path from LinuxContainerExecutor |  Blocker | mrv2, nodemanager | Kihwal Lee | Kihwal Lee |
| [MAPREDUCE-3240](https://issues.apache.org/jira/browse/MAPREDUCE-3240) | NM should send a SIGKILL for completed containers also |  Blocker | mrv2, nodemanager | Vinod Kumar Vavilapalli | Hitesh Shah |
| [MAPREDUCE-3228](https://issues.apache.org/jira/browse/MAPREDUCE-3228) | MR AM hangs when one node goes bad |  Blocker | applicationmaster, mrv2 | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3281](https://issues.apache.org/jira/browse/MAPREDUCE-3281) | TestLinuxContainerExecutorWithMocks failing on trunk. |  Blocker | test | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [HADOOP-7728](https://issues.apache.org/jira/browse/HADOOP-7728) | hadoop-setup-conf.sh should be modified to enable task memory manager |  Major | conf | Ramya Sunil | Ramya Sunil |
| [MAPREDUCE-3284](https://issues.apache.org/jira/browse/MAPREDUCE-3284) | bin/mapred queue fails with JobQueueClient ClassNotFoundException |  Major | mrv2 | Ramya Sunil | Arun C Murthy |
| [HADOOP-7778](https://issues.apache.org/jira/browse/HADOOP-7778) | FindBugs warning in Token.getKind() |  Major | . | Tom White | Tom White |
| [MAPREDUCE-3282](https://issues.apache.org/jira/browse/MAPREDUCE-3282) | bin/mapred job -list throws exception |  Critical | mrv2 | Ramya Sunil | Arun C Murthy |
| [MAPREDUCE-3186](https://issues.apache.org/jira/browse/MAPREDUCE-3186) | User jobs are getting hanged if the Resource manager process goes down and comes up while job is getting executed. |  Blocker | mrv2 | Ramgopal N | Eric Payne |
| [MAPREDUCE-3285](https://issues.apache.org/jira/browse/MAPREDUCE-3285) | Tests on branch-0.23 failing |  Blocker | mrv2 | Arun C Murthy | Siddharth Seth |
| [MAPREDUCE-3209](https://issues.apache.org/jira/browse/MAPREDUCE-3209) | Jenkins reports 160 FindBugs warnings |  Major | build, mrv2 | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3288](https://issues.apache.org/jira/browse/MAPREDUCE-3288) | Mapreduce 23 builds failing |  Blocker | mrv2 | Ramya Sunil | Mahadev konar |
| [MAPREDUCE-3258](https://issues.apache.org/jira/browse/MAPREDUCE-3258) | Job counters missing from AM and history UI |  Blocker | mrv2 | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-3290](https://issues.apache.org/jira/browse/MAPREDUCE-3290) | list-active-trackers throws NPE |  Major | mrv2 | Ramya Sunil | Arun C Murthy |
| [MAPREDUCE-3185](https://issues.apache.org/jira/browse/MAPREDUCE-3185) | RM Web UI does not sort the columns in some cases. |  Critical | mrv2 | Mahadev konar | Jonathan Eagles |
| [MAPREDUCE-3292](https://issues.apache.org/jira/browse/MAPREDUCE-3292) | In secure mode job submission fails with Provider org.apache.hadoop.mapreduce.security.token.JobTokenIndentifier$Renewer not found. |  Critical | mrv2 | Mahadev konar | Mahadev konar |
| [MAPREDUCE-3296](https://issues.apache.org/jira/browse/MAPREDUCE-3296) | Pending(9) findBugs warnings |  Major | build | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [HADOOP-7740](https://issues.apache.org/jira/browse/HADOOP-7740) | security audit logger is not on by default, fix the log4j properties to enable the logger |  Minor | conf | Arpit Gupta | Arpit Gupta |
| [MAPREDUCE-2775](https://issues.apache.org/jira/browse/MAPREDUCE-2775) | [MR-279] Decommissioned node does not shutdown |  Blocker | mrv2 | Ramya Sunil | Devaraj K |
| [MAPREDUCE-3304](https://issues.apache.org/jira/browse/MAPREDUCE-3304) | TestRMContainerAllocator#testBlackListedNodes fails intermittently |  Major | mrv2, test | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-3306](https://issues.apache.org/jira/browse/MAPREDUCE-3306) | Cannot run apps after MAPREDUCE-2989 |  Blocker | mrv2, nodemanager | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3295](https://issues.apache.org/jira/browse/MAPREDUCE-3295) | TestAMAuthorization failing on branch 0.23. |  Critical | . | Mahadev konar |  |
| [HADOOP-7770](https://issues.apache.org/jira/browse/HADOOP-7770) | ViewFS getFileChecksum throws FileNotFoundException for files in /tmp and /user |  Blocker | viewfs | Ravi Prakash | Ravi Prakash |
| [HDFS-2436](https://issues.apache.org/jira/browse/HDFS-2436) | FSNamesystem.setTimes(..) expects the path is a file. |  Major | . | Arpit Gupta | Uma Maheswara Rao G |
| [MAPREDUCE-3274](https://issues.apache.org/jira/browse/MAPREDUCE-3274) | Race condition in MR App Master Preemtion can cause a dead lock |  Blocker | applicationmaster, mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-3313](https://issues.apache.org/jira/browse/MAPREDUCE-3313) | TestResourceTrackerService failing in trunk some times |  Blocker | mrv2, test | Ravi Gummadi | Hitesh Shah |
| [MAPREDUCE-3198](https://issues.apache.org/jira/browse/MAPREDUCE-3198) | Change mode for hadoop-mapreduce-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/resources/mock-container-executor to 755 |  Trivial | mrv2 | Hitesh Shah | Arun C Murthy |
| [MAPREDUCE-3262](https://issues.apache.org/jira/browse/MAPREDUCE-3262) | A few events are not handled by the NodeManager in failure scenarios |  Critical | mrv2, nodemanager | Hitesh Shah | Hitesh Shah |
| [MAPREDUCE-3241](https://issues.apache.org/jira/browse/MAPREDUCE-3241) | (Rumen)TraceBuilder throws IllegalArgumentException |  Major | . | Devaraj K | Amar Kamat |
| [MAPREDUCE-3035](https://issues.apache.org/jira/browse/MAPREDUCE-3035) | MR V2 jobhistory does not contain rack information |  Critical | mrv2 | Karam Singh | chackaravarthy |
| [HDFS-2065](https://issues.apache.org/jira/browse/HDFS-2065) | Fix NPE in DFSClient.getFileChecksum |  Major | . | Bharath Mundlapudi | Uma Maheswara Rao G |
| [MAPREDUCE-3321](https://issues.apache.org/jira/browse/MAPREDUCE-3321) | Disable some failing legacy tests for MRv2 builds to go through |  Minor | mrv2 | Hitesh Shah | Hitesh Shah |
| [HADOOP-7771](https://issues.apache.org/jira/browse/HADOOP-7771) | NPE when running hdfs dfs -copyToLocal, -get etc |  Blocker | . | John George | John George |
| [MAPREDUCE-3316](https://issues.apache.org/jira/browse/MAPREDUCE-3316) | Rebooted link is not working properly |  Major | resourcemanager | Bhallamudi Venkata Siva Kamesh | Bhallamudi Venkata Siva Kamesh |
| [MAPREDUCE-3317](https://issues.apache.org/jira/browse/MAPREDUCE-3317) | Rumen TraceBuilder is emiting null as hostname |  Major | tools/rumen | Ravi Gummadi | Ravi Gummadi |
| [HADOOP-7782](https://issues.apache.org/jira/browse/HADOOP-7782) | Aggregate project javadocs |  Critical | build | Arun C Murthy | Tom White |
| [HDFS-2002](https://issues.apache.org/jira/browse/HDFS-2002) | Incorrect computation of needed blocks in getTurnOffTip() |  Major | namenode | Konstantin Shvachko | Plamen Jeliazkov |
| [MAPREDUCE-3332](https://issues.apache.org/jira/browse/MAPREDUCE-3332) | contrib/raid compile breaks due to changes in hdfs/protocol/datatransfer/Sender#writeBlock related to checksum handling |  Trivial | contrib/raid | Hitesh Shah | Hitesh Shah |
| [MAPREDUCE-3143](https://issues.apache.org/jira/browse/MAPREDUCE-3143) | Complete aggregation of user-logs spit out by containers onto DFS |  Major | mrv2, nodemanager | Vinod Kumar Vavilapalli |  |
| [HADOOP-7798](https://issues.apache.org/jira/browse/HADOOP-7798) | Release artifacts need to be signed for Nexus |  Blocker | build | Arun C Murthy | Doug Cutting |
| [HADOOP-7797](https://issues.apache.org/jira/browse/HADOOP-7797) | Fix the repository name to support pushing to the staging area of Nexus |  Major | build | Owen O'Malley | Owen O'Malley |
| [MAPREDUCE-3337](https://issues.apache.org/jira/browse/MAPREDUCE-3337) | Missing license headers for some files |  Blocker | mrv2 | Arun C Murthy | Arun C Murthy |
| [HDFS-1943](https://issues.apache.org/jira/browse/HDFS-1943) | fail to start datanode while start-dfs.sh is executed by root user |  Blocker | scripts | Wei Yongjun | Matt Foley |
| [HDFS-2346](https://issues.apache.org/jira/browse/HDFS-2346) | TestHost2NodesMap & TestReplicasMap will fail depending upon execution order of test methods |  Blocker | test | Uma Maheswara Rao G | Laxman |
| [HADOOP-7176](https://issues.apache.org/jira/browse/HADOOP-7176) | Redesign FsShell |  Major | . | Daryn Sharp | Daryn Sharp |
| [HADOOP-9128](https://issues.apache.org/jira/browse/HADOOP-9128) | MetricsDynamicMBeanBase can cause high cpu load |  Major | metrics | Nate Putnam |  |
| [HADOOP-8389](https://issues.apache.org/jira/browse/HADOOP-8389) | MetricsDynamicMBeanBase throws IllegalArgumentException for empty attribute list |  Major | metrics | Elias Ross |  |
| [MAPREDUCE-1506](https://issues.apache.org/jira/browse/MAPREDUCE-1506) | Assertion failure in TestTaskTrackerMemoryManager |  Major | tasktracker | Aaron Kimball |  |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-1927](https://issues.apache.org/jira/browse/MAPREDUCE-1927) | unit test for HADOOP-6835 (concatenated gzip support) |  Minor | test | Greg Roelofs | Greg Roelofs |
| [MAPREDUCE-2331](https://issues.apache.org/jira/browse/MAPREDUCE-2331) | Add coverage of task graph servlet to fair scheduler system test |  Major | . | Todd Lipcon | Todd Lipcon |
| [HDFS-780](https://issues.apache.org/jira/browse/HDFS-780) | Revive TestFuseDFS |  Major | fuse-dfs | Eli Collins | Eli Collins |
| [HDFS-1770](https://issues.apache.org/jira/browse/HDFS-1770) | TestFiRename fails due to invalid block size |  Minor | . | Eli Collins | Eli Collins |
| [MAPREDUCE-2426](https://issues.apache.org/jira/browse/MAPREDUCE-2426) | Make TestFairSchedulerSystem fail with more verbose output |  Trivial | contrib/fair-share | Todd Lipcon | Todd Lipcon |
| [HDFS-1844](https://issues.apache.org/jira/browse/HDFS-1844) | Move -fs usage tests from hdfs into common |  Major | test | Daryn Sharp | Daryn Sharp |
| [HADOOP-7230](https://issues.apache.org/jira/browse/HADOOP-7230) | Move -fs usage tests from hdfs into common |  Major | test | Daryn Sharp | Daryn Sharp |
| [HDFS-1855](https://issues.apache.org/jira/browse/HDFS-1855) | TestDatanodeBlockScanner.testBlockCorruptionRecoveryPolicy() part 2 fails in two different ways |  Major | test | Matt Foley | Matt Foley |
| [HDFS-1862](https://issues.apache.org/jira/browse/HDFS-1862) | Improve test reliability of HDFS-1594 |  Major | test | Aaron T. Myers | Aaron T. Myers |
| [HDFS-1902](https://issues.apache.org/jira/browse/HDFS-1902) | Fix path display for setrep |  Major | test | Daryn Sharp | Daryn Sharp |
| [HDFS-1911](https://issues.apache.org/jira/browse/HDFS-1911) | HDFS tests for viewfs |  Major | . | Sanjay Radia | Sanjay Radia |
| [HDFS-1912](https://issues.apache.org/jira/browse/HDFS-1912) | Update tests for FsShell standardized error messages |  Major | test | Daryn Sharp | Daryn Sharp |
| [HDFS-1903](https://issues.apache.org/jira/browse/HDFS-1903) | Fix path display for rm/rmr |  Major | test | Daryn Sharp | Daryn Sharp |
| [HDFS-1928](https://issues.apache.org/jira/browse/HDFS-1928) | Fix path display for touchz |  Major | test | Daryn Sharp | Daryn Sharp |
| [HDFS-1933](https://issues.apache.org/jira/browse/HDFS-1933) | Update tests for FsShell's "test" |  Major | test | Daryn Sharp | Daryn Sharp |
| [HDFS-1931](https://issues.apache.org/jira/browse/HDFS-1931) | Update tests for du/dus/df |  Major | . | Daryn Sharp | Daryn Sharp |
| [HDFS-1877](https://issues.apache.org/jira/browse/HDFS-1877) | Create a functional test for file read/write |  Minor | test | CW Chung | CW Chung |
| [HDFS-1983](https://issues.apache.org/jira/browse/HDFS-1983) | Fix path display for copy & rm |  Major | test | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-2536](https://issues.apache.org/jira/browse/MAPREDUCE-2536) | TestMRCLI broke due to change in usage output |  Minor | test | Daryn Sharp | Daryn Sharp |
| [HDFS-1968](https://issues.apache.org/jira/browse/HDFS-1968) | Enhance TestWriteRead to support File Append and Position Read |  Minor | test | CW Chung | CW Chung |
| [MAPREDUCE-2081](https://issues.apache.org/jira/browse/MAPREDUCE-2081) | [GridMix3] Implement functionality for get the list of job traces which has different intervals. |  Major | contrib/gridmix | Vinay Kumar Thota | Vinay Kumar Thota |
| [HDFS-2100](https://issues.apache.org/jira/browse/HDFS-2100) | Improve TestStorageRestore |  Minor | test | Aaron T. Myers | Aaron T. Myers |
| [HDFS-2111](https://issues.apache.org/jira/browse/HDFS-2111) | Add tests for ensuring that the DN will start with a few bad data directories (Part 1 of testing DiskChecker) |  Major | datanode, test | Harsh J | Harsh J |
| [HDFS-2131](https://issues.apache.org/jira/browse/HDFS-2131) | Tests for HADOOP-7361 |  Major | test | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-2154](https://issues.apache.org/jira/browse/HDFS-2154) | TestDFSShell should use test dir |  Minor | test | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-7298](https://issues.apache.org/jira/browse/HADOOP-7298) | Add test utility for writing multi-threaded tests |  Major | test | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2782](https://issues.apache.org/jira/browse/MAPREDUCE-2782) | MR-279: Unit (mockito) tests for CS |  Major | mrv2 | Arun C Murthy | Arun C Murthy |
| [HADOOP-7526](https://issues.apache.org/jira/browse/HADOOP-7526) | Add TestPath tests for URI conversion and reserved characters |  Minor | fs | Eli Collins | Eli Collins |
| [HDFS-2233](https://issues.apache.org/jira/browse/HDFS-2233) | Add WebUI tests with URI reserved chars in the path and filename |  Major | namenode | Eli Collins | Eli Collins |
| [HDFS-2332](https://issues.apache.org/jira/browse/HDFS-2332) | Add test for HADOOP-7629: using an immutable FsPermission as an IPC parameter |  Major | test | Todd Lipcon | Todd Lipcon |
| [HDFS-2522](https://issues.apache.org/jira/browse/HDFS-2522) | Disable TestDfsOverAvroRpc in 0.23 |  Minor | . | Suresh Srinivas | Suresh Srinivas |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-6683](https://issues.apache.org/jira/browse/HADOOP-6683) | the first optimization: ZlibCompressor does not fully utilize the buffer |  Minor | io | Kang Xiao | Kang Xiao |
| [HDFS-1473](https://issues.apache.org/jira/browse/HDFS-1473) | Refactor storage management into separate classes than fsimage file reading/writing |  Major | namenode | Todd Lipcon | Todd Lipcon |
| [HDFS-1557](https://issues.apache.org/jira/browse/HDFS-1557) | Separate Storage from FSImage |  Major | namenode | Ivan Kelly | Ivan Kelly |
| [HDFS-1629](https://issues.apache.org/jira/browse/HDFS-1629) | Add a method to BlockPlacementPolicy for not removing the chosen nodes |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-1445](https://issues.apache.org/jira/browse/HDFS-1445) | Batch the calls in DataStorage to FileUtil.createHardLink(), so we call it once per directory instead of once per file |  Major | datanode | Matt Foley | Matt Foley |
| [HDFS-1541](https://issues.apache.org/jira/browse/HDFS-1541) | Not marking datanodes dead When namenode in safemode |  Major | namenode | Hairong Kuang | Hairong Kuang |
| [HDFS-1767](https://issues.apache.org/jira/browse/HDFS-1767) | Namenode should ignore non-initial block reports from datanodes when in safemode during startup |  Major | datanode | Matt Foley | Matt Foley |
| [HDFS-1070](https://issues.apache.org/jira/browse/HDFS-1070) | Speedup NameNode image loading and saving by storing local file names |  Major | namenode | Hairong Kuang | Hairong Kuang |
| [HDFS-1854](https://issues.apache.org/jira/browse/HDFS-1854) | make failure message more useful in DFSTestUtil.waitReplication() |  Major | test | Matt Foley | Matt Foley |
| [HDFS-1856](https://issues.apache.org/jira/browse/HDFS-1856) | TestDatanodeBlockScanner waits forever, errs without giving information |  Major | test | Matt Foley | Matt Foley |
| [HDFS-1398](https://issues.apache.org/jira/browse/HDFS-1398) | HDFS federation: Upgrade and rolling back of Federation |  Major | . | Tanping Wang |  |
| [HADOOP-6919](https://issues.apache.org/jira/browse/HADOOP-6919) | Metrics2: metrics framework |  Major | metrics | Luke Lu | Luke Lu |
| [MAPREDUCE-2422](https://issues.apache.org/jira/browse/MAPREDUCE-2422) | Removed unused internal methods from DistributedCache |  Major | client | Tom White | Tom White |
| [HDFS-1826](https://issues.apache.org/jira/browse/HDFS-1826) | NameNode should save image to name directories in parallel during upgrade |  Major | namenode | Hairong Kuang | Matt Foley |
| [HDFS-1883](https://issues.apache.org/jira/browse/HDFS-1883) | Recurring failures in TestBackupNode since HDFS-1052 |  Major | test | Matt Foley |  |
| [HDFS-1922](https://issues.apache.org/jira/browse/HDFS-1922) | Recurring failure in TestJMXGet.testNameNode since build 477 on May 11 |  Major | test | Matt Foley | Luke Lu |
| [HDFS-1828](https://issues.apache.org/jira/browse/HDFS-1828) | TestBlocksWithNotEnoughRacks intermittently fails assert |  Major | namenode | Matt Foley | Matt Foley |
| [MAPREDUCE-2522](https://issues.apache.org/jira/browse/MAPREDUCE-2522) | MR 279: Security for JobHistory service |  Major | mrv2 | Siddharth Seth | Siddharth Seth |
| [HDFS-1884](https://issues.apache.org/jira/browse/HDFS-1884) | Improve TestDFSStorageStateRecovery |  Major | test | Matt Foley | Aaron T. Myers |
| [HDFS-1923](https://issues.apache.org/jira/browse/HDFS-1923) | Intermittent recurring failure in TestFiDataTransferProtocol2.pipeline\_Fi\_29 |  Major | test | Matt Foley | Tsz Wo Nicholas Sze |
| [HDFS-1295](https://issues.apache.org/jira/browse/HDFS-1295) | Improve namenode restart times by short-circuiting the first block reports from datanodes |  Major | namenode | dhruba borthakur | Matt Foley |
| [HDFS-2069](https://issues.apache.org/jira/browse/HDFS-2069) | Incorrect default trash interval value in the docs |  Trivial | documentation | Ravi Phulari | Harsh J |
| [HADOOP-7380](https://issues.apache.org/jira/browse/HADOOP-7380) | Add client failover functionality to o.a.h.io.(ipc\|retry) |  Major | ha, ipc | Aaron T. Myers | Aaron T. Myers |
| [HDFS-2134](https://issues.apache.org/jira/browse/HDFS-2134) | Move DecommissionManager to block management |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-2140](https://issues.apache.org/jira/browse/HDFS-2140) | Move Host2NodesMap to block management |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-2159](https://issues.apache.org/jira/browse/HDFS-2159) | Deprecate DistributedFileSystem.getClient() |  Major | hdfs-client | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-7465](https://issues.apache.org/jira/browse/HADOOP-7465) | A several tiny improvements for the LOG format |  Trivial | fs, ipc | XieXianshan | XieXianshan |
| [HDFS-2147](https://issues.apache.org/jira/browse/HDFS-2147) | Move cluster network topology to block management |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-2141](https://issues.apache.org/jira/browse/HDFS-2141) | Remove NameNode roles Active and Standby (they become states) |  Major | ha, namenode | Suresh Srinivas | Suresh Srinivas |
| [HDFS-2167](https://issues.apache.org/jira/browse/HDFS-2167) | Move dnsToSwitchMapping and hostsReader from FSNamesystem to DatanodeManager |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-2112](https://issues.apache.org/jira/browse/HDFS-2112) | Move ReplicationMonitor to block management |  Major | namenode | Tsz Wo Nicholas Sze | Uma Maheswara Rao G |
| [HDFS-2149](https://issues.apache.org/jira/browse/HDFS-2149) | Move EditLogOp serialization formats into FsEditLogOp implementations |  Major | namenode | Ivan Kelly | Ivan Kelly |
| [HDFS-2191](https://issues.apache.org/jira/browse/HDFS-2191) | Move datanodeMap from FSNamesystem to DatanodeManager |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-2200](https://issues.apache.org/jira/browse/HDFS-2200) | Set FSNamesystem.LOG to package private |  Minor | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-2199](https://issues.apache.org/jira/browse/HDFS-2199) | Move blockTokenSecretManager from FSNamesystem to BlockManager |  Major | namenode | Tsz Wo Nicholas Sze | Uma Maheswara Rao G |
| [MAPREDUCE-2641](https://issues.apache.org/jira/browse/MAPREDUCE-2641) | Fix the ExponentiallySmoothedTaskRuntimeEstimator and its unit test |  Minor | mrv2 | Josh Wills | Josh Wills |
| [HADOOP-6671](https://issues.apache.org/jira/browse/HADOOP-6671) | To use maven for hadoop common builds |  Major | build | Giridharan Kesavan | Alejandro Abdelnur |
| [HADOOP-7502](https://issues.apache.org/jira/browse/HADOOP-7502) | Use canonical (IDE friendly) generated-sources directory for generated sources |  Major | . | Luke Lu | Luke Lu |
| [HADOOP-7501](https://issues.apache.org/jira/browse/HADOOP-7501) | publish Hadoop Common artifacts (post HADOOP-6671) to Apache SNAPSHOTs repo |  Major | build | Alejandro Abdelnur | Tom White |
| [HADOOP-7508](https://issues.apache.org/jira/browse/HADOOP-7508) | compiled nativelib is in wrong directory and it is not picked up by surefire setup |  Major | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [HDFS-2108](https://issues.apache.org/jira/browse/HDFS-2108) | Move datanode heartbeat handling to BlockManager |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-7515](https://issues.apache.org/jira/browse/HADOOP-7515) | test-patch reports the wrong number of javadoc warnings |  Major | build | Tom White | Tom White |
| [HDFS-2228](https://issues.apache.org/jira/browse/HDFS-2228) | Move block and datanode code from FSNamesystem to BlockManager and DatanodeManager |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-7528](https://issues.apache.org/jira/browse/HADOOP-7528) | Maven build fails in Windows |  Major | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-7525](https://issues.apache.org/jira/browse/HADOOP-7525) | Make arguments to test-patch optional |  Major | scripts | Tom White | Tom White |
| [HDFS-2239](https://issues.apache.org/jira/browse/HDFS-2239) | Reduce access levels of the fields and methods in FSNamesystem |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-2237](https://issues.apache.org/jira/browse/HDFS-2237) | Change UnderReplicatedBlocks from public to package private |  Minor | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-7533](https://issues.apache.org/jira/browse/HADOOP-7533) | Allow test-patch to be run from any subproject directory |  Major | . | Tom White | Tom White |
| [HDFS-2265](https://issues.apache.org/jira/browse/HDFS-2265) | Remove unnecessary BlockTokenSecretManager fields/methods from BlockManager |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-7498](https://issues.apache.org/jira/browse/HADOOP-7498) | Remove legacy TAR layout creation |  Major | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-7496](https://issues.apache.org/jira/browse/HADOOP-7496) | break Maven TAR & bintar profiles into just LAYOUT & TAR proper |  Major | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [HDFS-2266](https://issues.apache.org/jira/browse/HDFS-2266) | Add a Namesystem interface to avoid directly referring to FSNamesystem |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-7561](https://issues.apache.org/jira/browse/HADOOP-7561) | Make test-patch only run tests for changed modules |  Major | . | Tom White | Tom White |
| [HADOOP-7560](https://issues.apache.org/jira/browse/HADOOP-7560) | Make hadoop-common a POM module with sub-modules (common & alfredo) |  Major | . | Alejandro Abdelnur | Alejandro Abdelnur |
| [MAPREDUCE-2807](https://issues.apache.org/jira/browse/MAPREDUCE-2807) | MR-279: AM restart does not work after RM refactor |  Major | applicationmaster, mrv2, resourcemanager | Sharad Agarwal | Sharad Agarwal |
| [MAPREDUCE-2891](https://issues.apache.org/jira/browse/MAPREDUCE-2891) | Docs for core protocols in yarn-api - AMRMProtocol |  Major | documentation, mrv2 | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-2898](https://issues.apache.org/jira/browse/MAPREDUCE-2898) | Docs for core protocols in yarn-api - ContainerManager |  Major | documentation, mrv2 | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-2897](https://issues.apache.org/jira/browse/MAPREDUCE-2897) | Docs for core protocols in yarn-api - ClientRMProtocol |  Major | documentation, mrv2 | Arun C Murthy | Arun C Murthy |
| [HDFS-2284](https://issues.apache.org/jira/browse/HDFS-2284) | Write Http access to HDFS |  Major | . | Sanjay Radia | Tsz Wo Nicholas Sze |
| [MAPREDUCE-2933](https://issues.apache.org/jira/browse/MAPREDUCE-2933) | Change allocate call to return ContainerStatus for completed containers rather than Container |  Blocker | applicationmaster, mrv2, nodemanager, resourcemanager | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-2896](https://issues.apache.org/jira/browse/MAPREDUCE-2896) | Remove all apis other than getters and setters in all org/apache/hadoop/yarn/api/records/\* |  Major | mrv2 | Arun C Murthy | Arun C Murthy |
| [HDFS-2317](https://issues.apache.org/jira/browse/HDFS-2317) | Read access to HDFS using HTTP REST |  Major | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [MAPREDUCE-2899](https://issues.apache.org/jira/browse/MAPREDUCE-2899) | Replace major parts of ApplicationSubmissionContext with a ContainerLaunchContext |  Major | mrv2, resourcemanager | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-3007](https://issues.apache.org/jira/browse/MAPREDUCE-3007) | JobClient cannot talk to JobHistory server in secure mode |  Major | jobhistoryserver, mrv2 | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [HDFS-2338](https://issues.apache.org/jira/browse/HDFS-2338) | Configuration option to enable/disable webhdfs. |  Major | webhdfs | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [HDFS-2318](https://issues.apache.org/jira/browse/HDFS-2318) | Provide authentication to webhdfs using SPNEGO |  Major | webhdfs | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [MAPREDUCE-2909](https://issues.apache.org/jira/browse/MAPREDUCE-2909) | Docs for remaining records in yarn-api |  Major | documentation, mrv2 | Arun C Murthy | Arun C Murthy |
| [HDFS-2340](https://issues.apache.org/jira/browse/HDFS-2340) | Support getFileBlockLocations and getDelegationToken in webhdfs |  Major | webhdfs | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-2356](https://issues.apache.org/jira/browse/HDFS-2356) | webhdfs: support case insensitive query parameter names |  Major | webhdfs | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-2366](https://issues.apache.org/jira/browse/HDFS-2366) | webhdfs throws a npe when ugi is null from getDelegationToken |  Major | webhdfs | Arpit Gupta | Tsz Wo Nicholas Sze |
| [MAPREDUCE-3099](https://issues.apache.org/jira/browse/MAPREDUCE-3099) | Add docs for setting up a single node MRv2 cluster. |  Major | . | Mahadev konar | Mahadev konar |
| [HDFS-2363](https://issues.apache.org/jira/browse/HDFS-2363) | Move datanodes size printing to BlockManager from FSNameSystem's metasave API |  Minor | namenode | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [MAPREDUCE-3098](https://issues.apache.org/jira/browse/MAPREDUCE-3098) | Report Application status as well as ApplicationMaster status in GetApplicationReportResponse |  Blocker | mrv2 | Hitesh Shah | Hitesh Shah |
| [HADOOP-7469](https://issues.apache.org/jira/browse/HADOOP-7469) | add a standard handler for socket connection problems which improves diagnostics |  Minor | util | Steve Loughran | Steve Loughran |
| [HDFS-2348](https://issues.apache.org/jira/browse/HDFS-2348) | Support getContentSummary and getFileChecksum in webhdfs |  Major | webhdfs | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [MAPREDUCE-2889](https://issues.apache.org/jira/browse/MAPREDUCE-2889) | Add docs for writing new application frameworks |  Critical | documentation, mrv2 | Arun C Murthy | Hitesh Shah |
| [MAPREDUCE-3137](https://issues.apache.org/jira/browse/MAPREDUCE-3137) | Fix broken merge of MR-2719 to 0.23 branch for the distributed shell test case |  Trivial | mrv2 | Hitesh Shah | Hitesh Shah |
| [MAPREDUCE-3134](https://issues.apache.org/jira/browse/MAPREDUCE-3134) | Add documentation for CapacityScheduler |  Blocker | documentation, mrv2, scheduler | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-2792](https://issues.apache.org/jira/browse/MAPREDUCE-2792) | [MR-279] Replace IP addresses with hostnames |  Blocker | mrv2, security | Ramya Sunil | Vinod Kumar Vavilapalli |
| [MAPREDUCE-3013](https://issues.apache.org/jira/browse/MAPREDUCE-3013) | Remove YarnConfiguration.YARN\_SECURITY\_INFO |  Major | mrv2, security | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-2702](https://issues.apache.org/jira/browse/MAPREDUCE-2702) | [MR-279] OutputCommitter changes for MR Application Master recovery |  Blocker | applicationmaster, mrv2 | Sharad Agarwal | Sharad Agarwal |
| [HDFS-2395](https://issues.apache.org/jira/browse/HDFS-2395) | webhdfs api's should return a root element in the json response |  Critical | webhdfs | Arpit Gupta | Tsz Wo Nicholas Sze |
| [HDFS-2403](https://issues.apache.org/jira/browse/HDFS-2403) | The renewer in NamenodeWebHdfsMethods.generateDelegationToken(..) is not used |  Major | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [MAPREDUCE-3141](https://issues.apache.org/jira/browse/MAPREDUCE-3141) | Yarn+MR secure mode is broken, uncovered after MAPREDUCE-3056 |  Blocker | applicationmaster, mrv2, security | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [HDFS-2404](https://issues.apache.org/jira/browse/HDFS-2404) | webhdfs liststatus json response is not correct |  Major | webhdfs | Arpit Gupta | Suresh Srinivas |
| [MAPREDUCE-2988](https://issues.apache.org/jira/browse/MAPREDUCE-2988) | Reenable TestLinuxContainerExecutor reflecting the current NM code. |  Critical | mrv2, security, test | Eric Payne | Robert Joseph Evans |
| [MAPREDUCE-3148](https://issues.apache.org/jira/browse/MAPREDUCE-3148) | Port MAPREDUCE-2702 to old mapred api |  Blocker | mrv2 | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-2666](https://issues.apache.org/jira/browse/MAPREDUCE-2666) | MR-279: Need to retrieve shuffle port number on ApplicationMaster restart |  Blocker | mrv2 | Robert Joseph Evans | Jonathan Eagles |
| [HDFS-2441](https://issues.apache.org/jira/browse/HDFS-2441) | webhdfs returns two content-type headers |  Major | webhdfs | Arpit Gupta | Tsz Wo Nicholas Sze |
| [HDFS-2428](https://issues.apache.org/jira/browse/HDFS-2428) | webhdfs api parameter validation should be better |  Major | webhdfs | Arpit Gupta | Tsz Wo Nicholas Sze |
| [HDFS-2424](https://issues.apache.org/jira/browse/HDFS-2424) | webhdfs liststatus json does not convert to a valid xml document |  Major | webhdfs | Arpit Gupta | Tsz Wo Nicholas Sze |
| [HDFS-2439](https://issues.apache.org/jira/browse/HDFS-2439) | webhdfs open an invalid path leads to a 500 which states a npe, we should return a 404 with appropriate error message |  Major | webhdfs | Arpit Gupta | Tsz Wo Nicholas Sze |
| [MAPREDUCE-3136](https://issues.apache.org/jira/browse/MAPREDUCE-3136) | Add docs for setting up real-world MRv2 clusters |  Blocker | documentation, mrv2 | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-3127](https://issues.apache.org/jira/browse/MAPREDUCE-3127) | Unable to restrict users based on resourcemanager.admin.acls value set |  Blocker | mrv2, resourcemanager | Amol Kekre | Arun C Murthy |
| [MAPREDUCE-3144](https://issues.apache.org/jira/browse/MAPREDUCE-3144) | Augment JobHistory to include information needed for serving aggregated logs. |  Critical | mrv2 | Vinod Kumar Vavilapalli | Siddharth Seth |
| [HDFS-2453](https://issues.apache.org/jira/browse/HDFS-2453) | tail using a webhdfs uri throws an error |  Major | webhdfs | Arpit Gupta | Tsz Wo Nicholas Sze |
| [MAPREDUCE-3104](https://issues.apache.org/jira/browse/MAPREDUCE-3104) | Implement Application ACLs, Queue ACLs and their interaction |  Blocker | mrv2, resourcemanager, security | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [HDFS-2427](https://issues.apache.org/jira/browse/HDFS-2427) | webhdfs mkdirs api call creates path with 777 permission, we should default it to 755 |  Major | webhdfs | Arpit Gupta | Tsz Wo Nicholas Sze |
| [HADOOP-7564](https://issues.apache.org/jira/browse/HADOOP-7564) | Remove test-patch SVN externals |  Major | . | Tom White | Tom White |
| [MAPREDUCE-3233](https://issues.apache.org/jira/browse/MAPREDUCE-3233) | AM fails to restart when first AM is killed |  Blocker | mrv2 | Karam Singh | Mahadev konar |
| [MAPREDUCE-2708](https://issues.apache.org/jira/browse/MAPREDUCE-2708) | [MR-279] Design and implement MR Application Master recovery |  Blocker | applicationmaster, mrv2 | Sharad Agarwal | Sharad Agarwal |
| [MAPREDUCE-2747](https://issues.apache.org/jira/browse/MAPREDUCE-2747) | [MR-279] [Security] Cleanup LinuxContainerExecutor binary sources |  Blocker | mrv2, nodemanager, security | Vinod Kumar Vavilapalli | Robert Joseph Evans |
| [MAPREDUCE-3249](https://issues.apache.org/jira/browse/MAPREDUCE-3249) | Recovery of MR AMs with reduces fails the subsequent generation of the job |  Blocker | applicationmaster, mrv2 | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-2746](https://issues.apache.org/jira/browse/MAPREDUCE-2746) | [MR-279] [Security] Yarn servers can't communicate with each other with hadoop.security.authorization set to true |  Blocker | mrv2, security | Vinod Kumar Vavilapalli | Arun C Murthy |
| [MAPREDUCE-2977](https://issues.apache.org/jira/browse/MAPREDUCE-2977) | ResourceManager needs to renew and cancel tokens associated with a job |  Blocker | mrv2, resourcemanager, security | Owen O'Malley | Arun C Murthy |
| [MAPREDUCE-3250](https://issues.apache.org/jira/browse/MAPREDUCE-3250) | When AM restarts, client keeps reconnecting to the new AM and prints a lots of logs. |  Blocker | applicationmaster, mrv2 | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [HDFS-2501](https://issues.apache.org/jira/browse/HDFS-2501) | add version prefix and root methods to webhdfs |  Major | webhdfs | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [MAPREDUCE-2858](https://issues.apache.org/jira/browse/MAPREDUCE-2858) | MRv2 WebApp Security |  Blocker | applicationmaster, mrv2, security | Luke Lu | Robert Joseph Evans |
| [HDFS-2494](https://issues.apache.org/jira/browse/HDFS-2494) | [webhdfs] When Getting the file using OP=OPEN with DN http address, ESTABLISHED sockets are growing. |  Major | webhdfs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [MAPREDUCE-3175](https://issues.apache.org/jira/browse/MAPREDUCE-3175) | Yarn httpservers not created with access Control lists |  Blocker | mrv2 | Thomas Graves | Jonathan Eagles |
| [MAPREDUCE-3257](https://issues.apache.org/jira/browse/MAPREDUCE-3257) | Authorization checks needed for AM-\>RM protocol |  Blocker | applicationmaster, mrv2, resourcemanager, security | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [HADOOP-7753](https://issues.apache.org/jira/browse/HADOOP-7753) | Support fadvise and sync\_data\_range in NativeIO, add ReadaheadPool class |  Major | io, native, performance | Todd Lipcon | Todd Lipcon |
| [HDFS-2432](https://issues.apache.org/jira/browse/HDFS-2432) | webhdfs setreplication api should return a 403 when called on a directory |  Major | webhdfs | Arpit Gupta | Tsz Wo Nicholas Sze |
| [MAPREDUCE-2989](https://issues.apache.org/jira/browse/MAPREDUCE-2989) | JobHistory should link to task logs |  Critical | mrv2 | Siddharth Seth | Siddharth Seth |
| [HDFS-2493](https://issues.apache.org/jira/browse/HDFS-2493) | Remove reference to FSNamesystem in blockmanagement classes |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [MAPREDUCE-3256](https://issues.apache.org/jira/browse/MAPREDUCE-3256) | Authorization checks needed for AM-\>NM protocol |  Blocker | applicationmaster, mrv2, nodemanager, security | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-2766](https://issues.apache.org/jira/browse/MAPREDUCE-2766) | [MR-279] Set correct permissions for files in dist cache |  Blocker | mrv2 | Ramya Sunil | Hitesh Shah |
| [MAPREDUCE-3146](https://issues.apache.org/jira/browse/MAPREDUCE-3146) | Add a MR specific command line to dump logs for a given TaskAttemptID |  Critical | mrv2, nodemanager | Vinod Kumar Vavilapalli | Siddharth Seth |
| [MAPREDUCE-2696](https://issues.apache.org/jira/browse/MAPREDUCE-2696) | Container logs aren't getting cleaned up when LogAggregation is disabled |  Major | mrv2, nodemanager | Arun C Murthy | Siddharth Seth |
| [HDFS-2385](https://issues.apache.org/jira/browse/HDFS-2385) | Support delegation token renewal in webhdfs |  Major | webhdfs | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [MAPREDUCE-3220](https://issues.apache.org/jira/browse/MAPREDUCE-3220) | ant test TestCombineOutputCollector failing on trunk |  Minor | mrv2, test | Hitesh Shah | Devaraj K |
| [MAPREDUCE-3103](https://issues.apache.org/jira/browse/MAPREDUCE-3103) | Implement Job ACLs for MRAppMaster |  Blocker | mrv2, security | Vinod Kumar Vavilapalli | Mahadev konar |
| [HDFS-2416](https://issues.apache.org/jira/browse/HDFS-2416) | distcp with a webhdfs uri on a secure cluster fails |  Major | webhdfs | Arpit Gupta | Jitendra Nath Pandey |
| [HDFS-2527](https://issues.apache.org/jira/browse/HDFS-2527) | Remove the use of Range header from webhdfs |  Major | webhdfs | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-2528](https://issues.apache.org/jira/browse/HDFS-2528) | webhdfs rest call to a secure dn fails when a token is sent |  Major | webhdfs | Arpit Gupta | Tsz Wo Nicholas Sze |
| [HDFS-2540](https://issues.apache.org/jira/browse/HDFS-2540) | Change WebHdfsFileSystem to two-step create/append |  Major | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-2539](https://issues.apache.org/jira/browse/HDFS-2539) | Support doAs and GETHOMEDIRECTORY in webhdfs |  Major | webhdfs | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-7001](https://issues.apache.org/jira/browse/HADOOP-7001) | Allow configuration changes without restarting configured nodes |  Major | conf | Patrick Kling | Patrick Kling |
| [MAPREDUCE-2517](https://issues.apache.org/jira/browse/MAPREDUCE-2517) | Porting Gridmix v3 system tests into trunk branch. |  Major | contrib/gridmix | Vinay Kumar Thota | Vinay Kumar Thota |
| [MAPREDUCE-2544](https://issues.apache.org/jira/browse/MAPREDUCE-2544) | Gridmix compression emulation system tests. |  Major | contrib/gridmix | Vinay Kumar Thota | Vinay Kumar Thota |
| [MAPREDUCE-2554](https://issues.apache.org/jira/browse/MAPREDUCE-2554) | Gridmix distributed cache emulation system tests. |  Major | contrib/gridmix | Vinay Kumar Thota | Yiting Wu |
| [MAPREDUCE-2563](https://issues.apache.org/jira/browse/MAPREDUCE-2563) | Gridmix high ram jobs emulation system tests. |  Major | contrib/gridmix | Vinay Kumar Thota | Vinay Kumar Thota |
| [MAPREDUCE-2033](https://issues.apache.org/jira/browse/MAPREDUCE-2033) | [Herriot] Gridmix generate data tests with various submission policies and different user resolvers. |  Major | contrib/gridmix | Vinay Kumar Thota | Vinay Kumar Thota |
| [MAPREDUCE-2053](https://issues.apache.org/jira/browse/MAPREDUCE-2053) | [Herriot] Test Gridmix file pool for different input file sizes based on pool minimum size. |  Major | contrib/gridmix | Vinay Kumar Thota | Vinay Kumar Thota |
| [MAPREDUCE-2622](https://issues.apache.org/jira/browse/MAPREDUCE-2622) | Remove the last remaining reference to "io.sort.mb" |  Minor | test | Harsh J | Harsh J |
| [HDFS-2196](https://issues.apache.org/jira/browse/HDFS-2196) | Make ant build system work with hadoop-common JAR generated by Maven |  Major | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [MAPREDUCE-2741](https://issues.apache.org/jira/browse/MAPREDUCE-2741) | Make ant build system work with hadoop-common JAR generated by Maven |  Major | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-7512](https://issues.apache.org/jira/browse/HADOOP-7512) | Fix example mistake in WritableComparable javadocs |  Trivial | documentation | Harsh J | Harsh J |
| [HADOOP-6158](https://issues.apache.org/jira/browse/HADOOP-6158) | Move CyclicIteration to HDFS |  Minor | util | Owen O'Malley | Eli Collins |
| [HDFS-2096](https://issues.apache.org/jira/browse/HDFS-2096) | Mavenization of hadoop-hdfs |  Major | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-7579](https://issues.apache.org/jira/browse/HADOOP-7579) | Rename package names from alfredo to auth |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [MAPREDUCE-2986](https://issues.apache.org/jira/browse/MAPREDUCE-2986) | Multiple node managers support for the MiniYARNCluster |  Critical | mrv2, test | Anupam Seth | Anupam Seth |
| [HADOOP-7762](https://issues.apache.org/jira/browse/HADOOP-7762) | Common side of MR-2736 (MR1 removal) |  Major | scripts | Eli Collins | Eli Collins |


