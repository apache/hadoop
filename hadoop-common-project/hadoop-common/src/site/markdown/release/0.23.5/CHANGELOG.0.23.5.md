
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

## Release 0.23.5 - 2012-11-29

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-4080](https://issues.apache.org/jira/browse/HDFS-4080) | Add a separate logger for block state change logs to enable turning off those logs |  Major | namenode | Kihwal Lee | Kihwal Lee |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-8789](https://issues.apache.org/jira/browse/HADOOP-8789) | Tests setLevel(Level.OFF) should be Level.ERROR |  Minor | test | Andy Isaacson | Andy Isaacson |
| [HADOOP-8755](https://issues.apache.org/jira/browse/HADOOP-8755) | Print thread dump when tests fail due to timeout |  Major | test | Andrey Klochkov | Andrey Klochkov |
| [HADOOP-8851](https://issues.apache.org/jira/browse/HADOOP-8851) | Use -XX:+HeapDumpOnOutOfMemoryError JVM option in the forked tests |  Minor | test | Ivan A. Veselovsky | Ivan A. Veselovsky |
| [HDFS-3483](https://issues.apache.org/jira/browse/HDFS-3483) | Better error message when hdfs fsck is run against a ViewFS config |  Major | . | Stephen Chu | Stephen Fritz |
| [HADOOP-8889](https://issues.apache.org/jira/browse/HADOOP-8889) | Upgrade to Surefire 2.12.3 |  Major | build, test | Todd Lipcon | Todd Lipcon |
| [HADOOP-8932](https://issues.apache.org/jira/browse/HADOOP-8932) | JNI-based user-group mapping modules can be too chatty on lookup failures |  Major | security | Kihwal Lee | Kihwal Lee |
| [HADOOP-8926](https://issues.apache.org/jira/browse/HADOOP-8926) | hadoop.util.PureJavaCrc32 cache hit-ratio is low for static data |  Major | util | Gopal V | Gopal V |
| [HADOOP-8930](https://issues.apache.org/jira/browse/HADOOP-8930) | Cumulative code coverage calculation |  Major | test | Andrey Klochkov | Andrey Klochkov |
| [MAPREDUCE-4229](https://issues.apache.org/jira/browse/MAPREDUCE-4229) | Counter names' memory usage can be decreased by interning |  Major | jobtracker | Todd Lipcon | Miomir Boljanovic |
| [MAPREDUCE-4752](https://issues.apache.org/jira/browse/MAPREDUCE-4752) | Reduce MR AM memory usage through String Interning |  Major | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [YARN-165](https://issues.apache.org/jira/browse/YARN-165) | RM should point tracking URL to RM web page for app when AM fails |  Blocker | resourcemanager | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4763](https://issues.apache.org/jira/browse/MAPREDUCE-4763) | repair test org.apache.hadoop.mapreduce.security.TestUmbilicalProtocolWithJobToken |  Minor | . | Ivan A. Veselovsky |  |
| [MAPREDUCE-4666](https://issues.apache.org/jira/browse/MAPREDUCE-4666) | JVM metrics for history server |  Minor | jobhistoryserver | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4517](https://issues.apache.org/jira/browse/MAPREDUCE-4517) | Too many INFO messages written out during AM to RM heartbeat |  Minor | applicationmaster | James Kinley | Jason Lowe |
| [YARN-216](https://issues.apache.org/jira/browse/YARN-216) | Remove jquery theming support |  Major | . | Todd Lipcon | Robert Joseph Evans |
| [MAPREDUCE-4802](https://issues.apache.org/jira/browse/MAPREDUCE-4802) | Takes a long time to load the task list on the AM for large jobs |  Major | mr-am, mrv2, webapps | Ravi Prakash | Ravi Prakash |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-8775](https://issues.apache.org/jira/browse/HADOOP-8775) | MR2 distcp permits non-positive value to -bandwidth option which causes job never to complete |  Major | . | Sandy Ryza | Sandy Ryza |
| [HDFS-3919](https://issues.apache.org/jira/browse/HDFS-3919) | MiniDFSCluster:waitClusterUp can hang forever |  Minor | test | Andy Isaacson | Andy Isaacson |
| [HADOOP-8819](https://issues.apache.org/jira/browse/HADOOP-8819) | Should use && instead of  & in a few places in FTPFileSystem,FTPInputStream,S3InputStream,ViewFileSystem,ViewFs |  Major | fs | Brandon Li | Brandon Li |
| [YARN-28](https://issues.apache.org/jira/browse/YARN-28) | TestCompositeService fails on jdk7 |  Major | . | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4674](https://issues.apache.org/jira/browse/MAPREDUCE-4674) | Hadoop examples secondarysort has a typo "secondarysrot" in the usage |  Minor | . | Robert Justice | Robert Justice |
| [HADOOP-8791](https://issues.apache.org/jira/browse/HADOOP-8791) | rm "Only deletes non empty directory and files." |  Major | documentation | Bertrand Dechoux | Jing Zhao |
| [HADOOP-8386](https://issues.apache.org/jira/browse/HADOOP-8386) | hadoop script doesn't work if 'cd' prints to stdout (default behavior in Ubuntu) |  Major | scripts | Christopher Berner | Christopher Berner |
| [YARN-116](https://issues.apache.org/jira/browse/YARN-116) | RM is missing ability to add include/exclude files without a restart |  Major | resourcemanager | xieguiming | xieguiming |
| [YARN-131](https://issues.apache.org/jira/browse/YARN-131) | Incorrect ACL properties in capacity scheduler documentation |  Major | capacityscheduler | Ahmed Radwan | Ahmed Radwan |
| [HDFS-3905](https://issues.apache.org/jira/browse/HDFS-3905) | Secure cluster cannot use hftp to an insecure cluster |  Critical | hdfs-client, security | Daryn Sharp | Daryn Sharp |
| [YARN-102](https://issues.apache.org/jira/browse/YARN-102) | Move the apache licence header to the top of the file in MemStore.java |  Trivial | resourcemanager | Devaraj K | Devaraj K |
| [HDFS-3996](https://issues.apache.org/jira/browse/HDFS-3996) | Add debug log removed in HDFS-3873 back |  Minor | . | Eli Collins | Eli Collins |
| [HDFS-3829](https://issues.apache.org/jira/browse/HDFS-3829) | TestHftpURLTimeouts fails intermittently with JDK7 |  Major | test | Trevor Robinson | Trevor Robinson |
| [MAPREDUCE-4554](https://issues.apache.org/jira/browse/MAPREDUCE-4554) | Job Credentials are not transmitted if security is turned off |  Major | job submission, security | Benoy Antony | Benoy Antony |
| [MAPREDUCE-4705](https://issues.apache.org/jira/browse/MAPREDUCE-4705) | Historyserver links expire before the history data does |  Critical | jobhistoryserver, mrv2 | Jason Lowe | Jason Lowe |
| [YARN-30](https://issues.apache.org/jira/browse/YARN-30) | TestNMWebServicesApps, TestRMWebServicesApps and TestRMWebServicesNodes fail on jdk7 |  Major | . | Thomas Graves | Thomas Graves |
| [HDFS-3824](https://issues.apache.org/jira/browse/HDFS-3824) | TestHftpDelegationToken fails intermittently with JDK7 |  Major | test | Trevor Robinson | Trevor Robinson |
| [HDFS-3224](https://issues.apache.org/jira/browse/HDFS-3224) | Bug in check for DN re-registration with different storage ID |  Minor | . | Eli Collins | Jason Lowe |
| [HADOOP-8906](https://issues.apache.org/jira/browse/HADOOP-8906) | paths with multiple globs are unreliable |  Critical | fs | Daryn Sharp | Daryn Sharp |
| [HDFS-4016](https://issues.apache.org/jira/browse/HDFS-4016) | back-port HDFS-3582 to branch-0.23 |  Minor | . | Ivan A. Veselovsky | Ivan A. Veselovsky |
| [MAPREDUCE-4521](https://issues.apache.org/jira/browse/MAPREDUCE-4521) | mapreduce.user.classpath.first incompatibility with 0.20/1.x |  Major | mrv2 | Jason Lowe | Ravi Prakash |
| [YARN-161](https://issues.apache.org/jira/browse/YARN-161) | Yarn Common has multiple compiler warnings for unchecked operations |  Major | api | Chris Nauroth | Chris Nauroth |
| [YARN-43](https://issues.apache.org/jira/browse/YARN-43) | TestResourceTrackerService fail intermittently on jdk7 |  Major | . | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4721](https://issues.apache.org/jira/browse/MAPREDUCE-4721) | Task startup time in JHS is same as job startup time. |  Major | jobhistoryserver | Ravi Prakash | Ravi Prakash |
| [YARN-32](https://issues.apache.org/jira/browse/YARN-32) | TestApplicationTokens fails intermintently on jdk7 |  Major | . | Thomas Graves | Vinod Kumar Vavilapalli |
| [YARN-163](https://issues.apache.org/jira/browse/YARN-163) | Retrieving container log via NM webapp can hang with multibyte characters in log |  Major | nodemanager | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4479](https://issues.apache.org/jira/browse/MAPREDUCE-4479) | Fix parameter order in assertEquals() in TestCombineInputFileFormat.java |  Major | test | Mariappan Asokan | Mariappan Asokan |
| [MAPREDUCE-4733](https://issues.apache.org/jira/browse/MAPREDUCE-4733) | Reducer can fail to make progress during shuffle if too many reducers complete consecutively |  Major | applicationmaster, mrv2 | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4740](https://issues.apache.org/jira/browse/MAPREDUCE-4740) | only .jars can be added to the Distributed Cache classpath |  Blocker | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [YARN-174](https://issues.apache.org/jira/browse/YARN-174) | TestNodeStatusUpdater is failing in trunk |  Major | nodemanager | Robert Joseph Evans | Vinod Kumar Vavilapalli |
| [HDFS-4090](https://issues.apache.org/jira/browse/HDFS-4090) | getFileChecksum() result incompatible when called against zero-byte files. |  Critical | hdfs-client | Kihwal Lee | Kihwal Lee |
| [HADOOP-8962](https://issues.apache.org/jira/browse/HADOOP-8962) | RawLocalFileSystem.listStatus fails when a child filename contains a colon |  Critical | fs | Jason Lowe | Jason Lowe |
| [YARN-177](https://issues.apache.org/jira/browse/YARN-177) | CapacityScheduler - adding a queue while the RM is running has wacky results |  Critical | capacityscheduler | Thomas Graves | Arun C Murthy |
| [YARN-178](https://issues.apache.org/jira/browse/YARN-178) | Fix custom ProcessTree instance creation |  Critical | . | Radim Kolar | Radim Kolar |
| [YARN-180](https://issues.apache.org/jira/browse/YARN-180) | Capacity scheduler - containers that get reserved create container token to early |  Critical | capacityscheduler | Thomas Graves | Arun C Murthy |
| [YARN-139](https://issues.apache.org/jira/browse/YARN-139) | Interrupted Exception within AsyncDispatcher leads to user confusion |  Major | api | Nathan Roberts | Vinod Kumar Vavilapalli |
| [MAPREDUCE-4741](https://issues.apache.org/jira/browse/MAPREDUCE-4741) | WARN and ERROR messages logged during normal AM shutdown |  Minor | applicationmaster, mrv2 | Jason Lowe | Vinod Kumar Vavilapalli |
| [MAPREDUCE-4730](https://issues.apache.org/jira/browse/MAPREDUCE-4730) | AM crashes due to OOM while serving up map task completion events |  Blocker | applicationmaster, mrv2 | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4748](https://issues.apache.org/jira/browse/MAPREDUCE-4748) | Invalid event: T\_ATTEMPT\_SUCCEEDED at SUCCEEDED |  Blocker | mrv2 | Robert Joseph Evans | Jason Lowe |
| [MAPREDUCE-1806](https://issues.apache.org/jira/browse/MAPREDUCE-1806) | CombineFileInputFormat does not work with paths not on default FS |  Major | harchive | Paul Yang | Gera Shegalov |
| [HADOOP-8986](https://issues.apache.org/jira/browse/HADOOP-8986) | Server$Call object is never released after it is sent |  Critical | ipc | Robert Joseph Evans | Robert Joseph Evans |
| [YARN-159](https://issues.apache.org/jira/browse/YARN-159) | RM web ui applications page should be sorted to display last app first |  Major | resourcemanager | Thomas Graves | Thomas Graves |
| [YARN-166](https://issues.apache.org/jira/browse/YARN-166) | capacity scheduler doesn't allow capacity \< 1.0 |  Major | capacityscheduler | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4724](https://issues.apache.org/jira/browse/MAPREDUCE-4724) | job history web ui applications page should be sorted to display last app first |  Major | jobhistoryserver | Thomas Graves | Thomas Graves |
| [YARN-189](https://issues.apache.org/jira/browse/YARN-189) | deadlock in RM - AMResponse object |  Blocker | resourcemanager | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4746](https://issues.apache.org/jira/browse/MAPREDUCE-4746) | The MR Application Master does not have a config to set environment variables |  Major | applicationmaster | Robert Parker | Robert Parker |
| [MAPREDUCE-4729](https://issues.apache.org/jira/browse/MAPREDUCE-4729) | job history UI not showing all job attempts |  Major | jobhistoryserver | Thomas Graves | Vinod Kumar Vavilapalli |
| [MAPREDUCE-4771](https://issues.apache.org/jira/browse/MAPREDUCE-4771) | KeyFieldBasedPartitioner not partitioning properly when configured |  Major | mrv2 | Jason Lowe | Jason Lowe |
| [HDFS-4075](https://issues.apache.org/jira/browse/HDFS-4075) | Reduce recommissioning overhead |  Critical | namenode | Kihwal Lee | Kihwal Lee |
| [YARN-201](https://issues.apache.org/jira/browse/YARN-201) | CapacityScheduler can take a very long time to schedule containers if requests are off cluster |  Critical | capacityscheduler | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4772](https://issues.apache.org/jira/browse/MAPREDUCE-4772) | Fetch failures can take way too long for a map to be restarted |  Critical | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [HDFS-3990](https://issues.apache.org/jira/browse/HDFS-3990) | NN's health report has severe performance problems |  Critical | namenode | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-4782](https://issues.apache.org/jira/browse/MAPREDUCE-4782) | NLineInputFormat skips first line of last InputSplit |  Blocker | client | Mark Fuhs | Mark Fuhs |
| [HDFS-4162](https://issues.apache.org/jira/browse/HDFS-4162) | Some malformed and unquoted HTML strings are returned from datanode web ui |  Minor | datanode | Derek Dagit | Derek Dagit |
| [MAPREDUCE-4774](https://issues.apache.org/jira/browse/MAPREDUCE-4774) | JobImpl does not handle asynchronous task events in FAILED state |  Major | applicationmaster, mrv2 | Ivan A. Veselovsky | Jason Lowe |
| [YARN-206](https://issues.apache.org/jira/browse/YARN-206) | TestApplicationCleanup.testContainerCleanup occasionally fails |  Major | resourcemanager | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4751](https://issues.apache.org/jira/browse/MAPREDUCE-4751) | AM stuck in KILL\_WAIT for days |  Major | . | Ravi Prakash | Vinod Kumar Vavilapalli |
| [MAPREDUCE-4787](https://issues.apache.org/jira/browse/MAPREDUCE-4787) | TestJobMonitorAndPrint is broken |  Major | test | Ravi Prakash | Robert Parker |
| [HDFS-4172](https://issues.apache.org/jira/browse/HDFS-4172) | namenode does not URI-encode parameters when building URI for datanode request |  Minor | namenode | Derek Dagit | Derek Dagit |
| [MAPREDUCE-4425](https://issues.apache.org/jira/browse/MAPREDUCE-4425) | Speculation + Fetch failures can lead to a hung job |  Critical | mrv2 | Siddharth Seth | Jason Lowe |
| [MAPREDUCE-4786](https://issues.apache.org/jira/browse/MAPREDUCE-4786) | Job End Notification retry interval is 5 milliseconds by default |  Major | mrv2 | Ravi Prakash | Ravi Prakash |
| [HADOOP-9022](https://issues.apache.org/jira/browse/HADOOP-9022) | Hadoop distcp tool fails to copy file if -m 0 specified |  Major | . | Haiyang Jiang | Jonathan Eagles |
| [HADOOP-9025](https://issues.apache.org/jira/browse/HADOOP-9025) | org.apache.hadoop.tools.TestCopyListing failing |  Major | . | Robert Joseph Evans | Jonathan Eagles |
| [HDFS-4181](https://issues.apache.org/jira/browse/HDFS-4181) | LeaseManager tries to double remove and prints extra messages |  Critical | namenode | Kihwal Lee | Kihwal Lee |
| [YARN-212](https://issues.apache.org/jira/browse/YARN-212) | NM state machine ignores an APPLICATION\_CONTAINER\_FINISHED event when it shouldn't |  Blocker | nodemanager | Nathan Roberts | Nathan Roberts |
| [YARN-144](https://issues.apache.org/jira/browse/YARN-144) | MiniMRYarnCluster launches RM and JHS on default ports |  Major | . | Robert Parker | Robert Parker |
| [HDFS-4182](https://issues.apache.org/jira/browse/HDFS-4182) | SecondaryNameNode leaks NameCache entries |  Critical | namenode | Todd Lipcon | Robert Joseph Evans |
| [MAPREDUCE-4797](https://issues.apache.org/jira/browse/MAPREDUCE-4797) | LocalContainerAllocator can loop forever trying to contact the RM |  Major | applicationmaster | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4720](https://issues.apache.org/jira/browse/MAPREDUCE-4720) | Browser thinks History Server main page JS is taking too long |  Major | . | Robert Joseph Evans | Ravi Prakash |
| [HDFS-4186](https://issues.apache.org/jira/browse/HDFS-4186) | logSync() is called with the write lock held while releasing lease |  Critical | namenode | Kihwal Lee | Kihwal Lee |
| [MAPREDUCE-4801](https://issues.apache.org/jira/browse/MAPREDUCE-4801) | ShuffleHandler can generate large logs due to prematurely closed channels |  Critical | . | Jason Lowe | Jason Lowe |
| [YARN-214](https://issues.apache.org/jira/browse/YARN-214) | RMContainerImpl does not handle event EXPIRE at state RUNNING |  Major | resourcemanager | Jason Lowe | Jonathan Eagles |
| [YARN-151](https://issues.apache.org/jira/browse/YARN-151) | Browser thinks RM main page JS is taking too long |  Major | . | Robert Joseph Evans | Ravi Prakash |
| [MAPREDUCE-4549](https://issues.apache.org/jira/browse/MAPREDUCE-4549) | Distributed cache conflicts breaks backwards compatability |  Blocker | mrv2 | Robert Joseph Evans | Robert Joseph Evans |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-4107](https://issues.apache.org/jira/browse/MAPREDUCE-4107) | Fix tests in org.apache.hadoop.ipc.TestSocketFactory |  Major | mrv2 | Devaraj K | Devaraj K |
| [YARN-202](https://issues.apache.org/jira/browse/YARN-202) | Log Aggregation generates a storm of fsync() for namenode |  Critical | . | Kihwal Lee | Kihwal Lee |
| [YARN-186](https://issues.apache.org/jira/browse/YARN-186) | Coverage fixing LinuxContainerExecutor |  Major | resourcemanager, scheduler | Aleksey Gorshkov | Aleksey Gorshkov |
| [YARN-188](https://issues.apache.org/jira/browse/YARN-188) | Coverage fixing for CapacityScheduler |  Major | capacityscheduler | Aleksey Gorshkov | Aleksey Gorshkov |
| [YARN-219](https://issues.apache.org/jira/browse/YARN-219) | NM should aggregate logs when application finishes. |  Critical | nodemanager | Robert Joseph Evans | Robert Joseph Evans |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-4596](https://issues.apache.org/jira/browse/MAPREDUCE-4596) | Split StateMachine state from states seen by MRClientProtocol (for Job, Task, TaskAttempt) |  Major | applicationmaster, mrv2 | Siddharth Seth | Siddharth Seth |
| [YARN-154](https://issues.apache.org/jira/browse/YARN-154) | Create Yarn trunk and commit jobs |  Major | . | Eli Collins | Robert Joseph Evans |
| [MAPREDUCE-4266](https://issues.apache.org/jira/browse/MAPREDUCE-4266) | remove Ant remnants from MR |  Major | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-8860](https://issues.apache.org/jira/browse/HADOOP-8860) | Split MapReduce and YARN sections in documentation navigation |  Major | documentation | Tom White | Tom White |


