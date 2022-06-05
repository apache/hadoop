
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

## Release 2.0.3-alpha - 2013-02-14

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-9070](https://issues.apache.org/jira/browse/HADOOP-9070) | Kerberos SASL server cannot find kerberos key |  Blocker | ipc | Daryn Sharp | Daryn Sharp |
| [HADOOP-8999](https://issues.apache.org/jira/browse/HADOOP-8999) | SASL negotiation is flawed |  Major | ipc | Daryn Sharp | Daryn Sharp |
| [HDFS-4451](https://issues.apache.org/jira/browse/HDFS-4451) | hdfs balancer command returns exit code 1 on success instead of 0 |  Major | balancer & mover | Joshua Blatt |  |
| [HDFS-4369](https://issues.apache.org/jira/browse/HDFS-4369) | GetBlockKeysResponseProto does not handle null response |  Blocker | namenode | Suresh Srinivas | Suresh Srinivas |
| [HDFS-4367](https://issues.apache.org/jira/browse/HDFS-4367) | GetDataEncryptionKeyResponseProto  does not handle null response |  Blocker | namenode | Suresh Srinivas | Suresh Srinivas |
| [HDFS-4364](https://issues.apache.org/jira/browse/HDFS-4364) | GetLinkTargetResponseProto does not handle null path |  Blocker | . | Suresh Srinivas | Suresh Srinivas |
| [HDFS-4362](https://issues.apache.org/jira/browse/HDFS-4362) | GetDelegationTokenResponseProto does not handle null token |  Critical | . | Suresh Srinivas | Suresh Srinivas |
| [HDFS-4350](https://issues.apache.org/jira/browse/HDFS-4350) | Make enabling of stale marking on read and write paths independent |  Major | . | Andrew Wang | Andrew Wang |
| [HDFS-4122](https://issues.apache.org/jira/browse/HDFS-4122) | Cleanup HDFS logs and reduce the size of logged messages |  Major | datanode, hdfs-client, namenode | Suresh Srinivas | Suresh Srinivas |
| [HDFS-4080](https://issues.apache.org/jira/browse/HDFS-4080) | Add a separate logger for block state change logs to enable turning off those logs |  Major | namenode | Kihwal Lee | Kihwal Lee |
| [HDFS-1331](https://issues.apache.org/jira/browse/HDFS-1331) | dfs -test should work like /bin/test |  Minor | tools | Allen Wittenauer | Andy Isaacson |
| [MAPREDUCE-4928](https://issues.apache.org/jira/browse/MAPREDUCE-4928) | Use token request messages defined in hadoop common |  Major | applicationmaster, security | Suresh Srinivas | Suresh Srinivas |
| [MAPREDUCE-4123](https://issues.apache.org/jira/browse/MAPREDUCE-4123) | ./mapred groups gives NoClassDefFoundError |  Critical | mrv2 | Nishan Shetty | Devaraj K |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-9090](https://issues.apache.org/jira/browse/HADOOP-9090) | Support on-demand publish of metrics |  Minor | metrics | Mostafa Elhemali | Mostafa Elhemali |
| [HADOOP-9054](https://issues.apache.org/jira/browse/HADOOP-9054) | Add AuthenticationHandler that uses Kerberos but allows for an alternate form of authentication for browsers |  Major | security | Robert Kanter | Robert Kanter |
| [HADOOP-8597](https://issues.apache.org/jira/browse/HADOOP-8597) | FsShell's Text command should be able to read avro data files |  Major | fs | Harsh J | Ivan Vladimirov Ivanov |
| [HDFS-4456](https://issues.apache.org/jira/browse/HDFS-4456) | Add concat to HttpFS and WebHDFS REST API docs |  Major | webhdfs | Tsz Wo Nicholas Sze | Plamen Jeliazkov |
| [HDFS-4213](https://issues.apache.org/jira/browse/HDFS-4213) | When the client calls hsync, allows the client to update the file length in the NameNode |  Major | hdfs-client, namenode | Jing Zhao | Jing Zhao |
| [HDFS-3598](https://issues.apache.org/jira/browse/HDFS-3598) | WebHDFS: support file concat |  Major | webhdfs | Tsz Wo Nicholas Sze | Plamen Jeliazkov |
| [HDFS-3077](https://issues.apache.org/jira/browse/HDFS-3077) | Quorum-based protocol for reading and writing edit logs |  Major | ha, namenode | Todd Lipcon | Todd Lipcon |
| [HDFS-3049](https://issues.apache.org/jira/browse/HDFS-3049) | During the normal loading NN startup process, fall back on a different EditLog if we see one that is corrupt |  Minor | namenode | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-1245](https://issues.apache.org/jira/browse/HDFS-1245) | Plugable block id generation |  Major | namenode | Dmytro Molkov | Konstantin Shvachko |
| [MAPREDUCE-4808](https://issues.apache.org/jira/browse/MAPREDUCE-4808) | Refactor MapOutput and MergeManager to facilitate reuse by Shuffle implementations |  Major | . | Arun C Murthy | Mariappan Asokan |
| [MAPREDUCE-4520](https://issues.apache.org/jira/browse/MAPREDUCE-4520) | Add experimental support for MR AM to schedule CPUs along-with memory |  Major | . | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-3678](https://issues.apache.org/jira/browse/MAPREDUCE-3678) | The Map tasks logs should have the value of input split it processed |  Major | mrv1, mrv2 | Bejoy KS | Harsh J |
| [MAPREDUCE-2454](https://issues.apache.org/jira/browse/MAPREDUCE-2454) | Allow external sorter plugin for MR |  Minor | . | Mariappan Asokan | Mariappan Asokan |
| [YARN-286](https://issues.apache.org/jira/browse/YARN-286) | Add a YARN ApplicationClassLoader |  Major | applications | Tom White | Tom White |
| [YARN-187](https://issues.apache.org/jira/browse/YARN-187) | Add hierarchical queues to the fair scheduler |  Major | scheduler | Sandy Ryza | Sandy Ryza |
| [YARN-146](https://issues.apache.org/jira/browse/YARN-146) | Add unit tests for computing fair share in the fair scheduler |  Major | resourcemanager | Sandy Ryza | Sandy Ryza |
| [YARN-145](https://issues.apache.org/jira/browse/YARN-145) | Add a Web UI to the fair share scheduler |  Major | resourcemanager | Sandy Ryza | Sandy Ryza |
| [YARN-2](https://issues.apache.org/jira/browse/YARN-2) | Enhance CS to schedule accounting for both memory and cpu cores |  Major | capacityscheduler, scheduler | Arun C Murthy | Arun C Murthy |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-9276](https://issues.apache.org/jira/browse/HADOOP-9276) | Allow BoundedByteArrayOutputStream to be resettable |  Minor | . | Arun C Murthy | Arun C Murthy |
| [HADOOP-9247](https://issues.apache.org/jira/browse/HADOOP-9247) | parametrize Clover "generateXxx" properties to make them re-definable via -D in mvn calls |  Minor | . | Ivan A. Veselovsky | Ivan A. Veselovsky |
| [HADOOP-9216](https://issues.apache.org/jira/browse/HADOOP-9216) | CompressionCodecFactory#getCodecClasses should trim the result of parsing by Configuration. |  Major | io | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [HADOOP-9192](https://issues.apache.org/jira/browse/HADOOP-9192) | Move token related request/response messages to common |  Major | security | Suresh Srinivas | Suresh Srinivas |
| [HADOOP-9162](https://issues.apache.org/jira/browse/HADOOP-9162) | Add utility to check native library availability |  Minor | native | Binglin Chang | Binglin Chang |
| [HADOOP-9153](https://issues.apache.org/jira/browse/HADOOP-9153) | Support createNonRecursive in ViewFileSystem |  Major | viewfs | Sandy Ryza | Sandy Ryza |
| [HADOOP-9147](https://issues.apache.org/jira/browse/HADOOP-9147) | Add missing fields to FIleStatus.toString |  Trivial | . | Jonathan Allen | Jonathan Allen |
| [HADOOP-9127](https://issues.apache.org/jira/browse/HADOOP-9127) | Update documentation for ZooKeeper Failover Controller |  Major | documentation | Daisuke Kobayashi | Daisuke Kobayashi |
| [HADOOP-9118](https://issues.apache.org/jira/browse/HADOOP-9118) | FileSystemContractBaseTest test data for read/write isn't rigorous enough |  Trivial | test | Steve Loughran |  |
| [HADOOP-9106](https://issues.apache.org/jira/browse/HADOOP-9106) | Allow configuration of IPC connect timeout |  Major | ipc | Todd Lipcon | Robert Parker |
| [HADOOP-9093](https://issues.apache.org/jira/browse/HADOOP-9093) | Move all the Exception in PathExceptions to o.a.h.fs package |  Major | . | Suresh Srinivas | Suresh Srinivas |
| [HADOOP-9004](https://issues.apache.org/jira/browse/HADOOP-9004) | Allow security unit tests to use external KDC |  Major | security, test | Stephen Chu | Stephen Chu |
| [HADOOP-8998](https://issues.apache.org/jira/browse/HADOOP-8998) | set Cache-Control no-cache header on all dynamic content |  Minor | . | Andy Isaacson | Alejandro Abdelnur |
| [HADOOP-8992](https://issues.apache.org/jira/browse/HADOOP-8992) | Enhance unit-test coverage of class HarFileSystem |  Minor | . | Ivan A. Veselovsky | Ivan A. Veselovsky |
| [HADOOP-8985](https://issues.apache.org/jira/browse/HADOOP-8985) | Add namespace declarations in .proto files for languages other than java |  Minor | ha, ipc | Binglin Chang | Binglin Chang |
| [HADOOP-8951](https://issues.apache.org/jira/browse/HADOOP-8951) | RunJar to fail with user-comprehensible error message if jar missing |  Minor | util | Steve Loughran | Steve Loughran |
| [HADOOP-8932](https://issues.apache.org/jira/browse/HADOOP-8932) | JNI-based user-group mapping modules can be too chatty on lookup failures |  Major | security | Kihwal Lee | Kihwal Lee |
| [HADOOP-8931](https://issues.apache.org/jira/browse/HADOOP-8931) | Add Java version to startup message |  Trivial | . | Eli Collins | Eli Collins |
| [HADOOP-8930](https://issues.apache.org/jira/browse/HADOOP-8930) | Cumulative code coverage calculation |  Major | test | Andrey Klochkov | Andrey Klochkov |
| [HADOOP-8929](https://issues.apache.org/jira/browse/HADOOP-8929) | Add toString, other improvements for SampleQuantiles |  Major | metrics | Todd Lipcon | Todd Lipcon |
| [HADOOP-8926](https://issues.apache.org/jira/browse/HADOOP-8926) | hadoop.util.PureJavaCrc32 cache hit-ratio is low for static data |  Major | util | Gopal V | Gopal V |
| [HADOOP-8925](https://issues.apache.org/jira/browse/HADOOP-8925) | Remove the packaging |  Minor | build | Eli Collins | Eli Collins |
| [HADOOP-8922](https://issues.apache.org/jira/browse/HADOOP-8922) | Provide alternate JSONP output for JMXJsonServlet to allow javascript in browser dashboard |  Trivial | metrics | Damien Hardy | Damien Hardy |
| [HADOOP-8909](https://issues.apache.org/jira/browse/HADOOP-8909) | Hadoop Common Maven protoc calls must not depend on external sh script |  Major | build | Chris Nauroth | Chris Nauroth |
| [HADOOP-8894](https://issues.apache.org/jira/browse/HADOOP-8894) | GenericTestUtils.waitFor should dump thread stacks on timeout |  Major | . | Todd Lipcon | Todd Lipcon |
| [HADOOP-8889](https://issues.apache.org/jira/browse/HADOOP-8889) | Upgrade to Surefire 2.12.3 |  Major | build, test | Todd Lipcon | Todd Lipcon |
| [HADOOP-8866](https://issues.apache.org/jira/browse/HADOOP-8866) | SampleQuantiles#query is O(N^2) instead of O(N) |  Minor | . | Andrew Wang | Andrew Wang |
| [HADOOP-8851](https://issues.apache.org/jira/browse/HADOOP-8851) | Use -XX:+HeapDumpOnOutOfMemoryError JVM option in the forked tests |  Minor | test | Ivan A. Veselovsky | Ivan A. Veselovsky |
| [HADOOP-8849](https://issues.apache.org/jira/browse/HADOOP-8849) | FileUtil#fullyDelete should grant the target directories +rwx permissions before trying to delete them |  Minor | . | Ivan A. Veselovsky | Ivan A. Veselovsky |
| [HADOOP-8812](https://issues.apache.org/jira/browse/HADOOP-8812) | ExitUtil#terminate should print Exception#toString |  Minor | . | Eli Collins | Eli Collins |
| [HADOOP-8806](https://issues.apache.org/jira/browse/HADOOP-8806) | libhadoop.so: dlopen should be better at locating libsnappy.so, etc. |  Minor | build | Colin Patrick McCabe | Colin Patrick McCabe |
| [HADOOP-8804](https://issues.apache.org/jira/browse/HADOOP-8804) | Improve Web UIs when the wildcard address is used |  Minor | . | Eli Collins | Senthil V Kumar |
| [HADOOP-8789](https://issues.apache.org/jira/browse/HADOOP-8789) | Tests setLevel(Level.OFF) should be Level.ERROR |  Minor | test | Andy Isaacson | Andy Isaacson |
| [HADOOP-8755](https://issues.apache.org/jira/browse/HADOOP-8755) | Print thread dump when tests fail due to timeout |  Major | test | Andrey Klochkov | Andrey Klochkov |
| [HADOOP-8736](https://issues.apache.org/jira/browse/HADOOP-8736) | Add Builder for building an RPC server |  Major | ipc | Brandon Li | Brandon Li |
| [HADOOP-8712](https://issues.apache.org/jira/browse/HADOOP-8712) | Change default hadoop.security.group.mapping |  Minor | security | Robert Parker | Robert Parker |
| [HADOOP-8561](https://issues.apache.org/jira/browse/HADOOP-8561) | Introduce HADOOP\_PROXY\_USER for secure impersonation in child hadoop client processes |  Major | security | Luke Lu | Yu Gao |
| [HADOOP-7886](https://issues.apache.org/jira/browse/HADOOP-7886) | Add toString to FileStatus |  Minor | . | Jakob Homan | SreeHari |
| [HADOOP-7688](https://issues.apache.org/jira/browse/HADOOP-7688) | When a servlet filter throws an exception in init(..), the Jetty server failed silently. |  Major | . | Tsz Wo Nicholas Sze | Uma Maheswara Rao G |
| [HDFS-4393](https://issues.apache.org/jira/browse/HDFS-4393) | Empty request and responses in protocol translators can be static final members |  Minor | . | Brandon Li | Brandon Li |
| [HDFS-4392](https://issues.apache.org/jira/browse/HDFS-4392) | Use NetUtils#getFreeSocketPort in MiniDFSCluster |  Trivial | test | Andrew Purtell | Andrew Purtell |
| [HDFS-4381](https://issues.apache.org/jira/browse/HDFS-4381) |  Document fsimage format details in FSImageFormat class javadoc |  Major | namenode | Jing Zhao | Jing Zhao |
| [HDFS-4375](https://issues.apache.org/jira/browse/HDFS-4375) | Use token request messages defined in hadoop common |  Major | namenode, security | Suresh Srinivas | Suresh Srinivas |
| [HDFS-4259](https://issues.apache.org/jira/browse/HDFS-4259) | Improve pipeline DN replacement failure message |  Minor | hdfs-client | Harsh J | Harsh J |
| [HDFS-4231](https://issues.apache.org/jira/browse/HDFS-4231) | Introduce HAState for BackupNode |  Major | ha, namenode | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-4214](https://issues.apache.org/jira/browse/HDFS-4214) | OfflineEditsViewer should print out the offset at which it encountered an error |  Trivial | tools | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-4155](https://issues.apache.org/jira/browse/HDFS-4155) | libhdfs implementation of hsync API |  Major | libhdfs | Liang Xie | Liang Xie |
| [HDFS-4153](https://issues.apache.org/jira/browse/HDFS-4153) | Add START\_MSG/SHUTDOWN\_MSG for JournalNode |  Major | journal-node | Liang Xie | Liang Xie |
| [HDFS-4143](https://issues.apache.org/jira/browse/HDFS-4143) | Change INodeFile.blocks to private |  Minor | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-4121](https://issues.apache.org/jira/browse/HDFS-4121) | Add namespace declarations in hdfs .proto files for languages other than java |  Minor | . | Binglin Chang | Binglin Chang |
| [HDFS-4110](https://issues.apache.org/jira/browse/HDFS-4110) | Refine JNStorage log |  Trivial | journal-node | Liang Xie | Liang Xie |
| [HDFS-4088](https://issues.apache.org/jira/browse/HDFS-4088) | Remove "throws QuotaExceededException" from an INodeDirectoryWithQuota constructor |  Minor | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-4074](https://issues.apache.org/jira/browse/HDFS-4074) | Remove empty constructors for INode |  Trivial | namenode | Brandon Li | Brandon Li |
| [HDFS-4073](https://issues.apache.org/jira/browse/HDFS-4073) | Two minor improvements to FSDirectory |  Minor | namenode | Tsz Wo Nicholas Sze | Jing Zhao |
| [HDFS-4068](https://issues.apache.org/jira/browse/HDFS-4068) | DatanodeID and DatanodeInfo member should be private |  Minor | datanode | Eli Collins | Eli Collins |
| [HDFS-4058](https://issues.apache.org/jira/browse/HDFS-4058) | DirectoryScanner may fail with IOOB if the directory scanning threads return out of volume order |  Major | datanode | Eli Collins | Eli Collins |
| [HDFS-4048](https://issues.apache.org/jira/browse/HDFS-4048) | Use ERROR instead of INFO for volume failure logs |  Major | . | Stephen Chu | Stephen Chu |
| [HDFS-4041](https://issues.apache.org/jira/browse/HDFS-4041) | Hadoop HDFS Maven protoc calls must not depend on external sh script |  Major | build | Chris Nauroth | Chris Nauroth |
| [HDFS-4037](https://issues.apache.org/jira/browse/HDFS-4037) | Rename the getReplication() method in BlockCollection to getBlockReplication() |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-4014](https://issues.apache.org/jira/browse/HDFS-4014) | Fix warnings found by findbugs2 |  Major | . | Eli Collins | Eli Collins |
| [HDFS-4008](https://issues.apache.org/jira/browse/HDFS-4008) | TestBalancerWithEncryptedTransfer needs a timeout |  Minor | test | Eli Collins | Eli Collins |
| [HDFS-4000](https://issues.apache.org/jira/browse/HDFS-4000) | TestParallelLocalRead fails with "input ByteBuffers must be direct buffers" |  Major | . | Eli Collins | Colin Patrick McCabe |
| [HDFS-3957](https://issues.apache.org/jira/browse/HDFS-3957) | Change MutableQuantiles to use a shared thread for rolling over metrics |  Minor | . | Andrew Wang | Andrew Wang |
| [HDFS-3939](https://issues.apache.org/jira/browse/HDFS-3939) | NN RPC address cleanup |  Minor | namenode | Eli Collins | Eli Collins |
| [HDFS-3925](https://issues.apache.org/jira/browse/HDFS-3925) | Prettify PipelineAck#toString() for printing to a log |  Minor | . | Andrew Wang | Andrew Wang |
| [HDFS-3910](https://issues.apache.org/jira/browse/HDFS-3910) | DFSTestUtil#waitReplication should timeout |  Minor | test | Eli Collins | Eli Collins |
| [HDFS-3896](https://issues.apache.org/jira/browse/HDFS-3896) | Add descriptions for dfs.namenode.rpc-address and dfs.namenode.servicerpc-address to hdfs-default.xml |  Minor | . | Jeff Lord | Jeff Lord |
| [HDFS-3813](https://issues.apache.org/jira/browse/HDFS-3813) | Log error message if security and WebHDFS are enabled but principal/keytab are not configured |  Major | security, webhdfs | Stephen Chu | Stephen Chu |
| [HDFS-3703](https://issues.apache.org/jira/browse/HDFS-3703) | Decrease the datanode failure detection time |  Major | datanode, namenode | Nicolas Liochon | Jing Zhao |
| [HDFS-3682](https://issues.apache.org/jira/browse/HDFS-3682) | MiniDFSCluster#init should provide more info when it fails |  Minor | test | Eli Collins | Todd Lipcon |
| [HDFS-3680](https://issues.apache.org/jira/browse/HDFS-3680) | Allow customized audit logging in HDFS FSNamesystem |  Minor | namenode | Marcelo Vanzin | Marcelo Vanzin |
| [HDFS-3483](https://issues.apache.org/jira/browse/HDFS-3483) | Better error message when hdfs fsck is run against a ViewFS config |  Major | . | Stephen Chu | Stephen Fritz |
| [HDFS-2946](https://issues.apache.org/jira/browse/HDFS-2946) | HA: Put a cap on the number of completed edits files retained by the NN |  Major | ha, namenode | Aaron T. Myers | Aaron T. Myers |
| [HDFS-2656](https://issues.apache.org/jira/browse/HDFS-2656) | Implement a pure c client based on webhdfs |  Major | webhdfs | Zhanwei Wang | Jing Zhao |
| [MAPREDUCE-4977](https://issues.apache.org/jira/browse/MAPREDUCE-4977) | Documentation for pluggable shuffle and pluggable sort |  Major | documentation | Alejandro Abdelnur | Alejandro Abdelnur |
| [MAPREDUCE-4971](https://issues.apache.org/jira/browse/MAPREDUCE-4971) | Minor extensibility enhancements |  Minor | . | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-4949](https://issues.apache.org/jira/browse/MAPREDUCE-4949) | Enable multiple pi jobs to run in parallel |  Minor | examples | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-4907](https://issues.apache.org/jira/browse/MAPREDUCE-4907) | TrackerDistributedCacheManager issues too many getFileStatus calls |  Major | mrv1, tasktracker | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-4899](https://issues.apache.org/jira/browse/MAPREDUCE-4899) | Provide a plugin to the Yarn Web App Proxy to generate tracking links for M/R appllications given the ID |  Major | . | Derek Dagit | Derek Dagit |
| [MAPREDUCE-4845](https://issues.apache.org/jira/browse/MAPREDUCE-4845) | ClusterStatus.getMaxMemory() and getUsedMemory() exist in MR1 but not MR2 |  Major | client | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-4838](https://issues.apache.org/jira/browse/MAPREDUCE-4838) | Add extra info to JH files |  Major | . | Arun C Murthy | Zhijie Shen |
| [MAPREDUCE-4822](https://issues.apache.org/jira/browse/MAPREDUCE-4822) | Unnecessary conversions in History Events |  Trivial | jobhistoryserver | Robert Joseph Evans | Chu Tong |
| [MAPREDUCE-4811](https://issues.apache.org/jira/browse/MAPREDUCE-4811) | JobHistoryServer should show when it was started in WebUI About page |  Minor | jobhistoryserver, mrv2 | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-4810](https://issues.apache.org/jira/browse/MAPREDUCE-4810) | Add admin command options for ApplicationMaster |  Minor | applicationmaster | Jason Lowe | Jerry Chen |
| [MAPREDUCE-4802](https://issues.apache.org/jira/browse/MAPREDUCE-4802) | Takes a long time to load the task list on the AM for large jobs |  Major | mr-am, mrv2, webapps | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-4777](https://issues.apache.org/jira/browse/MAPREDUCE-4777) | In TestIFile, testIFileReaderWithCodec relies on testIFileWriterWithCodec |  Minor | . | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-4764](https://issues.apache.org/jira/browse/MAPREDUCE-4764) | repair test org.apache.hadoop.mapreduce.security.TestBinaryTokenFile |  Major | . | Ivan A. Veselovsky |  |
| [MAPREDUCE-4763](https://issues.apache.org/jira/browse/MAPREDUCE-4763) | repair test org.apache.hadoop.mapreduce.security.TestUmbilicalProtocolWithJobToken |  Minor | . | Ivan A. Veselovsky |  |
| [MAPREDUCE-4752](https://issues.apache.org/jira/browse/MAPREDUCE-4752) | Reduce MR AM memory usage through String Interning |  Major | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-4736](https://issues.apache.org/jira/browse/MAPREDUCE-4736) | Remove obsolete option [-rootDir] from TestDFSIO |  Trivial | test | Brandon Li | Brandon Li |
| [MAPREDUCE-4723](https://issues.apache.org/jira/browse/MAPREDUCE-4723) | Fix warnings found by findbugs 2 |  Major | . | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-4703](https://issues.apache.org/jira/browse/MAPREDUCE-4703) | Add the ability to start the MiniMRClientCluster using the configurations used before it is being stopped. |  Major | mrv1, mrv2, test | Ahmed Radwan | Ahmed Radwan |
| [MAPREDUCE-4666](https://issues.apache.org/jira/browse/MAPREDUCE-4666) | JVM metrics for history server |  Minor | jobhistoryserver | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4616](https://issues.apache.org/jira/browse/MAPREDUCE-4616) | Improvement to MultipleOutputs javadocs |  Minor | documentation | Tony Burton | Tony Burton |
| [MAPREDUCE-4517](https://issues.apache.org/jira/browse/MAPREDUCE-4517) | Too many INFO messages written out during AM to RM heartbeat |  Minor | applicationmaster | James Kinley | Jason Lowe |
| [MAPREDUCE-4458](https://issues.apache.org/jira/browse/MAPREDUCE-4458) | Warn if java.library.path is used for AM or Task |  Major | mrv2 | Robert Joseph Evans | Robert Parker |
| [MAPREDUCE-4229](https://issues.apache.org/jira/browse/MAPREDUCE-4229) | Counter names' memory usage can be decreased by interning |  Major | jobtracker | Todd Lipcon | Miomir Boljanovic |
| [YARN-331](https://issues.apache.org/jira/browse/YARN-331) | Fill in missing fair scheduler documentation |  Major | scheduler | Sandy Ryza | Sandy Ryza |
| [YARN-328](https://issues.apache.org/jira/browse/YARN-328) | Use token request messages defined in hadoop common |  Major | resourcemanager | Suresh Srinivas | Suresh Srinivas |
| [YARN-315](https://issues.apache.org/jira/browse/YARN-315) | Use security token protobuf definition from hadoop common |  Major | . | Suresh Srinivas | Suresh Srinivas |
| [YARN-285](https://issues.apache.org/jira/browse/YARN-285) | RM should be able to provide a tracking link for apps that have already been purged |  Major | . | Derek Dagit | Derek Dagit |
| [YARN-277](https://issues.apache.org/jira/browse/YARN-277) | Use AMRMClient in DistributedShell to exemplify the approach |  Major | . | Bikas Saha | Bikas Saha |
| [YARN-254](https://issues.apache.org/jira/browse/YARN-254) | Update fair scheduler web UI for hierarchical queues |  Major | resourcemanager, scheduler | Sandy Ryza | Sandy Ryza |
| [YARN-222](https://issues.apache.org/jira/browse/YARN-222) | Fair scheduler should create queue for each user by default |  Major | resourcemanager, scheduler | Sandy Ryza | Sandy Ryza |
| [YARN-216](https://issues.apache.org/jira/browse/YARN-216) | Remove jquery theming support |  Major | . | Todd Lipcon | Robert Joseph Evans |
| [YARN-184](https://issues.apache.org/jira/browse/YARN-184) | Remove unnecessary locking in fair scheduler, and address findbugs excludes. |  Major | . | Sandy Ryza | Sandy Ryza |
| [YARN-183](https://issues.apache.org/jira/browse/YARN-183) | Clean up fair scheduler code |  Minor | scheduler | Sandy Ryza | Sandy Ryza |
| [YARN-169](https://issues.apache.org/jira/browse/YARN-169) | Update log4j.appender.EventCounter to use org.apache.hadoop.log.metrics.EventCounter |  Minor | nodemanager | Anthony Rojas | Anthony Rojas |
| [YARN-165](https://issues.apache.org/jira/browse/YARN-165) | RM should point tracking URL to RM web page for app when AM fails |  Blocker | resourcemanager | Jason Lowe | Jason Lowe |
| [YARN-129](https://issues.apache.org/jira/browse/YARN-129) | Simplify classpath construction for mini YARN tests |  Major | client | Tom White | Tom White |
| [YARN-57](https://issues.apache.org/jira/browse/YARN-57) | Plugable process tree |  Major | nodemanager | Radim Kolar | Radim Kolar |
| [YARN-23](https://issues.apache.org/jira/browse/YARN-23) | FairScheduler: FSQueueSchedulable#updateDemand() - potential redundant aggregation |  Major | scheduler | Karthik Kambatla | Karthik Kambatla |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-9289](https://issues.apache.org/jira/browse/HADOOP-9289) | FsShell rm -f fails for non-matching globs |  Blocker | fs | Daryn Sharp | Daryn Sharp |
| [HADOOP-9278](https://issues.apache.org/jira/browse/HADOOP-9278) | HarFileSystem may leak file handle |  Major | fs | Chris Nauroth | Chris Nauroth |
| [HADOOP-9260](https://issues.apache.org/jira/browse/HADOOP-9260) | Hadoop version may be not correct when starting name node or data node |  Critical | . | Jerry Chen | Chris Nauroth |
| [HADOOP-9255](https://issues.apache.org/jira/browse/HADOOP-9255) | relnotes.py missing last jira |  Critical | scripts | Thomas Graves | Thomas Graves |
| [HADOOP-9252](https://issues.apache.org/jira/browse/HADOOP-9252) | StringUtils.humanReadableInt(..) has a race condition |  Minor | util | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-9231](https://issues.apache.org/jira/browse/HADOOP-9231) | Parametrize staging URL for the uniformity of distributionManagement |  Major | build | Konstantin Boudnik | Konstantin Boudnik |
| [HADOOP-9221](https://issues.apache.org/jira/browse/HADOOP-9221) | Convert remaining xdocs to APT |  Major | . | Andy Isaacson | Andy Isaacson |
| [HADOOP-9215](https://issues.apache.org/jira/browse/HADOOP-9215) | when using cmake-2.6, libhadoop.so doesn't get created (only libhadoop.so.1.0.0) |  Blocker | . | Thomas Graves | Colin Patrick McCabe |
| [HADOOP-9212](https://issues.apache.org/jira/browse/HADOOP-9212) | Potential deadlock in FileSystem.Cache/IPC/UGI |  Major | fs | Tom White | Tom White |
| [HADOOP-9203](https://issues.apache.org/jira/browse/HADOOP-9203) | RPCCallBenchmark should find a random available port |  Trivial | ipc, test | Andrew Purtell | Andrew Purtell |
| [HADOOP-9193](https://issues.apache.org/jira/browse/HADOOP-9193) | hadoop script can inadvertently expand wildcard arguments when delegating to hdfs script |  Minor | scripts | Jason Lowe | Andy Isaacson |
| [HADOOP-9190](https://issues.apache.org/jira/browse/HADOOP-9190) | packaging docs is broken |  Major | documentation | Thomas Graves | Andy Isaacson |
| [HADOOP-9183](https://issues.apache.org/jira/browse/HADOOP-9183) | Potential deadlock in ActiveStandbyElector |  Major | ha | Tom White | Tom White |
| [HADOOP-9181](https://issues.apache.org/jira/browse/HADOOP-9181) | Set daemon flag for HttpServer's QueuedThreadPool |  Major | . | Liang Xie | Liang Xie |
| [HADOOP-9178](https://issues.apache.org/jira/browse/HADOOP-9178) | src/main/conf is missing hadoop-policy.xml |  Minor | . | Sandy Ryza | Sandy Ryza |
| [HADOOP-9173](https://issues.apache.org/jira/browse/HADOOP-9173) | Add security token protobuf definition to common and use it in hdfs |  Major | . | Suresh Srinivas | Suresh Srinivas |
| [HADOOP-9155](https://issues.apache.org/jira/browse/HADOOP-9155) | FsPermission should have different default value, 777 for directory and 666 for file |  Minor | . | Binglin Chang | Binglin Chang |
| [HADOOP-9152](https://issues.apache.org/jira/browse/HADOOP-9152) | HDFS can report negative DFS Used on clusters with very small amounts of data |  Minor | fs | Brock Noland | Brock Noland |
| [HADOOP-9135](https://issues.apache.org/jira/browse/HADOOP-9135) | JniBasedUnixGroupsMappingWithFallback should log at debug rather than info during fallback |  Trivial | security | Colin Patrick McCabe | Colin Patrick McCabe |
| [HADOOP-9124](https://issues.apache.org/jira/browse/HADOOP-9124) | SortedMapWritable violates contract of Map interface for equals() and hashCode() |  Minor | io | Patrick Hunt | Surenkumar Nihalani |
| [HADOOP-9113](https://issues.apache.org/jira/browse/HADOOP-9113) | o.a.h.fs.TestDelegationTokenRenewer is failing intermittently |  Major | security, test | Karthik Kambatla | Karthik Kambatla |
| [HADOOP-9105](https://issues.apache.org/jira/browse/HADOOP-9105) | FsShell -moveFromLocal erroneously fails |  Major | fs | Daryn Sharp | Daryn Sharp |
| [HADOOP-9103](https://issues.apache.org/jira/browse/HADOOP-9103) | UTF8 class does not properly decode Unicode characters outside the basic multilingual plane |  Major | io | yixiaohua | Todd Lipcon |
| [HADOOP-9097](https://issues.apache.org/jira/browse/HADOOP-9097) | Maven RAT plugin is not checking all source files |  Critical | build | Tom White | Thomas Graves |
| [HADOOP-9072](https://issues.apache.org/jira/browse/HADOOP-9072) | Hadoop-Common-0.23-Build Fails to build in Jenkins |  Major | . | Robert Parker | Robert Parker |
| [HADOOP-9064](https://issues.apache.org/jira/browse/HADOOP-9064) | Augment DelegationTokenRenewer API to cancel the tokens on calls to removeRenewAction |  Major | security | Karthik Kambatla | Karthik Kambatla |
| [HADOOP-9049](https://issues.apache.org/jira/browse/HADOOP-9049) | DelegationTokenRenewer needs to be Singleton and FileSystems should register/deregister to/from. |  Major | security | Karthik Kambatla | Karthik Kambatla |
| [HADOOP-9041](https://issues.apache.org/jira/browse/HADOOP-9041) | FileSystem initialization can go into infinite loop |  Critical | fs | Radim Kolar | Radim Kolar |
| [HADOOP-9025](https://issues.apache.org/jira/browse/HADOOP-9025) | org.apache.hadoop.tools.TestCopyListing failing |  Major | . | Robert Joseph Evans | Jonathan Eagles |
| [HADOOP-9022](https://issues.apache.org/jira/browse/HADOOP-9022) | Hadoop distcp tool fails to copy file if -m 0 specified |  Major | . | Haiyang Jiang | Jonathan Eagles |
| [HADOOP-8994](https://issues.apache.org/jira/browse/HADOOP-8994) | TestDFSShell creates file named "noFileHere", making further tests hard to understand |  Minor | test | Andy Isaacson | Andy Isaacson |
| [HADOOP-8986](https://issues.apache.org/jira/browse/HADOOP-8986) | Server$Call object is never released after it is sent |  Critical | ipc | Robert Joseph Evans | Robert Joseph Evans |
| [HADOOP-8981](https://issues.apache.org/jira/browse/HADOOP-8981) | TestMetricsSystemImpl fails on Windows |  Major | metrics | Chris Nauroth | Xuan Gong |
| [HADOOP-8962](https://issues.apache.org/jira/browse/HADOOP-8962) | RawLocalFileSystem.listStatus fails when a child filename contains a colon |  Critical | fs | Jason Lowe | Jason Lowe |
| [HADOOP-8948](https://issues.apache.org/jira/browse/HADOOP-8948) | TestFileUtil.testGetDU fails on Windows due to incorrect assumption of line separator |  Major | test | Chris Nauroth | Chris Nauroth |
| [HADOOP-8913](https://issues.apache.org/jira/browse/HADOOP-8913) | hadoop-metrics2.properties should give units in comment for sampling period |  Minor | metrics | Sandy Ryza | Sandy Ryza |
| [HADOOP-8912](https://issues.apache.org/jira/browse/HADOOP-8912) | adding .gitattributes file to prevent CRLF and LF mismatches for source and text files |  Major | build | Raja Aluri | Raja Aluri |
| [HADOOP-8911](https://issues.apache.org/jira/browse/HADOOP-8911) | CRLF characters in source and text files |  Major | build | Raja Aluri | Raja Aluri |
| [HADOOP-8906](https://issues.apache.org/jira/browse/HADOOP-8906) | paths with multiple globs are unreliable |  Critical | fs | Daryn Sharp | Daryn Sharp |
| [HADOOP-8901](https://issues.apache.org/jira/browse/HADOOP-8901) | GZip and Snappy support may not work without unversioned libraries |  Minor | native | Colin Patrick McCabe | Colin Patrick McCabe |
| [HADOOP-8900](https://issues.apache.org/jira/browse/HADOOP-8900) | BuiltInGzipDecompressor throws IOException - stored gzip size doesn't match decompressed size |  Major | . | Slavik Krassovsky | Andy Isaacson |
| [HADOOP-8883](https://issues.apache.org/jira/browse/HADOOP-8883) | Anonymous fallback in KerberosAuthenticator is broken |  Major | . | Robert Kanter | Robert Kanter |
| [HADOOP-8881](https://issues.apache.org/jira/browse/HADOOP-8881) | FileBasedKeyStoresFactory initialization logging should be debug not info |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-8878](https://issues.apache.org/jira/browse/HADOOP-8878) | uppercase namenode hostname causes hadoop dfs calls with webhdfs filesystem and fsck to fail when security is on |  Major | . | Arpit Gupta | Arpit Gupta |
| [HADOOP-8855](https://issues.apache.org/jira/browse/HADOOP-8855) | SSL-based image transfer does not work when Kerberos is disabled |  Minor | security | Todd Lipcon | Todd Lipcon |
| [HADOOP-8843](https://issues.apache.org/jira/browse/HADOOP-8843) | Old trash directories are never deleted on upgrade from 1.x |  Critical | . | Robert Joseph Evans | Jason Lowe |
| [HADOOP-8833](https://issues.apache.org/jira/browse/HADOOP-8833) | fs -text should make sure to call inputstream.seek(0) before using input stream |  Major | fs | Harsh J | Harsh J |
| [HADOOP-8822](https://issues.apache.org/jira/browse/HADOOP-8822) | relnotes.py was deleted post mavenization |  Major | . | Robert Joseph Evans | Robert Joseph Evans |
| [HADOOP-8819](https://issues.apache.org/jira/browse/HADOOP-8819) | Should use && instead of  & in a few places in FTPFileSystem,FTPInputStream,S3InputStream,ViewFileSystem,ViewFs |  Major | fs | Brandon Li | Brandon Li |
| [HADOOP-8816](https://issues.apache.org/jira/browse/HADOOP-8816) | HTTP Error 413 full HEAD if using kerberos authentication |  Major | net | Moritz Moeller | Moritz Moeller |
| [HADOOP-8811](https://issues.apache.org/jira/browse/HADOOP-8811) | Compile hadoop native library in FreeBSD |  Critical | native | Radim Kolar | Radim Kolar |
| [HADOOP-8795](https://issues.apache.org/jira/browse/HADOOP-8795) | BASH tab completion doesn't look in PATH, assumes path to executable is specified |  Minor | scripts | Sean Mackrory | Sean Mackrory |
| [HADOOP-8791](https://issues.apache.org/jira/browse/HADOOP-8791) | rm "Only deletes non empty directory and files." |  Major | documentation | Bertrand Dechoux | Jing Zhao |
| [HADOOP-8786](https://issues.apache.org/jira/browse/HADOOP-8786) | HttpServer continues to start even if AuthenticationFilter fails to init |  Major | . | Todd Lipcon | Todd Lipcon |
| [HADOOP-8780](https://issues.apache.org/jira/browse/HADOOP-8780) | Update DeprecatedProperties apt file |  Major | . | Ahmed Radwan | Ahmed Radwan |
| [HADOOP-8756](https://issues.apache.org/jira/browse/HADOOP-8756) | Fix SEGV when libsnappy is in java.library.path but not LD\_LIBRARY\_PATH |  Minor | native | Colin Patrick McCabe | Colin Patrick McCabe |
| [HADOOP-8713](https://issues.apache.org/jira/browse/HADOOP-8713) | TestRPCCompatibility fails intermittently with JDK7 |  Major | test | Trevor Robinson | Trevor Robinson |
| [HADOOP-8684](https://issues.apache.org/jira/browse/HADOOP-8684) | Deadlock between WritableComparator and WritableComparable |  Minor | io | Hiroshi Ikeda | Jing Zhao |
| [HADOOP-8616](https://issues.apache.org/jira/browse/HADOOP-8616) | ViewFS configuration requires a trailing slash |  Major | viewfs | Eli Collins | Sandy Ryza |
| [HADOOP-8589](https://issues.apache.org/jira/browse/HADOOP-8589) | ViewFs tests fail when tests and home dirs are nested |  Major | fs, test | Andrey Klochkov | Sanjay Radia |
| [HADOOP-8418](https://issues.apache.org/jira/browse/HADOOP-8418) | Fix UGI for IBM JDK running on Windows |  Major | security | Luke Lu | Yu Gao |
| [HADOOP-7294](https://issues.apache.org/jira/browse/HADOOP-7294) | FileUtil uses wrong stat command for FreeBSD |  Major | fs | Vitalii Tymchyshyn |  |
| [HADOOP-7115](https://issues.apache.org/jira/browse/HADOOP-7115) | Add a cache for getpwuid\_r and getpwgid\_r calls |  Major | . | Arun C Murthy | Alejandro Abdelnur |
| [HADOOP-6762](https://issues.apache.org/jira/browse/HADOOP-6762) | exception while doing RPC I/O closes channel |  Critical | . | sam rash | sam rash |
| [HADOOP-6607](https://issues.apache.org/jira/browse/HADOOP-6607) | Add different variants of non caching HTTP headers |  Minor | io | Steve Loughran | Alejandro Abdelnur |
| [HDFS-4468](https://issues.apache.org/jira/browse/HDFS-4468) | Fix TestHDFSCLI and TestQuota for HADOOP-9252 |  Minor | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-4462](https://issues.apache.org/jira/browse/HDFS-4462) | 2NN will fail to checkpoint after an HDFS upgrade from a pre-federation version of HDFS |  Major | namenode | Aaron T. Myers | Aaron T. Myers |
| [HDFS-4458](https://issues.apache.org/jira/browse/HDFS-4458) | start balancer failed with "Failed to create file [/system/balancer.id]"  if configure IP on fs.defaultFS |  Major | balancer & mover | Wenwu Peng | Binglin Chang |
| [HDFS-4452](https://issues.apache.org/jira/browse/HDFS-4452) | getAdditionalBlock() can create multiple blocks if the client times out and retries. |  Critical | namenode | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-4444](https://issues.apache.org/jira/browse/HDFS-4444) | Add space between total transaction time and number of transactions in FSEditLog#printStatistics |  Trivial | . | Stephen Chu | Stephen Chu |
| [HDFS-4443](https://issues.apache.org/jira/browse/HDFS-4443) | Remove trailing '`' character from HDFS nodelist jsp |  Trivial | namenode | Christian Rohling | Christian Rohling |
| [HDFS-4428](https://issues.apache.org/jira/browse/HDFS-4428) | FsDatasetImpl should disclose what the error is when a rename fails |  Minor | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-4426](https://issues.apache.org/jira/browse/HDFS-4426) | Secondary namenode shuts down immediately after startup |  Blocker | namenode | Jason Lowe | Arpit Agarwal |
| [HDFS-4415](https://issues.apache.org/jira/browse/HDFS-4415) | HostnameFilter should handle hostname resolution failures and continue processing |  Major | . | Robert Kanter | Robert Kanter |
| [HDFS-4404](https://issues.apache.org/jira/browse/HDFS-4404) | Create file failure when the machine of first attempted NameNode is down |  Critical | ha, hdfs-client | liaowenrui | Todd Lipcon |
| [HDFS-4403](https://issues.apache.org/jira/browse/HDFS-4403) | DFSClient can infer checksum type when not provided by reading first byte |  Minor | hdfs-client | Todd Lipcon | Todd Lipcon |
| [HDFS-4385](https://issues.apache.org/jira/browse/HDFS-4385) | Maven RAT plugin is not checking all source files |  Critical | build | Thomas Graves | Thomas Graves |
| [HDFS-4384](https://issues.apache.org/jira/browse/HDFS-4384) | test\_libhdfs\_threaded gets SEGV if JNIEnv cannot be initialized |  Minor | libhdfs | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-4377](https://issues.apache.org/jira/browse/HDFS-4377) | Some trivial DN comment cleanup |  Trivial | . | Eli Collins | Eli Collins |
| [HDFS-4363](https://issues.apache.org/jira/browse/HDFS-4363) | Combine PBHelper and HdfsProtoUtil and remove redundant methods |  Major | . | Suresh Srinivas | Suresh Srinivas |
| [HDFS-4359](https://issues.apache.org/jira/browse/HDFS-4359) | remove an unnecessary synchronized keyword in BPOfferService.java |  Major | datanode | Liang Xie | Liang Xie |
| [HDFS-4351](https://issues.apache.org/jira/browse/HDFS-4351) | Fix BlockPlacementPolicyDefault#chooseTarget when avoiding stale nodes |  Major | namenode | Andrew Wang | Andrew Wang |
| [HDFS-4347](https://issues.apache.org/jira/browse/HDFS-4347) | TestBackupNode can go into infinite loop "Waiting checkpoint to complete." |  Major | namenode, test | Konstantin Shvachko | Plamen Jeliazkov |
| [HDFS-4344](https://issues.apache.org/jira/browse/HDFS-4344) | dfshealth.jsp throws NumberFormatException when dfs.hosts/dfs.hosts.exclude includes port number |  Major | namenode | tamtam180 | Andy Isaacson |
| [HDFS-4315](https://issues.apache.org/jira/browse/HDFS-4315) | DNs with multiple BPs can have BPOfferServices fail to start due to unsynchronized map access |  Major | datanode | Aaron T. Myers | Aaron T. Myers |
| [HDFS-4308](https://issues.apache.org/jira/browse/HDFS-4308) | addBlock() should persist file blocks once |  Major | namenode | Konstantin Shvachko | Plamen Jeliazkov |
| [HDFS-4307](https://issues.apache.org/jira/browse/HDFS-4307) | SocketCache should use monotonic time |  Minor | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-4306](https://issues.apache.org/jira/browse/HDFS-4306) | PBHelper.convertLocatedBlock miss convert BlockToken |  Major | . | Binglin Chang | Binglin Chang |
| [HDFS-4302](https://issues.apache.org/jira/browse/HDFS-4302) | Precondition in EditLogFileInputStream's length() method is checked too early in NameNode startup, causing fatal exception |  Major | ha, namenode | Eugene Koontz | Eugene Koontz |
| [HDFS-4295](https://issues.apache.org/jira/browse/HDFS-4295) | Using port 1023 should be valid when starting Secure DataNode |  Major | security | Stephen Chu | Stephen Chu |
| [HDFS-4294](https://issues.apache.org/jira/browse/HDFS-4294) | Backwards compatibility is not maintained for TestVolumeId |  Major | . | Robert Parker | Robert Parker |
| [HDFS-4292](https://issues.apache.org/jira/browse/HDFS-4292) | Sanity check not correct in RemoteBlockReader2.newBlockReader |  Minor | . | Binglin Chang | Binglin Chang |
| [HDFS-4291](https://issues.apache.org/jira/browse/HDFS-4291) | edit log unit tests leave stray test\_edit\_log\_file around |  Minor | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-4288](https://issues.apache.org/jira/browse/HDFS-4288) | NN accepts incremental BR as IBR in safemode |  Critical | namenode | Daryn Sharp | Daryn Sharp |
| [HDFS-4282](https://issues.apache.org/jira/browse/HDFS-4282) | TestEditLog.testFuzzSequences FAILED in all pre-commit test |  Major | namenode, test | Junping Du | Todd Lipcon |
| [HDFS-4279](https://issues.apache.org/jira/browse/HDFS-4279) | NameNode does not initialize generic conf keys when started with -recover |  Minor | namenode | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-4274](https://issues.apache.org/jira/browse/HDFS-4274) | BlockPoolSliceScanner does not close verification log during shutdown |  Minor | datanode | Chris Nauroth | Chris Nauroth |
| [HDFS-4270](https://issues.apache.org/jira/browse/HDFS-4270) | Replications of the highest priority should be allowed to choose a source datanode that has reached its max replication limit |  Minor | namenode | Derek Dagit | Derek Dagit |
| [HDFS-4268](https://issues.apache.org/jira/browse/HDFS-4268) | Remove redundant enum NNHAStatusHeartbeat.State |  Major | namenode | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-4242](https://issues.apache.org/jira/browse/HDFS-4242) | Map.Entry is incorrectly used in LeaseManager |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-4238](https://issues.apache.org/jira/browse/HDFS-4238) | [HA] Standby namenode should not do purging of shared storage edits. |  Major | ha | Vinayakumar B | Todd Lipcon |
| [HDFS-4236](https://issues.apache.org/jira/browse/HDFS-4236) | Regression: HDFS-4171 puts artificial limit on username length |  Blocker | . | Allen Wittenauer | Alejandro Abdelnur |
| [HDFS-4232](https://issues.apache.org/jira/browse/HDFS-4232) | NN fails to write a fsimage with stale leases |  Blocker | namenode | Daryn Sharp | Daryn Sharp |
| [HDFS-4216](https://issues.apache.org/jira/browse/HDFS-4216) | Adding symlink should not ignore QuotaExceededException |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-4186](https://issues.apache.org/jira/browse/HDFS-4186) | logSync() is called with the write lock held while releasing lease |  Critical | namenode | Kihwal Lee | Kihwal Lee |
| [HDFS-4182](https://issues.apache.org/jira/browse/HDFS-4182) | SecondaryNameNode leaks NameCache entries |  Critical | namenode | Todd Lipcon | Robert Joseph Evans |
| [HDFS-4181](https://issues.apache.org/jira/browse/HDFS-4181) | LeaseManager tries to double remove and prints extra messages |  Critical | namenode | Kihwal Lee | Kihwal Lee |
| [HDFS-4179](https://issues.apache.org/jira/browse/HDFS-4179) | BackupNode: allow reads, fix checkpointing, safeMode |  Major | namenode | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-4178](https://issues.apache.org/jira/browse/HDFS-4178) | shell scripts should not close stderr |  Major | scripts | Andy Isaacson | Andy Isaacson |
| [HDFS-4172](https://issues.apache.org/jira/browse/HDFS-4172) | namenode does not URI-encode parameters when building URI for datanode request |  Minor | namenode | Derek Dagit | Derek Dagit |
| [HDFS-4171](https://issues.apache.org/jira/browse/HDFS-4171) | WebHDFS and HttpFs should accept only valid Unix user names |  Major | . | Harsh J | Alejandro Abdelnur |
| [HDFS-4164](https://issues.apache.org/jira/browse/HDFS-4164) | fuse\_dfs: add -lrt to the compiler command line on Linux |  Minor | fuse-dfs | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-4162](https://issues.apache.org/jira/browse/HDFS-4162) | Some malformed and unquoted HTML strings are returned from datanode web ui |  Minor | datanode | Derek Dagit | Derek Dagit |
| [HDFS-4156](https://issues.apache.org/jira/browse/HDFS-4156) | Seeking to a negative position should throw an IOE |  Major | . | Eli Collins | Eli Reisman |
| [HDFS-4140](https://issues.apache.org/jira/browse/HDFS-4140) | fuse-dfs handles open(O\_TRUNC) poorly |  Major | fuse-dfs | Andy Isaacson | Colin Patrick McCabe |
| [HDFS-4139](https://issues.apache.org/jira/browse/HDFS-4139) | fuse-dfs RO mode still allows file truncation |  Major | fuse-dfs | Andy Isaacson | Colin Patrick McCabe |
| [HDFS-4132](https://issues.apache.org/jira/browse/HDFS-4132) | when libwebhdfs is not enabled, nativeMiniDfsClient frees uninitialized memory |  Major | libhdfs | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-4127](https://issues.apache.org/jira/browse/HDFS-4127) | Log message is not correct in case of short of replica |  Minor | namenode | Junping Du | Junping Du |
| [HDFS-4112](https://issues.apache.org/jira/browse/HDFS-4112) | A few improvements on INodeDirectory |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-4107](https://issues.apache.org/jira/browse/HDFS-4107) | Add utility methods to cast INode to INodeFile and INodeFileUnderConstruction |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-4106](https://issues.apache.org/jira/browse/HDFS-4106) | BPServiceActor#lastHeartbeat, lastBlockReport and lastDeletedReport should be declared as volatile |  Minor | namenode, test | Jing Zhao | Jing Zhao |
| [HDFS-4105](https://issues.apache.org/jira/browse/HDFS-4105) | the SPNEGO user for secondary namenode should use the web keytab |  Major | . | Arpit Gupta | Arpit Gupta |
| [HDFS-4104](https://issues.apache.org/jira/browse/HDFS-4104) | dfs -test -d prints inappropriate error on nonexistent directory |  Minor | . | Andy Isaacson | Andy Isaacson |
| [HDFS-4099](https://issues.apache.org/jira/browse/HDFS-4099) | Clean up replication code and add more javadoc |  Minor | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-4090](https://issues.apache.org/jira/browse/HDFS-4090) | getFileChecksum() result incompatible when called against zero-byte files. |  Critical | hdfs-client | Kihwal Lee | Kihwal Lee |
| [HDFS-4075](https://issues.apache.org/jira/browse/HDFS-4075) | Reduce recommissioning overhead |  Critical | namenode | Kihwal Lee | Kihwal Lee |
| [HDFS-4072](https://issues.apache.org/jira/browse/HDFS-4072) | On file deletion remove corresponding blocks pending replication |  Minor | namenode | Jing Zhao | Jing Zhao |
| [HDFS-4061](https://issues.apache.org/jira/browse/HDFS-4061) | TestBalancer and TestUnderReplicatedBlocks need timeouts |  Major | . | Eli Collins | Eli Collins |
| [HDFS-4055](https://issues.apache.org/jira/browse/HDFS-4055) | TestAuditLogs is flaky |  Major | . | Binglin Chang | Binglin Chang |
| [HDFS-4049](https://issues.apache.org/jira/browse/HDFS-4049) | hflush performance regression due to nagling delays |  Critical | datanode, performance | Todd Lipcon | Todd Lipcon |
| [HDFS-4046](https://issues.apache.org/jira/browse/HDFS-4046) | ChecksumTypeProto use NULL as enum value which is illegal in C/C++ |  Minor | datanode, namenode | Binglin Chang | Binglin Chang |
| [HDFS-4044](https://issues.apache.org/jira/browse/HDFS-4044) | Duplicate ChecksumType definition in HDFS .proto files |  Major | datanode | Binglin Chang | Binglin Chang |
| [HDFS-4036](https://issues.apache.org/jira/browse/HDFS-4036) | FSDirectory.unprotectedAddFile(..) should not throw UnresolvedLinkException |  Major | namenode | Tsz Wo Nicholas Sze | Jing Zhao |
| [HDFS-4022](https://issues.apache.org/jira/browse/HDFS-4022) | Replication not happening for appended block |  Blocker | . | suja s | Vinayakumar B |
| [HDFS-4021](https://issues.apache.org/jira/browse/HDFS-4021) | Misleading error message when resources are low on the NameNode |  Minor | namenode | Colin Patrick McCabe | Christopher Conner |
| [HDFS-4020](https://issues.apache.org/jira/browse/HDFS-4020) | TestRBWBlockInvalidation may time out |  Major | . | Eli Collins | Eli Collins |
| [HDFS-4018](https://issues.apache.org/jira/browse/HDFS-4018) | TestDataNodeMultipleRegistrations#testMiniDFSClusterWithMultipleNN is missing some cluster cleanup |  Minor | . | Eli Collins | Eli Collins |
| [HDFS-4006](https://issues.apache.org/jira/browse/HDFS-4006) | TestCheckpoint#testSecondaryHasVeryOutOfDateImage occasionally fails due to unexpected exit |  Major | namenode | Eli Collins | Todd Lipcon |
| [HDFS-3999](https://issues.apache.org/jira/browse/HDFS-3999) | HttpFS OPEN operation expects len parameter, it should be length |  Major | . | Alejandro Abdelnur | Alejandro Abdelnur |
| [HDFS-3997](https://issues.apache.org/jira/browse/HDFS-3997) | OfflineImageViewer incorrectly passes value of imageVersion when visiting IS\_COMPRESSED element |  Trivial | namenode | Mithun Radhakrishnan | Mithun Radhakrishnan |
| [HDFS-3996](https://issues.apache.org/jira/browse/HDFS-3996) | Add debug log removed in HDFS-3873 back |  Minor | . | Eli Collins | Eli Collins |
| [HDFS-3992](https://issues.apache.org/jira/browse/HDFS-3992) | Method org.apache.hadoop.hdfs.TestHftpFileSystem.tearDown() sometimes throws NPEs |  Minor | . | Ivan A. Veselovsky | Ivan A. Veselovsky |
| [HDFS-3990](https://issues.apache.org/jira/browse/HDFS-3990) | NN's health report has severe performance problems |  Critical | namenode | Daryn Sharp | Daryn Sharp |
| [HDFS-3985](https://issues.apache.org/jira/browse/HDFS-3985) | Add timeouts to TestMulitipleNNDataBlockScanner |  Major | test | Eli Collins |  |
| [HDFS-3979](https://issues.apache.org/jira/browse/HDFS-3979) | Fix hsync semantics |  Major | datanode | Lars Hofhansl | Lars Hofhansl |
| [HDFS-3970](https://issues.apache.org/jira/browse/HDFS-3970) | BlockPoolSliceStorage#doRollback(..) should use BlockPoolSliceStorage instead of DataStorage to read prev version file. |  Major | datanode | Vinayakumar B | Vinayakumar B |
| [HDFS-3964](https://issues.apache.org/jira/browse/HDFS-3964) | Make NN log of fs.defaultFS debug rather than info |  Minor | namenode | Eli Collins | Eli Collins |
| [HDFS-3951](https://issues.apache.org/jira/browse/HDFS-3951) | datanode web ui does not work over HTTPS when datanode is started in secure mode |  Major | datanode, security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HDFS-3949](https://issues.apache.org/jira/browse/HDFS-3949) | NameNodeRpcServer#join should join on both client and server RPC servers |  Minor | namenode | Eli Collins | Eli Collins |
| [HDFS-3948](https://issues.apache.org/jira/browse/HDFS-3948) | TestWebHDFS#testNamenodeRestart occasionally fails |  Minor | test | Eli Collins | Jing Zhao |
| [HDFS-3938](https://issues.apache.org/jira/browse/HDFS-3938) | remove current limitations from HttpFS docs |  Major | documentation | Alejandro Abdelnur | Alejandro Abdelnur |
| [HDFS-3936](https://issues.apache.org/jira/browse/HDFS-3936) | MiniDFSCluster shutdown races with BlocksMap usage |  Major | . | Eli Collins | Eli Collins |
| [HDFS-3932](https://issues.apache.org/jira/browse/HDFS-3932) | NameNode Web UI broken if the rpc-address is set to the wildcard |  Major | . | Eli Collins | Eli Collins |
| [HDFS-3931](https://issues.apache.org/jira/browse/HDFS-3931) | TestDatanodeBlockScanner#testBlockCorruptionPolicy2 is broken |  Minor | test | Eli Collins | Andy Isaacson |
| [HDFS-3924](https://issues.apache.org/jira/browse/HDFS-3924) | Multi-byte id in HdfsVolumeId |  Major | hdfs-client | Andrew Wang | Andrew Wang |
| [HDFS-3921](https://issues.apache.org/jira/browse/HDFS-3921) | NN will prematurely consider blocks missing when entering active state while still in safe mode |  Major | . | Stephen Chu | Aaron T. Myers |
| [HDFS-3919](https://issues.apache.org/jira/browse/HDFS-3919) | MiniDFSCluster:waitClusterUp can hang forever |  Minor | test | Andy Isaacson | Andy Isaacson |
| [HDFS-3916](https://issues.apache.org/jira/browse/HDFS-3916) | libwebhdfs (C client) code cleanups |  Major | webhdfs | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3831](https://issues.apache.org/jira/browse/HDFS-3831) | Failure to renew tokens due to test-sources left in classpath |  Critical | security | Jason Lowe | Jason Lowe |
| [HDFS-3829](https://issues.apache.org/jira/browse/HDFS-3829) | TestHftpURLTimeouts fails intermittently with JDK7 |  Major | test | Trevor Robinson | Trevor Robinson |
| [HDFS-3824](https://issues.apache.org/jira/browse/HDFS-3824) | TestHftpDelegationToken fails intermittently with JDK7 |  Major | test | Trevor Robinson | Trevor Robinson |
| [HDFS-3804](https://issues.apache.org/jira/browse/HDFS-3804) | TestHftpFileSystem fails intermittently with JDK7 |  Major | test | Trevor Robinson | Trevor Robinson |
| [HDFS-3753](https://issues.apache.org/jira/browse/HDFS-3753) | Tests don't run with native libraries |  Major | build, test | Eli Collins | Colin Patrick McCabe |
| [HDFS-3678](https://issues.apache.org/jira/browse/HDFS-3678) | Edit log files are never being purged from 2NN |  Critical | namenode | Todd Lipcon | Aaron T. Myers |
| [HDFS-3626](https://issues.apache.org/jira/browse/HDFS-3626) | Creating file with invalid path can corrupt edit log |  Blocker | namenode | Todd Lipcon | Todd Lipcon |
| [HDFS-3625](https://issues.apache.org/jira/browse/HDFS-3625) | Fix TestBackupNode by properly initializing edit log |  Blocker | ha | Eli Collins | Junping Du |
| [HDFS-3616](https://issues.apache.org/jira/browse/HDFS-3616) | TestWebHdfsWithMultipleNameNodes fails with ConcurrentModificationException in DN shutdown |  Major | datanode | Uma Maheswara Rao G | Jing Zhao |
| [HDFS-3553](https://issues.apache.org/jira/browse/HDFS-3553) | Hftp proxy tokens are broken |  Blocker | . | Daryn Sharp | Daryn Sharp |
| [HDFS-3510](https://issues.apache.org/jira/browse/HDFS-3510) | Improve FSEditLog pre-allocation |  Major | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-3507](https://issues.apache.org/jira/browse/HDFS-3507) | DFS#isInSafeMode needs to execute only on Active NameNode |  Critical | ha | Vinayakumar B | Vinayakumar B |
| [HDFS-3429](https://issues.apache.org/jira/browse/HDFS-3429) | DataNode reads checksums even if client does not need them |  Major | datanode, performance | Todd Lipcon | Todd Lipcon |
| [HDFS-3373](https://issues.apache.org/jira/browse/HDFS-3373) | FileContext HDFS implementation can leak socket caches |  Major | hdfs-client | Todd Lipcon | John George |
| [HDFS-3224](https://issues.apache.org/jira/browse/HDFS-3224) | Bug in check for DN re-registration with different storage ID |  Minor | . | Eli Collins | Jason Lowe |
| [HDFS-2264](https://issues.apache.org/jira/browse/HDFS-2264) | NamenodeProtocol has the wrong value for clientPrincipal in KerberosInfo annotation |  Major | namenode | Aaron T. Myers | Aaron T. Myers |
| [HDFS-1322](https://issues.apache.org/jira/browse/HDFS-1322) | Document umask in DistributedFileSystem#mkdirs javadocs |  Major | . | Ravi Gummadi | Colin Patrick McCabe |
| [MAPREDUCE-4969](https://issues.apache.org/jira/browse/MAPREDUCE-4969) | TestKeyValueTextInputFormat test fails with Open JDK 7 |  Major | test | Arpit Agarwal | Arpit Agarwal |
| [MAPREDUCE-4953](https://issues.apache.org/jira/browse/MAPREDUCE-4953) | HadoopPipes misuses fprintf |  Major | pipes | Andy Isaacson | Andy Isaacson |
| [MAPREDUCE-4948](https://issues.apache.org/jira/browse/MAPREDUCE-4948) | TestYARNRunner.testHistoryServerToken failed on trunk |  Critical | client | Junping Du | Junping Du |
| [MAPREDUCE-4946](https://issues.apache.org/jira/browse/MAPREDUCE-4946) | Type conversion of map completion events leads to performance problems with large jobs |  Critical | mr-am | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4936](https://issues.apache.org/jira/browse/MAPREDUCE-4936) | JobImpl uber checks for cpu are wrong |  Critical | mrv2 | Daryn Sharp | Arun C Murthy |
| [MAPREDUCE-4934](https://issues.apache.org/jira/browse/MAPREDUCE-4934) | Maven RAT plugin is not checking all source files |  Critical | build | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4925](https://issues.apache.org/jira/browse/MAPREDUCE-4925) | The pentomino option parser may be buggy |  Major | examples | Karthik Kambatla | Karthik Kambatla |
| [MAPREDUCE-4924](https://issues.apache.org/jira/browse/MAPREDUCE-4924) | flakey test: org.apache.hadoop.mapred.TestClusterMRNotification.testMR |  Trivial | mrv1 | Robert Kanter | Robert Kanter |
| [MAPREDUCE-4923](https://issues.apache.org/jira/browse/MAPREDUCE-4923) | Add toString method to TaggedInputSplit |  Minor | mrv1, mrv2, task | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-4921](https://issues.apache.org/jira/browse/MAPREDUCE-4921) | JobClient should acquire HS token with RM principal |  Blocker | client | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-4920](https://issues.apache.org/jira/browse/MAPREDUCE-4920) | Use security token protobuf definition from hadoop common |  Major | . | Vinod Kumar Vavilapalli | Suresh Srinivas |
| [MAPREDUCE-4913](https://issues.apache.org/jira/browse/MAPREDUCE-4913) | TestMRAppMaster#testMRAppMasterMissingStaging occasionally exits |  Major | mr-am | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4902](https://issues.apache.org/jira/browse/MAPREDUCE-4902) | Fix typo "receievd" should be "received" in log output |  Trivial | . | Albert Chu | Albert Chu |
| [MAPREDUCE-4895](https://issues.apache.org/jira/browse/MAPREDUCE-4895) | Fix compilation failure of org.apache.hadoop.mapred.gridmix.TestResourceUsageEmulators |  Major | . | Dennis Y | Dennis Y |
| [MAPREDUCE-4894](https://issues.apache.org/jira/browse/MAPREDUCE-4894) | Renewal / cancellation of JobHistory tokens |  Blocker | jobhistoryserver, mrv2 | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-4893](https://issues.apache.org/jira/browse/MAPREDUCE-4893) | MR AppMaster can do sub-optimal assignment of containers to map tasks leading to poor node locality |  Major | applicationmaster | Bikas Saha | Bikas Saha |
| [MAPREDUCE-4890](https://issues.apache.org/jira/browse/MAPREDUCE-4890) | Invalid TaskImpl state transitions when task fails while speculating |  Critical | mr-am | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4884](https://issues.apache.org/jira/browse/MAPREDUCE-4884) | streaming tests fail to start MiniMRCluster due to "Queue configuration missing child queue names for root" |  Major | contrib/streaming, test | Chris Nauroth | Chris Nauroth |
| [MAPREDUCE-4861](https://issues.apache.org/jira/browse/MAPREDUCE-4861) | Cleanup: Remove unused mapreduce.security.token.DelegationTokenRenewal |  Major | . | Karthik Kambatla | Karthik Kambatla |
| [MAPREDUCE-4856](https://issues.apache.org/jira/browse/MAPREDUCE-4856) | TestJobOutputCommitter uses same directory as TestJobCleanup |  Major | test | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-4848](https://issues.apache.org/jira/browse/MAPREDUCE-4848) | TaskAttemptContext cast error during AM recovery |  Major | mr-am | Jason Lowe | Jerry Chen |
| [MAPREDUCE-4842](https://issues.apache.org/jira/browse/MAPREDUCE-4842) | Shuffle race can hang reducer |  Blocker | mrv2 | Jason Lowe | Mariappan Asokan |
| [MAPREDUCE-4836](https://issues.apache.org/jira/browse/MAPREDUCE-4836) | Elapsed time for running tasks on AM web UI tasks page is 0 |  Major | . | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-4833](https://issues.apache.org/jira/browse/MAPREDUCE-4833) | Task can get stuck in FAIL\_CONTAINER\_CLEANUP |  Critical | applicationmaster, mrv2 | Robert Joseph Evans | Robert Parker |
| [MAPREDUCE-4832](https://issues.apache.org/jira/browse/MAPREDUCE-4832) | MR AM can get in a split brain situation |  Critical | applicationmaster | Robert Joseph Evans | Jason Lowe |
| [MAPREDUCE-4825](https://issues.apache.org/jira/browse/MAPREDUCE-4825) | JobImpl.finished doesn't expect ERROR as a final job state |  Major | mr-am | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4819](https://issues.apache.org/jira/browse/MAPREDUCE-4819) | AM can rerun job after reporting final job status to the client |  Blocker | mr-am | Jason Lowe | Bikas Saha |
| [MAPREDUCE-4817](https://issues.apache.org/jira/browse/MAPREDUCE-4817) | Hardcoded task ping timeout kills tasks localizing large amounts of data |  Critical | applicationmaster, mr-am | Jason Lowe | Thomas Graves |
| [MAPREDUCE-4813](https://issues.apache.org/jira/browse/MAPREDUCE-4813) | AM timing out during job commit |  Critical | applicationmaster | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4801](https://issues.apache.org/jira/browse/MAPREDUCE-4801) | ShuffleHandler can generate large logs due to prematurely closed channels |  Critical | . | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4797](https://issues.apache.org/jira/browse/MAPREDUCE-4797) | LocalContainerAllocator can loop forever trying to contact the RM |  Major | applicationmaster | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4787](https://issues.apache.org/jira/browse/MAPREDUCE-4787) | TestJobMonitorAndPrint is broken |  Major | test | Ravi Prakash | Robert Parker |
| [MAPREDUCE-4786](https://issues.apache.org/jira/browse/MAPREDUCE-4786) | Job End Notification retry interval is 5 milliseconds by default |  Major | mrv2 | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-4782](https://issues.apache.org/jira/browse/MAPREDUCE-4782) | NLineInputFormat skips first line of last InputSplit |  Blocker | client | Mark Fuhs | Mark Fuhs |
| [MAPREDUCE-4778](https://issues.apache.org/jira/browse/MAPREDUCE-4778) | Fair scheduler event log is only written if directory exists on HDFS |  Major | jobtracker, scheduler | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-4774](https://issues.apache.org/jira/browse/MAPREDUCE-4774) | JobImpl does not handle asynchronous task events in FAILED state |  Major | applicationmaster, mrv2 | Ivan A. Veselovsky | Jason Lowe |
| [MAPREDUCE-4772](https://issues.apache.org/jira/browse/MAPREDUCE-4772) | Fetch failures can take way too long for a map to be restarted |  Critical | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-4771](https://issues.apache.org/jira/browse/MAPREDUCE-4771) | KeyFieldBasedPartitioner not partitioning properly when configured |  Major | mrv2 | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4751](https://issues.apache.org/jira/browse/MAPREDUCE-4751) | AM stuck in KILL\_WAIT for days |  Major | . | Ravi Prakash | Vinod Kumar Vavilapalli |
| [MAPREDUCE-4748](https://issues.apache.org/jira/browse/MAPREDUCE-4748) | Invalid event: T\_ATTEMPT\_SUCCEEDED at SUCCEEDED |  Blocker | mrv2 | Robert Joseph Evans | Jason Lowe |
| [MAPREDUCE-4746](https://issues.apache.org/jira/browse/MAPREDUCE-4746) | The MR Application Master does not have a config to set environment variables |  Major | applicationmaster | Robert Parker | Robert Parker |
| [MAPREDUCE-4741](https://issues.apache.org/jira/browse/MAPREDUCE-4741) | WARN and ERROR messages logged during normal AM shutdown |  Minor | applicationmaster, mrv2 | Jason Lowe | Vinod Kumar Vavilapalli |
| [MAPREDUCE-4740](https://issues.apache.org/jira/browse/MAPREDUCE-4740) | only .jars can be added to the Distributed Cache classpath |  Blocker | mrv2 | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-4733](https://issues.apache.org/jira/browse/MAPREDUCE-4733) | Reducer can fail to make progress during shuffle if too many reducers complete consecutively |  Major | applicationmaster, mrv2 | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4730](https://issues.apache.org/jira/browse/MAPREDUCE-4730) | AM crashes due to OOM while serving up map task completion events |  Blocker | applicationmaster, mrv2 | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4729](https://issues.apache.org/jira/browse/MAPREDUCE-4729) | job history UI not showing all job attempts |  Major | jobhistoryserver | Thomas Graves | Vinod Kumar Vavilapalli |
| [MAPREDUCE-4724](https://issues.apache.org/jira/browse/MAPREDUCE-4724) | job history web ui applications page should be sorted to display last app first |  Major | jobhistoryserver | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4721](https://issues.apache.org/jira/browse/MAPREDUCE-4721) | Task startup time in JHS is same as job startup time. |  Major | jobhistoryserver | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-4720](https://issues.apache.org/jira/browse/MAPREDUCE-4720) | Browser thinks History Server main page JS is taking too long |  Major | . | Robert Joseph Evans | Ravi Prakash |
| [MAPREDUCE-4712](https://issues.apache.org/jira/browse/MAPREDUCE-4712) | mr-jobhistory-daemon.sh doesn't accept --config |  Major | jobhistoryserver | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-4705](https://issues.apache.org/jira/browse/MAPREDUCE-4705) | Historyserver links expire before the history data does |  Critical | jobhistoryserver, mrv2 | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4681](https://issues.apache.org/jira/browse/MAPREDUCE-4681) | HDFS-3910 broke MR tests |  Major | . | Arun C Murthy | Arun C Murthy |
| [MAPREDUCE-4678](https://issues.apache.org/jira/browse/MAPREDUCE-4678) | Running the Pentomino example with defaults throws java.lang.NegativeArraySizeException |  Minor | examples | Chris McConnell | Chris McConnell |
| [MAPREDUCE-4674](https://issues.apache.org/jira/browse/MAPREDUCE-4674) | Hadoop examples secondarysort has a typo "secondarysrot" in the usage |  Minor | . | Robert Justice | Robert Justice |
| [MAPREDUCE-4654](https://issues.apache.org/jira/browse/MAPREDUCE-4654) | TestDistCp is @ignored |  Critical | test | Colin Patrick McCabe | Sandy Ryza |
| [MAPREDUCE-4637](https://issues.apache.org/jira/browse/MAPREDUCE-4637) | Killing an unassigned task attempt causes the job to fail |  Major | mrv2 | Tom White | Mayank Bansal |
| [MAPREDUCE-4607](https://issues.apache.org/jira/browse/MAPREDUCE-4607) | Race condition in ReduceTask completion can result in Task being incorrectly failed |  Major | . | Bikas Saha | Bikas Saha |
| [MAPREDUCE-4554](https://issues.apache.org/jira/browse/MAPREDUCE-4554) | Job Credentials are not transmitted if security is turned off |  Major | job submission, security | Benoy Antony | Benoy Antony |
| [MAPREDUCE-4521](https://issues.apache.org/jira/browse/MAPREDUCE-4521) | mapreduce.user.classpath.first incompatibility with 0.20/1.x |  Major | mrv2 | Jason Lowe | Ravi Prakash |
| [MAPREDUCE-4479](https://issues.apache.org/jira/browse/MAPREDUCE-4479) | Fix parameter order in assertEquals() in TestCombineInputFileFormat.java |  Major | test | Mariappan Asokan | Mariappan Asokan |
| [MAPREDUCE-4425](https://issues.apache.org/jira/browse/MAPREDUCE-4425) | Speculation + Fetch failures can lead to a hung job |  Critical | mrv2 | Siddharth Seth | Jason Lowe |
| [MAPREDUCE-4279](https://issues.apache.org/jira/browse/MAPREDUCE-4279) | getClusterStatus() fails with null pointer exception when running jobs in local mode |  Major | jobtracker | Rahul Jain | Devaraj K |
| [MAPREDUCE-4278](https://issues.apache.org/jira/browse/MAPREDUCE-4278) | cannot run two local jobs in parallel from the same gateway. |  Major | . | Araceli Henley | Sandy Ryza |
| [MAPREDUCE-4272](https://issues.apache.org/jira/browse/MAPREDUCE-4272) | SortedRanges.Range#compareTo is not spec compliant |  Major | task | Luke Lu | Yu Gao |
| [MAPREDUCE-2264](https://issues.apache.org/jira/browse/MAPREDUCE-2264) | Job status exceeds 100% in some cases |  Major | jobtracker | Adam Kramer | Devaraj K |
| [MAPREDUCE-1806](https://issues.apache.org/jira/browse/MAPREDUCE-1806) | CombineFileInputFormat does not work with paths not on default FS |  Major | harchive | Paul Yang | Gera Shegalov |
| [MAPREDUCE-1700](https://issues.apache.org/jira/browse/MAPREDUCE-1700) | User supplied dependencies may conflict with MapReduce system JARs |  Major | task | Tom White | Tom White |
| [YARN-364](https://issues.apache.org/jira/browse/YARN-364) | AggregatedLogDeletionService can take too long to delete logs |  Major | . | Jason Lowe | Jason Lowe |
| [YARN-360](https://issues.apache.org/jira/browse/YARN-360) | Allow apps to concurrently register tokens for renewal |  Critical | . | Daryn Sharp | Daryn Sharp |
| [YARN-357](https://issues.apache.org/jira/browse/YARN-357) | App submission should not be synchronized |  Major | resourcemanager | Daryn Sharp | Daryn Sharp |
| [YARN-355](https://issues.apache.org/jira/browse/YARN-355) | RM app submission jams under load |  Blocker | resourcemanager | Daryn Sharp | Daryn Sharp |
| [YARN-354](https://issues.apache.org/jira/browse/YARN-354) | WebAppProxyServer exits immediately after startup |  Blocker | . | Liang Xie | Liang Xie |
| [YARN-343](https://issues.apache.org/jira/browse/YARN-343) | Capacity Scheduler maximum-capacity value -1 is invalid |  Major | capacityscheduler | Thomas Graves | Xuan Gong |
| [YARN-336](https://issues.apache.org/jira/browse/YARN-336) | Fair scheduler FIFO scheduling within a queue only allows 1 app at a time |  Major | scheduler | Sandy Ryza | Sandy Ryza |
| [YARN-334](https://issues.apache.org/jira/browse/YARN-334) | Maven RAT plugin is not checking all source files |  Critical | . | Thomas Graves | Thomas Graves |
| [YARN-330](https://issues.apache.org/jira/browse/YARN-330) | Flakey test: TestNodeManagerShutdown#testKillContainersOnShutdown |  Major | nodemanager | Hitesh Shah | Sandy Ryza |
| [YARN-325](https://issues.apache.org/jira/browse/YARN-325) | RM CapacityScheduler can deadlock when getQueueInfo() is called and a container is completing |  Blocker | capacityscheduler | Jason Lowe | Arun C Murthy |
| [YARN-320](https://issues.apache.org/jira/browse/YARN-320) | RM should always be able to renew its own tokens |  Blocker | resourcemanager | Daryn Sharp | Daryn Sharp |
| [YARN-302](https://issues.apache.org/jira/browse/YARN-302) | Fair scheduler assignmultiple should default to false |  Major | resourcemanager, scheduler | Sandy Ryza | Sandy Ryza |
| [YARN-301](https://issues.apache.org/jira/browse/YARN-301) | Fair scheduler throws ConcurrentModificationException when iterating over app's priorities |  Major | resourcemanager, scheduler | Hong Shen | Hong Shen |
| [YARN-300](https://issues.apache.org/jira/browse/YARN-300) | After YARN-271, fair scheduler can infinite loop and not schedule any application. |  Major | resourcemanager, scheduler | Hong Shen | Sandy Ryza |
| [YARN-293](https://issues.apache.org/jira/browse/YARN-293) | Node Manager leaks LocalizerRunner object for every Container |  Critical | nodemanager | Devaraj K | Robert Joseph Evans |
| [YARN-288](https://issues.apache.org/jira/browse/YARN-288) | Fair scheduler queue doesn't accept any jobs when ACLs are configured. |  Major | resourcemanager, scheduler | Sandy Ryza | Sandy Ryza |
| [YARN-283](https://issues.apache.org/jira/browse/YARN-283) | Fair scheduler fails to get queue info without root prefix |  Major | scheduler | Sandy Ryza | Sandy Ryza |
| [YARN-282](https://issues.apache.org/jira/browse/YARN-282) | Fair scheduler web UI double counts Apps Submitted |  Major | . | Sandy Ryza | Sandy Ryza |
| [YARN-278](https://issues.apache.org/jira/browse/YARN-278) | Fair scheduler maxRunningApps config causes no apps to make progress |  Major | resourcemanager, scheduler | Sandy Ryza | Sandy Ryza |
| [YARN-272](https://issues.apache.org/jira/browse/YARN-272) | Fair scheduler log messages try to print objects without overridden toString methods |  Major | scheduler | Sandy Ryza | Sandy Ryza |
| [YARN-271](https://issues.apache.org/jira/browse/YARN-271) | Fair scheduler hits IllegalStateException trying to reserve different apps on same node |  Major | resourcemanager, scheduler | Sandy Ryza | Sandy Ryza |
| [YARN-267](https://issues.apache.org/jira/browse/YARN-267) | Fix fair scheduler web UI |  Major | resourcemanager, scheduler | Sandy Ryza | Sandy Ryza |
| [YARN-266](https://issues.apache.org/jira/browse/YARN-266) | RM and JHS Web UIs are blank because AppsBlock is not escaping string properly |  Critical | resourcemanager | Ravi Prakash | Ravi Prakash |
| [YARN-264](https://issues.apache.org/jira/browse/YARN-264) | y.s.rm.DelegationTokenRenewer attempts to renew token even after removing an app |  Major | . | Karthik Kambatla | Karthik Kambatla |
| [YARN-258](https://issues.apache.org/jira/browse/YARN-258) | RM web page UI shows Invalid Date for start and finish times |  Major | resourcemanager | Ravi Prakash | Ravi Prakash |
| [YARN-253](https://issues.apache.org/jira/browse/YARN-253) | Container launch may fail if no files were localized |  Critical | nodemanager | Tom White | Tom White |
| [YARN-251](https://issues.apache.org/jira/browse/YARN-251) | Proxy URI generation fails for blank tracking URIs |  Major | resourcemanager | Tom White | Tom White |
| [YARN-225](https://issues.apache.org/jira/browse/YARN-225) | Proxy Link in RM UI thows NPE in Secure mode |  Critical | resourcemanager | Devaraj K | Devaraj K |
| [YARN-224](https://issues.apache.org/jira/browse/YARN-224) | Fair scheduler logs too many nodeUpdate INFO messages |  Major | . | Sandy Ryza | Sandy Ryza |
| [YARN-223](https://issues.apache.org/jira/browse/YARN-223) | Change processTree interface to work better with native code |  Critical | . | Radim Kolar | Radim Kolar |
| [YARN-217](https://issues.apache.org/jira/browse/YARN-217) | yarn rmadmin commands fail in secure cluster |  Blocker | resourcemanager | Devaraj K | Devaraj K |
| [YARN-214](https://issues.apache.org/jira/browse/YARN-214) | RMContainerImpl does not handle event EXPIRE at state RUNNING |  Major | resourcemanager | Jason Lowe | Jonathan Eagles |
| [YARN-212](https://issues.apache.org/jira/browse/YARN-212) | NM state machine ignores an APPLICATION\_CONTAINER\_FINISHED event when it shouldn't |  Blocker | nodemanager | Nathan Roberts | Nathan Roberts |
| [YARN-206](https://issues.apache.org/jira/browse/YARN-206) | TestApplicationCleanup.testContainerCleanup occasionally fails |  Major | resourcemanager | Jason Lowe | Jason Lowe |
| [YARN-201](https://issues.apache.org/jira/browse/YARN-201) | CapacityScheduler can take a very long time to schedule containers if requests are off cluster |  Critical | capacityscheduler | Jason Lowe | Jason Lowe |
| [YARN-189](https://issues.apache.org/jira/browse/YARN-189) | deadlock in RM - AMResponse object |  Blocker | resourcemanager | Thomas Graves | Thomas Graves |
| [YARN-181](https://issues.apache.org/jira/browse/YARN-181) | capacity-scheduler.xml move breaks Eclipse import |  Critical | resourcemanager | Siddharth Seth | Siddharth Seth |
| [YARN-180](https://issues.apache.org/jira/browse/YARN-180) | Capacity scheduler - containers that get reserved create container token to early |  Critical | capacityscheduler | Thomas Graves | Arun C Murthy |
| [YARN-179](https://issues.apache.org/jira/browse/YARN-179) | Bunch of test failures on trunk |  Blocker | capacityscheduler | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [YARN-178](https://issues.apache.org/jira/browse/YARN-178) | Fix custom ProcessTree instance creation |  Critical | . | Radim Kolar | Radim Kolar |
| [YARN-177](https://issues.apache.org/jira/browse/YARN-177) | CapacityScheduler - adding a queue while the RM is running has wacky results |  Critical | capacityscheduler | Thomas Graves | Arun C Murthy |
| [YARN-170](https://issues.apache.org/jira/browse/YARN-170) | NodeManager stop() gets called twice on shutdown |  Major | nodemanager | Sandy Ryza | Sandy Ryza |
| [YARN-166](https://issues.apache.org/jira/browse/YARN-166) | capacity scheduler doesn't allow capacity \< 1.0 |  Major | capacityscheduler | Thomas Graves | Thomas Graves |
| [YARN-163](https://issues.apache.org/jira/browse/YARN-163) | Retrieving container log via NM webapp can hang with multibyte characters in log |  Major | nodemanager | Jason Lowe | Jason Lowe |
| [YARN-161](https://issues.apache.org/jira/browse/YARN-161) | Yarn Common has multiple compiler warnings for unchecked operations |  Major | api | Chris Nauroth | Chris Nauroth |
| [YARN-159](https://issues.apache.org/jira/browse/YARN-159) | RM web ui applications page should be sorted to display last app first |  Major | resourcemanager | Thomas Graves | Thomas Graves |
| [YARN-151](https://issues.apache.org/jira/browse/YARN-151) | Browser thinks RM main page JS is taking too long |  Major | . | Robert Joseph Evans | Ravi Prakash |
| [YARN-150](https://issues.apache.org/jira/browse/YARN-150) | AppRejectedTransition does not unregister app from master service and scheduler |  Major | . | Bikas Saha | Bikas Saha |
| [YARN-140](https://issues.apache.org/jira/browse/YARN-140) | Add capacity-scheduler-default.xml to provide a default set of configurations for the capacity scheduler. |  Major | capacityscheduler | Ahmed Radwan | Ahmed Radwan |
| [YARN-139](https://issues.apache.org/jira/browse/YARN-139) | Interrupted Exception within AsyncDispatcher leads to user confusion |  Major | api | Nathan Roberts | Vinod Kumar Vavilapalli |
| [YARN-136](https://issues.apache.org/jira/browse/YARN-136) | Make ClientTokenSecretManager part of RMContext |  Major | resourcemanager | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [YARN-133](https://issues.apache.org/jira/browse/YARN-133) | update web services docs for RM clusterMetrics |  Major | resourcemanager | Thomas Graves | Ravi Prakash |
| [YARN-131](https://issues.apache.org/jira/browse/YARN-131) | Incorrect ACL properties in capacity scheduler documentation |  Major | capacityscheduler | Ahmed Radwan | Ahmed Radwan |
| [YARN-127](https://issues.apache.org/jira/browse/YARN-127) | Move RMAdmin tool to the client package |  Major | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [YARN-116](https://issues.apache.org/jira/browse/YARN-116) | RM is missing ability to add include/exclude files without a restart |  Major | resourcemanager | xieguiming | xieguiming |
| [YARN-102](https://issues.apache.org/jira/browse/YARN-102) | Move the apache licence header to the top of the file in MemStore.java |  Trivial | resourcemanager | Devaraj K | Devaraj K |
| [YARN-94](https://issues.apache.org/jira/browse/YARN-94) | DistributedShell jar should point to Client as the main class by default |  Major | applications/distributed-shell | Vinod Kumar Vavilapalli | Hitesh Shah |
| [YARN-93](https://issues.apache.org/jira/browse/YARN-93) | Diagnostics missing from applications that have finished but failed |  Major | resourcemanager | Jason Lowe | Jason Lowe |
| [YARN-82](https://issues.apache.org/jira/browse/YARN-82) | YARN local-dirs defaults to /tmp/nm-local-dir |  Minor | nodemanager | Andy Isaacson | Hemanth Yamijala |
| [YARN-78](https://issues.apache.org/jira/browse/YARN-78) | Change UnmanagedAMLauncher to use YarnClientImpl |  Major | applications | Bikas Saha | Bikas Saha |
| [YARN-72](https://issues.apache.org/jira/browse/YARN-72) | NM should handle cleaning up containers when it shuts down |  Major | nodemanager | Hitesh Shah | Sandy Ryza |
| [YARN-53](https://issues.apache.org/jira/browse/YARN-53) | Add protocol to YARN to support GetGroups |  Major | resourcemanager | Alejandro Abdelnur | Bo Wang |
| [YARN-43](https://issues.apache.org/jira/browse/YARN-43) | TestResourceTrackerService fail intermittently on jdk7 |  Major | . | Thomas Graves | Thomas Graves |
| [YARN-40](https://issues.apache.org/jira/browse/YARN-40) | Provide support for missing yarn commands |  Major | client | Devaraj K | Devaraj K |
| [YARN-33](https://issues.apache.org/jira/browse/YARN-33) | LocalDirsHandler should validate the configured local and log dirs |  Major | nodemanager | Mayank Bansal | Mayank Bansal |
| [YARN-32](https://issues.apache.org/jira/browse/YARN-32) | TestApplicationTokens fails intermintently on jdk7 |  Major | . | Thomas Graves | Vinod Kumar Vavilapalli |
| [YARN-30](https://issues.apache.org/jira/browse/YARN-30) | TestNMWebServicesApps, TestRMWebServicesApps and TestRMWebServicesNodes fail on jdk7 |  Major | . | Thomas Graves | Thomas Graves |
| [YARN-28](https://issues.apache.org/jira/browse/YARN-28) | TestCompositeService fails on jdk7 |  Major | . | Thomas Graves | Thomas Graves |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-9217](https://issues.apache.org/jira/browse/HADOOP-9217) | Print thread dumps when hadoop-common tests fail |  Major | test | Andrey Klochkov | Andrey Klochkov |
| [HADOOP-9119](https://issues.apache.org/jira/browse/HADOOP-9119) | Add test to FileSystemContractBaseTest to verify integrity of overwritten files |  Minor | fs, test | Steve Loughran | Steve Loughran |
| [HADOOP-9067](https://issues.apache.org/jira/browse/HADOOP-9067) | provide test for method org.apache.hadoop.fs.LocalFileSystem.reportChecksumFailure(Path, FSDataInputStream, long, FSDataInputStream, long) |  Minor | . | Ivan A. Veselovsky | Ivan A. Veselovsky |
| [HADOOP-9042](https://issues.apache.org/jira/browse/HADOOP-9042) | Add a test for umask in FileSystemContractBaseTest |  Minor | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HADOOP-9038](https://issues.apache.org/jira/browse/HADOOP-9038) | provide unit-test coverage of class org.apache.hadoop.fs.LocalDirAllocator.AllocatorPerContext.PathIterator |  Minor | . | Ivan A. Veselovsky | Ivan A. Veselovsky |
| [HDFS-4349](https://issues.apache.org/jira/browse/HDFS-4349) | Test reading files from BackupNode |  Major | namenode, test | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-4199](https://issues.apache.org/jira/browse/HDFS-4199) | Provide test for HdfsVolumeId |  Minor | . | Ivan A. Veselovsky | Ivan A. Veselovsky |
| [HDFS-4007](https://issues.apache.org/jira/browse/HDFS-4007) | Rehabilitate bit-rotted unit tests under hadoop-hdfs-project/hadoop-hdfs/src/test/unit/ |  Minor | test | Colin Patrick McCabe | Colin Patrick McCabe |
| [MAPREDUCE-4905](https://issues.apache.org/jira/browse/MAPREDUCE-4905) | test org.apache.hadoop.mapred.pipes |  Major | . | Aleksey Gorshkov | Aleksey Gorshkov |
| [MAPREDUCE-4803](https://issues.apache.org/jira/browse/MAPREDUCE-4803) | Duplicate copies of TestIndexCache.java |  Minor | test | Mariappan Asokan | Mariappan Asokan |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-9035](https://issues.apache.org/jira/browse/HADOOP-9035) | Generalize setup of LoginContext |  Major | security | Daryn Sharp | Daryn Sharp |
| [HADOOP-9021](https://issues.apache.org/jira/browse/HADOOP-9021) | Enforce configured SASL method on the server |  Major | ipc, security | Daryn Sharp | Daryn Sharp |
| [HADOOP-9020](https://issues.apache.org/jira/browse/HADOOP-9020) | Add a SASL PLAIN server |  Major | ipc, security | Daryn Sharp | Daryn Sharp |
| [HADOOP-9015](https://issues.apache.org/jira/browse/HADOOP-9015) | Standardize creation of SaslRpcServers |  Major | ipc | Daryn Sharp | Daryn Sharp |
| [HADOOP-9014](https://issues.apache.org/jira/browse/HADOOP-9014) | Standardize creation of SaslRpcClients |  Major | ipc | Daryn Sharp | Daryn Sharp |
| [HADOOP-9013](https://issues.apache.org/jira/browse/HADOOP-9013) | UGI should not hardcode loginUser's authenticationType |  Major | fs, security | Daryn Sharp | Daryn Sharp |
| [HADOOP-9012](https://issues.apache.org/jira/browse/HADOOP-9012) | IPC Client sends wrong connection context |  Major | ipc | Daryn Sharp | Daryn Sharp |
| [HADOOP-9010](https://issues.apache.org/jira/browse/HADOOP-9010) | Map UGI authenticationMethod to RPC authMethod |  Major | fs, security | Daryn Sharp | Daryn Sharp |
| [HADOOP-9009](https://issues.apache.org/jira/browse/HADOOP-9009) | Add SecurityUtil methods to get/set authentication method |  Major | fs, security | Daryn Sharp | Daryn Sharp |
| [HADOOP-8784](https://issues.apache.org/jira/browse/HADOOP-8784) | Improve IPC.Client's token use |  Major | ipc, security | Daryn Sharp | Daryn Sharp |
| [HADOOP-8783](https://issues.apache.org/jira/browse/HADOOP-8783) | Improve RPC.Server's digest auth |  Major | ipc, security | Daryn Sharp | Daryn Sharp |
| [HDFS-4445](https://issues.apache.org/jira/browse/HDFS-4445) | All BKJM ledgers are not checked while tailing, So failover will fail. |  Blocker | . | Vinayakumar B | Vinayakumar B |
| [HDFS-4248](https://issues.apache.org/jira/browse/HDFS-4248) | Renames may remove file leases |  Blocker | namenode | Daryn Sharp | Daryn Sharp |
| [HDFS-4247](https://issues.apache.org/jira/browse/HDFS-4247) | saveNamespace should be tolerant of dangling lease |  Blocker | namenode | Daryn Sharp | Daryn Sharp |
| [HDFS-4130](https://issues.apache.org/jira/browse/HDFS-4130) | BKJM: The reading for editlog at NN starting using bkjm  is not efficient |  Major | ha, performance | Han Xiao | Han Xiao |
| [HDFS-4100](https://issues.apache.org/jira/browse/HDFS-4100) | Fix all findbug security warings |  Major | datanode, journal-node, security | Liang Xie | Liang Xie |
| [HDFS-4059](https://issues.apache.org/jira/browse/HDFS-4059) | Add number of stale DataNodes to metrics |  Minor | datanode, namenode | Jing Zhao | Jing Zhao |
| [HDFS-4038](https://issues.apache.org/jira/browse/HDFS-4038) | Override toString() for BookKeeperEditLogInputStream |  Minor | ha | Vinayakumar B | Vinayakumar B |
| [HDFS-4035](https://issues.apache.org/jira/browse/HDFS-4035) | LightWeightGSet and LightWeightHashSet increment a volatile without synchronization |  Major | . | Eli Collins | Eli Collins |
| [HDFS-4034](https://issues.apache.org/jira/browse/HDFS-4034) | Remove redundant null checks |  Major | . | Eli Collins | Eli Collins |
| [HDFS-4033](https://issues.apache.org/jira/browse/HDFS-4033) | Miscellaneous findbugs 2 fixes |  Major | . | Eli Collins | Eli Collins |
| [HDFS-4032](https://issues.apache.org/jira/browse/HDFS-4032) | Specify the charset explicitly rather than rely on the default |  Major | . | Eli Collins | Eli Collins |
| [HDFS-4031](https://issues.apache.org/jira/browse/HDFS-4031) | Update findbugsExcludeFile.xml to include findbugs 2 exclusions |  Major | namenode | Eli Collins | Eli Collins |
| [HDFS-4030](https://issues.apache.org/jira/browse/HDFS-4030) | BlockManager excessBlocksCount and postponedMisreplicatedBlocksCount should be AtomicLongs |  Major | namenode | Eli Collins | Eli Collins |
| [HDFS-4029](https://issues.apache.org/jira/browse/HDFS-4029) | GenerationStamp should use an AtomicLong |  Major | namenode | Eli Collins | Eli Collins |
| [HDFS-3935](https://issues.apache.org/jira/browse/HDFS-3935) | QJM: Add JournalNode to the start / stop scripts |  Major | . | Eli Collins | Andy Isaacson |
| [HDFS-3923](https://issues.apache.org/jira/browse/HDFS-3923) | libwebhdfs testing code cleanup |  Major | . | Jing Zhao | Jing Zhao |
| [HDFS-3920](https://issues.apache.org/jira/browse/HDFS-3920) | libwebdhfs code cleanup: string processing and using strerror consistently to handle all errors |  Major | . | Jing Zhao | Jing Zhao |
| [HDFS-3912](https://issues.apache.org/jira/browse/HDFS-3912) | Detecting and avoiding stale datanodes for writing |  Major | . | Jing Zhao | Jing Zhao |
| [HDFS-3810](https://issues.apache.org/jira/browse/HDFS-3810) | Implement format() for BKJM |  Major | . | Ivan Kelly | Ivan Kelly |
| [HDFS-3809](https://issues.apache.org/jira/browse/HDFS-3809) | Make BKJM use protobufs for all serialization with ZK |  Major | namenode | Ivan Kelly | Ivan Kelly |
| [HDFS-3789](https://issues.apache.org/jira/browse/HDFS-3789) | JournalManager#format() should be able to throw IOException |  Major | ha, namenode | Ivan Kelly | Ivan Kelly |
| [HDFS-3695](https://issues.apache.org/jira/browse/HDFS-3695) | Genericize format() to non-file JournalManagers |  Major | ha, namenode | Todd Lipcon | Todd Lipcon |
| [HDFS-3623](https://issues.apache.org/jira/browse/HDFS-3623) | BKJM: zkLatchWaitTimeout hard coded to 6000. Make use of ZKSessionTimeout instead. |  Major | namenode | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-3573](https://issues.apache.org/jira/browse/HDFS-3573) | Supply NamespaceInfo when instantiating JournalManagers |  Minor | namenode | Todd Lipcon | Todd Lipcon |
| [HDFS-3571](https://issues.apache.org/jira/browse/HDFS-3571) | Allow EditLogFileInputStream to read from a remote URL |  Major | ha, namenode | Todd Lipcon | Todd Lipcon |
| [HDFS-2908](https://issues.apache.org/jira/browse/HDFS-2908) | Add apache license header for StorageReport.java |  Minor | . | Suresh Srinivas | Brandon Li |
| [MAPREDUCE-4809](https://issues.apache.org/jira/browse/MAPREDUCE-4809) | Change visibility of classes for pluggable sort changes |  Major | . | Arun C Murthy | Mariappan Asokan |
| [MAPREDUCE-4807](https://issues.apache.org/jira/browse/MAPREDUCE-4807) | Allow MapOutputBuffer to be pluggable |  Major | . | Arun C Murthy | Mariappan Asokan |
| [MAPREDUCE-4049](https://issues.apache.org/jira/browse/MAPREDUCE-4049) | plugin for generic shuffle service |  Major | performance, task, tasktracker | Avner BenHanoch | Avner BenHanoch |
| [YARN-280](https://issues.apache.org/jira/browse/YARN-280) | RM does not reject app submission with invalid tokens |  Major | resourcemanager | Daryn Sharp | Daryn Sharp |
| [YARN-231](https://issues.apache.org/jira/browse/YARN-231) | Add FS-based persistent store implementation for RMStateStore |  Major | resourcemanager | Bikas Saha | Bikas Saha |
| [YARN-230](https://issues.apache.org/jira/browse/YARN-230) | Make changes for RM restart phase 1 |  Major | resourcemanager | Bikas Saha | Bikas Saha |
| [YARN-229](https://issues.apache.org/jira/browse/YARN-229) | Remove old code for restart |  Major | resourcemanager | Bikas Saha | Bikas Saha |
| [YARN-219](https://issues.apache.org/jira/browse/YARN-219) | NM should aggregate logs when application finishes. |  Critical | nodemanager | Robert Joseph Evans | Robert Joseph Evans |
| [YARN-204](https://issues.apache.org/jira/browse/YARN-204) | test coverage for org.apache.hadoop.tools |  Major | applications | Aleksey Gorshkov | Aleksey Gorshkov |
| [YARN-202](https://issues.apache.org/jira/browse/YARN-202) | Log Aggregation generates a storm of fsync() for namenode |  Critical | . | Kihwal Lee | Kihwal Lee |
| [YARN-188](https://issues.apache.org/jira/browse/YARN-188) | Coverage fixing for CapacityScheduler |  Major | capacityscheduler | Aleksey Gorshkov | Aleksey Gorshkov |
| [YARN-186](https://issues.apache.org/jira/browse/YARN-186) | Coverage fixing LinuxContainerExecutor |  Major | resourcemanager, scheduler | Aleksey Gorshkov | Aleksey Gorshkov |
| [YARN-135](https://issues.apache.org/jira/browse/YARN-135) | ClientTokens should be per app-attempt and be unregistered on App-finish. |  Major | resourcemanager | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [YARN-134](https://issues.apache.org/jira/browse/YARN-134) | ClientToAMSecretManager creates keys without checking for validity of the appID |  Major | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [YARN-103](https://issues.apache.org/jira/browse/YARN-103) | Add a yarn AM - RM client module |  Major | . | Bikas Saha | Bikas Saha |
| [YARN-50](https://issues.apache.org/jira/browse/YARN-50) | Implement renewal / cancellation of Delegation Tokens |  Blocker | . | Siddharth Seth | Siddharth Seth |
| [YARN-3](https://issues.apache.org/jira/browse/YARN-3) | Add support for CPU isolation/monitoring of containers |  Major | . | Arun C Murthy | Andrew Ferguson |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-8860](https://issues.apache.org/jira/browse/HADOOP-8860) | Split MapReduce and YARN sections in documentation navigation |  Major | documentation | Tom White | Tom White |
| [HADOOP-8427](https://issues.apache.org/jira/browse/HADOOP-8427) | Convert Forrest docs to APT, incremental |  Major | documentation | Eli Collins | Andy Isaacson |
| [HDFS-4326](https://issues.apache.org/jira/browse/HDFS-4326) | bump up Tomcat version for HttpFS to 6.0.36 |  Major | . | Alejandro Abdelnur | Alejandro Abdelnur |
| [HDFS-3944](https://issues.apache.org/jira/browse/HDFS-3944) | Httpfs resolveAuthority() is not resolving host correctly |  Major | . | Alejandro Abdelnur | Alejandro Abdelnur |
| [MAPREDUCE-4596](https://issues.apache.org/jira/browse/MAPREDUCE-4596) | Split StateMachine state from states seen by MRClientProtocol (for Job, Task, TaskAttempt) |  Major | applicationmaster, mrv2 | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-4266](https://issues.apache.org/jira/browse/MAPREDUCE-4266) | remove Ant remnants from MR |  Major | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [YARN-372](https://issues.apache.org/jira/browse/YARN-372) | Move InlineDispatcher from hadoop-yarn-server-resourcemanager to hadoop-yarn-common |  Minor | . | Siddharth Seth | Siddharth Seth |


