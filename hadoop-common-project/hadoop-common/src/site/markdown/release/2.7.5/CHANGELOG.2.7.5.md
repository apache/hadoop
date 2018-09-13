
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

## Release 2.7.5 - 2017-12-14



### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-6959](https://issues.apache.org/jira/browse/YARN-6959) | RM may allocate wrong AM Container for new attempt |  Major | capacity scheduler, fairscheduler, scheduler | Yuqi Wang | Yuqi Wang |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-8829](https://issues.apache.org/jira/browse/HDFS-8829) | Make SO\_RCVBUF and SO\_SNDBUF size configurable for DataTransferProtocol sockets and allow configuring auto-tuning |  Major | datanode | He Tianyi | He Tianyi |
| [HADOOP-13442](https://issues.apache.org/jira/browse/HADOOP-13442) | Optimize UGI group lookups |  Major | . | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-6937](https://issues.apache.org/jira/browse/MAPREDUCE-6937) | Backport MAPREDUCE-6870 to branch-2 while preserving compatibility |  Major | . | Zhe Zhang | Peter Bacsko |
| [HADOOP-14827](https://issues.apache.org/jira/browse/HADOOP-14827) | Allow StopWatch to accept a Timer parameter for tests |  Minor | common, test | Erik Krogen | Erik Krogen |
| [HDFS-12131](https://issues.apache.org/jira/browse/HDFS-12131) | Add some of the FSNamesystem JMX values as metrics |  Minor | hdfs, namenode | Erik Krogen | Erik Krogen |
| [HDFS-8865](https://issues.apache.org/jira/browse/HDFS-8865) | Improve quota initialization performance |  Major | . | Kihwal Lee | Kihwal Lee |
| [HDFS-12420](https://issues.apache.org/jira/browse/HDFS-12420) | Add an option to disallow 'namenode format -force' |  Major | . | Ajay Kumar | Ajay Kumar |
| [MAPREDUCE-6975](https://issues.apache.org/jira/browse/MAPREDUCE-6975) | Logging task counters |  Major | task | Prabhu Joseph | Prabhu Joseph |
| [HDFS-12823](https://issues.apache.org/jira/browse/HDFS-12823) | Backport HDFS-9259 "Make SO\_SNDBUF size configurable at DFSClient" to branch-2.7 |  Major | hdfs, hdfs-client | Erik Krogen | Erik Krogen |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-6165](https://issues.apache.org/jira/browse/MAPREDUCE-6165) | [JDK8] TestCombineFileInputFormat failed on JDK8 |  Minor | . | Wei Yan | Akira Ajisaka |
| [HDFS-8797](https://issues.apache.org/jira/browse/HDFS-8797) | WebHdfsFileSystem creates too many connections for pread |  Major | webhdfs | Jing Zhao | Jing Zhao |
| [HDFS-9003](https://issues.apache.org/jira/browse/HDFS-9003) | ForkJoin thread pool leaks |  Major | . | Kihwal Lee | Kihwal Lee |
| [HDFS-9107](https://issues.apache.org/jira/browse/HDFS-9107) | Prevent NN's unrecoverable death spiral after full GC |  Critical | namenode | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-6750](https://issues.apache.org/jira/browse/MAPREDUCE-6750) | TestHSAdminServer.testRefreshSuperUserGroups is failing |  Minor | test | Kihwal Lee | Kihwal Lee |
| [HDFS-10738](https://issues.apache.org/jira/browse/HDFS-10738) | Fix TestRefreshUserMappings.testRefreshSuperUserGroupsConfiguration test failure |  Major | test | Rakesh R | Rakesh R |
| [HADOOP-14702](https://issues.apache.org/jira/browse/HADOOP-14702) | Fix formatting issue and regression caused by conversion from APT to Markdown |  Minor | documentation | Doris Gu | Doris Gu |
| [HDFS-12157](https://issues.apache.org/jira/browse/HDFS-12157) | Do fsyncDirectory(..) outside of FSDataset lock |  Critical | datanode | Vinayakumar B | Vinayakumar B |
| [MAPREDUCE-6931](https://issues.apache.org/jira/browse/MAPREDUCE-6931) | Remove TestDFSIO "Total Throughput" calculation |  Critical | benchmarks, test | Dennis Huo | Dennis Huo |
| [HADOOP-14867](https://issues.apache.org/jira/browse/HADOOP-14867) | Update HDFS Federation setup document, for incorrect property name for secondary name node http address |  Major | . | Bharat Viswanadham | Bharat Viswanadham |
| [MAPREDUCE-6957](https://issues.apache.org/jira/browse/MAPREDUCE-6957) | shuffle hangs after a node manager connection timeout |  Major | mrv2 | Jooseong Kim | Jooseong Kim |
| [HDFS-12323](https://issues.apache.org/jira/browse/HDFS-12323) | NameNode terminates after full GC thinking QJM unresponsive if full GC is much longer than timeout |  Major | namenode, qjm | Erik Krogen | Erik Krogen |
| [YARN-5195](https://issues.apache.org/jira/browse/YARN-5195) | RM intermittently crashed with NPE while handling APP\_ATTEMPT\_REMOVED event when async-scheduling enabled in CapacityScheduler |  Major | resourcemanager | Karam Singh | sandflee |
| [HADOOP-14902](https://issues.apache.org/jira/browse/HADOOP-14902) | LoadGenerator#genFile write close timing is incorrectly calculated |  Major | fs | Jason Lowe | Hanisha Koneru |
| [YARN-7084](https://issues.apache.org/jira/browse/YARN-7084) | TestSchedulingMonitor#testRMStarts fails sporadically |  Major | . | Jason Lowe | Jason Lowe |
| [HDFS-12578](https://issues.apache.org/jira/browse/HDFS-12578) | TestDeadDatanode#testNonDFSUsedONDeadNodeReReg failing in branch-2.7 |  Blocker | test | Xiao Chen | Ajay Kumar |
| [HADOOP-14919](https://issues.apache.org/jira/browse/HADOOP-14919) | BZip2 drops records when reading data in splits |  Critical | . | Aki Tanaka | Jason Lowe |
| [HDFS-12832](https://issues.apache.org/jira/browse/HDFS-12832) | INode.getFullPathName may throw ArrayIndexOutOfBoundsException lead to NameNode exit |  Critical | namenode | DENG FEI | Konstantin Shvachko |
| [HDFS-12638](https://issues.apache.org/jira/browse/HDFS-12638) | Delete copy-on-truncate block along with the original block, when deleting a file being truncated |  Blocker | hdfs | Jiandan Yang | Konstantin Shvachko |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-9153](https://issues.apache.org/jira/browse/HDFS-9153) | Pretty-format the output for DFSIO |  Major | . | Kai Zheng | Kai Zheng |
| [HDFS-12596](https://issues.apache.org/jira/browse/HDFS-12596) | Add TestFsck#testFsckCorruptWhenOneReplicaIsCorrupt back to branch-2.7 |  Major | test | Xiao Chen | Xiao Chen |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-10711](https://issues.apache.org/jira/browse/HDFS-10711) | Optimize FSPermissionChecker group membership check |  Major | hdfs | Daryn Sharp | Daryn Sharp |
| [HADOOP-14881](https://issues.apache.org/jira/browse/HADOOP-14881) | LoadGenerator should use Time.monotonicNow() to measure durations |  Major | . | Chetna Chaudhari | Bharat Viswanadham |
| [YARN-5402](https://issues.apache.org/jira/browse/YARN-5402) | Fix NoSuchMethodError in ClusterMetricsInfo |  Major | webapp | Weiwei Yang | Weiwei Yang |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-10984](https://issues.apache.org/jira/browse/HDFS-10984) | Expose nntop output as metrics |  Major | namenode | Siddharth Wagle | Siddharth Wagle |
