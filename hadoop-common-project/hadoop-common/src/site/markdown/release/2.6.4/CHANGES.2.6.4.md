
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

## Release 2.6.4 - 2016-02-11

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-3154](https://issues.apache.org/jira/browse/YARN-3154) | Should not upload partial logs for MR jobs or other "short-running' applications |  Blocker | nodemanager, resourcemanager | Xuan Gong | Xuan Gong |
| [HADOOP-11252](https://issues.apache.org/jira/browse/HADOOP-11252) | RPC client does not time out by default |  Critical | ipc | Wilfred Spiegelenburg | Masatake Iwasaki |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-7694](https://issues.apache.org/jira/browse/HDFS-7694) | FSDataInputStream should support "unbuffer" |  Major | . | Colin P. McCabe | Colin P. McCabe |
| [HDFS-8722](https://issues.apache.org/jira/browse/HDFS-8722) | Optimize datanode writes for small writes and flushes |  Critical | . | Kihwal Lee | Kihwal Lee |
| [HDFS-8647](https://issues.apache.org/jira/browse/HDFS-8647) | Abstract BlockManager's rack policy into BlockPlacementPolicy |  Major | . | Ming Ma | Brahma Reddy Battula |
| [HDFS-9314](https://issues.apache.org/jira/browse/HDFS-9314) | Improve BlockPlacementPolicyDefault's picking of excess replicas |  Major | . | Ming Ma | Xiao Chen |
| [MAPREDUCE-6436](https://issues.apache.org/jira/browse/MAPREDUCE-6436) | JobHistory cache issue |  Blocker | . | Ryu Kobayashi | Kai Sasaki |
| [HDFS-9415](https://issues.apache.org/jira/browse/HDFS-9415) | Document dfs.cluster.administrators and dfs.permissions.superusergroup |  Major | documentation | Arpit Agarwal | Xiaobing Zhou |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-2975](https://issues.apache.org/jira/browse/YARN-2975) | FSLeafQueue app lists are accessed without required locks |  Blocker | . | Karthik Kambatla | Karthik Kambatla |
| [HDFS-6945](https://issues.apache.org/jira/browse/HDFS-6945) | BlockManager should remove a block from excessReplicateMap and decrement ExcessBlocks metric when the block is removed |  Critical | namenode | Akira Ajisaka | Akira Ajisaka |
| [HDFS-4660](https://issues.apache.org/jira/browse/HDFS-4660) | Block corruption can happen during pipeline recovery |  Blocker | datanode | Peng Zhang | Kihwal Lee |
| [YARN-3842](https://issues.apache.org/jira/browse/YARN-3842) | NMProxy should retry on NMNotYetReadyException |  Critical | . | Karthik Kambatla | Robert Kanter |
| [YARN-3695](https://issues.apache.org/jira/browse/YARN-3695) | ServerProxy (NMProxy, etc.) shouldn't retry forever for non network exception. |  Major | . | Junping Du | Raju Bairishetti |
| [HADOOP-12107](https://issues.apache.org/jira/browse/HADOOP-12107) | long running apps may have a huge number of StatisticsData instances under FileSystem |  Critical | fs | Sangjin Lee | Sangjin Lee |
| [YARN-3849](https://issues.apache.org/jira/browse/YARN-3849) | Too much of preemption activity causing continuos killing of containers across queues |  Critical | capacityscheduler | Sunil G | Sunil G |
| [HDFS-8767](https://issues.apache.org/jira/browse/HDFS-8767) | RawLocalFileSystem.listStatus() returns null for UNIX pipefile |  Critical | . | Haohui Mai | Kanaka Kumar Avvaru |
| [YARN-3535](https://issues.apache.org/jira/browse/YARN-3535) | Scheduler must re-request container resources when RMContainer transitions from ALLOCATED to KILLED |  Critical | capacityscheduler, fairscheduler, resourcemanager | Peng Zhang | Peng Zhang |
| [YARN-3857](https://issues.apache.org/jira/browse/YARN-3857) | Memory leak in ResourceManager with SIMPLE mode |  Critical | resourcemanager | mujunchao | mujunchao |
| [MAPREDUCE-5982](https://issues.apache.org/jira/browse/MAPREDUCE-5982) | Task attempts that fail from the ASSIGNED state can disappear |  Major | mr-am | Jason Lowe | Chang Li |
| [YARN-3697](https://issues.apache.org/jira/browse/YARN-3697) | FairScheduler: ContinuousSchedulingThread can fail to shutdown |  Critical | fairscheduler | zhihai xu | zhihai xu |
| [MAPREDUCE-6492](https://issues.apache.org/jira/browse/MAPREDUCE-6492) | AsyncDispatcher exit with NPE on TaskAttemptImpl#sendJHStartEventForAssignedFailTask |  Critical | . | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-4180](https://issues.apache.org/jira/browse/YARN-4180) | AMLauncher does not retry on failures when talking to NM |  Critical | resourcemanager | Anubhav Dhoot | Anubhav Dhoot |
| [HDFS-9178](https://issues.apache.org/jira/browse/HDFS-9178) | Slow datanode I/O can cause a wrong node to be marked bad |  Critical | . | Kihwal Lee | Kihwal Lee |
| [HDFS-9220](https://issues.apache.org/jira/browse/HDFS-9220) | Reading small file (\< 512 bytes) that is open for append fails due to incorrect checksum |  Blocker | . | Bogdan Raducanu | Jing Zhao |
| [HDFS-9313](https://issues.apache.org/jira/browse/HDFS-9313) | Possible NullPointerException in BlockManager if no excess replica can be chosen |  Major | . | Ming Ma | Ming Ma |
| [YARN-4354](https://issues.apache.org/jira/browse/YARN-4354) | Public resource localization fails with NPE |  Blocker | nodemanager | Jason Lowe | Jason Lowe |
| [YARN-4380](https://issues.apache.org/jira/browse/YARN-4380) | TestResourceLocalizationService.testDownloadingResourcesOnContainerKill fails intermittently |  Major | test | Tsuyoshi Ozawa | Varun Saxena |
| [HDFS-9294](https://issues.apache.org/jira/browse/HDFS-9294) | DFSClient  deadlock when close file and failed to renew lease |  Blocker | hdfs-client | DENG FEI | Brahma Reddy Battula |
| [YARN-4452](https://issues.apache.org/jira/browse/YARN-4452) | NPE when submit Unmanaged application |  Critical | . | Naganarasimha G R | Naganarasimha G R |
| [HDFS-9445](https://issues.apache.org/jira/browse/HDFS-9445) | Datanode may deadlock while handling a bad volume |  Blocker | . | Kihwal Lee | Walter Su |
| [MAPREDUCE-6577](https://issues.apache.org/jira/browse/MAPREDUCE-6577) | MR AM unable to load native library without MR\_AM\_ADMIN\_USER\_ENV set |  Critical | mr-am | Sangjin Lee | Sangjin Lee |
| [YARN-4546](https://issues.apache.org/jira/browse/YARN-4546) | ResourceManager crash due to scheduling opportunity overflow |  Critical | resourcemanager | Jason Lowe | Jason Lowe |
| [HDFS-9600](https://issues.apache.org/jira/browse/HDFS-9600) | do not check replication if the block is under construction |  Critical | . | Phil Yang | Phil Yang |
| [HDFS-9574](https://issues.apache.org/jira/browse/HDFS-9574) | Reduce client failures during datanode restart |  Major | . | Kihwal Lee | Kihwal Lee |
| [YARN-4414](https://issues.apache.org/jira/browse/YARN-4414) | Nodemanager connection errors are retried at multiple levels |  Major | nodemanager | Jason Lowe | Chang Li |
| [HADOOP-12706](https://issues.apache.org/jira/browse/HADOOP-12706) | TestLocalFsFCStatistics#testStatisticsThreadLocalDataCleanUp times out occasionally |  Major | test | Jason Lowe | Sangjin Lee |
| [YARN-4581](https://issues.apache.org/jira/browse/YARN-4581) | AHS writer thread leak makes RM crash while RM is recovering |  Major | resourcemanager | sandflee | sandflee |
| [MAPREDUCE-6554](https://issues.apache.org/jira/browse/MAPREDUCE-6554) | MRAppMaster servicestart failing  with NPE in MRAppMaster#parsePreviousJobHistory |  Critical | . | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-4598](https://issues.apache.org/jira/browse/YARN-4598) | Invalid event: RESOURCE\_FAILED at CONTAINER\_CLEANEDUP\_AFTER\_KILL |  Major | nodemanager | tangshangwen | tangshangwen |
| [MAPREDUCE-6619](https://issues.apache.org/jira/browse/MAPREDUCE-6619) | HADOOP\_CLASSPATH is overwritten in MR container |  Major | mrv2 | shanyu zhao | Junping Du |
| [MAPREDUCE-6618](https://issues.apache.org/jira/browse/MAPREDUCE-6618) | YarnClientProtocolProvider leaking the YarnClient thread. |  Major | . | Xuan Gong | Xuan Gong |
| [MAPREDUCE-6621](https://issues.apache.org/jira/browse/MAPREDUCE-6621) | Memory Leak in JobClient#submitJobInternal() |  Major | . | Xuan Gong | Xuan Gong |
| [MAPREDUCE-6363](https://issues.apache.org/jira/browse/MAPREDUCE-6363) | [NNBench] Lease mismatch error when running with multiple mappers |  Critical | benchmarks | Brahma Reddy Battula | Bibin A Chundatt |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-12736](https://issues.apache.org/jira/browse/HADOOP-12736) | TestTimedOutTestsListener#testThreadDumpAndDeadlocks sometimes times out |  Major | . | Xiao Chen | Xiao Chen |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-3893](https://issues.apache.org/jira/browse/YARN-3893) | Both RM in active state when Admin#transitionToActive failure from refeshAll() |  Critical | resourcemanager | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-2902](https://issues.apache.org/jira/browse/YARN-2902) | Killing a container that is localizing can orphan resources in the DOWNLOADING state |  Major | nodemanager | Jason Lowe | Varun Saxena |


