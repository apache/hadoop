
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

## Release 2.4.1 - 2014-06-30

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-10466](https://issues.apache.org/jira/browse/HADOOP-10466) | Lower the log level in UserGroupInformation |  Minor | security | Nicolas Liochon | Nicolas Liochon |
| [HDFS-4052](https://issues.apache.org/jira/browse/HDFS-4052) | BlockManager#invalidateWork should print logs outside the lock |  Minor | . | Jing Zhao | Jing Zhao |
| [YARN-1892](https://issues.apache.org/jira/browse/YARN-1892) | Excessive logging in RM |  Minor | scheduler | Siddharth Seth | Jian He |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-11273](https://issues.apache.org/jira/browse/HADOOP-11273) | TestMiniKdc failure: login options not compatible with IBM JDK |  Major | test | Gao Zhong Liang | Gao Zhong Liang |
| [HADOOP-10612](https://issues.apache.org/jira/browse/HADOOP-10612) | NFS failed to refresh the user group id mapping table |  Major | nfs | Brandon Li | Brandon Li |
| [HADOOP-10562](https://issues.apache.org/jira/browse/HADOOP-10562) | Namenode exits on exception without printing stack trace in AbstractDelegationTokenSecretManager |  Critical | . | Suresh Srinivas | Suresh Srinivas |
| [HADOOP-10527](https://issues.apache.org/jira/browse/HADOOP-10527) | Fix incorrect return code and allow more retries on EINTR |  Major | . | Kihwal Lee | Kihwal Lee |
| [HADOOP-10522](https://issues.apache.org/jira/browse/HADOOP-10522) | JniBasedUnixGroupMapping mishandles errors |  Critical | . | Kihwal Lee | Kihwal Lee |
| [HADOOP-10490](https://issues.apache.org/jira/browse/HADOOP-10490) | TestMapFile and TestBloomMapFile leak file descriptors. |  Minor | test | Chris Nauroth | Chris Nauroth |
| [HADOOP-10473](https://issues.apache.org/jira/browse/HADOOP-10473) | TestCallQueueManager is still flaky |  Minor | test | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-10456](https://issues.apache.org/jira/browse/HADOOP-10456) | Bug in Configuration.java exposed by Spark (ConcurrentModificationException) |  Major | conf | Nishkam Ravi | Nishkam Ravi |
| [HADOOP-10455](https://issues.apache.org/jira/browse/HADOOP-10455) | When there is an exception, ipc.Server should first check whether it is an terse exception |  Major | ipc | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-8826](https://issues.apache.org/jira/browse/HADOOP-8826) | Docs still refer to 0.20.205 as stable line |  Minor | . | Robert Joseph Evans | Mit Desai |
| [HDFS-6411](https://issues.apache.org/jira/browse/HDFS-6411) | nfs-hdfs-gateway mount raises I/O error and hangs when a unauthorized user attempts to access it |  Major | nfs | Zhongyi Xie | Brandon Li |
| [HDFS-6402](https://issues.apache.org/jira/browse/HDFS-6402) | Suppress findbugs warning for failure to override equals and hashCode in FsAclPermission. |  Trivial | namenode | Chris Nauroth | Chris Nauroth |
| [HDFS-6397](https://issues.apache.org/jira/browse/HDFS-6397) | NN shows inconsistent value in deadnode count |  Critical | . | Mohammad Kamrul Islam | Mohammad Kamrul Islam |
| [HDFS-6362](https://issues.apache.org/jira/browse/HDFS-6362) | InvalidateBlocks is inconsistent in usage of DatanodeUuid and StorageID |  Blocker | namenode | Arpit Agarwal | Arpit Agarwal |
| [HDFS-6361](https://issues.apache.org/jira/browse/HDFS-6361) | TestIdUserGroup.testUserUpdateSetting failed due to out of range nfsnobody Id |  Major | nfs | Yongjun Zhang | Yongjun Zhang |
| [HDFS-6340](https://issues.apache.org/jira/browse/HDFS-6340) | DN can't finalize upgrade |  Blocker | datanode | Rahul Singhal | Rahul Singhal |
| [HDFS-6329](https://issues.apache.org/jira/browse/HDFS-6329) | WebHdfs does not work if HA is enabled on NN but logical URI is not configured. |  Blocker | . | Kihwal Lee | Kihwal Lee |
| [HDFS-6326](https://issues.apache.org/jira/browse/HDFS-6326) | WebHdfs ACL compatibility is broken |  Blocker | webhdfs | Daryn Sharp | Chris Nauroth |
| [HDFS-6325](https://issues.apache.org/jira/browse/HDFS-6325) | Append should fail if the last block has insufficient number of replicas |  Major | namenode | Konstantin Shvachko | Keith Pak |
| [HDFS-6313](https://issues.apache.org/jira/browse/HDFS-6313) | WebHdfs may use the wrong NN when configured for multiple HA NNs |  Blocker | webhdfs | Daryn Sharp | Kihwal Lee |
| [HDFS-6245](https://issues.apache.org/jira/browse/HDFS-6245) | datanode fails to start with a bad disk even when failed volumes is set |  Major | . | Arpit Gupta | Arpit Agarwal |
| [HDFS-6236](https://issues.apache.org/jira/browse/HDFS-6236) | ImageServlet should use Time#monotonicNow to measure latency. |  Minor | namenode | Chris Nauroth | Chris Nauroth |
| [HDFS-6235](https://issues.apache.org/jira/browse/HDFS-6235) | TestFileJournalManager can fail on Windows due to file locking if tests run out of order. |  Trivial | namenode, test | Chris Nauroth | Chris Nauroth |
| [HDFS-6234](https://issues.apache.org/jira/browse/HDFS-6234) | TestDatanodeConfig#testMemlockLimit fails on Windows due to invalid file path. |  Trivial | datanode, test | Chris Nauroth | Chris Nauroth |
| [HDFS-6232](https://issues.apache.org/jira/browse/HDFS-6232) | OfflineEditsViewer throws a NPE on edits containing ACL modifications |  Major | tools | Stephen Chu | Akira AJISAKA |
| [HDFS-6231](https://issues.apache.org/jira/browse/HDFS-6231) | DFSClient hangs infinitely if using hedged reads and all eligible datanodes die. |  Major | hdfs-client | Chris Nauroth | Chris Nauroth |
| [HDFS-6229](https://issues.apache.org/jira/browse/HDFS-6229) | Race condition in failover can cause RetryCache fail to work |  Major | ha | Jing Zhao | Jing Zhao |
| [HDFS-6215](https://issues.apache.org/jira/browse/HDFS-6215) | Wrong error message for upgrade |  Minor | . | Kihwal Lee | Kihwal Lee |
| [HDFS-6209](https://issues.apache.org/jira/browse/HDFS-6209) | Fix flaky test TestValidateConfigurationSettings.testThatDifferentRPCandHttpPortsAreOK |  Minor | test | Arpit Agarwal | Arpit Agarwal |
| [HDFS-6208](https://issues.apache.org/jira/browse/HDFS-6208) | DataNode caching can leak file descriptors. |  Major | datanode | Chris Nauroth | Chris Nauroth |
| [HDFS-6206](https://issues.apache.org/jira/browse/HDFS-6206) | DFSUtil.substituteForWildcardAddress may throw NPE |  Major | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-6204](https://issues.apache.org/jira/browse/HDFS-6204) | TestRBWBlockInvalidation may fail |  Minor | test | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-6198](https://issues.apache.org/jira/browse/HDFS-6198) | DataNode rolling upgrade does not correctly identify current block pool directory and replace with trash on Windows. |  Major | datanode | Chris Nauroth | Chris Nauroth |
| [HDFS-6197](https://issues.apache.org/jira/browse/HDFS-6197) | Rolling upgrade rollback on Windows can fail attempting to rename edit log segment files to a destination that already exists. |  Minor | namenode | Chris Nauroth | Chris Nauroth |
| [HDFS-2882](https://issues.apache.org/jira/browse/HDFS-2882) | DN continues to start up, even if block pool fails to initialize |  Major | datanode | Todd Lipcon | Vinayakumar B |
| [MAPREDUCE-5841](https://issues.apache.org/jira/browse/MAPREDUCE-5841) | uber job doesn't terminate on getting mapred job kill |  Major | mrv2 | Sangjin Lee | Sangjin Lee |
| [MAPREDUCE-5835](https://issues.apache.org/jira/browse/MAPREDUCE-5835) | Killing Task might cause the job to go to ERROR state |  Critical | . | Ming Ma | Ming Ma |
| [MAPREDUCE-5832](https://issues.apache.org/jira/browse/MAPREDUCE-5832) | Few tests in TestJobClient fail on Windows |  Major | . | Jian He | Vinod Kumar Vavilapalli |
| [MAPREDUCE-5830](https://issues.apache.org/jira/browse/MAPREDUCE-5830) | HostUtil.getTaskLogUrl is not backwards binary compatible with 2.3 |  Blocker | . | Jason Lowe | Akira AJISAKA |
| [MAPREDUCE-5828](https://issues.apache.org/jira/browse/MAPREDUCE-5828) | TestMapReduceJobControl fails on JDK 7 + Windows |  Major | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-5827](https://issues.apache.org/jira/browse/MAPREDUCE-5827) | TestSpeculativeExecutionWithMRApp fails |  Major | . | Zhijie Shen | Zhijie Shen |
| [MAPREDUCE-5826](https://issues.apache.org/jira/browse/MAPREDUCE-5826) | TestHistoryServerFileSystemStateStoreService.testTokenStore fails in windows |  Major | . | Varun Vasudev | Varun Vasudev |
| [MAPREDUCE-5824](https://issues.apache.org/jira/browse/MAPREDUCE-5824) | TestPipesNonJavaInputFormat.testFormat fails in windows |  Major | . | Xuan Gong | Xuan Gong |
| [MAPREDUCE-5821](https://issues.apache.org/jira/browse/MAPREDUCE-5821) | IFile merge allocates new byte array for every value |  Major | performance, task | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-5818](https://issues.apache.org/jira/browse/MAPREDUCE-5818) | hsadmin cmd is missing in mapred.cmd |  Major | . | Jian He | Jian He |
| [MAPREDUCE-5815](https://issues.apache.org/jira/browse/MAPREDUCE-5815) | Fix NPE in TestMRAppMaster |  Blocker | client, mrv2 | Gera Shegalov | Akira AJISAKA |
| [MAPREDUCE-5714](https://issues.apache.org/jira/browse/MAPREDUCE-5714) | TestMRAppComponentDependencies causes surefire to exit without saying proper goodbye |  Major | test | Jinghui Wang | Jinghui Wang |
| [MAPREDUCE-3191](https://issues.apache.org/jira/browse/MAPREDUCE-3191) | docs for map output compression incorrectly reference SequenceFile |  Trivial | . | Todd Lipcon | Chen He |
| [YARN-2081](https://issues.apache.org/jira/browse/YARN-2081) | TestDistributedShell fails after YARN-1962 |  Minor | applications/distributed-shell | Hong Zhiguo | Hong Zhiguo |
| [YARN-2066](https://issues.apache.org/jira/browse/YARN-2066) | Wrong field is referenced in GetApplicationsRequestPBImpl#mergeLocalToBuilder() |  Minor | . | Ted Yu | Hong Zhiguo |
| [YARN-2016](https://issues.apache.org/jira/browse/YARN-2016) | Yarn getApplicationRequest start time range is not honored |  Major | resourcemanager | Venkat Ranganathan | Junping Du |
| [YARN-1986](https://issues.apache.org/jira/browse/YARN-1986) | In Fifo Scheduler, node heartbeat in between creating app and attempt causes NPE |  Critical | . | Jon Bringhurst | Hong Zhiguo |
| [YARN-1976](https://issues.apache.org/jira/browse/YARN-1976) | Tracking url missing http protocol for FAILED application |  Major | . | Yesha Vora | Junping Du |
| [YARN-1975](https://issues.apache.org/jira/browse/YARN-1975) | Used resources shows escaped html in CapacityScheduler and FairScheduler page |  Major | resourcemanager | Nathan Roberts | Mit Desai |
| [YARN-1934](https://issues.apache.org/jira/browse/YARN-1934) | Potential NPE in ZKRMStateStore caused by handling Disconnected event from ZK. |  Blocker | resourcemanager | Rohith Sharma K S | Karthik Kambatla |
| [YARN-1933](https://issues.apache.org/jira/browse/YARN-1933) | TestAMRestart and TestNodeHealthService failing sometimes on Windows |  Major | . | Jian He | Jian He |
| [YARN-1932](https://issues.apache.org/jira/browse/YARN-1932) | Javascript injection on the job status page |  Blocker | . | Mit Desai | Mit Desai |
| [YARN-1931](https://issues.apache.org/jira/browse/YARN-1931) | Private API change in YARN-1824 in 2.4 broke compatibility with previous releases |  Blocker | applications | Thomas Graves | Sandy Ryza |
| [YARN-1929](https://issues.apache.org/jira/browse/YARN-1929) | DeadLock in RM when automatic failover is enabled. |  Blocker | resourcemanager | Rohith Sharma K S | Karthik Kambatla |
| [YARN-1928](https://issues.apache.org/jira/browse/YARN-1928) | TestAMRMRPCNodeUpdates fails ocassionally |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-1926](https://issues.apache.org/jira/browse/YARN-1926) | DistributedShell unit tests fail on Windows |  Major | . | Varun Vasudev | Varun Vasudev |
| [YARN-1924](https://issues.apache.org/jira/browse/YARN-1924) | STATE\_STORE\_OP\_FAILED happens when ZKRMStateStore tries to update app(attempt) before storing it |  Critical | . | Arpit Gupta | Jian He |
| [YARN-1920](https://issues.apache.org/jira/browse/YARN-1920) | TestFileSystemApplicationHistoryStore.testMissingApplicationAttemptHistoryData fails in windows |  Major | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [YARN-1914](https://issues.apache.org/jira/browse/YARN-1914) | Test TestFSDownload.testDownloadPublicWithStatCache fails on Windows |  Major | . | Varun Vasudev | Varun Vasudev |
| [YARN-1910](https://issues.apache.org/jira/browse/YARN-1910) | TestAMRMTokens fails on windows |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-1908](https://issues.apache.org/jira/browse/YARN-1908) | Distributed shell with custom script has permission error. |  Major | applications/distributed-shell | Tassapol Athiapinya | Vinod Kumar Vavilapalli |
| [YARN-1907](https://issues.apache.org/jira/browse/YARN-1907) | TestRMApplicationHistoryWriter#testRMWritingMassiveHistory runs slow and intermittently fails |  Major | . | Mit Desai | Mit Desai |
| [YARN-1903](https://issues.apache.org/jira/browse/YARN-1903) | Killing Container on NEW and LOCALIZING will result in exitCode and diagnostics not set |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-1883](https://issues.apache.org/jira/browse/YARN-1883) | TestRMAdminService fails due to inconsistent entries in UserGroups |  Major | . | Mit Desai | Mit Desai |
| [YARN-1837](https://issues.apache.org/jira/browse/YARN-1837) | TestMoveApplication.testMoveRejectedByScheduler randomly fails |  Major | . | Tsuyoshi Ozawa | Hong Zhiguo |
| [YARN-1201](https://issues.apache.org/jira/browse/YARN-1201) | TestAMAuthorization fails with local hostname cannot be resolved |  Minor | resourcemanager | Nemon Lou | Wangda Tan |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-6189](https://issues.apache.org/jira/browse/HDFS-6189) | Multiple HDFS tests fail on Windows attempting to use a test root path containing a colon. |  Major | test | Chris Nauroth | Chris Nauroth |
| [MAPREDUCE-5843](https://issues.apache.org/jira/browse/MAPREDUCE-5843) | TestMRKeyValueTextInputFormat failing on Windows |  Major | . | Varun Vasudev | Varun Vasudev |
| [MAPREDUCE-5833](https://issues.apache.org/jira/browse/MAPREDUCE-5833) | TestRMContainerAllocator fails ocassionally |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-1947](https://issues.apache.org/jira/browse/YARN-1947) | TestRMDelegationTokens#testRMDTMasterKeyStateOnRollingMasterKey is failing intermittently |  Major | . | Jian He | Jian He |
| [YARN-1905](https://issues.apache.org/jira/browse/YARN-1905) | TestProcfsBasedProcessTree must only run on Linux. |  Trivial | nodemanager | Chris Nauroth | Chris Nauroth |
| [YARN-1750](https://issues.apache.org/jira/browse/YARN-1750) | TestNodeStatusUpdater#testNMRegistration is incorrect in test case |  Major | nodemanager | Ming Ma | Wangda Tan |
| [YARN-1281](https://issues.apache.org/jira/browse/YARN-1281) | TestZKRMStateStoreZKClientConnections fails intermittently |  Major | resourcemanager | Karthik Kambatla | Tsuyoshi Ozawa |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-2053](https://issues.apache.org/jira/browse/YARN-2053) | Slider AM fails to restart: NPE in RegisterApplicationMasterResponseProto$Builder.addAllNmTokensFromPreviousAttempts |  Major | resourcemanager | Sumit Mohanty | Wangda Tan |
| [YARN-1962](https://issues.apache.org/jira/browse/YARN-1962) | Timeline server is enabled by default |  Major | . | Mohammad Kamrul Islam | Mohammad Kamrul Islam |
| [YARN-1957](https://issues.apache.org/jira/browse/YARN-1957) | ProportionalCapacitPreemptionPolicy handling of corner cases... |  Major | resourcemanager | Carlo Curino | Carlo Curino |
| [YARN-1898](https://issues.apache.org/jira/browse/YARN-1898) | Standby RM's conf, stacks, logLevel, metrics, jmx and logs links are redirecting to Active RM |  Major | resourcemanager | Yesha Vora | Xuan Gong |
| [YARN-1861](https://issues.apache.org/jira/browse/YARN-1861) | Both RM stuck in standby mode when automatic failover is enabled |  Blocker | resourcemanager | Arpit Gupta | Karthik Kambatla |
| [YARN-1701](https://issues.apache.org/jira/browse/YARN-1701) | Improve default paths of timeline store and generic history store |  Major | . | Gera Shegalov | Tsuyoshi Ozawa |
| [YARN-1696](https://issues.apache.org/jira/browse/YARN-1696) | Document RM HA |  Blocker | resourcemanager | Karthik Kambatla | Tsuyoshi Ozawa |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


