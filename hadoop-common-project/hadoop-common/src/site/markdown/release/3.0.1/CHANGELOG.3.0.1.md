
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

## Release 3.0.1 - 2018-03-25

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-12990](https://issues.apache.org/jira/browse/HDFS-12990) | Change default NameNode RPC port back to 8020 |  Critical | namenode | Xiao Chen | Xiao Chen |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-13083](https://issues.apache.org/jira/browse/HDFS-13083) | RBF: Fix doc error setting up client |  Major | federation | tartarus | tartarus |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-14872](https://issues.apache.org/jira/browse/HADOOP-14872) | CryptoInputStream should implement unbuffer |  Major | fs, security | John Zhuge | John Zhuge |
| [YARN-7414](https://issues.apache.org/jira/browse/YARN-7414) | FairScheduler#getAppWeight() should be moved into FSAppAttempt#getWeight() |  Minor | fairscheduler | Daniel Templeton | Soumabrata Chakraborty |
| [HADOOP-15023](https://issues.apache.org/jira/browse/HADOOP-15023) | ValueQueue should also validate (lowWatermark \* numValues) \> 0 on construction |  Minor | . | Xiao Chen | Xiao Chen |
| [HDFS-12814](https://issues.apache.org/jira/browse/HDFS-12814) | Add blockId when warning slow mirror/disk in BlockReceiver |  Trivial | hdfs | Jiandan Yang | Jiandan Yang |
| [YARN-7524](https://issues.apache.org/jira/browse/YARN-7524) | Remove unused FairSchedulerEventLog |  Major | fairscheduler | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [YARN-7495](https://issues.apache.org/jira/browse/YARN-7495) | Improve robustness of the AggregatedLogDeletionService |  Major | log-aggregation | Jonathan Eagles | Jonathan Eagles |
| [YARN-7611](https://issues.apache.org/jira/browse/YARN-7611) | Node manager web UI should display container type in containers page |  Major | nodemanager, webapp | Weiwei Yang | Weiwei Yang |
| [YARN-6483](https://issues.apache.org/jira/browse/YARN-6483) | Add nodes transitioning to DECOMMISSIONING state to the list of updated nodes returned to the AM |  Major | resourcemanager | Juan Rodríguez Hortalá | Juan Rodríguez Hortalá |
| [HADOOP-15056](https://issues.apache.org/jira/browse/HADOOP-15056) | Fix TestUnbuffer#testUnbufferException failure |  Minor | test | Jack Bearden | Jack Bearden |
| [HADOOP-15012](https://issues.apache.org/jira/browse/HADOOP-15012) | Add readahead, dropbehind, and unbuffer to StreamCapabilities |  Major | fs | John Zhuge | John Zhuge |
| [HADOOP-15104](https://issues.apache.org/jira/browse/HADOOP-15104) | AliyunOSS: change the default value of max error retry |  Major | fs/oss | wujinhu | wujinhu |
| [HDFS-12910](https://issues.apache.org/jira/browse/HDFS-12910) | Secure Datanode Starter should log the port when it fails to bind |  Minor | datanode | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-12819](https://issues.apache.org/jira/browse/HDFS-12819) | Setting/Unsetting EC policy shows warning if the directory is not empty |  Minor | erasure-coding | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HDFS-12927](https://issues.apache.org/jira/browse/HDFS-12927) | Update erasure coding doc to address unsupported APIs |  Major | erasure-coding | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [YARN-5418](https://issues.apache.org/jira/browse/YARN-5418) | When partial log aggregation is enabled, display the list of aggregated files on the container log page |  Major | . | Siddharth Seth | Xuan Gong |
| [HDFS-12818](https://issues.apache.org/jira/browse/HDFS-12818) | Support multiple storages in DataNodeCluster / SimulatedFSDataset |  Minor | datanode, test | Erik Krogen | Erik Krogen |
| [HDFS-9023](https://issues.apache.org/jira/browse/HDFS-9023) | When NN is not able to identify DN for replication, reason behind it can be logged |  Critical | hdfs-client, namenode | Surendra Singh Lilhore | Xiao Chen |
| [HDFS-11847](https://issues.apache.org/jira/browse/HDFS-11847) | Enhance dfsadmin listOpenFiles command to list files blocking datanode decommissioning |  Major | hdfs | Manoj Govindassamy | Manoj Govindassamy |
| [YARN-7678](https://issues.apache.org/jira/browse/YARN-7678) | Ability to enable logging of container memory stats |  Major | nodemanager | Jim Brennan | Jim Brennan |
| [HDFS-11848](https://issues.apache.org/jira/browse/HDFS-11848) | Enhance dfsadmin listOpenFiles command to list files under a given path |  Major | . | Manoj Govindassamy | Yiqun Lin |
| [HDFS-12945](https://issues.apache.org/jira/browse/HDFS-12945) | Switch to ClientProtocol instead of NamenodeProtocols in NamenodeWebHdfsMethods |  Minor | . | Wei Yan | Wei Yan |
| [YARN-7590](https://issues.apache.org/jira/browse/YARN-7590) | Improve container-executor validation check |  Major | security, yarn | Eric Yang | Eric Yang |
| [MAPREDUCE-6984](https://issues.apache.org/jira/browse/MAPREDUCE-6984) | MR AM to clean up temporary files from previous attempt in case of no recovery |  Major | applicationmaster | Gergo Repas | Gergo Repas |
| [HADOOP-15185](https://issues.apache.org/jira/browse/HADOOP-15185) | Update adls connector to use the current version of ADLS SDK |  Major | fs/adl | Atul Sikaria | Atul Sikaria |
| [HADOOP-15189](https://issues.apache.org/jira/browse/HADOOP-15189) | backport HADOOP-15039 to branch-2 and branch-3 |  Blocker | . | Genmao Yu | Genmao Yu |
| [HADOOP-15186](https://issues.apache.org/jira/browse/HADOOP-15186) | Allow Azure Data Lake SDK dependency version to be set on the command line |  Major | build, fs/adl | Vishwajeet Dusane | Vishwajeet Dusane |
| [HDFS-13092](https://issues.apache.org/jira/browse/HDFS-13092) | Reduce verbosity for ThrottledAsyncChecker.java:schedule |  Minor | datanode | Mukul Kumar Singh | Mukul Kumar Singh |
| [HADOOP-15212](https://issues.apache.org/jira/browse/HADOOP-15212) | Add independent secret manager method for logging expired tokens |  Major | security | Daryn Sharp | Daryn Sharp |
| [YARN-7728](https://issues.apache.org/jira/browse/YARN-7728) | Expose container preemptions related information in Capacity Scheduler queue metrics |  Major | . | Eric Payne | Eric Payne |
| [MAPREDUCE-7048](https://issues.apache.org/jira/browse/MAPREDUCE-7048) | Uber AM can crash due to unknown task in statusUpdate |  Major | mr-am | Peter Bacsko | Peter Bacsko |
| [HADOOP-15204](https://issues.apache.org/jira/browse/HADOOP-15204) | Add Configuration API for parsing storage sizes |  Minor | conf | Anu Engineer | Anu Engineer |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-7361](https://issues.apache.org/jira/browse/YARN-7361) | Improve the docker container runtime documentation |  Major | . | Shane Kumpf | Shane Kumpf |
| [YARN-7489](https://issues.apache.org/jira/browse/YARN-7489) | ConcurrentModificationException in RMAppImpl#getRMAppMetrics |  Major | capacityscheduler | Tao Yang | Tao Yang |
| [YARN-7525](https://issues.apache.org/jira/browse/YARN-7525) | Incorrect query parameters in cluster nodes REST API document |  Minor | documentation | Tao Yang | Tao Yang |
| [HADOOP-15046](https://issues.apache.org/jira/browse/HADOOP-15046) | Document Apache Hadoop does not support Java 9 in BUILDING.txt |  Major | documentation | Akira Ajisaka | Hanisha Koneru |
| [YARN-7531](https://issues.apache.org/jira/browse/YARN-7531) | ResourceRequest.equal does not check ExecutionTypeRequest.enforceExecutionType() |  Major | api | Haibo Chen | Haibo Chen |
| [YARN-7513](https://issues.apache.org/jira/browse/YARN-7513) | Remove scheduler lock in FSAppAttempt.getWeight() |  Minor | fairscheduler | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [YARN-7390](https://issues.apache.org/jira/browse/YARN-7390) | All reservation related test cases failed when TestYarnClient runs against Fair Scheduler. |  Major | fairscheduler, reservation system | Yufei Gu | Yufei Gu |
| [YARN-7363](https://issues.apache.org/jira/browse/YARN-7363) | ContainerLocalizer doesn't have a valid log4j config when using LinuxContainerExecutor |  Major | nodemanager | Yufei Gu | Yufei Gu |
| [HDFS-12832](https://issues.apache.org/jira/browse/HDFS-12832) | INode.getFullPathName may throw ArrayIndexOutOfBoundsException lead to NameNode exit |  Critical | namenode | DENG FEI | Konstantin Shvachko |
| [HADOOP-15042](https://issues.apache.org/jira/browse/HADOOP-15042) | Azure PageBlobInputStream.skip() can return negative value when numberOfPagesRemaining is 0 |  Minor | fs/azure | Rajesh Balamohan | Rajesh Balamohan |
| [YARN-7558](https://issues.apache.org/jira/browse/YARN-7558) | "yarn logs" command fails to get logs for running containers if UI authentication is enabled. |  Critical | . | Namit Maheshwari | Xuan Gong |
| [HDFS-12638](https://issues.apache.org/jira/browse/HDFS-12638) | Delete copy-on-truncate block along with the original block, when deleting a file being truncated |  Blocker | hdfs | Jiandan Yang | Konstantin Shvachko |
| [HDFS-12836](https://issues.apache.org/jira/browse/HDFS-12836) | startTxId could be greater than endTxId when tailing in-progress edit log |  Major | hdfs | Chao Sun | Chao Sun |
| [MAPREDUCE-5124](https://issues.apache.org/jira/browse/MAPREDUCE-5124) | AM lacks flow control for task events |  Major | mr-am | Jason Lowe | Peter Bacsko |
| [YARN-7589](https://issues.apache.org/jira/browse/YARN-7589) | TestPBImplRecords fails with NullPointerException |  Major | . | Jason Lowe | Daniel Templeton |
| [YARN-7455](https://issues.apache.org/jira/browse/YARN-7455) | quote\_and\_append\_arg can overflow buffer |  Major | nodemanager | Jason Lowe | Jim Brennan |
| [HADOOP-15058](https://issues.apache.org/jira/browse/HADOOP-15058) | create-release site build outputs dummy shaded jars due to skipShade |  Blocker | . | Andrew Wang | Andrew Wang |
| [HADOOP-14985](https://issues.apache.org/jira/browse/HADOOP-14985) | Remove subversion related code from VersionInfoMojo.java |  Minor | build | Akira Ajisaka | Ajay Kumar |
| [HADOOP-15098](https://issues.apache.org/jira/browse/HADOOP-15098) | TestClusterTopology#testChooseRandom fails intermittently |  Major | test | Zsolt Venczel | Zsolt Venczel |
| [HDFS-12891](https://issues.apache.org/jira/browse/HDFS-12891) | Do not invalidate blocks if toInvalidate is empty |  Major | . | Zsolt Venczel | Zsolt Venczel |
| [YARN-7647](https://issues.apache.org/jira/browse/YARN-7647) | NM print inappropriate error log when node-labels is enabled |  Minor | . | Yang Wang | Yang Wang |
| [HDFS-12907](https://issues.apache.org/jira/browse/HDFS-12907) | Allow read-only access to reserved raw for non-superusers |  Major | namenode | Daryn Sharp | Rushabh S Shah |
| [HDFS-12881](https://issues.apache.org/jira/browse/HDFS-12881) | Output streams closed with IOUtils suppressing write errors |  Major | . | Jason Lowe | Ajay Kumar |
| [YARN-7595](https://issues.apache.org/jira/browse/YARN-7595) | Container launching code suppresses close exceptions after writes |  Major | nodemanager | Jason Lowe | Jim Brennan |
| [HADOOP-15085](https://issues.apache.org/jira/browse/HADOOP-15085) | Output streams closed with IOUtils suppressing write errors |  Major | . | Jason Lowe | Jim Brennan |
| [YARN-7629](https://issues.apache.org/jira/browse/YARN-7629) | TestContainerLaunch# fails after YARN-7381 |  Major | . | Jason Lowe | Jason Lowe |
| [YARN-7664](https://issues.apache.org/jira/browse/YARN-7664) | Several javadoc errors |  Blocker | . | Sean Mackrory | Sean Mackrory |
| [HADOOP-15123](https://issues.apache.org/jira/browse/HADOOP-15123) | KDiag tries to load krb5.conf from KRB5CCNAME instead of KRB5\_CONFIG |  Minor | security | Vipin Rathor | Vipin Rathor |
| [YARN-7661](https://issues.apache.org/jira/browse/YARN-7661) | NodeManager metrics return wrong value after update node resource |  Major | . | Yang Wang | Yang Wang |
| [HDFS-12347](https://issues.apache.org/jira/browse/HDFS-12347) | TestBalancerRPCDelay#testBalancerRPCDelay fails very frequently |  Critical | test | Xiao Chen | Bharat Viswanadham |
| [HDFS-12930](https://issues.apache.org/jira/browse/HDFS-12930) | Remove the extra space in HdfsImageViewer.md |  Trivial | documentation | Yiqun Lin | Rahul Pathak |
| [YARN-7662](https://issues.apache.org/jira/browse/YARN-7662) | [Atsv2] Define new set of configurations for reader and collectors to bind. |  Major | . | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-12845](https://issues.apache.org/jira/browse/HDFS-12845) | JournalNode Test failures |  Major | journal-node | Bharat Viswanadham | Bharat Viswanadham |
| [YARN-7466](https://issues.apache.org/jira/browse/YARN-7466) | ResourceRequest has a different default for allocationRequestId than Container |  Major | . | Chandni Singh | Chandni Singh |
| [YARN-7674](https://issues.apache.org/jira/browse/YARN-7674) | Update Timeline Reader web app address in UI2 |  Major | . | Rohith Sharma K S | Sunil Govindan |
| [HDFS-12938](https://issues.apache.org/jira/browse/HDFS-12938) | TestErasureCodigCLI testAll failing consistently. |  Major | erasure-coding, hdfs | Rushabh S Shah | Ajay Kumar |
| [YARN-7542](https://issues.apache.org/jira/browse/YARN-7542) | Fix issue that causes some Running Opportunistic Containers to be recovered as PAUSED |  Major | . | Arun Suresh | Sampada Dehankar |
| [HDFS-12915](https://issues.apache.org/jira/browse/HDFS-12915) | Fix findbugs warning in INodeFile$HeaderFormat.getBlockLayoutRedundancy |  Major | namenode | Wei-Chiu Chuang | Chris Douglas |
| [HADOOP-15122](https://issues.apache.org/jira/browse/HADOOP-15122) | Lock down version of doxia-module-markdown plugin |  Blocker | . | Elek, Marton | Elek, Marton |
| [HADOOP-15143](https://issues.apache.org/jira/browse/HADOOP-15143) | NPE due to Invalid KerberosTicket in UGI |  Major | . | Jitendra Nath Pandey | Mukul Kumar Singh |
| [YARN-7692](https://issues.apache.org/jira/browse/YARN-7692) | Skip validating priority acls while recovering applications |  Blocker | resourcemanager | Charan Hebri | Sunil Govindan |
| [MAPREDUCE-7028](https://issues.apache.org/jira/browse/MAPREDUCE-7028) | Concurrent task progress updates causing NPE in Application Master |  Blocker | mr-am | Gergo Repas | Gergo Repas |
| [YARN-7602](https://issues.apache.org/jira/browse/YARN-7602) | NM should reference the singleton JvmMetrics instance |  Major | nodemanager | Haibo Chen | Haibo Chen |
| [HDFS-12913](https://issues.apache.org/jira/browse/HDFS-12913) | TestDNFencingWithReplication.testFencingStress fix mini cluster not yet active issue |  Major | . | Zsolt Venczel | Zsolt Venczel |
| [HDFS-12860](https://issues.apache.org/jira/browse/HDFS-12860) | StripedBlockUtil#getRangesInternalBlocks throws exception for the block group size larger than 2GB |  Major | erasure-coding | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [YARN-7619](https://issues.apache.org/jira/browse/YARN-7619) | Max AM Resource value in Capacity Scheduler UI has to be refreshed for every user |  Major | capacity scheduler, yarn | Eric Payne | Eric Payne |
| [YARN-7699](https://issues.apache.org/jira/browse/YARN-7699) | queueUsagePercentage is coming as INF for getApp REST api call |  Major | webapp | Sunil Govindan | Sunil Govindan |
| [HDFS-12985](https://issues.apache.org/jira/browse/HDFS-12985) | NameNode crashes during restart after an OpenForWrite file present in the Snapshot got deleted |  Major | hdfs | Manoj Govindassamy | Manoj Govindassamy |
| [YARN-7508](https://issues.apache.org/jira/browse/YARN-7508) | NPE in FiCaSchedulerApp when debug log enabled in async-scheduling mode |  Major | capacityscheduler | Tao Yang | Tao Yang |
| [YARN-7663](https://issues.apache.org/jira/browse/YARN-7663) | RMAppImpl:Invalid event: START at KILLED |  Major | resourcemanager | lujie | lujie |
| [YARN-6948](https://issues.apache.org/jira/browse/YARN-6948) | Invalid event: ATTEMPT\_ADDED at FINAL\_SAVING |  Major | yarn | lujie | lujie |
| [HDFS-12994](https://issues.apache.org/jira/browse/HDFS-12994) | TestReconstructStripedFile.testNNSendsErasureCodingTasks fails due to socket timeout |  Major | erasure-coding | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [YARN-7665](https://issues.apache.org/jira/browse/YARN-7665) | Allow FS scheduler state dump to be turned on/off separately from FS debug log |  Major | . | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [HADOOP-15060](https://issues.apache.org/jira/browse/HADOOP-15060) | TestShellBasedUnixGroupsMapping.testFiniteGroupResolutionTime flaky |  Major | . | Miklos Szegedi | Miklos Szegedi |
| [YARN-7735](https://issues.apache.org/jira/browse/YARN-7735) | Fix typo in YARN documentation |  Minor | documentation | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-7727](https://issues.apache.org/jira/browse/YARN-7727) | Incorrect log levels in few logs with QueuePriorityContainerCandidateSelector |  Minor | yarn | Prabhu Joseph | Prabhu Joseph |
| [HDFS-11915](https://issues.apache.org/jira/browse/HDFS-11915) | Sync rbw dir on the first hsync() to avoid file lost on power failure |  Critical | . | Kanaka Kumar Avvaru | Vinayakumar B |
| [YARN-7705](https://issues.apache.org/jira/browse/YARN-7705) | Create the container log directory with correct sticky bit in C code |  Major | nodemanager | Yufei Gu | Yufei Gu |
| [YARN-7479](https://issues.apache.org/jira/browse/YARN-7479) | TestContainerManagerSecurity.testContainerManager[Simple] flaky in trunk |  Major | test | Botong Huang | Akira Ajisaka |
| [HDFS-13004](https://issues.apache.org/jira/browse/HDFS-13004) | TestLeaseRecoveryStriped#testLeaseRecovery is failing when safeLength is 0MB or larger than the test file |  Major | hdfs | Zsolt Venczel | Zsolt Venczel |
| [HDFS-9049](https://issues.apache.org/jira/browse/HDFS-9049) | Make Datanode Netty reverse proxy port to be configurable |  Major | datanode | Vinayakumar B | Vinayakumar B |
| [YARN-7758](https://issues.apache.org/jira/browse/YARN-7758) | Add an additional check to the validity of container and application ids passed to container-executor |  Major | nodemanager | Miklos Szegedi | Yufei Gu |
| [HADOOP-15150](https://issues.apache.org/jira/browse/HADOOP-15150) | in FsShell, UGI params should be overidden through env vars(-D arg) |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-15166](https://issues.apache.org/jira/browse/HADOOP-15166) | CLI MiniCluster fails with ClassNotFoundException o.a.h.yarn.server.timelineservice.collector.TimelineCollectorManager |  Major | . | Gera Shegalov | Gera Shegalov |
| [HDFS-13039](https://issues.apache.org/jira/browse/HDFS-13039) | StripedBlockReader#createBlockReader leaks socket on IOException |  Critical | datanode, erasure-coding | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HADOOP-15181](https://issues.apache.org/jira/browse/HADOOP-15181) | Typo in SecureMode.md |  Trivial | documentation | Masahiro Tanaka | Masahiro Tanaka |
| [YARN-7796](https://issues.apache.org/jira/browse/YARN-7796) | Container-executor fails with segfault on certain OS configurations |  Major | nodemanager | Gergo Repas | Gergo Repas |
| [YARN-7806](https://issues.apache.org/jira/browse/YARN-7806) | Distributed Shell should use timeline async api's |  Major | distributed-shell | Sumana Sathish | Rohith Sharma K S |
| [HADOOP-15121](https://issues.apache.org/jira/browse/HADOOP-15121) | Encounter NullPointerException when using DecayRpcScheduler |  Major | . | Tao Jie | Tao Jie |
| [MAPREDUCE-7015](https://issues.apache.org/jira/browse/MAPREDUCE-7015) | Possible race condition in JHS if the job is not loaded |  Major | jobhistoryserver | Peter Bacsko | Peter Bacsko |
| [YARN-7737](https://issues.apache.org/jira/browse/YARN-7737) | prelaunch.err file not found exception on container failure |  Major | . | Jonathan Hung | Keqiu Hu |
| [HDFS-13063](https://issues.apache.org/jira/browse/HDFS-13063) | Fix the incorrect spelling in HDFSHighAvailabilityWithQJM.md |  Trivial | documentation | Jianfei Jiang | Jianfei Jiang |
| [YARN-7102](https://issues.apache.org/jira/browse/YARN-7102) | NM heartbeat stuck when responseId overflows MAX\_INT |  Critical | . | Botong Huang | Botong Huang |
| [MAPREDUCE-7041](https://issues.apache.org/jira/browse/MAPREDUCE-7041) | MR should not try to clean up at first job attempt |  Major | . | Takanobu Asanuma | Gergo Repas |
| [HDFS-13054](https://issues.apache.org/jira/browse/HDFS-13054) | Handling PathIsNotEmptyDirectoryException in DFSClient delete call |  Major | hdfs-client | Nanda kumar | Nanda kumar |
| [MAPREDUCE-7020](https://issues.apache.org/jira/browse/MAPREDUCE-7020) | Task timeout in uber mode can crash AM |  Major | mr-am | Akira Ajisaka | Peter Bacsko |
| [YARN-7765](https://issues.apache.org/jira/browse/YARN-7765) | [Atsv2] GSSException: No valid credentials provided - Failed to find any Kerberos tgt thrown by Timelinev2Client & HBaseClient in NM |  Blocker | . | Sumana Sathish | Rohith Sharma K S |
| [HDFS-13065](https://issues.apache.org/jira/browse/HDFS-13065) | TestErasureCodingMultipleRacks#testSkewedRack3 is failing |  Major | hdfs | Gabor Bota | Gabor Bota |
| [HDFS-12974](https://issues.apache.org/jira/browse/HDFS-12974) | Exception message is not printed when creating an encryption zone fails with AuthorizationException |  Minor | encryption | fang zhenyi | fang zhenyi |
| [YARN-7698](https://issues.apache.org/jira/browse/YARN-7698) | A misleading variable's name in ApplicationAttemptEventDispatcher |  Minor | resourcemanager | Jinjiang Ling | Jinjiang Ling |
| [YARN-7790](https://issues.apache.org/jira/browse/YARN-7790) | Improve Capacity Scheduler Async Scheduling to better handle node failures |  Critical | . | Sumana Sathish | Wangda Tan |
| [HDFS-12528](https://issues.apache.org/jira/browse/HDFS-12528) | Add an option to not disable short-circuit reads on failures |  Major | hdfs-client, performance | Andre Araujo | Xiao Chen |
| [HDFS-12897](https://issues.apache.org/jira/browse/HDFS-12897) | getErasureCodingPolicy should handle .snapshot dir better |  Major | erasure-coding, hdfs, snapshots | Harshakiran Reddy | LiXin Ge |
| [MAPREDUCE-7033](https://issues.apache.org/jira/browse/MAPREDUCE-7033) | Map outputs implicitly rely on permissive umask for shuffle |  Critical | mrv2 | Jason Lowe | Jason Lowe |
| [HDFS-12942](https://issues.apache.org/jira/browse/HDFS-12942) | Synchronization issue in FSDataSetImpl#moveBlock |  Major | . | Ajay Kumar | Ajay Kumar |
| [HDFS-13100](https://issues.apache.org/jira/browse/HDFS-13100) | Handle IllegalArgumentException when GETSERVERDEFAULTS is not implemented in webhdfs. |  Critical | hdfs, webhdfs | Yongjun Zhang | Yongjun Zhang |
| [YARN-7849](https://issues.apache.org/jira/browse/YARN-7849) | TestMiniYarnClusterNodeUtilization#testUpdateNodeUtilization fails due to heartbeat sync error |  Major | test | Jason Lowe | Botong Huang |
| [YARN-7801](https://issues.apache.org/jira/browse/YARN-7801) | AmFilterInitializer should addFilter after fill all parameters |  Critical | . | Sumana Sathish | Wangda Tan |
| [YARN-7890](https://issues.apache.org/jira/browse/YARN-7890) | NPE during container relaunch |  Major | . | Billie Rinaldi | Jason Lowe |
| [HDFS-12935](https://issues.apache.org/jira/browse/HDFS-12935) | Get ambiguous result for DFSAdmin command in HA mode when only one namenode is up |  Major | tools | Jianfei Jiang | Jianfei Jiang |
| [HDFS-13120](https://issues.apache.org/jira/browse/HDFS-13120) | Snapshot diff could be corrupted after concat |  Major | namenode, snapshots | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-8693](https://issues.apache.org/jira/browse/HDFS-8693) | refreshNamenodes does not support adding a new standby to a running DN |  Critical | datanode, ha | Jian Fang | Ajith S |
| [MAPREDUCE-7052](https://issues.apache.org/jira/browse/MAPREDUCE-7052) | TestFixedLengthInputFormat#testFormatCompressedIn is flaky |  Major | client, test | Peter Bacsko | Peter Bacsko |
| [HDFS-13112](https://issues.apache.org/jira/browse/HDFS-13112) | Token expiration edits may cause log corruption or deadlock |  Critical | namenode | Daryn Sharp | Daryn Sharp |
| [YARN-7873](https://issues.apache.org/jira/browse/YARN-7873) | Revert YARN-6078 |  Blocker | . | Billie Rinaldi | Billie Rinaldi |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-7011](https://issues.apache.org/jira/browse/MAPREDUCE-7011) | TestClientDistributedCacheManager::testDetermineCacheVisibilities assumes all parent dirs set other exec |  Trivial | . | Chris Douglas | Chris Douglas |
| [HADOOP-14696](https://issues.apache.org/jira/browse/HADOOP-14696) | parallel tests don't work for Windows |  Minor | test | Allen Wittenauer | Allen Wittenauer |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-14993](https://issues.apache.org/jira/browse/HADOOP-14993) | AliyunOSS: Override listFiles and listLocatedStatus |  Major | fs/oss | Genmao Yu | Genmao Yu |
| [YARN-6953](https://issues.apache.org/jira/browse/YARN-6953) | Clean up ResourceUtils.setMinimumAllocationForMandatoryResources() and setMaximumAllocationForMandatoryResources() |  Minor | resourcemanager | Daniel Templeton | Manikandan R |
| [HDFS-12801](https://issues.apache.org/jira/browse/HDFS-12801) | RBF: Set MountTableResolver as default file resolver |  Minor | . | Íñigo Goiri | Íñigo Goiri |
| [HADOOP-15024](https://issues.apache.org/jira/browse/HADOOP-15024) | AliyunOSS: support user agent configuration and include that & Hadoop version information to oss server |  Major | fs, fs/oss | Sammi Chen | Sammi Chen |
| [HDFS-12858](https://issues.apache.org/jira/browse/HDFS-12858) | RBF: Add router admin commands usage in HDFS commands reference doc |  Minor | documentation | Yiqun Lin | Yiqun Lin |
| [HDFS-12835](https://issues.apache.org/jira/browse/HDFS-12835) | RBF: Fix Javadoc parameter errors |  Minor | . | Wei Yan | Wei Yan |
| [YARN-6907](https://issues.apache.org/jira/browse/YARN-6907) | Node information page in the old web UI should report resource types |  Major | resourcemanager | Daniel Templeton | Gergely Novák |
| [HDFS-12396](https://issues.apache.org/jira/browse/HDFS-12396) | Webhdfs file system should get delegation token from kms provider. |  Major | encryption, kms, webhdfs | Rushabh S Shah | Rushabh S Shah |
| [YARN-7610](https://issues.apache.org/jira/browse/YARN-7610) | Extend Distributed Shell to support launching job with opportunistic containers |  Major | applications/distributed-shell | Weiwei Yang | Weiwei Yang |
| [HDFS-12875](https://issues.apache.org/jira/browse/HDFS-12875) | RBF: Complete logic for -readonly option of dfsrouteradmin add command |  Major | . | Yiqun Lin | Íñigo Goiri |
| [YARN-7383](https://issues.apache.org/jira/browse/YARN-7383) | Node resource is not parsed correctly for resource names containing dot |  Major | nodemanager, resourcemanager | Jonathan Hung | Gergely Novák |
| [YARN-7119](https://issues.apache.org/jira/browse/YARN-7119) | Support multiple resource types in rmadmin updateNodeResource command |  Major | nodemanager, resourcemanager | Daniel Templeton | Manikandan R |
| [YARN-7617](https://issues.apache.org/jira/browse/YARN-7617) | Add a flag in distributed shell to automatically PROMOTE opportunistic containers to guaranteed once they are started |  Minor | applications/distributed-shell | Weiwei Yang | Weiwei Yang |
| [HDFS-12937](https://issues.apache.org/jira/browse/HDFS-12937) | RBF: Add more unit tests for router admin commands |  Major | test | Yiqun Lin | Yiqun Lin |
| [YARN-7032](https://issues.apache.org/jira/browse/YARN-7032) | [ATSv2] NPE while starting hbase co-processor when HBase authorization is enabled. |  Critical | . | Rohith Sharma K S | Rohith Sharma K S |
| [HADOOP-14965](https://issues.apache.org/jira/browse/HADOOP-14965) | s3a input stream "normal" fadvise mode to be adaptive |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-15086](https://issues.apache.org/jira/browse/HADOOP-15086) | NativeAzureFileSystem file rename is not atomic |  Major | fs/azure | Shixiong Zhu | Thomas Marquardt |
| [HDFS-12988](https://issues.apache.org/jira/browse/HDFS-12988) | RBF: Mount table entries not properly updated in the local cache |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-7716](https://issues.apache.org/jira/browse/YARN-7716) | metricsTimeStart and metricsTimeEnd should be all lower case in the doc |  Major | timelinereader | Haibo Chen | Haibo Chen |
| [HDFS-12802](https://issues.apache.org/jira/browse/HDFS-12802) | RBF: Control MountTableResolver cache size |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-12919](https://issues.apache.org/jira/browse/HDFS-12919) | RBF: Support erasure coding methods in RouterRpcServer |  Critical | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-6736](https://issues.apache.org/jira/browse/YARN-6736) | Consider writing to both ats v1 & v2 from RM for smoother upgrades |  Major | timelineserver | Vrushali C | Aaron Gresch |
| [HADOOP-15027](https://issues.apache.org/jira/browse/HADOOP-15027) | AliyunOSS: Support multi-thread pre-read to improve sequential read from Hadoop to Aliyun OSS performance |  Major | fs/oss | wujinhu | wujinhu |
| [HDFS-13028](https://issues.apache.org/jira/browse/HDFS-13028) | RBF: Fix spurious TestRouterRpc#testProxyGetStats |  Minor | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-5094](https://issues.apache.org/jira/browse/YARN-5094) | some YARN container events have timestamp of -1 |  Critical | . | Sangjin Lee | Haibo Chen |
| [YARN-7782](https://issues.apache.org/jira/browse/YARN-7782) | Enable user re-mapping for Docker containers in yarn-default.xml |  Blocker | security, yarn | Eric Yang | Eric Yang |
| [HDFS-12772](https://issues.apache.org/jira/browse/HDFS-12772) | RBF: Federation Router State State Store internal API |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13042](https://issues.apache.org/jira/browse/HDFS-13042) | RBF: Heartbeat Router State |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13049](https://issues.apache.org/jira/browse/HDFS-13049) | RBF: Inconsistent Router OPTS config in branch-2 and branch-3 |  Minor | . | Wei Yan | Wei Yan |
| [HDFS-12574](https://issues.apache.org/jira/browse/HDFS-12574) | Add CryptoInputStream to WebHdfsFileSystem read call. |  Major | encryption, kms, webhdfs | Rushabh S Shah | Rushabh S Shah |
| [HDFS-13044](https://issues.apache.org/jira/browse/HDFS-13044) | RBF: Add a safe mode for the Router |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13043](https://issues.apache.org/jira/browse/HDFS-13043) | RBF: Expose the state of the Routers in the federation |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-12997](https://issues.apache.org/jira/browse/HDFS-12997) | Move logging to slf4j in BlockPoolSliceStorage and Storage |  Major | . | Ajay Kumar | Ajay Kumar |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-15149](https://issues.apache.org/jira/browse/HADOOP-15149) | CryptoOutputStream should implement StreamCapabilities |  Major | fs | Mike Drob | Xiao Chen |
| [YARN-7691](https://issues.apache.org/jira/browse/YARN-7691) | Add Unit Tests for ContainersLauncher |  Major | . | Sampada Dehankar | Sampada Dehankar |
| [HADOOP-15177](https://issues.apache.org/jira/browse/HADOOP-15177) | Update the release year to 2018 |  Blocker | build | Akira Ajisaka | Bharat Viswanadham |
| [HADOOP-15197](https://issues.apache.org/jira/browse/HADOOP-15197) | Remove tomcat from the Hadoop-auth test bundle |  Major | . | Xiao Chen | Xiao Chen |


