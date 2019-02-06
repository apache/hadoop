
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

## Release 2.10.0 - Unreleased (as of 2018-09-02)

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-12883](https://issues.apache.org/jira/browse/HDFS-12883) | RBF: Document Router and State Store metrics |  Major | documentation | Yiqun Lin | Yiqun Lin |
| [HDFS-12895](https://issues.apache.org/jira/browse/HDFS-12895) | RBF: Add ACL support for mount table |  Major | . | Yiqun Lin | Yiqun Lin |
| [HDFS-13099](https://issues.apache.org/jira/browse/HDFS-13099) | RBF: Use the ZooKeeper as the default State Store |  Minor | documentation | Yiqun Lin | Yiqun Lin |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-13083](https://issues.apache.org/jira/browse/HDFS-13083) | RBF: Fix doc error setting up client |  Major | federation | tartarus | tartarus |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-13283](https://issues.apache.org/jira/browse/HDFS-13283) | Percentage based Reserved Space Calculation for DataNode |  Major | datanode, hdfs | Lukas Majercak | Lukas Majercak |
| [HDFS-13553](https://issues.apache.org/jira/browse/HDFS-13553) | RBF: Support global quota |  Major | . | Íñigo Goiri | Yiqun Lin |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-14987](https://issues.apache.org/jira/browse/HADOOP-14987) | Improve KMSClientProvider log around delegation token checking |  Major | . | Xiaoyu Yao | Xiaoyu Yao |
| [HADOOP-14872](https://issues.apache.org/jira/browse/HADOOP-14872) | CryptoInputStream should implement unbuffer |  Major | fs, security | John Zhuge | John Zhuge |
| [HADOOP-14960](https://issues.apache.org/jira/browse/HADOOP-14960) | Add GC time percentage monitor/alerter |  Major | . | Misha Dmitriev | Misha Dmitriev |
| [HADOOP-15023](https://issues.apache.org/jira/browse/HADOOP-15023) | ValueQueue should also validate (lowWatermark \* numValues) \> 0 on construction |  Minor | . | Xiao Chen | Xiao Chen |
| [YARN-6851](https://issues.apache.org/jira/browse/YARN-6851) | Capacity Scheduler: document configs for controlling # containers allowed to be allocated per node heartbeat |  Minor | . | Wei Yan | Wei Yan |
| [YARN-7495](https://issues.apache.org/jira/browse/YARN-7495) | Improve robustness of the AggregatedLogDeletionService |  Major | log-aggregation | Jonathan Eagles | Jonathan Eagles |
| [HADOOP-15056](https://issues.apache.org/jira/browse/HADOOP-15056) | Fix TestUnbuffer#testUnbufferException failure |  Minor | test | Jack Bearden | Jack Bearden |
| [HADOOP-15012](https://issues.apache.org/jira/browse/HADOOP-15012) | Add readahead, dropbehind, and unbuffer to StreamCapabilities |  Major | fs | John Zhuge | John Zhuge |
| [HADOOP-15104](https://issues.apache.org/jira/browse/HADOOP-15104) | AliyunOSS: change the default value of max error retry |  Major | fs/oss | wujinhu | wujinhu |
| [YARN-7642](https://issues.apache.org/jira/browse/YARN-7642) | Add test case to verify context update after container promotion or demotion with or without auto update |  Minor | nodemanager | Weiwei Yang | Weiwei Yang |
| [HADOOP-15111](https://issues.apache.org/jira/browse/HADOOP-15111) | AliyunOSS: backport HADOOP-14993 to branch-2 |  Major | fs/oss | Genmao Yu | Genmao Yu |
| [HDFS-12818](https://issues.apache.org/jira/browse/HDFS-12818) | Support multiple storages in DataNodeCluster / SimulatedFSDataset |  Minor | datanode, test | Erik Krogen | Erik Krogen |
| [HDFS-9023](https://issues.apache.org/jira/browse/HDFS-9023) | When NN is not able to identify DN for replication, reason behind it can be logged |  Critical | hdfs-client, namenode | Surendra Singh Lilhore | Xiao Chen |
| [YARN-7678](https://issues.apache.org/jira/browse/YARN-7678) | Ability to enable logging of container memory stats |  Major | nodemanager | Jim Brennan | Jim Brennan |
| [HDFS-12945](https://issues.apache.org/jira/browse/HDFS-12945) | Switch to ClientProtocol instead of NamenodeProtocols in NamenodeWebHdfsMethods |  Minor | . | Wei Yan | Wei Yan |
| [YARN-7622](https://issues.apache.org/jira/browse/YARN-7622) | Allow fair-scheduler configuration on HDFS |  Minor | fairscheduler, resourcemanager | Greg Phillips | Greg Phillips |
| [YARN-7590](https://issues.apache.org/jira/browse/YARN-7590) | Improve container-executor validation check |  Major | security, yarn | Eric Yang | Eric Yang |
| [MAPREDUCE-7029](https://issues.apache.org/jira/browse/MAPREDUCE-7029) | FileOutputCommitter is slow on filesystems lacking recursive delete |  Minor | . | Karthik Palaniappan | Karthik Palaniappan |
| [MAPREDUCE-6984](https://issues.apache.org/jira/browse/MAPREDUCE-6984) | MR AM to clean up temporary files from previous attempt in case of no recovery |  Major | applicationmaster | Gergo Repas | Gergo Repas |
| [HADOOP-15189](https://issues.apache.org/jira/browse/HADOOP-15189) | backport HADOOP-15039 to branch-2 and branch-3 |  Blocker | . | Genmao Yu | Genmao Yu |
| [HADOOP-15212](https://issues.apache.org/jira/browse/HADOOP-15212) | Add independent secret manager method for logging expired tokens |  Major | security | Daryn Sharp | Daryn Sharp |
| [YARN-7728](https://issues.apache.org/jira/browse/YARN-7728) | Expose container preemptions related information in Capacity Scheduler queue metrics |  Major | . | Eric Payne | Eric Payne |
| [MAPREDUCE-7048](https://issues.apache.org/jira/browse/MAPREDUCE-7048) | Uber AM can crash due to unknown task in statusUpdate |  Major | mr-am | Peter Bacsko | Peter Bacsko |
| [HADOOP-13972](https://issues.apache.org/jira/browse/HADOOP-13972) | ADLS to support per-store configuration |  Major | fs/adl | John Zhuge | Sharad Sonker |
| [YARN-7813](https://issues.apache.org/jira/browse/YARN-7813) | Capacity Scheduler Intra-queue Preemption should be configurable for each queue |  Major | capacity scheduler, scheduler preemption | Eric Payne | Eric Payne |
| [HADOOP-15235](https://issues.apache.org/jira/browse/HADOOP-15235) | Authentication Tokens should use HMAC instead of MAC |  Major | security | Robert Kanter | Robert Kanter |
| [HDFS-11187](https://issues.apache.org/jira/browse/HDFS-11187) | Optimize disk access for last partial chunk checksum of Finalized replica |  Major | datanode | Wei-Chiu Chuang | Gabor Bota |
| [HADOOP-15266](https://issues.apache.org/jira/browse/HADOOP-15266) |  [branch-2] Upper/Lower case conversion support for group names in LdapGroupsMapping |  Major | . | Nanda kumar | Nanda kumar |
| [HADOOP-15279](https://issues.apache.org/jira/browse/HADOOP-15279) | increase maven heap size recommendations |  Minor | build, documentation, test | Allen Wittenauer | Allen Wittenauer |
| [HDFS-12884](https://issues.apache.org/jira/browse/HDFS-12884) | BlockUnderConstructionFeature.truncateBlock should be of type BlockInfo |  Major | namenode | Konstantin Shvachko | chencan |
| [HADOOP-15334](https://issues.apache.org/jira/browse/HADOOP-15334) | Upgrade Maven surefire plugin |  Major | build | Arpit Agarwal | Arpit Agarwal |
| [HADOOP-15312](https://issues.apache.org/jira/browse/HADOOP-15312) | Undocumented KeyProvider configuration keys |  Major | . | Wei-Chiu Chuang | LiXin Ge |
| [YARN-7623](https://issues.apache.org/jira/browse/YARN-7623) | Fix the CapacityScheduler Queue configuration documentation |  Major | . | Arun Suresh | Jonathan Hung |
| [HDFS-13314](https://issues.apache.org/jira/browse/HDFS-13314) | NameNode should optionally exit if it detects FsImage corruption |  Major | namenode | Arpit Agarwal | Arpit Agarwal |
| [HDFS-13418](https://issues.apache.org/jira/browse/HDFS-13418) |  NetworkTopology should be configurable when enable DFSNetworkTopology |  Major | . | Tao Jie | Tao Jie |
| [HADOOP-15394](https://issues.apache.org/jira/browse/HADOOP-15394) | Backport PowerShell NodeFencer HADOOP-14309 to branch-2 |  Minor | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13462](https://issues.apache.org/jira/browse/HDFS-13462) | Add BIND\_HOST configuration for JournalNode's HTTP and RPC Servers |  Major | hdfs, journal-node | Lukas Majercak | Lukas Majercak |
| [HDFS-13492](https://issues.apache.org/jira/browse/HDFS-13492) | Limit httpfs binds to certain IP addresses in branch-2 |  Major | httpfs | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-12981](https://issues.apache.org/jira/browse/HDFS-12981) | renameSnapshot a Non-Existent snapshot to itself should throw error |  Minor | hdfs | Sailesh Patel | Kitti Nanasi |
| [HADOOP-15441](https://issues.apache.org/jira/browse/HADOOP-15441) | Log kms url and token service at debug level. |  Minor | . | Wei-Chiu Chuang | Gabor Bota |
| [YARN-8249](https://issues.apache.org/jira/browse/YARN-8249) | Few REST api's in RMWebServices are missing static user check |  Critical | webapp, yarn | Sunil Govindan | Sunil Govindan |
| [HADOOP-15486](https://issues.apache.org/jira/browse/HADOOP-15486) | Make NetworkTopology#netLock fair |  Major | net | Nanda kumar | Nanda kumar |
| [HADOOP-15449](https://issues.apache.org/jira/browse/HADOOP-15449) | Increase default timeout of ZK session to avoid frequent NameNode failover |  Critical | common | Karthik Palanisamy | Karthik Palanisamy |
| [HDFS-13602](https://issues.apache.org/jira/browse/HDFS-13602) | Add checkOperation(WRITE) checks in FSNamesystem |  Major | ha, namenode | Erik Krogen | Chao Sun |
| [HDFS-13644](https://issues.apache.org/jira/browse/HDFS-13644) | Backport HDFS-10376 to branch-2 |  Major | . | Yiqun Lin | Zsolt Venczel |
| [HDFS-13653](https://issues.apache.org/jira/browse/HDFS-13653) | Make dfs.client.failover.random.order a per nameservice configuration |  Major | federation | Ekanth Sethuramalingam | Ekanth Sethuramalingam |
| [HDFS-13714](https://issues.apache.org/jira/browse/HDFS-13714) | Fix TestNameNodePrunesMissingStorages test failures on Windows |  Major | hdfs, namenode, test | Lukas Majercak | Lukas Majercak |
| [HDFS-13719](https://issues.apache.org/jira/browse/HDFS-13719) | Docs around dfs.image.transfer.timeout are misleading |  Major | . | Kitti Nanasi | Kitti Nanasi |
| [HDFS-11060](https://issues.apache.org/jira/browse/HDFS-11060) | make DEFAULT\_MAX\_CORRUPT\_FILEBLOCKS\_RETURNED configurable |  Minor | hdfs | Lantao Jin | Lantao Jin |
| [YARN-8155](https://issues.apache.org/jira/browse/YARN-8155) | Improve ATSv2 client logging in RM and NM publisher |  Major | . | Rohith Sharma K S | Abhishek Modi |
| [HDFS-13814](https://issues.apache.org/jira/browse/HDFS-13814) | Remove super user privilege requirement for NameNode.getServiceStatus |  Minor | namenode | Chao Sun | Chao Sun |
| [YARN-8559](https://issues.apache.org/jira/browse/YARN-8559) | Expose mutable-conf scheduler's configuration in RM /scheduler-conf endpoint |  Major | resourcemanager | Anna Savarin | Weiwei Yang |
| [HDFS-13813](https://issues.apache.org/jira/browse/HDFS-13813) | Exit NameNode if dangling child inode is detected when saving FsImage |  Major | hdfs, namenode | Siyao Meng | Siyao Meng |
| [HDFS-13821](https://issues.apache.org/jira/browse/HDFS-13821) | RBF: Add dfs.federation.router.mount-table.cache.enable so that users can disable cache |  Major | hdfs | Fei Hui | Fei Hui |
| [HADOOP-15689](https://issues.apache.org/jira/browse/HADOOP-15689) | Add "\*.patch" into .gitignore file of branch-2 |  Major | . | Rui Gao | Rui Gao |
| [HDFS-13831](https://issues.apache.org/jira/browse/HDFS-13831) | Make block increment deletion number configurable |  Major | . | Yiqun Lin | Ryan Wu |
| [YARN-8051](https://issues.apache.org/jira/browse/YARN-8051) | TestRMEmbeddedElector#testCallbackSynchronization is flakey |  Major | test | Robert Kanter | Robert Kanter |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-12052](https://issues.apache.org/jira/browse/HDFS-12052) | Set SWEBHDFS delegation token kind when ssl is enabled in HttpFS |  Major | httpfs, webhdfs | Zoran Dimitrijevic | Zoran Dimitrijevic |
| [HDFS-12318](https://issues.apache.org/jira/browse/HDFS-12318) | Fix IOException condition for openInfo in DFSInputStream |  Major | . | legend | legend |
| [HDFS-12614](https://issues.apache.org/jira/browse/HDFS-12614) | FSPermissionChecker#getINodeAttrs() throws NPE when INodeAttributesProvider configured |  Major | . | Manoj Govindassamy | Manoj Govindassamy |
| [YARN-7370](https://issues.apache.org/jira/browse/YARN-7370) | Preemption properties should be refreshable |  Major | capacity scheduler, scheduler preemption | Eric Payne | Gergely Novák |
| [YARN-7428](https://issues.apache.org/jira/browse/YARN-7428) | Add containerId to Localizer failed logs |  Minor | nodemanager | Prabhu Joseph | Prabhu Joseph |
| [HDFS-12783](https://issues.apache.org/jira/browse/HDFS-12783) | [branch-2] "dfsrouter" should use hdfsScript |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-12788](https://issues.apache.org/jira/browse/HDFS-12788) | Reset the upload button when file upload fails |  Critical | ui, webhdfs | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-7469](https://issues.apache.org/jira/browse/YARN-7469) | Capacity Scheduler Intra-queue preemption: User can starve if newest app is exactly at user limit |  Major | capacity scheduler, yarn | Eric Payne | Eric Payne |
| [HADOOP-14982](https://issues.apache.org/jira/browse/HADOOP-14982) | Clients using FailoverOnNetworkExceptionRetry can go into a loop if they're used without authenticating with kerberos in HA env |  Major | common | Peter Bacsko | Peter Bacsko |
| [YARN-7489](https://issues.apache.org/jira/browse/YARN-7489) | ConcurrentModificationException in RMAppImpl#getRMAppMetrics |  Major | capacityscheduler | Tao Yang | Tao Yang |
| [YARN-7525](https://issues.apache.org/jira/browse/YARN-7525) | Incorrect query parameters in cluster nodes REST API document |  Minor | documentation | Tao Yang | Tao Yang |
| [HDFS-12813](https://issues.apache.org/jira/browse/HDFS-12813) | RequestHedgingProxyProvider can hide Exception thrown from the Namenode for proxy size of 1 |  Major | ha | Mukul Kumar Singh | Mukul Kumar Singh |
| [HADOOP-15045](https://issues.apache.org/jira/browse/HADOOP-15045) | ISA-L build options are documented in branch-2 |  Major | build, documentation | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-15067](https://issues.apache.org/jira/browse/HADOOP-15067) | GC time percentage reported in JvmMetrics should be a gauge, not counter |  Major | . | Misha Dmitriev | Misha Dmitriev |
| [YARN-7363](https://issues.apache.org/jira/browse/YARN-7363) | ContainerLocalizer doesn't have a valid log4j config when using LinuxContainerExecutor |  Major | nodemanager | Yufei Gu | Yufei Gu |
| [HDFS-12754](https://issues.apache.org/jira/browse/HDFS-12754) | Lease renewal can hit a deadlock |  Major | . | Kuhu Shukla | Kuhu Shukla |
| [HDFS-12832](https://issues.apache.org/jira/browse/HDFS-12832) | INode.getFullPathName may throw ArrayIndexOutOfBoundsException lead to NameNode exit |  Critical | namenode | DENG FEI | Konstantin Shvachko |
| [HDFS-11754](https://issues.apache.org/jira/browse/HDFS-11754) | Make FsServerDefaults cache configurable. |  Minor | . | Rushabh S Shah | Mikhail Erofeev |
| [HADOOP-15042](https://issues.apache.org/jira/browse/HADOOP-15042) | Azure PageBlobInputStream.skip() can return negative value when numberOfPagesRemaining is 0 |  Minor | fs/azure | Rajesh Balamohan | Rajesh Balamohan |
| [HDFS-12638](https://issues.apache.org/jira/browse/HDFS-12638) | Delete copy-on-truncate block along with the original block, when deleting a file being truncated |  Blocker | hdfs | Jiandan Yang | Konstantin Shvachko |
| [YARN-4813](https://issues.apache.org/jira/browse/YARN-4813) | TestRMWebServicesDelegationTokenAuthentication.testDoAs fails intermittently |  Major | resourcemanager | Daniel Templeton | Gergo Repas |
| [MAPREDUCE-5124](https://issues.apache.org/jira/browse/MAPREDUCE-5124) | AM lacks flow control for task events |  Major | mr-am | Jason Lowe | Peter Bacsko |
| [YARN-7455](https://issues.apache.org/jira/browse/YARN-7455) | quote\_and\_append\_arg can overflow buffer |  Major | nodemanager | Jason Lowe | Jim Brennan |
| [YARN-5594](https://issues.apache.org/jira/browse/YARN-5594) | Handle old RMDelegationToken format when recovering RM |  Major | resourcemanager | Tatyana But | Robert Kanter |
| [HADOOP-14985](https://issues.apache.org/jira/browse/HADOOP-14985) | Remove subversion related code from VersionInfoMojo.java |  Minor | build | Akira Ajisaka | Ajay Kumar |
| [HDFS-11751](https://issues.apache.org/jira/browse/HDFS-11751) | DFSZKFailoverController daemon exits with wrong status code |  Major | auto-failover | Doris Gu | Bharat Viswanadham |
| [HDFS-12889](https://issues.apache.org/jira/browse/HDFS-12889) | Router UI is missing robots.txt file |  Major | . | Bharat Viswanadham | Bharat Viswanadham |
| [YARN-7607](https://issues.apache.org/jira/browse/YARN-7607) | Remove the trailing duplicated timestamp in container diagnostics message |  Minor | nodemanager | Weiwei Yang | Weiwei Yang |
| [HADOOP-15080](https://issues.apache.org/jira/browse/HADOOP-15080) | Aliyun OSS: update oss sdk from 2.8.1 to 2.8.3 to remove its dependency on Cat-x "json-lib" |  Blocker | fs/oss | Chris Douglas | Sammi Chen |
| [YARN-7608](https://issues.apache.org/jira/browse/YARN-7608) | Incorrect sTarget column causing DataTable warning on RM application and scheduler web page |  Major | resourcemanager, webapp | Weiwei Yang | Gergely Novák |
| [HDFS-12833](https://issues.apache.org/jira/browse/HDFS-12833) | Distcp : Update the usage of delete option for dependency with update and overwrite option |  Minor | distcp, hdfs | Harshakiran Reddy | usharani |
| [YARN-7647](https://issues.apache.org/jira/browse/YARN-7647) | NM print inappropriate error log when node-labels is enabled |  Minor | . | Yang Wang | Yang Wang |
| [HDFS-12907](https://issues.apache.org/jira/browse/HDFS-12907) | Allow read-only access to reserved raw for non-superusers |  Major | namenode | Daryn Sharp | Rushabh S Shah |
| [HDFS-12881](https://issues.apache.org/jira/browse/HDFS-12881) | Output streams closed with IOUtils suppressing write errors |  Major | . | Jason Lowe | Ajay Kumar |
| [YARN-7595](https://issues.apache.org/jira/browse/YARN-7595) | Container launching code suppresses close exceptions after writes |  Major | nodemanager | Jason Lowe | Jim Brennan |
| [HADOOP-15085](https://issues.apache.org/jira/browse/HADOOP-15085) | Output streams closed with IOUtils suppressing write errors |  Major | . | Jason Lowe | Jim Brennan |
| [HADOOP-15123](https://issues.apache.org/jira/browse/HADOOP-15123) | KDiag tries to load krb5.conf from KRB5CCNAME instead of KRB5\_CONFIG |  Minor | security | Vipin Rathor | Vipin Rathor |
| [YARN-7661](https://issues.apache.org/jira/browse/YARN-7661) | NodeManager metrics return wrong value after update node resource |  Major | . | Yang Wang | Yang Wang |
| [HDFS-12347](https://issues.apache.org/jira/browse/HDFS-12347) | TestBalancerRPCDelay#testBalancerRPCDelay fails very frequently |  Critical | test | Xiao Chen | Bharat Viswanadham |
| [YARN-7662](https://issues.apache.org/jira/browse/YARN-7662) | [Atsv2] Define new set of configurations for reader and collectors to bind. |  Major | . | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-7674](https://issues.apache.org/jira/browse/YARN-7674) | Update Timeline Reader web app address in UI2 |  Major | . | Rohith Sharma K S | Sunil Govindan |
| [YARN-7542](https://issues.apache.org/jira/browse/YARN-7542) | Fix issue that causes some Running Opportunistic Containers to be recovered as PAUSED |  Major | . | Arun Suresh | Sampada Dehankar |
| [HADOOP-15143](https://issues.apache.org/jira/browse/HADOOP-15143) | NPE due to Invalid KerberosTicket in UGI |  Major | . | Jitendra Nath Pandey | Mukul Kumar Singh |
| [YARN-7692](https://issues.apache.org/jira/browse/YARN-7692) | Skip validating priority acls while recovering applications |  Blocker | resourcemanager | Charan Hebri | Sunil Govindan |
| [MAPREDUCE-7028](https://issues.apache.org/jira/browse/MAPREDUCE-7028) | Concurrent task progress updates causing NPE in Application Master |  Blocker | mr-am | Gergo Repas | Gergo Repas |
| [YARN-7619](https://issues.apache.org/jira/browse/YARN-7619) | Max AM Resource value in Capacity Scheduler UI has to be refreshed for every user |  Major | capacity scheduler, yarn | Eric Payne | Eric Payne |
| [YARN-7699](https://issues.apache.org/jira/browse/YARN-7699) | queueUsagePercentage is coming as INF for getApp REST api call |  Major | webapp | Sunil Govindan | Sunil Govindan |
| [HDFS-12985](https://issues.apache.org/jira/browse/HDFS-12985) | NameNode crashes during restart after an OpenForWrite file present in the Snapshot got deleted |  Major | hdfs | Manoj Govindassamy | Manoj Govindassamy |
| [YARN-4227](https://issues.apache.org/jira/browse/YARN-4227) | Ignore expired containers from removed nodes in FairScheduler |  Critical | fairscheduler | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [YARN-7508](https://issues.apache.org/jira/browse/YARN-7508) | NPE in FiCaSchedulerApp when debug log enabled in async-scheduling mode |  Major | capacityscheduler | Tao Yang | Tao Yang |
| [YARN-7663](https://issues.apache.org/jira/browse/YARN-7663) | RMAppImpl:Invalid event: START at KILLED |  Major | resourcemanager | lujie | lujie |
| [YARN-6948](https://issues.apache.org/jira/browse/YARN-6948) | Invalid event: ATTEMPT\_ADDED at FINAL\_SAVING |  Major | yarn | lujie | lujie |
| [HADOOP-15060](https://issues.apache.org/jira/browse/HADOOP-15060) | TestShellBasedUnixGroupsMapping.testFiniteGroupResolutionTime flaky |  Major | . | Miklos Szegedi | Miklos Szegedi |
| [YARN-7735](https://issues.apache.org/jira/browse/YARN-7735) | Fix typo in YARN documentation |  Minor | documentation | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-7727](https://issues.apache.org/jira/browse/YARN-7727) | Incorrect log levels in few logs with QueuePriorityContainerCandidateSelector |  Minor | yarn | Prabhu Joseph | Prabhu Joseph |
| [HDFS-11915](https://issues.apache.org/jira/browse/HDFS-11915) | Sync rbw dir on the first hsync() to avoid file lost on power failure |  Critical | . | Kanaka Kumar Avvaru | Vinayakumar B |
| [YARN-7705](https://issues.apache.org/jira/browse/YARN-7705) | Create the container log directory with correct sticky bit in C code |  Major | nodemanager | Yufei Gu | Yufei Gu |
| [HDFS-9049](https://issues.apache.org/jira/browse/HDFS-9049) | Make Datanode Netty reverse proxy port to be configurable |  Major | datanode | Vinayakumar B | Vinayakumar B |
| [YARN-7758](https://issues.apache.org/jira/browse/YARN-7758) | Add an additional check to the validity of container and application ids passed to container-executor |  Major | nodemanager | Miklos Szegedi | Yufei Gu |
| [HADOOP-15150](https://issues.apache.org/jira/browse/HADOOP-15150) | in FsShell, UGI params should be overidden through env vars(-D arg) |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-15181](https://issues.apache.org/jira/browse/HADOOP-15181) | Typo in SecureMode.md |  Trivial | documentation | Masahiro Tanaka | Masahiro Tanaka |
| [YARN-7806](https://issues.apache.org/jira/browse/YARN-7806) | Distributed Shell should use timeline async api's |  Major | distributed-shell | Sumana Sathish | Rohith Sharma K S |
| [HADOOP-15121](https://issues.apache.org/jira/browse/HADOOP-15121) | Encounter NullPointerException when using DecayRpcScheduler |  Major | . | Tao Jie | Tao Jie |
| [MAPREDUCE-7015](https://issues.apache.org/jira/browse/MAPREDUCE-7015) | Possible race condition in JHS if the job is not loaded |  Major | jobhistoryserver | Peter Bacsko | Peter Bacsko |
| [YARN-7737](https://issues.apache.org/jira/browse/YARN-7737) | prelaunch.err file not found exception on container failure |  Major | . | Jonathan Hung | Keqiu Hu |
| [HDFS-13063](https://issues.apache.org/jira/browse/HDFS-13063) | Fix the incorrect spelling in HDFSHighAvailabilityWithQJM.md |  Trivial | documentation | Jianfei Jiang | Jianfei Jiang |
| [YARN-7102](https://issues.apache.org/jira/browse/YARN-7102) | NM heartbeat stuck when responseId overflows MAX\_INT |  Critical | . | Botong Huang | Botong Huang |
| [MAPREDUCE-7041](https://issues.apache.org/jira/browse/MAPREDUCE-7041) | MR should not try to clean up at first job attempt |  Major | . | Takanobu Asanuma | Gergo Repas |
| [MAPREDUCE-7020](https://issues.apache.org/jira/browse/MAPREDUCE-7020) | Task timeout in uber mode can crash AM |  Major | mr-am | Akira Ajisaka | Peter Bacsko |
| [YARN-7765](https://issues.apache.org/jira/browse/YARN-7765) | [Atsv2] GSSException: No valid credentials provided - Failed to find any Kerberos tgt thrown by Timelinev2Client & HBaseClient in NM |  Blocker | . | Sumana Sathish | Rohith Sharma K S |
| [HDFS-12974](https://issues.apache.org/jira/browse/HDFS-12974) | Exception message is not printed when creating an encryption zone fails with AuthorizationException |  Minor | encryption | fang zhenyi | fang zhenyi |
| [YARN-7698](https://issues.apache.org/jira/browse/YARN-7698) | A misleading variable's name in ApplicationAttemptEventDispatcher |  Minor | resourcemanager | Jinjiang Ling | Jinjiang Ling |
| [HDFS-12528](https://issues.apache.org/jira/browse/HDFS-12528) | Add an option to not disable short-circuit reads on failures |  Major | hdfs-client, performance | Andre Araujo | Xiao Chen |
| [HDFS-13100](https://issues.apache.org/jira/browse/HDFS-13100) | Handle IllegalArgumentException when GETSERVERDEFAULTS is not implemented in webhdfs. |  Critical | hdfs, webhdfs | Yongjun Zhang | Yongjun Zhang |
| [YARN-7849](https://issues.apache.org/jira/browse/YARN-7849) | TestMiniYarnClusterNodeUtilization#testUpdateNodeUtilization fails due to heartbeat sync error |  Major | test | Jason Lowe | Botong Huang |
| [YARN-7801](https://issues.apache.org/jira/browse/YARN-7801) | AmFilterInitializer should addFilter after fill all parameters |  Critical | . | Sumana Sathish | Wangda Tan |
| [YARN-7890](https://issues.apache.org/jira/browse/YARN-7890) | NPE during container relaunch |  Major | . | Billie Rinaldi | Jason Lowe |
| [HDFS-13115](https://issues.apache.org/jira/browse/HDFS-13115) | In getNumUnderConstructionBlocks(), ignore the inodeIds for which the inodes have been deleted |  Major | . | Yongjun Zhang | Yongjun Zhang |
| [HDFS-12935](https://issues.apache.org/jira/browse/HDFS-12935) | Get ambiguous result for DFSAdmin command in HA mode when only one namenode is up |  Major | tools | Jianfei Jiang | Jianfei Jiang |
| [HDFS-13120](https://issues.apache.org/jira/browse/HDFS-13120) | Snapshot diff could be corrupted after concat |  Major | namenode, snapshots | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-10453](https://issues.apache.org/jira/browse/HDFS-10453) | ReplicationMonitor thread could stuck for long time due to the race between replication and delete of same file in a large cluster. |  Major | namenode | He Xiaoqiao | He Xiaoqiao |
| [HDFS-8693](https://issues.apache.org/jira/browse/HDFS-8693) | refreshNamenodes does not support adding a new standby to a running DN |  Critical | datanode, ha | Jian Fang | Ajith S |
| [MAPREDUCE-7052](https://issues.apache.org/jira/browse/MAPREDUCE-7052) | TestFixedLengthInputFormat#testFormatCompressedIn is flaky |  Major | client, test | Peter Bacsko | Peter Bacsko |
| [HDFS-13112](https://issues.apache.org/jira/browse/HDFS-13112) | Token expiration edits may cause log corruption or deadlock |  Critical | namenode | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-7053](https://issues.apache.org/jira/browse/MAPREDUCE-7053) | Timed out tasks can fail to produce thread dump |  Major | . | Jason Lowe | Jason Lowe |
| [HADOOP-15206](https://issues.apache.org/jira/browse/HADOOP-15206) | BZip2 drops and duplicates records when input split size is small |  Major | . | Aki Tanaka | Aki Tanaka |
| [YARN-7937](https://issues.apache.org/jira/browse/YARN-7937) | Fix http method name in Cluster Application Timeout Update API example request |  Minor | docs, documentation | Charan Hebri | Charan Hebri |
| [YARN-7947](https://issues.apache.org/jira/browse/YARN-7947) | Capacity Scheduler intra-queue preemption can NPE for non-schedulable apps |  Major | capacity scheduler, scheduler preemption | Eric Payne | Eric Payne |
| [YARN-7945](https://issues.apache.org/jira/browse/YARN-7945) | Java Doc error in UnmanagedAMPoolManager for branch-2 |  Major | . | Rohith Sharma K S | Botong Huang |
| [HADOOP-14903](https://issues.apache.org/jira/browse/HADOOP-14903) | Add json-smart explicitly to pom.xml |  Major | common | Ray Chiang | Ray Chiang |
| [HADOOP-15236](https://issues.apache.org/jira/browse/HADOOP-15236) | Fix typo in RequestHedgingProxyProvider and RequestHedgingRMFailoverProxyProvider |  Trivial | documentation | Akira Ajisaka | Gabor Bota |
| [MAPREDUCE-7027](https://issues.apache.org/jira/browse/MAPREDUCE-7027) | HadoopArchiveLogs shouldn't delete the original logs if the HAR creation fails |  Critical | mrv2 | Gergely Novák | Gergely Novák |
| [HDFS-12781](https://issues.apache.org/jira/browse/HDFS-12781) | After Datanode down, In Namenode UI Datanode tab is throwing warning message. |  Major | datanode | Harshakiran Reddy | Brahma Reddy Battula |
| [HDFS-12070](https://issues.apache.org/jira/browse/HDFS-12070) | Failed block recovery leaves files open indefinitely and at risk for data loss |  Major | . | Daryn Sharp | Kihwal Lee |
| [HADOOP-15251](https://issues.apache.org/jira/browse/HADOOP-15251) | Backport HADOOP-13514 (surefire upgrade) to branch-2 |  Major | test | Chris Douglas | Chris Douglas |
| [HDFS-13194](https://issues.apache.org/jira/browse/HDFS-13194) | CachePool permissions incorrectly checked |  Major | . | Yiqun Lin | Jianfei Jiang |
| [HADOOP-15276](https://issues.apache.org/jira/browse/HADOOP-15276) | branch-2 site not building after ADL troubleshooting doc added |  Major | documentation | Steve Loughran | Steve Loughran |
| [YARN-7835](https://issues.apache.org/jira/browse/YARN-7835) | [Atsv2] Race condition in NM while publishing events if second attempt is launched on the same node |  Critical | . | Rohith Sharma K S | Rohith Sharma K S |
| [HADOOP-15275](https://issues.apache.org/jira/browse/HADOOP-15275) | Incorrect javadoc for return type of RetryPolicy#shouldRetry |  Minor | documentation | Nanda kumar | Nanda kumar |
| [YARN-7511](https://issues.apache.org/jira/browse/YARN-7511) | NPE in ContainerLocalizer when localization failed for running container |  Major | nodemanager | Tao Yang | Tao Yang |
| [MAPREDUCE-7023](https://issues.apache.org/jira/browse/MAPREDUCE-7023) | TestHadoopArchiveLogs.testCheckFilesAndSeedApps fails on rerun |  Minor | test | Gergely Novák | Gergely Novák |
| [HADOOP-15283](https://issues.apache.org/jira/browse/HADOOP-15283) | Upgrade from findbugs 3.0.1 to spotbugs 3.1.2 in branch-2 to fix docker image build |  Major | . | Xiao Chen | Akira Ajisaka |
| [HADOOP-15286](https://issues.apache.org/jira/browse/HADOOP-15286) | Remove unused imports from TestKMSWithZK.java |  Minor | test | Akira Ajisaka | Ajay Kumar |
| [HDFS-13040](https://issues.apache.org/jira/browse/HDFS-13040) | Kerberized inotify client fails despite kinit properly |  Major | namenode | Wei-Chiu Chuang | Xiao Chen |
| [YARN-7736](https://issues.apache.org/jira/browse/YARN-7736) | Fix itemization in YARN federation document |  Minor | documentation | Akira Ajisaka | Sen Zhao |
| [HDFS-13164](https://issues.apache.org/jira/browse/HDFS-13164) | File not closed if streamer fail with DSQuotaExceededException |  Major | hdfs-client | Xiao Chen | Xiao Chen |
| [HDFS-13109](https://issues.apache.org/jira/browse/HDFS-13109) | Support fully qualified hdfs path in EZ commands |  Major | hdfs | Hanisha Koneru | Hanisha Koneru |
| [MAPREDUCE-6930](https://issues.apache.org/jira/browse/MAPREDUCE-6930) | mapreduce.map.cpu.vcores and mapreduce.reduce.cpu.vcores are both present twice in mapred-default.xml |  Major | mrv2 | Daniel Templeton | Sen Zhao |
| [HDFS-10618](https://issues.apache.org/jira/browse/HDFS-10618) | TestPendingReconstruction#testPendingAndInvalidate is flaky due to race condition |  Major | . | Eric Badger | Eric Badger |
| [HDFS-10803](https://issues.apache.org/jira/browse/HDFS-10803) | TestBalancerWithMultipleNameNodes#testBalancing2OutOf3Blockpools fails intermittently due to no free space available |  Major | . | Yiqun Lin | Yiqun Lin |
| [HDFS-12156](https://issues.apache.org/jira/browse/HDFS-12156) | TestFSImage fails without -Pnative |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [HDFS-13261](https://issues.apache.org/jira/browse/HDFS-13261) | Fix incorrect null value check |  Minor | hdfs | Jianfei Jiang | Jianfei Jiang |
| [HDFS-12886](https://issues.apache.org/jira/browse/HDFS-12886) | Ignore minReplication for block recovery |  Major | hdfs, namenode | Lukas Majercak | Lukas Majercak |
| [YARN-8039](https://issues.apache.org/jira/browse/YARN-8039) | Clean up log dir configuration in TestLinuxContainerExecutorWithMocks.testStartLocalizer |  Minor | . | Miklos Szegedi | Miklos Szegedi |
| [HDFS-13296](https://issues.apache.org/jira/browse/HDFS-13296) | GenericTestUtils generates paths with drive letter in Windows and fail webhdfs related test cases |  Major | . | Xiao Liang | Xiao Liang |
| [HDFS-13268](https://issues.apache.org/jira/browse/HDFS-13268) | TestWebHdfsFileContextMainOperations fails on Windows |  Major | . | Íñigo Goiri | Xiao Liang |
| [YARN-8054](https://issues.apache.org/jira/browse/YARN-8054) | Improve robustness of the LocalDirsHandlerService MonitoringTimerTask thread |  Major | . | Jonathan Eagles | Jonathan Eagles |
| [YARN-7873](https://issues.apache.org/jira/browse/YARN-7873) | Revert YARN-6078 |  Blocker | . | Billie Rinaldi | Billie Rinaldi |
| [HDFS-13195](https://issues.apache.org/jira/browse/HDFS-13195) | DataNode conf page  cannot display the current value after reconfig |  Minor | datanode | maobaolong | maobaolong |
| [YARN-8063](https://issues.apache.org/jira/browse/YARN-8063) | DistributedShellTimelinePlugin wrongly check for entityId instead of entityType |  Major | . | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-8068](https://issues.apache.org/jira/browse/YARN-8068) | Application Priority field causes NPE in app timeline publish when Hadoop 2.7 based clients to 2.8+ |  Blocker | yarn | Sunil Govindan | Sunil Govindan |
| [HADOOP-12862](https://issues.apache.org/jira/browse/HADOOP-12862) | LDAP Group Mapping over SSL can not specify trust store |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-15317](https://issues.apache.org/jira/browse/HADOOP-15317) | Improve NetworkTopology chooseRandom's loop |  Major | . | Xiao Chen | Xiao Chen |
| [HADOOP-15355](https://issues.apache.org/jira/browse/HADOOP-15355) | TestCommonConfigurationFields is broken by HADOOP-15312 |  Major | test | Konstantin Shvachko | LiXin Ge |
| [HDFS-13176](https://issues.apache.org/jira/browse/HDFS-13176) | WebHdfs file path gets truncated when having semicolon (;) inside |  Major | webhdfs | Zsolt Venczel | Zsolt Venczel |
| [HADOOP-15375](https://issues.apache.org/jira/browse/HADOOP-15375) | Branch-2 pre-commit failed to build docker image |  Major | . | Xiao Chen | Xiao Chen |
| [HADOOP-15357](https://issues.apache.org/jira/browse/HADOOP-15357) | Configuration.getPropsWithPrefix no longer does variable substitution |  Major | . | Jim Brennan | Jim Brennan |
| [MAPREDUCE-7062](https://issues.apache.org/jira/browse/MAPREDUCE-7062) | Update mapreduce.job.tags description for making use for ATSv2 purpose. |  Major | . | Charan Hebri | Charan Hebri |
| [YARN-8073](https://issues.apache.org/jira/browse/YARN-8073) | TimelineClientImpl doesn't honor yarn.timeline-service.versions configuration |  Major | . | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-6629](https://issues.apache.org/jira/browse/YARN-6629) | NPE occurred when container allocation proposal is applied but its resource requests are removed before |  Critical | . | Tao Yang | Tao Yang |
| [HDFS-13427](https://issues.apache.org/jira/browse/HDFS-13427) | Fix the section titles of transparent encryption document |  Minor | documentation | Akira Ajisaka | Akira Ajisaka |
| [YARN-7527](https://issues.apache.org/jira/browse/YARN-7527) | Over-allocate node resource in async-scheduling mode of CapacityScheduler |  Major | capacityscheduler | Tao Yang | Tao Yang |
| [HDFS-7101](https://issues.apache.org/jira/browse/HDFS-7101) | Potential null dereference in DFSck#doWork() |  Minor | . | Ted Yu | skrho |
| [YARN-8120](https://issues.apache.org/jira/browse/YARN-8120) | JVM can crash with SIGSEGV when exiting due to custom leveldb logger |  Major | nodemanager, resourcemanager | Jason Lowe | Jason Lowe |
| [YARN-8147](https://issues.apache.org/jira/browse/YARN-8147) | TestClientRMService#testGetApplications sporadically fails |  Major | test | Jason Lowe | Jason Lowe |
| [HADOOP-14970](https://issues.apache.org/jira/browse/HADOOP-14970) | MiniHadoopClusterManager doesn't respect lack of format option |  Minor | . | Erik Krogen | Erik Krogen |
| [YARN-8156](https://issues.apache.org/jira/browse/YARN-8156) | Increase the default value of yarn.timeline-service.app-collector.linger-period.ms |  Major | . | Rohith Sharma K S | Charan Hebri |
| [YARN-8165](https://issues.apache.org/jira/browse/YARN-8165) | Incorrect queue name logging in AbstractContainerAllocator |  Trivial | capacityscheduler | Weiwei Yang | Weiwei Yang |
| [YARN-8164](https://issues.apache.org/jira/browse/YARN-8164) | Fix a potential NPE in AbstractSchedulerPlanFollower |  Major | . | lujie | lujie |
| [HDFS-12828](https://issues.apache.org/jira/browse/HDFS-12828) | OIV ReverseXML Processor fails with escaped characters |  Critical | hdfs | Erik Krogen | Erik Krogen |
| [HADOOP-15180](https://issues.apache.org/jira/browse/HADOOP-15180) | branch-2 : daemon processes' sysout overwrites 'ulimit -a' in daemon's out file |  Minor | scripts | Ranith Sardar | Ranith Sardar |
| [HADOOP-15396](https://issues.apache.org/jira/browse/HADOOP-15396) | Some java source files are executable |  Minor | . | Akira Ajisaka | Shashikant Banerjee |
| [YARN-6827](https://issues.apache.org/jira/browse/YARN-6827) | [ATS1/1.5] NPE exception while publishing recovering applications into ATS during RM restart. |  Major | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-7786](https://issues.apache.org/jira/browse/YARN-7786) | NullPointerException while launching ApplicationMaster |  Major | . | lujie | lujie |
| [HDFS-13408](https://issues.apache.org/jira/browse/HDFS-13408) | MiniDFSCluster to support being built on randomized base directory |  Major | test | Xiao Liang | Xiao Liang |
| [HADOOP-15390](https://issues.apache.org/jira/browse/HADOOP-15390) | Yarn RM logs flooded by DelegationTokenRenewer trying to renew KMS tokens |  Critical | . | Xiao Chen | Xiao Chen |
| [HDFS-13336](https://issues.apache.org/jira/browse/HDFS-13336) | Test cases of TestWriteToReplica failed in windows |  Major | . | Xiao Liang | Xiao Liang |
| [YARN-7598](https://issues.apache.org/jira/browse/YARN-7598) | Document how to use classpath isolation for aux-services in YARN |  Major | . | Xuan Gong | Xuan Gong |
| [HADOOP-15385](https://issues.apache.org/jira/browse/HADOOP-15385) | Many tests are failing in hadoop-distcp project in branch-2 |  Critical | tools/distcp | Rushabh S Shah | Jason Lowe |
| [MAPREDUCE-7042](https://issues.apache.org/jira/browse/MAPREDUCE-7042) | Killed MR job data does not move to mapreduce.jobhistory.done-dir when ATS v2 is enabled |  Major | . | Yesha Vora | Xuan Gong |
| [YARN-8205](https://issues.apache.org/jira/browse/YARN-8205) | Application State is not updated to ATS if AM launching is delayed. |  Critical | . | Sumana Sathish | Rohith Sharma K S |
| [MAPREDUCE-7072](https://issues.apache.org/jira/browse/MAPREDUCE-7072) | mapred job -history prints duplicate counter in human output |  Major | client | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [YARN-8221](https://issues.apache.org/jira/browse/YARN-8221) | RMWebServices also need to honor yarn.resourcemanager.display.per-user-apps |  Major | webapp | Sunil Govindan | Sunil Govindan |
| [HDFS-13509](https://issues.apache.org/jira/browse/HDFS-13509) | Bug fix for breakHardlinks() of ReplicaInfo/LocalReplica, and fix TestFileAppend failures on Windows |  Major | . | Xiao Liang | Xiao Liang |
| [MAPREDUCE-7073](https://issues.apache.org/jira/browse/MAPREDUCE-7073) | Optimize TokenCache#obtainTokensForNamenodesInternal |  Major | . | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-8232](https://issues.apache.org/jira/browse/YARN-8232) | RMContainer lost queue name when RM HA happens |  Major | resourcemanager | Hu Ziqian | Hu Ziqian |
| [HDFS-13537](https://issues.apache.org/jira/browse/HDFS-13537) | TestHdfsHelper does not generate jceks path properly for relative path in Windows |  Major | . | Xiao Liang | Xiao Liang |
| [HADOOP-15446](https://issues.apache.org/jira/browse/HADOOP-15446) | WASB: PageBlobInputStream.skip breaks HBASE replication |  Major | fs/azure | Thomas Marquardt | Thomas Marquardt |
| [YARN-8244](https://issues.apache.org/jira/browse/YARN-8244) |  TestContainerSchedulerQueuing.testStartMultipleContainers failed |  Major | . | Miklos Szegedi | Jim Brennan |
| [HDFS-13586](https://issues.apache.org/jira/browse/HDFS-13586) | Fsync fails on directories on Windows |  Critical | datanode, hdfs | Lukas Majercak | Lukas Majercak |
| [HDFS-13590](https://issues.apache.org/jira/browse/HDFS-13590) | Backport HDFS-12378 to branch-2 |  Major | datanode, hdfs, test | Lukas Majercak | Lukas Majercak |
| [HADOOP-15478](https://issues.apache.org/jira/browse/HADOOP-15478) | WASB: hflush() and hsync() regression |  Major | fs/azure | Thomas Marquardt | Thomas Marquardt |
| [HADOOP-15450](https://issues.apache.org/jira/browse/HADOOP-15450) | Avoid fsync storm triggered by DiskChecker and handle disk full situation |  Blocker | . | Kihwal Lee | Arpit Agarwal |
| [HDFS-13601](https://issues.apache.org/jira/browse/HDFS-13601) | Optimize ByteString conversions in PBHelper |  Major | . | Andrew Wang | Andrew Wang |
| [HDFS-13588](https://issues.apache.org/jira/browse/HDFS-13588) | Fix TestFsDatasetImpl test failures on Windows |  Major | . | Xiao Liang | Xiao Liang |
| [YARN-8310](https://issues.apache.org/jira/browse/YARN-8310) | Handle old NMTokenIdentifier, AMRMTokenIdentifier, and ContainerTokenIdentifier formats |  Major | . | Robert Kanter | Robert Kanter |
| [YARN-8344](https://issues.apache.org/jira/browse/YARN-8344) | Missing nm.stop() in TestNodeManagerResync to fix testKillContainersOnResync |  Major | . | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [YARN-8327](https://issues.apache.org/jira/browse/YARN-8327) | Fix TestAggregatedLogFormat#testReadAcontainerLogs1 on Windows |  Major | log-aggregation | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [YARN-8346](https://issues.apache.org/jira/browse/YARN-8346) | Upgrading to 3.1 kills running containers with error "Opportunistic container queue is full" |  Blocker | . | Rohith Sharma K S | Jason Lowe |
| [HDFS-13611](https://issues.apache.org/jira/browse/HDFS-13611) | Unsafe use of Text as a ConcurrentHashMap key in PBHelperClient |  Major | . | Andrew Wang | Andrew Wang |
| [HDFS-13618](https://issues.apache.org/jira/browse/HDFS-13618) | Fix TestDataNodeFaultInjector test failures on Windows |  Major | test | Xiao Liang | Xiao Liang |
| [HADOOP-15473](https://issues.apache.org/jira/browse/HADOOP-15473) | Configure serialFilter in KeyProvider to avoid UnrecoverableKeyException caused by JDK-8189997 |  Critical | kms | Gabor Bota | Gabor Bota |
| [HDFS-13626](https://issues.apache.org/jira/browse/HDFS-13626) | Fix incorrect username when deny the setOwner operation |  Minor | namenode | luhuachao | Zsolt Venczel |
| [MAPREDUCE-7103](https://issues.apache.org/jira/browse/MAPREDUCE-7103) | Fix TestHistoryViewerPrinter on windows due to a mismatch line separator |  Minor | . | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [YARN-8359](https://issues.apache.org/jira/browse/YARN-8359) | Exclude containermanager.linux test classes on Windows |  Major | . | Giovanni Matteo Fumarola | Jason Lowe |
| [HDFS-13664](https://issues.apache.org/jira/browse/HDFS-13664) | Refactor ConfiguredFailoverProxyProvider to make inheritance easier |  Minor | hdfs-client | Chao Sun | Chao Sun |
| [YARN-8405](https://issues.apache.org/jira/browse/YARN-8405) | RM zk-state-store.parent-path ACLs has been changed since HADOOP-14773 |  Major | . | Rohith Sharma K S | Íñigo Goiri |
| [MAPREDUCE-7108](https://issues.apache.org/jira/browse/MAPREDUCE-7108) | TestFileOutputCommitter fails on Windows |  Minor | test | Zuoming Zhang | Zuoming Zhang |
| [MAPREDUCE-7101](https://issues.apache.org/jira/browse/MAPREDUCE-7101) | Add config parameter to allow JHS to alway scan user dir irrespective of modTime |  Critical | . | Wangda Tan | Thomas Marquardt |
| [YARN-8404](https://issues.apache.org/jira/browse/YARN-8404) | Timeline event publish need to be async to avoid Dispatcher thread leak in case ATS is down |  Blocker | . | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-13675](https://issues.apache.org/jira/browse/HDFS-13675) | Speed up TestDFSAdminWithHA |  Major | hdfs, namenode | Lukas Majercak | Lukas Majercak |
| [HDFS-13673](https://issues.apache.org/jira/browse/HDFS-13673) | TestNameNodeMetrics fails on Windows |  Minor | test | Zuoming Zhang | Zuoming Zhang |
| [HDFS-13676](https://issues.apache.org/jira/browse/HDFS-13676) | TestEditLogRace fails on Windows |  Minor | test | Zuoming Zhang | Zuoming Zhang |
| [HADOOP-15523](https://issues.apache.org/jira/browse/HADOOP-15523) | Shell command timeout given is in seconds whereas it is taken as millisec while scheduling |  Major | . | Bilwa S T | Bilwa S T |
| [YARN-8444](https://issues.apache.org/jira/browse/YARN-8444) | NodeResourceMonitor crashes on bad swapFree value |  Major | . | Jim Brennan | Jim Brennan |
| [YARN-8457](https://issues.apache.org/jira/browse/YARN-8457) | Compilation is broken with -Pyarn-ui |  Major | webapp | Sunil Govindan | Sunil Govindan |
| [YARN-8401](https://issues.apache.org/jira/browse/YARN-8401) | [UI2] new ui is not accessible with out internet connection |  Blocker | . | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-8451](https://issues.apache.org/jira/browse/YARN-8451) | Multiple NM heartbeat thread created when a slow NM resync with RM |  Major | nodemanager | Botong Huang | Botong Huang |
| [HADOOP-15548](https://issues.apache.org/jira/browse/HADOOP-15548) | Randomize local dirs |  Minor | . | Jim Brennan | Jim Brennan |
| [YARN-8473](https://issues.apache.org/jira/browse/YARN-8473) | Containers being launched as app tears down can leave containers in NEW state |  Major | nodemanager | Jason Lowe | Jason Lowe |
| [HDFS-13729](https://issues.apache.org/jira/browse/HDFS-13729) | Fix broken links to RBF documentation |  Minor | documentation | jwhitter | Gabor Bota |
| [YARN-8515](https://issues.apache.org/jira/browse/YARN-8515) | container-executor can crash with SIGPIPE after nodemanager restart |  Major | . | Jim Brennan | Jim Brennan |
| [YARN-8421](https://issues.apache.org/jira/browse/YARN-8421) | when moving app, activeUsers is increased, even though app does not have outstanding request |  Major | . | kyungwan nam |  |
| [HADOOP-15614](https://issues.apache.org/jira/browse/HADOOP-15614) | TestGroupsCaching.testExceptionOnBackgroundRefreshHandled reliably fails |  Major | . | Kihwal Lee | Weiwei Yang |
| [YARN-4606](https://issues.apache.org/jira/browse/YARN-4606) | CapacityScheduler: applications could get starved because computation of #activeUsers considers pending apps |  Critical | capacity scheduler, capacityscheduler | Karam Singh | Manikandan R |
| [HADOOP-15637](https://issues.apache.org/jira/browse/HADOOP-15637) | LocalFs#listLocatedStatus does not filter out hidden .crc files |  Minor | fs | Erik Krogen | Erik Krogen |
| [HADOOP-15644](https://issues.apache.org/jira/browse/HADOOP-15644) | Hadoop Docker Image Pip Install Fails on branch-2 |  Critical | build | Haibo Chen | Haibo Chen |
| [YARN-6966](https://issues.apache.org/jira/browse/YARN-6966) | NodeManager metrics may return wrong negative values when NM restart |  Major | . | Yang Wang | Szilard Nemeth |
| [YARN-8331](https://issues.apache.org/jira/browse/YARN-8331) | Race condition in NM container launched after done |  Major | . | Yang Wang | Pradeep Ambati |
| [HDFS-13758](https://issues.apache.org/jira/browse/HDFS-13758) | DatanodeManager should throw exception if it has BlockRecoveryCommand but the block is not under construction |  Major | namenode | Wei-Chiu Chuang | chencan |
| [YARN-8612](https://issues.apache.org/jira/browse/YARN-8612) | Fix NM Collector Service Port issue in YarnConfiguration |  Major | ATSv2 | Prabha Manepalli | Prabha Manepalli |
| [HADOOP-15674](https://issues.apache.org/jira/browse/HADOOP-15674) | Test failure TestSSLHttpServer.testExcludedCiphers with TLS\_ECDHE\_RSA\_WITH\_AES\_128\_CBC\_SHA256 cipher suite |  Major | common | Gabor Bota | Szilard Nemeth |
| [YARN-8640](https://issues.apache.org/jira/browse/YARN-8640) | Restore previous state in container-executor after failure |  Major | . | Jim Brennan | Jim Brennan |
| [YARN-8679](https://issues.apache.org/jira/browse/YARN-8679) | [ATSv2] If HBase cluster is down for long time, high chances that NM ContainerManager dispatcher get blocked |  Major | . | Rohith Sharma K S | Wangda Tan |
| [HADOOP-14314](https://issues.apache.org/jira/browse/HADOOP-14314) | The OpenSolaris taxonomy link is dead in InterfaceClassification.md |  Major | documentation | Daniel Templeton | Rui Gao |
| [YARN-8649](https://issues.apache.org/jira/browse/YARN-8649) | NPE in localizer hearbeat processing if a container is killed while localizing |  Major | . | lujie | lujie |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-13337](https://issues.apache.org/jira/browse/HDFS-13337) | Backport HDFS-4275 to branch-2.9 |  Minor | . | Íñigo Goiri | Xiao Liang |
| [HDFS-13503](https://issues.apache.org/jira/browse/HDFS-13503) | Fix TestFsck test failures on Windows |  Major | hdfs | Xiao Liang | Xiao Liang |
| [HDFS-13542](https://issues.apache.org/jira/browse/HDFS-13542) | TestBlockManager#testNeededReplicationWhileAppending fails due to improper cluster shutdown in TestBlockManager#testBlockManagerMachinesArray on Windows |  Minor | . | Anbang Hu | Anbang Hu |
| [HDFS-13551](https://issues.apache.org/jira/browse/HDFS-13551) | TestMiniDFSCluster#testClusterSetStorageCapacity does not shut down cluster |  Minor | . | Anbang Hu | Anbang Hu |
| [HDFS-11700](https://issues.apache.org/jira/browse/HDFS-11700) | TestHDFSServerPorts#testBackupNodePorts doesn't pass on Windows |  Minor | . | Anbang Hu | Anbang Hu |
| [HDFS-13548](https://issues.apache.org/jira/browse/HDFS-13548) | TestResolveHdfsSymlink#testFcResolveAfs fails on Windows |  Minor | . | Anbang Hu | Anbang Hu |
| [HDFS-13567](https://issues.apache.org/jira/browse/HDFS-13567) | TestNameNodeMetrics#testGenerateEDEKTime,TestNameNodeMetrics#testResourceCheck should use a different cluster basedir |  Minor | . | Anbang Hu | Anbang Hu |
| [HDFS-13557](https://issues.apache.org/jira/browse/HDFS-13557) | TestDFSAdmin#testListOpenFiles fails on Windows |  Minor | . | Anbang Hu | Anbang Hu |
| [HDFS-13550](https://issues.apache.org/jira/browse/HDFS-13550) | TestDebugAdmin#testComputeMetaCommand fails on Windows |  Minor | . | Anbang Hu | Anbang Hu |
| [HDFS-13559](https://issues.apache.org/jira/browse/HDFS-13559) | TestBlockScanner does not close TestContext properly |  Minor | . | Anbang Hu | Anbang Hu |
| [HDFS-13570](https://issues.apache.org/jira/browse/HDFS-13570) | TestQuotaByStorageType,TestQuota,TestDFSOutputStream fail on Windows |  Minor | . | Anbang Hu | Anbang Hu |
| [HDFS-13558](https://issues.apache.org/jira/browse/HDFS-13558) | TestDatanodeHttpXFrame does not shut down cluster |  Minor | . | Anbang Hu | Anbang Hu |
| [HDFS-13554](https://issues.apache.org/jira/browse/HDFS-13554) | TestDatanodeRegistration#testForcedRegistration does not shut down cluster |  Minor | . | Anbang Hu | Anbang Hu |
| [HDFS-13556](https://issues.apache.org/jira/browse/HDFS-13556) | TestNestedEncryptionZones does not shut down cluster |  Minor | . | Anbang Hu | Anbang Hu |
| [HDFS-13560](https://issues.apache.org/jira/browse/HDFS-13560) | Insufficient system resources exist to complete the requested service for some tests on Windows |  Minor | . | Anbang Hu | Anbang Hu |
| [HDFS-13592](https://issues.apache.org/jira/browse/HDFS-13592) | TestNameNodePrunesMissingStorages#testNameNodePrunesUnreportedStorages does not shut down cluster properly |  Minor | . | Anbang Hu | Anbang Hu |
| [HDFS-13593](https://issues.apache.org/jira/browse/HDFS-13593) | TestBlockReaderLocalLegacy#testBlockReaderLocalLegacyWithAppend fails on Windows |  Minor | test | Anbang Hu | Anbang Hu |
| [HDFS-13587](https://issues.apache.org/jira/browse/HDFS-13587) | TestQuorumJournalManager fails on Windows |  Minor | . | Anbang Hu | Anbang Hu |
| [HDFS-13620](https://issues.apache.org/jira/browse/HDFS-13620) | Randomize the test directory path for TestHDFSFileSystemContract |  Minor | . | Anbang Hu | Anbang Hu |
| [HDFS-13591](https://issues.apache.org/jira/browse/HDFS-13591) | TestDFSShell#testSetrepLow fails on Windows |  Minor | . | Anbang Hu | Anbang Hu |
| [HDFS-13632](https://issues.apache.org/jira/browse/HDFS-13632) | Randomize baseDir for MiniJournalCluster in MiniQJMHACluster for TestDFSAdminWithHA |  Minor | . | Anbang Hu | Anbang Hu |
| [MAPREDUCE-7102](https://issues.apache.org/jira/browse/MAPREDUCE-7102) | Fix TestJavaSerialization for Windows due a mismatch line separator |  Minor | . | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [HDFS-13652](https://issues.apache.org/jira/browse/HDFS-13652) | Randomize baseDir for MiniDFSCluster in TestBlockScanner |  Minor | . | Anbang Hu | Anbang Hu |
| [YARN-8370](https://issues.apache.org/jira/browse/YARN-8370) | Some Node Manager tests fail on Windows due to improper path/file separator |  Minor | . | Anbang Hu | Anbang Hu |
| [YARN-8422](https://issues.apache.org/jira/browse/YARN-8422) | TestAMSimulator failing with NPE |  Minor | . | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [HADOOP-15532](https://issues.apache.org/jira/browse/HADOOP-15532) | TestBasicDiskValidator fails with NoSuchFileException |  Minor | . | Íñigo Goiri | Giovanni Matteo Fumarola |
| [HDFS-13563](https://issues.apache.org/jira/browse/HDFS-13563) | TestDFSAdminWithHA times out on Windows |  Minor | . | Anbang Hu | Lukas Majercak |
| [HDFS-13681](https://issues.apache.org/jira/browse/HDFS-13681) | Fix TestStartup.testNNFailToStartOnReadOnlyNNDir test failure on Windows |  Major | test | Xiao Liang | Xiao Liang |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-14799](https://issues.apache.org/jira/browse/HADOOP-14799) | Update nimbus-jose-jwt to 4.41.1 |  Major | . | Ray Chiang | Ray Chiang |
| [HADOOP-14997](https://issues.apache.org/jira/browse/HADOOP-14997) |  Add hadoop-aliyun as dependency of hadoop-cloud-storage |  Minor | fs/oss | Genmao Yu | Genmao Yu |
| [HDFS-12801](https://issues.apache.org/jira/browse/HDFS-12801) | RBF: Set MountTableResolver as default file resolver |  Minor | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-7430](https://issues.apache.org/jira/browse/YARN-7430) | Enable user re-mapping for Docker containers by default |  Blocker | security, yarn | Eric Yang | Eric Yang |
| [HADOOP-15024](https://issues.apache.org/jira/browse/HADOOP-15024) | AliyunOSS: support user agent configuration and include that & Hadoop version information to oss server |  Major | fs, fs/oss | Sammi Chen | Sammi Chen |
| [HDFS-12858](https://issues.apache.org/jira/browse/HDFS-12858) | RBF: Add router admin commands usage in HDFS commands reference doc |  Minor | documentation | Yiqun Lin | Yiqun Lin |
| [HDFS-12835](https://issues.apache.org/jira/browse/HDFS-12835) | RBF: Fix Javadoc parameter errors |  Minor | . | Wei Yan | Wei Yan |
| [HDFS-12396](https://issues.apache.org/jira/browse/HDFS-12396) | Webhdfs file system should get delegation token from kms provider. |  Major | encryption, kms, webhdfs | Rushabh S Shah | Rushabh S Shah |
| [YARN-6704](https://issues.apache.org/jira/browse/YARN-6704) | Add support for work preserving NM restart when FederationInterceptor is enabled in AMRMProxyService |  Major | . | Botong Huang | Botong Huang |
| [HDFS-12875](https://issues.apache.org/jira/browse/HDFS-12875) | RBF: Complete logic for -readonly option of dfsrouteradmin add command |  Major | . | Yiqun Lin | Íñigo Goiri |
| [YARN-7630](https://issues.apache.org/jira/browse/YARN-7630) | Fix AMRMToken rollover handling in AMRMProxy |  Minor | . | Botong Huang | Botong Huang |
| [HDFS-12937](https://issues.apache.org/jira/browse/HDFS-12937) | RBF: Add more unit tests for router admin commands |  Major | test | Yiqun Lin | Yiqun Lin |
| [YARN-7032](https://issues.apache.org/jira/browse/YARN-7032) | [ATSv2] NPE while starting hbase co-processor when HBase authorization is enabled. |  Critical | . | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-12988](https://issues.apache.org/jira/browse/HDFS-12988) | RBF: Mount table entries not properly updated in the local cache |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-12802](https://issues.apache.org/jira/browse/HDFS-12802) | RBF: Control MountTableResolver cache size |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-12934](https://issues.apache.org/jira/browse/HDFS-12934) | RBF: Federation supports global quota |  Major | . | Yiqun Lin | Yiqun Lin |
| [HDFS-12972](https://issues.apache.org/jira/browse/HDFS-12972) | RBF: Display mount table quota info in Web UI and admin command |  Major | . | Yiqun Lin | Yiqun Lin |
| [YARN-6736](https://issues.apache.org/jira/browse/YARN-6736) | Consider writing to both ats v1 & v2 from RM for smoother upgrades |  Major | timelineserver | Vrushali C | Aaron Gresch |
| [HADOOP-15027](https://issues.apache.org/jira/browse/HADOOP-15027) | AliyunOSS: Support multi-thread pre-read to improve sequential read from Hadoop to Aliyun OSS performance |  Major | fs/oss | wujinhu | wujinhu |
| [HDFS-12973](https://issues.apache.org/jira/browse/HDFS-12973) | RBF: Document global quota supporting in federation |  Major | . | Yiqun Lin | Yiqun Lin |
| [HDFS-13028](https://issues.apache.org/jira/browse/HDFS-13028) | RBF: Fix spurious TestRouterRpc#testProxyGetStats |  Minor | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-12772](https://issues.apache.org/jira/browse/HDFS-12772) | RBF: Federation Router State State Store internal API |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13042](https://issues.apache.org/jira/browse/HDFS-13042) | RBF: Heartbeat Router State |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13049](https://issues.apache.org/jira/browse/HDFS-13049) | RBF: Inconsistent Router OPTS config in branch-2 and branch-3 |  Minor | . | Wei Yan | Wei Yan |
| [HDFS-12574](https://issues.apache.org/jira/browse/HDFS-12574) | Add CryptoInputStream to WebHdfsFileSystem read call. |  Major | encryption, kms, webhdfs | Rushabh S Shah | Rushabh S Shah |
| [HDFS-13044](https://issues.apache.org/jira/browse/HDFS-13044) | RBF: Add a safe mode for the Router |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13043](https://issues.apache.org/jira/browse/HDFS-13043) | RBF: Expose the state of the Routers in the federation |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13068](https://issues.apache.org/jira/browse/HDFS-13068) | RBF: Add router admin option to manage safe mode |  Major | . | Íñigo Goiri | Yiqun Lin |
| [HDFS-13119](https://issues.apache.org/jira/browse/HDFS-13119) | RBF: Manage unavailable clusters |  Major | . | Íñigo Goiri | Yiqun Lin |
| [HDFS-13187](https://issues.apache.org/jira/browse/HDFS-13187) | RBF: Fix Routers information shown in the web UI |  Minor | . | Wei Yan | Wei Yan |
| [HDFS-13184](https://issues.apache.org/jira/browse/HDFS-13184) | RBF: Improve the unit test TestRouterRPCClientRetries |  Minor | test | Yiqun Lin | Yiqun Lin |
| [HDFS-13199](https://issues.apache.org/jira/browse/HDFS-13199) | RBF: Fix the hdfs router page missing label icon issue |  Major | federation, hdfs | maobaolong | maobaolong |
| [HADOOP-15090](https://issues.apache.org/jira/browse/HADOOP-15090) | Add ADL troubleshooting doc |  Major | documentation, fs/adl | Steve Loughran | Steve Loughran |
| [YARN-7919](https://issues.apache.org/jira/browse/YARN-7919) | Refactor timelineservice-hbase module into submodules |  Major | timelineservice | Haibo Chen | Haibo Chen |
| [YARN-8003](https://issues.apache.org/jira/browse/YARN-8003) | Backport the code structure changes in YARN-7346 to branch-2 |  Major | . | Haibo Chen | Haibo Chen |
| [HDFS-13214](https://issues.apache.org/jira/browse/HDFS-13214) | RBF: Complete document of Router configuration |  Major | . | Tao Jie | Yiqun Lin |
| [HADOOP-15267](https://issues.apache.org/jira/browse/HADOOP-15267) | S3A multipart upload fails when SSE-C encryption is enabled |  Critical | fs/s3 | Anis Elleuch | Anis Elleuch |
| [HDFS-13230](https://issues.apache.org/jira/browse/HDFS-13230) | RBF: ConnectionManager's cleanup task will compare each pool's own active conns with its total conns |  Minor | . | Wei Yan | Chao Sun |
| [HDFS-13233](https://issues.apache.org/jira/browse/HDFS-13233) | RBF: MountTableResolver doesn't return the correct mount point of the given path |  Major | hdfs | wangzhiyuan | wangzhiyuan |
| [HDFS-13212](https://issues.apache.org/jira/browse/HDFS-13212) | RBF: Fix router location cache issue |  Major | federation, hdfs | Weiwei Wu | Weiwei Wu |
| [HDFS-13232](https://issues.apache.org/jira/browse/HDFS-13232) | RBF: ConnectionPool should return first usable connection |  Minor | . | Wei Yan | Ekanth Sethuramalingam |
| [HDFS-13240](https://issues.apache.org/jira/browse/HDFS-13240) | RBF: Update some inaccurate document descriptions |  Minor | . | Yiqun Lin | Yiqun Lin |
| [HDFS-11399](https://issues.apache.org/jira/browse/HDFS-11399) | Many tests fails in Windows due to injecting disk failures |  Major | . | Yiqun Lin | Yiqun Lin |
| [HDFS-13241](https://issues.apache.org/jira/browse/HDFS-13241) | RBF: TestRouterSafemode failed if the port 8888 is in use |  Major | hdfs, test | maobaolong | maobaolong |
| [HDFS-13253](https://issues.apache.org/jira/browse/HDFS-13253) | RBF: Quota management incorrect parent-child relationship judgement |  Major | . | Yiqun Lin | Yiqun Lin |
| [HDFS-13226](https://issues.apache.org/jira/browse/HDFS-13226) | RBF: Throw the exception if mount table entry validated failed |  Major | hdfs | maobaolong | maobaolong |
| [HADOOP-15308](https://issues.apache.org/jira/browse/HADOOP-15308) | TestConfiguration fails on Windows because of paths |  Major | test | Íñigo Goiri | Xiao Liang |
| [HDFS-12773](https://issues.apache.org/jira/browse/HDFS-12773) | RBF: Improve State Store FS implementation |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13198](https://issues.apache.org/jira/browse/HDFS-13198) | RBF: RouterHeartbeatService throws out CachedStateStore related exceptions when starting router |  Minor | . | Wei Yan | Wei Yan |
| [HDFS-13224](https://issues.apache.org/jira/browse/HDFS-13224) | RBF: Resolvers to support mount points across multiple subclusters |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HADOOP-15262](https://issues.apache.org/jira/browse/HADOOP-15262) | AliyunOSS: move files under a directory in parallel when rename a directory |  Major | fs/oss | wujinhu | wujinhu |
| [HDFS-13215](https://issues.apache.org/jira/browse/HDFS-13215) | RBF: Move Router to its own module |  Major | . | Íñigo Goiri | Wei Yan |
| [HDFS-13307](https://issues.apache.org/jira/browse/HDFS-13307) | RBF: Improve the use of setQuota command |  Major | . | liuhongtong | liuhongtong |
| [HDFS-13250](https://issues.apache.org/jira/browse/HDFS-13250) | RBF: Router to manage requests across multiple subclusters |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13318](https://issues.apache.org/jira/browse/HDFS-13318) | RBF: Fix FindBugs in hadoop-hdfs-rbf |  Minor | . | Íñigo Goiri | Ekanth Sethuramalingam |
| [HDFS-12792](https://issues.apache.org/jira/browse/HDFS-12792) | RBF: Test Router-based federation using HDFSContract |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-7581](https://issues.apache.org/jira/browse/YARN-7581) | HBase filters are not constructed correctly in ATSv2 |  Major | ATSv2 | Haibo Chen | Haibo Chen |
| [YARN-7986](https://issues.apache.org/jira/browse/YARN-7986) | ATSv2 REST API queries do not return results for uppercase application tags |  Critical | . | Charan Hebri | Charan Hebri |
| [HDFS-12512](https://issues.apache.org/jira/browse/HDFS-12512) | RBF: Add WebHDFS |  Major | fs | Íñigo Goiri | Wei Yan |
| [HDFS-13291](https://issues.apache.org/jira/browse/HDFS-13291) | RBF: Implement available space based OrderResolver |  Major | . | Yiqun Lin | Yiqun Lin |
| [HDFS-13204](https://issues.apache.org/jira/browse/HDFS-13204) | RBF: Optimize name service safe mode icon |  Minor | . | liuhongtong | liuhongtong |
| [HDFS-13352](https://issues.apache.org/jira/browse/HDFS-13352) | RBF: Add xsl stylesheet for hdfs-rbf-default.xml |  Major | documentation | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-8010](https://issues.apache.org/jira/browse/YARN-8010) | Add config in FederationRMFailoverProxy to not bypass facade cache when failing over |  Minor | . | Botong Huang | Botong Huang |
| [HDFS-13347](https://issues.apache.org/jira/browse/HDFS-13347) | RBF: Cache datanode reports |  Minor | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13289](https://issues.apache.org/jira/browse/HDFS-13289) | RBF: TestConnectionManager#testCleanup() test case need correction |  Minor | . | Dibyendu Karmakar | Dibyendu Karmakar |
| [HDFS-13364](https://issues.apache.org/jira/browse/HDFS-13364) | RBF: Support NamenodeProtocol in the Router |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-6936](https://issues.apache.org/jira/browse/YARN-6936) | [Atsv2] Retrospect storing entities into sub application table from client perspective |  Major | . | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-13353](https://issues.apache.org/jira/browse/HDFS-13353) | RBF: TestRouterWebHDFSContractCreate failed |  Major | test | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-8107](https://issues.apache.org/jira/browse/YARN-8107) | Give an informative message when incorrect format is used in ATSv2 filter attributes |  Major | ATSv2 | Charan Hebri | Rohith Sharma K S |
| [YARN-8110](https://issues.apache.org/jira/browse/YARN-8110) | AMRMProxy recover should catch for all throwable to avoid premature exit |  Major | . | Botong Huang | Botong Huang |
| [HDFS-13402](https://issues.apache.org/jira/browse/HDFS-13402) | RBF: Fix  java doc for StateStoreFileSystemImpl |  Minor | hdfs | Yiran Wu | Yiran Wu |
| [HDFS-13380](https://issues.apache.org/jira/browse/HDFS-13380) | RBF: mv/rm fail after the directory exceeded the quota limit |  Major | . | Weiwei Wu | Yiqun Lin |
| [HDFS-13410](https://issues.apache.org/jira/browse/HDFS-13410) | RBF: Support federation with no subclusters |  Minor | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13384](https://issues.apache.org/jira/browse/HDFS-13384) | RBF: Improve timeout RPC call mechanism |  Minor | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13045](https://issues.apache.org/jira/browse/HDFS-13045) | RBF: Improve error message returned from subcluster |  Minor | . | Wei Yan | Íñigo Goiri |
| [HDFS-13428](https://issues.apache.org/jira/browse/HDFS-13428) | RBF: Remove LinkedList From StateStoreFileImpl.java |  Trivial | federation | BELUGA BEHR | BELUGA BEHR |
| [HADOOP-14999](https://issues.apache.org/jira/browse/HADOOP-14999) | AliyunOSS: provide one asynchronous multi-part based uploading mechanism |  Major | fs/oss | Genmao Yu | Genmao Yu |
| [YARN-7810](https://issues.apache.org/jira/browse/YARN-7810) | TestDockerContainerRuntime test failures due to UID lookup of a non-existent user |  Major | . | Shane Kumpf | Shane Kumpf |
| [HDFS-13435](https://issues.apache.org/jira/browse/HDFS-13435) | RBF: Improve the error loggings for printing the stack trace |  Major | . | Yiqun Lin | Yiqun Lin |
| [YARN-7189](https://issues.apache.org/jira/browse/YARN-7189) | Container-executor doesn't remove Docker containers that error out early |  Major | yarn | Eric Badger | Eric Badger |
| [HDFS-13466](https://issues.apache.org/jira/browse/HDFS-13466) | RBF: Add more router-related information to the UI |  Minor | . | Wei Yan | Wei Yan |
| [HDFS-13453](https://issues.apache.org/jira/browse/HDFS-13453) | RBF: getMountPointDates should fetch latest subdir time/date when parent dir is not present but /parent/child dirs are present in mount table |  Major | . | Dibyendu Karmakar | Dibyendu Karmakar |
| [HDFS-13478](https://issues.apache.org/jira/browse/HDFS-13478) | RBF: Disabled Nameservice store API |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13490](https://issues.apache.org/jira/browse/HDFS-13490) | RBF: Fix setSafeMode in the Router |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13484](https://issues.apache.org/jira/browse/HDFS-13484) | RBF: Disable Nameservices from the federation |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13326](https://issues.apache.org/jira/browse/HDFS-13326) | RBF: Improve the interfaces to modify and view mount tables |  Minor | . | Wei Yan | Gang Li |
| [HDFS-13499](https://issues.apache.org/jira/browse/HDFS-13499) | RBF: Show disabled name services in the UI |  Minor | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-8215](https://issues.apache.org/jira/browse/YARN-8215) | ATS v2 returns invalid YARN\_CONTAINER\_ALLOCATED\_HOST\_HTTP\_ADDRESS from NM |  Critical | ATSv2 | Yesha Vora | Rohith Sharma K S |
| [HDFS-13508](https://issues.apache.org/jira/browse/HDFS-13508) | RBF: Normalize paths (automatically) when adding, updating, removing or listing mount table entries |  Minor | . | Ekanth Sethuramalingam | Ekanth Sethuramalingam |
| [HDFS-13434](https://issues.apache.org/jira/browse/HDFS-13434) | RBF: Fix dead links in RBF document |  Major | documentation | Akira Ajisaka | Chetna Chaudhari |
| [HDFS-13488](https://issues.apache.org/jira/browse/HDFS-13488) | RBF: Reject requests when a Router is overloaded |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13525](https://issues.apache.org/jira/browse/HDFS-13525) | RBF: Add unit test TestStateStoreDisabledNameservice |  Major | . | Yiqun Lin | Yiqun Lin |
| [YARN-8253](https://issues.apache.org/jira/browse/YARN-8253) | HTTPS Ats v2 api call fails with "bad HTTP parsed" |  Critical | ATSv2 | Yesha Vora | Charan Hebri |
| [HADOOP-15454](https://issues.apache.org/jira/browse/HADOOP-15454) | TestRollingFileSystemSinkWithLocal fails on Windows |  Major | test | Xiao Liang | Xiao Liang |
| [HDFS-13346](https://issues.apache.org/jira/browse/HDFS-13346) | RBF: Fix synchronization of router quota and nameservice quota |  Major | . | liuhongtong | Yiqun Lin |
| [YARN-8247](https://issues.apache.org/jira/browse/YARN-8247) | Incorrect HTTP status code returned by ATSv2 for non-whitelisted users |  Critical | ATSv2 | Charan Hebri | Rohith Sharma K S |
| [YARN-8130](https://issues.apache.org/jira/browse/YARN-8130) | Race condition when container events are published for KILLED applications |  Major | ATSv2 | Charan Hebri | Rohith Sharma K S |
| [YARN-7900](https://issues.apache.org/jira/browse/YARN-7900) | [AMRMProxy] AMRMClientRelayer for stateful FederationInterceptor |  Major | . | Botong Huang | Botong Huang |
| [HADOOP-15498](https://issues.apache.org/jira/browse/HADOOP-15498) | TestHadoopArchiveLogs (#testGenerateScript, #testPrepareWorkingDir) fails on Windows |  Minor | . | Anbang Hu | Anbang Hu |
| [HDFS-13637](https://issues.apache.org/jira/browse/HDFS-13637) | RBF: Router fails when threadIndex (in ConnectionPool) wraps around Integer.MIN\_VALUE |  Critical | federation | CR Hota | CR Hota |
| [YARN-4781](https://issues.apache.org/jira/browse/YARN-4781) | Support intra-queue preemption for fairness ordering policy. |  Major | scheduler | Wangda Tan | Eric Payne |
| [HADOOP-15506](https://issues.apache.org/jira/browse/HADOOP-15506) | Upgrade Azure Storage Sdk version to 7.0.0 and update corresponding code blocks |  Minor | fs/azure | Esfandiar Manii | Esfandiar Manii |
| [HADOOP-15529](https://issues.apache.org/jira/browse/HADOOP-15529) | ContainerLaunch#testInvalidEnvVariableSubstitutionType is not supported in Windows |  Minor | . | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [HADOOP-15533](https://issues.apache.org/jira/browse/HADOOP-15533) | Make WASB listStatus messages consistent |  Trivial | fs/azure | Esfandiar Manii | Esfandiar Manii |
| [HADOOP-15458](https://issues.apache.org/jira/browse/HADOOP-15458) | TestLocalFileSystem#testFSOutputStreamBuilder fails on Windows |  Minor | test | Xiao Liang | Xiao Liang |
| [HDFS-13528](https://issues.apache.org/jira/browse/HDFS-13528) | RBF: If a directory exceeds quota limit then quota usage is not refreshed for other mount entries |  Major | . | Dibyendu Karmakar | Dibyendu Karmakar |
| [HDFS-13710](https://issues.apache.org/jira/browse/HDFS-13710) | RBF:  setQuota and getQuotaUsage should check the dfs.federation.router.quota.enable |  Major | federation, hdfs | yanghuafeng | yanghuafeng |
| [HDFS-13726](https://issues.apache.org/jira/browse/HDFS-13726) | RBF: Fix RBF configuration links |  Minor | documentation | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-13475](https://issues.apache.org/jira/browse/HDFS-13475) | RBF: Admin cannot enforce Router enter SafeMode |  Major | . | Wei Yan | Chao Sun |
| [HDFS-13733](https://issues.apache.org/jira/browse/HDFS-13733) | RBF: Add Web UI configurations and descriptions to RBF document |  Minor | documentation | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-13743](https://issues.apache.org/jira/browse/HDFS-13743) | RBF: Router throws NullPointerException due to the invalid initialization of MountTableResolver |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-13583](https://issues.apache.org/jira/browse/HDFS-13583) | RBF: Router admin clrQuota is not synchronized with nameservice |  Major | . | Dibyendu Karmakar | Dibyendu Karmakar |
| [HDFS-13750](https://issues.apache.org/jira/browse/HDFS-13750) | RBF: Router ID in RouterRpcClient is always null |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-8129](https://issues.apache.org/jira/browse/YARN-8129) | Improve error message for invalid value in fields attribute |  Minor | ATSv2 | Charan Hebri | Abhishek Modi |
| [HDFS-13848](https://issues.apache.org/jira/browse/HDFS-13848) | Refactor NameNode failover proxy providers |  Major | ha, hdfs-client | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-13634](https://issues.apache.org/jira/browse/HDFS-13634) | RBF: Configurable value in xml for async connection request queue size. |  Major | federation | CR Hota | CR Hota |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-15149](https://issues.apache.org/jira/browse/HADOOP-15149) | CryptoOutputStream should implement StreamCapabilities |  Major | fs | Mike Drob | Xiao Chen |
| [HADOOP-15177](https://issues.apache.org/jira/browse/HADOOP-15177) | Update the release year to 2018 |  Blocker | build | Akira Ajisaka | Bharat Viswanadham |
| [YARN-8412](https://issues.apache.org/jira/browse/YARN-8412) | Move ResourceRequest.clone logic everywhere into a proper API |  Minor | . | Botong Huang | Botong Huang |
