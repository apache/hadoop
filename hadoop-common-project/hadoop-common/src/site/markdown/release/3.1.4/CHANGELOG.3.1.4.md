
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

## Release 3.1.4 - 2020-07-21



### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-9761](https://issues.apache.org/jira/browse/YARN-9761) | Allow overriding application submissions based on server side configs |  Major | . | Jonathan Hung | pralabhkumar |
| [YARN-9760](https://issues.apache.org/jira/browse/YARN-9760) | Support configuring application priorities on a workflow level |  Major | . | Jonathan Hung | Varun Saxena |
| [HDFS-14745](https://issues.apache.org/jira/browse/HDFS-14745) | Backport HDFS persistent memory read cache support to branch-3.1 |  Major | . | Feilong He | Feilong He |
| [HDFS-12943](https://issues.apache.org/jira/browse/HDFS-12943) | Consistent Reads from Standby Node |  Major | hdfs | Konstantin Shvachko | Konstantin Shvachko |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-8361](https://issues.apache.org/jira/browse/YARN-8361) | Change App Name Placement Rule to use App Name instead of App Id for configuration |  Major | yarn | Zian Chen | Zian Chen |
| [YARN-8750](https://issues.apache.org/jira/browse/YARN-8750) | Refactor TestQueueMetrics |  Minor | resourcemanager | Szilard Nemeth | Szilard Nemeth |
| [HADOOP-15849](https://issues.apache.org/jira/browse/HADOOP-15849) | Upgrade netty version to 3.10.6 |  Major | . | Xiao Chen | Xiao Chen |
| [HDFS-14064](https://issues.apache.org/jira/browse/HDFS-14064) | WEBHDFS: Support Enable/Disable EC Policy |  Major | erasure-coding, webhdfs | Ayush Saxena | Ayush Saxena |
| [HDFS-14113](https://issues.apache.org/jira/browse/HDFS-14113) | EC : Add Configuration to restrict UserDefined Policies |  Major | erasure-coding | Ayush Saxena | Ayush Saxena |
| [HDFS-14124](https://issues.apache.org/jira/browse/HDFS-14124) | EC : Support EC Commands (set/get/unset EcPolicy) via WebHdfs |  Major | erasure-coding, httpfs, webhdfs | Souryakanta Dwivedy | Ayush Saxena |
| [HDFS-14006](https://issues.apache.org/jira/browse/HDFS-14006) | Refactor name node to allow different token verification implementations |  Major | . | CR Hota | CR Hota |
| [HADOOP-15909](https://issues.apache.org/jira/browse/HADOOP-15909) | KeyProvider class should implement Closeable |  Major | kms | Kuhu Shukla | Kuhu Shukla |
| [HDFS-14187](https://issues.apache.org/jira/browse/HDFS-14187) | Make warning message more clear when there are not enough data nodes for EC write |  Major | erasure-coding | Kitti Nanasi | Kitti Nanasi |
| [HDFS-14235](https://issues.apache.org/jira/browse/HDFS-14235) | Handle ArrayIndexOutOfBoundsException in DataNodeDiskMetrics#slowDiskDetectionDaemon |  Major | . | Surendra Singh Lilhore | Ranith Sardar |
| [HADOOP-16126](https://issues.apache.org/jira/browse/HADOOP-16126) | ipc.Client.stop() may sleep too long to wait for all connections |  Major | ipc | Tsz-wo Sze | Tsz-wo Sze |
| [HADOOP-16140](https://issues.apache.org/jira/browse/HADOOP-16140) | hadoop fs expunge to add -immediate option to purge trash immediately |  Major | fs | Stephen O'Donnell | Stephen O'Donnell |
| [HADOOP-15014](https://issues.apache.org/jira/browse/HADOOP-15014) | KMS should log the IP address of the clients |  Major | kms | Zsombor Gegesy | Zsombor Gegesy |
| [HDFS-14460](https://issues.apache.org/jira/browse/HDFS-14460) | DFSUtil#getNamenodeWebAddr should return HTTPS address based on policy configured |  Major | . | CR Hota | CR Hota |
| [HDFS-14624](https://issues.apache.org/jira/browse/HDFS-14624) | When decommissioning a node, log remaining blocks to replicate periodically |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-13693](https://issues.apache.org/jira/browse/HDFS-13693) | Remove unnecessary search in INodeDirectory.addChild during image loading |  Major | namenode | zhouyingchao | Lisheng Sun |
| [HDFS-14313](https://issues.apache.org/jira/browse/HDFS-14313) | Get hdfs used space from FsDatasetImpl#volumeMap#ReplicaInfo in memory  instead of df/du |  Major | datanode, performance | Lisheng Sun | Lisheng Sun |
| [HDFS-14678](https://issues.apache.org/jira/browse/HDFS-14678) | Allow triggerBlockReport to a specific namenode |  Major | datanode | Leon Gao | Leon Gao |
| [HDFS-14523](https://issues.apache.org/jira/browse/HDFS-14523) | Remove excess read lock for NetworkToplogy |  Major | . | Wu Weiwei | Wu Weiwei |
| [HDFS-14497](https://issues.apache.org/jira/browse/HDFS-14497) | Write lock held by metasave impact following RPC processing |  Major | namenode | Xiaoqiao He | Xiaoqiao He |
| [YARN-9756](https://issues.apache.org/jira/browse/YARN-9756) | Create metric that sums total memory/vcores preempted per round |  Major | capacity scheduler | Eric Payne | Manikandan R |
| [YARN-9810](https://issues.apache.org/jira/browse/YARN-9810) | Add queue capacity/maxcapacity percentage metrics |  Major | . | Jonathan Hung | Shubham Gupta |
| [HADOOP-16531](https://issues.apache.org/jira/browse/HADOOP-16531) | Log more detail for slow RPC |  Major | . | Chen Zhang | Chen Zhang |
| [YARN-9763](https://issues.apache.org/jira/browse/YARN-9763) | Print application tags in application summary |  Major | . | Jonathan Hung | Manoj Kumar |
| [YARN-9764](https://issues.apache.org/jira/browse/YARN-9764) | Print application submission context label in application summary |  Major | . | Jonathan Hung | Manoj Kumar |
| [YARN-9824](https://issues.apache.org/jira/browse/YARN-9824) | Fall back to configured queue ordering policy class name |  Major | . | Jonathan Hung | Jonathan Hung |
| [HADOOP-16069](https://issues.apache.org/jira/browse/HADOOP-16069) | Support configure ZK\_DTSM\_ZK\_KERBEROS\_PRINCIPAL in ZKDelegationTokenSecretManager using principal with Schema /\_HOST |  Minor | common | luhuachao | luhuachao |
| [YARN-9762](https://issues.apache.org/jira/browse/YARN-9762) | Add submission context label to audit logs |  Major | . | Jonathan Hung | Manoj Kumar |
| [HDFS-14850](https://issues.apache.org/jira/browse/HDFS-14850) | Optimize FileSystemAccessService#getFileSystemConfiguration |  Major | httpfs, performance | Lisheng Sun | Lisheng Sun |
| [HDFS-14192](https://issues.apache.org/jira/browse/HDFS-14192) | Track missing DFS operations in Statistics and StorageStatistics |  Major | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-16625](https://issues.apache.org/jira/browse/HADOOP-16625) | Backport HADOOP-14624 to branch-3.1 |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-9356](https://issues.apache.org/jira/browse/YARN-9356) | Add more tests to ratio method in TestResourceCalculator |  Major | . | Szilard Nemeth | Zoltan Siegl |
| [HDFS-14915](https://issues.apache.org/jira/browse/HDFS-14915) | Move Superuser Check Before Taking Lock For Encryption API |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-14921](https://issues.apache.org/jira/browse/HDFS-14921) | Remove SuperUser Check in Setting Storage Policy in FileStatus During Listing |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-14923](https://issues.apache.org/jira/browse/HDFS-14923) | Remove dead code from HealthMonitor |  Minor | . | Fei Hui | Fei Hui |
| [YARN-9914](https://issues.apache.org/jira/browse/YARN-9914) | Use separate configs for free disk space checking for full and not-full disks |  Minor | yarn | Jim Brennan | Jim Brennan |
| [MAPREDUCE-7208](https://issues.apache.org/jira/browse/MAPREDUCE-7208) | Tuning TaskRuntimeEstimator |  Minor | . | Ahmed Hussein | Ahmed Hussein |
| [HDFS-14942](https://issues.apache.org/jira/browse/HDFS-14942) | Change Log Level to debug in JournalNodeSyncer#syncWithJournalAtIndex |  Minor | . | Lisheng Sun | Lisheng Sun |
| [HDFS-14979](https://issues.apache.org/jira/browse/HDFS-14979) | [Observer Node] Balancer should submit getBlocks to Observer Node when possible |  Major | balancer & mover, hdfs | Erik Krogen | Erik Krogen |
| [HADOOP-16705](https://issues.apache.org/jira/browse/HADOOP-16705) | MBeanInfoBuilder puts unnecessary memory pressure on the system with a debug log |  Major | metrics | Lukas Majercak | Lukas Majercak |
| [HADOOP-16712](https://issues.apache.org/jira/browse/HADOOP-16712) | Config ha.failover-controller.active-standby-elector.zk.op.retries is not in core-default.xml |  Trivial | . | Wei-Chiu Chuang | Xieming Li |
| [HADOOP-16703](https://issues.apache.org/jira/browse/HADOOP-16703) | Backport HADOOP-16152 to branch-3.1 |  Major | . | Siyao Meng | Siyao Meng |
| [HDFS-14952](https://issues.apache.org/jira/browse/HDFS-14952) | Skip safemode if blockTotal is 0 in new NN |  Trivial | namenode | Rajesh Balamohan | Xiaoqiao He |
| [YARN-8842](https://issues.apache.org/jira/browse/YARN-8842) | Expose metrics for custom resource types in QueueMetrics |  Major | . | Szilard Nemeth | Szilard Nemeth |
| [YARN-9966](https://issues.apache.org/jira/browse/YARN-9966) | Code duplication in UserGroupMappingPlacementRule |  Major | . | Szilard Nemeth | Kevin Su |
| [YARN-9937](https://issues.apache.org/jira/browse/YARN-9937) | Add missing queue configs in RMWebService#CapacitySchedulerQueueInfo |  Major | capacity scheduler | Prabhu Joseph | Prabhu Joseph |
| [HADOOP-16718](https://issues.apache.org/jira/browse/HADOOP-16718) | Allow disabling Server Name Indication (SNI) for Jetty |  Major | . | Siyao Meng | Aravindan Vijayan |
| [HADOOP-16735](https://issues.apache.org/jira/browse/HADOOP-16735) | Make it clearer in config default that EnvironmentVariableCredentialsProvider supports AWS\_SESSION\_TOKEN |  Minor | documentation, fs/s3 | Mingliang Liu | Mingliang Liu |
| [YARN-10012](https://issues.apache.org/jira/browse/YARN-10012) | Guaranteed and max capacity queue metrics for custom resources |  Major | . | Jonathan Hung | Manikandan R |
| [HDFS-15050](https://issues.apache.org/jira/browse/HDFS-15050) | Optimize log information when DFSInputStream meet CannotObtainBlockLengthException |  Major | dfsclient | Xiaoqiao He | Xiaoqiao He |
| [YARN-10033](https://issues.apache.org/jira/browse/YARN-10033) | TestProportionalCapacityPreemptionPolicy not initializing vcores for effective max resources |  Major | capacity scheduler, test | Eric Payne | Eric Payne |
| [YARN-10039](https://issues.apache.org/jira/browse/YARN-10039) | Allow disabling app submission from REST endpoints |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-9894](https://issues.apache.org/jira/browse/YARN-9894) | CapacitySchedulerPerf test for measuring hundreds of apps in a large number of queues. |  Major | capacity scheduler, test | Eric Payne | Eric Payne |
| [HADOOP-16771](https://issues.apache.org/jira/browse/HADOOP-16771) | Update checkstyle to 8.26 and maven-checkstyle-plugin to 3.1.0 |  Major | build | Andras Bokor | Andras Bokor |
| [YARN-10009](https://issues.apache.org/jira/browse/YARN-10009) | In Capacity Scheduler, DRC can treat minimum user limit percent as a max when custom resource is defined |  Critical | capacity scheduler | Eric Payne | Eric Payne |
| [HDFS-12999](https://issues.apache.org/jira/browse/HDFS-12999) | When reach the end of the block group, it may not need to flush all the data packets(flushAllInternals) twice. |  Major | erasure-coding, hdfs-client | lufei | lufei |
| [HDFS-15074](https://issues.apache.org/jira/browse/HDFS-15074) | DataNode.DataTransfer thread should catch all the expception and log it. |  Major | datanode | Surendra Singh Lilhore | Hemanth Boyina |
| [HDFS-14740](https://issues.apache.org/jira/browse/HDFS-14740) | Recover data blocks from persistent memory read cache during datanode restarts |  Major | caching, datanode | Feilong He | Feilong He |
| [HDFS-15097](https://issues.apache.org/jira/browse/HDFS-15097) | Purge log in KMS and HttpFS |  Minor | httpfs, kms | Doris Gu | Doris Gu |
| [HDFS-14968](https://issues.apache.org/jira/browse/HDFS-14968) | Add ability to know datanode staleness |  Minor | datanode, logging, namenode | Ahmed Hussein | Ahmed Hussein |
| [YARN-7913](https://issues.apache.org/jira/browse/YARN-7913) | Improve error handling when application recovery fails with exception |  Major | resourcemanager | Gergo Repas | Wilfred Spiegelenburg |
| [HDFS-15119](https://issues.apache.org/jira/browse/HDFS-15119) | Allow expiration of cached locations in DFSInputStream |  Minor | dfsclient | Ahmed Hussein | Ahmed Hussein |
| [MAPREDUCE-7262](https://issues.apache.org/jira/browse/MAPREDUCE-7262) | MRApp helpers block for long intervals (500ms) |  Minor | mr-am | Ahmed Hussein | Ahmed Hussein |
| [YARN-10084](https://issues.apache.org/jira/browse/YARN-10084) | Allow inheritance of max app lifetime / default app lifetime |  Major | capacity scheduler | Eric Payne | Eric Payne |
| [HDFS-12491](https://issues.apache.org/jira/browse/HDFS-12491) | Support wildcard in CLASSPATH for libhdfs |  Major | libhdfs | John Zhuge | Muhammad Samir Khan |
| [YARN-10116](https://issues.apache.org/jira/browse/YARN-10116) | Expose diagnostics in RMAppManager summary |  Major | . | Jonathan Hung | Jonathan Hung |
| [HDFS-14758](https://issues.apache.org/jira/browse/HDFS-14758) | Decrease lease hard limit |  Minor | . | Eric Payne | Hemanth Boyina |
| [HDFS-15086](https://issues.apache.org/jira/browse/HDFS-15086) | Block scheduled counter never get decremet if the block got deleted before replication. |  Major | 3.1.1 | Surendra Singh Lilhore | Hemanth Boyina |
| [HDFS-15174](https://issues.apache.org/jira/browse/HDFS-15174) | Optimize ReplicaCachingGetSpaceUsed by reducing unnecessary io operations |  Major | . | Lisheng Sun | Lisheng Sun |
| [YARN-9018](https://issues.apache.org/jira/browse/YARN-9018) | Add functionality to AuxiliaryLocalPathHandler to return all locations to read for a given path |  Major | . | Kuhu Shukla | Kuhu Shukla |
| [HDFS-14861](https://issues.apache.org/jira/browse/HDFS-14861) | Reset LowRedundancyBlocks Iterator periodically |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [HADOOP-16882](https://issues.apache.org/jira/browse/HADOOP-16882) | Update jackson-databind to 2.9.10.2 in branch-3.1, branch-2.10 |  Blocker | . | Wei-Chiu Chuang | Lisheng Sun |
| [HADOOP-16776](https://issues.apache.org/jira/browse/HADOOP-16776) | backport HADOOP-16775: distcp copies to s3 are randomly corrupted |  Blocker | tools/distcp | Amir Shenavandeh | Amir Shenavandeh |
| [HDFS-15197](https://issues.apache.org/jira/browse/HDFS-15197) | [SBN read] Change ObserverRetryOnActiveException log to debug |  Minor | hdfs | Chen Liang | Chen Liang |
| [YARN-10200](https://issues.apache.org/jira/browse/YARN-10200) | Add number of containers to RMAppManager summary |  Major | . | Jonathan Hung | Jonathan Hung |
| [HADOOP-16952](https://issues.apache.org/jira/browse/HADOOP-16952) | Add .diff to gitignore |  Minor | . | Ayush Saxena | Ayush Saxena |
| [YARN-10212](https://issues.apache.org/jira/browse/YARN-10212) | Create separate configuration for max global AM attempts |  Major | . | Jonathan Hung | Bilwa S T |
| [YARN-9954](https://issues.apache.org/jira/browse/YARN-9954) | Configurable max application tags and max tag length |  Major | . | Jonathan Hung | Bilwa S T |
| [HADOOP-17001](https://issues.apache.org/jira/browse/HADOOP-17001) | The suffix name of the unified compression class |  Major | io | bianqi | bianqi |
| [HDFS-15295](https://issues.apache.org/jira/browse/HDFS-15295) | AvailableSpaceBlockPlacementPolicy should use chooseRandomWithStorageTypeTwoTrial() for better performance. |  Minor | . | Jinglun | Jinglun |
| [HADOOP-17127](https://issues.apache.org/jira/browse/HADOOP-17127) | Use RpcMetrics.TIMEUNIT to initialize rpc queueTime and processingTime |  Minor | common | Jim Brennan | Jim Brennan |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-13806](https://issues.apache.org/jira/browse/HDFS-13806) | EC: No error message for unsetting EC policy of the directory inherits the erasure coding policy from an ancestor directory |  Minor | erasure-coding | Souryakanta Dwivedy | Ayush Saxena |
| [HDFS-12459](https://issues.apache.org/jira/browse/HDFS-12459) | Fix revert: Add new op GETFILEBLOCKLOCATIONS to WebHDFS REST API |  Major | webhdfs | Weiwei Yang | Weiwei Yang |
| [HADOOP-15418](https://issues.apache.org/jira/browse/HADOOP-15418) | Hadoop KMSAuthenticationFilter needs to use getPropsByPrefix instead of iterator to avoid ConcurrentModificationException |  Major | common | Suma Shivaprasad | Suma Shivaprasad |
| [HDFS-14004](https://issues.apache.org/jira/browse/HDFS-14004) | TestLeaseRecovery2#testCloseWhileRecoverLease fails intermittently in trunk |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-13959](https://issues.apache.org/jira/browse/HDFS-13959) | TestUpgradeDomainBlockPlacementPolicy is flaky |  Major | . | Ayush Saxena | Ayush Saxena |
| [YARN-8948](https://issues.apache.org/jira/browse/YARN-8948) | PlacementRule interface should be for all YarnSchedulers |  Major | . | Bibin Chundatt | Bibin Chundatt |
| [HADOOP-16013](https://issues.apache.org/jira/browse/HADOOP-16013) | DecayRpcScheduler decay thread should run as a daemon |  Major | ipc | Erik Krogen | Erik Krogen |
| [HDFS-14175](https://issues.apache.org/jira/browse/HDFS-14175) | EC: Native XOR decoder should reset the output buffer before using it. |  Major | ec, hdfs | Surendra Singh Lilhore | Ayush Saxena |
| [HDFS-14202](https://issues.apache.org/jira/browse/HDFS-14202) | "dfs.disk.balancer.max.disk.throughputInMBperSec" property is not working as per set value. |  Major | diskbalancer | Ranith Sardar | Ranith Sardar |
| [HADOOP-16032](https://issues.apache.org/jira/browse/HADOOP-16032) | Distcp It should clear sub directory ACL before applying new ACL on it. |  Major | tools/distcp | Ranith Sardar | Ranith Sardar |
| [HADOOP-16127](https://issues.apache.org/jira/browse/HADOOP-16127) | In ipc.Client, put a new connection could happen after stop |  Major | ipc | Tsz-wo Sze | Tsz-wo Sze |
| [YARN-4901](https://issues.apache.org/jira/browse/YARN-4901) | QueueMetrics needs to be cleared before MockRM is initialized |  Major | scheduler | Daniel Templeton | Peter Bacsko |
| [HADOOP-16161](https://issues.apache.org/jira/browse/HADOOP-16161) | NetworkTopology#getWeightUsingNetworkLocation return unexpected result |  Major | net | Xiaoqiao He | Xiaoqiao He |
| [HDFS-14434](https://issues.apache.org/jira/browse/HDFS-14434) | webhdfs that connect secure hdfs should not use user.name parameter |  Minor | webhdfs | KWON BYUNGCHANG | KWON BYUNGCHANG |
| [HDFS-14527](https://issues.apache.org/jira/browse/HDFS-14527) | Stop all DataNodes may result in NN terminate |  Major | namenode | Xiaoqiao He | Xiaoqiao He |
| [HDFS-14494](https://issues.apache.org/jira/browse/HDFS-14494) | Move Server logging of StatedId inside receiveRequestState() |  Major | . | Konstantin Shvachko | Shweta |
| [HDFS-14618](https://issues.apache.org/jira/browse/HDFS-14618) | Incorrect synchronization of ArrayList field (ArrayList is thread-unsafe). |  Critical | . | Paul Ward | Paul Ward |
| [HDFS-14610](https://issues.apache.org/jira/browse/HDFS-14610) | HashMap is not thread safe. Field storageMap is typically synchronized by storageMap. However, in one place, field storageMap is not protected with synchronized. |  Critical | . | Paul Ward | Paul Ward |
| [HDFS-14499](https://issues.apache.org/jira/browse/HDFS-14499) | Misleading REM\_QUOTA value with snapshot and trash feature enabled for a directory |  Major | snapshots | Shashikant Banerjee | Shashikant Banerjee |
| [HDFS-14647](https://issues.apache.org/jira/browse/HDFS-14647) | NPE during secure namenode startup |  Major | hdfs | Fengnan Li | Fengnan Li |
| [HADOOP-16461](https://issues.apache.org/jira/browse/HADOOP-16461) | Regression: FileSystem cache lock parses XML within the lock |  Major | fs | Gopal Vijayaraghavan | Gopal Vijayaraghavan |
| [HDFS-14660](https://issues.apache.org/jira/browse/HDFS-14660) | [SBN Read] ObserverNameNode should throw StandbyException for requests not from ObserverProxyProvider |  Major | . | Chao Sun | Chao Sun |
| [HDFS-14569](https://issues.apache.org/jira/browse/HDFS-14569) | Result of crypto -listZones is not formatted properly |  Major | . | Hemanth Boyina | Hemanth Boyina |
| [HADOOP-12282](https://issues.apache.org/jira/browse/HADOOP-12282) | Connection thread's name should be updated after address changing is detected |  Major | ipc | zhouyingchao | Lisheng Sun |
| [HDFS-14686](https://issues.apache.org/jira/browse/HDFS-14686) | HttpFS: HttpFSFileSystem#getErasureCodingPolicy always returns null |  Major | httpfs | Siyao Meng | Siyao Meng |
| [HADOOP-15865](https://issues.apache.org/jira/browse/HADOOP-15865) | ConcurrentModificationException in Configuration.overlay() method |  Major | . | Oleksandr Shevchenko | Oleksandr Shevchenko |
| [HDFS-14759](https://issues.apache.org/jira/browse/HDFS-14759) | HDFS cat logs an info message |  Major | . | Eric Badger | Eric Badger |
| [HDFS-2470](https://issues.apache.org/jira/browse/HDFS-2470) | NN should automatically set permissions on dfs.namenode.\*.dir |  Major | namenode | Aaron Myers | Siddharth Wagle |
| [YARN-9718](https://issues.apache.org/jira/browse/YARN-9718) | Yarn REST API, services endpoint remote command ejection |  Major | . | Eric Yang | Eric Yang |
| [YARN-9813](https://issues.apache.org/jira/browse/YARN-9813) | RM does not start on JDK11 when UIv2 is enabled |  Critical | resourcemanager, yarn | Adam Antal | Adam Antal |
| [YARN-9820](https://issues.apache.org/jira/browse/YARN-9820) | RM logs InvalidStateTransitionException when app is submitted |  Critical | . | Rohith Sharma K S | Prabhu Joseph |
| [HDFS-14838](https://issues.apache.org/jira/browse/HDFS-14838) | RBF: Display RPC (instead of HTTP) Port Number in RBF web UI |  Minor | rbf, ui | Xieming Li | Xieming Li |
| [HDFS-14699](https://issues.apache.org/jira/browse/HDFS-14699) | Erasure Coding: Storage not considered in live replica when replication streams hard limit reached to threshold |  Critical | ec | Zhao Yi Ming | Zhao Yi Ming |
| [YARN-9833](https://issues.apache.org/jira/browse/YARN-9833) | Race condition when DirectoryCollection.checkDirs() runs during container launch |  Major | . | Peter Bacsko | Peter Bacsko |
| [YARN-9837](https://issues.apache.org/jira/browse/YARN-9837) | YARN Service fails to fetch status for Stopped apps with bigger spec files |  Major | yarn-native-services | Tarun Parimi | Tarun Parimi |
| [YARN-2255](https://issues.apache.org/jira/browse/YARN-2255) | YARN Audit logging not added to log4j.properties |  Major | . | Varun Saxena | Aihua Xu |
| [HDFS-14836](https://issues.apache.org/jira/browse/HDFS-14836) | FileIoProvider should not increase FileIoErrors metric in datanode volume metric |  Minor | . | Aiphago | Aiphago |
| [HADOOP-16581](https://issues.apache.org/jira/browse/HADOOP-16581) | ValueQueue does not trigger an async refill when number of values falls below watermark |  Major | common, kms | Yuval Degani | Yuval Degani |
| [HDFS-14853](https://issues.apache.org/jira/browse/HDFS-14853) | NPE in DFSNetworkTopology#chooseRandomWithStorageType() when the excludedNode is not present |  Major | . | Ranith Sardar | Ranith Sardar |
| [HDFS-13660](https://issues.apache.org/jira/browse/HDFS-13660) | DistCp job fails when new data is appended in the file while the distCp copy job is running |  Critical | distcp | Mukund Thakur | Mukund Thakur |
| [HDFS-14808](https://issues.apache.org/jira/browse/HDFS-14808) | EC: Improper size values for corrupt ec block in LOG |  Major | ec | Harshakiran Reddy | Ayush Saxena |
| [HDFS-14849](https://issues.apache.org/jira/browse/HDFS-14849) | Erasure Coding: the internal block is replicated many times when datanode is decommissioning |  Major | ec, erasure-coding | HuangTao | HuangTao |
| [YARN-9858](https://issues.apache.org/jira/browse/YARN-9858) | Optimize RMContext getExclusiveEnforcedPartitions |  Major | . | Jonathan Hung | Jonathan Hung |
| [HDFS-14492](https://issues.apache.org/jira/browse/HDFS-14492) | Snapshot memory leak |  Major | snapshots | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-14418](https://issues.apache.org/jira/browse/HDFS-14418) | Remove redundant super user priveledge checks from namenode. |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-14216](https://issues.apache.org/jira/browse/HDFS-14216) | NullPointerException happens in NamenodeWebHdfs |  Critical | . | lujie | lujie |
| [HDFS-14637](https://issues.apache.org/jira/browse/HDFS-14637) | Namenode may not replicate blocks to meet the policy after enabling upgradeDomain |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-14879](https://issues.apache.org/jira/browse/HDFS-14879) | Header was wrong in Snapshot web UI |  Major | . | Hemanth Boyina | Hemanth Boyina |
| [HDFS-14655](https://issues.apache.org/jira/browse/HDFS-14655) | [SBN Read] Namenode crashes if one of The JN is down |  Critical | . | Harshakiran Reddy | Ayush Saxena |
| [HDFS-14859](https://issues.apache.org/jira/browse/HDFS-14859) | Prevent unnecessary evaluation of costly operation getNumLiveDataNodes when dfs.namenode.safemode.min.datanodes is not zero |  Major | hdfs | Srinivasu Majeti | Srinivasu Majeti |
| [YARN-6715](https://issues.apache.org/jira/browse/YARN-6715) | Fix documentation about NodeHealthScriptRunner |  Major | documentation, nodemanager | Peter Bacsko | Peter Bacsko |
| [YARN-9552](https://issues.apache.org/jira/browse/YARN-9552) | FairScheduler: NODE\_UPDATE can cause NoSuchElementException |  Major | fairscheduler | Peter Bacsko | Peter Bacsko |
| [HDFS-14754](https://issues.apache.org/jira/browse/HDFS-14754) | Erasure Coding :  The number of Under-Replicated Blocks never reduced |  Critical | ec | Hemanth Boyina | Hemanth Boyina |
| [HDFS-14245](https://issues.apache.org/jira/browse/HDFS-14245) | Class cast error in GetGroups with ObserverReadProxyProvider |  Major | . | Shen Yinjie | Erik Krogen |
| [HDFS-14373](https://issues.apache.org/jira/browse/HDFS-14373) | EC : Decoding is failing when block group last incomplete cell fall in to AlignedStripe |  Critical | ec, hdfs-client | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-14509](https://issues.apache.org/jira/browse/HDFS-14509) | DN throws InvalidToken due to inequality of password when upgrade NN 2.x to 3.x |  Blocker | . | Yuxuan Wang | Yuxuan Wang |
| [HDFS-14886](https://issues.apache.org/jira/browse/HDFS-14886) | In NameNode Web UI's Startup Progress page, Loading edits always shows 0 sec |  Major | . | Hemanth Boyina | Hemanth Boyina |
| [YARN-8453](https://issues.apache.org/jira/browse/YARN-8453) | Additional Unit  tests to verify queue limit and max-limit with multiple resource types |  Major | capacity scheduler | Sunil G | Adam Antal |
| [HDFS-14890](https://issues.apache.org/jira/browse/HDFS-14890) | Setting permissions on name directory fails on non posix compliant filesystems |  Blocker | . | hirik | Siddharth Wagle |
| [HADOOP-16580](https://issues.apache.org/jira/browse/HADOOP-16580) | Disable retry of FailoverOnNetworkExceptionRetry in case of AccessControlException |  Major | common | Adam Antal | Adam Antal |
| [HDFS-14909](https://issues.apache.org/jira/browse/HDFS-14909) | DFSNetworkTopology#chooseRandomWithStorageType() should not decrease storage count for excluded node which is already part of excluded scope |  Major | namenode | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HADOOP-16600](https://issues.apache.org/jira/browse/HADOOP-16600) | StagingTestBase uses methods not available in Mockito 1.8.5 in branch-3.1 |  Major | . | Lisheng Sun | Duo Zhang |
| [MAPREDUCE-6441](https://issues.apache.org/jira/browse/MAPREDUCE-6441) | Improve temporary directory name generation in LocalDistributedCacheManager for concurrent processes |  Major | . | William Watson | Haibo Chen |
| [HADOOP-16662](https://issues.apache.org/jira/browse/HADOOP-16662) | Remove unnecessary InnerNode check in NetworkTopology#add() |  Minor | . | Lisheng Sun | Lisheng Sun |
| [HDFS-14847](https://issues.apache.org/jira/browse/HDFS-14847) | Erasure Coding: Blocks are over-replicated while EC decommissioning |  Critical | ec | Fei Hui | Fei Hui |
| [HDFS-14913](https://issues.apache.org/jira/browse/HDFS-14913) | Correct the value of available count in DFSNetworkTopology#chooseRandomWithStorageType() |  Major | . | Ayush Saxena | Ayush Saxena |
| [YARN-9915](https://issues.apache.org/jira/browse/YARN-9915) | Fix FindBug issue in QueueMetrics |  Minor | . | Prabhu Joseph | Prabhu Joseph |
| [HDFS-12749](https://issues.apache.org/jira/browse/HDFS-12749) | DN may not send block report to NN after NN restart |  Major | datanode | TanYuxin | Xiaoqiao He |
| [HDFS-13901](https://issues.apache.org/jira/browse/HDFS-13901) | INode access time is ignored because of race between open and rename |  Major | . | Jinglun | Jinglun |
| [YARN-9921](https://issues.apache.org/jira/browse/YARN-9921) | Issue in PlacementConstraint when YARN Service AM retries allocation on component failure. |  Major | . | Tarun Parimi | Tarun Parimi |
| [HDFS-14910](https://issues.apache.org/jira/browse/HDFS-14910) | Rename Snapshot with Pre Descendants Fail With IllegalArgumentException. |  Blocker | . | Íñigo Goiri | Wei-Chiu Chuang |
| [HDFS-14308](https://issues.apache.org/jira/browse/HDFS-14308) | DFSStripedInputStream curStripeBuf is not freed by unbuffer() |  Major | ec | Joe McDonnell | Zhao Yi Ming |
| [HDFS-14931](https://issues.apache.org/jira/browse/HDFS-14931) | hdfs crypto commands limit column width |  Major | . | Eric Badger | Eric Badger |
| [HADOOP-16669](https://issues.apache.org/jira/browse/HADOOP-16669) | TestRawLocalFileSystemContract.testPermission fails if no native library |  Minor | common, test | Steve Loughran | Steve Loughran |
| [HDFS-14920](https://issues.apache.org/jira/browse/HDFS-14920) | Erasure Coding: Decommission may hang If one or more datanodes are out of service during decommission |  Major | ec | Fei Hui | Fei Hui |
| [HDFS-13736](https://issues.apache.org/jira/browse/HDFS-13736) | BlockPlacementPolicyDefault can not choose favored nodes when 'dfs.namenode.block-placement-policy.default.prefer-local-node' set to false |  Major | . | hu xiaodong | hu xiaodong |
| [HDFS-14925](https://issues.apache.org/jira/browse/HDFS-14925) | rename operation should check nest snapshot |  Major | namenode | Junwang Zhao | Junwang Zhao |
| [YARN-9949](https://issues.apache.org/jira/browse/YARN-9949) | Add missing queue configs for root queue in RMWebService#CapacitySchedulerInfo |  Minor | capacity scheduler | Prabhu Joseph | Prabhu Joseph |
| [HDFS-14945](https://issues.apache.org/jira/browse/HDFS-14945) | Revise PacketResponder's log. |  Minor | datanode | Xudong Cao | Xudong Cao |
| [HDFS-14946](https://issues.apache.org/jira/browse/HDFS-14946) | Erasure Coding: Block recovery failed during decommissioning |  Major | . | Fei Hui | Fei Hui |
| [HDFS-14384](https://issues.apache.org/jira/browse/HDFS-14384) | When lastLocatedBlock token expire, it will take 1~3s second to refetch it. |  Major | hdfs-client | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-14806](https://issues.apache.org/jira/browse/HDFS-14806) | Bootstrap standby may fail if used in-progress tailing |  Major | namenode | Chen Liang | Chen Liang |
| [HDFS-14958](https://issues.apache.org/jira/browse/HDFS-14958) | TestBalancerWithNodeGroup is not using NetworkTopologyWithNodeGroup |  Minor | hdfs | Jim Brennan | Jim Brennan |
| [HDFS-14720](https://issues.apache.org/jira/browse/HDFS-14720) | DataNode shouldn't report block as bad block if the block length is Long.MAX\_VALUE. |  Major | datanode | Surendra Singh Lilhore | Hemanth Boyina |
| [HADOOP-16677](https://issues.apache.org/jira/browse/HADOOP-16677) | Recalculate the remaining timeout millis correctly while throwing an InterupptedException in SocketIOWithTimeout. |  Minor | common | Xudong Cao | Xudong Cao |
| [HDFS-14884](https://issues.apache.org/jira/browse/HDFS-14884) | Add sanity check that zone key equals feinfo key while setting Xattrs |  Major | encryption, hdfs | Mukul Kumar Singh | Mukul Kumar Singh |
| [HADOOP-15097](https://issues.apache.org/jira/browse/HADOOP-15097) | AbstractContractDeleteTest::testDeleteNonEmptyDirRecursive with misleading path |  Minor | fs, test | zhoutai.zt | Xieming Li |
| [YARN-9984](https://issues.apache.org/jira/browse/YARN-9984) | FSPreemptionThread can cause NullPointerException while app is unregistered with containers running on a node |  Major | fairscheduler | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [YARN-9983](https://issues.apache.org/jira/browse/YARN-9983) | Typo in YARN Service overview documentation |  Trivial | documentation | Denes Gerencser | Denes Gerencser |
| [HADOOP-16719](https://issues.apache.org/jira/browse/HADOOP-16719) | Remove the disallowed element config within maven-checkstyle-plugin |  Major | . | Wanqiang Ji | Wanqiang Ji |
| [HADOOP-16700](https://issues.apache.org/jira/browse/HADOOP-16700) | RpcQueueTime may be negative when the response has to be sent later |  Minor | . | xuzq | xuzq |
| [HADOOP-15686](https://issues.apache.org/jira/browse/HADOOP-15686) | Supress bogus AbstractWadlGeneratorGrammarGenerator in KMS stderr |  Major | kms | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-14940](https://issues.apache.org/jira/browse/HDFS-14940) | HDFS Balancer : Do not allow to set balancer maximum network bandwidth more than 1TB |  Minor | balancer & mover | Souryakanta Dwivedy | Hemanth Boyina |
| [YARN-9838](https://issues.apache.org/jira/browse/YARN-9838) | Fix resource inconsistency for queues when moving app with reserved container to another queue |  Critical | capacity scheduler | jiulongzhu | jiulongzhu |
| [YARN-9968](https://issues.apache.org/jira/browse/YARN-9968) | Public Localizer is exiting in NodeManager due to NullPointerException |  Major | nodemanager | Tarun Parimi | Tarun Parimi |
| [YARN-9011](https://issues.apache.org/jira/browse/YARN-9011) | Race condition during decommissioning |  Major | nodemanager | Peter Bacsko | Peter Bacsko |
| [HDFS-14973](https://issues.apache.org/jira/browse/HDFS-14973) | Balancer getBlocks RPC dispersal does not function properly |  Major | balancer & mover | Erik Krogen | Erik Krogen |
| [MAPREDUCE-7240](https://issues.apache.org/jira/browse/MAPREDUCE-7240) | Exception ' Invalid event: TA\_TOO\_MANY\_FETCH\_FAILURE at SUCCESS\_FINISHING\_CONTAINER' cause job error |  Critical | . | luhuachao | luhuachao |
| [HDFS-14986](https://issues.apache.org/jira/browse/HDFS-14986) | ReplicaCachingGetSpaceUsed throws  ConcurrentModificationException |  Major | datanode, performance | Ryan Wu | Aiphago |
| [MAPREDUCE-7249](https://issues.apache.org/jira/browse/MAPREDUCE-7249) | Invalid event TA\_TOO\_MANY\_FETCH\_FAILURE at SUCCESS\_CONTAINER\_CLEANUP causes job failure |  Critical | applicationmaster, mrv2 | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [HDFS-15010](https://issues.apache.org/jira/browse/HDFS-15010) | BlockPoolSlice#addReplicaThreadPool static pool should be initialized by static method |  Major | datanode | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HADOOP-16744](https://issues.apache.org/jira/browse/HADOOP-16744) |  Fix building instruction to enable zstd |  Minor | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-14869](https://issues.apache.org/jira/browse/HDFS-14869) | Data loss in case of distcp using snapshot diff. Replication should include rename records if file was skipped in the previous iteration |  Major | distcp | Aasha Medhi | Aasha Medhi |
| [HADOOP-16754](https://issues.apache.org/jira/browse/HADOOP-16754) | Fix docker failed to build yetus/hadoop |  Blocker | build | Kevin Su | Kevin Su |
| [HDFS-15032](https://issues.apache.org/jira/browse/HDFS-15032) | Balancer crashes when it fails to contact an unavailable NN via ObserverReadProxyProvider |  Major | balancer & mover | Erik Krogen | Erik Krogen |
| [HDFS-15036](https://issues.apache.org/jira/browse/HDFS-15036) | Active NameNode should not silently fail the image transfer |  Major | namenode | Konstantin Shvachko | Chen Liang |
| [HDFS-14519](https://issues.apache.org/jira/browse/HDFS-14519) | NameQuota is not update after concat operation, so namequota is wrong |  Major | . | Ranith Sardar | Ranith Sardar |
| [YARN-10055](https://issues.apache.org/jira/browse/YARN-10055) | bower install fails |  Blocker | build, yarn-ui-v2 | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15076](https://issues.apache.org/jira/browse/HDFS-15076) | Fix tests that hold FSDirectory lock, without holding FSNamesystem lock. |  Major | test | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-15073](https://issues.apache.org/jira/browse/HDFS-15073) | Replace curator-shaded guava import with the standard one |  Minor | hdfs-client | Akira Ajisaka | Chandra Sanivarapu |
| [HADOOP-16042](https://issues.apache.org/jira/browse/HADOOP-16042) | Update the link to HadoopJavaVersion |  Minor | documentation | Akira Ajisaka | Chandra Sanivarapu |
| [HDFS-14934](https://issues.apache.org/jira/browse/HDFS-14934) | [SBN Read] Standby NN throws many InterruptedExceptions when dfs.ha.tail-edits.period is 0 |  Major | . | Takanobu Asanuma | Ayush Saxena |
| [YARN-10053](https://issues.apache.org/jira/browse/YARN-10053) | Placement rules do not use correct group service init |  Major | yarn | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [HDFS-15068](https://issues.apache.org/jira/browse/HDFS-15068) | DataNode could meet deadlock if invoke refreshVolumes when register |  Major | datanode | Xiaoqiao He | Aiphago |
| [MAPREDUCE-7255](https://issues.apache.org/jira/browse/MAPREDUCE-7255) | Fix typo in MapReduce documentaion example |  Trivial | documentation | Sergey Pogorelov | Sergey Pogorelov |
| [HDFS-15072](https://issues.apache.org/jira/browse/HDFS-15072) | HDFS MiniCluster fails to start when run in directory path with a % |  Minor | . | Geoffrey Jacoby | Masatake Iwasaki |
| [HDFS-15077](https://issues.apache.org/jira/browse/HDFS-15077) | Fix intermittent failure of TestDFSClientRetries#testLeaseRenewSocketTimeout |  Minor | test | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-15080](https://issues.apache.org/jira/browse/HDFS-15080) | Fix the issue in reading persistent memory cached data with an offset |  Major | caching, datanode | Feilong He | Feilong He |
| [YARN-7387](https://issues.apache.org/jira/browse/YARN-7387) | org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestIncreaseAllocationExpirer fails intermittently |  Major | . | Miklos Szegedi | Jim Brennan |
| [YARN-8672](https://issues.apache.org/jira/browse/YARN-8672) | TestContainerManager#testLocalingResourceWhileContainerRunning occasionally times out |  Major | nodemanager | Jason Darrell Lowe | Chandni Singh |
| [HDFS-14957](https://issues.apache.org/jira/browse/HDFS-14957) | INodeReference Space Consumed was not same in QuotaUsage and ContentSummary |  Major | namenode | Hemanth Boyina | Hemanth Boyina |
| [MAPREDUCE-7252](https://issues.apache.org/jira/browse/MAPREDUCE-7252) | Handling 0 progress in SimpleExponential task runtime estimator |  Minor | . | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-16749](https://issues.apache.org/jira/browse/HADOOP-16749) | Configuration parsing of CDATA values are blank |  Major | conf | Jonathan Turner Eagles | Daryn Sharp |
| [HDFS-15095](https://issues.apache.org/jira/browse/HDFS-15095) | Fix accidental comment in flaky test TestDecommissioningStatus |  Major | hdfs | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15099](https://issues.apache.org/jira/browse/HDFS-15099) | [SBN Read] checkOperation(WRITE) should throw ObserverRetryOnActiveException on ObserverNode |  Major | namenode | Konstantin Shvachko | Chen Liang |
| [HDFS-14578](https://issues.apache.org/jira/browse/HDFS-14578) | AvailableSpaceBlockPlacementPolicy always prefers local node |  Major | block placement | Wei-Chiu Chuang | Ayush Saxena |
| [HADOOP-16683](https://issues.apache.org/jira/browse/HADOOP-16683) | Disable retry of FailoverOnNetworkExceptionRetry in case of wrapped AccessControlException |  Major | common | Adam Antal | Adam Antal |
| [MAPREDUCE-7256](https://issues.apache.org/jira/browse/MAPREDUCE-7256) | Fix javadoc error in SimpleExponentialSmoothing |  Minor | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-8373](https://issues.apache.org/jira/browse/YARN-8373) | RM  Received RMFatalEvent of type CRITICAL\_THREAD\_CRASH |  Major | fairscheduler, resourcemanager | Girish Bhat | Wilfred Spiegelenburg |
| [MAPREDUCE-7247](https://issues.apache.org/jira/browse/MAPREDUCE-7247) | Modify HistoryServerRest.html content,change The job attempt id‘s datatype from string to int |  Major | documentation | zhaoshengjie |  |
| [YARN-8148](https://issues.apache.org/jira/browse/YARN-8148) | Update decimal values for queue capacities shown on queue status CLI |  Major | client | Prabhu Joseph | Prabhu Joseph |
| [HADOOP-16808](https://issues.apache.org/jira/browse/HADOOP-16808) | Use forkCount and reuseForks parameters instead of forkMode in the config of maven surefire plugin |  Minor | build | Akira Ajisaka | Xieming Li |
| [HADOOP-16793](https://issues.apache.org/jira/browse/HADOOP-16793) |  Remove WARN log when ipc connection interrupted in Client#handleSaslConnectionFailure() |  Minor | . | Lisheng Sun | Lisheng Sun |
| [YARN-9790](https://issues.apache.org/jira/browse/YARN-9790) | Failed to set default-application-lifetime if maximum-application-lifetime is less than or equal to zero |  Major | . | kyungwan nam | kyungwan nam |
| [HDFS-14993](https://issues.apache.org/jira/browse/HDFS-14993) | checkDiskError doesn't work during datanode startup |  Major | datanode | Yang Yun | Yang Yun |
| [HDFS-13179](https://issues.apache.org/jira/browse/HDFS-13179) | TestLazyPersistReplicaRecovery#testDnRestartWithSavedReplicas fails intermittently |  Critical | fs | Gabor Bota | Ahmed Hussein |
| [MAPREDUCE-7259](https://issues.apache.org/jira/browse/MAPREDUCE-7259) | testSpeculateSuccessfulWithUpdateEvents fails Intermittently |  Minor | . | Ahmed Hussein | Ahmed Hussein |
| [MAPREDUCE-7079](https://issues.apache.org/jira/browse/MAPREDUCE-7079) | JobHistory#ServiceStop implementation is incorrect |  Major | . | Jason Darrell Lowe | Ahmed Hussein |
| [HDFS-15118](https://issues.apache.org/jira/browse/HDFS-15118) | [SBN Read] Slow clients when Observer reads are enabled but there are no Observers on the cluster. |  Major | hdfs-client | Konstantin Shvachko | Chen Liang |
| [HDFS-7175](https://issues.apache.org/jira/browse/HDFS-7175) | Client-side SocketTimeoutException during Fsck |  Major | namenode | Carl Steinbach | Stephen O'Donnell |
| [HDFS-15148](https://issues.apache.org/jira/browse/HDFS-15148) | dfs.namenode.send.qop.enabled should not apply to primary NN port |  Major | . | Chen Liang | Chen Liang |
| [HDFS-15115](https://issues.apache.org/jira/browse/HDFS-15115) | Namenode crash caused by NPE in BlockPlacementPolicyDefault when dynamically change logger to debug |  Major | . | wangzhixiang | wangzhixiang |
| [HDFS-15157](https://issues.apache.org/jira/browse/HDFS-15157) | Fix compilation failure for branch-3.1 |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-15158](https://issues.apache.org/jira/browse/HDFS-15158) | The number of failed volumes mismatch  with volumeFailures of Datanode metrics |  Minor | datanode | Yang Yun | Yang Yun |
| [HADOOP-16849](https://issues.apache.org/jira/browse/HADOOP-16849) | start-build-env.sh behaves incorrectly when username is numeric only |  Minor | build | Jihyun Cho | Jihyun Cho |
| [HDFS-15161](https://issues.apache.org/jira/browse/HDFS-15161) | When evictableMmapped or evictable size is zero, do not throw NoSuchElementException in ShortCircuitCache#close() |  Major | . | Lisheng Sun | Lisheng Sun |
| [HDFS-15164](https://issues.apache.org/jira/browse/HDFS-15164) | Fix TestDelegationTokensWithHA |  Major | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-16868](https://issues.apache.org/jira/browse/HADOOP-16868) | ipc.Server readAndProcess threw NullPointerException |  Major | rpc-server | Tsz-wo Sze | Tsz-wo Sze |
| [HADOOP-16869](https://issues.apache.org/jira/browse/HADOOP-16869) |  Upgrade findbugs-maven-plugin to 3.0.5 to fix mvn findbugs:findbugs failure |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15052](https://issues.apache.org/jira/browse/HDFS-15052) | WebHDFS getTrashRoot leads to OOM due to FileSystem object creation |  Major | webhdfs | Wei-Chiu Chuang | Masatake Iwasaki |
| [HDFS-15185](https://issues.apache.org/jira/browse/HDFS-15185) | StartupProgress reports edits segments until the entire startup completes |  Major | namenode | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-15166](https://issues.apache.org/jira/browse/HDFS-15166) | Remove redundant field fStream in ByteStringLog |  Major | . | Konstantin Shvachko | Xieming Li |
| [HADOOP-16841](https://issues.apache.org/jira/browse/HADOOP-16841) | The description of hadoop.http.authentication.signature.secret.file contains outdated information |  Minor | documentation | Akira Ajisaka | Xieming Li |
| [YARN-10156](https://issues.apache.org/jira/browse/YARN-10156) | Fix typo 'complaint' which means quite different in Federation.md |  Minor | documentation, federation | Sungpeo Kook | Sungpeo Kook |
| [HDFS-15147](https://issues.apache.org/jira/browse/HDFS-15147) | LazyPersistTestCase wait logic is error-prone |  Minor | . | Ahmed Hussein | Ahmed Hussein |
| [HDFS-14668](https://issues.apache.org/jira/browse/HDFS-14668) | Support Fuse with Users from multiple Security Realms |  Critical | fuse-dfs | Sailesh Patel | Istvan Fajth |
| [HDFS-15111](https://issues.apache.org/jira/browse/HDFS-15111) | stopStandbyServices() should log which service state it is transitioning from. |  Major | hdfs, logging | Konstantin Shvachko | Xieming Li |
| [HDFS-15199](https://issues.apache.org/jira/browse/HDFS-15199) | NPE in BlockSender |  Major | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-16891](https://issues.apache.org/jira/browse/HADOOP-16891) | Upgrade jackson-databind to 2.9.10.3 |  Blocker | . | Siyao Meng | Siyao Meng |
| [HADOOP-16840](https://issues.apache.org/jira/browse/HADOOP-16840) | AliyunOSS: getFileStatus throws FileNotFoundException in versioning bucket |  Major | fs/oss | wujinhu | wujinhu |
| [YARN-9427](https://issues.apache.org/jira/browse/YARN-9427) | TestContainerSchedulerQueuing.testKillOnlyRequiredOpportunisticContainers fails sporadically |  Major | scheduler, test | Prabhu Joseph | Ahmed Hussein |
| [HDFS-15216](https://issues.apache.org/jira/browse/HDFS-15216) | Wrong Use Case of -showprogress in fsck |  Major | . | Ravuri Sushma sree | Ravuri Sushma sree |
| [HDFS-15211](https://issues.apache.org/jira/browse/HDFS-15211) | EC: File write hangs during close in case of Exception during updatePipeline |  Critical | . | Ayush Saxena | Ayush Saxena |
| [HDFS-15208](https://issues.apache.org/jira/browse/HDFS-15208) | Suppress bogus AbstractWadlGeneratorGrammarGenerator in KMS stderr in hdfs |  Trivial | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-15219](https://issues.apache.org/jira/browse/HDFS-15219) | DFS Client will stuck when ResponseProcessor.run throw Error |  Major | hdfs-client | zhengchenyu | zhengchenyu |
| [HADOOP-16949](https://issues.apache.org/jira/browse/HADOOP-16949) | pylint fails in the build environment |  Critical | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-14836](https://issues.apache.org/jira/browse/HADOOP-14836) | Upgrade maven-clean-plugin to 3.1.0 |  Major | build | Allen Wittenauer | Akira Ajisaka |
| [MAPREDUCE-7272](https://issues.apache.org/jira/browse/MAPREDUCE-7272) | TaskAttemptListenerImpl excessive log messages |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15283](https://issues.apache.org/jira/browse/HDFS-15283) | Cache pool MAXTTL is not persisted and restored on cluster restart |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [HADOOP-16944](https://issues.apache.org/jira/browse/HADOOP-16944) | Use Yetus 0.12.0 in GitHub PR |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15276](https://issues.apache.org/jira/browse/HDFS-15276) | Concat on INodeRefernce fails with illegal state exception |  Critical | . | Hemanth Boyina | Hemanth Boyina |
| [HDFS-15281](https://issues.apache.org/jira/browse/HDFS-15281) | ZKFC ignores dfs.namenode.rpc-bind-host and uses dfs.namenode.rpc-address to bind to host address |  Major | ha, namenode | Dhiraj Hegde | Dhiraj Hegde |
| [HDFS-15297](https://issues.apache.org/jira/browse/HDFS-15297) | TestNNHandlesBlockReportPerStorage::blockReport\_02 fails intermittently in trunk |  Major | datanode, test | Mingliang Liu | Ayush Saxena |
| [HADOOP-17014](https://issues.apache.org/jira/browse/HADOOP-17014) | Upgrade jackson-databind to 2.9.10.4 |  Blocker | . | Siyao Meng | Siyao Meng |
| [HDFS-15286](https://issues.apache.org/jira/browse/HDFS-15286) | Concat on a same files deleting the file |  Critical | . | Hemanth Boyina | Hemanth Boyina |
| [HADOOP-15565](https://issues.apache.org/jira/browse/HADOOP-15565) | ViewFileSystem.close doesn't close child filesystems and causes FileSystem objects leak. |  Major | . | Jinglun | Jinglun |
| [HADOOP-17052](https://issues.apache.org/jira/browse/HADOOP-17052) | NetUtils.connect() throws unchecked exception (UnresolvedAddressException) causing clients to abort |  Major | net | Dhiraj Hegde | Dhiraj Hegde |
| [YARN-10347](https://issues.apache.org/jira/browse/YARN-10347) | Fix double locking in CapacityScheduler#reinitialize in branch-3.1 |  Critical | capacity scheduler | Masatake Iwasaki | Masatake Iwasaki |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-10072](https://issues.apache.org/jira/browse/YARN-10072) | TestCSAllocateCustomResource failures |  Major | yarn | Jim Brennan | Jim Brennan |
| [YARN-10161](https://issues.apache.org/jira/browse/YARN-10161) | TestRouterWebServicesREST is corrupting STDOUT |  Minor | yarn | Jim Brennan | Jim Brennan |
| [HADOOP-14206](https://issues.apache.org/jira/browse/HADOOP-14206) | TestSFTPFileSystem#testFileExists failure: Invalid encoding for signature |  Major | fs, test | John Zhuge | Jim Brennan |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-15636](https://issues.apache.org/jira/browse/HADOOP-15636) | Add ITestDynamoDBMetadataStore |  Minor | fs/s3, test | Sean Mackrory | Gabor Bota |
| [HADOOP-15787](https://issues.apache.org/jira/browse/HADOOP-15787) | [JDK11] TestIPC.testRTEDuringConnectionSetup fails |  Major | . | Akira Ajisaka | Zsolt Venczel |
| [HDFS-14262](https://issues.apache.org/jira/browse/HDFS-14262) | [SBN read] Unclear Log.WARN message in GlobalStateIdContext |  Major | hdfs | Shweta | Shweta |
| [HDFS-14590](https://issues.apache.org/jira/browse/HDFS-14590) | [SBN Read] Add the document link to the top page |  Major | documentation | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-9791](https://issues.apache.org/jira/browse/YARN-9791) | Queue Mutation API does not allow to remove a config |  Major | capacity scheduler | Prabhu Joseph | Prabhu Joseph |
| [HDFS-14822](https://issues.apache.org/jira/browse/HDFS-14822) | [SBN read] Revisit GlobalStateIdContext locking when getting server state id |  Major | hdfs | Chen Liang | Chen Liang |
| [HDFS-14785](https://issues.apache.org/jira/browse/HDFS-14785) | [SBN read] Change client logging to be less aggressive |  Major | hdfs | Chen Liang | Chen Liang |
| [YARN-9864](https://issues.apache.org/jira/browse/YARN-9864) | Format CS Configuration present in Configuration Store |  Major | capacity scheduler | Prabhu Joseph | Prabhu Joseph |
| [YARN-9801](https://issues.apache.org/jira/browse/YARN-9801) | SchedConfCli does not work with https mode |  Major | . | Prabhu Joseph | Prabhu Joseph |
| [HDFS-14858](https://issues.apache.org/jira/browse/HDFS-14858) | [SBN read] Allow configurably enable/disable AlignmentContext on NameNode |  Major | hdfs | Chen Liang | Chen Liang |
| [HDFS-12979](https://issues.apache.org/jira/browse/HDFS-12979) | StandbyNode should upload FsImage to ObserverNode after checkpointing. |  Major | hdfs | Konstantin Shvachko | Chen Liang |
| [YARN-9873](https://issues.apache.org/jira/browse/YARN-9873) | Mutation API Config Change need to update Version Number |  Major | . | Prabhu Joseph | Prabhu Joseph |
| [HDFS-14162](https://issues.apache.org/jira/browse/HDFS-14162) | Balancer should work with ObserverNode |  Major | . | Konstantin Shvachko | Erik Krogen |
| [YARN-9773](https://issues.apache.org/jira/browse/YARN-9773) | Add QueueMetrics for Custom Resources |  Major | . | Manikandan R | Manikandan R |
| [HADOOP-16598](https://issues.apache.org/jira/browse/HADOOP-16598) | Backport "HADOOP-16558 [COMMON+HDFS] use protobuf-maven-plugin to generate protobuf classes" to all active branches |  Major | common | Duo Zhang | Duo Zhang |
| [YARN-9950](https://issues.apache.org/jira/browse/YARN-9950) | Unset Ordering Policy of Leaf/Parent queue converted from Parent/Leaf queue respectively |  Major | capacity scheduler | Prabhu Joseph | Prabhu Joseph |
| [YARN-9900](https://issues.apache.org/jira/browse/YARN-9900) | Revert to previous state when Invalid Config is applied and Refresh Support in SchedulerConfig Format |  Major | capacity scheduler | Prabhu Joseph | Prabhu Joseph |
| [HADOOP-16610](https://issues.apache.org/jira/browse/HADOOP-16610) | Upgrade to yetus 0.11.1 and use emoji vote on github pre commit |  Major | build | Duo Zhang | Duo Zhang |
| [YARN-9909](https://issues.apache.org/jira/browse/YARN-9909) | Offline format of YarnConfigurationStore |  Major | capacity scheduler | Prabhu Joseph | Prabhu Joseph |
| [YARN-9836](https://issues.apache.org/jira/browse/YARN-9836) | General usability improvements in showSimulationTrace.html |  Minor | scheduler-load-simulator | Adam Antal | Adam Antal |
| [HADOOP-16758](https://issues.apache.org/jira/browse/HADOOP-16758) | Refine testing.md to tell user better how to use auth-keys.xml |  Minor | fs/s3 | Mingliang Liu | Mingliang Liu |
| [HADOOP-16609](https://issues.apache.org/jira/browse/HADOOP-16609) | Add Jenkinsfile for all active branches |  Major | build | Duo Zhang | Akira Ajisaka |
| [YARN-10022](https://issues.apache.org/jira/browse/YARN-10022) | Create RM Rest API to validate a CapacityScheduler Configuration |  Major | . | Kinga Marton | Kinga Marton |
| [HDFS-15173](https://issues.apache.org/jira/browse/HDFS-15173) | RBF: Delete repeated configuration 'dfs.federation.router.metrics.enable' |  Minor | documentation, rbf | panlijie | panlijie |
| [YARN-10139](https://issues.apache.org/jira/browse/YARN-10139) | ValidateAndGetSchedulerConfiguration API fails when cluster max allocation \> default 8GB |  Major | . | Prabhu Joseph | Prabhu Joseph |
| [HDFS-14731](https://issues.apache.org/jira/browse/HDFS-14731) | [FGL] Remove redundant locking on NameNode. |  Major | namenode | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-14918](https://issues.apache.org/jira/browse/HADOOP-14918) | Remove the Local Dynamo DB test option |  Major | fs/s3 | Steve Loughran | Gabor Bota |
| [HDFS-15305](https://issues.apache.org/jira/browse/HDFS-15305) | Extend ViewFS and provide ViewFSOverloadScheme implementation with scheme configurable. |  Major | fs, hadoop-client, hdfs-client, viewfs | Uma Maheswara Rao G | Uma Maheswara Rao G |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-8016](https://issues.apache.org/jira/browse/YARN-8016) | Refine PlacementRule interface and add a app-name queue mapping rule as an example |  Major | . | Zian Chen | Zian Chen |
| [HADOOP-16491](https://issues.apache.org/jira/browse/HADOOP-16491) | Upgrade jetty version to 9.3.27 |  Major | . | Hrishikesh Gadre | Hrishikesh Gadre |
| [HADOOP-16542](https://issues.apache.org/jira/browse/HADOOP-16542) | Update commons-beanutils version to 1.9.4 |  Major | . | Wei-Chiu Chuang | Kevin Su |
| [HADOOP-16551](https://issues.apache.org/jira/browse/HADOOP-16551) | The changelog\*.md seems not generated when create-release |  Blocker | . | Zhankun Tang |  |
| [YARN-9730](https://issues.apache.org/jira/browse/YARN-9730) | Support forcing configured partitions to be exclusive based on app node label |  Major | . | Jonathan Hung | Jonathan Hung |
| [HDFS-14959](https://issues.apache.org/jira/browse/HDFS-14959) | [SBNN read] access time should be turned off |  Major | documentation | Wei-Chiu Chuang | Chao Sun |
| [HADOOP-16784](https://issues.apache.org/jira/browse/HADOOP-16784) | Update the year to 2020 |  Major | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-16871](https://issues.apache.org/jira/browse/HADOOP-16871) | Upgrade Netty version to 4.1.45.Final to handle CVE-2019-20444,CVE-2019-16869 |  Major | . | Aray Chenchu Sukesh | Aray Chenchu Sukesh |
| [HADOOP-16982](https://issues.apache.org/jira/browse/HADOOP-16982) | Update Netty to 4.1.48.Final |  Blocker | . | Wei-Chiu Chuang | Lisheng Sun |


