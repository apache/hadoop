
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

## Release 3.2.2 - 2021-01-03



### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-15691](https://issues.apache.org/jira/browse/HADOOP-15691) | Add PathCapabilities to FS and FC to complement StreamCapabilities |  Major | . | Steve Loughran | Steve Loughran |
| [YARN-9760](https://issues.apache.org/jira/browse/YARN-9760) | Support configuring application priorities on a workflow level |  Major | . | Jonathan Hung | Varun Saxena |
| [HDFS-14905](https://issues.apache.org/jira/browse/HDFS-14905) | Backport HDFS persistent memory read cache support to branch-3.2 |  Major | caching, datanode | Feilong He | Feilong He |
| [HDFS-12943](https://issues.apache.org/jira/browse/HDFS-12943) | Consistent Reads from Standby Node |  Major | hdfs | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-16790](https://issues.apache.org/jira/browse/HADOOP-16790) | Add Write Convenience Methods |  Minor | . | David Mollitor | David Mollitor |
| [HADOOP-17210](https://issues.apache.org/jira/browse/HADOOP-17210) | backport HADOOP-15691 PathCapabilities API to branch-3.2 |  Major | fs, fs/s3 | Steve Loughran | Steve Loughran |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-8750](https://issues.apache.org/jira/browse/YARN-8750) | Refactor TestQueueMetrics |  Minor | resourcemanager | Szilard Nemeth | Szilard Nemeth |
| [HADOOP-15849](https://issues.apache.org/jira/browse/HADOOP-15849) | Upgrade netty version to 3.10.6 |  Major | . | Xiao Chen | Xiao Chen |
| [HDFS-12946](https://issues.apache.org/jira/browse/HDFS-12946) | Add a tool to check rack configuration against EC policies |  Major | erasure-coding | Xiao Chen | Kitti Nanasi |
| [HDFS-14113](https://issues.apache.org/jira/browse/HDFS-14113) | EC : Add Configuration to restrict UserDefined Policies |  Major | erasure-coding | Ayush Saxena | Ayush Saxena |
| [HDFS-14006](https://issues.apache.org/jira/browse/HDFS-14006) | Refactor name node to allow different token verification implementations |  Major | . | CR Hota | CR Hota |
| [HADOOP-15909](https://issues.apache.org/jira/browse/HADOOP-15909) | KeyProvider class should implement Closeable |  Major | kms | Kuhu Shukla | Kuhu Shukla |
| [HDFS-14061](https://issues.apache.org/jira/browse/HDFS-14061) | Check if the cluster topology supports the EC policy before setting, enabling or adding it |  Major | erasure-coding, hdfs | Kitti Nanasi | Kitti Nanasi |
| [HDFS-14187](https://issues.apache.org/jira/browse/HDFS-14187) | Make warning message more clear when there are not enough data nodes for EC write |  Major | erasure-coding | Kitti Nanasi | Kitti Nanasi |
| [HDFS-14125](https://issues.apache.org/jira/browse/HDFS-14125) | Use parameterized log format in ECTopologyVerifier |  Trivial | erasure-coding | Kitti Nanasi | Kitti Nanasi |
| [HDFS-14188](https://issues.apache.org/jira/browse/HDFS-14188) | Make hdfs ec -verifyClusterSetup command accept an erasure coding policy as a parameter |  Major | erasure-coding | Kitti Nanasi | Kitti Nanasi |
| [HADOOP-16126](https://issues.apache.org/jira/browse/HADOOP-16126) | ipc.Client.stop() may sleep too long to wait for all connections |  Major | ipc | Tsz-wo Sze | Tsz-wo Sze |
| [HADOOP-15014](https://issues.apache.org/jira/browse/HADOOP-15014) | KMS should log the IP address of the clients |  Major | kms | Zsombor Gegesy | Zsombor Gegesy |
| [HDFS-14460](https://issues.apache.org/jira/browse/HDFS-14460) | DFSUtil#getNamenodeWebAddr should return HTTPS address based on policy configured |  Major | . | CR Hota | CR Hota |
| [HDFS-14624](https://issues.apache.org/jira/browse/HDFS-14624) | When decommissioning a node, log remaining blocks to replicate periodically |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-13693](https://issues.apache.org/jira/browse/HDFS-13693) | Remove unnecessary search in INodeDirectory.addChild during image loading |  Major | namenode | zhouyingchao | Lisheng Sun |
| [HDFS-14313](https://issues.apache.org/jira/browse/HDFS-14313) | Get hdfs used space from FsDatasetImpl#volumeMap#ReplicaInfo in memory  instead of df/du |  Major | datanode, performance | Lisheng Sun | Lisheng Sun |
| [HDFS-14678](https://issues.apache.org/jira/browse/HDFS-14678) | Allow triggerBlockReport to a specific namenode |  Major | datanode | Leon Gao | Leon Gao |
| [HDFS-14523](https://issues.apache.org/jira/browse/HDFS-14523) | Remove excess read lock for NetworkToplogy |  Major | . | Wu Weiwei | Wu Weiwei |
| [HDFS-14497](https://issues.apache.org/jira/browse/HDFS-14497) | Write lock held by metasave impact following RPC processing |  Major | namenode | Xiaoqiao He | Xiaoqiao He |
| [HADOOP-16531](https://issues.apache.org/jira/browse/HADOOP-16531) | Log more detail for slow RPC |  Major | . | Chen Zhang | Chen Zhang |
| [YARN-9764](https://issues.apache.org/jira/browse/YARN-9764) | Print application submission context label in application summary |  Major | . | Jonathan Hung | Manoj Kumar |
| [YARN-9824](https://issues.apache.org/jira/browse/YARN-9824) | Fall back to configured queue ordering policy class name |  Major | . | Jonathan Hung | Jonathan Hung |
| [HADOOP-16069](https://issues.apache.org/jira/browse/HADOOP-16069) | Support configure ZK\_DTSM\_ZK\_KERBEROS\_PRINCIPAL in ZKDelegationTokenSecretManager using principal with Schema /\_HOST |  Minor | common | luhuachao | luhuachao |
| [YARN-9762](https://issues.apache.org/jira/browse/YARN-9762) | Add submission context label to audit logs |  Major | . | Jonathan Hung | Manoj Kumar |
| [HDFS-14850](https://issues.apache.org/jira/browse/HDFS-14850) | Optimize FileSystemAccessService#getFileSystemConfiguration |  Major | httpfs, performance | Lisheng Sun | Lisheng Sun |
| [HDFS-14192](https://issues.apache.org/jira/browse/HDFS-14192) | Track missing DFS operations in Statistics and StorageStatistics |  Major | . | Ayush Saxena | Ayush Saxena |
| [YARN-9356](https://issues.apache.org/jira/browse/YARN-9356) | Add more tests to ratio method in TestResourceCalculator |  Major | . | Szilard Nemeth | Zoltan Siegl |
| [HADOOP-16643](https://issues.apache.org/jira/browse/HADOOP-16643) | Update netty4 to the latest 4.1.42 |  Major | . | Wei-Chiu Chuang | Lisheng Sun |
| [HADOOP-16640](https://issues.apache.org/jira/browse/HADOOP-16640) | WASB: Override getCanonicalServiceName() to return full url of WASB filesystem |  Major | fs/azure | Da Zhou | Da Zhou |
| [HDFS-14915](https://issues.apache.org/jira/browse/HDFS-14915) | Move Superuser Check Before Taking Lock For Encryption API |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-14921](https://issues.apache.org/jira/browse/HDFS-14921) | Remove SuperUser Check in Setting Storage Policy in FileStatus During Listing |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-14923](https://issues.apache.org/jira/browse/HDFS-14923) | Remove dead code from HealthMonitor |  Minor | . | Hui Fei | Hui Fei |
| [YARN-9914](https://issues.apache.org/jira/browse/YARN-9914) | Use separate configs for free disk space checking for full and not-full disks |  Minor | yarn | Jim Brennan | Jim Brennan |
| [MAPREDUCE-7208](https://issues.apache.org/jira/browse/MAPREDUCE-7208) | Tuning TaskRuntimeEstimator |  Minor | . | Ahmed Hussein | Ahmed Hussein |
| [HDFS-14942](https://issues.apache.org/jira/browse/HDFS-14942) | Change Log Level to debug in JournalNodeSyncer#syncWithJournalAtIndex |  Minor | . | Lisheng Sun | Lisheng Sun |
| [HDFS-14979](https://issues.apache.org/jira/browse/HDFS-14979) | [Observer Node] Balancer should submit getBlocks to Observer Node when possible |  Major | balancer & mover, hdfs | Erik Krogen | Erik Krogen |
| [HADOOP-16705](https://issues.apache.org/jira/browse/HADOOP-16705) | MBeanInfoBuilder puts unnecessary memory pressure on the system with a debug log |  Major | metrics | Lukas Majercak | Lukas Majercak |
| [HADOOP-16712](https://issues.apache.org/jira/browse/HADOOP-16712) | Config ha.failover-controller.active-standby-elector.zk.op.retries is not in core-default.xml |  Trivial | . | Wei-Chiu Chuang | Xieming Li |
| [HDFS-14952](https://issues.apache.org/jira/browse/HDFS-14952) | Skip safemode if blockTotal is 0 in new NN |  Trivial | namenode | Rajesh Balamohan | Xiaoqiao He |
| [YARN-8842](https://issues.apache.org/jira/browse/YARN-8842) | Expose metrics for custom resource types in QueueMetrics |  Major | . | Szilard Nemeth | Szilard Nemeth |
| [YARN-9966](https://issues.apache.org/jira/browse/YARN-9966) | Code duplication in UserGroupMappingPlacementRule |  Major | . | Szilard Nemeth | Kevin Su |
| [YARN-9937](https://issues.apache.org/jira/browse/YARN-9937) | Add missing queue configs in RMWebService#CapacitySchedulerQueueInfo |  Major | capacity scheduler | Prabhu Joseph | Prabhu Joseph |
| [HADOOP-16718](https://issues.apache.org/jira/browse/HADOOP-16718) | Allow disabling Server Name Indication (SNI) for Jetty |  Major | . | Siyao Meng | Aravindan Vijayan |
| [HADOOP-16729](https://issues.apache.org/jira/browse/HADOOP-16729) | Extract version numbers to head of pom.xml |  Minor | build | Tamas Penzes | Tamas Penzes |
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
| [HADOOP-16775](https://issues.apache.org/jira/browse/HADOOP-16775) | DistCp reuses the same temp file within the task attempt for different files. |  Major | tools/distcp | Amir Shenavandeh | Amir Shenavandeh |
| [HDFS-15097](https://issues.apache.org/jira/browse/HDFS-15097) | Purge log in KMS and HttpFS |  Minor | httpfs, kms | Doris Gu | Doris Gu |
| [HADOOP-16753](https://issues.apache.org/jira/browse/HADOOP-16753) | Refactor HAAdmin |  Major | ha | Akira Ajisaka | Xieming Li |
| [HDFS-14968](https://issues.apache.org/jira/browse/HDFS-14968) | Add ability to know datanode staleness |  Minor | datanode, logging, namenode | Ahmed Hussein | Ahmed Hussein |
| [YARN-7913](https://issues.apache.org/jira/browse/YARN-7913) | Improve error handling when application recovery fails with exception |  Major | resourcemanager | Gergo Repas | Wilfred Spiegelenburg |
| [HDFS-15117](https://issues.apache.org/jira/browse/HDFS-15117) | EC: Add getECTopologyResultForPolicies to DistributedFileSystem |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-15119](https://issues.apache.org/jira/browse/HDFS-15119) | Allow expiration of cached locations in DFSInputStream |  Minor | dfsclient | Ahmed Hussein | Ahmed Hussein |
| [MAPREDUCE-7262](https://issues.apache.org/jira/browse/MAPREDUCE-7262) | MRApp helpers block for long intervals (500ms) |  Minor | mr-am | Ahmed Hussein | Ahmed Hussein |
| [MAPREDUCE-7260](https://issues.apache.org/jira/browse/MAPREDUCE-7260) | Cross origin request support for Job history server web UI |  Critical | jobhistoryserver | Adam Antal | Adam Antal |
| [YARN-10084](https://issues.apache.org/jira/browse/YARN-10084) | Allow inheritance of max app lifetime / default app lifetime |  Major | capacity scheduler | Eric Payne | Eric Payne |
| [HDFS-12491](https://issues.apache.org/jira/browse/HDFS-12491) | Support wildcard in CLASSPATH for libhdfs |  Major | libhdfs | John Zhuge | Muhammad Samir Khan |
| [YARN-10116](https://issues.apache.org/jira/browse/YARN-10116) | Expose diagnostics in RMAppManager summary |  Major | . | Jonathan Hung | Jonathan Hung |
| [HADOOP-16739](https://issues.apache.org/jira/browse/HADOOP-16739) | Fix native build failure of hadoop-pipes on CentOS 8 |  Major | tools/pipes | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-16847](https://issues.apache.org/jira/browse/HADOOP-16847) | Test TestGroupsCaching fail if HashSet iterates in a different order |  Minor | test | testfixer0 | testfixer0 |
| [HDFS-14758](https://issues.apache.org/jira/browse/HDFS-14758) | Decrease lease hard limit |  Minor | . | Eric Payne | Hemanth Boyina |
| [HDFS-15086](https://issues.apache.org/jira/browse/HDFS-15086) | Block scheduled counter never get decremet if the block got deleted before replication. |  Major | 3.1.1 | Surendra Singh Lilhore | Hemanth Boyina |
| [HDFS-15174](https://issues.apache.org/jira/browse/HDFS-15174) | Optimize ReplicaCachingGetSpaceUsed by reducing unnecessary io operations |  Major | . | Lisheng Sun | Lisheng Sun |
| [YARN-9018](https://issues.apache.org/jira/browse/YARN-9018) | Add functionality to AuxiliaryLocalPathHandler to return all locations to read for a given path |  Major | . | Kuhu Shukla | Kuhu Shukla |
| [HDFS-14861](https://issues.apache.org/jira/browse/HDFS-14861) | Reset LowRedundancyBlocks Iterator periodically |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [HADOOP-16899](https://issues.apache.org/jira/browse/HADOOP-16899) | Update HdfsDesign.md to reduce ambiguity |  Minor | documentation | Akshay Nehe | Akshay Nehe |
| [HADOOP-16772](https://issues.apache.org/jira/browse/HADOOP-16772) | Extract version numbers to head of pom.xml (addendum) |  Major | build | Tamas Penzes | Tamas Penzes |
| [HDFS-15197](https://issues.apache.org/jira/browse/HDFS-15197) | [SBN read] Change ObserverRetryOnActiveException log to debug |  Minor | hdfs | Chen Liang | Chen Liang |
| [HADOOP-16935](https://issues.apache.org/jira/browse/HADOOP-16935) | Backport HADOOP-10848. Cleanup calling of sun.security.krb5.Config to branch-3.2 |  Minor | . | Siyao Meng | Siyao Meng |
| [YARN-10200](https://issues.apache.org/jira/browse/YARN-10200) | Add number of containers to RMAppManager summary |  Major | . | Jonathan Hung | Jonathan Hung |
| [HADOOP-16952](https://issues.apache.org/jira/browse/HADOOP-16952) | Add .diff to gitignore |  Minor | . | Ayush Saxena | Ayush Saxena |
| [MAPREDUCE-7266](https://issues.apache.org/jira/browse/MAPREDUCE-7266) | historyContext doesn't need to be a class attribute inside JobHistoryServer |  Minor | jobhistoryserver | Siddharth Ahuja | Siddharth Ahuja |
| [YARN-10003](https://issues.apache.org/jira/browse/YARN-10003) | YarnConfigurationStore#checkVersion throws exception that belongs to RMStateStore |  Major | . | Szilard Nemeth | Benjamin Teke |
| [YARN-10212](https://issues.apache.org/jira/browse/YARN-10212) | Create separate configuration for max global AM attempts |  Major | . | Jonathan Hung | Bilwa S T |
| [YARN-5277](https://issues.apache.org/jira/browse/YARN-5277) | When localizers fail due to resource timestamps being out, provide more diagnostics |  Major | nodemanager | Steve Loughran | Siddharth Ahuja |
| [YARN-9995](https://issues.apache.org/jira/browse/YARN-9995) | Code cleanup in TestSchedConfCLI |  Minor | . | Szilard Nemeth | Bilwa S T |
| [YARN-9354](https://issues.apache.org/jira/browse/YARN-9354) | Resources should be created with ResourceTypesTestHelper instead of TestUtils |  Trivial | . | Szilard Nemeth | Andras Gyori |
| [YARN-10002](https://issues.apache.org/jira/browse/YARN-10002) | Code cleanup and improvements in ConfigurationStoreBaseTest |  Minor | . | Szilard Nemeth | Benjamin Teke |
| [YARN-9954](https://issues.apache.org/jira/browse/YARN-9954) | Configurable max application tags and max tag length |  Major | . | Jonathan Hung | Bilwa S T |
| [YARN-10001](https://issues.apache.org/jira/browse/YARN-10001) | Add explanation of unimplemented methods in InMemoryConfigurationStore |  Major | . | Szilard Nemeth | Siddharth Ahuja |
| [HADOOP-17001](https://issues.apache.org/jira/browse/HADOOP-17001) | The suffix name of the unified compression class |  Major | io | bianqi | bianqi |
| [YARN-9997](https://issues.apache.org/jira/browse/YARN-9997) | Code cleanup in ZKConfigurationStore |  Minor | . | Szilard Nemeth | Andras Gyori |
| [YARN-9996](https://issues.apache.org/jira/browse/YARN-9996) | Code cleanup in QueueAdminConfigurationMutationACLPolicy |  Major | . | Szilard Nemeth | Siddharth Ahuja |
| [YARN-9998](https://issues.apache.org/jira/browse/YARN-9998) | Code cleanup in LeveldbConfigurationStore |  Minor | . | Szilard Nemeth | Benjamin Teke |
| [YARN-9999](https://issues.apache.org/jira/browse/YARN-9999) | TestFSSchedulerConfigurationStore: Extend from ConfigurationStoreBaseTest, general code cleanup |  Minor | . | Szilard Nemeth | Benjamin Teke |
| [HDFS-15295](https://issues.apache.org/jira/browse/HDFS-15295) | AvailableSpaceBlockPlacementPolicy should use chooseRandomWithStorageTypeTwoTrial() for better performance. |  Minor | . | Jinglun | Jinglun |
| [YARN-10189](https://issues.apache.org/jira/browse/YARN-10189) | Code cleanup in LeveldbRMStateStore |  Minor | . | Benjamin Teke | Benjamin Teke |
| [HADOOP-16886](https://issues.apache.org/jira/browse/HADOOP-16886) | Add hadoop.http.idle\_timeout.ms to core-default.xml |  Major | . | Wei-Chiu Chuang | Lisheng Sun |
| [YARN-10260](https://issues.apache.org/jira/browse/YARN-10260) | Allow transitioning queue from DRAINING to RUNNING state |  Major | . | Jonathan Hung | Bilwa S T |
| [HADOOP-17042](https://issues.apache.org/jira/browse/HADOOP-17042) | Hadoop distcp throws "ERROR: Tools helper ///usr/lib/hadoop/libexec/tools/hadoop-distcp.sh was not found" |  Minor | tools/distcp | Aki Tanaka | Aki Tanaka |
| [HADOOP-14698](https://issues.apache.org/jira/browse/HADOOP-14698) | Make copyFromLocal's -t option available for put as well |  Major | . | Andras Bokor | Andras Bokor |
| [YARN-6492](https://issues.apache.org/jira/browse/YARN-6492) | Generate queue metrics for each partition |  Major | capacity scheduler | Jonathan Hung | Manikandan R |
| [HADOOP-17047](https://issues.apache.org/jira/browse/HADOOP-17047) | TODO comments exist in trunk while the related issues are already fixed. |  Trivial | . | Rungroj Maipradit | Rungroj Maipradit |
| [HDFS-15406](https://issues.apache.org/jira/browse/HDFS-15406) | Improve the speed of Datanode Block Scan |  Major | . | Hemanth Boyina | Hemanth Boyina |
| [HADOOP-17090](https://issues.apache.org/jira/browse/HADOOP-17090) | Increase precommit job timeout from 5 hours to 20 hours |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [YARN-10297](https://issues.apache.org/jira/browse/YARN-10297) | TestContinuousScheduling#testFairSchedulerContinuousSchedulingInitTime fails intermittently |  Major | . | Jonathan Hung | Jim Brennan |
| [HADOOP-17127](https://issues.apache.org/jira/browse/HADOOP-17127) | Use RpcMetrics.TIMEUNIT to initialize rpc queueTime and processingTime |  Minor | common | Jim Brennan | Jim Brennan |
| [HDFS-15404](https://issues.apache.org/jira/browse/HDFS-15404) | ShellCommandFencer should expose info about source |  Major | . | Chen Liang | Chen Liang |
| [HADOOP-17147](https://issues.apache.org/jira/browse/HADOOP-17147) | Dead link in hadoop-kms/index.md.vm |  Minor | documentation, kms | Akira Ajisaka | Xieming Li |
| [YARN-10343](https://issues.apache.org/jira/browse/YARN-10343) | Legacy RM UI should include labeled metrics for allocated, total, and reserved resources. |  Major | . | Eric Payne | Eric Payne |
| [YARN-1529](https://issues.apache.org/jira/browse/YARN-1529) | Add Localization overhead metrics to NM |  Major | nodemanager | Gera Shegalov | Jim Brennan |
| [YARN-10251](https://issues.apache.org/jira/browse/YARN-10251) | Show extended resources on legacy RM UI. |  Major | . | Eric Payne | Eric Payne |
| [HADOOP-17159](https://issues.apache.org/jira/browse/HADOOP-17159) | Make UGI support forceful relogin from keytab ignoring the last login time |  Major | security | Sandeep Guggilam | Sandeep Guggilam |
| [YARN-10353](https://issues.apache.org/jira/browse/YARN-10353) | Log vcores used and cumulative cpu in containers monitor |  Minor | yarn | Jim Brennan | Jim Brennan |
| [YARN-10369](https://issues.apache.org/jira/browse/YARN-10369) | Make NMTokenSecretManagerInRM sending NMToken for nodeId DEBUG |  Minor | yarn | Jim Brennan | Jim Brennan |
| [YARN-10390](https://issues.apache.org/jira/browse/YARN-10390) | LeafQueue: retain user limits cache across assignContainers() calls |  Major | capacity scheduler, capacityscheduler | Muhammad Samir Khan | Muhammad Samir Khan |
| [HDFS-15574](https://issues.apache.org/jira/browse/HDFS-15574) | Remove unnecessary sort of block list in DirectoryScanner |  Major | . | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-15583](https://issues.apache.org/jira/browse/HDFS-15583) | Backport DirectoryScanner improvements HDFS-14476, HDFS-14751 and HDFS-15048 to branch 3.2 and 3.1 |  Major | datanode | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-15581](https://issues.apache.org/jira/browse/HDFS-15581) | Access Controlled HTTPFS Proxy |  Minor | httpfs | Richard | Richard |
| [HDFS-15415](https://issues.apache.org/jira/browse/HDFS-15415) | Reduce locking in Datanode DirectoryScanner |  Major | datanode | Stephen O'Donnell | Stephen O'Donnell |
| [HADOOP-17287](https://issues.apache.org/jira/browse/HADOOP-17287) | Support new Instance by non default constructor by ReflectionUtils |  Major | . | Baolong Mao | Baolong Mao |
| [YARN-10451](https://issues.apache.org/jira/browse/YARN-10451) | RM (v1) UI NodesPage can NPE when yarn.io/gpu resource type is defined. |  Major | . | Eric Payne | Eric Payne |
| [YARN-9667](https://issues.apache.org/jira/browse/YARN-9667) | Container-executor.c duplicates messages to stdout |  Major | nodemanager, yarn | Adam Antal | Peter Bacsko |
| [MAPREDUCE-7301](https://issues.apache.org/jira/browse/MAPREDUCE-7301) | Expose Mini MR Cluster attribute for testing |  Minor | test | Swaroopa Kadam | Swaroopa Kadam |
| [HDFS-15567](https://issues.apache.org/jira/browse/HDFS-15567) | [SBN Read] HDFS should expose msync() API to allow downstream applications call it explicitly. |  Major | ha, hdfs-client | Konstantin Shvachko | Konstantin Shvachko |
| [YARN-10450](https://issues.apache.org/jira/browse/YARN-10450) | Add cpu and memory utilization per node and cluster-wide metrics |  Minor | yarn | Jim Brennan | Jim Brennan |
| [YARN-10475](https://issues.apache.org/jira/browse/YARN-10475) | Scale RM-NM heartbeat interval based on node utilization |  Minor | yarn | Jim Brennan | Jim Brennan |
| [HDFS-15665](https://issues.apache.org/jira/browse/HDFS-15665) | Balancer logging improvement |  Major | balancer & mover | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-17342](https://issues.apache.org/jira/browse/HADOOP-17342) | Creating a token identifier should not do kerberos name resolution |  Major | common | Jim Brennan | Jim Brennan |
| [YARN-10479](https://issues.apache.org/jira/browse/YARN-10479) | RMProxy should retry on SocketTimeout Exceptions |  Major | yarn | Jim Brennan | Jim Brennan |
| [HDFS-15623](https://issues.apache.org/jira/browse/HDFS-15623) | Respect configured values of rpc.engine |  Major | hdfs | Hector Sandoval Chaverri | Hector Sandoval Chaverri |
| [HDFS-14395](https://issues.apache.org/jira/browse/HDFS-14395) | Remove WARN Logging From Interrupts in DataStreamer |  Minor | hdfs-client | David Mollitor | David Mollitor |
| [HADOOP-17367](https://issues.apache.org/jira/browse/HADOOP-17367) | Add InetAddress api to ProxyUsers.authorize |  Major | performance, security | Ahmed Hussein | Ahmed Hussein |
| [MAPREDUCE-7304](https://issues.apache.org/jira/browse/MAPREDUCE-7304) | Enhance the map-reduce Job end notifier to be able to notify the given URL via a custom class |  Major | mrv2 | Daniel Fritsi | Zoltán Erdmann |
| [MAPREDUCE-7309](https://issues.apache.org/jira/browse/MAPREDUCE-7309) | Improve performance of reading resource request for mapper/reducers from config |  Major | applicationmaster | Wangda Tan | Peter Bacsko |
| [HADOOP-17389](https://issues.apache.org/jira/browse/HADOOP-17389) | KMS should log full UGI principal |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15717](https://issues.apache.org/jira/browse/HDFS-15717) | Improve fsck logging |  Major | logging, namenode | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15751](https://issues.apache.org/jira/browse/HDFS-15751) | Add documentation for msync() API to filesystem.md |  Major | documentation | Konstantin Shvachko | Konstantin Shvachko |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-15418](https://issues.apache.org/jira/browse/HADOOP-15418) | Hadoop KMSAuthenticationFilter needs to use getPropsByPrefix instead of iterator to avoid ConcurrentModificationException |  Major | common | Suma Shivaprasad | Suma Shivaprasad |
| [HDFS-14004](https://issues.apache.org/jira/browse/HDFS-14004) | TestLeaseRecovery2#testCloseWhileRecoverLease fails intermittently in trunk |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-13959](https://issues.apache.org/jira/browse/HDFS-13959) | TestUpgradeDomainBlockPlacementPolicy is flaky |  Major | . | Ayush Saxena | Ayush Saxena |
| [YARN-8948](https://issues.apache.org/jira/browse/YARN-8948) | PlacementRule interface should be for all YarnSchedulers |  Major | . | Bibin Chundatt | Bibin Chundatt |
| [HADOOP-16013](https://issues.apache.org/jira/browse/HADOOP-16013) | DecayRpcScheduler decay thread should run as a daemon |  Major | ipc | Erik Krogen | Erik Krogen |
| [HDFS-14175](https://issues.apache.org/jira/browse/HDFS-14175) | EC: Native XOR decoder should reset the output buffer before using it. |  Major | ec, hdfs | Surendra Singh Lilhore | Ayush Saxena |
| [HDFS-14202](https://issues.apache.org/jira/browse/HDFS-14202) | "dfs.disk.balancer.max.disk.throughputInMBperSec" property is not working as per set value. |  Major | diskbalancer | Ranith Sardar | Ranith Sardar |
| [HADOOP-16127](https://issues.apache.org/jira/browse/HADOOP-16127) | In ipc.Client, put a new connection could happen after stop |  Major | ipc | Tsz-wo Sze | Tsz-wo Sze |
| [YARN-4901](https://issues.apache.org/jira/browse/YARN-4901) | QueueMetrics needs to be cleared before MockRM is initialized |  Major | scheduler | Daniel Templeton | Peter Bacsko |
| [HADOOP-16161](https://issues.apache.org/jira/browse/HADOOP-16161) | NetworkTopology#getWeightUsingNetworkLocation return unexpected result |  Major | net | Xiaoqiao He | Xiaoqiao He |
| [HDFS-14434](https://issues.apache.org/jira/browse/HDFS-14434) | webhdfs that connect secure hdfs should not use user.name parameter |  Minor | webhdfs | KWON BYUNGCHANG | KWON BYUNGCHANG |
| [HDFS-14527](https://issues.apache.org/jira/browse/HDFS-14527) | Stop all DataNodes may result in NN terminate |  Major | namenode | Xiaoqiao He | Xiaoqiao He |
| [HDFS-14494](https://issues.apache.org/jira/browse/HDFS-14494) | Move Server logging of StatedId inside receiveRequestState() |  Major | . | Konstantin Shvachko | Shweta |
| [HDFS-14599](https://issues.apache.org/jira/browse/HDFS-14599) | HDFS-12487 breaks test TestDiskBalancer.testDiskBalancerWithFedClusterWithOneNameServiceEmpty |  Major | diskbalancer | Wei-Chiu Chuang | Xiaoqiao He |
| [HDFS-14618](https://issues.apache.org/jira/browse/HDFS-14618) | Incorrect synchronization of ArrayList field (ArrayList is thread-unsafe). |  Critical | . | Paul Ward | Paul Ward |
| [HDFS-14610](https://issues.apache.org/jira/browse/HDFS-14610) | HashMap is not thread safe. Field storageMap is typically synchronized by storageMap. However, in one place, field storageMap is not protected with synchronized. |  Critical | . | Paul Ward | Paul Ward |
| [HDFS-14499](https://issues.apache.org/jira/browse/HDFS-14499) | Misleading REM\_QUOTA value with snapshot and trash feature enabled for a directory |  Major | snapshots | Shashikant Banerjee | Shashikant Banerjee |
| [HADOOP-16451](https://issues.apache.org/jira/browse/HADOOP-16451) | Update jackson-databind to 2.9.9.1 |  Major | . | Wei-Chiu Chuang | Siyao Meng |
| [HDFS-14647](https://issues.apache.org/jira/browse/HDFS-14647) | NPE during secure namenode startup |  Major | hdfs | Fengnan Li | Fengnan Li |
| [HADOOP-16461](https://issues.apache.org/jira/browse/HADOOP-16461) | Regression: FileSystem cache lock parses XML within the lock |  Major | fs | Gopal Vijayaraghavan | Gopal Vijayaraghavan |
| [HDFS-14660](https://issues.apache.org/jira/browse/HDFS-14660) | [SBN Read] ObserverNameNode should throw StandbyException for requests not from ObserverProxyProvider |  Major | . | Chao Sun | Chao Sun |
| [HADOOP-16460](https://issues.apache.org/jira/browse/HADOOP-16460) | ABFS: fix for Sever Name Indication (SNI) |  Major | fs/azure | Thomas Marqardt | Sneha Vijayarajan |
| [HDFS-14569](https://issues.apache.org/jira/browse/HDFS-14569) | Result of crypto -listZones is not formatted properly |  Major | . | Hemanth Boyina | Hemanth Boyina |
| [HADOOP-12282](https://issues.apache.org/jira/browse/HADOOP-12282) | Connection thread's name should be updated after address changing is detected |  Major | ipc | zhouyingchao | Lisheng Sun |
| [HDFS-14686](https://issues.apache.org/jira/browse/HDFS-14686) | HttpFS: HttpFSFileSystem#getErasureCodingPolicy always returns null |  Major | httpfs | Siyao Meng | Siyao Meng |
| [HADOOP-15865](https://issues.apache.org/jira/browse/HADOOP-15865) | ConcurrentModificationException in Configuration.overlay() method |  Major | . | Oleksandr Shevchenko | Oleksandr Shevchenko |
| [HADOOP-16487](https://issues.apache.org/jira/browse/HADOOP-16487) | Update jackson-databind to 2.9.9.2 |  Critical | . | Siyao Meng | Siyao Meng |
| [HDFS-14759](https://issues.apache.org/jira/browse/HDFS-14759) | HDFS cat logs an info message |  Major | . | Eric Badger | Eric Badger |
| [HADOOP-16533](https://issues.apache.org/jira/browse/HADOOP-16533) | Update jackson-databind to 2.9.9.3 |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [HDFS-14699](https://issues.apache.org/jira/browse/HDFS-14699) | Erasure Coding: Storage not considered in live replica when replication streams hard limit reached to threshold |  Critical | ec | Zhao Yi Ming | Zhao Yi Ming |
| [YARN-9833](https://issues.apache.org/jira/browse/YARN-9833) | Race condition when DirectoryCollection.checkDirs() runs during container launch |  Major | . | Peter Bacsko | Peter Bacsko |
| [YARN-9837](https://issues.apache.org/jira/browse/YARN-9837) | YARN Service fails to fetch status for Stopped apps with bigger spec files |  Major | yarn-native-services | Tarun Parimi | Tarun Parimi |
| [YARN-2255](https://issues.apache.org/jira/browse/YARN-2255) | YARN Audit logging not added to log4j.properties |  Major | . | Varun Saxena | Aihua Xu |
| [HDFS-14836](https://issues.apache.org/jira/browse/HDFS-14836) | FileIoProvider should not increase FileIoErrors metric in datanode volume metric |  Minor | . | Aiphago | Aiphago |
| [HADOOP-16582](https://issues.apache.org/jira/browse/HADOOP-16582) | LocalFileSystem's mkdirs() does not work as expected under viewfs. |  Major | . | Kihwal Lee | Kihwal Lee |
| [HADOOP-16581](https://issues.apache.org/jira/browse/HADOOP-16581) | ValueQueue does not trigger an async refill when number of values falls below watermark |  Major | common, kms | Yuval Degani | Yuval Degani |
| [HDFS-14853](https://issues.apache.org/jira/browse/HDFS-14853) | NPE in DFSNetworkTopology#chooseRandomWithStorageType() when the excludedNode is not present |  Major | . | Ranith Sardar | Ranith Sardar |
| [HDFS-13660](https://issues.apache.org/jira/browse/HDFS-13660) | DistCp job fails when new data is appended in the file while the distCp copy job is running |  Critical | distcp | Mukund Thakur | Mukund Thakur |
| [HDFS-14808](https://issues.apache.org/jira/browse/HDFS-14808) | EC: Improper size values for corrupt ec block in LOG |  Major | ec | Harshakiran Reddy | Ayush Saxena |
| [HDFS-14849](https://issues.apache.org/jira/browse/HDFS-14849) | Erasure Coding: the internal block is replicated many times when datanode is decommissioning |  Major | ec, erasure-coding | HuangTao | HuangTao |
| [YARN-9858](https://issues.apache.org/jira/browse/YARN-9858) | Optimize RMContext getExclusiveEnforcedPartitions |  Major | . | Jonathan Hung | Jonathan Hung |
| [HDFS-14492](https://issues.apache.org/jira/browse/HDFS-14492) | Snapshot memory leak |  Major | snapshots | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-14418](https://issues.apache.org/jira/browse/HDFS-14418) | Remove redundant super user priveledge checks from namenode. |  Major | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-16619](https://issues.apache.org/jira/browse/HADOOP-16619) | Upgrade jackson and jackson-databind to 2.9.10 |  Major | . | Siyao Meng | Siyao Meng |
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
| [HADOOP-16662](https://issues.apache.org/jira/browse/HADOOP-16662) | Remove unnecessary InnerNode check in NetworkTopology#add() |  Minor | . | Lisheng Sun | Lisheng Sun |
| [HDFS-14847](https://issues.apache.org/jira/browse/HDFS-14847) | Erasure Coding: Blocks are over-replicated while EC decommissioning |  Critical | ec | Hui Fei | Hui Fei |
| [HDFS-14913](https://issues.apache.org/jira/browse/HDFS-14913) | Correct the value of available count in DFSNetworkTopology#chooseRandomWithStorageType() |  Major | . | Ayush Saxena | Ayush Saxena |
| [YARN-9915](https://issues.apache.org/jira/browse/YARN-9915) | Fix FindBug issue in QueueMetrics |  Minor | . | Prabhu Joseph | Prabhu Joseph |
| [HDFS-12749](https://issues.apache.org/jira/browse/HDFS-12749) | DN may not send block report to NN after NN restart |  Major | datanode | TanYuxin | Xiaoqiao He |
| [HDFS-13901](https://issues.apache.org/jira/browse/HDFS-13901) | INode access time is ignored because of race between open and rename |  Major | . | Jinglun | Jinglun |
| [HDFS-14910](https://issues.apache.org/jira/browse/HDFS-14910) | Rename Snapshot with Pre Descendants Fail With IllegalArgumentException. |  Blocker | . | Íñigo Goiri | Wei-Chiu Chuang |
| [HDFS-14308](https://issues.apache.org/jira/browse/HDFS-14308) | DFSStripedInputStream curStripeBuf is not freed by unbuffer() |  Major | ec | Joe McDonnell | Zhao Yi Ming |
| [HDFS-14931](https://issues.apache.org/jira/browse/HDFS-14931) | hdfs crypto commands limit column width |  Major | . | Eric Badger | Eric Badger |
| [HADOOP-16669](https://issues.apache.org/jira/browse/HADOOP-16669) | TestRawLocalFileSystemContract.testPermission fails if no native library |  Minor | common, test | Steve Loughran | Steve Loughran |
| [HDFS-14920](https://issues.apache.org/jira/browse/HDFS-14920) | Erasure Coding: Decommission may hang If one or more datanodes are out of service during decommission |  Major | ec | Hui Fei | Hui Fei |
| [HDFS-13736](https://issues.apache.org/jira/browse/HDFS-13736) | BlockPlacementPolicyDefault can not choose favored nodes when 'dfs.namenode.block-placement-policy.default.prefer-local-node' set to false |  Major | . | hu xiaodong | hu xiaodong |
| [HDFS-14925](https://issues.apache.org/jira/browse/HDFS-14925) | rename operation should check nest snapshot |  Major | namenode | Junwang Zhao | Junwang Zhao |
| [YARN-9949](https://issues.apache.org/jira/browse/YARN-9949) | Add missing queue configs for root queue in RMWebService#CapacitySchedulerInfo |  Minor | capacity scheduler | Prabhu Joseph | Prabhu Joseph |
| [HDFS-14945](https://issues.apache.org/jira/browse/HDFS-14945) | Revise PacketResponder's log. |  Minor | datanode | Xudong Cao | Xudong Cao |
| [HDFS-14946](https://issues.apache.org/jira/browse/HDFS-14946) | Erasure Coding: Block recovery failed during decommissioning |  Major | . | Hui Fei | Hui Fei |
| [HDFS-14384](https://issues.apache.org/jira/browse/HDFS-14384) | When lastLocatedBlock token expire, it will take 1~3s second to refetch it. |  Major | hdfs-client | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-14806](https://issues.apache.org/jira/browse/HDFS-14806) | Bootstrap standby may fail if used in-progress tailing |  Major | namenode | Chen Liang | Chen Liang |
| [HDFS-14941](https://issues.apache.org/jira/browse/HDFS-14941) | Potential editlog race condition can cause corrupted file |  Major | namenode | Chen Liang | Chen Liang |
| [HDFS-14958](https://issues.apache.org/jira/browse/HDFS-14958) | TestBalancerWithNodeGroup is not using NetworkTopologyWithNodeGroup |  Minor | hdfs | Jim Brennan | Jim Brennan |
| [HDFS-14720](https://issues.apache.org/jira/browse/HDFS-14720) | DataNode shouldn't report block as bad block if the block length is Long.MAX\_VALUE. |  Major | datanode | Surendra Singh Lilhore | Hemanth Boyina |
| [HADOOP-16676](https://issues.apache.org/jira/browse/HADOOP-16676) | Backport HADOOP-16152 to branch-3.2 |  Major | common | DW | Siyao Meng |
| [HADOOP-16677](https://issues.apache.org/jira/browse/HADOOP-16677) | Recalculate the remaining timeout millis correctly while throwing an InterupptedException in SocketIOWithTimeout. |  Minor | common | Xudong Cao | Xudong Cao |
| [HDFS-14884](https://issues.apache.org/jira/browse/HDFS-14884) | Add sanity check that zone key equals feinfo key while setting Xattrs |  Major | encryption, hdfs | Mukul Kumar Singh | Mukul Kumar Singh |
| [HADOOP-15097](https://issues.apache.org/jira/browse/HADOOP-15097) | AbstractContractDeleteTest::testDeleteNonEmptyDirRecursive with misleading path |  Minor | fs, test | zhoutai.zt | Xieming Li |
| [HADOOP-16710](https://issues.apache.org/jira/browse/HADOOP-16710) | testing\_azure.md documentation is misleading |  Major | fs/azure, test | Andras Bokor | Andras Bokor |
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
| [HADOOP-16685](https://issues.apache.org/jira/browse/HADOOP-16685) | FileSystem#listStatusIterator does not check if given path exists |  Major | fs | Sahil Takiar | Sahil Takiar |
| [MAPREDUCE-7240](https://issues.apache.org/jira/browse/MAPREDUCE-7240) | Exception ' Invalid event: TA\_TOO\_MANY\_FETCH\_FAILURE at SUCCESS\_FINISHING\_CONTAINER' cause job error |  Critical | . | luhuachao | luhuachao |
| [MAPREDUCE-7249](https://issues.apache.org/jira/browse/MAPREDUCE-7249) | Invalid event TA\_TOO\_MANY\_FETCH\_FAILURE at SUCCESS\_CONTAINER\_CLEANUP causes job failure |  Critical | applicationmaster, mrv2 | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [YARN-9993](https://issues.apache.org/jira/browse/YARN-9993) | Remove incorrectly committed files from YARN-9011 |  Major | yarn | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [HDFS-15010](https://issues.apache.org/jira/browse/HDFS-15010) | BlockPoolSlice#addReplicaThreadPool static pool should be initialized by static method |  Major | datanode | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HADOOP-16744](https://issues.apache.org/jira/browse/HADOOP-16744) |  Fix building instruction to enable zstd |  Minor | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-9985](https://issues.apache.org/jira/browse/YARN-9985) | Unsupported "transitionToObserver" option displaying for rmadmin command |  Minor | RM, yarn | Souryakanta Dwivedy | Ayush Saxena |
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
| [MAPREDUCE-7247](https://issues.apache.org/jira/browse/MAPREDUCE-7247) | Modify HistoryServerRest.html content,change The job attempt id‘s datatype from string to int |  Major | documentation | zhaoshengjie | zhaoshengjie |
| [YARN-9970](https://issues.apache.org/jira/browse/YARN-9970) | Refactor TestUserGroupMappingPlacementRule#verifyQueueMapping |  Major | . | Manikandan R | Manikandan R |
| [YARN-8148](https://issues.apache.org/jira/browse/YARN-8148) | Update decimal values for queue capacities shown on queue status CLI |  Major | client | Prabhu Joseph | Prabhu Joseph |
| [HADOOP-16808](https://issues.apache.org/jira/browse/HADOOP-16808) | Use forkCount and reuseForks parameters instead of forkMode in the config of maven surefire plugin |  Minor | build | Akira Ajisaka | Xieming Li |
| [HADOOP-16793](https://issues.apache.org/jira/browse/HADOOP-16793) |  Remove WARN log when ipc connection interrupted in Client#handleSaslConnectionFailure() |  Minor | . | Lisheng Sun | Lisheng Sun |
| [YARN-9462](https://issues.apache.org/jira/browse/YARN-9462) | TestResourceTrackerService.testNodeRemovalGracefully fails sporadically |  Minor | resourcemanager, test | Prabhu Joseph | Prabhu Joseph |
| [YARN-9790](https://issues.apache.org/jira/browse/YARN-9790) | Failed to set default-application-lifetime if maximum-application-lifetime is less than or equal to zero |  Major | . | kyungwan nam | kyungwan nam |
| [HDFS-14993](https://issues.apache.org/jira/browse/HDFS-14993) | checkDiskError doesn't work during datanode startup |  Major | datanode | Yang Yun | Yang Yun |
| [HDFS-13179](https://issues.apache.org/jira/browse/HDFS-13179) | TestLazyPersistReplicaRecovery#testDnRestartWithSavedReplicas fails intermittently |  Critical | fs | Gabor Bota | Ahmed Hussein |
| [MAPREDUCE-7259](https://issues.apache.org/jira/browse/MAPREDUCE-7259) | testSpeculateSuccessfulWithUpdateEvents fails Intermittently |  Minor | . | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15146](https://issues.apache.org/jira/browse/HDFS-15146) | TestBalancerRPCDelay.testBalancerRPCDelay fails intermittently |  Minor | . | Ahmed Hussein | Ahmed Hussein |
| [MAPREDUCE-7079](https://issues.apache.org/jira/browse/MAPREDUCE-7079) | JobHistory#ServiceStop implementation is incorrect |  Major | . | Jason Darrell Lowe | Ahmed Hussein |
| [HDFS-15118](https://issues.apache.org/jira/browse/HDFS-15118) | [SBN Read] Slow clients when Observer reads are enabled but there are no Observers on the cluster. |  Major | hdfs-client | Konstantin Shvachko | Chen Liang |
| [HDFS-7175](https://issues.apache.org/jira/browse/HDFS-7175) | Client-side SocketTimeoutException during Fsck |  Major | namenode | Carl Steinbach | Stephen O'Donnell |
| [HDFS-15148](https://issues.apache.org/jira/browse/HDFS-15148) | dfs.namenode.send.qop.enabled should not apply to primary NN port |  Major | . | Chen Liang | Chen Liang |
| [HADOOP-16410](https://issues.apache.org/jira/browse/HADOOP-16410) | Hadoop 3.2 azure jars incompatible with alpine 3.9 |  Minor | fs/azure | Jose Luis Pedrosa |  |
| [HDFS-15115](https://issues.apache.org/jira/browse/HDFS-15115) | Namenode crash caused by NPE in BlockPlacementPolicyDefault when dynamically change logger to debug |  Major | . | wangzhixiang | wangzhixiang |
| [HDFS-15158](https://issues.apache.org/jira/browse/HDFS-15158) | The number of failed volumes mismatch  with volumeFailures of Datanode metrics |  Minor | datanode | Yang Yun | Yang Yun |
| [HADOOP-16849](https://issues.apache.org/jira/browse/HADOOP-16849) | start-build-env.sh behaves incorrectly when username is numeric only |  Minor | build | Jihyun Cho | Jihyun Cho |
| [HDFS-15161](https://issues.apache.org/jira/browse/HDFS-15161) | When evictableMmapped or evictable size is zero, do not throw NoSuchElementException in ShortCircuitCache#close() |  Major | . | Lisheng Sun | Lisheng Sun |
| [HDFS-15164](https://issues.apache.org/jira/browse/HDFS-15164) | Fix TestDelegationTokensWithHA |  Major | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-16868](https://issues.apache.org/jira/browse/HADOOP-16868) | ipc.Server readAndProcess threw NullPointerException |  Major | rpc-server | Tsz-wo Sze | Tsz-wo Sze |
| [HADOOP-16869](https://issues.apache.org/jira/browse/HADOOP-16869) |  Upgrade findbugs-maven-plugin to 3.0.5 to fix mvn findbugs:findbugs failure |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15052](https://issues.apache.org/jira/browse/HDFS-15052) | WebHDFS getTrashRoot leads to OOM due to FileSystem object creation |  Major | webhdfs | Wei-Chiu Chuang | Masatake Iwasaki |
| [HDFS-15185](https://issues.apache.org/jira/browse/HDFS-15185) | StartupProgress reports edits segments until the entire startup completes |  Major | namenode | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-15166](https://issues.apache.org/jira/browse/HDFS-15166) | Remove redundant field fStream in ByteStringLog |  Major | . | Konstantin Shvachko | Xieming Li |
| [YARN-10143](https://issues.apache.org/jira/browse/YARN-10143) | YARN-10101 broke Yarn logs CLI |  Blocker | yarn | Adam Antal | Adam Antal |
| [HADOOP-16841](https://issues.apache.org/jira/browse/HADOOP-16841) | The description of hadoop.http.authentication.signature.secret.file contains outdated information |  Minor | documentation | Akira Ajisaka | Xieming Li |
| [YARN-10156](https://issues.apache.org/jira/browse/YARN-10156) | Fix typo 'complaint' which means quite different in Federation.md |  Minor | documentation, federation | Sungpeo Kook | Sungpeo Kook |
| [HDFS-15147](https://issues.apache.org/jira/browse/HDFS-15147) | LazyPersistTestCase wait logic is error-prone |  Minor | . | Ahmed Hussein | Ahmed Hussein |
| [HDFS-14668](https://issues.apache.org/jira/browse/HDFS-14668) | Support Fuse with Users from multiple Security Realms |  Critical | fuse-dfs | Sailesh Patel | István Fajth |
| [HDFS-15111](https://issues.apache.org/jira/browse/HDFS-15111) | stopStandbyServices() should log which service state it is transitioning from. |  Major | hdfs, logging | Konstantin Shvachko | Xieming Li |
| [HDFS-15199](https://issues.apache.org/jira/browse/HDFS-15199) | NPE in BlockSender |  Major | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-16891](https://issues.apache.org/jira/browse/HADOOP-16891) | Upgrade jackson-databind to 2.9.10.3 |  Blocker | . | Siyao Meng | Siyao Meng |
| [HDFS-15204](https://issues.apache.org/jira/browse/HDFS-15204) | TestRetryCacheWithHA testRemoveCacheDescriptor fails intermittently |  Major | hdfs | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-16840](https://issues.apache.org/jira/browse/HADOOP-16840) | AliyunOSS: getFileStatus throws FileNotFoundException in versioning bucket |  Major | fs/oss | wujinhu | wujinhu |
| [YARN-9419](https://issues.apache.org/jira/browse/YARN-9419) | Log a warning if GPU isolation is enabled but LinuxContainerExecutor is disabled |  Major | . | Szilard Nemeth | Andras Gyori |
| [YARN-9427](https://issues.apache.org/jira/browse/YARN-9427) | TestContainerSchedulerQueuing.testKillOnlyRequiredOpportunisticContainers fails sporadically |  Major | scheduler, test | Prabhu Joseph | Ahmed Hussein |
| [HDFS-15135](https://issues.apache.org/jira/browse/HDFS-15135) | EC : ArrayIndexOutOfBoundsException in BlockRecoveryWorker#RecoveryTaskStriped. |  Major | erasure-coding | Surendra Singh Lilhore | Ravuri Sushma sree |
| [HDFS-14442](https://issues.apache.org/jira/browse/HDFS-14442) | Disagreement between HAUtil.getAddressOfActive and RpcInvocationHandler.getConnectionId |  Major | . | Erik Krogen | Ravuri Sushma sree |
| [HDFS-15216](https://issues.apache.org/jira/browse/HDFS-15216) | Wrong Use Case of -showprogress in fsck |  Major | . | Ravuri Sushma sree | Ravuri Sushma sree |
| [HDFS-15211](https://issues.apache.org/jira/browse/HDFS-15211) | EC: File write hangs during close in case of Exception during updatePipeline |  Critical | . | Ayush Saxena | Ayush Saxena |
| [HDFS-15208](https://issues.apache.org/jira/browse/HDFS-15208) | Suppress bogus AbstractWadlGeneratorGrammarGenerator in KMS stderr in hdfs |  Trivial | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-15223](https://issues.apache.org/jira/browse/HDFS-15223) | FSCK fails if one namenode is not available |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-15232](https://issues.apache.org/jira/browse/HDFS-15232) | Fix libhdfspp test failures with GCC 7 |  Major | native, test | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15219](https://issues.apache.org/jira/browse/HDFS-15219) | DFS Client will stuck when ResponseProcessor.run throw Error |  Major | hdfs-client | zhengchenyu | zhengchenyu |
| [HDFS-15191](https://issues.apache.org/jira/browse/HDFS-15191) | EOF when reading legacy buffer in BlockTokenIdentifier |  Major | hdfs | Steven Rand | Steven Rand |
| [YARN-10202](https://issues.apache.org/jira/browse/YARN-10202) | Fix documentation about NodeAttributes. |  Minor | documentation | Sen Zhao | Sen Zhao |
| [HADOOP-16949](https://issues.apache.org/jira/browse/HADOOP-16949) | pylint fails in the build environment |  Critical | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-14836](https://issues.apache.org/jira/browse/HADOOP-14836) | Upgrade maven-clean-plugin to 3.1.0 |  Major | build | Allen Wittenauer | Akira Ajisaka |
| [YARN-10207](https://issues.apache.org/jira/browse/YARN-10207) | CLOSE\_WAIT socket connection leaks during rendering of (corrupted) aggregated logs on the JobHistoryServer Web UI |  Major | yarn | Siddharth Ahuja | Siddharth Ahuja |
| [HDFS-12862](https://issues.apache.org/jira/browse/HDFS-12862) | CacheDirective becomes invalid when NN restart or failover |  Major | caching, hdfs | Wang XL | Wang XL |
| [MAPREDUCE-7272](https://issues.apache.org/jira/browse/MAPREDUCE-7272) | TaskAttemptListenerImpl excessive log messages |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15283](https://issues.apache.org/jira/browse/HDFS-15283) | Cache pool MAXTTL is not persisted and restored on cluster restart |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [HADOOP-16944](https://issues.apache.org/jira/browse/HADOOP-16944) | Use Yetus 0.12.0 in GitHub PR |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15276](https://issues.apache.org/jira/browse/HDFS-15276) | Concat on INodeRefernce fails with illegal state exception |  Critical | . | Hemanth Boyina | Hemanth Boyina |
| [YARN-10223](https://issues.apache.org/jira/browse/YARN-10223) | Duplicate jersey-test-framework-core dependency in yarn-server-common |  Minor | build | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15281](https://issues.apache.org/jira/browse/HDFS-15281) | ZKFC ignores dfs.namenode.rpc-bind-host and uses dfs.namenode.rpc-address to bind to host address |  Major | ha, namenode | Dhiraj Hegde | Dhiraj Hegde |
| [HDFS-15297](https://issues.apache.org/jira/browse/HDFS-15297) | TestNNHandlesBlockReportPerStorage::blockReport\_02 fails intermittently in trunk |  Major | datanode, test | Mingliang Liu | Ayush Saxena |
| [HADOOP-17014](https://issues.apache.org/jira/browse/HADOOP-17014) | Upgrade jackson-databind to 2.9.10.4 |  Blocker | . | Siyao Meng | Siyao Meng |
| [YARN-9848](https://issues.apache.org/jira/browse/YARN-9848) | revert YARN-4946 |  Blocker | log-aggregation, resourcemanager | Steven Rand | Steven Rand |
| [HDFS-15286](https://issues.apache.org/jira/browse/HDFS-15286) | Concat on a same files deleting the file |  Critical | . | Hemanth Boyina | Hemanth Boyina |
| [YARN-10256](https://issues.apache.org/jira/browse/YARN-10256) | Refactor TestContainerSchedulerQueuing.testContainerUpdateExecTypeGuaranteedToOpportunistic |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15270](https://issues.apache.org/jira/browse/HDFS-15270) | Account for \*env == NULL in hdfsThreadDestructor |  Major | . | Babneet Singh | Babneet Singh |
| [YARN-8959](https://issues.apache.org/jira/browse/YARN-8959) | TestContainerResizing fails randomly |  Minor | . | Bibin Chundatt | Ahmed Hussein |
| [HDFS-15323](https://issues.apache.org/jira/browse/HDFS-15323) | StandbyNode fails transition to active due to insufficient transaction tailing |  Major | namenode, qjm | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-17025](https://issues.apache.org/jira/browse/HADOOP-17025) | Fix invalid metastore configuration in S3GuardTool tests |  Minor | fs/s3, test | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-15339](https://issues.apache.org/jira/browse/HDFS-15339) | TestHDFSCLI fails for user names with the dot/dash character |  Major | test | Yan Xiaole | Yan Xiaole |
| [HDFS-15250](https://issues.apache.org/jira/browse/HDFS-15250) | Setting \`dfs.client.use.datanode.hostname\` to true can crash the system because of unhandled UnresolvedAddressException |  Major | . | Ctest | Ctest |
| [HDFS-14367](https://issues.apache.org/jira/browse/HDFS-14367) | EC: Parameter maxPoolSize in striped reconstruct thread pool isn't affecting number of threads |  Major | ec | Guo Lei | Guo Lei |
| [HADOOP-15565](https://issues.apache.org/jira/browse/HADOOP-15565) | ViewFileSystem.close doesn't close child filesystems and causes FileSystem objects leak. |  Major | . | Jinglun | Jinglun |
| [YARN-9444](https://issues.apache.org/jira/browse/YARN-9444) | YARN API ResourceUtils's getRequestedResourcesFromConfig doesn't recognize yarn.io/gpu as a valid resource |  Minor | api | Gergely Pollak | Gergely Pollak |
| [HADOOP-17044](https://issues.apache.org/jira/browse/HADOOP-17044) | Revert "HADOOP-8143. Change distcp to have -pb on by default" |  Major | tools/distcp | Steve Loughran | Steve Loughran |
| [HDFS-15293](https://issues.apache.org/jira/browse/HDFS-15293) | Relax the condition for accepting a fsimage when receiving a checkpoint |  Critical | namenode | Chen Liang | Chen Liang |
| [HADOOP-17024](https://issues.apache.org/jira/browse/HADOOP-17024) | ListStatus on ViewFS root (ls "/") should list the linkFallBack root (configured target root). |  Major | fs, viewfs | Uma Maheswara Rao G | Abhishek Das |
| [HADOOP-17040](https://issues.apache.org/jira/browse/HADOOP-17040) | Fix intermittent failure of ITestBlockingThreadPoolExecutorService |  Minor | fs/s3, test | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-15363](https://issues.apache.org/jira/browse/HDFS-15363) | BlockPlacementPolicyWithNodeGroup should validate if it is initialized by NetworkTopologyWithNodeGroup |  Major | . | Hemanth Boyina | Hemanth Boyina |
| [MAPREDUCE-7278](https://issues.apache.org/jira/browse/MAPREDUCE-7278) | Speculative execution behavior is observed even when mapreduce.map.speculative and mapreduce.reduce.speculative are false |  Major | task | Tarun Parimi | Tarun Parimi |
| [HADOOP-7002](https://issues.apache.org/jira/browse/HADOOP-7002) | Wrong description of copyFromLocal and copyToLocal in documentation |  Minor | . | Jingguo Yao | Andras Bokor |
| [HADOOP-17052](https://issues.apache.org/jira/browse/HADOOP-17052) | NetUtils.connect() throws unchecked exception (UnresolvedAddressException) causing clients to abort |  Major | net | Dhiraj Hegde | Dhiraj Hegde |
| [HADOOP-17062](https://issues.apache.org/jira/browse/HADOOP-17062) | Fix shelldocs path in Jenkinsfile |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17056](https://issues.apache.org/jira/browse/HADOOP-17056) | shelldoc fails in hadoop-common |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [YARN-10286](https://issues.apache.org/jira/browse/YARN-10286) | PendingContainers bugs in the scheduler outputs |  Critical | . | Adam Antal | Andras Gyori |
| [HDFS-15396](https://issues.apache.org/jira/browse/HDFS-15396) | Fix TestViewFileSystemOverloadSchemeHdfsFileSystemContract#testListStatusRootDir |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-15386](https://issues.apache.org/jira/browse/HDFS-15386) | ReplicaNotFoundException keeps happening in DN after removing multiple DN's data directories |  Major | . | Toshihiro Suzuki | Toshihiro Suzuki |
| [YARN-10300](https://issues.apache.org/jira/browse/YARN-10300) | appMasterHost not set in RM ApplicationSummary when AM fails before first heartbeat |  Major | . | Eric Badger | Eric Badger |
| [HADOOP-17059](https://issues.apache.org/jira/browse/HADOOP-17059) | ArrayIndexOfboundsException in ViewFileSystem#listStatus |  Major | viewfs | Hemanth Boyina | Hemanth Boyina |
| [YARN-10296](https://issues.apache.org/jira/browse/YARN-10296) | Make ContainerPBImpl#getId/setId synchronized |  Minor | . | Benjamin Teke | Benjamin Teke |
| [YARN-10295](https://issues.apache.org/jira/browse/YARN-10295) | CapacityScheduler NPE can cause apps to get stuck without resources |  Major | capacityscheduler | Benjamin Teke | Benjamin Teke |
| [HADOOP-17060](https://issues.apache.org/jira/browse/HADOOP-17060) | listStatus and getFileStatus behave inconsistent in the case of ViewFs implementation for isDirectory |  Major | viewfs | Srinivasu Majeti | Uma Maheswara Rao G |
| [YARN-10312](https://issues.apache.org/jira/browse/YARN-10312) | Add support for yarn logs -logFile to retain backward compatibility |  Major | client | Jim Brennan | Jim Brennan |
| [HDFS-15403](https://issues.apache.org/jira/browse/HDFS-15403) | NPE in FileIoProvider#transferToSocketFully |  Major | . | Hemanth Boyina | Hemanth Boyina |
| [HADOOP-17029](https://issues.apache.org/jira/browse/HADOOP-17029) | ViewFS does not return correct user/group and ACL |  Major | fs, viewfs | Abhishek Das | Abhishek Das |
| [HDFS-15421](https://issues.apache.org/jira/browse/HDFS-15421) | IBR leak causes standby NN to be stuck in safe mode |  Blocker | namenode | Kihwal Lee | Akira Ajisaka |
| [YARN-9903](https://issues.apache.org/jira/browse/YARN-9903) | Support reservations continue looking for Node Labels |  Major | . | Tarun Parimi | Jim Brennan |
| [HADOOP-17032](https://issues.apache.org/jira/browse/HADOOP-17032) | Handle an internal dir in viewfs having multiple children mount points pointing to different filesystems |  Major | fs, viewfs | Abhishek Das | Abhishek Das |
| [HDFS-15446](https://issues.apache.org/jira/browse/HDFS-15446) | CreateSnapshotOp fails during edit log loading for /.reserved/raw/path with error java.io.FileNotFoundException: Directory does not exist: /.reserved/raw/path |  Major | hdfs | Srinivasu Majeti | Stephen O'Donnell |
| [HADOOP-17081](https://issues.apache.org/jira/browse/HADOOP-17081) | MetricsSystem doesn't start the sink adapters on restart |  Minor | metrics | Madhusoodan | Madhusoodan |
| [HDFS-15451](https://issues.apache.org/jira/browse/HDFS-15451) | Restarting name node stuck in safe mode when using provided storage |  Major | namenode | shanyu zhao | shanyu zhao |
| [HADOOP-17120](https://issues.apache.org/jira/browse/HADOOP-17120) | Fix failure of docker image creation due to pip2 install error |  Major | . | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-10347](https://issues.apache.org/jira/browse/YARN-10347) | Fix double locking in CapacityScheduler#reinitialize in branch-3.1 |  Critical | capacity scheduler | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-10348](https://issues.apache.org/jira/browse/YARN-10348) | Allow RM to always cancel tokens after app completes |  Major | yarn | Jim Brennan | Jim Brennan |
| [MAPREDUCE-7284](https://issues.apache.org/jira/browse/MAPREDUCE-7284) | TestCombineFileInputFormat#testMissingBlocks fails |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [HDFS-14498](https://issues.apache.org/jira/browse/HDFS-14498) | LeaseManager can loop forever on the file for which create has failed |  Major | namenode | Sergey Shelukhin | Stephen O'Donnell |
| [HADOOP-17130](https://issues.apache.org/jira/browse/HADOOP-17130) | Configuration.getValByRegex() shouldn't update the results while fetching. |  Major | common | Mukund Thakur | Mukund Thakur |
| [HADOOP-17119](https://issues.apache.org/jira/browse/HADOOP-17119) | Jetty upgrade to 9.4.x causes MR app fail with IOException |  Major | . | Bilwa S T | Bilwa S T |
| [YARN-4771](https://issues.apache.org/jira/browse/YARN-4771) | Some containers can be skipped during log aggregation after NM restart |  Major | nodemanager | Jason Darrell Lowe | Jim Brennan |
| [MAPREDUCE-7051](https://issues.apache.org/jira/browse/MAPREDUCE-7051) | Fix typo in MultipleOutputFormat |  Trivial | . | ywheel | ywheel |
| [HDFS-15313](https://issues.apache.org/jira/browse/HDFS-15313) | Ensure inodes in active filesystem are not deleted during snapshot delete |  Major | snapshots | Shashikant Banerjee | Shashikant Banerjee |
| [HDFS-14950](https://issues.apache.org/jira/browse/HDFS-14950) | missing libhdfspp libs in dist-package |  Major | build, libhdfs++ | Yuan Zhou | Yuan Zhou |
| [HADOOP-17184](https://issues.apache.org/jira/browse/HADOOP-17184) | Add --mvn-custom-repos parameter to yetus calls |  Major | build | Mingliang Liu | Mingliang Liu |
| [HDFS-15499](https://issues.apache.org/jira/browse/HDFS-15499) | Clean up httpfs/pom.xml to remove aws-java-sdk-s3 exclusion |  Major | httpfs | Mingliang Liu | Mingliang Liu |
| [HADOOP-17164](https://issues.apache.org/jira/browse/HADOOP-17164) | UGI loginUserFromKeytab doesn't set the last login time |  Major | security | Sandeep Guggilam | Sandeep Guggilam |
| [YARN-4575](https://issues.apache.org/jira/browse/YARN-4575) | ApplicationResourceUsageReport should return ALL  reserved resource |  Major | . | Bibin Chundatt | Bibin Chundatt |
| [HADOOP-17196](https://issues.apache.org/jira/browse/HADOOP-17196) | Fix C/C++ standard warnings |  Major | build | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-17204](https://issues.apache.org/jira/browse/HADOOP-17204) | Fix typo in Hadoop KMS document |  Trivial | documentation, kms | Akira Ajisaka | Xieming Li |
| [HADOOP-17209](https://issues.apache.org/jira/browse/HADOOP-17209) | Erasure Coding: Native library memory leak |  Major | native | Sean Chow | Sean Chow |
| [HADOOP-16925](https://issues.apache.org/jira/browse/HADOOP-16925) | MetricsConfig incorrectly loads the configuration whose value is String list in the properties file |  Major | metrics | Jiayi Liu | Jiayi Liu |
| [HDFS-14852](https://issues.apache.org/jira/browse/HDFS-14852) | Removing from LowRedundancyBlocks does not remove the block from all queues |  Major | namenode | Hui Fei | Hui Fei |
| [HDFS-15290](https://issues.apache.org/jira/browse/HDFS-15290) | NPE in HttpServer during NameNode startup |  Major | namenode | Konstantin Shvachko | Simbarashe Dzinamarira |
| [YARN-10430](https://issues.apache.org/jira/browse/YARN-10430) | Log improvements in NodeStatusUpdaterImpl |  Minor | nodemanager | Bilwa S T | Bilwa S T |
| [MAPREDUCE-7294](https://issues.apache.org/jira/browse/MAPREDUCE-7294) | Only application master should upload resource to Yarn Shared Cache |  Major | mrv2 | zhenzhao wang | zhenzhao wang |
| [MAPREDUCE-7289](https://issues.apache.org/jira/browse/MAPREDUCE-7289) | Fix wrong comment in LongLong.java |  Trivial | documentation, examples | Akira Ajisaka | Wanqiang Ji |
| [YARN-9809](https://issues.apache.org/jira/browse/YARN-9809) | NMs should supply a health status when registering with RM |  Major | . | Eric Badger | Eric Badger |
| [YARN-10393](https://issues.apache.org/jira/browse/YARN-10393) | MR job live lock caused by completed state container leak in heartbeat between node manager and RM |  Major | nodemanager, yarn | zhenzhao wang | Jim Brennan |
| [YARN-10455](https://issues.apache.org/jira/browse/YARN-10455) | TestNMProxy.testNMProxyRPCRetry is not consistent |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17223](https://issues.apache.org/jira/browse/HADOOP-17223) | update  org.apache.httpcomponents:httpclient to 4.5.13 and httpcore to 4.4.13 |  Blocker | . | Pranav Bheda | Pranav Bheda |
| [HDFS-15628](https://issues.apache.org/jira/browse/HDFS-15628) | HttpFS server throws NPE if a file is a symlink |  Major | fs, httpfs | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15627](https://issues.apache.org/jira/browse/HDFS-15627) | Audit log deletes before collecting blocks |  Major | logging, namenode | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17309](https://issues.apache.org/jira/browse/HADOOP-17309) | Javadoc warnings and errors are ignored in the precommit jobs |  Major | build, documentation | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15639](https://issues.apache.org/jira/browse/HDFS-15639) | [JDK 11] Fix Javadoc errors in hadoop-hdfs-client |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-15618](https://issues.apache.org/jira/browse/HDFS-15618) | Improve datanode shutdown latency |  Major | datanode | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15622](https://issues.apache.org/jira/browse/HDFS-15622) | Deleted blocks linger in the replications queue |  Major | hdfs | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15641](https://issues.apache.org/jira/browse/HDFS-15641) | DataNode could meet deadlock if invoke refreshNameNode |  Critical | . | Hongbing Wang | Hongbing Wang |
| [HDFS-15644](https://issues.apache.org/jira/browse/HDFS-15644) | Failed volumes can cause DNs to stop block reporting |  Major | block placement, datanode | Ahmed Hussein | Ahmed Hussein |
| [YARN-10467](https://issues.apache.org/jira/browse/YARN-10467) | ContainerIdPBImpl objects can be leaked in RMNodeImpl.completedContainers |  Major | resourcemanager | Haibo Chen | Haibo Chen |
| [HADOOP-17329](https://issues.apache.org/jira/browse/HADOOP-17329) | mvn site commands fails due to MetricsSystemImpl changes |  Major | . | Xiaoqiao He | Xiaoqiao He |
| [YARN-10472](https://issues.apache.org/jira/browse/YARN-10472) | Backport YARN-10314 to branch-3.2 |  Blocker | yarn | Siyao Meng | Siyao Meng |
| [HADOOP-17340](https://issues.apache.org/jira/browse/HADOOP-17340) | TestLdapGroupsMapping failing -string mismatch in exception validation |  Major | test | Steve Loughran | Steve Loughran |
| [HADOOP-17352](https://issues.apache.org/jira/browse/HADOOP-17352) | Update PATCH\_NAMING\_RULE in the personality file |  Minor | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17096](https://issues.apache.org/jira/browse/HADOOP-17096) | ZStandardCompressor throws java.lang.InternalError: Error (generic) |  Major | io | Stephen Jung (Stripe) | Stephen Jung (Stripe) |
| [HADOOP-17358](https://issues.apache.org/jira/browse/HADOOP-17358) | Improve excessive reloading of Configurations |  Major | conf | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15538](https://issues.apache.org/jira/browse/HDFS-15538) | Fix the documentation for dfs.namenode.replication.max-streams in hdfs-default.xml |  Major | . | Xieming Li | Xieming Li |
| [HADOOP-17362](https://issues.apache.org/jira/browse/HADOOP-17362) | Doing hadoop ls on Har file triggers too many RPC calls |  Major | fs | Ahmed Hussein | Ahmed Hussein |
| [YARN-10485](https://issues.apache.org/jira/browse/YARN-10485) | TimelineConnector swallows InterruptedException |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17360](https://issues.apache.org/jira/browse/HADOOP-17360) | Log the remote address for authentication success |  Minor | ipc | Ahmed Hussein | Ahmed Hussein |
| [YARN-10396](https://issues.apache.org/jira/browse/YARN-10396) | Max applications calculation per queue disregards queue level settings in absolute mode |  Major | capacity scheduler | Benjamin Teke | Benjamin Teke |
| [HADOOP-17346](https://issues.apache.org/jira/browse/HADOOP-17346) | Fair call queue is defeated by abusive service principals |  Major | common, ipc | Ahmed Hussein | Ahmed Hussein |
| [YARN-10470](https://issues.apache.org/jira/browse/YARN-10470) | When building new web ui with root user, the bower install should support it. |  Major | build, yarn-ui-v2 | zhuqi | zhuqi |
| [HADOOP-16080](https://issues.apache.org/jira/browse/HADOOP-16080) | hadoop-aws does not work with hadoop-client-api |  Major | fs/s3 | Keith Turner | Chao Sun |
| [HDFS-15707](https://issues.apache.org/jira/browse/HDFS-15707) | NNTop counts don't add up as expected |  Major | hdfs, metrics, namenode | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15709](https://issues.apache.org/jira/browse/HDFS-15709) | EC: Socket file descriptor leak in StripedBlockChecksumReconstructor |  Major | datanode, ec, erasure-coding | Yushi Hayasaka | Yushi Hayasaka |
| [HDFS-15240](https://issues.apache.org/jira/browse/HDFS-15240) | Erasure Coding: dirty buffer causes reconstruction block error |  Blocker | datanode, erasure-coding | HuangTao | HuangTao |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-10072](https://issues.apache.org/jira/browse/YARN-10072) | TestCSAllocateCustomResource failures |  Major | yarn | Jim Brennan | Jim Brennan |
| [YARN-10161](https://issues.apache.org/jira/browse/YARN-10161) | TestRouterWebServicesREST is corrupting STDOUT |  Minor | yarn | Jim Brennan | Jim Brennan |
| [HADOOP-14206](https://issues.apache.org/jira/browse/HADOOP-14206) | TestSFTPFileSystem#testFileExists failure: Invalid encoding for signature |  Major | fs, test | John Zhuge | Jim Brennan |
| [MAPREDUCE-7288](https://issues.apache.org/jira/browse/MAPREDUCE-7288) | Fix TestLongLong#testRightShift |  Minor | . | Wanqiang Ji | Wanqiang Ji |
| [HDFS-15514](https://issues.apache.org/jira/browse/HDFS-15514) | Remove useless dfs.webhdfs.enabled |  Minor | test | Hui Fei | Hui Fei |
| [HADOOP-17205](https://issues.apache.org/jira/browse/HADOOP-17205) | Move personality file from Yetus to Hadoop repository |  Major | test, yetus | Chao Sun | Chao Sun |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-15775](https://issues.apache.org/jira/browse/HADOOP-15775) | [JDK9] Add missing javax.activation-api dependency |  Critical | test | Akira Ajisaka | Akira Ajisaka |
| [HDFS-14096](https://issues.apache.org/jira/browse/HDFS-14096) | [SPS] : Add Support for Storage Policy Satisfier in ViewFs |  Major | federation | Ayush Saxena | Ayush Saxena |
| [HADOOP-15787](https://issues.apache.org/jira/browse/HADOOP-15787) | [JDK11] TestIPC.testRTEDuringConnectionSetup fails |  Major | . | Akira Ajisaka | Zsolt Venczel |
| [HDFS-14262](https://issues.apache.org/jira/browse/HDFS-14262) | [SBN read] Unclear Log.WARN message in GlobalStateIdContext |  Major | hdfs | Shweta | Shweta |
| [YARN-7243](https://issues.apache.org/jira/browse/YARN-7243) | Moving logging APIs over to slf4j in hadoop-yarn-server-resourcemanager |  Major | . | Yeliang Cang | Prabhu Joseph |
| [HDFS-13404](https://issues.apache.org/jira/browse/HDFS-13404) | RBF: TestRouterWebHDFSContractAppend.testRenameFileBeingAppended fails |  Major | test | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-16117](https://issues.apache.org/jira/browse/HADOOP-16117) | Update AWS SDK to 1.11.563 |  Major | build, fs/s3 | Steve Loughran | Steve Loughran |
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
| [HADOOP-16612](https://issues.apache.org/jira/browse/HADOOP-16612) | Track Azure Blob File System client-perceived latency |  Major | fs/azure, hdfs-client | Jeetesh Mangwani | Jeetesh Mangwani |
| [HADOOP-16758](https://issues.apache.org/jira/browse/HADOOP-16758) | Refine testing.md to tell user better how to use auth-keys.xml |  Minor | fs/s3 | Mingliang Liu | Mingliang Liu |
| [HADOOP-16609](https://issues.apache.org/jira/browse/HADOOP-16609) | Add Jenkinsfile for all active branches |  Major | build | Duo Zhang | Akira Ajisaka |
| [HADOOP-16785](https://issues.apache.org/jira/browse/HADOOP-16785) | Improve wasb and abfs resilience on double close() calls |  Major | fs/azure | Steve Loughran | Steve Loughran |
| [YARN-10026](https://issues.apache.org/jira/browse/YARN-10026) | Pull out common code pieces from ATS v1.5 and v2 |  Major | ATSv2, yarn | Adam Antal | Adam Antal |
| [YARN-10028](https://issues.apache.org/jira/browse/YARN-10028) | Integrate the new abstract log servlet to the JobHistory server |  Major | yarn | Adam Antal | Adam Antal |
| [YARN-10083](https://issues.apache.org/jira/browse/YARN-10083) | Provide utility to ask whether an application is in final status |  Minor | . | Adam Antal | Adam Antal |
| [YARN-10109](https://issues.apache.org/jira/browse/YARN-10109) | Allow stop and convert from leaf to parent queue in a single Mutation API call |  Major | capacity scheduler | Prabhu Joseph | Prabhu Joseph |
| [YARN-10101](https://issues.apache.org/jira/browse/YARN-10101) | Support listing of aggregated logs for containers belonging to an application attempt |  Major | log-aggregation, yarn | Adam Antal | Adam Antal |
| [YARN-10022](https://issues.apache.org/jira/browse/YARN-10022) | Create RM Rest API to validate a CapacityScheduler Configuration |  Major | . | Kinga Marton | Kinga Marton |
| [HDFS-15173](https://issues.apache.org/jira/browse/HDFS-15173) | RBF: Delete repeated configuration 'dfs.federation.router.metrics.enable' |  Minor | documentation, rbf | panlijie | panlijie |
| [YARN-10139](https://issues.apache.org/jira/browse/YARN-10139) | ValidateAndGetSchedulerConfiguration API fails when cluster max allocation \> default 8GB |  Major | . | Prabhu Joseph | Prabhu Joseph |
| [HDFS-14731](https://issues.apache.org/jira/browse/HDFS-14731) | [FGL] Remove redundant locking on NameNode. |  Major | namenode | Konstantin Shvachko | Konstantin Shvachko |
| [YARN-10194](https://issues.apache.org/jira/browse/YARN-10194) | YARN RMWebServices /scheduler-conf/validate leaks ZK Connections |  Blocker | capacityscheduler | Akhil PB | Prabhu Joseph |
| [HDFS-14353](https://issues.apache.org/jira/browse/HDFS-14353) | Erasure Coding: metrics xmitsInProgress become to negative. |  Major | datanode, erasure-coding | Baolong Mao | Baolong Mao |
| [HDFS-15305](https://issues.apache.org/jira/browse/HDFS-15305) | Extend ViewFS and provide ViewFSOverloadScheme implementation with scheme configurable. |  Major | fs, hadoop-client, hdfs-client, viewfs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-15306](https://issues.apache.org/jira/browse/HDFS-15306) | Make mount-table to read from central place ( Let's say from HDFS) |  Major | configuration, hadoop-client | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-16756](https://issues.apache.org/jira/browse/HADOOP-16756) | distcp -update to S3A; abfs, etc always overwrites due to block size mismatch |  Major | fs/s3, tools/distcp | Daisuke Kobayashi | Steve Loughran |
| [HDFS-15322](https://issues.apache.org/jira/browse/HDFS-15322) | Make NflyFS to work when ViewFsOverloadScheme's scheme and target uris schemes are same. |  Major | fs, nflyFs, viewfs, viewfsOverloadScheme | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-15321](https://issues.apache.org/jira/browse/HDFS-15321) | Make DFSAdmin tool to work with ViewFSOverloadScheme |  Major | dfsadmin, fs, viewfs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-15330](https://issues.apache.org/jira/browse/HDFS-15330) | Document the ViewFSOverloadScheme details in ViewFS guide |  Major | viewfs, viewfsOverloadScheme | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-15389](https://issues.apache.org/jira/browse/HDFS-15389) | DFSAdmin should close filesystem and dfsadmin -setBalancerBandwidth should work with ViewFSOverloadScheme |  Major | dfsadmin, viewfsOverloadScheme | Ayush Saxena | Ayush Saxena |
| [HDFS-15394](https://issues.apache.org/jira/browse/HDFS-15394) | Add all available fs.viewfs.overload.scheme.target.\<scheme\>.impl classes in core-default.xml bydefault. |  Major | configuration, viewfs, viewfsOverloadScheme | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-15387](https://issues.apache.org/jira/browse/HDFS-15387) | FSUsage$DF should consider ViewFSOverloadScheme in processPath |  Minor | viewfs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-15418](https://issues.apache.org/jira/browse/HDFS-15418) | ViewFileSystemOverloadScheme should represent mount links as non symlinks |  Major | . | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-15427](https://issues.apache.org/jira/browse/HDFS-15427) | Merged ListStatus with Fallback target filesystem and InternalDirViewFS. |  Major | viewfs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-15429](https://issues.apache.org/jira/browse/HDFS-15429) | mkdirs should work when parent dir is internalDir and fallback configured. |  Major | . | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-15436](https://issues.apache.org/jira/browse/HDFS-15436) | Default mount table name used by ViewFileSystem should be configurable |  Major | viewfs, viewfsOverloadScheme | Virajith Jalaparti | Virajith Jalaparti |
| [HDFS-15450](https://issues.apache.org/jira/browse/HDFS-15450) | Fix NN trash emptier to work if ViewFSOveroadScheme enabled |  Major | namenode, viewfsOverloadScheme | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-15462](https://issues.apache.org/jira/browse/HDFS-15462) | Add fs.viewfs.overload.scheme.target.ofs.impl to core-default.xml |  Major | configuration, viewfs, viewfsOverloadScheme | Siyao Meng | Siyao Meng |
| [HDFS-15464](https://issues.apache.org/jira/browse/HDFS-15464) | ViewFsOverloadScheme should work when -fs option pointing to remote cluster without mount links |  Major | viewfsOverloadScheme | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-17101](https://issues.apache.org/jira/browse/HADOOP-17101) | Replace Guava Function with Java8+ Function |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17099](https://issues.apache.org/jira/browse/HADOOP-17099) | Replace Guava Predicate with Java8+ Predicate |  Minor | . | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15478](https://issues.apache.org/jira/browse/HDFS-15478) | When Empty mount points, we are assigning fallback link to self. But it should not use full URI for target fs. |  Major | . | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-17100](https://issues.apache.org/jira/browse/HADOOP-17100) | Replace Guava Supplier with Java8+ Supplier in Hadoop |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15515](https://issues.apache.org/jira/browse/HDFS-15515) | mkdirs on fallback should throw IOE out instead of suppressing and returning false |  Major | . | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-17199](https://issues.apache.org/jira/browse/HADOOP-17199) | Backport HADOOP-13230 list/getFileStatus changes for preserved directory markers |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-8631](https://issues.apache.org/jira/browse/HDFS-8631) | WebHDFS : Support setQuota |  Major | . | nijel | Chao Sun |
| [YARN-10332](https://issues.apache.org/jira/browse/YARN-10332) | RESOURCE\_UPDATE event was repeatedly registered in DECOMMISSIONING state |  Minor | resourcemanager | yehuanhuan | yehuanhuan |
| [HDFS-15459](https://issues.apache.org/jira/browse/HDFS-15459) | TestBlockTokenWithDFSStriped fails intermittently |  Major | hdfs | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15461](https://issues.apache.org/jira/browse/HDFS-15461) | TestDFSClientRetries#testGetFileChecksum fails intermittently |  Major | dfsclient, test | Ahmed Hussein | Ahmed Hussein |
| [HDFS-9776](https://issues.apache.org/jira/browse/HDFS-9776) | TestHAAppend#testMultipleAppendsDuringCatchupTailing is flaky |  Major | . | Vinayakumar B | Ahmed Hussein |
| [HADOOP-17330](https://issues.apache.org/jira/browse/HADOOP-17330) | Backport HADOOP-16005-"NativeAzureFileSystem does not support setXAttr" to branch-3.2 |  Major | fs/azure | Sally Zuo | Sally Zuo |
| [HDFS-15643](https://issues.apache.org/jira/browse/HDFS-15643) | EC: Fix checksum computation in case of native encoders |  Blocker | . | Ahmed Hussein | Ayush Saxena |
| [HADOOP-17343](https://issues.apache.org/jira/browse/HADOOP-17343) | Upgrade aws-java-sdk to 1.11.901 |  Minor | build, fs/s3 | Dongjoon Hyun | Steve Loughran |
| [HADOOP-17325](https://issues.apache.org/jira/browse/HADOOP-17325) | WASB: Test failures |  Major | fs/azure, test | Sneha Vijayarajan | Steve Loughran |
| [HDFS-15708](https://issues.apache.org/jira/browse/HDFS-15708) | TestURLConnectionFactory fails by NoClassDefFoundError in branch-3.3 and branch-3.2 |  Blocker | test | Akira Ajisaka | Chao Sun |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-14394](https://issues.apache.org/jira/browse/HDFS-14394) | Add -std=c99 / -std=gnu99 to libhdfs compile flags |  Major | hdfs-client, libhdfs, native | Sahil Takiar | Sahil Takiar |
| [HADOOP-16365](https://issues.apache.org/jira/browse/HADOOP-16365) | Upgrade jackson-databind to 2.9.9 |  Major | build | Shweta | Shweta |
| [HADOOP-16491](https://issues.apache.org/jira/browse/HADOOP-16491) | Upgrade jetty version to 9.3.27 |  Major | . | Hrishikesh Gadre | Hrishikesh Gadre |
| [HADOOP-16542](https://issues.apache.org/jira/browse/HADOOP-16542) | Update commons-beanutils version to 1.9.4 |  Major | . | Wei-Chiu Chuang | Kevin Su |
| [YARN-9730](https://issues.apache.org/jira/browse/YARN-9730) | Support forcing configured partitions to be exclusive based on app node label |  Major | . | Jonathan Hung | Jonathan Hung |
| [HADOOP-16675](https://issues.apache.org/jira/browse/HADOOP-16675) | Upgrade jackson-databind to 2.9.10.1 |  Blocker | security | Wei-Chiu Chuang | Lisheng Sun |
| [HDFS-14959](https://issues.apache.org/jira/browse/HDFS-14959) | [SBNN read] access time should be turned off |  Major | documentation | Wei-Chiu Chuang | Chao Sun |
| [HADOOP-16784](https://issues.apache.org/jira/browse/HADOOP-16784) | Update the year to 2020 |  Major | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-16803](https://issues.apache.org/jira/browse/HADOOP-16803) | Upgrade jackson-databind to 2.9.10.2 |  Blocker | security | Akira Ajisaka | Masatake Iwasaki |
| [HADOOP-16871](https://issues.apache.org/jira/browse/HADOOP-16871) | Upgrade Netty version to 4.1.45.Final to handle CVE-2019-20444,CVE-2019-16869 |  Major | . | Aray Chenchu Sukesh | Aray Chenchu Sukesh |
| [HADOOP-16647](https://issues.apache.org/jira/browse/HADOOP-16647) | Support OpenSSL 1.1.1 LTS |  Critical | security | Wei-Chiu Chuang | Rakesh Radhakrishnan |
| [HADOOP-16982](https://issues.apache.org/jira/browse/HADOOP-16982) | Update Netty to 4.1.48.Final |  Blocker | . | Wei-Chiu Chuang | Lisheng Sun |
| [HADOOP-16990](https://issues.apache.org/jira/browse/HADOOP-16990) | Update Mockserver |  Major | . | Wei-Chiu Chuang | Attila Doroszlai |
| [YARN-10540](https://issues.apache.org/jira/browse/YARN-10540) | Node page is broken in YARN UI1 and UI2 including RMWebService api for nodes |  Critical | webapp | Sunil G | Jim Brennan |
| [HADOOP-17445](https://issues.apache.org/jira/browse/HADOOP-17445) | Update the year to 2021 |  Major | . | Xiaoqiao He | Xiaoqiao He |


