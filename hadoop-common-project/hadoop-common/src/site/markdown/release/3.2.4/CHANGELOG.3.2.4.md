
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

## Release 3.2.4 - 2022-07-12



### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-16337](https://issues.apache.org/jira/browse/HDFS-16337) | Show start time of Datanode on Web |  Minor | . | Tao Li | Tao Li |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-15075](https://issues.apache.org/jira/browse/HDFS-15075) | Remove process command timing from BPServiceActor |  Major | . | Íñigo Goiri | Xiaoqiao He |
| [HDFS-15150](https://issues.apache.org/jira/browse/HDFS-15150) | Introduce read write lock to Datanode |  Major | datanode | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-16175](https://issues.apache.org/jira/browse/HDFS-16175) | Improve the configurable value of Server #PURGE\_INTERVAL\_NANOS |  Major | ipc | JiangHua Zhu | JiangHua Zhu |
| [HDFS-16173](https://issues.apache.org/jira/browse/HDFS-16173) | Improve CopyCommands#Put#executor queue configurability |  Major | fs | JiangHua Zhu | JiangHua Zhu |
| [HADOOP-17897](https://issues.apache.org/jira/browse/HADOOP-17897) | Allow nested blocks in switch case in checkstyle settings |  Minor | build | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-17857](https://issues.apache.org/jira/browse/HADOOP-17857) | Check real user ACLs in addition to proxied user ACLs |  Major | . | Eric Payne | Eric Payne |
| [HDFS-14997](https://issues.apache.org/jira/browse/HDFS-14997) | BPServiceActor processes commands from NameNode asynchronously |  Major | datanode | Xiaoqiao He | Xiaoqiao He |
| [HADOOP-17926](https://issues.apache.org/jira/browse/HADOOP-17926) | Maven-eclipse-plugin is no longer needed since Eclipse can import Maven projects by itself. |  Minor | documentation | Rintaro Ikeda | Rintaro Ikeda |
| [YARN-10935](https://issues.apache.org/jira/browse/YARN-10935) | AM Total Queue Limit goes below per-user AM Limit if parent is full. |  Major | capacity scheduler, capacityscheduler | Eric Payne | Eric Payne |
| [HDFS-16241](https://issues.apache.org/jira/browse/HDFS-16241) | Standby close reconstruction thread |  Major | . | zhanghuazong | zhanghuazong |
| [YARN-1115](https://issues.apache.org/jira/browse/YARN-1115) | Provide optional means for a scheduler to check real user ACLs |  Major | capacity scheduler, scheduler | Eric Payne |  |
| [HDFS-16279](https://issues.apache.org/jira/browse/HDFS-16279) | Print detail datanode info when process first storage report |  Minor | . | Tao Li | Tao Li |
| [HDFS-16294](https://issues.apache.org/jira/browse/HDFS-16294) | Remove invalid DataNode#CONFIG\_PROPERTY\_SIMULATED |  Major | datanode | JiangHua Zhu | JiangHua Zhu |
| [HDFS-16299](https://issues.apache.org/jira/browse/HDFS-16299) | Fix bug for TestDataNodeVolumeMetrics#verifyDataNodeVolumeMetrics |  Minor | . | Tao Li | Tao Li |
| [HDFS-16301](https://issues.apache.org/jira/browse/HDFS-16301) | Improve BenchmarkThroughput#SIZE naming standardization |  Minor | benchmarks, test | JiangHua Zhu | JiangHua Zhu |
| [YARN-10997](https://issues.apache.org/jira/browse/YARN-10997) | Revisit allocation and reservation logging |  Major | . | Andras Gyori | Andras Gyori |
| [HDFS-16315](https://issues.apache.org/jira/browse/HDFS-16315) | Add metrics related to Transfer and NativeCopy for DataNode |  Major | . | Tao Li | Tao Li |
| [HADOOP-17998](https://issues.apache.org/jira/browse/HADOOP-17998) | Allow get command to run with multi threads. |  Major | fs | Chengwei Wang | Chengwei Wang |
| [HDFS-16345](https://issues.apache.org/jira/browse/HDFS-16345) | Fix test cases fail in TestBlockStoragePolicy |  Major | build | guophilipse | guophilipse |
| [HADOOP-18035](https://issues.apache.org/jira/browse/HADOOP-18035) | Skip unit test failures to run all the unit tests |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-18040](https://issues.apache.org/jira/browse/HADOOP-18040) | Use maven.test.failure.ignore instead of ignoreTestFailure |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HDFS-16352](https://issues.apache.org/jira/browse/HDFS-16352) | return the real datanode numBlocks in #getDatanodeStorageReport |  Major | . | qinyuren | qinyuren |
| [HDFS-16386](https://issues.apache.org/jira/browse/HDFS-16386) | Reduce DataNode load when FsDatasetAsyncDiskService is working |  Major | datanode | JiangHua Zhu | JiangHua Zhu |
| [HDFS-16391](https://issues.apache.org/jira/browse/HDFS-16391) | Avoid evaluation of LOG.debug statement in NameNodeHeartbeatService |  Trivial | . | wangzhaohui | wangzhaohui |
| [YARN-8234](https://issues.apache.org/jira/browse/YARN-8234) | Improve RM system metrics publisher's performance by pushing events to timeline server in batch |  Critical | resourcemanager, timelineserver | Hu Ziqian | Ashutosh Gupta |
| [HDFS-16430](https://issues.apache.org/jira/browse/HDFS-16430) | Validate maximum blocks in EC group when adding an EC policy |  Minor | ec, erasure-coding | daimin | daimin |
| [HDFS-16403](https://issues.apache.org/jira/browse/HDFS-16403) | Improve FUSE IO performance by supporting FUSE parameter max\_background |  Minor | fuse-dfs | daimin | daimin |
| [HADOOP-18136](https://issues.apache.org/jira/browse/HADOOP-18136) | Verify FileUtils.unTar() handling of missing .tar files |  Minor | test, util | Steve Loughran | Steve Loughran |
| [HDFS-16529](https://issues.apache.org/jira/browse/HDFS-16529) | Remove unnecessary setObserverRead in TestConsistentReadsObserver |  Trivial | test | wangzhaohui | wangzhaohui |
| [HDFS-16530](https://issues.apache.org/jira/browse/HDFS-16530) | setReplication debug log creates a new string even if debug is disabled |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-16427](https://issues.apache.org/jira/browse/HDFS-16427) | Add debug log for BlockManager#chooseExcessRedundancyStriped |  Minor | erasure-coding | Tao Li | Tao Li |
| [HDFS-16389](https://issues.apache.org/jira/browse/HDFS-16389) | Improve NNThroughputBenchmark test mkdirs |  Major | benchmarks, namenode | JiangHua Zhu | JiangHua Zhu |
| [MAPREDUCE-7373](https://issues.apache.org/jira/browse/MAPREDUCE-7373) | Building MapReduce NativeTask fails on Fedora 34+ |  Major | build, nativetask | Kengo Seki | Kengo Seki |
| [HDFS-16355](https://issues.apache.org/jira/browse/HDFS-16355) | Improve the description of dfs.block.scanner.volume.bytes.per.second |  Minor | documentation, hdfs | guophilipse | guophilipse |
| [HADOOP-18088](https://issues.apache.org/jira/browse/HADOOP-18088) | Replace log4j 1.x with reload4j |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-16501](https://issues.apache.org/jira/browse/HDFS-16501) | Print the exception when reporting a bad block |  Major | datanode | qinyuren | qinyuren |
| [YARN-11116](https://issues.apache.org/jira/browse/YARN-11116) | Migrate Times util from SimpleDateFormat to thread-safe DateTimeFormatter class |  Minor | . | Jonathan Turner Eagles | Jonathan Turner Eagles |
| [YARN-10080](https://issues.apache.org/jira/browse/YARN-10080) | Support show app id on localizer thread pool |  Major | nodemanager | zhoukang | Ashutosh Gupta |
| [HADOOP-18240](https://issues.apache.org/jira/browse/HADOOP-18240) | Upgrade Yetus to 0.14.0 |  Major | build | Akira Ajisaka | Ashutosh Gupta |
| [HDFS-16585](https://issues.apache.org/jira/browse/HDFS-16585) | Add @VisibleForTesting in Dispatcher.java after HDFS-16268 |  Trivial | . | Wei-Chiu Chuang | Ashutosh Gupta |
| [HDFS-16610](https://issues.apache.org/jira/browse/HDFS-16610) | Make fsck read timeout configurable |  Major | hdfs-client | Stephen O'Donnell | Stephen O'Donnell |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-13983](https://issues.apache.org/jira/browse/HDFS-13983) | TestOfflineImageViewer crashes in windows |  Major | . | Vinayakumar B | Vinayakumar B |
| [YARN-9744](https://issues.apache.org/jira/browse/YARN-9744) | RollingLevelDBTimelineStore.getEntityByTime fails with NPE |  Major | timelineserver | Prabhu Joseph | Prabhu Joseph |
| [HDFS-15113](https://issues.apache.org/jira/browse/HDFS-15113) | Missing IBR when NameNode restart if open processCommand async feature |  Blocker | datanode | Xiaoqiao He | Xiaoqiao He |
| [HADOOP-16985](https://issues.apache.org/jira/browse/HADOOP-16985) | Handle release package related issues |  Major | . | Vinayakumar B | Vinayakumar B |
| [HADOOP-17116](https://issues.apache.org/jira/browse/HADOOP-17116) | Skip Retry INFO logging on first failover from a proxy |  Major | ha | Hanisha Koneru | Hanisha Koneru |
| [HDFS-15651](https://issues.apache.org/jira/browse/HDFS-15651) | Client could not obtain block when DN CommandProcessingThread exit |  Major | . | Yiqun Lin | Mingxiang Li |
| [HDFS-15963](https://issues.apache.org/jira/browse/HDFS-15963) | Unreleased volume references cause an infinite loop |  Critical | datanode | Shuyan Zhang | Shuyan Zhang |
| [HDFS-14575](https://issues.apache.org/jira/browse/HDFS-14575) | LeaseRenewer#daemon threads leak in DFSClient |  Major | . | Tao Yang | Renukaprasad C |
| [HADOOP-17796](https://issues.apache.org/jira/browse/HADOOP-17796) | Upgrade jetty version to 9.4.43 |  Major | . | Wei-Chiu Chuang | Renukaprasad C |
| [HDFS-15175](https://issues.apache.org/jira/browse/HDFS-15175) | Multiple CloseOp shared block instance causes the standby namenode to crash when rolling editlog |  Critical | . | Yicong Cai | Wan Chang |
| [HDFS-16177](https://issues.apache.org/jira/browse/HDFS-16177) | Bug fix for Util#receiveFile |  Minor | . | Tao Li | Tao Li |
| [YARN-10814](https://issues.apache.org/jira/browse/YARN-10814) | YARN shouldn't start with empty hadoop.http.authentication.signature.secret.file |  Major | . | Benjamin Teke | Tamas Domok |
| [HADOOP-17874](https://issues.apache.org/jira/browse/HADOOP-17874) | ExceptionsHandler to add terse/suppressed Exceptions in thread-safe manner |  Major | . | Viraj Jasani | Viraj Jasani |
| [HADOOP-15129](https://issues.apache.org/jira/browse/HADOOP-15129) | Datanode caches namenode DNS lookup failure and cannot startup |  Minor | ipc | Karthik Palaniappan | Chris Nauroth |
| [YARN-10901](https://issues.apache.org/jira/browse/YARN-10901) | Permission checking error on an existing directory in LogAggregationFileController#verifyAndCreateRemoteLogDir |  Major | nodemanager | Tamas Domok | Tamas Domok |
| [HDFS-16207](https://issues.apache.org/jira/browse/HDFS-16207) | Remove NN logs stack trace for non-existent xattr query |  Major | namenode | Ahmed Hussein | Ahmed Hussein |
| [HDFS-16187](https://issues.apache.org/jira/browse/HDFS-16187) | SnapshotDiff behaviour with Xattrs and Acls is not consistent across NN restarts with checkpointing |  Major | snapshots | Srinivasu Majeti | Shashikant Banerjee |
| [HDFS-16198](https://issues.apache.org/jira/browse/HDFS-16198) | Short circuit read leaks Slot objects when InvalidToken exception is thrown |  Major | . | Eungsop Yoo | Eungsop Yoo |
| [YARN-10870](https://issues.apache.org/jira/browse/YARN-10870) | Missing user filtering check -\> yarn.webapp.filter-entity-list-by-user for RM Scheduler page |  Major | yarn | Siddharth Ahuja | Gergely Pollák |
| [HADOOP-17919](https://issues.apache.org/jira/browse/HADOOP-17919) | Fix command line example in Hadoop Cluster Setup documentation |  Minor | documentation | Rintaro Ikeda | Rintaro Ikeda |
| [HDFS-16235](https://issues.apache.org/jira/browse/HDFS-16235) | Deadlock in LeaseRenewer for static remove method |  Major | hdfs | angerszhu | angerszhu |
| [HDFS-16181](https://issues.apache.org/jira/browse/HDFS-16181) | [SBN Read] Fix metric of RpcRequestCacheMissAmount can't display when tailEditLog form JN |  Critical | . | wangzhaohui | wangzhaohui |
| [HADOOP-17925](https://issues.apache.org/jira/browse/HADOOP-17925) | BUILDING.txt should not encourage to activate docs profile on building binary artifacts |  Minor | documentation | Rintaro Ikeda | Masatake Iwasaki |
| [HADOOP-16532](https://issues.apache.org/jira/browse/HADOOP-16532) | Fix TestViewFsTrash to use the correct homeDir. |  Minor | test, viewfs | Steve Loughran | Xing Lin |
| [HDFS-16268](https://issues.apache.org/jira/browse/HDFS-16268) | Balancer stuck when moving striped blocks due to NPE |  Major | balancer & mover, erasure-coding | Leon Gao | Leon Gao |
| [HDFS-7612](https://issues.apache.org/jira/browse/HDFS-7612) | TestOfflineEditsViewer.testStored() uses incorrect default value for cacheDir |  Major | test | Konstantin Shvachko | Michael Kuchenbecker |
| [HDFS-16311](https://issues.apache.org/jira/browse/HDFS-16311) | Metric metadataOperationRate calculation error in DataNodeVolumeMetrics |  Major | . | Tao Li | Tao Li |
| [HDFS-16182](https://issues.apache.org/jira/browse/HDFS-16182) | numOfReplicas is given the wrong value in  BlockPlacementPolicyDefault$chooseTarget can cause DataStreamer to fail with Heterogeneous Storage |  Major | namanode | Max  Xie | Max  Xie |
| [HADOOP-17999](https://issues.apache.org/jira/browse/HADOOP-17999) | No-op implementation of setWriteChecksum and setVerifyChecksum in ViewFileSystem |  Major | . | Abhishek Das | Abhishek Das |
| [HDFS-16329](https://issues.apache.org/jira/browse/HDFS-16329) | Fix log format for BlockManager |  Minor | . | Tao Li | Tao Li |
| [HDFS-16330](https://issues.apache.org/jira/browse/HDFS-16330) | Fix incorrect placeholder for Exception logs in DiskBalancer |  Major | . | Viraj Jasani | Viraj Jasani |
| [HDFS-16328](https://issues.apache.org/jira/browse/HDFS-16328) | Correct disk balancer param desc |  Minor | documentation, hdfs | guophilipse | guophilipse |
| [HDFS-16343](https://issues.apache.org/jira/browse/HDFS-16343) | Add some debug logs when the dfsUsed are not used during Datanode startup |  Major | datanode | Mukul Kumar Singh | Mukul Kumar Singh |
| [YARN-10991](https://issues.apache.org/jira/browse/YARN-10991) | Fix to ignore the grouping "[]" for resourcesStr in parseResourcesString method |  Minor | distributed-shell | Ashutosh Gupta | Ashutosh Gupta |
| [HADOOP-17975](https://issues.apache.org/jira/browse/HADOOP-17975) | Fallback to simple auth does not work for a secondary DistributedFileSystem instance |  Major | ipc | István Fajth | István Fajth |
| [HDFS-16350](https://issues.apache.org/jira/browse/HDFS-16350) | Datanode start time should be set after RPC server starts successfully |  Minor | . | Viraj Jasani | Viraj Jasani |
| [YARN-11007](https://issues.apache.org/jira/browse/YARN-11007) | Correct words in YARN documents |  Minor | documentation | guophilipse | guophilipse |
| [HDFS-16332](https://issues.apache.org/jira/browse/HDFS-16332) | Expired block token causes slow read due to missing handling in sasl handshake |  Major | datanode, dfs, dfsclient | Shinya Yoshida | Shinya Yoshida |
| [YARN-9063](https://issues.apache.org/jira/browse/YARN-9063) | ATS 1.5 fails to start if RollingLevelDb files are corrupt or missing |  Major | timelineserver, timelineservice | Tarun Parimi | Ashutosh Gupta |
| [HDFS-16333](https://issues.apache.org/jira/browse/HDFS-16333) | fix balancer bug when transfer an EC block |  Major | balancer & mover, erasure-coding | qinyuren | qinyuren |
| [HDFS-16373](https://issues.apache.org/jira/browse/HDFS-16373) | Fix MiniDFSCluster restart in case of multiple namenodes |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-16377](https://issues.apache.org/jira/browse/HDFS-16377) | Should CheckNotNull before access FsDatasetSpi |  Major | . | Tao Li | Tao Li |
| [YARN-6862](https://issues.apache.org/jira/browse/YARN-6862) | Nodemanager resource usage metrics sometimes are negative |  Major | nodemanager | YunFan Zhou | Benjamin Teke |
| [YARN-10178](https://issues.apache.org/jira/browse/YARN-10178) | Global Scheduler async thread crash caused by 'Comparison method violates its general contract |  Major | capacity scheduler | tuyu | Andras Gyori |
| [HDFS-16395](https://issues.apache.org/jira/browse/HDFS-16395) | Remove useless NNThroughputBenchmark#dummyActionNoSynch() |  Major | benchmarks, namenode | JiangHua Zhu | JiangHua Zhu |
| [HADOOP-18063](https://issues.apache.org/jira/browse/HADOOP-18063) | Remove unused import AbstractJavaKeyStoreProvider in Shell class |  Minor | . | JiangHua Zhu | JiangHua Zhu |
| [HDFS-16409](https://issues.apache.org/jira/browse/HDFS-16409) | Fix typo: testHasExeceptionsReturnsCorrectValue -\> testHasExceptionsReturnsCorrectValue |  Trivial | . | Ashutosh Gupta | Ashutosh Gupta |
| [HDFS-16408](https://issues.apache.org/jira/browse/HDFS-16408) | Ensure LeaseRecheckIntervalMs is greater than zero |  Major | namenode | Jingxuan Fu | Jingxuan Fu |
| [YARN-11055](https://issues.apache.org/jira/browse/YARN-11055) | In cgroups-operations.c some fprintf format strings don't end with "\\n" |  Minor | nodemanager | Gera Shegalov | Gera Shegalov |
| [HDFS-16303](https://issues.apache.org/jira/browse/HDFS-16303) | Losing over 100 datanodes in state decommissioning results in full blockage of all datanode decommissioning |  Major | . | Kevin Wikant | Kevin Wikant |
| [HDFS-16443](https://issues.apache.org/jira/browse/HDFS-16443) | Fix edge case where DatanodeAdminDefaultMonitor doubly enqueues a DatanodeDescriptor on exception |  Major | hdfs | Kevin Wikant | Kevin Wikant |
| [HDFS-16449](https://issues.apache.org/jira/browse/HDFS-16449) | Fix hadoop web site release notes and changelog not available |  Minor | documentation | guophilipse | guophilipse |
| [HADOOP-18192](https://issues.apache.org/jira/browse/HADOOP-18192) | Fix multiple\_bindings warning about slf4j-reload4j |  Major | . | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-16479](https://issues.apache.org/jira/browse/HDFS-16479) | EC: NameNode should not send a reconstruction work when the source datanodes are insufficient |  Critical | ec, erasure-coding | Yuanbo Liu | Takanobu Asanuma |
| [HDFS-16509](https://issues.apache.org/jira/browse/HDFS-16509) | Fix decommission UnsupportedOperationException: Remove unsupported |  Major | namenode | daimin | daimin |
| [HDFS-16456](https://issues.apache.org/jira/browse/HDFS-16456) | EC: Decommission a rack with only on dn will fail when the rack number is equal with replication |  Critical | ec, namenode | caozhiqiang | caozhiqiang |
| [HDFS-16437](https://issues.apache.org/jira/browse/HDFS-16437) | ReverseXML processor doesn't accept XML files without the SnapshotDiffSection. |  Critical | hdfs | yanbin.zhang | yanbin.zhang |
| [HDFS-16507](https://issues.apache.org/jira/browse/HDFS-16507) | [SBN read] Avoid purging edit log which is in progress |  Critical | . | Tao Li | Tao Li |
| [YARN-10720](https://issues.apache.org/jira/browse/YARN-10720) | YARN WebAppProxyServlet should support connection timeout to prevent proxy server from hanging |  Critical | . | Qi Zhu | Qi Zhu |
| [HDFS-16428](https://issues.apache.org/jira/browse/HDFS-16428) | Source path with storagePolicy cause wrong typeConsumed while rename |  Major | hdfs, namenode | lei w | lei w |
| [YARN-11014](https://issues.apache.org/jira/browse/YARN-11014) | YARN incorrectly validates maximum capacity resources on the validation API |  Major | . | Benjamin Teke | Benjamin Teke |
| [YARN-11075](https://issues.apache.org/jira/browse/YARN-11075) | Explicitly declare serialVersionUID in LogMutation class |  Major | . | Benjamin Teke | Benjamin Teke |
| [HDFS-11041](https://issues.apache.org/jira/browse/HDFS-11041) | Unable to unregister FsDatasetState MBean if DataNode is shutdown twice |  Trivial | datanode | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-16538](https://issues.apache.org/jira/browse/HDFS-16538) |  EC decoding failed due to not enough valid inputs |  Major | erasure-coding | qinyuren | qinyuren |
| [HDFS-16544](https://issues.apache.org/jira/browse/HDFS-16544) | EC decoding failed due to invalid buffer |  Major | erasure-coding | qinyuren | qinyuren |
| [HDFS-16546](https://issues.apache.org/jira/browse/HDFS-16546) | Fix UT TestOfflineImageViewer#testReverseXmlWithoutSnapshotDiffSection to branch branch-3.2 |  Major | test | daimin | daimin |
| [HDFS-16552](https://issues.apache.org/jira/browse/HDFS-16552) | Fix NPE for TestBlockManager |  Major | . | Tao Li | Tao Li |
| [MAPREDUCE-7246](https://issues.apache.org/jira/browse/MAPREDUCE-7246) |  In MapredAppMasterRest#Mapreduce\_Application\_Master\_Info\_API, the datatype of appId should be "string". |  Major | documentation | jenny | Ashutosh Gupta |
| [YARN-10187](https://issues.apache.org/jira/browse/YARN-10187) | Removing hadoop-yarn-project/hadoop-yarn/README as it is no longer maintained. |  Minor | documentation | N Sanketh Reddy | Ashutosh Gupta |
| [HDFS-16185](https://issues.apache.org/jira/browse/HDFS-16185) | Fix comment in LowRedundancyBlocks.java |  Minor | documentation | Akira Ajisaka | Ashutosh Gupta |
| [HADOOP-17479](https://issues.apache.org/jira/browse/HADOOP-17479) | Fix the examples of hadoop config prefix |  Minor | documentation | Akira Ajisaka | Ashutosh Gupta |
| [HDFS-16579](https://issues.apache.org/jira/browse/HDFS-16579) | Fix build failure for TestBlockManager on branch-3.2 |  Major | . | Tao Li | Tao Li |
| [YARN-11092](https://issues.apache.org/jira/browse/YARN-11092) | Upgrade jquery ui to 1.13.1 |  Major | . | D M Murali Krishna Reddy | Ashutosh Gupta |
| [YARN-11133](https://issues.apache.org/jira/browse/YARN-11133) | YarnClient gets the wrong EffectiveMinCapacity value |  Major | api | Zilong Zhu | Zilong Zhu |
| [YARN-10850](https://issues.apache.org/jira/browse/YARN-10850) | TimelineService v2 lists containers for all attempts when filtering for one |  Major | timelinereader | Benjamin Teke | Benjamin Teke |
| [YARN-11126](https://issues.apache.org/jira/browse/YARN-11126) | ZKConfigurationStore Java deserialisation vulnerability |  Major | yarn | Tamas Domok | Tamas Domok |
| [YARN-11162](https://issues.apache.org/jira/browse/YARN-11162) | Set the zk acl for nodes created by ZKConfigurationStore. |  Major | resourcemanager | Owen O'Malley | Owen O'Malley |
| [HDFS-16586](https://issues.apache.org/jira/browse/HDFS-16586) | Purge FsDatasetAsyncDiskService threadgroup; it causes BPServiceActor$CommandProcessingThread IllegalThreadStateException 'fatal exception and exit' |  Major | datanode | Michael Stack | Michael Stack |
| [HADOOP-18251](https://issues.apache.org/jira/browse/HADOOP-18251) | Fix failure of extracting JIRA id from commit message in git\_jira\_fix\_version\_check.py |  Minor | build | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-16583](https://issues.apache.org/jira/browse/HDFS-16583) | DatanodeAdminDefaultMonitor can get stuck in an infinite loop |  Major | . | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-16623](https://issues.apache.org/jira/browse/HDFS-16623) | IllegalArgumentException in LifelineSender |  Major | . | ZanderXu | ZanderXu |
| [HDFS-16064](https://issues.apache.org/jira/browse/HDFS-16064) | Determine when to invalidate corrupt replicas based on number of usable replicas |  Major | datanode, namenode | Kevin Wikant | Kevin Wikant |
| [HADOOP-18100](https://issues.apache.org/jira/browse/HADOOP-18100) | Change scope of inner classes in InodeTree to make them accessible outside package |  Major | . | Abhishek Das | Abhishek Das |
| [HADOOP-18334](https://issues.apache.org/jira/browse/HADOOP-18334) | Fix create-release to address removal of GPG\_AGENT\_INFO in branch-3.2 |  Major | build | Masatake Iwasaki | Masatake Iwasaki |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-7342](https://issues.apache.org/jira/browse/MAPREDUCE-7342) | Stop RMService in TestClientRedirect.testRedirect() |  Minor | . | Zhengxi Li | Zhengxi Li |
| [MAPREDUCE-7311](https://issues.apache.org/jira/browse/MAPREDUCE-7311) | Fix non-idempotent test in TestTaskProgressReporter |  Minor | . | Zhengxi Li | Zhengxi Li |
| [HDFS-15862](https://issues.apache.org/jira/browse/HDFS-15862) | Make TestViewfsWithNfs3.testNfsRenameSingleNN() idempotent |  Minor | nfs | Zhengxi Li | Zhengxi Li |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-15457](https://issues.apache.org/jira/browse/HDFS-15457) | TestFsDatasetImpl fails intermittently |  Major | hdfs | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15818](https://issues.apache.org/jira/browse/HDFS-15818) | Fix TestFsDatasetImpl.testReadLockCanBeDisabledByConfig |  Minor | test | Leon Gao | Leon Gao |
| [YARN-10503](https://issues.apache.org/jira/browse/YARN-10503) | Support queue capacity in terms of absolute resources with custom resourceType. |  Critical | . | Qi Zhu | Qi Zhu |
| [HADOOP-17126](https://issues.apache.org/jira/browse/HADOOP-17126) | implement non-guava Precondition checkNotNull |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17929](https://issues.apache.org/jira/browse/HADOOP-17929) | implement non-guava Precondition checkArgument |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17947](https://issues.apache.org/jira/browse/HADOOP-17947) | Provide alternative to Guava VisibleForTesting |  Major | . | Viraj Jasani | Viraj Jasani |
| [HADOOP-17930](https://issues.apache.org/jira/browse/HADOOP-17930) | implement non-guava Precondition checkState |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17374](https://issues.apache.org/jira/browse/HADOOP-17374) | AliyunOSS: support ListObjectsV2 |  Major | fs/oss | wujinhu | wujinhu |
| [HDFS-16336](https://issues.apache.org/jira/browse/HDFS-16336) | De-flake TestRollingUpgrade#testRollback |  Minor | hdfs, test | Kevin Wikant | Viraj Jasani |
| [HDFS-16171](https://issues.apache.org/jira/browse/HDFS-16171) | De-flake testDecommissionStatus |  Major | . | Viraj Jasani | Viraj Jasani |
| [HDFS-16169](https://issues.apache.org/jira/browse/HDFS-16169) | Fix TestBlockTokenWithDFSStriped#testEnd2End failure |  Major | test | Hui Fei | secfree |
| [HDFS-16484](https://issues.apache.org/jira/browse/HDFS-16484) | [SPS]: Fix an infinite loop bug in SPSPathIdProcessor thread |  Major | . | qinyuren | qinyuren |
| [HADOOP-16663](https://issues.apache.org/jira/browse/HADOOP-16663) | Backport "HADOOP-16560 [YARN] use protobuf-maven-plugin to generate protobuf classes" to all active branches |  Major | . | Duo Zhang | Duo Zhang |
| [HADOOP-16664](https://issues.apache.org/jira/browse/HADOOP-16664) | Backport "HADOOP-16561 [MAPREDUCE] use protobuf-maven-plugin to generate protobuf classes" to all active branches |  Major | . | Duo Zhang | Duo Zhang |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-16298](https://issues.apache.org/jira/browse/HDFS-16298) | Improve error msg for BlockMissingException |  Minor | . | Tao Li | Tao Li |
| [HDFS-16312](https://issues.apache.org/jira/browse/HDFS-16312) | Fix typo for DataNodeVolumeMetrics and ProfilingFileIoEvents |  Minor | . | Tao Li | Tao Li |
| [HDFS-16326](https://issues.apache.org/jira/browse/HDFS-16326) | Simplify the code for DiskBalancer |  Minor | . | Tao Li | Tao Li |
| [HDFS-16339](https://issues.apache.org/jira/browse/HDFS-16339) | Show the threshold when mover threads quota is exceeded |  Minor | . | Tao Li | Tao Li |
| [YARN-10820](https://issues.apache.org/jira/browse/YARN-10820) | Make GetClusterNodesRequestPBImpl thread safe |  Major | client | Prabhu Joseph | SwathiChandrashekar |
| [HADOOP-13464](https://issues.apache.org/jira/browse/HADOOP-13464) | update GSON to 2.7+ |  Minor | build | Sean Busbey | Igor Dvorzhak |
| [HADOOP-18191](https://issues.apache.org/jira/browse/HADOOP-18191) | Log retry count while handling exceptions in RetryInvocationHandler |  Minor | . | Viraj Jasani | Viraj Jasani |
| [HDFS-16551](https://issues.apache.org/jira/browse/HDFS-16551) | Backport HADOOP-17588 to 3.3 and other active old branches. |  Major | . | Renukaprasad C | Renukaprasad C |
| [HDFS-16618](https://issues.apache.org/jira/browse/HDFS-16618) | sync\_file\_range error should include more volume and file info |  Minor | . | Viraj Jasani | Viraj Jasani |
| [HADOOP-18300](https://issues.apache.org/jira/browse/HADOOP-18300) | Update Gson to 2.9.0 |  Minor | build | Igor Dvorzhak | Igor Dvorzhak |


