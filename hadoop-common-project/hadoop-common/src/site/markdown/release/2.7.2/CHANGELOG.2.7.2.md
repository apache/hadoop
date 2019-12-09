
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

## Release 2.7.2 - 2016-01-25

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-4087](https://issues.apache.org/jira/browse/YARN-4087) | Followup fixes after YARN-2019 regarding RM behavior when state-store error occurs |  Major | . | Jian He | Jian He |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-7645](https://issues.apache.org/jira/browse/HDFS-7645) | Rolling upgrade is restoring blocks from trash multiple times |  Major | datanode | Nathan Roberts | Keisuke Ogiwara |
| [YARN-3248](https://issues.apache.org/jira/browse/YARN-3248) | Display count of nodes blacklisted by apps in the web UI |  Major | capacityscheduler, resourcemanager | Varun Vasudev | Varun Vasudev |
| [HADOOP-11812](https://issues.apache.org/jira/browse/HADOOP-11812) | Implement listLocatedStatus for ViewFileSystem to speed up split calculation |  Blocker | fs | Gera Shegalov | Gera Shegalov |
| [HADOOP-7139](https://issues.apache.org/jira/browse/HADOOP-7139) | Allow appending to existing SequenceFiles |  Major | io | Stephen Rose | Kanaka Kumar Avvaru |
| [HDFS-8659](https://issues.apache.org/jira/browse/HDFS-8659) | Block scanner INFO message is spamming logs |  Major | datanode | Yongjun Zhang | Yongjun Zhang |
| [HDFS-8722](https://issues.apache.org/jira/browse/HDFS-8722) | Optimize datanode writes for small writes and flushes |  Critical | . | Kihwal Lee | Kihwal Lee |
| [HADOOP-12232](https://issues.apache.org/jira/browse/HADOOP-12232) | Upgrade Tomcat dependency to 6.0.44. |  Major | build | Chris Nauroth | Chris Nauroth |
| [YARN-3170](https://issues.apache.org/jira/browse/YARN-3170) | YARN architecture document needs updating |  Major | documentation | Allen Wittenauer | Brahma Reddy Battula |
| [HDFS-7314](https://issues.apache.org/jira/browse/HDFS-7314) | When the DFSClient lease cannot be renewed, abort open-for-write files rather than the entire DFSClient |  Major | . | Ming Ma | Ming Ma |
| [YARN-3978](https://issues.apache.org/jira/browse/YARN-3978) | Configurably turn off the saving of container info in Generic AHS |  Major | timelineserver, yarn | Eric Payne | Eric Payne |
| [HADOOP-12280](https://issues.apache.org/jira/browse/HADOOP-12280) | Skip unit tests based on maven profile rather than NativeCodeLoader.isNativeCodeLoaded |  Minor | test | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-5323](https://issues.apache.org/jira/browse/HADOOP-5323) | Trash documentation should describe its directory structure and configurations |  Minor | documentation | Suman Sehgal | Weiwei Yang |
| [HDFS-8384](https://issues.apache.org/jira/browse/HDFS-8384) | Allow NN to startup if there are files having a lease but are not under construction |  Minor | namenode | Tsz Wo Nicholas Sze | Jing Zhao |
| [HADOOP-12413](https://issues.apache.org/jira/browse/HADOOP-12413) | AccessControlList should avoid calling getGroupNames in isUserInList with empty groups. |  Major | security | zhihai xu | zhihai xu |
| [YARN-4158](https://issues.apache.org/jira/browse/YARN-4158) | Remove duplicate close for LogWriter in AppLogAggregatorImpl#uploadLogsForContainers |  Minor | nodemanager | zhihai xu | zhihai xu |
| [YARN-3727](https://issues.apache.org/jira/browse/YARN-3727) | For better error recovery, check if the directory exists before using it for localization. |  Major | nodemanager | zhihai xu | zhihai xu |
| [HDFS-9221](https://issues.apache.org/jira/browse/HDFS-9221) | HdfsServerConstants#ReplicaState#getState should avoid calling values() since it creates a temporary array |  Major | performance | Staffan Friberg | Staffan Friberg |
| [HDFS-9434](https://issues.apache.org/jira/browse/HDFS-9434) | Recommission a datanode with 500k blocks may pause NN for 30 seconds |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-12825](https://issues.apache.org/jira/browse/HADOOP-12825) | Log slow name resolutions |  Major | . | Sidharta Seethana | Sidharta Seethana |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-9242](https://issues.apache.org/jira/browse/HADOOP-9242) | Duplicate surefire plugin config in hadoop-common |  Major | test | Andrey Klochkov | Andrey Klochkov |
| [HDFS-6945](https://issues.apache.org/jira/browse/HDFS-6945) | BlockManager should remove a block from excessReplicateMap and decrement ExcessBlocks metric when the block is removed |  Critical | namenode | Akira Ajisaka | Akira Ajisaka |
| [HDFS-8046](https://issues.apache.org/jira/browse/HDFS-8046) | Allow better control of getContentSummary |  Major | . | Kihwal Lee | Kihwal Lee |
| [YARN-2890](https://issues.apache.org/jira/browse/YARN-2890) | MiniYarnCluster should turn on timeline service if configured to do so |  Major | . | Mit Desai | Mit Desai |
| [HDFS-7725](https://issues.apache.org/jira/browse/HDFS-7725) | Incorrect "nodes in service" metrics caused all writes to fail |  Major | . | Ming Ma | Ming Ma |
| [HDFS-8099](https://issues.apache.org/jira/browse/HDFS-8099) | Change "DFSInputStream has been closed already" message to debug log level |  Minor | hdfs-client | Charles Lamb | Charles Lamb |
| [HADOOP-11491](https://issues.apache.org/jira/browse/HADOOP-11491) | HarFs incorrectly declared as requiring an authority |  Critical | fs | Gera Shegalov | Brahma Reddy Battula |
| [MAPREDUCE-5649](https://issues.apache.org/jira/browse/MAPREDUCE-5649) | Reduce cannot use more than 2G memory  for the final merge |  Major | mrv2 | stanley shi | Gera Shegalov |
| [HDFS-8219](https://issues.apache.org/jira/browse/HDFS-8219) | setStoragePolicy with folder behavior is different after cluster restart |  Major | . | Peter Shi | Surendra Singh Lilhore |
| [MAPREDUCE-6273](https://issues.apache.org/jira/browse/MAPREDUCE-6273) | HistoryFileManager should check whether summaryFile exists to avoid FileNotFoundException causing HistoryFileInfo into MOVE\_FAILED state |  Minor | jobhistoryserver | zhihai xu | zhihai xu |
| [HDFS-8431](https://issues.apache.org/jira/browse/HDFS-8431) | hdfs crypto class not found in Windows |  Critical | scripts | Sumana Sathish | Anu Engineer |
| [HDFS-7609](https://issues.apache.org/jira/browse/HDFS-7609) | Avoid retry cache collision when Standby NameNode loading edits |  Critical | namenode | Carrey Zhan | Ming Ma |
| [MAPREDUCE-6377](https://issues.apache.org/jira/browse/MAPREDUCE-6377) | JHS sorting on state column not working in webUi |  Minor | jobhistoryserver | Bibin A Chundatt | zhihai xu |
| [YARN-3780](https://issues.apache.org/jira/browse/YARN-3780) | Should use equals when compare Resource in RMNodeImpl#ReconnectNodeTransition |  Minor | resourcemanager | zhihai xu | zhihai xu |
| [HDFS-8615](https://issues.apache.org/jira/browse/HDFS-8615) | Correct HTTP method in WebHDFS document |  Major | documentation | Akira Ajisaka | Brahma Reddy Battula |
| [YARN-3802](https://issues.apache.org/jira/browse/YARN-3802) | Two RMNodes for the same NodeId are used in RM sometimes after NM is reconnected. |  Major | resourcemanager | zhihai xu | zhihai xu |
| [MAPREDUCE-5948](https://issues.apache.org/jira/browse/MAPREDUCE-5948) | org.apache.hadoop.mapred.LineRecordReader does not handle multibyte record delimiters well |  Critical | . | Kris Geusebroek | Akira Ajisaka |
| [HADOOP-8151](https://issues.apache.org/jira/browse/HADOOP-8151) | Error handling in snappy decompressor throws invalid exceptions |  Major | io, native | Todd Lipcon | Matt Foley |
| [HDFS-8656](https://issues.apache.org/jira/browse/HDFS-8656) | Preserve compatibility of ClientProtocol#rollingUpgrade after finalization |  Critical | rolling upgrades | Andrew Wang | Andrew Wang |
| [YARN-3793](https://issues.apache.org/jira/browse/YARN-3793) | Several NPEs when deleting local files on NM recovery |  Major | nodemanager | Karthik Kambatla | Varun Saxena |
| [YARN-3508](https://issues.apache.org/jira/browse/YARN-3508) | Prevent processing preemption events on the main RM dispatcher |  Major | resourcemanager, scheduler | Jason Lowe | Varun Saxena |
| [MAPREDUCE-6425](https://issues.apache.org/jira/browse/MAPREDUCE-6425) | ShuffleHandler passes wrong "base" parameter to getMapOutputInfo if mapId is not in the cache. |  Major | mrv2, nodemanager | zhihai xu | zhihai xu |
| [HADOOP-12186](https://issues.apache.org/jira/browse/HADOOP-12186) | ActiveStandbyElector shouldn't call monitorLockNodeAsync multiple times |  Major | ha | zhihai xu | zhihai xu |
| [YARN-3690](https://issues.apache.org/jira/browse/YARN-3690) | [JDK8] 'mvn site' fails |  Major | api, site | Akira Ajisaka | Brahma Reddy Battula |
| [MAPREDUCE-6426](https://issues.apache.org/jira/browse/MAPREDUCE-6426) | TestShuffleHandler#testGetMapOutputInfo is failing |  Major | test | Devaraj K | zhihai xu |
| [HADOOP-12191](https://issues.apache.org/jira/browse/HADOOP-12191) | Bzip2Factory is not thread safe |  Major | io | Jason Lowe | Brahma Reddy Battula |
| [HDFS-8767](https://issues.apache.org/jira/browse/HDFS-8767) | RawLocalFileSystem.listStatus() returns null for UNIX pipefile |  Critical | . | Haohui Mai | Kanaka Kumar Avvaru |
| [YARN-3535](https://issues.apache.org/jira/browse/YARN-3535) | Scheduler must re-request container resources when RMContainer transitions from ALLOCATED to KILLED |  Critical | capacityscheduler, fairscheduler, resourcemanager | Peng Zhang | Peng Zhang |
| [YARN-3905](https://issues.apache.org/jira/browse/YARN-3905) | Application History Server UI NPEs when accessing apps run after RM restart |  Major | timelineserver | Eric Payne | Eric Payne |
| [YARN-3878](https://issues.apache.org/jira/browse/YARN-3878) | AsyncDispatcher can hang while stopping if it is configured for draining events on stop |  Critical | . | Varun Saxena | Varun Saxena |
| [YARN-2019](https://issues.apache.org/jira/browse/YARN-2019) | Retrospect on decision of making RM crashed if any exception throw in ZKRMStateStore |  Critical | . | Junping Du | Jian He |
| [HDFS-8806](https://issues.apache.org/jira/browse/HDFS-8806) | Inconsistent metrics: number of missing blocks with replication factor 1 not properly cleared |  Major | . | Zhe Zhang | Zhe Zhang |
| [YARN-3967](https://issues.apache.org/jira/browse/YARN-3967) | Fetch the application report from the AHS if the RM does not know about it |  Major | . | Mit Desai | Mit Desai |
| [YARN-3925](https://issues.apache.org/jira/browse/YARN-3925) | ContainerLogsUtils#getContainerLogFile fails to read container log files from full disks. |  Critical | nodemanager | zhihai xu | zhihai xu |
| [YARN-3990](https://issues.apache.org/jira/browse/YARN-3990) | AsyncDispatcher may overloaded with RMAppNodeUpdateEvent when Node is connected/disconnected |  Critical | resourcemanager | Rohith Sharma K S | Bibin A Chundatt |
| [HDFS-8850](https://issues.apache.org/jira/browse/HDFS-8850) | VolumeScanner thread exits with exception if there is no block pool to be scanned but there are suspicious blocks |  Major | datanode | Colin P. McCabe | Colin P. McCabe |
| [HADOOP-12304](https://issues.apache.org/jira/browse/HADOOP-12304) | Applications using FileContext fail with the default file system configured to be wasb/s3/etc. |  Blocker | fs | Chris Nauroth | Chris Nauroth |
| [HADOOP-11932](https://issues.apache.org/jira/browse/HADOOP-11932) |  MetricsSinkAdapter hangs when being stopped |  Critical | . | Jian He | Brahma Reddy Battula |
| [YARN-3999](https://issues.apache.org/jira/browse/YARN-3999) | RM hangs on draining events |  Major | . | Jian He | Jian He |
| [HDFS-8879](https://issues.apache.org/jira/browse/HDFS-8879) | Quota by storage type usage incorrectly initialized upon namenode restart |  Major | namenode | Kihwal Lee | Xiaoyu Yao |
| [YARN-4005](https://issues.apache.org/jira/browse/YARN-4005) | Completed container whose app is finished is not removed from NMStateStore |  Major | . | Jun Gong | Jun Gong |
| [YARN-4047](https://issues.apache.org/jira/browse/YARN-4047) | ClientRMService getApplications has high scheduler lock contention |  Major | resourcemanager | Jason Lowe | Jason Lowe |
| [HDFS-8891](https://issues.apache.org/jira/browse/HDFS-8891) | HDFS concat should keep srcs order |  Blocker | . | Yong Zhang | Yong Zhang |
| [MAPREDUCE-6439](https://issues.apache.org/jira/browse/MAPREDUCE-6439) | AM may fail instead of retrying if RM shuts down during the allocate call |  Critical | . | Anubhav Dhoot | Anubhav Dhoot |
| [HDFS-8852](https://issues.apache.org/jira/browse/HDFS-8852) | HDFS architecture documentation of version 2.x is outdated about append write support |  Major | documentation | Hong Dai Thanh | Ajith S |
| [YARN-3857](https://issues.apache.org/jira/browse/YARN-3857) | Memory leak in ResourceManager with SIMPLE mode |  Critical | resourcemanager | mujunchao | mujunchao |
| [HDFS-8867](https://issues.apache.org/jira/browse/HDFS-8867) | Enable optimized block reports |  Major | . | Rushabh S Shah | Daryn Sharp |
| [HDFS-8863](https://issues.apache.org/jira/browse/HDFS-8863) | The remaining space check in BlockPlacementPolicyDefault is flawed |  Critical | . | Kihwal Lee | Kihwal Lee |
| [MAPREDUCE-6454](https://issues.apache.org/jira/browse/MAPREDUCE-6454) | MapReduce doesn't set the HADOOP\_CLASSPATH for jar lib in distributed cache. |  Critical | . | Junping Du | Junping Du |
| [YARN-3896](https://issues.apache.org/jira/browse/YARN-3896) | RMNode transitioned from RUNNING to REBOOTED because its response id had not been reset synchronously |  Major | resourcemanager | Jun Gong | Jun Gong |
| [HDFS-8846](https://issues.apache.org/jira/browse/HDFS-8846) | Add a unit test for INotify functionality across a layout version upgrade |  Major | namenode | Zhe Zhang | Zhe Zhang |
| [HDFS-8950](https://issues.apache.org/jira/browse/HDFS-8950) | NameNode refresh doesn't remove DataNodes that are no longer in the allowed list |  Major | datanode, namenode | Daniel Templeton | Daniel Templeton |
| [HADOOP-12359](https://issues.apache.org/jira/browse/HADOOP-12359) | hadoop fs -getmerge doc is wrong |  Major | documentation | Daniel Templeton | Jagadesh Kiran N |
| [HADOOP-10365](https://issues.apache.org/jira/browse/HADOOP-10365) | BufferedOutputStream in FileUtil#unpackEntries() should be closed in finally block |  Minor | util | Ted Yu | Kiran Kumar M R |
| [HDFS-8995](https://issues.apache.org/jira/browse/HDFS-8995) | Flaw in registration bookeeping can make DN die on reconnect |  Critical | . | Kihwal Lee | Kihwal Lee |
| [HADOOP-12006](https://issues.apache.org/jira/browse/HADOOP-12006) | Remove unimplemented option for \`hadoop fs -ls\` from document in branch-2.7 |  Major | documentation, fs | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-12213](https://issues.apache.org/jira/browse/HADOOP-12213) | Interrupted exception can occur when Client#stop is called |  Minor | . | Oleg Zhurakousky | Kuhu Shukla |
| [YARN-4103](https://issues.apache.org/jira/browse/YARN-4103) | RM WebServices missing scheme for appattempts logLinks |  Major | . | Jonathan Eagles | Jonathan Eagles |
| [YARN-4105](https://issues.apache.org/jira/browse/YARN-4105) | Capacity Scheduler headroom for DRF is wrong |  Major | capacityscheduler | Chang Li | Chang Li |
| [MAPREDUCE-6442](https://issues.apache.org/jira/browse/MAPREDUCE-6442) | Stack trace is missing when error occurs in client protocol provider's constructor |  Major | client | Chang Li | Chang Li |
| [YARN-4096](https://issues.apache.org/jira/browse/YARN-4096) | App local logs are leaked if log aggregation fails to initialize for the app |  Major | log-aggregation, nodemanager | Jason Lowe | Jason Lowe |
| [HDFS-9033](https://issues.apache.org/jira/browse/HDFS-9033) | dfsadmin -metasave prints "NaN" for cache used% |  Major | . | Archana T | Brahma Reddy Battula |
| [MAPREDUCE-6474](https://issues.apache.org/jira/browse/MAPREDUCE-6474) | ShuffleHandler can possibly exhaust nodemanager file descriptors |  Major | mrv2, nodemanager | Nathan Roberts | Kuhu Shukla |
| [HDFS-9042](https://issues.apache.org/jira/browse/HDFS-9042) | Update document for the Storage policy name |  Minor | documentation | J.Andreina | J.Andreina |
| [MAPREDUCE-6472](https://issues.apache.org/jira/browse/MAPREDUCE-6472) | MapReduce AM should have java.io.tmpdir=./tmp to be consistent with tasks |  Major | mr-am | Jason Lowe | Naganarasimha G R |
| [MAPREDUCE-6481](https://issues.apache.org/jira/browse/MAPREDUCE-6481) | LineRecordReader may give incomplete record and wrong position/key information for uncompressed input sometimes. |  Critical | mrv2 | zhihai xu | zhihai xu |
| [MAPREDUCE-5982](https://issues.apache.org/jira/browse/MAPREDUCE-5982) | Task attempts that fail from the ASSIGNED state can disappear |  Major | mr-am | Jason Lowe | Chang Li |
| [YARN-3697](https://issues.apache.org/jira/browse/YARN-3697) | FairScheduler: ContinuousSchedulingThread can fail to shutdown |  Critical | fairscheduler | zhihai xu | zhihai xu |
| [HDFS-9043](https://issues.apache.org/jira/browse/HDFS-9043) | Doc updation for commands in HDFS Federation |  Minor | documentation | J.Andreina | J.Andreina |
| [YARN-3975](https://issues.apache.org/jira/browse/YARN-3975) | WebAppProxyServlet should not redirect to RM page if AHS is enabled |  Major | . | Mit Desai | Mit Desai |
| [YARN-3624](https://issues.apache.org/jira/browse/YARN-3624) | ApplicationHistoryServer reverses the order of the filters it gets |  Major | timelineserver | Mit Desai | Mit Desai |
| [HDFS-9106](https://issues.apache.org/jira/browse/HDFS-9106) | Transfer failure during pipeline recovery causes permanent write failures |  Critical | . | Kihwal Lee | Kihwal Lee |
| [MAPREDUCE-6492](https://issues.apache.org/jira/browse/MAPREDUCE-6492) | AsyncDispatcher exit with NPE on TaskAttemptImpl#sendJHStartEventForAssignedFailTask |  Critical | . | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-4180](https://issues.apache.org/jira/browse/YARN-4180) | AMLauncher does not retry on failures when talking to NM |  Critical | resourcemanager | Anubhav Dhoot | Anubhav Dhoot |
| [MAPREDUCE-6497](https://issues.apache.org/jira/browse/MAPREDUCE-6497) | Fix wrong value of JOB\_FINISHED event in JobHistoryEventHandler |  Major | . | Shinichi Yamashita | Shinichi Yamashita |
| [HADOOP-12230](https://issues.apache.org/jira/browse/HADOOP-12230) | hadoop-project declares duplicate, conflicting curator dependencies |  Minor | build | Steve Loughran | Rakesh R |
| [YARN-3619](https://issues.apache.org/jira/browse/YARN-3619) | ContainerMetrics unregisters during getMetrics and leads to ConcurrentModificationException |  Major | nodemanager | Jason Lowe | zhihai xu |
| [YARN-4209](https://issues.apache.org/jira/browse/YARN-4209) | RMStateStore FENCED state doesn't work due to updateFencedState called by stateMachine.doTransition |  Critical | resourcemanager | zhihai xu | zhihai xu |
| [HDFS-9178](https://issues.apache.org/jira/browse/HDFS-9178) | Slow datanode I/O can cause a wrong node to be marked bad |  Critical | . | Kihwal Lee | Kihwal Lee |
| [HADOOP-12465](https://issues.apache.org/jira/browse/HADOOP-12465) | Incorrect javadoc in WritableUtils.java |  Minor | documentation | Martin Petricek | Jagadesh Kiran N |
| [HDFS-8676](https://issues.apache.org/jira/browse/HDFS-8676) | Delayed rolling upgrade finalization can cause heartbeat expiration and write failures |  Critical | . | Kihwal Lee | Walter Su |
| [HDFS-9220](https://issues.apache.org/jira/browse/HDFS-9220) | Reading small file (\< 512 bytes) that is open for append fails due to incorrect checksum |  Blocker | . | Bogdan Raducanu | Jing Zhao |
| [HADOOP-12464](https://issues.apache.org/jira/browse/HADOOP-12464) | Interrupted client may try to fail-over and retry |  Major | ipc | Kihwal Lee | Kihwal Lee |
| [YARN-4281](https://issues.apache.org/jira/browse/YARN-4281) | 2.7 RM app page is broken |  Blocker | . | Chang Li | Chang Li |
| [YARN-3798](https://issues.apache.org/jira/browse/YARN-3798) | ZKRMStateStore shouldn't create new session without occurrance of SESSIONEXPIED |  Blocker | resourcemanager | Bibin A Chundatt | Varun Saxena |
| [MAPREDUCE-6518](https://issues.apache.org/jira/browse/MAPREDUCE-6518) | Set SO\_KEEPALIVE on shuffle connections |  Major | mrv2, nodemanager | Nathan Roberts | Chang Li |
| [HDFS-9273](https://issues.apache.org/jira/browse/HDFS-9273) | ACLs on root directory may be lost after NN restart |  Critical | namenode | Xiao Chen | Xiao Chen |
| [YARN-4000](https://issues.apache.org/jira/browse/YARN-4000) | RM crashes with NPE if leaf queue becomes parent queue during restart |  Major | capacityscheduler, resourcemanager | Jason Lowe | Varun Saxena |
| [YARN-4009](https://issues.apache.org/jira/browse/YARN-4009) | CORS support for ResourceManager REST API |  Major | . | Prakash Ramachandran | Varun Vasudev |
| [YARN-4041](https://issues.apache.org/jira/browse/YARN-4041) | Slow delegation token renewal can severely prolong RM recovery |  Major | resourcemanager | Jason Lowe | Sunil Govindan |
| [HDFS-9290](https://issues.apache.org/jira/browse/HDFS-9290) | DFSClient#callAppend() is not backward compatible for slightly older NameNodes |  Blocker | . | Tony Wu | Tony Wu |
| [HDFS-9305](https://issues.apache.org/jira/browse/HDFS-9305) | Delayed heartbeat processing causes storm of subsequent heartbeats |  Major | datanode | Chris Nauroth | Arpit Agarwal |
| [HDFS-9317](https://issues.apache.org/jira/browse/HDFS-9317) | Document fsck -blockId and -storagepolicy options in branch-2.7 |  Major | documentation | Akira Ajisaka | Akira Ajisaka |
| [HDFS-9083](https://issues.apache.org/jira/browse/HDFS-9083) | Replication violates block placement policy. |  Blocker | namenode | Rushabh S Shah | Rushabh S Shah |
| [YARN-4313](https://issues.apache.org/jira/browse/YARN-4313) | Race condition in MiniMRYarnCluster when getting history server address |  Major | . | Jian He | Jian He |
| [YARN-4312](https://issues.apache.org/jira/browse/YARN-4312) | TestSubmitApplicationWithRMHA fails on branch-2.7 and branch-2.6 as some of the test cases time out |  Major | . | Varun Saxena | Varun Saxena |
| [YARN-4320](https://issues.apache.org/jira/browse/YARN-4320) | TestJobHistoryEventHandler fails as AHS in MiniYarnCluster no longer binds to default port 8188 |  Major | . | Varun Saxena | Varun Saxena |
| [MAPREDUCE-6528](https://issues.apache.org/jira/browse/MAPREDUCE-6528) | Memory leak for HistoryFileManager.getJobSummary() |  Critical | jobhistoryserver | Junping Du | Junping Du |
| [MAPREDUCE-6451](https://issues.apache.org/jira/browse/MAPREDUCE-6451) | DistCp has incorrect chunkFilePath for multiple jobs when strategy is dynamic |  Major | distcp | Kuhu Shukla | Kuhu Shukla |
| [YARN-4321](https://issues.apache.org/jira/browse/YARN-4321) | Incessant retries if NoAuthException is thrown by Zookeeper in non HA mode |  Major | resourcemanager | Varun Saxena | Varun Saxena |
| [YARN-4326](https://issues.apache.org/jira/browse/YARN-4326) | Fix TestDistributedShell timeout as AHS in MiniYarnCluster no longer binds to default port 8188 |  Major | . | MENG DING | MENG DING |
| [HDFS-9289](https://issues.apache.org/jira/browse/HDFS-9289) | Make DataStreamer#block thread safe and verify genStamp in commitBlock |  Critical | . | Chang Li | Chang Li |
| [YARN-4127](https://issues.apache.org/jira/browse/YARN-4127) | RM fail with noAuth error if switched from failover mode to non-failover mode |  Major | resourcemanager | Jian He | Varun Saxena |
| [HADOOP-12526](https://issues.apache.org/jira/browse/HADOOP-12526) | [Branch-2] there are duplicate dependency definitions in pom's |  Major | build | Sangjin Lee | Sangjin Lee |
| [HADOOP-12451](https://issues.apache.org/jira/browse/HADOOP-12451) | [Branch-2] Setting HADOOP\_HOME explicitly should be allowed |  Blocker | scripts | Karthik Kambatla | Karthik Kambatla |
| [YARN-4241](https://issues.apache.org/jira/browse/YARN-4241) | Fix typo of property name in yarn-default.xml |  Major | documentation | Anthony Rojas | Anthony Rojas |
| [MAPREDUCE-6540](https://issues.apache.org/jira/browse/MAPREDUCE-6540) | TestMRTimelineEventHandling fails |  Major | test | Sangjin Lee | Sangjin Lee |
| [YARN-4354](https://issues.apache.org/jira/browse/YARN-4354) | Public resource localization fails with NPE |  Blocker | nodemanager | Jason Lowe | Jason Lowe |
| [HDFS-9413](https://issues.apache.org/jira/browse/HDFS-9413) | getContentSummary() on standby should throw StandbyException |  Critical | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-9431](https://issues.apache.org/jira/browse/HDFS-9431) | DistributedFileSystem#concat fails if the target path is relative. |  Major | hdfs-client | Kazuho Fujii | Kazuho Fujii |
| [YARN-2859](https://issues.apache.org/jira/browse/YARN-2859) | ApplicationHistoryServer binds to default port 8188 in MiniYARNCluster |  Critical | timelineserver | Hitesh Shah | Vinod Kumar Vavilapalli |
| [HADOOP-12577](https://issues.apache.org/jira/browse/HADOOP-12577) | Bump up commons-collections version to 3.2.2 to address a security flaw |  Blocker | build, security | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-4344](https://issues.apache.org/jira/browse/YARN-4344) | NMs reconnecting with changed capabilities can lead to wrong cluster resource calculations |  Critical | resourcemanager | Varun Vasudev | Varun Vasudev |
| [MAPREDUCE-5883](https://issues.apache.org/jira/browse/MAPREDUCE-5883) | "Total megabyte-seconds" in job counters is slightly misleading |  Minor | . | Nathan Roberts | Nathan Roberts |
| [YARN-4365](https://issues.apache.org/jira/browse/YARN-4365) | FileSystemNodeLabelStore should check for root dir existence on startup |  Major | resourcemanager | Jason Lowe | Kuhu Shukla |
| [HADOOP-12415](https://issues.apache.org/jira/browse/HADOOP-12415) | hdfs and nfs builds broken on -missing compile-time dependency on netty |  Major | nfs | Konstantin Boudnik | Tom Zeng |
| [MAPREDUCE-6549](https://issues.apache.org/jira/browse/MAPREDUCE-6549) | multibyte delimiters with LineRecordReader cause duplicate records |  Major | mrv1, mrv2 | Dustin Cote | Wilfred Spiegelenburg |
| [HDFS-9426](https://issues.apache.org/jira/browse/HDFS-9426) | Rollingupgrade finalization is not backward compatible |  Blocker | . | Kihwal Lee | Kihwal Lee |
| [HDFS-9470](https://issues.apache.org/jira/browse/HDFS-9470) | Encryption zone on root not loaded from fsimage after NN restart |  Critical | . | Xiao Chen | Xiao Chen |
| [HDFS-9294](https://issues.apache.org/jira/browse/HDFS-9294) | DFSClient  deadlock when close file and failed to renew lease |  Blocker | hdfs-client | DENG FEI | Brahma Reddy Battula |
| [YARN-4348](https://issues.apache.org/jira/browse/YARN-4348) | ZKRMStateStore.syncInternal shouldn't wait for sync completion for avoiding blocking ZK's event thread |  Blocker | . | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [YARN-4424](https://issues.apache.org/jira/browse/YARN-4424) | Fix deadlock in RMAppImpl |  Blocker | . | Yesha Vora | Jian He |
| [YARN-4434](https://issues.apache.org/jira/browse/YARN-4434) | NodeManager Disk Checker parameter documentation is not correct |  Minor | documentation, nodemanager | Takashi Ohnishi | Weiwei Yang |
| [HDFS-9445](https://issues.apache.org/jira/browse/HDFS-9445) | Datanode may deadlock while handling a bad volume |  Blocker | . | Kihwal Lee | Walter Su |
| [HDFS-9574](https://issues.apache.org/jira/browse/HDFS-9574) | Reduce client failures during datanode restart |  Major | . | Kihwal Lee | Kihwal Lee |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-3580](https://issues.apache.org/jira/browse/YARN-3580) | [JDK 8] TestClientRMService.testGetLabelsToNodes fails |  Major | test | Robert Kanter | Robert Kanter |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-3136](https://issues.apache.org/jira/browse/YARN-3136) | getTransferredContainers can be a bottleneck during AM registration |  Major | scheduler | Jason Lowe | Sunil Govindan |
| [YARN-3700](https://issues.apache.org/jira/browse/YARN-3700) | ATS Web Performance issue at load time when large number of jobs |  Major | resourcemanager, webapp, yarn | Xuan Gong | Xuan Gong |
| [YARN-3740](https://issues.apache.org/jira/browse/YARN-3740) | Fixed the typo with the configuration name: APPLICATION\_HISTORY\_PREFIX\_MAX\_APPS |  Major | resourcemanager, webapp, yarn | Xuan Gong | Xuan Gong |
| [YARN-3969](https://issues.apache.org/jira/browse/YARN-3969) | Allow jobs to be submitted to reservation that is active but does not have any allocations |  Major | capacityscheduler, fairscheduler, resourcemanager | Subru Krishnan | Subru Krishnan |
| [YARN-4092](https://issues.apache.org/jira/browse/YARN-4092) | RM HA UI redirection needs to be fixed when both RMs are in standby mode |  Major | resourcemanager | Xuan Gong | Xuan Gong |
| [YARN-2801](https://issues.apache.org/jira/browse/YARN-2801) | Add documentation for node labels feature |  Major | documentation | Gururaj Shetty | Wangda Tan |
| [YARN-3893](https://issues.apache.org/jira/browse/YARN-3893) | Both RM in active state when Admin#transitionToActive failure from refeshAll() |  Critical | resourcemanager | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-4101](https://issues.apache.org/jira/browse/YARN-4101) | RM should print alert messages if Zookeeper and Resourcemanager gets connection issue |  Critical | yarn | Yesha Vora | Xuan Gong |
| [YARN-2513](https://issues.apache.org/jira/browse/YARN-2513) | Host framework UIs in YARN for use with the ATS |  Major | timelineserver | Jonathan Eagles | Jonathan Eagles |
| [YARN-2902](https://issues.apache.org/jira/browse/YARN-2902) | Killing a container that is localizing can orphan resources in the DOWNLOADING state |  Major | nodemanager | Jason Lowe | Varun Saxena |


