
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

## Release 3.0.3 - 2018-05-31

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-13099](https://issues.apache.org/jira/browse/HDFS-13099) | RBF: Use the ZooKeeper as the default State Store |  Minor | documentation | Yiqun Lin | Yiqun Lin |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-13283](https://issues.apache.org/jira/browse/HDFS-13283) | Percentage based Reserved Space Calculation for DataNode |  Major | datanode, hdfs | Lukas Majercak | Lukas Majercak |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-12455](https://issues.apache.org/jira/browse/HDFS-12455) | WebHDFS - Adding "snapshot enabled" status to ListStatus query result. |  Major | snapshots, webhdfs | Ajay Kumar | Ajay Kumar |
| [HDFS-13062](https://issues.apache.org/jira/browse/HDFS-13062) | Provide support for JN to use separate journal disk per namespace |  Major | federation, journal-node | Bharat Viswanadham | Bharat Viswanadham |
| [HDFS-12933](https://issues.apache.org/jira/browse/HDFS-12933) | Improve logging when DFSStripedOutputStream failed to write some blocks |  Minor | erasure-coding | Xiao Chen | chencan |
| [HADOOP-13972](https://issues.apache.org/jira/browse/HADOOP-13972) | ADLS to support per-store configuration |  Major | fs/adl | John Zhuge | Sharad Sonker |
| [YARN-7813](https://issues.apache.org/jira/browse/YARN-7813) | Capacity Scheduler Intra-queue Preemption should be configurable for each queue |  Major | capacity scheduler, scheduler preemption | Eric Payne | Eric Payne |
| [HDFS-13175](https://issues.apache.org/jira/browse/HDFS-13175) | Add more information for checking argument in DiskBalancerVolume |  Minor | diskbalancer | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HDFS-11187](https://issues.apache.org/jira/browse/HDFS-11187) | Optimize disk access for last partial chunk checksum of Finalized replica |  Major | datanode | Wei-Chiu Chuang | Gabor Bota |
| [MAPREDUCE-7061](https://issues.apache.org/jira/browse/MAPREDUCE-7061) | SingleCluster setup document needs to be updated |  Major | . | Bharat Viswanadham | Bharat Viswanadham |
| [HADOOP-15263](https://issues.apache.org/jira/browse/HADOOP-15263) | hadoop cloud-storage module to mark hadoop-common as provided; add azure-datalake |  Minor | build | Steve Loughran | Steve Loughran |
| [MAPREDUCE-7060](https://issues.apache.org/jira/browse/MAPREDUCE-7060) | Cherry Pick PathOutputCommitter class/factory to branch-3.0 |  Minor | . | Steve Loughran | Steve Loughran |
| [HADOOP-15279](https://issues.apache.org/jira/browse/HADOOP-15279) | increase maven heap size recommendations |  Minor | build, documentation, test | Allen Wittenauer | Allen Wittenauer |
| [HDFS-13170](https://issues.apache.org/jira/browse/HDFS-13170) | Port webhdfs unmaskedpermission parameter to HTTPFS |  Major | . | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-13225](https://issues.apache.org/jira/browse/HDFS-13225) | StripeReader#checkMissingBlocks() 's IOException info is incomplete |  Major | erasure-coding, hdfs-client | lufei | lufei |
| [HDFS-11394](https://issues.apache.org/jira/browse/HDFS-11394) | Support for getting erasure coding policy through WebHDFS#FileStatus |  Major | erasure-coding, namenode | Kai Sasaki | Kai Sasaki |
| [HADOOP-15311](https://issues.apache.org/jira/browse/HADOOP-15311) | HttpServer2 needs a way to configure the acceptor/selector count |  Major | common | Erik Krogen | Erik Krogen |
| [HDFS-11600](https://issues.apache.org/jira/browse/HDFS-11600) | Refactor TestDFSStripedOutputStreamWithFailure test classes |  Minor | erasure-coding, test | Andrew Wang | SammiChen |
| [HDFS-12884](https://issues.apache.org/jira/browse/HDFS-12884) | BlockUnderConstructionFeature.truncateBlock should be of type BlockInfo |  Major | namenode | Konstantin Shvachko | chencan |
| [HADOOP-15334](https://issues.apache.org/jira/browse/HADOOP-15334) | Upgrade Maven surefire plugin |  Major | build | Arpit Agarwal | Arpit Agarwal |
| [HADOOP-15312](https://issues.apache.org/jira/browse/HADOOP-15312) | Undocumented KeyProvider configuration keys |  Major | . | Wei-Chiu Chuang | LiXin Ge |
| [YARN-7623](https://issues.apache.org/jira/browse/YARN-7623) | Fix the CapacityScheduler Queue configuration documentation |  Major | . | Arun Suresh | Jonathan Hung |
| [HDFS-13314](https://issues.apache.org/jira/browse/HDFS-13314) | NameNode should optionally exit if it detects FsImage corruption |  Major | namenode | Arpit Agarwal | Arpit Agarwal |
| [HADOOP-15342](https://issues.apache.org/jira/browse/HADOOP-15342) | Update ADLS connector to use the current SDK version (2.2.7) |  Major | fs/adl | Atul Sikaria | Atul Sikaria |
| [HDFS-13462](https://issues.apache.org/jira/browse/HDFS-13462) | Add BIND\_HOST configuration for JournalNode's HTTP and RPC Servers |  Major | hdfs, journal-node | Lukas Majercak | Lukas Majercak |
| [HADOOP-14841](https://issues.apache.org/jira/browse/HADOOP-14841) | Kms client should disconnect if unable to get output stream from connection. |  Major | kms | Xiao Chen | Rushabh S Shah |
| [HDFS-12981](https://issues.apache.org/jira/browse/HDFS-12981) | renameSnapshot a Non-Existent snapshot to itself should throw error |  Minor | hdfs | Sailesh Patel | Kitti Nanasi |
| [YARN-8201](https://issues.apache.org/jira/browse/YARN-8201) | Skip stacktrace of few exception from ClientRMService |  Minor | . | Bibin A Chundatt | Bilwa S T |
| [HADOOP-15441](https://issues.apache.org/jira/browse/HADOOP-15441) | Log kms url and token service at debug level. |  Minor | . | Wei-Chiu Chuang | Gabor Bota |
| [HDFS-13544](https://issues.apache.org/jira/browse/HDFS-13544) | Improve logging for JournalNode in federated cluster |  Major | federation, hdfs | Hanisha Koneru | Hanisha Koneru |
| [HADOOP-15486](https://issues.apache.org/jira/browse/HADOOP-15486) | Make NetworkTopology#netLock fair |  Major | net | Nanda kumar | Nanda kumar |
| [HDFS-13493](https://issues.apache.org/jira/browse/HDFS-13493) | Reduce the HttpServer2 thread count on DataNodes |  Major | datanode | Erik Krogen | Erik Krogen |
| [HADOOP-15449](https://issues.apache.org/jira/browse/HADOOP-15449) | Increase default timeout of ZK session to avoid frequent NameNode failover |  Critical | common | Karthik Palanisamy | Karthik Palanisamy |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-11968](https://issues.apache.org/jira/browse/HDFS-11968) | ViewFS: StoragePolicies commands fail with HDFS federation |  Major | hdfs | Mukul Kumar Singh | Mukul Kumar Singh |
| [HDFS-12813](https://issues.apache.org/jira/browse/HDFS-12813) | RequestHedgingProxyProvider can hide Exception thrown from the Namenode for proxy size of 1 |  Major | ha | Mukul Kumar Singh | Mukul Kumar Singh |
| [HDFS-13048](https://issues.apache.org/jira/browse/HDFS-13048) | LowRedundancyReplicatedBlocks metric can be negative |  Major | metrics | Akira Ajisaka | Akira Ajisaka |
| [HDFS-13115](https://issues.apache.org/jira/browse/HDFS-13115) | In getNumUnderConstructionBlocks(), ignore the inodeIds for which the inodes have been deleted |  Major | . | Yongjun Zhang | Yongjun Zhang |
| [HDFS-10453](https://issues.apache.org/jira/browse/HDFS-10453) | ReplicationMonitor thread could stuck for long time due to the race between replication and delete of same file in a large cluster. |  Major | namenode | He Xiaoqiao | He Xiaoqiao |
| [MAPREDUCE-7053](https://issues.apache.org/jira/browse/MAPREDUCE-7053) | Timed out tasks can fail to produce thread dump |  Major | . | Jason Lowe | Jason Lowe |
| [HADOOP-15206](https://issues.apache.org/jira/browse/HADOOP-15206) | BZip2 drops and duplicates records when input split size is small |  Major | . | Aki Tanaka | Aki Tanaka |
| [YARN-7937](https://issues.apache.org/jira/browse/YARN-7937) | Fix http method name in Cluster Application Timeout Update API example request |  Minor | docs, documentation | Charan Hebri | Charan Hebri |
| [YARN-7947](https://issues.apache.org/jira/browse/YARN-7947) | Capacity Scheduler intra-queue preemption can NPE for non-schedulable apps |  Major | capacity scheduler, scheduler preemption | Eric Payne | Eric Payne |
| [HADOOP-10571](https://issues.apache.org/jira/browse/HADOOP-10571) | Use Log.\*(Object, Throwable) overload to log exceptions |  Major | . | Arpit Agarwal | Andras Bokor |
| [HDFS-12781](https://issues.apache.org/jira/browse/HDFS-12781) | After Datanode down, In Namenode UI Datanode tab is throwing warning message. |  Major | datanode | Harshakiran Reddy | Brahma Reddy Battula |
| [HDFS-12070](https://issues.apache.org/jira/browse/HDFS-12070) | Failed block recovery leaves files open indefinitely and at risk for data loss |  Major | . | Daryn Sharp | Kihwal Lee |
| [HDFS-13145](https://issues.apache.org/jira/browse/HDFS-13145) | SBN crash when transition to ANN with in-progress edit tailing enabled |  Major | ha, namenode | Chao Sun | Chao Sun |
| [HDFS-13114](https://issues.apache.org/jira/browse/HDFS-13114) | CryptoAdmin#ReencryptZoneCommand should resolve Namespace info from path |  Major | encryption, hdfs | Hanisha Koneru | Hanisha Koneru |
| [HDFS-13081](https://issues.apache.org/jira/browse/HDFS-13081) | Datanode#checkSecureConfig should allow SASL and privileged HTTP |  Major | datanode, security | Xiaoyu Yao | Ajay Kumar |
| [MAPREDUCE-7059](https://issues.apache.org/jira/browse/MAPREDUCE-7059) | Downward Compatibility issue: MR job fails because of unknown setErasureCodingPolicy method from 3.x client to HDFS 2.x cluster |  Critical | job submission | Jiandan Yang | Jiandan Yang |
| [HADOOP-15275](https://issues.apache.org/jira/browse/HADOOP-15275) | Incorrect javadoc for return type of RetryPolicy#shouldRetry |  Minor | documentation | Nanda kumar | Nanda kumar |
| [YARN-7511](https://issues.apache.org/jira/browse/YARN-7511) | NPE in ContainerLocalizer when localization failed for running container |  Major | nodemanager | Tao Yang | Tao Yang |
| [MAPREDUCE-7023](https://issues.apache.org/jira/browse/MAPREDUCE-7023) | TestHadoopArchiveLogs.testCheckFilesAndSeedApps fails on rerun |  Minor | test | Gergely Novák | Gergely Novák |
| [HDFS-13040](https://issues.apache.org/jira/browse/HDFS-13040) | Kerberized inotify client fails despite kinit properly |  Major | namenode | Wei-Chiu Chuang | Xiao Chen |
| [YARN-7736](https://issues.apache.org/jira/browse/YARN-7736) | Fix itemization in YARN federation document |  Minor | documentation | Akira Ajisaka | Sen Zhao |
| [HDFS-13164](https://issues.apache.org/jira/browse/HDFS-13164) | File not closed if streamer fail with DSQuotaExceededException |  Major | hdfs-client | Xiao Chen | Xiao Chen |
| [HADOOP-15289](https://issues.apache.org/jira/browse/HADOOP-15289) | FileStatus.readFields() assertion incorrect |  Critical | . | Steve Loughran | Steve Loughran |
| [HDFS-13109](https://issues.apache.org/jira/browse/HDFS-13109) | Support fully qualified hdfs path in EZ commands |  Major | hdfs | Hanisha Koneru | Hanisha Koneru |
| [HADOOP-15296](https://issues.apache.org/jira/browse/HADOOP-15296) | Fix a wrong link for RBF in the top page |  Minor | documentation | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-15273](https://issues.apache.org/jira/browse/HADOOP-15273) | distcp can't handle remote stores with different checksum algorithms |  Critical | tools/distcp | Steve Loughran | Steve Loughran |
| [MAPREDUCE-6930](https://issues.apache.org/jira/browse/MAPREDUCE-6930) | mapreduce.map.cpu.vcores and mapreduce.reduce.cpu.vcores are both present twice in mapred-default.xml |  Major | mrv2 | Daniel Templeton | Sen Zhao |
| [HDFS-13190](https://issues.apache.org/jira/browse/HDFS-13190) | Document WebHDFS support for snapshot diff |  Major | documentation, webhdfs | Xiaoyu Yao | Lokesh Jain |
| [HDFS-13244](https://issues.apache.org/jira/browse/HDFS-13244) | Add stack, conf, metrics links to utilities dropdown in NN webUI |  Major | . | Bharat Viswanadham | Bharat Viswanadham |
| [HDFS-12156](https://issues.apache.org/jira/browse/HDFS-12156) | TestFSImage fails without -Pnative |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [HDFS-13239](https://issues.apache.org/jira/browse/HDFS-13239) | Fix non-empty dir warning message when setting default EC policy |  Minor | . | Hanisha Koneru | Bharat Viswanadham |
| [YARN-8022](https://issues.apache.org/jira/browse/YARN-8022) | ResourceManager UI cluster/app/\<app-id\> page fails to render |  Blocker | webapp | Tarun Parimi | Tarun Parimi |
| [MAPREDUCE-7064](https://issues.apache.org/jira/browse/MAPREDUCE-7064) | Flaky test TestTaskAttempt#testReducerCustomResourceTypes |  Major | client, test | Peter Bacsko | Peter Bacsko |
| [HDFS-12723](https://issues.apache.org/jira/browse/HDFS-12723) | TestReadStripedFileWithMissingBlocks#testReadFileWithMissingBlocks failing consistently. |  Major | . | Rushabh S Shah | Ajay Kumar |
| [YARN-7636](https://issues.apache.org/jira/browse/YARN-7636) | Re-reservation count may overflow when cluster resource exhausted for a long time |  Major | capacityscheduler | Tao Yang | Tao Yang |
| [HDFS-12886](https://issues.apache.org/jira/browse/HDFS-12886) | Ignore minReplication for block recovery |  Major | hdfs, namenode | Lukas Majercak | Lukas Majercak |
| [HDFS-13296](https://issues.apache.org/jira/browse/HDFS-13296) | GenericTestUtils generates paths with drive letter in Windows and fail webhdfs related test cases |  Major | . | Xiao Liang | Xiao Liang |
| [HDFS-13268](https://issues.apache.org/jira/browse/HDFS-13268) | TestWebHdfsFileContextMainOperations fails on Windows |  Major | . | Íñigo Goiri | Xiao Liang |
| [YARN-8054](https://issues.apache.org/jira/browse/YARN-8054) | Improve robustness of the LocalDirsHandlerService MonitoringTimerTask thread |  Major | . | Jonathan Eagles | Jonathan Eagles |
| [HDFS-13195](https://issues.apache.org/jira/browse/HDFS-13195) | DataNode conf page  cannot display the current value after reconfig |  Minor | datanode | maobaolong | maobaolong |
| [YARN-8063](https://issues.apache.org/jira/browse/YARN-8063) | DistributedShellTimelinePlugin wrongly check for entityId instead of entityType |  Major | . | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-8062](https://issues.apache.org/jira/browse/YARN-8062) | yarn rmadmin -getGroups returns group from which the user has been removed |  Critical | . | Sumana Sathish | Sunil Govindan |
| [YARN-8068](https://issues.apache.org/jira/browse/YARN-8068) | Application Priority field causes NPE in app timeline publish when Hadoop 2.7 based clients to 2.8+ |  Blocker | yarn | Sunil Govindan | Sunil Govindan |
| [YARN-7734](https://issues.apache.org/jira/browse/YARN-7734) | YARN-5418 breaks TestContainerLogsPage.testContainerLogPageAccess |  Major | . | Miklos Szegedi | Tao Yang |
| [HDFS-13087](https://issues.apache.org/jira/browse/HDFS-13087) | Snapshotted encryption zone information should be immutable |  Major | encryption | LiXin Ge | LiXin Ge |
| [HADOOP-12862](https://issues.apache.org/jira/browse/HADOOP-12862) | LDAP Group Mapping over SSL can not specify trust store |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-13349](https://issues.apache.org/jira/browse/HDFS-13349) | Unresolved merge conflict in ViewFs.md |  Blocker | documentation | Gera Shegalov | Yiqun Lin |
| [HADOOP-15317](https://issues.apache.org/jira/browse/HADOOP-15317) | Improve NetworkTopology chooseRandom's loop |  Major | . | Xiao Chen | Xiao Chen |
| [HADOOP-15355](https://issues.apache.org/jira/browse/HADOOP-15355) | TestCommonConfigurationFields is broken by HADOOP-15312 |  Major | test | Konstantin Shvachko | LiXin Ge |
| [HDFS-13350](https://issues.apache.org/jira/browse/HDFS-13350) | Negative legacy block ID will confuse Erasure Coding to be considered as striped block |  Major | erasure-coding | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [YARN-7905](https://issues.apache.org/jira/browse/YARN-7905) | Parent directory permission incorrect during public localization |  Critical | . | Bibin A Chundatt | Bilwa S T |
| [HDFS-13420](https://issues.apache.org/jira/browse/HDFS-13420) | License header is displayed in ArchivalStorage/MemoryStorage html pages |  Minor | documentation | Akira Ajisaka | Akira Ajisaka |
| [HDFS-13328](https://issues.apache.org/jira/browse/HDFS-13328) | Abstract ReencryptionHandler recursive logic in separate class. |  Major | namenode | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HADOOP-15357](https://issues.apache.org/jira/browse/HADOOP-15357) | Configuration.getPropsWithPrefix no longer does variable substitution |  Major | . | Jim Brennan | Jim Brennan |
| [MAPREDUCE-7062](https://issues.apache.org/jira/browse/MAPREDUCE-7062) | Update mapreduce.job.tags description for making use for ATSv2 purpose. |  Major | . | Charan Hebri | Charan Hebri |
| [YARN-8073](https://issues.apache.org/jira/browse/YARN-8073) | TimelineClientImpl doesn't honor yarn.timeline-service.versions configuration |  Major | . | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-13427](https://issues.apache.org/jira/browse/HDFS-13427) | Fix the section titles of transparent encryption document |  Minor | documentation | Akira Ajisaka | Akira Ajisaka |
| [YARN-7527](https://issues.apache.org/jira/browse/YARN-7527) | Over-allocate node resource in async-scheduling mode of CapacityScheduler |  Major | capacityscheduler | Tao Yang | Tao Yang |
| [YARN-8120](https://issues.apache.org/jira/browse/YARN-8120) | JVM can crash with SIGSEGV when exiting due to custom leveldb logger |  Major | nodemanager, resourcemanager | Jason Lowe | Jason Lowe |
| [YARN-8147](https://issues.apache.org/jira/browse/YARN-8147) | TestClientRMService#testGetApplications sporadically fails |  Major | test | Jason Lowe | Jason Lowe |
| [HDFS-13436](https://issues.apache.org/jira/browse/HDFS-13436) | Fix javadoc of package-info.java |  Major | documentation | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-14970](https://issues.apache.org/jira/browse/HADOOP-14970) | MiniHadoopClusterManager doesn't respect lack of format option |  Minor | . | Erik Krogen | Erik Krogen |
| [HDFS-13330](https://issues.apache.org/jira/browse/HDFS-13330) | ShortCircuitCache#fetchOrCreate never retries |  Major | . | Wei-Chiu Chuang | Gabor Bota |
| [YARN-8156](https://issues.apache.org/jira/browse/YARN-8156) | Increase the default value of yarn.timeline-service.app-collector.linger-period.ms |  Major | . | Rohith Sharma K S | Charan Hebri |
| [YARN-8165](https://issues.apache.org/jira/browse/YARN-8165) | Incorrect queue name logging in AbstractContainerAllocator |  Trivial | capacityscheduler | Weiwei Yang | Weiwei Yang |
| [HDFS-12828](https://issues.apache.org/jira/browse/HDFS-12828) | OIV ReverseXML Processor fails with escaped characters |  Critical | hdfs | Erik Krogen | Erik Krogen |
| [HADOOP-15396](https://issues.apache.org/jira/browse/HADOOP-15396) | Some java source files are executable |  Minor | . | Akira Ajisaka | Shashikant Banerjee |
| [YARN-6827](https://issues.apache.org/jira/browse/YARN-6827) | [ATS1/1.5] NPE exception while publishing recovering applications into ATS during RM restart. |  Major | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-7786](https://issues.apache.org/jira/browse/YARN-7786) | NullPointerException while launching ApplicationMaster |  Major | . | lujie | lujie |
| [HDFS-10183](https://issues.apache.org/jira/browse/HDFS-10183) | Prevent race condition during class initialization |  Minor | fs | Pavel Avgustinov | Pavel Avgustinov |
| [HDFS-13388](https://issues.apache.org/jira/browse/HDFS-13388) | RequestHedgingProxyProvider calls multiple configured NNs all the time |  Major | hdfs-client | Jinglun | Jinglun |
| [HDFS-13433](https://issues.apache.org/jira/browse/HDFS-13433) | webhdfs requests can be routed incorrectly in federated cluster |  Critical | webhdfs | Arpit Agarwal | Arpit Agarwal |
| [HDFS-13408](https://issues.apache.org/jira/browse/HDFS-13408) | MiniDFSCluster to support being built on randomized base directory |  Major | test | Xiao Liang | Xiao Liang |
| [HADOOP-15390](https://issues.apache.org/jira/browse/HADOOP-15390) | Yarn RM logs flooded by DelegationTokenRenewer trying to renew KMS tokens |  Critical | . | Xiao Chen | Xiao Chen |
| [HDFS-13336](https://issues.apache.org/jira/browse/HDFS-13336) | Test cases of TestWriteToReplica failed in windows |  Major | . | Xiao Liang | Xiao Liang |
| [YARN-7598](https://issues.apache.org/jira/browse/YARN-7598) | Document how to use classpath isolation for aux-services in YARN |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-8183](https://issues.apache.org/jira/browse/YARN-8183) | Fix ConcurrentModificationException inside RMAppAttemptMetrics#convertAtomicLongMaptoLongMap |  Critical | yarn | Sumana Sathish | Suma Shivaprasad |
| [HADOOP-15411](https://issues.apache.org/jira/browse/HADOOP-15411) | AuthenticationFilter should use Configuration.getPropsWithPrefix instead of iterator |  Critical | . | Suma Shivaprasad | Suma Shivaprasad |
| [MAPREDUCE-7042](https://issues.apache.org/jira/browse/MAPREDUCE-7042) | Killed MR job data does not move to mapreduce.jobhistory.done-dir when ATS v2 is enabled |  Major | . | Yesha Vora | Xuan Gong |
| [YARN-8205](https://issues.apache.org/jira/browse/YARN-8205) | Application State is not updated to ATS if AM launching is delayed. |  Critical | . | Sumana Sathish | Rohith Sharma K S |
| [YARN-8004](https://issues.apache.org/jira/browse/YARN-8004) | Add unit tests for inter queue preemption for dominant resource calculator |  Critical | yarn | Sumana Sathish | Zian Chen |
| [YARN-8221](https://issues.apache.org/jira/browse/YARN-8221) | RMWebServices also need to honor yarn.resourcemanager.display.per-user-apps |  Major | webapp | Sunil Govindan | Sunil Govindan |
| [YARN-8210](https://issues.apache.org/jira/browse/YARN-8210) | AMRMClient logging on every heartbeat to track updation of AM RM token causes too many log lines to be generated in AM logs |  Major | yarn | Suma Shivaprasad | Suma Shivaprasad |
| [HDFS-13509](https://issues.apache.org/jira/browse/HDFS-13509) | Bug fix for breakHardlinks() of ReplicaInfo/LocalReplica, and fix TestFileAppend failures on Windows |  Major | . | Xiao Liang | Xiao Liang |
| [MAPREDUCE-7073](https://issues.apache.org/jira/browse/MAPREDUCE-7073) | Optimize TokenCache#obtainTokensForNamenodesInternal |  Major | . | Bibin A Chundatt | Bibin A Chundatt |
| [HADOOP-15406](https://issues.apache.org/jira/browse/HADOOP-15406) | hadoop-nfs dependencies for mockito and junit are not test scope |  Major | nfs | Jason Lowe | Jason Lowe |
| [YARN-6385](https://issues.apache.org/jira/browse/YARN-6385) | Fix checkstyle warnings in TestFileSystemApplicationHistoryStore |  Minor | . | Yiqun Lin | Yiqun Lin |
| [YARN-8222](https://issues.apache.org/jira/browse/YARN-8222) | Fix potential NPE when gets RMApp from RM context |  Critical | . | Tao Yang | Tao Yang |
| [HDFS-13481](https://issues.apache.org/jira/browse/HDFS-13481) | TestRollingFileSystemSinkWithHdfs#testFlushThread: test failed intermittently |  Major | hdfs | Gabor Bota | Gabor Bota |
| [YARN-8217](https://issues.apache.org/jira/browse/YARN-8217) | RmAuthenticationFilterInitializer /TimelineAuthenticationFilterInitializer should use Configuration.getPropsWithPrefix instead of iterator |  Major | . | Suma Shivaprasad | Suma Shivaprasad |
| [YARN-8025](https://issues.apache.org/jira/browse/YARN-8025) | UsersManangers#getComputedResourceLimitForActiveUsers throws NPE due to preComputedActiveUserLimit is empty |  Major | yarn | Jiandan Yang | Tao Yang |
| [YARN-8232](https://issues.apache.org/jira/browse/YARN-8232) | RMContainer lost queue name when RM HA happens |  Major | resourcemanager | Hu Ziqian | Hu Ziqian |
| [HDFS-13136](https://issues.apache.org/jira/browse/HDFS-13136) | Avoid taking FSN lock while doing group member lookup for FSD permission check |  Major | namenode | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-13537](https://issues.apache.org/jira/browse/HDFS-13537) | TestHdfsHelper does not generate jceks path properly for relative path in Windows |  Major | . | Xiao Liang | Xiao Liang |
| [YARN-7003](https://issues.apache.org/jira/browse/YARN-7003) | DRAINING state of queues is not recovered after RM restart |  Major | capacityscheduler | Tao Yang | Tao Yang |
| [YARN-8244](https://issues.apache.org/jira/browse/YARN-8244) |  TestContainerSchedulerQueuing.testStartMultipleContainers failed |  Major | . | Miklos Szegedi | Jim Brennan |
| [YARN-8288](https://issues.apache.org/jira/browse/YARN-8288) | Fix wrong number of table columns in Resource Model doc |  Major | . | Weiwei Yang | Weiwei Yang |
| [HDFS-13539](https://issues.apache.org/jira/browse/HDFS-13539) | DFSStripedInputStream NPE when reportCheckSumFailure |  Major | . | Xiao Chen | Xiao Chen |
| [YARN-8278](https://issues.apache.org/jira/browse/YARN-8278) | DistributedScheduling is not working in HA |  Blocker | . | Bibin A Chundatt | Bibin A Chundatt |
| [HDFS-13581](https://issues.apache.org/jira/browse/HDFS-13581) | DN UI logs link is broken when https is enabled |  Minor | datanode | Namit Maheshwari | Shashikant Banerjee |
| [HDFS-13586](https://issues.apache.org/jira/browse/HDFS-13586) | Fsync fails on directories on Windows |  Critical | datanode, hdfs | Lukas Majercak | Lukas Majercak |
| [YARN-8179](https://issues.apache.org/jira/browse/YARN-8179) | Preemption does not happen due to natural\_termination\_factor when DRF is used |  Major | . | kyungwan nam | kyungwan nam |
| [HADOOP-15450](https://issues.apache.org/jira/browse/HADOOP-15450) | Avoid fsync storm triggered by DiskChecker and handle disk full situation |  Blocker | . | Kihwal Lee | Arpit Agarwal |
| [HDFS-13601](https://issues.apache.org/jira/browse/HDFS-13601) | Optimize ByteString conversions in PBHelper |  Major | . | Andrew Wang | Andrew Wang |
| [HDFS-13540](https://issues.apache.org/jira/browse/HDFS-13540) | DFSStripedInputStream should only allocate new buffers when reading |  Major | . | Xiao Chen | Xiao Chen |
| [HDFS-13588](https://issues.apache.org/jira/browse/HDFS-13588) | Fix TestFsDatasetImpl test failures on Windows |  Major | . | Xiao Liang | Xiao Liang |
| [YARN-8310](https://issues.apache.org/jira/browse/YARN-8310) | Handle old NMTokenIdentifier, AMRMTokenIdentifier, and ContainerTokenIdentifier formats |  Major | . | Robert Kanter | Robert Kanter |
| [YARN-8344](https://issues.apache.org/jira/browse/YARN-8344) | Missing nm.stop() in TestNodeManagerResync to fix testKillContainersOnResync |  Major | . | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [YARN-8327](https://issues.apache.org/jira/browse/YARN-8327) | Fix TestAggregatedLogFormat#testReadAcontainerLogs1 on Windows |  Major | log-aggregation | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [YARN-8346](https://issues.apache.org/jira/browse/YARN-8346) | Upgrading to 3.1 kills running containers with error "Opportunistic container queue is full" |  Blocker | . | Rohith Sharma K S | Jason Lowe |
| [HDFS-13611](https://issues.apache.org/jira/browse/HDFS-13611) | Unsafe use of Text as a ConcurrentHashMap key in PBHelperClient |  Major | . | Andrew Wang | Andrew Wang |
| [HDFS-13618](https://issues.apache.org/jira/browse/HDFS-13618) | Fix TestDataNodeFaultInjector test failures on Windows |  Major | test | Xiao Liang | Xiao Liang |
| [HADOOP-15473](https://issues.apache.org/jira/browse/HADOOP-15473) | Configure serialFilter in KeyProvider to avoid UnrecoverableKeyException caused by JDK-8189997 |  Critical | kms | Gabor Bota | Gabor Bota |
| [YARN-8338](https://issues.apache.org/jira/browse/YARN-8338) | TimelineService V1.5 doesn't come up after HADOOP-15406 |  Critical | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-15313](https://issues.apache.org/jira/browse/HADOOP-15313) | TestKMS should close providers |  Major | kms, test | Xiao Chen | Xiao Chen |
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
| [HDFS-13619](https://issues.apache.org/jira/browse/HDFS-13619) | TestAuditLoggerWithCommands fails on Windows |  Minor | test | Anbang Hu | Anbang Hu |
| [HDFS-13620](https://issues.apache.org/jira/browse/HDFS-13620) | Randomize the test directory path for TestHDFSFileSystemContract |  Minor | . | Anbang Hu | Anbang Hu |
| [HDFS-13591](https://issues.apache.org/jira/browse/HDFS-13591) | TestDFSShell#testSetrepLow fails on Windows |  Minor | . | Anbang Hu | Anbang Hu |
| [HDFS-13632](https://issues.apache.org/jira/browse/HDFS-13632) | Randomize baseDir for MiniJournalCluster in MiniQJMHACluster for TestDFSAdminWithHA |  Minor | . | Anbang Hu | Anbang Hu |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-13068](https://issues.apache.org/jira/browse/HDFS-13068) | RBF: Add router admin option to manage safe mode |  Major | . | Íñigo Goiri | Yiqun Lin |
| [HADOOP-15040](https://issues.apache.org/jira/browse/HADOOP-15040) | Upgrade AWS SDK to 1.11.271: NPE bug spams logs w/ Yarn Log Aggregation |  Blocker | fs/s3 | Aaron Fabbri | Aaron Fabbri |
| [HDFS-13119](https://issues.apache.org/jira/browse/HDFS-13119) | RBF: Manage unavailable clusters |  Major | . | Íñigo Goiri | Yiqun Lin |
| [HADOOP-15247](https://issues.apache.org/jira/browse/HADOOP-15247) | Move commons-net up to 3.6 |  Minor | fs | Steve Loughran | Steve Loughran |
| [HDFS-13187](https://issues.apache.org/jira/browse/HDFS-13187) | RBF: Fix Routers information shown in the web UI |  Minor | . | Wei Yan | Wei Yan |
| [HDFS-13184](https://issues.apache.org/jira/browse/HDFS-13184) | RBF: Improve the unit test TestRouterRPCClientRetries |  Minor | test | Yiqun Lin | Yiqun Lin |
| [HDFS-13199](https://issues.apache.org/jira/browse/HDFS-13199) | RBF: Fix the hdfs router page missing label icon issue |  Major | federation, hdfs | maobaolong | maobaolong |
| [HADOOP-15264](https://issues.apache.org/jira/browse/HADOOP-15264) | AWS "shaded" SDK 1.11.271 is pulling in netty 4.1.17 |  Blocker | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-15090](https://issues.apache.org/jira/browse/HADOOP-15090) | Add ADL troubleshooting doc |  Major | documentation, fs/adl | Steve Loughran | Steve Loughran |
| [HDFS-13214](https://issues.apache.org/jira/browse/HDFS-13214) | RBF: Complete document of Router configuration |  Major | . | Tao Jie | Yiqun Lin |
| [HADOOP-15267](https://issues.apache.org/jira/browse/HADOOP-15267) | S3A multipart upload fails when SSE-C encryption is enabled |  Critical | fs/s3 | Anis Elleuch | Anis Elleuch |
| [HDFS-13230](https://issues.apache.org/jira/browse/HDFS-13230) | RBF: ConnectionManager's cleanup task will compare each pool's own active conns with its total conns |  Minor | . | Wei Yan | Chao Sun |
| [HDFS-13233](https://issues.apache.org/jira/browse/HDFS-13233) | RBF: MountTableResolver doesn't return the correct mount point of the given path |  Major | hdfs | wangzhiyuan | wangzhiyuan |
| [HADOOP-15277](https://issues.apache.org/jira/browse/HADOOP-15277) | remove .FluentPropertyBeanIntrospector from CLI operation log output |  Minor | conf | Steve Loughran | Steve Loughran |
| [HDFS-13212](https://issues.apache.org/jira/browse/HDFS-13212) | RBF: Fix router location cache issue |  Major | federation, hdfs | Weiwei Wu | Weiwei Wu |
| [HDFS-13232](https://issues.apache.org/jira/browse/HDFS-13232) | RBF: ConnectionPool should return first usable connection |  Minor | . | Wei Yan | Ekanth S |
| [HDFS-13240](https://issues.apache.org/jira/browse/HDFS-13240) | RBF: Update some inaccurate document descriptions |  Minor | . | Yiqun Lin | Yiqun Lin |
| [HDFS-11399](https://issues.apache.org/jira/browse/HDFS-11399) | Many tests fails in Windows due to injecting disk failures |  Major | . | Yiqun Lin | Yiqun Lin |
| [HDFS-13241](https://issues.apache.org/jira/browse/HDFS-13241) | RBF: TestRouterSafemode failed if the port 8888 is in use |  Major | hdfs, test | maobaolong | maobaolong |
| [HDFS-13253](https://issues.apache.org/jira/browse/HDFS-13253) | RBF: Quota management incorrect parent-child relationship judgement |  Major | . | Yiqun Lin | Yiqun Lin |
| [HDFS-13226](https://issues.apache.org/jira/browse/HDFS-13226) | RBF: Throw the exception if mount table entry validated failed |  Major | hdfs | maobaolong | maobaolong |
| [HDFS-12505](https://issues.apache.org/jira/browse/HDFS-12505) | Extend TestFileStatusWithECPolicy with a random EC policy |  Major | erasure-coding, test | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-12587](https://issues.apache.org/jira/browse/HDFS-12587) | Use Parameterized tests in TestBlockInfoStriped and TestLowRedundancyBlockQueues to test all EC policies |  Major | erasure-coding, test | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-15308](https://issues.apache.org/jira/browse/HADOOP-15308) | TestConfiguration fails on Windows because of paths |  Major | test | Íñigo Goiri | Xiao Liang |
| [HDFS-12773](https://issues.apache.org/jira/browse/HDFS-12773) | RBF: Improve State Store FS implementation |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13198](https://issues.apache.org/jira/browse/HDFS-13198) | RBF: RouterHeartbeatService throws out CachedStateStore related exceptions when starting router |  Minor | . | Wei Yan | Wei Yan |
| [HDFS-13224](https://issues.apache.org/jira/browse/HDFS-13224) | RBF: Resolvers to support mount points across multiple subclusters |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HADOOP-15262](https://issues.apache.org/jira/browse/HADOOP-15262) | AliyunOSS: move files under a directory in parallel when rename a directory |  Major | fs/oss | wujinhu | wujinhu |
| [HDFS-13215](https://issues.apache.org/jira/browse/HDFS-13215) | RBF: Move Router to its own module |  Major | . | Íñigo Goiri | Wei Yan |
| [HDFS-13250](https://issues.apache.org/jira/browse/HDFS-13250) | RBF: Router to manage requests across multiple subclusters |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13318](https://issues.apache.org/jira/browse/HDFS-13318) | RBF: Fix FindBugs in hadoop-hdfs-rbf |  Minor | . | Íñigo Goiri | Ekanth S |
| [HDFS-12792](https://issues.apache.org/jira/browse/HDFS-12792) | RBF: Test Router-based federation using HDFSContract |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-7986](https://issues.apache.org/jira/browse/YARN-7986) | ATSv2 REST API queries do not return results for uppercase application tags |  Critical | . | Charan Hebri | Charan Hebri |
| [HDFS-12512](https://issues.apache.org/jira/browse/HDFS-12512) | RBF: Add WebHDFS |  Major | fs | Íñigo Goiri | Wei Yan |
| [HDFS-13291](https://issues.apache.org/jira/browse/HDFS-13291) | RBF: Implement available space based OrderResolver |  Major | . | Yiqun Lin | Yiqun Lin |
| [HDFS-13204](https://issues.apache.org/jira/browse/HDFS-13204) | RBF: Optimize name service safe mode icon |  Minor | . | liuhongtong | liuhongtong |
| [HDFS-13352](https://issues.apache.org/jira/browse/HDFS-13352) | RBF: Add xsl stylesheet for hdfs-rbf-default.xml |  Major | documentation | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-13347](https://issues.apache.org/jira/browse/HDFS-13347) | RBF: Cache datanode reports |  Minor | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13289](https://issues.apache.org/jira/browse/HDFS-13289) | RBF: TestConnectionManager#testCleanup() test case need correction |  Minor | . | Dibyendu Karmakar | Dibyendu Karmakar |
| [HDFS-13364](https://issues.apache.org/jira/browse/HDFS-13364) | RBF: Support NamenodeProtocol in the Router |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HADOOP-14651](https://issues.apache.org/jira/browse/HADOOP-14651) | Update okhttp version to 2.7.5 |  Major | fs/adl | Ray Chiang | Ray Chiang |
| [YARN-6936](https://issues.apache.org/jira/browse/YARN-6936) | [Atsv2] Retrospect storing entities into sub application table from client perspective |  Major | . | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-13353](https://issues.apache.org/jira/browse/HDFS-13353) | RBF: TestRouterWebHDFSContractCreate failed |  Major | test | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-8107](https://issues.apache.org/jira/browse/YARN-8107) | Give an informative message when incorrect format is used in ATSv2 filter attributes |  Major | ATSv2 | Charan Hebri | Rohith Sharma K S |
| [HDFS-13402](https://issues.apache.org/jira/browse/HDFS-13402) | RBF: Fix  java doc for StateStoreFileSystemImpl |  Minor | hdfs | Yiran Wu | Yiran Wu |
| [HDFS-13410](https://issues.apache.org/jira/browse/HDFS-13410) | RBF: Support federation with no subclusters |  Minor | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13384](https://issues.apache.org/jira/browse/HDFS-13384) | RBF: Improve timeout RPC call mechanism |  Minor | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13045](https://issues.apache.org/jira/browse/HDFS-13045) | RBF: Improve error message returned from subcluster |  Minor | . | Wei Yan | Íñigo Goiri |
| [HDFS-13428](https://issues.apache.org/jira/browse/HDFS-13428) | RBF: Remove LinkedList From StateStoreFileImpl.java |  Trivial | federation | BELUGA BEHR | BELUGA BEHR |
| [HDFS-13386](https://issues.apache.org/jira/browse/HDFS-13386) | RBF: Wrong date information in list file(-ls) result |  Minor | . | Dibyendu Karmakar | Dibyendu Karmakar |
| [YARN-8027](https://issues.apache.org/jira/browse/YARN-8027) | Setting hostname of docker container breaks for --net=host in docker 1.13 |  Major | yarn | Jim Brennan | Jim Brennan |
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
| [HDFS-13508](https://issues.apache.org/jira/browse/HDFS-13508) | RBF: Normalize paths (automatically) when adding, updating, removing or listing mount table entries |  Minor | . | Ekanth S | Ekanth S |
| [HDFS-13434](https://issues.apache.org/jira/browse/HDFS-13434) | RBF: Fix dead links in RBF document |  Major | documentation | Akira Ajisaka | Chetna Chaudhari |
| [YARN-8212](https://issues.apache.org/jira/browse/YARN-8212) | Pending backlog for async allocation threads should be configurable |  Major | . | Weiwei Yang | Tao Yang |
| [HDFS-13488](https://issues.apache.org/jira/browse/HDFS-13488) | RBF: Reject requests when a Router is overloaded |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13525](https://issues.apache.org/jira/browse/HDFS-13525) | RBF: Add unit test TestStateStoreDisabledNameservice |  Major | . | Yiqun Lin | Yiqun Lin |
| [YARN-8253](https://issues.apache.org/jira/browse/YARN-8253) | HTTPS Ats v2 api call fails with "bad HTTP parsed" |  Critical | ATSv2 | Yesha Vora | Charan Hebri |
| [HADOOP-15454](https://issues.apache.org/jira/browse/HADOOP-15454) | TestRollingFileSystemSinkWithLocal fails on Windows |  Major | test | Xiao Liang | Xiao Liang |
| [YARN-8247](https://issues.apache.org/jira/browse/YARN-8247) | Incorrect HTTP status code returned by ATSv2 for non-whitelisted users |  Critical | ATSv2 | Charan Hebri | Rohith Sharma K S |
| [YARN-8130](https://issues.apache.org/jira/browse/YARN-8130) | Race condition when container events are published for KILLED applications |  Major | ATSv2 | Charan Hebri | Rohith Sharma K S |
| [HADOOP-15498](https://issues.apache.org/jira/browse/HADOOP-15498) | TestHadoopArchiveLogs (#testGenerateScript, #testPrepareWorkingDir) fails on Windows |  Minor | . | Anbang Hu | Anbang Hu |
| [HADOOP-15497](https://issues.apache.org/jira/browse/HADOOP-15497) | TestTrash should use proper test path to avoid failing on Windows |  Minor | . | Anbang Hu | Anbang Hu |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-13052](https://issues.apache.org/jira/browse/HDFS-13052) | WebHDFS: Add support for snasphot diff |  Major | . | Lokesh Jain | Lokesh Jain |
| [HADOOP-14742](https://issues.apache.org/jira/browse/HADOOP-14742) | Document multi-URI replication Inode for ViewFS |  Major | documentation, viewfs | Chris Douglas | Gera Shegalov |


