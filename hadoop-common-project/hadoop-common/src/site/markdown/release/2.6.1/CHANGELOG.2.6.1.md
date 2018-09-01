
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

## Release 2.6.1 - 2015-09-23



### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-7278](https://issues.apache.org/jira/browse/HDFS-7278) | Add a command that allows sysadmins to manually trigger full block reports from a DN |  Major | datanode | Colin P. McCabe | Colin P. McCabe |
| [YARN-2301](https://issues.apache.org/jira/browse/YARN-2301) | Improve yarn container command |  Major | . | Jian He | Naganarasimha G R |
| [HDFS-7531](https://issues.apache.org/jira/browse/HDFS-7531) | Improve the concurrent access on FsVolumeList |  Major | datanode | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HDFS-7579](https://issues.apache.org/jira/browse/HDFS-7579) | Improve log reporting during block report rpc failure |  Minor | datanode | Charles Lamb | Charles Lamb |
| [HDFS-7446](https://issues.apache.org/jira/browse/HDFS-7446) | HDFS inotify should have the ability to determine what txid it has read up to |  Major | hdfs-client | Colin P. McCabe | Colin P. McCabe |
| [HDFS-7182](https://issues.apache.org/jira/browse/HDFS-7182) | JMX metrics aren't accessible when NN is busy |  Major | . | Ming Ma | Ming Ma |
| [HADOOP-11466](https://issues.apache.org/jira/browse/HADOOP-11466) | FastByteComparisons: do not use UNSAFE\_COMPARER on the SPARC architecture because it is slower there |  Minor | io, performance, util | Suman Somasundar | Suman Somasundar |
| [HADOOP-11506](https://issues.apache.org/jira/browse/HADOOP-11506) | Configuration variable expansion regex expensive for long values |  Major | conf | Dmitriy V. Ryaboy | Gera Shegalov |
| [YARN-3230](https://issues.apache.org/jira/browse/YARN-3230) | Clarify application states on the web UI |  Major | . | Jian He | Jian He |
| [MAPREDUCE-6267](https://issues.apache.org/jira/browse/MAPREDUCE-6267) | Refactor JobSubmitter#copyAndConfigureFiles into it's own class |  Minor | . | Chris Trezzo | Chris Trezzo |
| [YARN-3249](https://issues.apache.org/jira/browse/YARN-3249) | Add a "kill application" button to Resource Manager's Web UI |  Minor | resourcemanager | Ryu Kobayashi | Ryu Kobayashi |
| [YARN-3248](https://issues.apache.org/jira/browse/YARN-3248) | Display count of nodes blacklisted by apps in the web UI |  Major | capacityscheduler, resourcemanager | Varun Vasudev | Varun Vasudev |
| [HADOOP-11812](https://issues.apache.org/jira/browse/HADOOP-11812) | Implement listLocatedStatus for ViewFileSystem to speed up split calculation |  Blocker | fs | Gera Shegalov | Gera Shegalov |
| [HADOOP-7139](https://issues.apache.org/jira/browse/HADOOP-7139) | Allow appending to existing SequenceFiles |  Major | io | Stephen Rose | Kanaka Kumar Avvaru |
| [HDFS-7314](https://issues.apache.org/jira/browse/HDFS-7314) | When the DFSClient lease cannot be renewed, abort open-for-write files rather than the entire DFSClient |  Major | . | Ming Ma | Ming Ma |
| [YARN-3978](https://issues.apache.org/jira/browse/YARN-3978) | Configurably turn off the saving of container info in Generic AHS |  Major | timelineserver, yarn | Eric Payne | Eric Payne |
| [HADOOP-12280](https://issues.apache.org/jira/browse/HADOOP-12280) | Skip unit tests based on maven profile rather than NativeCodeLoader.isNativeCodeLoaded |  Minor | test | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-8384](https://issues.apache.org/jira/browse/HDFS-8384) | Allow NN to startup if there are files having a lease but are not under construction |  Minor | namenode | Tsz Wo Nicholas Sze | Jing Zhao |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-7213](https://issues.apache.org/jira/browse/HDFS-7213) | processIncrementalBlockReport performance degradation |  Critical | namenode | Daryn Sharp | Eric Payne |
| [HDFS-7235](https://issues.apache.org/jira/browse/HDFS-7235) | DataNode#transferBlock should report blocks that don't exist using reportBadBlock |  Major | datanode, namenode | Yongjun Zhang | Yongjun Zhang |
| [HDFS-7263](https://issues.apache.org/jira/browse/HDFS-7263) | Snapshot read can reveal future bytes for appended files. |  Major | hdfs-client | Konstantin Shvachko | Tao Luo |
| [HADOOP-10786](https://issues.apache.org/jira/browse/HADOOP-10786) | Fix UGI#reloginFromKeytab on Java 8 |  Major | security | Tobi Vollebregt | Stephen Chu |
| [YARN-2856](https://issues.apache.org/jira/browse/YARN-2856) | Application recovery throw InvalidStateTransitonException: Invalid event: ATTEMPT\_KILLED at ACCEPTED |  Critical | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-2816](https://issues.apache.org/jira/browse/YARN-2816) | NM fail to start with NPE during container recovery |  Major | nodemanager | zhihai xu | zhihai xu |
| [YARN-2414](https://issues.apache.org/jira/browse/YARN-2414) | RM web UI: app page will crash if app is failed before any attempt has been created |  Major | webapp | Zhijie Shen | Wangda Tan |
| [HDFS-7225](https://issues.apache.org/jira/browse/HDFS-7225) | Remove stale block invalidation work when DN re-registers with different UUID |  Major | namenode | Zhe Zhang | Zhe Zhang |
| [YARN-2865](https://issues.apache.org/jira/browse/YARN-2865) | Application recovery continuously fails with "Application with id already present. Cannot duplicate" |  Critical | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-7425](https://issues.apache.org/jira/browse/HDFS-7425) | NameNode block deletion logging uses incorrect appender. |  Minor | namenode | Chris Nauroth | Chris Nauroth |
| [HDFS-4882](https://issues.apache.org/jira/browse/HDFS-4882) | Prevent the Namenode's LeaseManager from looping forever in checkLeases |  Critical | hdfs-client, namenode | Zesheng Wu | Ravi Prakash |
| [YARN-2906](https://issues.apache.org/jira/browse/YARN-2906) | CapacitySchedulerPage shows HTML tags for a queue's Active Users |  Major | capacityscheduler | Jason Lowe | Jason Lowe |
| [HADOOP-11333](https://issues.apache.org/jira/browse/HADOOP-11333) | Fix deadlock in DomainSocketWatcher when the notification pipe is full |  Major | . | yunjiong zhao | yunjiong zhao |
| [YARN-2905](https://issues.apache.org/jira/browse/YARN-2905) | AggregatedLogsBlock page can infinitely loop if the aggregated log file is corrupted |  Blocker | . | Jason Lowe | Varun Saxena |
| [YARN-2894](https://issues.apache.org/jira/browse/YARN-2894) | When ACL's are enabled, if RM switches then application can not be viewed from web. |  Major | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-2874](https://issues.apache.org/jira/browse/YARN-2874) | Dead lock in "DelegationTokenRenewer" which blocks RM to execute any further apps |  Blocker | resourcemanager | Naganarasimha G R | Naganarasimha G R |
| [HADOOP-11343](https://issues.apache.org/jira/browse/HADOOP-11343) | Overflow is not properly handled in caclulating final iv for AES CTR |  Blocker | security | Haifeng Chen | Haifeng Chen |
| [HADOOP-11368](https://issues.apache.org/jira/browse/HADOOP-11368) | Fix SSLFactory truststore reloader thread leak in KMSClientProvider |  Major | kms | Arun Suresh | Arun Suresh |
| [HDFS-7489](https://issues.apache.org/jira/browse/HDFS-7489) | Incorrect locking in FsVolumeList#checkDirs can hang datanodes |  Critical | datanode | Noah Lorang | Noah Lorang |
| [YARN-2910](https://issues.apache.org/jira/browse/YARN-2910) | FSLeafQueue can throw ConcurrentModificationException |  Major | fairscheduler | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [HDFS-7503](https://issues.apache.org/jira/browse/HDFS-7503) | Namenode restart after large deletions can cause slow processReport (due to logging) |  Major | namenode | Arpit Agarwal | Arpit Agarwal |
| [YARN-2917](https://issues.apache.org/jira/browse/YARN-2917) | Potential deadlock in AsyncDispatcher when system.exit called in AsyncDispatcher#dispatch and AsyscDispatcher#serviceStop from shutdown hook |  Critical | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [HADOOP-11238](https://issues.apache.org/jira/browse/HADOOP-11238) | Update the NameNode's Group Cache in the background when possible |  Minor | . | Chris Li | Chris Li |
| [MAPREDUCE-6166](https://issues.apache.org/jira/browse/MAPREDUCE-6166) | Reducers do not validate checksum of map outputs when fetching directly to disk |  Major | mrv2 | Eric Payne | Eric Payne |
| [YARN-2964](https://issues.apache.org/jira/browse/YARN-2964) | RM prematurely cancels tokens for jobs that submit jobs (oozie) |  Blocker | resourcemanager | Daryn Sharp | Jian He |
| [HDFS-7552](https://issues.apache.org/jira/browse/HDFS-7552) | change FsVolumeList toString() to fix TestDataNodeVolumeFailureToleration |  Major | datanode, test | Liang Xie | Liang Xie |
| [HDFS-7443](https://issues.apache.org/jira/browse/HDFS-7443) | Datanode upgrade to BLOCKID\_BASED\_LAYOUT fails if duplicate block files are present in the same volume |  Blocker | . | Kihwal Lee | Colin P. McCabe |
| [YARN-2952](https://issues.apache.org/jira/browse/YARN-2952) | Incorrect version check in RMStateStore |  Major | . | Jian He | Rohith Sharma K S |
| [YARN-2340](https://issues.apache.org/jira/browse/YARN-2340) | NPE thrown when RM restart after queue is STOPPED. There after RM can not recovery application's and remain in standby |  Critical | resourcemanager, scheduler | Nishan Shetty | Rohith Sharma K S |
| [YARN-2992](https://issues.apache.org/jira/browse/YARN-2992) | ZKRMStateStore crashes due to session expiry |  Blocker | resourcemanager | Karthik Kambatla | Karthik Kambatla |
| [YARN-2922](https://issues.apache.org/jira/browse/YARN-2922) | ConcurrentModificationException in CapacityScheduler's LeafQueue |  Major | capacityscheduler, resourcemanager, scheduler | Jason Tufo | Rohith Sharma K S |
| [YARN-2978](https://issues.apache.org/jira/browse/YARN-2978) | ResourceManager crashes with NPE while getting queue info |  Critical | . | Jason Tufo | Varun Saxena |
| [YARN-2997](https://issues.apache.org/jira/browse/YARN-2997) | NM keeps sending already-sent completed containers to RM until containers are removed from context |  Major | nodemanager | Chengbing Liu | Chengbing Liu |
| [HDFS-7596](https://issues.apache.org/jira/browse/HDFS-7596) | NameNode should prune dead storages from storageMap |  Major | namenode | Arpit Agarwal | Arpit Agarwal |
| [HDFS-7533](https://issues.apache.org/jira/browse/HDFS-7533) | Datanode sometimes does not shutdown on receiving upgrade shutdown command |  Major | . | Kihwal Lee | Eric Payne |
| [HDFS-7470](https://issues.apache.org/jira/browse/HDFS-7470) | SecondaryNameNode need twice memory when calling reloadFromImageFile |  Major | namenode | yunjiong zhao | yunjiong zhao |
| [YARN-2637](https://issues.apache.org/jira/browse/YARN-2637) | maximum-am-resource-percent could be respected for both LeafQueue/User when trying to activate applications. |  Critical | resourcemanager | Wangda Tan | Craig Welch |
| [HADOOP-11350](https://issues.apache.org/jira/browse/HADOOP-11350) | The size of header buffer of HttpServer is too small when HTTPS is enabled |  Major | security | Benoy Antony | Benoy Antony |
| [HDFS-7610](https://issues.apache.org/jira/browse/HDFS-7610) | Fix removal of dynamically added DN volumes |  Major | datanode | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HDFS-3443](https://issues.apache.org/jira/browse/HDFS-3443) | Fix NPE when namenode transition to active during startup by adding checkNNStartup() in NameNodeRpcServer |  Major | auto-failover, ha | suja s | Vinayakumar B |
| [HDFS-7575](https://issues.apache.org/jira/browse/HDFS-7575) | Upgrade should generate a unique storage ID for each volume |  Critical | . | Lars Francke | Arpit Agarwal |
| [HADOOP-11482](https://issues.apache.org/jira/browse/HADOOP-11482) | Use correct UGI when KMSClientProvider is called by a proxy user |  Major | . | Arun Suresh | Arun Suresh |
| [YARN-3024](https://issues.apache.org/jira/browse/YARN-3024) | LocalizerRunner should give DIE action when all resources are localized |  Major | nodemanager | Chengbing Liu | Chengbing Liu |
| [HADOOP-11316](https://issues.apache.org/jira/browse/HADOOP-11316) | "mvn package -Pdist,docs -DskipTests -Dtar" fails because of non-ascii characters |  Blocker | . | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [YARN-3103](https://issues.apache.org/jira/browse/YARN-3103) | AMRMClientImpl does not update AMRM token properly |  Blocker | client | Jason Lowe | Jason Lowe |
| [MAPREDUCE-6230](https://issues.apache.org/jira/browse/MAPREDUCE-6230) | MR AM does not survive RM restart if RM activated a new AMRM secret key |  Blocker | mr-am | Jason Lowe | Jason Lowe |
| [HDFS-7707](https://issues.apache.org/jira/browse/HDFS-7707) | Edit log corruption due to delayed block removal again |  Major | namenode | Yongjun Zhang | Yongjun Zhang |
| [HDFS-7733](https://issues.apache.org/jira/browse/HDFS-7733) | NFS: readdir/readdirplus return null directory attribute on failure |  Major | nfs | Arpit Agarwal | Arpit Agarwal |
| [MAPREDUCE-6237](https://issues.apache.org/jira/browse/MAPREDUCE-6237) | Multiple mappers with DBInputFormat don't work because of reusing conections |  Major | mrv2 | Kannan Rajah | Kannan Rajah |
| [YARN-3094](https://issues.apache.org/jira/browse/YARN-3094) | reset timer for liveness monitors after RM recovery |  Major | resourcemanager | Jun Gong | Jun Gong |
| [HDFS-7714](https://issues.apache.org/jira/browse/HDFS-7714) | Simultaneous restart of HA NameNodes and DataNode can cause DataNode to register successfully with only one NameNode. |  Major | datanode | Chris Nauroth | Vinayakumar B |
| [YARN-2246](https://issues.apache.org/jira/browse/YARN-2246) | Job History Link in RM UI is redirecting to the URL which contains Job Id twice |  Major | webapp | Devaraj K | Devaraj K |
| [HADOOP-11295](https://issues.apache.org/jira/browse/HADOOP-11295) | RPC Server Reader thread can't shutdown if RPCCallQueue is full |  Major | . | Ming Ma | Ming Ma |
| [HDFS-7788](https://issues.apache.org/jira/browse/HDFS-7788) | Post-2.6 namenode may not start up with an image containing inodes created with an old release. |  Blocker | . | Kihwal Lee | Rushabh S Shah |
| [HADOOP-11604](https://issues.apache.org/jira/browse/HADOOP-11604) | Prevent ConcurrentModificationException while closing domain sockets during shutdown of DomainSocketWatcher thread. |  Critical | net | Liang Xie | Chris Nauroth |
| [YARN-3238](https://issues.apache.org/jira/browse/YARN-3238) | Connection timeouts to nodemanagers are retried at multiple levels |  Blocker | . | Jason Lowe | Jason Lowe |
| [YARN-3207](https://issues.apache.org/jira/browse/YARN-3207) | secondary filter matches entites which do not have the key being filtered for. |  Major | timelineserver | Prakash Ramachandran | Zhijie Shen |
| [HDFS-7009](https://issues.apache.org/jira/browse/HDFS-7009) | Active NN and standby NN have different live nodes |  Major | datanode | Ming Ma | Ming Ma |
| [HDFS-7763](https://issues.apache.org/jira/browse/HDFS-7763) | fix zkfc hung issue due to not catching exception in a corner case |  Major | ha | Liang Xie | Liang Xie |
| [YARN-3239](https://issues.apache.org/jira/browse/YARN-3239) | WebAppProxy does not support a final tracking url which has query fragments and params |  Major | . | Hitesh Shah | Jian He |
| [YARN-3251](https://issues.apache.org/jira/browse/YARN-3251) | Fix CapacityScheduler deadlock when computing absolute max avail capacity (short term fix for 2.6.1) |  Blocker | . | Jason Lowe | Craig Welch |
| [HDFS-7871](https://issues.apache.org/jira/browse/HDFS-7871) | NameNodeEditLogRoller can keep printing "Swallowing exception" message |  Critical | . | Jing Zhao | Jing Zhao |
| [YARN-3222](https://issues.apache.org/jira/browse/YARN-3222) | RMNodeImpl#ReconnectNodeTransition should send scheduler events in sequential order |  Critical | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-3231](https://issues.apache.org/jira/browse/YARN-3231) | FairScheduler: Changing queueMaxRunningApps interferes with pending jobs |  Critical | . | Siqi Li | Siqi Li |
| [YARN-3242](https://issues.apache.org/jira/browse/YARN-3242) | Asynchrony in ZK-close can lead to ZKRMStateStore watcher receiving events for old client |  Critical | resourcemanager | zhihai xu | zhihai xu |
| [HADOOP-11674](https://issues.apache.org/jira/browse/HADOOP-11674) | oneByteBuf in CryptoInputStream and CryptoOutputStream should be non static |  Critical | io | Sean Busbey | Sean Busbey |
| [HDFS-7885](https://issues.apache.org/jira/browse/HDFS-7885) | Datanode should not trust the generation stamp provided by client |  Critical | datanode | vitthal (Suhas) Gogate | Tsz Wo Nicholas Sze |
| [YARN-3227](https://issues.apache.org/jira/browse/YARN-3227) | Timeline renew delegation token fails when RM user's TGT is expired |  Critical | . | Jonathan Eagles | Zhijie Shen |
| [YARN-3287](https://issues.apache.org/jira/browse/YARN-3287) | TimelineClient kerberos authentication failure uses wrong login context. |  Major | . | Jonathan Eagles | Daryn Sharp |
| [HDFS-7830](https://issues.apache.org/jira/browse/HDFS-7830) | DataNode does not release the volume lock when adding a volume fails. |  Major | datanode | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [YARN-3267](https://issues.apache.org/jira/browse/YARN-3267) | Timelineserver applies the ACL rules after applying the limit on the number of records |  Major | . | Prakash Ramachandran | Chang Li |
| [HDFS-7915](https://issues.apache.org/jira/browse/HDFS-7915) | The DataNode can sometimes allocate a ShortCircuitShm slot and fail to tell the DFSClient about it because of a network error |  Major | . | Colin P. McCabe | Colin P. McCabe |
| [HDFS-7929](https://issues.apache.org/jira/browse/HDFS-7929) | inotify unable fetch pre-upgrade edit log segments once upgrade starts |  Major | . | Zhe Zhang | Zhe Zhang |
| [HDFS-7587](https://issues.apache.org/jira/browse/HDFS-7587) | Edit log corruption can happen if append fails with a quota violation |  Blocker | namenode | Kihwal Lee | Jing Zhao |
| [HDFS-7930](https://issues.apache.org/jira/browse/HDFS-7930) | commitBlockSynchronization() does not remove locations |  Blocker | namenode | Konstantin Shvachko | Yi Liu |
| [YARN-3369](https://issues.apache.org/jira/browse/YARN-3369) | Missing NullPointer check in AppSchedulingInfo causes RM to die |  Blocker | resourcemanager | Giovanni Matteo Fumarola | Brahma Reddy Battula |
| [YARN-3393](https://issues.apache.org/jira/browse/YARN-3393) | Getting application(s) goes wrong when app finishes before starting the attempt |  Critical | timelineserver | Zhijie Shen | Zhijie Shen |
| [HDFS-7884](https://issues.apache.org/jira/browse/HDFS-7884) | NullPointerException in BlockSender |  Blocker | datanode | Tsz Wo Nicholas Sze | Brahma Reddy Battula |
| [HDFS-7960](https://issues.apache.org/jira/browse/HDFS-7960) | The full block report should prune zombie storages even if they're not empty |  Critical | . | Lei (Eddy) Xu | Colin P. McCabe |
| [HDFS-7742](https://issues.apache.org/jira/browse/HDFS-7742) | favoring decommissioning node for replication can cause a block to stay underreplicated for long periods |  Major | namenode | Nathan Roberts | Nathan Roberts |
| [MAPREDUCE-6303](https://issues.apache.org/jira/browse/MAPREDUCE-6303) | Read timeout when retrying a fetch error can be fatal to a reducer |  Blocker | . | Jason Lowe | Jason Lowe |
| [HDFS-7999](https://issues.apache.org/jira/browse/HDFS-7999) | FsDatasetImpl#createTemporary sometimes holds the FSDatasetImpl lock for a very long time |  Major | . | zhouyingchao | zhouyingchao |
| [HDFS-8072](https://issues.apache.org/jira/browse/HDFS-8072) | Reserved RBW space is not released if client terminates while writing block |  Major | datanode | Arpit Agarwal | Arpit Agarwal |
| [HDFS-8046](https://issues.apache.org/jira/browse/HDFS-8046) | Allow better control of getContentSummary |  Major | . | Kihwal Lee | Kihwal Lee |
| [YARN-2890](https://issues.apache.org/jira/browse/YARN-2890) | MiniYarnCluster should turn on timeline service if configured to do so |  Major | . | Mit Desai | Mit Desai |
| [YARN-3055](https://issues.apache.org/jira/browse/YARN-3055) | The token is not renewed properly if it's shared by jobs (oozie) in DelegationTokenRenewer |  Blocker | security | Yi Liu | Daryn Sharp |
| [HDFS-8127](https://issues.apache.org/jira/browse/HDFS-8127) | NameNode Failover during HA upgrade can cause DataNode to finalize upgrade |  Blocker | ha | Jing Zhao | Jing Zhao |
| [MAPREDUCE-6300](https://issues.apache.org/jira/browse/MAPREDUCE-6300) | Task list sort by task id broken |  Minor | . | Siqi Li | Siqi Li |
| [YARN-3493](https://issues.apache.org/jira/browse/YARN-3493) | RM fails to come up with error "Failed to load/recover state" when  mem settings are changed |  Critical | yarn | Sumana Sathish | Jian He |
| [MAPREDUCE-6238](https://issues.apache.org/jira/browse/MAPREDUCE-6238) | MR2 can't run local jobs with -libjars command options which is a regression from MR1 |  Critical | mrv2 | zhihai xu | zhihai xu |
| [HADOOP-11730](https://issues.apache.org/jira/browse/HADOOP-11730) | Regression: s3n read failure recovery broken |  Major | fs/s3 | Takenori Sato | Takenori Sato |
| [HADOOP-11802](https://issues.apache.org/jira/browse/HADOOP-11802) | DomainSocketWatcher thread terminates sometimes after there is an I/O error during requestShortCircuitShm |  Major | . | Eric Payne | Colin P. McCabe |
| [HDFS-8070](https://issues.apache.org/jira/browse/HDFS-8070) | Pre-HDFS-7915 DFSClient cannot use short circuit on post-HDFS-7915 DataNode |  Blocker | caching | Gopal V | Colin P. McCabe |
| [YARN-3464](https://issues.apache.org/jira/browse/YARN-3464) | Race condition in LocalizerRunner kills localizer before localizing all resources |  Critical | nodemanager | zhihai xu | zhihai xu |
| [MAPREDUCE-6324](https://issues.apache.org/jira/browse/MAPREDUCE-6324) | Uber jobs fail to update AMRM token when it rolls over |  Blocker | mr-am | Jason Lowe | Jason Lowe |
| [HADOOP-11491](https://issues.apache.org/jira/browse/HADOOP-11491) | HarFs incorrectly declared as requiring an authority |  Critical | fs | Gera Shegalov | Brahma Reddy Battula |
| [MAPREDUCE-5649](https://issues.apache.org/jira/browse/MAPREDUCE-5649) | Reduce cannot use more than 2G memory  for the final merge |  Major | mrv2 | stanley shi | Gera Shegalov |
| [HDFS-8219](https://issues.apache.org/jira/browse/HDFS-8219) | setStoragePolicy with folder behavior is different after cluster restart |  Major | . | Peter Shi | Surendra Singh Lilhore |
| [HDFS-7980](https://issues.apache.org/jira/browse/HDFS-7980) | Incremental BlockReport will dramatically slow down the startup of  a namenode |  Major | . | Hui Zheng | Walter Su |
| [HDFS-7894](https://issues.apache.org/jira/browse/HDFS-7894) | Rolling upgrade readiness is not updated in jmx until query command is issued. |  Critical | . | Kihwal Lee | Brahma Reddy Battula |
| [HDFS-8245](https://issues.apache.org/jira/browse/HDFS-8245) | Standby namenode doesn't process DELETED\_BLOCK if the add block request is in edit log. |  Major | . | Rushabh S Shah | Rushabh S Shah |
| [MAPREDUCE-6361](https://issues.apache.org/jira/browse/MAPREDUCE-6361) | NPE issue in shuffle caused by concurrent issue between copySucceeded() in one thread and copyFailed() in another thread on the same host |  Critical | . | Junping Du | Junping Du |
| [YARN-3526](https://issues.apache.org/jira/browse/YARN-3526) | ApplicationMaster tracking URL is incorrectly redirected on a QJM cluster |  Major | resourcemanager, webapp | Weiwei Yang | Weiwei Yang |
| [HDFS-8404](https://issues.apache.org/jira/browse/HDFS-8404) | Pending block replication can get stuck using older genstamp |  Major | namenode | Nathan Roberts | Nathan Roberts |
| [HDFS-8431](https://issues.apache.org/jira/browse/HDFS-8431) | hdfs crypto class not found in Windows |  Critical | scripts | Sumana Sathish | Anu Engineer |
| [HADOOP-11934](https://issues.apache.org/jira/browse/HADOOP-11934) | Use of JavaKeyStoreProvider in LdapGroupsMapping causes infinite loop |  Blocker | security | Mike Yoder | Larry McCay |
| [HDFS-7609](https://issues.apache.org/jira/browse/HDFS-7609) | Avoid retry cache collision when Standby NameNode loading edits |  Critical | namenode | Carrey Zhan | Ming Ma |
| [YARN-3725](https://issues.apache.org/jira/browse/YARN-3725) | App submission via REST API is broken in secure mode due to Timeline DT service address is empty |  Blocker | resourcemanager, timelineserver | Zhijie Shen | Zhijie Shen |
| [HDFS-8486](https://issues.apache.org/jira/browse/HDFS-8486) | DN startup may cause severe data loss |  Blocker | datanode | Daryn Sharp | Daryn Sharp |
| [HDFS-8270](https://issues.apache.org/jira/browse/HDFS-8270) | create() always retried with hardcoded timeout when file already exists with open lease |  Major | hdfs-client | Andrey Stepachev | J.Andreina |
| [YARN-3585](https://issues.apache.org/jira/browse/YARN-3585) | NodeManager cannot exit on SHUTDOWN event triggered and NM recovery is enabled |  Critical | . | Peng Zhang | Rohith Sharma K S |
| [YARN-3733](https://issues.apache.org/jira/browse/YARN-3733) | Fix DominantRC#compare() does not work as expected if cluster resource is empty |  Blocker | resourcemanager | Bibin A Chundatt | Rohith Sharma K S |
| [HDFS-8480](https://issues.apache.org/jira/browse/HDFS-8480) | Fix performance and timeout issues in HDFS-7929 by using hard-links to preserve old edit logs instead of copying them |  Critical | . | Zhe Zhang | Zhe Zhang |
| [YARN-3832](https://issues.apache.org/jira/browse/YARN-3832) | Resource Localization fails on a cluster due to existing cache directories |  Critical | nodemanager | Ranga Swamy | Brahma Reddy Battula |
| [HADOOP-8151](https://issues.apache.org/jira/browse/HADOOP-8151) | Error handling in snappy decompressor throws invalid exceptions |  Major | io, native | Todd Lipcon | Matt Foley |
| [YARN-3850](https://issues.apache.org/jira/browse/YARN-3850) | NM fails to read files from full disks which can lead to container logs being lost and other issues |  Blocker | log-aggregation, nodemanager | Varun Saxena | Varun Saxena |
| [YARN-3990](https://issues.apache.org/jira/browse/YARN-3990) | AsyncDispatcher may overloaded with RMAppNodeUpdateEvent when Node is connected/disconnected |  Critical | resourcemanager | Rohith Sharma K S | Bibin A Chundatt |
| [HADOOP-11932](https://issues.apache.org/jira/browse/HADOOP-11932) |  MetricsSinkAdapter hangs when being stopped |  Critical | . | Jian He | Brahma Reddy Battula |
| [YARN-3999](https://issues.apache.org/jira/browse/YARN-3999) | RM hangs on draining events |  Major | . | Jian He | Jian He |
| [YARN-4047](https://issues.apache.org/jira/browse/YARN-4047) | ClientRMService getApplications has high scheduler lock contention |  Major | resourcemanager | Jason Lowe | Jason Lowe |
| [HDFS-8863](https://issues.apache.org/jira/browse/HDFS-8863) | The remaining space check in BlockPlacementPolicyDefault is flawed |  Critical | . | Kihwal Lee | Kihwal Lee |
| [HDFS-8846](https://issues.apache.org/jira/browse/HDFS-8846) | Add a unit test for INotify functionality across a layout version upgrade |  Major | namenode | Zhe Zhang | Zhe Zhang |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-7035](https://issues.apache.org/jira/browse/HDFS-7035) | Make adding a new data directory to the DataNode an atomic operation and improve error handling |  Major | datanode | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [YARN-2766](https://issues.apache.org/jira/browse/YARN-2766) |  ApplicationHistoryManager is expected to return a sorted list of apps/attempts/containers |  Major | timelineserver | Robert Kanter | Robert Kanter |
| [YARN-1984](https://issues.apache.org/jira/browse/YARN-1984) | LeveldbTimelineStore does not handle db exceptions properly |  Major | . | Jason Lowe | Varun Saxena |
| [YARN-2920](https://issues.apache.org/jira/browse/YARN-2920) | CapacityScheduler should be notified when labels on nodes changed |  Major | . | Wangda Tan | Wangda Tan |
| [YARN-3092](https://issues.apache.org/jira/browse/YARN-3092) | Create common ResourceUsage class to track labeled resource usages in Capacity Scheduler |  Major | api, client, resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-3011](https://issues.apache.org/jira/browse/YARN-3011) | NM dies because of the failure of resource localization |  Major | nodemanager | Wang Hao | Varun Saxena |
| [YARN-3099](https://issues.apache.org/jira/browse/YARN-3099) | Capacity Scheduler LeafQueue/ParentQueue should use ResourceUsage to track used-resources-by-label. |  Major | api, client, resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-3098](https://issues.apache.org/jira/browse/YARN-3098) | Create common QueueCapacities class in Capacity Scheduler to track capacities-by-labels of queues |  Major | capacityscheduler | Wangda Tan | Wangda Tan |
| [YARN-2694](https://issues.apache.org/jira/browse/YARN-2694) | Ensure only single node labels specified in resource request / host, and node label expression only specified when resourceName=ANY |  Major | capacityscheduler, resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-3124](https://issues.apache.org/jira/browse/YARN-3124) | Capacity Scheduler LeafQueue/ParentQueue should use QueueCapacities to track capacities-by-label |  Major | api, client, resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-1809](https://issues.apache.org/jira/browse/YARN-1809) | Synchronize RM and Generic History Service Web-UIs |  Major | . | Zhijie Shen | Xuan Gong |
| [YARN-1884](https://issues.apache.org/jira/browse/YARN-1884) | ContainerReport should have nodeHttpAddress |  Major | . | Zhijie Shen | Xuan Gong |
| [HADOOP-11710](https://issues.apache.org/jira/browse/HADOOP-11710) | Make CryptoOutputStream behave like DFSOutputStream wrt synchronization |  Critical | fs | Sean Busbey | Sean Busbey |
| [YARN-3171](https://issues.apache.org/jira/browse/YARN-3171) | Sort by Application id, AppAttempt & ContainerID doesn't work in ATS / RM web ui |  Minor | timelineserver | Jeff Zhang | Naganarasimha G R |
| [YARN-3487](https://issues.apache.org/jira/browse/YARN-3487) | CapacityScheduler scheduler lock obtained unnecessarily when calling getQueue |  Critical | capacityscheduler | Jason Lowe | Jason Lowe |
| [YARN-3544](https://issues.apache.org/jira/browse/YARN-3544) | AM logs link missing in the RM UI for a completed app |  Blocker | . | Hitesh Shah | Xuan Gong |
| [YARN-2918](https://issues.apache.org/jira/browse/YARN-2918) | Don't fail RM if queue's configured labels are not existed in cluster-node-labels |  Major | resourcemanager | Rohith Sharma K S | Wangda Tan |
| [YARN-3700](https://issues.apache.org/jira/browse/YARN-3700) | ATS Web Performance issue at load time when large number of jobs |  Major | resourcemanager, webapp, yarn | Xuan Gong | Xuan Gong |
| [YARN-3740](https://issues.apache.org/jira/browse/YARN-3740) | Fixed the typo with the configuration name: APPLICATION\_HISTORY\_PREFIX\_MAX\_APPS |  Major | resourcemanager, webapp, yarn | Xuan Gong | Xuan Gong |
| [YARN-2900](https://issues.apache.org/jira/browse/YARN-2900) | Application (Attempt and Container) Not Found in AHS results in Internal Server Error (500) |  Major | timelineserver | Jonathan Eagles | Mit Desai |


