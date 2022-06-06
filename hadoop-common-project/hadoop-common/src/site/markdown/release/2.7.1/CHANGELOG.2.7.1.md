
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

## Release 2.7.1 - 2015-07-06

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-8226](https://issues.apache.org/jira/browse/HDFS-8226) | Non-HA rollback compatibility broken |  Blocker | . | J.Andreina | J.Andreina |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-11851](https://issues.apache.org/jira/browse/HADOOP-11851) | s3n to swallow IOEs on inner stream close |  Minor | fs/s3 | Steve Loughran | Takenori Sato |
| [HDFS-8521](https://issues.apache.org/jira/browse/HDFS-8521) | Add @VisibleForTesting annotation to {{BlockPoolSlice#selectReplicaToDelete}} |  Trivial | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-8361](https://issues.apache.org/jira/browse/HDFS-8361) | Choose SSD over DISK in block placement |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-8204](https://issues.apache.org/jira/browse/HDFS-8204) | Mover/Balancer should not schedule two replicas to the same DN |  Minor | balancer & mover | Walter Su | Walter Su |
| [HDFS-7770](https://issues.apache.org/jira/browse/HDFS-7770) | Need document for storage type label of data node storage locations under dfs.datanode.data.dir |  Major | documentation | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-7546](https://issues.apache.org/jira/browse/HDFS-7546) | Document, and set an accepting default for dfs.namenode.kerberos.principal.pattern |  Minor | security | Harsh J | Harsh J |
| [YARN-3539](https://issues.apache.org/jira/browse/YARN-3539) | Compatibility doc to state that ATS v1 is a stable REST API |  Major | documentation | Steve Loughran | Steve Loughran |
| [YARN-3489](https://issues.apache.org/jira/browse/YARN-3489) | RMServerUtils.validateResourceRequests should only obtain queue info once |  Major | resourcemanager | Jason Lowe | Varun Saxena |
| [YARN-3469](https://issues.apache.org/jira/browse/YARN-3469) | ZKRMStateStore: Avoid setting watches that are not required |  Minor | . | Jun Gong | Jun Gong |
| [YARN-3193](https://issues.apache.org/jira/browse/YARN-3193) | When visit standby RM webui, it will redirect to the active RM webui slowly. |  Minor | webapp | Japs\_123 | Steve Loughran |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-12103](https://issues.apache.org/jira/browse/HADOOP-12103) | Small refactoring of DelegationTokenAuthenticationFilter to allow code sharing |  Minor | security | Yongjun Zhang | Yongjun Zhang |
| [HADOOP-12100](https://issues.apache.org/jira/browse/HADOOP-12100) | ImmutableFsPermission should not override applyUmask since that method doesn't modify the FsPermission |  Major | . | Robert Kanter | Bibin A Chundatt |
| [HADOOP-12078](https://issues.apache.org/jira/browse/HADOOP-12078) | The default retry policy does not handle RetriableException correctly |  Critical | ipc | Arpit Agarwal | Arpit Agarwal |
| [HADOOP-12058](https://issues.apache.org/jira/browse/HADOOP-12058) | Fix dead links to DistCp and Hadoop Archives pages. |  Minor | documentation, site | Kazuho Fujii | Kazuho Fujii |
| [HADOOP-11973](https://issues.apache.org/jira/browse/HADOOP-11973) | Ensure ZkDelegationTokenSecretManager namespace znodes get created with ACLs |  Major | security | Gregory Chanan | Gregory Chanan |
| [HADOOP-11966](https://issues.apache.org/jira/browse/HADOOP-11966) | Variable cygwin is undefined in hadoop-config.sh when executed through hadoop-daemon.sh. |  Critical | scripts | Chris Nauroth | Chris Nauroth |
| [HADOOP-11934](https://issues.apache.org/jira/browse/HADOOP-11934) | Use of JavaKeyStoreProvider in LdapGroupsMapping causes infinite loop |  Blocker | security | Mike Yoder | Larry McCay |
| [HADOOP-11891](https://issues.apache.org/jira/browse/HADOOP-11891) | OsSecureRandom should lazily fill its reservoir |  Major | security | Arun Suresh | Arun Suresh |
| [HADOOP-11872](https://issues.apache.org/jira/browse/HADOOP-11872) | "hadoop dfs" command prints message about using "yarn jar" on Windows(branch-2 only) |  Minor | scripts | Varun Vasudev | Varun Vasudev |
| [HADOOP-11868](https://issues.apache.org/jira/browse/HADOOP-11868) | Invalid user logins trigger large backtraces in server log |  Major | . | Chang Li | Chang Li |
| [HADOOP-11802](https://issues.apache.org/jira/browse/HADOOP-11802) | DomainSocketWatcher thread terminates sometimes after there is an I/O error during requestShortCircuitShm |  Major | . | Eric Payne | Colin Patrick McCabe |
| [HADOOP-11730](https://issues.apache.org/jira/browse/HADOOP-11730) | Regression: s3n read failure recovery broken |  Major | fs/s3 | Takenori Sato | Takenori Sato |
| [HADOOP-11663](https://issues.apache.org/jira/browse/HADOOP-11663) | Remove description about Java 6 from docs |  Minor | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-9658](https://issues.apache.org/jira/browse/HADOOP-9658) | SnappyCodec#checkNativeCodeLoaded may unexpectedly fail when native code is not loaded |  Major | . | Zhijie Shen | Zhijie Shen |
| [HDFS-8681](https://issues.apache.org/jira/browse/HDFS-8681) | BlockScanner is incorrectly disabled by default |  Blocker | datanode | Andrew Wang | Arpit Agarwal |
| [HDFS-8633](https://issues.apache.org/jira/browse/HDFS-8633) | Fix setting of dfs.datanode.readahead.bytes in hdfs-default.xml to match DFSConfigKeys |  Minor | datanode | Ray Chiang | Ray Chiang |
| [HDFS-8626](https://issues.apache.org/jira/browse/HDFS-8626) | Reserved RBW space is not released if creation of RBW File fails |  Blocker | . | Kanaka Kumar Avvaru | Kanaka Kumar Avvaru |
| [HDFS-8600](https://issues.apache.org/jira/browse/HDFS-8600) | TestWebHdfsFileSystemContract.testGetFileBlockLocations fails in branch-2.7 |  Major | webhdfs | Arpit Agarwal | Arpit Agarwal |
| [HDFS-8596](https://issues.apache.org/jira/browse/HDFS-8596) | TestDistributedFileSystem et al tests are broken in branch-2 due to incorrect setting of "datanode" attribute |  Blocker | datanode | Yongjun Zhang | Yongjun Zhang |
| [HDFS-8595](https://issues.apache.org/jira/browse/HDFS-8595) | TestCommitBlockSynchronization fails in branch-2.7 |  Major | test | Arpit Agarwal | Arpit Agarwal |
| [HDFS-8583](https://issues.apache.org/jira/browse/HDFS-8583) | Document that NFS gateway does not work with rpcbind on SLES 11 |  Major | documentation | Arpit Agarwal | Arpit Agarwal |
| [HDFS-8576](https://issues.apache.org/jira/browse/HDFS-8576) |  Lease recovery should return true if the lease can be released and the file can be closed |  Major | namenode | J.Andreina | J.Andreina |
| [HDFS-8572](https://issues.apache.org/jira/browse/HDFS-8572) | DN always uses HTTP/localhost@REALM principals in SPNEGO |  Blocker | . | Haohui Mai | Haohui Mai |
| [HDFS-8566](https://issues.apache.org/jira/browse/HDFS-8566) | HDFS documentation about debug commands wrongly identifies them as "hdfs dfs" commands |  Major | documentation | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-8544](https://issues.apache.org/jira/browse/HDFS-8544) | Incorrect port specified in HFTP Guide document in branch-2 |  Major | documentation | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-8523](https://issues.apache.org/jira/browse/HDFS-8523) | Remove usage information on unsupported operation "fsck -showprogress" from branch-2 |  Major | documentation | J.Andreina | J.Andreina |
| [HDFS-8522](https://issues.apache.org/jira/browse/HDFS-8522) | Change heavily recorded NN logs from INFO to DEBUG level |  Major | namenode | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-8486](https://issues.apache.org/jira/browse/HDFS-8486) | DN startup may cause severe data loss |  Blocker | datanode | Daryn Sharp | Daryn Sharp |
| [HDFS-8480](https://issues.apache.org/jira/browse/HDFS-8480) | Fix performance and timeout issues in HDFS-7929 by using hard-links to preserve old edit logs instead of copying them |  Critical | . | Zhe Zhang | Zhe Zhang |
| [HDFS-8451](https://issues.apache.org/jira/browse/HDFS-8451) | DFSClient probe for encryption testing interprets empty URI property for "enabled" |  Blocker | encryption | Steve Loughran | Steve Loughran |
| [HDFS-8405](https://issues.apache.org/jira/browse/HDFS-8405) | Fix a typo in NamenodeFsck |  Minor | namenode | Tsz Wo Nicholas Sze | Takanobu Asanuma |
| [HDFS-8404](https://issues.apache.org/jira/browse/HDFS-8404) | Pending block replication can get stuck using older genstamp |  Major | namenode | Nathan Roberts | Nathan Roberts |
| [HDFS-8305](https://issues.apache.org/jira/browse/HDFS-8305) | HDFS INotify: the destination field of RenameOp should always end with the file name |  Major | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-8273](https://issues.apache.org/jira/browse/HDFS-8273) | FSNamesystem#Delete() should not call logSync() when holding the lock |  Blocker | namenode | Jing Zhao | Haohui Mai |
| [HDFS-8270](https://issues.apache.org/jira/browse/HDFS-8270) | create() always retried with hardcoded timeout when file already exists with open lease |  Major | hdfs-client | Andrey Stepachev | J.Andreina |
| [HDFS-8269](https://issues.apache.org/jira/browse/HDFS-8269) | getBlockLocations() does not resolve the .reserved path and generates incorrect edit logs when updating the atime |  Blocker | . | Yesha Vora | Haohui Mai |
| [HDFS-8245](https://issues.apache.org/jira/browse/HDFS-8245) | Standby namenode doesn't process DELETED\_BLOCK if the add block request is in edit log. |  Major | . | Rushabh S Shah | Rushabh S Shah |
| [HDFS-8213](https://issues.apache.org/jira/browse/HDFS-8213) | DFSClient should use hdfs.client.htrace HTrace configuration prefix rather than hadoop.htrace |  Critical | . | Billie Rinaldi | Colin Patrick McCabe |
| [HDFS-8179](https://issues.apache.org/jira/browse/HDFS-8179) | DFSClient#getServerDefaults returns null within 1 hour of system start |  Blocker | . | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-8163](https://issues.apache.org/jira/browse/HDFS-8163) | Using monotonicNow for block report scheduling causes test failures on recently restarted systems |  Blocker | datanode | Arpit Agarwal | Arpit Agarwal |
| [HDFS-8153](https://issues.apache.org/jira/browse/HDFS-8153) | Error Message points to wrong parent directory in case of path component name length error |  Major | namenode | Anu Engineer | Anu Engineer |
| [HDFS-8151](https://issues.apache.org/jira/browse/HDFS-8151) | Always use snapshot path as source when invalid snapshot names are used for diff based distcp |  Minor | distcp | Sushmitha Sreenivasan | Jing Zhao |
| [HDFS-8149](https://issues.apache.org/jira/browse/HDFS-8149) | The footer of the Web UI "Hadoop, 2014" is old |  Major | . | Akira AJISAKA | Brahma Reddy Battula |
| [HDFS-8147](https://issues.apache.org/jira/browse/HDFS-8147) | Mover should not schedule two replicas to the same DN storage |  Major | balancer & mover | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-8127](https://issues.apache.org/jira/browse/HDFS-8127) | NameNode Failover during HA upgrade can cause DataNode to finalize upgrade |  Blocker | ha | Jing Zhao | Jing Zhao |
| [HDFS-8091](https://issues.apache.org/jira/browse/HDFS-8091) | ACLStatus and XAttributes not properly presented to INodeAttributesProvider before returning to client |  Major | namenode | Arun Suresh | Arun Suresh |
| [HDFS-8081](https://issues.apache.org/jira/browse/HDFS-8081) | Split getAdditionalBlock() into two methods. |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-8070](https://issues.apache.org/jira/browse/HDFS-8070) | Pre-HDFS-7915 DFSClient cannot use short circuit on post-HDFS-7915 DataNode |  Blocker | caching | Gopal V | Colin Patrick McCabe |
| [HDFS-7980](https://issues.apache.org/jira/browse/HDFS-7980) | Incremental BlockReport will dramatically slow down the startup of  a namenode |  Major | . | Hui Zheng | Walter Su |
| [HDFS-7934](https://issues.apache.org/jira/browse/HDFS-7934) | Update RollingUpgrade rollback documentation: should use bootstrapstandby for standby NN |  Critical | documentation | J.Andreina | J.Andreina |
| [HDFS-7931](https://issues.apache.org/jira/browse/HDFS-7931) | DistributedFIleSystem should not look for keyProvider in cache if Encryption is disabled |  Minor | hdfs-client | Arun Suresh | Arun Suresh |
| [HDFS-7916](https://issues.apache.org/jira/browse/HDFS-7916) | 'reportBadBlocks' from datanodes to standby Node BPServiceActor goes for infinite loop |  Critical | datanode | Vinayakumar B | Rushabh S Shah |
| [HDFS-7894](https://issues.apache.org/jira/browse/HDFS-7894) | Rolling upgrade readiness is not updated in jmx until query command is issued. |  Critical | . | Kihwal Lee | Brahma Reddy Battula |
| [HDFS-6300](https://issues.apache.org/jira/browse/HDFS-6300) | Prevent multiple balancers from running simultaneously |  Critical | balancer & mover | Rakesh R | Rakesh R |
| [HDFS-5215](https://issues.apache.org/jira/browse/HDFS-5215) | dfs.datanode.du.reserved is not considered while computing available space |  Major | datanode | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-4660](https://issues.apache.org/jira/browse/HDFS-4660) | Block corruption can happen during pipeline recovery |  Blocker | datanode | Peng Zhang | Kihwal Lee |
| [MAPREDUCE-6410](https://issues.apache.org/jira/browse/MAPREDUCE-6410) | Aggregated Logs Deletion doesnt work after refreshing Log Retention Settings in secure cluster |  Critical | . | Zhang Wei | Varun Saxena |
| [MAPREDUCE-6387](https://issues.apache.org/jira/browse/MAPREDUCE-6387) | Serialize the recently added Task#encryptedSpillKey field at the end |  Minor | . | Arun Suresh | Arun Suresh |
| [MAPREDUCE-6361](https://issues.apache.org/jira/browse/MAPREDUCE-6361) | NPE issue in shuffle caused by concurrent issue between copySucceeded() in one thread and copyFailed() in another thread on the same host |  Critical | . | Junping Du | Junping Du |
| [MAPREDUCE-6339](https://issues.apache.org/jira/browse/MAPREDUCE-6339) | Job history file is not flushed correctly because isTimerActive flag is not set true when flushTimerTask is scheduled. |  Critical | mrv2 | zhihai xu | zhihai xu |
| [MAPREDUCE-6334](https://issues.apache.org/jira/browse/MAPREDUCE-6334) | Fetcher#copyMapOutput is leaking usedMemory upon IOException during InMemoryMapOutput shuffle handler |  Blocker | . | Eric Payne | Eric Payne |
| [MAPREDUCE-6324](https://issues.apache.org/jira/browse/MAPREDUCE-6324) | Uber jobs fail to update AMRM token when it rolls over |  Blocker | mr-am | Jason Lowe | Jason Lowe |
| [MAPREDUCE-6300](https://issues.apache.org/jira/browse/MAPREDUCE-6300) | Task list sort by task id broken |  Minor | . | Siqi Li | Siqi Li |
| [MAPREDUCE-6259](https://issues.apache.org/jira/browse/MAPREDUCE-6259) | IllegalArgumentException due to missing job submit time |  Major | jobhistoryserver | zhihai xu | zhihai xu |
| [MAPREDUCE-6252](https://issues.apache.org/jira/browse/MAPREDUCE-6252) | JobHistoryServer should not fail when encountering a missing directory |  Major | jobhistoryserver | Craig Welch | Craig Welch |
| [MAPREDUCE-6251](https://issues.apache.org/jira/browse/MAPREDUCE-6251) | JobClient needs additional retries at a higher level to address not-immediately-consistent dfs corner cases |  Major | jobhistoryserver, mrv2 | Craig Welch | Craig Welch |
| [MAPREDUCE-6238](https://issues.apache.org/jira/browse/MAPREDUCE-6238) | MR2 can't run local jobs with -libjars command options which is a regression from MR1 |  Critical | mrv2 | zhihai xu | zhihai xu |
| [YARN-3850](https://issues.apache.org/jira/browse/YARN-3850) | NM fails to read files from full disks which can lead to container logs being lost and other issues |  Blocker | log-aggregation, nodemanager | Varun Saxena | Varun Saxena |
| [YARN-3842](https://issues.apache.org/jira/browse/YARN-3842) | NMProxy should retry on NMNotYetReadyException |  Critical | . | Karthik Kambatla | Robert Kanter |
| [YARN-3832](https://issues.apache.org/jira/browse/YARN-3832) | Resource Localization fails on a cluster due to existing cache directories |  Critical | nodemanager | Ranga Swamy | Brahma Reddy Battula |
| [YARN-3809](https://issues.apache.org/jira/browse/YARN-3809) | Failed to launch new attempts because ApplicationMasterLauncher's threads all hang |  Major | resourcemanager | Jun Gong | Jun Gong |
| [YARN-3804](https://issues.apache.org/jira/browse/YARN-3804) | Both RM are on standBy state when kerberos user not in yarn.admin.acl |  Critical | resourcemanager | Bibin A Chundatt | Varun Saxena |
| [YARN-3764](https://issues.apache.org/jira/browse/YARN-3764) | CapacityScheduler should forbid moving LeafQueue from one parent to another |  Blocker | . | Wangda Tan | Wangda Tan |
| [YARN-3753](https://issues.apache.org/jira/browse/YARN-3753) | RM failed to come up with "java.io.IOException: Wait for ZKClient creation timed out" |  Critical | yarn | Sumana Sathish | Jian He |
| [YARN-3733](https://issues.apache.org/jira/browse/YARN-3733) | Fix DominantRC#compare() does not work as expected if cluster resource is empty |  Blocker | resourcemanager | Bibin A Chundatt | Rohith Sharma K S |
| [YARN-3725](https://issues.apache.org/jira/browse/YARN-3725) | App submission via REST API is broken in secure mode due to Timeline DT service address is empty |  Blocker | resourcemanager, timelineserver | Zhijie Shen | Zhijie Shen |
| [YARN-3723](https://issues.apache.org/jira/browse/YARN-3723) | Need to clearly document primaryFilter and otherInfo value type |  Critical | timelineserver | Zhijie Shen | Zhijie Shen |
| [YARN-3701](https://issues.apache.org/jira/browse/YARN-3701) | Isolating the error of generating a single app report when getting all apps from generic history service |  Blocker | timelineserver | Zhijie Shen | Zhijie Shen |
| [YARN-3694](https://issues.apache.org/jira/browse/YARN-3694) | Fix dead link for TimelineServer REST API |  Minor | documentation | Akira AJISAKA | Jagadesh Kiran N |
| [YARN-3681](https://issues.apache.org/jira/browse/YARN-3681) | yarn cmd says "could not find main class 'queue'" in windows |  Blocker | yarn | Sumana Sathish | Varun Saxena |
| [YARN-3677](https://issues.apache.org/jira/browse/YARN-3677) | Fix findbugs warnings in yarn-server-resourcemanager |  Minor | resourcemanager | Akira AJISAKA | Vinod Kumar Vavilapalli |
| [YARN-3675](https://issues.apache.org/jira/browse/YARN-3675) | FairScheduler: RM quits when node removal races with continousscheduling on the same node |  Critical | fairscheduler | Anubhav Dhoot | Anubhav Dhoot |
| [YARN-3646](https://issues.apache.org/jira/browse/YARN-3646) | Applications are getting stuck some times in case of retry policy forever |  Major | client | Raju Bairishetti | Raju Bairishetti |
| [YARN-3626](https://issues.apache.org/jira/browse/YARN-3626) | On Windows localized resources are not moved to the front of the classpath when they should be |  Major | yarn | Craig Welch | Craig Welch |
| [YARN-3614](https://issues.apache.org/jira/browse/YARN-3614) | FileSystemRMStateStore throw exception when failed to remove application, that cause resourcemanager to crash |  Critical | resourcemanager | lachisis |  |
| [YARN-3601](https://issues.apache.org/jira/browse/YARN-3601) | Fix UT TestRMFailover.testRMWebAppRedirect |  Critical | resourcemanager, webapp | Weiwei Yang | Weiwei Yang |
| [YARN-3585](https://issues.apache.org/jira/browse/YARN-3585) | NodeManager cannot exit on SHUTDOWN event triggered and NM recovery is enabled |  Critical | . | Peng Zhang | Rohith Sharma K S |
| [YARN-3554](https://issues.apache.org/jira/browse/YARN-3554) | Default value for maximum nodemanager connect wait time is too high |  Major | . | Jason Lowe | Naganarasimha G R |
| [YARN-3537](https://issues.apache.org/jira/browse/YARN-3537) | NPE when NodeManager.serviceInit fails and stopRecoveryStore invoked |  Major | nodemanager | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-3526](https://issues.apache.org/jira/browse/YARN-3526) | ApplicationMaster tracking URL is incorrectly redirected on a QJM cluster |  Major | resourcemanager, webapp | Weiwei Yang | Weiwei Yang |
| [YARN-3522](https://issues.apache.org/jira/browse/YARN-3522) | DistributedShell uses the wrong user to put timeline data |  Blocker | timelineserver | Zhijie Shen | Zhijie Shen |
| [YARN-3516](https://issues.apache.org/jira/browse/YARN-3516) | killing ContainerLocalizer action doesn't take effect when private localizer receives FETCH\_FAILURE status. |  Minor | nodemanager | zhihai xu | zhihai xu |
| [YARN-3497](https://issues.apache.org/jira/browse/YARN-3497) | ContainerManagementProtocolProxy modifies IPC timeout conf without making a copy |  Major | client | Jason Lowe | Jason Lowe |
| [YARN-3493](https://issues.apache.org/jira/browse/YARN-3493) | RM fails to come up with error "Failed to load/recover state" when  mem settings are changed |  Critical | yarn | Sumana Sathish | Jian He |
| [YARN-3485](https://issues.apache.org/jira/browse/YARN-3485) | FairScheduler headroom calculation doesn't consider maxResources for Fifo and FairShare policies |  Critical | fairscheduler | Karthik Kambatla | Karthik Kambatla |
| [YARN-3476](https://issues.apache.org/jira/browse/YARN-3476) | Nodemanager can fail to delete local logs if log aggregation fails |  Major | log-aggregation, nodemanager | Jason Lowe | Rohith Sharma K S |
| [YARN-3472](https://issues.apache.org/jira/browse/YARN-3472) | Possible leak in DelegationTokenRenewer#allTokens |  Major | . | Jian He | Rohith Sharma K S |
| [YARN-3466](https://issues.apache.org/jira/browse/YARN-3466) | Fix RM nodes web page to sort by node HTTP-address, #containers and node-label column |  Major | resourcemanager, webapp | Jason Lowe | Jason Lowe |
| [YARN-3465](https://issues.apache.org/jira/browse/YARN-3465) | Use LinkedHashMap to preserve order of resource requests |  Major | nodemanager | zhihai xu | zhihai xu |
| [YARN-3464](https://issues.apache.org/jira/browse/YARN-3464) | Race condition in LocalizerRunner kills localizer before localizing all resources |  Critical | nodemanager | zhihai xu | zhihai xu |
| [YARN-3462](https://issues.apache.org/jira/browse/YARN-3462) | Patches applied for YARN-2424 are inconsistent between trunk and branch-2 |  Major | . | Sidharta Seethana | Naganarasimha G R |
| [YARN-3457](https://issues.apache.org/jira/browse/YARN-3457) | NPE when NodeManager.serviceInit fails and stopRecoveryStore called |  Minor | nodemanager | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-3434](https://issues.apache.org/jira/browse/YARN-3434) | Interaction between reservations and userlimit can result in significant ULF violation |  Major | capacityscheduler | Thomas Graves | Thomas Graves |
| [YARN-3385](https://issues.apache.org/jira/browse/YARN-3385) | Race condition: KeeperException$NoNodeException will cause RM shutdown during ZK node deletion. |  Critical | resourcemanager | zhihai xu | zhihai xu |
| [YARN-3382](https://issues.apache.org/jira/browse/YARN-3382) | Some of UserMetricsInfo metrics are incorrectly set to root queue metrics |  Major | webapp | Rohit Agarwal | Rohit Agarwal |
| [YARN-3358](https://issues.apache.org/jira/browse/YARN-3358) | Audit log not present while refreshing Service ACLs |  Minor | resourcemanager | Varun Saxena | Varun Saxena |
| [YARN-3351](https://issues.apache.org/jira/browse/YARN-3351) | AppMaster tracking URL is broken in HA |  Major | webapp | Anubhav Dhoot | Anubhav Dhoot |
| [YARN-3243](https://issues.apache.org/jira/browse/YARN-3243) | CapacityScheduler should pass headroom from parent to children to make sure ParentQueue obey its capacity limits. |  Major | capacityscheduler, resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-2238](https://issues.apache.org/jira/browse/YARN-2238) | filtering on UI sticks even if I move away from the page |  Major | webapp | Sangjin Lee | Jian He |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-8597](https://issues.apache.org/jira/browse/HDFS-8597) | Fix TestFSImage#testZeroBlockSize on Windows |  Major | datanode, test | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-7164](https://issues.apache.org/jira/browse/HDFS-7164) | Feature documentation for HDFS-6581 |  Major | documentation | Arpit Agarwal | Arpit Agarwal |
| [YARN-3711](https://issues.apache.org/jira/browse/YARN-3711) | Documentation of ResourceManager HA should explain configurations about listen addresses |  Minor | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-3686](https://issues.apache.org/jira/browse/YARN-3686) | CapacityScheduler should trim default\_node\_label\_expression |  Critical | api, client, resourcemanager | Wangda Tan | Sunil G |
| [YARN-3609](https://issues.apache.org/jira/browse/YARN-3609) | Move load labels from storage from serviceInit to serviceStart to make it works with RM HA case. |  Major | resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-3544](https://issues.apache.org/jira/browse/YARN-3544) | AM logs link missing in the RM UI for a completed app |  Blocker | . | Hitesh Shah | Xuan Gong |
| [YARN-3487](https://issues.apache.org/jira/browse/YARN-3487) | CapacityScheduler scheduler lock obtained unnecessarily when calling getQueue |  Critical | capacityscheduler | Jason Lowe | Jason Lowe |
| [YARN-3301](https://issues.apache.org/jira/browse/YARN-3301) | Fix the format issue of the new RM web UI and AHS web UI after YARN-3272 / YARN-3262 |  Major | resourcemanager | Xuan Gong | Xuan Gong |
| [YARN-3006](https://issues.apache.org/jira/browse/YARN-3006) | Improve the error message when attempting manual failover with auto-failover enabled |  Minor | . | Akira AJISAKA | Akira AJISAKA |
| [YARN-2918](https://issues.apache.org/jira/browse/YARN-2918) | Don't fail RM if queue's configured labels are not existed in cluster-node-labels |  Major | resourcemanager | Rohith Sharma K S | Wangda Tan |
| [YARN-2900](https://issues.apache.org/jira/browse/YARN-2900) | Application (Attempt and Container) Not Found in AHS results in Internal Server Error (500) |  Major | timelineserver | Jonathan Eagles | Mit Desai |
| [YARN-2605](https://issues.apache.org/jira/browse/YARN-2605) | [RM HA] Rest api endpoints doing redirect incorrectly |  Major | resourcemanager | bc Wong | Xuan Gong |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


