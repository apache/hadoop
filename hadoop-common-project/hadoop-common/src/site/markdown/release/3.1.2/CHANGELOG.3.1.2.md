
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

## Release 3.1.2 - 2019-01-29



### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-13448](https://issues.apache.org/jira/browse/HDFS-13448) | HDFS Block Placement - Ignore Locality for First Block Replica |  Minor | block placement, hdfs-client | BELUGA BEHR | BELUGA BEHR |
| [HADOOP-15677](https://issues.apache.org/jira/browse/HADOOP-15677) | WASB: Add support for StreamCapabilities |  Major | fs/azure | Thomas Marquardt | Thomas Marquardt |
| [HADOOP-15996](https://issues.apache.org/jira/browse/HADOOP-15996) | Plugin interface to support more complex usernames in Hadoop |  Major | security | Eric Yang | Bolke de Bruin |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-8226](https://issues.apache.org/jira/browse/YARN-8226) | Improve anti-affinity section description in YARN Service API doc |  Major | docs, documentation | Charan Hebri | Gour Saha |
| [HADOOP-15609](https://issues.apache.org/jira/browse/HADOOP-15609) | Retry KMS calls when SSLHandshakeException occurs |  Major | common, kms | Kitti Nanasi | Kitti Nanasi |
| [HADOOP-15612](https://issues.apache.org/jira/browse/HADOOP-15612) | Improve exception when tfile fails to load LzoCodec |  Major | . | Gera Shegalov | Gera Shegalov |
| [HDFS-11060](https://issues.apache.org/jira/browse/HDFS-11060) | make DEFAULT\_MAX\_CORRUPT\_FILEBLOCKS\_RETURNED configurable |  Minor | hdfs | Lantao Jin | Lantao Jin |
| [HDFS-13727](https://issues.apache.org/jira/browse/HDFS-13727) | Log full stack trace if DiskBalancer exits with an unhandled exception |  Minor | diskbalancer | Stephen O'Donnell | Gabor Bota |
| [YARN-8584](https://issues.apache.org/jira/browse/YARN-8584) | Several typos in Log Aggregation related classes |  Minor | . | Szilard Nemeth | Szilard Nemeth |
| [HDFS-13728](https://issues.apache.org/jira/browse/HDFS-13728) | Disk Balancer should not fail if volume usage is greater than capacity |  Minor | diskbalancer | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-13447](https://issues.apache.org/jira/browse/HDFS-13447) | Fix Typos - Node Not Chosen |  Trivial | namenode | BELUGA BEHR | BELUGA BEHR |
| [YARN-8601](https://issues.apache.org/jira/browse/YARN-8601) | Print ExecutionType in Container report CLI |  Minor | . | Bilwa S T | Bilwa S T |
| [HDFS-13658](https://issues.apache.org/jira/browse/HDFS-13658) | Expose HighestPriorityLowRedundancy blocks statistics |  Major | hdfs | Kitti Nanasi | Kitti Nanasi |
| [YARN-8568](https://issues.apache.org/jira/browse/YARN-8568) | Replace the deprecated zk-address property in the HA config example in ResourceManagerHA.md |  Minor | yarn | Antal Bálint Steinbach | Antal Bálint Steinbach |
| [HDFS-13735](https://issues.apache.org/jira/browse/HDFS-13735) | Make QJM HTTP URL connection timeout configurable |  Minor | qjm | Chao Sun | Chao Sun |
| [HDFS-13814](https://issues.apache.org/jira/browse/HDFS-13814) | Remove super user privilege requirement for NameNode.getServiceStatus |  Minor | namenode | Chao Sun | Chao Sun |
| [YARN-8559](https://issues.apache.org/jira/browse/YARN-8559) | Expose mutable-conf scheduler's configuration in RM /scheduler-conf endpoint |  Major | resourcemanager | Anna Savarin | Weiwei Yang |
| [HDFS-13813](https://issues.apache.org/jira/browse/HDFS-13813) | Exit NameNode if dangling child inode is detected when saving FsImage |  Major | hdfs, namenode | Siyao Meng | Siyao Meng |
| [HADOOP-14212](https://issues.apache.org/jira/browse/HADOOP-14212) | Expose SecurityEnabled boolean field in JMX for other services besides NameNode |  Minor | . | Ray Burgemeestre | Adam Antal |
| [HDFS-13217](https://issues.apache.org/jira/browse/HDFS-13217) | Audit log all EC policy names during addErasureCodingPolicies |  Major | erasure-coding | liaoyuxiangqin | liaoyuxiangqin |
| [HADOOP-9214](https://issues.apache.org/jira/browse/HADOOP-9214) | Create a new touch command to allow modifying atime and mtime |  Minor | tools | Brian Burton | Hrishikesh Gadre |
| [YARN-8242](https://issues.apache.org/jira/browse/YARN-8242) | YARN NM: OOM error while reading back the state store on recovery |  Critical | yarn | Kanwaljeet Sachdev | Pradeep Ambati |
| [HDFS-13821](https://issues.apache.org/jira/browse/HDFS-13821) | RBF: Add dfs.federation.router.mount-table.cache.enable so that users can disable cache |  Major | hdfs | Fei Hui | Fei Hui |
| [HDFS-13861](https://issues.apache.org/jira/browse/HDFS-13861) | RBF: Illegal Router Admin command leads to printing usage for all commands |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-13831](https://issues.apache.org/jira/browse/HDFS-13831) | Make block increment deletion number configurable |  Major | . | Yiqun Lin | Ryan Wu |
| [YARN-8051](https://issues.apache.org/jira/browse/YARN-8051) | TestRMEmbeddedElector#testCallbackSynchronization is flakey |  Major | test | Robert Kanter | Robert Kanter |
| [HDFS-13857](https://issues.apache.org/jira/browse/HDFS-13857) | RBF: Choose to enable the default nameservice to read/write files |  Major | federation, hdfs | yanghuafeng | yanghuafeng |
| [HDFS-13812](https://issues.apache.org/jira/browse/HDFS-13812) | Fix the inconsistent default refresh interval on Caching documentation |  Trivial | documentation | BELUGA BEHR | Hrishikesh Gadre |
| [YARN-8638](https://issues.apache.org/jira/browse/YARN-8638) | Allow linux container runtimes to be pluggable |  Minor | nodemanager | Craig Condit | Craig Condit |
| [HDFS-13902](https://issues.apache.org/jira/browse/HDFS-13902) |  Add JMX, conf and stacks menus to the datanode page |  Minor | datanode | fengchuang | fengchuang |
| [YARN-8680](https://issues.apache.org/jira/browse/YARN-8680) | YARN NM: Implement Iterable Abstraction for LocalResourceTracker state |  Critical | yarn | Pradeep Ambati | Pradeep Ambati |
| [HADOOP-15726](https://issues.apache.org/jira/browse/HADOOP-15726) | Create utility to limit frequency of log statements |  Major | common, util | Erik Krogen | Erik Krogen |
| [YARN-7974](https://issues.apache.org/jira/browse/YARN-7974) | Allow updating application tracking url after registration |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-8758](https://issues.apache.org/jira/browse/YARN-8758) | Support getting PreemptionMessage when using AMRMClientAsync |  Major | yarn | Krishna Kishore | Zian Chen |
| [YARN-8896](https://issues.apache.org/jira/browse/YARN-8896) | Limit the maximum number of container assignments per heartbeat |  Major | . | Weiwei Yang | Zhankun Tang |
| [HADOOP-15804](https://issues.apache.org/jira/browse/HADOOP-15804) | upgrade to commons-compress 1.18 |  Major | . | PJ Fanning | Akira Ajisaka |
| [YARN-8908](https://issues.apache.org/jira/browse/YARN-8908) | Fix errors in yarn-default.xml related to GPU/FPGA |  Major | . | Zhankun Tang | Zhankun Tang |
| [HDFS-13941](https://issues.apache.org/jira/browse/HDFS-13941) | make storageId in BlockPoolTokenSecretManager.checkAccess optional |  Major | . | Ajay Kumar | Ajay Kumar |
| [HDFS-14029](https://issues.apache.org/jira/browse/HDFS-14029) | Sleep in TestLazyPersistFiles should be put into a loop |  Trivial | hdfs | Adam Antal | Adam Antal |
| [YARN-8915](https://issues.apache.org/jira/browse/YARN-8915) | Update the doc about the default value of "maximum-container-assignments" for capacity scheduler |  Minor | . | Zhankun Tang | Zhankun Tang |
| [HADOOP-15855](https://issues.apache.org/jira/browse/HADOOP-15855) | Review hadoop credential doc, including object store details |  Minor | documentation, security | Steve Loughran | Steve Loughran |
| [YARN-7225](https://issues.apache.org/jira/browse/YARN-7225) | Add queue and partition info to RM audit log |  Major | resourcemanager | Jonathan Hung | Eric Payne |
| [YARN-8969](https://issues.apache.org/jira/browse/YARN-8969) | AbstractYarnScheduler#getNodeTracker should return generic type to avoid type casting |  Major | . | Wanqiang Ji | Wanqiang Ji |
| [YARN-8977](https://issues.apache.org/jira/browse/YARN-8977) | Remove unnecessary type casting when calling AbstractYarnScheduler#getSchedulerNode |  Trivial | . | Wanqiang Ji | Wanqiang Ji |
| [HDFS-14070](https://issues.apache.org/jira/browse/HDFS-14070) | Refactor NameNodeWebHdfsMethods to allow better extensibility |  Major | . | CR Hota | CR Hota |
| [HADOOP-12558](https://issues.apache.org/jira/browse/HADOOP-12558) | distcp documentation is woefully out of date |  Critical | documentation, tools/distcp | Allen Wittenauer | Dinesh Chitlangia |
| [HADOOP-15919](https://issues.apache.org/jira/browse/HADOOP-15919) | AliyunOSS: Enable Yarn to use OSS |  Major | fs/oss | wujinhu | wujinhu |
| [HADOOP-15943](https://issues.apache.org/jira/browse/HADOOP-15943) | AliyunOSS: add missing owner & group attributes for oss FileStatus |  Major | fs/oss | wujinhu | wujinhu |
| [MAPREDUCE-7164](https://issues.apache.org/jira/browse/MAPREDUCE-7164) | FileOutputCommitter does not report progress while merging paths. |  Major | . | Kuhu Shukla | Kuhu Shukla |
| [YARN-9069](https://issues.apache.org/jira/browse/YARN-9069) | Fix SchedulerInfo#getSchedulerType for custom schedulers |  Minor | . | Bilwa S T | Bilwa S T |
| [HDFS-14095](https://issues.apache.org/jira/browse/HDFS-14095) | EC: Track Erasure Coding commands in DFS statistics |  Major | erasure-coding | Ayush Saxena | Ayush Saxena |
| [YARN-9036](https://issues.apache.org/jira/browse/YARN-9036) | Escape newlines in health report in YARN UI |  Major | . | Jonathan Hung | Keqiu Hu |
| [YARN-9085](https://issues.apache.org/jira/browse/YARN-9085) | Add Guaranteed and MaxCapacity to CSQueueMetrics |  Major | . | Jonathan Hung | Jonathan Hung |
| [HADOOP-15808](https://issues.apache.org/jira/browse/HADOOP-15808) | Harden Token service loader use |  Major | security | Steve Loughran | Steve Loughran |
| [YARN-9122](https://issues.apache.org/jira/browse/YARN-9122) | Add table of contents to YARN Service API document |  Minor | documentation | Akira Ajisaka | Zhankun Tang |
| [HDFS-14171](https://issues.apache.org/jira/browse/HDFS-14171) | Performance improvement in Tailing EditLog |  Major | namenode | Kenneth Yang | Kenneth Yang |
| [HADOOP-15959](https://issues.apache.org/jira/browse/HADOOP-15959) | revert HADOOP-12751 |  Minor | security | Steve Loughran | Steve Loughran |
| [HADOOP-16019](https://issues.apache.org/jira/browse/HADOOP-16019) | ZKDelegationTokenSecretManager won't log exception message occured in function setJaasConfiguration |  Minor | common | luhuachao | luhuachao |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-7773](https://issues.apache.org/jira/browse/YARN-7773) | YARN Federation used Mysql as state store throw exception, Unknown column 'homeSubCluster' in 'field list' |  Blocker | federation | Yiran Wu | Yiran Wu |
| [YARN-8426](https://issues.apache.org/jira/browse/YARN-8426) | Upgrade jquery-ui to 1.12.1 in YARN |  Major | webapp | Sunil Govindan | Sunil Govindan |
| [HADOOP-15550](https://issues.apache.org/jira/browse/HADOOP-15550) | Avoid static initialization of ObjectMappers |  Minor | performance | Todd Lipcon | Todd Lipcon |
| [HDFS-13721](https://issues.apache.org/jira/browse/HDFS-13721) | NPE in DataNode due to uninitialized DiskBalancer |  Major | datanode, diskbalancer | Xiao Chen | Xiao Chen |
| [YARN-8360](https://issues.apache.org/jira/browse/YARN-8360) | Yarn service conflict between restart policy and NM configuration |  Critical | yarn | Chandni Singh | Suma Shivaprasad |
| [YARN-8380](https://issues.apache.org/jira/browse/YARN-8380) | Support bind propagation options for mounts in docker runtime |  Major | . | Billie Rinaldi | Billie Rinaldi |
| [YARN-8544](https://issues.apache.org/jira/browse/YARN-8544) | [DS] AM registration fails when hadoop authorization is enabled |  Blocker | . | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-8548](https://issues.apache.org/jira/browse/YARN-8548) | AllocationRespose proto setNMToken initBuilder not done |  Major | . | Bibin A Chundatt | Bilwa S T |
| [YARN-7748](https://issues.apache.org/jira/browse/YARN-7748) | TestContainerResizing.testIncreaseContainerUnreservedWhenApplicationCompleted fails due to multiple container fail events |  Major | capacityscheduler | Haibo Chen | Weiwei Yang |
| [YARN-8577](https://issues.apache.org/jira/browse/YARN-8577) | Fix the broken anchor in SLS site-doc |  Minor | documentation | Weiwei Yang | Weiwei Yang |
| [YARN-4606](https://issues.apache.org/jira/browse/YARN-4606) | CapacityScheduler: applications could get starved because computation of #activeUsers considers pending apps |  Critical | capacity scheduler, capacityscheduler | Karam Singh | Manikandan R |
| [YARN-8330](https://issues.apache.org/jira/browse/YARN-8330) | Avoid publishing reserved container to ATS from RM |  Critical | yarn-native-services | Yesha Vora | Suma Shivaprasad |
| [YARN-8429](https://issues.apache.org/jira/browse/YARN-8429) | Improve diagnostic message when artifact is not set properly |  Major | . | Yesha Vora | Gour Saha |
| [YARN-8571](https://issues.apache.org/jira/browse/YARN-8571) | Validate service principal format prior to launching yarn service |  Major | security, yarn | Eric Yang | Eric Yang |
| [HADOOP-15637](https://issues.apache.org/jira/browse/HADOOP-15637) | LocalFs#listLocatedStatus does not filter out hidden .crc files |  Minor | fs | Erik Krogen | Erik Krogen |
| [YARN-8579](https://issues.apache.org/jira/browse/YARN-8579) | New AM attempt could not retrieve previous attempt component data |  Critical | . | Yesha Vora | Gour Saha |
| [YARN-8397](https://issues.apache.org/jira/browse/YARN-8397) | Potential thread leak in ActivitiesManager |  Major | . | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-8595](https://issues.apache.org/jira/browse/YARN-8595) | [UI2] Container diagnostic information is missing from container page |  Major | yarn-ui-v2 | Akhil PB | Akhil PB |
| [YARN-8403](https://issues.apache.org/jira/browse/YARN-8403) | Nodemanager logs failed to download file with INFO level |  Major | yarn | Eric Yang | Eric Yang |
| [YARN-8610](https://issues.apache.org/jira/browse/YARN-8610) | Yarn Service Upgrade: Typo in Error message |  Major | . | Chandni Singh | Chandni Singh |
| [YARN-8593](https://issues.apache.org/jira/browse/YARN-8593) | Add RM web service endpoint to get user information |  Major | resourcemanager | Akhil PB | Akhil PB |
| [YARN-8594](https://issues.apache.org/jira/browse/YARN-8594) | [UI2] Display current logged in user |  Major | . | Akhil PB | Akhil PB |
| [YARN-8592](https://issues.apache.org/jira/browse/YARN-8592) | [UI2] rmip:port/ui2 endpoint shows a blank page in windows OS and Chrome browser |  Major | . | Akhil S Naik | Akhil PB |
| [YARN-8318](https://issues.apache.org/jira/browse/YARN-8318) | [UI2] IP address in component page shows N/A |  Major | yarn-ui-v2 | Yesha Vora | Yesha Vora |
| [YARN-6966](https://issues.apache.org/jira/browse/YARN-6966) | NodeManager metrics may return wrong negative values when NM restart |  Major | . | Yang Wang | Szilard Nemeth |
| [YARN-8620](https://issues.apache.org/jira/browse/YARN-8620) | [UI2] YARN Services UI new submission failures are not debuggable |  Major | yarn-ui-v2 | Akhil PB | Akhil PB |
| [YARN-8615](https://issues.apache.org/jira/browse/YARN-8615) | [UI2] Resource Usage tab shows only memory related info. No info available for vcores/gpu. |  Major | yarn-ui-v2 | Sumana Sathish | Akhil PB |
| [HDFS-13792](https://issues.apache.org/jira/browse/HDFS-13792) | Fix FSN read/write lock metrics name |  Trivial | documentation, metrics | Chao Sun | Chao Sun |
| [YARN-8629](https://issues.apache.org/jira/browse/YARN-8629) | Container cleanup fails while trying to delete Cgroups |  Critical | . | Yesha Vora | Suma Shivaprasad |
| [YARN-8407](https://issues.apache.org/jira/browse/YARN-8407) | Container launch exception in AM log should be printed in ERROR level |  Major | . | Yesha Vora | Yesha Vora |
| [HDFS-13799](https://issues.apache.org/jira/browse/HDFS-13799) | TestEditLogTailer#testTriggersLogRollsForAllStandbyNN fails due to missing synchronization between rollEditsRpcExecutor and tailerThread shutdown |  Minor | ha | Hrishikesh Gadre | Hrishikesh Gadre |
| [HDFS-13786](https://issues.apache.org/jira/browse/HDFS-13786) | EC: Display erasure coding policy for sub-directories is not working |  Major | erasure-coding | Souryakanta Dwivedy | Ayush Saxena |
| [HDFS-13785](https://issues.apache.org/jira/browse/HDFS-13785) | EC: "removePolicy" is not working for built-in/system Erasure Code policies |  Minor | documentation, erasure-coding | Souryakanta Dwivedy | Ayush Saxena |
| [YARN-8633](https://issues.apache.org/jira/browse/YARN-8633) | Update DataTables version in yarn-common in line with JQuery 3 upgrade |  Major | yarn | Akhil PB | Akhil PB |
| [YARN-8331](https://issues.apache.org/jira/browse/YARN-8331) | Race condition in NM container launched after done |  Major | . | Yang Wang | Pradeep Ambati |
| [YARN-8521](https://issues.apache.org/jira/browse/YARN-8521) | NPE in AllocationTagsManager when a container is removed more than once |  Major | resourcemanager | Weiwei Yang | Weiwei Yang |
| [YARN-8575](https://issues.apache.org/jira/browse/YARN-8575) | Avoid committing allocation proposal to unavailable nodes in async scheduling |  Major | capacityscheduler | Tao Yang | Tao Yang |
| [HDFS-13668](https://issues.apache.org/jira/browse/HDFS-13668) | FSPermissionChecker may throws AIOOE when check inode permission |  Major | namenode | He Xiaoqiao | He Xiaoqiao |
| [HADOOP-15638](https://issues.apache.org/jira/browse/HADOOP-15638) | KMS Accept Queue Size default changed from 500 to 128 in Hadoop 3.x |  Major | kms | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-13823](https://issues.apache.org/jira/browse/HDFS-13823) | NameNode UI : "Utilities -\> Browse the file system -\> open a file -\> Head the file" is not working |  Major | ui | Nanda kumar | Nanda kumar |
| [HDFS-13738](https://issues.apache.org/jira/browse/HDFS-13738) | fsck -list-corruptfileblocks has infinite loop if user is not privileged. |  Major | tools | Wei-Chiu Chuang | Yuen-Kuei Hsueh |
| [HDFS-13758](https://issues.apache.org/jira/browse/HDFS-13758) | DatanodeManager should throw exception if it has BlockRecoveryCommand but the block is not under construction |  Major | namenode | Wei-Chiu Chuang | chencan |
| [YARN-8614](https://issues.apache.org/jira/browse/YARN-8614) | Fix few annotation typos in YarnConfiguration |  Trivial | . | Sen Zhao | Sen Zhao |
| [HDFS-13819](https://issues.apache.org/jira/browse/HDFS-13819) | TestDirectoryScanner#testDirectoryScannerInFederatedCluster is flaky |  Minor | hdfs | Daniel Templeton | Daniel Templeton |
| [YARN-8656](https://issues.apache.org/jira/browse/YARN-8656) | container-executor should not write cgroup tasks files for docker containers |  Major | . | Jim Brennan | Jim Brennan |
| [YARN-8474](https://issues.apache.org/jira/browse/YARN-8474) | sleeper service fails to launch with "Authentication Required" |  Critical | yarn | Sumana Sathish | Billie Rinaldi |
| [YARN-8667](https://issues.apache.org/jira/browse/YARN-8667) | Cleanup symlinks when container restarted by NM to solve issue "find: File system loop detected;" for tar ball artifacts. |  Critical | . | Rohith Sharma K S | Chandni Singh |
| [HDFS-10240](https://issues.apache.org/jira/browse/HDFS-10240) | Race between close/recoverLease leads to missing block |  Major | . | zhouyingchao | Jinglun |
| [HADOOP-15655](https://issues.apache.org/jira/browse/HADOOP-15655) | Enhance KMS client retry behavior |  Critical | kms | Kitti Nanasi | Kitti Nanasi |
| [YARN-8612](https://issues.apache.org/jira/browse/YARN-8612) | Fix NM Collector Service Port issue in YarnConfiguration |  Major | ATSv2 | Prabha Manepalli | Prabha Manepalli |
| [HDFS-13747](https://issues.apache.org/jira/browse/HDFS-13747) | Statistic for list\_located\_status is incremented incorrectly by listStatusIterator |  Minor | hdfs-client | Todd Lipcon | Antal Mihalyi |
| [HADOOP-15674](https://issues.apache.org/jira/browse/HADOOP-15674) | Test failure TestSSLHttpServer.testExcludedCiphers with TLS\_ECDHE\_RSA\_WITH\_AES\_128\_CBC\_SHA256 cipher suite |  Major | common | Gabor Bota | Szilard Nemeth |
| [YARN-8640](https://issues.apache.org/jira/browse/YARN-8640) | Restore previous state in container-executor after failure |  Major | . | Jim Brennan | Jim Brennan |
| [YARN-8679](https://issues.apache.org/jira/browse/YARN-8679) | [ATSv2] If HBase cluster is down for long time, high chances that NM ContainerManager dispatcher get blocked |  Major | . | Rohith Sharma K S | Wangda Tan |
| [HDFS-13772](https://issues.apache.org/jira/browse/HDFS-13772) | Erasure coding: Unnecessary NameNode Logs displaying for Enabling/Disabling Erasure coding policies which are already enabled/disabled |  Trivial | erasure-coding | Souryakanta Dwivedy | Ayush Saxena |
| [YARN-8649](https://issues.apache.org/jira/browse/YARN-8649) | NPE in localizer hearbeat processing if a container is killed while localizing |  Major | . | lujie | lujie |
| [YARN-8719](https://issues.apache.org/jira/browse/YARN-8719) | Typo correction for yarn configuration in OpportunisticContainers(federation) docs |  Major | documentation, federation | Y. SREENIVASULU REDDY | Y. SREENIVASULU REDDY |
| [YARN-8675](https://issues.apache.org/jira/browse/YARN-8675) | Setting hostname of docker container breaks with "host" networking mode for Apps which do not run as a YARN service |  Major | . | Yesha Vora | Suma Shivaprasad |
| [HDFS-13858](https://issues.apache.org/jira/browse/HDFS-13858) | RBF: Add check to have single valid argument to safemode command |  Major | federation | Soumyapn | Ayush Saxena |
| [HDFS-13731](https://issues.apache.org/jira/browse/HDFS-13731) | ReencryptionUpdater fails with ConcurrentModificationException during processCheckpoints |  Major | encryption | Xiao Chen | Zsolt Venczel |
| [YARN-8723](https://issues.apache.org/jira/browse/YARN-8723) | Fix a typo in CS init error message when resource calculator is not correctly set |  Minor | . | Weiwei Yang | Abhishek Modi |
| [HADOOP-15705](https://issues.apache.org/jira/browse/HADOOP-15705) | Typo in the definition of "stable" in the interface classification |  Minor | . | Daniel Templeton | Daniel Templeton |
| [HDFS-13863](https://issues.apache.org/jira/browse/HDFS-13863) | FsDatasetImpl should log DiskOutOfSpaceException |  Major | hdfs | Fei Hui | Fei Hui |
| [HADOOP-15698](https://issues.apache.org/jira/browse/HADOOP-15698) | KMS log4j is not initialized properly at startup |  Major | kms | Kitti Nanasi | Kitti Nanasi |
| [HADOOP-15680](https://issues.apache.org/jira/browse/HADOOP-15680) | ITestNativeAzureFileSystemConcurrencyLive times out |  Major | . | Andras Bokor | Andras Bokor |
| [HADOOP-15706](https://issues.apache.org/jira/browse/HADOOP-15706) | Typo in compatibility doc: SHOUD -\> SHOULD |  Trivial | . | Daniel Templeton | Laszlo Kollar |
| [HDFS-13027](https://issues.apache.org/jira/browse/HDFS-13027) | Handle possible NPEs due to deleted blocks in race condition |  Major | namenode | Vinayakumar B | Vinayakumar B |
| [YARN-8535](https://issues.apache.org/jira/browse/YARN-8535) | Fix DistributedShell unit tests |  Major | distributed-shell, timelineservice | Eric Yang | Abhishek Modi |
| [HDFS-13867](https://issues.apache.org/jira/browse/HDFS-13867) | RBF: Add validation for max arguments for Router admin ls, clrQuota, setQuota, rm and nameservice commands |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-13774](https://issues.apache.org/jira/browse/HDFS-13774) | EC: "hdfs ec -getPolicy" is not retrieving policy details when the special REPLICATION policy set on the directory |  Minor | erasure-coding | Souryakanta Dwivedy | Ayush Saxena |
| [HADOOP-10219](https://issues.apache.org/jira/browse/HADOOP-10219) | ipc.Client.setupIOstreams() needs to check for ClientCache.stopClient requested shutdowns |  Major | ipc | Steve Loughran | Kihwal Lee |
| [HDFS-13815](https://issues.apache.org/jira/browse/HDFS-13815) | RBF: Add check to order command |  Major | federation | Soumyapn | Ranith Sardar |
| [HADOOP-15696](https://issues.apache.org/jira/browse/HADOOP-15696) | KMS performance regression due to too many open file descriptors after Jetty migration |  Blocker | kms | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [MAPREDUCE-7131](https://issues.apache.org/jira/browse/MAPREDUCE-7131) | Job History Server has race condition where it moves files from intermediate to finished but thinks file is in intermediate |  Major | . | Anthony Hsu | Anthony Hsu |
| [HDFS-13836](https://issues.apache.org/jira/browse/HDFS-13836) | RBF: Handle mount table znode with null value |  Major | federation, hdfs | yanghuafeng | yanghuafeng |
| [YARN-8751](https://issues.apache.org/jira/browse/YARN-8751) | Container-executor permission check errors cause the NM to be marked unhealthy |  Critical | . | Shane Kumpf | Craig Condit |
| [HDFS-13862](https://issues.apache.org/jira/browse/HDFS-13862) | RBF: Router logs are not capturing few of the dfsrouteradmin commands |  Major | . | Soumyapn | Ayush Saxena |
| [HDFS-12716](https://issues.apache.org/jira/browse/HDFS-12716) |  'dfs.datanode.failed.volumes.tolerated' to support minimum number of volumes to be available |  Major | datanode | usharani | Ranith Sardar |
| [HDFS-13895](https://issues.apache.org/jira/browse/HDFS-13895) | EC: Fix Intermittent Failure in TestDFSStripedOutputStreamWithFailureWithRandomECPolicy |  Major | erasure-coding | Ayush Saxena | Ayush Saxena |
| [YARN-8709](https://issues.apache.org/jira/browse/YARN-8709) | CS preemption monitor always fails since one under-served queue was deleted |  Major | capacityscheduler, scheduler preemption | Tao Yang | Tao Yang |
| [HDFS-13051](https://issues.apache.org/jira/browse/HDFS-13051) | Fix dead lock during async editlog rolling if edit queue is full |  Major | namenode | zhangwei | Daryn Sharp |
| [YARN-8630](https://issues.apache.org/jira/browse/YARN-8630) | ATSv2 REST APIs should honor filter-entity-list-by-user in non-secure cluster when ACls are enabled |  Major | . | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-8729](https://issues.apache.org/jira/browse/YARN-8729) | Node status updater thread could be lost after it is restarted |  Critical | nodemanager | Tao Yang | Tao Yang |
| [HDFS-13914](https://issues.apache.org/jira/browse/HDFS-13914) | Fix DN UI logs link broken when https is enabled after HDFS-13902 |  Minor | datanode | Jianfei Jiang | Jianfei Jiang |
| [MAPREDUCE-7133](https://issues.apache.org/jira/browse/MAPREDUCE-7133) | History Server task attempts REST API returns invalid data |  Major | jobhistoryserver | Oleksandr Shevchenko | Oleksandr Shevchenko |
| [HDFS-13838](https://issues.apache.org/jira/browse/HDFS-13838) | WebHdfsFileSystem.getFileStatus() won't return correct "snapshot enabled" status |  Major | hdfs, webhdfs | Siyao Meng | Siyao Meng |
| [HADOOP-15733](https://issues.apache.org/jira/browse/HADOOP-15733) | Correct the log when Invalid emptier Interval configured |  Major | trash | Harshakiran Reddy | Ayush Saxena |
| [YARN-8720](https://issues.apache.org/jira/browse/YARN-8720) | CapacityScheduler does not enforce max resource allocation check at queue level |  Major | capacity scheduler, capacityscheduler, resourcemanager | Tarun Parimi | Tarun Parimi |
| [YARN-8782](https://issues.apache.org/jira/browse/YARN-8782) | Fix exception message in Resource.throwExceptionWhenArrayOutOfBound |  Minor | . | Szilard Nemeth | Gergely Pollak |
| [HDFS-13844](https://issues.apache.org/jira/browse/HDFS-13844) | Fix the fmt\_bytes function in the dfs-dust.js |  Minor | hdfs, ui | yanghuafeng | yanghuafeng |
| [YARN-8726](https://issues.apache.org/jira/browse/YARN-8726) | [UI2] YARN UI2 is not accessible when config.env file failed to load |  Critical | yarn-ui-v2 | Akhil PB | Akhil PB |
| [YARN-8652](https://issues.apache.org/jira/browse/YARN-8652) | [UI2] YARN UI2 breaks if getUserInfo REST API is not available in older versions. |  Critical | . | Akhil PB | Akhil PB |
| [YARN-8787](https://issues.apache.org/jira/browse/YARN-8787) | Fix broken list items in PlacementConstraints documentation |  Minor | documentation | Masahiro Tanaka | Masahiro Tanaka |
| [HDFS-11719](https://issues.apache.org/jira/browse/HDFS-11719) | Arrays.fill() wrong index in BlockSender.readChecksum() exception handling |  Major | datanode | Tao Zhang | Tao Zhang |
| [YARN-8648](https://issues.apache.org/jira/browse/YARN-8648) | Container cgroups are leaked when using docker |  Major | . | Jim Brennan | Jim Brennan |
| [HADOOP-15755](https://issues.apache.org/jira/browse/HADOOP-15755) | StringUtils#createStartupShutdownMessage throws NPE when args is null |  Major | . | Lokesh Jain | Dinesh Chitlangia |
| [MAPREDUCE-3801](https://issues.apache.org/jira/browse/MAPREDUCE-3801) | org.apache.hadoop.mapreduce.v2.app.TestRuntimeEstimators.testExponentialEstimator fails intermittently |  Major | mrv2 | Robert Joseph Evans | Jason Lowe |
| [MAPREDUCE-7137](https://issues.apache.org/jira/browse/MAPREDUCE-7137) | MRAppBenchmark.benchmark1() fails with NullPointerException |  Minor | test | Oleksandr Shevchenko | Oleksandr Shevchenko |
| [MAPREDUCE-7138](https://issues.apache.org/jira/browse/MAPREDUCE-7138) | ThrottledContainerAllocator in MRAppBenchmark should implement RMHeartbeatHandler |  Minor | test | Oleksandr Shevchenko | Oleksandr Shevchenko |
| [HDFS-13886](https://issues.apache.org/jira/browse/HDFS-13886) | HttpFSFileSystem.getFileStatus() doesn't return "snapshot enabled" bit |  Major | httpfs | Siyao Meng | Siyao Meng |
| [HDFS-13868](https://issues.apache.org/jira/browse/HDFS-13868) | WebHDFS: GETSNAPSHOTDIFF API NPE when param "snapshotname" is given but "oldsnapshotname" is not. |  Major | hdfs, webhdfs | Siyao Meng | Pranay Singh |
| [YARN-8771](https://issues.apache.org/jira/browse/YARN-8771) | CapacityScheduler fails to unreserve when cluster resource contains empty resource type |  Critical | capacityscheduler | Tao Yang | Tao Yang |
| [HDFS-13908](https://issues.apache.org/jira/browse/HDFS-13908) | TestDataNodeMultipleRegistrations is flaky |  Major | . | Íñigo Goiri | Ayush Saxena |
| [HADOOP-15684](https://issues.apache.org/jira/browse/HADOOP-15684) | triggerActiveLogRoll stuck on dead name node, when ConnectTimeoutException happens. |  Critical | ha | Rong Tang | Rong Tang |
| [HADOOP-15772](https://issues.apache.org/jira/browse/HADOOP-15772) | Remove the 'Path ... should be specified as a URI' warnings on startup |  Major | conf | Arpit Agarwal | Ayush Saxena |
| [YARN-8784](https://issues.apache.org/jira/browse/YARN-8784) | DockerLinuxContainerRuntime prevents access to distributed cache entries on a full disk |  Major | nodemanager | Jason Lowe | Eric Badger |
| [HADOOP-15736](https://issues.apache.org/jira/browse/HADOOP-15736) | Trash : Negative Value For Deletion Interval Leads To Abnormal Behaviour. |  Major | . | Ayush Saxena | Ayush Saxena |
| [YARN-8628](https://issues.apache.org/jira/browse/YARN-8628) | [UI2] Few duplicated or inconsistent information displayed in UI2 |  Major | yarn-ui-v2 | Akhil PB | Akhil PB |
| [YARN-8742](https://issues.apache.org/jira/browse/YARN-8742) | [UI2] Container logs on Application / Service pages on UI2 are not available many case, improve error messages in such cases. |  Major | yarn-ui-v2 | Akhil PB | Akhil PB |
| [MAPREDUCE-7125](https://issues.apache.org/jira/browse/MAPREDUCE-7125) | JobResourceUploader creates LocalFileSystem when it's not necessary |  Major | job submission | Peter Cseh | Peter Cseh |
| [YARN-8815](https://issues.apache.org/jira/browse/YARN-8815) | RM fails to recover finished unmanaged AM |  Critical | . | Rakesh Shah | Bibin A Chundatt |
| [YARN-8752](https://issues.apache.org/jira/browse/YARN-8752) | yarn-registry.md has wrong word ong-lived, it should be long-lived |  Trivial | documentation | leiqiang | leiqiang |
| [YARN-8824](https://issues.apache.org/jira/browse/YARN-8824) | App Nodelabel missed after  RM restart for finished apps |  Major | . | Bibin A Chundatt | Bibin A Chundatt |
| [HDFS-13840](https://issues.apache.org/jira/browse/HDFS-13840) | RBW Blocks which are having less GS should be added to Corrupt |  Minor | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-8804](https://issues.apache.org/jira/browse/YARN-8804) | resourceLimits may be wrongly calculated when leaf-queue is blocked in cluster with 3+ level queues |  Critical | capacityscheduler | Tao Yang | Tao Yang |
| [YARN-8774](https://issues.apache.org/jira/browse/YARN-8774) | Memory leak when CapacityScheduler allocates from reserved container with non-default label |  Critical | capacityscheduler | Tao Yang | Tao Yang |
| [HADOOP-15817](https://issues.apache.org/jira/browse/HADOOP-15817) | Reuse Object Mapper in KMSJSONReader |  Major | kms | Jonathan Eagles | Jonathan Eagles |
| [YARN-8844](https://issues.apache.org/jira/browse/YARN-8844) | TestNMProxy unit test is failing |  Major | yarn | Eric Yang | Eric Yang |
| [HDFS-13957](https://issues.apache.org/jira/browse/HDFS-13957) | Fix incorrect option used in description of InMemoryAliasMap |  Minor | documentation | Yiqun Lin | Yiqun Lin |
| [YARN-7957](https://issues.apache.org/jira/browse/YARN-7957) | [UI2] YARN service delete option disappears after stopping application |  Critical | yarn-ui-v2 | Yesha Vora | Akhil PB |
| [YARN-8797](https://issues.apache.org/jira/browse/YARN-8797) | [UI2] Improve error pages in new YARN UI |  Major | yarn-ui-v2 | Akhil PB | Akhil PB |
| [HADOOP-15820](https://issues.apache.org/jira/browse/HADOOP-15820) | ZStandardDecompressor native code sets an integer field as a long |  Blocker | . | Jason Lowe | Jason Lowe |
| [HDFS-13964](https://issues.apache.org/jira/browse/HDFS-13964) | RBF: TestRouterWebHDFSContractAppend fails with No Active Namenode under nameservice |  Major | . | Ayush Saxena | Ayush Saxena |
| [YARN-7825](https://issues.apache.org/jira/browse/YARN-7825) | [UI2] Maintain constant horizontal application info bar for Application Attempt page |  Major | yarn-ui-v2 | Yesha Vora | Akhil PB |
| [HDFS-13768](https://issues.apache.org/jira/browse/HDFS-13768) |  Adding replicas to volume map makes DataNode start slowly |  Major | . | Yiqun Lin | Surendra Singh Lilhore |
| [HDFS-13962](https://issues.apache.org/jira/browse/HDFS-13962) | Add null check for add-replica pool to avoid lock acquiring |  Major | . | Yiqun Lin | Surendra Singh Lilhore |
| [YARN-8853](https://issues.apache.org/jira/browse/YARN-8853) | [UI2] Application Attempts tab is not shown correctly when there are no attempts |  Major | . | Charan Hebri | Akhil PB |
| [YARN-8845](https://issues.apache.org/jira/browse/YARN-8845) | hadoop.registry.rm.enabled is not used |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13926](https://issues.apache.org/jira/browse/HDFS-13926) | ThreadLocal aggregations for FileSystem.Statistics are incorrect with striped reads |  Major | erasure-coding | Xiao Chen | Hrishikesh Gadre |
| [YARN-8666](https://issues.apache.org/jira/browse/YARN-8666) | [UI2] Remove application tab from YARN Queue Page |  Major | yarn-ui-v2 | Yesha Vora | Yesha Vora |
| [YARN-8753](https://issues.apache.org/jira/browse/YARN-8753) | [UI2] Lost nodes representation missing from Nodemanagers Chart |  Major | yarn-ui-v2 | Yesha Vora | Yesha Vora |
| [HADOOP-15679](https://issues.apache.org/jira/browse/HADOOP-15679) | ShutdownHookManager shutdown time needs to be configurable & extended |  Major | util | Steve Loughran | Steve Loughran |
| [HDFS-13945](https://issues.apache.org/jira/browse/HDFS-13945) | TestDataNodeVolumeFailure is Flaky |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-13802](https://issues.apache.org/jira/browse/HDFS-13802) | RBF: Remove FSCK from Router Web UI |  Major | . | Fei Hui | Fei Hui |
| [YARN-8869](https://issues.apache.org/jira/browse/YARN-8869) | YARN Service Client might not work correctly with RM REST API for Kerberos authentication |  Blocker | . | Eric Yang | Eric Yang |
| [MAPREDUCE-7132](https://issues.apache.org/jira/browse/MAPREDUCE-7132) | JobSplitWriter prints unnecessary warnings if EC(RS10,4) is used |  Major | client, mrv2 | Peter Bacsko | Peter Bacsko |
| [YARN-8892](https://issues.apache.org/jira/browse/YARN-8892) | YARN UI2 doc changes to update security status (verified under security environment) |  Blocker | . | Sunil Govindan | Sunil Govindan |
| [YARN-8810](https://issues.apache.org/jira/browse/YARN-8810) | Yarn Service: discrepancy between hashcode and equals of ConfigFile |  Minor | . | Chandni Singh | Chandni Singh |
| [HADOOP-15802](https://issues.apache.org/jira/browse/HADOOP-15802) | start-build-env.sh creates an invalid /etc/sudoers.d/hadoop-build-${USER\_ID} file entry |  Minor | build | Jon Boone | Jon Boone |
| [HADOOP-15861](https://issues.apache.org/jira/browse/HADOOP-15861) | Move DelegationTokenIssuer to the right path |  Blocker | security | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-15859](https://issues.apache.org/jira/browse/HADOOP-15859) | ZStandardDecompressor.c mistakes a class for an instance |  Blocker | . | Ben Lau | Jason Lowe |
| [HDFS-14000](https://issues.apache.org/jira/browse/HDFS-14000) | RBF: Documentation should reflect right scripts for v3.0 and above |  Major | . | CR Hota | CR Hota |
| [YARN-8868](https://issues.apache.org/jira/browse/YARN-8868) | Set HTTPOnly attribute to Cookie |  Major | . | Chandni Singh | Chandni Singh |
| [HDFS-14003](https://issues.apache.org/jira/browse/HDFS-14003) | Fix findbugs warning in trunk for FSImageFormatPBINode |  Major | . | Yiqun Lin | Yiqun Lin |
| [HDFS-14005](https://issues.apache.org/jira/browse/HDFS-14005) | RBF: Web UI update to bootstrap-3.3.7 |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-8910](https://issues.apache.org/jira/browse/YARN-8910) | Misleading log statement in NM when max retries is -1 |  Minor | . | Chandni Singh | Chandni Singh |
| [HADOOP-15850](https://issues.apache.org/jira/browse/HADOOP-15850) | CopyCommitter#concatFileChunks should check that the blocks per chunk is not 0 |  Critical | tools/distcp | Ted Yu | Ted Yu |
| [YARN-7502](https://issues.apache.org/jira/browse/YARN-7502) | Nodemanager restart docs should describe nodemanager supervised property |  Major | documentation | Jason Lowe | Suma Shivaprasad |
| [HADOOP-15866](https://issues.apache.org/jira/browse/HADOOP-15866) | Renamed HADOOP\_SECURITY\_GROUP\_SHELL\_COMMAND\_TIMEOUT keys break compatibility |  Blocker | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-8826](https://issues.apache.org/jira/browse/YARN-8826) | Fix lingering timeline collector after serviceStop in TimelineCollectorManager |  Trivial | ATSv2 | Prabha Manepalli | Prabha Manepalli |
| [HADOOP-15822](https://issues.apache.org/jira/browse/HADOOP-15822) | zstd compressor can fail with a small output buffer |  Major | . | Jason Lowe | Jason Lowe |
| [HDFS-14021](https://issues.apache.org/jira/browse/HDFS-14021) | TestReconstructStripedBlocksWithRackAwareness#testReconstructForNotEnoughRacks fails intermittently |  Major | erasure-coding, test | Xiao Chen | Xiao Chen |
| [MAPREDUCE-7151](https://issues.apache.org/jira/browse/MAPREDUCE-7151) | RMContainerAllocator#handleJobPriorityChange expects application\_priority always |  Major | . | Bibin A Chundatt | Bilwa S T |
| [HADOOP-14445](https://issues.apache.org/jira/browse/HADOOP-14445) | Use DelegationTokenIssuer to create KMS delegation tokens that can authenticate to all KMS instances |  Major | kms | Wei-Chiu Chuang | Xiao Chen |
| [HDFS-14028](https://issues.apache.org/jira/browse/HDFS-14028) | HDFS OIV temporary dir deletes folder |  Major | hdfs | Adam Antal | Adam Antal |
| [HDFS-14027](https://issues.apache.org/jira/browse/HDFS-14027) | DFSStripedOutputStream should implement both hsync methods |  Critical | erasure-coding | Xiao Chen | Xiao Chen |
| [YARN-8950](https://issues.apache.org/jira/browse/YARN-8950) | Compilation fails with dependency convergence error for hbase.profile=2.0 |  Blocker | . | Rohith Sharma K S | Rohith Sharma K S |
| [HADOOP-15899](https://issues.apache.org/jira/browse/HADOOP-15899) | Update AWS Java SDK versions in NOTICE.txt |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-15900](https://issues.apache.org/jira/browse/HADOOP-15900) | Update JSch versions in LICENSE.txt |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [HDFS-14043](https://issues.apache.org/jira/browse/HDFS-14043) | Tolerate corrupted seen\_txid file |  Major | hdfs, namenode | Lukas Majercak | Lukas Majercak |
| [YARN-8858](https://issues.apache.org/jira/browse/YARN-8858) | CapacityScheduler should respect maximum node resource when per-queue maximum-allocation is being used. |  Major | . | Sumana Sathish | Wangda Tan |
| [YARN-8970](https://issues.apache.org/jira/browse/YARN-8970) | Improve the debug message in CS#allocateContainerOnSingleNode |  Trivial | . | Weiwei Yang | Zhankun Tang |
| [YARN-8865](https://issues.apache.org/jira/browse/YARN-8865) | RMStateStore contains large number of expired RMDelegationToken |  Major | resourcemanager | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [HDFS-14048](https://issues.apache.org/jira/browse/HDFS-14048) | DFSOutputStream close() throws exception on subsequent call after DataNode restart |  Major | hdfs-client | Erik Krogen | Erik Krogen |
| [MAPREDUCE-7156](https://issues.apache.org/jira/browse/MAPREDUCE-7156) | NullPointerException when reaching max shuffle connections |  Major | mrv2 | Peter Bacsko | Peter Bacsko |
| [YARN-8866](https://issues.apache.org/jira/browse/YARN-8866) | Fix a parsing error for crossdomain.xml |  Major | build, yarn-ui-v2 | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-14039](https://issues.apache.org/jira/browse/HDFS-14039) | ec -listPolicies doesn't show correct state for the default policy when the default is not RS(6,3) |  Major | erasure-coding | Xiao Chen | Kitti Nanasi |
| [HADOOP-15916](https://issues.apache.org/jira/browse/HADOOP-15916) | Upgrade Maven Surefire plugin to 3.0.0-M1 |  Blocker | build | Akira Ajisaka | Akira Ajisaka |
| [YARN-9002](https://issues.apache.org/jira/browse/YARN-9002) | YARN Service keytab does not support s3, wasb, gs and is restricted to HDFS and local filesystem only |  Major | yarn-native-services | Gour Saha | Gour Saha |
| [YARN-8233](https://issues.apache.org/jira/browse/YARN-8233) | NPE in CapacityScheduler#tryCommit when handling allocate/reserve proposal whose allocatedOrReservedContainer is null |  Critical | capacityscheduler | Tao Yang | Tao Yang |
| [HDFS-14065](https://issues.apache.org/jira/browse/HDFS-14065) | Failed Storage Locations shows nothing in the Datanode Volume Failures |  Major | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-15923](https://issues.apache.org/jira/browse/HADOOP-15923) | create-release script should set max-cache-ttl as well as default-cache-ttl for gpg-agent |  Blocker | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-15912](https://issues.apache.org/jira/browse/HADOOP-15912) | start-build-env.sh still creates an invalid /etc/sudoers.d/hadoop-build-${USER\_ID} file entry after HADOOP-15802 |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-15869](https://issues.apache.org/jira/browse/HADOOP-15869) | BlockDecompressorStream#decompress should not return -1 in case of IOException. |  Major | . | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [MAPREDUCE-7158](https://issues.apache.org/jira/browse/MAPREDUCE-7158) | Inefficient Flush Logic in JobHistory EventWriter |  Major | . | Zichen Sun | Zichen Sun |
| [HADOOP-15930](https://issues.apache.org/jira/browse/HADOOP-15930) | Exclude MD5 checksum files from release artifact |  Critical | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-15925](https://issues.apache.org/jira/browse/HADOOP-15925) | The config and log of gpg-agent are removed in create-release script |  Major | build | Akira Ajisaka | Dinesh Chitlangia |
| [HDFS-13963](https://issues.apache.org/jira/browse/HDFS-13963) | NN UI is broken with IE11 |  Minor | namenode, ui | Daisuke Kobayashi | Ayush Saxena |
| [HDFS-14056](https://issues.apache.org/jira/browse/HDFS-14056) | Fix error messages in HDFS-12716 |  Minor | hdfs | Adam Antal | Ayush Saxena |
| [MAPREDUCE-7162](https://issues.apache.org/jira/browse/MAPREDUCE-7162) | TestEvents#testEvents fails |  Critical | jobhistoryserver, test | Zhaohui Xin | Zhaohui Xin |
| [YARN-9056](https://issues.apache.org/jira/browse/YARN-9056) | Yarn Service Upgrade: Instance state changes from UPGRADING to READY without performing a readiness check |  Critical | . | Chandni Singh | Chandni Singh |
| [YARN-8812](https://issues.apache.org/jira/browse/YARN-8812) | Containers fail during creating a symlink which started with hyphen for a resource file |  Minor | . | Oleksandr Shevchenko | Oleksandr Shevchenko |
| [YARN-9067](https://issues.apache.org/jira/browse/YARN-9067) | YARN Resource Manager is running OOM because of leak of Configuration Object |  Major | yarn-native-services | Eric Yang | Eric Yang |
| [MAPREDUCE-7165](https://issues.apache.org/jira/browse/MAPREDUCE-7165) | mapred-site.xml is misformatted in single node setup document |  Major | documentation | Akira Ajisaka | Zhaohui Xin |
| [HADOOP-15970](https://issues.apache.org/jira/browse/HADOOP-15970) | Upgrade plexus-utils from 2.0.5 to 3.1.0 |  Major | security | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-15974](https://issues.apache.org/jira/browse/HADOOP-15974) | Upgrade Curator version to 2.13.0 to fix ZK tests |  Major | . | Jason Lowe | Akira Ajisaka |
| [YARN-9071](https://issues.apache.org/jira/browse/YARN-9071) | NM and service AM don't have updated status for reinitialized containers |  Critical | . | Billie Rinaldi | Chandni Singh |
| [MAPREDUCE-7159](https://issues.apache.org/jira/browse/MAPREDUCE-7159) | FrameworkUploader: ensure proper permissions of generated framework tar.gz if restrictive umask is used |  Major | mrv2 | Peter Bacsko | Peter Bacsko |
| [YARN-9009](https://issues.apache.org/jira/browse/YARN-9009) | Fix flaky test TestEntityGroupFSTimelineStore.testCleanLogs |  Minor | . | OrDTesters | OrDTesters |
| [MAPREDUCE-7170](https://issues.apache.org/jira/browse/MAPREDUCE-7170) | Doc typo in PluggableShuffleAndPluggableSort.md |  Minor | documentation | Zhaohui Xin | Zhaohui Xin |
| [YARN-9040](https://issues.apache.org/jira/browse/YARN-9040) | LevelDBCacheTimelineStore in ATS 1.5 leaks native memory |  Major | timelineserver | Tarun Parimi | Tarun Parimi |
| [YARN-9084](https://issues.apache.org/jira/browse/YARN-9084) | Service Upgrade: With default readiness check, the status of upgrade is reported to be successful prematurely |  Major | . | Chandni Singh | Chandni Singh |
| [HDFS-13661](https://issues.apache.org/jira/browse/HDFS-13661) | Ls command with e option fails when the filesystem is not HDFS |  Major | erasure-coding, tools | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-15973](https://issues.apache.org/jira/browse/HADOOP-15973) | Configuration: Included properties are not cached if resource is a stream |  Critical | . | Eric Payne | Eric Payne |
| [YARN-9154](https://issues.apache.org/jira/browse/YARN-9154) | Fix itemization in YARN service quickstart document |  Minor | documentation | Akira Ajisaka | Ayush Saxena |
| [HDFS-14166](https://issues.apache.org/jira/browse/HDFS-14166) | Ls with -e option not giving the result in proper format |  Major | . | Soumyapn | Shubham Dewan |
| [HDFS-14046](https://issues.apache.org/jira/browse/HDFS-14046) | In-Maintenance ICON is missing in datanode info page |  Major | datanode | Harshakiran Reddy | Ranith Sardar |
| [MAPREDUCE-7174](https://issues.apache.org/jira/browse/MAPREDUCE-7174) | MapReduce example wordmedian should handle generic options |  Major | . | Fei Hui | Fei Hui |
| [YARN-9164](https://issues.apache.org/jira/browse/YARN-9164) | Shutdown NM may cause NPE when opportunistic container scheduling is enabled |  Critical | . | lujie | lujie |
| [HADOOP-15997](https://issues.apache.org/jira/browse/HADOOP-15997) | KMS client uses wrong UGI after HADOOP-14445 |  Blocker | kms | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-15992](https://issues.apache.org/jira/browse/HADOOP-15992) | JSON License is included in the transitive dependency of aliyun-sdk-oss 3.0.0 |  Blocker | . | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-16030](https://issues.apache.org/jira/browse/HADOOP-16030) | AliyunOSS: bring fixes back from HADOOP-15671 |  Blocker | fs/oss | wujinhu | wujinhu |
| [YARN-9173](https://issues.apache.org/jira/browse/YARN-9173) | FairShare calculation broken for large values after YARN-8833 |  Major | fairscheduler | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [YARN-8833](https://issues.apache.org/jira/browse/YARN-8833) | Avoid potential integer overflow when computing fair shares |  Major | fairscheduler | liyakun | liyakun |
| [YARN-8747](https://issues.apache.org/jira/browse/YARN-8747) | [UI2] YARN UI2 page loading failed due to js error under some time zone configuration |  Critical | webapp | collinma | collinma |
| [YARN-9194](https://issues.apache.org/jira/browse/YARN-9194) | Invalid event: REGISTERED and LAUNCH\_FAILED at FAILED, and NullPointerException happens in RM while shutdown a NM |  Critical | . | lujie | lujie |
| [YARN-9204](https://issues.apache.org/jira/browse/YARN-9204) |  RM fails to start if absolute resource is specified for partition capacity in CS queues |  Blocker | yarn | Jiandan Yang | Jiandan Yang |
| [YARN-9210](https://issues.apache.org/jira/browse/YARN-9210) | RM nodes web page can not display node info |  Blocker | yarn | Jiandan Yang | Jiandan Yang |
| [YARN-9205](https://issues.apache.org/jira/browse/YARN-9205) | When using custom resource type, application will fail to run due to the CapacityScheduler throws InvalidResourceRequestException(GREATER\_THEN\_MAX\_ALLOCATION) |  Critical | . | Zhankun Tang | Zhankun Tang |
| [HADOOP-15781](https://issues.apache.org/jira/browse/HADOOP-15781) | S3A assumed role tests failing due to changed error text in AWS exceptions |  Major | fs/s3, test | Steve Loughran | Steve Loughran |
| [HDFS-14228](https://issues.apache.org/jira/browse/HDFS-14228) | Incorrect getSnapshottableDirListing() javadoc |  Major | snapshots | Wei-Chiu Chuang | Dinesh Chitlangia |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-8907](https://issues.apache.org/jira/browse/YARN-8907) | Modify a logging message in TestCapacityScheduler |  Trivial | . | Zhankun Tang | Zhankun Tang |
| [YARN-8904](https://issues.apache.org/jira/browse/YARN-8904) | TestRMDelegationTokens can fail in testRMDTMasterKeyStateOnRollingMasterKey |  Minor | test | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [YARN-8944](https://issues.apache.org/jira/browse/YARN-8944) | TestContainerAllocation.testUserLimitAllocationMultipleContainers failure after YARN-8896 |  Minor | capacity scheduler | Wilfred Spiegelenburg | Wilfred Spiegelenburg |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-13743](https://issues.apache.org/jira/browse/HDFS-13743) | RBF: Router throws NullPointerException due to the invalid initialization of MountTableResolver |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-13583](https://issues.apache.org/jira/browse/HDFS-13583) | RBF: Router admin clrQuota is not synchronized with nameservice |  Major | . | Dibyendu Karmakar | Dibyendu Karmakar |
| [YARN-8263](https://issues.apache.org/jira/browse/YARN-8263) | DockerClient still touches hadoop.tmp.dir |  Minor | . | Jason Lowe | Craig Condit |
| [YARN-8287](https://issues.apache.org/jira/browse/YARN-8287) | Update documentation and yarn-default related to the Docker runtime |  Minor | . | Shane Kumpf | Craig Condit |
| [YARN-8624](https://issues.apache.org/jira/browse/YARN-8624) | Cleanup ENTRYPOINT documentation |  Minor | . | Craig Condit | Craig Condit |
| [YARN-8136](https://issues.apache.org/jira/browse/YARN-8136) | Add version attribute to site doc examples and quickstart |  Major | site | Gour Saha | Eric Yang |
| [YARN-8588](https://issues.apache.org/jira/browse/YARN-8588) | Logging improvements for better debuggability |  Major | . | Suma Shivaprasad | Suma Shivaprasad |
| [YARN-8520](https://issues.apache.org/jira/browse/YARN-8520) | Document best practice for user management |  Major | documentation, yarn | Eric Yang | Eric Yang |
| [HDFS-13750](https://issues.apache.org/jira/browse/HDFS-13750) | RBF: Router ID in RouterRpcClient is always null |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-8129](https://issues.apache.org/jira/browse/YARN-8129) | Improve error message for invalid value in fields attribute |  Minor | ATSv2 | Charan Hebri | Abhishek Modi |
| [HDFS-13848](https://issues.apache.org/jira/browse/HDFS-13848) | Refactor NameNode failover proxy providers |  Major | ha, hdfs-client | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-13634](https://issues.apache.org/jira/browse/HDFS-13634) | RBF: Configurable value in xml for async connection request queue size. |  Major | federation | CR Hota | CR Hota |
| [YARN-8642](https://issues.apache.org/jira/browse/YARN-8642) | Add support for tmpfs mounts with the Docker runtime |  Major | . | Shane Kumpf | Craig Condit |
| [HADOOP-15107](https://issues.apache.org/jira/browse/HADOOP-15107) | Stabilize/tune S3A committers; review correctness & docs |  Blocker | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-15731](https://issues.apache.org/jira/browse/HADOOP-15731) | TestDistributedShell fails on Windows |  Major | . | Botong Huang | Botong Huang |
| [HDFS-13237](https://issues.apache.org/jira/browse/HDFS-13237) | [Documentation] RBF: Mount points across multiple subclusters |  Minor | . | Íñigo Goiri | Íñigo Goiri |
| [HADOOP-15759](https://issues.apache.org/jira/browse/HADOOP-15759) | AliyunOSS: update oss-sdk version to 3.0.0 |  Major | fs/oss | wujinhu | wujinhu |
| [HADOOP-15748](https://issues.apache.org/jira/browse/HADOOP-15748) | S3 listing inconsistency can raise NPE in globber |  Major | fs | Steve Loughran | Steve Loughran |
| [YARN-8801](https://issues.apache.org/jira/browse/YARN-8801) | java doc comments in docker-util.h is confusing |  Minor | . | Zian Chen | Zian Chen |
| [HADOOP-15671](https://issues.apache.org/jira/browse/HADOOP-15671) | AliyunOSS: Support Assume Roles in AliyunOSS |  Major | fs/oss | wujinhu | wujinhu |
| [YARN-8623](https://issues.apache.org/jira/browse/YARN-8623) | Update Docker examples to use image which exists |  Minor | . | Craig Condit | Craig Condit |
| [HDFS-13790](https://issues.apache.org/jira/browse/HDFS-13790) | RBF: Move ClientProtocol APIs to its own module |  Major | . | Íñigo Goiri | Chao Sun |
| [YARN-8785](https://issues.apache.org/jira/browse/YARN-8785) | Improve the error message when a bind mount is not whitelisted |  Major | . | Simon Prewo | Simon Prewo |
| [YARN-6989](https://issues.apache.org/jira/browse/YARN-6989) | Ensure timeline service v2 codebase gets UGI from HttpServletRequest in a consistent way |  Major | timelineserver | Vrushali C | Abhishek Modi |
| [YARN-5742](https://issues.apache.org/jira/browse/YARN-5742) | Serve aggregated logs of historical apps from timeline service |  Critical | timelineserver | Varun Saxena | Rohith Sharma K S |
| [YARN-8834](https://issues.apache.org/jira/browse/YARN-8834) | Provide Java client for fetching Yarn specific entities from TimelineReader |  Critical | timelinereader | Rohith Sharma K S | Abhishek Modi |
| [HADOOP-15837](https://issues.apache.org/jira/browse/HADOOP-15837) | DynamoDB table Update can fail S3A FS init |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-15839](https://issues.apache.org/jira/browse/HADOOP-15839) | Review + update cloud store sensitive keys in hadoop.security.sensitive-config-keys |  Minor | conf | Steve Loughran | Steve Loughran |
| [YARN-8687](https://issues.apache.org/jira/browse/YARN-8687) | YARN service example is out-dated |  Major | yarn-native-services | Eric Yang | Suma Shivaprasad |
| [YARN-6098](https://issues.apache.org/jira/browse/YARN-6098) | Add documentation for Delete Queue |  Major | capacity scheduler, documentation | Naganarasimha G R | Suma Shivaprasad |
| [HADOOP-15607](https://issues.apache.org/jira/browse/HADOOP-15607) | AliyunOSS: fix duplicated partNumber issue in AliyunOSSBlockOutputStream |  Critical | . | wujinhu | wujinhu |
| [HADOOP-15868](https://issues.apache.org/jira/browse/HADOOP-15868) | AliyunOSS: update document for properties of multiple part download, multiple part upload and directory copy |  Major | fs/oss | wujinhu | wujinhu |
| [HADOOP-15110](https://issues.apache.org/jira/browse/HADOOP-15110) | Gauges are getting logged in exceptions from AutoRenewalThreadForUserCreds |  Minor | metrics, security | Harshakiran Reddy | LiXin Ge |
| [HADOOP-15917](https://issues.apache.org/jira/browse/HADOOP-15917) | AliyunOSS: fix incorrect ReadOps and WriteOps in statistics |  Major | fs/oss | wujinhu | wujinhu |
| [YARN-8303](https://issues.apache.org/jira/browse/YARN-8303) | YarnClient should contact TimelineReader for application/attempt/container report |  Critical | . | Rohith Sharma K S | Abhishek Modi |
| [YARN-8299](https://issues.apache.org/jira/browse/YARN-8299) | Yarn Service Upgrade: Add GET APIs that returns instances matching query params |  Critical | . | Chandni Singh | Chandni Singh |
| [YARN-8160](https://issues.apache.org/jira/browse/YARN-8160) | Yarn Service Upgrade: Support upgrade of service that use docker containers |  Major | . | Chandni Singh | Chandni Singh |
| [YARN-8298](https://issues.apache.org/jira/browse/YARN-8298) | Yarn Service Upgrade: Support express upgrade of a service |  Major | . | Chandni Singh | Chandni Singh |
| [YARN-8986](https://issues.apache.org/jira/browse/YARN-8986) | publish all exposed ports to random ports when using bridge network |  Minor | yarn | Charo Zhang | Charo Zhang |
| [HADOOP-15932](https://issues.apache.org/jira/browse/HADOOP-15932) | Oozie unable to create sharelib in s3a filesystem |  Critical | fs, fs/s3 | Soumitra Sulav | Steve Loughran |
| [YARN-8665](https://issues.apache.org/jira/browse/YARN-8665) | Yarn Service Upgrade:  Support cancelling upgrade |  Major | . | Chandni Singh | Chandni Singh |
| [HADOOP-16009](https://issues.apache.org/jira/browse/HADOOP-16009) | Replace the url of the repository in Apache Hadoop source code |  Major | documentation | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-15323](https://issues.apache.org/jira/browse/HADOOP-15323) | AliyunOSS: Improve copy file performance for AliyunOSSFileSystemStore |  Major | fs/oss | wujinhu | wujinhu |
| [YARN-8822](https://issues.apache.org/jira/browse/YARN-8822) | Nvidia-docker v2 support for YARN GPU feature |  Critical | . | Zhankun Tang | Charo Zhang |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-8545](https://issues.apache.org/jira/browse/YARN-8545) | YARN native service should return container if launch failed |  Critical | . | Wangda Tan | Chandni Singh |
| [HDFS-13788](https://issues.apache.org/jira/browse/HDFS-13788) | Update EC documentation about rack fault tolerance |  Major | documentation, erasure-coding | Xiao Chen | Kitti Nanasi |
| [HADOOP-15816](https://issues.apache.org/jira/browse/HADOOP-15816) | Upgrade Apache Zookeeper version due to security concerns |  Major | . | Boris Vulikh | Akira Ajisaka |
| [HADOOP-15882](https://issues.apache.org/jira/browse/HADOOP-15882) | Upgrade maven-shade-plugin from 2.4.3 to 3.2.0 |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-15815](https://issues.apache.org/jira/browse/HADOOP-15815) | Upgrade Eclipse Jetty version to 9.3.24 |  Major | . | Boris Vulikh | Boris Vulikh |
| [YARN-8488](https://issues.apache.org/jira/browse/YARN-8488) | YARN service/components/instances should have SUCCEEDED/FAILED states |  Major | yarn-native-services | Wangda Tan | Suma Shivaprasad |


