
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

## Release 3.1.2 - Unreleased (as of 2018-09-02)



### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-13448](https://issues.apache.org/jira/browse/HDFS-13448) | HDFS Block Placement - Ignore Locality for First Block Replica |  Minor | block placement, hdfs-client | BELUGA BEHR | BELUGA BEHR |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
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
| [HDFS-13732](https://issues.apache.org/jira/browse/HDFS-13732) | ECAdmin should print the policy name when an EC policy is set |  Trivial | erasure-coding, tools | Soumyapn | Zsolt Venczel |
| [HADOOP-9214](https://issues.apache.org/jira/browse/HADOOP-9214) | Create a new touch command to allow modifying atime and mtime |  Minor | tools | Brian Burton | Hrishikesh Gadre |
| [YARN-8242](https://issues.apache.org/jira/browse/YARN-8242) | YARN NM: OOM error while reading back the state store on recovery |  Critical | yarn | Kanwaljeet Sachdev | Pradeep Ambati |
| [HDFS-13821](https://issues.apache.org/jira/browse/HDFS-13821) | RBF: Add dfs.federation.router.mount-table.cache.enable so that users can disable cache |  Major | hdfs | Fei Hui | Fei Hui |
| [HDFS-13861](https://issues.apache.org/jira/browse/HDFS-13861) | RBF: Illegal Router Admin command leads to printing usage for all commands |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-13831](https://issues.apache.org/jira/browse/HDFS-13831) | Make block increment deletion number configurable |  Major | . | Yiqun Lin | Ryan Wu |
| [YARN-8051](https://issues.apache.org/jira/browse/YARN-8051) | TestRMEmbeddedElector#testCallbackSynchronization is flakey |  Major | test | Robert Kanter | Robert Kanter |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-7773](https://issues.apache.org/jira/browse/YARN-7773) | YARN Federation used Mysql as state store throw exception, Unknown column 'homeSubCluster' in 'field list' |  Blocker | federation | Yiran Wu | Yiran Wu |
| [YARN-8426](https://issues.apache.org/jira/browse/YARN-8426) | Upgrade jquery-ui to 1.12.1 in YARN |  Major | webapp | Sunil Govindan | Sunil Govindan |
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


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-8545](https://issues.apache.org/jira/browse/YARN-8545) | YARN native service should return container if launch failed |  Critical | . | Wangda Tan | Chandni Singh |
| [HDFS-13788](https://issues.apache.org/jira/browse/HDFS-13788) | Update EC documentation about rack fault tolerance |  Major | documentation, erasure-coding | Xiao Chen | Kitti Nanasi |
