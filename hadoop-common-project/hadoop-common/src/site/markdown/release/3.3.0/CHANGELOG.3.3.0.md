
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

## Release 3.3.0 - 2020-07-06



### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-14339](https://issues.apache.org/jira/browse/HDFS-14339) | Inconsistent log level practices in RpcProgramNfs3.java |  Major | nfs | Anuhan Torgonshar | Anuhan Torgonshar |
| [HDFS-15186](https://issues.apache.org/jira/browse/HDFS-15186) | Erasure Coding: Decommission may generate the parity block's content with all 0 in some case |  Critical | datanode, erasure-coding | Yao Guangdong | Yao Guangdong |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-15885](https://issues.apache.org/jira/browse/HADOOP-15885) | Add base64 (urlString) support to DTUtil |  Minor | . | Íñigo Goiri | Íñigo Goiri |
| [HADOOP-15950](https://issues.apache.org/jira/browse/HADOOP-15950) | Failover for LdapGroupsMapping |  Major | common, security | Lukas Majercak | Lukas Majercak |
| [HDFS-14001](https://issues.apache.org/jira/browse/HDFS-14001) | [PROVIDED Storage] bootstrapStandby should manage the InMemoryAliasMap |  Major | . | Íñigo Goiri | Virajith Jalaparti |
| [YARN-8762](https://issues.apache.org/jira/browse/YARN-8762) | [Umbrella] Support Interactive Docker Shell to running Containers |  Major | . | Zian Chen | Eric Yang |
| [HADOOP-15996](https://issues.apache.org/jira/browse/HADOOP-15996) | Plugin interface to support more complex usernames in Hadoop |  Major | security | Eric Yang | Bolke de Bruin |
| [HADOOP-15229](https://issues.apache.org/jira/browse/HADOOP-15229) | Add FileSystem builder-based openFile() API to match createFile(); S3A to implement S3 Select through this API. |  Major | fs, fs/azure, fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-14118](https://issues.apache.org/jira/browse/HDFS-14118) | Support using DNS to resolve nameservices to IP addresses |  Major | . | Fengnan Li | Fengnan Li |
| [HADOOP-16125](https://issues.apache.org/jira/browse/HADOOP-16125) | Support multiple bind users in LdapGroupsMapping |  Major | common, security | Lukas Majercak | Lukas Majercak |
| [YARN-9228](https://issues.apache.org/jira/browse/YARN-9228) | [Umbrella] Docker image life cycle management on HDFS |  Major | . | Eric Yang | Eric Yang |
| [YARN-9016](https://issues.apache.org/jira/browse/YARN-9016) | DocumentStore as a backend for ATSv2 |  Major | ATSv2 | Sushil Ks | Sushil Ks |
| [HDFS-14234](https://issues.apache.org/jira/browse/HDFS-14234) | Limit WebHDFS to specifc user, host, directory triples |  Trivial | webhdfs | Clay B. | Clay B. |
| [HADOOP-16095](https://issues.apache.org/jira/browse/HADOOP-16095) | Support impersonation for AuthenticationFilter |  Major | security | Eric Yang | Eric Yang |
| [HDFS-12345](https://issues.apache.org/jira/browse/HDFS-12345) | Scale testing HDFS NameNode with real metadata and workloads (Dynamometer) |  Major | namenode, test | Zhe Zhang | Erik Krogen |
| [YARN-9473](https://issues.apache.org/jira/browse/YARN-9473) | [Umbrella] Support Vector Engine ( a new accelerator hardware) based on pluggable device framework |  Major | nodemanager | Zhankun Tang | Peter Bacsko |
| [HDFS-13783](https://issues.apache.org/jira/browse/HDFS-13783) | Balancer: make balancer to be a long service process for easy to monitor it. |  Major | balancer & mover | maobaolong | Chen Zhang |
| [HADOOP-16398](https://issues.apache.org/jira/browse/HADOOP-16398) | Exports Hadoop metrics to Prometheus |  Major | metrics | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-16527](https://issues.apache.org/jira/browse/HADOOP-16527) | Add a whitelist of endpoints to skip Kerberos authentication |  Major | security | Akira Ajisaka | Akira Ajisaka |
| [HDFS-12904](https://issues.apache.org/jira/browse/HDFS-12904) | Add DataTransferThrottler to the Datanode transfers |  Minor | datanode | Íñigo Goiri | Lisheng Sun |
| [YARN-9761](https://issues.apache.org/jira/browse/YARN-9761) | Allow overriding application submissions based on server side configs |  Major | . | Jonathan Hung | pralabhkumar |
| [YARN-9808](https://issues.apache.org/jira/browse/YARN-9808) | Zero length files in container log output haven't got a header |  Major | log-aggregation, yarn | Adam Antal | Adam Antal |
| [HADOOP-15691](https://issues.apache.org/jira/browse/HADOOP-15691) | Add PathCapabilities to FS and FC to complement StreamCapabilities |  Major | . | Steve Loughran | Steve Loughran |
| [HADOOP-15616](https://issues.apache.org/jira/browse/HADOOP-15616) | Incorporate Tencent Cloud COS File System Implementation |  Major | fs/cos | Junping Du | Yang Yu |
| [YARN-9760](https://issues.apache.org/jira/browse/YARN-9760) | Support configuring application priorities on a workflow level |  Major | . | Jonathan Hung | Varun Saxena |
| [HDFS-13762](https://issues.apache.org/jira/browse/HDFS-13762) | Support non-volatile storage class memory(SCM) in HDFS cache directives |  Major | caching, datanode | Sammi Chen | Feilong He |
| [HDFS-12943](https://issues.apache.org/jira/browse/HDFS-12943) | Consistent Reads from Standby Node |  Major | hdfs | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-13571](https://issues.apache.org/jira/browse/HDFS-13571) | Deadnode detection |  Major | hdfs-client | Gang Xie | Lisheng Sun |
| [YARN-9923](https://issues.apache.org/jira/browse/YARN-9923) | Introduce HealthReporter interface to support multiple health checker files |  Major | nodemanager, yarn | Adam Antal | Adam Antal |
| [YARN-8851](https://issues.apache.org/jira/browse/YARN-8851) | [Umbrella] A pluggable device plugin framework to ease vendor plugin development |  Major | yarn | Zhankun Tang | Zhankun Tang |
| [YARN-9414](https://issues.apache.org/jira/browse/YARN-9414) | Application Catalog for YARN applications |  Major | . | Eric Yang | Eric Yang |
| [YARN-5542](https://issues.apache.org/jira/browse/YARN-5542) | Scheduling of opportunistic containers |  Major | . | Konstantinos Karanasos |  |
| [HDFS-13616](https://issues.apache.org/jira/browse/HDFS-13616) | Batch listing of multiple directories |  Major | . | Andrew Wang | Chao Sun |
| [HDFS-14743](https://issues.apache.org/jira/browse/HDFS-14743) | Enhance INodeAttributeProvider/ AccessControlEnforcer Interface in HDFS to support Authorization of mkdir, rm, rmdir, copy, move etc... |  Critical | hdfs | Ramesh Mani | Wei-Chiu Chuang |
| [MAPREDUCE-7237](https://issues.apache.org/jira/browse/MAPREDUCE-7237) | Supports config the shuffle's path cache related parameters |  Major | mrv2 | Wanqiang Ji | Wanqiang Ji |
| [HADOOP-16661](https://issues.apache.org/jira/browse/HADOOP-16661) | Support TLS 1.3 |  Major | security | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-16912](https://issues.apache.org/jira/browse/HADOOP-16912) | Emit per priority RPC queue time and processing time from DecayRpcScheduler |  Major | common | Fengnan Li | Fengnan Li |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-8226](https://issues.apache.org/jira/browse/YARN-8226) | Improve anti-affinity section description in YARN Service API doc |  Major | docs, documentation | Charan Hebri | Gour Saha |
| [HADOOP-15356](https://issues.apache.org/jira/browse/HADOOP-15356) | Make HTTP timeout configurable in ADLS Connector |  Major | fs/adl | Atul Sikaria | Atul Sikaria |
| [HADOOP-15586](https://issues.apache.org/jira/browse/HADOOP-15586) | Fix wrong log statement in AbstractService |  Minor | util | Szilard Nemeth | Szilard Nemeth |
| [HADOOP-15657](https://issues.apache.org/jira/browse/HADOOP-15657) | Registering MutableQuantiles via Metric annotation |  Major | metrics | Sushil Ks | Sushil Ks |
| [YARN-8621](https://issues.apache.org/jira/browse/YARN-8621) | Add test coverage of custom Resource Types for the apps/\<appId\> REST API endpoint |  Major | . | Szilard Nemeth | Szilard Nemeth |
| [HDFS-13947](https://issues.apache.org/jira/browse/HDFS-13947) | Review of DirectoryScanner Class |  Major | datanode | David Mollitor | David Mollitor |
| [YARN-8732](https://issues.apache.org/jira/browse/YARN-8732) | Add unit tests of min/max allocation for custom resource types in FairScheduler |  Minor | . | Szilard Nemeth | Szilard Nemeth |
| [YARN-8750](https://issues.apache.org/jira/browse/YARN-8750) | Refactor TestQueueMetrics |  Minor | resourcemanager | Szilard Nemeth | Szilard Nemeth |
| [HDFS-13950](https://issues.apache.org/jira/browse/HDFS-13950) | ACL documentation update to indicate that ACL entries are capped by 32 |  Minor | hdfs | Adam Antal | Adam Antal |
| [HDFS-13958](https://issues.apache.org/jira/browse/HDFS-13958) | Miscellaneous Improvements for FsVolumeSpi |  Major | datanode | David Mollitor | David Mollitor |
| [YARN-8644](https://issues.apache.org/jira/browse/YARN-8644) | Improve unit test for RMAppImpl.FinalTransition |  Minor | . | Szilard Nemeth | Szilard Nemeth |
| [HDFS-13967](https://issues.apache.org/jira/browse/HDFS-13967) | HDFS Router Quota Class Review |  Minor | federation, hdfs | David Mollitor | David Mollitor |
| [HADOOP-15832](https://issues.apache.org/jira/browse/HADOOP-15832) | Upgrade BouncyCastle to 1.60 |  Major | . | Robert Kanter | Robert Kanter |
| [HDFS-13882](https://issues.apache.org/jira/browse/HDFS-13882) | Set a maximum delay for retrying locateFollowingBlock |  Major | . | Kitti Nanasi | Kitti Nanasi |
| [MAPREDUCE-7149](https://issues.apache.org/jira/browse/MAPREDUCE-7149) | javadocs for FileInputFormat and OutputFormat to mention DT collection |  Minor | client | Steve Loughran | Steve Loughran |
| [HDFS-13968](https://issues.apache.org/jira/browse/HDFS-13968) | BlockReceiver Array-Based Queue |  Minor | datanode | David Mollitor | David Mollitor |
| [HADOOP-15717](https://issues.apache.org/jira/browse/HADOOP-15717) | TGT renewal thread does not log IOException |  Major | . | Szilard Nemeth | Szilard Nemeth |
| [HADOOP-15831](https://issues.apache.org/jira/browse/HADOOP-15831) | Include modificationTime in the toString method of CopyListingFileStatus |  Minor | . | Ted Yu | Ted Yu |
| [HDFS-13156](https://issues.apache.org/jira/browse/HDFS-13156) | HDFS Block Placement Policy - Client Local Rack |  Minor | documentation | David Mollitor | Ayush Saxena |
| [HADOOP-15849](https://issues.apache.org/jira/browse/HADOOP-15849) | Upgrade netty version to 3.10.6 |  Major | . | Xiao Chen | Xiao Chen |
| [YARN-8836](https://issues.apache.org/jira/browse/YARN-8836) | Add tags and attributes in resource definition |  Major | . | Weiwei Yang | Weiwei Yang |
| [HDFS-13987](https://issues.apache.org/jira/browse/HDFS-13987) | RBF: Review of RandomResolver Class |  Minor | federation | David Mollitor | David Mollitor |
| [MAPREDUCE-7150](https://issues.apache.org/jira/browse/MAPREDUCE-7150) | Optimize collections used by MR JHS to reduce its memory |  Major | jobhistoryserver, mrv2 | Misha Dmitriev | Misha Dmitriev |
| [HADOOP-15854](https://issues.apache.org/jira/browse/HADOOP-15854) | AuthToken Use StringBuilder instead of StringBuffer |  Trivial | auth | David Mollitor | David Mollitor |
| [HADOOP-11100](https://issues.apache.org/jira/browse/HADOOP-11100) | Support to configure ftpClient.setControlKeepAliveTimeout |  Minor | fs | Krishnamoorthy Dharmalingam | Adam Antal |
| [YARN-8899](https://issues.apache.org/jira/browse/YARN-8899) | TestCleanupAfterKIll is failing due to unsatisfied dependencies |  Blocker | yarn-native-services | Eric Yang | Robert Kanter |
| [YARN-8618](https://issues.apache.org/jira/browse/YARN-8618) | Yarn Service: When all the components of a service have restart policy NEVER then initiation of service upgrade should fail |  Major | . | Chandni Singh | Chandni Singh |
| [HADOOP-15804](https://issues.apache.org/jira/browse/HADOOP-15804) | upgrade to commons-compress 1.18 |  Major | . | PJ Fanning | Akira Ajisaka |
| [YARN-8916](https://issues.apache.org/jira/browse/YARN-8916) | Define a constant "docker" string in "ContainerRuntimeConstants.java" for better maintainability |  Minor | . | Zhankun Tang | Zhankun Tang |
| [YARN-8908](https://issues.apache.org/jira/browse/YARN-8908) | Fix errors in yarn-default.xml related to GPU/FPGA |  Major | . | Zhankun Tang | Zhankun Tang |
| [HDFS-13994](https://issues.apache.org/jira/browse/HDFS-13994) | Improve DataNode BlockSender waitForMinLength |  Minor | datanode | David Mollitor | David Mollitor |
| [HADOOP-15821](https://issues.apache.org/jira/browse/HADOOP-15821) | Move Hadoop YARN Registry to Hadoop Registry |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-8542](https://issues.apache.org/jira/browse/YARN-8542) | Yarn Service: Add component name to container json |  Major | . | Chandni Singh | Chandni Singh |
| [YARN-8923](https://issues.apache.org/jira/browse/YARN-8923) | Cleanup references to ENV file type in YARN service code |  Minor | yarn-native-services | Suma Shivaprasad | Suma Shivaprasad |
| [YARN-6586](https://issues.apache.org/jira/browse/YARN-6586) | YARN to facilitate HTTPS in AM web server |  Major | yarn | Haibo Chen | Robert Kanter |
| [HDFS-13941](https://issues.apache.org/jira/browse/HDFS-13941) | make storageId in BlockPoolTokenSecretManager.checkAccess optional |  Major | . | Ajay Kumar | Ajay Kumar |
| [HDFS-14026](https://issues.apache.org/jira/browse/HDFS-14026) | Overload BlockPoolTokenSecretManager.checkAccess to make storageId and storageType optional |  Major | . | Ajay Kumar | Ajay Kumar |
| [HDFS-14029](https://issues.apache.org/jira/browse/HDFS-14029) | Sleep in TestLazyPersistFiles should be put into a loop |  Trivial | hdfs | Adam Antal | Adam Antal |
| [HADOOP-9567](https://issues.apache.org/jira/browse/HADOOP-9567) | Provide auto-renewal for keytab based logins |  Minor | security | Harsh J | Hrishikesh Gadre |
| [YARN-8915](https://issues.apache.org/jira/browse/YARN-8915) | Update the doc about the default value of "maximum-container-assignments" for capacity scheduler |  Minor | . | Zhankun Tang | Zhankun Tang |
| [HDFS-14008](https://issues.apache.org/jira/browse/HDFS-14008) | NN should log snapshotdiff report |  Major | namenode | Pranay Singh | Pranay Singh |
| [HDFS-13996](https://issues.apache.org/jira/browse/HDFS-13996) | Make HttpFS' ACLs RegEx configurable |  Major | httpfs | Siyao Meng | Siyao Meng |
| [YARN-8954](https://issues.apache.org/jira/browse/YARN-8954) | Reservations list field in ReservationListInfo is not accessible |  Minor | resourcemanager, restapi | Oleksandr Shevchenko | Oleksandr Shevchenko |
| [YARN-7225](https://issues.apache.org/jira/browse/YARN-7225) | Add queue and partition info to RM audit log |  Major | resourcemanager | Jonathan Hung | Eric Payne |
| [HADOOP-15687](https://issues.apache.org/jira/browse/HADOOP-15687) | Credentials class should allow access to aliases |  Trivial | . | Lars Francke | Lars Francke |
| [YARN-8969](https://issues.apache.org/jira/browse/YARN-8969) | AbstractYarnScheduler#getNodeTracker should return generic type to avoid type casting |  Major | . | Wanqiang Ji | Wanqiang Ji |
| [HDFS-14053](https://issues.apache.org/jira/browse/HDFS-14053) | Provide ability for NN to re-replicate based on topology changes. |  Major | . | ellen johansen | Hrishikesh Gadre |
| [HDFS-14051](https://issues.apache.org/jira/browse/HDFS-14051) | Refactor NameNodeHttpServer#initWebHdfs to specify local keytab |  Major | . | Íñigo Goiri | CR Hota |
| [YARN-8957](https://issues.apache.org/jira/browse/YARN-8957) | Add Serializable interface to ComponentContainers |  Minor | . | Zhankun Tang | Zhankun Tang |
| [YARN-8976](https://issues.apache.org/jira/browse/YARN-8976) | Remove redundant modifiers in interface "ApplicationConstants" |  Trivial | . | Zhankun Tang | Zhankun Tang |
| [HADOOP-15907](https://issues.apache.org/jira/browse/HADOOP-15907) | Add missing maven modules in BUILDING.txt |  Trivial | . | Wanqiang Ji | Wanqiang Ji |
| [MAPREDUCE-7148](https://issues.apache.org/jira/browse/MAPREDUCE-7148) | Fast fail jobs when exceeds dfs quota limitation |  Major | task | Wang Yan | Wang Yan |
| [YARN-8977](https://issues.apache.org/jira/browse/YARN-8977) | Remove unnecessary type casting when calling AbstractYarnScheduler#getSchedulerNode |  Trivial | . | Wanqiang Ji | Wanqiang Ji |
| [YARN-8997](https://issues.apache.org/jira/browse/YARN-8997) | [Submarine] Small refactors of modifier, condition check and redundant local variables |  Minor | . | Zhankun Tang | Zhankun Tang |
| [HDFS-14070](https://issues.apache.org/jira/browse/HDFS-14070) | Refactor NameNodeWebHdfsMethods to allow better extensibility |  Major | . | CR Hota | CR Hota |
| [HADOOP-15926](https://issues.apache.org/jira/browse/HADOOP-15926) | Document upgrading the section in NOTICE.txt when upgrading the version of AWS SDK |  Minor | documentation | Akira Ajisaka | Dinesh Chitlangia |
| [HDFS-14045](https://issues.apache.org/jira/browse/HDFS-14045) | Use different metrics in DataNode to better measure latency of heartbeat/blockReports/incrementalBlockReports of Active/Standby NN |  Major | datanode | Jiandan Yang | Jiandan Yang |
| [HADOOP-12558](https://issues.apache.org/jira/browse/HADOOP-12558) | distcp documentation is woefully out of date |  Critical | documentation, tools/distcp | Allen Wittenauer | Dinesh Chitlangia |
| [HDFS-14063](https://issues.apache.org/jira/browse/HDFS-14063) | Support noredirect param for CREATE/APPEND/OPEN/GETFILECHECKSUM in HttpFS |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-8860](https://issues.apache.org/jira/browse/YARN-8860) | Federation client intercepter class contains unwanted character |  Minor | router | Rakesh Shah | Abhishek Modi |
| [HDFS-14015](https://issues.apache.org/jira/browse/HDFS-14015) | Improve error handling in hdfsThreadDestructor in native thread local storage |  Major | native | Daniel Templeton | Daniel Templeton |
| [HADOOP-15919](https://issues.apache.org/jira/browse/HADOOP-15919) | AliyunOSS: Enable Yarn to use OSS |  Major | fs/oss | wujinhu | wujinhu |
| [HADOOP-14739](https://issues.apache.org/jira/browse/HADOOP-14739) | Update start-build-env.sh and build instruction for docker for Mac instead of docker toolbox. |  Minor | build, documentation | Akira Ajisaka | Dinesh Chitlangia |
| [HDFS-14064](https://issues.apache.org/jira/browse/HDFS-14064) | WEBHDFS: Support Enable/Disable EC Policy |  Major | erasure-coding, webhdfs | Ayush Saxena | Ayush Saxena |
| [YARN-8964](https://issues.apache.org/jira/browse/YARN-8964) | [UI2] YARN ui2 should use clusters/{cluster name} for all ATSv2 REST APIs |  Major | . | Rohith Sharma K S | Akhil PB |
| [HADOOP-15943](https://issues.apache.org/jira/browse/HADOOP-15943) | AliyunOSS: add missing owner & group attributes for oss FileStatus |  Major | fs/oss | wujinhu | wujinhu |
| [HDFS-14108](https://issues.apache.org/jira/browse/HDFS-14108) | Performance improvement in BlockManager Data Structures |  Minor | hdfs | David Mollitor | David Mollitor |
| [HDFS-14102](https://issues.apache.org/jira/browse/HDFS-14102) | Performance improvement in BlockPlacementPolicyDefault |  Minor | . | David Mollitor | David Mollitor |
| [MAPREDUCE-7164](https://issues.apache.org/jira/browse/MAPREDUCE-7164) | FileOutputCommitter does not report progress while merging paths. |  Major | . | Kuhu Shukla | Kuhu Shukla |
| [YARN-8975](https://issues.apache.org/jira/browse/YARN-8975) | [Submarine] Use predefined Charset object StandardCharsets.UTF\_8 instead of String "UTF-8" |  Trivial | . | Zhankun Tang | Zhankun Tang |
| [YARN-8974](https://issues.apache.org/jira/browse/YARN-8974) | Improve the assertion message in TestGPUResourceHandler |  Trivial | . | Zhankun Tang | Zhankun Tang |
| [YARN-9061](https://issues.apache.org/jira/browse/YARN-9061) | Improve the GPU/FPGA module log message of container-executor |  Minor | . | Zhankun Tang | Zhankun Tang |
| [YARN-9069](https://issues.apache.org/jira/browse/YARN-9069) | Fix SchedulerInfo#getSchedulerType for custom schedulers |  Minor | . | Bilwa S T | Bilwa S T |
| [HDFS-14095](https://issues.apache.org/jira/browse/HDFS-14095) | EC: Track Erasure Coding commands in DFS statistics |  Major | erasure-coding | Ayush Saxena | Ayush Saxena |
| [YARN-9036](https://issues.apache.org/jira/browse/YARN-9036) | Escape newlines in health report in YARN UI |  Major | . | Jonathan Hung | Keqiu Hu |
| [HDFS-14106](https://issues.apache.org/jira/browse/HDFS-14106) | Refactor NamenodeFsck#copyBlock |  Minor | namenode | David Mollitor | David Mollitor |
| [HDFS-12946](https://issues.apache.org/jira/browse/HDFS-12946) | Add a tool to check rack configuration against EC policies |  Major | erasure-coding | Xiao Chen | Kitti Nanasi |
| [HDFS-13818](https://issues.apache.org/jira/browse/HDFS-13818) | Extend OIV to detect FSImage corruption |  Major | hdfs | Adam Antal | Adam Antal |
| [HDFS-14119](https://issues.apache.org/jira/browse/HDFS-14119) | Improve GreedyPlanner Parameter Logging |  Trivial | hdfs | David Mollitor | David Mollitor |
| [HDFS-14105](https://issues.apache.org/jira/browse/HDFS-14105) | Replace TreeSet in NamenodeFsck with HashSet |  Trivial | . | David Mollitor | David Mollitor |
| [YARN-8985](https://issues.apache.org/jira/browse/YARN-8985) | Improve debug log in FSParentQueue when assigning container |  Minor | fairscheduler | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [HDFS-14113](https://issues.apache.org/jira/browse/HDFS-14113) | EC : Add Configuration to restrict UserDefined Policies |  Major | erasure-coding | Ayush Saxena | Ayush Saxena |
| [YARN-9085](https://issues.apache.org/jira/browse/YARN-9085) | Add Guaranteed and MaxCapacity to CSQueueMetrics |  Major | . | Jonathan Hung | Jonathan Hung |
| [HDFS-14124](https://issues.apache.org/jira/browse/HDFS-14124) | EC : Support EC Commands (set/get/unset EcPolicy) via WebHdfs |  Major | erasure-coding, httpfs, webhdfs | Souryakanta Dwivedy | Ayush Saxena |
| [YARN-9051](https://issues.apache.org/jira/browse/YARN-9051) | Integrate multiple CustomResourceTypesConfigurationProvider implementations into one |  Minor | . | Szilard Nemeth | Szilard Nemeth |
| [YARN-9008](https://issues.apache.org/jira/browse/YARN-9008) | Extend YARN distributed shell with file localization feature |  Major | yarn | Peter Bacsko | Peter Bacsko |
| [HDFS-13985](https://issues.apache.org/jira/browse/HDFS-13985) | Clearer error message for ReplicaNotFoundException |  Major | hdfs | Adam Antal | Adam Antal |
| [HDFS-13970](https://issues.apache.org/jira/browse/HDFS-13970) | Use MultiMap for CacheManager Directives to simplify the code |  Minor | caching, hdfs | David Mollitor | David Mollitor |
| [HDFS-14006](https://issues.apache.org/jira/browse/HDFS-14006) | Refactor name node to allow different token verification implementations |  Major | . | CR Hota | CR Hota |
| [YARN-9122](https://issues.apache.org/jira/browse/YARN-9122) | Add table of contents to YARN Service API document |  Minor | documentation | Akira Ajisaka | Zhankun Tang |
| [HADOOP-16000](https://issues.apache.org/jira/browse/HADOOP-16000) | Remove TLSv1 and SSLv2Hello from the default value of hadoop.ssl.enabled.protocols |  Major | security | Akira Ajisaka | Gabor Bota |
| [YARN-9095](https://issues.apache.org/jira/browse/YARN-9095) | Removed Unused field from Resource: NUM\_MANDATORY\_RESOURCES |  Minor | . | Szilard Nemeth | Vidura Bhathiya Mudalige |
| [MAPREDUCE-7166](https://issues.apache.org/jira/browse/MAPREDUCE-7166) | map-only job should ignore node lost event when task is already succeeded |  Major | mrv2 | Zhaohui Xin | Lei Li |
| [YARN-9130](https://issues.apache.org/jira/browse/YARN-9130) | Add Bind\_HOST configuration for Yarn Web Proxy |  Major | yarn | Rong Tang | Rong Tang |
| [HADOOP-15965](https://issues.apache.org/jira/browse/HADOOP-15965) | Upgrade to ADLS SDK which has major performance improvement for ingress/egress |  Major | fs/adl | Vishwajeet Dusane | Vishwajeet Dusane |
| [HADOOP-16014](https://issues.apache.org/jira/browse/HADOOP-16014) | Fix test, checkstyle and javadoc issues in TestKerberosAuthenticationHandler |  Major | test | Dinesh Chitlangia | Dinesh Chitlangia |
| [HDFS-13946](https://issues.apache.org/jira/browse/HDFS-13946) | Log longest FSN write/read lock held stack trace |  Minor | . | Yiqun Lin | Yiqun Lin |
| [HADOOP-15962](https://issues.apache.org/jira/browse/HADOOP-15962) | The buffer size is small when unpacking tar archives |  Minor | common, util | David Mollitor | David Mollitor |
| [YARN-8878](https://issues.apache.org/jira/browse/YARN-8878) | Remove StringBuffer from ManagedParentQueue.java |  Trivial | resourcemanager | David Mollitor | David Mollitor |
| [YARN-8894](https://issues.apache.org/jira/browse/YARN-8894) | Improve InMemoryPlan#toString |  Minor | reservation system | David Mollitor | David Mollitor |
| [HDFS-14171](https://issues.apache.org/jira/browse/HDFS-14171) | Performance improvement in Tailing EditLog |  Major | namenode | Kenneth Yang | Kenneth Yang |
| [HDFS-14184](https://issues.apache.org/jira/browse/HDFS-14184) | [SPS] Add support for URI based path in satisfystoragepolicy command |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-14163](https://issues.apache.org/jira/browse/HDFS-14163) | Debug Admin Command Should Support Generic Options. |  Major | . | Harshakiran Reddy | Ayush Saxena |
| [YARN-6523](https://issues.apache.org/jira/browse/YARN-6523) | Optimize system credentials sent in node heartbeat responses |  Major | RM | Naganarasimha G R | Manikandan R |
| [HADOOP-15909](https://issues.apache.org/jira/browse/HADOOP-15909) | KeyProvider class should implement Closeable |  Major | kms | Kuhu Shukla | Kuhu Shukla |
| [HADOOP-16029](https://issues.apache.org/jira/browse/HADOOP-16029) | Consecutive StringBuilder.append can be reused |  Major | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-15481](https://issues.apache.org/jira/browse/HADOOP-15481) | Emit FairCallQueue stats as metrics |  Major | metrics, rpc-server | Erik Krogen | Christopher Gregorian |
| [HADOOP-15994](https://issues.apache.org/jira/browse/HADOOP-15994) | Upgrade Jackson2 to 2.9.8 |  Major | security | Akira Ajisaka | lqjacklee |
| [HDFS-14213](https://issues.apache.org/jira/browse/HDFS-14213) | Remove Jansson from BUILDING.txt |  Minor | documentation | Akira Ajisaka | Dinesh Chitlangia |
| [HDFS-14221](https://issues.apache.org/jira/browse/HDFS-14221) | Replace Guava Optional with Java Optional |  Major | . | Arpit Agarwal | Arpit Agarwal |
| [HDFS-14222](https://issues.apache.org/jira/browse/HDFS-14222) | Make ThrottledAsyncChecker constructor public |  Major | . | Arpit Agarwal | Arpit Agarwal |
| [HDFS-14153](https://issues.apache.org/jira/browse/HDFS-14153) | [SPS] : Add Support for Storage Policy Satisfier in WEBHDFS |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-14061](https://issues.apache.org/jira/browse/HDFS-14061) | Check if the cluster topology supports the EC policy before setting, enabling or adding it |  Major | erasure-coding, hdfs | Kitti Nanasi | Kitti Nanasi |
| [HDFS-14185](https://issues.apache.org/jira/browse/HDFS-14185) | Cleanup method calls to static Assert methods in TestAddStripedBlocks |  Minor | hdfs | Shweta | Shweta |
| [HADOOP-16075](https://issues.apache.org/jira/browse/HADOOP-16075) | Upgrade checkstyle version to 8.16 |  Minor | build | Dinesh Chitlangia | Dinesh Chitlangia |
| [HDFS-14187](https://issues.apache.org/jira/browse/HDFS-14187) | Make warning message more clear when there are not enough data nodes for EC write |  Major | erasure-coding | Kitti Nanasi | Kitti Nanasi |
| [HADOOP-16089](https://issues.apache.org/jira/browse/HADOOP-16089) | AliyunOSS: update oss-sdk version to 3.4.1 |  Major | fs/oss | wujinhu | wujinhu |
| [HDFS-14125](https://issues.apache.org/jira/browse/HDFS-14125) | Use parameterized log format in ECTopologyVerifier |  Trivial | erasure-coding | Kitti Nanasi | Kitti Nanasi |
| [HDFS-14231](https://issues.apache.org/jira/browse/HDFS-14231) | DataXceiver#run() should not log exceptions caused by InvalidToken exception as an error |  Major | hdfs | Kitti Nanasi | Kitti Nanasi |
| [HDFS-14250](https://issues.apache.org/jira/browse/HDFS-14250) | [SBN read] msync should sync with active NameNode to fetch the latest stateID |  Major | namenode | Chao Sun | Chao Sun |
| [YARN-8219](https://issues.apache.org/jira/browse/YARN-8219) | Add application launch time to ATSV2 |  Major | timelineserver | Kanwaljeet Sachdev | Abhishek Modi |
| [YARN-7171](https://issues.apache.org/jira/browse/YARN-7171) | RM UI should sort memory / cores numerically |  Major | . | Eric Maynard | Ahmed Hussein |
| [HDFS-14172](https://issues.apache.org/jira/browse/HDFS-14172) | Avoid NPE when SectionName#fromString returns null |  Minor | . | Xiang Li | Xiang Li |
| [YARN-9282](https://issues.apache.org/jira/browse/YARN-9282) | Typo in javadoc of class LinuxContainerExecutor: hadoop.security.authetication should be 'authentication' |  Trivial | . | Szilard Nemeth | Charan Hebri |
| [HDFS-14260](https://issues.apache.org/jira/browse/HDFS-14260) | Replace synchronized method in BlockReceiver with atomic value |  Minor | datanode | David Mollitor | David Mollitor |
| [HADOOP-16097](https://issues.apache.org/jira/browse/HADOOP-16097) | Provide proper documentation for FairCallQueue |  Major | documentation, ipc | Erik Krogen | Erik Krogen |
| [HADOOP-16108](https://issues.apache.org/jira/browse/HADOOP-16108) | Tail Follow Interval Should Allow To Specify The Sleep Interval To Save Unnecessary RPC's |  Major | . | Harshakiran Reddy | Ayush Saxena |
| [HDFS-14241](https://issues.apache.org/jira/browse/HDFS-14241) | Provide feedback on successful renameSnapshot and deleteSnapshot |  Minor | hdfs, shell | Siyao Meng | Siyao Meng |
| [HDFS-13209](https://issues.apache.org/jira/browse/HDFS-13209) | DistributedFileSystem.create should allow an option to provide StoragePolicy |  Major | hdfs | Jean-Marc Spaggiari | Ayush Saxena |
| [YARN-8295](https://issues.apache.org/jira/browse/YARN-8295) | [UI2] Improve "Resource Usage" tab error message when there are no data available. |  Minor | yarn-ui-v2 | Gergely Novák | Charan Hebri |
| [YARN-8927](https://issues.apache.org/jira/browse/YARN-8927) | Support trust top-level image like "centos" when "library" is configured in "docker.trusted.registries" |  Major | . | Zhankun Tang | Zhankun Tang |
| [HDFS-14258](https://issues.apache.org/jira/browse/HDFS-14258) | Introduce Java Concurrent Package To DataXceiverServer Class |  Minor | datanode | David Mollitor | David Mollitor |
| [YARN-7824](https://issues.apache.org/jira/browse/YARN-7824) | [UI2] Yarn Component Instance page should include link to container logs |  Major | yarn-ui-v2 | Yesha Vora | Akhil PB |
| [HADOOP-15281](https://issues.apache.org/jira/browse/HADOOP-15281) | Distcp to add no-rename copy option |  Major | tools/distcp | Steve Loughran | Andrew Olson |
| [HDFS-9596](https://issues.apache.org/jira/browse/HDFS-9596) | Remove Shuffle Method From DFSUtil |  Trivial | . | David Mollitor | David Mollitor |
| [HDFS-14296](https://issues.apache.org/jira/browse/HDFS-14296) | Prefer ArrayList over LinkedList in VolumeScanner |  Minor | datanode | David Mollitor | David Mollitor |
| [YARN-9309](https://issues.apache.org/jira/browse/YARN-9309) | Improve graph text in SLS to avoid overlapping |  Minor | . | Bilwa S T | Bilwa S T |
| [HDFS-14188](https://issues.apache.org/jira/browse/HDFS-14188) | Make hdfs ec -verifyClusterSetup command accept an erasure coding policy as a parameter |  Major | erasure-coding | Kitti Nanasi | Kitti Nanasi |
| [HADOOP-15967](https://issues.apache.org/jira/browse/HADOOP-15967) | KMS Benchmark Tool |  Major | . | Wei-Chiu Chuang | George Huang |
| [HDFS-14286](https://issues.apache.org/jira/browse/HDFS-14286) | Logging stale datanode information |  Trivial | hdfs | Karthik Palanisamy | Karthik Palanisamy |
| [HDFS-14235](https://issues.apache.org/jira/browse/HDFS-14235) | Handle ArrayIndexOutOfBoundsException in DataNodeDiskMetrics#slowDiskDetectionDaemon |  Major | . | Surendra Singh Lilhore | Ranith Sardar |
| [HDFS-14267](https://issues.apache.org/jira/browse/HDFS-14267) | Add test\_libhdfs\_ops to libhdfs tests, mark libhdfs\_read/write.c as examples |  Major | libhdfs, native, test | Sahil Takiar | Sahil Takiar |
| [HDFS-14302](https://issues.apache.org/jira/browse/HDFS-14302) | Refactor NameNodeWebHdfsMethods#generateDelegationToken() to allow better extensibility |  Major | . | CR Hota | CR Hota |
| [HADOOP-16035](https://issues.apache.org/jira/browse/HADOOP-16035) | Jenkinsfile for Hadoop |  Major | build | Allen Wittenauer | Allen Wittenauer |
| [HDFS-14298](https://issues.apache.org/jira/browse/HDFS-14298) | Improve log messages of ECTopologyVerifier |  Minor | . | Kitti Nanasi | Kitti Nanasi |
| [YARN-9168](https://issues.apache.org/jira/browse/YARN-9168) | DistributedShell client timeout should be -1 by default |  Minor | . | Zhankun Tang | Zhankun Tang |
| [HDFS-7133](https://issues.apache.org/jira/browse/HDFS-7133) | Support clearing namespace quota on "/" |  Major | namenode | Guo Ruijing | Ayush Saxena |
| [HADOOP-16126](https://issues.apache.org/jira/browse/HADOOP-16126) | ipc.Client.stop() may sleep too long to wait for all connections |  Major | ipc | Tsz-wo Sze | Tsz-wo Sze |
| [YARN-9287](https://issues.apache.org/jira/browse/YARN-9287) | Consecutive StringBuilder append should be reused |  Major | . | Ayush Saxena | Ayush Saxena |
| [YARN-9087](https://issues.apache.org/jira/browse/YARN-9087) | Improve logging for initialization of Resource plugins |  Major | yarn | Szilard Nemeth | Szilard Nemeth |
| [YARN-9121](https://issues.apache.org/jira/browse/YARN-9121) | Replace GpuDiscoverer.getInstance() to a readable object for easy access control |  Major | . | Szilard Nemeth | Szilard Nemeth |
| [YARN-9322](https://issues.apache.org/jira/browse/YARN-9322) | Store metrics for custom resource types into FSQueueMetrics and query them in FairSchedulerQueueInfo |  Major | . | Szilard Nemeth | Szilard Nemeth |
| [YARN-9139](https://issues.apache.org/jira/browse/YARN-9139) | Simplify initializer code of GpuDiscoverer |  Major | . | Szilard Nemeth | Szilard Nemeth |
| [HDFS-14247](https://issues.apache.org/jira/browse/HDFS-14247) | Repeat adding node description into network topology |  Minor | datanode | HuangTao | HuangTao |
| [YARN-9332](https://issues.apache.org/jira/browse/YARN-9332) | RackResolver tool should accept multiple hosts |  Minor | yarn | Lantao Jin | Lantao Jin |
| [HDFS-14182](https://issues.apache.org/jira/browse/HDFS-14182) | Datanode usage histogram is clicked to show ip list |  Major | . | fengchuang | fengchuang |
| [HDFS-14321](https://issues.apache.org/jira/browse/HDFS-14321) | Fix -Xcheck:jni issues in libhdfs, run ctest with -Xcheck:jni enabled |  Major | hdfs-client, libhdfs, native | Sahil Takiar | Sahil Takiar |
| [HADOOP-16148](https://issues.apache.org/jira/browse/HADOOP-16148) | Cleanup LineReader Unit Test |  Trivial | common | David Mollitor | David Mollitor |
| [HADOOP-16162](https://issues.apache.org/jira/browse/HADOOP-16162) | Remove unused Job Summary Appender configurations from log4j.properties |  Major | conf | Chen Zhi | Chen Zhi |
| [HDFS-14336](https://issues.apache.org/jira/browse/HDFS-14336) | Fix checkstyle for NameNodeMXBean |  Trivial | namenode | Danny Becker | Danny Becker |
| [YARN-9298](https://issues.apache.org/jira/browse/YARN-9298) | Implement FS placement rules using PlacementRule interface |  Major | scheduler | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [HDFS-14326](https://issues.apache.org/jira/browse/HDFS-14326) | Add CorruptFilesCount to JMX |  Minor | fs, metrics, namenode | Danny Becker | Danny Becker |
| [YARN-9138](https://issues.apache.org/jira/browse/YARN-9138) | Improve test coverage for nvidia-smi binary execution of GpuDiscoverer |  Major | . | Szilard Nemeth | Szilard Nemeth |
| [YARN-8218](https://issues.apache.org/jira/browse/YARN-8218) | Add application launch time to ATSV1 |  Major | . | Kanwaljeet Sachdev | Abhishek Modi |
| [YARN-9150](https://issues.apache.org/jira/browse/YARN-9150) | Making TimelineSchemaCreator support different backends for Timeline Schema Creation in ATSv2 |  Major | ATSv2 | Sushil Ks | Sushil Ks |
| [HADOOP-16157](https://issues.apache.org/jira/browse/HADOOP-16157) | [Clean-up] Remove NULL check before instanceof in AzureNativeFileSystemStore |  Minor | tools | Shweta | Shweta |
| [MAPREDUCE-7191](https://issues.apache.org/jira/browse/MAPREDUCE-7191) | JobHistoryServer should log exception when loading/parsing history file failed |  Minor | mrv2 | Jiandan Yang | Jiandan Yang |
| [YARN-9381](https://issues.apache.org/jira/browse/YARN-9381) | The yarn-default.xml has two identical property named yarn.timeline-service.http-cross-origin.enabled |  Trivial | yarn | jenny | Abhishek Modi |
| [MAPREDUCE-7192](https://issues.apache.org/jira/browse/MAPREDUCE-7192) | JobHistoryServer attempts page support jump to  containers log page in NM when logAggregation is disable |  Major | mrv2 | Jiandan Yang | Jiandan Yang |
| [HDFS-14346](https://issues.apache.org/jira/browse/HDFS-14346) | Better time precision in getTimeDuration |  Minor | namenode | Chao Sun | Chao Sun |
| [HDFS-14366](https://issues.apache.org/jira/browse/HDFS-14366) | Improve HDFS append performance |  Major | hdfs | Chao Sun | Chao Sun |
| [YARN-4404](https://issues.apache.org/jira/browse/YARN-4404) | Typo in comment in SchedulerUtils |  Trivial | resourcemanager | Daniel Templeton | Yesha Vora |
| [MAPREDUCE-7188](https://issues.apache.org/jira/browse/MAPREDUCE-7188) | [Clean-up] Remove NULL check before instanceof and fix checkstyle issue in TaskResult |  Minor | . | Shweta | Shweta |
| [YARN-9340](https://issues.apache.org/jira/browse/YARN-9340) | [Clean-up] Remove NULL check before instanceof in ResourceRequestSetKey |  Minor | yarn | Shweta | Shweta |
| [HDFS-14328](https://issues.apache.org/jira/browse/HDFS-14328) | [Clean-up] Remove NULL check before instanceof in TestGSet |  Minor | . | Shweta | Shweta |
| [HADOOP-16167](https://issues.apache.org/jira/browse/HADOOP-16167) | "hadoop CLASSFILE" prints error messages on Ubuntu 18 |  Major | scripts | Daniel Templeton | Daniel Templeton |
| [YARN-9385](https://issues.apache.org/jira/browse/YARN-9385) | YARN Services with simple authentication doesn't respect current UGI |  Major | security, yarn-native-services | Todd Lipcon | Eric Yang |
| [HDFS-14211](https://issues.apache.org/jira/browse/HDFS-14211) | [Consistent Observer Reads] Allow for configurable "always msync" mode |  Major | hdfs-client | Erik Krogen | Erik Krogen |
| [YARN-9370](https://issues.apache.org/jira/browse/YARN-9370) | Better logging in recoverAssignedGpus in class GpuResourceAllocator |  Trivial | . | Szilard Nemeth | Yesha Vora |
| [HADOOP-16147](https://issues.apache.org/jira/browse/HADOOP-16147) | Allow CopyListing sequence file keys and values to be more easily customized |  Major | tools/distcp | Andrew Olson | Andrew Olson |
| [YARN-9358](https://issues.apache.org/jira/browse/YARN-9358) | Add javadoc to new methods introduced in FSQueueMetrics with YARN-9322 |  Major | . | Szilard Nemeth | Zoltan Siegl |
| [YARN-8967](https://issues.apache.org/jira/browse/YARN-8967) | Change FairScheduler to use PlacementRule interface |  Major | capacityscheduler, fairscheduler | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [HDFS-14304](https://issues.apache.org/jira/browse/HDFS-14304) | High lock contention on hdfsHashMutex in libhdfs |  Major | hdfs-client, libhdfs, native | Sahil Takiar | Sahil Takiar |
| [HDFS-14295](https://issues.apache.org/jira/browse/HDFS-14295) | Add Threadpool for DataTransfers |  Major | datanode | David Mollitor | David Mollitor |
| [HDFS-14395](https://issues.apache.org/jira/browse/HDFS-14395) | Remove WARN Logging From Interrupts |  Minor | hdfs-client | David Mollitor | David Mollitor |
| [YARN-9264](https://issues.apache.org/jira/browse/YARN-9264) | [Umbrella] Follow-up on IntelOpenCL FPGA plugin |  Major | nodemanager | Peter Bacsko | Peter Bacsko |
| [MAPREDUCE-7190](https://issues.apache.org/jira/browse/MAPREDUCE-7190) | Add SleepJob additional parameter to make parallel runs distinguishable |  Major | . | Adam Antal | Adam Antal |
| [YARN-9214](https://issues.apache.org/jira/browse/YARN-9214) | Add AbstractYarnScheduler#getValidQueues method to remove duplication |  Major | . | Wanqiang Ji | Wanqiang Ji |
| [HDFS-13960](https://issues.apache.org/jira/browse/HDFS-13960) | hdfs dfs -checksum command should optionally show block size in output |  Minor | hdfs | Adam Antal | Lokesh Jain |
| [HDFS-14327](https://issues.apache.org/jira/browse/HDFS-14327) | Using FQDN instead of IP to access servers with DNS resolving |  Major | . | Fengnan Li | Fengnan Li |
| [YARN-9394](https://issues.apache.org/jira/browse/YARN-9394) | Use new API of RackResolver to get better performance |  Major | yarn | Lantao Jin | Lantao Jin |
| [HADOOP-16208](https://issues.apache.org/jira/browse/HADOOP-16208) | Do Not Log InterruptedException in Client |  Minor | common | David Mollitor | David Mollitor |
| [HDFS-14371](https://issues.apache.org/jira/browse/HDFS-14371) | Improve Logging in FSNamesystem by adding parameterized logging |  Minor | hdfs | Shweta | Shweta |
| [HADOOP-10848](https://issues.apache.org/jira/browse/HADOOP-10848) | Cleanup calling of sun.security.krb5.Config |  Minor | . | Kai Zheng | Akira Ajisaka |
| [YARN-9463](https://issues.apache.org/jira/browse/YARN-9463) | Add queueName info when failing with queue capacity sanity check |  Trivial | capacity scheduler | Aihua Xu | Aihua Xu |
| [HADOOP-16179](https://issues.apache.org/jira/browse/HADOOP-16179) | hadoop-common pom should not depend on kerb-simplekdc |  Major | common | Todd Lipcon | Todd Lipcon |
| [HADOOP-16052](https://issues.apache.org/jira/browse/HADOOP-16052) | Remove Subversion and Forrest from Dockerfile |  Minor | build | Akira Ajisaka | Xieming Li |
| [HADOOP-16243](https://issues.apache.org/jira/browse/HADOOP-16243) | Change Log Level to trace in NetUtils.java |  Major | . | Bharat Viswanadham | chencan |
| [HADOOP-16227](https://issues.apache.org/jira/browse/HADOOP-16227) | Upgrade checkstyle to 8.19 |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-16249](https://issues.apache.org/jira/browse/HADOOP-16249) | Make CallerContext LimitedPrivate scope to Public |  Minor | ipc | Kenneth Yang | Kenneth Yang |
| [HADOOP-15014](https://issues.apache.org/jira/browse/HADOOP-15014) | KMS should log the IP address of the clients |  Major | kms | Zsombor Gegesy | Zsombor Gegesy |
| [YARN-9123](https://issues.apache.org/jira/browse/YARN-9123) | Clean up and split testcases in TestNMWebServices for GPU support |  Minor | . | Szilard Nemeth | Szilard Nemeth |
| [YARN-9349](https://issues.apache.org/jira/browse/YARN-9349) | When doTransition() method occurs exception, the log level practices are inconsistent |  Major | nodemanager | Anuhan Torgonshar |  |
| [HDFS-14432](https://issues.apache.org/jira/browse/HDFS-14432) | dfs.datanode.shared.file.descriptor.paths duplicated in hdfs-default.xml |  Minor | hdfs | puleya7 | puleya7 |
| [HDFS-14374](https://issues.apache.org/jira/browse/HDFS-14374) | Expose total number of delegation tokens in AbstractDelegationTokenSecretManager |  Major | . | CR Hota | CR Hota |
| [HADOOP-16026](https://issues.apache.org/jira/browse/HADOOP-16026) | Replace incorrect use of system property user.name |  Major | . | Dinesh Chitlangia | Dinesh Chitlangia |
| [YARN-9081](https://issues.apache.org/jira/browse/YARN-9081) | Update jackson from 1.9.13 to 2.x in hadoop-yarn-services-core |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [HDFS-3246](https://issues.apache.org/jira/browse/HDFS-3246) | pRead equivalent for direct read path |  Major | hdfs-client, performance | Henry Robinson | Sahil Takiar |
| [HDFS-14463](https://issues.apache.org/jira/browse/HDFS-14463) | Add Log Level link under NameNode and DataNode Web UI Utilities dropdown |  Trivial | webhdfs | Siyao Meng | Siyao Meng |
| [HDFS-14460](https://issues.apache.org/jira/browse/HDFS-14460) | DFSUtil#getNamenodeWebAddr should return HTTPS address based on policy configured |  Major | . | CR Hota | CR Hota |
| [HADOOP-16282](https://issues.apache.org/jira/browse/HADOOP-16282) | Avoid FileStream to improve performance |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-14453](https://issues.apache.org/jira/browse/HDFS-14453) | Improve Bad Sequence Number Error Message |  Minor | ipc | David Mollitor | Shweta |
| [HADOOP-16059](https://issues.apache.org/jira/browse/HADOOP-16059) | Use SASL Factories Cache to Improve Performance |  Critical | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-16292](https://issues.apache.org/jira/browse/HADOOP-16292) | Refactor checkTrustAndSend in SaslDataTransferClient to make it cleaner |  Major | . | Sherwood Zheng | Sherwood Zheng |
| [YARN-9529](https://issues.apache.org/jira/browse/YARN-9529) | Log correct cpu controller path on error while initializing CGroups. |  Major | nodemanager | Jonathan Hung | Jonathan Hung |
| [HADOOP-16289](https://issues.apache.org/jira/browse/HADOOP-16289) | Allow extra jsvc startup option in hadoop\_start\_secure\_daemon in hadoop-functions.sh |  Major | scripts | Siyao Meng | Siyao Meng |
| [HADOOP-16238](https://issues.apache.org/jira/browse/HADOOP-16238) | Add the possbility to set SO\_REUSEADDR in IPC Server Listener |  Minor | ipc | Peter Bacsko | Peter Bacsko |
| [YARN-9453](https://issues.apache.org/jira/browse/YARN-9453) | Clean up code long if-else chain in ApplicationCLI#run |  Major | . | Szilard Nemeth | Wanqiang Ji |
| [YARN-9546](https://issues.apache.org/jira/browse/YARN-9546) | Add configuration option for YARN Native services AM classpath |  Major | . | Gergely Pollak | Gergely Pollak |
| [HDFS-14507](https://issues.apache.org/jira/browse/HDFS-14507) | Document -blockingDecommission option for hdfs dfsadmin -listOpenFiles |  Minor | documentation | Siyao Meng | Siyao Meng |
| [YARN-9145](https://issues.apache.org/jira/browse/YARN-9145) | [Umbrella] Dynamically add or remove auxiliary services |  Major | nodemanager | Billie Rinaldi | Billie Rinaldi |
| [HDFS-14451](https://issues.apache.org/jira/browse/HDFS-14451) | Incorrect header or version mismatch log message |  Minor | ipc | David Mollitor | Shweta |
| [HDFS-14502](https://issues.apache.org/jira/browse/HDFS-14502) | keepResults option in NNThroughputBenchmark should call saveNamespace() |  Major | benchmarks, hdfs | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-16323](https://issues.apache.org/jira/browse/HADOOP-16323) | https everywhere in Maven settings |  Minor | build | Akira Ajisaka | Akira Ajisaka |
| [YARN-9563](https://issues.apache.org/jira/browse/YARN-9563) | Resource report REST API could return NaN or Inf |  Minor | . | Ahmed Hussein | Ahmed Hussein |
| [HDFS-13654](https://issues.apache.org/jira/browse/HDFS-13654) | Use a random secret when a secret file doesn't  exist in HttpFS. This should be default. |  Major | httpfs, security | Pulkit Bhardwaj | Takanobu Asanuma |
| [YARN-9592](https://issues.apache.org/jira/browse/YARN-9592) | Use Logger format in ContainersMonitorImpl |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-9545](https://issues.apache.org/jira/browse/YARN-9545) | Create healthcheck REST endpoint for ATSv2 |  Major | ATSv2 | Zoltan Siegl | Zoltan Siegl |
| [HADOOP-16344](https://issues.apache.org/jira/browse/HADOOP-16344) | Make DurationInfo  "public unstable" |  Minor | util | Kevin Risden | Kevin Su |
| [YARN-9471](https://issues.apache.org/jira/browse/YARN-9471) | Cleanup in TestLogAggregationIndexFileController |  Major | log-aggregation, yarn | Adam Antal | Adam Antal |
| [HDFS-10659](https://issues.apache.org/jira/browse/HDFS-10659) | Namenode crashes after Journalnode re-installation in an HA cluster due to missing paxos directory |  Major | ha, journal-node | Amit Anand | star |
| [HDFS-10210](https://issues.apache.org/jira/browse/HDFS-10210) | Remove the defunct startKdc profile from hdfs |  Major | test | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-9569](https://issues.apache.org/jira/browse/YARN-9569) | Auto-created leaf queues do not honor cluster-wide min/max memory/vcores |  Major | capacity scheduler | Craig Condit | Craig Condit |
| [YARN-9602](https://issues.apache.org/jira/browse/YARN-9602) | Use logger format in Container Executor. |  Major | . | Abhishek Modi | Abhishek Modi |
| [HDFS-14513](https://issues.apache.org/jira/browse/HDFS-14513) | FSImage which is saving should be clean while NameNode shutdown |  Major | namenode | Xiaoqiao He | Xiaoqiao He |
| [YARN-9543](https://issues.apache.org/jira/browse/YARN-9543) | [UI2] Handle ATSv2 server down or failures cases gracefully in YARN UI v2 |  Major | ATSv2, yarn-ui-v2 | Zoltan Siegl | Zoltan Siegl |
| [HADOOP-16369](https://issues.apache.org/jira/browse/HADOOP-16369) | Fix zstandard shortname misspelled as zts |  Major | . | Jonathan Turner Eagles | Jonathan Turner Eagles |
| [HDFS-14560](https://issues.apache.org/jira/browse/HDFS-14560) | Allow block replication parameters to be refreshable |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-14203](https://issues.apache.org/jira/browse/HDFS-14203) | Refactor OIV Delimited output entry building mechanism |  Minor | tools | Adam Antal | Adam Antal |
| [HADOOP-14807](https://issues.apache.org/jira/browse/HADOOP-14807) | should prevent the possibility of  NPE about ReconfigurableBase.java |  Minor | . | hu xiaodong | hu xiaodong |
| [HDFS-12770](https://issues.apache.org/jira/browse/HDFS-12770) | Add doc about how to disable client socket cache |  Trivial | hdfs-client | Weiwei Yang | Weiwei Yang |
| [HADOOP-9157](https://issues.apache.org/jira/browse/HADOOP-9157) | Better option for curl in hadoop-auth-examples |  Minor | documentation | Jingguo Yao | Andras Bokor |
| [HDFS-14340](https://issues.apache.org/jira/browse/HDFS-14340) | Lower the log level when can't get postOpAttr |  Minor | nfs | Anuhan Torgonshar | Anuhan Torgonshar |
| [HADOOP-15914](https://issues.apache.org/jira/browse/HADOOP-15914) | hadoop jar command has no help argument |  Major | common | Adam Antal | Adam Antal |
| [YARN-9630](https://issues.apache.org/jira/browse/YARN-9630) | [UI2] Add a link in docs's top page |  Major | documentation, yarn-ui-v2 | Wanqiang Ji | Wanqiang Ji |
| [HADOOP-16156](https://issues.apache.org/jira/browse/HADOOP-16156) | [Clean-up] Remove NULL check before instanceof and fix checkstyle in InnerNodeImpl |  Minor | . | Shweta | Shweta |
| [HDFS-14201](https://issues.apache.org/jira/browse/HDFS-14201) | Ability to disallow safemode NN to become active |  Major | auto-failover | Xiao Liang | Xiao Liang |
| [HDFS-14487](https://issues.apache.org/jira/browse/HDFS-14487) | Missing Space in Client Error Message |  Minor | hdfs-client | David Mollitor | Shweta |
| [HDFS-14398](https://issues.apache.org/jira/browse/HDFS-14398) | Update HAState.java to fix typos. |  Trivial | namenode | bianqi | Nikhil Navadiya |
| [HDFS-14103](https://issues.apache.org/jira/browse/HDFS-14103) | Review Logging of BlockPlacementPolicyDefault |  Minor | . | David Mollitor | David Mollitor |
| [YARN-9631](https://issues.apache.org/jira/browse/YARN-9631) | hadoop-yarn-applications-catalog-webapp doesn't respect mvn test -D parameter |  Major | . | Wei-Chiu Chuang | Eric Yang |
| [HADOOP-14385](https://issues.apache.org/jira/browse/HADOOP-14385) | HttpExceptionUtils#validateResponse swallows exceptions |  Trivial | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-12564](https://issues.apache.org/jira/browse/HDFS-12564) | Add the documents of swebhdfs configurations on the client side |  Major | documentation, webhdfs | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-14403](https://issues.apache.org/jira/browse/HDFS-14403) | Cost-Based RPC FairCallQueue |  Major | ipc, namenode | Erik Krogen | Christopher Gregorian |
| [HADOOP-16266](https://issues.apache.org/jira/browse/HADOOP-16266) | Add more fine-grained processing time metrics to the RPC layer |  Minor | ipc | Christopher Gregorian | Erik Krogen |
| [HADOOP-16350](https://issues.apache.org/jira/browse/HADOOP-16350) | Ability to tell HDFS client not to request KMS Information from NameNode |  Major | common, kms | Greg Senia |  |
| [HADOOP-16396](https://issues.apache.org/jira/browse/HADOOP-16396) | Allow authoritative mode on a subdirectory |  Major | fs/s3 | Sean Mackrory | Sean Mackrory |
| [YARN-9629](https://issues.apache.org/jira/browse/YARN-9629) | Support configurable MIN\_LOG\_ROLLING\_INTERVAL |  Minor | log-aggregation, nodemanager, yarn | Adam Antal | Adam Antal |
| [HDFS-13694](https://issues.apache.org/jira/browse/HDFS-13694) | Making md5 computing being in parallel with image loading |  Major | . | zhouyingchao | Lisheng Sun |
| [HADOOP-16409](https://issues.apache.org/jira/browse/HADOOP-16409) | Allow authoritative mode on non-qualified paths |  Major | fs/s3 | Sean Mackrory | Sean Mackrory |
| [HDFS-14632](https://issues.apache.org/jira/browse/HDFS-14632) | Reduce useless #getNumLiveDataNodes call in SafeModeMonitor |  Major | namenode | Xiaoqiao He | Xiaoqiao He |
| [HDFS-14624](https://issues.apache.org/jira/browse/HDFS-14624) | When decommissioning a node, log remaining blocks to replicate periodically |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [YARN-9573](https://issues.apache.org/jira/browse/YARN-9573) | DistributedShell cannot specify LogAggregationContext |  Major | distributed-shell, log-aggregation, yarn | Adam Antal | Adam Antal |
| [YARN-9337](https://issues.apache.org/jira/browse/YARN-9337) | GPU auto-discovery script runs even when the resource is given by hand |  Major | yarn | Adam Antal | Adam Antal |
| [YARN-9360](https://issues.apache.org/jira/browse/YARN-9360) | Do not expose innards of QueueMetrics object into FSLeafQueue#computeMaxAMResource |  Major | . | Szilard Nemeth | Peter Bacsko |
| [YARN-9127](https://issues.apache.org/jira/browse/YARN-9127) | Create more tests to verify GpuDeviceInformationParser |  Major | . | Szilard Nemeth | Peter Bacsko |
| [YARN-9326](https://issues.apache.org/jira/browse/YARN-9326) | Fair Scheduler configuration defaults are not documented in case of min and maxResources |  Major | docs, documentation, fairscheduler, yarn | Adam Antal | Adam Antal |
| [HDFS-14547](https://issues.apache.org/jira/browse/HDFS-14547) | DirectoryWithQuotaFeature.quota costs additional memory even the storage type quota is not set. |  Major | . | Jinglun | Jinglun |
| [HDFS-13693](https://issues.apache.org/jira/browse/HDFS-13693) | Remove unnecessary search in INodeDirectory.addChild during image loading |  Major | namenode | zhouyingchao | Lisheng Sun |
| [HADOOP-16431](https://issues.apache.org/jira/browse/HADOOP-16431) | Remove useless log in IOUtils.java and ExceptionDiags.java |  Minor | . | Lisheng Sun | Lisheng Sun |
| [HDFS-14673](https://issues.apache.org/jira/browse/HDFS-14673) | The console log is noisy when using DNSDomainNameResolver to resolve NameNode. |  Minor | . | Akira Ajisaka | Kevin Su |
| [HDFS-12967](https://issues.apache.org/jira/browse/HDFS-12967) | NNBench should support multi-cluster access |  Major | benchmarks | Chen Zhang | Chen Zhang |
| [HADOOP-16452](https://issues.apache.org/jira/browse/HADOOP-16452) | Increase ipc.maximum.data.length default from 64MB to 128MB |  Major | ipc | Wei-Chiu Chuang | Siyao Meng |
| [HDFS-14449](https://issues.apache.org/jira/browse/HDFS-14449) | Expose total number of DT in JMX for Namenode |  Major | . | Fengnan Li | Fengnan Li |
| [HDFS-14419](https://issues.apache.org/jira/browse/HDFS-14419) | Avoid repeated calls to the listOpenFiles function |  Minor | namenode, performance | HuangTao | HuangTao |
| [HDFS-14683](https://issues.apache.org/jira/browse/HDFS-14683) | WebHDFS: Add erasureCodingPolicy field to GETCONTENTSUMMARY response |  Major | . | Siyao Meng | Siyao Meng |
| [YARN-9375](https://issues.apache.org/jira/browse/YARN-9375) | Use Configured in GpuDiscoverer and FpgaDiscoverer |  Major | nodemanager, yarn | Adam Antal | Adam Antal |
| [YARN-9093](https://issues.apache.org/jira/browse/YARN-9093) | Remove commented code block from the beginning of TestDefaultContainerExecutor |  Trivial | . | Szilard Nemeth | Vidura Bhathiya Mudalige |
| [HADOOP-15942](https://issues.apache.org/jira/browse/HADOOP-15942) | Change the logging level form DEBUG to ERROR for RuntimeErrorException in JMXJsonServlet |  Major | common | Anuhan Torgonshar | Anuhan Torgonshar |
| [YARN-9667](https://issues.apache.org/jira/browse/YARN-9667) | Container-executor.c duplicates messages to stdout |  Major | nodemanager, yarn | Adam Antal | Peter Bacsko |
| [YARN-9678](https://issues.apache.org/jira/browse/YARN-9678) | TestGpuResourceHandler / TestFpgaResourceHandler should be renamed |  Major | . | Szilard Nemeth | Kevin Su |
| [HDFS-14652](https://issues.apache.org/jira/browse/HDFS-14652) | HealthMonitor connection retry times should be configurable |  Major | . | Chen Zhang | Chen Zhang |
| [HDFS-14313](https://issues.apache.org/jira/browse/HDFS-14313) | Get hdfs used space from FsDatasetImpl#volumeMap#ReplicaInfo in memory  instead of df/du |  Major | datanode, performance | Lisheng Sun | Lisheng Sun |
| [HDFS-14608](https://issues.apache.org/jira/browse/HDFS-14608) | DataNode#DataTransfer should be named |  Minor | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-14616](https://issues.apache.org/jira/browse/HDFS-14616) | Add the warn log when the volume available space isn't enough |  Minor | hdfs | liying | liying |
| [YARN-9711](https://issues.apache.org/jira/browse/YARN-9711) | Missing spaces in NMClientImpl |  Trivial | client | Charles Xu | Charles Xu |
| [HDFS-14662](https://issues.apache.org/jira/browse/HDFS-14662) | Document the usage of the new Balancer "asService" parameter |  Major | . | Chen Zhang | Chen Zhang |
| [HDFS-14701](https://issues.apache.org/jira/browse/HDFS-14701) | Change Log Level to warn in SlotReleaser |  Minor | . | Lisheng Sun | Lisheng Sun |
| [HDFS-14705](https://issues.apache.org/jira/browse/HDFS-14705) | Remove unused configuration dfs.min.replication |  Trivial | . | Wei-Chiu Chuang | CR Hota |
| [HDFS-14693](https://issues.apache.org/jira/browse/HDFS-14693) | NameNode should log a warning when EditLog IPC logger's pending size exceeds limit. |  Minor | namenode | Xudong Cao | Xudong Cao |
| [YARN-9094](https://issues.apache.org/jira/browse/YARN-9094) | Remove unused interface method: NodeResourceUpdaterPlugin#handleUpdatedResourceFromRM |  Trivial | . | Szilard Nemeth | Gergely Pollak |
| [YARN-9096](https://issues.apache.org/jira/browse/YARN-9096) | Some GpuResourcePlugin and ResourcePluginManager methods are synchronized unnecessarily |  Major | . | Szilard Nemeth | Gergely Pollak |
| [YARN-9092](https://issues.apache.org/jira/browse/YARN-9092) | Create an object for cgroups mount enable and cgroups mount path as they belong together |  Minor | . | Szilard Nemeth | Gergely Pollak |
| [YARN-9124](https://issues.apache.org/jira/browse/YARN-9124) | Resolve contradiction in ResourceUtils: addMandatoryResources / checkMandatoryResources work differently |  Minor | . | Szilard Nemeth | Adam Antal |
| [YARN-8199](https://issues.apache.org/jira/browse/YARN-8199) | Logging fileSize of log files under NM Local Dir |  Major | log-aggregation | Prabhu Joseph | Prabhu Joseph |
| [HDFS-14195](https://issues.apache.org/jira/browse/HDFS-14195) | OIV: print out storage policy id in oiv Delimited output |  Minor | tools | Wang, Xinglong | Wang, Xinglong |
| [YARN-9729](https://issues.apache.org/jira/browse/YARN-9729) | [UI2] Fix error message for logs when ATSv2 is offline |  Major | yarn-ui-v2 | Zoltan Siegl | Zoltan Siegl |
| [YARN-9657](https://issues.apache.org/jira/browse/YARN-9657) | AbstractLivelinessMonitor add serviceName to PingChecker thread |  Minor | . | Bibin Chundatt | Bilwa S T |
| [YARN-9715](https://issues.apache.org/jira/browse/YARN-9715) | [UI2] yarn-container-log URI need to be encoded to avoid potential misuses |  Major | . | Prabhu Joseph | Akhil PB |
| [HADOOP-16453](https://issues.apache.org/jira/browse/HADOOP-16453) | Update how exceptions are handled in NetUtils |  Minor | . | Lisheng Sun | Lisheng Sun |
| [YARN-9464](https://issues.apache.org/jira/browse/YARN-9464) | Support "Pending Resource" metrics in RM's RESTful API |  Major | . | Zhankun Tang | Prabhu Joseph |
| [YARN-9135](https://issues.apache.org/jira/browse/YARN-9135) | NM State store ResourceMappings serialization are tested with Strings instead of real Device objects |  Major | . | Szilard Nemeth | Peter Bacsko |
| [HDFS-13505](https://issues.apache.org/jira/browse/HDFS-13505) | Turn on HDFS ACLs by default. |  Major | . | Ajay Kumar | Siyao Meng |
| [HDFS-14370](https://issues.apache.org/jira/browse/HDFS-14370) | Edit log tailing fast-path should allow for backoff |  Major | namenode, qjm | Erik Krogen | Erik Krogen |
| [YARN-9442](https://issues.apache.org/jira/browse/YARN-9442) | container working directory has group read permissions |  Minor | yarn | Jim Brennan | Jim Brennan |
| [HDFS-14625](https://issues.apache.org/jira/browse/HDFS-14625) | Make DefaultAuditLogger class in FSnamesystem to Abstract |  Major | . | hemanthboyina | hemanthboyina |
| [HDFS-14491](https://issues.apache.org/jira/browse/HDFS-14491) | More Clarity on Namenode UI Around Blocks and Replicas |  Minor | . | Alan Jackoway | Siyao Meng |
| [YARN-9134](https://issues.apache.org/jira/browse/YARN-9134) | No test coverage for redefining FPGA / GPU resource types in TestResourceUtils |  Major | . | Szilard Nemeth | Peter Bacsko |
| [YARN-9133](https://issues.apache.org/jira/browse/YARN-9133) | Make tests more easy to comprehend in TestGpuResourceHandler |  Major | . | Szilard Nemeth | Peter Bacsko |
| [YARN-9140](https://issues.apache.org/jira/browse/YARN-9140) | Code cleanup in ResourcePluginManager.initialize and in TestResourcePluginManager |  Trivial | . | Szilard Nemeth | Peter Bacsko |
| [YARN-9676](https://issues.apache.org/jira/browse/YARN-9676) | Add DEBUG and TRACE level messages to AppLogAggregatorImpl and connected classes |  Major | . | Adam Antal | Adam Antal |
| [YARN-9488](https://issues.apache.org/jira/browse/YARN-9488) | Skip YARNFeatureNotEnabledException from ClientRMService |  Minor | resourcemanager | Prabhu Joseph | Prabhu Joseph |
| [YARN-9679](https://issues.apache.org/jira/browse/YARN-9679) | Regular code cleanup in TestResourcePluginManager |  Major | . | Szilard Nemeth | Adam Antal |
| [HADOOP-16504](https://issues.apache.org/jira/browse/HADOOP-16504) | Increase ipc.server.listen.queue.size default from 128 to 256 |  Major | . | Lisheng Sun | Lisheng Sun |
| [YARN-8586](https://issues.apache.org/jira/browse/YARN-8586) | Extract log aggregation related fields and methods from RMAppImpl |  Major | . | Szilard Nemeth | Peter Bacsko |
| [YARN-9100](https://issues.apache.org/jira/browse/YARN-9100) | Add tests for GpuResourceAllocator and do minor code cleanup |  Major | . | Szilard Nemeth | Peter Bacsko |
| [HDFS-14678](https://issues.apache.org/jira/browse/HDFS-14678) | Allow triggerBlockReport to a specific namenode |  Major | datanode | Leon Gao | Leon Gao |
| [HDFS-14523](https://issues.apache.org/jira/browse/HDFS-14523) | Remove excess read lock for NetworkToplogy |  Major | . | Wu Weiwei | Wu Weiwei |
| [HADOOP-15246](https://issues.apache.org/jira/browse/HADOOP-15246) | SpanReceiverInfo - Prefer ArrayList over LinkedList |  Trivial | common | David Mollitor | David Mollitor |
| [HADOOP-16158](https://issues.apache.org/jira/browse/HADOOP-16158) | DistCp to support checksum validation when copy blocks in parallel |  Major | tools/distcp | Kai Xie | Kai Xie |
| [HADOOP-14784](https://issues.apache.org/jira/browse/HADOOP-14784) | [KMS] Improve KeyAuthorizationKeyProvider#toString() |  Trivial | . | Wei-Chiu Chuang | Yeliang Cang |
| [HDFS-14746](https://issues.apache.org/jira/browse/HDFS-14746) | Trivial test code update after HDFS-14687 |  Trivial | ec | Wei-Chiu Chuang | Kevin Su |
| [HDFS-13709](https://issues.apache.org/jira/browse/HDFS-13709) | Report bad block to NN when transfer block encounter EIO exception |  Major | datanode | Chen Zhang | Chen Zhang |
| [HDFS-14665](https://issues.apache.org/jira/browse/HDFS-14665) | HttpFS: LISTSTATUS response is missing HDFS-specific fields |  Major | httpfs | Siyao Meng | Siyao Meng |
| [HADOOP-16523](https://issues.apache.org/jira/browse/HADOOP-16523) | Minor spell mistake in comment (PR#388) |  Major | . | Wei-Chiu Chuang |  |
| [HDFS-14276](https://issues.apache.org/jira/browse/HDFS-14276) | [SBN read] Reduce tailing overhead |  Major | ha, namenode | Wei-Chiu Chuang | Ayush Saxena |
| [HADOOP-16061](https://issues.apache.org/jira/browse/HADOOP-16061) | Update Apache Yetus to 0.10.0 |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HDFS-14358](https://issues.apache.org/jira/browse/HDFS-14358) | Provide LiveNode and DeadNode filter in DataNode UI |  Major | . | Ravuri Sushma sree | hemanthboyina |
| [HDFS-14675](https://issues.apache.org/jira/browse/HDFS-14675) | Increase Balancer Defaults Further |  Major | balancer & mover | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-14617](https://issues.apache.org/jira/browse/HDFS-14617) | Improve fsimage load time by writing sub-sections to the fsimage index |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-14497](https://issues.apache.org/jira/browse/HDFS-14497) | Write lock held by metasave impact following RPC processing |  Major | namenode | Xiaoqiao He | Xiaoqiao He |
| [HDFS-14760](https://issues.apache.org/jira/browse/HDFS-14760) | Log INFO mode if snapshot usage and actual usage differ |  Major | . | CR Hota | CR Hota |
| [HDFS-14710](https://issues.apache.org/jira/browse/HDFS-14710) | RBF: Improve some RPC performance by using previous block |  Minor | rbf | xuzq | xuzq |
| [YARN-9756](https://issues.apache.org/jira/browse/YARN-9756) | Create metric that sums total memory/vcores preempted per round |  Major | capacity scheduler | Eric Payne | Manikandan R |
| [HDFS-14104](https://issues.apache.org/jira/browse/HDFS-14104) | Review getImageTxIdToRetain |  Minor | namenode | David Mollitor | David Mollitor |
| [HDFS-14256](https://issues.apache.org/jira/browse/HDFS-14256) | Review Logging of NameNode Class |  Minor | namenode | David Mollitor | David Mollitor |
| [YARN-9783](https://issues.apache.org/jira/browse/YARN-9783) | Remove low-level zookeeper test to be able to build Hadoop against zookeeper 3.5.5 |  Major | test | Mate Szalay-Beko | Mate Szalay-Beko |
| [HDFS-14748](https://issues.apache.org/jira/browse/HDFS-14748) | Make DataNodePeerMetrics#minOutlierDetectionSamples configurable |  Major | . | Lisheng Sun | Lisheng Sun |
| [HADOOP-15998](https://issues.apache.org/jira/browse/HADOOP-15998) | Ensure jar validation works on Windows. |  Blocker | build | Brian Grunkemeyer | Brian Grunkemeyer |
| [HDFS-13843](https://issues.apache.org/jira/browse/HDFS-13843) | RBF: Add optional parameter -d for detailed listing of mount points. |  Major | federation | Soumyapn | Ayush Saxena |
| [YARN-9400](https://issues.apache.org/jira/browse/YARN-9400) | Remove unnecessary if at EntityGroupFSTimelineStore#parseApplicationId |  Minor | . | Prabhu Joseph | Prabhu Joseph |
| [HDFS-14633](https://issues.apache.org/jira/browse/HDFS-14633) | The StorageType quota and consume in QuotaFeature is not handled for rename |  Major | . | Jinglun | Jinglun |
| [HADOOP-16268](https://issues.apache.org/jira/browse/HADOOP-16268) | Allow custom wrapped exception to be thrown by server if RPC call queue is filled up |  Major | . | CR Hota | CR Hota |
| [HDFS-14812](https://issues.apache.org/jira/browse/HDFS-14812) | RBF: MountTableRefresherService should load cache when refresh |  Major | . | xuzq | xuzq |
| [YARN-9810](https://issues.apache.org/jira/browse/YARN-9810) | Add queue capacity/maxcapacity percentage metrics |  Major | . | Jonathan Hung | Shubham Gupta |
| [HDFS-14784](https://issues.apache.org/jira/browse/HDFS-14784) | Add more methods to WebHdfsTestUtil to support tests outside of package |  Major | . | Chen Zhang | Chen Zhang |
| [HDFS-13913](https://issues.apache.org/jira/browse/HDFS-13913) | LazyPersistFileScrubber.run() should log meaningful warn message |  Minor | namenode | Daniel Templeton | Daniel Green |
| [HADOOP-16531](https://issues.apache.org/jira/browse/HADOOP-16531) | Log more detail for slow RPC |  Major | . | Chen Zhang | Chen Zhang |
| [YARN-9763](https://issues.apache.org/jira/browse/YARN-9763) | Print application tags in application summary |  Major | . | Jonathan Hung | Manoj Kumar |
| [YARN-9795](https://issues.apache.org/jira/browse/YARN-9795) | ClusterMetrics to include AM allocation delay |  Minor | . | Fengnan Li | Fengnan Li |
| [YARN-8995](https://issues.apache.org/jira/browse/YARN-8995) | Log events info in AsyncDispatcher when event queue size cumulatively reaches a certain number every time. |  Major | metrics, nodemanager, resourcemanager | zhuqi | zhuqi |
| [YARN-9787](https://issues.apache.org/jira/browse/YARN-9787) | Typo in analysesErrorMsg |  Trivial | yarn | David Mollitor | Kevin Su |
| [YARN-9764](https://issues.apache.org/jira/browse/YARN-9764) | Print application submission context label in application summary |  Major | . | Jonathan Hung | Manoj Kumar |
| [HADOOP-16549](https://issues.apache.org/jira/browse/HADOOP-16549) | Remove Unsupported SSL/TLS Versions from Docs/Properties |  Minor | documentation, security | Daisuke Kobayashi | Daisuke Kobayashi |
| [YARN-9824](https://issues.apache.org/jira/browse/YARN-9824) | Fall back to configured queue ordering policy class name |  Major | . | Jonathan Hung | Jonathan Hung |
| [HADOOP-15184](https://issues.apache.org/jira/browse/HADOOP-15184) | Add GitHub pull request template |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [YARN-9815](https://issues.apache.org/jira/browse/YARN-9815) | ReservationACLsTestBase fails with NPE |  Minor | yarn | Ahmed Hussein | Ahmed Hussein |
| [HDFS-14840](https://issues.apache.org/jira/browse/HDFS-14840) | Use Java Conccurent Instead of Synchronization in BlockPoolTokenSecretManager |  Minor | hdfs | David Mollitor | David Mollitor |
| [HDFS-14799](https://issues.apache.org/jira/browse/HDFS-14799) | Do Not Call Map containsKey In Conjunction with get |  Minor | namenode | David Mollitor | hemanthboyina |
| [HDFS-14795](https://issues.apache.org/jira/browse/HDFS-14795) | Add Throttler for writing block |  Minor | . | Lisheng Sun | Lisheng Sun |
| [HADOOP-16556](https://issues.apache.org/jira/browse/HADOOP-16556) | Fix some LGTM alerts |  Minor | common | Malcolm Taylor | Malcolm Taylor |
| [HADOOP-16069](https://issues.apache.org/jira/browse/HADOOP-16069) | Support configure ZK\_DTSM\_ZK\_KERBEROS\_PRINCIPAL in ZKDelegationTokenSecretManager using principal with Schema /\_HOST |  Minor | common | luhuachao | luhuachao |
| [HDFS-14844](https://issues.apache.org/jira/browse/HDFS-14844) | Make buffer of BlockReaderRemote#newBlockReader#BufferedOutputStream  configurable |  Minor | . | Lisheng Sun | Lisheng Sun |
| [HADOOP-16445](https://issues.apache.org/jira/browse/HADOOP-16445) | Allow separate custom signing algorithms for S3 and DDB |  Major | fs/s3 | Siddharth Seth | Siddharth Seth |
| [YARN-9762](https://issues.apache.org/jira/browse/YARN-9762) | Add submission context label to audit logs |  Major | . | Jonathan Hung | Manoj Kumar |
| [HDFS-14837](https://issues.apache.org/jira/browse/HDFS-14837) | Review of Block.java |  Minor | hdfs-client | David Mollitor | David Mollitor |
| [HDFS-14843](https://issues.apache.org/jira/browse/HDFS-14843) | Double Synchronization in BlockReportLeaseManager |  Minor | . | David Mollitor | David Mollitor |
| [HDFS-14832](https://issues.apache.org/jira/browse/HDFS-14832) | RBF : Add Icon for ReadOnly False |  Minor | . | hemanthboyina | hemanthboyina |
| [HDFS-11934](https://issues.apache.org/jira/browse/HDFS-11934) | Add assertion to TestDefaultNameNodePort#testGetAddressFromConf |  Minor | hdfs | legend | Nikhil Navadiya |
| [YARN-9857](https://issues.apache.org/jira/browse/YARN-9857) | TestDelegationTokenRenewer throws NPE but tests pass |  Minor | . | Ahmed Hussein | Ahmed Hussein |
| [HDFS-14564](https://issues.apache.org/jira/browse/HDFS-14564) | Add libhdfs APIs for readFully; add readFully to ByteBufferPositionedReadable |  Major | hdfs-client, libhdfs, native | Sahil Takiar | Sahil Takiar |
| [HDFS-14850](https://issues.apache.org/jira/browse/HDFS-14850) | Optimize FileSystemAccessService#getFileSystemConfiguration |  Major | httpfs, performance | Lisheng Sun | Lisheng Sun |
| [HDFS-14192](https://issues.apache.org/jira/browse/HDFS-14192) | Track missing DFS operations in Statistics and StorageStatistics |  Major | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-16599](https://issues.apache.org/jira/browse/HADOOP-16599) | Allow a SignerInitializer to be specified along with a Custom Signer |  Major | fs/s3 | Siddharth Seth | Siddharth Seth |
| [HDFS-14888](https://issues.apache.org/jira/browse/HDFS-14888) | RBF: Enable Parallel Test Profile for builds |  Major | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-16624](https://issues.apache.org/jira/browse/HADOOP-16624) | Upgrade hugo to the latest version in Dockerfile |  Minor | build | Akira Ajisaka | Kevin Su |
| [HDFS-14814](https://issues.apache.org/jira/browse/HDFS-14814) | RBF: RouterQuotaUpdateService supports inherited rule. |  Major | . | Jinglun | Jinglun |
| [YARN-9356](https://issues.apache.org/jira/browse/YARN-9356) | Add more tests to ratio method in TestResourceCalculator |  Major | . | Szilard Nemeth | Zoltan Siegl |
| [HDFS-14898](https://issues.apache.org/jira/browse/HDFS-14898) | Use Relative URLS in Hadoop HDFS HTTP FS |  Major | httpfs | David Mollitor | David Mollitor |
| [YARN-7291](https://issues.apache.org/jira/browse/YARN-7291) | Better input parsing for resource in allocation file |  Minor | fairscheduler | Yufei Gu | Zoltan Siegl |
| [YARN-9860](https://issues.apache.org/jira/browse/YARN-9860) | Enable service mode for Docker containers on YARN |  Major | . | Prabhu Joseph | Prabhu Joseph |
| [HDFS-14899](https://issues.apache.org/jira/browse/HDFS-14899) | Use Relative URLS in Hadoop HDFS RBF |  Major | rbf | David Mollitor | David Mollitor |
| [HDFS-14238](https://issues.apache.org/jira/browse/HDFS-14238) | A log in NNThroughputBenchmark should  change log level to "INFO" instead of "ERROR" |  Major | . | Shen Yinjie | Shen Yinjie |
| [HADOOP-16643](https://issues.apache.org/jira/browse/HADOOP-16643) | Update netty4 to the latest 4.1.42 |  Major | . | Wei-Chiu Chuang | Lisheng Sun |
| [HDFS-14810](https://issues.apache.org/jira/browse/HDFS-14810) | Review FSNameSystem editlog sync |  Major | namenode | Xiaoqiao He | Xiaoqiao He |
| [HADOOP-16152](https://issues.apache.org/jira/browse/HADOOP-16152) | Upgrade Eclipse Jetty version to 9.4.x |  Major | . | Yuming Wang | Siyao Meng |
| [HADOOP-16579](https://issues.apache.org/jira/browse/HADOOP-16579) | Upgrade to Apache Curator 4.2.0 and ZooKeeper 3.5.6 in Hadoop |  Major | . | Mate Szalay-Beko | Norbert Kalmár |
| [HDFS-14918](https://issues.apache.org/jira/browse/HDFS-14918) | Remove useless getRedundancyThread from BlockManagerTestUtil |  Minor | . | Fei Hui | Fei Hui |
| [HDFS-14915](https://issues.apache.org/jira/browse/HDFS-14915) | Move Superuser Check Before Taking Lock For Encryption API |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-14921](https://issues.apache.org/jira/browse/HDFS-14921) | Remove SuperUser Check in Setting Storage Policy in FileStatus During Listing |  Major | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-16615](https://issues.apache.org/jira/browse/HADOOP-16615) | Add password check for credential provider |  Major | . | hong dongdong | hong dongdong |
| [HDFS-14917](https://issues.apache.org/jira/browse/HDFS-14917) | Change the ICON of "Decommissioned & dead" datanode on "dfshealth.html" |  Trivial | ui | Xieming Li | Xieming Li |
| [HDFS-14923](https://issues.apache.org/jira/browse/HDFS-14923) | Remove dead code from HealthMonitor |  Minor | . | Fei Hui | Fei Hui |
| [YARN-9914](https://issues.apache.org/jira/browse/YARN-9914) | Use separate configs for free disk space checking for full and not-full disks |  Minor | yarn | Jim Brennan | Jim Brennan |
| [HDFS-14935](https://issues.apache.org/jira/browse/HDFS-14935) | Refactor DFSNetworkTopology#isNodeInScope |  Minor | . | Lisheng Sun | Lisheng Sun |
| [YARN-2442](https://issues.apache.org/jira/browse/YARN-2442) | ResourceManager JMX UI does not give HA State |  Major | resourcemanager | Nishan Shetty | Rohith Sharma K S |
| [YARN-9889](https://issues.apache.org/jira/browse/YARN-9889) | [UI] Add Application Tag column to RM All Applications table |  Major | . | Kinga Marton | Kinga Marton |
| [HDFS-14936](https://issues.apache.org/jira/browse/HDFS-14936) | Add getNumOfChildren() for interface InnerNode |  Minor | . | Lisheng Sun | Lisheng Sun |
| [HDFS-14927](https://issues.apache.org/jira/browse/HDFS-14927) | RBF: Add metrics for async callers thread pool |  Minor | rbf | Leon Gao | Leon Gao |
| [HADOOP-16678](https://issues.apache.org/jira/browse/HADOOP-16678) | Review of ArrayWritable |  Minor | common | David Mollitor | David Mollitor |
| [HDFS-14775](https://issues.apache.org/jira/browse/HDFS-14775) | Add Timestamp for longest FSN write/read lock held log |  Major | . | Chen Zhang | Chen Zhang |
| [MAPREDUCE-7208](https://issues.apache.org/jira/browse/MAPREDUCE-7208) | Tuning TaskRuntimeEstimator |  Minor | . | Ahmed Hussein | Ahmed Hussein |
| [HDFS-14942](https://issues.apache.org/jira/browse/HDFS-14942) | Change Log Level to debug in JournalNodeSyncer#syncWithJournalAtIndex |  Minor | . | Lisheng Sun | Lisheng Sun |
| [HDFS-14815](https://issues.apache.org/jira/browse/HDFS-14815) | RBF: Update the quota in MountTable when calling setQuota on a MountTable src. |  Major | . | Jinglun | Jinglun |
| [YARN-9677](https://issues.apache.org/jira/browse/YARN-9677) | Make FpgaDevice and GpuDevice classes more similar to each other |  Major | . | Szilard Nemeth | Kevin Su |
| [YARN-9890](https://issues.apache.org/jira/browse/YARN-9890) | [UI2] Add Application tag to the app table and app detail page. |  Major | . | Kinga Marton | Kinga Marton |
| [HDFS-14928](https://issues.apache.org/jira/browse/HDFS-14928) | UI: unifying the WebUI across different components. |  Trivial | ui | Xieming Li | Xieming Li |
| [HDFS-14975](https://issues.apache.org/jira/browse/HDFS-14975) | Add LF for SetECPolicyCommand usage |  Trivial | . | Fei Hui | Fei Hui |
| [YARN-9537](https://issues.apache.org/jira/browse/YARN-9537) | Add configuration to disable AM preemption |  Major | fairscheduler | zhoukang | zhoukang |
| [HDFS-14979](https://issues.apache.org/jira/browse/HDFS-14979) | [Observer Node] Balancer should submit getBlocks to Observer Node when possible |  Major | balancer & mover, hdfs | Erik Krogen | Erik Krogen |
| [HADOOP-16705](https://issues.apache.org/jira/browse/HADOOP-16705) | MBeanInfoBuilder puts unnecessary memory pressure on the system with a debug log |  Major | metrics | Lukas Majercak | Lukas Majercak |
| [HADOOP-16691](https://issues.apache.org/jira/browse/HADOOP-16691) | Unify Logging in UserGroupInformation |  Minor | common | David Mollitor | David Mollitor |
| [HDFS-14882](https://issues.apache.org/jira/browse/HDFS-14882) | Consider DataNode load when #getBlockLocation |  Major | namenode | Xiaoqiao He | Xiaoqiao He |
| [HDFS-14974](https://issues.apache.org/jira/browse/HDFS-14974) | RBF: Make tests use free ports |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-14955](https://issues.apache.org/jira/browse/HDFS-14955) | RBF: getQuotaUsage() on mount point should return global quota. |  Minor | . | Jinglun | Jinglun |
| [HADOOP-16712](https://issues.apache.org/jira/browse/HADOOP-16712) | Config ha.failover-controller.active-standby-elector.zk.op.retries is not in core-default.xml |  Trivial | . | Wei-Chiu Chuang | Xieming Li |
| [YARN-9886](https://issues.apache.org/jira/browse/YARN-9886) | Queue mapping based on userid passed through application tag |  Major | scheduler | Kinga Marton | Szilard Nemeth |
| [HDFS-14952](https://issues.apache.org/jira/browse/HDFS-14952) | Skip safemode if blockTotal is 0 in new NN |  Trivial | namenode | Rajesh Balamohan | Xiaoqiao He |
| [HDFS-14995](https://issues.apache.org/jira/browse/HDFS-14995) | Use log variable directly instead of passing as argument in InvalidateBlocks#printBlockDeletionTime() |  Minor | . | Lisheng Sun | Lisheng Sun |
| [HDFS-14949](https://issues.apache.org/jira/browse/HDFS-14949) | Add getServerDefaults() support to HttpFS |  Major | . | Kihwal Lee | hemanthboyina |
| [HADOOP-15852](https://issues.apache.org/jira/browse/HADOOP-15852) | Refactor QuotaUsage |  Minor | common | David Mollitor | David Mollitor |
| [HDFS-15002](https://issues.apache.org/jira/browse/HDFS-15002) |  RBF: Fix annotation in RouterAdmin |  Trivial | . | Jinglun | Jinglun |
| [YARN-8842](https://issues.apache.org/jira/browse/YARN-8842) | Expose metrics for custom resource types in QueueMetrics |  Major | . | Szilard Nemeth | Szilard Nemeth |
| [HADOOP-16717](https://issues.apache.org/jira/browse/HADOOP-16717) | Remove GenericsUtil isLog4jLogger dependency on Log4jLoggerAdapter |  Major | . | David Mollitor | Xieming Li |
| [YARN-9966](https://issues.apache.org/jira/browse/YARN-9966) | Code duplication in UserGroupMappingPlacementRule |  Major | . | Szilard Nemeth | Kevin Su |
| [YARN-9991](https://issues.apache.org/jira/browse/YARN-9991) | Queue mapping based on userid passed through application tag: Change prefix to 'userid' |  Major | scheduler | Szilard Nemeth | Szilard Nemeth |
| [YARN-9362](https://issues.apache.org/jira/browse/YARN-9362) | Code cleanup in TestNMLeveldbStateStoreService |  Minor | . | Szilard Nemeth | Denes Gerencser |
| [YARN-9937](https://issues.apache.org/jira/browse/YARN-9937) | Add missing queue configs in RMWebService#CapacitySchedulerQueueInfo |  Major | capacity scheduler | Prabhu Joseph | Prabhu Joseph |
| [HDFS-15013](https://issues.apache.org/jira/browse/HDFS-15013) | Reduce NameNode overview tab response time |  Minor | namenode | HuangTao | HuangTao |
| [HADOOP-16718](https://issues.apache.org/jira/browse/HADOOP-16718) | Allow disabling Server Name Indication (SNI) for Jetty |  Major | . | Siyao Meng | Aravindan Vijayan |
| [YARN-9958](https://issues.apache.org/jira/browse/YARN-9958) | Remove the invalid lock in ContainerExecutor |  Major | . | Wanqiang Ji | Wanqiang Ji |
| [HADOOP-16729](https://issues.apache.org/jira/browse/HADOOP-16729) | Extract version numbers to head of pom.xml |  Minor | build | Tamas Penzes | Tamas Penzes |
| [MAPREDUCE-7250](https://issues.apache.org/jira/browse/MAPREDUCE-7250) | FrameworkUploader: skip replication check entirely if timeout == 0 |  Major | mrv2 | Peter Bacsko | Peter Bacsko |
| [YARN-9052](https://issues.apache.org/jira/browse/YARN-9052) | Replace all MockRM submit method definitions with a builder |  Minor | . | Szilard Nemeth | Szilard Nemeth |
| [YARN-9128](https://issues.apache.org/jira/browse/YARN-9128) | Use SerializationUtils from apache commons to serialize / deserialize ResourceMappings |  Major | . | Szilard Nemeth | Adam Antal |
| [HDFS-15023](https://issues.apache.org/jira/browse/HDFS-15023) | [SBN read] ZKFC should check the state before joining the election |  Major | . | Fei Hui | Fei Hui |
| [HADOOP-16735](https://issues.apache.org/jira/browse/HADOOP-16735) | Make it clearer in config default that EnvironmentVariableCredentialsProvider supports AWS\_SESSION\_TOKEN |  Minor | documentation, fs/s3 | Mingliang Liu | Mingliang Liu |
| [HDFS-15028](https://issues.apache.org/jira/browse/HDFS-15028) | Keep the capacity of volume and reduce a system call |  Minor | datanode | Yang Yun | Yang Yun |
| [YARN-10012](https://issues.apache.org/jira/browse/YARN-10012) | Guaranteed and max capacity queue metrics for custom resources |  Major | . | Jonathan Hung | Manikandan R |
| [HDFS-14522](https://issues.apache.org/jira/browse/HDFS-14522) | Allow compact property description in xml in httpfs |  Major | httpfs | Akira Ajisaka | Masatake Iwasaki |
| [YARN-5106](https://issues.apache.org/jira/browse/YARN-5106) | Provide a builder interface for FairScheduler allocations for use in tests |  Major | fairscheduler | Karthik Kambatla | Adam Antal |
| [HDFS-14854](https://issues.apache.org/jira/browse/HDFS-14854) | Create improved decommission monitor implementation |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-15050](https://issues.apache.org/jira/browse/HDFS-15050) | Optimize log information when DFSInputStream meet CannotObtainBlockLengthException |  Major | dfsclient | Xiaoqiao He | Xiaoqiao He |
| [HDFS-15016](https://issues.apache.org/jira/browse/HDFS-15016) | RBF: getDatanodeReport() should return the latest update |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-10033](https://issues.apache.org/jira/browse/YARN-10033) | TestProportionalCapacityPreemptionPolicy not initializing vcores for effective max resources |  Major | capacity scheduler, test | Eric Payne | Eric Payne |
| [YARN-10039](https://issues.apache.org/jira/browse/YARN-10039) | Allow disabling app submission from REST endpoints |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-9894](https://issues.apache.org/jira/browse/YARN-9894) | CapacitySchedulerPerf test for measuring hundreds of apps in a large number of queues. |  Major | capacity scheduler, test | Eric Payne | Eric Payne |
| [YARN-10038](https://issues.apache.org/jira/browse/YARN-10038) | [UI] Finish Time is not correctly parsed in the RM Apps page |  Minor | webapp | Íñigo Goiri | Íñigo Goiri |
| [HADOOP-16771](https://issues.apache.org/jira/browse/HADOOP-16771) | Update checkstyle to 8.26 and maven-checkstyle-plugin to 3.1.0 |  Major | build | Andras Bokor | Andras Bokor |
| [HDFS-15062](https://issues.apache.org/jira/browse/HDFS-15062) | Add LOG when sendIBRs failed |  Major | datanode | Fei Hui | Fei Hui |
| [YARN-10009](https://issues.apache.org/jira/browse/YARN-10009) | In Capacity Scheduler, DRC can treat minimum user limit percent as a max when custom resource is defined |  Critical | capacity scheduler | Eric Payne | Eric Payne |
| [YARN-10036](https://issues.apache.org/jira/browse/YARN-10036) | Install yarnpkg and upgrade nodejs in Dockerfile |  Major | buid, yarn-ui-v2 | Akira Ajisaka | Akira Ajisaka |
| [HDFS-12999](https://issues.apache.org/jira/browse/HDFS-12999) | When reach the end of the block group, it may not need to flush all the data packets(flushAllInternals) twice. |  Major | erasure-coding, hdfs-client | lufei | lufei |
| [HDFS-14997](https://issues.apache.org/jira/browse/HDFS-14997) | BPServiceActor processes commands from NameNode asynchronously |  Major | datanode | Xiaoqiao He | Xiaoqiao He |
| [HDFS-15003](https://issues.apache.org/jira/browse/HDFS-15003) | RBF: Make Router support storage type quota. |  Major | . | Jinglun | Jinglun |
| [HDFS-15081](https://issues.apache.org/jira/browse/HDFS-15081) | Typo in RetryCache#waitForCompletion annotation |  Trivial | namenode | Fei Hui | Fei Hui |
| [HDFS-15074](https://issues.apache.org/jira/browse/HDFS-15074) | DataNode.DataTransfer thread should catch all the expception and log it. |  Major | datanode | Surendra Singh Lilhore | hemanthboyina |
| [HDFS-14937](https://issues.apache.org/jira/browse/HDFS-14937) | [SBN read] ObserverReadProxyProvider should throw InterruptException |  Major | . | xuzq | xuzq |
| [HDFS-14740](https://issues.apache.org/jira/browse/HDFS-14740) | Recover data blocks from persistent memory read cache during datanode restarts |  Major | caching, datanode | Feilong He | Feilong He |
| [HADOOP-16777](https://issues.apache.org/jira/browse/HADOOP-16777) | Add Tez to LimitedPrivate of ClusterStorageCapacityExceededException |  Minor | common | Wang Yan | Wang Yan |
| [HDFS-15091](https://issues.apache.org/jira/browse/HDFS-15091) | Cache Admin and Quota Commands Should Check SuperUser Before Taking Lock |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-15090](https://issues.apache.org/jira/browse/HDFS-15090) | RBF: MountPoint Listing Should Return Flag Values Of Destination |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-15096](https://issues.apache.org/jira/browse/HDFS-15096) | RBF: GetServerDefaults Should be Cached At Router |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-15094](https://issues.apache.org/jira/browse/HDFS-15094) | RBF: Reuse ugi string in ConnectionPoolID |  Major | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-15993](https://issues.apache.org/jira/browse/HADOOP-15993) | Upgrade Kafka version in hadoop-kafka module |  Major | build, security | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15107](https://issues.apache.org/jira/browse/HDFS-15107) | dfs.client.server-defaults.validity.period.ms to support time units |  Major | . | Ayush Saxena | Ayush Saxena |
| [YARN-9989](https://issues.apache.org/jira/browse/YARN-9989) | Typo in CapacityScheduler documentation:  Runtime Configuration |  Major | . | Szilard Nemeth | Kevin Su |
| [YARN-9868](https://issues.apache.org/jira/browse/YARN-9868) | Validate %primary\_group queue in CS queue manager |  Major | . | Manikandan R | Manikandan R |
| [YARN-9912](https://issues.apache.org/jira/browse/YARN-9912) | Capacity scheduler: support u:user2:%secondary\_group queue mapping |  Major | capacity scheduler, capacityscheduler | Manikandan R | Manikandan R |
| [HDFS-15097](https://issues.apache.org/jira/browse/HDFS-15097) | Purge log in KMS and HttpFS |  Minor | httpfs, kms | Doris Gu | Doris Gu |
| [HDFS-15112](https://issues.apache.org/jira/browse/HDFS-15112) | RBF: Do not return FileNotFoundException when a subcluster is unavailable |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HADOOP-16753](https://issues.apache.org/jira/browse/HADOOP-16753) | Refactor HAAdmin |  Major | ha | Akira Ajisaka | Xieming Li |
| [HDFS-14968](https://issues.apache.org/jira/browse/HDFS-14968) | Add ability to know datanode staleness |  Minor | datanode, logging, namenode | Ahmed Hussein | Ahmed Hussein |
| [YARN-7913](https://issues.apache.org/jira/browse/YARN-7913) | Improve error handling when application recovery fails with exception |  Major | resourcemanager | Gergo Repas | Wilfred Spiegelenburg |
| [YARN-8472](https://issues.apache.org/jira/browse/YARN-8472) | YARN Container Phase 2 |  Major | . | Eric Yang | Eric Yang |
| [HDFS-15117](https://issues.apache.org/jira/browse/HDFS-15117) | EC: Add getECTopologyResultForPolicies to DistributedFileSystem |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-15119](https://issues.apache.org/jira/browse/HDFS-15119) | Allow expiration of cached locations in DFSInputStream |  Minor | dfsclient | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-16811](https://issues.apache.org/jira/browse/HADOOP-16811) | Use JUnit TemporaryFolder Rule in TestFileUtils |  Minor | common, test | David Mollitor | David Mollitor |
| [MAPREDUCE-7262](https://issues.apache.org/jira/browse/MAPREDUCE-7262) | MRApp helpers block for long intervals (500ms) |  Minor | mr-am | Ahmed Hussein | Ahmed Hussein |
| [YARN-9768](https://issues.apache.org/jira/browse/YARN-9768) | RM Renew Delegation token thread should timeout and retry |  Major | . | CR Hota | Manikandan R |
| [MAPREDUCE-7260](https://issues.apache.org/jira/browse/MAPREDUCE-7260) | Cross origin request support for Job history server web UI |  Critical | jobhistoryserver | Adam Antal | Adam Antal |
| [YARN-10084](https://issues.apache.org/jira/browse/YARN-10084) | Allow inheritance of max app lifetime / default app lifetime |  Major | capacity scheduler | Eric Payne | Eric Payne |
| [HDFS-12491](https://issues.apache.org/jira/browse/HDFS-12491) | Support wildcard in CLASSPATH for libhdfs |  Major | libhdfs | John Zhuge | Muhammad Samir Khan |
| [YARN-10116](https://issues.apache.org/jira/browse/YARN-10116) | Expose diagnostics in RMAppManager summary |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-9624](https://issues.apache.org/jira/browse/YARN-9624) | Use switch case for ProtoUtils#convertFromProtoFormat containerState |  Major | . | Bibin Chundatt | Bilwa S T |
| [HADOOP-16739](https://issues.apache.org/jira/browse/HADOOP-16739) | Fix native build failure of hadoop-pipes on CentOS 8 |  Major | tools/pipes | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-16847](https://issues.apache.org/jira/browse/HADOOP-16847) | Test TestGroupsCaching fail if HashSet iterates in a different order |  Minor | test | testfixer0 | testfixer0 |
| [HDFS-15150](https://issues.apache.org/jira/browse/HDFS-15150) | Introduce read write lock to Datanode |  Major | datanode | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-14758](https://issues.apache.org/jira/browse/HDFS-14758) | Decrease lease hard limit |  Minor | . | Eric Payne | hemanthboyina |
| [HDFS-15127](https://issues.apache.org/jira/browse/HDFS-15127) | RBF: Do not allow writes when a subcluster is unavailable for HASH\_ALL mount points. |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [MAPREDUCE-7263](https://issues.apache.org/jira/browse/MAPREDUCE-7263) | Remove obsolete validateTargetPath() from FrameworkUploader |  Major | mrv2 | Adam Antal | Hudáky Márton Gyula |
| [YARN-10137](https://issues.apache.org/jira/browse/YARN-10137) | UIv2 build is broken in trunk |  Critical | yarn, yarn-ui-v2 | Adam Antal | Adam Antal |
| [HDFS-15086](https://issues.apache.org/jira/browse/HDFS-15086) | Block scheduled counter never get decremet if the block got deleted before replication. |  Major | 3.1.1 | Surendra Singh Lilhore | hemanthboyina |
| [HADOOP-16850](https://issues.apache.org/jira/browse/HADOOP-16850) | Support getting thread info from thread group for JvmMetrics to improve the performance |  Major | metrics, performance | Tao Yang | Tao Yang |
| [HADOOP-13666](https://issues.apache.org/jira/browse/HADOOP-13666) | Supporting rack exclusion in countNumOfAvailableNodes in NetworkTopology |  Major | net | Íñigo Goiri | Íñigo Goiri |
| [HADOOP-16833](https://issues.apache.org/jira/browse/HADOOP-16833) | InstrumentedLock should log lock queue time |  Major | util | Stephen O'Donnell | Stephen O'Donnell |
| [YARN-8374](https://issues.apache.org/jira/browse/YARN-8374) | Upgrade objenesis to 2.6 |  Major | build, timelineservice | Jason Darrell Lowe | Akira Ajisaka |
| [HDFS-13739](https://issues.apache.org/jira/browse/HDFS-13739) | Add option to disable rack local write preference |  Major | balancer & mover, block placement, datanode, fs, hdfs, hdfs-client, namenode, nn, performance | Hari Sekhon | Ayush Saxena |
| [HDFS-15176](https://issues.apache.org/jira/browse/HDFS-15176) | Enable GcTimePercentage Metric in NameNode's JvmMetrics. |  Minor | . | Jinglun | Jinglun |
| [HDFS-15174](https://issues.apache.org/jira/browse/HDFS-15174) | Optimize ReplicaCachingGetSpaceUsed by reducing unnecessary io operations |  Major | . | Lisheng Sun | Lisheng Sun |
| [YARN-10074](https://issues.apache.org/jira/browse/YARN-10074) | Update netty to 4.1.42Final in yarn-csi |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-9018](https://issues.apache.org/jira/browse/YARN-9018) | Add functionality to AuxiliaryLocalPathHandler to return all locations to read for a given path |  Major | . | Kuhu Shukla | Kuhu Shukla |
| [HDFS-14861](https://issues.apache.org/jira/browse/HDFS-14861) | Reset LowRedundancyBlocks Iterator periodically |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-15120](https://issues.apache.org/jira/browse/HDFS-15120) | Refresh BlockPlacementPolicy at runtime. |  Major | . | Jinglun | Jinglun |
| [HDFS-15190](https://issues.apache.org/jira/browse/HDFS-15190) | HttpFS : Add Support for Storage Policy Satisfier |  Major | . | hemanthboyina | hemanthboyina |
| [HDFS-15033](https://issues.apache.org/jira/browse/HDFS-15033) | Support to save replica cached files to other place and make expired time configurable |  Minor | datanode | Yang Yun | Yang Yun |
| [YARN-10148](https://issues.apache.org/jira/browse/YARN-10148) | Add Unit test for queue ACL for both FS and CS |  Major | scheduler | Kinga Marton | Kinga Marton |
| [HADOOP-16899](https://issues.apache.org/jira/browse/HADOOP-16899) | Update HdfsDesign.md to reduce ambiguity |  Minor | documentation | Akshay Nehe | Akshay Nehe |
| [HADOOP-14630](https://issues.apache.org/jira/browse/HADOOP-14630) | Contract Tests to verify create, mkdirs and rename under a file is forbidden |  Major | fs, fs/azure, fs/s3, fs/swift | Steve Loughran | Steve Loughran |
| [HDFS-12101](https://issues.apache.org/jira/browse/HDFS-12101) | DFSClient.rename() to unwrap ParentNotDirectoryException; define policy for renames under a file |  Minor | fs | Steve Loughran | Steve Loughran |
| [HADOOP-16898](https://issues.apache.org/jira/browse/HADOOP-16898) | Batch listing of multiple directories to be an unstable interface |  Major | fs | Steve Loughran | Steve Loughran |
| [HADOOP-16772](https://issues.apache.org/jira/browse/HADOOP-16772) | Extract version numbers to head of pom.xml (addendum) |  Major | build | Tamas Penzes | Tamas Penzes |
| [YARN-9050](https://issues.apache.org/jira/browse/YARN-9050) | [Umbrella] Usability improvements for scheduler activities |  Major | capacityscheduler | Tao Yang | Tao Yang |
| [HDFS-15159](https://issues.apache.org/jira/browse/HDFS-15159) | Prevent adding same DN multiple times in PendingReconstructionBlocks |  Major | . | hemanthboyina | hemanthboyina |
| [HDFS-15197](https://issues.apache.org/jira/browse/HDFS-15197) | [SBN read] Change ObserverRetryOnActiveException log to debug |  Minor | hdfs | Chen Liang | Chen Liang |
| [HADOOP-16927](https://issues.apache.org/jira/browse/HADOOP-16927) | Update hadoop-thirdparty dependency version to 1.0.0 |  Major | . | Vinayakumar B | Vinayakumar B |
| [HADOOP-15620](https://issues.apache.org/jira/browse/HADOOP-15620) | Über-jira: S3A phase VI: Hadoop 3.3 features |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-13377](https://issues.apache.org/jira/browse/HDFS-13377) | The owner of folder can set quota for his sub folder |  Minor | namenode | Yang Yun | Yang Yun |
| [HADOOP-16938](https://issues.apache.org/jira/browse/HADOOP-16938) | Make non-HA proxy providers pluggable |  Major | . | Roger Liu | Roger Liu |
| [HDFS-15154](https://issues.apache.org/jira/browse/HDFS-15154) | Allow only hdfs superusers the ability to assign HDFS storage policies |  Major | hdfs | Bob Cauthen | Siddharth Wagle |
| [YARN-10200](https://issues.apache.org/jira/browse/YARN-10200) | Add number of containers to RMAppManager summary |  Major | . | Jonathan Hung | Jonathan Hung |
| [HDFS-15075](https://issues.apache.org/jira/browse/HDFS-15075) | Remove process command timing from BPServiceActor |  Major | . | Íñigo Goiri | Xiaoqiao He |
| [YARN-10210](https://issues.apache.org/jira/browse/YARN-10210) | Add a RMFailoverProxyProvider that does DNS resolution on failover |  Major | . | Roger Liu | Roger Liu |
| [HDFS-15238](https://issues.apache.org/jira/browse/HDFS-15238) | RBF：NamenodeHeartbeatService caused memory to grow rapidly |  Major | . | xuzq | xuzq |
| [HDFS-15239](https://issues.apache.org/jira/browse/HDFS-15239) | Add button to go to the parent directory in the explorer |  Major | . | Íñigo Goiri | hemanthboyina |
| [HADOOP-16910](https://issues.apache.org/jira/browse/HADOOP-16910) | ABFS Streams to update FileSystem.Statistics counters on IO. |  Major | fs/azure | Mehakmeet Singh | Mehakmeet Singh |
| [HADOOP-15619](https://issues.apache.org/jira/browse/HADOOP-15619) | Über-JIRA: S3Guard Phase IV: Hadoop 3.3 features |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-15258](https://issues.apache.org/jira/browse/HDFS-15258) | RBF: Mark Router FSCK unstable |  Major | rbf | Akira Ajisaka | Akira Ajisaka |
| [YARN-10063](https://issues.apache.org/jira/browse/YARN-10063) | Usage output of container-executor binary needs to include --http/--https argument |  Minor | . | Siddharth Ahuja | Siddharth Ahuja |
| [MAPREDUCE-7266](https://issues.apache.org/jira/browse/MAPREDUCE-7266) | historyContext doesn't need to be a class attribute inside JobHistoryServer |  Minor | jobhistoryserver | Siddharth Ahuja | Siddharth Ahuja |
| [YARN-10003](https://issues.apache.org/jira/browse/YARN-10003) | YarnConfigurationStore#checkVersion throws exception that belongs to RMStateStore |  Major | . | Szilard Nemeth | Benjamin Teke |
| [YARN-10212](https://issues.apache.org/jira/browse/YARN-10212) | Create separate configuration for max global AM attempts |  Major | . | Jonathan Hung | Bilwa S T |
| [HDFS-14476](https://issues.apache.org/jira/browse/HDFS-14476) | lock too long when fix inconsistent blocks between disk and in-memory |  Major | datanode | Sean Chow | Sean Chow |
| [YARN-5277](https://issues.apache.org/jira/browse/YARN-5277) | When localizers fail due to resource timestamps being out, provide more diagnostics |  Major | nodemanager | Steve Loughran | Siddharth Ahuja |
| [YARN-9995](https://issues.apache.org/jira/browse/YARN-9995) | Code cleanup in TestSchedConfCLI |  Minor | . | Szilard Nemeth | Bilwa S T |
| [YARN-9354](https://issues.apache.org/jira/browse/YARN-9354) | Resources should be created with ResourceTypesTestHelper instead of TestUtils |  Trivial | . | Szilard Nemeth | Andras Gyori |
| [YARN-10002](https://issues.apache.org/jira/browse/YARN-10002) | Code cleanup and improvements in ConfigurationStoreBaseTest |  Minor | . | Szilard Nemeth | Benjamin Teke |
| [YARN-9954](https://issues.apache.org/jira/browse/YARN-9954) | Configurable max application tags and max tag length |  Major | . | Jonathan Hung | Bilwa S T |
| [HADOOP-16972](https://issues.apache.org/jira/browse/HADOOP-16972) | Ignore AuthenticationFilterInitializer for KMSWebServer |  Blocker | kms | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-10001](https://issues.apache.org/jira/browse/YARN-10001) | Add explanation of unimplemented methods in InMemoryConfigurationStore |  Major | . | Szilard Nemeth | Siddharth Ahuja |
| [HADOOP-17001](https://issues.apache.org/jira/browse/HADOOP-17001) | The suffix name of the unified compression class |  Major | io | bianqi | bianqi |
| [YARN-9997](https://issues.apache.org/jira/browse/YARN-9997) | Code cleanup in ZKConfigurationStore |  Minor | . | Szilard Nemeth | Andras Gyori |
| [YARN-9996](https://issues.apache.org/jira/browse/YARN-9996) | Code cleanup in QueueAdminConfigurationMutationACLPolicy |  Major | . | Szilard Nemeth | Siddharth Ahuja |
| [HADOOP-16914](https://issues.apache.org/jira/browse/HADOOP-16914) | Adding Output Stream Counters in ABFS |  Major | fs/azure | Mehakmeet Singh | Mehakmeet Singh |
| [YARN-9998](https://issues.apache.org/jira/browse/YARN-9998) | Code cleanup in LeveldbConfigurationStore |  Minor | . | Szilard Nemeth | Benjamin Teke |
| [YARN-9999](https://issues.apache.org/jira/browse/YARN-9999) | TestFSSchedulerConfigurationStore: Extend from ConfigurationStoreBaseTest, general code cleanup |  Minor | . | Szilard Nemeth | Benjamin Teke |
| [YARN-10189](https://issues.apache.org/jira/browse/YARN-10189) | Code cleanup in LeveldbRMStateStore |  Minor | . | Benjamin Teke | Benjamin Teke |
| [YARN-10237](https://issues.apache.org/jira/browse/YARN-10237) | Add isAbsoluteResource config for queue in scheduler response |  Minor | scheduler | Prabhu Joseph | Prabhu Joseph |
| [YARN-10160](https://issues.apache.org/jira/browse/YARN-10160) | Add auto queue creation related configs to RMWebService#CapacitySchedulerQueueInfo |  Major | . | Prabhu Joseph | Prabhu Joseph |
| [HADOOP-17046](https://issues.apache.org/jira/browse/HADOOP-17046) | Support downstreams' existing Hadoop-rpc implementations using non-shaded protobuf classes. |  Major | rpc-server | Vinayakumar B | Vinayakumar B |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-8035](https://issues.apache.org/jira/browse/YARN-8035) | Uncaught exception in ContainersMonitorImpl during relaunch due to the process ID changing |  Major | . | Shane Kumpf | Shane Kumpf |
| [HDFS-13376](https://issues.apache.org/jira/browse/HDFS-13376) | Specify minimum GCC version to avoid TLS support error in Build of hadoop-hdfs-native-client |  Major | build, documentation, native | LiXin Ge | LiXin Ge |
| [YARN-8388](https://issues.apache.org/jira/browse/YARN-8388) | TestCGroupElasticMemoryController.testNormalExit() hangs on Linux |  Major | nodemanager | Haibo Chen | Miklos Szegedi |
| [YARN-8108](https://issues.apache.org/jira/browse/YARN-8108) | RM metrics rest API throws GSSException in kerberized environment |  Blocker | . | Kshitij Badani | Sunil G |
| [HADOOP-15814](https://issues.apache.org/jira/browse/HADOOP-15814) | Maven 3.3.3 unable to parse pom file |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-15817](https://issues.apache.org/jira/browse/HADOOP-15817) | Reuse Object Mapper in KMSJSONReader |  Major | kms | Jonathan Turner Eagles | Jonathan Turner Eagles |
| [HDFS-13957](https://issues.apache.org/jira/browse/HDFS-13957) | Fix incorrect option used in description of InMemoryAliasMap |  Minor | documentation | Yiqun Lin | Yiqun Lin |
| [YARN-4254](https://issues.apache.org/jira/browse/YARN-4254) | ApplicationAttempt stuck for ever due to UnknownHostException |  Major | . | Bibin Chundatt | Bibin Chundatt |
| [HDFS-13964](https://issues.apache.org/jira/browse/HDFS-13964) | RBF: TestRouterWebHDFSContractAppend fails with No Active Namenode under nameservice |  Major | . | Ayush Saxena | Ayush Saxena |
| [YARN-8788](https://issues.apache.org/jira/browse/YARN-8788) | mvn package -Pyarn-ui fails on JDK9 |  Major | . | Akira Ajisaka | Vidura Bhathiya Mudalige |
| [YARN-7825](https://issues.apache.org/jira/browse/YARN-7825) | [UI2] Maintain constant horizontal application info bar for Application Attempt page |  Major | yarn-ui-v2 | Yesha Vora | Akhil PB |
| [YARN-8659](https://issues.apache.org/jira/browse/YARN-8659) | RMWebServices returns only RUNNING apps when filtered with queue |  Major | yarn | Prabhu Joseph | Szilard Nemeth |
| [YARN-8843](https://issues.apache.org/jira/browse/YARN-8843) | updateNodeResource does not support units for memory |  Minor | . | Íñigo Goiri | Manikandan R |
| [HDFS-13962](https://issues.apache.org/jira/browse/HDFS-13962) | Add null check for add-replica pool to avoid lock acquiring |  Major | . | Yiqun Lin | Surendra Singh Lilhore |
| [MAPREDUCE-7035](https://issues.apache.org/jira/browse/MAPREDUCE-7035) | Skip javadoc build for auto-generated sources in hadoop-mapreduce-client |  Minor | client, documentation | Mukul Kumar Singh | Mukul Kumar Singh |
| [YARN-8853](https://issues.apache.org/jira/browse/YARN-8853) | [UI2] Application Attempts tab is not shown correctly when there are no attempts |  Major | yarn-ui-v2 | Charan Hebri | Akhil PB |
| [MAPREDUCE-7130](https://issues.apache.org/jira/browse/MAPREDUCE-7130) | Rumen crashes trying to handle MRAppMaster recovery events |  Minor | tools/rumen | Jonathan Bender | Peter Bacsko |
| [HDFS-13949](https://issues.apache.org/jira/browse/HDFS-13949) | Correct the description of dfs.datanode.disk.check.timeout in hdfs-default.xml |  Minor | documentation | Toshihiro Suzuki | Toshihiro Suzuki |
| [HADOOP-15708](https://issues.apache.org/jira/browse/HADOOP-15708) | Reading values from Configuration before adding deprecations make it impossible to read value with deprecated key |  Minor | conf | Szilard Nemeth | Zoltan Siegl |
| [YARN-8666](https://issues.apache.org/jira/browse/YARN-8666) | [UI2] Remove application tab from YARN Queue Page |  Major | yarn-ui-v2 | Yesha Vora | Yesha Vora |
| [YARN-8753](https://issues.apache.org/jira/browse/YARN-8753) | [UI2] Lost nodes representation missing from Nodemanagers Chart |  Major | yarn-ui-v2 | Yesha Vora | Yesha Vora |
| [YARN-8710](https://issues.apache.org/jira/browse/YARN-8710) | Service AM should set a finite limit on NM container max retries |  Major | yarn-native-services | Suma Shivaprasad | Suma Shivaprasad |
| [HDFS-13951](https://issues.apache.org/jira/browse/HDFS-13951) | HDFS DelegationTokenFetcher can't print non-HDFS tokens in a tokenfile |  Minor | tools | Steve Loughran | Steve Loughran |
| [HDFS-13945](https://issues.apache.org/jira/browse/HDFS-13945) | TestDataNodeVolumeFailure is Flaky |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-13802](https://issues.apache.org/jira/browse/HDFS-13802) | RBF: Remove FSCK from Router Web UI |  Major | . | Fei Hui | Fei Hui |
| [YARN-8830](https://issues.apache.org/jira/browse/YARN-8830) | SLS tool fix node addition |  Critical | . | Bibin Chundatt | Bibin Chundatt |
| [YARN-8869](https://issues.apache.org/jira/browse/YARN-8869) | YARN Service Client might not work correctly with RM REST API for Kerberos authentication |  Blocker | . | Eric Yang | Eric Yang |
| [YARN-8775](https://issues.apache.org/jira/browse/YARN-8775) | TestDiskFailures.testLocalDirsFailures sometimes can fail on concurrent File modifications |  Major | test, yarn | Antal Bálint Steinbach | Antal Bálint Steinbach |
| [HADOOP-15853](https://issues.apache.org/jira/browse/HADOOP-15853) | TestConfigurationDeprecation leaves behind a temp file, resulting in a license issue |  Major | test | Robert Kanter | Ayush Saxena |
| [YARN-8879](https://issues.apache.org/jira/browse/YARN-8879) | Kerberos principal is needed when submitting a submarine job |  Critical | . | Zac Zhou | Zac Zhou |
| [HDFS-13993](https://issues.apache.org/jira/browse/HDFS-13993) | TestDataNodeVolumeFailure#testTolerateVolumeFailuresAfterAddingMoreVolumes is flaky |  Major | . | Ayush Saxena | Ayush Saxena |
| [YARN-8810](https://issues.apache.org/jira/browse/YARN-8810) | Yarn Service: discrepancy between hashcode and equals of ConfigFile |  Minor | . | Chandni Singh | Chandni Singh |
| [YARN-8759](https://issues.apache.org/jira/browse/YARN-8759) | Copy of resource-types.xml is not deleted if test fails, causes other test failures |  Major | yarn | Antal Bálint Steinbach | Antal Bálint Steinbach |
| [HDFS-14000](https://issues.apache.org/jira/browse/HDFS-14000) | RBF: Documentation should reflect right scripts for v3.0 and above |  Major | . | CR Hota | CR Hota |
| [YARN-8868](https://issues.apache.org/jira/browse/YARN-8868) | Set HTTPOnly attribute to Cookie |  Major | . | Chandni Singh | Chandni Singh |
| [YARN-8864](https://issues.apache.org/jira/browse/YARN-8864) | NM incorrectly logs container user as the user who sent a start/stop container request in its audit log |  Major | nodemanager | Haibo Chen | Wilfred Spiegelenburg |
| [HDFS-13990](https://issues.apache.org/jira/browse/HDFS-13990) | Synchronization Issue With HashResolver |  Minor | federation | David Mollitor | David Mollitor |
| [HDFS-14003](https://issues.apache.org/jira/browse/HDFS-14003) | Fix findbugs warning in trunk for FSImageFormatPBINode |  Major | . | Yiqun Lin | Yiqun Lin |
| [HDFS-14005](https://issues.apache.org/jira/browse/HDFS-14005) | RBF: Web UI update to bootstrap-3.3.7 |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-14002](https://issues.apache.org/jira/browse/HDFS-14002) | TestLayoutVersion#testNameNodeFeatureMinimumCompatibleLayoutVersions fails |  Major | test | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-15418](https://issues.apache.org/jira/browse/HADOOP-15418) | Hadoop KMSAuthenticationFilter needs to use getPropsByPrefix instead of iterator to avoid ConcurrentModificationException |  Major | common | Suma Shivaprasad | Suma Shivaprasad |
| [HDFS-14007](https://issues.apache.org/jira/browse/HDFS-14007) | Incompatible layout when generating FSImage |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-9872](https://issues.apache.org/jira/browse/HDFS-9872) | HDFS bytes-default configurations should accept multiple size units |  Major | . | Yiqun Lin | Yiqun Lin |
| [YARN-8910](https://issues.apache.org/jira/browse/YARN-8910) | Misleading log statement in NM when max retries is -1 |  Minor | . | Chandni Singh | Chandni Singh |
| [HADOOP-15850](https://issues.apache.org/jira/browse/HADOOP-15850) | CopyCommitter#concatFileChunks should check that the blocks per chunk is not 0 |  Critical | tools/distcp | Ted Yu | Ted Yu |
| [HDFS-13983](https://issues.apache.org/jira/browse/HDFS-13983) | TestOfflineImageViewer crashes in windows |  Major | . | Vinayakumar B | Vinayakumar B |
| [YARN-7502](https://issues.apache.org/jira/browse/YARN-7502) | Nodemanager restart docs should describe nodemanager supervised property |  Major | documentation | Jason Darrell Lowe | Suma Shivaprasad |
| [HADOOP-15866](https://issues.apache.org/jira/browse/HADOOP-15866) | Renamed HADOOP\_SECURITY\_GROUP\_SHELL\_COMMAND\_TIMEOUT keys break compatibility |  Blocker | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-8922](https://issues.apache.org/jira/browse/YARN-8922) | Fix test-container-executor |  Major | test | Robert Kanter | Robert Kanter |
| [YARN-8826](https://issues.apache.org/jira/browse/YARN-8826) | Fix lingering timeline collector after serviceStop in TimelineCollectorManager |  Trivial | ATSv2 | Prabha Manepalli | Prabha Manepalli |
| [HADOOP-15873](https://issues.apache.org/jira/browse/HADOOP-15873) | Add JavaBeans Activation Framework API to LICENSE.txt |  Major | . | Wei-Chiu Chuang | Akira Ajisaka |
| [YARN-8919](https://issues.apache.org/jira/browse/YARN-8919) | Some tests fail due to NoClassDefFoundError for OperatorCreationException |  Critical | . | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-14004](https://issues.apache.org/jira/browse/HDFS-14004) | TestLeaseRecovery2#testCloseWhileRecoverLease fails intermittently in trunk |  Major | . | Ayush Saxena | Ayush Saxena |
| [MAPREDUCE-4669](https://issues.apache.org/jira/browse/MAPREDUCE-4669) | MRAM web UI does not work with HTTPS |  Major | mr-am | Alejandro Abdelnur | Robert Kanter |
| [YARN-8814](https://issues.apache.org/jira/browse/YARN-8814) | Yarn Service Upgrade: Update the swagger definition |  Major | . | Chandni Singh | Chandni Singh |
| [HADOOP-15856](https://issues.apache.org/jira/browse/HADOOP-15856) | Trunk build fails to compile native on Windows |  Blocker | native | Vinayakumar B | Vinayakumar B |
| [YARN-8939](https://issues.apache.org/jira/browse/YARN-8939) | Javadoc build fails in hadoop-yarn-csi |  Major | . | Takanobu Asanuma | Weiwei Yang |
| [YARN-8938](https://issues.apache.org/jira/browse/YARN-8938) | Add service upgrade cancel and express examples to the service upgrade doc |  Minor | . | Chandni Singh | Chandni Singh |
| [HDFS-14021](https://issues.apache.org/jira/browse/HDFS-14021) | TestReconstructStripedBlocksWithRackAwareness#testReconstructForNotEnoughRacks fails intermittently |  Major | erasure-coding, test | Xiao Chen | Xiao Chen |
| [MAPREDUCE-7151](https://issues.apache.org/jira/browse/MAPREDUCE-7151) | RMContainerAllocator#handleJobPriorityChange expects application\_priority always |  Major | . | Bibin Chundatt | Bilwa S T |
| [YARN-8929](https://issues.apache.org/jira/browse/YARN-8929) | DefaultOOMHandler should only pick running containers to kill upon oom events |  Major | nodemanager | Haibo Chen | Haibo Chen |
| [YARN-8587](https://issues.apache.org/jira/browse/YARN-8587) | Delays are noticed to launch docker container |  Major | . | Yesha Vora | dockerzhang |
| [HDFS-14025](https://issues.apache.org/jira/browse/HDFS-14025) | TestPendingReconstruction.testPendingAndInvalidate fails |  Major | . | Ayush Saxena | Ayush Saxena |
| [YARN-8941](https://issues.apache.org/jira/browse/YARN-8941) | Fix findbugs warnings in hadoop-yarn-csi |  Minor | . | Takanobu Asanuma | Weiwei Yang |
| [YARN-8930](https://issues.apache.org/jira/browse/YARN-8930) | CGroup-based strict container memory enforcement does not work with CGroupElasticMemoryController |  Major | nodemanager | Haibo Chen | Haibo Chen |
| [HDFS-13959](https://issues.apache.org/jira/browse/HDFS-13959) | TestUpgradeDomainBlockPlacementPolicy is flaky |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-14028](https://issues.apache.org/jira/browse/HDFS-14028) | HDFS OIV temporary dir deletes folder |  Major | hdfs | Adam Antal | Adam Antal |
| [HDFS-14027](https://issues.apache.org/jira/browse/HDFS-14027) | DFSStripedOutputStream should implement both hsync methods |  Critical | erasure-coding | Xiao Chen | Xiao Chen |
| [YARN-8950](https://issues.apache.org/jira/browse/YARN-8950) | Compilation fails with dependency convergence error for hbase.profile=2.0 |  Blocker | . | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-8854](https://issues.apache.org/jira/browse/YARN-8854) | Upgrade jquery datatable version references to v1.10.19 |  Critical | . | Akhil PB | Akhil PB |
| [HADOOP-15886](https://issues.apache.org/jira/browse/HADOOP-15886) | Fix findbugs warnings in RegistryDNS.java |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [YARN-8897](https://issues.apache.org/jira/browse/YARN-8897) | LoadBasedRouterPolicy throws NPE in case of sub cluster unavailability |  Minor | federation, router | Akshay Agarwal | Bilwa S T |
| [HDFS-14049](https://issues.apache.org/jira/browse/HDFS-14049) | TestHttpFSServerWebServer fails on Windows because of missing winutils.exe |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HADOOP-15899](https://issues.apache.org/jira/browse/HADOOP-15899) | Update AWS Java SDK versions in NOTICE.txt |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-15900](https://issues.apache.org/jira/browse/HADOOP-15900) | Update JSch versions in LICENSE.txt |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [HDFS-14042](https://issues.apache.org/jira/browse/HDFS-14042) | Fix NPE when PROVIDED storage is missing |  Major | . | Íñigo Goiri | Virajith Jalaparti |
| [HDFS-14043](https://issues.apache.org/jira/browse/HDFS-14043) | Tolerate corrupted seen\_txid file |  Major | hdfs, namenode | Lukas Majercak | Lukas Majercak |
| [YARN-8858](https://issues.apache.org/jira/browse/YARN-8858) | CapacityScheduler should respect maximum node resource when per-queue maximum-allocation is being used. |  Major | . | Sumana Sathish | Wangda Tan |
| [YARN-8970](https://issues.apache.org/jira/browse/YARN-8970) | Improve the debug message in CS#allocateContainerOnSingleNode |  Trivial | . | Weiwei Yang | Zhankun Tang |
| [YARN-8865](https://issues.apache.org/jira/browse/YARN-8865) | RMStateStore contains large number of expired RMDelegationToken |  Major | resourcemanager | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [HDFS-14048](https://issues.apache.org/jira/browse/HDFS-14048) | DFSOutputStream close() throws exception on subsequent call after DataNode restart |  Major | hdfs-client | Erik Krogen | Erik Krogen |
| [MAPREDUCE-7156](https://issues.apache.org/jira/browse/MAPREDUCE-7156) | NullPointerException when reaching max shuffle connections |  Major | mrv2 | Peter Bacsko | Peter Bacsko |
| [YARN-8866](https://issues.apache.org/jira/browse/YARN-8866) | Fix a parsing error for crossdomain.xml |  Major | build, yarn-ui-v2 | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-6729](https://issues.apache.org/jira/browse/YARN-6729) | Clarify documentation on how to enable cgroup support |  Major | nodemanager | Yufei Gu | Zhankun Tang |
| [HDFS-14039](https://issues.apache.org/jira/browse/HDFS-14039) | ec -listPolicies doesn't show correct state for the default policy when the default is not RS(6,3) |  Major | erasure-coding | Xiao Chen | Kitti Nanasi |
| [HADOOP-15903](https://issues.apache.org/jira/browse/HADOOP-15903) | Allow HttpServer2 to discover resources in /static when symlinks are used |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-8990](https://issues.apache.org/jira/browse/YARN-8990) | Fix fair scheduler race condition in app submit and queue cleanup |  Blocker | fairscheduler | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [HADOOP-15916](https://issues.apache.org/jira/browse/HADOOP-15916) | Upgrade Maven Surefire plugin to 3.0.0-M1 |  Blocker | build | Akira Ajisaka | Akira Ajisaka |
| [YARN-9002](https://issues.apache.org/jira/browse/YARN-9002) | YARN Service keytab does not support s3, wasb, gs and is restricted to HDFS and local filesystem only |  Major | yarn-native-services | Gour Saha | Gour Saha |
| [YARN-8233](https://issues.apache.org/jira/browse/YARN-8233) | NPE in CapacityScheduler#tryCommit when handling allocate/reserve proposal whose allocatedOrReservedContainer is null |  Critical | capacityscheduler | Tao Yang | Tao Yang |
| [HDFS-14065](https://issues.apache.org/jira/browse/HDFS-14065) | Failed Storage Locations shows nothing in the Datanode Volume Failures |  Major | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-15923](https://issues.apache.org/jira/browse/HADOOP-15923) | create-release script should set max-cache-ttl as well as default-cache-ttl for gpg-agent |  Blocker | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-15912](https://issues.apache.org/jira/browse/HADOOP-15912) | start-build-env.sh still creates an invalid /etc/sudoers.d/hadoop-build-${USER\_ID} file entry after HADOOP-15802 |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-15869](https://issues.apache.org/jira/browse/HADOOP-15869) | BlockDecompressorStream#decompress should not return -1 in case of IOException. |  Major | . | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [MAPREDUCE-7158](https://issues.apache.org/jira/browse/MAPREDUCE-7158) | Inefficient Flush Logic in JobHistory EventWriter |  Major | . | Zichen Sun | Zichen Sun |
| [HADOOP-15930](https://issues.apache.org/jira/browse/HADOOP-15930) | Exclude MD5 checksum files from release artifact |  Critical | build | Akira Ajisaka | Akira Ajisaka |
| [YARN-8856](https://issues.apache.org/jira/browse/YARN-8856) | TestTimelineReaderWebServicesHBaseStorage tests failing with NoClassDefFoundError |  Major | . | Jason Darrell Lowe | Sushil Ks |
| [HDFS-14054](https://issues.apache.org/jira/browse/HDFS-14054) | TestLeaseRecovery2: testHardLeaseRecoveryAfterNameNodeRestart2 and testHardLeaseRecoveryWithRenameAfterNameNodeRestart are flaky |  Major | . | Zsolt Venczel | Zsolt Venczel |
| [HADOOP-15925](https://issues.apache.org/jira/browse/HADOOP-15925) | The config and log of gpg-agent are removed in create-release script |  Major | build | Akira Ajisaka | Dinesh Chitlangia |
| [HDFS-13963](https://issues.apache.org/jira/browse/HDFS-13963) | NN UI is broken with IE11 |  Minor | namenode, ui | Daisuke Kobayashi | Ayush Saxena |
| [HDFS-14056](https://issues.apache.org/jira/browse/HDFS-14056) | Fix error messages in HDFS-12716 |  Minor | hdfs | Adam Antal | Ayush Saxena |
| [HADOOP-15939](https://issues.apache.org/jira/browse/HADOOP-15939) | Filter overlapping objenesis class in hadoop-client-minicluster |  Minor | build | Xiaoyu Yao | Xiaoyu Yao |
| [YARN-8936](https://issues.apache.org/jira/browse/YARN-8936) | Bump up Atsv2 hbase versions |  Major | . | Rohith Sharma K S | Vrushali C |
| [YARN-8984](https://issues.apache.org/jira/browse/YARN-8984) | AMRMClient#OutstandingSchedRequests leaks when AllocationTags is null or empty |  Critical | . | Yang Wang | Yang Wang |
| [YARN-9042](https://issues.apache.org/jira/browse/YARN-9042) | Javadoc error in deviceplugin package |  Major | . | Rohith Sharma K S | Zhankun Tang |
| [HADOOP-15358](https://issues.apache.org/jira/browse/HADOOP-15358) | SFTPConnectionPool connections leakage |  Critical | fs | Mikhail Pryakhin | Mikhail Pryakhin |
| [HADOOP-15948](https://issues.apache.org/jira/browse/HADOOP-15948) | Inconsistency in get and put syntax if filename/dirname contains space |  Minor | fs | vivek kumar | Ayush Saxena |
| [HDFS-13816](https://issues.apache.org/jira/browse/HDFS-13816) | dfs.getQuotaUsage() throws NPE on non-existent dir instead of FileNotFoundException |  Major | namenode | Vinayakumar B | Vinayakumar B |
| [MAPREDUCE-7162](https://issues.apache.org/jira/browse/MAPREDUCE-7162) | TestEvents#testEvents fails |  Critical | jobhistoryserver, test | Zhaohui Xin | Zhaohui Xin |
| [YARN-9056](https://issues.apache.org/jira/browse/YARN-9056) | Yarn Service Upgrade: Instance state changes from UPGRADING to READY without performing a readiness check |  Critical | . | Chandni Singh | Chandni Singh |
| [MAPREDUCE-6190](https://issues.apache.org/jira/browse/MAPREDUCE-6190) | If a task stucks before its first heartbeat, it never timeouts and the MR job becomes stuck |  Major | . | Ankit Malhotra | Zhaohui Xin |
| [YARN-8812](https://issues.apache.org/jira/browse/YARN-8812) | Containers fail during creating a symlink which started with hyphen for a resource file |  Minor | . | Oleksandr Shevchenko | Oleksandr Shevchenko |
| [YARN-9044](https://issues.apache.org/jira/browse/YARN-9044) | LogsCLI should contact ATSv2 for "-am" option |  Major | . | Rohith Sharma K S | Rohith Sharma K S |
| [HADOOP-15947](https://issues.apache.org/jira/browse/HADOOP-15947) | Fix ITestDynamoDBMetadataStore test error issues |  Major | fs/s3 | Gabor Bota | Gabor Bota |
| [YARN-9030](https://issues.apache.org/jira/browse/YARN-9030) | Log aggregation changes to handle filesystems which do not support setting permissions |  Major | log-aggregation | Suma Shivaprasad | Suma Shivaprasad |
| [YARN-8948](https://issues.apache.org/jira/browse/YARN-8948) | PlacementRule interface should be for all YarnSchedulers |  Major | . | Bibin Chundatt | Bibin Chundatt |
| [YARN-9067](https://issues.apache.org/jira/browse/YARN-9067) | YARN Resource Manager is running OOM because of leak of Configuration Object |  Major | yarn-native-services | Eric Yang | Eric Yang |
| [YARN-9010](https://issues.apache.org/jira/browse/YARN-9010) | Fix the incorrect trailing slash deletion in constructor method of CGroupsHandlerImpl |  Major | . | Zhankun Tang | Zhankun Tang |
| [MAPREDUCE-7165](https://issues.apache.org/jira/browse/MAPREDUCE-7165) | mapred-site.xml is misformatted in single node setup document |  Major | documentation | Akira Ajisaka | Zhaohui Xin |
| [HDFS-14075](https://issues.apache.org/jira/browse/HDFS-14075) | Terminate the namenode when failed to start log segment |  Critical | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-15970](https://issues.apache.org/jira/browse/HADOOP-15970) | Upgrade plexus-utils from 2.0.5 to 3.1.0 |  Major | security | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-15974](https://issues.apache.org/jira/browse/HADOOP-15974) | Upgrade Curator version to 2.13.0 to fix ZK tests |  Major | . | Jason Darrell Lowe | Akira Ajisaka |
| [YARN-9071](https://issues.apache.org/jira/browse/YARN-9071) | NM and service AM don't have updated status for reinitialized containers |  Critical | . | Billie Rinaldi | Chandni Singh |
| [YARN-8994](https://issues.apache.org/jira/browse/YARN-8994) | Fix race condition between move app and queue cleanup in Fair Scheduler |  Major | fairscheduler | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [YARN-9019](https://issues.apache.org/jira/browse/YARN-9019) | Ratio calculation of ResourceCalculator implementations could return NaN |  Major | . | Szilard Nemeth | Szilard Nemeth |
| [YARN-9025](https://issues.apache.org/jira/browse/YARN-9025) | TestFairScheduler#testChildMaxResources is flaky |  Major | . | Szilard Nemeth | Szilard Nemeth |
| [MAPREDUCE-7159](https://issues.apache.org/jira/browse/MAPREDUCE-7159) | FrameworkUploader: ensure proper permissions of generated framework tar.gz if restrictive umask is used |  Major | mrv2 | Peter Bacsko | Peter Bacsko |
| [YARN-9009](https://issues.apache.org/jira/browse/YARN-9009) | Fix flaky test TestEntityGroupFSTimelineStore.testCleanLogs |  Minor | . | OrDTesters | OrDTesters |
| [YARN-8738](https://issues.apache.org/jira/browse/YARN-8738) | FairScheduler should not parse negative maxResources or minResources values as positive |  Major | fairscheduler | Sen Zhao | Szilard Nemeth |
| [HDFS-14137](https://issues.apache.org/jira/browse/HDFS-14137) | TestMaintenanceState fails with ArrayIndexOutOfBound Exception |  Major | . | Ayush Saxena | Ayush Saxena |
| [YARN-9114](https://issues.apache.org/jira/browse/YARN-9114) | [UI2] log service redirect url need to support user name |  Major | webapp, yarn-ui-v2 | Sunil G | Akhil PB |
| [HADOOP-15995](https://issues.apache.org/jira/browse/HADOOP-15995) | Add ldap.bind.password.alias in LdapGroupsMapping to distinguish aliases when using multiple providers through CompositeGroupsMapping |  Major | common | Lukas Majercak | Lukas Majercak |
| [HDFS-14144](https://issues.apache.org/jira/browse/HDFS-14144) | TestPred fails in Trunk |  Major | . | Ayush Saxena | Ayush Saxena |
| [MAPREDUCE-7170](https://issues.apache.org/jira/browse/MAPREDUCE-7170) | Doc typo in PluggableShuffleAndPluggableSort.md |  Minor | documentation | Zhaohui Xin | Zhaohui Xin |
| [HDFS-14145](https://issues.apache.org/jira/browse/HDFS-14145) | TestBlockStorageMovementAttemptedItems#testNoBlockMovementAttemptFinishedReportAdded fails sporadically in Trunk |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-14088](https://issues.apache.org/jira/browse/HDFS-14088) | RequestHedgingProxyProvider can throw NullPointerException when failover due to no lock on currentUsedProxy |  Major | hdfs-client | Yuxuan Wang | Yuxuan Wang |
| [YARN-9040](https://issues.apache.org/jira/browse/YARN-9040) | LevelDBCacheTimelineStore in ATS 1.5 leaks native memory |  Major | timelineserver | Tarun Parimi | Tarun Parimi |
| [YARN-9084](https://issues.apache.org/jira/browse/YARN-9084) | Service Upgrade: With default readiness check, the status of upgrade is reported to be successful prematurely |  Major | . | Chandni Singh | Chandni Singh |
| [HDFS-13661](https://issues.apache.org/jira/browse/HDFS-13661) | Ls command with e option fails when the filesystem is not HDFS |  Major | erasure-coding, tools | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-9154](https://issues.apache.org/jira/browse/YARN-9154) | Fix itemization in YARN service quickstart document |  Minor | documentation | Akira Ajisaka | Ayush Saxena |
| [HDFS-14166](https://issues.apache.org/jira/browse/HDFS-14166) | Ls with -e option not giving the result in proper format |  Major | . | Soumyapn | Shubham Dewan |
| [HDFS-14165](https://issues.apache.org/jira/browse/HDFS-14165) | In NameNode UI under DataNode tab ,the Capacity column is Non-Aligned |  Minor | . | Shubham Dewan | Shubham Dewan |
| [HDFS-14046](https://issues.apache.org/jira/browse/HDFS-14046) | In-Maintenance ICON is missing in datanode info page |  Major | datanode | Harshakiran Reddy | Ranith Sardar |
| [HDFS-14183](https://issues.apache.org/jira/browse/HDFS-14183) | [SPS] Remove the -w parameter from the -satisfystoragepolicy usage |  Major | . | Ayush Saxena | Ayush Saxena |
| [MAPREDUCE-7174](https://issues.apache.org/jira/browse/MAPREDUCE-7174) | MapReduce example wordmedian should handle generic options |  Major | . | Fei Hui | Fei Hui |
| [YARN-9164](https://issues.apache.org/jira/browse/YARN-9164) | Shutdown NM may cause NPE when opportunistic container scheduling is enabled |  Critical | . | lujie | lujie |
| [YARN-8567](https://issues.apache.org/jira/browse/YARN-8567) | Fetching yarn logs fails for long running application if it is not present in timeline store |  Major | log-aggregation | Tarun Parimi | Tarun Parimi |
| [HADOOP-15997](https://issues.apache.org/jira/browse/HADOOP-15997) | KMS client uses wrong UGI after HADOOP-14445 |  Blocker | kms | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-16028](https://issues.apache.org/jira/browse/HADOOP-16028) | Fix NetworkTopology chooseRandom function to support excluded nodes |  Major | . | Sihai Ke | Sihai Ke |
| [HADOOP-16030](https://issues.apache.org/jira/browse/HADOOP-16030) | AliyunOSS: bring fixes back from HADOOP-15671 |  Blocker | fs/oss | wujinhu | wujinhu |
| [YARN-9162](https://issues.apache.org/jira/browse/YARN-9162) | Fix TestRMAdminCLI#testHelp |  Major | resourcemanager, test | Ayush Saxena | Ayush Saxena |
| [HADOOP-16031](https://issues.apache.org/jira/browse/HADOOP-16031) | TestSecureLogins#testValidKerberosName fails |  Major | security | Akira Ajisaka | Akira Ajisaka |
| [YARN-9173](https://issues.apache.org/jira/browse/YARN-9173) | FairShare calculation broken for large values after YARN-8833 |  Major | fairscheduler | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [HDFS-14189](https://issues.apache.org/jira/browse/HDFS-14189) | Fix intermittent failure of TestNameNodeMetrics |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-14132](https://issues.apache.org/jira/browse/HDFS-14132) | Add BlockLocation.isStriped() to determine if block is replicated or Striped |  Major | hdfs | Shweta | Shweta |
| [YARN-8833](https://issues.apache.org/jira/browse/YARN-8833) | Avoid potential integer overflow when computing fair shares |  Major | fairscheduler | liyakun | liyakun |
| [HADOOP-16016](https://issues.apache.org/jira/browse/HADOOP-16016) | TestSSLFactory#testServerWeakCiphers sporadically fails in precommit builds |  Major | security, test | Jason Darrell Lowe | Akira Ajisaka |
| [HDFS-14198](https://issues.apache.org/jira/browse/HDFS-14198) | Upload and Create button doesn't get enabled after getting reset. |  Major | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-16013](https://issues.apache.org/jira/browse/HADOOP-16013) | DecayRpcScheduler decay thread should run as a daemon |  Major | ipc | Erik Krogen | Erik Krogen |
| [HADOOP-16043](https://issues.apache.org/jira/browse/HADOOP-16043) | NPE in ITestDynamoDBMetadataStore when fs.s3a.s3guard.ddb.table is not set |  Major | fs/s3 | Adam Antal | Gabor Bota |
| [YARN-9179](https://issues.apache.org/jira/browse/YARN-9179) | Fix NPE in AbstractYarnScheduler#updateNewContainerInfo |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [YARN-8747](https://issues.apache.org/jira/browse/YARN-8747) | [UI2] YARN UI2 page loading failed due to js error under some time zone configuration |  Critical | webapp | collinma | collinma |
| [YARN-9203](https://issues.apache.org/jira/browse/YARN-9203) | Fix typos in yarn-default.xml |  Trivial | documentation | Rahul Padmanabhan | Rahul Padmanabhan |
| [YARN-9194](https://issues.apache.org/jira/browse/YARN-9194) | Invalid event: REGISTERED and LAUNCH\_FAILED at FAILED, and NullPointerException happens in RM while shutdown a NM |  Critical | . | lujie | lujie |
| [HDFS-14175](https://issues.apache.org/jira/browse/HDFS-14175) | EC: Native XOR decoder should reset the output buffer before using it. |  Major | ec, hdfs | Surendra Singh Lilhore | Ayush Saxena |
| [YARN-9197](https://issues.apache.org/jira/browse/YARN-9197) | NPE in service AM when failed to launch container |  Major | yarn-native-services | kyungwan nam | kyungwan nam |
| [YARN-9204](https://issues.apache.org/jira/browse/YARN-9204) |  RM fails to start if absolute resource is specified for partition capacity in CS queues |  Blocker | yarn | Jiandan Yang | Jiandan Yang |
| [HDFS-14207](https://issues.apache.org/jira/browse/HDFS-14207) | ZKFC should catch exception when ha configuration missing |  Major | hdfs | Fei Hui | Fei Hui |
| [HADOOP-15922](https://issues.apache.org/jira/browse/HADOOP-15922) | DelegationTokenAuthenticationFilter get wrong doAsUser since it does not decode URL |  Major | common, kms | Xiaoqiao He | Xiaoqiao He |
| [HDFS-14218](https://issues.apache.org/jira/browse/HDFS-14218) | EC: Ls -e throw NPE when directory ec policy is disabled |  Major | . | Surendra Singh Lilhore | Ayush Saxena |
| [YARN-9210](https://issues.apache.org/jira/browse/YARN-9210) | RM nodes web page can not display node info |  Blocker | yarn | Jiandan Yang | Jiandan Yang |
| [YARN-9205](https://issues.apache.org/jira/browse/YARN-9205) | When using custom resource type, application will fail to run due to the CapacityScheduler throws InvalidResourceRequestException(GREATER\_THEN\_MAX\_ALLOCATION) |  Critical | . | Zhankun Tang | Zhankun Tang |
| [YARN-8961](https://issues.apache.org/jira/browse/YARN-8961) | [UI2] Flow Run End Time shows 'Invalid date' |  Major | . | Charan Hebri | Akhil PB |
| [HADOOP-16065](https://issues.apache.org/jira/browse/HADOOP-16065) | -Ddynamodb should be -Ddynamo in AWS SDK testing document |  Minor | documentation | Akira Ajisaka | Akira Ajisaka |
| [HDFS-14228](https://issues.apache.org/jira/browse/HDFS-14228) | Incorrect getSnapshottableDirListing() javadoc |  Major | snapshots | Wei-Chiu Chuang | Dinesh Chitlangia |
| [YARN-9222](https://issues.apache.org/jira/browse/YARN-9222) | Print launchTime in ApplicationSummary |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-8901](https://issues.apache.org/jira/browse/YARN-8901) | Restart "NEVER" policy does not work with component dependency |  Critical | . | Yesha Vora | Suma Shivaprasad |
| [YARN-9237](https://issues.apache.org/jira/browse/YARN-9237) | NM should ignore sending finished apps to RM during RM fail-over |  Major | yarn | Jiandan Yang | Jiandan Yang |
| [YARN-6616](https://issues.apache.org/jira/browse/YARN-6616) | YARN AHS shows submitTime for jobs same as startTime |  Minor | . | Prabhu Joseph | Prabhu Joseph |
| [HDFS-14236](https://issues.apache.org/jira/browse/HDFS-14236) | Lazy persist copy/ put fails with ViewFs |  Major | fs | Hanisha Koneru | Hanisha Koneru |
| [YARN-9251](https://issues.apache.org/jira/browse/YARN-9251) | Build failure for -Dhbase.profile=2.0 |  Blocker | . | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-9099](https://issues.apache.org/jira/browse/YARN-9099) | GpuResourceAllocator#getReleasingGpus calculates number of GPUs in a wrong way |  Major | . | Szilard Nemeth | Szilard Nemeth |
| [HADOOP-16084](https://issues.apache.org/jira/browse/HADOOP-16084) | Fix the comment for getClass in Configuration |  Trivial | . | Fengnan Li | Fengnan Li |
| [YARN-9262](https://issues.apache.org/jira/browse/YARN-9262) | TestRMAppAttemptTransitions is failing with an NPE |  Critical | resourcemanager | Sunil G | lujie |
| [YARN-9231](https://issues.apache.org/jira/browse/YARN-9231) | TestDistributedShell fix timeout |  Major | distributed-shell | Prabhu Joseph | Prabhu Joseph |
| [HDFS-14232](https://issues.apache.org/jira/browse/HDFS-14232) | libhdfs is not included in binary tarball |  Critical | build, libhdfs | Akira Ajisaka | Akira Ajisaka |
| [HDFS-14158](https://issues.apache.org/jira/browse/HDFS-14158) | Checkpointer ignores configured time period \> 5 minutes |  Minor | namenode | Timo Walter | Timo Walter |
| [MAPREDUCE-7177](https://issues.apache.org/jira/browse/MAPREDUCE-7177) | Disable speculative execution in TestDFSIO |  Major | . | Kihwal Lee | Zhaohui Xin |
| [HADOOP-16076](https://issues.apache.org/jira/browse/HADOOP-16076) | SPNEGO+SSL Client Connections with HttpClient Broken |  Major | build, security | Larry McCay | Larry McCay |
| [HDFS-14202](https://issues.apache.org/jira/browse/HDFS-14202) | "dfs.disk.balancer.max.disk.throughputInMBperSec" property is not working as per set value. |  Major | diskbalancer | Ranith Sardar | Ranith Sardar |
| [YARN-9149](https://issues.apache.org/jira/browse/YARN-9149) | yarn container -status misses logUrl when integrated with ATSv2 |  Major | . | Rohith Sharma K S | Abhishek Modi |
| [YARN-9246](https://issues.apache.org/jira/browse/YARN-9246) | NPE when executing a command yarn node -status or -states without additional arguments |  Minor | client | Masahiro Tanaka | Masahiro Tanaka |
| [HDFS-14242](https://issues.apache.org/jira/browse/HDFS-14242) | OIV WebImageViewer: NPE when param op is not specified |  Major | tools | Siyao Meng | Siyao Meng |
| [YARN-7627](https://issues.apache.org/jira/browse/YARN-7627) | [ATSv2] When passing a non-number as metricslimit, the error message is wrong |  Trivial | api | Grant Sohn | Charan Hebri |
| [YARN-8498](https://issues.apache.org/jira/browse/YARN-8498) | Yarn NodeManager OOM Listener Fails Compilation on Ubuntu 18.04 |  Blocker | . | Jack Bearden | Ayush Saxena |
| [YARN-9206](https://issues.apache.org/jira/browse/YARN-9206) | RMServerUtils does not count SHUTDOWN as an accepted state |  Major | . | Kuhu Shukla | Kuhu Shukla |
| [HADOOP-16032](https://issues.apache.org/jira/browse/HADOOP-16032) | Distcp It should clear sub directory ACL before applying new ACL on it. |  Major | tools/distcp | Ranith Sardar | Ranith Sardar |
| [HDFS-14140](https://issues.apache.org/jira/browse/HDFS-14140) | JournalNodeSyncer authentication is failing in secure cluster |  Major | journal-node, security | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [YARN-9257](https://issues.apache.org/jira/browse/YARN-9257) | Distributed Shell client throws a NPE for a non-existent queue |  Major | distributed-shell | Charan Hebri | Charan Hebri |
| [YARN-8761](https://issues.apache.org/jira/browse/YARN-8761) | Service AM support for decommissioning component instances |  Major | . | Billie Rinaldi | Billie Rinaldi |
| [HADOOP-16098](https://issues.apache.org/jira/browse/HADOOP-16098) | Fix javadoc warnings in hadoop-aws |  Minor | fs/s3 | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-14266](https://issues.apache.org/jira/browse/HDFS-14266) | EC : Fsck -blockId shows null for EC Blocks if One Block Is Not Available. |  Major | . | Harshakiran Reddy | Ayush Saxena |
| [HADOOP-10007](https://issues.apache.org/jira/browse/HADOOP-10007) | distcp / mv is not working on ftp |  Major | fs | Fabian Zimmermann |  |
| [HDFS-14274](https://issues.apache.org/jira/browse/HDFS-14274) | EC: NPE While Listing EC Policy For A Directory Following Replication Policy. |  Major | erasure-coding | Souryakanta Dwivedy | Ayush Saxena |
| [HDFS-14263](https://issues.apache.org/jira/browse/HDFS-14263) | Remove unnecessary block file exists check from FsDatasetImpl#getBlockInputStream() |  Major | datanode | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [YARN-7761](https://issues.apache.org/jira/browse/YARN-7761) | [UI2] Clicking 'master container log' or 'Link' next to 'log' under application's appAttempt goes to Old UI's Log link |  Major | yarn-ui-v2 | Sumana Sathish | Akhil PB |
| [YARN-9295](https://issues.apache.org/jira/browse/YARN-9295) | [UI2] Fix label typo in Cluster Overview page |  Trivial | yarn-ui-v2 | Charan Hebri | Charan Hebri |
| [YARN-9308](https://issues.apache.org/jira/browse/YARN-9308) | fairscheduler-statedump.log gets generated regardless of service again after the merge of HDFS-7240 |  Blocker | fairscheduler, scheduler | Akira Ajisaka | Wilfred Spiegelenburg |
| [YARN-9284](https://issues.apache.org/jira/browse/YARN-9284) | Fix the unit of yarn.service.am-resource.memory in the document |  Minor | documentation, yarn-native-services | Masahiro Tanaka | Masahiro Tanaka |
| [YARN-9283](https://issues.apache.org/jira/browse/YARN-9283) | Javadoc of LinuxContainerExecutor#addSchedPriorityCommand has a wrong property name as reference |  Minor | documentation | Szilard Nemeth | Adam Antal |
| [HADOOP-16116](https://issues.apache.org/jira/browse/HADOOP-16116) | Fix Spelling Mistakes - DECOMISSIONED |  Trivial | . | David Mollitor | David Mollitor |
| [HDFS-14287](https://issues.apache.org/jira/browse/HDFS-14287) | DataXceiverServer May Double-Close PeerServer |  Minor | datanode | David Mollitor | David Mollitor |
| [YARN-9286](https://issues.apache.org/jira/browse/YARN-9286) | [Timeline Server] Sorting based on FinalStatus shows pop-up message |  Minor | timelineserver | Nallasivan | Bilwa S T |
| [HDFS-14081](https://issues.apache.org/jira/browse/HDFS-14081) | hdfs dfsadmin -metasave metasave\_test results NPE |  Major | hdfs | Shweta | Shweta |
| [HDFS-14273](https://issues.apache.org/jira/browse/HDFS-14273) | Fix checkstyle issues in BlockLocation's method javadoc |  Trivial | . | Shweta | Shweta |
| [HADOOP-15813](https://issues.apache.org/jira/browse/HADOOP-15813) | Enable more reliable SSL connection reuse |  Major | common | Daryn Sharp | Daryn Sharp |
| [YARN-9319](https://issues.apache.org/jira/browse/YARN-9319) | Fix compilation issue of  handling typedef an existing name by gcc compiler |  Blocker | . | Wei-Chiu Chuang | Zhankun Tang |
| [YARN-9238](https://issues.apache.org/jira/browse/YARN-9238) | Avoid allocating opportunistic containers to previous/removed/non-exist application attempt |  Critical | . | lujie | lujie |
| [YARN-9118](https://issues.apache.org/jira/browse/YARN-9118) | Handle exceptions with parsing user defined GPU devices in GpuDiscoverer |  Major | . | Szilard Nemeth | Szilard Nemeth |
| [HDFS-14279](https://issues.apache.org/jira/browse/HDFS-14279) | [SBN Read] Race condition in ObserverReadProxyProvider |  Major | hdfs, namenode | Erik Krogen | Erik Krogen |
| [HADOOP-16129](https://issues.apache.org/jira/browse/HADOOP-16129) | Misc. bug fixes for KMS Benchmark |  Major | kms | Wei-Chiu Chuang | George Huang |
| [HDFS-14285](https://issues.apache.org/jira/browse/HDFS-14285) | libhdfs hdfsRead copies entire array even if its only partially filled |  Major | hdfs-client, libhdfs, native | Sahil Takiar | Sahil Takiar |
| [YARN-9317](https://issues.apache.org/jira/browse/YARN-9317) | Avoid repeated YarnConfiguration#timelineServiceV2Enabled check |  Major | . | Bibin Chundatt | Prabhu Joseph |
| [YARN-9300](https://issues.apache.org/jira/browse/YARN-9300) | Lazy preemption should trigger an update on queue preemption metrics for CapacityScheduler |  Major | capacityscheduler | Tao Yang | Tao Yang |
| [YARN-9213](https://issues.apache.org/jira/browse/YARN-9213) | RM Web UI v1 does not show custom resource allocations for containers page |  Major | . | Szilard Nemeth | Szilard Nemeth |
| [YARN-9329](https://issues.apache.org/jira/browse/YARN-9329) | updatePriority is blocked when using FairScheduler |  Major | . | Jiandan Yang | Jiandan Yang |
| [HDFS-14299](https://issues.apache.org/jira/browse/HDFS-14299) | ViewFs: Correct error message for read only operations |  Minor | federation | hu xiaodong | hu xiaodong |
| [HADOOP-16127](https://issues.apache.org/jira/browse/HADOOP-16127) | In ipc.Client, put a new connection could happen after stop |  Major | ipc | Tsz-wo Sze | Tsz-wo Sze |
| [YARN-8378](https://issues.apache.org/jira/browse/YARN-8378) | ApplicationHistoryManagerImpl#getApplications doesn't honor filters |  Minor | resourcemanager, yarn | Lantao Jin | Lantao Jin |
| [YARN-9311](https://issues.apache.org/jira/browse/YARN-9311) | TestRMRestart hangs due to a deadlock |  Major | . | Prabhu Joseph | Prabhu Joseph |
| [YARN-9248](https://issues.apache.org/jira/browse/YARN-9248) | RMContainerImpl:Invalid event: ACQUIRED at KILLED |  Major | . | lujie | lujie |
| [YARN-9318](https://issues.apache.org/jira/browse/YARN-9318) | Resources#multiplyAndRoundUp does not consider Resource Types |  Major | . | Szilard Nemeth | Szilard Nemeth |
| [HADOOP-16107](https://issues.apache.org/jira/browse/HADOOP-16107) | FilterFileSystem doesn't wrap all create() or new builder calls; may skip CRC logic |  Blocker | fs | Steve Loughran | Steve Loughran |
| [HADOOP-16149](https://issues.apache.org/jira/browse/HADOOP-16149) | hadoop-mapreduce-client-app build not converging due to transient dependencies |  Major | build | Steve Loughran | Steve Loughran |
| [YARN-9334](https://issues.apache.org/jira/browse/YARN-9334) | YARN Service Client does not work with SPNEGO when knox is configured |  Major | yarn-native-services | Tarun Parimi | Billie Rinaldi |
| [YARN-9323](https://issues.apache.org/jira/browse/YARN-9323) | FSLeafQueue#computeMaxAMResource does not override zero values for custom resources |  Major | . | Szilard Nemeth | Szilard Nemeth |
| [HADOOP-16067](https://issues.apache.org/jira/browse/HADOOP-16067) | Incorrect Format Debug Statement KMSACLs |  Trivial | kms | David Mollitor | Charan Hebri |
| [HDFS-14324](https://issues.apache.org/jira/browse/HDFS-14324) | Fix TestDataNodeVolumeFailure |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-13997](https://issues.apache.org/jira/browse/HDFS-13997) | Secondary NN Web UI displays nothing, and the console log shows moment is not defined. |  Major | namenode | Rui Chen | Ayush Saxena |
| [HDFS-14272](https://issues.apache.org/jira/browse/HDFS-14272) | [SBN read] ObserverReadProxyProvider should sync with active txnID on startup |  Major | tools | Wei-Chiu Chuang | Erik Krogen |
| [HDFS-14314](https://issues.apache.org/jira/browse/HDFS-14314) | fullBlockReportLeaseId should be reset after registering to NN |  Critical | datanode | star | star |
| [YARN-7266](https://issues.apache.org/jira/browse/YARN-7266) | Timeline Server event handler threads locked |  Major | ATSv2, timelineserver | Venkata Puneet Ravuri | Prabhu Joseph |
| [HADOOP-16150](https://issues.apache.org/jira/browse/HADOOP-16150) | checksumFS doesn't wrap concat(): concatenated files don't have checksums |  Major | fs | Steve Loughran | Steve Loughran |
| [YARN-8803](https://issues.apache.org/jira/browse/YARN-8803) | [UI2] Show flow runs in the order of recently created time in graph widgets |  Major | yarn-ui-v2 | Akhil PB | Akhil PB |
| [HDFS-14111](https://issues.apache.org/jira/browse/HDFS-14111) | hdfsOpenFile on HDFS causes unnecessary IO from file offset 0 |  Major | hdfs-client, libhdfs | Todd Lipcon | Sahil Takiar |
| [YARN-9341](https://issues.apache.org/jira/browse/YARN-9341) | Reentrant lock() before try |  Minor | yarn | Prabhu Joseph | Prabhu Joseph |
| [HDFS-14317](https://issues.apache.org/jira/browse/HDFS-14317) | Standby does not trigger edit log rolling when in-progress edit log tailing is enabled |  Critical | . | Ekanth Sethuramalingam | Ekanth Sethuramalingam |
| [HADOOP-16174](https://issues.apache.org/jira/browse/HADOOP-16174) | Disable wildfly logs to the console |  Major | fs/azure | Denes Gerencser | Denes Gerencser |
| [HDFS-14347](https://issues.apache.org/jira/browse/HDFS-14347) | Restore a comment line mistakenly removed in ProtobufRpcEngine |  Major | . | Konstantin Shvachko | Fengnan Li |
| [HDFS-14333](https://issues.apache.org/jira/browse/HDFS-14333) | Datanode fails to start if any disk has errors during Namenode registration |  Major | datanode | Stephen O'Donnell | Stephen O'Donnell |
| [HADOOP-16192](https://issues.apache.org/jira/browse/HADOOP-16192) | CallQueue backoff bug fixes: doesn't perform backoff when add() is used, and doesn't update backoff when refreshed |  Major | ipc | Erik Krogen | Erik Krogen |
| [YARN-9365](https://issues.apache.org/jira/browse/YARN-9365) | fix wrong command in TimelineServiceV2.md |  Major | timelineserver | Runlin Zhang | Runlin Zhang |
| [YARN-9357](https://issues.apache.org/jira/browse/YARN-9357) | Modify HBase Liveness monitor log to debug |  Minor | ATSv2 | Prabhu Joseph | Prabhu Joseph |
| [HDFS-14359](https://issues.apache.org/jira/browse/HDFS-14359) | Inherited ACL permissions masked when parent directory does not exist (mkdir -p) |  Major | . | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-14037](https://issues.apache.org/jira/browse/HDFS-14037) | Fix SSLFactory truststore reloader thread leak in URLConnectionFactory |  Major | hdfs-client, webhdfs | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-14348](https://issues.apache.org/jira/browse/HDFS-14348) | Fix JNI exception handling issues in libhdfs |  Major | hdfs-client, libhdfs, native | Sahil Takiar | Sahil Takiar |
| [YARN-9411](https://issues.apache.org/jira/browse/YARN-9411) | TestYarnNativeServices fails sporadically with bind address in use |  Major | test, yarn-native-services | Prabhu Joseph | Prabhu Joseph |
| [HADOOP-15652](https://issues.apache.org/jira/browse/HADOOP-15652) | Fix typos SPENGO into SPNEGO |  Trivial | documentation | okumin | okumin |
| [HADOOP-16199](https://issues.apache.org/jira/browse/HADOOP-16199) | KMSLoadBlanceClientProvider does not select token correctly |  Major | . | Xiaoyu Yao | Xiaoyu Yao |
| [YARN-9227](https://issues.apache.org/jira/browse/YARN-9227) | DistributedShell RelativePath is not removed at end |  Minor | distributed-shell | Prabhu Joseph | Prabhu Joseph |
| [YARN-9431](https://issues.apache.org/jira/browse/YARN-9431) | Fix flaky junit test fair.TestAppRunnability after YARN-8967 |  Minor | fairscheduler, test | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [HADOOP-16225](https://issues.apache.org/jira/browse/HADOOP-16225) | Fix links to the developer mailing lists in DownstreamDev.md |  Minor | documentation | Akira Ajisaka | Wanqiang Ji |
| [HADOOP-16226](https://issues.apache.org/jira/browse/HADOOP-16226) | new Path(String str) does not remove all the trailing slashes of str |  Minor | fs | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-16232](https://issues.apache.org/jira/browse/HADOOP-16232) | Fix errors in the checkstyle configration xmls |  Major | build | Akira Ajisaka | Wanqiang Ji |
| [YARN-4901](https://issues.apache.org/jira/browse/YARN-4901) | QueueMetrics needs to be cleared before MockRM is initialized |  Major | scheduler | Daniel Templeton | Peter Bacsko |
| [HADOOP-16011](https://issues.apache.org/jira/browse/HADOOP-16011) | OsSecureRandom very slow compared to other SecureRandom implementations |  Major | security | Todd Lipcon | Siyao Meng |
| [HDFS-14389](https://issues.apache.org/jira/browse/HDFS-14389) | getAclStatus returns incorrect permissions and owner when an iNodeAttributeProvider is configured |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [YARN-9396](https://issues.apache.org/jira/browse/YARN-9396) | YARN\_RM\_CONTAINER\_CREATED published twice to ATS |  Major | ATSv2 | Prabhu Joseph | Prabhu Joseph |
| [HDFS-14407](https://issues.apache.org/jira/browse/HDFS-14407) | Fix misuse of SLF4j logging API in DatasetVolumeChecker#checkAllVolumes |  Minor | . | Wanqiang Ji | Wanqiang Ji |
| [HDFS-14416](https://issues.apache.org/jira/browse/HDFS-14416) | Fix TestHdfsConfigFields for field dfs.client.failover.resolver.useFQDN |  Major | . | Íñigo Goiri | Fengnan Li |
| [YARN-9413](https://issues.apache.org/jira/browse/YARN-9413) | Queue resource leak after app fail for CapacityScheduler |  Major | capacityscheduler | Tao Yang | Tao Yang |
| [HADOOP-14635](https://issues.apache.org/jira/browse/HADOOP-14635) | Javadoc correction for AccessControlList#buildACL |  Minor | documentation | Bibin Chundatt | Yeliang Cang |
| [HADOOP-12890](https://issues.apache.org/jira/browse/HADOOP-12890) | Fix typo in AbstractService |  Trivial | documentation | Mike Drob | Gabor Liptak |
| [HADOOP-16240](https://issues.apache.org/jira/browse/HADOOP-16240) | start-build-env.sh can consume all disk space during image creation |  Minor | build | Craig Condit | Craig Condit |
| [HDFS-12245](https://issues.apache.org/jira/browse/HDFS-12245) | Fix INodeId javadoc |  Major | documentation, namenode | Wei-Chiu Chuang | Adam Antal |
| [HDFS-14420](https://issues.apache.org/jira/browse/HDFS-14420) | Fix typo in KeyShell console |  Minor | kms | hu xiaodong | hu xiaodong |
| [HADOOP-14544](https://issues.apache.org/jira/browse/HADOOP-14544) | DistCp documentation for command line options is misaligned. |  Minor | documentation | Chris Nauroth | Masatake Iwasaki |
| [YARN-9481](https://issues.apache.org/jira/browse/YARN-9481) | [JDK 11] Build fails due to hard-coded target version in hadoop-yarn-applications-catalog-webapp |  Major | build | Kei Kori | Kei Kori |
| [YARN-9379](https://issues.apache.org/jira/browse/YARN-9379) | Can't specify docker runtime through environment |  Minor | nodemanager | caozhiqiang | caozhiqiang |
| [YARN-9336](https://issues.apache.org/jira/browse/YARN-9336) | JobHistoryServer leaks CLOSE\_WAIT tcp connections when using LogAggregationIndexedFileController |  Major | log-aggregation | Tarun Parimi | Tarun Parimi |
| [HDFS-10477](https://issues.apache.org/jira/browse/HDFS-10477) | Stop decommission a rack of DataNodes caused NameNode fail over to standby |  Major | namenode | yunjiong zhao | yunjiong zhao |
| [HADOOP-15881](https://issues.apache.org/jira/browse/HADOOP-15881) | Remove JUnit from LICENSE.txt |  Major | . | Akira Ajisaka | Kei Kori |
| [YARN-9487](https://issues.apache.org/jira/browse/YARN-9487) | NodeManager native build shouldn't link against librt on macOS |  Major | nodemanager | Siyao Meng | Siyao Meng |
| [YARN-6695](https://issues.apache.org/jira/browse/YARN-6695) | Race condition in RM for publishing container events vs appFinished events causes NPE |  Critical | . | Rohith Sharma K S | Prabhu Joseph |
| [YARN-8622](https://issues.apache.org/jira/browse/YARN-8622) | NodeManager native build fails due to getgrouplist not found on macOS |  Major | nodemanager | Ewan Higgs | Siyao Meng |
| [YARN-9495](https://issues.apache.org/jira/browse/YARN-9495) | Fix findbugs warnings in hadoop-yarn-server-resourcemanager module |  Minor | . | Tao Yang | Tao Yang |
| [HADOOP-16265](https://issues.apache.org/jira/browse/HADOOP-16265) | Configuration#getTimeDuration is not consistent between default value and manual settings. |  Major | . | star | star |
| [HDFS-14445](https://issues.apache.org/jira/browse/HDFS-14445) | TestTrySendErrorReportWhenNNThrowsIOException fails in trunk |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-14435](https://issues.apache.org/jira/browse/HDFS-14435) | ObserverReadProxyProvider is unable to properly fetch HAState from Standby NNs |  Major | ha, nn | Erik Krogen | Erik Krogen |
| [YARN-9339](https://issues.apache.org/jira/browse/YARN-9339) | Apps pending metric incorrect after moving app to a new queue |  Minor | . | Billie Rinaldi | Abhishek Modi |
| [YARN-9491](https://issues.apache.org/jira/browse/YARN-9491) | TestApplicationMasterServiceFair\>ApplicationMasterServiceTestBase.testUpdateTrackingUrl fails intermittent |  Minor | . | Prabhu Joseph | Prabhu Joseph |
| [YARN-9501](https://issues.apache.org/jira/browse/YARN-9501) | TestCapacitySchedulerOvercommit#testReducePreemptAndCancel fails intermittent |  Minor | capacityscheduler, test | Prabhu Joseph | Prabhu Joseph |
| [YARN-9424](https://issues.apache.org/jira/browse/YARN-9424) | Change getDeclaredMethods to getMethods in FederationClientInterceptor#invokeConcurrent() |  Major | federation | Shen Yinjie | Shen Yinjie |
| [MAPREDUCE-7200](https://issues.apache.org/jira/browse/MAPREDUCE-7200) | Remove stale eclipse templates |  Minor | . | Akira Ajisaka | Wanqiang Ji |
| [HDFS-13677](https://issues.apache.org/jira/browse/HDFS-13677) | Dynamic refresh Disk configuration results in overwriting VolumeMap |  Blocker | . | xuzq | xuzq |
| [YARN-6929](https://issues.apache.org/jira/browse/YARN-6929) | yarn.nodemanager.remote-app-log-dir structure is not scalable |  Major | log-aggregation | Prabhu Joseph | Prabhu Joseph |
| [YARN-9285](https://issues.apache.org/jira/browse/YARN-9285) | RM UI progress column is of wrong type |  Minor | yarn | Ahmed Hussein | Ahmed Hussein |
| [YARN-9528](https://issues.apache.org/jira/browse/YARN-9528) | Federation RMs starting up at the same time can give duplicate application IDs |  Minor | . | Young Chen | Young Chen |
| [HDFS-14438](https://issues.apache.org/jira/browse/HDFS-14438) | Fix typo in OfflineEditsVisitorFactory |  Major | hdfs | bianqi | bianqi |
| [HDFS-14372](https://issues.apache.org/jira/browse/HDFS-14372) | NPE while DN is shutting down |  Major | . | lujie | lujie |
| [YARN-9524](https://issues.apache.org/jira/browse/YARN-9524) | TestAHSWebServices and TestLogsCLI test case failures |  Major | log-aggregation, test | Prabhu Joseph | Prabhu Joseph |
| [YARN-9432](https://issues.apache.org/jira/browse/YARN-9432) | Reserved containers leak after its request has been cancelled or satisfied when multi-nodes enabled |  Major | capacityscheduler | Tao Yang | Tao Yang |
| [YARN-9513](https://issues.apache.org/jira/browse/YARN-9513) | [JDK11] Fix TestMetricsInvariantChecker#testManyRuns in case of JDK greater than 8 |  Major | test | Siyao Meng | Adam Antal |
| [HADOOP-16293](https://issues.apache.org/jira/browse/HADOOP-16293) | AuthenticationFilterInitializer doc has speudo instead of pseudo |  Trivial | auth, documentation | Prabhu Joseph | Prabhu Joseph |
| [YARN-9535](https://issues.apache.org/jira/browse/YARN-9535) | Typos in Docker documentation |  Major | . | Szilard Nemeth | Charan Hebri |
| [HADOOP-16299](https://issues.apache.org/jira/browse/HADOOP-16299) | [JDK 11] Build fails without specifying -Djavac.version=11 |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [YARN-9483](https://issues.apache.org/jira/browse/YARN-9483) | DistributedShell does not release container when failed to localize at launch |  Major | . | Prabhu Joseph | Prabhu Joseph |
| [HADOOP-16278](https://issues.apache.org/jira/browse/HADOOP-16278) | With S3A Filesystem, Long Running services End up Doing lot of GC and eventually die |  Major | common, fs/s3, metrics | Rajat Khandelwal | Rajat Khandelwal |
| [YARN-9522](https://issues.apache.org/jira/browse/YARN-9522) | AppBlock ignores full qualified class name of PseudoAuthenticationHandler |  Minor | . | Prabhu Joseph | Prabhu Joseph |
| [YARN-9504](https://issues.apache.org/jira/browse/YARN-9504) | [UI2] Fair scheduler queue view page does not show actual capacity |  Major | fairscheduler, yarn-ui-v2 | Zoltan Siegl | Zoltan Siegl |
| [YARN-9493](https://issues.apache.org/jira/browse/YARN-9493) | Scheduler Page does not display the right page by query string |  Major | capacity scheduler, resourcemanager, webapp | Wanqiang Ji | Wanqiang Ji |
| [HADOOP-16161](https://issues.apache.org/jira/browse/HADOOP-16161) | NetworkTopology#getWeightUsingNetworkLocation return unexpected result |  Major | net | Xiaoqiao He | Xiaoqiao He |
| [YARN-9519](https://issues.apache.org/jira/browse/YARN-9519) | TFile log aggregation file format is not working for yarn.log-aggregation.TFile.remote-app-log-dir config |  Major | log-aggregation | Adam Antal | Adam Antal |
| [HDFS-14482](https://issues.apache.org/jira/browse/HDFS-14482) | Crash when using libhdfs with bad classpath |  Major | . | Todd Lipcon | Sahil Takiar |
| [YARN-9508](https://issues.apache.org/jira/browse/YARN-9508) | YarnConfiguration areNodeLabel enabled is costly in allocation flow |  Critical | . | Bibin Chundatt | Bilwa S T |
| [HADOOP-16247](https://issues.apache.org/jira/browse/HADOOP-16247) | NPE in FsUrlConnection |  Major | hdfs-client | Karthik Palanisamy | Karthik Palanisamy |
| [YARN-9554](https://issues.apache.org/jira/browse/YARN-9554) | TimelineEntity DAO has java.util.Set interface which JAXB can't handle |  Major | timelineservice | Prabhu Joseph | Prabhu Joseph |
| [HDFS-14323](https://issues.apache.org/jira/browse/HDFS-14323) | Distcp fails in Hadoop 3.x when 2.x source webhdfs url has special characters in hdfs file path |  Major | webhdfs | Srinivasu Majeti | Srinivasu Majeti |
| [YARN-9575](https://issues.apache.org/jira/browse/YARN-9575) | Fix TestYarnConfigurationFields testcase failing |  Major | test, yarn-native-services | Prabhu Joseph | Prabhu Joseph |
| [MAPREDUCE-7205](https://issues.apache.org/jira/browse/MAPREDUCE-7205) | Treat container scheduler kill exit code as a task attempt killing event |  Major | applicationmaster, mr-am, mrv2 | Wanqiang Ji | Wanqiang Ji |
| [MAPREDUCE-7198](https://issues.apache.org/jira/browse/MAPREDUCE-7198) | mapreduce.task.timeout=0 configuration used to disable timeout doesn't work |  Minor | . | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-12948](https://issues.apache.org/jira/browse/HADOOP-12948) | Remove the defunct startKdc profile from hadoop-common |  Major | test | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-9080](https://issues.apache.org/jira/browse/YARN-9080) | Bucket Directories as part of ATS done accumulates |  Major | . | Prabhu Joseph | Prabhu Joseph |
| [YARN-9482](https://issues.apache.org/jira/browse/YARN-9482) | DistributedShell job with localization fails in unsecure cluster |  Major | distributed-shell | Prabhu Joseph | Prabhu Joseph |
| [YARN-9558](https://issues.apache.org/jira/browse/YARN-9558) | Log Aggregation testcases failing |  Major | log-aggregation, test | Prabhu Joseph | Prabhu Joseph |
| [HDFS-14500](https://issues.apache.org/jira/browse/HDFS-14500) | NameNode StartupProgress continues to report edit log segments after the LOADING\_EDITS phase is finished |  Major | namenode | Erik Krogen | Erik Krogen |
| [YARN-9503](https://issues.apache.org/jira/browse/YARN-9503) | Fix JavaDoc error in TestSchedulerOvercommit |  Minor | documentation, test | Wanqiang Ji | Wanqiang Ji |
| [YARN-9500](https://issues.apache.org/jira/browse/YARN-9500) | Fix typos in ResourceModel.md |  Trivial | documentation | leiqiang | leiqiang |
| [HDFS-14434](https://issues.apache.org/jira/browse/HDFS-14434) | webhdfs that connect secure hdfs should not use user.name parameter |  Minor | webhdfs | KWON BYUNGCHANG | KWON BYUNGCHANG |
| [HADOOP-16331](https://issues.apache.org/jira/browse/HADOOP-16331) | Fix ASF License check in pom.xml |  Major | . | Wanqiang Ji | Akira Ajisaka |
| [HDFS-14512](https://issues.apache.org/jira/browse/HDFS-14512) | ONE\_SSD policy will be violated while write data with DistributedFileSystem.create(....favoredNodes) |  Major | . | Shen Yinjie | Ayush Saxena |
| [HADOOP-16334](https://issues.apache.org/jira/browse/HADOOP-16334) | Fix yetus-wrapper not working when HADOOP\_YETUS\_VERSION \>= 0.9.0 |  Major | yetus | Wanqiang Ji | Wanqiang Ji |
| [YARN-9553](https://issues.apache.org/jira/browse/YARN-9553) | Fix NPE in EntityGroupFSTimelineStore#getEntityTimelines |  Major | timelineservice | Prabhu Joseph | Prabhu Joseph |
| [HDFS-14521](https://issues.apache.org/jira/browse/HDFS-14521) | Suppress setReplication logging. |  Major | . | Kihwal Lee | Kihwal Lee |
| [YARN-9033](https://issues.apache.org/jira/browse/YARN-9033) | ResourceHandlerChain#bootstrap is invoked twice during NM start if LinuxContainerExecutor enabled |  Major | yarn | Zhankun Tang | Zhankun Tang |
| [YARN-9027](https://issues.apache.org/jira/browse/YARN-9027) | EntityGroupFSTimelineStore fails to init LevelDBCacheTimelineStore |  Major | timelineserver | Prabhu Joseph | Prabhu Joseph |
| [YARN-9507](https://issues.apache.org/jira/browse/YARN-9507) | Fix NPE in NodeManager#serviceStop on startup failure |  Minor | . | Bilwa S T | Bilwa S T |
| [YARN-8947](https://issues.apache.org/jira/browse/YARN-8947) | [UI2] Active User info missing from UI2 |  Major | yarn-ui-v2 | Akhil PB | Akhil PB |
| [YARN-8906](https://issues.apache.org/jira/browse/YARN-8906) | [UI2] NM hostnames not displayed correctly in Node Heatmap Chart |  Major | . | Charan Hebri | Akhil PB |
| [YARN-9580](https://issues.apache.org/jira/browse/YARN-9580) | Fulfilled reservation information in assignment is lost when transferring in ParentQueue#assignContainers |  Major | capacityscheduler | Tao Yang | Tao Yang |
| [YARN-9595](https://issues.apache.org/jira/browse/YARN-9595) | FPGA plugin: NullPointerException in FpgaNodeResourceUpdateHandler.updateConfiguredResource() |  Major | nodemanager | Peter Bacsko | Peter Bacsko |
| [YARN-8625](https://issues.apache.org/jira/browse/YARN-8625) | Aggregate Resource Allocation for each job is not present in ATS |  Major | ATSv2 | Prabhu Joseph | Prabhu Joseph |
| [YARN-9600](https://issues.apache.org/jira/browse/YARN-9600) | Support self-adaption width for columns of containers table on app attempt page |  Minor | webapp | Tao Yang | Tao Yang |
| [HDFS-14527](https://issues.apache.org/jira/browse/HDFS-14527) | Stop all DataNodes may result in NN terminate |  Major | namenode | Xiaoqiao He | Xiaoqiao He |
| [HDFS-14486](https://issues.apache.org/jira/browse/HDFS-14486) | The exception classes in some throw statements do not accurately describe why they are thrown |  Minor | . | Haicheng Chen | Ayush Saxena |
| [HADOOP-16345](https://issues.apache.org/jira/browse/HADOOP-16345) | Potential NPE when instantiating FairCallQueue metrics |  Major | ipc | Erik Krogen | Erik Krogen |
| [HDFS-14494](https://issues.apache.org/jira/browse/HDFS-14494) | Move Server logging of StatedId inside receiveRequestState() |  Major | . | Konstantin Shvachko | Shweta |
| [YARN-9594](https://issues.apache.org/jira/browse/YARN-9594) | Fix missing break statement in ContainerScheduler#handle |  Major | . | lujie | lujie |
| [YARN-9565](https://issues.apache.org/jira/browse/YARN-9565) | RMAppImpl#ranNodes not cleared on FinalTransition |  Major | . | Bibin Chundatt | Bilwa S T |
| [YARN-9547](https://issues.apache.org/jira/browse/YARN-9547) | ContainerStatusPBImpl default execution type is not returned |  Major | . | Bibin Chundatt | Bilwa S T |
| [HDFS-13231](https://issues.apache.org/jira/browse/HDFS-13231) | Extend visualization for Decommissioning, Maintenance Mode under Datanode tab in the NameNode UI |  Major | datanode, namenode | Haibo Yan | Stephen O'Donnell |
| [HADOOP-15960](https://issues.apache.org/jira/browse/HADOOP-15960) | Update guava to 27.0-jre in hadoop-project |  Critical | common, security | Gabor Bota | Gabor Bota |
| [HDFS-14549](https://issues.apache.org/jira/browse/HDFS-14549) | EditLogTailer shouldn't output full stack trace when interrupted |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HADOOP-16373](https://issues.apache.org/jira/browse/HADOOP-16373) | Fix typo in FileSystemShell#test documentation |  Trivial | documentation | Dinesh Chitlangia | Dinesh Chitlangia |
| [HDFS-14556](https://issues.apache.org/jira/browse/HDFS-14556) | Spelling Mistake "gloablly" |  Trivial | hdfs-client | David Mollitor | David Mollitor |
| [HDFS-14535](https://issues.apache.org/jira/browse/HDFS-14535) | The default 8KB buffer in requestFileDescriptors#BufferedOutputStream is causing lots of heap allocation in HBase when using short-circut read |  Major | hdfs-client | Zheng Hu | Zheng Hu |
| [HDFS-13730](https://issues.apache.org/jira/browse/HDFS-13730) | BlockReaderRemote.sendReadResult throws NPE |  Major | hdfs-client | Wei-Chiu Chuang | Yuanbo Liu |
| [HDFS-12315](https://issues.apache.org/jira/browse/HDFS-12315) | Use Path instead of String in the TestHdfsAdmin.verifyOpenFiles() |  Trivial | . | Oleg Danilov | Oleg Danilov |
| [HDFS-12314](https://issues.apache.org/jira/browse/HDFS-12314) | Typo in the TestDataNodeHotSwapVolumes.testAddOneNewVolume() |  Trivial | . | Oleg Danilov | Oleg Danilov |
| [YARN-9584](https://issues.apache.org/jira/browse/YARN-9584) | Should put initializeProcessTrees method call before get pid |  Critical | nodemanager | Wanqiang Ji | Wanqiang Ji |
| [HDFS-14010](https://issues.apache.org/jira/browse/HDFS-14010) | Pass correct DF usage to ReservedSpaceCalculator builder |  Minor | . | Lukas Majercak | Lukas Majercak |
| [HDFS-14078](https://issues.apache.org/jira/browse/HDFS-14078) | Admin helper fails to prettify NullPointerExceptions |  Major | . | Marton Elek | Marton Elek |
| [HDFS-14101](https://issues.apache.org/jira/browse/HDFS-14101) | Random failure of testListCorruptFilesCorruptedBlock |  Major | test | Kihwal Lee | Zsolt Venczel |
| [HDFS-14537](https://issues.apache.org/jira/browse/HDFS-14537) | Journaled Edits Cache is not cleared when formatting the JN |  Major | . | Ranith Sardar | Ranith Sardar |
| [HDFS-14581](https://issues.apache.org/jira/browse/HDFS-14581) | Appending to EC files crashes NameNode |  Critical | erasure-coding | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-14465](https://issues.apache.org/jira/browse/HDFS-14465) | When the Block expected replications is larger than the number of DataNodes, entering maintenance will never exit. |  Major | . | Yicong Cai | Yicong Cai |
| [HDFS-13893](https://issues.apache.org/jira/browse/HDFS-13893) | DiskBalancer: no validations for Disk balancer commands |  Major | diskbalancer | Harshakiran Reddy | Lokesh Jain |
| [YARN-9209](https://issues.apache.org/jira/browse/YARN-9209) | When nodePartition is not set in Placement Constraints, containers are allocated only in default partition |  Major | capacity scheduler, scheduler | Tarun Parimi | Tarun Parimi |
| [HADOOP-15989](https://issues.apache.org/jira/browse/HADOOP-15989) | Synchronized at CompositeService#removeService is not required |  Major | common | Prabhu Joseph | Prabhu Joseph |
| [HDFS-12487](https://issues.apache.org/jira/browse/HDFS-12487) | FsDatasetSpi.isValidBlock() lacks null pointer check inside and neither do the callers |  Major | balancer & mover, diskbalancer | liumi | liumi |
| [HDFS-14074](https://issues.apache.org/jira/browse/HDFS-14074) | DataNode runs async disk checks  maybe  throws NullPointerException, and DataNode failed to register to NameSpace. |  Major | hdfs | guangyi lu | guangyi lu |
| [HDFS-14541](https://issues.apache.org/jira/browse/HDFS-14541) |  When evictableMmapped or evictable size is zero, do not throw NoSuchElementException |  Major | hdfs-client, performance | Zheng Hu | Lisheng Sun |
| [HDFS-13371](https://issues.apache.org/jira/browse/HDFS-13371) | NPE for FsServerDefaults.getKeyProviderUri() for clientProtocol communication between 2.7 and 3.X |  Minor | . | Sherwood Zheng | Sherwood Zheng |
| [HDFS-14598](https://issues.apache.org/jira/browse/HDFS-14598) | Findbugs warning caused by HDFS-12487 |  Minor | diskbalancer | Wei-Chiu Chuang | Xiaoqiao He |
| [HADOOP-16390](https://issues.apache.org/jira/browse/HADOOP-16390) | Build fails due to bad use of '\>' in javadoc |  Critical | build | Kei Kori | Kei Kori |
| [YARN-9639](https://issues.apache.org/jira/browse/YARN-9639) | DecommissioningNodesWatcher cause memory leak |  Blocker | . | Bibin Chundatt | Bilwa S T |
| [HDFS-14599](https://issues.apache.org/jira/browse/HDFS-14599) | HDFS-12487 breaks test TestDiskBalancer.testDiskBalancerWithFedClusterWithOneNameServiceEmpty |  Major | diskbalancer | Wei-Chiu Chuang | Xiaoqiao He |
| [YARN-9581](https://issues.apache.org/jira/browse/YARN-9581) | Fix WebAppUtils#getRMWebAppURLWithScheme ignores rm2 |  Major | client | Prabhu Joseph | Prabhu Joseph |
| [HDFS-14618](https://issues.apache.org/jira/browse/HDFS-14618) | Incorrect synchronization of ArrayList field (ArrayList is thread-unsafe). |  Critical | . | Paul Ward | Paul Ward |
| [YARN-9661](https://issues.apache.org/jira/browse/YARN-9661) | Fix typos in LocalityMulticastAMRMProxyPolicy and AbstractConfigurableFederationPolicy |  Major | federation, yarn | hunshenshi | hunshenshi |
| [HDFS-14610](https://issues.apache.org/jira/browse/HDFS-14610) | HashMap is not thread safe. Field storageMap is typically synchronized by storageMap. However, in one place, field storageMap is not protected with synchronized. |  Critical | . | Paul Ward | Paul Ward |
| [YARN-9327](https://issues.apache.org/jira/browse/YARN-9327) | Improve synchronisation in ProtoUtils#convertToProtoFormat block |  Critical | . | Bibin Chundatt | Bibin Chundatt |
| [YARN-9655](https://issues.apache.org/jira/browse/YARN-9655) | AllocateResponse in FederationInterceptor lost  applicationPriority |  Major | federation | hunshenshi | hunshenshi |
| [HADOOP-16385](https://issues.apache.org/jira/browse/HADOOP-16385) | Namenode crashes with "RedundancyMonitor thread received Runtime exception" |  Major | . | krishna reddy | Ayush Saxena |
| [YARN-9658](https://issues.apache.org/jira/browse/YARN-9658) | Fix UT failures in TestLeafQueue |  Minor | . | Tao Yang | Tao Yang |
| [YARN-9644](https://issues.apache.org/jira/browse/YARN-9644) | First RMContext object is always leaked during switch over |  Blocker | . | Bibin Chundatt | Bibin Chundatt |
| [HDFS-14629](https://issues.apache.org/jira/browse/HDFS-14629) | Property value Hard Coded in DNConf.java |  Trivial | . | hemanthboyina | hemanthboyina |
| [HADOOP-16411](https://issues.apache.org/jira/browse/HADOOP-16411) | Fix javadoc warnings in hadoop-dynamometer |  Minor | tools | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-9557](https://issues.apache.org/jira/browse/YARN-9557) | Application fails in diskchecker when ReadWriteDiskValidator is configured. |  Critical | nodemanager | Anuruddh Nayak | Bilwa S T |
| [HDFS-12703](https://issues.apache.org/jira/browse/HDFS-12703) | Exceptions are fatal to decommissioning monitor |  Critical | namenode | Daryn Sharp | Xiaoqiao He |
| [HADOOP-16381](https://issues.apache.org/jira/browse/HADOOP-16381) | The JSON License is included in binary tarball via azure-documentdb:1.16.2 |  Blocker | . | Akira Ajisaka | Sushil Ks |
| [HDFS-12748](https://issues.apache.org/jira/browse/HDFS-12748) | NameNode memory leak when accessing webhdfs GETHOMEDIRECTORY |  Major | hdfs | Jiandan Yang | Weiwei Yang |
| [HADOOP-16418](https://issues.apache.org/jira/browse/HADOOP-16418) | Fix checkstyle and findbugs warnings in hadoop-dynamometer |  Minor | tools | Masatake Iwasaki | Erik Krogen |
| [YARN-9625](https://issues.apache.org/jira/browse/YARN-9625) | UI2 - No link to a queue on the Queues page for Fair Scheduler |  Major | . | Charan Hebri | Zoltan Siegl |
| [HDFS-14466](https://issues.apache.org/jira/browse/HDFS-14466) | Add a regression test for HDFS-14323 |  Minor | fs, test, webhdfs | Yuya Ebihara | Masatake Iwasaki |
| [HDFS-14499](https://issues.apache.org/jira/browse/HDFS-14499) | Misleading REM\_QUOTA value with snapshot and trash feature enabled for a directory |  Major | snapshots | Shashikant Banerjee | Shashikant Banerjee |
| [YARN-9235](https://issues.apache.org/jira/browse/YARN-9235) | If linux container executor is not set for a GPU cluster GpuResourceHandlerImpl is not initialized and NPE is thrown |  Major | yarn | Antal Bálint Steinbach | Adam Antal |
| [YARN-9626](https://issues.apache.org/jira/browse/YARN-9626) | UI2 - Fair scheduler queue apps page issues |  Major | . | Charan Hebri | Zoltan Siegl |
| [HDFS-14642](https://issues.apache.org/jira/browse/HDFS-14642) | processMisReplicatedBlocks does not return correct processed count |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [YARN-9645](https://issues.apache.org/jira/browse/YARN-9645) | Fix Invalid event FINISHED\_CONTAINERS\_PULLED\_BY\_AM at NEW on NM restart |  Major | . | krishna reddy | Bilwa S T |
| [YARN-9646](https://issues.apache.org/jira/browse/YARN-9646) | DistributedShell tests failed to bind to a local host name |  Major | test | Ray Yang | Ray Yang |
| [YARN-9682](https://issues.apache.org/jira/browse/YARN-9682) | Wrong log message when finalizing the upgrade |  Trivial | . | kyungwan nam | kyungwan nam |
| [HDFS-13647](https://issues.apache.org/jira/browse/HDFS-13647) | Fix the description of storageType option for space quota |  Major | documentation, tools | Takanobu Asanuma | Takanobu Asanuma |
| [MAPREDUCE-6521](https://issues.apache.org/jira/browse/MAPREDUCE-6521) | MiniMRYarnCluster should not create /tmp/hadoop-yarn/staging on local filesystem in unit test |  Major | test | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-9568](https://issues.apache.org/jira/browse/YARN-9568) | NPE in MiniYarnCluster during FileSystemNodeAttributeStore.recover |  Minor | resourcemanager, test | Steve Loughran | Steve Loughran |
| [YARN-6046](https://issues.apache.org/jira/browse/YARN-6046) | Documentation correction in YarnApplicationSecurity |  Trivial | . | Bibin Chundatt | Yousef Abu-Salah |
| [HADOOP-16440](https://issues.apache.org/jira/browse/HADOOP-16440) | Distcp can not preserve timestamp with -delete  option |  Major | . | ludun | ludun |
| [MAPREDUCE-7076](https://issues.apache.org/jira/browse/MAPREDUCE-7076) | TestNNBench#testNNBenchCreateReadAndDelete failing in our internal build |  Minor | test | Rushabh Shah | Kevin Su |
| [YARN-9668](https://issues.apache.org/jira/browse/YARN-9668) | UGI conf doesn't read user overridden configurations on RM and NM startup |  Major | . | Jonathan Hung | Jonathan Hung |
| [HADOOP-16451](https://issues.apache.org/jira/browse/HADOOP-16451) | Update jackson-databind to 2.9.9.1 |  Major | . | Wei-Chiu Chuang | Siyao Meng |
| [HDFS-14647](https://issues.apache.org/jira/browse/HDFS-14647) | NPE during secure namenode startup |  Major | hdfs | Fengnan Li | Fengnan Li |
| [HADOOP-9844](https://issues.apache.org/jira/browse/HADOOP-9844) | NPE when trying to create an error message response of SASL RPC |  Major | ipc | Steve Loughran | Steve Loughran |
| [HADOOP-16461](https://issues.apache.org/jira/browse/HADOOP-16461) | Regression: FileSystem cache lock parses XML within the lock |  Major | fs | Gopal Vijayaraghavan | Gopal Vijayaraghavan |
| [HDFS-14135](https://issues.apache.org/jira/browse/HDFS-14135) | TestWebHdfsTimeouts Fails intermittently in trunk |  Major | . | Ayush Saxena | Ayush Saxena |
| [MAPREDUCE-6973](https://issues.apache.org/jira/browse/MAPREDUCE-6973) | Fix comments on creating \_SUCCESS file. |  Trivial | documentation | Mehul Garnara (MG) | Mehul Garnara (MG) |
| [HADOOP-16245](https://issues.apache.org/jira/browse/HADOOP-16245) | Enabling SSL within LdapGroupsMapping can break system SSL configs |  Major | common, security | Erik Krogen | Erik Krogen |
| [HDFS-14425](https://issues.apache.org/jira/browse/HDFS-14425) | Native build fails on macos due to jlong in hdfs.c |  Major | . | hunshenshi | hunshenshi |
| [HDFS-14660](https://issues.apache.org/jira/browse/HDFS-14660) | [SBN Read] ObserverNameNode should throw StandbyException for requests not from ObserverProxyProvider |  Major | . | Chao Sun | Chao Sun |
| [HDFS-14429](https://issues.apache.org/jira/browse/HDFS-14429) | Block remain in COMMITTED but not COMPLETE caused by Decommission |  Major | . | Yicong Cai | Yicong Cai |
| [HADOOP-16435](https://issues.apache.org/jira/browse/HADOOP-16435) | RpcMetrics should not be retained forever |  Critical | rpc-server | Zoltan Haindrich | Zoltan Haindrich |
| [HADOOP-15910](https://issues.apache.org/jira/browse/HADOOP-15910) | Javadoc for LdapAuthenticationHandler#ENABLE\_START\_TLS is wrong |  Trivial | . | Ted Yu | Don Jeba |
| [HDFS-14677](https://issues.apache.org/jira/browse/HDFS-14677) | TestDataNodeHotSwapVolumes#testAddVolumesConcurrently fails intermittently in trunk |  Major | . | Chen Zhang | Chen Zhang |
| [HADOOP-16460](https://issues.apache.org/jira/browse/HADOOP-16460) | ABFS: fix for Sever Name Indication (SNI) |  Major | fs/azure | Thomas Marqardt | Sneha Vijayarajan |
| [HDFS-14569](https://issues.apache.org/jira/browse/HDFS-14569) | Result of crypto -listZones is not formatted properly |  Major | . | hemanthboyina | hemanthboyina |
| [YARN-9596](https://issues.apache.org/jira/browse/YARN-9596) | QueueMetrics has incorrect metrics when labelled partitions are involved |  Major | capacity scheduler | Muhammad Samir Khan | Muhammad Samir Khan |
| [HDFS-14681](https://issues.apache.org/jira/browse/HDFS-14681) | RBF: TestDisableRouterQuota failed because port 8888 was occupied |  Minor | rbf | Wei-Chiu Chuang | Chao Sun |
| [HDFS-14661](https://issues.apache.org/jira/browse/HDFS-14661) | RBF: updateMountTableEntry shouldn't update mountTableEntry if targetPath not exist |  Major | rbf | xuzq | xuzq |
| [MAPREDUCE-7225](https://issues.apache.org/jira/browse/MAPREDUCE-7225) | Fix broken current folder expansion during MR job start |  Major | mrv2 | Adam Antal | Peter Bacsko |
| [HDFS-13529](https://issues.apache.org/jira/browse/HDFS-13529) | Fix default trash policy emptier trigger time correctly |  Major | namenode | Xiaoqiao He | Xiaoqiao He |
| [HADOOP-12282](https://issues.apache.org/jira/browse/HADOOP-12282) | Connection thread's name should be updated after address changing is detected |  Major | ipc | zhouyingchao | Lisheng Sun |
| [HADOOP-15410](https://issues.apache.org/jira/browse/HADOOP-15410) | Update scope of log4j in hadoop-auth to provided |  Major | . | lqjack | lqjacklee |
| [HDFS-14686](https://issues.apache.org/jira/browse/HDFS-14686) | HttpFS: HttpFSFileSystem#getErasureCodingPolicy always returns null |  Major | httpfs | Siyao Meng | Siyao Meng |
| [HADOOP-15681](https://issues.apache.org/jira/browse/HADOOP-15681) | AuthenticationFilter should generate valid date format for Set-Cookie header regardless of default Locale |  Minor | security | Cao Manh Dat | Cao Manh Dat |
| [HDFS-13131](https://issues.apache.org/jira/browse/HDFS-13131) | Modifying testcase testEnableAndDisableErasureCodingPolicy |  Minor | . | chencan | chencan |
| [HADOOP-15865](https://issues.apache.org/jira/browse/HADOOP-15865) | ConcurrentModificationException in Configuration.overlay() method |  Major | . | Oleksandr Shevchenko | Oleksandr Shevchenko |
| [HDFS-14685](https://issues.apache.org/jira/browse/HDFS-14685) | DefaultAuditLogger doesn't print CallerContext |  Major | hdfs | xuzq | xuzq |
| [HDFS-14462](https://issues.apache.org/jira/browse/HDFS-14462) | WebHDFS throws "Error writing request body to server" instead of DSQuotaExceededException |  Major | webhdfs | Erik Krogen | Simbarashe Dzinamarira |
| [HDFS-12826](https://issues.apache.org/jira/browse/HDFS-12826) | Document Saying the RPC port, But it's required IPC port in HDFS Federation Document. |  Minor | balancer & mover, documentation | Harshakiran Reddy | usharani |
| [HDFS-14669](https://issues.apache.org/jira/browse/HDFS-14669) | TestDirectoryScanner#testDirectoryScannerInFederatedCluster fails intermittently in trunk |  Minor | datanode | qiang Liu | qiang Liu |
| [HADOOP-16487](https://issues.apache.org/jira/browse/HADOOP-16487) | Update jackson-databind to 2.9.9.2 |  Critical | . | Siyao Meng | Siyao Meng |
| [HDFS-14679](https://issues.apache.org/jira/browse/HDFS-14679) | Failed to add erasure code policies with example template |  Minor | ec | Yuan Zhou | Yuan Zhou |
| [YARN-9410](https://issues.apache.org/jira/browse/YARN-9410) | Typo in documentation: Using FPGA On YARN |  Major | . | Szilard Nemeth | Kevin Su |
| [HDFS-14557](https://issues.apache.org/jira/browse/HDFS-14557) | JournalNode error: Can't scan a pre-transactional edit log |  Major | ha | Wei-Chiu Chuang | Stephen O'Donnell |
| [HADOOP-16457](https://issues.apache.org/jira/browse/HADOOP-16457) | Hadoop does not work with Kerberos config in hdfs-site.xml for simple security |  Minor | . | Eric Yang | Prabhu Joseph |
| [HDFS-14692](https://issues.apache.org/jira/browse/HDFS-14692) | Upload button should not encode complete url |  Major | . | Lokesh Jain | Lokesh Jain |
| [HADOOP-15908](https://issues.apache.org/jira/browse/HADOOP-15908) | hadoop-build-tools jar is downloaded from remote repository instead of using from local |  Minor | build | Oleksandr Shevchenko | Oleksandr Shevchenko |
| [HDFS-14631](https://issues.apache.org/jira/browse/HDFS-14631) | The DirectoryScanner doesn't fix the wrongly placed replica. |  Major | . | Jinglun | Jinglun |
| [YARN-9601](https://issues.apache.org/jira/browse/YARN-9601) | Potential NPE in ZookeeperFederationStateStore#getPoliciesConfigurations |  Major | federation, yarn | hunshenshi | hunshenshi |
| [YARN-9685](https://issues.apache.org/jira/browse/YARN-9685) | NPE when rendering the info table of leaf queue in non-accessible partitions |  Major | capacityscheduler | Tao Yang | Tao Yang |
| [HDFS-14459](https://issues.apache.org/jira/browse/HDFS-14459) | ClosedChannelException silently ignored in FsVolumeList.addBlockPool() |  Major | datanode | Stephen O'Donnell | Stephen O'Donnell |
| [YARN-9732](https://issues.apache.org/jira/browse/YARN-9732) | yarn.system-metrics-publisher.enabled=false is not honored by RM |  Major | resourcemanager, timelineclient | KWON BYUNGCHANG | KWON BYUNGCHANG |
| [YARN-9527](https://issues.apache.org/jira/browse/YARN-9527) | Rogue LocalizerRunner/ContainerLocalizer repeatedly downloading same file |  Major | yarn | Jim Brennan | Jim Brennan |
| [HDFS-14623](https://issues.apache.org/jira/browse/HDFS-14623) | In NameNode Web UI, for Head the file (first 32K) old data is showing |  Major | . | hemanthboyina | hemanthboyina |
| [HDFS-12125](https://issues.apache.org/jira/browse/HDFS-12125) | Document the missing EC removePolicy command |  Major | documentation, erasure-coding | Wenxin He | Siyao Meng |
| [HDFS-13359](https://issues.apache.org/jira/browse/HDFS-13359) | DataXceiver hung due to the lock in FsDatasetImpl#getBlockInputStream |  Major | datanode | Yiqun Lin | Yiqun Lin |
| [YARN-9722](https://issues.apache.org/jira/browse/YARN-9722) | PlacementRule logs object ID in place of queue name. |  Minor | resourcemanager | Prabhu Joseph | Prabhu Joseph |
| [YARN-9451](https://issues.apache.org/jira/browse/YARN-9451) | AggregatedLogsBlock shows wrong NM http port |  Minor | nodemanager | Prabhu Joseph | Prabhu Joseph |
| [YARN-9723](https://issues.apache.org/jira/browse/YARN-9723) | ApplicationPlacementContext is not required for terminated jobs during recovery |  Major | resourcemanager | Prabhu Joseph | Prabhu Joseph |
| [HDFS-12914](https://issues.apache.org/jira/browse/HDFS-12914) | Block report leases cause missing blocks until next report |  Critical | namenode | Daryn Sharp | Santosh Marella |
| [YARN-9719](https://issues.apache.org/jira/browse/YARN-9719) | Failed to restart yarn-service if it doesn’t exist in RM |  Major | yarn-native-services | kyungwan nam | kyungwan nam |
| [HDFS-14148](https://issues.apache.org/jira/browse/HDFS-14148) | HDFS OIV ReverseXML SnapshotSection parser throws exception when there are more than one snapshottable directory |  Major | hdfs | Siyao Meng | Siyao Meng |
| [HDFS-14708](https://issues.apache.org/jira/browse/HDFS-14708) | TestLargeBlockReport#testBlockReportSucceedsWithLargerLengthLimit fails in trunk |  Minor | . | Lisheng Sun | Lisheng Sun |
| [YARN-9744](https://issues.apache.org/jira/browse/YARN-9744) | RollingLevelDBTimelineStore.getEntityByTime fails with NPE |  Major | timelineserver | Prabhu Joseph | Prabhu Joseph |
| [HADOOP-16495](https://issues.apache.org/jira/browse/HADOOP-16495) | Fix invalid metric types in PrometheusMetricsSink |  Major | metrics | Akira Ajisaka | Akira Ajisaka |
| [YARN-9747](https://issues.apache.org/jira/browse/YARN-9747) | Reduce additional namenode call by EntityGroupFSTimelineStore#cleanLogs |  Major | timelineserver | Prabhu Joseph | Prabhu Joseph |
| [HDFS-14595](https://issues.apache.org/jira/browse/HDFS-14595) | HDFS-11848 breaks API compatibility |  Blocker | . | Wei-Chiu Chuang | Siyao Meng |
| [HDFS-14423](https://issues.apache.org/jira/browse/HDFS-14423) | Percent (%) and plus (+) characters no longer work in WebHDFS |  Major | webhdfs | Jing Wang | Masatake Iwasaki |
| [HDFS-14719](https://issues.apache.org/jira/browse/HDFS-14719) | Correct the safemode threshold value in BlockManagerSafeMode |  Major | namenode | Surendra Singh Lilhore | hemanthboyina |
| [MAPREDUCE-7230](https://issues.apache.org/jira/browse/MAPREDUCE-7230) | TestHSWebApp.testLogsViewSingle fails |  Major | jobhistoryserver, test | Prabhu Joseph | Prabhu Joseph |
| [YARN-9749](https://issues.apache.org/jira/browse/YARN-9749) | TestAppLogAggregatorImpl#testDFSQuotaExceeded fails on trunk |  Major | log-aggregation, test | Peter Bacsko | Adam Antal |
| [YARN-9461](https://issues.apache.org/jira/browse/YARN-9461) | TestRMWebServicesDelegationTokenAuthentication.testCancelledDelegationToken fails with HTTP 400 |  Minor | resourcemanager, test | Peter Bacsko | Peter Bacsko |
| [HADOOP-16391](https://issues.apache.org/jira/browse/HADOOP-16391) | Duplicate values in rpcDetailedMetrics |  Major | . | Bilwa S T | Bilwa S T |
| [HDFS-14456](https://issues.apache.org/jira/browse/HDFS-14456) | HAState#prepareToEnterState needn't a lock |  Major | hdfs | hunshenshi | hunshenshi |
| [YARN-2599](https://issues.apache.org/jira/browse/YARN-2599) | Standby RM should expose jmx endpoint |  Major | resourcemanager | Karthik Kambatla | Rohith Sharma K S |
| [HDFS-12012](https://issues.apache.org/jira/browse/HDFS-12012) | Fix spelling mistakes in BPServiceActor.java. |  Major | datanode | chencan | chencan |
| [HDFS-14127](https://issues.apache.org/jira/browse/HDFS-14127) | Add a description about the observer read configuration |  Minor | . | xiangheng | xiangheng |
| [HDFS-14687](https://issues.apache.org/jira/browse/HDFS-14687) | Standby Namenode never come out of safemode when EC files are being written. |  Critical | ec, namenode | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-13101](https://issues.apache.org/jira/browse/HDFS-13101) | Yet another fsimage corruption related to snapshot |  Critical | snapshots | Yongjun Zhang | Shashikant Banerjee |
| [YARN-9758](https://issues.apache.org/jira/browse/YARN-9758) | Upgrade JQuery to latest version for YARN UI |  Major | yarn | Akhil PB | Akhil PB |
| [HDFS-13201](https://issues.apache.org/jira/browse/HDFS-13201) | Fix prompt message in testPolicyAndStateCantBeNull |  Minor | . | chencan | chencan |
| [HDFS-14311](https://issues.apache.org/jira/browse/HDFS-14311) | Multi-threading conflict at layoutVersion when loading block pool storage |  Major | rolling upgrades | Yicong Cai | Yicong Cai |
| [HDFS-14582](https://issues.apache.org/jira/browse/HDFS-14582) | Failed to start DN with ArithmeticException when NULL checksum used |  Major | datanode | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HADOOP-16496](https://issues.apache.org/jira/browse/HADOOP-16496) | Apply HDDS-1870 (ConcurrentModification at PrometheusMetricsSink) to Hadoop common |  Major | metrics | Akira Ajisaka | Akira Ajisaka |
| [YARN-5857](https://issues.apache.org/jira/browse/YARN-5857) | TestLogAggregationService.testFixedSizeThreadPool fails intermittently on trunk |  Minor | . | Varun Saxena | Bilwa S T |
| [YARN-9217](https://issues.apache.org/jira/browse/YARN-9217) | Nodemanager will fail to start if GPU is misconfigured on the node or GPU drivers missing |  Major | yarn | Antal Bálint Steinbach | Peter Bacsko |
| [HDFS-14759](https://issues.apache.org/jira/browse/HDFS-14759) | HDFS cat logs an info message |  Major | . | Eric Badger | Eric Badger |
| [HADOOP-16494](https://issues.apache.org/jira/browse/HADOOP-16494) | Add SHA-256 or SHA-512 checksum to release artifacts to comply with the release distribution policy |  Blocker | build | Akira Ajisaka | Akira Ajisaka |
| [HDFS-14741](https://issues.apache.org/jira/browse/HDFS-14741) | RBF: RecoverLease should be return false when the file is open in multiple destination |  Major | rbf | xuzq | xuzq |
| [HDFS-14583](https://issues.apache.org/jira/browse/HDFS-14583) | FileStatus#toString() will throw IllegalArgumentException |  Major | . | xuzq | xuzq |
| [YARN-9774](https://issues.apache.org/jira/browse/YARN-9774) | Fix order of arguments for assertEquals in TestSLSUtils |  Minor | test | Nikhil Navadiya | Nikhil Navadiya |
| [HDFS-13596](https://issues.apache.org/jira/browse/HDFS-13596) | NN restart fails after RollingUpgrade from 2.x to 3.x |  Blocker | hdfs | Hanisha Koneru | Fei Hui |
| [HDFS-14396](https://issues.apache.org/jira/browse/HDFS-14396) | Failed to load image from FSImageFile when downgrade from 3.x to 2.x |  Blocker | rolling upgrades | Fei Hui | Fei Hui |
| [HDFS-14747](https://issues.apache.org/jira/browse/HDFS-14747) | RBF: IsFileClosed should be return false when the file is open in multiple destination |  Major | rbf | xuzq | xuzq |
| [HDFS-14761](https://issues.apache.org/jira/browse/HDFS-14761) | RBF: MountTableResolver cannot invalidate cache correctly |  Major | rbf | Yuxuan Wang | Yuxuan Wang |
| [HDFS-14722](https://issues.apache.org/jira/browse/HDFS-14722) | RBF: GetMountPointStatus should return mountTable information when getFileInfoAll throw IOException |  Major | rbf | xuzq | xuzq |
| [YARN-8917](https://issues.apache.org/jira/browse/YARN-8917) | Absolute (maximum) capacity of level3+ queues is wrongly calculated for absolute resource |  Critical | capacityscheduler | Tao Yang | Tao Yang |
| [YARN-9771](https://issues.apache.org/jira/browse/YARN-9771) | Add GPU in the container-executor.cfg example |  Trivial | nodemanager, yarn | Adam Antal | Kinga Marton |
| [YARN-9642](https://issues.apache.org/jira/browse/YARN-9642) | Fix Memory Leak in AbstractYarnScheduler caused by timer |  Blocker | resourcemanager | Bibin Chundatt | Bibin Chundatt |
| [HDFS-13977](https://issues.apache.org/jira/browse/HDFS-13977) | NameNode can kill itself if it tries to send too many txns to a QJM simultaneously |  Major | namenode, qjm | Erik Krogen | Erik Krogen |
| [HDFS-2470](https://issues.apache.org/jira/browse/HDFS-2470) | NN should automatically set permissions on dfs.namenode.\*.dir |  Major | namenode | Aaron Myers | Siddharth Wagle |
| [HADOOP-15958](https://issues.apache.org/jira/browse/HADOOP-15958) | Revisiting LICENSE and NOTICE files |  Blocker | . | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-16533](https://issues.apache.org/jira/browse/HADOOP-16533) | Update jackson-databind to 2.9.9.3 |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [YARN-9438](https://issues.apache.org/jira/browse/YARN-9438) | launchTime not written to state store for running applications |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-9640](https://issues.apache.org/jira/browse/YARN-9640) | Slow event processing could cause too many attempt unregister events |  Critical | . | Bibin Chundatt | Bibin Chundatt |
| [HDFS-14721](https://issues.apache.org/jira/browse/HDFS-14721) | RBF: ProxyOpComplete is not accurate in FederationRPCPerformanceMonitor |  Major | rbf | xuzq | xuzq |
| [HDFS-11246](https://issues.apache.org/jira/browse/HDFS-11246) | FSNameSystem#logAuditEvent should be called outside the read or write locks |  Major | . | Kuhu Shukla | Xiaoqiao He |
| [HDFS-12212](https://issues.apache.org/jira/browse/HDFS-12212) | Options.Rename.To\_TRASH is considered even when Options.Rename.NONE is specified |  Major | namenode | Vinayakumar B | Vinayakumar B |
| [YARN-9714](https://issues.apache.org/jira/browse/YARN-9714) | ZooKeeper connection in ZKRMStateStore leaks after RM transitioned to standby |  Major | resourcemanager | Tao Yang | Tao Yang |
| [HDFS-14796](https://issues.apache.org/jira/browse/HDFS-14796) | Define LOG instead of BlockManager.LOG in ErasureCodingWork/ReplicationWork |  Major | . | Fei Hui | Fei Hui |
| [YARN-9540](https://issues.apache.org/jira/browse/YARN-9540) | TestRMAppTransitions fails intermittently |  Minor | resourcemanager, test | Prabhu Joseph | Tao Yang |
| [HDFS-8178](https://issues.apache.org/jira/browse/HDFS-8178) | QJM doesn't move aside stale inprogress edits files |  Major | qjm | Zhe Zhang | Istvan Fajth |
| [YARN-9798](https://issues.apache.org/jira/browse/YARN-9798) | ApplicationMasterServiceTestBase#testRepeatedFinishApplicationMaster fails intermittently |  Minor | test | Tao Yang | Tao Yang |
| [YARN-9800](https://issues.apache.org/jira/browse/YARN-9800) | TestRMDelegationTokens can fail in testRemoveExpiredMasterKeyInRMStateStore |  Major | test, yarn | Adam Antal | Adam Antal |
| [YARN-9793](https://issues.apache.org/jira/browse/YARN-9793) | Remove duplicate sentence from TimelineServiceV2.md |  Major | ATSv2, docs | Kinga Marton | Kinga Marton |
| [YARN-8174](https://issues.apache.org/jira/browse/YARN-8174) | Add containerId to ResourceLocalizationService fetch failure log statement |  Minor | nodemanager | Prabhu Joseph | Prabhu Joseph |
| [HDFS-14706](https://issues.apache.org/jira/browse/HDFS-14706) | Checksums are not checked if block meta file is less than 7 bytes |  Major | . | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-14630](https://issues.apache.org/jira/browse/HDFS-14630) | Configuration.getTimeDurationHelper() should not log time unit warning in info log. |  Minor | hdfs | Surendra Singh Lilhore | hemanthboyina |
| [YARN-9797](https://issues.apache.org/jira/browse/YARN-9797) | LeafQueue#activateApplications should use resourceCalculator#fitsIn |  Blocker | . | Bibin Chundatt | Bilwa S T |
| [HADOOP-16534](https://issues.apache.org/jira/browse/HADOOP-16534) | Exclude submarine from hadoop source build |  Major | . | Nanda kumar | Nanda kumar |
| [HDFS-14807](https://issues.apache.org/jira/browse/HDFS-14807) | SetTimes updates all negative values apart from -1 |  Major | . | Harshakiran Reddy | Ayush Saxena |
| [YARN-9785](https://issues.apache.org/jira/browse/YARN-9785) | Fix DominantResourceCalculator when one resource is zero |  Blocker | . | Bilwa S T | Bilwa S T |
| [HDFS-14777](https://issues.apache.org/jira/browse/HDFS-14777) | RBF: Set ReadOnly is failing for mount Table but actually readonly succeed to set |  Major | . | Ranith Sardar | Ranith Sardar |
| [YARN-9718](https://issues.apache.org/jira/browse/YARN-9718) | Yarn REST API, services endpoint remote command ejection |  Major | . | Eric Yang | Eric Yang |
| [HDFS-14826](https://issues.apache.org/jira/browse/HDFS-14826) | dfs.ha.zkfc.port property duplicated in hdfs-default.xml |  Major | hdfs | Renukaprasad C | Renukaprasad C |
| [YARN-9817](https://issues.apache.org/jira/browse/YARN-9817) | Fix failing testcases due to not initialized AsyncDispatcher -  ArithmeticException: / by zero |  Major | test | Prabhu Joseph | Prabhu Joseph |
| [YARN-9813](https://issues.apache.org/jira/browse/YARN-9813) | RM does not start on JDK11 when UIv2 is enabled |  Critical | resourcemanager, yarn | Adam Antal | Adam Antal |
| [YARN-9812](https://issues.apache.org/jira/browse/YARN-9812) | mvn javadoc:javadoc fails in hadoop-sls |  Major | documentation | Akira Ajisaka | Abhishek Modi |
| [YARN-9784](https://issues.apache.org/jira/browse/YARN-9784) | org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestLeafQueue is flaky |  Major | test | Kinga Marton | Kinga Marton |
| [YARN-9820](https://issues.apache.org/jira/browse/YARN-9820) | RM logs InvalidStateTransitionException when app is submitted |  Critical | . | Rohith Sharma K S | Prabhu Joseph |
| [HADOOP-16554](https://issues.apache.org/jira/browse/HADOOP-16554) | mvn javadoc:javadoc fails in hadoop-aws |  Major | documentation | Akira Ajisaka | Xieming Li |
| [YARN-9728](https://issues.apache.org/jira/browse/YARN-9728) |  ResourceManager REST API can produce an illegal xml response |  Major | api, resourcemanager | Thomas | Prabhu Joseph |
| [HDFS-14835](https://issues.apache.org/jira/browse/HDFS-14835) | RBF: Secured Router should not run when it can't initialize DelegationTokenSecretManager |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-14838](https://issues.apache.org/jira/browse/HDFS-14838) | RBF: Display RPC (instead of HTTP) Port Number in RBF web UI |  Minor | rbf, ui | Xieming Li | Xieming Li |
| [YARN-9816](https://issues.apache.org/jira/browse/YARN-9816) | EntityGroupFSTimelineStore#scanActiveLogs fails when undesired files are present under /ats/active. |  Major | timelineserver | Prabhu Joseph | Prabhu Joseph |
| [HDFS-14699](https://issues.apache.org/jira/browse/HDFS-14699) | Erasure Coding: Storage not considered in live replica when replication streams hard limit reached to threshold |  Critical | ec | Zhao Yi Ming | Zhao Yi Ming |
| [HDFS-14798](https://issues.apache.org/jira/browse/HDFS-14798) | Synchronize invalidateBlocks in DatanodeDescriptor |  Minor | namenode | David Mollitor | hemanthboyina |
| [HDFS-14821](https://issues.apache.org/jira/browse/HDFS-14821) | Make HDFS-14617 (fsimage sub-sections) off by default |  Blocker | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-14303](https://issues.apache.org/jira/browse/HDFS-14303) | check block directory logic not correct when there is only meta file, print no meaning warn log |  Minor | datanode, hdfs | qiang Liu | qiang Liu |
| [YARN-9833](https://issues.apache.org/jira/browse/YARN-9833) | Race condition when DirectoryCollection.checkDirs() runs during container launch |  Major | . | Peter Bacsko | Peter Bacsko |
| [YARN-9837](https://issues.apache.org/jira/browse/YARN-9837) | YARN Service fails to fetch status for Stopped apps with bigger spec files |  Major | yarn-native-services | Tarun Parimi | Tarun Parimi |
| [YARN-2255](https://issues.apache.org/jira/browse/YARN-2255) | YARN Audit logging not added to log4j.properties |  Major | . | Varun Saxena | Aihua Xu |
| [YARN-9814](https://issues.apache.org/jira/browse/YARN-9814) | JobHistoryServer can't delete aggregated files, if remote app root directory is created by NodeManager |  Minor | log-aggregation, yarn | Adam Antal | Adam Antal |
| [HDFS-14836](https://issues.apache.org/jira/browse/HDFS-14836) | FileIoProvider should not increase FileIoErrors metric in datanode volume metric |  Minor | . | Aiphago | Aiphago |
| [HDFS-14846](https://issues.apache.org/jira/browse/HDFS-14846) | libhdfs tests are failing on trunk due to jni usage bugs |  Major | libhdfs, native | Sahil Takiar | Sahil Takiar |
| [HADOOP-16582](https://issues.apache.org/jira/browse/HADOOP-16582) | LocalFileSystem's mkdirs() does not work as expected under viewfs. |  Major | . | Kihwal Lee | Kihwal Lee |
| [HDFS-14609](https://issues.apache.org/jira/browse/HDFS-14609) | RBF: Security should use common AuthenticationFilter |  Major | . | CR Hota | Chen Zhang |
| [HADOOP-16581](https://issues.apache.org/jira/browse/HADOOP-16581) | ValueQueue does not trigger an async refill when number of values falls below watermark |  Major | common, kms | Yuval Degani | Yuval Degani |
| [HDFS-14853](https://issues.apache.org/jira/browse/HDFS-14853) | NPE in DFSNetworkTopology#chooseRandomWithStorageType() when the excludedNode is not present |  Major | . | Ranith Sardar | Ranith Sardar |
| [HDFS-13660](https://issues.apache.org/jira/browse/HDFS-13660) | DistCp job fails when new data is appended in the file while the distCp copy job is running |  Critical | distcp | Mukund Thakur | Mukund Thakur |
| [HDFS-14868](https://issues.apache.org/jira/browse/HDFS-14868) | RBF: Fix typo in TestRouterQuota |  Trivial | . | Jinglun | Jinglun |
| [HDFS-14808](https://issues.apache.org/jira/browse/HDFS-14808) | EC: Improper size values for corrupt ec block in LOG |  Major | ec | Harshakiran Reddy | Ayush Saxena |
| [HDFS-14845](https://issues.apache.org/jira/browse/HDFS-14845) | Ignore AuthenticationFilterInitializer for HttpFSServerWebServer and honor hadoop.http.authentication configs |  Critical | httpfs | Akira Ajisaka | Prabhu Joseph |
| [HADOOP-16602](https://issues.apache.org/jira/browse/HADOOP-16602) | mvn package fails in hadoop-aws |  Major | documentation | Xieming Li | Xieming Li |
| [HDFS-14874](https://issues.apache.org/jira/browse/HDFS-14874) | Fix TestHDFSCLI and TestDFSShell test break because of logging change in mkdir |  Major | hdfs | Gabor Bota | Gabor Bota |
| [HDFS-14873](https://issues.apache.org/jira/browse/HDFS-14873) | Fix dfsadmin doc for triggerBlockReport |  Major | documentation | Fei Hui | Fei Hui |
| [HDFS-14849](https://issues.apache.org/jira/browse/HDFS-14849) | Erasure Coding: the internal block is replicated many times when datanode is decommissioning |  Major | ec, erasure-coding | HuangTao | HuangTao |
| [HDFS-14876](https://issues.apache.org/jira/browse/HDFS-14876) | Remove unused imports from TestBlockMissingException.java and TestClose.java |  Minor | test | Lisheng Sun | Lisheng Sun |
| [YARN-9858](https://issues.apache.org/jira/browse/YARN-9858) | Optimize RMContext getExclusiveEnforcedPartitions |  Major | . | Jonathan Hung | Jonathan Hung |
| [HDFS-14492](https://issues.apache.org/jira/browse/HDFS-14492) | Snapshot memory leak |  Major | snapshots | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-14418](https://issues.apache.org/jira/browse/HDFS-14418) | Remove redundant super user priveledge checks from namenode. |  Major | . | Ayush Saxena | Ayush Saxena |
| [YARN-9862](https://issues.apache.org/jira/browse/YARN-9862) | yarn-services-core test timeout |  Major | . | Prabhu Joseph | Prabhu Joseph |
| [HDFS-14885](https://issues.apache.org/jira/browse/HDFS-14885) | UI: Fix a typo on WebUI of DataNode. |  Trivial | datanode, ui | Xieming Li | Xieming Li |
| [HADOOP-16619](https://issues.apache.org/jira/browse/HADOOP-16619) | Upgrade jackson and jackson-databind to 2.9.10 |  Major | . | Siyao Meng | Siyao Meng |
| [HDFS-14216](https://issues.apache.org/jira/browse/HDFS-14216) | NullPointerException happens in NamenodeWebHdfs |  Critical | . | lujie | lujie |
| [HADOOP-16605](https://issues.apache.org/jira/browse/HADOOP-16605) | NPE in TestAdlSdkConfiguration failing in yetus |  Major | fs/adl | Steve Loughran | Sneha Vijayarajan |
| [HDFS-14881](https://issues.apache.org/jira/browse/HDFS-14881) | Safemode 'forceExit' option, doesn’t shown in help message |  Major | . | Renukaprasad C | Renukaprasad C |
| [HDFS-14637](https://issues.apache.org/jira/browse/HDFS-14637) | Namenode may not replicate blocks to meet the policy after enabling upgradeDomain |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-14879](https://issues.apache.org/jira/browse/HDFS-14879) | Header was wrong in Snapshot web UI |  Major | . | hemanthboyina | hemanthboyina |
| [HDFS-14655](https://issues.apache.org/jira/browse/HDFS-14655) | [SBN Read] Namenode crashes if one of The JN is down |  Critical | . | Harshakiran Reddy | Ayush Saxena |
| [HDFS-14859](https://issues.apache.org/jira/browse/HDFS-14859) | Prevent unnecessary evaluation of costly operation getNumLiveDataNodes when dfs.namenode.safemode.min.datanodes is not zero |  Major | hdfs | Srinivasu Majeti | Srinivasu Majeti |
| [YARN-6715](https://issues.apache.org/jira/browse/YARN-6715) | Fix documentation about NodeHealthScriptRunner |  Major | documentation, nodemanager | Peter Bacsko | Peter Bacsko |
| [YARN-9552](https://issues.apache.org/jira/browse/YARN-9552) | FairScheduler: NODE\_UPDATE can cause NoSuchElementException |  Major | fairscheduler | Peter Bacsko | Peter Bacsko |
| [HDFS-14754](https://issues.apache.org/jira/browse/HDFS-14754) | Erasure Coding :  The number of Under-Replicated Blocks never reduced |  Critical | ec | hemanthboyina | hemanthboyina |
| [HDFS-14900](https://issues.apache.org/jira/browse/HDFS-14900) |  Fix build failure of hadoop-hdfs-native-client |  Major | . | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-14245](https://issues.apache.org/jira/browse/HDFS-14245) | Class cast error in GetGroups with ObserverReadProxyProvider |  Major | . | Shen Yinjie | Erik Krogen |
| [HDFS-14373](https://issues.apache.org/jira/browse/HDFS-14373) | EC : Decoding is failing when block group last incomplete cell fall in to AlignedStripe |  Critical | ec, hdfs-client | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-14509](https://issues.apache.org/jira/browse/HDFS-14509) | DN throws InvalidToken due to inequality of password when upgrade NN 2.x to 3.x |  Blocker | . | Yuxuan Wang | Yuxuan Wang |
| [HADOOP-13907](https://issues.apache.org/jira/browse/HADOOP-13907) | Fix TestWebDelegationToken#testKerberosDelegationTokenAuthenticator on Windows |  Major | security | Xiaoyu Yao | Kitti Nanasi |
| [HDFS-14886](https://issues.apache.org/jira/browse/HDFS-14886) | In NameNode Web UI's Startup Progress page, Loading edits always shows 0 sec |  Major | . | hemanthboyina | hemanthboyina |
| [YARN-8453](https://issues.apache.org/jira/browse/YARN-8453) | Additional Unit  tests to verify queue limit and max-limit with multiple resource types |  Major | capacity scheduler | Sunil G | Adam Antal |
| [HDFS-14890](https://issues.apache.org/jira/browse/HDFS-14890) | Setting permissions on name directory fails on non posix compliant filesystems |  Blocker | . | hirik | Siddharth Wagle |
| [HADOOP-15169](https://issues.apache.org/jira/browse/HADOOP-15169) | "hadoop.ssl.enabled.protocols" should be considered in httpserver2 |  Major | security | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-16580](https://issues.apache.org/jira/browse/HADOOP-16580) | Disable retry of FailoverOnNetworkExceptionRetry in case of AccessControlException |  Major | common | Adam Antal | Adam Antal |
| [HDFS-14739](https://issues.apache.org/jira/browse/HDFS-14739) | RBF: LS command for mount point shows wrong owner and permission information. |  Major | . | xuzq | Jinglun |
| [HDFS-14909](https://issues.apache.org/jira/browse/HDFS-14909) | DFSNetworkTopology#chooseRandomWithStorageType() should not decrease storage count for excluded node which is already part of excluded scope |  Major | namenode | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HADOOP-16662](https://issues.apache.org/jira/browse/HADOOP-16662) | Remove unnecessary InnerNode check in NetworkTopology#add() |  Minor | . | Lisheng Sun | Lisheng Sun |
| [HDFS-14887](https://issues.apache.org/jira/browse/HDFS-14887) | RBF: In Router Web UI, Observer Namenode Information displaying as Unavailable |  Major | . | hemanthboyina | hemanthboyina |
| [HDFS-14847](https://issues.apache.org/jira/browse/HDFS-14847) | Erasure Coding: Blocks are over-replicated while EC decommissioning |  Critical | ec | Fei Hui | Fei Hui |
| [HDFS-14916](https://issues.apache.org/jira/browse/HDFS-14916) | RBF: line breaks are missing from the output of 'hdfs dfsrouteradmin -ls' |  Minor | rbf, ui | Xieming Li | Xieming Li |
| [HDFS-14913](https://issues.apache.org/jira/browse/HDFS-14913) | Correct the value of available count in DFSNetworkTopology#chooseRandomWithStorageType() |  Major | . | Ayush Saxena | Ayush Saxena |
| [YARN-9915](https://issues.apache.org/jira/browse/YARN-9915) | Fix FindBug issue in QueueMetrics |  Minor | . | Prabhu Joseph | Prabhu Joseph |
| [HDFS-12749](https://issues.apache.org/jira/browse/HDFS-12749) | DN may not send block report to NN after NN restart |  Major | datanode | TanYuxin | Xiaoqiao He |
| [HDFS-13901](https://issues.apache.org/jira/browse/HDFS-13901) | INode access time is ignored because of race between open and rename |  Major | . | Jinglun | Jinglun |
| [HADOOP-16658](https://issues.apache.org/jira/browse/HADOOP-16658) | S3A connector does not support including the token renewer in the token identifier |  Major | fs/s3 | Philip Zampino | Philip Zampino |
| [YARN-9921](https://issues.apache.org/jira/browse/YARN-9921) | Issue in PlacementConstraint when YARN Service AM retries allocation on component failure. |  Major | . | Tarun Parimi | Tarun Parimi |
| [HADOOP-16614](https://issues.apache.org/jira/browse/HADOOP-16614) | Missing leveldbjni package of aarch64 platform |  Major | . | liusheng |  |
| [HDFS-14910](https://issues.apache.org/jira/browse/HDFS-14910) | Rename Snapshot with Pre Descendants Fail With IllegalArgumentException. |  Blocker | . | Íñigo Goiri | Wei-Chiu Chuang |
| [HDFS-14933](https://issues.apache.org/jira/browse/HDFS-14933) | Fixing a typo in documentation of Observer NameNode |  Trivial | documentation | Xieming Li | Xieming Li |
| [HDFS-14308](https://issues.apache.org/jira/browse/HDFS-14308) | DFSStripedInputStream curStripeBuf is not freed by unbuffer() |  Major | ec | Joe McDonnell | Zhao Yi Ming |
| [HDFS-14931](https://issues.apache.org/jira/browse/HDFS-14931) | hdfs crypto commands limit column width |  Major | . | Eric Badger | Eric Badger |
| [HDFS-14730](https://issues.apache.org/jira/browse/HDFS-14730) | Remove unused configuration dfs.web.authentication.filter |  Major | . | Chen Zhang | Chen Zhang |
| [HDFS-14920](https://issues.apache.org/jira/browse/HDFS-14920) | Erasure Coding: Decommission may hang If one or more datanodes are out of service during decommission |  Major | ec | Fei Hui | Fei Hui |
| [HDFS-14768](https://issues.apache.org/jira/browse/HDFS-14768) | EC : Busy DN replica should be consider in live replica check. |  Major | datanode, erasure-coding, hdfs, namenode | guojh | guojh |
| [HDFS-13736](https://issues.apache.org/jira/browse/HDFS-13736) | BlockPlacementPolicyDefault can not choose favored nodes when 'dfs.namenode.block-placement-policy.default.prefer-local-node' set to false |  Major | . | hu xiaodong | hu xiaodong |
| [HDFS-14925](https://issues.apache.org/jira/browse/HDFS-14925) | rename operation should check nest snapshot |  Major | namenode | Junwang Zhao | Junwang Zhao |
| [YARN-9949](https://issues.apache.org/jira/browse/YARN-9949) | Add missing queue configs for root queue in RMWebService#CapacitySchedulerInfo |  Minor | capacity scheduler | Prabhu Joseph | Prabhu Joseph |
| [HDFS-14945](https://issues.apache.org/jira/browse/HDFS-14945) | Revise PacketResponder's log. |  Minor | datanode | Xudong Cao | Xudong Cao |
| [HDFS-14946](https://issues.apache.org/jira/browse/HDFS-14946) | Erasure Coding: Block recovery failed during decommissioning |  Major | . | Fei Hui | Fei Hui |
| [HDFS-14938](https://issues.apache.org/jira/browse/HDFS-14938) | Add check if excludedNodes contain scope in DFSNetworkTopology#chooseRandomWithStorageType() |  Major | . | Lisheng Sun | Lisheng Sun |
| [HADOOP-16681](https://issues.apache.org/jira/browse/HADOOP-16681) | mvn javadoc:javadoc fails in hadoop-aws |  Major | documentation | Xieming Li | Xieming Li |
| [HDFS-14384](https://issues.apache.org/jira/browse/HDFS-14384) | When lastLocatedBlock token expire, it will take 1~3s second to refetch it. |  Major | hdfs-client | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-14880](https://issues.apache.org/jira/browse/HDFS-14880) | Correct the sequence of statistics & exit message in balencer |  Major | balancer & mover | Renukaprasad C | Renukaprasad C |
| [HDFS-14806](https://issues.apache.org/jira/browse/HDFS-14806) | Bootstrap standby may fail if used in-progress tailing |  Major | namenode | Chen Liang | Chen Liang |
| [HDFS-14941](https://issues.apache.org/jira/browse/HDFS-14941) | Potential editlog race condition can cause corrupted file |  Major | namenode | Chen Liang | Chen Liang |
| [HDFS-14958](https://issues.apache.org/jira/browse/HDFS-14958) | TestBalancerWithNodeGroup is not using NetworkTopologyWithNodeGroup |  Minor | hdfs | Jim Brennan | Jim Brennan |
| [HDFS-14720](https://issues.apache.org/jira/browse/HDFS-14720) | DataNode shouldn't report block as bad block if the block length is Long.MAX\_VALUE. |  Major | datanode | Surendra Singh Lilhore | hemanthboyina |
| [HDFS-14962](https://issues.apache.org/jira/browse/HDFS-14962) | RBF: ConnectionPool#newConnection() error log wrong protocol class |  Minor | rbf | Yuxuan Wang | Yuxuan Wang |
| [HADOOP-16701](https://issues.apache.org/jira/browse/HADOOP-16701) | Fix broken links in site index |  Minor | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-16702](https://issues.apache.org/jira/browse/HADOOP-16702) | Move documentation of hadoop-cos to under src directory. |  Major | tools | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-16665](https://issues.apache.org/jira/browse/HADOOP-16665) | Filesystems to be closed if they failed during initialize() |  Major | fs, fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-14922](https://issues.apache.org/jira/browse/HDFS-14922) | Prevent snapshot modification time got change on startup |  Major | . | hemanthboyina | hemanthboyina |
| [HADOOP-16677](https://issues.apache.org/jira/browse/HADOOP-16677) | Recalculate the remaining timeout millis correctly while throwing an InterupptedException in SocketIOWithTimeout. |  Minor | common | Xudong Cao | Xudong Cao |
| [HADOOP-16585](https://issues.apache.org/jira/browse/HADOOP-16585) |  [Tool:NNloadGeneratorMR] Multiple threads are using same id for creating file LoadGenerator#write |  Major | . | Ranith Sardar | Ranith Sardar |
| [HDFS-14884](https://issues.apache.org/jira/browse/HDFS-14884) | Add sanity check that zone key equals feinfo key while setting Xattrs |  Major | encryption, hdfs | Mukul Kumar Singh | Mukul Kumar Singh |
| [HADOOP-15097](https://issues.apache.org/jira/browse/HADOOP-15097) | AbstractContractDeleteTest::testDeleteNonEmptyDirRecursive with misleading path |  Minor | fs, test | zhoutai.zt | Xieming Li |
| [HDFS-14802](https://issues.apache.org/jira/browse/HDFS-14802) | The feature of protect directories should be used in RenameOp |  Major | hdfs | Fei Hui | Fei Hui |
| [YARN-9982](https://issues.apache.org/jira/browse/YARN-9982) | Fix Container API example link in NodeManager REST API doc |  Trivial | documentation | Charan Hebri | Charan Hebri |
| [HDFS-14967](https://issues.apache.org/jira/browse/HDFS-14967) | TestWebHDFS  fails in Windows |  Major | . | Renukaprasad C | Renukaprasad C |
| [YARN-9965](https://issues.apache.org/jira/browse/YARN-9965) | Fix NodeManager failing to start on subsequent times when Hdfs Auxillary Jar is set |  Major | auxservices, nodemanager | Prabhu Joseph | Prabhu Joseph |
| [YARN-9984](https://issues.apache.org/jira/browse/YARN-9984) | FSPreemptionThread can cause NullPointerException while app is unregistered with containers running on a node |  Major | fairscheduler | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [YARN-9986](https://issues.apache.org/jira/browse/YARN-9986) | signalToContainer REST API does not work even if requested by the app owner |  Major | restapi | kyungwan nam | kyungwan nam |
| [HDFS-14992](https://issues.apache.org/jira/browse/HDFS-14992) | TestOfflineEditsViewer is failing in Trunk |  Major | . | hemanthboyina | hemanthboyina |
| [YARN-9983](https://issues.apache.org/jira/browse/YARN-9983) | Typo in YARN Service overview documentation |  Trivial | documentation | Denes Gerencser | Denes Gerencser |
| [HADOOP-16719](https://issues.apache.org/jira/browse/HADOOP-16719) | Remove the disallowed element config within maven-checkstyle-plugin |  Major | . | Wanqiang Ji | Wanqiang Ji |
| [HADOOP-16700](https://issues.apache.org/jira/browse/HADOOP-16700) | RpcQueueTime may be negative when the response has to be sent later |  Minor | . | xuzq | xuzq |
| [HADOOP-15686](https://issues.apache.org/jira/browse/HADOOP-15686) | Supress bogus AbstractWadlGeneratorGrammarGenerator in KMS stderr |  Major | kms | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-16276](https://issues.apache.org/jira/browse/HADOOP-16276) | Fix jsvc startup command in hadoop-functions.sh due to jsvc \>= 1.0.11 changed default current working directory |  Major | scripts | Siyao Meng | Siyao Meng |
| [HDFS-14996](https://issues.apache.org/jira/browse/HDFS-14996) | RBF: GetFileStatus fails for directory with EC policy set in case of multiple destinations |  Major | ec, rbf | Ayush Saxena | Ayush Saxena |
| [HDFS-14940](https://issues.apache.org/jira/browse/HDFS-14940) | HDFS Balancer : Do not allow to set balancer maximum network bandwidth more than 1TB |  Minor | balancer & mover | Souryakanta Dwivedy | hemanthboyina |
| [HDFS-14924](https://issues.apache.org/jira/browse/HDFS-14924) | RenameSnapshot not updating new modification time |  Major | . | hemanthboyina | hemanthboyina |
| [HDFS-13842](https://issues.apache.org/jira/browse/HDFS-13842) | RBF: Exceptions are conflicting when creating the same mount entry twice |  Major | federation | Soumyapn | Ranith Sardar |
| [YARN-9838](https://issues.apache.org/jira/browse/YARN-9838) | Fix resource inconsistency for queues when moving app with reserved container to another queue |  Critical | capacity scheduler | jiulongzhu | jiulongzhu |
| [YARN-9968](https://issues.apache.org/jira/browse/YARN-9968) | Public Localizer is exiting in NodeManager due to NullPointerException |  Major | nodemanager | Tarun Parimi | Tarun Parimi |
| [YARN-9011](https://issues.apache.org/jira/browse/YARN-9011) | Race condition during decommissioning |  Major | nodemanager | Peter Bacsko | Peter Bacsko |
| [HDFS-14973](https://issues.apache.org/jira/browse/HDFS-14973) | Balancer getBlocks RPC dispersal does not function properly |  Major | balancer & mover | Erik Krogen | Erik Krogen |
| [HADOOP-16685](https://issues.apache.org/jira/browse/HADOOP-16685) | FileSystem#listStatusIterator does not check if given path exists |  Major | fs | Sahil Takiar | Sahil Takiar |
| [YARN-9290](https://issues.apache.org/jira/browse/YARN-9290) | Invalid SchedulingRequest not rejected in Scheduler PlacementConstraintsHandler |  Major | . | Prabhu Joseph | Prabhu Joseph |
| [MAPREDUCE-7240](https://issues.apache.org/jira/browse/MAPREDUCE-7240) | Exception ' Invalid event: TA\_TOO\_MANY\_FETCH\_FAILURE at SUCCESS\_FINISHING\_CONTAINER' cause job error |  Critical | . | luhuachao | luhuachao |
| [HDFS-14986](https://issues.apache.org/jira/browse/HDFS-14986) | ReplicaCachingGetSpaceUsed throws  ConcurrentModificationException |  Major | datanode, performance | Ryan Wu | Aiphago |
| [MAPREDUCE-7249](https://issues.apache.org/jira/browse/MAPREDUCE-7249) | Invalid event TA\_TOO\_MANY\_FETCH\_FAILURE at SUCCESS\_CONTAINER\_CLEANUP causes job failure |  Critical | applicationmaster, mrv2 | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [HDFS-14961](https://issues.apache.org/jira/browse/HDFS-14961) | [SBN read] Prevent ZKFC changing Observer Namenode state |  Major | . | Íñigo Goiri | Ayush Saxena |
| [HDFS-15010](https://issues.apache.org/jira/browse/HDFS-15010) | BlockPoolSlice#addReplicaThreadPool static pool should be initialized by static method |  Major | datanode | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [YARN-9990](https://issues.apache.org/jira/browse/YARN-9990) | Testcase fails with "Insufficient configured threads: required=16 \< max=10" |  Major | . | Prabhu Joseph | Prabhu Joseph |
| [HDFS-15009](https://issues.apache.org/jira/browse/HDFS-15009) | FSCK "-list-corruptfileblocks" return Invalid Entries |  Major | . | hemanthboyina | hemanthboyina |
| [HDFS-9695](https://issues.apache.org/jira/browse/HDFS-9695) | HTTPFS - CHECKACCESS operation missing |  Major | . | Bert Hekman | hemanthboyina |
| [HDFS-15026](https://issues.apache.org/jira/browse/HDFS-15026) | TestPendingReconstruction#testPendingReconstruction() fail in trunk |  Minor | . | Lisheng Sun | Lisheng Sun |
| [HADOOP-16441](https://issues.apache.org/jira/browse/HADOOP-16441) |  if use -Dbundle.openssl=true, bundled with unnecessary libk5crypto.\* |  Major | build | KWON BYUNGCHANG | KWON BYUNGCHANG |
| [YARN-9969](https://issues.apache.org/jira/browse/YARN-9969) | Improve yarn.scheduler.capacity.queue-mappings documentation |  Major | . | Manikandan R | Manikandan R |
| [YARN-9938](https://issues.apache.org/jira/browse/YARN-9938) | Validate Parent Queue for QueueMapping contains dynamic group as parent queue |  Major | . | Manikandan R | Manikandan R |
| [HADOOP-16744](https://issues.apache.org/jira/browse/HADOOP-16744) |  Fix building instruction to enable zstd |  Minor | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-10006](https://issues.apache.org/jira/browse/YARN-10006) | IOException used in place of YARNException in CapacitySheduler |  Minor | capacity scheduler | Prabhu Joseph | Adam Antal |
| [HDFS-14751](https://issues.apache.org/jira/browse/HDFS-14751) | Synchronize on diffs in DirectoryScanner |  Minor | . | Lisheng Sun | Lisheng Sun |
| [YARN-9985](https://issues.apache.org/jira/browse/YARN-9985) | Unsupported "transitionToObserver" option displaying for rmadmin command |  Minor | RM, yarn | Souryakanta Dwivedy | Ayush Saxena |
| [HDFS-15040](https://issues.apache.org/jira/browse/HDFS-15040) | RBF: Secured Router should not run when SecretManager is not running |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-7721](https://issues.apache.org/jira/browse/YARN-7721) | TestContinuousScheduling fails sporadically with NPE |  Major | fairscheduler | Jason Darrell Lowe | Wilfred Spiegelenburg |
| [HDFS-15045](https://issues.apache.org/jira/browse/HDFS-15045) | DataStreamer#createBlockOutputStream() should log exception in warn. |  Major | dfsclient | Surendra Singh Lilhore | Ravuri Sushma sree |
| [HADOOP-16754](https://issues.apache.org/jira/browse/HADOOP-16754) | Fix docker failed to build yetus/hadoop |  Blocker | build | Kevin Su | Kevin Su |
| [HDFS-15032](https://issues.apache.org/jira/browse/HDFS-15032) | Balancer crashes when it fails to contact an unavailable NN via ObserverReadProxyProvider |  Major | balancer & mover | Erik Krogen | Erik Krogen |
| [HDFS-15036](https://issues.apache.org/jira/browse/HDFS-15036) | Active NameNode should not silently fail the image transfer |  Major | namenode | Konstantin Shvachko | Chen Liang |
| [HADOOP-16755](https://issues.apache.org/jira/browse/HADOOP-16755) | Fix broken link in single node cluster setup documentation |  Trivial | documentation | Denes Gerencser | Denes Gerencser |
| [HDFS-15048](https://issues.apache.org/jira/browse/HDFS-15048) | Fix findbug in DirectoryScanner |  Major | . | Takanobu Asanuma | Masatake Iwasaki |
| [HADOOP-16765](https://issues.apache.org/jira/browse/HADOOP-16765) | Fix curator dependencies for gradle projects using hadoop-minicluster |  Major | . | Mate Szalay-Beko | Mate Szalay-Beko |
| [HDFS-14908](https://issues.apache.org/jira/browse/HDFS-14908) | LeaseManager should check parent-child relationship when filter open files. |  Minor | . | Jinglun | Jinglun |
| [HDFS-14519](https://issues.apache.org/jira/browse/HDFS-14519) | NameQuota is not update after concat operation, so namequota is wrong |  Major | . | Ranith Sardar | Ranith Sardar |
| [YARN-10020](https://issues.apache.org/jira/browse/YARN-10020) | Fix build instruction of hadoop-yarn-ui |  Minor | yarn-ui-v2 | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-15012](https://issues.apache.org/jira/browse/HDFS-15012) | NN fails to parse Edit logs after applying HDFS-13101 |  Blocker | nn | Eric Lin | Shashikant Banerjee |
| [YARN-10037](https://issues.apache.org/jira/browse/YARN-10037) | Upgrade build tools for YARN Web UI v2 |  Major | build, security, yarn-ui-v2 | Akira Ajisaka | Masatake Iwasaki |
| [YARN-10042](https://issues.apache.org/jira/browse/YARN-10042) | Uupgrade grpc-xxx depdencies to 1.26.0 |  Major | . | liusheng | liusheng |
| [HADOOP-16774](https://issues.apache.org/jira/browse/HADOOP-16774) | TestDiskChecker and TestReadWriteDiskValidator fails when run with -Pparallel-tests |  Major | . | Vinayakumar B | Vinayakumar B |
| [YARN-10054](https://issues.apache.org/jira/browse/YARN-10054) | Upgrade yarn to the latest version in Dockerfile |  Major | build, yarn-ui-v2 | Akira Ajisaka | Akira Ajisaka |
| [YARN-10055](https://issues.apache.org/jira/browse/YARN-10055) | bower install fails |  Blocker | build, yarn-ui-v2 | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15076](https://issues.apache.org/jira/browse/HDFS-15076) | Fix tests that hold FSDirectory lock, without holding FSNamesystem lock. |  Major | test | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-15073](https://issues.apache.org/jira/browse/HDFS-15073) | Replace curator-shaded guava import with the standard one |  Minor | hdfs-client | Akira Ajisaka | Chandra Sanivarapu |
| [YARN-10057](https://issues.apache.org/jira/browse/YARN-10057) | Upgrade the dependencies managed by yarnpkg |  Major | build, yarn-ui-v2 | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15054](https://issues.apache.org/jira/browse/HDFS-15054) | Delete Snapshot not updating new modification time |  Major | . | hemanthboyina | hemanthboyina |
| [HADOOP-16042](https://issues.apache.org/jira/browse/HADOOP-16042) | Update the link to HadoopJavaVersion |  Minor | documentation | Akira Ajisaka | Chandra Sanivarapu |
| [YARN-10041](https://issues.apache.org/jira/browse/YARN-10041) | Should not use AbstractPath to create unix domain socket |  Major | test | zhao bo | liusheng |
| [HDFS-14934](https://issues.apache.org/jira/browse/HDFS-14934) | [SBN Read] Standby NN throws many InterruptedExceptions when dfs.ha.tail-edits.period is 0 |  Major | . | Takanobu Asanuma | Ayush Saxena |
| [HDFS-15063](https://issues.apache.org/jira/browse/HDFS-15063) | HttpFS : getFileStatus doesn't return ecPolicy |  Major | . | hemanthboyina | hemanthboyina |
| [YARN-10053](https://issues.apache.org/jira/browse/YARN-10053) | Placement rules do not use correct group service init |  Major | yarn | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [HDFS-15068](https://issues.apache.org/jira/browse/HDFS-15068) | DataNode could meet deadlock if invoke refreshVolumes when register |  Major | datanode | Xiaoqiao He | Aiphago |
| [HDFS-15089](https://issues.apache.org/jira/browse/HDFS-15089) | RBF: SmallFix for RBFMetrics in doc |  Trivial | documentation, rbf | luhuachao | luhuachao |
| [MAPREDUCE-7255](https://issues.apache.org/jira/browse/MAPREDUCE-7255) | Fix typo in MapReduce documentaion example |  Trivial | documentation | Sergey Pogorelov | Sergey Pogorelov |
| [YARN-9956](https://issues.apache.org/jira/browse/YARN-9956) | Improve connection error message for YARN ApiServerClient |  Major | . | Eric Yang | Prabhu Joseph |
| [HADOOP-16773](https://issues.apache.org/jira/browse/HADOOP-16773) | Fix duplicate assertj-core dependency in hadoop-common module. |  Minor | build | Akira Ajisaka | Xieming Li |
| [HDFS-15072](https://issues.apache.org/jira/browse/HDFS-15072) | HDFS MiniCluster fails to start when run in directory path with a % |  Minor | . | Geoffrey Jacoby | Masatake Iwasaki |
| [HDFS-15077](https://issues.apache.org/jira/browse/HDFS-15077) | Fix intermittent failure of TestDFSClientRetries#testLeaseRenewSocketTimeout |  Minor | test | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-15080](https://issues.apache.org/jira/browse/HDFS-15080) | Fix the issue in reading persistent memory cached data with an offset |  Major | caching, datanode | Feilong He | Feilong He |
| [YARN-7387](https://issues.apache.org/jira/browse/YARN-7387) | org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestIncreaseAllocationExpirer fails intermittently |  Major | . | Miklos Szegedi | Jim Brennan |
| [YARN-8672](https://issues.apache.org/jira/browse/YARN-8672) | TestContainerManager#testLocalingResourceWhileContainerRunning occasionally times out |  Major | nodemanager | Jason Darrell Lowe | Chandni Singh |
| [HDFS-14957](https://issues.apache.org/jira/browse/HDFS-14957) | INodeReference Space Consumed was not same in QuotaUsage and ContentSummary |  Major | namenode | hemanthboyina | hemanthboyina |
| [MAPREDUCE-7252](https://issues.apache.org/jira/browse/MAPREDUCE-7252) | Handling 0 progress in SimpleExponential task runtime estimator |  Minor | . | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15102](https://issues.apache.org/jira/browse/HDFS-15102) | HttpFS: put requests are not supported for path "/" |  Major | . | hemanthboyina | hemanthboyina |
| [HDFS-15110](https://issues.apache.org/jira/browse/HDFS-15110) | HttpFS: POST requests are not supported for path "/" |  Major | . | hemanthboyina | hemanthboyina |
| [HADOOP-16749](https://issues.apache.org/jira/browse/HADOOP-16749) | Configuration parsing of CDATA values are blank |  Major | conf | Jonathan Turner Eagles | Daryn Sharp |
| [HDFS-15095](https://issues.apache.org/jira/browse/HDFS-15095) | Fix accidental comment in flaky test TestDecommissioningStatus |  Major | hdfs | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-16590](https://issues.apache.org/jira/browse/HADOOP-16590) | IBM Java has deprecated OS login module classes and OS principal classes. |  Major | security | Nicholas Marion |  |
| [YARN-10019](https://issues.apache.org/jira/browse/YARN-10019) | container-executor: misc improvements in child processes and exec() calls |  Minor | nodemanager | Peter Bacsko | Peter Bacsko |
| [HDFS-15099](https://issues.apache.org/jira/browse/HDFS-15099) | [SBN Read] checkOperation(WRITE) should throw ObserverRetryOnActiveException on ObserverNode |  Major | namenode | Konstantin Shvachko | Chen Liang |
| [HDFS-15108](https://issues.apache.org/jira/browse/HDFS-15108) | RBF: MembershipNamenodeResolver should invalidate cache incase of active namenode update |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-14578](https://issues.apache.org/jira/browse/HDFS-14578) | AvailableSpaceBlockPlacementPolicy always prefers local node |  Major | block placement | Wei-Chiu Chuang | Ayush Saxena |
| [YARN-9866](https://issues.apache.org/jira/browse/YARN-9866) | u:user2:%primary\_group is not working as expected |  Major | . | Manikandan R | Manikandan R |
| [HADOOP-16683](https://issues.apache.org/jira/browse/HADOOP-16683) | Disable retry of FailoverOnNetworkExceptionRetry in case of wrapped AccessControlException |  Major | common | Adam Antal | Adam Antal |
| [MAPREDUCE-7256](https://issues.apache.org/jira/browse/MAPREDUCE-7256) | Fix javadoc error in SimpleExponentialSmoothing |  Minor | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-8373](https://issues.apache.org/jira/browse/YARN-8373) | RM  Received RMFatalEvent of type CRITICAL\_THREAD\_CRASH |  Major | fairscheduler, resourcemanager | Girish Bhat | Wilfred Spiegelenburg |
| [YARN-9512](https://issues.apache.org/jira/browse/YARN-9512) | [JDK11] TestAuxServices#testCustomizedAuxServiceClassPath fails because of ClassCastException. |  Major | test | Siyao Meng | Akira Ajisaka |
| [MAPREDUCE-7247](https://issues.apache.org/jira/browse/MAPREDUCE-7247) | Modify HistoryServerRest.html content,change The job attempt id‘s datatype from string to int |  Major | documentation | zhaoshengjie |  |
| [YARN-10070](https://issues.apache.org/jira/browse/YARN-10070) | NPE if no rule is defined and application-tag-based-placement is enabled |  Major | . | Kinga Marton | Kinga Marton |
| [YARN-9970](https://issues.apache.org/jira/browse/YARN-9970) | Refactor TestUserGroupMappingPlacementRule#verifyQueueMapping |  Major | . | Manikandan R | Manikandan R |
| [YARN-8148](https://issues.apache.org/jira/browse/YARN-8148) | Update decimal values for queue capacities shown on queue status CLI |  Major | client | Prabhu Joseph | Prabhu Joseph |
| [YARN-10081](https://issues.apache.org/jira/browse/YARN-10081) | Exception message from ClientRMProxy#getRMAddress is misleading |  Trivial | yarn | Adam Antal | Ravuri Sushma sree |
| [HADOOP-16808](https://issues.apache.org/jira/browse/HADOOP-16808) | Use forkCount and reuseForks parameters instead of forkMode in the config of maven surefire plugin |  Minor | build | Akira Ajisaka | Xieming Li |
| [HADOOP-16793](https://issues.apache.org/jira/browse/HADOOP-16793) |  Remove WARN log when ipc connection interrupted in Client#handleSaslConnectionFailure() |  Minor | . | Lisheng Sun | Lisheng Sun |
| [HDFS-15126](https://issues.apache.org/jira/browse/HDFS-15126) | TestDatanodeRegistration#testForcedRegistration fails intermittently |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [YARN-9462](https://issues.apache.org/jira/browse/YARN-9462) | TestResourceTrackerService.testNodeRemovalGracefully fails sporadically |  Minor | resourcemanager, test | Prabhu Joseph | Prabhu Joseph |
| [YARN-9790](https://issues.apache.org/jira/browse/YARN-9790) | Failed to set default-application-lifetime if maximum-application-lifetime is less than or equal to zero |  Major | . | kyungwan nam | kyungwan nam |
| [HDFS-15128](https://issues.apache.org/jira/browse/HDFS-15128) | Unit test failing to clean testing data and crashed future Maven test run due to failure in TestDataNodeVolumeFailureToleration |  Critical | hdfs, test | Ctest | Ctest |
| [HDFS-15143](https://issues.apache.org/jira/browse/HDFS-15143) | LocatedStripedBlock returns wrong block type |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-14993](https://issues.apache.org/jira/browse/HDFS-14993) | checkDiskError doesn't work during datanode startup |  Major | datanode | Yang Yun | Yang Yun |
| [HDFS-15145](https://issues.apache.org/jira/browse/HDFS-15145) | HttpFS: getAclStatus() returns permission as null |  Major | . | hemanthboyina | hemanthboyina |
| [HDFS-13179](https://issues.apache.org/jira/browse/HDFS-13179) | TestLazyPersistReplicaRecovery#testDnRestartWithSavedReplicas fails intermittently |  Critical | fs | Gabor Bota | Ahmed Hussein |
| [MAPREDUCE-7259](https://issues.apache.org/jira/browse/MAPREDUCE-7259) | testSpeculateSuccessfulWithUpdateEvents fails Intermittently |  Minor | . | Ahmed Hussein | Ahmed Hussein |
| [YARN-10107](https://issues.apache.org/jira/browse/YARN-10107) | Invoking NMWebServices#getNMResourceInfo tries to execute gpu discovery binary even if auto discovery is turned off |  Major | . | Szilard Nemeth | Szilard Nemeth |
| [MAPREDUCE-7079](https://issues.apache.org/jira/browse/MAPREDUCE-7079) | JobHistory#ServiceStop implementation is incorrect |  Major | . | Jason Darrell Lowe | Ahmed Hussein |
| [HDFS-15118](https://issues.apache.org/jira/browse/HDFS-15118) | [SBN Read] Slow clients when Observer reads are enabled but there are no Observers on the cluster. |  Major | hdfs-client | Konstantin Shvachko | Chen Liang |
| [YARN-9743](https://issues.apache.org/jira/browse/YARN-9743) | [JDK11] TestTimelineWebServices.testContextFactory fails |  Major | timelineservice | Adam Antal | Akira Ajisaka |
| [HDFS-7175](https://issues.apache.org/jira/browse/HDFS-7175) | Client-side SocketTimeoutException during Fsck |  Major | namenode | Carl Steinbach | Stephen O'Donnell |
| [HADOOP-16834](https://issues.apache.org/jira/browse/HADOOP-16834) | Replace com.sun.istack.Nullable with javax.annotation.Nullable in DNS.java |  Minor | build | Akira Ajisaka | Xieming Li |
| [HDFS-15136](https://issues.apache.org/jira/browse/HDFS-15136) | LOG flooding in secure mode when Cookies are not set in request header |  Major | . | Renukaprasad C | Renukaprasad C |
| [HDFS-15115](https://issues.apache.org/jira/browse/HDFS-15115) | Namenode crash caused by NPE in BlockPlacementPolicyDefault when dynamically change logger to debug |  Major | . | wangzhixiang | wangzhixiang |
| [HDFS-15158](https://issues.apache.org/jira/browse/HDFS-15158) | The number of failed volumes mismatch  with volumeFailures of Datanode metrics |  Minor | datanode | Yang Yun | Yang Yun |
| [HADOOP-16851](https://issues.apache.org/jira/browse/HADOOP-16851) | unused import in Configuration class |  Trivial | conf | runzhou wu | Jan Hentschel |
| [HADOOP-16849](https://issues.apache.org/jira/browse/HADOOP-16849) | start-build-env.sh behaves incorrectly when username is numeric only |  Minor | build | Jihyun Cho | Jihyun Cho |
| [HADOOP-16856](https://issues.apache.org/jira/browse/HADOOP-16856) | cmake is missing in the CentOS 8 section of BUILDING.txt |  Minor | build, documentation | Akira Ajisaka | Masatake Iwasaki |
| [HDFS-15161](https://issues.apache.org/jira/browse/HDFS-15161) | When evictableMmapped or evictable size is zero, do not throw NoSuchElementException in ShortCircuitCache#close() |  Major | . | Lisheng Sun | Lisheng Sun |
| [YARN-9521](https://issues.apache.org/jira/browse/YARN-9521) | RM failed to start due to system services |  Major | . | kyungwan nam | kyungwan nam |
| [YARN-10136](https://issues.apache.org/jira/browse/YARN-10136) | [Router] : Application metrics are hardcode as N/A in UI. |  Major | federation, yarn | Sushanta Sen | Bilwa S T |
| [HDFS-15164](https://issues.apache.org/jira/browse/HDFS-15164) | Fix TestDelegationTokensWithHA |  Major | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-16868](https://issues.apache.org/jira/browse/HADOOP-16868) | ipc.Server readAndProcess threw NullPointerException |  Major | rpc-server | Tsz-wo Sze | Tsz-wo Sze |
| [HDFS-15165](https://issues.apache.org/jira/browse/HDFS-15165) | In Du missed calling getAttributesProvider |  Major | . | Bharat Viswanadham | Bharat Viswanadham |
| [YARN-10119](https://issues.apache.org/jira/browse/YARN-10119) | Cannot reset the AM failure count for YARN Service |  Major | . | kyungwan nam | kyungwan nam |
| [HADOOP-16869](https://issues.apache.org/jira/browse/HADOOP-16869) |  Upgrade findbugs-maven-plugin to 3.0.5 to fix mvn findbugs:findbugs failure |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15052](https://issues.apache.org/jira/browse/HDFS-15052) | WebHDFS getTrashRoot leads to OOM due to FileSystem object creation |  Major | webhdfs | Wei-Chiu Chuang | Masatake Iwasaki |
| [YARN-10147](https://issues.apache.org/jira/browse/YARN-10147) | FPGA plugin can't find the localized aocx file |  Major | nodemanager | Peter Bacsko | Peter Bacsko |
| [HDFS-15185](https://issues.apache.org/jira/browse/HDFS-15185) | StartupProgress reports edits segments until the entire startup completes |  Major | namenode | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-15182](https://issues.apache.org/jira/browse/HDFS-15182) | TestBlockManager#testOneOfTwoRacksDecommissioned() fail in trunk |  Minor | . | Lisheng Sun | Lisheng Sun |
| [HDFS-15166](https://issues.apache.org/jira/browse/HDFS-15166) | Remove redundant field fStream in ByteStringLog |  Major | . | Konstantin Shvachko | Xieming Li |
| [HDFS-15187](https://issues.apache.org/jira/browse/HDFS-15187) | CORRUPT replica mismatch between namenodes after failover |  Critical | . | Ayush Saxena | Ayush Saxena |
| [YARN-10143](https://issues.apache.org/jira/browse/YARN-10143) | YARN-10101 broke Yarn logs CLI |  Blocker | yarn | Adam Antal | Adam Antal |
| [HADOOP-16841](https://issues.apache.org/jira/browse/HADOOP-16841) | The description of hadoop.http.authentication.signature.secret.file contains outdated information |  Minor | documentation | Akira Ajisaka | Xieming Li |
| [YARN-8767](https://issues.apache.org/jira/browse/YARN-8767) | TestStreamingStatus fails |  Major | . | Andras Bokor | Andras Bokor |
| [YARN-10156](https://issues.apache.org/jira/browse/YARN-10156) | Fix typo 'complaint' which means quite different in Federation.md |  Minor | documentation, federation | Sungpeo Kook | Sungpeo Kook |
| [YARN-9593](https://issues.apache.org/jira/browse/YARN-9593) | Updating scheduler conf with comma in config value fails |  Major | . | Anthony Hsu | Tanu Ajmera |
| [YARN-10141](https://issues.apache.org/jira/browse/YARN-10141) | Interceptor in FederationInterceptorREST doesnt update on RM switchover |  Major | federation, restapi | D M Murali Krishna Reddy | D M Murali Krishna Reddy |
| [YARN-10152](https://issues.apache.org/jira/browse/YARN-10152) | Fix findbugs warnings in hadoop-yarn-applications-mawo-core module |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15167](https://issues.apache.org/jira/browse/HDFS-15167) | Block Report Interval shouldn't be reset apart from first Block Report |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-15147](https://issues.apache.org/jira/browse/HDFS-15147) | LazyPersistTestCase wait logic is error-prone |  Minor | . | Ahmed Hussein | Ahmed Hussein |
| [HDFS-14668](https://issues.apache.org/jira/browse/HDFS-14668) | Support Fuse with Users from multiple Security Realms |  Critical | fuse-dfs | Sailesh Patel | Istvan Fajth |
| [HDFS-15124](https://issues.apache.org/jira/browse/HDFS-15124) | Crashing bugs in NameNode when using a valid configuration for \`dfs.namenode.audit.loggers\` |  Critical | namenode | Ctest | Ctest |
| [YARN-10155](https://issues.apache.org/jira/browse/YARN-10155) | TestDelegationTokenRenewer.testTokenThreadTimeout fails in trunk |  Major | yarn | Adam Antal | Manikandan R |
| [HDFS-15111](https://issues.apache.org/jira/browse/HDFS-15111) | stopStandbyServices() should log which service state it is transitioning from. |  Major | hdfs, logging | Konstantin Shvachko | Xieming Li |
| [HDFS-15199](https://issues.apache.org/jira/browse/HDFS-15199) | NPE in BlockSender |  Major | . | Ayush Saxena | Ayush Saxena |
| [YARN-9452](https://issues.apache.org/jira/browse/YARN-9452) | Fix TestDistributedShell and TestTimelineAuthFilterForV2 failures |  Major | ATSv2, distributed-shell, test | Prabhu Joseph | Prabhu Joseph |
| [HADOOP-16891](https://issues.apache.org/jira/browse/HADOOP-16891) | Upgrade jackson-databind to 2.9.10.3 |  Blocker | . | Siyao Meng | Siyao Meng |
| [HDFS-15149](https://issues.apache.org/jira/browse/HDFS-15149) | TestDeadNodeDetection test cases time-out |  Major | datanode | Ahmed Hussein | Lisheng Sun |
| [HADOOP-16897](https://issues.apache.org/jira/browse/HADOOP-16897) | Sort fields in ReflectionUtils.java |  Minor | test, util | cpugputpu | cpugputpu |
| [HADOOP-16437](https://issues.apache.org/jira/browse/HADOOP-16437) | Documentation typos: fs.s3a.experimental.fadvise -\> fs.s3a.experimental.input.fadvise |  Major | documentation, fs/s3 | Josh Rosen | Josh Rosen |
| [HDFS-15204](https://issues.apache.org/jira/browse/HDFS-15204) | TestRetryCacheWithHA testRemoveCacheDescriptor fails intermittently |  Major | hdfs | Ahmed Hussein | Ahmed Hussein |
| [HDFS-14977](https://issues.apache.org/jira/browse/HDFS-14977) | Quota Usage and Content summary are not same in Truncate with Snapshot |  Major | . | hemanthboyina | hemanthboyina |
| [YARN-10173](https://issues.apache.org/jira/browse/YARN-10173) | Make pid file generation timeout configurable in case of reacquired container |  Minor | yarn | Adam Antal | Adam Antal |
| [HDFS-15212](https://issues.apache.org/jira/browse/HDFS-15212) | TestEncryptionZones.testVersionAndSuiteNegotiation fails in trunk |  Major | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-16840](https://issues.apache.org/jira/browse/HADOOP-16840) | AliyunOSS: getFileStatus throws FileNotFoundException in versioning bucket |  Major | fs/oss | wujinhu | wujinhu |
| [YARN-9419](https://issues.apache.org/jira/browse/YARN-9419) | Log a warning if GPU isolation is enabled but LinuxContainerExecutor is disabled |  Major | . | Szilard Nemeth | Andras Gyori |
| [YARN-9427](https://issues.apache.org/jira/browse/YARN-9427) | TestContainerSchedulerQueuing.testKillOnlyRequiredOpportunisticContainers fails sporadically |  Major | scheduler, test | Prabhu Joseph | Ahmed Hussein |
| [HDFS-15135](https://issues.apache.org/jira/browse/HDFS-15135) | EC : ArrayIndexOutOfBoundsException in BlockRecoveryWorker#RecoveryTaskStriped. |  Major | erasure-coding | Surendra Singh Lilhore | Ravuri Sushma sree |
| [HDFS-14442](https://issues.apache.org/jira/browse/HDFS-14442) | Disagreement between HAUtil.getAddressOfActive and RpcInvocationHandler.getConnectionId |  Major | . | Erik Krogen | Ravuri Sushma sree |
| [HDFS-14612](https://issues.apache.org/jira/browse/HDFS-14612) | SlowDiskReport won't update when SlowDisks is always empty in heartbeat |  Major | . | Haibin Huang | Haibin Huang |
| [HDFS-15155](https://issues.apache.org/jira/browse/HDFS-15155) | writeIoRate of DataNodeVolumeMetrics is never used |  Major | hdfs | Haibin Huang | Haibin Huang |
| [HDFS-15216](https://issues.apache.org/jira/browse/HDFS-15216) | Wrong Use Case of -showprogress in fsck |  Major | . | Ravuri Sushma sree | Ravuri Sushma sree |
| [HADOOP-16885](https://issues.apache.org/jira/browse/HADOOP-16885) | Encryption zone file copy failure leaks temp file .\_COPYING\_ and wrapped stream |  Major | . | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-11396](https://issues.apache.org/jira/browse/HDFS-11396) | TestNameNodeMetadataConsistency#testGenerationStampInFuture timed out |  Minor | namenode, test | John Zhuge | Ayush Saxena |
| [YARN-10195](https://issues.apache.org/jira/browse/YARN-10195) | Dependency divergence building Timeline Service on HBase 2.2.0 and above |  Major | build, timelineservice | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-14820](https://issues.apache.org/jira/browse/HDFS-14820) |  The default 8KB buffer of BlockReaderRemote#newBlockReader#BufferedOutputStream is too big |  Major | . | Lisheng Sun | Lisheng Sun |
| [HDFS-15211](https://issues.apache.org/jira/browse/HDFS-15211) | EC: File write hangs during close in case of Exception during updatePipeline |  Critical | . | Ayush Saxena | Ayush Saxena |
| [HDFS-14919](https://issues.apache.org/jira/browse/HDFS-14919) | Provide Non DFS Used per DataNode in DataNode UI |  Major | . | Lisheng Sun | Lisheng Sun |
| [HDFS-15208](https://issues.apache.org/jira/browse/HDFS-15208) | Suppress bogus AbstractWadlGeneratorGrammarGenerator in KMS stderr in hdfs |  Trivial | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-10034](https://issues.apache.org/jira/browse/YARN-10034) | Allocation tags are not removed when node decommission |  Major | . | kyungwan nam | kyungwan nam |
| [HDFS-15223](https://issues.apache.org/jira/browse/HDFS-15223) | FSCK fails if one namenode is not available |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-15200](https://issues.apache.org/jira/browse/HDFS-15200) | Delete Corrupt Replica Immediately Irrespective of Replicas On Stale Storage |  Critical | . | Ayush Saxena | Ayush Saxena |
| [HDFS-15214](https://issues.apache.org/jira/browse/HDFS-15214) | WebHDFS: Add snapshot counts to Content Summary |  Major | . | hemanthboyina | hemanthboyina |
| [HDFS-15227](https://issues.apache.org/jira/browse/HDFS-15227) | NPE if the last block changes from COMMITTED to COMPLETE during FSCK |  Major | . | krishna reddy | Ayush Saxena |
| [YARN-10198](https://issues.apache.org/jira/browse/YARN-10198) | [managedParent].%primary\_group mapping rule doesn't work after YARN-9868 |  Major | capacity scheduler | Peter Bacsko | Peter Bacsko |
| [HDFS-15113](https://issues.apache.org/jira/browse/HDFS-15113) | Missing IBR when NameNode restart if open processCommand async feature |  Blocker | datanode | Xiaoqiao He | Xiaoqiao He |
| [HDFS-15232](https://issues.apache.org/jira/browse/HDFS-15232) | Fix libhdfspp test failures with GCC 7 |  Major | native, test | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15201](https://issues.apache.org/jira/browse/HDFS-15201) | SnapshotCounter hits MaxSnapshotID limit |  Major | snapshots | Karthik Palanisamy | Karthik Palanisamy |
| [HDFS-15219](https://issues.apache.org/jira/browse/HDFS-15219) | DFS Client will stuck when ResponseProcessor.run throw Error |  Major | hdfs-client | zhengchenyu | zhengchenyu |
| [HDFS-15215](https://issues.apache.org/jira/browse/HDFS-15215) | The Timestamp for longest write/read lock held log is wrong |  Major | . | Toshihiro Suzuki | Toshihiro Suzuki |
| [HDFS-15234](https://issues.apache.org/jira/browse/HDFS-15234) | Add a default method body for the INodeAttributeProvider#checkPermissionWithContext API |  Blocker | namenode | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-15193](https://issues.apache.org/jira/browse/HDFS-15193) | Improving the error message for missing \`dfs.namenode.rpc-address.$NAMESERVICE\` |  Minor | hdfs | Ctest | Ctest |
| [YARN-10202](https://issues.apache.org/jira/browse/YARN-10202) | Fix documentation about NodeAttributes. |  Minor | documentation | Sen Zhao | Sen Zhao |
| [MAPREDUCE-7268](https://issues.apache.org/jira/browse/MAPREDUCE-7268) | Fix TestMapreduceConfigFields |  Major | mrv2 | Ayush Saxena | Wanqiang Ji |
| [HADOOP-16949](https://issues.apache.org/jira/browse/HADOOP-16949) | pylint fails in the build environment |  Critical | build | Akira Ajisaka | Akira Ajisaka |
| [MAPREDUCE-7269](https://issues.apache.org/jira/browse/MAPREDUCE-7269) | TestNetworkedJob fails |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-14836](https://issues.apache.org/jira/browse/HADOOP-14836) | Upgrade maven-clean-plugin to 3.1.0 |  Major | build | Allen Wittenauer | Akira Ajisaka |
| [YARN-10207](https://issues.apache.org/jira/browse/YARN-10207) | CLOSE\_WAIT socket connection leaks during rendering of (corrupted) aggregated logs on the JobHistoryServer Web UI |  Major | yarn | Siddharth Ahuja | Siddharth Ahuja |
| [YARN-10226](https://issues.apache.org/jira/browse/YARN-10226) | NPE in Capacity Scheduler while using %primary\_group queue mapping |  Critical | capacity scheduler | Peter Bacsko | Peter Bacsko |
| [HDFS-15269](https://issues.apache.org/jira/browse/HDFS-15269) | NameNode should check the authorization API version only once during initialization |  Blocker | namenode | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-16932](https://issues.apache.org/jira/browse/HADOOP-16932) | distcp copy calls getFileStatus() needlessly and can fail against S3 |  Minor | fs/s3, tools/distcp | Thangamani Murugasamy | Steve Loughran |
| [YARN-2710](https://issues.apache.org/jira/browse/YARN-2710) | RM HA tests failed intermittently on trunk |  Major | client | Wangda Tan | Ahmed Hussein |
| [HDFS-12862](https://issues.apache.org/jira/browse/HDFS-12862) | CacheDirective becomes invalid when NN restart or failover |  Major | caching, hdfs | Wang XL | Wang XL |
| [MAPREDUCE-7272](https://issues.apache.org/jira/browse/MAPREDUCE-7272) | TaskAttemptListenerImpl excessive log messages |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [YARN-10219](https://issues.apache.org/jira/browse/YARN-10219) | YARN service placement constraints is broken |  Blocker | . | Eric Yang | Eric Yang |
| [YARN-10233](https://issues.apache.org/jira/browse/YARN-10233) | [YARN UI2] No Logs were found in "YARN Daemon Logs" page |  Blocker | yarn-ui-v2 | Akhil PB | Akhil PB |
| [HADOOP-16985](https://issues.apache.org/jira/browse/HADOOP-16985) | Handle release package related issues |  Major | . | Vinayakumar B | Vinayakumar B |
| [HDFS-15283](https://issues.apache.org/jira/browse/HDFS-15283) | Cache pool MAXTTL is not persisted and restored on cluster restart |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-15218](https://issues.apache.org/jira/browse/HDFS-15218) | RBF: MountTableRefresherService failed to refresh other router MountTableEntries in secure mode. |  Major | rbf | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HADOOP-16944](https://issues.apache.org/jira/browse/HADOOP-16944) | Use Yetus 0.12.0 in GitHub PR |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15276](https://issues.apache.org/jira/browse/HDFS-15276) | Concat on INodeRefernce fails with illegal state exception |  Critical | . | hemanthboyina | hemanthboyina |
| [HADOOP-16341](https://issues.apache.org/jira/browse/HADOOP-16341) | ShutDownHookManager: Regressed performance on Hook removals after HADOOP-15679 |  Major | common | Gopal Vijayaraghavan | Gopal Vijayaraghavan |
| [YARN-10223](https://issues.apache.org/jira/browse/YARN-10223) | Duplicate jersey-test-framework-core dependency in yarn-server-common |  Minor | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17007](https://issues.apache.org/jira/browse/HADOOP-17007) | hadoop-cos fails to build |  Major | fs/cos | Wei-Chiu Chuang | Yang Yu |
| [YARN-9848](https://issues.apache.org/jira/browse/YARN-9848) | revert YARN-4946 |  Blocker | log-aggregation, resourcemanager | Steven Rand | Steven Rand |
| [HDFS-15286](https://issues.apache.org/jira/browse/HDFS-15286) | Concat on a same files deleting the file |  Critical | . | hemanthboyina | hemanthboyina |
| [HDFS-15301](https://issues.apache.org/jira/browse/HDFS-15301) | statfs function in hdfs-fuse is not working |  Major | fuse-dfs, libhdfs | Aryan Gupta | Aryan Gupta |
| [HDFS-15334](https://issues.apache.org/jira/browse/HDFS-15334) | INodeAttributeProvider's new API checkPermissionWithContext not getting called in for authorization |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-15323](https://issues.apache.org/jira/browse/HDFS-15323) | StandbyNode fails transition to active due to insufficient transaction tailing |  Major | namenode, qjm | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-15343](https://issues.apache.org/jira/browse/HDFS-15343) | TestConfiguredFailoverProxyProvider is failing |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-15565](https://issues.apache.org/jira/browse/HADOOP-15565) | ViewFileSystem.close doesn't close child filesystems and causes FileSystem objects leak. |  Major | . | Jinglun | Jinglun |
| [YARN-9898](https://issues.apache.org/jira/browse/YARN-9898) | Dependency netty-all-4.1.27.Final doesn't support ARM platform |  Major | . | liusheng | liusheng |
| [YARN-10265](https://issues.apache.org/jira/browse/YARN-10265) | Upgrade Netty-all dependency to latest version 4.1.50 to fix ARM support issue |  Major | . | liusheng | liusheng |
| [YARN-9444](https://issues.apache.org/jira/browse/YARN-9444) | YARN API ResourceUtils's getRequestedResourcesFromConfig doesn't recognize yarn.io/gpu as a valid resource |  Minor | api | Gergely Pollak | Gergely Pollak |
| [HDFS-15038](https://issues.apache.org/jira/browse/HDFS-15038) | TestFsck testFsckListCorruptSnapshotFiles is failing in trunk |  Major | . | hemanthboyina | hemanthboyina |
| [HDFS-15293](https://issues.apache.org/jira/browse/HDFS-15293) | Relax the condition for accepting a fsimage when receiving a checkpoint |  Critical | namenode | Chen Liang | Chen Liang |
| [YARN-10228](https://issues.apache.org/jira/browse/YARN-10228) | Yarn Service fails if am java opts contains ZK authentication file path |  Major | . | Bilwa S T | Bilwa S T |
| [MAPREDUCE-7278](https://issues.apache.org/jira/browse/MAPREDUCE-7278) | Speculative execution behavior is observed even when mapreduce.map.speculative and mapreduce.reduce.speculative are false |  Major | task | Tarun Parimi | Tarun Parimi |
| [YARN-10314](https://issues.apache.org/jira/browse/YARN-10314) | YarnClient throws NoClassDefFoundError for WebSocketException with only shaded client jars |  Blocker | yarn | Vinayakumar B | Vinayakumar B |
| [HDFS-15421](https://issues.apache.org/jira/browse/HDFS-15421) | IBR leak causes standby NN to be stuck in safe mode |  Blocker | namenode | Kihwal Lee | Akira Ajisaka |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-8907](https://issues.apache.org/jira/browse/YARN-8907) | Modify a logging message in TestCapacityScheduler |  Trivial | . | Zhankun Tang | Zhankun Tang |
| [YARN-8904](https://issues.apache.org/jira/browse/YARN-8904) | TestRMDelegationTokens can fail in testRMDTMasterKeyStateOnRollingMasterKey |  Minor | test | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [YARN-8944](https://issues.apache.org/jira/browse/YARN-8944) | TestContainerAllocation.testUserLimitAllocationMultipleContainers failure after YARN-8896 |  Minor | capacity scheduler | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [YARN-9263](https://issues.apache.org/jira/browse/YARN-9263) | TestConfigurationNodeAttributesProvider fails after Mockito updated |  Minor | . | Weiwei Yang | Weiwei Yang |
| [YARN-9315](https://issues.apache.org/jira/browse/YARN-9315) | TestCapacitySchedulerMetrics fails intermittently |  Minor | capacity scheduler | Prabhu Joseph | Prabhu Joseph |
| [YARN-9316](https://issues.apache.org/jira/browse/YARN-9316) | TestPlacementConstraintsUtil#testInterAppConstraintsByAppID fails intermittently |  Minor | capacity scheduler | Prabhu Joseph | Prabhu Joseph |
| [YARN-9324](https://issues.apache.org/jira/browse/YARN-9324) | TestSchedulingRequestContainerAllocation(Async) fails with junit-4.11 |  Major | capacity scheduler | Prabhu Joseph | Prabhu Joseph |
| [YARN-9338](https://issues.apache.org/jira/browse/YARN-9338) | Timeline related testcases are failing |  Major | . | Prabhu Joseph | Abhishek Modi |
| [YARN-9299](https://issues.apache.org/jira/browse/YARN-9299) | TestTimelineReaderWhitelistAuthorizationFilter ignores Http Errors |  Major | . | Prabhu Joseph | Prabhu Joseph |
| [YARN-9404](https://issues.apache.org/jira/browse/YARN-9404) | TestApplicationLifetimeMonitor#testApplicationLifetimeMonitor fails intermittent |  Major | resourcemanager | Prabhu Joseph | Prabhu Joseph |
| [YARN-9405](https://issues.apache.org/jira/browse/YARN-9405) | TestYarnNativeServices#testExpressUpgrade fails intermittent |  Major | yarn-native-services | Prabhu Joseph | Prabhu Joseph |
| [YARN-9325](https://issues.apache.org/jira/browse/YARN-9325) | TestQueueManagementDynamicEditPolicy fails intermittent |  Minor | capacity scheduler | Prabhu Joseph | Prabhu Joseph |
| [HDFS-11950](https://issues.apache.org/jira/browse/HDFS-11950) | Disable libhdfs zerocopy test on Mac |  Minor | libhdfs | John Zhuge | Akira Ajisaka |
| [HDFS-11949](https://issues.apache.org/jira/browse/HDFS-11949) | Add testcase for ensuring that FsShell cann't move file to the target directory that file exists |  Minor | test | legend | legend |
| [YARN-10072](https://issues.apache.org/jira/browse/YARN-10072) | TestCSAllocateCustomResource failures |  Major | yarn | Jim Brennan | Jim Brennan |
| [HDFS-15092](https://issues.apache.org/jira/browse/HDFS-15092) | TestRedudantBlocks#testProcessOverReplicatedAndRedudantBlock sometimes fails |  Minor | test | Fei Hui | Fei Hui |
| [YARN-10161](https://issues.apache.org/jira/browse/YARN-10161) | TestRouterWebServicesREST is corrupting STDOUT |  Minor | yarn | Jim Brennan | Jim Brennan |
| [HADOOP-14206](https://issues.apache.org/jira/browse/HADOOP-14206) | TestSFTPFileSystem#testFileExists failure: Invalid encoding for signature |  Major | fs, test | John Zhuge | Jim Brennan |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-12975](https://issues.apache.org/jira/browse/HDFS-12975) | Changes to the NameNode to support reads from standby |  Major | namenode | Konstantin Shvachko | Chao Sun |
| [HDFS-12977](https://issues.apache.org/jira/browse/HDFS-12977) | Add stateId to RPC headers. |  Major | ipc, namenode | Konstantin Shvachko | Plamen Jeliazkov |
| [HDFS-13331](https://issues.apache.org/jira/browse/HDFS-13331) | Add lastSeenStateId to RpcRequestHeader. |  Major | . | Plamen Jeliazkov | Plamen Jeliazkov |
| [HDFS-11807](https://issues.apache.org/jira/browse/HDFS-11807) | libhdfs++: Get minidfscluster tests running under valgrind |  Major | hdfs-client | James Clampffer | Anatoli Shein |
| [HDFS-13286](https://issues.apache.org/jira/browse/HDFS-13286) | Add haadmin commands to transition between standby and observer |  Major | ha, hdfs, namenode | Chao Sun | Chao Sun |
| [HDFS-13578](https://issues.apache.org/jira/browse/HDFS-13578) | Add ReadOnly annotation to methods in ClientProtocol |  Major | . | Chao Sun | Chao Sun |
| [HDFS-13399](https://issues.apache.org/jira/browse/HDFS-13399) | Make Client field AlignmentContext non-static. |  Major | . | Plamen Jeliazkov | Plamen Jeliazkov |
| [HDFS-13607](https://issues.apache.org/jira/browse/HDFS-13607) | [Edit Tail Fast Path Pt 1] Enhance JournalNode with an in-memory cache of recent edit transactions |  Major | ha, journal-node | Erik Krogen | Erik Krogen |
| [HDFS-13608](https://issues.apache.org/jira/browse/HDFS-13608) | [Edit Tail Fast Path Pt 2] Add ability for JournalNode to serve edits via RPC |  Major | . | Erik Krogen | Erik Krogen |
| [HDFS-13609](https://issues.apache.org/jira/browse/HDFS-13609) | [Edit Tail Fast Path Pt 3] NameNode-side changes to support tailing edits via RPC |  Major | ha, namenode | Erik Krogen | Erik Krogen |
| [HDFS-13706](https://issues.apache.org/jira/browse/HDFS-13706) | ClientGCIContext should be correctly named ClientGSIContext |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-12976](https://issues.apache.org/jira/browse/HDFS-12976) | Introduce ObserverReadProxyProvider |  Major | hdfs-client | Konstantin Shvachko | Chao Sun |
| [HDFS-13665](https://issues.apache.org/jira/browse/HDFS-13665) | Move RPC response serialization into Server.doResponse |  Major | . | Plamen Jeliazkov | Plamen Jeliazkov |
| [HADOOP-15349](https://issues.apache.org/jira/browse/HADOOP-15349) | S3Guard DDB retryBackoff to be more informative on limits exceeded |  Major | fs/s3 | Steve Loughran | Gabor Bota |
| [HDFS-13610](https://issues.apache.org/jira/browse/HDFS-13610) | [Edit Tail Fast Path Pt 4] Cleanup: integration test, documentation, remove unnecessary dummy sync |  Major | ha, journal-node, namenode | Erik Krogen | Erik Krogen |
| [HDFS-13150](https://issues.apache.org/jira/browse/HDFS-13150) | [Edit Tail Fast Path] Allow SbNN to tail in-progress edits from JN via RPC |  Major | ha, hdfs, journal-node, namenode | Erik Krogen | Erik Krogen |
| [HDFS-13688](https://issues.apache.org/jira/browse/HDFS-13688) | Introduce msync API call |  Major | . | Chen Liang | Chen Liang |
| [HDFS-13789](https://issues.apache.org/jira/browse/HDFS-13789) | Reduce logging frequency of QuorumJournalManager#selectInputStreams |  Trivial | namenode, qjm | Erik Krogen | Erik Krogen |
| [HDFS-13767](https://issues.apache.org/jira/browse/HDFS-13767) | Add msync server implementation. |  Major | namenode | Chen Liang | Chen Liang |
| [HDFS-13851](https://issues.apache.org/jira/browse/HDFS-13851) | Remove AlignmentContext from AbstractNNFailoverProxyProvider |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-13782](https://issues.apache.org/jira/browse/HDFS-13782) | ObserverReadProxyProvider should work with IPFailoverProxyProvider |  Major | test | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-13779](https://issues.apache.org/jira/browse/HDFS-13779) | Implement performFailover logic for ObserverReadProxyProvider. |  Major | hdfs-client | Konstantin Shvachko | Erik Krogen |
| [HADOOP-15635](https://issues.apache.org/jira/browse/HADOOP-15635) | s3guard set-capacity command to fail fast if bucket is unguarded |  Minor | fs/s3 | Steve Loughran | Gabor Bota |
| [HADOOP-15750](https://issues.apache.org/jira/browse/HADOOP-15750) | remove obsolete S3A test ITestS3ACredentialsInURL |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-13880](https://issues.apache.org/jira/browse/HDFS-13880) | Add mechanism to allow certain RPC calls to bypass sync |  Major | namenode | Chen Liang | Chen Liang |
| [HDFS-13778](https://issues.apache.org/jira/browse/HDFS-13778) | TestStateAlignmentContextWithHA should use real ObserverReadProxyProvider instead of AlignmentContextProxyProvider. |  Major | test | Konstantin Shvachko | Plamen Jeliazkov |
| [HDFS-13749](https://issues.apache.org/jira/browse/HDFS-13749) | Use getServiceStatus to discover observer namenodes |  Major | . | Chao Sun | Chao Sun |
| [HDFS-13898](https://issues.apache.org/jira/browse/HDFS-13898) | Throw retriable exception for getBlockLocations when ObserverNameNode is in safemode |  Major | . | Chao Sun | Chao Sun |
| [HDFS-13791](https://issues.apache.org/jira/browse/HDFS-13791) | Limit logging frequency of edit tail related statements |  Major | hdfs, qjm | Erik Krogen | Erik Krogen |
| [HADOOP-15767](https://issues.apache.org/jira/browse/HADOOP-15767) | [JDK10] Building native package on JDK10 fails due to missing javah |  Major | native | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-13944](https://issues.apache.org/jira/browse/HDFS-13944) | [JDK10] Fix javadoc errors in hadoop-hdfs-rbf module |  Major | documentation | Akira Ajisaka | Íñigo Goiri |
| [HADOOP-15621](https://issues.apache.org/jira/browse/HADOOP-15621) | S3Guard: Implement time-based (TTL) expiry for Authoritative Directory Listing |  Major | fs/s3 | Aaron Fabbri | Gabor Bota |
| [HDFS-13877](https://issues.apache.org/jira/browse/HDFS-13877) | HttpFS: Implement GETSNAPSHOTDIFF |  Major | httpfs | Siyao Meng | Siyao Meng |
| [HDFS-13961](https://issues.apache.org/jira/browse/HDFS-13961) | TestObserverNode refactoring |  Major | test | Konstantin Shvachko | Konstantin Shvachko |
| [YARN-8763](https://issues.apache.org/jira/browse/YARN-8763) | Add WebSocket logic to the Node Manager web server to establish servlet |  Major | . | Zian Chen | Zian Chen |
| [HADOOP-15775](https://issues.apache.org/jira/browse/HADOOP-15775) | [JDK9] Add missing javax.activation-api dependency |  Critical | test | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-15827](https://issues.apache.org/jira/browse/HADOOP-15827) | NPE in DynamoDBMetadataStore.lambda$listChildren for root + auth S3Guard |  Blocker | fs/s3 | Steve Loughran | Gabor Bota |
| [YARN-7652](https://issues.apache.org/jira/browse/YARN-7652) | Handle AM register requests asynchronously in FederationInterceptor |  Major | amrmproxy, federation | Subramaniam Krishnan | Botong Huang |
| [HADOOP-15825](https://issues.apache.org/jira/browse/HADOOP-15825) | ABFS: Enable some tests for namespace not enabled account using OAuth |  Major | fs/azure | Da Zhou | Da Zhou |
| [HADOOP-15785](https://issues.apache.org/jira/browse/HADOOP-15785) | [JDK10] Javadoc build fails on JDK 10 in hadoop-common |  Major | build, documentation | Takanobu Asanuma | Dinesh Chitlangia |
| [YARN-8777](https://issues.apache.org/jira/browse/YARN-8777) | Container Executor C binary change to execute interactive docker command |  Major | . | Zian Chen | Eric Yang |
| [YARN-5742](https://issues.apache.org/jira/browse/YARN-5742) | Serve aggregated logs of historical apps from timeline service |  Critical | timelineserver | Varun Saxena | Rohith Sharma K S |
| [YARN-8834](https://issues.apache.org/jira/browse/YARN-8834) | Provide Java client for fetching Yarn specific entities from TimelineReader |  Critical | timelinereader | Rohith Sharma K S | Abhishek Modi |
| [YARN-3879](https://issues.apache.org/jira/browse/YARN-3879) | [Storage implementation] Create HDFS backing storage implementation for ATS reads |  Major | timelineserver | Tsuyoshi Ozawa | Abhishek Modi |
| [HDFS-13523](https://issues.apache.org/jira/browse/HDFS-13523) | Support observer nodes in MiniDFSCluster |  Major | namenode, test | Erik Krogen | Konstantin Shvachko |
| [HDFS-13906](https://issues.apache.org/jira/browse/HDFS-13906) | RBF: Add multiple paths for dfsrouteradmin "rm" and "clrquota" commands |  Major | federation | Soumyapn | Ayush Saxena |
| [HADOOP-15826](https://issues.apache.org/jira/browse/HADOOP-15826) | @Retries annotation of putObject() call & uses wrong |  Minor | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-8448](https://issues.apache.org/jira/browse/YARN-8448) | AM HTTPS Support for AM communication with RMWeb proxy |  Major | . | Robert Kanter | Robert Kanter |
| [HDFS-13925](https://issues.apache.org/jira/browse/HDFS-13925) | Unit Test for transitioning between different states |  Major | test | Sherwood Zheng | Sherwood Zheng |
| [YARN-8582](https://issues.apache.org/jira/browse/YARN-8582) | Document YARN support for HTTPS in AM Web server |  Major | docs | Robert Kanter | Robert Kanter |
| [YARN-8449](https://issues.apache.org/jira/browse/YARN-8449) | RM HA for AM web server HTTPS Support |  Major | . | Robert Kanter | Robert Kanter |
| [YARN-8873](https://issues.apache.org/jira/browse/YARN-8873) | [YARN-8811] Add CSI java-based client library |  Major | . | Weiwei Yang | Weiwei Yang |
| [HDFS-14011](https://issues.apache.org/jira/browse/HDFS-14011) | RBF: Add more information to HdfsFileStatus for a mount point |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [HDFS-13566](https://issues.apache.org/jira/browse/HDFS-13566) | Add configurable additional RPC listener to NameNode |  Major | ipc | Chen Liang | Chen Liang |
| [HDFS-13924](https://issues.apache.org/jira/browse/HDFS-13924) | Handle BlockMissingException when reading from observer |  Major | . | Chao Sun | Chao Sun |
| [HADOOP-14775](https://issues.apache.org/jira/browse/HADOOP-14775) | Change junit dependency in parent pom file to junit 5 while maintaining backward compatibility to junit4. |  Major | build | Ajay Kumar | Akira Ajisaka |
| [HADOOP-15823](https://issues.apache.org/jira/browse/HADOOP-15823) | ABFS: Stop requiring client ID and tenant ID for MSI |  Major | . | Sean Mackrory | Da Zhou |
| [HADOOP-15868](https://issues.apache.org/jira/browse/HADOOP-15868) | AliyunOSS: update document for properties of multiple part download, multiple part upload and directory copy |  Major | fs/oss | wujinhu | wujinhu |
| [YARN-8569](https://issues.apache.org/jira/browse/YARN-8569) | Create an interface to provide cluster information to application |  Major | . | Eric Yang | Eric Yang |
| [HDFS-13845](https://issues.apache.org/jira/browse/HDFS-13845) | RBF: The default MountTableResolver should fail resolving multi-destination paths |  Major | federation, hdfs | yanghuafeng | yanghuafeng |
| [YARN-8871](https://issues.apache.org/jira/browse/YARN-8871) | Document behavior of YARN-5742 |  Major | . | Vrushali C | Suma Shivaprasad |
| [YARN-7754](https://issues.apache.org/jira/browse/YARN-7754) | [Atsv2] Update document for running v1 and v2 TS |  Major | . | Rohith Sharma K S | Suma Shivaprasad |
| [HDFS-13942](https://issues.apache.org/jira/browse/HDFS-13942) | [JDK10] Fix javadoc errors in hadoop-hdfs module |  Major | documentation | Akira Ajisaka | Dinesh Chitlangia |
| [HDFS-14033](https://issues.apache.org/jira/browse/HDFS-14033) | [libhdfs++] Disable libhdfs++ build on systems that do not support thread\_local |  Major | . | Anatoli Shein | Anatoli Shein |
| [HDFS-14016](https://issues.apache.org/jira/browse/HDFS-14016) | ObserverReadProxyProvider should enable observer read by default |  Major | . | Chen Liang | Chen Liang |
| [HDFS-12026](https://issues.apache.org/jira/browse/HDFS-12026) | libhdfs++: Fix compilation errors and warnings when compiling with Clang |  Blocker | hdfs-client | Anatoli Shein | Anatoli Shein |
| [HADOOP-15895](https://issues.apache.org/jira/browse/HADOOP-15895) | [JDK9+] Add missing javax.annotation-api dependency to hadoop-yarn-csi |  Major | build | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-14024](https://issues.apache.org/jira/browse/HDFS-14024) | RBF: ProvidedCapacityTotal json exception in NamenodeHeartbeatService |  Major | . | CR Hota | CR Hota |
| [YARN-8893](https://issues.apache.org/jira/browse/YARN-8893) | [AMRMProxy] Fix thread leak in AMRMClientRelayer and UAM client |  Major | amrmproxy, federation | Botong Huang | Botong Huang |
| [HDFS-13834](https://issues.apache.org/jira/browse/HDFS-13834) | RBF: Connection creator thread should catch Throwable |  Critical | . | CR Hota | CR Hota |
| [YARN-8905](https://issues.apache.org/jira/browse/YARN-8905) | [Router] Add JvmMetricsInfo and pause monitor |  Minor | . | Bibin Chundatt | Bilwa S T |
| [HADOOP-15904](https://issues.apache.org/jira/browse/HADOOP-15904) | [JDK 11] Javadoc build failed due to "bad use of '\>'" in hadoop-hdfs |  Major | build | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-15902](https://issues.apache.org/jira/browse/HADOOP-15902) | [JDK 11] Specify the HTML version of Javadoc to 4.01 |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-14047](https://issues.apache.org/jira/browse/HDFS-14047) | [libhdfs++] Fix hdfsGetLastExceptionRootCause bug in test\_libhdfs\_threaded.c |  Major | libhdfs, native | Anatoli Shein | Anatoli Shein |
| [HDFS-12284](https://issues.apache.org/jira/browse/HDFS-12284) | RBF: Support for Kerberos authentication |  Major | security | Zhe Zhang | Sherwood Zheng |
| [YARN-8880](https://issues.apache.org/jira/browse/YARN-8880) | Phase 1 - Add configurations for pluggable plugin framework |  Major | . | Zhankun Tang | Zhankun Tang |
| [YARN-8988](https://issues.apache.org/jira/browse/YARN-8988) | Reduce the verbose log on RM heartbeat path when distributed node-attributes is enabled |  Major | . | Weiwei Yang | Tao Yang |
| [YARN-8902](https://issues.apache.org/jira/browse/YARN-8902) | [CSI] Add volume manager that manages CSI volume lifecycle |  Major | . | Weiwei Yang | Weiwei Yang |
| [YARN-8987](https://issues.apache.org/jira/browse/YARN-8987) | Usability improvements node-attributes CLI |  Critical | . | Weiwei Yang | Bibin Chundatt |
| [YARN-8877](https://issues.apache.org/jira/browse/YARN-8877) | [CSI] Extend service spec to allow setting resource attributes |  Major | . | Weiwei Yang | Weiwei Yang |
| [YARN-8776](https://issues.apache.org/jira/browse/YARN-8776) | Container Executor change to create stdin/stdout pipeline |  Major | . | Zian Chen | Eric Yang |
| [HDFS-13852](https://issues.apache.org/jira/browse/HDFS-13852) | RBF: The DN\_REPORT\_TIME\_OUT and DN\_REPORT\_CACHE\_EXPIRE should be configured in RBFConfigKeys. |  Major | federation, hdfs | yanghuafeng | yanghuafeng |
| [HADOOP-15917](https://issues.apache.org/jira/browse/HADOOP-15917) | AliyunOSS: fix incorrect ReadOps and WriteOps in statistics |  Major | fs/oss | wujinhu | wujinhu |
| [HDFS-14035](https://issues.apache.org/jira/browse/HDFS-14035) | NN status discovery does not leverage delegation token |  Major | . | Chen Liang | Chen Liang |
| [HADOOP-15936](https://issues.apache.org/jira/browse/HADOOP-15936) | [JDK 11] MiniDFSClusterManager & MiniHadoopClusterManager compilation fails due to the usage of '\_' as identifier |  Major | build | Devaraj Kavali | Zsolt Venczel |
| [YARN-8303](https://issues.apache.org/jira/browse/YARN-8303) | YarnClient should contact TimelineReader for application/attempt/container report |  Critical | . | Rohith Sharma K S | Abhishek Modi |
| [HDFS-14017](https://issues.apache.org/jira/browse/HDFS-14017) | ObserverReadProxyProviderWithIPFailover should work with HA configuration |  Major | . | Chen Liang | Chen Liang |
| [YARN-8881](https://issues.apache.org/jira/browse/YARN-8881) | [YARN-8851] Add basic pluggable device plugin framework |  Major | . | Zhankun Tang | Zhankun Tang |
| [YARN-8778](https://issues.apache.org/jira/browse/YARN-8778) | Add Command Line interface to invoke interactive docker shell |  Major | . | Zian Chen | Eric Yang |
| [YARN-8953](https://issues.apache.org/jira/browse/YARN-8953) | [CSI] CSI driver adaptor module support in NodeManager |  Major | . | Weiwei Yang | Weiwei Yang |
| [YARN-8838](https://issues.apache.org/jira/browse/YARN-8838) | Add security check for container user is same as websocket user |  Major | nodemanager | Eric Yang | Eric Yang |
| [HDFS-13776](https://issues.apache.org/jira/browse/HDFS-13776) | RBF: Add Storage policies related ClientProtocol APIs |  Major | . | Dibyendu Karmakar | Dibyendu Karmakar |
| [HDFS-14089](https://issues.apache.org/jira/browse/HDFS-14089) | RBF: Failed to specify server's Kerberos pricipal name in NamenodeHeartbeatService |  Minor | . | Ranith Sardar | Ranith Sardar |
| [HDFS-14067](https://issues.apache.org/jira/browse/HDFS-14067) | Allow manual failover between standby and observer |  Major | . | Chao Sun | Chao Sun |
| [HDFS-14094](https://issues.apache.org/jira/browse/HDFS-14094) | Fix the order of logging arguments in ObserverReadProxyProvider. |  Major | logging | Konstantin Shvachko | Ayush Saxena |
| [YARN-9054](https://issues.apache.org/jira/browse/YARN-9054) | Fix FederationStateStoreFacade#buildGetSubClustersCacheRequest |  Major | federation | Bibin Chundatt | Bibin Chundatt |
| [YARN-9058](https://issues.apache.org/jira/browse/YARN-9058) | [CSI] YARN service fail to launch due to CSI changes |  Blocker | . | Eric Yang | Weiwei Yang |
| [YARN-8986](https://issues.apache.org/jira/browse/YARN-8986) | publish all exposed ports to random ports when using bridge network |  Minor | yarn | dockerzhang | dockerzhang |
| [YARN-9034](https://issues.apache.org/jira/browse/YARN-9034) | ApplicationCLI should have option to take clusterId |  Major | . | Rohith Sharma K S | Rohith Sharma K S |
| [HADOOP-15798](https://issues.apache.org/jira/browse/HADOOP-15798) | LocalMetadataStore put() does not retain isDeleted in parent listing |  Major | fs/s3 | Gabor Bota | Gabor Bota |
| [HADOOP-15370](https://issues.apache.org/jira/browse/HADOOP-15370) | S3A log message on rm s3a://bucket/ not intuitive |  Trivial | fs/s3 | Steve Loughran | Gabor Bota |
| [YARN-8882](https://issues.apache.org/jira/browse/YARN-8882) | [YARN-8851] Add a shared device mapping manager (scheduler) for device plugins |  Major | . | Zhankun Tang | Zhankun Tang |
| [YARN-8989](https://issues.apache.org/jira/browse/YARN-8989) | Move DockerCommandPlugin volume related APIs' invocation from DockerLinuxContainerRuntime#prepareContainer to #launchContainer |  Major | . | Zhankun Tang | Zhankun Tang |
| [HDFS-13713](https://issues.apache.org/jira/browse/HDFS-13713) | Add specification of Multipart Upload API to FS specification, with contract tests |  Blocker | fs, test | Steve Loughran | Ewan Higgs |
| [HADOOP-14927](https://issues.apache.org/jira/browse/HADOOP-14927) | ITestS3GuardTool failures in testDestroyNoBucket() |  Minor | fs/s3 | Aaron Fabbri | Gabor Bota |
| [HDFS-13547](https://issues.apache.org/jira/browse/HDFS-13547) | Add ingress port based sasl resolver |  Major | security | Chen Liang | Chen Liang |
| [HDFS-14120](https://issues.apache.org/jira/browse/HDFS-14120) | ORFPP should also clone DT for the virtual IP |  Major | . | Chen Liang | Chen Liang |
| [HDFS-14085](https://issues.apache.org/jira/browse/HDFS-14085) | RBF: LS command for root shows wrong owner and permission information. |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-14114](https://issues.apache.org/jira/browse/HDFS-14114) | RBF: MIN\_ACTIVE\_RATIO should be configurable |  Major | . | Fei Hui | Fei Hui |
| [YARN-9057](https://issues.apache.org/jira/browse/YARN-9057) | [CSI] CSI jar file should not bundle third party dependencies |  Blocker | build | Eric Yang | Weiwei Yang |
| [YARN-8914](https://issues.apache.org/jira/browse/YARN-8914) | Add xtermjs to YARN UI2 |  Major | yarn-ui-v2 | Eric Yang | Eric Yang |
| [HDFS-14131](https://issues.apache.org/jira/browse/HDFS-14131) | Create user guide for "Consistent reads from Observer" feature. |  Major | documentation | Konstantin Shvachko | Chao Sun |
| [HADOOP-15428](https://issues.apache.org/jira/browse/HADOOP-15428) | s3guard bucket-info will create s3guard table if FS is set to do this automatically |  Major | fs/s3 | Steve Loughran | Gabor Bota |
| [HADOOP-15988](https://issues.apache.org/jira/browse/HADOOP-15988) | Should be able to set empty directory flag to TRUE in DynamoDBMetadataStore#innerGet when using authoritative directory listings |  Major | fs/s3 | Gabor Bota | Gabor Bota |
| [HDFS-14142](https://issues.apache.org/jira/browse/HDFS-14142) | Move ipfailover config key out of HdfsClientConfigKeys |  Minor | . | Chen Liang | Chen Liang |
| [YARN-8885](https://issues.apache.org/jira/browse/YARN-8885) | [DevicePlugin] Support NM APIs to query device resource allocation |  Major | . | Zhankun Tang | Zhankun Tang |
| [YARN-9015](https://issues.apache.org/jira/browse/YARN-9015) | [DevicePlugin] Add an interface for device plugin to provide customized scheduler |  Major | . | Zhankun Tang | Zhankun Tang |
| [YARN-8962](https://issues.apache.org/jira/browse/YARN-8962) | Add ability to use interactive shell with normal yarn container |  Major | . | Eric Yang | Eric Yang |
| [HDFS-13873](https://issues.apache.org/jira/browse/HDFS-13873) | ObserverNode should reject read requests when it is too far behind. |  Major | hdfs-client, namenode | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-14138](https://issues.apache.org/jira/browse/HDFS-14138) | Description errors in the comparison logic of transaction ID |  Minor | . | xiangheng | xiangheng |
| [HDFS-14146](https://issues.apache.org/jira/browse/HDFS-14146) | Handle exception from internalQueueCall |  Critical | ipc | Chao Sun | Chao Sun |
| [YARN-9032](https://issues.apache.org/jira/browse/YARN-9032) | Support sh shell for interactive container shell at command line |  Blocker | yarn | Eric Yang | Eric Yang |
| [YARN-9089](https://issues.apache.org/jira/browse/YARN-9089) | Add Terminal Link to Service component instance page for UI2 |  Major | yarn-ui-v2 | Eric Yang | Eric Yang |
| [YARN-8963](https://issues.apache.org/jira/browse/YARN-8963) | Add flag to disable interactive shell |  Major | . | Eric Yang | Eric Yang |
| [YARN-9091](https://issues.apache.org/jira/browse/YARN-9091) | Improve terminal message when connection is refused |  Major | . | Eric Yang | Eric Yang |
| [HDFS-14152](https://issues.apache.org/jira/browse/HDFS-14152) | RBF: Fix a typo in RouterAdmin usage |  Major | . | Takanobu Asanuma | Ayush Saxena |
| [HDFS-13869](https://issues.apache.org/jira/browse/HDFS-13869) | RBF: Handle NPE for NamenodeBeanMetrics#getFederationMetrics |  Major | namenode | Surendra Singh Lilhore | Ranith Sardar |
| [HDFS-14096](https://issues.apache.org/jira/browse/HDFS-14096) | [SPS] : Add Support for Storage Policy Satisfier in ViewFs |  Major | federation | Ayush Saxena | Ayush Saxena |
| [HADOOP-15969](https://issues.apache.org/jira/browse/HADOOP-15969) | ABFS: getNamespaceEnabled can fail blocking user access thru ACLs |  Major | fs/azure | Da Zhou | Da Zhou |
| [HADOOP-15972](https://issues.apache.org/jira/browse/HADOOP-15972) | ABFS: reduce list page size to to 500 |  Major | fs/azure | Da Zhou | Da Zhou |
| [HADOOP-16004](https://issues.apache.org/jira/browse/HADOOP-16004) | ABFS: Convert 404 error response in AbfsInputStream and AbfsOutPutStream to FileNotFoundException |  Major | fs/azure | Da Zhou | Da Zhou |
| [YARN-9125](https://issues.apache.org/jira/browse/YARN-9125) | Carriage Return character in launch command cause node manager to become unhealthy |  Major | . | Eric Yang | Billie Rinaldi |
| [HDFS-14116](https://issues.apache.org/jira/browse/HDFS-14116) | Fix class cast error in NNThroughputBenchmark with ObserverReadProxyProvider. |  Major | hdfs-client | Chen Liang | Chao Sun |
| [HDFS-14149](https://issues.apache.org/jira/browse/HDFS-14149) | Adjust annotations on new interfaces/classes for SBN reads. |  Major | . | Konstantin Shvachko | Chao Sun |
| [HDFS-14151](https://issues.apache.org/jira/browse/HDFS-14151) | RBF: Make the read-only column of Mount Table clearly understandable |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-9072](https://issues.apache.org/jira/browse/YARN-9072) | Web browser close without proper exit can leak shell process |  Major | . | Eric Yang | Eric Yang |
| [YARN-9117](https://issues.apache.org/jira/browse/YARN-9117) | Container shell does not work when using yarn.nodemanager.linux-container-executor.nonsecure-mode.local-user is set |  Major | . | Eric Yang | Eric Yang |
| [YARN-9075](https://issues.apache.org/jira/browse/YARN-9075) | Dynamically add or remove auxiliary services |  Major | nodemanager | Billie Rinaldi | Billie Rinaldi |
| [HDFS-13443](https://issues.apache.org/jira/browse/HDFS-13443) | RBF: Update mount table cache immediately after changing (add/update/remove) mount table entries. |  Major | fs | Mohammad Arshad | Mohammad Arshad |
| [YARN-9126](https://issues.apache.org/jira/browse/YARN-9126) | Container reinit always fails in branch-3.2 and trunk |  Major | . | Eric Yang | Chandni Singh |
| [HDFS-14160](https://issues.apache.org/jira/browse/HDFS-14160) | ObserverReadInvocationHandler should implement RpcInvocationHandler |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [YARN-9129](https://issues.apache.org/jira/browse/YARN-9129) | Ensure flush after printing to log plus additional cleanup |  Major | . | Billie Rinaldi | Eric Yang |
| [YARN-8523](https://issues.apache.org/jira/browse/YARN-8523) | Interactive docker shell |  Major | . | Eric Yang | Zian Chen |
| [HADOOP-15935](https://issues.apache.org/jira/browse/HADOOP-15935) | [JDK 11] Update maven.plugin-tools.version to 3.6.0 |  Major | build | Devaraj Kavali | Dinesh Chitlangia |
| [HDFS-14154](https://issues.apache.org/jira/browse/HDFS-14154) | Document dfs.ha.tail-edits.period in user guide. |  Major | documentation | Chao Sun | Chao Sun |
| [HADOOP-16015](https://issues.apache.org/jira/browse/HADOOP-16015) | Add bouncycastle jars to hadoop-aws as test dependencies |  Major | fs/s3, test | Steve Loughran | Steve Loughran |
| [YARN-9131](https://issues.apache.org/jira/browse/YARN-9131) | Document usage of Dynamic auxiliary services |  Major | . | Eric Yang | Billie Rinaldi |
| [YARN-9152](https://issues.apache.org/jira/browse/YARN-9152) | Auxiliary service REST API query does not return running services |  Major | . | Eric Yang | Billie Rinaldi |
| [YARN-8925](https://issues.apache.org/jira/browse/YARN-8925) | Updating distributed node attributes only when necessary |  Major | resourcemanager | Tao Yang | Tao Yang |
| [YARN-5168](https://issues.apache.org/jira/browse/YARN-5168) | Add port mapping handling when docker container use bridge network |  Major | . | Jun Gong | Xun Liu |
| [HDFS-14170](https://issues.apache.org/jira/browse/HDFS-14170) | Fix white spaces related to SBN reads. |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-16009](https://issues.apache.org/jira/browse/HADOOP-16009) | Replace the url of the repository in Apache Hadoop source code |  Major | documentation | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-15860](https://issues.apache.org/jira/browse/HADOOP-15860) | ABFS: Throw IllegalArgumentException when Directory/File name ends with a period(.) |  Major | fs/azure | Sean Mackrory | Shweta |
| [HDFS-14167](https://issues.apache.org/jira/browse/HDFS-14167) | RBF: Add stale nodes to federation metrics |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-14161](https://issues.apache.org/jira/browse/HDFS-14161) | RBF: Throw StandbyException instead of IOException so that client can retry when can not get connection |  Major | . | Fei Hui | Fei Hui |
| [HADOOP-15323](https://issues.apache.org/jira/browse/HADOOP-15323) | AliyunOSS: Improve copy file performance for AliyunOSSFileSystemStore |  Major | fs/oss | wujinhu | wujinhu |
| [YARN-9147](https://issues.apache.org/jira/browse/YARN-9147) | Auxiliary manifest file deleted from HDFS does not trigger service to be removed |  Major | . | Eric Yang | Billie Rinaldi |
| [YARN-9038](https://issues.apache.org/jira/browse/YARN-9038) | [CSI] Add ability to publish/unpublish volumes on node managers |  Major | . | Weiwei Yang | Weiwei Yang |
| [YARN-6149](https://issues.apache.org/jira/browse/YARN-6149) | Allow port range to be specified while starting NM Timeline collector manager. |  Major | timelineserver | Varun Saxena | Abhishek Modi |
| [YARN-9166](https://issues.apache.org/jira/browse/YARN-9166) | Fix logging for preemption of Opportunistic containers for Guaranteed containers. |  Minor | . | Abhishek Modi | Abhishek Modi |
| [HADOOP-15937](https://issues.apache.org/jira/browse/HADOOP-15937) | [JDK 11] Update maven-shade-plugin.version to 3.2.1 |  Major | build | Devaraj Kavali | Dinesh Chitlangia |
| [YARN-9169](https://issues.apache.org/jira/browse/YARN-9169) | Add metrics for queued opportunistic and guaranteed containers. |  Major | . | Abhishek Modi | Abhishek Modi |
| [YARN-8822](https://issues.apache.org/jira/browse/YARN-8822) | Nvidia-docker v2 support for YARN GPU feature |  Critical | . | Zhankun Tang | dockerzhang |
| [YARN-9037](https://issues.apache.org/jira/browse/YARN-9037) | [CSI] Ignore volume resource in resource calculators based on tags |  Major | . | Weiwei Yang | Sunil G |
| [HDFS-14150](https://issues.apache.org/jira/browse/HDFS-14150) | RBF: Quotas of the sub-cluster should be removed when removing the mount point |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-14191](https://issues.apache.org/jira/browse/HDFS-14191) | RBF: Remove hard coded router status from FederationMetrics. |  Major | . | Ranith Sardar | Ranith Sardar |
| [HADOOP-16027](https://issues.apache.org/jira/browse/HADOOP-16027) | [DOC] Effective use of FS instances during S3A integration tests |  Major | fs/s3 | Gabor Bota | Gabor Bota |
| [HDFS-13856](https://issues.apache.org/jira/browse/HDFS-13856) | RBF: RouterAdmin should support dfsrouteradmin -refreshRouterArgs command |  Major | federation, hdfs | yanghuafeng | yanghuafeng |
| [HADOOP-16045](https://issues.apache.org/jira/browse/HADOOP-16045) | Don't run TestDU on Windows |  Trivial | common, test | Lukas Majercak | Lukas Majercak |
| [HADOOP-14556](https://issues.apache.org/jira/browse/HADOOP-14556) | S3A to support Delegation Tokens |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-14206](https://issues.apache.org/jira/browse/HDFS-14206) | RBF: Cleanup quota modules |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-14129](https://issues.apache.org/jira/browse/HDFS-14129) | RBF: Create new policy provider for router |  Major | namenode | Surendra Singh Lilhore | Ranith Sardar |
| [HADOOP-15941](https://issues.apache.org/jira/browse/HADOOP-15941) | [JDK 11] Compilation failure: package com.sun.jndi.ldap is not visible |  Major | common | Uma Maheswara Rao G | Takanobu Asanuma |
| [HDFS-14193](https://issues.apache.org/jira/browse/HDFS-14193) | RBF: Inconsistency with the Default Namespace |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-14156](https://issues.apache.org/jira/browse/HDFS-14156) | RBF: rollEdit() command fails with Router |  Major | . | Harshakiran Reddy | Shubham Dewan |
| [HADOOP-16046](https://issues.apache.org/jira/browse/HADOOP-16046) | [JDK 11] Correct the compiler exclusion of org/apache/hadoop/yarn/webapp/hamlet/\*\* classes for \>= Java 9 |  Major | build | Devaraj Kavali | Devaraj Kavali |
| [HADOOP-15787](https://issues.apache.org/jira/browse/HADOOP-15787) | [JDK11] TestIPC.testRTEDuringConnectionSetup fails |  Major | . | Akira Ajisaka | Zsolt Venczel |
| [YARN-9146](https://issues.apache.org/jira/browse/YARN-9146) | REST API to trigger storing auxiliary manifest file and publish to NMs |  Major | . | Eric Yang | Billie Rinaldi |
| [YARN-8101](https://issues.apache.org/jira/browse/YARN-8101) | Add UT to verify node-attributes in RM nodes rest API |  Minor | resourcemanager, restapi | Weiwei Yang | Prabhu Joseph |
| [HDFS-14209](https://issues.apache.org/jira/browse/HDFS-14209) | RBF: setQuota() through router is working for only the mount Points under the Source column in MountTable |  Major | . | Shubham Dewan | Shubham Dewan |
| [YARN-9116](https://issues.apache.org/jira/browse/YARN-9116) | Capacity Scheduler: implements queue level maximum-allocation inheritance |  Major | capacity scheduler | Aihua Xu | Aihua Xu |
| [YARN-8867](https://issues.apache.org/jira/browse/YARN-8867) | Retrieve the status of resource localization |  Major | yarn | Chandni Singh | Chandni Singh |
| [HDFS-14223](https://issues.apache.org/jira/browse/HDFS-14223) | RBF: Add configuration documents for using multiple sub-clusters |  Major | documentation | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-9221](https://issues.apache.org/jira/browse/YARN-9221) | Add a flag to enable dynamic auxiliary service feature |  Major | . | Eric Yang | Billie Rinaldi |
| [HDFS-14224](https://issues.apache.org/jira/browse/HDFS-14224) | RBF: NPE in getContentSummary() for getEcPolicy() in case of multiple destinations |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-14215](https://issues.apache.org/jira/browse/HDFS-14215) | RBF: Remove dependency on availability of default namespace |  Major | . | Ayush Saxena | Ayush Saxena |
| [YARN-9074](https://issues.apache.org/jira/browse/YARN-9074) | Docker container rm command should be executed after stop |  Major | . | Zhaohui Xin | Zhaohui Xin |
| [YARN-9086](https://issues.apache.org/jira/browse/YARN-9086) | [CSI] Run csi-driver-adaptor as aux service |  Major | . | Weiwei Yang | Weiwei Yang |
| [HADOOP-14178](https://issues.apache.org/jira/browse/HADOOP-14178) | Move Mockito up to version 2.23.4 |  Major | test | Steve Loughran | Akira Ajisaka |
| [HADOOP-16041](https://issues.apache.org/jira/browse/HADOOP-16041) | UserAgent string for ABFS |  Major | fs/azure | Shweta | Shweta |
| [HADOOP-15938](https://issues.apache.org/jira/browse/HADOOP-15938) | [JDK 11] Remove animal-sniffer-maven-plugin to fix build |  Major | build | Devaraj Kavali | Dinesh Chitlangia |
| [HDFS-14225](https://issues.apache.org/jira/browse/HDFS-14225) | RBF : MiniRouterDFSCluster should configure the failover proxy provider for namespace |  Minor | federation | Surendra Singh Lilhore | Ranith Sardar |
| [YARN-9275](https://issues.apache.org/jira/browse/YARN-9275) | Add link to NodeAttributes doc in PlacementConstraints document |  Minor | documentation | Weiwei Yang | Masatake Iwasaki |
| [YARN-6735](https://issues.apache.org/jira/browse/YARN-6735) | Have a way to turn off container metrics from NMs |  Major | timelineserver | Vrushali C | Abhishek Modi |
| [HDFS-14252](https://issues.apache.org/jira/browse/HDFS-14252) | RBF : Exceptions are exposing the actual sub cluster path |  Major | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-15954](https://issues.apache.org/jira/browse/HADOOP-15954) | ABFS: Enable owner and group conversion for MSI and login user using OAuth |  Major | fs/azure | junhua gu | Da Zhou |
| [YARN-9253](https://issues.apache.org/jira/browse/YARN-9253) | Add UT to verify Placement Constraint in Distributed Shell |  Major | . | Prabhu Joseph | Prabhu Joseph |
| [YARN-9252](https://issues.apache.org/jira/browse/YARN-9252) | Allocation Tag Namespace support in Distributed Shell |  Major | distributed-shell | Prabhu Joseph | Prabhu Joseph |
| [YARN-8555](https://issues.apache.org/jira/browse/YARN-8555) | Parameterize TestSchedulingRequestContainerAllocation(Async) to cover both PC handler options |  Minor | . | Weiwei Yang | Prabhu Joseph |
| [YARN-996](https://issues.apache.org/jira/browse/YARN-996) | REST API support for node resource configuration |  Major | graceful, nodemanager, scheduler | Junping Du | Íñigo Goiri |
| [YARN-9229](https://issues.apache.org/jira/browse/YARN-9229) | Document docker registry deployment with NFS Gateway |  Major | . | Eric Yang | Eric Yang |
| [HADOOP-15364](https://issues.apache.org/jira/browse/HADOOP-15364) | Add support for S3 Select to S3A |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-14230](https://issues.apache.org/jira/browse/HDFS-14230) | RBF: Throw RetriableException instead of IOException when no namenodes available |  Major | . | Fei Hui | Fei Hui |
| [YARN-9184](https://issues.apache.org/jira/browse/YARN-9184) | Docker run doesn't pull down latest image if the image exists locally |  Major | nodemanager | Zhaohui Xin | Zhaohui Xin |
| [HDFS-13617](https://issues.apache.org/jira/browse/HDFS-13617) | Allow wrapping NN QOP into token in encrypted message |  Major | . | Chen Liang | Chen Liang |
| [HDFS-13358](https://issues.apache.org/jira/browse/HDFS-13358) | RBF: Support for Delegation Token (RPC) |  Major | . | Sherwood Zheng | CR Hota |
| [HDFS-14262](https://issues.apache.org/jira/browse/HDFS-14262) | [SBN read] Unclear Log.WARN message in GlobalStateIdContext |  Major | hdfs | Shweta | Shweta |
| [YARN-9293](https://issues.apache.org/jira/browse/YARN-9293) | Optimize MockAMLauncher event handling |  Major | . | Bibin Chundatt | Bibin Chundatt |
| [HDFS-14226](https://issues.apache.org/jira/browse/HDFS-14226) | RBF: Setting attributes should set on all subclusters' directories. |  Major | . | Takanobu Asanuma | Ayush Saxena |
| [HDFS-14268](https://issues.apache.org/jira/browse/HDFS-14268) | RBF: Fix the location of the DNs in getDatanodeReport() |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-9060](https://issues.apache.org/jira/browse/YARN-9060) | [YARN-8851] Phase 1 - Support device isolation and use the Nvidia GPU plugin as an example |  Major | . | Zhankun Tang | Zhankun Tang |
| [HADOOP-15843](https://issues.apache.org/jira/browse/HADOOP-15843) | s3guard bucket-info command to not print a stack trace on bucket-not-found |  Minor | fs/s3 | Steve Loughran | Adam Antal |
| [HADOOP-16104](https://issues.apache.org/jira/browse/HADOOP-16104) | Wasb tests to downgrade to skip when test a/c is namespace enabled |  Major | fs/azure, test | Steve Loughran | Masatake Iwasaki |
| [HDFS-14249](https://issues.apache.org/jira/browse/HDFS-14249) | RBF: Tooling to identify the subcluster location of a file |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-9258](https://issues.apache.org/jira/browse/YARN-9258) | Support to specify allocation tags without constraint in distributed shell CLI |  Major | distributed-shell | Prabhu Joseph | Prabhu Joseph |
| [YARN-9156](https://issues.apache.org/jira/browse/YARN-9156) | [YARN-8851] Improve debug message in device plugin method compatibility check of ResourcePluginManager |  Trivial | . | Zhankun Tang | Zhankun Tang |
| [YARN-8891](https://issues.apache.org/jira/browse/YARN-8891) | Documentation of the pluggable device framework |  Major | documentation | Zhankun Tang | Zhankun Tang |
| [YARN-9244](https://issues.apache.org/jira/browse/YARN-9244) | Document docker registry deployment with direct S3 driver |  Major | . | Eric Yang | Suma Shivaprasad |
| [YARN-8821](https://issues.apache.org/jira/browse/YARN-8821) | [YARN-8851] GPU hierarchy/topology scheduling support based on pluggable device framework |  Major | . | Zhankun Tang | Zhankun Tang |
| [HDFS-14130](https://issues.apache.org/jira/browse/HDFS-14130) | Make ZKFC ObserverNode aware |  Major | ha | Konstantin Shvachko | xiangheng |
| [YARN-9331](https://issues.apache.org/jira/browse/YARN-9331) | [YARN-8851] Fix a bug that lacking cgroup initialization when bootstrap DeviceResourceHandlerImpl |  Major | . | Zhankun Tang | Zhankun Tang |
| [HADOOP-16093](https://issues.apache.org/jira/browse/HADOOP-16093) | Move DurationInfo from hadoop-aws to hadoop-common org.apache.hadoop.util |  Minor | fs/s3, util | Steve Loughran | Abhishek Modi |
| [YARN-8783](https://issues.apache.org/jira/browse/YARN-8783) | Improve the documentation for the docker.trusted.registries configuration |  Major | . | Simon Prewo | Eric Yang |
| [HADOOP-16136](https://issues.apache.org/jira/browse/HADOOP-16136) | ABFS: Should only transform username to short name |  Major | . | Da Zhou | Da Zhou |
| [YARN-9245](https://issues.apache.org/jira/browse/YARN-9245) | Add support for Docker Images command |  Major | yarn | Chandni Singh | Chandni Singh |
| [YARN-5336](https://issues.apache.org/jira/browse/YARN-5336) | Limit the flow name size & consider cleanup for hex chars |  Major | timelineserver | Vrushali C | Sushil Ks |
| [YARN-3841](https://issues.apache.org/jira/browse/YARN-3841) | [Storage implementation] Adding retry semantics to HDFS backing storage |  Major | timelineserver | Tsuyoshi Ozawa | Abhishek Modi |
| [HADOOP-16068](https://issues.apache.org/jira/browse/HADOOP-16068) | ABFS Authentication and Delegation Token plugins to optionally be bound to specific URI of the store |  Major | fs/azure | Steve Loughran | Steve Loughran |
| [YARN-7904](https://issues.apache.org/jira/browse/YARN-7904) | Privileged, trusted containers should be supported only in ENTRYPOINT mode |  Major | . | Eric Badger | Eric Yang |
| [HDFS-14259](https://issues.apache.org/jira/browse/HDFS-14259) | RBF: Fix safemode message for Router |  Major | . | Íñigo Goiri | Ranith Sardar |
| [HDFS-14329](https://issues.apache.org/jira/browse/HDFS-14329) | RBF: Add maintenance nodes to federation metrics |  Major | . | Ayush Saxena | Ayush Saxena |
| [YARN-7477](https://issues.apache.org/jira/browse/YARN-7477) | Moving logging APIs over to slf4j in hadoop-yarn-common |  Major | . | Yeliang Cang | Prabhu Joseph |
| [HDFS-14331](https://issues.apache.org/jira/browse/HDFS-14331) | RBF: IOE While Removing Mount Entry |  Major | rbf | Surendra Singh Lilhore | Ayush Saxena |
| [HDFS-14335](https://issues.apache.org/jira/browse/HDFS-14335) | RBF: Fix heartbeat typos in the Router. |  Trivial | . | CR Hota | CR Hota |
| [YARN-7243](https://issues.apache.org/jira/browse/YARN-7243) | Moving logging APIs over to slf4j in hadoop-yarn-server-resourcemanager |  Major | . | Yeliang Cang | Prabhu Joseph |
| [HDFS-7663](https://issues.apache.org/jira/browse/HDFS-7663) | Erasure Coding: Append on striped file |  Major | . | Jing Zhao | Ayush Saxena |
| [HADOOP-16163](https://issues.apache.org/jira/browse/HADOOP-16163) | NPE in setup/teardown of ITestAbfsDelegationTokens |  Major | fs/azure, test | Steve Loughran | Steve Loughran |
| [HDFS-14334](https://issues.apache.org/jira/browse/HDFS-14334) | RBF: Use human readable format for long numbers in the Router UI |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-9239](https://issues.apache.org/jira/browse/YARN-9239) | Document docker registry deployment with Ozone CSI driver |  Major | . | Eric Yang | Eric Yang |
| [YARN-8549](https://issues.apache.org/jira/browse/YARN-8549) | Adding a NoOp timeline writer and reader plugin classes for ATSv2 |  Minor | ATSv2, timelineclient, timelineserver | Prabha Manepalli | Prabha Manepalli |
| [YARN-9265](https://issues.apache.org/jira/browse/YARN-9265) | FPGA plugin fails to recognize Intel Processing Accelerator Card |  Critical | . | Peter Bacsko | Peter Bacsko |
| [YARN-8643](https://issues.apache.org/jira/browse/YARN-8643) | Docker image life cycle management on HDFS |  Major | yarn | Eric Yang | Eric Yang |
| [HDFS-14343](https://issues.apache.org/jira/browse/HDFS-14343) | RBF: Fix renaming folders spread across multiple subclusters |  Major | . | Íñigo Goiri | Ayush Saxena |
| [YARN-8805](https://issues.apache.org/jira/browse/YARN-8805) | Automatically convert the launch command to the exec form when using entrypoint support |  Major | . | Shane Kumpf | Eric Yang |
| [HDFS-14270](https://issues.apache.org/jira/browse/HDFS-14270) | [SBN Read] StateId and TrasactionId not present in Trace level logging |  Trivial | namenode | Shweta | Shweta |
| [YARN-9266](https://issues.apache.org/jira/browse/YARN-9266) | General improvements in IntelFpgaOpenclPlugin |  Major | . | Peter Bacsko | Peter Bacsko |
| [HADOOP-16109](https://issues.apache.org/jira/browse/HADOOP-16109) | Parquet reading S3AFileSystem causes EOF |  Blocker | fs/s3 | Dave Christianson | Steve Loughran |
| [YARN-8376](https://issues.apache.org/jira/browse/YARN-8376) | Separate white list for docker.trusted.registries and docker.privileged-container.registries |  Major | . | Eric Yang | Eric Yang |
| [HADOOP-15625](https://issues.apache.org/jira/browse/HADOOP-15625) | S3A input stream to use etags/version number to detect changed source files |  Major | fs/s3 | Brahma Reddy Battula | Ben Roling |
| [HDFS-14354](https://issues.apache.org/jira/browse/HDFS-14354) | Refactor MappableBlock to align with the implementation of SCM cache |  Major | caching, datanode | Feilong He | Feilong He |
| [YARN-9343](https://issues.apache.org/jira/browse/YARN-9343) | Replace isDebugEnabled with SLF4J parameterized log messages |  Major | . | Prabhu Joseph | Prabhu Joseph |
| [HADOOP-16182](https://issues.apache.org/jira/browse/HADOOP-16182) | Update abfs storage back-end with "close" flag when application is done writing to a file |  Major | fs/azure | Vishwajeet Dusane | Vishwajeet Dusane |
| [YARN-9363](https://issues.apache.org/jira/browse/YARN-9363) | Replace isDebugEnabled with SLF4J parameterized log messages for remaining code |  Minor | yarn | Prabhu Joseph | Prabhu Joseph |
| [HADOOP-16124](https://issues.apache.org/jira/browse/HADOOP-16124) | Extend documentation in testing.md about endpoint constants |  Trivial | fs/s3 | Adam Antal | Adam Antal |
| [YARN-9364](https://issues.apache.org/jira/browse/YARN-9364) | Remove commons-logging dependency from remaining hadoop-yarn |  Major | . | Prabhu Joseph | Prabhu Joseph |
| [HADOOP-16191](https://issues.apache.org/jira/browse/HADOOP-16191) | AliyunOSS: improvements for copyFile/copyDirectory and logging |  Major | fs/oss | wujinhu | wujinhu |
| [HDFS-14351](https://issues.apache.org/jira/browse/HDFS-14351) | RBF: Optimize configuration item resolving for monitor namenode |  Major | rbf | Xiaoqiao He | Xiaoqiao He |
| [YARN-9387](https://issues.apache.org/jira/browse/YARN-9387) | Update document for ATS HBase Custom tablenames (-entityTableName) |  Critical | ATSv2 | Prabhu Joseph | Prabhu Joseph |
| [YARN-9389](https://issues.apache.org/jira/browse/YARN-9389) | FlowActivity and FlowRun table prefix is wrong |  Minor | ATSv2 | Prabhu Joseph | Prabhu Joseph |
| [YARN-9398](https://issues.apache.org/jira/browse/YARN-9398) | Javadoc error on FPGA related java files |  Major | . | Eric Yang | Peter Bacsko |
| [YARN-9267](https://issues.apache.org/jira/browse/YARN-9267) | General improvements in FpgaResourceHandlerImpl |  Major | . | Peter Bacsko | Peter Bacsko |
| [YARN-9402](https://issues.apache.org/jira/browse/YARN-9402) | Opportunistic containers should not be scheduled on Decommissioning nodes. |  Major | . | Abhishek Modi | Abhishek Modi |
| [HADOOP-16201](https://issues.apache.org/jira/browse/HADOOP-16201) | S3AFileSystem#innerMkdirs builds needless lists |  Trivial | fs/s3 | Lokesh Jain | Lokesh Jain |
| [HDFS-14388](https://issues.apache.org/jira/browse/HDFS-14388) | RBF: Prevent loading metric system when disabled |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-9391](https://issues.apache.org/jira/browse/YARN-9391) | Disable PATH variable to be passed to Docker container |  Major | . | Eric Yang | Jim Brennan |
| [YARN-9268](https://issues.apache.org/jira/browse/YARN-9268) | General improvements in FpgaDevice |  Major | . | Peter Bacsko | Peter Bacsko |
| [YARN-9269](https://issues.apache.org/jira/browse/YARN-9269) | Minor cleanup in FpgaResourceAllocator |  Minor | . | Peter Bacsko | Peter Bacsko |
| [HADOOP-16186](https://issues.apache.org/jira/browse/HADOOP-16186) | S3Guard: NPE in DynamoDBMetadataStore.lambda$listChildren |  Major | fs/s3 | Steve Loughran | Gabor Bota |
| [HADOOP-15999](https://issues.apache.org/jira/browse/HADOOP-15999) | S3Guard: Better support for out-of-band operations |  Major | fs/s3 | Sean Mackrory | Gabor Bota |
| [HDFS-14393](https://issues.apache.org/jira/browse/HDFS-14393) | Refactor FsDatasetCache for SCM cache implementation |  Major | . | Rakesh Radhakrishnan | Rakesh Radhakrishnan |
| [HADOOP-16058](https://issues.apache.org/jira/browse/HADOOP-16058) | S3A tests to include Terasort |  Major | fs/s3, test | Steve Loughran | Steve Loughran |
| [YARN-9270](https://issues.apache.org/jira/browse/YARN-9270) | Minor cleanup in TestFpgaDiscoverer |  Minor | . | Peter Bacsko | Peter Bacsko |
| [YARN-7129](https://issues.apache.org/jira/browse/YARN-7129) | Application Catalog initial project setup and source |  Major | applications | Eric Yang | Eric Yang |
| [YARN-9348](https://issues.apache.org/jira/browse/YARN-9348) | Build issues on hadoop-yarn-application-catalog-webapp |  Major | . | Eric Yang | Eric Yang |
| [HDFS-14316](https://issues.apache.org/jira/browse/HDFS-14316) | RBF: Support unavailable subclusters for mount points with multiple destinations |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-14355](https://issues.apache.org/jira/browse/HDFS-14355) | Implement HDFS cache on SCM by using pure java mapped byte buffer |  Major | caching, datanode | Feilong He | Feilong He |
| [HADOOP-16220](https://issues.apache.org/jira/browse/HADOOP-16220) | Add findbugs ignores for unjustified issues during update to guava to 27.0-jre in hadoop-project |  Major | . | Gabor Bota | Gabor Bota |
| [YARN-9255](https://issues.apache.org/jira/browse/YARN-9255) | Improve recommend applications order |  Major | . | Eric Yang | Eric Yang |
| [YARN-9418](https://issues.apache.org/jira/browse/YARN-9418) | ATSV2 /apps/appId/entities/YARN\_CONTAINER rest api does not show metrics |  Critical | ATSv2 | Prabhu Joseph | Prabhu Joseph |
| [HADOOP-16218](https://issues.apache.org/jira/browse/HADOOP-16218) | findbugs warning of null param to non-nullable method in Configuration with Guava update |  Minor | build | Steve Loughran | Steve Loughran |
| [YARN-9303](https://issues.apache.org/jira/browse/YARN-9303) | Username splits won't help timelineservice.app\_flow table |  Major | ATSv2 | Prabhu Joseph | Prabhu Joseph |
| [HADOOP-16210](https://issues.apache.org/jira/browse/HADOOP-16210) | Update guava to 27.0-jre in hadoop-project trunk |  Critical | build | Gabor Bota | Gabor Bota |
| [YARN-9441](https://issues.apache.org/jira/browse/YARN-9441) | Component name should start with Apache Hadoop for consistency |  Minor | . | Weiwei Yang | Weiwei Yang |
| [HADOOP-16197](https://issues.apache.org/jira/browse/HADOOP-16197) | S3AUtils.translateException to map CredentialInitializationException to AccessDeniedException |  Major | . | Steve Loughran | Steve Loughran |
| [HDFS-13853](https://issues.apache.org/jira/browse/HDFS-13853) | RBF: RouterAdmin update cmd is overwriting the entry not updating the existing |  Major | . | Dibyendu Karmakar | Ayush Saxena |
| [YARN-9382](https://issues.apache.org/jira/browse/YARN-9382) | Publish container killed, paused and resumed events to ATSv2. |  Major | . | Abhishek Modi | Abhishek Modi |
| [YARN-9335](https://issues.apache.org/jira/browse/YARN-9335) | [atsv2] Restrict the number of elements held in timeline collector when backend is unreachable for async calls |  Major | . | Vrushali C | Abhishek Modi |
| [YARN-9313](https://issues.apache.org/jira/browse/YARN-9313) | Support asynchronized scheduling mode and multi-node lookup mechanism for scheduler activities |  Major | . | Tao Yang | Tao Yang |
| [HDFS-14369](https://issues.apache.org/jira/browse/HDFS-14369) | RBF: Fix trailing "/" for webhdfs |  Major | . | CR Hota | Akira Ajisaka |
| [YARN-999](https://issues.apache.org/jira/browse/YARN-999) | In case of long running tasks, reduce node resource should balloon out resource quickly by calling preemption API and suspending running task. |  Major | graceful, nodemanager, scheduler | Junping Du | Íñigo Goiri |
| [YARN-9435](https://issues.apache.org/jira/browse/YARN-9435) | Add Opportunistic Scheduler metrics in ResourceManager. |  Major | . | Abhishek Modi | Abhishek Modi |
| [HADOOP-16195](https://issues.apache.org/jira/browse/HADOOP-16195) | S3A MarshalledCredentials.toString() doesn't print full date/time of expiry |  Minor | fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-13699](https://issues.apache.org/jira/browse/HDFS-13699) | Add DFSClient sending handshake token to DataNode, and allow DataNode overwrite downstream QOP |  Major | . | Chen Liang | Chen Liang |
| [HADOOP-14747](https://issues.apache.org/jira/browse/HADOOP-14747) | S3AInputStream to implement CanUnbuffer |  Major | fs/s3 | Steve Loughran | Sahil Takiar |
| [HADOOP-16237](https://issues.apache.org/jira/browse/HADOOP-16237) | Fix new findbugs issues after update guava to 27.0-jre in hadoop-project trunk |  Critical | . | Gabor Bota | Gabor Bota |
| [YARN-9281](https://issues.apache.org/jira/browse/YARN-9281) | Add express upgrade button to Appcatalog UI |  Major | . | Eric Yang | Eric Yang |
| [YARN-9474](https://issues.apache.org/jira/browse/YARN-9474) | Remove hard coded sleep from Opportunistic Scheduler tests. |  Major | . | Abhishek Modi | Abhishek Modi |
| [YARN-9439](https://issues.apache.org/jira/browse/YARN-9439) | Support asynchronized scheduling mode and multi-node lookup mechanism for app activities |  Major | capacityscheduler | Tao Yang | Tao Yang |
| [YARN-7848](https://issues.apache.org/jira/browse/YARN-7848) | Force removal of docker containers that do not get removed on first try |  Major | . | Eric Badger | Eric Yang |
| [YARN-8943](https://issues.apache.org/jira/browse/YARN-8943) | Upgrade JUnit from 4 to 5 in hadoop-yarn-api |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-16253](https://issues.apache.org/jira/browse/HADOOP-16253) | Update AssertJ to 3.12.2 |  Major | test | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-14422](https://issues.apache.org/jira/browse/HDFS-14422) | RBF: Router shouldn't allow READ operations in safe mode |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-8530](https://issues.apache.org/jira/browse/YARN-8530) | Add security filters to Application catalog |  Major | security, yarn-native-services | Eric Yang | Eric Yang |
| [YARN-9466](https://issues.apache.org/jira/browse/YARN-9466) | App catalog navigation stylesheet does not display correctly in Safari |  Major | . | Eric Yang | Eric Yang |
| [HADOOP-16508](https://issues.apache.org/jira/browse/HADOOP-16508) | [hadoop-yarn-project] Fix order of actual and expected expression in assert statements |  Major | . | Prabhu Joseph | Prabhu Joseph |
| [YARN-9448](https://issues.apache.org/jira/browse/YARN-9448) | Fix Opportunistic Scheduling for node local allocations. |  Major | . | Abhishek Modi | Abhishek Modi |
| [YARN-9254](https://issues.apache.org/jira/browse/YARN-9254) | Externalize Solr data storage |  Major | . | Eric Yang | Eric Yang |
| [YARN-2889](https://issues.apache.org/jira/browse/YARN-2889) | Limit the number of opportunistic container allocated per AM heartbeat |  Major | nodemanager, resourcemanager | Konstantinos Karanasos | Abhishek Modi |
| [YARN-8551](https://issues.apache.org/jira/browse/YARN-8551) | Build Common module for MaWo application |  Major | . | Yesha Vora | Yesha Vora |
| [YARN-9475](https://issues.apache.org/jira/browse/YARN-9475) | Create basic VE plugin |  Major | nodemanager | Peter Bacsko | Peter Bacsko |
| [HADOOP-16252](https://issues.apache.org/jira/browse/HADOOP-16252) | Use configurable dynamo table name prefix in S3Guard tests |  Major | fs/s3 | Ben Roling | Ben Roling |
| [HDFS-13972](https://issues.apache.org/jira/browse/HDFS-13972) | RBF: Support for Delegation Token (WebHDFS) |  Major | . | Íñigo Goiri | CR Hota |
| [HADOOP-16222](https://issues.apache.org/jira/browse/HADOOP-16222) | Fix new deprecations after guava 27.0 update in trunk |  Major | . | Gabor Bota | Gabor Bota |
| [HDFS-14457](https://issues.apache.org/jira/browse/HDFS-14457) | RBF: Add order text SPACE in CLI command 'hdfs dfsrouteradmin' |  Major | rbf | luhuachao | luhuachao |
| [YARN-9486](https://issues.apache.org/jira/browse/YARN-9486) | Docker container exited with failure does not get clean up correctly |  Major | . | Eric Yang | Eric Yang |
| [HADOOP-16242](https://issues.apache.org/jira/browse/HADOOP-16242) | ABFS: add bufferpool to AbfsOutputStream |  Major | fs/azure | Da Zhou | Da Zhou |
| [YARN-9476](https://issues.apache.org/jira/browse/YARN-9476) | Create unit tests for VE plugin |  Major | . | Peter Bacsko | Peter Bacsko |
| [HADOOP-16221](https://issues.apache.org/jira/browse/HADOOP-16221) | S3Guard: fail write that doesn't update metadata store |  Major | fs/s3 | Ben Roling | Ben Roling |
| [HDFS-14454](https://issues.apache.org/jira/browse/HDFS-14454) | RBF: getContentSummary() should allow non-existing folders |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-9440](https://issues.apache.org/jira/browse/YARN-9440) | Improve diagnostics for scheduler and app activities |  Major | capacityscheduler | Tao Yang | Tao Yang |
| [HADOOP-16270](https://issues.apache.org/jira/browse/HADOOP-16270) | [JDK 11] Remove unintentional override of the version of Maven Dependency Plugin |  Major | build | Akira Ajisaka | Xieming Li |
| [HDFS-14401](https://issues.apache.org/jira/browse/HDFS-14401) | Refine the implementation for HDFS cache on SCM |  Major | caching, datanode | Feilong He | Feilong He |
| [HADOOP-16269](https://issues.apache.org/jira/browse/HADOOP-16269) | ABFS: add listFileStatus with StartFrom |  Major | fs/azure | Da Zhou | Da Zhou |
| [YARN-9489](https://issues.apache.org/jira/browse/YARN-9489) | Support filtering by request-priorities and allocation-request-ids for query results of app activities |  Major | . | Tao Yang | Tao Yang |
| [YARN-9539](https://issues.apache.org/jira/browse/YARN-9539) | Improve cleanup process of app activities and make some conditions configurable |  Major | capacityscheduler | Tao Yang | Tao Yang |
| [HDFS-14426](https://issues.apache.org/jira/browse/HDFS-14426) | RBF: Add delegation token total count as one of the federation metrics |  Major | . | Fengnan Li | Fengnan Li |
| [HADOOP-16306](https://issues.apache.org/jira/browse/HADOOP-16306) | AliyunOSS: Remove temporary files when upload small files to OSS |  Major | fs/oss | wujinhu | wujinhu |
| [HDFS-14210](https://issues.apache.org/jira/browse/HDFS-14210) | RBF: ACL commands should work over all the destinations |  Major | . | Shubham Dewan | Ayush Saxena |
| [HDFS-14490](https://issues.apache.org/jira/browse/HDFS-14490) | RBF: Remove unnecessary quota checks |  Major | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-16050](https://issues.apache.org/jira/browse/HADOOP-16050) | S3A SSL connections should use OpenSSL |  Major | fs/s3 | Justin Uang | Sahil Takiar |
| [HDFS-14447](https://issues.apache.org/jira/browse/HDFS-14447) | RBF: Router should support RefreshUserMappingsProtocol |  Major | rbf | Shen Yinjie | Shen Yinjie |
| [YARN-9505](https://issues.apache.org/jira/browse/YARN-9505) | Add container allocation latency for Opportunistic Scheduler |  Major | . | Abhishek Modi | Abhishek Modi |
| [HADOOP-16085](https://issues.apache.org/jira/browse/HADOOP-16085) | S3Guard: use object version or etags to protect against inconsistent read after replace/overwrite |  Major | fs/s3 | Ben Roling | Ben Roling |
| [HDFS-13995](https://issues.apache.org/jira/browse/HDFS-13995) | RBF: Security documentation |  Major | . | CR Hota | CR Hota |
| [HADOOP-16287](https://issues.apache.org/jira/browse/HADOOP-16287) | KerberosAuthenticationHandler Trusted Proxy Support for Knox |  Major | auth | Prabhu Joseph | Prabhu Joseph |
| [HDFS-14440](https://issues.apache.org/jira/browse/HDFS-14440) | RBF: Optimize the file write process in case of multiple destinations. |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-14402](https://issues.apache.org/jira/browse/HDFS-14402) | Use FileChannel.transferTo() method for transferring block to SCM cache |  Major | caching, datanode | Feilong He | Feilong He |
| [YARN-9497](https://issues.apache.org/jira/browse/YARN-9497) | Support grouping by diagnostics for query results of scheduler and app activities |  Major | . | Tao Yang | Tao Yang |
| [HDFS-13255](https://issues.apache.org/jira/browse/HDFS-13255) | RBF: Fail when try to remove mount point paths |  Major | . | Wu Weiwei | Akira Ajisaka |
| [HADOOP-16332](https://issues.apache.org/jira/browse/HADOOP-16332) | Remove S3A's depedency on http core |  Critical | build, fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-13909](https://issues.apache.org/jira/browse/HDFS-13909) | RBF: Add Cache pools and directives related ClientProtocol APIs |  Major | . | Dibyendu Karmakar | Ayush Saxena |
| [YARN-8693](https://issues.apache.org/jira/browse/YARN-8693) | Add signalToContainer REST API for RMWebServices |  Major | restapi | Tao Yang | Tao Yang |
| [HDFS-14516](https://issues.apache.org/jira/browse/HDFS-14516) | RBF: Create hdfs-rbf-site.xml for RBF specific properties |  Major | rbf | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-13787](https://issues.apache.org/jira/browse/HDFS-13787) | RBF: Add Snapshot related ClientProtocol APIs |  Major | federation | Ranith Sardar | Íñigo Goiri |
| [HADOOP-13656](https://issues.apache.org/jira/browse/HADOOP-13656) | fs -expunge to take a filesystem |  Minor | fs | Steve Loughran | Shweta |
| [HDFS-14475](https://issues.apache.org/jira/browse/HDFS-14475) | RBF: Expose router security enabled status on the UI |  Major | . | CR Hota | CR Hota |
| [HDFS-13480](https://issues.apache.org/jira/browse/HDFS-13480) | RBF: Separate namenodeHeartbeat and routerHeartbeat to different config key |  Major | . | maobaolong | Ayush Saxena |
| [HADOOP-16118](https://issues.apache.org/jira/browse/HADOOP-16118) | S3Guard to support on-demand DDB tables |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-14508](https://issues.apache.org/jira/browse/HDFS-14508) | RBF: Clean-up and refactor UI components |  Minor | . | CR Hota | Takanobu Asanuma |
| [YARN-7537](https://issues.apache.org/jira/browse/YARN-7537) | [Atsv2] load hbase configuration from filesystem rather than URL |  Major | . | Rohith Sharma K S | Prabhu Joseph |
| [MAPREDUCE-7210](https://issues.apache.org/jira/browse/MAPREDUCE-7210) | Replace \`mapreduce.job.counters.limit\` with \`mapreduce.job.counters.max\` in mapred-default.xml |  Major | . | Wanqiang Ji | Wanqiang Ji |
| [HDFS-14526](https://issues.apache.org/jira/browse/HDFS-14526) | RBF: Update the document of RBF related metrics |  Major | documentation | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-13404](https://issues.apache.org/jira/browse/HDFS-13404) | RBF: TestRouterWebHDFSContractAppend.testRenameFileBeingAppended fails |  Major | test | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-16314](https://issues.apache.org/jira/browse/HADOOP-16314) | Make sure all end point URL is covered by the same AuthenticationFilter |  Major | security | Eric Yang | Prabhu Joseph |
| [HDFS-14356](https://issues.apache.org/jira/browse/HDFS-14356) | Implement HDFS cache on SCM with native PMDK libs |  Major | caching, datanode | Feilong He | Feilong He |
| [HADOOP-16117](https://issues.apache.org/jira/browse/HADOOP-16117) | Update AWS SDK to 1.11.563 |  Major | build, fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-9590](https://issues.apache.org/jira/browse/YARN-9590) | Correct incompatible, incomplete and redundant activities |  Major | . | Tao Yang | Tao Yang |
| [MAPREDUCE-6794](https://issues.apache.org/jira/browse/MAPREDUCE-6794) | Remove unused properties from TTConfig.java |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [HDFS-14550](https://issues.apache.org/jira/browse/HDFS-14550) | RBF: Failed to get statistics from NameNodes before 2.9.0 |  Major | . | Akira Ajisaka | Xiaoqiao He |
| [HADOOP-15563](https://issues.apache.org/jira/browse/HADOOP-15563) | S3Guard to support creating on-demand DDB tables |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-14553](https://issues.apache.org/jira/browse/HDFS-14553) | Make queue size of BlockReportProcessingThread configurable |  Major | namenode | Xiaoqiao He | Xiaoqiao He |
| [MAPREDUCE-7214](https://issues.apache.org/jira/browse/MAPREDUCE-7214) | Remove unused pieces related to \`mapreduce.job.userlog.retain.hours\` |  Major | . | Wanqiang Ji | Wanqiang Ji |
| [HADOOP-16354](https://issues.apache.org/jira/browse/HADOOP-16354) | Enable AuthFilter as default for WebHdfs |  Major | security | Prabhu Joseph | Prabhu Joseph |
| [HADOOP-16367](https://issues.apache.org/jira/browse/HADOOP-16367) | ApplicationHistoryServer related testcases failing |  Major | security, test | Prabhu Joseph | Prabhu Joseph |
| [YARN-9578](https://issues.apache.org/jira/browse/YARN-9578) | Add limit/actions/summarize options for app activities REST API |  Major | capacityscheduler | Tao Yang | Tao Yang |
| [HADOOP-16366](https://issues.apache.org/jira/browse/HADOOP-16366) | Fix TimelineReaderServer ignores ProxyUserAuthenticationFilterInitializer |  Major | security | Prabhu Joseph | Prabhu Joseph |
| [HDFS-14545](https://issues.apache.org/jira/browse/HDFS-14545) | RBF: Router should support GetUserMappingsProtocol |  Major | . | Íñigo Goiri | Ayush Saxena |
| [YARN-8499](https://issues.apache.org/jira/browse/YARN-8499) | ATS v2 Generic TimelineStorageMonitor |  Major | ATSv2 | Sunil G | Prabhu Joseph |
| [HADOOP-16279](https://issues.apache.org/jira/browse/HADOOP-16279) | S3Guard: Implement time-based (TTL) expiry for entries (and tombstones) |  Major | fs/s3 | Gabor Bota | Gabor Bota |
| [HADOOP-16376](https://issues.apache.org/jira/browse/HADOOP-16376) | ABFS: Override access() to no-op for now |  Major | fs/azure | Da Zhou | Da Zhou |
| [YARN-9574](https://issues.apache.org/jira/browse/YARN-9574) | ArtifactId of MaWo application is wrong |  Major | . | Wanqiang Ji | Wanqiang Ji |
| [HADOOP-16340](https://issues.apache.org/jira/browse/HADOOP-16340) | ABFS driver continues to retry on IOException responses from REST operations |  Major | fs/azure | Robert Levas | Robert Levas |
| [HADOOP-16379](https://issues.apache.org/jira/browse/HADOOP-16379) | S3AInputStream#unbuffer should merge input stream stats into fs-wide stats |  Major | fs/s3 | Sahil Takiar | Sahil Takiar |
| [HADOOP-15183](https://issues.apache.org/jira/browse/HADOOP-15183) | S3Guard store becomes inconsistent after partial failure of rename |  Blocker | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-15658](https://issues.apache.org/jira/browse/HADOOP-15658) | Memory leak in S3AOutputStream |  Major | fs/s3 | Piotr Nowojski | Steve Loughran |
| [HADOOP-16364](https://issues.apache.org/jira/browse/HADOOP-16364) | S3Guard table destroy to map IllegalArgumentExceptions to IOEs |  Minor | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-15604](https://issues.apache.org/jira/browse/HADOOP-15604) | Bulk commits of S3A MPUs place needless excessive load on S3 & S3Guard |  Major | fs/s3 | Gabor Bota | Steve Loughran |
| [HADOOP-16368](https://issues.apache.org/jira/browse/HADOOP-16368) | S3A list operation doesn't pick up etags from results |  Minor | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-9374](https://issues.apache.org/jira/browse/YARN-9374) | HBaseTimelineWriterImpl sync writes has to avoid thread blocking if storage down |  Major | ATSv2 | Prabhu Joseph | Prabhu Joseph |
| [HADOOP-16363](https://issues.apache.org/jira/browse/HADOOP-16363) | S3Guard DDB store prune() doesn't translate AWS exceptions to IOEs |  Minor | fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-14590](https://issues.apache.org/jira/browse/HDFS-14590) | [SBN Read] Add the document link to the top page |  Major | documentation | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-9477](https://issues.apache.org/jira/browse/YARN-9477) | Implement VE discovery using libudev |  Major | . | Peter Bacsko | Peter Bacsko |
| [YARN-6055](https://issues.apache.org/jira/browse/YARN-6055) | ContainersMonitorImpl need be adjusted when NM resource changed. |  Major | graceful, nodemanager, scheduler | Junping Du | Íñigo Goiri |
| [HDFS-14036](https://issues.apache.org/jira/browse/HDFS-14036) | RBF: Add hdfs-rbf-default.xml to HdfsConfiguration by default |  Major | . | Íñigo Goiri | Takanobu Asanuma |
| [YARN-9623](https://issues.apache.org/jira/browse/YARN-9623) | Auto adjust max queue length of app activities to make sure activities on all nodes can be covered |  Major | . | Tao Yang | Tao Yang |
| [YARN-9560](https://issues.apache.org/jira/browse/YARN-9560) | Restructure DockerLinuxContainerRuntime to extend a new OCIContainerRuntime |  Major | . | Eric Badger | Eric Badger |
| [HDFS-14620](https://issues.apache.org/jira/browse/HDFS-14620) | RBF: Fix 'not a super user' error when disabling a namespace in kerberos with superuser principal |  Major | . | luhuachao | luhuachao |
| [HDFS-14622](https://issues.apache.org/jira/browse/HDFS-14622) | [Dynamometer] State transition err when CCM( HDFS Centralized Cache Management) feature is used |  Major | tools | TanYuxin | Erik Krogen |
| [YARN-9660](https://issues.apache.org/jira/browse/YARN-9660) | Enhance documentation of Docker on YARN support |  Major | documentation, nodemanager | Peter Bacsko | Peter Bacsko |
| [HDFS-14640](https://issues.apache.org/jira/browse/HDFS-14640) | [Dynamometer] Fix TestDynamometerInfra failures |  Major | test, tools | Erik Krogen | Erik Krogen |
| [HDFS-14410](https://issues.apache.org/jira/browse/HDFS-14410) | Make Dynamometer documentation properly compile onto the Hadoop site |  Major | . | Erik Krogen | Erik Krogen |
| [HADOOP-16357](https://issues.apache.org/jira/browse/HADOOP-16357) | TeraSort Job failing on S3 DirectoryStagingCommitter: destination path exists |  Minor | fs/s3 | Prabhu Joseph | Steve Loughran |
| [HDFS-14611](https://issues.apache.org/jira/browse/HDFS-14611) | Move handshake secret field from Token to BlockAccessToken |  Blocker | hdfs | Chen Liang | Chen Liang |
| [HADOOP-16384](https://issues.apache.org/jira/browse/HADOOP-16384) | S3A: Avoid inconsistencies between DDB and S3 |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-16397](https://issues.apache.org/jira/browse/HADOOP-16397) | Hadoop S3Guard Prune command to support a -tombstone option. |  Minor | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-16406](https://issues.apache.org/jira/browse/HADOOP-16406) | ITestDynamoDBMetadataStore.testProvisionTable times out intermittently |  Minor | fs/s3, test | Steve Loughran | Steve Loughran |
| [HDFS-14458](https://issues.apache.org/jira/browse/HDFS-14458) | Report pmem stats to namenode |  Major | . | Feilong He | Feilong He |
| [HDFS-14357](https://issues.apache.org/jira/browse/HDFS-14357) | Update documentation for HDFS cache on SCM support |  Major | . | Feilong He | Feilong He |
| [HDFS-14593](https://issues.apache.org/jira/browse/HDFS-14593) | RBF: Implement deletion feature for expired records in State Store |  Major | rbf | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-16383](https://issues.apache.org/jira/browse/HADOOP-16383) | Pass ITtlTimeProvider instance in initialize method in MetadataStore interface |  Major | . | Gabor Bota | Gabor Bota |
| [HDFS-14653](https://issues.apache.org/jira/browse/HDFS-14653) | RBF: Correct the default value for dfs.federation.router.namenode.heartbeat.enable |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-14577](https://issues.apache.org/jira/browse/HDFS-14577) | RBF: FederationUtil#newInstance should allow constructor without context |  Major | . | CR Hota | CR Hota |
| [HADOOP-15847](https://issues.apache.org/jira/browse/HADOOP-15847) | S3Guard testConcurrentTableCreations to set r & w capacity == 0 |  Major | fs/s3, test | Steve Loughran | lqjacklee |
| [HADOOP-16380](https://issues.apache.org/jira/browse/HADOOP-16380) | S3A tombstones can confuse empty directory status |  Blocker | fs/s3, test | Steve Loughran | Steve Loughran |
| [HADOOP-16447](https://issues.apache.org/jira/browse/HADOOP-16447) | Upgrade JUnit5 from 5.3.1 to 5.5.1 to support global timeout |  Major | test | Akira Ajisaka | Kevin Su |
| [HDFS-14670](https://issues.apache.org/jira/browse/HDFS-14670) | RBF: Create secret manager instance using FederationUtil#newInstance. |  Major | . | CR Hota | CR Hota |
| [HDFS-14639](https://issues.apache.org/jira/browse/HDFS-14639) | [Dynamometer] Unnecessary duplicate bin directory appears in dist layout |  Major | namenode, test | Erik Krogen | Erik Krogen |
| [HADOOP-16472](https://issues.apache.org/jira/browse/HADOOP-16472) | findbugs warning on LocalMetadataStore.ttlTimeProvider sync |  Major | build, fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-16275](https://issues.apache.org/jira/browse/HADOOP-16275) | Upgrade Mockito to the latest version |  Minor | test | Akira Ajisaka | Kevin Su |
| [HADOOP-16479](https://issues.apache.org/jira/browse/HADOOP-16479) | ABFS FileStatus.getModificationTime returns localized time instead of UTC |  Major | fs/azure | Joan Sala Reixach | Bilahari T H |
| [HDFS-14034](https://issues.apache.org/jira/browse/HDFS-14034) | Support getQuotaUsage API in WebHDFS |  Major | fs, webhdfs | Erik Krogen | Chao Sun |
| [HDFS-14700](https://issues.apache.org/jira/browse/HDFS-14700) | Clean up pmem cache before setting pmem cache capacity |  Minor | caching, datanode | Feilong He | Feilong He |
| [HADOOP-16315](https://issues.apache.org/jira/browse/HADOOP-16315) | ABFS: transform full UPN for named user in AclStatus |  Major | fs/azure | Da Zhou | Da Zhou |
| [HADOOP-16499](https://issues.apache.org/jira/browse/HADOOP-16499) | S3A retry policy to be exponential |  Critical | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-16481](https://issues.apache.org/jira/browse/HADOOP-16481) | ITestS3GuardDDBRootOperations.test\_300\_MetastorePrune needs to set region |  Major | fs/s3, test | Steve Loughran | Steve Loughran |
| [YARN-9694](https://issues.apache.org/jira/browse/YARN-9694) | UI always show default-rack for all the nodes while running SLS. |  Major | . | Abhishek Modi | Abhishek Modi |
| [HADOOP-16509](https://issues.apache.org/jira/browse/HADOOP-16509) | [hadoop-mapreduce-project] Fix order of actual and expected expression in assert statements |  Major | . | Adam Antal | Adam Antal |
| [HDFS-14717](https://issues.apache.org/jira/browse/HDFS-14717) | Junit not found in hadoop-dynamometer-infra |  Major | . | Kevin Su | Kevin Su |
| [YARN-9608](https://issues.apache.org/jira/browse/YARN-9608) | DecommissioningNodesWatcher should get lists of running applications on node from RMNode. |  Major | . | Abhishek Modi | Abhishek Modi |
| [HADOOP-16500](https://issues.apache.org/jira/browse/HADOOP-16500) | S3ADelegationTokens to only log at debug on startup |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-9683](https://issues.apache.org/jira/browse/YARN-9683) | Remove reapDockerContainerNoPid left behind by YARN-9074 |  Trivial | yarn | Adam Antal | Kevin Su |
| [HDFS-14713](https://issues.apache.org/jira/browse/HDFS-14713) | RBF: RouterAdmin supports refreshRouterArgs command but not on display |  Major | . | wangzhaohui | wangzhaohui |
| [YARN-9765](https://issues.apache.org/jira/browse/YARN-9765) | SLS runner crashes when run with metrics turned off. |  Major | . | Abhishek Modi | Abhishek Modi |
| [YARN-9752](https://issues.apache.org/jira/browse/YARN-9752) | Add support for allocation id in SLS. |  Major | . | Abhishek Modi | Abhishek Modi |
| [HDFS-14714](https://issues.apache.org/jira/browse/HDFS-14714) | RBF: implement getReplicatedBlockStats interface |  Major | . | Chen Zhang | Chen Zhang |
| [HDFS-14756](https://issues.apache.org/jira/browse/HDFS-14756) | RBF: getQuotaUsage may ignore some folders |  Major | . | Chen Zhang | Chen Zhang |
| [HDFS-14744](https://issues.apache.org/jira/browse/HDFS-14744) | RBF: Non secured routers should not log in error mode when UGI is default. |  Major | . | CR Hota | CR Hota |
| [HDFS-14763](https://issues.apache.org/jira/browse/HDFS-14763) | Fix package name of audit log class in Dynamometer document |  Major | documentation, tools | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-16470](https://issues.apache.org/jira/browse/HADOOP-16470) | Make last AWS credential provider in default auth chain EC2ContainerCredentialsProviderWrapper |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-14755](https://issues.apache.org/jira/browse/HDFS-14755) | [Dynamometer] Hadoop-2 DataNode fail to start |  Major | tools | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-16469](https://issues.apache.org/jira/browse/HADOOP-16469) | Typo in s3a committers.md doc |  Minor | documentation, fs/s3 | Steve Loughran |  |
| [HDFS-14674](https://issues.apache.org/jira/browse/HDFS-14674) | [SBN read] Got an unexpected txid when tail editlog |  Blocker | . | wangzhaohui | wangzhaohui |
| [HDFS-14766](https://issues.apache.org/jira/browse/HDFS-14766) | RBF: MountTableStoreImpl#getMountTableEntries returns extra entry |  Major | . | Chen Zhang | Chen Zhang |
| [YARN-9775](https://issues.apache.org/jira/browse/YARN-9775) | RMWebServices /scheduler-conf GET returns all hadoop configurations for ZKConfigurationStore |  Major | restapi | Prabhu Joseph | Prabhu Joseph |
| [YARN-9755](https://issues.apache.org/jira/browse/YARN-9755) | RM fails to start with FileSystemBasedConfigurationProvider |  Major | resourcemanager | Prabhu Joseph | Prabhu Joseph |
| [HDFS-14779](https://issues.apache.org/jira/browse/HDFS-14779) | Fix logging error in TestEditLog#testMultiStreamsLoadEditWithConfMaxTxns |  Major | . | Jonathan Hung | Jonathan Hung |
| [HADOOP-16416](https://issues.apache.org/jira/browse/HADOOP-16416) | mark DynamoDBMetadataStore.deleteTrackingValueMap as final |  Trivial | fs/s3 | Steve Loughran | Kevin Su |
| [HDFS-8631](https://issues.apache.org/jira/browse/HDFS-8631) | WebHDFS : Support setQuota |  Major | . | nijel | Chao Sun |
| [YARN-9754](https://issues.apache.org/jira/browse/YARN-9754) | Add support for arbitrary DAG AM Simulator. |  Major | . | Abhishek Modi | Abhishek Modi |
| [YARN-9664](https://issues.apache.org/jira/browse/YARN-9664) | Improve response of scheduler/app activities for better understanding |  Major | . | Tao Yang | Tao Yang |
| [YARN-8678](https://issues.apache.org/jira/browse/YARN-8678) | Queue Management API - rephrase error messages |  Major | . | Akhil PB | Prabhu Joseph |
| [HDFS-14711](https://issues.apache.org/jira/browse/HDFS-14711) | RBF: RBFMetrics throws NullPointerException if stateStore disabled |  Major | . | Chen Zhang | Chen Zhang |
| [YARN-9791](https://issues.apache.org/jira/browse/YARN-9791) | Queue Mutation API does not allow to remove a config |  Major | capacity scheduler | Prabhu Joseph | Prabhu Joseph |
| [YARN-7982](https://issues.apache.org/jira/browse/YARN-7982) | Do ACLs check while retrieving entity-types per application |  Major | . | Rohith Sharma K S | Prabhu Joseph |
| [HDFS-14654](https://issues.apache.org/jira/browse/HDFS-14654) | RBF: TestRouterRpc#testNamenodeMetrics is flaky |  Major | . | Takanobu Asanuma | Chen Zhang |
| [YARN-9804](https://issues.apache.org/jira/browse/YARN-9804) | Update ATSv2 document for latest feature supports |  Blocker | . | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-14819](https://issues.apache.org/jira/browse/HDFS-14819) | [Dynamometer] Cannot parse audit logs with ‘=‘ in unexpected places when starting a workload. |  Major | . | Soya Miyoshi | Soya Miyoshi |
| [HDFS-14817](https://issues.apache.org/jira/browse/HDFS-14817) | [Dynamometer] start-dynamometer-cluster.sh shows its usage even if correct arguments are given. |  Major | tools | Soya Miyoshi | Soya Miyoshi |
| [YARN-9821](https://issues.apache.org/jira/browse/YARN-9821) | NM hangs at serviceStop when ATSV2 Backend Hbase is Down |  Major | ATSv2 | Prabhu Joseph | Prabhu Joseph |
| [HADOOP-16430](https://issues.apache.org/jira/browse/HADOOP-16430) | S3AFilesystem.delete to incrementally update s3guard with deletions |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-16490](https://issues.apache.org/jira/browse/HADOOP-16490) | Avoid/handle cached 404s during S3A file creation |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-9819](https://issues.apache.org/jira/browse/YARN-9819) | Make TestOpportunisticContainerAllocatorAMService more resilient. |  Major | . | Abhishek Modi | Abhishek Modi |
| [HADOOP-16562](https://issues.apache.org/jira/browse/HADOOP-16562) | [pb-upgrade] Update docker image to have 3.7.1 protoc executable |  Major | . | Vinayakumar B | Vinayakumar B |
| [YARN-9794](https://issues.apache.org/jira/browse/YARN-9794) | RM crashes due to runtime errors in TimelineServiceV2Publisher |  Major | . | Tarun Parimi | Tarun Parimi |
| [HADOOP-16371](https://issues.apache.org/jira/browse/HADOOP-16371) | Option to disable GCM for SSL connections when running on Java 8 |  Major | fs/s3 | Sahil Takiar | Sahil Takiar |
| [HDFS-14822](https://issues.apache.org/jira/browse/HDFS-14822) | [SBN read] Revisit GlobalStateIdContext locking when getting server state id |  Major | hdfs | Chen Liang | Chen Liang |
| [HADOOP-16557](https://issues.apache.org/jira/browse/HADOOP-16557) | [pb-upgrade] Upgrade protobuf.version to 3.7.1 |  Major | . | Vinayakumar B | Vinayakumar B |
| [HDFS-14833](https://issues.apache.org/jira/browse/HDFS-14833) | RBF: Router Update Doesn't Sync Quota |  Major | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-16589](https://issues.apache.org/jira/browse/HADOOP-16589) | [pb-upgrade] Update docker image to make 3.7.1 protoc as default |  Major | . | Vinayakumar B | Vinayakumar B |
| [HDFS-14818](https://issues.apache.org/jira/browse/HDFS-14818) | Check native pmdk lib by 'hadoop checknative' command |  Minor | native | Feilong He | Feilong He |
| [HADOOP-16558](https://issues.apache.org/jira/browse/HADOOP-16558) | [COMMON+HDFS] use protobuf-maven-plugin to generate protobuf classes |  Major | common | Vinayakumar B | Vinayakumar B |
| [HADOOP-16565](https://issues.apache.org/jira/browse/HADOOP-16565) | Region must be provided when requesting session credentials or SdkClientException will be thrown |  Major | fs/s3 | Gabor Bota | Gabor Bota |
| [HADOOP-16138](https://issues.apache.org/jira/browse/HADOOP-16138) | hadoop fs mkdir / of nonexistent abfs container raises NPE |  Minor | fs/azure | Steve Loughran | Gabor Bota |
| [HADOOP-16591](https://issues.apache.org/jira/browse/HADOOP-16591) | S3A ITest\*MRjob failures |  Major | fs/s3 | Siddharth Seth | Siddharth Seth |
| [HADOOP-16560](https://issues.apache.org/jira/browse/HADOOP-16560) | [YARN] use protobuf-maven-plugin to generate protobuf classes |  Major | . | Vinayakumar B | Duo Zhang |
| [HADOOP-16561](https://issues.apache.org/jira/browse/HADOOP-16561) | [MAPREDUCE] use protobuf-maven-plugin to generate protobuf classes |  Major | . | Vinayakumar B | Duo Zhang |
| [HDFS-14461](https://issues.apache.org/jira/browse/HDFS-14461) | RBF: Fix intermittently failing kerberos related unit test |  Major | . | CR Hota | Xiaoqiao He |
| [HDFS-14785](https://issues.apache.org/jira/browse/HDFS-14785) | [SBN read] Change client logging to be less aggressive |  Major | hdfs | Chen Liang | Chen Liang |
| [YARN-9859](https://issues.apache.org/jira/browse/YARN-9859) | Refactor OpportunisticContainerAllocator |  Major | . | Abhishek Modi | Abhishek Modi |
| [YARN-9864](https://issues.apache.org/jira/browse/YARN-9864) | Format CS Configuration present in Configuration Store |  Major | capacity scheduler | Prabhu Joseph | Prabhu Joseph |
| [YARN-9801](https://issues.apache.org/jira/browse/YARN-9801) | SchedConfCli does not work with https mode |  Major | . | Prabhu Joseph | Prabhu Joseph |
| [HADOOP-16458](https://issues.apache.org/jira/browse/HADOOP-16458) | LocatedFileStatusFetcher scans failing intermittently against S3 store |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-16578](https://issues.apache.org/jira/browse/HADOOP-16578) | ABFS: fileSystemExists() should not call container level apis |  Major | fs/azure | Da Zhou | Sneha Vijayarajan |
| [YARN-9870](https://issues.apache.org/jira/browse/YARN-9870) | Remove unused function from OpportunisticContainerAllocatorAMService |  Minor | . | Abhishek Modi | Abhishek Modi |
| [YARN-9792](https://issues.apache.org/jira/browse/YARN-9792) | Document examples of SchedulerConf with Node Labels |  Minor | capacity scheduler | Prabhu Joseph | Prabhu Joseph |
| [HADOOP-13373](https://issues.apache.org/jira/browse/HADOOP-13373) | Add S3A implementation of FSMainOperationsBaseTest |  Minor | fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-14858](https://issues.apache.org/jira/browse/HDFS-14858) | [SBN read] Allow configurably enable/disable AlignmentContext on NameNode |  Major | hdfs | Chen Liang | Chen Liang |
| [HADOOP-16620](https://issues.apache.org/jira/browse/HADOOP-16620) | [pb-upgrade] Remove protocol buffers 3.7.1 from requirements in BUILDING.txt |  Minor | build, documentation | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-15729](https://issues.apache.org/jira/browse/HADOOP-15729) | [s3a] stop treat fs.s3a.max.threads as the long-term minimum |  Major | fs/s3 | Sean Mackrory | Sean Mackrory |
| [HDFS-12979](https://issues.apache.org/jira/browse/HDFS-12979) | StandbyNode should upload FsImage to ObserverNode after checkpointing. |  Major | hdfs | Konstantin Shvachko | Chen Liang |
| [YARN-9782](https://issues.apache.org/jira/browse/YARN-9782) | Avoid DNS resolution while running SLS. |  Major | . | Abhishek Modi | Abhishek Modi |
| [HADOOP-16207](https://issues.apache.org/jira/browse/HADOOP-16207) | Improved S3A MR tests |  Critical | fs/s3, test | Steve Loughran | Steve Loughran |
| [HADOOP-16570](https://issues.apache.org/jira/browse/HADOOP-16570) | S3A committers leak threads/raises OOM on job/task commit at scale |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-16626](https://issues.apache.org/jira/browse/HADOOP-16626) | S3A ITestRestrictedReadAccess fails |  Major | fs/s3 | Siddharth Seth | Steve Loughran |
| [HADOOP-16512](https://issues.apache.org/jira/browse/HADOOP-16512) | [hadoop-tools] Fix order of actual and expected expression in assert statements |  Major | . | Adam Antal | Kevin Su |
| [HADOOP-16587](https://issues.apache.org/jira/browse/HADOOP-16587) | Make AAD endpoint configurable on all Auth flows |  Minor | fs/azure | Bilahari T H | Bilahari T H |
| [YARN-9873](https://issues.apache.org/jira/browse/YARN-9873) | Mutation API Config Change need to update Version Number |  Major | . | Prabhu Joseph | Prabhu Joseph |
| [HDFS-14162](https://issues.apache.org/jira/browse/HDFS-14162) | Balancer should work with ObserverNode |  Major | . | Konstantin Shvachko | Erik Krogen |
| [HADOOP-16650](https://issues.apache.org/jira/browse/HADOOP-16650) | ITestS3AClosedFS failing -junit test thread |  Blocker | fs/s3, test | Steve Loughran | Steve Loughran |
| [YARN-9699](https://issues.apache.org/jira/browse/YARN-9699) | Migration tool that help to generate CS config based on FS config [Phase 1] |  Major | . | Wanqiang Ji | Peter Bacsko |
| [HADOOP-16635](https://issues.apache.org/jira/browse/HADOOP-16635) | S3A innerGetFileStatus s"directories only" scan still does a HEAD |  Blocker | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-16634](https://issues.apache.org/jira/browse/HADOOP-16634) | S3A ITest failures without S3Guard |  Major | fs/s3, test | Steve Loughran | Steve Loughran |
| [YARN-9840](https://issues.apache.org/jira/browse/YARN-9840) | Capacity scheduler: add support for Secondary Group rule mapping |  Major | capacity scheduler | Peter Bacsko | Manikandan R |
| [HADOOP-16651](https://issues.apache.org/jira/browse/HADOOP-16651) | S3 getBucketLocation() can return "US" for us-east |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-16478](https://issues.apache.org/jira/browse/HADOOP-16478) | S3Guard bucket-info fails if the bucket location is denied to the caller |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-9773](https://issues.apache.org/jira/browse/YARN-9773) | Add QueueMetrics for Custom Resources |  Major | . | Manikandan R | Manikandan R |
| [YARN-9841](https://issues.apache.org/jira/browse/YARN-9841) | Capacity scheduler: add support for combined %user + %primary\_group mapping |  Major | capacity scheduler | Peter Bacsko | Manikandan R |
| [YARN-9884](https://issues.apache.org/jira/browse/YARN-9884) | Make container-executor mount logic modular |  Major | . | Eric Badger | Eric Badger |
| [YARN-9875](https://issues.apache.org/jira/browse/YARN-9875) | FSSchedulerConfigurationStore fails to update with hdfs path |  Major | capacityscheduler | Prabhu Joseph | Prabhu Joseph |
| [HDFS-14638](https://issues.apache.org/jira/browse/HDFS-14638) | [Dynamometer] Fix scripts to refer to current build structure |  Major | namenode, test | Erik Krogen | Takanobu Asanuma |
| [HDFS-14907](https://issues.apache.org/jira/browse/HDFS-14907) | [Dynamometer] DataNode can't find junit jar when using Hadoop-3 binary |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-14824](https://issues.apache.org/jira/browse/HDFS-14824) | [Dynamometer] Dynamometer in org.apache.hadoop.tools does not output the benchmark results. |  Major | . | Soya Miyoshi | Takanobu Asanuma |
| [HADOOP-16510](https://issues.apache.org/jira/browse/HADOOP-16510) | [hadoop-common] Fix order of actual and expected expression in assert statements |  Major | . | Adam Antal | Adam Antal |
| [YARN-9950](https://issues.apache.org/jira/browse/YARN-9950) | Unset Ordering Policy of Leaf/Parent queue converted from Parent/Leaf queue respectively |  Major | capacity scheduler | Prabhu Joseph | Prabhu Joseph |
| [HDFS-14891](https://issues.apache.org/jira/browse/HDFS-14891) | RBF: namenode links in NameFederation Health page (federationhealth.html)  cannot use https scheme |  Major | rbf, ui | Xieming Li | Xieming Li |
| [YARN-9865](https://issues.apache.org/jira/browse/YARN-9865) | Capacity scheduler: add support for combined %user + %secondary\_group mapping |  Major | . | Manikandan R | Manikandan R |
| [YARN-9697](https://issues.apache.org/jira/browse/YARN-9697) | Efficient allocation of Opportunistic containers. |  Major | . | Abhishek Modi | Abhishek Modi |
| [HADOOP-16477](https://issues.apache.org/jira/browse/HADOOP-16477) | S3A delegation token tests fail if fs.s3a.encryption.key set |  Major | fs/s3, test | Steve Loughran | Steve Loughran |
| [HDFS-14648](https://issues.apache.org/jira/browse/HDFS-14648) | Implement DeadNodeDetector basic model |  Major | . | Lisheng Sun | Lisheng Sun |
| [YARN-9900](https://issues.apache.org/jira/browse/YARN-9900) | Revert to previous state when Invalid Config is applied and Refresh Support in SchedulerConfig Format |  Major | capacity scheduler | Prabhu Joseph | Prabhu Joseph |
| [HADOOP-16484](https://issues.apache.org/jira/browse/HADOOP-16484) | S3A to warn or fail if S3Guard is disabled |  Minor | fs/s3 | Steve Loughran | Gabor Bota |
| [YARN-9562](https://issues.apache.org/jira/browse/YARN-9562) | Add Java changes for the new RuncContainerRuntime |  Major | . | Eric Badger | Eric Badger |
| [HADOOP-16657](https://issues.apache.org/jira/browse/HADOOP-16657) | Move remaining log4j APIs over to slf4j in hadoop-common. |  Major | . | Minni Mittal | Minni Mittal |
| [HADOOP-16610](https://issues.apache.org/jira/browse/HADOOP-16610) | Upgrade to yetus 0.11.1 and use emoji vote on github pre commit |  Major | build | Duo Zhang | Duo Zhang |
| [YARN-9909](https://issues.apache.org/jira/browse/YARN-9909) | Offline format of YarnConfigurationStore |  Major | capacity scheduler | Prabhu Joseph | Prabhu Joseph |
| [HADOOP-16632](https://issues.apache.org/jira/browse/HADOOP-16632) | Speculating & Partitioned S3A magic committers can leave pending files under \_\_magic |  Minor | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-9836](https://issues.apache.org/jira/browse/YARN-9836) | General usability improvements in showSimulationTrace.html |  Minor | scheduler-load-simulator | Adam Antal | Adam Antal |
| [HADOOP-16707](https://issues.apache.org/jira/browse/HADOOP-16707) | NPE in UGI.getCurrentUser in ITestAbfsIdentityTransformer setup |  Major | auth, fs/azure, security | Steve Loughran | Steve Loughran |
| [HADOOP-16687](https://issues.apache.org/jira/browse/HADOOP-16687) | ABFS: Fix testcase added for HADOOP-16138 for namespace enabled account |  Major | fs/azure | Sneha Vijayarajan | Sneha Vijayarajan |
| [HDFS-14651](https://issues.apache.org/jira/browse/HDFS-14651) | DeadNodeDetector checks dead node periodically |  Major | . | Lisheng Sun | Lisheng Sun |
| [HADOOP-16708](https://issues.apache.org/jira/browse/HADOOP-16708) | HadoopExecutors cleanup to only log at debug |  Minor | util | Steve Loughran | David Mollitor |
| [YARN-9899](https://issues.apache.org/jira/browse/YARN-9899) | Migration tool that help to generate CS config based on FS config [Phase 2] |  Major | . | Szilard Nemeth | Peter Bacsko |
| [HDFS-14649](https://issues.apache.org/jira/browse/HDFS-14649) | Add suspect probe for DeadNodeDetector |  Major | . | Lisheng Sun | Lisheng Sun |
| [HADOOP-16455](https://issues.apache.org/jira/browse/HADOOP-16455) | ABFS: Implement FileSystem.access() method |  Minor | fs/azure | Bilahari T H | Bilahari T H |
| [HADOOP-16660](https://issues.apache.org/jira/browse/HADOOP-16660) | ABFS: Make RetryCount in ExponentialRetryPolicy Configurable |  Major | fs/azure | Sneha Vijayarajan | Sneha Vijayarajan |
| [HDFS-15019](https://issues.apache.org/jira/browse/HDFS-15019) | Refactor the unit test of TestDeadNodeDetection |  Minor | . | Yiqun Lin | Lisheng Sun |
| [HDFS-14825](https://issues.apache.org/jira/browse/HDFS-14825) | [Dynamometer] Workload doesn't start unless an absolute path of Mapper class given |  Major | . | Soya Miyoshi | Takanobu Asanuma |
| [HDFS-13811](https://issues.apache.org/jira/browse/HDFS-13811) | RBF: Race condition between router admin quota update and periodic quota update service |  Major | . | Dibyendu Karmakar | Jinglun |
| [YARN-9781](https://issues.apache.org/jira/browse/YARN-9781) | SchedConfCli to get current stored scheduler configuration |  Major | capacity scheduler | Prabhu Joseph | Prabhu Joseph |
| [YARN-9780](https://issues.apache.org/jira/browse/YARN-9780) | SchedulerConf Mutation API does not Allow Stop and Remove Queue in a single call |  Major | capacity scheduler | Prabhu Joseph | Prabhu Joseph |
| [YARN-9789](https://issues.apache.org/jira/browse/YARN-9789) | Disable Option for Write Ahead Logs of LogMutation |  Major | capacity scheduler | Prabhu Joseph | Prabhu Joseph |
| [YARN-9607](https://issues.apache.org/jira/browse/YARN-9607) | Auto-configuring rollover-size of IFile format for non-appendable filesystems |  Major | log-aggregation, yarn | Adam Antal | Adam Antal |
| [YARN-9561](https://issues.apache.org/jira/browse/YARN-9561) | Add C changes for the new RuncContainerRuntime |  Major | . | Eric Badger | Eric Badger |
| [HADOOP-16612](https://issues.apache.org/jira/browse/HADOOP-16612) | Track Azure Blob File System client-perceived latency |  Major | fs/azure, hdfs-client | Jeetesh Mangwani | Jeetesh Mangwani |
| [HDFS-15043](https://issues.apache.org/jira/browse/HDFS-15043) | RBF: The detail of the Exception is not shown in ZKDelegationTokenSecretManagerImpl |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-16758](https://issues.apache.org/jira/browse/HADOOP-16758) | Refine testing.md to tell user better how to use auth-keys.xml |  Minor | fs/s3 | Mingliang Liu | Mingliang Liu |
| [HDFS-14983](https://issues.apache.org/jira/browse/HDFS-14983) | RBF: Add dfsrouteradmin -refreshSuperUserGroupsConfiguration command option |  Minor | rbf | Akira Ajisaka | Xieming Li |
| [HDFS-15044](https://issues.apache.org/jira/browse/HDFS-15044) | [Dynamometer] Show the line of audit log when parsing it unsuccessfully |  Major | tools | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-16757](https://issues.apache.org/jira/browse/HADOOP-16757) | Increase timeout unit test rule for MetadataStoreTestBase |  Major | fs/s3 | Mingliang Liu | Mingliang Liu |
| [HADOOP-16764](https://issues.apache.org/jira/browse/HADOOP-16764) | Rewrite Python example codes using Python3 |  Minor | documentation | Kengo Seki | Kengo Seki |
| [HADOOP-16751](https://issues.apache.org/jira/browse/HADOOP-16751) | DurationInfo text parsing/formatting should be moved out of hotpath |  Minor | fs/s3 | Rajesh Balamohan | Rajesh Balamohan |
| [YARN-10035](https://issues.apache.org/jira/browse/YARN-10035) | Add ability to filter the Cluster Applications API request by name |  Major | yarn | Adam Antal | Adam Antal |
| [HDFS-15066](https://issues.apache.org/jira/browse/HDFS-15066) | HttpFS: Implement setErasureCodingPolicy , unsetErasureCodingPolicy , getErasureCodingPolicy |  Major | . | hemanthboyina | hemanthboyina |
| [HADOOP-16645](https://issues.apache.org/jira/browse/HADOOP-16645) | S3A Delegation Token extension point to use StoreContext |  Minor | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-16699](https://issues.apache.org/jira/browse/HADOOP-16699) | ABFS: Enhance driver debug logs |  Major | fs/azure | Sneha Vijayarajan | Sneha Vijayarajan |
| [YARN-10068](https://issues.apache.org/jira/browse/YARN-10068) | TimelineV2Client may leak file descriptors creating ClientResponse objects. |  Critical | ATSv2 | Anand Srinivasan | Anand Srinivasan |
| [HADOOP-16642](https://issues.apache.org/jira/browse/HADOOP-16642) | ITestDynamoDBMetadataStoreScale fails when throttled. |  Major | fs/s3, test | Steve Loughran | Steve Loughran |
| [HDFS-15100](https://issues.apache.org/jira/browse/HDFS-15100) | RBF: Print stacktrace when DFSRouter fails to fetch/parse JMX output from NameNode |  Major | rbf | Akira Ajisaka | Akira Ajisaka |
| [YARN-10071](https://issues.apache.org/jira/browse/YARN-10071) | Sync Mockito version with other modules |  Major | build, test | Akira Ajisaka | Adam Antal |
| [HADOOP-16697](https://issues.apache.org/jira/browse/HADOOP-16697) | audit/tune s3a authoritative flag in s3guard DDB Table |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-10067](https://issues.apache.org/jira/browse/YARN-10067) | Add dry-run feature to FS-CS converter tool |  Major | . | Peter Bacsko | Peter Bacsko |
| [YARN-10026](https://issues.apache.org/jira/browse/YARN-10026) | Pull out common code pieces from ATS v1.5 and v2 |  Major | ATSv2, yarn | Adam Antal | Adam Antal |
| [HADOOP-16797](https://issues.apache.org/jira/browse/HADOOP-16797) | Add dockerfile for ARM builds |  Major | . | Vinayakumar B | Vinayakumar B |
| [YARN-9788](https://issues.apache.org/jira/browse/YARN-9788) | Queue Management API does not support parallel updates |  Major | capacity scheduler | Prabhu Joseph | Prabhu Joseph |
| [HADOOP-16005](https://issues.apache.org/jira/browse/HADOOP-16005) | NativeAzureFileSystem does not support setXAttr |  Major | fs/azure | Clemens Wolff | Clemens Wolff |
| [YARN-10028](https://issues.apache.org/jira/browse/YARN-10028) | Integrate the new abstract log servlet to the JobHistory server |  Major | yarn | Adam Antal | Adam Antal |
| [YARN-10082](https://issues.apache.org/jira/browse/YARN-10082) | FS-CS converter: disable terminal placement rule checking |  Critical | . | Peter Bacsko | Peter Bacsko |
| [HADOOP-16621](https://issues.apache.org/jira/browse/HADOOP-16621) | [pb-upgrade] Remove Protobuf classes from signatures of Public APIs |  Critical | common | Steve Loughran | Vinayakumar B |
| [YARN-9525](https://issues.apache.org/jira/browse/YARN-9525) | IFile format is not working against s3a remote folder |  Major | log-aggregation | Adam Antal | Adam Antal |
| [HADOOP-16346](https://issues.apache.org/jira/browse/HADOOP-16346) | Stabilize S3A OpenSSL support |  Blocker | fs/s3 | Steve Loughran | Sahil Takiar |
| [HADOOP-16759](https://issues.apache.org/jira/browse/HADOOP-16759) | Filesystem openFile() builder to take a FileStatus param |  Minor | fs, fs/azure, fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-10083](https://issues.apache.org/jira/browse/YARN-10083) | Provide utility to ask whether an application is in final status |  Minor | . | Adam Antal | Adam Antal |
| [HADOOP-16792](https://issues.apache.org/jira/browse/HADOOP-16792) | Let s3 clients configure request timeout |  Major | fs/s3 | Mustafa Iman | Mustafa Iman |
| [HADOOP-16827](https://issues.apache.org/jira/browse/HADOOP-16827) | TestHarFileSystem.testInheritedMethodsImplemented broken |  Major | fs, test | Steve Loughran | Steve Loughran |
| [HADOOP-16746](https://issues.apache.org/jira/browse/HADOOP-16746) | S3A empty dir markers are not created in s3guard as authoritative |  Critical | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-10085](https://issues.apache.org/jira/browse/YARN-10085) | FS-CS converter: remove mixed ordering policy check |  Critical | . | Peter Bacsko | Peter Bacsko |
| [YARN-10104](https://issues.apache.org/jira/browse/YARN-10104) | FS-CS converter: dry run should work without output defined |  Major | . | Peter Bacsko | Peter Bacsko |
| [YARN-10015](https://issues.apache.org/jira/browse/YARN-10015) | Correct the sample command in SLS README file |  Trivial | yarn | Aihua Xu | Aihua Xu |
| [YARN-10099](https://issues.apache.org/jira/browse/YARN-10099) | FS-CS converter: handle allow-undeclared-pools and user-as-default-queue properly and fix misc issues |  Major | . | Peter Bacsko | Peter Bacsko |
| [YARN-8982](https://issues.apache.org/jira/browse/YARN-8982) | [Router] Add locality policy |  Major | . | Giovanni Matteo Fumarola | Young Chen |
| [HADOOP-16732](https://issues.apache.org/jira/browse/HADOOP-16732) | S3Guard to support encrypted DynamoDB table |  Major | fs/s3 | Mingliang Liu | Mingliang Liu |
| [HADOOP-16825](https://issues.apache.org/jira/browse/HADOOP-16825) | ITestAzureBlobFileSystemCheckAccess failing |  Major | fs/azure, test | Steve Loughran | Bilahari T H |
| [HADOOP-16845](https://issues.apache.org/jira/browse/HADOOP-16845) | ITestAbfsClient.testContinuationTokenHavingEqualSign failing |  Major | fs/azure, test | Sneha Vijayarajan | Sneha Vijayarajan |
| [HADOOP-16596](https://issues.apache.org/jira/browse/HADOOP-16596) | [pb-upgrade] Use shaded protobuf classes from hadoop-thirdparty dependency |  Major | . | Vinayakumar B | Vinayakumar B |
| [YARN-10109](https://issues.apache.org/jira/browse/YARN-10109) | Allow stop and convert from leaf to parent queue in a single Mutation API call |  Major | capacity scheduler | Prabhu Joseph | Prabhu Joseph |
| [YARN-10101](https://issues.apache.org/jira/browse/YARN-10101) | Support listing of aggregated logs for containers belonging to an application attempt |  Major | log-aggregation, yarn | Adam Antal | Adam Antal |
| [YARN-10127](https://issues.apache.org/jira/browse/YARN-10127) | FSQueueConverter should not set App Ordering Policy to Parent Queue |  Major | . | Prabhu Joseph | Peter Bacsko |
| [YARN-10022](https://issues.apache.org/jira/browse/YARN-10022) | Create RM Rest API to validate a CapacityScheduler Configuration |  Major | . | Kinga Marton | Kinga Marton |
| [HDFS-13989](https://issues.apache.org/jira/browse/HDFS-13989) | RBF: Add FSCK to the Router |  Major | . | Íñigo Goiri | Akira Ajisaka |
| [YARN-10029](https://issues.apache.org/jira/browse/YARN-10029) | Add option to UIv2 to get container logs from the new JHS API |  Major | yarn | Adam Antal | Adam Antal |
| [HADOOP-16823](https://issues.apache.org/jira/browse/HADOOP-16823) | Large DeleteObject requests are their own Thundering Herd |  Minor | fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-15173](https://issues.apache.org/jira/browse/HDFS-15173) | RBF: Delete repeated configuration 'dfs.federation.router.metrics.enable' |  Minor | documentation, rbf | panlijie | panlijie |
| [HADOOP-15961](https://issues.apache.org/jira/browse/HADOOP-15961) | S3A committers: make sure there's regular progress() calls |  Minor | fs/s3 | Steve Loughran | lqjacklee |
| [YARN-10139](https://issues.apache.org/jira/browse/YARN-10139) | ValidateAndGetSchedulerConfiguration API fails when cluster max allocation \> default 8GB |  Major | . | Prabhu Joseph | Prabhu Joseph |
| [HADOOP-16706](https://issues.apache.org/jira/browse/HADOOP-16706) | ITestClientUrlScheme fails for accounts which don't support HTTP |  Minor | fs/azure, test | Steve Loughran | Steve Loughran |
| [HADOOP-16711](https://issues.apache.org/jira/browse/HADOOP-16711) | S3A bucket existence checks to support v2 API and "no checks at all" |  Minor | fs/s3 | Rajesh Balamohan | Mukund Thakur |
| [HDFS-15172](https://issues.apache.org/jira/browse/HDFS-15172) | Remove unnecessary  deadNodeDetectInterval in DeadNodeDetector#checkDeadNodes() |  Major | . | Lisheng Sun | Lisheng Sun |
| [HDFS-15041](https://issues.apache.org/jira/browse/HDFS-15041) | Make MAX\_LOCK\_HOLD\_MS and full queue size configurable |  Major | namenode | zhuqi | zhuqi |
| [HADOOP-16853](https://issues.apache.org/jira/browse/HADOOP-16853) | ITestS3GuardOutOfBandOperations failing on versioned S3 buckets |  Minor | fs/s3, test | Steve Loughran | Steve Loughran |
| [YARN-10157](https://issues.apache.org/jira/browse/YARN-10157) | FS-CS converter: initPropertyActions() is not called without rules file |  Major | . | Peter Bacsko | Peter Bacsko |
| [YARN-10135](https://issues.apache.org/jira/browse/YARN-10135) | FS-CS converter tool: issue warning on dynamic auto-create mapping rules |  Major | . | Peter Bacsko | Peter Bacsko |
| [YARN-10130](https://issues.apache.org/jira/browse/YARN-10130) | FS-CS converter: Do not allow output dir to be the same as input dir |  Major | . | Szilard Nemeth | Adam Antal |
| [HDFS-14731](https://issues.apache.org/jira/browse/HDFS-14731) | [FGL] Remove redundant locking on NameNode. |  Major | namenode | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-16767](https://issues.apache.org/jira/browse/HADOOP-16767) | S3AInputStream reopening does not handle non IO exceptions properly |  Major | . | Sergei Poganshev | Sergei Poganshev |
| [YARN-10175](https://issues.apache.org/jira/browse/YARN-10175) | FS-CS converter: only convert placement rules if a cmd line switch is defined |  Major | . | Peter Bacsko | Peter Bacsko |
| [YARN-10167](https://issues.apache.org/jira/browse/YARN-10167) | FS-CS Converter: Need to validate c-s.xml after converting |  Major | . | Wangda Tan | Peter Bacsko |
| [HADOOP-16905](https://issues.apache.org/jira/browse/HADOOP-16905) | Update jackson-databind to 2.10.3 to relieve us from the endless CVE patches |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-6924](https://issues.apache.org/jira/browse/YARN-6924) | Metrics for Federation AMRMProxy |  Major | . | Giovanni Matteo Fumarola | Young Chen |
| [YARN-10168](https://issues.apache.org/jira/browse/YARN-10168) | FS-CS Converter: tool doesn't handle min/max resource conversion correctly |  Blocker | . | Wangda Tan | Peter Bacsko |
| [HADOOP-16890](https://issues.apache.org/jira/browse/HADOOP-16890) | ABFS: Change in expiry calculation for MSI token provider |  Minor | fs/azure | Bilahari T H | Bilahari T H |
| [YARN-10191](https://issues.apache.org/jira/browse/YARN-10191) | FS-CS converter: call System.exit function call for every code path in main method |  Blocker | . | Peter Bacsko | Peter Bacsko |
| [YARN-10193](https://issues.apache.org/jira/browse/YARN-10193) | FS-CS converter: fix incorrect capacity conversion |  Blocker | . | Peter Bacsko | Peter Bacsko |
| [YARN-10110](https://issues.apache.org/jira/browse/YARN-10110) | In Federation Secure cluster Application submission fails when authorization is enabled |  Blocker | federation | Sushanta Sen | Bilwa S T |
| [YARN-9538](https://issues.apache.org/jira/browse/YARN-9538) | Document scheduler/app activities and REST APIs |  Major | documentation | Tao Yang | Tao Yang |
| [YARN-9567](https://issues.apache.org/jira/browse/YARN-9567) | Add diagnostics for outstanding resource requests on app attempts page |  Major | capacityscheduler | Tao Yang | Tao Yang |
| [HADOOP-16858](https://issues.apache.org/jira/browse/HADOOP-16858) | S3Guard fsck: Add option to remove orphaned entries |  Major | fs/s3 | Gabor Bota | Gabor Bota |
| [HDFS-15088](https://issues.apache.org/jira/browse/HDFS-15088) | RBF: Correct annotation typo of RouterPermissionChecker#checkPermission |  Trivial | rbf | Xiaoqiao He | Xiaoqiao He |
| [YARN-9879](https://issues.apache.org/jira/browse/YARN-9879) | Allow multiple leaf queues with the same name in CapacityScheduler |  Major | . | Gergely Pollak | Gergely Pollak |
| [YARN-10197](https://issues.apache.org/jira/browse/YARN-10197) | FS-CS converter: fix emitted ordering policy string and max-am-resource percent value |  Major | . | Peter Bacsko | Peter Bacsko |
| [YARN-10043](https://issues.apache.org/jira/browse/YARN-10043) | FairOrderingPolicy Improvements |  Major | . | Manikandan R | Manikandan R |
| [HDFS-13470](https://issues.apache.org/jira/browse/HDFS-13470) | RBF: Add Browse the Filesystem button to the UI |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HADOOP-15430](https://issues.apache.org/jira/browse/HADOOP-15430) | hadoop fs -mkdir -p path-ending-with-slash/ fails with s3guard |  Minor | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-16939](https://issues.apache.org/jira/browse/HADOOP-16939) | fs.s3a.authoritative.path should support multiple FS URIs |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-16859](https://issues.apache.org/jira/browse/HADOOP-16859) | ABFS: Add unbuffer support to AbfsInputStream |  Major | fs/azure | Sahil Takiar | Sahil Takiar |
| [YARN-10120](https://issues.apache.org/jira/browse/YARN-10120) | In Federation Router Nodes/Applications/About pages throws 500 exception when https is enabled |  Critical | federation | Sushanta Sen | Bilwa S T |
| [HADOOP-16465](https://issues.apache.org/jira/browse/HADOOP-16465) | Tune S3AFileSystem.listLocatedStatus |  Major | fs/s3 | Steve Loughran | Mukund Thakur |
| [YARN-10234](https://issues.apache.org/jira/browse/YARN-10234) | FS-CS converter: don't enable auto-create queue property for root |  Critical | . | Peter Bacsko | Peter Bacsko |
| [HADOOP-13873](https://issues.apache.org/jira/browse/HADOOP-13873) | log DNS addresses on s3a init |  Minor | fs/s3 | Steve Loughran | Mukund Thakur |
| [HADOOP-16959](https://issues.apache.org/jira/browse/HADOOP-16959) | Resolve hadoop-cos dependency conflict |  Major | build, fs/cos | Yang Yu | Yang Yu |
| [HADOOP-16986](https://issues.apache.org/jira/browse/HADOOP-16986) | s3a to not need wildfly on the classpath |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-14742](https://issues.apache.org/jira/browse/HDFS-14742) | RBF:TestRouterFaultTolerant tests are flaky |  Major | test | Chen Zhang | Akira Ajisaka |
| [HADOOP-16794](https://issues.apache.org/jira/browse/HADOOP-16794) | S3A reverts KMS encryption to the bucket's default KMS key in rename/copy |  Major | fs/s3 | Mukund Thakur | Mukund Thakur |
| [HADOOP-16920](https://issues.apache.org/jira/browse/HADOOP-16920) | ABFS: Make list page size configurable |  Minor | fs/azure | Bilahari T H | Bilahari T H |
| [YARN-10194](https://issues.apache.org/jira/browse/YARN-10194) | YARN RMWebServices /scheduler-conf/validate leaks ZK Connections |  Blocker | capacityscheduler | Akhil PB | Prabhu Joseph |
| [YARN-10215](https://issues.apache.org/jira/browse/YARN-10215) | Endpoint for obtaining direct URL for the logs |  Major | yarn | Adam Antal | Andras Gyori |
| [HDFS-14353](https://issues.apache.org/jira/browse/HDFS-14353) | Erasure Coding: metrics xmitsInProgress become to negative. |  Major | datanode, erasure-coding | maobaolong | maobaolong |
| [HADOOP-16953](https://issues.apache.org/jira/browse/HADOOP-16953) | HADOOP-16953. tune s3guard disabled warnings |  Minor | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-16835](https://issues.apache.org/jira/browse/HADOOP-16835) | catch and downgrade all exceptions trying to load openssl native libs through wildfly |  Minor | fs/azure, fs/s3 | Steve Loughran | Steve Loughran |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-13338](https://issues.apache.org/jira/browse/HDFS-13338) | Update BUILDING.txt for building native libraries |  Critical | build, documentation, native | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-13403](https://issues.apache.org/jira/browse/HDFS-13403) | libhdfs++: Use hdfs::IoService object rather than asio::io\_service |  Critical | . | James Clampffer | James Clampffer |
| [HDFS-13534](https://issues.apache.org/jira/browse/HDFS-13534) | libhdfs++: Fix GCC7 build |  Major | . | James Clampffer | James Clampffer |
| [HADOOP-15816](https://issues.apache.org/jira/browse/HADOOP-15816) | Upgrade Apache Zookeeper version due to security concerns |  Major | . | Boris Vulikh | Akira Ajisaka |
| [HADOOP-15882](https://issues.apache.org/jira/browse/HADOOP-15882) | Upgrade maven-shade-plugin from 2.4.3 to 3.2.0 |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-15815](https://issues.apache.org/jira/browse/HADOOP-15815) | Upgrade Eclipse Jetty version to 9.3.24 |  Major | . | Boris Vulikh | Boris Vulikh |
| [HDFS-13870](https://issues.apache.org/jira/browse/HDFS-13870) | WebHDFS: Document ALLOWSNAPSHOT and DISALLOWSNAPSHOT API doc |  Minor | documentation, webhdfs | Siyao Meng | Siyao Meng |
| [YARN-8489](https://issues.apache.org/jira/browse/YARN-8489) | Need to support "dominant" component concept inside YARN service |  Major | yarn-native-services | Wangda Tan | Zac Zhou |
| [HDFS-12729](https://issues.apache.org/jira/browse/HDFS-12729) | Document special paths in HDFS |  Major | documentation | Christopher Douglas | Masatake Iwasaki |
| [YARN-9191](https://issues.apache.org/jira/browse/YARN-9191) | Add cli option in DS to support enforceExecutionType in resource requests. |  Major | . | Abhishek Modi | Abhishek Modi |
| [YARN-9428](https://issues.apache.org/jira/browse/YARN-9428) | Add metrics for paused containers in NodeManager |  Major | . | Abhishek Modi | Abhishek Modi |
| [HDFS-14394](https://issues.apache.org/jira/browse/HDFS-14394) | Add -std=c99 / -std=gnu99 to libhdfs compile flags |  Major | hdfs-client, libhdfs, native | Sahil Takiar | Sahil Takiar |
| [HADOOP-15242](https://issues.apache.org/jira/browse/HADOOP-15242) | Fix typos in hadoop-functions.sh |  Trivial | . | Ray Chiang | Ray Chiang |
| [YARN-9433](https://issues.apache.org/jira/browse/YARN-9433) | Remove unused constants from RMAuditLogger |  Minor | yarn | Adam Antal | Igor Rudenko |
| [HDFS-14433](https://issues.apache.org/jira/browse/HDFS-14433) | Remove the extra empty space in the DataStreamer logging |  Trivial | hdfs | Yishuang Lu | Yishuang Lu |
| [YARN-9469](https://issues.apache.org/jira/browse/YARN-9469) | Fix typo in YarnConfiguration: physical memory |  Trivial | yarn | Adam Antal | Igor Rudenko |
| [HADOOP-16263](https://issues.apache.org/jira/browse/HADOOP-16263) | Update BUILDING.txt with macOS native build instructions |  Minor | . | Siyao Meng | Siyao Meng |
| [HADOOP-16365](https://issues.apache.org/jira/browse/HADOOP-16365) | Upgrade jackson-databind to 2.9.9 |  Major | build | Shweta | Shweta |
| [YARN-9599](https://issues.apache.org/jira/browse/YARN-9599) | TestContainerSchedulerQueuing#testQueueShedding fails intermittently. |  Minor | . | Abhishek Modi | Abhishek Modi |
| [YARN-9559](https://issues.apache.org/jira/browse/YARN-9559) | Create AbstractContainersLauncher for pluggable ContainersLauncher logic |  Major | . | Jonathan Hung | Jonathan Hung |
| [HADOOP-16491](https://issues.apache.org/jira/browse/HADOOP-16491) | Upgrade jetty version to 9.3.27 |  Major | . | Hrishikesh Gadre | Hrishikesh Gadre |
| [HADOOP-16351](https://issues.apache.org/jira/browse/HADOOP-16351) | Change ":" to ApplicationConstants.CLASS\_PATH\_SEPARATOR |  Trivial | common | Kevin Su | Kevin Su |
| [HDFS-14729](https://issues.apache.org/jira/browse/HDFS-14729) | Upgrade Bootstrap and jQuery versions used in HDFS UIs |  Major | ui | Vivek Ratnavel Subramanian | Vivek Ratnavel Subramanian |
| [HADOOP-16438](https://issues.apache.org/jira/browse/HADOOP-16438) | Introduce a config to control SSL Channel mode in Azure DataLake Store Gen1 |  Major | fs/adl | Sneha Vijayarajan | Sneha Vijayarajan |
| [HADOOP-16542](https://issues.apache.org/jira/browse/HADOOP-16542) | Update commons-beanutils version to 1.9.4 |  Major | . | Wei-Chiu Chuang | Kevin Su |
| [HADOOP-16555](https://issues.apache.org/jira/browse/HADOOP-16555) | Update commons-compress to 1.19 |  Major | . | Wei-Chiu Chuang | YiSheng Lien |
| [YARN-9730](https://issues.apache.org/jira/browse/YARN-9730) | Support forcing configured partitions to be exclusive based on app node label |  Major | . | Jonathan Hung | Jonathan Hung |
| [HADOOP-16675](https://issues.apache.org/jira/browse/HADOOP-16675) | Upgrade jackson-databind to 2.9.10.1 |  Blocker | security | Wei-Chiu Chuang | Lisheng Sun |
| [HADOOP-16656](https://issues.apache.org/jira/browse/HADOOP-16656) | Document FairCallQueue configs in core-default.xml |  Major | conf, documentation | Siyao Meng | Siyao Meng |
| [HDFS-14959](https://issues.apache.org/jira/browse/HDFS-14959) | [SBNN read] access time should be turned off |  Major | documentation | Wei-Chiu Chuang | Chao Sun |
| [HADOOP-16654](https://issues.apache.org/jira/browse/HADOOP-16654) | Delete hadoop-ozone and hadoop-hdds subprojects from apache trunk |  Major | . | Marton Elek | Sandeep Nemuri |
| [HDFS-15047](https://issues.apache.org/jira/browse/HDFS-15047) | Document the new decommission monitor (HDFS-14854) |  Major | documentation | Wei-Chiu Chuang | Masatake Iwasaki |
| [HADOOP-16784](https://issues.apache.org/jira/browse/HADOOP-16784) | Update the year to 2020 |  Major | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-16358](https://issues.apache.org/jira/browse/HADOOP-16358) | Add an ARM CI for Hadoop |  Major | build | Zhenyu Zheng | Zhenyu Zheng |
| [HADOOP-16803](https://issues.apache.org/jira/browse/HADOOP-16803) | Upgrade jackson-databind to 2.9.10.2 |  Blocker | security | Akira Ajisaka | Masatake Iwasaki |
| [HADOOP-16670](https://issues.apache.org/jira/browse/HADOOP-16670) | Stripping Submarine code from Hadoop codebase. |  Blocker | . | Wei-Chiu Chuang | Zhankun Tang |
| [HADOOP-16871](https://issues.apache.org/jira/browse/HADOOP-16871) | Upgrade Netty version to 4.1.45.Final to handle CVE-2019-20444,CVE-2019-16869 |  Major | . | Aray Chenchu Sukesh | Aray Chenchu Sukesh |
| [HADOOP-16647](https://issues.apache.org/jira/browse/HADOOP-16647) | Support OpenSSL 1.1.1 LTS |  Critical | security | Wei-Chiu Chuang | Rakesh Radhakrishnan |
| [HADOOP-16982](https://issues.apache.org/jira/browse/HADOOP-16982) | Update Netty to 4.1.48.Final |  Blocker | . | Wei-Chiu Chuang | Lisheng Sun |
| [YARN-10247](https://issues.apache.org/jira/browse/YARN-10247) | Application priority queue ACLs are not respected |  Blocker | capacity scheduler | Sunil G | Sunil G |


