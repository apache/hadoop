
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

## Release 3.2.0 - Unreleased (as of 2018-09-02)

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-6257](https://issues.apache.org/jira/browse/YARN-6257) | CapacityScheduler REST API produces incorrect JSON - JSON object operationsInfo contains deplicate key |  Minor | capacityscheduler | Tao Yang | Tao Yang |
| [HADOOP-15146](https://issues.apache.org/jira/browse/HADOOP-15146) | Remove DataOutputByteBuffer |  Minor | common | BELUGA BEHR | BELUGA BEHR |
| [YARN-8191](https://issues.apache.org/jira/browse/YARN-8191) | Fair scheduler: queue deletion without RM restart |  Major | fairscheduler | Gergo Repas | Gergo Repas |
| [HADOOP-15495](https://issues.apache.org/jira/browse/HADOOP-15495) | Upgrade common-lang version to 3.7 in hadoop-common-project and hadoop-tools |  Major | . | Takanobu Asanuma | Takanobu Asanuma |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-14667](https://issues.apache.org/jira/browse/HADOOP-14667) | Flexible Visual Studio support |  Major | build | Allen Wittenauer | Allen Wittenauer |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-5764](https://issues.apache.org/jira/browse/YARN-5764) | NUMA awareness support for launching containers |  Major | nodemanager, yarn | Olasoji | Devaraj K |
| [HDFS-13056](https://issues.apache.org/jira/browse/HDFS-13056) | Expose file-level composite CRCs in HDFS which are comparable across different instances/layouts |  Major | datanode, distcp, erasure-coding, federation, hdfs | Dennis Huo | Dennis Huo |
| [HDFS-13283](https://issues.apache.org/jira/browse/HDFS-13283) | Percentage based Reserved Space Calculation for DataNode |  Major | datanode, hdfs | Lukas Majercak | Lukas Majercak |
| [HDFS-13448](https://issues.apache.org/jira/browse/HDFS-13448) | HDFS Block Placement - Ignore Locality for First Block Replica |  Minor | block placement, hdfs-client | BELUGA BEHR | BELUGA BEHR |
| [YARN-7812](https://issues.apache.org/jira/browse/YARN-7812) | Improvements to Rich Placement Constraints in YARN |  Major | . | Arun Suresh |  |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-8028](https://issues.apache.org/jira/browse/YARN-8028) | Support authorizeUserAccessToQueue in RMWebServices |  Major | . | Wangda Tan | Wangda Tan |
| [HADOOP-15332](https://issues.apache.org/jira/browse/HADOOP-15332) | Fix typos in hadoop-aws markdown docs |  Minor | . | Gabor Bota | Gabor Bota |
| [HADOOP-15330](https://issues.apache.org/jira/browse/HADOOP-15330) | Remove jdk1.7 profile from hadoop-annotations module |  Minor | . | Akira Ajisaka | fang zhenyi |
| [HADOOP-15295](https://issues.apache.org/jira/browse/HADOOP-15295) | Remove redundant logging related to tags from Configuration |  Major | . | Ajay Kumar | Ajay Kumar |
| [HADOOP-15339](https://issues.apache.org/jira/browse/HADOOP-15339) | Support additional key/value propereties in JMX bean registration |  Major | common | Elek, Marton | Elek, Marton |
| [YARN-8077](https://issues.apache.org/jira/browse/YARN-8077) | The vmemLimit parameter in ContainersMonitorImpl#isProcessTreeOverLimit is confusing |  Trivial | nodemanager | Sen Zhao | Sen Zhao |
| [HDFS-13357](https://issues.apache.org/jira/browse/HDFS-13357) | Improve AclException message "Invalid ACL: only directories may have a default ACL." |  Minor | . | Wei-Chiu Chuang | Gabor Bota |
| [HADOOP-15342](https://issues.apache.org/jira/browse/HADOOP-15342) | Update ADLS connector to use the current SDK version (2.2.7) |  Major | fs/adl | Atul Sikaria | Atul Sikaria |
| [YARN-8082](https://issues.apache.org/jira/browse/YARN-8082) | Include LocalizedResource size information in the NM download log for localization |  Minor | . | Kuhu Shukla | Kuhu Shukla |
| [YARN-1151](https://issues.apache.org/jira/browse/YARN-1151) | Ability to configure auxiliary services from HDFS-based JAR files |  Major | nodemanager | john lilley | Xuan Gong |
| [HDFS-13363](https://issues.apache.org/jira/browse/HDFS-13363) | Record file path when FSDirAclOp throws AclException |  Minor | . | Wei-Chiu Chuang | Gabor Bota |
| [MAPREDUCE-7069](https://issues.apache.org/jira/browse/MAPREDUCE-7069) | Add ability to specify user environment variables individually |  Major | . | Jim Brennan | Jim Brennan |
| [HDFS-13418](https://issues.apache.org/jira/browse/HDFS-13418) |  NetworkTopology should be configurable when enable DFSNetworkTopology |  Major | . | Tao Jie | Tao Jie |
| [HDFS-13439](https://issues.apache.org/jira/browse/HDFS-13439) | Add test case for read block operation when it is moved |  Major | . | Ajay Kumar | Ajay Kumar |
| [HDFS-13462](https://issues.apache.org/jira/browse/HDFS-13462) | Add BIND\_HOST configuration for JournalNode's HTTP and RPC Servers |  Major | hdfs, journal-node | Lukas Majercak | Lukas Majercak |
| [HADOOP-15393](https://issues.apache.org/jira/browse/HADOOP-15393) | Upgrade the version of commons-lang3 to 3.7 |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-7966](https://issues.apache.org/jira/browse/YARN-7966) | Remove method AllocationConfiguration#getQueueAcl and related unit tests |  Minor | fairscheduler | Yufei Gu | Sen Zhao |
| [YARN-8169](https://issues.apache.org/jira/browse/YARN-8169) | Cleanup RackResolver.java |  Minor | yarn | BELUGA BEHR | BELUGA BEHR |
| [YARN-8185](https://issues.apache.org/jira/browse/YARN-8185) | Improve log in DirectoryCollection constructor |  Major | nodemanager | Yufei Gu | Yufei Gu |
| [HDFS-13468](https://issues.apache.org/jira/browse/HDFS-13468) | Add erasure coding metrics into ReadStatistics |  Major | erasure-coding | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [YARN-8140](https://issues.apache.org/jira/browse/YARN-8140) | Improve log message when launch cmd is ran for stopped yarn service |  Major | yarn-native-services | Yesha Vora | Eric Yang |
| [MAPREDUCE-7086](https://issues.apache.org/jira/browse/MAPREDUCE-7086) | Add config to allow FileInputFormat to ignore directories when recursive=false |  Major | . | Sergey Shelukhin | Sergey Shelukhin |
| [HADOOP-15377](https://issues.apache.org/jira/browse/HADOOP-15377) | Improve debug messages in MetricsConfig.java |  Minor | common | BELUGA BEHR | BELUGA BEHR |
| [HADOOP-15382](https://issues.apache.org/jira/browse/HADOOP-15382) | Log kinit output in credential renewal thread |  Minor | security | Wei-Chiu Chuang | Gabor Bota |
| [YARN-8163](https://issues.apache.org/jira/browse/YARN-8163) | Add support for Node Labels in opportunistic scheduling. |  Major | . | Abhishek Modi | Abhishek Modi |
| [HDFS-5926](https://issues.apache.org/jira/browse/HDFS-5926) | Documentation should clarify dfs.datanode.du.reserved impact from reserved disk capacity |  Minor | documentation | Alexander Fahlke | Gabor Bota |
| [MAPREDUCE-7093](https://issues.apache.org/jira/browse/MAPREDUCE-7093) | Use assertEquals instead of assertTrue(a == b) in TestMapReduceJobControlWithMocks |  Minor | test | Akira Ajisaka | Abhishek Modi |
| [HDFS-12981](https://issues.apache.org/jira/browse/HDFS-12981) | renameSnapshot a Non-Existent snapshot to itself should throw error |  Minor | hdfs | Sailesh Patel | Kitti Nanasi |
| [YARN-8239](https://issues.apache.org/jira/browse/YARN-8239) | [UI2] Clicking on Node Manager UI under AM container info / App Attempt page goes to old RM UI |  Major | yarn-ui-v2 | Sumana Sathish | Sunil Govindan |
| [YARN-8260](https://issues.apache.org/jira/browse/YARN-8260) | [UI2] Per-application tracking URL is no longer available in YARN UI2 |  Major | yarn-ui-v2 | Sunil Govindan | Sunil Govindan |
| [YARN-8201](https://issues.apache.org/jira/browse/YARN-8201) | Skip stacktrace of few exception from ClientRMService |  Minor | . | Bibin A Chundatt | Bilwa S T |
| [HADOOP-15354](https://issues.apache.org/jira/browse/HADOOP-15354) | hadoop-aliyun & hadoop-azure modules to mark hadoop-common as provided |  Major | build, fs/azure, fs/oss | Steve Loughran | Steve Loughran |
| [YARN-3610](https://issues.apache.org/jira/browse/YARN-3610) | FairScheduler: Add steady-fair-shares to the REST API documentation |  Major | documentation, fairscheduler | Karthik Kambatla | Ray Chiang |
| [HADOOP-15441](https://issues.apache.org/jira/browse/HADOOP-15441) | Log kms url and token service at debug level. |  Minor | . | Wei-Chiu Chuang | Gabor Bota |
| [HDFS-13544](https://issues.apache.org/jira/browse/HDFS-13544) | Improve logging for JournalNode in federated cluster |  Major | federation, hdfs | Hanisha Koneru | Hanisha Koneru |
| [YARN-8249](https://issues.apache.org/jira/browse/YARN-8249) | Few REST api's in RMWebServices are missing static user check |  Critical | webapp, yarn | Sunil Govindan | Sunil Govindan |
| [YARN-8123](https://issues.apache.org/jira/browse/YARN-8123) | Skip compiling old hamlet package when the Java version is 10 or upper |  Major | webapp | Akira Ajisaka | Dinesh Chitlangia |
| [HDFS-13512](https://issues.apache.org/jira/browse/HDFS-13512) | WebHdfs getFileStatus doesn't return ecPolicy |  Major | . | Ajay Kumar | Ajay Kumar |
| [HADOOP-15250](https://issues.apache.org/jira/browse/HADOOP-15250) | Split-DNS MultiHomed Server Network Cluster Network IPC Client Bind Addr Wrong |  Critical | ipc, net | Greg Senia | Ajay Kumar |
| [HADOOP-15154](https://issues.apache.org/jira/browse/HADOOP-15154) | Abstract new method assertCapability for StreamCapabilities testing |  Minor | test | Xiao Chen | Zsolt Venczel |
| [HADOOP-15457](https://issues.apache.org/jira/browse/HADOOP-15457) | Add Security-Related HTTP Response Header in WEBUIs. |  Major | . | Kanwaljeet Sachdev | Kanwaljeet Sachdev |
| [HDFS-13589](https://issues.apache.org/jira/browse/HDFS-13589) | Add dfsAdmin command to query if "upgrade" is finalized |  Major | hdfs | Hanisha Koneru | Hanisha Koneru |
| [HADOOP-15486](https://issues.apache.org/jira/browse/HADOOP-15486) | Make NetworkTopology#netLock fair |  Major | net | Nanda kumar | Nanda kumar |
| [HDFS-13493](https://issues.apache.org/jira/browse/HDFS-13493) | Reduce the HttpServer2 thread count on DataNodes |  Major | datanode | Erik Krogen | Erik Krogen |
| [HDFS-13598](https://issues.apache.org/jira/browse/HDFS-13598) | Reduce unnecessary byte-to-string transform operation in INodesInPath#toString |  Minor | . | Yiqun Lin | Gabor Bota |
| [YARN-8213](https://issues.apache.org/jira/browse/YARN-8213) | Add Capacity Scheduler performance metrics |  Critical | capacityscheduler, metrics | Weiwei Yang | Weiwei Yang |
| [HADOOP-15477](https://issues.apache.org/jira/browse/HADOOP-15477) | Make unjar in RunJar overrideable |  Trivial | . | Johan Gustavsson | Johan Gustavsson |
| [HDFS-13628](https://issues.apache.org/jira/browse/HDFS-13628) | Update Archival Storage doc for Provided Storage |  Major | documentation | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-15449](https://issues.apache.org/jira/browse/HADOOP-15449) | Increase default timeout of ZK session to avoid frequent NameNode failover |  Critical | common | Karthik Palanisamy | Karthik Palanisamy |
| [MAPREDUCE-7098](https://issues.apache.org/jira/browse/MAPREDUCE-7098) | Upgrade common-langs version to 3.7 in hadoop-mapreduce-project |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-8333](https://issues.apache.org/jira/browse/YARN-8333) | Load balance YARN services using RegistryDNS multiple A records |  Major | yarn-native-services | Eric Yang | Eric Yang |
| [HDFS-13440](https://issues.apache.org/jira/browse/HDFS-13440) | Log HDFS file name when client fails to connect |  Trivial | . | Wei-Chiu Chuang | Gabor Bota |
| [HADOOP-14783](https://issues.apache.org/jira/browse/HADOOP-14783) | [KMS] Add missing configuration properties into kms-default.xml |  Minor | kms | Wei-Chiu Chuang | Chetna Chaudhari |
| [HDFS-13602](https://issues.apache.org/jira/browse/HDFS-13602) | Add checkOperation(WRITE) checks in FSNamesystem |  Major | ha, namenode | Erik Krogen | Chao Sun |
| [HDFS-13155](https://issues.apache.org/jira/browse/HDFS-13155) | BlockPlacementPolicyDefault.chooseTargetInOrder Not Checking Return Value for NULL |  Minor | namenode | BELUGA BEHR | Zsolt Venczel |
| [YARN-8389](https://issues.apache.org/jira/browse/YARN-8389) | Improve the description of machine-list property in Federation docs |  Major | documentation, federation | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-15507](https://issues.apache.org/jira/browse/HADOOP-15507) | Add MapReduce counters about EC bytes read |  Major | . | Xiao Chen | Xiao Chen |
| [HDFS-13511](https://issues.apache.org/jira/browse/HDFS-13511) | Provide specialized exception when block length cannot be obtained |  Major | . | Ted Yu | Gabor Bota |
| [HADOOP-15512](https://issues.apache.org/jira/browse/HADOOP-15512) | clean up Shell from JDK7 workarounds |  Minor | common | Steve Loughran | Zsolt Venczel |
| [HDFS-13659](https://issues.apache.org/jira/browse/HDFS-13659) | Add more test coverage for contentSummary for snapshottable path |  Major | namenode, test | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-6677](https://issues.apache.org/jira/browse/YARN-6677) | Preempt opportunistic containers when root container cgroup goes over memory limit |  Major | nodemanager | Haibo Chen | Haibo Chen |
| [YARN-8400](https://issues.apache.org/jira/browse/YARN-8400) | Fix typos in YARN Federation documentation page |  Trivial | . | Bibin A Chundatt | Giovanni Matteo Fumarola |
| [HADOOP-15499](https://issues.apache.org/jira/browse/HADOOP-15499) | Performance severe drop when running RawErasureCoderBenchmark with NativeRSRawErasureCoder |  Major | . | Sammi Chen | Sammi Chen |
| [YARN-8322](https://issues.apache.org/jira/browse/YARN-8322) | Change log level when there is an IOException when the allocation file is loaded |  Minor | fairscheduler | Haibo Chen | Szilard Nemeth |
| [YARN-8321](https://issues.apache.org/jira/browse/YARN-8321) | AllocationFileLoaderService. getAllocationFile() should be declared as VisibleForTest |  Trivial | fairscheduler | Haibo Chen | Szilard Nemeth |
| [HDFS-13653](https://issues.apache.org/jira/browse/HDFS-13653) | Make dfs.client.failover.random.order a per nameservice configuration |  Major | federation | Ekanth Sethuramalingam | Ekanth Sethuramalingam |
| [YARN-8363](https://issues.apache.org/jira/browse/YARN-8363) | Upgrade commons-lang version to 3.7 in hadoop-yarn-project |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-8325](https://issues.apache.org/jira/browse/YARN-8325) | Miscellaneous QueueManager code clean up |  Minor | fairscheduler | Haibo Chen | Szilard Nemeth |
| [YARN-8394](https://issues.apache.org/jira/browse/YARN-8394) | Improve data locality documentation for Capacity Scheduler |  Major | . | Weiwei Yang | Weiwei Yang |
| [HDFS-13641](https://issues.apache.org/jira/browse/HDFS-13641) | Add metrics for edit log tailing |  Major | metrics | Chao Sun | Chao Sun |
| [HDFS-13582](https://issues.apache.org/jira/browse/HDFS-13582) | Improve backward compatibility for HDFS-13176 (WebHdfs file path gets truncated when having semicolon (;) inside) |  Major | . | Zsolt Venczel | Zsolt Venczel |
| [HDFS-13686](https://issues.apache.org/jira/browse/HDFS-13686) | Add overall metrics for FSNamesystemLock |  Major | hdfs, namenode | Lukas Majercak | Lukas Majercak |
| [HDFS-13621](https://issues.apache.org/jira/browse/HDFS-13621) | Upgrade common-lang version to 3.7 in hadoop-hdfs-project |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [MAPREDUCE-7063](https://issues.apache.org/jira/browse/MAPREDUCE-7063) | Fix log level inconsistency in CombineFileInputFormat.java |  Minor | client | BELUGA BEHR | Vidura Bhathiya Mudalige |
| [YARN-8440](https://issues.apache.org/jira/browse/YARN-8440) | Typo in YarnConfiguration javadoc: "Miniumum request grant-able.." |  Trivial | . | Szilard Nemeth | Szilard Nemeth |
| [YARN-7449](https://issues.apache.org/jira/browse/YARN-7449) | Split up class TestYarnClient to TestYarnClient and TestYarnClientImpl |  Minor | client, yarn | Yufei Gu | Szilard Nemeth |
| [YARN-8442](https://issues.apache.org/jira/browse/YARN-8442) | Strange characters and missing spaces in FairScheduler documentation |  Major | . | Szilard Nemeth | Szilard Nemeth |
| [YARN-8441](https://issues.apache.org/jira/browse/YARN-8441) | Typo in CSQueueUtils local variable names: queueGuranteedResource |  Trivial | resourcemanager | Szilard Nemeth | Szilard Nemeth |
| [MAPREDUCE-7113](https://issues.apache.org/jira/browse/MAPREDUCE-7113) | Typos in test names in TestTaskAttempt: "testAppDiognostic" |  Minor | . | Szilard Nemeth | Szilard Nemeth |
| [HADOOP-15551](https://issues.apache.org/jira/browse/HADOOP-15551) | Avoid use of Java8 streams in Configuration.addTags |  Major | performance | Todd Lipcon | Todd Lipcon |
| [HDFS-13692](https://issues.apache.org/jira/browse/HDFS-13692) | StorageInfoDefragmenter floods log when compacting StorageInfo TreeSet |  Minor | . | Yiqun Lin | Bharat Viswanadham |
| [YARN-8214](https://issues.apache.org/jira/browse/YARN-8214) | Change default RegistryDNS port |  Major | . | Billie Rinaldi | Billie Rinaldi |
| [YARN-8461](https://issues.apache.org/jira/browse/YARN-8461) | Support strict memory control on individual container with elastic control memory mechanism |  Major | nodemanager | Haibo Chen | Haibo Chen |
| [HADOOP-14313](https://issues.apache.org/jira/browse/HADOOP-14313) | Replace/improve Hadoop's byte[] comparator |  Major | common | Vikas Vishwakarma | Vikas Vishwakarma |
| [HDFS-13703](https://issues.apache.org/jira/browse/HDFS-13703) | Avoid allocation of CorruptedBlocks hashmap when no corrupted blocks are hit |  Major | performance | Todd Lipcon | Todd Lipcon |
| [HADOOP-15554](https://issues.apache.org/jira/browse/HADOOP-15554) | Improve JIT performance for Configuration parsing |  Minor | conf, performance | Todd Lipcon | Todd Lipcon |
| [HDFS-13536](https://issues.apache.org/jira/browse/HDFS-13536) | [PROVIDED Storage] HA for InMemoryAliasMap |  Major | . | Virajith Jalaparti | Virajith Jalaparti |
| [HDFS-13714](https://issues.apache.org/jira/browse/HDFS-13714) | Fix TestNameNodePrunesMissingStorages test failures on Windows |  Major | hdfs, namenode, test | Lukas Majercak | Lukas Majercak |
| [HDFS-13712](https://issues.apache.org/jira/browse/HDFS-13712) | BlockReaderRemote.read() logging improvement |  Minor | hdfs-client | Gergo Repas | Gergo Repas |
| [YARN-8302](https://issues.apache.org/jira/browse/YARN-8302) | ATS v2 should handle HBase connection issue properly |  Major | ATSv2 | Yesha Vora | Billie Rinaldi |
| [HDFS-13719](https://issues.apache.org/jira/browse/HDFS-13719) | Docs around dfs.image.transfer.timeout are misleading |  Major | . | Kitti Nanasi | Kitti Nanasi |
| [HADOOP-15591](https://issues.apache.org/jira/browse/HADOOP-15591) | KMSClientProvider should log KMS DT acquisition at INFO level |  Minor | kms | Kitti Nanasi | Kitti Nanasi |
| [HADOOP-15581](https://issues.apache.org/jira/browse/HADOOP-15581) | Set default jetty log level to INFO in KMS |  Major | . | Kitti Nanasi | Kitti Nanasi |
| [HADOOP-15568](https://issues.apache.org/jira/browse/HADOOP-15568) | fix some typos in the .sh comments |  Trivial | bin | Steve Loughran | Steve Loughran |
| [YARN-8502](https://issues.apache.org/jira/browse/YARN-8502) | Use path strings consistently for webservice endpoints in RMWebServices |  Major | . | Szilard Nemeth | Szilard Nemeth |
| [HADOOP-15531](https://issues.apache.org/jira/browse/HADOOP-15531) | Use commons-text instead of commons-lang in some classes to fix deprecation warnings |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-15598](https://issues.apache.org/jira/browse/HADOOP-15598) | DataChecksum calculate checksum is contented on hashtable synchronization |  Major | common | Prasanth Jayachandran | Prasanth Jayachandran |
| [YARN-8524](https://issues.apache.org/jira/browse/YARN-8524) | Single parameter Resource / LightWeightResource constructor looks confusing |  Major | api | Szilard Nemeth | Szilard Nemeth |
| [YARN-8361](https://issues.apache.org/jira/browse/YARN-8361) | Change App Name Placement Rule to use App Name instead of App Id for configuration |  Major | yarn | Zian Chen | Zian Chen |
| [HDFS-13690](https://issues.apache.org/jira/browse/HDFS-13690) | Improve error message when creating encryption zone while KMS is unreachable |  Minor | encryption, hdfs, kms | Kitti Nanasi | Kitti Nanasi |
| [YARN-8501](https://issues.apache.org/jira/browse/YARN-8501) | Reduce complexity of RMWebServices' getApps method |  Major | restapi | Szilard Nemeth | Szilard Nemeth |
| [YARN-7300](https://issues.apache.org/jira/browse/YARN-7300) | DiskValidator is not used in LocalDirAllocator |  Major | . | Haibo Chen | Szilard Nemeth |
| [HADOOP-15596](https://issues.apache.org/jira/browse/HADOOP-15596) | Stack trace should not be printed out when running hadoop key commands |  Minor | common | Kitti Nanasi | Kitti Nanasi |
| [YARN-7133](https://issues.apache.org/jira/browse/YARN-7133) | Clean up lock-try order in fair scheduler |  Major | fairscheduler | Daniel Templeton | Szilard Nemeth |
| [HDFS-13761](https://issues.apache.org/jira/browse/HDFS-13761) | Add toString Method to AclFeature Class |  Minor | . | Shweta | Shweta |
| [HADOOP-15609](https://issues.apache.org/jira/browse/HADOOP-15609) | Retry KMS calls when SSLHandshakeException occurs |  Major | common, kms | Kitti Nanasi | Kitti Nanasi |
| [HADOOP-15612](https://issues.apache.org/jira/browse/HADOOP-15612) | Improve exception when tfile fails to load LzoCodec |  Major | . | Gera Shegalov | Gera Shegalov |
| [HDFS-11060](https://issues.apache.org/jira/browse/HDFS-11060) | make DEFAULT\_MAX\_CORRUPT\_FILEBLOCKS\_RETURNED configurable |  Minor | hdfs | Lantao Jin | Lantao Jin |
| [HADOOP-15611](https://issues.apache.org/jira/browse/HADOOP-15611) | Log more details for FairCallQueue |  Minor | . | Ryan Wu | Ryan Wu |
| [HDFS-13727](https://issues.apache.org/jira/browse/HDFS-13727) | Log full stack trace if DiskBalancer exits with an unhandled exception |  Minor | diskbalancer | Stephen O'Donnell | Gabor Bota |
| [YARN-8517](https://issues.apache.org/jira/browse/YARN-8517) | getContainer and getContainers ResourceManager REST API methods are not documented |  Major | resourcemanager | Szilard Nemeth | Antal Bálint Steinbach |
| [YARN-8566](https://issues.apache.org/jira/browse/YARN-8566) | Add diagnostic message for unschedulable containers |  Major | resourcemanager | Szilard Nemeth | Szilard Nemeth |
| [YARN-8584](https://issues.apache.org/jira/browse/YARN-8584) | Several typos in Log Aggregation related classes |  Minor | . | Szilard Nemeth | Szilard Nemeth |
| [YARN-8155](https://issues.apache.org/jira/browse/YARN-8155) | Improve ATSv2 client logging in RM and NM publisher |  Major | . | Rohith Sharma K S | Abhishek Modi |
| [HADOOP-15476](https://issues.apache.org/jira/browse/HADOOP-15476) | fix logging for split-dns multihome |  Major | . | Ajay Kumar | Ajay Kumar |
| [YARN-7948](https://issues.apache.org/jira/browse/YARN-7948) | Enable fair scheduler to refresh maximum allocation for multiple resource types |  Major | fairscheduler | Yufei Gu | Szilard Nemeth |
| [HDFS-13796](https://issues.apache.org/jira/browse/HDFS-13796) | Allow verbosity of InMemoryLevelDBAliasMapServer to be configurable |  Trivial | . | Virajith Jalaparti | Virajith Jalaparti |
| [YARN-8626](https://issues.apache.org/jira/browse/YARN-8626) | Create HomePolicyManager that sends all the requests to the home subcluster |  Minor | . | Giovanni Matteo Fumarola | Íñigo Goiri |
| [HDFS-13728](https://issues.apache.org/jira/browse/HDFS-13728) | Disk Balancer should not fail if volume usage is greater than capacity |  Minor | diskbalancer | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-13447](https://issues.apache.org/jira/browse/HDFS-13447) | Fix Typos - Node Not Chosen |  Trivial | namenode | BELUGA BEHR | BELUGA BEHR |
| [YARN-8601](https://issues.apache.org/jira/browse/YARN-8601) | Print ExecutionType in Container report CLI |  Minor | . | Bilwa S T | Bilwa S T |
| [HDFS-13658](https://issues.apache.org/jira/browse/HDFS-13658) | Expose HighestPriorityLowRedundancy blocks statistics |  Major | hdfs | Kitti Nanasi | Kitti Nanasi |
| [YARN-8568](https://issues.apache.org/jira/browse/YARN-8568) | Replace the deprecated zk-address property in the HA config example in ResourceManagerHA.md |  Minor | yarn | Antal Bálint Steinbach | Antal Bálint Steinbach |
| [HDFS-13735](https://issues.apache.org/jira/browse/HDFS-13735) | Make QJM HTTP URL connection timeout configurable |  Minor | qjm | Chao Sun | Chao Sun |
| [YARN-4946](https://issues.apache.org/jira/browse/YARN-4946) | RM should not consider an application as COMPLETED when log aggregation is not in a terminal state |  Major | log-aggregation | Robert Kanter | Szilard Nemeth |
| [HDFS-13814](https://issues.apache.org/jira/browse/HDFS-13814) | Remove super user privilege requirement for NameNode.getServiceStatus |  Minor | namenode | Chao Sun | Chao Sun |
| [YARN-8559](https://issues.apache.org/jira/browse/YARN-8559) | Expose mutable-conf scheduler's configuration in RM /scheduler-conf endpoint |  Major | resourcemanager | Anna Savarin | Weiwei Yang |
| [HDFS-13813](https://issues.apache.org/jira/browse/HDFS-13813) | Exit NameNode if dangling child inode is detected when saving FsImage |  Major | hdfs, namenode | Siyao Meng | Siyao Meng |
| [HADOOP-14212](https://issues.apache.org/jira/browse/HADOOP-14212) | Expose SecurityEnabled boolean field in JMX for other services besides NameNode |  Minor | . | Ray Burgemeestre | Adam Antal |
| [HDFS-13217](https://issues.apache.org/jira/browse/HDFS-13217) | Audit log all EC policy names during addErasureCodingPolicies |  Major | erasure-coding | liaoyuxiangqin | liaoyuxiangqin |
| [HDFS-13732](https://issues.apache.org/jira/browse/HDFS-13732) | ECAdmin should print the policy name when an EC policy is set |  Trivial | erasure-coding, tools | Soumyapn | Zsolt Venczel |
| [HDFS-13829](https://issues.apache.org/jira/browse/HDFS-13829) | Remove redundant condition judgement in DirectoryScanner#scan |  Minor | datanode | liaoyuxiangqin | liaoyuxiangqin |
| [HDFS-13822](https://issues.apache.org/jira/browse/HDFS-13822) | speedup libhdfs++ build (enable parallel build) |  Minor | . | Pradeep Ambati | Allen Wittenauer |
| [HADOOP-9214](https://issues.apache.org/jira/browse/HADOOP-9214) | Create a new touch command to allow modifying atime and mtime |  Minor | tools | Brian Burton | Hrishikesh Gadre |
| [YARN-8242](https://issues.apache.org/jira/browse/YARN-8242) | YARN NM: OOM error while reading back the state store on recovery |  Critical | yarn | Kanwaljeet Sachdev | Pradeep Ambati |
| [YARN-8683](https://issues.apache.org/jira/browse/YARN-8683) | Support to display pending scheduling requests in RM app attempt page |  Major | webapp | Tao Yang | Tao Yang |
| [HDFS-13821](https://issues.apache.org/jira/browse/HDFS-13821) | RBF: Add dfs.federation.router.mount-table.cache.enable so that users can disable cache |  Major | hdfs | Fei Hui | Fei Hui |
| [HDFS-13861](https://issues.apache.org/jira/browse/HDFS-13861) | RBF: Illegal Router Admin command leads to printing usage for all commands |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-13831](https://issues.apache.org/jira/browse/HDFS-13831) | Make block increment deletion number configurable |  Major | . | Yiqun Lin | Ryan Wu |
| [YARN-8051](https://issues.apache.org/jira/browse/YARN-8051) | TestRMEmbeddedElector#testCallbackSynchronization is flakey |  Major | test | Robert Kanter | Robert Kanter |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-8040](https://issues.apache.org/jira/browse/YARN-8040) | [UI2] New YARN UI webapp does not respect current pathname for REST api |  Major | yarn-ui-v2 | Sunil Govindan | Sunil Govindan |
| [HADOOP-15062](https://issues.apache.org/jira/browse/HADOOP-15062) | TestCryptoStreamsWithOpensslAesCtrCryptoCodec fails on Debian 9 |  Major | . | Miklos Szegedi | Miklos Szegedi |
| [HDFS-11043](https://issues.apache.org/jira/browse/HDFS-11043) | TestWebHdfsTimeouts fails |  Major | webhdfs | Andrew Wang | Chao Sun |
| [HADOOP-15331](https://issues.apache.org/jira/browse/HADOOP-15331) | Fix a race condition causing parsing error of java.io.BufferedInputStream in class org.apache.hadoop.conf.Configuration |  Major | common | Miklos Szegedi | Miklos Szegedi |
| [HDFS-11900](https://issues.apache.org/jira/browse/HDFS-11900) | Hedged reads thread pool creation not synchronized |  Major | hdfs-client | John Zhuge | John Zhuge |
| [YARN-8032](https://issues.apache.org/jira/browse/YARN-8032) | Yarn service should expose failuresValidityInterval to users and use it for launching containers |  Major | . | Chandni Singh | Chandni Singh |
| [YARN-8043](https://issues.apache.org/jira/browse/YARN-8043) | Add the exception message for failed launches running under LCE |  Major | . | Shane Kumpf | Shane Kumpf |
| [MAPREDUCE-6441](https://issues.apache.org/jira/browse/MAPREDUCE-6441) | Improve temporary directory name generation in LocalDistributedCacheManager for concurrent processes |  Major | . | William Watson | Haibo Chen |
| [HADOOP-15299](https://issues.apache.org/jira/browse/HADOOP-15299) | Bump Hadoop's Jackson 2 dependency 2.9.x |  Major | . | Sean Mackrory | Sean Mackrory |
| [YARN-7734](https://issues.apache.org/jira/browse/YARN-7734) | YARN-5418 breaks TestContainerLogsPage.testContainerLogPageAccess |  Major | . | Miklos Szegedi | Tao Yang |
| [HDFS-13087](https://issues.apache.org/jira/browse/HDFS-13087) | Snapshotted encryption zone information should be immutable |  Major | encryption | LiXin Ge | LiXin Ge |
| [HADOOP-12862](https://issues.apache.org/jira/browse/HADOOP-12862) | LDAP Group Mapping over SSL can not specify trust store |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-15352](https://issues.apache.org/jira/browse/HADOOP-15352) | Fix default local maven repository path in create-release script |  Minor | scripts | Elek, Marton | Elek, Marton |
| [HADOOP-15317](https://issues.apache.org/jira/browse/HADOOP-15317) | Improve NetworkTopology chooseRandom's loop |  Major | . | Xiao Chen | Xiao Chen |
| [HADOOP-15355](https://issues.apache.org/jira/browse/HADOOP-15355) | TestCommonConfigurationFields is broken by HADOOP-15312 |  Major | test | Konstantin Shvachko | LiXin Ge |
| [YARN-7764](https://issues.apache.org/jira/browse/YARN-7764) | Findbugs warning: Resource#getResources may expose internal representation |  Major | api | Weiwei Yang | Weiwei Yang |
| [YARN-8115](https://issues.apache.org/jira/browse/YARN-8115) | [UI2] URL data like nodeHTTPAddress must be encoded in UI before using to access NM |  Major | yarn-ui-v2 | Sunil Govindan | Sreenath Somarajapuram |
| [HADOOP-14855](https://issues.apache.org/jira/browse/HADOOP-14855) | Hadoop scripts may errantly believe a daemon is still running, preventing it from starting |  Major | scripts | Aaron T. Myers | Robert Kanter |
| [HDFS-13350](https://issues.apache.org/jira/browse/HDFS-13350) | Negative legacy block ID will confuse Erasure Coding to be considered as striped block |  Major | erasure-coding | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [YARN-8119](https://issues.apache.org/jira/browse/YARN-8119) | [UI2] Timeline Server address' url scheme should be removed while accessing via KNOX |  Major | yarn-ui-v2 | Sunil Govindan | Sunil Govindan |
| [HDFS-13176](https://issues.apache.org/jira/browse/HDFS-13176) | WebHdfs file path gets truncated when having semicolon (;) inside |  Major | webhdfs | Zsolt Venczel | Zsolt Venczel |
| [YARN-8083](https://issues.apache.org/jira/browse/YARN-8083) | [UI2] All YARN related configurations are paged together in conf page |  Major | yarn-ui-v2 | Zoltan Haindrich | Gergely Novák |
| [HDFS-13292](https://issues.apache.org/jira/browse/HDFS-13292) | Crypto command should give proper exception when trying to set key on existing EZ directory |  Major | hdfs, kms | Harshakiran Reddy | Ranith Sardar |
| [HADOOP-15366](https://issues.apache.org/jira/browse/HADOOP-15366) | Add a helper shutdown routine in HadoopExecutor to ensure clean shutdown |  Minor | . | Shashikant Banerjee | Shashikant Banerjee |
| [YARN-7905](https://issues.apache.org/jira/browse/YARN-7905) | Parent directory permission incorrect during public localization |  Critical | . | Bibin A Chundatt | Bilwa S T |
| [HADOOP-15328](https://issues.apache.org/jira/browse/HADOOP-15328) | Fix the typo in HttpAuthentication.md |  Minor | common | fang zhenyi | fang zhenyi |
| [HADOOP-15374](https://issues.apache.org/jira/browse/HADOOP-15374) | Add links of the new features of 3.1.0 to the top page |  Major | documentation | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-7804](https://issues.apache.org/jira/browse/YARN-7804) | Refresh action on Grid view page should not be redirected to graph view |  Major | yarn-ui-v2 | Yesha Vora | Gergely Novák |
| [HDFS-13420](https://issues.apache.org/jira/browse/HDFS-13420) | License header is displayed in ArchivalStorage/MemoryStorage html pages |  Minor | documentation | Akira Ajisaka | Akira Ajisaka |
| [HDFS-13328](https://issues.apache.org/jira/browse/HDFS-13328) | Abstract ReencryptionHandler recursive logic in separate class. |  Major | namenode | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HADOOP-15340](https://issues.apache.org/jira/browse/HADOOP-15340) | Provide meaningful RPC server name for RpcMetrics |  Major | common | Elek, Marton | Elek, Marton |
| [HADOOP-15357](https://issues.apache.org/jira/browse/HADOOP-15357) | Configuration.getPropsWithPrefix no longer does variable substitution |  Major | . | Jim Brennan | Jim Brennan |
| [YARN-7984](https://issues.apache.org/jira/browse/YARN-7984) | Delete registry entries from ZK on ServiceClient stop and clean up stop/destroy behavior |  Critical | yarn-native-services | Billie Rinaldi | Billie Rinaldi |
| [YARN-8133](https://issues.apache.org/jira/browse/YARN-8133) | Doc link broken for yarn-service from overview page. |  Blocker | yarn-native-services | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-8116](https://issues.apache.org/jira/browse/YARN-8116) | Nodemanager fails with NumberFormatException: For input string: "" |  Critical | . | Yesha Vora | Chandni Singh |
| [MAPREDUCE-7062](https://issues.apache.org/jira/browse/MAPREDUCE-7062) | Update mapreduce.job.tags description for making use for ATSv2 purpose. |  Major | . | Charan Hebri | Charan Hebri |
| [YARN-8073](https://issues.apache.org/jira/browse/YARN-8073) | TimelineClientImpl doesn't honor yarn.timeline-service.versions configuration |  Major | . | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-8127](https://issues.apache.org/jira/browse/YARN-8127) | Resource leak when async scheduling is enabled |  Critical | . | Weiwei Yang | Tao Yang |
| [HADOOP-12502](https://issues.apache.org/jira/browse/HADOOP-12502) | SetReplication OutOfMemoryError |  Major | . | Philipp Schuegerl | Vinayakumar B |
| [HDFS-13427](https://issues.apache.org/jira/browse/HDFS-13427) | Fix the section titles of transparent encryption document |  Minor | documentation | Akira Ajisaka | Akira Ajisaka |
| [HDFS-7101](https://issues.apache.org/jira/browse/HDFS-7101) | Potential null dereference in DFSck#doWork() |  Minor | . | Ted Yu | skrho |
| [HDFS-13426](https://issues.apache.org/jira/browse/HDFS-13426) | Fix javadoc in FsDatasetAsyncDiskService#removeVolume |  Minor | hdfs | Shashikant Banerjee | Shashikant Banerjee |
| [YARN-8120](https://issues.apache.org/jira/browse/YARN-8120) | JVM can crash with SIGSEGV when exiting due to custom leveldb logger |  Major | nodemanager, resourcemanager | Jason Lowe | Jason Lowe |
| [YARN-8147](https://issues.apache.org/jira/browse/YARN-8147) | TestClientRMService#testGetApplications sporadically fails |  Major | test | Jason Lowe | Jason Lowe |
| [HDFS-13436](https://issues.apache.org/jira/browse/HDFS-13436) | Fix javadoc of package-info.java |  Major | documentation | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-15379](https://issues.apache.org/jira/browse/HADOOP-15379) | Make IrqHandler.bind() public |  Minor | util | Steve Loughran | Ajay Kumar |
| [YARN-8154](https://issues.apache.org/jira/browse/YARN-8154) | Fix missing titles in PlacementConstraints document |  Minor | documentation | Akira Ajisaka | Weiwei Yang |
| [YARN-8153](https://issues.apache.org/jira/browse/YARN-8153) | Guaranteed containers always stay in SCHEDULED on NM after restart |  Major | . | Yang Wang | Yang Wang |
| [HADOOP-14970](https://issues.apache.org/jira/browse/HADOOP-14970) | MiniHadoopClusterManager doesn't respect lack of format option |  Minor | . | Erik Krogen | Erik Krogen |
| [HDFS-13438](https://issues.apache.org/jira/browse/HDFS-13438) | Fix javadoc in FsVolumeList#removeVolume |  Minor | . | Shashikant Banerjee | Shashikant Banerjee |
| [YARN-8142](https://issues.apache.org/jira/browse/YARN-8142) | yarn service application stops when AM is killed with SIGTERM |  Major | yarn-native-services | Yesha Vora | Billie Rinaldi |
| [MAPREDUCE-7077](https://issues.apache.org/jira/browse/MAPREDUCE-7077) | Pipe mapreduce job fails with Permission denied for jobTokenPassword |  Critical | . | Yesha Vora | Akira Ajisaka |
| [HDFS-13330](https://issues.apache.org/jira/browse/HDFS-13330) | ShortCircuitCache#fetchOrCreate never retries |  Major | . | Wei-Chiu Chuang | Gabor Bota |
| [YARN-8156](https://issues.apache.org/jira/browse/YARN-8156) | Increase the default value of yarn.timeline-service.app-collector.linger-period.ms |  Major | . | Rohith Sharma K S | Charan Hebri |
| [HADOOP-15369](https://issues.apache.org/jira/browse/HADOOP-15369) | Avoid usage of ${project.version} in parent poms |  Major | build | Elek, Marton | Elek, Marton |
| [YARN-8162](https://issues.apache.org/jira/browse/YARN-8162) | Remove Method DirectoryCollection#verifyDirUsingMkdir |  Major | nodemanager | Yufei Gu | Yufei Gu |
| [YARN-7773](https://issues.apache.org/jira/browse/YARN-7773) | YARN Federation used Mysql as state store throw exception, Unknown column 'homeSubCluster' in 'field list' |  Blocker | federation | Yiran Wu | Yiran Wu |
| [YARN-8165](https://issues.apache.org/jira/browse/YARN-8165) | Incorrect queue name logging in AbstractContainerAllocator |  Trivial | capacityscheduler | Weiwei Yang | Weiwei Yang |
| [YARN-8164](https://issues.apache.org/jira/browse/YARN-8164) | Fix a potential NPE in AbstractSchedulerPlanFollower |  Major | . | lujie | lujie |
| [YARN-7088](https://issues.apache.org/jira/browse/YARN-7088) | Add application launch time to Resource Manager REST API |  Major | . | Abdullah Yousufi | Kanwaljeet Sachdev |
| [YARN-8096](https://issues.apache.org/jira/browse/YARN-8096) | Wrong condition in AmIpFilter#getProxyAddresses() to update the proxy IP list |  Major | . | Oleksandr Shevchenko | Oleksandr Shevchenko |
| [HDFS-12828](https://issues.apache.org/jira/browse/HDFS-12828) | OIV ReverseXML Processor fails with escaped characters |  Critical | hdfs | Erik Krogen | Erik Krogen |
| [HADOOP-15391](https://issues.apache.org/jira/browse/HADOOP-15391) | Add missing css file in hadoop-aws, hadoop-aliyun, hadoop-azure and hadoop-azure-datalake modules |  Major | documentation | Yiqun Lin | Yiqun Lin |
| [YARN-8171](https://issues.apache.org/jira/browse/YARN-8171) | [UI2] AM Node link from attempt page should not redirect to new tab |  Major | yarn-ui-v2 | Sunil Govindan | Sunil Govindan |
| [YARN-8145](https://issues.apache.org/jira/browse/YARN-8145) | yarn rmadmin -getGroups doesn't return updated groups for user |  Major | . | Sumana Sathish | Sunil Govindan |
| [HDFS-13463](https://issues.apache.org/jira/browse/HDFS-13463) | Fix javadoc in FsDatasetImpl#checkAndUpdate |  Minor | datanode | Shashikant Banerjee | Shashikant Banerjee |
| [HDFS-13464](https://issues.apache.org/jira/browse/HDFS-13464) | Fix javadoc in FsVolumeList#handleVolumeFailures |  Minor | documentation | Shashikant Banerjee | Shashikant Banerjee |
| [HADOOP-15396](https://issues.apache.org/jira/browse/HADOOP-15396) | Some java source files are executable |  Minor | . | Akira Ajisaka | Shashikant Banerjee |
| [YARN-6827](https://issues.apache.org/jira/browse/YARN-6827) | [ATS1/1.5] NPE exception while publishing recovering applications into ATS during RM restart. |  Major | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-8182](https://issues.apache.org/jira/browse/YARN-8182) | [UI2] Proxy- Clicking on nodes under Nodes HeatMap gives 401 error |  Critical | . | Sumana Sathish | Sunil Govindan |
| [YARN-8189](https://issues.apache.org/jira/browse/YARN-8189) | [UI2] Nodes page column headers are half truncated |  Major | . | Sunil Govindan | Sunil Govindan |
| [YARN-7830](https://issues.apache.org/jira/browse/YARN-7830) | [UI2] Post selecting grid view in Attempt page, attempt info page should also be opened with grid view |  Major | yarn-ui-v2 | Yesha Vora | Gergely Novák |
| [YARN-7786](https://issues.apache.org/jira/browse/YARN-7786) | NullPointerException while launching ApplicationMaster |  Major | . | lujie | lujie |
| [HDFS-10183](https://issues.apache.org/jira/browse/HDFS-10183) | Prevent race condition during class initialization |  Minor | fs | Pavel Avgustinov | Pavel Avgustinov |
| [HDFS-13055](https://issues.apache.org/jira/browse/HDFS-13055) | Aggregate usage statistics from datanodes |  Major | . | Ajay Kumar | Ajay Kumar |
| [HDFS-13388](https://issues.apache.org/jira/browse/HDFS-13388) | RequestHedgingProxyProvider calls multiple configured NNs all the time |  Major | hdfs-client | Jinglun | Jinglun |
| [YARN-7956](https://issues.apache.org/jira/browse/YARN-7956) | [UI2] Avoid duplicating Components link under Services/\<ServiceName\>/Components |  Major | yarn-ui-v2 | Yesha Vora | Yesha Vora |
| [HDFS-13408](https://issues.apache.org/jira/browse/HDFS-13408) | MiniDFSCluster to support being built on randomized base directory |  Major | test | Xiao Liang | Xiao Liang |
| [HDFS-13356](https://issues.apache.org/jira/browse/HDFS-13356) | Balancer:Set default value of minBlockSize to 10mb |  Major | balancer & mover | Bharat Viswanadham | Bharat Viswanadham |
| [HADOOP-15390](https://issues.apache.org/jira/browse/HADOOP-15390) | Yarn RM logs flooded by DelegationTokenRenewer trying to renew KMS tokens |  Critical | . | Xiao Chen | Xiao Chen |
| [HDFS-13336](https://issues.apache.org/jira/browse/HDFS-13336) | Test cases of TestWriteToReplica failed in windows |  Major | . | Xiao Liang | Xiao Liang |
| [YARN-7598](https://issues.apache.org/jira/browse/YARN-7598) | Document how to use classpath isolation for aux-services in YARN |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-8196](https://issues.apache.org/jira/browse/YARN-8196) | yarn.webapp.api-service.enable should be highlighted in the quickstart |  Trivial | documentation | Davide  Vergari | Billie Rinaldi |
| [YARN-8183](https://issues.apache.org/jira/browse/YARN-8183) | Fix ConcurrentModificationException inside RMAppAttemptMetrics#convertAtomicLongMaptoLongMap |  Critical | yarn | Sumana Sathish | Suma Shivaprasad |
| [HADOOP-15402](https://issues.apache.org/jira/browse/HADOOP-15402) | Prevent double logout of UGI's LoginContext |  Major | security | Daryn Sharp | Daryn Sharp |
| [YARN-8188](https://issues.apache.org/jira/browse/YARN-8188) | RM Nodes UI data table index for sorting column need to be corrected post Application tags display |  Major | resourcemanager, webapp | Weiwei Yang | Weiwei Yang |
| [HADOOP-15411](https://issues.apache.org/jira/browse/HADOOP-15411) | AuthenticationFilter should use Configuration.getPropsWithPrefix instead of iterator |  Critical | . | Suma Shivaprasad | Suma Shivaprasad |
| [MAPREDUCE-7042](https://issues.apache.org/jira/browse/MAPREDUCE-7042) | Killed MR job data does not move to mapreduce.jobhistory.done-dir when ATS v2 is enabled |  Major | . | Yesha Vora | Xuan Gong |
| [YARN-8205](https://issues.apache.org/jira/browse/YARN-8205) | Application State is not updated to ATS if AM launching is delayed. |  Critical | . | Sumana Sathish | Rohith Sharma K S |
| [YARN-8004](https://issues.apache.org/jira/browse/YARN-8004) | Add unit tests for inter queue preemption for dominant resource calculator |  Critical | yarn | Sumana Sathish | Zian Chen |
| [YARN-8208](https://issues.apache.org/jira/browse/YARN-8208) | Add log statement for Docker client configuration file at INFO level |  Minor | yarn-native-services | Yesha Vora | Yesha Vora |
| [YARN-8211](https://issues.apache.org/jira/browse/YARN-8211) | Yarn registry dns log finds BufferUnderflowException on port ping |  Major | yarn-native-services | Yesha Vora | Eric Yang |
| [MAPREDUCE-7072](https://issues.apache.org/jira/browse/MAPREDUCE-7072) | mapred job -history prints duplicate counter in human output |  Major | client | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [YARN-8221](https://issues.apache.org/jira/browse/YARN-8221) | RMWebServices also need to honor yarn.resourcemanager.display.per-user-apps |  Major | webapp | Sunil Govindan | Sunil Govindan |
| [YARN-8210](https://issues.apache.org/jira/browse/YARN-8210) | AMRMClient logging on every heartbeat to track updation of AM RM token causes too many log lines to be generated in AM logs |  Major | yarn | Suma Shivaprasad | Suma Shivaprasad |
| [YARN-8005](https://issues.apache.org/jira/browse/YARN-8005) | Add unit tests for queue priority with dominant resource calculator |  Critical | . | Sumana Sathish | Zian Chen |
| [YARN-8225](https://issues.apache.org/jira/browse/YARN-8225) | YARN precommit build failing in TestPlacementConstraintTransformations |  Critical | . | Billie Rinaldi | Shane Kumpf |
| [HDFS-13509](https://issues.apache.org/jira/browse/HDFS-13509) | Bug fix for breakHardlinks() of ReplicaInfo/LocalReplica, and fix TestFileAppend failures on Windows |  Major | . | Xiao Liang | Xiao Liang |
| [YARN-8187](https://issues.apache.org/jira/browse/YARN-8187) | [UI2] Individual Node page does not contain breadcrumb trail |  Critical | yarn-ui-v2 | Sumana Sathish | Zian Chen |
| [YARN-7799](https://issues.apache.org/jira/browse/YARN-7799) | YARN Service dependency follow up work |  Critical | client, resourcemanager | Gour Saha | Billie Rinaldi |
| [MAPREDUCE-7073](https://issues.apache.org/jira/browse/MAPREDUCE-7073) | Optimize TokenCache#obtainTokensForNamenodesInternal |  Major | . | Bibin A Chundatt | Bibin A Chundatt |
| [HADOOP-15406](https://issues.apache.org/jira/browse/HADOOP-15406) | hadoop-nfs dependencies for mockito and junit are not test scope |  Major | nfs | Jason Lowe | Jason Lowe |
| [YARN-6385](https://issues.apache.org/jira/browse/YARN-6385) | Fix checkstyle warnings in TestFileSystemApplicationHistoryStore |  Minor | . | Yiqun Lin | Yiqun Lin |
| [YARN-8222](https://issues.apache.org/jira/browse/YARN-8222) | Fix potential NPE when gets RMApp from RM context |  Critical | . | Tao Yang | Tao Yang |
| [HADOOP-12071](https://issues.apache.org/jira/browse/HADOOP-12071) | conftest is not documented |  Minor | documentation | Kengo Seki | Kengo Seki |
| [YARN-8209](https://issues.apache.org/jira/browse/YARN-8209) | NPE in DeletionService |  Critical | . | Chandni Singh | Eric Badger |
| [HDFS-13481](https://issues.apache.org/jira/browse/HDFS-13481) | TestRollingFileSystemSinkWithHdfs#testFlushThread: test failed intermittently |  Major | hdfs | Gabor Bota | Gabor Bota |
| [HADOOP-15434](https://issues.apache.org/jira/browse/HADOOP-15434) | Upgrade to ADLS SDK that exposes current timeout |  Major | . | Sean Mackrory | Sean Mackrory |
| [YARN-8217](https://issues.apache.org/jira/browse/YARN-8217) | RmAuthenticationFilterInitializer /TimelineAuthenticationFilterInitializer should use Configuration.getPropsWithPrefix instead of iterator |  Major | . | Suma Shivaprasad | Suma Shivaprasad |
| [YARN-7818](https://issues.apache.org/jira/browse/YARN-7818) | Remove privileged operation warnings during container launch for the ContainerRuntimes |  Major | . | Yesha Vora | Shane Kumpf |
| [YARN-8223](https://issues.apache.org/jira/browse/YARN-8223) | ClassNotFoundException when auxiliary service is loaded from HDFS |  Blocker | . | Charan Hebri | Zian Chen |
| [YARN-8079](https://issues.apache.org/jira/browse/YARN-8079) | Support static and archive unmodified local resources in service AM |  Critical | . | Wangda Tan | Suma Shivaprasad |
| [YARN-8025](https://issues.apache.org/jira/browse/YARN-8025) | UsersManangers#getComputedResourceLimitForActiveUsers throws NPE due to preComputedActiveUserLimit is empty |  Major | yarn | Jiandan Yang | Tao Yang |
| [YARN-8251](https://issues.apache.org/jira/browse/YARN-8251) | [UI2] Clicking on Application link at the header goes to Diagnostics Tab instead of AppAttempt Tab |  Major | yarn-ui-v2 | Sumana Sathish | Yesha Vora |
| [YARN-8232](https://issues.apache.org/jira/browse/YARN-8232) | RMContainer lost queue name when RM HA happens |  Major | resourcemanager | Hu Ziqian | Hu Ziqian |
| [YARN-7894](https://issues.apache.org/jira/browse/YARN-7894) | Improve ATS response for DS\_CONTAINER when container launch fails |  Major | timelineserver | Charan Hebri | Chandni Singh |
| [YARN-8264](https://issues.apache.org/jira/browse/YARN-8264) | [UI2 GPU] GPU Info tab disappears if we click any sub link under List of Applications or List of Containers |  Major | . | Sumana Sathish | Sunil Govindan |
| [HDFS-13136](https://issues.apache.org/jira/browse/HDFS-13136) | Avoid taking FSN lock while doing group member lookup for FSD permission check |  Major | namenode | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-13537](https://issues.apache.org/jira/browse/HDFS-13537) | TestHdfsHelper does not generate jceks path properly for relative path in Windows |  Major | . | Xiao Liang | Xiao Liang |
| [MAPREDUCE-7095](https://issues.apache.org/jira/browse/MAPREDUCE-7095) | Race conditions in closing FadvisedChunkedFile |  Minor | . | Miklos Szegedi | Miklos Szegedi |
| [HADOOP-15446](https://issues.apache.org/jira/browse/HADOOP-15446) | WASB: PageBlobInputStream.skip breaks HBASE replication |  Major | fs/azure | Thomas Marquardt | Thomas Marquardt |
| [YARN-7715](https://issues.apache.org/jira/browse/YARN-7715) | Support NM promotion/demotion of running containers. |  Major | . | Arun Suresh | Miklos Szegedi |
| [YARN-7003](https://issues.apache.org/jira/browse/YARN-7003) | DRAINING state of queues is not recovered after RM restart |  Major | capacityscheduler | Tao Yang | Tao Yang |
| [YARN-8268](https://issues.apache.org/jira/browse/YARN-8268) | Fair scheduler: reservable queue is configured both as parent and leaf queue |  Major | fairscheduler | Gergo Repas | Gergo Repas |
| [YARN-8244](https://issues.apache.org/jira/browse/YARN-8244) |  TestContainerSchedulerQueuing.testStartMultipleContainers failed |  Major | . | Miklos Szegedi | Jim Brennan |
| [YARN-8265](https://issues.apache.org/jira/browse/YARN-8265) | Service AM should retrieve new IP for docker container relaunched by NM |  Critical | yarn-native-services | Eric Yang | Billie Rinaldi |
| [YARN-8271](https://issues.apache.org/jira/browse/YARN-8271) | [UI2] Improve labeling of certain tables |  Major | yarn-ui-v2 | Yesha Vora | Yesha Vora |
| [YARN-8288](https://issues.apache.org/jira/browse/YARN-8288) | Fix wrong number of table columns in Resource Model doc |  Major | . | Weiwei Yang | Weiwei Yang |
| [HDFS-13539](https://issues.apache.org/jira/browse/HDFS-13539) | DFSStripedInputStream NPE when reportCheckSumFailure |  Major | . | Xiao Chen | Xiao Chen |
| [YARN-8266](https://issues.apache.org/jira/browse/YARN-8266) | [UI2] Clicking on application from cluster view should redirect to application attempt page |  Major | yarn-ui-v2 | Yesha Vora | Yesha Vora |
| [YARN-8166](https://issues.apache.org/jira/browse/YARN-8166) | [UI2] Service page header links are broken |  Major | yarn-ui-v2 | Yesha Vora | Yesha Vora |
| [YARN-8236](https://issues.apache.org/jira/browse/YARN-8236) | Invalid kerberos principal file name cause NPE in native service |  Critical | yarn-native-services | Sunil Govindan | Gour Saha |
| [YARN-8278](https://issues.apache.org/jira/browse/YARN-8278) | DistributedScheduling is not working in HA |  Blocker | . | Bibin A Chundatt | Bibin A Chundatt |
| [HADOOP-15466](https://issues.apache.org/jira/browse/HADOOP-15466) | Correct units in adl.http.timeout |  Major | fs/adl | Sean Mackrory | Sean Mackrory |
| [YARN-8300](https://issues.apache.org/jira/browse/YARN-8300) | Fix NPE in DefaultUpgradeComponentsFinder |  Major | yarn | Suma Shivaprasad | Suma Shivaprasad |
| [YARN-8071](https://issues.apache.org/jira/browse/YARN-8071) | Add ability to specify nodemanager environment variables individually |  Major | yarn | Jim Brennan | Jim Brennan |
| [HDFS-13581](https://issues.apache.org/jira/browse/HDFS-13581) | DN UI logs link is broken when https is enabled |  Minor | datanode | Namit Maheshwari | Shashikant Banerjee |
| [MAPREDUCE-7094](https://issues.apache.org/jira/browse/MAPREDUCE-7094) | LocalDistributedCacheManager leaves classloaders open, which leaks FDs |  Major | . | Adam Szita | Adam Szita |
| [YARN-8128](https://issues.apache.org/jira/browse/YARN-8128) | Document better the per-node per-app file limit in YARN log aggregation |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-8293](https://issues.apache.org/jira/browse/YARN-8293) | In YARN Services UI, "User Name for service" should be completely removed in secure clusters |  Major | yarn-ui-v2 | Sunil Govindan | Sunil Govindan |
| [YARN-8141](https://issues.apache.org/jira/browse/YARN-8141) | YARN Native Service: Respect YARN\_CONTAINER\_RUNTIME\_DOCKER\_LOCAL\_RESOURCE\_MOUNTS specified in service spec |  Critical | yarn-native-services | Wangda Tan | Chandni Singh |
| [YARN-8296](https://issues.apache.org/jira/browse/YARN-8296) | Update YarnServiceApi documentation and yarn service UI code to remove references to unique\_component\_support |  Major | yarn-native-services, yarn-ui-v2 | Suma Shivaprasad | Suma Shivaprasad |
| [HDFS-13586](https://issues.apache.org/jira/browse/HDFS-13586) | Fsync fails on directories on Windows |  Critical | datanode, hdfs | Lukas Majercak | Lukas Majercak |
| [HDFS-13573](https://issues.apache.org/jira/browse/HDFS-13573) | Javadoc for BlockPlacementPolicyDefault is inaccurate |  Trivial | . | Yiqun Lin | Zsolt Venczel |
| [YARN-8248](https://issues.apache.org/jira/browse/YARN-8248) | Job hangs when a job requests a resource that its queue does not have |  Major | fairscheduler | Szilard Nemeth | Szilard Nemeth |
| [YARN-8179](https://issues.apache.org/jira/browse/YARN-8179) | Preemption does not happen due to natural\_termination\_factor when DRF is used |  Major | . | kyungwan nam | kyungwan nam |
| [HADOOP-15474](https://issues.apache.org/jira/browse/HADOOP-15474) | Rename properties introduced for \<tags\> |  Major | conf | Nanda kumar | Zsolt Venczel |
| [HADOOP-15450](https://issues.apache.org/jira/browse/HADOOP-15450) | Avoid fsync storm triggered by DiskChecker and handle disk full situation |  Blocker | . | Kihwal Lee | Arpit Agarwal |
| [YARN-8290](https://issues.apache.org/jira/browse/YARN-8290) | SystemMetricsPublisher.appACLsUpdated should be invoked after application information is published to ATS to avoid "User is not set in the application report" Exception |  Critical | . | Yesha Vora | Eric Yang |
| [YARN-8332](https://issues.apache.org/jira/browse/YARN-8332) | Incorrect min/max allocation property name in resource types doc |  Critical | documentation | Weiwei Yang | Weiwei Yang |
| [YARN-8273](https://issues.apache.org/jira/browse/YARN-8273) | Log aggregation does not warn if HDFS quota in target directory is exceeded |  Major | log-aggregation | Gergo Repas | Gergo Repas |
| [HDFS-13601](https://issues.apache.org/jira/browse/HDFS-13601) | Optimize ByteString conversions in PBHelper |  Major | . | Andrew Wang | Andrew Wang |
| [HDFS-13540](https://issues.apache.org/jira/browse/HDFS-13540) | DFSStripedInputStream should only allocate new buffers when reading |  Major | . | Xiao Chen | Xiao Chen |
| [YARN-8297](https://issues.apache.org/jira/browse/YARN-8297) | Incorrect ATS Url used for Wire encrypted cluster |  Blocker | yarn-ui-v2 | Yesha Vora | Sunil Govindan |
| [HDFS-13588](https://issues.apache.org/jira/browse/HDFS-13588) | Fix TestFsDatasetImpl test failures on Windows |  Major | . | Xiao Liang | Xiao Liang |
| [YARN-8310](https://issues.apache.org/jira/browse/YARN-8310) | Handle old NMTokenIdentifier, AMRMTokenIdentifier, and ContainerTokenIdentifier formats |  Major | . | Robert Kanter | Robert Kanter |
| [YARN-8336](https://issues.apache.org/jira/browse/YARN-8336) | Fix potential connection leak in SchedConfCLI and YarnWebServiceUtils |  Major | . | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [YARN-8344](https://issues.apache.org/jira/browse/YARN-8344) | Missing nm.stop() in TestNodeManagerResync to fix testKillContainersOnResync |  Major | . | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [YARN-8348](https://issues.apache.org/jira/browse/YARN-8348) | Incorrect and missing AfterClass in HBase-tests to fix NPE failures |  Major | . | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [YARN-8327](https://issues.apache.org/jira/browse/YARN-8327) | Fix TestAggregatedLogFormat#testReadAcontainerLogs1 on Windows |  Major | log-aggregation | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [YARN-8346](https://issues.apache.org/jira/browse/YARN-8346) | Upgrading to 3.1 kills running containers with error "Opportunistic container queue is full" |  Blocker | . | Rohith Sharma K S | Jason Lowe |
| [HDFS-13611](https://issues.apache.org/jira/browse/HDFS-13611) | Unsafe use of Text as a ConcurrentHashMap key in PBHelperClient |  Major | . | Andrew Wang | Andrew Wang |
| [YARN-8316](https://issues.apache.org/jira/browse/YARN-8316) | Diagnostic message should improve when yarn service fails to launch due to ATS unavailability |  Major | yarn-native-services | Yesha Vora | Billie Rinaldi |
| [YARN-8357](https://issues.apache.org/jira/browse/YARN-8357) | Yarn Service: NPE when service is saved first and then started. |  Critical | . | Chandni Singh | Chandni Singh |
| [HDFS-13618](https://issues.apache.org/jira/browse/HDFS-13618) | Fix TestDataNodeFaultInjector test failures on Windows |  Major | test | Xiao Liang | Xiao Liang |
| [HADOOP-15473](https://issues.apache.org/jira/browse/HADOOP-15473) | Configure serialFilter in KeyProvider to avoid UnrecoverableKeyException caused by JDK-8189997 |  Critical | kms | Gabor Bota | Gabor Bota |
| [YARN-8292](https://issues.apache.org/jira/browse/YARN-8292) | Fix the dominant resource preemption cannot happen when some of the resource vector becomes negative |  Critical | yarn | Sumana Sathish | Wangda Tan |
| [HADOOP-15455](https://issues.apache.org/jira/browse/HADOOP-15455) | Incorrect debug message in KMSACL#hasAccess |  Trivial | . | Wei-Chiu Chuang | Yuen-Kuei Hsueh |
| [YARN-8338](https://issues.apache.org/jira/browse/YARN-8338) | TimelineService V1.5 doesn't come up after HADOOP-15406 |  Critical | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [YARN-8339](https://issues.apache.org/jira/browse/YARN-8339) | Service AM should localize static/archive resource types to container working directory instead of 'resources' |  Critical | yarn-native-services | Suma Shivaprasad | Suma Shivaprasad |
| [YARN-8369](https://issues.apache.org/jira/browse/YARN-8369) | Javadoc build failed due to "bad use of '\>'" |  Critical | build, docs | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-8362](https://issues.apache.org/jira/browse/YARN-8362) | Number of remaining retries are updated twice after a container failure in NM |  Critical | . | Chandni Singh | Chandni Singh |
| [HDFS-13626](https://issues.apache.org/jira/browse/HDFS-13626) | Fix incorrect username when deny the setOwner operation |  Minor | namenode | luhuachao | Zsolt Venczel |
| [YARN-8377](https://issues.apache.org/jira/browse/YARN-8377) | Javadoc build failed in hadoop-yarn-server-nodemanager |  Critical | build, docs | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-8368](https://issues.apache.org/jira/browse/YARN-8368) | yarn app start cli should print applicationId |  Critical | . | Yesha Vora | Rohith Sharma K S |
| [YARN-8350](https://issues.apache.org/jira/browse/YARN-8350) | NPE in service AM related to placement policy |  Critical | yarn-native-services | Billie Rinaldi | Gour Saha |
| [YARN-8367](https://issues.apache.org/jira/browse/YARN-8367) | Fix NPE in SingleConstraintAppPlacementAllocator when placement constraint in SchedulingRequest is null |  Major | scheduler | Gour Saha | Weiwei Yang |
| [HADOOP-15490](https://issues.apache.org/jira/browse/HADOOP-15490) | Multiple declaration of maven-enforcer-plugin found in pom.xml |  Minor | . | Nanda kumar | Nanda kumar |
| [HDFS-13646](https://issues.apache.org/jira/browse/HDFS-13646) | DFSAdmin doesn't display specialized help for triggerBlockReport |  Major | tools | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-8197](https://issues.apache.org/jira/browse/YARN-8197) | Tracking URL in the app state does not get redirected to MR ApplicationMaster for Running applications |  Critical | yarn | Sumana Sathish | Sunil Govindan |
| [YARN-8308](https://issues.apache.org/jira/browse/YARN-8308) | Yarn service app fails due to issues with Renew Token |  Major | yarn-native-services | Yesha Vora | Gour Saha |
| [YARN-7340](https://issues.apache.org/jira/browse/YARN-7340) | Fix the missing time stamp in exception message in Class NoOverCommitPolicy |  Minor | reservation system | Yufei Gu | Dinesh Chitlangia |
| [HDFS-13636](https://issues.apache.org/jira/browse/HDFS-13636) | Cross-Site Scripting vulnerability in HttpServer2 |  Major | . | Haibo Yan | Haibo Yan |
| [YARN-7962](https://issues.apache.org/jira/browse/YARN-7962) | Race Condition When Stopping DelegationTokenRenewer causes RM crash during failover |  Critical | resourcemanager | BELUGA BEHR | BELUGA BEHR |
| [YARN-8372](https://issues.apache.org/jira/browse/YARN-8372) | Distributed shell app master should not release containers when shutdown if keep-container is true |  Critical | distributed-shell | Charan Hebri | Suma Shivaprasad |
| [YARN-8375](https://issues.apache.org/jira/browse/YARN-8375) | TestCGroupElasticMemoryController fails surefire build |  Major | . | Jason Lowe | Miklos Szegedi |
| [YARN-8319](https://issues.apache.org/jira/browse/YARN-8319) | More YARN pages need to honor yarn.resourcemanager.display.per-user-apps |  Major | webapp | Vinod Kumar Vavilapalli | Sunil Govindan |
| [MAPREDUCE-7097](https://issues.apache.org/jira/browse/MAPREDUCE-7097) | MapReduce JHS should honor yarn.webapp.filter-entity-list-by-user |  Major | . | Vinod Kumar Vavilapalli | Sunil Govindan |
| [YARN-8276](https://issues.apache.org/jira/browse/YARN-8276) | [UI2] After version field became mandatory, form-based submission of new YARN service doesn't work |  Critical | yarn-ui-v2 | Gergely Novák | Gergely Novák |
| [HDFS-13339](https://issues.apache.org/jira/browse/HDFS-13339) | Volume reference can't be released and may lead to deadlock when DataXceiver does a check volume |  Critical | datanode | liaoyuxiangqin | Zsolt Venczel |
| [YARN-8390](https://issues.apache.org/jira/browse/YARN-8390) | Fix API incompatible changes in FairScheduler's AllocationFileLoaderService |  Major | fairscheduler | Gergo Repas | Gergo Repas |
| [YARN-8382](https://issues.apache.org/jira/browse/YARN-8382) | cgroup file leak in NM |  Major | nodemanager | Hu Ziqian | Hu Ziqian |
| [YARN-8365](https://issues.apache.org/jira/browse/YARN-8365) | Revisit the record type used by Registry DNS for upstream resolution |  Major | yarn-native-services | Shane Kumpf | Shane Kumpf |
| [HDFS-13545](https://issues.apache.org/jira/browse/HDFS-13545) |  "guarded" is misspelled as "gaurded" in FSPermissionChecker.java |  Trivial | documentation | Jianchao Jia | Jianchao Jia |
| [YARN-8396](https://issues.apache.org/jira/browse/YARN-8396) | Click on an individual container continuously spins and doesn't load the page |  Blocker | . | Charan Hebri | Sunil Govindan |
| [MAPREDUCE-7103](https://issues.apache.org/jira/browse/MAPREDUCE-7103) | Fix TestHistoryViewerPrinter on windows due to a mismatch line separator |  Minor | . | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [HADOOP-15217](https://issues.apache.org/jira/browse/HADOOP-15217) | FsUrlConnection does not handle paths with spaces |  Major | fs | Joseph Fourny | Zsolt Venczel |
| [HDFS-12950](https://issues.apache.org/jira/browse/HDFS-12950) | [oiv] ls will fail in  secure cluster |  Major | . | Brahma Reddy Battula | Wei-Chiu Chuang |
| [YARN-8386](https://issues.apache.org/jira/browse/YARN-8386) |  App log can not be viewed from Logs tab in secure cluster |  Critical | yarn-ui-v2 | Yesha Vora | Sunil Govindan |
| [HDFS-13615](https://issues.apache.org/jira/browse/HDFS-13615) | libhdfs++ SaslProtocol hanging while accessing invalid lock |  Major | . | Mitchell Tracy | Mitchell Tracy |
| [YARN-8359](https://issues.apache.org/jira/browse/YARN-8359) | Exclude containermanager.linux test classes on Windows |  Major | . | Giovanni Matteo Fumarola | Jason Lowe |
| [HDFS-13642](https://issues.apache.org/jira/browse/HDFS-13642) | Creating a file with block size smaller than EC policy's cell size should fail |  Major | erasure-coding | Xiao Chen | Xiao Chen |
| [HDFS-13664](https://issues.apache.org/jira/browse/HDFS-13664) | Refactor ConfiguredFailoverProxyProvider to make inheritance easier |  Minor | hdfs-client | Chao Sun | Chao Sun |
| [HDFS-12670](https://issues.apache.org/jira/browse/HDFS-12670) | can't renew HDFS tokens with only the hdfs client jar |  Critical | . | Thomas Graves | Arpit Agarwal |
| [HDFS-13667](https://issues.apache.org/jira/browse/HDFS-13667) | Typo: Marking all "datandoes" as stale |  Trivial | namenode | Wei-Chiu Chuang | Nanda kumar |
| [YARN-8323](https://issues.apache.org/jira/browse/YARN-8323) | FairScheduler.allocConf should be declared as volatile |  Major | fairscheduler | Haibo Chen | Szilard Nemeth |
| [YARN-8413](https://issues.apache.org/jira/browse/YARN-8413) | Flow activity page is failing with "Timeline server failed with an error" |  Major | yarn-ui-v2 | Yesha Vora | Sunil Govindan |
| [YARN-8405](https://issues.apache.org/jira/browse/YARN-8405) | RM zk-state-store.parent-path ACLs has been changed since HADOOP-14773 |  Major | . | Rohith Sharma K S | Íñigo Goiri |
| [YARN-8419](https://issues.apache.org/jira/browse/YARN-8419) | [UI2] User cannot submit a new service as submit button is always disabled |  Major | . | Suma Shivaprasad | Suma Shivaprasad |
| [HADOOP-15307](https://issues.apache.org/jira/browse/HADOOP-15307) | NFS: flavor AUTH\_SYS should use VerifierNone |  Major | nfs | Wei-Chiu Chuang | Gabor Bota |
| [MAPREDUCE-7108](https://issues.apache.org/jira/browse/MAPREDUCE-7108) | TestFileOutputCommitter fails on Windows |  Minor | test | Zuoming Zhang | Zuoming Zhang |
| [MAPREDUCE-7101](https://issues.apache.org/jira/browse/MAPREDUCE-7101) | Add config parameter to allow JHS to alway scan user dir irrespective of modTime |  Critical | . | Wangda Tan | Thomas Marquardt |
| [HADOOP-15527](https://issues.apache.org/jira/browse/HADOOP-15527) | loop until TIMEOUT before sending kill -9 |  Major | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [YARN-8404](https://issues.apache.org/jira/browse/YARN-8404) | Timeline event publish need to be async to avoid Dispatcher thread leak in case ATS is down |  Blocker | . | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-8410](https://issues.apache.org/jira/browse/YARN-8410) | Registry DNS lookup fails to return for CNAMEs |  Major | yarn-native-services | Shane Kumpf | Shane Kumpf |
| [YARN-8426](https://issues.apache.org/jira/browse/YARN-8426) | Upgrade jquery-ui to 1.12.1 in YARN |  Major | webapp | Sunil Govindan | Sunil Govindan |
| [HDFS-13675](https://issues.apache.org/jira/browse/HDFS-13675) | Speed up TestDFSAdminWithHA |  Major | hdfs, namenode | Lukas Majercak | Lukas Majercak |
| [HDFS-13679](https://issues.apache.org/jira/browse/HDFS-13679) | Fix Typo in javadoc for ScanInfoPerBlockPool#addAll |  Minor | . | Shashikant Banerjee | Shashikant Banerjee |
| [HDFS-13673](https://issues.apache.org/jira/browse/HDFS-13673) | TestNameNodeMetrics fails on Windows |  Minor | test | Zuoming Zhang | Zuoming Zhang |
| [HDFS-13676](https://issues.apache.org/jira/browse/HDFS-13676) | TestEditLogRace fails on Windows |  Minor | test | Zuoming Zhang | Zuoming Zhang |
| [HDFS-13174](https://issues.apache.org/jira/browse/HDFS-13174) | hdfs mover -p /path times out after 20 min |  Major | balancer & mover | Istvan Fajth | Istvan Fajth |
| [HADOOP-15504](https://issues.apache.org/jira/browse/HADOOP-15504) | Upgrade Maven and Maven Wagon versions |  Major | build | Sean Mackrory | Sean Mackrory |
| [HADOOP-15523](https://issues.apache.org/jira/browse/HADOOP-15523) | Shell command timeout given is in seconds whereas it is taken as millisec while scheduling |  Major | . | Bilwa S T | Bilwa S T |
| [YARN-8437](https://issues.apache.org/jira/browse/YARN-8437) | Build oom-listener fails on older versions |  Major | . | Miklos Szegedi | Miklos Szegedi |
| [YARN-8391](https://issues.apache.org/jira/browse/YARN-8391) | Investigate AllocationFileLoaderService.reloadListener locking issue |  Critical | fairscheduler | Haibo Chen | Szilard Nemeth |
| [HDFS-13682](https://issues.apache.org/jira/browse/HDFS-13682) | Cannot create encryption zone after KMS auth token expires |  Critical | encryption, kms, namenode | Xiao Chen | Xiao Chen |
| [HADOOP-15549](https://issues.apache.org/jira/browse/HADOOP-15549) | Upgrade to commons-configuration 2.1 regresses task CPU consumption |  Major | metrics | Todd Lipcon | Todd Lipcon |
| [YARN-8444](https://issues.apache.org/jira/browse/YARN-8444) | NodeResourceMonitor crashes on bad swapFree value |  Major | . | Jim Brennan | Jim Brennan |
| [MAPREDUCE-7114](https://issues.apache.org/jira/browse/MAPREDUCE-7114) | Make FrameworkUploader symlink ignore improvement |  Major | . | Gergo Repas | Gergo Repas |
| [YARN-8184](https://issues.apache.org/jira/browse/YARN-8184) | Too many metrics if containerLocalizer/ResourceLocalizationService uses ReadWriteDiskValidator |  Major | nodemanager | Yufei Gu | Yufei Gu |
| [YARN-8326](https://issues.apache.org/jira/browse/YARN-8326) | Yarn 3.0 seems runs slower than Yarn 2.6 |  Major | yarn | Hsin-Liang Huang | Shane Kumpf |
| [YARN-8443](https://issues.apache.org/jira/browse/YARN-8443) | Total #VCores in cluster metrics is wrong when CapacityScheduler reserved some containers |  Major | webapp | Tao Yang | Tao Yang |
| [YARN-8457](https://issues.apache.org/jira/browse/YARN-8457) | Compilation is broken with -Pyarn-ui |  Major | webapp | Sunil Govindan | Sunil Govindan |
| [HADOOP-15550](https://issues.apache.org/jira/browse/HADOOP-15550) | Avoid static initialization of ObjectMappers |  Minor | performance | Todd Lipcon | Todd Lipcon |
| [YARN-8438](https://issues.apache.org/jira/browse/YARN-8438) | TestContainer.testKillOnNew flaky on trunk |  Major | nodemanager | Szilard Nemeth | Szilard Nemeth |
| [YARN-8464](https://issues.apache.org/jira/browse/YARN-8464) | Async scheduling thread could be interrupted when there are no NodeManagers in cluster |  Blocker | capacity scheduler | Charan Hebri | Sunil Govindan |
| [YARN-8423](https://issues.apache.org/jira/browse/YARN-8423) | GPU does not get released even though the application gets killed. |  Critical | yarn | Sumana Sathish | Sunil Govindan |
| [YARN-8401](https://issues.apache.org/jira/browse/YARN-8401) | [UI2] new ui is not accessible with out internet connection |  Blocker | . | Bibin A Chundatt | Bibin A Chundatt |
| [HDFS-13705](https://issues.apache.org/jira/browse/HDFS-13705) | The native ISA-L library loading failure should be made warning rather than an error message |  Minor | erasure-coding | Nilotpal Nandi | Shashikant Banerjee |
| [YARN-8409](https://issues.apache.org/jira/browse/YARN-8409) | ActiveStandbyElectorBasedElectorService is failing with NPE |  Major | . | Yesha Vora | Chandni Singh |
| [YARN-8379](https://issues.apache.org/jira/browse/YARN-8379) | Improve balancing resources in already satisfied queues by using Capacity Scheduler preemption |  Major | . | Wangda Tan | Zian Chen |
| [YARN-8455](https://issues.apache.org/jira/browse/YARN-8455) | Add basic ACL check for all ATS v2 REST APIs |  Major | . | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-8469](https://issues.apache.org/jira/browse/YARN-8469) | [UI2] URL needs to be trimmed to handle index.html redirection while accessing via knox |  Major | yarn-ui-v2 | Sunil Govindan | Sunil Govindan |
| [YARN-8451](https://issues.apache.org/jira/browse/YARN-8451) | Multiple NM heartbeat thread created when a slow NM resync with RM |  Major | nodemanager | Botong Huang | Botong Huang |
| [HADOOP-15548](https://issues.apache.org/jira/browse/HADOOP-15548) | Randomize local dirs |  Minor | . | Jim Brennan | Jim Brennan |
| [HDFS-13707](https://issues.apache.org/jira/browse/HDFS-13707) | [PROVIDED Storage] Fix failing integration tests in ITestProvidedImplementation |  Major | . | Virajith Jalaparti | Virajith Jalaparti |
| [HADOOP-15574](https://issues.apache.org/jira/browse/HADOOP-15574) | Suppress build error if there are no docs after excluding private annotations |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-13702](https://issues.apache.org/jira/browse/HDFS-13702) | Remove HTrace hooks from DFSClient to reduce CPU usage |  Major | performance | Todd Lipcon | Todd Lipcon |
| [HDFS-13635](https://issues.apache.org/jira/browse/HDFS-13635) | Incorrect message when block is not found |  Major | datanode | Wei-Chiu Chuang | Gabor Bota |
| [YARN-8415](https://issues.apache.org/jira/browse/YARN-8415) | TimelineWebServices.getEntity should throw ForbiddenException instead of 404 when ACL checks fail |  Major | . | Sumana Sathish | Suma Shivaprasad |
| [HDFS-13715](https://issues.apache.org/jira/browse/HDFS-13715) | diskbalancer does not work if one of the blockpools are empty on a Federated cluster |  Major | diskbalancer | Namit Maheshwari | Bharat Viswanadham |
| [YARN-8459](https://issues.apache.org/jira/browse/YARN-8459) | Improve Capacity Scheduler logs to debug invalid states |  Major | capacity scheduler | Wangda Tan | Wangda Tan |
| [YARN-7451](https://issues.apache.org/jira/browse/YARN-7451) | Add missing tests to verify the presence of custom resources of RM apps and scheduler webservice endpoints |  Major | resourcemanager, restapi | Grant Sohn | Szilard Nemeth |
| [YARN-8435](https://issues.apache.org/jira/browse/YARN-8435) | Fix NPE when the same client simultaneously contact for the first time Yarn Router |  Critical | router | rangjiaheng | rangjiaheng |
| [HADOOP-15571](https://issues.apache.org/jira/browse/HADOOP-15571) | Multiple FileContexts created with the same configuration object should be allowed to have different umask |  Critical | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [YARN-8492](https://issues.apache.org/jira/browse/YARN-8492) | ATSv2 HBase tests are failing with ClassNotFoundException |  Major | . | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-13121](https://issues.apache.org/jira/browse/HDFS-13121) | NPE when request file descriptors when SC read |  Minor | hdfs-client | Gang Xie | Zsolt Venczel |
| [HDFS-13721](https://issues.apache.org/jira/browse/HDFS-13721) | NPE in DataNode due to uninitialized DiskBalancer |  Major | datanode, diskbalancer | Xiao Chen | Xiao Chen |
| [YARN-6265](https://issues.apache.org/jira/browse/YARN-6265) | yarn.resourcemanager.fail-fast is used inconsistently |  Major | resourcemanager | Daniel Templeton | Yuanbo Liu |
| [HDFS-13722](https://issues.apache.org/jira/browse/HDFS-13722) | HDFS Native Client Fails Compilation on Ubuntu 18.04 |  Minor | . | Jack Bearden | Jack Bearden |
| [YARN-8473](https://issues.apache.org/jira/browse/YARN-8473) | Containers being launched as app tears down can leave containers in NEW state |  Major | nodemanager | Jason Lowe | Jason Lowe |
| [YARN-8512](https://issues.apache.org/jira/browse/YARN-8512) | ATSv2 entities are not published to HBase from second attempt onwards |  Major | . | Yesha Vora | Rohith Sharma K S |
| [YARN-8491](https://issues.apache.org/jira/browse/YARN-8491) | TestServiceCLI#testEnableFastLaunch fail when umask is 077 |  Major | . | K G Bakthavachalam | K G Bakthavachalam |
| [HADOOP-15594](https://issues.apache.org/jira/browse/HADOOP-15594) | Exclude commons-lang3 from hadoop-client-minicluster |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-13723](https://issues.apache.org/jira/browse/HDFS-13723) | Occasional "Should be different group" error in TestRefreshUserMappings#testGroupMappingRefresh |  Major | security, test | Siyao Meng | Siyao Meng |
| [HDFS-12837](https://issues.apache.org/jira/browse/HDFS-12837) | Intermittent failure in TestReencryptionWithKMS |  Major | encryption, test | Surendra Singh Lilhore | Xiao Chen |
| [HADOOP-15316](https://issues.apache.org/jira/browse/HADOOP-15316) | GenericTestUtils can exceed maxSleepTime |  Trivial | . | Sean Mackrory | Adam Antal |
| [HDFS-13729](https://issues.apache.org/jira/browse/HDFS-13729) | Fix broken links to RBF documentation |  Minor | documentation | jwhitter | Gabor Bota |
| [YARN-8518](https://issues.apache.org/jira/browse/YARN-8518) | test-container-executor test\_is\_empty() is broken |  Major | . | Jim Brennan | Jim Brennan |
| [HDFS-13663](https://issues.apache.org/jira/browse/HDFS-13663) | Should throw exception when incorrect block size is set |  Major | . | Yongjun Zhang | Shweta |
| [YARN-8515](https://issues.apache.org/jira/browse/YARN-8515) | container-executor can crash with SIGPIPE after nodemanager restart |  Major | . | Jim Brennan | Jim Brennan |
| [YARN-8421](https://issues.apache.org/jira/browse/YARN-8421) | when moving app, activeUsers is increased, even though app does not have outstanding request |  Major | . | kyungwan nam |  |
| [YARN-8511](https://issues.apache.org/jira/browse/YARN-8511) | When AM releases a container, RM removes allocation tags before it is released by NM |  Major | capacity scheduler | Weiwei Yang | Weiwei Yang |
| [HDFS-13524](https://issues.apache.org/jira/browse/HDFS-13524) | Occasional "All datanodes are bad" error in TestLargeBlock#testLargeBlockSize |  Major | . | Wei-Chiu Chuang | Siyao Meng |
| [YARN-8538](https://issues.apache.org/jira/browse/YARN-8538) | Fix valgrind leak check on container executor |  Major | . | Billie Rinaldi | Billie Rinaldi |
| [HDFS-13485](https://issues.apache.org/jira/browse/HDFS-13485) | DataNode WebHDFS endpoint throws NPE |  Minor | datanode, webhdfs | Wei-Chiu Chuang | Siyao Meng |
| [HADOOP-15610](https://issues.apache.org/jira/browse/HADOOP-15610) | Hadoop Docker Image Pip Install Fails |  Critical | . | Jack Bearden | Jack Bearden |
| [HADOOP-15614](https://issues.apache.org/jira/browse/HADOOP-15614) | TestGroupsCaching.testExceptionOnBackgroundRefreshHandled reliably fails |  Major | . | Kihwal Lee | Weiwei Yang |
| [YARN-8436](https://issues.apache.org/jira/browse/YARN-8436) | FSParentQueue: Comparison method violates its general contract |  Minor | fairscheduler | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [MAPREDUCE-7118](https://issues.apache.org/jira/browse/MAPREDUCE-7118) | Distributed cache conflicts breaks backwards compatability |  Blocker | mrv2 | Jason Lowe | Jason Lowe |
| [YARN-8528](https://issues.apache.org/jira/browse/YARN-8528) | Final states in ContainerAllocation might be modified externally causing unexpected allocation results |  Major | capacity scheduler | Xintong Song | Xintong Song |
| [YARN-6964](https://issues.apache.org/jira/browse/YARN-6964) | Fair scheduler misuses Resources operations |  Major | fairscheduler | Daniel Templeton | Szilard Nemeth |
| [YARN-8360](https://issues.apache.org/jira/browse/YARN-8360) | Yarn service conflict between restart policy and NM configuration |  Critical | yarn | Chandni Singh | Suma Shivaprasad |
| [YARN-8380](https://issues.apache.org/jira/browse/YARN-8380) | Support bind propagation options for mounts in docker runtime |  Major | . | Billie Rinaldi | Billie Rinaldi |
| [YARN-8544](https://issues.apache.org/jira/browse/YARN-8544) | [DS] AM registration fails when hadoop authorization is enabled |  Blocker | . | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-8548](https://issues.apache.org/jira/browse/YARN-8548) | AllocationRespose proto setNMToken initBuilder not done |  Major | . | Bibin A Chundatt | Bilwa S T |
| [YARN-7748](https://issues.apache.org/jira/browse/YARN-7748) | TestContainerResizing.testIncreaseContainerUnreservedWhenApplicationCompleted fails due to multiple container fail events |  Major | capacityscheduler | Haibo Chen | Weiwei Yang |
| [YARN-8541](https://issues.apache.org/jira/browse/YARN-8541) | RM startup failure on recovery after user deletion |  Blocker | resourcemanager | yimeng | Bibin A Chundatt |
| [YARN-8577](https://issues.apache.org/jira/browse/YARN-8577) | Fix the broken anchor in SLS site-doc |  Minor | documentation | Weiwei Yang | Weiwei Yang |
| [HADOOP-15395](https://issues.apache.org/jira/browse/HADOOP-15395) | DefaultImpersonationProvider fails to parse proxy user config if username has . in it |  Major | . | Ajay Kumar | Ajay Kumar |
| [YARN-4606](https://issues.apache.org/jira/browse/YARN-4606) | CapacityScheduler: applications could get starved because computation of #activeUsers considers pending apps |  Critical | capacity scheduler, capacityscheduler | Karam Singh | Manikandan R |
| [YARN-8330](https://issues.apache.org/jira/browse/YARN-8330) | Avoid publishing reserved container to ATS from RM |  Critical | yarn-native-services | Yesha Vora | Suma Shivaprasad |
| [HDFS-13622](https://issues.apache.org/jira/browse/HDFS-13622) | mkdir should print the parent directory in the error message when parent directories do not exist |  Major | . | Zoltan Haindrich | Shweta |
| [HADOOP-15593](https://issues.apache.org/jira/browse/HADOOP-15593) | UserGroupInformation TGT renewer throws NPE |  Blocker | security | Wei-Chiu Chuang | Gabor Bota |
| [YARN-8429](https://issues.apache.org/jira/browse/YARN-8429) | Improve diagnostic message when artifact is not set properly |  Major | . | Yesha Vora | Gour Saha |
| [HDFS-13765](https://issues.apache.org/jira/browse/HDFS-13765) | Fix javadoc for FSDirMkdirOp#createParentDirectories |  Minor | documentation | Lokesh Jain | Lokesh Jain |
| [YARN-8571](https://issues.apache.org/jira/browse/YARN-8571) | Validate service principal format prior to launching yarn service |  Major | security, yarn | Eric Yang | Eric Yang |
| [YARN-8596](https://issues.apache.org/jira/browse/YARN-8596) | Allow SQLFederationStateStore to submit the same app in the same subcluster |  Major | federation | Íñigo Goiri | Giovanni Matteo Fumarola |
| [YARN-8508](https://issues.apache.org/jira/browse/YARN-8508) | On NodeManager container gets cleaned up before its pid file is created |  Critical | . | Sumana Sathish | Chandni Singh |
| [YARN-8434](https://issues.apache.org/jira/browse/YARN-8434) | Update federation documentation of Nodemanager configurations |  Minor | . | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-8591](https://issues.apache.org/jira/browse/YARN-8591) | [ATSv2] NPE while checking for entity acl in non-secure cluster |  Major | timelinereader, timelineserver | Akhil PB | Rohith Sharma K S |
| [YARN-8558](https://issues.apache.org/jira/browse/YARN-8558) | NM recovery level db not cleaned up properly on container finish |  Critical | . | Bibin A Chundatt | Bibin A Chundatt |
| [HADOOP-15637](https://issues.apache.org/jira/browse/HADOOP-15637) | LocalFs#listLocatedStatus does not filter out hidden .crc files |  Minor | fs | Erik Krogen | Erik Krogen |
| [YARN-8605](https://issues.apache.org/jira/browse/YARN-8605) | TestDominantResourceFairnessPolicy.testModWhileSorting is flaky |  Minor | fairscheduler | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [YARN-8418](https://issues.apache.org/jira/browse/YARN-8418) | App local logs could leaked if log aggregation fails to initialize for the app |  Critical | . | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-8579](https://issues.apache.org/jira/browse/YARN-8579) | New AM attempt could not retrieve previous attempt component data |  Critical | . | Yesha Vora | Gour Saha |
| [HDFS-13322](https://issues.apache.org/jira/browse/HDFS-13322) | fuse dfs - uid persists when switching between ticket caches |  Minor | fuse-dfs | Alex Volskiy | Istvan Fajth |
| [YARN-8397](https://issues.apache.org/jira/browse/YARN-8397) | Potential thread leak in ActivitiesManager |  Major | . | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-8522](https://issues.apache.org/jira/browse/YARN-8522) | Application fails with InvalidResourceRequestException |  Critical | . | Yesha Vora | Zian Chen |
| [YARN-8606](https://issues.apache.org/jira/browse/YARN-8606) | Opportunistic scheduling does not work post RM failover |  Blocker | . | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-8595](https://issues.apache.org/jira/browse/YARN-8595) | [UI2] Container diagnostic information is missing from container page |  Major | yarn-ui-v2 | Akhil PB | Akhil PB |
| [YARN-8403](https://issues.apache.org/jira/browse/YARN-8403) | Nodemanager logs failed to download file with INFO level |  Major | yarn | Eric Yang | Eric Yang |
| [YARN-8600](https://issues.apache.org/jira/browse/YARN-8600) | RegistryDNS hang when remote lookup does not reply |  Critical | yarn | Eric Yang | Eric Yang |
| [YARN-8610](https://issues.apache.org/jira/browse/YARN-8610) | Yarn Service Upgrade: Typo in Error message |  Major | . | Chandni Singh | Chandni Singh |
| [YARN-8593](https://issues.apache.org/jira/browse/YARN-8593) | Add RM web service endpoint to get user information |  Major | resourcemanager | Akhil PB | Akhil PB |
| [YARN-8594](https://issues.apache.org/jira/browse/YARN-8594) | [UI2] Display current logged in user |  Major | . | Akhil PB | Akhil PB |
| [YARN-8592](https://issues.apache.org/jira/browse/YARN-8592) | [UI2] rmip:port/ui2 endpoint shows a blank page in windows OS and Chrome browser |  Major | . | Akhil S Naik | Akhil PB |
| [YARN-8318](https://issues.apache.org/jira/browse/YARN-8318) | [UI2] IP address in component page shows N/A |  Major | yarn-ui-v2 | Yesha Vora | Yesha Vora |
| [YARN-6966](https://issues.apache.org/jira/browse/YARN-6966) | NodeManager metrics may return wrong negative values when NM restart |  Major | . | Yang Wang | Szilard Nemeth |
| [YARN-8603](https://issues.apache.org/jira/browse/YARN-8603) | [UI2] Latest run application should be listed first in the RM UI |  Major | yarn-ui-v2 | Sumana Sathish | Akhil PB |
| [YARN-8608](https://issues.apache.org/jira/browse/YARN-8608) | [UI2] No information available per application appAttempt about 'Total Outstanding Resource Requests' |  Major | yarn-ui-v2 | Sumana Sathish | Akhil PB |
| [YARN-8620](https://issues.apache.org/jira/browse/YARN-8620) | [UI2] YARN Services UI new submission failures are not debuggable |  Major | yarn-ui-v2 | Akhil PB | Akhil PB |
| [YARN-8615](https://issues.apache.org/jira/browse/YARN-8615) | [UI2] Resource Usage tab shows only memory related info. No info available for vcores/gpu. |  Major | yarn-ui-v2 | Sumana Sathish | Akhil PB |
| [HDFS-13792](https://issues.apache.org/jira/browse/HDFS-13792) | Fix FSN read/write lock metrics name |  Trivial | documentation, metrics | Chao Sun | Chao Sun |
| [YARN-8629](https://issues.apache.org/jira/browse/YARN-8629) | Container cleanup fails while trying to delete Cgroups |  Critical | . | Yesha Vora | Suma Shivaprasad |
| [YARN-8407](https://issues.apache.org/jira/browse/YARN-8407) | Container launch exception in AM log should be printed in ERROR level |  Major | . | Yesha Vora | Yesha Vora |
| [YARN-8399](https://issues.apache.org/jira/browse/YARN-8399) | NodeManager is giving 403 GSS exception post upgrade to 3.1 in secure mode |  Major | timelineservice | Sunil Govindan | Sunil Govindan |
| [HDFS-13799](https://issues.apache.org/jira/browse/HDFS-13799) | TestEditLogTailer#testTriggersLogRollsForAllStandbyNN fails due to missing synchronization between rollEditsRpcExecutor and tailerThread shutdown |  Minor | ha | Hrishikesh Gadre | Hrishikesh Gadre |
| [HDFS-13786](https://issues.apache.org/jira/browse/HDFS-13786) | EC: Display erasure coding policy for sub-directories is not working |  Major | erasure-coding | Souryakanta Dwivedy | Ayush Saxena |
| [HDFS-13785](https://issues.apache.org/jira/browse/HDFS-13785) | EC: "removePolicy" is not working for built-in/system Erasure Code policies |  Minor | documentation, erasure-coding | Souryakanta Dwivedy | Ayush Saxena |
| [YARN-8633](https://issues.apache.org/jira/browse/YARN-8633) | Update DataTables version in yarn-common in line with JQuery 3 upgrade |  Major | yarn | Akhil PB | Akhil PB |
| [YARN-8331](https://issues.apache.org/jira/browse/YARN-8331) | Race condition in NM container launched after done |  Major | . | Yang Wang | Pradeep Ambati |
| [YARN-8521](https://issues.apache.org/jira/browse/YARN-8521) | NPE in AllocationTagsManager when a container is removed more than once |  Major | resourcemanager | Weiwei Yang | Weiwei Yang |
| [YARN-8575](https://issues.apache.org/jira/browse/YARN-8575) | Avoid committing allocation proposal to unavailable nodes in async scheduling |  Major | capacityscheduler | Tao Yang | Tao Yang |
| [HDFS-13795](https://issues.apache.org/jira/browse/HDFS-13795) | Fix potential NPE in InMemoryLevelDBAliasMapServer |  Major | . | Virajith Jalaparti | Virajith Jalaparti |
| [HDFS-13668](https://issues.apache.org/jira/browse/HDFS-13668) | FSPermissionChecker may throws AIOOE when check inode permission |  Major | namenode | He Xiaoqiao | He Xiaoqiao |
| [HADOOP-15638](https://issues.apache.org/jira/browse/HADOOP-15638) | KMS Accept Queue Size default changed from 500 to 128 in Hadoop 3.x |  Major | kms | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-13823](https://issues.apache.org/jira/browse/HDFS-13823) | NameNode UI : "Utilities -\> Browse the file system -\> open a file -\> Head the file" is not working |  Major | ui | Nanda kumar | Nanda kumar |
| [HDFS-13738](https://issues.apache.org/jira/browse/HDFS-13738) | fsck -list-corruptfileblocks has infinite loop if user is not privileged. |  Major | tools | Wei-Chiu Chuang | Yuen-Kuei Hsueh |
| [HDFS-13758](https://issues.apache.org/jira/browse/HDFS-13758) | DatanodeManager should throw exception if it has BlockRecoveryCommand but the block is not under construction |  Major | namenode | Wei-Chiu Chuang | chencan |
| [YARN-8614](https://issues.apache.org/jira/browse/YARN-8614) | Fix few annotation typos in YarnConfiguration |  Trivial | . | Sen Zhao | Sen Zhao |
| [HDFS-13819](https://issues.apache.org/jira/browse/HDFS-13819) | TestDirectoryScanner#testDirectoryScannerInFederatedCluster is flaky |  Minor | hdfs | Daniel Templeton | Daniel Templeton |
| [YARN-8656](https://issues.apache.org/jira/browse/YARN-8656) | container-executor should not write cgroup tasks files for docker containers |  Major | . | Jim Brennan | Jim Brennan |
| [YARN-8474](https://issues.apache.org/jira/browse/YARN-8474) | sleeper service fails to launch with "Authentication Required" |  Critical | yarn | Sumana Sathish | Billie Rinaldi |
| [HDFS-13746](https://issues.apache.org/jira/browse/HDFS-13746) | Still occasional "Should be different group" failure in TestRefreshUserMappings#testGroupMappingRefresh |  Major | . | Siyao Meng | Siyao Meng |
| [YARN-8667](https://issues.apache.org/jira/browse/YARN-8667) | Cleanup symlinks when container restarted by NM to solve issue "find: File system loop detected;" for tar ball artifacts. |  Critical | . | Rohith Sharma K S | Chandni Singh |
| [HDFS-10240](https://issues.apache.org/jira/browse/HDFS-10240) | Race between close/recoverLease leads to missing block |  Major | . | zhouyingchao | Jinglun |
| [HADOOP-15655](https://issues.apache.org/jira/browse/HADOOP-15655) | Enhance KMS client retry behavior |  Critical | kms | Kitti Nanasi | Kitti Nanasi |
| [YARN-8612](https://issues.apache.org/jira/browse/YARN-8612) | Fix NM Collector Service Port issue in YarnConfiguration |  Major | ATSv2 | Prabha Manepalli | Prabha Manepalli |
| [HDFS-13747](https://issues.apache.org/jira/browse/HDFS-13747) | Statistic for list\_located\_status is incremented incorrectly by listStatusIterator |  Minor | hdfs-client | Todd Lipcon | Antal Mihalyi |
| [HADOOP-8807](https://issues.apache.org/jira/browse/HADOOP-8807) | Update README and website to reflect HADOOP-8662 |  Trivial | documentation | Eli Collins | Andras Bokor |
| [HADOOP-15674](https://issues.apache.org/jira/browse/HADOOP-15674) | Test failure TestSSLHttpServer.testExcludedCiphers with TLS\_ECDHE\_RSA\_WITH\_AES\_128\_CBC\_SHA256 cipher suite |  Major | common | Gabor Bota | Szilard Nemeth |
| [YARN-8640](https://issues.apache.org/jira/browse/YARN-8640) | Restore previous state in container-executor after failure |  Major | . | Jim Brennan | Jim Brennan |
| [YARN-8679](https://issues.apache.org/jira/browse/YARN-8679) | [ATSv2] If HBase cluster is down for long time, high chances that NM ContainerManager dispatcher get blocked |  Major | . | Rohith Sharma K S | Wangda Tan |
| [HDFS-13772](https://issues.apache.org/jira/browse/HDFS-13772) | Erasure coding: Unnecessary NameNode Logs displaying for Enabling/Disabling Erasure coding policies which are already enabled/disabled |  Trivial | erasure-coding | Souryakanta Dwivedy | Ayush Saxena |
| [HADOOP-14314](https://issues.apache.org/jira/browse/HADOOP-14314) | The OpenSolaris taxonomy link is dead in InterfaceClassification.md |  Major | documentation | Daniel Templeton | Rui Gao |
| [YARN-8649](https://issues.apache.org/jira/browse/YARN-8649) | NPE in localizer hearbeat processing if a container is killed while localizing |  Major | . | lujie | lujie |
| [HDFS-13805](https://issues.apache.org/jira/browse/HDFS-13805) | Journal Nodes should allow to format non-empty directories with "-force" option |  Major | journal-node | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [YARN-8632](https://issues.apache.org/jira/browse/YARN-8632) | Threads in SLS quit without logging exception |  Major | scheduler-load-simulator | Xianghao Lu | Xianghao Lu |
| [MAPREDUCE-6861](https://issues.apache.org/jira/browse/MAPREDUCE-6861) | Add metrics tags for ShuffleClientMetrics |  Major | . | Akira Ajisaka | Zoltan Siegl |
| [YARN-8719](https://issues.apache.org/jira/browse/YARN-8719) | Typo correction for yarn configuration in OpportunisticContainers(federation) docs |  Major | documentation, federation | Y. SREENIVASULU REDDY | Y. SREENIVASULU REDDY |
| [YARN-8675](https://issues.apache.org/jira/browse/YARN-8675) | Setting hostname of docker container breaks with "host" networking mode for Apps which do not run as a YARN service |  Major | . | Yesha Vora | Suma Shivaprasad |
| [HADOOP-15633](https://issues.apache.org/jira/browse/HADOOP-15633) | fs.TrashPolicyDefault: Can't create trash directory |  Major | common | Fei Hui | Fei Hui |
| [HDFS-13858](https://issues.apache.org/jira/browse/HDFS-13858) | RBF: Add check to have single valid argument to safemode command |  Major | federation | Soumyapn | Ayush Saxena |
| [HDFS-13837](https://issues.apache.org/jira/browse/HDFS-13837) | Enable debug log for LeaseRenewer in TestDistributedFileSystem |  Major | hdfs | Shweta | Shweta |
| [HDFS-13731](https://issues.apache.org/jira/browse/HDFS-13731) | ReencryptionUpdater fails with ConcurrentModificationException during processCheckpoints |  Major | encryption | Xiao Chen | Zsolt Venczel |
| [YARN-8723](https://issues.apache.org/jira/browse/YARN-8723) | Fix a typo in CS init error message when resource calculator is not correctly set |  Minor | . | Weiwei Yang | Abhishek Modi |
| [HADOOP-15705](https://issues.apache.org/jira/browse/HADOOP-15705) | Typo in the definition of "stable" in the interface classification |  Minor | . | Daniel Templeton | Daniel Templeton |
| [HDFS-13863](https://issues.apache.org/jira/browse/HDFS-13863) | FsDatasetImpl should log DiskOutOfSpaceException |  Major | hdfs | Fei Hui | Fei Hui |
| [HADOOP-15698](https://issues.apache.org/jira/browse/HADOOP-15698) | KMS log4j is not initialized properly at startup |  Major | kms | Kitti Nanasi | Kitti Nanasi |
| [HADOOP-15706](https://issues.apache.org/jira/browse/HADOOP-15706) | Typo in compatibility doc: SHOUD -\> SHOULD |  Trivial | . | Daniel Templeton | Laszlo Kollar |
| [HDFS-13027](https://issues.apache.org/jira/browse/HDFS-13027) | Handle possible NPEs due to deleted blocks in race condition |  Major | namenode | Vinayakumar B | Vinayakumar B |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-7066](https://issues.apache.org/jira/browse/MAPREDUCE-7066) | TestQueue fails on Java9 |  Major | test | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-15313](https://issues.apache.org/jira/browse/HADOOP-15313) | TestKMS should close providers |  Major | kms, test | Xiao Chen | Xiao Chen |
| [HDFS-13129](https://issues.apache.org/jira/browse/HDFS-13129) | Add a test for DfsAdmin refreshSuperUserGroupsConfiguration |  Minor | namenode | Mukul Kumar Singh | Mukul Kumar Singh |
| [HADOOP-14188](https://issues.apache.org/jira/browse/HADOOP-14188) | Remove the usage of org.mockito.internal.util.reflection.Whitebox |  Major | test | Akira Ajisaka | Ewan Higgs |
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
| [HDFS-13627](https://issues.apache.org/jira/browse/HDFS-13627) | TestErasureCodingExerciseAPIs fails on Windows |  Minor | . | Anbang Hu | Anbang Hu |
| [HDFS-13591](https://issues.apache.org/jira/browse/HDFS-13591) | TestDFSShell#testSetrepLow fails on Windows |  Minor | . | Anbang Hu | Anbang Hu |
| [HDFS-13632](https://issues.apache.org/jira/browse/HDFS-13632) | Randomize baseDir for MiniJournalCluster in MiniQJMHACluster for TestDFSAdminWithHA |  Minor | . | Anbang Hu | Anbang Hu |
| [HDFS-13629](https://issues.apache.org/jira/browse/HDFS-13629) | Some tests in TestDiskBalancerCommand fail on Windows due to MiniDFSCluster path conflict and improper path usage |  Minor | . | Anbang Hu | Anbang Hu |
| [HDFS-13631](https://issues.apache.org/jira/browse/HDFS-13631) | TestDFSAdmin#testCheckNumOfBlocksInReportCommand should use a separate MiniDFSCluster path |  Minor | . | Anbang Hu | Anbang Hu |
| [HDFS-13651](https://issues.apache.org/jira/browse/HDFS-13651) | TestReencryptionHandler fails on Windows |  Minor | . | Anbang Hu | Anbang Hu |
| [HDFS-13648](https://issues.apache.org/jira/browse/HDFS-13648) | Fix TestGetConf#testGetJournalNodes on Windows due to a mismatch line separator |  Major | . | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [MAPREDUCE-7102](https://issues.apache.org/jira/browse/MAPREDUCE-7102) | Fix TestJavaSerialization for Windows due a mismatch line separator |  Minor | . | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [MAPREDUCE-7105](https://issues.apache.org/jira/browse/MAPREDUCE-7105) | Fix TestNativeCollectorOnlyHandler.testOnCall on Windows because of the path format |  Minor | . | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [HDFS-13652](https://issues.apache.org/jira/browse/HDFS-13652) | Randomize baseDir for MiniDFSCluster in TestBlockScanner |  Minor | . | Anbang Hu | Anbang Hu |
| [HDFS-13649](https://issues.apache.org/jira/browse/HDFS-13649) | Randomize baseDir for MiniDFSCluster in TestReconstructStripedFile and TestReconstructStripedFileWithRandomECPolicy |  Minor | . | Anbang Hu | Anbang Hu |
| [HDFS-13650](https://issues.apache.org/jira/browse/HDFS-13650) | Randomize baseDir for MiniDFSCluster in TestDFSStripedInputStream and TestDFSStripedInputStreamWithRandomECPolicy |  Minor | . | Anbang Hu | Anbang Hu |
| [HADOOP-15520](https://issues.apache.org/jira/browse/HADOOP-15520) | Add tests for various org.apache.hadoop.util classes |  Minor | test, util | Arash Nabili | Arash Nabili |
| [YARN-8370](https://issues.apache.org/jira/browse/YARN-8370) | Some Node Manager tests fail on Windows due to improper path/file separator |  Minor | . | Anbang Hu | Anbang Hu |
| [YARN-8422](https://issues.apache.org/jira/browse/YARN-8422) | TestAMSimulator failing with NPE |  Minor | . | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [HADOOP-15532](https://issues.apache.org/jira/browse/HADOOP-15532) | TestBasicDiskValidator fails with NoSuchFileException |  Minor | . | Íñigo Goiri | Giovanni Matteo Fumarola |
| [HDFS-13563](https://issues.apache.org/jira/browse/HDFS-13563) | TestDFSAdminWithHA times out on Windows |  Minor | . | Anbang Hu | Lukas Majercak |
| [HDFS-13681](https://issues.apache.org/jira/browse/HDFS-13681) | Fix TestStartup.testNNFailToStartOnReadOnlyNNDir test failure on Windows |  Major | test | Xiao Liang | Xiao Liang |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-10794](https://issues.apache.org/jira/browse/HDFS-10794) | [SPS]: Provide storage policy satisfy worker at DN for co-ordinating the block storage movement work |  Major | datanode, namenode | Rakesh R | Rakesh R |
| [HDFS-10800](https://issues.apache.org/jira/browse/HDFS-10800) | [SPS]: Daemon thread in Namenode to find blocks placed in other storage than what the policy specifies |  Major | namenode | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-10801](https://issues.apache.org/jira/browse/HDFS-10801) | [SPS]: Protocol buffer changes for sending storage movement commands from NN to DN |  Major | datanode, namenode | Uma Maheswara Rao G | Rakesh R |
| [HDFS-10884](https://issues.apache.org/jira/browse/HDFS-10884) | [SPS]: Add block movement tracker to track the completion of block movement future tasks at DN |  Major | datanode | Rakesh R | Rakesh R |
| [HDFS-10954](https://issues.apache.org/jira/browse/HDFS-10954) | [SPS]: Provide mechanism to send blocks movement result back to NN from coordinator DN |  Major | datanode, namenode | Rakesh R | Rakesh R |
| [HDFS-11029](https://issues.apache.org/jira/browse/HDFS-11029) | [SPS]:Provide retry mechanism for the blocks which were failed while moving its storage at DNs |  Major | namenode | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-11068](https://issues.apache.org/jira/browse/HDFS-11068) | [SPS]: Provide unique trackID to track the block movement sends to coordinator |  Major | datanode, namenode | Rakesh R | Rakesh R |
| [HDFS-10802](https://issues.apache.org/jira/browse/HDFS-10802) | [SPS]: Add satisfyStoragePolicy API in HdfsAdmin |  Major | hdfs-client | Uma Maheswara Rao G | Yuanbo Liu |
| [HDFS-11151](https://issues.apache.org/jira/browse/HDFS-11151) | [SPS]: StoragePolicySatisfier should gracefully handle when there is no target node with the required storage type |  Major | datanode, namenode | Rakesh R | Rakesh R |
| [HDFS-10885](https://issues.apache.org/jira/browse/HDFS-10885) | [SPS]: Mover tool should not be allowed to run when Storage Policy Satisfier is on |  Major | datanode, namenode | Wei Zhou | Wei Zhou |
| [HDFS-11123](https://issues.apache.org/jira/browse/HDFS-11123) | [SPS] Make storage policy satisfier daemon work on/off dynamically |  Major | datanode, namenode | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-11032](https://issues.apache.org/jira/browse/HDFS-11032) | [SPS]: Handling of block movement failure at the coordinator datanode |  Major | datanode | Rakesh R | Rakesh R |
| [HDFS-11248](https://issues.apache.org/jira/browse/HDFS-11248) | [SPS]: Handle partial block location movements |  Major | datanode, namenode | Rakesh R | Rakesh R |
| [HDFS-11193](https://issues.apache.org/jira/browse/HDFS-11193) | [SPS]: Erasure coded files should be considered for satisfying storage policy |  Major | namenode | Rakesh R | Rakesh R |
| [HDFS-11289](https://issues.apache.org/jira/browse/HDFS-11289) | [SPS]: Make SPS movement monitor timeouts configurable |  Major | namenode | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-11293](https://issues.apache.org/jira/browse/HDFS-11293) | [SPS]: Local DN should be given preference as source node, when target available in same node |  Critical | namenode | Yuanbo Liu | Uma Maheswara Rao G |
| [HDFS-11150](https://issues.apache.org/jira/browse/HDFS-11150) | [SPS]: Provide persistence when satisfying storage policy. |  Major | datanode, namenode | Yuanbo Liu | Yuanbo Liu |
| [HDFS-11186](https://issues.apache.org/jira/browse/HDFS-11186) | [SPS]: Daemon thread of SPS should start only in Active NN |  Major | datanode, namenode | Wei Zhou | Wei Zhou |
| [HDFS-11309](https://issues.apache.org/jira/browse/HDFS-11309) | [SPS]: chooseTargetTypeInSameNode should pass accurate block size to chooseStorage4Block while choosing target |  Major | namenode | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-11243](https://issues.apache.org/jira/browse/HDFS-11243) | [SPS]: Add a protocol command from NN to DN for dropping the SPS work and queues |  Major | datanode, namenode | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-11239](https://issues.apache.org/jira/browse/HDFS-11239) | [SPS]: Check Mover file ID lease also to determine whether Mover is running |  Major | datanode, namenode | Wei Zhou | Wei Zhou |
| [HDFS-11336](https://issues.apache.org/jira/browse/HDFS-11336) | [SPS]: Remove xAttrs when movements done or SPS disabled |  Major | datanode, namenode | Yuanbo Liu | Yuanbo Liu |
| [HDFS-11338](https://issues.apache.org/jira/browse/HDFS-11338) | [SPS]: Fix timeout issue in unit tests caused by longger NN down time |  Major | datanode, namenode | Wei Zhou | Rakesh R |
| [HDFS-11334](https://issues.apache.org/jira/browse/HDFS-11334) | [SPS]: NN switch and rescheduling movements can lead to have more than one coordinator for same file blocks |  Major | datanode, namenode | Uma Maheswara Rao G | Rakesh R |
| [HDFS-11572](https://issues.apache.org/jira/browse/HDFS-11572) | [SPS]: SPS should clean Xattrs when no blocks required to satisfy for a file |  Major | namenode | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-11695](https://issues.apache.org/jira/browse/HDFS-11695) | [SPS]: Namenode failed to start while loading SPS xAttrs from the edits log. |  Blocker | namenode | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-11883](https://issues.apache.org/jira/browse/HDFS-11883) | [SPS] : Handle NPE in BlockStorageMovementTracker when dropSPSWork() called |  Major | datanode | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-11762](https://issues.apache.org/jira/browse/HDFS-11762) | [SPS] : Empty files should be ignored in StoragePolicySatisfier. |  Major | namenode | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-11726](https://issues.apache.org/jira/browse/HDFS-11726) | [SPS] : StoragePolicySatisfier should not select same storage type as source and destination in same datanode. |  Major | namenode | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-11966](https://issues.apache.org/jira/browse/HDFS-11966) | [SPS] Correct the log in BlockStorageMovementAttemptedItems#blockStorageMovementResultCheck |  Minor | namenode | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-11670](https://issues.apache.org/jira/browse/HDFS-11670) | [SPS]: Add CLI command for satisfy storage policy operations |  Major | datanode, namenode | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-11965](https://issues.apache.org/jira/browse/HDFS-11965) | [SPS]: Should give chance to satisfy the low redundant blocks before removing the xattr |  Major | namenode | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-11264](https://issues.apache.org/jira/browse/HDFS-11264) | [SPS]: Double checks to ensure that SPS/Mover are not running together |  Major | datanode, namenode | Wei Zhou | Rakesh R |
| [HDFS-11874](https://issues.apache.org/jira/browse/HDFS-11874) | [SPS]: Document the SPS feature |  Major | documentation | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-12146](https://issues.apache.org/jira/browse/HDFS-12146) | [SPS] : Fix TestStoragePolicySatisfierWithStripedFile#testSPSWhenFileHasLowRedundancyBlocks |  Major | namenode | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-12141](https://issues.apache.org/jira/browse/HDFS-12141) | [SPS]: Fix checkstyle warnings |  Major | datanode, namenode | Rakesh R | Rakesh R |
| [HDFS-12152](https://issues.apache.org/jira/browse/HDFS-12152) | [SPS]: Re-arrange StoragePolicySatisfyWorker stopping sequence to improve thread cleanup time |  Minor | datanode, namenode | Rakesh R | Rakesh R |
| [HDFS-12214](https://issues.apache.org/jira/browse/HDFS-12214) | [SPS]: Fix review comments of StoragePolicySatisfier feature |  Major | datanode, namenode | Rakesh R | Rakesh R |
| [HDFS-12225](https://issues.apache.org/jira/browse/HDFS-12225) | [SPS]: Optimize extended attributes for tracking SPS movements |  Major | datanode, namenode | Uma Maheswara Rao G | Surendra Singh Lilhore |
| [HDFS-12291](https://issues.apache.org/jira/browse/HDFS-12291) | [SPS]: Provide a mechanism to recursively iterate and satisfy storage policy of all the files under the given dir |  Major | datanode, namenode | Rakesh R | Surendra Singh Lilhore |
| [HDFS-12570](https://issues.apache.org/jira/browse/HDFS-12570) | [SPS]: Refactor Co-ordinator datanode logic to track the block storage movements |  Major | datanode, namenode | Rakesh R | Rakesh R |
| [HDFS-12556](https://issues.apache.org/jira/browse/HDFS-12556) | [SPS] : Block movement analysis should be done in read lock. |  Major | datanode, namenode | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-12310](https://issues.apache.org/jira/browse/HDFS-12310) | [SPS]: Provide an option to track the status of in progress requests |  Major | datanode, namenode | Uma Maheswara Rao G | Surendra Singh Lilhore |
| [HDFS-12790](https://issues.apache.org/jira/browse/HDFS-12790) | [SPS]: Rebasing HDFS-10285 branch after HDFS-10467, HDFS-12599 and HDFS-11968 commits |  Major | datanode, namenode | Rakesh R | Rakesh R |
| [HDFS-12106](https://issues.apache.org/jira/browse/HDFS-12106) | [SPS]: Improve storage policy satisfier configurations |  Major | namenode | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-12955](https://issues.apache.org/jira/browse/HDFS-12955) | [SPS]: Move SPS classes to a separate package |  Trivial | nn | Uma Maheswara Rao G | Rakesh R |
| [HDFS-12982](https://issues.apache.org/jira/browse/HDFS-12982) | [SPS]: Reduce the locking and cleanup the Namesystem access |  Major | datanode, namenode | Rakesh R | Rakesh R |
| [HDFS-12911](https://issues.apache.org/jira/browse/HDFS-12911) | [SPS]: Modularize the SPS code and expose necessary interfaces for external/internal implementations. |  Major | datanode, namenode | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-13025](https://issues.apache.org/jira/browse/HDFS-13025) | [SPS]: Implement a mechanism to scan the files for external SPS |  Major | namenode | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-13033](https://issues.apache.org/jira/browse/HDFS-13033) | [SPS]: Implement a mechanism to do file block movements for external SPS |  Major | . | Rakesh R | Rakesh R |
| [HDFS-13057](https://issues.apache.org/jira/browse/HDFS-13057) | [SPS]: Revisit configurations to make SPS service modes internal/external/none |  Blocker | . | Rakesh R | Rakesh R |
| [HDFS-13075](https://issues.apache.org/jira/browse/HDFS-13075) | [SPS]: Provide External Context implementation. |  Major | . | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-13050](https://issues.apache.org/jira/browse/HDFS-13050) | [SPS] : Create start/stop script to start external SPS process. |  Blocker | namenode | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-13077](https://issues.apache.org/jira/browse/HDFS-13077) | [SPS]: Fix review comments of external storage policy satisfier |  Major | . | Rakesh R | Rakesh R |
| [HDFS-13097](https://issues.apache.org/jira/browse/HDFS-13097) | [SPS]: Fix the branch review comments(Part1) |  Major | namenode | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-13110](https://issues.apache.org/jira/browse/HDFS-13110) | [SPS]: Reduce the number of APIs in NamenodeProtocol used by external satisfier |  Major | . | Rakesh R | Rakesh R |
| [HDFS-13166](https://issues.apache.org/jira/browse/HDFS-13166) | [SPS]: Implement caching mechanism to keep LIVE datanodes to minimize costly getLiveDatanodeStorageReport() calls |  Major | . | Rakesh R | Rakesh R |
| [HADOOP-15262](https://issues.apache.org/jira/browse/HADOOP-15262) | AliyunOSS: move files under a directory in parallel when rename a directory |  Major | fs/oss | wujinhu | wujinhu |
| [YARN-8002](https://issues.apache.org/jira/browse/YARN-8002) | Support NOT\_SELF and ALL namespace types for allocation tag |  Major | resourcemanager | Weiwei Yang | Weiwei Yang |
| [HDFS-13307](https://issues.apache.org/jira/browse/HDFS-13307) | RBF: Improve the use of setQuota command |  Major | . | liuhongtong | liuhongtong |
| [YARN-7497](https://issues.apache.org/jira/browse/YARN-7497) | Add file system based scheduler configuration store |  Major | yarn | Jiandan Yang | Jiandan Yang |
| [HDFS-13289](https://issues.apache.org/jira/browse/HDFS-13289) | RBF: TestConnectionManager#testCleanup() test case need correction |  Minor | . | Dibyendu Karmakar | Dibyendu Karmakar |
| [HADOOP-14758](https://issues.apache.org/jira/browse/HADOOP-14758) | S3GuardTool.prune to handle UnsupportedOperationException |  Trivial | fs/s3 | Steve Loughran | Gabor Bota |
| [HDFS-13364](https://issues.apache.org/jira/browse/HDFS-13364) | RBF: Support NamenodeProtocol in the Router |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-8013](https://issues.apache.org/jira/browse/YARN-8013) | Support application tags when defining application namespaces for placement constraints |  Major | . | Weiwei Yang | Weiwei Yang |
| [YARN-7946](https://issues.apache.org/jira/browse/YARN-7946) | Update TimelineServerV2 doc as per YARN-7919 |  Major | . | Rohith Sharma K S | Haibo Chen |
| [YARN-6936](https://issues.apache.org/jira/browse/YARN-6936) | [Atsv2] Retrospect storing entities into sub application table from client perspective |  Major | . | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-13353](https://issues.apache.org/jira/browse/HDFS-13353) | RBF: TestRouterWebHDFSContractCreate failed |  Major | test | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-15367](https://issues.apache.org/jira/browse/HADOOP-15367) | Update the initialization code in the docker hadoop-runner baseimage |  Major | . | Elek, Marton | Elek, Marton |
| [HADOOP-14759](https://issues.apache.org/jira/browse/HADOOP-14759) | S3GuardTool prune to prune specific bucket entries |  Minor | fs/s3 | Steve Loughran | Gabor Bota |
| [YARN-8107](https://issues.apache.org/jira/browse/YARN-8107) | Give an informative message when incorrect format is used in ATSv2 filter attributes |  Major | ATSv2 | Charan Hebri | Rohith Sharma K S |
| [YARN-8048](https://issues.apache.org/jira/browse/YARN-8048) | Support auto-spawning of admin configured services during bootstrap of rm/apiserver |  Major | . | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-13402](https://issues.apache.org/jira/browse/HDFS-13402) | RBF: Fix  java doc for StateStoreFileSystemImpl |  Minor | hdfs | Yiran Wu | Yiran Wu |
| [YARN-7574](https://issues.apache.org/jira/browse/YARN-7574) | Add support for Node Labels on Auto Created Leaf Queue Template |  Major | capacity scheduler | Suma Shivaprasad | Suma Shivaprasad |
| [HDFS-13380](https://issues.apache.org/jira/browse/HDFS-13380) | RBF: mv/rm fail after the directory exceeded the quota limit |  Major | . | Weiwei Wu | Yiqun Lin |
| [YARN-7667](https://issues.apache.org/jira/browse/YARN-7667) | Docker Stop grace period should be configurable |  Major | yarn | Eric Badger | Eric Badger |
| [HDFS-13410](https://issues.apache.org/jira/browse/HDFS-13410) | RBF: Support federation with no subclusters |  Minor | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13384](https://issues.apache.org/jira/browse/HDFS-13384) | RBF: Improve timeout RPC call mechanism |  Minor | . | Íñigo Goiri | Íñigo Goiri |
| [HADOOP-15376](https://issues.apache.org/jira/browse/HADOOP-15376) | Remove double semi colons on imports that make Clover fall over. |  Minor | . | Ewan Higgs | Ewan Higgs |
| [YARN-7973](https://issues.apache.org/jira/browse/YARN-7973) | Support ContainerRelaunch for Docker containers |  Major | . | Shane Kumpf | Shane Kumpf |
| [YARN-7941](https://issues.apache.org/jira/browse/YARN-7941) | Transitive dependencies for component are not resolved |  Major | . | Rohith Sharma K S | Billie Rinaldi |
| [HDFS-13045](https://issues.apache.org/jira/browse/HDFS-13045) | RBF: Improve error message returned from subcluster |  Minor | . | Wei Yan | Íñigo Goiri |
| [HDFS-13428](https://issues.apache.org/jira/browse/HDFS-13428) | RBF: Remove LinkedList From StateStoreFileImpl.java |  Trivial | federation | BELUGA BEHR | BELUGA BEHR |
| [HDFS-13386](https://issues.apache.org/jira/browse/HDFS-13386) | RBF: Wrong date information in list file(-ls) result |  Minor | . | Dibyendu Karmakar | Dibyendu Karmakar |
| [YARN-7221](https://issues.apache.org/jira/browse/YARN-7221) | Add security check for privileged docker container |  Major | security | Eric Yang | Eric Yang |
| [HADOOP-15350](https://issues.apache.org/jira/browse/HADOOP-15350) | [JDK10] Update maven plugin tools to fix compile error in hadoop-maven-plugins module |  Major | build | Akira Ajisaka | Takanobu Asanuma |
| [YARN-7931](https://issues.apache.org/jira/browse/YARN-7931) | [atsv2 read acls] Include domain table creation as part of schema creator |  Major | . | Vrushali C | Vrushali C |
| [YARN-7936](https://issues.apache.org/jira/browse/YARN-7936) | Add default service AM Xmx |  Major | . | Jian He | Jian He |
| [YARN-8018](https://issues.apache.org/jira/browse/YARN-8018) | Yarn Service Upgrade: Add support for initiating service upgrade |  Major | . | Chandni Singh | Chandni Singh |
| [HADOOP-14999](https://issues.apache.org/jira/browse/HADOOP-14999) | AliyunOSS: provide one asynchronous multi-part based uploading mechanism |  Major | fs/oss | Genmao Yu | Genmao Yu |
| [YARN-7142](https://issues.apache.org/jira/browse/YARN-7142) | Support placement policy in yarn native services |  Major | yarn-native-services | Billie Rinaldi | Gour Saha |
| [YARN-8138](https://issues.apache.org/jira/browse/YARN-8138) | Add unit test to validate queue priority preemption works under node partition. |  Minor | . | Charan Hebri | Zian Chen |
| [YARN-8060](https://issues.apache.org/jira/browse/YARN-8060) | Create default readiness check for service components |  Major | yarn-native-services | Billie Rinaldi | Billie Rinaldi |
| [HDFS-13435](https://issues.apache.org/jira/browse/HDFS-13435) | RBF: Improve the error loggings for printing the stack trace |  Major | . | Yiqun Lin | Yiqun Lin |
| [YARN-8126](https://issues.apache.org/jira/browse/YARN-8126) | Support auto-spawning of admin configured services during bootstrap of RM |  Major | . | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-7996](https://issues.apache.org/jira/browse/YARN-7996) | Allow user supplied Docker client configurations with YARN native services |  Major | . | Shane Kumpf | Shane Kumpf |
| [YARN-8134](https://issues.apache.org/jira/browse/YARN-8134) | Support specifying node resources in SLS |  Major | . | Abhishek Modi | Abhishek Modi |
| [HDFS-13466](https://issues.apache.org/jira/browse/HDFS-13466) | RBF: Add more router-related information to the UI |  Minor | . | Wei Yan | Wei Yan |
| [YARN-5888](https://issues.apache.org/jira/browse/YARN-5888) | [UI2] Improve unit tests for new YARN UI |  Minor | yarn-ui-v2 | Akhil PB | Akhil PB |
| [HDFS-13453](https://issues.apache.org/jira/browse/HDFS-13453) | RBF: getMountPointDates should fetch latest subdir time/date when parent dir is not present but /parent/child dirs are present in mount table |  Major | . | Dibyendu Karmakar | Dibyendu Karmakar |
| [YARN-8186](https://issues.apache.org/jira/browse/YARN-8186) | [Router] Federation: routing getAppState REST invocations transparently to multiple RMs |  Major | . | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [YARN-8137](https://issues.apache.org/jira/browse/YARN-8137) | Parallelize node addition in SLS |  Major | . | Abhishek Modi | Abhishek Modi |
| [YARN-8111](https://issues.apache.org/jira/browse/YARN-8111) | Simplify PlacementConstraints API by removing allocationTagToIntraApp |  Minor | resourcemanager | Weiwei Yang | Weiwei Yang |
| [YARN-8064](https://issues.apache.org/jira/browse/YARN-8064) | Docker ".cmd" files should not be put in hadoop.tmp.dir |  Critical | . | Eric Badger | Eric Badger |
| [HDFS-13478](https://issues.apache.org/jira/browse/HDFS-13478) | RBF: Disabled Nameservice store API |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-8177](https://issues.apache.org/jira/browse/YARN-8177) | Documentation changes for auto creation of Leaf Queues with node label |  Major | . | Suma Shivaprasad | Suma Shivaprasad |
| [HADOOP-14756](https://issues.apache.org/jira/browse/HADOOP-14756) | S3Guard: expose capability query in MetadataStore and add tests of authoritative mode |  Major | fs/s3 | Steve Loughran | Gabor Bota |
| [HDFS-13490](https://issues.apache.org/jira/browse/HDFS-13490) | RBF: Fix setSafeMode in the Router |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13484](https://issues.apache.org/jira/browse/HDFS-13484) | RBF: Disable Nameservices from the federation |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HADOOP-15404](https://issues.apache.org/jira/browse/HADOOP-15404) | Remove multibyte characters in DataNodeUsageReportUtil |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-7939](https://issues.apache.org/jira/browse/YARN-7939) | Yarn Service Upgrade: add support to upgrade a component instance |  Major | . | Chandni Singh | Chandni Singh |
| [HDFS-13326](https://issues.apache.org/jira/browse/HDFS-13326) | RBF: Improve the interfaces to modify and view mount tables |  Minor | . | Wei Yan | Gang Li |
| [YARN-8122](https://issues.apache.org/jira/browse/YARN-8122) | Component health threshold monitor |  Major | . | Gour Saha | Gour Saha |
| [HDFS-13499](https://issues.apache.org/jira/browse/HDFS-13499) | RBF: Show disabled name services in the UI |  Minor | . | Íñigo Goiri | Íñigo Goiri |
| [HADOOP-13756](https://issues.apache.org/jira/browse/HADOOP-13756) | LocalMetadataStore#put(DirListingMetadata) should also put file metadata into fileHash. |  Major | fs/s3, test | Lei (Eddy) Xu | Gabor Bota |
| [YARN-8215](https://issues.apache.org/jira/browse/YARN-8215) | ATS v2 returns invalid YARN\_CONTAINER\_ALLOCATED\_HOST\_HTTP\_ADDRESS from NM |  Critical | ATSv2 | Yesha Vora | Rohith Sharma K S |
| [YARN-8152](https://issues.apache.org/jira/browse/YARN-8152) | Add chart in SLS to illustrate the throughput of the scheduler |  Major | scheduler-load-simulator | Weiwei Yang | Tao Yang |
| [YARN-8204](https://issues.apache.org/jira/browse/YARN-8204) | Yarn Service Upgrade: Add a flag to disable upgrade |  Major | . | Chandni Singh | Chandni Singh |
| [YARN-7781](https://issues.apache.org/jira/browse/YARN-7781) | Update YARN-Services-Examples.md to be in sync with the latest code |  Major | . | Gour Saha | Gour Saha |
| [HDFS-13508](https://issues.apache.org/jira/browse/HDFS-13508) | RBF: Normalize paths (automatically) when adding, updating, removing or listing mount table entries |  Minor | . | Ekanth Sethuramalingam | Ekanth Sethuramalingam |
| [HDFS-13434](https://issues.apache.org/jira/browse/HDFS-13434) | RBF: Fix dead links in RBF document |  Major | documentation | Akira Ajisaka | Chetna Chaudhari |
| [HDFS-13165](https://issues.apache.org/jira/browse/HDFS-13165) | [SPS]: Collects successfully moved block details via IBR |  Major | . | Rakesh R | Rakesh R |
| [YARN-8195](https://issues.apache.org/jira/browse/YARN-8195) | Fix constraint cardinality check in the presence of multiple target allocation tags |  Critical | . | Weiwei Yang | Weiwei Yang |
| [HADOOP-15239](https://issues.apache.org/jira/browse/HADOOP-15239) | S3ABlockOutputStream.flush() be no-op when stream closed |  Trivial | fs/s3 | Steve Loughran | Gabor Bota |
| [YARN-8228](https://issues.apache.org/jira/browse/YARN-8228) | Docker does not support hostnames greater than 64 characters |  Critical | yarn-native-services | Yesha Vora | Shane Kumpf |
| [YARN-8212](https://issues.apache.org/jira/browse/YARN-8212) | Pending backlog for async allocation threads should be configurable |  Major | . | Weiwei Yang | Tao Yang |
| [YARN-2674](https://issues.apache.org/jira/browse/YARN-2674) | Distributed shell AM may re-launch containers if RM work preserving restart happens |  Major | applications, resourcemanager | Chun Chen | Shane Kumpf |
| [HDFS-13488](https://issues.apache.org/jira/browse/HDFS-13488) | RBF: Reject requests when a Router is overloaded |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-8113](https://issues.apache.org/jira/browse/YARN-8113) | Update placement constraints doc with application namespaces and inter-app constraints |  Major | documentation | Weiwei Yang | Weiwei Yang |
| [YARN-8194](https://issues.apache.org/jira/browse/YARN-8194) | Exception when reinitializing a container using LinuxContainerExecutor |  Blocker | . | Chandni Singh | Chandni Singh |
| [YARN-8151](https://issues.apache.org/jira/browse/YARN-8151) | Yarn RM Epoch should wrap around |  Major | . | Young Chen | Young Chen |
| [YARN-7961](https://issues.apache.org/jira/browse/YARN-7961) | Improve status response when yarn application is destroyed |  Major | yarn-native-services | Yesha Vora | Gour Saha |
| [HDFS-13525](https://issues.apache.org/jira/browse/HDFS-13525) | RBF: Add unit test TestStateStoreDisabledNameservice |  Major | . | Yiqun Lin | Yiqun Lin |
| [HADOOP-15444](https://issues.apache.org/jira/browse/HADOOP-15444) | ITestS3GuardToolDynamo should only run with -Ddynamo |  Major | . | Aaron Fabbri | Aaron Fabbri |
| [YARN-5151](https://issues.apache.org/jira/browse/YARN-5151) | [UI2] Support kill application from new YARN UI |  Major | . | Wangda Tan | Gergely Novák |
| [YARN-8253](https://issues.apache.org/jira/browse/YARN-8253) | HTTPS Ats v2 api call fails with "bad HTTP parsed" |  Critical | ATSv2 | Yesha Vora | Charan Hebri |
| [YARN-8207](https://issues.apache.org/jira/browse/YARN-8207) | Docker container launch use popen have risk of shell expansion |  Blocker | yarn-native-services | Eric Yang | Eric Yang |
| [HADOOP-13649](https://issues.apache.org/jira/browse/HADOOP-13649) | s3guard: implement time-based (TTL) expiry for LocalMetadataStore |  Minor | fs/s3 | Aaron Fabbri | Gabor Bota |
| [HADOOP-15420](https://issues.apache.org/jira/browse/HADOOP-15420) | s3guard ITestS3GuardToolLocal failures in diff tests |  Minor | . | Aaron Fabbri | Gabor Bota |
| [YARN-8261](https://issues.apache.org/jira/browse/YARN-8261) | Docker container launch fails due to .cmd file creation failure |  Blocker | . | Eric Badger | Jason Lowe |
| [HADOOP-15454](https://issues.apache.org/jira/browse/HADOOP-15454) | TestRollingFileSystemSinkWithLocal fails on Windows |  Major | test | Xiao Liang | Xiao Liang |
| [HDFS-13346](https://issues.apache.org/jira/browse/HDFS-13346) | RBF: Fix synchronization of router quota and nameservice quota |  Major | . | liuhongtong | Yiqun Lin |
| [YARN-8243](https://issues.apache.org/jira/browse/YARN-8243) | Flex down should remove instance with largest component instance ID first |  Critical | yarn-native-services | Gour Saha | Gour Saha |
| [YARN-7654](https://issues.apache.org/jira/browse/YARN-7654) | Support ENTRY\_POINT for docker container |  Blocker | yarn | Eric Yang | Eric Yang |
| [YARN-8247](https://issues.apache.org/jira/browse/YARN-8247) | Incorrect HTTP status code returned by ATSv2 for non-whitelisted users |  Critical | ATSv2 | Charan Hebri | Rohith Sharma K S |
| [YARN-8130](https://issues.apache.org/jira/browse/YARN-8130) | Race condition when container events are published for KILLED applications |  Major | ATSv2 | Charan Hebri | Rohith Sharma K S |
| [YARN-8081](https://issues.apache.org/jira/browse/YARN-8081) | Yarn Service Upgrade: Add support to upgrade a component |  Major | . | Chandni Singh | Chandni Singh |
| [YARN-8284](https://issues.apache.org/jira/browse/YARN-8284) | get\_docker\_command refactoring |  Minor | . | Jason Lowe | Eric Badger |
| [YARN-7933](https://issues.apache.org/jira/browse/YARN-7933) | [atsv2 read acls] Add TimelineWriter#writeDomain |  Major | . | Vrushali C | Rohith Sharma K S |
| [YARN-7900](https://issues.apache.org/jira/browse/YARN-7900) | [AMRMProxy] AMRMClientRelayer for stateful FederationInterceptor |  Major | . | Botong Huang | Botong Huang |
| [YARN-8206](https://issues.apache.org/jira/browse/YARN-8206) | Sending a kill does not immediately kill docker containers |  Major | . | Eric Badger | Eric Badger |
| [YARN-7960](https://issues.apache.org/jira/browse/YARN-7960) | Add no-new-privileges flag to docker run |  Major | . | Eric Badger | Eric Badger |
| [YARN-8285](https://issues.apache.org/jira/browse/YARN-8285) | Remove unused environment variables from the Docker runtime |  Trivial | . | Shane Kumpf | Eric Badger |
| [YARN-4599](https://issues.apache.org/jira/browse/YARN-4599) | Set OOM control for memory cgroups |  Major | nodemanager | Karthik Kambatla | Miklos Szegedi |
| [YARN-7530](https://issues.apache.org/jira/browse/YARN-7530) | hadoop-yarn-services-api should be part of hadoop-yarn-services |  Blocker | yarn-native-services | Eric Yang | Chandni Singh |
| [YARN-6919](https://issues.apache.org/jira/browse/YARN-6919) | Add default volume mount list |  Major | yarn | Eric Badger | Eric Badger |
| [HADOOP-15494](https://issues.apache.org/jira/browse/HADOOP-15494) | TestRawLocalFileSystemContract fails on Windows |  Minor | test | Anbang Hu | Anbang Hu |
| [HADOOP-15498](https://issues.apache.org/jira/browse/HADOOP-15498) | TestHadoopArchiveLogs (#testGenerateScript, #testPrepareWorkingDir) fails on Windows |  Minor | . | Anbang Hu | Anbang Hu |
| [HADOOP-15497](https://issues.apache.org/jira/browse/HADOOP-15497) | TestTrash should use proper test path to avoid failing on Windows |  Minor | . | Anbang Hu | Anbang Hu |
| [YARN-8329](https://issues.apache.org/jira/browse/YARN-8329) | Docker client configuration can still be set incorrectly |  Major | . | Shane Kumpf | Shane Kumpf |
| [HADOOP-14946](https://issues.apache.org/jira/browse/HADOOP-14946) | S3Guard testPruneCommandCLI can fail |  Major | fs/s3 | Steve Loughran | Gabor Bota |
| [HADOOP-15480](https://issues.apache.org/jira/browse/HADOOP-15480) | AbstractS3GuardToolTestBase.testDiffCommand fails when using dynamo |  Major | fs/s3 | Gabor Bota | Gabor Bota |
| [HDFS-12978](https://issues.apache.org/jira/browse/HDFS-12978) | Fine-grained locking while consuming journal stream. |  Major | namenode | Konstantin Shvachko | Konstantin Shvachko |
| [YARN-8384](https://issues.apache.org/jira/browse/YARN-8384) | stdout.txt, stderr.txt logs of a launched docker container is coming with primary group of submit user instead of hadoop |  Critical | yarn-native-services | Sunil Govindan | Eric Yang |
| [YARN-8349](https://issues.apache.org/jira/browse/YARN-8349) | Remove YARN registry entries when a service is killed by the RM |  Critical | yarn-native-services | Shane Kumpf | Billie Rinaldi |
| [HDFS-13637](https://issues.apache.org/jira/browse/HDFS-13637) | RBF: Router fails when threadIndex (in ConnectionPool) wraps around Integer.MIN\_VALUE |  Critical | federation | CR Hota | CR Hota |
| [YARN-8342](https://issues.apache.org/jira/browse/YARN-8342) | Using docker image from a non-privileged registry, the launch\_command is not honored |  Critical | . | Wangda Tan | Eric Yang |
| [HDFS-13281](https://issues.apache.org/jira/browse/HDFS-13281) | Namenode#createFile should be /.reserved/raw/ aware. |  Critical | encryption | Rushabh S Shah | Rushabh S Shah |
| [YARN-4677](https://issues.apache.org/jira/browse/YARN-4677) | RMNodeResourceUpdateEvent update from scheduler can lead to race condition |  Major | graceful, resourcemanager, scheduler | Brook Zhou | Wilfred Spiegelenburg |
| [HADOOP-15137](https://issues.apache.org/jira/browse/HADOOP-15137) | ClassNotFoundException: org.apache.hadoop.yarn.server.api.DistributedSchedulingAMProtocol when using hadoop-client-minicluster |  Major | . | Jeff Zhang | Bharat Viswanadham |
| [HADOOP-15514](https://issues.apache.org/jira/browse/HADOOP-15514) | NoClassDefFoundError for TimelineCollectorManager when starting MiniYARNCluster |  Major | . | Jeff Zhang | Rohith Sharma K S |
| [HADOOP-15513](https://issues.apache.org/jira/browse/HADOOP-15513) | Add additional test cases to cover some corner cases for FileUtil#symlink |  Major | . | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [HADOOP-15516](https://issues.apache.org/jira/browse/HADOOP-15516) | Add test cases to cover FileUtil#readLink |  Minor | . | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [HADOOP-15506](https://issues.apache.org/jira/browse/HADOOP-15506) | Upgrade Azure Storage Sdk version to 7.0.0 and update corresponding code blocks |  Minor | fs/azure | Esfandiar Manii | Esfandiar Manii |
| [YARN-6931](https://issues.apache.org/jira/browse/YARN-6931) | Make the aggregation interval in AppLevelTimelineCollector configurable |  Minor | timelineserver | Haibo Chen | Abhishek Modi |
| [HADOOP-15529](https://issues.apache.org/jira/browse/HADOOP-15529) | ContainerLaunch#testInvalidEnvVariableSubstitutionType is not supported in Windows |  Minor | . | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [YARN-8411](https://issues.apache.org/jira/browse/YARN-8411) | Enable stopped system services to be started during RM start |  Critical | . | Billie Rinaldi | Billie Rinaldi |
| [YARN-8259](https://issues.apache.org/jira/browse/YARN-8259) | Revisit liveliness checks for Docker containers |  Blocker | . | Shane Kumpf | Shane Kumpf |
| [HADOOP-15537](https://issues.apache.org/jira/browse/HADOOP-15537) | Clean up ContainerLaunch and ContainerExecutor pre-HADOOP-15528 |  Minor | . | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [HDFS-13186](https://issues.apache.org/jira/browse/HDFS-13186) | [PROVIDED Phase 2] Multipart Uploader API |  Major | . | Ewan Higgs | Ewan Higgs |
| [HADOOP-15533](https://issues.apache.org/jira/browse/HADOOP-15533) | Make WASB listStatus messages consistent |  Trivial | fs/azure | Esfandiar Manii | Esfandiar Manii |
| [HADOOP-14918](https://issues.apache.org/jira/browse/HADOOP-14918) | Remove the Local Dynamo DB test option |  Major | fs/s3 | Steve Loughran | Gabor Bota |
| [HADOOP-14396](https://issues.apache.org/jira/browse/HADOOP-14396) | Add builder interface to FileContext |  Major | fs | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HADOOP-15458](https://issues.apache.org/jira/browse/HADOOP-15458) | TestLocalFileSystem#testFSOutputStreamBuilder fails on Windows |  Minor | test | Xiao Liang | Xiao Liang |
| [HADOOP-15416](https://issues.apache.org/jira/browse/HADOOP-15416) | s3guard diff assert failure if source path not found |  Minor | fs/s3 | Steve Loughran | Gabor Bota |
| [HADOOP-15423](https://issues.apache.org/jira/browse/HADOOP-15423) | Merge fileCache and dirCache into one single cache in LocalMetadataStore |  Minor | . | Gabor Bota | Gabor Bota |
| [YARN-8465](https://issues.apache.org/jira/browse/YARN-8465) | Dshell docker container gets marked as lost after NM restart |  Major | yarn-native-services | Yesha Vora | Shane Kumpf |
| [YARN-8485](https://issues.apache.org/jira/browse/YARN-8485) | Priviledged container app launch is failing intermittently |  Major | yarn-native-services | Yesha Vora | Eric Yang |
| [HDFS-13381](https://issues.apache.org/jira/browse/HDFS-13381) | [SPS]: Use DFSUtilClient#makePathFromFileId() to prepare satisfier file path |  Major | . | Rakesh R | Rakesh R |
| [HADOOP-15215](https://issues.apache.org/jira/browse/HADOOP-15215) | s3guard set-capacity command to fail on read/write of 0 |  Minor | fs/s3 | Steve Loughran | Gabor Bota |
| [HDFS-13528](https://issues.apache.org/jira/browse/HDFS-13528) | RBF: If a directory exceeds quota limit then quota usage is not refreshed for other mount entries |  Major | . | Dibyendu Karmakar | Dibyendu Karmakar |
| [YARN-7556](https://issues.apache.org/jira/browse/YARN-7556) | Fair scheduler configuration should allow resource types in the minResources and maxResources properties |  Critical | fairscheduler | Daniel Templeton | Szilard Nemeth |
| [HDFS-13710](https://issues.apache.org/jira/browse/HDFS-13710) | RBF:  setQuota and getQuotaUsage should check the dfs.federation.router.quota.enable |  Major | federation, hdfs | yanghuafeng | yanghuafeng |
| [YARN-7899](https://issues.apache.org/jira/browse/YARN-7899) | [AMRMProxy] Stateful FederationInterceptor for pending requests |  Major | . | Botong Huang | Botong Huang |
| [HDFS-13726](https://issues.apache.org/jira/browse/HDFS-13726) | RBF: Fix RBF configuration links |  Minor | documentation | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-13475](https://issues.apache.org/jira/browse/HDFS-13475) | RBF: Admin cannot enforce Router enter SafeMode |  Major | . | Wei Yan | Chao Sun |
| [YARN-8299](https://issues.apache.org/jira/browse/YARN-8299) | Yarn Service Upgrade: Add GET APIs that returns instances matching query params |  Major | . | Chandni Singh | Chandni Singh |
| [HDFS-13733](https://issues.apache.org/jira/browse/HDFS-13733) | RBF: Add Web UI configurations and descriptions to RBF document |  Minor | documentation | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-6995](https://issues.apache.org/jira/browse/YARN-6995) | Improve use of ResourceNotFoundException in resource types code |  Minor | nodemanager, resourcemanager | Daniel Templeton | Szilard Nemeth |
| [HDFS-13743](https://issues.apache.org/jira/browse/HDFS-13743) | RBF: Router throws NullPointerException due to the invalid initialization of MountTableResolver |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-8301](https://issues.apache.org/jira/browse/YARN-8301) | Yarn Service Upgrade: Add documentation |  Critical | . | Chandni Singh | Chandni Singh |
| [HDFS-13076](https://issues.apache.org/jira/browse/HDFS-13076) | [SPS]: Cleanup work for HDFS-10285 |  Major | . | Rakesh R | Rakesh R |
| [HDFS-13583](https://issues.apache.org/jira/browse/HDFS-13583) | RBF: Router admin clrQuota is not synchronized with nameservice |  Major | . | Dibyendu Karmakar | Dibyendu Karmakar |
| [YARN-8546](https://issues.apache.org/jira/browse/YARN-8546) | Resource leak caused by a reserved container being released more than once under async scheduling |  Major | capacity scheduler | Weiwei Yang | Tao Yang |
| [YARN-8175](https://issues.apache.org/jira/browse/YARN-8175) | Add support for Node Labels in SLS |  Major | . | Abhishek Modi | Abhishek Modi |
| [HDFS-13084](https://issues.apache.org/jira/browse/HDFS-13084) | [SPS]: Fix the branch review comments |  Major | . | Uma Maheswara Rao G | Rakesh R |
| [YARN-8263](https://issues.apache.org/jira/browse/YARN-8263) | DockerClient still touches hadoop.tmp.dir |  Minor | . | Jason Lowe | Craig Condit |
| [YARN-7159](https://issues.apache.org/jira/browse/YARN-7159) | Normalize unit of resource objects in RM to avoid unit conversion in critical path |  Critical | nodemanager, resourcemanager | Wangda Tan | Manikandan R |
| [YARN-8287](https://issues.apache.org/jira/browse/YARN-8287) | Update documentation and yarn-default related to the Docker runtime |  Minor | . | Shane Kumpf | Craig Condit |
| [YARN-8624](https://issues.apache.org/jira/browse/YARN-8624) | Cleanup ENTRYPOINT documentation |  Minor | . | Craig Condit | Craig Condit |
| [YARN-7089](https://issues.apache.org/jira/browse/YARN-7089) | Mark the log-aggregation-controller APIs as public |  Major | . | Xuan Gong | Zian Chen |
| [HADOOP-15400](https://issues.apache.org/jira/browse/HADOOP-15400) | Improve S3Guard documentation on Authoritative Mode implementation |  Minor | fs/s3 | Aaron Fabbri | Gabor Bota |
| [YARN-8136](https://issues.apache.org/jira/browse/YARN-8136) | Add version attribute to site doc examples and quickstart |  Major | site | Gour Saha | Eric Yang |
| [YARN-8588](https://issues.apache.org/jira/browse/YARN-8588) | Logging improvements for better debuggability |  Major | . | Suma Shivaprasad | Suma Shivaprasad |
| [YARN-8520](https://issues.apache.org/jira/browse/YARN-8520) | Document best practice for user management |  Major | documentation, yarn | Eric Yang | Eric Yang |
| [HDFS-13808](https://issues.apache.org/jira/browse/HDFS-13808) | [SPS]: Remove unwanted FSNamesystem #isFileOpenedForWrite() and #getFileInfo() function |  Minor | . | Rakesh R | Rakesh R |
| [HADOOP-15576](https://issues.apache.org/jira/browse/HADOOP-15576) | S3A  Multipart Uploader to work with S3Guard and encryption |  Blocker | fs/s3 | Steve Loughran | Ewan Higgs |
| [YARN-8561](https://issues.apache.org/jira/browse/YARN-8561) | [Submarine] Initial implementation: Training job submission and job history retrieval |  Major | . | Wangda Tan | Wangda Tan |
| [HADOOP-15645](https://issues.apache.org/jira/browse/HADOOP-15645) | ITestS3GuardToolLocal.testDiffCommand fails if bucket has per-bucket binding to DDB |  Blocker | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-7417](https://issues.apache.org/jira/browse/YARN-7417) | re-factory IndexedFileAggregatedLogsBlock and TFileAggregatedLogsBlock to remove duplicate codes |  Major | . | Xuan Gong | Zian Chen |
| [YARN-8160](https://issues.apache.org/jira/browse/YARN-8160) | Yarn Service Upgrade: Support upgrade of service that use docker containers |  Major | . | Chandni Singh | Chandni Singh |
| [HADOOP-15552](https://issues.apache.org/jira/browse/HADOOP-15552) | Move logging APIs over to slf4j in hadoop-tools - Part2 |  Major | . | Giovanni Matteo Fumarola | Ian Pickering |
| [HADOOP-15642](https://issues.apache.org/jira/browse/HADOOP-15642) | Update aws-sdk version to 1.11.375 |  Blocker | build, fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-14154](https://issues.apache.org/jira/browse/HADOOP-14154) | Persist isAuthoritative bit in DynamoDBMetaStore (authoritative mode support) |  Minor | fs/s3 | Rajesh Balamohan | Gabor Bota |
| [HADOOP-14624](https://issues.apache.org/jira/browse/HADOOP-14624) | Add GenericTestUtils.DelayAnswer that accept slf4j logger API |  Major | . | Wenxin He | Wenxin He |
| [HDFS-13750](https://issues.apache.org/jira/browse/HDFS-13750) | RBF: Router ID in RouterRpcClient is always null |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-8129](https://issues.apache.org/jira/browse/YARN-8129) | Improve error message for invalid value in fields attribute |  Minor | ATSv2 | Charan Hebri | Abhishek Modi |
| [YARN-7494](https://issues.apache.org/jira/browse/YARN-7494) | Add muti-node lookup mechanism and pluggable nodes sorting policies to optimize placement decision |  Major | capacity scheduler | Sunil Govindan | Sunil Govindan |
| [YARN-8298](https://issues.apache.org/jira/browse/YARN-8298) | Yarn Service Upgrade: Support express upgrade of a service |  Major | . | Chandni Singh | Chandni Singh |
| [YARN-8015](https://issues.apache.org/jira/browse/YARN-8015) | Support all types of placement constraint support for Capacity Scheduler |  Critical | capacity scheduler | Weiwei Yang | Weiwei Yang |
| [HDFS-13848](https://issues.apache.org/jira/browse/HDFS-13848) | Refactor NameNode failover proxy providers |  Major | ha, hdfs-client | Konstantin Shvachko | Konstantin Shvachko |
| [YARN-8705](https://issues.apache.org/jira/browse/YARN-8705) | Refactor the UAM heartbeat thread in preparation for YARN-8696 |  Major | . | Botong Huang | Botong Huang |
| [HADOOP-15699](https://issues.apache.org/jira/browse/HADOOP-15699) | Fix some of testContainerManager failures in Windows |  Major | . | Botong Huang | Botong Huang |
| [YARN-8697](https://issues.apache.org/jira/browse/YARN-8697) | LocalityMulticastAMRMProxyPolicy should fallback to random sub-cluster when cannot resolve resource |  Major | . | Botong Huang | Botong Huang |
| [HDFS-13634](https://issues.apache.org/jira/browse/HDFS-13634) | RBF: Configurable value in xml for async connection request queue size. |  Major | federation | CR Hota | CR Hota |
| [YARN-8642](https://issues.apache.org/jira/browse/YARN-8642) | Add support for tmpfs mounts with the Docker runtime |  Major | . | Shane Kumpf | Craig Condit |
| [HADOOP-15667](https://issues.apache.org/jira/browse/HADOOP-15667) | FileSystemMultipartUploader should verify that UploadHandle has non-0 length |  Major | fs/s3 | Ewan Higgs | Ewan Higgs |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-8016](https://issues.apache.org/jira/browse/YARN-8016) | Refine PlacementRule interface and add a app-name queue mapping rule as an example |  Major | . | Zian Chen | Zian Chen |
| [YARN-8091](https://issues.apache.org/jira/browse/YARN-8091) | Revisit checkUserAccessToQueue RM REST API |  Critical | . | Wangda Tan | Wangda Tan |
| [YARN-8274](https://issues.apache.org/jira/browse/YARN-8274) | Docker command error during container relaunch |  Critical | . | Billie Rinaldi | Jason Lowe |
| [YARN-8080](https://issues.apache.org/jira/browse/YARN-8080) | YARN native service should support component restart policy |  Critical | . | Wangda Tan | Suma Shivaprasad |
| [HADOOP-15482](https://issues.apache.org/jira/browse/HADOOP-15482) | Upgrade jackson-databind to version 2.9.5 |  Major | . | Lokesh Jain | Lokesh Jain |
| [YARN-7668](https://issues.apache.org/jira/browse/YARN-7668) | Remove unused variables from ContainerLocalizer |  Trivial | . | Ray Chiang | Dedunu Dhananjaya |
| [YARN-8412](https://issues.apache.org/jira/browse/YARN-8412) | Move ResourceRequest.clone logic everywhere into a proper API |  Minor | . | Botong Huang | Botong Huang |
| [HADOOP-15483](https://issues.apache.org/jira/browse/HADOOP-15483) | Upgrade jquery to version 3.3.1 |  Major | . | Lokesh Jain | Lokesh Jain |
| [YARN-8506](https://issues.apache.org/jira/browse/YARN-8506) | Make GetApplicationsRequestPBImpl thread safe |  Critical | . | Wangda Tan | Wangda Tan |
| [YARN-8545](https://issues.apache.org/jira/browse/YARN-8545) | YARN native service should return container if launch failed |  Critical | . | Wangda Tan | Chandni Singh |
| [HDFS-11610](https://issues.apache.org/jira/browse/HDFS-11610) | sun.net.spi.nameservice.NameService has moved to a new location |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [HDFS-13788](https://issues.apache.org/jira/browse/HDFS-13788) | Update EC documentation about rack fault tolerance |  Major | documentation, erasure-coding | Xiao Chen | Kitti Nanasi |
| [YARN-8488](https://issues.apache.org/jira/browse/YARN-8488) | YARN service/components/instances should have SUCCEEDED/FAILED states |  Major | yarn-native-services | Wangda Tan | Suma Shivaprasad |
