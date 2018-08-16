
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

## Release 3.1.1 - 2018-08-02



### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-14667](https://issues.apache.org/jira/browse/HADOOP-14667) | Flexible Visual Studio support |  Major | build | Allen Wittenauer | Allen Wittenauer |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-13056](https://issues.apache.org/jira/browse/HDFS-13056) | Expose file-level composite CRCs in HDFS which are comparable across different instances/layouts |  Major | datanode, distcp, erasure-coding, federation, hdfs | Dennis Huo | Dennis Huo |
| [HDFS-13283](https://issues.apache.org/jira/browse/HDFS-13283) | Percentage based Reserved Space Calculation for DataNode |  Major | datanode, hdfs | Lukas Majercak | Lukas Majercak |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-8028](https://issues.apache.org/jira/browse/YARN-8028) | Support authorizeUserAccessToQueue in RMWebServices |  Major | . | Wangda Tan | Wangda Tan |
| [HADOOP-15332](https://issues.apache.org/jira/browse/HADOOP-15332) | Fix typos in hadoop-aws markdown docs |  Minor | . | Gabor Bota | Gabor Bota |
| [HADOOP-15330](https://issues.apache.org/jira/browse/HADOOP-15330) | Remove jdk1.7 profile from hadoop-annotations module |  Minor | . | Akira Ajisaka | fang zhenyi |
| [HADOOP-15342](https://issues.apache.org/jira/browse/HADOOP-15342) | Update ADLS connector to use the current SDK version (2.2.7) |  Major | fs/adl | Atul Sikaria | Atul Sikaria |
| [YARN-1151](https://issues.apache.org/jira/browse/YARN-1151) | Ability to configure auxiliary services from HDFS-based JAR files |  Major | nodemanager | john lilley | Xuan Gong |
| [HDFS-13418](https://issues.apache.org/jira/browse/HDFS-13418) |  NetworkTopology should be configurable when enable DFSNetworkTopology |  Major | . | Tao Jie | Tao Jie |
| [HDFS-13439](https://issues.apache.org/jira/browse/HDFS-13439) | Add test case for read block operation when it is moved |  Major | . | Ajay Kumar | Ajay Kumar |
| [HDFS-13462](https://issues.apache.org/jira/browse/HDFS-13462) | Add BIND\_HOST configuration for JournalNode's HTTP and RPC Servers |  Major | hdfs, journal-node | Lukas Majercak | Lukas Majercak |
| [YARN-8140](https://issues.apache.org/jira/browse/YARN-8140) | Improve log message when launch cmd is ran for stopped yarn service |  Major | yarn-native-services | Yesha Vora | Eric Yang |
| [MAPREDUCE-7086](https://issues.apache.org/jira/browse/MAPREDUCE-7086) | Add config to allow FileInputFormat to ignore directories when recursive=false |  Major | . | Sergey Shelukhin | Sergey Shelukhin |
| [HDFS-12981](https://issues.apache.org/jira/browse/HDFS-12981) | renameSnapshot a Non-Existent snapshot to itself should throw error |  Minor | hdfs | Sailesh Patel | Kitti Nanasi |
| [YARN-8239](https://issues.apache.org/jira/browse/YARN-8239) | [UI2] Clicking on Node Manager UI under AM container info / App Attempt page goes to old RM UI |  Major | yarn-ui-v2 | Sumana Sathish | Sunil Govindan |
| [YARN-8260](https://issues.apache.org/jira/browse/YARN-8260) | [UI2] Per-application tracking URL is no longer available in YARN UI2 |  Major | yarn-ui-v2 | Sunil Govindan | Sunil Govindan |
| [YARN-8201](https://issues.apache.org/jira/browse/YARN-8201) | Skip stacktrace of few exception from ClientRMService |  Minor | . | Bibin A Chundatt | Bilwa S T |
| [HADOOP-15441](https://issues.apache.org/jira/browse/HADOOP-15441) | Log kms url and token service at debug level. |  Minor | . | Wei-Chiu Chuang | Gabor Bota |
| [HDFS-13544](https://issues.apache.org/jira/browse/HDFS-13544) | Improve logging for JournalNode in federated cluster |  Major | federation, hdfs | Hanisha Koneru | Hanisha Koneru |
| [YARN-8249](https://issues.apache.org/jira/browse/YARN-8249) | Few REST api's in RMWebServices are missing static user check |  Critical | webapp, yarn | Sunil Govindan | Sunil Govindan |
| [HDFS-13512](https://issues.apache.org/jira/browse/HDFS-13512) | WebHdfs getFileStatus doesn't return ecPolicy |  Major | . | Ajay Kumar | Ajay Kumar |
| [HADOOP-15250](https://issues.apache.org/jira/browse/HADOOP-15250) | Split-DNS MultiHomed Server Network Cluster Network IPC Client Bind Addr Wrong |  Critical | ipc, net | Greg Senia | Ajay Kumar |
| [HDFS-13589](https://issues.apache.org/jira/browse/HDFS-13589) | Add dfsAdmin command to query if "upgrade" is finalized |  Major | hdfs | Hanisha Koneru | Hanisha Koneru |
| [HADOOP-15486](https://issues.apache.org/jira/browse/HADOOP-15486) | Make NetworkTopology#netLock fair |  Major | net | Nanda kumar | Nanda kumar |
| [YARN-8213](https://issues.apache.org/jira/browse/YARN-8213) | Add Capacity Scheduler performance metrics |  Critical | capacityscheduler, metrics | Weiwei Yang | Weiwei Yang |
| [HDFS-13628](https://issues.apache.org/jira/browse/HDFS-13628) | Update Archival Storage doc for Provided Storage |  Major | documentation | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-15449](https://issues.apache.org/jira/browse/HADOOP-15449) | Increase default timeout of ZK session to avoid frequent NameNode failover |  Critical | common | Karthik Palanisamy | Karthik Palanisamy |
| [YARN-8333](https://issues.apache.org/jira/browse/YARN-8333) | Load balance YARN services using RegistryDNS multiple A records |  Major | yarn-native-services | Eric Yang | Eric Yang |
| [HDFS-13602](https://issues.apache.org/jira/browse/HDFS-13602) | Add checkOperation(WRITE) checks in FSNamesystem |  Major | ha, namenode | Erik Krogen | Chao Sun |
| [HDFS-13155](https://issues.apache.org/jira/browse/HDFS-13155) | BlockPlacementPolicyDefault.chooseTargetInOrder Not Checking Return Value for NULL |  Minor | namenode | BELUGA BEHR | Zsolt Venczel |
| [YARN-8389](https://issues.apache.org/jira/browse/YARN-8389) | Improve the description of machine-list property in Federation docs |  Major | documentation, federation | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-13511](https://issues.apache.org/jira/browse/HDFS-13511) | Provide specialized exception when block length cannot be obtained |  Major | . | Ted Yu | Gabor Bota |
| [HDFS-13659](https://issues.apache.org/jira/browse/HDFS-13659) | Add more test coverage for contentSummary for snapshottable path |  Major | namenode, test | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-8400](https://issues.apache.org/jira/browse/YARN-8400) | Fix typos in YARN Federation documentation page |  Trivial | . | Bibin A Chundatt | Giovanni Matteo Fumarola |
| [HADOOP-15499](https://issues.apache.org/jira/browse/HADOOP-15499) | Performance severe drop when running RawErasureCoderBenchmark with NativeRSRawErasureCoder |  Major | . | Sammi Chen | Sammi Chen |
| [HDFS-13653](https://issues.apache.org/jira/browse/HDFS-13653) | Make dfs.client.failover.random.order a per nameservice configuration |  Major | federation | Ekanth Sethuramalingam | Ekanth Sethuramalingam |
| [YARN-8394](https://issues.apache.org/jira/browse/YARN-8394) | Improve data locality documentation for Capacity Scheduler |  Major | . | Weiwei Yang | Weiwei Yang |
| [HDFS-13641](https://issues.apache.org/jira/browse/HDFS-13641) | Add metrics for edit log tailing |  Major | metrics | Chao Sun | Chao Sun |
| [HDFS-13686](https://issues.apache.org/jira/browse/HDFS-13686) | Add overall metrics for FSNamesystemLock |  Major | hdfs, namenode | Lukas Majercak | Lukas Majercak |
| [HDFS-13692](https://issues.apache.org/jira/browse/HDFS-13692) | StorageInfoDefragmenter floods log when compacting StorageInfo TreeSet |  Minor | . | Yiqun Lin | Bharat Viswanadham |
| [YARN-8214](https://issues.apache.org/jira/browse/YARN-8214) | Change default RegistryDNS port |  Major | . | Billie Rinaldi | Billie Rinaldi |
| [HDFS-13703](https://issues.apache.org/jira/browse/HDFS-13703) | Avoid allocation of CorruptedBlocks hashmap when no corrupted blocks are hit |  Major | performance | Todd Lipcon | Todd Lipcon |
| [HADOOP-15554](https://issues.apache.org/jira/browse/HADOOP-15554) | Improve JIT performance for Configuration parsing |  Minor | conf, performance | Todd Lipcon | Todd Lipcon |
| [HDFS-13714](https://issues.apache.org/jira/browse/HDFS-13714) | Fix TestNameNodePrunesMissingStorages test failures on Windows |  Major | hdfs, namenode, test | Lukas Majercak | Lukas Majercak |
| [HDFS-13712](https://issues.apache.org/jira/browse/HDFS-13712) | BlockReaderRemote.read() logging improvement |  Minor | hdfs-client | Gergo Repas | Gergo Repas |
| [YARN-8302](https://issues.apache.org/jira/browse/YARN-8302) | ATS v2 should handle HBase connection issue properly |  Major | ATSv2 | Yesha Vora | Billie Rinaldi |
| [HDFS-13674](https://issues.apache.org/jira/browse/HDFS-13674) | Improve documentation on Metrics |  Minor | documentation, metrics | Chao Sun | Chao Sun |
| [HDFS-13719](https://issues.apache.org/jira/browse/HDFS-13719) | Docs around dfs.image.transfer.timeout are misleading |  Major | . | Kitti Nanasi | Kitti Nanasi |
| [HADOOP-15598](https://issues.apache.org/jira/browse/HADOOP-15598) | DataChecksum calculate checksum is contented on hashtable synchronization |  Major | common | Prasanth Jayachandran | Prasanth Jayachandran |
| [YARN-8501](https://issues.apache.org/jira/browse/YARN-8501) | Reduce complexity of RMWebServices' getApps method |  Major | restapi | Szilard Nemeth | Szilard Nemeth |
| [HADOOP-15547](https://issues.apache.org/jira/browse/HADOOP-15547) | WASB: improve listStatus performance |  Major | fs/azure | Thomas Marquardt | Thomas Marquardt |
| [YARN-8155](https://issues.apache.org/jira/browse/YARN-8155) | Improve ATSv2 client logging in RM and NM publisher |  Major | . | Rohith Sharma K S | Abhishek Modi |
| [HADOOP-15476](https://issues.apache.org/jira/browse/HADOOP-15476) | fix logging for split-dns multihome |  Major | . | Ajay Kumar | Ajay Kumar |


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
| [YARN-7734](https://issues.apache.org/jira/browse/YARN-7734) | YARN-5418 breaks TestContainerLogsPage.testContainerLogPageAccess |  Major | . | Miklos Szegedi | Tao Yang |
| [HDFS-13087](https://issues.apache.org/jira/browse/HDFS-13087) | Snapshotted encryption zone information should be immutable |  Major | encryption | LiXin Ge | LiXin Ge |
| [HADOOP-12862](https://issues.apache.org/jira/browse/HADOOP-12862) | LDAP Group Mapping over SSL can not specify trust store |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-15317](https://issues.apache.org/jira/browse/HADOOP-15317) | Improve NetworkTopology chooseRandom's loop |  Major | . | Xiao Chen | Xiao Chen |
| [HADOOP-15355](https://issues.apache.org/jira/browse/HADOOP-15355) | TestCommonConfigurationFields is broken by HADOOP-15312 |  Major | test | Konstantin Shvachko | LiXin Ge |
| [YARN-7764](https://issues.apache.org/jira/browse/YARN-7764) | Findbugs warning: Resource#getResources may expose internal representation |  Major | api | Weiwei Yang | Weiwei Yang |
| [YARN-8106](https://issues.apache.org/jira/browse/YARN-8106) | Update LogAggregationIndexedFileController to use readFull instead read to avoid IOException while loading log meta |  Critical | log-aggregation | Prabhu Joseph | Prabhu Joseph |
| [YARN-8115](https://issues.apache.org/jira/browse/YARN-8115) | [UI2] URL data like nodeHTTPAddress must be encoded in UI before using to access NM |  Major | yarn-ui-v2 | Sunil Govindan | Sreenath Somarajapuram |
| [HDFS-13350](https://issues.apache.org/jira/browse/HDFS-13350) | Negative legacy block ID will confuse Erasure Coding to be considered as striped block |  Major | erasure-coding | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [YARN-8119](https://issues.apache.org/jira/browse/YARN-8119) | [UI2] Timeline Server address' url scheme should be removed while accessing via KNOX |  Major | yarn-ui-v2 | Sunil Govindan | Sunil Govindan |
| [YARN-8083](https://issues.apache.org/jira/browse/YARN-8083) | [UI2] All YARN related configurations are paged together in conf page |  Major | yarn-ui-v2 | Zoltan Haindrich | Gergely Novák |
| [HADOOP-15366](https://issues.apache.org/jira/browse/HADOOP-15366) | Add a helper shutdown routine in HadoopExecutor to ensure clean shutdown |  Minor | . | Shashikant Banerjee | Shashikant Banerjee |
| [YARN-7905](https://issues.apache.org/jira/browse/YARN-7905) | Parent directory permission incorrect during public localization |  Critical | . | Bibin A Chundatt | Bilwa S T |
| [HADOOP-15374](https://issues.apache.org/jira/browse/HADOOP-15374) | Add links of the new features of 3.1.0 to the top page |  Major | documentation | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-7804](https://issues.apache.org/jira/browse/YARN-7804) | Refresh action on Grid view page should not be redirected to graph view |  Major | yarn-ui-v2 | Yesha Vora | Gergely Novák |
| [HDFS-13420](https://issues.apache.org/jira/browse/HDFS-13420) | License header is displayed in ArchivalStorage/MemoryStorage html pages |  Minor | documentation | Akira Ajisaka | Akira Ajisaka |
| [HDFS-13328](https://issues.apache.org/jira/browse/HDFS-13328) | Abstract ReencryptionHandler recursive logic in separate class. |  Major | namenode | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HADOOP-15357](https://issues.apache.org/jira/browse/HADOOP-15357) | Configuration.getPropsWithPrefix no longer does variable substitution |  Major | . | Jim Brennan | Jim Brennan |
| [YARN-7984](https://issues.apache.org/jira/browse/YARN-7984) | Delete registry entries from ZK on ServiceClient stop and clean up stop/destroy behavior |  Critical | yarn-native-services | Billie Rinaldi | Billie Rinaldi |
| [YARN-8133](https://issues.apache.org/jira/browse/YARN-8133) | Doc link broken for yarn-service from overview page. |  Blocker | yarn-native-services | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-8116](https://issues.apache.org/jira/browse/YARN-8116) | Nodemanager fails with NumberFormatException: For input string: "" |  Critical | . | Yesha Vora | Chandni Singh |
| [MAPREDUCE-7062](https://issues.apache.org/jira/browse/MAPREDUCE-7062) | Update mapreduce.job.tags description for making use for ATSv2 purpose. |  Major | . | Charan Hebri | Charan Hebri |
| [YARN-8073](https://issues.apache.org/jira/browse/YARN-8073) | TimelineClientImpl doesn't honor yarn.timeline-service.versions configuration |  Major | . | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-8127](https://issues.apache.org/jira/browse/YARN-8127) | Resource leak when async scheduling is enabled |  Critical | . | Weiwei Yang | Tao Yang |
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
| [YARN-8165](https://issues.apache.org/jira/browse/YARN-8165) | Incorrect queue name logging in AbstractContainerAllocator |  Trivial | capacityscheduler | Weiwei Yang | Weiwei Yang |
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
| [HDFS-13388](https://issues.apache.org/jira/browse/HDFS-13388) | RequestHedgingProxyProvider calls multiple configured NNs all the time |  Major | hdfs-client | Jinglun | Jinglun |
| [YARN-7956](https://issues.apache.org/jira/browse/YARN-7956) | [UI2] Avoid duplicating Components link under Services/\<ServiceName\>/Components |  Major | yarn-ui-v2 | Yesha Vora | Yesha Vora |
| [HDFS-13433](https://issues.apache.org/jira/browse/HDFS-13433) | webhdfs requests can be routed incorrectly in federated cluster |  Critical | webhdfs | Arpit Agarwal | Arpit Agarwal |
| [HDFS-13408](https://issues.apache.org/jira/browse/HDFS-13408) | MiniDFSCluster to support being built on randomized base directory |  Major | test | Xiao Liang | Xiao Liang |
| [HDFS-13356](https://issues.apache.org/jira/browse/HDFS-13356) | Balancer:Set default value of minBlockSize to 10mb |  Major | balancer & mover | Bharat Viswanadham | Bharat Viswanadham |
| [HADOOP-15390](https://issues.apache.org/jira/browse/HADOOP-15390) | Yarn RM logs flooded by DelegationTokenRenewer trying to renew KMS tokens |  Critical | . | Xiao Chen | Xiao Chen |
| [HDFS-13336](https://issues.apache.org/jira/browse/HDFS-13336) | Test cases of TestWriteToReplica failed in windows |  Major | . | Xiao Liang | Xiao Liang |
| [YARN-8196](https://issues.apache.org/jira/browse/YARN-8196) | yarn.webapp.api-service.enable should be highlighted in the quickstart |  Trivial | documentation | Davide  Vergari | Billie Rinaldi |
| [YARN-8183](https://issues.apache.org/jira/browse/YARN-8183) | Fix ConcurrentModificationException inside RMAppAttemptMetrics#convertAtomicLongMaptoLongMap |  Critical | yarn | Sumana Sathish | Suma Shivaprasad |
| [YARN-8188](https://issues.apache.org/jira/browse/YARN-8188) | RM Nodes UI data table index for sorting column need to be corrected post Application tags display |  Major | resourcemanager, webapp | Weiwei Yang | Weiwei Yang |
| [HADOOP-15411](https://issues.apache.org/jira/browse/HADOOP-15411) | AuthenticationFilter should use Configuration.getPropsWithPrefix instead of iterator |  Critical | . | Suma Shivaprasad | Suma Shivaprasad |
| [MAPREDUCE-7042](https://issues.apache.org/jira/browse/MAPREDUCE-7042) | Killed MR job data does not move to mapreduce.jobhistory.done-dir when ATS v2 is enabled |  Major | . | Yesha Vora | Xuan Gong |
| [YARN-8205](https://issues.apache.org/jira/browse/YARN-8205) | Application State is not updated to ATS if AM launching is delayed. |  Critical | . | Sumana Sathish | Rohith Sharma K S |
| [YARN-8004](https://issues.apache.org/jira/browse/YARN-8004) | Add unit tests for inter queue preemption for dominant resource calculator |  Critical | yarn | Sumana Sathish | Zian Chen |
| [YARN-8208](https://issues.apache.org/jira/browse/YARN-8208) | Add log statement for Docker client configuration file at INFO level |  Minor | yarn-native-services | Yesha Vora | Yesha Vora |
| [YARN-8211](https://issues.apache.org/jira/browse/YARN-8211) | Yarn registry dns log finds BufferUnderflowException on port ping |  Major | yarn-native-services | Yesha Vora | Eric Yang |
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
| [YARN-8209](https://issues.apache.org/jira/browse/YARN-8209) | NPE in DeletionService |  Critical | . | Chandni Singh | Eric Badger |
| [HDFS-13481](https://issues.apache.org/jira/browse/HDFS-13481) | TestRollingFileSystemSinkWithHdfs#testFlushThread: test failed intermittently |  Major | hdfs | Gabor Bota | Gabor Bota |
| [YARN-8217](https://issues.apache.org/jira/browse/YARN-8217) | RmAuthenticationFilterInitializer /TimelineAuthenticationFilterInitializer should use Configuration.getPropsWithPrefix instead of iterator |  Major | . | Suma Shivaprasad | Suma Shivaprasad |
| [YARN-7818](https://issues.apache.org/jira/browse/YARN-7818) | Remove privileged operation warnings during container launch for the ContainerRuntimes |  Major | . | Yesha Vora | Shane Kumpf |
| [YARN-8223](https://issues.apache.org/jira/browse/YARN-8223) | ClassNotFoundException when auxiliary service is loaded from HDFS |  Blocker | . | Charan Hebri | Zian Chen |
| [YARN-8079](https://issues.apache.org/jira/browse/YARN-8079) | Support static and archive unmodified local resources in service AM |  Critical | . | Wangda Tan | Suma Shivaprasad |
| [YARN-8025](https://issues.apache.org/jira/browse/YARN-8025) | UsersManangers#getComputedResourceLimitForActiveUsers throws NPE due to preComputedActiveUserLimit is empty |  Major | yarn | Jiandan Yang | Tao Yang |
| [YARN-8251](https://issues.apache.org/jira/browse/YARN-8251) | [UI2] Clicking on Application link at the header goes to Diagnostics Tab instead of AppAttempt Tab |  Major | yarn-ui-v2 | Sumana Sathish | Yesha Vora |
| [YARN-8232](https://issues.apache.org/jira/browse/YARN-8232) | RMContainer lost queue name when RM HA happens |  Major | resourcemanager | Hu Ziqian | Hu Ziqian |
| [YARN-7894](https://issues.apache.org/jira/browse/YARN-7894) | Improve ATS response for DS\_CONTAINER when container launch fails |  Major | timelineserver | Charan Hebri | Chandni Singh |
| [YARN-8264](https://issues.apache.org/jira/browse/YARN-8264) | [UI2 GPU] GPU Info tab disappears if we click any sub link under List of Applications or List of Containers |  Major | . | Sumana Sathish | Sunil Govindan |
| [HDFS-13537](https://issues.apache.org/jira/browse/HDFS-13537) | TestHdfsHelper does not generate jceks path properly for relative path in Windows |  Major | . | Xiao Liang | Xiao Liang |
| [YARN-8202](https://issues.apache.org/jira/browse/YARN-8202) | DefaultAMSProcessor should properly check units of requested custom resource types against minimum/maximum allocation |  Blocker | . | Szilard Nemeth | Szilard Nemeth |
| [HADOOP-15446](https://issues.apache.org/jira/browse/HADOOP-15446) | WASB: PageBlobInputStream.skip breaks HBASE replication |  Major | fs/azure | Thomas Marquardt | Thomas Marquardt |
| [YARN-7003](https://issues.apache.org/jira/browse/YARN-7003) | DRAINING state of queues is not recovered after RM restart |  Major | capacityscheduler | Tao Yang | Tao Yang |
| [YARN-8244](https://issues.apache.org/jira/browse/YARN-8244) |  TestContainerSchedulerQueuing.testStartMultipleContainers failed |  Major | . | Miklos Szegedi | Jim Brennan |
| [YARN-8265](https://issues.apache.org/jira/browse/YARN-8265) | Service AM should retrieve new IP for docker container relaunched by NM |  Critical | yarn-native-services | Eric Yang | Billie Rinaldi |
| [YARN-8271](https://issues.apache.org/jira/browse/YARN-8271) | [UI2] Improve labeling of certain tables |  Major | yarn-ui-v2 | Yesha Vora | Yesha Vora |
| [YARN-8288](https://issues.apache.org/jira/browse/YARN-8288) | Fix wrong number of table columns in Resource Model doc |  Major | . | Weiwei Yang | Weiwei Yang |
| [HDFS-13539](https://issues.apache.org/jira/browse/HDFS-13539) | DFSStripedInputStream NPE when reportCheckSumFailure |  Major | . | Xiao Chen | Xiao Chen |
| [YARN-8266](https://issues.apache.org/jira/browse/YARN-8266) | [UI2] Clicking on application from cluster view should redirect to application attempt page |  Major | yarn-ui-v2 | Yesha Vora | Yesha Vora |
| [YARN-8166](https://issues.apache.org/jira/browse/YARN-8166) | [UI2] Service page header links are broken |  Major | yarn-ui-v2 | Yesha Vora | Yesha Vora |
| [YARN-8236](https://issues.apache.org/jira/browse/YARN-8236) | Invalid kerberos principal file name cause NPE in native service |  Critical | yarn-native-services | Sunil Govindan | Gour Saha |
| [YARN-8278](https://issues.apache.org/jira/browse/YARN-8278) | DistributedScheduling is not working in HA |  Blocker | . | Bibin A Chundatt | Bibin A Chundatt |
| [HADOOP-15442](https://issues.apache.org/jira/browse/HADOOP-15442) | ITestS3AMetrics.testMetricsRegister can't know metrics source's name |  Major | fs/s3, metrics | Sean Mackrory | Sean Mackrory |
| [YARN-8300](https://issues.apache.org/jira/browse/YARN-8300) | Fix NPE in DefaultUpgradeComponentsFinder |  Major | yarn | Suma Shivaprasad | Suma Shivaprasad |
| [HDFS-13581](https://issues.apache.org/jira/browse/HDFS-13581) | DN UI logs link is broken when https is enabled |  Minor | datanode | Namit Maheshwari | Shashikant Banerjee |
| [YARN-8128](https://issues.apache.org/jira/browse/YARN-8128) | Document better the per-node per-app file limit in YARN log aggregation |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-8293](https://issues.apache.org/jira/browse/YARN-8293) | In YARN Services UI, "User Name for service" should be completely removed in secure clusters |  Major | yarn-ui-v2 | Sunil Govindan | Sunil Govindan |
| [YARN-8141](https://issues.apache.org/jira/browse/YARN-8141) | YARN Native Service: Respect YARN\_CONTAINER\_RUNTIME\_DOCKER\_LOCAL\_RESOURCE\_MOUNTS specified in service spec |  Critical | yarn-native-services | Wangda Tan | Chandni Singh |
| [YARN-8296](https://issues.apache.org/jira/browse/YARN-8296) | Update YarnServiceApi documentation and yarn service UI code to remove references to unique\_component\_support |  Major | yarn-native-services, yarn-ui-v2 | Suma Shivaprasad | Suma Shivaprasad |
| [HDFS-13586](https://issues.apache.org/jira/browse/HDFS-13586) | Fsync fails on directories on Windows |  Critical | datanode, hdfs | Lukas Majercak | Lukas Majercak |
| [HADOOP-15478](https://issues.apache.org/jira/browse/HADOOP-15478) | WASB: hflush() and hsync() regression |  Major | fs/azure | Thomas Marquardt | Thomas Marquardt |
| [YARN-8179](https://issues.apache.org/jira/browse/YARN-8179) | Preemption does not happen due to natural\_termination\_factor when DRF is used |  Major | . | kyungwan nam | kyungwan nam |
| [HADOOP-15450](https://issues.apache.org/jira/browse/HADOOP-15450) | Avoid fsync storm triggered by DiskChecker and handle disk full situation |  Blocker | . | Kihwal Lee | Arpit Agarwal |
| [YARN-8290](https://issues.apache.org/jira/browse/YARN-8290) | SystemMetricsPublisher.appACLsUpdated should be invoked after application information is published to ATS to avoid "User is not set in the application report" Exception |  Critical | . | Yesha Vora | Eric Yang |
| [YARN-8332](https://issues.apache.org/jira/browse/YARN-8332) | Incorrect min/max allocation property name in resource types doc |  Critical | documentation | Weiwei Yang | Weiwei Yang |
| [HDFS-13601](https://issues.apache.org/jira/browse/HDFS-13601) | Optimize ByteString conversions in PBHelper |  Major | . | Andrew Wang | Andrew Wang |
| [HDFS-13540](https://issues.apache.org/jira/browse/HDFS-13540) | DFSStripedInputStream should only allocate new buffers when reading |  Major | . | Xiao Chen | Xiao Chen |
| [YARN-8297](https://issues.apache.org/jira/browse/YARN-8297) | Incorrect ATS Url used for Wire encrypted cluster |  Blocker | yarn-ui-v2 | Yesha Vora | Sunil Govindan |
| [HDFS-13588](https://issues.apache.org/jira/browse/HDFS-13588) | Fix TestFsDatasetImpl test failures on Windows |  Major | . | Xiao Liang | Xiao Liang |
| [YARN-8310](https://issues.apache.org/jira/browse/YARN-8310) | Handle old NMTokenIdentifier, AMRMTokenIdentifier, and ContainerTokenIdentifier formats |  Major | . | Robert Kanter | Robert Kanter |
| [YARN-8344](https://issues.apache.org/jira/browse/YARN-8344) | Missing nm.stop() in TestNodeManagerResync to fix testKillContainersOnResync |  Major | . | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [YARN-8327](https://issues.apache.org/jira/browse/YARN-8327) | Fix TestAggregatedLogFormat#testReadAcontainerLogs1 on Windows |  Major | log-aggregation | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [HDFS-13611](https://issues.apache.org/jira/browse/HDFS-13611) | Unsafe use of Text as a ConcurrentHashMap key in PBHelperClient |  Major | . | Andrew Wang | Andrew Wang |
| [YARN-8316](https://issues.apache.org/jira/browse/YARN-8316) | Diagnostic message should improve when yarn service fails to launch due to ATS unavailability |  Major | yarn-native-services | Yesha Vora | Billie Rinaldi |
| [YARN-8357](https://issues.apache.org/jira/browse/YARN-8357) | Yarn Service: NPE when service is saved first and then started. |  Critical | . | Chandni Singh | Chandni Singh |
| [HDFS-13618](https://issues.apache.org/jira/browse/HDFS-13618) | Fix TestDataNodeFaultInjector test failures on Windows |  Major | test | Xiao Liang | Xiao Liang |
| [HADOOP-15473](https://issues.apache.org/jira/browse/HADOOP-15473) | Configure serialFilter in KeyProvider to avoid UnrecoverableKeyException caused by JDK-8189997 |  Critical | kms | Gabor Bota | Gabor Bota |
| [YARN-8292](https://issues.apache.org/jira/browse/YARN-8292) | Fix the dominant resource preemption cannot happen when some of the resource vector becomes negative |  Critical | yarn | Sumana Sathish | Wangda Tan |
| [YARN-8338](https://issues.apache.org/jira/browse/YARN-8338) | TimelineService V1.5 doesn't come up after HADOOP-15406 |  Critical | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [YARN-8339](https://issues.apache.org/jira/browse/YARN-8339) | Service AM should localize static/archive resource types to container working directory instead of 'resources' |  Critical | yarn-native-services | Suma Shivaprasad | Suma Shivaprasad |
| [YARN-8369](https://issues.apache.org/jira/browse/YARN-8369) | Javadoc build failed due to "bad use of '\>'" |  Critical | build, docs | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-8362](https://issues.apache.org/jira/browse/YARN-8362) | Number of remaining retries are updated twice after a container failure in NM |  Critical | . | Chandni Singh | Chandni Singh |
| [YARN-8377](https://issues.apache.org/jira/browse/YARN-8377) | Javadoc build failed in hadoop-yarn-server-nodemanager |  Critical | build, docs | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-8368](https://issues.apache.org/jira/browse/YARN-8368) | yarn app start cli should print applicationId |  Critical | . | Yesha Vora | Rohith Sharma K S |
| [YARN-8350](https://issues.apache.org/jira/browse/YARN-8350) | NPE in service AM related to placement policy |  Critical | yarn-native-services | Billie Rinaldi | Gour Saha |
| [YARN-8367](https://issues.apache.org/jira/browse/YARN-8367) | Fix NPE in SingleConstraintAppPlacementAllocator when placement constraint in SchedulingRequest is null |  Major | scheduler | Gour Saha | Weiwei Yang |
| [YARN-8197](https://issues.apache.org/jira/browse/YARN-8197) | Tracking URL in the app state does not get redirected to MR ApplicationMaster for Running applications |  Critical | yarn | Sumana Sathish | Sunil Govindan |
| [YARN-8308](https://issues.apache.org/jira/browse/YARN-8308) | Yarn service app fails due to issues with Renew Token |  Major | yarn-native-services | Yesha Vora | Gour Saha |
| [HDFS-13636](https://issues.apache.org/jira/browse/HDFS-13636) | Cross-Site Scripting vulnerability in HttpServer2 |  Major | . | Haibo Yan | Haibo Yan |
| [YARN-7962](https://issues.apache.org/jira/browse/YARN-7962) | Race Condition When Stopping DelegationTokenRenewer causes RM crash during failover |  Critical | resourcemanager | BELUGA BEHR | BELUGA BEHR |
| [YARN-8372](https://issues.apache.org/jira/browse/YARN-8372) | Distributed shell app master should not release containers when shutdown if keep-container is true |  Critical | distributed-shell | Charan Hebri | Suma Shivaprasad |
| [YARN-8319](https://issues.apache.org/jira/browse/YARN-8319) | More YARN pages need to honor yarn.resourcemanager.display.per-user-apps |  Major | webapp | Vinod Kumar Vavilapalli | Sunil Govindan |
| [MAPREDUCE-7097](https://issues.apache.org/jira/browse/MAPREDUCE-7097) | MapReduce JHS should honor yarn.webapp.filter-entity-list-by-user |  Major | . | Vinod Kumar Vavilapalli | Sunil Govindan |
| [YARN-8276](https://issues.apache.org/jira/browse/YARN-8276) | [UI2] After version field became mandatory, form-based submission of new YARN service doesn't work |  Critical | yarn-ui-v2 | Gergely Novák | Gergely Novák |
| [HDFS-13339](https://issues.apache.org/jira/browse/HDFS-13339) | Volume reference can't be released and may lead to deadlock when DataXceiver does a check volume |  Critical | datanode | liaoyuxiangqin | Zsolt Venczel |
| [YARN-8382](https://issues.apache.org/jira/browse/YARN-8382) | cgroup file leak in NM |  Major | nodemanager | Hu Ziqian | Hu Ziqian |
| [YARN-8365](https://issues.apache.org/jira/browse/YARN-8365) | Revisit the record type used by Registry DNS for upstream resolution |  Major | yarn-native-services | Shane Kumpf | Shane Kumpf |
| [HDFS-13545](https://issues.apache.org/jira/browse/HDFS-13545) |  "guarded" is misspelled as "gaurded" in FSPermissionChecker.java |  Trivial | documentation | Jianchao Jia | Jianchao Jia |
| [YARN-8396](https://issues.apache.org/jira/browse/YARN-8396) | Click on an individual container continuously spins and doesn't load the page |  Blocker | . | Charan Hebri | Sunil Govindan |
| [MAPREDUCE-7103](https://issues.apache.org/jira/browse/MAPREDUCE-7103) | Fix TestHistoryViewerPrinter on windows due to a mismatch line separator |  Minor | . | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [HADOOP-15217](https://issues.apache.org/jira/browse/HADOOP-15217) | FsUrlConnection does not handle paths with spaces |  Major | fs | Joseph Fourny | Zsolt Venczel |
| [HDFS-12950](https://issues.apache.org/jira/browse/HDFS-12950) | [oiv] ls will fail in  secure cluster |  Major | . | Brahma Reddy Battula | Wei-Chiu Chuang |
| [YARN-8386](https://issues.apache.org/jira/browse/YARN-8386) |  App log can not be viewed from Logs tab in secure cluster |  Critical | yarn-ui-v2 | Yesha Vora | Sunil Govindan |
| [YARN-8359](https://issues.apache.org/jira/browse/YARN-8359) | Exclude containermanager.linux test classes on Windows |  Major | . | Giovanni Matteo Fumarola | Jason Lowe |
| [HDFS-13664](https://issues.apache.org/jira/browse/HDFS-13664) | Refactor ConfiguredFailoverProxyProvider to make inheritance easier |  Minor | hdfs-client | Chao Sun | Chao Sun |
| [HDFS-12670](https://issues.apache.org/jira/browse/HDFS-12670) | can't renew HDFS tokens with only the hdfs client jar |  Critical | . | Thomas Graves | Arpit Agarwal |
| [HDFS-13667](https://issues.apache.org/jira/browse/HDFS-13667) | Typo: Marking all "datandoes" as stale |  Trivial | namenode | Wei-Chiu Chuang | Nanda kumar |
| [YARN-8413](https://issues.apache.org/jira/browse/YARN-8413) | Flow activity page is failing with "Timeline server failed with an error" |  Major | yarn-ui-v2 | Yesha Vora | Sunil Govindan |
| [YARN-8405](https://issues.apache.org/jira/browse/YARN-8405) | RM zk-state-store.parent-path ACLs has been changed since HADOOP-14773 |  Major | . | Rohith Sharma K S | Íñigo Goiri |
| [YARN-8419](https://issues.apache.org/jira/browse/YARN-8419) | [UI2] User cannot submit a new service as submit button is always disabled |  Major | . | Suma Shivaprasad | Suma Shivaprasad |
| [MAPREDUCE-7108](https://issues.apache.org/jira/browse/MAPREDUCE-7108) | TestFileOutputCommitter fails on Windows |  Minor | test | Zuoming Zhang | Zuoming Zhang |
| [MAPREDUCE-7101](https://issues.apache.org/jira/browse/MAPREDUCE-7101) | Add config parameter to allow JHS to alway scan user dir irrespective of modTime |  Critical | . | Wangda Tan | Thomas Marquardt |
| [HADOOP-15527](https://issues.apache.org/jira/browse/HADOOP-15527) | loop until TIMEOUT before sending kill -9 |  Major | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [YARN-8404](https://issues.apache.org/jira/browse/YARN-8404) | Timeline event publish need to be async to avoid Dispatcher thread leak in case ATS is down |  Blocker | . | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-8410](https://issues.apache.org/jira/browse/YARN-8410) | Registry DNS lookup fails to return for CNAMEs |  Major | yarn-native-services | Shane Kumpf | Shane Kumpf |
| [HDFS-13675](https://issues.apache.org/jira/browse/HDFS-13675) | Speed up TestDFSAdminWithHA |  Major | hdfs, namenode | Lukas Majercak | Lukas Majercak |
| [HDFS-13673](https://issues.apache.org/jira/browse/HDFS-13673) | TestNameNodeMetrics fails on Windows |  Minor | test | Zuoming Zhang | Zuoming Zhang |
| [HDFS-13676](https://issues.apache.org/jira/browse/HDFS-13676) | TestEditLogRace fails on Windows |  Minor | test | Zuoming Zhang | Zuoming Zhang |
| [HDFS-13174](https://issues.apache.org/jira/browse/HDFS-13174) | hdfs mover -p /path times out after 20 min |  Major | balancer & mover | Istvan Fajth | Istvan Fajth |
| [HADOOP-15523](https://issues.apache.org/jira/browse/HADOOP-15523) | Shell command timeout given is in seconds whereas it is taken as millisec while scheduling |  Major | . | Bilwa S T | Bilwa S T |
| [HDFS-13682](https://issues.apache.org/jira/browse/HDFS-13682) | Cannot create encryption zone after KMS auth token expires |  Critical | encryption, kms, namenode | Xiao Chen | Xiao Chen |
| [YARN-8445](https://issues.apache.org/jira/browse/YARN-8445) | YARN native service doesn't allow service name equals to component name |  Major | . | Chandni Singh | Chandni Singh |
| [YARN-8444](https://issues.apache.org/jira/browse/YARN-8444) | NodeResourceMonitor crashes on bad swapFree value |  Major | . | Jim Brennan | Jim Brennan |
| [YARN-8326](https://issues.apache.org/jira/browse/YARN-8326) | Yarn 3.0 seems runs slower than Yarn 2.6 |  Major | yarn | Hsin-Liang Huang | Shane Kumpf |
| [YARN-8443](https://issues.apache.org/jira/browse/YARN-8443) | Total #VCores in cluster metrics is wrong when CapacityScheduler reserved some containers |  Major | webapp | Tao Yang | Tao Yang |
| [YARN-8457](https://issues.apache.org/jira/browse/YARN-8457) | Compilation is broken with -Pyarn-ui |  Major | webapp | Sunil Govindan | Sunil Govindan |
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
| [HADOOP-15574](https://issues.apache.org/jira/browse/HADOOP-15574) | Suppress build error if there are no docs after excluding private annotations |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-13702](https://issues.apache.org/jira/browse/HDFS-13702) | Remove HTrace hooks from DFSClient to reduce CPU usage |  Major | performance | Todd Lipcon | Todd Lipcon |
| [HDFS-13635](https://issues.apache.org/jira/browse/HDFS-13635) | Incorrect message when block is not found |  Major | datanode | Wei-Chiu Chuang | Gabor Bota |
| [YARN-8415](https://issues.apache.org/jira/browse/YARN-8415) | TimelineWebServices.getEntity should throw ForbiddenException instead of 404 when ACL checks fail |  Major | . | Sumana Sathish | Suma Shivaprasad |
| [HDFS-13715](https://issues.apache.org/jira/browse/HDFS-13715) | diskbalancer does not work if one of the blockpools are empty on a Federated cluster |  Major | diskbalancer | Namit Maheshwari | Bharat Viswanadham |
| [YARN-8459](https://issues.apache.org/jira/browse/YARN-8459) | Improve Capacity Scheduler logs to debug invalid states |  Major | capacity scheduler | Wangda Tan | Wangda Tan |
| [HADOOP-15571](https://issues.apache.org/jira/browse/HADOOP-15571) | Multiple FileContexts created with the same configuration object should be allowed to have different umask |  Critical | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [HDFS-13121](https://issues.apache.org/jira/browse/HDFS-13121) | NPE when request file descriptors when SC read |  Minor | hdfs-client | Gang Xie | Zsolt Venczel |
| [YARN-6265](https://issues.apache.org/jira/browse/YARN-6265) | yarn.resourcemanager.fail-fast is used inconsistently |  Major | resourcemanager | Daniel Templeton | Yuanbo Liu |
| [YARN-8473](https://issues.apache.org/jira/browse/YARN-8473) | Containers being launched as app tears down can leave containers in NEW state |  Major | nodemanager | Jason Lowe | Jason Lowe |
| [YARN-8512](https://issues.apache.org/jira/browse/YARN-8512) | ATSv2 entities are not published to HBase from second attempt onwards |  Major | . | Yesha Vora | Rohith Sharma K S |
| [YARN-8491](https://issues.apache.org/jira/browse/YARN-8491) | TestServiceCLI#testEnableFastLaunch fail when umask is 077 |  Major | . | K G Bakthavachalam | K G Bakthavachalam |
| [HADOOP-15541](https://issues.apache.org/jira/browse/HADOOP-15541) | AWS SDK can mistake stream timeouts for EOF and throw SdkClientExceptions |  Major | fs/s3 | Sean Mackrory | Sean Mackrory |
| [HDFS-13723](https://issues.apache.org/jira/browse/HDFS-13723) | Occasional "Should be different group" error in TestRefreshUserMappings#testGroupMappingRefresh |  Major | security, test | Siyao Meng | Siyao Meng |
| [HDFS-12837](https://issues.apache.org/jira/browse/HDFS-12837) | Intermittent failure in TestReencryptionWithKMS |  Major | encryption, test | Surendra Singh Lilhore | Xiao Chen |
| [HDFS-13729](https://issues.apache.org/jira/browse/HDFS-13729) | Fix broken links to RBF documentation |  Minor | documentation | jwhitter | Gabor Bota |
| [YARN-8518](https://issues.apache.org/jira/browse/YARN-8518) | test-container-executor test\_is\_empty() is broken |  Major | . | Jim Brennan | Jim Brennan |
| [YARN-8515](https://issues.apache.org/jira/browse/YARN-8515) | container-executor can crash with SIGPIPE after nodemanager restart |  Major | . | Jim Brennan | Jim Brennan |
| [YARN-8421](https://issues.apache.org/jira/browse/YARN-8421) | when moving app, activeUsers is increased, even though app does not have outstanding request |  Major | . | kyungwan nam |  |
| [YARN-8511](https://issues.apache.org/jira/browse/YARN-8511) | When AM releases a container, RM removes allocation tags before it is released by NM |  Major | capacity scheduler | Weiwei Yang | Weiwei Yang |
| [HDFS-13524](https://issues.apache.org/jira/browse/HDFS-13524) | Occasional "All datanodes are bad" error in TestLargeBlock#testLargeBlockSize |  Major | . | Wei-Chiu Chuang | Siyao Meng |
| [YARN-8538](https://issues.apache.org/jira/browse/YARN-8538) | Fix valgrind leak check on container executor |  Major | . | Billie Rinaldi | Billie Rinaldi |
| [HADOOP-15610](https://issues.apache.org/jira/browse/HADOOP-15610) | Hadoop Docker Image Pip Install Fails |  Critical | . | Jack Bearden | Jack Bearden |
| [HADOOP-15614](https://issues.apache.org/jira/browse/HADOOP-15614) | TestGroupsCaching.testExceptionOnBackgroundRefreshHandled reliably fails |  Major | . | Kihwal Lee | Weiwei Yang |
| [MAPREDUCE-7118](https://issues.apache.org/jira/browse/MAPREDUCE-7118) | Distributed cache conflicts breaks backwards compatability |  Blocker | mrv2 | Jason Lowe | Jason Lowe |
| [YARN-8528](https://issues.apache.org/jira/browse/YARN-8528) | Final states in ContainerAllocation might be modified externally causing unexpected allocation results |  Major | capacity scheduler | Xintong Song | Xintong Song |
| [YARN-8541](https://issues.apache.org/jira/browse/YARN-8541) | RM startup failure on recovery after user deletion |  Blocker | resourcemanager | yimeng | Bibin A Chundatt |
| [HADOOP-15593](https://issues.apache.org/jira/browse/HADOOP-15593) | UserGroupInformation TGT renewer throws NPE |  Blocker | security | Wei-Chiu Chuang | Gabor Bota |
| [HDFS-13765](https://issues.apache.org/jira/browse/HDFS-13765) | Fix javadoc for FSDirMkdirOp#createParentDirectories |  Minor | documentation | Lokesh Jain | Lokesh Jain |
| [YARN-8508](https://issues.apache.org/jira/browse/YARN-8508) | On NodeManager container gets cleaned up before its pid file is created |  Critical | . | Sumana Sathish | Chandni Singh |
| [YARN-8434](https://issues.apache.org/jira/browse/YARN-8434) | Update federation documentation of Nodemanager configurations |  Minor | . | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-8591](https://issues.apache.org/jira/browse/YARN-8591) | [ATSv2] NPE while checking for entity acl in non-secure cluster |  Major | timelinereader, timelineserver | Akhil PB | Rohith Sharma K S |
| [YARN-8558](https://issues.apache.org/jira/browse/YARN-8558) | NM recovery level db not cleaned up properly on container finish |  Critical | . | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-8418](https://issues.apache.org/jira/browse/YARN-8418) | App local logs could leaked if log aggregation fails to initialize for the app |  Critical | . | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-8522](https://issues.apache.org/jira/browse/YARN-8522) | Application fails with InvalidResourceRequestException |  Critical | . | Yesha Vora | Zian Chen |
| [YARN-8606](https://issues.apache.org/jira/browse/YARN-8606) | Opportunistic scheduling does not work post RM failover |  Blocker | . | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-8600](https://issues.apache.org/jira/browse/YARN-8600) | RegistryDNS hang when remote lookup does not reply |  Critical | yarn | Eric Yang | Eric Yang |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-7066](https://issues.apache.org/jira/browse/MAPREDUCE-7066) | TestQueue fails on Java9 |  Major | test | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-15313](https://issues.apache.org/jira/browse/HADOOP-15313) | TestKMS should close providers |  Major | kms, test | Xiao Chen | Xiao Chen |
| [HDFS-13129](https://issues.apache.org/jira/browse/HDFS-13129) | Add a test for DfsAdmin refreshSuperUserGroupsConfiguration |  Minor | namenode | Mukul Kumar Singh | Mukul Kumar Singh |
| [HDFS-13503](https://issues.apache.org/jira/browse/HDFS-13503) | Fix TestFsck test failures on Windows |  Major | hdfs | Xiao Liang | Xiao Liang |
| [HDFS-13315](https://issues.apache.org/jira/browse/HDFS-13315) | Add a test for the issue reported in HDFS-11481 which is fixed by HDFS-10997. |  Major | . | Yongjun Zhang | Yongjun Zhang |
| [HDFS-13542](https://issues.apache.org/jira/browse/HDFS-13542) | TestBlockManager#testNeededReplicationWhileAppending fails due to improper cluster shutdown in TestBlockManager#testBlockManagerMachinesArray on Windows |  Minor | . | Anbang Hu | Anbang Hu |
| [HDFS-13551](https://issues.apache.org/jira/browse/HDFS-13551) | TestMiniDFSCluster#testClusterSetStorageCapacity does not shut down cluster |  Minor | . | Anbang Hu | Anbang Hu |
| [HDFS-11700](https://issues.apache.org/jira/browse/HDFS-11700) | TestHDFSServerPorts#testBackupNodePorts doesn't pass on Windows |  Minor | . | Anbang Hu | Anbang Hu |
| [HDFS-13548](https://issues.apache.org/jira/browse/HDFS-13548) | TestResolveHdfsSymlink#testFcResolveAfs fails on Windows |  Minor | . | Anbang Hu | Anbang Hu |
| [HDFS-13567](https://issues.apache.org/jira/browse/HDFS-13567) | TestNameNodeMetrics#testGenerateEDEKTime,TestNameNodeMetrics#testResourceCheck should use a different cluster basedir |  Minor | . | Anbang Hu | Anbang Hu |
| [HDFS-13557](https://issues.apache.org/jira/browse/HDFS-13557) | TestDFSAdmin#testListOpenFiles fails on Windows |  Minor | . | Anbang Hu | Anbang Hu |
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
| [YARN-8370](https://issues.apache.org/jira/browse/YARN-8370) | Some Node Manager tests fail on Windows due to improper path/file separator |  Minor | . | Anbang Hu | Anbang Hu |
| [YARN-8422](https://issues.apache.org/jira/browse/YARN-8422) | TestAMSimulator failing with NPE |  Minor | . | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [HADOOP-15532](https://issues.apache.org/jira/browse/HADOOP-15532) | TestBasicDiskValidator fails with NoSuchFileException |  Minor | . | Íñigo Goiri | Giovanni Matteo Fumarola |
| [HDFS-13563](https://issues.apache.org/jira/browse/HDFS-13563) | TestDFSAdminWithHA times out on Windows |  Minor | . | Anbang Hu | Lukas Majercak |
| [HDFS-13681](https://issues.apache.org/jira/browse/HDFS-13681) | Fix TestStartup.testNNFailToStartOnReadOnlyNNDir test failure on Windows |  Major | test | Xiao Liang | Xiao Liang |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-8002](https://issues.apache.org/jira/browse/YARN-8002) | Support NOT\_SELF and ALL namespace types for allocation tag |  Major | resourcemanager | Weiwei Yang | Weiwei Yang |
| [HDFS-13289](https://issues.apache.org/jira/browse/HDFS-13289) | RBF: TestConnectionManager#testCleanup() test case need correction |  Minor | . | Dibyendu Karmakar | Dibyendu Karmakar |
| [YARN-8013](https://issues.apache.org/jira/browse/YARN-8013) | Support application tags when defining application namespaces for placement constraints |  Major | . | Weiwei Yang | Weiwei Yang |
| [YARN-6936](https://issues.apache.org/jira/browse/YARN-6936) | [Atsv2] Retrospect storing entities into sub application table from client perspective |  Major | . | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-13353](https://issues.apache.org/jira/browse/HDFS-13353) | RBF: TestRouterWebHDFSContractCreate failed |  Major | test | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-8107](https://issues.apache.org/jira/browse/YARN-8107) | Give an informative message when incorrect format is used in ATSv2 filter attributes |  Major | ATSv2 | Charan Hebri | Rohith Sharma K S |
| [YARN-8110](https://issues.apache.org/jira/browse/YARN-8110) | AMRMProxy recover should catch for all throwable to avoid premature exit |  Major | . | Botong Huang | Botong Huang |
| [YARN-8048](https://issues.apache.org/jira/browse/YARN-8048) | Support auto-spawning of admin configured services during bootstrap of rm/apiserver |  Major | . | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-13402](https://issues.apache.org/jira/browse/HDFS-13402) | RBF: Fix  java doc for StateStoreFileSystemImpl |  Minor | hdfs | Yiran Wu | Yiran Wu |
| [YARN-7574](https://issues.apache.org/jira/browse/YARN-7574) | Add support for Node Labels on Auto Created Leaf Queue Template |  Major | capacity scheduler | Suma Shivaprasad | Suma Shivaprasad |
| [HDFS-13410](https://issues.apache.org/jira/browse/HDFS-13410) | RBF: Support federation with no subclusters |  Minor | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13384](https://issues.apache.org/jira/browse/HDFS-13384) | RBF: Improve timeout RPC call mechanism |  Minor | . | Íñigo Goiri | Íñigo Goiri |
| [HADOOP-15376](https://issues.apache.org/jira/browse/HADOOP-15376) | Remove double semi colons on imports that make Clover fall over. |  Minor | . | Ewan Higgs | Ewan Higgs |
| [YARN-7973](https://issues.apache.org/jira/browse/YARN-7973) | Support ContainerRelaunch for Docker containers |  Major | . | Shane Kumpf | Shane Kumpf |
| [YARN-7941](https://issues.apache.org/jira/browse/YARN-7941) | Transitive dependencies for component are not resolved |  Major | . | Rohith Sharma K S | Billie Rinaldi |
| [HADOOP-15346](https://issues.apache.org/jira/browse/HADOOP-15346) | S3ARetryPolicy for 400/BadArgument to be "fail" |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-13045](https://issues.apache.org/jira/browse/HDFS-13045) | RBF: Improve error message returned from subcluster |  Minor | . | Wei Yan | Íñigo Goiri |
| [HDFS-13428](https://issues.apache.org/jira/browse/HDFS-13428) | RBF: Remove LinkedList From StateStoreFileImpl.java |  Trivial | federation | BELUGA BEHR | BELUGA BEHR |
| [HDFS-13386](https://issues.apache.org/jira/browse/HDFS-13386) | RBF: Wrong date information in list file(-ls) result |  Minor | . | Dibyendu Karmakar | Dibyendu Karmakar |
| [YARN-7221](https://issues.apache.org/jira/browse/YARN-7221) | Add security check for privileged docker container |  Major | security | Eric Yang | Eric Yang |
| [YARN-7936](https://issues.apache.org/jira/browse/YARN-7936) | Add default service AM Xmx |  Major | . | Jian He | Jian He |
| [YARN-8018](https://issues.apache.org/jira/browse/YARN-8018) | Yarn Service Upgrade: Add support for initiating service upgrade |  Major | . | Chandni Singh | Chandni Singh |
| [HADOOP-14999](https://issues.apache.org/jira/browse/HADOOP-14999) | AliyunOSS: provide one asynchronous multi-part based uploading mechanism |  Major | fs/oss | Genmao Yu | Genmao Yu |
| [YARN-7142](https://issues.apache.org/jira/browse/YARN-7142) | Support placement policy in yarn native services |  Major | yarn-native-services | Billie Rinaldi | Gour Saha |
| [YARN-8138](https://issues.apache.org/jira/browse/YARN-8138) | Add unit test to validate queue priority preemption works under node partition. |  Minor | . | Charan Hebri | Zian Chen |
| [YARN-8060](https://issues.apache.org/jira/browse/YARN-8060) | Create default readiness check for service components |  Major | yarn-native-services | Billie Rinaldi | Billie Rinaldi |
| [HDFS-13435](https://issues.apache.org/jira/browse/HDFS-13435) | RBF: Improve the error loggings for printing the stack trace |  Major | . | Yiqun Lin | Yiqun Lin |
| [YARN-8126](https://issues.apache.org/jira/browse/YARN-8126) | Support auto-spawning of admin configured services during bootstrap of RM |  Major | . | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-7996](https://issues.apache.org/jira/browse/YARN-7996) | Allow user supplied Docker client configurations with YARN native services |  Major | . | Shane Kumpf | Shane Kumpf |
| [HDFS-13466](https://issues.apache.org/jira/browse/HDFS-13466) | RBF: Add more router-related information to the UI |  Minor | . | Wei Yan | Wei Yan |
| [YARN-5888](https://issues.apache.org/jira/browse/YARN-5888) | [UI2] Improve unit tests for new YARN UI |  Minor | yarn-ui-v2 | Akhil PB | Akhil PB |
| [HDFS-13453](https://issues.apache.org/jira/browse/HDFS-13453) | RBF: getMountPointDates should fetch latest subdir time/date when parent dir is not present but /parent/child dirs are present in mount table |  Major | . | Dibyendu Karmakar | Dibyendu Karmakar |
| [YARN-8111](https://issues.apache.org/jira/browse/YARN-8111) | Simplify PlacementConstraints API by removing allocationTagToIntraApp |  Minor | resourcemanager | Weiwei Yang | Weiwei Yang |
| [YARN-8064](https://issues.apache.org/jira/browse/YARN-8064) | Docker ".cmd" files should not be put in hadoop.tmp.dir |  Critical | . | Eric Badger | Eric Badger |
| [HDFS-13478](https://issues.apache.org/jira/browse/HDFS-13478) | RBF: Disabled Nameservice store API |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-8177](https://issues.apache.org/jira/browse/YARN-8177) | Documentation changes for auto creation of Leaf Queues with node label |  Major | . | Suma Shivaprasad | Suma Shivaprasad |
| [HDFS-13490](https://issues.apache.org/jira/browse/HDFS-13490) | RBF: Fix setSafeMode in the Router |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13484](https://issues.apache.org/jira/browse/HDFS-13484) | RBF: Disable Nameservices from the federation |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-7939](https://issues.apache.org/jira/browse/YARN-7939) | Yarn Service Upgrade: add support to upgrade a component instance |  Major | . | Chandni Singh | Chandni Singh |
| [HDFS-13326](https://issues.apache.org/jira/browse/HDFS-13326) | RBF: Improve the interfaces to modify and view mount tables |  Minor | . | Wei Yan | Gang Li |
| [YARN-8122](https://issues.apache.org/jira/browse/YARN-8122) | Component health threshold monitor |  Major | . | Gour Saha | Gour Saha |
| [HDFS-13499](https://issues.apache.org/jira/browse/HDFS-13499) | RBF: Show disabled name services in the UI |  Minor | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-8215](https://issues.apache.org/jira/browse/YARN-8215) | ATS v2 returns invalid YARN\_CONTAINER\_ALLOCATED\_HOST\_HTTP\_ADDRESS from NM |  Critical | ATSv2 | Yesha Vora | Rohith Sharma K S |
| [YARN-8152](https://issues.apache.org/jira/browse/YARN-8152) | Add chart in SLS to illustrate the throughput of the scheduler |  Major | scheduler-load-simulator | Weiwei Yang | Tao Yang |
| [YARN-8204](https://issues.apache.org/jira/browse/YARN-8204) | Yarn Service Upgrade: Add a flag to disable upgrade |  Major | . | Chandni Singh | Chandni Singh |
| [YARN-7781](https://issues.apache.org/jira/browse/YARN-7781) | Update YARN-Services-Examples.md to be in sync with the latest code |  Major | . | Gour Saha | Gour Saha |
| [HDFS-13508](https://issues.apache.org/jira/browse/HDFS-13508) | RBF: Normalize paths (automatically) when adding, updating, removing or listing mount table entries |  Minor | . | Ekanth Sethuramalingam | Ekanth Sethuramalingam |
| [HDFS-13434](https://issues.apache.org/jira/browse/HDFS-13434) | RBF: Fix dead links in RBF document |  Major | documentation | Akira Ajisaka | Chetna Chaudhari |
| [YARN-8195](https://issues.apache.org/jira/browse/YARN-8195) | Fix constraint cardinality check in the presence of multiple target allocation tags |  Critical | . | Weiwei Yang | Weiwei Yang |
| [YARN-8228](https://issues.apache.org/jira/browse/YARN-8228) | Docker does not support hostnames greater than 64 characters |  Critical | yarn-native-services | Yesha Vora | Shane Kumpf |
| [YARN-8212](https://issues.apache.org/jira/browse/YARN-8212) | Pending backlog for async allocation threads should be configurable |  Major | . | Weiwei Yang | Tao Yang |
| [YARN-2674](https://issues.apache.org/jira/browse/YARN-2674) | Distributed shell AM may re-launch containers if RM work preserving restart happens |  Major | applications, resourcemanager | Chun Chen | Shane Kumpf |
| [HDFS-13488](https://issues.apache.org/jira/browse/HDFS-13488) | RBF: Reject requests when a Router is overloaded |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-8113](https://issues.apache.org/jira/browse/YARN-8113) | Update placement constraints doc with application namespaces and inter-app constraints |  Major | documentation | Weiwei Yang | Weiwei Yang |
| [YARN-8194](https://issues.apache.org/jira/browse/YARN-8194) | Exception when reinitializing a container using LinuxContainerExecutor |  Blocker | . | Chandni Singh | Chandni Singh |
| [YARN-7961](https://issues.apache.org/jira/browse/YARN-7961) | Improve status response when yarn application is destroyed |  Major | yarn-native-services | Yesha Vora | Gour Saha |
| [HDFS-13525](https://issues.apache.org/jira/browse/HDFS-13525) | RBF: Add unit test TestStateStoreDisabledNameservice |  Major | . | Yiqun Lin | Yiqun Lin |
| [YARN-5151](https://issues.apache.org/jira/browse/YARN-5151) | [UI2] Support kill application from new YARN UI |  Major | . | Wangda Tan | Gergely Novák |
| [YARN-8253](https://issues.apache.org/jira/browse/YARN-8253) | HTTPS Ats v2 api call fails with "bad HTTP parsed" |  Critical | ATSv2 | Yesha Vora | Charan Hebri |
| [YARN-8207](https://issues.apache.org/jira/browse/YARN-8207) | Docker container launch use popen have risk of shell expansion |  Blocker | yarn-native-services | Eric Yang | Eric Yang |
| [YARN-8261](https://issues.apache.org/jira/browse/YARN-8261) | Docker container launch fails due to .cmd file creation failure |  Blocker | . | Eric Badger | Jason Lowe |
| [HADOOP-15454](https://issues.apache.org/jira/browse/HADOOP-15454) | TestRollingFileSystemSinkWithLocal fails on Windows |  Major | test | Xiao Liang | Xiao Liang |
| [HDFS-13346](https://issues.apache.org/jira/browse/HDFS-13346) | RBF: Fix synchronization of router quota and nameservice quota |  Major | . | liuhongtong | Yiqun Lin |
| [YARN-8243](https://issues.apache.org/jira/browse/YARN-8243) | Flex down should remove instance with largest component instance ID first |  Critical | yarn-native-services | Gour Saha | Gour Saha |
| [YARN-7654](https://issues.apache.org/jira/browse/YARN-7654) | Support ENTRY\_POINT for docker container |  Blocker | yarn | Eric Yang | Eric Yang |
| [YARN-8247](https://issues.apache.org/jira/browse/YARN-8247) | Incorrect HTTP status code returned by ATSv2 for non-whitelisted users |  Critical | ATSv2 | Charan Hebri | Rohith Sharma K S |
| [YARN-8130](https://issues.apache.org/jira/browse/YARN-8130) | Race condition when container events are published for KILLED applications |  Major | ATSv2 | Charan Hebri | Rohith Sharma K S |
| [YARN-8081](https://issues.apache.org/jira/browse/YARN-8081) | Yarn Service Upgrade: Add support to upgrade a component |  Major | . | Chandni Singh | Chandni Singh |
| [YARN-8284](https://issues.apache.org/jira/browse/YARN-8284) | get\_docker\_command refactoring |  Minor | . | Jason Lowe | Eric Badger |
| [HADOOP-15469](https://issues.apache.org/jira/browse/HADOOP-15469) | S3A directory committer commit job fails if \_temporary directory created under dest |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-8206](https://issues.apache.org/jira/browse/YARN-8206) | Sending a kill does not immediately kill docker containers |  Major | . | Eric Badger | Eric Badger |
| [YARN-7960](https://issues.apache.org/jira/browse/YARN-7960) | Add no-new-privileges flag to docker run |  Major | . | Eric Badger | Eric Badger |
| [YARN-7530](https://issues.apache.org/jira/browse/YARN-7530) | hadoop-yarn-services-api should be part of hadoop-yarn-services |  Blocker | yarn-native-services | Eric Yang | Chandni Singh |
| [YARN-6919](https://issues.apache.org/jira/browse/YARN-6919) | Add default volume mount list |  Major | yarn | Eric Badger | Eric Badger |
| [HADOOP-15498](https://issues.apache.org/jira/browse/HADOOP-15498) | TestHadoopArchiveLogs (#testGenerateScript, #testPrepareWorkingDir) fails on Windows |  Minor | . | Anbang Hu | Anbang Hu |
| [YARN-8329](https://issues.apache.org/jira/browse/YARN-8329) | Docker client configuration can still be set incorrectly |  Major | . | Shane Kumpf | Shane Kumpf |
| [HDFS-12978](https://issues.apache.org/jira/browse/HDFS-12978) | Fine-grained locking while consuming journal stream. |  Major | namenode | Konstantin Shvachko | Konstantin Shvachko |
| [YARN-8384](https://issues.apache.org/jira/browse/YARN-8384) | stdout.txt, stderr.txt logs of a launched docker container is coming with primary group of submit user instead of hadoop |  Critical | yarn-native-services | Sunil Govindan | Eric Yang |
| [YARN-8349](https://issues.apache.org/jira/browse/YARN-8349) | Remove YARN registry entries when a service is killed by the RM |  Critical | yarn-native-services | Shane Kumpf | Billie Rinaldi |
| [HDFS-13637](https://issues.apache.org/jira/browse/HDFS-13637) | RBF: Router fails when threadIndex (in ConnectionPool) wraps around Integer.MIN\_VALUE |  Critical | federation | CR Hota | CR Hota |
| [YARN-8342](https://issues.apache.org/jira/browse/YARN-8342) | Using docker image from a non-privileged registry, the launch\_command is not honored |  Critical | . | Wangda Tan | Eric Yang |
| [HDFS-13281](https://issues.apache.org/jira/browse/HDFS-13281) | Namenode#createFile should be /.reserved/raw/ aware. |  Critical | encryption | Rushabh S Shah | Rushabh S Shah |
| [YARN-4677](https://issues.apache.org/jira/browse/YARN-4677) | RMNodeResourceUpdateEvent update from scheduler can lead to race condition |  Major | graceful, resourcemanager, scheduler | Brook Zhou | Wilfred Spiegelenburg |
| [HADOOP-15137](https://issues.apache.org/jira/browse/HADOOP-15137) | ClassNotFoundException: org.apache.hadoop.yarn.server.api.DistributedSchedulingAMProtocol when using hadoop-client-minicluster |  Major | . | Jeff Zhang | Bharat Viswanadham |
| [HDFS-13547](https://issues.apache.org/jira/browse/HDFS-13547) | Add ingress port based sasl resolver |  Major | security | Chen Liang | Chen Liang |
| [HADOOP-15514](https://issues.apache.org/jira/browse/HADOOP-15514) | NoClassDefFoundError for TimelineCollectorManager when starting MiniYARNCluster |  Major | . | Jeff Zhang | Rohith Sharma K S |
| [HADOOP-15516](https://issues.apache.org/jira/browse/HADOOP-15516) | Add test cases to cover FileUtil#readLink |  Minor | . | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [HADOOP-15506](https://issues.apache.org/jira/browse/HADOOP-15506) | Upgrade Azure Storage Sdk version to 7.0.0 and update corresponding code blocks |  Minor | fs/azure | Esfandiar Manii | Esfandiar Manii |
| [HADOOP-15529](https://issues.apache.org/jira/browse/HADOOP-15529) | ContainerLaunch#testInvalidEnvVariableSubstitutionType is not supported in Windows |  Minor | . | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [YARN-8411](https://issues.apache.org/jira/browse/YARN-8411) | Enable stopped system services to be started during RM start |  Critical | . | Billie Rinaldi | Billie Rinaldi |
| [YARN-8259](https://issues.apache.org/jira/browse/YARN-8259) | Revisit liveliness checks for Docker containers |  Blocker | . | Shane Kumpf | Shane Kumpf |
| [HADOOP-15533](https://issues.apache.org/jira/browse/HADOOP-15533) | Make WASB listStatus messages consistent |  Trivial | fs/azure | Esfandiar Manii | Esfandiar Manii |
| [HADOOP-15458](https://issues.apache.org/jira/browse/HADOOP-15458) | TestLocalFileSystem#testFSOutputStreamBuilder fails on Windows |  Minor | test | Xiao Liang | Xiao Liang |
| [YARN-8465](https://issues.apache.org/jira/browse/YARN-8465) | Dshell docker container gets marked as lost after NM restart |  Major | yarn-native-services | Yesha Vora | Shane Kumpf |
| [YARN-8485](https://issues.apache.org/jira/browse/YARN-8485) | Priviledged container app launch is failing intermittently |  Major | yarn-native-services | Yesha Vora | Eric Yang |
| [HDFS-13528](https://issues.apache.org/jira/browse/HDFS-13528) | RBF: If a directory exceeds quota limit then quota usage is not refreshed for other mount entries |  Major | . | Dibyendu Karmakar | Dibyendu Karmakar |
| [HDFS-13710](https://issues.apache.org/jira/browse/HDFS-13710) | RBF:  setQuota and getQuotaUsage should check the dfs.federation.router.quota.enable |  Major | federation, hdfs | yanghuafeng | yanghuafeng |
| [HADOOP-15384](https://issues.apache.org/jira/browse/HADOOP-15384) | distcp numListstatusThreads option doesn't get to -delete scan |  Major | tools/distcp | Steve Loughran | Steve Loughran |
| [HDFS-13726](https://issues.apache.org/jira/browse/HDFS-13726) | RBF: Fix RBF configuration links |  Minor | documentation | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-13475](https://issues.apache.org/jira/browse/HDFS-13475) | RBF: Admin cannot enforce Router enter SafeMode |  Major | . | Wei Yan | Chao Sun |
| [HDFS-13733](https://issues.apache.org/jira/browse/HDFS-13733) | RBF: Add Web UI configurations and descriptions to RBF document |  Minor | documentation | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-8301](https://issues.apache.org/jira/browse/YARN-8301) | Yarn Service Upgrade: Add documentation |  Critical | . | Chandni Singh | Chandni Singh |
| [YARN-8546](https://issues.apache.org/jira/browse/YARN-8546) | Resource leak caused by a reserved container being released more than once under async scheduling |  Major | capacity scheduler | Weiwei Yang | Tao Yang |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-8091](https://issues.apache.org/jira/browse/YARN-8091) | Revisit checkUserAccessToQueue RM REST API |  Critical | . | Wangda Tan | Wangda Tan |
| [YARN-8274](https://issues.apache.org/jira/browse/YARN-8274) | Docker command error during container relaunch |  Critical | . | Billie Rinaldi | Jason Lowe |
| [YARN-8080](https://issues.apache.org/jira/browse/YARN-8080) | YARN native service should support component restart policy |  Critical | . | Wangda Tan | Suma Shivaprasad |
| [HADOOP-15483](https://issues.apache.org/jira/browse/HADOOP-15483) | Upgrade jquery to version 3.3.1 |  Major | . | Lokesh Jain | Lokesh Jain |
| [YARN-8506](https://issues.apache.org/jira/browse/YARN-8506) | Make GetApplicationsRequestPBImpl thread safe |  Critical | . | Wangda Tan | Wangda Tan |


