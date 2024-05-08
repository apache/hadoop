
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

## Release 3.4.0 - 2024-03-04



### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-15380](https://issues.apache.org/jira/browse/HDFS-15380) | RBF: Could not fetch real remote IP in RouterWebHdfsMethods |  Major | webhdfs | Tao Li | Tao Li |
| [HDFS-15814](https://issues.apache.org/jira/browse/HDFS-15814) | Make some parameters configurable for DataNodeDiskMetrics |  Major | hdfs | Tao Li | Tao Li |
| [HDFS-16265](https://issues.apache.org/jira/browse/HDFS-16265) | Refactor HDFS tool tests for better reuse |  Blocker | hdfs-client, libhdfs++, tools | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-17956](https://issues.apache.org/jira/browse/HADOOP-17956) | Replace all default Charset usage with UTF-8 |  Major | common | Viraj Jasani | Viraj Jasani |
| [HDFS-16278](https://issues.apache.org/jira/browse/HDFS-16278) | Make HDFS snapshot tools cross platform |  Major | hdfs-client, libhdfs++, tools | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16285](https://issues.apache.org/jira/browse/HDFS-16285) | Make HDFS ownership tools cross platform |  Major | hdfs-client, libhdfs++, tools | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16419](https://issues.apache.org/jira/browse/HDFS-16419) | Make HDFS data transfer tools cross platform |  Major | hdfs-client, libhdfs++, tools | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16511](https://issues.apache.org/jira/browse/HDFS-16511) | Improve lock type for ReplicaMap under fine-grain lock mode. |  Major | hdfs | Mingxiang Li | Mingxiang Li |
| [HDFS-16534](https://issues.apache.org/jira/browse/HDFS-16534) | Split datanode block pool locks to volume grain. |  Major | datanode | Mingxiang Li | Mingxiang Li |
| [HADOOP-18219](https://issues.apache.org/jira/browse/HADOOP-18219) | Fix shadedclient test failure |  Blocker | test | Gautham Banasandra | Akira Ajisaka |
| [HADOOP-18621](https://issues.apache.org/jira/browse/HADOOP-18621) | CryptoOutputStream::close leak when encrypted zones + quota exceptions |  Critical | fs | Colm Dougan | Colm Dougan |
| [YARN-5597](https://issues.apache.org/jira/browse/YARN-5597) | YARN Federation improvements |  Major | federation | Subramaniam Krishnan | Subramaniam Krishnan |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-17010](https://issues.apache.org/jira/browse/HADOOP-17010) | Add queue capacity weights support in FairCallQueue |  Major | ipc | Fengnan Li | Fengnan Li |
| [HDFS-15288](https://issues.apache.org/jira/browse/HDFS-15288) | Add Available Space Rack Fault Tolerant BPP |  Major | block placement | Ayush Saxena | Ayush Saxena |
| [HDFS-13183](https://issues.apache.org/jira/browse/HDFS-13183) | Standby NameNode process getBlocks request to reduce Active load |  Major | balancer & mover, namenode | Xiaoqiao He | Xiaoqiao He |
| [HDFS-15463](https://issues.apache.org/jira/browse/HDFS-15463) | Add a tool to validate FsImage |  Major | namenode | Tsz-wo Sze | Tsz-wo Sze |
| [HADOOP-17165](https://issues.apache.org/jira/browse/HADOOP-17165) | Implement service-user feature in DecayRPCScheduler |  Major | rpc-server | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-15891](https://issues.apache.org/jira/browse/HADOOP-15891) | Provide Regex Based Mount Point In Inode Tree |  Major | viewfs | zhenzhao wang | zhenzhao wang |
| [HDFS-15025](https://issues.apache.org/jira/browse/HDFS-15025) | Applying NVDIMM storage media to HDFS |  Major | datanode, hdfs | YaYun Wang | YaYun Wang |
| [HDFS-15098](https://issues.apache.org/jira/browse/HDFS-15098) | Add SM4 encryption method for HDFS |  Major | hdfs | liusheng | liusheng |
| [HADOOP-17125](https://issues.apache.org/jira/browse/HADOOP-17125) | Using snappy-java in SnappyCodec |  Major | common | DB Tsai | L. C. Hsieh |
| [HDFS-15294](https://issues.apache.org/jira/browse/HDFS-15294) | Federation balance tool |  Major | rbf, tools | Jinglun | Jinglun |
| [HADOOP-17292](https://issues.apache.org/jira/browse/HADOOP-17292) | Using lz4-java in Lz4Codec |  Major | common | L. C. Hsieh | L. C. Hsieh |
| [HDFS-14090](https://issues.apache.org/jira/browse/HDFS-14090) | RBF: Improved isolation for downstream name nodes. {Static} |  Major | rbf | CR Hota | Fengnan Li |
| [HDFS-15711](https://issues.apache.org/jira/browse/HDFS-15711) | Add Metrics to HttpFS Server |  Major | httpfs | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-16492](https://issues.apache.org/jira/browse/HADOOP-16492) | Support HuaweiCloud Object Storage as a Hadoop Backend File System |  Major | fs | zhongjun | lixianwei |
| [HDFS-15759](https://issues.apache.org/jira/browse/HDFS-15759) | EC: Verify EC reconstruction correctness on DataNode |  Major | datanode, ec, erasure-coding | Toshihiko Uchida | Toshihiko Uchida |
| [HDFS-15970](https://issues.apache.org/jira/browse/HDFS-15970) | Print network topology on the web |  Minor | namanode, ui | Tao Li | Tao Li |
| [HDFS-16048](https://issues.apache.org/jira/browse/HDFS-16048) | RBF: Print network topology on the router web |  Minor | rbf | Tao Li | Tao Li |
| [HDFS-13916](https://issues.apache.org/jira/browse/HDFS-13916) | Distcp SnapshotDiff to support WebHDFS |  Major | distcp, webhdfs | Xun REN | Xun REN |
| [HDFS-16203](https://issues.apache.org/jira/browse/HDFS-16203) | Discover datanodes with unbalanced block pool usage by the standard deviation |  Major | datanode, ui | Tao Li | Tao Li |
| [HADOOP-18003](https://issues.apache.org/jira/browse/HADOOP-18003) | Add a method appendIfAbsent for CallerContext |  Minor | common | Tao Li | Tao Li |
| [HDFS-16337](https://issues.apache.org/jira/browse/HDFS-16337) | Show start time of Datanode on Web |  Minor | datanode, ui | Tao Li | Tao Li |
| [HDFS-16331](https://issues.apache.org/jira/browse/HDFS-16331) | Make dfs.blockreport.intervalMsec reconfigurable |  Major | datanode | Tao Li | Tao Li |
| [HDFS-16371](https://issues.apache.org/jira/browse/HDFS-16371) | Exclude slow disks when choosing volume |  Major | datanode | Tao Li | Tao Li |
| [HADOOP-18055](https://issues.apache.org/jira/browse/HADOOP-18055) | Async Profiler endpoint for Hadoop daemons |  Major | common | Viraj Jasani | Viraj Jasani |
| [HDFS-16400](https://issues.apache.org/jira/browse/HDFS-16400) | Reconfig DataXceiver parameters for datanode |  Major | datanode | Tao Li | Tao Li |
| [HDFS-16399](https://issues.apache.org/jira/browse/HDFS-16399) | Reconfig cache report parameters for datanode |  Major | datanode | Tao Li | Tao Li |
| [HDFS-16398](https://issues.apache.org/jira/browse/HDFS-16398) | Reconfig block report parameters for datanode |  Major | datanode | Tao Li | Tao Li |
| [HDFS-16451](https://issues.apache.org/jira/browse/HDFS-16451) | RBF: Add search box for Router's tab-mounttable web page |  Minor | rbf | Max  Xie | Max  Xie |
| [HDFS-16396](https://issues.apache.org/jira/browse/HDFS-16396) | Reconfig slow peer parameters for datanode |  Major | datanode | Tao Li | Tao Li |
| [HDFS-16397](https://issues.apache.org/jira/browse/HDFS-16397) | Reconfig slow disk parameters for datanode |  Major | datanode | Tao Li | Tao Li |
| [YARN-11084](https://issues.apache.org/jira/browse/YARN-11084) | Introduce new config to specify AM default node-label when not specified |  Major | nodeattibute | Junfan Zhang | Junfan Zhang |
| [YARN-11069](https://issues.apache.org/jira/browse/YARN-11069) | Dynamic Queue ACL handling in Legacy and Flexible Auto Created Queues |  Major | capacity scheduler, yarn | Tamas Domok | Tamas Domok |
| [HDFS-16413](https://issues.apache.org/jira/browse/HDFS-16413) | Reconfig dfs usage parameters for datanode |  Major | datanode | Tao Li | Tao Li |
| [HDFS-16521](https://issues.apache.org/jira/browse/HDFS-16521) | DFS API to retrieve slow datanodes |  Major | datanode, dfsclient | Viraj Jasani | Viraj Jasani |
| [HDFS-16568](https://issues.apache.org/jira/browse/HDFS-16568) | dfsadmin -reconfig option to start/query reconfig on all live datanodes |  Major | dfsadmin | Viraj Jasani | Viraj Jasani |
| [HDFS-16582](https://issues.apache.org/jira/browse/HDFS-16582) | Expose aggregate latency of slow node as perceived by the reporting node |  Major | metrics | Viraj Jasani | Viraj Jasani |
| [HDFS-16595](https://issues.apache.org/jira/browse/HDFS-16595) | Slow peer metrics - add median, mad and upper latency limits |  Major | metrics | Viraj Jasani | Viraj Jasani |
| [HADOOP-18345](https://issues.apache.org/jira/browse/HADOOP-18345) | Enhance client protocol to propagate last seen state IDs for multiple nameservices. |  Major | common | Simbarashe Dzinamarira | Simbarashe Dzinamarira |
| [YARN-11241](https://issues.apache.org/jira/browse/YARN-11241) | Add uncleaning option for local app log file with log-aggregation enabled |  Major | log-aggregation | Ashutosh Gupta | Ashutosh Gupta |
| [YARN-11255](https://issues.apache.org/jira/browse/YARN-11255) | Support loading alternative docker client config from system environment |  Major | yarn | Ashutosh Gupta | Ashutosh Gupta |
| [HDFS-16858](https://issues.apache.org/jira/browse/HDFS-16858) | Dynamically adjust max slow disks to exclude |  Major | datanode | dingshun | dingshun |
| [HADOOP-18671](https://issues.apache.org/jira/browse/HADOOP-18671) | Add recoverLease(), setSafeMode(), isFileClosed() APIs to FileSystem |  Major | fs | Wei-Chiu Chuang | Tak-Lon (Stephen) Wu |
| [HDFS-16965](https://issues.apache.org/jira/browse/HDFS-16965) | Add switch to decide whether to enable native codec. |  Minor | erasure-coding | WangYuanben | WangYuanben |
| [MAPREDUCE-7432](https://issues.apache.org/jira/browse/MAPREDUCE-7432) | Make Manifest Committer the default for abfs and gcs |  Major | client | Steve Loughran | Steve Loughran |
| [HDFS-17113](https://issues.apache.org/jira/browse/HDFS-17113) | Reconfig transfer and write bandwidth for datanode. |  Major | datanode | WangYuanben | WangYuanben |
| [MAPREDUCE-7456](https://issues.apache.org/jira/browse/MAPREDUCE-7456) | Extend add-opens flag to container launch commands on JDK17 nodes |  Major | build, mrv2 | Peter Szucs | Peter Szucs |
| [MAPREDUCE-7449](https://issues.apache.org/jira/browse/MAPREDUCE-7449) | Add add-opens flag to container launch commands on JDK17 nodes |  Major | mr-am, mrv2 | Benjamin Teke | Benjamin Teke |
| [HDFS-17063](https://issues.apache.org/jira/browse/HDFS-17063) | Support to configure different capacity reserved for each disk of DataNode. |  Minor | datanode, hdfs | Jiale Qi | Jiale Qi |
| [HDFS-17294](https://issues.apache.org/jira/browse/HDFS-17294) | Reconfigure the scheduling cycle of the slowPeerCollectorDaemon thread. |  Major | configuration | Zhaobo Huang | Zhaobo Huang |
| [HDFS-17301](https://issues.apache.org/jira/browse/HDFS-17301) | Add read and write dataXceiver threads count metrics to datanode. |  Major | datanode | Zhaobo Huang | Zhaobo Huang |
| [HADOOP-19017](https://issues.apache.org/jira/browse/HADOOP-19017) | Setup pre-commit CI for Windows 10 |  Critical | build | Gautham Banasandra | Gautham Banasandra |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-15245](https://issues.apache.org/jira/browse/HDFS-15245) | Improve JournalNode web UI |  Major | journal-node, ui | Jianfei Jiang | Jianfei Jiang |
| [MAPREDUCE-7241](https://issues.apache.org/jira/browse/MAPREDUCE-7241) | FileInputFormat listStatus with less memory footprint |  Major | job submission | Zhihua Deng | Zhihua Deng |
| [HDFS-15242](https://issues.apache.org/jira/browse/HDFS-15242) | Add metrics for operations hold lock times of FsDatasetImpl |  Major | datanode | Xiaoqiao He | Xiaoqiao He |
| [HADOOP-16952](https://issues.apache.org/jira/browse/HADOOP-16952) | Add .diff to gitignore |  Minor | build | Ayush Saxena | Ayush Saxena |
| [HADOOP-16954](https://issues.apache.org/jira/browse/HADOOP-16954) | Add -S option in "Count" command to show only Snapshot Counts |  Major | common | Hemanth Boyina | Hemanth Boyina |
| [YARN-10063](https://issues.apache.org/jira/browse/YARN-10063) | Usage output of container-executor binary needs to include --http/--https argument |  Minor | yarn | Siddharth Ahuja | Siddharth Ahuja |
| [YARN-10212](https://issues.apache.org/jira/browse/YARN-10212) | Create separate configuration for max global AM attempts |  Major | am | Jonathan Hung | Bilwa S T |
| [HDFS-15261](https://issues.apache.org/jira/browse/HDFS-15261) | RBF: Add Block Related Metrics |  Major | metrics, rbf | Ayush Saxena | Ayush Saxena |
| [HDFS-15247](https://issues.apache.org/jira/browse/HDFS-15247) | RBF: Provide Non DFS Used per DataNode in DataNode UI |  Major | datanode, rbf, ui | Ayush Saxena | Lisheng Sun |
| [YARN-5277](https://issues.apache.org/jira/browse/YARN-5277) | When localizers fail due to resource timestamps being out, provide more diagnostics |  Major | nodemanager | Steve Loughran | Siddharth Ahuja |
| [YARN-9995](https://issues.apache.org/jira/browse/YARN-9995) | Code cleanup in TestSchedConfCLI |  Minor | test | Szilard Nemeth | Bilwa S T |
| [YARN-9354](https://issues.apache.org/jira/browse/YARN-9354) | Resources should be created with ResourceTypesTestHelper instead of TestUtils |  Trivial | resourcemanager | Szilard Nemeth | Andras Gyori |
| [YARN-10002](https://issues.apache.org/jira/browse/YARN-10002) | Code cleanup and improvements in ConfigurationStoreBaseTest |  Minor | test, yarn | Szilard Nemeth | Benjamin Teke |
| [HDFS-15277](https://issues.apache.org/jira/browse/HDFS-15277) | Parent directory in the explorer does not support all path formats |  Minor | ui | Jianfei Jiang | Jianfei Jiang |
| [HADOOP-16951](https://issues.apache.org/jira/browse/HADOOP-16951) | Tidy Up Text and ByteWritables Classes |  Minor | common | David Mollitor | David Mollitor |
| [YARN-9954](https://issues.apache.org/jira/browse/YARN-9954) | Configurable max application tags and max tag length |  Major | resourcemanager | Jonathan Hung | Bilwa S T |
| [YARN-10001](https://issues.apache.org/jira/browse/YARN-10001) | Add explanation of unimplemented methods in InMemoryConfigurationStore |  Major | capacity scheduler | Szilard Nemeth | Siddharth Ahuja |
| [MAPREDUCE-7199](https://issues.apache.org/jira/browse/MAPREDUCE-7199) | HsJobsBlock reuse JobACLsManager for checkAccess |  Minor | mrv2 | Bibin Chundatt | Bilwa S T |
| [HDFS-15217](https://issues.apache.org/jira/browse/HDFS-15217) | Add more information to longest write/read lock held log |  Major | namanode | Toshihiro Suzuki | Toshihiro Suzuki |
| [HADOOP-17001](https://issues.apache.org/jira/browse/HADOOP-17001) | The suffix name of the unified compression class |  Major | io | bianqi | bianqi |
| [YARN-9997](https://issues.apache.org/jira/browse/YARN-9997) | Code cleanup in ZKConfigurationStore |  Minor | resourcemanager | Szilard Nemeth | Andras Gyori |
| [YARN-9996](https://issues.apache.org/jira/browse/YARN-9996) | Code cleanup in QueueAdminConfigurationMutationACLPolicy |  Major | resourcemanager | Szilard Nemeth | Siddharth Ahuja |
| [YARN-9998](https://issues.apache.org/jira/browse/YARN-9998) | Code cleanup in LeveldbConfigurationStore |  Minor | resourcemanager | Szilard Nemeth | Benjamin Teke |
| [YARN-9999](https://issues.apache.org/jira/browse/YARN-9999) | TestFSSchedulerConfigurationStore: Extend from ConfigurationStoreBaseTest, general code cleanup |  Minor | test | Szilard Nemeth | Benjamin Teke |
| [HDFS-15295](https://issues.apache.org/jira/browse/HDFS-15295) | AvailableSpaceBlockPlacementPolicy should use chooseRandomWithStorageTypeTwoTrial() for better performance. |  Minor | block placement | Jinglun | Jinglun |
| [HADOOP-16054](https://issues.apache.org/jira/browse/HADOOP-16054) | Update Dockerfile to use Bionic |  Major | build, test | Akira Ajisaka | Akira Ajisaka |
| [YARN-10189](https://issues.apache.org/jira/browse/YARN-10189) | Code cleanup in LeveldbRMStateStore |  Minor | resourcemanager | Benjamin Teke | Benjamin Teke |
| [HADOOP-16886](https://issues.apache.org/jira/browse/HADOOP-16886) | Add hadoop.http.idle\_timeout.ms to core-default.xml |  Major | conf | Wei-Chiu Chuang | Lisheng Sun |
| [HDFS-15328](https://issues.apache.org/jira/browse/HDFS-15328) | Use DFSConfigKeys  MONITOR\_CLASS\_DEFAULT  constant |  Minor | hdfs | bianqi | bianqi |
| [HDFS-14283](https://issues.apache.org/jira/browse/HDFS-14283) | DFSInputStream to prefer cached replica |  Major | hdfs-client | Wei-Chiu Chuang | Lisheng Sun |
| [HDFS-15347](https://issues.apache.org/jira/browse/HDFS-15347) | Replace the deprecated method shaHex |  Minor | balancer & mover | bianqi | bianqi |
| [HDFS-15338](https://issues.apache.org/jira/browse/HDFS-15338) | listOpenFiles() should throw InvalidPathException in case of invalid paths |  Minor | namanode | Jinglun | Jinglun |
| [YARN-10160](https://issues.apache.org/jira/browse/YARN-10160) | Add auto queue creation related configs to RMWebService#CapacitySchedulerQueueInfo |  Major | capacity scheduler, webapp | Prabhu Joseph | Prabhu Joseph |
| [HDFS-15350](https://issues.apache.org/jira/browse/HDFS-15350) | Set dfs.client.failover.random.order to true as default |  Major | hdfs-client | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-15255](https://issues.apache.org/jira/browse/HDFS-15255) | Consider StorageType when DatanodeManager#sortLocatedBlock() |  Major | hdfs | Lisheng Sun | Lisheng Sun |
| [HDFS-15345](https://issues.apache.org/jira/browse/HDFS-15345) | RBF: RouterPermissionChecker#checkSuperuserPrivilege should use UGI#getGroups after HADOOP-13442 |  Major | rbf | Xiaoyu Yao | Xiaoyu Yao |
| [YARN-10260](https://issues.apache.org/jira/browse/YARN-10260) | Allow transitioning queue from DRAINING to RUNNING state |  Major | capacity scheduler | Jonathan Hung | Bilwa S T |
| [HDFS-15344](https://issues.apache.org/jira/browse/HDFS-15344) | DataNode#checkSuperuserPrivilege should use UGI#getGroups after HADOOP-13442 |  Major | datanode | Xiaoyu Yao | Xiaoyu Yao |
| [HADOOP-14254](https://issues.apache.org/jira/browse/HADOOP-14254) | Add a Distcp option to preserve Erasure Coding attributes |  Major | tools/distcp | Wei-Chiu Chuang | Ayush Saxena |
| [HADOOP-16965](https://issues.apache.org/jira/browse/HADOOP-16965) | Introduce StreamContext for Abfs Input and Output streams. |  Major | fs/azure | Mukund Thakur | Mukund Thakur |
| [HADOOP-17036](https://issues.apache.org/jira/browse/HADOOP-17036) | TestFTPFileSystem failing as ftp server dir already exists |  Minor | fs, test | Steve Loughran | Mikhail Pryakhin |
| [HDFS-15356](https://issues.apache.org/jira/browse/HDFS-15356) | Unify configuration \`dfs.ha.allow.stale.reads\` to DFSConfigKeys |  Major | hdfs | Xiaoqiao He | Xiaoqiao He |
| [HDFS-15358](https://issues.apache.org/jira/browse/HDFS-15358) | RBF: Unify router datanode UI with namenode datanode UI |  Major | datanode, rbf, ui | Ayush Saxena | Ayush Saxena |
| [HADOOP-17042](https://issues.apache.org/jira/browse/HADOOP-17042) | Hadoop distcp throws "ERROR: Tools helper ///usr/lib/hadoop/libexec/tools/hadoop-distcp.sh was not found" |  Minor | tools/distcp | Aki Tanaka | Aki Tanaka |
| [HDFS-15202](https://issues.apache.org/jira/browse/HDFS-15202) | HDFS-client: boost ShortCircuit Cache |  Minor | dfsclient | Danil Lipovoy | Danil Lipovoy |
| [HDFS-15207](https://issues.apache.org/jira/browse/HDFS-15207) | VolumeScanner skip to scan blocks accessed during recent scan peroid |  Minor | datanode | Yang Yun | Yang Yun |
| [HDFS-14999](https://issues.apache.org/jira/browse/HDFS-14999) | Avoid Potential Infinite Loop in DFSNetworkTopology |  Major | dfs | Ayush Saxena | Ayush Saxena |
| [HDFS-15353](https://issues.apache.org/jira/browse/HDFS-15353) | Use sudo instead of su to allow nologin user for secure DataNode |  Major | datanode, security | Akira Ajisaka | Kei Kori |
| [HDFS-13639](https://issues.apache.org/jira/browse/HDFS-13639) | SlotReleaser is not fast enough |  Major | hdfs-client | Gang Xie | Lisheng Sun |
| [HDFS-15369](https://issues.apache.org/jira/browse/HDFS-15369) | Refactor method VolumeScanner#runLoop() |  Minor | datanode | Yang Yun | Yang Yun |
| [HDFS-15355](https://issues.apache.org/jira/browse/HDFS-15355) | Make the default block storage policy ID configurable |  Minor | block placement, namenode | Yang Yun | Yang Yun |
| [HDFS-15368](https://issues.apache.org/jira/browse/HDFS-15368) | TestBalancerWithHANameNodes#testBalancerWithObserver failed occasionally |  Major | balancer, test | Xiaoqiao He | Xiaoqiao He |
| [HADOOP-14698](https://issues.apache.org/jira/browse/HADOOP-14698) | Make copyFromLocal's -t option available for put as well |  Major | common | Andras Bokor | Andras Bokor |
| [HDFS-10792](https://issues.apache.org/jira/browse/HDFS-10792) | RedundantEditLogInputStream should log caught exceptions |  Minor | namenode | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-6492](https://issues.apache.org/jira/browse/YARN-6492) | Generate queue metrics for each partition |  Major | capacity scheduler | Jonathan Hung | Manikandan R |
| [HADOOP-16828](https://issues.apache.org/jira/browse/HADOOP-16828) | Zookeeper Delegation Token Manager fetch sequence number by batch |  Major | security | Fengnan Li | Fengnan Li |
| [HDFS-14960](https://issues.apache.org/jira/browse/HDFS-14960) | TestBalancerWithNodeGroup should not succeed with DFSNetworkTopology |  Minor | hdfs | Jim Brennan | Jim Brennan |
| [HDFS-15359](https://issues.apache.org/jira/browse/HDFS-15359) | EC: Allow closing a file with committed blocks |  Major | erasure-coding | Ayush Saxena | Ayush Saxena |
| [HADOOP-17047](https://issues.apache.org/jira/browse/HADOOP-17047) | TODO comments exist in trunk while the related issues are already fixed. |  Trivial | fs | Rungroj Maipradit | Rungroj Maipradit |
| [HDFS-15376](https://issues.apache.org/jira/browse/HDFS-15376) | Update the error about command line POST in httpfs documentation |  Major | httpfs | bianqi | bianqi |
| [HDFS-15406](https://issues.apache.org/jira/browse/HDFS-15406) | Improve the speed of Datanode Block Scan |  Major | datanode | Hemanth Boyina | Hemanth Boyina |
| [HADOOP-17009](https://issues.apache.org/jira/browse/HADOOP-17009) | Embrace Immutability of Java Collections |  Minor | common | David Mollitor | David Mollitor |
| [YARN-9460](https://issues.apache.org/jira/browse/YARN-9460) | QueueACLsManager and ReservationsACLManager should not use instanceof checks |  Major | resourcemanager | Szilard Nemeth | Bilwa S T |
| [YARN-10321](https://issues.apache.org/jira/browse/YARN-10321) | Break down TestUserGroupMappingPlacementRule#testMapping into test scenarios |  Minor | test | Szilard Nemeth | Szilard Nemeth |
| [HDFS-15383](https://issues.apache.org/jira/browse/HDFS-15383) | RBF: Disable watch in ZKDelegationSecretManager for performance |  Major | rbf | Fengnan Li | Fengnan Li |
| [HDFS-15416](https://issues.apache.org/jira/browse/HDFS-15416) | Improve DataStorage#addStorageLocations() for empty locations |  Major | datanode | JiangHua Zhu | JiangHua Zhu |
| [HADOOP-17090](https://issues.apache.org/jira/browse/HADOOP-17090) | Increase precommit job timeout from 5 hours to 20 hours |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17084](https://issues.apache.org/jira/browse/HADOOP-17084) | Update Dockerfile\_aarch64 to use Bionic |  Major | build, test | RuiChen | zhaorenhai |
| [HDFS-15312](https://issues.apache.org/jira/browse/HDFS-15312) | Apply umask when creating directory by WebHDFS |  Minor | webhdfs | Ye Ni | Ye Ni |
| [HDFS-15425](https://issues.apache.org/jira/browse/HDFS-15425) | Review Logging of DFSClient |  Minor | dfsclient | Hongbing Wang | Hongbing Wang |
| [YARN-8047](https://issues.apache.org/jira/browse/YARN-8047) | RMWebApp make external class pluggable |  Minor | resourcemanager, webapp | Bibin Chundatt | Bilwa S T |
| [YARN-10333](https://issues.apache.org/jira/browse/YARN-10333) | YarnClient obtain Delegation Token for Log Aggregation Path |  Major | log-aggregation | Prabhu Joseph | Prabhu Joseph |
| [HADOOP-17079](https://issues.apache.org/jira/browse/HADOOP-17079) | Optimize UGI#getGroups by adding UGI#getGroupsSet |  Major | build | Xiaoyu Yao | Xiaoyu Yao |
| [YARN-10297](https://issues.apache.org/jira/browse/YARN-10297) | TestContinuousScheduling#testFairSchedulerContinuousSchedulingInitTime fails intermittently |  Major | fairscheduler, test | Jonathan Hung | Jim Brennan |
| [HDFS-15371](https://issues.apache.org/jira/browse/HDFS-15371) | Nonstandard characters exist in NameNode.java |  Minor | namanode | JiangHua Zhu | Zhao Yi Ming |
| [HADOOP-17127](https://issues.apache.org/jira/browse/HADOOP-17127) | Use RpcMetrics.TIMEUNIT to initialize rpc queueTime and processingTime |  Minor | common | Jim Brennan | Jim Brennan |
| [HDFS-15385](https://issues.apache.org/jira/browse/HDFS-15385) | Upgrade boost library to 1.72 |  Critical | build, libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-16930](https://issues.apache.org/jira/browse/HADOOP-16930) | Add com.amazonaws.auth.profile.ProfileCredentialsProvider to hadoop-aws docs |  Minor | documentation, fs/s3 | Nicholas Chammas | Nicholas Chammas |
| [HDFS-15476](https://issues.apache.org/jira/browse/HDFS-15476) | Make AsyncStream class' executor\_ member private |  Minor | build, libhdfs++ | Suraj Naik | Suraj Naik |
| [HDFS-15381](https://issues.apache.org/jira/browse/HDFS-15381) | Fix typo corrputBlocksFiles to corruptBlocksFiles |  Trivial | hdfs | bianqi | bianqi |
| [HDFS-15404](https://issues.apache.org/jira/browse/HDFS-15404) | ShellCommandFencer should expose info about source |  Major | ha, tools | Chen Liang | Chen Liang |
| [HADOOP-17147](https://issues.apache.org/jira/browse/HADOOP-17147) | Dead link in hadoop-kms/index.md.vm |  Minor | documentation, kms | Akira Ajisaka | Xieming Li |
| [HADOOP-17113](https://issues.apache.org/jira/browse/HADOOP-17113) | Adding ReadAhead Counters in ABFS |  Major | fs/azure | Mehakmeet Singh | Mehakmeet Singh |
| [YARN-10319](https://issues.apache.org/jira/browse/YARN-10319) | Record Last N Scheduler Activities from ActivitiesManager |  Major | activitiesmanager, resourcemanager, router | Prabhu Joseph | Prabhu Joseph |
| [HADOOP-17141](https://issues.apache.org/jira/browse/HADOOP-17141) | Add Capability To Get Text Length |  Minor | common, io | David Mollitor | David Mollitor |
| [YARN-10208](https://issues.apache.org/jira/browse/YARN-10208) | Add capacityScheduler metric for NODE\_UPDATE interval |  Minor | capacity scheduler, metrics | Pranjal Protim Borah | Pranjal Protim Borah |
| [YARN-10343](https://issues.apache.org/jira/browse/YARN-10343) | Legacy RM UI should include labeled metrics for allocated, total, and reserved resources. |  Major | resourcemanager, ui | Eric Payne | Eric Payne |
| [YARN-1529](https://issues.apache.org/jira/browse/YARN-1529) | Add Localization overhead metrics to NM |  Major | nodemanager | Gera Shegalov | Jim Brennan |
| [YARN-10381](https://issues.apache.org/jira/browse/YARN-10381) | Send out application attempt state along with other elements in the application attempt object returned from appattempts REST API call |  Minor | yarn-ui-v2 | Siddharth Ahuja | Siddharth Ahuja |
| [YARN-10361](https://issues.apache.org/jira/browse/YARN-10361) | Make custom DAO classes configurable into RMWebApp#JAXBContextResolver |  Major | resourcemanager, webapp | Prabhu Joseph | Bilwa S T |
| [HDFS-15512](https://issues.apache.org/jira/browse/HDFS-15512) | Remove smallBufferSize in DFSClient |  Minor | dfsclient | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-10251](https://issues.apache.org/jira/browse/YARN-10251) | Show extended resources on legacy RM UI. |  Major | . | Eric Payne | Eric Payne |
| [HDFS-15520](https://issues.apache.org/jira/browse/HDFS-15520) | Use visitor pattern to visit namespace tree |  Major | namenode | Tsz-wo Sze | Tsz-wo Sze |
| [YARN-10389](https://issues.apache.org/jira/browse/YARN-10389) | Option to override RMWebServices with custom WebService class |  Major | resourcemanager, webservice | Prabhu Joseph | Tanu Ajmera |
| [HDFS-15493](https://issues.apache.org/jira/browse/HDFS-15493) | Update block map and name cache in parallel while loading fsimage. |  Major | namenode | Chengwei Wang | Chengwei Wang |
| [HADOOP-17206](https://issues.apache.org/jira/browse/HADOOP-17206) | Add python2 to required package on CentOS 8 for building documentation |  Minor | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-15519](https://issues.apache.org/jira/browse/HDFS-15519) | Check inaccessible INodes in FsImageValidation |  Major | tools | Tsz-wo Sze | Tsz-wo Sze |
| [YARN-10399](https://issues.apache.org/jira/browse/YARN-10399) | Refactor NodeQueueLoadMonitor class to make it extendable |  Minor | resourcemanager | Zhengbo Li | Zhengbo Li |
| [HDFS-15448](https://issues.apache.org/jira/browse/HDFS-15448) | Remove duplicate BlockPoolManager starting when run DataNode |  Major | datanode | JiangHua Zhu | JiangHua Zhu |
| [HADOOP-17159](https://issues.apache.org/jira/browse/HADOOP-17159) | Make UGI support forceful relogin from keytab ignoring the last login time |  Major | security | Sandeep Guggilam | Sandeep Guggilam |
| [HADOOP-17232](https://issues.apache.org/jira/browse/HADOOP-17232) | Erasure Coding: Typo in document |  Trivial | documentation | Hui Fei | Hui Fei |
| [HDFS-15550](https://issues.apache.org/jira/browse/HDFS-15550) | Remove unused imports from TestFileTruncate.java |  Minor | test | Ravuri Sushma sree | Ravuri Sushma sree |
| [YARN-10342](https://issues.apache.org/jira/browse/YARN-10342) | [UI1] Provide a way to hide Tools section in Web UIv1 |  Minor | ui | Andras Gyori | Andras Gyori |
| [YARN-10407](https://issues.apache.org/jira/browse/YARN-10407) | Add phantomjsdriver.log to gitignore |  Minor | yarn | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-17235](https://issues.apache.org/jira/browse/HADOOP-17235) | Erasure Coding: Remove dead code from common side |  Minor | erasure-coding | Hui Fei | Hui Fei |
| [YARN-9136](https://issues.apache.org/jira/browse/YARN-9136) | getNMResourceInfo NodeManager REST API method is not documented |  Major | documentation, nodemanager | Szilard Nemeth | Hudáky Márton Gyula |
| [YARN-10353](https://issues.apache.org/jira/browse/YARN-10353) | Log vcores used and cumulative cpu in containers monitor |  Minor | yarn | Jim Brennan | Jim Brennan |
| [YARN-10369](https://issues.apache.org/jira/browse/YARN-10369) | Make NMTokenSecretManagerInRM sending NMToken for nodeId DEBUG |  Minor | yarn | Jim Brennan | Jim Brennan |
| [HDFS-14694](https://issues.apache.org/jira/browse/HDFS-14694) | Call recoverLease on DFSOutputStream close exception |  Major | hdfs-client | Chen Zhang | Lisheng Sun |
| [YARN-10390](https://issues.apache.org/jira/browse/YARN-10390) | LeafQueue: retain user limits cache across assignContainers() calls |  Major | capacity scheduler, capacityscheduler | Muhammad Samir Khan | Muhammad Samir Khan |
| [HDFS-15574](https://issues.apache.org/jira/browse/HDFS-15574) | Remove unnecessary sort of block list in DirectoryScanner |  Major | datanode | Stephen O'Donnell | Stephen O'Donnell |
| [HADOOP-17208](https://issues.apache.org/jira/browse/HADOOP-17208) | LoadBalanceKMSClientProvider#deleteKey should invalidateCache via all KMSClientProvider instances |  Major | common | Xiaoyu Yao | Xiaoyu Yao |
| [HADOOP-17270](https://issues.apache.org/jira/browse/HADOOP-17270) | Fix testCompressorDecompressorWithExeedBufferLimit to cover the intended scenario |  Minor | test | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-15579](https://issues.apache.org/jira/browse/HDFS-15579) | RBF: The constructor of PathLocation may got some misunderstanding |  Minor | rbf | Janus Chow | Janus Chow |
| [HDFS-15554](https://issues.apache.org/jira/browse/HDFS-15554) | RBF: force router check file existence in destinations before adding/updating mount points |  Minor | rbf | Fengnan Li | Fengnan Li |
| [HADOOP-17259](https://issues.apache.org/jira/browse/HADOOP-17259) | Allow SSLFactory fallback to input config if ssl-\*.xml fail to load from classpath |  Major | common | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-15581](https://issues.apache.org/jira/browse/HDFS-15581) | Access Controlled HTTPFS Proxy |  Minor | httpfs | Richard | Richard |
| [HDFS-15557](https://issues.apache.org/jira/browse/HDFS-15557) | Log the reason why a storage log file can't be deleted |  Minor | hdfs | Ye Ni | Ye Ni |
| [YARN-6754](https://issues.apache.org/jira/browse/YARN-6754) | Fair scheduler docs should explain meaning of weight=0 for a queue |  Major | docs | Daniel Templeton | Takeru Kuramoto |
| [HADOOP-17283](https://issues.apache.org/jira/browse/HADOOP-17283) | Hadoop - Upgrade to JQuery 3.5.1 |  Major | build, common | Aryan Gupta | Aryan Gupta |
| [HADOOP-17282](https://issues.apache.org/jira/browse/HADOOP-17282) | libzstd-dev should be used instead of libzstd1-dev on Ubuntu 18.04 or higher |  Minor | common | Takeru Kuramoto | Takeru Kuramoto |
| [HDFS-15594](https://issues.apache.org/jira/browse/HDFS-15594) | Lazy calculate live datanodes in safe mode tip |  Minor | namenode | Ye Ni | Ye Ni |
| [HDFS-15577](https://issues.apache.org/jira/browse/HDFS-15577) | Refactor TestTracing |  Major | test | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-15530](https://issues.apache.org/jira/browse/HDFS-15530) | RBF: Fix typo in DFS\_ROUTER\_QUOTA\_CACHE\_UPDATE\_INTERVAL var definition |  Minor | rbf | Sha Fanghao | Sha Fanghao |
| [HDFS-15604](https://issues.apache.org/jira/browse/HDFS-15604) | Fix Typo for HdfsDataNodeAdminGuide doc |  Trivial | documentation | Hui Fei | Hui Fei |
| [HDFS-15603](https://issues.apache.org/jira/browse/HDFS-15603) | RBF: Fix getLocationsForPath twice in create operation |  Major | rbf | Zhaohui Wang | Zhaohui Wang |
| [HADOOP-17284](https://issues.apache.org/jira/browse/HADOOP-17284) | Support BCFKS keystores for Hadoop Credential Provider |  Major | security | Xiaoyu Yao | Xiaoyu Yao |
| [HADOOP-17280](https://issues.apache.org/jira/browse/HADOOP-17280) | Service-user cost shouldn't be accumulated to totalDecayedCallCost and totalRawCallCost. |  Major | ipc | Jinglun | Jinglun |
| [HDFS-15415](https://issues.apache.org/jira/browse/HDFS-15415) | Reduce locking in Datanode DirectoryScanner |  Major | datanode | Stephen O'Donnell | Stephen O'Donnell |
| [HADOOP-17287](https://issues.apache.org/jira/browse/HADOOP-17287) | Support new Instance by non default constructor by ReflectionUtils |  Major | common | Baolong Mao | Baolong Mao |
| [HADOOP-17276](https://issues.apache.org/jira/browse/HADOOP-17276) | Extend CallerContext to make it include many items |  Major | common, ipc | Hui Fei | Hui Fei |
| [YARN-10451](https://issues.apache.org/jira/browse/YARN-10451) | RM (v1) UI NodesPage can NPE when yarn.io/gpu resource type is defined. |  Major | resourcemanager, ui | Eric Payne | Eric Payne |
| [MAPREDUCE-7301](https://issues.apache.org/jira/browse/MAPREDUCE-7301) | Expose Mini MR Cluster attribute for testing |  Minor | test | Swaroopa Kadam | Swaroopa Kadam |
| [HDFS-15567](https://issues.apache.org/jira/browse/HDFS-15567) | [SBN Read] HDFS should expose msync() API to allow downstream applications call it explicitly. |  Major | ha, hdfs-client | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-17304](https://issues.apache.org/jira/browse/HADOOP-17304) | KMS ACL: Allow DeleteKey Operation to Invalidate Cache |  Major | kms | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-15633](https://issues.apache.org/jira/browse/HDFS-15633) | Avoid redundant RPC calls for getDiskStatus |  Major | dfsclient | Ayush Saxena | Ayush Saxena |
| [YARN-10450](https://issues.apache.org/jira/browse/YARN-10450) | Add cpu and memory utilization per node and cluster-wide metrics |  Minor | yarn | Jim Brennan | Jim Brennan |
| [HADOOP-17144](https://issues.apache.org/jira/browse/HADOOP-17144) | Update Hadoop's lz4 to v1.9.2 |  Major | build, common | Hemanth Boyina | Hemanth Boyina |
| [HDFS-15629](https://issues.apache.org/jira/browse/HDFS-15629) | Add seqno when warning slow mirror/disk in BlockReceiver |  Major | datanode | Haibin Huang | Haibin Huang |
| [HADOOP-17302](https://issues.apache.org/jira/browse/HADOOP-17302) | Upgrade to jQuery 3.5.1 in hadoop-sls |  Major | build, common | Aryan Gupta | Aryan Gupta |
| [HDFS-15652](https://issues.apache.org/jira/browse/HDFS-15652) | Make block size from NNThroughputBenchmark configurable |  Minor | benchmarks | Hui Fei | Hui Fei |
| [YARN-10475](https://issues.apache.org/jira/browse/YARN-10475) | Scale RM-NM heartbeat interval based on node utilization |  Minor | yarn | Jim Brennan | Jim Brennan |
| [HDFS-15665](https://issues.apache.org/jira/browse/HDFS-15665) | Balancer logging improvement |  Major | balancer & mover | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-17342](https://issues.apache.org/jira/browse/HADOOP-17342) | Creating a token identifier should not do kerberos name resolution |  Major | common | Jim Brennan | Jim Brennan |
| [YARN-10479](https://issues.apache.org/jira/browse/YARN-10479) | RMProxy should retry on SocketTimeout Exceptions |  Major | yarn | Jim Brennan | Jim Brennan |
| [HDFS-15623](https://issues.apache.org/jira/browse/HDFS-15623) | Respect configured values of rpc.engine |  Major | hdfs | Hector Sandoval Chaverri | Hector Sandoval Chaverri |
| [HDFS-15668](https://issues.apache.org/jira/browse/HDFS-15668) | RBF: Fix RouterRPCMetrics annocation and document misplaced error |  Minor | documentation | Hongbing Wang | Hongbing Wang |
| [HADOOP-17369](https://issues.apache.org/jira/browse/HADOOP-17369) | Bump up snappy-java to 1.1.8.1 |  Minor | common | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-10480](https://issues.apache.org/jira/browse/YARN-10480) | replace href tags with ng-href |  Trivial | applications-catalog, webapp | Gabriel Medeiros Coelho | Gabriel Medeiros Coelho |
| [HDFS-15608](https://issues.apache.org/jira/browse/HDFS-15608) | Rename variable DistCp#CLEANUP |  Trivial | distcp | JiangHua Zhu | JiangHua Zhu |
| [HDFS-15469](https://issues.apache.org/jira/browse/HDFS-15469) | Dynamically configure the size of PacketReceiver#MAX\_PACKET\_SIZE |  Major | hdfs-client | JiangHua Zhu | JiangHua Zhu |
| [HADOOP-17367](https://issues.apache.org/jira/browse/HADOOP-17367) | Add InetAddress api to ProxyUsers.authorize |  Major | performance, security | Ahmed Hussein | Ahmed Hussein |
| [MAPREDUCE-7304](https://issues.apache.org/jira/browse/MAPREDUCE-7304) | Enhance the map-reduce Job end notifier to be able to notify the given URL via a custom class |  Major | mrv2 | Daniel Fritsi | Zoltán Erdmann |
| [HDFS-15684](https://issues.apache.org/jira/browse/HDFS-15684) | EC: Call recoverLease on DFSStripedOutputStream close exception |  Major | dfsclient, ec | Hongbing Wang | Hongbing Wang |
| [MAPREDUCE-7309](https://issues.apache.org/jira/browse/MAPREDUCE-7309) | Improve performance of reading resource request for mapper/reducers from config |  Major | applicationmaster | Wangda Tan | Peter Bacsko |
| [HDFS-15694](https://issues.apache.org/jira/browse/HDFS-15694) | Avoid calling UpdateHeartBeatState inside DataNodeDescriptor |  Major | datanode | Ahmed Hussein | Ahmed Hussein |
| [HDFS-14904](https://issues.apache.org/jira/browse/HDFS-14904) | Add Option to let Balancer prefer highly utilized nodes in each iteration |  Major | balancer & mover | Leon Gao | Leon Gao |
| [HDFS-15705](https://issues.apache.org/jira/browse/HDFS-15705) | Fix a typo in SecondaryNameNode.java |  Trivial | hdfs | Sixiang Ma | Sixiang Ma |
| [HDFS-15703](https://issues.apache.org/jira/browse/HDFS-15703) | Don't generate edits for set operations that are no-op |  Major | namenode | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17392](https://issues.apache.org/jira/browse/HADOOP-17392) | Remote exception messages should not include the exception class |  Major | ipc | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15706](https://issues.apache.org/jira/browse/HDFS-15706) | HttpFS: Log more information on request failures |  Major | httpfs | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17389](https://issues.apache.org/jira/browse/HADOOP-17389) | KMS should log full UGI principal |  Major | kms | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15221](https://issues.apache.org/jira/browse/HDFS-15221) | Add checking of effective filesystem during initializing storage locations |  Minor | datanode | Yang Yun | Yang Yun |
| [HDFS-15712](https://issues.apache.org/jira/browse/HDFS-15712) | Upgrade googletest to 1.10.0 |  Critical | build, libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-17425](https://issues.apache.org/jira/browse/HADOOP-17425) | Bump up snappy-java to 1.1.8.2 |  Minor | build, common | L. C. Hsieh | L. C. Hsieh |
| [HDFS-15717](https://issues.apache.org/jira/browse/HDFS-15717) | Improve fsck logging |  Major | logging, namenode | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15728](https://issues.apache.org/jira/browse/HDFS-15728) | Update description of dfs.datanode.handler.count in hdfs-default.xml |  Minor | configuration | liuyan | liuyan |
| [HDFS-15704](https://issues.apache.org/jira/browse/HDFS-15704) | Mitigate lease monitor's rapid infinite loop |  Major | namenode | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15733](https://issues.apache.org/jira/browse/HDFS-15733) | Add seqno in log when BlockReceiver receive packet |  Minor | datanode | Haibin Huang | Haibin Huang |
| [HDFS-15655](https://issues.apache.org/jira/browse/HDFS-15655) | Add option to make balancer prefer to get cold blocks |  Minor | balancer & mover | Yang Yun | Yang Yun |
| [HDFS-15569](https://issues.apache.org/jira/browse/HDFS-15569) | Speed up the Storage#doRecover during datanode rolling upgrade |  Major | datanode | Hemanth Boyina | Hemanth Boyina |
| [HDFS-15749](https://issues.apache.org/jira/browse/HDFS-15749) | Make size of editPendingQ can be configurable |  Major | hdfs | Baolong Mao | Baolong Mao |
| [HDFS-15745](https://issues.apache.org/jira/browse/HDFS-15745) | Make DataNodePeerMetrics#LOW\_THRESHOLD\_MS and MIN\_OUTLIER\_DETECTION\_NODES configurable |  Major | datanode, metrics | Haibin Huang | Haibin Huang |
| [HDFS-15751](https://issues.apache.org/jira/browse/HDFS-15751) | Add documentation for msync() API to filesystem.md |  Major | documentation | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-15754](https://issues.apache.org/jira/browse/HDFS-15754) | Create packet metrics for DataNode |  Minor | datanode | Fengnan Li | Fengnan Li |
| [YARN-10538](https://issues.apache.org/jira/browse/YARN-10538) | Add recommissioning nodes to the list of updated nodes returned to the AM |  Major | resourcemanager | Srinivas S T | Srinivas S T |
| [YARN-10541](https://issues.apache.org/jira/browse/YARN-10541) | capture the performance metrics of ZKRMStateStore |  Minor | resourcemanager | Max  Xie | Max  Xie |
| [HADOOP-17408](https://issues.apache.org/jira/browse/HADOOP-17408) | Optimize NetworkTopology while sorting of block locations |  Major | common, net | Ahmed Hussein | Ahmed Hussein |
| [YARN-8529](https://issues.apache.org/jira/browse/YARN-8529) | Add timeout to RouterWebServiceUtil#invokeRMWebService |  Major | router, webservice | Íñigo Goiri | Minni Mittal |
| [YARN-4589](https://issues.apache.org/jira/browse/YARN-4589) | Diagnostics for localization timeouts is lacking |  Major | nodemanager | Chang Li | Chang Li |
| [YARN-10562](https://issues.apache.org/jira/browse/YARN-10562) | Follow up changes for YARN-9833 |  Major | yarn | Jim Brennan | Jim Brennan |
| [HDFS-15758](https://issues.apache.org/jira/browse/HDFS-15758) | Fix typos in MutableMetric |  Trivial | metrics | Haibin Huang | Haibin Huang |
| [HDFS-15783](https://issues.apache.org/jira/browse/HDFS-15783) | Speed up BlockPlacementPolicyRackFaultTolerant#verifyBlockPlacement |  Major | block placement | Akira Ajisaka | Akira Ajisaka |
| [YARN-10519](https://issues.apache.org/jira/browse/YARN-10519) | Refactor QueueMetricsForCustomResources class to move to yarn-common package |  Major | metrics | Minni Mittal | Minni Mittal |
| [YARN-10490](https://issues.apache.org/jira/browse/YARN-10490) | "yarn top" command not quitting completely with ctrl+c |  Minor | yarn | Agshin Kazimli | Agshin Kazimli |
| [HADOOP-17478](https://issues.apache.org/jira/browse/HADOOP-17478) | Improve the description of hadoop.http.authentication.signature.secret.file |  Minor | documentation | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17452](https://issues.apache.org/jira/browse/HADOOP-17452) | Upgrade guice to 4.2.3 |  Major | build, common | Yuming Wang | Yuming Wang |
| [HADOOP-17465](https://issues.apache.org/jira/browse/HADOOP-17465) | Update Dockerfile to use Focal |  Major | build, test | Gautham Banasandra | Gautham Banasandra |
| [HDFS-15789](https://issues.apache.org/jira/browse/HDFS-15789) | Lease renewal does not require namesystem lock |  Major | hdfs | Jim Brennan | Jim Brennan |
| [HDFS-15740](https://issues.apache.org/jira/browse/HDFS-15740) | Make basename cross-platform |  Major | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-17501](https://issues.apache.org/jira/browse/HADOOP-17501) | Fix logging typo in ShutdownHookManager |  Major | common | Konstantin Shvachko | Fengnan Li |
| [HADOOP-17354](https://issues.apache.org/jira/browse/HADOOP-17354) | Move Jenkinsfile outside of the root directory |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17508](https://issues.apache.org/jira/browse/HADOOP-17508) | Simplify dependency installation instructions |  Trivial | documentation | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-17509](https://issues.apache.org/jira/browse/HADOOP-17509) | Parallelize building of dependencies |  Minor | build | Gautham Banasandra | Gautham Banasandra |
| [HDFS-15803](https://issues.apache.org/jira/browse/HDFS-15803) | EC: Remove unnecessary method (getWeight) in StripedReconstructionInfo |  Trivial | erasure-coding | Haiyang Hu | Haiyang Hu |
| [HDFS-15799](https://issues.apache.org/jira/browse/HDFS-15799) | Make DisallowedDatanodeException terse |  Minor | hdfs | Richard | Richard |
| [HDFS-15819](https://issues.apache.org/jira/browse/HDFS-15819) | Fix a codestyle issue for TestQuotaByStorageType |  Trivial | hdfs | Baolong Mao | Baolong Mao |
| [YARN-10610](https://issues.apache.org/jira/browse/YARN-10610) | Add queuePath to RESTful API for CapacityScheduler consistent with FairScheduler queuePath |  Major | capacity scheduler | Qi Zhu | Qi Zhu |
| [HDFS-15813](https://issues.apache.org/jira/browse/HDFS-15813) | DataStreamer: keep sending heartbeat packets while streaming |  Major | hdfs | Jim Brennan | Jim Brennan |
| [YARN-9650](https://issues.apache.org/jira/browse/YARN-9650) | Set thread names for CapacityScheduler AsyncScheduleThread |  Minor | capacity scheduler | Bibin Chundatt | Amogh Desai |
| [MAPREDUCE-7319](https://issues.apache.org/jira/browse/MAPREDUCE-7319) | Log list of mappers at trace level in ShuffleHandler audit log |  Minor | yarn | Jim Brennan | Jim Brennan |
| [HDFS-15821](https://issues.apache.org/jira/browse/HDFS-15821) | Add metrics for in-service datanodes |  Minor | metrics | Zehao Chen | Zehao Chen |
| [YARN-10625](https://issues.apache.org/jira/browse/YARN-10625) | FairScheduler: add global flag to disable AM-preemption |  Major | fairscheduler | Peter Bacsko | Peter Bacsko |
| [YARN-10626](https://issues.apache.org/jira/browse/YARN-10626) | Log resource allocation in NM log at container start time |  Major | nodemanager | Eric Badger | Eric Badger |
| [HDFS-15815](https://issues.apache.org/jira/browse/HDFS-15815) |  if required storageType are unavailable, log the failed reason during choosing Datanode |  Minor | block placement | Yang Yun | Yang Yun |
| [HDFS-15830](https://issues.apache.org/jira/browse/HDFS-15830) | Support to make dfs.image.parallel.load reconfigurable |  Major | namenode | Hui Fei | Hui Fei |
| [HDFS-15835](https://issues.apache.org/jira/browse/HDFS-15835) | Erasure coding: Add/remove logs for the better readability/debugging |  Minor | erasure-coding, hdfs | Bhavik Patel | Bhavik Patel |
| [HDFS-15826](https://issues.apache.org/jira/browse/HDFS-15826) | Solve the problem of incorrect progress of delegation tokens when loading FsImage |  Major | namanode | JiangHua Zhu | JiangHua Zhu |
| [HDFS-15734](https://issues.apache.org/jira/browse/HDFS-15734) | [READ] DirectoryScanner#scan need not check StorageType.PROVIDED |  Minor | datanode | Yuxuan Wang | Yuxuan Wang |
| [HADOOP-17538](https://issues.apache.org/jira/browse/HADOOP-17538) | Add kms-default.xml and httpfs-default.xml to site index |  Minor | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-10613](https://issues.apache.org/jira/browse/YARN-10613) | Config to allow Intra- and Inter-queue preemption to  enable/disable conservativeDRF |  Minor | capacity scheduler, scheduler preemption | Eric Payne | Eric Payne |
| [YARN-10653](https://issues.apache.org/jira/browse/YARN-10653) | Fixed the findbugs issues introduced by YARN-10647. |  Major | test | Qi Zhu | Qi Zhu |
| [HDFS-15856](https://issues.apache.org/jira/browse/HDFS-15856) | Make write pipeline retry times configurable. |  Minor | hdfs-client | Qi Zhu | Qi Zhu |
| [MAPREDUCE-7324](https://issues.apache.org/jira/browse/MAPREDUCE-7324) | ClientHSSecurityInfo class is in wrong META-INF file |  Major | mapreduce-client | Eric Badger | Eric Badger |
| [HADOOP-17546](https://issues.apache.org/jira/browse/HADOOP-17546) | Update Description of hadoop-http-auth-signature-secret in HttpAuthentication.md |  Minor | documentation | Ravuri Sushma sree | Ravuri Sushma sree |
| [YARN-10623](https://issues.apache.org/jira/browse/YARN-10623) | Capacity scheduler should support refresh queue automatically by a thread policy. |  Major | capacity scheduler | Qi Zhu | Qi Zhu |
| [HADOOP-17552](https://issues.apache.org/jira/browse/HADOOP-17552) | Change ipc.client.rpc-timeout.ms from 0 to 120000 by default to avoid potential hang |  Major | ipc | Haoze Wu | Haoze Wu |
| [HDFS-15384](https://issues.apache.org/jira/browse/HDFS-15384) | Document getLocatedBlocks(String src, long start) of DFSClient only return partial blocks |  Minor | documentation | Yang Yun | Yang Yun |
| [YARN-10658](https://issues.apache.org/jira/browse/YARN-10658) | CapacityScheduler QueueInfo add queue path field to avoid ambiguous QueueName. |  Major | capacity scheduler | Qi Zhu | Qi Zhu |
| [YARN-10664](https://issues.apache.org/jira/browse/YARN-10664) | Allow parameter expansion in NM\_ADMIN\_USER\_ENV |  Major | yarn | Jim Brennan | Jim Brennan |
| [HADOOP-17570](https://issues.apache.org/jira/browse/HADOOP-17570) | Apply YETUS-1102 to re-enable GitHub comments |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17514](https://issues.apache.org/jira/browse/HADOOP-17514) | Remove trace subcommand from hadoop CLI |  Minor | scripts | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-17482](https://issues.apache.org/jira/browse/HADOOP-17482) | Remove Commons Logger from FileSystem Class |  Minor | common | David Mollitor | David Mollitor |
| [HDFS-15882](https://issues.apache.org/jira/browse/HDFS-15882) | Fix incorrectly initializing RandomAccessFile based on configuration options |  Major | namanode | Xie Lei | Xie Lei |
| [HDFS-15843](https://issues.apache.org/jira/browse/HDFS-15843) | [libhdfs++] Make write cross platform |  Major | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [YARN-10497](https://issues.apache.org/jira/browse/YARN-10497) | Fix an issue in CapacityScheduler which fails to delete queues |  Major | capacity scheduler | Wangda Tan | Wangda Tan |
| [HADOOP-17594](https://issues.apache.org/jira/browse/HADOOP-17594) | DistCp: Expose the JobId for applications executing through run method |  Major | tools/distcp | Ayush Saxena | Ayush Saxena |
| [YARN-10476](https://issues.apache.org/jira/browse/YARN-10476) |  Queue metrics for Unmanaged applications |  Minor | resourcemanager | Cyrus Jackson | Cyrus Jackson |
| [HDFS-15787](https://issues.apache.org/jira/browse/HDFS-15787) | Remove unnecessary Lease Renew  in FSNamesystem#internalReleaseLease |  Major | namenode | Lisheng Sun | Lisheng Sun |
| [HDFS-15903](https://issues.apache.org/jira/browse/HDFS-15903) | Refactor X-Platform library |  Minor | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-17599](https://issues.apache.org/jira/browse/HADOOP-17599) | Remove NULL checks before instanceof |  Minor | common | Jiajun Jiang | Jiajun Jiang |
| [HDFS-15913](https://issues.apache.org/jira/browse/HDFS-15913) | Remove useless NULL checks before instanceof |  Minor | hdfs | Jiajun Jiang | Jiajun Jiang |
| [HDFS-15907](https://issues.apache.org/jira/browse/HDFS-15907) | Reduce Memory Overhead of AclFeature by avoiding AtomicInteger |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-15911](https://issues.apache.org/jira/browse/HDFS-15911) | Provide blocks moved count in Balancer iteration result |  Major | balancer & mover | Viraj Jasani | Viraj Jasani |
| [HDFS-15919](https://issues.apache.org/jira/browse/HDFS-15919) | BlockPoolManager should log stack trace if unable to get Namenode addresses |  Major | datanode | Stephen O'Donnell | Stephen O'Donnell |
| [HADOOP-17133](https://issues.apache.org/jira/browse/HADOOP-17133) | Implement HttpServer2 metrics |  Major | httpfs, kms | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17531](https://issues.apache.org/jira/browse/HADOOP-17531) | DistCp: Reduce memory usage on copying huge directories |  Critical | tools/distcp | Ayush Saxena | Ayush Saxena |
| [HDFS-15879](https://issues.apache.org/jira/browse/HDFS-15879) | Exclude slow nodes when choose targets for blocks |  Major | block placement | Tao Li | Tao Li |
| [HDFS-15764](https://issues.apache.org/jira/browse/HDFS-15764) | Notify Namenode missing or new block on disk as soon as possible |  Minor | datanode | Yang Yun | Yang Yun |
| [HADOOP-16870](https://issues.apache.org/jira/browse/HADOOP-16870) | Use spotbugs-maven-plugin instead of findbugs-maven-plugin |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17222](https://issues.apache.org/jira/browse/HADOOP-17222) |  Create socket address leveraging URI cache |  Major | common, hdfs-client | Rui Fan | Rui Fan |
| [YARN-10544](https://issues.apache.org/jira/browse/YARN-10544) | AMParams.java having un-necessary access identifier static final |  Trivial | resourcemanager | ANANDA G B | ANANDA G B |
| [HDFS-15932](https://issues.apache.org/jira/browse/HDFS-15932) | Improve the balancer error message when process exits abnormally. |  Major | balancer | Renukaprasad C | Renukaprasad C |
| [HDFS-15863](https://issues.apache.org/jira/browse/HDFS-15863) | RBF: Validation message to be corrected in FairnessPolicyController |  Minor | rbf | Renukaprasad C | Renukaprasad C |
| [HADOOP-16524](https://issues.apache.org/jira/browse/HADOOP-16524) | Automatic keystore reloading for HttpServer2 |  Major | common | Kihwal Lee | Borislav Iordanov |
| [YARN-10726](https://issues.apache.org/jira/browse/YARN-10726) | Log the size of DelegationTokenRenewer event queue in case of too many pending events |  Major | resourcemanager | Qi Zhu | Qi Zhu |
| [HDFS-15931](https://issues.apache.org/jira/browse/HDFS-15931) | Fix non-static inner classes for better memory management |  Major | hdfs | Viraj Jasani | Viraj Jasani |
| [HADOOP-17371](https://issues.apache.org/jira/browse/HADOOP-17371) | Bump Jetty to the latest version 9.4.35 |  Major | build, common | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-15942](https://issues.apache.org/jira/browse/HDFS-15942) | Increase Quota initialization threads |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-15909](https://issues.apache.org/jira/browse/HDFS-15909) | Make fnmatch cross platform |  Major | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-17613](https://issues.apache.org/jira/browse/HADOOP-17613) | Log not flushed fully when daemon shutdown |  Major | common | Renukaprasad C | Renukaprasad C |
| [HDFS-15937](https://issues.apache.org/jira/browse/HDFS-15937) | Reduce memory used during datanode layout upgrade |  Major | datanode | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-15955](https://issues.apache.org/jira/browse/HDFS-15955) | Make explicit\_bzero cross platform |  Major | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [HDFS-15962](https://issues.apache.org/jira/browse/HDFS-15962) | Make strcasecmp cross platform |  Major | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-17569](https://issues.apache.org/jira/browse/HADOOP-17569) | Building native code fails on Fedora 33 |  Major | build, common | Kengo Seki | Masatake Iwasaki |
| [HADOOP-17633](https://issues.apache.org/jira/browse/HADOOP-17633) | Bump json-smart to 2.4.2 and nimbus-jose-jwt to 9.8 due to CVEs |  Major | auth, build | helen huang | Viraj Jasani |
| [HADOOP-17620](https://issues.apache.org/jira/browse/HADOOP-17620) | DistCp: Use Iterator for listing target directory as well |  Major | tools/distcp | Ayush Saxena | Ayush Saxena |
| [YARN-10743](https://issues.apache.org/jira/browse/YARN-10743) | Add a policy for not aggregating for containers which are killed because exceeding container log size limit. |  Major | nodemanager | Qi Zhu | Qi Zhu |
| [HDFS-15978](https://issues.apache.org/jira/browse/HDFS-15978) | Solve DatanodeManager#getBlockRecoveryCommand() printing IOException |  Trivial | namanode | JiangHua Zhu | JiangHua Zhu |
| [HDFS-15967](https://issues.apache.org/jira/browse/HDFS-15967) | Improve the log for Short Circuit Local Reads |  Minor | datanode | Bhavik Patel | Bhavik Patel |
| [HADOOP-17675](https://issues.apache.org/jira/browse/HADOOP-17675) | LdapGroupsMapping$LdapSslSocketFactory ClassNotFoundException |  Major | common | Tamas Mate | István Fajth |
| [HDFS-15934](https://issues.apache.org/jira/browse/HDFS-15934) | Make DirectoryScanner reconcile blocks batch size and interval between batch configurable. |  Major | datanode, diskbalancer | Qi Zhu | Qi Zhu |
| [HADOOP-11616](https://issues.apache.org/jira/browse/HADOOP-11616) | Remove workaround for Curator's ChildReaper requiring Guava 15+ |  Major | common | Robert Kanter | Viraj Jasani |
| [HADOOP-17690](https://issues.apache.org/jira/browse/HADOOP-17690) | Improve the log for The DecayRpcScheduler |  Minor | ipc | Bhavik Patel | Bhavik Patel |
| [HDFS-16003](https://issues.apache.org/jira/browse/HDFS-16003) | ProcessReport print invalidatedBlocks should judge debug level at first |  Minor | namanode | lei w | lei w |
| [HADOOP-17678](https://issues.apache.org/jira/browse/HADOOP-17678) | Dockerfile for building on Centos 7 |  Major | build | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16007](https://issues.apache.org/jira/browse/HDFS-16007) | Deserialization of ReplicaState should avoid throwing ArrayIndexOutOfBoundsException |  Major | hdfs | junwen yang | Viraj Jasani |
| [HADOOP-16822](https://issues.apache.org/jira/browse/HADOOP-16822) | Provide source artifacts for hadoop-client-api |  Major | build | Karel Kolman | Karel Kolman |
| [HADOOP-17693](https://issues.apache.org/jira/browse/HADOOP-17693) | Dockerfile for building on Centos 8 |  Major | build | Gautham Banasandra | Gautham Banasandra |
| [MAPREDUCE-7343](https://issues.apache.org/jira/browse/MAPREDUCE-7343) | Increase the job name max length in mapred job -list |  Major | mapreduce-client | Ayush Saxena | Ayush Saxena |
| [YARN-10737](https://issues.apache.org/jira/browse/YARN-10737) | Fix typos in CapacityScheduler#schedule. |  Minor | capacity scheduler | Qi Zhu | Qi Zhu |
| [YARN-10545](https://issues.apache.org/jira/browse/YARN-10545) | Improve the readability of diagnostics log in yarn-ui2 web page. |  Minor | yarn-ui-v2 | huangkunlun | huangkunlun |
| [HADOOP-17680](https://issues.apache.org/jira/browse/HADOOP-17680) | Allow ProtobufRpcEngine to be extensible |  Major | common | Hector Sandoval Chaverri | Hector Sandoval Chaverri |
| [YARN-10763](https://issues.apache.org/jira/browse/YARN-10763) | Add  the number of containers assigned per second metrics to ClusterMetrics |  Minor | metrics | chaosju | chaosju |
| [HDFS-15877](https://issues.apache.org/jira/browse/HDFS-15877) | BlockReconstructionWork should resetTargets() before BlockManager#validateReconstructionWork return false |  Minor | block placement | Haiyang Hu | Haiyang Hu |
| [YARN-10258](https://issues.apache.org/jira/browse/YARN-10258) | Add metrics for 'ApplicationsRunning' in NodeManager |  Minor | nodemanager | ANANDA G B | ANANDA G B |
| [HDFS-15757](https://issues.apache.org/jira/browse/HDFS-15757) | RBF: Improving Router Connection Management |  Major | rbf | Fengnan Li | Fengnan Li |
| [HDFS-16018](https://issues.apache.org/jira/browse/HDFS-16018) | Optimize the display of hdfs "count -e" or "count -t" command |  Minor | dfsclient | Hongbing Wang | Hongbing Wang |
| [YARN-9279](https://issues.apache.org/jira/browse/YARN-9279) | Remove the old hamlet package |  Major | webapp | Akira Ajisaka | Akira Ajisaka |
| [YARN-10123](https://issues.apache.org/jira/browse/YARN-10123) | Error message around yarn app -stop/start can be improved to highlight that an implementation at framework level is needed for the stop/start functionality to work |  Minor | client, documentation | Siddharth Ahuja | Siddharth Ahuja |
| [YARN-10753](https://issues.apache.org/jira/browse/YARN-10753) | Document the removal of FS default queue creation |  Major | fairscheduler | Benjamin Teke | Benjamin Teke |
| [HDFS-15790](https://issues.apache.org/jira/browse/HDFS-15790) | Make ProtobufRpcEngineProtos and ProtobufRpcEngineProtos2 Co-Exist |  Critical | ipc | David Mollitor | Vinayakumar B |
| [HDFS-16024](https://issues.apache.org/jira/browse/HDFS-16024) | RBF: Rename data to the Trash should be based on src locations |  Major | rbf | Xiangyi Zhu | Xiangyi Zhu |
| [HDFS-15971](https://issues.apache.org/jira/browse/HDFS-15971) | Make mkstemp cross platform |  Major | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [HDFS-15946](https://issues.apache.org/jira/browse/HDFS-15946) | Fix java doc in FSPermissionChecker |  Minor | documentation | Tao Li | Tao Li |
| [HADOOP-17727](https://issues.apache.org/jira/browse/HADOOP-17727) | Modularize docker images |  Major | build | Gautham Banasandra | Gautham Banasandra |
| [YARN-10792](https://issues.apache.org/jira/browse/YARN-10792) | Set Completed AppAttempt LogsLink to Log Server Url |  Major | webapp | Prabhu Joseph | Abhinaba Sarkar |
| [HADOOP-17756](https://issues.apache.org/jira/browse/HADOOP-17756) | Increase precommit job timeout from 20 hours to 24 hours. |  Major | build | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-10802](https://issues.apache.org/jira/browse/YARN-10802) | Change Capacity Scheduler minimum-user-limit-percent to accept decimal values |  Major | capacity scheduler | Benjamin Teke | Benjamin Teke |
| [HDFS-16073](https://issues.apache.org/jira/browse/HDFS-16073) | Remove redundant RPC requests for getFileLinkInfo in ClientNamenodeProtocolTranslatorPB |  Minor | hdfs-client | lei w | lei w |
| [HDFS-16074](https://issues.apache.org/jira/browse/HDFS-16074) | Remove an expensive debug string concatenation |  Major | hdfs-client | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-17724](https://issues.apache.org/jira/browse/HADOOP-17724) | Add Dockerfile for Debian 10 |  Major | build | Gautham Banasandra | Gautham Banasandra |
| [HDFS-15842](https://issues.apache.org/jira/browse/HDFS-15842) | HDFS mover to emit metrics |  Major | balancer & mover | Leon Gao | Leon Gao |
| [HDFS-16080](https://issues.apache.org/jira/browse/HDFS-16080) | RBF: Invoking method in all locations should break the loop after successful result |  Minor | rbf | Viraj Jasani | Viraj Jasani |
| [HDFS-16075](https://issues.apache.org/jira/browse/HDFS-16075) | Use empty array constants present in StorageType and DatanodeInfo to avoid creating redundant objects |  Major | hdfs | Viraj Jasani | Viraj Jasani |
| [MAPREDUCE-7354](https://issues.apache.org/jira/browse/MAPREDUCE-7354) | Use empty array constants present in TaskCompletionEvent to avoid creating redundant objects |  Minor | mrv2 | Viraj Jasani | Viraj Jasani |
| [HDFS-16082](https://issues.apache.org/jira/browse/HDFS-16082) | Avoid non-atomic operations on exceptionsSinceLastBalance and failedTimesSinceLastSuccessfulBalance in Balancer |  Major | balancer | Viraj Jasani | Viraj Jasani |
| [HADOOP-17766](https://issues.apache.org/jira/browse/HADOOP-17766) | CI for Debian 10 |  Major | build | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16076](https://issues.apache.org/jira/browse/HDFS-16076) | Avoid using slow DataNodes for reading by sorting locations |  Major | hdfs | Tao Li | Tao Li |
| [HDFS-16085](https://issues.apache.org/jira/browse/HDFS-16085) | Move the getPermissionChecker out of the read lock |  Minor | namanode | Tao Li | Tao Li |
| [YARN-10834](https://issues.apache.org/jira/browse/YARN-10834) | Intra-queue preemption: apps that don't use defined custom resource won't be preempted. |  Major | scheduler preemption | Eric Payne | Eric Payne |
| [HADOOP-17777](https://issues.apache.org/jira/browse/HADOOP-17777) | Update clover-maven-plugin version from 3.3.0 to 4.4.1 |  Major | build, common | Wanqiang Ji | Wanqiang Ji |
| [HDFS-16096](https://issues.apache.org/jira/browse/HDFS-16096) | Delete useless method DirectoryWithQuotaFeature#setQuota |  Major | hdfs | Xiangyi Zhu | Xiangyi Zhu |
| [HDFS-16090](https://issues.apache.org/jira/browse/HDFS-16090) | Fine grained locking for datanodeNetworkCounts |  Major | datanode | Viraj Jasani | Viraj Jasani |
| [HADOOP-17778](https://issues.apache.org/jira/browse/HADOOP-17778) | CI for Centos 8 |  Major | build | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16086](https://issues.apache.org/jira/browse/HDFS-16086) | Add volume information to datanode log for tracing |  Minor | datanode | Tao Li | Tao Li |
| [YARN-9698](https://issues.apache.org/jira/browse/YARN-9698) | [Umbrella] Tools to help migration from Fair Scheduler to Capacity Scheduler |  Major | capacity scheduler | Weiwei Yang | Weiwei Yang |
| [HDFS-16101](https://issues.apache.org/jira/browse/HDFS-16101) | Remove unuse variable and IoException in ProvidedStorageMap |  Minor | namenode | lei w | lei w |
| [HADOOP-17749](https://issues.apache.org/jira/browse/HADOOP-17749) | Remove lock contention in SelectorPool of SocketIOWithTimeout |  Major | common | Xuesen Liang | Xuesen Liang |
| [HDFS-16114](https://issues.apache.org/jira/browse/HDFS-16114) | the balancer parameters print error |  Minor | balancer | jiaguodong | jiaguodong |
| [HADOOP-17775](https://issues.apache.org/jira/browse/HADOOP-17775) | Remove JavaScript package from Docker environment |  Major | build | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-16088](https://issues.apache.org/jira/browse/HDFS-16088) | Standby NameNode process getLiveDatanodeStorageReport request to reduce Active load |  Major | namanode | Tao Li | Tao Li |
| [HADOOP-17794](https://issues.apache.org/jira/browse/HADOOP-17794) | Add a sample configuration to use ZKDelegationTokenSecretManager in Hadoop KMS |  Major | documentation, kms, security | Akira Ajisaka | Akira Ajisaka |
| [HDFS-16122](https://issues.apache.org/jira/browse/HDFS-16122) | Fix DistCpContext#toString() |  Minor | distcp | Tao Li | Tao Li |
| [HADOOP-12665](https://issues.apache.org/jira/browse/HADOOP-12665) | Document hadoop.security.token.service.use\_ip |  Major | documentation | Arpit Agarwal | Akira Ajisaka |
| [HDFS-15785](https://issues.apache.org/jira/browse/HDFS-15785) | Datanode to support using DNS to resolve nameservices to IP addresses to get list of namenodes |  Major | datanode | Leon Gao | Leon Gao |
| [HADOOP-17672](https://issues.apache.org/jira/browse/HADOOP-17672) | Remove an invalid comment content in the FileContext class |  Major | common | JiangHua Zhu | JiangHua Zhu |
| [YARN-10456](https://issues.apache.org/jira/browse/YARN-10456) | RM PartitionQueueMetrics records are named QueueMetrics in Simon metrics registry |  Major | resourcemanager | Eric Payne | Eric Payne |
| [HDFS-15650](https://issues.apache.org/jira/browse/HDFS-15650) | Make the socket timeout for computing checksum of striped blocks configurable |  Minor | datanode, ec, erasure-coding | Yushi Hayasaka | Yushi Hayasaka |
| [YARN-10858](https://issues.apache.org/jira/browse/YARN-10858) | [UI2] YARN-10826 breaks Queue view |  Major | yarn-ui-v2 | Andras Gyori | Masatake Iwasaki |
| [HADOOP-16290](https://issues.apache.org/jira/browse/HADOOP-16290) | Enable RpcMetrics units to be configurable |  Major | ipc, metrics | Erik Krogen | Viraj Jasani |
| [YARN-10860](https://issues.apache.org/jira/browse/YARN-10860) | Make max container per heartbeat configs refreshable |  Major | capacity scheduler | Eric Badger | Eric Badger |
| [HADOOP-17813](https://issues.apache.org/jira/browse/HADOOP-17813) | Checkstyle - Allow line length: 100 |  Major | common | Akira Ajisaka | Viraj Jasani |
| [HDFS-16119](https://issues.apache.org/jira/browse/HDFS-16119) | start balancer with parameters -hotBlockTimeInterval xxx is invalid |  Minor | balancer | jiaguodong | jiaguodong |
| [HDFS-16137](https://issues.apache.org/jira/browse/HDFS-16137) | Improve the comments related to FairCallQueue#queues |  Minor | ipc | JiangHua Zhu | JiangHua Zhu |
| [HADOOP-17811](https://issues.apache.org/jira/browse/HADOOP-17811) | ABFS ExponentialRetryPolicy doesn't pick up configuration values |  Minor | documentation, fs/azure | Brian Frank Loss | Brian Frank Loss |
| [HADOOP-17819](https://issues.apache.org/jira/browse/HADOOP-17819) | Add extensions to ProtobufRpcEngine RequestHeaderProto |  Major | common | Hector Sandoval Chaverri | Hector Sandoval Chaverri |
| [HDFS-15936](https://issues.apache.org/jira/browse/HDFS-15936) | Solve BlockSender#sendPacket() does not record SocketTimeout exception |  Minor | datanode | JiangHua Zhu | JiangHua Zhu |
| [YARN-10628](https://issues.apache.org/jira/browse/YARN-10628) | Add node usage metrics in SLS |  Major | scheduler-load-simulator | VADAGA ANANYO RAO | VADAGA ANANYO RAO |
| [YARN-10663](https://issues.apache.org/jira/browse/YARN-10663) | Add runningApps stats in SLS |  Major | yarn | VADAGA ANANYO RAO | VADAGA ANANYO RAO |
| [YARN-10856](https://issues.apache.org/jira/browse/YARN-10856) | Prevent ATS v2 health check REST API call if the ATS service itself is disabled. |  Major | yarn-ui-v2 | Siddharth Ahuja | Benjamin Teke |
| [HADOOP-17815](https://issues.apache.org/jira/browse/HADOOP-17815) | Run CI for Centos 7 |  Critical | build | Gautham Banasandra | Gautham Banasandra |
| [YARN-10854](https://issues.apache.org/jira/browse/YARN-10854) | Support marking inactive node as untracked without configured include path |  Major | resourcemanager | Tao Yang | Tao Yang |
| [HDFS-16149](https://issues.apache.org/jira/browse/HDFS-16149) | Improve the parameter annotation in FairCallQueue#priorityLevels |  Minor | ipc | JiangHua Zhu | JiangHua Zhu |
| [YARN-10874](https://issues.apache.org/jira/browse/YARN-10874) | Refactor NM ContainerLaunch#getEnvDependencies's unit tests |  Minor | yarn | Tamas Domok | Tamas Domok |
| [HDFS-16146](https://issues.apache.org/jira/browse/HDFS-16146) | All three replicas are lost due to not adding a new DataNode in time |  Major | dfsclient | Shuyan Zhang | Shuyan Zhang |
| [YARN-10355](https://issues.apache.org/jira/browse/YARN-10355) | Refactor NM ContainerLaunch.java#orderEnvByDependencies |  Minor | yarn | Benjamin Teke | Tamas Domok |
| [YARN-10849](https://issues.apache.org/jira/browse/YARN-10849) | Clarify testcase documentation for TestServiceAM#testContainersReleasedWhenPreLaunchFails |  Minor | test | Szilard Nemeth | Szilard Nemeth |
| [HDFS-16153](https://issues.apache.org/jira/browse/HDFS-16153) | Avoid evaluation of LOG.debug statement in QuorumJournalManager |  Trivial | journal-node | Zhaohui Wang | Zhaohui Wang |
| [HDFS-16154](https://issues.apache.org/jira/browse/HDFS-16154) | TestMiniJournalCluster failing intermittently because of not reseting UserGroupInformation completely |  Minor | journal-node | Zhaohui Wang | Zhaohui Wang |
| [HADOOP-17837](https://issues.apache.org/jira/browse/HADOOP-17837) | Make it easier to debug UnknownHostExceptions from NetUtils.connect |  Minor | common | Bryan Beaudreault | Bryan Beaudreault |
| [HADOOP-17787](https://issues.apache.org/jira/browse/HADOOP-17787) | Refactor fetching of credentials in Jenkins |  Major | build | Gautham Banasandra | Gautham Banasandra |
| [HDFS-15976](https://issues.apache.org/jira/browse/HDFS-15976) | Make mkdtemp cross platform |  Major | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16163](https://issues.apache.org/jira/browse/HDFS-16163) | Avoid locking entire blockPinningFailures map |  Major | balancer | Viraj Jasani | Viraj Jasani |
| [HADOOP-17825](https://issues.apache.org/jira/browse/HADOOP-17825) | Add BuiltInGzipCompressor |  Major | common | L. C. Hsieh | L. C. Hsieh |
| [HDFS-16162](https://issues.apache.org/jira/browse/HDFS-16162) | Improve DFSUtil#checkProtectedDescendants() related parameter comments |  Major | documentation | JiangHua Zhu | JiangHua Zhu |
| [HDFS-16160](https://issues.apache.org/jira/browse/HDFS-16160) | Improve the parameter annotation in DatanodeProtocol#sendHeartbeat |  Minor | datanode | Tao Li | Tao Li |
| [HDFS-16180](https://issues.apache.org/jira/browse/HDFS-16180) | FsVolumeImpl.nextBlock should consider that the block meta file has been deleted. |  Minor | datanode | Max  Xie | Max  Xie |
| [HDFS-16175](https://issues.apache.org/jira/browse/HDFS-16175) | Improve the configurable value of Server #PURGE\_INTERVAL\_NANOS |  Major | ipc | JiangHua Zhu | JiangHua Zhu |
| [HDFS-16173](https://issues.apache.org/jira/browse/HDFS-16173) | Improve CopyCommands#Put#executor queue configurability |  Major | fs | JiangHua Zhu | JiangHua Zhu |
| [YARN-10891](https://issues.apache.org/jira/browse/YARN-10891) | Extend QueueInfo with max-parallel-apps in CapacityScheduler |  Major | capacity scheduler | Tamas Domok | Tamas Domok |
| [HADOOP-17544](https://issues.apache.org/jira/browse/HADOOP-17544) | Mark KeyProvider as Stable |  Major | security | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15966](https://issues.apache.org/jira/browse/HDFS-15966) | Empty the statistical parameters when emptying the redundant queue |  Minor | hdfs | zhanghuazong | zhanghuazong |
| [HDFS-16202](https://issues.apache.org/jira/browse/HDFS-16202) | Use constants HdfsClientConfigKeys.Failover.PREFIX instead of "dfs.client.failover." |  Minor | hdfs-client | Weison Wei | Weison Wei |
| [HDFS-16138](https://issues.apache.org/jira/browse/HDFS-16138) | BlockReportProcessingThread exit doesn't print the actual stack |  Major | block placement | Renukaprasad C | Renukaprasad C |
| [HDFS-16204](https://issues.apache.org/jira/browse/HDFS-16204) | Improve FSDirEncryptionZoneOp related parameter comments |  Major | documentation | JiangHua Zhu | JiangHua Zhu |
| [HDFS-16209](https://issues.apache.org/jira/browse/HDFS-16209) | Add description for dfs.namenode.caching.enabled |  Major | documentation | Tao Li | Tao Li |
| [HADOOP-17897](https://issues.apache.org/jira/browse/HADOOP-17897) | Allow nested blocks in switch case in checkstyle settings |  Minor | build | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-10693](https://issues.apache.org/jira/browse/YARN-10693) | Add documentation for YARN-10623 auto refresh queue conf in CS |  Major | capacity scheduler | Qi Zhu | Qi Zhu |
| [HADOOP-17857](https://issues.apache.org/jira/browse/HADOOP-17857) | Check real user ACLs in addition to proxied user ACLs |  Major | security | Eric Payne | Eric Payne |
| [HADOOP-17887](https://issues.apache.org/jira/browse/HADOOP-17887) | Remove GzipOutputStream |  Major | common | L. C. Hsieh | L. C. Hsieh |
| [HDFS-16065](https://issues.apache.org/jira/browse/HDFS-16065) | RBF: Add metrics to record Router's operations |  Major | rbf | Janus Chow | Janus Chow |
| [HDFS-16188](https://issues.apache.org/jira/browse/HDFS-16188) | RBF: Router to support resolving monitored namenodes with DNS |  Minor | rbf | Leon Gao | Leon Gao |
| [HDFS-16210](https://issues.apache.org/jira/browse/HDFS-16210) | RBF: Add the option of refreshCallQueue to RouterAdmin |  Major | rbf | Janus Chow | Janus Chow |
| [HDFS-15160](https://issues.apache.org/jira/browse/HDFS-15160) | ReplicaMap, Disk Balancer, Directory Scanner and various FsDatasetImpl methods should use datanode readlock |  Major | datanode | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-16197](https://issues.apache.org/jira/browse/HDFS-16197) | Simplify getting NNStorage in FSNamesystem |  Major | namenode | JiangHua Zhu | JiangHua Zhu |
| [YARN-10928](https://issues.apache.org/jira/browse/YARN-10928) | Support default queue properties of capacity scheduler to simplify configuration management |  Major | capacity scheduler | Weihao Zheng | Weihao Zheng |
| [HDFS-16221](https://issues.apache.org/jira/browse/HDFS-16221) | RBF: Add usage of refreshCallQueue for Router |  Major | rbf | Janus Chow | Janus Chow |
| [HDFS-16223](https://issues.apache.org/jira/browse/HDFS-16223) | AvailableSpaceRackFaultTolerantBlockPlacementPolicy should use chooseRandomWithStorageTypeTwoTrial() for better performance. |  Major | block placement | Ayush Saxena | Ayush Saxena |
| [HADOOP-17900](https://issues.apache.org/jira/browse/HADOOP-17900) | Move ClusterStorageCapacityExceededException to Public from LimitedPrivate |  Major | common, hdfs-client | Ayush Saxena | Ayush Saxena |
| [HDFS-15920](https://issues.apache.org/jira/browse/HDFS-15920) | Solve the problem that the value of SafeModeMonitor#RECHECK\_INTERVAL can be configured |  Major | block placement | JiangHua Zhu | JiangHua Zhu |
| [HDFS-16225](https://issues.apache.org/jira/browse/HDFS-16225) | Fix typo for FederationTestUtils |  Minor | rbf | Tao Li | Tao Li |
| [HADOOP-17913](https://issues.apache.org/jira/browse/HADOOP-17913) | Filter deps with release labels |  Blocker | build | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-17914](https://issues.apache.org/jira/browse/HADOOP-17914) | Print RPC response length in the exception message |  Minor | ipc | Tao Li | Tao Li |
| [HDFS-16229](https://issues.apache.org/jira/browse/HDFS-16229) | Remove the use of obsolete BLOCK\_DELETION\_INCREMENT |  Trivial | documentation, namenode | JiangHua Zhu | JiangHua Zhu |
| [HADOOP-17893](https://issues.apache.org/jira/browse/HADOOP-17893) | Improve PrometheusSink for Namenode TopMetrics |  Major | metrics | Max  Xie | Max  Xie |
| [HADOOP-17926](https://issues.apache.org/jira/browse/HADOOP-17926) | Maven-eclipse-plugin is no longer needed since Eclipse can import Maven projects by itself. |  Minor | documentation | Rintaro Ikeda | Rintaro Ikeda |
| [HDFS-16063](https://issues.apache.org/jira/browse/HDFS-16063) | Add toString to EditLogFileInputStream |  Minor | namanode | David Mollitor | Dionisii Iuzhakov |
| [YARN-10935](https://issues.apache.org/jira/browse/YARN-10935) | AM Total Queue Limit goes below per-user AM Limit if parent is full. |  Major | capacity scheduler, capacityscheduler | Eric Payne | Eric Payne |
| [HDFS-16232](https://issues.apache.org/jira/browse/HDFS-16232) | Fix java doc for BlockReaderRemote#newBlockReader |  Minor | documentation | Tao Li | Tao Li |
| [HADOOP-17939](https://issues.apache.org/jira/browse/HADOOP-17939) | Support building on Apple Silicon |  Major | build, common | Dongjoon Hyun | Dongjoon Hyun |
| [HDFS-16237](https://issues.apache.org/jira/browse/HDFS-16237) | Record the BPServiceActor information that communicates with Standby |  Major | datanode | JiangHua Zhu | JiangHua Zhu |
| [HADOOP-17941](https://issues.apache.org/jira/browse/HADOOP-17941) | Update xerces to 2.12.1 |  Minor | build, common | Zhongwei Zhu | Zhongwei Zhu |
| [HADOOP-17905](https://issues.apache.org/jira/browse/HADOOP-17905) | Modify Text.ensureCapacity() to efficiently max out the backing array size |  Major | io | Peter Bacsko | Peter Bacsko |
| [HDFS-16246](https://issues.apache.org/jira/browse/HDFS-16246) | Print lockWarningThreshold in InstrumentedLock#logWarning and InstrumentedLock#logWaitWarning |  Minor | common | Tao Li | Tao Li |
| [HDFS-16238](https://issues.apache.org/jira/browse/HDFS-16238) | Improve comments related to EncryptionZoneManager |  Minor | documentation, encryption, namenode | JiangHua Zhu | JiangHua Zhu |
| [HDFS-16242](https://issues.apache.org/jira/browse/HDFS-16242) | JournalMetrics should add JournalId  MetricTag to distinguish different nameservice journal metrics. |  Minor | journal-node | Max  Xie | Max  Xie |
| [HDFS-16247](https://issues.apache.org/jira/browse/HDFS-16247) | RBF: Fix the ProcessingAvgTime and ProxyAvgTime code comments and document metrics describe ms unit |  Major | rbf | Haiyang Hu | Haiyang Hu |
| [HDFS-16250](https://issues.apache.org/jira/browse/HDFS-16250) | Refactor AllowSnapshotMock using GMock |  Major | hdfs-client, libhdfs++, tools | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16252](https://issues.apache.org/jira/browse/HDFS-16252) | Correct docs for dfs.http.client.retry.policy.spec |  Major | documentation | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-16251](https://issues.apache.org/jira/browse/HDFS-16251) | Make hdfs\_cat tool cross platform |  Major | hdfs-client, libhdfs++, tools | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16263](https://issues.apache.org/jira/browse/HDFS-16263) | Add CMakeLists for hdfs\_allowSnapshot |  Major | hdfs-client, libhdfs++, tools | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16241](https://issues.apache.org/jira/browse/HDFS-16241) | Standby close reconstruction thread |  Major | namanode | zhanghuazong | zhanghuazong |
| [HDFS-16264](https://issues.apache.org/jira/browse/HDFS-16264) | When adding block keys, the records come from the specific Block Pool |  Minor | datanode | JiangHua Zhu | JiangHua Zhu |
| [HDFS-16260](https://issues.apache.org/jira/browse/HDFS-16260) | Make hdfs\_deleteSnapshot tool cross platform |  Major | hdfs-client, libhdfs++, tools | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16267](https://issues.apache.org/jira/browse/HDFS-16267) | Make hdfs\_df tool cross platform |  Major | hdfs-client, libhdfs++, tools | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16274](https://issues.apache.org/jira/browse/HDFS-16274) | Improve error msg for FSNamesystem#startFileInt |  Minor | namanode | Tao Li | Tao Li |
| [HADOOP-17888](https://issues.apache.org/jira/browse/HADOOP-17888) | The error of Constant  annotation in AzureNativeFileSystemStore.java |  Minor | fs/azure | guoxin | guoxin |
| [HDFS-16277](https://issues.apache.org/jira/browse/HDFS-16277) | Improve decision in AvailableSpaceBlockPlacementPolicy |  Major | block placement | guophilipse | guophilipse |
| [HADOOP-17770](https://issues.apache.org/jira/browse/HADOOP-17770) | WASB : Support disabling buffered reads in positional reads |  Major | fs/azure | Anoop Sam John | Anoop Sam John |
| [HDFS-16282](https://issues.apache.org/jira/browse/HDFS-16282) | Duplicate generic usage information to hdfs debug command |  Minor | tools | daimin | daimin |
| [YARN-1115](https://issues.apache.org/jira/browse/YARN-1115) | Provide optional means for a scheduler to check real user ACLs |  Major | capacity scheduler, scheduler | Eric Payne | Eric Payne |
| [HDFS-16279](https://issues.apache.org/jira/browse/HDFS-16279) | Print detail datanode info when process first storage report |  Minor | datanode | Tao Li | Tao Li |
| [HDFS-16091](https://issues.apache.org/jira/browse/HDFS-16091) | WebHDFS should support getSnapshotDiffReportListing |  Major | webhdfs | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-16290](https://issues.apache.org/jira/browse/HDFS-16290) | Make log more standardized when executing verifyAndSetNamespaceInfo() |  Minor | datanode | JiangHua Zhu | JiangHua Zhu |
| [HDFS-16286](https://issues.apache.org/jira/browse/HDFS-16286) | Debug tool to verify the correctness of erasure coding on file |  Minor | erasure-coding, tools | daimin | daimin |
| [HDFS-16266](https://issues.apache.org/jira/browse/HDFS-16266) | Add remote port information to HDFS audit log |  Major | ipc, namanode | Tao Li | Tao Li |
| [HDFS-16291](https://issues.apache.org/jira/browse/HDFS-16291) | Make the comment of INode#ReclaimContext more standardized |  Minor | documentation, namenode | JiangHua Zhu | JiangHua Zhu |
| [HDFS-16294](https://issues.apache.org/jira/browse/HDFS-16294) | Remove invalid DataNode#CONFIG\_PROPERTY\_SIMULATED |  Major | datanode | JiangHua Zhu | JiangHua Zhu |
| [HDFS-16296](https://issues.apache.org/jira/browse/HDFS-16296) | RBF: RouterRpcFairnessPolicyController add denied permits for each nameservice |  Major | rbf | Janus Chow | Janus Chow |
| [HDFS-16273](https://issues.apache.org/jira/browse/HDFS-16273) | RBF: RouterRpcFairnessPolicyController add availableHandleOnPerNs metrics |  Major | rbf | Xiangyi Zhu | Xiangyi Zhu |
| [HDFS-16302](https://issues.apache.org/jira/browse/HDFS-16302) | RBF: RouterRpcFairnessPolicyController record requests handled by each nameservice |  Major | rbf | Janus Chow | Janus Chow |
| [HDFS-16307](https://issues.apache.org/jira/browse/HDFS-16307) | Improve HdfsBlockPlacementPolicies docs readability |  Minor | documentation | guophilipse | guophilipse |
| [HDFS-16299](https://issues.apache.org/jira/browse/HDFS-16299) | Fix bug for TestDataNodeVolumeMetrics#verifyDataNodeVolumeMetrics |  Minor | datanode, test | Tao Li | Tao Li |
| [HDFS-16301](https://issues.apache.org/jira/browse/HDFS-16301) | Improve BenchmarkThroughput#SIZE naming standardization |  Minor | benchmarks, test | JiangHua Zhu | JiangHua Zhu |
| [HDFS-16305](https://issues.apache.org/jira/browse/HDFS-16305) | Record the remote NameNode address when the rolling log is triggered |  Major | namenode | JiangHua Zhu | JiangHua Zhu |
| [HDFS-16287](https://issues.apache.org/jira/browse/HDFS-16287) | Support to make dfs.namenode.avoid.read.slow.datanode  reconfigurable |  Major | datanode | Haiyang Hu | Haiyang Hu |
| [YARN-10997](https://issues.apache.org/jira/browse/YARN-10997) | Revisit allocation and reservation logging |  Major | capacity scheduler | Andras Gyori | Andras Gyori |
| [HDFS-16321](https://issues.apache.org/jira/browse/HDFS-16321) | Fix invalid config in TestAvailableSpaceRackFaultTolerantBPP |  Minor | test | guophilipse | guophilipse |
| [YARN-11001](https://issues.apache.org/jira/browse/YARN-11001) | Add docs on removing node label mapping from a node |  Minor | documentation | Manu Zhang | Manu Zhang |
| [HDFS-16315](https://issues.apache.org/jira/browse/HDFS-16315) | Add metrics related to Transfer and NativeCopy for DataNode |  Major | datanode, metrics | Tao Li | Tao Li |
| [HDFS-16310](https://issues.apache.org/jira/browse/HDFS-16310) | RBF: Add client port to CallerContext for Router |  Major | rbf | Tao Li | Tao Li |
| [HDFS-16320](https://issues.apache.org/jira/browse/HDFS-16320) | Datanode retrieve slownode information from NameNode |  Major | datanode | Janus Chow | Janus Chow |
| [HADOOP-17998](https://issues.apache.org/jira/browse/HADOOP-17998) | Allow get command to run with multi threads. |  Major | fs | Chengwei Wang | Chengwei Wang |
| [HDFS-16344](https://issues.apache.org/jira/browse/HDFS-16344) | Improve DirectoryScanner.Stats#toString |  Major | . | Tao Li | Tao Li |
| [HADOOP-18023](https://issues.apache.org/jira/browse/HADOOP-18023) | Allow cp command to run with multi threads. |  Major | fs | Chengwei Wang | Chengwei Wang |
| [HADOOP-18029](https://issues.apache.org/jira/browse/HADOOP-18029) | Update CompressionCodecFactory to handle uppercase file extensions |  Minor | common, io, test | Desmond Sisson | Desmond Sisson |
| [HDFS-16358](https://issues.apache.org/jira/browse/HDFS-16358) | HttpFS implementation for getSnapshotDiffReportListing |  Major | httpfs | Viraj Jasani | Viraj Jasani |
| [HDFS-16364](https://issues.apache.org/jira/browse/HDFS-16364) | Remove unnecessary brackets in NameNodeRpcServer#L453 |  Trivial | namanode | Zhaohui Wang | Zhaohui Wang |
| [HDFS-16314](https://issues.apache.org/jira/browse/HDFS-16314) | Support to make dfs.namenode.block-placement-policy.exclude-slow-nodes.enabled reconfigurable |  Major | block placement | Haiyang Hu | Haiyang Hu |
| [HDFS-16338](https://issues.apache.org/jira/browse/HDFS-16338) | Fix error configuration message in FSImage |  Minor | hdfs | guophilipse | guophilipse |
| [HDFS-16351](https://issues.apache.org/jira/browse/HDFS-16351) | Add path exception information in FSNamesystem |  Minor | hdfs | guophilipse | guophilipse |
| [HDFS-16354](https://issues.apache.org/jira/browse/HDFS-16354) | Add description of GETSNAPSHOTDIFFLISTING to WebHDFS doc |  Minor | documentation, webhdfs | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-16345](https://issues.apache.org/jira/browse/HDFS-16345) | Fix test cases fail in TestBlockStoragePolicy |  Major | build | guophilipse | guophilipse |
| [HADOOP-18034](https://issues.apache.org/jira/browse/HADOOP-18034) | Bump mina-core from 2.0.16 to 2.1.5 in /hadoop-project |  Major | build | Ayush Saxena | Ayush Saxena |
| [HADOOP-18001](https://issues.apache.org/jira/browse/HADOOP-18001) | Update to Jetty 9.4.44 |  Major | build, common | Yuan Luo | Yuan Luo |
| [HADOOP-18040](https://issues.apache.org/jira/browse/HADOOP-18040) | Use maven.test.failure.ignore instead of ignoreTestFailure |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17643](https://issues.apache.org/jira/browse/HADOOP-17643) | WASB : Make metadata checks case insensitive |  Major | fs/azure | Anoop Sam John | Anoop Sam John |
| [HADOOP-17982](https://issues.apache.org/jira/browse/HADOOP-17982) | OpensslCipher initialization error should log a WARN message |  Trivial | kms, security | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-18042](https://issues.apache.org/jira/browse/HADOOP-18042) | Fix jetty version in LICENSE-binary |  Major | build, common | Yuan Luo | Yuan Luo |
| [HDFS-16327](https://issues.apache.org/jira/browse/HDFS-16327) | Make dfs.namenode.max.slowpeer.collect.nodes reconfigurable |  Major | namenode | Tao Li | Tao Li |
| [HDFS-16378](https://issues.apache.org/jira/browse/HDFS-16378) | Add datanode address to BlockReportLeaseManager logs |  Minor | datanode | Tao Li | Tao Li |
| [HDFS-16375](https://issues.apache.org/jira/browse/HDFS-16375) | The FBR lease ID should be exposed to the log |  Major | datanode | Tao Li | Tao Li |
| [YARN-11048](https://issues.apache.org/jira/browse/YARN-11048) | Add tests that shows how to delete config values with Mutation API |  Minor | capacity scheduler, restapi | Szilard Nemeth | Szilard Nemeth |
| [HDFS-16352](https://issues.apache.org/jira/browse/HDFS-16352) | return the real datanode numBlocks in #getDatanodeStorageReport |  Major | datanode | qinyuren | qinyuren |
| [YARN-11050](https://issues.apache.org/jira/browse/YARN-11050) | Typo in method name: RMWebServiceProtocol#removeFromCluserNodeLabels |  Trivial | resourcemanager, webservice | Szilard Nemeth | Szilard Nemeth |
| [HDFS-16386](https://issues.apache.org/jira/browse/HDFS-16386) | Reduce DataNode load when FsDatasetAsyncDiskService is working |  Major | datanode | JiangHua Zhu | JiangHua Zhu |
| [HDFS-16391](https://issues.apache.org/jira/browse/HDFS-16391) | Avoid evaluation of LOG.debug statement in NameNodeHeartbeatService |  Trivial | rbf | Zhaohui Wang | Zhaohui Wang |
| [YARN-8234](https://issues.apache.org/jira/browse/YARN-8234) | Improve RM system metrics publisher's performance by pushing events to timeline server in batch |  Critical | resourcemanager, timelineserver | Hu Ziqian | Ashutosh Gupta |
| [HADOOP-18052](https://issues.apache.org/jira/browse/HADOOP-18052) | Support Apple Silicon in start-build-env.sh |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HDFS-16348](https://issues.apache.org/jira/browse/HDFS-16348) | Mark slownode as badnode to recover pipeline |  Major | datanode | Janus Chow | Janus Chow |
| [HADOOP-18060](https://issues.apache.org/jira/browse/HADOOP-18060) | RPCMetrics increases the number of handlers in processing |  Major | rpc-server | JiangHua Zhu | JiangHua Zhu |
| [HDFS-16407](https://issues.apache.org/jira/browse/HDFS-16407) | Make hdfs\_du tool cross platform |  Major | hdfs-client, libhdfs++, tools | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-18056](https://issues.apache.org/jira/browse/HADOOP-18056) | DistCp: Filter duplicates in the source paths |  Major | tools/distcp | Ayush Saxena | Ayush Saxena |
| [HDFS-16404](https://issues.apache.org/jira/browse/HDFS-16404) | Fix typo for CachingGetSpaceUsed |  Minor | fs | Tao Li | Tao Li |
| [HADOOP-18044](https://issues.apache.org/jira/browse/HADOOP-18044) | Hadoop - Upgrade to JQuery 3.6.0 |  Major | build, common | Yuan Luo | Yuan Luo |
| [HDFS-16043](https://issues.apache.org/jira/browse/HDFS-16043) | Add markedDeleteBlockScrubberThread to delete blocks asynchronously |  Major | hdfs, namanode | Xiangyi Zhu | Xiangyi Zhu |
| [HDFS-16426](https://issues.apache.org/jira/browse/HDFS-16426) | fix nextBlockReportTime when trigger full block report force |  Major | datanode | qinyuren | qinyuren |
| [HDFS-16430](https://issues.apache.org/jira/browse/HDFS-16430) | Validate maximum blocks in EC group when adding an EC policy |  Minor | ec, erasure-coding | daimin | daimin |
| [HDFS-16403](https://issues.apache.org/jira/browse/HDFS-16403) | Improve FUSE IO performance by supporting FUSE parameter max\_background |  Minor | fuse-dfs | daimin | daimin |
| [HDFS-16262](https://issues.apache.org/jira/browse/HDFS-16262) | Async refresh of cached locations in DFSInputStream |  Major | hdfs-client | Bryan Beaudreault | Bryan Beaudreault |
| [HDFS-16401](https://issues.apache.org/jira/browse/HDFS-16401) | Remove the worthless DatasetVolumeChecker#numAsyncDatasetChecks |  Major | datanode | JiangHua Zhu | JiangHua Zhu |
| [HADOOP-18093](https://issues.apache.org/jira/browse/HADOOP-18093) | Better exception handling for testFileStatusOnMountLink() in ViewFsBaseTest.java |  Trivial | test | Xing Lin | Xing Lin |
| [HDFS-16423](https://issues.apache.org/jira/browse/HDFS-16423) | balancer should not get blocks on stale storages |  Major | balancer & mover | qinyuren | qinyuren |
| [HDFS-16444](https://issues.apache.org/jira/browse/HDFS-16444) | Show start time of JournalNode on Web |  Major | journal-node | Tao Li | Tao Li |
| [YARN-10459](https://issues.apache.org/jira/browse/YARN-10459) | containerLaunchedOnNode method not need to hold schedulerApptemt lock |  Major | scheduler | Ryan Wu | Minni Mittal |
| [HDFS-16445](https://issues.apache.org/jira/browse/HDFS-16445) | Make HDFS count, mkdir, rm cross platform |  Major | hdfs-client, libhdfs++, tools | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16447](https://issues.apache.org/jira/browse/HDFS-16447) | RBF: Registry HDFS Router's RPCServer & RPCClient metrics for PrometheusSink |  Minor | rbf | Max  Xie | Max  Xie |
| [HADOOP-18110](https://issues.apache.org/jira/browse/HADOOP-18110) | ViewFileSystem: Add Support for Localized Trash Root |  Major | common | Xing Lin | Xing Lin |
| [HDFS-16440](https://issues.apache.org/jira/browse/HDFS-16440) | RBF: Support router get HAServiceStatus with Lifeline RPC address |  Minor | rbf | YulongZ | YulongZ |
| [HADOOP-18117](https://issues.apache.org/jira/browse/HADOOP-18117) | Add an option to preserve root directory permissions |  Minor | tools | Mohanad Elsafty | Mohanad Elsafty |
| [HDFS-16459](https://issues.apache.org/jira/browse/HDFS-16459) | RBF: register RBFMetrics in MetricsSystem for promethuessink |  Minor | rbf | Max  Xie | Max  Xie |
| [HDFS-16461](https://issues.apache.org/jira/browse/HDFS-16461) | Expose JournalNode storage info in the jmx metrics |  Major | journal-node, metrics | Viraj Jasani | Viraj Jasani |
| [YARN-10580](https://issues.apache.org/jira/browse/YARN-10580) | Fix some issues in TestRMWebServicesCapacitySchedDynamicConfig |  Minor | resourcemanager, webservice | Szilard Nemeth | Tamas Domok |
| [HADOOP-18139](https://issues.apache.org/jira/browse/HADOOP-18139) | Allow configuration of zookeeper server principal |  Major | auth | Owen O'Malley | Owen O'Malley |
| [HDFS-16480](https://issues.apache.org/jira/browse/HDFS-16480) | Fix typo: indicies -\> indices |  Minor | block placement | Jiale Qi | Jiale Qi |
| [HADOOP-18128](https://issues.apache.org/jira/browse/HADOOP-18128) | outputstream.md typo issue |  Major | documentation | leo sun | leo sun |
| [YARN-11076](https://issues.apache.org/jira/browse/YARN-11076) | Upgrade jQuery version in Yarn UI2 |  Major | yarn-ui-v2 | Tamas Domok | Tamas Domok |
| [HDFS-16462](https://issues.apache.org/jira/browse/HDFS-16462) | Make HDFS get tool cross platform |  Major | hdfs-client, libhdfs++, tools | Gautham Banasandra | Gautham Banasandra |
| [YARN-11049](https://issues.apache.org/jira/browse/YARN-11049) | MutableConfScheduler is referred as plain String instead of class name |  Minor | resourcemanager | Szilard Nemeth | Szilard Nemeth |
| [HDFS-15382](https://issues.apache.org/jira/browse/HDFS-15382) | Split one FsDatasetImpl lock to volume grain locks. |  Major | datanode | Mingxiang Li | Mingxiang Li |
| [HDFS-16495](https://issues.apache.org/jira/browse/HDFS-16495) | RBF should prepend the client ip rather than append it. |  Major | rbf | Owen O'Malley | Owen O'Malley |
| [HADOOP-18144](https://issues.apache.org/jira/browse/HADOOP-18144) | getTrashRoot/s in ViewFileSystem should return viewFS path, not targetFS path |  Major | common | Xing Lin | Xing Lin |
| [HDFS-16494](https://issues.apache.org/jira/browse/HDFS-16494) | Removed reuse of AvailableSpaceVolumeChoosingPolicy#initLocks() |  Major | datanode | JiangHua Zhu | JiangHua Zhu |
| [HDFS-16470](https://issues.apache.org/jira/browse/HDFS-16470) | Make HDFS find tool cross platform |  Major | hdfs-client, libhdfs++, tools | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16504](https://issues.apache.org/jira/browse/HDFS-16504) | Add parameter for NameNode to process getBloks request |  Minor | balancer & mover, namanode | Max  Xie | Max  Xie |
| [YARN-11086](https://issues.apache.org/jira/browse/YARN-11086) | Add space in debug log of ParentQueue |  Minor | capacity scheduler | Junfan Zhang | Junfan Zhang |
| [HDFS-16471](https://issues.apache.org/jira/browse/HDFS-16471) | Make HDFS ls tool cross platform |  Major | hdfs-client, libhdfs++, tools | Gautham Banasandra | Gautham Banasandra |
| [YARN-10547](https://issues.apache.org/jira/browse/YARN-10547) | Decouple job parsing logic from SLSRunner |  Minor | scheduler-load-simulator | Szilard Nemeth | Szilard Nemeth |
| [YARN-10552](https://issues.apache.org/jira/browse/YARN-10552) | Eliminate code duplication in SLSCapacityScheduler and SLSFairScheduler |  Minor | scheduler-load-simulator | Szilard Nemeth | Szilard Nemeth |
| [YARN-11094](https://issues.apache.org/jira/browse/YARN-11094) | Follow up changes for YARN-10547 |  Minor | scheduler-load-simulator | Szilard Nemeth | Szilard Nemeth |
| [HDFS-16434](https://issues.apache.org/jira/browse/HDFS-16434) | Add opname to read/write lock for remaining operations |  Major | block placement | Tao Li | Tao Li |
| [YARN-10548](https://issues.apache.org/jira/browse/YARN-10548) | Decouple AM runner logic from SLSRunner |  Minor | scheduler-load-simulator | Szilard Nemeth | Szilard Nemeth |
| [YARN-11052](https://issues.apache.org/jira/browse/YARN-11052) | Improve code quality in TestRMWebServicesNodeLabels |  Minor | test | Szilard Nemeth | Szilard Nemeth |
| [YARN-10549](https://issues.apache.org/jira/browse/YARN-10549) | Decouple RM runner logic from SLSRunner |  Minor | scheduler-load-simulator | Szilard Nemeth | Szilard Nemeth |
| [YARN-10550](https://issues.apache.org/jira/browse/YARN-10550) | Decouple NM runner logic from SLSRunner |  Minor | scheduler-load-simulator | Szilard Nemeth | Szilard Nemeth |
| [YARN-11088](https://issues.apache.org/jira/browse/YARN-11088) | Introduce the config to control the AM allocated to non-exclusive nodes |  Major | capacity scheduler | Junfan Zhang | Junfan Zhang |
| [YARN-11103](https://issues.apache.org/jira/browse/YARN-11103) | SLS cleanup after previously merged SLS refactor jiras |  Minor | scheduler-load-simulator | Szilard Nemeth | Szilard Nemeth |
| [HDFS-16472](https://issues.apache.org/jira/browse/HDFS-16472) | Make HDFS setrep tool cross platform |  Major | hdfs-client, libhdfs++, tools | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16527](https://issues.apache.org/jira/browse/HDFS-16527) | Add global timeout rule for TestRouterDistCpProcedure |  Minor | test | Tao Li | Tao Li |
| [HDFS-16529](https://issues.apache.org/jira/browse/HDFS-16529) | Remove unnecessary setObserverRead in TestConsistentReadsObserver |  Trivial | test | Zhaohui Wang | Zhaohui Wang |
| [HDFS-16530](https://issues.apache.org/jira/browse/HDFS-16530) | setReplication debug log creates a new string even if debug is disabled |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [HADOOP-18188](https://issues.apache.org/jira/browse/HADOOP-18188) | Support touch command for directory |  Major | common | Akira Ajisaka | Viraj Jasani |
| [HDFS-16457](https://issues.apache.org/jira/browse/HDFS-16457) | Make fs.getspaceused.classname reconfigurable |  Major | namenode | yanbin.zhang | yanbin.zhang |
| [HDFS-16427](https://issues.apache.org/jira/browse/HDFS-16427) | Add debug log for BlockManager#chooseExcessRedundancyStriped |  Minor | erasure-coding | Tao Li | Tao Li |
| [HDFS-16497](https://issues.apache.org/jira/browse/HDFS-16497) | EC: Add param comment for liveBusyBlockIndices with HDFS-14768 |  Minor | erasure-coding, namanode | caozhiqiang | caozhiqiang |
| [HDFS-16473](https://issues.apache.org/jira/browse/HDFS-16473) | Make HDFS stat tool cross platform |  Major | hdfs-client, libhdfs++, tools | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16516](https://issues.apache.org/jira/browse/HDFS-16516) | fix filesystemshell wrong params |  Minor | documentation | guophilipse | guophilipse |
| [HDFS-16474](https://issues.apache.org/jira/browse/HDFS-16474) | Make HDFS tail tool cross platform |  Major | hdfs-client, libhdfs++, tools | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16389](https://issues.apache.org/jira/browse/HDFS-16389) | Improve NNThroughputBenchmark test mkdirs |  Major | benchmarks, namenode | JiangHua Zhu | JiangHua Zhu |
| [MAPREDUCE-7373](https://issues.apache.org/jira/browse/MAPREDUCE-7373) | Building MapReduce NativeTask fails on Fedora 34+ |  Major | build, nativetask | Kengo Seki | Kengo Seki |
| [HDFS-16355](https://issues.apache.org/jira/browse/HDFS-16355) | Improve the description of dfs.block.scanner.volume.bytes.per.second |  Minor | documentation, hdfs | guophilipse | guophilipse |
| [HADOOP-18155](https://issues.apache.org/jira/browse/HADOOP-18155) | Refactor tests in TestFileUtil |  Trivial | common | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-18088](https://issues.apache.org/jira/browse/HADOOP-18088) | Replace log4j 1.x with reload4j |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-16501](https://issues.apache.org/jira/browse/HDFS-16501) | Print the exception when reporting a bad block |  Major | datanode | qinyuren | qinyuren |
| [HDFS-16500](https://issues.apache.org/jira/browse/HDFS-16500) | Make asynchronous blocks deletion lock and unlock durtion threshold configurable |  Major | namanode | Chengwei Wang | Chengwei Wang |
| [HADOOP-17551](https://issues.apache.org/jira/browse/HADOOP-17551) | Upgrade maven-site-plugin to 3.11.0 |  Major | build, common | Akira Ajisaka | Ashutosh Gupta |
| [HADOOP-18214](https://issues.apache.org/jira/browse/HADOOP-18214) | Update BUILDING.txt |  Minor | build, documentation | Steve Loughran | Steve Loughran |
| [HDFS-16519](https://issues.apache.org/jira/browse/HDFS-16519) | Add throttler to EC reconstruction |  Minor | datanode, ec | daimin | daimin |
| [HADOOP-16202](https://issues.apache.org/jira/browse/HADOOP-16202) | Enhance openFile() for better read performance against object stores |  Major | fs, fs/s3, tools/distcp | Steve Loughran | Steve Loughran |
| [HDFS-16554](https://issues.apache.org/jira/browse/HDFS-16554) | Remove unused configuration dfs.namenode.block.deletion.increment. |  Major | namenode | Chengwei Wang | Chengwei Wang |
| [HDFS-16539](https://issues.apache.org/jira/browse/HDFS-16539) | RBF: Support refreshing/changing router fairness policy controller without rebooting router |  Minor | rbf | Felix N | Felix N |
| [HDFS-16553](https://issues.apache.org/jira/browse/HDFS-16553) | Fix checkstyle for the length of BlockManager construction method over limit. |  Major | namenode | Chengwei Wang | Chengwei Wang |
| [YARN-11116](https://issues.apache.org/jira/browse/YARN-11116) | Migrate Times util from SimpleDateFormat to thread-safe DateTimeFormatter class |  Minor | utils, yarn-common | Jonathan Turner Eagles | Jonathan Turner Eagles |
| [HDFS-16562](https://issues.apache.org/jira/browse/HDFS-16562) | Upgrade moment.min.js to 2.29.2 |  Major | build, common | D M Murali Krishna Reddy | D M Murali Krishna Reddy |
| [HDFS-16468](https://issues.apache.org/jira/browse/HDFS-16468) | Define ssize\_t for Windows |  Major | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16520](https://issues.apache.org/jira/browse/HDFS-16520) | Improve EC pread: avoid potential reading whole block |  Major | dfsclient, ec, erasure-coding | daimin | daimin |
| [MAPREDUCE-7379](https://issues.apache.org/jira/browse/MAPREDUCE-7379) | RMContainerRequestor#makeRemoteRequest has confusing log message |  Trivial | mrv2 | Szilard Nemeth | Ashutosh Gupta |
| [HADOOP-18167](https://issues.apache.org/jira/browse/HADOOP-18167) | Add metrics to track delegation token secret manager operations |  Major | metrics, security | Hector Sandoval Chaverri | Hector Sandoval Chaverri |
| [YARN-11114](https://issues.apache.org/jira/browse/YARN-11114) | RMWebServices returns only apps matching exactly the submitted queue name |  Major | capacity scheduler, webapp | Szilard Nemeth | Szilard Nemeth |
| [HADOOP-18193](https://issues.apache.org/jira/browse/HADOOP-18193) | Support nested mount points in INodeTree |  Major | viewfs | Lei Yang | Lei Yang |
| [HDFS-16465](https://issues.apache.org/jira/browse/HDFS-16465) | Remove redundant strings.h inclusions |  Major | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [YARN-10080](https://issues.apache.org/jira/browse/YARN-10080) | Support show app id on localizer thread pool |  Major | nodemanager | zhoukang | Ashutosh Gupta |
| [HDFS-16584](https://issues.apache.org/jira/browse/HDFS-16584) | Record StandbyNameNode information when Balancer is running |  Major | balancer & mover, namenode | JiangHua Zhu | JiangHua Zhu |
| [HADOOP-18172](https://issues.apache.org/jira/browse/HADOOP-18172) | Change scope of getRootFallbackLink for InodeTree to make them accessible from outside package |  Minor | fs | Xing Lin | Xing Lin |
| [HADOOP-18249](https://issues.apache.org/jira/browse/HADOOP-18249) | Fix getUri() in HttpRequest has been deprecated |  Major | common | Shilun Fan | Shilun Fan |
| [HADOOP-18240](https://issues.apache.org/jira/browse/HADOOP-18240) | Upgrade Yetus to 0.14.0 |  Major | build | Akira Ajisaka | Ashutosh Gupta |
| [HDFS-16585](https://issues.apache.org/jira/browse/HDFS-16585) | Add @VisibleForTesting in Dispatcher.java after HDFS-16268 |  Trivial | balancer | Wei-Chiu Chuang | Ashutosh Gupta |
| [HDFS-16599](https://issues.apache.org/jira/browse/HDFS-16599) | Fix typo in hadoop-hdfs-rbf module |  Minor | rbf | Shilun Fan | Shilun Fan |
| [YARN-11142](https://issues.apache.org/jira/browse/YARN-11142) | Remove unused Imports in Hadoop YARN project |  Minor | yarn | Ashutosh Gupta | Ashutosh Gupta |
| [HDFS-16603](https://issues.apache.org/jira/browse/HDFS-16603) | Improve DatanodeHttpServer With Netty recommended method |  Minor | datanode | Shilun Fan | Shilun Fan |
| [HDFS-16610](https://issues.apache.org/jira/browse/HDFS-16610) | Make fsck read timeout configurable |  Major | hdfs-client | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-16576](https://issues.apache.org/jira/browse/HDFS-16576) | Remove unused imports in HDFS project |  Minor | hdfs | Ashutosh Gupta | Ashutosh Gupta |
| [HDFS-16621](https://issues.apache.org/jira/browse/HDFS-16621) | Remove unused JNStorage#getCurrentDir() |  Minor | journal-node, qjm | JiangHua Zhu | JiangHua Zhu |
| [HDFS-16463](https://issues.apache.org/jira/browse/HDFS-16463) | Make dirent cross platform compatible |  Major | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16627](https://issues.apache.org/jira/browse/HDFS-16627) | Improve BPServiceActor#register log to add NameNode address |  Minor | hdfs | Shilun Fan | Shilun Fan |
| [HDFS-16609](https://issues.apache.org/jira/browse/HDFS-16609) | Fix Flakes Junit Tests that often report timeouts |  Major | test | Shilun Fan | Shilun Fan |
| [YARN-11175](https://issues.apache.org/jira/browse/YARN-11175) | Refactor LogAggregationFileControllerFactory |  Minor | log-aggregation | Szilard Nemeth | Szilard Nemeth |
| [YARN-11176](https://issues.apache.org/jira/browse/YARN-11176) | Refactor TestAggregatedLogDeletionService |  Minor | log-aggregation | Szilard Nemeth | Szilard Nemeth |
| [HDFS-16469](https://issues.apache.org/jira/browse/HDFS-16469) | Locate protoc-gen-hrpc across platforms |  Major | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16613](https://issues.apache.org/jira/browse/HDFS-16613) | EC: Improve performance of decommissioning dn with many ec blocks |  Major | ec, erasure-coding, namenode | caozhiqiang | caozhiqiang |
| [HDFS-16629](https://issues.apache.org/jira/browse/HDFS-16629) | [JDK 11] Fix javadoc  warnings in hadoop-hdfs module |  Minor | hdfs | Shilun Fan | Shilun Fan |
| [YARN-11172](https://issues.apache.org/jira/browse/YARN-11172) | Fix testDelegationToken |  Major | test | Chenyu Zheng | Chenyu Zheng |
| [YARN-11182](https://issues.apache.org/jira/browse/YARN-11182) | Refactor TestAggregatedLogDeletionService: 2nd phase |  Minor | log-aggregation | Szilard Nemeth | Szilard Nemeth |
| [HADOOP-18271](https://issues.apache.org/jira/browse/HADOOP-18271) | Remove unused Imports in Hadoop Common project |  Minor | common | Ashutosh Gupta | Ashutosh Gupta |
| [HADOOP-18288](https://issues.apache.org/jira/browse/HADOOP-18288) | Total requests and total requests per sec served by RPC servers |  Major | rpc-server | Viraj Jasani | Viraj Jasani |
| [HADOOP-18314](https://issues.apache.org/jira/browse/HADOOP-18314) | Add some description for PowerShellFencer |  Major | ha | JiangHua Zhu | JiangHua Zhu |
| [HADOOP-18284](https://issues.apache.org/jira/browse/HADOOP-18284) | Remove Unnecessary semicolon ';' |  Minor | common | Shilun Fan | Shilun Fan |
| [YARN-11202](https://issues.apache.org/jira/browse/YARN-11202) | Optimize ClientRMService.getApplications |  Major | yarn | Tamas Domok | Tamas Domok |
| [HDFS-16647](https://issues.apache.org/jira/browse/HDFS-16647) | Delete unused NameNode#FS\_HDFS\_IMPL\_KEY |  Minor | namenode | JiangHua Zhu | JiangHua Zhu |
| [HDFS-16638](https://issues.apache.org/jira/browse/HDFS-16638) | Add isDebugEnabled check for debug blockLogs in BlockManager |  Trivial | namenode | dzcxzl | dzcxzl |
| [HADOOP-18297](https://issues.apache.org/jira/browse/HADOOP-18297) | Upgrade dependency-check-maven to 7.1.1 |  Minor | security | Ashutosh Gupta | Ashutosh Gupta |
| [HDFS-16466](https://issues.apache.org/jira/browse/HDFS-16466) | Implement Linux permission flags on Windows |  Major | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [MAPREDUCE-7201](https://issues.apache.org/jira/browse/MAPREDUCE-7201) | Make Job History File Permissions configurable |  Major | jobhistoryserver | Prabhu Joseph | Ashutosh Gupta |
| [HADOOP-18294](https://issues.apache.org/jira/browse/HADOOP-18294) | Ensure build folder exists before writing checksum file.ProtocRunner#writeChecksums |  Minor | common | Ashutosh Gupta | Ashutosh Gupta |
| [HADOOP-18336](https://issues.apache.org/jira/browse/HADOOP-18336) | tag FSDataInputStream.getWrappedStream() @Public/@Stable |  Minor | fs | Steve Loughran | Ashutosh Gupta |
| [HADOOP-13144](https://issues.apache.org/jira/browse/HADOOP-13144) | Enhancing IPC client throughput via multiple connections per user |  Minor | ipc | Jason Kace | Íñigo Goiri |
| [HADOOP-18332](https://issues.apache.org/jira/browse/HADOOP-18332) | Remove rs-api dependency by downgrading jackson to 2.12.7 |  Major | build | PJ Fanning | PJ Fanning |
| [HDFS-16666](https://issues.apache.org/jira/browse/HDFS-16666) | Pass CMake args for Windows in pom.xml |  Major | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16464](https://issues.apache.org/jira/browse/HDFS-16464) | Create only libhdfspp static libraries for Windows |  Major | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16640](https://issues.apache.org/jira/browse/HDFS-16640) | RBF: Show datanode IP list when click DN histogram in Router |  Minor | rbf | Zhaohui Wang | Zhaohui Wang |
| [HDFS-16605](https://issues.apache.org/jira/browse/HDFS-16605) | Improve Code With Lambda in hadoop-hdfs-rbf module |  Minor | rbf | Shilun Fan | Shilun Fan |
| [HDFS-16467](https://issues.apache.org/jira/browse/HDFS-16467) | Ensure Protobuf generated headers are included first |  Major | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16655](https://issues.apache.org/jira/browse/HDFS-16655) | OIV: print out erasure coding policy name in oiv Delimited output |  Minor | erasure-coding | Max  Xie | Max  Xie |
| [HDFS-16660](https://issues.apache.org/jira/browse/HDFS-16660) | Improve Code With Lambda in IPCLoggerChannel class |  Minor | journal-node | ZanderXu | ZanderXu |
| [HDFS-16619](https://issues.apache.org/jira/browse/HDFS-16619) | Fix HttpHeaders.Values And HttpHeaders.Names Deprecated Import. |  Major | hdfs | Shilun Fan | Shilun Fan |
| [HDFS-16658](https://issues.apache.org/jira/browse/HDFS-16658) | BlockManager should output some logs when logEveryBlock is true. |  Minor | block placement | ZanderXu | ZanderXu |
| [HDFS-16671](https://issues.apache.org/jira/browse/HDFS-16671) | RBF: RouterRpcFairnessPolicyController supports configurable permit acquire timeout |  Major | rbf | ZanderXu | ZanderXu |
| [YARN-11063](https://issues.apache.org/jira/browse/YARN-11063) | Support auto queue creation template wildcards for arbitrary queue depths |  Major | capacity scheduler | Andras Gyori | Bence Kosztolnik |
| [HADOOP-18358](https://issues.apache.org/jira/browse/HADOOP-18358) | Update commons-math3 from 3.1.1 to 3.6.1. |  Minor | build, common | Shilun Fan | Shilun Fan |
| [HADOOP-18301](https://issues.apache.org/jira/browse/HADOOP-18301) | Upgrade commons-io to 2.11.0 |  Minor | build, common | Ashutosh Gupta | Ashutosh Gupta |
| [HDFS-16712](https://issues.apache.org/jira/browse/HDFS-16712) | Fix incorrect placeholder in DataNode.java |  Major | datanode | ZanderXu | ZanderXu |
| [YARN-11029](https://issues.apache.org/jira/browse/YARN-11029) | Refactor AMRMProxy Service code and Added Some Metrics |  Major | amrmproxy | Minni Mittal | Shilun Fan |
| [HDFS-16642](https://issues.apache.org/jira/browse/HDFS-16642) | [SBN read] Moving selecting inputstream from JN in EditlogTailer out of FSNLock |  Major | ha, namanode | ZanderXu | ZanderXu |
| [HDFS-16648](https://issues.apache.org/jira/browse/HDFS-16648) | Normalize the usage of debug logs in NameNode |  Minor | namanode | ZanderXu | ZanderXu |
| [HDFS-16709](https://issues.apache.org/jira/browse/HDFS-16709) | Remove redundant cast in FSEditLogOp.class |  Major | namanode | ZanderXu | ZanderXu |
| [MAPREDUCE-7385](https://issues.apache.org/jira/browse/MAPREDUCE-7385) | impove JobEndNotifier#httpNotification With recommended methods |  Minor | mrv1 | Shilun Fan | Shilun Fan |
| [HDFS-16702](https://issues.apache.org/jira/browse/HDFS-16702) | MiniDFSCluster should report cause of exception in assertion error |  Minor | hdfs | Steve Vaughan | Steve Vaughan |
| [HDFS-16723](https://issues.apache.org/jira/browse/HDFS-16723) | Replace incorrect SafeModeException with StandbyException in RouterRpcServer.class |  Major | rbf | ZanderXu | ZanderXu |
| [HDFS-16678](https://issues.apache.org/jira/browse/HDFS-16678) | RBF supports disable getNodeUsage() in RBFMetrics |  Major | rbf | ZanderXu | ZanderXu |
| [HDFS-16704](https://issues.apache.org/jira/browse/HDFS-16704) | Datanode return empty response instead of NPE for GetVolumeInfo during restarting |  Major | datanode | ZanderXu | ZanderXu |
| [YARN-10885](https://issues.apache.org/jira/browse/YARN-10885) | Make FederationStateStoreFacade#getApplicationHomeSubCluster use JCache |  Major | federation | chaosju | Shilun Fan |
| [HDFS-16705](https://issues.apache.org/jira/browse/HDFS-16705) | RBF: Support healthMonitor timeout configurable and cache NN and client proxy in NamenodeHeartbeatService |  Major | rbf | ZanderXu | ZanderXu |
| [HADOOP-18365](https://issues.apache.org/jira/browse/HADOOP-18365) | Updated addresses are still accessed using the old IP address |  Major | common | Steve Vaughan | Steve Vaughan |
| [HDFS-16717](https://issues.apache.org/jira/browse/HDFS-16717) | Replace NPE with IOException in DataNode.class |  Major | datanode | ZanderXu | ZanderXu |
| [HDFS-16687](https://issues.apache.org/jira/browse/HDFS-16687) | RouterFsckServlet replicates code from DfsServlet base class |  Major | federation | Steve Vaughan | Steve Vaughan |
| [HADOOP-18333](https://issues.apache.org/jira/browse/HADOOP-18333) | hadoop-client-runtime impact by CVE-2022-2047 CVE-2022-2048 due to shaded jetty |  Major | build | phoebe chen | Ashutosh Gupta |
| [HADOOP-18361](https://issues.apache.org/jira/browse/HADOOP-18361) | Update commons-net from 3.6 to 3.8.0. |  Minor | common | Shilun Fan | Shilun Fan |
| [HADOOP-18406](https://issues.apache.org/jira/browse/HADOOP-18406) | Adds alignment context to call path for creating RPC proxy with multiple connections per user. |  Major | ipc | Simbarashe Dzinamarira | Simbarashe Dzinamarira |
| [YARN-11253](https://issues.apache.org/jira/browse/YARN-11253) | Add Configuration to delegationToken RemoverScanInterval |  Major | resourcemanager | Shilun Fan | Shilun Fan |
| [HDFS-16684](https://issues.apache.org/jira/browse/HDFS-16684) | Exclude self from JournalNodeSyncer when using a bind host |  Major | journal-node | Steve Vaughan | Steve Vaughan |
| [HDFS-16735](https://issues.apache.org/jira/browse/HDFS-16735) | Reduce the number of HeartbeatManager loops |  Major | datanode, namanode | Shuyan Zhang | Shuyan Zhang |
| [YARN-11196](https://issues.apache.org/jira/browse/YARN-11196) | NUMA Awareness support in DefaultContainerExecutor |  Major | nodemanager | Prabhu Joseph | Samrat Deb |
| [MAPREDUCE-7409](https://issues.apache.org/jira/browse/MAPREDUCE-7409) | Make shuffle key length configurable |  Major | mrv2 | András Győri | Ashutosh Gupta |
| [HADOOP-18441](https://issues.apache.org/jira/browse/HADOOP-18441) | Remove org.apache.hadoop.maven.plugin.shade.resource.ServicesResourceTransformer |  Major | common | PJ Fanning | PJ Fanning |
| [HADOOP-18388](https://issues.apache.org/jira/browse/HADOOP-18388) | Allow dynamic groupSearchFilter in LdapGroupsMapping |  Major | security | Ayush Saxena | Ayush Saxena |
| [HADOOP-18427](https://issues.apache.org/jira/browse/HADOOP-18427) | Improve ZKDelegationTokenSecretManager#startThead With recommended methods. |  Minor | common | Shilun Fan | Shilun Fan |
| [YARN-11278](https://issues.apache.org/jira/browse/YARN-11278) | Ambiguous error message in mutation API |  Major | capacity scheduler | András Győri | Ashutosh Gupta |
| [YARN-11274](https://issues.apache.org/jira/browse/YARN-11274) | Improve Nodemanager#NodeStatusUpdaterImpl Log |  Minor | nodemanager | Shilun Fan | Shilun Fan |
| [YARN-11286](https://issues.apache.org/jira/browse/YARN-11286) | Make AsyncDispatcher#printEventDetailsExecutor thread pool parameter configurable |  Minor | resourcemanager | Shilun Fan | Shilun Fan |
| [HDFS-16663](https://issues.apache.org/jira/browse/HDFS-16663) | EC: Allow block reconstruction pending timeout refreshable to increase decommission performance |  Major | ec, namenode | caozhiqiang | caozhiqiang |
| [HDFS-16770](https://issues.apache.org/jira/browse/HDFS-16770) | [Documentation] RBF: Duplicate statement to be removed for better readabilty |  Minor | documentation, rbf | Renukaprasad C | Renukaprasad C |
| [HDFS-16686](https://issues.apache.org/jira/browse/HDFS-16686) | GetJournalEditServlet fails to authorize valid Kerberos request |  Major | journal-node | Steve Vaughan | Steve Vaughan |
| [HADOOP-15072](https://issues.apache.org/jira/browse/HADOOP-15072) | Upgrade Apache Kerby version to 2.0.x |  Major | security | Jiajia Li | Colm O hEigeartaigh |
| [HADOOP-18118](https://issues.apache.org/jira/browse/HADOOP-18118) | Fix KMS Accept Queue Size default value to 500 |  Minor | common | guophilipse | guophilipse |
| [MAPREDUCE-7407](https://issues.apache.org/jira/browse/MAPREDUCE-7407) | Avoid stopContainer() on dead node |  Major | mrv2 | Ashutosh Gupta | Ashutosh Gupta |
| [HADOOP-18446](https://issues.apache.org/jira/browse/HADOOP-18446) | Add a re-queue metric to RpcMetrics.java to quantify the number of re-queue RPCs |  Minor | metrics | ZanderXu | ZanderXu |
| [YARN-11303](https://issues.apache.org/jira/browse/YARN-11303) | Upgrade jquery ui to 1.13.2 |  Major | security | D M Murali Krishna Reddy | Ashutosh Gupta |
| [HDFS-16341](https://issues.apache.org/jira/browse/HDFS-16341) | Fix BlockPlacementPolicy details in hdfs defaults |  Minor | documentation | guophilipse | guophilipse |
| [HADOOP-18451](https://issues.apache.org/jira/browse/HADOOP-18451) | Update hsqldb.version from 2.3.4 to 2.5.2 |  Major | common | Shilun Fan | Shilun Fan |
| [HADOOP-16769](https://issues.apache.org/jira/browse/HADOOP-16769) | LocalDirAllocator to provide diagnostics when file creation fails |  Minor | util | Ramesh Kumar Thangarajan | Ashutosh Gupta |
| [HADOOP-18341](https://issues.apache.org/jira/browse/HADOOP-18341) | upgrade commons-configuration2 to 2.8.0 and commons-text to 1.9 |  Major | common | PJ Fanning | PJ Fanning |
| [HDFS-16776](https://issues.apache.org/jira/browse/HDFS-16776) | Erasure Coding: The length of targets should be checked when DN gets a reconstruction task |  Major | erasure-coding | Ruinan Gu | Ruinan Gu |
| [HDFS-16771](https://issues.apache.org/jira/browse/HDFS-16771) | JN should tersely print logs about NewerTxnIdException |  Major | journal-node | ZanderXu | ZanderXu |
| [HADOOP-18466](https://issues.apache.org/jira/browse/HADOOP-18466) | Limit the findbugs suppression IS2\_INCONSISTENT\_SYNC to S3AFileSystem field |  Minor | fs/s3 | Viraj Jasani | Viraj Jasani |
| [YARN-11306](https://issues.apache.org/jira/browse/YARN-11306) | Refactor NM#FederationInterceptor#recover Code |  Major | federation, nodemanager | Shilun Fan | Shilun Fan |
| [YARN-11290](https://issues.apache.org/jira/browse/YARN-11290) | Improve Query Condition of FederationStateStore#getApplicationsHomeSubCluster |  Minor | federation | Shilun Fan | Shilun Fan |
| [YARN-11240](https://issues.apache.org/jira/browse/YARN-11240) | Fix incorrect placeholder in yarn-module |  Minor | yarn | Shilun Fan | Shilun Fan |
| [YARN-6169](https://issues.apache.org/jira/browse/YARN-6169) | container-executor message on empty configuration file can be improved |  Trivial | container-executor | Miklos Szegedi | Riya Khandelwal |
| [YARN-11187](https://issues.apache.org/jira/browse/YARN-11187) | Remove WhiteBox in yarn module. |  Minor | test | Shilun Fan | Shilun Fan |
| [MAPREDUCE-7370](https://issues.apache.org/jira/browse/MAPREDUCE-7370) | Parallelize MultipleOutputs#close call |  Major | mapreduce-client | Prabhu Joseph | Ashutosh Gupta |
| [HADOOP-18469](https://issues.apache.org/jira/browse/HADOOP-18469) | Add XMLUtils methods to centralise code that creates secure XML parsers |  Major | common | PJ Fanning | PJ Fanning |
| [HADOOP-18442](https://issues.apache.org/jira/browse/HADOOP-18442) | Remove the hadoop-openstack module |  Major | build, fs, fs/swift | Steve Loughran | Steve Loughran |
| [HADOOP-18468](https://issues.apache.org/jira/browse/HADOOP-18468) | upgrade jettison json jar due to fix CVE-2022-40149 |  Major | build | PJ Fanning | PJ Fanning |
| [YARN-6766](https://issues.apache.org/jira/browse/YARN-6766) | Add helper method in FairSchedulerAppsBlock to print app info |  Minor | webapp | Daniel Templeton | Riya Khandelwal |
| [HDFS-16774](https://issues.apache.org/jira/browse/HDFS-16774) | Improve async delete replica on datanode |  Major | datanode | Haiyang Hu | Haiyang Hu |
| [HADOOP-17779](https://issues.apache.org/jira/browse/HADOOP-17779) | Lock File System Creator Semaphore Uninterruptibly |  Minor | fs | David Mollitor | David Mollitor |
| [HADOOP-18483](https://issues.apache.org/jira/browse/HADOOP-18483) | Exclude Dockerfile\_windows\_10 from hadolint |  Major | common | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-18133](https://issues.apache.org/jira/browse/HADOOP-18133) | Add Dockerfile for Windows 10 |  Critical | build | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-18360](https://issues.apache.org/jira/browse/HADOOP-18360) | Update commons-csv from 1.0 to 1.9.0. |  Minor | common | Shilun Fan | Shilun Fan |
| [HADOOP-18493](https://issues.apache.org/jira/browse/HADOOP-18493) | update jackson-databind 2.12.7.1 due to CVE fixes |  Major | common | PJ Fanning | PJ Fanning |
| [HADOOP-18462](https://issues.apache.org/jira/browse/HADOOP-18462) | InstrumentedWriteLock should consider Reentrant case |  Major | common | ZanderXu | ZanderXu |
| [HDFS-6874](https://issues.apache.org/jira/browse/HDFS-6874) | Add GETFILEBLOCKLOCATIONS operation to HttpFS |  Major | httpfs | Gao Zhong Liang | Ashutosh Gupta |
| [HADOOP-17563](https://issues.apache.org/jira/browse/HADOOP-17563) | Update Bouncy Castle to 1.68 or later |  Major | build | Takanobu Asanuma | PJ Fanning |
| [HADOOP-18497](https://issues.apache.org/jira/browse/HADOOP-18497) | Upgrade commons-text version to fix CVE-2022-42889 |  Major | build | Xiaoqiao He | PJ Fanning |
| [YARN-11328](https://issues.apache.org/jira/browse/YARN-11328) | Refactoring part of the code of SQLFederationStateStore |  Major | federation, router | Shilun Fan | Shilun Fan |
| [HDFS-16803](https://issues.apache.org/jira/browse/HDFS-16803) | Improve some annotations in hdfs module |  Major | documentation, namenode | JiangHua Zhu | JiangHua Zhu |
| [HDFS-16795](https://issues.apache.org/jira/browse/HDFS-16795) | Use secure XML parser utils in hdfs classes |  Major | hdfs | PJ Fanning | PJ Fanning |
| [HADOOP-18500](https://issues.apache.org/jira/browse/HADOOP-18500) | Upgrade maven-shade-plugin to 3.3.0 |  Minor | build | Willi Raschkowski | Willi Raschkowski |
| [HADOOP-18506](https://issues.apache.org/jira/browse/HADOOP-18506) | Update build instructions for Windows using VS2019 |  Major | build, documentation | Gautham Banasandra | Gautham Banasandra |
| [YARN-11330](https://issues.apache.org/jira/browse/YARN-11330) | Use secure XML parser utils in YARN |  Major | yarn | PJ Fanning | PJ Fanning |
| [MAPREDUCE-7411](https://issues.apache.org/jira/browse/MAPREDUCE-7411) | Use secure XML parser utils in MapReduce |  Major | mrv1, mrv2 | PJ Fanning | PJ Fanning |
| [YARN-11356](https://issues.apache.org/jira/browse/YARN-11356) | Upgrade DataTables to 1.11.5 to fix CVEs |  Major | yarn | Bence Kosztolnik | Bence Kosztolnik |
| [HDFS-16817](https://issues.apache.org/jira/browse/HDFS-16817) | Remove useless DataNode lock related configuration |  Major | datanode | Haiyang Hu | Haiyang Hu |
| [HDFS-16802](https://issues.apache.org/jira/browse/HDFS-16802) | Print options when accessing ClientProtocol#rename2() |  Minor | namenode | JiangHua Zhu | JiangHua Zhu |
| [YARN-11360](https://issues.apache.org/jira/browse/YARN-11360) | Add number of decommissioning/shutdown nodes to YARN cluster metrics. |  Major | client, resourcemanager | Chris Nauroth | Chris Nauroth |
| [HADOOP-18472](https://issues.apache.org/jira/browse/HADOOP-18472) | Upgrade to snakeyaml 1.33 |  Major | common | PJ Fanning | PJ Fanning |
| [HADOOP-18512](https://issues.apache.org/jira/browse/HADOOP-18512) | upgrade woodstox-core to 5.4.0 for security fix |  Major | common | phoebe chen | PJ Fanning |
| [YARN-11363](https://issues.apache.org/jira/browse/YARN-11363) | Remove unused TimelineVersionWatcher and TimelineVersion from hadoop-yarn-server-tests |  Major | test, yarn | Ashutosh Gupta | Ashutosh Gupta |
| [YARN-11364](https://issues.apache.org/jira/browse/YARN-11364) | Docker Container to accept docker Image name with sha256 digest |  Major | yarn | Ashutosh Gupta | Ashutosh Gupta |
| [HDFS-16811](https://issues.apache.org/jira/browse/HDFS-16811) | Support DecommissionBackoffMonitor parameters reconfigurable |  Major | datanode, namanode | Haiyang Hu | Haiyang Hu |
| [HADOOP-18517](https://issues.apache.org/jira/browse/HADOOP-18517) | ABFS: Add fs.azure.enable.readahead option to disable readahead |  Major | fs/azure | Steve Loughran | Steve Loughran |
| [HADOOP-18502](https://issues.apache.org/jira/browse/HADOOP-18502) | Hadoop metrics should return 0 when there is no change |  Major | metrics | leo sun | leo sun |
| [HADOOP-18433](https://issues.apache.org/jira/browse/HADOOP-18433) | Fix main thread name. |  Major | common, ipc | Chenyu Zheng | Chenyu Zheng |
| [YARN-10005](https://issues.apache.org/jira/browse/YARN-10005) | Code improvements in MutableCSConfigurationProvider |  Minor | capacity scheduler | Szilard Nemeth | Peter Szucs |
| [MAPREDUCE-7390](https://issues.apache.org/jira/browse/MAPREDUCE-7390) | Remove WhiteBox in mapreduce module. |  Minor | mrv2 | Shilun Fan | Shilun Fan |
| [MAPREDUCE-5608](https://issues.apache.org/jira/browse/MAPREDUCE-5608) | Replace and deprecate mapred.tasktracker.indexcache.mb |  Major | mapreduce-client | Sandy Ryza | Ashutosh Gupta |
| [HADOOP-18484](https://issues.apache.org/jira/browse/HADOOP-18484) | upgrade hsqldb to v2.7.1 due to CVE |  Major | common | PJ Fanning | Ashutosh Gupta |
| [YARN-11369](https://issues.apache.org/jira/browse/YARN-11369) | Commons.compress throws an IllegalArgumentException with large uids after 1.21 |  Major | client | Benjamin Teke | Benjamin Teke |
| [HDFS-16844](https://issues.apache.org/jira/browse/HDFS-16844) | [RBF] The routers should be resiliant against exceptions from StateStore |  Major | rbf | Owen O'Malley | Owen O'Malley |
| [HDFS-16813](https://issues.apache.org/jira/browse/HDFS-16813) | Remove parameter validation logic such as dfs.namenode.decommission.blocks.per.interval in DatanodeAdminManager#activate |  Major | datanode | Haiyang Hu | Haiyang Hu |
| [HDFS-16841](https://issues.apache.org/jira/browse/HDFS-16841) | Enhance the function of DebugAdmin#VerifyECCommand |  Major | erasure-coding | Haiyang Hu | Haiyang Hu |
| [HDFS-16840](https://issues.apache.org/jira/browse/HDFS-16840) | Enhance the usage description about oiv in HDFSCommands.md and OfflineImageViewerPB |  Major | hdfs | Haiyang Hu | Haiyang Hu |
| [HDFS-16779](https://issues.apache.org/jira/browse/HDFS-16779) | Add ErasureCodingPolicy information to the response description for GETFILESTATUS in WebHDFS.md |  Major | webhdfs | ZanderXu | ZanderXu |
| [YARN-11381](https://issues.apache.org/jira/browse/YARN-11381) | Fix hadoop-yarn-common module Java Doc Errors |  Major | yarn | Shilun Fan | Shilun Fan |
| [YARN-11380](https://issues.apache.org/jira/browse/YARN-11380) | Fix hadoop-yarn-api module Java Doc Errors |  Major | yarn | Shilun Fan | Shilun Fan |
| [HDFS-16846](https://issues.apache.org/jira/browse/HDFS-16846) | EC: Only EC blocks should be effected by max-streams-hard-limit configuration |  Major | erasure-coding | caozhiqiang | caozhiqiang |
| [HDFS-16851](https://issues.apache.org/jira/browse/HDFS-16851) | RBF: Add a utility to dump the StateStore |  Major | rbf | Owen O'Malley | Owen O'Malley |
| [HDFS-16839](https://issues.apache.org/jira/browse/HDFS-16839) | It should consider EC reconstruction work when we determine if a node is busy |  Major | ec, erasure-coding | Ruinan Gu | Ruinan Gu |
| [HDFS-16860](https://issues.apache.org/jira/browse/HDFS-16860) | Upgrade moment.min.js to 2.29.4 |  Major | build, ui | D M Murali Krishna Reddy | Anurag Parvatikar |
| [YARN-11385](https://issues.apache.org/jira/browse/YARN-11385) | Fix hadoop-yarn-server-common module Java Doc Errors |  Minor | yarn | Shilun Fan | Shilun Fan |
| [HADOOP-18573](https://issues.apache.org/jira/browse/HADOOP-18573) | Improve error reporting on non-standard kerberos names |  Blocker | security | Steve Loughran | Steve Loughran |
| [HADOOP-18561](https://issues.apache.org/jira/browse/HADOOP-18561) | CVE-2021-37533 on commons-net is included in hadoop common and hadoop-client-runtime |  Blocker | build | phoebe chen | Steve Loughran |
| [HDFS-16873](https://issues.apache.org/jira/browse/HDFS-16873) | FileStatus compareTo does not specify ordering |  Trivial | documentation | DDillon | DDillon |
| [HADOOP-18538](https://issues.apache.org/jira/browse/HADOOP-18538) | Upgrade kafka to 2.8.2 |  Major | build | D M Murali Krishna Reddy | D M Murali Krishna Reddy |
| [HDFS-16652](https://issues.apache.org/jira/browse/HDFS-16652) | Upgrade jquery datatable version references to v1.10.19 |  Major | ui | D M Murali Krishna Reddy | D M Murali Krishna Reddy |
| [YARN-11393](https://issues.apache.org/jira/browse/YARN-11393) | Fs2cs could be extended to set ULF to -1 upon conversion |  Major | yarn | Susheel Gupta | Susheel Gupta |
| [HDFS-16879](https://issues.apache.org/jira/browse/HDFS-16879) | EC : Fsck -blockId shows number of redundant internal block replicas for EC Blocks |  Major | erasure-coding | Haiyang Hu | Haiyang Hu |
| [HDFS-16883](https://issues.apache.org/jira/browse/HDFS-16883) | Duplicate field name in hdfs-default.xml |  Minor | documentation | YUBI LEE | YUBI LEE |
| [HDFS-16887](https://issues.apache.org/jira/browse/HDFS-16887) | Log start and end of phase/step in startup progress |  Minor | namenode | Viraj Jasani | Viraj Jasani |
| [YARN-11409](https://issues.apache.org/jira/browse/YARN-11409) | Fix Typo of ResourceManager#webapp module |  Minor | resourcemanager | Shilun Fan | Shilun Fan |
| [HADOOP-18595](https://issues.apache.org/jira/browse/HADOOP-18595) | Fix "the the" and friends typos |  Minor | common | Shilun Fan | Nikita Eshkeev |
| [HDFS-16891](https://issues.apache.org/jira/browse/HDFS-16891) | Avoid the overhead of copy-on-write exception list while loading inodes sub sections in parallel |  Major | namenode | Viraj Jasani | Viraj Jasani |
| [HDFS-16893](https://issues.apache.org/jira/browse/HDFS-16893) | Standardize the usage of DFSClient debug log |  Minor | dfsclient | Hualong Zhang | Hualong Zhang |
| [HADOOP-18604](https://issues.apache.org/jira/browse/HADOOP-18604) | Add compile platform in the hadoop version output |  Major | build, common | Ayush Saxena | Ayush Saxena |
| [HDFS-16888](https://issues.apache.org/jira/browse/HDFS-16888) | BlockManager#maxReplicationStreams, replicationStreamsHardLimit, blocksReplWorkMultiplier and PendingReconstructionBlocks#timeout should be volatile |  Major | block placement | Haiyang Hu | Haiyang Hu |
| [HADOOP-18592](https://issues.apache.org/jira/browse/HADOOP-18592) | Sasl connection failure should log remote address |  Major | ipc | Viraj Jasani | Viraj Jasani |
| [YARN-11419](https://issues.apache.org/jira/browse/YARN-11419) | Remove redundant exception capture in NMClientAsyncImpl and improve readability in ContainerShellWebSocket, etc |  Minor | client | jingxiong zhong | jingxiong zhong |
| [HDFS-16848](https://issues.apache.org/jira/browse/HDFS-16848) | RBF: Improve StateStoreZookeeperImpl |  Major | rbf | Sun Hao | Sun Hao |
| [HDFS-16903](https://issues.apache.org/jira/browse/HDFS-16903) | Fix javadoc of Class LightWeightResizableGSet |  Trivial | datanode, hdfs | farmmamba | farmmamba |
| [HDFS-16898](https://issues.apache.org/jira/browse/HDFS-16898) | Remove write lock for processCommandFromActor of DataNode to reduce impact on heartbeat |  Major | datanode | farmmamba | farmmamba |
| [HADOOP-18625](https://issues.apache.org/jira/browse/HADOOP-18625) | Fix method name  of RPC.Builder#setnumReaders |  Minor | ipc | Haiyang Hu | Haiyang Hu |
| [MAPREDUCE-7431](https://issues.apache.org/jira/browse/MAPREDUCE-7431) | ShuffleHandler is not working correctly in SSL mode after the Netty 4 upgrade |  Major | mrv2 | Tamas Domok | Tamas Domok |
| [HDFS-16882](https://issues.apache.org/jira/browse/HDFS-16882) | RBF: Add cache hit rate metric in MountTableResolver#getDestinationForPath |  Minor | rbf | farmmamba | farmmamba |
| [HDFS-16907](https://issues.apache.org/jira/browse/HDFS-16907) | Add LastHeartbeatResponseTime for BP service actor |  Major | datanode | Viraj Jasani | Viraj Jasani |
| [HADOOP-18628](https://issues.apache.org/jira/browse/HADOOP-18628) | Server connection should log host name before returning VersionMismatch error |  Minor | ipc | Viraj Jasani | Viraj Jasani |
| [YARN-11323](https://issues.apache.org/jira/browse/YARN-11323) | [Federation] Improve Router Handler FinishApps |  Major | federation, router, yarn | Shilun Fan | Shilun Fan |
| [YARN-11333](https://issues.apache.org/jira/browse/YARN-11333) | Federation: Improve Yarn Router Web Page |  Major | federation, router | Shilun Fan | Shilun Fan |
| [YARN-11425](https://issues.apache.org/jira/browse/YARN-11425) | [Federation] Router Supports SubClusterCleaner |  Major | federation | Shilun Fan | Shilun Fan |
| [HDFS-16914](https://issues.apache.org/jira/browse/HDFS-16914) | Add some logs for updateBlockForPipeline RPC. |  Minor | namanode | farmmamba | farmmamba |
| [HADOOP-18215](https://issues.apache.org/jira/browse/HADOOP-18215) | Enhance WritableName to be able to return aliases for classes that use serializers |  Minor | common | Bryan Beaudreault | Bryan Beaudreault |
| [YARN-11439](https://issues.apache.org/jira/browse/YARN-11439) | Fix Typo of hadoop-yarn-ui README.md |  Minor | yarn-ui-v2 | Shilun Fan | Shilun Fan |
| [HDFS-16916](https://issues.apache.org/jira/browse/HDFS-16916) | Improve the use of JUnit Test in DFSClient |  Minor | dfsclient | Hualong Zhang | Hualong Zhang |
| [HADOOP-18622](https://issues.apache.org/jira/browse/HADOOP-18622) | Upgrade ant to 1.10.13 |  Major | common | Aleksandr Nikolaev | Aleksandr Nikolaev |
| [YARN-11394](https://issues.apache.org/jira/browse/YARN-11394) | Fix hadoop-yarn-server-resourcemanager module Java Doc Errors. |  Major | resourcemanager | Shilun Fan | Shilun Fan |
| [HADOOP-18535](https://issues.apache.org/jira/browse/HADOOP-18535) | Implement token storage solution based on MySQL |  Major | common | Hector Sandoval Chaverri | Hector Sandoval Chaverri |
| [YARN-11370](https://issues.apache.org/jira/browse/YARN-11370) | Refactor MemoryFederationStateStore code. |  Major | federation, router | Shilun Fan | Shilun Fan |
| [HADOOP-18645](https://issues.apache.org/jira/browse/HADOOP-18645) | Provide keytab file key name with ServiceStateException |  Minor | common | Viraj Jasani | Viraj Jasani |
| [HADOOP-18646](https://issues.apache.org/jira/browse/HADOOP-18646) | Upgrade Netty to 4.1.89.Final |  Major | build | Aleksandr Nikolaev | Aleksandr Nikolaev |
| [HADOOP-18661](https://issues.apache.org/jira/browse/HADOOP-18661) | Fix bin/hadoop usage script terminology |  Blocker | scripts | Steve Loughran | Steve Loughran |
| [HDFS-16947](https://issues.apache.org/jira/browse/HDFS-16947) | RBF NamenodeHeartbeatService to report error for not being able to register namenode in state store |  Major | rbf | Viraj Jasani | Viraj Jasani |
| [HDFS-16953](https://issues.apache.org/jira/browse/HDFS-16953) | RBF: Mount table store APIs should update cache only if state store record is successfully updated |  Major | rbf | Viraj Jasani | Viraj Jasani |
| [HADOOP-18644](https://issues.apache.org/jira/browse/HADOOP-18644) | Add bswap support for LoongArch |  Major | native | zhaixiaojuan | zhaixiaojuan |
| [HDFS-16948](https://issues.apache.org/jira/browse/HDFS-16948) | Update log of BlockManager#chooseExcessRedundancyStriped when EC internal block is moved by balancer |  Major | erasure-coding | Ruinan Gu | Ruinan Gu |
| [HDFS-16964](https://issues.apache.org/jira/browse/HDFS-16964) | Improve processing of excess redundancy after failover |  Major | block placement | Shuyan Zhang | Shuyan Zhang |
| [YARN-11426](https://issues.apache.org/jira/browse/YARN-11426) | Improve YARN NodeLabel Memory Display |  Major | resourcemanager | Shilun Fan | Shilun Fan |
| [HADOOP-18458](https://issues.apache.org/jira/browse/HADOOP-18458) | AliyunOSS: AliyunOSSBlockOutputStream to support heap/off-heap buffer before uploading data to OSS |  Major | fs/oss | wujinhu | wujinhu |
| [HDFS-16959](https://issues.apache.org/jira/browse/HDFS-16959) | RBF: State store cache loading metrics |  Major | rbf | Viraj Jasani | Viraj Jasani |
| [YARN-10146](https://issues.apache.org/jira/browse/YARN-10146) | [Federation] Add missing REST APIs for Router |  Major | federation | Bilwa S T | Shilun Fan |
| [HDFS-16967](https://issues.apache.org/jira/browse/HDFS-16967) | RBF: File based state stores should allow concurrent access to the records |  Major | rbf | Viraj Jasani | Viraj Jasani |
| [HADOOP-18684](https://issues.apache.org/jira/browse/HADOOP-18684) | S3A filesystem to support binding to other URI schemes |  Major | common | Harshit Gupta | Harshit Gupta |
| [YARN-11436](https://issues.apache.org/jira/browse/YARN-11436) | [Federation] MemoryFederationStateStore Support Version. |  Major | federation | Shilun Fan | Shilun Fan |
| [HDFS-16973](https://issues.apache.org/jira/browse/HDFS-16973) | RBF: MountTableResolver cache size lookup should take read lock |  Major | rbf | Viraj Jasani | Viraj Jasani |
| [HADOOP-18687](https://issues.apache.org/jira/browse/HADOOP-18687) | Remove unnecessary dependency on json-smart |  Major | auth | Michiel de Jong | Michiel de Jong |
| [HDFS-16952](https://issues.apache.org/jira/browse/HDFS-16952) | Support getLinkTarget API in WebHDFS |  Minor | webhdfs | Hualong Zhang | Hualong Zhang |
| [HDFS-16971](https://issues.apache.org/jira/browse/HDFS-16971) | Add read time metrics for remote reads in Statistics |  Minor | hdfs | Melissa You | Melissa You |
| [HDFS-16974](https://issues.apache.org/jira/browse/HDFS-16974) | Consider volumes average load of each DataNode when choosing target. |  Major | datanode | Shuyan Zhang | Shuyan Zhang |
| [HADOOP-18590](https://issues.apache.org/jira/browse/HADOOP-18590) | Publish SBOM artifacts |  Major | build | Dongjoon Hyun | Dongjoon Hyun |
| [YARN-11465](https://issues.apache.org/jira/browse/YARN-11465) | Improved YarnClient Log Format |  Minor | client | Lu Yuan | Lu Yuan |
| [YARN-11438](https://issues.apache.org/jira/browse/YARN-11438) | [Federation] ZookeeperFederationStateStore Support Version. |  Major | federation | Shilun Fan | Shilun Fan |
| [HADOOP-18597](https://issues.apache.org/jira/browse/HADOOP-18597) | Simplify single node instructions for creating directories for Map Reduce |  Trivial | documentation | Nikita Eshkeev | Nikita Eshkeev |
| [HADOOP-18691](https://issues.apache.org/jira/browse/HADOOP-18691) | Add a CallerContext getter on the Schedulable interface |  Major | common | Christos Bisias | Christos Bisias |
| [YARN-11463](https://issues.apache.org/jira/browse/YARN-11463) | Node Labels root directory creation doesn't have a retry logic |  Major | capacity scheduler | Benjamin Teke | Ashutosh Gupta |
| [HADOOP-18710](https://issues.apache.org/jira/browse/HADOOP-18710) | Add RPC metrics for response time |  Minor | metrics | liuguanghua | liuguanghua |
| [HADOOP-18689](https://issues.apache.org/jira/browse/HADOOP-18689) | Bump jettison from 1.5.3 to 1.5.4 in /hadoop-project |  Major | common | Ayush Saxena | Ayush Saxena |
| [HDFS-16988](https://issues.apache.org/jira/browse/HDFS-16988) | Improve NameServices info at JournalNode web UI |  Minor | journal-node, ui | Zhaohui Wang | Zhaohui Wang |
| [HDFS-16981](https://issues.apache.org/jira/browse/HDFS-16981) | Support getFileLinkStatus API in WebHDFS |  Major | webhdfs | Hualong Zhang | Hualong Zhang |
| [YARN-11437](https://issues.apache.org/jira/browse/YARN-11437) | [Federation] SQLFederationStateStore Support Version. |  Major | federation | Shilun Fan | Shilun Fan |
| [HADOOP-18637](https://issues.apache.org/jira/browse/HADOOP-18637) | S3A to support upload of files greater than 2 GB using DiskBlocks |  Major | fs/s3 | Harshit Gupta | Harshit Gupta |
| [YARN-11474](https://issues.apache.org/jira/browse/YARN-11474) | The yarn queue list is displayed on the CLI |  Minor | client | Lu Yuan | Lu Yuan |
| [HDFS-16995](https://issues.apache.org/jira/browse/HDFS-16995) | Remove unused parameters at NameNodeHttpServer#initWebHdfs |  Minor | webhdfs | Zhaohui Wang | Zhaohui Wang |
| [HDFS-16707](https://issues.apache.org/jira/browse/HDFS-16707) | RBF: Expose RouterRpcFairnessPolicyController related request record metrics for each nameservice to Prometheus |  Minor | rbf | Jiale Qi | Jiale Qi |
| [HADOOP-18725](https://issues.apache.org/jira/browse/HADOOP-18725) | Avoid cross-platform build for irrelevant Dockerfile changes |  Major | build | Gautham Banasandra | Gautham Banasandra |
| [YARN-11462](https://issues.apache.org/jira/browse/YARN-11462) | Fix Typo of hadoop-yarn-common |  Minor | yarn | Shilun Fan | Shilun Fan |
| [YARN-11450](https://issues.apache.org/jira/browse/YARN-11450) | Improvements for TestYarnConfigurationFields and TestConfigurationFieldsBase |  Minor | test | Szilard Nemeth | Szilard Nemeth |
| [HDFS-16997](https://issues.apache.org/jira/browse/HDFS-16997) | Set the locale to avoid printing useless logs in BlockSender |  Major | block placement | Shuyan Zhang | Shuyan Zhang |
| [YARN-10144](https://issues.apache.org/jira/browse/YARN-10144) | Federation: Add missing FederationClientInterceptor APIs |  Major | federation, router | D M Murali Krishna Reddy | Shilun Fan |
| [HADOOP-18134](https://issues.apache.org/jira/browse/HADOOP-18134) | Setup Jenkins nightly CI for Windows 10 |  Critical | build | Gautham Banasandra | Gautham Banasandra |
| [YARN-11470](https://issues.apache.org/jira/browse/YARN-11470) | FederationStateStoreFacade Cache Support Guava Cache |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11477](https://issues.apache.org/jira/browse/YARN-11477) | [Federation] MemoryFederationStateStore Support Store ApplicationSubmitData |  Major | federation | Shilun Fan | Shilun Fan |
| [HADOOP-18359](https://issues.apache.org/jira/browse/HADOOP-18359) | Update commons-cli from 1.2 to 1.5. |  Major | common | Shilun Fan | Shilun Fan |
| [YARN-11479](https://issues.apache.org/jira/browse/YARN-11479) | [Federation] ZookeeperFederationStateStore Support Store ApplicationSubmitData |  Major | federation | Shilun Fan | Shilun Fan |
| [HDFS-16990](https://issues.apache.org/jira/browse/HDFS-16990) | HttpFS Add Support getFileLinkStatus API |  Major | httpfs | Hualong Zhang | Hualong Zhang |
| [YARN-11351](https://issues.apache.org/jira/browse/YARN-11351) | [Federation] Router Support Calls SubCluster's RMAdminRequest |  Major | federation, router | Shilun Fan | Shilun Fan |
| [HDFS-16978](https://issues.apache.org/jira/browse/HDFS-16978) | RBF: Admin command to support bulk add of mount points |  Minor | rbf | Viraj Jasani | Viraj Jasani |
| [HDFS-17001](https://issues.apache.org/jira/browse/HDFS-17001) | Support getStatus API in WebHDFS |  Major | webhdfs | Hualong Zhang | Hualong Zhang |
| [YARN-11495](https://issues.apache.org/jira/browse/YARN-11495) | Fix typos in hadoop-yarn-server-web-proxy |  Minor | webapp | Shilun Fan | Shilun Fan |
| [HDFS-17015](https://issues.apache.org/jira/browse/HDFS-17015) | Typos in HDFS Documents |  Minor | configuration | Liang Yan | Liang Yan |
| [HDFS-17009](https://issues.apache.org/jira/browse/HDFS-17009) | RBF: state store putAll should also return failed records |  Major | rbf | Viraj Jasani | Viraj Jasani |
| [HDFS-17012](https://issues.apache.org/jira/browse/HDFS-17012) | Remove unused DFSConfigKeys#DFS\_DATANODE\_PMEM\_CACHE\_DIRS\_DEFAULT |  Minor | datanode, hdfs | JiangHua Zhu | JiangHua Zhu |
| [HDFS-16979](https://issues.apache.org/jira/browse/HDFS-16979) | RBF: Add dfsrouter port in hdfsauditlog |  Major | rbf | liuguanghua | liuguanghua |
| [HDFS-16653](https://issues.apache.org/jira/browse/HDFS-16653) | Improve error messages in ShortCircuitCache |  Minor | dfsadmin | ECFuzz | ECFuzz |
| [HDFS-17014](https://issues.apache.org/jira/browse/HDFS-17014) | HttpFS Add Support getStatus API |  Major | webhdfs | Hualong Zhang | Hualong Zhang |
| [YARN-11496](https://issues.apache.org/jira/browse/YARN-11496) | Improve TimelineService log format |  Minor | timelineservice | Xianming Lei | Xianming Lei |
| [HDFS-16909](https://issues.apache.org/jira/browse/HDFS-16909) | Improve ReplicaMap#mergeAll method. |  Minor | datanode | farmmamba | farmmamba |
| [HDFS-17020](https://issues.apache.org/jira/browse/HDFS-17020) | RBF: mount table addAll should print failed records in std error |  Major | rbf | Viraj Jasani | Viraj Jasani |
| [HDFS-16908](https://issues.apache.org/jira/browse/HDFS-16908) | Fix javadoc of field IncrementalBlockReportManager#readyToSend. |  Major | datanode | farmmamba | farmmamba |
| [YARN-11276](https://issues.apache.org/jira/browse/YARN-11276) | Add lru cache for RMWebServices.getApps |  Minor | resourcemanager | Xianming Lei | Xianming Lei |
| [HDFS-17026](https://issues.apache.org/jira/browse/HDFS-17026) | RBF: NamenodeHeartbeatService should update JMX report with configurable frequency |  Major | rbf | Hector Sandoval Chaverri | Hector Sandoval Chaverri |
| [HDFS-17031](https://issues.apache.org/jira/browse/HDFS-17031) | RBF: Reduce repeated code in RouterRpcServer |  Minor | rbf | Chengwei Wang | Chengwei Wang |
| [HADOOP-18709](https://issues.apache.org/jira/browse/HADOOP-18709) | Add curator based ZooKeeper communication support over SSL/TLS into the common library |  Major | common | Ferenc Erdelyi | Ferenc Erdelyi |
| [YARN-11277](https://issues.apache.org/jira/browse/YARN-11277) | trigger deletion of log-dir by size for NonAggregatingLogHandler |  Minor | nodemanager | Xianming Lei | Xianming Lei |
| [HDFS-17028](https://issues.apache.org/jira/browse/HDFS-17028) | RBF: Optimize debug logs of class ConnectionPool and other related class. |  Minor | rbf | farmmamba | farmmamba |
| [YARN-11497](https://issues.apache.org/jira/browse/YARN-11497) | Support removal of only selective node states in untracked removal flow |  Major | resourcemanager | Mudit Sharma | Mudit Sharma |
| [HDFS-17029](https://issues.apache.org/jira/browse/HDFS-17029) | Support getECPolices API in WebHDFS |  Major | webhdfs | Hualong Zhang | Hualong Zhang |
| [HDFS-17035](https://issues.apache.org/jira/browse/HDFS-17035) | FsVolumeImpl#getActualNonDfsUsed may return negative value |  Minor | datanode | farmmamba | farmmamba |
| [HADOOP-11219](https://issues.apache.org/jira/browse/HADOOP-11219) | [Umbrella] Upgrade to netty 4 |  Major | build, common | Haohui Mai | Haohui Mai |
| [HDFS-17037](https://issues.apache.org/jira/browse/HDFS-17037) | Consider nonDfsUsed when running balancer |  Major | balancer & mover | Shuyan Zhang | Shuyan Zhang |
| [YARN-11504](https://issues.apache.org/jira/browse/YARN-11504) | [Federation] YARN Federation Supports Non-HA mode. |  Major | federation, nodemanager | Shilun Fan | Shilun Fan |
| [YARN-11429](https://issues.apache.org/jira/browse/YARN-11429) | Improve updateTestDataAutomatically in TestRMWebServicesCapacitySched |  Major | yarn | Tamas Domok | Tamas Domok |
| [HDFS-17030](https://issues.apache.org/jira/browse/HDFS-17030) | Limit wait time for getHAServiceState in ObserverReaderProxy |  Minor | hdfs | Xing Lin | Xing Lin |
| [HDFS-17042](https://issues.apache.org/jira/browse/HDFS-17042) | Add rpcCallSuccesses and OverallRpcProcessingTime to RpcMetrics for Namenode |  Major | hdfs | Xing Lin | Xing Lin |
| [HDFS-17043](https://issues.apache.org/jira/browse/HDFS-17043) | HttpFS implementation for getAllErasureCodingPolicies |  Major | httpfs | Hualong Zhang | Hualong Zhang |
| [HADOOP-18774](https://issues.apache.org/jira/browse/HADOOP-18774) | Add .vscode to gitignore |  Major | common | Xiaoqiao He | Xiaoqiao He |
| [YARN-11506](https://issues.apache.org/jira/browse/YARN-11506) | The formatted yarn queue list is displayed on the command line |  Minor | yarn | Lu Yuan | Lu Yuan |
| [HDFS-17053](https://issues.apache.org/jira/browse/HDFS-17053) | Optimize method BlockInfoStriped#findSlot to reduce time complexity. |  Trivial | hdfs | farmmamba | farmmamba |
| [YARN-11511](https://issues.apache.org/jira/browse/YARN-11511) | Improve TestRMWebServices test config and data |  Major | capacityscheduler | Tamas Domok | Bence Kosztolnik |
| [HADOOP-18713](https://issues.apache.org/jira/browse/HADOOP-18713) | Update solr from 8.8.2 to 8.11.2 |  Minor | common | Xuesen Liang | Xuesen Liang |
| [HDFS-17057](https://issues.apache.org/jira/browse/HDFS-17057) | RBF: Add DataNode maintenance states to Federation UI |  Major | rbf | Haiyang Hu | Haiyang Hu |
| [HDFS-17055](https://issues.apache.org/jira/browse/HDFS-17055) | Export HAState as a metric from Namenode for monitoring |  Minor | hdfs | Xing Lin | Xing Lin |
| [HDFS-17044](https://issues.apache.org/jira/browse/HDFS-17044) | Set size of non-exist block to NO\_ACK when process FBR or IBR to avoid useless report from DataNode |  Major | namenode | Haiyang Hu | Haiyang Hu |
| [HADOOP-18789](https://issues.apache.org/jira/browse/HADOOP-18789) | Remove ozone from hadoop dev support |  Trivial | common | Xiaoqiao He | Xiaoqiao He |
| [HDFS-17065](https://issues.apache.org/jira/browse/HDFS-17065) | Fix typos in hadoop-hdfs-project |  Minor | hdfs | Zhaohui Wang | Zhaohui Wang |
| [HADOOP-18779](https://issues.apache.org/jira/browse/HADOOP-18779) | Improve hadoop-function.sh#status script |  Major | common | Shilun Fan | Shilun Fan |
| [HDFS-17073](https://issues.apache.org/jira/browse/HDFS-17073) | Enhance the warning message output for BlockGroupNonStripedChecksumComputer#compute |  Major | hdfs | Haiyang Hu | Haiyang Hu |
| [HDFS-17070](https://issues.apache.org/jira/browse/HDFS-17070) | Remove unused import in DataNodeMetricHelper.java. |  Trivial | datanode | farmmamba | farmmamba |
| [HDFS-17064](https://issues.apache.org/jira/browse/HDFS-17064) | Document the usage of the new Balancer "sortTopNodes" and "hotBlockTimeInterval" parameter |  Major | balancer, documentation | Haiyang Hu | Haiyang Hu |
| [HDFS-17033](https://issues.apache.org/jira/browse/HDFS-17033) | Update fsck to display stale state info of blocks accurately |  Minor | datanode, namanode | WangYuanben | WangYuanben |
| [HDFS-17076](https://issues.apache.org/jira/browse/HDFS-17076) | Remove the unused method isSlownodeByNameserviceId in DataNode |  Major | datanode | Haiyang Hu | Haiyang Hu |
| [HADOOP-18794](https://issues.apache.org/jira/browse/HADOOP-18794) | ipc.server.handler.queue.size missing from core-default.xml |  Major | rpc-server | WangYuanben | WangYuanben |
| [HDFS-17082](https://issues.apache.org/jira/browse/HDFS-17082) | Add documentation for provisionSnapshotTrash command to HDFSCommands.md  and HdfsSnapshots.md |  Major | documentation | Haiyang Hu | Haiyang Hu |
| [HDFS-17083](https://issues.apache.org/jira/browse/HDFS-17083) | Support getErasureCodeCodecs API in WebHDFS |  Major | webhdfs | Hualong Zhang | Hualong Zhang |
| [HDFS-17068](https://issues.apache.org/jira/browse/HDFS-17068) | Datanode should record last directory scan time. |  Minor | datanode | farmmamba | farmmamba |
| [HDFS-17086](https://issues.apache.org/jira/browse/HDFS-17086) | Fix the parameter settings in TestDiskspaceQuotaUpdate#updateCountForQuota. |  Major | test | Haiyang Hu | Haiyang Hu |
| [HADOOP-18801](https://issues.apache.org/jira/browse/HADOOP-18801) | Delete path directly when it can not be parsed in trash |  Major | common | farmmamba | farmmamba |
| [HDFS-17075](https://issues.apache.org/jira/browse/HDFS-17075) | Reconfig disk balancer parameters for datanode |  Major | datanode | Haiyang Hu | Haiyang Hu |
| [HDFS-17091](https://issues.apache.org/jira/browse/HDFS-17091) | Blocks on DECOMMISSIONING DNs should be sorted properly in LocatedBlocks |  Major | hdfs | WangYuanben | WangYuanben |
| [HDFS-17088](https://issues.apache.org/jira/browse/HDFS-17088) | Improve the debug verifyEC and dfsrouteradmin commands in HDFSCommands.md |  Major | dfsadmin | Haiyang Hu | Haiyang Hu |
| [YARN-11540](https://issues.apache.org/jira/browse/YARN-11540) | Fix typo: form -\> from |  Trivial | nodemanager | Seokchan Yoon | Seokchan Yoon |
| [HDFS-17074](https://issues.apache.org/jira/browse/HDFS-17074) | Remove incorrect comment in TestRedudantBlocks#setup |  Trivial | test | farmmamba | farmmamba |
| [HDFS-17112](https://issues.apache.org/jira/browse/HDFS-17112) | Show decommission duration in JMX and HTML |  Major | namenode | Shuyan Zhang | Shuyan Zhang |
| [HDFS-17119](https://issues.apache.org/jira/browse/HDFS-17119) | RBF: Logger fix for StateStoreMySQLImpl |  Trivial | rbf | Zhaohui Wang | Zhaohui Wang |
| [HDFS-17115](https://issues.apache.org/jira/browse/HDFS-17115) | HttpFS Add Support getErasureCodeCodecs API |  Major | httpfs | Hualong Zhang | Hualong Zhang |
| [HDFS-17117](https://issues.apache.org/jira/browse/HDFS-17117) | Print reconstructionQueuesInitProgress periodically when BlockManager processMisReplicatesAsync. |  Major | namenode | Haiyang Hu | Haiyang Hu |
| [HDFS-17116](https://issues.apache.org/jira/browse/HDFS-17116) | RBF: Update invoke millisecond time as monotonicNow() in RouterSafemodeService. |  Major | rbf | Haiyang Hu | Haiyang Hu |
| [HDFS-17135](https://issues.apache.org/jira/browse/HDFS-17135) | Update fsck -blockId  to display excess state info of blocks |  Major | namnode | Haiyang Hu | Haiyang Hu |
| [HDFS-17136](https://issues.apache.org/jira/browse/HDFS-17136) | Fix annotation description and typo in BlockPlacementPolicyDefault Class |  Minor | block placement | Zhaobo Huang | Zhaobo Huang |
| [YARN-11416](https://issues.apache.org/jira/browse/YARN-11416) | FS2CS should use CapacitySchedulerConfiguration in FSQueueConverterBuilder |  Major | capacity scheduler | Benjamin Teke | Susheel Gupta |
| [HDFS-17118](https://issues.apache.org/jira/browse/HDFS-17118) | Fix minor checkstyle warnings in TestObserverReadProxyProvider |  Trivial | hdfs | Xing Lin | Xing Lin |
| [HADOOP-18836](https://issues.apache.org/jira/browse/HADOOP-18836) | Some properties are missing from hadoop-policy.xml. |  Major | common, documentation, security | WangYuanben | WangYuanben |
| [HADOOP-18810](https://issues.apache.org/jira/browse/HADOOP-18810) | Document missing a lot of properties in core-default.xml |  Major | common, documentation | WangYuanben | WangYuanben |
| [HDFS-17144](https://issues.apache.org/jira/browse/HDFS-17144) | Remove incorrect comment in method storeAllocatedBlock |  Trivial | namenode | farmmamba | farmmamba |
| [HDFS-17137](https://issues.apache.org/jira/browse/HDFS-17137) | Standby/Observer NameNode skip to handle redundant replica block logic when set decrease replication. |  Major | namenode | Haiyang Hu | Haiyang Hu |
| [HADOOP-18840](https://issues.apache.org/jira/browse/HADOOP-18840) | Add enQueue time to RpcMetrics |  Minor | rpc-server | Liangjun He | Liangjun He |
| [HDFS-17145](https://issues.apache.org/jira/browse/HDFS-17145) | Fix description of property dfs.namenode.file.close.num-committed-allowed. |  Trivial | documentation | farmmamba | farmmamba |
| [HDFS-17148](https://issues.apache.org/jira/browse/HDFS-17148) | RBF: SQLDelegationTokenSecretManager must cleanup expired tokens in SQL |  Major | rbf | Hector Sandoval Chaverri | Hector Sandoval Chaverri |
| [HDFS-17087](https://issues.apache.org/jira/browse/HDFS-17087) | Add Throttler for datanode reading block |  Major | datanode | Haiyang Hu | Haiyang Hu |
| [HDFS-17162](https://issues.apache.org/jira/browse/HDFS-17162) | RBF: Add missing comments in StateStoreService |  Minor | rbf | TIsNotT | TIsNotT |
| [HADOOP-18328](https://issues.apache.org/jira/browse/HADOOP-18328) | S3A supports S3 on Outposts |  Major | fs/s3 | Sotetsu Suzugamine | Sotetsu Suzugamine |
| [HDFS-17168](https://issues.apache.org/jira/browse/HDFS-17168) | Support getTrashRoots API in WebHDFS |  Major | webhdfs | Hualong Zhang | Hualong Zhang |
| [HADOOP-18880](https://issues.apache.org/jira/browse/HADOOP-18880) | Add some rpc related metrics to Metrics.md |  Major | documentation | Haiyang Hu | Haiyang Hu |
| [HDFS-17140](https://issues.apache.org/jira/browse/HDFS-17140) | Revisit the BPOfferService.reportBadBlocks() method. |  Minor | datanode | Liangjun He | Liangjun He |
| [YARN-11564](https://issues.apache.org/jira/browse/YARN-11564) | Fix wrong config in yarn-default.xml |  Major | router | Chenyu Zheng | Chenyu Zheng |
| [HDFS-17177](https://issues.apache.org/jira/browse/HDFS-17177) | ErasureCodingWork reconstruct ignore the block length is Long.MAX\_VALUE. |  Major | erasure-coding | Haiyang Hu | Haiyang Hu |
| [HDFS-17139](https://issues.apache.org/jira/browse/HDFS-17139) | RBF: For the doc of the class RouterAdminProtocolTranslatorPB, it describes the function of the class ClientNamenodeProtocolTranslatorPB |  Minor | rbf | Jian Zhang | Jian Zhang |
| [HDFS-17178](https://issues.apache.org/jira/browse/HDFS-17178) | BootstrapStandby needs to handle RollingUpgrade |  Minor | namenode | Danny Becker | Danny Becker |
| [MAPREDUCE-7453](https://issues.apache.org/jira/browse/MAPREDUCE-7453) | Revert HADOOP-18649 |  Major | mrv2 | Chenyu Zheng | Chenyu Zheng |
| [HDFS-17180](https://issues.apache.org/jira/browse/HDFS-17180) | HttpFS Add Support getTrashRoots API |  Major | webhdfs | Hualong Zhang | Hualong Zhang |
| [HDFS-17192](https://issues.apache.org/jira/browse/HDFS-17192) | Add bock info when constructing remote block reader meets IOException |  Trivial | hdfs-client | farmmamba | farmmamba |
| [HADOOP-18797](https://issues.apache.org/jira/browse/HADOOP-18797) | Support Concurrent Writes With S3A Magic Committer |  Major | fs/s3 | Emanuel Velzi | Syed Shameerur Rahman |
| [HDFS-17184](https://issues.apache.org/jira/browse/HDFS-17184) | Improve BlockReceiver to throws DiskOutOfSpaceException when initialize |  Major | datanode | Haiyang Hu | Haiyang Hu |
| [YARN-11567](https://issues.apache.org/jira/browse/YARN-11567) | Aggregate container launch debug artifacts automatically in case of error |  Minor | yarn | Bence Kosztolnik | Bence Kosztolnik |
| [HDFS-17197](https://issues.apache.org/jira/browse/HDFS-17197) | Show file replication when listing corrupt files. |  Major | fs, namanode | Shuyan Zhang | Shuyan Zhang |
| [HDFS-17204](https://issues.apache.org/jira/browse/HDFS-17204) | EC: Reduce unnecessary log when processing excess redundancy. |  Major | erasure-coding | Shuyan Zhang | Shuyan Zhang |
| [YARN-11468](https://issues.apache.org/jira/browse/YARN-11468) | Zookeeper SSL/TLS support |  Critical | resourcemanager | Ferenc Erdelyi | Ferenc Erdelyi |
| [HDFS-17211](https://issues.apache.org/jira/browse/HDFS-17211) | Fix comments in the RemoteParam class |  Minor | rbf | xiaojunxiang | xiaojunxiang |
| [HDFS-17194](https://issues.apache.org/jira/browse/HDFS-17194) | Enhance the log message for striped block recovery |  Major | logging | Haiyang Hu | Haiyang Hu |
| [HADOOP-18917](https://issues.apache.org/jira/browse/HADOOP-18917) | upgrade to commons-io 2.14.0 |  Major | build | PJ Fanning | PJ Fanning |
| [HDFS-17205](https://issues.apache.org/jira/browse/HDFS-17205) | HdfsServerConstants.MIN\_BLOCKS\_FOR\_WRITE should be configurable |  Major | hdfs | Haiyang Hu | Haiyang Hu |
| [HDFS-17200](https://issues.apache.org/jira/browse/HDFS-17200) | Add some datanode related metrics to Metrics.md |  Minor | datanode, metrics | Zhaobo Huang | Zhaobo Huang |
| [HDFS-17171](https://issues.apache.org/jira/browse/HDFS-17171) | CONGESTION\_RATIO should be configurable |  Minor | datanode | farmmamba | farmmamba |
| [HDFS-16740](https://issues.apache.org/jira/browse/HDFS-16740) | Mini cluster test flakiness |  Major | hdfs, test | Steve Vaughan | Steve Vaughan |
| [HDFS-17208](https://issues.apache.org/jira/browse/HDFS-17208) | Add the metrics PendingAsyncDiskOperations  in datanode |  Major | datanode | Haiyang Hu | Haiyang Hu |
| [HDFS-17217](https://issues.apache.org/jira/browse/HDFS-17217) | Add lifeline RPC start up  log when NameNode#startCommonServices |  Major | namenode | Haiyang Hu | Haiyang Hu |
| [HADOOP-18890](https://issues.apache.org/jira/browse/HADOOP-18890) | remove okhttp usage |  Major | build, common | PJ Fanning | PJ Fanning |
| [HADOOP-18926](https://issues.apache.org/jira/browse/HADOOP-18926) | Add documentation related to NodeFencer |  Minor | documentation, ha | JiangHua Zhu | JiangHua Zhu |
| [YARN-11583](https://issues.apache.org/jira/browse/YARN-11583) | Improve Node Link for YARN Federation Web Page |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11469](https://issues.apache.org/jira/browse/YARN-11469) | Refactor FederationStateStoreFacade Cache Code |  Major | federation | Shilun Fan | Shilun Fan |
| [HADOOP-18916](https://issues.apache.org/jira/browse/HADOOP-18916) | module-info classes from external dependencies appearing in uber jars |  Major | build | PJ Fanning | PJ Fanning |
| [HDFS-17210](https://issues.apache.org/jira/browse/HDFS-17210) | Optimize AvailableSpaceBlockPlacementPolicy |  Minor | hdfs | Fei Guo | guophilipse |
| [HDFS-17228](https://issues.apache.org/jira/browse/HDFS-17228) | Improve documentation related to BlockManager |  Minor | block placement, documentation | JiangHua Zhu | JiangHua Zhu |
| [HADOOP-18867](https://issues.apache.org/jira/browse/HADOOP-18867) | Upgrade ZooKeeper to 3.6.4 |  Minor | build | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-18942](https://issues.apache.org/jira/browse/HADOOP-18942) | Upgrade ZooKeeper to 3.7.2 |  Major | common | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-17235](https://issues.apache.org/jira/browse/HDFS-17235) | Fix javadoc errors in BlockManager |  Major | documentation | Haiyang Hu | Haiyang Hu |
| [HADOOP-18919](https://issues.apache.org/jira/browse/HADOOP-18919) | Zookeeper SSL/TLS support in HDFS ZKFC |  Major | common | Zita Dombi | Zita Dombi |
| [HADOOP-18868](https://issues.apache.org/jira/browse/HADOOP-18868) | Optimize the configuration and use of callqueue overflow trigger failover |  Major | common | Haiyang Hu | Haiyang Hu |
| [HADOOP-18949](https://issues.apache.org/jira/browse/HADOOP-18949) | upgrade maven dependency plugin due to security issue |  Major | build | PJ Fanning | PJ Fanning |
| [HADOOP-18920](https://issues.apache.org/jira/browse/HADOOP-18920) | RPC Metrics : Optimize logic for log slow RPCs |  Major | metrics | Haiyang Hu | Haiyang Hu |
| [HADOOP-18933](https://issues.apache.org/jira/browse/HADOOP-18933) | upgrade netty to 4.1.100 due to CVE |  Major | build | PJ Fanning | PJ Fanning |
| [HDFS-15273](https://issues.apache.org/jira/browse/HDFS-15273) | CacheReplicationMonitor hold lock for long time and lead to NN out of service |  Major | caching, namenode | Xiaoqiao He | Xiaoqiao He |
| [YARN-11592](https://issues.apache.org/jira/browse/YARN-11592) | Add timeout to GPGUtils#invokeRMWebService. |  Major | federation | Shilun Fan | Shilun Fan |
| [HADOOP-18936](https://issues.apache.org/jira/browse/HADOOP-18936) | Upgrade to jetty 9.4.53 |  Major | build | PJ Fanning | PJ Fanning |
| [MAPREDUCE-7457](https://issues.apache.org/jira/browse/MAPREDUCE-7457) | Limit number of spill files getting created |  Critical | mrv2 | Mudit Sharma | Mudit Sharma |
| [HADOOP-18963](https://issues.apache.org/jira/browse/HADOOP-18963) | Fix typos in .gitignore |  Minor | common | 袁焊忠 | 袁焊忠 |
| [HDFS-17243](https://issues.apache.org/jira/browse/HDFS-17243) | Add the parameter storage type for getBlocks method |  Major | balancer | Haiyang Hu | Haiyang Hu |
| [HDFS-16791](https://issues.apache.org/jira/browse/HDFS-16791) | Add getEnclosingRoot() API to filesystem interface and implementations |  Major | fs | Tom McCormick | Tom McCormick |
| [HADOOP-18954](https://issues.apache.org/jira/browse/HADOOP-18954) | Filter NaN values from JMX json interface |  Major | common | Bence Kosztolnik | Bence Kosztolnik |
| [HADOOP-18964](https://issues.apache.org/jira/browse/HADOOP-18964) | Update plugin for SBOM generation to 2.7.10 |  Major | common | Vinod Anandan | Vinod Anandan |
| [HDFS-17172](https://issues.apache.org/jira/browse/HDFS-17172) | Support FSNamesystemLock Parameters reconfigurable |  Major | namanode | Haiyang Hu | Haiyang Hu |
| [HADOOP-18956](https://issues.apache.org/jira/browse/HADOOP-18956) | Zookeeper SSL/TLS support in ZKDelegationTokenSecretManager and ZKSignerSecretProvider |  Major | common | Zita Dombi | István Fajth |
| [HADOOP-18957](https://issues.apache.org/jira/browse/HADOOP-18957) | Use StandardCharsets.UTF\_8 constant |  Major | common | PJ Fanning | PJ Fanning |
| [YARN-11611](https://issues.apache.org/jira/browse/YARN-11611) | Remove json-io to 4.14.1 due to CVE-2023-34610 |  Major | yarn | Benjamin Teke | Benjamin Teke |
| [HDFS-17263](https://issues.apache.org/jira/browse/HDFS-17263) | RBF: Fix client ls trash path cannot get except default nameservices trash path |  Major | rbf | liuguanghua | liuguanghua |
| [HADOOP-18924](https://issues.apache.org/jira/browse/HADOOP-18924) | Upgrade grpc jars to v1.53.0 due to CVEs |  Major | build | PJ Fanning | PJ Fanning |
| [HDFS-17259](https://issues.apache.org/jira/browse/HDFS-17259) | Fix typo in TestFsDatasetImpl Class. |  Trivial | test | Zhaobo Huang | Zhaobo Huang |
| [HDFS-17218](https://issues.apache.org/jira/browse/HDFS-17218) | NameNode should process time out excess redundancy blocks |  Major | namanode | Haiyang Hu | Haiyang Hu |
| [HDFS-17250](https://issues.apache.org/jira/browse/HDFS-17250) | EditLogTailer#triggerActiveLogRoll should handle thread Interrupted |  Major | hdfs | Haiyang Hu | Haiyang Hu |
| [YARN-11420](https://issues.apache.org/jira/browse/YARN-11420) | Stabilize TestNMClient |  Major | yarn | Bence Kosztolnik | Susheel Gupta |
| [HADOOP-18982](https://issues.apache.org/jira/browse/HADOOP-18982) | Fix doc about loading native libraries |  Major | documentation | Shuyan Zhang | Shuyan Zhang |
| [YARN-11423](https://issues.apache.org/jira/browse/YARN-11423) | [Federation] Router Supports CLI Commands |  Major | federation | Shilun Fan | Shilun Fan |
| [HADOOP-18925](https://issues.apache.org/jira/browse/HADOOP-18925) | S3A: add option "fs.s3a.optimized.copy.from.local.enabled" to enable/disable CopyFromLocalOperation |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-18989](https://issues.apache.org/jira/browse/HADOOP-18989) | Use thread pool to improve the speed of creating control files in TestDFSIO |  Major | benchmarks, common | farmmamba | farmmamba |
| [HDFS-17279](https://issues.apache.org/jira/browse/HDFS-17279) | RBF: Fix link to Fedbalance document |  Major | rbf | Haiyang Hu | Haiyang Hu |
| [HDFS-17272](https://issues.apache.org/jira/browse/HDFS-17272) | NNThroughputBenchmark should support specifying the base directory for multi-client test |  Major | namenode | caozhiqiang | caozhiqiang |
| [HDFS-17152](https://issues.apache.org/jira/browse/HDFS-17152) | Fix the documentation of count command in FileSystemShell.md |  Trivial | documentation | farmmamba | farmmamba |
| [HDFS-17242](https://issues.apache.org/jira/browse/HDFS-17242) | Make congestion backoff time configurable |  Minor | hdfs-client | farmmamba | farmmamba |
| [HDFS-17282](https://issues.apache.org/jira/browse/HDFS-17282) | Reconfig 'SlowIoWarningThreshold' parameters for datanode. |  Minor | datanode | Zhaobo Huang | Zhaobo Huang |
| [YARN-11630](https://issues.apache.org/jira/browse/YARN-11630) | Passing admin Java options to container localizers |  Major | yarn | Peter Szucs | Peter Szucs |
| [YARN-11563](https://issues.apache.org/jira/browse/YARN-11563) | Fix typo in AbstractContainerAllocator from CSAssignemnt to CSAssignment |  Trivial | capacityscheduler | wangzhongwei | wangzhongwei |
| [HADOOP-18613](https://issues.apache.org/jira/browse/HADOOP-18613) | Upgrade ZooKeeper to version 3.8.3 |  Major | common | Tamas Penzes | Bilwa S T |
| [YARN-11634](https://issues.apache.org/jira/browse/YARN-11634) | Speed-up TestTimelineClient |  Minor | yarn | Bence Kosztolnik | Bence Kosztolnik |
| [HDFS-17285](https://issues.apache.org/jira/browse/HDFS-17285) | RBF: Add a safe mode check period configuration |  Minor | rbf | liuguanghua | liuguanghua |
| [HDFS-17215](https://issues.apache.org/jira/browse/HDFS-17215) | RBF: Fix some method annotations about @throws |  Minor | rbf | xiaojunxiang | xiaojunxiang |
| [HDFS-17275](https://issues.apache.org/jira/browse/HDFS-17275) | Judge whether the block has been deleted in the block report |  Minor | hdfs | lei w | lei w |
| [HDFS-17297](https://issues.apache.org/jira/browse/HDFS-17297) | The NameNode should remove block from the BlocksMap if the block is marked as deleted. |  Major | namanode | Haiyang Hu | Haiyang Hu |
| [HDFS-17277](https://issues.apache.org/jira/browse/HDFS-17277) | Delete invalid code logic in namenode format |  Minor | namenode | zhangzhanchang | zhangzhanchang |
| [HADOOP-18540](https://issues.apache.org/jira/browse/HADOOP-18540) | Upgrade Bouncy Castle to 1.70 |  Major | build | D M Murali Krishna Reddy | D M Murali Krishna Reddy |
| [HDFS-17310](https://issues.apache.org/jira/browse/HDFS-17310) | DiskBalancer: Enhance the log message for submitPlan |  Major | datanode | Haiyang Hu | Haiyang Hu |
| [HDFS-17023](https://issues.apache.org/jira/browse/HDFS-17023) | RBF: Record proxy time when call invokeConcurrent method. |  Minor | rbf | farmmamba | farmmamba |
| [YARN-11529](https://issues.apache.org/jira/browse/YARN-11529) | Add metrics for ContainerMonitorImpl. |  Minor | nodemanager | Xianming Lei | Xianming Lei |
| [HDFS-17306](https://issues.apache.org/jira/browse/HDFS-17306) | RBF:Router should not return nameservices that does not enable observer nodes in RpcResponseHeaderProto |  Major | rdf, router | liuguanghua | liuguanghua |
| [HDFS-17322](https://issues.apache.org/jira/browse/HDFS-17322) | RetryCache#MAX\_CAPACITY seems to be MIN\_CAPACITY |  Trivial | ipc | farmmamba | farmmamba |
| [HDFS-17325](https://issues.apache.org/jira/browse/HDFS-17325) | Doc: Fix the documentation of fs expunge command in FileSystemShell.md |  Minor | documentation, fs | liuguanghua | liuguanghua |
| [HDFS-17315](https://issues.apache.org/jira/browse/HDFS-17315) | Optimize the namenode format code logic. |  Major | namenode | Zhaobo Huang | Zhaobo Huang |
| [YARN-11642](https://issues.apache.org/jira/browse/YARN-11642) | Fix Flaky Test TestTimelineAuthFilterForV2#testPutTimelineEntities |  Major | timelineservice | Shilun Fan | Shilun Fan |
| [HDFS-17317](https://issues.apache.org/jira/browse/HDFS-17317) | DebugAdmin metaOut not  need multiple close |  Major | hdfs | xy | xy |
| [HDFS-17312](https://issues.apache.org/jira/browse/HDFS-17312) | packetsReceived metric should ignore heartbeat packet |  Major | datanode | farmmamba | farmmamba |
| [HDFS-17128](https://issues.apache.org/jira/browse/HDFS-17128) | RBF: SQLDelegationTokenSecretManager should use version of tokens updated by other routers |  Major | rbf | Hector Sandoval Chaverri | Hector Sandoval Chaverri |
| [HADOOP-19034](https://issues.apache.org/jira/browse/HADOOP-19034) | Fix Download Maven Url Not Found |  Major | common | Shilun Fan | Shilun Fan |
| [MAPREDUCE-7468](https://issues.apache.org/jira/browse/MAPREDUCE-7468) | Change add-opens flag's default value from true to false |  Major | mrv2 | Benjamin Teke | Benjamin Teke |
| [HADOOP-18895](https://issues.apache.org/jira/browse/HADOOP-18895) | upgrade to commons-compress 1.24.0 due to CVE |  Major | build | PJ Fanning | PJ Fanning |
| [HADOOP-19040](https://issues.apache.org/jira/browse/HADOOP-19040) | mvn site commands fails due to MetricsSystem And MetricsSystemImpl changes. |  Major | build | Shilun Fan | Shilun Fan |
| [HADOOP-19031](https://issues.apache.org/jira/browse/HADOOP-19031) | Enhance access control for RunJar |  Major | security | Xiaoqiao He | Xiaoqiao He |
| [HADOOP-19038](https://issues.apache.org/jira/browse/HADOOP-19038) | Improve create-release RUN script |  Major | build | Shilun Fan | Shilun Fan |
| [HDFS-17343](https://issues.apache.org/jira/browse/HDFS-17343) | Revert HDFS-16016. BPServiceActor to provide new thread to handle IBR |  Major | namenode | Shilun Fan | Shilun Fan |
| [HADOOP-19039](https://issues.apache.org/jira/browse/HADOOP-19039) | Hadoop 3.4.0 Highlight big features and improvements. |  Major | common | Shilun Fan | Shilun Fan |
| [YARN-10888](https://issues.apache.org/jira/browse/YARN-10888) | [Umbrella] New capacity modes for CS |  Major | capacity scheduler | Szilard Nemeth | Benjamin Teke |
| [HADOOP-19051](https://issues.apache.org/jira/browse/HADOOP-19051) | Hadoop 3.4.0 Big feature/improvement highlight addendum |  Major | common | Benjamin Teke | Benjamin Teke |
| [YARN-10889](https://issues.apache.org/jira/browse/YARN-10889) | [Umbrella] Queue Creation in Capacity Scheduler - Tech debts |  Major | capacity scheduler | Szilard Nemeth | Benjamin Teke |
| [HDFS-17359](https://issues.apache.org/jira/browse/HDFS-17359) | EC: recheck failed streamers should only after flushing all packets. |  Minor | ec | farmmamba | farmmamba |
| [HADOOP-18987](https://issues.apache.org/jira/browse/HADOOP-18987) | Corrections to Hadoop FileSystem API Definition |  Minor | documentation | Dieter De Paepe | Dieter De Paepe |
| [HADOOP-18993](https://issues.apache.org/jira/browse/HADOOP-18993) | S3A: Add option fs.s3a.classloader.isolation (#6301) |  Minor | fs/s3 | Antonio Murgia | Antonio Murgia |
| [HADOOP-19059](https://issues.apache.org/jira/browse/HADOOP-19059) | S3A: update AWS SDK to 2.23.19 to support S3 Access Grants |  Minor | build, fs/s3 | Jason Han | Jason Han |
| [HADOOP-18930](https://issues.apache.org/jira/browse/HADOOP-18930) | S3A: make fs.s3a.create.performance an option you can set for the entire bucket |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-19065](https://issues.apache.org/jira/browse/HADOOP-19065) | Update Protocol Buffers installation to 3.21.12 |  Major | build | Zhaobo Huang | Zhaobo Huang |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-15196](https://issues.apache.org/jira/browse/HDFS-15196) | RBF: RouterRpcServer getListing cannot list large dirs correctly |  Critical | rbf | Fengnan Li | Fengnan Li |
| [HDFS-15252](https://issues.apache.org/jira/browse/HDFS-15252) | HttpFS: setWorkingDirectory should not accept invalid paths |  Major | httpfs | Hemanth Boyina | Hemanth Boyina |
| [HDFS-15256](https://issues.apache.org/jira/browse/HDFS-15256) | Fix typo in DataXceiverServer#run() |  Trivial | datanode | Lisheng Sun | Lisheng Sun |
| [HDFS-15249](https://issues.apache.org/jira/browse/HDFS-15249) | ThrottledAsyncChecker is not thread-safe. |  Major | federation | Toshihiro Suzuki | Toshihiro Suzuki |
| [HDFS-15263](https://issues.apache.org/jira/browse/HDFS-15263) | Fix the logic of scope and excluded scope in Network Topology |  Major | net | Ayush Saxena | Ayush Saxena |
| [YARN-10207](https://issues.apache.org/jira/browse/YARN-10207) | CLOSE\_WAIT socket connection leaks during rendering of (corrupted) aggregated logs on the JobHistoryServer Web UI |  Major | yarn | Siddharth Ahuja | Siddharth Ahuja |
| [YARN-10226](https://issues.apache.org/jira/browse/YARN-10226) | NPE in Capacity Scheduler while using %primary\_group queue mapping |  Critical | capacity scheduler | Peter Bacsko | Peter Bacsko |
| [HDFS-15269](https://issues.apache.org/jira/browse/HDFS-15269) | NameNode should check the authorization API version only once during initialization |  Blocker | namenode | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-16962](https://issues.apache.org/jira/browse/HADOOP-16962) | Making \`getBoolean\` log warning message for unrecognized value |  Major | conf | Ctest | Ctest |
| [HADOOP-16967](https://issues.apache.org/jira/browse/HADOOP-16967) | TestSequenceFile#testRecursiveSeqFileCreate fails in subsequent run |  Minor | common, test | Ctest | Ctest |
| [MAPREDUCE-7272](https://issues.apache.org/jira/browse/MAPREDUCE-7272) | TaskAttemptListenerImpl excessive log messages |  Major | test | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-16958](https://issues.apache.org/jira/browse/HADOOP-16958) | NPE when hadoop.security.authorization is enabled but the input PolicyProvider for ZKFCRpcServer is NULL |  Critical | common, ha | Ctest | Ctest |
| [YARN-10219](https://issues.apache.org/jira/browse/YARN-10219) | YARN service placement constraints is broken |  Blocker | yarn | Eric Yang | Eric Yang |
| [YARN-10233](https://issues.apache.org/jira/browse/YARN-10233) | [YARN UI2] No Logs were found in "YARN Daemon Logs" page |  Blocker | yarn-ui-v2 | Akhil PB | Akhil PB |
| [MAPREDUCE-7273](https://issues.apache.org/jira/browse/MAPREDUCE-7273) | JHS: make sure that Kerberos relogin is performed when KDC becomes offline then online again |  Major | jobhistoryserver | Peter Bacsko | Peter Bacsko |
| [HDFS-15266](https://issues.apache.org/jira/browse/HDFS-15266) | Add missing DFSOps Statistics in WebHDFS |  Major | webhdfs | Ayush Saxena | Ayush Saxena |
| [HDFS-15218](https://issues.apache.org/jira/browse/HDFS-15218) | RBF: MountTableRefresherService failed to refresh other router MountTableEntries in secure mode. |  Major | rbf | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HADOOP-16971](https://issues.apache.org/jira/browse/HADOOP-16971) | TestFileContextResolveAfs#testFileContextResolveAfs creates dangling link and fails for subsequent runs |  Minor | common, fs, test | Ctest | Ctest |
| [HDFS-15275](https://issues.apache.org/jira/browse/HDFS-15275) | HttpFS: Response of Create was not correct with noredirect and data are true |  Major | httpfs | Hemanth Boyina | Hemanth Boyina |
| [HDFS-15276](https://issues.apache.org/jira/browse/HDFS-15276) | Concat on INodeRefernce fails with illegal state exception |  Critical | namanode | Hemanth Boyina | Hemanth Boyina |
| [HDFS-15281](https://issues.apache.org/jira/browse/HDFS-15281) | ZKFC ignores dfs.namenode.rpc-bind-host and uses dfs.namenode.rpc-address to bind to host address |  Major | ha, namenode | Dhiraj Hegde | Dhiraj Hegde |
| [HDFS-15297](https://issues.apache.org/jira/browse/HDFS-15297) | TestNNHandlesBlockReportPerStorage::blockReport\_02 fails intermittently in trunk |  Major | datanode, test | Mingliang Liu | Ayush Saxena |
| [HDFS-15298](https://issues.apache.org/jira/browse/HDFS-15298) | Fix the findbugs warnings introduced in HDFS-15217 |  Major | namanode | Toshihiro Suzuki | Toshihiro Suzuki |
| [HDFS-15301](https://issues.apache.org/jira/browse/HDFS-15301) | statfs function in hdfs-fuse is not working |  Major | fuse-dfs, libhdfs | Aryan Gupta | Aryan Gupta |
| [HDFS-15210](https://issues.apache.org/jira/browse/HDFS-15210) | EC : File write hanged when DN is shutdown by admin command. |  Major | ec | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-15285](https://issues.apache.org/jira/browse/HDFS-15285) | The same distance and load nodes don't shuffle when consider DataNode load |  Major | datanode | Lisheng Sun | Lisheng Sun |
| [HDFS-15265](https://issues.apache.org/jira/browse/HDFS-15265) | HttpFS: validate content-type in HttpFSUtils |  Major | httpfs | Hemanth Boyina | Hemanth Boyina |
| [HDFS-15309](https://issues.apache.org/jira/browse/HDFS-15309) | Remove redundant String.valueOf method on ExtendedBlockId.java |  Trivial | hdfs-client | bianqi | bianqi |
| [HADOOP-16957](https://issues.apache.org/jira/browse/HADOOP-16957) | NodeBase.normalize doesn't removing all trailing slashes. |  Major | net | Ayush Saxena | Ayush Saxena |
| [HADOOP-17011](https://issues.apache.org/jira/browse/HADOOP-17011) | Tolerate leading and trailing spaces in fs.defaultFS |  Major | common | Ctest | Ctest |
| [HDFS-15320](https://issues.apache.org/jira/browse/HDFS-15320) | StringIndexOutOfBoundsException in HostRestrictingAuthorizationFilter |  Major | webhdfs | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15325](https://issues.apache.org/jira/browse/HDFS-15325) | TestRefreshCallQueue is failing due to changed CallQueue constructor |  Major | test | Konstantin Shvachko | Fengnan Li |
| [YARN-10256](https://issues.apache.org/jira/browse/YARN-10256) | Refactor TestContainerSchedulerQueuing.testContainerUpdateExecTypeGuaranteedToOpportunistic |  Major | test | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15270](https://issues.apache.org/jira/browse/HDFS-15270) | Account for \*env == NULL in hdfsThreadDestructor |  Major | libhdfs | Babneet Singh | Babneet Singh |
| [HDFS-15331](https://issues.apache.org/jira/browse/HDFS-15331) | Remove invalid exclusions that minicluster dependency on HDFS |  Major | build | Wanqiang Ji | Wanqiang Ji |
| [YARN-8959](https://issues.apache.org/jira/browse/YARN-8959) | TestContainerResizing fails randomly |  Minor | test | Bibin Chundatt | Ahmed Hussein |
| [HDFS-15332](https://issues.apache.org/jira/browse/HDFS-15332) | Quota Space consumed was wrong in truncate with Snapshots |  Major | qouta, snapshots | Hemanth Boyina | Hemanth Boyina |
| [YARN-9017](https://issues.apache.org/jira/browse/YARN-9017) | PlacementRule order is not maintained in CS |  Major | capacity scheduler | Bibin Chundatt | Bilwa S T |
| [HDFS-15323](https://issues.apache.org/jira/browse/HDFS-15323) | StandbyNode fails transition to active due to insufficient transaction tailing |  Major | namenode, qjm | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-17025](https://issues.apache.org/jira/browse/HADOOP-17025) | Fix invalid metastore configuration in S3GuardTool tests |  Minor | fs/s3, test | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-15339](https://issues.apache.org/jira/browse/HDFS-15339) | TestHDFSCLI fails for user names with the dot/dash character |  Major | test | Yan Xiaole | Yan Xiaole |
| [HDFS-15250](https://issues.apache.org/jira/browse/HDFS-15250) | Setting \`dfs.client.use.datanode.hostname\` to true can crash the system because of unhandled UnresolvedAddressException |  Major | hdfs-client | Ctest | Ctest |
| [HADOOP-16768](https://issues.apache.org/jira/browse/HADOOP-16768) | SnappyCompressor test cases wrongly assume that the compressed data is always smaller than the input data |  Major | io, test | zhao bo | Akira Ajisaka |
| [HDFS-15243](https://issues.apache.org/jira/browse/HDFS-15243) | Add an option to prevent sub-directories of protected directories from deletion |  Major | namenode | liuyanyu | liuyanyu |
| [HDFS-14367](https://issues.apache.org/jira/browse/HDFS-14367) | EC: Parameter maxPoolSize in striped reconstruct thread pool isn't affecting number of threads |  Major | ec | Guo Lei | Guo Lei |
| [YARN-9301](https://issues.apache.org/jira/browse/YARN-9301) | Too many InvalidStateTransitionException with SLS |  Major | scheduler-load-simulator | Bibin Chundatt | Bilwa S T |
| [HADOOP-17035](https://issues.apache.org/jira/browse/HADOOP-17035) | Trivial typo(s) which are 'timout', 'interruped' in comment, LOG and documents |  Trivial | documentation | Sungpeo Kook | Sungpeo Kook |
| [HDFS-15300](https://issues.apache.org/jira/browse/HDFS-15300) | RBF: updateActiveNamenode() is invalid when RPC address is IP |  Major | rbf | ZanderXu | ZanderXu |
| [HADOOP-15524](https://issues.apache.org/jira/browse/HADOOP-15524) | BytesWritable causes OOME when array size reaches Integer.MAX\_VALUE |  Major | io | Joseph Smith | Joseph Smith |
| [YARN-10154](https://issues.apache.org/jira/browse/YARN-10154) | CS Dynamic Queues cannot be configured with absolute resources |  Major | capacity scheduler | Sunil G | Manikandan R |
| [HDFS-15316](https://issues.apache.org/jira/browse/HDFS-15316) | Deletion failure should not remove directory from snapshottables |  Major | namanode | Hemanth Boyina | Hemanth Boyina |
| [YARN-9898](https://issues.apache.org/jira/browse/YARN-9898) | Dependency netty-all-4.1.27.Final doesn't support ARM platform |  Major | buid | liusheng | liusheng |
| [YARN-10265](https://issues.apache.org/jira/browse/YARN-10265) | Upgrade Netty-all dependency to latest version 4.1.50 to fix ARM support issue |  Major | buid | liusheng | liusheng |
| [YARN-9444](https://issues.apache.org/jira/browse/YARN-9444) | YARN API ResourceUtils's getRequestedResourcesFromConfig doesn't recognize yarn.io/gpu as a valid resource |  Minor | api | Gergely Pollák | Gergely Pollák |
| [HDFS-15293](https://issues.apache.org/jira/browse/HDFS-15293) | Relax the condition for accepting a fsimage when receiving a checkpoint |  Critical | namenode | Chen Liang | Chen Liang |
| [HADOOP-17024](https://issues.apache.org/jira/browse/HADOOP-17024) | ListStatus on ViewFS root (ls "/") should list the linkFallBack root (configured target root). |  Major | fs, viewfs | Uma Maheswara Rao G | Abhishek Das |
| [MAPREDUCE-6826](https://issues.apache.org/jira/browse/MAPREDUCE-6826) | Job fails with InvalidStateTransitonException: Invalid event: JOB\_TASK\_COMPLETED at SUCCEEDED/COMMITTING |  Major | mrv2 | Varun Saxena | Bilwa S T |
| [HADOOP-16586](https://issues.apache.org/jira/browse/HADOOP-16586) | ITestS3GuardFsck, others fails when run using a local metastore |  Major | fs/s3 | Siddharth Seth | Masatake Iwasaki |
| [HADOOP-16900](https://issues.apache.org/jira/browse/HADOOP-16900) | Very large files can be truncated when written through S3AFileSystem |  Major | fs/s3 | Andrew Olson | Mukund Thakur |
| [YARN-10228](https://issues.apache.org/jira/browse/YARN-10228) | Yarn Service fails if am java opts contains ZK authentication file path |  Major | yarn | Bilwa S T | Bilwa S T |
| [HADOOP-17049](https://issues.apache.org/jira/browse/HADOOP-17049) | javax.activation-api and jakarta.activation-api define overlapping classes |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17040](https://issues.apache.org/jira/browse/HADOOP-17040) | Fix intermittent failure of ITestBlockingThreadPoolExecutorService |  Minor | fs/s3, test | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-15363](https://issues.apache.org/jira/browse/HDFS-15363) | BlockPlacementPolicyWithNodeGroup should validate if it is initialized by NetworkTopologyWithNodeGroup |  Major | block placement | Hemanth Boyina | Hemanth Boyina |
| [HDFS-15093](https://issues.apache.org/jira/browse/HDFS-15093) | RENAME.TO\_TRASH is ignored When RENAME.OVERWRITE is specified |  Major | hdfs-client | Harshakiran Reddy | Ayush Saxena |
| [HDFS-12288](https://issues.apache.org/jira/browse/HDFS-12288) | Fix DataNode's xceiver count calculation |  Major | datanode, hdfs | Lukas Majercak | Lisheng Sun |
| [HDFS-15373](https://issues.apache.org/jira/browse/HDFS-15373) | Fix number of threads in IPCLoggerChannel#createParallelExecutor |  Major | journal-node | Ayush Saxena | Ayush Saxena |
| [HDFS-15362](https://issues.apache.org/jira/browse/HDFS-15362) | FileWithSnapshotFeature#updateQuotaAndCollectBlocks should collect all distinct blocks |  Major | snapshots | Hemanth Boyina | Hemanth Boyina |
| [MAPREDUCE-7278](https://issues.apache.org/jira/browse/MAPREDUCE-7278) | Speculative execution behavior is observed even when mapreduce.map.speculative and mapreduce.reduce.speculative are false |  Major | task | Tarun Parimi | Tarun Parimi |
| [HADOOP-7002](https://issues.apache.org/jira/browse/HADOOP-7002) | Wrong description of copyFromLocal and copyToLocal in documentation |  Minor | documentation | Jingguo Yao | Andras Bokor |
| [HADOOP-17052](https://issues.apache.org/jira/browse/HADOOP-17052) | NetUtils.connect() throws unchecked exception (UnresolvedAddressException) causing clients to abort |  Major | net | Dhiraj Hegde | Dhiraj Hegde |
| [YARN-10254](https://issues.apache.org/jira/browse/YARN-10254) | CapacityScheduler incorrect User Group Mapping after leaf queue change |  Major | capacity scheduler | Gergely Pollák | Gergely Pollák |
| [HADOOP-17062](https://issues.apache.org/jira/browse/HADOOP-17062) | Fix shelldocs path in Jenkinsfile |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17056](https://issues.apache.org/jira/browse/HADOOP-17056) | shelldoc fails in hadoop-common |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [YARN-10286](https://issues.apache.org/jira/browse/YARN-10286) | PendingContainers bugs in the scheduler outputs |  Critical | resourcemanager | Adam Antal | Andras Gyori |
| [HDFS-15396](https://issues.apache.org/jira/browse/HDFS-15396) | Fix TestViewFileSystemOverloadSchemeHdfsFileSystemContract#testListStatusRootDir |  Major | test | Ayush Saxena | Ayush Saxena |
| [HDFS-15386](https://issues.apache.org/jira/browse/HDFS-15386) | ReplicaNotFoundException keeps happening in DN after removing multiple DN's data directories |  Major | datanode | Toshihiro Suzuki | Toshihiro Suzuki |
| [HDFS-15398](https://issues.apache.org/jira/browse/HDFS-15398) | EC: hdfs client hangs due to exception during addBlock |  Critical | ec, hdfs-client | Hongbing Wang | Hongbing Wang |
| [YARN-10300](https://issues.apache.org/jira/browse/YARN-10300) | appMasterHost not set in RM ApplicationSummary when AM fails before first heartbeat |  Major | am | Eric Badger | Eric Badger |
| [HADOOP-17059](https://issues.apache.org/jira/browse/HADOOP-17059) | ArrayIndexOfboundsException in ViewFileSystem#listStatus |  Major | viewfs | Hemanth Boyina | Hemanth Boyina |
| [YARN-10296](https://issues.apache.org/jira/browse/YARN-10296) | Make ContainerPBImpl#getId/setId synchronized |  Minor | yarn-common | Benjamin Teke | Benjamin Teke |
| [HADOOP-17060](https://issues.apache.org/jira/browse/HADOOP-17060) | listStatus and getFileStatus behave inconsistent in the case of ViewFs implementation for isDirectory |  Major | viewfs | Srinivasu Majeti | Uma Maheswara Rao G |
| [YARN-10312](https://issues.apache.org/jira/browse/YARN-10312) | Add support for yarn logs -logFile to retain backward compatibility |  Major | client | Jim Brennan | Jim Brennan |
| [HDFS-15351](https://issues.apache.org/jira/browse/HDFS-15351) | Blocks scheduled count was wrong on truncate |  Major | block placement | Hemanth Boyina | Hemanth Boyina |
| [HDFS-15403](https://issues.apache.org/jira/browse/HDFS-15403) | NPE in FileIoProvider#transferToSocketFully |  Major | datanode | Hemanth Boyina | Hemanth Boyina |
| [HDFS-15372](https://issues.apache.org/jira/browse/HDFS-15372) | Files in snapshots no longer see attribute provider permissions |  Major | snapshots | Stephen O'Donnell | Stephen O'Donnell |
| [HADOOP-9851](https://issues.apache.org/jira/browse/HADOOP-9851) | dfs -chown does not like "+" plus sign in user name |  Minor | fs | Marc Villacorta | Andras Bokor |
| [YARN-10308](https://issues.apache.org/jira/browse/YARN-10308) | Update javadoc and variable names for keytab in yarn services as it supports filesystems other than hdfs and local file system |  Minor | documentation | Bilwa S T | Bilwa S T |
| [MAPREDUCE-7281](https://issues.apache.org/jira/browse/MAPREDUCE-7281) | Fix NoClassDefFoundError on 'mapred minicluster' |  Major | scripts | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-17029](https://issues.apache.org/jira/browse/HADOOP-17029) | ViewFS does not return correct user/group and ACL |  Major | fs, viewfs | Abhishek Das | Abhishek Das |
| [HDFS-14546](https://issues.apache.org/jira/browse/HDFS-14546) | Document block placement policies |  Major | documentation | Íñigo Goiri | Amithsha |
| [HADOOP-17068](https://issues.apache.org/jira/browse/HADOOP-17068) | client fails forever when namenode ipaddr changed |  Major | hdfs-client | Sean Chow | Sean Chow |
| [HDFS-15378](https://issues.apache.org/jira/browse/HDFS-15378) | TestReconstructStripedFile#testErasureCodingWorkerXmitsWeight is failing on trunk |  Major | test | Hemanth Boyina | Hemanth Boyina |
| [YARN-10328](https://issues.apache.org/jira/browse/YARN-10328) | Too many ZK Curator NodeExists exception logs in YARN Service AM logs |  Major | yarn | Bilwa S T | Bilwa S T |
| [YARN-9903](https://issues.apache.org/jira/browse/YARN-9903) | Support reservations continue looking for Node Labels |  Major | capacity scheduler | Tarun Parimi | Jim Brennan |
| [YARN-10331](https://issues.apache.org/jira/browse/YARN-10331) | Upgrade node.js to 10.21.0 |  Critical | build, yarn-ui-v2 | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17032](https://issues.apache.org/jira/browse/HADOOP-17032) | Handle an internal dir in viewfs having multiple children mount points pointing to different filesystems |  Major | fs, viewfs | Abhishek Das | Abhishek Das |
| [YARN-10318](https://issues.apache.org/jira/browse/YARN-10318) | ApplicationHistory Web UI incorrect column indexing |  Minor | yarn | Andras Gyori | Andras Gyori |
| [YARN-10330](https://issues.apache.org/jira/browse/YARN-10330) | Add missing test scenarios to TestUserGroupMappingPlacementRule and TestAppNameMappingPlacementRule |  Major | capacity scheduler, capacityscheduler, test | Peter Bacsko | Peter Bacsko |
| [HDFS-15446](https://issues.apache.org/jira/browse/HDFS-15446) | CreateSnapshotOp fails during edit log loading for /.reserved/raw/path with error java.io.FileNotFoundException: Directory does not exist: /.reserved/raw/path |  Major | hdfs | Srinivasu Majeti | Stephen O'Donnell |
| [HDFS-15451](https://issues.apache.org/jira/browse/HDFS-15451) | Restarting name node stuck in safe mode when using provided storage |  Major | namenode | shanyu zhao | shanyu zhao |
| [HADOOP-17117](https://issues.apache.org/jira/browse/HADOOP-17117) | Fix typos in hadoop-aws documentation |  Trivial | documentation, fs/s3 | Sebastian Nagel | Sebastian Nagel |
| [YARN-10344](https://issues.apache.org/jira/browse/YARN-10344) | Sync netty versions in hadoop-yarn-csi |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [YARN-10341](https://issues.apache.org/jira/browse/YARN-10341) | Yarn Service Container Completed event doesn't get processed |  Critical | service-scheduler | Bilwa S T | Bilwa S T |
| [HADOOP-17116](https://issues.apache.org/jira/browse/HADOOP-17116) | Skip Retry INFO logging on first failover from a proxy |  Major | ha | Hanisha Koneru | Hanisha Koneru |
| [HADOOP-16998](https://issues.apache.org/jira/browse/HADOOP-16998) | WASB : NativeAzureFsOutputStream#close() throwing IllegalArgumentException |  Major | fs/azure | Anoop Sam John | Anoop Sam John |
| [YARN-10348](https://issues.apache.org/jira/browse/YARN-10348) | Allow RM to always cancel tokens after app completes |  Major | yarn | Jim Brennan | Jim Brennan |
| [MAPREDUCE-7284](https://issues.apache.org/jira/browse/MAPREDUCE-7284) | TestCombineFileInputFormat#testMissingBlocks fails |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [YARN-10350](https://issues.apache.org/jira/browse/YARN-10350) | TestUserGroupMappingPlacementRule fails |  Major | test | Akira Ajisaka | Bilwa S T |
| [MAPREDUCE-7285](https://issues.apache.org/jira/browse/MAPREDUCE-7285) | Junit class missing from hadoop-mapreduce-client-jobclient-\*-tests jar |  Major | test | Eric Badger | Masatake Iwasaki |
| [HDFS-14498](https://issues.apache.org/jira/browse/HDFS-14498) | LeaseManager can loop forever on the file for which create has failed |  Major | namenode | Sergey Shelukhin | Stephen O'Donnell |
| [YARN-10339](https://issues.apache.org/jira/browse/YARN-10339) | Timeline Client in Nodemanager gets 403 errors when simple auth is used in kerberos environments |  Major | timelineclient | Tarun Parimi | Tarun Parimi |
| [HDFS-15198](https://issues.apache.org/jira/browse/HDFS-15198) | RBF: Add test for MountTableRefresherService failed to refresh other router MountTableEntries in secure mode |  Major | rbf | Chenyu Zheng | Chenyu Zheng |
| [HADOOP-17119](https://issues.apache.org/jira/browse/HADOOP-17119) | Jetty upgrade to 9.4.x causes MR app fail with IOException |  Major | common | Bilwa S T | Bilwa S T |
| [HDFS-15246](https://issues.apache.org/jira/browse/HDFS-15246) | ArrayIndexOfboundsException in BlockManager CreateLocatedBlock |  Major | namanode | Hemanth Boyina | Hemanth Boyina |
| [HADOOP-17138](https://issues.apache.org/jira/browse/HADOOP-17138) | Fix spotbugs warnings surfaced after upgrade to 4.0.6 |  Minor | build | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-4771](https://issues.apache.org/jira/browse/YARN-4771) | Some containers can be skipped during log aggregation after NM restart |  Major | nodemanager | Jason Darrell Lowe | Jim Brennan |
| [HADOOP-17153](https://issues.apache.org/jira/browse/HADOOP-17153) | Add boost installation steps to build instruction on CentOS 8 |  Major | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-10367](https://issues.apache.org/jira/browse/YARN-10367) | Failed to get nodejs 10.21.0 when building docker image |  Blocker | build, webapp | Akira Ajisaka | Akira Ajisaka |
| [YARN-10362](https://issues.apache.org/jira/browse/YARN-10362) | Javadoc for TimelineReaderAuthenticationFilterInitializer is broken |  Minor | documentation | Xieming Li | Xieming Li |
| [YARN-10366](https://issues.apache.org/jira/browse/YARN-10366) | Yarn rmadmin help message shows two labels for one node for --replaceLabelsOnNode |  Major | yarn | Tanu Ajmera | Tanu Ajmera |
| [MAPREDUCE-7051](https://issues.apache.org/jira/browse/MAPREDUCE-7051) | Fix typo in MultipleOutputFormat |  Trivial | mrv1 | ywheel | ywheel |
| [HDFS-15313](https://issues.apache.org/jira/browse/HDFS-15313) | Ensure inodes in active filesystem are not deleted during snapshot delete |  Major | snapshots | Shashikant Banerjee | Shashikant Banerjee |
| [HDFS-14950](https://issues.apache.org/jira/browse/HDFS-14950) | missing libhdfspp libs in dist-package |  Major | build, libhdfs++ | Yuan Zhou | Yuan Zhou |
| [YARN-10359](https://issues.apache.org/jira/browse/YARN-10359) | Log container report only if list is not empty |  Minor | nodemanager | Bilwa S T | Bilwa S T |
| [HDFS-15229](https://issues.apache.org/jira/browse/HDFS-15229) |  Truncate info should be logged at INFO level |  Major | . | Ravuri Sushma sree | Ravuri Sushma sree |
| [HDFS-15503](https://issues.apache.org/jira/browse/HDFS-15503) | File and directory permissions are not able to be modified from WebUI |  Major | ui | Hemanth Boyina | Hemanth Boyina |
| [YARN-10383](https://issues.apache.org/jira/browse/YARN-10383) | YarnCommands.md is inconsistent with the source code |  Minor | documentation | zhaoshengjie | zhaoshengjie |
| [YARN-10377](https://issues.apache.org/jira/browse/YARN-10377) | Clicking on queue in Capacity Scheduler legacy ui does not show any applications |  Major | ui | Tarun Parimi | Tarun Parimi |
| [HADOOP-17184](https://issues.apache.org/jira/browse/HADOOP-17184) | Add --mvn-custom-repos parameter to yetus calls |  Major | build | Mingliang Liu | Mingliang Liu |
| [HDFS-15499](https://issues.apache.org/jira/browse/HDFS-15499) | Clean up httpfs/pom.xml to remove aws-java-sdk-s3 exclusion |  Major | httpfs | Mingliang Liu | Mingliang Liu |
| [HADOOP-17186](https://issues.apache.org/jira/browse/HADOOP-17186) | Fixing javadoc in ListingOperationCallbacks |  Major | build, documentation | Akira Ajisaka | Mukund Thakur |
| [HADOOP-17164](https://issues.apache.org/jira/browse/HADOOP-17164) | UGI loginUserFromKeytab doesn't set the last login time |  Major | security | Sandeep Guggilam | Sandeep Guggilam |
| [YARN-4575](https://issues.apache.org/jira/browse/YARN-4575) | ApplicationResourceUsageReport should return ALL  reserved resource |  Major | scheduler | Bibin Chundatt | Bibin Chundatt |
| [YARN-10388](https://issues.apache.org/jira/browse/YARN-10388) | RMNode updatedCapability flag not set while RecommissionNodeTransition |  Major | resourcemanager | Pranjal Protim Borah | Pranjal Protim Borah |
| [HADOOP-17182](https://issues.apache.org/jira/browse/HADOOP-17182) | Dead links in breadcrumbs |  Major | documentation | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15443](https://issues.apache.org/jira/browse/HDFS-15443) | Setting dfs.datanode.max.transfer.threads to a very small value can cause strange failure. |  Major | datanode | AMC-team | AMC-team |
| [YARN-10364](https://issues.apache.org/jira/browse/YARN-10364) | Absolute Resource [memory=0] is considered as Percentage config type |  Major | capacity scheduler | Prabhu Joseph | Bilwa S T |
| [HDFS-15508](https://issues.apache.org/jira/browse/HDFS-15508) | [JDK 11] Fix javadoc errors in hadoop-hdfs-rbf module |  Major | documentation | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15506](https://issues.apache.org/jira/browse/HDFS-15506) | [JDK 11] Fix javadoc errors in hadoop-hdfs module |  Major | documentation | Akira Ajisaka | Xieming Li |
| [HDFS-15507](https://issues.apache.org/jira/browse/HDFS-15507) | [JDK 11] Fix javadoc errors in hadoop-hdfs-client module |  Major | documentation | Akira Ajisaka | Xieming Li |
| [HADOOP-17196](https://issues.apache.org/jira/browse/HADOOP-17196) | Fix C/C++ standard warnings |  Major | build | Gautham Banasandra | Gautham Banasandra |
| [HDFS-15523](https://issues.apache.org/jira/browse/HDFS-15523) | Fix the findbugs warnings from HDFS-15520 |  Major | namenode | Tsz-wo Sze | Tsz-wo Sze |
| [HADOOP-17204](https://issues.apache.org/jira/browse/HADOOP-17204) | Fix typo in Hadoop KMS document |  Trivial | documentation, kms | Akira Ajisaka | Xieming Li |
| [YARN-10336](https://issues.apache.org/jira/browse/YARN-10336) | RM page should throw exception when command injected in RM REST API to get applications |  Major | webapp | Rajshree Mishra | Bilwa S T |
| [HDFS-15439](https://issues.apache.org/jira/browse/HDFS-15439) | Setting dfs.mover.retry.max.attempts to negative value will retry forever. |  Major | balancer & mover | AMC-team | AMC-team |
| [YARN-10391](https://issues.apache.org/jira/browse/YARN-10391) | --module-gpu functionality is broken in container-executor |  Major | nodemanager | Eric Badger | Eric Badger |
| [HDFS-15535](https://issues.apache.org/jira/browse/HDFS-15535) | RBF: Fix Namespace path to snapshot path resolution for snapshot API |  Major | rbf | Ayush Saxena | Ayush Saxena |
| [HDFS-14504](https://issues.apache.org/jira/browse/HDFS-14504) | Rename with Snapshots does not honor quota limit |  Major | snapshots | Shashikant Banerjee | Hemanth Boyina |
| [HADOOP-17209](https://issues.apache.org/jira/browse/HADOOP-17209) | Erasure Coding: Native library memory leak |  Major | native | Sean Chow | Sean Chow |
| [HADOOP-17220](https://issues.apache.org/jira/browse/HADOOP-17220) | Upgrade slf4j to 1.7.30 ( To Address: CVE-2018-8088) |  Major | build, common | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-14852](https://issues.apache.org/jira/browse/HDFS-14852) | Removing from LowRedundancyBlocks does not remove the block from all queues |  Major | namenode | Hui Fei | Hui Fei |
| [HDFS-15536](https://issues.apache.org/jira/browse/HDFS-15536) | RBF:  Clear Quota in Router was not consistent |  Critical | rbf | Hemanth Boyina | Hemanth Boyina |
| [HDFS-15510](https://issues.apache.org/jira/browse/HDFS-15510) | RBF: Quota and Content Summary was not correct in Multiple Destinations |  Critical | rbf | Hemanth Boyina | Hemanth Boyina |
| [HDFS-15540](https://issues.apache.org/jira/browse/HDFS-15540) | Directories protected from delete can still be moved to the trash |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-15471](https://issues.apache.org/jira/browse/HDFS-15471) | TestHDFSContractMultipartUploader fails on trunk |  Major | test | Ahmed Hussein | Steve Loughran |
| [HDFS-15290](https://issues.apache.org/jira/browse/HDFS-15290) | NPE in HttpServer during NameNode startup |  Major | namenode | Konstantin Shvachko | Simbarashe Dzinamarira |
| [HADOOP-17240](https://issues.apache.org/jira/browse/HADOOP-17240) | Fix wrong command line for setting up CentOS 8 |  Minor | documentation | Masatake Iwasaki | Takeru Kuramoto |
| [YARN-10419](https://issues.apache.org/jira/browse/YARN-10419) | Javadoc error in hadoop-yarn-server-common module |  Major | build, documentation | Akira Ajisaka | Masatake Iwasaki |
| [YARN-10416](https://issues.apache.org/jira/browse/YARN-10416) | Typos in YarnScheduler#allocate method's doc comment |  Minor | docs | Wanqiang Ji | Siddharth Ahuja |
| [HADOOP-17245](https://issues.apache.org/jira/browse/HADOOP-17245) | Add RootedOzFS AbstractFileSystem to core-default.xml |  Major | fs | Bharat Viswanadham | Bharat Viswanadham |
| [HADOOP-17158](https://issues.apache.org/jira/browse/HADOOP-17158) | Test timeout for ITestAbfsInputStreamStatistics#testReadAheadCounters |  Major | fs/azure | Mehakmeet Singh | Mehakmeet Singh |
| [YARN-10397](https://issues.apache.org/jira/browse/YARN-10397) | SchedulerRequest should be forwarded to scheduler if custom scheduler supports placement constraints |  Minor | capacity scheduler | Bilwa S T | Bilwa S T |
| [HDFS-15573](https://issues.apache.org/jira/browse/HDFS-15573) | Only log warning if considerLoad and considerStorageType are both true |  Major | hdfs | Stephen O'Donnell | Stephen O'Donnell |
| [YARN-10430](https://issues.apache.org/jira/browse/YARN-10430) | Log improvements in NodeStatusUpdaterImpl |  Minor | nodemanager | Bilwa S T | Bilwa S T |
| [HADOOP-17262](https://issues.apache.org/jira/browse/HADOOP-17262) | Switch to Yetus main branch |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17246](https://issues.apache.org/jira/browse/HADOOP-17246) | Fix build the hadoop-build Docker image failed |  Major | build | Wanqiang Ji | Wanqiang Ji |
| [HDFS-15438](https://issues.apache.org/jira/browse/HDFS-15438) | Setting dfs.disk.balancer.max.disk.errors = 0 will fail the block copy |  Major | balancer & mover | AMC-team | AMC-team |
| [HADOOP-17203](https://issues.apache.org/jira/browse/HADOOP-17203) | Test failures in ITestAzureBlobFileSystemCheckAccess in ABFS |  Major | fs/azure | Mehakmeet Singh | Thomas Marqardt |
| [MAPREDUCE-7294](https://issues.apache.org/jira/browse/MAPREDUCE-7294) | Only application master should upload resource to Yarn Shared Cache |  Major | mrv2 | zhenzhao wang | zhenzhao wang |
| [HADOOP-17277](https://issues.apache.org/jira/browse/HADOOP-17277) | Correct spelling errors for separator |  Trivial | common | Hui Fei | Hui Fei |
| [YARN-10443](https://issues.apache.org/jira/browse/YARN-10443) | Document options of logs CLI |  Major | yarn | Adam Antal | Ankit Kumar |
| [YARN-10438](https://issues.apache.org/jira/browse/YARN-10438) | Handle null containerId in ClientRMService#getContainerReport() |  Major | resourcemanager | Raghvendra Singh | Shubham Gupta |
| [HADOOP-17286](https://issues.apache.org/jira/browse/HADOOP-17286) | Upgrade to jQuery 3.5.1 in hadoop-yarn-common |  Major | build, common | Wei-Chiu Chuang | Aryan Gupta |
| [HDFS-15591](https://issues.apache.org/jira/browse/HDFS-15591) | RBF: Fix webHdfs file display error |  Major | rbf, webhdfs | Zhaohui Wang | Zhaohui Wang |
| [MAPREDUCE-7289](https://issues.apache.org/jira/browse/MAPREDUCE-7289) | Fix wrong comment in LongLong.java |  Trivial | documentation, examples | Akira Ajisaka | Wanqiang Ji |
| [HDFS-15600](https://issues.apache.org/jira/browse/HDFS-15600) | TestRouterQuota fails in trunk |  Major | rbf | Ayush Saxena | huangtianhua |
| [YARN-9809](https://issues.apache.org/jira/browse/YARN-9809) | NMs should supply a health status when registering with RM |  Major | nodemanager, resourcemanager | Eric Badger | Eric Badger |
| [YARN-10447](https://issues.apache.org/jira/browse/YARN-10447) | TestLeafQueue: ActivitiesManager thread might interfere with ongoing stubbing |  Major | test | Peter Bacsko | Peter Bacsko |
| [HADOOP-17297](https://issues.apache.org/jira/browse/HADOOP-17297) | Use Yetus before YETUS-994 to enable adding comments to GitHub |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15458](https://issues.apache.org/jira/browse/HDFS-15458) | TestNameNodeRetryCacheMetrics fails intermittently |  Major | hdfs, namenode | Ahmed Hussein | Hui Fei |
| [HADOOP-17294](https://issues.apache.org/jira/browse/HADOOP-17294) | Fix typos existance to existence |  Trivial | common | Ikko Ashimine | Ikko Ashimine |
| [HDFS-15543](https://issues.apache.org/jira/browse/HDFS-15543) | RBF: Write Should allow, when a subcluster is unavailable for RANDOM mount points with fault Tolerance enabled. |  Major | rbf | Harshakiran Reddy | Hemanth Boyina |
| [YARN-10393](https://issues.apache.org/jira/browse/YARN-10393) | MR job live lock caused by completed state container leak in heartbeat between node manager and RM |  Major | nodemanager, yarn | zhenzhao wang | Jim Brennan |
| [HDFS-15253](https://issues.apache.org/jira/browse/HDFS-15253) | Set default throttle value on dfs.image.transfer.bandwidthPerSec |  Major | namenode | Karthik Palanisamy | Karthik Palanisamy |
| [HDFS-15610](https://issues.apache.org/jira/browse/HDFS-15610) | Reduce datanode upgrade/hardlink thread |  Major | datanode | Karthik Palanisamy | Karthik Palanisamy |
| [YARN-10455](https://issues.apache.org/jira/browse/YARN-10455) | TestNMProxy.testNMProxyRPCRetry is not consistent |  Major | test | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15456](https://issues.apache.org/jira/browse/HDFS-15456) | TestExternalStoragePolicySatisfier fails intermittently |  Major | test | Ahmed Hussein | Leon Gao |
| [HADOOP-17223](https://issues.apache.org/jira/browse/HADOOP-17223) | update  org.apache.httpcomponents:httpclient to 4.5.13 and httpcore to 4.4.13 |  Blocker | common | Pranav Bheda | Pranav Bheda |
| [YARN-10448](https://issues.apache.org/jira/browse/YARN-10448) | SLS should set default user to handle SYNTH format |  Major | scheduler-load-simulator | Qi Zhu | Qi Zhu |
| [HDFS-15628](https://issues.apache.org/jira/browse/HDFS-15628) | HttpFS server throws NPE if a file is a symlink |  Major | fs, httpfs | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15627](https://issues.apache.org/jira/browse/HDFS-15627) | Audit log deletes before collecting blocks |  Major | logging, namenode | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17309](https://issues.apache.org/jira/browse/HADOOP-17309) | Javadoc warnings and errors are ignored in the precommit jobs |  Major | build, documentation | Akira Ajisaka | Akira Ajisaka |
| [HDFS-14383](https://issues.apache.org/jira/browse/HDFS-14383) | Compute datanode load based on StoragePolicy |  Major | hdfs, namenode | Karthik Palanisamy | Ayush Saxena |
| [HADOOP-17310](https://issues.apache.org/jira/browse/HADOOP-17310) | Touch command with -c  option is broken |  Major | command | Ayush Saxena | Ayush Saxena |
| [HADOOP-17298](https://issues.apache.org/jira/browse/HADOOP-17298) | Backslash in username causes build failure in the environment started by start-build-env.sh. |  Minor | build | Takeru Kuramoto | Takeru Kuramoto |
| [HDFS-15639](https://issues.apache.org/jira/browse/HDFS-15639) | [JDK 11] Fix Javadoc errors in hadoop-hdfs-client |  Major | documentation | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-17315](https://issues.apache.org/jira/browse/HADOOP-17315) | Use shaded guava in ClientCache.java |  Minor | build | Akira Ajisaka | Akira Ajisaka |
| [YARN-10453](https://issues.apache.org/jira/browse/YARN-10453) | Add partition resource info to get-node-labels and label-mappings api responses |  Major | yarn | Akhil PB | Akhil PB |
| [HDFS-15622](https://issues.apache.org/jira/browse/HDFS-15622) | Deleted blocks linger in the replications queue |  Major | hdfs | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15641](https://issues.apache.org/jira/browse/HDFS-15641) | DataNode could meet deadlock if invoke refreshNameNode |  Critical | datanode | Hongbing Wang | Hongbing Wang |
| [HADOOP-17328](https://issues.apache.org/jira/browse/HADOOP-17328) | LazyPersist Overwrite fails in direct write mode |  Major | command | Ayush Saxena | Ayush Saxena |
| [HDFS-15580](https://issues.apache.org/jira/browse/HDFS-15580) | [JDK 12] DFSTestUtil#addDataNodeLayoutVersion fails |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [MAPREDUCE-7302](https://issues.apache.org/jira/browse/MAPREDUCE-7302) | Upgrading to JUnit 4.13 causes testcase TestFetcher.testCorruptedIFile() to fail |  Major | test | Peter Bacsko | Peter Bacsko |
| [HDFS-15644](https://issues.apache.org/jira/browse/HDFS-15644) | Failed volumes can cause DNs to stop block reporting |  Major | block placement, datanode | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17236](https://issues.apache.org/jira/browse/HADOOP-17236) | Bump up snakeyaml to 1.26 to mitigate CVE-2017-18640 |  Major | build, command | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-10467](https://issues.apache.org/jira/browse/YARN-10467) | ContainerIdPBImpl objects can be leaked in RMNodeImpl.completedContainers |  Major | resourcemanager | Haibo Chen | Haibo Chen |
| [HADOOP-17329](https://issues.apache.org/jira/browse/HADOOP-17329) | mvn site commands fails due to MetricsSystemImpl changes |  Major | build, common | Xiaoqiao He | Xiaoqiao He |
| [YARN-10442](https://issues.apache.org/jira/browse/YARN-10442) | RM should make sure node label file highly available |  Major | resourcemanager | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-15651](https://issues.apache.org/jira/browse/HDFS-15651) | Client could not obtain block when DN CommandProcessingThread exit |  Major | datanode | Yiqun Lin | Mingxiang Li |
| [HADOOP-17341](https://issues.apache.org/jira/browse/HADOOP-17341) | Upgrade commons-codec to 1.15 |  Minor | build, common | Dongjoon Hyun | Dongjoon Hyun |
| [HADOOP-17340](https://issues.apache.org/jira/browse/HADOOP-17340) | TestLdapGroupsMapping failing -string mismatch in exception validation |  Major | test | Steve Loughran | Steve Loughran |
| [HDFS-15667](https://issues.apache.org/jira/browse/HDFS-15667) | Audit log record the unexpected allowed result when delete called |  Major | hdfs | Baolong Mao | Baolong Mao |
| [HADOOP-17352](https://issues.apache.org/jira/browse/HADOOP-17352) | Update PATCH\_NAMING\_RULE in the personality file |  Minor | build | Akira Ajisaka | Akira Ajisaka |
| [YARN-10458](https://issues.apache.org/jira/browse/YARN-10458) | Hive On Tez queries fails upon submission to dynamically created pools |  Major | resourcemanager | Anand Srinivasan | Peter Bacsko |
| [HDFS-15485](https://issues.apache.org/jira/browse/HDFS-15485) | Fix outdated properties of JournalNode when performing rollback |  Minor | journal-node | Deegue | Deegue |
| [HADOOP-17096](https://issues.apache.org/jira/browse/HADOOP-17096) | Fix ZStandardCompressor input buffer offset |  Major | io | Stephen Jung (Stripe) | Stephen Jung (Stripe) |
| [HADOOP-17324](https://issues.apache.org/jira/browse/HADOOP-17324) | Don't relocate org.bouncycastle in shaded client jars |  Critical | build | Chao Sun | Chao Sun |
| [HADOOP-17373](https://issues.apache.org/jira/browse/HADOOP-17373) | hadoop-client-integration-tests doesn't work when building with skipShade |  Major | build | Chao Sun | Chao Sun |
| [HADOOP-17358](https://issues.apache.org/jira/browse/HADOOP-17358) | Improve excessive reloading of Configurations |  Major | conf | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15545](https://issues.apache.org/jira/browse/HDFS-15545) | (S)Webhdfs will not use updated delegation tokens available in the ugi after the old ones expire |  Major | webhdfs | Issac Buenrostro | Issac Buenrostro |
| [HDFS-15538](https://issues.apache.org/jira/browse/HDFS-15538) | Fix the documentation for dfs.namenode.replication.max-streams in hdfs-default.xml |  Major | documentation | Xieming Li | Xieming Li |
| [HADOOP-17362](https://issues.apache.org/jira/browse/HADOOP-17362) | Doing hadoop ls on Har file triggers too many RPC calls |  Major | fs | Ahmed Hussein | Ahmed Hussein |
| [YARN-10485](https://issues.apache.org/jira/browse/YARN-10485) | TimelineConnector swallows InterruptedException |  Major | yarn-client | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17360](https://issues.apache.org/jira/browse/HADOOP-17360) | Log the remote address for authentication success |  Minor | ipc | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15685](https://issues.apache.org/jira/browse/HDFS-15685) | [JDK 14] TestConfiguredFailoverProxyProvider#testResolveDomainNameUsingDNS fails |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [MAPREDUCE-7305](https://issues.apache.org/jira/browse/MAPREDUCE-7305) | [JDK 11] TestMRJobsWithProfiler fails |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [YARN-10396](https://issues.apache.org/jira/browse/YARN-10396) | Max applications calculation per queue disregards queue level settings in absolute mode |  Major | capacity scheduler | Benjamin Teke | Benjamin Teke |
| [HADOOP-17390](https://issues.apache.org/jira/browse/HADOOP-17390) | Skip license check on lz4 code files |  Major | build | Zhihua Deng | Zhihua Deng |
| [HADOOP-17346](https://issues.apache.org/jira/browse/HADOOP-17346) | Fair call queue is defeated by abusive service principals |  Major | common, ipc | Ahmed Hussein | Ahmed Hussein |
| [YARN-10470](https://issues.apache.org/jira/browse/YARN-10470) | When building new web ui with root user, the bower install should support it. |  Major | build, yarn-ui-v2 | Qi Zhu | Qi Zhu |
| [YARN-10468](https://issues.apache.org/jira/browse/YARN-10468) | TestNodeStatusUpdater does not handle early failure in threads |  Major | nodemanager | Ahmed Hussein | Ahmed Hussein |
| [YARN-10488](https://issues.apache.org/jira/browse/YARN-10488) | Several typos in package: org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair |  Minor | fairscheduler | Szilard Nemeth | Ankit Kumar |
| [HDFS-15698](https://issues.apache.org/jira/browse/HDFS-15698) | Fix the typo of dfshealth.html after HDFS-15358 |  Trivial | namenode | Hui Fei | Hui Fei |
| [YARN-10498](https://issues.apache.org/jira/browse/YARN-10498) | Fix Yarn CapacityScheduler Markdown document |  Trivial | documentation | zhaoshengjie | zhaoshengjie |
| [HADOOP-17399](https://issues.apache.org/jira/browse/HADOOP-17399) | lz4 sources missing for native Visual Studio project |  Major | native | Gautham Banasandra | Gautham Banasandra |
| [HDFS-15695](https://issues.apache.org/jira/browse/HDFS-15695) | NN should not let the balancer run in safemode |  Major | namenode | Ahmed Hussein | Ahmed Hussein |
| [YARN-9883](https://issues.apache.org/jira/browse/YARN-9883) | Reshape SchedulerHealth class |  Minor | resourcemanager, yarn | Adam Antal | D M Murali Krishna Reddy |
| [YARN-10511](https://issues.apache.org/jira/browse/YARN-10511) | Update yarn.nodemanager.env-whitelist value in docs |  Minor | documentation | Andrea Scarpino | Andrea Scarpino |
| [HADOOP-16881](https://issues.apache.org/jira/browse/HADOOP-16881) | KerberosAuthentication does not disconnect HttpURLConnection leading to CLOSE\_WAIT cnxns |  Major | auth, security | Prabhu Joseph | Attila Magyar |
| [HADOOP-16080](https://issues.apache.org/jira/browse/HADOOP-16080) | hadoop-aws does not work with hadoop-client-api |  Major | fs/s3 | Keith Turner | Chao Sun |
| [HDFS-15660](https://issues.apache.org/jira/browse/HDFS-15660) | StorageTypeProto is not compatiable between 3.x and 2.6 |  Major | build | Ryan Wu | Ryan Wu |
| [HDFS-15707](https://issues.apache.org/jira/browse/HDFS-15707) | NNTop counts don't add up as expected |  Major | hdfs, metrics, namenode | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15709](https://issues.apache.org/jira/browse/HDFS-15709) | EC: Socket file descriptor leak in StripedBlockChecksumReconstructor |  Major | datanode, ec, erasure-coding | Yushi Hayasaka | Yushi Hayasaka |
| [YARN-10495](https://issues.apache.org/jira/browse/YARN-10495) | make the rpath of container-executor configurable |  Major | yarn | angerszhu | angerszhu |
| [HDFS-15240](https://issues.apache.org/jira/browse/HDFS-15240) | Erasure Coding: dirty buffer causes reconstruction block error |  Blocker | datanode, erasure-coding | HuangTao | HuangTao |
| [YARN-10491](https://issues.apache.org/jira/browse/YARN-10491) | Fix deprecation warnings in SLSWebApp.java |  Minor | build | Akira Ajisaka | Ankit Kumar |
| [HADOOP-13571](https://issues.apache.org/jira/browse/HADOOP-13571) | ServerSocketUtil.getPort() should use loopback address, not 0.0.0.0 |  Major | net | Eric Badger | Eric Badger |
| [HDFS-15725](https://issues.apache.org/jira/browse/HDFS-15725) | Lease Recovery never completes for a committed block which the DNs never finalize |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-15170](https://issues.apache.org/jira/browse/HDFS-15170) | EC: Block gets marked as CORRUPT in case of failover and pipeline recovery |  Critical | erasure-coding | Ayush Saxena | Ayush Saxena |
| [YARN-10499](https://issues.apache.org/jira/browse/YARN-10499) | TestRouterWebServicesREST fails |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [YARN-10536](https://issues.apache.org/jira/browse/YARN-10536) | Client in distributedShell swallows interrupt exceptions |  Major | client, distributed-shell | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15116](https://issues.apache.org/jira/browse/HDFS-15116) | Correct spelling of comments for NNStorage.setRestoreFailedStorage |  Trivial | namanode | Xudong Cao | Xudong Cao |
| [HDFS-15743](https://issues.apache.org/jira/browse/HDFS-15743) | Fix -Pdist build failure of hadoop-hdfs-native-client |  Major | build | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-15739](https://issues.apache.org/jira/browse/HDFS-15739) | Missing Javadoc for a param in DFSNetworkTopology |  Minor | hdfs | zhanghuazong | zhanghuazong |
| [YARN-10334](https://issues.apache.org/jira/browse/YARN-10334) | TestDistributedShell leaks resources on timeout/failure |  Major | distributed-shell, test, yarn | Ahmed Hussein | Ahmed Hussein |
| [YARN-10558](https://issues.apache.org/jira/browse/YARN-10558) | Fix failure of TestDistributedShell#testDSShellWithOpportunisticContainers |  Minor | test | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-15719](https://issues.apache.org/jira/browse/HDFS-15719) | [Hadoop 3] Both NameNodes can crash simultaneously due to the short JN socket timeout |  Critical | hdfs, journal-node, namanode | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-10560](https://issues.apache.org/jira/browse/YARN-10560) | Upgrade node.js to 10.23.1 and yarn to 1.22.5 in Web UI v2 |  Major | webapp, yarn-ui-v2 | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17444](https://issues.apache.org/jira/browse/HADOOP-17444) | ADLFS: Update SDK version from 2.3.6 to 2.3.9 |  Minor | fs/adl | Bilahari T H | Bilahari T H |
| [YARN-10528](https://issues.apache.org/jira/browse/YARN-10528) | maxAMShare should only be accepted for leaf queues, not parent queues |  Major | fairscheduler | Siddharth Ahuja | Siddharth Ahuja |
| [YARN-10553](https://issues.apache.org/jira/browse/YARN-10553) | Refactor TestDistributedShell |  Major | distributed-shell, test | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17438](https://issues.apache.org/jira/browse/HADOOP-17438) | Increase docker memory limit in Jenkins |  Major | build, scripts, test, yetus | Ahmed Hussein | Ahmed Hussein |
| [MAPREDUCE-7310](https://issues.apache.org/jira/browse/MAPREDUCE-7310) | Clear the fileMap in JHEventHandlerForSigtermTest |  Minor | test | Zhengxi Li | Zhengxi Li |
| [YARN-7200](https://issues.apache.org/jira/browse/YARN-7200) | SLS generates a realtimetrack.json file but that file is missing the closing ']' |  Minor | scheduler-load-simulator | Grant Sohn | Agshin Kazimli |
| [HADOOP-16947](https://issues.apache.org/jira/browse/HADOOP-16947) | Stale record should be remove when MutableRollingAverages generating aggregate data. |  Major | metrics | Haibin Huang | Haibin Huang |
| [YARN-10515](https://issues.apache.org/jira/browse/YARN-10515) | Fix flaky test TestCapacitySchedulerAutoQueueCreation.testDynamicAutoQueueCreationWithTags |  Major | test | Peter Bacsko | Peter Bacsko |
| [HADOOP-17224](https://issues.apache.org/jira/browse/HADOOP-17224) | Install Intel ISA-L library in Dockerfile |  Blocker | build | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-15632](https://issues.apache.org/jira/browse/HDFS-15632) | AbstractContractDeleteTest should set recursive parameter to true for recursive test cases. |  Major | test | Konstantin Shvachko | Anton Kutuzov |
| [HADOOP-17496](https://issues.apache.org/jira/browse/HADOOP-17496) | Build failure due to python2.7 deprecation by pip |  Blocker | build | Gautham Banasandra | Gautham Banasandra |
| [HDFS-15661](https://issues.apache.org/jira/browse/HDFS-15661) | The DeadNodeDetector shouldn't be shared by different DFSClients. |  Major | datanode | Jinglun | Jinglun |
| [HDFS-10498](https://issues.apache.org/jira/browse/HDFS-10498) | Intermittent test failure org.apache.hadoop.hdfs.server.namenode.snapshot.TestSnapshotFileLength.testSnapshotfileLength |  Major | hdfs, snapshots | Hanisha Koneru | Jim Brennan |
| [HADOOP-17506](https://issues.apache.org/jira/browse/HADOOP-17506) | Fix typo in BUILDING.txt |  Trivial | documentation | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-17507](https://issues.apache.org/jira/browse/HADOOP-17507) | Add build instructions for installing GCC 9 and CMake 3.19 |  Trivial | documentation | Gautham Banasandra | Gautham Banasandra |
| [HDFS-15791](https://issues.apache.org/jira/browse/HDFS-15791) | Possible Resource Leak in FSImageFormatProtobuf |  Major | namenode | Narges Shadab | Narges Shadab |
| [HDFS-15795](https://issues.apache.org/jira/browse/HDFS-15795) | EC: Wrong checksum when reconstruction was failed by exception |  Major | datanode, ec, erasure-coding | Yushi Hayasaka | Yushi Hayasaka |
| [HDFS-15779](https://issues.apache.org/jira/browse/HDFS-15779) | EC: fix NPE caused by StripedWriter.clearBuffers during reconstruct block |  Major | erasure-coding | Hongbing Wang | Hongbing Wang |
| [YARN-10611](https://issues.apache.org/jira/browse/YARN-10611) | Fix that shaded should be used for google guava imports in YARN-10352. |  Major | test | Qi Zhu | Qi Zhu |
| [HDFS-15798](https://issues.apache.org/jira/browse/HDFS-15798) | EC: Reconstruct task failed, and It would be XmitsInProgress of DN has negative number |  Major | erasure-coding | Haiyang Hu | Haiyang Hu |
| [HADOOP-17513](https://issues.apache.org/jira/browse/HADOOP-17513) | Checkstyle IllegalImport does not catch guava imports |  Major | build, common | Ahmed Hussein | Ahmed Hussein |
| [YARN-10428](https://issues.apache.org/jira/browse/YARN-10428) | Zombie applications in the YARN queue using FAIR + sizebasedweight |  Critical | capacityscheduler | Guang Yang | Andras Gyori |
| [YARN-10607](https://issues.apache.org/jira/browse/YARN-10607) | User environment is unable to prepend PATH when mapreduce.admin.user.env also sets PATH |  Major | container, nodeattibute | Eric Badger | Eric Badger |
| [HDFS-15792](https://issues.apache.org/jira/browse/HDFS-15792) | ClasscastException while loading FSImage |  Major | nn | Renukaprasad C | Renukaprasad C |
| [YARN-10593](https://issues.apache.org/jira/browse/YARN-10593) | Fix incorrect string comparison in GpuDiscoverer |  Major | resourcemanager | Peter Bacsko | Peter Bacsko |
| [HADOOP-17516](https://issues.apache.org/jira/browse/HADOOP-17516) | Upgrade ant to 1.10.9 |  Major | common | Akira Ajisaka | Akira Ajisaka |
| [YARN-10618](https://issues.apache.org/jira/browse/YARN-10618) | RM UI2 Application page shows the AM preempted containers instead of the nonAM ones |  Minor | yarn-ui-v2 | Benjamin Teke | Benjamin Teke |
| [YARN-10500](https://issues.apache.org/jira/browse/YARN-10500) | TestDelegationTokenRenewer fails intermittently |  Major | test | Akira Ajisaka | Masatake Iwasaki |
| [HDFS-15839](https://issues.apache.org/jira/browse/HDFS-15839) | RBF: Cannot get method setBalancerBandwidth on Router Client |  Major | rbf | Yang Yun | Yang Yun |
| [HDFS-15806](https://issues.apache.org/jira/browse/HDFS-15806) | DeadNodeDetector should close all the threads when it is closed. |  Major | datanode | Jinglun | Jinglun |
| [HADOOP-17534](https://issues.apache.org/jira/browse/HADOOP-17534) | Upgrade Jackson databind to 2.10.5.1 |  Major | build | Adam Roberts | Akira Ajisaka |
| [MAPREDUCE-7323](https://issues.apache.org/jira/browse/MAPREDUCE-7323) | Remove job\_history\_summary.py |  Major | examples | Akira Ajisaka | Akira Ajisaka |
| [YARN-10647](https://issues.apache.org/jira/browse/YARN-10647) | Fix TestRMNodeLabelsManager failed after YARN-10501. |  Major | test | Qi Zhu | Qi Zhu |
| [HADOOP-17510](https://issues.apache.org/jira/browse/HADOOP-17510) | Hadoop prints sensitive Cookie information. |  Major | security | Renukaprasad C | Renukaprasad C |
| [HDFS-15422](https://issues.apache.org/jira/browse/HDFS-15422) | Reported IBR is partially replaced with stored info when queuing. |  Critical | namenode | Kihwal Lee | Stephen O'Donnell |
| [YARN-10651](https://issues.apache.org/jira/browse/YARN-10651) | CapacityScheduler crashed with NPE in AbstractYarnScheduler.updateNodeResource() |  Major | capacity scheduler | Haibo Chen | Haibo Chen |
| [MAPREDUCE-7320](https://issues.apache.org/jira/browse/MAPREDUCE-7320) | ClusterMapReduceTestCase does not clean directories |  Major | test | Ahmed Hussein | Ahmed Hussein |
| [YARN-10656](https://issues.apache.org/jira/browse/YARN-10656) | Parsing error in CapacityScheduler.md |  Major | documentation | Akira Ajisaka | Akira Ajisaka |
| [HDFS-14013](https://issues.apache.org/jira/browse/HDFS-14013) | Skip any credentials stored in HDFS when starting ZKFC |  Major | hdfs | Krzysztof Adamski | Stephen O'Donnell |
| [HDFS-15849](https://issues.apache.org/jira/browse/HDFS-15849) | ExpiredHeartbeats metric should be of Type.COUNTER |  Major | metrics | Konstantin Shvachko | Qi Zhu |
| [HADOOP-17560](https://issues.apache.org/jira/browse/HADOOP-17560) |  Fix some spelling errors |  Trivial | documentation | jiaguodong | jiaguodong |
| [YARN-10649](https://issues.apache.org/jira/browse/YARN-10649) | Fix RMNodeImpl.updateExistContainers leak |  Major | resourcemanager | Max  Xie | Max  Xie |
| [YARN-10672](https://issues.apache.org/jira/browse/YARN-10672) | All testcases in TestReservations are flaky |  Major | reservation system, test | Szilard Nemeth | Szilard Nemeth |
| [HADOOP-17557](https://issues.apache.org/jira/browse/HADOOP-17557) | skip-dir option is not processed by Yetus |  Major | build, precommit, yetus | Ahmed Hussein | Ahmed Hussein |
| [YARN-10676](https://issues.apache.org/jira/browse/YARN-10676) | Improve code quality in TestTimelineAuthenticationFilterForV1 |  Minor | test, timelineservice | Szilard Nemeth | Szilard Nemeth |
| [YARN-10675](https://issues.apache.org/jira/browse/YARN-10675) | Consolidate YARN-10672 and YARN-10447 |  Major | capacity scheduler | Szilard Nemeth | Szilard Nemeth |
| [YARN-10678](https://issues.apache.org/jira/browse/YARN-10678) | Try blocks without catch blocks in SLS scheduler classes can swallow other exceptions |  Major | scheduler-load-simulator | Szilard Nemeth | Szilard Nemeth |
| [YARN-10677](https://issues.apache.org/jira/browse/YARN-10677) | Logger of SLSFairScheduler is provided with the wrong class |  Major | scheduler-load-simulator | Szilard Nemeth | Szilard Nemeth |
| [YARN-10681](https://issues.apache.org/jira/browse/YARN-10681) | Fix assertion failure message in BaseSLSRunnerTest |  Trivial | scheduler-load-simulator | Szilard Nemeth | Szilard Nemeth |
| [YARN-10679](https://issues.apache.org/jira/browse/YARN-10679) | Better logging of uncaught exceptions throughout SLS |  Major | scheduler-load-simulator | Szilard Nemeth | Szilard Nemeth |
| [YARN-10671](https://issues.apache.org/jira/browse/YARN-10671) | Fix Typo in TestSchedulingRequestContainerAllocation |  Minor | capacity scheduler, test | D M Murali Krishna Reddy | D M Murali Krishna Reddy |
| [HDFS-15875](https://issues.apache.org/jira/browse/HDFS-15875) | Check whether file is being truncated before truncate |  Major | datanode, fs, namanode | Hui Fei | Hui Fei |
| [HADOOP-17573](https://issues.apache.org/jira/browse/HADOOP-17573) | Fix compilation error of OBSFileSystem in trunk |  Major | fs/huawei | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-17582](https://issues.apache.org/jira/browse/HADOOP-17582) | Replace GitHub App Token with GitHub OAuth token |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [YARN-10687](https://issues.apache.org/jira/browse/YARN-10687) | Add option to disable/enable free disk space checking and percentage checking for full and not-full disks |  Major | nodemanager | Qi Zhu | Qi Zhu |
| [HADOOP-17581](https://issues.apache.org/jira/browse/HADOOP-17581) | Fix reference to LOG is ambiguous after HADOOP-17482 |  Major | test | Xiaoyu Yao | Xiaoyu Yao |
| [HADOOP-17586](https://issues.apache.org/jira/browse/HADOOP-17586) | Upgrade org.codehaus.woodstox:stax2-api to 4.2.1 |  Major | build, common | Ayush Saxena | Ayush Saxena |
| [HDFS-15816](https://issues.apache.org/jira/browse/HDFS-15816) | Fix shouldAvoidStaleDataNodesForWrite returns when no stale node in cluster. |  Minor | block placement | Yang Yun | Yang Yun |
| [HADOOP-17585](https://issues.apache.org/jira/browse/HADOOP-17585) | Correct timestamp format in the docs for the touch command |  Major | common | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-15809](https://issues.apache.org/jira/browse/HDFS-15809) | DeadNodeDetector doesn't remove live nodes from dead node set. |  Major | datanode | Jinglun | Jinglun |
| [HADOOP-17532](https://issues.apache.org/jira/browse/HADOOP-17532) | Yarn Job execution get failed when LZ4 Compression Codec is used |  Major | common | Bhavik Patel | Bhavik Patel |
| [YARN-10588](https://issues.apache.org/jira/browse/YARN-10588) | Percentage of queue and cluster is zero in WebUI |  Major | resourcemanager | Bilwa S T | Bilwa S T |
| [YARN-10682](https://issues.apache.org/jira/browse/YARN-10682) | The scheduler monitor policies conf should trim values separated by comma |  Major | capacity scheduler | Qi Zhu | Qi Zhu |
| [MAPREDUCE-7322](https://issues.apache.org/jira/browse/MAPREDUCE-7322) | revisiting TestMRIntermediateDataEncryption |  Major | job submission, security, test | Ahmed Hussein | Ahmed Hussein |
| [YARN-10652](https://issues.apache.org/jira/browse/YARN-10652) | Capacity Scheduler fails to handle user weights for a user that has a "." (dot) in it |  Major | capacity scheduler | Siddharth Ahuja | Siddharth Ahuja |
| [HADOOP-17578](https://issues.apache.org/jira/browse/HADOOP-17578) | Improve UGI debug log to help troubleshooting TokenCache related issues |  Major | security | Xiaoyu Yao | Xiaoyu Yao |
| [YARN-10685](https://issues.apache.org/jira/browse/YARN-10685) | Fix typos in AbstractCSQueue |  Major | capacity scheduler | Qi Zhu | Qi Zhu |
| [YARN-10703](https://issues.apache.org/jira/browse/YARN-10703) | Fix potential null pointer error of gpuNodeResourceUpdateHandler in NodeResourceMonitorImpl. |  Major | nodemanager | Qi Zhu | Qi Zhu |
| [HDFS-15868](https://issues.apache.org/jira/browse/HDFS-15868) | Possible Resource Leak in EditLogFileOutputStream |  Major | namanode | Narges Shadab | Narges Shadab |
| [HADOOP-17592](https://issues.apache.org/jira/browse/HADOOP-17592) | Fix the wrong CIDR range example in Proxy User documentation |  Minor | documentation | Kwangsun Noh | Kwangsun Noh |
| [YARN-10706](https://issues.apache.org/jira/browse/YARN-10706) | Upgrade com.github.eirslett:frontend-maven-plugin to 1.11.2 |  Major | buid | Mingliang Liu | Mingliang Liu |
| [MAPREDUCE-7325](https://issues.apache.org/jira/browse/MAPREDUCE-7325) | Intermediate data encryption is broken in LocalJobRunner |  Major | job submission, security | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17598](https://issues.apache.org/jira/browse/HADOOP-17598) | Fix java doc issue introduced by HADOOP-17578 |  Minor | documentation | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-15908](https://issues.apache.org/jira/browse/HDFS-15908) | Possible Resource Leak in org.apache.hadoop.hdfs.qjournal.server.Journal |  Major | journal-node | Narges Shadab | Narges Shadab |
| [HDFS-15910](https://issues.apache.org/jira/browse/HDFS-15910) | Replace bzero with explicit\_bzero for better safety |  Critical | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [YARN-10697](https://issues.apache.org/jira/browse/YARN-10697) | Resources are displayed in bytes in UI for schedulers other than capacity |  Major | ui, webapp | Bilwa S T | Bilwa S T |
| [HDFS-15918](https://issues.apache.org/jira/browse/HDFS-15918) | Replace RAND\_pseudo\_bytes in sasl\_digest\_md5.cc |  Critical | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-17602](https://issues.apache.org/jira/browse/HADOOP-17602) | Upgrade JUnit to 4.13.1 |  Major | build, security, test | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15922](https://issues.apache.org/jira/browse/HDFS-15922) | Use memcpy for copying non-null terminated string in jni\_helper.c |  Major | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [HDFS-15900](https://issues.apache.org/jira/browse/HDFS-15900) | RBF: empty blockpool id on dfsrouter caused by UNAVAILABLE NameNode |  Major | rbf | Harunobu Daikoku | Harunobu Daikoku |
| [HDFS-15935](https://issues.apache.org/jira/browse/HDFS-15935) | Use memcpy for copying non-null terminated string |  Major | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [YARN-10501](https://issues.apache.org/jira/browse/YARN-10501) | Can't remove all node labels after add node label without nodemanager port |  Critical | yarn | caozhiqiang | caozhiqiang |
| [YARN-10437](https://issues.apache.org/jira/browse/YARN-10437) | Destroy yarn service if any YarnException occurs during submitApp |  Minor | yarn-native-services | D M Murali Krishna Reddy | D M Murali Krishna Reddy |
| [YARN-10439](https://issues.apache.org/jira/browse/YARN-10439) | Yarn Service AM listens on all IP's on the machine |  Minor | security, yarn-native-services | D M Murali Krishna Reddy | D M Murali Krishna Reddy |
| [YARN-10441](https://issues.apache.org/jira/browse/YARN-10441) | Add support for hadoop.http.rmwebapp.scheduler.page.class |  Major | scheduler | D M Murali Krishna Reddy | D M Murali Krishna Reddy |
| [YARN-10466](https://issues.apache.org/jira/browse/YARN-10466) | Fix NullPointerException in  yarn-services Component.java |  Minor | yarn-service | D M Murali Krishna Reddy | D M Murali Krishna Reddy |
| [YARN-10716](https://issues.apache.org/jira/browse/YARN-10716) | Fix typo in ContainerRuntime |  Trivial | documentation | Wanqiang Ji | xishuhai |
| [HDFS-15928](https://issues.apache.org/jira/browse/HDFS-15928) | Replace RAND\_pseudo\_bytes in rpc\_engine.cc |  Critical | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [HDFS-15929](https://issues.apache.org/jira/browse/HDFS-15929) | Replace RAND\_pseudo\_bytes in util.cc |  Critical | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [HDFS-15927](https://issues.apache.org/jira/browse/HDFS-15927) | Catch polymorphic type by reference |  Major | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [YARN-10718](https://issues.apache.org/jira/browse/YARN-10718) | Fix CapacityScheduler#initScheduler log error. |  Major | capacity scheduler | Qi Zhu | Qi Zhu |
| [HDFS-15494](https://issues.apache.org/jira/browse/HDFS-15494) | TestReplicaCachingGetSpaceUsed#testReplicaCachingGetSpaceUsedByRBWReplica Fails on Windows |  Major | datanode, test | Ravuri Sushma sree | Ravuri Sushma sree |
| [HDFS-15222](https://issues.apache.org/jira/browse/HDFS-15222) |  Correct the "hdfs fsck -list-corruptfileblocks" command output |  Minor | hdfs, tools | Souryakanta Dwivedy | Ravuri Sushma sree |
| [HADOOP-17610](https://issues.apache.org/jira/browse/HADOOP-17610) | DelegationTokenAuthenticator prints token information |  Major | security | Ravuri Sushma sree | Ravuri Sushma sree |
| [HADOOP-17587](https://issues.apache.org/jira/browse/HADOOP-17587) | Kinit with keytab should not display the keytab file's full path in any logs |  Major | security | Ravuri Sushma sree | Ravuri Sushma sree |
| [HDFS-15944](https://issues.apache.org/jira/browse/HDFS-15944) | Prevent truncation by snprintf |  Critical | fuse-dfs, libhdfs | Gautham Banasandra | Gautham Banasandra |
| [HDFS-15930](https://issues.apache.org/jira/browse/HDFS-15930) | Fix some @param errors in DirectoryScanner. |  Minor | datanode | Qi Zhu | Qi Zhu |
| [HADOOP-17619](https://issues.apache.org/jira/browse/HADOOP-17619) | Fix DelegationTokenRenewer#updateRenewalTime java doc error. |  Minor | documentation | Qi Zhu | Qi Zhu |
| [HDFS-15950](https://issues.apache.org/jira/browse/HDFS-15950) | Remove unused hdfs.proto import |  Major | hdfs-client | Gautham Banasandra | Gautham Banasandra |
| [HDFS-15947](https://issues.apache.org/jira/browse/HDFS-15947) | Replace deprecated protobuf APIs |  Critical | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [HDFS-15949](https://issues.apache.org/jira/browse/HDFS-15949) | Fix integer overflow |  Major | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-17588](https://issues.apache.org/jira/browse/HADOOP-17588) | CryptoInputStream#close() should be synchronized |  Major | crypto | Renukaprasad C | Renukaprasad C |
| [HADOOP-17621](https://issues.apache.org/jira/browse/HADOOP-17621) | hadoop-auth to remove jetty-server dependency |  Major | auth | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-15948](https://issues.apache.org/jira/browse/HDFS-15948) | Fix test4tests for libhdfspp |  Critical | build, libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-17617](https://issues.apache.org/jira/browse/HADOOP-17617) | Incorrect representation of RESPONSE for Get Key Version in KMS index.md.vm file |  Major | documentation | Ravuri Sushma sree | Ravuri Sushma sree |
| [MAPREDUCE-7270](https://issues.apache.org/jira/browse/MAPREDUCE-7270) | TestHistoryViewerPrinter could be failed when the locale isn't English. |  Minor | test | Sungpeo Kook | Sungpeo Kook |
| [HDFS-15916](https://issues.apache.org/jira/browse/HDFS-15916) | DistCp: Backward compatibility: Distcp fails from Hadoop 3 to Hadoop 2 for snapshotdiff |  Major | distcp | Srinivasu Majeti | Ayush Saxena |
| [MAPREDUCE-7329](https://issues.apache.org/jira/browse/MAPREDUCE-7329) | HadoopPipes task may fail when linux kernel version change from 3.x to 4.x |  Major | pipes | chaoli | chaoli |
| [MAPREDUCE-7334](https://issues.apache.org/jira/browse/MAPREDUCE-7334) | TestJobEndNotifier fails |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17608](https://issues.apache.org/jira/browse/HADOOP-17608) | Fix TestKMS failure |  Major | kms | Akira Ajisaka | Akira Ajisaka |
| [YARN-10736](https://issues.apache.org/jira/browse/YARN-10736) | Fix GetApplicationsRequest JavaDoc |  Major | documentation | Miklos Gergely | Miklos Gergely |
| [HDFS-15423](https://issues.apache.org/jira/browse/HDFS-15423) | RBF: WebHDFS create shouldn't choose DN from all sub-clusters |  Major | rbf, webhdfs | Chao Sun | Fengnan Li |
| [HDFS-15977](https://issues.apache.org/jira/browse/HDFS-15977) | Call explicit\_bzero only if it is available |  Major | libhdfs++ | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15963](https://issues.apache.org/jira/browse/HDFS-15963) | Unreleased volume references cause an infinite loop |  Critical | datanode | Shuyan Zhang | Shuyan Zhang |
| [HADOOP-17642](https://issues.apache.org/jira/browse/HADOOP-17642) | Remove appender EventCounter to avoid instantiation |  Major | common | Viraj Jasani | Viraj Jasani |
| [HADOOP-17635](https://issues.apache.org/jira/browse/HADOOP-17635) | Update the ubuntu version in the build instruction |  Major | build, documentation | Akira Ajisaka | Masatake Iwasaki |
| [YARN-10460](https://issues.apache.org/jira/browse/YARN-10460) | Upgrading to JUnit 4.13 causes tests in TestNodeStatusUpdater to fail |  Major | nodemanager, test | Peter Bacsko | Peter Bacsko |
| [HADOOP-17505](https://issues.apache.org/jira/browse/HADOOP-17505) | public interface GroupMappingServiceProvider needs default impl for getGroupsSet() |  Major | security | Vinayakumar B | Vinayakumar B |
| [HDFS-15974](https://issues.apache.org/jira/browse/HDFS-15974) | RBF: Unable to display the datanode UI of the router |  Major | rbf, ui | Xiangyi Zhu | Xiangyi Zhu |
| [HADOOP-17655](https://issues.apache.org/jira/browse/HADOOP-17655) | Upgrade Jetty to 9.4.40 |  Blocker | common | Akira Ajisaka | Akira Ajisaka |
| [YARN-10705](https://issues.apache.org/jira/browse/YARN-10705) | Misleading DEBUG log for container assignment needs to be removed when the container is actually reserved, not assigned in FairScheduler |  Minor | yarn | Siddharth Ahuja | Siddharth Ahuja |
| [YARN-10749](https://issues.apache.org/jira/browse/YARN-10749) | Can't remove all node labels after add node label without nodemanager port, broken by YARN-10647 |  Major | nodelabel | D M Murali Krishna Reddy | D M Murali Krishna Reddy |
| [HDFS-15566](https://issues.apache.org/jira/browse/HDFS-15566) | NN restart fails after RollingUpgrade from  3.1.3/3.2.1 to 3.3.0 |  Blocker | namanode | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-17650](https://issues.apache.org/jira/browse/HADOOP-17650) | Fails to build using Maven 3.8.1 |  Major | build | Wei-Chiu Chuang | Viraj Jasani |
| [HDFS-15621](https://issues.apache.org/jira/browse/HDFS-15621) | Datanode DirectoryScanner uses excessive memory |  Major | datanode | Stephen O'Donnell | Stephen O'Donnell |
| [HADOOP-17674](https://issues.apache.org/jira/browse/HADOOP-17674) | Use spotbugs-maven-plugin in hadoop-huaweicloud |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15624](https://issues.apache.org/jira/browse/HDFS-15624) |  Fix the SetQuotaByStorageTypeOp problem after updating hadoop |  Major | hdfs | YaYun Wang | huangtianhua |
| [HDFS-15561](https://issues.apache.org/jira/browse/HDFS-15561) | RBF: Fix NullPointException when start dfsrouter |  Major | rbf | Xie Lei | Fengnan Li |
| [HDFS-15865](https://issues.apache.org/jira/browse/HDFS-15865) | Interrupt DataStreamer thread |  Minor | datanode | Karthik Palanisamy | Karthik Palanisamy |
| [HDFS-15810](https://issues.apache.org/jira/browse/HDFS-15810) | RBF: RBFMetrics's TotalCapacity out of bounds |  Major | rbf | Weison Wei | Fengnan Li |
| [HADOOP-17657](https://issues.apache.org/jira/browse/HADOOP-17657) | SequenceFile.Writer should implement StreamCapabilities |  Major | io | Kishen Das | Kishen Das |
| [YARN-10756](https://issues.apache.org/jira/browse/YARN-10756) | Remove additional junit 4.11 dependency from javadoc |  Major | build, test, timelineservice | ANANDA G B | Akira Ajisaka |
| [HADOOP-17375](https://issues.apache.org/jira/browse/HADOOP-17375) | Fix the error of TestDynamometerInfra |  Major | test | Akira Ajisaka | Takanobu Asanuma |
| [HDFS-16001](https://issues.apache.org/jira/browse/HDFS-16001) | TestOfflineEditsViewer.testStored() fails reading negative value of FSEditLogOpCodes |  Blocker | hdfs | Konstantin Shvachko | Akira Ajisaka |
| [HADOOP-17686](https://issues.apache.org/jira/browse/HADOOP-17686) | Avoid potential NPE by using Path#getParentPath API in hadoop-huaweicloud |  Major | fs | Error Reporter | Viraj Jasani |
| [HADOOP-17689](https://issues.apache.org/jira/browse/HADOOP-17689) | Avoid Potential NPE in org.apache.hadoop.fs |  Major | fs | Error Reporter | Viraj Jasani |
| [HDFS-15988](https://issues.apache.org/jira/browse/HDFS-15988) | Stabilise HDFS Pre-Commit |  Major | build, test | Ayush Saxena | Ayush Saxena |
| [HADOOP-17142](https://issues.apache.org/jira/browse/HADOOP-17142) | Fix outdated properties of journal node when perform rollback |  Minor | . | Deegue | Deegue |
| [HADOOP-17107](https://issues.apache.org/jira/browse/HADOOP-17107) | hadoop-azure parallel tests not working on recent JDKs |  Major | build, fs/azure | Steve Loughran | Steve Loughran |
| [YARN-10555](https://issues.apache.org/jira/browse/YARN-10555) |  Missing access check before getAppAttempts |  Critical | webapp | lujie | lujie |
| [HADOOP-17703](https://issues.apache.org/jira/browse/HADOOP-17703) | checkcompatibility.py errors out when specifying annotations |  Major | scripts | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-17699](https://issues.apache.org/jira/browse/HADOOP-17699) | Remove hardcoded SunX509 usage from SSLFactory |  Major | common | Xiaoyu Yao | Xiaoyu Yao |
| [YARN-10777](https://issues.apache.org/jira/browse/YARN-10777) | Bump node-sass from 4.13.0 to 4.14.1 in /hadoop-yarn-project/hadoop-yarn/hadoop-yarn-ui/src/main/webapp |  Major | yarn-ui-v2 | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-10701](https://issues.apache.org/jira/browse/YARN-10701) | The yarn.resource-types should support multi types without trimmed. |  Major | resourcemanager | Qi Zhu | Qi Zhu |
| [HADOOP-14922](https://issues.apache.org/jira/browse/HADOOP-14922) | Build of Mapreduce Native Task module fails with unknown opcode "bswap" |  Major | common | Anup Halarnkar | Anup Halarnkar |
| [YARN-10766](https://issues.apache.org/jira/browse/YARN-10766) | [UI2] Bump moment-timezone to 0.5.33 |  Major | yarn, yarn-ui-v2 | Andras Gyori | Andras Gyori |
| [HADOOP-17718](https://issues.apache.org/jira/browse/HADOOP-17718) | Explicitly set locale in the Dockerfile |  Blocker | build | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-17700](https://issues.apache.org/jira/browse/HADOOP-17700) | ExitUtil#halt info log should log HaltException |  Major | common | Viraj Jasani | Viraj Jasani |
| [YARN-7769](https://issues.apache.org/jira/browse/YARN-7769) | FS QueueManager should not create default queue at init |  Major | fairscheduler | Wilfred Spiegelenburg | Benjamin Teke |
| [YARN-10770](https://issues.apache.org/jira/browse/YARN-10770) | container-executor permission is wrong in SecureContainer.md |  Major | documentation | Akira Ajisaka | Siddharth Ahuja |
| [YARN-10691](https://issues.apache.org/jira/browse/YARN-10691) | DominantResourceCalculator isInvalidDivisor should consider only countable resource types |  Major | yarn-common | Bilwa S T | Bilwa S T |
| [HDFS-16031](https://issues.apache.org/jira/browse/HDFS-16031) | Possible Resource Leak in org.apache.hadoop.hdfs.server.aliasmap#InMemoryAliasMap |  Major | server | Narges Shadab | Narges Shadab |
| [MAPREDUCE-7348](https://issues.apache.org/jira/browse/MAPREDUCE-7348) | TestFrameworkUploader#testNativeIO fails |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15915](https://issues.apache.org/jira/browse/HDFS-15915) | Race condition with async edits logging due to updating txId outside of the namesystem log |  Major | hdfs, namenode | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-16046](https://issues.apache.org/jira/browse/HDFS-16046) | TestBalanceProcedureScheduler and TestDistCpProcedure timeout |  Major | rbf, test | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17723](https://issues.apache.org/jira/browse/HADOOP-17723) | [build] fix the Dockerfile for ARM |  Blocker | build | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-16051](https://issues.apache.org/jira/browse/HDFS-16051) | Misspelt words in DataXceiver.java line 881 and line 885 |  Trivial | datanode | Ning Sheng | Ning Sheng |
| [HDFS-15998](https://issues.apache.org/jira/browse/HDFS-15998) | Fix NullPointException In listOpenFiles |  Major | namanode | Haiyang Hu | Haiyang Hu |
| [YARN-10797](https://issues.apache.org/jira/browse/YARN-10797) | Logging parameter issues in scheduler package |  Minor | capacity scheduler | Szilard Nemeth | Szilard Nemeth |
| [YARN-10796](https://issues.apache.org/jira/browse/YARN-10796) | Capacity Scheduler: dynamic queue cannot scale out properly if its capacity is 0% |  Major | capacity scheduler, capacityscheduler | Peter Bacsko | Peter Bacsko |
| [HDFS-16050](https://issues.apache.org/jira/browse/HDFS-16050) | Some dynamometer tests fail |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17631](https://issues.apache.org/jira/browse/HADOOP-17631) | Configuration ${env.VAR:-FALLBACK} should eval FALLBACK when restrictSystemProps=true |  Minor | common | Steve Loughran | Steve Loughran |
| [YARN-10809](https://issues.apache.org/jira/browse/YARN-10809) | testWithHbaseConfAtHdfsFileSystem consistently failing |  Major | test | Viraj Jasani | Viraj Jasani |
| [HADOOP-17750](https://issues.apache.org/jira/browse/HADOOP-17750) | Fix asf license errors in newly added files by HADOOP-17727 |  Major | build | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-10803](https://issues.apache.org/jira/browse/YARN-10803) | [JDK 11] TestRMFailoverProxyProvider and TestNoHaRMFailoverProxyProvider fails by ClassCastException |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [HDFS-16057](https://issues.apache.org/jira/browse/HDFS-16057) | Make sure the order for location in ENTERING\_MAINTENANCE state |  Minor | block placement | Tao Li | Tao Li |
| [YARN-10816](https://issues.apache.org/jira/browse/YARN-10816) | Avoid doing delegation token ops when yarn.timeline-service.http-authentication.type=simple |  Major | timelineclient | Tarun Parimi | Tarun Parimi |
| [HADOOP-17645](https://issues.apache.org/jira/browse/HADOOP-17645) | Fix test failures in org.apache.hadoop.fs.azure.ITestOutputStreamSemantics |  Minor | fs/azure | Anoop Sam John | Anoop Sam John |
| [HDFS-16055](https://issues.apache.org/jira/browse/HDFS-16055) | Quota is not preserved in snapshot INode |  Major | hdfs | Siyao Meng | Siyao Meng |
| [HDFS-16068](https://issues.apache.org/jira/browse/HDFS-16068) | WebHdfsFileSystem has a possible connection leak in connection with HttpFS |  Major | webhdfs | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-10767](https://issues.apache.org/jira/browse/YARN-10767) | Yarn Logs Command retrying on Standby RM for 30 times |  Major | yarn-common | D M Murali Krishna Reddy | D M Murali Krishna Reddy |
| [HDFS-15618](https://issues.apache.org/jira/browse/HDFS-15618) | Improve datanode shutdown latency |  Major | datanode | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17760](https://issues.apache.org/jira/browse/HADOOP-17760) | Delete hadoop.ssl.enabled and dfs.https.enable from docs and core-default.xml |  Major | documentation | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-13671](https://issues.apache.org/jira/browse/HDFS-13671) | Namenode deletes large dir slowly caused by FoldedTreeSet#removeAndGet |  Major | namnode | Yiqun Lin | Haibin Huang |
| [HDFS-16061](https://issues.apache.org/jira/browse/HDFS-16061) | DFTestUtil.waitReplication can produce false positives |  Major | hdfs | Ahmed Hussein | Ahmed Hussein |
| [HDFS-14575](https://issues.apache.org/jira/browse/HDFS-14575) | LeaseRenewer#daemon threads leak in DFSClient |  Major | dfsclient | Tao Yang | Renukaprasad C |
| [YARN-10826](https://issues.apache.org/jira/browse/YARN-10826) | [UI2] Upgrade Node.js to at least v12.22.1 |  Major | yarn-ui-v2 | Akira Ajisaka | Masatake Iwasaki |
| [HADOOP-17769](https://issues.apache.org/jira/browse/HADOOP-17769) | Upgrade JUnit to 4.13.2 |  Major | build, test | Ahmed Hussein | Ahmed Hussein |
| [YARN-10824](https://issues.apache.org/jira/browse/YARN-10824) | Title not set for JHS and NM webpages |  Major | nodemanager | Rajshree Mishra | Bilwa S T |
| [HDFS-16092](https://issues.apache.org/jira/browse/HDFS-16092) | Avoid creating LayoutFlags redundant objects |  Major | hdfs | Viraj Jasani | Viraj Jasani |
| [HDFS-16099](https://issues.apache.org/jira/browse/HDFS-16099) | Make bpServiceToActive to be volatile |  Major | datanode | Shuyan Zhang | Shuyan Zhang |
| [HDFS-16109](https://issues.apache.org/jira/browse/HDFS-16109) | Fix flaky some unit tests since they offen timeout |  Minor | test | Tao Li | Tao Li |
| [HDFS-16108](https://issues.apache.org/jira/browse/HDFS-16108) | Incorrect log placeholders used in JournalNodeSyncer |  Minor | journal-node | Viraj Jasani | Viraj Jasani |
| [MAPREDUCE-7353](https://issues.apache.org/jira/browse/MAPREDUCE-7353) | Mapreduce job fails when NM is stopped |  Major | task | Bilwa S T | Bilwa S T |
| [HDFS-16121](https://issues.apache.org/jira/browse/HDFS-16121) | Iterative snapshot diff report can generate duplicate records for creates, deletes and Renames |  Major | snapshots | Srinivasu Majeti | Shashikant Banerjee |
| [HDFS-15796](https://issues.apache.org/jira/browse/HDFS-15796) | ConcurrentModificationException error happens on NameNode occasionally |  Critical | hdfs | Daniel Ma | Daniel Ma |
| [HADOOP-17793](https://issues.apache.org/jira/browse/HADOOP-17793) | Better token validation |  Major | security | Artem Smotrakov | Artem Smotrakov |
| [HDFS-16042](https://issues.apache.org/jira/browse/HDFS-16042) | DatanodeAdminMonitor scan should be delay based |  Major | datanode | Ahmed Hussein | Ahmed Hussein |
| [HDFS-16127](https://issues.apache.org/jira/browse/HDFS-16127) | Improper pipeline close recovery causes a permanent write failure or data loss. |  Major | hdfs | Kihwal Lee | Kihwal Lee |
| [YARN-10855](https://issues.apache.org/jira/browse/YARN-10855) | yarn logs cli fails to retrieve logs if any TFile is corrupt or empty |  Major | yarn | Jim Brennan | Jim Brennan |
| [HDFS-16087](https://issues.apache.org/jira/browse/HDFS-16087) | RBF balance process is stuck at DisableWrite stage |  Major | rbf | Eric Yin | Eric Yin |
| [HADOOP-17028](https://issues.apache.org/jira/browse/HADOOP-17028) | ViewFS should initialize target filesystems lazily |  Major | client-mounts, fs, viewfs | Uma Maheswara Rao G | Abhishek Das |
| [YARN-10630](https://issues.apache.org/jira/browse/YARN-10630) | [UI2] Ambiguous queue name resolution |  Major | yarn-ui-v2 | Andras Gyori | Andras Gyori |
| [HADOOP-17796](https://issues.apache.org/jira/browse/HADOOP-17796) | Upgrade jetty version to 9.4.43 |  Major | build, common | Wei-Chiu Chuang | Renukaprasad C |
| [YARN-10833](https://issues.apache.org/jira/browse/YARN-10833) | RM logs endpoint vulnerable to clickjacking |  Major | webapp | Benjamin Teke | Benjamin Teke |
| [HADOOP-17317](https://issues.apache.org/jira/browse/HADOOP-17317) | [JDK 11] Upgrade dnsjava to remove illegal access warnings |  Major | common | Akira Ajisaka | Akira Ajisaka |
| [HDFS-12920](https://issues.apache.org/jira/browse/HDFS-12920) | HDFS default value change (with adding time unit) breaks old version MR tarball work with Hadoop 3.x |  Critical | configuration, hdfs | Junping Du | Akira Ajisaka |
| [HADOOP-17807](https://issues.apache.org/jira/browse/HADOOP-17807) | Use separate source dir for platform builds |  Critical | build | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16111](https://issues.apache.org/jira/browse/HDFS-16111) | Add a configuration to RoundRobinVolumeChoosingPolicy to avoid failed volumes at datanodes. |  Major | datanode | Zhihai Xu | Zhihai Xu |
| [HDFS-16145](https://issues.apache.org/jira/browse/HDFS-16145) | CopyListing fails with FNF exception with snapshot diff |  Major | distcp | Shashikant Banerjee | Shashikant Banerjee |
| [YARN-10813](https://issues.apache.org/jira/browse/YARN-10813) | Set default capacity of root for node labels |  Major | capacity scheduler | Andras Gyori | Andras Gyori |
| [HDFS-16144](https://issues.apache.org/jira/browse/HDFS-16144) | Revert HDFS-15372 (Files in snapshots no longer see attribute provider permissions) |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [YARN-9551](https://issues.apache.org/jira/browse/YARN-9551) | TestTimelineClientV2Impl.testSyncCall fails intermittently |  Minor | ATSv2, test | Prabhu Joseph | Andras Gyori |
| [HDFS-15175](https://issues.apache.org/jira/browse/HDFS-15175) | Multiple CloseOp shared block instance causes the standby namenode to crash when rolling editlog |  Critical | namnode | Yicong Cai | Wan Chang |
| [YARN-10869](https://issues.apache.org/jira/browse/YARN-10869) | CS considers only the default maximum-allocation-mb/vcore property as a maximum when it creates dynamic queues |  Major | capacity scheduler | Benjamin Teke | Benjamin Teke |
| [YARN-10789](https://issues.apache.org/jira/browse/YARN-10789) | RM HA startup can fail due to race conditions in ZKConfigurationStore |  Major | capacity scheduler | Tarun Parimi | Tarun Parimi |
| [HDFS-14529](https://issues.apache.org/jira/browse/HDFS-14529) | SetTimes to throw FileNotFoundException if inode is not found |  Major | hdfs | Harshakiran Reddy | Wei-Chiu Chuang |
| [HADOOP-17812](https://issues.apache.org/jira/browse/HADOOP-17812) | NPE in S3AInputStream read() after failure to reconnect to store |  Major | fs/s3 | Bobby Wang | Bobby Wang |
| [YARN-6221](https://issues.apache.org/jira/browse/YARN-6221) | Entities missing from ATS when summary log file info got returned to the ATS before the domain log |  Critical | yarn | Sushmitha Sreenivasan | Xiaomin Zhang |
| [MAPREDUCE-7258](https://issues.apache.org/jira/browse/MAPREDUCE-7258) | HistoryServerRest.html#Task\_Counters\_API, modify the jobTaskCounters's itemName from "taskcounterGroup" to "taskCounterGroup". |  Minor | documentation | jenny | jenny |
| [YARN-10878](https://issues.apache.org/jira/browse/YARN-10878) | TestNMSimulator imports com.google.common.base.Supplier; |  Major | buid | Steve Loughran | Steve Loughran |
| [HADOOP-17816](https://issues.apache.org/jira/browse/HADOOP-17816) | Run optional CI for changes in C |  Critical | build | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-17370](https://issues.apache.org/jira/browse/HADOOP-17370) | Upgrade commons-compress to 1.21 |  Major | common | Dongjoon Hyun | Akira Ajisaka |
| [HDFS-16151](https://issues.apache.org/jira/browse/HDFS-16151) | Improve the parameter comments related to ProtobufRpcEngine2#Server() |  Minor | documentation | JiangHua Zhu | JiangHua Zhu |
| [HADOOP-17844](https://issues.apache.org/jira/browse/HADOOP-17844) | Upgrade JSON smart to 2.4.7 |  Major | build, common | Renukaprasad C | Renukaprasad C |
| [YARN-10873](https://issues.apache.org/jira/browse/YARN-10873) | Graceful Decommission ignores launched containers and gets deactivated before timeout |  Major | RM | Prabhu Joseph | Srinivas S T |
| [HDFS-16174](https://issues.apache.org/jira/browse/HDFS-16174) | Refactor TempFile and TempDir in libhdfs++ |  Major | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16177](https://issues.apache.org/jira/browse/HDFS-16177) | Bug fix for Util#receiveFile |  Minor | hdfs-common | Tao Li | Tao Li |
| [HADOOP-17836](https://issues.apache.org/jira/browse/HADOOP-17836) | Improve logging on ABFS error reporting |  Minor | fs/azure | Steve Loughran | Steve Loughran |
| [HDFS-16178](https://issues.apache.org/jira/browse/HDFS-16178) | Make recursive rmdir in libhdfs++ cross platform |  Major | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [YARN-10814](https://issues.apache.org/jira/browse/YARN-10814) | YARN shouldn't start with empty hadoop.http.authentication.signature.secret.file |  Major | security | Benjamin Teke | Tamas Domok |
| [HADOOP-17858](https://issues.apache.org/jira/browse/HADOOP-17858) | Avoid possible class loading deadlock with VerifierNone initialization |  Major | common | Viraj Jasani | Viraj Jasani |
| [HADOOP-17854](https://issues.apache.org/jira/browse/HADOOP-17854) | Run junit in Jenkins only if surefire reports exist |  Major | build | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-17886](https://issues.apache.org/jira/browse/HADOOP-17886) | Upgrade ant to 1.10.11 |  Major | build, common | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17874](https://issues.apache.org/jira/browse/HADOOP-17874) | ExceptionsHandler to add terse/suppressed Exceptions in thread-safe manner |  Major | common | Viraj Jasani | Viraj Jasani |
| [HADOOP-15129](https://issues.apache.org/jira/browse/HADOOP-15129) | Datanode caches namenode DNS lookup failure and cannot startup |  Minor | ipc | Karthik Palaniappan | Chris Nauroth |
| [HDFS-16199](https://issues.apache.org/jira/browse/HDFS-16199) | Resolve log placeholders in NamenodeBeanMetrics |  Minor | metrics | Viraj Jasani | Viraj Jasani |
| [HADOOP-17870](https://issues.apache.org/jira/browse/HADOOP-17870) | HTTP Filesystem to qualify paths in open()/getFileStatus() |  Minor | fs | VinothKumar Raman | VinothKumar Raman |
| [HADOOP-17899](https://issues.apache.org/jira/browse/HADOOP-17899) | Avoid using implicit dependency on junit-jupiter-api |  Major | test | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-10901](https://issues.apache.org/jira/browse/YARN-10901) | Permission checking error on an existing directory in LogAggregationFileController#verifyAndCreateRemoteLogDir |  Major | nodemanager | Tamas Domok | Tamas Domok |
| [HADOOP-17877](https://issues.apache.org/jira/browse/HADOOP-17877) | BuiltInGzipCompressor header and trailer should not be static variables |  Critical | compress, io | L. C. Hsieh | L. C. Hsieh |
| [HADOOP-17804](https://issues.apache.org/jira/browse/HADOOP-17804) | Prometheus metrics only include the last set of labels |  Major | common | Adam Binford | Adam Binford |
| [HDFS-16207](https://issues.apache.org/jira/browse/HDFS-16207) | Remove NN logs stack trace for non-existent xattr query |  Major | namenode | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17901](https://issues.apache.org/jira/browse/HADOOP-17901) | Performance degradation in Text.append() after HADOOP-16951 |  Critical | common | Peter Bacsko | Peter Bacsko |
| [HADOOP-17904](https://issues.apache.org/jira/browse/HADOOP-17904) | Test Result Not Working In Jenkins Result |  Major | build, test | Ayush Saxena | Ayush Saxena |
| [YARN-10903](https://issues.apache.org/jira/browse/YARN-10903) | Too many "Failed to accept allocation proposal" because of wrong Headroom check for DRF |  Major | capacityscheduler | jackwangcs | jackwangcs |
| [HDFS-16187](https://issues.apache.org/jira/browse/HDFS-16187) | SnapshotDiff behaviour with Xattrs and Acls is not consistent across NN restarts with checkpointing |  Major | snapshots | Srinivasu Majeti | Shashikant Banerjee |
| [HDFS-16198](https://issues.apache.org/jira/browse/HDFS-16198) | Short circuit read leaks Slot objects when InvalidToken exception is thrown |  Major | block placement | Eungsop Yoo | Eungsop Yoo |
| [YARN-10870](https://issues.apache.org/jira/browse/YARN-10870) | Missing user filtering check -\> yarn.webapp.filter-entity-list-by-user for RM Scheduler page |  Major | yarn | Siddharth Ahuja | Gergely Pollák |
| [HADOOP-17891](https://issues.apache.org/jira/browse/HADOOP-17891) | lz4-java and snappy-java should be excluded from relocation in shaded Hadoop libraries |  Major | build | L. C. Hsieh | L. C. Hsieh |
| [HADOOP-17907](https://issues.apache.org/jira/browse/HADOOP-17907) | FileUtil#fullyDelete deletes contents of sym-linked directory when symlink cannot be deleted because of local fs fault |  Major | fs | Weihao Zheng | Weihao Zheng |
| [YARN-10936](https://issues.apache.org/jira/browse/YARN-10936) | Fix typo in LogAggregationFileController |  Trivial | log-aggregation | Tamas Domok | Tibor Kovács |
| [HADOOP-17919](https://issues.apache.org/jira/browse/HADOOP-17919) | Fix command line example in Hadoop Cluster Setup documentation |  Minor | documentation | Rintaro Ikeda | Rintaro Ikeda |
| [HADOOP-17902](https://issues.apache.org/jira/browse/HADOOP-17902) | Fix Hadoop build on Debian 10 |  Blocker | build | Gautham Banasandra | Gautham Banasandra |
| [YARN-10937](https://issues.apache.org/jira/browse/YARN-10937) | Fix log message arguments in LogAggregationFileController |  Trivial | . | Tamas Domok | Tibor Kovács |
| [HDFS-16230](https://issues.apache.org/jira/browse/HDFS-16230) | Remove irrelevant trim() call in TestStorageRestore |  Trivial | test | Thomas Leplus | Thomas Leplus |
| [HDFS-16129](https://issues.apache.org/jira/browse/HDFS-16129) | HttpFS signature secret file misusage |  Major | httpfs | Tamas Domok | Tamas Domok |
| [HDFS-16205](https://issues.apache.org/jira/browse/HDFS-16205) | Make hdfs\_allowSnapshot tool cross platform |  Major | hdfs-client, libhdfs++, tools | Gautham Banasandra | Gautham Banasandra |
| [YARN-9606](https://issues.apache.org/jira/browse/YARN-9606) | Set sslfactory for AuthenticatedURL() while creating LogsCLI#webServiceClient |  Major | webservice | Bilwa S T | Bilwa S T |
| [HDFS-16233](https://issues.apache.org/jira/browse/HDFS-16233) | Do not use exception handler to implement copy-on-write for EnumCounters |  Major | namenode | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-16235](https://issues.apache.org/jira/browse/HDFS-16235) | Deadlock in LeaseRenewer for static remove method |  Major | hdfs | angerszhu | angerszhu |
| [HDFS-16236](https://issues.apache.org/jira/browse/HDFS-16236) | Example command for daemonlog is not correct |  Minor | documentation | Renukaprasad C | Renukaprasad C |
| [HADOOP-17931](https://issues.apache.org/jira/browse/HADOOP-17931) | Fix typos in usage message in winutils.exe |  Minor | winutils | Íñigo Goiri | Gautham Banasandra |
| [HDFS-16240](https://issues.apache.org/jira/browse/HDFS-16240) | Replace unshaded guava in HttpFSServerWebServer |  Major | httpfs | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-17940](https://issues.apache.org/jira/browse/HADOOP-17940) | Upgrade Kafka to 2.8.1 |  Major | build, common | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-10970](https://issues.apache.org/jira/browse/YARN-10970) | Standby RM should expose prom endpoint |  Major | resourcemanager | Max  Xie | Max  Xie |
| [YARN-10823](https://issues.apache.org/jira/browse/YARN-10823) | Expose all node labels for root without explicit configurations |  Major | capacity scheduler | Andras Gyori | Andras Gyori |
| [HDFS-16181](https://issues.apache.org/jira/browse/HDFS-16181) | [SBN Read] Fix metric of RpcRequestCacheMissAmount can't display when tailEditLog form JN |  Critical | journal-node, metrics | Zhaohui Wang | Zhaohui Wang |
| [HDFS-16254](https://issues.apache.org/jira/browse/HDFS-16254) | Cleanup protobuf on exit of hdfs\_allowSnapshot |  Major | hdfs-client, libhdfs++, tools | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-17925](https://issues.apache.org/jira/browse/HADOOP-17925) | BUILDING.txt should not encourage to activate docs profile on building binary artifacts |  Minor | documentation | Rintaro Ikeda | Masatake Iwasaki |
| [HDFS-16244](https://issues.apache.org/jira/browse/HDFS-16244) | Add the necessary write lock in Checkpointer#doCheckpoint() |  Major | namenode | JiangHua Zhu | JiangHua Zhu |
| [HADOOP-16532](https://issues.apache.org/jira/browse/HADOOP-16532) | Fix TestViewFsTrash to use the correct homeDir. |  Minor | test, viewfs | Steve Loughran | Xing Lin |
| [HDFS-16268](https://issues.apache.org/jira/browse/HDFS-16268) | Balancer stuck when moving striped blocks due to NPE |  Major | balancer & mover, erasure-coding | Leon Gao | Leon Gao |
| [HDFS-16271](https://issues.apache.org/jira/browse/HDFS-16271) | RBF: NullPointerException when setQuota through routers with quota disabled |  Major | rbf | Chengwei Wang | Chengwei Wang |
| [YARN-10976](https://issues.apache.org/jira/browse/YARN-10976) | Fix resource leak due to Files.walk |  Minor | nodemanager | lujie | lujie |
| [HADOOP-17932](https://issues.apache.org/jira/browse/HADOOP-17932) | Distcp file length comparison have no effect |  Major | common, tools, tools/distcp | yinan zhan | yinan zhan |
| [HDFS-16272](https://issues.apache.org/jira/browse/HDFS-16272) | Int overflow in computing safe length during EC block recovery |  Critical | ec, erasure-coding | daimin | daimin |
| [HADOOP-17908](https://issues.apache.org/jira/browse/HADOOP-17908) | Add missing RELEASENOTES and CHANGELOG to upstream |  Minor | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-17971](https://issues.apache.org/jira/browse/HADOOP-17971) | Exclude IBM Java security classes from being shaded/relocated |  Major | build | Nicholas Marion | Nicholas Marion |
| [HDFS-7612](https://issues.apache.org/jira/browse/HDFS-7612) | TestOfflineEditsViewer.testStored() uses incorrect default value for cacheDir |  Major | test | Konstantin Shvachko | Michael Kuchenbecker |
| [HADOOP-17985](https://issues.apache.org/jira/browse/HADOOP-17985) | Disable JIRA plugin for YETUS on Hadoop |  Critical | build | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16269](https://issues.apache.org/jira/browse/HDFS-16269) | [Fix] Improve NNThroughputBenchmark#blockReport operation |  Major | benchmarks, namenode | JiangHua Zhu | JiangHua Zhu |
| [HDFS-16259](https://issues.apache.org/jira/browse/HDFS-16259) | Catch and re-throw sub-classes of AccessControlException thrown by any permission provider plugins (eg Ranger) |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-16300](https://issues.apache.org/jira/browse/HDFS-16300) | Use libcrypto in Windows for libhdfspp |  Blocker | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16304](https://issues.apache.org/jira/browse/HDFS-16304) | Locate OpenSSL libs for libhdfspp |  Major | build, hdfs-client, native | Sangjin Lee | Gautham Banasandra |
| [HDFS-16311](https://issues.apache.org/jira/browse/HDFS-16311) | Metric metadataOperationRate calculation error in DataNodeVolumeMetrics |  Major | datanode, metrics | Tao Li | Tao Li |
| [YARN-10996](https://issues.apache.org/jira/browse/YARN-10996) | Fix race condition of User object acquisitions |  Major | capacity scheduler | Andras Gyori | Andras Gyori |
| [HADOOP-18006](https://issues.apache.org/jira/browse/HADOOP-18006) | maven-enforcer-plugin's execution of banned-illegal-imports gets overridden in child poms |  Major | build | Viraj Jasani | Viraj Jasani |
| [HDFS-16182](https://issues.apache.org/jira/browse/HDFS-16182) | numOfReplicas is given the wrong value in  BlockPlacementPolicyDefault$chooseTarget can cause DataStreamer to fail with Heterogeneous Storage |  Major | namanode | Max  Xie | Max  Xie |
| [HADOOP-17999](https://issues.apache.org/jira/browse/HADOOP-17999) | No-op implementation of setWriteChecksum and setVerifyChecksum in ViewFileSystem |  Major | viewfs | Abhishek Das | Abhishek Das |
| [HDFS-16329](https://issues.apache.org/jira/browse/HDFS-16329) | Fix log format for BlockManager |  Minor | block placement | Tao Li | Tao Li |
| [HDFS-16330](https://issues.apache.org/jira/browse/HDFS-16330) | Fix incorrect placeholder for Exception logs in DiskBalancer |  Major | datanode | Viraj Jasani | Viraj Jasani |
| [HDFS-16328](https://issues.apache.org/jira/browse/HDFS-16328) | Correct disk balancer param desc |  Minor | documentation, hdfs | guophilipse | guophilipse |
| [HDFS-16334](https://issues.apache.org/jira/browse/HDFS-16334) | Correct NameNode ACL description |  Minor | documentation | guophilipse | guophilipse |
| [HDFS-16343](https://issues.apache.org/jira/browse/HDFS-16343) | Add some debug logs when the dfsUsed are not used during Datanode startup |  Major | datanode | Mukul Kumar Singh | Mukul Kumar Singh |
| [YARN-10760](https://issues.apache.org/jira/browse/YARN-10760) | Number of allocated OPPORTUNISTIC containers can dip below 0 |  Minor | resourcemanager | Andrew Chung | Andrew Chung |
| [HADOOP-18016](https://issues.apache.org/jira/browse/HADOOP-18016) | Make certain methods LimitedPrivate in S3AUtils.java |  Major | fs/s3 | Mehakmeet Singh | Mehakmeet Singh |
| [YARN-10991](https://issues.apache.org/jira/browse/YARN-10991) | Fix to ignore the grouping "[]" for resourcesStr in parseResourcesString method |  Minor | distributed-shell | Ashutosh Gupta | Ashutosh Gupta |
| [HADOOP-17975](https://issues.apache.org/jira/browse/HADOOP-17975) | Fallback to simple auth does not work for a secondary DistributedFileSystem instance |  Major | ipc | István Fajth | István Fajth |
| [HADOOP-17995](https://issues.apache.org/jira/browse/HADOOP-17995) | Stale record should be remove when DataNodePeerMetrics#dumpSendPacketDownstreamAvgInfoAsJson |  Major | common | Haiyang Hu | Haiyang Hu |
| [HDFS-16350](https://issues.apache.org/jira/browse/HDFS-16350) | Datanode start time should be set after RPC server starts successfully |  Minor | datanode | Viraj Jasani | Viraj Jasani |
| [YARN-11007](https://issues.apache.org/jira/browse/YARN-11007) | Correct words in YARN documents |  Minor | documentation | guophilipse | guophilipse |
| [YARN-10975](https://issues.apache.org/jira/browse/YARN-10975) | EntityGroupFSTimelineStore#ActiveLogParser parses already processed files |  Major | timelineserver | Prabhu Joseph | Ravuri Sushma sree |
| [HDFS-16361](https://issues.apache.org/jira/browse/HDFS-16361) | Fix log format for QueryCommand |  Minor | command, diskbalancer | Tao Li | Tao Li |
| [HADOOP-18027](https://issues.apache.org/jira/browse/HADOOP-18027) | Include static imports in the maven plugin rules |  Major | build | Viraj Jasani | Viraj Jasani |
| [HDFS-16359](https://issues.apache.org/jira/browse/HDFS-16359) | RBF: RouterRpcServer#invokeAtAvailableNs does not take effect when retrying |  Major | rbf | Tao Li | Tao Li |
| [HDFS-16332](https://issues.apache.org/jira/browse/HDFS-16332) | Expired block token causes slow read due to missing handling in sasl handshake |  Major | datanode, dfs, dfsclient | Shinya Yoshida | Shinya Yoshida |
| [HADOOP-18021](https://issues.apache.org/jira/browse/HADOOP-18021) | Provide a public wrapper of Configuration#substituteVars |  Major | conf | Andras Gyori | Andras Gyori |
| [HDFS-16369](https://issues.apache.org/jira/browse/HDFS-16369) | RBF: Fix the retry logic of RouterRpcServer#invokeAtAvailableNs |  Major | rbf | Ayush Saxena | Ayush Saxena |
| [HDFS-16370](https://issues.apache.org/jira/browse/HDFS-16370) | Fix assert message for BlockInfo |  Minor | block placement | Tao Li | Tao Li |
| [HDFS-16293](https://issues.apache.org/jira/browse/HDFS-16293) | Client sleeps and holds 'dataQueue' when DataNodes are congested |  Major | hdfs-client | Yuanxin Zhu | Yuanxin Zhu |
| [YARN-9063](https://issues.apache.org/jira/browse/YARN-9063) | ATS 1.5 fails to start if RollingLevelDb files are corrupt or missing |  Major | timelineserver, timelineservice | Tarun Parimi | Ashutosh Gupta |
| [YARN-10757](https://issues.apache.org/jira/browse/YARN-10757) | jsonschema2pojo-maven-plugin version is not defined |  Major | build | Akira Ajisaka | Tamas Domok |
| [YARN-11023](https://issues.apache.org/jira/browse/YARN-11023) | Extend the root QueueInfo with max-parallel-apps in CapacityScheduler |  Major | capacity scheduler | Tamas Domok | Tamas Domok |
| [MAPREDUCE-7368](https://issues.apache.org/jira/browse/MAPREDUCE-7368) | DBOutputFormat.DBRecordWriter#write must throw exception when it fails |  Major | mrv2 | Stamatis Zampetakis | Stamatis Zampetakis |
| [YARN-11016](https://issues.apache.org/jira/browse/YARN-11016) | Queue weight is incorrectly reset to zero |  Major | capacity scheduler | Andras Gyori | Andras Gyori |
| [HDFS-16324](https://issues.apache.org/jira/browse/HDFS-16324) | Fix error log in BlockManagerSafeMode |  Minor | hdfs | guophilipse | guophilipse |
| [HDFS-15788](https://issues.apache.org/jira/browse/HDFS-15788) | Correct the statement for pmem cache to reflect cache persistence support |  Minor | documentation | Feilong He | Feilong He |
| [YARN-11006](https://issues.apache.org/jira/browse/YARN-11006) | Allow overriding user limit factor and maxAMResourcePercent with AQCv2 templates |  Major | capacity scheduler | Benjamin Teke | Benjamin Teke |
| [HDFS-16333](https://issues.apache.org/jira/browse/HDFS-16333) | fix balancer bug when transfer an EC block |  Major | balancer & mover, erasure-coding | qinyuren | qinyuren |
| [YARN-11020](https://issues.apache.org/jira/browse/YARN-11020) | [UI2] No container is found for an application attempt with a single AM container |  Major | yarn-ui-v2 | Andras Gyori | Andras Gyori |
| [HADOOP-18043](https://issues.apache.org/jira/browse/HADOOP-18043) | Use mina-core 2.0.22 to fix LDAP unit test failures |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [HDFS-16373](https://issues.apache.org/jira/browse/HDFS-16373) | Fix MiniDFSCluster restart in case of multiple namenodes |  Major | test | Ayush Saxena | Ayush Saxena |
| [HDFS-16014](https://issues.apache.org/jira/browse/HDFS-16014) | Fix an issue in checking native pmdk lib by 'hadoop checknative' command |  Major | native | Feilong He | Feilong He |
| [YARN-11045](https://issues.apache.org/jira/browse/YARN-11045) | ATSv2 storage monitor fails to read from hbase cluster |  Major | timelineservice | Viraj Jasani | Viraj Jasani |
| [YARN-11044](https://issues.apache.org/jira/browse/YARN-11044) | TestApplicationLimits.testLimitsComputation() has some ineffective asserts |  Major | capacity scheduler, test | Benjamin Teke | Benjamin Teke |
| [HDFS-16377](https://issues.apache.org/jira/browse/HDFS-16377) | Should CheckNotNull before access FsDatasetSpi |  Major | datanode | Tao Li | Tao Li |
| [YARN-10427](https://issues.apache.org/jira/browse/YARN-10427) | Duplicate Job IDs in SLS output |  Major | scheduler-load-simulator | Drew Merrill | Szilard Nemeth |
| [YARN-6862](https://issues.apache.org/jira/browse/YARN-6862) | Nodemanager resource usage metrics sometimes are negative |  Major | nodemanager | YunFan Zhou | Benjamin Teke |
| [HADOOP-13500](https://issues.apache.org/jira/browse/HADOOP-13500) | Synchronizing iteration of Configuration properties object |  Major | conf | Jason Darrell Lowe | Dhananjay Badaya |
| [YARN-11047](https://issues.apache.org/jira/browse/YARN-11047) | ResourceManager and NodeManager unable to connect to Hbase when ATSv2 is enabled |  Major | timelineservice | Minni Mittal | Viraj Jasani |
| [HDFS-16385](https://issues.apache.org/jira/browse/HDFS-16385) | Fix Datanode retrieve slownode information bug. |  Major | datanode | Jackson Wang | Jackson Wang |
| [YARN-10178](https://issues.apache.org/jira/browse/YARN-10178) | Global Scheduler async thread crash caused by 'Comparison method violates its general contract |  Major | capacity scheduler | tuyu | Andras Gyori |
| [HDFS-16392](https://issues.apache.org/jira/browse/HDFS-16392) | TestWebHdfsFileSystemContract#testResponseCode fails |  Major | test | secfree | secfree |
| [YARN-11053](https://issues.apache.org/jira/browse/YARN-11053) | AuxService should not use class name as default system classes |  Major | auxservices | Cheng Pan | Cheng Pan |
| [HDFS-16395](https://issues.apache.org/jira/browse/HDFS-16395) | Remove useless NNThroughputBenchmark#dummyActionNoSynch() |  Major | benchmarks, namenode | JiangHua Zhu | JiangHua Zhu |
| [HADOOP-18057](https://issues.apache.org/jira/browse/HADOOP-18057) | Fix typo: validateEncrytionSecrets -\> validateEncryptionSecrets |  Major | fs/s3 | Ashutosh Gupta | Ashutosh Gupta |
| [HADOOP-18045](https://issues.apache.org/jira/browse/HADOOP-18045) | Disable TestDynamometerInfra |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [HDFS-14099](https://issues.apache.org/jira/browse/HDFS-14099) | Unknown frame descriptor when decompressing multiple frames in ZStandardDecompressor |  Major | compress, io | ZanderXu | ZanderXu |
| [HADOOP-18063](https://issues.apache.org/jira/browse/HADOOP-18063) | Remove unused import AbstractJavaKeyStoreProvider in Shell class |  Minor | command | JiangHua Zhu | JiangHua Zhu |
| [HDFS-16393](https://issues.apache.org/jira/browse/HDFS-16393) | RBF: Fix TestRouterRPCMultipleDestinationMountTableResolver |  Major | rbf | Ayush Saxena | Ayush Saxena |
| [HDFS-16409](https://issues.apache.org/jira/browse/HDFS-16409) | Fix typo: testHasExeceptionsReturnsCorrectValue -\> testHasExceptionsReturnsCorrectValue |  Trivial | test | Ashutosh Gupta | Ashutosh Gupta |
| [HDFS-16408](https://issues.apache.org/jira/browse/HDFS-16408) | Ensure LeaseRecheckIntervalMs is greater than zero |  Major | namenode | ECFuzz | ECFuzz |
| [HDFS-16410](https://issues.apache.org/jira/browse/HDFS-16410) | Insecure Xml parsing in OfflineEditsXmlLoader |  Minor | tools | Ashutosh Gupta | Ashutosh Gupta |
| [HDFS-16417](https://issues.apache.org/jira/browse/HDFS-16417) | RBF: StaticRouterRpcFairnessPolicyController init fails with division by 0 if concurrent ns handler count is configured |  Minor | rbf | Felix N | Felix N |
| [HADOOP-18077](https://issues.apache.org/jira/browse/HADOOP-18077) | ProfileOutputServlet unable to proceed due to NPE |  Major | common | Viraj Jasani | Viraj Jasani |
| [HDFS-16420](https://issues.apache.org/jira/browse/HDFS-16420) | Avoid deleting unique data blocks when deleting redundancy striped blocks |  Critical | ec, erasure-coding | qinyuren | Jackson Wang |
| [YARN-11055](https://issues.apache.org/jira/browse/YARN-11055) | In cgroups-operations.c some fprintf format strings don't end with "\\n" |  Minor | nodemanager | Gera Shegalov | Gera Shegalov |
| [YARN-11065](https://issues.apache.org/jira/browse/YARN-11065) | Bump follow-redirects from 1.13.3 to 1.14.7 in hadoop-yarn-ui |  Major | yarn-ui-v2 | Akira Ajisaka | Akira Ajisaka |
| [HDFS-16402](https://issues.apache.org/jira/browse/HDFS-16402) | Improve HeartbeatManager logic to avoid incorrect stats |  Major | datanode | Tao Li | Tao Li |
| [HADOOP-17593](https://issues.apache.org/jira/browse/HADOOP-17593) | hadoop-huaweicloud and hadoop-cloud-storage to remove log4j as transitive dependency |  Major | build | Steve Loughran | lixianwei |
| [YARN-10561](https://issues.apache.org/jira/browse/YARN-10561) | Upgrade node.js to 12.22.1 and yarn to 1.22.5 in YARN application catalog webapp |  Critical | webapp | Akira Ajisaka | Akira Ajisaka |
| [HDFS-16303](https://issues.apache.org/jira/browse/HDFS-16303) | Losing over 100 datanodes in state decommissioning results in full blockage of all datanode decommissioning |  Major | block placement, datanode | Kevin Wikant | Kevin Wikant |
| [HDFS-16443](https://issues.apache.org/jira/browse/HDFS-16443) | Fix edge case where DatanodeAdminDefaultMonitor doubly enqueues a DatanodeDescriptor on exception |  Major | hdfs | Kevin Wikant | Kevin Wikant |
| [YARN-10822](https://issues.apache.org/jira/browse/YARN-10822) | Containers going from New to Scheduled transition for killed container on recovery |  Major | container, nodemanager | Minni Mittal | Minni Mittal |
| [HADOOP-18101](https://issues.apache.org/jira/browse/HADOOP-18101) | Bump aliyun-sdk-oss to 3.13.2 and jdom2 to 2.0.6.1 |  Major | build, common | Aswin Shakil | Aswin Shakil |
| [HDFS-16411](https://issues.apache.org/jira/browse/HDFS-16411) | RBF: RouterId is NULL when set dfs.federation.router.rpc.enable=false |  Major | rbf | YulongZ | YulongZ |
| [HDFS-16406](https://issues.apache.org/jira/browse/HDFS-16406) | DataNode metric ReadsFromLocalClient does not count short-circuit reads |  Minor | datanode, metrics | secfree | secfree |
| [HADOOP-18096](https://issues.apache.org/jira/browse/HADOOP-18096) | Distcp: Sync moves filtered file to home directory rather than deleting |  Critical | tools/distcp | Ayush Saxena | Ayush Saxena |
| [HDFS-16449](https://issues.apache.org/jira/browse/HDFS-16449) | Fix hadoop web site release notes and changelog not available |  Minor | documentation | guophilipse | guophilipse |
| [YARN-10788](https://issues.apache.org/jira/browse/YARN-10788) | TestCsiClient fails |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-18126](https://issues.apache.org/jira/browse/HADOOP-18126) | Update junit 5 version due to build issues |  Major | build | PJ Fanning | PJ Fanning |
| [YARN-11068](https://issues.apache.org/jira/browse/YARN-11068) | Update transitive log4j2 dependency to 2.17.1 |  Major | buid | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-16316](https://issues.apache.org/jira/browse/HDFS-16316) | Improve DirectoryScanner: add regular file check related block |  Major | datanode | JiangHua Zhu | JiangHua Zhu |
| [YARN-11071](https://issues.apache.org/jira/browse/YARN-11071) | AutoCreatedQueueTemplate incorrect wildcard level |  Major | capacity scheduler | Tamas Domok | Tamas Domok |
| [YARN-11070](https://issues.apache.org/jira/browse/YARN-11070) | Minimum resource ratio is overridden by subsequent labels |  Major | yarn | Andras Gyori | Andras Gyori |
| [YARN-11033](https://issues.apache.org/jira/browse/YARN-11033) | isAbsoluteResource is not correct for dynamically created queues |  Minor | yarn | Tamas Domok | Tamas Domok |
| [YARN-10894](https://issues.apache.org/jira/browse/YARN-10894) | Follow up YARN-10237: fix the new test case in TestRMWebServicesCapacitySched |  Major | webapp | Tamas Domok | Tamas Domok |
| [YARN-11022](https://issues.apache.org/jira/browse/YARN-11022) | Fix the documentation for max-parallel-apps in CS |  Major | capacity scheduler | Tamas Domok | Tamas Domok |
| [YARN-11042](https://issues.apache.org/jira/browse/YARN-11042) | Fix testQueueSubmitWithACLsEnabledWithQueueMapping in TestAppManager |  Major | yarn | Tamas Domok | Tamas Domok |
| [HADOOP-18151](https://issues.apache.org/jira/browse/HADOOP-18151) | Switch the baseurl for Centos 8 |  Blocker | build | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16496](https://issues.apache.org/jira/browse/HDFS-16496) | Snapshot diff on snapshotable directory fails with not snapshottable error |  Major | namanode | Stephen O'Donnell | Stephen O'Donnell |
| [YARN-11067](https://issues.apache.org/jira/browse/YARN-11067) | Resource overcommitment due to incorrect resource normalisation logical order |  Major | capacity scheduler | Andras Gyori | Andras Gyori |
| [HADOOP-18129](https://issues.apache.org/jira/browse/HADOOP-18129) | Change URI[] in INodeLink to String[] to reduce memory footprint of ViewFileSystem |  Major | viewfs | Abhishek Das | Abhishek Das |
| [HDFS-16503](https://issues.apache.org/jira/browse/HDFS-16503) | Should verify whether the path name is valid in the WebHDFS |  Major | webhdfs | Tao Li | Tao Li |
| [YARN-11089](https://issues.apache.org/jira/browse/YARN-11089) | Fix typo in RM audit log |  Major | resourcemanager | Junfan Zhang | Junfan Zhang |
| [YARN-11087](https://issues.apache.org/jira/browse/YARN-11087) | Introduce the config to control the refresh interval in RMDelegatedNodeLabelsUpdater |  Major | nodelabel | Junfan Zhang | Junfan Zhang |
| [YARN-11100](https://issues.apache.org/jira/browse/YARN-11100) | Fix StackOverflowError in SLS scheduler event handling |  Major | scheduler-load-simulator | Szilard Nemeth | Szilard Nemeth |
| [HDFS-16523](https://issues.apache.org/jira/browse/HDFS-16523) | Fix dependency error in hadoop-hdfs on M1 Mac |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HDFS-16498](https://issues.apache.org/jira/browse/HDFS-16498) | Fix NPE for checkBlockReportLease |  Major | datanode, namanode | Tao Li | Tao Li |
| [YARN-11106](https://issues.apache.org/jira/browse/YARN-11106) | Fix the test failure due to missing conf of yarn.resourcemanager.node-labels.am.default-node-label-expression |  Major | test | Junfan Zhang | Junfan Zhang |
| [HDFS-16518](https://issues.apache.org/jira/browse/HDFS-16518) | KeyProviderCache close cached KeyProvider with Hadoop ShutdownHookManager |  Major | hdfs | Lei Yang | Lei Yang |
| [HADOOP-18169](https://issues.apache.org/jira/browse/HADOOP-18169) | getDelegationTokens in ViewFs should also fetch the token from the fallback FS |  Major | fs | Xing Lin | Xing Lin |
| [YARN-11102](https://issues.apache.org/jira/browse/YARN-11102) | Fix spotbugs error in hadoop-sls module |  Major | scheduler-load-simulator | Akira Ajisaka | Szilard Nemeth |
| [HDFS-16479](https://issues.apache.org/jira/browse/HDFS-16479) | EC: NameNode should not send a reconstruction work when the source datanodes are insufficient |  Critical | ec, erasure-coding | Yuanbo Liu | Takanobu Asanuma |
| [HDFS-16509](https://issues.apache.org/jira/browse/HDFS-16509) | Fix decommission UnsupportedOperationException: Remove unsupported |  Major | namenode | daimin | daimin |
| [YARN-11107](https://issues.apache.org/jira/browse/YARN-11107) | When NodeLabel is enabled for a YARN cluster, AM blacklist program does not work properly |  Major | resourcemanager | Xiping Zhang | Xiping Zhang |
| [HDFS-16456](https://issues.apache.org/jira/browse/HDFS-16456) | EC: Decommission a rack with only on dn will fail when the rack number is equal with replication |  Critical | ec, namenode | caozhiqiang | caozhiqiang |
| [HDFS-16535](https://issues.apache.org/jira/browse/HDFS-16535) | SlotReleaser should reuse the domain socket based on socket paths |  Major | hdfs-client | Quanlong Huang | Quanlong Huang |
| [HADOOP-18109](https://issues.apache.org/jira/browse/HADOOP-18109) | Ensure that default permissions of directories under internal ViewFS directories are the same as directories on target filesystems |  Major | viewfs | Chentao Yu | Chentao Yu |
| [HDFS-16422](https://issues.apache.org/jira/browse/HDFS-16422) | Fix thread safety of EC decoding during concurrent preads |  Critical | dfsclient, ec, erasure-coding | daimin | daimin |
| [HDFS-16437](https://issues.apache.org/jira/browse/HDFS-16437) | ReverseXML processor doesn't accept XML files without the SnapshotDiffSection. |  Critical | hdfs | yanbin.zhang | yanbin.zhang |
| [HDFS-16507](https://issues.apache.org/jira/browse/HDFS-16507) | [SBN read] Avoid purging edit log which is in progress |  Critical | namanode | Tao Li | Tao Li |
| [YARN-10720](https://issues.apache.org/jira/browse/YARN-10720) | YARN WebAppProxyServlet should support connection timeout to prevent proxy server from hanging |  Critical | webproxy | Qi Zhu | Qi Zhu |
| [HDFS-16428](https://issues.apache.org/jira/browse/HDFS-16428) | Source path with storagePolicy cause wrong typeConsumed while rename |  Major | hdfs, namenode | lei w | lei w |
| [YARN-11014](https://issues.apache.org/jira/browse/YARN-11014) | YARN incorrectly validates maximum capacity resources on the validation API |  Major | capacity scheduler | Benjamin Teke | Benjamin Teke |
| [YARN-11075](https://issues.apache.org/jira/browse/YARN-11075) | Explicitly declare serialVersionUID in LogMutation class |  Major | capacity scheduler | Benjamin Teke | Benjamin Teke |
| [HDFS-11041](https://issues.apache.org/jira/browse/HDFS-11041) | Unable to unregister FsDatasetState MBean if DataNode is shutdown twice |  Trivial | datanode | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-18160](https://issues.apache.org/jira/browse/HADOOP-18160) | \`org.wildfly.openssl\` should not be shaded by Hadoop build |  Major | build | André F. | André F. |
| [HADOOP-18202](https://issues.apache.org/jira/browse/HADOOP-18202) | create-release fails fatal: unsafe repository ('/build/source' is owned by someone else) |  Major | build | Steve Loughran | Steve Loughran |
| [HDFS-16538](https://issues.apache.org/jira/browse/HDFS-16538) |  EC decoding failed due to not enough valid inputs |  Major | erasure-coding | qinyuren | qinyuren |
| [HDFS-16544](https://issues.apache.org/jira/browse/HDFS-16544) | EC decoding failed due to invalid buffer |  Major | erasure-coding | qinyuren | qinyuren |
| [YARN-11111](https://issues.apache.org/jira/browse/YARN-11111) | Recovery failure when node-label configure-type transit from delegated-centralized to centralized |  Major | yarn | Junfan Zhang | Junfan Zhang |
| [HDFS-16552](https://issues.apache.org/jira/browse/HDFS-16552) | Fix NPE for TestBlockManager |  Major | test | Tao Li | Tao Li |
| [MAPREDUCE-7246](https://issues.apache.org/jira/browse/MAPREDUCE-7246) |  In MapredAppMasterRest#Mapreduce\_Application\_Master\_Info\_API, the datatype of appId should be "string". |  Major | documentation | jenny | Ashutosh Gupta |
| [HADOOP-18216](https://issues.apache.org/jira/browse/HADOOP-18216) | Document "io.file.buffer.size" must be greater than zero |  Minor | io | ECFuzz | ECFuzz |
| [YARN-10187](https://issues.apache.org/jira/browse/YARN-10187) | Removing hadoop-yarn-project/hadoop-yarn/README as it is no longer maintained. |  Minor | documentation | N Sanketh Reddy | Ashutosh Gupta |
| [HDFS-16564](https://issues.apache.org/jira/browse/HDFS-16564) | Use uint32\_t for hdfs\_find |  Major | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-16515](https://issues.apache.org/jira/browse/HADOOP-16515) | Update the link to compatibility guide |  Minor | documentation | Akira Ajisaka | Ashutosh Gupta |
| [HDFS-16185](https://issues.apache.org/jira/browse/HDFS-16185) | Fix comment in LowRedundancyBlocks.java |  Minor | documentation | Akira Ajisaka | Ashutosh Gupta |
| [HADOOP-17479](https://issues.apache.org/jira/browse/HADOOP-17479) | Fix the examples of hadoop config prefix |  Minor | documentation | Akira Ajisaka | Ashutosh Gupta |
| [MAPREDUCE-7376](https://issues.apache.org/jira/browse/MAPREDUCE-7376) | AggregateWordCount fetches wrong results |  Major | aggregate | Ayush Saxena | Ayush Saxena |
| [HDFS-16572](https://issues.apache.org/jira/browse/HDFS-16572) | Fix typo in readme of hadoop-project-dist |  Trivial | documentation | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-18222](https://issues.apache.org/jira/browse/HADOOP-18222) | Prevent DelegationTokenSecretManagerMetrics from registering multiple times |  Major | common | Hector Sandoval Chaverri | Hector Sandoval Chaverri |
| [HDFS-16525](https://issues.apache.org/jira/browse/HDFS-16525) | System.err should be used when error occurs in multiple methods in DFSAdmin class |  Major | dfsadmin | yanbin.zhang | yanbin.zhang |
| [YARN-11123](https://issues.apache.org/jira/browse/YARN-11123) | ResourceManager webapps test failures due to org.apache.hadoop.metrics2.MetricsException and subsequent java.net.BindException: Address already in use |  Major | resourcemanager | Szilard Nemeth | Szilard Nemeth |
| [YARN-11073](https://issues.apache.org/jira/browse/YARN-11073) | Avoid unnecessary preemption for tiny queues under certain corner cases |  Major | capacity scheduler, scheduler preemption | Jian Chen | Jian Chen |
| [MAPREDUCE-7377](https://issues.apache.org/jira/browse/MAPREDUCE-7377) | Remove unused imports in MapReduce project |  Minor | build | Ashutosh Gupta | Ashutosh Gupta |
| [HDFS-16540](https://issues.apache.org/jira/browse/HDFS-16540) | Data locality is lost when DataNode pod restarts in kubernetes |  Major | namenode | Huaxiang Sun | Huaxiang Sun |
| [YARN-11092](https://issues.apache.org/jira/browse/YARN-11092) | Upgrade jquery ui to 1.13.1 |  Major | buid | D M Murali Krishna Reddy | Ashutosh Gupta |
| [YARN-11133](https://issues.apache.org/jira/browse/YARN-11133) | YarnClient gets the wrong EffectiveMinCapacity value |  Major | api | Zilong Zhu | Zilong Zhu |
| [YARN-10850](https://issues.apache.org/jira/browse/YARN-10850) | TimelineService v2 lists containers for all attempts when filtering for one |  Major | timelinereader | Benjamin Teke | Benjamin Teke |
| [YARN-11126](https://issues.apache.org/jira/browse/YARN-11126) | ZKConfigurationStore Java deserialisation vulnerability |  Major | yarn | Tamas Domok | Tamas Domok |
| [YARN-11141](https://issues.apache.org/jira/browse/YARN-11141) | Capacity Scheduler does not support ambiguous queue names when moving application across queues |  Major | capacity scheduler | András Győri | András Győri |
| [YARN-11147](https://issues.apache.org/jira/browse/YARN-11147) | ResourceUsage and QueueCapacities classes provide node label iterators that are not thread safe |  Major | capacity scheduler | András Győri | András Győri |
| [YARN-11152](https://issues.apache.org/jira/browse/YARN-11152) | QueueMetrics is leaking memory when creating a new queue during reinitialisation |  Major | capacity scheduler | András Győri | András Győri |
| [HADOOP-18245](https://issues.apache.org/jira/browse/HADOOP-18245) | Extend KMS related exceptions that get mapped to ConnectException |  Major | kms | Ritesh Shukla | Ritesh Shukla |
| [HADOOP-18120](https://issues.apache.org/jira/browse/HADOOP-18120) | Hadoop auth does not handle HTTP Headers in a case-insensitive way |  Critical | auth | Daniel Fritsi | János Makai |
| [HDFS-16453](https://issues.apache.org/jira/browse/HDFS-16453) | Upgrade okhttp from 2.7.5 to 4.9.3 |  Major | hdfs-client | Ivan Viaznikov | Ashutosh Gupta |
| [YARN-11162](https://issues.apache.org/jira/browse/YARN-11162) | Set the zk acl for nodes created by ZKConfigurationStore. |  Major | resourcemanager | Owen O'Malley | Owen O'Malley |
| [HDFS-16586](https://issues.apache.org/jira/browse/HDFS-16586) | Purge FsDatasetAsyncDiskService threadgroup; it causes BPServiceActor$CommandProcessingThread IllegalThreadStateException 'fatal exception and exit' |  Major | datanode | Michael Stack | Michael Stack |
| [HADOOP-18251](https://issues.apache.org/jira/browse/HADOOP-18251) | Fix failure of extracting JIRA id from commit message in git\_jira\_fix\_version\_check.py |  Minor | build | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-16561](https://issues.apache.org/jira/browse/HDFS-16561) | Handle error returned by strtol |  Major | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [YARN-11128](https://issues.apache.org/jira/browse/YARN-11128) | Fix comments in TestProportionalCapacityPreemptionPolicy\* |  Minor | capacityscheduler, documentation | Ashutosh Gupta | Ashutosh Gupta |
| [HDFS-15225](https://issues.apache.org/jira/browse/HDFS-15225) | RBF: Add snapshot counts to content summary in router |  Major | rbf | Quan Li | Ayush Saxena |
| [HDFS-16583](https://issues.apache.org/jira/browse/HDFS-16583) | DatanodeAdminDefaultMonitor can get stuck in an infinite loop |  Major | datanode | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-16604](https://issues.apache.org/jira/browse/HDFS-16604) | Install gtest via FetchContent\_Declare in CMake |  Blocker | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-18268](https://issues.apache.org/jira/browse/HADOOP-18268) | Install Maven from Apache archives |  Blocker | build | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-18274](https://issues.apache.org/jira/browse/HADOOP-18274) | Use CMake 3.19.0 in Debian 10 |  Blocker | build | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16602](https://issues.apache.org/jira/browse/HDFS-16602) | Use "defined" directive along with #if |  Major | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16608](https://issues.apache.org/jira/browse/HDFS-16608) | Fix the link in TestClientProtocolForPipelineRecovery |  Minor | documentation | Samrat Deb | Samrat Deb |
| [HDFS-16563](https://issues.apache.org/jira/browse/HDFS-16563) | Namenode WebUI prints sensitive information on Token Expiry |  Major | namanode, security, webhdfs | Renukaprasad C | Renukaprasad C |
| [HDFS-16623](https://issues.apache.org/jira/browse/HDFS-16623) | IllegalArgumentException in LifelineSender |  Major | datanode | ZanderXu | ZanderXu |
| [HDFS-16628](https://issues.apache.org/jira/browse/HDFS-16628) | RBF: Correct target directory when move to trash for kerberos login user. |  Major | rbf | Xiping Zhang | Xiping Zhang |
| [HDFS-16064](https://issues.apache.org/jira/browse/HDFS-16064) | Determine when to invalidate corrupt replicas based on number of usable replicas |  Major | datanode, namenode | Kevin Wikant | Kevin Wikant |
| [YARN-9827](https://issues.apache.org/jira/browse/YARN-9827) | Fix Http Response code in GenericExceptionHandler. |  Major | webapp | Abhishek Modi | Ashutosh Gupta |
| [HDFS-16635](https://issues.apache.org/jira/browse/HDFS-16635) | Fix javadoc error in Java 11 |  Major | build, documentation | Akira Ajisaka | Ashutosh Gupta |
| [MAPREDUCE-7387](https://issues.apache.org/jira/browse/MAPREDUCE-7387) | Fix TestJHSSecurity#testDelegationToken AssertionError due to HDFS-16563 |  Major | test | Shilun Fan | Shilun Fan |
| [MAPREDUCE-7369](https://issues.apache.org/jira/browse/MAPREDUCE-7369) | MapReduce tasks timing out when spends more time on MultipleOutputs#close |  Major | mrv1, mrv2 | Prabhu Joseph | Ashutosh Gupta |
| [YARN-11185](https://issues.apache.org/jira/browse/YARN-11185) | Pending app metrics are increased doubly when a queue reaches its max-parallel-apps limit |  Major | capacity scheduler | András Győri | András Győri |
| [MAPREDUCE-7389](https://issues.apache.org/jira/browse/MAPREDUCE-7389) | Typo in description of "mapreduce.application.classpath" in mapred-default.xml |  Trivial | mrv2 | Christian Bartolomäus | Christian Bartolomäus |
| [HADOOP-18159](https://issues.apache.org/jira/browse/HADOOP-18159) | Certificate doesn't match any of the subject alternative names: [\*.s3.amazonaws.com, s3.amazonaws.com] |  Major | fs/s3 | André F. | André F. |
| [YARN-9971](https://issues.apache.org/jira/browse/YARN-9971) | YARN Native Service HttpProbe logs THIS\_HOST in error messages |  Minor | yarn-native-services | Prabhu Joseph | Ashutosh Gupta |
| [MAPREDUCE-7391](https://issues.apache.org/jira/browse/MAPREDUCE-7391) | TestLocalDistributedCacheManager failing after HADOOP-16202 |  Major | test | Steve Loughran | Steve Loughran |
| [YARN-11188](https://issues.apache.org/jira/browse/YARN-11188) | Only files belong to the first file controller are removed even if multiple log aggregation file controllers are configured |  Major | log-aggregation | Szilard Nemeth | Szilard Nemeth |
| [YARN-10974](https://issues.apache.org/jira/browse/YARN-10974) | CS UI: queue filter and openQueues param do not work as expected |  Major | capacity scheduler | Chengbing Liu | Chengbing Liu |
| [HADOOP-18237](https://issues.apache.org/jira/browse/HADOOP-18237) | Upgrade Apache Xerces Java to 2.12.2 |  Major | build | Ashutosh Gupta | Ashutosh Gupta |
| [YARN-10320](https://issues.apache.org/jira/browse/YARN-10320) | Replace FSDataInputStream#read with readFully in Log Aggregation |  Major | log-aggregation | Prabhu Joseph | Ashutosh Gupta |
| [YARN-10303](https://issues.apache.org/jira/browse/YARN-10303) | One yarn rest api example of yarn document is error |  Minor | documentation | bright.zhou | Ashutosh Gupta |
| [HDFS-16633](https://issues.apache.org/jira/browse/HDFS-16633) | Reserved Space For Replicas is not released on some cases |  Major | hdfs | Prabhu Joseph | Ashutosh Gupta |
| [HDFS-16591](https://issues.apache.org/jira/browse/HDFS-16591) | StateStoreZooKeeper fails to initialize |  Major | rbf | Hector Sandoval Chaverri | Hector Sandoval Chaverri |
| [YARN-11204](https://issues.apache.org/jira/browse/YARN-11204) | Various MapReduce tests fail with NPE in AggregatedLogDeletionService.stopRMClient |  Major | log-aggregation | Szilard Nemeth | Szilard Nemeth |
| [HADOOP-18321](https://issues.apache.org/jira/browse/HADOOP-18321) | Fix when to read an additional record from a BZip2 text file split |  Critical | io | Ashutosh Gupta | Ashutosh Gupta |
| [HADOOP-15789](https://issues.apache.org/jira/browse/HADOOP-15789) | DistCp does not clean staging folder if class extends DistCp |  Minor | tools/distcp | Lawrence Andrews | Lawrence Andrews |
| [HADOOP-18100](https://issues.apache.org/jira/browse/HADOOP-18100) | Change scope of inner classes in InodeTree to make them accessible outside package |  Major | viewfs | Abhishek Das | Abhishek Das |
| [HADOOP-18217](https://issues.apache.org/jira/browse/HADOOP-18217) | shutdownhookmanager should not be multithreaded (deadlock possible) |  Minor | util | Catherinot Remi | Catherinot Remi |
| [YARN-11198](https://issues.apache.org/jira/browse/YARN-11198) | Deletion of assigned resources (e.g. GPU's, NUMA, FPGA's) from State Store |  Major | nodemanager | Prabhu Joseph | Samrat Deb |
| [HADOOP-18074](https://issues.apache.org/jira/browse/HADOOP-18074) | Partial/Incomplete groups list can be returned in LDAP groups lookup |  Major | security | Philippe Lanoe | Larry McCay |
| [HDFS-16566](https://issues.apache.org/jira/browse/HDFS-16566) | Erasure Coding: Recovery may cause excess replicas when busy DN exsits |  Major | ec, erasure-coding | Ruinan Gu | Ruinan Gu |
| [HDFS-16654](https://issues.apache.org/jira/browse/HDFS-16654) | Link OpenSSL lib for CMake deps check |  Major | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [YARN-11192](https://issues.apache.org/jira/browse/YARN-11192) | TestRouterWebServicesREST failing after YARN-9827 |  Major | federation | Shilun Fan | Shilun Fan |
| [HDFS-16665](https://issues.apache.org/jira/browse/HDFS-16665) | Fix duplicate sources for hdfspp\_test\_shim\_static |  Critical | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16667](https://issues.apache.org/jira/browse/HDFS-16667) | Use malloc for buffer allocation in uriparser2 |  Major | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [YARN-11211](https://issues.apache.org/jira/browse/YARN-11211) | QueueMetrics leaks Configuration objects when validation API is called multiple times |  Major | capacity scheduler | András Győri | András Győri |
| [HDFS-16680](https://issues.apache.org/jira/browse/HDFS-16680) | Skip libhdfspp Valgrind tests on Windows |  Major | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16681](https://issues.apache.org/jira/browse/HDFS-16681) | Do not pass GCC flags for MSVC in libhdfspp |  Major | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [MAPREDUCE-7372](https://issues.apache.org/jira/browse/MAPREDUCE-7372) | MapReduce set permission too late in copyJar method |  Major | mrv2 | Zhang Dongsheng | Zhang Dongsheng |
| [HDFS-16533](https://issues.apache.org/jira/browse/HDFS-16533) | COMPOSITE\_CRC failed between replicated file and striped file due to invalid requested length |  Major | hdfs, hdfs-client | ZanderXu | ZanderXu |
| [YARN-11210](https://issues.apache.org/jira/browse/YARN-11210) | Fix YARN RMAdminCLI retry logic for non-retryable kerberos configuration exception |  Major | client | Kevin Wikant | Kevin Wikant |
| [HADOOP-18079](https://issues.apache.org/jira/browse/HADOOP-18079) | Upgrade Netty to 4.1.77.Final |  Major | build | Renukaprasad C | Wei-Chiu Chuang |
| [HADOOP-18364](https://issues.apache.org/jira/browse/HADOOP-18364) | All method metrics related to the RPC protocol should be initialized |  Major | metrics | Shuyan Zhang | Shuyan Zhang |
| [HADOOP-18363](https://issues.apache.org/jira/browse/HADOOP-18363) | Fix bug preventing hadoop-metrics2 from emitting metrics to \> 1 Ganglia servers. |  Major | metrics | Ashutosh Gupta | Ashutosh Gupta |
| [HADOOP-18390](https://issues.apache.org/jira/browse/HADOOP-18390) | Fix out of sync import for HADOOP-18321 |  Minor | common | Ashutosh Gupta | Ashutosh Gupta |
| [HADOOP-18387](https://issues.apache.org/jira/browse/HADOOP-18387) | Fix incorrect placeholder in hadoop-common |  Minor | common | Shilun Fan | Shilun Fan |
| [YARN-11237](https://issues.apache.org/jira/browse/YARN-11237) | Bug while disabling proxy failover with Federation |  Major | federation | Ashutosh Gupta | Ashutosh Gupta |
| [HADOOP-18383](https://issues.apache.org/jira/browse/HADOOP-18383) | Codecs with @DoNotPool annotation are not closed causing memory leak |  Major | common | Kevin Sewell | Kevin Sewell |
| [HADOOP-18404](https://issues.apache.org/jira/browse/HADOOP-18404) | Fix broken link to wiki help page in org.apache.hadoop.util.Shell |  Major | documentation | Paul King | Paul King |
| [HDFS-16676](https://issues.apache.org/jira/browse/HDFS-16676) | DatanodeAdminManager$Monitor reports a node as invalid continuously |  Major | namenode | Prabhu Joseph | Ashutosh Gupta |
| [YARN-11254](https://issues.apache.org/jira/browse/YARN-11254) | hadoop-minikdc dependency duplicated in hadoop-yarn-server-nodemanager |  Minor | nodemanager | Clara Fang | Clara Fang |
| [HDFS-16729](https://issues.apache.org/jira/browse/HDFS-16729) | RBF: fix some unreasonably annotated docs |  Major | documentation, rbf | JiangHua Zhu | JiangHua Zhu |
| [YARN-9425](https://issues.apache.org/jira/browse/YARN-9425) | Make initialDelay configurable for FederationStateStoreService#scheduledExecutorService |  Major | federation | Shen Yinjie | Ashutosh Gupta |
| [HDFS-4043](https://issues.apache.org/jira/browse/HDFS-4043) | Namenode Kerberos Login does not use proper hostname for host qualified hdfs principal name. |  Major | security | Ahad Rana | Steve Vaughan |
| [HDFS-16724](https://issues.apache.org/jira/browse/HDFS-16724) | RBF should support get the information about ancestor mount points |  Major | rbf | ZanderXu | ZanderXu |
| [MAPREDUCE-7403](https://issues.apache.org/jira/browse/MAPREDUCE-7403) | Support spark dynamic partitioning in the Manifest Committer |  Major | mrv2 | Steve Loughran | Steve Loughran |
| [HDFS-16728](https://issues.apache.org/jira/browse/HDFS-16728) | RBF throw IndexOutOfBoundsException with disableNameServices |  Major | rbf | ZanderXu | ZanderXu |
| [HDFS-16738](https://issues.apache.org/jira/browse/HDFS-16738) | Invalid CallerContext caused NullPointerException |  Critical | namanode | ZanderXu | ZanderXu |
| [HDFS-16732](https://issues.apache.org/jira/browse/HDFS-16732) | [SBN READ] Avoid get location from observer when the block report is delayed. |  Critical | hdfs | Chenyu Zheng | Chenyu Zheng |
| [HDFS-16734](https://issues.apache.org/jira/browse/HDFS-16734) | RBF: fix some bugs when handling getContentSummary RPC |  Major | rbf | ZanderXu | ZanderXu |
| [HADOOP-18375](https://issues.apache.org/jira/browse/HADOOP-18375) | Fix failure of shelltest for hadoop\_add\_ldlibpath |  Minor | test | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-11287](https://issues.apache.org/jira/browse/YARN-11287) | Fix NoClassDefFoundError: org/junit/platform/launcher/core/LauncherFactory after YARN-10793 |  Major | build, test | Shilun Fan | Shilun Fan |
| [HADOOP-18428](https://issues.apache.org/jira/browse/HADOOP-18428) | Parameterize platform toolset version |  Major | common | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16755](https://issues.apache.org/jira/browse/HDFS-16755) | TestQJMWithFaults.testUnresolvableHostName() can fail due to unexpected host resolution |  Minor | test | Steve Vaughan | Steve Vaughan |
| [HDFS-16750](https://issues.apache.org/jira/browse/HDFS-16750) | NameNode should use NameNode.getRemoteUser() to log audit event to avoid possible NPE |  Major | namanode | ZanderXu | ZanderXu |
| [HDFS-16593](https://issues.apache.org/jira/browse/HDFS-16593) | Correct inaccurate BlocksRemoved metric on DataNode side |  Minor | datanode, metrics | ZanderXu | ZanderXu |
| [HDFS-16748](https://issues.apache.org/jira/browse/HDFS-16748) | RBF: DFSClient should uniquely identify writing files by namespace id and iNodeId |  Critical | rbf | ZanderXu | ZanderXu |
| [HDFS-16659](https://issues.apache.org/jira/browse/HDFS-16659) | JournalNode should throw NewerTxnIdException if SinceTxId is bigger than HighestWrittenTxId |  Critical | journal-node | ZanderXu | ZanderXu |
| [HADOOP-18426](https://issues.apache.org/jira/browse/HADOOP-18426) | Improve the accuracy of MutableStat mean |  Major | common | Shuyan Zhang | Shuyan Zhang |
| [HDFS-16756](https://issues.apache.org/jira/browse/HDFS-16756) | RBF proxies the client's user by the login user to enable CacheEntry |  Major | rbf | ZanderXu | ZanderXu |
| [YARN-11301](https://issues.apache.org/jira/browse/YARN-11301) | Fix NoClassDefFoundError: org/junit/platform/launcher/core/LauncherFactory after YARN-11269 |  Major | test, timelineserver | Shilun Fan | Shilun Fan |
| [HADOOP-18452](https://issues.apache.org/jira/browse/HADOOP-18452) | Fix TestKMS#testKMSHAZooKeeperDelegationToken Failed By Hadoop-18427 |  Major | common, test | Shilun Fan | Shilun Fan |
| [HADOOP-18400](https://issues.apache.org/jira/browse/HADOOP-18400) |  Fix file split duplicating records from a succeeding split when reading BZip2 text files |  Critical | common | Ashutosh Gupta | Ashutosh Gupta |
| [HDFS-16772](https://issues.apache.org/jira/browse/HDFS-16772) | refreshHostsReader should use the new configuration |  Major | datanode, namanode | ZanderXu | ZanderXu |
| [YARN-11305](https://issues.apache.org/jira/browse/YARN-11305) | Fix TestLogAggregationService#testLocalFileDeletionAfterUpload Failed After YARN-11241(#4703) |  Major | log-aggregation | Shilun Fan | Shilun Fan |
| [HADOOP-16674](https://issues.apache.org/jira/browse/HADOOP-16674) | TestDNS.testRDNS can fail with ServiceUnavailableException |  Minor | common, net | Steve Loughran | Ashutosh Gupta |
| [HDFS-16706](https://issues.apache.org/jira/browse/HDFS-16706) | ViewFS doc points to wrong mount table name |  Minor | documentation | Prabhu Joseph | Samrat Deb |
| [YARN-11296](https://issues.apache.org/jira/browse/YARN-11296) | Fix SQLFederationStateStore#Sql script bug |  Major | federation | Shilun Fan | Shilun Fan |
| [HADOOP-18456](https://issues.apache.org/jira/browse/HADOOP-18456) | NullPointerException in ObjectListingIterator's constructor |  Blocker | fs/s3 | Quanlong Huang | Steve Loughran |
| [HADOOP-18444](https://issues.apache.org/jira/browse/HADOOP-18444) | Add Support for localized trash for ViewFileSystem in Trash.moveToAppropriateTrash |  Major | fs | Xing Lin | Xing Lin |
| [HADOOP-18443](https://issues.apache.org/jira/browse/HADOOP-18443) | Upgrade snakeyaml to 1.32 |  Major | security | Ashutosh Gupta | Ashutosh Gupta |
| [HDFS-16766](https://issues.apache.org/jira/browse/HDFS-16766) | hdfs ec command loads (administrator provided) erasure code policy files without disabling xml entity expansion |  Major | erasure-coding, security | Jing | Ashutosh Gupta |
| [HDFS-16798](https://issues.apache.org/jira/browse/HDFS-16798) | SerialNumberMap should decrease current counter if the item exist |  Major | namanode | ZanderXu | ZanderXu |
| [HDFS-16777](https://issues.apache.org/jira/browse/HDFS-16777) | datatables@1.10.17  sonatype-2020-0988 vulnerability |  Major | ui | Eugene Shinn (Truveta) | Ashutosh Gupta |
| [YARN-10680](https://issues.apache.org/jira/browse/YARN-10680) | Revisit try blocks without catch blocks but having finally blocks |  Minor | scheduler-load-simulator | Szilard Nemeth | Susheel Gupta |
| [YARN-11039](https://issues.apache.org/jira/browse/YARN-11039) | LogAggregationFileControllerFactory::getFileControllerForRead can leak threads |  Blocker | log-aggregation | Rajesh Balamohan | Steve Loughran |
| [HADOOP-18471](https://issues.apache.org/jira/browse/HADOOP-18471) | An unhandled ArrayIndexOutOfBoundsException in DefaultStringifier.storeArray() if provided with an empty input |  Minor | common, io | ConfX | ConfX |
| [HADOOP-9946](https://issues.apache.org/jira/browse/HADOOP-9946) | NumAllSinks metrics shows lower value than NumActiveSinks |  Major | metrics | Akira Ajisaka | Ashutosh Gupta |
| [YARN-11357](https://issues.apache.org/jira/browse/YARN-11357) | Fix FederationClientInterceptor#submitApplication Can't Update SubClusterId |  Major | federation | Shilun Fan | Shilun Fan |
| [HADOOP-18499](https://issues.apache.org/jira/browse/HADOOP-18499) | S3A to support HTTPS web proxies |  Major | fs/s3 | Mehakmeet Singh | Mehakmeet Singh |
| [MAPREDUCE-7426](https://issues.apache.org/jira/browse/MAPREDUCE-7426) | Fix typo in class StartEndTImesBase |  Trivial | mrv2 | Samrat Deb | Samrat Deb |
| [YARN-11365](https://issues.apache.org/jira/browse/YARN-11365) | Fix NM class not found on Windows |  Blocker | yarn | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-18233](https://issues.apache.org/jira/browse/HADOOP-18233) | Initialization race condition with TemporaryAWSCredentialsProvider |  Major | auth, fs/s3 | Jason Sleight | Jimmy Wong |
| [MAPREDUCE-7425](https://issues.apache.org/jira/browse/MAPREDUCE-7425) | Document Fix for yarn.app.mapreduce.client-am.ipc.max-retries |  Major | yarn | teng wang | teng wang |
| [MAPREDUCE-7386](https://issues.apache.org/jira/browse/MAPREDUCE-7386) | Maven parallel builds (skipping tests) fail |  Critical | build | Steve Vaughan | Steve Vaughan |
| [YARN-11367](https://issues.apache.org/jira/browse/YARN-11367) | [Federation] Fix DefaultRequestInterceptorREST Client NPE |  Major | federation, router | Shilun Fan | Shilun Fan |
| [HADOOP-18504](https://issues.apache.org/jira/browse/HADOOP-18504) |  An unhandled NullPointerException in class KeyProvider |  Major | common | ConfX | ConfX |
| [HDFS-16834](https://issues.apache.org/jira/browse/HDFS-16834) | PoolAlignmentContext should not max poolLocalStateId with sharedGlobalStateId when sending requests to the namenode. |  Major | namnode | Simbarashe Dzinamarira | Simbarashe Dzinamarira |
| [HADOOP-18528](https://issues.apache.org/jira/browse/HADOOP-18528) | Disable abfs prefetching by default |  Major | fs/azure | Mehakmeet Singh | Mehakmeet Singh |
| [HDFS-16836](https://issues.apache.org/jira/browse/HDFS-16836) | StandbyCheckpointer can still trigger rollback fs image after RU is finalized |  Major | hdfs | Lei Yang | Lei Yang |
| [HADOOP-18429](https://issues.apache.org/jira/browse/HADOOP-18429) | MutableGaugeFloat#incr(float) get stuck in an infinite loop |  Major | metrics | asdfgh19 | Ashutosh Gupta |
| [HADOOP-18324](https://issues.apache.org/jira/browse/HADOOP-18324) | Interrupting RPC Client calls can lead to thread exhaustion |  Critical | ipc | Owen O'Malley | Owen O'Malley |
| [HADOOP-8728](https://issues.apache.org/jira/browse/HADOOP-8728) | Display (fs -text) shouldn't hard-depend on Writable serialized sequence files. |  Minor | fs | Harsh J | Ashutosh Gupta |
| [HADOOP-18532](https://issues.apache.org/jira/browse/HADOOP-18532) | Update command usage in FileSystemShell.md |  Trivial | documentation | guophilipse | guophilipse |
| [HDFS-16832](https://issues.apache.org/jira/browse/HDFS-16832) | [SBN READ] Fix NPE when check the block location of empty directory |  Major | namanode | Chenyu Zheng | Chenyu Zheng |
| [HDFS-16547](https://issues.apache.org/jira/browse/HDFS-16547) | [SBN read] Namenode in safe mode should not be transfered to observer state |  Major | namanode | Tao Li | Tao Li |
| [YARN-8262](https://issues.apache.org/jira/browse/YARN-8262) | get\_executable in container-executor should provide meaningful error codes |  Minor | container-executor | Miklos Szegedi | Susheel Gupta |
| [HDFS-16838](https://issues.apache.org/jira/browse/HDFS-16838) | Fix NPE in testAddRplicaProcessorForAddingReplicaInMap |  Major | test | ZanderXu | ZanderXu |
| [HDFS-16826](https://issues.apache.org/jira/browse/HDFS-16826) | [RBF SBN] ConnectionManager should advance the client stateId for every request |  Major | namanode, rbf | ZanderXu | ZanderXu |
| [HADOOP-18498](https://issues.apache.org/jira/browse/HADOOP-18498) | [ABFS]: Error introduced when SAS Token containing '?' prefix is passed |  Minor | fs/azure | Sree Bhattacharyya | Sree Bhattacharyya |
| [HDFS-16845](https://issues.apache.org/jira/browse/HDFS-16845) | Add configuration flag to enable observer reads on routers without using ObserverReadProxyProvider |  Critical | configuration | Simbarashe Dzinamarira | Simbarashe Dzinamarira |
| [HDFS-16847](https://issues.apache.org/jira/browse/HDFS-16847) | RBF: StateStore writer should not commit tmp fail if there was an error in writing the file. |  Critical | hdfs, rbf | Simbarashe Dzinamarira | Simbarashe Dzinamarira |
| [HADOOP-18408](https://issues.apache.org/jira/browse/HADOOP-18408) | [ABFS]: ITestAbfsManifestCommitProtocol  fails on nonHNS configuration |  Minor | fs/azure, test | Pranav Saxena | Sree Bhattacharyya |
| [HDFS-16550](https://issues.apache.org/jira/browse/HDFS-16550) | [SBN read] Improper cache-size for journal node may cause cluster crash |  Major | journal-node | Tao Li | Tao Li |
| [HDFS-16809](https://issues.apache.org/jira/browse/HDFS-16809) | EC striped block is not sufficient when doing in maintenance |  Major | ec, erasure-coding | dingshun | dingshun |
| [HDFS-16837](https://issues.apache.org/jira/browse/HDFS-16837) | [RBF SBN] ClientGSIContext should merge RouterFederatedStates to get the max state id for each namespace |  Major | rbf | ZanderXu | ZanderXu |
| [HADOOP-18402](https://issues.apache.org/jira/browse/HADOOP-18402) | S3A committer NPE in spark job abort |  Blocker | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-10978](https://issues.apache.org/jira/browse/YARN-10978) | YARN-10978. Fix ApplicationClassLoader to Correctly Expand Glob for Windows Path |  Major | utils | Akshat Bordia | Akshat Bordia |
| [YARN-11386](https://issues.apache.org/jira/browse/YARN-11386) | Fix issue with classpath resolution |  Critical | nodemanager | Gautham Banasandra | Gautham Banasandra |
| [YARN-11390](https://issues.apache.org/jira/browse/YARN-11390) | TestResourceTrackerService.testNodeRemovalNormally: Shutdown nodes should be 0 now expected: \<1\> but was: \<0\> |  Major | yarn | Bence Kosztolnik | Bence Kosztolnik |
| [HDFS-16868](https://issues.apache.org/jira/browse/HDFS-16868) | Fix audit log duplicate issue when an ACE occurs in FSNamesystem. |  Major | fs | Beibei Zhao | Beibei Zhao |
| [HADOOP-18569](https://issues.apache.org/jira/browse/HADOOP-18569) | NFS Gateway may release buffer too early |  Blocker | nfs | Attila Doroszlai | Attila Doroszlai |
| [HADOOP-18574](https://issues.apache.org/jira/browse/HADOOP-18574) | Changing log level of IOStatistics increment to make the DEBUG logs less noisy |  Major | fs/s3 | Mehakmeet Singh | Mehakmeet Singh |
| [HDFS-16852](https://issues.apache.org/jira/browse/HDFS-16852) | Register the shutdown hook only when not in shutdown for KeyProviderCache constructor |  Minor | hdfs | Xing Lin | Xing Lin |
| [HADOOP-18567](https://issues.apache.org/jira/browse/HADOOP-18567) | LogThrottlingHelper: the dependent recorder is not triggered correctly |  Major | common | Chengbing Liu | Chengbing Liu |
| [HDFS-16871](https://issues.apache.org/jira/browse/HDFS-16871) | DiskBalancer process may throws IllegalArgumentException when the target DataNode has capital letter in hostname |  Major | datanode | Daniel Ma | Daniel Ma |
| [HDFS-16689](https://issues.apache.org/jira/browse/HDFS-16689) | Standby NameNode crashes when transitioning to Active with in-progress tailer |  Critical | namanode | ZanderXu | ZanderXu |
| [HDFS-16831](https://issues.apache.org/jira/browse/HDFS-16831) | [RBF SBN] GetNamenodesForNameserviceId should shuffle Observer NameNodes every time |  Major | namanode, rbf | ZanderXu | ZanderXu |
| [YARN-11395](https://issues.apache.org/jira/browse/YARN-11395) | Resource Manager UI, cluster/appattempt/\*, can not present FINAL\_SAVING state |  Critical | yarn | Bence Kosztolnik | Bence Kosztolnik |
| [YARN-10879](https://issues.apache.org/jira/browse/YARN-10879) | Incorrect WARN text in ACL check for application tag based placement |  Minor | resourcemanager | Brian Goerlitz | Susheel Gupta |
| [HDFS-16861](https://issues.apache.org/jira/browse/HDFS-16861) | RBF. Truncate API always fails when dirs use AllResolver oder on Router |  Major | rbf | Max  Xie | Max  Xie |
| [YARN-11392](https://issues.apache.org/jira/browse/YARN-11392) | ClientRMService implemented getCallerUgi and verifyUserAccessForRMApp methods but forget to use sometimes, caused audit log missing. |  Major | yarn | Beibei Zhao | Beibei Zhao |
| [HDFS-16881](https://issues.apache.org/jira/browse/HDFS-16881) | Warn if AccessControlEnforcer runs for a long time to check permission |  Major | namanode | Tsz-wo Sze | Tsz-wo Sze |
| [HDFS-16877](https://issues.apache.org/jira/browse/HDFS-16877) | Namenode doesn't use alignment context in TestObserverWithRouter |  Major | hdfs, rbf | Simbarashe Dzinamarira | Simbarashe Dzinamarira |
| [HADOOP-18581](https://issues.apache.org/jira/browse/HADOOP-18581) | Handle Server KDC re-login when Server and Client run in same JVM. |  Major | common | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-16885](https://issues.apache.org/jira/browse/HDFS-16885) | Fix TestHdfsConfigFields#testCompareConfigurationClassAgainstXml failed |  Major | configuration, namanode | Haiyang Hu | Haiyang Hu |
| [HDFS-16872](https://issues.apache.org/jira/browse/HDFS-16872) | Fix log throttling by declaring LogThrottlingHelper as static members |  Major | namanode | Chengbing Liu | Chengbing Liu |
| [HDFS-16884](https://issues.apache.org/jira/browse/HDFS-16884) | Fix TestFsDatasetImpl#testConcurrentWriteAndDeleteBlock failed |  Major | test | Haiyang Hu | Haiyang Hu |
| [YARN-11190](https://issues.apache.org/jira/browse/YARN-11190) | CS Mapping rule bug: User matcher does not work correctly for usernames with dot |  Major | capacity scheduler | Szilard Nemeth | Szilard Nemeth |
| [YARN-11413](https://issues.apache.org/jira/browse/YARN-11413) | Fix Junit Test ERROR Introduced By YARN-6412 |  Major | api | Shilun Fan | Shilun Fan |
| [HADOOP-18591](https://issues.apache.org/jira/browse/HADOOP-18591) | Fix a typo in Trash |  Minor | documentation | xiaoping.huang | xiaoping.huang |
| [MAPREDUCE-7375](https://issues.apache.org/jira/browse/MAPREDUCE-7375) | JobSubmissionFiles don't set right permission after mkdirs |  Major | mrv2 | Zhang Dongsheng | Zhang Dongsheng |
| [HDFS-16764](https://issues.apache.org/jira/browse/HDFS-16764) | ObserverNamenode handles addBlock rpc and throws a FileNotFoundException |  Critical | namanode | ZanderXu | ZanderXu |
| [HDFS-16876](https://issues.apache.org/jira/browse/HDFS-16876) | Garbage collect map entries in shared RouterStateIdContext using information from namenodeResolver instead of the map of active connectionPools. |  Critical | rbf | Simbarashe Dzinamarira | Simbarashe Dzinamarira |
| [HADOOP-17717](https://issues.apache.org/jira/browse/HADOOP-17717) | Update wildfly openssl to 1.1.3.Final |  Major | build, common | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-18601](https://issues.apache.org/jira/browse/HADOOP-18601) | Fix build failure with docs profile |  Major | build | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-18598](https://issues.apache.org/jira/browse/HADOOP-18598) | maven site generation doesn't include javadocs |  Blocker | site | Steve Loughran | Steve Loughran |
| [HDFS-16821](https://issues.apache.org/jira/browse/HDFS-16821) | Fix regression in HDFS-13522 that enables observer reads by default. |  Major | hdfs | Simbarashe Dzinamarira | Simbarashe Dzinamarira |
| [HADOOP-18584](https://issues.apache.org/jira/browse/HADOOP-18584) | [NFS GW] Fix regression after netty4 migration |  Major | common | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-18279](https://issues.apache.org/jira/browse/HADOOP-18279) | Cancel fileMonitoringTimer even if trustManager isn't defined |  Major | common, test | Steve Vaughan | Steve Vaughan |
| [HADOOP-18576](https://issues.apache.org/jira/browse/HADOOP-18576) | Java 11 JavaDoc fails due to missing package comments |  Major | build, common | Steve Loughran | Steve Vaughan |
| [HADOOP-18612](https://issues.apache.org/jira/browse/HADOOP-18612) | Avoid mixing canonical and non-canonical when performing comparisons |  Minor | common, test | Steve Vaughan | Steve Vaughan |
| [HDFS-16895](https://issues.apache.org/jira/browse/HDFS-16895) | NamenodeHeartbeatService should use credentials of logged in user |  Major | rbf | Hector Sandoval Chaverri | Hector Sandoval Chaverri |
| [HDFS-16910](https://issues.apache.org/jira/browse/HDFS-16910) | Fix incorrectly initializing RandomAccessFile caused flush performance decreased for JN |  Major | namanode | Haiyang Hu | Haiyang Hu |
| [HDFS-16761](https://issues.apache.org/jira/browse/HDFS-16761) | Namenode UI for Datanodes page not loading if any data node is down |  Major | namenode, ui | Krishna Reddy | Zita Dombi |
| [HDFS-16925](https://issues.apache.org/jira/browse/HDFS-16925) | Namenode audit log to only include IP address of client |  Major | namanode | Viraj Jasani | Viraj Jasani |
| [YARN-11408](https://issues.apache.org/jira/browse/YARN-11408) | Add a check of autoQueueCreation is disabled for emitDefaultUserLimitFactor method |  Major | yarn | Susheel Gupta | Susheel Gupta |
| [HADOOP-18582](https://issues.apache.org/jira/browse/HADOOP-18582) | No need to clean tmp files in distcp direct mode |  Major | tools/distcp | 10000kang | 10000kang |
| [HADOOP-18641](https://issues.apache.org/jira/browse/HADOOP-18641) | cyclonedx maven plugin breaks builds on recent maven releases (3.9.0) |  Major | build | Steve Loughran | Steve Loughran |
| [MAPREDUCE-7428](https://issues.apache.org/jira/browse/MAPREDUCE-7428) | Fix failures related to Junit 4 to Junit 5 upgrade in org.apache.hadoop.mapreduce.v2.app.webapp |  Critical | test | Ashutosh Gupta | Akira Ajisaka |
| [HADOOP-18636](https://issues.apache.org/jira/browse/HADOOP-18636) | LocalDirAllocator cannot recover from directory tree deletion during the life of a filesystem client |  Minor | fs, fs/azure, fs/s3 | Steve Loughran | Steve Loughran |
| [MAPREDUCE-7434](https://issues.apache.org/jira/browse/MAPREDUCE-7434) | Fix ShuffleHandler tests |  Major | tets | Tamas Domok | Tamas Domok |
| [HDFS-16935](https://issues.apache.org/jira/browse/HDFS-16935) | TestFsDatasetImpl.testReportBadBlocks brittle |  Minor | test | Steve Loughran | Viraj Jasani |
| [HDFS-16923](https://issues.apache.org/jira/browse/HDFS-16923) | The getListing RPC will throw NPE if the path does not exist |  Critical | namenode | ZanderXu | ZanderXu |
| [HDFS-16896](https://issues.apache.org/jira/browse/HDFS-16896) | HDFS Client hedged read has increased failure rate than without hedged read |  Major | hdfs-client | Tom McCormick | Tom McCormick |
| [YARN-11383](https://issues.apache.org/jira/browse/YARN-11383) | Workflow priority mappings is case sensitive |  Major | yarn | Aparajita Choudhary | Aparajita Choudhary |
| [HDFS-16939](https://issues.apache.org/jira/browse/HDFS-16939) | Fix the thread safety bug in LowRedundancyBlocks |  Major | namanode | Shuyan Zhang | Shuyan Zhang |
| [HDFS-16934](https://issues.apache.org/jira/browse/HDFS-16934) | org.apache.hadoop.hdfs.tools.TestDFSAdmin#testAllDatanodesReconfig regression |  Minor | dfsadmin, test | Steve Loughran | Shilun Fan |
| [HDFS-16942](https://issues.apache.org/jira/browse/HDFS-16942) | Send error to datanode if FBR is rejected due to bad lease |  Major | datanode, namenode | Stephen O'Donnell | Stephen O'Donnell |
| [HADOOP-18668](https://issues.apache.org/jira/browse/HADOOP-18668) | Path capability probe for truncate is only honored by RawLocalFileSystem |  Major | fs, httpfs, viewfs | Viraj Jasani | Viraj Jasani |
| [HADOOP-18666](https://issues.apache.org/jira/browse/HADOOP-18666) | A whitelist of endpoints to skip Kerberos authentication doesn't work for ResourceManager and Job History Server |  Major | security | YUBI LEE | YUBI LEE |
| [HADOOP-18329](https://issues.apache.org/jira/browse/HADOOP-18329) | Add support for IBM Semeru OE JRE 11.0.15.0 and greater |  Major | auth, common | Jack | Jack |
| [HADOOP-18662](https://issues.apache.org/jira/browse/HADOOP-18662) | ListFiles with recursive fails with FNF |  Major | common | Ayush Saxena | Ayush Saxena |
| [YARN-11461](https://issues.apache.org/jira/browse/YARN-11461) | NPE in determineMissingParents when the queue is invalid |  Major | capacity scheduler | Tamas Domok | Tamas Domok |
| [HADOOP-18548](https://issues.apache.org/jira/browse/HADOOP-18548) | Hadoop Archive tool (HAR) should acquire delegation tokens from source and destination file systems |  Major | tools | Wei-Chiu Chuang | Szabolcs Gál |
| [HADOOP-18680](https://issues.apache.org/jira/browse/HADOOP-18680) | Insufficient heap during full test runs in Docker container. |  Minor | build | Chris Nauroth | Chris Nauroth |
| [HDFS-16949](https://issues.apache.org/jira/browse/HDFS-16949) | Update ReadTransferRate to ReadLatencyPerGB for effective percentile metrics |  Minor | datanode | Ravindra Dingankar | Ravindra Dingankar |
| [HDFS-16911](https://issues.apache.org/jira/browse/HDFS-16911) | Distcp with snapshot diff to support Ozone filesystem. |  Major | distcp | Sadanand Shenoy | Sadanand Shenoy |
| [YARN-11326](https://issues.apache.org/jira/browse/YARN-11326) | [Federation] Add RM FederationStateStoreService Metrics |  Major | federation, resourcemanager | Shilun Fan | Shilun Fan |
| [HDFS-16982](https://issues.apache.org/jira/browse/HDFS-16982) | Use the right Quantiles Array for Inverse Quantiles snapshot |  Minor | datanode, metrics | Ravindra Dingankar | Ravindra Dingankar |
| [HDFS-16954](https://issues.apache.org/jira/browse/HDFS-16954) | RBF: The operation of renaming a multi-subcluster directory to a single-cluster directory should throw ioexception |  Minor | rbf | Max  Xie | Max  Xie |
| [HDFS-16986](https://issues.apache.org/jira/browse/HDFS-16986) | EC: Fix locationBudget in getListing() |  Major | erasure-coding | Shuyan Zhang | Shuyan Zhang |
| [HADOOP-18714](https://issues.apache.org/jira/browse/HADOOP-18714) | Wrong StringUtils.join() called in AbstractContractRootDirectoryTest |  Trivial | test | Attila Doroszlai | Attila Doroszlai |
| [HADOOP-18705](https://issues.apache.org/jira/browse/HADOOP-18705) | ABFS should exclude incompatible credential providers |  Major | fs/azure | Tamas Domok | Tamas Domok |
| [HDFS-16975](https://issues.apache.org/jira/browse/HDFS-16975) | FileWithSnapshotFeature.isCurrentFileDeleted is not reloaded from FSImage. |  Major | namanode | Tsz-wo Sze | Tsz-wo Sze |
| [HADOOP-18660](https://issues.apache.org/jira/browse/HADOOP-18660) | Filesystem Spelling Mistake |  Trivial | fs | Sebastian Baunsgaard | Sebastian Baunsgaard |
| [MAPREDUCE-7437](https://issues.apache.org/jira/browse/MAPREDUCE-7437) | MR Fetcher class to use an AtomicInteger to generate IDs. |  Major | build, client | Steve Loughran | Steve Loughran |
| [HDFS-16672](https://issues.apache.org/jira/browse/HDFS-16672) | Fix lease interval comparison in BlockReportLeaseManager |  Trivial | namenode | dzcxzl | dzcxzl |
| [YARN-11459](https://issues.apache.org/jira/browse/YARN-11459) | Consider changing label called "max resource" on UIv1 and UIv2 |  Major | yarn, yarn-ui-v2 | Riya Khandelwal | Riya Khandelwal |
| [HDFS-16972](https://issues.apache.org/jira/browse/HDFS-16972) | Delete a snapshot may deleteCurrentFile |  Major | snapshots | Tsz-wo Sze | Tsz-wo Sze |
| [YARN-11482](https://issues.apache.org/jira/browse/YARN-11482) | Fix bug of DRF comparison DominantResourceFairnessComparator2 in fair scheduler |  Major | fairscheduler | Xiaoqiao He | Xiaoqiao He |
| [HDFS-16897](https://issues.apache.org/jira/browse/HDFS-16897) | Fix abundant Broken pipe exception in BlockSender |  Minor | hdfs | fanluo | fanluo |
| [HADOOP-18729](https://issues.apache.org/jira/browse/HADOOP-18729) | Fix mvnsite on Windows 10 |  Critical | build, site | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16865](https://issues.apache.org/jira/browse/HDFS-16865) | RBF: The source path is always / after RBF proxied the complete, addBlock and getAdditionalDatanode RPC. |  Major | rbf | ZanderXu | ZanderXu |
| [HADOOP-18734](https://issues.apache.org/jira/browse/HADOOP-18734) | Create qbt.sh symlink on Windows |  Critical | build | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16999](https://issues.apache.org/jira/browse/HDFS-16999) | Fix wrong use of processFirstBlockReport() |  Major | block placement | Shuyan Zhang | Shuyan Zhang |
| [YARN-11467](https://issues.apache.org/jira/browse/YARN-11467) | RM failover may fail when the nodes.exclude-path file does not exist |  Minor | resourcemanager | dzcxzl | dzcxzl |
| [HDFS-16985](https://issues.apache.org/jira/browse/HDFS-16985) | Fix data missing issue when delete local block file. |  Major | datanode | Chengwei Wang | Chengwei Wang |
| [YARN-11489](https://issues.apache.org/jira/browse/YARN-11489) | Fix memory leak of DelegationTokenRenewer futures in DelegationTokenRenewerPoolTracker |  Major | resourcemanager | Chun Chen | Chun Chen |
| [YARN-11312](https://issues.apache.org/jira/browse/YARN-11312) | [UI2] Refresh buttons don't work after EmberJS upgrade |  Minor | yarn-ui-v2 | Brian Goerlitz | Susheel Gupta |
| [HADOOP-18652](https://issues.apache.org/jira/browse/HADOOP-18652) | Path.suffix raises NullPointerException |  Minor | hdfs-client | Patrick Grandjean | Patrick Grandjean |
| [HDFS-17018](https://issues.apache.org/jira/browse/HDFS-17018) | Improve dfsclient log format |  Minor | dfsclient | Xianming Lei | Xianming Lei |
| [HDFS-16697](https://issues.apache.org/jira/browse/HDFS-16697) | Add logs if resources are not available in NameNodeResourcePolicy |  Minor | namenode | ECFuzz | ECFuzz |
| [HADOOP-17518](https://issues.apache.org/jira/browse/HADOOP-17518) | Usage of incorrect regex range A-z |  Minor | httpfs | Marcono1234 | Nishtha Shah |
| [HDFS-17022](https://issues.apache.org/jira/browse/HDFS-17022) | Fix the exception message to print the Identifier pattern |  Minor | httpfs | Nishtha Shah | Nishtha Shah |
| [HDFS-17017](https://issues.apache.org/jira/browse/HDFS-17017) | Fix the issue of arguments number limit in report command in DFSAdmin. |  Major | dfsadmin | Haiyang Hu | Haiyang Hu |
| [HADOOP-18746](https://issues.apache.org/jira/browse/HADOOP-18746) | Install Python 3 for Windows 10 docker image |  Major | build | Gautham Banasandra | Gautham Banasandra |
| [YARN-11490](https://issues.apache.org/jira/browse/YARN-11490) | JMX QueueMetrics breaks after mutable config validation in CS |  Major | capacityscheduler | Tamas Domok | Tamas Domok |
| [HDFS-17000](https://issues.apache.org/jira/browse/HDFS-17000) | Potential infinite loop in TestDFSStripedOutputStreamUpdatePipeline.testDFSStripedOutputStreamUpdatePipeline |  Major | test | Marcono1234 | Marcono1234 |
| [HDFS-17027](https://issues.apache.org/jira/browse/HDFS-17027) | RBF: Add supports for observer.auto-msync-period when using routers |  Major | rbf | Simbarashe Dzinamarira | Simbarashe Dzinamarira |
| [HDFS-16996](https://issues.apache.org/jira/browse/HDFS-16996) | Fix flaky testFsCloseAfterClusterShutdown in TestFileCreation |  Major | test | Uma Maheswara Rao G | Nishtha Shah |
| [HDFS-16983](https://issues.apache.org/jira/browse/HDFS-16983) | Fix concat operation doesn't honor dfs.permissions.enabled |  Major | namenode | caozhiqiang | caozhiqiang |
| [HDFS-17011](https://issues.apache.org/jira/browse/HDFS-17011) | Fix the metric of  "HttpPort" at DataNodeInfo |  Minor | datanode | Zhaohui Wang | Zhaohui Wang |
| [HDFS-17019](https://issues.apache.org/jira/browse/HDFS-17019) |  Optimize the logic for reconfigure slow peer enable for Namenode |  Major | namanode | Haiyang Hu | Haiyang Hu |
| [HDFS-17003](https://issues.apache.org/jira/browse/HDFS-17003) | Erasure Coding: invalidate wrong block after reporting bad blocks from datanode |  Critical | namenode | farmmamba | farmmamba |
| [HADOOP-18718](https://issues.apache.org/jira/browse/HADOOP-18718) | Fix several maven build warnings |  Minor | build | Dongjoon Hyun | Dongjoon Hyun |
| [MAPREDUCE-7435](https://issues.apache.org/jira/browse/MAPREDUCE-7435) | ManifestCommitter OOM on azure job |  Major | client | Steve Loughran | Steve Loughran |
| [HDFS-16946](https://issues.apache.org/jira/browse/HDFS-16946) | RBF: top real owners metrics can't been parsed json string |  Minor | rbf | Max  Xie | Nishtha Shah |
| [HDFS-17041](https://issues.apache.org/jira/browse/HDFS-17041) | RBF: Fix putAll impl for mysql and file based state stores |  Major | rbf | Viraj Jasani | Viraj Jasani |
| [HDFS-17045](https://issues.apache.org/jira/browse/HDFS-17045) | File renamed from a snapshottable dir to a non-snapshottable dir cannot be deleted. |  Major | namenode, snapshots | Tsz-wo Sze | Tsz-wo Sze |
| [YARN-11513](https://issues.apache.org/jira/browse/YARN-11513) | Applications submitted to ambiguous queue fail during recovery if "Specified" Placement Rule is used |  Major | yarn | Susheel Gupta | Susheel Gupta |
| [MAPREDUCE-7441](https://issues.apache.org/jira/browse/MAPREDUCE-7441) | Race condition in closing FadvisedFileRegion |  Major | yarn | Benjamin Teke | Benjamin Teke |
| [HADOOP-18751](https://issues.apache.org/jira/browse/HADOOP-18751) | Fix incorrect output path in javadoc build phase |  Critical | build | Gautham Banasandra | Gautham Banasandra |
| [HDFS-17052](https://issues.apache.org/jira/browse/HDFS-17052) | Improve BlockPlacementPolicyRackFaultTolerant to avoid choose nodes failed when no enough Rack. |  Major | namanode | Hualong Zhang | Hualong Zhang |
| [YARN-11528](https://issues.apache.org/jira/browse/YARN-11528) | Lock triple-beam to the version compatible with node.js 12 to avoid compilation error |  Major | build | Ayush Saxena | Masatake Iwasaki |
| [YARN-11464](https://issues.apache.org/jira/browse/YARN-11464) | TestFSQueueConverter#testAutoCreateV2FlagsInWeightMode has a missing dot before auto-queue-creation-v2.enabled for method call assertNoValueForQueues |  Major | yarn | Susheel Gupta | Susheel Gupta |
| [HDFS-17081](https://issues.apache.org/jira/browse/HDFS-17081) | EC: Add logic for striped blocks in isSufficientlyReplicated |  Major | erasure-coding | Haiyang Hu | Haiyang Hu |
| [HADOOP-18757](https://issues.apache.org/jira/browse/HADOOP-18757) | S3A Committer only finalizes the commits in a single thread |  Major | fs/s3 | Moditha Hewasinghage | Moditha Hewasinghage |
| [HDFS-17094](https://issues.apache.org/jira/browse/HDFS-17094) | EC: Fix bug in block recovery when there are stale datanodes |  Major | erasure-coding | Shuyan Zhang | Shuyan Zhang |
| [YARN-9877](https://issues.apache.org/jira/browse/YARN-9877) | Intermittent TIME\_OUT of LogAggregationReport |  Major | log-aggregation, resourcemanager, yarn | Adam Antal | Adam Antal |
| [HDFS-17067](https://issues.apache.org/jira/browse/HDFS-17067) | Use BlockingThreadPoolExecutorService for nnProbingThreadPool in ObserverReadProxy |  Major | hdfs | Xing Lin | Xing Lin |
| [YARN-11534](https://issues.apache.org/jira/browse/YARN-11534) | Incorrect exception handling during container recovery |  Major | yarn | Peter Szucs | Peter Szucs |
| [HADOOP-18807](https://issues.apache.org/jira/browse/HADOOP-18807) | Close child file systems in ViewFileSystem when cache is disabled. |  Major | fs | Shuyan Zhang | Shuyan Zhang |
| [MAPREDUCE-7442](https://issues.apache.org/jira/browse/MAPREDUCE-7442) | exception message is not intusive when accessing the job configuration web UI |  Major | applicationmaster | Jiandan Yang | Jiandan Yang |
| [HADOOP-18823](https://issues.apache.org/jira/browse/HADOOP-18823) | Add Labeler Github Action. |  Major | build | Ayush Saxena | Ayush Saxena |
| [HDFS-17111](https://issues.apache.org/jira/browse/HDFS-17111) | RBF: Optimize msync to only call nameservices that have observer reads enabled. |  Major | rbf | Simbarashe Dzinamarira | Simbarashe Dzinamarira |
| [YARN-11539](https://issues.apache.org/jira/browse/YARN-11539) | Flexible AQC: setting capacity with leaf-template doesn't work |  Major | capacity scheduler | Benjamin Teke | Benjamin Teke |
| [YARN-11538](https://issues.apache.org/jira/browse/YARN-11538) | CS UI: queue filter do not work as expected when submitting apps with leaf queue‘s name |  Major | resourcemanager | Jiandan Yang | Jiandan Yang |
| [HDFS-17134](https://issues.apache.org/jira/browse/HDFS-17134) | RBF: Fix duplicate results of getListing through Router. |  Major | rbf | Shuyan Zhang | Shuyan Zhang |
| [MAPREDUCE-7446](https://issues.apache.org/jira/browse/MAPREDUCE-7446) | NegativeArraySizeException when running MR jobs with large data size |  Major | mrv1 | Peter Szucs | Peter Szucs |
| [YARN-11545](https://issues.apache.org/jira/browse/YARN-11545) | FS2CS not converts ACLs when all users are allowed |  Major | yarn | Peter Szucs | Peter Szucs |
| [HDFS-17122](https://issues.apache.org/jira/browse/HDFS-17122) | Rectify the table length discrepancy in the DataNode UI. |  Major | ui | Hualong Zhang | Hualong Zhang |
| [HADOOP-18826](https://issues.apache.org/jira/browse/HADOOP-18826) | abfs getFileStatus(/) fails with "Value for one of the query parameters specified in the request URI is invalid.", 400 |  Major | fs/azure | Sergey Shabalov | Anuj Modi |
| [HDFS-17150](https://issues.apache.org/jira/browse/HDFS-17150) | EC: Fix the bug of failed lease recovery. |  Major | erasure-coding | Shuyan Zhang | Shuyan Zhang |
| [HDFS-17154](https://issues.apache.org/jira/browse/HDFS-17154) | EC: Fix bug in updateBlockForPipeline after failover |  Major | erasure-coding | Shuyan Zhang | Shuyan Zhang |
| [HDFS-17156](https://issues.apache.org/jira/browse/HDFS-17156) | Client may receive old state ID which will lead to inconsistent reads |  Minor | rbf | Chunyi Yang | Chunyi Yang |
| [YARN-11551](https://issues.apache.org/jira/browse/YARN-11551) | RM format-conf-store should delete all the content of ZKConfigStore |  Major | resourcemanager | Benjamin Teke | Benjamin Teke |
| [HDFS-17151](https://issues.apache.org/jira/browse/HDFS-17151) | EC: Fix wrong metadata in BlockInfoStriped after recovery |  Major | erasure-coding | Shuyan Zhang | Shuyan Zhang |
| [HDFS-17093](https://issues.apache.org/jira/browse/HDFS-17093) | Fix block report lease issue to avoid missing some storages report. |  Minor | namenode | Yanlei Yu | Yanlei Yu |
| [YARN-11552](https://issues.apache.org/jira/browse/YARN-11552) | timeline endpoint: /clusters/{clusterid}/apps/{appid}/entity-types Error when using hdfs store |  Major | timelineservice | Jiandan Yang | Jiandan Yang |
| [YARN-11554](https://issues.apache.org/jira/browse/YARN-11554) | Fix TestRMFailover#testWebAppProxyInStandAloneMode Failed |  Major | resourcemanager | Shilun Fan | Shilun Fan |
| [HDFS-17166](https://issues.apache.org/jira/browse/HDFS-17166) | RBF: Throwing NoNamenodesAvailableException for a long time, when failover |  Major | rbf | Jian Zhang | Jian Zhang |
| [HDFS-16933](https://issues.apache.org/jira/browse/HDFS-16933) | A race in SerialNumberMap will cause wrong owner, group and XATTR |  Major | namanode | ZanderXu | ZanderXu |
| [HDFS-17167](https://issues.apache.org/jira/browse/HDFS-17167) | Observer NameNode -observer startup option conflicts with -rollingUpgrade startup option |  Minor | namenode | Danny Becker | Danny Becker |
| [HADOOP-18870](https://issues.apache.org/jira/browse/HADOOP-18870) | CURATOR-599 change broke functionality introduced in HADOOP-18139 and HADOOP-18709 |  Major | common | Ferenc Erdelyi | Ferenc Erdelyi |
| [HADOOP-18824](https://issues.apache.org/jira/browse/HADOOP-18824) | ZKDelegationTokenSecretManager causes ArithmeticException due to improper numRetries value checking |  Critical | common | ConfX | ConfX |
| [HDFS-17190](https://issues.apache.org/jira/browse/HDFS-17190) | EC: Fix bug of OIV processing XAttr. |  Major | erasure-coding | Shuyan Zhang | Shuyan Zhang |
| [HDFS-17138](https://issues.apache.org/jira/browse/HDFS-17138) | RBF: We changed the hadoop.security.auth\_to\_local configuration of one router, the other routers stopped working |  Major | rbf | Xiping Zhang | Xiping Zhang |
| [HDFS-17105](https://issues.apache.org/jira/browse/HDFS-17105) |  mistakenly purge editLogs even after it is empty in NNStorageRetentionManager |  Minor | namanode | ConfX | ConfX |
| [HDFS-17198](https://issues.apache.org/jira/browse/HDFS-17198) | RBF: fix bug of getRepresentativeQuorum when records have same dateModified |  Major | rbf | Jian Zhang | Jian Zhang |
| [YARN-11573](https://issues.apache.org/jira/browse/YARN-11573) | Add config option to make container allocation prefer nodes without reserved containers |  Minor | capacity scheduler | Szilard Nemeth | Szilard Nemeth |
| [YARN-11558](https://issues.apache.org/jira/browse/YARN-11558) | Fix dependency convergence error on hbase2 profile |  Major | buid, yarn | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-18912](https://issues.apache.org/jira/browse/HADOOP-18912) | upgrade snappy-java to 1.1.10.4 due to CVE |  Major | build | PJ Fanning | PJ Fanning |
| [HDFS-17133](https://issues.apache.org/jira/browse/HDFS-17133) | TestFsDatasetImpl missing null check when cleaning up |  Critical | test | ConfX | ConfX |
| [HDFS-17209](https://issues.apache.org/jira/browse/HDFS-17209) | Correct comments to align with the code |  Trivial | datanode | Yu Wang | Yu Wang |
| [YARN-11578](https://issues.apache.org/jira/browse/YARN-11578) | Fix performance issue of permission check in verifyAndCreateRemoteLogDir |  Major | log-aggregation | Tamas Domok | Tamas Domok |
| [HADOOP-18922](https://issues.apache.org/jira/browse/HADOOP-18922) | Race condition in ZKDelegationTokenSecretManager creating znode |  Major | common | Kevin Risden | Kevin Risden |
| [HADOOP-18929](https://issues.apache.org/jira/browse/HADOOP-18929) | Build failure while trying to create apache 3.3.7 release locally. |  Critical | build | Mukund Thakur | PJ Fanning |
| [YARN-11590](https://issues.apache.org/jira/browse/YARN-11590) | RM process stuck after calling confStore.format() when ZK SSL/TLS is enabled,  as netty thread waits indefinitely |  Major | resourcemanager | Ferenc Erdelyi | Ferenc Erdelyi |
| [HDFS-17220](https://issues.apache.org/jira/browse/HDFS-17220) | fix same available space policy in AvailableSpaceVolumeChoosingPolicy |  Major | hdfs | Fei Guo | Fei Guo |
| [HADOOP-18941](https://issues.apache.org/jira/browse/HADOOP-18941) | Modify HBase version in BUILDING.txt |  Minor | common | Zepeng Zhang | Zepeng Zhang |
| [YARN-11595](https://issues.apache.org/jira/browse/YARN-11595) | Fix hadoop-yarn-client#java.lang.NoClassDefFoundError |  Major | yarn-client | Shilun Fan | Shilun Fan |
| [HDFS-17237](https://issues.apache.org/jira/browse/HDFS-17237) | Remove IPCLoggerChannel Metrics when the logger is closed |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-17231](https://issues.apache.org/jira/browse/HDFS-17231) | HA: Safemode should exit when resources are from low to available |  Major | ha | kuper | kuper |
| [HDFS-17024](https://issues.apache.org/jira/browse/HDFS-17024) | Potential data race introduced by HDFS-15865 |  Major | dfsclient | Wei-Chiu Chuang | Segawa Hiroaki |
| [YARN-11597](https://issues.apache.org/jira/browse/YARN-11597) | NPE when getting the static files in SLSWebApp |  Major | scheduler-load-simulator | Junfan Zhang | Junfan Zhang |
| [HADOOP-18905](https://issues.apache.org/jira/browse/HADOOP-18905) | Negative timeout in ZKFailovercontroller due to overflow |  Major | common | ConfX | ConfX |
| [YARN-11584](https://issues.apache.org/jira/browse/YARN-11584) | [CS] Attempting to create Leaf Queue with empty shortname should fail without crashing RM |  Major | capacity scheduler | Brian Goerlitz | Brian Goerlitz |
| [HDFS-17246](https://issues.apache.org/jira/browse/HDFS-17246) | Fix shaded client for building Hadoop on Windows |  Major | hdfs-client | Gautham Banasandra | Gautham Banasandra |
| [MAPREDUCE-7459](https://issues.apache.org/jira/browse/MAPREDUCE-7459) | Fixed TestHistoryViewerPrinter flakiness during string comparison |  Minor | test | Rajiv Ramachandran | Rajiv Ramachandran |
| [YARN-11599](https://issues.apache.org/jira/browse/YARN-11599) | Incorrect log4j properties file in SLS sample conf |  Major | scheduler-load-simulator | Junfan Zhang | Junfan Zhang |
| [YARN-11608](https://issues.apache.org/jira/browse/YARN-11608) | QueueCapacityVectorInfo NPE when accesible labels config is used |  Major | capacity scheduler | Benjamin Teke | Benjamin Teke |
| [HDFS-17249](https://issues.apache.org/jira/browse/HDFS-17249) | Fix TestDFSUtil.testIsValidName() unit test failure |  Minor | test | liuguanghua | liuguanghua |
| [HADOOP-18969](https://issues.apache.org/jira/browse/HADOOP-18969) | S3A:  AbstractS3ACostTest to clear bucket fs.s3a.create.performance flag |  Minor | fs/s3, test | Steve Loughran | Steve Loughran |
| [YARN-11616](https://issues.apache.org/jira/browse/YARN-11616) | Fast fail when multiple attribute kvs are specified |  Major | nodeattibute | Junfan Zhang | Junfan Zhang |
| [HDFS-17261](https://issues.apache.org/jira/browse/HDFS-17261) | RBF: Fix getFileInfo return wrong path when get mountTable path which multi-level |  Minor | rbf | liuguanghua | liuguanghua |
| [HDFS-17271](https://issues.apache.org/jira/browse/HDFS-17271) | Web UI DN report shows random order when sorting with dead DNs |  Minor | namenode, rbf, ui | Felix N | Felix N |
| [HDFS-17233](https://issues.apache.org/jira/browse/HDFS-17233) | The conf dfs.datanode.lifeline.interval.seconds is not considering time unit seconds |  Major | datanode | Hemanth Boyina | Palakur Eshwitha Sai |
| [HDFS-17260](https://issues.apache.org/jira/browse/HDFS-17260) | Fix the logic for reconfigure slow peer enable for Namenode. |  Major | namanode | Zhaobo Huang | Zhaobo Huang |
| [HDFS-17232](https://issues.apache.org/jira/browse/HDFS-17232) | RBF: Fix NoNamenodesAvailableException for a long time, when use observer |  Major | rbf | Jian Zhang | Jian Zhang |
| [HDFS-17270](https://issues.apache.org/jira/browse/HDFS-17270) | RBF: Fix ZKDelegationTokenSecretManagerImpl use closed zookeeper client  to get token in some case |  Major | rbf | lei w | lei w |
| [HDFS-17262](https://issues.apache.org/jira/browse/HDFS-17262) | Fixed the verbose log.warn in DFSUtil.addTransferRateMetric() |  Major | logging | Bryan Beaudreault | Ravindra Dingankar |
| [HDFS-17265](https://issues.apache.org/jira/browse/HDFS-17265) | RBF: Throwing an exception prevents the permit from being released when using FairnessPolicyController |  Major | rbf | Jian Zhang | Jian Zhang |
| [HDFS-17278](https://issues.apache.org/jira/browse/HDFS-17278) | Detect order dependent flakiness in TestViewfsWithNfs3.java under hadoop-hdfs-nfs module |  Minor | nfs, test | Ruby | Ruby |
| [HADOOP-19011](https://issues.apache.org/jira/browse/HADOOP-19011) | Possible ConcurrentModificationException if Exec command fails |  Major | common | Attila Doroszlai | Attila Doroszlai |
| [MAPREDUCE-7463](https://issues.apache.org/jira/browse/MAPREDUCE-7463) | Fix missing comma in  HistoryServerRest.html response body |  Minor | documentation | wangzhongwei | wangzhongwei |
| [HDFS-17240](https://issues.apache.org/jira/browse/HDFS-17240) | Fix a typo in DataStorage.java |  Trivial | datanode | Yu Wang | Yu Wang |
| [HDFS-17056](https://issues.apache.org/jira/browse/HDFS-17056) | EC: Fix verifyClusterSetup output in case of an invalid param. |  Major | erasure-coding | Ayush Saxena | Zhaobo Huang |
| [HDFS-17298](https://issues.apache.org/jira/browse/HDFS-17298) | Fix NPE in DataNode.handleBadBlock and BlockSender |  Major | datanode | Haiyang Hu | Haiyang Hu |
| [HDFS-17284](https://issues.apache.org/jira/browse/HDFS-17284) | EC: Fix int overflow in calculating numEcReplicatedTasks and numReplicationTasks during block recovery |  Major | ec, namenode | Hualong Zhang | Hualong Zhang |
| [HADOOP-19010](https://issues.apache.org/jira/browse/HADOOP-19010) | NullPointerException in Hadoop Credential Check CLI Command |  Major | common | Anika Kelhanka | Anika Kelhanka |
| [HDFS-17182](https://issues.apache.org/jira/browse/HDFS-17182) | DataSetLockManager.lockLeakCheck() is not thread-safe. |  Minor | datanode | liuguanghua | liuguanghua |
| [HDFS-17309](https://issues.apache.org/jira/browse/HDFS-17309) | RBF: Fix Router Safemode check contidition error |  Major | rbf | liuguanghua | liuguanghua |
| [YARN-11646](https://issues.apache.org/jira/browse/YARN-11646) | QueueCapacityConfigParser shouldn't ignore capacity config with 0 memory |  Major | capacityscheduler | Tamas Domok | Tamas Domok |
| [HDFS-17290](https://issues.apache.org/jira/browse/HDFS-17290) | HDFS: add client rpc backoff metrics due to disconnection from lowest priority queue |  Major | metrics | Lei Yang | Lei Yang |
| [HADOOP-18894](https://issues.apache.org/jira/browse/HADOOP-18894) | upgrade sshd-core due to CVEs |  Major | build, common | PJ Fanning | PJ Fanning |
| [YARN-11639](https://issues.apache.org/jira/browse/YARN-11639) | ConcurrentModificationException and NPE in PriorityUtilizationQueueOrderingPolicy |  Major | capacity scheduler | Ferenc Erdelyi | Ferenc Erdelyi |
| [HADOOP-19049](https://issues.apache.org/jira/browse/HADOOP-19049) | Class loader leak caused by StatisticsDataReferenceCleaner thread |  Major | common | Jia Fan | Jia Fan |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-10327](https://issues.apache.org/jira/browse/YARN-10327) | Remove duplication of checking for invalid application ID in TestLogsCLI |  Trivial | test | Hudáky Márton Gyula | Hudáky Márton Gyula |
| [MAPREDUCE-7280](https://issues.apache.org/jira/browse/MAPREDUCE-7280) | MiniMRYarnCluster has hard-coded timeout waiting to start history server, with no way to disable |  Major | test | Nick Dimiduk | Masatake Iwasaki |
| [MAPREDUCE-7288](https://issues.apache.org/jira/browse/MAPREDUCE-7288) | Fix TestLongLong#testRightShift |  Minor | test | Wanqiang Ji | Wanqiang Ji |
| [HDFS-15514](https://issues.apache.org/jira/browse/HDFS-15514) | Remove useless dfs.webhdfs.enabled |  Minor | test | Hui Fei | Hui Fei |
| [HADOOP-17205](https://issues.apache.org/jira/browse/HADOOP-17205) | Move personality file from Yetus to Hadoop repository |  Major | test, yetus | Chao Sun | Chao Sun |
| [HDFS-15564](https://issues.apache.org/jira/browse/HDFS-15564) | Add Test annotation for TestPersistBlocks#testRestartDfsWithSync |  Minor | hdfs | Hui Fei | Hui Fei |
| [HDFS-15559](https://issues.apache.org/jira/browse/HDFS-15559) | Complement initialize member variables in TestHdfsConfigFields#initializeMemberVariables |  Minor | test | Lisheng Sun | Lisheng Sun |
| [HDFS-15576](https://issues.apache.org/jira/browse/HDFS-15576) | Erasure Coding: Add rs and rs-legacy codec test for addPolicies |  Minor | erasure-coding, test | Hui Fei | Hui Fei |
| [YARN-9333](https://issues.apache.org/jira/browse/YARN-9333) | TestFairSchedulerPreemption.testRelaxLocalityPreemptionWithNoLessAMInRemainingNodes fails intermittently |  Major | yarn | Prabhu Joseph | Peter Bacsko |
| [HDFS-15690](https://issues.apache.org/jira/browse/HDFS-15690) | Add lz4-java as hadoop-hdfs test dependency |  Major | test | L. C. Hsieh | L. C. Hsieh |
| [YARN-10520](https://issues.apache.org/jira/browse/YARN-10520) | Deprecated the residual nested class for the LCEResourceHandler |  Major | nodemanager | Wanqiang Ji | Wanqiang Ji |
| [HDFS-15898](https://issues.apache.org/jira/browse/HDFS-15898) | Test case TestOfflineImageViewer fails |  Minor | test | Hui Fei | Hui Fei |
| [HDFS-15904](https://issues.apache.org/jira/browse/HDFS-15904) | Flaky test TestBalancer#testBalancerWithSortTopNodes() |  Major | balancer & mover, test | Viraj Jasani | Viraj Jasani |
| [HDFS-16041](https://issues.apache.org/jira/browse/HDFS-16041) | TestErasureCodingCLI fails |  Minor | test | Hui Fei | Hui Fei |
| [MAPREDUCE-7342](https://issues.apache.org/jira/browse/MAPREDUCE-7342) | Stop RMService in TestClientRedirect.testRedirect() |  Minor | test | Zhengxi Li | Zhengxi Li |
| [MAPREDUCE-7311](https://issues.apache.org/jira/browse/MAPREDUCE-7311) | Fix non-idempotent test in TestTaskProgressReporter |  Minor | test | Zhengxi Li | Zhengxi Li |
| [HDFS-16224](https://issues.apache.org/jira/browse/HDFS-16224) | testBalancerWithObserverWithFailedNode times out |  Trivial | test | Leon Gao | Leon Gao |
| [HADOOP-17868](https://issues.apache.org/jira/browse/HADOOP-17868) | Add more test for the BuiltInGzipCompressor |  Major | test | L. C. Hsieh | L. C. Hsieh |
| [HADOOP-17936](https://issues.apache.org/jira/browse/HADOOP-17936) | TestLocalFSCopyFromLocal.testDestinationFileIsToParentDirectory failure after reverting HADOOP-16878 |  Major | test | Chao Sun | Chao Sun |
| [HDFS-15862](https://issues.apache.org/jira/browse/HDFS-15862) | Make TestViewfsWithNfs3.testNfsRenameSingleNN() idempotent |  Minor | nfs | Zhengxi Li | Zhengxi Li |
| [YARN-6272](https://issues.apache.org/jira/browse/YARN-6272) | TestAMRMClient#testAMRMClientWithContainerResourceChange fails intermittently |  Major | yarn | Ray Chiang | Andras Gyori |
| [HADOOP-18089](https://issues.apache.org/jira/browse/HADOOP-18089) | Test coverage for Async profiler servlets |  Minor | common | Viraj Jasani | Viraj Jasani |
| [YARN-11081](https://issues.apache.org/jira/browse/YARN-11081) | TestYarnConfigurationFields consistently keeps failing |  Minor | test | Viraj Jasani | Viraj Jasani |
| [HDFS-16573](https://issues.apache.org/jira/browse/HDFS-16573) | Fix test TestDFSStripedInputStreamWithRandomECPolicy |  Minor | test | daimin | daimin |
| [HDFS-16637](https://issues.apache.org/jira/browse/HDFS-16637) | TestHDFSCLI#testAll consistently failing |  Major | test | Viraj Jasani | Viraj Jasani |
| [YARN-11248](https://issues.apache.org/jira/browse/YARN-11248) | Add unit test for FINISHED\_CONTAINERS\_PULLED\_BY\_AM event on DECOMMISSIONING |  Major | test | Ashutosh Gupta | Ashutosh Gupta |
| [HDFS-16625](https://issues.apache.org/jira/browse/HDFS-16625) | Unit tests aren't checking for PMDK availability |  Major | test | Steve Vaughan | Steve Vaughan |
| [YARN-11388](https://issues.apache.org/jira/browse/YARN-11388) | Prevent resource leaks in TestClientRMService. |  Minor | test | Chris Nauroth | Chris Nauroth |
| [YARN-5607](https://issues.apache.org/jira/browse/YARN-5607) | Document TestContainerResourceUsage#waitForContainerCompletion |  Major | resourcemanager, test | Karthik Kambatla | Susheel Gupta |
| [HDFS-17010](https://issues.apache.org/jira/browse/HDFS-17010) | Add a subtree test to TestSnapshotDiffReport |  Minor | test | Tsz-wo Sze | Tsz-wo Sze |
| [YARN-11526](https://issues.apache.org/jira/browse/YARN-11526) | Add a unit test |  Minor | client | Lu Yuan | Lu Yuan |
| [YARN-11621](https://issues.apache.org/jira/browse/YARN-11621) | Fix intermittently failing unit test: TestAMRMProxy.testAMRMProxyTokenRenewal |  Major | yarn | Susheel Gupta | Susheel Gupta |
| [HDFS-16904](https://issues.apache.org/jira/browse/HDFS-16904) | Close webhdfs during the teardown |  Major | hdfs | Steve Vaughan | Steve Vaughan |
| [HDFS-17370](https://issues.apache.org/jira/browse/HDFS-17370) | Fix junit dependency for running parameterized tests in hadoop-hdfs-rbf |  Major | . | Takanobu Asanuma | Takanobu Asanuma |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-16169](https://issues.apache.org/jira/browse/HADOOP-16169) | ABFS: Bug fix for getPathProperties |  Major | fs/azure | Da Zhou | Da Zhou |
| [HDFS-15146](https://issues.apache.org/jira/browse/HDFS-15146) | TestBalancerRPCDelay. testBalancerRPCDelayQpsDefault fails intermittently |  Minor | balancer, test | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15051](https://issues.apache.org/jira/browse/HDFS-15051) | RBF: Impose directory level permissions for Mount entries |  Major | rbf | Xiaoqiao He | Xiaoqiao He |
| [YARN-10234](https://issues.apache.org/jira/browse/YARN-10234) | FS-CS converter: don't enable auto-create queue property for root |  Critical | fairscheduler | Peter Bacsko | Peter Bacsko |
| [YARN-10240](https://issues.apache.org/jira/browse/YARN-10240) | Prevent Fatal CancelledException in TimelineV2Client when stopping |  Major | ATSv2 | Tarun Parimi | Tarun Parimi |
| [HADOOP-17002](https://issues.apache.org/jira/browse/HADOOP-17002) | ABFS: Avoid storage calls to check if the account is HNS enabled or not |  Minor | fs/azure | Bilahari T H | Bilahari T H |
| [YARN-10159](https://issues.apache.org/jira/browse/YARN-10159) | TimelineConnector does not destroy the jersey client |  Major | ATSv2 | Prabhu Joseph | Tanu Ajmera |
| [YARN-10194](https://issues.apache.org/jira/browse/YARN-10194) | YARN RMWebServices /scheduler-conf/validate leaks ZK Connections |  Blocker | capacityscheduler | Akhil PB | Prabhu Joseph |
| [YARN-10215](https://issues.apache.org/jira/browse/YARN-10215) | Endpoint for obtaining direct URL for the logs |  Major | yarn | Adam Antal | Andras Gyori |
| [YARN-6973](https://issues.apache.org/jira/browse/YARN-6973) | Adding RM Cluster Id in ApplicationReport |  Major | applications, federation | Giovanni Matteo Fumarola | Bilwa S T |
| [YARN-6553](https://issues.apache.org/jira/browse/YARN-6553) | Replace MockResourceManagerFacade with MockRM for AMRMProxy/Router tests |  Major | federation, router, test | Giovanni Matteo Fumarola | Bilwa S T |
| [HDFS-14353](https://issues.apache.org/jira/browse/HDFS-14353) | Erasure Coding: metrics xmitsInProgress become to negative. |  Major | datanode, erasure-coding | Baolong Mao | Baolong Mao |
| [HDFS-15305](https://issues.apache.org/jira/browse/HDFS-15305) | Extend ViewFS and provide ViewFSOverloadScheme implementation with scheme configurable. |  Major | fs, hadoop-client, hdfs-client, viewfs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [YARN-10257](https://issues.apache.org/jira/browse/YARN-10257) | FS-CS converter: skip increment properties for mem/vcores and fix DRF check |  Major | fairscheduler, fs-cs | Peter Bacsko | Peter Bacsko |
| [HADOOP-17027](https://issues.apache.org/jira/browse/HADOOP-17027) | Add tests for reading fair call queue capacity weight configs |  Major | ipc | Fengnan Li | Fengnan Li |
| [YARN-8942](https://issues.apache.org/jira/browse/YARN-8942) | PriorityBasedRouterPolicy throws exception if all sub-cluster weights have negative value |  Minor | federation | Akshay Agarwal | Bilwa S T |
| [YARN-10259](https://issues.apache.org/jira/browse/YARN-10259) | Reserved Containers not allocated from available space of other nodes in CandidateNodeSet in MultiNodePlacement |  Major | capacityscheduler | Prabhu Joseph | Prabhu Joseph |
| [HDFS-15306](https://issues.apache.org/jira/browse/HDFS-15306) | Make mount-table to read from central place ( Let's say from HDFS) |  Major | configuration, hadoop-client | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-15082](https://issues.apache.org/jira/browse/HDFS-15082) | RBF: Check each component length of destination path when add/update mount entry |  Major | rbf | Xiaoqiao He | Xiaoqiao He |
| [HDFS-15340](https://issues.apache.org/jira/browse/HDFS-15340) | RBF: Implement BalanceProcedureScheduler basic framework |  Major | rbf | Jinglun | Jinglun |
| [HDFS-15322](https://issues.apache.org/jira/browse/HDFS-15322) | Make NflyFS to work when ViewFsOverloadScheme's scheme and target uris schemes are same. |  Major | fs, nflyFs, viewfs, viewfsOverloadScheme | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [YARN-10108](https://issues.apache.org/jira/browse/YARN-10108) | FS-CS converter: nestedUserQueue with default rule results in invalid queue mapping |  Major | capacity scheduler, fs-cs | Prabhu Joseph | Gergely Pollák |
| [HADOOP-17053](https://issues.apache.org/jira/browse/HADOOP-17053) | ABFS: FS initialize fails for incompatible account-agnostic Token Provider setting |  Major | fs/azure | Sneha Vijayarajan | Sneha Vijayarajan |
| [HDFS-15321](https://issues.apache.org/jira/browse/HDFS-15321) | Make DFSAdmin tool to work with ViewFSOverloadScheme |  Major | dfsadmin, fs, viewfs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [YARN-10284](https://issues.apache.org/jira/browse/YARN-10284) | Add lazy initialization of LogAggregationFileControllerFactory in LogServlet |  Major | log-aggregation, yarn | Adam Antal | Adam Antal |
| [HDFS-15330](https://issues.apache.org/jira/browse/HDFS-15330) | Document the ViewFSOverloadScheme details in ViewFS guide |  Major | viewfs, viewfsOverloadScheme | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-15389](https://issues.apache.org/jira/browse/HDFS-15389) | DFSAdmin should close filesystem and dfsadmin -setBalancerBandwidth should work with ViewFSOverloadScheme |  Major | dfsadmin, viewfsOverloadScheme | Ayush Saxena | Ayush Saxena |
| [HDFS-15394](https://issues.apache.org/jira/browse/HDFS-15394) | Add all available fs.viewfs.overload.scheme.target.\<scheme\>.impl classes in core-default.xml bydefault. |  Major | configuration, viewfs, viewfsOverloadScheme | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [YARN-10293](https://issues.apache.org/jira/browse/YARN-10293) | Reserved Containers not allocated from available space of other nodes in CandidateNodeSet in MultiNodePlacement (YARN-10259) |  Major | capacity scheduler | Prabhu Joseph | Prabhu Joseph |
| [HDFS-15387](https://issues.apache.org/jira/browse/HDFS-15387) | FSUsage$DF should consider ViewFSOverloadScheme in processPath |  Minor | viewfs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [YARN-10292](https://issues.apache.org/jira/browse/YARN-10292) | FS-CS converter: add an option to enable asynchronous scheduling in CapacityScheduler |  Major | fairscheduler | Benjamin Teke | Benjamin Teke |
| [HDFS-15346](https://issues.apache.org/jira/browse/HDFS-15346) | FedBalance tool implementation |  Major | rbf | Jinglun | Jinglun |
| [HADOOP-16888](https://issues.apache.org/jira/browse/HADOOP-16888) | [JDK11] Support JDK11 in the precommit job |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17004](https://issues.apache.org/jira/browse/HADOOP-17004) | ABFS: Improve the ABFS driver documentation |  Minor | fs/azure | Bilahari T H | Bilahari T H |
| [HDFS-15418](https://issues.apache.org/jira/browse/HDFS-15418) | ViewFileSystemOverloadScheme should represent mount links as non symlinks |  Major | hdfs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-16922](https://issues.apache.org/jira/browse/HADOOP-16922) | ABFS: Change in User-Agent header |  Minor | fs/azure | Bilahari T H | Bilahari T H |
| [YARN-9930](https://issues.apache.org/jira/browse/YARN-9930) | Support max running app logic for CapacityScheduler |  Major | capacity scheduler, capacityscheduler | zhoukang | Peter Bacsko |
| [HDFS-15428](https://issues.apache.org/jira/browse/HDFS-15428) | Javadocs fails for hadoop-federation-balance |  Minor | documentation | Xieming Li | Xieming Li |
| [HDFS-15427](https://issues.apache.org/jira/browse/HDFS-15427) | Merged ListStatus with Fallback target filesystem and InternalDirViewFS. |  Major | viewfs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [YARN-10316](https://issues.apache.org/jira/browse/YARN-10316) | FS-CS converter: convert maxAppsDefault, maxRunningApps settings |  Major | fairscheduler, fs-cs | Peter Bacsko | Peter Bacsko |
| [HADOOP-17054](https://issues.apache.org/jira/browse/HADOOP-17054) | ABFS: Fix idempotency test failures when SharedKey is set as AuthType |  Major | fs/azure | Sneha Vijayarajan | Sneha Vijayarajan |
| [HADOOP-17015](https://issues.apache.org/jira/browse/HADOOP-17015) | ABFS: Make PUT and POST operations idempotent |  Major | fs/azure | Sneha Vijayarajan | Sneha Vijayarajan |
| [HDFS-15429](https://issues.apache.org/jira/browse/HDFS-15429) | mkdirs should work when parent dir is internalDir and fallback configured. |  Major | hdfs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [YARN-6526](https://issues.apache.org/jira/browse/YARN-6526) | Refactoring SQLFederationStateStore by avoiding to recreate a connection at every call |  Major | federation | Giovanni Matteo Fumarola | Bilwa S T |
| [HDFS-15436](https://issues.apache.org/jira/browse/HDFS-15436) | Default mount table name used by ViewFileSystem should be configurable |  Major | viewfs, viewfsOverloadScheme | Virajith Jalaparti | Virajith Jalaparti |
| [HDFS-15410](https://issues.apache.org/jira/browse/HDFS-15410) | Add separated config file hdfs-fedbalance-default.xml for fedbalance tool |  Major | rbf | Jinglun | Jinglun |
| [HDFS-15374](https://issues.apache.org/jira/browse/HDFS-15374) | Add documentation for fedbalance tool |  Major | documentation, rbf | Jinglun | Jinglun |
| [YARN-10325](https://issues.apache.org/jira/browse/YARN-10325) | Document max-parallel-apps for Capacity Scheduler |  Major | capacity scheduler, capacityscheduler | Peter Bacsko | Peter Bacsko |
| [HADOOP-16961](https://issues.apache.org/jira/browse/HADOOP-16961) | ABFS: Adding metrics to AbfsInputStream (AbfsInputStreamStatistics) |  Major | fs/azure | Gabor Bota | Mehakmeet Singh |
| [HDFS-15430](https://issues.apache.org/jira/browse/HDFS-15430) | create should work when parent dir is internalDir and fallback configured. |  Major | hdfs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-15450](https://issues.apache.org/jira/browse/HDFS-15450) | Fix NN trash emptier to work if ViewFSOveroadScheme enabled |  Major | namenode, viewfsOverloadScheme | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-17111](https://issues.apache.org/jira/browse/HADOOP-17111) | Replace Guava Optional with Java8+ Optional |  Major | build, common | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15417](https://issues.apache.org/jira/browse/HDFS-15417) | RBF: Get the datanode report from cache for federation WebHDFS operations |  Major | federation, rbf, webhdfs | Ye Ni | Ye Ni |
| [HDFS-15449](https://issues.apache.org/jira/browse/HDFS-15449) | Optionally ignore port number in mount-table name when picking from initialized uri |  Major | hdfs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [YARN-10337](https://issues.apache.org/jira/browse/YARN-10337) | TestRMHATimelineCollectors fails on hadoop trunk |  Major | test, yarn | Ahmed Hussein | Bilwa S T |
| [HDFS-15462](https://issues.apache.org/jira/browse/HDFS-15462) | Add fs.viewfs.overload.scheme.target.ofs.impl to core-default.xml |  Major | configuration, viewfs, viewfsOverloadScheme | Siyao Meng | Siyao Meng |
| [HDFS-15464](https://issues.apache.org/jira/browse/HDFS-15464) | ViewFsOverloadScheme should work when -fs option pointing to remote cluster without mount links |  Major | viewfsOverloadScheme | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-17101](https://issues.apache.org/jira/browse/HADOOP-17101) | Replace Guava Function with Java8+ Function |  Major | build | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17099](https://issues.apache.org/jira/browse/HADOOP-17099) | Replace Guava Predicate with Java8+ Predicate |  Minor | build | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15479](https://issues.apache.org/jira/browse/HDFS-15479) | Ordered snapshot deletion: make it a configurable feature |  Major | snapshots | Tsz-wo Sze | Tsz-wo Sze |
| [HDFS-15478](https://issues.apache.org/jira/browse/HDFS-15478) | When Empty mount points, we are assigning fallback link to self. But it should not use full URI for target fs. |  Major | hdfs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-17100](https://issues.apache.org/jira/browse/HADOOP-17100) | Replace Guava Supplier with Java8+ Supplier in Hadoop |  Major | build | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15480](https://issues.apache.org/jira/browse/HDFS-15480) | Ordered snapshot deletion: record snapshot deletion in XAttr |  Major | snapshots | Tsz-wo Sze | Shashikant Banerjee |
| [HADOOP-17132](https://issues.apache.org/jira/browse/HADOOP-17132) | ABFS: Fix For Idempotency code |  Major | fs/azure | Sneha Vijayarajan | Sneha Vijayarajan |
| [YARN-10315](https://issues.apache.org/jira/browse/YARN-10315) | Avoid sending RMNodeResourceupdate event if resource is same |  Major | graceful | Bibin Chundatt | Sushil Ks |
| [HADOOP-13221](https://issues.apache.org/jira/browse/HADOOP-13221) | s3a create() doesn't check for an ancestor path being a file |  Major | fs/s3 | Steve Loughran | Sean Mackrory |
| [HDFS-15488](https://issues.apache.org/jira/browse/HDFS-15488) | Add a command to list all snapshots for a snaphottable root with snapshot Ids |  Major | snapshots | Shashikant Banerjee | Shashikant Banerjee |
| [HDFS-15481](https://issues.apache.org/jira/browse/HDFS-15481) | Ordered snapshot deletion: garbage collect deleted snapshots |  Major | snapshots | Tsz-wo Sze | Tsz-wo Sze |
| [HDFS-15498](https://issues.apache.org/jira/browse/HDFS-15498) | Show snapshots deletion status in snapList cmd |  Major | snapshots | Shashikant Banerjee | Shashikant Banerjee |
| [HADOOP-17091](https://issues.apache.org/jira/browse/HADOOP-17091) | [JDK11] Fix Javadoc errors |  Major | build | Uma Maheswara Rao G | Akira Ajisaka |
| [YARN-10229](https://issues.apache.org/jira/browse/YARN-10229) | [Federation] Client should be able to submit application to RM directly using normal client conf |  Major | amrmproxy, federation | JohnsonGuo | Bilwa S T |
| [HDFS-15497](https://issues.apache.org/jira/browse/HDFS-15497) | Make snapshot limit on global as well per snapshot root directory configurable |  Major | snapshots | Shashikant Banerjee | Shashikant Banerjee |
| [HADOOP-17131](https://issues.apache.org/jira/browse/HADOOP-17131) | Refactor S3A Listing code for better isolation |  Major | fs/s3 | Mukund Thakur | Mukund Thakur |
| [HADOOP-17179](https://issues.apache.org/jira/browse/HADOOP-17179) | [JDK 11] Fix javadoc error  in Java API link detection |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17137](https://issues.apache.org/jira/browse/HADOOP-17137) | ABFS: Tests ITestAbfsNetworkStatistics need to be config setting agnostic |  Minor | fs/azure, test | Sneha Vijayarajan | Bilahari T H |
| [HADOOP-17149](https://issues.apache.org/jira/browse/HADOOP-17149) | ABFS: Test failure: testFailedRequestWhenCredentialsNotCorrect fails when run with SharedKey |  Minor | fs/azure | Sneha Vijayarajan | Bilahari T H |
| [HADOOP-17163](https://issues.apache.org/jira/browse/HADOOP-17163) | ABFS: Add debug log for rename failures |  Major | fs/azure | Bilahari T H | Bilahari T H |
| [HDFS-15492](https://issues.apache.org/jira/browse/HDFS-15492) | Make trash root inside each snapshottable directory |  Major | hdfs, hdfs-client | Siyao Meng | Siyao Meng |
| [HDFS-15518](https://issues.apache.org/jira/browse/HDFS-15518) | Wrong operation name in FsNamesystem for listSnapshots |  Major | snapshots | Mukul Kumar Singh | Aryan Gupta |
| [HDFS-15496](https://issues.apache.org/jira/browse/HDFS-15496) | Add UI for deleted snapshots |  Major | snapshots | Mukul Kumar Singh | Vivek Ratnavel Subramanian |
| [HDFS-15524](https://issues.apache.org/jira/browse/HDFS-15524) | Add edit log entry for Snapshot deletion GC thread snapshot deletion |  Major | snapshots | Shashikant Banerjee | Shashikant Banerjee |
| [HDFS-15483](https://issues.apache.org/jira/browse/HDFS-15483) | Ordered snapshot deletion: Disallow rename between two snapshottable directories |  Major | snapshots | Tsz-wo Sze | Shashikant Banerjee |
| [HDFS-15525](https://issues.apache.org/jira/browse/HDFS-15525) | Make trash root inside each snapshottable directory for WebHDFS |  Major | webhdfs | Siyao Meng | Siyao Meng |
| [HDFS-15533](https://issues.apache.org/jira/browse/HDFS-15533) | Provide DFS API compatible class(ViewDistributedFileSystem), but use ViewFileSystemOverloadScheme inside |  Major | dfs, viewfs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [YARN-10360](https://issues.apache.org/jira/browse/YARN-10360) | Support Multi Node Placement in SingleConstraintAppPlacementAllocator |  Major | capacityscheduler, multi-node-placement | Prabhu Joseph | Prabhu Joseph |
| [YARN-10106](https://issues.apache.org/jira/browse/YARN-10106) | Yarn logs CLI filtering by application attempt |  Trivial | yarn | Adam Antal | Hudáky Márton Gyula |
| [YARN-10304](https://issues.apache.org/jira/browse/YARN-10304) | Create an endpoint for remote application log directory path query |  Minor | yarn | Andras Gyori | Andras Gyori |
| [YARN-1806](https://issues.apache.org/jira/browse/YARN-1806) | webUI update to allow end users to request thread dump |  Major | nodemanager | Ming Ma | Siddharth Ahuja |
| [HDFS-15500](https://issues.apache.org/jira/browse/HDFS-15500) | In-order deletion of snapshots: Diff lists must be update only in the last snapshot |  Major | snapshots | Mukul Kumar Singh | Tsz-wo Sze |
| [HDFS-15531](https://issues.apache.org/jira/browse/HDFS-15531) | Namenode UI: List snapshots in separate table for each snapshottable directory |  Major | ui | Vivek Ratnavel Subramanian | Vivek Ratnavel Subramanian |
| [YARN-10408](https://issues.apache.org/jira/browse/YARN-10408) | Extract MockQueueHierarchyBuilder to a separate class |  Major | resourcemanager, test | Gergely Pollák | Gergely Pollák |
| [YARN-10409](https://issues.apache.org/jira/browse/YARN-10409) | Improve MockQueueHierarchyBuilder to detect queue ambiguity |  Major | resourcemanager, test | Gergely Pollák | Gergely Pollák |
| [YARN-10371](https://issues.apache.org/jira/browse/YARN-10371) | Create variable context class for CS queue mapping rules |  Major | yarn | Gergely Pollák | Gergely Pollák |
| [YARN-10373](https://issues.apache.org/jira/browse/YARN-10373) | Create Matchers for CS mapping rules |  Major | yarn | Gergely Pollák | Gergely Pollák |
| [HDFS-15542](https://issues.apache.org/jira/browse/HDFS-15542) | Add identified snapshot corruption tests for ordered snapshot deletion |  Major | snapshots, test | Shashikant Banerjee | Shashikant Banerjee |
| [YARN-10386](https://issues.apache.org/jira/browse/YARN-10386) | Create new JSON schema for Placement Rules |  Major | capacity scheduler, capacityscheduler | Peter Bacsko | Peter Bacsko |
| [YARN-10374](https://issues.apache.org/jira/browse/YARN-10374) | Create Actions for CS mapping rules |  Major | yarn | Gergely Pollák | Gergely Pollák |
| [YARN-10372](https://issues.apache.org/jira/browse/YARN-10372) | Create MappingRule class to represent each CS mapping rule |  Major | yarn | Gergely Pollák | Gergely Pollák |
| [YARN-10375](https://issues.apache.org/jira/browse/YARN-10375) | CS Mapping rule config parser should return MappingRule objects |  Major | yarn | Gergely Pollák | Gergely Pollák |
| [HDFS-15529](https://issues.apache.org/jira/browse/HDFS-15529) | getChildFilesystems should include fallback fs as well |  Critical | viewfs, viewfsOverloadScheme | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [YARN-10376](https://issues.apache.org/jira/browse/YARN-10376) | Create a class that covers the functionality of UserGroupMappingPlacementRule and AppNameMappingPlacementRule using the new mapping rules |  Major | yarn | Gergely Pollák | Gergely Pollák |
| [YARN-10332](https://issues.apache.org/jira/browse/YARN-10332) | RESOURCE\_UPDATE event was repeatedly registered in DECOMMISSIONING state |  Minor | resourcemanager | yehuanhuan | yehuanhuan |
| [YARN-10411](https://issues.apache.org/jira/browse/YARN-10411) | Create an allowCreate flag for MappingRuleAction |  Major | resourcemanager, scheduler | Gergely Pollák | Gergely Pollák |
| [HDFS-15558](https://issues.apache.org/jira/browse/HDFS-15558) | ViewDistributedFileSystem#recoverLease should call super.recoverLease when there are no mounts configured |  Major | viewfs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [YARN-10415](https://issues.apache.org/jira/browse/YARN-10415) | Create a group matcher which checks ALL groups of the user |  Major | resourcemanager, scheduler | Gergely Pollák | Gergely Pollák |
| [HADOOP-17181](https://issues.apache.org/jira/browse/HADOOP-17181) | Handle transient stream read failures in FileSystem contract tests |  Minor | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-10387](https://issues.apache.org/jira/browse/YARN-10387) | Implement logic which returns MappingRule objects based on mapping rules |  Major | capacity scheduler, resourcemanager | Peter Bacsko | Peter Bacsko |
| [HDFS-15563](https://issues.apache.org/jira/browse/HDFS-15563) | Incorrect getTrashRoot return value when a non-snapshottable dir prefix matches the path of a snapshottable dir |  Major | snapshots | Nilotpal Nandi | Siyao Meng |
| [HDFS-15551](https://issues.apache.org/jira/browse/HDFS-15551) | Tiny Improve for DeadNode detector |  Minor | hdfs-client | dark\_num | imbajin |
| [HDFS-15555](https://issues.apache.org/jira/browse/HDFS-15555) | RBF: Refresh cacheNS when SocketException occurs |  Major | rbf | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15532](https://issues.apache.org/jira/browse/HDFS-15532) | listFiles on root/InternalDir will fail if fallback root has file |  Major | viewfs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-15539](https://issues.apache.org/jira/browse/HDFS-15539) | When disallowing snapshot on a dir, throw exception if its trash root is not empty |  Major | hdfs | Siyao Meng | Siyao Meng |
| [HDFS-15568](https://issues.apache.org/jira/browse/HDFS-15568) | namenode start failed to start when dfs.namenode.snapshot.max.limit set |  Major | snapshots | Nilotpal Nandi | Shashikant Banerjee |
| [HDFS-15578](https://issues.apache.org/jira/browse/HDFS-15578) | Fix the rename issues with fallback fs enabled |  Major | viewfs, viewfsOverloadScheme | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-15585](https://issues.apache.org/jira/browse/HDFS-15585) | ViewDFS#getDelegationToken should not throw UnsupportedOperationException. |  Major | viewfs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [YARN-10424](https://issues.apache.org/jira/browse/YARN-10424) | Adapt existing AppName and UserGroupMapping unittests to ensure backwards compatibility |  Major | resourcemanager, test | Benjamin Teke | Benjamin Teke |
| [HADOOP-17215](https://issues.apache.org/jira/browse/HADOOP-17215) | ABFS: Support for conditional overwrite |  Major | fs/azure | Sneha Vijayarajan | Sneha Vijayarajan |
| [HDFS-14811](https://issues.apache.org/jira/browse/HDFS-14811) | RBF: TestRouterRpc#testErasureCoding is flaky |  Major | rbf | Chen Zhang | Chen Zhang |
| [HADOOP-17279](https://issues.apache.org/jira/browse/HADOOP-17279) | ABFS: Test testNegativeScenariosForCreateOverwriteDisabled fails for non-HNS account |  Major | fs/azure | Sneha Vijayarajan | Sneha Vijayarajan |
| [HDFS-15590](https://issues.apache.org/jira/browse/HDFS-15590) | namenode fails to start when ordered snapshot deletion feature is disabled |  Major | snapshots | Nilotpal Nandi | Shashikant Banerjee |
| [HDFS-15596](https://issues.apache.org/jira/browse/HDFS-15596) | ViewHDFS#create(f, permission, cflags, bufferSize, replication, blockSize, progress, checksumOpt) should not be restricted to DFS only. |  Major | hdfs-client | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-15598](https://issues.apache.org/jira/browse/HDFS-15598) | ViewHDFS#canonicalizeUri should not be restricted to DFS only API. |  Major | viewfs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [YARN-10413](https://issues.apache.org/jira/browse/YARN-10413) | Change fs2cs to generate mapping rules in the new format |  Major | fs-cs, scheduler | Peter Bacsko | Peter Bacsko |
| [HDFS-15607](https://issues.apache.org/jira/browse/HDFS-15607) | Create trash dir when allowing snapshottable dir |  Major | hdfs | Siyao Meng | Siyao Meng |
| [HDFS-15613](https://issues.apache.org/jira/browse/HDFS-15613) | RBF: Router FSCK fails after HDFS-14442 |  Major | rbf | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15611](https://issues.apache.org/jira/browse/HDFS-15611) | Add list Snapshot command in WebHDFS |  Major | snapshots | Shashikant Banerjee | Shashikant Banerjee |
| [HADOOP-17281](https://issues.apache.org/jira/browse/HADOOP-17281) | Implement FileSystem.listStatusIterator() in S3AFileSystem |  Major | fs/s3 | Mukund Thakur | Mukund Thakur |
| [HDFS-13293](https://issues.apache.org/jira/browse/HDFS-13293) | RBF: The RouterRPCServer should transfer client IP via CallerContext to NamenodeRpcServer |  Major | rbf | Baolong Mao | Hui Fei |
| [HDFS-15625](https://issues.apache.org/jira/browse/HDFS-15625) | Namenode trashEmptier should not init ViewFs on startup |  Major | namenode, viewfs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [YARN-10454](https://issues.apache.org/jira/browse/YARN-10454) | Add applicationName policy |  Major | capacity scheduler, resourcemanager | Peter Bacsko | Peter Bacsko |
| [HDFS-15620](https://issues.apache.org/jira/browse/HDFS-15620) | RBF: Fix test failures after HADOOP-17281 |  Major | rbf, test | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15614](https://issues.apache.org/jira/browse/HDFS-15614) | Initialize snapshot trash root during NameNode startup if enabled |  Major | namanode, snapshots | Siyao Meng | Siyao Meng |
| [HADOOP-16915](https://issues.apache.org/jira/browse/HADOOP-16915) | ABFS: Test failure ITestAzureBlobFileSystemRandomRead.testRandomReadPerformance |  Major | fs/azure, test | Bilahari T H | Bilahari T H |
| [HADOOP-17301](https://issues.apache.org/jira/browse/HADOOP-17301) | ABFS: read-ahead error reporting breaks buffer management |  Critical | fs/azure | Sneha Vijayarajan | Sneha Vijayarajan |
| [HADOOP-17288](https://issues.apache.org/jira/browse/HADOOP-17288) | Use shaded guava from thirdparty |  Major | common, hadoop-thirdparty | Ayush Saxena | Ayush Saxena |
| [HADOOP-17175](https://issues.apache.org/jira/browse/HADOOP-17175) | [JDK11] Fix javadoc errors in hadoop-common module |  Major | documentation | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17319](https://issues.apache.org/jira/browse/HADOOP-17319) | Update the checkstyle config to ban some guava functions |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15630](https://issues.apache.org/jira/browse/HDFS-15630) | RBF: Fix wrong client IP info in CallerContext when requests mount points with multi-destinations. |  Major | rbf | Chengwei Wang | Chengwei Wang |
| [HDFS-15459](https://issues.apache.org/jira/browse/HDFS-15459) | TestBlockTokenWithDFSStriped fails intermittently |  Major | hdfs | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15640](https://issues.apache.org/jira/browse/HDFS-15640) | Add diff threshold to FedBalance |  Major | rbf | Jinglun | Jinglun |
| [HDFS-15461](https://issues.apache.org/jira/browse/HDFS-15461) | TestDFSClientRetries#testGetFileChecksum fails intermittently |  Major | dfsclient, test | Ahmed Hussein | Ahmed Hussein |
| [HDFS-9776](https://issues.apache.org/jira/browse/HDFS-9776) | TestHAAppend#testMultipleAppendsDuringCatchupTailing is flaky |  Major | test | Vinayakumar B | Ahmed Hussein |
| [HDFS-15457](https://issues.apache.org/jira/browse/HDFS-15457) | TestFsDatasetImpl fails intermittently |  Major | hdfs | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15460](https://issues.apache.org/jira/browse/HDFS-15460) | TestFileCreation#testServerDefaultsWithMinimalCaching fails intermittently |  Major | hdfs, test | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15657](https://issues.apache.org/jira/browse/HDFS-15657) | RBF: TestRouter#testNamenodeHeartBeatEnableDefault fails by BindException |  Major | rbf, test | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15654](https://issues.apache.org/jira/browse/HDFS-15654) | TestBPOfferService#testMissBlocksWhenReregister fails intermittently |  Major | datanode | Ahmed Hussein | Ahmed Hussein |
| [YARN-10420](https://issues.apache.org/jira/browse/YARN-10420) | Update CS MappingRule documentation with the new format and features |  Major | capacity scheduler, documentation | Gergely Pollák | Peter Bacsko |
| [HDFS-15643](https://issues.apache.org/jira/browse/HDFS-15643) | EC: Fix checksum computation in case of native encoders |  Blocker | erasure-coding | Ahmed Hussein | Ayush Saxena |
| [HDFS-15548](https://issues.apache.org/jira/browse/HDFS-15548) | Allow configuring DISK/ARCHIVE storage types on same device mount |  Major | datanode | Leon Gao | Leon Gao |
| [HADOOP-17344](https://issues.apache.org/jira/browse/HADOOP-17344) | Harmonize guava version and shade guava in yarn-csi |  Major | common | Wei-Chiu Chuang | Akira Ajisaka |
| [YARN-10425](https://issues.apache.org/jira/browse/YARN-10425) | Replace the legacy placement engine in CS with the new one |  Major | capacity scheduler, resourcemanager | Gergely Pollák | Gergely Pollák |
| [HDFS-15674](https://issues.apache.org/jira/browse/HDFS-15674) | TestBPOfferService#testMissBlocksWhenReregister fails on trunk |  Major | datanode, test | Ahmed Hussein | Masatake Iwasaki |
| [YARN-10486](https://issues.apache.org/jira/browse/YARN-10486) | FS-CS converter: handle case when weight=0 and allow more lenient capacity checks in Capacity Scheduler |  Major | yarn | Peter Bacsko | Peter Bacsko |
| [YARN-10457](https://issues.apache.org/jira/browse/YARN-10457) | Add a configuration switch to change between legacy and JSON placement rule format |  Major | capacity scheduler | Gergely Pollák | Gergely Pollák |
| [HDFS-15635](https://issues.apache.org/jira/browse/HDFS-15635) | ViewFileSystemOverloadScheme support specifying mount table loader imp through conf |  Major | viewfsOverloadScheme | Junfan Zhang | Junfan Zhang |
| [HADOOP-17394](https://issues.apache.org/jira/browse/HADOOP-17394) | [JDK 11] mvn package -Pdocs fails |  Major | build, documentation | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15677](https://issues.apache.org/jira/browse/HDFS-15677) | TestRouterRpcMultiDestination#testGetCachedDatanodeReport fails on trunk |  Major | rbf, test | Ahmed Hussein | Masatake Iwasaki |
| [HDFS-15689](https://issues.apache.org/jira/browse/HDFS-15689) | allow/disallowSnapshot on EZ roots shouldn't fail due to trash provisioning/emptiness check |  Major | hdfs | Siyao Meng | Siyao Meng |
| [HDFS-15716](https://issues.apache.org/jira/browse/HDFS-15716) | TestUpgradeDomainBlockPlacementPolicy flaky |  Major | namenode, test | Ahmed Hussein | Ahmed Hussein |
| [YARN-10380](https://issues.apache.org/jira/browse/YARN-10380) | Import logic of multi-node allocation in CapacityScheduler |  Critical | capacity scheduler | Wangda Tan | Qi Zhu |
| [YARN-10031](https://issues.apache.org/jira/browse/YARN-10031) | Create a general purpose log request with additional query parameters |  Major | yarn | Adam Antal | Andras Gyori |
| [YARN-10526](https://issues.apache.org/jira/browse/YARN-10526) | RMAppManager CS Placement ignores parent path |  Major | capacity scheduler | Gergely Pollák | Gergely Pollák |
| [YARN-10463](https://issues.apache.org/jira/browse/YARN-10463) | For Federation, we should support getApplicationAttemptReport. |  Major | federation, router | Qi Zhu | Qi Zhu |
| [HDFS-15308](https://issues.apache.org/jira/browse/HDFS-15308) | TestReconstructStripedFile#testNNSendsErasureCodingTasks fails intermittently |  Major | erasure-coding | Toshihiko Uchida | Hemanth Boyina |
| [HDFS-15648](https://issues.apache.org/jira/browse/HDFS-15648) | TestFileChecksum should be parameterized |  Major | test | Ahmed Hussein | Masatake Iwasaki |
| [HDFS-15748](https://issues.apache.org/jira/browse/HDFS-15748) | RBF: Move the router related part from hadoop-federation-balance module to hadoop-hdfs-rbf. |  Major | rbf | Jinglun | Jinglun |
| [HDFS-15766](https://issues.apache.org/jira/browse/HDFS-15766) | RBF: MockResolver.getMountPoints() breaks the semantic of FileSubclusterResolver. |  Major | rbf | Jinglun | Jinglun |
| [YARN-10507](https://issues.apache.org/jira/browse/YARN-10507) | Add the capability to fs2cs to write the converted placement rules inside capacity-scheduler.xml |  Major | capacity scheduler | Peter Bacsko | Peter Bacsko |
| [HADOOP-15348](https://issues.apache.org/jira/browse/HADOOP-15348) | S3A Input Stream bytes read counter isn't getting through to StorageStatistics/instrumentation properly |  Minor | fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-15702](https://issues.apache.org/jira/browse/HDFS-15702) | Fix intermittent falilure of TestDecommission#testAllocAndIBRWhileDecommission |  Minor | hdfs, test | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-10504](https://issues.apache.org/jira/browse/YARN-10504) | Implement weight mode in Capacity Scheduler |  Major | capacity scheduler | Benjamin Teke | Benjamin Teke |
| [YARN-10570](https://issues.apache.org/jira/browse/YARN-10570) | Remove "experimental" warning message from fs2cs |  Major | scheduler | Peter Bacsko | Peter Bacsko |
| [YARN-10563](https://issues.apache.org/jira/browse/YARN-10563) | Fix dependency exclusion problem in poms |  Critical | buid, resourcemanager | Peter Bacsko | Peter Bacsko |
| [HDFS-14558](https://issues.apache.org/jira/browse/HDFS-14558) | RBF: Isolation/Fairness documentation |  Major | rbf | CR Hota | Fengnan Li |
| [HDFS-15762](https://issues.apache.org/jira/browse/HDFS-15762) | TestMultipleNNPortQOP#testMultipleNNPortOverwriteDownStream fails intermittently |  Minor | hdfs, test | Toshihiko Uchida | Toshihiko Uchida |
| [YARN-10525](https://issues.apache.org/jira/browse/YARN-10525) | Add weight mode conversion to fs2cs |  Major | capacity scheduler, fs-cs | Qi Zhu | Peter Bacsko |
| [HDFS-15672](https://issues.apache.org/jira/browse/HDFS-15672) | TestBalancerWithMultipleNameNodes#testBalancingBlockpoolsWithBlockPoolPolicy fails on trunk |  Major | balancer, test | Ahmed Hussein | Masatake Iwasaki |
| [YARN-10506](https://issues.apache.org/jira/browse/YARN-10506) | Update queue creation logic to use weight mode and allow the flexible static/dynamic creation |  Major | capacity scheduler, resourcemanager | Benjamin Teke | Andras Gyori |
| [HDFS-15549](https://issues.apache.org/jira/browse/HDFS-15549) | Use Hardlink to move replica between DISK and ARCHIVE storage if on same filesystem mount |  Major | datanode | Leon Gao | Leon Gao |
| [YARN-10574](https://issues.apache.org/jira/browse/YARN-10574) | Fix the FindBugs warning introduced in YARN-10506 |  Major | capacity scheduler, resourcemanager | Gergely Pollák | Gergely Pollák |
| [YARN-10535](https://issues.apache.org/jira/browse/YARN-10535) | Make queue placement in CapacityScheduler compliant with auto-queue-placement |  Major | capacity scheduler | Wangda Tan | Gergely Pollák |
| [YARN-10573](https://issues.apache.org/jira/browse/YARN-10573) | Enhance placement rule conversion in fs2cs in weight mode and enable it by default |  Major | capacity scheduler | Peter Bacsko | Peter Bacsko |
| [YARN-10512](https://issues.apache.org/jira/browse/YARN-10512) | CS Flexible Auto Queue Creation: Modify RM /scheduler endpoint to include mode of operation for CS |  Major | capacity scheduler | Benjamin Teke | Szilard Nemeth |
| [YARN-10578](https://issues.apache.org/jira/browse/YARN-10578) | Fix Auto Queue Creation parent handling |  Major | capacity scheduler | Andras Gyori | Andras Gyori |
| [YARN-10579](https://issues.apache.org/jira/browse/YARN-10579) | CS Flexible Auto Queue Creation: Modify RM /scheduler endpoint to include weight values for queues |  Major | capacity scheduler | Szilard Nemeth | Szilard Nemeth |
| [HDFS-15767](https://issues.apache.org/jira/browse/HDFS-15767) | RBF: Router federation rename of directory. |  Major | rbf | Jinglun | Jinglun |
| [YARN-10596](https://issues.apache.org/jira/browse/YARN-10596) | Allow static definition of childless ParentQueues with auto-queue-creation-v2 enabled |  Major | capacity scheduler | Andras Gyori | Andras Gyori |
| [YARN-10531](https://issues.apache.org/jira/browse/YARN-10531) | Be able to disable user limit factor for CapacityScheduler Leaf Queue |  Major | capacity scheduler | Wangda Tan | Qi Zhu |
| [YARN-10587](https://issues.apache.org/jira/browse/YARN-10587) | Fix AutoCreateLeafQueueCreation cap related caculation when in absolute mode. |  Major | capacity scheduler | Qi Zhu | Qi Zhu |
| [YARN-10598](https://issues.apache.org/jira/browse/YARN-10598) | CS Flexible Auto Queue Creation: Modify RM /scheduler endpoint to extend the creation type with additional information |  Major | capacity scheduler | Benjamin Teke | Benjamin Teke |
| [YARN-10599](https://issues.apache.org/jira/browse/YARN-10599) | fs2cs should generate new "auto-queue-creation-v2.enabled" properties for all parents |  Major | resourcemanager | Peter Bacsko | Peter Bacsko |
| [YARN-10600](https://issues.apache.org/jira/browse/YARN-10600) | Convert root queue in fs2cs weight mode conversion |  Major | capacity scheduler | Benjamin Teke | Benjamin Teke |
| [HADOOP-17424](https://issues.apache.org/jira/browse/HADOOP-17424) | Replace HTrace with No-Op tracer |  Major | common | Siyao Meng | Siyao Meng |
| [YARN-10604](https://issues.apache.org/jira/browse/YARN-10604) | Support auto queue creation without mapping rules |  Major | capacity scheduler | Andras Gyori | Andras Gyori |
| [YARN-10605](https://issues.apache.org/jira/browse/YARN-10605) | Add queue-mappings-override.enable property in FS2CS conversions |  Major | capacity scheduler | Andras Gyori | Andras Gyori |
| [YARN-10585](https://issues.apache.org/jira/browse/YARN-10585) | Create a class which can convert from legacy mapping rule format to the new JSON format |  Major | resourcemanager, scheduler | Gergely Pollák | Gergely Pollák |
| [YARN-10352](https://issues.apache.org/jira/browse/YARN-10352) | Skip schedule on not heartbeated nodes in Multi Node Placement |  Major | scheduler | Prabhu Joseph | Prabhu Joseph |
| [YARN-10612](https://issues.apache.org/jira/browse/YARN-10612) | Fix findbugs issue introduced in YARN-10585 |  Major | scheduler | Gergely Pollák | Gergely Pollák |
| [HADOOP-17432](https://issues.apache.org/jira/browse/HADOOP-17432) | [JDK 16] KerberosUtil#getOidInstance is broken by JEP 396 |  Major | auth | Akira Ajisaka | Akira Ajisaka |
| [YARN-10615](https://issues.apache.org/jira/browse/YARN-10615) | Fix Auto Queue Creation hierarchy construction to use queue path instead of short queue name |  Critical | yarn | Andras Gyori | Andras Gyori |
| [HDFS-15820](https://issues.apache.org/jira/browse/HDFS-15820) | Ensure snapshot root trash provisioning happens only post safe mode exit |  Major | snapshots | Shashikant Banerjee | Shashikant Banerjee |
| [HDFS-15683](https://issues.apache.org/jira/browse/HDFS-15683) | Allow configuring DISK/ARCHIVE capacity for individual volumes |  Major | datanode | Leon Gao | Leon Gao |
| [HDFS-15817](https://issues.apache.org/jira/browse/HDFS-15817) | Rename snapshots while marking them deleted |  Major | snapshots | Shashikant Banerjee | Shashikant Banerjee |
| [HDFS-15818](https://issues.apache.org/jira/browse/HDFS-15818) | Fix TestFsDatasetImpl.testReadLockCanBeDisabledByConfig |  Minor | test | Leon Gao | Leon Gao |
| [YARN-10619](https://issues.apache.org/jira/browse/YARN-10619) | CS Mapping Rule %specified rule catches default submissions |  Major | capacity scheduler | Gergely Pollák | Gergely Pollák |
| [YARN-10620](https://issues.apache.org/jira/browse/YARN-10620) | fs2cs: parentQueue for certain placement rules are not set during conversion |  Major | capacity scheduler | Peter Bacsko | Peter Bacsko |
| [HADOOP-13327](https://issues.apache.org/jira/browse/HADOOP-13327) | Add OutputStream + Syncable to the Filesystem Specification |  Major | fs | Steve Loughran | Steve Loughran |
| [YARN-10624](https://issues.apache.org/jira/browse/YARN-10624) | Support max queues limit configuration in new auto created queue, consistent with old auto created. |  Major | capacity scheduler | Qi Zhu | Qi Zhu |
| [YARN-10622](https://issues.apache.org/jira/browse/YARN-10622) | Fix preemption policy to exclude childless ParentQueues |  Major | capacity scheduler | Andras Gyori | Andras Gyori |
| [HDFS-15836](https://issues.apache.org/jira/browse/HDFS-15836) | RBF: Fix contract tests after HADOOP-13327 |  Major | rbf | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17038](https://issues.apache.org/jira/browse/HADOOP-17038) | Support disabling buffered reads in ABFS positional reads |  Major | fs/azure | Anoop Sam John | Anoop Sam John |
| [HADOOP-17109](https://issues.apache.org/jira/browse/HADOOP-17109) | add guava BaseEncoding to illegalClasses |  Major | build, common | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15834](https://issues.apache.org/jira/browse/HDFS-15834) | Remove the usage of org.apache.log4j.Level |  Major | hdfs-common | Akira Ajisaka | Akira Ajisaka |
| [YARN-10635](https://issues.apache.org/jira/browse/YARN-10635) | CSMapping rule can return paths with empty parts |  Major | capacity scheduler | Gergely Pollák | Gergely Pollák |
| [YARN-10636](https://issues.apache.org/jira/browse/YARN-10636) | CS Auto Queue creation should reject submissions with empty path parts |  Major | capacity scheduler | Gergely Pollák | Gergely Pollák |
| [YARN-10513](https://issues.apache.org/jira/browse/YARN-10513) | CS Flexible Auto Queue Creation RM UIv2 modifications |  Major | capacity scheduler, resourcemanager, ui | Benjamin Teke | Andras Gyori |
| [HDFS-15845](https://issues.apache.org/jira/browse/HDFS-15845) | RBF: Router fails to start due to NoClassDefFoundError for hadoop-federation-balance |  Major | rbf | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-15847](https://issues.apache.org/jira/browse/HDFS-15847) | create client protocol: add ecPolicyName & storagePolicy param to debug statement string |  Minor | erasure-coding, namanode | Bhavik Patel | Bhavik Patel |
| [HADOOP-16748](https://issues.apache.org/jira/browse/HADOOP-16748) | Migrate to Python 3 and upgrade Yetus to 0.13.0 |  Major | common | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15781](https://issues.apache.org/jira/browse/HDFS-15781) | Add metrics for how blocks are moved in replaceBlock |  Minor | datanode | Leon Gao | Leon Gao |
| [YARN-10609](https://issues.apache.org/jira/browse/YARN-10609) | Update the document for YARN-10531(Be able to disable user limit factor for CapacityScheduler Leaf Queue) |  Major | documentation | Qi Zhu | Qi Zhu |
| [YARN-10627](https://issues.apache.org/jira/browse/YARN-10627) | Extend logging to give more information about weight mode |  Major | yarn | Benjamin Teke | Benjamin Teke |
| [YARN-10655](https://issues.apache.org/jira/browse/YARN-10655) | Limit queue creation depth relative to its first static parent |  Major | yarn | Andras Gyori | Andras Gyori |
| [YARN-10532](https://issues.apache.org/jira/browse/YARN-10532) | Capacity Scheduler Auto Queue Creation: Allow auto delete queue when queue is not being used |  Major | capacity scheduler | Wangda Tan | Qi Zhu |
| [YARN-10639](https://issues.apache.org/jira/browse/YARN-10639) | Queueinfo related capacity, should adjusted to weight mode. |  Major | capacity scheduler | Qi Zhu | Qi Zhu |
| [YARN-10640](https://issues.apache.org/jira/browse/YARN-10640) | Adjust the queue Configured capacity to  Configured weight number for weight mode in UI. |  Major | capacity scheduler, ui | Qi Zhu | Qi Zhu |
| [HDFS-15848](https://issues.apache.org/jira/browse/HDFS-15848) | Snapshot Operations: Add debug logs at the entry point |  Minor | snapshots | Bhavik Patel | Bhavik Patel |
| [YARN-10412](https://issues.apache.org/jira/browse/YARN-10412) | Move CS placement rule related changes to a separate package |  Major | capacity scheduler | Gergely Pollák | Gergely Pollák |
| [HADOOP-17548](https://issues.apache.org/jira/browse/HADOOP-17548) | ABFS: Toggle Store Mkdirs request overwrite parameter |  Major | fs/azure | Sumangala Patki | Sumangala Patki |
| [YARN-10689](https://issues.apache.org/jira/browse/YARN-10689) | Fix the findbugs issues in extractFloatValueFromWeightConfig. |  Major | capacity scheduler | Qi Zhu | Qi Zhu |
| [YARN-10686](https://issues.apache.org/jira/browse/YARN-10686) | Fix TestCapacitySchedulerAutoQueueCreation#testAutoQueueCreationFailsForEmptyPathWithAQCAndWeightMode |  Major | capacity scheduler | Qi Zhu | Qi Zhu |
| [HDFS-15890](https://issues.apache.org/jira/browse/HDFS-15890) | Improve the Logs for File Concat Operation |  Minor | namenode | Bhavik Patel | Bhavik Patel |
| [HDFS-13975](https://issues.apache.org/jira/browse/HDFS-13975) | TestBalancer#testMaxIterationTime fails sporadically |  Major | balancer, test | Jason Darrell Lowe | Toshihiko Uchida |
| [YARN-10688](https://issues.apache.org/jira/browse/YARN-10688) | ClusterMetrics should support GPU capacity related metrics. |  Major | metrics, resourcemanager | Qi Zhu | Qi Zhu |
| [YARN-10659](https://issues.apache.org/jira/browse/YARN-10659) | Improve CS MappingRule %secondary\_group evaluation |  Major | capacity scheduler | Gergely Pollák | Gergely Pollák |
| [YARN-10692](https://issues.apache.org/jira/browse/YARN-10692) | Add Node GPU Utilization and apply to NodeMetrics. |  Major | gpu | Qi Zhu | Qi Zhu |
| [YARN-10641](https://issues.apache.org/jira/browse/YARN-10641) | Refactor the max app related update, and fix maxApplications update error when add new queues. |  Critical | capacity scheduler | Qi Zhu | Qi Zhu |
| [YARN-10674](https://issues.apache.org/jira/browse/YARN-10674) | fs2cs should generate auto-created queue deletion properties |  Major | scheduler | Qi Zhu | Qi Zhu |
| [HDFS-15902](https://issues.apache.org/jira/browse/HDFS-15902) | Improve the log for HTTPFS server operation |  Minor | httpfs | Bhavik Patel | Bhavik Patel |
| [YARN-10713](https://issues.apache.org/jira/browse/YARN-10713) | ClusterMetrics should support custom resource capacity related metrics. |  Major | metrics | Qi Zhu | Qi Zhu |
| [YARN-10120](https://issues.apache.org/jira/browse/YARN-10120) | In Federation Router Nodes/Applications/About pages throws 500 exception when https is enabled |  Critical | federation | Sushanta Sen | Bilwa S T |
| [YARN-10597](https://issues.apache.org/jira/browse/YARN-10597) | CSMappingPlacementRule should not create new instance of Groups |  Major | capacity scheduler | Gergely Pollák | Gergely Pollák |
| [HDFS-15921](https://issues.apache.org/jira/browse/HDFS-15921) | Improve the log for the Storage Policy Operations |  Minor | namenode | Bhavik Patel | Bhavik Patel |
| [YARN-9618](https://issues.apache.org/jira/browse/YARN-9618) | NodesListManager event improvement |  Critical | resourcemanager | Bibin Chundatt | Qi Zhu |
| [HDFS-15940](https://issues.apache.org/jira/browse/HDFS-15940) | Some tests in TestBlockRecovery are consistently failing |  Major | test | Viraj Jasani | Viraj Jasani |
| [YARN-10714](https://issues.apache.org/jira/browse/YARN-10714) | Remove dangling dynamic queues on reinitialization |  Major | capacity scheduler | Andras Gyori | Andras Gyori |
| [YARN-10564](https://issues.apache.org/jira/browse/YARN-10564) | Support Auto Queue Creation template configurations |  Major | capacity scheduler | Andras Gyori | Andras Gyori |
| [YARN-10702](https://issues.apache.org/jira/browse/YARN-10702) | Add cluster metric for amount of CPU used by RM Event Processor |  Minor | yarn | Jim Brennan | Jim Brennan |
| [YARN-10503](https://issues.apache.org/jira/browse/YARN-10503) | Support queue capacity in terms of absolute resources with custom resourceType. |  Critical | gpu | Qi Zhu | Qi Zhu |
| [HADOOP-17630](https://issues.apache.org/jira/browse/HADOOP-17630) | [JDK 15] TestPrintableString fails due to Unicode 13.0 support |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17524](https://issues.apache.org/jira/browse/HADOOP-17524) | Remove EventCounter and Log counters from JVM Metrics |  Major | common | Akira Ajisaka | Viraj Jasani |
| [HADOOP-17576](https://issues.apache.org/jira/browse/HADOOP-17576) | ABFS: Disable throttling update for auth failures |  Major | fs/azure | Sumangala Patki | Sumangala Patki |
| [YARN-10723](https://issues.apache.org/jira/browse/YARN-10723) | Change CS nodes page in UI to support custom resource. |  Major | resourcemanager | Qi Zhu | Qi Zhu |
| [HADOOP-16948](https://issues.apache.org/jira/browse/HADOOP-16948) | ABFS: Support infinite lease dirs |  Minor | common | Billie Rinaldi | Billie Rinaldi |
| [YARN-10654](https://issues.apache.org/jira/browse/YARN-10654) | Dots '.' in CSMappingRule path variables should be replaced |  Major | capacity scheduler | Gergely Pollák | Peter Bacsko |
| [HADOOP-17112](https://issues.apache.org/jira/browse/HADOOP-17112) | whitespace not allowed in paths when saving files to s3a via committer |  Blocker | fs/s3 | Krzysztof Adamski | Krzysztof Adamski |
| [YARN-10637](https://issues.apache.org/jira/browse/YARN-10637) | fs2cs: add queue autorefresh policy during conversion |  Major | fairscheduler, fs-cs | Qi Zhu | Qi Zhu |
| [HADOOP-17661](https://issues.apache.org/jira/browse/HADOOP-17661) | mvn versions:set fails to parse pom.xml |  Blocker | build | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-15961](https://issues.apache.org/jira/browse/HDFS-15961) | standby namenode failed to start ordered snapshot deletion is enabled while having snapshottable directories |  Major | snapshots | Nilotpal Nandi | Shashikant Banerjee |
| [YARN-10739](https://issues.apache.org/jira/browse/YARN-10739) | GenericEventHandler.printEventQueueDetails causes RM recovery to take too much time |  Critical | resourcemanager | Zhanqi Cai | Qi Zhu |
| [HADOOP-11245](https://issues.apache.org/jira/browse/HADOOP-11245) | Update NFS gateway to use Netty4 |  Major | nfs | Brandon Li | Wei-Chiu Chuang |
| [YARN-10707](https://issues.apache.org/jira/browse/YARN-10707) | Support custom resources in ResourceUtilization, and update Node GPU Utilization to use. |  Major | gpu, yarn | Qi Zhu | Qi Zhu |
| [HADOOP-17653](https://issues.apache.org/jira/browse/HADOOP-17653) | Do not use guava's Files.createTempDir() |  Major | common | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-15952](https://issues.apache.org/jira/browse/HDFS-15952) | TestRouterRpcMultiDestination#testProxyGetTransactionID and testProxyVersionRequest are flaky |  Major | rbf | Harunobu Daikoku | Akira Ajisaka |
| [HDFS-15923](https://issues.apache.org/jira/browse/HDFS-15923) | RBF:  Authentication failed when rename accross sub clusters |  Major | rbf | zhuobin zheng | zhuobin zheng |
| [HADOOP-17644](https://issues.apache.org/jira/browse/HADOOP-17644) | Add back the exceptions removed by HADOOP-17432 for compatibility |  Blocker | build | Akira Ajisaka | Quan Li |
| [HDFS-15997](https://issues.apache.org/jira/browse/HDFS-15997) | Implement dfsadmin -provisionSnapshotTrash -all |  Major | dfsadmin | Siyao Meng | Siyao Meng |
| [YARN-10642](https://issues.apache.org/jira/browse/YARN-10642) | Race condition: AsyncDispatcher can get stuck by the changes introduced in YARN-8995 |  Critical | resourcemanager | Chenyu Zheng | Chenyu Zheng |
| [YARN-9615](https://issues.apache.org/jira/browse/YARN-9615) | Add dispatcher metrics to RM |  Major | metrics, resourcemanager | Jonathan Hung | Qi Zhu |
| [YARN-10571](https://issues.apache.org/jira/browse/YARN-10571) | Refactor dynamic queue handling logic |  Minor | capacity scheduler | Andras Gyori | Andras Gyori |
| [HADOOP-17685](https://issues.apache.org/jira/browse/HADOOP-17685) | Fix junit deprecation warnings in hadoop-common module |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [YARN-10761](https://issues.apache.org/jira/browse/YARN-10761) | Add more event type to RM Dispatcher event metrics. |  Major | resourcemanager | Qi Zhu | Qi Zhu |
| [HADOOP-17665](https://issues.apache.org/jira/browse/HADOOP-17665) | Ignore missing keystore configuration in reloading mechanism |  Major | common | Borislav Iordanov | Borislav Iordanov |
| [HADOOP-17663](https://issues.apache.org/jira/browse/HADOOP-17663) | Remove useless property hadoop.assemblies.version in pom file |  Trivial | build | Wei-Chiu Chuang | Akira Ajisaka |
| [HADOOP-17115](https://issues.apache.org/jira/browse/HADOOP-17115) | Replace Guava Sets usage by Hadoop's own Sets in hadoop-common and hadoop-tools |  Major | common | Ahmed Hussein | Viraj Jasani |
| [HADOOP-17666](https://issues.apache.org/jira/browse/HADOOP-17666) | Update LICENSE for 3.3.1 |  Blocker | common | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-10771](https://issues.apache.org/jira/browse/YARN-10771) | Add cluster metric for size of SchedulerEventQueue and RMEventQueue |  Major | metrics, resourcemanager | chaosju | chaosju |
| [HADOOP-17722](https://issues.apache.org/jira/browse/HADOOP-17722) | Replace Guava Sets usage by Hadoop's own Sets in hadoop-mapreduce-project |  Major | common | Viraj Jasani | Viraj Jasani |
| [HADOOP-17720](https://issues.apache.org/jira/browse/HADOOP-17720) | Replace Guava Sets usage by Hadoop's own Sets in hadoop-hdfs-project |  Major | common | Viraj Jasani | Viraj Jasani |
| [HADOOP-17721](https://issues.apache.org/jira/browse/HADOOP-17721) | Replace Guava Sets usage by Hadoop's own Sets in hadoop-yarn-project |  Major | common | Viraj Jasani | Viraj Jasani |
| [YARN-10783](https://issues.apache.org/jira/browse/YARN-10783) | Allow definition of auto queue template properties in root |  Major | capacity scheduler | Andras Gyori | Andras Gyori |
| [YARN-10782](https://issues.apache.org/jira/browse/YARN-10782) | Extend /scheduler endpoint with template properties |  Major | capacity scheduler | Andras Gyori | Andras Gyori |
| [HDFS-15973](https://issues.apache.org/jira/browse/HDFS-15973) | RBF: Add permission check before doing router federation rename. |  Major | rbf | Jinglun | Jinglun |
| [HADOOP-17152](https://issues.apache.org/jira/browse/HADOOP-17152) | Implement wrapper for guava newArrayList and newLinkedList |  Major | common | Ahmed Hussein | Viraj Jasani |
| [YARN-10807](https://issues.apache.org/jira/browse/YARN-10807) | Parents node labels are incorrectly added to child queues in weight mode |  Major | capacity scheduler | Benjamin Teke | Benjamin Teke |
| [YARN-10801](https://issues.apache.org/jira/browse/YARN-10801) | Fix Auto Queue template to properly set all configuration properties |  Major | capacity scheduler | Andras Gyori | Andras Gyori |
| [YARN-10780](https://issues.apache.org/jira/browse/YARN-10780) | Optimise retrieval of configured node labels in CS queues |  Major | capacity scheduler | Andras Gyori | Andras Gyori |
| [HDFS-15659](https://issues.apache.org/jira/browse/HDFS-15659) | Set dfs.namenode.redundancy.considerLoad to false in MiniDFSCluster |  Major | test | Akira Ajisaka | Ahmed Hussein |
| [HADOOP-17331](https://issues.apache.org/jira/browse/HADOOP-17331) | [JDK 15] TestDNS fails by UncheckedIOException |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15671](https://issues.apache.org/jira/browse/HDFS-15671) | TestBalancerRPCDelay#testBalancerRPCDelayQpsDefault fails on Trunk |  Major | balancer, test | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17596](https://issues.apache.org/jira/browse/HADOOP-17596) | ABFS: Change default Readahead Queue Depth from num(processors) to const |  Major | fs/azure | Sumangala Patki | Sumangala Patki |
| [HADOOP-17715](https://issues.apache.org/jira/browse/HADOOP-17715) | ABFS: Append blob tests with non HNS accounts fail |  Minor | fs/azure | Sneha Varma | Sneha Varma |
| [HADOOP-17714](https://issues.apache.org/jira/browse/HADOOP-17714) | ABFS: testBlobBackCompatibility, testRandomRead & WasbAbfsCompatibility tests fail when triggered with default configs |  Minor | test | Sneha Varma | Sneha Varma |
| [HADOOP-17795](https://issues.apache.org/jira/browse/HADOOP-17795) | Provide fallbacks for callqueue.impl and scheduler.impl |  Major | ipc | Viraj Jasani | Viraj Jasani |
| [HADOOP-16272](https://issues.apache.org/jira/browse/HADOOP-16272) | Update HikariCP to 4.0.3 |  Major | build, common | Yuming Wang | Viraj Jasani |
| [HDFS-16067](https://issues.apache.org/jira/browse/HDFS-16067) | Support Append API in NNThroughputBenchmark |  Minor | namanode | Renukaprasad C | Renukaprasad C |
| [YARN-10657](https://issues.apache.org/jira/browse/YARN-10657) | We should make max application per queue to support node label. |  Major | capacity scheduler | Qi Zhu | Andras Gyori |
| [YARN-10829](https://issues.apache.org/jira/browse/YARN-10829) | Support getApplications API in FederationClientInterceptor |  Major | federation, router | Akshat Bordia | Akshat Bordia |
| [HDFS-16140](https://issues.apache.org/jira/browse/HDFS-16140) | TestBootstrapAliasmap fails by BindException |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [YARN-10727](https://issues.apache.org/jira/browse/YARN-10727) | ParentQueue does not validate the queue on removal |  Major | capacity scheduler | Andras Gyori | Andras Gyori |
| [YARN-10790](https://issues.apache.org/jira/browse/YARN-10790) | CS Flexible AQC: Add separate parent and leaf template property. |  Major | capacity scheduler | Andras Gyori | Andras Gyori |
| [HADOOP-17814](https://issues.apache.org/jira/browse/HADOOP-17814) | Provide fallbacks for identity/cost providers and backoff enable |  Major | ipc | Viraj Jasani | Viraj Jasani |
| [YARN-10841](https://issues.apache.org/jira/browse/YARN-10841) | Fix token reset synchronization for UAM response token |  Minor | federation | Minni Mittal | Minni Mittal |
| [YARN-10838](https://issues.apache.org/jira/browse/YARN-10838) | Implement an optimised version of Configuration getPropsWithPrefix |  Major | capacity scheduler | Andras Gyori | Andras Gyori |
| [HDFS-16184](https://issues.apache.org/jira/browse/HDFS-16184) | De-flake TestBlockScanner#testSkipRecentAccessFile |  Major | test | Viraj Jasani | Viraj Jasani |
| [HDFS-16143](https://issues.apache.org/jira/browse/HDFS-16143) | TestEditLogTailer#testStandbyTriggersLogRollsWhenTailInProgressEdits is flaky |  Major | test | Akira Ajisaka | Viraj Jasani |
| [HDFS-16192](https://issues.apache.org/jira/browse/HDFS-16192) | ViewDistributedFileSystem#rename wrongly using src in the place of dst. |  Major | viewfs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-17156](https://issues.apache.org/jira/browse/HADOOP-17156) | Clear abfs readahead requests on stream close |  Major | fs/azure | Rajesh Balamohan | Mukund Thakur |
| [YARN-10576](https://issues.apache.org/jira/browse/YARN-10576) | Update Capacity Scheduler documentation with JSON-based placement mapping |  Major | capacity scheduler, documentation | Peter Bacsko | Benjamin Teke |
| [YARN-10522](https://issues.apache.org/jira/browse/YARN-10522) | Document for Flexible Auto Queue Creation in Capacity Scheduler |  Major | capacity scheduler | Qi Zhu | Benjamin Teke |
| [YARN-10646](https://issues.apache.org/jira/browse/YARN-10646) | TestCapacitySchedulerWeightMode test descriptor comments doesn't reflect the correct scenario |  Major | capacity scheduler | Benjamin Teke | Benjamin Teke |
| [YARN-10919](https://issues.apache.org/jira/browse/YARN-10919) | Remove LeafQueue#scheduler field |  Minor | capacity scheduler | Szilard Nemeth | Benjamin Teke |
| [YARN-10893](https://issues.apache.org/jira/browse/YARN-10893) | Add metrics for getClusterMetrics and getApplications APIs in FederationClientInterceptor |  Major | federation, metrics, router | Akshat Bordia | Akshat Bordia |
| [YARN-10914](https://issues.apache.org/jira/browse/YARN-10914) | Simplify duplicated code for tracking ResourceUsage in AbstractCSQueue |  Minor | capacity scheduler | Szilard Nemeth | Tamas Domok |
| [YARN-10910](https://issues.apache.org/jira/browse/YARN-10910) | AbstractCSQueue#setupQueueConfigs: Separate validation logic from initialization logic |  Minor | capacity scheduler | Szilard Nemeth | Benjamin Teke |
| [YARN-10852](https://issues.apache.org/jira/browse/YARN-10852) | Optimise CSConfiguration getAllUserWeightsForQueue |  Major | capacity scheduler | Andras Gyori | Andras Gyori |
| [YARN-10872](https://issues.apache.org/jira/browse/YARN-10872) | Replace getPropsWithPrefix calls in AutoCreatedQueueTemplate |  Major | capacity scheduler | Andras Gyori | Benjamin Teke |
| [YARN-10912](https://issues.apache.org/jira/browse/YARN-10912) | AbstractCSQueue#updateConfigurableResourceRequirement: Separate validation logic from initialization logic |  Minor | capacity scheduler | Szilard Nemeth | Tamas Domok |
| [YARN-10917](https://issues.apache.org/jira/browse/YARN-10917) | Investigate and simplify CapacitySchedulerConfigValidator#validateQueueHierarchy |  Minor | capacity scheduler | Szilard Nemeth | Tamas Domok |
| [YARN-10915](https://issues.apache.org/jira/browse/YARN-10915) | AbstractCSQueue: Simplify complex logic in methods: deriveCapacityFromAbsoluteConfigurations and updateEffectiveResources |  Minor | capacity scheduler | Szilard Nemeth | Benjamin Teke |
| [HDFS-16218](https://issues.apache.org/jira/browse/HDFS-16218) | RBF: Use HdfsConfiguration for passing in Router principal |  Major | rbf | Akira Ajisaka | Fengnan Li |
| [HDFS-16217](https://issues.apache.org/jira/browse/HDFS-16217) | RBF: Set default value of hdfs.fedbalance.procedure.scheduler.journal.uri by adding appropriate config resources |  Major | rbf | Akira Ajisaka | Viraj Jasani |
| [HDFS-16227](https://issues.apache.org/jira/browse/HDFS-16227) | testMoverWithStripedFile fails intermittently |  Major | test | Viraj Jasani | Viraj Jasani |
| [YARN-10913](https://issues.apache.org/jira/browse/YARN-10913) | AbstractCSQueue: Group preemption methods and fields into a separate class |  Minor | capacity scheduler | Szilard Nemeth | Szilard Nemeth |
| [YARN-10950](https://issues.apache.org/jira/browse/YARN-10950) | Code cleanup in QueueCapacities |  Minor | capacity scheduler | Szilard Nemeth | Adam Antal |
| [YARN-10911](https://issues.apache.org/jira/browse/YARN-10911) | AbstractCSQueue: Create a separate class for usernames and weights that are travelling in a Map |  Minor | capacity scheduler, test | Szilard Nemeth | Szilard Nemeth |
| [HDFS-16213](https://issues.apache.org/jira/browse/HDFS-16213) | Flaky test TestFsDatasetImpl#testDnRestartWithHardLink |  Major | test | Viraj Jasani | Viraj Jasani |
| [YARN-10897](https://issues.apache.org/jira/browse/YARN-10897) | Introduce QueuePath class |  Major | resourcemanager, yarn | Andras Gyori | Andras Gyori |
| [YARN-10961](https://issues.apache.org/jira/browse/YARN-10961) | TestCapacityScheduler: reuse appHelper where feasible |  Major | capacity scheduler, test | Tamas Domok | Tamas Domok |
| [HDFS-16219](https://issues.apache.org/jira/browse/HDFS-16219) | RBF: Set default map tasks and bandwidth in RouterFederationRename |  Major | rbf | Akira Ajisaka | Viraj Jasani |
| [HADOOP-17910](https://issues.apache.org/jira/browse/HADOOP-17910) | [JDK 17] TestNetUtils fails |  Major | common | Akira Ajisaka | Viraj Jasani |
| [HDFS-16231](https://issues.apache.org/jira/browse/HDFS-16231) | Fix TestDataNodeMetrics#testReceivePacketSlowMetrics |  Major | datanode, metrics | Haiyang Hu | Haiyang Hu |
| [YARN-10957](https://issues.apache.org/jira/browse/YARN-10957) | Use invokeConcurrent Overload with Collection in getClusterMetrics |  Major | federation, router | Akshat Bordia | Akshat Bordia |
| [YARN-10960](https://issues.apache.org/jira/browse/YARN-10960) | Extract test queues and related methods from TestCapacityScheduler |  Major | capacity scheduler, test | Tamas Domok | Tamas Domok |
| [HADOOP-17929](https://issues.apache.org/jira/browse/HADOOP-17929) | implement non-guava Precondition checkArgument |  Major | command | Ahmed Hussein | Ahmed Hussein |
| [HDFS-16222](https://issues.apache.org/jira/browse/HDFS-16222) | Fix ViewDFS with mount points for HDFS only API |  Major | viewfs | Ayush Saxena | Ayush Saxena |
| [HADOOP-17198](https://issues.apache.org/jira/browse/HADOOP-17198) | Support S3 Access Points |  Major | fs/s3 | Steve Loughran | Bogdan Stolojan |
| [HADOOP-17951](https://issues.apache.org/jira/browse/HADOOP-17951) | AccessPoint verifyBucketExistsV2 always returns false |  Trivial | fs/s3 | Bogdan Stolojan | Bogdan Stolojan |
| [HADOOP-17947](https://issues.apache.org/jira/browse/HADOOP-17947) | Provide alternative to Guava VisibleForTesting |  Major | common | Viraj Jasani | Viraj Jasani |
| [HADOOP-17930](https://issues.apache.org/jira/browse/HADOOP-17930) | implement non-guava Precondition checkState |  Major | common | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17952](https://issues.apache.org/jira/browse/HADOOP-17952) | Replace Guava VisibleForTesting by Hadoop's own annotation in hadoop-common-project modules |  Major | common | Viraj Jasani | Viraj Jasani |
| [YARN-10962](https://issues.apache.org/jira/browse/YARN-10962) | Do not extend from CapacitySchedulerTestBase when not needed |  Major | capacity scheduler, test | Tamas Domok | Tamas Domok |
| [HADOOP-17957](https://issues.apache.org/jira/browse/HADOOP-17957) | Replace Guava VisibleForTesting by Hadoop's own annotation in hadoop-hdfs-project modules |  Major | build, common | Viraj Jasani | Viraj Jasani |
| [HADOOP-17959](https://issues.apache.org/jira/browse/HADOOP-17959) | Replace Guava VisibleForTesting by Hadoop's own annotation in hadoop-cloud-storage-project and hadoop-mapreduce-project modules |  Major | build, common | Viraj Jasani | Viraj Jasani |
| [HADOOP-17962](https://issues.apache.org/jira/browse/HADOOP-17962) | Replace Guava VisibleForTesting by Hadoop's own annotation in hadoop-tools modules |  Major | build, common | Viraj Jasani | Viraj Jasani |
| [HADOOP-17963](https://issues.apache.org/jira/browse/HADOOP-17963) | Replace Guava VisibleForTesting by Hadoop's own annotation in hadoop-yarn-project modules |  Major | build, common | Viraj Jasani | Viraj Jasani |
| [HADOOP-17123](https://issues.apache.org/jira/browse/HADOOP-17123) | remove guava Preconditions from Hadoop-common-project modules |  Major | common | Ahmed Hussein | Ahmed Hussein |
| [HDFS-16276](https://issues.apache.org/jira/browse/HDFS-16276) | RBF:  Remove the useless configuration of rpc isolation in md |  Minor | documentation, rbf | Xiangyi Zhu | Xiangyi Zhu |
| [YARN-10942](https://issues.apache.org/jira/browse/YARN-10942) | Move AbstractCSQueue fields to separate objects that are tracking usage |  Minor | capacity scheduler | Szilard Nemeth | Szilard Nemeth |
| [YARN-10954](https://issues.apache.org/jira/browse/YARN-10954) | Remove commented code block from CSQueueUtils#loadCapacitiesByLabelsFromConf |  Trivial | capacity scheduler | Szilard Nemeth | Andras Gyori |
| [YARN-10949](https://issues.apache.org/jira/browse/YARN-10949) | Simplify AbstractCSQueue#updateMaxAppRelatedField and find a more meaningful name for this method |  Minor | capacity scheduler | Szilard Nemeth | Andras Gyori |
| [YARN-10958](https://issues.apache.org/jira/browse/YARN-10958) | Use correct configuration for Group service init in CSMappingPlacementRule |  Major | capacity scheduler | Peter Bacsko | Szilard Nemeth |
| [YARN-10916](https://issues.apache.org/jira/browse/YARN-10916) | Simplify GuaranteedOrZeroCapacityOverTimePolicy#computeQueueManagementChanges |  Minor | capacity scheduler | Szilard Nemeth | Szilard Nemeth |
| [YARN-10948](https://issues.apache.org/jira/browse/YARN-10948) | Rename SchedulerQueue#activeQueue to activateQueue |  Minor | capacity scheduler | Szilard Nemeth | Adam Antal |
| [YARN-10930](https://issues.apache.org/jira/browse/YARN-10930) | Introduce universal configured capacity vector |  Major | capacity scheduler | Andras Gyori | Andras Gyori |
| [YARN-10909](https://issues.apache.org/jira/browse/YARN-10909) | AbstractCSQueue: Annotate all methods with VisibleForTesting that are only used by test code |  Minor | capacity scheduler | Szilard Nemeth | Szilard Nemeth |
| [HADOOP-17970](https://issues.apache.org/jira/browse/HADOOP-17970) | unguava: remove Preconditions from hdfs-projects module |  Major | common | Ahmed Hussein | Ahmed Hussein |
| [YARN-10924](https://issues.apache.org/jira/browse/YARN-10924) | Clean up CapacityScheduler#initScheduler |  Minor | capacity scheduler | Szilard Nemeth | Szilard Nemeth |
| [YARN-10904](https://issues.apache.org/jira/browse/YARN-10904) | Remove unnecessary fields from AbstractCSQueue or group fields by feature if possible |  Minor | capacity scheduler | Szilard Nemeth | Szilard Nemeth |
| [YARN-10985](https://issues.apache.org/jira/browse/YARN-10985) | Add some tests to verify ACL behaviour in CapacitySchedulerConfiguration |  Minor | capacity scheduler | Szilard Nemeth | Szilard Nemeth |
| [HADOOP-17374](https://issues.apache.org/jira/browse/HADOOP-17374) | AliyunOSS: support ListObjectsV2 |  Major | fs/oss | wujinhu | wujinhu |
| [YARN-10998](https://issues.apache.org/jira/browse/YARN-10998) | Add YARN\_ROUTER\_HEAPSIZE to yarn-env for routers |  Minor | federation, router | Minni Mittal | Minni Mittal |
| [HADOOP-18018](https://issues.apache.org/jira/browse/HADOOP-18018) | unguava: remove Preconditions from hadoop-tools modules |  Major | common | Viraj Jasani | Viraj Jasani |
| [HADOOP-18017](https://issues.apache.org/jira/browse/HADOOP-18017) | unguava: remove Preconditions from hadoop-yarn-project modules |  Major | common | Viraj Jasani | Viraj Jasani |
| [YARN-11003](https://issues.apache.org/jira/browse/YARN-11003) | Make RMNode aware of all (OContainer inclusive) allocated resources |  Minor | container, resourcemanager | Andrew Chung | Andrew Chung |
| [HDFS-16336](https://issues.apache.org/jira/browse/HDFS-16336) | De-flake TestRollingUpgrade#testRollback |  Minor | hdfs, test | Kevin Wikant | Viraj Jasani |
| [HDFS-16171](https://issues.apache.org/jira/browse/HDFS-16171) | De-flake testDecommissionStatus |  Major | test | Viraj Jasani | Viraj Jasani |
| [HADOOP-18022](https://issues.apache.org/jira/browse/HADOOP-18022) | Add restrict-imports-enforcer-rule for Guava Preconditions in hadoop-main pom |  Major | common | Viraj Jasani | Viraj Jasani |
| [HADOOP-18025](https://issues.apache.org/jira/browse/HADOOP-18025) | Upgrade HBase version to 1.7.1 for hbase1 profile |  Major | build | Viraj Jasani | Viraj Jasani |
| [YARN-11031](https://issues.apache.org/jira/browse/YARN-11031) | Improve the maintainability of RM webapp tests like TestRMWebServicesCapacitySched |  Major | capacity scheduler | Tamas Domok | Tamas Domok |
| [YARN-11038](https://issues.apache.org/jira/browse/YARN-11038) | Fix testQueueSubmitWithACL\* tests in TestAppManager |  Major | yarn | Tamas Domok | Tamas Domok |
| [YARN-11005](https://issues.apache.org/jira/browse/YARN-11005) | Implement the core QUEUE\_LENGTH\_THEN\_RESOURCES OContainer allocation policy |  Minor | resourcemanager | Andrew Chung | Andrew Chung |
| [YARN-10982](https://issues.apache.org/jira/browse/YARN-10982) | Replace all occurences of queuePath with the new QueuePath class |  Major | capacity scheduler | Andras Gyori | Tibor Kovács |
| [HADOOP-18039](https://issues.apache.org/jira/browse/HADOOP-18039) | Upgrade hbase2 version and fix TestTimelineWriterHBaseDown |  Major | build | Viraj Jasani | Viraj Jasani |
| [YARN-11024](https://issues.apache.org/jira/browse/YARN-11024) | Create an AbstractLeafQueue to store the common LeafQueue + AutoCreatedLeafQueue functionality |  Major | capacity scheduler | Benjamin Teke | Benjamin Teke |
| [YARN-10907](https://issues.apache.org/jira/browse/YARN-10907) | Minimize usages of AbstractCSQueue#csContext |  Major | capacity scheduler | Szilard Nemeth | Benjamin Teke |
| [YARN-10929](https://issues.apache.org/jira/browse/YARN-10929) | Do not use a separate config in legacy CS AQC |  Minor | capacity scheduler | Szilard Nemeth | Benjamin Teke |
| [YARN-11043](https://issues.apache.org/jira/browse/YARN-11043) | Clean up checkstyle warnings from YARN-11024/10907/10929 |  Major | capacity scheduler | Benjamin Teke | Benjamin Teke |
| [YARN-10963](https://issues.apache.org/jira/browse/YARN-10963) | Split TestCapacityScheduler by test categories |  Major | capacity scheduler | Tamas Domok | Tamas Domok |
| [YARN-10951](https://issues.apache.org/jira/browse/YARN-10951) | CapacityScheduler: Move all fields and initializer code that belongs to async scheduling to a new class |  Minor | capacity scheduler | Szilard Nemeth | Szilard Nemeth |
| [HADOOP-16908](https://issues.apache.org/jira/browse/HADOOP-16908) | Prune Jackson 1 from the codebase and restrict it's usage for future |  Major | common | Wei-Chiu Chuang | Viraj Jasani |
| [HDFS-16168](https://issues.apache.org/jira/browse/HDFS-16168) | TestHDFSFileSystemContract#testAppend fails |  Major | test | Hui Fei | secfree |
| [YARN-8859](https://issues.apache.org/jira/browse/YARN-8859) | Add audit logs for router service |  Major | router | Bibin Chundatt | Minni Mittal |
| [YARN-10632](https://issues.apache.org/jira/browse/YARN-10632) | Make auto queue creation maximum allowed depth configurable |  Major | capacity scheduler | Qi Zhu | Andras Gyori |
| [YARN-11034](https://issues.apache.org/jira/browse/YARN-11034) | Add enhanced headroom in AllocateResponse |  Major | federation | Minni Mittal | Minni Mittal |
| [HDFS-16429](https://issues.apache.org/jira/browse/HDFS-16429) | Add DataSetLockManager to manage fine-grain locks for FsDataSetImpl |  Major | hdfs | Mingxiang Li | Mingxiang Li |
| [HDFS-16169](https://issues.apache.org/jira/browse/HDFS-16169) | Fix TestBlockTokenWithDFSStriped#testEnd2End failure |  Major | test | Hui Fei | secfree |
| [YARN-10995](https://issues.apache.org/jira/browse/YARN-10995) | Move PendingApplicationComparator from GuaranteedOrZeroCapacityOverTimePolicy |  Major | capacity scheduler | Benjamin Teke | Benjamin Teke |
| [YARN-10947](https://issues.apache.org/jira/browse/YARN-10947) | Simplify AbstractCSQueue#initializeQueueState |  Minor | capacity scheduler | Szilard Nemeth | Andras Gyori |
| [YARN-10944](https://issues.apache.org/jira/browse/YARN-10944) | AbstractCSQueue: Eliminate code duplication in overloaded versions of setMaxCapacity |  Minor | capacity scheduler | Szilard Nemeth | Andras Gyori |
| [YARN-10590](https://issues.apache.org/jira/browse/YARN-10590) | Consider legacy auto queue creation absolute resource template to avoid rounding errors |  Major | capacity scheduler | Qi Zhu | Andras Gyori |
| [HDFS-16458](https://issues.apache.org/jira/browse/HDFS-16458) | [SPS]: Fix bug for unit test of reconfiguring SPS mode |  Major | sps, test | Tao Li | Tao Li |
| [YARN-10983](https://issues.apache.org/jira/browse/YARN-10983) | Follow-up changes for YARN-10904 |  Minor | capacityscheduler | Szilard Nemeth | Benjamin Teke |
| [YARN-10945](https://issues.apache.org/jira/browse/YARN-10945) | Add javadoc to all methods of AbstractCSQueue |  Major | capacity scheduler, documentation | Szilard Nemeth | András Győri |
| [YARN-10918](https://issues.apache.org/jira/browse/YARN-10918) | Simplify method: CapacitySchedulerQueueManager#parseQueue |  Minor | capacity scheduler | Szilard Nemeth | Andras Gyori |
| [HADOOP-17526](https://issues.apache.org/jira/browse/HADOOP-17526) | Use Slf4jRequestLog for HttpRequestLog |  Major | common | Akira Ajisaka | Duo Zhang |
| [YARN-11036](https://issues.apache.org/jira/browse/YARN-11036) | Do not inherit from TestRMWebServicesCapacitySched |  Major | capacity scheduler, test | Tamas Domok | Tamas Domok |
| [YARN-10049](https://issues.apache.org/jira/browse/YARN-10049) | FIFOOrderingPolicy Improvements |  Major | scheduler | Manikandan R | Benjamin Teke |
| [HDFS-16499](https://issues.apache.org/jira/browse/HDFS-16499) | [SPS]: Should not start indefinitely while another SPS process is running |  Major | sps | Tao Li | Tao Li |
| [YARN-10565](https://issues.apache.org/jira/browse/YARN-10565) | Follow-up to YARN-10504 |  Major | capacity scheduler | Benjamin Teke | Benjamin Teke |
| [HDFS-13248](https://issues.apache.org/jira/browse/HDFS-13248) | RBF: Namenode need to choose block location for the client |  Major | rbf | Wu Weiwei | Owen O'Malley |
| [HDFS-15987](https://issues.apache.org/jira/browse/HDFS-15987) | Improve oiv tool to parse fsimage file in parallel with delimited format |  Major | tools | Hongbing Wang | Hongbing Wang |
| [HADOOP-13386](https://issues.apache.org/jira/browse/HADOOP-13386) | Upgrade Avro to 1.9.2 |  Major | build | Java Developer | PJ Fanning |
| [HDFS-16477](https://issues.apache.org/jira/browse/HDFS-16477) | [SPS]: Add metric PendingSPSPaths for getting the number of paths to be processed by SPS |  Major | sps | Tao Li | Tao Li |
| [HADOOP-18180](https://issues.apache.org/jira/browse/HADOOP-18180) | Remove use of scala jar twitter util-core with java futures in S3A prefetching stream |  Major | fs/s3 | PJ Fanning | PJ Fanning |
| [HDFS-16460](https://issues.apache.org/jira/browse/HDFS-16460) | [SPS]: Handle failure retries for moving tasks |  Major | sps | Tao Li | Tao Li |
| [HDFS-16484](https://issues.apache.org/jira/browse/HDFS-16484) | [SPS]: Fix an infinite loop bug in SPSPathIdProcessor thread |  Major | sps | qinyuren | qinyuren |
| [HDFS-16526](https://issues.apache.org/jira/browse/HDFS-16526) | Add metrics for slow DataNode |  Major | datanode, metrics | Renukaprasad C | Renukaprasad C |
| [HDFS-16255](https://issues.apache.org/jira/browse/HDFS-16255) | RBF: Fix dead link to fedbalance document |  Minor | documentation | Akira Ajisaka | Ashutosh Gupta |
| [HDFS-16488](https://issues.apache.org/jira/browse/HDFS-16488) | [SPS]: Expose metrics to JMX for external SPS |  Major | metrics, sps | Tao Li | Tao Li |
| [HADOOP-18177](https://issues.apache.org/jira/browse/HADOOP-18177) | document use and architecture design of prefetching s3a input stream |  Major | documentation, fs/s3 | Steve Loughran | Ahmar Suhail |
| [HADOOP-17682](https://issues.apache.org/jira/browse/HADOOP-17682) | ABFS: Support FileStatus input to OpenFileWithOptions() via OpenFileParameters |  Major | fs/azure | Sumangala Patki | Sumangala Patki |
| [HADOOP-15983](https://issues.apache.org/jira/browse/HADOOP-15983) | Use jersey-json that is built to use jackson2 |  Major | build | Akira Ajisaka | PJ Fanning |
| [YARN-11130](https://issues.apache.org/jira/browse/YARN-11130) | RouterClientRMService Has Unused import |  Minor | federation, router | Shilun Fan | Shilun Fan |
| [YARN-11122](https://issues.apache.org/jira/browse/YARN-11122) | Support getClusterNodes API in FederationClientInterceptor |  Major | federation | Shilun Fan | Shilun Fan |
| [HADOOP-18229](https://issues.apache.org/jira/browse/HADOOP-18229) | Fix Hadoop Common Java Doc Errors |  Major | build, common | Shilun Fan | Shilun Fan |
| [YARN-10465](https://issues.apache.org/jira/browse/YARN-10465) | Support getNodeToLabels, getLabelsToNodes, getClusterNodeLabels API's for Federation |  Major | federation | D M Murali Krishna Reddy | Shilun Fan |
| [YARN-11137](https://issues.apache.org/jira/browse/YARN-11137) | Improve log message in FederationClientInterceptor |  Minor | federation | Shilun Fan | Shilun Fan |
| [HDFS-15878](https://issues.apache.org/jira/browse/HDFS-15878) | RBF: Fix TestRouterWebHDFSContractCreate#testSyncable |  Major | hdfs, rbf | Renukaprasad C | Hanley Yang |
| [YARN-10487](https://issues.apache.org/jira/browse/YARN-10487) | Support getQueueUserAcls, listReservations, getApplicationAttempts, getContainerReport, getContainers, getResourceTypeInfo API's for Federation |  Major | federation, router | D M Murali Krishna Reddy | Shilun Fan |
| [YARN-11159](https://issues.apache.org/jira/browse/YARN-11159) | Support failApplicationAttempt, updateApplicationPriority, updateApplicationTimeouts API's for Federation |  Major | federation | Shilun Fan | Shilun Fan |
| [HDFS-16598](https://issues.apache.org/jira/browse/HDFS-16598) | Fix DataNode FsDatasetImpl lock issue without GS checks. |  Major | datanode | ZanderXu | ZanderXu |
| [HADOOP-18289](https://issues.apache.org/jira/browse/HADOOP-18289) | Remove WhiteBox in hadoop-kms module. |  Minor | common | Shilun Fan | Shilun Fan |
| [HDFS-16600](https://issues.apache.org/jira/browse/HDFS-16600) | Fix deadlock of fine-grain lock for FsDatastImpl of DataNode. |  Major | datanode | ZanderXu | ZanderXu |
| [YARN-10122](https://issues.apache.org/jira/browse/YARN-10122) | Support signalToContainer API for Federation |  Major | federation, yarn | Sushanta Sen | Shilun Fan |
| [HADOOP-18266](https://issues.apache.org/jira/browse/HADOOP-18266) | Replace with HashSet/TreeSet constructor in Hadoop-common-project |  Trivial | common | Samrat Deb | Samrat Deb |
| [YARN-9874](https://issues.apache.org/jira/browse/YARN-9874) | Remove unnecessary LevelDb write call in LeveldbConfigurationStore#confirmMutation |  Minor | capacityscheduler | Prabhu Joseph | Ashutosh Gupta |
| [HDFS-16256](https://issues.apache.org/jira/browse/HDFS-16256) | Minor fixes in HDFS Fedbalance document |  Minor | documentation | Akira Ajisaka | Ashutosh Gupta |
| [YARN-9822](https://issues.apache.org/jira/browse/YARN-9822) | TimelineCollectorWebService#putEntities blocked when ATSV2 HBase is down. |  Major | ATSv2 | Prabhu Joseph | Ashutosh Gupta |
| [YARN-10287](https://issues.apache.org/jira/browse/YARN-10287) | Update scheduler-conf corrupts the CS configuration when removing queue which is referred in queue mapping |  Major | capacity scheduler | Akhil PB | Ashutosh Gupta |
| [YARN-9403](https://issues.apache.org/jira/browse/YARN-9403) | GET /apps/{appid}/entities/YARN\_APPLICATION accesses application table instead of entity table |  Major | ATSv2 | Prabhu Joseph | Ashutosh Gupta |
| [HDFS-16283](https://issues.apache.org/jira/browse/HDFS-16283) | RBF: improve renewLease() to call only a specific NameNode rather than make fan-out calls |  Major | rbf | Aihua Xu | ZanderXu |
| [HADOOP-18231](https://issues.apache.org/jira/browse/HADOOP-18231) | tests in ITestS3AInputStreamPerformance are failing |  Minor | fs/s3 | Ahmar Suhail | Ahmar Suhail |
| [HADOOP-18254](https://issues.apache.org/jira/browse/HADOOP-18254) | Add in configuration option to enable prefetching |  Minor | fs/s3 | Ahmar Suhail | Ahmar Suhail |
| [YARN-11160](https://issues.apache.org/jira/browse/YARN-11160) | Support getResourceProfiles, getResourceProfile API's for Federation |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-8900](https://issues.apache.org/jira/browse/YARN-8900) | [Router] Federation: routing getContainers REST invocations transparently to multiple RMs |  Major | federation, router | Giovanni Matteo Fumarola | Shilun Fan |
| [HDFS-15079](https://issues.apache.org/jira/browse/HDFS-15079) | RBF: Client maybe get an unexpected result with network anomaly |  Critical | rbf | Hui Fei | ZanderXu |
| [YARN-11203](https://issues.apache.org/jira/browse/YARN-11203) | Fix typo in hadoop-yarn-server-router module |  Minor | federation | Shilun Fan | Shilun Fan |
| [YARN-11161](https://issues.apache.org/jira/browse/YARN-11161) | Support getAttributesToNodes, getClusterNodeAttributes, getNodesToAttributes API's for Federation |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-10883](https://issues.apache.org/jira/browse/YARN-10883) | [Router] Router Audit Log Add Client IP Address. |  Major | federation, router | chaosju | Shilun Fan |
| [HADOOP-18190](https://issues.apache.org/jira/browse/HADOOP-18190) | Collect IOStatistics during S3A prefetching |  Major | fs/s3 | Steve Loughran | Ahmar Suhail |
| [HADOOP-18344](https://issues.apache.org/jira/browse/HADOOP-18344) | AWS SDK update to 1.12.262 to address jackson  CVE-2018-7489 and AWS CVE-2022-31159 |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-11212](https://issues.apache.org/jira/browse/YARN-11212) | [Federation] Add getNodeToLabels REST APIs for Router |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11180](https://issues.apache.org/jira/browse/YARN-11180) | Refactor some code of getNewApplication, submitApplication, forceKillApplication, getApplicationReport |  Minor | federation | Shilun Fan | Shilun Fan |
| [YARN-11220](https://issues.apache.org/jira/browse/YARN-11220) | [Federation] Add getLabelsToNodes, getClusterNodeLabels, getLabelsOnNode REST APIs for Router |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-8973](https://issues.apache.org/jira/browse/YARN-8973) | [Router] Add missing methods in RMWebProtocol |  Major | federation, router | Giovanni Matteo Fumarola | Shilun Fan |
| [YARN-6972](https://issues.apache.org/jira/browse/YARN-6972) | Adding RM ClusterId in AppInfo |  Major | federation | Giovanni Matteo Fumarola | Tanuj Nayak |
| [YARN-11230](https://issues.apache.org/jira/browse/YARN-11230) | [Federation] Add getContainer, signalToContainer  REST APIs for Router |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11235](https://issues.apache.org/jira/browse/YARN-11235) | [RESERVATION] Refactor Policy Code and Define getReservationHomeSubcluster |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-10793](https://issues.apache.org/jira/browse/YARN-10793) | Upgrade Junit from 4 to 5 in hadoop-yarn-server-applicationhistoryservice |  Major | test | ANANDA G B | Ashutosh Gupta |
| [YARN-11227](https://issues.apache.org/jira/browse/YARN-11227) | [Federation] Add getAppTimeout, getAppTimeouts, updateApplicationTimeout REST APIs for Router |  Major | federation | Shilun Fan | Shilun Fan |
| [HDFS-13274](https://issues.apache.org/jira/browse/HDFS-13274) | RBF: Extend RouterRpcClient to use multiple sockets |  Major | rbf | Íñigo Goiri | Íñigo Goiri |
| [YARN-6539](https://issues.apache.org/jira/browse/YARN-6539) | Create SecureLogin inside Router |  Minor | federation, router | Giovanni Matteo Fumarola | Xie YiFan |
| [YARN-11148](https://issues.apache.org/jira/browse/YARN-11148) | In federation and security mode, nm recover may fail. |  Major | nodemanager | Chenyu Zheng | Chenyu Zheng |
| [YARN-11236](https://issues.apache.org/jira/browse/YARN-11236) | [RESERVATION] Implement FederationReservationHomeSubClusterStore With MemoryStore |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11223](https://issues.apache.org/jira/browse/YARN-11223) | [Federation] Add getAppPriority, updateApplicationPriority REST APIs for Router |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11224](https://issues.apache.org/jira/browse/YARN-11224) | [Federation] Add getAppQueue, updateAppQueue REST APIs for Router |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11252](https://issues.apache.org/jira/browse/YARN-11252) | [RESERVATION] Yarn Federation Router Supports Update / Delete Reservation in MemoryStore |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11269](https://issues.apache.org/jira/browse/YARN-11269) | Upgrade JUnit from 4 to 5 in hadoop-yarn-server-timeline-pluginstorage |  Major | test, yarn | Ashutosh Gupta | Ashutosh Gupta |
| [YARN-11250](https://issues.apache.org/jira/browse/YARN-11250) | Capture the Performance Metrics of ZookeeperFederationStateStore |  Major | federation | Shilun Fan | Shilun Fan |
| [HADOOP-18380](https://issues.apache.org/jira/browse/HADOOP-18380) | fs.s3a.prefetch.block.size to be read through longBytesOption |  Major | fs/s3 | Steve Loughran | Viraj Jasani |
| [YARN-11219](https://issues.apache.org/jira/browse/YARN-11219) | [Federation] Add getAppActivities, getAppStatistics REST APIs for Router |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-8482](https://issues.apache.org/jira/browse/YARN-8482) | [Router] Add cache for fast answers to getApps |  Major | federation, router | Giovanni Matteo Fumarola | Shilun Fan |
| [YARN-11275](https://issues.apache.org/jira/browse/YARN-11275) | [Federation] Add batchFinishApplicationMaster in UAMPoolManager |  Major | federation, nodemanager | Shilun Fan | Shilun Fan |
| [YARN-11245](https://issues.apache.org/jira/browse/YARN-11245) | Upgrade JUnit from 4 to 5 in hadoop-yarn-csi |  Major | yarn-csi | Ashutosh Gupta | Ashutosh Gupta |
| [YARN-11272](https://issues.apache.org/jira/browse/YARN-11272) | [RESERVATION] Federation StateStore: Support storage/retrieval of Reservations With Zk |  Major | federation, reservation system | Shilun Fan | Shilun Fan |
| [HADOOP-18339](https://issues.apache.org/jira/browse/HADOOP-18339) | S3A storage class option only picked up when buffering writes to disk |  Major | fs/s3 | Steve Loughran | Monthon Klongklaew |
| [YARN-11177](https://issues.apache.org/jira/browse/YARN-11177) | Support getNewReservation, submitReservation, updateReservation, deleteReservation API's for Federation |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-6667](https://issues.apache.org/jira/browse/YARN-6667) | Handle containerId duplicate without failing the heartbeat in Federation Interceptor |  Minor | federation, router | Botong Huang | Shilun Fan |
| [YARN-11284](https://issues.apache.org/jira/browse/YARN-11284) | [Federation] Improve UnmanagedAMPoolManager WithoutBlock ServiceStop |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11273](https://issues.apache.org/jira/browse/YARN-11273) | [RESERVATION] Federation StateStore: Support storage/retrieval of Reservations With SQL |  Major | federation | Shilun Fan | Shilun Fan |
| [HADOOP-18410](https://issues.apache.org/jira/browse/HADOOP-18410) | S3AInputStream.unbuffer() async drain not releasing http connections |  Blocker | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-11298](https://issues.apache.org/jira/browse/YARN-11298) | Improve Yarn Router Junit Test Close MockRM |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11297](https://issues.apache.org/jira/browse/YARN-11297) | Improve Yarn Router Reservation Submission Code |  Major | federation | Shilun Fan | Shilun Fan |
| [HDFS-13522](https://issues.apache.org/jira/browse/HDFS-13522) | HDFS-13522: Add federated nameservices states to client protocol and propagate it between routers and clients. |  Major | federation, namenode | Erik Krogen | Simbarashe Dzinamarira |
| [YARN-11265](https://issues.apache.org/jira/browse/YARN-11265) | Upgrade JUnit from 4 to 5 in hadoop-yarn-server-sharedcachemanager |  Major | test, yarn | Ashutosh Gupta | Ashutosh Gupta |
| [HADOOP-18302](https://issues.apache.org/jira/browse/HADOOP-18302) | Remove WhiteBox in hadoop-common module. |  Minor | common | Shilun Fan | Shilun Fan |
| [YARN-11261](https://issues.apache.org/jira/browse/YARN-11261) | Upgrade JUnit from 4 to 5 in hadoop-yarn-server-web-proxy |  Major | test, yarn | Ashutosh Gupta | Ashutosh Gupta |
| [HDFS-16767](https://issues.apache.org/jira/browse/HDFS-16767) | RBF: Support observer node from Router-Based Federation |  Major | rbf | Simbarashe Dzinamarira | Simbarashe Dzinamarira |
| [HADOOP-18186](https://issues.apache.org/jira/browse/HADOOP-18186) | s3a prefetching to use SemaphoredDelegatingExecutor for submitting work |  Major | fs/s3 | Steve Loughran | Viraj Jasani |
| [YARN-11293](https://issues.apache.org/jira/browse/YARN-11293) | [Federation] Router Support DelegationToken storeNewMasterKey/removeStoredMasterKey With MemoryStateStore |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11283](https://issues.apache.org/jira/browse/YARN-11283) | [Federation] Fix Typo of NodeManager AMRMProxy. |  Minor | federation, nodemanager | Shilun Fan | Shilun Fan |
| [HADOOP-18377](https://issues.apache.org/jira/browse/HADOOP-18377) | hadoop-aws maven build to add a prefetch profile to run all tests with prefetching |  Major | fs/s3, test | Steve Loughran | Viraj Jasani |
| [YARN-11307](https://issues.apache.org/jira/browse/YARN-11307) | Fix Yarn Router Broken Link |  Minor | federation, router | Shilun Fan | Shilun Fan |
| [HADOOP-18455](https://issues.apache.org/jira/browse/HADOOP-18455) | s3a prefetching Executor should be closed |  Major | fs/s3 | Viraj Jasani | Viraj Jasani |
| [YARN-11270](https://issues.apache.org/jira/browse/YARN-11270) | Upgrade JUnit from 4 to 5 in hadoop-yarn-server-timelineservice-hbase-client |  Major | test, yarn | Ashutosh Gupta | Ashutosh Gupta |
| [YARN-11271](https://issues.apache.org/jira/browse/YARN-11271) | Upgrade JUnit from 4 to 5 in hadoop-yarn-server-timelineservice-hbase-common |  Major | test, yarn | Ashutosh Gupta | Ashutosh Gupta |
| [YARN-11316](https://issues.apache.org/jira/browse/YARN-11316) | [Federation] Fix Yarn federation.md table format |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11308](https://issues.apache.org/jira/browse/YARN-11308) | Router Page display the db username and password in mask mode |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11310](https://issues.apache.org/jira/browse/YARN-11310) | [Federation] Refactoring Router's Federation Web Page |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11238](https://issues.apache.org/jira/browse/YARN-11238) | Optimizing FederationClientInterceptor Call with Parallelism |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11318](https://issues.apache.org/jira/browse/YARN-11318) | Improve FederationInterceptorREST#createInterceptorForSubCluster Use WebAppUtils |  Major | federation, router | Shilun Fan | Shilun Fan |
| [YARN-11324](https://issues.apache.org/jira/browse/YARN-11324) | [Federation] Fix some PBImpl classes to avoid NPE. |  Major | federation, router, yarn | Shilun Fan | Shilun Fan |
| [HADOOP-18382](https://issues.apache.org/jira/browse/HADOOP-18382) | Upgrade AWS SDK to V2 - Prerequisites |  Minor | fs/s3 | Ahmar Suhail | Ahmar Suhail |
| [YARN-11313](https://issues.apache.org/jira/browse/YARN-11313) | [Federation] Add SQLServer Script and Supported DB Version in Federation.md |  Major | federation | Shilun Fan | Shilun Fan |
| [HADOOP-18378](https://issues.apache.org/jira/browse/HADOOP-18378) | Implement readFully(long position, byte[] buffer, int offset, int length) |  Minor | fs/s3 | Ahmar Suhail | Alessandro Passaro |
| [YARN-11260](https://issues.apache.org/jira/browse/YARN-11260) | Upgrade JUnit from 4 to 5 in hadoop-yarn-server-timelineservice |  Major | test, yarn | Ashutosh Gupta | Ashutosh Gupta |
| [HDFS-16783](https://issues.apache.org/jira/browse/HDFS-16783) | Remove the redundant lock in deepCopyReplica and getFinalizedBlocks |  Major | datanode | ZanderXu | ZanderXu |
| [HDFS-16787](https://issues.apache.org/jira/browse/HDFS-16787) | Remove the redundant lock in DataSetLockManager#removeLock. |  Major | datanode | ZanderXu | ZanderXu |
| [HADOOP-18480](https://issues.apache.org/jira/browse/HADOOP-18480) | upgrade  AWS SDK to 1.12.316 |  Major | build, fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-11315](https://issues.apache.org/jira/browse/YARN-11315) | [Federation] YARN Federation Router Supports Cross-Origin. |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11317](https://issues.apache.org/jira/browse/YARN-11317) | [Federation] Refactoring Yarn Router's About Web Page. |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11334](https://issues.apache.org/jira/browse/YARN-11334) | [Federation] Improve SubClusterState#fromString parameter and LogMessage |  Trivial | federation | Shilun Fan | Shilun Fan |
| [YARN-11327](https://issues.apache.org/jira/browse/YARN-11327) | [Federation] Refactoring Yarn Router's Node Web Page. |  Major | federation, router | Shilun Fan | Shilun Fan |
| [YARN-11294](https://issues.apache.org/jira/browse/YARN-11294) | [Federation] Router Support DelegationToken storeNewToken/updateStoredToken/removeStoredToken With MemoryStateStore |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-8041](https://issues.apache.org/jira/browse/YARN-8041) | [Router] Federation: Improve Router REST API Metrics |  Minor | federation, router | YR | Shilun Fan |
| [YARN-11247](https://issues.apache.org/jira/browse/YARN-11247) | Remove unused classes introduced by YARN-9615 |  Minor | resourcemanager | Shilun Fan | Shilun Fan |
| [HADOOP-18304](https://issues.apache.org/jira/browse/HADOOP-18304) | Improve S3A committers documentation clarity |  Trivial | documentation | Daniel Carl Jones | Daniel Carl Jones |
| [HADOOP-18189](https://issues.apache.org/jira/browse/HADOOP-18189) | S3PrefetchingInputStream to support status probes when closed |  Minor | fs/s3 | Steve Loughran | Viraj Jasani |
| [HADOOP-18465](https://issues.apache.org/jira/browse/HADOOP-18465) | S3A server-side encryption tests fail before checking encryption tests should skip |  Minor | fs/s3, test | Daniel Carl Jones | Daniel Carl Jones |
| [YARN-11342](https://issues.apache.org/jira/browse/YARN-11342) | [Federation] Refactor FederationClientInterceptor#submitApplication Use FederationActionRetry |  Major | federation, router | Shilun Fan | Shilun Fan |
| [YARN-11295](https://issues.apache.org/jira/browse/YARN-11295) | [Federation] Router Support DelegationToken in MemoryStore mode |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11345](https://issues.apache.org/jira/browse/YARN-11345) | [Federation] Refactoring Yarn Router's Application Web Page. |  Major | federation, router | Shilun Fan | Shilun Fan |
| [YARN-11336](https://issues.apache.org/jira/browse/YARN-11336) | Upgrade Junit 4 to 5 in hadoop-yarn-applications-catalog-webapp |  Major | test | Ashutosh Gupta | Ashutosh Gupta |
| [YARN-11338](https://issues.apache.org/jira/browse/YARN-11338) | Upgrade Junit 4 to 5 in hadoop-yarn-applications-unmanaged-am-launcher |  Major | test | Ashutosh Gupta | Ashutosh Gupta |
| [YARN-11229](https://issues.apache.org/jira/browse/YARN-11229) | [Federation] Add checkUserAccessToQueue REST APIs for Router |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11332](https://issues.apache.org/jira/browse/YARN-11332) | [Federation] Improve FederationClientInterceptor#ThreadPool thread pool configuration. |  Major | federation, router | Shilun Fan | Shilun Fan |
| [YARN-11337](https://issues.apache.org/jira/browse/YARN-11337) | Upgrade Junit 4 to 5 in hadoop-yarn-applications-mawo |  Major | test | Ashutosh Gupta | Ashutosh Gupta |
| [YARN-11339](https://issues.apache.org/jira/browse/YARN-11339) | Upgrade Junit 4 to 5 in hadoop-yarn-services-api |  Major | test | Ashutosh Gupta | Ashutosh Gupta |
| [YARN-11264](https://issues.apache.org/jira/browse/YARN-11264) | Upgrade JUnit from 4 to 5 in hadoop-yarn-server-tests |  Major | test, yarn | Ashutosh Gupta | Ashutosh Gupta |
| [HADOOP-18482](https://issues.apache.org/jira/browse/HADOOP-18482) | ITestS3APrefetchingInputStream does not skip if no CSV test file available |  Minor | fs/s3 | Daniel Carl Jones | Daniel Carl Jones |
| [YARN-11366](https://issues.apache.org/jira/browse/YARN-11366) | Improve equals, hashCode(), toString() methods of the Federation Base Object |  Major | federation, router | Shilun Fan | Shilun Fan |
| [YARN-11354](https://issues.apache.org/jira/browse/YARN-11354) | [Federation] Add Yarn Router's NodeLabel Web Page. |  Major | federation, router | Shilun Fan | Shilun Fan |
| [YARN-11368](https://issues.apache.org/jira/browse/YARN-11368) | [Federation] Improve Yarn Router's Federation Page style. |  Major | federation, router | Shilun Fan | Shilun Fan |
| [HADOOP-15327](https://issues.apache.org/jira/browse/HADOOP-15327) | Upgrade MR ShuffleHandler to use Netty4 |  Major | common | Xiaoyu Yao | Szilard Nemeth |
| [HDFS-16785](https://issues.apache.org/jira/browse/HDFS-16785) | Avoid to hold write lock to improve performance when add volume. |  Major | datanode | ZanderXu | ZanderXu |
| [YARN-11359](https://issues.apache.org/jira/browse/YARN-11359) | [Federation] Routing admin invocations transparently to multiple RMs. |  Major | federation, router | Shilun Fan | Shilun Fan |
| [MAPREDUCE-7422](https://issues.apache.org/jira/browse/MAPREDUCE-7422) | Upgrade Junit 4 to 5 in hadoop-mapreduce-examples |  Major | test | Ashutosh Gupta | Ashutosh Gupta |
| [YARN-6946](https://issues.apache.org/jira/browse/YARN-6946) | Upgrade JUnit from 4 to 5 in hadoop-yarn-common |  Major | test | Akira Ajisaka | Ashutosh Gupta |
| [YARN-11371](https://issues.apache.org/jira/browse/YARN-11371) | [Federation] Refactor FederationInterceptorREST#createNewApplication\\submitApplication Use FederationActionRetry |  Major | federation | Shilun Fan | Shilun Fan |
| [HADOOP-18531](https://issues.apache.org/jira/browse/HADOOP-18531) | assertion failure in ITestS3APrefetchingInputStream |  Major | fs/s3, test | Steve Loughran | Ashutosh Gupta |
| [HADOOP-18457](https://issues.apache.org/jira/browse/HADOOP-18457) | ABFS: Support for account level throttling |  Major | fs/azure | Anmol Asrani | Anmol Asrani |
| [YARN-10946](https://issues.apache.org/jira/browse/YARN-10946) | AbstractCSQueue: Create separate class for constructing Queue API objects |  Minor | capacity scheduler | Szilard Nemeth | Peter Szucs |
| [YARN-11158](https://issues.apache.org/jira/browse/YARN-11158) | Support getDelegationToken, renewDelegationToken, cancelDelegationToken API's for Federation |  Major | federation | Shilun Fan | Shilun Fan |
| [HADOOP-18560](https://issues.apache.org/jira/browse/HADOOP-18560) | AvroFSInput opens a stream twice and discards the second one without closing |  Blocker | fs | Steve Loughran | Steve Loughran |
| [YARN-11373](https://issues.apache.org/jira/browse/YARN-11373) | [Federation] Support refreshQueues、refreshNodes API's for Federation. |  Major | federation, router | Shilun Fan | Shilun Fan |
| [YARN-11350](https://issues.apache.org/jira/browse/YARN-11350) | [Federation] Router Support DelegationToken With ZK |  Major | federation, router | Shilun Fan | Shilun Fan |
| [YARN-11358](https://issues.apache.org/jira/browse/YARN-11358) | [Federation] Add FederationInterceptor#allow-partial-result config. |  Major | federation, router | Shilun Fan | Shilun Fan |
| [HADOOP-18526](https://issues.apache.org/jira/browse/HADOOP-18526) | Leak of S3AInstrumentation instances via hadoop Metrics references |  Blocker | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-18546](https://issues.apache.org/jira/browse/HADOOP-18546) | disable purging list of in progress reads in abfs stream closed |  Blocker | fs/azure | Steve Loughran | Pranav Saxena |
| [HADOOP-18577](https://issues.apache.org/jira/browse/HADOOP-18577) | ABFS: add probes of readahead fix |  Major | fs/azure | Steve Loughran | Steve Loughran |
| [YARN-11226](https://issues.apache.org/jira/browse/YARN-11226) | [Federation] Add createNewReservation, submitReservation, updateReservation, deleteReservation REST APIs for Router |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11225](https://issues.apache.org/jira/browse/YARN-11225) | [Federation] Add postDelegationToken, postDelegationTokenExpiration, cancelDelegationToken  REST APIs for Router |  Major | federation | Shilun Fan | Shilun Fan |
| [HADOOP-18320](https://issues.apache.org/jira/browse/HADOOP-18320) | Improve S3A delegations token documentation |  Minor | fs/s3 | Ahmar Suhail | Ahmar Suhail |
| [YARN-11374](https://issues.apache.org/jira/browse/YARN-11374) | [Federation] Support refreshSuperUserGroupsConfiguration、refreshUserToGroupsMappings API's for Federation |  Major | federation, router | Shilun Fan | Shilun Fan |
| [MAPREDUCE-7417](https://issues.apache.org/jira/browse/MAPREDUCE-7417) | Upgrade Junit 4 to 5 in hadoop-mapreduce-client-uploader |  Major | test | Ashutosh Gupta | Ashutosh Gupta |
| [MAPREDUCE-7413](https://issues.apache.org/jira/browse/MAPREDUCE-7413) | Upgrade Junit 4 to 5 in hadoop-mapreduce-client-hs-plugins |  Major | test | Ashutosh Gupta | Ashutosh Gupta |
| [YARN-11320](https://issues.apache.org/jira/browse/YARN-11320) | [Federation] Add getSchedulerInfo REST APIs for Router |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-6971](https://issues.apache.org/jira/browse/YARN-6971) | Clean up different ways to create resources |  Minor | resourcemanager, scheduler | Yufei Gu | Riya Khandelwal |
| [YARN-10965](https://issues.apache.org/jira/browse/YARN-10965) | Centralize queue resource calculation based on CapacityVectors |  Major | capacity scheduler | Andras Gyori | Andras Gyori |
| [YARN-11218](https://issues.apache.org/jira/browse/YARN-11218) | [Federation] Add getActivities, getBulkActivities REST APIs for Router |  Major | federation | Shilun Fan | Shilun Fan |
| [HADOOP-18246](https://issues.apache.org/jira/browse/HADOOP-18246) | Remove lower limit on s3a prefetching/caching block size |  Minor | fs/s3 | Daniel Carl Jones | Ankit Saurabh |
| [YARN-11217](https://issues.apache.org/jira/browse/YARN-11217) | [Federation] Add dumpSchedulerLogs REST APIs for Router |  Major | federation | Shilun Fan | Shilun Fan |
| [HADOOP-18620](https://issues.apache.org/jira/browse/HADOOP-18620) | Avoid using grizzly-http-\* APIs |  Major | common | Viraj Jasani | Viraj Jasani |
| [HADOOP-18206](https://issues.apache.org/jira/browse/HADOOP-18206) | Cleanup the commons-logging references in the code base |  Major | common | Duo Zhang | Viraj Jasani |
| [HADOOP-18630](https://issues.apache.org/jira/browse/HADOOP-18630) | Add gh-pages in asf.yaml to deploy the current trunk doc |  Major | common | Ayush Saxena | Simhadri Govindappa |
| [YARN-3657](https://issues.apache.org/jira/browse/YARN-3657) | Federation maintenance mechanisms (simple CLI and command propagation) |  Major | nodemanager, resourcemanager | Carlo Curino | Shilun Fan |
| [YARN-6572](https://issues.apache.org/jira/browse/YARN-6572) | Refactoring Router services to use common util classes for pipeline creations |  Major | federation | Giovanni Matteo Fumarola | Shilun Fan |
| [HADOOP-18351](https://issues.apache.org/jira/browse/HADOOP-18351) | S3A prefetching: Error logging during reads |  Minor | fs/s3 | Ahmar Suhail | Ankit Saurabh |
| [YARN-11349](https://issues.apache.org/jira/browse/YARN-11349) | [Federation] Router Support DelegationToken With SQL |  Major | federation, router | Shilun Fan | Shilun Fan |
| [YARN-11228](https://issues.apache.org/jira/browse/YARN-11228) | [Federation] Add getAppAttempts, getAppAttempt REST APIs for Router |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-5604](https://issues.apache.org/jira/browse/YARN-5604) | Add versioning for FederationStateStore |  Major | federation, router | Subramaniam Krishnan | Shilun Fan |
| [YARN-11222](https://issues.apache.org/jira/browse/YARN-11222) | [Federation] Add addToClusterNodeLabels, removeFromClusterNodeLabels REST APIs for Router |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11289](https://issues.apache.org/jira/browse/YARN-11289) | [Federation] Improve NM FederationInterceptor removeAppFromRegistry |  Major | federation, nodemanager | Shilun Fan | Shilun Fan |
| [YARN-11221](https://issues.apache.org/jira/browse/YARN-11221) | [Federation] Add replaceLabelsOnNodes, replaceLabelsOnNode REST APIs for Router |  Major | federation | Shilun Fan | Shilun Fan |
| [HADOOP-18642](https://issues.apache.org/jira/browse/HADOOP-18642) | Cut excess dependencies from hadoop-azure, hadoop-aliyun transitive imports; fix LICENSE-binary |  Blocker | build, fs/azure, fs/oss | Steve Loughran | Steve Loughran |
| [YARN-11375](https://issues.apache.org/jira/browse/YARN-11375) | [Federation] Support refreshAdminAcls、refreshServiceAcls API's for Federation |  Major | federation, router | Shilun Fan | Shilun Fan |
| [HADOOP-18648](https://issues.apache.org/jira/browse/HADOOP-18648) | Avoid loading kms log4j properties dynamically by KMSWebServer |  Major | kms | Viraj Jasani | Viraj Jasani |
| [YARN-8972](https://issues.apache.org/jira/browse/YARN-8972) | [Router] Add support to prevent DoS attack over ApplicationSubmissionContext size |  Major | federation, router | Giovanni Matteo Fumarola | Shilun Fan |
| [HADOOP-18653](https://issues.apache.org/jira/browse/HADOOP-18653) | LogLevel servlet to determine log impl before using setLevel |  Major | common | Viraj Jasani | Viraj Jasani |
| [HADOOP-18649](https://issues.apache.org/jira/browse/HADOOP-18649) | CLA and CRLA appenders to be replaced with RFA |  Major | common | Viraj Jasani | Viraj Jasani |
| [HADOOP-18654](https://issues.apache.org/jira/browse/HADOOP-18654) | Remove unused custom appender TaskLogAppender |  Major | common | Viraj Jasani | Viraj Jasani |
| [YARN-11445](https://issues.apache.org/jira/browse/YARN-11445) | [Federation] Add getClusterInfo, getClusterUserInfo REST APIs for Router. |  Major | federation | Shilun Fan | Shilun Fan |
| [HADOOP-18631](https://issues.apache.org/jira/browse/HADOOP-18631) | Migrate Async appenders to log4j properties |  Major | common | Viraj Jasani | Viraj Jasani |
| [HADOOP-18669](https://issues.apache.org/jira/browse/HADOOP-18669) | Remove Log4Json Layout |  Major | common | Viraj Jasani | Viraj Jasani |
| [YARN-11376](https://issues.apache.org/jira/browse/YARN-11376) | [Federation] Support updateNodeResource、refreshNodesResources API's for Federation. |  Major | federation, router | Shilun Fan | Shilun Fan |
| [HADOOP-18606](https://issues.apache.org/jira/browse/HADOOP-18606) | Add reason in in x-ms-client-request-id on a retry API call. |  Minor | fs/azure | Pranav Saxena | Pranav Saxena |
| [HADOOP-18146](https://issues.apache.org/jira/browse/HADOOP-18146) | ABFS: Add changes for expect hundred continue header with append requests |  Major | fs/azure | Anmol Asrani | Anmol Asrani |
| [YARN-11446](https://issues.apache.org/jira/browse/YARN-11446) | [Federation] Add updateSchedulerConfiguration, getSchedulerConfiguration REST APIs for Router. |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11442](https://issues.apache.org/jira/browse/YARN-11442) | Refactor FederationInterceptorREST Code |  Major | federation, router | Shilun Fan | Shilun Fan |
| [HADOOP-18647](https://issues.apache.org/jira/browse/HADOOP-18647) | x-ms-client-request-id to have some way that identifies retry of an API. |  Minor | fs/azure | Pranav Saxena | Pranav Saxena |
| [HADOOP-18012](https://issues.apache.org/jira/browse/HADOOP-18012) | ABFS: Enable config controlled ETag check for Rename idempotency |  Major | fs/azure | Sneha Vijayarajan | Sree Bhattacharyya |
| [YARN-11377](https://issues.apache.org/jira/browse/YARN-11377) | [Federation] Support addToClusterNodeLabels、removeFromClusterNodeLabels、replaceLabelsOnNode API's for Federation |  Major | federation, router | Shilun Fan | Shilun Fan |
| [YARN-10846](https://issues.apache.org/jira/browse/YARN-10846) | Add dispatcher metrics to NM |  Major | nodemanager | chaosju | Shilun Fan |
| [YARN-11239](https://issues.apache.org/jira/browse/YARN-11239) | Optimize FederationClientInterceptor audit log |  Major | federation | Shilun Fan | Shilun Fan |
| [HADOOP-18399](https://issues.apache.org/jira/browse/HADOOP-18399) | S3A Prefetch - SingleFilePerBlockCache to use LocalDirAllocator |  Major | fs/s3 | Steve Loughran | Viraj Jasani |
| [YARN-11378](https://issues.apache.org/jira/browse/YARN-11378) | [Federation] Support checkForDecommissioningNodes、refreshClusterMaxPriority API's for Federation |  Major | federation, router | Shilun Fan | Shilun Fan |
| [YARN-11379](https://issues.apache.org/jira/browse/YARN-11379) | [Federation] Support mapAttributesToNodes、getGroupsForUser API's for Federation |  Major | federation, router | Shilun Fan | Shilun Fan |
| [YARN-9049](https://issues.apache.org/jira/browse/YARN-9049) | Add application submit data to state store |  Major | federation | Bibin Chundatt | Shilun Fan |
| [YARN-11079](https://issues.apache.org/jira/browse/YARN-11079) | Make an AbstractParentQueue to store common ParentQueue and ManagedParentQueue functionality |  Major | capacity scheduler | Benjamin Teke | Susheel Gupta |
| [YARN-11340](https://issues.apache.org/jira/browse/YARN-11340) | [Federation] Improve SQLFederationStateStore DataSource Config |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11424](https://issues.apache.org/jira/browse/YARN-11424) | [Federation] Router AdminCLI Supports DeregisterSubCluster. |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-6740](https://issues.apache.org/jira/browse/YARN-6740) | Federation Router (hiding multiple RMs for ApplicationClientProtocol) phase 2 |  Major | federation, router | Giovanni Matteo Fumarola | Shilun Fan |
| [HADOOP-18688](https://issues.apache.org/jira/browse/HADOOP-18688) | S3A audit header to include count of items in delete ops |  Major | fs/s3 | Steve Loughran | Viraj Jasani |
| [YARN-11493](https://issues.apache.org/jira/browse/YARN-11493) | [Federation] ConfiguredRMFailoverProxyProvider Supports Randomly Select an Router. |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-8898](https://issues.apache.org/jira/browse/YARN-8898) | Fix FederationInterceptor#allocate to set application priority in allocateResponse |  Major | federation | Bibin Chundatt | Shilun Fan |
| [MAPREDUCE-7419](https://issues.apache.org/jira/browse/MAPREDUCE-7419) | Upgrade Junit 4 to 5 in hadoop-mapreduce-client-common |  Major | test | Ashutosh Gupta | Ashutosh Gupta |
| [YARN-11478](https://issues.apache.org/jira/browse/YARN-11478) | [Federation] SQLFederationStateStore Support Store ApplicationSubmitData |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-7720](https://issues.apache.org/jira/browse/YARN-7720) | Race condition between second app attempt and UAM timeout when first attempt node is down |  Major | federation | Botong Huang | Shilun Fan |
| [YARN-11492](https://issues.apache.org/jira/browse/YARN-11492) | Improve createJerseyClient#setConnectTimeout Code |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11500](https://issues.apache.org/jira/browse/YARN-11500) | Fix typos in hadoop-yarn-server-common#federation |  Minor | federation | Shilun Fan | Shilun Fan |
| [HADOOP-18752](https://issues.apache.org/jira/browse/HADOOP-18752) | Change fs.s3a.directory.marker.retention to "keep" |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-11502](https://issues.apache.org/jira/browse/YARN-11502) | Refactor AMRMProxy#FederationInterceptor#registerApplicationMaster |  Major | amrmproxy, federation, nodemanager | Shilun Fan | Shilun Fan |
| [YARN-11505](https://issues.apache.org/jira/browse/YARN-11505) | [Federation] Add Steps To Set up a Test Cluster. |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11516](https://issues.apache.org/jira/browse/YARN-11516) | Improve existsApplicationHomeSubCluster/existsReservationHomeSubCluster Log Level |  Minor | federation, router | Shilun Fan | Shilun Fan |
| [YARN-11510](https://issues.apache.org/jira/browse/YARN-11510) | [Federation] Fix NodeManager#TestFederationInterceptor Flaky Unit Test |  Major | federation, nodemanager | Shilun Fan | Shilun Fan |
| [YARN-11517](https://issues.apache.org/jira/browse/YARN-11517) | Improve Federation#RouterCLI deregisterSubCluster Code |  Major | federation, router | Shilun Fan | Shilun Fan |
| [HADOOP-18756](https://issues.apache.org/jira/browse/HADOOP-18756) | CachingBlockManager to use AtomicBoolean for closed flag |  Major | fs/s3 | Steve Loughran | Viraj Jasani |
| [YARN-11519](https://issues.apache.org/jira/browse/YARN-11519) | [Federation] Add RouterAuditLog to log4j.properties |  Major | router | Shilun Fan | Shilun Fan |
| [YARN-11090](https://issues.apache.org/jira/browse/YARN-11090) | [GPG] Support Secure Mode |  Major | gpg | tuyu | Shilun Fan |
| [YARN-11000](https://issues.apache.org/jira/browse/YARN-11000) | Replace queue resource calculation logic in updateClusterResource |  Major | capacity scheduler | Andras Gyori | Andras Gyori |
| [YARN-11524](https://issues.apache.org/jira/browse/YARN-11524) | Improve the Policy Description in Federation.md |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11509](https://issues.apache.org/jira/browse/YARN-11509) | The FederationInterceptor#launchUAM Added retry logic. |  Minor | amrmproxy | Shilun Fan | Shilun Fan |
| [YARN-11515](https://issues.apache.org/jira/browse/YARN-11515) | [Federation] Improve DefaultRequestInterceptor#init Code |  Minor | federation, router | Shilun Fan | Shilun Fan |
| [YARN-11531](https://issues.apache.org/jira/browse/YARN-11531) | [Federation] Code cleanup for NodeManager#amrmproxy |  Minor | federation | Shilun Fan | Shilun Fan |
| [YARN-11525](https://issues.apache.org/jira/browse/YARN-11525) | [Federation] Router CLI Supports Save the SubClusterPolicyConfiguration Of Queues. |  Major | federation, router | Shilun Fan | Shilun Fan |
| [YARN-11533](https://issues.apache.org/jira/browse/YARN-11533) | CapacityScheduler CapacityConfigType changed in legacy queue allocation mode |  Major | capacity scheduler | Benjamin Teke | Benjamin Teke |
| [HADOOP-18795](https://issues.apache.org/jira/browse/HADOOP-18795) | s3a DelegationToken plugin to expand return type of deploy/binding |  Minor | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-11521](https://issues.apache.org/jira/browse/YARN-11521) | Create a test set that runs with both Legacy/Uniform queue calculation |  Major | capacityscheduler | Tamas Domok | Tamas Domok |
| [YARN-11508](https://issues.apache.org/jira/browse/YARN-11508) | [Minor] Improve UnmanagedAMPoolManager/UnmanagedApplicationManager Code |  Minor | federation | Shilun Fan | Shilun Fan |
| [YARN-11520](https://issues.apache.org/jira/browse/YARN-11520) | Support capacity vector for AQCv2 dynamic templates |  Major | capacityscheduler | Tamas Domok | Benjamin Teke |
| [YARN-11543](https://issues.apache.org/jira/browse/YARN-11543) | Fix checkstyle issues after YARN-11520 |  Major | capacity scheduler | Benjamin Teke | Benjamin Teke |
| [HADOOP-18183](https://issues.apache.org/jira/browse/HADOOP-18183) | s3a audit logs to publish range start/end of GET requests in audit header |  Minor | fs/s3 | Steve Loughran | Ankit Saurabh |
| [YARN-11536](https://issues.apache.org/jira/browse/YARN-11536) | [Federation] Router CLI Supports Batch Save the SubClusterPolicyConfiguration Of Queues. |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11153](https://issues.apache.org/jira/browse/YARN-11153) | Make proxy server support YARN federation. |  Major | yarn | Chenyu Zheng | Chenyu Zheng |
| [YARN-10201](https://issues.apache.org/jira/browse/YARN-10201) | Make AMRMProxyPolicy aware of SC load |  Major | amrmproxy | Young Chen | Shilun Fan |
| [HADOOP-18832](https://issues.apache.org/jira/browse/HADOOP-18832) | Upgrade aws-java-sdk to 1.12.499+ |  Major | fs/s3 | Viraj Jasani | Viraj Jasani |
| [YARN-11154](https://issues.apache.org/jira/browse/YARN-11154) | Make router support proxy server. |  Major | yarn | Chenyu Zheng | Chenyu Zheng |
| [YARN-10218](https://issues.apache.org/jira/browse/YARN-10218) | [GPG] Support HTTPS in GPG |  Major | federation | Bilwa S T | Shilun Fan |
| [HADOOP-18820](https://issues.apache.org/jira/browse/HADOOP-18820) | AWS SDK v2: make the v1 bridging support optional |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-18853](https://issues.apache.org/jira/browse/HADOOP-18853) | AWS SDK V2 - Upgrade SDK to 2.20.28 and restores multipart copy |  Major | fs/s3 | Ahmar Suhail | Ahmar Suhail |
| [YARN-6537](https://issues.apache.org/jira/browse/YARN-6537) | Running RM tests against the Router |  Minor | federation, resourcemanager | Giovanni Matteo Fumarola | Shilun Fan |
| [YARN-11435](https://issues.apache.org/jira/browse/YARN-11435) | [Router] FederationStateStoreFacade is not reinitialized with Router conf |  Major | federation, router, yarn | Aparajita Choudhary | Shilun Fan |
| [YARN-11537](https://issues.apache.org/jira/browse/YARN-11537) | [Federation] Router CLI Supports List SubClusterPolicyConfiguration Of Queues. |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11434](https://issues.apache.org/jira/browse/YARN-11434) | [Router] UGI conf doesn't read user overridden configurations on Router startup |  Major | federation, router, yarn | Aparajita Choudhary | Shilun Fan |
| [YARN-8980](https://issues.apache.org/jira/browse/YARN-8980) | Mapreduce application container start  fail after AM restart. |  Major | federation | Bibin Chundatt | Chenyu Zheng |
| [YARN-6476](https://issues.apache.org/jira/browse/YARN-6476) | Advanced Federation UI based on YARN UI v2 |  Major | yarn, yarn-ui-v2 | Carlo Curino | Shilun Fan |
| [HADOOP-18863](https://issues.apache.org/jira/browse/HADOOP-18863) | AWS SDK V2 - AuditFailureExceptions aren't being translated properly |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-18818](https://issues.apache.org/jira/browse/HADOOP-18818) | Merge aws v2 upgrade feature branch into trunk |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-11562](https://issues.apache.org/jira/browse/YARN-11562) | [Federation] GPG Support Query Policies In Web. |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11433](https://issues.apache.org/jira/browse/YARN-11433) | Router's main() should support generic options |  Major | federation, router, yarn | Aparajita Choudhary | Aparajita Choudhary |
| [HADOOP-18888](https://issues.apache.org/jira/browse/HADOOP-18888) | S3A. createS3AsyncClient() always enables multipart |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-18906](https://issues.apache.org/jira/browse/HADOOP-18906) | Increase default batch size of ZKDTSM token seqnum to reduce overflow speed of zonde dataVersion. |  Major | security | Xiaoqiao He | Xiaoqiao He |
| [YARN-11570](https://issues.apache.org/jira/browse/YARN-11570) | Add YARN\_GLOBALPOLICYGENERATOR\_HEAPSIZE to yarn-env for GPG |  Minor | federation | Shilun Fan | Shilun Fan |
| [YARN-11547](https://issues.apache.org/jira/browse/YARN-11547) | [Federation] Router Supports Remove individual application records from FederationStateStore. |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11580](https://issues.apache.org/jira/browse/YARN-11580) | YARN Router Web supports displaying information for Non-Federation. |  Major | federation, router | Shilun Fan | Shilun Fan |
| [YARN-11579](https://issues.apache.org/jira/browse/YARN-11579) | Fix 'Physical Mem Used' and 'Physical VCores Used' are not displaying data |  Major | federation, router | Shilun Fan | Shilun Fan |
| [HADOOP-18876](https://issues.apache.org/jira/browse/HADOOP-18876) | ABFS: Change default from disk to bytebuffer for fs.azure.data.blocks.buffer |  Major | build | Anmol Asrani | Anmol Asrani |
| [HADOOP-18861](https://issues.apache.org/jira/browse/HADOOP-18861) | ABFS: Fix failing CPK tests on trunk |  Minor | build | Anmol Asrani | Anmol Asrani |
| [YARN-9048](https://issues.apache.org/jira/browse/YARN-9048) | Add znode hierarchy in Federation ZK State Store |  Major | federation | Bibin Chundatt | Shilun Fan |
| [YARN-11588](https://issues.apache.org/jira/browse/YARN-11588) | Fix uncleaned threads in YARN Federation interceptor threadpool |  Major | federation, router | Jeffrey Chang | Jeffrey Chang |
| [HADOOP-18889](https://issues.apache.org/jira/browse/HADOOP-18889) | S3A: V2 SDK client does not work with third-party store |  Critical | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-18857](https://issues.apache.org/jira/browse/HADOOP-18857) | AWS v2 SDK: fail meaningfully if legacy v2 signing requested |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-18927](https://issues.apache.org/jira/browse/HADOOP-18927) | S3ARetryHandler to treat SocketExceptions as connectivity failures |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-11571](https://issues.apache.org/jira/browse/YARN-11571) | [GPG] Add Information About YARN GPG in Federation.md |  Major | federation | Shilun Fan | Shilun Fan |
| [HADOOP-18908](https://issues.apache.org/jira/browse/HADOOP-18908) | Improve s3a region handling, including determining from endpoint |  Major | fs/s3 | Steve Loughran | Ahmar Suhail |
| [HADOOP-18829](https://issues.apache.org/jira/browse/HADOOP-18829) | s3a prefetch LRU cache eviction metric |  Major | fs/s3 | Viraj Jasani | Viraj Jasani |
| [HADOOP-18946](https://issues.apache.org/jira/browse/HADOOP-18946) | S3A: testMultiObjectExceptionFilledIn() assertion error |  Minor | fs/s3, test | Steve Loughran | Steve Loughran |
| [HADOOP-18945](https://issues.apache.org/jira/browse/HADOOP-18945) | S3A: IAMInstanceCredentialsProvider failing: Failed to load credentials from IMDS |  Blocker | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-18939](https://issues.apache.org/jira/browse/HADOOP-18939) | NPE in AWS v2 SDK RetryOnErrorCodeCondition.shouldRetry() |  Critical | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-11576](https://issues.apache.org/jira/browse/YARN-11576) | Improve FederationInterceptorREST AuditLog |  Major | federation, router | Shilun Fan | Shilun Fan |
| [HADOOP-18932](https://issues.apache.org/jira/browse/HADOOP-18932) | Upgrade AWS v2 SDK to 2.20.160 and v1 to 1.12.565 |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-18948](https://issues.apache.org/jira/browse/HADOOP-18948) | S3A. Add option fs.s3a.directory.operations.purge.uploads to purge on rename/delete |  Minor | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-11593](https://issues.apache.org/jira/browse/YARN-11593) | [Federation] Improve command line help information. |  Major | federation | Shilun Fan | Shilun Fan |
| [HADOOP-18918](https://issues.apache.org/jira/browse/HADOOP-18918) | ITestS3GuardTool fails if SSE/DSSE encryption is used |  Minor | fs/s3, test | Viraj Jasani | Viraj Jasani |
| [HADOOP-18850](https://issues.apache.org/jira/browse/HADOOP-18850) | Enable dual-layer server-side encryption with AWS KMS keys (DSSE-KMS) |  Major | fs/s3, security | Akira Ajisaka | Viraj Jasani |
| [YARN-11594](https://issues.apache.org/jira/browse/YARN-11594) | [Federation] Improve Yarn Federation documentation |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11609](https://issues.apache.org/jira/browse/YARN-11609) | Improve the time unit for FederationRMAdminInterceptor#heartbeatExpirationMillis |  Minor | federation | WangYuanben | WangYuanben |
| [YARN-11548](https://issues.apache.org/jira/browse/YARN-11548) | [Federation] Router Supports Format FederationStateStore. |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11011](https://issues.apache.org/jira/browse/YARN-11011) | Make YARN Router throw Exception to client clearly |  Major | federation, router, yarn | Yuan Luo | Shilun Fan |
| [YARN-11484](https://issues.apache.org/jira/browse/YARN-11484) | [Federation] Router Supports Yarn Client CLI Cmds. |  Major | federation | Shilun Fan | Shilun Fan |
| [HADOOP-18872](https://issues.apache.org/jira/browse/HADOOP-18872) | ABFS: Misreporting Retry Count for Sub-sequential and Parallel Operations |  Major | build | Anmol Asrani | Anuj Modi |
| [YARN-11483](https://issues.apache.org/jira/browse/YARN-11483) | [Federation] Router AdminCLI Supports Clean Finish Apps. |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11610](https://issues.apache.org/jira/browse/YARN-11610) | [Federation] Add WeightedHomePolicyManager |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11577](https://issues.apache.org/jira/browse/YARN-11577) | Improve FederationInterceptorREST Method Result |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11485](https://issues.apache.org/jira/browse/YARN-11485) | [Federation] Router Supports Yarn Admin CLI Cmds. |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11614](https://issues.apache.org/jira/browse/YARN-11614) | [Federation] Add Federation PolicyManager Validation Rules |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11620](https://issues.apache.org/jira/browse/YARN-11620) | [Federation] Improve FederationClientInterceptor To Return Partial Results of subClusters. |  Major | federation | Shilun Fan | Shilun Fan |
| [HADOOP-18995](https://issues.apache.org/jira/browse/HADOOP-18995) | S3A: Upgrade AWS SDK version to 2.21.33 for Amazon S3 Express One Zone support |  Major | fs/s3 | Ahmar Suhail | Ahmar Suhail |
| [HADOOP-18915](https://issues.apache.org/jira/browse/HADOOP-18915) | Tune/extend S3A http connection and thread pool settings |  Major | fs/s3 | Ahmar Suhail | Steve Loughran |
| [HADOOP-18996](https://issues.apache.org/jira/browse/HADOOP-18996) | S3A to provide full support for S3 Express One Zone |  Major | fs/s3 | Ahmar Suhail | Steve Loughran |
| [YARN-11561](https://issues.apache.org/jira/browse/YARN-11561) | [Federation] GPG Supports Format PolicyStateStore. |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11613](https://issues.apache.org/jira/browse/YARN-11613) | [Federation] Router CLI Supports Delete SubClusterPolicyConfiguration Of Queues. |  Major | federation | Shilun Fan | Shilun Fan |
| [HADOOP-18997](https://issues.apache.org/jira/browse/HADOOP-18997) | S3A: Add option fs.s3a.s3express.create.session to enable/disable CreateSession |  Minor | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-11619](https://issues.apache.org/jira/browse/YARN-11619) | [Federation] Router CLI Supports List SubClusters. |  Major | federation | Shilun Fan | Shilun Fan |
| [HADOOP-19008](https://issues.apache.org/jira/browse/HADOOP-19008) | S3A: Upgrade AWS SDK to 2.21.41 |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-11627](https://issues.apache.org/jira/browse/YARN-11627) | [GPG] Improve GPGPolicyFacade#getPolicyManager |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11629](https://issues.apache.org/jira/browse/YARN-11629) | [GPG] Improve GPGOverviewBlock Infomation |  Major | federation | Shilun Fan | Shilun Fan |
| [HADOOP-17912](https://issues.apache.org/jira/browse/HADOOP-17912) | ABFS: Support for Encryption Context |  Major | fs/azure | Sumangala Patki | Pranav Saxena |
| [YARN-11632](https://issues.apache.org/jira/browse/YARN-11632) | [Doc] Add allow-partial-result description to Yarn Federation documentation |  Major | federation | Shilun Fan | Shilun Fan |
| [HADOOP-18971](https://issues.apache.org/jira/browse/HADOOP-18971) | ABFS: Enable Footer Read Optimizations with Appropriate Footer Read Buffer Size |  Major | fs/azure | Anuj Modi | Anuj Modi |
| [YARN-11556](https://issues.apache.org/jira/browse/YARN-11556) | Let Federation.md more standardized |  Minor | documentation | WangYuanben | WangYuanben |
| [YARN-11553](https://issues.apache.org/jira/browse/YARN-11553) | Change the time unit of scCleanerIntervalMs in Router |  Minor | router | WangYuanben | WangYuanben |
| [HADOOP-19004](https://issues.apache.org/jira/browse/HADOOP-19004) | S3A: Support Authentication through HttpSigner API |  Major | fs/s3 | Steve Loughran | Harshit Gupta |
| [HADOOP-18865](https://issues.apache.org/jira/browse/HADOOP-18865) | ABFS: Adding 100 continue in userAgent String and dynamically removing it if retry is without the header enabled. |  Minor | build | Anmol Asrani | Anmol Asrani |
| [HADOOP-19027](https://issues.apache.org/jira/browse/HADOOP-19027) | S3A: S3AInputStream doesn't recover from HTTP/channel exceptions |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-19033](https://issues.apache.org/jira/browse/HADOOP-19033) | S3A: disable checksum validation |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-18959](https://issues.apache.org/jira/browse/HADOOP-18959) | Use builder for prefetch CachingBlockManager |  Major | fs/s3 | Viraj Jasani | Viraj Jasani |
| [HADOOP-18883](https://issues.apache.org/jira/browse/HADOOP-18883) | Expect-100 JDK bug resolution: prevent multiple server calls |  Major | fs/azure | Pranav Saxena | Pranav Saxena |
| [HADOOP-19015](https://issues.apache.org/jira/browse/HADOOP-19015) | Increase fs.s3a.connection.maximum to 500 to minimize risk of Timeout waiting for connection from pool |  Major | fs/s3 | Mukund Thakur | Mukund Thakur |
| [HADOOP-18975](https://issues.apache.org/jira/browse/HADOOP-18975) | AWS SDK v2:  extend support for FIPS endpoints |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-19046](https://issues.apache.org/jira/browse/HADOOP-19046) | S3A: update AWS sdk versions to 2.23.5 and 1.12.599 |  Major | build, fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-18830](https://issues.apache.org/jira/browse/HADOOP-18830) | S3A: Cut S3 Select |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-18980](https://issues.apache.org/jira/browse/HADOOP-18980) | S3A credential provider remapping: make extensible |  Minor | fs/s3 | Steve Loughran | Viraj Jasani |
| [HADOOP-19044](https://issues.apache.org/jira/browse/HADOOP-19044) | AWS SDK V2 - Update S3A region logic |  Major | fs/s3 | Ahmar Suhail | Viraj Jasani |
| [HADOOP-19045](https://issues.apache.org/jira/browse/HADOOP-19045) | HADOOP-19045. S3A: CreateSession Timeout after 10 seconds |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-19069](https://issues.apache.org/jira/browse/HADOOP-19069) | Use hadoop-thirdparty 1.2.0 |  Major | hadoop-thirdparty | Shilun Fan | Shilun Fan |
| [HADOOP-19084](https://issues.apache.org/jira/browse/HADOOP-19084) | prune dependency exports of hadoop-\* modules |  Blocker | build | Steve Loughran | Steve Loughran |
| [HADOOP-19099](https://issues.apache.org/jira/browse/HADOOP-19099) | Add Protobuf Compatibility Notes |  Major | documentation | Shilun Fan | Shilun Fan |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-15465](https://issues.apache.org/jira/browse/HDFS-15465) | Support WebHDFS accesses to the data stored in secure Datanode through insecure Namenode |  Minor | federation, webhdfs | Toshihiko Uchida | Toshihiko Uchida |
| [HDFS-15854](https://issues.apache.org/jira/browse/HDFS-15854) | Make some parameters configurable for SlowDiskTracker and SlowPeerTracker |  Major | block placement | Tao Li | Tao Li |
| [HDFS-15870](https://issues.apache.org/jira/browse/HDFS-15870) | Remove unused configuration dfs.namenode.stripe.min |  Minor | configuration | Tao Li | Tao Li |
| [HDFS-15808](https://issues.apache.org/jira/browse/HDFS-15808) | Add metrics for FSNamesystem read/write lock hold long time |  Major | hdfs | Tao Li | Tao Li |
| [HDFS-15873](https://issues.apache.org/jira/browse/HDFS-15873) | Add namenode address in logs for block report |  Minor | datanode, hdfs | Tao Li | Tao Li |
| [HDFS-15906](https://issues.apache.org/jira/browse/HDFS-15906) | Close FSImage and FSNamesystem after formatting is complete |  Minor | namanode | Tao Li | Tao Li |
| [HDFS-15892](https://issues.apache.org/jira/browse/HDFS-15892) | Add metric for editPendingQ in FSEditLogAsync |  Minor | metrics | Tao Li | Tao Li |
| [HDFS-15938](https://issues.apache.org/jira/browse/HDFS-15938) | Fix java doc in FSEditLog |  Minor | documentation | Tao Li | Tao Li |
| [HDFS-15951](https://issues.apache.org/jira/browse/HDFS-15951) | Remove unused parameters in NameNodeProxiesClient |  Minor | hdfs-client | Tao Li | Tao Li |
| [HDFS-15975](https://issues.apache.org/jira/browse/HDFS-15975) | Use LongAdder instead of AtomicLong |  Major | metrics | Tao Li | Tao Li |
| [HDFS-15991](https://issues.apache.org/jira/browse/HDFS-15991) | Add location into datanode info for NameNodeMXBean |  Minor | metrics, namanode | Tao Li | Tao Li |
| [HDFS-16078](https://issues.apache.org/jira/browse/HDFS-16078) | Remove unused parameters for DatanodeManager.handleLifeline() |  Minor | namanode | Tao Li | Tao Li |
| [HDFS-16079](https://issues.apache.org/jira/browse/HDFS-16079) | Improve the block state change log |  Minor | block placement | Tao Li | Tao Li |
| [HDFS-16089](https://issues.apache.org/jira/browse/HDFS-16089) | EC: Add metric EcReconstructionValidateTimeMillis for StripedBlockReconstructor |  Minor | erasure-coding, metrics | Tao Li | Tao Li |
| [HDFS-16104](https://issues.apache.org/jira/browse/HDFS-16104) | Remove unused parameter and fix java doc for DiskBalancerCLI |  Minor | diskbalancer, documentation | Tao Li | Tao Li |
| [HDFS-16106](https://issues.apache.org/jira/browse/HDFS-16106) | Fix flaky unit test TestDFSShell |  Minor | test | Tao Li | Tao Li |
| [HDFS-16110](https://issues.apache.org/jira/browse/HDFS-16110) | Remove unused method reportChecksumFailure in DFSClient |  Minor | dfsclient | Tao Li | Tao Li |
| [HDFS-16131](https://issues.apache.org/jira/browse/HDFS-16131) | Show storage type for failed volumes on namenode web |  Minor | namanode, ui | Tao Li | Tao Li |
| [HDFS-16194](https://issues.apache.org/jira/browse/HDFS-16194) | Simplify the code with DatanodeID#getXferAddrWithHostname |  Minor | datanode, metrics, namanode | Tao Li | Tao Li |
| [HDFS-16280](https://issues.apache.org/jira/browse/HDFS-16280) | Fix typo for ShortCircuitReplica#isStale |  Minor | hdfs-client | Tao Li | Tao Li |
| [HDFS-16281](https://issues.apache.org/jira/browse/HDFS-16281) | Fix flaky unit tests failed due to timeout |  Minor | test | Tao Li | Tao Li |
| [HDFS-16298](https://issues.apache.org/jira/browse/HDFS-16298) | Improve error msg for BlockMissingException |  Minor | hdfs-client | Tao Li | Tao Li |
| [HDFS-16312](https://issues.apache.org/jira/browse/HDFS-16312) | Fix typo for DataNodeVolumeMetrics and ProfilingFileIoEvents |  Minor | datanode, metrics | Tao Li | Tao Li |
| [HADOOP-18005](https://issues.apache.org/jira/browse/HADOOP-18005) | Correct log format for LdapGroupsMapping |  Minor | security | Tao Li | Tao Li |
| [HDFS-16319](https://issues.apache.org/jira/browse/HDFS-16319) | Add metrics doc for ReadLockLongHoldCount and WriteLockLongHoldCount |  Minor | metrics | Tao Li | Tao Li |
| [HDFS-16326](https://issues.apache.org/jira/browse/HDFS-16326) | Simplify the code for DiskBalancer |  Minor | diskbalancer | Tao Li | Tao Li |
| [HDFS-16335](https://issues.apache.org/jira/browse/HDFS-16335) | Fix HDFSCommands.md |  Minor | documentation | Tao Li | Tao Li |
| [HDFS-16339](https://issues.apache.org/jira/browse/HDFS-16339) | Show the threshold when mover threads quota is exceeded |  Minor | datanode | Tao Li | Tao Li |
| [HDFS-16435](https://issues.apache.org/jira/browse/HDFS-16435) | Remove no need TODO comment for ObserverReadProxyProvider |  Minor | namanode | Tao Li | Tao Li |
| [HDFS-16541](https://issues.apache.org/jira/browse/HDFS-16541) | Fix a typo in NameNodeLayoutVersion. |  Minor | namanode | ZhiWei Shi | ZhiWei Shi |
| [HDFS-16587](https://issues.apache.org/jira/browse/HDFS-16587) | Allow configuring Handler number for the JournalNodeRpcServer |  Major | journal-node | ZanderXu | ZanderXu |
| [HDFS-16866](https://issues.apache.org/jira/browse/HDFS-16866) | Fix a typo in Dispatcher. |  Minor | balancer | ZhiWei Shi | ZhiWei Shi |
| [HDFS-17047](https://issues.apache.org/jira/browse/HDFS-17047) | BlockManager#addStoredBlock should log storage id when AddBlockResult is REPLACED |  Minor | hdfs | farmmamba | farmmamba |
| [YARN-9586](https://issues.apache.org/jira/browse/YARN-9586) | [QA] Need more doc for yarn.federation.policy-manager-params when LoadBasedRouterPolicy is used |  Major | federation | Shen Yinjie | Shilun Fan |
| [YARN-10247](https://issues.apache.org/jira/browse/YARN-10247) | Application priority queue ACLs are not respected |  Blocker | capacity scheduler | Sunil G | Sunil G |
| [HADOOP-17033](https://issues.apache.org/jira/browse/HADOOP-17033) | Update commons-codec from 1.11 to 1.14 |  Major | common | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-17055](https://issues.apache.org/jira/browse/HADOOP-17055) | Remove residual code of Ozone |  Major | common, ozone | Wanqiang Ji | Wanqiang Ji |
| [YARN-10274](https://issues.apache.org/jira/browse/YARN-10274) | Merge QueueMapping and QueueMappingEntity |  Major | yarn | Gergely Pollák | Gergely Pollák |
| [YARN-10281](https://issues.apache.org/jira/browse/YARN-10281) | Redundant QueuePath usage in UserGroupMappingPlacementRule and AppNameMappingPlacementRule |  Major | capacity scheduler | Gergely Pollák | Gergely Pollák |
| [YARN-10279](https://issues.apache.org/jira/browse/YARN-10279) | Avoid unnecessary QueueMappingEntity creations |  Minor | resourcemanager | Gergely Pollák | Hudáky Márton Gyula |
| [YARN-10277](https://issues.apache.org/jira/browse/YARN-10277) | CapacityScheduler test TestUserGroupMappingPlacementRule should build proper hierarchy |  Major | capacity scheduler | Gergely Pollák | Szilard Nemeth |
| [HADOOP-16866](https://issues.apache.org/jira/browse/HADOOP-16866) | Upgrade spotbugs to 4.0.6 |  Minor | build, command | Tsuyoshi Ozawa | Masatake Iwasaki |
| [HADOOP-17234](https://issues.apache.org/jira/browse/HADOOP-17234) | Add .asf.yaml to allow github and jira integration |  Major | build, common | Ayush Saxena | Ayush Saxena |
| [MAPREDUCE-7298](https://issues.apache.org/jira/browse/MAPREDUCE-7298) | Distcp doesn't close the job after the job is completed |  Major | distcp | Aasha Medhi | Aasha Medhi |
| [HADOOP-16990](https://issues.apache.org/jira/browse/HADOOP-16990) | Update Mockserver |  Major | hdfs-client | Wei-Chiu Chuang | Attila Doroszlai |
| [HADOOP-17030](https://issues.apache.org/jira/browse/HADOOP-17030) | Remove unused joda-time |  Major | build, common | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-10278](https://issues.apache.org/jira/browse/YARN-10278) | CapacityScheduler test framework ProportionalCapacityPreemptionPolicyMockFramework need some review |  Major | capacity scheduler, test | Gergely Pollák | Szilard Nemeth |
| [YARN-10540](https://issues.apache.org/jira/browse/YARN-10540) | Node page is broken in YARN UI1 and UI2 including RMWebService api for nodes |  Critical | webapp | Sunil G | Jim Brennan |
| [HADOOP-17445](https://issues.apache.org/jira/browse/HADOOP-17445) | Update the year to 2021 |  Major | common | Xiaoqiao He | Xiaoqiao He |
| [HDFS-15731](https://issues.apache.org/jira/browse/HDFS-15731) | Reduce threadCount for unit tests to reduce the memory usage |  Major | build, test | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17571](https://issues.apache.org/jira/browse/HADOOP-17571) | Upgrade com.fasterxml.woodstox:woodstox-core for security reasons |  Major | common | Viraj Jasani | Viraj Jasani |
| [HDFS-15895](https://issues.apache.org/jira/browse/HDFS-15895) | DFSAdmin#printOpenFiles has redundant String#format usage |  Minor | dfsadmin | Viraj Jasani | Viraj Jasani |
| [HDFS-15926](https://issues.apache.org/jira/browse/HDFS-15926) | Removed duplicate dependency of hadoop-annotations |  Minor | hdfs | Viraj Jasani | Viraj Jasani |
| [HADOOP-17614](https://issues.apache.org/jira/browse/HADOOP-17614) | Bump netty to the latest 4.1.61 |  Blocker | build, common | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-17622](https://issues.apache.org/jira/browse/HADOOP-17622) | Avoid usage of deprecated IOUtils#cleanup API |  Minor | common | Viraj Jasani | Viraj Jasani |
| [HADOOP-17624](https://issues.apache.org/jira/browse/HADOOP-17624) | Remove any rocksdb exclusion code |  Major | common | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-17625](https://issues.apache.org/jira/browse/HADOOP-17625) | Update to Jetty 9.4.39 |  Major | build, common | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-15989](https://issues.apache.org/jira/browse/HDFS-15989) | Split TestBalancer into two classes |  Major | balancer, test | Viraj Jasani | Viraj Jasani |
| [HDFS-15850](https://issues.apache.org/jira/browse/HDFS-15850) | Superuser actions should be reported to external enforcers |  Major | security | Vivek Ratnavel Subramanian | Vivek Ratnavel Subramanian |
| [YARN-10746](https://issues.apache.org/jira/browse/YARN-10746) | RmWebApp add default-node-label-expression to the queue info |  Major | resourcemanager, webapp | Gergely Pollák | Gergely Pollák |
| [YARN-10750](https://issues.apache.org/jira/browse/YARN-10750) | TestMetricsInvariantChecker.testManyRuns is broken since HADOOP-17524 |  Major | test | Gergely Pollák | Gergely Pollák |
| [YARN-10747](https://issues.apache.org/jira/browse/YARN-10747) | Bump YARN CSI protobuf version to 3.7.1 |  Major | yarn | Siyao Meng | Siyao Meng |
| [HADOOP-17676](https://issues.apache.org/jira/browse/HADOOP-17676) | Restrict imports from org.apache.curator.shaded |  Major | common | Viraj Jasani | Viraj Jasani |
| [HADOOP-17683](https://issues.apache.org/jira/browse/HADOOP-17683) | Update commons-io to 2.8.0 |  Major | build, common | Wei-Chiu Chuang | Akira Ajisaka |
| [HADOOP-17426](https://issues.apache.org/jira/browse/HADOOP-17426) | Upgrade to hadoop-thirdparty-1.1.0 |  Major | hadoop-thirdparty | Ayush Saxena | Wei-Chiu Chuang |
| [YARN-10779](https://issues.apache.org/jira/browse/YARN-10779) | Add option to disable lowercase conversion in GetApplicationsRequestPBImpl and ApplicationSubmissionContextPBImpl |  Major | resourcemanager | Peter Bacsko | Peter Bacsko |
| [HADOOP-17732](https://issues.apache.org/jira/browse/HADOOP-17732) | Keep restrict-imports-enforcer-rule for Guava Sets in hadoop-main pom |  Major | common | Viraj Jasani | Viraj Jasani |
| [HADOOP-17739](https://issues.apache.org/jira/browse/HADOOP-17739) | Use hadoop-thirdparty 1.1.1 |  Major | hadoop-thirdparty | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [MAPREDUCE-7350](https://issues.apache.org/jira/browse/MAPREDUCE-7350) | Replace Guava Lists usage by Hadoop's own Lists in hadoop-mapreduce-project |  Major | common | Viraj Jasani | Viraj Jasani |
| [HADOOP-17743](https://issues.apache.org/jira/browse/HADOOP-17743) | Replace Guava Lists usage by Hadoop's own Lists in hadoop-common, hadoop-tools and cloud-storage projects |  Major | common | Viraj Jasani | Viraj Jasani |
| [HDFS-16054](https://issues.apache.org/jira/browse/HDFS-16054) | Replace Guava Lists usage by Hadoop's own Lists in hadoop-hdfs-project |  Major | hdfs-common | Viraj Jasani | Viraj Jasani |
| [YARN-10805](https://issues.apache.org/jira/browse/YARN-10805) | Replace Guava Lists usage by Hadoop's own Lists in hadoop-yarn-project |  Major | yarn-common | Viraj Jasani | Viraj Jasani |
| [HADOOP-17753](https://issues.apache.org/jira/browse/HADOOP-17753) | Keep restrict-imports-enforcer-rule for Guava Lists in hadoop-main pom |  Minor | common | Viraj Jasani | Viraj Jasani |
| [YARN-10820](https://issues.apache.org/jira/browse/YARN-10820) | Make GetClusterNodesRequestPBImpl thread safe |  Major | client | Prabhu Joseph | SwathiChandrashekar |
| [HADOOP-17788](https://issues.apache.org/jira/browse/HADOOP-17788) | Replace IOUtils#closeQuietly usages |  Major | common | Viraj Jasani | Viraj Jasani |
| [MAPREDUCE-7356](https://issues.apache.org/jira/browse/MAPREDUCE-7356) | Remove few duplicate dependencies from mapreduce-client's child poms |  Minor | client | Viraj Jasani | Viraj Jasani |
| [HDFS-16139](https://issues.apache.org/jira/browse/HDFS-16139) | Update BPServiceActor Scheduler's nextBlockReportTime atomically |  Major | datanode | Viraj Jasani | Viraj Jasani |
| [HADOOP-17808](https://issues.apache.org/jira/browse/HADOOP-17808) | ipc.Client not setting interrupt flag after catching InterruptedException |  Minor | ipc | Viraj Jasani | Viraj Jasani |
| [HADOOP-17835](https://issues.apache.org/jira/browse/HADOOP-17835) | Use CuratorCache implementation instead of PathChildrenCache / TreeCache |  Major | common | Viraj Jasani | Viraj Jasani |
| [HADOOP-17841](https://issues.apache.org/jira/browse/HADOOP-17841) | Remove ListenerHandle from Hadoop registry |  Minor | common | Viraj Jasani | Viraj Jasani |
| [HADOOP-17799](https://issues.apache.org/jira/browse/HADOOP-17799) | Improve the GitHub pull request template |  Major | build, documentation | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17834](https://issues.apache.org/jira/browse/HADOOP-17834) | Bump aliyun-sdk-oss to 3.13.0 |  Major | build, common | Siyao Meng | Siyao Meng |
| [HADOOP-17892](https://issues.apache.org/jira/browse/HADOOP-17892) | Add Hadoop code formatter in dev-support |  Major | common | Viraj Jasani | Viraj Jasani |
| [MAPREDUCE-7363](https://issues.apache.org/jira/browse/MAPREDUCE-7363) | Rename JobClientUnitTest to TestJobClient |  Major | mrv2 | Dongjoon Hyun | Dongjoon Hyun |
| [HADOOP-17950](https://issues.apache.org/jira/browse/HADOOP-17950) | Provide replacement for deprecated APIs of commons-io IOUtils |  Major | common | Viraj Jasani | Viraj Jasani |
| [HADOOP-17955](https://issues.apache.org/jira/browse/HADOOP-17955) | Bump netty to the latest 4.1.68 |  Major | build, common | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-17967](https://issues.apache.org/jira/browse/HADOOP-17967) | Keep restrict-imports-enforcer-rule for Guava VisibleForTesting in hadoop-main pom |  Major | build, common | Viraj Jasani | Viraj Jasani |
| [HADOOP-17946](https://issues.apache.org/jira/browse/HADOOP-17946) | Update commons-lang to 3.12.0 |  Minor | build, common | Sean Busbey | Renukaprasad C |
| [HADOOP-17968](https://issues.apache.org/jira/browse/HADOOP-17968) | Migrate checkstyle module illegalimport to maven enforcer banned-illegal-imports |  Major | build | Viraj Jasani | Viraj Jasani |
| [HDFS-16323](https://issues.apache.org/jira/browse/HDFS-16323) | DatanodeHttpServer doesn't require handler state map while retrieving filter handlers |  Minor | datanode | Viraj Jasani | Viraj Jasani |
| [HADOOP-18014](https://issues.apache.org/jira/browse/HADOOP-18014) | CallerContext should not include some characters |  Major | ipc | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-13464](https://issues.apache.org/jira/browse/HADOOP-13464) | update GSON to 2.7+ |  Minor | build | Sean Busbey | Igor Dvorzhak |
| [HADOOP-18061](https://issues.apache.org/jira/browse/HADOOP-18061) | Update the year to 2022 |  Major | common | Ayush Saxena | Ayush Saxena |
| [MAPREDUCE-7371](https://issues.apache.org/jira/browse/MAPREDUCE-7371) | DistributedCache alternative APIs should not use DistributedCache APIs internally |  Major | mrv1, mrv2 | Viraj Jasani | Viraj Jasani |
| [YARN-11026](https://issues.apache.org/jira/browse/YARN-11026) | Make AppPlacementAllocator configurable in AppSchedulingInfo |  Major | scheduler | Minni Mittal | Minni Mittal |
| [HADOOP-18098](https://issues.apache.org/jira/browse/HADOOP-18098) | Basic verification for the release candidate vote |  Major | build | Viraj Jasani | Viraj Jasani |
| [HDFS-16481](https://issues.apache.org/jira/browse/HDFS-16481) | Provide support to set Http and Rpc ports in MiniJournalCluster |  Major | test | Viraj Jasani | Viraj Jasani |
| [HADOOP-18131](https://issues.apache.org/jira/browse/HADOOP-18131) | Upgrade maven enforcer plugin and relevant dependencies |  Major | build | Viraj Jasani | Viraj Jasani |
| [HDFS-16502](https://issues.apache.org/jira/browse/HDFS-16502) | Reconfigure Block Invalidate limit |  Major | block placement | Viraj Jasani | Viraj Jasani |
| [HDFS-16522](https://issues.apache.org/jira/browse/HDFS-16522) | Set Http and Ipc ports for Datanodes in MiniDFSCluster |  Major | tets | Viraj Jasani | Viraj Jasani |
| [HADOOP-18191](https://issues.apache.org/jira/browse/HADOOP-18191) | Log retry count while handling exceptions in RetryInvocationHandler |  Minor | common, io | Viraj Jasani | Viraj Jasani |
| [HADOOP-18196](https://issues.apache.org/jira/browse/HADOOP-18196) | Remove replace-guava from replacer plugin |  Major | build | Viraj Jasani | Viraj Jasani |
| [HADOOP-18125](https://issues.apache.org/jira/browse/HADOOP-18125) | Utility to identify git commit / Jira fixVersion discrepancies for RC preparation |  Major | build | Viraj Jasani | Viraj Jasani |
| [HDFS-16035](https://issues.apache.org/jira/browse/HDFS-16035) | Remove DummyGroupMapping as it is not longer used anywhere |  Minor | httpfs, test | Viraj Jasani | Ashutosh Gupta |
| [HADOOP-18228](https://issues.apache.org/jira/browse/HADOOP-18228) | Update hadoop-vote to use HADOOP\_RC\_VERSION dir |  Minor | build | Viraj Jasani | Viraj Jasani |
| [HADOOP-18224](https://issues.apache.org/jira/browse/HADOOP-18224) | Upgrade maven compiler plugin to 3.10.1 |  Major | build | Viraj Jasani | Viraj Jasani |
| [HDFS-16618](https://issues.apache.org/jira/browse/HDFS-16618) | sync\_file\_range error should include more volume and file info |  Minor | datanode | Viraj Jasani | Viraj Jasani |
| [HDFS-16616](https://issues.apache.org/jira/browse/HDFS-16616) | Remove the use if Sets#newHashSet and Sets#newTreeSet |  Major | hdfs-common | Samrat Deb | Samrat Deb |
| [HADOOP-18300](https://issues.apache.org/jira/browse/HADOOP-18300) | Update google-gson to 2.9.0 |  Minor | build | Igor Dvorzhak | Igor Dvorzhak |
| [HADOOP-18397](https://issues.apache.org/jira/browse/HADOOP-18397) | Shutdown AWSSecurityTokenService when its resources are no longer in use |  Major | fs/s3 | Viraj Jasani | Viraj Jasani |
| [HDFS-16730](https://issues.apache.org/jira/browse/HDFS-16730) | Update the doc that append to EC files is supported |  Major | documentation, erasure-coding | Wei-Chiu Chuang | Ashutosh Gupta |
| [HDFS-16822](https://issues.apache.org/jira/browse/HDFS-16822) | HostRestrictingAuthorizationFilter should pass through requests if they don't access WebHDFS API |  Major | webhdfs | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-16833](https://issues.apache.org/jira/browse/HDFS-16833) | NameNode should log internal EC blocks instead of the EC block group when it receives block reports |  Major | erasure-coding | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-18575](https://issues.apache.org/jira/browse/HADOOP-18575) | Make XML transformer factory more lenient |  Major | common | PJ Fanning | PJ Fanning |
| [HADOOP-18586](https://issues.apache.org/jira/browse/HADOOP-18586) | Update the year to 2023 |  Major | common | Ayush Saxena | Ayush Saxena |
| [HADOOP-18587](https://issues.apache.org/jira/browse/HADOOP-18587) | upgrade to jettison 1.5.3 to fix CVE-2022-40150 |  Major | common | PJ Fanning | PJ Fanning |
| [HDFS-16886](https://issues.apache.org/jira/browse/HDFS-16886) | Fix documentation for StateStoreRecordOperations#get(Class ..., Query ...) |  Trivial | documentation | Simbarashe Dzinamarira | Simbarashe Dzinamarira |
| [HADOOP-18602](https://issues.apache.org/jira/browse/HADOOP-18602) | Remove netty3 dependency |  Major | build | Tamas Domok | Tamas Domok |
| [HDFS-16902](https://issues.apache.org/jira/browse/HDFS-16902) | Add Namenode status to BPServiceActor metrics and improve logging in offerservice |  Major | namanode | Viraj Jasani | Viraj Jasani |
| [MAPREDUCE-7433](https://issues.apache.org/jira/browse/MAPREDUCE-7433) | Remove unused mapred/LoggingHttpResponseEncoder.java |  Major | mrv1 | Tamas Domok | Tamas Domok |
| [HADOOP-18524](https://issues.apache.org/jira/browse/HADOOP-18524) | Deploy Hadoop trunk version website |  Major | documentation | Ayush Saxena | Ayush Saxena |
| [HDFS-16901](https://issues.apache.org/jira/browse/HDFS-16901) | RBF: Routers should propagate the real user in the UGI via the caller context |  Major | rbf | Simbarashe Dzinamarira | Simbarashe Dzinamarira |
| [HDFS-16890](https://issues.apache.org/jira/browse/HDFS-16890) | RBF: Add period state refresh to keep router state near active namenode's |  Major | rbf | Simbarashe Dzinamarira | Simbarashe Dzinamarira |
| [HDFS-16917](https://issues.apache.org/jira/browse/HDFS-16917) | Add transfer rate quantile metrics for DataNode reads |  Minor | datanode | Ravindra Dingankar | Ravindra Dingankar |
| [HADOOP-18658](https://issues.apache.org/jira/browse/HADOOP-18658) | snakeyaml dependency: upgrade to v2.0 |  Major | common | PJ Fanning | PJ Fanning |
| [HADOOP-18676](https://issues.apache.org/jira/browse/HADOOP-18676) | Include jettison as direct dependency of hadoop-common |  Major | common | Andras Katona | Andras Katona |
| [HDFS-16943](https://issues.apache.org/jira/browse/HDFS-16943) | RBF: Implement MySQL based StateStoreDriver |  Major | hdfs, rbf | Simbarashe Dzinamarira | Simbarashe Dzinamarira |
| [HADOOP-18711](https://issues.apache.org/jira/browse/HADOOP-18711) | upgrade nimbus jwt jar due to issues in its embedded shaded json-smart code |  Major | common | PJ Fanning | PJ Fanning |
| [HDFS-16998](https://issues.apache.org/jira/browse/HDFS-16998) | RBF: Add ops metrics for getSlowDatanodeReport in RouterClientActivity |  Major | rbf | Viraj Jasani | Viraj Jasani |
| [HADOOP-17612](https://issues.apache.org/jira/browse/HADOOP-17612) | Upgrade Zookeeper to 3.6.3 and Curator to 5.2.0 |  Major | common | Viraj Jasani | Viraj Jasani |
| [HDFS-17008](https://issues.apache.org/jira/browse/HDFS-17008) | Fix RBF JDK 11 javadoc warnings |  Major | rbf | Viraj Jasani | Viraj Jasani |
| [YARN-11498](https://issues.apache.org/jira/browse/YARN-11498) | Exclude Jettison from jersey-json artifact in hadoop-yarn-common's pom.xml |  Major | build | Devaspati Krishnatri | Devaspati Krishnatri |
| [HADOOP-18782](https://issues.apache.org/jira/browse/HADOOP-18782) | upgrade to snappy-java 1.1.10.1 due to CVEs |  Major | common | PJ Fanning | PJ Fanning |
| [HADOOP-18773](https://issues.apache.org/jira/browse/HADOOP-18773) | Upgrade maven-shade-plugin to 3.4.1 |  Minor | build | Rohit Kumar | Rohit Kumar |
| [HADOOP-18783](https://issues.apache.org/jira/browse/HADOOP-18783) | upgrade netty to 4.1.94 due to CVE |  Major | common | PJ Fanning | PJ Fanning |
| [HADOOP-18837](https://issues.apache.org/jira/browse/HADOOP-18837) | Upgrade Okio to 3.4.0 due to CVE-2023-3635 |  Major | common | Rohit Kumar | Rohit Kumar |
| [YARN-11037](https://issues.apache.org/jira/browse/YARN-11037) | Add configurable logic to split resource request to least loaded SC |  Major | federation | Minni Mittal | Minni Mittal |
| [YARN-11535](https://issues.apache.org/jira/browse/YARN-11535) | Remove jackson-dataformat-yaml dependency |  Major | build, yarn | Susheel Gupta | Benjamin Teke |
| [HADOOP-18073](https://issues.apache.org/jira/browse/HADOOP-18073) | S3A: Upgrade AWS SDK to V2 |  Major | auth, fs/s3 | xiaowei sun | Ahmar Suhail |
| [HADOOP-18851](https://issues.apache.org/jira/browse/HADOOP-18851) | Performance improvement for DelegationTokenSecretManager. |  Major | common | Vikas Kumar | Vikas Kumar |
| [HADOOP-18923](https://issues.apache.org/jira/browse/HADOOP-18923) | Switch to SPDX identifier for license name |  Minor | common | Colm O hEigeartaigh | Colm O hEigeartaigh |
| [HADOOP-19020](https://issues.apache.org/jira/browse/HADOOP-19020) | Update the year to 2024 |  Major | common | Ayush Saxena | Ayush Saxena |
| [HADOOP-19056](https://issues.apache.org/jira/browse/HADOOP-19056) | Highlight RBF features and improvements targeting version 3.4 |  Major | build, common | Takanobu Asanuma | Takanobu Asanuma |


