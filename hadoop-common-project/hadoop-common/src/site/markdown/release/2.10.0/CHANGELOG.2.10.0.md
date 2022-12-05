
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
# "Apache Hadoop" Changelog

## Release 2.10.0 - 2019-10-22

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-12883](https://issues.apache.org/jira/browse/HDFS-12883) | RBF: Document Router and State Store metrics |  Major | documentation | Yiqun Lin | Yiqun Lin |
| [HDFS-12895](https://issues.apache.org/jira/browse/HDFS-12895) | RBF: Add ACL support for mount table |  Major | . | Yiqun Lin | Yiqun Lin |
| [HDFS-13099](https://issues.apache.org/jira/browse/HDFS-13099) | RBF: Use the ZooKeeper as the default State Store |  Minor | documentation | Yiqun Lin | Yiqun Lin |
| [HADOOP-16055](https://issues.apache.org/jira/browse/HADOOP-16055) | Upgrade AWS SDK to 1.11.271 in branch-2 |  Blocker | fs/s3 | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-16053](https://issues.apache.org/jira/browse/HADOOP-16053) | Backport HADOOP-14816 to branch-2 |  Major | build | Akira Ajisaka | Akira Ajisaka |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-13083](https://issues.apache.org/jira/browse/HDFS-13083) | RBF: Fix doc error setting up client |  Major | federation | tartarus | tartarus |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-13283](https://issues.apache.org/jira/browse/HDFS-13283) | Percentage based Reserved Space Calculation for DataNode |  Major | datanode, hdfs | Lukas Majercak | Lukas Majercak |
| [HDFS-13553](https://issues.apache.org/jira/browse/HDFS-13553) | RBF: Support global quota |  Major | . | Íñigo Goiri | Yiqun Lin |
| [HADOOP-15950](https://issues.apache.org/jira/browse/HADOOP-15950) | Failover for LdapGroupsMapping |  Major | common, security | Lukas Majercak | Lukas Majercak |
| [YARN-9761](https://issues.apache.org/jira/browse/YARN-9761) | Allow overriding application submissions based on server side configs |  Major | . | Jonathan Hung | pralabhkumar |
| [YARN-9760](https://issues.apache.org/jira/browse/YARN-9760) | Support configuring application priorities on a workflow level |  Major | . | Jonathan Hung | Varun Saxena |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-14987](https://issues.apache.org/jira/browse/HADOOP-14987) | Improve KMSClientProvider log around delegation token checking |  Major | . | Xiaoyu Yao | Xiaoyu Yao |
| [HADOOP-14872](https://issues.apache.org/jira/browse/HADOOP-14872) | CryptoInputStream should implement unbuffer |  Major | fs, security | John Zhuge | John Zhuge |
| [HADOOP-14960](https://issues.apache.org/jira/browse/HADOOP-14960) | Add GC time percentage monitor/alerter |  Major | . | Misha Dmitriev | Misha Dmitriev |
| [HADOOP-15023](https://issues.apache.org/jira/browse/HADOOP-15023) | ValueQueue should also validate (lowWatermark \* numValues) \> 0 on construction |  Minor | . | Xiao Chen | Xiao Chen |
| [YARN-6851](https://issues.apache.org/jira/browse/YARN-6851) | Capacity Scheduler: document configs for controlling # containers allowed to be allocated per node heartbeat |  Minor | . | Wei Yan | Wei Yan |
| [YARN-7495](https://issues.apache.org/jira/browse/YARN-7495) | Improve robustness of the AggregatedLogDeletionService |  Major | log-aggregation | Jonathan Turner Eagles | Jonathan Turner Eagles |
| [HADOOP-15056](https://issues.apache.org/jira/browse/HADOOP-15056) | Fix TestUnbuffer#testUnbufferException failure |  Minor | test | Jack Bearden | Jack Bearden |
| [HADOOP-15012](https://issues.apache.org/jira/browse/HADOOP-15012) | Add readahead, dropbehind, and unbuffer to StreamCapabilities |  Major | fs | John Zhuge | John Zhuge |
| [HADOOP-15104](https://issues.apache.org/jira/browse/HADOOP-15104) | AliyunOSS: change the default value of max error retry |  Major | fs/oss | wujinhu | wujinhu |
| [YARN-7274](https://issues.apache.org/jira/browse/YARN-7274) | Ability to disable elasticity at leaf queue level |  Major | capacityscheduler | Scott Brokaw | Zian Chen |
| [YARN-7642](https://issues.apache.org/jira/browse/YARN-7642) | Add test case to verify context update after container promotion or demotion with or without auto update |  Minor | nodemanager | Weiwei Yang | Weiwei Yang |
| [HADOOP-15111](https://issues.apache.org/jira/browse/HADOOP-15111) | AliyunOSS: backport HADOOP-14993 to branch-2 |  Major | fs/oss | Genmao Yu | Genmao Yu |
| [HDFS-12818](https://issues.apache.org/jira/browse/HDFS-12818) | Support multiple storages in DataNodeCluster / SimulatedFSDataset |  Minor | datanode, test | Erik Krogen | Erik Krogen |
| [HDFS-9023](https://issues.apache.org/jira/browse/HDFS-9023) | When NN is not able to identify DN for replication, reason behind it can be logged |  Critical | hdfs-client, namenode | Surendra Singh Lilhore | Xiao Chen |
| [YARN-7678](https://issues.apache.org/jira/browse/YARN-7678) | Ability to enable logging of container memory stats |  Major | nodemanager | Jim Brennan | Jim Brennan |
| [HDFS-12945](https://issues.apache.org/jira/browse/HDFS-12945) | Switch to ClientProtocol instead of NamenodeProtocols in NamenodeWebHdfsMethods |  Minor | . | Wei Yan | Wei Yan |
| [YARN-7622](https://issues.apache.org/jira/browse/YARN-7622) | Allow fair-scheduler configuration on HDFS |  Minor | fairscheduler, resourcemanager | Greg Phillips | Greg Phillips |
| [YARN-7590](https://issues.apache.org/jira/browse/YARN-7590) | Improve container-executor validation check |  Major | security, yarn | Eric Yang | Eric Yang |
| [MAPREDUCE-7029](https://issues.apache.org/jira/browse/MAPREDUCE-7029) | FileOutputCommitter is slow on filesystems lacking recursive delete |  Minor | . | Karthik Palaniappan | Karthik Palaniappan |
| [MAPREDUCE-6984](https://issues.apache.org/jira/browse/MAPREDUCE-6984) | MR AM to clean up temporary files from previous attempt in case of no recovery |  Major | applicationmaster | Gergo Repas | Gergo Repas |
| [HADOOP-15189](https://issues.apache.org/jira/browse/HADOOP-15189) | backport HADOOP-15039 to branch-2 and branch-3 |  Blocker | . | Genmao Yu | Genmao Yu |
| [HADOOP-15212](https://issues.apache.org/jira/browse/HADOOP-15212) | Add independent secret manager method for logging expired tokens |  Major | security | Daryn Sharp | Daryn Sharp |
| [YARN-7728](https://issues.apache.org/jira/browse/YARN-7728) | Expose container preemptions related information in Capacity Scheduler queue metrics |  Major | . | Eric Payne | Eric Payne |
| [MAPREDUCE-7048](https://issues.apache.org/jira/browse/MAPREDUCE-7048) | Uber AM can crash due to unknown task in statusUpdate |  Major | mr-am | Peter Bacsko | Peter Bacsko |
| [HADOOP-13972](https://issues.apache.org/jira/browse/HADOOP-13972) | ADLS to support per-store configuration |  Major | fs/adl | John Zhuge | Sharad Sonker |
| [YARN-7813](https://issues.apache.org/jira/browse/YARN-7813) | Capacity Scheduler Intra-queue Preemption should be configurable for each queue |  Major | capacity scheduler, scheduler preemption | Eric Payne | Eric Payne |
| [HADOOP-15235](https://issues.apache.org/jira/browse/HADOOP-15235) | Authentication Tokens should use HMAC instead of MAC |  Major | security | Robert Kanter | Robert Kanter |
| [HDFS-11187](https://issues.apache.org/jira/browse/HDFS-11187) | Optimize disk access for last partial chunk checksum of Finalized replica |  Major | datanode | Wei-Chiu Chuang | Gabor Bota |
| [HADOOP-15266](https://issues.apache.org/jira/browse/HADOOP-15266) |  [branch-2] Upper/Lower case conversion support for group names in LdapGroupsMapping |  Major | . | Nanda kumar | Nanda kumar |
| [HADOOP-15279](https://issues.apache.org/jira/browse/HADOOP-15279) | increase maven heap size recommendations |  Minor | build, documentation, test | Allen Wittenauer | Allen Wittenauer |
| [HDFS-12884](https://issues.apache.org/jira/browse/HDFS-12884) | BlockUnderConstructionFeature.truncateBlock should be of type BlockInfo |  Major | namenode | Konstantin Shvachko | chencan |
| [HADOOP-15334](https://issues.apache.org/jira/browse/HADOOP-15334) | Upgrade Maven surefire plugin |  Major | build | Arpit Agarwal | Arpit Agarwal |
| [HADOOP-15312](https://issues.apache.org/jira/browse/HADOOP-15312) | Undocumented KeyProvider configuration keys |  Major | . | Wei-Chiu Chuang | LiXin Ge |
| [YARN-7623](https://issues.apache.org/jira/browse/YARN-7623) | Fix the CapacityScheduler Queue configuration documentation |  Major | . | Arun Suresh | Jonathan Hung |
| [HDFS-13314](https://issues.apache.org/jira/browse/HDFS-13314) | NameNode should optionally exit if it detects FsImage corruption |  Major | namenode | Arpit Agarwal | Arpit Agarwal |
| [HDFS-13418](https://issues.apache.org/jira/browse/HDFS-13418) |  NetworkTopology should be configurable when enable DFSNetworkTopology |  Major | . | Tao Jie | Tao Jie |
| [HADOOP-15394](https://issues.apache.org/jira/browse/HADOOP-15394) | Backport PowerShell NodeFencer HADOOP-14309 to branch-2 |  Minor | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13462](https://issues.apache.org/jira/browse/HDFS-13462) | Add BIND\_HOST configuration for JournalNode's HTTP and RPC Servers |  Major | hdfs, journal-node | Lukas Majercak | Lukas Majercak |
| [HDFS-13492](https://issues.apache.org/jira/browse/HDFS-13492) | Limit httpfs binds to certain IP addresses in branch-2 |  Major | httpfs | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-12981](https://issues.apache.org/jira/browse/HDFS-12981) | renameSnapshot a Non-Existent snapshot to itself should throw error |  Minor | hdfs | Sailesh Patel | Kitti Nanasi |
| [HADOOP-15441](https://issues.apache.org/jira/browse/HADOOP-15441) | Log kms url and token service at debug level. |  Minor | . | Wei-Chiu Chuang | Gabor Bota |
| [HDFS-13544](https://issues.apache.org/jira/browse/HDFS-13544) | Improve logging for JournalNode in federated cluster |  Major | federation, hdfs | Hanisha Koneru | Hanisha Koneru |
| [YARN-8249](https://issues.apache.org/jira/browse/YARN-8249) | Few REST api's in RMWebServices are missing static user check |  Critical | webapp, yarn | Sunil G | Sunil G |
| [HADOOP-15486](https://issues.apache.org/jira/browse/HADOOP-15486) | Make NetworkTopology#netLock fair |  Major | net | Nanda kumar | Nanda kumar |
| [HADOOP-15449](https://issues.apache.org/jira/browse/HADOOP-15449) | Increase default timeout of ZK session to avoid frequent NameNode failover |  Critical | common | Karthik Palanisamy | Karthik Palanisamy |
| [HDFS-13602](https://issues.apache.org/jira/browse/HDFS-13602) | Add checkOperation(WRITE) checks in FSNamesystem |  Major | ha, namenode | Erik Krogen | Chao Sun |
| [HDFS-13644](https://issues.apache.org/jira/browse/HDFS-13644) | Backport HDFS-10376 to branch-2 |  Major | . | Yiqun Lin | Zsolt Venczel |
| [HDFS-13653](https://issues.apache.org/jira/browse/HDFS-13653) | Make dfs.client.failover.random.order a per nameservice configuration |  Major | federation | Ekanth Sethuramalingam | Ekanth Sethuramalingam |
| [HDFS-13686](https://issues.apache.org/jira/browse/HDFS-13686) | Add overall metrics for FSNamesystemLock |  Major | hdfs, namenode | Lukas Majercak | Lukas Majercak |
| [HDFS-13714](https://issues.apache.org/jira/browse/HDFS-13714) | Fix TestNameNodePrunesMissingStorages test failures on Windows |  Major | hdfs, namenode, test | Lukas Majercak | Lukas Majercak |
| [HDFS-13719](https://issues.apache.org/jira/browse/HDFS-13719) | Docs around dfs.image.transfer.timeout are misleading |  Major | documentation | Kitti Nanasi | Kitti Nanasi |
| [HDFS-11060](https://issues.apache.org/jira/browse/HDFS-11060) | make DEFAULT\_MAX\_CORRUPT\_FILEBLOCKS\_RETURNED configurable |  Minor | hdfs | Lantao Jin | Lantao Jin |
| [YARN-8155](https://issues.apache.org/jira/browse/YARN-8155) | Improve ATSv2 client logging in RM and NM publisher |  Major | . | Rohith Sharma K S | Abhishek Modi |
| [HDFS-13814](https://issues.apache.org/jira/browse/HDFS-13814) | Remove super user privilege requirement for NameNode.getServiceStatus |  Minor | namenode | Chao Sun | Chao Sun |
| [YARN-8559](https://issues.apache.org/jira/browse/YARN-8559) | Expose mutable-conf scheduler's configuration in RM /scheduler-conf endpoint |  Major | resourcemanager | Anna Savarin | Weiwei Yang |
| [HDFS-13813](https://issues.apache.org/jira/browse/HDFS-13813) | Exit NameNode if dangling child inode is detected when saving FsImage |  Major | hdfs, namenode | Siyao Meng | Siyao Meng |
| [HDFS-13821](https://issues.apache.org/jira/browse/HDFS-13821) | RBF: Add dfs.federation.router.mount-table.cache.enable so that users can disable cache |  Major | hdfs | Fei Hui | Fei Hui |
| [HADOOP-15689](https://issues.apache.org/jira/browse/HADOOP-15689) | Add "\*.patch" into .gitignore file of branch-2 |  Major | . | Rui Gao | Rui Gao |
| [HDFS-13831](https://issues.apache.org/jira/browse/HDFS-13831) | Make block increment deletion number configurable |  Major | . | Yiqun Lin | Ryan Wu |
| [YARN-8051](https://issues.apache.org/jira/browse/YARN-8051) | TestRMEmbeddedElector#testCallbackSynchronization is flakey |  Major | test | Robert Kanter | Robert Kanter |
| [HADOOP-15547](https://issues.apache.org/jira/browse/HADOOP-15547) | WASB: improve listStatus performance |  Major | fs/azure | Thomas Marqardt | Thomas Marqardt |
| [HDFS-13857](https://issues.apache.org/jira/browse/HDFS-13857) | RBF: Choose to enable the default nameservice to read/write files |  Major | federation, hdfs | yanghuafeng | yanghuafeng |
| [HDFS-13812](https://issues.apache.org/jira/browse/HDFS-13812) | Fix the inconsistent default refresh interval on Caching documentation |  Trivial | documentation | David Mollitor | Hrishikesh Gadre |
| [HADOOP-15657](https://issues.apache.org/jira/browse/HADOOP-15657) | Registering MutableQuantiles via Metric annotation |  Major | metrics | Sushil Ks | Sushil Ks |
| [HDFS-13902](https://issues.apache.org/jira/browse/HDFS-13902) |  Add JMX, conf and stacks menus to the datanode page |  Minor | datanode | fengchuang | fengchuang |
| [HADOOP-15726](https://issues.apache.org/jira/browse/HADOOP-15726) | Create utility to limit frequency of log statements |  Major | common, util | Erik Krogen | Erik Krogen |
| [YARN-7974](https://issues.apache.org/jira/browse/YARN-7974) | Allow updating application tracking url after registration |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-8750](https://issues.apache.org/jira/browse/YARN-8750) | Refactor TestQueueMetrics |  Minor | resourcemanager | Szilard Nemeth | Szilard Nemeth |
| [YARN-8896](https://issues.apache.org/jira/browse/YARN-8896) | Limit the maximum number of container assignments per heartbeat |  Major | . | Weiwei Yang | Zhankun Tang |
| [HADOOP-15804](https://issues.apache.org/jira/browse/HADOOP-15804) | upgrade to commons-compress 1.18 |  Major | . | PJ Fanning | Akira Ajisaka |
| [YARN-8915](https://issues.apache.org/jira/browse/YARN-8915) | Update the doc about the default value of "maximum-container-assignments" for capacity scheduler |  Minor | . | Zhankun Tang | Zhankun Tang |
| [YARN-7225](https://issues.apache.org/jira/browse/YARN-7225) | Add queue and partition info to RM audit log |  Major | resourcemanager | Jonathan Hung | Eric Payne |
| [HADOOP-15919](https://issues.apache.org/jira/browse/HADOOP-15919) | AliyunOSS: Enable Yarn to use OSS |  Major | fs/oss | wujinhu | wujinhu |
| [HADOOP-15943](https://issues.apache.org/jira/browse/HADOOP-15943) | AliyunOSS: add missing owner & group attributes for oss FileStatus |  Major | fs/oss | wujinhu | wujinhu |
| [YARN-9036](https://issues.apache.org/jira/browse/YARN-9036) | Escape newlines in health report in YARN UI |  Major | . | Jonathan Hung | Keqiu Hu |
| [YARN-9085](https://issues.apache.org/jira/browse/YARN-9085) | Add Guaranteed and MaxCapacity to CSQueueMetrics |  Major | . | Jonathan Hung | Jonathan Hung |
| [HDFS-14171](https://issues.apache.org/jira/browse/HDFS-14171) | Performance improvement in Tailing EditLog |  Major | namenode | Kenneth Yang | Kenneth Yang |
| [HADOOP-15481](https://issues.apache.org/jira/browse/HADOOP-15481) | Emit FairCallQueue stats as metrics |  Major | metrics, rpc-server | Erik Krogen | Christopher Gregorian |
| [HADOOP-15617](https://issues.apache.org/jira/browse/HADOOP-15617) | Node.js and npm package loading in the Dockerfile failing on branch-2 |  Major | build | Allen Wittenauer | Akhil PB |
| [HADOOP-16089](https://issues.apache.org/jira/browse/HADOOP-16089) | AliyunOSS: update oss-sdk version to 3.4.1 |  Major | fs/oss | wujinhu | wujinhu |
| [YARN-7171](https://issues.apache.org/jira/browse/YARN-7171) | RM UI should sort memory / cores numerically |  Major | . | Eric Maynard | Ahmed Hussein |
| [YARN-9282](https://issues.apache.org/jira/browse/YARN-9282) | Typo in javadoc of class LinuxContainerExecutor: hadoop.security.authetication should be 'authentication' |  Trivial | . | Szilard Nemeth | Charan Hebri |
| [HADOOP-16126](https://issues.apache.org/jira/browse/HADOOP-16126) | ipc.Client.stop() may sleep too long to wait for all connections |  Major | ipc | Tsz-wo Sze | Tsz-wo Sze |
| [HDFS-14247](https://issues.apache.org/jira/browse/HDFS-14247) | Repeat adding node description into network topology |  Minor | datanode | HuangTao | HuangTao |
| [YARN-9150](https://issues.apache.org/jira/browse/YARN-9150) | Making TimelineSchemaCreator support different backends for Timeline Schema Creation in ATSv2 |  Major | ATSv2 | Sushil Ks | Sushil Ks |
| [HDFS-14366](https://issues.apache.org/jira/browse/HDFS-14366) | Improve HDFS append performance |  Major | hdfs | Chao Sun | Chao Sun |
| [HDFS-14205](https://issues.apache.org/jira/browse/HDFS-14205) | Backport HDFS-6440 to branch-2 |  Major | . | Chen Liang | Chao Sun |
| [HDFS-14391](https://issues.apache.org/jira/browse/HDFS-14391) | Backport HDFS-9659 to branch-2 |  Minor | . | Chao Sun | Chao Sun |
| [HDFS-14415](https://issues.apache.org/jira/browse/HDFS-14415) | Backport HDFS-13799 to branch-2 |  Trivial | . | Chao Sun | Chao Sun |
| [HDFS-14432](https://issues.apache.org/jira/browse/HDFS-14432) | dfs.datanode.shared.file.descriptor.paths duplicated in hdfs-default.xml |  Minor | hdfs | puleya7 | puleya7 |
| [YARN-9529](https://issues.apache.org/jira/browse/YARN-9529) | Log correct cpu controller path on error while initializing CGroups. |  Major | nodemanager | Jonathan Hung | Jonathan Hung |
| [HDFS-14502](https://issues.apache.org/jira/browse/HDFS-14502) | keepResults option in NNThroughputBenchmark should call saveNamespace() |  Major | benchmarks, hdfs | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-16323](https://issues.apache.org/jira/browse/HADOOP-16323) | https everywhere in Maven settings |  Minor | build | Akira Ajisaka | Akira Ajisaka |
| [YARN-9563](https://issues.apache.org/jira/browse/YARN-9563) | Resource report REST API could return NaN or Inf |  Minor | . | Ahmed Hussein | Ahmed Hussein |
| [HDFS-14513](https://issues.apache.org/jira/browse/HDFS-14513) | FSImage which is saving should be clean while NameNode shutdown |  Major | namenode | Xiaoqiao He | Xiaoqiao He |
| [HADOOP-16369](https://issues.apache.org/jira/browse/HADOOP-16369) | Fix zstandard shortname misspelled as zts |  Major | . | Jonathan Turner Eagles | Jonathan Turner Eagles |
| [HADOOP-16359](https://issues.apache.org/jira/browse/HADOOP-16359) | Bundle ZSTD native in branch-2 |  Major | native | Chao Sun | Chao Sun |
| [HADOOP-15253](https://issues.apache.org/jira/browse/HADOOP-15253) | Should update maxQueueSize when refresh call queue |  Minor | . | Tao Jie | Tao Jie |
| [HADOOP-16266](https://issues.apache.org/jira/browse/HADOOP-16266) | Add more fine-grained processing time metrics to the RPC layer |  Minor | ipc | Christopher Gregorian | Erik Krogen |
| [HDFS-13694](https://issues.apache.org/jira/browse/HDFS-13694) | Making md5 computing being in parallel with image loading |  Major | . | zhouyingchao | Lisheng Sun |
| [HDFS-14632](https://issues.apache.org/jira/browse/HDFS-14632) | Reduce useless #getNumLiveDataNodes call in SafeModeMonitor |  Major | namenode | Xiaoqiao He | Xiaoqiao He |
| [HDFS-14547](https://issues.apache.org/jira/browse/HDFS-14547) | DirectoryWithQuotaFeature.quota costs additional memory even the storage type quota is not set. |  Major | . | Jinglun | Jinglun |
| [HDFS-14697](https://issues.apache.org/jira/browse/HDFS-14697) | Backport HDFS-14513 to branch-2 |  Minor | namenode | Xiaoqiao He | Xiaoqiao He |
| [YARN-8045](https://issues.apache.org/jira/browse/YARN-8045) | Reduce log output from container status calls |  Major | . | Shane Kumpf | Craig Condit |
| [HDFS-14313](https://issues.apache.org/jira/browse/HDFS-14313) | Get hdfs used space from FsDatasetImpl#volumeMap#ReplicaInfo in memory  instead of df/du |  Major | datanode, performance | Lisheng Sun | Lisheng Sun |
| [HDFS-14696](https://issues.apache.org/jira/browse/HDFS-14696) | Backport HDFS-11273 to branch-2 (Move TransferFsImage#doGetUrl function to a Util class) |  Major | . | Siyao Meng | Siyao Meng |
| [HDFS-14370](https://issues.apache.org/jira/browse/HDFS-14370) | Edit log tailing fast-path should allow for backoff |  Major | namenode, qjm | Erik Krogen | Erik Krogen |
| [YARN-9442](https://issues.apache.org/jira/browse/YARN-9442) | container working directory has group read permissions |  Minor | yarn | Jim Brennan | Jim Brennan |
| [HADOOP-16459](https://issues.apache.org/jira/browse/HADOOP-16459) | Backport [HADOOP-16266] "Add more fine-grained processing time metrics to the RPC layer" to branch-2 |  Major | . | Erik Krogen | Erik Krogen |
| [HDFS-14707](https://issues.apache.org/jira/browse/HDFS-14707) |  Add JAVA\_LIBRARY\_PATH to HTTPFS startup options in branch-2 |  Major | httpfs | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-14723](https://issues.apache.org/jira/browse/HDFS-14723) | Add helper method FSNamesystem#setBlockManagerForTesting() in branch-2 |  Blocker | namenode | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-14276](https://issues.apache.org/jira/browse/HDFS-14276) | [SBN read] Reduce tailing overhead |  Major | ha, namenode | Wei-Chiu Chuang | Ayush Saxena |
| [HDFS-14617](https://issues.apache.org/jira/browse/HDFS-14617) | Improve fsimage load time by writing sub-sections to the fsimage index |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [YARN-9756](https://issues.apache.org/jira/browse/YARN-9756) | Create metric that sums total memory/vcores preempted per round |  Major | capacity scheduler | Eric Payne | Manikandan R |
| [HDFS-14633](https://issues.apache.org/jira/browse/HDFS-14633) | The StorageType quota and consume in QuotaFeature is not handled for rename |  Major | . | Jinglun | Jinglun |
| [HADOOP-16439](https://issues.apache.org/jira/browse/HADOOP-16439) | Upgrade bundled Tomcat in branch-2 |  Major | httpfs, kms | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-9810](https://issues.apache.org/jira/browse/YARN-9810) | Add queue capacity/maxcapacity percentage metrics |  Major | . | Jonathan Hung | Shubham Gupta |
| [YARN-9763](https://issues.apache.org/jira/browse/YARN-9763) | Print application tags in application summary |  Major | . | Jonathan Hung | Manoj Kumar |
| [YARN-9764](https://issues.apache.org/jira/browse/YARN-9764) | Print application submission context label in application summary |  Major | . | Jonathan Hung | Manoj Kumar |
| [HADOOP-16530](https://issues.apache.org/jira/browse/HADOOP-16530) | Update xercesImpl in branch-2 |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-9824](https://issues.apache.org/jira/browse/YARN-9824) | Fall back to configured queue ordering policy class name |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-9825](https://issues.apache.org/jira/browse/YARN-9825) | Changes for initializing placement rules with ResourceScheduler in branch-2 |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-9762](https://issues.apache.org/jira/browse/YARN-9762) | Add submission context label to audit logs |  Major | . | Jonathan Hung | Manoj Kumar |
| [HDFS-14667](https://issues.apache.org/jira/browse/HDFS-14667) | Backport [HDFS-14403] "Cost-based FairCallQueue" to branch-2 |  Major | . | Erik Krogen | Erik Krogen |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-6896](https://issues.apache.org/jira/browse/MAPREDUCE-6896) | Document wrong spelling in usage of MapredTestDriver tools. |  Major | documentation | LiXin Ge | LiXin Ge |
| [HDFS-12052](https://issues.apache.org/jira/browse/HDFS-12052) | Set SWEBHDFS delegation token kind when ssl is enabled in HttpFS |  Major | httpfs, webhdfs | Zoran Dimitrijevic | Zoran Dimitrijevic |
| [HDFS-12318](https://issues.apache.org/jira/browse/HDFS-12318) | Fix IOException condition for openInfo in DFSInputStream |  Major | . | legend | legend |
| [HDFS-12614](https://issues.apache.org/jira/browse/HDFS-12614) | FSPermissionChecker#getINodeAttrs() throws NPE when INodeAttributesProvider configured |  Major | . | Manoj Govindassamy | Manoj Govindassamy |
| [YARN-7396](https://issues.apache.org/jira/browse/YARN-7396) | NPE when accessing container logs due to null dirsHandler |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-7370](https://issues.apache.org/jira/browse/YARN-7370) | Preemption properties should be refreshable |  Major | capacity scheduler, scheduler preemption | Eric Payne | Gergely Novák |
| [YARN-7428](https://issues.apache.org/jira/browse/YARN-7428) | Add containerId to Localizer failed logs |  Minor | nodemanager | Prabhu Joseph | Prabhu Joseph |
| [YARN-7410](https://issues.apache.org/jira/browse/YARN-7410) | Cleanup FixedValueResource to avoid dependency to ResourceUtils |  Major | resourcemanager | Sunil G | Wangda Tan |
| [HDFS-12783](https://issues.apache.org/jira/browse/HDFS-12783) | [branch-2] "dfsrouter" should use hdfsScript |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-12788](https://issues.apache.org/jira/browse/HDFS-12788) | Reset the upload button when file upload fails |  Critical | ui, webhdfs | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-7469](https://issues.apache.org/jira/browse/YARN-7469) | Capacity Scheduler Intra-queue preemption: User can starve if newest app is exactly at user limit |  Major | capacity scheduler, yarn | Eric Payne | Eric Payne |
| [HADOOP-14982](https://issues.apache.org/jira/browse/HADOOP-14982) | Clients using FailoverOnNetworkExceptionRetry can go into a loop if they're used without authenticating with kerberos in HA env |  Major | common | Peter Bacsko | Peter Bacsko |
| [YARN-7489](https://issues.apache.org/jira/browse/YARN-7489) | ConcurrentModificationException in RMAppImpl#getRMAppMetrics |  Major | capacityscheduler | Tao Yang | Tao Yang |
| [YARN-7525](https://issues.apache.org/jira/browse/YARN-7525) | Incorrect query parameters in cluster nodes REST API document |  Minor | documentation | Tao Yang | Tao Yang |
| [HDFS-12813](https://issues.apache.org/jira/browse/HDFS-12813) | RequestHedgingProxyProvider can hide Exception thrown from the Namenode for proxy size of 1 |  Major | ha | Mukul Kumar Singh | Mukul Kumar Singh |
| [HADOOP-15045](https://issues.apache.org/jira/browse/HADOOP-15045) | ISA-L build options are documented in branch-2 |  Major | build, documentation | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-15067](https://issues.apache.org/jira/browse/HADOOP-15067) | GC time percentage reported in JvmMetrics should be a gauge, not counter |  Major | . | Misha Dmitriev | Misha Dmitriev |
| [YARN-7363](https://issues.apache.org/jira/browse/YARN-7363) | ContainerLocalizer doesn't have a valid log4j config when using LinuxContainerExecutor |  Major | nodemanager | Yufei Gu | Yufei Gu |
| [HDFS-12754](https://issues.apache.org/jira/browse/HDFS-12754) | Lease renewal can hit a deadlock |  Major | . | Kuhu Shukla | Kuhu Shukla |
| [HDFS-12832](https://issues.apache.org/jira/browse/HDFS-12832) | INode.getFullPathName may throw ArrayIndexOutOfBoundsException lead to NameNode exit |  Critical | namenode | DENG FEI | Konstantin Shvachko |
| [HDFS-11754](https://issues.apache.org/jira/browse/HDFS-11754) | Make FsServerDefaults cache configurable. |  Minor | . | Rushabh Shah | Mikhail Erofeev |
| [HADOOP-15042](https://issues.apache.org/jira/browse/HADOOP-15042) | Azure PageBlobInputStream.skip() can return negative value when numberOfPagesRemaining is 0 |  Minor | fs/azure | Rajesh Balamohan | Rajesh Balamohan |
| [HDFS-12638](https://issues.apache.org/jira/browse/HDFS-12638) | Delete copy-on-truncate block along with the original block, when deleting a file being truncated |  Blocker | hdfs | Jiandan Yang | Konstantin Shvachko |
| [YARN-4813](https://issues.apache.org/jira/browse/YARN-4813) | TestRMWebServicesDelegationTokenAuthentication.testDoAs fails intermittently |  Major | resourcemanager | Daniel Templeton | Gergo Repas |
| [MAPREDUCE-5124](https://issues.apache.org/jira/browse/MAPREDUCE-5124) | AM lacks flow control for task events |  Major | mr-am | Jason Darrell Lowe | Peter Bacsko |
| [YARN-7455](https://issues.apache.org/jira/browse/YARN-7455) | quote\_and\_append\_arg can overflow buffer |  Major | nodemanager | Jason Darrell Lowe | Jim Brennan |
| [YARN-7594](https://issues.apache.org/jira/browse/YARN-7594) | TestNMWebServices#testGetNMResourceInfo fails on trunk |  Major | nodemanager, webapp | Gergely Novák | Gergely Novák |
| [YARN-5594](https://issues.apache.org/jira/browse/YARN-5594) | Handle old RMDelegationToken format when recovering RM |  Major | resourcemanager | Tatyana But | Robert Kanter |
| [HADOOP-14985](https://issues.apache.org/jira/browse/HADOOP-14985) | Remove subversion related code from VersionInfoMojo.java |  Minor | build | Akira Ajisaka | Ajay Kumar |
| [HDFS-11751](https://issues.apache.org/jira/browse/HDFS-11751) | DFSZKFailoverController daemon exits with wrong status code |  Major | auto-failover | Doris Gu | Bharat Viswanadham |
| [HDFS-12889](https://issues.apache.org/jira/browse/HDFS-12889) | Router UI is missing robots.txt file |  Major | . | Bharat Viswanadham | Bharat Viswanadham |
| [YARN-7607](https://issues.apache.org/jira/browse/YARN-7607) | Remove the trailing duplicated timestamp in container diagnostics message |  Minor | nodemanager | Weiwei Yang | Weiwei Yang |
| [HADOOP-15080](https://issues.apache.org/jira/browse/HADOOP-15080) | Aliyun OSS: update oss sdk from 2.8.1 to 2.8.3 to remove its dependency on Cat-x "json-lib" |  Blocker | fs/oss | Christopher Douglas | Sammi Chen |
| [YARN-7608](https://issues.apache.org/jira/browse/YARN-7608) | Incorrect sTarget column causing DataTable warning on RM application and scheduler web page |  Major | resourcemanager, webapp | Weiwei Yang | Gergely Novák |
| [HDFS-12833](https://issues.apache.org/jira/browse/HDFS-12833) | Distcp : Update the usage of delete option for dependency with update and overwrite option |  Minor | distcp, hdfs | Harshakiran Reddy | usharani |
| [YARN-7647](https://issues.apache.org/jira/browse/YARN-7647) | NM print inappropriate error log when node-labels is enabled |  Minor | . | Yang Wang | Yang Wang |
| [HDFS-12907](https://issues.apache.org/jira/browse/HDFS-12907) | Allow read-only access to reserved raw for non-superusers |  Major | namenode | Daryn Sharp | Rushabh Shah |
| [HDFS-12881](https://issues.apache.org/jira/browse/HDFS-12881) | Output streams closed with IOUtils suppressing write errors |  Major | . | Jason Darrell Lowe | Ajay Kumar |
| [YARN-7595](https://issues.apache.org/jira/browse/YARN-7595) | Container launching code suppresses close exceptions after writes |  Major | nodemanager | Jason Darrell Lowe | Jim Brennan |
| [HADOOP-15085](https://issues.apache.org/jira/browse/HADOOP-15085) | Output streams closed with IOUtils suppressing write errors |  Major | . | Jason Darrell Lowe | Jim Brennan |
| [HADOOP-15123](https://issues.apache.org/jira/browse/HADOOP-15123) | KDiag tries to load krb5.conf from KRB5CCNAME instead of KRB5\_CONFIG |  Minor | security | Vipin Rathor | Vipin Rathor |
| [YARN-7661](https://issues.apache.org/jira/browse/YARN-7661) | NodeManager metrics return wrong value after update node resource |  Major | . | Yang Wang | Yang Wang |
| [HDFS-12347](https://issues.apache.org/jira/browse/HDFS-12347) | TestBalancerRPCDelay#testBalancerRPCDelay fails very frequently |  Critical | test | Xiao Chen | Bharat Viswanadham |
| [YARN-7662](https://issues.apache.org/jira/browse/YARN-7662) | [Atsv2] Define new set of configurations for reader and collectors to bind. |  Major | . | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-7674](https://issues.apache.org/jira/browse/YARN-7674) | Update Timeline Reader web app address in UI2 |  Major | . | Rohith Sharma K S | Sunil G |
| [YARN-7542](https://issues.apache.org/jira/browse/YARN-7542) | Fix issue that causes some Running Opportunistic Containers to be recovered as PAUSED |  Major | . | Arun Suresh | Sampada Dehankar |
| [HADOOP-15143](https://issues.apache.org/jira/browse/HADOOP-15143) | NPE due to Invalid KerberosTicket in UGI |  Major | . | Jitendra Nath Pandey | Mukul Kumar Singh |
| [YARN-7692](https://issues.apache.org/jira/browse/YARN-7692) | Skip validating priority acls while recovering applications |  Blocker | resourcemanager | Charan Hebri | Sunil G |
| [MAPREDUCE-7028](https://issues.apache.org/jira/browse/MAPREDUCE-7028) | Concurrent task progress updates causing NPE in Application Master |  Blocker | mr-am | Gergo Repas | Gergo Repas |
| [YARN-7619](https://issues.apache.org/jira/browse/YARN-7619) | Max AM Resource value in Capacity Scheduler UI has to be refreshed for every user |  Major | capacity scheduler, yarn | Eric Payne | Eric Payne |
| [YARN-7699](https://issues.apache.org/jira/browse/YARN-7699) | queueUsagePercentage is coming as INF for getApp REST api call |  Major | webapp | Sunil G | Sunil G |
| [HDFS-12985](https://issues.apache.org/jira/browse/HDFS-12985) | NameNode crashes during restart after an OpenForWrite file present in the Snapshot got deleted |  Major | hdfs | Manoj Govindassamy | Manoj Govindassamy |
| [YARN-4227](https://issues.apache.org/jira/browse/YARN-4227) | Ignore expired containers from removed nodes in FairScheduler |  Critical | fairscheduler | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [YARN-7508](https://issues.apache.org/jira/browse/YARN-7508) | NPE in FiCaSchedulerApp when debug log enabled in async-scheduling mode |  Major | capacityscheduler | Tao Yang | Tao Yang |
| [YARN-7663](https://issues.apache.org/jira/browse/YARN-7663) | RMAppImpl:Invalid event: START at KILLED |  Major | resourcemanager | lujie | lujie |
| [YARN-6948](https://issues.apache.org/jira/browse/YARN-6948) | Invalid event: ATTEMPT\_ADDED at FINAL\_SAVING |  Major | yarn | lujie | lujie |
| [HADOOP-15060](https://issues.apache.org/jira/browse/HADOOP-15060) | TestShellBasedUnixGroupsMapping.testFiniteGroupResolutionTime flaky |  Major | . | Miklos Szegedi | Miklos Szegedi |
| [YARN-7735](https://issues.apache.org/jira/browse/YARN-7735) | Fix typo in YARN documentation |  Minor | documentation | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-7727](https://issues.apache.org/jira/browse/YARN-7727) | Incorrect log levels in few logs with QueuePriorityContainerCandidateSelector |  Minor | yarn | Prabhu Joseph | Prabhu Joseph |
| [HDFS-11915](https://issues.apache.org/jira/browse/HDFS-11915) | Sync rbw dir on the first hsync() to avoid file lost on power failure |  Critical | . | Kanaka Kumar Avvaru | Vinayakumar B |
| [YARN-7705](https://issues.apache.org/jira/browse/YARN-7705) | Create the container log directory with correct sticky bit in C code |  Major | nodemanager | Yufei Gu | Yufei Gu |
| [HDFS-9049](https://issues.apache.org/jira/browse/HDFS-9049) | Make Datanode Netty reverse proxy port to be configurable |  Major | datanode | Vinayakumar B | Vinayakumar B |
| [YARN-7758](https://issues.apache.org/jira/browse/YARN-7758) | Add an additional check to the validity of container and application ids passed to container-executor |  Major | nodemanager | Miklos Szegedi | Yufei Gu |
| [HADOOP-15150](https://issues.apache.org/jira/browse/HADOOP-15150) | in FsShell, UGI params should be overidden through env vars(-D arg) |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-15181](https://issues.apache.org/jira/browse/HADOOP-15181) | Typo in SecureMode.md |  Trivial | documentation | Masahiro Tanaka | Masahiro Tanaka |
| [YARN-7806](https://issues.apache.org/jira/browse/YARN-7806) | Distributed Shell should use timeline async api's |  Major | distributed-shell | Sumana Sathish | Rohith Sharma K S |
| [HADOOP-15121](https://issues.apache.org/jira/browse/HADOOP-15121) | Encounter NullPointerException when using DecayRpcScheduler |  Major | . | Tao Jie | Tao Jie |
| [MAPREDUCE-7015](https://issues.apache.org/jira/browse/MAPREDUCE-7015) | Possible race condition in JHS if the job is not loaded |  Major | jobhistoryserver | Peter Bacsko | Peter Bacsko |
| [YARN-7737](https://issues.apache.org/jira/browse/YARN-7737) | prelaunch.err file not found exception on container failure |  Major | . | Jonathan Hung | Keqiu Hu |
| [HDFS-13063](https://issues.apache.org/jira/browse/HDFS-13063) | Fix the incorrect spelling in HDFSHighAvailabilityWithQJM.md |  Trivial | documentation | Jianfei Jiang | Jianfei Jiang |
| [YARN-7102](https://issues.apache.org/jira/browse/YARN-7102) | NM heartbeat stuck when responseId overflows MAX\_INT |  Critical | . | Botong Huang | Botong Huang |
| [MAPREDUCE-7041](https://issues.apache.org/jira/browse/MAPREDUCE-7041) | MR should not try to clean up at first job attempt |  Major | . | Takanobu Asanuma | Gergo Repas |
| [MAPREDUCE-7020](https://issues.apache.org/jira/browse/MAPREDUCE-7020) | Task timeout in uber mode can crash AM |  Major | mr-am | Akira Ajisaka | Peter Bacsko |
| [YARN-7765](https://issues.apache.org/jira/browse/YARN-7765) | [Atsv2] GSSException: No valid credentials provided - Failed to find any Kerberos tgt thrown by Timelinev2Client & HBaseClient in NM |  Blocker | . | Sumana Sathish | Rohith Sharma K S |
| [HDFS-12974](https://issues.apache.org/jira/browse/HDFS-12974) | Exception message is not printed when creating an encryption zone fails with AuthorizationException |  Minor | encryption | fang zhenyi | fang zhenyi |
| [YARN-7698](https://issues.apache.org/jira/browse/YARN-7698) | A misleading variable's name in ApplicationAttemptEventDispatcher |  Minor | resourcemanager | Jinjiang Ling | Jinjiang Ling |
| [HDFS-12528](https://issues.apache.org/jira/browse/HDFS-12528) | Add an option to not disable short-circuit reads on failures |  Major | hdfs-client, performance | Andre Araujo | Xiao Chen |
| [HDFS-13100](https://issues.apache.org/jira/browse/HDFS-13100) | Handle IllegalArgumentException when GETSERVERDEFAULTS is not implemented in webhdfs. |  Critical | hdfs, webhdfs | Yongjun Zhang | Yongjun Zhang |
| [YARN-7849](https://issues.apache.org/jira/browse/YARN-7849) | TestMiniYarnClusterNodeUtilization#testUpdateNodeUtilization fails due to heartbeat sync error |  Major | test | Jason Darrell Lowe | Botong Huang |
| [YARN-7801](https://issues.apache.org/jira/browse/YARN-7801) | AmFilterInitializer should addFilter after fill all parameters |  Critical | . | Sumana Sathish | Wangda Tan |
| [YARN-7890](https://issues.apache.org/jira/browse/YARN-7890) | NPE during container relaunch |  Major | . | Billie Rinaldi | Jason Darrell Lowe |
| [HDFS-13115](https://issues.apache.org/jira/browse/HDFS-13115) | In getNumUnderConstructionBlocks(), ignore the inodeIds for which the inodes have been deleted |  Major | . | Yongjun Zhang | Yongjun Zhang |
| [HDFS-12935](https://issues.apache.org/jira/browse/HDFS-12935) | Get ambiguous result for DFSAdmin command in HA mode when only one namenode is up |  Major | tools | Jianfei Jiang | Jianfei Jiang |
| [HDFS-13120](https://issues.apache.org/jira/browse/HDFS-13120) | Snapshot diff could be corrupted after concat |  Major | namenode, snapshots | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-10453](https://issues.apache.org/jira/browse/HDFS-10453) | ReplicationMonitor thread could stuck for long time due to the race between replication and delete of same file in a large cluster. |  Major | namenode | Xiaoqiao He | Xiaoqiao He |
| [HDFS-8693](https://issues.apache.org/jira/browse/HDFS-8693) | refreshNamenodes does not support adding a new standby to a running DN |  Critical | datanode, ha | Jian Fang | Ajith S |
| [MAPREDUCE-7052](https://issues.apache.org/jira/browse/MAPREDUCE-7052) | TestFixedLengthInputFormat#testFormatCompressedIn is flaky |  Major | client, test | Peter Bacsko | Peter Bacsko |
| [HDFS-13112](https://issues.apache.org/jira/browse/HDFS-13112) | Token expiration edits may cause log corruption or deadlock |  Critical | namenode | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-7053](https://issues.apache.org/jira/browse/MAPREDUCE-7053) | Timed out tasks can fail to produce thread dump |  Major | . | Jason Darrell Lowe | Jason Darrell Lowe |
| [HADOOP-15206](https://issues.apache.org/jira/browse/HADOOP-15206) | BZip2 drops and duplicates records when input split size is small |  Major | . | Aki Tanaka | Aki Tanaka |
| [YARN-7937](https://issues.apache.org/jira/browse/YARN-7937) | Fix http method name in Cluster Application Timeout Update API example request |  Minor | docs, documentation | Charan Hebri | Charan Hebri |
| [YARN-7947](https://issues.apache.org/jira/browse/YARN-7947) | Capacity Scheduler intra-queue preemption can NPE for non-schedulable apps |  Major | capacity scheduler, scheduler preemption | Eric Payne | Eric Payne |
| [YARN-7945](https://issues.apache.org/jira/browse/YARN-7945) | Java Doc error in UnmanagedAMPoolManager for branch-2 |  Major | . | Rohith Sharma K S | Botong Huang |
| [HADOOP-14903](https://issues.apache.org/jira/browse/HADOOP-14903) | Add json-smart explicitly to pom.xml |  Major | common | Ray Chiang | Ray Chiang |
| [HADOOP-15236](https://issues.apache.org/jira/browse/HADOOP-15236) | Fix typo in RequestHedgingProxyProvider and RequestHedgingRMFailoverProxyProvider |  Trivial | documentation | Akira Ajisaka | Gabor Bota |
| [MAPREDUCE-7027](https://issues.apache.org/jira/browse/MAPREDUCE-7027) | HadoopArchiveLogs shouldn't delete the original logs if the HAR creation fails |  Critical | harchive | Gergely Novák | Gergely Novák |
| [HDFS-12781](https://issues.apache.org/jira/browse/HDFS-12781) | After Datanode down, In Namenode UI Datanode tab is throwing warning message. |  Major | datanode | Harshakiran Reddy | Brahma Reddy Battula |
| [HDFS-12070](https://issues.apache.org/jira/browse/HDFS-12070) | Failed block recovery leaves files open indefinitely and at risk for data loss |  Major | . | Daryn Sharp | Kihwal Lee |
| [HADOOP-15251](https://issues.apache.org/jira/browse/HADOOP-15251) | Backport HADOOP-13514 (surefire upgrade) to branch-2 |  Major | test | Christopher Douglas | Christopher Douglas |
| [HDFS-13194](https://issues.apache.org/jira/browse/HDFS-13194) | CachePool permissions incorrectly checked |  Major | . | Yiqun Lin | Jianfei Jiang |
| [HADOOP-15276](https://issues.apache.org/jira/browse/HADOOP-15276) | branch-2 site not building after ADL troubleshooting doc added |  Major | documentation | Steve Loughran | Steve Loughran |
| [YARN-7835](https://issues.apache.org/jira/browse/YARN-7835) | [Atsv2] Race condition in NM while publishing events if second attempt is launched on the same node |  Critical | . | Rohith Sharma K S | Rohith Sharma K S |
| [HADOOP-15275](https://issues.apache.org/jira/browse/HADOOP-15275) | Incorrect javadoc for return type of RetryPolicy#shouldRetry |  Minor | documentation | Nanda kumar | Nanda kumar |
| [YARN-7511](https://issues.apache.org/jira/browse/YARN-7511) | NPE in ContainerLocalizer when localization failed for running container |  Major | nodemanager | Tao Yang | Tao Yang |
| [MAPREDUCE-7023](https://issues.apache.org/jira/browse/MAPREDUCE-7023) | TestHadoopArchiveLogs.testCheckFilesAndSeedApps fails on rerun |  Minor | test | Gergely Novák | Gergely Novák |
| [HADOOP-15283](https://issues.apache.org/jira/browse/HADOOP-15283) | Upgrade from findbugs 3.0.1 to spotbugs 3.1.2 in branch-2 to fix docker image build |  Major | . | Xiao Chen | Akira Ajisaka |
| [HADOOP-15286](https://issues.apache.org/jira/browse/HADOOP-15286) | Remove unused imports from TestKMSWithZK.java |  Minor | test | Akira Ajisaka | Ajay Kumar |
| [HDFS-13040](https://issues.apache.org/jira/browse/HDFS-13040) | Kerberized inotify client fails despite kinit properly |  Major | namenode | Wei-Chiu Chuang | Xiao Chen |
| [YARN-7736](https://issues.apache.org/jira/browse/YARN-7736) | Fix itemization in YARN federation document |  Minor | documentation | Akira Ajisaka | Sen Zhao |
| [HDFS-13164](https://issues.apache.org/jira/browse/HDFS-13164) | File not closed if streamer fail with DSQuotaExceededException |  Major | hdfs-client | Xiao Chen | Xiao Chen |
| [HDFS-13109](https://issues.apache.org/jira/browse/HDFS-13109) | Support fully qualified hdfs path in EZ commands |  Major | hdfs | Hanisha Koneru | Hanisha Koneru |
| [MAPREDUCE-6930](https://issues.apache.org/jira/browse/MAPREDUCE-6930) | mapreduce.map.cpu.vcores and mapreduce.reduce.cpu.vcores are both present twice in mapred-default.xml |  Major | mrv2 | Daniel Templeton | Sen Zhao |
| [HDFS-10618](https://issues.apache.org/jira/browse/HDFS-10618) | TestPendingReconstruction#testPendingAndInvalidate is flaky due to race condition |  Major | . | Eric Badger | Eric Badger |
| [HDFS-10803](https://issues.apache.org/jira/browse/HDFS-10803) | TestBalancerWithMultipleNameNodes#testBalancing2OutOf3Blockpools fails intermittently due to no free space available |  Major | . | Yiqun Lin | Yiqun Lin |
| [HDFS-12156](https://issues.apache.org/jira/browse/HDFS-12156) | TestFSImage fails without -Pnative |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [HDFS-13261](https://issues.apache.org/jira/browse/HDFS-13261) | Fix incorrect null value check |  Minor | hdfs | Jianfei Jiang | Jianfei Jiang |
| [HDFS-12886](https://issues.apache.org/jira/browse/HDFS-12886) | Ignore minReplication for block recovery |  Major | hdfs, namenode | Lukas Majercak | Lukas Majercak |
| [YARN-8039](https://issues.apache.org/jira/browse/YARN-8039) | Clean up log dir configuration in TestLinuxContainerExecutorWithMocks.testStartLocalizer |  Minor | . | Miklos Szegedi | Miklos Szegedi |
| [HDFS-13296](https://issues.apache.org/jira/browse/HDFS-13296) | GenericTestUtils generates paths with drive letter in Windows and fail webhdfs related test cases |  Major | . | Xiao Liang | Xiao Liang |
| [HDFS-13268](https://issues.apache.org/jira/browse/HDFS-13268) | TestWebHdfsFileContextMainOperations fails on Windows |  Major | . | Íñigo Goiri | Xiao Liang |
| [YARN-8054](https://issues.apache.org/jira/browse/YARN-8054) | Improve robustness of the LocalDirsHandlerService MonitoringTimerTask thread |  Major | . | Jonathan Turner Eagles | Jonathan Turner Eagles |
| [YARN-7873](https://issues.apache.org/jira/browse/YARN-7873) | Revert YARN-6078 |  Blocker | . | Billie Rinaldi | Billie Rinaldi |
| [HDFS-13195](https://issues.apache.org/jira/browse/HDFS-13195) | DataNode conf page  cannot display the current value after reconfig |  Minor | datanode | maobaolong | maobaolong |
| [YARN-8063](https://issues.apache.org/jira/browse/YARN-8063) | DistributedShellTimelinePlugin wrongly check for entityId instead of entityType |  Major | . | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-8068](https://issues.apache.org/jira/browse/YARN-8068) | Application Priority field causes NPE in app timeline publish when Hadoop 2.7 based clients to 2.8+ |  Blocker | yarn | Sunil G | Sunil G |
| [HADOOP-12862](https://issues.apache.org/jira/browse/HADOOP-12862) | LDAP Group Mapping over SSL can not specify trust store |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-15317](https://issues.apache.org/jira/browse/HADOOP-15317) | Improve NetworkTopology chooseRandom's loop |  Major | . | Xiao Chen | Xiao Chen |
| [HADOOP-15355](https://issues.apache.org/jira/browse/HADOOP-15355) | TestCommonConfigurationFields is broken by HADOOP-15312 |  Major | test | Konstantin Shvachko | LiXin Ge |
| [HDFS-13176](https://issues.apache.org/jira/browse/HDFS-13176) | WebHdfs file path gets truncated when having semicolon (;) inside |  Major | webhdfs | Zsolt Venczel | Zsolt Venczel |
| [HADOOP-15375](https://issues.apache.org/jira/browse/HADOOP-15375) | Branch-2 pre-commit failed to build docker image |  Major | . | Xiao Chen | Xiao Chen |
| [HADOOP-15357](https://issues.apache.org/jira/browse/HADOOP-15357) | Configuration.getPropsWithPrefix no longer does variable substitution |  Major | . | Jim Brennan | Jim Brennan |
| [MAPREDUCE-7062](https://issues.apache.org/jira/browse/MAPREDUCE-7062) | Update mapreduce.job.tags description for making use for ATSv2 purpose. |  Major | . | Charan Hebri | Charan Hebri |
| [YARN-8073](https://issues.apache.org/jira/browse/YARN-8073) | TimelineClientImpl doesn't honor yarn.timeline-service.versions configuration |  Major | . | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-6629](https://issues.apache.org/jira/browse/YARN-6629) | NPE occurred when container allocation proposal is applied but its resource requests are removed before |  Critical | . | Tao Yang | Tao Yang |
| [HDFS-13427](https://issues.apache.org/jira/browse/HDFS-13427) | Fix the section titles of transparent encryption document |  Minor | documentation | Akira Ajisaka | Akira Ajisaka |
| [YARN-7527](https://issues.apache.org/jira/browse/YARN-7527) | Over-allocate node resource in async-scheduling mode of CapacityScheduler |  Major | capacityscheduler | Tao Yang | Tao Yang |
| [HDFS-7101](https://issues.apache.org/jira/browse/HDFS-7101) | Potential null dereference in DFSck#doWork() |  Minor | . | Ted Yu | skrho |
| [YARN-8120](https://issues.apache.org/jira/browse/YARN-8120) | JVM can crash with SIGSEGV when exiting due to custom leveldb logger |  Major | nodemanager, resourcemanager | Jason Darrell Lowe | Jason Darrell Lowe |
| [YARN-8147](https://issues.apache.org/jira/browse/YARN-8147) | TestClientRMService#testGetApplications sporadically fails |  Major | test | Jason Darrell Lowe | Jason Darrell Lowe |
| [HADOOP-14970](https://issues.apache.org/jira/browse/HADOOP-14970) | MiniHadoopClusterManager doesn't respect lack of format option |  Minor | . | Erik Krogen | Erik Krogen |
| [YARN-8156](https://issues.apache.org/jira/browse/YARN-8156) | Increase the default value of yarn.timeline-service.app-collector.linger-period.ms |  Major | . | Rohith Sharma K S | Charan Hebri |
| [YARN-8165](https://issues.apache.org/jira/browse/YARN-8165) | Incorrect queue name logging in AbstractContainerAllocator |  Trivial | capacityscheduler | Weiwei Yang | Weiwei Yang |
| [YARN-8164](https://issues.apache.org/jira/browse/YARN-8164) | Fix a potential NPE in AbstractSchedulerPlanFollower |  Major | . | lujie | lujie |
| [HDFS-12828](https://issues.apache.org/jira/browse/HDFS-12828) | OIV ReverseXML Processor fails with escaped characters |  Critical | hdfs | Erik Krogen | Erik Krogen |
| [HADOOP-15180](https://issues.apache.org/jira/browse/HADOOP-15180) | branch-2 : daemon processes' sysout overwrites 'ulimit -a' in daemon's out file |  Minor | scripts | Ranith Sardar | Ranith Sardar |
| [HADOOP-15396](https://issues.apache.org/jira/browse/HADOOP-15396) | Some java source files are executable |  Minor | . | Akira Ajisaka | Shashikant Banerjee |
| [YARN-6827](https://issues.apache.org/jira/browse/YARN-6827) | [ATS1/1.5] NPE exception while publishing recovering applications into ATS during RM restart. |  Major | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-7786](https://issues.apache.org/jira/browse/YARN-7786) | NullPointerException while launching ApplicationMaster |  Major | . | lujie | lujie |
| [HDFS-13408](https://issues.apache.org/jira/browse/HDFS-13408) | MiniDFSCluster to support being built on randomized base directory |  Major | test | Xiao Liang | Xiao Liang |
| [HADOOP-15390](https://issues.apache.org/jira/browse/HADOOP-15390) | Yarn RM logs flooded by DelegationTokenRenewer trying to renew KMS tokens |  Critical | . | Xiao Chen | Xiao Chen |
| [HDFS-13336](https://issues.apache.org/jira/browse/HDFS-13336) | Test cases of TestWriteToReplica failed in windows |  Major | . | Xiao Liang | Xiao Liang |
| [YARN-7598](https://issues.apache.org/jira/browse/YARN-7598) | Document how to use classpath isolation for aux-services in YARN |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-8183](https://issues.apache.org/jira/browse/YARN-8183) | Fix ConcurrentModificationException inside RMAppAttemptMetrics#convertAtomicLongMaptoLongMap |  Critical | yarn | Sumana Sathish | Suma Shivaprasad |
| [HADOOP-15385](https://issues.apache.org/jira/browse/HADOOP-15385) | Many tests are failing in hadoop-distcp project in branch-2 |  Critical | tools/distcp | Rushabh Shah | Jason Darrell Lowe |
| [MAPREDUCE-7042](https://issues.apache.org/jira/browse/MAPREDUCE-7042) | Killed MR job data does not move to mapreduce.jobhistory.done-dir when ATS v2 is enabled |  Major | . | Yesha Vora | Xuan Gong |
| [YARN-8205](https://issues.apache.org/jira/browse/YARN-8205) | Application State is not updated to ATS if AM launching is delayed. |  Critical | . | Sumana Sathish | Rohith Sharma K S |
| [MAPREDUCE-7072](https://issues.apache.org/jira/browse/MAPREDUCE-7072) | mapred job -history prints duplicate counter in human output |  Major | client | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [YARN-8221](https://issues.apache.org/jira/browse/YARN-8221) | RMWebServices also need to honor yarn.resourcemanager.display.per-user-apps |  Major | webapp | Sunil G | Sunil G |
| [HDFS-13509](https://issues.apache.org/jira/browse/HDFS-13509) | Bug fix for breakHardlinks() of ReplicaInfo/LocalReplica, and fix TestFileAppend failures on Windows |  Major | . | Xiao Liang | Xiao Liang |
| [MAPREDUCE-7073](https://issues.apache.org/jira/browse/MAPREDUCE-7073) | Optimize TokenCache#obtainTokensForNamenodesInternal |  Major | . | Bibin Chundatt | Bibin Chundatt |
| [YARN-8025](https://issues.apache.org/jira/browse/YARN-8025) | UsersManangers#getComputedResourceLimitForActiveUsers throws NPE due to preComputedActiveUserLimit is empty |  Major | yarn | Jiandan Yang | Tao Yang |
| [YARN-8232](https://issues.apache.org/jira/browse/YARN-8232) | RMContainer lost queue name when RM HA happens |  Major | resourcemanager | Hu Ziqian | Hu Ziqian |
| [HDFS-13537](https://issues.apache.org/jira/browse/HDFS-13537) | TestHdfsHelper does not generate jceks path properly for relative path in Windows |  Major | . | Xiao Liang | Xiao Liang |
| [HADOOP-15446](https://issues.apache.org/jira/browse/HADOOP-15446) | WASB: PageBlobInputStream.skip breaks HBASE replication |  Major | fs/azure | Thomas Marqardt | Thomas Marqardt |
| [YARN-8244](https://issues.apache.org/jira/browse/YARN-8244) |  TestContainerSchedulerQueuing.testStartMultipleContainers failed |  Major | . | Miklos Szegedi | Jim Brennan |
| [HDFS-13586](https://issues.apache.org/jira/browse/HDFS-13586) | Fsync fails on directories on Windows |  Critical | datanode, hdfs | Lukas Majercak | Lukas Majercak |
| [HDFS-13590](https://issues.apache.org/jira/browse/HDFS-13590) | Backport HDFS-12378 to branch-2 |  Major | datanode, hdfs, test | Lukas Majercak | Lukas Majercak |
| [HADOOP-15478](https://issues.apache.org/jira/browse/HADOOP-15478) | WASB: hflush() and hsync() regression |  Major | fs/azure | Thomas Marqardt | Thomas Marqardt |
| [HADOOP-15450](https://issues.apache.org/jira/browse/HADOOP-15450) | Avoid fsync storm triggered by DiskChecker and handle disk full situation |  Blocker | . | Kihwal Lee | Arpit Agarwal |
| [HDFS-13601](https://issues.apache.org/jira/browse/HDFS-13601) | Optimize ByteString conversions in PBHelper |  Major | . | Andrew Wang | Andrew Wang |
| [HDFS-13588](https://issues.apache.org/jira/browse/HDFS-13588) | Fix TestFsDatasetImpl test failures on Windows |  Major | . | Xiao Liang | Xiao Liang |
| [YARN-8310](https://issues.apache.org/jira/browse/YARN-8310) | Handle old NMTokenIdentifier, AMRMTokenIdentifier, and ContainerTokenIdentifier formats |  Major | . | Robert Kanter | Robert Kanter |
| [YARN-8344](https://issues.apache.org/jira/browse/YARN-8344) | Missing nm.stop() in TestNodeManagerResync to fix testKillContainersOnResync |  Major | . | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [YARN-8327](https://issues.apache.org/jira/browse/YARN-8327) | Fix TestAggregatedLogFormat#testReadAcontainerLogs1 on Windows |  Major | log-aggregation | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [YARN-8346](https://issues.apache.org/jira/browse/YARN-8346) | Upgrading to 3.1 kills running containers with error "Opportunistic container queue is full" |  Blocker | . | Rohith Sharma K S | Jason Darrell Lowe |
| [HDFS-13611](https://issues.apache.org/jira/browse/HDFS-13611) | Unsafe use of Text as a ConcurrentHashMap key in PBHelperClient |  Major | . | Andrew Wang | Andrew Wang |
| [HDFS-13618](https://issues.apache.org/jira/browse/HDFS-13618) | Fix TestDataNodeFaultInjector test failures on Windows |  Major | test | Xiao Liang | Xiao Liang |
| [HADOOP-15473](https://issues.apache.org/jira/browse/HADOOP-15473) | Configure serialFilter in KeyProvider to avoid UnrecoverableKeyException caused by JDK-8189997 |  Critical | kms | Gabor Bota | Gabor Bota |
| [HDFS-13626](https://issues.apache.org/jira/browse/HDFS-13626) | Fix incorrect username when deny the setOwner operation |  Minor | namenode | luhuachao | Zsolt Venczel |
| [MAPREDUCE-7103](https://issues.apache.org/jira/browse/MAPREDUCE-7103) | Fix TestHistoryViewerPrinter on windows due to a mismatch line separator |  Minor | . | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [YARN-8359](https://issues.apache.org/jira/browse/YARN-8359) | Exclude containermanager.linux test classes on Windows |  Major | . | Giovanni Matteo Fumarola | Jason Darrell Lowe |
| [HDFS-13664](https://issues.apache.org/jira/browse/HDFS-13664) | Refactor ConfiguredFailoverProxyProvider to make inheritance easier |  Minor | hdfs-client | Chao Sun | Chao Sun |
| [YARN-8405](https://issues.apache.org/jira/browse/YARN-8405) | RM zk-state-store.parent-path ACLs has been changed since HADOOP-14773 |  Major | . | Rohith Sharma K S | Íñigo Goiri |
| [MAPREDUCE-7108](https://issues.apache.org/jira/browse/MAPREDUCE-7108) | TestFileOutputCommitter fails on Windows |  Minor | test | Zuoming Zhang | Zuoming Zhang |
| [MAPREDUCE-7101](https://issues.apache.org/jira/browse/MAPREDUCE-7101) | Add config parameter to allow JHS to alway scan user dir irrespective of modTime |  Critical | . | Wangda Tan | Thomas Marqardt |
| [YARN-8404](https://issues.apache.org/jira/browse/YARN-8404) | Timeline event publish need to be async to avoid Dispatcher thread leak in case ATS is down |  Blocker | . | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-13675](https://issues.apache.org/jira/browse/HDFS-13675) | Speed up TestDFSAdminWithHA |  Major | hdfs, namenode | Lukas Majercak | Lukas Majercak |
| [HDFS-13673](https://issues.apache.org/jira/browse/HDFS-13673) | TestNameNodeMetrics fails on Windows |  Minor | test | Zuoming Zhang | Zuoming Zhang |
| [HDFS-13676](https://issues.apache.org/jira/browse/HDFS-13676) | TestEditLogRace fails on Windows |  Minor | test | Zuoming Zhang | Zuoming Zhang |
| [HADOOP-15523](https://issues.apache.org/jira/browse/HADOOP-15523) | Shell command timeout given is in seconds whereas it is taken as millisec while scheduling |  Major | . | Bilwa S T | Bilwa S T |
| [YARN-8444](https://issues.apache.org/jira/browse/YARN-8444) | NodeResourceMonitor crashes on bad swapFree value |  Major | . | Jim Brennan | Jim Brennan |
| [YARN-8457](https://issues.apache.org/jira/browse/YARN-8457) | Compilation is broken with -Pyarn-ui |  Major | webapp | Sunil G | Sunil G |
| [YARN-8401](https://issues.apache.org/jira/browse/YARN-8401) | [UI2] new ui is not accessible with out internet connection |  Blocker | . | Bibin Chundatt | Bibin Chundatt |
| [YARN-8451](https://issues.apache.org/jira/browse/YARN-8451) | Multiple NM heartbeat thread created when a slow NM resync with RM |  Major | nodemanager | Botong Huang | Botong Huang |
| [HADOOP-15548](https://issues.apache.org/jira/browse/HADOOP-15548) | Randomize local dirs |  Minor | . | Jim Brennan | Jim Brennan |
| [YARN-8473](https://issues.apache.org/jira/browse/YARN-8473) | Containers being launched as app tears down can leave containers in NEW state |  Major | nodemanager | Jason Darrell Lowe | Jason Darrell Lowe |
| [HDFS-13729](https://issues.apache.org/jira/browse/HDFS-13729) | Fix broken links to RBF documentation |  Minor | documentation | jwhitter | Gabor Bota |
| [YARN-8515](https://issues.apache.org/jira/browse/YARN-8515) | container-executor can crash with SIGPIPE after nodemanager restart |  Major | . | Jim Brennan | Jim Brennan |
| [YARN-8421](https://issues.apache.org/jira/browse/YARN-8421) | when moving app, activeUsers is increased, even though app does not have outstanding request |  Major | . | kyungwan nam |  |
| [HADOOP-15614](https://issues.apache.org/jira/browse/HADOOP-15614) | TestGroupsCaching.testExceptionOnBackgroundRefreshHandled reliably fails |  Major | . | Kihwal Lee | Weiwei Yang |
| [YARN-4606](https://issues.apache.org/jira/browse/YARN-4606) | CapacityScheduler: applications could get starved because computation of #activeUsers considers pending apps |  Critical | capacity scheduler, capacityscheduler | Karam Singh | Manikandan R |
| [HADOOP-15637](https://issues.apache.org/jira/browse/HADOOP-15637) | LocalFs#listLocatedStatus does not filter out hidden .crc files |  Minor | fs | Erik Krogen | Erik Krogen |
| [HADOOP-15644](https://issues.apache.org/jira/browse/HADOOP-15644) | Hadoop Docker Image Pip Install Fails on branch-2 |  Critical | build | Haibo Chen | Haibo Chen |
| [YARN-6966](https://issues.apache.org/jira/browse/YARN-6966) | NodeManager metrics may return wrong negative values when NM restart |  Major | . | Yang Wang | Szilard Nemeth |
| [YARN-8331](https://issues.apache.org/jira/browse/YARN-8331) | Race condition in NM container launched after done |  Major | . | Yang Wang | Pradeep Ambati |
| [HDFS-13758](https://issues.apache.org/jira/browse/HDFS-13758) | DatanodeManager should throw exception if it has BlockRecoveryCommand but the block is not under construction |  Major | namenode | Wei-Chiu Chuang | chencan |
| [YARN-8612](https://issues.apache.org/jira/browse/YARN-8612) | Fix NM Collector Service Port issue in YarnConfiguration |  Major | ATSv2 | Prabha Manepalli | Prabha Manepalli |
| [HADOOP-15674](https://issues.apache.org/jira/browse/HADOOP-15674) | Test failure TestSSLHttpServer.testExcludedCiphers with TLS\_ECDHE\_RSA\_WITH\_AES\_128\_CBC\_SHA256 cipher suite |  Major | common | Gabor Bota | Szilard Nemeth |
| [YARN-8640](https://issues.apache.org/jira/browse/YARN-8640) | Restore previous state in container-executor after failure |  Major | . | Jim Brennan | Jim Brennan |
| [YARN-8679](https://issues.apache.org/jira/browse/YARN-8679) | [ATSv2] If HBase cluster is down for long time, high chances that NM ContainerManager dispatcher get blocked |  Major | . | Rohith Sharma K S | Wangda Tan |
| [HADOOP-14314](https://issues.apache.org/jira/browse/HADOOP-14314) | The OpenSolaris taxonomy link is dead in InterfaceClassification.md |  Major | documentation | Daniel Templeton | Rui Gao |
| [YARN-8649](https://issues.apache.org/jira/browse/YARN-8649) | NPE in localizer hearbeat processing if a container is killed while localizing |  Major | . | lujie | lujie |
| [HADOOP-10219](https://issues.apache.org/jira/browse/HADOOP-10219) | ipc.Client.setupIOstreams() needs to check for ClientCache.stopClient requested shutdowns |  Major | ipc | Steve Loughran | Kihwal Lee |
| [MAPREDUCE-7131](https://issues.apache.org/jira/browse/MAPREDUCE-7131) | Job History Server has race condition where it moves files from intermediate to finished but thinks file is in intermediate |  Major | . | Anthony Hsu | Anthony Hsu |
| [HDFS-13836](https://issues.apache.org/jira/browse/HDFS-13836) | RBF: Handle mount table znode with null value |  Major | federation, hdfs | yanghuafeng | yanghuafeng |
| [HDFS-12716](https://issues.apache.org/jira/browse/HDFS-12716) |  'dfs.datanode.failed.volumes.tolerated' to support minimum number of volumes to be available |  Major | datanode | usharani | Ranith Sardar |
| [YARN-8709](https://issues.apache.org/jira/browse/YARN-8709) | CS preemption monitor always fails since one under-served queue was deleted |  Major | capacityscheduler, scheduler preemption | Tao Yang | Tao Yang |
| [HDFS-13051](https://issues.apache.org/jira/browse/HDFS-13051) | Fix dead lock during async editlog rolling if edit queue is full |  Major | namenode | zhangwei | Daryn Sharp |
| [HDFS-13914](https://issues.apache.org/jira/browse/HDFS-13914) | Fix DN UI logs link broken when https is enabled after HDFS-13902 |  Minor | datanode | Jianfei Jiang | Jianfei Jiang |
| [MAPREDUCE-7133](https://issues.apache.org/jira/browse/MAPREDUCE-7133) | History Server task attempts REST API returns invalid data |  Major | jobhistoryserver | Oleksandr Shevchenko | Oleksandr Shevchenko |
| [YARN-8720](https://issues.apache.org/jira/browse/YARN-8720) | CapacityScheduler does not enforce max resource allocation check at queue level |  Major | capacity scheduler, capacityscheduler, resourcemanager | Tarun Parimi | Tarun Parimi |
| [HDFS-13844](https://issues.apache.org/jira/browse/HDFS-13844) | Fix the fmt\_bytes function in the dfs-dust.js |  Minor | hdfs, ui | yanghuafeng | yanghuafeng |
| [HADOOP-15755](https://issues.apache.org/jira/browse/HADOOP-15755) | StringUtils#createStartupShutdownMessage throws NPE when args is null |  Major | . | Lokesh Jain | Dinesh Chitlangia |
| [MAPREDUCE-3801](https://issues.apache.org/jira/browse/MAPREDUCE-3801) | org.apache.hadoop.mapreduce.v2.app.TestRuntimeEstimators.testExponentialEstimator fails intermittently |  Major | mrv2 | Robert Joseph Evans | Jason Darrell Lowe |
| [MAPREDUCE-7137](https://issues.apache.org/jira/browse/MAPREDUCE-7137) | MRAppBenchmark.benchmark1() fails with NullPointerException |  Minor | test | Oleksandr Shevchenko | Oleksandr Shevchenko |
| [MAPREDUCE-7138](https://issues.apache.org/jira/browse/MAPREDUCE-7138) | ThrottledContainerAllocator in MRAppBenchmark should implement RMHeartbeatHandler |  Minor | test | Oleksandr Shevchenko | Oleksandr Shevchenko |
| [HDFS-13908](https://issues.apache.org/jira/browse/HDFS-13908) | TestDataNodeMultipleRegistrations is flaky |  Major | . | Íñigo Goiri | Ayush Saxena |
| [HADOOP-15772](https://issues.apache.org/jira/browse/HADOOP-15772) | Remove the 'Path ... should be specified as a URI' warnings on startup |  Major | conf | Arpit Agarwal | Ayush Saxena |
| [YARN-8804](https://issues.apache.org/jira/browse/YARN-8804) | resourceLimits may be wrongly calculated when leaf-queue is blocked in cluster with 3+ level queues |  Critical | capacityscheduler | Tao Yang | Tao Yang |
| [YARN-8774](https://issues.apache.org/jira/browse/YARN-8774) | Memory leak when CapacityScheduler allocates from reserved container with non-default label |  Critical | capacityscheduler | Tao Yang | Tao Yang |
| [YARN-8840](https://issues.apache.org/jira/browse/YARN-8840) | Add missing cleanupSSLConfig() call for TestTimelineClient test |  Minor | test, timelineclient | Aki Tanaka | Aki Tanaka |
| [HADOOP-15817](https://issues.apache.org/jira/browse/HADOOP-15817) | Reuse Object Mapper in KMSJSONReader |  Major | kms | Jonathan Turner Eagles | Jonathan Turner Eagles |
| [HADOOP-15820](https://issues.apache.org/jira/browse/HADOOP-15820) | ZStandardDecompressor native code sets an integer field as a long |  Blocker | . | Jason Darrell Lowe | Jason Darrell Lowe |
| [HDFS-13964](https://issues.apache.org/jira/browse/HDFS-13964) | RBF: TestRouterWebHDFSContractAppend fails with No Active Namenode under nameservice |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-13768](https://issues.apache.org/jira/browse/HDFS-13768) |  Adding replicas to volume map makes DataNode start slowly |  Major | . | Yiqun Lin | Surendra Singh Lilhore |
| [HADOOP-15818](https://issues.apache.org/jira/browse/HADOOP-15818) | Fix deprecated maven-surefire-plugin configuration in hadoop-kms module |  Minor | kms | Akira Ajisaka | Vidura Bhathiya Mudalige |
| [HDFS-13802](https://issues.apache.org/jira/browse/HDFS-13802) | RBF: Remove FSCK from Router Web UI |  Major | . | Fei Hui | Fei Hui |
| [HADOOP-15859](https://issues.apache.org/jira/browse/HADOOP-15859) | ZStandardDecompressor.c mistakes a class for an instance |  Blocker | . | Ben Lau | Jason Darrell Lowe |
| [HADOOP-15850](https://issues.apache.org/jira/browse/HADOOP-15850) | CopyCommitter#concatFileChunks should check that the blocks per chunk is not 0 |  Critical | tools/distcp | Ted Yu | Ted Yu |
| [YARN-7502](https://issues.apache.org/jira/browse/YARN-7502) | Nodemanager restart docs should describe nodemanager supervised property |  Major | documentation | Jason Darrell Lowe | Suma Shivaprasad |
| [HADOOP-15866](https://issues.apache.org/jira/browse/HADOOP-15866) | Renamed HADOOP\_SECURITY\_GROUP\_SHELL\_COMMAND\_TIMEOUT keys break compatibility |  Blocker | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-8826](https://issues.apache.org/jira/browse/YARN-8826) | Fix lingering timeline collector after serviceStop in TimelineCollectorManager |  Trivial | ATSv2 | Prabha Manepalli | Prabha Manepalli |
| [HADOOP-15822](https://issues.apache.org/jira/browse/HADOOP-15822) | zstd compressor can fail with a small output buffer |  Major | . | Jason Darrell Lowe | Jason Darrell Lowe |
| [HDFS-13959](https://issues.apache.org/jira/browse/HDFS-13959) | TestUpgradeDomainBlockPlacementPolicy is flaky |  Major | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-15899](https://issues.apache.org/jira/browse/HADOOP-15899) | Update AWS Java SDK versions in NOTICE.txt |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-15900](https://issues.apache.org/jira/browse/HADOOP-15900) | Update JSch versions in LICENSE.txt |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [HDFS-14043](https://issues.apache.org/jira/browse/HDFS-14043) | Tolerate corrupted seen\_txid file |  Major | hdfs, namenode | Lukas Majercak | Lukas Majercak |
| [YARN-8858](https://issues.apache.org/jira/browse/YARN-8858) | CapacityScheduler should respect maximum node resource when per-queue maximum-allocation is being used. |  Major | . | Sumana Sathish | Wangda Tan |
| [HDFS-14048](https://issues.apache.org/jira/browse/HDFS-14048) | DFSOutputStream close() throws exception on subsequent call after DataNode restart |  Major | hdfs-client | Erik Krogen | Erik Krogen |
| [MAPREDUCE-7156](https://issues.apache.org/jira/browse/MAPREDUCE-7156) | NullPointerException when reaching max shuffle connections |  Major | mrv2 | Peter Bacsko | Peter Bacsko |
| [YARN-8233](https://issues.apache.org/jira/browse/YARN-8233) | NPE in CapacityScheduler#tryCommit when handling allocate/reserve proposal whose allocatedOrReservedContainer is null |  Critical | capacityscheduler | Tao Yang | Tao Yang |
| [HADOOP-15923](https://issues.apache.org/jira/browse/HADOOP-15923) | create-release script should set max-cache-ttl as well as default-cache-ttl for gpg-agent |  Blocker | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-15930](https://issues.apache.org/jira/browse/HADOOP-15930) | Exclude MD5 checksum files from release artifact |  Critical | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-15925](https://issues.apache.org/jira/browse/HADOOP-15925) | The config and log of gpg-agent are removed in create-release script |  Major | build | Akira Ajisaka | Dinesh Chitlangia |
| [HDFS-14056](https://issues.apache.org/jira/browse/HDFS-14056) | Fix error messages in HDFS-12716 |  Minor | hdfs | Adam Antal | Ayush Saxena |
| [YARN-7794](https://issues.apache.org/jira/browse/YARN-7794) | SLSRunner is not loading timeline service jars causing failure |  Blocker | scheduler-load-simulator | Sunil G | Yufei Gu |
| [HADOOP-16008](https://issues.apache.org/jira/browse/HADOOP-16008) | Fix typo in CommandsManual.md |  Minor | . | Akira Ajisaka | Shweta |
| [HADOOP-15973](https://issues.apache.org/jira/browse/HADOOP-15973) | Configuration: Included properties are not cached if resource is a stream |  Critical | . | Eric Payne | Eric Payne |
| [YARN-9175](https://issues.apache.org/jira/browse/YARN-9175) | Null resources check in ResourceInfo for branch-3.0 |  Major | . | Jonathan Hung | Jonathan Hung |
| [HADOOP-15992](https://issues.apache.org/jira/browse/HADOOP-15992) | JSON License is included in the transitive dependency of aliyun-sdk-oss 3.0.0 |  Blocker | . | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-16030](https://issues.apache.org/jira/browse/HADOOP-16030) | AliyunOSS: bring fixes back from HADOOP-15671 |  Blocker | fs/oss | wujinhu | wujinhu |
| [YARN-9162](https://issues.apache.org/jira/browse/YARN-9162) | Fix TestRMAdminCLI#testHelp |  Major | resourcemanager, test | Ayush Saxena | Ayush Saxena |
| [YARN-8833](https://issues.apache.org/jira/browse/YARN-8833) | Avoid potential integer overflow when computing fair shares |  Major | fairscheduler | liyakun | liyakun |
| [HADOOP-16016](https://issues.apache.org/jira/browse/HADOOP-16016) | TestSSLFactory#testServerWeakCiphers sporadically fails in precommit builds |  Major | security, test | Jason Darrell Lowe | Akira Ajisaka |
| [HADOOP-16013](https://issues.apache.org/jira/browse/HADOOP-16013) | DecayRpcScheduler decay thread should run as a daemon |  Major | ipc | Erik Krogen | Erik Krogen |
| [YARN-8747](https://issues.apache.org/jira/browse/YARN-8747) | [UI2] YARN UI2 page loading failed due to js error under some time zone configuration |  Critical | webapp | collinma | collinma |
| [YARN-9210](https://issues.apache.org/jira/browse/YARN-9210) | RM nodes web page can not display node info |  Blocker | yarn | Jiandan Yang | Jiandan Yang |
| [YARN-7088](https://issues.apache.org/jira/browse/YARN-7088) | Add application launch time to Resource Manager REST API |  Major | . | Abdullah Yousufi | Kanwaljeet Sachdev |
| [YARN-9222](https://issues.apache.org/jira/browse/YARN-9222) | Print launchTime in ApplicationSummary |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-6616](https://issues.apache.org/jira/browse/YARN-6616) | YARN AHS shows submitTime for jobs same as startTime |  Minor | . | Prabhu Joseph | Prabhu Joseph |
| [MAPREDUCE-7177](https://issues.apache.org/jira/browse/MAPREDUCE-7177) | Disable speculative execution in TestDFSIO |  Major | . | Kihwal Lee | Zhaohui Xin |
| [YARN-9206](https://issues.apache.org/jira/browse/YARN-9206) | RMServerUtils does not count SHUTDOWN as an accepted state |  Major | . | Kuhu Shukla | Kuhu Shukla |
| [YARN-9283](https://issues.apache.org/jira/browse/YARN-9283) | Javadoc of LinuxContainerExecutor#addSchedPriorityCommand has a wrong property name as reference |  Minor | documentation | Szilard Nemeth | Adam Antal |
| [HADOOP-15813](https://issues.apache.org/jira/browse/HADOOP-15813) | Enable more reliable SSL connection reuse |  Major | common | Daryn Sharp | Daryn Sharp |
| [HDFS-14314](https://issues.apache.org/jira/browse/HDFS-14314) | fullBlockReportLeaseId should be reset after registering to NN |  Critical | datanode | star | star |
| [YARN-5714](https://issues.apache.org/jira/browse/YARN-5714) | ContainerExecutor does not order environment map |  Trivial | nodemanager | Remi Catherinot | Remi Catherinot |
| [HADOOP-16192](https://issues.apache.org/jira/browse/HADOOP-16192) | CallQueue backoff bug fixes: doesn't perform backoff when add() is used, and doesn't update backoff when refreshed |  Major | ipc | Erik Krogen | Erik Krogen |
| [HDFS-14399](https://issues.apache.org/jira/browse/HDFS-14399) | Backport HDFS-10536 to branch-2 |  Critical | . | Chao Sun | Chao Sun |
| [HDFS-14407](https://issues.apache.org/jira/browse/HDFS-14407) | Fix misuse of SLF4j logging API in DatasetVolumeChecker#checkAllVolumes |  Minor | . | Wanqiang Ji | Wanqiang Ji |
| [HDFS-14414](https://issues.apache.org/jira/browse/HDFS-14414) | Clean up findbugs warning in branch-2 |  Major | . | Wei-Chiu Chuang | Dinesh Chitlangia |
| [HADOOP-14544](https://issues.apache.org/jira/browse/HADOOP-14544) | DistCp documentation for command line options is misaligned. |  Minor | documentation | Chris Nauroth | Masatake Iwasaki |
| [HDFS-10477](https://issues.apache.org/jira/browse/HDFS-10477) | Stop decommission a rack of DataNodes caused NameNode fail over to standby |  Major | namenode | yunjiong zhao | yunjiong zhao |
| [HDFS-13677](https://issues.apache.org/jira/browse/HDFS-13677) | Dynamic refresh Disk configuration results in overwriting VolumeMap |  Blocker | . | xuzq | xuzq |
| [YARN-9285](https://issues.apache.org/jira/browse/YARN-9285) | RM UI progress column is of wrong type |  Minor | yarn | Ahmed Hussein | Ahmed Hussein |
| [HDFS-14500](https://issues.apache.org/jira/browse/HDFS-14500) | NameNode StartupProgress continues to report edit log segments after the LOADING\_EDITS phase is finished |  Major | namenode | Erik Krogen | Erik Krogen |
| [HADOOP-16331](https://issues.apache.org/jira/browse/HADOOP-16331) | Fix ASF License check in pom.xml |  Major | . | Wanqiang Ji | Akira Ajisaka |
| [HDFS-14514](https://issues.apache.org/jira/browse/HDFS-14514) | Actual read size of open file in encryption zone still larger than listing size even after enabling HDFS-11402 in Hadoop 2 |  Major | encryption, hdfs, snapshots | Siyao Meng | Siyao Meng |
| [HDFS-14512](https://issues.apache.org/jira/browse/HDFS-14512) | ONE\_SSD policy will be violated while write data with DistributedFileSystem.create(....favoredNodes) |  Major | . | Shen Yinjie | Ayush Saxena |
| [HDFS-14521](https://issues.apache.org/jira/browse/HDFS-14521) | Suppress setReplication logging. |  Major | . | Kihwal Lee | Kihwal Lee |
| [YARN-8625](https://issues.apache.org/jira/browse/YARN-8625) | Aggregate Resource Allocation for each job is not present in ATS |  Major | ATSv2 | Prabhu Joseph | Prabhu Joseph |
| [HADOOP-16345](https://issues.apache.org/jira/browse/HADOOP-16345) | Potential NPE when instantiating FairCallQueue metrics |  Major | ipc | Erik Krogen | Erik Krogen |
| [HDFS-14494](https://issues.apache.org/jira/browse/HDFS-14494) | Move Server logging of StatedId inside receiveRequestState() |  Major | . | Konstantin Shvachko | Shweta |
| [HDFS-14535](https://issues.apache.org/jira/browse/HDFS-14535) | The default 8KB buffer in requestFileDescriptors#BufferedOutputStream is causing lots of heap allocation in HBase when using short-circut read |  Major | hdfs-client | Zheng Hu | Zheng Hu |
| [HDFS-13730](https://issues.apache.org/jira/browse/HDFS-13730) | BlockReaderRemote.sendReadResult throws NPE |  Major | hdfs-client | Wei-Chiu Chuang | Yuanbo Liu |
| [HDFS-13770](https://issues.apache.org/jira/browse/HDFS-13770) | dfsadmin -report does not always decrease "missing blocks (with replication factor 1)" metrics when file is deleted |  Major | hdfs | Kitti Nanasi | Kitti Nanasi |
| [HDFS-14101](https://issues.apache.org/jira/browse/HDFS-14101) | Random failure of testListCorruptFilesCorruptedBlock |  Major | test | Kihwal Lee | Zsolt Venczel |
| [HDFS-14465](https://issues.apache.org/jira/browse/HDFS-14465) | When the Block expected replications is larger than the number of DataNodes, entering maintenance will never exit. |  Major | . | Yicong Cai | Yicong Cai |
| [HDFS-14541](https://issues.apache.org/jira/browse/HDFS-14541) |  When evictableMmapped or evictable size is zero, do not throw NoSuchElementException |  Major | hdfs-client, performance | Zheng Hu | Lisheng Sun |
| [HDFS-14629](https://issues.apache.org/jira/browse/HDFS-14629) | Property value Hard Coded in DNConf.java |  Trivial | . | hemanthboyina | hemanthboyina |
| [HDFS-12703](https://issues.apache.org/jira/browse/HDFS-12703) | Exceptions are fatal to decommissioning monitor |  Critical | namenode | Daryn Sharp | Xiaoqiao He |
| [HDFS-12748](https://issues.apache.org/jira/browse/HDFS-12748) | NameNode memory leak when accessing webhdfs GETHOMEDIRECTORY |  Major | hdfs | Jiandan Yang | Weiwei Yang |
| [HADOOP-16386](https://issues.apache.org/jira/browse/HADOOP-16386) | FindBugs warning in branch-2: GlobalStorageStatistics defines non-transient non-serializable instance field map |  Major | fs | Wei-Chiu Chuang | Masatake Iwasaki |
| [MAPREDUCE-6521](https://issues.apache.org/jira/browse/MAPREDUCE-6521) | MiniMRYarnCluster should not create /tmp/hadoop-yarn/staging on local filesystem in unit test |  Major | test | Masatake Iwasaki | Masatake Iwasaki |
| [MAPREDUCE-7076](https://issues.apache.org/jira/browse/MAPREDUCE-7076) | TestNNBench#testNNBenchCreateReadAndDelete failing in our internal build |  Minor | test | Rushabh Shah | kevin su |
| [YARN-9668](https://issues.apache.org/jira/browse/YARN-9668) | UGI conf doesn't read user overridden configurations on RM and NM startup |  Major | . | Jonathan Hung | Jonathan Hung |
| [HADOOP-9844](https://issues.apache.org/jira/browse/HADOOP-9844) | NPE when trying to create an error message response of SASL RPC |  Major | ipc | Steve Loughran | Steve Loughran |
| [HADOOP-16245](https://issues.apache.org/jira/browse/HADOOP-16245) | Enabling SSL within LdapGroupsMapping can break system SSL configs |  Major | common, security | Erik Krogen | Erik Krogen |
| [HDFS-14660](https://issues.apache.org/jira/browse/HDFS-14660) | [SBN Read] ObserverNameNode should throw StandbyException for requests not from ObserverProxyProvider |  Major | . | Chao Sun | Chao Sun |
| [HDFS-14429](https://issues.apache.org/jira/browse/HDFS-14429) | Block remain in COMMITTED but not COMPLETE caused by Decommission |  Major | . | Yicong Cai | Yicong Cai |
| [HDFS-14672](https://issues.apache.org/jira/browse/HDFS-14672) | Backport HDFS-12703 to branch-2 |  Major | namenode | Xiaoqiao He | Xiaoqiao He |
| [HADOOP-16435](https://issues.apache.org/jira/browse/HADOOP-16435) | RpcMetrics should not be retained forever |  Critical | rpc-server | Zoltan Haindrich | Zoltan Haindrich |
| [HDFS-14464](https://issues.apache.org/jira/browse/HDFS-14464) | Remove unnecessary log message from DFSInputStream |  Trivial | . | Kihwal Lee | Chao Sun |
| [HDFS-14569](https://issues.apache.org/jira/browse/HDFS-14569) | Result of crypto -listZones is not formatted properly |  Major | . | hemanthboyina | hemanthboyina |
| [YARN-9596](https://issues.apache.org/jira/browse/YARN-9596) | QueueMetrics has incorrect metrics when labelled partitions are involved |  Major | capacity scheduler | Muhammad Samir Khan | Muhammad Samir Khan |
| [HADOOP-15237](https://issues.apache.org/jira/browse/HADOOP-15237) | In KMS docs there should be one space between KMS\_LOG and NOTE |  Minor | kms | Snigdhanjali Mishra | Snigdhanjali Mishra |
| [HDFS-14462](https://issues.apache.org/jira/browse/HDFS-14462) | WebHDFS throws "Error writing request body to server" instead of DSQuotaExceededException |  Major | webhdfs | Erik Krogen | Simbarashe Dzinamarira |
| [HDFS-14631](https://issues.apache.org/jira/browse/HDFS-14631) | The DirectoryScanner doesn't fix the wrongly placed replica. |  Major | . | Jinglun | Jinglun |
| [HDFS-14724](https://issues.apache.org/jira/browse/HDFS-14724) | Fix JDK7 compatibility in branch-2 |  Blocker | . | Wei-Chiu Chuang | Chen Liang |
| [HDFS-14423](https://issues.apache.org/jira/browse/HDFS-14423) | Percent (%) and plus (+) characters no longer work in WebHDFS |  Major | webhdfs | Jing Wang | Masatake Iwasaki |
| [HDFS-13101](https://issues.apache.org/jira/browse/HDFS-13101) | Yet another fsimage corruption related to snapshot |  Major | snapshots | Yongjun Zhang | Shashikant Banerjee |
| [HDFS-14311](https://issues.apache.org/jira/browse/HDFS-14311) | Multi-threading conflict at layoutVersion when loading block pool storage |  Major | rolling upgrades | Yicong Cai | Yicong Cai |
| [HADOOP-16494](https://issues.apache.org/jira/browse/HADOOP-16494) | Add SHA-256 or SHA-512 checksum to release artifacts to comply with the release distribution policy |  Blocker | build | Akira Ajisaka | Akira Ajisaka |
| [HDFS-13977](https://issues.apache.org/jira/browse/HDFS-13977) | NameNode can kill itself if it tries to send too many txns to a QJM simultaneously |  Major | namenode, qjm | Erik Krogen | Erik Krogen |
| [YARN-9438](https://issues.apache.org/jira/browse/YARN-9438) | launchTime not written to state store for running applications |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-7585](https://issues.apache.org/jira/browse/YARN-7585) | NodeManager should go unhealthy when state store throws DBException |  Major | nodemanager | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [HDFS-14726](https://issues.apache.org/jira/browse/HDFS-14726) | Fix JN incompatibility issue in branch-2 due to backport of HDFS-10519 |  Blocker | journal-node | Chen Liang | Chen Liang |
| [YARN-9806](https://issues.apache.org/jira/browse/YARN-9806) | TestNMSimulator#testNMSimulator fails in branch-2 |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-9820](https://issues.apache.org/jira/browse/YARN-9820) | RM logs InvalidStateTransitionException when app is submitted |  Critical | . | Rohith Sharma K S | Prabhu Joseph |
| [HDFS-14303](https://issues.apache.org/jira/browse/HDFS-14303) | check block directory logic not correct when there is only meta file, print no meaning warn log |  Minor | datanode, hdfs | qiang Liu | qiang Liu |
| [HADOOP-16582](https://issues.apache.org/jira/browse/HADOOP-16582) | LocalFileSystem's mkdirs() does not work as expected under viewfs. |  Major | . | Kihwal Lee | Kihwal Lee |
| [HADOOP-16581](https://issues.apache.org/jira/browse/HADOOP-16581) | ValueQueue does not trigger an async refill when number of values falls below watermark |  Major | common, kms | Yuval Degani | Yuval Degani |
| [HDFS-14853](https://issues.apache.org/jira/browse/HDFS-14853) | NPE in DFSNetworkTopology#chooseRandomWithStorageType() when the excludedNode is not present |  Major | . | Ranith Sardar | Ranith Sardar |
| [YARN-9858](https://issues.apache.org/jira/browse/YARN-9858) | Optimize RMContext getExclusiveEnforcedPartitions |  Major | . | Jonathan Hung | Jonathan Hung |
| [HDFS-14655](https://issues.apache.org/jira/browse/HDFS-14655) | [SBN Read] Namenode crashes if one of The JN is down |  Critical | . | Harshakiran Reddy | Ayush Saxena |
| [HDFS-14245](https://issues.apache.org/jira/browse/HDFS-14245) | Class cast error in GetGroups with ObserverReadProxyProvider |  Major | . | Shen Yinjie | Erik Krogen |
| [HDFS-14509](https://issues.apache.org/jira/browse/HDFS-14509) | DN throws InvalidToken due to inequality of password when upgrade NN 2.x to 3.x |  Blocker | . | Yuxuan Wang | Yuxuan Wang |
| [HADOOP-16655](https://issues.apache.org/jira/browse/HADOOP-16655) | Change cipher suite when fetching tomcat tarball for branch-2 |  Major | . | Jonathan Hung | Jonathan Hung |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-13337](https://issues.apache.org/jira/browse/HDFS-13337) | Backport HDFS-4275 to branch-2.9 |  Minor | . | Íñigo Goiri | Xiao Liang |
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
| [HDFS-13620](https://issues.apache.org/jira/browse/HDFS-13620) | Randomize the test directory path for TestHDFSFileSystemContract |  Minor | . | Anbang Hu | Anbang Hu |
| [HDFS-13591](https://issues.apache.org/jira/browse/HDFS-13591) | TestDFSShell#testSetrepLow fails on Windows |  Minor | . | Anbang Hu | Anbang Hu |
| [HDFS-13632](https://issues.apache.org/jira/browse/HDFS-13632) | Randomize baseDir for MiniJournalCluster in MiniQJMHACluster for TestDFSAdminWithHA |  Minor | . | Anbang Hu | Anbang Hu |
| [MAPREDUCE-7102](https://issues.apache.org/jira/browse/MAPREDUCE-7102) | Fix TestJavaSerialization for Windows due a mismatch line separator |  Minor | . | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [HDFS-13652](https://issues.apache.org/jira/browse/HDFS-13652) | Randomize baseDir for MiniDFSCluster in TestBlockScanner |  Minor | . | Anbang Hu | Anbang Hu |
| [YARN-8370](https://issues.apache.org/jira/browse/YARN-8370) | Some Node Manager tests fail on Windows due to improper path/file separator |  Minor | . | Anbang Hu | Anbang Hu |
| [YARN-8422](https://issues.apache.org/jira/browse/YARN-8422) | TestAMSimulator failing with NPE |  Minor | . | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [HADOOP-15532](https://issues.apache.org/jira/browse/HADOOP-15532) | TestBasicDiskValidator fails with NoSuchFileException |  Minor | . | Íñigo Goiri | Giovanni Matteo Fumarola |
| [HDFS-13563](https://issues.apache.org/jira/browse/HDFS-13563) | TestDFSAdminWithHA times out on Windows |  Minor | . | Anbang Hu | Lukas Majercak |
| [HDFS-13681](https://issues.apache.org/jira/browse/HDFS-13681) | Fix TestStartup.testNNFailToStartOnReadOnlyNNDir test failure on Windows |  Major | test | Xiao Liang | Xiao Liang |
| [YARN-8944](https://issues.apache.org/jira/browse/YARN-8944) | TestContainerAllocation.testUserLimitAllocationMultipleContainers failure after YARN-8896 |  Minor | capacity scheduler | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [HDFS-11950](https://issues.apache.org/jira/browse/HDFS-11950) | Disable libhdfs zerocopy test on Mac |  Minor | libhdfs | John Zhuge | Akira Ajisaka |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-4081](https://issues.apache.org/jira/browse/YARN-4081) | Add support for multiple resource types in the Resource class |  Major | resourcemanager | Varun Vasudev | Varun Vasudev |
| [YARN-4172](https://issues.apache.org/jira/browse/YARN-4172) | Extend DominantResourceCalculator to account for all resources |  Major | resourcemanager | Varun Vasudev | Varun Vasudev |
| [YARN-4715](https://issues.apache.org/jira/browse/YARN-4715) | Add support to read resource types from a config file |  Major | nodemanager, resourcemanager | Varun Vasudev | Varun Vasudev |
| [YARN-4829](https://issues.apache.org/jira/browse/YARN-4829) | Add support for binary units |  Major | nodemanager, resourcemanager | Varun Vasudev | Varun Vasudev |
| [YARN-4830](https://issues.apache.org/jira/browse/YARN-4830) | Add support for resource types in the nodemanager |  Major | nodemanager | Varun Vasudev | Varun Vasudev |
| [YARN-5242](https://issues.apache.org/jira/browse/YARN-5242) | Update DominantResourceCalculator to consider all resource types in calculations |  Major | resourcemanager | Varun Vasudev | Varun Vasudev |
| [YARN-5586](https://issues.apache.org/jira/browse/YARN-5586) | Update the Resources class to consider all resource types |  Major | nodemanager, resourcemanager | Varun Vasudev | Varun Vasudev |
| [YARN-6761](https://issues.apache.org/jira/browse/YARN-6761) | Fix build for YARN-3926 branch |  Major | nodemanager, resourcemanager | Varun Vasudev | Varun Vasudev |
| [YARN-6786](https://issues.apache.org/jira/browse/YARN-6786) | ResourcePBImpl imports cleanup |  Trivial | resourcemanager | Daniel Templeton | Yeliang Cang |
| [YARN-6788](https://issues.apache.org/jira/browse/YARN-6788) | Improve performance of resource profile branch |  Blocker | nodemanager, resourcemanager | Sunil G | Sunil G |
| [YARN-6994](https://issues.apache.org/jira/browse/YARN-6994) | Remove last uses of Long from resource types code |  Minor | resourcemanager | Daniel Templeton | Daniel Templeton |
| [YARN-6892](https://issues.apache.org/jira/browse/YARN-6892) | Improve API implementation in Resources and DominantResourceCalculator class |  Major | nodemanager, resourcemanager | Sunil G | Sunil G |
| [YARN-6610](https://issues.apache.org/jira/browse/YARN-6610) | DominantResourceCalculator#getResourceAsValue dominant param is updated to handle multiple resources |  Critical | resourcemanager | Daniel Templeton | Daniel Templeton |
| [YARN-7030](https://issues.apache.org/jira/browse/YARN-7030) | Performance optimizations in Resource and ResourceUtils class |  Critical | nodemanager, resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-7042](https://issues.apache.org/jira/browse/YARN-7042) | Clean up unit tests after YARN-6610 |  Major | test | Daniel Templeton | Daniel Templeton |
| [YARN-6789](https://issues.apache.org/jira/browse/YARN-6789) | Add Client API to get all supported resource types from RM |  Major | nodemanager, resourcemanager | Sunil G | Sunil G |
| [YARN-6781](https://issues.apache.org/jira/browse/YARN-6781) | ResourceUtils#initializeResourcesMap takes an unnecessary Map parameter |  Minor | resourcemanager | Daniel Templeton | Yu-Tang Lin |
| [YARN-7067](https://issues.apache.org/jira/browse/YARN-7067) | Optimize ResourceType information display in UI |  Critical | nodemanager, resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-7039](https://issues.apache.org/jira/browse/YARN-7039) | Fix javac and javadoc errors in YARN-3926 branch |  Major | nodemanager, resourcemanager | Sunil G | Sunil G |
| [YARN-7093](https://issues.apache.org/jira/browse/YARN-7093) | Improve log message in ResourceUtils |  Trivial | nodemanager, resourcemanager | Sunil G | Sunil G |
| [YARN-6933](https://issues.apache.org/jira/browse/YARN-6933) | ResourceUtils.DISALLOWED\_NAMES check is duplicated |  Major | resourcemanager | Daniel Templeton | Manikandan R |
| [YARN-7137](https://issues.apache.org/jira/browse/YARN-7137) | Move newly added APIs to unstable in YARN-3926 branch |  Blocker | nodemanager, resourcemanager | Wangda Tan | Wangda Tan |
| [HADOOP-14799](https://issues.apache.org/jira/browse/HADOOP-14799) | Update nimbus-jose-jwt to 4.41.1 |  Major | . | Ray Chiang | Ray Chiang |
| [YARN-7345](https://issues.apache.org/jira/browse/YARN-7345) | GPU Isolation: Incorrect minor device numbers written to devices.deny file |  Major | . | Jonathan Hung | Jonathan Hung |
| [HADOOP-14997](https://issues.apache.org/jira/browse/HADOOP-14997) |  Add hadoop-aliyun as dependency of hadoop-cloud-storage |  Minor | fs/oss | Genmao Yu | Genmao Yu |
| [YARN-7143](https://issues.apache.org/jira/browse/YARN-7143) | FileNotFound handling in ResourceUtils is inconsistent |  Major | resourcemanager | Daniel Templeton | Daniel Templeton |
| [YARN-6909](https://issues.apache.org/jira/browse/YARN-6909) | Use LightWeightedResource when number of resource types more than two |  Critical | resourcemanager | Daniel Templeton | Sunil G |
| [HDFS-12801](https://issues.apache.org/jira/browse/HDFS-12801) | RBF: Set MountTableResolver as default file resolver |  Minor | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-7430](https://issues.apache.org/jira/browse/YARN-7430) | Enable user re-mapping for Docker containers by default |  Blocker | security, yarn | Eric Yang | Eric Yang |
| [HADOOP-15024](https://issues.apache.org/jira/browse/HADOOP-15024) | AliyunOSS: support user agent configuration and include that & Hadoop version information to oss server |  Major | fs, fs/oss | Sammi Chen | Sammi Chen |
| [HDFS-12858](https://issues.apache.org/jira/browse/HDFS-12858) | RBF: Add router admin commands usage in HDFS commands reference doc |  Minor | documentation | Yiqun Lin | Yiqun Lin |
| [HDFS-12835](https://issues.apache.org/jira/browse/HDFS-12835) | RBF: Fix Javadoc parameter errors |  Minor | . | Wei Yan | Wei Yan |
| [YARN-7573](https://issues.apache.org/jira/browse/YARN-7573) | Gpu Information page could be empty for nodes without GPU |  Major | webapp, yarn-ui-v2 | Sunil G | Sunil G |
| [HDFS-12396](https://issues.apache.org/jira/browse/HDFS-12396) | Webhdfs file system should get delegation token from kms provider. |  Major | encryption, kms, webhdfs | Rushabh Shah | Rushabh Shah |
| [YARN-6704](https://issues.apache.org/jira/browse/YARN-6704) | Add support for work preserving NM restart when FederationInterceptor is enabled in AMRMProxyService |  Major | . | Botong Huang | Botong Huang |
| [HDFS-12875](https://issues.apache.org/jira/browse/HDFS-12875) | RBF: Complete logic for -readonly option of dfsrouteradmin add command |  Major | . | Yiqun Lin | Íñigo Goiri |
| [YARN-7383](https://issues.apache.org/jira/browse/YARN-7383) | Node resource is not parsed correctly for resource names containing dot |  Major | nodemanager, resourcemanager | Jonathan Hung | Gergely Novák |
| [YARN-7630](https://issues.apache.org/jira/browse/YARN-7630) | Fix AMRMToken rollover handling in AMRMProxy |  Minor | . | Botong Huang | Botong Huang |
| [HDFS-12937](https://issues.apache.org/jira/browse/HDFS-12937) | RBF: Add more unit tests for router admin commands |  Major | test | Yiqun Lin | Yiqun Lin |
| [YARN-7032](https://issues.apache.org/jira/browse/YARN-7032) | [ATSv2] NPE while starting hbase co-processor when HBase authorization is enabled. |  Critical | . | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-12988](https://issues.apache.org/jira/browse/HDFS-12988) | RBF: Mount table entries not properly updated in the local cache |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-12802](https://issues.apache.org/jira/browse/HDFS-12802) | RBF: Control MountTableResolver cache size |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-12934](https://issues.apache.org/jira/browse/HDFS-12934) | RBF: Federation supports global quota |  Major | . | Yiqun Lin | Yiqun Lin |
| [HDFS-12972](https://issues.apache.org/jira/browse/HDFS-12972) | RBF: Display mount table quota info in Web UI and admin command |  Major | . | Yiqun Lin | Yiqun Lin |
| [YARN-6736](https://issues.apache.org/jira/browse/YARN-6736) | Consider writing to both ats v1 & v2 from RM for smoother upgrades |  Major | timelineserver | Vrushali C | Aaron Gresch |
| [HADOOP-15027](https://issues.apache.org/jira/browse/HADOOP-15027) | AliyunOSS: Support multi-thread pre-read to improve sequential read from Hadoop to Aliyun OSS performance |  Major | fs/oss | wujinhu | wujinhu |
| [HDFS-12973](https://issues.apache.org/jira/browse/HDFS-12973) | RBF: Document global quota supporting in federation |  Major | . | Yiqun Lin | Yiqun Lin |
| [HDFS-13028](https://issues.apache.org/jira/browse/HDFS-13028) | RBF: Fix spurious TestRouterRpc#testProxyGetStats |  Minor | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-12772](https://issues.apache.org/jira/browse/HDFS-12772) | RBF: Federation Router State State Store internal API |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13042](https://issues.apache.org/jira/browse/HDFS-13042) | RBF: Heartbeat Router State |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13049](https://issues.apache.org/jira/browse/HDFS-13049) | RBF: Inconsistent Router OPTS config in branch-2 and branch-3 |  Minor | . | Wei Yan | Wei Yan |
| [YARN-7817](https://issues.apache.org/jira/browse/YARN-7817) | Add Resource reference to RM's NodeInfo object so REST API can get non memory/vcore resource usages. |  Major | . | Sumana Sathish | Sunil G |
| [HDFS-12574](https://issues.apache.org/jira/browse/HDFS-12574) | Add CryptoInputStream to WebHdfsFileSystem read call. |  Major | encryption, kms, webhdfs | Rushabh Shah | Rushabh Shah |
| [HDFS-13044](https://issues.apache.org/jira/browse/HDFS-13044) | RBF: Add a safe mode for the Router |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13043](https://issues.apache.org/jira/browse/HDFS-13043) | RBF: Expose the state of the Routers in the federation |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13068](https://issues.apache.org/jira/browse/HDFS-13068) | RBF: Add router admin option to manage safe mode |  Major | . | Íñigo Goiri | Yiqun Lin |
| [YARN-7860](https://issues.apache.org/jira/browse/YARN-7860) | Fix UT failure TestRMWebServiceAppsNodelabel#testAppsRunning |  Major | . | Weiwei Yang | Sunil G |
| [HDFS-13119](https://issues.apache.org/jira/browse/HDFS-13119) | RBF: Manage unavailable clusters |  Major | . | Íñigo Goiri | Yiqun Lin |
| [YARN-7223](https://issues.apache.org/jira/browse/YARN-7223) | Document GPU isolation feature |  Blocker | . | Wangda Tan | Wangda Tan |
| [HDFS-13187](https://issues.apache.org/jira/browse/HDFS-13187) | RBF: Fix Routers information shown in the web UI |  Minor | . | Wei Yan | Wei Yan |
| [HDFS-13184](https://issues.apache.org/jira/browse/HDFS-13184) | RBF: Improve the unit test TestRouterRPCClientRetries |  Minor | test | Yiqun Lin | Yiqun Lin |
| [HDFS-13199](https://issues.apache.org/jira/browse/HDFS-13199) | RBF: Fix the hdfs router page missing label icon issue |  Major | federation, hdfs | maobaolong | maobaolong |
| [HADOOP-15090](https://issues.apache.org/jira/browse/HADOOP-15090) | Add ADL troubleshooting doc |  Major | documentation, fs/adl | Steve Loughran | Steve Loughran |
| [YARN-7919](https://issues.apache.org/jira/browse/YARN-7919) | Refactor timelineservice-hbase module into submodules |  Major | timelineservice | Haibo Chen | Haibo Chen |
| [YARN-8003](https://issues.apache.org/jira/browse/YARN-8003) | Backport the code structure changes in YARN-7346 to branch-2 |  Major | . | Haibo Chen | Haibo Chen |
| [HDFS-13214](https://issues.apache.org/jira/browse/HDFS-13214) | RBF: Complete document of Router configuration |  Major | . | Tao Jie | Yiqun Lin |
| [HADOOP-15267](https://issues.apache.org/jira/browse/HADOOP-15267) | S3A multipart upload fails when SSE-C encryption is enabled |  Critical | fs/s3 | Anis Elleuch | Anis Elleuch |
| [HDFS-13230](https://issues.apache.org/jira/browse/HDFS-13230) | RBF: ConnectionManager's cleanup task will compare each pool's own active conns with its total conns |  Minor | . | Wei Yan | Chao Sun |
| [HDFS-13233](https://issues.apache.org/jira/browse/HDFS-13233) | RBF: MountTableResolver doesn't return the correct mount point of the given path |  Major | hdfs | wangzhiyuan | wangzhiyuan |
| [HDFS-13212](https://issues.apache.org/jira/browse/HDFS-13212) | RBF: Fix router location cache issue |  Major | federation, hdfs | Wu Weiwei | Wu Weiwei |
| [HDFS-13232](https://issues.apache.org/jira/browse/HDFS-13232) | RBF: ConnectionPool should return first usable connection |  Minor | . | Wei Yan | Ekanth Sethuramalingam |
| [HDFS-13240](https://issues.apache.org/jira/browse/HDFS-13240) | RBF: Update some inaccurate document descriptions |  Minor | . | Yiqun Lin | Yiqun Lin |
| [HDFS-11399](https://issues.apache.org/jira/browse/HDFS-11399) | Many tests fails in Windows due to injecting disk failures |  Major | . | Yiqun Lin | Yiqun Lin |
| [HDFS-13241](https://issues.apache.org/jira/browse/HDFS-13241) | RBF: TestRouterSafemode failed if the port 8888 is in use |  Major | hdfs, test | maobaolong | maobaolong |
| [HDFS-13253](https://issues.apache.org/jira/browse/HDFS-13253) | RBF: Quota management incorrect parent-child relationship judgement |  Major | . | Yiqun Lin | Yiqun Lin |
| [HDFS-13226](https://issues.apache.org/jira/browse/HDFS-13226) | RBF: Throw the exception if mount table entry validated failed |  Major | hdfs | maobaolong | maobaolong |
| [HADOOP-15308](https://issues.apache.org/jira/browse/HADOOP-15308) | TestConfiguration fails on Windows because of paths |  Major | test | Íñigo Goiri | Xiao Liang |
| [HDFS-12773](https://issues.apache.org/jira/browse/HDFS-12773) | RBF: Improve State Store FS implementation |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13198](https://issues.apache.org/jira/browse/HDFS-13198) | RBF: RouterHeartbeatService throws out CachedStateStore related exceptions when starting router |  Minor | . | Wei Yan | Wei Yan |
| [HDFS-13224](https://issues.apache.org/jira/browse/HDFS-13224) | RBF: Resolvers to support mount points across multiple subclusters |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13299](https://issues.apache.org/jira/browse/HDFS-13299) | RBF : Fix compilation error in branch-2 (TestMultipleDestinationResolver) |  Blocker | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-15262](https://issues.apache.org/jira/browse/HADOOP-15262) | AliyunOSS: move files under a directory in parallel when rename a directory |  Major | fs/oss | wujinhu | wujinhu |
| [HDFS-13215](https://issues.apache.org/jira/browse/HDFS-13215) | RBF: Move Router to its own module |  Major | . | Íñigo Goiri | Wei Yan |
| [HDFS-13307](https://issues.apache.org/jira/browse/HDFS-13307) | RBF: Improve the use of setQuota command |  Major | . | liuhongtong | liuhongtong |
| [HDFS-13250](https://issues.apache.org/jira/browse/HDFS-13250) | RBF: Router to manage requests across multiple subclusters |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13318](https://issues.apache.org/jira/browse/HDFS-13318) | RBF: Fix FindBugs in hadoop-hdfs-rbf |  Minor | . | Íñigo Goiri | Ekanth Sethuramalingam |
| [HDFS-12792](https://issues.apache.org/jira/browse/HDFS-12792) | RBF: Test Router-based federation using HDFSContract |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-7581](https://issues.apache.org/jira/browse/YARN-7581) | HBase filters are not constructed correctly in ATSv2 |  Major | ATSv2 | Haibo Chen | Haibo Chen |
| [YARN-7986](https://issues.apache.org/jira/browse/YARN-7986) | ATSv2 REST API queries do not return results for uppercase application tags |  Critical | . | Charan Hebri | Charan Hebri |
| [HDFS-12512](https://issues.apache.org/jira/browse/HDFS-12512) | RBF: Add WebHDFS |  Major | fs | Íñigo Goiri | Wei Yan |
| [HDFS-13291](https://issues.apache.org/jira/browse/HDFS-13291) | RBF: Implement available space based OrderResolver |  Major | . | Yiqun Lin | Yiqun Lin |
| [HDFS-13204](https://issues.apache.org/jira/browse/HDFS-13204) | RBF: Optimize name service safe mode icon |  Minor | . | liuhongtong | liuhongtong |
| [HDFS-13352](https://issues.apache.org/jira/browse/HDFS-13352) | RBF: Add xsl stylesheet for hdfs-rbf-default.xml |  Major | documentation | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-8010](https://issues.apache.org/jira/browse/YARN-8010) | Add config in FederationRMFailoverProxy to not bypass facade cache when failing over |  Minor | . | Botong Huang | Botong Huang |
| [HDFS-13347](https://issues.apache.org/jira/browse/HDFS-13347) | RBF: Cache datanode reports |  Minor | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13289](https://issues.apache.org/jira/browse/HDFS-13289) | RBF: TestConnectionManager#testCleanup() test case need correction |  Minor | . | Dibyendu Karmakar | Dibyendu Karmakar |
| [HDFS-13364](https://issues.apache.org/jira/browse/HDFS-13364) | RBF: Support NamenodeProtocol in the Router |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HADOOP-14651](https://issues.apache.org/jira/browse/HADOOP-14651) | Update okhttp version to 2.7.5 |  Major | fs/adl | Ray Chiang | Ray Chiang |
| [YARN-6936](https://issues.apache.org/jira/browse/YARN-6936) | [Atsv2] Retrospect storing entities into sub application table from client perspective |  Major | . | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-13353](https://issues.apache.org/jira/browse/HDFS-13353) | RBF: TestRouterWebHDFSContractCreate failed |  Major | test | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-8107](https://issues.apache.org/jira/browse/YARN-8107) | Give an informative message when incorrect format is used in ATSv2 filter attributes |  Major | ATSv2 | Charan Hebri | Rohith Sharma K S |
| [YARN-8110](https://issues.apache.org/jira/browse/YARN-8110) | AMRMProxy recover should catch for all throwable to avoid premature exit |  Major | . | Botong Huang | Botong Huang |
| [HDFS-13402](https://issues.apache.org/jira/browse/HDFS-13402) | RBF: Fix  java doc for StateStoreFileSystemImpl |  Minor | hdfs | Yiran Wu | Yiran Wu |
| [HDFS-13380](https://issues.apache.org/jira/browse/HDFS-13380) | RBF: mv/rm fail after the directory exceeded the quota limit |  Major | . | Wu Weiwei | Yiqun Lin |
| [HDFS-13410](https://issues.apache.org/jira/browse/HDFS-13410) | RBF: Support federation with no subclusters |  Minor | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13384](https://issues.apache.org/jira/browse/HDFS-13384) | RBF: Improve timeout RPC call mechanism |  Minor | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13045](https://issues.apache.org/jira/browse/HDFS-13045) | RBF: Improve error message returned from subcluster |  Minor | . | Wei Yan | Íñigo Goiri |
| [HDFS-13428](https://issues.apache.org/jira/browse/HDFS-13428) | RBF: Remove LinkedList From StateStoreFileImpl.java |  Trivial | federation | David Mollitor | David Mollitor |
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
| [HDFS-13508](https://issues.apache.org/jira/browse/HDFS-13508) | RBF: Normalize paths (automatically) when adding, updating, removing or listing mount table entries |  Minor | . | Ekanth Sethuramalingam | Ekanth Sethuramalingam |
| [HDFS-13434](https://issues.apache.org/jira/browse/HDFS-13434) | RBF: Fix dead links in RBF document |  Major | documentation | Akira Ajisaka | Chetna Chaudhari |
| [HDFS-13488](https://issues.apache.org/jira/browse/HDFS-13488) | RBF: Reject requests when a Router is overloaded |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13525](https://issues.apache.org/jira/browse/HDFS-13525) | RBF: Add unit test TestStateStoreDisabledNameservice |  Major | . | Yiqun Lin | Yiqun Lin |
| [YARN-8253](https://issues.apache.org/jira/browse/YARN-8253) | HTTPS Ats v2 api call fails with "bad HTTP parsed" |  Critical | ATSv2 | Yesha Vora | Charan Hebri |
| [HADOOP-15454](https://issues.apache.org/jira/browse/HADOOP-15454) | TestRollingFileSystemSinkWithLocal fails on Windows |  Major | test | Xiao Liang | Xiao Liang |
| [HDFS-13346](https://issues.apache.org/jira/browse/HDFS-13346) | RBF: Fix synchronization of router quota and nameservice quota |  Major | . | liuhongtong | Yiqun Lin |
| [YARN-8247](https://issues.apache.org/jira/browse/YARN-8247) | Incorrect HTTP status code returned by ATSv2 for non-whitelisted users |  Critical | ATSv2 | Charan Hebri | Rohith Sharma K S |
| [YARN-8130](https://issues.apache.org/jira/browse/YARN-8130) | Race condition when container events are published for KILLED applications |  Major | ATSv2 | Charan Hebri | Rohith Sharma K S |
| [YARN-7900](https://issues.apache.org/jira/browse/YARN-7900) | [AMRMProxy] AMRMClientRelayer for stateful FederationInterceptor |  Major | . | Botong Huang | Botong Huang |
| [HADOOP-15498](https://issues.apache.org/jira/browse/HADOOP-15498) | TestHadoopArchiveLogs (#testGenerateScript, #testPrepareWorkingDir) fails on Windows |  Minor | . | Anbang Hu | Anbang Hu |
| [HADOOP-15497](https://issues.apache.org/jira/browse/HADOOP-15497) | TestTrash should use proper test path to avoid failing on Windows |  Minor | . | Anbang Hu | Anbang Hu |
| [HDFS-13637](https://issues.apache.org/jira/browse/HDFS-13637) | RBF: Router fails when threadIndex (in ConnectionPool) wraps around Integer.MIN\_VALUE |  Critical | federation | CR Hota | CR Hota |
| [YARN-4781](https://issues.apache.org/jira/browse/YARN-4781) | Support intra-queue preemption for fairness ordering policy. |  Major | scheduler | Wangda Tan | Eric Payne |
| [HADOOP-15506](https://issues.apache.org/jira/browse/HADOOP-15506) | Upgrade Azure Storage Sdk version to 7.0.0 and update corresponding code blocks |  Minor | fs/azure | Esfandiar Manii | Esfandiar Manii |
| [HADOOP-15529](https://issues.apache.org/jira/browse/HADOOP-15529) | ContainerLaunch#testInvalidEnvVariableSubstitutionType is not supported in Windows |  Minor | . | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [HADOOP-15533](https://issues.apache.org/jira/browse/HADOOP-15533) | Make WASB listStatus messages consistent |  Trivial | fs/azure | Esfandiar Manii | Esfandiar Manii |
| [HADOOP-15458](https://issues.apache.org/jira/browse/HADOOP-15458) | TestLocalFileSystem#testFSOutputStreamBuilder fails on Windows |  Minor | test | Xiao Liang | Xiao Liang |
| [YARN-8481](https://issues.apache.org/jira/browse/YARN-8481) | AMRMProxyPolicies should accept heartbeat response from new/unknown subclusters |  Minor | amrmproxy, federation | Botong Huang | Botong Huang |
| [HDFS-13528](https://issues.apache.org/jira/browse/HDFS-13528) | RBF: If a directory exceeds quota limit then quota usage is not refreshed for other mount entries |  Major | . | Dibyendu Karmakar | Dibyendu Karmakar |
| [HDFS-13710](https://issues.apache.org/jira/browse/HDFS-13710) | RBF:  setQuota and getQuotaUsage should check the dfs.federation.router.quota.enable |  Major | federation, hdfs | yanghuafeng | yanghuafeng |
| [HDFS-13726](https://issues.apache.org/jira/browse/HDFS-13726) | RBF: Fix RBF configuration links |  Minor | documentation | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-13475](https://issues.apache.org/jira/browse/HDFS-13475) | RBF: Admin cannot enforce Router enter SafeMode |  Major | . | Wei Yan | Chao Sun |
| [HDFS-13733](https://issues.apache.org/jira/browse/HDFS-13733) | RBF: Add Web UI configurations and descriptions to RBF document |  Minor | documentation | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-13743](https://issues.apache.org/jira/browse/HDFS-13743) | RBF: Router throws NullPointerException due to the invalid initialization of MountTableResolver |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-13583](https://issues.apache.org/jira/browse/HDFS-13583) | RBF: Router admin clrQuota is not synchronized with nameservice |  Major | . | Dibyendu Karmakar | Dibyendu Karmakar |
| [HDFS-13750](https://issues.apache.org/jira/browse/HDFS-13750) | RBF: Router ID in RouterRpcClient is always null |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-8129](https://issues.apache.org/jira/browse/YARN-8129) | Improve error message for invalid value in fields attribute |  Minor | ATSv2 | Charan Hebri | Abhishek Modi |
| [YARN-8581](https://issues.apache.org/jira/browse/YARN-8581) | [AMRMProxy] Add sub-cluster timeout in LocalityMulticastAMRMProxyPolicy |  Major | amrmproxy, federation | Botong Huang | Botong Huang |
| [YARN-8673](https://issues.apache.org/jira/browse/YARN-8673) | [AMRMProxy] More robust responseId resync after an YarnRM master slave switch |  Major | amrmproxy | Botong Huang | Botong Huang |
| [HDFS-13848](https://issues.apache.org/jira/browse/HDFS-13848) | Refactor NameNode failover proxy providers |  Major | ha, hdfs-client | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-13634](https://issues.apache.org/jira/browse/HDFS-13634) | RBF: Configurable value in xml for async connection request queue size. |  Major | federation | CR Hota | CR Hota |
| [HADOOP-15731](https://issues.apache.org/jira/browse/HADOOP-15731) | TestDistributedShell fails on Windows |  Major | . | Botong Huang | Botong Huang |
| [HADOOP-15759](https://issues.apache.org/jira/browse/HADOOP-15759) | AliyunOSS: update oss-sdk version to 3.0.0 |  Major | fs/oss | wujinhu | wujinhu |
| [HADOOP-15748](https://issues.apache.org/jira/browse/HADOOP-15748) | S3 listing inconsistency can raise NPE in globber |  Major | fs | Steve Loughran | Steve Loughran |
| [YARN-8696](https://issues.apache.org/jira/browse/YARN-8696) | [AMRMProxy] FederationInterceptor upgrade: home sub-cluster heartbeat async |  Major | nodemanager | Botong Huang | Botong Huang |
| [HADOOP-15671](https://issues.apache.org/jira/browse/HADOOP-15671) | AliyunOSS: Support Assume Roles in AliyunOSS |  Major | fs/oss | wujinhu | wujinhu |
| [HDFS-13790](https://issues.apache.org/jira/browse/HDFS-13790) | RBF: Move ClientProtocol APIs to its own module |  Major | . | Íñigo Goiri | Chao Sun |
| [YARN-7652](https://issues.apache.org/jira/browse/YARN-7652) | Handle AM register requests asynchronously in FederationInterceptor |  Major | amrmproxy, federation | Subramaniam Krishnan | Botong Huang |
| [YARN-6989](https://issues.apache.org/jira/browse/YARN-6989) | Ensure timeline service v2 codebase gets UGI from HttpServletRequest in a consistent way |  Major | timelineserver | Vrushali C | Abhishek Modi |
| [YARN-3879](https://issues.apache.org/jira/browse/YARN-3879) | [Storage implementation] Create HDFS backing storage implementation for ATS reads |  Major | timelineserver | Tsuyoshi Ozawa | Abhishek Modi |
| [HADOOP-15837](https://issues.apache.org/jira/browse/HADOOP-15837) | DynamoDB table Update can fail S3A FS init |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-15607](https://issues.apache.org/jira/browse/HADOOP-15607) | AliyunOSS: fix duplicated partNumber issue in AliyunOSSBlockOutputStream |  Critical | . | wujinhu | wujinhu |
| [HADOOP-15868](https://issues.apache.org/jira/browse/HADOOP-15868) | AliyunOSS: update document for properties of multiple part download, multiple part upload and directory copy |  Major | fs/oss | wujinhu | wujinhu |
| [YARN-8893](https://issues.apache.org/jira/browse/YARN-8893) | [AMRMProxy] Fix thread leak in AMRMClientRelayer and UAM client |  Major | amrmproxy, federation | Botong Huang | Botong Huang |
| [YARN-8905](https://issues.apache.org/jira/browse/YARN-8905) | [Router] Add JvmMetricsInfo and pause monitor |  Minor | . | Bibin Chundatt | Bilwa S T |
| [HADOOP-15917](https://issues.apache.org/jira/browse/HADOOP-15917) | AliyunOSS: fix incorrect ReadOps and WriteOps in statistics |  Major | fs/oss | wujinhu | wujinhu |
| [HADOOP-16009](https://issues.apache.org/jira/browse/HADOOP-16009) | Replace the url of the repository in Apache Hadoop source code |  Major | documentation | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-15323](https://issues.apache.org/jira/browse/HADOOP-15323) | AliyunOSS: Improve copy file performance for AliyunOSSFileSystemStore |  Major | fs/oss | wujinhu | wujinhu |
| [YARN-9182](https://issues.apache.org/jira/browse/YARN-9182) | Backport YARN-6445 resource profile performance improvements to branch-2 |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-9181](https://issues.apache.org/jira/browse/YARN-9181) | Backport YARN-6232 for generic resource type usage to branch-2 |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-9177](https://issues.apache.org/jira/browse/YARN-9177) | Use resource map for app metrics in TestCombinedSystemMetricsPublisher for branch-2 |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-9188](https://issues.apache.org/jira/browse/YARN-9188) | Port YARN-7136 to branch-2 |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-9187](https://issues.apache.org/jira/browse/YARN-9187) | Backport YARN-6852 for GPU-specific native changes to branch-2 |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-9180](https://issues.apache.org/jira/browse/YARN-9180) | Port YARN-7033 NM recovery of assigned resources to branch-3.0/branch-2 |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-9280](https://issues.apache.org/jira/browse/YARN-9280) | Backport YARN-6620 to YARN-8200/branch-2 |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-9174](https://issues.apache.org/jira/browse/YARN-9174) | Backport YARN-7224 for refactoring of GpuDevice class |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-9289](https://issues.apache.org/jira/browse/YARN-9289) | Backport YARN-7330 for GPU in UI to branch-2 |  Major | . | Jonathan Hung | Jonathan Hung |
| [HDFS-14262](https://issues.apache.org/jira/browse/HDFS-14262) | [SBN read] Unclear Log.WARN message in GlobalStateIdContext |  Major | hdfs | Shweta | Shweta |
| [YARN-8549](https://issues.apache.org/jira/browse/YARN-8549) | Adding a NoOp timeline writer and reader plugin classes for ATSv2 |  Minor | ATSv2, timelineclient, timelineserver | Prabha Manepalli | Prabha Manepalli |
| [HADOOP-16109](https://issues.apache.org/jira/browse/HADOOP-16109) | Parquet reading S3AFileSystem causes EOF |  Blocker | fs/s3 | Dave Christianson | Steve Loughran |
| [YARN-9397](https://issues.apache.org/jira/browse/YARN-9397) | Fix empty NMResourceInfo object test failures in branch-2 |  Major | . | Jonathan Hung | Jonathan Hung |
| [HADOOP-16191](https://issues.apache.org/jira/browse/HADOOP-16191) | AliyunOSS: improvements for copyFile/copyDirectory and logging |  Major | fs/oss | wujinhu | wujinhu |
| [YARN-9271](https://issues.apache.org/jira/browse/YARN-9271) | Backport YARN-6927 for resource type support in MapReduce |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-9291](https://issues.apache.org/jira/browse/YARN-9291) | Backport YARN-7637 to branch-2 |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-9409](https://issues.apache.org/jira/browse/YARN-9409) | Port resource type changes from YARN-7237 to branch-3.0/branch-2 |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-9272](https://issues.apache.org/jira/browse/YARN-9272) | Backport YARN-7738 for refreshing max allocation for multiple resource types |  Major | . | Jonathan Hung | Jonathan Hung |
| [HADOOP-16205](https://issues.apache.org/jira/browse/HADOOP-16205) | Backporting ABFS driver from trunk to branch 2.0 |  Major | fs/azure | Esfandiar Manii | Yuan Gao |
| [HADOOP-16269](https://issues.apache.org/jira/browse/HADOOP-16269) | ABFS: add listFileStatus with StartFrom |  Major | fs/azure | Da Zhou | Da Zhou |
| [HADOOP-16306](https://issues.apache.org/jira/browse/HADOOP-16306) | AliyunOSS: Remove temporary files when upload small files to OSS |  Major | fs/oss | wujinhu | wujinhu |
| [HDFS-14034](https://issues.apache.org/jira/browse/HDFS-14034) | Support getQuotaUsage API in WebHDFS |  Major | fs, webhdfs | Erik Krogen | Chao Sun |
| [YARN-9775](https://issues.apache.org/jira/browse/YARN-9775) | RMWebServices /scheduler-conf GET returns all hadoop configurations for ZKConfigurationStore |  Major | restapi | Prabhu Joseph | Prabhu Joseph |
| [HDFS-14771](https://issues.apache.org/jira/browse/HDFS-14771) | Backport HDFS-14617 to branch-2 (Improve fsimage load time by writing sub-sections to the fsimage index) |  Major | namenode | Xiaoqiao He | Xiaoqiao He |
| [HDFS-14822](https://issues.apache.org/jira/browse/HDFS-14822) | [SBN read] Revisit GlobalStateIdContext locking when getting server state id |  Major | hdfs | Chen Liang | Chen Liang |
| [HDFS-14785](https://issues.apache.org/jira/browse/HDFS-14785) | [SBN read] Change client logging to be less aggressive |  Major | hdfs | Chen Liang | Chen Liang |
| [HDFS-14858](https://issues.apache.org/jira/browse/HDFS-14858) | [SBN read] Allow configurably enable/disable AlignmentContext on NameNode |  Major | hdfs | Chen Liang | Chen Liang |
| [HDFS-12979](https://issues.apache.org/jira/browse/HDFS-12979) | StandbyNode should upload FsImage to ObserverNode after checkpointing. |  Major | hdfs | Konstantin Shvachko | Chen Liang |
| [HADOOP-16630](https://issues.apache.org/jira/browse/HADOOP-16630) | Backport HADOOP-16548 - "ABFS: Config to enable/disable flush operation" to branch-2 |  Minor | fs/azure | Sneha Vijayarajan | Sneha Vijayarajan |
| [HADOOP-16631](https://issues.apache.org/jira/browse/HADOOP-16631) | Backport HADOOP-16578 - "ABFS: fileSystemExists() should not call container level apis" to Branch-2 |  Major | fs/azure | Sneha Vijayarajan | Sneha Vijayarajan |
| [HDFS-14162](https://issues.apache.org/jira/browse/HDFS-14162) | Balancer should work with ObserverNode |  Major | . | Konstantin Shvachko | Erik Krogen |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-15149](https://issues.apache.org/jira/browse/HADOOP-15149) | CryptoOutputStream should implement StreamCapabilities |  Major | fs | Mike Drob | Xiao Chen |
| [HADOOP-15177](https://issues.apache.org/jira/browse/HADOOP-15177) | Update the release year to 2018 |  Blocker | build | Akira Ajisaka | Bharat Viswanadham |
| [YARN-8412](https://issues.apache.org/jira/browse/YARN-8412) | Move ResourceRequest.clone logic everywhere into a proper API |  Minor | . | Botong Huang | Botong Huang |
| [HDFS-13870](https://issues.apache.org/jira/browse/HDFS-13870) | WebHDFS: Document ALLOWSNAPSHOT and DISALLOWSNAPSHOT API doc |  Minor | documentation, webhdfs | Siyao Meng | Siyao Meng |
| [HDFS-12729](https://issues.apache.org/jira/browse/HDFS-12729) | Document special paths in HDFS |  Major | documentation | Christopher Douglas | Masatake Iwasaki |
| [HADOOP-15711](https://issues.apache.org/jira/browse/HADOOP-15711) | Move branch-2 precommit/nightly test builds to java 8 |  Critical | . | Jonathan Hung | Jonathan Hung |
| [HDFS-14510](https://issues.apache.org/jira/browse/HDFS-14510) | Backport HDFS-13087 to branch-2 (Snapshotted encryption zone information should be immutable) |  Major | encryption, snapshots | Wei-Chiu Chuang | Siyao Meng |
| [HDFS-14585](https://issues.apache.org/jira/browse/HDFS-14585) | Backport HDFS-8901 Use ByteBuffer in DFSInputStream#read to branch2.9 |  Major | . | Lisheng Sun | Lisheng Sun |
| [HDFS-14483](https://issues.apache.org/jira/browse/HDFS-14483) | Backport HDFS-14111,HDFS-3246 ByteBuffer pread interface to branch-2.9 |  Major | . | Zheng Hu | Lisheng Sun |
| [YARN-9559](https://issues.apache.org/jira/browse/YARN-9559) | Create AbstractContainersLauncher for pluggable ContainersLauncher logic |  Major | . | Jonathan Hung | Jonathan Hung |
| [HDFS-14725](https://issues.apache.org/jira/browse/HDFS-14725) | Backport HDFS-12914 to branch-2 (Block report leases cause missing blocks until next report) |  Major | namenode | Wei-Chiu Chuang | Xiaoqiao He |
| [YARN-8200](https://issues.apache.org/jira/browse/YARN-8200) | Backport resource types/GPU features to branch-3.0/branch-2 |  Major | . | Jonathan Hung | Jonathan Hung |
| [HADOOP-16555](https://issues.apache.org/jira/browse/HADOOP-16555) | Update commons-compress to 1.19 |  Major | . | Wei-Chiu Chuang | YiSheng Lien |
| [YARN-9730](https://issues.apache.org/jira/browse/YARN-9730) | Support forcing configured partitions to be exclusive based on app node label |  Major | . | Jonathan Hung | Jonathan Hung |
| [HADOOP-16544](https://issues.apache.org/jira/browse/HADOOP-16544) | update io.netty in branch-2 |  Major | . | Wei-Chiu Chuang | Masatake Iwasaki |
| [HADOOP-16588](https://issues.apache.org/jira/browse/HADOOP-16588) | Update commons-beanutils version to 1.9.4 in branch-2 |  Critical | . | Wei-Chiu Chuang | Wei-Chiu Chuang |


