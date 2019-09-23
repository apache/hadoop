
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

## Release 3.1.0 - 2018-04-06

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-15008](https://issues.apache.org/jira/browse/HADOOP-15008) | Metrics sinks may emit too frequently if multiple sink periods are configured |  Minor | metrics | Erik Krogen | Erik Krogen |
| [HDFS-12825](https://issues.apache.org/jira/browse/HDFS-12825) | Fsck report shows config key name for min replication issues |  Minor | hdfs | Harshakiran Reddy | Gabor Bota |
| [HDFS-12883](https://issues.apache.org/jira/browse/HDFS-12883) | RBF: Document Router and State Store metrics |  Major | documentation | Yiqun Lin | Yiqun Lin |
| [HDFS-12895](https://issues.apache.org/jira/browse/HDFS-12895) | RBF: Add ACL support for mount table |  Major | . | Yiqun Lin | Yiqun Lin |
| [YARN-7190](https://issues.apache.org/jira/browse/YARN-7190) | Ensure only NM classpath in 2.x gets TSv2 related hbase jars, not the user classpath |  Major | timelineclient, timelinereader, timelineserver | Vrushali C | Varun Saxena |
| [HADOOP-13282](https://issues.apache.org/jira/browse/HADOOP-13282) | S3 blob etags to be made visible in S3A status/getFileChecksum() calls |  Minor | fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-13099](https://issues.apache.org/jira/browse/HDFS-13099) | RBF: Use the ZooKeeper as the default State Store |  Minor | documentation | Yiqun Lin | Yiqun Lin |
| [YARN-7677](https://issues.apache.org/jira/browse/YARN-7677) | Docker image cannot set HADOOP\_CONF\_DIR |  Major | . | Eric Badger | Jim Brennan |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-13083](https://issues.apache.org/jira/browse/HDFS-13083) | RBF: Fix doc error setting up client |  Major | federation | tartarus | tartarus |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-15005](https://issues.apache.org/jira/browse/HADOOP-15005) | Support meta tag element in Hadoop XML configurations |  Major | . | Ajay Kumar | Ajay Kumar |
| [YARN-3926](https://issues.apache.org/jira/browse/YARN-3926) | [Umbrella] Extend the YARN resource model for easier resource-type management and profiles |  Major | nodemanager, resourcemanager | Varun Vasudev | Varun Vasudev |
| [HDFS-7877](https://issues.apache.org/jira/browse/HDFS-7877) | [Umbrella] Support maintenance state for datanodes |  Major | datanode, namenode | Ming Ma | Ming Ma |
| [HADOOP-13055](https://issues.apache.org/jira/browse/HADOOP-13055) | Implement linkMergeSlash and linkFallback for ViewFileSystem |  Major | fs, viewfs | Zhe Zhang | Manoj Govindassamy |
| [YARN-6871](https://issues.apache.org/jira/browse/YARN-6871) | Add additional deSelects params in RMWebServices#getAppReport |  Major | resourcemanager, router | Giovanni Matteo Fumarola | Tanuj Nayak |
| [HADOOP-14840](https://issues.apache.org/jira/browse/HADOOP-14840) | Tool to estimate resource requirements of an application pipeline based on prior executions |  Major | tools | Subru Krishnan | Rui Li |
| [HDFS-206](https://issues.apache.org/jira/browse/HDFS-206) | Support for head in FSShell |  Minor | . | Olga Natkovich | Gabor Bota |
| [YARN-5079](https://issues.apache.org/jira/browse/YARN-5079) | [Umbrella] Native YARN framework layer for services and beyond |  Major | . | Vinod Kumar Vavilapalli |  |
| [YARN-4757](https://issues.apache.org/jira/browse/YARN-4757) | [Umbrella] Simplified discovery of services via DNS mechanisms |  Major | . | Vinod Kumar Vavilapalli |  |
| [HADOOP-13786](https://issues.apache.org/jira/browse/HADOOP-13786) | Add S3A committers for zero-rename commits to S3 endpoints |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-9806](https://issues.apache.org/jira/browse/HDFS-9806) | Allow HDFS block replicas to be provided by an external storage system |  Major | . | Chris Douglas |  |
| [YARN-6592](https://issues.apache.org/jira/browse/YARN-6592) | [Umbrella] Rich placement constraints in YARN |  Major | . | Konstantinos Karanasos |  |
| [HDFS-12998](https://issues.apache.org/jira/browse/HDFS-12998) | SnapshotDiff - Provide an iterator-based listing API for calculating snapshotDiff |  Major | snapshots | Shashikant Banerjee | Shashikant Banerjee |
| [YARN-5764](https://issues.apache.org/jira/browse/YARN-5764) | NUMA awareness support for launching containers |  Major | nodemanager, yarn | Olasoji | Devaraj K |
| [YARN-5983](https://issues.apache.org/jira/browse/YARN-5983) | [Umbrella] Support for FPGA as a Resource in YARN |  Major | yarn | Zhankun Tang | Zhankun Tang |
| [YARN-6223](https://issues.apache.org/jira/browse/YARN-6223) | [Umbrella] Natively support GPU configuration/discovery/scheduling/isolation on YARN |  Major | . | Wangda Tan | Wangda Tan |
| [HADOOP-14898](https://issues.apache.org/jira/browse/HADOOP-14898) | Create official Docker images for development and testing features |  Major | . | Elek, Marton | Elek, Marton |
| [HDFS-13553](https://issues.apache.org/jira/browse/HDFS-13553) | RBF: Support global quota |  Major | . | Íñigo Goiri | Yiqun Lin |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-7022](https://issues.apache.org/jira/browse/YARN-7022) | Improve click interaction in queue topology in new YARN UI |  Major | yarn-ui-v2 | Abdullah Yousufi | Abdullah Yousufi |
| [YARN-7033](https://issues.apache.org/jira/browse/YARN-7033) | Add support for NM Recovery of assigned resources (e.g. GPU's, NUMA, FPGA's) to container |  Major | nodemanager | Devaraj K | Devaraj K |
| [HADOOP-14850](https://issues.apache.org/jira/browse/HADOOP-14850) | Read HttpServer2 resources directly from the source tree (if exists) |  Major | . | Elek, Marton | Elek, Marton |
| [HADOOP-14849](https://issues.apache.org/jira/browse/HADOOP-14849) | some wrong spelling words update |  Trivial | . | Chen Hongfei | Chen Hongfei |
| [HADOOP-14844](https://issues.apache.org/jira/browse/HADOOP-14844) | Remove requirement to specify TenantGuid for MSI Token Provider |  Major | fs/adl | Atul Sikaria | Atul Sikaria |
| [YARN-7057](https://issues.apache.org/jira/browse/YARN-7057) | FSAppAttempt#getResourceUsage doesn't need to consider resources queued for preemption |  Major | fairscheduler | Karthik Kambatla | Karthik Kambatla |
| [HADOOP-14804](https://issues.apache.org/jira/browse/HADOOP-14804) | correct wrong parameters format order in core-default.xml |  Trivial | . | Chen Hongfei | Chen Hongfei |
| [HADOOP-14864](https://issues.apache.org/jira/browse/HADOOP-14864) | FSDataInputStream#unbuffer UOE should include stream class name |  Minor | fs | John Zhuge | Bharat Viswanadham |
| [HDFS-12441](https://issues.apache.org/jira/browse/HDFS-12441) | Suppress UnresolvedPathException in namenode log |  Minor | . | Kihwal Lee | Kihwal Lee |
| [HADOOP-13714](https://issues.apache.org/jira/browse/HADOOP-13714) | Tighten up our compatibility guidelines for Hadoop 3 |  Blocker | documentation | Karthik Kambatla | Daniel Templeton |
| [HADOOP-7308](https://issues.apache.org/jira/browse/HADOOP-7308) | Remove unused TaskLogAppender configurations from log4j.properties |  Major | conf | Todd Lipcon | Todd Lipcon |
| [YARN-7045](https://issues.apache.org/jira/browse/YARN-7045) | Remove FSLeafQueue#addAppSchedulable |  Major | fairscheduler | Yufei Gu | Sen Zhao |
| [HDFS-12486](https://issues.apache.org/jira/browse/HDFS-12486) | GetConf to get journalnodeslist |  Major | journal-node, shell | Bharat Viswanadham | Bharat Viswanadham |
| [HDFS-12320](https://issues.apache.org/jira/browse/HDFS-12320) | Add  quantiles for transactions batched in Journal sync |  Major | metrics, namenode | Hanisha Koneru | Hanisha Koneru |
| [HDFS-12516](https://issues.apache.org/jira/browse/HDFS-12516) | Suppress the fsnamesystem lock warning on nn startup |  Major | . | Ajay Kumar | Ajay Kumar |
| [HDFS-12304](https://issues.apache.org/jira/browse/HDFS-12304) | Remove unused parameter from FsDatasetImpl#addVolume |  Minor | . | Chen Liang | Chen Liang |
| [YARN-65](https://issues.apache.org/jira/browse/YARN-65) | Reduce RM app memory footprint once app has completed |  Major | resourcemanager | Jason Lowe | Manikandan R |
| [HDFS-5040](https://issues.apache.org/jira/browse/HDFS-5040) | Audit log for admin commands/ logging output of all DFS admin commands |  Major | namenode | Raghu C Doppalapudi | Kuhu Shukla |
| [HDFS-12560](https://issues.apache.org/jira/browse/HDFS-12560) |  Remove the extra word "it" in HdfsUserGuide.md |  Trivial | . | fang zhenyi | fang zhenyi |
| [YARN-6333](https://issues.apache.org/jira/browse/YARN-6333) | Improve doc for minSharePreemptionTimeout, fairSharePreemptionTimeout and fairSharePreemptionThreshold |  Major | fairscheduler | Yufei Gu | Chetna Chaudhari |
| [HDFS-12552](https://issues.apache.org/jira/browse/HDFS-12552) | Use slf4j instead of log4j in FSNamesystem |  Major | . | Ajay Kumar | Ajay Kumar |
| [HADOOP-14908](https://issues.apache.org/jira/browse/HADOOP-14908) | CrossOriginFilter should trigger regex on more input |  Major | common, security | Allen Wittenauer | Johannes Alberti |
| [HDFS-12455](https://issues.apache.org/jira/browse/HDFS-12455) | WebHDFS - Adding "snapshot enabled" status to ListStatus query result. |  Major | snapshots, webhdfs | Ajay Kumar | Ajay Kumar |
| [HDFS-12420](https://issues.apache.org/jira/browse/HDFS-12420) | Add an option to disallow 'namenode format -force' |  Major | . | Ajay Kumar | Ajay Kumar |
| [YARN-2162](https://issues.apache.org/jira/browse/YARN-2162) | add ability in Fair Scheduler to optionally configure maxResources in terms of percentage |  Major | fairscheduler, scheduler | Ashwin Shankar | Yufei Gu |
| [YARN-7207](https://issues.apache.org/jira/browse/YARN-7207) | Cache the RM proxy server address |  Major | RM | Yufei Gu | Yufei Gu |
| [HADOOP-14920](https://issues.apache.org/jira/browse/HADOOP-14920) | KMSClientProvider won't work with KMS delegation token retrieved from non-Java client. |  Major | kms | Xiaoyu Yao | Xiaoyu Yao |
| [HADOOP-14184](https://issues.apache.org/jira/browse/HADOOP-14184) | Remove service loader config entry for ftp fs |  Minor | fs | John Zhuge | Sen Zhao |
| [HDFS-12542](https://issues.apache.org/jira/browse/HDFS-12542) | Update javadoc and documentation for listStatus |  Major | documentation | Ajay Kumar | Ajay Kumar |
| [YARN-7359](https://issues.apache.org/jira/browse/YARN-7359) | TestAppManager.testQueueSubmitWithNoPermission() should be scheduler agnostic |  Minor | . | Haibo Chen | Haibo Chen |
| [YARN-7261](https://issues.apache.org/jira/browse/YARN-7261) | Add debug message for better download latency monitoring |  Major | nodemanager | Yufei Gu | Yufei Gu |
| [HDFS-12650](https://issues.apache.org/jira/browse/HDFS-12650) | Use slf4j instead of log4j in LeaseManager |  Major | . | Ajay Kumar | Ajay Kumar |
| [YARN-7357](https://issues.apache.org/jira/browse/YARN-7357) | Several methods in TestZKRMStateStore.TestZKRMStateStoreTester.TestZKRMStateStoreInternal should have @Override annotations |  Trivial | resourcemanager | Daniel Templeton | Sen Zhao |
| [YARN-4163](https://issues.apache.org/jira/browse/YARN-4163) | Audit getQueueInfo and getApplications calls |  Major | . | Chang Li | Chang Li |
| [HADOOP-9657](https://issues.apache.org/jira/browse/HADOOP-9657) | NetUtils.wrapException to have special handling for 0.0.0.0 addresses and :0 ports |  Minor | net | Steve Loughran | Varun Saxena |
| [YARN-7397](https://issues.apache.org/jira/browse/YARN-7397) | Reduce lock contention in FairScheduler#getAppWeight() |  Major | fairscheduler | Daniel Templeton | Daniel Templeton |
| [HDFS-7878](https://issues.apache.org/jira/browse/HDFS-7878) | API - expose a unique file identifier |  Major | . | Sergey Shelukhin | Chris Douglas |
| [YARN-6413](https://issues.apache.org/jira/browse/YARN-6413) | FileSystem based Yarn Registry implementation |  Major | amrmproxy, api, resourcemanager | Ellen Hui | Ellen Hui |
| [HDFS-12771](https://issues.apache.org/jira/browse/HDFS-12771) | Add genstamp and block size to metasave Corrupt blocks list |  Minor | . | Kuhu Shukla | Kuhu Shukla |
| [HDFS-10528](https://issues.apache.org/jira/browse/HDFS-10528) | Add logging to successful standby checkpointing |  Major | namenode | Xiaoyu Yao | Xiaoyu Yao |
| [YARN-7401](https://issues.apache.org/jira/browse/YARN-7401) | Reduce lock contention in ClusterNodeTracker#getClusterCapacity() |  Major | resourcemanager | Daniel Templeton | Daniel Templeton |
| [HDFS-7060](https://issues.apache.org/jira/browse/HDFS-7060) | Avoid taking locks when sending heartbeats from the DataNode |  Major | . | Haohui Mai | Jiandan Yang |
| [HADOOP-14872](https://issues.apache.org/jira/browse/HADOOP-14872) | CryptoInputStream should implement unbuffer |  Major | fs, security | John Zhuge | John Zhuge |
| [YARN-7413](https://issues.apache.org/jira/browse/YARN-7413) | Support resource type in SLS |  Major | scheduler-load-simulator | Yufei Gu | Yufei Gu |
| [HADOOP-14876](https://issues.apache.org/jira/browse/HADOOP-14876) | Create downstream developer docs from the compatibility guidelines |  Critical | documentation | Daniel Templeton | Daniel Templeton |
| [YARN-7414](https://issues.apache.org/jira/browse/YARN-7414) | FairScheduler#getAppWeight() should be moved into FSAppAttempt#getWeight() |  Minor | fairscheduler | Daniel Templeton | Soumabrata Chakraborty |
| [HDFS-12814](https://issues.apache.org/jira/browse/HDFS-12814) | Add blockId when warning slow mirror/disk in BlockReceiver |  Trivial | hdfs | Jiandan Yang | Jiandan Yang |
| [HADOOP-13514](https://issues.apache.org/jira/browse/HADOOP-13514) | Upgrade maven surefire plugin to 2.20.1 |  Major | build | Ewan Higgs | Akira Ajisaka |
| [YARN-7524](https://issues.apache.org/jira/browse/YARN-7524) | Remove unused FairSchedulerEventLog |  Major | fairscheduler | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [YARN-6851](https://issues.apache.org/jira/browse/YARN-6851) | Capacity Scheduler: document configs for controlling # containers allowed to be allocated per node heartbeat |  Minor | . | Wei Yan | Wei Yan |
| [YARN-7495](https://issues.apache.org/jira/browse/YARN-7495) | Improve robustness of the AggregatedLogDeletionService |  Major | log-aggregation | Jonathan Eagles | Jonathan Eagles |
| [HDFS-12594](https://issues.apache.org/jira/browse/HDFS-12594) | snapshotDiff fails if the report exceeds the RPC response limit |  Major | hdfs | Shashikant Banerjee | Shashikant Banerjee |
| [HDFS-12877](https://issues.apache.org/jira/browse/HDFS-12877) | Add open(PathHandle) with default buffersize |  Trivial | . | Chris Douglas | Chris Douglas |
| [HADOOP-14976](https://issues.apache.org/jira/browse/HADOOP-14976) | Set HADOOP\_SHELL\_EXECNAME explicitly in scripts |  Major | . | Arpit Agarwal | Arpit Agarwal |
| [HADOOP-15039](https://issues.apache.org/jira/browse/HADOOP-15039) | Move SemaphoredDelegatingExecutor to hadoop-common |  Minor | fs, fs/oss, fs/s3 | Genmao Yu | Genmao Yu |
| [YARN-6483](https://issues.apache.org/jira/browse/YARN-6483) | Add nodes transitioning to DECOMMISSIONING state to the list of updated nodes returned to the AM |  Major | resourcemanager | Juan Rodríguez Hortalá | Juan Rodríguez Hortalá |
| [HADOOP-15056](https://issues.apache.org/jira/browse/HADOOP-15056) | Fix TestUnbuffer#testUnbufferException failure |  Minor | test | Jack Bearden | Jack Bearden |
| [HADOOP-15012](https://issues.apache.org/jira/browse/HADOOP-15012) | Add readahead, dropbehind, and unbuffer to StreamCapabilities |  Major | fs | John Zhuge | John Zhuge |
| [HADOOP-15104](https://issues.apache.org/jira/browse/HADOOP-15104) | AliyunOSS: change the default value of max error retry |  Major | fs/oss | wujinhu | wujinhu |
| [YARN-7274](https://issues.apache.org/jira/browse/YARN-7274) | Ability to disable elasticity at leaf queue level |  Major | capacityscheduler | Scott Brokaw | Zian Chen |
| [HDFS-12882](https://issues.apache.org/jira/browse/HDFS-12882) | Support full open(PathHandle) contract in HDFS |  Major | hdfs-client | Chris Douglas | Chris Douglas |
| [YARN-7625](https://issues.apache.org/jira/browse/YARN-7625) | Expose NM node/containers resource utilization in JVM metrics |  Major | nodemanager | Weiwei Yang | Weiwei Yang |
| [HADOOP-14914](https://issues.apache.org/jira/browse/HADOOP-14914) | Change to a safely casting long to int. |  Major | . | Yufei Gu | Ajay Kumar |
| [HDFS-12910](https://issues.apache.org/jira/browse/HDFS-12910) | Secure Datanode Starter should log the port when it fails to bind |  Minor | datanode | Stephen O'Donnell | Stephen O'Donnell |
| [YARN-7642](https://issues.apache.org/jira/browse/YARN-7642) | Add test case to verify context update after container promotion or demotion with or without auto update |  Minor | nodemanager | Weiwei Yang | Weiwei Yang |
| [YARN-5418](https://issues.apache.org/jira/browse/YARN-5418) | When partial log aggregation is enabled, display the list of aggregated files on the container log page |  Major | . | Siddharth Seth | Xuan Gong |
| [HADOOP-15106](https://issues.apache.org/jira/browse/HADOOP-15106) | FileSystem::open(PathHandle) should throw a specific exception on validation failure |  Minor | . | Chris Douglas | Chris Douglas |
| [HDFS-12818](https://issues.apache.org/jira/browse/HDFS-12818) | Support multiple storages in DataNodeCluster / SimulatedFSDataset |  Minor | datanode, test | Erik Krogen | Erik Krogen |
| [HDFS-12932](https://issues.apache.org/jira/browse/HDFS-12932) | Fix confusing LOG message for block replication |  Minor | hdfs | Chao Sun | Chao Sun |
| [HDFS-9023](https://issues.apache.org/jira/browse/HDFS-9023) | When NN is not able to identify DN for replication, reason behind it can be logged |  Critical | hdfs-client, namenode | Surendra Singh Lilhore | Xiao Chen |
| [YARN-7580](https://issues.apache.org/jira/browse/YARN-7580) | ContainersMonitorImpl logged message lacks detail when exceeding memory limits |  Major | nodemanager | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [HDFS-12351](https://issues.apache.org/jira/browse/HDFS-12351) | Explicitly describe the minimal number of DataNodes required to support an EC policy in EC document. |  Minor | documentation, erasure-coding | Lei (Eddy) Xu | Hanisha Koneru |
| [HDFS-12629](https://issues.apache.org/jira/browse/HDFS-12629) | NameNode UI should report total blocks count by type - replicated and erasure coded |  Major | hdfs | Manoj Govindassamy | Manoj Govindassamy |
| [YARN-7687](https://issues.apache.org/jira/browse/YARN-7687) | ContainerLogAppender Improvements |  Trivial | . | BELUGA BEHR |  |
| [YARN-7688](https://issues.apache.org/jira/browse/YARN-7688) | Miscellaneous Improvements To ProcfsBasedProcessTree |  Minor | nodemanager | BELUGA BEHR |  |
| [HDFS-11847](https://issues.apache.org/jira/browse/HDFS-11847) | Enhance dfsadmin listOpenFiles command to list files blocking datanode decommissioning |  Major | hdfs | Manoj Govindassamy | Manoj Govindassamy |
| [YARN-7678](https://issues.apache.org/jira/browse/YARN-7678) | Ability to enable logging of container memory stats |  Major | nodemanager | Jim Brennan | Jim Brennan |
| [HDFS-11848](https://issues.apache.org/jira/browse/HDFS-11848) | Enhance dfsadmin listOpenFiles command to list files under a given path |  Major | . | Manoj Govindassamy | Yiqun Lin |
| [HDFS-12945](https://issues.apache.org/jira/browse/HDFS-12945) | Switch to ClientProtocol instead of NamenodeProtocols in NamenodeWebHdfsMethods |  Minor | . | Wei Yan | Wei Yan |
| [HDFS-12808](https://issues.apache.org/jira/browse/HDFS-12808) | Add LOG.isDebugEnabled() guard for LOG.debug("...") |  Minor | . | Mehran Hassani | Bharat Viswanadham |
| [YARN-7722](https://issues.apache.org/jira/browse/YARN-7722) | Rename variables in MockNM, MockRM for better clarity |  Trivial | . | lovekesh bansal | lovekesh bansal |
| [YARN-7622](https://issues.apache.org/jira/browse/YARN-7622) | Allow fair-scheduler configuration on HDFS |  Minor | fairscheduler, resourcemanager | Greg Phillips | Greg Phillips |
| [HADOOP-15033](https://issues.apache.org/jira/browse/HADOOP-15033) | Use java.util.zip.CRC32C for Java 9 and above |  Major | performance, util | Dmitry Chuyko | Dmitry Chuyko |
| [YARN-7590](https://issues.apache.org/jira/browse/YARN-7590) | Improve container-executor validation check |  Major | security, yarn | Eric Yang | Eric Yang |
| [HADOOP-15157](https://issues.apache.org/jira/browse/HADOOP-15157) | Zookeeper authentication related properties to support CredentialProviders |  Minor | security | Gergo Repas | Gergo Repas |
| [MAPREDUCE-7029](https://issues.apache.org/jira/browse/MAPREDUCE-7029) | FileOutputCommitter is slow on filesystems lacking recursive delete |  Minor | . | Karthik Palaniappan | Karthik Palaniappan |
| [HADOOP-15114](https://issues.apache.org/jira/browse/HADOOP-15114) | Add closeStreams(...) to IOUtils |  Major | . | Ajay Kumar | Ajay Kumar |
| [MAPREDUCE-6984](https://issues.apache.org/jira/browse/MAPREDUCE-6984) | MR AM to clean up temporary files from previous attempt in case of no recovery |  Major | applicationmaster | Gergo Repas | Gergo Repas |
| [HDFS-13036](https://issues.apache.org/jira/browse/HDFS-13036) | Reusing the volume storage ID obtained by replicaInfo |  Major | datanode | liaoyuxiangqin | liaoyuxiangqin |
| [YARN-7755](https://issues.apache.org/jira/browse/YARN-7755) | Clean up deprecation messages for allocation increments in FS config |  Minor | fairscheduler | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [MAPREDUCE-7022](https://issues.apache.org/jira/browse/MAPREDUCE-7022) | Fast fail rogue jobs based on task scratch dir size |  Major | task | Johan Gustavsson | Johan Gustavsson |
| [YARN-2185](https://issues.apache.org/jira/browse/YARN-2185) | Use pipes when localizing archives |  Major | nodemanager | Jason Lowe | Miklos Szegedi |
| [HDFS-13092](https://issues.apache.org/jira/browse/HDFS-13092) | Reduce verbosity for ThrottledAsyncChecker.java:schedule |  Minor | datanode | Mukul Kumar Singh | Mukul Kumar Singh |
| [HDFS-13062](https://issues.apache.org/jira/browse/HDFS-13062) | Provide support for JN to use separate journal disk per namespace |  Major | federation, journal-node | Bharat Viswanadham | Bharat Viswanadham |
| [HADOOP-15170](https://issues.apache.org/jira/browse/HADOOP-15170) | Add symlink support to FileUtil#unTarUsingJava |  Minor | util | Jason Lowe | Ajay Kumar |
| [HADOOP-15168](https://issues.apache.org/jira/browse/HADOOP-15168) | Add kdiag tool to hadoop command |  Minor | . | Bharat Viswanadham | Bharat Viswanadham |
| [HDFS-13073](https://issues.apache.org/jira/browse/HDFS-13073) | Cleanup code in InterQJournalProtocol.proto |  Minor | journal-node | Bharat Viswanadham | Bharat Viswanadham |
| [HADOOP-15212](https://issues.apache.org/jira/browse/HADOOP-15212) | Add independent secret manager method for logging expired tokens |  Major | security | Daryn Sharp | Daryn Sharp |
| [YARN-7841](https://issues.apache.org/jira/browse/YARN-7841) | Cleanup AllocationFileLoaderService's reloadAllocations method |  Minor | yarn | Szilard Nemeth | Szilard Nemeth |
| [HDFS-12947](https://issues.apache.org/jira/browse/HDFS-12947) | Limit the number of Snapshots allowed to be created for a Snapshottable Directory |  Major | snapshots | Shashikant Banerjee | Shashikant Banerjee |
| [HDFS-12933](https://issues.apache.org/jira/browse/HDFS-12933) | Improve logging when DFSStripedOutputStream failed to write some blocks |  Minor | erasure-coding | Xiao Chen | chencan |
| [YARN-7728](https://issues.apache.org/jira/browse/YARN-7728) | Expose container preemptions related information in Capacity Scheduler queue metrics |  Major | . | Eric Payne | Eric Payne |
| [YARN-7655](https://issues.apache.org/jira/browse/YARN-7655) | Avoid AM preemption caused by RRs for specific nodes or racks |  Major | fairscheduler | Steven Rand | Steven Rand |
| [HADOOP-15187](https://issues.apache.org/jira/browse/HADOOP-15187) | Remove ADL mock test dependency on REST call invoked from Java SDK |  Major | fs/adl | Vishwajeet Dusane | Vishwajeet Dusane |
| [MAPREDUCE-7048](https://issues.apache.org/jira/browse/MAPREDUCE-7048) | Uber AM can crash due to unknown task in statusUpdate |  Major | mr-am | Peter Bacsko | Peter Bacsko |
| [HADOOP-15195](https://issues.apache.org/jira/browse/HADOOP-15195) | With SELinux enabled, directories mounted with start-build-env.sh may not be accessible. |  Major | build | Grigori Rybkine | Grigori Rybkine |
| [HADOOP-14531](https://issues.apache.org/jira/browse/HADOOP-14531) | [Umbrella] Improve S3A error handling & reporting |  Blocker | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-15204](https://issues.apache.org/jira/browse/HADOOP-15204) | Add Configuration API for parsing storage sizes |  Minor | conf | Anu Engineer | Anu Engineer |
| [HDFS-13142](https://issues.apache.org/jira/browse/HDFS-13142) | Define and Implement a DiifList Interface to store and manage SnapshotDiffs |  Major | snapshots | Shashikant Banerjee | Shashikant Banerjee |
| [HADOOP-13972](https://issues.apache.org/jira/browse/HADOOP-13972) | ADLS to support per-store configuration |  Major | fs/adl | John Zhuge | Sharad Sonker |
| [HDFS-13153](https://issues.apache.org/jira/browse/HDFS-13153) | Enable HDFS diskbalancer by default |  Major | diskbalancer | Ajay Kumar | Ajay Kumar |
| [HADOOP-14875](https://issues.apache.org/jira/browse/HADOOP-14875) | Create end user documentation from the compatibility guidelines |  Critical | documentation | Daniel Templeton | Daniel Templeton |
| [HADOOP-15070](https://issues.apache.org/jira/browse/HADOOP-15070) | add test to verify FileSystem and paths differentiate on user info |  Minor | fs, test | Steve Loughran | Steve Loughran |
| [YARN-7813](https://issues.apache.org/jira/browse/YARN-7813) | Capacity Scheduler Intra-queue Preemption should be configurable for each queue |  Major | capacity scheduler, scheduler preemption | Eric Payne | Eric Payne |
| [HDFS-13168](https://issues.apache.org/jira/browse/HDFS-13168) | XmlImageVisitor - Prefer Array over LinkedList |  Minor | hdfs | BELUGA BEHR | BELUGA BEHR |
| [HDFS-13167](https://issues.apache.org/jira/browse/HDFS-13167) | DatanodeAdminManager Improvements |  Trivial | hdfs | BELUGA BEHR | BELUGA BEHR |
| [HADOOP-15235](https://issues.apache.org/jira/browse/HADOOP-15235) | Authentication Tokens should use HMAC instead of MAC |  Major | security | Robert Kanter | Robert Kanter |
| [HADOOP-12897](https://issues.apache.org/jira/browse/HADOOP-12897) | KerberosAuthenticator.authenticate to include URL on IO failures |  Minor | security | Steve Loughran | Ajay Kumar |
| [HDFS-13175](https://issues.apache.org/jira/browse/HDFS-13175) | Add more information for checking argument in DiskBalancerVolume |  Minor | diskbalancer | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HDFS-11187](https://issues.apache.org/jira/browse/HDFS-11187) | Optimize disk access for last partial chunk checksum of Finalized replica |  Major | datanode | Wei-Chiu Chuang | Gabor Bota |
| [HADOOP-15255](https://issues.apache.org/jira/browse/HADOOP-15255) | Upper/Lower case conversion support for group names in LdapGroupsMapping |  Major | . | Nanda kumar | Nanda kumar |
| [HADOOP-13374](https://issues.apache.org/jira/browse/HADOOP-13374) | Add the L&N verification script |  Major | . | Xiao Chen | Allen Wittenauer |
| [HADOOP-15178](https://issues.apache.org/jira/browse/HADOOP-15178) | Generalize NetUtils#wrapException to handle other subclasses with String Constructor |  Major | . | Ajay Kumar | Ajay Kumar |
| [HDFS-13193](https://issues.apache.org/jira/browse/HDFS-13193) | Various Improvements for BlockTokenSecretManager |  Trivial | hdfs | BELUGA BEHR | BELUGA BEHR |
| [HADOOP-14959](https://issues.apache.org/jira/browse/HADOOP-14959) | DelegationTokenAuthenticator.authenticate() to wrap network exceptions |  Minor | net, security | Steve Loughran | Ajay Kumar |
| [MAPREDUCE-7010](https://issues.apache.org/jira/browse/MAPREDUCE-7010) | Make Job History File Permissions configurable |  Major | . | Andras Bokor | Gergely Novák |
| [HDFS-13192](https://issues.apache.org/jira/browse/HDFS-13192) | Change the code order in getFileEncryptionInfo to avoid unnecessary call of assignment |  Minor | encryption | LiXin Ge | LiXin Ge |
| [MAPREDUCE-7061](https://issues.apache.org/jira/browse/MAPREDUCE-7061) | SingleCluster setup document needs to be updated |  Major | . | Bharat Viswanadham | Bharat Viswanadham |
| [HADOOP-15263](https://issues.apache.org/jira/browse/HADOOP-15263) | hadoop cloud-storage module to mark hadoop-common as provided; add azure-datalake |  Minor | build | Steve Loughran | Steve Loughran |
| [HADOOP-15007](https://issues.apache.org/jira/browse/HADOOP-15007) | Stabilize and document Configuration \<tag\> element |  Blocker | conf | Steve Loughran | Ajay Kumar |
| [HDFS-13102](https://issues.apache.org/jira/browse/HDFS-13102) | Implement SnapshotSkipList class to store Multi level DirectoryDiffs |  Major | snapshots | Shashikant Banerjee | Shashikant Banerjee |
| [HDFS-13202](https://issues.apache.org/jira/browse/HDFS-13202) | Fix the outdated javadocs in HAUtil |  Trivial | . | Chao Sun | Chao Sun |
| [YARN-5028](https://issues.apache.org/jira/browse/YARN-5028) | RMStateStore should trim down app state for completed applications |  Major | resourcemanager | Karthik Kambatla | Gergo Repas |
| [HADOOP-15279](https://issues.apache.org/jira/browse/HADOOP-15279) | increase maven heap size recommendations |  Minor | build, documentation, test | Allen Wittenauer | Allen Wittenauer |
| [HDFS-13171](https://issues.apache.org/jira/browse/HDFS-13171) | Handle Deletion of nodes in SnasphotSkipList |  Major | snapshots | Shashikant Banerjee | Shashikant Banerjee |
| [HADOOP-15252](https://issues.apache.org/jira/browse/HADOOP-15252) | Checkstyle version is not compatible with IDEA's checkstyle plugin |  Major | . | Andras Bokor | Andras Bokor |
| [HDFS-13173](https://issues.apache.org/jira/browse/HDFS-13173) | Replace ArrayList with DirectoryDiffList(SnapshotSkipList) to store DirectoryDiffs |  Major | snapshots | Shashikant Banerjee | Shashikant Banerjee |
| [HADOOP-15282](https://issues.apache.org/jira/browse/HADOOP-15282) | HADOOP-15235 broke TestHttpFSServerWebServer |  Major | test | Robert Kanter | Robert Kanter |
| [HDFS-13170](https://issues.apache.org/jira/browse/HDFS-13170) | Port webhdfs unmaskedpermission parameter to HTTPFS |  Major | . | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-13223](https://issues.apache.org/jira/browse/HDFS-13223) | Reduce DiffListBySkipList memory usage |  Major | snapshots | Tsz Wo Nicholas Sze | Shashikant Banerjee |
| [HDFS-13227](https://issues.apache.org/jira/browse/HDFS-13227) | Add a method to  calculate cumulative diff over multiple snapshots in DirectoryDiffList |  Minor | snapshots | Shashikant Banerjee | Shashikant Banerjee |
| [HDFS-13222](https://issues.apache.org/jira/browse/HDFS-13222) | Update getBlocks method to take minBlockSize in RPC calls |  Major | balancer & mover | Bharat Viswanadham | Bharat Viswanadham |
| [HDFS-13225](https://issues.apache.org/jira/browse/HDFS-13225) | StripeReader#checkMissingBlocks() 's IOException info is incomplete |  Major | erasure-coding, hdfs-client | lufei | lufei |
| [HDFS-11394](https://issues.apache.org/jira/browse/HDFS-11394) | Support for getting erasure coding policy through WebHDFS#FileStatus |  Major | erasure-coding, namenode | Kai Sasaki | Kai Sasaki |
| [HDFS-13252](https://issues.apache.org/jira/browse/HDFS-13252) | Code refactoring: Remove Diff.ListType |  Major | snapshots | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-12780](https://issues.apache.org/jira/browse/HDFS-12780) | Fix spelling mistake in DistCpUtils.java |  Trivial | . | Jianfei Jiang | Jianfei Jiang |
| [HADOOP-15311](https://issues.apache.org/jira/browse/HADOOP-15311) | HttpServer2 needs a way to configure the acceptor/selector count |  Major | common | Erik Krogen | Erik Krogen |
| [HDFS-13235](https://issues.apache.org/jira/browse/HDFS-13235) | DiskBalancer: Update Documentation to add newly added options |  Major | diskbalancer, documentation | Bharat Viswanadham | Bharat Viswanadham |
| [HDFS-336](https://issues.apache.org/jira/browse/HDFS-336) | dfsadmin -report should report number of blocks from datanode |  Minor | . | Lohit Vijayarenu | Bharat Viswanadham |
| [HDFS-11600](https://issues.apache.org/jira/browse/HDFS-11600) | Refactor TestDFSStripedOutputStreamWithFailure test classes |  Minor | erasure-coding, test | Andrew Wang | Sammi Chen |
| [HDFS-13257](https://issues.apache.org/jira/browse/HDFS-13257) | Code cleanup: INode never throws QuotaExceededException |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-13275](https://issues.apache.org/jira/browse/HDFS-13275) | Adding log for BlockPoolManager#refreshNamenodes failures |  Minor | datanode | Xiaoyu Yao | Ajay Kumar |
| [HDFS-13246](https://issues.apache.org/jira/browse/HDFS-13246) | FileInputStream redundant closes in readReplicasFromCache |  Minor | datanode | liaoyuxiangqin | liaoyuxiangqin |
| [HADOOP-15209](https://issues.apache.org/jira/browse/HADOOP-15209) | DistCp to eliminate needless deletion of files under already-deleted directories |  Major | tools/distcp | Steve Loughran | Steve Loughran |
| [MAPREDUCE-7047](https://issues.apache.org/jira/browse/MAPREDUCE-7047) | Make HAR tool support IndexedLogAggregtionController |  Major | . | Xuan Gong | Xuan Gong |
| [HDFS-12884](https://issues.apache.org/jira/browse/HDFS-12884) | BlockUnderConstructionFeature.truncateBlock should be of type BlockInfo |  Major | namenode | Konstantin Shvachko | chencan |
| [YARN-7064](https://issues.apache.org/jira/browse/YARN-7064) | Use cgroup to get container resource utilization |  Major | nodemanager | Miklos Szegedi | Miklos Szegedi |
| [HADOOP-15334](https://issues.apache.org/jira/browse/HADOOP-15334) | Upgrade Maven surefire plugin |  Major | build | Arpit Agarwal | Arpit Agarwal |
| [HADOOP-14825](https://issues.apache.org/jira/browse/HADOOP-14825) | Über-JIRA: S3Guard Phase II: Hadoop 3.1 features |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-15312](https://issues.apache.org/jira/browse/HADOOP-15312) | Undocumented KeyProvider configuration keys |  Major | . | Wei-Chiu Chuang | LiXin Ge |
| [YARN-7623](https://issues.apache.org/jira/browse/YARN-7623) | Fix the CapacityScheduler Queue configuration documentation |  Major | . | Arun Suresh | Jonathan Hung |
| [HDFS-13314](https://issues.apache.org/jira/browse/HDFS-13314) | NameNode should optionally exit if it detects FsImage corruption |  Major | namenode | Arpit Agarwal | Arpit Agarwal |
| [YARN-8076](https://issues.apache.org/jira/browse/YARN-8076) | Support to specify application tags in distributed shell |  Major | distributed-shell | Weiwei Yang | Weiwei Yang |
| [HADOOP-14831](https://issues.apache.org/jira/browse/HADOOP-14831) | Über-jira: S3a phase IV: Hadoop 3.1 features |  Blocker | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-5881](https://issues.apache.org/jira/browse/YARN-5881) | [Umbrella] Enable configuration of queue capacity in terms of absolute resources |  Major | . | Sean Po | Sunil Govindan |
| [HADOOP-14841](https://issues.apache.org/jira/browse/HADOOP-14841) | Kms client should disconnect if unable to get output stream from connection. |  Major | kms | Xiao Chen | Rushabh S Shah |
| [HDFS-13493](https://issues.apache.org/jira/browse/HDFS-13493) | Reduce the HttpServer2 thread count on DataNodes |  Major | datanode | Erik Krogen | Erik Krogen |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-7023](https://issues.apache.org/jira/browse/YARN-7023) | Incorrect ReservationId.compareTo() implementation |  Minor | reservation system | Oleg Danilov | Oleg Danilov |
| [YARN-7152](https://issues.apache.org/jira/browse/YARN-7152) | [ATSv2] Registering timeline client before AMRMClient service init throw exception. |  Major | timelineclient | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-6992](https://issues.apache.org/jira/browse/YARN-6992) | Kill application button is visible even if the application is FINISHED in RM UI |  Major | . | Sumana Sathish | Suma Shivaprasad |
| [YARN-7140](https://issues.apache.org/jira/browse/YARN-7140) | CollectorInfo should have Public visibility |  Minor | . | Varun Saxena | Varun Saxena |
| [YARN-7130](https://issues.apache.org/jira/browse/YARN-7130) | ATSv2 documentation changes post merge |  Major | timelineserver | Varun Saxena | Varun Saxena |
| [HDFS-12406](https://issues.apache.org/jira/browse/HDFS-12406) | dfsadmin command prints "Exception encountered" even if there is no exception, when debug is enabled |  Minor | hdfs-client | Nanda kumar | Nanda kumar |
| [YARN-4727](https://issues.apache.org/jira/browse/YARN-4727) | Unable to override the $HADOOP\_CONF\_DIR env variable for container |  Major | nodemanager | Terence Yim | Jason Lowe |
| [YARN-7163](https://issues.apache.org/jira/browse/YARN-7163) | RMContext need not to be injected to webapp and other Always Running services. |  Blocker | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-12424](https://issues.apache.org/jira/browse/HDFS-12424) | Datatable sorting on the Datanode Information page in the Namenode UI is broken |  Major | . | Shawna Martell | Shawna Martell |
| [HDFS-12323](https://issues.apache.org/jira/browse/HDFS-12323) | NameNode terminates after full GC thinking QJM unresponsive if full GC is much longer than timeout |  Major | namenode, qjm | Erik Krogen | Erik Krogen |
| [YARN-7149](https://issues.apache.org/jira/browse/YARN-7149) | Cross-queue preemption sometimes starves an underserved queue |  Major | capacity scheduler | Eric Payne | Eric Payne |
| [YARN-7172](https://issues.apache.org/jira/browse/YARN-7172) | ResourceCalculator.fitsIn() should not take a cluster resource parameter |  Major | scheduler | Daniel Templeton | Sen Zhao |
| [YARN-7199](https://issues.apache.org/jira/browse/YARN-7199) | Fix TestAMRMClientContainerRequest.testOpportunisticAndGuaranteedRequests |  Blocker | . | Botong Huang | Botong Huang |
| [MAPREDUCE-6960](https://issues.apache.org/jira/browse/MAPREDUCE-6960) | Shuffle Handler prints disk error stack traces for every read failure. |  Major | . | Kuhu Shukla | Kuhu Shukla |
| [HDFS-12480](https://issues.apache.org/jira/browse/HDFS-12480) | TestNameNodeMetrics#testTransactionAndCheckpointMetrics Fails in trunk |  Blocker | test | Brahma Reddy Battula | Hanisha Koneru |
| [HDFS-11799](https://issues.apache.org/jira/browse/HDFS-11799) | Introduce a config to allow setting up write pipeline with fewer nodes than replication factor |  Major | . | Yongjun Zhang | Brahma Reddy Battula |
| [YARN-7196](https://issues.apache.org/jira/browse/YARN-7196) | Fix finicky TestContainerManager tests |  Major | . | Arun Suresh | Arun Suresh |
| [YARN-6771](https://issues.apache.org/jira/browse/YARN-6771) | Use classloader inside configuration class to make new classes |  Major | . | Jongyoul Lee | Jongyoul Lee |
| [HDFS-12526](https://issues.apache.org/jira/browse/HDFS-12526) | FSDirectory should use Time.monotonicNow for durations |  Minor | . | Chetna Chaudhari | Bharat Viswanadham |
| [HDFS-12371](https://issues.apache.org/jira/browse/HDFS-12371) | "BlockVerificationFailures" and "BlocksVerified" show up as 0 in Datanode JMX |  Major | metrics | Sai Nukavarapu | Hanisha Koneru |
| [YARN-7034](https://issues.apache.org/jira/browse/YARN-7034) | DefaultLinuxContainerRuntime and DockerLinuxContainerRuntime sends client environment variables to container-executor |  Blocker | nodemanager | Miklos Szegedi | Miklos Szegedi |
| [HDFS-12507](https://issues.apache.org/jira/browse/HDFS-12507) | StripedBlockUtil.java:694: warning - Tag @link: reference not found: StripingCell |  Minor | documentation | Tsz Wo Nicholas Sze | Mukul Kumar Singh |
| [MAPREDUCE-6966](https://issues.apache.org/jira/browse/MAPREDUCE-6966) | DistSum should use Time.monotonicNow for measuring durations |  Minor | . | Chetna Chaudhari | Chetna Chaudhari |
| [YARN-6878](https://issues.apache.org/jira/browse/YARN-6878) | TestCapacityScheduler.testDefaultNodeLabelExpressionQueueConfig() has the args to assertEqual() in the wrong order |  Trivial | capacity scheduler, test | Daniel Templeton | Sen Zhao |
| [HDFS-12064](https://issues.apache.org/jira/browse/HDFS-12064) | Reuse object mapper in HDFS |  Minor | . | Mingliang Liu | Hanisha Koneru |
| [HDFS-12535](https://issues.apache.org/jira/browse/HDFS-12535) | Change the Scope of the Class DFSUtilClient to Private |  Major | . | Bharat Viswanadham | Bharat Viswanadham |
| [HDFS-12536](https://issues.apache.org/jira/browse/HDFS-12536) | Add documentation for getconf command with -journalnodes option |  Major | . | Bharat Viswanadham | Bharat Viswanadham |
| [HADOOP-14905](https://issues.apache.org/jira/browse/HADOOP-14905) | Fix javadocs issues in Hadoop HDFS-NFS |  Major | nfs | Mukul Kumar Singh | Mukul Kumar Singh |
| [HADOOP-14904](https://issues.apache.org/jira/browse/HADOOP-14904) | Fix javadocs issues in Hadoop HDFS |  Minor | . | Mukul Kumar Singh | Mukul Kumar Singh |
| [HDFS-12339](https://issues.apache.org/jira/browse/HDFS-12339) | NFS Gateway on Shutdown Gives Unregistration Failure. Does Not Unregister with rpcbind Portmapper |  Major | nfs | Sailesh Patel | Mukul Kumar Singh |
| [HDFS-12375](https://issues.apache.org/jira/browse/HDFS-12375) | Fail to start/stop journalnodes using start-dfs.sh/stop-dfs.sh. |  Major | federation, journal-node, scripts | Wenxin He | Bharat Viswanadham |
| [YARN-7153](https://issues.apache.org/jira/browse/YARN-7153) | Remove duplicated code in AMRMClientAsyncImpl.java |  Minor | client | Sen Zhao | Sen Zhao |
| [HADOOP-14897](https://issues.apache.org/jira/browse/HADOOP-14897) | Loosen compatibility guidelines for native dependencies |  Blocker | documentation, native | Chris Douglas | Daniel Templeton |
| [HDFS-12529](https://issues.apache.org/jira/browse/HDFS-12529) | Get source for config tags from file name |  Major | . | Ajay Kumar | Ajay Kumar |
| [YARN-7118](https://issues.apache.org/jira/browse/YARN-7118) | AHS REST API can return NullPointerException |  Major | . | Prabhu Joseph | Billie Rinaldi |
| [HDFS-12495](https://issues.apache.org/jira/browse/HDFS-12495) | TestPendingInvalidateBlock#testPendingDeleteUnknownBlocks fails intermittently |  Major | . | Eric Badger | Eric Badger |
| [HADOOP-14822](https://issues.apache.org/jira/browse/HADOOP-14822) | hadoop-project/pom.xml is executable |  Minor | . | Akira Ajisaka | Ajay Kumar |
| [YARN-7157](https://issues.apache.org/jira/browse/YARN-7157) | Add admin configuration to filter per-user's apps in secure cluster |  Major | webapp | Sunil Govindan | Sunil Govindan |
| [YARN-7257](https://issues.apache.org/jira/browse/YARN-7257) | AggregatedLogsBlock reports a bad 'end' value as a bad 'start' value |  Major | log-aggregation | Jason Lowe | Jason Lowe |
| [YARN-7084](https://issues.apache.org/jira/browse/YARN-7084) | TestSchedulingMonitor#testRMStarts fails sporadically |  Major | . | Jason Lowe | Jason Lowe |
| [HDFS-12271](https://issues.apache.org/jira/browse/HDFS-12271) | Incorrect statement in Downgrade section of HDFS Rolling Upgrade document |  Minor | documentation | Nanda kumar | Nanda kumar |
| [HDFS-12576](https://issues.apache.org/jira/browse/HDFS-12576) | JournalNodes are getting started, even though dfs.namenode.shared.edits.dir is not configured |  Major | journal-node | Bharat Viswanadham | Bharat Viswanadham |
| [HDFS-11968](https://issues.apache.org/jira/browse/HDFS-11968) | ViewFS: StoragePolicies commands fail with HDFS federation |  Major | hdfs | Mukul Kumar Singh | Mukul Kumar Singh |
| [YARN-6943](https://issues.apache.org/jira/browse/YARN-6943) | Update Yarn to YARN in documentation |  Minor | documentation | Miklos Szegedi | Chetna Chaudhari |
| [YARN-7211](https://issues.apache.org/jira/browse/YARN-7211) | AMSimulator in SLS does't work due to refactor of responseId |  Blocker | scheduler-load-simulator | Yufei Gu | Botong Huang |
| [HADOOP-14459](https://issues.apache.org/jira/browse/HADOOP-14459) | SerializationFactory shouldn't throw a NullPointerException if the serializations list is not defined |  Minor | . | Nandor Kollar | Nandor Kollar |
| [YARN-7279](https://issues.apache.org/jira/browse/YARN-7279) | Fix typo in helper message of ContainerLauncher |  Trivial | . | Elek, Marton | Elek, Marton |
| [YARN-7258](https://issues.apache.org/jira/browse/YARN-7258) | Add Node and Rack Hints to Opportunistic Scheduler |  Major | . | Arun Suresh | kartheek muthyala |
| [YARN-7285](https://issues.apache.org/jira/browse/YARN-7285) | ContainerExecutor always launches with priorities due to yarn-default property |  Minor | nodemanager | Jason Lowe | Jason Lowe |
| [HDFS-12494](https://issues.apache.org/jira/browse/HDFS-12494) | libhdfs SIGSEGV in setTLSExceptionStrings |  Major | libhdfs | John Zhuge | John Zhuge |
| [YARN-7245](https://issues.apache.org/jira/browse/YARN-7245) | Max AM Resource column in Active Users Info section of Capacity Scheduler UI page should be updated per-user |  Major | capacity scheduler, yarn | Eric Payne | Eric Payne |
| [HDFS-11575](https://issues.apache.org/jira/browse/HDFS-11575) | Supporting HDFS NFS gateway with Federated HDFS |  Major | nfs | Mukul Kumar Singh | Mukul Kumar Singh |
| [HADOOP-14910](https://issues.apache.org/jira/browse/HADOOP-14910) | Upgrade netty-all jar to latest 4.0.x.Final |  Critical | . | Vinayakumar B | Vinayakumar B |
| [MAPREDUCE-6951](https://issues.apache.org/jira/browse/MAPREDUCE-6951) | Improve exception message when mapreduce.jobhistory.webapp.address is in wrong format |  Major | applicationmaster | Prabhu Joseph | Prabhu Joseph |
| [HDFS-12627](https://issues.apache.org/jira/browse/HDFS-12627) | Fix typo in DFSAdmin command output |  Trivial | . | Ajay Kumar | Ajay Kumar |
| [HADOOP-13102](https://issues.apache.org/jira/browse/HADOOP-13102) | Update GroupsMapping documentation to reflect the new changes |  Major | documentation | Anu Engineer | Esther Kundin |
| [YARN-7270](https://issues.apache.org/jira/browse/YARN-7270) | Fix unsafe casting from long to int for class Resource and its sub-classes |  Major | resourcemanager | Yufei Gu | Yufei Gu |
| [YARN-7124](https://issues.apache.org/jira/browse/YARN-7124) | LogAggregationTFileController deletes/renames while file is open |  Critical | nodemanager | Daryn Sharp | Jason Lowe |
| [YARN-7341](https://issues.apache.org/jira/browse/YARN-7341) | TestRouterWebServiceUtil#testMergeMetrics is flakey |  Major | federation | Robert Kanter | Robert Kanter |
| [HADOOP-14958](https://issues.apache.org/jira/browse/HADOOP-14958) | CLONE - Fix source-level compatibility after HADOOP-11252 |  Blocker | . | Junping Du | Junping Du |
| [YARN-7294](https://issues.apache.org/jira/browse/YARN-7294) | TestSignalContainer#testSignalRequestDeliveryToNM fails intermittently with Fair scheduler |  Major | . | Miklos Szegedi | Miklos Szegedi |
| [YARN-7355](https://issues.apache.org/jira/browse/YARN-7355) | TestDistributedShell should be scheduler agnostic |  Major | . | Haibo Chen | Haibo Chen |
| [HDFS-12683](https://issues.apache.org/jira/browse/HDFS-12683) | DFSZKFailOverController re-order logic for logging Exception |  Major | . | Bharat Viswanadham | Bharat Viswanadham |
| [HADOOP-14966](https://issues.apache.org/jira/browse/HADOOP-14966) | Handle JDK-8071638 for hadoop-common |  Blocker | . | Bibin A Chundatt | Bibin A Chundatt |
| [HDFS-12695](https://issues.apache.org/jira/browse/HDFS-12695) | Add a link to HDFS router federation document in site.xml |  Minor | documentation | Yiqun Lin | Yiqun Lin |
| [YARN-7385](https://issues.apache.org/jira/browse/YARN-7385) | TestFairScheduler#testUpdateDemand and TestFSLeafQueue#testUpdateDemand are failing with NPE |  Major | test | Robert Kanter | Yufei Gu |
| [HADOOP-14977](https://issues.apache.org/jira/browse/HADOOP-14977) | Xenial dockerfile needs ant in main build for findbugs |  Trivial | build, test | Allen Wittenauer | Akira Ajisaka |
| [HDFS-12579](https://issues.apache.org/jira/browse/HDFS-12579) | JournalNodeSyncer should use fromUrl field of EditLogManifestResponse to construct servlet Url |  Major | . | Hanisha Koneru | Hanisha Koneru |
| [YARN-7375](https://issues.apache.org/jira/browse/YARN-7375) | Possible NPE in RMWebapp when HA is enabled and the active RM fails |  Major | . | Chandni Singh | Chandni Singh |
| [YARN-6747](https://issues.apache.org/jira/browse/YARN-6747) | TestFSAppStarvation.testPreemptionEnable fails intermittently |  Major | . | Sunil Govindan | Miklos Szegedi |
| [YARN-7336](https://issues.apache.org/jira/browse/YARN-7336) | Unsafe cast from long to int Resource.hashCode() method |  Critical | resourcemanager | Daniel Templeton | Miklos Szegedi |
| [HADOOP-14990](https://issues.apache.org/jira/browse/HADOOP-14990) | Clean up jdiff xml files added for 2.8.2 release |  Blocker | . | Subru Krishnan | Junping Du |
| [HADOOP-14980](https://issues.apache.org/jira/browse/HADOOP-14980) | [JDK9] Upgrade maven-javadoc-plugin to 3.0.0-M1 |  Minor | build | ligongyi | ligongyi |
| [HDFS-12714](https://issues.apache.org/jira/browse/HDFS-12714) | Hadoop 3 missing fix for HDFS-5169 |  Major | native | Joe McDonnell | Joe McDonnell |
| [YARN-7146](https://issues.apache.org/jira/browse/YARN-7146) | Many RM unit tests failing with FairScheduler |  Major | test | Robert Kanter | Robert Kanter |
| [YARN-7396](https://issues.apache.org/jira/browse/YARN-7396) | NPE when accessing container logs due to null dirsHandler |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-7370](https://issues.apache.org/jira/browse/YARN-7370) | Preemption properties should be refreshable |  Major | capacity scheduler, scheduler preemption | Eric Payne | Gergely Novák |
| [YARN-7400](https://issues.apache.org/jira/browse/YARN-7400) | incorrect log preview displayed in jobhistory server ui |  Major | yarn | Santhosh B Gowda | Xuan Gong |
| [HADOOP-15013](https://issues.apache.org/jira/browse/HADOOP-15013) | Fix ResourceEstimator findbugs issues |  Blocker | . | Allen Wittenauer | Arun Suresh |
| [YARN-7432](https://issues.apache.org/jira/browse/YARN-7432) | Fix DominantResourceFairnessPolicy serializable findbugs issues |  Blocker | . | Allen Wittenauer | Daniel Templeton |
| [YARN-7434](https://issues.apache.org/jira/browse/YARN-7434) | Router getApps REST invocation fails with multiple RMs |  Critical | . | Subru Krishnan | Íñigo Goiri |
| [HADOOP-15015](https://issues.apache.org/jira/browse/HADOOP-15015) | TestConfigurationFieldsBase to use SLF4J for logging |  Trivial | conf, test | Steve Loughran | Steve Loughran |
| [YARN-7428](https://issues.apache.org/jira/browse/YARN-7428) | Add containerId to Localizer failed logs |  Minor | nodemanager | Prabhu Joseph | Prabhu Joseph |
| [YARN-4793](https://issues.apache.org/jira/browse/YARN-4793) | [Umbrella] Simplified API layer for services and beyond |  Major | . | Vinod Kumar Vavilapalli |  |
| [HADOOP-15018](https://issues.apache.org/jira/browse/HADOOP-15018) | Update JAVA\_HOME in create-release for Xenial Dockerfile |  Blocker | build | Andrew Wang | Andrew Wang |
| [HDFS-12788](https://issues.apache.org/jira/browse/HDFS-12788) | Reset the upload button when file upload fails |  Critical | ui, webhdfs | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-7453](https://issues.apache.org/jira/browse/YARN-7453) | Fix issue where RM fails to switch to active after first successful start |  Blocker | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-7458](https://issues.apache.org/jira/browse/YARN-7458) | TestContainerManagerSecurity is still flakey |  Major | test | Robert Kanter | Robert Kanter |
| [YARN-7454](https://issues.apache.org/jira/browse/YARN-7454) | RMAppAttemptMetrics#getAggregateResourceUsage can NPE due to double lookup |  Minor | resourcemanager | Jason Lowe | Jason Lowe |
| [YARN-7465](https://issues.apache.org/jira/browse/YARN-7465) | start-yarn.sh fails to start ResourceManager unless running as root |  Blocker | . | Sean Mackrory |  |
| [HDFS-12791](https://issues.apache.org/jira/browse/HDFS-12791) | NameNode Fsck http Connection can timeout for directories with multiple levels |  Major | tools | Mukul Kumar Singh | Mukul Kumar Singh |
| [HDFS-12797](https://issues.apache.org/jira/browse/HDFS-12797) | Add Test for NFS mount of not supported filesystems like (file:///) |  Minor | nfs | Mukul Kumar Singh | Mukul Kumar Singh |
| [HADOOP-14929](https://issues.apache.org/jira/browse/HADOOP-14929) | Cleanup usage of decodecomponent and use QueryStringDecoder from netty |  Major | . | Bharat Viswanadham | Bharat Viswanadham |
| [HDFS-12498](https://issues.apache.org/jira/browse/HDFS-12498) | Journal Syncer is not started in Federated + HA cluster |  Major | federation, journal-node | Bharat Viswanadham | Bharat Viswanadham |
| [YARN-7452](https://issues.apache.org/jira/browse/YARN-7452) | Decommissioning node default value to be zero in new YARN UI |  Trivial | yarn-ui-v2 | Vasudevan Skm | Vasudevan Skm |
| [YARN-7445](https://issues.apache.org/jira/browse/YARN-7445) | Render Applications and Services page with filters in new YARN UI |  Major | yarn-ui-v2 | Vasudevan Skm | Vasudevan Skm |
| [HADOOP-15031](https://issues.apache.org/jira/browse/HADOOP-15031) | Fix javadoc issues in Hadoop Common |  Minor | common | Mukul Kumar Singh | Mukul Kumar Singh |
| [HDFS-12705](https://issues.apache.org/jira/browse/HDFS-12705) | WebHdfsFileSystem exceptions should retain the caused by exception |  Major | hdfs | Daryn Sharp | Hanisha Koneru |
| [YARN-7462](https://issues.apache.org/jira/browse/YARN-7462) | Render outstanding resource requests on application page of new YARN UI |  Major | yarn-ui-v2 | Vasudevan Skm | Vasudevan Skm |
| [YARN-7464](https://issues.apache.org/jira/browse/YARN-7464) | Introduce filters in Nodes page of new YARN UI |  Major | yarn-ui-v2 | Vasudevan Skm | Vasudevan Skm |
| [YARN-7361](https://issues.apache.org/jira/browse/YARN-7361) | Improve the docker container runtime documentation |  Major | . | Shane Kumpf | Shane Kumpf |
| [YARN-7492](https://issues.apache.org/jira/browse/YARN-7492) | Set up SASS for new YARN UI styling |  Major | yarn-ui-v2 | Vasudevan Skm | Vasudevan Skm |
| [YARN-7469](https://issues.apache.org/jira/browse/YARN-7469) | Capacity Scheduler Intra-queue preemption: User can starve if newest app is exactly at user limit |  Major | capacity scheduler, yarn | Eric Payne | Eric Payne |
| [HADOOP-14982](https://issues.apache.org/jira/browse/HADOOP-14982) | Clients using FailoverOnNetworkExceptionRetry can go into a loop if they're used without authenticating with kerberos in HA env |  Major | common | Peter Bacsko | Peter Bacsko |
| [YARN-7489](https://issues.apache.org/jira/browse/YARN-7489) | ConcurrentModificationException in RMAppImpl#getRMAppMetrics |  Major | capacityscheduler | Tao Yang | Tao Yang |
| [YARN-7525](https://issues.apache.org/jira/browse/YARN-7525) | Incorrect query parameters in cluster nodes REST API document |  Minor | documentation | Tao Yang | Tao Yang |
| [HDFS-12813](https://issues.apache.org/jira/browse/HDFS-12813) | RequestHedgingProxyProvider can hide Exception thrown from the Namenode for proxy size of 1 |  Major | ha | Mukul Kumar Singh | Mukul Kumar Singh |
| [HADOOP-15046](https://issues.apache.org/jira/browse/HADOOP-15046) | Document Apache Hadoop does not support Java 9 in BUILDING.txt |  Major | documentation | Akira Ajisaka | Hanisha Koneru |
| [YARN-7513](https://issues.apache.org/jira/browse/YARN-7513) | Remove scheduler lock in FSAppAttempt.getWeight() |  Minor | fairscheduler | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [YARN-7390](https://issues.apache.org/jira/browse/YARN-7390) | All reservation related test cases failed when TestYarnClient runs against Fair Scheduler. |  Major | fairscheduler, reservation system | Yufei Gu | Yufei Gu |
| [YARN-7290](https://issues.apache.org/jira/browse/YARN-7290) | Method canContainerBePreempted can return true when it shouldn't |  Major | fairscheduler | Steven Rand | Steven Rand |
| [MAPREDUCE-7014](https://issues.apache.org/jira/browse/MAPREDUCE-7014) | Fix java doc errors in jdk1.8 |  Major | . | Rohith Sharma K S | Steve Loughran |
| [YARN-7363](https://issues.apache.org/jira/browse/YARN-7363) | ContainerLocalizer doesn't have a valid log4j config when using LinuxContainerExecutor |  Major | nodemanager | Yufei Gu | Yufei Gu |
| [HDFS-12754](https://issues.apache.org/jira/browse/HDFS-12754) | Lease renewal can hit a deadlock |  Major | . | Kuhu Shukla | Kuhu Shukla |
| [YARN-7499](https://issues.apache.org/jira/browse/YARN-7499) | Layout changes to Application details page in new YARN UI |  Major | yarn-ui-v2 | Vasudevan Skm | Vasudevan Skm |
| [HDFS-12857](https://issues.apache.org/jira/browse/HDFS-12857) | StoragePolicyAdmin should support schema based path |  Major | namenode | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-12832](https://issues.apache.org/jira/browse/HDFS-12832) | INode.getFullPathName may throw ArrayIndexOutOfBoundsException lead to NameNode exit |  Critical | namenode | DENG FEI | Konstantin Shvachko |
| [HADOOP-15054](https://issues.apache.org/jira/browse/HADOOP-15054) | upgrade hadoop dependency on commons-codec to 1.11 |  Major | . | PJ Fanning | Bharat Viswanadham |
| [HDFS-11754](https://issues.apache.org/jira/browse/HDFS-11754) | Make FsServerDefaults cache configurable. |  Minor | . | Rushabh S Shah | Mikhail Erofeev |
| [HADOOP-15042](https://issues.apache.org/jira/browse/HADOOP-15042) | Azure PageBlobInputStream.skip() can return negative value when numberOfPagesRemaining is 0 |  Minor | fs/azure | Rajesh Balamohan | Rajesh Balamohan |
| [YARN-7509](https://issues.apache.org/jira/browse/YARN-7509) | AsyncScheduleThread and ResourceCommitterService are still running after RM is transitioned to standby |  Critical | . | Tao Yang | Tao Yang |
| [HDFS-12681](https://issues.apache.org/jira/browse/HDFS-12681) | Make HdfsLocatedFileStatus a subtype of LocatedFileStatus |  Major | . | Chris Douglas | Chris Douglas |
| [YARN-7558](https://issues.apache.org/jira/browse/YARN-7558) | "yarn logs" command fails to get logs for running containers if UI authentication is enabled. |  Critical | . | Namit Maheshwari | Xuan Gong |
| [HDFS-12638](https://issues.apache.org/jira/browse/HDFS-12638) | Delete copy-on-truncate block along with the original block, when deleting a file being truncated |  Blocker | hdfs | Jiandan Yang | Konstantin Shvachko |
| [YARN-7546](https://issues.apache.org/jira/browse/YARN-7546) | Layout changes in Queue UI to show queue details on right pane |  Major | yarn-ui-v2 | Vasudevan Skm | Vasudevan Skm |
| [HDFS-12836](https://issues.apache.org/jira/browse/HDFS-12836) | startTxId could be greater than endTxId when tailing in-progress edit log |  Major | hdfs | Chao Sun | Chao Sun |
| [YARN-4813](https://issues.apache.org/jira/browse/YARN-4813) | TestRMWebServicesDelegationTokenAuthentication.testDoAs fails intermittently |  Major | resourcemanager | Daniel Templeton | Gergo Repas |
| [MAPREDUCE-5124](https://issues.apache.org/jira/browse/MAPREDUCE-5124) | AM lacks flow control for task events |  Major | mr-am | Jason Lowe | Peter Bacsko |
| [YARN-7589](https://issues.apache.org/jira/browse/YARN-7589) | TestPBImplRecords fails with NullPointerException |  Major | . | Jason Lowe | Daniel Templeton |
| [YARN-7455](https://issues.apache.org/jira/browse/YARN-7455) | quote\_and\_append\_arg can overflow buffer |  Major | nodemanager | Jason Lowe | Jim Brennan |
| [HADOOP-14600](https://issues.apache.org/jira/browse/HADOOP-14600) | LocatedFileStatus constructor forces RawLocalFS to exec a process to get the permissions |  Major | fs | Steve Loughran | Ping Liu |
| [YARN-7594](https://issues.apache.org/jira/browse/YARN-7594) | TestNMWebServices#testGetNMResourceInfo fails on trunk |  Major | nodemanager, webapp | Gergely Novák | Gergely Novák |
| [YARN-5594](https://issues.apache.org/jira/browse/YARN-5594) | Handle old RMDelegationToken format when recovering RM |  Major | resourcemanager | Tatyana But | Robert Kanter |
| [HADOOP-15058](https://issues.apache.org/jira/browse/HADOOP-15058) | create-release site build outputs dummy shaded jars due to skipShade |  Blocker | . | Andrew Wang | Andrew Wang |
| [HADOOP-14985](https://issues.apache.org/jira/browse/HADOOP-14985) | Remove subversion related code from VersionInfoMojo.java |  Minor | build | Akira Ajisaka | Ajay Kumar |
| [YARN-7586](https://issues.apache.org/jira/browse/YARN-7586) | Application Placement should be done before ACL checks in ResourceManager |  Blocker | . | Suma Shivaprasad | Suma Shivaprasad |
| [HDFS-11751](https://issues.apache.org/jira/browse/HDFS-11751) | DFSZKFailoverController daemon exits with wrong status code |  Major | auto-failover | Doris Gu | Bharat Viswanadham |
| [HADOOP-15080](https://issues.apache.org/jira/browse/HADOOP-15080) | Aliyun OSS: update oss sdk from 2.8.1 to 2.8.3 to remove its dependency on Cat-x "json-lib" |  Blocker | fs/oss | Chris Douglas | Sammi Chen |
| [HADOOP-15098](https://issues.apache.org/jira/browse/HADOOP-15098) | TestClusterTopology#testChooseRandom fails intermittently |  Major | test | Zsolt Venczel | Zsolt Venczel |
| [YARN-7608](https://issues.apache.org/jira/browse/YARN-7608) | Incorrect sTarget column causing DataTable warning on RM application and scheduler web page |  Major | resourcemanager, webapp | Weiwei Yang | Gergely Novák |
| [HDFS-12891](https://issues.apache.org/jira/browse/HDFS-12891) | Do not invalidate blocks if toInvalidate is empty |  Major | . | Zsolt Venczel | Zsolt Venczel |
| [YARN-7635](https://issues.apache.org/jira/browse/YARN-7635) | TestRMWebServicesSchedulerActivities fails in trunk |  Major | test | Sunil Govindan | Sunil Govindan |
| [HDFS-12833](https://issues.apache.org/jira/browse/HDFS-12833) | Distcp : Update the usage of delete option for dependency with update and overwrite option |  Minor | distcp, hdfs | Harshakiran Reddy | usharani |
| [YARN-7647](https://issues.apache.org/jira/browse/YARN-7647) | NM print inappropriate error log when node-labels is enabled |  Minor | . | Yang Wang | Yang Wang |
| [YARN-7536](https://issues.apache.org/jira/browse/YARN-7536) | em-table improvement for better filtering in new YARN UI |  Minor | yarn-ui-v2 | Vasudevan Skm | Vasudevan Skm |
| [HDFS-12907](https://issues.apache.org/jira/browse/HDFS-12907) | Allow read-only access to reserved raw for non-superusers |  Major | namenode | Daryn Sharp | Rushabh S Shah |
| [HDFS-12881](https://issues.apache.org/jira/browse/HDFS-12881) | Output streams closed with IOUtils suppressing write errors |  Major | . | Jason Lowe | Ajay Kumar |
| [YARN-7595](https://issues.apache.org/jira/browse/YARN-7595) | Container launching code suppresses close exceptions after writes |  Major | nodemanager | Jason Lowe | Jim Brennan |
| [HADOOP-15085](https://issues.apache.org/jira/browse/HADOOP-15085) | Output streams closed with IOUtils suppressing write errors |  Major | . | Jason Lowe | Jim Brennan |
| [YARN-7629](https://issues.apache.org/jira/browse/YARN-7629) | TestContainerLaunch# fails after YARN-7381 |  Major | . | Jason Lowe | Jason Lowe |
| [YARN-7664](https://issues.apache.org/jira/browse/YARN-7664) | Several javadoc errors |  Blocker | . | Sean Mackrory | Sean Mackrory |
| [HADOOP-15123](https://issues.apache.org/jira/browse/HADOOP-15123) | KDiag tries to load krb5.conf from KRB5CCNAME instead of KRB5\_CONFIG |  Minor | security | Vipin Rathor | Vipin Rathor |
| [HADOOP-15109](https://issues.apache.org/jira/browse/HADOOP-15109) | TestDFSIO -read -random doesn't work on file sized 4GB |  Minor | fs, test | zhoutai.zt | Ajay Kumar |
| [YARN-7661](https://issues.apache.org/jira/browse/YARN-7661) | NodeManager metrics return wrong value after update node resource |  Major | . | Yang Wang | Yang Wang |
| [HDFS-12930](https://issues.apache.org/jira/browse/HDFS-12930) | Remove the extra space in HdfsImageViewer.md |  Trivial | documentation | Yiqun Lin | Rahul Pathak |
| [YARN-7662](https://issues.apache.org/jira/browse/YARN-7662) | [Atsv2] Define new set of configurations for reader and collectors to bind. |  Major | . | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-7466](https://issues.apache.org/jira/browse/YARN-7466) | ResourceRequest has a different default for allocationRequestId than Container |  Major | . | Chandni Singh | Chandni Singh |
| [YARN-7674](https://issues.apache.org/jira/browse/YARN-7674) | Update Timeline Reader web app address in UI2 |  Major | . | Rohith Sharma K S | Sunil Govindan |
| [YARN-7577](https://issues.apache.org/jira/browse/YARN-7577) | Unit Fail: TestAMRestart#testPreemptedAMRestartOnRMRestart |  Major | . | Miklos Szegedi | Miklos Szegedi |
| [HDFS-12949](https://issues.apache.org/jira/browse/HDFS-12949) | Fix findbugs warning in ImageWriter.java |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [HDFS-12938](https://issues.apache.org/jira/browse/HDFS-12938) | TestErasureCodigCLI testAll failing consistently. |  Major | erasure-coding, hdfs | Rushabh S Shah | Ajay Kumar |
| [HDFS-12951](https://issues.apache.org/jira/browse/HDFS-12951) | Incorrect javadoc in SaslDataTransferServer.java#receive |  Major | encryption | Mukul Kumar Singh | Mukul Kumar Singh |
| [HDFS-12959](https://issues.apache.org/jira/browse/HDFS-12959) | Fix TestOpenFilesWithSnapshot redundant configurations |  Minor | hdfs | Manoj Govindassamy | Manoj Govindassamy |
| [YARN-7542](https://issues.apache.org/jira/browse/YARN-7542) | Fix issue that causes some Running Opportunistic Containers to be recovered as PAUSED |  Major | . | Arun Suresh | Sampada Dehankar |
| [HDFS-12915](https://issues.apache.org/jira/browse/HDFS-12915) | Fix findbugs warning in INodeFile$HeaderFormat.getBlockLayoutRedundancy |  Major | namenode | Wei-Chiu Chuang | Chris Douglas |
| [YARN-7555](https://issues.apache.org/jira/browse/YARN-7555) | Support multiple resource types in YARN native services |  Critical | yarn-native-services | Wangda Tan | Wangda Tan |
| [HADOOP-15122](https://issues.apache.org/jira/browse/HADOOP-15122) | Lock down version of doxia-module-markdown plugin |  Blocker | . | Elek, Marton | Elek, Marton |
| [HADOOP-15143](https://issues.apache.org/jira/browse/HADOOP-15143) | NPE due to Invalid KerberosTicket in UGI |  Major | . | Jitendra Nath Pandey | Mukul Kumar Singh |
| [HADOOP-15152](https://issues.apache.org/jira/browse/HADOOP-15152) | Typo in javadoc of ReconfigurableBase#reconfigurePropertyImpl |  Trivial | common | Nanda kumar | Nanda kumar |
| [HADOOP-15155](https://issues.apache.org/jira/browse/HADOOP-15155) | Error in javadoc of ReconfigurableBase#reconfigureProperty |  Minor | . | Ajay Kumar | Ajay Kumar |
| [YARN-7585](https://issues.apache.org/jira/browse/YARN-7585) | NodeManager should go unhealthy when state store throws DBException |  Major | nodemanager | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [YARN-6894](https://issues.apache.org/jira/browse/YARN-6894) | RM Apps API returns only active apps when query parameter queue used |  Minor | resourcemanager, restapi | Grant Sohn | Gergely Novák |
| [YARN-7692](https://issues.apache.org/jira/browse/YARN-7692) | Skip validating priority acls while recovering applications |  Blocker | resourcemanager | Charan Hebri | Sunil Govindan |
| [MAPREDUCE-7028](https://issues.apache.org/jira/browse/MAPREDUCE-7028) | Concurrent task progress updates causing NPE in Application Master |  Blocker | mr-am | Gergo Repas | Gergo Repas |
| [YARN-7602](https://issues.apache.org/jira/browse/YARN-7602) | NM should reference the singleton JvmMetrics instance |  Major | nodemanager | Haibo Chen | Haibo Chen |
| [HADOOP-15093](https://issues.apache.org/jira/browse/HADOOP-15093) | Deprecation of yarn.resourcemanager.zk-address is undocumented |  Major | documentation | Namit Maheshwari | Ajay Kumar |
| [HDFS-12931](https://issues.apache.org/jira/browse/HDFS-12931) | Handle InvalidEncryptionKeyException during DistributedFileSystem#getFileChecksum |  Major | encryption | Mukul Kumar Singh | Mukul Kumar Singh |
| [HDFS-12948](https://issues.apache.org/jira/browse/HDFS-12948) | DiskBalancer report command top option should only take positive numeric values |  Minor | diskbalancer | Namit Maheshwari | Shashikant Banerjee |
| [HDFS-12913](https://issues.apache.org/jira/browse/HDFS-12913) | TestDNFencingWithReplication.testFencingStress fix mini cluster not yet active issue |  Major | . | Zsolt Venczel | Zsolt Venczel |
| [HDFS-12987](https://issues.apache.org/jira/browse/HDFS-12987) | Document - Disabling the Lazy persist file scrubber. |  Trivial | documentation, hdfs | Karthik Palanisamy | Karthik Palanisamy |
| [HDFS-12860](https://issues.apache.org/jira/browse/HDFS-12860) | StripedBlockUtil#getRangesInternalBlocks throws exception for the block group size larger than 2GB |  Major | erasure-coding | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [YARN-7619](https://issues.apache.org/jira/browse/YARN-7619) | Max AM Resource value in Capacity Scheduler UI has to be refreshed for every user |  Major | capacity scheduler, yarn | Eric Payne | Eric Payne |
| [YARN-7645](https://issues.apache.org/jira/browse/YARN-7645) | TestContainerResourceUsage#testUsageAfterAMRestartWithMultipleContainers is flakey with FairScheduler |  Major | test | Robert Kanter | Robert Kanter |
| [YARN-7699](https://issues.apache.org/jira/browse/YARN-7699) | queueUsagePercentage is coming as INF for getApp REST api call |  Major | webapp | Sunil Govindan | Sunil Govindan |
| [HDFS-12985](https://issues.apache.org/jira/browse/HDFS-12985) | NameNode crashes during restart after an OpenForWrite file present in the Snapshot got deleted |  Major | hdfs | Manoj Govindassamy | Manoj Govindassamy |
| [YARN-4227](https://issues.apache.org/jira/browse/YARN-4227) | Ignore expired containers from removed nodes in FairScheduler |  Critical | fairscheduler | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [YARN-7718](https://issues.apache.org/jira/browse/YARN-7718) | DistributedShell failed to specify resource other than memory/vcores from container\_resources |  Critical | . | Wangda Tan | Wangda Tan |
| [YARN-7508](https://issues.apache.org/jira/browse/YARN-7508) | NPE in FiCaSchedulerApp when debug log enabled in async-scheduling mode |  Major | capacityscheduler | Tao Yang | Tao Yang |
| [YARN-7663](https://issues.apache.org/jira/browse/YARN-7663) | RMAppImpl:Invalid event: START at KILLED |  Major | resourcemanager | lujie | lujie |
| [YARN-6948](https://issues.apache.org/jira/browse/YARN-6948) | Invalid event: ATTEMPT\_ADDED at FINAL\_SAVING |  Major | yarn | lujie | lujie |
| [HDFS-12994](https://issues.apache.org/jira/browse/HDFS-12994) | TestReconstructStripedFile.testNNSendsErasureCodingTasks fails due to socket timeout |  Major | erasure-coding | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [YARN-7665](https://issues.apache.org/jira/browse/YARN-7665) | Allow FS scheduler state dump to be turned on/off separately from FS debug log |  Major | . | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [YARN-7689](https://issues.apache.org/jira/browse/YARN-7689) | TestRMContainerAllocator fails after YARN-6124 |  Major | scheduler | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [HADOOP-15163](https://issues.apache.org/jira/browse/HADOOP-15163) | Fix S3ACommitter documentation |  Minor | documentation, fs/s3 | Alessandro Andrioni | Alessandro Andrioni |
| [HADOOP-15060](https://issues.apache.org/jira/browse/HADOOP-15060) | TestShellBasedUnixGroupsMapping.testFiniteGroupResolutionTime flaky |  Major | . | Miklos Szegedi | Miklos Szegedi |
| [YARN-7735](https://issues.apache.org/jira/browse/YARN-7735) | Fix typo in YARN documentation |  Minor | documentation | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-7727](https://issues.apache.org/jira/browse/YARN-7727) | Incorrect log levels in few logs with QueuePriorityContainerCandidateSelector |  Minor | yarn | Prabhu Joseph | Prabhu Joseph |
| [HDFS-12984](https://issues.apache.org/jira/browse/HDFS-12984) | BlockPoolSlice can leak in a mini dfs cluster |  Major | . | Robert Joseph Evans | Ajay Kumar |
| [HDFS-11915](https://issues.apache.org/jira/browse/HDFS-11915) | Sync rbw dir on the first hsync() to avoid file lost on power failure |  Critical | . | Kanaka Kumar Avvaru | Vinayakumar B |
| [YARN-7731](https://issues.apache.org/jira/browse/YARN-7731) | RegistryDNS should handle upstream DNS returning CNAME |  Major | . | Billie Rinaldi | Eric Yang |
| [YARN-7671](https://issues.apache.org/jira/browse/YARN-7671) | Improve Diagonstic message for stop yarn native service |  Major | . | Yesha Vora | Chandni Singh |
| [YARN-7705](https://issues.apache.org/jira/browse/YARN-7705) | Create the container log directory with correct sticky bit in C code |  Major | nodemanager | Yufei Gu | Yufei Gu |
| [HDFS-13016](https://issues.apache.org/jira/browse/HDFS-13016) | globStatus javadoc refers to glob pattern as "regular expression" |  Trivial | documentation, hdfs | Ryanne Dolan | Mukul Kumar Singh |
| [HADOOP-15172](https://issues.apache.org/jira/browse/HADOOP-15172) | Fix the javadoc warning in WriteOperationHelper.java |  Minor | documentation, fs/s3 | Mukul Kumar Singh | Mukul Kumar Singh |
| [YARN-7479](https://issues.apache.org/jira/browse/YARN-7479) | TestContainerManagerSecurity.testContainerManager[Simple] flaky in trunk |  Major | test | Botong Huang | Akira Ajisaka |
| [HDFS-13004](https://issues.apache.org/jira/browse/HDFS-13004) | TestLeaseRecoveryStriped#testLeaseRecovery is failing when safeLength is 0MB or larger than the test file |  Major | hdfs | Zsolt Venczel | Zsolt Venczel |
| [HDFS-9049](https://issues.apache.org/jira/browse/HDFS-9049) | Make Datanode Netty reverse proxy port to be configurable |  Major | datanode | Vinayakumar B | Vinayakumar B |
| [YARN-7758](https://issues.apache.org/jira/browse/YARN-7758) | Add an additional check to the validity of container and application ids passed to container-executor |  Major | nodemanager | Miklos Szegedi | Yufei Gu |
| [YARN-7717](https://issues.apache.org/jira/browse/YARN-7717) | Add configuration consistency for module.enabled and docker.privileged-containers.enabled |  Major | . | Yesha Vora | Eric Badger |
| [HADOOP-15150](https://issues.apache.org/jira/browse/HADOOP-15150) | in FsShell, UGI params should be overidden through env vars(-D arg) |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-7750](https://issues.apache.org/jira/browse/YARN-7750) | [UI2] Render time related fields in all pages to the browser timezone |  Major | yarn-ui-v2 | Vasudevan Skm | Vasudevan Skm |
| [YARN-7740](https://issues.apache.org/jira/browse/YARN-7740) | Fix logging for destroy yarn service cli when app does not exist and some minor bugs |  Major | yarn-native-services | Yesha Vora | Jian He |
| [YARN-7139](https://issues.apache.org/jira/browse/YARN-7139) | FairScheduler: finished applications are always restored to default queue |  Major | fairscheduler | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [YARN-7753](https://issues.apache.org/jira/browse/YARN-7753) | [UI2] Meta information about Application logs has to be pulled from ATS 1.5 instead of ATS2 |  Major | yarn-ui-v2 | Sunil Govindan | Sunil Govindan |
| [HADOOP-14788](https://issues.apache.org/jira/browse/HADOOP-14788) | Credentials readTokenStorageFile to stop wrapping IOEs in IOEs |  Minor | security | Steve Loughran | Ajay Kumar |
| [HDFS-13039](https://issues.apache.org/jira/browse/HDFS-13039) | StripedBlockReader#createBlockReader leaks socket on IOException |  Critical | datanode, erasure-coding | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HADOOP-15181](https://issues.apache.org/jira/browse/HADOOP-15181) | Typo in SecureMode.md |  Trivial | documentation | Masahiro Tanaka | Masahiro Tanaka |
| [YARN-7738](https://issues.apache.org/jira/browse/YARN-7738) | CapacityScheduler: Support refresh maximum allocation for multiple resource types |  Blocker | . | Sumana Sathish | Wangda Tan |
| [YARN-7766](https://issues.apache.org/jira/browse/YARN-7766) | Introduce a new config property for YARN Service dependency tarball location |  Major | applications, client, yarn-native-services | Gour Saha | Gour Saha |
| [HDFS-12963](https://issues.apache.org/jira/browse/HDFS-12963) | Error log level in ShortCircuitRegistry#removeShm |  Minor | . | hu xiaodong | hu xiaodong |
| [YARN-7796](https://issues.apache.org/jira/browse/YARN-7796) | Container-executor fails with segfault on certain OS configurations |  Major | nodemanager | Gergo Repas | Gergo Repas |
| [YARN-7749](https://issues.apache.org/jira/browse/YARN-7749) | [UI2] GPU information tab in left hand side disappears when we click other tabs below |  Major | . | Sumana Sathish | Vasudevan Skm |
| [YARN-7806](https://issues.apache.org/jira/browse/YARN-7806) | Distributed Shell should use timeline async api's |  Major | distributed-shell | Sumana Sathish | Rohith Sharma K S |
| [HDFS-13023](https://issues.apache.org/jira/browse/HDFS-13023) | Journal Sync does not work on a secure cluster |  Major | journal-node | Namit Maheshwari | Bharat Viswanadham |
| [HADOOP-15121](https://issues.apache.org/jira/browse/HADOOP-15121) | Encounter NullPointerException when using DecayRpcScheduler |  Major | . | Tao Jie | Tao Jie |
| [MAPREDUCE-7015](https://issues.apache.org/jira/browse/MAPREDUCE-7015) | Possible race condition in JHS if the job is not loaded |  Major | jobhistoryserver | Peter Bacsko | Peter Bacsko |
| [YARN-7737](https://issues.apache.org/jira/browse/YARN-7737) | prelaunch.err file not found exception on container failure |  Major | . | Jonathan Hung | Keqiu Hu |
| [YARN-7777](https://issues.apache.org/jira/browse/YARN-7777) | Fix user name format in YARN Registry DNS name |  Major | . | Jian He | Jian He |
| [YARN-7628](https://issues.apache.org/jira/browse/YARN-7628) | [Documentation] Documenting the ability to disable elasticity at leaf queue |  Major | capacity scheduler | Zian Chen | Zian Chen |
| [HDFS-13063](https://issues.apache.org/jira/browse/HDFS-13063) | Fix the incorrect spelling in HDFSHighAvailabilityWithQJM.md |  Trivial | documentation | Jianfei Jiang | Jianfei Jiang |
| [YARN-7102](https://issues.apache.org/jira/browse/YARN-7102) | NM heartbeat stuck when responseId overflows MAX\_INT |  Critical | . | Botong Huang | Botong Huang |
| [MAPREDUCE-7041](https://issues.apache.org/jira/browse/MAPREDUCE-7041) | MR should not try to clean up at first job attempt |  Major | . | Takanobu Asanuma | Gergo Repas |
| [YARN-7742](https://issues.apache.org/jira/browse/YARN-7742) | [UI2] Duplicated containers are rendered per attempt |  Major | . | Rohith Sharma K S | Vasudevan Skm |
| [YARN-7760](https://issues.apache.org/jira/browse/YARN-7760) | [UI2] Clicking 'Master Node' or link next to 'AM Node Web UI' under application's appAttempt page goes to OLD RM UI |  Major | . | Sumana Sathish | Vasudevan Skm |
| [MAPREDUCE-7020](https://issues.apache.org/jira/browse/MAPREDUCE-7020) | Task timeout in uber mode can crash AM |  Major | mr-am | Akira Ajisaka | Peter Bacsko |
| [YARN-7765](https://issues.apache.org/jira/browse/YARN-7765) | [Atsv2] GSSException: No valid credentials provided - Failed to find any Kerberos tgt thrown by Timelinev2Client & HBaseClient in NM |  Blocker | . | Sumana Sathish | Rohith Sharma K S |
| [HDFS-13065](https://issues.apache.org/jira/browse/HDFS-13065) | TestErasureCodingMultipleRacks#testSkewedRack3 is failing |  Major | hdfs | Gabor Bota | Gabor Bota |
| [HDFS-12974](https://issues.apache.org/jira/browse/HDFS-12974) | Exception message is not printed when creating an encryption zone fails with AuthorizationException |  Minor | encryption | fang zhenyi | fang zhenyi |
| [YARN-7698](https://issues.apache.org/jira/browse/YARN-7698) | A misleading variable's name in ApplicationAttemptEventDispatcher |  Minor | resourcemanager | Jinjiang Ling | Jinjiang Ling |
| [YARN-7790](https://issues.apache.org/jira/browse/YARN-7790) | Improve Capacity Scheduler Async Scheduling to better handle node failures |  Critical | . | Sumana Sathish | Wangda Tan |
| [HDFS-12528](https://issues.apache.org/jira/browse/HDFS-12528) | Add an option to not disable short-circuit reads on failures |  Major | hdfs-client, performance | Andre Araujo | Xiao Chen |
| [YARN-7861](https://issues.apache.org/jira/browse/YARN-7861) | [UI2] Logs page shows duplicated containers with ATS |  Major | yarn-ui-v2 | Sunil Govindan | Sunil Govindan |
| [YARN-7828](https://issues.apache.org/jira/browse/YARN-7828) | Clicking on yarn service should take to component tab |  Major | yarn-ui-v2 | Yesha Vora | Sunil Govindan |
| [HDFS-13061](https://issues.apache.org/jira/browse/HDFS-13061) | SaslDataTransferClient#checkTrustAndSend should not trust a partially trusted channel |  Major | . | Xiaoyu Yao | Ajay Kumar |
| [HDFS-13060](https://issues.apache.org/jira/browse/HDFS-13060) | Adding a BlacklistBasedTrustedChannelResolver for TrustedChannelResolver |  Major | datanode, security | Xiaoyu Yao | Ajay Kumar |
| [HDFS-12897](https://issues.apache.org/jira/browse/HDFS-12897) | getErasureCodingPolicy should handle .snapshot dir better |  Major | erasure-coding, hdfs, snapshots | Harshakiran Reddy | LiXin Ge |
| [MAPREDUCE-7033](https://issues.apache.org/jira/browse/MAPREDUCE-7033) | Map outputs implicitly rely on permissive umask for shuffle |  Critical | mrv2 | Jason Lowe | Jason Lowe |
| [HDFS-13048](https://issues.apache.org/jira/browse/HDFS-13048) | LowRedundancyReplicatedBlocks metric can be negative |  Major | metrics | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-15198](https://issues.apache.org/jira/browse/HADOOP-15198) | Correct the spelling in CopyFilter.java |  Major | tools/distcp | Mukul Kumar Singh | Mukul Kumar Singh |
| [YARN-7831](https://issues.apache.org/jira/browse/YARN-7831) | YARN Service CLI should use hadoop.http.authentication.type to determine authentication method |  Major | . | Eric Yang | Eric Yang |
| [YARN-7879](https://issues.apache.org/jira/browse/YARN-7879) | NM user is unable to access the application filecache due to permissions |  Critical | . | Shane Kumpf | Jason Lowe |
| [HDFS-13100](https://issues.apache.org/jira/browse/HDFS-13100) | Handle IllegalArgumentException when GETSERVERDEFAULTS is not implemented in webhdfs. |  Critical | hdfs, webhdfs | Yongjun Zhang | Yongjun Zhang |
| [YARN-7876](https://issues.apache.org/jira/browse/YARN-7876) | Localized jars that are expanded after localization are not fully copied |  Blocker | . | Miklos Szegedi | Miklos Szegedi |
| [YARN-7849](https://issues.apache.org/jira/browse/YARN-7849) | TestMiniYarnClusterNodeUtilization#testUpdateNodeUtilization fails due to heartbeat sync error |  Major | test | Jason Lowe | Botong Huang |
| [YARN-7801](https://issues.apache.org/jira/browse/YARN-7801) | AmFilterInitializer should addFilter after fill all parameters |  Critical | . | Sumana Sathish | Wangda Tan |
| [YARN-7889](https://issues.apache.org/jira/browse/YARN-7889) | Missing kerberos token when check for RM REST API availability |  Major | yarn-native-services | Eric Yang | Eric Yang |
| [YARN-7850](https://issues.apache.org/jira/browse/YARN-7850) | [UI2] Log Aggregation status to be displayed in Application Page |  Major | yarn-ui-v2 | Yesha Vora | Gergely Novák |
| [YARN-7866](https://issues.apache.org/jira/browse/YARN-7866) | [UI2] Error to be displayed correctly while accessing kerberized cluster without kinit |  Major | yarn-ui-v2 | Sumana Sathish | Sunil Govindan |
| [YARN-7890](https://issues.apache.org/jira/browse/YARN-7890) | NPE during container relaunch |  Major | . | Billie Rinaldi | Jason Lowe |
| [HDFS-11701](https://issues.apache.org/jira/browse/HDFS-11701) | NPE from Unresolved Host causes permanent DFSInputStream failures |  Major | hdfs-client | James Moore | Lokesh Jain |
| [HDFS-13115](https://issues.apache.org/jira/browse/HDFS-13115) | In getNumUnderConstructionBlocks(), ignore the inodeIds for which the inodes have been deleted |  Major | . | Yongjun Zhang | Yongjun Zhang |
| [HDFS-12935](https://issues.apache.org/jira/browse/HDFS-12935) | Get ambiguous result for DFSAdmin command in HA mode when only one namenode is up |  Major | tools | Jianfei Jiang | Jianfei Jiang |
| [YARN-7827](https://issues.apache.org/jira/browse/YARN-7827) | Stop and Delete Yarn Service from RM UI fails with HTTP ERROR 404 |  Critical | yarn-ui-v2 | Yesha Vora | Sunil Govindan |
| [HDFS-13120](https://issues.apache.org/jira/browse/HDFS-13120) | Snapshot diff could be corrupted after concat |  Major | namenode, snapshots | Xiaoyu Yao | Xiaoyu Yao |
| [YARN-7909](https://issues.apache.org/jira/browse/YARN-7909) | YARN service REST API returns charset=null when kerberos enabled |  Major | yarn-native-services | Eric Yang | Eric Yang |
| [HDFS-13130](https://issues.apache.org/jira/browse/HDFS-13130) | Log object instance get incorrectly in SlowDiskTracker |  Minor | . | Jianfei Jiang | Jianfei Jiang |
| [YARN-7906](https://issues.apache.org/jira/browse/YARN-7906) | Fix mvn site fails with error: Multiple sources of package comments found for package "o.a.h.y.client.api.impl" |  Blocker | build, documentation | Akira Ajisaka | Akira Ajisaka |
| [YARN-5848](https://issues.apache.org/jira/browse/YARN-5848) | Remove unnecessary public/crossdomain.xml from YARN UIv2 sub project |  Blocker | yarn-ui-v2 | Allen Wittenauer | Sunil Govindan |
| [YARN-7697](https://issues.apache.org/jira/browse/YARN-7697) | NM goes down with OOM due to leak in log-aggregation |  Blocker | . | Santhosh B Gowda | Xuan Gong |
| [YARN-7739](https://issues.apache.org/jira/browse/YARN-7739) | DefaultAMSProcessor should properly check customized resource types against minimum/maximum allocation |  Blocker | . | Wangda Tan | Wangda Tan |
| [HDFS-10453](https://issues.apache.org/jira/browse/HDFS-10453) | ReplicationMonitor thread could stuck for long time due to the race between replication and delete of same file in a large cluster. |  Major | namenode | He Xiaoqiao | He Xiaoqiao |
| [HDFS-8693](https://issues.apache.org/jira/browse/HDFS-8693) | refreshNamenodes does not support adding a new standby to a running DN |  Critical | datanode, ha | Jian Fang | Ajith S |
| [MAPREDUCE-7052](https://issues.apache.org/jira/browse/MAPREDUCE-7052) | TestFixedLengthInputFormat#testFormatCompressedIn is flaky |  Major | client, test | Peter Bacsko | Peter Bacsko |
| [HDFS-13112](https://issues.apache.org/jira/browse/HDFS-13112) | Token expiration edits may cause log corruption or deadlock |  Critical | namenode | Daryn Sharp | Daryn Sharp |
| [HDFS-13151](https://issues.apache.org/jira/browse/HDFS-13151) | Fix the javadoc error in ReplicaInfo |  Minor | . | Bharat Viswanadham | Bharat Viswanadham |
| [MAPREDUCE-7053](https://issues.apache.org/jira/browse/MAPREDUCE-7053) | Timed out tasks can fail to produce thread dump |  Major | . | Jason Lowe | Jason Lowe |
| [HDFS-13058](https://issues.apache.org/jira/browse/HDFS-13058) | Fix dfs.namenode.shared.edits.dir in TestJournalNode |  Major | journal-node, test | Bharat Viswanadham | Bharat Viswanadham |
| [HADOOP-15206](https://issues.apache.org/jira/browse/HADOOP-15206) | BZip2 drops and duplicates records when input split size is small |  Major | . | Aki Tanaka | Aki Tanaka |
| [YARN-7937](https://issues.apache.org/jira/browse/YARN-7937) | Fix http method name in Cluster Application Timeout Update API example request |  Minor | docs, documentation | Charan Hebri | Charan Hebri |
| [HADOOP-15223](https://issues.apache.org/jira/browse/HADOOP-15223) | Replace Collections.EMPTY\* with empty\* when available |  Minor | . | Akira Ajisaka | fang zhenyi |
| [HDFS-13159](https://issues.apache.org/jira/browse/HDFS-13159) | TestTruncateQuotaUpdate fails in trunk |  Major | test | Arpit Agarwal | Nanda kumar |
| [YARN-7947](https://issues.apache.org/jira/browse/YARN-7947) | Capacity Scheduler intra-queue preemption can NPE for non-schedulable apps |  Major | capacity scheduler, scheduler preemption | Eric Payne | Eric Payne |
| [HADOOP-10571](https://issues.apache.org/jira/browse/HADOOP-10571) | Use Log.\*(Object, Throwable) overload to log exceptions |  Major | . | Arpit Agarwal | Andras Bokor |
| [HADOOP-6852](https://issues.apache.org/jira/browse/HADOOP-6852) | apparent bug in concatenated-bzip2 support (decoding) |  Major | io | Greg Roelofs | Zsolt Venczel |
| [YARN-7942](https://issues.apache.org/jira/browse/YARN-7942) | Yarn ServiceClient does not not delete znode from secure ZooKeeper |  Blocker | yarn-native-services | Eric Yang | Billie Rinaldi |
| [HADOOP-15236](https://issues.apache.org/jira/browse/HADOOP-15236) | Fix typo in RequestHedgingProxyProvider and RequestHedgingRMFailoverProxyProvider |  Trivial | documentation | Akira Ajisaka | Gabor Bota |
| [YARN-7675](https://issues.apache.org/jira/browse/YARN-7675) | [UI2] Support loading pre-2.8 version /scheduler REST response for queue page |  Major | yarn-ui-v2 | Gergely Novák | Gergely Novák |
| [YARN-7949](https://issues.apache.org/jira/browse/YARN-7949) | [UI2] ArtifactsId should not be a compulsory field for new service |  Major | yarn-ui-v2 | Yesha Vora | Yesha Vora |
| [YARN-5714](https://issues.apache.org/jira/browse/YARN-5714) | ContainerExecutor does not order environment map |  Trivial | nodemanager | Remi Catherinot | Remi Catherinot |
| [MAPREDUCE-7027](https://issues.apache.org/jira/browse/MAPREDUCE-7027) | HadoopArchiveLogs shouldn't delete the original logs if the HAR creation fails |  Critical | mrv2 | Gergely Novák | Gergely Novák |
| [HDFS-12865](https://issues.apache.org/jira/browse/HDFS-12865) | RequestHedgingProxyProvider should handle case when none of the proxies are available |  Major | ha | Mukul Kumar Singh | Mukul Kumar Singh |
| [HADOOP-15254](https://issues.apache.org/jira/browse/HADOOP-15254) | Correct the wrong word spelling 'intialize' |  Minor | . | fang zhenyi | fang zhenyi |
| [HDFS-12781](https://issues.apache.org/jira/browse/HDFS-12781) | After Datanode down, In Namenode UI Datanode tab is throwing warning message. |  Major | datanode | Harshakiran Reddy | Brahma Reddy Battula |
| [HDFS-12070](https://issues.apache.org/jira/browse/HDFS-12070) | Failed block recovery leaves files open indefinitely and at risk for data loss |  Major | . | Daryn Sharp | Kihwal Lee |
| [HADOOP-15265](https://issues.apache.org/jira/browse/HADOOP-15265) | Exclude json-smart explicitly in hadoop-auth avoid being pulled in transitively |  Major | . | Nishant Bangarwa | Nishant Bangarwa |
| [YARN-7963](https://issues.apache.org/jira/browse/YARN-7963) | TestServiceAM and TestServiceMonitor test cases are hanging |  Major | yarn-native-services | Eric Yang | Chandni Singh |
| [HDFS-13145](https://issues.apache.org/jira/browse/HDFS-13145) | SBN crash when transition to ANN with in-progress edit tailing enabled |  Major | ha, namenode | Chao Sun | Chao Sun |
| [HDFS-13181](https://issues.apache.org/jira/browse/HDFS-13181) | DiskBalancer: Add an configuration for valid plan hours |  Major | diskbalancer | Bharat Viswanadham | Bharat Viswanadham |
| [HDFS-13143](https://issues.apache.org/jira/browse/HDFS-13143) | SnapshotDiff - snapshotDiffReport might be inconsistent if the snapshotDiff calculation happens between a snapshot and the current tree |  Major | snapshots | Shashikant Banerjee | Shashikant Banerjee |
| [HDFS-13194](https://issues.apache.org/jira/browse/HDFS-13194) | CachePool permissions incorrectly checked |  Major | . | Yiqun Lin | Jianfei Jiang |
| [HDFS-13114](https://issues.apache.org/jira/browse/HDFS-13114) | CryptoAdmin#ReencryptZoneCommand should resolve Namespace info from path |  Major | encryption, hdfs | Hanisha Koneru | Hanisha Koneru |
| [HDFS-13081](https://issues.apache.org/jira/browse/HDFS-13081) | Datanode#checkSecureConfig should allow SASL and privileged HTTP |  Major | datanode, security | Xiaoyu Yao | Ajay Kumar |
| [YARN-7985](https://issues.apache.org/jira/browse/YARN-7985) | Service name is validated twice in ServiceClient when a service is created |  Trivial | yarn-native-services | Chandni Singh | Chandni Singh |
| [MAPREDUCE-7059](https://issues.apache.org/jira/browse/MAPREDUCE-7059) | Downward Compatibility issue: MR job fails because of unknown setErasureCodingPolicy method from 3.x client to HDFS 2.x cluster |  Critical | job submission | Jiandan Yang | Jiandan Yang |
| [YARN-7835](https://issues.apache.org/jira/browse/YARN-7835) | [Atsv2] Race condition in NM while publishing events if second attempt is launched on the same node |  Critical | . | Rohith Sharma K S | Rohith Sharma K S |
| [HADOOP-15275](https://issues.apache.org/jira/browse/HADOOP-15275) | Incorrect javadoc for return type of RetryPolicy#shouldRetry |  Minor | documentation | Nanda kumar | Nanda kumar |
| [YARN-7958](https://issues.apache.org/jira/browse/YARN-7958) | ServiceMaster should only wait for recovery of containers with id that match the current application id |  Critical | yarn | Chandni Singh | Chandni Singh |
| [HDFS-13211](https://issues.apache.org/jira/browse/HDFS-13211) | Fix a bug in DirectoryDiffList.getMinListForRange |  Major | snapshots | Shashikant Banerjee | Shashikant Banerjee |
| [HDFS-13210](https://issues.apache.org/jira/browse/HDFS-13210) | Fix the typo in MiniDFSCluster class |  Trivial | test | Yiqun Lin | fang zhenyi |
| [YARN-7511](https://issues.apache.org/jira/browse/YARN-7511) | NPE in ContainerLocalizer when localization failed for running container |  Major | nodemanager | Tao Yang | Tao Yang |
| [HADOOP-15261](https://issues.apache.org/jira/browse/HADOOP-15261) | Upgrade commons-io from 2.4 to 2.5 |  Major | minikdc | PandaMonkey | PandaMonkey |
| [MAPREDUCE-7023](https://issues.apache.org/jira/browse/MAPREDUCE-7023) | TestHadoopArchiveLogs.testCheckFilesAndSeedApps fails on rerun |  Minor | test | Gergely Novák | Gergely Novák |
| [HDFS-13178](https://issues.apache.org/jira/browse/HDFS-13178) | Disk Balancer: Add skipDateCheck option to DiskBalancer Execute command |  Major | diskbalancer | Bharat Viswanadham | Bharat Viswanadham |
| [HADOOP-15286](https://issues.apache.org/jira/browse/HADOOP-15286) | Remove unused imports from TestKMSWithZK.java |  Minor | test | Akira Ajisaka | Ajay Kumar |
| [YARN-7995](https://issues.apache.org/jira/browse/YARN-7995) | Remove unnecessary boxings and unboxings from PlacementConstraintParser.java |  Minor | . | Akira Ajisaka | Sen Zhao |
| [HDFS-13040](https://issues.apache.org/jira/browse/HDFS-13040) | Kerberized inotify client fails despite kinit properly |  Major | namenode | Wei-Chiu Chuang | Xiao Chen |
| [HADOOP-15288](https://issues.apache.org/jira/browse/HADOOP-15288) | TestSwiftFileSystemBlockLocation doesn't compile |  Critical | build, fs/swift | Steve Loughran | Steve Loughran |
| [YARN-7736](https://issues.apache.org/jira/browse/YARN-7736) | Fix itemization in YARN federation document |  Minor | documentation | Akira Ajisaka | Sen Zhao |
| [HDFS-13164](https://issues.apache.org/jira/browse/HDFS-13164) | File not closed if streamer fail with DSQuotaExceededException |  Major | hdfs-client | Xiao Chen | Xiao Chen |
| [HADOOP-15289](https://issues.apache.org/jira/browse/HADOOP-15289) | FileStatus.readFields() assertion incorrect |  Critical | . | Steve Loughran | Steve Loughran |
| [HDFS-13188](https://issues.apache.org/jira/browse/HDFS-13188) | Disk Balancer: Support multiple block pools during block move |  Major | diskbalancer | Bharat Viswanadham | Bharat Viswanadham |
| [HDFS-13109](https://issues.apache.org/jira/browse/HDFS-13109) | Support fully qualified hdfs path in EZ commands |  Major | hdfs | Hanisha Koneru | Hanisha Koneru |
| [HADOOP-15296](https://issues.apache.org/jira/browse/HADOOP-15296) | Fix a wrong link for RBF in the top page |  Minor | documentation | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-8011](https://issues.apache.org/jira/browse/YARN-8011) | TestOpportunisticContainerAllocatorAMService#testContainerPromoteAndDemoteBeforeContainerStart fails sometimes in trunk |  Minor | . | Tao Yang | Tao Yang |
| [HADOOP-15292](https://issues.apache.org/jira/browse/HADOOP-15292) | Distcp's use of pread is slowing it down. |  Minor | tools/distcp | Virajith Jalaparti | Virajith Jalaparti |
| [HADOOP-15273](https://issues.apache.org/jira/browse/HADOOP-15273) | distcp can't handle remote stores with different checksum algorithms |  Critical | tools/distcp | Steve Loughran | Steve Loughran |
| [HADOOP-15280](https://issues.apache.org/jira/browse/HADOOP-15280) | TestKMS.testWebHDFSProxyUserKerb and TestKMS.testWebHDFSProxyUserSimple fail in trunk |  Major | . | Ray Chiang | Bharat Viswanadham |
| [YARN-7944](https://issues.apache.org/jira/browse/YARN-7944) | [UI2] Remove master node link from headers of application pages |  Major | yarn-ui-v2 | Yesha Vora | Yesha Vora |
| [MAPREDUCE-6930](https://issues.apache.org/jira/browse/MAPREDUCE-6930) | mapreduce.map.cpu.vcores and mapreduce.reduce.cpu.vcores are both present twice in mapred-default.xml |  Major | mrv2 | Daniel Templeton | Sen Zhao |
| [YARN-8000](https://issues.apache.org/jira/browse/YARN-8000) | Yarn Service: component instance name shows up as component name in container record |  Major | . | Chandni Singh | Chandni Singh |
| [HDFS-13190](https://issues.apache.org/jira/browse/HDFS-13190) | Document WebHDFS support for snapshot diff |  Major | documentation, webhdfs | Xiaoyu Yao | Lokesh Jain |
| [HDFS-13244](https://issues.apache.org/jira/browse/HDFS-13244) | Add stack, conf, metrics links to utilities dropdown in NN webUI |  Major | . | Bharat Viswanadham | Bharat Viswanadham |
| [HDFS-10618](https://issues.apache.org/jira/browse/HDFS-10618) | TestPendingReconstruction#testPendingAndInvalidate is flaky due to race condition |  Major | . | Eric Badger | Eric Badger |
| [YARN-8024](https://issues.apache.org/jira/browse/YARN-8024) | LOG in class MaxRunningAppsEnforcer is initialized with a faulty class FairScheduler |  Major | fairscheduler | Yufei Gu | Sen Zhao |
| [HDFS-10803](https://issues.apache.org/jira/browse/HDFS-10803) | TestBalancerWithMultipleNameNodes#testBalancing2OutOf3Blockpools fails intermittently due to no free space available |  Major | . | Yiqun Lin | Yiqun Lin |
| [HDFS-12156](https://issues.apache.org/jira/browse/HDFS-12156) | TestFSImage fails without -Pnative |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [HDFS-13271](https://issues.apache.org/jira/browse/HDFS-13271) | WebHDFS: Add constructor in SnapshottableDirectoryStatus with HdfsFileStatus as argument |  Major | webhdfs | Lokesh Jain | Lokesh Jain |
| [HDFS-13239](https://issues.apache.org/jira/browse/HDFS-13239) | Fix non-empty dir warning message when setting default EC policy |  Minor | . | Hanisha Koneru | Bharat Viswanadham |
| [YARN-8022](https://issues.apache.org/jira/browse/YARN-8022) | ResourceManager UI cluster/app/\<app-id\> page fails to render |  Blocker | webapp | Tarun Parimi | Tarun Parimi |
| [HDFS-13249](https://issues.apache.org/jira/browse/HDFS-13249) | Document webhdfs support for getting snapshottable directory list |  Major | documentation, webhdfs | Lokesh Jain | Lokesh Jain |
| [MAPREDUCE-7064](https://issues.apache.org/jira/browse/MAPREDUCE-7064) | Flaky test TestTaskAttempt#testReducerCustomResourceTypes |  Major | client, test | Peter Bacsko | Peter Bacsko |
| [HDFS-13261](https://issues.apache.org/jira/browse/HDFS-13261) | Fix incorrect null value check |  Minor | hdfs | Jianfei Jiang | Jianfei Jiang |
| [HADOOP-15305](https://issues.apache.org/jira/browse/HADOOP-15305) | Replace FileUtils.writeStringToFile(File, String) with (File, String, Charset) to fix deprecation warnings |  Minor | . | Akira Ajisaka | fang zhenyi |
| [HDFS-12723](https://issues.apache.org/jira/browse/HDFS-12723) | TestReadStripedFileWithMissingBlocks#testReadFileWithMissingBlocks failing consistently. |  Major | . | Rushabh S Shah | Ajay Kumar |
| [HDFS-13251](https://issues.apache.org/jira/browse/HDFS-13251) | Avoid using hard coded datanode data dirs in unit tests |  Major | test | Xiaoyu Yao | Ajay Kumar |
| [HDFS-13280](https://issues.apache.org/jira/browse/HDFS-13280) | WebHDFS: Fix NPE in get snasphottable directory list call |  Major | webhdfs | Lokesh Jain | Lokesh Jain |
| [YARN-7952](https://issues.apache.org/jira/browse/YARN-7952) | RM should be able to recover log aggregation status after restart/fail-over |  Major | . | Xuan Gong | Xuan Gong |
| [HADOOP-15234](https://issues.apache.org/jira/browse/HADOOP-15234) | Throw meaningful message on null when initializing KMSWebApp |  Major | kms | Xiao Chen | fang zhenyi |
| [YARN-7636](https://issues.apache.org/jira/browse/YARN-7636) | Re-reservation count may overflow when cluster resource exhausted for a long time |  Major | capacityscheduler | Tao Yang | Tao Yang |
| [HDFS-12886](https://issues.apache.org/jira/browse/HDFS-12886) | Ignore minReplication for block recovery |  Major | hdfs, namenode | Lukas Majercak | Lukas Majercak |
| [YARN-8039](https://issues.apache.org/jira/browse/YARN-8039) | Clean up log dir configuration in TestLinuxContainerExecutorWithMocks.testStartLocalizer |  Minor | . | Miklos Szegedi | Miklos Szegedi |
| [HDFS-13296](https://issues.apache.org/jira/browse/HDFS-13296) | GenericTestUtils generates paths with drive letter in Windows and fail webhdfs related test cases |  Major | . | Xiao Liang | Xiao Liang |
| [HDFS-13268](https://issues.apache.org/jira/browse/HDFS-13268) | TestWebHdfsFileContextMainOperations fails on Windows |  Major | . | Íñigo Goiri | Xiao Liang |
| [YARN-8054](https://issues.apache.org/jira/browse/YARN-8054) | Improve robustness of the LocalDirsHandlerService MonitoringTimerTask thread |  Major | . | Jonathan Eagles | Jonathan Eagles |
| [YARN-7873](https://issues.apache.org/jira/browse/YARN-7873) | Revert YARN-6078 |  Blocker | . | Billie Rinaldi | Billie Rinaldi |
| [HDFS-13195](https://issues.apache.org/jira/browse/HDFS-13195) | DataNode conf page  cannot display the current value after reconfig |  Minor | datanode | maobaolong | maobaolong |
| [HADOOP-14067](https://issues.apache.org/jira/browse/HADOOP-14067) | VersionInfo should load version-info.properties from its own classloader |  Major | common | Thejas M Nair | Thejas M Nair |
| [YARN-8063](https://issues.apache.org/jira/browse/YARN-8063) | DistributedShellTimelinePlugin wrongly check for entityId instead of entityType |  Major | . | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-8062](https://issues.apache.org/jira/browse/YARN-8062) | yarn rmadmin -getGroups returns group from which the user has been removed |  Critical | . | Sumana Sathish | Sunil Govindan |
| [YARN-8068](https://issues.apache.org/jira/browse/YARN-8068) | Application Priority field causes NPE in app timeline publish when Hadoop 2.7 based clients to 2.8+ |  Blocker | yarn | Sunil Govindan | Sunil Govindan |
| [YARN-8075](https://issues.apache.org/jira/browse/YARN-8075) | DShell does not fail when we ask more GPUs than available even though AM throws 'InvalidResourceRequestException' |  Major | . | Sumana Sathish | Wangda Tan |
| [HADOOP-15320](https://issues.apache.org/jira/browse/HADOOP-15320) | Remove customized getFileBlockLocations for hadoop-azure and hadoop-azure-datalake |  Major | fs/adl, fs/azure | shanyu zhao | shanyu zhao |
| [YARN-8085](https://issues.apache.org/jira/browse/YARN-8085) | ResourceProfilesManager should be set in RMActiveServiceContext |  Blocker | capacityscheduler | Tao Yang | Tao Yang |
| [YARN-8086](https://issues.apache.org/jira/browse/YARN-8086) | ManagedParentQueue with no leaf queues cause JS error in new UI |  Blocker | . | Suma Shivaprasad | Suma Shivaprasad |
| [YARN-6629](https://issues.apache.org/jira/browse/YARN-6629) | NPE occurred when container allocation proposal is applied but its resource requests are removed before |  Critical | . | Tao Yang | Tao Yang |
| [MAPREDUCE-7036](https://issues.apache.org/jira/browse/MAPREDUCE-7036) | ASF License warning in hadoop-mapreduce-client |  Minor | test | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-7527](https://issues.apache.org/jira/browse/YARN-7527) | Over-allocate node resource in async-scheduling mode of CapacityScheduler |  Major | capacityscheduler | Tao Yang | Tao Yang |
| [YARN-7598](https://issues.apache.org/jira/browse/YARN-7598) | Document how to use classpath isolation for aux-services in YARN |  Major | . | Xuan Gong | Xuan Gong |
| [HDFS-13136](https://issues.apache.org/jira/browse/HDFS-13136) | Avoid taking FSN lock while doing group member lookup for FSD permission check |  Major | namenode | Xiaoyu Yao | Xiaoyu Yao |
| [HADOOP-15450](https://issues.apache.org/jira/browse/HADOOP-15450) | Avoid fsync storm triggered by DiskChecker and handle disk full situation |  Blocker | . | Kihwal Lee | Arpit Agarwal |
| [YARN-8346](https://issues.apache.org/jira/browse/YARN-8346) | Upgrading to 3.1 kills running containers with error "Opportunistic container queue is full" |  Blocker | . | Rohith Sharma K S | Jason Lowe |
| [HADOOP-9747](https://issues.apache.org/jira/browse/HADOOP-9747) | Reduce unnecessary UGI synchronization |  Critical | security | Daryn Sharp | Daryn Sharp |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-6953](https://issues.apache.org/jira/browse/MAPREDUCE-6953) | Skip the testcase testJobWithChangePriority if FairScheduler is used |  Major | client | Peter Bacsko | Peter Bacsko |
| [HDFS-12730](https://issues.apache.org/jira/browse/HDFS-12730) | Verify open files captured in the snapshots across config disable and enable |  Major | hdfs | Manoj Govindassamy | Manoj Govindassamy |
| [HADOOP-15117](https://issues.apache.org/jira/browse/HADOOP-15117) | open(PathHandle) contract test should be exhaustive for default options |  Major | . | Chris Douglas | Chris Douglas |
| [HDFS-13106](https://issues.apache.org/jira/browse/HDFS-13106) | Need to exercise all HDFS APIs for EC |  Major | hdfs | Haibo Yan | Haibo Yan |
| [HDFS-13107](https://issues.apache.org/jira/browse/HDFS-13107) | Add Mover Cli Unit Tests for Federated cluster |  Major | balancer & mover, test | Bharat Viswanadham | Bharat Viswanadham |
| [HDFS-13550](https://issues.apache.org/jira/browse/HDFS-13550) | TestDebugAdmin#testComputeMetaCommand fails on Windows |  Minor | . | Anbang Hu | Anbang Hu |


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
| [YARN-5707](https://issues.apache.org/jira/browse/YARN-5707) | Add manager class for resource profiles |  Major | resourcemanager | Varun Vasudev | Varun Vasudev |
| [YARN-5708](https://issues.apache.org/jira/browse/YARN-5708) | Implement APIs to get resource profiles from the RM |  Major | client | Varun Vasudev | Varun Vasudev |
| [YARN-5587](https://issues.apache.org/jira/browse/YARN-5587) | Add support for resource profiles |  Major | nodemanager, resourcemanager | Varun Vasudev | Varun Vasudev |
| [YARN-5951](https://issues.apache.org/jira/browse/YARN-5951) | Changes to allow CapacityScheduler to use configuration store |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-5946](https://issues.apache.org/jira/browse/YARN-5946) | Create YarnConfigurationStore interface and InMemoryConfigurationStore class |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-5588](https://issues.apache.org/jira/browse/YARN-5588) | Add support for resource profiles in distributed shell |  Major | nodemanager, resourcemanager | Varun Vasudev | Varun Vasudev |
| [YARN-6232](https://issues.apache.org/jira/browse/YARN-6232) | Update resource usage and preempted resource calculations to take into account all resource types |  Major | resourcemanager | Varun Vasudev | Varun Vasudev |
| [YARN-5948](https://issues.apache.org/jira/browse/YARN-5948) | Implement MutableConfigurationManager for handling storage into configuration store |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-5952](https://issues.apache.org/jira/browse/YARN-5952) | Create REST API for changing YARN scheduler configurations |  Major | . | Jonathan Hung | Jonathan Hung |
| [HDFS-10706](https://issues.apache.org/jira/browse/HDFS-10706) | [READ] Add tool generating FSImage from external store |  Major | namenode, tools | Chris Douglas | Chris Douglas |
| [YARN-6445](https://issues.apache.org/jira/browse/YARN-6445) | [YARN-3926] Performance improvements in resource profile branch with respect to SLS |  Major | nodemanager, resourcemanager | Varun Vasudev | Varun Vasudev |
| [HDFS-11653](https://issues.apache.org/jira/browse/HDFS-11653) | [READ] ProvidedReplica should return an InputStream that is bounded by its length |  Major | . | Virajith Jalaparti | Virajith Jalaparti |
| [HDFS-11663](https://issues.apache.org/jira/browse/HDFS-11663) | [READ] Fix NullPointerException in ProvidedBlocksBuilder |  Major | . | Virajith Jalaparti | Virajith Jalaparti |
| [HDFS-11703](https://issues.apache.org/jira/browse/HDFS-11703) | [READ] Tests for ProvidedStorageMap |  Major | . | Virajith Jalaparti | Virajith Jalaparti |
| [YARN-5949](https://issues.apache.org/jira/browse/YARN-5949) | Add pluggable configuration ACL policy interface and implementation |  Major | . | Jonathan Hung | Jonathan Hung |
| [HDFS-11791](https://issues.apache.org/jira/browse/HDFS-11791) | [READ] Test for increasing replication of provided files. |  Major | . | Virajith Jalaparti | Virajith Jalaparti |
| [HDFS-11792](https://issues.apache.org/jira/browse/HDFS-11792) | [READ] Test cases for ProvidedVolumeDF and ProviderBlockIteratorImpl |  Major | . | Virajith Jalaparti | Virajith Jalaparti |
| [HDFS-11673](https://issues.apache.org/jira/browse/HDFS-11673) | [READ] Handle failures of Datanode with PROVIDED storage |  Major | . | Virajith Jalaparti | Virajith Jalaparti |
| [YARN-6575](https://issues.apache.org/jira/browse/YARN-6575) | Support global configuration mutation in MutableConfProvider |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-5953](https://issues.apache.org/jira/browse/YARN-5953) | Create CLI for changing YARN configurations |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-6761](https://issues.apache.org/jira/browse/YARN-6761) | Fix build for YARN-3926 branch |  Major | nodemanager, resourcemanager | Varun Vasudev | Varun Vasudev |
| [YARN-6786](https://issues.apache.org/jira/browse/YARN-6786) | ResourcePBImpl imports cleanup |  Trivial | resourcemanager | Daniel Templeton | Yeliang Cang |
| [YARN-5947](https://issues.apache.org/jira/browse/YARN-5947) | Create LeveldbConfigurationStore class using Leveldb as backing store |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-6322](https://issues.apache.org/jira/browse/YARN-6322) | Disable queue refresh when configuration mutation is enabled |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-6593](https://issues.apache.org/jira/browse/YARN-6593) | [API] Introduce Placement Constraint object |  Major | . | Konstantinos Karanasos | Konstantinos Karanasos |
| [YARN-6788](https://issues.apache.org/jira/browse/YARN-6788) | Improve performance of resource profile branch |  Blocker | nodemanager, resourcemanager | Sunil Govindan | Sunil Govindan |
| [HDFS-12091](https://issues.apache.org/jira/browse/HDFS-12091) | [READ] Check that the replicas served from a {{ProvidedVolumeImpl}} belong to the correct external storage |  Major | . | Virajith Jalaparti | Virajith Jalaparti |
| [HDFS-12093](https://issues.apache.org/jira/browse/HDFS-12093) | [READ] Share remoteFS between ProvidedReplica instances. |  Major | . | Ewan Higgs | Virajith Jalaparti |
| [YARN-6471](https://issues.apache.org/jira/browse/YARN-6471) | Support to add min/max resource configuration for a queue |  Major | capacity scheduler | Sunil Govindan | Sunil Govindan |
| [YARN-6935](https://issues.apache.org/jira/browse/YARN-6935) | ResourceProfilesManagerImpl.parseResource() has no need of the key parameter |  Major | resourcemanager | Daniel Templeton | Manikandan R |
| [HDFS-12289](https://issues.apache.org/jira/browse/HDFS-12289) | [READ] HDFS-12091 breaks the tests for provided block reads |  Major | . | Virajith Jalaparti | Virajith Jalaparti |
| [YARN-6994](https://issues.apache.org/jira/browse/YARN-6994) | Remove last uses of Long from resource types code |  Minor | resourcemanager | Daniel Templeton | Daniel Templeton |
| [YARN-6892](https://issues.apache.org/jira/browse/YARN-6892) | Improve API implementation in Resources and DominantResourceCalculator class |  Major | nodemanager, resourcemanager | Sunil Govindan | Sunil Govindan |
| [YARN-6908](https://issues.apache.org/jira/browse/YARN-6908) | ResourceProfilesManagerImpl is missing @Overrides on methods |  Minor | resourcemanager | Daniel Templeton | Sunil Govindan |
| [YARN-6610](https://issues.apache.org/jira/browse/YARN-6610) | DominantResourceCalculator#getResourceAsValue dominant param is updated to handle multiple resources |  Critical | resourcemanager | Daniel Templeton | Daniel Templeton |
| [YARN-7030](https://issues.apache.org/jira/browse/YARN-7030) | Performance optimizations in Resource and ResourceUtils class |  Critical | nodemanager, resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-7042](https://issues.apache.org/jira/browse/YARN-7042) | Clean up unit tests after YARN-6610 |  Major | test | Daniel Templeton | Daniel Templeton |
| [YARN-6789](https://issues.apache.org/jira/browse/YARN-6789) | Add Client API to get all supported resource types from RM |  Major | nodemanager, resourcemanager | Sunil Govindan | Sunil Govindan |
| [YARN-6781](https://issues.apache.org/jira/browse/YARN-6781) | ResourceUtils#initializeResourcesMap takes an unnecessary Map parameter |  Minor | resourcemanager | Daniel Templeton | Yu-Tang Lin |
| [YARN-7043](https://issues.apache.org/jira/browse/YARN-7043) | Cleanup ResourceProfileManager |  Critical | . | Wangda Tan | Wangda Tan |
| [YARN-7067](https://issues.apache.org/jira/browse/YARN-7067) | Optimize ResourceType information display in UI |  Critical | nodemanager, resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-7039](https://issues.apache.org/jira/browse/YARN-7039) | Fix javac and javadoc errors in YARN-3926 branch |  Major | nodemanager, resourcemanager | Sunil Govindan | Sunil Govindan |
| [YARN-7024](https://issues.apache.org/jira/browse/YARN-7024) | Fix issues on recovery in LevelDB store |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-7093](https://issues.apache.org/jira/browse/YARN-7093) | Improve log message in ResourceUtils |  Trivial | nodemanager, resourcemanager | Sunil Govindan | Sunil Govindan |
| [YARN-7075](https://issues.apache.org/jira/browse/YARN-7075) | Better styling for donut charts in new YARN UI |  Major | . | Da Ding | Da Ding |
| [HADOOP-14103](https://issues.apache.org/jira/browse/HADOOP-14103) | Sort out hadoop-aws contract-test-options.xml |  Minor | fs/s3, test | Steve Loughran | John Zhuge |
| [YARN-6933](https://issues.apache.org/jira/browse/YARN-6933) | ResourceUtils.DISALLOWED\_NAMES check is duplicated |  Major | resourcemanager | Daniel Templeton | Manikandan R |
| [YARN-5328](https://issues.apache.org/jira/browse/YARN-5328) | Plan/ResourceAllocation data structure enhancements required to support recurring reservations in ReservationSystem |  Major | resourcemanager | Subru Krishnan | Subru Krishnan |
| [YARN-7056](https://issues.apache.org/jira/browse/YARN-7056) | Document Resource Profiles feature |  Major | nodemanager, resourcemanager | Sunil Govindan | Sunil Govindan |
| [YARN-7144](https://issues.apache.org/jira/browse/YARN-7144) | Log Aggregation controller should not swallow the exceptions when it calls closeWriter and closeReader. |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-7104](https://issues.apache.org/jira/browse/YARN-7104) | Improve Nodes Heatmap in new YARN UI with better color coding |  Major | . | Da Ding | Da Ding |
| [YARN-6600](https://issues.apache.org/jira/browse/YARN-6600) | Introduce default and max lifetime of application at LeafQueue level |  Major | capacity scheduler | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-5330](https://issues.apache.org/jira/browse/YARN-5330) | SharingPolicy enhancements required to support recurring reservations in ReservationSystem |  Major | resourcemanager | Subru Krishnan | Carlo Curino |
| [YARN-7072](https://issues.apache.org/jira/browse/YARN-7072) | Add a new log aggregation file format controller |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-7136](https://issues.apache.org/jira/browse/YARN-7136) | Additional Performance Improvement for Resource Profile Feature |  Critical | nodemanager, resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-7137](https://issues.apache.org/jira/browse/YARN-7137) | Move newly added APIs to unstable in YARN-3926 branch |  Blocker | nodemanager, resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-7194](https://issues.apache.org/jira/browse/YARN-7194) | Log aggregation status is always Failed with the newly added log aggregation IndexedFileFormat |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-6612](https://issues.apache.org/jira/browse/YARN-6612) | Update fair scheduler policies to be aware of resource types |  Major | fairscheduler | Daniel Templeton | Daniel Templeton |
| [YARN-7174](https://issues.apache.org/jira/browse/YARN-7174) | Add retry logic in LogsCLI when fetch running application logs |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-6840](https://issues.apache.org/jira/browse/YARN-6840) | Implement zookeeper based store for scheduler configuration updates |  Major | . | Wangda Tan | Jonathan Hung |
| [HDFS-12473](https://issues.apache.org/jira/browse/HDFS-12473) | Change hosts JSON file format |  Major | . | Ming Ma | Ming Ma |
| [HDFS-11035](https://issues.apache.org/jira/browse/HDFS-11035) | Better documentation for maintenace mode and upgrade domain |  Major | datanode, documentation | Wei-Chiu Chuang | Ming Ma |
| [YARN-7046](https://issues.apache.org/jira/browse/YARN-7046) | Add closing logic to configuration store |  Major | . | Jonathan Hung | Jonathan Hung |
| [MAPREDUCE-6947](https://issues.apache.org/jira/browse/MAPREDUCE-6947) |  Moving logging APIs over to slf4j in hadoop-mapreduce-examples |  Major | . | Gergely Novák | Gergely Novák |
| [HADOOP-14894](https://issues.apache.org/jira/browse/HADOOP-14894) | ReflectionUtils should use Time.monotonicNow to mesaure duration |  Minor | . | Bharat Viswanadham | Bharat Viswanadham |
| [HADOOP-14892](https://issues.apache.org/jira/browse/HADOOP-14892) | MetricsSystemImpl should use Time.monotonicNow for measuring durations |  Minor | . | Chetna Chaudhari | Chetna Chaudhari |
| [YARN-7238](https://issues.apache.org/jira/browse/YARN-7238) | Documentation for API based scheduler configuration management |  Major | . | Jonathan Hung | Jonathan Hung |
| [HADOOP-14893](https://issues.apache.org/jira/browse/HADOOP-14893) | WritableRpcEngine should use Time.monotonicNow |  Minor | . | Chetna Chaudhari | Chetna Chaudhari |
| [HDFS-12386](https://issues.apache.org/jira/browse/HDFS-12386) | Add fsserver defaults call to WebhdfsFileSystem. |  Minor | webhdfs | Rushabh S Shah | Rushabh S Shah |
| [YARN-7252](https://issues.apache.org/jira/browse/YARN-7252) | Removing queue then failing over results in exception |  Critical | . | Jonathan Hung | Jonathan Hung |
| [YARN-7251](https://issues.apache.org/jira/browse/YARN-7251) | Misc changes to YARN-5734 |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-6962](https://issues.apache.org/jira/browse/YARN-6962) | Add support for updateContainers when allocating using FederationInterceptor |  Minor | . | Botong Huang | Botong Huang |
| [YARN-7259](https://issues.apache.org/jira/browse/YARN-7259) | Add size-based rolling policy to LogAggregationIndexedFileController |  Major | . | Xuan Gong | Xuan Gong |
| [MAPREDUCE-6971](https://issues.apache.org/jira/browse/MAPREDUCE-6971) | Moving logging APIs over to slf4j in hadoop-mapreduce-client-app |  Major | . | Jinjiang Ling | Jinjiang Ling |
| [YARN-6916](https://issues.apache.org/jira/browse/YARN-6916) | Moving logging APIs over to slf4j in hadoop-yarn-server-common |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [HDFS-12584](https://issues.apache.org/jira/browse/HDFS-12584) | [READ] Fix errors in image generation tool from latest rebase |  Major | . | Virajith Jalaparti | Virajith Jalaparti |
| [YARN-6975](https://issues.apache.org/jira/browse/YARN-6975) | Moving logging APIs over to slf4j in hadoop-yarn-server-tests, hadoop-yarn-server-web-proxy and hadoop-yarn-server-router |  Major | . | Yeliang Cang | Yeliang Cang |
| [YARN-2037](https://issues.apache.org/jira/browse/YARN-2037) | Add work preserving restart support for Unmanaged AMs |  Major | resourcemanager | Karthik Kambatla | Botong Huang |
| [YARN-5329](https://issues.apache.org/jira/browse/YARN-5329) | Placement Agent enhancements required to support recurring reservations in ReservationSystem |  Blocker | resourcemanager | Subru Krishnan | Carlo Curino |
| [YARN-6182](https://issues.apache.org/jira/browse/YARN-6182) | Fix alignment issues and missing information in new YARN UI's Queue page |  Major | yarn-ui-v2 | Akhil PB | Akhil PB |
| [HADOOP-14845](https://issues.apache.org/jira/browse/HADOOP-14845) | Azure wasb: getFileStatus not making any auth checks |  Major | fs/azure, security | Sivaguru Sankaridurg | Sivaguru Sankaridurg |
| [HADOOP-14899](https://issues.apache.org/jira/browse/HADOOP-14899) | Restrict Access to setPermission operation when authorization is enabled in WASB |  Major | fs/azure | Kannapiran Srinivasan | Kannapiran Srinivasan |
| [YARN-7237](https://issues.apache.org/jira/browse/YARN-7237) | Cleanup usages of ResourceProfiles |  Critical | nodemanager, resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-7296](https://issues.apache.org/jira/browse/YARN-7296) | convertToProtoFormat(Resource r) is not setting for all resource types |  Major | . | lovekesh bansal | lovekesh bansal |
| [HADOOP-14913](https://issues.apache.org/jira/browse/HADOOP-14913) | Sticky bit implementation for rename() operation in Azure WASB |  Major | fs, fs/azure | Varada Hemeswari | Varada Hemeswari |
| [YARN-6620](https://issues.apache.org/jira/browse/YARN-6620) | Add support in NodeManager to isolate GPU devices by using CGroups |  Major | . | Wangda Tan | Wangda Tan |
| [YARN-7205](https://issues.apache.org/jira/browse/YARN-7205) | Log improvements for the ResourceUtils |  Major | nodemanager, resourcemanager | Jian He | Sunil Govindan |
| [YARN-7180](https://issues.apache.org/jira/browse/YARN-7180) | Remove class ResourceType |  Major | resourcemanager, scheduler | Yufei Gu | Sunil Govindan |
| [HADOOP-14935](https://issues.apache.org/jira/browse/HADOOP-14935) | Azure: POSIX permissions are taking effect in access() method even when authorization is enabled |  Major | fs/azure | Santhosh G Nayak | Santhosh G Nayak |
| [YARN-7254](https://issues.apache.org/jira/browse/YARN-7254) | UI and metrics changes related to absolute resource configuration |  Major | capacity scheduler | Sunil Govindan | Sunil Govindan |
| [YARN-7311](https://issues.apache.org/jira/browse/YARN-7311) | Fix TestRMWebServicesReservation parametrization for fair scheduler |  Blocker | fairscheduler, reservation system | Yufei Gu | Yufei Gu |
| [HDFS-12605](https://issues.apache.org/jira/browse/HDFS-12605) | [READ] TestNameNodeProvidedImplementation#testProvidedDatanodeFailures fails after rebase |  Major | . | Virajith Jalaparti | Virajith Jalaparti |
| [YARN-7345](https://issues.apache.org/jira/browse/YARN-7345) | GPU Isolation: Incorrect minor device numbers written to devices.deny file |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-7338](https://issues.apache.org/jira/browse/YARN-7338) | Support same origin policy for cross site scripting prevention. |  Major | yarn-ui-v2 | Vrushali C | Sunil Govindan |
| [YARN-4090](https://issues.apache.org/jira/browse/YARN-4090) | Make Collections.sort() more efficient by caching resource usage |  Major | fairscheduler | Xianyin Xin | Yufei Gu |
| [YARN-6984](https://issues.apache.org/jira/browse/YARN-6984) | DominantResourceCalculator.isAnyMajorResourceZero() should test all resources |  Major | scheduler | Daniel Templeton | Sunil Govindan |
| [YARN-4827](https://issues.apache.org/jira/browse/YARN-4827) | Document configuration of ReservationSystem for FairScheduler |  Blocker | capacity scheduler | Subru Krishnan | Yufei Gu |
| [YARN-5516](https://issues.apache.org/jira/browse/YARN-5516) | Add REST API for supporting recurring reservations |  Major | resourcemanager | Sangeetha Abdu Jyothi | Sean Po |
| [MAPREDUCE-6977](https://issues.apache.org/jira/browse/MAPREDUCE-6977) | Moving logging APIs over to slf4j in hadoop-mapreduce-client-common |  Major | client | Jinjiang Ling | Jinjiang Ling |
| [YARN-6505](https://issues.apache.org/jira/browse/YARN-6505) | Define the strings used in SLS JSON input file format |  Major | scheduler-load-simulator | Yufei Gu | Gergely Novák |
| [YARN-7332](https://issues.apache.org/jira/browse/YARN-7332) | Compute effectiveCapacity per each resource vector |  Major | capacity scheduler | Sunil Govindan | Sunil Govindan |
| [YARN-7224](https://issues.apache.org/jira/browse/YARN-7224) | Support GPU isolation for docker container |  Major | . | Wangda Tan | Wangda Tan |
| [YARN-7374](https://issues.apache.org/jira/browse/YARN-7374) | Improve performance of DRF comparisons for resource types in fair scheduler |  Critical | fairscheduler | Daniel Templeton | Daniel Templeton |
| [YARN-6927](https://issues.apache.org/jira/browse/YARN-6927) | Add support for individual resource types requests in MapReduce |  Major | resourcemanager | Daniel Templeton | Gergo Repas |
| [YARN-6594](https://issues.apache.org/jira/browse/YARN-6594) | [API] Introduce SchedulingRequest object |  Major | . | Konstantinos Karanasos | Konstantinos Karanasos |
| [HADOOP-14997](https://issues.apache.org/jira/browse/HADOOP-14997) |  Add hadoop-aliyun as dependency of hadoop-cloud-storage |  Minor | fs/oss | Genmao Yu | Genmao Yu |
| [YARN-7289](https://issues.apache.org/jira/browse/YARN-7289) | Application lifetime does not work with FairScheduler |  Major | resourcemanager | Miklos Szegedi | Miklos Szegedi |
| [YARN-7392](https://issues.apache.org/jira/browse/YARN-7392) | Render cluster information on new YARN web ui |  Major | webapp | Vasudevan Skm | Vasudevan Skm |
| [HDFS-11902](https://issues.apache.org/jira/browse/HDFS-11902) | [READ] Merge BlockFormatProvider and FileRegionProvider. |  Major | . | Virajith Jalaparti | Virajith Jalaparti |
| [YARN-7307](https://issues.apache.org/jira/browse/YARN-7307) | Allow client/AM update supported resource types via YARN APIs |  Blocker | nodemanager, resourcemanager | Wangda Tan | Sunil Govindan |
| [HDFS-12607](https://issues.apache.org/jira/browse/HDFS-12607) | [READ] Even one dead datanode with PROVIDED storage results in ProvidedStorageInfo being marked as FAILED |  Major | . | Virajith Jalaparti | Virajith Jalaparti |
| [YARN-7394](https://issues.apache.org/jira/browse/YARN-7394) | Merge code paths for Reservation/Plan queues and Auto Created queues |  Major | capacity scheduler | Suma Shivaprasad | Suma Shivaprasad |
| [HDFS-12671](https://issues.apache.org/jira/browse/HDFS-12671) | [READ] Test NameNode restarts when PROVIDED is configured |  Major | . | Virajith Jalaparti | Virajith Jalaparti |
| [HDFS-12789](https://issues.apache.org/jira/browse/HDFS-12789) | [READ] Image generation tool does not close an opened stream |  Major | . | Virajith Jalaparti | Virajith Jalaparti |
| [YARN-7166](https://issues.apache.org/jira/browse/YARN-7166) | Container REST endpoints should report resource types |  Major | resourcemanager | Daniel Templeton | Daniel Templeton |
| [YARN-7143](https://issues.apache.org/jira/browse/YARN-7143) | FileNotFound handling in ResourceUtils is inconsistent |  Major | resourcemanager | Daniel Templeton | Daniel Templeton |
| [HDFS-12776](https://issues.apache.org/jira/browse/HDFS-12776) | [READ] Increasing replication for PROVIDED files should create local replicas |  Major | . | Virajith Jalaparti | Virajith Jalaparti |
| [HDFS-12779](https://issues.apache.org/jira/browse/HDFS-12779) | [READ] Allow cluster id to be specified to the Image generation tool |  Trivial | . | Virajith Jalaparti | Virajith Jalaparti |
| [HDFS-12777](https://issues.apache.org/jira/browse/HDFS-12777) | [READ] Reduce memory and CPU footprint for PROVIDED volumes. |  Major | . | Virajith Jalaparti | Virajith Jalaparti |
| [YARN-7406](https://issues.apache.org/jira/browse/YARN-7406) | Moving logging APIs over to slf4j in hadoop-yarn-api |  Major | . | Yeliang Cang | Yeliang Cang |
| [YARN-7442](https://issues.apache.org/jira/browse/YARN-7442) | [YARN-7069] Limit format of resource type name |  Blocker | nodemanager, resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-7369](https://issues.apache.org/jira/browse/YARN-7369) | Improve the resource types docs |  Major | docs | Daniel Templeton | Daniel Templeton |
| [YARN-6595](https://issues.apache.org/jira/browse/YARN-6595) | [API] Add Placement Constraints at the application level |  Major | . | Konstantinos Karanasos | Arun Suresh |
| [YARN-7411](https://issues.apache.org/jira/browse/YARN-7411) | Inter-Queue preemption's computeFixpointAllocation need to handle absolute resources while computing normalizedGuarantee |  Major | resourcemanager | Sunil Govindan | Sunil Govindan |
| [YARN-7488](https://issues.apache.org/jira/browse/YARN-7488) | Make ServiceClient.getAppId method public to return ApplicationId for a service name |  Major | . | Gour Saha | Gour Saha |
| [HADOOP-14993](https://issues.apache.org/jira/browse/HADOOP-14993) | AliyunOSS: Override listFiles and listLocatedStatus |  Major | fs/oss | Genmao Yu | Genmao Yu |
| [YARN-6953](https://issues.apache.org/jira/browse/YARN-6953) | Clean up ResourceUtils.setMinimumAllocationForMandatoryResources() and setMaximumAllocationForMandatoryResources() |  Minor | resourcemanager | Daniel Templeton | Manikandan R |
| [HDFS-12775](https://issues.apache.org/jira/browse/HDFS-12775) | [READ] Fix reporting of Provided volumes |  Major | . | Virajith Jalaparti | Virajith Jalaparti |
| [YARN-7482](https://issues.apache.org/jira/browse/YARN-7482) | Max applications calculation per queue has to be retrospected with absolute resource support |  Major | capacity scheduler | Sunil Govindan | Sunil Govindan |
| [YARN-7486](https://issues.apache.org/jira/browse/YARN-7486) | Race condition in service AM that can cause NPE |  Major | . | Jian He | Jian He |
| [YARN-7503](https://issues.apache.org/jira/browse/YARN-7503) | Configurable heap size / JVM opts in service AM |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-7419](https://issues.apache.org/jira/browse/YARN-7419) | CapacityScheduler: Allow auto leaf queue creation after queue mapping |  Major | capacity scheduler | Suma Shivaprasad | Suma Shivaprasad |
| [YARN-7483](https://issues.apache.org/jira/browse/YARN-7483) | CapacityScheduler test cases cleanup post YARN-5881 |  Major | test | Sunil Govindan | Sunil Govindan |
| [HDFS-12801](https://issues.apache.org/jira/browse/HDFS-12801) | RBF: Set MountTableResolver as default file resolver |  Minor | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-7430](https://issues.apache.org/jira/browse/YARN-7430) | Enable user re-mapping for Docker containers by default |  Blocker | security, yarn | Eric Yang | Eric Yang |
| [YARN-7218](https://issues.apache.org/jira/browse/YARN-7218) | ApiServer REST API naming convention /ws/v1 is already used in Hadoop v2 |  Major | api, applications | Eric Yang | Eric Yang |
| [YARN-7448](https://issues.apache.org/jira/browse/YARN-7448) | [API] Add SchedulingRequest to the AllocateRequest |  Major | . | Arun Suresh | Panagiotis Garefalakis |
| [YARN-7529](https://issues.apache.org/jira/browse/YARN-7529) | TestYarnNativeServices#testRecoverComponentsAfterRMRestart() fails intermittently |  Major | . | Chandni Singh | Chandni Singh |
| [YARN-6128](https://issues.apache.org/jira/browse/YARN-6128) | Add support for AMRMProxy HA |  Major | amrmproxy, nodemanager | Subru Krishnan | Botong Huang |
| [HADOOP-15024](https://issues.apache.org/jira/browse/HADOOP-15024) | AliyunOSS: support user agent configuration and include that & Hadoop version information to oss server |  Major | fs, fs/oss | Sammi Chen | Sammi Chen |
| [HDFS-12778](https://issues.apache.org/jira/browse/HDFS-12778) | [READ] Report multiple locations for PROVIDED blocks |  Major | . | Virajith Jalaparti | Virajith Jalaparti |
| [YARN-5534](https://issues.apache.org/jira/browse/YARN-5534) | Allow user provided Docker volume mount list |  Major | yarn | luhuichun | Shane Kumpf |
| [YARN-7330](https://issues.apache.org/jira/browse/YARN-7330) | Add support to show GPU in UI including metrics |  Blocker | . | Wangda Tan | Wangda Tan |
| [YARN-7538](https://issues.apache.org/jira/browse/YARN-7538) | Fix performance regression introduced by Capacity Scheduler absolute min/max resource refactoring |  Major | capacity scheduler | Sunil Govindan | Sunil Govindan |
| [YARN-7544](https://issues.apache.org/jira/browse/YARN-7544) | Use queue-path.capacity/maximum-capacity to specify CapacityScheduler absolute min/max resources |  Major | capacity scheduler | Sunil Govindan | Sunil Govindan |
| [YARN-6168](https://issues.apache.org/jira/browse/YARN-6168) | Restarted RM may not inform AM about all existing containers |  Major | . | Billie Rinaldi | Chandni Singh |
| [HDFS-12809](https://issues.apache.org/jira/browse/HDFS-12809) | [READ] Fix the randomized selection of locations in {{ProvidedBlocksBuilder}}. |  Major | . | Virajith Jalaparti | Virajith Jalaparti |
| [HDFS-12858](https://issues.apache.org/jira/browse/HDFS-12858) | RBF: Add router admin commands usage in HDFS commands reference doc |  Minor | documentation | Yiqun Lin | Yiqun Lin |
| [YARN-7564](https://issues.apache.org/jira/browse/YARN-7564) | Cleanup to fix checkstyle issues of YARN-5881 branch |  Minor | . | Sunil Govindan | Sunil Govindan |
| [YARN-7480](https://issues.apache.org/jira/browse/YARN-7480) | Render tooltips on columns where text is clipped in new YARN UI |  Major | yarn-ui-v2 | Vasudevan Skm | Vasudevan Skm |
| [YARN-7575](https://issues.apache.org/jira/browse/YARN-7575) | NPE in scheduler UI when max-capacity is not configured |  Major | capacity scheduler | Eric Payne | Sunil Govindan |
| [YARN-7533](https://issues.apache.org/jira/browse/YARN-7533) | Documentation for absolute resource support in Capacity Scheduler |  Major | capacity scheduler | Sunil Govindan | Sunil Govindan |
| [HDFS-12835](https://issues.apache.org/jira/browse/HDFS-12835) | RBF: Fix Javadoc parameter errors |  Minor | . | Wei Yan | Wei Yan |
| [YARN-7541](https://issues.apache.org/jira/browse/YARN-7541) | Node updates don't update the maximum cluster capability for resources other than CPU and memory |  Critical | resourcemanager | Daniel Templeton | Daniel Templeton |
| [YARN-7573](https://issues.apache.org/jira/browse/YARN-7573) | Gpu Information page could be empty for nodes without GPU |  Major | webapp, yarn-ui-v2 | Sunil Govindan | Sunil Govindan |
| [HDFS-12685](https://issues.apache.org/jira/browse/HDFS-12685) | [READ] FsVolumeImpl exception when scanning Provided storage volume |  Major | . | Ewan Higgs | Virajith Jalaparti |
| [HDFS-12665](https://issues.apache.org/jira/browse/HDFS-12665) | [AliasMap] Create a version of the AliasMap that runs in memory in the Namenode (leveldb) |  Major | . | Ewan Higgs | Ewan Higgs |
| [YARN-7487](https://issues.apache.org/jira/browse/YARN-7487) | Ensure volume to include GPU base libraries after created by plugin |  Major | . | Wangda Tan | Wangda Tan |
| [YARN-6507](https://issues.apache.org/jira/browse/YARN-6507) | Add support in NodeManager to isolate FPGA devices with CGroups |  Major | yarn | Zhankun Tang | Zhankun Tang |
| [MAPREDUCE-6994](https://issues.apache.org/jira/browse/MAPREDUCE-6994) | Uploader tool for Distributed Cache Deploy code changes |  Major | . | Miklos Szegedi | Miklos Szegedi |
| [HDFS-12591](https://issues.apache.org/jira/browse/HDFS-12591) | [READ] Implement LevelDBFileRegionFormat |  Minor | hdfs | Ewan Higgs | Ewan Higgs |
| [YARN-6907](https://issues.apache.org/jira/browse/YARN-6907) | Node information page in the old web UI should report resource types |  Major | resourcemanager | Daniel Templeton | Gergely Novák |
| [YARN-7587](https://issues.apache.org/jira/browse/YARN-7587) | Skip dispatching opportunistic containers to nodes whose queue is already full |  Major | . | Weiwei Yang | Weiwei Yang |
| [HDFS-12396](https://issues.apache.org/jira/browse/HDFS-12396) | Webhdfs file system should get delegation token from kms provider. |  Major | encryption, kms, webhdfs | Rushabh S Shah | Rushabh S Shah |
| [YARN-7092](https://issues.apache.org/jira/browse/YARN-7092) | Render application specific log under application tab in new YARN UI |  Major | yarn-ui-v2 | Akhil PB | Akhil PB |
| [HADOOP-15071](https://issues.apache.org/jira/browse/HADOOP-15071) | s3a troubleshooting docs to add a couple more failure modes |  Minor | documentation, fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-7438](https://issues.apache.org/jira/browse/YARN-7438) | Additional changes to make SchedulingPlacementSet agnostic to ResourceRequest / placement algorithm |  Major | . | Wangda Tan | Wangda Tan |
| [HDFS-12885](https://issues.apache.org/jira/browse/HDFS-12885) | Add visibility/stability annotations |  Trivial | . | Chris Douglas | Chris Douglas |
| [HADOOP-14475](https://issues.apache.org/jira/browse/HADOOP-14475) | Metrics of S3A don't print out  when enable it in Hadoop metrics property file |  Major | fs/s3 | Yonger | Yonger |
| [HDFS-12713](https://issues.apache.org/jira/browse/HDFS-12713) | [READ] Refactor FileRegion and BlockAliasMap to separate out HDFS metadata and PROVIDED storage metadata |  Major | . | Virajith Jalaparti | Ewan Higgs |
| [HDFS-12894](https://issues.apache.org/jira/browse/HDFS-12894) | [READ] Skip setting block count of ProvidedDatanodeStorageInfo on DN registration update |  Major | . | Virajith Jalaparti | Virajith Jalaparti |
| [YARN-7610](https://issues.apache.org/jira/browse/YARN-7610) | Extend Distributed Shell to support launching job with opportunistic containers |  Major | applications/distributed-shell | Weiwei Yang | Weiwei Yang |
| [HDFS-11640](https://issues.apache.org/jira/browse/HDFS-11640) | [READ] Datanodes should use a unique identifier when reading from external stores |  Major | . | Virajith Jalaparti | Virajith Jalaparti |
| [HDFS-12887](https://issues.apache.org/jira/browse/HDFS-12887) | [READ] Allow Datanodes with Provided volumes to start when blocks with the same id exist locally |  Major | . | Virajith Jalaparti | Virajith Jalaparti |
| [MAPREDUCE-6998](https://issues.apache.org/jira/browse/MAPREDUCE-6998) | Moving logging APIs over to slf4j in hadoop-mapreduce-client-jobclient |  Major | . | Akira Ajisaka | Gergely Novák |
| [MAPREDUCE-7000](https://issues.apache.org/jira/browse/MAPREDUCE-7000) | Moving logging APIs over to slf4j in hadoop-mapreduce-client-nativetask |  Minor | . | Jinjiang Ling | Jinjiang Ling |
| [HDFS-12874](https://issues.apache.org/jira/browse/HDFS-12874) | [READ] Documentation for provided storage |  Major | . | Chris Douglas | Virajith Jalaparti |
| [YARN-7522](https://issues.apache.org/jira/browse/YARN-7522) | Introduce AllocationTagsManager to associate allocation tags to nodes |  Major | . | Wangda Tan | Wangda Tan |
| [HDFS-12905](https://issues.apache.org/jira/browse/HDFS-12905) | [READ] Handle decommissioning and under-maintenance Datanodes with Provided storage. |  Major | . | Virajith Jalaparti | Virajith Jalaparti |
| [HDFS-12893](https://issues.apache.org/jira/browse/HDFS-12893) | [READ] Support replication of Provided blocks with non-default topologies. |  Major | . | Virajith Jalaparti | Virajith Jalaparti |
| [YARN-7443](https://issues.apache.org/jira/browse/YARN-7443) | Add native FPGA module support to do isolation with cgroups |  Major | yarn | Zhankun Tang | Zhankun Tang |
| [YARN-7473](https://issues.apache.org/jira/browse/YARN-7473) | Implement Framework and policy for capacity management of auto created queues |  Major | capacity scheduler | Suma Shivaprasad | Suma Shivaprasad |
| [YARN-7420](https://issues.apache.org/jira/browse/YARN-7420) | YARN UI changes to depict auto created queues |  Major | capacity scheduler | Suma Shivaprasad | Suma Shivaprasad |
| [YARN-7520](https://issues.apache.org/jira/browse/YARN-7520) | Queue Ordering policy changes for ordering auto created leaf queues within Managed parent Queues |  Major | capacity scheduler | Suma Shivaprasad | Suma Shivaprasad |
| [YARN-6704](https://issues.apache.org/jira/browse/YARN-6704) | Add support for work preserving NM restart when FederationInterceptor is enabled in AMRMProxyService |  Major | . | Botong Huang | Botong Huang |
| [YARN-7632](https://issues.apache.org/jira/browse/YARN-7632) | Effective min and max resource need to be set for auto created leaf queues upon creation and capacity management |  Major | capacity scheduler | Suma Shivaprasad | Suma Shivaprasad |
| [MAPREDUCE-7018](https://issues.apache.org/jira/browse/MAPREDUCE-7018) | Apply erasure coding properly to framework tarball and support plain tar |  Major | . | Miklos Szegedi | Miklos Szegedi |
| [HDFS-12875](https://issues.apache.org/jira/browse/HDFS-12875) | RBF: Complete logic for -readonly option of dfsrouteradmin add command |  Major | . | Yiqun Lin | Íñigo Goiri |
| [YARN-7634](https://issues.apache.org/jira/browse/YARN-7634) | Queue ACL validations should validate parent queue ACLs before auto-creating leaf queues |  Major | capacity scheduler | Suma Shivaprasad | Suma Shivaprasad |
| [YARN-7641](https://issues.apache.org/jira/browse/YARN-7641) | Allow searchable filter for Application page log viewer in new YARN UI |  Major | yarn-ui-v2 | Vasudevan Skm | Vasudevan Skm |
| [YARN-7383](https://issues.apache.org/jira/browse/YARN-7383) | Node resource is not parsed correctly for resource names containing dot |  Major | nodemanager, resourcemanager | Jonathan Hung | Gergely Novák |
| [YARN-7643](https://issues.apache.org/jira/browse/YARN-7643) | Handle recovery of applications in case of auto-created leaf queue mapping |  Major | capacity scheduler | Suma Shivaprasad | Suma Shivaprasad |
| [HDFS-12912](https://issues.apache.org/jira/browse/HDFS-12912) | [READ] Fix configuration and implementation of LevelDB-based alias maps |  Major | . | Virajith Jalaparti | Virajith Jalaparti |
| [YARN-7119](https://issues.apache.org/jira/browse/YARN-7119) | Support multiple resource types in rmadmin updateNodeResource command |  Major | nodemanager, resourcemanager | Daniel Templeton | Manikandan R |
| [YARN-7630](https://issues.apache.org/jira/browse/YARN-7630) | Fix AMRMToken rollover handling in AMRMProxy |  Minor | . | Botong Huang | Botong Huang |
| [YARN-7565](https://issues.apache.org/jira/browse/YARN-7565) | Yarn service pre-maturely releases the container after AM restart |  Major | . | Chandni Singh | Chandni Singh |
| [YARN-7638](https://issues.apache.org/jira/browse/YARN-7638) | Unit tests related to preemption for auto created leaf queues feature |  Major | capacity scheduler | Suma Shivaprasad | Suma Shivaprasad |
| [YARN-7633](https://issues.apache.org/jira/browse/YARN-7633) | Documentation for auto queue creation feature and related configurations |  Major | capacity scheduler | Suma Shivaprasad | Suma Shivaprasad |
| [HDFS-12712](https://issues.apache.org/jira/browse/HDFS-12712) | [9806] Code style cleanup |  Minor | . | Íñigo Goiri | Virajith Jalaparti |
| [HDFS-12903](https://issues.apache.org/jira/browse/HDFS-12903) | [READ] Fix closing streams in ImageWriter |  Major | . | Íñigo Goiri | Virajith Jalaparti |
| [YARN-7617](https://issues.apache.org/jira/browse/YARN-7617) | Add a flag in distributed shell to automatically PROMOTE opportunistic containers to guaranteed once they are started |  Minor | applications/distributed-shell | Weiwei Yang | Weiwei Yang |
| [HDFS-12937](https://issues.apache.org/jira/browse/HDFS-12937) | RBF: Add more unit tests for router admin commands |  Major | test | Yiqun Lin | Yiqun Lin |
| [YARN-7620](https://issues.apache.org/jira/browse/YARN-7620) | Allow node partition filters on Queues page of new YARN UI |  Major | yarn-ui-v2 | Vasudevan Skm | Vasudevan Skm |
| [YARN-7670](https://issues.apache.org/jira/browse/YARN-7670) | Modifications to the ResourceScheduler to support SchedulingRequests |  Major | . | Arun Suresh | Arun Suresh |
| [YARN-7032](https://issues.apache.org/jira/browse/YARN-7032) | [ATSv2] NPE while starting hbase co-processor when HBase authorization is enabled. |  Critical | . | Rohith Sharma K S | Rohith Sharma K S |
| [HADOOP-14965](https://issues.apache.org/jira/browse/HADOOP-14965) | s3a input stream "normal" fadvise mode to be adaptive |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-15133](https://issues.apache.org/jira/browse/HADOOP-15133) | [JDK9] Ignore com.sun.javadoc.\* and com.sun.tools.\* in animal-sniffer-maven-plugin to compile with Java 9 |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [YARN-7669](https://issues.apache.org/jira/browse/YARN-7669) | API and interface modifications for placement constraint processor |  Major | . | Arun Suresh | Arun Suresh |
| [HADOOP-15113](https://issues.apache.org/jira/browse/HADOOP-15113) | NPE in S3A getFileStatus: null instrumentation on using closed instance |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-15086](https://issues.apache.org/jira/browse/HADOOP-15086) | NativeAzureFileSystem file rename is not atomic |  Major | fs/azure | Shixiong Zhu | Thomas Marquardt |
| [YARN-7653](https://issues.apache.org/jira/browse/YARN-7653) | Rack cardinality support for AllocationTagsManager |  Major | . | Panagiotis Garefalakis | Panagiotis Garefalakis |
| [YARN-6596](https://issues.apache.org/jira/browse/YARN-6596) | Introduce Placement Constraint Manager module |  Major | . | Konstantinos Karanasos | Konstantinos Karanasos |
| [YARN-7612](https://issues.apache.org/jira/browse/YARN-7612) | Add Processor Framework for Rich Placement Constraints |  Major | . | Arun Suresh | Arun Suresh |
| [YARN-7613](https://issues.apache.org/jira/browse/YARN-7613) | Implement Basic algorithm for constraint based placement |  Major | . | Arun Suresh | Panagiotis Garefalakis |
| [YARN-7682](https://issues.apache.org/jira/browse/YARN-7682) | Expose canSatisfyConstraints utility function to validate a placement against a constraint |  Major | . | Arun Suresh | Panagiotis Garefalakis |
| [HDFS-12988](https://issues.apache.org/jira/browse/HDFS-12988) | RBF: Mount table entries not properly updated in the local cache |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-7557](https://issues.apache.org/jira/browse/YARN-7557) | It should be possible to specify resource types in the fair scheduler increment value |  Critical | fairscheduler | Daniel Templeton | Gergo Repas |
| [YARN-7666](https://issues.apache.org/jira/browse/YARN-7666) | Introduce scheduler specific environment variable support in ApplicationSubmissionContext for better scheduling placement configurations |  Major | . | Sunil Govindan | Sunil Govindan |
| [YARN-7242](https://issues.apache.org/jira/browse/YARN-7242) | Support to specify values of different resource types in DistributedShell for easier testing |  Critical | nodemanager, resourcemanager | Wangda Tan | Gergely Novák |
| [YARN-7704](https://issues.apache.org/jira/browse/YARN-7704) | Document improvement for registry dns |  Major | . | Jian He | Jian He |
| [HADOOP-15161](https://issues.apache.org/jira/browse/HADOOP-15161) | s3a: Stream and common statistics missing from metrics |  Major | . | Sean Mackrory | Sean Mackrory |
| [HDFS-12802](https://issues.apache.org/jira/browse/HDFS-12802) | RBF: Control MountTableResolver cache size |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-12934](https://issues.apache.org/jira/browse/HDFS-12934) | RBF: Federation supports global quota |  Major | . | Yiqun Lin | Yiqun Lin |
| [YARN-7681](https://issues.apache.org/jira/browse/YARN-7681) | Double-check placement constraints in scheduling phase before actual allocation is made |  Major | RM, scheduler | Weiwei Yang | Weiwei Yang |
| [YARN-5366](https://issues.apache.org/jira/browse/YARN-5366) | Improve handling of the Docker container life cycle |  Major | yarn | Shane Kumpf | Shane Kumpf |
| [MAPREDUCE-7030](https://issues.apache.org/jira/browse/MAPREDUCE-7030) | Uploader tool should ignore symlinks to the same directory |  Minor | . | Miklos Szegedi | Miklos Szegedi |
| [YARN-7724](https://issues.apache.org/jira/browse/YARN-7724) | yarn application status should support application name |  Major | yarn-native-services | Yesha Vora | Jian He |
| [YARN-7696](https://issues.apache.org/jira/browse/YARN-7696) | Add container tags to ContainerTokenIdentifier, api.Container and NMContainerStatus to handle all recovery cases |  Major | . | Arun Suresh | Arun Suresh |
| [HDFS-12972](https://issues.apache.org/jira/browse/HDFS-12972) | RBF: Display mount table quota info in Web UI and admin command |  Major | . | Yiqun Lin | Yiqun Lin |
| [MAPREDUCE-7034](https://issues.apache.org/jira/browse/MAPREDUCE-7034) | Moving logging APIs over to slf4j the rest of all in hadoop-mapreduce |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-15079](https://issues.apache.org/jira/browse/HADOOP-15079) | ITestS3AFileOperationCost#testFakeDirectoryDeletion failing after OutputCommitter patch |  Critical | . | Sean Mackrory | Steve Loughran |
| [HDFS-12919](https://issues.apache.org/jira/browse/HDFS-12919) | RBF: Support erasure coding methods in RouterRpcServer |  Critical | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-6736](https://issues.apache.org/jira/browse/YARN-6736) | Consider writing to both ats v1 & v2 from RM for smoother upgrades |  Major | timelineserver | Vrushali C | Aaron Gresch |
| [MAPREDUCE-7032](https://issues.apache.org/jira/browse/MAPREDUCE-7032) | Add the ability to specify a delayed replication count |  Major | . | Miklos Szegedi | Miklos Szegedi |
| [HADOOP-15141](https://issues.apache.org/jira/browse/HADOOP-15141) | Support IAM Assumed roles in S3A |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-15027](https://issues.apache.org/jira/browse/HADOOP-15027) | AliyunOSS: Support multi-thread pre-read to improve sequential read from Hadoop to Aliyun OSS performance |  Major | fs/oss | wujinhu | wujinhu |
| [YARN-6619](https://issues.apache.org/jira/browse/YARN-6619) | AMRMClient Changes to use the PlacementConstraint and SchcedulingRequest objects |  Major | . | Arun Suresh | Arun Suresh |
| [YARN-7709](https://issues.apache.org/jira/browse/YARN-7709) | Remove SELF from TargetExpression type |  Blocker | . | Wangda Tan | Konstantinos Karanasos |
| [YARN-6599](https://issues.apache.org/jira/browse/YARN-6599) | Support anti-affinity constraint via AppPlacementAllocator |  Major | . | Wangda Tan | Wangda Tan |
| [YARN-7745](https://issues.apache.org/jira/browse/YARN-7745) | Allow DistributedShell to take a placement specification for containers it wants to launch |  Major | . | Arun Suresh | Arun Suresh |
| [HDFS-12973](https://issues.apache.org/jira/browse/HDFS-12973) | RBF: Document global quota supporting in federation |  Major | . | Yiqun Lin | Yiqun Lin |
| [HDFS-13028](https://issues.apache.org/jira/browse/HDFS-13028) | RBF: Fix spurious TestRouterRpc#testProxyGetStats |  Minor | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-5094](https://issues.apache.org/jira/browse/YARN-5094) | some YARN container events have timestamp of -1 |  Critical | . | Sangjin Lee | Haibo Chen |
| [MAPREDUCE-6995](https://issues.apache.org/jira/browse/MAPREDUCE-6995) | Uploader tool for Distributed Cache Deploy documentation |  Major | . | Miklos Szegedi | Miklos Szegedi |
| [YARN-7774](https://issues.apache.org/jira/browse/YARN-7774) | Miscellaneous fixes to the PlacementProcessor |  Blocker | . | Arun Suresh | Arun Suresh |
| [YARN-7763](https://issues.apache.org/jira/browse/YARN-7763) | Allow Constraints specified in the SchedulingRequest to override application level constraints |  Blocker | . | Wangda Tan | Weiwei Yang |
| [YARN-7729](https://issues.apache.org/jira/browse/YARN-7729) | Add support for setting the PID namespace mode |  Major | nodemanager | Shane Kumpf | Billie Rinaldi |
| [YARN-7788](https://issues.apache.org/jira/browse/YARN-7788) | Factor out management of temp tags from AllocationTagsManager |  Major | . | Arun Suresh | Arun Suresh |
| [YARN-7779](https://issues.apache.org/jira/browse/YARN-7779) | Display allocation tags in RM web UI and expose same through REST API |  Major | RM | Weiwei Yang | Weiwei Yang |
| [YARN-7782](https://issues.apache.org/jira/browse/YARN-7782) | Enable user re-mapping for Docker containers in yarn-default.xml |  Blocker | security, yarn | Eric Yang | Eric Yang |
| [YARN-7605](https://issues.apache.org/jira/browse/YARN-7605) | Implement doAs for Api Service REST API |  Major | . | Eric Yang | Eric Yang |
| [YARN-7540](https://issues.apache.org/jira/browse/YARN-7540) | Convert yarn app cli to call yarn api services |  Major | . | Eric Yang | Eric Yang |
| [HDFS-12772](https://issues.apache.org/jira/browse/HDFS-12772) | RBF: Federation Router State State Store internal API |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-7783](https://issues.apache.org/jira/browse/YARN-7783) | Add validation step to ensure constraints are not violated due to order in which a request is processed |  Blocker | . | Arun Suresh | Arun Suresh |
| [YARN-7807](https://issues.apache.org/jira/browse/YARN-7807) | Assume intra-app anti-affinity as default for scheduling request inside AppPlacementAllocator |  Blocker | . | Wangda Tan | Wangda Tan |
| [YARN-7795](https://issues.apache.org/jira/browse/YARN-7795) | Fix jenkins issues of YARN-6592 branch |  Blocker | . | Sunil Govindan | Sunil Govindan |
| [HDFS-13042](https://issues.apache.org/jira/browse/HDFS-13042) | RBF: Heartbeat Router State |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-7798](https://issues.apache.org/jira/browse/YARN-7798) | Refactor SLS Reservation Creation |  Minor | . | Young Chen | Young Chen |
| [HDFS-13049](https://issues.apache.org/jira/browse/HDFS-13049) | RBF: Inconsistent Router OPTS config in branch-2 and branch-3 |  Minor | . | Wei Yan | Wei Yan |
| [YARN-7814](https://issues.apache.org/jira/browse/YARN-7814) | Remove automatic mounting of the cgroups root directory into Docker containers |  Major | . | Shane Kumpf | Shane Kumpf |
| [YARN-7784](https://issues.apache.org/jira/browse/YARN-7784) | Fix Cluster metrics when placement processor is enabled |  Major | metrics, RM | Weiwei Yang | Arun Suresh |
| [YARN-6597](https://issues.apache.org/jira/browse/YARN-6597) | Add RMContainer recovery test to verify tag population in the AllocationTagsManager |  Major | . | Konstantinos Karanasos | Panagiotis Garefalakis |
| [YARN-7817](https://issues.apache.org/jira/browse/YARN-7817) | Add Resource reference to RM's NodeInfo object so REST API can get non memory/vcore resource usages. |  Major | . | Sumana Sathish | Sunil Govindan |
| [YARN-7797](https://issues.apache.org/jira/browse/YARN-7797) | Docker host network can not obtain IP address for RegistryDNS |  Major | nodemanager | Eric Yang | Eric Yang |
| [HDFS-12574](https://issues.apache.org/jira/browse/HDFS-12574) | Add CryptoInputStream to WebHdfsFileSystem read call. |  Major | encryption, kms, webhdfs | Rushabh S Shah | Rushabh S Shah |
| [YARN-5148](https://issues.apache.org/jira/browse/YARN-5148) | [UI2] Add page to new YARN UI to view server side configurations/logs/JVM-metrics |  Major | webapp, yarn-ui-v2 | Wangda Tan | Kai Sasaki |
| [YARN-7723](https://issues.apache.org/jira/browse/YARN-7723) | Avoid using docker volume --format option to run against older docker releases |  Major | . | Wangda Tan | Wangda Tan |
| [YARN-7780](https://issues.apache.org/jira/browse/YARN-7780) | Documentation for Placement Constraints |  Major | . | Arun Suresh | Konstantinos Karanasos |
| [YARN-7811](https://issues.apache.org/jira/browse/YARN-7811) | Service AM should use configured default docker network |  Major | yarn-native-services | Billie Rinaldi | Billie Rinaldi |
| [YARN-7822](https://issues.apache.org/jira/browse/YARN-7822) | Constraint satisfaction checker support for composite OR and AND constraints |  Major | . | Arun Suresh | Weiwei Yang |
| [HDFS-13044](https://issues.apache.org/jira/browse/HDFS-13044) | RBF: Add a safe mode for the Router |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-7816](https://issues.apache.org/jira/browse/YARN-7816) | YARN Service - Two different users are unable to launch a service of the same name |  Major | applications | Gour Saha | Gour Saha |
| [HDFS-13043](https://issues.apache.org/jira/browse/HDFS-13043) | RBF: Expose the state of the Routers in the federation |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-12997](https://issues.apache.org/jira/browse/HDFS-12997) | Move logging to slf4j in BlockPoolSliceStorage and Storage |  Major | . | Ajay Kumar | Ajay Kumar |
| [HDFS-13068](https://issues.apache.org/jira/browse/HDFS-13068) | RBF: Add router admin option to manage safe mode |  Major | . | Íñigo Goiri | Yiqun Lin |
| [YARN-7839](https://issues.apache.org/jira/browse/YARN-7839) | Modify PlacementAlgorithm to Check node capacity before placing request on node |  Major | . | Arun Suresh | Panagiotis Garefalakis |
| [YARN-7868](https://issues.apache.org/jira/browse/YARN-7868) | Provide improved error message when YARN service is disabled |  Major | yarn-native-services | Eric Yang | Eric Yang |
| [YARN-7778](https://issues.apache.org/jira/browse/YARN-7778) | Merging of placement constraints defined at different levels |  Major | . | Konstantinos Karanasos | Weiwei Yang |
| [YARN-7860](https://issues.apache.org/jira/browse/YARN-7860) | Fix UT failure TestRMWebServiceAppsNodelabel#testAppsRunning |  Major | . | Weiwei Yang | Sunil Govindan |
| [YARN-7516](https://issues.apache.org/jira/browse/YARN-7516) | Security check for trusted docker image |  Major | . | Eric Yang | Eric Yang |
| [YARN-7815](https://issues.apache.org/jira/browse/YARN-7815) | Make the YARN mounts added to Docker containers more restrictive |  Major | . | Shane Kumpf | Shane Kumpf |
| [HADOOP-15214](https://issues.apache.org/jira/browse/HADOOP-15214) | Make Hadoop compatible with Guava 21.0 |  Minor | . | Igor Dvorzhak | Igor Dvorzhak |
| [YARN-5428](https://issues.apache.org/jira/browse/YARN-5428) | Allow for specifying the docker client configuration directory |  Major | yarn | Shane Kumpf | Shane Kumpf |
| [YARN-7838](https://issues.apache.org/jira/browse/YARN-7838) | Support AND/OR constraints in Distributed Shell |  Critical | distributed-shell | Weiwei Yang | Weiwei Yang |
| [HADOOP-13974](https://issues.apache.org/jira/browse/HADOOP-13974) | S3Guard CLI to support list/purge of pending multipart commits |  Major | fs/s3 | Steve Loughran | Aaron Fabbri |
| [YARN-7917](https://issues.apache.org/jira/browse/YARN-7917) | Fix failing test TestDockerContainerRuntime#testLaunchContainerWithDockerTokens |  Minor | nodemanager | Shane Kumpf | Shane Kumpf |
| [YARN-7914](https://issues.apache.org/jira/browse/YARN-7914) | Fix exit code handling for short lived Docker containers |  Critical | . | Shane Kumpf | Shane Kumpf |
| [HADOOP-15040](https://issues.apache.org/jira/browse/HADOOP-15040) | Upgrade AWS SDK to 1.11.271: NPE bug spams logs w/ Yarn Log Aggregation |  Blocker | fs/s3 | Aaron Fabbri | Aaron Fabbri |
| [YARN-7789](https://issues.apache.org/jira/browse/YARN-7789) | Should fail RM if 3rd resource type is configured but RM uses DefaultResourceCalculator |  Critical | . | Sumana Sathish | Zian Chen |
| [HADOOP-15076](https://issues.apache.org/jira/browse/HADOOP-15076) | Enhance S3A troubleshooting documents and add a performance document |  Blocker | documentation, fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-15176](https://issues.apache.org/jira/browse/HADOOP-15176) | Enhance IAM Assumed Role support in S3A client |  Blocker | fs/s3, test | Steve Loughran | Steve Loughran |
| [YARN-7920](https://issues.apache.org/jira/browse/YARN-7920) | Simplify configuration for PlacementConstraints |  Blocker | . | Wangda Tan | Wangda Tan |
| [YARN-7292](https://issues.apache.org/jira/browse/YARN-7292) | Retrospect Resource Profile Behavior for overriding capability |  Blocker | nodemanager, resourcemanager | Wangda Tan | Wangda Tan |
| [HADOOP-14507](https://issues.apache.org/jira/browse/HADOOP-14507) | extend per-bucket secret key config with explicit getPassword() on fs.s3a.$bucket.secret.key |  Critical | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-7328](https://issues.apache.org/jira/browse/YARN-7328) | ResourceUtils allows yarn.nodemanager.resource-types.memory-mb and .vcores to override yarn.nodemanager.resource.memory-mb and .cpu-vcores |  Critical | nodemanager | Daniel Templeton | lovekesh bansal |
| [HDFS-13119](https://issues.apache.org/jira/browse/HDFS-13119) | RBF: Manage unavailable clusters |  Major | . | Íñigo Goiri | Yiqun Lin |
| [YARN-7940](https://issues.apache.org/jira/browse/YARN-7940) | Service AM gets NoAuth with secure ZK |  Blocker | yarn-native-services | Billie Rinaldi | Billie Rinaldi |
| [YARN-7223](https://issues.apache.org/jira/browse/YARN-7223) | Document GPU isolation feature |  Blocker | . | Wangda Tan | Wangda Tan |
| [HADOOP-15247](https://issues.apache.org/jira/browse/HADOOP-15247) | Move commons-net up to 3.6 |  Minor | fs | Steve Loughran | Steve Loughran |
| [YARN-7916](https://issues.apache.org/jira/browse/YARN-7916) | Remove call to docker logs on failure in container-executor |  Major | . | Shane Kumpf | Shane Kumpf |
| [YARN-7836](https://issues.apache.org/jira/browse/YARN-7836) | YARN Service component update PUT API should not use component name from JSON body |  Major | api, yarn-native-services | Gour Saha | Gour Saha |
| [YARN-7934](https://issues.apache.org/jira/browse/YARN-7934) | [GQ] Refactor preemption calculators to allow overriding for Federation Global Algos |  Major | . | Carlo Curino | Carlo Curino |
| [YARN-7921](https://issues.apache.org/jira/browse/YARN-7921) | Transform a PlacementConstraint to a string expression |  Major | . | Weiwei Yang | Weiwei Yang |
| [HDFS-13187](https://issues.apache.org/jira/browse/HDFS-13187) | RBF: Fix Routers information shown in the web UI |  Minor | . | Wei Yan | Wei Yan |
| [HDFS-13184](https://issues.apache.org/jira/browse/HDFS-13184) | RBF: Improve the unit test TestRouterRPCClientRetries |  Minor | test | Yiqun Lin | Yiqun Lin |
| [YARN-7893](https://issues.apache.org/jira/browse/YARN-7893) | Document the FPGA isolation feature |  Blocker | . | Zhankun Tang | Zhankun Tang |
| [YARN-7959](https://issues.apache.org/jira/browse/YARN-7959) | Add .vm extension to PlacementConstraints.md to ensure proper filtering |  Critical | documentation | Weiwei Yang | Weiwei Yang |
| [HDFS-13199](https://issues.apache.org/jira/browse/HDFS-13199) | RBF: Fix the hdfs router page missing label icon issue |  Major | federation, hdfs | maobaolong | maobaolong |
| [YARN-7929](https://issues.apache.org/jira/browse/YARN-7929) | Support to set container execution type in SLS |  Major | scheduler-load-simulator | Jiandan Yang | Jiandan Yang |
| [HADOOP-15264](https://issues.apache.org/jira/browse/HADOOP-15264) | AWS "shaded" SDK 1.11.271 is pulling in netty 4.1.17 |  Blocker | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-7446](https://issues.apache.org/jira/browse/YARN-7446) | Docker container privileged mode and --user flag contradict each other |  Major | . | Eric Yang | Eric Yang |
| [YARN-7954](https://issues.apache.org/jira/browse/YARN-7954) | Component status stays "Ready" when yarn service is stopped |  Major | . | Yesha Vora | Gour Saha |
| [YARN-7955](https://issues.apache.org/jira/browse/YARN-7955) | Calling stop on an already stopped service says "Successfully stopped service" |  Major | . | Gour Saha | Gour Saha |
| [YARN-7637](https://issues.apache.org/jira/browse/YARN-7637) | GPU volume creation command fails when work preserving is disabled at NM |  Critical | nodemanager | Sunil Govindan | Zian Chen |
| [HADOOP-15274](https://issues.apache.org/jira/browse/HADOOP-15274) | Move hadoop-openstack to slf4j |  Minor | fs/swift | Steve Loughran | fang zhenyi |
| [HADOOP-14652](https://issues.apache.org/jira/browse/HADOOP-14652) | Update metrics-core version to 3.2.4 |  Major | . | Ray Chiang | Ray Chiang |
| [HDFS-1686](https://issues.apache.org/jira/browse/HDFS-1686) | Federation: Add more Balancer tests with federation setting |  Minor | balancer & mover, test | Tsz Wo Nicholas Sze | Bharat Viswanadham |
| [HADOOP-13761](https://issues.apache.org/jira/browse/HADOOP-13761) | S3Guard: implement retries for DDB failures and throttling; translate exceptions |  Blocker | fs/s3 | Aaron Fabbri | Aaron Fabbri |
| [YARN-7915](https://issues.apache.org/jira/browse/YARN-7915) | Trusted image log message repeated multiple times |  Major | . | Eric Badger | Shane Kumpf |
| [HADOOP-15090](https://issues.apache.org/jira/browse/HADOOP-15090) | Add ADL troubleshooting doc |  Major | documentation, fs/adl | Steve Loughran | Steve Loughran |
| [YARN-7972](https://issues.apache.org/jira/browse/YARN-7972) | Support inter-app placement constraints for allocation tags by application ID |  Major | . | Weiwei Yang | Weiwei Yang |
| [HADOOP-15271](https://issues.apache.org/jira/browse/HADOOP-15271) | Remove unicode multibyte characters from JavaDoc |  Major | documentation | Akira Ajisaka | Takanobu Asanuma |
| [HADOOP-15287](https://issues.apache.org/jira/browse/HADOOP-15287) | JDK9 JavaDoc build fails due to one-character underscore identifiers in hadoop-yarn-common |  Major | documentation | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-15291](https://issues.apache.org/jira/browse/HADOOP-15291) | TestMiniKdc fails on Java 9 |  Major | test | Akira Ajisaka | Takanobu Asanuma |
| [YARN-7346](https://issues.apache.org/jira/browse/YARN-7346) | Add a profile to allow optional compilation for ATSv2 with HBase-2.0 |  Major | . | Ted Yu | Haibo Chen |
| [YARN-7919](https://issues.apache.org/jira/browse/YARN-7919) | Refactor timelineservice-hbase module into submodules |  Major | timelineservice | Haibo Chen | Haibo Chen |
| [HDFS-13214](https://issues.apache.org/jira/browse/HDFS-13214) | RBF: Complete document of Router configuration |  Major | . | Tao Jie | Yiqun Lin |
| [HADOOP-15267](https://issues.apache.org/jira/browse/HADOOP-15267) | S3A multipart upload fails when SSE-C encryption is enabled |  Critical | fs/s3 | Anis Elleuch | Anis Elleuch |
| [YARN-7891](https://issues.apache.org/jira/browse/YARN-7891) | LogAggregationIndexedFileController should support read from HAR file |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-7626](https://issues.apache.org/jira/browse/YARN-7626) | Allow regular expression matching in container-executor.cfg for devices and named docker volumes mount |  Major | . | Zian Chen | Zian Chen |
| [HDFS-13230](https://issues.apache.org/jira/browse/HDFS-13230) | RBF: ConnectionManager's cleanup task will compare each pool's own active conns with its total conns |  Minor | . | Wei Yan | Chao Sun |
| [HDFS-13233](https://issues.apache.org/jira/browse/HDFS-13233) | RBF: MountTableResolver doesn't return the correct mount point of the given path |  Major | hdfs | wangzhiyuan | wangzhiyuan |
| [HADOOP-15277](https://issues.apache.org/jira/browse/HADOOP-15277) | remove .FluentPropertyBeanIntrospector from CLI operation log output |  Minor | conf | Steve Loughran | Steve Loughran |
| [HADOOP-15293](https://issues.apache.org/jira/browse/HADOOP-15293) | TestLogLevel fails on Java 9 |  Major | test | Akira Ajisaka | Takanobu Asanuma |
| [HDFS-13212](https://issues.apache.org/jira/browse/HDFS-13212) | RBF: Fix router location cache issue |  Major | federation, hdfs | Weiwei Wu | Weiwei Wu |
| [HDFS-13232](https://issues.apache.org/jira/browse/HDFS-13232) | RBF: ConnectionPool should return first usable connection |  Minor | . | Wei Yan | Ekanth Sethuramalingam |
| [HDFS-13240](https://issues.apache.org/jira/browse/HDFS-13240) | RBF: Update some inaccurate document descriptions |  Minor | . | Yiqun Lin | Yiqun Lin |
| [YARN-7523](https://issues.apache.org/jira/browse/YARN-7523) | Introduce description and version field in Service record |  Critical | . | Gour Saha | Chandni Singh |
| [HADOOP-15297](https://issues.apache.org/jira/browse/HADOOP-15297) | Make S3A etag =\> checksum feature optional |  Blocker | fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-11399](https://issues.apache.org/jira/browse/HDFS-11399) | Many tests fails in Windows due to injecting disk failures |  Major | . | Yiqun Lin | Yiqun Lin |
| [HDFS-12677](https://issues.apache.org/jira/browse/HDFS-12677) | Extend TestReconstructStripedFile with a random EC policy |  Major | erasure-coding, test | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-13241](https://issues.apache.org/jira/browse/HDFS-13241) | RBF: TestRouterSafemode failed if the port 8888 is in use |  Major | hdfs, test | maobaolong | maobaolong |
| [HDFS-13253](https://issues.apache.org/jira/browse/HDFS-13253) | RBF: Quota management incorrect parent-child relationship judgement |  Major | . | Yiqun Lin | Yiqun Lin |
| [HDFS-13226](https://issues.apache.org/jira/browse/HDFS-13226) | RBF: Throw the exception if mount table entry validated failed |  Major | hdfs | maobaolong | maobaolong |
| [HDFS-12505](https://issues.apache.org/jira/browse/HDFS-12505) | Extend TestFileStatusWithECPolicy with a random EC policy |  Major | erasure-coding, test | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-12587](https://issues.apache.org/jira/browse/HDFS-12587) | Use Parameterized tests in TestBlockInfoStriped and TestLowRedundancyBlockQueues to test all EC policies |  Major | erasure-coding, test | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-5015](https://issues.apache.org/jira/browse/YARN-5015) | Support sliding window retry capability for container restart |  Major | nodemanager | Varun Vasudev | Chandni Singh |
| [YARN-7657](https://issues.apache.org/jira/browse/YARN-7657) | Queue Mapping could provide options to provide 'user' specific auto-created queues under a specified group parent queue |  Major | capacity scheduler | Suma Shivaprasad | Suma Shivaprasad |
| [HADOOP-15308](https://issues.apache.org/jira/browse/HADOOP-15308) | TestConfiguration fails on Windows because of paths |  Major | test | Íñigo Goiri | Xiao Liang |
| [HDFS-12773](https://issues.apache.org/jira/browse/HDFS-12773) | RBF: Improve State Store FS implementation |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HADOOP-15294](https://issues.apache.org/jira/browse/HADOOP-15294) | TestUGILoginFromKeytab fails on Java9 |  Major | security | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-7999](https://issues.apache.org/jira/browse/YARN-7999) | Docker launch fails when user private filecache directory is missing |  Major | . | Eric Yang | Jason Lowe |
| [HDFS-13198](https://issues.apache.org/jira/browse/HDFS-13198) | RBF: RouterHeartbeatService throws out CachedStateStore related exceptions when starting router |  Minor | . | Wei Yan | Wei Yan |
| [HADOOP-15278](https://issues.apache.org/jira/browse/HADOOP-15278) | log s3a at info |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-13224](https://issues.apache.org/jira/browse/HDFS-13224) | RBF: Resolvers to support mount points across multiple subclusters |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-13215](https://issues.apache.org/jira/browse/HDFS-13215) | RBF: Move Router to its own module |  Major | . | Íñigo Goiri | Wei Yan |
| [YARN-8053](https://issues.apache.org/jira/browse/YARN-8053) | Add hadoop-distcp in exclusion in hbase-server dependencies for timelineservice-hbase packages. |  Major | . | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-13250](https://issues.apache.org/jira/browse/HDFS-13250) | RBF: Router to manage requests across multiple subclusters |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-11190](https://issues.apache.org/jira/browse/HDFS-11190) | [READ] Namenode support for data stored in external stores. |  Major | . | Virajith Jalaparti | Virajith Jalaparti |
| [HDFS-10675](https://issues.apache.org/jira/browse/HDFS-10675) | [READ] Datanode support to read from external stores. |  Major | . | Virajith Jalaparti | Virajith Jalaparti |
| [HDFS-13318](https://issues.apache.org/jira/browse/HDFS-13318) | RBF: Fix FindBugs in hadoop-hdfs-rbf |  Minor | . | Íñigo Goiri | Ekanth Sethuramalingam |
| [HDFS-12792](https://issues.apache.org/jira/browse/HDFS-12792) | RBF: Test Router-based federation using HDFSContract |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-7581](https://issues.apache.org/jira/browse/YARN-7581) | HBase filters are not constructed correctly in ATSv2 |  Major | ATSv2 | Haibo Chen | Haibo Chen |
| [YARN-7986](https://issues.apache.org/jira/browse/YARN-7986) | ATSv2 REST API queries do not return results for uppercase application tags |  Critical | . | Charan Hebri | Charan Hebri |
| [HDFS-12512](https://issues.apache.org/jira/browse/HDFS-12512) | RBF: Add WebHDFS |  Major | fs | Íñigo Goiri | Wei Yan |
| [YARN-8070](https://issues.apache.org/jira/browse/YARN-8070) | Yarn Service API site doc broken due to unwanted character in YarnServiceAPI.md |  Blocker | site | Gour Saha | Gour Saha |
| [HDFS-13291](https://issues.apache.org/jira/browse/HDFS-13291) | RBF: Implement available space based OrderResolver |  Major | . | Yiqun Lin | Yiqun Lin |
| [HDFS-13204](https://issues.apache.org/jira/browse/HDFS-13204) | RBF: Optimize name service safe mode icon |  Minor | . | liuhongtong | liuhongtong |
| [HDFS-13352](https://issues.apache.org/jira/browse/HDFS-13352) | RBF: Add xsl stylesheet for hdfs-rbf-default.xml |  Major | documentation | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-8010](https://issues.apache.org/jira/browse/YARN-8010) | Add config in FederationRMFailoverProxy to not bypass facade cache when failing over |  Minor | . | Botong Huang | Botong Huang |
| [HDFS-13347](https://issues.apache.org/jira/browse/HDFS-13347) | RBF: Cache datanode reports |  Minor | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-8069](https://issues.apache.org/jira/browse/YARN-8069) | Clean up example hostnames |  Major | . | Billie Rinaldi | Billie Rinaldi |
| [HDFS-13364](https://issues.apache.org/jira/browse/HDFS-13364) | RBF: Support NamenodeProtocol in the Router |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HADOOP-14651](https://issues.apache.org/jira/browse/HADOOP-14651) | Update okhttp version to 2.7.5 |  Major | fs/adl | Ray Chiang | Ray Chiang |
| [YARN-8027](https://issues.apache.org/jira/browse/YARN-8027) | Setting hostname of docker container breaks for --net=host in docker 1.13 |  Major | yarn | Jim Brennan | Jim Brennan |
| [YARN-7810](https://issues.apache.org/jira/browse/YARN-7810) | TestDockerContainerRuntime test failures due to UID lookup of a non-existent user |  Major | . | Shane Kumpf | Shane Kumpf |
| [HADOOP-15497](https://issues.apache.org/jira/browse/HADOOP-15497) | TestTrash should use proper test path to avoid failing on Windows |  Minor | . | Anbang Hu | Anbang Hu |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-12376](https://issues.apache.org/jira/browse/HDFS-12376) | Enable JournalNode Sync by default |  Major | hdfs | Hanisha Koneru | Hanisha Koneru |
| [YARN-6499](https://issues.apache.org/jira/browse/YARN-6499) | Remove the doc about Schedulable#redistributeShare() |  Trivial | fairscheduler | Yufei Gu | Chetna Chaudhari |
| [YARN-7343](https://issues.apache.org/jira/browse/YARN-7343) | Add a junit test for ContainerScheduler recovery |  Minor | . | kartheek muthyala | Sampada Dehankar |
| [YARN-6124](https://issues.apache.org/jira/browse/YARN-6124) | Make SchedulingEditPolicy can be enabled / disabled / updated with RMAdmin -refreshQueues |  Major | . | Wangda Tan | Zian Chen |
| [HADOOP-15149](https://issues.apache.org/jira/browse/HADOOP-15149) | CryptoOutputStream should implement StreamCapabilities |  Major | fs | Mike Drob | Xiao Chen |
| [YARN-7691](https://issues.apache.org/jira/browse/YARN-7691) | Add Unit Tests for ContainersLauncher |  Major | . | Sampada Dehankar | Sampada Dehankar |
| [YARN-7468](https://issues.apache.org/jira/browse/YARN-7468) | Provide means for container network policy control |  Major | nodemanager | Clay B. | Xuan Gong |
| [YARN-6486](https://issues.apache.org/jira/browse/YARN-6486) | FairScheduler: Deprecate continuous scheduling |  Major | fairscheduler | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [HADOOP-15177](https://issues.apache.org/jira/browse/HADOOP-15177) | Update the release year to 2018 |  Blocker | build | Akira Ajisaka | Bharat Viswanadham |
| [HADOOP-15197](https://issues.apache.org/jira/browse/HADOOP-15197) | Remove tomcat from the Hadoop-auth test bundle |  Major | . | Xiao Chen | Xiao Chen |
| [HADOOP-14325](https://issues.apache.org/jira/browse/HADOOP-14325) | [Umbrella] Stabilise S3A Server Side Encryption |  Major | documentation, fs/s3, test | Steve Loughran |  |
| [YARN-7918](https://issues.apache.org/jira/browse/YARN-7918) | Fix TestAMRMClientPlacementConstraints |  Critical | . | Botong Huang | Gergely Novák |
| [HDFS-13052](https://issues.apache.org/jira/browse/HDFS-13052) | WebHDFS: Add support for snasphot diff |  Major | . | Lokesh Jain | Lokesh Jain |
| [HADOOP-14742](https://issues.apache.org/jira/browse/HADOOP-14742) | Document multi-URI replication Inode for ViewFS |  Major | documentation, viewfs | Chris Douglas | Gera Shegalov |
| [HDFS-13141](https://issues.apache.org/jira/browse/HDFS-13141) | WebHDFS: Add support for getting snasphottable directory list |  Major | webhdfs | Lokesh Jain | Lokesh Jain |
| [YARN-8072](https://issues.apache.org/jira/browse/YARN-8072) | RM log is getting flooded with MemoryPlacementConstraintManager info logs |  Critical | . | Zian Chen | Zian Chen |


