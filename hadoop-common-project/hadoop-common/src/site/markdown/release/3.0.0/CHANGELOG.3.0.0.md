
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

## Release 3.0.0 - 2017-12-13

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-6623](https://issues.apache.org/jira/browse/YARN-6623) | Add support to turn off launching privileged containers in the container-executor |  Blocker | nodemanager | Varun Vasudev | Varun Vasudev |
| [HADOOP-14816](https://issues.apache.org/jira/browse/HADOOP-14816) | Update Dockerfile to use Xenial |  Major | build, test | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-14957](https://issues.apache.org/jira/browse/HADOOP-14957) | ReconfigurationTaskStatus is exposing guava Optional in its public api |  Major | common | Haibo Chen | Xiao Chen |
| [MAPREDUCE-6983](https://issues.apache.org/jira/browse/MAPREDUCE-6983) | Moving logging APIs over to slf4j in hadoop-mapreduce-client-core |  Major | . | Jinjiang Ling | Jinjiang Ling |
| [HDFS-12682](https://issues.apache.org/jira/browse/HDFS-12682) | ECAdmin -listPolicies will always show SystemErasureCodingPolicies state as DISABLED |  Blocker | erasure-coding | Xiao Chen | Xiao Chen |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-1492](https://issues.apache.org/jira/browse/YARN-1492) | truly shared cache for jars (jobjar/libjar) |  Major | . | Sangjin Lee | Chris Trezzo |
| [HDFS-10467](https://issues.apache.org/jira/browse/HDFS-10467) | Router-based HDFS federation |  Major | fs | Íñigo Goiri | Íñigo Goiri |
| [YARN-5734](https://issues.apache.org/jira/browse/YARN-5734) | OrgQueue for easy CapacityScheduler queue configuration management |  Major | . | Min Shen | Min Shen |
| [MAPREDUCE-5951](https://issues.apache.org/jira/browse/MAPREDUCE-5951) | Add support for the YARN Shared Cache |  Major | . | Chris Trezzo | Chris Trezzo |
| [YARN-6871](https://issues.apache.org/jira/browse/YARN-6871) | Add additional deSelects params in RMWebServices#getAppReport |  Major | resourcemanager, router | Giovanni Matteo Fumarola | Tanuj Nayak |
| [HADOOP-14840](https://issues.apache.org/jira/browse/HADOOP-14840) | Tool to estimate resource requirements of an application pipeline based on prior executions |  Major | tools | Subru Krishnan | Rui Li |
| [YARN-3813](https://issues.apache.org/jira/browse/YARN-3813) | Support Application timeout feature in YARN. |  Major | scheduler | nijel | Rohith Sharma K S |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-6829](https://issues.apache.org/jira/browse/MAPREDUCE-6829) | Add peak memory usage counter for each task |  Major | mrv2 | Yufei Gu | Miklos Szegedi |
| [YARN-7045](https://issues.apache.org/jira/browse/YARN-7045) | Remove FSLeafQueue#addAppSchedulable |  Major | fairscheduler | Yufei Gu | Sen Zhao |
| [YARN-7240](https://issues.apache.org/jira/browse/YARN-7240) | Add more states and transitions to stabilize the NM Container state machine |  Major | . | Arun Suresh | kartheek muthyala |
| [HADOOP-14909](https://issues.apache.org/jira/browse/HADOOP-14909) | Fix the word of "erasure encoding" in the top page |  Trivial | documentation | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-14095](https://issues.apache.org/jira/browse/HADOOP-14095) | Document caveats about the default JavaKeyStoreProvider in KMS |  Major | documentation, kms | Xiao Chen | Xiao Chen |
| [HADOOP-14928](https://issues.apache.org/jira/browse/HADOOP-14928) | Update site release notes for 3.0.0 GA |  Major | site | Andrew Wang | Andrew Wang |
| [HDFS-12420](https://issues.apache.org/jira/browse/HDFS-12420) | Add an option to disallow 'namenode format -force' |  Major | . | Ajay Kumar | Ajay Kumar |
| [HADOOP-14521](https://issues.apache.org/jira/browse/HADOOP-14521) | KMS client needs retry logic |  Major | . | Rushabh S Shah | Rushabh S Shah |
| [YARN-2162](https://issues.apache.org/jira/browse/YARN-2162) | add ability in Fair Scheduler to optionally configure maxResources in terms of percentage |  Major | fairscheduler, scheduler | Ashwin Shankar | Yufei Gu |
| [YARN-7207](https://issues.apache.org/jira/browse/YARN-7207) | Cache the RM proxy server address |  Major | RM | Yufei Gu | Yufei Gu |
| [HADOOP-14939](https://issues.apache.org/jira/browse/HADOOP-14939) | Update project release notes with HDFS-10467 for 3.0.0 |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-12573](https://issues.apache.org/jira/browse/HDFS-12573) | Divide the total block metrics into replica and ec |  Major | erasure-coding, metrics, namenode | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-12553](https://issues.apache.org/jira/browse/HDFS-12553) | Add nameServiceId to QJournalProtocol |  Major | journal-node | Bharat Viswanadham | Bharat Viswanadham |
| [HDFS-12603](https://issues.apache.org/jira/browse/HDFS-12603) | Enable async edit logging by default |  Major | namenode | Andrew Wang | Andrew Wang |
| [HDFS-12642](https://issues.apache.org/jira/browse/HDFS-12642) | Log block and datanode details in BlockRecoveryWorker |  Major | datanode | Xiao Chen | Xiao Chen |
| [HADOOP-14938](https://issues.apache.org/jira/browse/HADOOP-14938) | Configuration.updatingResource map should be initialized lazily |  Major | . | Misha Dmitriev | Misha Dmitriev |
| [HDFS-12613](https://issues.apache.org/jira/browse/HDFS-12613) | Native EC coder should implement release() as idempotent function. |  Major | erasure-coding | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [MAPREDUCE-6972](https://issues.apache.org/jira/browse/MAPREDUCE-6972) | Enable try-with-resources for RecordReader |  Major | . | Zoltan Haindrich | Zoltan Haindrich |
| [HADOOP-14880](https://issues.apache.org/jira/browse/HADOOP-14880) | [KMS] Document&test missing KMS client side configs |  Minor | . | Wei-Chiu Chuang | Gabor Bota |
| [HDFS-12619](https://issues.apache.org/jira/browse/HDFS-12619) | Do not catch and throw unchecked exceptions if IBRs fail to process |  Minor | namenode | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-14771](https://issues.apache.org/jira/browse/HADOOP-14771) | hadoop-client does not include hadoop-yarn-client |  Critical | common | Haibo Chen | Ajay Kumar |
| [YARN-7359](https://issues.apache.org/jira/browse/YARN-7359) | TestAppManager.testQueueSubmitWithNoPermission() should be scheduler agnostic |  Minor | . | Haibo Chen | Haibo Chen |
| [HDFS-12448](https://issues.apache.org/jira/browse/HDFS-12448) | Make sure user defined erasure coding policy ID will not overflow |  Major | erasure-coding | Sammi Chen | Huafeng Wang |
| [HADOOP-14944](https://issues.apache.org/jira/browse/HADOOP-14944) | Add JvmMetrics to KMS |  Major | kms | Xiao Chen | Xiao Chen |
| [YARN-7261](https://issues.apache.org/jira/browse/YARN-7261) | Add debug message for better download latency monitoring |  Major | nodemanager | Yufei Gu | Yufei Gu |
| [YARN-7357](https://issues.apache.org/jira/browse/YARN-7357) | Several methods in TestZKRMStateStore.TestZKRMStateStoreTester.TestZKRMStateStoreInternal should have @Override annotations |  Trivial | resourcemanager | Daniel Templeton | Sen Zhao |
| [YARN-4163](https://issues.apache.org/jira/browse/YARN-4163) | Audit getQueueInfo and getApplications calls |  Major | . | Chang Li | Chang Li |
| [HADOOP-9657](https://issues.apache.org/jira/browse/HADOOP-9657) | NetUtils.wrapException to have special handling for 0.0.0.0 addresses and :0 ports |  Minor | net | Steve Loughran | Varun Saxena |
| [YARN-7389](https://issues.apache.org/jira/browse/YARN-7389) | Make TestResourceManager Scheduler agnostic |  Major | test | Robert Kanter | Robert Kanter |
| [HDFS-12544](https://issues.apache.org/jira/browse/HDFS-12544) | SnapshotDiff - support diff generation on any snapshot root descendant directory |  Major | hdfs | Manoj Govindassamy | Manoj Govindassamy |
| [YARN-7358](https://issues.apache.org/jira/browse/YARN-7358) | TestZKConfigurationStore and TestLeveldbConfigurationStore should explicitly set capacity scheduler |  Minor | resourcemanager | Haibo Chen | Haibo Chen |
| [YARN-7320](https://issues.apache.org/jira/browse/YARN-7320) | Duplicate LiteralByteStrings in SystemCredentialsForAppsProto.credentialsForApp\_ |  Major | . | Misha Dmitriev | Misha Dmitriev |
| [YARN-7262](https://issues.apache.org/jira/browse/YARN-7262) | Add a hierarchy into the ZKRMStateStore for delegation token znodes to prevent jute buffer overflow |  Major | . | Robert Kanter | Robert Kanter |
| [YARN-7397](https://issues.apache.org/jira/browse/YARN-7397) | Reduce lock contention in FairScheduler#getAppWeight() |  Major | fairscheduler | Daniel Templeton | Daniel Templeton |
| [HADOOP-14992](https://issues.apache.org/jira/browse/HADOOP-14992) | Upgrade Avro patch version |  Major | build | Chris Douglas | Bharat Viswanadham |
| [YARN-6413](https://issues.apache.org/jira/browse/YARN-6413) | FileSystem based Yarn Registry implementation |  Major | amrmproxy, api, resourcemanager | Ellen Hui | Ellen Hui |
| [HDFS-12482](https://issues.apache.org/jira/browse/HDFS-12482) | Provide a configuration to adjust the weight of EC recovery tasks to adjust the speed of recovery |  Minor | erasure-coding | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HDFS-12744](https://issues.apache.org/jira/browse/HDFS-12744) | More logs when short-circuit read is failed and disabled |  Major | datanode | Weiwei Yang | Weiwei Yang |
| [HDFS-12771](https://issues.apache.org/jira/browse/HDFS-12771) | Add genstamp and block size to metasave Corrupt blocks list |  Minor | . | Kuhu Shukla | Kuhu Shukla |
| [HADOOP-14987](https://issues.apache.org/jira/browse/HADOOP-14987) | Improve KMSClientProvider log around delegation token checking |  Major | . | Xiaoyu Yao | Xiaoyu Yao |
| [MAPREDUCE-6975](https://issues.apache.org/jira/browse/MAPREDUCE-6975) | Logging task counters |  Major | task | Prabhu Joseph | Prabhu Joseph |
| [YARN-7401](https://issues.apache.org/jira/browse/YARN-7401) | Reduce lock contention in ClusterNodeTracker#getClusterCapacity() |  Major | resourcemanager | Daniel Templeton | Daniel Templeton |
| [HDFS-7060](https://issues.apache.org/jira/browse/HDFS-7060) | Avoid taking locks when sending heartbeats from the DataNode |  Major | . | Haohui Mai | Jiandan Yang |
| [YARN-7413](https://issues.apache.org/jira/browse/YARN-7413) | Support resource type in SLS |  Major | scheduler-load-simulator | Yufei Gu | Yufei Gu |
| [YARN-7386](https://issues.apache.org/jira/browse/YARN-7386) | Duplicate Strings in various places in Yarn memory |  Major | . | Misha Dmitriev | Misha Dmitriev |
| [HADOOP-14960](https://issues.apache.org/jira/browse/HADOOP-14960) | Add GC time percentage monitor/alerter |  Major | . | Misha Dmitriev | Misha Dmitriev |
| [HADOOP-15037](https://issues.apache.org/jira/browse/HADOOP-15037) | Add site release notes for OrgQueue and resource types |  Major | . | Andrew Wang | Andrew Wang |
| [HADOOP-14876](https://issues.apache.org/jira/browse/HADOOP-14876) | Create downstream developer docs from the compatibility guidelines |  Critical | documentation | Daniel Templeton | Daniel Templeton |
| [HADOOP-14112](https://issues.apache.org/jira/browse/HADOOP-14112) | Über-jira adl:// Azure Data Lake Phase I: Stabilization |  Major | fs/adl | Steve Loughran | John Zhuge |
| [HADOOP-15104](https://issues.apache.org/jira/browse/HADOOP-15104) | AliyunOSS: change the default value of max error retry |  Major | fs/oss | wujinhu | wujinhu |
| [HADOOP-13204](https://issues.apache.org/jira/browse/HADOOP-13204) | Über-jira: S3a phase III: scale and tuning |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-14552](https://issues.apache.org/jira/browse/HADOOP-14552) | Über-jira: WASB client phase II: performance and testing |  Major | fs/azure | Steve Loughran | Thomas Marquardt |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-7172](https://issues.apache.org/jira/browse/YARN-7172) | ResourceCalculator.fitsIn() should not take a cluster resource parameter |  Major | scheduler | Daniel Templeton | Sen Zhao |
| [HADOOP-14901](https://issues.apache.org/jira/browse/HADOOP-14901) | ReuseObjectMapper in Hadoop Common |  Minor | . | Hanisha Koneru | Hanisha Koneru |
| [YARN-7248](https://issues.apache.org/jira/browse/YARN-7248) | NM returns new SCHEDULED container status to older clients |  Blocker | nodemanager | Jason Lowe | Arun Suresh |
| [HADOOP-14902](https://issues.apache.org/jira/browse/HADOOP-14902) | LoadGenerator#genFile write close timing is incorrectly calculated |  Major | fs | Jason Lowe | Hanisha Koneru |
| [YARN-7084](https://issues.apache.org/jira/browse/YARN-7084) | TestSchedulingMonitor#testRMStarts fails sporadically |  Major | . | Jason Lowe | Jason Lowe |
| [HDFS-12453](https://issues.apache.org/jira/browse/HDFS-12453) | TestDataNodeHotSwapVolumes fails in trunk Jenkins runs |  Critical | test | Arpit Agarwal | Lei (Eddy) Xu |
| [HADOOP-14915](https://issues.apache.org/jira/browse/HADOOP-14915) | method name is incorrect in ConfServlet |  Minor | . | Bharat Viswanadham | Bharat Viswanadham |
| [HADOOP-14752](https://issues.apache.org/jira/browse/HADOOP-14752) | TestCopyFromLocal#testCopyFromLocalWithThreads is fleaky |  Major | test | Andras Bokor | Andras Bokor |
| [HDFS-12569](https://issues.apache.org/jira/browse/HDFS-12569) | Unset EC policy logs empty payload in edit log |  Blocker | erasure-coding | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [YARN-6943](https://issues.apache.org/jira/browse/YARN-6943) | Update Yarn to YARN in documentation |  Minor | documentation | Miklos Szegedi | Chetna Chaudhari |
| [YARN-7211](https://issues.apache.org/jira/browse/YARN-7211) | AMSimulator in SLS does't work due to refactor of responseId |  Blocker | scheduler-load-simulator | Yufei Gu | Botong Huang |
| [HADOOP-14459](https://issues.apache.org/jira/browse/HADOOP-14459) | SerializationFactory shouldn't throw a NullPointerException if the serializations list is not defined |  Minor | . | Nandor Kollar | Nandor Kollar |
| [YARN-7044](https://issues.apache.org/jira/browse/YARN-7044) | TestContainerAllocation#testAMContainerAllocationWhenDNSUnavailable fails |  Major | capacity scheduler, test | Wangda Tan | Akira Ajisaka |
| [YARN-7226](https://issues.apache.org/jira/browse/YARN-7226) | Whitelisted variables do not support delayed variable expansion |  Major | nodemanager | Jason Lowe | Jason Lowe |
| [HADOOP-14616](https://issues.apache.org/jira/browse/HADOOP-14616) | Client fails to read a block with erasure code (XOR, native) when one of the data block is lost |  Blocker | . | Ayappan | Huafeng Wang |
| [YARN-7279](https://issues.apache.org/jira/browse/YARN-7279) | Fix typo in helper message of ContainerLauncher |  Trivial | . | Elek, Marton | Elek, Marton |
| [YARN-7258](https://issues.apache.org/jira/browse/YARN-7258) | Add Node and Rack Hints to Opportunistic Scheduler |  Major | . | Arun Suresh | kartheek muthyala |
| [YARN-7009](https://issues.apache.org/jira/browse/YARN-7009) | TestNMClient.testNMClientNoCleanupOnStop is flaky by design |  Major | . | Miklos Szegedi | Miklos Szegedi |
| [HDFS-12567](https://issues.apache.org/jira/browse/HDFS-12567) | BlockPlacementPolicyRackFaultTolerant fails with racks with very few nodes |  Major | erasure-coding | Andrew Wang | Andrew Wang |
| [HDFS-12494](https://issues.apache.org/jira/browse/HDFS-12494) | libhdfs SIGSEGV in setTLSExceptionStrings |  Major | libhdfs | John Zhuge | John Zhuge |
| [YARN-7245](https://issues.apache.org/jira/browse/YARN-7245) | Max AM Resource column in Active Users Info section of Capacity Scheduler UI page should be updated per-user |  Major | capacity scheduler, yarn | Eric Payne | Eric Payne |
| [HDFS-12606](https://issues.apache.org/jira/browse/HDFS-12606) | When using native decoder, DFSStripedStream#close crashes JVM after being called multiple times. |  Critical | erasure-coding | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HDFS-12599](https://issues.apache.org/jira/browse/HDFS-12599) | Remove Mockito dependency from DataNodeTestUtils |  Minor | test | Ted Yu | Ted Yu |
| [YARN-7309](https://issues.apache.org/jira/browse/YARN-7309) | TestClientRMService#testUpdateApplicationPriorityRequest and TestClientRMService#testUpdatePriorityAndKillAppWithZeroClusterResource test functionality not supported by FairScheduler |  Major | test | Robert Kanter | Robert Kanter |
| [HADOOP-14912](https://issues.apache.org/jira/browse/HADOOP-14912) | FairCallQueue may defer servicing calls |  Major | ipc | Daryn Sharp | Daryn Sharp |
| [HDFS-12635](https://issues.apache.org/jira/browse/HDFS-12635) | Unnecessary exception declaration of the CellBuffers constructor |  Minor | . | Huafeng Wang | Huafeng Wang |
| [HDFS-12622](https://issues.apache.org/jira/browse/HDFS-12622) | Fix enumerate in HDFSErasureCoding.md |  Minor | documentation | Akira Ajisaka | Yiqun Lin |
| [YARN-7082](https://issues.apache.org/jira/browse/YARN-7082) | TestContainerManagerSecurity failing in trunk |  Major | . | Varun Saxena | Akira Ajisaka |
| [HADOOP-13556](https://issues.apache.org/jira/browse/HADOOP-13556) | Change Configuration.getPropsWithPrefix to use getProps instead of iterator |  Major | . | Larry McCay | Larry McCay |
| [HADOOP-13102](https://issues.apache.org/jira/browse/HADOOP-13102) | Update GroupsMapping documentation to reflect the new changes |  Major | documentation | Anu Engineer | Esther Kundin |
| [YARN-7270](https://issues.apache.org/jira/browse/YARN-7270) | Fix unsafe casting from long to int for class Resource and its sub-classes |  Major | resourcemanager | Yufei Gu | Yufei Gu |
| [YARN-7310](https://issues.apache.org/jira/browse/YARN-7310) | TestAMRMProxy#testAMRMProxyE2E fails with FairScheduler |  Major | test | Robert Kanter | Robert Kanter |
| [YARN-7269](https://issues.apache.org/jira/browse/YARN-7269) | Tracking URL in the app state does not get redirected to ApplicationMaster for Running applications |  Critical | . | Sumana Sathish | Tan, Wangda |
| [HDFS-12659](https://issues.apache.org/jira/browse/HDFS-12659) | Update TestDeadDatanode#testNonDFSUsedONDeadNodeReReg to increase heartbeat recheck interval |  Minor | . | Ajay Kumar | Ajay Kumar |
| [HDFS-12485](https://issues.apache.org/jira/browse/HDFS-12485) | expunge may fail to remove trash from encryption zone |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-14949](https://issues.apache.org/jira/browse/HADOOP-14949) | TestKMS#testACLs fails intermittently |  Major | kms, test | Xiao Chen | Xiao Chen |
| [YARN-7124](https://issues.apache.org/jira/browse/YARN-7124) | LogAggregationTFileController deletes/renames while file is open |  Critical | nodemanager | Daryn Sharp | Jason Lowe |
| [YARN-7333](https://issues.apache.org/jira/browse/YARN-7333) | container-executor fails to remove entries from a directory that is not writable or executable |  Critical | . | Jason Lowe | Jason Lowe |
| [YARN-7308](https://issues.apache.org/jira/browse/YARN-7308) | TestApplicationACLs fails with FairScheduler |  Major | test | Robert Kanter | Robert Kanter |
| [HADOOP-14948](https://issues.apache.org/jira/browse/HADOOP-14948) | Document missing config key hadoop.treat.subject.external |  Minor | security | Wei-Chiu Chuang | Ajay Kumar |
| [HDFS-12614](https://issues.apache.org/jira/browse/HDFS-12614) | FSPermissionChecker#getINodeAttrs() throws NPE when INodeAttributesProvider configured |  Major | . | Manoj Govindassamy | Manoj Govindassamy |
| [YARN-7341](https://issues.apache.org/jira/browse/YARN-7341) | TestRouterWebServiceUtil#testMergeMetrics is flakey |  Major | federation | Robert Kanter | Robert Kanter |
| [HDFS-12612](https://issues.apache.org/jira/browse/HDFS-12612) | DFSStripedOutputStream#close will throw if called a second time with a failed streamer |  Major | erasure-coding | Andrew Wang | Lei (Eddy) Xu |
| [HADOOP-14958](https://issues.apache.org/jira/browse/HADOOP-14958) | CLONE - Fix source-level compatibility after HADOOP-11252 |  Blocker | . | Junping Du | Junping Du |
| [YARN-7294](https://issues.apache.org/jira/browse/YARN-7294) | TestSignalContainer#testSignalRequestDeliveryToNM fails intermittently with Fair scheduler |  Major | . | Miklos Szegedi | Miklos Szegedi |
| [YARN-7170](https://issues.apache.org/jira/browse/YARN-7170) | Improve bower dependencies for YARN UI v2 |  Critical | webapp | Sunil Govindan | Sunil Govindan |
| [YARN-7355](https://issues.apache.org/jira/browse/YARN-7355) | TestDistributedShell should be scheduler agnostic |  Major | . | Haibo Chen | Haibo Chen |
| [HDFS-12497](https://issues.apache.org/jira/browse/HDFS-12497) | Re-enable TestDFSStripedOutputStreamWithFailure tests |  Major | erasure-coding | Andrew Wang | Huafeng Wang |
| [HADOOP-14942](https://issues.apache.org/jira/browse/HADOOP-14942) | DistCp#cleanup() should check whether jobFS is null |  Minor | . | Ted Yu | Andras Bokor |
| [YARN-7318](https://issues.apache.org/jira/browse/YARN-7318) | Fix shell check warnings of SLS. |  Major | . | Wangda Tan | Gergely Novák |
| [HDFS-12518](https://issues.apache.org/jira/browse/HDFS-12518) | Re-encryption should handle task cancellation and progress better |  Major | encryption | Xiao Chen | Xiao Chen |
| [HADOOP-14966](https://issues.apache.org/jira/browse/HADOOP-14966) | Handle JDK-8071638 for hadoop-common |  Blocker | . | Bibin A Chundatt | Bibin A Chundatt |
| [HDFS-12249](https://issues.apache.org/jira/browse/HDFS-12249) | dfsadmin -metaSave to output maintenance mode blocks |  Minor | namenode | Wei-Chiu Chuang | Wellington Chevreuil |
| [HDFS-12695](https://issues.apache.org/jira/browse/HDFS-12695) | Add a link to HDFS router federation document in site.xml |  Minor | documentation | Yiqun Lin | Yiqun Lin |
| [YARN-7382](https://issues.apache.org/jira/browse/YARN-7382) | NoSuchElementException in FairScheduler after failover causes RM crash |  Blocker | fairscheduler | Robert Kanter | Robert Kanter |
| [YARN-7385](https://issues.apache.org/jira/browse/YARN-7385) | TestFairScheduler#testUpdateDemand and TestFSLeafQueue#testUpdateDemand are failing with NPE |  Major | test | Robert Kanter | Yufei Gu |
| [HADOOP-14030](https://issues.apache.org/jira/browse/HADOOP-14030) | PreCommit TestKDiag failure |  Major | security | John Zhuge | Wei-Chiu Chuang |
| [HADOOP-14979](https://issues.apache.org/jira/browse/HADOOP-14979) | Upgrade maven-dependency-plugin to 3.0.2 |  Major | build | liyunzhang | liyunzhang |
| [HADOOP-14977](https://issues.apache.org/jira/browse/HADOOP-14977) | Xenial dockerfile needs ant in main build for findbugs |  Trivial | build, test | Allen Wittenauer | Akira Ajisaka |
| [YARN-7339](https://issues.apache.org/jira/browse/YARN-7339) | LocalityMulticastAMRMProxyPolicy should handle cancel request properly |  Minor | . | Botong Huang | Botong Huang |
| [HDFS-9914](https://issues.apache.org/jira/browse/HDFS-9914) | Fix configurable WebhDFS connect/read timeout |  Blocker | hdfs-client, webhdfs | Xiaoyu Yao | Xiaoyu Yao |
| [YARN-7375](https://issues.apache.org/jira/browse/YARN-7375) | Possible NPE in RMWebapp when HA is enabled and the active RM fails |  Major | . | Chandni Singh | Chandni Singh |
| [HDFS-12582](https://issues.apache.org/jira/browse/HDFS-12582) | Replace HdfsFileStatus constructor with a builder pattern. |  Major | . | Bharat Viswanadham | Bharat Viswanadham |
| [HADOOP-14986](https://issues.apache.org/jira/browse/HADOOP-14986) | Enforce JDK limitations |  Major | build | Chris Douglas | Chris Douglas |
| [HADOOP-14991](https://issues.apache.org/jira/browse/HADOOP-14991) | Add missing figures to Resource Estimator tool |  Major | . | Subru Krishnan | Rui Li |
| [YARN-7299](https://issues.apache.org/jira/browse/YARN-7299) | Fix TestDistributedScheduler |  Major | . | Jason Lowe | Arun Suresh |
| [YARN-6747](https://issues.apache.org/jira/browse/YARN-6747) | TestFSAppStarvation.testPreemptionEnable fails intermittently |  Major | . | Sunil Govindan | Miklos Szegedi |
| [YARN-7336](https://issues.apache.org/jira/browse/YARN-7336) | Unsafe cast from long to int Resource.hashCode() method |  Critical | resourcemanager | Daniel Templeton | Miklos Szegedi |
| [YARN-7244](https://issues.apache.org/jira/browse/YARN-7244) | ShuffleHandler is not aware of disks that are added |  Major | . | Kuhu Shukla | Kuhu Shukla |
| [HADOOP-14990](https://issues.apache.org/jira/browse/HADOOP-14990) | Clean up jdiff xml files added for 2.8.2 release |  Blocker | . | Subru Krishnan | Junping Du |
| [HADOOP-14919](https://issues.apache.org/jira/browse/HADOOP-14919) | BZip2 drops records when reading data in splits |  Critical | . | Aki Tanaka | Jason Lowe |
| [HDFS-12699](https://issues.apache.org/jira/browse/HDFS-12699) | TestMountTable fails with Java 7 |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-12714](https://issues.apache.org/jira/browse/HDFS-12714) | Hadoop 3 missing fix for HDFS-5169 |  Major | native | Joe McDonnell | Joe McDonnell |
| [HDFS-12219](https://issues.apache.org/jira/browse/HDFS-12219) | Javadoc for FSNamesystem#getMaxObjects is incorrect |  Trivial | . | Erik Krogen | Erik Krogen |
| [YARN-7412](https://issues.apache.org/jira/browse/YARN-7412) | test\_docker\_util.test\_check\_mount\_permitted() is failing |  Critical | nodemanager | Haibo Chen | Eric Badger |
| [MAPREDUCE-6999](https://issues.apache.org/jira/browse/MAPREDUCE-6999) | Fix typo "onf" in DynamicInputChunk.java |  Trivial | . | fang zhenyi | fang zhenyi |
| [YARN-7364](https://issues.apache.org/jira/browse/YARN-7364) | Queue dash board in new YARN UI has incorrect values |  Critical | webapp | Sunil Govindan | Sunil Govindan |
| [YARN-7370](https://issues.apache.org/jira/browse/YARN-7370) | Preemption properties should be refreshable |  Major | capacity scheduler, scheduler preemption | Eric Payne | Gergely Novák |
| [YARN-7400](https://issues.apache.org/jira/browse/YARN-7400) | incorrect log preview displayed in jobhistory server ui |  Major | yarn | Santhosh B Gowda | Xuan Gong |
| [HADOOP-15013](https://issues.apache.org/jira/browse/HADOOP-15013) | Fix ResourceEstimator findbugs issues |  Blocker | . | Allen Wittenauer | Arun Suresh |
| [YARN-7432](https://issues.apache.org/jira/browse/YARN-7432) | Fix DominantResourceFairnessPolicy serializable findbugs issues |  Blocker | . | Allen Wittenauer | Daniel Templeton |
| [YARN-7434](https://issues.apache.org/jira/browse/YARN-7434) | Router getApps REST invocation fails with multiple RMs |  Critical | . | Subru Krishnan | Íñigo Goiri |
| [HDFS-12725](https://issues.apache.org/jira/browse/HDFS-12725) | BlockPlacementPolicyRackFaultTolerant fails with very uneven racks |  Major | erasure-coding | Xiao Chen | Xiao Chen |
| [YARN-5085](https://issues.apache.org/jira/browse/YARN-5085) | Add support for change of container ExecutionType |  Major | . | Arun Suresh | Arun Suresh |
| [YARN-7428](https://issues.apache.org/jira/browse/YARN-7428) | Add containerId to Localizer failed logs |  Minor | nodemanager | Prabhu Joseph | Prabhu Joseph |
| [YARN-7410](https://issues.apache.org/jira/browse/YARN-7410) | Cleanup FixedValueResource to avoid dependency to ResourceUtils |  Major | resourcemanager | Sunil Govindan | Wangda Tan |
| [YARN-7360](https://issues.apache.org/jira/browse/YARN-7360) | TestRM.testNMTokenSentForNormalContainer() should be scheduler agnostic |  Major | test | Haibo Chen | Haibo Chen |
| [HADOOP-15018](https://issues.apache.org/jira/browse/HADOOP-15018) | Update JAVA\_HOME in create-release for Xenial Dockerfile |  Blocker | build | Andrew Wang | Andrew Wang |
| [HDFS-12788](https://issues.apache.org/jira/browse/HDFS-12788) | Reset the upload button when file upload fails |  Critical | ui, webhdfs | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-7453](https://issues.apache.org/jira/browse/YARN-7453) | Fix issue where RM fails to switch to active after first successful start |  Blocker | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-7458](https://issues.apache.org/jira/browse/YARN-7458) | TestContainerManagerSecurity is still flakey |  Major | test | Robert Kanter | Robert Kanter |
| [HADOOP-15025](https://issues.apache.org/jira/browse/HADOOP-15025) | Ensure singleton for ResourceEstimatorService |  Major | . | Subru Krishnan | Rui Li |
| [HDFS-12732](https://issues.apache.org/jira/browse/HDFS-12732) | Correct spellings of ramdomly to randomly in log. |  Trivial | . | hu xiaodong | hu xiaodong |
| [YARN-7454](https://issues.apache.org/jira/browse/YARN-7454) | RMAppAttemptMetrics#getAggregateResourceUsage can NPE due to double lookup |  Minor | resourcemanager | Jason Lowe | Jason Lowe |
| [YARN-7388](https://issues.apache.org/jira/browse/YARN-7388) | TestAMRestart should be scheduler agnostic |  Major | . | Haibo Chen | Haibo Chen |
| [YARN-7465](https://issues.apache.org/jira/browse/YARN-7465) | start-yarn.sh fails to start ResourceManager unless running as root |  Blocker | . | Sean Mackrory |  |
| [HADOOP-8522](https://issues.apache.org/jira/browse/HADOOP-8522) | ResetableGzipOutputStream creates invalid gzip files when finish() and resetState() are used |  Major | io | Mike Percy | Mike Percy |
| [YARN-7475](https://issues.apache.org/jira/browse/YARN-7475) | Fix Container log link in new YARN UI |  Major | . | Sunil Govindan | Sunil Govindan |
| [HADOOP-15036](https://issues.apache.org/jira/browse/HADOOP-15036) | Update LICENSE.txt for HADOOP-14840 |  Major | . | Arun Suresh | Arun Suresh |
| [YARN-6078](https://issues.apache.org/jira/browse/YARN-6078) | Containers stuck in Localizing state |  Major | . | Jagadish | Billie Rinaldi |
| [YARN-7469](https://issues.apache.org/jira/browse/YARN-7469) | Capacity Scheduler Intra-queue preemption: User can starve if newest app is exactly at user limit |  Major | capacity scheduler, yarn | Eric Payne | Eric Payne |
| [HADOOP-15067](https://issues.apache.org/jira/browse/HADOOP-15067) | GC time percentage reported in JvmMetrics should be a gauge, not counter |  Major | . | Misha Dmitriev | Misha Dmitriev |
| [YARN-7290](https://issues.apache.org/jira/browse/YARN-7290) | Method canContainerBePreempted can return true when it shouldn't |  Major | fairscheduler | Steven Rand | Steven Rand |
| [HDFS-12754](https://issues.apache.org/jira/browse/HDFS-12754) | Lease renewal can hit a deadlock |  Major | . | Kuhu Shukla | Kuhu Shukla |
| [YARN-6647](https://issues.apache.org/jira/browse/YARN-6647) | RM can crash during transitionToStandby due to InterruptedException |  Critical | resourcemanager | Jason Lowe | Bibin A Chundatt |
| [HDFS-11754](https://issues.apache.org/jira/browse/HDFS-11754) | Make FsServerDefaults cache configurable. |  Minor | . | Rushabh S Shah | Mikhail Erofeev |
| [YARN-7509](https://issues.apache.org/jira/browse/YARN-7509) | AsyncScheduleThread and ResourceCommitterService are still running after RM is transitioned to standby |  Critical | . | Tao Yang | Tao Yang |
| [YARN-7589](https://issues.apache.org/jira/browse/YARN-7589) | TestPBImplRecords fails with NullPointerException |  Major | . | Jason Lowe | Daniel Templeton |
| [HADOOP-15058](https://issues.apache.org/jira/browse/HADOOP-15058) | create-release site build outputs dummy shaded jars due to skipShade |  Blocker | . | Andrew Wang | Andrew Wang |
| [HDFS-12889](https://issues.apache.org/jira/browse/HDFS-12889) | Router UI is missing robots.txt file |  Major | . | Bharat Viswanadham | Bharat Viswanadham |
| [HDFS-12872](https://issues.apache.org/jira/browse/HDFS-12872) | EC Checksum broken when BlockAccessToken is enabled |  Critical | erasure-coding | Xiao Chen | Xiao Chen |
| [HDFS-11576](https://issues.apache.org/jira/browse/HDFS-11576) | Block recovery will fail indefinitely if recovery time \> heartbeat interval |  Critical | datanode, hdfs, namenode | Lukas Majercak | Lukas Majercak |
| [YARN-7607](https://issues.apache.org/jira/browse/YARN-7607) | Remove the trailing duplicated timestamp in container diagnostics message |  Minor | nodemanager | Weiwei Yang | Weiwei Yang |
| [HDFS-12840](https://issues.apache.org/jira/browse/HDFS-12840) | Creating a file with non-default EC policy in a EC zone is not correctly serialized in the editlog |  Blocker | erasure-coding | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HADOOP-15080](https://issues.apache.org/jira/browse/HADOOP-15080) | Aliyun OSS: update oss sdk from 2.8.1 to 2.8.3 to remove its dependency on Cat-x "json-lib" |  Blocker | fs/oss | Chris Douglas | Sammi Chen |
| [HADOOP-15059](https://issues.apache.org/jira/browse/HADOOP-15059) | 3.0 deployment cannot work with old version MR tar ball which breaks rolling upgrade |  Blocker | security | Junping Du | Jason Lowe |
| [YARN-7591](https://issues.apache.org/jira/browse/YARN-7591) | NPE in async-scheduling mode of CapacityScheduler |  Critical | capacityscheduler | Tao Yang | Tao Yang |
| [YARN-7381](https://issues.apache.org/jira/browse/YARN-7381) | Enable the configuration: yarn.nodemanager.log-container-debug-info.enabled by default in yarn-default.xml |  Critical | . | Xuan Gong | Xuan Gong |


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
| [YARN-5216](https://issues.apache.org/jira/browse/YARN-5216) | Expose configurable preemption policy for OPPORTUNISTIC containers running on the NM |  Major | distributed-scheduling | Arun Suresh | Hitesh Sharma |
| [YARN-5951](https://issues.apache.org/jira/browse/YARN-5951) | Changes to allow CapacityScheduler to use configuration store |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-5946](https://issues.apache.org/jira/browse/YARN-5946) | Create YarnConfigurationStore interface and InMemoryConfigurationStore class |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-6232](https://issues.apache.org/jira/browse/YARN-6232) | Update resource usage and preempted resource calculations to take into account all resource types |  Major | resourcemanager | Varun Vasudev | Varun Vasudev |
| [YARN-5948](https://issues.apache.org/jira/browse/YARN-5948) | Implement MutableConfigurationManager for handling storage into configuration store |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-5952](https://issues.apache.org/jira/browse/YARN-5952) | Create REST API for changing YARN scheduler configurations |  Major | . | Jonathan Hung | Jonathan Hung |
| [HDFS-10882](https://issues.apache.org/jira/browse/HDFS-10882) | Federation State Store Interface API |  Major | fs | Jason Kace | Jason Kace |
| [YARN-6445](https://issues.apache.org/jira/browse/YARN-6445) | [YARN-3926] Performance improvements in resource profile branch with respect to SLS |  Major | nodemanager, resourcemanager | Varun Vasudev | Varun Vasudev |
| [HDFS-10630](https://issues.apache.org/jira/browse/HDFS-10630) | Federation State Store FS Implementation |  Major | hdfs | Íñigo Goiri | Jason Kace |
| [YARN-5949](https://issues.apache.org/jira/browse/YARN-5949) | Add pluggable configuration ACL policy interface and implementation |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-6575](https://issues.apache.org/jira/browse/YARN-6575) | Support global configuration mutation in MutableConfProvider |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-5953](https://issues.apache.org/jira/browse/YARN-5953) | Create CLI for changing YARN configurations |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-6761](https://issues.apache.org/jira/browse/YARN-6761) | Fix build for YARN-3926 branch |  Major | nodemanager, resourcemanager | Varun Vasudev | Varun Vasudev |
| [YARN-6786](https://issues.apache.org/jira/browse/YARN-6786) | ResourcePBImpl imports cleanup |  Trivial | resourcemanager | Daniel Templeton | Yeliang Cang |
| [YARN-5292](https://issues.apache.org/jira/browse/YARN-5292) | NM Container lifecycle and state transitions to support for PAUSED container state. |  Major | . | Hitesh Sharma | Hitesh Sharma |
| [HDFS-12223](https://issues.apache.org/jira/browse/HDFS-12223) | Rebasing HDFS-10467 |  Major | fs | Íñigo Goiri | Íñigo Goiri |
| [HDFS-10687](https://issues.apache.org/jira/browse/HDFS-10687) | Federation Membership State Store internal API |  Major | hdfs | Íñigo Goiri | Jason Kace |
| [YARN-5947](https://issues.apache.org/jira/browse/YARN-5947) | Create LeveldbConfigurationStore class using Leveldb as backing store |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-6322](https://issues.apache.org/jira/browse/YARN-6322) | Disable queue refresh when configuration mutation is enabled |  Major | . | Jonathan Hung | Jonathan Hung |
| [HDFS-11826](https://issues.apache.org/jira/browse/HDFS-11826) | Federation Namenode Heartbeat |  Major | fs | Íñigo Goiri | Íñigo Goiri |
| [YARN-6788](https://issues.apache.org/jira/browse/YARN-6788) | Improve performance of resource profile branch |  Blocker | nodemanager, resourcemanager | Sunil Govindan | Sunil Govindan |
| [HDFS-10880](https://issues.apache.org/jira/browse/HDFS-10880) | Federation Mount Table State Store internal API |  Major | fs | Jason Kace | Íñigo Goiri |
| [HDFS-10646](https://issues.apache.org/jira/browse/HDFS-10646) | Federation admin tool |  Major | fs | Íñigo Goiri | Íñigo Goiri |
| [YARN-6994](https://issues.apache.org/jira/browse/YARN-6994) | Remove last uses of Long from resource types code |  Minor | resourcemanager | Daniel Templeton | Daniel Templeton |
| [YARN-6892](https://issues.apache.org/jira/browse/YARN-6892) | Improve API implementation in Resources and DominantResourceCalculator class |  Major | nodemanager, resourcemanager | Sunil Govindan | Sunil Govindan |
| [HDFS-11554](https://issues.apache.org/jira/browse/HDFS-11554) | [Documentation] Router-based federation documentation |  Minor | fs | Íñigo Goiri | Íñigo Goiri |
| [HDFS-12312](https://issues.apache.org/jira/browse/HDFS-12312) | Rebasing HDFS-10467 (2) |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-6610](https://issues.apache.org/jira/browse/YARN-6610) | DominantResourceCalculator#getResourceAsValue dominant param is updated to handle multiple resources |  Critical | resourcemanager | Daniel Templeton | Daniel Templeton |
| [YARN-7030](https://issues.apache.org/jira/browse/YARN-7030) | Performance optimizations in Resource and ResourceUtils class |  Critical | nodemanager, resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-7042](https://issues.apache.org/jira/browse/YARN-7042) | Clean up unit tests after YARN-6610 |  Major | test | Daniel Templeton | Daniel Templeton |
| [YARN-6789](https://issues.apache.org/jira/browse/YARN-6789) | Add Client API to get all supported resource types from RM |  Major | nodemanager, resourcemanager | Sunil Govindan | Sunil Govindan |
| [YARN-6781](https://issues.apache.org/jira/browse/YARN-6781) | ResourceUtils#initializeResourcesMap takes an unnecessary Map parameter |  Minor | resourcemanager | Daniel Templeton | Yu-Tang Lin |
| [HDFS-10631](https://issues.apache.org/jira/browse/HDFS-10631) | Federation State Store ZooKeeper implementation |  Major | fs | Íñigo Goiri | Jason Kace |
| [YARN-7067](https://issues.apache.org/jira/browse/YARN-7067) | Optimize ResourceType information display in UI |  Critical | nodemanager, resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-7039](https://issues.apache.org/jira/browse/YARN-7039) | Fix javac and javadoc errors in YARN-3926 branch |  Major | nodemanager, resourcemanager | Sunil Govindan | Sunil Govindan |
| [YARN-7024](https://issues.apache.org/jira/browse/YARN-7024) | Fix issues on recovery in LevelDB store |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-7093](https://issues.apache.org/jira/browse/YARN-7093) | Improve log message in ResourceUtils |  Trivial | nodemanager, resourcemanager | Sunil Govindan | Sunil Govindan |
| [YARN-6933](https://issues.apache.org/jira/browse/YARN-6933) | ResourceUtils.DISALLOWED\_NAMES check is duplicated |  Major | resourcemanager | Daniel Templeton | Manikandan R |
| [YARN-5328](https://issues.apache.org/jira/browse/YARN-5328) | Plan/ResourceAllocation data structure enhancements required to support recurring reservations in ReservationSystem |  Major | resourcemanager | Subru Krishnan | Subru Krishnan |
| [HDFS-12384](https://issues.apache.org/jira/browse/HDFS-12384) | Fixing compilation issue with BanDuplicateClasses |  Major | fs | Íñigo Goiri | Íñigo Goiri |
| [HDFS-12335](https://issues.apache.org/jira/browse/HDFS-12335) | Federation Metrics |  Major | fs | Giovanni Matteo Fumarola | Íñigo Goiri |
| [YARN-5330](https://issues.apache.org/jira/browse/YARN-5330) | SharingPolicy enhancements required to support recurring reservations in ReservationSystem |  Major | resourcemanager | Subru Krishnan | Carlo Curino |
| [YARN-7072](https://issues.apache.org/jira/browse/YARN-7072) | Add a new log aggregation file format controller |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-7136](https://issues.apache.org/jira/browse/YARN-7136) | Additional Performance Improvement for Resource Profile Feature |  Critical | nodemanager, resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-7137](https://issues.apache.org/jira/browse/YARN-7137) | Move newly added APIs to unstable in YARN-3926 branch |  Blocker | nodemanager, resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-6059](https://issues.apache.org/jira/browse/YARN-6059) | Update paused container state in the NM state store |  Blocker | . | Hitesh Sharma | Hitesh Sharma |
| [HDFS-12430](https://issues.apache.org/jira/browse/HDFS-12430) | Rebasing HDFS-10467 After HDFS-12269 and HDFS-12218 |  Major | fs | Íñigo Goiri | Íñigo Goiri |
| [YARN-7194](https://issues.apache.org/jira/browse/YARN-7194) | Log aggregation status is always Failed with the newly added log aggregation IndexedFileFormat |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-6612](https://issues.apache.org/jira/browse/YARN-6612) | Update fair scheduler policies to be aware of resource types |  Major | fairscheduler | Daniel Templeton | Daniel Templeton |
| [HDFS-12450](https://issues.apache.org/jira/browse/HDFS-12450) | Fixing TestNamenodeHeartbeat and support non-HA |  Major | fs | Íñigo Goiri | Íñigo Goiri |
| [YARN-6840](https://issues.apache.org/jira/browse/YARN-6840) | Implement zookeeper based store for scheduler configuration updates |  Major | . | Wangda Tan | Jonathan Hung |
| [YARN-7046](https://issues.apache.org/jira/browse/YARN-7046) | Add closing logic to configuration store |  Major | . | Jonathan Hung | Jonathan Hung |
| [HDFS-12381](https://issues.apache.org/jira/browse/HDFS-12381) | [Documentation] Adding configuration keys for the Router |  Minor | fs | Íñigo Goiri | Íñigo Goiri |
| [YARN-7238](https://issues.apache.org/jira/browse/YARN-7238) | Documentation for API based scheduler configuration management |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-7252](https://issues.apache.org/jira/browse/YARN-7252) | Removing queue then failing over results in exception |  Critical | . | Jonathan Hung | Jonathan Hung |
| [YARN-7251](https://issues.apache.org/jira/browse/YARN-7251) | Misc changes to YARN-5734 |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-7250](https://issues.apache.org/jira/browse/YARN-7250) | Update Shared cache client api to use URLs |  Minor | . | Chris Trezzo | Chris Trezzo |
| [YARN-6509](https://issues.apache.org/jira/browse/YARN-6509) | Add a size threshold beyond which yarn logs will require a force option |  Major | . | Siddharth Seth | Xuan Gong |
| [YARN-7259](https://issues.apache.org/jira/browse/YARN-7259) | Add size-based rolling policy to LogAggregationIndexedFileController |  Major | . | Xuan Gong | Xuan Gong |
| [MAPREDUCE-6971](https://issues.apache.org/jira/browse/MAPREDUCE-6971) | Moving logging APIs over to slf4j in hadoop-mapreduce-client-app |  Major | . | Jinjiang Ling | Jinjiang Ling |
| [YARN-6550](https://issues.apache.org/jira/browse/YARN-6550) | Capture launch\_container.sh logs to a separate log file |  Major | . | Wangda Tan | Suma Shivaprasad |
| [HDFS-12580](https://issues.apache.org/jira/browse/HDFS-12580) | Rebasing HDFS-10467 after HDFS-12447 |  Major | fs | Íñigo Goiri | Íñigo Goiri |
| [YARN-6916](https://issues.apache.org/jira/browse/YARN-6916) | Moving logging APIs over to slf4j in hadoop-yarn-server-common |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [YARN-7219](https://issues.apache.org/jira/browse/YARN-7219) | Make AllocateRequestProto compatible with branch-2/branch-2.8 |  Critical | yarn | Ray Chiang | Ray Chiang |
| [YARN-6975](https://issues.apache.org/jira/browse/YARN-6975) | Moving logging APIs over to slf4j in hadoop-yarn-server-tests, hadoop-yarn-server-web-proxy and hadoop-yarn-server-router |  Major | . | Yeliang Cang | Yeliang Cang |
| [YARN-2037](https://issues.apache.org/jira/browse/YARN-2037) | Add work preserving restart support for Unmanaged AMs |  Major | resourcemanager | Karthik Kambatla | Botong Huang |
| [YARN-5329](https://issues.apache.org/jira/browse/YARN-5329) | Placement Agent enhancements required to support recurring reservations in ReservationSystem |  Blocker | resourcemanager | Subru Krishnan | Carlo Curino |
| [YARN-6182](https://issues.apache.org/jira/browse/YARN-6182) | Fix alignment issues and missing information in new YARN UI's Queue page |  Major | yarn-ui-v2 | Akhil PB | Akhil PB |
| [YARN-2960](https://issues.apache.org/jira/browse/YARN-2960) | Add documentation for the YARN shared cache |  Major | . | Chris Trezzo | Chris Trezzo |
| [HDFS-12273](https://issues.apache.org/jira/browse/HDFS-12273) | Federation UI |  Major | fs | Íñigo Goiri | Íñigo Goiri |
| [HDFS-12577](https://issues.apache.org/jira/browse/HDFS-12577) | Rename Router tooling |  Major | fs | Íñigo Goiri | Íñigo Goiri |
| [HDFS-12541](https://issues.apache.org/jira/browse/HDFS-12541) | Extend TestSafeModeWithStripedFile with a random EC policy |  Major | erasure-coding, test | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-7304](https://issues.apache.org/jira/browse/YARN-7304) | Merge YARN-5734 branch to branch-3.0 |  Major | . | Xuan Gong | Xuan Gong |
| [HDFS-12547](https://issues.apache.org/jira/browse/HDFS-12547) | Extend TestQuotaWithStripedBlocks with a random EC policy |  Major | erasure-coding, test | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-7205](https://issues.apache.org/jira/browse/YARN-7205) | Log improvements for the ResourceUtils |  Major | nodemanager, resourcemanager | Jian He | Sunil Govindan |
| [HDFS-12637](https://issues.apache.org/jira/browse/HDFS-12637) | Extend TestDistributedFileSystemWithECFile with a random EC policy |  Major | erasure-coding, test | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-7275](https://issues.apache.org/jira/browse/YARN-7275) | NM Statestore cleanup for Container updates |  Blocker | . | Arun Suresh | kartheek muthyala |
| [YARN-7311](https://issues.apache.org/jira/browse/YARN-7311) | Fix TestRMWebServicesReservation parametrization for fair scheduler |  Blocker | fairscheduler, reservation system | Yufei Gu | Yufei Gu |
| [YARN-6546](https://issues.apache.org/jira/browse/YARN-6546) | SLS is slow while loading 10k queues |  Major | scheduler-load-simulator | Yufei Gu | Yufei Gu |
| [YARN-7345](https://issues.apache.org/jira/browse/YARN-7345) | GPU Isolation: Incorrect minor device numbers written to devices.deny file |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-7338](https://issues.apache.org/jira/browse/YARN-7338) | Support same origin policy for cross site scripting prevention. |  Major | yarn-ui-v2 | Vrushali C | Sunil Govindan |
| [HDFS-12620](https://issues.apache.org/jira/browse/HDFS-12620) | Backporting HDFS-10467 to branch-2 |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-4090](https://issues.apache.org/jira/browse/YARN-4090) | Make Collections.sort() more efficient by caching resource usage |  Major | fairscheduler | Xianyin Xin | Yufei Gu |
| [YARN-7353](https://issues.apache.org/jira/browse/YARN-7353) | Docker permitted volumes don't properly check for directories |  Major | yarn | Eric Badger | Eric Badger |
| [YARN-6984](https://issues.apache.org/jira/browse/YARN-6984) | DominantResourceCalculator.isAnyMajorResourceZero() should test all resources |  Major | scheduler | Daniel Templeton | Sunil Govindan |
| [YARN-3661](https://issues.apache.org/jira/browse/YARN-3661) | Basic Federation UI |  Major | nodemanager, resourcemanager | Giovanni Matteo Fumarola | Íñigo Goiri |
| [YARN-4827](https://issues.apache.org/jira/browse/YARN-4827) | Document configuration of ReservationSystem for FairScheduler |  Blocker | capacity scheduler | Subru Krishnan | Yufei Gu |
| [YARN-5516](https://issues.apache.org/jira/browse/YARN-5516) | Add REST API for supporting recurring reservations |  Major | resourcemanager | Sangeetha Abdu Jyothi | Sean Po |
| [YARN-6505](https://issues.apache.org/jira/browse/YARN-6505) | Define the strings used in SLS JSON input file format |  Major | scheduler-load-simulator | Yufei Gu | Gergely Novák |
| [YARN-7178](https://issues.apache.org/jira/browse/YARN-7178) | Add documentation for Container Update API |  Blocker | . | Arun Suresh | Arun Suresh |
| [YARN-7374](https://issues.apache.org/jira/browse/YARN-7374) | Improve performance of DRF comparisons for resource types in fair scheduler |  Critical | fairscheduler | Daniel Templeton | Daniel Templeton |
| [YARN-6927](https://issues.apache.org/jira/browse/YARN-6927) | Add support for individual resource types requests in MapReduce |  Major | resourcemanager | Daniel Templeton | Gergo Repas |
| [YARN-7407](https://issues.apache.org/jira/browse/YARN-7407) | Moving logging APIs over to slf4j in hadoop-yarn-applications |  Major | . | Yeliang Cang | Yeliang Cang |
| [YARN-7379](https://issues.apache.org/jira/browse/YARN-7379) | Moving logging APIs over to slf4j in hadoop-yarn-client |  Major | . | Yeliang Cang | Yeliang Cang |
| [YARN-7276](https://issues.apache.org/jira/browse/YARN-7276) | Federation Router Web Service fixes |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HADOOP-14997](https://issues.apache.org/jira/browse/HADOOP-14997) |  Add hadoop-aliyun as dependency of hadoop-cloud-storage |  Minor | fs/oss | Genmao Yu | Genmao Yu |
| [YARN-7289](https://issues.apache.org/jira/browse/YARN-7289) | Application lifetime does not work with FairScheduler |  Major | resourcemanager | Miklos Szegedi | Miklos Szegedi |
| [YARN-7286](https://issues.apache.org/jira/browse/YARN-7286) | Add support for docker to have no capabilities |  Major | yarn | Eric Badger | Eric Badger |
| [HDFS-11467](https://issues.apache.org/jira/browse/HDFS-11467) | Support ErasureCoding section in OIV XML/ReverseXML |  Blocker | tools | Wei-Chiu Chuang | Huafeng Wang |
| [YARN-7307](https://issues.apache.org/jira/browse/YARN-7307) | Allow client/AM update supported resource types via YARN APIs |  Blocker | nodemanager, resourcemanager | Wangda Tan | Sunil Govindan |
| [MAPREDUCE-6997](https://issues.apache.org/jira/browse/MAPREDUCE-6997) | Moving logging APIs over to slf4j in hadoop-mapreduce-client-hs |  Major | . | Akira Ajisaka | Gergely Novák |
| [MAPREDUCE-7001](https://issues.apache.org/jira/browse/MAPREDUCE-7001) | Moving logging APIs over to slf4j in hadoop-mapreduce-client-shuffle |  Trivial | . | Jinjiang Ling | Jinjiang Ling |
| [YARN-7166](https://issues.apache.org/jira/browse/YARN-7166) | Container REST endpoints should report resource types |  Major | resourcemanager | Daniel Templeton | Daniel Templeton |
| [YARN-7143](https://issues.apache.org/jira/browse/YARN-7143) | FileNotFound handling in ResourceUtils is inconsistent |  Major | resourcemanager | Daniel Templeton | Daniel Templeton |
| [YARN-7437](https://issues.apache.org/jira/browse/YARN-7437) | Rename PlacementSet and SchedulingPlacementSet |  Major | . | Wangda Tan | Wangda Tan |
| [YARN-6909](https://issues.apache.org/jira/browse/YARN-6909) | Use LightWeightedResource when number of resource types more than two |  Critical | resourcemanager | Daniel Templeton | Sunil Govindan |
| [YARN-7406](https://issues.apache.org/jira/browse/YARN-7406) | Moving logging APIs over to slf4j in hadoop-yarn-api |  Major | . | Yeliang Cang | Yeliang Cang |
| [YARN-7442](https://issues.apache.org/jira/browse/YARN-7442) | [YARN-7069] Limit format of resource type name |  Blocker | nodemanager, resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-7369](https://issues.apache.org/jira/browse/YARN-7369) | Improve the resource types docs |  Major | docs | Daniel Templeton | Daniel Templeton |
| [HADOOP-14993](https://issues.apache.org/jira/browse/HADOOP-14993) | AliyunOSS: Override listFiles and listLocatedStatus |  Major | fs/oss | Genmao Yu | Genmao Yu |
| [YARN-7430](https://issues.apache.org/jira/browse/YARN-7430) | Enable user re-mapping for Docker containers by default |  Blocker | security, yarn | Eric Yang | Eric Yang |
| [HADOOP-15024](https://issues.apache.org/jira/browse/HADOOP-15024) | AliyunOSS: support user agent configuration and include that & Hadoop version information to oss server |  Major | fs, fs/oss | Sammi Chen | Sammi Chen |
| [YARN-7541](https://issues.apache.org/jira/browse/YARN-7541) | Node updates don't update the maximum cluster capability for resources other than CPU and memory |  Critical | resourcemanager | Daniel Templeton | Daniel Templeton |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-7317](https://issues.apache.org/jira/browse/YARN-7317) | Fix overallocation resulted from ceiling in LocalityMulticastAMRMProxyPolicy |  Minor | . | Botong Huang | Botong Huang |
| [HDFS-12847](https://issues.apache.org/jira/browse/HDFS-12847) | Regenerate editsStored and editsStored.xml in HDFS tests |  Major | test | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HADOOP-13493](https://issues.apache.org/jira/browse/HADOOP-13493) | Compatibility Docs should clarify the policy for what takes precedence when a conflict is found |  Critical | documentation | Robert Kanter | Daniel Templeton |


