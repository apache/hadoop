
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

## Release 3.0.0-beta1 - 2017-10-03

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-14539](https://issues.apache.org/jira/browse/HADOOP-14539) | Move commons logging APIs over to slf4j in hadoop-common |  Major | . | Akira Ajisaka | Wenxin He |
| [HDFS-12206](https://issues.apache.org/jira/browse/HDFS-12206) | Rename the split EC / replicated block metrics |  Major | metrics | Andrew Wang | Andrew Wang |
| [HADOOP-13595](https://issues.apache.org/jira/browse/HADOOP-13595) | Rework hadoop\_usage to be broken up by clients/daemons/etc. |  Blocker | scripts | Allen Wittenauer | Allen Wittenauer |
| [HDFS-6984](https://issues.apache.org/jira/browse/HDFS-6984) | Serialize FileStatus via protobuf |  Major | . | Colin P. McCabe | Chris Douglas |
| [YARN-6961](https://issues.apache.org/jira/browse/YARN-6961) | Remove commons-logging dependency from hadoop-yarn-server-applicationhistoryservice module |  Minor | build | Akira Ajisaka | Yeliang Cang |
| [HDFS-11957](https://issues.apache.org/jira/browse/HDFS-11957) | Enable POSIX ACL inheritance by default |  Major | security | John Zhuge | John Zhuge |
| [MAPREDUCE-6870](https://issues.apache.org/jira/browse/MAPREDUCE-6870) | Add configuration for MR job to finish when all reducers are complete (even with unfinished mappers) |  Major | . | Zhe Zhang | Peter Bacsko |
| [HADOOP-14260](https://issues.apache.org/jira/browse/HADOOP-14260) | Configuration.dumpConfiguration should redact sensitive information |  Major | conf, security | Vihang Karajgaonkar | John Zhuge |
| [HADOOP-14726](https://issues.apache.org/jira/browse/HADOOP-14726) | Mark FileStatus::isDir as final |  Minor | fs | Chris Douglas | Chris Douglas |
| [HDFS-12303](https://issues.apache.org/jira/browse/HDFS-12303) | Change default EC cell size to 1MB for better performance |  Blocker | . | Wei Zhou | Wei Zhou |
| [HDFS-12258](https://issues.apache.org/jira/browse/HDFS-12258) | ec -listPolicies should list all policies in system, no matter it's enabled or disabled |  Major | . | Sammi Chen | Wei Zhou |
| [MAPREDUCE-6892](https://issues.apache.org/jira/browse/MAPREDUCE-6892) | Issues with the count of failed/killed tasks in the jhist file |  Major | client, jobhistoryserver | Peter Bacsko | Peter Bacsko |
| [HADOOP-14414](https://issues.apache.org/jira/browse/HADOOP-14414) | Calling maven-site-plugin directly for docs profile is unnecessary |  Minor | . | Andras Bokor | Andras Bokor |
| [HDFS-12218](https://issues.apache.org/jira/browse/HDFS-12218) | Rename split EC / replicated block metrics in BlockManager |  Blocker | erasure-coding, metrics | Andrew Wang | Andrew Wang |
| [HADOOP-14847](https://issues.apache.org/jira/browse/HADOOP-14847) | Remove Guava Supplier and change to java Supplier in AMRMClient and AMRMClientAysnc |  Blocker | . | Bharat Viswanadham | Bharat Viswanadham |
| [HDFS-12414](https://issues.apache.org/jira/browse/HDFS-12414) | Ensure to use CLI command to enable/disable erasure coding policy |  Major | . | Sammi Chen | Sammi Chen |
| [HDFS-12438](https://issues.apache.org/jira/browse/HDFS-12438) | Rename dfs.datanode.ec.reconstruction.stripedblock.threads.size to dfs.datanode.ec.reconstruction.threads |  Major | . | Andrew Wang | Andrew Wang |
| [HADOOP-14738](https://issues.apache.org/jira/browse/HADOOP-14738) | Remove S3N and obsolete bits of S3A; rework docs |  Blocker | fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-7859](https://issues.apache.org/jira/browse/HDFS-7859) | Erasure Coding: Persist erasure coding policies in NameNode |  Major | . | Kai Zheng | Sammi Chen |
| [HDFS-12395](https://issues.apache.org/jira/browse/HDFS-12395) | Support erasure coding policy operations in namenode edit log |  Major | erasure-coding | Sammi Chen | Sammi Chen |
| [HADOOP-14670](https://issues.apache.org/jira/browse/HADOOP-14670) | Increase minimum cmake version for all platforms |  Major | build | Allen Wittenauer | Allen Wittenauer |
| [HDFS-12447](https://issues.apache.org/jira/browse/HDFS-12447) | Rename AddECPolicyResponse to AddErasureCodingPolicyResponse |  Major | . | Sammi Chen | Sammi Chen |
| [HDFS-7337](https://issues.apache.org/jira/browse/HDFS-7337) | Configurable and pluggable erasure codec and policy |  Critical | erasure-coding | Zhe Zhang | Sammi Chen |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-6959](https://issues.apache.org/jira/browse/YARN-6959) | RM may allocate wrong AM Container for new attempt |  Major | capacity scheduler, fairscheduler, scheduler | Yuqi Wang | Yuqi Wang |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-4161](https://issues.apache.org/jira/browse/YARN-4161) | Capacity Scheduler : Assign single or multiple containers per heart beat driven by configuration |  Major | capacity scheduler | Mayank Bansal | Wei Yan |
| [HADOOP-14560](https://issues.apache.org/jira/browse/HADOOP-14560) | Make HttpServer2 backlog size configurable |  Major | common | Alexander Krasheninnikov | Alexander Krasheninnikov |
| [HDFS-10899](https://issues.apache.org/jira/browse/HDFS-10899) | Add functionality to re-encrypt EDEKs |  Major | encryption, kms | Xiao Chen | Xiao Chen |
| [YARN-5355](https://issues.apache.org/jira/browse/YARN-5355) | YARN Timeline Service v.2: alpha 2 |  Critical | timelineserver | Sangjin Lee | Vrushali C |
| [HADOOP-13345](https://issues.apache.org/jira/browse/HADOOP-13345) | S3Guard: Improved Consistency for S3A |  Major | fs/s3 | Chris Nauroth | Chris Nauroth |
| [HADOOP-12077](https://issues.apache.org/jira/browse/HADOOP-12077) | Provide a multi-URI replication Inode for ViewFs |  Major | fs | Gera Shegalov | Gera Shegalov |
| [HDFS-7877](https://issues.apache.org/jira/browse/HDFS-7877) | [Umbrella] Support maintenance state for datanodes |  Major | datanode, namenode | Ming Ma | Ming Ma |
| [YARN-2915](https://issues.apache.org/jira/browse/YARN-2915) | Enable YARN RM scale out via federation using multiple RM's |  Major | nodemanager, resourcemanager | Sriram Rao | Subru Krishnan |
| [MAPREDUCE-6732](https://issues.apache.org/jira/browse/MAPREDUCE-6732) | mapreduce tasks for YARN Timeline Service v.2: alpha 2 |  Major | . | Sangjin Lee | Vrushali C |
| [YARN-5220](https://issues.apache.org/jira/browse/YARN-5220) | Scheduling of OPPORTUNISTIC containers through YARN RM |  Major | resourcemanager | Konstantinos Karanasos | Konstantinos Karanasos |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-12042](https://issues.apache.org/jira/browse/HDFS-12042) | Lazy initialize AbstractINodeDiffList#diffs for snapshots to reduce memory consumption |  Major | . | Misha Dmitriev | Misha Dmitriev |
| [HDFS-12078](https://issues.apache.org/jira/browse/HDFS-12078) | Add time unit to the description of property dfs.namenode.stale.datanode.interval in hdfs-default.xml |  Minor | documentation, hdfs | Weiwei Yang | Weiwei Yang |
| [YARN-6752](https://issues.apache.org/jira/browse/YARN-6752) | Display reserved resources in web UI per application |  Major | fairscheduler | Abdullah Yousufi | Abdullah Yousufi |
| [YARN-6746](https://issues.apache.org/jira/browse/YARN-6746) | SchedulerUtils.checkResourceRequestMatchingNodePartition() is dead code |  Minor | scheduler | Daniel Templeton | Deepti Sawhney |
| [YARN-6410](https://issues.apache.org/jira/browse/YARN-6410) | FSContext.scheduler should be final |  Minor | fairscheduler | Daniel Templeton | Yeliang Cang |
| [YARN-6764](https://issues.apache.org/jira/browse/YARN-6764) | Simplify the logic in FairScheduler#attemptScheduling |  Trivial | fairscheduler | Yufei Gu | Yufei Gu |
| [HADOOP-14443](https://issues.apache.org/jira/browse/HADOOP-14443) | Azure: Support retry and client side failover for authorization, SASKey and delegation token generation |  Major | fs/azure | Santhosh G Nayak | Santhosh G Nayak |
| [HADOOP-14535](https://issues.apache.org/jira/browse/HADOOP-14535) | wasb: implement high-performance random access and seek of block blobs |  Major | fs/azure | Thomas Marquardt | Thomas Marquardt |
| [HADOOP-14629](https://issues.apache.org/jira/browse/HADOOP-14629) | Improve exception checking in FileContext related JUnit tests |  Major | fs, test | Andras Bokor | Andras Bokor |
| [YARN-6689](https://issues.apache.org/jira/browse/YARN-6689) | PlacementRule should be configurable |  Major | . | Jonathan Hung | Jonathan Hung |
| [HDFS-12130](https://issues.apache.org/jira/browse/HDFS-12130) | Optimizing permission check for getContentSummary |  Major | namenode | Chen Liang | Chen Liang |
| [HDFS-12137](https://issues.apache.org/jira/browse/HDFS-12137) | DN dataset lock should be fair |  Critical | datanode | Daryn Sharp | Daryn Sharp |
| [HDFS-11786](https://issues.apache.org/jira/browse/HDFS-11786) | Add support to make copyFromLocal multi threaded |  Major | hdfs | Mukul Kumar Singh | Mukul Kumar Singh |
| [HDFS-12138](https://issues.apache.org/jira/browse/HDFS-12138) | Remove redundant 'public' modifiers from BlockCollection |  Trivial | namenode | Chen Liang | Chen Liang |
| [HADOOP-14640](https://issues.apache.org/jira/browse/HADOOP-14640) | Azure: Support affinity for service running on localhost and reuse SPNEGO hadoop.auth cookie for authorization, SASKey and delegation token generation |  Major | fs/azure | Santhosh G Nayak | Santhosh G Nayak |
| [YARN-6778](https://issues.apache.org/jira/browse/YARN-6778) | In ResourceWeights, weights and setWeights() should be final |  Minor | scheduler | Daniel Templeton | Daniel Templeton |
| [HDFS-12067](https://issues.apache.org/jira/browse/HDFS-12067) | Correct dfsadmin commands usage message to reflects IPC port |  Major | . | steven-wugang | steven-wugang |
| [HADOOP-14666](https://issues.apache.org/jira/browse/HADOOP-14666) | Tests use assertTrue(....equals(...)) instead of assertEquals() |  Minor | test | Daniel Templeton | Daniel Templeton |
| [HADOOP-14659](https://issues.apache.org/jira/browse/HADOOP-14659) | UGI getShortUserName does not need to search the Subject |  Major | common | Daryn Sharp | Daryn Sharp |
| [HADOOP-14557](https://issues.apache.org/jira/browse/HADOOP-14557) | Document  HADOOP-8143  (Change distcp to have -pb on by default) |  Trivial | . | Wei-Chiu Chuang | Bharat Viswanadham |
| [YARN-6768](https://issues.apache.org/jira/browse/YARN-6768) | Improve performance of yarn api record toString and fromString |  Major | . | Jonathan Eagles | Jonathan Eagles |
| [YARN-6779](https://issues.apache.org/jira/browse/YARN-6779) | DominantResourceFairnessPolicy.DominantResourceFairnessComparator.calculateShares() should be @VisibleForTesting |  Trivial | fairscheduler | Daniel Templeton | Yeliang Cang |
| [YARN-6845](https://issues.apache.org/jira/browse/YARN-6845) | Variable scheduler of FSLeafQueue duplicates the one of its parent FSQueue. |  Trivial | fairscheduler | Yufei Gu | Yufei Gu |
| [HADOOP-14597](https://issues.apache.org/jira/browse/HADOOP-14597) | Native compilation broken with OpenSSL-1.1.0 because EVP\_CIPHER\_CTX has been made opaque |  Major | . | Ravi Prakash | Ravi Prakash |
| [HDFS-12193](https://issues.apache.org/jira/browse/HDFS-12193) | Fix style issues in HttpFS tests |  Trivial | httpfs | Zoran Dimitrijevic | Zoran Dimitrijevic |
| [HADOOP-14681](https://issues.apache.org/jira/browse/HADOOP-14681) | Remove MockitoMaker class |  Major | test | Andras Bokor | Andras Bokor |
| [HDFS-12143](https://issues.apache.org/jira/browse/HDFS-12143) | Improve performance of getting and removing inode features |  Major | namenode | Daryn Sharp | Daryn Sharp |
| [HDFS-12171](https://issues.apache.org/jira/browse/HDFS-12171) | Reduce IIP object allocations for inode lookup |  Major | namenode | Daryn Sharp | Daryn Sharp |
| [HDFS-12190](https://issues.apache.org/jira/browse/HDFS-12190) | Enable 'hdfs dfs -stat' to display access time |  Major | hdfs, shell | Yongjun Zhang | Yongjun Zhang |
| [YARN-6864](https://issues.apache.org/jira/browse/YARN-6864) | FSPreemptionThread cleanup for readability |  Minor | fairscheduler | Daniel Templeton | Daniel Templeton |
| [HADOOP-14455](https://issues.apache.org/jira/browse/HADOOP-14455) | ViewFileSystem#rename should support be supported within same nameservice with different mountpoints |  Major | viewfs | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-14690](https://issues.apache.org/jira/browse/HADOOP-14690) | RetryInvocationHandler$RetryInfo should override toString() |  Minor | . | Akira Ajisaka | Yeliang Cang |
| [HADOOP-14709](https://issues.apache.org/jira/browse/HADOOP-14709) | Fix checkstyle warnings in ContractTestUtils |  Minor | test | Steve Loughran | Thomas Marquardt |
| [MAPREDUCE-6914](https://issues.apache.org/jira/browse/MAPREDUCE-6914) | Tests use assertTrue(....equals(...)) instead of assertEquals() |  Minor | test | Daniel Templeton | Daniel Templeton |
| [YARN-6832](https://issues.apache.org/jira/browse/YARN-6832) | Tests use assertTrue(....equals(...)) instead of assertEquals() |  Minor | test | Daniel Templeton | Daniel Templeton |
| [HADOOP-14706](https://issues.apache.org/jira/browse/HADOOP-14706) | Adding a helper method to determine whether a log is Log4j implement |  Minor | util | Wenxin He | Wenxin He |
| [HADOOP-14471](https://issues.apache.org/jira/browse/HADOOP-14471) | Upgrade Jetty to latest 9.3 version |  Major | . | John Zhuge | John Zhuge |
| [HDFS-12251](https://issues.apache.org/jira/browse/HDFS-12251) | Add document for StreamCapabilities |  Major | . | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HDFS-12224](https://issues.apache.org/jira/browse/HDFS-12224) | Add tests to TestJournalNodeSync for sync after JN downtime |  Major | hdfs | Hanisha Koneru | Hanisha Koneru |
| [YARN-6802](https://issues.apache.org/jira/browse/YARN-6802) | Add Max AM Resource and AM Resource Usage to Leaf Queue View in FairScheduler WebUI |  Major | fairscheduler | YunFan Zhou | YunFan Zhou |
| [HDFS-12036](https://issues.apache.org/jira/browse/HDFS-12036) | Add audit log for some erasure coding operations |  Major | namenode | Wei-Chiu Chuang | Huafeng Wang |
| [HDFS-12264](https://issues.apache.org/jira/browse/HDFS-12264) | DataNode uses a deprecated method IoUtils#cleanup. |  Major | . | Ajay Kumar | Ajay Kumar |
| [YARN-6757](https://issues.apache.org/jira/browse/YARN-6757) | Refactor the usage of yarn.nodemanager.linux-container-executor.cgroups.mount-path |  Minor | nodemanager | Miklos Szegedi | Miklos Szegedi |
| [YARN-6811](https://issues.apache.org/jira/browse/YARN-6811) | [ATS1.5]  All history logs should be kept under its own User Directory. |  Major | timelineclient, timelineserver | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-6879](https://issues.apache.org/jira/browse/YARN-6879) | TestLeafQueue.testDRFUserLimits() has commented out code |  Trivial | capacity scheduler, test | Daniel Templeton | Angela Wang |
| [MAPREDUCE-6923](https://issues.apache.org/jira/browse/MAPREDUCE-6923) | Optimize MapReduce Shuffle I/O for small partitions |  Major | . | Robert Schmidtke | Robert Schmidtke |
| [HDFS-12287](https://issues.apache.org/jira/browse/HDFS-12287) | Remove a no-longer applicable TODO comment in DatanodeManager |  Trivial | namenode | Chen Liang | Chen Liang |
| [YARN-6952](https://issues.apache.org/jira/browse/YARN-6952) | Enable scheduling monitor in FS |  Major | fairscheduler, resourcemanager | Yufei Gu | Yufei Gu |
| [YARN-6884](https://issues.apache.org/jira/browse/YARN-6884) | AllocationFileLoaderService.loadQueue() has an if without braces |  Trivial | fairscheduler | Daniel Templeton | weiyuan |
| [YARN-6882](https://issues.apache.org/jira/browse/YARN-6882) | AllocationFileLoaderService.reloadAllocations() should use the diamond operator |  Trivial | fairscheduler | Daniel Templeton | Larry Lo |
| [HADOOP-14627](https://issues.apache.org/jira/browse/HADOOP-14627) | Support MSI and DeviceCode token provider in ADLS |  Major | fs/adl | Atul Sikaria | Atul Sikaria |
| [HDFS-12221](https://issues.apache.org/jira/browse/HDFS-12221) | Replace xerces in XmlEditsVisitor |  Major | . | Lei (Eddy) Xu | Ajay Kumar |
| [HADOOP-14741](https://issues.apache.org/jira/browse/HADOOP-14741) | Refactor curator based ZooKeeper communication into common library |  Major | . | Subru Krishnan | Íñigo Goiri |
| [HDFS-12162](https://issues.apache.org/jira/browse/HDFS-12162) | Update listStatus document to describe the behavior when the argument is a file |  Major | hdfs, httpfs | Yongjun Zhang | Ajay Kumar |
| [YARN-6881](https://issues.apache.org/jira/browse/YARN-6881) | LOG is unused in AllocationConfiguration |  Major | fairscheduler | Daniel Templeton | weiyuan |
| [YARN-6917](https://issues.apache.org/jira/browse/YARN-6917) | Queue path is recomputed from scratch on every allocation |  Minor | capacityscheduler | Jason Lowe | Eric Payne |
| [HADOOP-14673](https://issues.apache.org/jira/browse/HADOOP-14673) | Remove leftover hadoop\_xml\_escape from functions |  Major | scripts | Allen Wittenauer | Ajay Kumar |
| [HADOOP-14662](https://issues.apache.org/jira/browse/HADOOP-14662) | Update azure-storage sdk to version 5.4.0 |  Major | fs/azure | Thomas Marquardt | Thomas Marquardt |
| [HDFS-12301](https://issues.apache.org/jira/browse/HDFS-12301) | NN File Browser UI: Navigate to a path when enter is pressed |  Trivial | ui | Ravi Prakash | Ravi Prakash |
| [HDFS-12269](https://issues.apache.org/jira/browse/HDFS-12269) | Better to return a Map rather than HashMap in getErasureCodingCodecs |  Minor | erasure-coding | Huafeng Wang | Huafeng Wang |
| [YARN-3254](https://issues.apache.org/jira/browse/YARN-3254) | HealthReport should include disk full information |  Major | nodemanager | Akira Ajisaka | Suma Shivaprasad |
| [HDFS-12072](https://issues.apache.org/jira/browse/HDFS-12072) | Provide fairness between EC and non-EC recovery tasks. |  Major | erasure-coding | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HDFS-12117](https://issues.apache.org/jira/browse/HDFS-12117) | HttpFS does not seem to support SNAPSHOT related methods for WebHDFS REST Interface |  Major | httpfs | Wellington Chevreuil | Wellington Chevreuil |
| [HADOOP-14705](https://issues.apache.org/jira/browse/HADOOP-14705) | Add batched interface reencryptEncryptedKeys to KMS |  Major | kms | Xiao Chen | Xiao Chen |
| [YARN-7053](https://issues.apache.org/jira/browse/YARN-7053) | Move curator transaction support to ZKCuratorManager |  Major | . | Jonathan Hung | Jonathan Hung |
| [HADOOP-14251](https://issues.apache.org/jira/browse/HADOOP-14251) | Credential provider should handle property key deprecation |  Critical | security | John Zhuge | John Zhuge |
| [YARN-7049](https://issues.apache.org/jira/browse/YARN-7049) | FSAppAttempt preemption related fields have confusing names |  Major | fairscheduler | Karthik Kambatla | Karthik Kambatla |
| [HDFS-11986](https://issues.apache.org/jira/browse/HDFS-11986) | Dfsadmin should report erasure coding related information separately |  Major | erasure-coding, hdfs | Manoj Govindassamy | Manoj Govindassamy |
| [YARN-6999](https://issues.apache.org/jira/browse/YARN-6999) | Add log about how to solve Error: Could not find or load main class org.apache.hadoop.mapreduce.v2.app.MRAppMaster |  Minor | documentation, security | Linlin Zhou | Linlin Zhou |
| [YARN-7037](https://issues.apache.org/jira/browse/YARN-7037) | Optimize data transfer with zero-copy approach for containerlogs REST API in NMWebServices |  Major | nodemanager | Tao Yang | Tao Yang |
| [HDFS-12356](https://issues.apache.org/jira/browse/HDFS-12356) | Unit test for JournalNode sync during Rolling Upgrade |  Major | ha | Hanisha Koneru | Hanisha Koneru |
| [YARN-6780](https://issues.apache.org/jira/browse/YARN-6780) | ResourceWeights.toString() cleanup |  Minor | scheduler | Daniel Templeton | weiyuan |
| [YARN-6721](https://issues.apache.org/jira/browse/YARN-6721) | container-executor should have stack checking |  Critical | nodemanager, security | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-14781](https://issues.apache.org/jira/browse/HADOOP-14781) | Clarify that HADOOP\_CONF\_DIR shouldn't actually be set in hadoop-env.sh |  Major | documentation, scripts | Allen Wittenauer | Allen Wittenauer |
| [HDFS-12380](https://issues.apache.org/jira/browse/HDFS-12380) | Simplify dataQueue.wait condition logical operation in DataStreamer::run() |  Major | hdfs-client | liaoyuxiangqin | liaoyuxiangqin |
| [HDFS-12300](https://issues.apache.org/jira/browse/HDFS-12300) | Audit-log delegation token related operations |  Major | namenode | Xiao Chen | Xiao Chen |
| [YARN-7022](https://issues.apache.org/jira/browse/YARN-7022) | Improve click interaction in queue topology in new YARN UI |  Major | yarn-ui-v2 | Abdullah Yousufi | Abdullah Yousufi |
| [HDFS-12182](https://issues.apache.org/jira/browse/HDFS-12182) | BlockManager.metaSave does not distinguish between "under replicated" and "missing" blocks |  Trivial | hdfs | Wellington Chevreuil | Wellington Chevreuil |
| [HADOOP-14688](https://issues.apache.org/jira/browse/HADOOP-14688) | Intern strings in KeyVersion and EncryptedKeyVersion |  Major | kms | Xiao Chen | Xiao Chen |
| [HDFS-12377](https://issues.apache.org/jira/browse/HDFS-12377) | Refactor TestReadStripedFileWithDecoding to avoid test timeouts |  Major | erasure-coding | Andrew Wang | Andrew Wang |
| [HADOOP-14827](https://issues.apache.org/jira/browse/HADOOP-14827) | Allow StopWatch to accept a Timer parameter for tests |  Minor | common, test | Erik Krogen | Erik Krogen |
| [HDFS-12131](https://issues.apache.org/jira/browse/HDFS-12131) | Add some of the FSNamesystem JMX values as metrics |  Minor | hdfs, namenode | Erik Krogen | Erik Krogen |
| [HDFS-12402](https://issues.apache.org/jira/browse/HDFS-12402) | Refactor ErasureCodingPolicyManager and related codes |  Major | erasure-coding | Sammi Chen | Sammi Chen |
| [HADOOP-14844](https://issues.apache.org/jira/browse/HADOOP-14844) | Remove requirement to specify TenantGuid for MSI Token Provider |  Major | fs/adl | Atul Sikaria | Atul Sikaria |
| [YARN-6799](https://issues.apache.org/jira/browse/YARN-6799) | Remove the duplicated code in CGroupsHandlerImp.java |  Trivial | nodemanager | Yufei Gu | weiyuan |
| [HADOOP-14520](https://issues.apache.org/jira/browse/HADOOP-14520) | WASB: Block compaction for Azure Block Blobs |  Major | fs/azure | Georgi Chalakov | Georgi Chalakov |
| [HADOOP-14839](https://issues.apache.org/jira/browse/HADOOP-14839) | DistCp log output should contain copied and deleted files and directories |  Major | tools/distcp | Konstantin Shaposhnikov | Yiqun Lin |
| [YARN-7132](https://issues.apache.org/jira/browse/YARN-7132) | FairScheduler.initScheduler() contains a surprising unary plus |  Minor | fairscheduler | Daniel Templeton | Yeliang Cang |
| [HADOOP-14843](https://issues.apache.org/jira/browse/HADOOP-14843) | Improve FsPermission symbolic parsing unit test coverage |  Minor | fs | Jason Lowe | Bharat Viswanadham |
| [YARN-7057](https://issues.apache.org/jira/browse/YARN-7057) | FSAppAttempt#getResourceUsage doesn't need to consider resources queued for preemption |  Major | fairscheduler | Karthik Kambatla | Karthik Kambatla |
| [HDFS-12412](https://issues.apache.org/jira/browse/HDFS-12412) | Change ErasureCodingWorker.stripedReadPool to cached thread pool |  Major | erasure-coding | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HADOOP-14804](https://issues.apache.org/jira/browse/HADOOP-14804) | correct wrong parameters format order in core-default.xml |  Trivial | . | Chen Hongfei | Chen Hongfei |
| [HDFS-12409](https://issues.apache.org/jira/browse/HDFS-12409) | Add metrics of execution time of different stages in EC recovery task |  Minor | erasure-coding | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HADOOP-14864](https://issues.apache.org/jira/browse/HADOOP-14864) | FSDataInputStream#unbuffer UOE should include stream class name |  Minor | fs | John Zhuge | Bharat Viswanadham |
| [HADOOP-14869](https://issues.apache.org/jira/browse/HADOOP-14869) | Upgrade apache kerby verion to v1.0.1 |  Major | . | Wei Zhou | Wei Zhou |
| [MAPREDUCE-6956](https://issues.apache.org/jira/browse/MAPREDUCE-6956) | FileOutputCommitter to gain abstract superclass PathOutputCommitter |  Minor | mrv2 | Steve Loughran | Steve Loughran |
| [MAPREDUCE-6954](https://issues.apache.org/jira/browse/MAPREDUCE-6954) | Disable erasure coding for files that are uploaded to the MR staging area |  Major | client | Peter Bacsko | Peter Bacsko |
| [HDFS-12349](https://issues.apache.org/jira/browse/HDFS-12349) | Improve log message when it could not alloc enough blocks for EC |  Minor | erasure-coding | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HDFS-12441](https://issues.apache.org/jira/browse/HDFS-12441) | Suppress UnresolvedPathException in namenode log |  Minor | . | Kihwal Lee | Kihwal Lee |
| [HADOOP-13714](https://issues.apache.org/jira/browse/HADOOP-13714) | Tighten up our compatibility guidelines for Hadoop 3 |  Blocker | documentation | Karthik Kambatla | Daniel Templeton |
| [HDFS-12472](https://issues.apache.org/jira/browse/HDFS-12472) | Add JUNIT timeout to TestBlockStatsMXBean |  Minor | . | Lei (Eddy) Xu | Bharat Viswanadham |
| [HDFS-12460](https://issues.apache.org/jira/browse/HDFS-12460) | Make addErasureCodingPolicy an idempotent operation |  Major | erasure-coding | Sammi Chen | Sammi Chen |
| [HDFS-12479](https://issues.apache.org/jira/browse/HDFS-12479) | Some misuses of lock in DFSStripedOutputStream |  Minor | erasure-coding | Huafeng Wang | Huafeng Wang |
| [MAPREDUCE-6958](https://issues.apache.org/jira/browse/MAPREDUCE-6958) | Shuffle audit logger should log size of shuffle transfer |  Minor | . | Jason Lowe | Jason Lowe |
| [HDFS-12444](https://issues.apache.org/jira/browse/HDFS-12444) | Reduce runtime of TestWriteReadStripedFile |  Major | erasure-coding, test | Andrew Wang | Huafeng Wang |
| [HDFS-12445](https://issues.apache.org/jira/browse/HDFS-12445) | Correct spellings of choosen to chosen. |  Trivial | . | hu xiaodong | hu xiaodong |
| [HADOOP-7308](https://issues.apache.org/jira/browse/HADOOP-7308) | Remove unused TaskLogAppender configurations from log4j.properties |  Major | conf | Todd Lipcon | Todd Lipcon |
| [HDFS-12496](https://issues.apache.org/jira/browse/HDFS-12496) | Make QuorumJournalManager timeout properties configurable |  Major | . | Ajay Kumar | Ajay Kumar |
| [HDFS-12530](https://issues.apache.org/jira/browse/HDFS-12530) | Processor argument in Offline Image Viewer should be case insensitive |  Minor | tools | Hanisha Koneru | Hanisha Koneru |
| [HDFS-12304](https://issues.apache.org/jira/browse/HDFS-12304) | Remove unused parameter from FsDatasetImpl#addVolume |  Minor | . | Chen Liang | Chen Liang |
| [YARN-65](https://issues.apache.org/jira/browse/YARN-65) | Reduce RM app memory footprint once app has completed |  Major | resourcemanager | Jason Lowe | Manikandan R |
| [YARN-4879](https://issues.apache.org/jira/browse/YARN-4879) | Enhance Allocate Protocol to Identify Requests Explicitly |  Major | applications, resourcemanager | Subru Krishnan | Subru Krishnan |
| [YARN-6930](https://issues.apache.org/jira/browse/YARN-6930) | Admins should be able to explicitly enable specific LinuxContainerRuntime in the NodeManager |  Major | nodemanager | Vinod Kumar Vavilapalli | Shane Kumpf |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-6093](https://issues.apache.org/jira/browse/YARN-6093) | Minor bugs with AMRMtoken renewal and state store availability when using FederationRMFailoverProxyProvider during RM failover |  Minor | amrmproxy, federation | Botong Huang | Botong Huang |
| [YARN-6370](https://issues.apache.org/jira/browse/YARN-6370) | Properly handle rack requests for non-active subclusters in LocalityMulticastAMRMProxyPolicy |  Minor | . | Botong Huang | Botong Huang |
| [HDFS-12043](https://issues.apache.org/jira/browse/HDFS-12043) | Add counters for block re-replication |  Major | . | Chen Liang | Chen Liang |
| [YARN-6694](https://issues.apache.org/jira/browse/YARN-6694) | Add certain envs to the default yarn.nodemanager.env-whitelist |  Major | . | Jian He | Jian He |
| [MAPREDUCE-6905](https://issues.apache.org/jira/browse/MAPREDUCE-6905) | Fix meaningless operations in TestDFSIO in some situation. |  Major | tools/rumen | LiXin Ge | LiXin Ge |
| [HDFS-12079](https://issues.apache.org/jira/browse/HDFS-12079) | Description of dfs.block.invalidate.limit is incorrect in hdfs-default.xml |  Minor | documentation | Weiwei Yang | Weiwei Yang |
| [HADOOP-13414](https://issues.apache.org/jira/browse/HADOOP-13414) | Hide Jetty Server version header in HTTP responses |  Major | security | Vinayakumar B | Surendra Singh Lilhore |
| [HDFS-12089](https://issues.apache.org/jira/browse/HDFS-12089) | Fix ambiguous NN retry log message |  Major | webhdfs | Eric Badger | Eric Badger |
| [HADOOP-14608](https://issues.apache.org/jira/browse/HADOOP-14608) | KMS JMX servlet path not backwards compatible |  Minor | kms | John Zhuge | John Zhuge |
| [YARN-6708](https://issues.apache.org/jira/browse/YARN-6708) | Nodemanager container crash after ext3 folder limit |  Critical | . | Bibin A Chundatt | Bibin A Chundatt |
| [HADOOP-14563](https://issues.apache.org/jira/browse/HADOOP-14563) | LoadBalancingKMSClientProvider#warmUpEncryptedKeys swallows IOException |  Major | . | Rushabh S Shah | Rushabh S Shah |
| [MAPREDUCE-6246](https://issues.apache.org/jira/browse/MAPREDUCE-6246) | DBOutputFormat.java appending extra semicolon to query which is incompatible with DB2 |  Major | mrv1, mrv2 | ramtin | Gergely Novák |
| [HADOOP-14634](https://issues.apache.org/jira/browse/HADOOP-14634) | Remove jline from main Hadoop pom.xml |  Major | . | Ray Chiang | Ray Chiang |
| [YARN-6428](https://issues.apache.org/jira/browse/YARN-6428) | Queue AM limit is not honored  in CS always |  Major | . | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-6770](https://issues.apache.org/jira/browse/YARN-6770) | [Docs] A small mistake in the example of TimelineClient |  Trivial | docs | Jinjiang Ling | Jinjiang Ling |
| [HADOOP-10829](https://issues.apache.org/jira/browse/HADOOP-10829) | Iteration on CredentialProviderFactory.serviceLoader  is thread-unsafe |  Major | security | Benoy Antony | Benoy Antony |
| [HDFS-12085](https://issues.apache.org/jira/browse/HDFS-12085) | Reconfigure namenode heartbeat interval fails if the interval was set with time unit |  Minor | hdfs, tools | Weiwei Yang | Weiwei Yang |
| [HDFS-12052](https://issues.apache.org/jira/browse/HDFS-12052) | Set SWEBHDFS delegation token kind when ssl is enabled in HttpFS |  Major | httpfs, webhdfs | Zoran Dimitrijevic | Zoran Dimitrijevic |
| [HDFS-12114](https://issues.apache.org/jira/browse/HDFS-12114) | Consistent HttpFS property names |  Major | httpfs | John Zhuge | John Zhuge |
| [HADOOP-14581](https://issues.apache.org/jira/browse/HADOOP-14581) | Restrict setOwner to list of user when security is enabled in wasb |  Major | fs/azure | Varada Hemeswari | Varada Hemeswari |
| [YARN-6809](https://issues.apache.org/jira/browse/YARN-6809) | Fix typo in ResourceManagerHA.md |  Trivial | documentation | Akira Ajisaka | Yeliang Cang |
| [YARN-6797](https://issues.apache.org/jira/browse/YARN-6797) | TimelineWriter does not fully consume the POST response |  Major | timelineclient | Jason Lowe | Jason Lowe |
| [HDFS-11502](https://issues.apache.org/jira/browse/HDFS-11502) | Datanode UI should display hostname based on JMX bean instead of window.location.hostname |  Major | hdfs | Jeffrey E  Rodriguez | Jeffrey E  Rodriguez |
| [HADOOP-14646](https://issues.apache.org/jira/browse/HADOOP-14646) | FileContextMainOperationsBaseTest#testListStatusFilterWithSomeMatches never runs |  Minor | test | Andras Bokor | Andras Bokor |
| [YARN-6654](https://issues.apache.org/jira/browse/YARN-6654) | RollingLevelDBTimelineStore backwards incompatible after fst upgrade |  Blocker | . | Jonathan Eagles | Jonathan Eagles |
| [YARN-6805](https://issues.apache.org/jira/browse/YARN-6805) | NPE in LinuxContainerExecutor due to null PrivilegedOperationException exit code |  Major | nodemanager | Jason Lowe | Jason Lowe |
| [YARN-6792](https://issues.apache.org/jira/browse/YARN-6792) | Incorrect XML convertion in NodeIDsInfo and LabelsToNodesInfo |  Blocker | resourcemanager | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [YARN-6769](https://issues.apache.org/jira/browse/YARN-6769) | Make schedulables without demand less needy in FairSharePolicy#compare |  Major | fairscheduler | YunFan Zhou | YunFan Zhou |
| [YARN-6759](https://issues.apache.org/jira/browse/YARN-6759) | Fix TestRMRestart.testRMRestartWaitForPreviousAMToFinish failure |  Major | . | Naganarasimha G R | Naganarasimha G R |
| [YARN-3260](https://issues.apache.org/jira/browse/YARN-3260) | AM attempt fail to register before RM processes launch event |  Critical | resourcemanager | Jason Lowe | Bibin A Chundatt |
| [HDFS-12140](https://issues.apache.org/jira/browse/HDFS-12140) | Remove BPOfferService lock contention to get block pool id |  Critical | datanode | Daryn Sharp | Daryn Sharp |
| [HDFS-12112](https://issues.apache.org/jira/browse/HDFS-12112) | TestBlockManager#testBlockManagerMachinesArray sometimes fails with NPE |  Minor | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-6714](https://issues.apache.org/jira/browse/YARN-6714) | IllegalStateException while handling APP\_ATTEMPT\_REMOVED event when async-scheduling enabled in CapacityScheduler |  Major | . | Tao Yang | Tao Yang |
| [MAPREDUCE-6889](https://issues.apache.org/jira/browse/MAPREDUCE-6889) | Add Job#close API to shutdown MR client services. |  Major | . | Rohith Sharma K S | Rohith Sharma K S |
| [MAPREDUCE-6910](https://issues.apache.org/jira/browse/MAPREDUCE-6910) | MapReduceTrackingUriPlugin can not return the right URI of history server with HTTPS |  Major | jobhistoryserver | Lantao Jin | Lantao Jin |
| [HDFS-12154](https://issues.apache.org/jira/browse/HDFS-12154) | Incorrect javadoc description in StorageLocationChecker#check |  Major | . | Nanda kumar | Nanda kumar |
| [YARN-6798](https://issues.apache.org/jira/browse/YARN-6798) | Fix NM startup failure with old state store due to version mismatch |  Major | nodemanager | Ray Chiang | Botong Huang |
| [HADOOP-14637](https://issues.apache.org/jira/browse/HADOOP-14637) | GenericTestUtils.waitFor needs to check condition again after max wait time |  Major | . | Daniel Templeton | Daniel Templeton |
| [YARN-6819](https://issues.apache.org/jira/browse/YARN-6819) | Application report fails if app rejected due to nodesize |  Major | . | Bibin A Chundatt | Bibin A Chundatt |
| [HADOOP-14669](https://issues.apache.org/jira/browse/HADOOP-14669) | GenericTestUtils.waitFor should use monotonic time |  Trivial | test | Jason Lowe | Daniel Templeton |
| [HDFS-12133](https://issues.apache.org/jira/browse/HDFS-12133) | Correct ContentSummaryComputationContext Logger class name. |  Minor | namenode | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-12158](https://issues.apache.org/jira/browse/HDFS-12158) | Secondary Namenode's web interface lack configs for X-FRAME-OPTIONS protection |  Major | namenode | Mukul Kumar Singh | Mukul Kumar Singh |
| [HDFS-12139](https://issues.apache.org/jira/browse/HDFS-12139) | HTTPFS liststatus returns incorrect pathSuffix for path of file |  Major | httpfs | Yongjun Zhang | Yongjun Zhang |
| [YARN-6837](https://issues.apache.org/jira/browse/YARN-6837) | Null LocalResource visibility or resource type can crash the nodemanager |  Major | . | Jinjiang Ling | Jinjiang Ling |
| [HDFS-11472](https://issues.apache.org/jira/browse/HDFS-11472) | Fix inconsistent replica size after a data pipeline failure |  Critical | datanode | Wei-Chiu Chuang | Erik Krogen |
| [HDFS-12166](https://issues.apache.org/jira/browse/HDFS-12166) | Do not deprecate HTTPFS\_TEMP |  Minor | httpfs | John Zhuge | John Zhuge |
| [HDFS-11742](https://issues.apache.org/jira/browse/HDFS-11742) | Improve balancer usability after HDFS-8818 |  Blocker | . | Kihwal Lee | Kihwal Lee |
| [HDFS-12094](https://issues.apache.org/jira/browse/HDFS-12094) | Log torrent when none isa-l EC is used. |  Minor | erasure-coding | LiXin Ge | LiXin Ge |
| [HDFS-12176](https://issues.apache.org/jira/browse/HDFS-12176) | dfsadmin shows DFS Used%: NaN% if the cluster has zero block. |  Trivial | . | Wei-Chiu Chuang | Weiwei Yang |
| [YARN-6844](https://issues.apache.org/jira/browse/YARN-6844) | AMRMClientImpl.checkNodeLabelExpression() has wrong error message |  Minor | . | Daniel Templeton | Manikandan R |
| [YARN-6150](https://issues.apache.org/jira/browse/YARN-6150) | TestContainerManagerSecurity tests for Yarn Server are flakey |  Major | test | Daniel Sturman | Daniel Sturman |
| [YARN-6307](https://issues.apache.org/jira/browse/YARN-6307) | Refactor FairShareComparator#compare |  Major | fairscheduler | Yufei Gu | Yufei Gu |
| [HADOOP-14578](https://issues.apache.org/jira/browse/HADOOP-14578) | Bind IPC connections to kerberos UPN host for proxy users |  Major | ipc | Daryn Sharp | Daryn Sharp |
| [HDFS-11896](https://issues.apache.org/jira/browse/HDFS-11896) | Non-dfsUsed will be doubled on dead node re-registration |  Blocker | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-14692](https://issues.apache.org/jira/browse/HADOOP-14692) | Upgrade Apache Rat |  Trivial | build | Allen Wittenauer | Allen Wittenauer |
| [HDFS-12062](https://issues.apache.org/jira/browse/HDFS-12062) | removeErasureCodingPolicy needs super user permission |  Critical | erasure-coding | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-12044](https://issues.apache.org/jira/browse/HDFS-12044) | Mismatch between BlockManager#maxReplicationStreams and ErasureCodingWorker.stripedReconstructionPool pool size causes slow and bursty recovery |  Major | erasure-coding | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HADOOP-14229](https://issues.apache.org/jira/browse/HADOOP-14229) | hadoop.security.auth\_to\_local example is incorrect in the documentation |  Major | . | Andras Bokor | Andras Bokor |
| [YARN-6870](https://issues.apache.org/jira/browse/YARN-6870) | Fix floating point inaccuracies in resource availability check in AllocationBasedResourceUtilizationTracker |  Major | api, nodemanager | Brook Zhou | Brook Zhou |
| [YARN-5728](https://issues.apache.org/jira/browse/YARN-5728) | TestMiniYarnClusterNodeUtilization.testUpdateNodeUtilization timeout |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-14677](https://issues.apache.org/jira/browse/HADOOP-14677) | mvn clean compile fails |  Major | build | Andras Bokor | Andras Bokor |
| [HDFS-12082](https://issues.apache.org/jira/browse/HDFS-12082) | BlockInvalidateLimit value is incorrectly set after namenode heartbeat interval reconfigured |  Major | hdfs, namenode | Weiwei Yang | Weiwei Yang |
| [MAPREDUCE-6924](https://issues.apache.org/jira/browse/MAPREDUCE-6924) | Revert MAPREDUCE-6199 MAPREDUCE-6286 and MAPREDUCE-5875 |  Major | . | Andrew Wang | Junping Du |
| [HADOOP-14420](https://issues.apache.org/jira/browse/HADOOP-14420) | generateReports property is not applicable for maven-site-plugin:attach-descriptor goal |  Minor | . | Andras Bokor | Andras Bokor |
| [HADOOP-14644](https://issues.apache.org/jira/browse/HADOOP-14644) | Increase max heap size of Maven javadoc plugin |  Major | test | Andras Bokor | Andras Bokor |
| [HADOOP-14343](https://issues.apache.org/jira/browse/HADOOP-14343) | Wrong pid file name in error message when starting secure daemon |  Minor | . | Andras Bokor | Andras Bokor |
| [MAPREDUCE-6921](https://issues.apache.org/jira/browse/MAPREDUCE-6921) | TestUmbilicalProtocolWithJobToken#testJobTokenRpc fails |  Major | . | Sonia Garudi | Sonia Garudi |
| [HADOOP-14676](https://issues.apache.org/jira/browse/HADOOP-14676) | Wrong default value for "fs.df.interval" |  Major | common, conf, fs | Konstantin Shvachko | Sherwood Zheng |
| [HADOOP-14701](https://issues.apache.org/jira/browse/HADOOP-14701) | Configuration can log misleading warnings about an attempt to override final parameter |  Major | conf | Andrew Sherman | Andrew Sherman |
| [YARN-5731](https://issues.apache.org/jira/browse/YARN-5731) | Preemption calculation is not accurate when reserved containers are present in queue. |  Major | capacity scheduler | Sunil Govindan | Wangda Tan |
| [HADOOP-14683](https://issues.apache.org/jira/browse/HADOOP-14683) | FileStatus.compareTo binary compatible issue |  Blocker | . | Sergey Shelukhin | Akira Ajisaka |
| [HDFS-12107](https://issues.apache.org/jira/browse/HDFS-12107) | FsDatasetImpl#removeVolumes floods the logs when removing the volume |  Major | . | Haohui Mai | Kelvin Chu |
| [HADOOP-14702](https://issues.apache.org/jira/browse/HADOOP-14702) | Fix formatting issue and regression caused by conversion from APT to Markdown |  Minor | documentation | Doris Gu | Doris Gu |
| [YARN-6872](https://issues.apache.org/jira/browse/YARN-6872) | Ensure apps could run given NodeLabels are disabled post RM switchover/restart |  Major | resourcemanager | Sunil Govindan | Sunil Govindan |
| [HDFS-12217](https://issues.apache.org/jira/browse/HDFS-12217) | HDFS snapshots doesn't capture all open files when one of the open files is deleted |  Major | snapshots | Manoj Govindassamy | Manoj Govindassamy |
| [YARN-6846](https://issues.apache.org/jira/browse/YARN-6846) | Nodemanager can fail to fully delete application local directories when applications are killed |  Critical | nodemanager | Jason Lowe | Jason Lowe |
| [YARN-6678](https://issues.apache.org/jira/browse/YARN-6678) | Handle IllegalStateException in Async Scheduling mode of CapacityScheduler |  Major | capacityscheduler | Tao Yang | Tao Yang |
| [YARN-6932](https://issues.apache.org/jira/browse/YARN-6932) | Fix TestFederationRMFailoverProxyProvider test case failure |  Major | . | Arun Suresh | Subru Krishnan |
| [YARN-6895](https://issues.apache.org/jira/browse/YARN-6895) | [FairScheduler] Preemption reservation may cause regular reservation leaks |  Blocker | fairscheduler | Miklos Szegedi | Miklos Szegedi |
| [HADOOP-13963](https://issues.apache.org/jira/browse/HADOOP-13963) | /bin/bash is hard coded in some of the scripts |  Major | scripts | Miklos Szegedi | Ajay Kumar |
| [HADOOP-14722](https://issues.apache.org/jira/browse/HADOOP-14722) | Azure: BlockBlobInputStream position incorrect after seek |  Major | fs/azure | Thomas Marquardt | Thomas Marquardt |
| [YARN-6951](https://issues.apache.org/jira/browse/YARN-6951) | Fix debug log when Resource Handler chain is enabled |  Minor | . | Yang Wang | Yang Wang |
| [HADOOP-14727](https://issues.apache.org/jira/browse/HADOOP-14727) | Socket not closed properly when reading Configurations with BlockReaderRemote |  Blocker | conf | Xiao Chen | Jonathan Eagles |
| [YARN-6920](https://issues.apache.org/jira/browse/YARN-6920) | Fix resource leak that happens during container re-initialization. |  Major | nodemanager | Arun Suresh | Arun Suresh |
| [HADOOP-14730](https://issues.apache.org/jira/browse/HADOOP-14730) | Support protobuf FileStatus in AdlFileSystem |  Major | . | Vishwajeet Dusane | Chris Douglas |
| [HDFS-12198](https://issues.apache.org/jira/browse/HDFS-12198) | Document missing namenode metrics that were added recently |  Minor | documentation | Yiqun Lin | Yiqun Lin |
| [HADOOP-14680](https://issues.apache.org/jira/browse/HADOOP-14680) | Azure: IndexOutOfBoundsException in BlockBlobInputStream |  Minor | fs/azure | Rajesh Balamohan | Thomas Marquardt |
| [MAPREDUCE-6927](https://issues.apache.org/jira/browse/MAPREDUCE-6927) | MR job should only set tracking url if history was successfully written |  Major | . | Eric Badger | Eric Badger |
| [YARN-6890](https://issues.apache.org/jira/browse/YARN-6890) | If UI is not secured, we allow user to kill other users' job even yarn cluster is secured. |  Critical | . | Sumana Sathish | Junping Du |
| [HDFS-10326](https://issues.apache.org/jira/browse/HDFS-10326) | Disable setting tcp socket send/receive buffers for write pipelines |  Major | datanode, hdfs | Daryn Sharp | Daryn Sharp |
| [HADOOP-14598](https://issues.apache.org/jira/browse/HADOOP-14598) | Blacklist Http/HttpsFileSystem in FsUrlStreamHandlerFactory |  Major | fs/azure, test | Steve Loughran | Steve Loughran |
| [YARN-6515](https://issues.apache.org/jira/browse/YARN-6515) | Fix warnings from Spotbugs in hadoop-yarn-server-nodemanager |  Major | nodemanager | Naganarasimha G R | Naganarasimha G R |
| [HDFS-12157](https://issues.apache.org/jira/browse/HDFS-12157) | Do fsyncDirectory(..) outside of FSDataset lock |  Critical | datanode | Vinayakumar B | Vinayakumar B |
| [HDFS-12278](https://issues.apache.org/jira/browse/HDFS-12278) | LeaseManager operations are inefficient in 2.8. |  Blocker | namenode | Rushabh S Shah | Rushabh S Shah |
| [HADOOP-14743](https://issues.apache.org/jira/browse/HADOOP-14743) | CompositeGroupsMapping should not swallow exceptions |  Major | security | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-14754](https://issues.apache.org/jira/browse/HADOOP-14754) | TestCommonConfigurationFields failed: core-default.xml has 2 wasb properties missing in classes |  Minor | common, fs/azure | John Zhuge | John Zhuge |
| [HADOOP-14760](https://issues.apache.org/jira/browse/HADOOP-14760) | Add missing override to LoadBalancingKMSClientProvider |  Minor | kms | Xiao Chen | Xiao Chen |
| [YARN-5927](https://issues.apache.org/jira/browse/YARN-5927) | BaseContainerManagerTest::waitForNMContainerState timeout accounting is not accurate |  Trivial | . | Miklos Szegedi | Kai Sasaki |
| [YARN-6967](https://issues.apache.org/jira/browse/YARN-6967) | Limit application attempt's diagnostic message size thoroughly |  Major | resourcemanager | Chengbing Liu | Chengbing Liu |
| [HDFS-11303](https://issues.apache.org/jira/browse/HDFS-11303) | Hedged read might hang infinitely if read data from all DN failed |  Major | hdfs-client | Chen Zhang | Chen Zhang |
| [YARN-6996](https://issues.apache.org/jira/browse/YARN-6996) | Change javax.cache library implementation from JSR107 to Apache Geronimo |  Blocker | . | Ray Chiang | Ray Chiang |
| [YARN-6987](https://issues.apache.org/jira/browse/YARN-6987) | Log app attempt during InvalidStateTransition |  Major | . | Jonathan Eagles | Jonathan Eagles |
| [HDFS-11696](https://issues.apache.org/jira/browse/HDFS-11696) | Fix warnings from Spotbugs in hadoop-hdfs |  Major | . | Yiqun Lin | Yiqun Lin |
| [HDFS-12066](https://issues.apache.org/jira/browse/HDFS-12066) | When Namenode is in safemode,may not allowed to remove an user's erasure coding policy |  Major | hdfs | lufei | lufei |
| [HDFS-12054](https://issues.apache.org/jira/browse/HDFS-12054) | FSNamesystem#addErasureCodingPolicies should call checkNameNodeSafeMode() to ensure Namenode is not in safemode |  Major | hdfs | lufei | lufei |
| [YARN-7014](https://issues.apache.org/jira/browse/YARN-7014) | container-executor has off-by-one error which can corrupt the heap |  Critical | yarn | Shane Kumpf | Jason Lowe |
| [HADOOP-14773](https://issues.apache.org/jira/browse/HADOOP-14773) | Extend ZKCuratorManager API for more reusability |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [YARN-6965](https://issues.apache.org/jira/browse/YARN-6965) | Duplicate instantiation in FairSchedulerQueueInfo |  Minor | fairscheduler | Masahiro Tanaka | Masahiro Tanaka |
| [YARN-7020](https://issues.apache.org/jira/browse/YARN-7020) | TestAMRMProxy#testAMRMProxyTokenRenewal is flakey |  Major | . | Robert Kanter | Robert Kanter |
| [MAPREDUCE-6940](https://issues.apache.org/jira/browse/MAPREDUCE-6940) | Copy-paste error in the TaskAttemptUnsuccessfulCompletionEvent constructor |  Minor | . | Oleg Danilov | Oleg Danilov |
| [MAPREDUCE-6936](https://issues.apache.org/jira/browse/MAPREDUCE-6936) | Remove unnecessary dependency of hadoop-yarn-server-common from hadoop-mapreduce-client-common |  Major | mrv2 | Haibo Chen | Haibo Chen |
| [HDFS-12250](https://issues.apache.org/jira/browse/HDFS-12250) | Reduce usage of FsPermissionExtension in unit tests |  Minor | test | Chris Douglas | Chris Douglas |
| [HDFS-12316](https://issues.apache.org/jira/browse/HDFS-12316) | Verify HDFS snapshot deletion doesn't crash the ongoing file writes |  Major | . | Manoj Govindassamy | Manoj Govindassamy |
| [YARN-7007](https://issues.apache.org/jira/browse/YARN-7007) | NPE in RM while using YarnClient.getApplications() |  Major | . | Lingfeng Su | Lingfeng Su |
| [HDFS-12325](https://issues.apache.org/jira/browse/HDFS-12325) | SFTPFileSystem operations should restore cwd |  Major | . | Namit Maheshwari | Chen Liang |
| [HDFS-11738](https://issues.apache.org/jira/browse/HDFS-11738) | Hedged pread takes more time when block moved from initial locations |  Major | hdfs-client | Vinayakumar B | Vinayakumar B |
| [YARN-2416](https://issues.apache.org/jira/browse/YARN-2416) | InvalidStateTransitonException in ResourceManager if AMLauncher does not receive response for startContainers() call in time |  Critical | resourcemanager | Jian Fang | Jonathan Eagles |
| [YARN-7048](https://issues.apache.org/jira/browse/YARN-7048) | Fix tests faking kerberos to explicitly set ugi auth type |  Major | yarn | Daryn Sharp | Daryn Sharp |
| [HADOOP-14687](https://issues.apache.org/jira/browse/HADOOP-14687) | AuthenticatedURL will reuse bad/expired session cookies |  Critical | common | Daryn Sharp | Daryn Sharp |
| [YARN-6251](https://issues.apache.org/jira/browse/YARN-6251) | Do async container release to prevent deadlock during container updates |  Major | . | Arun Suresh | Arun Suresh |
| [YARN-7090](https://issues.apache.org/jira/browse/YARN-7090) | testRMRestartAfterNodeLabelDisabled get failed when CapacityScheduler is configured |  Major | test | Yesha Vora | Wangda Tan |
| [HDFS-12318](https://issues.apache.org/jira/browse/HDFS-12318) | Fix IOException condition for openInfo in DFSInputStream |  Major | . | legend | legend |
| [YARN-7074](https://issues.apache.org/jira/browse/YARN-7074) | Fix NM state store update comment |  Minor | nodemanager | Botong Huang | Botong Huang |
| [HDFS-12344](https://issues.apache.org/jira/browse/HDFS-12344) | LocatedFileStatus regression: no longer accepting null FSPermission |  Minor | fs | Ewan Higgs | Ewan Higgs |
| [YARN-6640](https://issues.apache.org/jira/browse/YARN-6640) |  AM heartbeat stuck when responseId overflows MAX\_INT |  Blocker | . | Botong Huang | Botong Huang |
| [HDFS-12319](https://issues.apache.org/jira/browse/HDFS-12319) | DirectoryScanner will throw IllegalStateException when Multiple BP's are present |  Blocker | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-12299](https://issues.apache.org/jira/browse/HDFS-12299) | Race Between update pipeline and DN Re-Registration |  Critical | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-7052](https://issues.apache.org/jira/browse/YARN-7052) | RM SchedulingMonitor gives no indication why the spawned thread crashed. |  Critical | yarn | Eric Payne | Eric Payne |
| [YARN-7087](https://issues.apache.org/jira/browse/YARN-7087) | NM failed to perform log aggregation due to absent container |  Blocker | log-aggregation | Jason Lowe | Jason Lowe |
| [HDFS-12215](https://issues.apache.org/jira/browse/HDFS-12215) | DataNode#transferBlock does not create its daemon in the xceiver thread group |  Major | datanode | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HDFS-12248](https://issues.apache.org/jira/browse/HDFS-12248) | SNN will not upload fsimage on IOE and Interrupted exceptions |  Critical | ha, namenode, rolling upgrades | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-12358](https://issues.apache.org/jira/browse/HDFS-12358) | Handle IOException when transferring edit log to Journal current dir through JN sync |  Major | . | Hanisha Koneru | Hanisha Koneru |
| [MAPREDUCE-6945](https://issues.apache.org/jira/browse/MAPREDUCE-6945) | TestMapFileOutputFormat missing @after annotation |  Minor | . | Ajay Kumar | Ajay Kumar |
| [YARN-7051](https://issues.apache.org/jira/browse/YARN-7051) | Avoid concurrent modification exception in FifoIntraQueuePreemptionPlugin |  Critical | capacity scheduler, scheduler preemption, yarn | Eric Payne | Eric Payne |
| [YARN-7099](https://issues.apache.org/jira/browse/YARN-7099) | ResourceHandlerModule.parseConfiguredCGroupPath only works for privileged yarn users. |  Minor | nodemanager | Miklos Szegedi | Miklos Szegedi |
| [YARN-7112](https://issues.apache.org/jira/browse/YARN-7112) | TestAMRMProxy is failing with invalid request |  Major | . | Jason Lowe | Jason Lowe |
| [HDFS-12293](https://issues.apache.org/jira/browse/HDFS-12293) | DataNode should log file name on disk error |  Major | datanode | Wei-Chiu Chuang | Ajay Kumar |
| [YARN-7076](https://issues.apache.org/jira/browse/YARN-7076) | yarn application -list -appTypes \<appType\> is not working |  Blocker | . | Jian He | Jian He |
| [YARN-5816](https://issues.apache.org/jira/browse/YARN-5816) | TestDelegationTokenRenewer#testCancelWithMultipleAppSubmissions is still flakey |  Minor | resourcemanager, test | Daniel Templeton | Robert Kanter |
| [YARN-6756](https://issues.apache.org/jira/browse/YARN-6756) | ContainerRequest#executionTypeRequest causes NPE |  Critical | . | Jian He | Jian He |
| [HDFS-12191](https://issues.apache.org/jira/browse/HDFS-12191) | Provide option to not capture the accessTime change of a file to snapshot if no other modification has been done to this file |  Major | hdfs, namenode | Yongjun Zhang | Yongjun Zhang |
| [YARN-6982](https://issues.apache.org/jira/browse/YARN-6982) | Potential issue on setting AMContainerSpec#tokenConf to null before app is completed |  Major | . | Rohith Sharma K S | Manikandan R |
| [HDFS-12336](https://issues.apache.org/jira/browse/HDFS-12336) | Listing encryption zones still fails when deleted EZ is not a direct child of snapshottable directory |  Minor | encryption, hdfs | Wellington Chevreuil | Wellington Chevreuil |
| [HADOOP-14814](https://issues.apache.org/jira/browse/HADOOP-14814) | Fix incompatible API change on FsServerDefaults to HADOOP-14104 |  Blocker | . | Junping Du | Junping Du |
| [MAPREDUCE-6931](https://issues.apache.org/jira/browse/MAPREDUCE-6931) | Remove TestDFSIO "Total Throughput" calculation |  Critical | benchmarks, test | Dennis Huo | Dennis Huo |
| [YARN-7115](https://issues.apache.org/jira/browse/YARN-7115) | Move BoundedAppender to org.hadoop.yarn.util pacakge |  Major | . | Jian He | Jian He |
| [YARN-7077](https://issues.apache.org/jira/browse/YARN-7077) | TestAMSimulator and TestNMSimulator fail |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [YARN-7116](https://issues.apache.org/jira/browse/YARN-7116) | CapacityScheduler Web UI: Queue's AM usage is always show on per-user's AM usage. |  Major | capacity scheduler, webapp | Wangda Tan | Wangda Tan |
| [HADOOP-14364](https://issues.apache.org/jira/browse/HADOOP-14364) | refresh changelog/release notes with newer Apache Yetus build |  Major | build, documentation | Allen Wittenauer | Allen Wittenauer |
| [HDFS-12317](https://issues.apache.org/jira/browse/HDFS-12317) | HDFS metrics render error in the page of Github |  Minor | documentation, metrics | Yiqun Lin | Yiqun Lin |
| [HADOOP-14824](https://issues.apache.org/jira/browse/HADOOP-14824) | Update ADLS SDK to 2.2.2 for MSI fix |  Major | fs/adl | Atul Sikaria | Atul Sikaria |
| [HDFS-12363](https://issues.apache.org/jira/browse/HDFS-12363) | Possible NPE in BlockManager$StorageInfoDefragmenter#scanAndCompactStorages |  Major | namenode | Xiao Chen | Xiao Chen |
| [YARN-7141](https://issues.apache.org/jira/browse/YARN-7141) | Move logging APIs to slf4j in timelineservice after ATSv2 merge |  Minor | . | Varun Saxena | Varun Saxena |
| [HDFS-11964](https://issues.apache.org/jira/browse/HDFS-11964) | Decoding inputs should be correctly prepared in pread |  Major | erasure-coding | Lei (Eddy) Xu | Takanobu Asanuma |
| [YARN-7120](https://issues.apache.org/jira/browse/YARN-7120) | CapacitySchedulerPage NPE in "Aggregate scheduler counts" section |  Minor | . | Eric Payne | Eric Payne |
| [HADOOP-14674](https://issues.apache.org/jira/browse/HADOOP-14674) | Correct javadoc for getRandomizedTempPath |  Major | common | Mukul Kumar Singh | Mukul Kumar Singh |
| [YARN-7023](https://issues.apache.org/jira/browse/YARN-7023) | Incorrect ReservationId.compareTo() implementation |  Minor | reservation system | Oleg Danilov | Oleg Danilov |
| [HDFS-12383](https://issues.apache.org/jira/browse/HDFS-12383) | Re-encryption updater should handle canceled tasks better |  Major | encryption | Xiao Chen | Xiao Chen |
| [YARN-7152](https://issues.apache.org/jira/browse/YARN-7152) | [ATSv2] Registering timeline client before AMRMClient service init throw exception. |  Major | timelineclient | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-12388](https://issues.apache.org/jira/browse/HDFS-12388) | A bad error message in DFSStripedOutputStream |  Major | erasure-coding | Kai Zheng | Huafeng Wang |
| [HADOOP-14820](https://issues.apache.org/jira/browse/HADOOP-14820) | Wasb mkdirs security checks inconsistent with HDFS |  Major | fs/azure | Sivaguru Sankaridurg | Sivaguru Sankaridurg |
| [HDFS-12359](https://issues.apache.org/jira/browse/HDFS-12359) | Re-encryption should operate with minimum KMS ACL requirements. |  Major | encryption | Xiao Chen | Xiao Chen |
| [HDFS-11882](https://issues.apache.org/jira/browse/HDFS-11882) | Precisely calculate acked length of striped block groups in updatePipeline |  Critical | erasure-coding, test | Akira Ajisaka | Andrew Wang |
| [HDFS-12392](https://issues.apache.org/jira/browse/HDFS-12392) | Writing striped file failed due to different cell size |  Major | erasure-coding | Sammi Chen | Sammi Chen |
| [YARN-7164](https://issues.apache.org/jira/browse/YARN-7164) | TestAMRMClientOnRMRestart fails sporadically with bind address in use |  Major | test | Jason Lowe | Jason Lowe |
| [YARN-6992](https://issues.apache.org/jira/browse/YARN-6992) | Kill application button is visible even if the application is FINISHED in RM UI |  Major | . | Sumana Sathish | Suma Shivaprasad |
| [HDFS-12357](https://issues.apache.org/jira/browse/HDFS-12357) | Let NameNode to bypass external attribute provider for special user |  Major | . | Yongjun Zhang | Yongjun Zhang |
| [HDFS-12369](https://issues.apache.org/jira/browse/HDFS-12369) | Edit log corruption due to hard lease recovery of not-closed file which has snapshots |  Major | namenode | Xiao Chen | Xiao Chen |
| [HDFS-12404](https://issues.apache.org/jira/browse/HDFS-12404) | Rename hdfs config authorization.provider.bypass.users to attributes.provider.bypass.users |  Major | hdfs | Yongjun Zhang | Manoj Govindassamy |
| [HDFS-12400](https://issues.apache.org/jira/browse/HDFS-12400) | Provide a way for NN to drain the local key cache before re-encryption |  Major | encryption | Xiao Chen | Xiao Chen |
| [YARN-7140](https://issues.apache.org/jira/browse/YARN-7140) | CollectorInfo should have Public visibility |  Minor | . | Varun Saxena | Varun Saxena |
| [YARN-7130](https://issues.apache.org/jira/browse/YARN-7130) | ATSv2 documentation changes post merge |  Major | timelineserver | Varun Saxena | Varun Saxena |
| [HADOOP-14851](https://issues.apache.org/jira/browse/HADOOP-14851) | LambdaTestUtils.eventually() doesn't spin on Assertion failures |  Major | test | Steve Loughran | Steve Loughran |
| [YARN-7181](https://issues.apache.org/jira/browse/YARN-7181) | CPUTimeTracker.updateElapsedJiffies can report negative usage |  Major | . | Miklos Szegedi | Miklos Szegedi |
| [HDFS-12417](https://issues.apache.org/jira/browse/HDFS-12417) | Disable flaky TestDFSStripedOutputStreamWithFailure |  Major | test | Chris Douglas | Andrew Wang |
| [HADOOP-14856](https://issues.apache.org/jira/browse/HADOOP-14856) | Fix AWS, Jetty, HBase, Ehcache entries for NOTICE.txt |  Major | . | Ray Chiang | Ray Chiang |
| [HDFS-12407](https://issues.apache.org/jira/browse/HDFS-12407) | Journal nodes fails to shutdown cleanly if JournalNodeHttpServer or JournalNodeRpcServer fails to start |  Major | . | Ajay Kumar | Ajay Kumar |
| [YARN-7185](https://issues.apache.org/jira/browse/YARN-7185) | ContainerScheduler should only look at availableResource for GUARANTEED containers when OPPORTUNISTIC container queuing is enabled. |  Blocker | yarn | Sumana Sathish | Tan, Wangda |
| [HDFS-12222](https://issues.apache.org/jira/browse/HDFS-12222) | Document and test BlockLocation for erasure-coded files |  Major | . | Andrew Wang | Huafeng Wang |
| [HADOOP-14867](https://issues.apache.org/jira/browse/HADOOP-14867) | Update HDFS Federation setup document, for incorrect property name for secondary name node http address |  Major | . | Bharat Viswanadham | Bharat Viswanadham |
| [YARN-4727](https://issues.apache.org/jira/browse/YARN-4727) | Unable to override the $HADOOP\_CONF\_DIR env variable for container |  Major | nodemanager | Terence Yim | Jason Lowe |
| [MAPREDUCE-6957](https://issues.apache.org/jira/browse/MAPREDUCE-6957) | shuffle hangs after a node manager connection timeout |  Major | mrv2 | Jooseong Kim | Jooseong Kim |
| [HDFS-12457](https://issues.apache.org/jira/browse/HDFS-12457) | Revert HDFS-11156 Add new op GETFILEBLOCKLOCATIONS to WebHDFS REST API |  Major | webhdfs | Andrew Wang | Andrew Wang |
| [HDFS-12378](https://issues.apache.org/jira/browse/HDFS-12378) | TestClientProtocolForPipelineRecovery#testZeroByteBlockRecovery fails on trunk |  Blocker | test | Xiao Chen | Lei (Eddy) Xu |
| [HDFS-12456](https://issues.apache.org/jira/browse/HDFS-12456) | TestNamenodeMetrics.testSyncAndBlockReportMetric fails |  Minor | hdfs, metrics | Bharat Viswanadham | Bharat Viswanadham |
| [YARN-7163](https://issues.apache.org/jira/browse/YARN-7163) | RMContext need not to be injected to webapp and other Always Running services. |  Blocker | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-12424](https://issues.apache.org/jira/browse/HDFS-12424) | Datatable sorting on the Datanode Information page in the Namenode UI is broken |  Major | . | Shawna Martell | Shawna Martell |
| [HADOOP-14853](https://issues.apache.org/jira/browse/HADOOP-14853) | hadoop-mapreduce-client-app is not a client module |  Major | . | Haibo Chen | Haibo Chen |
| [HDFS-12323](https://issues.apache.org/jira/browse/HDFS-12323) | NameNode terminates after full GC thinking QJM unresponsive if full GC is much longer than timeout |  Major | namenode, qjm | Erik Krogen | Erik Krogen |
| [HDFS-10701](https://issues.apache.org/jira/browse/HDFS-10701) | TestDFSStripedOutputStreamWithFailure#testBlockTokenExpired occasionally fails |  Major | erasure-coding | Wei-Chiu Chuang | Sammi Chen |
| [YARN-6977](https://issues.apache.org/jira/browse/YARN-6977) | Node information is not provided for non am containers in RM logs |  Major | capacity scheduler | Sumana Sathish | Suma Shivaprasad |
| [YARN-7149](https://issues.apache.org/jira/browse/YARN-7149) | Cross-queue preemption sometimes starves an underserved queue |  Major | capacity scheduler | Eric Payne | Eric Payne |
| [YARN-7192](https://issues.apache.org/jira/browse/YARN-7192) | Add a pluggable StateMachine Listener that is notified of NM Container State changes |  Major | . | Arun Suresh | Arun Suresh |
| [HADOOP-14835](https://issues.apache.org/jira/browse/HADOOP-14835) | mvn site build throws SAX errors |  Blocker | build, site | Allen Wittenauer | Andrew Wang |
| [MAPREDUCE-6960](https://issues.apache.org/jira/browse/MAPREDUCE-6960) | Shuffle Handler prints disk error stack traces for every read failure. |  Major | . | Kuhu Shukla | Kuhu Shukla |
| [HDFS-12480](https://issues.apache.org/jira/browse/HDFS-12480) | TestNameNodeMetrics#testTransactionAndCheckpointMetrics Fails in trunk |  Blocker | test | Brahma Reddy Battula | Hanisha Koneru |
| [HDFS-11799](https://issues.apache.org/jira/browse/HDFS-11799) | Introduce a config to allow setting up write pipeline with fewer nodes than replication factor |  Major | . | Yongjun Zhang | Brahma Reddy Battula |
| [HDFS-12437](https://issues.apache.org/jira/browse/HDFS-12437) | Fix test setup in TestLeaseRecoveryStriped |  Major | erasure-coding, test | Arpit Agarwal | Andrew Wang |
| [YARN-7196](https://issues.apache.org/jira/browse/YARN-7196) | Fix finicky TestContainerManager tests |  Major | . | Arun Suresh | Arun Suresh |
| [YARN-6771](https://issues.apache.org/jira/browse/YARN-6771) | Use classloader inside configuration class to make new classes |  Major | . | Jongyoul Lee | Jongyoul Lee |
| [HDFS-12526](https://issues.apache.org/jira/browse/HDFS-12526) | FSDirectory should use Time.monotonicNow for durations |  Minor | . | Chetna Chaudhari | Bharat Viswanadham |
| [YARN-6968](https://issues.apache.org/jira/browse/YARN-6968) | Hardcoded absolute pathname in DockerLinuxContainerRuntime |  Major | nodemanager | Miklos Szegedi | Eric Badger |
| [HDFS-12371](https://issues.apache.org/jira/browse/HDFS-12371) | "BlockVerificationFailures" and "BlocksVerified" show up as 0 in Datanode JMX |  Major | metrics | Sai Nukavarapu | Hanisha Koneru |
| [MAPREDUCE-6964](https://issues.apache.org/jira/browse/MAPREDUCE-6964) | BaileyBorweinPlouffe should use Time.monotonicNow for measuring durations |  Minor | examples | Chetna Chaudhari | Chetna Chaudhari |
| [YARN-6991](https://issues.apache.org/jira/browse/YARN-6991) | "Kill application" button does not show error if other user tries to kill the application for secure cluster |  Major | . | Sumana Sathish | Suma Shivaprasad |
| [YARN-7034](https://issues.apache.org/jira/browse/YARN-7034) | DefaultLinuxContainerRuntime and DockerLinuxContainerRuntime sends client environment variables to container-executor |  Blocker | nodemanager | Miklos Szegedi | Miklos Szegedi |
| [HDFS-12523](https://issues.apache.org/jira/browse/HDFS-12523) | Thread pools in ErasureCodingWorker do not shutdown |  Major | erasure-coding | Lei (Eddy) Xu | Huafeng Wang |
| [MAPREDUCE-6966](https://issues.apache.org/jira/browse/MAPREDUCE-6966) | DistSum should use Time.monotonicNow for measuring durations |  Minor | . | Chetna Chaudhari | Chetna Chaudhari |
| [MAPREDUCE-6965](https://issues.apache.org/jira/browse/MAPREDUCE-6965) | QuasiMonteCarlo should use Time.monotonicNow for measuring durations |  Minor | examples | Chetna Chaudhari | Chetna Chaudhari |
| [MAPREDUCE-6967](https://issues.apache.org/jira/browse/MAPREDUCE-6967) | gridmix/SleepReducer should use Time.monotonicNow for measuring durations |  Minor | . | Chetna Chaudhari | Chetna Chaudhari |
| [YARN-7153](https://issues.apache.org/jira/browse/YARN-7153) | Remove duplicated code in AMRMClientAsyncImpl.java |  Minor | client | Sen Zhao | Sen Zhao |
| [HADOOP-14897](https://issues.apache.org/jira/browse/HADOOP-14897) | Loosen compatibility guidelines for native dependencies |  Blocker | documentation, native | Chris Douglas | Daniel Templeton |
| [YARN-7118](https://issues.apache.org/jira/browse/YARN-7118) | AHS REST API can return NullPointerException |  Major | . | Prabhu Joseph | Billie Rinaldi |
| [HDFS-12495](https://issues.apache.org/jira/browse/HDFS-12495) | TestPendingInvalidateBlock#testPendingDeleteUnknownBlocks fails intermittently |  Major | . | Eric Badger | Eric Badger |
| [MAPREDUCE-6968](https://issues.apache.org/jira/browse/MAPREDUCE-6968) | Staging directory erasure coding config property has a typo |  Major | client | Jason Lowe | Jason Lowe |
| [HADOOP-14822](https://issues.apache.org/jira/browse/HADOOP-14822) | hadoop-project/pom.xml is executable |  Minor | . | Akira Ajisaka | Ajay Kumar |
| [YARN-7157](https://issues.apache.org/jira/browse/YARN-7157) | Add admin configuration to filter per-user's apps in secure cluster |  Major | webapp | Sunil Govindan | Sunil Govindan |
| [YARN-7253](https://issues.apache.org/jira/browse/YARN-7253) | Shared Cache Manager daemon command listed as admin subcmd in yarn script |  Trivial | . | Chris Trezzo | Chris Trezzo |
| [YARN-7257](https://issues.apache.org/jira/browse/YARN-7257) | AggregatedLogsBlock reports a bad 'end' value as a bad 'start' value |  Major | log-aggregation | Jason Lowe | Jason Lowe |
| [HDFS-12458](https://issues.apache.org/jira/browse/HDFS-12458) | TestReencryptionWithKMS fails regularly |  Major | encryption, test | Konstantin Shvachko | Xiao Chen |
| [HDFS-10576](https://issues.apache.org/jira/browse/HDFS-10576) | DiskBalancer followup work items |  Major | balancer & mover | Anu Engineer | Anu Engineer |
| [YARN-6625](https://issues.apache.org/jira/browse/YARN-6625) | yarn application -list returns a tracking URL for AM that doesn't work in secured and HA environment |  Major | amrmproxy | Yufei Gu | Yufei Gu |
| [YARN-7146](https://issues.apache.org/jira/browse/YARN-7146) | Many RM unit tests failing with FairScheduler |  Major | test | Robert Kanter | Robert Kanter |
| [YARN-4742](https://issues.apache.org/jira/browse/YARN-4742) | [Umbrella] Enhancements to Distributed Scheduling |  Major | . | Arun Suresh | Arun Suresh |
| [YARN-6102](https://issues.apache.org/jira/browse/YARN-6102) | RMActiveService context to be updated with new RMContext on failover |  Critical | . | Ajith S | Rohith Sharma K S |
| [HADOOP-14903](https://issues.apache.org/jira/browse/HADOOP-14903) | Add json-smart explicitly to pom.xml |  Major | common | Ray Chiang | Ray Chiang |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-5548](https://issues.apache.org/jira/browse/YARN-5548) | Use MockRMMemoryStateStore to reduce test failures |  Major | . | Bibin A Chundatt | Bibin A Chundatt |
| [HDFS-2319](https://issues.apache.org/jira/browse/HDFS-2319) | Add test cases for FSshell -stat |  Trivial | test | XieXianshan | Bharat Viswanadham |
| [HADOOP-14245](https://issues.apache.org/jira/browse/HADOOP-14245) | Use Mockito.when instead of Mockito.stub |  Minor | test | Akira Ajisaka | Andras Bokor |
| [YARN-5349](https://issues.apache.org/jira/browse/YARN-5349) | TestWorkPreservingRMRestart#testUAMRecoveryOnRMWorkPreservingRestart  fail intermittently |  Minor | . | sandflee | Jason Lowe |
| [HDFS-11988](https://issues.apache.org/jira/browse/HDFS-11988) | Verify HDFS Snapshots with open files captured are safe across truncates and appends on current version file |  Major | hdfs, snapshots | Manoj Govindassamy | Manoj Govindassamy |
| [HADOOP-14729](https://issues.apache.org/jira/browse/HADOOP-14729) | Upgrade JUnit 3 test cases to JUnit 4 |  Major | . | Akira Ajisaka | Ajay Kumar |
| [HDFS-11912](https://issues.apache.org/jira/browse/HDFS-11912) | Add a snapshot unit test with randomized file IO operations |  Minor | hdfs | George Huang | George Huang |
| [MAPREDUCE-6953](https://issues.apache.org/jira/browse/MAPREDUCE-6953) | Skip the testcase testJobWithChangePriority if FairScheduler is used |  Major | client | Peter Bacsko | Peter Bacsko |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-5300](https://issues.apache.org/jira/browse/YARN-5300) | Exclude generated federation protobuf sources from YARN Javadoc/findbugs build |  Minor | nodemanager, resourcemanager | Subru Krishnan | Subru Krishnan |
| [YARN-5156](https://issues.apache.org/jira/browse/YARN-5156) | YARN\_CONTAINER\_FINISHED of YARN\_CONTAINERs will always have running state |  Major | timelineserver | Li Lu | Vrushali C |
| [YARN-3662](https://issues.apache.org/jira/browse/YARN-3662) | Federation Membership State Store internal APIs |  Major | nodemanager, resourcemanager | Subru Krishnan | Subru Krishnan |
| [YARN-5229](https://issues.apache.org/jira/browse/YARN-5229) | Refactor #isApplicationEntity and #getApplicationEvent from HBaseTimelineWriterImpl |  Minor | timelineserver | Joep Rottinghuis | Vrushali C |
| [YARN-5406](https://issues.apache.org/jira/browse/YARN-5406) | In-memory based implementation of the FederationMembershipStateStore |  Major | nodemanager, resourcemanager | Subru Krishnan | Ellen Hui |
| [YARN-5390](https://issues.apache.org/jira/browse/YARN-5390) | Federation Subcluster Resolver |  Major | nodemanager, resourcemanager | Carlo Curino | Ellen Hui |
| [YARN-5307](https://issues.apache.org/jira/browse/YARN-5307) | Federation Application State Store internal APIs |  Major | nodemanager, resourcemanager | Subru Krishnan | Subru Krishnan |
| [YARN-3664](https://issues.apache.org/jira/browse/YARN-3664) | Federation PolicyStore internal APIs |  Major | nodemanager, resourcemanager | Subru Krishnan | Subru Krishnan |
| [YARN-5408](https://issues.apache.org/jira/browse/YARN-5408) | Compose Federation membership/application/policy APIs into an uber FederationStateStore API |  Major | nodemanager, resourcemanager | Subru Krishnan | Ellen Hui |
| [YARN-5407](https://issues.apache.org/jira/browse/YARN-5407) | In-memory based implementation of the FederationApplicationStateStore, FederationPolicyStateStore |  Major | nodemanager, resourcemanager | Subru Krishnan | Ellen Hui |
| [YARN-5519](https://issues.apache.org/jira/browse/YARN-5519) | Add SubClusterId in AddApplicationHomeSubClusterResponse for Router Failover |  Major | nodemanager, resourcemanager | Giovanni Matteo Fumarola | Ellen Hui |
| [YARN-3672](https://issues.apache.org/jira/browse/YARN-3672) | Create Facade for Federation State and Policy Store |  Major | nodemanager, resourcemanager | Subru Krishnan | Subru Krishnan |
| [YARN-5467](https://issues.apache.org/jira/browse/YARN-5467) | InputValidator for the FederationStateStore internal APIs |  Major | nodemanager, resourcemanager | Subru Krishnan | Giovanni Matteo Fumarola |
| [YARN-3673](https://issues.apache.org/jira/browse/YARN-3673) | Create a FailoverProxy for Federation services |  Major | nodemanager, resourcemanager | Subru Krishnan | Subru Krishnan |
| [YARN-3671](https://issues.apache.org/jira/browse/YARN-3671) | Integrate Federation services with ResourceManager |  Major | nodemanager, resourcemanager | Subru Krishnan | Subru Krishnan |
| [YARN-5612](https://issues.apache.org/jira/browse/YARN-5612) | Return SubClusterId in FederationStateStoreFacade#addApplicationHomeSubCluster for Router Failover |  Major | nodemanager, resourcemanager | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [YARN-5601](https://issues.apache.org/jira/browse/YARN-5601) | Make the RM epoch base value configurable |  Major | nodemanager, resourcemanager | Subru Krishnan | Subru Krishnan |
| [YARN-5323](https://issues.apache.org/jira/browse/YARN-5323) | Policies APIs (for Router and AMRMProxy policies) |  Major | nodemanager, resourcemanager | Carlo Curino | Carlo Curino |
| [YARN-5324](https://issues.apache.org/jira/browse/YARN-5324) | Stateless Federation router policies implementation |  Major | nodemanager, resourcemanager | Carlo Curino | Carlo Curino |
| [YARN-5325](https://issues.apache.org/jira/browse/YARN-5325) | Stateless ARMRMProxy policies implementation |  Major | nodemanager, resourcemanager | Carlo Curino | Carlo Curino |
| [YARN-5638](https://issues.apache.org/jira/browse/YARN-5638) | Introduce a collector timestamp to uniquely identify collectors creation order in collector discovery |  Major | timelineserver | Li Lu | Li Lu |
| [YARN-3649](https://issues.apache.org/jira/browse/YARN-3649) | Allow configurable prefix for hbase table names (like prod, exp, test etc) |  Major | timelineserver | Vrushali C | Vrushali C |
| [YARN-5715](https://issues.apache.org/jira/browse/YARN-5715) | introduce entity prefix for return and sort order |  Critical | timelineserver | Sangjin Lee | Rohith Sharma K S |
| [YARN-5391](https://issues.apache.org/jira/browse/YARN-5391) | PolicyManager to tie together Router/AMRM Federation policies |  Major | nodemanager, resourcemanager | Carlo Curino | Carlo Curino |
| [YARN-5265](https://issues.apache.org/jira/browse/YARN-5265) | Make HBase configuration for the timeline service configurable |  Major | timelineserver | Joep Rottinghuis | Joep Rottinghuis |
| [YARN-3359](https://issues.apache.org/jira/browse/YARN-3359) | Recover collector list when RM fails over |  Major | resourcemanager | Junping Du | Li Lu |
| [YARN-5634](https://issues.apache.org/jira/browse/YARN-5634) | Simplify initialization/use of RouterPolicy via a RouterPolicyFacade |  Major | nodemanager, resourcemanager | Carlo Curino | Carlo Curino |
| [YARN-5792](https://issues.apache.org/jira/browse/YARN-5792) | adopt the id prefix for YARN, MR, and DS entities |  Major | timelineserver | Sangjin Lee | Varun Saxena |
| [YARN-5676](https://issues.apache.org/jira/browse/YARN-5676) | Add a HashBasedRouterPolicy, and small policies and test refactoring. |  Major | nodemanager, resourcemanager | Carlo Curino | Carlo Curino |
| [YARN-5872](https://issues.apache.org/jira/browse/YARN-5872) | Add AlwayReject policies for router and amrmproxy. |  Major | nodemanager, resourcemanager | Carlo Curino | Carlo Curino |
| [YARN-5905](https://issues.apache.org/jira/browse/YARN-5905) | Update the RM webapp host that is reported as part of Federation membership to current primary RM's IP |  Minor | federation, resourcemanager | Subru Krishnan | Subru Krishnan |
| [YARN-5739](https://issues.apache.org/jira/browse/YARN-5739) | Provide timeline reader API to list available timeline entity types for one application |  Major | timelinereader | Li Lu | Li Lu |
| [MAPREDUCE-6818](https://issues.apache.org/jira/browse/MAPREDUCE-6818) | Remove direct reference to TimelineClientImpl |  Major | . | Li Lu | Li Lu |
| [YARN-5585](https://issues.apache.org/jira/browse/YARN-5585) | [Atsv2] Reader side changes for entity prefix and support for pagination via additional filters |  Critical | timelinereader | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-5980](https://issues.apache.org/jira/browse/YARN-5980) | Update documentation for single node hbase deploy |  Major | timelineserver | Vrushali C | Vrushali C |
| [YARN-5378](https://issues.apache.org/jira/browse/YARN-5378) | Accommodate app-id-\>cluster mapping |  Major | timelineserver | Joep Rottinghuis | Sangjin Lee |
| [YARN-6064](https://issues.apache.org/jira/browse/YARN-6064) | Support fromId for flowRuns and flow/flowRun apps REST API's |  Major | timelinereader | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-6094](https://issues.apache.org/jira/browse/YARN-6094) | Update the coprocessor to be a dynamically loaded one |  Major | timelineserver | Vrushali C | Vrushali C |
| [YARN-5410](https://issues.apache.org/jira/browse/YARN-5410) | Bootstrap Router server module |  Major | nodemanager, resourcemanager | Subru Krishnan | Giovanni Matteo Fumarola |
| [YARN-6253](https://issues.apache.org/jira/browse/YARN-6253) | FlowAcitivityColumnPrefix.store(byte[] rowKey, ...) drops timestamp |  Major | . | Haibo Chen | Haibo Chen |
| [YARN-6190](https://issues.apache.org/jira/browse/YARN-6190) | Validation and synchronization fixes in LocalityMulticastAMRMProxyPolicy |  Minor | federation | Botong Huang | Botong Huang |
| [YARN-6027](https://issues.apache.org/jira/browse/YARN-6027) | Support fromid(offset) filter for /flows API |  Major | timelineserver | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-6247](https://issues.apache.org/jira/browse/YARN-6247) | Share a single instance of SubClusterResolver instead of instantiating one per AM |  Minor | . | Botong Huang | Botong Huang |
| [YARN-6256](https://issues.apache.org/jira/browse/YARN-6256) | Add FROM\_ID info key for timeline entities in reader response. |  Major | timelineserver | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-6237](https://issues.apache.org/jira/browse/YARN-6237) | Move UID constant to TimelineReaderUtils |  Major | timelinereader | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-6318](https://issues.apache.org/jira/browse/YARN-6318) | timeline service schema creator fails if executed from a remote machine |  Minor | timelineserver | Sangjin Lee | Sangjin Lee |
| [YARN-6146](https://issues.apache.org/jira/browse/YARN-6146) | Add Builder methods for TimelineEntityFilters |  Major | timelineserver | Rohith Sharma K S | Haibo Chen |
| [YARN-5602](https://issues.apache.org/jira/browse/YARN-5602) | Utils for Federation State and Policy Store |  Major | nodemanager, resourcemanager | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [YARN-6203](https://issues.apache.org/jira/browse/YARN-6203) | Occasional test failure in TestWeightedRandomRouterPolicy |  Minor | federation | Botong Huang | Carlo Curino |
| [YARN-3663](https://issues.apache.org/jira/browse/YARN-3663) | Federation State and Policy Store (DBMS implementation) |  Major | nodemanager, resourcemanager | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [YARN-5411](https://issues.apache.org/jira/browse/YARN-5411) | Create a proxy chain for ApplicationClientProtocol in the Router |  Major | nodemanager, resourcemanager | Subru Krishnan | Giovanni Matteo Fumarola |
| [YARN-5413](https://issues.apache.org/jira/browse/YARN-5413) | Create a proxy chain for ResourceManager Admin API in the Router |  Major | nodemanager, resourcemanager | Subru Krishnan | Giovanni Matteo Fumarola |
| [YARN-5531](https://issues.apache.org/jira/browse/YARN-5531) | UnmanagedAM pool manager for federating application across clusters |  Major | nodemanager, resourcemanager | Subru Krishnan | Botong Huang |
| [YARN-6666](https://issues.apache.org/jira/browse/YARN-6666) | Fix unit test failure in TestRouterClientRMService |  Minor | . | Botong Huang | Botong Huang |
| [YARN-6484](https://issues.apache.org/jira/browse/YARN-6484) | [Documentation] Documenting the YARN Federation feature |  Major | nodemanager, resourcemanager | Subru Krishnan | Carlo Curino |
| [YARN-6658](https://issues.apache.org/jira/browse/YARN-6658) | Remove columnFor() methods of Columns in HBaseTimeline backend |  Major | . | Haibo Chen | Haibo Chen |
| [YARN-3666](https://issues.apache.org/jira/browse/YARN-3666) | Federation Intercepting and propagating AM- home RM communications |  Major | nodemanager, resourcemanager | Kishore Chaliparambil | Botong Huang |
| [YARN-5647](https://issues.apache.org/jira/browse/YARN-5647) | [ATSv2 Security] Collector side changes for loading auth filters and principals |  Major | timelineserver | Varun Saxena | Varun Saxena |
| [YARN-6511](https://issues.apache.org/jira/browse/YARN-6511) | Federation: transparently spanning application across multiple sub-clusters |  Major | . | Botong Huang | Botong Huang |
| [YARN-6638](https://issues.apache.org/jira/browse/YARN-6638) | [ATSv2 Security] Timeline reader side changes for loading auth filters and principals |  Major | timelineserver | Varun Saxena | Varun Saxena |
| [YARN-6724](https://issues.apache.org/jira/browse/YARN-6724) | Add ability to blacklist sub-clusters when invoking Routing policies |  Major | router | Subru Krishnan | Giovanni Matteo Fumarola |
| [YARN-5648](https://issues.apache.org/jira/browse/YARN-5648) | [ATSv2 Security] Client side changes for authentication |  Major | timelineserver | Varun Saxena | Varun Saxena |
| [YARN-3659](https://issues.apache.org/jira/browse/YARN-3659) | Federation: routing client invocations transparently to multiple RMs |  Major | client, resourcemanager, router | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [YARN-5067](https://issues.apache.org/jira/browse/YARN-5067) | Support specifying resources for AM containers in SLS |  Major | scheduler-load-simulator | Wangda Tan | Yufei Gu |
| [YARN-6681](https://issues.apache.org/jira/browse/YARN-6681) | Eliminate double-copy of child queues in canAssignToThisQueue |  Major | resourcemanager | Daryn Sharp | Daryn Sharp |
| [HADOOP-14615](https://issues.apache.org/jira/browse/HADOOP-14615) | Add ServiceOperations.stopQuietly that accept slf4j logger API |  Major | . | Wenxin He | Wenxin He |
| [HADOOP-14617](https://issues.apache.org/jira/browse/HADOOP-14617) | Add ReflectionUtils.logThreadInfo that accept slf4j logger API |  Major | . | Wenxin He | Wenxin He |
| [HADOOP-14571](https://issues.apache.org/jira/browse/HADOOP-14571) | Deprecate public APIs relate to log4j1 |  Major | . | Akira Ajisaka | Wenxin He |
| [HADOOP-14587](https://issues.apache.org/jira/browse/HADOOP-14587) | Use GenericTestUtils.setLogLevel when available in hadoop-common |  Major | . | Wenxin He | Wenxin He |
| [YARN-6776](https://issues.apache.org/jira/browse/YARN-6776) | Refactor ApplicaitonMasterService to move actual processing logic to a separate class |  Minor | . | Arun Suresh | Arun Suresh |
| [HADOOP-14638](https://issues.apache.org/jira/browse/HADOOP-14638) | Replace commons-logging APIs with slf4j in StreamPumper |  Major | . | Wenxin He | Wenxin He |
| [YARN-6801](https://issues.apache.org/jira/browse/YARN-6801) | NPE in RM while setting collectors map in NodeHeartbeatResponse |  Major | timelineserver | Vrushali C | Vrushali C |
| [YARN-6807](https://issues.apache.org/jira/browse/YARN-6807) | Adding required missing configs to Federation configuration guide based on e2e testing |  Major | documentation, federation | Subru Krishnan | Tanuj Nayak |
| [YARN-6815](https://issues.apache.org/jira/browse/YARN-6815) | [Bug] FederationStateStoreFacade return behavior should be consistent irrespective of whether caching is enabled or not |  Major | nodemanager, resourcemanager | Subru Krishnan | Subru Krishnan |
| [YARN-6821](https://issues.apache.org/jira/browse/YARN-6821) | Move FederationStateStore SQL DDL files from test resource to sbin |  Major | nodemanager, resourcemanager | Subru Krishnan | Subru Krishnan |
| [YARN-6706](https://issues.apache.org/jira/browse/YARN-6706) | Refactor ContainerScheduler to make oversubscription change easier |  Major | . | Haibo Chen | Haibo Chen |
| [HADOOP-14642](https://issues.apache.org/jira/browse/HADOOP-14642) | wasb: add support for caching Authorization and SASKeys |  Major | fs/azure | Sivaguru Sankaridurg | Sivaguru Sankaridurg |
| [YARN-6777](https://issues.apache.org/jira/browse/YARN-6777) | Support for ApplicationMasterService processing chain of interceptors |  Major | . | Arun Suresh | Arun Suresh |
| [YARN-6775](https://issues.apache.org/jira/browse/YARN-6775) | CapacityScheduler: Improvements to assignContainers, avoid unnecessary canAssignToUser/Queue calls |  Major | capacityscheduler | Nathan Roberts | Nathan Roberts |
| [YARN-4455](https://issues.apache.org/jira/browse/YARN-4455) | Support fetching metrics by time range |  Major | timelineserver | Varun Saxena | Varun Saxena |
| [YARN-6685](https://issues.apache.org/jira/browse/YARN-6685) | Add job count in to SLS JSON input format |  Major | scheduler-load-simulator | Yufei Gu | Yufei Gu |
| [YARN-6850](https://issues.apache.org/jira/browse/YARN-6850) | Ensure that supplemented timestamp is stored only for flow run metrics |  Major | timelineserver | Vrushali C | Varun Saxena |
| [YARN-6733](https://issues.apache.org/jira/browse/YARN-6733) | Add table for storing sub-application entities |  Major | timelineserver | Vrushali C | Vrushali C |
| [HADOOP-14518](https://issues.apache.org/jira/browse/HADOOP-14518) | Customize User-Agent header sent in HTTP/HTTPS requests by WASB. |  Minor | fs/azure | Georgi Chalakov | Georgi Chalakov |
| [YARN-6804](https://issues.apache.org/jira/browse/YARN-6804) | Allow custom hostname for docker containers in native services |  Major | yarn-native-services | Billie Rinaldi | Billie Rinaldi |
| [YARN-6866](https://issues.apache.org/jira/browse/YARN-6866) | Minor clean-up and fixes in anticipation of YARN-2915 merge with trunk |  Major | federation | Subru Krishnan | Botong Huang |
| [YARN-5412](https://issues.apache.org/jira/browse/YARN-5412) | Create a proxy chain for ResourceManager REST API in the Router |  Major | nodemanager, resourcemanager | Subru Krishnan | Giovanni Matteo Fumarola |
| [HADOOP-11875](https://issues.apache.org/jira/browse/HADOOP-11875) | [JDK9] Add a second copy of Hamlet without \_ as a one-character identifier |  Major | . | Tsuyoshi Ozawa | Akira Ajisaka |
| [YARN-6888](https://issues.apache.org/jira/browse/YARN-6888) | Refactor AppLevelTimelineCollector such that RM does not have aggregator threads created |  Major | timelineserver | Vrushali C | Vrushali C |
| [HADOOP-14678](https://issues.apache.org/jira/browse/HADOOP-14678) | AdlFilesystem#initialize swallows exception when getting user name |  Minor | fs/adl | John Zhuge | John Zhuge |
| [YARN-6734](https://issues.apache.org/jira/browse/YARN-6734) | Ensure sub-application user is extracted & sent to timeline service |  Major | timelineserver | Vrushali C | Rohith Sharma K S |
| [YARN-6902](https://issues.apache.org/jira/browse/YARN-6902) | Update Microsoft JDBC Driver for SQL Server version in License.txt |  Minor | federation | Botong Huang | Botong Huang |
| [HADOOP-14672](https://issues.apache.org/jira/browse/HADOOP-14672) | Shaded Hadoop-client-minicluster include unshaded classes, like: javax, sax, dom, etc. |  Blocker | . | Junping Du | Bharat Viswanadham |
| [HADOOP-14397](https://issues.apache.org/jira/browse/HADOOP-14397) | Pull up the builder pattern to FileSystem and add AbstractContractCreateTest for it |  Major | common, fs, hdfs-client | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HDFS-12151](https://issues.apache.org/jira/browse/HDFS-12151) | Hadoop 2 clients cannot writeBlock to Hadoop 3 DataNodes |  Major | rolling upgrades | Sean Mackrory | Sean Mackrory |
| [HADOOP-14495](https://issues.apache.org/jira/browse/HADOOP-14495) | Add set options interface to FSDataOutputStreamBuilder |  Major | fs | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [YARN-6853](https://issues.apache.org/jira/browse/YARN-6853) | Add MySql Scripts for FederationStateStore |  Major | . | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [HDFS-9388](https://issues.apache.org/jira/browse/HDFS-9388) | Refactor decommission related code to support maintenance state for datanodes |  Major | . | Ming Ma | Manoj Govindassamy |
| [YARN-6674](https://issues.apache.org/jira/browse/YARN-6674) | Add memory cgroup settings for opportunistic containers |  Major | nodemanager | Haibo Chen | Miklos Szegedi |
| [YARN-6673](https://issues.apache.org/jira/browse/YARN-6673) | Add cpu cgroup configurations for opportunistic containers |  Major | . | Haibo Chen | Miklos Szegedi |
| [YARN-5977](https://issues.apache.org/jira/browse/YARN-5977) | ContainerManagementProtocol changes to support change of container ExecutionType |  Major | . | Arun Suresh | kartheek muthyala |
| [HADOOP-14126](https://issues.apache.org/jira/browse/HADOOP-14126) | remove jackson, joda and other transient aws SDK dependencies from hadoop-aws |  Minor | build, fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-14685](https://issues.apache.org/jira/browse/HADOOP-14685) | Test jars to exclude from hadoop-client-minicluster jar |  Major | common | Bharat Viswanadham | Bharat Viswanadham |
| [YARN-6957](https://issues.apache.org/jira/browse/YARN-6957) | Moving logging APIs over to slf4j in hadoop-yarn-server-sharedcachemanager |  Major | . | Yeliang Cang | Yeliang Cang |
| [YARN-6955](https://issues.apache.org/jira/browse/YARN-6955) | Handle concurrent register AM requests in FederationInterceptor |  Minor | . | Botong Huang | Botong Huang |
| [YARN-6873](https://issues.apache.org/jira/browse/YARN-6873) | Moving logging APIs over to slf4j in hadoop-yarn-server-applicationhistoryservice |  Major | . | Yeliang Cang | Yeliang Cang |
| [YARN-6897](https://issues.apache.org/jira/browse/YARN-6897) | Refactoring RMWebServices by moving some util methods to RMWebAppUtil |  Major | . | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [HADOOP-14715](https://issues.apache.org/jira/browse/HADOOP-14715) | TestWasbRemoteCallHelper failing |  Major | fs/azure, test | Steve Loughran | Esfandiar Manii |
| [YARN-6970](https://issues.apache.org/jira/browse/YARN-6970) | Add PoolInitializationException as retriable exception in FederationFacade |  Major | federation | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [HDFS-11975](https://issues.apache.org/jira/browse/HDFS-11975) | Provide a system-default EC policy |  Major | erasure-coding | Lei (Eddy) Xu | luhuichun |
| [HADOOP-14628](https://issues.apache.org/jira/browse/HADOOP-14628) | Upgrade maven enforcer plugin to 3.0.0-M1 |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-14355](https://issues.apache.org/jira/browse/HADOOP-14355) | Update maven-war-plugin to 3.1.0 |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [YARN-6631](https://issues.apache.org/jira/browse/YARN-6631) | Refactor loader.js in new Yarn UI |  Major | . | Akhil PB | Akhil PB |
| [YARN-6874](https://issues.apache.org/jira/browse/YARN-6874) | Supplement timestamp for min start/max end time columns in flow run table to avoid overwrite |  Major | timelineserver | Varun Saxena | Vrushali C |
| [YARN-6958](https://issues.apache.org/jira/browse/YARN-6958) | Moving logging APIs over to slf4j in hadoop-yarn-server-timelineservice |  Major | . | Yeliang Cang | Yeliang Cang |
| [HADOOP-14183](https://issues.apache.org/jira/browse/HADOOP-14183) | Remove service loader config file for wasb fs |  Minor | fs/azure | John Zhuge | Esfandiar Manii |
| [YARN-6130](https://issues.apache.org/jira/browse/YARN-6130) | [ATSv2 Security] Generate a delegation token for AM when app collector is created and pass it to AM via NM and RM |  Major | timelineserver | Varun Saxena | Varun Saxena |
| [YARN-6133](https://issues.apache.org/jira/browse/YARN-6133) | [ATSv2 Security] Renew delegation token for app automatically if an app collector is active |  Major | timelineserver | Varun Saxena | Varun Saxena |
| [HADOOP-10392](https://issues.apache.org/jira/browse/HADOOP-10392) | Use FileSystem#makeQualified(Path) instead of Path#makeQualified(FileSystem) |  Minor | fs | Akira Ajisaka | Akira Ajisaka |
| [YARN-6820](https://issues.apache.org/jira/browse/YARN-6820) | Restrict read access to timelineservice v2 data |  Major | timelinereader | Vrushali C | Vrushali C |
| [YARN-6896](https://issues.apache.org/jira/browse/YARN-6896) | Federation: routing REST invocations transparently to multiple RMs (part 1 - basic execution) |  Major | . | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [YARN-6687](https://issues.apache.org/jira/browse/YARN-6687) | Validate that the duration of the periodic reservation is less than the periodicity |  Major | reservation system | Subru Krishnan | Subru Krishnan |
| [YARN-6905](https://issues.apache.org/jira/browse/YARN-6905) | Multiple HBaseTimelineStorage test failures due to missing FastNumberFormat |  Major | timelineserver | Sonia Garudi | Haibo Chen |
| [YARN-5978](https://issues.apache.org/jira/browse/YARN-5978) | ContainerScheduler and ContainerManager changes to support ExecType update |  Major | . | Arun Suresh | kartheek muthyala |
| [YARN-5146](https://issues.apache.org/jira/browse/YARN-5146) | Support for Fair Scheduler in new YARN UI |  Major | . | Wangda Tan | Abdullah Yousufi |
| [YARN-6741](https://issues.apache.org/jira/browse/YARN-6741) | Deleting all children of a Parent Queue on refresh throws exception |  Major | capacity scheduler | Naganarasimha G R | Naganarasimha G R |
| [HADOOP-14660](https://issues.apache.org/jira/browse/HADOOP-14660) | wasb: improve throughput by 34% when account limit exceeded |  Major | fs/azure | Thomas Marquardt | Thomas Marquardt |
| [YARN-7006](https://issues.apache.org/jira/browse/YARN-7006) | [ATSv2 Security] Changes for authentication for CollectorNodemanagerProtocol |  Major | timelineserver | Varun Saxena | Varun Saxena |
| [YARN-6900](https://issues.apache.org/jira/browse/YARN-6900) | ZooKeeper based implementation of the FederationStateStore |  Major | federation, nodemanager, resourcemanager | Subru Krishnan | Íñigo Goiri |
| [HDFS-11082](https://issues.apache.org/jira/browse/HDFS-11082) | Provide replicated EC policy to replicate files |  Critical | erasure-coding | Rakesh R | Sammi Chen |
| [YARN-6988](https://issues.apache.org/jira/browse/YARN-6988) | container-executor fails for docker when command length \> 4096 B |  Major | yarn | Eric Badger | Eric Badger |
| [YARN-7038](https://issues.apache.org/jira/browse/YARN-7038) | [Atsv2 Security] CollectorNodemanagerProtocol RPC interface doesn't work when service authorization is enabled |  Major | . | Rohith Sharma K S | Varun Saxena |
| [HADOOP-14769](https://issues.apache.org/jira/browse/HADOOP-14769) | WASB: delete recursive should not fail if a file is deleted |  Major | fs/azure | Thomas Marquardt | Thomas Marquardt |
| [HADOOP-14398](https://issues.apache.org/jira/browse/HADOOP-14398) | Modify documents for the FileSystem Builder API |  Major | fs | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [YARN-7041](https://issues.apache.org/jira/browse/YARN-7041) | Nodemanager NPE running jobs with security off |  Major | timelineserver | Aaron Gresch | Varun Saxena |
| [YARN-6134](https://issues.apache.org/jira/browse/YARN-6134) | [ATSv2 Security] Regenerate delegation token for app just before token expires if app collector is active |  Major | timelineserver | Varun Saxena | Varun Saxena |
| [YARN-6979](https://issues.apache.org/jira/browse/YARN-6979) | Add flag to notify all types of container updates to NM via NodeHeartbeatResponse |  Major | . | Arun Suresh | kartheek muthyala |
| [HADOOP-14194](https://issues.apache.org/jira/browse/HADOOP-14194) | Aliyun OSS should not use empty endpoint as default |  Major | fs/oss | Mingliang Liu | Genmao Yu |
| [YARN-6861](https://issues.apache.org/jira/browse/YARN-6861) | Reader API for sub application entities |  Major | timelinereader | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-6323](https://issues.apache.org/jira/browse/YARN-6323) | Rolling upgrade/config change is broken on timeline v2. |  Major | timelineserver | Li Lu | Vrushali C |
| [YARN-6047](https://issues.apache.org/jira/browse/YARN-6047) | Documentation updates for TimelineService v2 |  Major | documentation, timelineserver | Varun Saxena | Rohith Sharma K S |
| [MAPREDUCE-6838](https://issues.apache.org/jira/browse/MAPREDUCE-6838) | [ATSv2 Security] Add timeline delegation token received in allocate response to UGI |  Major | . | Varun Saxena | Varun Saxena |
| [YARN-5603](https://issues.apache.org/jira/browse/YARN-5603) | Metrics for Federation StateStore |  Major | . | Subru Krishnan | Ellen Hui |
| [YARN-6923](https://issues.apache.org/jira/browse/YARN-6923) | Metrics for Federation Router |  Major | . | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [HADOOP-14787](https://issues.apache.org/jira/browse/HADOOP-14787) | AliyunOSS: Implement the \`createNonRecursive\` operator |  Major | fs, fs/oss | Genmao Yu | Genmao Yu |
| [HADOOP-14649](https://issues.apache.org/jira/browse/HADOOP-14649) | Update aliyun-sdk-oss version to 2.8.1 |  Major | fs/oss | Ray Chiang | Genmao Yu |
| [YARN-7047](https://issues.apache.org/jira/browse/YARN-7047) | Moving logging APIs over to slf4j in hadoop-yarn-server-nodemanager |  Major | . | Yeliang Cang | Yeliang Cang |
| [YARN-6876](https://issues.apache.org/jira/browse/YARN-6876) | Create an abstract log writer for extendability |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-6386](https://issues.apache.org/jira/browse/YARN-6386) | Show decommissioning nodes in new YARN UI |  Major | webapp | Elek, Marton | Elek, Marton |
| [YARN-7010](https://issues.apache.org/jira/browse/YARN-7010) | Federation: routing REST invocations transparently to multiple RMs (part 2 - getApps) |  Major | . | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [YARN-5219](https://issues.apache.org/jira/browse/YARN-5219) | When an export var command fails in launch\_container.sh, the full container launch should fail |  Major | . | Hitesh Shah | Sunil Govindan |
| [HADOOP-14802](https://issues.apache.org/jira/browse/HADOOP-14802) | Add support for using container saskeys for all accesses |  Major | fs/azure | Sivaguru Sankaridurg | Sivaguru Sankaridurg |
| [YARN-7094](https://issues.apache.org/jira/browse/YARN-7094) | Document the current known issue with server-side NM graceful decom |  Blocker | graceful | Robert Kanter | Robert Kanter |
| [YARN-7095](https://issues.apache.org/jira/browse/YARN-7095) | Federation: routing getNode/getNodes/getMetrics REST invocations transparently to multiple RMs |  Major | . | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [YARN-6726](https://issues.apache.org/jira/browse/YARN-6726) | Fix issues with docker commands executed by container-executor |  Major | nodemanager | Shane Kumpf | Shane Kumpf |
| [YARN-6877](https://issues.apache.org/jira/browse/YARN-6877) | Create an abstract log reader for extendability |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-7071](https://issues.apache.org/jira/browse/YARN-7071) | Add vcores and number of containers in new YARN UI node heat map |  Major | yarn-ui-v2 | Abdullah Yousufi | Abdullah Yousufi |
| [YARN-7075](https://issues.apache.org/jira/browse/YARN-7075) | Better styling for donut charts in new YARN UI |  Major | . | Da Ding | Da Ding |
| [HADOOP-14103](https://issues.apache.org/jira/browse/HADOOP-14103) | Sort out hadoop-aws contract-test-options.xml |  Minor | fs/s3, test | Steve Loughran | John Zhuge |
| [YARN-7148](https://issues.apache.org/jira/browse/YARN-7148) | TestLogsCLI fails in trunk and branch-2 and javadoc error |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-6978](https://issues.apache.org/jira/browse/YARN-6978) | Add updateContainer API to NMClient. |  Major | . | Arun Suresh | kartheek muthyala |
| [HADOOP-14774](https://issues.apache.org/jira/browse/HADOOP-14774) | S3A case "testRandomReadOverBuffer" failed due to improper range parameter |  Minor | fs/s3 | Yonger | Yonger |
| [YARN-7144](https://issues.apache.org/jira/browse/YARN-7144) | Log Aggregation controller should not swallow the exceptions when it calls closeWriter and closeReader. |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-7104](https://issues.apache.org/jira/browse/YARN-7104) | Improve Nodes Heatmap in new YARN UI with better color coding |  Major | . | Da Ding | Da Ding |
| [HADOOP-13421](https://issues.apache.org/jira/browse/HADOOP-13421) | Switch to v2 of the S3 List Objects API in S3A |  Minor | fs/s3 | Steven K. Wong | Aaron Fabbri |
| [YARN-6600](https://issues.apache.org/jira/browse/YARN-6600) | Introduce default and max lifetime of application at LeafQueue level |  Major | capacity scheduler | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-6849](https://issues.apache.org/jira/browse/YARN-6849) | NMContainerStatus should have the Container ExecutionType. |  Major | . | Arun Suresh | kartheek muthyala |
| [YARN-7128](https://issues.apache.org/jira/browse/YARN-7128) | The error message in TimelineSchemaCreator is not enough to find out the error. |  Major | timelineserver | Jinjiang Ling | Jinjiang Ling |
| [YARN-7173](https://issues.apache.org/jira/browse/YARN-7173) | Container update RM-NM communication fix for backward compatibility |  Major | . | Arun Suresh | Arun Suresh |
| [HADOOP-14798](https://issues.apache.org/jira/browse/HADOOP-14798) | Update sshd-core and related mina-core library versions |  Major | . | Ray Chiang | Ray Chiang |
| [HADOOP-14796](https://issues.apache.org/jira/browse/HADOOP-14796) | Update json-simple version to 1.1.1 |  Major | . | Ray Chiang | Ray Chiang |
| [HADOOP-14648](https://issues.apache.org/jira/browse/HADOOP-14648) | Bump commons-configuration2 to 2.1.1 |  Major | . | Ray Chiang | Ray Chiang |
| [HADOOP-14653](https://issues.apache.org/jira/browse/HADOOP-14653) | Update joda-time version to 2.9.9 |  Major | . | Ray Chiang | Ray Chiang |
| [HADOOP-14797](https://issues.apache.org/jira/browse/HADOOP-14797) | Update re2j version to 1.1 |  Major | . | Ray Chiang | Ray Chiang |
| [HADOOP-14857](https://issues.apache.org/jira/browse/HADOOP-14857) | Fix downstream shaded client integration test |  Blocker | . | Sean Busbey | Sean Busbey |
| [HADOOP-14089](https://issues.apache.org/jira/browse/HADOOP-14089) | Automated checking for malformed client artifacts. |  Blocker | . | David Phillips | Sean Busbey |
| [YARN-7162](https://issues.apache.org/jira/browse/YARN-7162) | Remove XML excludes file format |  Blocker | graceful | Robert Kanter | Robert Kanter |
| [HADOOP-14553](https://issues.apache.org/jira/browse/HADOOP-14553) | Add (parallelized) integration tests to hadoop-azure |  Major | fs/azure | Steve Loughran | Steve Loughran |
| [YARN-7174](https://issues.apache.org/jira/browse/YARN-7174) | Add retry logic in LogsCLI when fetch running application logs |  Major | . | Xuan Gong | Xuan Gong |
| [HDFS-12470](https://issues.apache.org/jira/browse/HDFS-12470) | DiskBalancer: Some tests create plan files under system directory |  Major | diskbalancer, test | Akira Ajisaka | Hanisha Koneru |
| [HADOOP-14583](https://issues.apache.org/jira/browse/HADOOP-14583) | wasb throws an exception if you try to create a file and there's no parent directory |  Minor | fs/azure | Steve Loughran | Esfandiar Manii |
| [HDFS-12473](https://issues.apache.org/jira/browse/HDFS-12473) | Change hosts JSON file format |  Major | . | Ming Ma | Ming Ma |
| [HDFS-11035](https://issues.apache.org/jira/browse/HDFS-11035) | Better documentation for maintenace mode and upgrade domain |  Major | datanode, documentation | Wei-Chiu Chuang | Ming Ma |
| [YARN-4266](https://issues.apache.org/jira/browse/YARN-4266) | Allow users to enter containers as UID:GID pair instead of by username |  Major | yarn | Sidharta Seethana | luhuichun |
| [MAPREDUCE-6947](https://issues.apache.org/jira/browse/MAPREDUCE-6947) |  Moving logging APIs over to slf4j in hadoop-mapreduce-examples |  Major | . | Gergely Novák | Gergely Novák |
| [HADOOP-14799](https://issues.apache.org/jira/browse/HADOOP-14799) | Update nimbus-jose-jwt to 4.41.1 |  Major | . | Ray Chiang | Ray Chiang |
| [HADOOP-14892](https://issues.apache.org/jira/browse/HADOOP-14892) | MetricsSystemImpl should use Time.monotonicNow for measuring durations |  Minor | . | Chetna Chaudhari | Chetna Chaudhari |
| [HADOOP-14881](https://issues.apache.org/jira/browse/HADOOP-14881) | LoadGenerator should use Time.monotonicNow() to measure durations |  Major | . | Chetna Chaudhari | Bharat Viswanadham |
| [HADOOP-14893](https://issues.apache.org/jira/browse/HADOOP-14893) | WritableRpcEngine should use Time.monotonicNow |  Minor | . | Chetna Chaudhari | Chetna Chaudhari |
| [HADOOP-14890](https://issues.apache.org/jira/browse/HADOOP-14890) | Move up to AWS SDK 1.11.199 |  Blocker | build, fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-12386](https://issues.apache.org/jira/browse/HDFS-12386) | Add fsserver defaults call to WebhdfsFileSystem. |  Minor | webhdfs | Rushabh S Shah | Rushabh S Shah |
| [YARN-6691](https://issues.apache.org/jira/browse/YARN-6691) | Update YARN daemon startup/shutdown scripts to include Router service |  Major | nodemanager, resourcemanager | Subru Krishnan | Giovanni Matteo Fumarola |
| [HADOOP-14220](https://issues.apache.org/jira/browse/HADOOP-14220) | Enhance S3GuardTool with bucket-info and set-capacity commands, tests |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-6033](https://issues.apache.org/jira/browse/YARN-6033) | Add support for sections in container-executor configuration file |  Major | nodemanager | Varun Vasudev | Varun Vasudev |
| [YARN-6852](https://issues.apache.org/jira/browse/YARN-6852) | [YARN-6223] Native code changes to support isolate GPU devices by using CGroups |  Major | . | Wangda Tan | Wangda Tan |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-6969](https://issues.apache.org/jira/browse/YARN-6969) | Clean up unused code in class FairSchedulerQueueInfo |  Trivial | fairscheduler | Yufei Gu | Larry Lo |
| [YARN-6622](https://issues.apache.org/jira/browse/YARN-6622) | Document Docker work as experimental |  Blocker | documentation | Varun Vasudev | Varun Vasudev |
| [YARN-7203](https://issues.apache.org/jira/browse/YARN-7203) | Add container ExecutionType into ContainerReport |  Minor | . | Botong Huang | Botong Huang |


