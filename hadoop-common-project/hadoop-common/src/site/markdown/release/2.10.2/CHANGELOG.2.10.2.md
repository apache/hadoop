
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

## Release 2.10.2 - 2022-05-31



### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-17338](https://issues.apache.org/jira/browse/HADOOP-17338) | Intermittent S3AInputStream failures: Premature end of Content-Length delimited message body etc |  Major | fs/s3 | Yongjun Zhang | Yongjun Zhang |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-12077](https://issues.apache.org/jira/browse/HADOOP-12077) | Provide a multi-URI replication Inode for ViewFs |  Major | fs | Gera Shegalov | Gera Shegalov |
| [HADOOP-13055](https://issues.apache.org/jira/browse/HADOOP-13055) | Implement linkMergeSlash and linkFallback for ViewFileSystem |  Major | fs, viewfs | Zhe Zhang | Manoj Govindassamy |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-13722](https://issues.apache.org/jira/browse/HADOOP-13722) | Code cleanup -- ViewFileSystem and InodeTree |  Minor | viewfs | Manoj Govindassamy | Manoj Govindassamy |
| [YARN-1151](https://issues.apache.org/jira/browse/YARN-1151) | Ability to configure auxiliary services from HDFS-based JAR files |  Major | nodemanager | john lilley | Xuan Gong |
| [HADOOP-15584](https://issues.apache.org/jira/browse/HADOOP-15584) | move httpcomponents version in pom.xml |  Minor | build | Brandon Scheller | Brandon Scheller |
| [HADOOP-16208](https://issues.apache.org/jira/browse/HADOOP-16208) | Do Not Log InterruptedException in Client |  Minor | common | David Mollitor | David Mollitor |
| [HADOOP-16052](https://issues.apache.org/jira/browse/HADOOP-16052) | Remove Subversion and Forrest from Dockerfile |  Minor | build | Akira Ajisaka | Xieming Li |
| [YARN-10036](https://issues.apache.org/jira/browse/YARN-10036) | Install yarnpkg and upgrade nodejs in Dockerfile |  Major | buid, yarn-ui-v2 | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-16811](https://issues.apache.org/jira/browse/HADOOP-16811) | Use JUnit TemporaryFolder Rule in TestFileUtils |  Minor | common, test | David Mollitor | David Mollitor |
| [HADOOP-16054](https://issues.apache.org/jira/browse/HADOOP-16054) | Update Dockerfile to use Bionic |  Major | build, test | Akira Ajisaka | Akira Ajisaka |
| [YARN-10451](https://issues.apache.org/jira/browse/YARN-10451) | RM (v1) UI NodesPage can NPE when yarn.io/gpu resource type is defined. |  Major | . | Eric Payne | Eric Payne |
| [YARN-9667](https://issues.apache.org/jira/browse/YARN-9667) | Container-executor.c duplicates messages to stdout |  Major | nodemanager, yarn | Adam Antal | Peter Bacsko |
| [MAPREDUCE-7301](https://issues.apache.org/jira/browse/MAPREDUCE-7301) | Expose Mini MR Cluster attribute for testing |  Minor | test | Swaroopa Kadam | Swaroopa Kadam |
| [HDFS-15567](https://issues.apache.org/jira/browse/HDFS-15567) | [SBN Read] HDFS should expose msync() API to allow downstream applications call it explicitly. |  Major | ha, hdfs-client | Konstantin Shvachko | Konstantin Shvachko |
| [YARN-10450](https://issues.apache.org/jira/browse/YARN-10450) | Add cpu and memory utilization per node and cluster-wide metrics |  Minor | yarn | Jim Brennan | Jim Brennan |
| [HDFS-15652](https://issues.apache.org/jira/browse/HDFS-15652) | Make block size from NNThroughputBenchmark configurable |  Minor | benchmarks | Hui Fei | Hui Fei |
| [HDFS-15665](https://issues.apache.org/jira/browse/HDFS-15665) | Balancer logging improvement |  Major | balancer & mover | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-17342](https://issues.apache.org/jira/browse/HADOOP-17342) | Creating a token identifier should not do kerberos name resolution |  Major | common | Jim Brennan | Jim Brennan |
| [YARN-10479](https://issues.apache.org/jira/browse/YARN-10479) | RMProxy should retry on SocketTimeout Exceptions |  Major | yarn | Jim Brennan | Jim Brennan |
| [HDFS-15623](https://issues.apache.org/jira/browse/HDFS-15623) | Respect configured values of rpc.engine |  Major | hdfs | Hector Sandoval Chaverri | Hector Sandoval Chaverri |
| [HDFS-15717](https://issues.apache.org/jira/browse/HDFS-15717) | Improve fsck logging |  Major | logging, namenode | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15751](https://issues.apache.org/jira/browse/HDFS-15751) | Add documentation for msync() API to filesystem.md |  Major | documentation | Konstantin Shvachko | Konstantin Shvachko |
| [YARN-10562](https://issues.apache.org/jira/browse/YARN-10562) | Follow up changes for YARN-9833 |  Major | yarn | Jim Brennan | Jim Brennan |
| [HADOOP-17478](https://issues.apache.org/jira/browse/HADOOP-17478) | Improve the description of hadoop.http.authentication.signature.secret.file |  Minor | documentation | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17501](https://issues.apache.org/jira/browse/HADOOP-17501) | Fix logging typo in ShutdownHookManager |  Major | common | Konstantin Shvachko | Fengnan Li |
| [HADOOP-17354](https://issues.apache.org/jira/browse/HADOOP-17354) | Move Jenkinsfile outside of the root directory |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [MAPREDUCE-7319](https://issues.apache.org/jira/browse/MAPREDUCE-7319) | Log list of mappers at trace level in ShuffleHandler audit log |  Minor | yarn | Jim Brennan | Jim Brennan |
| [YARN-10626](https://issues.apache.org/jira/browse/YARN-10626) | Log resource allocation in NM log at container start time |  Major | . | Eric Badger | Eric Badger |
| [YARN-10613](https://issues.apache.org/jira/browse/YARN-10613) | Config to allow Intra- and Inter-queue preemption to  enable/disable conservativeDRF |  Minor | capacity scheduler, scheduler preemption | Eric Payne | Eric Payne |
| [MAPREDUCE-7324](https://issues.apache.org/jira/browse/MAPREDUCE-7324) | ClientHSSecurityInfo class is in wrong META-INF file |  Major | . | Eric Badger | Eric Badger |
| [HADOOP-17546](https://issues.apache.org/jira/browse/HADOOP-17546) | Update Description of hadoop-http-auth-signature-secret in HttpAuthentication.md |  Minor | . | Ravuri Sushma sree | Ravuri Sushma sree |
| [HADOOP-17570](https://issues.apache.org/jira/browse/HADOOP-17570) | Apply YETUS-1102 to re-enable GitHub comments |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-16870](https://issues.apache.org/jira/browse/HADOOP-16870) | Use spotbugs-maven-plugin instead of findbugs-maven-plugin |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15931](https://issues.apache.org/jira/browse/HDFS-15931) | Fix non-static inner classes for better memory management |  Major | . | Viraj Jasani | Viraj Jasani |
| [HADOOP-17680](https://issues.apache.org/jira/browse/HADOOP-17680) | Allow ProtobufRpcEngine to be extensible |  Major | common | Hector Sandoval Chaverri | Hector Sandoval Chaverri |
| [HADOOP-17756](https://issues.apache.org/jira/browse/HADOOP-17756) | Increase precommit job timeout from 20 hours to 24 hours. |  Major | build | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-15150](https://issues.apache.org/jira/browse/HDFS-15150) | Introduce read write lock to Datanode |  Major | datanode | Stephen O'Donnell | Stephen O'Donnell |
| [YARN-10834](https://issues.apache.org/jira/browse/YARN-10834) | Intra-queue preemption: apps that don't use defined custom resource won't be preempted. |  Major | . | Eric Payne | Eric Payne |
| [HADOOP-17775](https://issues.apache.org/jira/browse/HADOOP-17775) | Remove JavaScript package from Docker environment |  Major | build | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-12665](https://issues.apache.org/jira/browse/HADOOP-12665) | Document hadoop.security.token.service.use\_ip |  Major | documentation | Arpit Agarwal | Akira Ajisaka |
| [YARN-10456](https://issues.apache.org/jira/browse/YARN-10456) | RM PartitionQueueMetrics records are named QueueMetrics in Simon metrics registry |  Major | resourcemanager | Eric Payne | Eric Payne |
| [YARN-10860](https://issues.apache.org/jira/browse/YARN-10860) | Make max container per heartbeat configs refreshable |  Major | . | Eric Badger | Eric Badger |
| [HADOOP-17813](https://issues.apache.org/jira/browse/HADOOP-17813) | Checkstyle - Allow line length: 100 |  Major | . | Akira Ajisaka | Viraj Jasani |
| [HADOOP-17819](https://issues.apache.org/jira/browse/HADOOP-17819) | Add extensions to ProtobufRpcEngine RequestHeaderProto |  Major | common | Hector Sandoval Chaverri | Hector Sandoval Chaverri |
| [HADOOP-17897](https://issues.apache.org/jira/browse/HADOOP-17897) | Allow nested blocks in switch case in checkstyle settings |  Minor | build | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-17857](https://issues.apache.org/jira/browse/HADOOP-17857) | Check real user ACLs in addition to proxied user ACLs |  Major | . | Eric Payne | Eric Payne |
| [HADOOP-17926](https://issues.apache.org/jira/browse/HADOOP-17926) | Maven-eclipse-plugin is no longer needed since Eclipse can import Maven projects by itself. |  Minor | documentation | Rintaro Ikeda | Rintaro Ikeda |
| [YARN-10935](https://issues.apache.org/jira/browse/YARN-10935) | AM Total Queue Limit goes below per-user AM Limit if parent is full. |  Major | capacity scheduler, capacityscheduler | Eric Payne | Eric Payne |
| [HDFS-16257](https://issues.apache.org/jira/browse/HDFS-16257) | [HDFS] [RBF] Guava cache performance issue in Router MountTableResolver |  Major | . | Janus Chow | Janus Chow |
| [YARN-1115](https://issues.apache.org/jira/browse/YARN-1115) | Provide optional means for a scheduler to check real user ACLs |  Major | capacity scheduler, scheduler | Eric Payne |  |
| [HDFS-16294](https://issues.apache.org/jira/browse/HDFS-16294) | Remove invalid DataNode#CONFIG\_PROPERTY\_SIMULATED |  Major | datanode | JiangHua Zhu | JiangHua Zhu |
| [HDFS-16301](https://issues.apache.org/jira/browse/HDFS-16301) | Improve BenchmarkThroughput#SIZE naming standardization |  Minor | benchmarks, test | JiangHua Zhu | JiangHua Zhu |
| [HADOOP-18035](https://issues.apache.org/jira/browse/HADOOP-18035) | Skip unit test failures to run all the unit tests |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-18040](https://issues.apache.org/jira/browse/HADOOP-18040) | Use maven.test.failure.ignore instead of ignoreTestFailure |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [YARN-8234](https://issues.apache.org/jira/browse/YARN-8234) | Improve RM system metrics publisher's performance by pushing events to timeline server in batch |  Critical | resourcemanager, timelineserver | Hu Ziqian | Ashutosh Gupta |
| [HADOOP-18093](https://issues.apache.org/jira/browse/HADOOP-18093) | Better exception handling for testFileStatusOnMountLink() in ViewFsBaseTest.java |  Trivial | . | Xing Lin | Xing Lin |
| [HADOOP-18099](https://issues.apache.org/jira/browse/HADOOP-18099) | Upgrade bundled Tomcat to 8.5.75 |  Major | httpfs, kms | Akira Ajisaka | Ashutosh Gupta |
| [HADOOP-18110](https://issues.apache.org/jira/browse/HADOOP-18110) | ViewFileSystem: Add Support for Localized Trash Root |  Major | common | Xing Lin | Xing Lin |
| [HADOOP-18144](https://issues.apache.org/jira/browse/HADOOP-18144) | getTrashRoot/s in ViewFileSystem should return viewFS path, not targetFS path |  Major | common | Xing Lin | Xing Lin |
| [HADOOP-18136](https://issues.apache.org/jira/browse/HADOOP-18136) | Verify FileUtils.unTar() handling of missing .tar files |  Minor | test, util | Steve Loughran | Steve Loughran |
| [HDFS-16529](https://issues.apache.org/jira/browse/HDFS-16529) | Remove unnecessary setObserverRead in TestConsistentReadsObserver |  Trivial | test | wangzhaohui | wangzhaohui |
| [HADOOP-18155](https://issues.apache.org/jira/browse/HADOOP-18155) | Refactor tests in TestFileUtil |  Trivial | common | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-18088](https://issues.apache.org/jira/browse/HADOOP-18088) | Replace log4j 1.x with reload4j |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-11125](https://issues.apache.org/jira/browse/YARN-11125) | Backport YARN-6483 to branch-2.10 |  Major | resourcemanager | Ashutosh Gupta | Ashutosh Gupta |
| [HADOOP-18172](https://issues.apache.org/jira/browse/HADOOP-18172) | Change scope of getRootFallbackLink for InodeTree to make them accessible from outside package |  Minor | . | Xing Lin | Xing Lin |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-10650](https://issues.apache.org/jira/browse/HDFS-10650) | DFSClient#mkdirs and DFSClient#primitiveMkdir should use default directory permission |  Minor | . | John Zhuge | John Zhuge |
| [HADOOP-13965](https://issues.apache.org/jira/browse/HADOOP-13965) | Groups should be consistent in using default group mapping class |  Minor | security | Yiqun Lin | Yiqun Lin |
| [YARN-6977](https://issues.apache.org/jira/browse/YARN-6977) | Node information is not provided for non am containers in RM logs |  Major | capacity scheduler | Sumana Sathish | Suma Shivaprasad |
| [HADOOP-15261](https://issues.apache.org/jira/browse/HADOOP-15261) | Upgrade commons-io from 2.4 to 2.5 |  Major | minikdc | PandaMonkey | PandaMonkey |
| [HADOOP-15331](https://issues.apache.org/jira/browse/HADOOP-15331) | Fix a race condition causing parsing error of java.io.BufferedInputStream in class org.apache.hadoop.conf.Configuration |  Major | common | Miklos Szegedi | Miklos Szegedi |
| [YARN-8222](https://issues.apache.org/jira/browse/YARN-8222) | Fix potential NPE when gets RMApp from RM context |  Critical | . | Tao Yang | Tao Yang |
| [HDFS-13174](https://issues.apache.org/jira/browse/HDFS-13174) | hdfs mover -p /path times out after 20 min |  Major | balancer & mover | István Fajth | István Fajth |
| [HDFS-13723](https://issues.apache.org/jira/browse/HDFS-13723) | Occasional "Should be different group" error in TestRefreshUserMappings#testGroupMappingRefresh |  Major | security, test | Siyao Meng | Siyao Meng |
| [YARN-7266](https://issues.apache.org/jira/browse/YARN-7266) | Timeline Server event handler threads locked |  Major | ATSv2, timelineserver | Venkata Puneet Ravuri | Prabhu Joseph |
| [HDFS-13677](https://issues.apache.org/jira/browse/HDFS-13677) | Dynamic refresh Disk configuration results in overwriting VolumeMap |  Blocker | . | ZanderXu | ZanderXu |
| [HADOOP-16334](https://issues.apache.org/jira/browse/HADOOP-16334) | Fix yetus-wrapper not working when HADOOP\_YETUS\_VERSION \>= 0.9.0 |  Major | yetus | Wanqiang Ji | Wanqiang Ji |
| [YARN-9594](https://issues.apache.org/jira/browse/YARN-9594) | Fix missing break statement in ContainerScheduler#handle |  Major | . | lujie | lujie |
| [YARN-9744](https://issues.apache.org/jira/browse/YARN-9744) | RollingLevelDBTimelineStore.getEntityByTime fails with NPE |  Major | timelineserver | Prabhu Joseph | Prabhu Joseph |
| [YARN-9785](https://issues.apache.org/jira/browse/YARN-9785) | Fix DominantResourceCalculator when one resource is zero |  Blocker | . | Bilwa S T | Bilwa S T |
| [YARN-9833](https://issues.apache.org/jira/browse/YARN-9833) | Race condition when DirectoryCollection.checkDirs() runs during container launch |  Major | . | Peter Bacsko | Peter Bacsko |
| [HDFS-14216](https://issues.apache.org/jira/browse/HDFS-14216) | NullPointerException happens in NamenodeWebHdfs |  Critical | . | lujie | lujie |
| [YARN-9984](https://issues.apache.org/jira/browse/YARN-9984) | FSPreemptionThread can cause NullPointerException while app is unregistered with containers running on a node |  Major | fairscheduler | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [HADOOP-16841](https://issues.apache.org/jira/browse/HADOOP-16841) | The description of hadoop.http.authentication.signature.secret.file contains outdated information |  Minor | documentation | Akira Ajisaka | Xieming Li |
| [HADOOP-16768](https://issues.apache.org/jira/browse/HADOOP-16768) | SnappyCompressor test cases wrongly assume that the compressed data is always smaller than the input data |  Major | io, test | zhao bo | Akira Ajisaka |
| [HADOOP-17068](https://issues.apache.org/jira/browse/HADOOP-17068) | client fails forever when namenode ipaddr changed |  Major | hdfs-client | Sean Chow | Sean Chow |
| [HADOOP-17116](https://issues.apache.org/jira/browse/HADOOP-17116) | Skip Retry INFO logging on first failover from a proxy |  Major | ha | Hanisha Koneru | Hanisha Koneru |
| [MAPREDUCE-7294](https://issues.apache.org/jira/browse/MAPREDUCE-7294) | Only application master should upload resource to Yarn Shared Cache |  Major | mrv2 | zhenzhao wang | zhenzhao wang |
| [YARN-10438](https://issues.apache.org/jira/browse/YARN-10438) | Handle null containerId in ClientRMService#getContainerReport() |  Major | resourcemanager | Raghvendra Singh | Shubham Gupta |
| [MAPREDUCE-7289](https://issues.apache.org/jira/browse/MAPREDUCE-7289) | Fix wrong comment in LongLong.java |  Trivial | documentation, examples | Akira Ajisaka | Wanqiang Ji |
| [YARN-10393](https://issues.apache.org/jira/browse/YARN-10393) | MR job live lock caused by completed state container leak in heartbeat between node manager and RM |  Major | nodemanager, yarn | zhenzhao wang | Jim Brennan |
| [YARN-10455](https://issues.apache.org/jira/browse/YARN-10455) | TestNMProxy.testNMProxyRPCRetry is not consistent |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17223](https://issues.apache.org/jira/browse/HADOOP-17223) | update  org.apache.httpcomponents:httpclient to 4.5.13 and httpcore to 4.4.13 |  Blocker | . | Pranav Bheda | Pranav Bheda |
| [HADOOP-17309](https://issues.apache.org/jira/browse/HADOOP-17309) | Javadoc warnings and errors are ignored in the precommit jobs |  Major | build, documentation | Akira Ajisaka | Akira Ajisaka |
| [MAPREDUCE-7302](https://issues.apache.org/jira/browse/MAPREDUCE-7302) | Upgrading to JUnit 4.13 causes testcase TestFetcher.testCorruptedIFile() to fail |  Major | test | Peter Bacsko | Peter Bacsko |
| [HDFS-15644](https://issues.apache.org/jira/browse/HDFS-15644) | Failed volumes can cause DNs to stop block reporting |  Major | block placement, datanode | Ahmed Hussein | Ahmed Hussein |
| [YARN-10467](https://issues.apache.org/jira/browse/YARN-10467) | ContainerIdPBImpl objects can be leaked in RMNodeImpl.completedContainers |  Major | resourcemanager | Haibo Chen | Haibo Chen |
| [HADOOP-17340](https://issues.apache.org/jira/browse/HADOOP-17340) | TestLdapGroupsMapping failing -string mismatch in exception validation |  Major | test | Steve Loughran | Steve Loughran |
| [HADOOP-17352](https://issues.apache.org/jira/browse/HADOOP-17352) | Update PATCH\_NAMING\_RULE in the personality file |  Minor | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17358](https://issues.apache.org/jira/browse/HADOOP-17358) | Improve excessive reloading of Configurations |  Major | conf | Ahmed Hussein | Ahmed Hussein |
| [YARN-8558](https://issues.apache.org/jira/browse/YARN-8558) | NM recovery level db not cleaned up properly on container finish |  Critical | . | Bibin Chundatt | Bibin Chundatt |
| [YARN-10498](https://issues.apache.org/jira/browse/YARN-10498) | Fix Yarn CapacityScheduler Markdown document |  Trivial | documentation | zhaoshengjie | zhaoshengjie |
| [HDFS-15660](https://issues.apache.org/jira/browse/HDFS-15660) | StorageTypeProto is not compatiable between 3.x and 2.6 |  Major | . | Ryan Wu | Ryan Wu |
| [HDFS-15725](https://issues.apache.org/jira/browse/HDFS-15725) | Lease Recovery never completes for a committed block which the DNs never finalize |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [HADOOP-17438](https://issues.apache.org/jira/browse/HADOOP-17438) | Increase docker memory limit in Jenkins |  Major | build, scripts, test, yetus | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-16947](https://issues.apache.org/jira/browse/HADOOP-16947) | Stale record should be remove when MutableRollingAverages generating aggregate data. |  Major | . | Haibin Huang | Haibin Huang |
| [HDFS-15632](https://issues.apache.org/jira/browse/HDFS-15632) | AbstractContractDeleteTest should set recursive parameter to true for recursive test cases. |  Major | . | Konstantin Shvachko | Anton Kutuzov |
| [HDFS-10498](https://issues.apache.org/jira/browse/HDFS-10498) | Intermittent test failure org.apache.hadoop.hdfs.server.namenode.snapshot.TestSnapshotFileLength.testSnapshotfileLength |  Major | hdfs, snapshots | Hanisha Koneru | Jim Brennan |
| [HADOOP-17495](https://issues.apache.org/jira/browse/HADOOP-17495) | Backport HADOOP-16947 "Stale record should be remove when MutableRollingAverages generating aggregate data." to branch 2.10 |  Major | . | Felix N | Felix N |
| [HDFS-15801](https://issues.apache.org/jira/browse/HDFS-15801) | Backport HDFS-14582 to branch-2.10 (Failed to start DN with ArithmeticException when NULL checksum used) |  Major | . | Janus Chow | Janus Chow |
| [YARN-10428](https://issues.apache.org/jira/browse/YARN-10428) | Zombie applications in the YARN queue using FAIR + sizebasedweight |  Critical | capacityscheduler | Guang Yang | Andras Gyori |
| [HDFS-15792](https://issues.apache.org/jira/browse/HDFS-15792) | ClasscastException while loading FSImage |  Major | nn | Renukaprasad C | Renukaprasad C |
| [HADOOP-17516](https://issues.apache.org/jira/browse/HADOOP-17516) | Upgrade ant to 1.10.9 |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [YARN-10500](https://issues.apache.org/jira/browse/YARN-10500) | TestDelegationTokenRenewer fails intermittently |  Major | test | Akira Ajisaka | Masatake Iwasaki |
| [MAPREDUCE-7323](https://issues.apache.org/jira/browse/MAPREDUCE-7323) | Remove job\_history\_summary.py |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17510](https://issues.apache.org/jira/browse/HADOOP-17510) | Hadoop prints sensitive Cookie information. |  Major | . | Renukaprasad C | Renukaprasad C |
| [HDFS-15422](https://issues.apache.org/jira/browse/HDFS-15422) | Reported IBR is partially replaced with stored info when queuing. |  Critical | namenode | Kihwal Lee | Stephen O'Donnell |
| [YARN-10651](https://issues.apache.org/jira/browse/YARN-10651) | CapacityScheduler crashed with NPE in AbstractYarnScheduler.updateNodeResource() |  Major | . | Haibo Chen | Haibo Chen |
| [MAPREDUCE-7320](https://issues.apache.org/jira/browse/MAPREDUCE-7320) | ClusterMapReduceTestCase does not clean directories |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15849](https://issues.apache.org/jira/browse/HDFS-15849) | ExpiredHeartbeats metric should be of Type.COUNTER |  Major | metrics | Konstantin Shvachko | Qi Zhu |
| [HADOOP-17557](https://issues.apache.org/jira/browse/HADOOP-17557) | skip-dir option is not processed by Yetus |  Major | build, precommit, yetus | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17572](https://issues.apache.org/jira/browse/HADOOP-17572) | [branch-2.10] Docker image build fails due to the removal of openjdk-7-jdk package |  Blocker | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17582](https://issues.apache.org/jira/browse/HADOOP-17582) | Replace GitHub App Token with GitHub OAuth token |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17586](https://issues.apache.org/jira/browse/HADOOP-17586) | Upgrade org.codehaus.woodstox:stax2-api to 4.2.1 |  Major | . | Ayush Saxena | Ayush Saxena |
| [YARN-10588](https://issues.apache.org/jira/browse/YARN-10588) | Percentage of queue and cluster is zero in WebUI |  Major | . | Bilwa S T | Bilwa S T |
| [MAPREDUCE-7322](https://issues.apache.org/jira/browse/MAPREDUCE-7322) | revisiting TestMRIntermediateDataEncryption |  Major | job submission, security, test | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17592](https://issues.apache.org/jira/browse/HADOOP-17592) | Fix the wrong CIDR range example in Proxy User documentation |  Minor | documentation | Kwangsun Noh | Kwangsun Noh |
| [MAPREDUCE-7325](https://issues.apache.org/jira/browse/MAPREDUCE-7325) | Intermediate data encryption is broken in LocalJobRunner |  Major | job submission, security | Ahmed Hussein | Ahmed Hussein |
| [YARN-10697](https://issues.apache.org/jira/browse/YARN-10697) | Resources are displayed in bytes in UI for schedulers other than capacity |  Major | . | Bilwa S T | Bilwa S T |
| [HADOOP-17602](https://issues.apache.org/jira/browse/HADOOP-17602) | Upgrade JUnit to 4.13.1 |  Major | build, security, test | Ahmed Hussein | Ahmed Hussein |
| [MAPREDUCE-7332](https://issues.apache.org/jira/browse/MAPREDUCE-7332) | Fix SpillCallBackPathsFinder to use JDK7 on branch-2.10 |  Minor | job submission, security | Ahmed Hussein | Ahmed Hussein |
| [YARN-10501](https://issues.apache.org/jira/browse/YARN-10501) | Can't remove all node labels after add node label without nodemanager port |  Critical | yarn | caozhiqiang | caozhiqiang |
| [YARN-10716](https://issues.apache.org/jira/browse/YARN-10716) | Fix typo in ContainerRuntime |  Trivial | documentation | Wanqiang Ji | xishuhai |
| [HADOOP-17603](https://issues.apache.org/jira/browse/HADOOP-17603) | Upgrade tomcat-embed-core to 7.0.108 |  Major | build, security | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17601](https://issues.apache.org/jira/browse/HADOOP-17601) | Upgrade Jackson databind in branch-2.10 to 2.9.10.7 |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [YARN-10733](https://issues.apache.org/jira/browse/YARN-10733) | TimelineService Hbase tests are failing with timeout error on branch-2.10 |  Major | test, timelineserver, yarn | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15963](https://issues.apache.org/jira/browse/HDFS-15963) | Unreleased volume references cause an infinite loop |  Critical | datanode | Shuyan Zhang | Shuyan Zhang |
| [YARN-10460](https://issues.apache.org/jira/browse/YARN-10460) | Upgrading to JUnit 4.13 causes tests in TestNodeStatusUpdater to fail |  Major | nodemanager, test | Peter Bacsko | Peter Bacsko |
| [YARN-10749](https://issues.apache.org/jira/browse/YARN-10749) | Can't remove all node labels after add node label without nodemanager port, broken by YARN-10647 |  Major | . | D M Murali Krishna Reddy | D M Murali Krishna Reddy |
| [YARN-10555](https://issues.apache.org/jira/browse/YARN-10555) |  Missing access check before getAppAttempts |  Critical | webapp | lujie | lujie |
| [HADOOP-17718](https://issues.apache.org/jira/browse/HADOOP-17718) | Explicitly set locale in the Dockerfile |  Blocker | build | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-10770](https://issues.apache.org/jira/browse/YARN-10770) | container-executor permission is wrong in SecureContainer.md |  Major | documentation | Akira Ajisaka | Siddharth Ahuja |
| [HDFS-15915](https://issues.apache.org/jira/browse/HDFS-15915) | Race condition with async edits logging due to updating txId outside of the namesystem log |  Major | hdfs, namenode | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-16040](https://issues.apache.org/jira/browse/HDFS-16040) | RpcQueueTime metric counts requeued calls as unique events. |  Major | hdfs | Simbarashe Dzinamarira | Simbarashe Dzinamarira |
| [HDFS-16068](https://issues.apache.org/jira/browse/HDFS-16068) | WebHdfsFileSystem has a possible connection leak in connection with HttpFS |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-15618](https://issues.apache.org/jira/browse/HDFS-15618) | Improve datanode shutdown latency |  Major | datanode | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17769](https://issues.apache.org/jira/browse/HADOOP-17769) | Upgrade JUnit to 4.13.2 |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [YARN-10824](https://issues.apache.org/jira/browse/YARN-10824) | Title not set for JHS and NM webpages |  Major | . | Rajshree Mishra | Bilwa S T |
| [MAPREDUCE-7353](https://issues.apache.org/jira/browse/MAPREDUCE-7353) | Mapreduce job fails when NM is stopped |  Major | . | Bilwa S T | Bilwa S T |
| [HADOOP-17793](https://issues.apache.org/jira/browse/HADOOP-17793) | Better token validation |  Major | . | Artem Smotrakov | Artem Smotrakov |
| [HDFS-16042](https://issues.apache.org/jira/browse/HDFS-16042) | DatanodeAdminMonitor scan should be delay based |  Major | datanode | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17028](https://issues.apache.org/jira/browse/HADOOP-17028) | ViewFS should initialize target filesystems lazily |  Major | client-mounts, fs, viewfs | Uma Maheswara Rao G | Abhishek Das |
| [HADOOP-17370](https://issues.apache.org/jira/browse/HADOOP-17370) | Upgrade commons-compress to 1.21 |  Major | common | Dongjoon Hyun | Akira Ajisaka |
| [HADOOP-17886](https://issues.apache.org/jira/browse/HADOOP-17886) | Upgrade ant to 1.10.11 |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17885](https://issues.apache.org/jira/browse/HADOOP-17885) | Upgrade JSON smart to 1.3.3 on branch-2.10 |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [HDFS-16207](https://issues.apache.org/jira/browse/HDFS-16207) | Remove NN logs stack trace for non-existent xattr query |  Major | namenode | Ahmed Hussein | Ahmed Hussein |
| [HDFS-16198](https://issues.apache.org/jira/browse/HDFS-16198) | Short circuit read leaks Slot objects when InvalidToken exception is thrown |  Major | . | Eungsop Yoo | Eungsop Yoo |
| [HDFS-16233](https://issues.apache.org/jira/browse/HDFS-16233) | Do not use exception handler to implement copy-on-write for EnumCounters |  Major | namenode | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-16181](https://issues.apache.org/jira/browse/HDFS-16181) | [SBN Read] Fix metric of RpcRequestCacheMissAmount can't display when tailEditLog form JN |  Critical | . | wangzhaohui | wangzhaohui |
| [YARN-8127](https://issues.apache.org/jira/browse/YARN-8127) | Resource leak when async scheduling is enabled |  Critical | . | Weiwei Yang | Tao Yang |
| [HADOOP-17964](https://issues.apache.org/jira/browse/HADOOP-17964) | Increase Java heap size for running Maven in Dockerfile of branch-2.10 |  Major | build | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-16532](https://issues.apache.org/jira/browse/HADOOP-16532) | Fix TestViewFsTrash to use the correct homeDir. |  Minor | test, viewfs | Steve Loughran | Xing Lin |
| [HADOOP-17965](https://issues.apache.org/jira/browse/HADOOP-17965) | Fix documentation build failure using JDK 7 on branch-2.10 |  Major | build | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-7612](https://issues.apache.org/jira/browse/HDFS-7612) | TestOfflineEditsViewer.testStored() uses incorrect default value for cacheDir |  Major | test | Konstantin Shvachko | Michael Kuchenbecker |
| [HADOOP-17978](https://issues.apache.org/jira/browse/HADOOP-17978) | Exclude ASF license check for pkg-resolver JSON |  Major | build | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-17880](https://issues.apache.org/jira/browse/HADOOP-17880) | Build Hadoop on Centos 7 |  Major | build | baizhendong |  |
| [HADOOP-17988](https://issues.apache.org/jira/browse/HADOOP-17988) | Disable JIRA plugin for YETUS on Hadoop |  Critical | build | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16311](https://issues.apache.org/jira/browse/HDFS-16311) | Metric metadataOperationRate calculation error in DataNodeVolumeMetrics |  Major | . | Tao Li | Tao Li |
| [HADOOP-17999](https://issues.apache.org/jira/browse/HADOOP-17999) | No-op implementation of setWriteChecksum and setVerifyChecksum in ViewFileSystem |  Major | . | Abhishek Das | Abhishek Das |
| [YARN-9063](https://issues.apache.org/jira/browse/YARN-9063) | ATS 1.5 fails to start if RollingLevelDb files are corrupt or missing |  Major | timelineserver, timelineservice | Tarun Parimi | Ashutosh Gupta |
| [HADOOP-18049](https://issues.apache.org/jira/browse/HADOOP-18049) | Pin python lazy-object-proxy to 1.6.0 in Docker file as newer versions are incompatible with python2.7 |  Major | build | Dhananjay Badaya | Dhananjay Badaya |
| [HADOOP-13500](https://issues.apache.org/jira/browse/HADOOP-13500) | Synchronizing iteration of Configuration properties object |  Major | conf | Jason Darrell Lowe | Dhananjay Badaya |
| [YARN-10178](https://issues.apache.org/jira/browse/YARN-10178) | Global Scheduler async thread crash caused by 'Comparison method violates its general contract |  Major | capacity scheduler | tuyu | Andras Gyori |
| [HDFS-16410](https://issues.apache.org/jira/browse/HDFS-16410) | Insecure Xml parsing in OfflineEditsXmlLoader |  Minor | . | Ashutosh Gupta | Ashutosh Gupta |
| [HADOOP-18158](https://issues.apache.org/jira/browse/HADOOP-18158) | Fix failure of create-release script due to releasedocmaker changes in branch-2.10 |  Major | build | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-18129](https://issues.apache.org/jira/browse/HADOOP-18129) | Change URI[] in INodeLink to String[] to reduce memory footprint of ViewFileSystem |  Major | . | Abhishek Das | Abhishek Das |
| [HDFS-16517](https://issues.apache.org/jira/browse/HDFS-16517) | In 2.10 the distance metric is wrong for non-DN machines |  Major | . | Owen O'Malley | Owen O'Malley |
| [HDFS-16518](https://issues.apache.org/jira/browse/HDFS-16518) | KeyProviderCache close cached KeyProvider with Hadoop ShutdownHookManager |  Major | hdfs | Lei Yang | Lei Yang |
| [HADOOP-18169](https://issues.apache.org/jira/browse/HADOOP-18169) | getDelegationTokens in ViewFs should also fetch the token from the fallback FS |  Major | . | Xing Lin | Xing Lin |
| [YARN-10720](https://issues.apache.org/jira/browse/YARN-10720) | YARN WebAppProxyServlet should support connection timeout to prevent proxy server from hanging |  Critical | . | Qi Zhu | Qi Zhu |
| [HDFS-11041](https://issues.apache.org/jira/browse/HDFS-11041) | Unable to unregister FsDatasetState MBean if DataNode is shutdown twice |  Trivial | datanode | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [MAPREDUCE-7246](https://issues.apache.org/jira/browse/MAPREDUCE-7246) |  In MapredAppMasterRest#Mapreduce\_Application\_Master\_Info\_API, the datatype of appId should be "string". |  Major | documentation | jenny | Ashutosh Gupta |
| [YARN-11126](https://issues.apache.org/jira/browse/YARN-11126) | ZKConfigurationStore Java deserialisation vulnerability |  Major | yarn | Tamas Domok | Tamas Domok |
| [YARN-11162](https://issues.apache.org/jira/browse/YARN-11162) | Set the zk acl for nodes created by ZKConfigurationStore. |  Major | resourcemanager | Owen O'Malley | Owen O'Malley |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-7011](https://issues.apache.org/jira/browse/MAPREDUCE-7011) | TestClientDistributedCacheManager::testDetermineCacheVisibilities assumes all parent dirs set other exec |  Trivial | . | Christopher Douglas | Christopher Douglas |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-15133](https://issues.apache.org/jira/browse/HADOOP-15133) | [JDK9] Ignore com.sun.javadoc.\* and com.sun.tools.\* in animal-sniffer-maven-plugin to compile with Java 9 |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-15293](https://issues.apache.org/jira/browse/HADOOP-15293) | TestLogLevel fails on Java 9 |  Major | test | Akira Ajisaka | Takanobu Asanuma |
| [HADOOP-15513](https://issues.apache.org/jira/browse/HADOOP-15513) | Add additional test cases to cover some corner cases for FileUtil#symlink |  Major | . | Giovanni Matteo Fumarola | Giovanni Matteo Fumarola |
| [HADOOP-15783](https://issues.apache.org/jira/browse/HADOOP-15783) | [JDK10] TestSFTPFileSystem.testGetModifyTime fails |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-16764](https://issues.apache.org/jira/browse/HADOOP-16764) | Rewrite Python example codes using Python3 |  Minor | documentation | Kengo Seki | Kengo Seki |
| [HADOOP-16767](https://issues.apache.org/jira/browse/HADOOP-16767) | S3AInputStream reopening does not handle non IO exceptions properly |  Major | . | Sergei Poganshev | Sergei Poganshev |
| [HADOOP-17336](https://issues.apache.org/jira/browse/HADOOP-17336) | Backport HADOOP-16005-"NativeAzureFileSystem does not support setXAttr" and HADOOP-16785. "Improve wasb and abfs resilience on double close() calls. followup to abfs close() fix." to branch-2.10 |  Major | fs/azure | Sally Zuo | Sally Zuo |
| [HDFS-15716](https://issues.apache.org/jira/browse/HDFS-15716) | TestUpgradeDomainBlockPlacementPolicy flaky |  Major | namenode, test | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-16748](https://issues.apache.org/jira/browse/HADOOP-16748) | Migrate to Python 3 and upgrade Yetus to 0.13.0 |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [HDFS-13975](https://issues.apache.org/jira/browse/HDFS-13975) | TestBalancer#testMaxIterationTime fails sporadically |  Major | . | Jason Darrell Lowe | Toshihiko Uchida |
| [HDFS-16072](https://issues.apache.org/jira/browse/HDFS-16072) | TestBlockRecovery fails consistently on Branch-2.10 |  Major | datanode, test | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15659](https://issues.apache.org/jira/browse/HDFS-15659) | Set dfs.namenode.redundancy.considerLoad to false in MiniDFSCluster |  Major | test | Akira Ajisaka | Ahmed Hussein |
| [HADOOP-17952](https://issues.apache.org/jira/browse/HADOOP-17952) | Replace Guava VisibleForTesting by Hadoop's own annotation in hadoop-common-project modules |  Major | . | Viraj Jasani | Viraj Jasani |
| [YARN-8546](https://issues.apache.org/jira/browse/YARN-8546) | Resource leak caused by a reserved container being released more than once under async scheduling |  Major | capacity scheduler | Weiwei Yang | Tao Yang |
| [HDFS-13248](https://issues.apache.org/jira/browse/HDFS-13248) | RBF: Namenode need to choose block location for the client |  Major | . | Wu Weiwei | Owen O'Malley |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-15808](https://issues.apache.org/jira/browse/HDFS-15808) | Add metrics for FSNamesystem read/write lock hold long time |  Major | hdfs | Tao Li | Tao Li |
| [HDFS-16298](https://issues.apache.org/jira/browse/HDFS-16298) | Improve error msg for BlockMissingException |  Minor | . | Tao Li | Tao Li |
| [HDFS-16312](https://issues.apache.org/jira/browse/HDFS-16312) | Fix typo for DataNodeVolumeMetrics and ProfilingFileIoEvents |  Minor | . | Tao Li | Tao Li |
| [YARN-10540](https://issues.apache.org/jira/browse/YARN-10540) | Node page is broken in YARN UI1 and UI2 including RMWebService api for nodes |  Critical | webapp | Sunil G | Jim Brennan |
| [HADOOP-17445](https://issues.apache.org/jira/browse/HADOOP-17445) | Update the year to 2021 |  Major | . | Xiaoqiao He | Xiaoqiao He |
| [HADOOP-17571](https://issues.apache.org/jira/browse/HADOOP-17571) | Upgrade com.fasterxml.woodstox:woodstox-core for security reasons |  Major | . | Viraj Jasani | Viraj Jasani |
| [HDFS-15895](https://issues.apache.org/jira/browse/HDFS-15895) | DFSAdmin#printOpenFiles has redundant String#format usage |  Minor | . | Viraj Jasani | Viraj Jasani |
| [HADOOP-18061](https://issues.apache.org/jira/browse/HADOOP-18061) | Update the year to 2022 |  Major | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-18191](https://issues.apache.org/jira/browse/HADOOP-18191) | Log retry count while handling exceptions in RetryInvocationHandler |  Minor | . | Viraj Jasani | Viraj Jasani |
| [HADOOP-18125](https://issues.apache.org/jira/browse/HADOOP-18125) | Utility to identify git commit / Jira fixVersion discrepancies for RC preparation |  Major | . | Viraj Jasani | Viraj Jasani |
| [HDFS-16551](https://issues.apache.org/jira/browse/HDFS-16551) | Backport HADOOP-17588 to 3.3 and other active old branches. |  Major | . | Renukaprasad C | Renukaprasad C |


