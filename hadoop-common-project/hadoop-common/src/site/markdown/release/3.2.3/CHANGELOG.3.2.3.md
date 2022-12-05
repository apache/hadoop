
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

## Release 3.2.3 - 2022-03-02



### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-15691](https://issues.apache.org/jira/browse/HADOOP-15691) | Add PathCapabilities to FS and FC to complement StreamCapabilities |  Major | . | Steve Loughran | Steve Loughran |
| [HDFS-15711](https://issues.apache.org/jira/browse/HDFS-15711) | Add Metrics to HttpFS Server |  Major | httpfs | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15759](https://issues.apache.org/jira/browse/HDFS-15759) | EC: Verify EC reconstruction correctness on DataNode |  Major | datanode, ec, erasure-coding | Toshihiko Uchida | Toshihiko Uchida |
| [HDFS-16337](https://issues.apache.org/jira/browse/HDFS-16337) | Show start time of Datanode on Web |  Minor | . | tomscut | tomscut |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-16052](https://issues.apache.org/jira/browse/HADOOP-16052) | Remove Subversion and Forrest from Dockerfile |  Minor | build | Akira Ajisaka | Xieming Li |
| [YARN-9783](https://issues.apache.org/jira/browse/YARN-9783) | Remove low-level zookeeper test to be able to build Hadoop against zookeeper 3.5.5 |  Major | test | Mate Szalay-Beko | Mate Szalay-Beko |
| [HADOOP-16717](https://issues.apache.org/jira/browse/HADOOP-16717) | Remove GenericsUtil isLog4jLogger dependency on Log4jLoggerAdapter |  Major | . | David Mollitor | Xieming Li |
| [YARN-10036](https://issues.apache.org/jira/browse/YARN-10036) | Install yarnpkg and upgrade nodejs in Dockerfile |  Major | buid, yarn-ui-v2 | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-16811](https://issues.apache.org/jira/browse/HADOOP-16811) | Use JUnit TemporaryFolder Rule in TestFileUtils |  Minor | common, test | David Mollitor | David Mollitor |
| [HDFS-15075](https://issues.apache.org/jira/browse/HDFS-15075) | Remove process command timing from BPServiceActor |  Major | . | Íñigo Goiri | Xiaoqiao He |
| [HADOOP-16054](https://issues.apache.org/jira/browse/HADOOP-16054) | Update Dockerfile to use Bionic |  Major | build, test | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15574](https://issues.apache.org/jira/browse/HDFS-15574) | Remove unnecessary sort of block list in DirectoryScanner |  Major | . | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-15583](https://issues.apache.org/jira/browse/HDFS-15583) | Backport DirectoryScanner improvements HDFS-14476, HDFS-14751 and HDFS-15048 to branch 3.2 and 3.1 |  Major | datanode | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-15567](https://issues.apache.org/jira/browse/HDFS-15567) | [SBN Read] HDFS should expose msync() API to allow downstream applications call it explicitly. |  Major | ha, hdfs-client | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-15633](https://issues.apache.org/jira/browse/HDFS-15633) | Avoid redundant RPC calls for getDiskStatus |  Major | dfsclient | Ayush Saxena | Ayush Saxena |
| [YARN-10450](https://issues.apache.org/jira/browse/YARN-10450) | Add cpu and memory utilization per node and cluster-wide metrics |  Minor | yarn | Jim Brennan | Jim Brennan |
| [HDFS-15652](https://issues.apache.org/jira/browse/HDFS-15652) | Make block size from NNThroughputBenchmark configurable |  Minor | benchmarks | Hui Fei | Hui Fei |
| [YARN-10475](https://issues.apache.org/jira/browse/YARN-10475) | Scale RM-NM heartbeat interval based on node utilization |  Minor | yarn | Jim Brennan | Jim Brennan |
| [HDFS-15665](https://issues.apache.org/jira/browse/HDFS-15665) | Balancer logging improvement |  Major | balancer & mover | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-17342](https://issues.apache.org/jira/browse/HADOOP-17342) | Creating a token identifier should not do kerberos name resolution |  Major | common | Jim Brennan | Jim Brennan |
| [YARN-10479](https://issues.apache.org/jira/browse/YARN-10479) | RMProxy should retry on SocketTimeout Exceptions |  Major | yarn | Jim Brennan | Jim Brennan |
| [HDFS-15623](https://issues.apache.org/jira/browse/HDFS-15623) | Respect configured values of rpc.engine |  Major | hdfs | Hector Sandoval Chaverri | Hector Sandoval Chaverri |
| [HDFS-14395](https://issues.apache.org/jira/browse/HDFS-14395) | Remove WARN Logging From Interrupts in DataStreamer |  Minor | hdfs-client | David Mollitor | David Mollitor |
| [HADOOP-17367](https://issues.apache.org/jira/browse/HADOOP-17367) | Add InetAddress api to ProxyUsers.authorize |  Major | performance, security | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15694](https://issues.apache.org/jira/browse/HDFS-15694) | Avoid calling UpdateHeartBeatState inside DataNodeDescriptor |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15703](https://issues.apache.org/jira/browse/HDFS-15703) | Don't generate edits for set operations that are no-op |  Major | namenode | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17392](https://issues.apache.org/jira/browse/HADOOP-17392) | Remote exception messages should not include the exception class |  Major | ipc | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15706](https://issues.apache.org/jira/browse/HDFS-15706) | HttpFS: Log more information on request failures |  Major | httpfs | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17389](https://issues.apache.org/jira/browse/HADOOP-17389) | KMS should log full UGI principal |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15720](https://issues.apache.org/jira/browse/HDFS-15720) | namenode audit async logger should add some log4j config |  Minor | hdfs | Max  Xie |  |
| [HDFS-15704](https://issues.apache.org/jira/browse/HDFS-15704) | Mitigate lease monitor's rapid infinite loop |  Major | namenode | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15751](https://issues.apache.org/jira/browse/HDFS-15751) | Add documentation for msync() API to filesystem.md |  Major | documentation | Konstantin Shvachko | Konstantin Shvachko |
| [YARN-10538](https://issues.apache.org/jira/browse/YARN-10538) | Add recommissioning nodes to the list of updated nodes returned to the AM |  Major | . | Srinivas S T | Srinivas S T |
| [YARN-4589](https://issues.apache.org/jira/browse/YARN-4589) | Diagnostics for localization timeouts is lacking |  Major | . | Chang Li | Chang Li |
| [YARN-10562](https://issues.apache.org/jira/browse/YARN-10562) | Follow up changes for YARN-9833 |  Major | yarn | Jim Brennan | Jim Brennan |
| [HDFS-15783](https://issues.apache.org/jira/browse/HDFS-15783) | Speed up BlockPlacementPolicyRackFaultTolerant#verifyBlockPlacement |  Major | block placement | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17478](https://issues.apache.org/jira/browse/HADOOP-17478) | Improve the description of hadoop.http.authentication.signature.secret.file |  Minor | documentation | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15789](https://issues.apache.org/jira/browse/HDFS-15789) | Lease renewal does not require namesystem lock |  Major | hdfs | Jim Brennan | Jim Brennan |
| [HADOOP-17501](https://issues.apache.org/jira/browse/HADOOP-17501) | Fix logging typo in ShutdownHookManager |  Major | common | Konstantin Shvachko | Fengnan Li |
| [HADOOP-17354](https://issues.apache.org/jira/browse/HADOOP-17354) | Move Jenkinsfile outside of the root directory |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15799](https://issues.apache.org/jira/browse/HDFS-15799) | Make DisallowedDatanodeException terse |  Minor | hdfs | Richard | Richard |
| [HDFS-15813](https://issues.apache.org/jira/browse/HDFS-15813) | DataStreamer: keep sending heartbeat packets while streaming |  Major | hdfs | Jim Brennan | Jim Brennan |
| [MAPREDUCE-7319](https://issues.apache.org/jira/browse/MAPREDUCE-7319) | Log list of mappers at trace level in ShuffleHandler audit log |  Minor | yarn | Jim Brennan | Jim Brennan |
| [HDFS-15821](https://issues.apache.org/jira/browse/HDFS-15821) | Add metrics for in-service datanodes |  Minor | . | Zehao Chen | Zehao Chen |
| [YARN-10626](https://issues.apache.org/jira/browse/YARN-10626) | Log resource allocation in NM log at container start time |  Major | . | Eric Badger | Eric Badger |
| [HDFS-15815](https://issues.apache.org/jira/browse/HDFS-15815) |  if required storageType are unavailable, log the failed reason during choosing Datanode |  Minor | block placement | Yang Yun | Yang Yun |
| [HDFS-15826](https://issues.apache.org/jira/browse/HDFS-15826) | Solve the problem of incorrect progress of delegation tokens when loading FsImage |  Major | . | JiangHua Zhu | JiangHua Zhu |
| [HDFS-15734](https://issues.apache.org/jira/browse/HDFS-15734) | [READ] DirectoryScanner#scan need not check StorageType.PROVIDED |  Minor | datanode | Yuxuan Wang | Yuxuan Wang |
| [HADOOP-17538](https://issues.apache.org/jira/browse/HADOOP-17538) | Add kms-default.xml and httpfs-default.xml to site index |  Minor | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-10613](https://issues.apache.org/jira/browse/YARN-10613) | Config to allow Intra- and Inter-queue preemption to  enable/disable conservativeDRF |  Minor | capacity scheduler, scheduler preemption | Eric Payne | Eric Payne |
| [YARN-10653](https://issues.apache.org/jira/browse/YARN-10653) | Fixed the findbugs issues introduced by YARN-10647. |  Major | . | Qi Zhu | Qi Zhu |
| [MAPREDUCE-7324](https://issues.apache.org/jira/browse/MAPREDUCE-7324) | ClientHSSecurityInfo class is in wrong META-INF file |  Major | . | Eric Badger | Eric Badger |
| [HADOOP-17546](https://issues.apache.org/jira/browse/HADOOP-17546) | Update Description of hadoop-http-auth-signature-secret in HttpAuthentication.md |  Minor | . | Ravuri Sushma sree | Ravuri Sushma sree |
| [YARN-10664](https://issues.apache.org/jira/browse/YARN-10664) | Allow parameter expansion in NM\_ADMIN\_USER\_ENV |  Major | yarn | Jim Brennan | Jim Brennan |
| [HADOOP-17570](https://issues.apache.org/jira/browse/HADOOP-17570) | Apply YETUS-1102 to re-enable GitHub comments |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17594](https://issues.apache.org/jira/browse/HADOOP-17594) | DistCp: Expose the JobId for applications executing through run method |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-15911](https://issues.apache.org/jira/browse/HDFS-15911) | Provide blocks moved count in Balancer iteration result |  Major | balancer & mover | Viraj Jasani | Viraj Jasani |
| [HDFS-15919](https://issues.apache.org/jira/browse/HDFS-15919) | BlockPoolManager should log stack trace if unable to get Namenode addresses |  Major | datanode | Stephen O'Donnell | Stephen O'Donnell |
| [HADOOP-16870](https://issues.apache.org/jira/browse/HADOOP-16870) | Use spotbugs-maven-plugin instead of findbugs-maven-plugin |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15932](https://issues.apache.org/jira/browse/HDFS-15932) | Improve the balancer error message when process exits abnormally. |  Major | . | Renukaprasad C | Renukaprasad C |
| [HDFS-15931](https://issues.apache.org/jira/browse/HDFS-15931) | Fix non-static inner classes for better memory management |  Major | . | Viraj Jasani | Viraj Jasani |
| [HDFS-15942](https://issues.apache.org/jira/browse/HDFS-15942) | Increase Quota initialization threads |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-15937](https://issues.apache.org/jira/browse/HDFS-15937) | Reduce memory used during datanode layout upgrade |  Major | datanode | Stephen O'Donnell | Stephen O'Donnell |
| [HADOOP-17569](https://issues.apache.org/jira/browse/HADOOP-17569) | Building native code fails on Fedora 33 |  Major | build, common | Kengo Seki | Masatake Iwasaki |
| [HADOOP-17633](https://issues.apache.org/jira/browse/HADOOP-17633) | Bump json-smart to 2.4.2 and nimbus-jose-jwt to 9.8 due to CVEs |  Major | auth, build | helen huang | Viraj Jasani |
| [HADOOP-16822](https://issues.apache.org/jira/browse/HADOOP-16822) | Provide source artifacts for hadoop-client-api |  Major | . | Karel Kolman | Karel Kolman |
| [HADOOP-17680](https://issues.apache.org/jira/browse/HADOOP-17680) | Allow ProtobufRpcEngine to be extensible |  Major | common | Hector Sandoval Chaverri | Hector Sandoval Chaverri |
| [YARN-10123](https://issues.apache.org/jira/browse/YARN-10123) | Error message around yarn app -stop/start can be improved to highlight that an implementation at framework level is needed for the stop/start functionality to work |  Minor | client, documentation | Siddharth Ahuja | Siddharth Ahuja |
| [HADOOP-17756](https://issues.apache.org/jira/browse/HADOOP-17756) | Increase precommit job timeout from 20 hours to 24 hours. |  Major | build | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-16073](https://issues.apache.org/jira/browse/HDFS-16073) | Remove redundant RPC requests for getFileLinkInfo in ClientNamenodeProtocolTranslatorPB |  Minor | . | lei w | lei w |
| [HDFS-16074](https://issues.apache.org/jira/browse/HDFS-16074) | Remove an expensive debug string concatenation |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-15150](https://issues.apache.org/jira/browse/HDFS-15150) | Introduce read write lock to Datanode |  Major | datanode | Stephen O'Donnell | Stephen O'Donnell |
| [YARN-10834](https://issues.apache.org/jira/browse/YARN-10834) | Intra-queue preemption: apps that don't use defined custom resource won't be preempted. |  Major | . | Eric Payne | Eric Payne |
| [HADOOP-17749](https://issues.apache.org/jira/browse/HADOOP-17749) | Remove lock contention in SelectorPool of SocketIOWithTimeout |  Major | common | Xuesen Liang | Xuesen Liang |
| [HADOOP-17775](https://issues.apache.org/jira/browse/HADOOP-17775) | Remove JavaScript package from Docker environment |  Major | build | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-17794](https://issues.apache.org/jira/browse/HADOOP-17794) | Add a sample configuration to use ZKDelegationTokenSecretManager in Hadoop KMS |  Major | documentation, kms, security | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-12665](https://issues.apache.org/jira/browse/HADOOP-12665) | Document hadoop.security.token.service.use\_ip |  Major | documentation | Arpit Agarwal | Akira Ajisaka |
| [YARN-10456](https://issues.apache.org/jira/browse/YARN-10456) | RM PartitionQueueMetrics records are named QueueMetrics in Simon metrics registry |  Major | resourcemanager | Eric Payne | Eric Payne |
| [HDFS-15650](https://issues.apache.org/jira/browse/HDFS-15650) | Make the socket timeout for computing checksum of striped blocks configurable |  Minor | datanode, ec, erasure-coding | Yushi Hayasaka | Yushi Hayasaka |
| [YARN-10858](https://issues.apache.org/jira/browse/YARN-10858) | [UI2] YARN-10826 breaks Queue view |  Major | yarn-ui-v2 | Andras Gyori | Masatake Iwasaki |
| [YARN-10860](https://issues.apache.org/jira/browse/YARN-10860) | Make max container per heartbeat configs refreshable |  Major | . | Eric Badger | Eric Badger |
| [HADOOP-17813](https://issues.apache.org/jira/browse/HADOOP-17813) | Checkstyle - Allow line length: 100 |  Major | . | Akira Ajisaka | Viraj Jasani |
| [HADOOP-17819](https://issues.apache.org/jira/browse/HADOOP-17819) | Add extensions to ProtobufRpcEngine RequestHeaderProto |  Major | common | Hector Sandoval Chaverri | Hector Sandoval Chaverri |
| [HDFS-16153](https://issues.apache.org/jira/browse/HDFS-16153) | Avoid evaluation of LOG.debug statement in QuorumJournalManager |  Trivial | . | wangzhaohui | wangzhaohui |
| [HDFS-16154](https://issues.apache.org/jira/browse/HDFS-16154) | TestMiniJournalCluster failing intermittently because of not reseting UserGroupInformation completely |  Minor | . | wangzhaohui | wangzhaohui |
| [HADOOP-17849](https://issues.apache.org/jira/browse/HADOOP-17849) | Exclude spotbugs-annotations from transitive dependencies on branch-3.2 |  Major | . | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-16173](https://issues.apache.org/jira/browse/HDFS-16173) | Improve CopyCommands#Put#executor queue configurability |  Major | fs | JiangHua Zhu | JiangHua Zhu |
| [HDFS-15160](https://issues.apache.org/jira/browse/HDFS-15160) | ReplicaMap, Disk Balancer, Directory Scanner and various FsDatasetImpl methods should use datanode readlock |  Major | datanode | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-14997](https://issues.apache.org/jira/browse/HDFS-14997) | BPServiceActor processes commands from NameNode asynchronously |  Major | datanode | Xiaoqiao He | Xiaoqiao He |
| [HDFS-16241](https://issues.apache.org/jira/browse/HDFS-16241) | Standby close reconstruction thread |  Major | . | zhanghuazong | zhanghuazong |
| [HDFS-16286](https://issues.apache.org/jira/browse/HDFS-16286) | Debug tool to verify the correctness of erasure coding on file |  Minor | erasure-coding, tools | daimin | daimin |
| [HADOOP-17998](https://issues.apache.org/jira/browse/HADOOP-17998) | Allow get command to run with multi threads. |  Major | fs | Chengwei Wang | Chengwei Wang |
| [HADOOP-18023](https://issues.apache.org/jira/browse/HADOOP-18023) | Allow cp command to run with multi threads. |  Major | fs | Chengwei Wang | Chengwei Wang |
| [HADOOP-17643](https://issues.apache.org/jira/browse/HADOOP-17643) | WASB : Make metadata checks case insensitive |  Major | . | Anoop Sam John | Anoop Sam John |
| [HDFS-16386](https://issues.apache.org/jira/browse/HDFS-16386) | Reduce DataNode load when FsDatasetAsyncDiskService is working |  Major | datanode | JiangHua Zhu | JiangHua Zhu |
| [HDFS-16430](https://issues.apache.org/jira/browse/HDFS-16430) | Validate maximum blocks in EC group when adding an EC policy |  Minor | ec, erasure-coding | daimin | daimin |
| [HDFS-16403](https://issues.apache.org/jira/browse/HDFS-16403) | Improve FUSE IO performance by supporting FUSE parameter max\_background |  Minor | fuse-dfs | daimin | daimin |
| [HADOOP-18093](https://issues.apache.org/jira/browse/HADOOP-18093) | Better exception handling for testFileStatusOnMountLink() in ViewFsBaseTest.java |  Trivial | . | Xing Lin | Xing Lin |
| [HADOOP-18155](https://issues.apache.org/jira/browse/HADOOP-18155) | Refactor tests in TestFileUtil |  Trivial | common | Gautham Banasandra | Gautham Banasandra |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-15939](https://issues.apache.org/jira/browse/HADOOP-15939) | Filter overlapping objenesis class in hadoop-client-minicluster |  Minor | build | Xiaoyu Yao | Xiaoyu Yao |
| [YARN-8936](https://issues.apache.org/jira/browse/YARN-8936) | Bump up Atsv2 hbase versions |  Major | . | Rohith Sharma K S | Vrushali C |
| [HDFS-14189](https://issues.apache.org/jira/browse/HDFS-14189) | Fix intermittent failure of TestNameNodeMetrics |  Major | . | Ayush Saxena | Ayush Saxena |
| [YARN-9246](https://issues.apache.org/jira/browse/YARN-9246) | NPE when executing a command yarn node -status or -states without additional arguments |  Minor | client | Masahiro Tanaka | Masahiro Tanaka |
| [YARN-7266](https://issues.apache.org/jira/browse/YARN-7266) | Timeline Server event handler threads locked |  Major | ATSv2, timelineserver | Venkata Puneet Ravuri | Prabhu Joseph |
| [YARN-9990](https://issues.apache.org/jira/browse/YARN-9990) | Testcase fails with "Insufficient configured threads: required=16 \< max=10" |  Major | . | Prabhu Joseph | Prabhu Joseph |
| [YARN-10020](https://issues.apache.org/jira/browse/YARN-10020) | Fix build instruction of hadoop-yarn-ui |  Minor | yarn-ui-v2 | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-10037](https://issues.apache.org/jira/browse/YARN-10037) | Upgrade build tools for YARN Web UI v2 |  Major | build, security, yarn-ui-v2 | Akira Ajisaka | Masatake Iwasaki |
| [HDFS-15187](https://issues.apache.org/jira/browse/HDFS-15187) | CORRUPT replica mismatch between namenodes after failover |  Critical | . | Ayush Saxena | Ayush Saxena |
| [HDFS-15200](https://issues.apache.org/jira/browse/HDFS-15200) | Delete Corrupt Replica Immediately Irrespective of Replicas On Stale Storage |  Critical | . | Ayush Saxena | Ayush Saxena |
| [HDFS-15113](https://issues.apache.org/jira/browse/HDFS-15113) | Missing IBR when NameNode restart if open processCommand async feature |  Blocker | datanode | Xiaoqiao He | Xiaoqiao He |
| [HDFS-15210](https://issues.apache.org/jira/browse/HDFS-15210) | EC : File write hanged when DN is shutdown by admin command. |  Major | ec | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HADOOP-16768](https://issues.apache.org/jira/browse/HADOOP-16768) | SnappyCompressor test cases wrongly assume that the compressed data is always smaller than the input data |  Major | io, test | zhao bo | Akira Ajisaka |
| [HDFS-11041](https://issues.apache.org/jira/browse/HDFS-11041) | Unable to unregister FsDatasetState MBean if DataNode is shutdown twice |  Trivial | datanode | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-17068](https://issues.apache.org/jira/browse/HADOOP-17068) | client fails forever when namenode ipaddr changed |  Major | hdfs-client | Sean Chow | Sean Chow |
| [HDFS-15378](https://issues.apache.org/jira/browse/HDFS-15378) | TestReconstructStripedFile#testErasureCodingWorkerXmitsWeight is failing on trunk |  Major | . | Hemanth Boyina | Hemanth Boyina |
| [YARN-10331](https://issues.apache.org/jira/browse/YARN-10331) | Upgrade node.js to 10.21.0 |  Critical | build, yarn-ui-v2 | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17119](https://issues.apache.org/jira/browse/HADOOP-17119) | Jetty upgrade to 9.4.x causes MR app fail with IOException |  Major | . | Bilwa S T | Bilwa S T |
| [HADOOP-17138](https://issues.apache.org/jira/browse/HADOOP-17138) | Fix spotbugs warnings surfaced after upgrade to 4.0.6 |  Minor | . | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-15439](https://issues.apache.org/jira/browse/HDFS-15439) | Setting dfs.mover.retry.max.attempts to negative value will retry forever. |  Major | balancer & mover | AMC-team | AMC-team |
| [YARN-10430](https://issues.apache.org/jira/browse/YARN-10430) | Log improvements in NodeStatusUpdaterImpl |  Minor | nodemanager | Bilwa S T | Bilwa S T |
| [HDFS-15438](https://issues.apache.org/jira/browse/HDFS-15438) | Setting dfs.disk.balancer.max.disk.errors = 0 will fail the block copy |  Major | balancer & mover | AMC-team | AMC-team |
| [YARN-10438](https://issues.apache.org/jira/browse/YARN-10438) | Handle null containerId in ClientRMService#getContainerReport() |  Major | resourcemanager | Raghvendra Singh | Shubham Gupta |
| [HDFS-15628](https://issues.apache.org/jira/browse/HDFS-15628) | HttpFS server throws NPE if a file is a symlink |  Major | fs, httpfs | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15627](https://issues.apache.org/jira/browse/HDFS-15627) | Audit log deletes before collecting blocks |  Major | logging, namenode | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17309](https://issues.apache.org/jira/browse/HADOOP-17309) | Javadoc warnings and errors are ignored in the precommit jobs |  Major | build, documentation | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17310](https://issues.apache.org/jira/browse/HADOOP-17310) | Touch command with -c  option is broken |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-15639](https://issues.apache.org/jira/browse/HDFS-15639) | [JDK 11] Fix Javadoc errors in hadoop-hdfs-client |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-15622](https://issues.apache.org/jira/browse/HDFS-15622) | Deleted blocks linger in the replications queue |  Major | hdfs | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15641](https://issues.apache.org/jira/browse/HDFS-15641) | DataNode could meet deadlock if invoke refreshNameNode |  Critical | . | Hongbing Wang | Hongbing Wang |
| [MAPREDUCE-7302](https://issues.apache.org/jira/browse/MAPREDUCE-7302) | Upgrading to JUnit 4.13 causes testcase TestFetcher.testCorruptedIFile() to fail |  Major | test | Peter Bacsko | Peter Bacsko |
| [HDFS-15644](https://issues.apache.org/jira/browse/HDFS-15644) | Failed volumes can cause DNs to stop block reporting |  Major | block placement, datanode | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17236](https://issues.apache.org/jira/browse/HADOOP-17236) | Bump up snakeyaml to 1.26 to mitigate CVE-2017-18640 |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-10467](https://issues.apache.org/jira/browse/YARN-10467) | ContainerIdPBImpl objects can be leaked in RMNodeImpl.completedContainers |  Major | resourcemanager | Haibo Chen | Haibo Chen |
| [HADOOP-17329](https://issues.apache.org/jira/browse/HADOOP-17329) | mvn site commands fails due to MetricsSystemImpl changes |  Major | . | Xiaoqiao He | Xiaoqiao He |
| [HDFS-15651](https://issues.apache.org/jira/browse/HDFS-15651) | Client could not obtain block when DN CommandProcessingThread exit |  Major | . | Yiqun Lin | Mingxiang Li |
| [HADOOP-17340](https://issues.apache.org/jira/browse/HADOOP-17340) | TestLdapGroupsMapping failing -string mismatch in exception validation |  Major | test | Steve Loughran | Steve Loughran |
| [HADOOP-17352](https://issues.apache.org/jira/browse/HADOOP-17352) | Update PATCH\_NAMING\_RULE in the personality file |  Minor | build | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15485](https://issues.apache.org/jira/browse/HDFS-15485) | Fix outdated properties of JournalNode when performing rollback |  Minor | . | Deegue | Deegue |
| [HADOOP-17358](https://issues.apache.org/jira/browse/HADOOP-17358) | Improve excessive reloading of Configurations |  Major | conf | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15538](https://issues.apache.org/jira/browse/HDFS-15538) | Fix the documentation for dfs.namenode.replication.max-streams in hdfs-default.xml |  Major | . | Xieming Li | Xieming Li |
| [HADOOP-17362](https://issues.apache.org/jira/browse/HADOOP-17362) | Doing hadoop ls on Har file triggers too many RPC calls |  Major | fs | Ahmed Hussein | Ahmed Hussein |
| [YARN-10485](https://issues.apache.org/jira/browse/YARN-10485) | TimelineConnector swallows InterruptedException |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17360](https://issues.apache.org/jira/browse/HADOOP-17360) | Log the remote address for authentication success |  Minor | ipc | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17346](https://issues.apache.org/jira/browse/HADOOP-17346) | Fair call queue is defeated by abusive service principals |  Major | common, ipc | Ahmed Hussein | Ahmed Hussein |
| [YARN-10470](https://issues.apache.org/jira/browse/YARN-10470) | When building new web ui with root user, the bower install should support it. |  Major | build, yarn-ui-v2 | Qi Zhu | Qi Zhu |
| [YARN-10498](https://issues.apache.org/jira/browse/YARN-10498) | Fix Yarn CapacityScheduler Markdown document |  Trivial | documentation | zhaoshengjie | zhaoshengjie |
| [HDFS-15695](https://issues.apache.org/jira/browse/HDFS-15695) | NN should not let the balancer run in safemode |  Major | namenode | Ahmed Hussein | Ahmed Hussein |
| [YARN-10511](https://issues.apache.org/jira/browse/YARN-10511) | Update yarn.nodemanager.env-whitelist value in docs |  Minor | documentation | Andrea Scarpino | Andrea Scarpino |
| [HDFS-15707](https://issues.apache.org/jira/browse/HDFS-15707) | NNTop counts don't add up as expected |  Major | hdfs, metrics, namenode | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15709](https://issues.apache.org/jira/browse/HDFS-15709) | EC: Socket file descriptor leak in StripedBlockChecksumReconstructor |  Major | datanode, ec, erasure-coding | Yushi Hayasaka | Yushi Hayasaka |
| [YARN-10491](https://issues.apache.org/jira/browse/YARN-10491) | Fix deprecation warnings in SLSWebApp.java |  Minor | build | Akira Ajisaka | Ankit Kumar |
| [HADOOP-13571](https://issues.apache.org/jira/browse/HADOOP-13571) | ServerSocketUtil.getPort() should use loopback address, not 0.0.0.0 |  Major | . | Eric Badger | Eric Badger |
| [HDFS-15725](https://issues.apache.org/jira/browse/HDFS-15725) | Lease Recovery never completes for a committed block which the DNs never finalize |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-15170](https://issues.apache.org/jira/browse/HDFS-15170) | EC: Block gets marked as CORRUPT in case of failover and pipeline recovery |  Critical | erasure-coding | Ayush Saxena | Ayush Saxena |
| [HDFS-15719](https://issues.apache.org/jira/browse/HDFS-15719) | [Hadoop 3] Both NameNodes can crash simultaneously due to the short JN socket timeout |  Critical | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-10560](https://issues.apache.org/jira/browse/YARN-10560) | Upgrade node.js to 10.23.1 and yarn to 1.22.5 in Web UI v2 |  Major | webapp, yarn-ui-v2 | Akira Ajisaka | Akira Ajisaka |
| [YARN-10528](https://issues.apache.org/jira/browse/YARN-10528) | maxAMShare should only be accepted for leaf queues, not parent queues |  Major | . | Siddharth Ahuja | Siddharth Ahuja |
| [HADOOP-17438](https://issues.apache.org/jira/browse/HADOOP-17438) | Increase docker memory limit in Jenkins |  Major | build, scripts, test, yetus | Ahmed Hussein | Ahmed Hussein |
| [MAPREDUCE-7310](https://issues.apache.org/jira/browse/MAPREDUCE-7310) | Clear the fileMap in JHEventHandlerForSigtermTest |  Minor | test | Zhengxi Li | Zhengxi Li |
| [HADOOP-16947](https://issues.apache.org/jira/browse/HADOOP-16947) | Stale record should be remove when MutableRollingAverages generating aggregate data. |  Major | . | Haibin Huang | Haibin Huang |
| [HDFS-15632](https://issues.apache.org/jira/browse/HDFS-15632) | AbstractContractDeleteTest should set recursive parameter to true for recursive test cases. |  Major | . | Konstantin Shvachko | Anton Kutuzov |
| [HDFS-10498](https://issues.apache.org/jira/browse/HDFS-10498) | Intermittent test failure org.apache.hadoop.hdfs.server.namenode.snapshot.TestSnapshotFileLength.testSnapshotfileLength |  Major | hdfs, snapshots | Hanisha Koneru | Jim Brennan |
| [HADOOP-17506](https://issues.apache.org/jira/browse/HADOOP-17506) | Fix typo in BUILDING.txt |  Trivial | documentation | Gautham Banasandra | Gautham Banasandra |
| [HDFS-15795](https://issues.apache.org/jira/browse/HDFS-15795) | EC: Wrong checksum when reconstruction was failed by exception |  Major | datanode, ec, erasure-coding | Yushi Hayasaka | Yushi Hayasaka |
| [HDFS-15779](https://issues.apache.org/jira/browse/HDFS-15779) | EC: fix NPE caused by StripedWriter.clearBuffers during reconstruct block |  Major | . | Hongbing Wang | Hongbing Wang |
| [HDFS-15798](https://issues.apache.org/jira/browse/HDFS-15798) | EC: Reconstruct task failed, and It would be XmitsInProgress of DN has negative number |  Major | . | Haiyang Hu | Haiyang Hu |
| [YARN-10428](https://issues.apache.org/jira/browse/YARN-10428) | Zombie applications in the YARN queue using FAIR + sizebasedweight |  Critical | capacityscheduler | Guang Yang | Andras Gyori |
| [YARN-10607](https://issues.apache.org/jira/browse/YARN-10607) | User environment is unable to prepend PATH when mapreduce.admin.user.env also sets PATH |  Major | . | Eric Badger | Eric Badger |
| [HADOOP-17516](https://issues.apache.org/jira/browse/HADOOP-17516) | Upgrade ant to 1.10.9 |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [YARN-10500](https://issues.apache.org/jira/browse/YARN-10500) | TestDelegationTokenRenewer fails intermittently |  Major | test | Akira Ajisaka | Masatake Iwasaki |
| [HADOOP-17534](https://issues.apache.org/jira/browse/HADOOP-17534) | Upgrade Jackson databind to 2.10.5.1 |  Major | build | Adam Roberts | Akira Ajisaka |
| [MAPREDUCE-7323](https://issues.apache.org/jira/browse/MAPREDUCE-7323) | Remove job\_history\_summary.py |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [YARN-10647](https://issues.apache.org/jira/browse/YARN-10647) | Fix TestRMNodeLabelsManager failed after YARN-10501. |  Major | . | Qi Zhu | Qi Zhu |
| [HADOOP-17510](https://issues.apache.org/jira/browse/HADOOP-17510) | Hadoop prints sensitive Cookie information. |  Major | . | Renukaprasad C | Renukaprasad C |
| [HDFS-15422](https://issues.apache.org/jira/browse/HDFS-15422) | Reported IBR is partially replaced with stored info when queuing. |  Critical | namenode | Kihwal Lee | Stephen O'Donnell |
| [YARN-10651](https://issues.apache.org/jira/browse/YARN-10651) | CapacityScheduler crashed with NPE in AbstractYarnScheduler.updateNodeResource() |  Major | . | Haibo Chen | Haibo Chen |
| [MAPREDUCE-7320](https://issues.apache.org/jira/browse/MAPREDUCE-7320) | ClusterMapReduceTestCase does not clean directories |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [HDFS-14013](https://issues.apache.org/jira/browse/HDFS-14013) | Skip any credentials stored in HDFS when starting ZKFC |  Major | hdfs | Krzysztof Adamski | Stephen O'Donnell |
| [HDFS-15849](https://issues.apache.org/jira/browse/HDFS-15849) | ExpiredHeartbeats metric should be of Type.COUNTER |  Major | metrics | Konstantin Shvachko | Qi Zhu |
| [YARN-10672](https://issues.apache.org/jira/browse/YARN-10672) | All testcases in TestReservations are flaky |  Major | . | Szilard Nemeth | Szilard Nemeth |
| [HADOOP-17557](https://issues.apache.org/jira/browse/HADOOP-17557) | skip-dir option is not processed by Yetus |  Major | build, precommit, yetus | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15875](https://issues.apache.org/jira/browse/HDFS-15875) | Check whether file is being truncated before truncate |  Major | . | Hui Fei | Hui Fei |
| [HADOOP-17582](https://issues.apache.org/jira/browse/HADOOP-17582) | Replace GitHub App Token with GitHub OAuth token |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [YARN-10687](https://issues.apache.org/jira/browse/YARN-10687) | Add option to disable/enable free disk space checking and percentage checking for full and not-full disks |  Major | nodemanager | Qi Zhu | Qi Zhu |
| [HADOOP-17586](https://issues.apache.org/jira/browse/HADOOP-17586) | Upgrade org.codehaus.woodstox:stax2-api to 4.2.1 |  Major | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-17585](https://issues.apache.org/jira/browse/HADOOP-17585) | Correct timestamp format in the docs for the touch command |  Major | . | Stephen O'Donnell | Stephen O'Donnell |
| [YARN-10588](https://issues.apache.org/jira/browse/YARN-10588) | Percentage of queue and cluster is zero in WebUI |  Major | . | Bilwa S T | Bilwa S T |
| [MAPREDUCE-7322](https://issues.apache.org/jira/browse/MAPREDUCE-7322) | revisiting TestMRIntermediateDataEncryption |  Major | job submission, security, test | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17592](https://issues.apache.org/jira/browse/HADOOP-17592) | Fix the wrong CIDR range example in Proxy User documentation |  Minor | documentation | Kwangsun Noh | Kwangsun Noh |
| [YARN-10706](https://issues.apache.org/jira/browse/YARN-10706) | Upgrade com.github.eirslett:frontend-maven-plugin to 1.11.2 |  Major | buid | Mingliang Liu | Mingliang Liu |
| [MAPREDUCE-7325](https://issues.apache.org/jira/browse/MAPREDUCE-7325) | Intermediate data encryption is broken in LocalJobRunner |  Major | job submission, security | Ahmed Hussein | Ahmed Hussein |
| [YARN-10697](https://issues.apache.org/jira/browse/YARN-10697) | Resources are displayed in bytes in UI for schedulers other than capacity |  Major | . | Bilwa S T | Bilwa S T |
| [HADOOP-17602](https://issues.apache.org/jira/browse/HADOOP-17602) | Upgrade JUnit to 4.13.1 |  Major | build, security, test | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15900](https://issues.apache.org/jira/browse/HDFS-15900) | RBF: empty blockpool id on dfsrouter caused by UNAVAILABLE NameNode |  Major | rbf | Harunobu Daikoku | Harunobu Daikoku |
| [YARN-10501](https://issues.apache.org/jira/browse/YARN-10501) | Can't remove all node labels after add node label without nodemanager port |  Critical | yarn | caozhiqiang | caozhiqiang |
| [YARN-10716](https://issues.apache.org/jira/browse/YARN-10716) | Fix typo in ContainerRuntime |  Trivial | documentation | Wanqiang Ji | xishuhai |
| [HDFS-15950](https://issues.apache.org/jira/browse/HDFS-15950) | Remove unused hdfs.proto import |  Major | hdfs-client | Gautham Banasandra | Gautham Banasandra |
| [HDFS-15949](https://issues.apache.org/jira/browse/HDFS-15949) | Fix integer overflow |  Major | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [HDFS-15948](https://issues.apache.org/jira/browse/HDFS-15948) | Fix test4tests for libhdfspp |  Critical | build, libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-17608](https://issues.apache.org/jira/browse/HADOOP-17608) | Fix TestKMS failure |  Major | kms | Akira Ajisaka | Akira Ajisaka |
| [YARN-10460](https://issues.apache.org/jira/browse/YARN-10460) | Upgrading to JUnit 4.13 causes tests in TestNodeStatusUpdater to fail |  Major | nodemanager, test | Peter Bacsko | Peter Bacsko |
| [HADOOP-17641](https://issues.apache.org/jira/browse/HADOOP-17641) | ITestWasbUriAndConfiguration.testCanonicalServiceName() failing now mockaccount exists |  Minor | fs/azure, test | Steve Loughran | Steve Loughran |
| [HADOOP-17655](https://issues.apache.org/jira/browse/HADOOP-17655) | Upgrade Jetty to 9.4.40 |  Blocker | . | Akira Ajisaka | Akira Ajisaka |
| [YARN-10749](https://issues.apache.org/jira/browse/YARN-10749) | Can't remove all node labels after add node label without nodemanager port, broken by YARN-10647 |  Major | . | D M Murali Krishna Reddy | D M Murali Krishna Reddy |
| [HDFS-15621](https://issues.apache.org/jira/browse/HDFS-15621) | Datanode DirectoryScanner uses excessive memory |  Major | datanode | Stephen O'Donnell | Stephen O'Donnell |
| [YARN-10756](https://issues.apache.org/jira/browse/YARN-10756) | Remove additional junit 4.11 dependency from javadoc |  Major | build, test, timelineservice | ANANDA G B | Akira Ajisaka |
| [YARN-10555](https://issues.apache.org/jira/browse/YARN-10555) |  Missing access check before getAppAttempts |  Critical | webapp | lujie | lujie |
| [HADOOP-17703](https://issues.apache.org/jira/browse/HADOOP-17703) | checkcompatibility.py errors out when specifying annotations |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-14922](https://issues.apache.org/jira/browse/HADOOP-14922) | Build of Mapreduce Native Task module fails with unknown opcode "bswap" |  Major | . | Anup Halarnkar | Anup Halarnkar |
| [HADOOP-17718](https://issues.apache.org/jira/browse/HADOOP-17718) | Explicitly set locale in the Dockerfile |  Blocker | build | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-17700](https://issues.apache.org/jira/browse/HADOOP-17700) | ExitUtil#halt info log should log HaltException |  Major | . | Viraj Jasani | Viraj Jasani |
| [YARN-10770](https://issues.apache.org/jira/browse/YARN-10770) | container-executor permission is wrong in SecureContainer.md |  Major | documentation | Akira Ajisaka | Siddharth Ahuja |
| [HDFS-15915](https://issues.apache.org/jira/browse/HDFS-15915) | Race condition with async edits logging due to updating txId outside of the namesystem log |  Major | hdfs, namenode | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-16040](https://issues.apache.org/jira/browse/HDFS-16040) | RpcQueueTime metric counts requeued calls as unique events. |  Major | hdfs | Simbarashe Dzinamarira | Simbarashe Dzinamarira |
| [YARN-10809](https://issues.apache.org/jira/browse/YARN-10809) | testWithHbaseConfAtHdfsFileSystem consistently failing |  Major | . | Viraj Jasani | Viraj Jasani |
| [HDFS-16055](https://issues.apache.org/jira/browse/HDFS-16055) | Quota is not preserved in snapshot INode |  Major | hdfs | Siyao Meng | Siyao Meng |
| [HDFS-16068](https://issues.apache.org/jira/browse/HDFS-16068) | WebHdfsFileSystem has a possible connection leak in connection with HttpFS |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-10767](https://issues.apache.org/jira/browse/YARN-10767) | Yarn Logs Command retrying on Standby RM for 30 times |  Major | . | D M Murali Krishna Reddy | D M Murali Krishna Reddy |
| [HDFS-15618](https://issues.apache.org/jira/browse/HDFS-15618) | Improve datanode shutdown latency |  Major | datanode | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17760](https://issues.apache.org/jira/browse/HADOOP-17760) | Delete hadoop.ssl.enabled and dfs.https.enable from docs and core-default.xml |  Major | documentation | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-13671](https://issues.apache.org/jira/browse/HDFS-13671) | Namenode deletes large dir slowly caused by FoldedTreeSet#removeAndGet |  Major | . | Yiqun Lin | Haibin Huang |
| [HDFS-16061](https://issues.apache.org/jira/browse/HDFS-16061) | DFTestUtil.waitReplication can produce false positives |  Major | hdfs | Ahmed Hussein | Ahmed Hussein |
| [HDFS-14575](https://issues.apache.org/jira/browse/HDFS-14575) | LeaseRenewer#daemon threads leak in DFSClient |  Major | . | Tao Yang | Renukaprasad C |
| [YARN-10826](https://issues.apache.org/jira/browse/YARN-10826) | [UI2] Upgrade Node.js to at least v12.22.1 |  Major | yarn-ui-v2 | Akira Ajisaka | Masatake Iwasaki |
| [YARN-10828](https://issues.apache.org/jira/browse/YARN-10828) | Backport YARN-9789 to branch-3.2 |  Major | . | Tarun Parimi | Tarun Parimi |
| [HADOOP-17769](https://issues.apache.org/jira/browse/HADOOP-17769) | Upgrade JUnit to 4.13.2 |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [YARN-10824](https://issues.apache.org/jira/browse/YARN-10824) | Title not set for JHS and NM webpages |  Major | . | Rajshree Mishra | Bilwa S T |
| [HDFS-16092](https://issues.apache.org/jira/browse/HDFS-16092) | Avoid creating LayoutFlags redundant objects |  Major | . | Viraj Jasani | Viraj Jasani |
| [HDFS-16108](https://issues.apache.org/jira/browse/HDFS-16108) | Incorrect log placeholders used in JournalNodeSyncer |  Minor | . | Viraj Jasani | Viraj Jasani |
| [MAPREDUCE-7353](https://issues.apache.org/jira/browse/MAPREDUCE-7353) | Mapreduce job fails when NM is stopped |  Major | . | Bilwa S T | Bilwa S T |
| [HDFS-16121](https://issues.apache.org/jira/browse/HDFS-16121) | Iterative snapshot diff report can generate duplicate records for creates, deletes and Renames |  Major | snapshots | Srinivasu Majeti | Shashikant Banerjee |
| [HDFS-15796](https://issues.apache.org/jira/browse/HDFS-15796) | ConcurrentModificationException error happens on NameNode occasionally |  Critical | hdfs | Daniel Ma | Daniel Ma |
| [HADOOP-17793](https://issues.apache.org/jira/browse/HADOOP-17793) | Better token validation |  Major | . | Artem Smotrakov | Artem Smotrakov |
| [HDFS-16042](https://issues.apache.org/jira/browse/HDFS-16042) | DatanodeAdminMonitor scan should be delay based |  Major | datanode | Ahmed Hussein | Ahmed Hussein |
| [HDFS-16127](https://issues.apache.org/jira/browse/HDFS-16127) | Improper pipeline close recovery causes a permanent write failure or data loss. |  Major | . | Kihwal Lee | Kihwal Lee |
| [HADOOP-17028](https://issues.apache.org/jira/browse/HADOOP-17028) | ViewFS should initialize target filesystems lazily |  Major | client-mounts, fs, viewfs | Uma Maheswara Rao G | Abhishek Das |
| [HDFS-12920](https://issues.apache.org/jira/browse/HDFS-12920) | HDFS default value change (with adding time unit) breaks old version MR tarball work with Hadoop 3.x |  Critical | configuration, hdfs | Junping Du | Akira Ajisaka |
| [YARN-10813](https://issues.apache.org/jira/browse/YARN-10813) | Set default capacity of root for node labels |  Major | . | Andras Gyori | Andras Gyori |
| [YARN-9551](https://issues.apache.org/jira/browse/YARN-9551) | TestTimelineClientV2Impl.testSyncCall fails intermittently |  Minor | ATSv2, test | Prabhu Joseph | Andras Gyori |
| [HDFS-15175](https://issues.apache.org/jira/browse/HDFS-15175) | Multiple CloseOp shared block instance causes the standby namenode to crash when rolling editlog |  Critical | . | Yicong Cai | Wan Chang |
| [YARN-10789](https://issues.apache.org/jira/browse/YARN-10789) | RM HA startup can fail due to race conditions in ZKConfigurationStore |  Major | . | Tarun Parimi | Tarun Parimi |
| [YARN-6221](https://issues.apache.org/jira/browse/YARN-6221) | Entities missing from ATS when summary log file info got returned to the ATS before the domain log |  Critical | yarn | Sushmitha Sreenivasan | Xiaomin Zhang |
| [MAPREDUCE-7258](https://issues.apache.org/jira/browse/MAPREDUCE-7258) | HistoryServerRest.html#Task\_Counters\_API, modify the jobTaskCounters's itemName from "taskcounterGroup" to "taskCounterGroup". |  Minor | documentation | jenny | jenny |
| [YARN-8990](https://issues.apache.org/jira/browse/YARN-8990) | Fix fair scheduler race condition in app submit and queue cleanup |  Blocker | fairscheduler | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [YARN-8992](https://issues.apache.org/jira/browse/YARN-8992) | Fair scheduler can delete a dynamic queue while an application attempt is being added to the queue |  Major | fairscheduler | Haibo Chen | Wilfred Spiegelenburg |
| [HADOOP-17370](https://issues.apache.org/jira/browse/HADOOP-17370) | Upgrade commons-compress to 1.21 |  Major | common | Dongjoon Hyun | Akira Ajisaka |
| [HADOOP-17844](https://issues.apache.org/jira/browse/HADOOP-17844) | Upgrade JSON smart to 2.4.7 |  Major | . | Renukaprasad C | Renukaprasad C |
| [HADOOP-17850](https://issues.apache.org/jira/browse/HADOOP-17850) | Upgrade ZooKeeper to 3.4.14 in branch-3.2 |  Major | . | Akira Ajisaka | Masatake Iwasaki |
| [HDFS-16177](https://issues.apache.org/jira/browse/HDFS-16177) | Bug fix for Util#receiveFile |  Minor | . | tomscut | tomscut |
| [YARN-10814](https://issues.apache.org/jira/browse/YARN-10814) | YARN shouldn't start with empty hadoop.http.authentication.signature.secret.file |  Major | . | Benjamin Teke | Tamas Domok |
| [HADOOP-17858](https://issues.apache.org/jira/browse/HADOOP-17858) | Avoid possible class loading deadlock with VerifierNone initialization |  Major | . | Viraj Jasani | Viraj Jasani |
| [HADOOP-17886](https://issues.apache.org/jira/browse/HADOOP-17886) | Upgrade ant to 1.10.11 |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [YARN-10901](https://issues.apache.org/jira/browse/YARN-10901) | Permission checking error on an existing directory in LogAggregationFileController#verifyAndCreateRemoteLogDir |  Major | nodemanager | Tamas Domok | Tamas Domok |
| [HDFS-16187](https://issues.apache.org/jira/browse/HDFS-16187) | SnapshotDiff behaviour with Xattrs and Acls is not consistent across NN restarts with checkpointing |  Major | snapshots | Srinivasu Majeti | Shashikant Banerjee |
| [HDFS-16198](https://issues.apache.org/jira/browse/HDFS-16198) | Short circuit read leaks Slot objects when InvalidToken exception is thrown |  Major | . | Eungsop Yoo | Eungsop Yoo |
| [HADOOP-17917](https://issues.apache.org/jira/browse/HADOOP-17917) | Backport HADOOP-15993 to branch-3.2 which address CVE-2014-4611 |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-16233](https://issues.apache.org/jira/browse/HDFS-16233) | Do not use exception handler to implement copy-on-write for EnumCounters |  Major | namenode | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-16235](https://issues.apache.org/jira/browse/HDFS-16235) | Deadlock in LeaseRenewer for static remove method |  Major | hdfs | angerszhu | angerszhu |
| [HADOOP-17940](https://issues.apache.org/jira/browse/HADOOP-17940) | Upgrade Kafka to 2.8.1 |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-16272](https://issues.apache.org/jira/browse/HDFS-16272) | Int overflow in computing safe length during EC block recovery |  Critical | 3.1.1 | daimin | daimin |
| [HADOOP-17971](https://issues.apache.org/jira/browse/HADOOP-17971) | Exclude IBM Java security classes from being shaded/relocated |  Major | build | Nicholas Marion | Nicholas Marion |
| [HADOOP-17972](https://issues.apache.org/jira/browse/HADOOP-17972) | Backport HADOOP-17683 for branch-3.2 |  Major | security | Ananya Singh | Ananya Singh |
| [HADOOP-17993](https://issues.apache.org/jira/browse/HADOOP-17993) | Disable JIRA plugin for YETUS on Hadoop |  Critical | build | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16182](https://issues.apache.org/jira/browse/HDFS-16182) | numOfReplicas is given the wrong value in  BlockPlacementPolicyDefault$chooseTarget can cause DataStreamer to fail with Heterogeneous Storage |  Major | namanode | Max  Xie | Max  Xie |
| [HDFS-16350](https://issues.apache.org/jira/browse/HDFS-16350) | Datanode start time should be set after RPC server starts successfully |  Minor | . | Viraj Jasani | Viraj Jasani |
| [HADOOP-13500](https://issues.apache.org/jira/browse/HADOOP-13500) | Synchronizing iteration of Configuration properties object |  Major | conf | Jason Darrell Lowe | Dhananjay Badaya |
| [HDFS-16317](https://issues.apache.org/jira/browse/HDFS-16317) | Backport HDFS-14729 for branch-3.2 |  Major | security | Ananya Singh | Ananya Singh |
| [HDFS-14099](https://issues.apache.org/jira/browse/HDFS-14099) | Unknown frame descriptor when decompressing multiple frames in ZStandardDecompressor |  Major | . | xuzq | xuzq |
| [HDFS-16410](https://issues.apache.org/jira/browse/HDFS-16410) | Insecure Xml parsing in OfflineEditsXmlLoader |  Minor | . | Ashutosh Gupta | Ashutosh Gupta |
| [HDFS-16420](https://issues.apache.org/jira/browse/HDFS-16420) | Avoid deleting unique data blocks when deleting redundancy striped blocks |  Critical | ec, erasure-coding | qinyuren | Jackson Wang |
| [HDFS-16428](https://issues.apache.org/jira/browse/HDFS-16428) | Source path with storagePolicy cause wrong typeConsumed while rename |  Major | hdfs, namenode | lei w | lei w |
| [HDFS-16437](https://issues.apache.org/jira/browse/HDFS-16437) | ReverseXML processor doesn't accept XML files without the SnapshotDiffSection. |  Critical | hdfs | yanbin.zhang | yanbin.zhang |
| [HDFS-16422](https://issues.apache.org/jira/browse/HDFS-16422) | Fix thread safety of EC decoding during concurrent preads |  Critical | dfsclient, ec, erasure-coding | daimin | daimin |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-9338](https://issues.apache.org/jira/browse/YARN-9338) | Timeline related testcases are failing |  Major | . | Prabhu Joseph | Abhishek Modi |
| [HDFS-15092](https://issues.apache.org/jira/browse/HDFS-15092) | TestRedudantBlocks#testProcessOverReplicatedAndRedudantBlock sometimes fails |  Minor | test | Hui Fei | Hui Fei |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-15775](https://issues.apache.org/jira/browse/HADOOP-15775) | [JDK9] Add missing javax.activation-api dependency |  Critical | test | Akira Ajisaka | Akira Ajisaka |
| [YARN-9875](https://issues.apache.org/jira/browse/YARN-9875) | FSSchedulerConfigurationStore fails to update with hdfs path |  Major | capacityscheduler | Prabhu Joseph | Prabhu Joseph |
| [HADOOP-16764](https://issues.apache.org/jira/browse/HADOOP-16764) | Rewrite Python example codes using Python3 |  Minor | documentation | Kengo Seki | Kengo Seki |
| [HADOOP-16905](https://issues.apache.org/jira/browse/HADOOP-16905) | Update jackson-databind to 2.10.3 to relieve us from the endless CVE patches |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-10337](https://issues.apache.org/jira/browse/YARN-10337) | TestRMHATimelineCollectors fails on hadoop trunk |  Major | test, yarn | Ahmed Hussein | Bilwa S T |
| [HDFS-15464](https://issues.apache.org/jira/browse/HDFS-15464) | ViewFsOverloadScheme should work when -fs option pointing to remote cluster without mount links |  Major | viewfsOverloadScheme | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-15478](https://issues.apache.org/jira/browse/HDFS-15478) | When Empty mount points, we are assigning fallback link to self. But it should not use full URI for target fs. |  Major | . | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-15459](https://issues.apache.org/jira/browse/HDFS-15459) | TestBlockTokenWithDFSStriped fails intermittently |  Major | hdfs | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15461](https://issues.apache.org/jira/browse/HDFS-15461) | TestDFSClientRetries#testGetFileChecksum fails intermittently |  Major | dfsclient, test | Ahmed Hussein | Ahmed Hussein |
| [HDFS-9776](https://issues.apache.org/jira/browse/HDFS-9776) | TestHAAppend#testMultipleAppendsDuringCatchupTailing is flaky |  Major | . | Vinayakumar B | Ahmed Hussein |
| [HDFS-15457](https://issues.apache.org/jira/browse/HDFS-15457) | TestFsDatasetImpl fails intermittently |  Major | hdfs | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17330](https://issues.apache.org/jira/browse/HADOOP-17330) | Backport HADOOP-16005-"NativeAzureFileSystem does not support setXAttr" to branch-3.2 |  Major | fs/azure | Sally Zuo | Sally Zuo |
| [HDFS-15643](https://issues.apache.org/jira/browse/HDFS-15643) | EC: Fix checksum computation in case of native encoders |  Blocker | . | Ahmed Hussein | Ayush Saxena |
| [HADOOP-17325](https://issues.apache.org/jira/browse/HADOOP-17325) | WASB: Test failures |  Major | fs/azure, test | Sneha Vijayarajan | Steve Loughran |
| [HDFS-15716](https://issues.apache.org/jira/browse/HDFS-15716) | TestUpgradeDomainBlockPlacementPolicy flaky |  Major | namenode, test | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15762](https://issues.apache.org/jira/browse/HDFS-15762) | TestMultipleNNPortQOP#testMultipleNNPortOverwriteDownStream fails intermittently |  Minor | . | Toshihiko Uchida | Toshihiko Uchida |
| [HDFS-15672](https://issues.apache.org/jira/browse/HDFS-15672) | TestBalancerWithMultipleNameNodes#testBalancingBlockpoolsWithBlockPoolPolicy fails on trunk |  Major | . | Ahmed Hussein | Masatake Iwasaki |
| [HDFS-15818](https://issues.apache.org/jira/browse/HDFS-15818) | Fix TestFsDatasetImpl.testReadLockCanBeDisabledByConfig |  Minor | test | Leon Gao | Leon Gao |
| [HADOOP-16748](https://issues.apache.org/jira/browse/HADOOP-16748) | Migrate to Python 3 and upgrade Yetus to 0.13.0 |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15890](https://issues.apache.org/jira/browse/HDFS-15890) | Improve the Logs for File Concat Operation |  Minor | namenode | Bhavik Patel | Bhavik Patel |
| [HDFS-13975](https://issues.apache.org/jira/browse/HDFS-13975) | TestBalancer#testMaxIterationTime fails sporadically |  Major | . | Jason Darrell Lowe | Toshihiko Uchida |
| [YARN-10688](https://issues.apache.org/jira/browse/YARN-10688) | ClusterMetrics should support GPU capacity related metrics. |  Major | metrics, resourcemanager | Qi Zhu | Qi Zhu |
| [HDFS-15902](https://issues.apache.org/jira/browse/HDFS-15902) | Improve the log for HTTPFS server operation |  Minor | httpfs | Bhavik Patel | Bhavik Patel |
| [HDFS-15940](https://issues.apache.org/jira/browse/HDFS-15940) | Some tests in TestBlockRecovery are consistently failing |  Major | . | Viraj Jasani | Viraj Jasani |
| [YARN-10702](https://issues.apache.org/jira/browse/YARN-10702) | Add cluster metric for amount of CPU used by RM Event Processor |  Minor | yarn | Jim Brennan | Jim Brennan |
| [HADOOP-17630](https://issues.apache.org/jira/browse/HADOOP-17630) | [JDK 15] TestPrintableString fails due to Unicode 13.0 support |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [YARN-10723](https://issues.apache.org/jira/browse/YARN-10723) | Change CS nodes page in UI to support custom resource. |  Major | . | Qi Zhu | Qi Zhu |
| [HADOOP-17112](https://issues.apache.org/jira/browse/HADOOP-17112) | whitespace not allowed in paths when saving files to s3a via committer |  Blocker | fs/s3 | Krzysztof Adamski | Krzysztof Adamski |
| [HADOOP-17661](https://issues.apache.org/jira/browse/HADOOP-17661) | mvn versions:set fails to parse pom.xml |  Blocker | build | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-10642](https://issues.apache.org/jira/browse/YARN-10642) | Race condition: AsyncDispatcher can get stuck by the changes introduced in YARN-8995 |  Critical | resourcemanager | zhengchenyu | zhengchenyu |
| [HDFS-15659](https://issues.apache.org/jira/browse/HDFS-15659) | Set dfs.namenode.redundancy.considerLoad to false in MiniDFSCluster |  Major | test | Akira Ajisaka | Ahmed Hussein |
| [HADOOP-17840](https://issues.apache.org/jira/browse/HADOOP-17840) | Backport HADOOP-17837 to branch-3.2 |  Minor | . | Bryan Beaudreault | Bryan Beaudreault |
| [HADOOP-17126](https://issues.apache.org/jira/browse/HADOOP-17126) | implement non-guava Precondition checkNotNull |  Major | . | Ahmed Hussein | Ahmed Hussein |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-15870](https://issues.apache.org/jira/browse/HDFS-15870) | Remove unused configuration dfs.namenode.stripe.min |  Minor | . | tomscut | tomscut |
| [HDFS-15808](https://issues.apache.org/jira/browse/HDFS-15808) | Add metrics for FSNamesystem read/write lock hold long time |  Major | hdfs | tomscut | tomscut |
| [HDFS-15873](https://issues.apache.org/jira/browse/HDFS-15873) | Add namenode address in logs for block report |  Minor | datanode, hdfs | tomscut | tomscut |
| [HDFS-15906](https://issues.apache.org/jira/browse/HDFS-15906) | Close FSImage and FSNamesystem after formatting is complete |  Minor | . | tomscut | tomscut |
| [HDFS-15892](https://issues.apache.org/jira/browse/HDFS-15892) | Add metric for editPendingQ in FSEditLogAsync |  Minor | . | tomscut | tomscut |
| [HDFS-16078](https://issues.apache.org/jira/browse/HDFS-16078) | Remove unused parameters for DatanodeManager.handleLifeline() |  Minor | . | tomscut | tomscut |
| [YARN-10278](https://issues.apache.org/jira/browse/YARN-10278) | CapacityScheduler test framework ProportionalCapacityPreemptionPolicyMockFramework need some review |  Major | . | Gergely Pollák | Szilard Nemeth |
| [HDFS-15731](https://issues.apache.org/jira/browse/HDFS-15731) | Reduce threadCount for unit tests to reduce the memory usage |  Major | build, test | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17571](https://issues.apache.org/jira/browse/HADOOP-17571) | Upgrade com.fasterxml.woodstox:woodstox-core for security reasons |  Major | . | Viraj Jasani | Viraj Jasani |
| [HDFS-15895](https://issues.apache.org/jira/browse/HDFS-15895) | DFSAdmin#printOpenFiles has redundant String#format usage |  Minor | . | Viraj Jasani | Viraj Jasani |
| [HADOOP-17614](https://issues.apache.org/jira/browse/HADOOP-17614) | Bump netty to the latest 4.1.61 |  Blocker | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-17627](https://issues.apache.org/jira/browse/HADOOP-17627) | Backport to branch-3.2 HADOOP-17371, HADOOP-17621, HADOOP-17625 to update Jetty to 9.4.39 |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-15989](https://issues.apache.org/jira/browse/HDFS-15989) | Split TestBalancer into two classes |  Major | . | Viraj Jasani | Viraj Jasani |
| [HADOOP-17808](https://issues.apache.org/jira/browse/HADOOP-17808) | ipc.Client not setting interrupt flag after catching InterruptedException |  Minor | . | Viraj Jasani | Viraj Jasani |
| [HADOOP-17834](https://issues.apache.org/jira/browse/HADOOP-17834) | Bump aliyun-sdk-oss to 3.13.0 |  Major | . | Siyao Meng | Siyao Meng |
| [HADOOP-17955](https://issues.apache.org/jira/browse/HADOOP-17955) | Bump netty to the latest 4.1.68 |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-18061](https://issues.apache.org/jira/browse/HADOOP-18061) | Update the year to 2022 |  Major | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-18125](https://issues.apache.org/jira/browse/HADOOP-18125) | Utility to identify git commit / Jira fixVersion discrepancies for RC preparation |  Major | . | Viraj Jasani | Viraj Jasani |


