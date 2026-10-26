
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

## Release 3.5.0 - 2026-03-24



### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-19107](https://issues.apache.org/jira/browse/HADOOP-19107) | Drop support for HBase v1 timeline service & upgrade HBase v2 |  Major | . | Ayush Saxena | Ayush Saxena |
| [YARN-10058](https://issues.apache.org/jira/browse/YARN-10058) | Capacity Scheduler dispatcher hang when async thread crash |  Major | capacity scheduler | tuyu | Tao Yang |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-17302](https://issues.apache.org/jira/browse/HDFS-17302) | RBF: ProportionRouterRpcFairnessPolicyController-Sharing and isolation. |  Major | rbf | Jian Zhang | Jian Zhang |
| [HADOOP-19085](https://issues.apache.org/jira/browse/HADOOP-19085) | Compatibility Benchmark over HCFS Implementations |  Major | fs, test | Han Liu | Han Liu |
| [HADOOP-19050](https://issues.apache.org/jira/browse/HADOOP-19050) | Add S3 Access Grants Support in S3A |  Minor | fs/s3 | Jason Han | Jason Han |
| [HADOOP-19131](https://issues.apache.org/jira/browse/HADOOP-19131) | WrappedIO to export modern filesystem/statistics APIs in a reflection friendly form |  Major | fs, fs/azure, fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-19261](https://issues.apache.org/jira/browse/HADOOP-19261) | Support force close a DomainSocket for server service |  Major | . | Sammi Chen | Sammi Chen |
| [HDFS-17646](https://issues.apache.org/jira/browse/HDFS-17646) | Add Option to limit Balancer overUtilized nodes num in each iteration. |  Major | . | Zhaobo Huang | Zhaobo Huang |
| [HDFS-17657](https://issues.apache.org/jira/browse/HDFS-17657) | The balancer service supports httpserver. |  Minor | balancer & mover | Zhaobo Huang | Zhaobo Huang |
| [HDFS-17531](https://issues.apache.org/jira/browse/HDFS-17531) | RBF: Asynchronous router RPC |  Major | rbf | Jian Zhang | Jian Zhang |
| [HADOOP-19447](https://issues.apache.org/jira/browse/HADOOP-19447) | Add Caching Mechanism to HostResolver to Avoid Redundant Hostname Resolutions |  Major | common, yarn | Jiandan Yang |  |
| [HADOOP-19612](https://issues.apache.org/jira/browse/HADOOP-19612) | Add Support for Propagating Access Token via RPC Header in HDFS |  Major | hadoop-common | Tom McCormick | Tom McCormick |
| [HADOOP-19236](https://issues.apache.org/jira/browse/HADOOP-19236) | Integration of Volcano Engine TOS in Hadoop. |  Major | fs, tools | Jinglun | Zheng Hu |
| [HADOOP-19668](https://issues.apache.org/jira/browse/HADOOP-19668) | Add SubjectInheritingThread to restore pre JDK22 Subject behaviour in Threads |  Major | security | Istvan Toth | Istvan Toth |
| [YARN-11885](https://issues.apache.org/jira/browse/YARN-11885) | [Umbrella] YARN Capacity Scheduler UI |  Major | webapp | Benjamin Teke | Benjamin Teke |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-17283](https://issues.apache.org/jira/browse/HDFS-17283) | Change the name of variable SECOND in HdfsClientConfigKeys |  Trivial | hdfs-client | Haobo Zhang | Haobo Zhang |
| [YARN-11642](https://issues.apache.org/jira/browse/YARN-11642) | Fix Flaky Test TestTimelineAuthFilterForV2#testPutTimelineEntities |  Major | timelineservice | Shilun Fan | Shilun Fan |
| [HADOOP-18981](https://issues.apache.org/jira/browse/HADOOP-18981) | Move oncrpc/portmap from hadoop-nfs to hadoop-common |  Major | net | Xing Lin | Xing Lin |
| [HDFS-17291](https://issues.apache.org/jira/browse/HDFS-17291) | DataNode metric bytesWritten is not totally accurate in some situations. |  Major | datanode | Haobo Zhang | Haobo Zhang |
| [HADOOP-19034](https://issues.apache.org/jira/browse/HADOOP-19034) | Fix Download Maven Url Not Found |  Major | common | Shilun Fan | Shilun Fan |
| [HDFS-17331](https://issues.apache.org/jira/browse/HDFS-17331) | Fix Blocks are always -1 and DataNode\`s version are always UNKNOWN in federationhealth.html |  Major | . | lei w | lei w |
| [HDFS-17332](https://issues.apache.org/jira/browse/HDFS-17332) | DFSInputStream: avoid logging stacktrace until when we really need to fail a read request with a MissingBlockException |  Minor | hdfs | Xing Lin | Xing Lin |
| [YARN-11607](https://issues.apache.org/jira/browse/YARN-11607) | TestTimelineAuthFilterForV2 fails intermittently |  Major | timelineserver | Ayush Saxena | Susheel Gupta |
| [HDFS-17343](https://issues.apache.org/jira/browse/HDFS-17343) | Revert HDFS-16016. BPServiceActor to provide new thread to handle IBR |  Major | namenode | Shilun Fan | Shilun Fan |
| [HDFS-17311](https://issues.apache.org/jira/browse/HDFS-17311) | RBF: ConnectionManager creatorQueue should offer a pool that is not already in creatorQueue. |  Major | rbf | liuguanghua | liuguanghua |
| [HDFS-17293](https://issues.apache.org/jira/browse/HDFS-17293) | First packet data + checksum size will be set to 516 bytes when writing to a new block. |  Major | . | Haobo Zhang | Haobo Zhang |
| [HADOOP-19019](https://issues.apache.org/jira/browse/HADOOP-19019) | Parallel Maven Build Support for Apache Hadoop |  Major | build | Jialiang Cai | Jialiang Cai |
| [HADOOP-19039](https://issues.apache.org/jira/browse/HADOOP-19039) | Hadoop 3.4.0 Highlight big features and improvements. |  Major | common | Shilun Fan | Shilun Fan |
| [HDFS-17339](https://issues.apache.org/jira/browse/HDFS-17339) | BPServiceActor should skip cacheReport when one blockPool does not have CacheBlock on this DataNode |  Major | . | lei w | lei w |
| [HADOOP-19035](https://issues.apache.org/jira/browse/HADOOP-19035) | CrcUtil/CrcComposer should not throw IOException for non-IO |  Major | util | Tsz-wo Sze | Tsz-wo Sze |
| [HADOOP-19051](https://issues.apache.org/jira/browse/HADOOP-19051) | Hadoop 3.4.0 Big feature/improvement highlight addendum |  Major | common | Benjamin Teke | Benjamin Teke |
| [YARN-10889](https://issues.apache.org/jira/browse/YARN-10889) | [Umbrella] Queue Creation in Capacity Scheduler - Tech debts |  Major | capacity scheduler | Szilard Nemeth | Benjamin Teke |
| [YARN-11650](https://issues.apache.org/jira/browse/YARN-11650) | Refactoring variable names related multiNodePolicy in MultiNodePolicySpec, FiCaSchedulerApp and AbstractCSQueue |  Major | capacity scheduler, resourcemanager | Jiandan Yang | Jiandan Yang |
| [YARN-11653](https://issues.apache.org/jira/browse/YARN-11653) | Add Totoal\_Memory and Total\_Vcores columns in Nodes page |  Major | resourcemanager, ui | Jiandan Yang | Jiandan Yang |
| [HDFS-17359](https://issues.apache.org/jira/browse/HDFS-17359) | EC: recheck failed streamers should only after flushing all packets. |  Minor | ec | Haobo Zhang | Haobo Zhang |
| [HADOOP-18987](https://issues.apache.org/jira/browse/HADOOP-18987) | Corrections to Hadoop FileSystem API Definition |  Minor | documentation | Dieter De Paepe | Dieter De Paepe |
| [HDFS-17369](https://issues.apache.org/jira/browse/HDFS-17369) | Add uuid into datanode info for NameNodeMXBean |  Major | datanode | Haiyang Hu | Haiyang Hu |
| [HDFS-17353](https://issues.apache.org/jira/browse/HDFS-17353) | Fix failing RBF module tests |  Major | rbf | Pavel Subachev | Pavel Subachev |
| [YARN-11362](https://issues.apache.org/jira/browse/YARN-11362) | Fix several typos in YARN codebase of misspelled resource |  Major | yarn | Szilard Nemeth | Szilard Nemeth |
| [HADOOP-18993](https://issues.apache.org/jira/browse/HADOOP-18993) | S3A: Add option fs.s3a.classloader.isolation (#6301) |  Minor | fs/s3 | Antonio Murgia | Antonio Murgia |
| [HDFS-17342](https://issues.apache.org/jira/browse/HDFS-17342) | Fix DataNode may invalidates normal block causing missing block |  Major | datanode | Haiyang Hu | Haiyang Hu |
| [HADOOP-19059](https://issues.apache.org/jira/browse/HADOOP-19059) | S3A: update AWS SDK to 2.23.19 to support S3 Access Grants |  Minor | build, fs/s3 | Jason Han | Jason Han |
| [HDFS-17146](https://issues.apache.org/jira/browse/HDFS-17146) | Use the dfsadmin -reconfig command to initiate reconfiguration on all decommissioning datanodes. |  Major | dfsadmin, hdfs | Hualong Zhang | Hualong Zhang |
| [HDFS-17361](https://issues.apache.org/jira/browse/HDFS-17361) | DiskBalancer: Query command support with multiple nodes |  Major | datanode, diskbalancer | Haiyang Hu | Haiyang Hu |
| [HADOOP-19065](https://issues.apache.org/jira/browse/HADOOP-19065) | Update Protocol Buffers installation to 3.21.12 |  Major | build | Zhaobo Huang | Zhaobo Huang |
| [HDFS-17393](https://issues.apache.org/jira/browse/HDFS-17393) | Remove unused cond in FSNamesystem |  Major | . | ZanderXu | ZanderXu |
| [HDFS-17406](https://issues.apache.org/jira/browse/HDFS-17406) | Suppress UnresolvedPathException in hdfs router log |  Major | rbf | Haiyang Hu | Haiyang Hu |
| [HADOOP-19082](https://issues.apache.org/jira/browse/HADOOP-19082) | S3A: Update AWS SDK V2 to 2.24.6 |  Major | fs/s3 | Harshit Gupta | Harshit Gupta |
| [YARN-11657](https://issues.apache.org/jira/browse/YARN-11657) | Remove protobuf-2.5 as dependency of hadoop-yarn-api |  Major | api | Steve Loughran | Steve Loughran |
| [HDFS-17404](https://issues.apache.org/jira/browse/HDFS-17404) | Add Namenode info to log message when setting block keys from active nn |  Trivial | . | Joseph Dell'Aringa |  |
| [HDFS-17345](https://issues.apache.org/jira/browse/HDFS-17345) | Add a metrics to record block report generating cost time |  Minor | datanode | Haobo Zhang | Haobo Zhang |
| [HDFS-17391](https://issues.apache.org/jira/browse/HDFS-17391) | Adjust the checkpoint io buffer size to the chunk size |  Major | . | lei w | lei w |
| [HDFS-17380](https://issues.apache.org/jira/browse/HDFS-17380) | FsImageValidation: remove inaccessible nodes |  Major | tools | Tsz-wo Sze | Tsz-wo Sze |
| [HDFS-17433](https://issues.apache.org/jira/browse/HDFS-17433) | metrics sumOfActorCommandQueueLength should only record valid commands |  Minor | datanode | Haobo Zhang | Haobo Zhang |
| [YARN-11626](https://issues.apache.org/jira/browse/YARN-11626) | Optimization of the safeDelete operation in ZKRMStateStore |  Minor | resourcemanager | wangzhihui |  |
| [HDFS-17430](https://issues.apache.org/jira/browse/HDFS-17430) | RecoveringBlock will skip no live replicas when get block recovery command. |  Major | . | Haiyang Hu | Haiyang Hu |
| [HADOOP-19052](https://issues.apache.org/jira/browse/HADOOP-19052) | Hadoop use Shell command to get the count of the hard link which takes a lot of time |  Major | fs | liang yu |  |
| [HADOOP-19111](https://issues.apache.org/jira/browse/HADOOP-19111) | ipc client print client info message duplicate |  Trivial | common | Zhongkun Wu | Zhongkun Wu |
| [HADOOP-19127](https://issues.apache.org/jira/browse/HADOOP-19127) | Do not run unit tests on Windows pre-commit CI |  Major | build, test | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-19047](https://issues.apache.org/jira/browse/HADOOP-19047) | Support InMemory Tracking Of S3A Magic Commits |  Major | fs/s3 | Syed Shameerur Rahman | Syed Shameerur Rahman |
| [HDFS-17429](https://issues.apache.org/jira/browse/HDFS-17429) | Datatransfer sender.java LOG variable uses interface's, causing log fileName mistake |  Trivial | . | Zhongkun Wu |  |
| [HADOOP-19124](https://issues.apache.org/jira/browse/HADOOP-19124) | Update org.ehcache from 3.3.1 to 3.8.2. |  Major | common | Shilun Fan | Shilun Fan |
| [YARN-11663](https://issues.apache.org/jira/browse/YARN-11663) | [Federation] Add Cache Entity Nums Limit. |  Major | federation, yarn | Yuan Luo | Shilun Fan |
| [HDFS-17408](https://issues.apache.org/jira/browse/HDFS-17408) | Reduce the number of quota calculations in FSDirRenameOp |  Major | . | lei w | lei w |
| [HADOOP-19135](https://issues.apache.org/jira/browse/HADOOP-19135) | Remove Jcache 1.0-alpha |  Major | common | Shilun Fan | Shilun Fan |
| [YARN-11444](https://issues.apache.org/jira/browse/YARN-11444) | Improve YARN md documentation format |  Major | yarn | Shilun Fan | Shilun Fan |
| [YARN-11670](https://issues.apache.org/jira/browse/YARN-11670) | Add CallerContext in NodeManager |  Major | nodemanager | Jiandan Yang | Jiandan Yang |
| [HADOOP-18135](https://issues.apache.org/jira/browse/HADOOP-18135) | Produce Windows binaries of Hadoop |  Major | build | Gautham Banasandra | Gautham Banasandra |
| [HDFS-17461](https://issues.apache.org/jira/browse/HDFS-17461) | Fix spotbugs in PeerCache#getInternal |  Major | . | Haiyang Hu | Haiyang Hu |
| [HDFS-17478](https://issues.apache.org/jira/browse/HDFS-17478) | FSPermissionChecker to avoid obtaining a new AccessControlEnforcer instance before each authz call |  Major | namenode | Madhan Neethiraj | Madhan Neethiraj |
| [HDFS-17451](https://issues.apache.org/jira/browse/HDFS-17451) | RBF: fix spotbugs for redundant nullcheck of dns. |  Major | rbf | Jian Zhang | Jian Zhang |
| [HDFS-17367](https://issues.apache.org/jira/browse/HDFS-17367) | Add PercentUsed for Different StorageTypes in JMX |  Major | metrics, namenode | Hualong Zhang | Hualong Zhang |
| [HADOOP-19159](https://issues.apache.org/jira/browse/HADOOP-19159) | Fix hadoop-aws document for fs.s3a.committer.abort.pending.uploads |  Minor | documentation | Xi Chen | Xi Chen |
| [HADOOP-19151](https://issues.apache.org/jira/browse/HADOOP-19151) | Support configurable SASL mechanism |  Major | security | Tsz-wo Sze | Tsz-wo Sze |
| [HDFS-17456](https://issues.apache.org/jira/browse/HDFS-17456) | Fix the incorrect dfsused statistics of datanode when appending a file. |  Major | hdfs | fuchaohong | fuchaohong |
| [HADOOP-19146](https://issues.apache.org/jira/browse/HADOOP-19146) | noaa-cors-pds bucket access with global endpoint fails |  Minor | fs/s3, test | Viraj Jasani | Viraj Jasani |
| [HADOOP-19160](https://issues.apache.org/jira/browse/HADOOP-19160) | hadoop-auth should not depend on kerb-simplekdc |  Major | auth | Attila Doroszlai | Attila Doroszlai |
| [HDFS-17500](https://issues.apache.org/jira/browse/HDFS-17500) | Add missing operation name while authorizing some operations |  Major | namenode | Abhay | Abhay |
| [HDFS-17522](https://issues.apache.org/jira/browse/HDFS-17522) | JournalNode web interfaces lack configs for X-FRAME-OPTIONS protection |  Major | journal-node | wangzhihui | wangzhihui |
| [HADOOP-19152](https://issues.apache.org/jira/browse/HADOOP-19152) | Do not hard code security providers. |  Major | security | Tsz-wo Sze | Tsz-wo Sze |
| [HADOOP-18958](https://issues.apache.org/jira/browse/HADOOP-18958) |  Improve UserGroupInformation debug log |  Minor | common | wangzhihui | wangzhihui |
| [HADOOP-19172](https://issues.apache.org/jira/browse/HADOOP-19172) | Upgrade aws-java-sdk to 1.12.720 |  Minor | build, fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-17529](https://issues.apache.org/jira/browse/HDFS-17529) | RBF: Improve router state store cache entry deletion |  Major | hdfs, rbf | Felix N | Felix N |
| [HADOOP-19156](https://issues.apache.org/jira/browse/HADOOP-19156) | ZooKeeper based state stores use different ZK address configs |  Major | . | liu bin | liu bin |
| [YARN-11471](https://issues.apache.org/jira/browse/YARN-11471) | FederationStateStoreFacade Cache Support Caffeine |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11699](https://issues.apache.org/jira/browse/YARN-11699) | Diagnostics lacks userlimit info when user capacity has reached its maximum limit |  Major | capacity scheduler | Jiandan Yang | Jiandan Yang |
| [HADOOP-19193](https://issues.apache.org/jira/browse/HADOOP-19193) | Create orphan commit for website deployment |  Major | build, documentation | Cheng Pan | Cheng Pan |
| [HDFS-17539](https://issues.apache.org/jira/browse/HDFS-17539) | TestFileChecksum should not spin up a MiniDFSCluster for every test |  Minor | . | Felix N | Felix N |
| [HADOOP-19192](https://issues.apache.org/jira/browse/HADOOP-19192) | Log level is WARN when fail to load native hadoop libs |  Minor | documentation | Cheng Pan | Cheng Pan |
| [HADOOP-18931](https://issues.apache.org/jira/browse/HADOOP-18931) | FileSystem.getFileSystemClass() to log at debug the jar the .class came from |  Minor | fs | Steve Loughran | Viraj Jasani |
| [HDFS-17439](https://issues.apache.org/jira/browse/HDFS-17439) | Improve NNThroughputBenchmark to allow non super user to use the tool |  Major | benchmarks, namenode | Fateh Singh |  |
| [HADOOP-19203](https://issues.apache.org/jira/browse/HADOOP-19203) | WrappedIO BulkDelete API to raise IOEs as UncheckedIOExceptions |  Major | fs | Steve Loughran | Steve Loughran |
| [HADOOP-19194](https://issues.apache.org/jira/browse/HADOOP-19194) | Add test to find unshaded dependencies in the aws sdk |  Major | fs/s3 | Harshit Gupta | Harshit Gupta |
| [HADOOP-19195](https://issues.apache.org/jira/browse/HADOOP-19195) | Upgrade aws sdk v2 to 2.25.53 |  Major | fs/s3 | Harshit Gupta | Harshit Gupta |
| [HDFS-17534](https://issues.apache.org/jira/browse/HDFS-17534) | RBF: Support leader follower mode for multiple subclusters |  Major | rbf | Yuanbo Liu | Yuanbo Liu |
| [HADOOP-19218](https://issues.apache.org/jira/browse/HADOOP-19218) | Avoid DNS lookup while creating IPC Connection object |  Major | ipc | Viraj Jasani | Viraj Jasani |
| [HADOOP-19227](https://issues.apache.org/jira/browse/HADOOP-19227) | ipc.Server accelerate token negotiation only for the default mechanism |  Major | ipc | Tsz-wo Sze | Tsz-wo Sze |
| [HDFS-17576](https://issues.apache.org/jira/browse/HDFS-17576) | Support user defined auth Callback |  Major | security | Tsz-wo Sze | Tsz-wo Sze |
| [HADOOP-19228](https://issues.apache.org/jira/browse/HADOOP-19228) | ShellCommandFencer#setConfAsEnvVars should also replace '-' with '\_'. |  Major | . | fuchaohong | fuchaohong |
| [HDFS-16690](https://issues.apache.org/jira/browse/HDFS-16690) | Automatically format new unformatted JournalNodes using JournalNodeSyncer |  Major | journal-node | Steve Vaughan | Aswin M Prabhu |
| [HADOOP-19161](https://issues.apache.org/jira/browse/HADOOP-19161) | S3A: option "fs.s3a.performance.flags" to take list of performance flags |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-19244](https://issues.apache.org/jira/browse/HADOOP-19244) | Pullout arch-agnostic maven javadoc plugin configurations in hadoop-common |  Major | build, common | Cheng Pan | Cheng Pan |
| [HDFS-17575](https://issues.apache.org/jira/browse/HDFS-17575) | SaslDataTransferClient should use SaslParticipant to create messages |  Major | security | Tsz-wo Sze | Tsz-wo Sze |
| [HADOOP-17609](https://issues.apache.org/jira/browse/HADOOP-17609) | Make SM4 support optional for OpenSSL native code |  Major | native | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-19136](https://issues.apache.org/jira/browse/HADOOP-19136) | Upgrade commons-io to 2.16.1 |  Major | common | Shilun Fan | Shilun Fan |
| [HADOOP-19134](https://issues.apache.org/jira/browse/HADOOP-19134) | Use StringBuilder instead of StringBuffer |  Major | . | PJ Fanning | PJ Fanning |
| [HADOOP-19249](https://issues.apache.org/jira/browse/HADOOP-19249) | Getting NullPointerException when the unauthorised user tries to perform the key operation |  Major | common, security | Dhaval Shah | Dhaval Shah |
| [HDFS-17606](https://issues.apache.org/jira/browse/HDFS-17606) | Do not require implementing CustomizedCallbackHandler |  Major | security | Tsz-wo Sze | Tsz-wo Sze |
| [YARN-11711](https://issues.apache.org/jira/browse/YARN-11711) | Improve ServiceScheduler Code |  Minor | scheduler, yarn | Shilun Fan | Shilun Fan |
| [HDFS-17573](https://issues.apache.org/jira/browse/HDFS-17573) | Allow turn on both FSImage parallelization and compression |  Major | hdfs, namenode | Sungdong Kim | Sungdong Kim |
| [HADOOP-18487](https://issues.apache.org/jira/browse/HADOOP-18487) | Make protobuf 2.5 an optional runtime dependency. |  Major | build, ipc | Steve Loughran | Steve Loughran |
| [HADOOP-16928](https://issues.apache.org/jira/browse/HADOOP-16928) | [JDK13] Support HTML5 Javadoc |  Major | documentation | Akira Ajisaka | Cheng Pan |
| [YARN-11709](https://issues.apache.org/jira/browse/YARN-11709) | NodeManager should be shut down or blacklisted when it cannot run program "/var/lib/yarn-ce/bin/container-executor" |  Major | container-executor | Ferenc Erdelyi | Benjamin Teke |
| [YARN-11730](https://issues.apache.org/jira/browse/YARN-11730) | Resourcemanager node reporting enhancement for unregistered hosts |  Major | resourcemanager, yarn | Arjun Mohnot | Arjun Mohnot |
| [HDFS-17621](https://issues.apache.org/jira/browse/HDFS-17621) | Make PathIsNotEmptyDirectoryException terse |  Minor | hdfs | dzcxzl | dzcxzl |
| [HADOOP-19283](https://issues.apache.org/jira/browse/HADOOP-19283) | Move all DistCp execution logic to execute() |  Minor | common | Felix N | Felix N |
| [HADOOP-19165](https://issues.apache.org/jira/browse/HADOOP-19165) | Explore dropping protobuf 2.5.0 from the distro |  Major | build, yarn | Ayush Saxena | Ayush Saxena |
| [HADOOP-15760](https://issues.apache.org/jira/browse/HADOOP-15760) | Upgrade commons-collections to commons-collections4 |  Major | . | David Mollitor | Nihal Jain |
| [HADOOP-19281](https://issues.apache.org/jira/browse/HADOOP-19281) | MetricsSystemImpl should not print INFO message in CLI |  Major | metrics | Tsz-wo Sze | Sarveksha Yeshavantha Raju |
| [HDFS-17626](https://issues.apache.org/jira/browse/HDFS-17626) | Reduce lock contention at datanode startup |  Minor | . | Tao Li | Tao Li |
| [YARN-11734](https://issues.apache.org/jira/browse/YARN-11734) | Fix spotbugs in ServiceScheduler#load |  Major | yarn | Hualong Zhang | Hualong Zhang |
| [HDFS-17644](https://issues.apache.org/jira/browse/HDFS-17644) | Add log when a node selection is rejected by BPP UpgradeDomain |  Minor | hdfs | Lei Yang | Lei Yang |
| [HDFS-17607](https://issues.apache.org/jira/browse/HDFS-17607) | Reduce the number of times conf is loaded when DataNode startUp |  Major | . | lei w | lei w |
| [HADOOP-18682](https://issues.apache.org/jira/browse/HADOOP-18682) | Move hadoop docker scripts under the main source code |  Major | . | Ayush Saxena | Christos Bisias |
| [HADOOP-18610](https://issues.apache.org/jira/browse/HADOOP-18610) | ABFS OAuth2 Token Provider to support Azure Workload Identity for AKS |  Critical | tools | Haifeng Chen | Anuj Modi |
| [HADOOP-19306](https://issues.apache.org/jira/browse/HADOOP-19306) | Support user defined auth Callback in SaslRpcServer |  Major | ipc, security | Tsz-wo Sze | Tsz-wo Sze |
| [YARN-11738](https://issues.apache.org/jira/browse/YARN-11738) | Modernize SecretManager config |  Major | yarn | Bence Kosztolnik | Bence Kosztolnik |
| [HADOOP-19349](https://issues.apache.org/jira/browse/HADOOP-19349) | S3A : Improve Client Side Encryption Documentation |  Major | documentation, fs/s3 | Syed Shameerur Rahman | Syed Shameerur Rahman |
| [HADOOP-19358](https://issues.apache.org/jira/browse/HADOOP-19358) | Update command usage of appendToFile. |  Major | documentation | fuchaohong | fuchaohong |
| [HADOOP-19357](https://issues.apache.org/jira/browse/HADOOP-19357) | ABFS: Optimizations for Retry Handling and Throttling |  Minor | fs/azure | Manika Joshi | Manika Joshi |
| [YARN-7327](https://issues.apache.org/jira/browse/YARN-7327) | CapacityScheduler: Allocate containers asynchronously by default |  Trivial | . | Craig Ingram | Syed Shameerur Rahman |
| [HADOOP-19366](https://issues.apache.org/jira/browse/HADOOP-19366) | Install OpenJDk 17 in default ubuntu build container |  Major | build | Cheng Pan | Cheng Pan |
| [HADOOP-19371](https://issues.apache.org/jira/browse/HADOOP-19371) | JVM GC Metrics supports ZGC pause time and count |  Minor | common | dzcxzl | dzcxzl |
| [HDFS-17683](https://issues.apache.org/jira/browse/HDFS-17683) | Add metrics for acquiring dataset read/write lock |  Major | datanode | Haobo Zhang | Haobo Zhang |
| [HDFS-17696](https://issues.apache.org/jira/browse/HDFS-17696) | Optimize isBlockReplicatedOk method when scheduleReconStruction parameter is false |  Major | namenode | Haobo Zhang | Haobo Zhang |
| [HDFS-17695](https://issues.apache.org/jira/browse/HDFS-17695) | Fix javadoc for FSDirectory#resolvePath method. |  Trivial | namenode | Haobo Zhang | Haobo Zhang |
| [HADOOP-19278](https://issues.apache.org/jira/browse/HADOOP-19278) | S3A: remove option to delete directory markers |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-11752](https://issues.apache.org/jira/browse/YARN-11752) | Global Scheduler : Improve the container allocation time |  Major | capacity scheduler | Syed Shameerur Rahman | Syed Shameerur Rahman |
| [YARN-11751](https://issues.apache.org/jira/browse/YARN-11751) | Remove TestAppLevelTimelineCollector |  Major | timelineserver | Shilun Fan | Shilun Fan |
| [HADOOP-15984](https://issues.apache.org/jira/browse/HADOOP-15984) | Update jersey from 1.19 to 2.x |  Major | common, hdfs, mapreduce, yarn | Akira Ajisaka | Shilun Fan |
| [HDFS-17384](https://issues.apache.org/jira/browse/HDFS-17384) | HDFS NameNode Fine-Grained Locking Phase I |  Major | namenode | ZanderXu | ZanderXu |
| [HDFS-17711](https://issues.apache.org/jira/browse/HDFS-17711) | Change fsimage loading progress percentage discontinuous to continuous |  Minor | . | Sungdong Kim | Sungdong Kim |
| [HDFS-17704](https://issues.apache.org/jira/browse/HDFS-17704) | Fix TestDecommission and TestDecommissionWithBackoffMonitor often run timeout. |  Major | test | Haobo Zhang | Haobo Zhang |
| [HADOOP-19389](https://issues.apache.org/jira/browse/HADOOP-19389) | Optimize shell -text command I/O with multi-byte read. |  Minor | command, fs, fs/azure, fs/gcs, fs/s3 | Chris Nauroth | Chris Nauroth |
| [HADOOP-19401](https://issues.apache.org/jira/browse/HADOOP-19401) | Improve error message when OS can't identify the current user. |  Major | security | Chris Nauroth | Chris Nauroth |
| [YARN-11754](https://issues.apache.org/jira/browse/YARN-11754) | [JDK17] Fix SpotBugs Issues in YARN |  Minor | . | Shilun Fan | Shilun Fan |
| [HDFS-17721](https://issues.apache.org/jira/browse/HDFS-17721) | RBF: Allow routers to declare IP instead of hostname for admin address |  Minor | router | Felix N | Felix N |
| [HDFS-17719](https://issues.apache.org/jira/browse/HDFS-17719) | Upgrade JUnit from 4 to 5 in hadoop-hdfs-httpfs. |  Major | httpfs, test | Shilun Fan | Shilun Fan |
| [YARN-11761](https://issues.apache.org/jira/browse/YARN-11761) | Upgrade JUnit from 4 to 5 in hadoop-yarn-services-core. |  Major | test, yarn-service | Shilun Fan | Shilun Fan |
| [YARN-11760](https://issues.apache.org/jira/browse/YARN-11760) | Upgrade JUnit from 4 to 5 in hadoop-yarn-applications-distributedshell. |  Major | test, yarn | Shilun Fan | Shilun Fan |
| [YARN-11758](https://issues.apache.org/jira/browse/YARN-11758) | [UI2] On the Cluster Metrics page make the Resource Usage by Leaf Queues chart partition-aware |  Major | . | Ferenc Erdelyi | Ferenc Erdelyi |
| [HADOOP-19375](https://issues.apache.org/jira/browse/HADOOP-19375) | Organize JDK version-specific code in IDEA friendly approach |  Major | . | Cheng Pan | Cheng Pan |
| [HADOOP-19377](https://issues.apache.org/jira/browse/HADOOP-19377) | Avoid initializing useless HashMap in protocolImplMapArray. |  Minor | common | Haobo Zhang | Haobo Zhang |
| [YARN-11767](https://issues.apache.org/jira/browse/YARN-11767) | [UI2] upgrade moment.js to v2.30.1 |  Major | yarn-ui-v2 | Ferenc Erdelyi | Ferenc Erdelyi |
| [HDFS-17725](https://issues.apache.org/jira/browse/HDFS-17725) | DataNodeVolumeMetrics and BalancerMetrics class add MetricTag. |  Major | datanode | Zhaobo Huang | Zhaobo Huang |
| [YARN-11756](https://issues.apache.org/jira/browse/YARN-11756) | [UI2] Add the metrics from UI1 scheduler/application queues page to UI2 Queues page |  Major | . | Ferenc Erdelyi | Ferenc Erdelyi |
| [YARN-11757](https://issues.apache.org/jira/browse/YARN-11757) | [UI2] Add partition usage overview to the Queues page |  Major | yarn-ui-v2 | Ferenc Erdelyi | Ferenc Erdelyi |
| [YARN-11762](https://issues.apache.org/jira/browse/YARN-11762) | Upgrade JUnit from 4 to 5 in hadoop-yarn-server-globalpolicygenerator. |  Major | test, yarn | Shilun Fan | Shilun Fan |
| [HDFS-17745](https://issues.apache.org/jira/browse/HDFS-17745) | TestRouterMountTable should reset defaultNSEnable to true after each test method. |  Minor | . | Haobo Zhang | Haobo Zhang |
| [YARN-11782](https://issues.apache.org/jira/browse/YARN-11782) | [Federation] Fix incorrect error messages and improve failure handling. |  Major | federation | Shilun Fan | Shilun Fan |
| [HADOOP-19305](https://issues.apache.org/jira/browse/HADOOP-19305) | Fix ProcessEnvironment ClassCastException in Shell.java |  Major | . | Butao Zhang | Butao Zhang |
| [HDFS-17748](https://issues.apache.org/jira/browse/HDFS-17748) | Fix javadoc problems caused by HDFS-17496 |  Minor | datanode | Haobo Zhang | Haobo Zhang |
| [HDFS-17753](https://issues.apache.org/jira/browse/HDFS-17753) | Fix occasional failure of TestRouterHttpServerXFrame |  Minor | test | Haobo Zhang | Haobo Zhang |
| [HADOOP-19086](https://issues.apache.org/jira/browse/HADOOP-19086) | Update commons-logging to 1.3.0 |  Minor | build | Steve Loughran | Steve Loughran |
| [YARN-11786](https://issues.apache.org/jira/browse/YARN-11786) | Upgrade hadoop-yarn-server-timelineservice-hbase-tests to Support Trunk Compilation and Remove compatible hadoop version. |  Major | timelineservice | Shilun Fan | Shilun Fan |
| [HDFS-17754](https://issues.apache.org/jira/browse/HDFS-17754) | Add uriparser2 to notices |  Minor | hdfs-client | Chris Nauroth | Chris Nauroth |
| [HADOOP-19400](https://issues.apache.org/jira/browse/HADOOP-19400) | Expand specification and contract test coverage for InputStream reads. |  Major | documentation, fs, test | Chris Nauroth | Chris Nauroth |
| [MAPREDUCE-7503](https://issues.apache.org/jira/browse/MAPREDUCE-7503) | Fix ByteBuf leaks in TestShuffleChannelHandler |  Major | test | Istvan Toth | Istvan Toth |
| [MAPREDUCE-7504](https://issues.apache.org/jira/browse/MAPREDUCE-7504) | [JDK24] Remove terminally deprecated Thread.suspend from TestMergeManager |  Major | test | Istvan Toth | Istvan Toth |
| [HADOOP-19225](https://issues.apache.org/jira/browse/HADOOP-19225) | Upgrade Jetty to 9.4.57.v20241219 due to CVE-2024-8184 and other CVEs |  Major | build | Palakur Eshwitha Sai | PJ Fanning |
| [HADOOP-19475](https://issues.apache.org/jira/browse/HADOOP-19475) | Update Boost to 1.86.0 |  Major | build, native | Istvan Toth | Istvan Toth |
| [YARN-11787](https://issues.apache.org/jira/browse/YARN-11787) | [Federation] Enhance Exception Handling in FederationClientInterceptor#forceKillApplication |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11788](https://issues.apache.org/jira/browse/YARN-11788) | Remove unused unit tests. |  Major | resourcemanager, test | Shilun Fan | Shilun Fan |
| [HADOOP-19509](https://issues.apache.org/jira/browse/HADOOP-19509) | Add a config entry to make IPC.Client checkAsyncCall off by default |  Major | ipc | Haobo Zhang | Haobo Zhang |
| [HADOOP-18991](https://issues.apache.org/jira/browse/HADOOP-18991) | Remove commons-beanutils dependency from Hadoop 3 |  Major | common | Istvan Toth | Istvan Toth |
| [YARN-11795](https://issues.apache.org/jira/browse/YARN-11795) | [JDK19] Skip BaseFederationPoliciesTest.testReinitilializeBad3() on JVMs where Mockito cannot mock ByteBuffers |  Major | yarn | Istvan Toth | Istvan Toth |
| [YARN-11798](https://issues.apache.org/jira/browse/YARN-11798) | Precheck request separately to avoid redundant node checks and optimize performance for global scheduler. |  Major | capacityscheduler | Tao Yang | Tao Yang |
| [HADOOP-17178](https://issues.apache.org/jira/browse/HADOOP-17178) | [JDK 13] Javadoc HTML5 support |  Major | build, documentation | Akira Ajisaka | Cheng Pan |
| [HADOOP-19080](https://issues.apache.org/jira/browse/HADOOP-19080) | S3A to support writing to object lock buckets |  Minor | fs/s3 | Steve Loughran | Raphael Azzolini |
| [YARN-11805](https://issues.apache.org/jira/browse/YARN-11805) | [JDK18] Skip tests in TestTimelineWriterHBaseDown when InaccessibleObjectException is thrown |  Major | test, timelineservice | Istvan Toth | Istvan Toth |
| [HDFS-17718](https://issues.apache.org/jira/browse/HDFS-17718) | Upgrade JUnit from 4 to 5 in hadoop-hdfs-client. |  Major | hdfs-client, test | Shilun Fan | Hualong Zhang |
| [YARN-11807](https://issues.apache.org/jira/browse/YARN-11807) | [JDK18] Skip each test in hadoop-yarn-server-timelineservice-hbase-tests when InaccessibleObjectException is thrown |  Major | test, timelineservice | Istvan Toth | Istvan Toth |
| [YARN-11806](https://issues.apache.org/jira/browse/YARN-11806) | [JDK18] Skip tests that depend on custom SecurityManager when Java doesn't support it |  Major | test | Istvan Toth | Istvan Toth |
| [MAPREDUCE-7505](https://issues.apache.org/jira/browse/MAPREDUCE-7505) | [JDK18] Remove obsolete SecurityManager code from TestPipeApplication |  Major | test | Istvan Toth | Istvan Toth |
| [HADOOP-19546](https://issues.apache.org/jira/browse/HADOOP-19546) | Include cipher feature for HttpServer2 and SSLFactory |  Major | hadoop-common, hdfs, yarn | Bence Kosztolnik | Bence Kosztolnik |
| [HDFS-17226](https://issues.apache.org/jira/browse/HDFS-17226) | Building native libraries fails on Fedora 38 |  Major | libhdfs++, native | Kengo Seki | Kengo Seki |
| [HADOOP-19523](https://issues.apache.org/jira/browse/HADOOP-19523) | Upgrade to hadoop-thirdparty 1.4.0 |  Major | build | Steve Loughran | Steve Loughran |
| [HADOOP-19409](https://issues.apache.org/jira/browse/HADOOP-19409) | Upgrade JUnit from 4 to 5 in hadoop-cloud-storage-project |  Major | cloud-storage, test | Shilun Fan | Shilun Fan |
| [HADOOP-19545](https://issues.apache.org/jira/browse/HADOOP-19545) | [JDK21] Update to ApacheDS 2.0.0.AM27 and ldap-api 2.1.7 |  Major | auth, common, test | Istvan Toth | Istvan Toth |
| [HADOOP-19550](https://issues.apache.org/jira/browse/HADOOP-19550) | Migrate ViewFileSystemBaseTest to Junit 5 |  Major | . | Istvan Toth | Istvan Toth |
| [HDFS-17767](https://issues.apache.org/jira/browse/HDFS-17767) | [JDK18] Skip tests that depend on custom SecurityManager when Java doesn't support it |  Major | test | Istvan Toth | Istvan Toth |
| [HADOOP-19526](https://issues.apache.org/jira/browse/HADOOP-19526) | [JDK18] Skip tests in Hadoop common that depend on SecurityManager if the JVM does not support it |  Major | . | Istvan Toth | Istvan Toth |
| [HADOOP-19530](https://issues.apache.org/jira/browse/HADOOP-19530) | Add --enable-native-access=ALL-UNNAMED JVM option |  Major | . | Istvan Toth | Istvan Toth |
| [HADOOP-19571](https://issues.apache.org/jira/browse/HADOOP-19571) | Improve PrometheusMetricsSink#normalizeName performance |  Major | metrics | Ivan Andika | Ivan Andika |
| [HDFS-17637](https://issues.apache.org/jira/browse/HDFS-17637) | Fix spotbugs in HttpFSFileSystem#getXAttr |  Major | httpfs | Hualong Zhang | Hualong Zhang |
| [HADOOP-19568](https://issues.apache.org/jira/browse/HADOOP-19568) | [JDK24] Update byte-buddy to 1.15.11 |  Major | build | Istvan Toth | Istvan Toth |
| [HADOOP-8865](https://issues.apache.org/jira/browse/HADOOP-8865) | log warn when loading deprecated properties |  Major | conf | Jianbin Wei | Stamatis Zampetakis |
| [HADOOP-19413](https://issues.apache.org/jira/browse/HADOOP-19413) | Upgrade JUnit from 4 to 5 in hadoop-common-project. |  Major | hadoop-common | Shilun Fan | Shilun Fan |
| [HADOOP-19597](https://issues.apache.org/jira/browse/HADOOP-19597) | Log warning message on every set/get of a deprecated configuration property |  Major | hadoop-common | Stamatis Zampetakis | Stamatis Zampetakis |
| [YARN-11246](https://issues.apache.org/jira/browse/YARN-11246) | Upgrade JUnit from 4 to 5 in hadoop-yarn-server |  Major | test, yarn | Ashutosh Gupta | Ashutosh Gupta |
| [HADOOP-19384](https://issues.apache.org/jira/browse/HADOOP-19384) | S3A: Add support for ProfileCredentialsProvider |  Minor | fs/s3 | Venkatasubrahmanian Narayanan | Venkatasubrahmanian Narayanan |
| [HADOOP-19607](https://issues.apache.org/jira/browse/HADOOP-19607) | Remove workaround for protoc on M1 mac and unused property build.platform |  Major | build | Cheng Pan | Cheng Pan |
| [HADOOP-19535](https://issues.apache.org/jira/browse/HADOOP-19535) | S3A: Support WebIdentityTokenFileCredentialsProvider |  Major | fs/s3 | Syed Shameerur Rahman | Syed Shameerur Rahman |
| [HADOOP-19618](https://issues.apache.org/jira/browse/HADOOP-19618) | Replace AssumptionViolatedException with TestAbortedException |  Major | test | Shilun Fan | Shilun Fan |
| [HDFS-17811](https://issues.apache.org/jira/browse/HDFS-17811) | EC: DFSStripedInputStream supports retrying just like DFSInputStream |  Major | ec, hdfs-client | Haobo Zhang | Haobo Zhang |
| [YARN-11844](https://issues.apache.org/jira/browse/YARN-11844) | Support configuration of retry policy on GPU discovery |  Major | gpu, nodemanager | Chris Nauroth | Chris Nauroth |
| [HADOOP-19143](https://issues.apache.org/jira/browse/HADOOP-19143) | Upgrade commons-cli to 1.6.0. |  Major | build, common | Shilun Fan | Shilun Fan |
| [HADOOP-19657](https://issues.apache.org/jira/browse/HADOOP-19657) | Update 3.4.2 docs landing page to highlight changes shipped in the release |  Major | . | Ahmar Suhail | Ahmar Suhail |
| [HDFS-17365](https://issues.apache.org/jira/browse/HDFS-17365) | EC: Add extra redunency configuration in checkStreamerFailures to prevent data loss. |  Major | ec | Haobo Zhang | Haobo Zhang |
| [HADOOP-19661](https://issues.apache.org/jira/browse/HADOOP-19661) | Migrate CentOS 8 to Rocky Linux 8 in build env Dockerfile |  Major | build | Cheng Pan | Cheng Pan |
| [HADOOP-19659](https://issues.apache.org/jira/browse/HADOOP-19659) | Upgrade Debian 10 to 11 in build env Dockerfile |  Major | build | Cheng Pan | Cheng Pan |
| [HADOOP-19675](https://issues.apache.org/jira/browse/HADOOP-19675) | Close stale PRs updated over 100 days ago. |  Major | . | Xiaoqiao He | Xiaoqiao He |
| [HADOOP-19343](https://issues.apache.org/jira/browse/HADOOP-19343) | Add native support for GCS connector |  Major | fs | Abhishek Modi | Arunkumar Chacko |
| [HDFS-17830](https://issues.apache.org/jira/browse/HDFS-17830) | Fix failing TestZKDelegationTokenSecretManagerImpl due to static Curator reuse |  Major | rbf | Hualong Zhang | Hualong Zhang |
| [HADOOP-19684](https://issues.apache.org/jira/browse/HADOOP-19684) | Add JDK 21 to Ubuntu 20.04 docker development images |  Major | build | Istvan Toth | Istvan Toth |
| [HADOOP-19680](https://issues.apache.org/jira/browse/HADOOP-19680) | Update non-thirdparty Guava version to 32.0.1 |  Critical | . | Istvan Toth | Istvan Toth |
| [HADOOP-19166](https://issues.apache.org/jira/browse/HADOOP-19166) | [DOC] Drop Migrating from Apache Hadoop 1.x to Apache Hadoop 2.x |  Minor | . | Ayush Saxena | Atsuya Ishikawa |
| [HADOOP-19594](https://issues.apache.org/jira/browse/HADOOP-19594) | Bump Maven 3.9.10 |  Major | build | Cheng Pan | Cheng Pan |
| [HADOOP-19632](https://issues.apache.org/jira/browse/HADOOP-19632) | Upgrade nimbusds to 10.0.2 |  Major | build | Ananya Singh | Rohit Kumar |
| [YARN-11863](https://issues.apache.org/jira/browse/YARN-11863) | [JDK17] Remove JUnit4 NoExitSecurityManager |  Major | resourcemanager | Shilun Fan | Shilun Fan |
| [HADOOP-19701](https://issues.apache.org/jira/browse/HADOOP-19701) | Remove invalid \`licenses\` field of \`hadoop-aliyun\` SBOM by upgradding \`cyclonedx\` to 2.9.1 |  Major | build | Dongjoon Hyun | Dongjoon Hyun |
| [YARN-11864](https://issues.apache.org/jira/browse/YARN-11864) | [JDK17] Remove Usage of org.hamcrest in Unit Tests |  Major | . | Shilun Fan | Shilun Fan |
| [HADOOP-19282](https://issues.apache.org/jira/browse/HADOOP-19282) | S3A: STSClientFactory has hard-coded dependency on shaded HttpClient |  Major | fs/s3 | melin | Dmitry Lapshin |
| [HADOOP-19707](https://issues.apache.org/jira/browse/HADOOP-19707) | Surefire upgrade leads to increased report output, can cause Jenkins OOM |  Major | test | Michael Smith | Michael Smith |
| [HADOOP-19702](https://issues.apache.org/jira/browse/HADOOP-19702) | Update non-thirdparty Guava version to  33.4.8-jre |  Major | build | Istvan Toth | Istvan Toth |
| [HADOOP-19711](https://issues.apache.org/jira/browse/HADOOP-19711) | Upgrade hadoop3 docker scripts to 3.4.2 |  Major | build, docker | Shilun Fan | Shilun Fan |
| [HADOOP-19693](https://issues.apache.org/jira/browse/HADOOP-19693) | Update Java 24 to 25 in docker images |  Major | build | Istvan Toth | Istvan Toth |
| [YARN-11876](https://issues.apache.org/jira/browse/YARN-11876) | Fix AsyncDispatcher crash in TestAppManager |  Major | resourcemanager | Shilun Fan | Shilun Fan |
| [HADOOP-19720](https://issues.apache.org/jira/browse/HADOOP-19720) | Publish multi-arch hadoop-runner image to GitHub |  Major | build, docker | Attila Doroszlai | Attila Doroszlai |
| [MAPREDUCE-7521](https://issues.apache.org/jira/browse/MAPREDUCE-7521) | Fix TestUberAM failures after JUnit 5 migration |  Major | mapreduce-client | Shilun Fan | Shilun Fan |
| [HADOOP-19723](https://issues.apache.org/jira/browse/HADOOP-19723) | Build multi-arch hadoop image |  Major | docker | Attila Doroszlai | Attila Doroszlai |
| [HADOOP-19722](https://issues.apache.org/jira/browse/HADOOP-19722) | Pin robotframework version |  Minor | docker | Attila Doroszlai | Attila Doroszlai |
| [HADOOP-19012](https://issues.apache.org/jira/browse/HADOOP-19012) | Use CRC tables to speed up galoisFieldMultiply in CrcUtil |  Major | util | Tsz-wo Sze | Tsz-wo Sze |
| [HDFS-17846](https://issues.apache.org/jira/browse/HDFS-17846) | Enhance the stability of the unit test TestDirectoryScanner. |  Major | hdfs | Zhaobo Huang | Zhaobo Huang |
| [HADOOP-19359](https://issues.apache.org/jira/browse/HADOOP-19359) | Accelerate token negotiation for other similar mechanisms. |  Major | ipc, security | Tsz-wo Sze | Tsz-wo Sze |
| [HADOOP-19725](https://issues.apache.org/jira/browse/HADOOP-19725) | Upgrade SpotBugs Version to Support JDK 17 Compilation |  Major | build, hadoop-common | Shilun Fan | Shilun Fan |
| [HADOOP-19726](https://issues.apache.org/jira/browse/HADOOP-19726) | Add JDK 17 compile options for maven-surefire-plugin in hadoop-tos module |  Major | hadoop-tos | Shilun Fan | Shilun Fan |
| [HADOOP-19605](https://issues.apache.org/jira/browse/HADOOP-19605) | Upgrade Protobuf 3.25.5 for docker images |  Major | build | Cheng Pan | Cheng Pan |
| [HADOOP-19654](https://issues.apache.org/jira/browse/HADOOP-19654) | Upgrade AWS SDK to 2.35.4 |  Major | build, fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-19738](https://issues.apache.org/jira/browse/HADOOP-19738) | Upgrade to hadoop-thirdparty 1.5.0 |  Major | build | Steve Loughran | Steve Loughran |
| [YARN-11898](https://issues.apache.org/jira/browse/YARN-11898) | TestRMHA.testTransitionedToStandbyShouldNotHang hangs |  Major | test, yarn | Istvan Toth | Istvan Toth |
| [HADOOP-19670](https://issues.apache.org/jira/browse/HADOOP-19670) | [JDK22] Replace Thread with SubjectPreservingThread |  Major | common, hdfs, mapreduce, yarn | Istvan Toth | Istvan Toth |
| [HDFS-17853](https://issues.apache.org/jira/browse/HDFS-17853) | Support to make dfs.namenode.fs-limits.max-directory-items reconfigurable |  Major | namenode | caozhiqiang | caozhiqiang |
| [HADOOP-19708](https://issues.apache.org/jira/browse/HADOOP-19708) | volcano tos: disable shading when -DskipShade is set on a build |  Major | fs/volcano | Steve Loughran | Steve Loughran |
| [HADOOP-19731](https://issues.apache.org/jira/browse/HADOOP-19731) | Fix SpotBugs warnings introduced after SpotBugs version upgrade. |  Major | common, hdfs, mapreduce, yarn | Shilun Fan | Shilun Fan |
| [HADOOP-19574](https://issues.apache.org/jira/browse/HADOOP-19574) | [JDK22] Restore Subject propagation semantics for Java 22+ |  Critical | . | Istvan Toth | Istvan Toth |
| [HADOOP-19761](https://issues.apache.org/jira/browse/HADOOP-19761) | Upgrade jetty and http2-common to 9.4.58.v20250814 due to CVE-2025-5115 |  Minor | build, yarn | fuchaohong | fuchaohong |
| [YARN-10972](https://issues.apache.org/jira/browse/YARN-10972) | Remove stack traces from Jetty's response for Security Reasons |  Major | . | Tamas Domok | Tamas Domok |
| [HADOOP-19762](https://issues.apache.org/jira/browse/HADOOP-19762) | Move hadoop-gcp from hadoop-tools to hadoop-cloud-storage-project |  Major | fs/gcs | Chris Nauroth | Chris Nauroth |
| [YARN-11916](https://issues.apache.org/jira/browse/YARN-11916) | FileSystemTimelineReaderImpl vulnerable to race conditions |  Minor | timelineserver | Steve Loughran | Steve Loughran |
| [HADOOP-19777](https://issues.apache.org/jira/browse/HADOOP-19777) | Update trunk BUILDING.txt for Java 17 requirement |  Major | documentation | Chris Nauroth | Chris Nauroth |
| [HADOOP-19787](https://issues.apache.org/jira/browse/HADOOP-19787) | Remove unneeded deps from Hadoop-NFS |  Minor | nfs | Edward Capriolo | Edward Capriolo |
| [HADOOP-19789](https://issues.apache.org/jira/browse/HADOOP-19789) | Set -Pnative -Pyarn-ui in precommit mvninstall phase |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-19779](https://issues.apache.org/jira/browse/HADOOP-19779) | Use enforcer to fail fast if building with Java \<17 |  Major | build | Chris Nauroth | Chris Nauroth |
| [YARN-11922](https://issues.apache.org/jira/browse/YARN-11922) | ResourceManager not update SecretManager keysize immediately if recovery is on |  Minor | yarn | Bence Kosztolnik | Bence Kosztolnik |
| [HDFS-17873](https://issues.apache.org/jira/browse/HDFS-17873) | Use a password retrieval method (e.g. conf.getPassword) in HikariDataSourceConnectionFactory and MySQLStateStoreHikariDataSourceConnectionFactory, similar to HttpServer2 |  Major | rbf, security | Hiroki Egawa | Hiroki Egawa |
| [HADOOP-19798](https://issues.apache.org/jira/browse/HADOOP-19798) | Add Maven Wrapper |  Major | build | Cheng Pan | Cheng Pan |
| [HADOOP-19808](https://issues.apache.org/jira/browse/HADOOP-19808) | Jenkins switch from Debian 11 to Debian 13 |  Major | build | Cheng Pan | Cheng Pan |
| [HADOOP-19812](https://issues.apache.org/jira/browse/HADOOP-19812) | Reduce boost source code download size |  Major | build | Cheng Pan | Cheng Pan |
| [MAPREDUCE-7525](https://issues.apache.org/jira/browse/MAPREDUCE-7525) | Improve TestDFSIO to support different InputFormat |  Major | mapreduce-client | zhixingheyi-tian |  |
| [HADOOP-19807](https://issues.apache.org/jira/browse/HADOOP-19807) | Enable cross-platform support for dev container |  Major | build | Cheng Pan | Cheng Pan |
| [MAPREDUCE-7532](https://issues.apache.org/jira/browse/MAPREDUCE-7532) | Remove accidently tracked vim swp file |  Major | mapreduce-client | Cheng Pan | Cheng Pan |
| [HADOOP-19822](https://issues.apache.org/jira/browse/HADOOP-19822) | Upgrade Avro to 1.11.5 |  Major | common | Chris Nauroth | Chris Nauroth |
| [YARN-11897](https://issues.apache.org/jira/browse/YARN-11897) | NodeManager REST API backward compatibility with Jersey1 |  Major | yarn | Peter Szucs | chhinlinghean |
| [YARN-11877](https://issues.apache.org/jira/browse/YARN-11877) | Resolve JAXB IllegalAnnotation issue in TimelineEntity with Jersey 2.x |  Major | timelineservice | Shilun Fan | Shilun Fan |
| [HADOOP-19829](https://issues.apache.org/jira/browse/HADOOP-19829) | Bump lz4-java 1.10.4 |  Critical | build | Cheng Pan | Cheng Pan |
| [HADOOP-19827](https://issues.apache.org/jira/browse/HADOOP-19827) | Upgrade kafka-clients to 3.9.2 |  Major | build | Cheng Pan | Cheng Pan |
| [HADOOP-19834](https://issues.apache.org/jira/browse/HADOOP-19834) | Broken links and whitespace misalignments in S3A index.html |  Trivial | documentation, fs/s3 | Enrico Minack | Enrico Minack |
| [HADOOP-19788](https://issues.apache.org/jira/browse/HADOOP-19788) | Upgrade Netty due to CVE-2025-67735 |  Major | . | Edward Capriolo | Edward Capriolo |
| [HDFS-4277](https://issues.apache.org/jira/browse/HDFS-4277) | SocketTimeoutExceptions over the DataXciever service of a DN should print the DFSClient ID |  Minor | datanode | Harsh J | Amit Balode |
| [HADOOP-19837](https://issues.apache.org/jira/browse/HADOOP-19837) | Bump org.apache.zookeeper:zookeeper from 3.8.4 to 3.8.6 in hadoop-project |  Major | build | Jared Jia | Jared Jia |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-17289](https://issues.apache.org/jira/browse/HDFS-17289) | Considering the size of non-lastBlocks equals to complete block size can cause append failure. |  Major | . | Haobo Zhang | Haobo Zhang |
| [HADOOP-19031](https://issues.apache.org/jira/browse/HADOOP-19031) | CVE-2024-23454: Apache Hadoop: Temporary File Local Information Disclosure |  Major | security | Xiaoqiao He | Xiaoqiao He |
| [HDFS-17337](https://issues.apache.org/jira/browse/HDFS-17337) | RPC RESPONSE time seems not exactly accurate when using FSEditLogAsync. |  Major | namenode | Haobo Zhang | Haobo Zhang |
| [HADOOP-18894](https://issues.apache.org/jira/browse/HADOOP-18894) | upgrade sshd-core due to CVEs |  Major | build, common | PJ Fanning | PJ Fanning |
| [YARN-11645](https://issues.apache.org/jira/browse/YARN-11645) | Fix flaky json assert tests in TestRMWebServices |  Major | capacityscheduler | Tamas Domok | Tamas Domok |
| [HDFS-17346](https://issues.apache.org/jira/browse/HDFS-17346) | Fix DirectoryScanner check mark the normal blocks as corrupt. |  Major | datanode | Haiyang Hu | Haiyang Hu |
| [YARN-11641](https://issues.apache.org/jira/browse/YARN-11641) | Can't update a queue hierarchy in absolute mode when the configured capacities are zero |  Major | capacityscheduler | Tamas Domok | Tamas Domok |
| [YARN-11639](https://issues.apache.org/jira/browse/YARN-11639) | ConcurrentModificationException and NPE in PriorityUtilizationQueueOrderingPolicy |  Major | capacity scheduler | Ferenc Erdelyi | Ferenc Erdelyi |
| [HADOOP-19061](https://issues.apache.org/jira/browse/HADOOP-19061) | Capture exception in rpcRequestSender.start() in IPC.Connection.run() |  Major | ipc | Xing Lin | Xing Lin |
| [HADOOP-19049](https://issues.apache.org/jira/browse/HADOOP-19049) | Class loader leak caused by StatisticsDataReferenceCleaner thread |  Major | common | Jia Fan | Jia Fan |
| [HDFS-17376](https://issues.apache.org/jira/browse/HDFS-17376) | Distcp creates Factor 1 replication file on target if Source is EC |  Major | distcp | Sadanand Shenoy | Sadanand Shenoy |
| [HDFS-17181](https://issues.apache.org/jira/browse/HDFS-17181) | WebHDFS not considering whether a DN is good when called from outside the cluster |  Major | namenode, webhdfs | Lars Francke | Lars Francke |
| [HDFS-17358](https://issues.apache.org/jira/browse/HDFS-17358) | EC: infinite lease recovery caused by the length of RWR equals to zero. |  Major | ec | Haobo Zhang | Haobo Zhang |
| [HDFS-17299](https://issues.apache.org/jira/browse/HDFS-17299) | HDFS is not rack failure tolerant while creating a new file. |  Critical | . | Rushabh Shah | Ritesh |
| [HDFS-17422](https://issues.apache.org/jira/browse/HDFS-17422) |  Enhance the stability of the unit test TestDFSAdmin |  Major | . | lei w | lei w |
| [YARN-5305](https://issues.apache.org/jira/browse/YARN-5305) | Yarn Application Log Aggregation fails due to NM can not get correct HDFS delegation token III |  Major | yarn | Xianyin Xin | Peter Szucs |
| [HDFS-17354](https://issues.apache.org/jira/browse/HDFS-17354) | Delay invoke  clearStaleNamespacesInRouterStateIdContext during router start up |  Major | . | lei w | lei w |
| [HADOOP-19116](https://issues.apache.org/jira/browse/HADOOP-19116) | update to zookeeper client 3.8.4 due to  CVE-2024-23944 |  Major | CVE | PJ Fanning | PJ Fanning |
| [HADOOP-19088](https://issues.apache.org/jira/browse/HADOOP-19088) | upgrade to jersey-json 1.22.0 |  Major | build | PJ Fanning | PJ Fanning |
| [HDFS-17368](https://issues.apache.org/jira/browse/HDFS-17368) | HA: Standy should exit safemode when resources are from low available |  Major | ha, namenode | Zilong Zhu | Zilong Zhu |
| [HDFS-17216](https://issues.apache.org/jira/browse/HDFS-17216) | When distcp handle the small files, the bandwidth parameter will be invalid, resulting in serious overspeed behavior |  Major | distcp | xiaojunxiang | xiaojunxiang |
| [HDFS-17443](https://issues.apache.org/jira/browse/HDFS-17443) | TestNameEditsConfigs does not check null before closing fileSys and cluster |  Major | . | rstest | rstest |
| [YARN-11668](https://issues.apache.org/jira/browse/YARN-11668) | Potential concurrent modification exception for node attributes of node manager |  Major | . | Junfan Zhang | Junfan Zhang |
| [HDFS-17103](https://issues.apache.org/jira/browse/HDFS-17103) | Fix file system cleanup in TestNameEditsConfigs |  Critical | . | rstest | rstest |
| [HDFS-17448](https://issues.apache.org/jira/browse/HDFS-17448) |  Enhance the stability of the unit test TestDiskBalancerCommand |  Major | . | Haiyang Hu | Haiyang Hu |
| [HADOOP-19115](https://issues.apache.org/jira/browse/HADOOP-19115) | upgrade to nimbus-jose-jwt 9.37.2 due to CVE |  Major | build, CVE | PJ Fanning | PJ Fanning |
| [HDFS-17449](https://issues.apache.org/jira/browse/HDFS-17449) | Fix ill-formed decommission host name and port pair triggers IndexOutOfBound error |  Major | . | rstest | rstest |
| [HDFS-17453](https://issues.apache.org/jira/browse/HDFS-17453) | IncrementalBlockReport can have race condition with Edit Log Tailer |  Major | auto-failover, ha, hdfs, namenode | Danny Becker | Danny Becker |
| [HDFS-17455](https://issues.apache.org/jira/browse/HDFS-17455) | Fix Client throw IndexOutOfBoundsException in DFSInputStream#fetchBlockAt |  Major | dfsclient | Haiyang Hu | Haiyang Hu |
| [HDFS-17465](https://issues.apache.org/jira/browse/HDFS-17465) | RBF: Use ProportionRouterRpcFairnessPolicyController get  “java.Lang. Error: Maximum permit count exceeded” |  Blocker | rbf | Xiping Zhang | Xiping Zhang |
| [HDFS-17383](https://issues.apache.org/jira/browse/HDFS-17383) | Datanode current block token should come from active NameNode in HA mode |  Major | . | lei w | lei w |
| [HADOOP-19130](https://issues.apache.org/jira/browse/HADOOP-19130) | FTPFileSystem rename with full qualified path broken |  Major | fs | shawn | shawn |
| [YARN-11684](https://issues.apache.org/jira/browse/YARN-11684) | PriorityQueueComparator violates general contract |  Major | capacityscheduler | Tamas Domok | Tamas Domok |
| [HDFS-17499](https://issues.apache.org/jira/browse/HDFS-17499) | removeQueuedBlock in PendingDataNodeMessages has memory leak |  Major | namenode | Danny Becker |  |
| [HDFS-17471](https://issues.apache.org/jira/browse/HDFS-17471) | Correct the percentage of sample range. |  Major | hdfs | fuchaohong | fuchaohong |
| [HDFS-17508](https://issues.apache.org/jira/browse/HDFS-17508) | RBF: MembershipStateStore can overwrite valid records when refreshing the local cache |  Major | rbf | Danny Becker | Danny Becker |
| [HDFS-17503](https://issues.apache.org/jira/browse/HDFS-17503) | Unreleased volume references because of OOM |  Major | . | Zilong Zhu | Zilong Zhu |
| [HDFS-17488](https://issues.apache.org/jira/browse/HDFS-17488) | DN can fail IBRs with NPE when a volume is removed |  Major | hdfs | Felix N | Felix N |
| [HADOOP-19170](https://issues.apache.org/jira/browse/HADOOP-19170) | Fixes compilation issues on Mac |  Major | . | Chenyu Zheng | Chenyu Zheng |
| [HDFS-17099](https://issues.apache.org/jira/browse/HDFS-17099) | Fix Null Pointer Exception when stop namesystem in HDFS |  Major | . | rstest | rstest |
| [HDFS-17520](https://issues.apache.org/jira/browse/HDFS-17520) | TestDFSAdmin.testAllDatanodesReconfig and TestDFSAdmin.testDecommissionDataNodesReconfig failed |  Major | hdfs | ZanderXu | ZanderXu |
| [HADOOP-19073](https://issues.apache.org/jira/browse/HADOOP-19073) | WASB: Fix connection leak in FolderRenamePending |  Major | fs/azure | xy | xy |
| [MAPREDUCE-7474](https://issues.apache.org/jira/browse/MAPREDUCE-7474) | [ABFS] Improve commit resilience and performance in Manifest Committer |  Major | client | Steve Loughran | Steve Loughran |
| [HADOOP-19167](https://issues.apache.org/jira/browse/HADOOP-19167) | Change of Codec configuration does not work |  Minor | compress | Zhikai Hu |  |
| [HDFS-17509](https://issues.apache.org/jira/browse/HDFS-17509) | RBF: Fix ClientProtocol.concat  will throw NPE if tgr is a empty file. |  Minor | . | liuguanghua | liuguanghua |
| [MAPREDUCE-7475](https://issues.apache.org/jira/browse/MAPREDUCE-7475) | Fix non-idempotent unit tests |  Minor | test | Kaiyao Ke | Kaiyao Ke |
| [HADOOP-13147](https://issues.apache.org/jira/browse/HADOOP-13147) | Constructors must not call overrideable methods in PureJavaCrc32C |  Blocker | . | Sebb | Sebb |
| [HADOOP-19163](https://issues.apache.org/jira/browse/HADOOP-19163) | Upgrade protobuf version to 3.25.3 |  Major | build, hadoop-thirdparty | Bilwa S T | Bilwa S T |
| [HADOOP-18962](https://issues.apache.org/jira/browse/HADOOP-18962) | Upgrade kafka to 3.4.0 |  Major | build | D M Murali Krishna Reddy | D M Murali Krishna Reddy |
| [HADOOP-19188](https://issues.apache.org/jira/browse/HADOOP-19188) | TestHarFileSystem and TestFilterFileSystem failing after bulk delete API added |  Minor | fs, test | Steve Loughran | Mukund Thakur |
| [HADOOP-19114](https://issues.apache.org/jira/browse/HADOOP-19114) | upgrade to commons-compress 1.26.1 due to cves |  Major | build, CVE | PJ Fanning | PJ Fanning |
| [HADOOP-19196](https://issues.apache.org/jira/browse/HADOOP-19196) | Bulk delete api doesn't take the path to delete as the base path |  Minor | fs | Steve Loughran | Mukund Thakur |
| [HDFS-17551](https://issues.apache.org/jira/browse/HDFS-17551) | Fix unit test failure caused by HDFS-17464 |  Minor | . | Haobo Zhang | Haobo Zhang |
| [YARN-11701](https://issues.apache.org/jira/browse/YARN-11701) | Enhance Federation Cache Clean Conditions |  Major | federation | Shilun Fan | Shilun Fan |
| [HDFS-17528](https://issues.apache.org/jira/browse/HDFS-17528) | FsImageValidation: set txid when saving a new image |  Major | tools | Tsz-wo Sze | Tsz-wo Sze |
| [HADOOP-14451](https://issues.apache.org/jira/browse/HADOOP-14451) | Deadlock in NativeIO |  Blocker | . | Ajith S | Vinayakumar B |
| [HADOOP-19215](https://issues.apache.org/jira/browse/HADOOP-19215) | Fix unit tests testSlowConnection and testBadSetup failed in TestRPC |  Minor | test | Haobo Zhang | Haobo Zhang |
| [HDFS-17564](https://issues.apache.org/jira/browse/HDFS-17564) | EC: Fix the issue of inaccurate metrics when decommission mark busy DN |  Major | . | Haiyang Hu | Haiyang Hu |
| [HDFS-17557](https://issues.apache.org/jira/browse/HDFS-17557) | Fix bug for TestRedundancyMonitor#testChooseTargetWhenAllDataNodesStop |  Major | . | Haiyang Hu | Haiyang Hu |
| [HDFS-17555](https://issues.apache.org/jira/browse/HDFS-17555) | Fix NumberFormatException of NNThroughputBenchmark when configured dfs.blocksize. |  Major | benchmarks, hdfs | wangzhongwei | wangzhongwei |
| [HDFS-17566](https://issues.apache.org/jira/browse/HDFS-17566) | Got wrong sorted block order when StorageType is considered. |  Major | . | Chenyu Zheng | Chenyu Zheng |
| [HADOOP-19222](https://issues.apache.org/jira/browse/HADOOP-19222) | Switch yum repo baseurl due to CentOS 7 sunset |  Major | build | Cheng Pan | Cheng Pan |
| [HDFS-17574](https://issues.apache.org/jira/browse/HDFS-17574) | Make NNThroughputBenchmark support human-friendly units about blocksize. |  Major | benchmarks, hdfs | wangzhongwei | wangzhongwei |
| [HADOOP-19246](https://issues.apache.org/jira/browse/HADOOP-19246) | Update the yasm rpm download address |  Major | . | Chenyu Zheng | Chenyu Zheng |
| [HADOOP-19153](https://issues.apache.org/jira/browse/HADOOP-19153) | hadoop-common still exports logback as a transitive dependency |  Major | build, common | Steve Loughran | Steve Loughran |
| [HADOOP-19180](https://issues.apache.org/jira/browse/HADOOP-19180) | EC: Fix calculation errors caused by special index order |  Critical | . | Chenyu Zheng | Chenyu Zheng |
| [HDFS-17605](https://issues.apache.org/jira/browse/HDFS-17605) | Reduced memory overhead of TestBPOfferService |  Major | test | Stephen O'Donnell | Stephen O'Donnell |
| [HADOOP-18786](https://issues.apache.org/jira/browse/HADOOP-18786) | Hadoop build depends on archives.apache.org |  Critical | build | Christopher Tubbs | Christopher Tubbs |
| [HADOOP-18542](https://issues.apache.org/jira/browse/HADOOP-18542) | Azure Token provider requires tenant and client IDs despite being optional |  Major | fs/azure, hadoop-thirdparty | Carl |  |
| [HDFS-16084](https://issues.apache.org/jira/browse/HDFS-16084) | getJNIEnv() returns invalid pointer when called twice after getGlobalJNIEnv() failed |  Major | libhdfs | Antoine Pitrou | kevin cai |
| [HDFS-17599](https://issues.apache.org/jira/browse/HDFS-17599) | EC: Fix the mismatch between locations and indices for mover |  Major | balancer & mover | Tao Li | Tao Li |
| [HADOOP-19248](https://issues.apache.org/jira/browse/HADOOP-19248) | Protobuf code generate and replace should happen together |  Major | common | Cheng Pan | Cheng Pan |
| [HADOOP-19250](https://issues.apache.org/jira/browse/HADOOP-19250) | Fix test TestServiceInterruptHandling.testRegisterAndRaise |  Major | test | Chenyu Zheng | Chenyu Zheng |
| [HADOOP-19271](https://issues.apache.org/jira/browse/HADOOP-19271) | [ABFS]: NPE in AbfsManagedApacheHttpConnection.toString() when not connected |  Blocker | fs/azure | Steve Loughran | Pranav Saxena |
| [HADOOP-19277](https://issues.apache.org/jira/browse/HADOOP-19277) | Files and directories mixed up in TreeScanResults#dump |  Trivial | test | Attila Doroszlai | Attila Doroszlai |
| [HADOOP-19272](https://issues.apache.org/jira/browse/HADOOP-19272) | S3A: AWS SDK 2.25.53 warnings logged about transfer manager not using CRT client |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-19164](https://issues.apache.org/jira/browse/HADOOP-19164) | Hadoop CLI MiniCluster is broken |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-17526](https://issues.apache.org/jira/browse/HDFS-17526) | getMetadataInputStream should use getShareDeleteFileInputStream for windows |  Major | datanode | Danny Becker | Danny Becker |
| [YARN-11560](https://issues.apache.org/jira/browse/YARN-11560) | Fix NPE bug when multi-node enabled with schedule asynchronously |  Blocker | capacity scheduler | wangzhongwei | wangzhongwei |
| [HADOOP-19285](https://issues.apache.org/jira/browse/HADOOP-19285) | [ABFS] Restore ETAGS\_AVAILABLE to abfs path capabilities |  Critical | fs/azure | Steve Loughran | Steve Loughran |
| [YARN-11733](https://issues.apache.org/jira/browse/YARN-11733) | Fix the order of updating CPU controls with cgroup v1 |  Major | yarn | Peter Szucs | Peter Szucs |
| [YARN-11702](https://issues.apache.org/jira/browse/YARN-11702) | Fix Yarn over allocating containers |  Major | capacity scheduler, fairscheduler, scheduler, yarn | Syed Shameerur Rahman | Syed Shameerur Rahman |
| [HDFS-17624](https://issues.apache.org/jira/browse/HDFS-17624) | Fix DFSNetworkTopology#chooseRandomWithStorageType() availableCount when excluded node is not in selected scope. |  Major | . | fuchaohong | fuchaohong |
| [HADOOP-19290](https://issues.apache.org/jira/browse/HADOOP-19290) | Operating on / in ChecksumFileSystem throws NPE |  Major | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-19280](https://issues.apache.org/jira/browse/HADOOP-19280) | ABFS: Initialize ABFS client timer only when metric collection is enabled |  Major | fs/azure | Manish Bhatt | Manish Bhatt |
| [HADOOP-19294](https://issues.apache.org/jira/browse/HADOOP-19294) | NPE on maven enforcer with -Pnative on arm mac |  Major | build | Steve Loughran | Steve Loughran |
| [HADOOP-19288](https://issues.apache.org/jira/browse/HADOOP-19288) | hadoop-client-runtime exclude dnsjava InetAddressResolverProvider |  Major | build | dzcxzl | dzcxzl |
| [YARN-11708](https://issues.apache.org/jira/browse/YARN-11708) | Setting maximum-application-lifetime using AQCv2 templates doesn't  apply on the first submitted app |  Major | . | Benjamin Teke | Susheel Gupta |
| [HADOOP-19299](https://issues.apache.org/jira/browse/HADOOP-19299) | ConcurrentModificationException in HttpReferrerAuditHeader |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [MAPREDUCE-7494](https://issues.apache.org/jira/browse/MAPREDUCE-7494) | File stream leak when LineRecordReader is interrupted |  Minor | client | Davin Tjong | Davin Tjong |
| [YARN-11732](https://issues.apache.org/jira/browse/YARN-11732) | Potential NPE when calling SchedulerNode#reservedContainer for CapacityScheduler |  Major | capacityscheduler | Tao Yang | Tao Yang |
| [HDFS-17631](https://issues.apache.org/jira/browse/HDFS-17631) | Fix RedundantEditLogInputStream.nextOp()  state error when EditLogInputStream.skipUntil() throw IOException |  Major | . | liuguanghua | liuguanghua |
| [HDFS-17636](https://issues.apache.org/jira/browse/HDFS-17636) | Don't add declspec for Windows |  Blocker | libhdfs | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-19309](https://issues.apache.org/jira/browse/HADOOP-19309) | S3A CopyFromLocalFile operation fails when the source file does not contain file scheme. |  Major | fs/s3 | Syed Shameerur Rahman | Syed Shameerur Rahman |
| [HDFS-17654](https://issues.apache.org/jira/browse/HDFS-17654) | Fix bugs in TestRouterMountTable |  Trivial | rbf, test | Haobo Zhang | Haobo Zhang |
| [YARN-11191](https://issues.apache.org/jira/browse/YARN-11191) | Global Scheduler refreshQueue cause deadLock |  Major | capacity scheduler | ben yang | Tamas Domok |
| [HADOOP-18583](https://issues.apache.org/jira/browse/HADOOP-18583) | hadoop checknative fails to load openssl 3.x |  Major | native | Sebastian Klemke | Sebastian Klemke |
| [HADOOP-19106](https://issues.apache.org/jira/browse/HADOOP-19106) | [ABFS] All tests of. ITestAzureBlobFileSystemAuthorization fails with NPE |  Major | fs/azure | Mukund Thakur | Anuj Modi |
| [HADOOP-19342](https://issues.apache.org/jira/browse/HADOOP-19342) | SaslRpcServer.AuthMethod print INFO messages in client side |  Major | security | Tsz-wo Sze | Tsz-wo Sze |
| [HDFS-17668](https://issues.apache.org/jira/browse/HDFS-17668) | Treat null SASL negotiated QOP as auth in DataTransferSaslUtil#checkSaslComplete() |  Major | . | Istvan Toth | Istvan Toth |
| [HDFS-17679](https://issues.apache.org/jira/browse/HDFS-17679) | Use saslClient#hasInitialResponse() instead of heuristics in SaslParticipant#createFirstMessage() |  Major | security | Istvan Toth | Istvan Toth |
| [HDFS-17682](https://issues.apache.org/jira/browse/HDFS-17682) | Fix incorrect command of fs2img tool. |  Major | documentation | fuchaohong | fuchaohong |
| [HDFS-17648](https://issues.apache.org/jira/browse/HDFS-17648) | Fix BalancerMetrics duplicate registration issue. |  Major | . | Zhaobo Huang | Zhaobo Huang |
| [HADOOP-19337](https://issues.apache.org/jira/browse/HADOOP-19337) | Fix ZKFailoverController NPE issue due to integer overflow in parseInt when initHM. |  Trivial | common | rstest | rstest |
| [HADOOP-19339](https://issues.apache.org/jira/browse/HADOOP-19339) | OutofBounds Exception due to assumption about buffer size in BlockCompressorStream |  Major | common | rstest | rstest |
| [HADOOP-19360](https://issues.apache.org/jira/browse/HADOOP-19360) | Disable releases for apache.snapshots repo |  Major | build | Attila Doroszlai | Attila Doroszlai |
| [HDFS-17655](https://issues.apache.org/jira/browse/HDFS-17655) | Cannot run HDFS balancer with BlockPlacementPolicyWithNodeGroup |  Major | balancer & mover | YUBI LEE | YUBI LEE |
| [HADOOP-19370](https://issues.apache.org/jira/browse/HADOOP-19370) | Fix error links of huaweicloud in site index. |  Minor | documentation | fuchaohong | fuchaohong |
| [HDFS-17602](https://issues.apache.org/jira/browse/HDFS-17602) | RBF: Fix mount point with SPACE order can not find the available namespace. |  Critical | router | Zhongkun Wu | Zhongkun Wu |
| [HADOOP-19382](https://issues.apache.org/jira/browse/HADOOP-19382) | [ABFS] ITestAzureBlobFileSystemInitAndCreate failure |  Minor | fs/azure, test | Steve Loughran | Anuj Modi |
| [HDFS-17706](https://issues.apache.org/jira/browse/HDFS-17706) | TestBlockTokenWithDFSStriped fails due to closed streams |  Minor | . | Felix N | Felix N |
| [YARN-11753](https://issues.apache.org/jira/browse/YARN-11753) | NodeManager can be marked unhealthy if an application is killed |  Major | container-executor | Benjamin Teke | Benjamin Teke |
| [HADOOP-19392](https://issues.apache.org/jira/browse/HADOOP-19392) | class org.apache.hadoop.fs.ftp.FtpTestServer does not compile |  Blocker | common | Yaniv Kunda | Yaniv Kunda |
| [YARN-9741](https://issues.apache.org/jira/browse/YARN-9741) | [JDK11] TestAHSWebServices.testAbout fails |  Major | timelineservice | Adam Antal |  |
| [YARN-11759](https://issues.apache.org/jira/browse/YARN-11759) | Fix log statement in RMAppImpl#processNodeUpdate |  Major | resourcemanager | yang yang | yang yang |
| [HADOOP-19405](https://issues.apache.org/jira/browse/HADOOP-19405) | hadoop-aws and hadoop-azure tests have stopped running |  Critical | build | Steve Loughran | Shilun Fan |
| [MAPREDUCE-7497](https://issues.apache.org/jira/browse/MAPREDUCE-7497) | mapreduce tests have stopped running. |  Major | mapreduce-client | Shilun Fan | Shilun Fan |
| [HDFS-17724](https://issues.apache.org/jira/browse/HDFS-17724) | Set recover.lease.on.close.exception as an instance member in the DfsClientConf.java |  Minor | hadoop-client | Abhey Rana | Abhey Rana |
| [YARN-11384](https://issues.apache.org/jira/browse/YARN-11384) | NPE in DelegationTokenRenewer causes all subsequent apps to fail with "Timer already cancelled" |  Major | yarn | Aditya Sharma | Cheng Pan |
| [YARN-11745](https://issues.apache.org/jira/browse/YARN-11745) | YARN ResourceManager throws java.lang.IllegalArgumentExceptio: Comparison method violates its general contract! |  Major | yarn | chhinlinghean | chhinlinghean |
| [HDFS-17729](https://issues.apache.org/jira/browse/HDFS-17729) | Inconsistent mtime in the results of -stat and -ls command due to different TimeZone |  Major | shell | Haobo Zhang | Haobo Zhang |
| [HADOOP-19464](https://issues.apache.org/jira/browse/HADOOP-19464) | S3A: Restore Compatibility with EMRFS FileSystem |  Major | fs/s3 | Syed Shameerur Rahman | Syed Shameerur Rahman |
| [YARN-11764](https://issues.apache.org/jira/browse/YARN-11764) | yarn tests have stopped running. |  Major | gpg, nodemanager, resourcemanager | Shilun Fan | Shilun Fan |
| [YARN-11777](https://issues.apache.org/jira/browse/YARN-11777) | [UI2] fix the ASF licence comment tag in the partition-usage.hsb introduced by YARN-11757 |  Major | yarn-ui-v2 | Ferenc Erdelyi | Ferenc Erdelyi |
| [YARN-11780](https://issues.apache.org/jira/browse/YARN-11780) | [UI2] typo in the yarn-queue-partition-capacity-labels.hbs queue status section and improve the title tag wordings |  Minor | . | Ferenc Erdelyi | Ferenc Erdelyi |
| [YARN-11783](https://issues.apache.org/jira/browse/YARN-11783) | Upgrade wro4j to 1.8.0 |  Critical | yarn-ui-v2 | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-19476](https://issues.apache.org/jira/browse/HADOOP-19476) | Create python3 symlink needed for mvnsite |  Critical | common | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-19270](https://issues.apache.org/jira/browse/HADOOP-19270) | Use stable sort in commandQueue |  Major | tools | Kim gichan | Kim gichan |
| [YARN-11785](https://issues.apache.org/jira/browse/YARN-11785) | Race condition in QueueMetrics due to non-thread-safe HashMap causes MetricsException |  Major | capacityscheduler | Tao Yang | Tao Yang |
| [YARN-11776](https://issues.apache.org/jira/browse/YARN-11776) | Handle NPE in the RMDelegationTokenIdentifier if localServiceAddress is null |  Major | resourcemanager | Abhey Rana | Abhey Rana |
| [HADOOP-19487](https://issues.apache.org/jira/browse/HADOOP-19487) | Upgrade libopenssl to 3.1.1 for rsync on Windows |  Critical | build | Gautham Banasandra | Gautham Banasandra |
| [YARN-11797](https://issues.apache.org/jira/browse/YARN-11797) | [JDK11] Fix tests trying to connect to 0.0.0.0 |  Major | test | Istvan Toth | Istvan Toth |
| [YARN-11799](https://issues.apache.org/jira/browse/YARN-11799) | [JDK24] Remove PrintStream write counting from TestYarnCLI |  Major | yarn | Istvan Toth | Istvan Toth |
| [HDFS-17759](https://issues.apache.org/jira/browse/HDFS-17759) | Explicitly depend on jackson-core in hadoop-hdfs |  Critical | hdfs | Istvan Toth | Istvan Toth |
| [HADOOP-19488](https://issues.apache.org/jira/browse/HADOOP-19488) | RunJar throws UnsupportedOperationException on Windows |  Major | hadoop-common | Sangjin Lee | Sangjin Lee |
| [HADOOP-19508](https://issues.apache.org/jira/browse/HADOOP-19508) | Set charsetEncoder in HadoopArchiveLogs |  Major | test | Istvan Toth | Istvan Toth |
| [YARN-11796](https://issues.apache.org/jira/browse/YARN-11796) | [JDK17] Accept extra JDK 17 options in tests |  Major | test | Istvan Toth | Istvan Toth |
| [HADOOP-19514](https://issues.apache.org/jira/browse/HADOOP-19514) | SecretManager logs at INFO in bin/hadoop calls |  Blocker | bin | Steve Loughran | Chris Nauroth |
| [HDFS-16644](https://issues.apache.org/jira/browse/HDFS-16644) | java.io.IOException Invalid token in javax.security.sasl.qop |  Major | . | Walter Su | Zilong Zhu |
| [HDFS-15230](https://issues.apache.org/jira/browse/HDFS-15230) | Sanity check should not assume key base name can be derived from version name |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-11802](https://issues.apache.org/jira/browse/YARN-11802) | [JDK23] Avoid reflection in TestAsyncDispatcher |  Major | 3.5.0 | Istvan Toth | Istvan Toth |
| [HDFS-17760](https://issues.apache.org/jira/browse/HDFS-17760) | Fix MoveToTrash throws ParentNotDirectoryException when there is a file inode with the same name  in the trash |  Major | dfsclient | liuguanghua | liuguanghua |
| [HDFS-17752](https://issues.apache.org/jira/browse/HDFS-17752) | Host2DatanodeMap will not update when re-register a node with a different hostname |  Major | namenode | WenjingLiu | WenjingLiu |
| [HADOOP-19537](https://issues.apache.org/jira/browse/HADOOP-19537) | Document skip.platformToolsetDetection option in BUILDING.txt |  Minor | documentation | Istvan Toth | Istvan Toth |
| [YARN-11810](https://issues.apache.org/jira/browse/YARN-11810) | Fix SQL script in SQLServer/FederationStateStoreTables.sql |  Major | federation | Peter Szucs | Peter Szucs |
| [YARN-11803](https://issues.apache.org/jira/browse/YARN-11803) | [JDK24] Skip SecurityManager tests in TestJavaSandboxLinuxContainerRuntime when SecurityManager is not available |  Major | test | Istvan Toth | Istvan Toth |
| [HADOOP-19521](https://issues.apache.org/jira/browse/HADOOP-19521) | Fix time unit mismatch in method updateDeferredMetrics |  Major | metrics | Haobo Zhang | Haobo Zhang |
| [YARN-11808](https://issues.apache.org/jira/browse/YARN-11808) | RM memory leak due to Opportunistic container request cancellation at App level |  Major | RM, yarn | Ashish Ranjan | Gautham Banasandra |
| [HDFS-17770](https://issues.apache.org/jira/browse/HDFS-17770) | TestRouterRpcSingleNS#testSaveNamespace should leave safemode after finishing |  Minor | test | Haobo Zhang | Haobo Zhang |
| [HDFS-17768](https://issues.apache.org/jira/browse/HDFS-17768) | Observer namenode network delay causing empty block location for getBatchedListing |  Major | namenode | Dimas Shidqi Parikesit | Dimas Shidqi Parikesit |
| [HADOOP-19538](https://issues.apache.org/jira/browse/HADOOP-19538) | Update Boost to 1.86.0 in Windows build image |  Major | build, native | Istvan Toth | Istvan Toth |
| [HADOOP-19547](https://issues.apache.org/jira/browse/HADOOP-19547) | Migrate FileContextPermissionBase to Junit 5 |  Major | common, fs, test | Istvan Toth | Istvan Toth |
| [HDFS-17772](https://issues.apache.org/jira/browse/HDFS-17772) | Fix JournaledEditsCache int overflow while the maximum capacity to be Integer MAX\_VALUE. |  Minor | namenode | Guo Wei | Guo Wei |
| [HADOOP-19555](https://issues.apache.org/jira/browse/HADOOP-19555) | Fix testRenameFileWithFullQualifiedPath on Windows |  Critical | hadoop-common | Gautham Banasandra | Gautham Banasandra |
| [YARN-11815](https://issues.apache.org/jira/browse/YARN-11815) | NodeQueueLoadMonitor scheduler running on standby RMs |  Minor | resourcemanager | Nihal Agarwal | Gautham Banasandra |
| [HADOOP-19560](https://issues.apache.org/jira/browse/HADOOP-19560) | Update build instructions for Windows |  Major | build, documentation | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-19532](https://issues.apache.org/jira/browse/HADOOP-19532) | Update commons-lang3 to 3.17.0 |  Major | . | Istvan Toth | Istvan Toth |
| [HADOOP-19558](https://issues.apache.org/jira/browse/HADOOP-19558) | Skip testRenameFileBeingAppended on Windows |  Critical | hadoop-common | Gautham Banasandra | Gautham Banasandra |
| [YARN-11818](https://issues.apache.org/jira/browse/YARN-11818) | Fix "submitted by user jenkins to unknown queue: default" error in hadoop-yarn-client tests |  Major | yarn-client | Shilun Fan | Shilun Fan |
| [HADOOP-19551](https://issues.apache.org/jira/browse/HADOOP-19551) | Fix compilation error of native libraries on newer GCC |  Major | native | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-11801](https://issues.apache.org/jira/browse/YARN-11801) | NPE in FifoCandidatesSelector.selectCandidates when preempting resources for an auto-created queue without child queues |  Major | yarn | Susheel Gupta | Susheel Gupta |
| [HADOOP-19563](https://issues.apache.org/jira/browse/HADOOP-19563) | Upgrade libopenssl to 3.1.2 on Windows |  Blocker | build | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-19562](https://issues.apache.org/jira/browse/HADOOP-19562) | Fix TestTextCommand on Windows |  Major | hadoop-common, test | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-19570](https://issues.apache.org/jira/browse/HADOOP-19570) | Upgrade libxxhash to 0.8.3 in Windows 10 |  Major | build | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-19554](https://issues.apache.org/jira/browse/HADOOP-19554) | LocalDirAllocator still doesn't always recover from directory tree deletion |  Major | common | Steve Loughran | Steve Loughran |
| [YARN-11713](https://issues.apache.org/jira/browse/YARN-11713) | yarn-ui build fails in ARM docker in MacOs phantomjs error |  Blocker | yarn-ui-v2 | Mukund Thakur | Masatake Iwasaki |
| [YARN-11825](https://issues.apache.org/jira/browse/YARN-11825) | Yarn-ui2 build fails with java17 |  Major | yarn, yarn-ui-v2 | D M Murali Krishna Reddy | D M Murali Krishna Reddy |
| [MAPREDUCE-7506](https://issues.apache.org/jira/browse/MAPREDUCE-7506) | Fix Jobhistoryserver UI - WebAppException controller for jobhistory not found |  Major | jobhistoryserver | D M Murali Krishna Reddy | D M Murali Krishna Reddy |
| [HADOOP-19600](https://issues.apache.org/jira/browse/HADOOP-19600) | Fix Maven download link |  Blocker | build | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-19576](https://issues.apache.org/jira/browse/HADOOP-19576) | Insert Overwrite Jobs With MagicCommitter Fails On S3 Express Storage |  Major | . | Syed Shameerur Rahman | Syed Shameerur Rahman |
| [HADOOP-19573](https://issues.apache.org/jira/browse/HADOOP-19573) | S3A: ITestS3AConfiguration.testDirectoryAllocatorDefval() failing |  Minor | fs/s3, test | Steve Loughran | Steve Loughran |
| [HADOOP-19603](https://issues.apache.org/jira/browse/HADOOP-19603) | Fix TestFsShellList.testList on Windows |  Major | hadoop-common | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-19602](https://issues.apache.org/jira/browse/HADOOP-19602) | Upgrade libopenssl to 3.1.4 for rsync on Windows |  Major | build | Gautham Banasandra | Gautham Banasandra |
| [YARN-11834](https://issues.apache.org/jira/browse/YARN-11834) | [Capacity Scheduler] Application Stuck In ACCEPTED State due to Race Condition |  Major | capacity scheduler | Syed Shameerur Rahman | Syed Shameerur Rahman |
| [YARN-11837](https://issues.apache.org/jira/browse/YARN-11837) | RM UI2 cannot show logs for non-MapReduce jobs with MR ACLs enabled |  Major | resourcemanager | Susheel Gupta | Susheel Gupta |
| [YARN-11840](https://issues.apache.org/jira/browse/YARN-11840) | Fix Log in NodeStatusUpdaterImpl.java |  Major | . | zeekling | zeekling |
| [YARN-11836](https://issues.apache.org/jira/browse/YARN-11836) | YARN CLI fails to fetch logs with "-am" option if user is not in Admin ACLs |  Major | yarn-common | Peter Szucs | Peter Szucs |
| [HADOOP-19651](https://issues.apache.org/jira/browse/HADOOP-19651) | Upgrade libopenssl to 3.5.2-1 needed for rsync |  Blocker | build | Gautham Banasandra | Gautham Banasandra |
| [HDFS-17815](https://issues.apache.org/jira/browse/HDFS-17815) | Fix upload fsimage failure when checkpoint takes a long time |  Major | namenode | caozhiqiang | caozhiqiang |
| [YARN-11824](https://issues.apache.org/jira/browse/YARN-11824) | Prevent test dependencies from leaking into the distro. |  Blocker | build | Steve Loughran | Chris Nauroth |
| [HADOOP-19652](https://issues.apache.org/jira/browse/HADOOP-19652) | Fix dependency exclusion list of hadoop-client-runtime. |  Major | client-mounts | Cheng Pan | Cheng Pan |
| [HDFS-17680](https://issues.apache.org/jira/browse/HDFS-17680) | HDFS ui in the datanodes doesn't redirect to https when dfs.http.policy is HTTPS\_ONLY |  Minor | datanode, ui | Luis Pigueiras | Michael Smith |
| [HADOOP-19648](https://issues.apache.org/jira/browse/HADOOP-19648) | cos use token credential will lost token field |  Critical | cloud-storage, fs/cos | sanqingleo | sanqingleo |
| [MAPREDUCE-7502](https://issues.apache.org/jira/browse/MAPREDUCE-7502) | TestPipeApplication silently hangs |  Major | . | Istvan Toth | Istvan Toth |
| [MAPREDUCE-7517](https://issues.apache.org/jira/browse/MAPREDUCE-7517) | TestPipeApplication hangs with JAVA 17+ |  Major | mapreduce-client, test | Istvan Toth | Istvan Toth |
| [HDFS-17829](https://issues.apache.org/jira/browse/HDFS-17829) | [JDK24] TestDFSUtil fails with Java 24 beacause of InetSocketAddress.toString changes |  Major | hdfs, test | Istvan Toth | Istvan Toth |
| [YARN-11838](https://issues.apache.org/jira/browse/YARN-11838) | YARN ConcurrentModificationException When Refreshing Node Attributes |  Major | nodeattibute, yarn | Syed Shameerur Rahman | Syed Shameerur Rahman |
| [HDFS-17833](https://issues.apache.org/jira/browse/HDFS-17833) | [JDK24] TestObserverReadProxyProvider fails with Java 24 because of InetSocketAddress.toString changes |  Major | test | Istvan Toth | Istvan Toth |
| [HDFS-17832](https://issues.apache.org/jira/browse/HDFS-17832) | [JDK24] TestNameNodeResourceChecker fails with Java 24 |  Major | . | Istvan Toth | Istvan Toth |
| [HADOOP-17629](https://issues.apache.org/jira/browse/HADOOP-17629) | TestRPC#testAuthorization fails |  Major | test | Akira Ajisaka | Tsz-wo Sze |
| [HDFS-17378](https://issues.apache.org/jira/browse/HDFS-17378) | Missing operationType for some operations in authorizer |  Minor | hdfs, namenode | Sebastian Bernauer |  |
| [HADOOP-19717](https://issues.apache.org/jira/browse/HADOOP-19717) | Resolve build error caused by missing Checker Framework (NonNull not recognized) |  Major | hdfs, tos | Shilun Fan | Shilun Fan |
| [YARN-11875](https://issues.apache.org/jira/browse/YARN-11875) | Fix build failure caused by color@5.0.2 |  Major | buid | Shilun Fan | Shilun Fan |
| [MAPREDUCE-7448](https://issues.apache.org/jira/browse/MAPREDUCE-7448) | Skipping cleanup with FileOutputCommitter V1 can corrupt output: warn and document |  Critical | . | rstest | rstest |
| [YARN-11873](https://issues.apache.org/jira/browse/YARN-11873) | Add yarn.lock for app catalog webapp |  Critical | webapp | Michael Smith |  |
| [HADOOP-18944](https://issues.apache.org/jira/browse/HADOOP-18944) | S3A: openfile to set async drain threshold correctly |  Minor | fs/s3 | Steve Loughran | Mehakmeet Singh |
| [HADOOP-19712](https://issues.apache.org/jira/browse/HADOOP-19712) | S3A: Deadlock observed in IOStatistics EvaluatingStatisticsMap.entryset() |  Major | fs, fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-19719](https://issues.apache.org/jira/browse/HADOOP-19719) | Upgrade to wildfly version with support for openssl 3 |  Major | build, fs/azure, fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-17847](https://issues.apache.org/jira/browse/HDFS-17847) | OIV exits with code 0 (success) even when the output file is incomplete |  Major | command | Srinivasu Majeti | Srinivasu Majeti |
| [YARN-11839](https://issues.apache.org/jira/browse/YARN-11839) | [RM HA] - In corner case, RM stay in ACTIVE with RMStateStore in FENCED state |  Critical | resourcemanager | Vinayakumar B | Vinayakumar B |
| [HADOOP-18090](https://issues.apache.org/jira/browse/HADOOP-18090) | Exclude com/jcraft/jsch classes from being shaded/relocated |  Major | build | mkv |  |
| [YARN-11891](https://issues.apache.org/jira/browse/YARN-11891) | Yarn logs command hangs for running applications |  Major | yarn-client | D M Murali Krishna Reddy | D M Murali Krishna Reddy |
| [YARN-11881](https://issues.apache.org/jira/browse/YARN-11881) | Use hbase-shaded-client-byo-hadoop in hadoop-yarn-server-timelineservice-hbase |  Major | build, timelineservice | Istvan Toth | Istvan Toth |
| [HDFS-17854](https://issues.apache.org/jira/browse/HDFS-17854) | Namenode Web UI file deletion bug |  Major | namenode | Jim Halfpenny | Jim Halfpenny |
| [HDFS-17834](https://issues.apache.org/jira/browse/HDFS-17834) | Fix invalid HTTP links for DataNodes in IPv6 environment. |  Minor | . | sunhui | sunhui |
| [HADOOP-19745](https://issues.apache.org/jira/browse/HADOOP-19745) | The build reported a license-header validation error |  Trivial | build | Senthil Kumar | Steve Loughran |
| [HADOOP-19747](https://issues.apache.org/jira/browse/HADOOP-19747) | switch lz4-java to at.yawk.lz4 version due to CVE |  Major | build, common | PJ Fanning | PJ Fanning |
| [HADOOP-19744](https://issues.apache.org/jira/browse/HADOOP-19744) | [JDK24] Do not use SecurityManager in SubjectUtil.checkThreadInheritsSubject |  Major | . | Istvan Toth | Istvan Toth |
| [YARN-11912](https://issues.apache.org/jira/browse/YARN-11912) | Suppress Spotbugs warnings introduced in recent trunk changes |  Minor | buid | Chris Nauroth | Chris Nauroth |
| [HADOOP-19760](https://issues.apache.org/jira/browse/HADOOP-19760) | hadoop-azure JavaDoc build fails |  Major | build | Chris Nauroth | Chris Nauroth |
| [HADOOP-19755](https://issues.apache.org/jira/browse/HADOOP-19755) | hadoop-azure build fails |  Major | fs/azure | Sangjin Lee | Anmol Asrani |
| [YARN-11902](https://issues.apache.org/jira/browse/YARN-11902) | Fix Hadoop build failure caused by mktemp@2.0.2 |  Minor | yarn | Senthil Kumar | Senthil Kumar |
| [YARN-11895](https://issues.apache.org/jira/browse/YARN-11895) | Migrate Yarn Native Service to Jersey2 |  Major | yarn | Bence Kosztolnik | Bence Kosztolnik |
| [HDFS-17793](https://issues.apache.org/jira/browse/HDFS-17793) | RBF: Enable the router asynchronous RPC feature to handle getDelegationToken request errors |  Major | . | Xiping Zhang | Xiping Zhang |
| [HADOOP-19780](https://issues.apache.org/jira/browse/HADOOP-19780) | Fix website build in GitHub workflow |  Major | build, documentation | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-19776](https://issues.apache.org/jira/browse/HADOOP-19776) | trunk pre-commits for native code still try to use Java 8 |  Major | precommit | Chris Nauroth | Chris Nauroth |
| [HADOOP-19756](https://issues.apache.org/jira/browse/HADOOP-19756) | exception.c compile macro fights the future |  Major | . | Edward Capriolo | Edward Capriolo |
| [HDFS-17831](https://issues.apache.org/jira/browse/HDFS-17831) | [JDK24] Unexpected exception in org.apache.hadoop.hdfs.server.namenode.TestCheckpoint with Java 24 |  Major | test | Istvan Toth | Istvan Toth |
| [YARN-11921](https://issues.apache.org/jira/browse/YARN-11921) | Replace unsupported org.xolstice.maven.plugins in hadoop-yarn-csi |  Major | build, yarn-csi | Edward Capriolo | Edward Capriolo |
| [YARN-11874](https://issues.apache.org/jira/browse/YARN-11874) | ResourceManager REST api scheduler info contains @xsi.type instead of type |  Major | yarn | Bence Kosztolnik | Bence Kosztolnik |
| [YARN-11918](https://issues.apache.org/jira/browse/YARN-11918) | Fix YARN UI v2 build in Ubuntu 24.04 ARM |  Blocker | yarn-ui-v2 | Akira Ajisaka | Akira Ajisaka |
| [YARN-11925](https://issues.apache.org/jira/browse/YARN-11925) | Fix the configurability of RM webservice class |  Major | resourcemanager, yarn | Peter Szucs | Peter Szucs |
| [YARN-11923](https://issues.apache.org/jira/browse/YARN-11923) | YARN web proxy AmIpFilter allows TRACE, bypassing sparkUI TRACE block |  Major | yarn | Susheel Gupta | Susheel Gupta |
| [YARN-11712](https://issues.apache.org/jira/browse/YARN-11712) | Yarn-ui build fails in ARM docker looking for python2. |  Blocker | yarn-ui-v2 | Mukund Thakur | Masatake Iwasaki |
| [HADOOP-19697](https://issues.apache.org/jira/browse/HADOOP-19697) | google gs connector registration failing |  Blocker | fs/gcs | Steve Loughran | Steve Loughran |
| [HADOOP-19811](https://issues.apache.org/jira/browse/HADOOP-19811) | hadoop-gcp does not relocate shaded OpenTelemetry dependencies |  Blocker | fs/gcs | Chris Nauroth | Chris Nauroth |
| [YARN-11926](https://issues.apache.org/jira/browse/YARN-11926) | hadoop-yarn-server-resourcemanager: TestRMWebServicesReservation fails due to outdated timestamps |  Minor | resourcemanager | Ronald Macmaster | Chris Nauroth |
| [YARN-11930](https://issues.apache.org/jira/browse/YARN-11930) | Newly released protobuf-maven-plugin incompatible with hadoop-yarn-csi build |  Blocker | build, yarn-csi | Chris Nauroth | Chris Nauroth |
| [MAPREDUCE-7530](https://issues.apache.org/jira/browse/MAPREDUCE-7530) | MR AM REST API backward compatibility with Jersey1 |  Major | mr-am | Bence Kosztolnik | Bence Kosztolnik |
| [YARN-11932](https://issues.apache.org/jira/browse/YARN-11932) | Fix TestYarnFederationWithFairScheduler timeout caused by shared NodeLabel storage |  Major | router | Shilun Fan | Shilun Fan |
| [YARN-11931](https://issues.apache.org/jira/browse/YARN-11931) | Fix router webapp tests by adding EclipseLink test dependency |  Major | router | Shilun Fan | Shilun Fan |
| [YARN-11933](https://issues.apache.org/jira/browse/YARN-11933) | TestAMSimulator fails due to shared node labels store directory |  Major | scheduler-load-simulator | Shilun Fan | Shilun Fan |
| [MAPREDUCE-7531](https://issues.apache.org/jira/browse/MAPREDUCE-7531) | TestMRJobs.testThreadDumpOnTaskTimeout flaky due to thread dump delayed write |  Major | mapreduce-client | Shilun Fan | Shilun Fan |
| [YARN-11934](https://issues.apache.org/jira/browse/YARN-11934) | Fix testComponentHealthThresholdMonitor race condition |  Major | yarn-native-services | Shilun Fan | Shilun Fan |
| [YARN-11935](https://issues.apache.org/jira/browse/YARN-11935) | Fix deadlock in TestRMHA#testTransitionedToStandbyShouldNotHang |  Major | resourcemanager | Shilun Fan | Shilun Fan |
| [HDFS-17885](https://issues.apache.org/jira/browse/HDFS-17885) | Fix TestDFSAdmin.testAllDatanodesReconfig flaky test |  Major | dfsadmin | Shilun Fan | Shilun Fan |
| [HDFS-17874](https://issues.apache.org/jira/browse/HDFS-17874) | Unsafe Jackson Polymorphic Deserialization in HDFS DiskBalancer NodePlan |  Major | diskbalancer, security | Cyl | PJ Fanning |
| [MAPREDUCE-7533](https://issues.apache.org/jira/browse/MAPREDUCE-7533) | MR AM UI wont be loaded on root path |  Major | mrv2 | Bence Kosztolnik | Bence Kosztolnik |
| [MAPREDUCE-7527](https://issues.apache.org/jira/browse/MAPREDUCE-7527) | Fix /jobhistory/attempts/ page not rendering attempt information |  Major | . | D M Murali Krishna Reddy | Ayush Saxena |
| [HADOOP-19733](https://issues.apache.org/jira/browse/HADOOP-19733) | S3A: Credentials provider classes not found despite setting \`fs.s3a.classloader.isolation\` to \`false\` |  Minor | fs/s3 | Brandon | Brandon |
| [HADOOP-19793](https://issues.apache.org/jira/browse/HADOOP-19793) | S3A: Regression: maximum size of a single upload is now only 2GB |  Minor | fs/s3 | Steve Loughran | Aaron Fabbri |
| [YARN-9511](https://issues.apache.org/jira/browse/YARN-9511) | TestAuxServices#testRemoteAuxServiceClassPath YarnRuntimeException: The remote jarfile should not be writable by group or others. The current Permission is 436 |  Major | test | Siyao Meng | Peter Szucs |
| [YARN-11919](https://issues.apache.org/jira/browse/YARN-11919) | linux-container-executor segfault with get\_user\_info |  Major | . | Edward Capriolo | Edward Capriolo |
| [HADOOP-19843](https://issues.apache.org/jira/browse/HADOOP-19843) | Regenerate tracked proto2 source code for non-x86\_64 platforms |  Major | . | Cheng Pan | Cheng Pan |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-17370](https://issues.apache.org/jira/browse/HDFS-17370) | Fix junit dependency for running parameterized tests in hadoop-hdfs-rbf |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-17432](https://issues.apache.org/jira/browse/HDFS-17432) | Fix junit dependency to enable JUnit4 tests to run in hadoop-hdfs-rbf |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-17435](https://issues.apache.org/jira/browse/HDFS-17435) | Fix TestRouterRpc failed |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-17441](https://issues.apache.org/jira/browse/HDFS-17441) | Fix junit dependency by adding missing library in hadoop-hdfs-rbf |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-11744](https://issues.apache.org/jira/browse/YARN-11744) | Tackle flaky test testGetRunningContainersToKill |  Major | test, yarn-service | Cheng Pan | Cheng Pan |
| [MAPREDUCE-6932](https://issues.apache.org/jira/browse/MAPREDUCE-6932) | Upgrade JUnit from 4 to 5 in hadoop-mapreduce |  Major | test | Akira Ajisaka | Ashutosh Gupta |
| [YARN-11790](https://issues.apache.org/jira/browse/YARN-11790) | TestAmFilter#testProxyUpdate fails in some networks |  Minor | test, webproxy | Chris Nauroth | Chris Nauroth |
| [YARN-11816](https://issues.apache.org/jira/browse/YARN-11816) | Fix flaky test: TestCapacitySchedulerMultiNodes#testCheckRequestOnceForUnsatisfiedRequest |  Minor | capacity scheduler, resourcemanager, test | Tao Yang | Tao Yang |
| [HDFS-17804](https://issues.apache.org/jira/browse/HDFS-17804) | Recover test for hadoop-hdfs-native-client |  Major | test | Cheng Pan | Cheng Pan |
| [YARN-6939](https://issues.apache.org/jira/browse/YARN-6939) | Upgrade JUnit from 4 to 5 in hadoop-yarn |  Major | test, yarn | Akira Ajisaka | Ashutosh Gupta |
| [HDFS-17841](https://issues.apache.org/jira/browse/HDFS-17841) | TestWebHDFSTimeouts fail with JDK17 |  Major | test | Tsz-wo Sze | Tsz-wo Sze |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-18845](https://issues.apache.org/jira/browse/HADOOP-18845) | Add ability to configure ConnectionTTL of http connections while creating S3 Client. |  Major | fs/s3 | Mukund Thakur | Mukund Thakur |
| [YARN-11631](https://issues.apache.org/jira/browse/YARN-11631) | [GPG] Add GPGWebServices |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-10125](https://issues.apache.org/jira/browse/YARN-10125) | In Federation, kill application from client does not kill Unmanaged AM's and containers launched by Unmanaged AM |  Major | client, federation, router | D M Murali Krishna Reddy | Shilun Fan |
| [HADOOP-19004](https://issues.apache.org/jira/browse/HADOOP-19004) | S3A: Support Authentication through HttpSigner API |  Major | fs/s3 | Steve Loughran | Harshit Gupta |
| [YARN-11638](https://issues.apache.org/jira/browse/YARN-11638) | [GPG] GPG Support CLI. |  Major | federation | Shilun Fan | Shilun Fan |
| [HADOOP-19027](https://issues.apache.org/jira/browse/HADOOP-19027) | S3A: S3AInputStream doesn't recover from HTTP/channel exceptions |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-19033](https://issues.apache.org/jira/browse/HADOOP-19033) | S3A: disable checksums when fs.s3a.checksum.validation = false |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-18883](https://issues.apache.org/jira/browse/HADOOP-18883) | Expect-100 JDK bug resolution: prevent multiple server calls |  Major | fs/azure | Pranav Saxena | Pranav Saxena |
| [HADOOP-19015](https://issues.apache.org/jira/browse/HADOOP-19015) | Increase fs.s3a.connection.maximum to 500 to minimize risk of Timeout waiting for connection from pool |  Major | fs/s3 | Mukund Thakur | Mukund Thakur |
| [HADOOP-18975](https://issues.apache.org/jira/browse/HADOOP-18975) | AWS SDK v2:  extend support for FIPS endpoints |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-19046](https://issues.apache.org/jira/browse/HADOOP-19046) | S3A: update AWS sdk versions to 2.23.5 and 1.12.599 |  Major | build, fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-11041](https://issues.apache.org/jira/browse/YARN-11041) | Replace all occurences of queuePath with the new QueuePath class - followup |  Major | capacity scheduler | Tibor Kovács | Peter Szucs |
| [HADOOP-18830](https://issues.apache.org/jira/browse/HADOOP-18830) | S3A: Cut S3 Select |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-18980](https://issues.apache.org/jira/browse/HADOOP-18980) | S3A credential provider remapping: make extensible |  Minor | fs/s3 | Steve Loughran | Viraj Jasani |
| [HADOOP-19044](https://issues.apache.org/jira/browse/HADOOP-19044) | AWS SDK V2 - Update S3A region logic |  Major | fs/s3 | Ahmar Suhail | Viraj Jasani |
| [HADOOP-19045](https://issues.apache.org/jira/browse/HADOOP-19045) | HADOOP-19045. S3A: CreateSession Timeout after 10 seconds |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-19069](https://issues.apache.org/jira/browse/HADOOP-19069) | Use hadoop-thirdparty 1.2.0 |  Major | hadoop-thirdparty | Shilun Fan | Shilun Fan |
| [HADOOP-19057](https://issues.apache.org/jira/browse/HADOOP-19057) | S3 public test bucket landsat-pds unreadable -needs replacement |  Critical | fs/s3, test | Steve Loughran | Steve Loughran |
| [HADOOP-19084](https://issues.apache.org/jira/browse/HADOOP-19084) | prune dependency exports of hadoop-\* modules |  Blocker | build | Steve Loughran | Steve Loughran |
| [HADOOP-19099](https://issues.apache.org/jira/browse/HADOOP-19099) | Add Protobuf Compatibility Notes |  Major | documentation | Shilun Fan | Shilun Fan |
| [HADOOP-19097](https://issues.apache.org/jira/browse/HADOOP-19097) | core-default fs.s3a.connection.establish.timeout value too low -warning always printed |  Minor | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-19043](https://issues.apache.org/jira/browse/HADOOP-19043) | S3A: Regression: ITestS3AOpenCost fails on prefetch test runs |  Minor | fs/s3, test | Steve Loughran | Steve Loughran |
| [HADOOP-19066](https://issues.apache.org/jira/browse/HADOOP-19066) | AWS SDK V2 - Enabling FIPS should be allowed with central endpoint |  Major | fs/s3 | Viraj Jasani | Viraj Jasani |
| [HADOOP-19119](https://issues.apache.org/jira/browse/HADOOP-19119) | spotbugs complaining about possible NPE in org.apache.hadoop.crypto.key.kms.ValueQueue.getSize() |  Minor | crypto | Steve Loughran | Steve Loughran |
| [HADOOP-19089](https://issues.apache.org/jira/browse/HADOOP-19089) | [ABFS] Reverting Back Support of setXAttr() and getXAttr() on root path |  Major | fs/azure | Anuj Modi | Anuj Modi |
| [HADOOP-19141](https://issues.apache.org/jira/browse/HADOOP-19141) | Update VectorIO default values consistently |  Major | fs, fs/s3 | Dongjoon Hyun | Dongjoon Hyun |
| [HADOOP-19098](https://issues.apache.org/jira/browse/HADOOP-19098) | Vector IO: consistent specified rejection of overlapping ranges |  Major | fs, fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-19101](https://issues.apache.org/jira/browse/HADOOP-19101) | Vectored Read into off-heap buffer broken in fallback implementation |  Blocker | fs, fs/azure | Steve Loughran | Steve Loughran |
| [HADOOP-19096](https://issues.apache.org/jira/browse/HADOOP-19096) | [ABFS] Enhancing Client-Side Throttling Metrics Updation Logic |  Major | fs/azure | Anuj Modi | Anuj Modi |
| [HADOOP-19102](https://issues.apache.org/jira/browse/HADOOP-19102) | [ABFS]: FooterReadBufferSize should not be greater than readBufferSize |  Major | fs/azure | Pranav Saxena | Pranav Saxena |
| [YARN-11672](https://issues.apache.org/jira/browse/YARN-11672) | Create a CgroupHandler implementation for cgroup v2 |  Major | . | Benjamin Teke | Benjamin Teke |
| [YARN-11690](https://issues.apache.org/jira/browse/YARN-11690) | Update container executor to use CGROUP2\_SUPER\_MAGIC in cgroup 2 scenarios |  Major | container-executor | Benjamin Teke | Benjamin Teke |
| [YARN-11674](https://issues.apache.org/jira/browse/YARN-11674) | Update CpuResourceHandler implementation for cgroup v2 support |  Major | . | Benjamin Teke | Benjamin Teke |
| [YARN-11675](https://issues.apache.org/jira/browse/YARN-11675) | Update MemoryResourceHandler implementation for cgroup v2 support |  Major | . | Benjamin Teke | Peter Szucs |
| [YARN-11685](https://issues.apache.org/jira/browse/YARN-11685) | Create a config to enable/disable cgroup v2 functionality |  Major | . | Benjamin Teke | Peter Szucs |
| [YARN-11689](https://issues.apache.org/jira/browse/YARN-11689) | Update the cgroup v2 init error handling to provide more straightforward error messages |  Major | . | Benjamin Teke | Benjamin Teke |
| [HADOOP-19013](https://issues.apache.org/jira/browse/HADOOP-19013) | fs.getXattrs(path) for S3FS doesn't have x-amz-server-side-encryption-aws-kms-key-id header. |  Major | fs/s3 | Mukund Thakur | Mukund Thakur |
| [YARN-11681](https://issues.apache.org/jira/browse/YARN-11681) | Update the cgroup documentation with v2 support |  Major | . | Benjamin Teke | Benjamin Teke |
| [HADOOP-18325](https://issues.apache.org/jira/browse/HADOOP-18325) | ABFS: Add correlated metric support for ABFS operations |  Major | fs/azure | Anmol Asrani | Anmol Asrani |
| [YARN-11687](https://issues.apache.org/jira/browse/YARN-11687) | Update CGroupsResourceCalculator to track usages using cgroupv2 |  Major | . | Benjamin Teke | Bence Kosztolnik |
| [HADOOP-19178](https://issues.apache.org/jira/browse/HADOOP-19178) | WASB Driver Deprecation and eventual removal |  Major | fs/azure | Sneha Vijayarajan | Anuj Modi |
| [HADOOP-18516](https://issues.apache.org/jira/browse/HADOOP-18516) | [ABFS]: Support fixed SAS token config in addition to Custom SASTokenProvider Implementation |  Minor | fs/azure | Sree Bhattacharyya | Anuj Modi |
| [HADOOP-19137](https://issues.apache.org/jira/browse/HADOOP-19137) | [ABFS]Prevent ABFS initialization for non-hierarchical-namespace account if Customer-provided-key configs given. |  Major | fs/azure | Pranav Saxena | Pranav Saxena |
| [HADOOP-18508](https://issues.apache.org/jira/browse/HADOOP-18508) | support multiple s3a integration test runs on same bucket in parallel |  Minor | fs/s3, test | Steve Loughran | Steve Loughran |
| [HADOOP-19204](https://issues.apache.org/jira/browse/HADOOP-19204) | VectorIO regression: empty ranges are now rejected |  Major | fs | Steve Loughran | Steve Loughran |
| [HADOOP-19210](https://issues.apache.org/jira/browse/HADOOP-19210) | s3a: TestS3AAWSCredentialsProvider and TestS3AInputStreamRetry really slow |  Major | fs/s3, test | Steve Loughran | Steve Loughran |
| [HADOOP-19205](https://issues.apache.org/jira/browse/HADOOP-19205) | S3A initialization/close slower than with v1 SDK |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-19208](https://issues.apache.org/jira/browse/HADOOP-19208) | ABFS: Fixing logic to determine HNS nature of account to avoid extra getAcl() calls |  Major | fs/azure | Anuj Modi | Anuj Modi |
| [YARN-11705](https://issues.apache.org/jira/browse/YARN-11705) | Turn off Node Manager working directories validation by default |  Major | . | Bence Kosztolnik | Bence Kosztolnik |
| [HADOOP-19120](https://issues.apache.org/jira/browse/HADOOP-19120) | [ABFS]: ApacheHttpClient adaptation as network library |  Major | fs/azure | Pranav Saxena | Pranav Saxena |
| [HADOOP-19245](https://issues.apache.org/jira/browse/HADOOP-19245) | S3ABlockOutputStream no longer sends progress events in close() |  Critical | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-19253](https://issues.apache.org/jira/browse/HADOOP-19253) | Google GCS changes fail due to VectorIO changes |  Major | fs | Steve Loughran | Steve Loughran |
| [HADOOP-18965](https://issues.apache.org/jira/browse/HADOOP-18965) | ITestS3AHugeFilesEncryption failure |  Major | fs/s3, test | Steve Loughran |  |
| [HADOOP-19187](https://issues.apache.org/jira/browse/HADOOP-19187) | ABFS: [FnsOverBlob] Making AbfsClient Abstract for supporting both DFS and Blob Endpoint |  Major | fs/azure | Anuj Modi | Anuj Modi |
| [HADOOP-19072](https://issues.apache.org/jira/browse/HADOOP-19072) | S3A: expand optimisations on stores with "fs.s3a.performance.flags" for mkdir |  Major | fs/s3 | Steve Loughran | Viraj Jasani |
| [YARN-10345](https://issues.apache.org/jira/browse/YARN-10345) | HsWebServices containerlogs does not honor ACLs for completed jobs |  Critical | yarn | Prabhu Joseph | Bence Kosztolnik |
| [HADOOP-19257](https://issues.apache.org/jira/browse/HADOOP-19257) | S3A: ITestAssumeRole.testAssumeRoleBadInnerAuth failure |  Minor | fs/s3, test | Steve Loughran | Steve Loughran |
| [HADOOP-18938](https://issues.apache.org/jira/browse/HADOOP-18938) | S3A region logic to handle vpce and non standard endpoints |  Major | fs/s3 | Ahmar Suhail | Shintaro Onuma |
| [HADOOP-19201](https://issues.apache.org/jira/browse/HADOOP-19201) | S3A: Support external id in assume role |  Major | fs/s3 | Smith Cruise | Smith Cruise |
| [HADOOP-19189](https://issues.apache.org/jira/browse/HADOOP-19189) | ITestS3ACommitterFactory failing |  Minor | fs/s3, test | Steve Loughran | Steve Loughran |
| [HADOOP-19262](https://issues.apache.org/jira/browse/HADOOP-19262) | [JDK17] Upgade wildfly-openssl:1.1.3.Final to 2.1.4.Final+ |  Major | fs/azure, fs/s3 | Steve Loughran | Saikat Roy |
| [HADOOP-19221](https://issues.apache.org/jira/browse/HADOOP-19221) | S3A: Unable to recover from failure of multipart block upload attempt "Status Code: 400; Error Code: RequestTimeout" |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-19284](https://issues.apache.org/jira/browse/HADOOP-19284) | ABFS: Allow "fs.azure.account.hns.enabled" to be set as Account Specific Config |  Major | fs/azure | Descifrado | Anuj Modi |
| [HADOOP-19219](https://issues.apache.org/jira/browse/HADOOP-19219) | Resolve Certificate error in Hadoop-auth tests. |  Major | build, common | Muskan Mishra | Muskan Mishra |
| [HADOOP-19296](https://issues.apache.org/jira/browse/HADOOP-19296) | [JDK17] Upgrade maven-war-plugin to 3.4.0 |  Major | build, common | Shilun Fan | Shilun Fan |
| [HADOOP-19286](https://issues.apache.org/jira/browse/HADOOP-19286) | Support S3A cross region access when S3 region/endpoint is set |  Major | fs/s3 | Syed Shameerur Rahman | Syed Shameerur Rahman |
| [HADOOP-18308](https://issues.apache.org/jira/browse/HADOOP-18308) | Update to Apache LDAP API 2.0.x |  Major | build | Colm O hEigeartaigh | Colm O hEigeartaigh |
| [HADOOP-19310](https://issues.apache.org/jira/browse/HADOOP-19310) | [JDK17] Add JPMS options required by Java 17+ |  Major | build, common | Cheng Pan | Cheng Pan |
| [HADOOP-19243](https://issues.apache.org/jira/browse/HADOOP-19243) | Upgrade Mockito version to 4.11.0 |  Major | build | Muskan Mishra | Muskan Mishra |
| [HADOOP-19297](https://issues.apache.org/jira/browse/HADOOP-19297) | [JDK17] Upgrade maven.plugin-tools.version to 3.10.2 |  Major | build, common | Shilun Fan | Shilun Fan |
| [HADOOP-19122](https://issues.apache.org/jira/browse/HADOOP-19122) | [ABFS] testListPathWithValueGreaterThanServerMaximum assert failure on heavily loaded store |  Minor | fs/azure, test | Steve Loughran | Anuj Modi |
| [HADOOP-18656](https://issues.apache.org/jira/browse/HADOOP-18656) | ABFS: Support for Pagination in Recursive Directory Delete |  Minor | fs/azure | Sree Bhattacharyya | Anuj Modi |
| [HADOOP-19330](https://issues.apache.org/jira/browse/HADOOP-19330) | S3A: Add LeakReporter; use in S3AInputStream |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-18708](https://issues.apache.org/jira/browse/HADOOP-18708) | AWS SDK V2 - Implement CSE |  Major | fs/s3 | Ahmar Suhail | Syed Shameerur Rahman |
| [HADOOP-19317](https://issues.apache.org/jira/browse/HADOOP-19317) | S3A: fs.s3a.connection.expect.continue controls 100 CONTINUE behavior |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-18960](https://issues.apache.org/jira/browse/HADOOP-18960) | ABFS contract-tests with Hadoop-Commons intermittently failing |  Minor | fs/azure | Pranav Saxena | Anuj Modi |
| [HADOOP-19336](https://issues.apache.org/jira/browse/HADOOP-19336) | S3A: Test failures after CSE support added |  Major | fs/s3 | Steve Loughran | Syed Shameerur Rahman |
| [HADOOP-19226](https://issues.apache.org/jira/browse/HADOOP-19226) | ABFS: [FnsOverBlob] Implementing Azure Rest APIs on Blob Endpoint for AbfsBlobClient |  Major | fs/azure | Anuj Modi | Anuj Modi |
| [HADOOP-19367](https://issues.apache.org/jira/browse/HADOOP-19367) | Fix setting final field value on Java 17 |  Major | test | Cheng Pan | Cheng Pan |
| [HADOOP-19207](https://issues.apache.org/jira/browse/HADOOP-19207) | ABFS: [FnsOverBlob] Response Handling of Blob Endpoint APIs and Metadata APIs |  Major | fs/azure | Anuj Modi | Anuj Modi |
| [HDFS-17496](https://issues.apache.org/jira/browse/HDFS-17496) | DataNode supports more fine-grained dataset lock based on blockid |  Major | datanode | Haobo Zhang | Haobo Zhang |
| [YARN-11743](https://issues.apache.org/jira/browse/YARN-11743) | Cgroup v2 support should fall back to v1 when there are no v2 controllers |  Major | yarn | Benjamin Teke | Peter Szucs |
| [HADOOP-19351](https://issues.apache.org/jira/browse/HADOOP-19351) | S3A: Add config option to skip test with performance mode |  Minor | fs/s3, test | Chung En Lee | Chung En Lee |
| [MAPREDUCE-7418](https://issues.apache.org/jira/browse/MAPREDUCE-7418) | Upgrade Junit 4 to 5 in hadoop-mapreduce-client-app |  Major | test | Ashutosh Gupta | Ashutosh Gupta |
| [MAPREDUCE-7414](https://issues.apache.org/jira/browse/MAPREDUCE-7414) | Upgrade Junit 4 to 5 in hadoop-mapreduce-client-hs |  Major | test | Ashutosh Gupta | Ashutosh Gupta |
| [MAPREDUCE-7415](https://issues.apache.org/jira/browse/MAPREDUCE-7415) | Upgrade Junit 4 to 5 in hadoop-mapreduce-client-nativetask |  Major | test | Ashutosh Gupta | Ashutosh Gupta |
| [MAPREDUCE-7416](https://issues.apache.org/jira/browse/MAPREDUCE-7416) | Upgrade Junit 4 to 5 in hadoop-mapreduce-client-shuffle |  Major | test | Ashutosh Gupta | Ashutosh Gupta |
| [MAPREDUCE-7420](https://issues.apache.org/jira/browse/MAPREDUCE-7420) | Upgrade Junit 4 to 5 in hadoop-mapreduce-client-core |  Major | test | Ashutosh Gupta | Ashutosh Gupta |
| [MAPREDUCE-7421](https://issues.apache.org/jira/browse/MAPREDUCE-7421) | Upgrade Junit 4 to 5 in hadoop-mapreduce-client-jobclient |  Major | test | Ashutosh Gupta | Ashutosh Gupta |
| [HADOOP-19393](https://issues.apache.org/jira/browse/HADOOP-19393) | ABFS: Returning FileAlreadyExists Exception for UnauthorizedBlobOverwrite Rename Errors |  Minor | fs/azure | Manika Joshi | Manika Joshi |
| [HADOOP-19232](https://issues.apache.org/jira/browse/HADOOP-19232) | ABFS: [FnsOverBlob] Implementing Ingress Support with various Fallback Handling |  Major | fs/azure | Anuj Modi | Anmol Asrani |
| [HADOOP-19233](https://issues.apache.org/jira/browse/HADOOP-19233) | ABFS: [FnsOverBlob] Implementing Rename and Delete APIs over Blob Endpoint |  Major | fs/azure | Anuj Modi | Manish Bhatt |
| [HADOOP-19404](https://issues.apache.org/jira/browse/HADOOP-19404) | ABFS: [FNS Over Blob] Update documentation for FNS Blob Onboard |  Major | fs/azure | Anmol Asrani | Anmol Asrani |
| [HADOOP-19448](https://issues.apache.org/jira/browse/HADOOP-19448) | ABFS: [FnsOverBlob][Optimizations] Reduce Network Calls In Create and Mkdir Flow |  Major | fs/azure | Anuj Modi | Anmol Asrani |
| [HADOOP-19354](https://issues.apache.org/jira/browse/HADOOP-19354) | S3A: InputStreams to be created by factory under S3AStore |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-19467](https://issues.apache.org/jira/browse/HADOOP-19467) | HADOOP-19467: [ABFS][FnsOverBlob] Fixing Config Name in Documenatation |  Major | fs/azure | Anuj Modi | Anuj Modi |
| [HADOOP-19443](https://issues.apache.org/jira/browse/HADOOP-19443) | ABFS: [FnsOverBlob][Tests] Update Test Scripts to Run Tests with Blob Endpoint |  Major | fs/azure | Anuj Modi | Anuj Modi |
| [YARN-11258](https://issues.apache.org/jira/browse/YARN-11258) | Upgrade JUnit from 4 to 5 in hadoop-yarn-server-common |  Major | test, yarn | Ashutosh Gupta | Ashutosh Gupta |
| [HDFS-17731](https://issues.apache.org/jira/browse/HDFS-17731) | [ARR] Add unit test for async RouterAdminServer |  Major | rbf | Haobo Zhang | Haobo Zhang |
| [YARN-11244](https://issues.apache.org/jira/browse/YARN-11244) | Upgrade JUnit from 4 to 5 in hadoop-yarn-client |  Major | test, yarn | Ashutosh Gupta | Ashutosh Gupta |
| [YARN-11263](https://issues.apache.org/jira/browse/YARN-11263) | Upgrade JUnit from 4 to 5 in hadoop-yarn-server-nodemanager |  Major | nodemanager, test, yarn | Ashutosh Gupta | Ashutosh Gupta |
| [YARN-11268](https://issues.apache.org/jira/browse/YARN-11268) | Upgrade JUnit from 4 to 5 in  hadoop-yarn-server-timelineservice-documentstore |  Major | test, yarn | Ashutosh Gupta | Ashutosh Gupta |
| [HADOOP-19445](https://issues.apache.org/jira/browse/HADOOP-19445) | ABFS: [FnsOverBlob][Tests] Add Tests For Negative Scenarios Identified for Rename Operation |  Major | fs/azure | Anuj Modi | Manish Bhatt |
| [HADOOP-19185](https://issues.apache.org/jira/browse/HADOOP-19185) | Improve ABFS metric integration with iOStatistics |  Major | fs/azure | Steve Loughran | Manish Bhatt |
| [HADOOP-19348](https://issues.apache.org/jira/browse/HADOOP-19348) | S3A: Add initial support for analytics-accelerator-s3 |  Major | fs/s3 | Ahmar Suhail | Ahmar Suhail |
| [HADOOP-19303](https://issues.apache.org/jira/browse/HADOOP-19303) | VectorIO API to support releasing buffers on failure |  Major | fs, fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-17744](https://issues.apache.org/jira/browse/HDFS-17744) | [ARR] getEnclosingRoot RPC adapts to async rpc |  Major | rbf | Haobo Zhang | Haobo Zhang |
| [HADOOP-19431](https://issues.apache.org/jira/browse/HADOOP-19431) | Upgrade JUnit from 4 to 5 in hadoop-distcp. |  Major | build, test | Shilun Fan | Shilun Fan |
| [HADOOP-19500](https://issues.apache.org/jira/browse/HADOOP-19500) | Skip tests that require JavaScript engine when it's not available |  Major | . | Istvan Toth | Istvan Toth |
| [HADOOP-19506](https://issues.apache.org/jira/browse/HADOOP-19506) | Fix TestThrottledInputStream when bandwidth is equal to throttle limit |  Major | test | Istvan Toth | Istvan Toth |
| [HADOOP-19446](https://issues.apache.org/jira/browse/HADOOP-19446) | ABFS: [FnsOverBlob][Tests] Add Tests For Negative Scenarios Identified for Delete Operation |  Major | fs/azure | Anuj Modi | Manika Joshi |
| [HADOOP-19499](https://issues.apache.org/jira/browse/HADOOP-19499) | Handle JDK-8225499 IpAddr.toString() format changes in tests |  Major | . | Istvan Toth | Istvan Toth |
| [HADOOP-19495](https://issues.apache.org/jira/browse/HADOOP-19495) | [JDK24] Add JDK 24 to Ubuntu 20.04 docker development images |  Major | build | Istvan Toth | Istvan Toth |
| [YARN-11266](https://issues.apache.org/jira/browse/YARN-11266) | Upgrade JUnit from 4 to 5 in hadoop-yarn-server-timelineservice-hbase-tests |  Major | test, yarn | Ashutosh Gupta | Ashutosh Gupta |
| [YARN-11262](https://issues.apache.org/jira/browse/YARN-11262) | Upgrade JUnit from 4 to 5 in hadoop-yarn-server-resourcemanager |  Major | yarn | Ashutosh Gupta | Ashutosh Gupta |
| [HADOOP-15224](https://issues.apache.org/jira/browse/HADOOP-15224) | S3A: Add option to set checksum on S3 objects |  Minor | fs/s3 | Steve Loughran | Raphael Azzolini |
| [HADOOP-19437](https://issues.apache.org/jira/browse/HADOOP-19437) | Upgrade JUnit from 4 to 5 in hadoop-kafka. |  Major | build, test | Shilun Fan | Shilun Fan |
| [HADOOP-19438](https://issues.apache.org/jira/browse/HADOOP-19438) | Upgrade JUnit from 4 to 5 in hadoop-resourceestimator. |  Major | build, test | Shilun Fan | Shilun Fan |
| [HADOOP-19439](https://issues.apache.org/jira/browse/HADOOP-19439) | Upgrade JUnit from 4 to 5 in hadoop-rumen. |  Major | build, test | Shilun Fan | Shilun Fan |
| [HADOOP-19455](https://issues.apache.org/jira/browse/HADOOP-19455) | HADOOP-19455. S3A: Enable logging of SDK client metrics |  Minor | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-19512](https://issues.apache.org/jira/browse/HADOOP-19512) | S3A: Test failures during AWS SDK upgrade |  Minor | fs/s3, test | Steve Loughran | Steve Loughran |
| [HADOOP-19440](https://issues.apache.org/jira/browse/HADOOP-19440) | Upgrade JUnit from 4 to 5 in hadoop-sls. |  Major | build, test | Shilun Fan | Shilun Fan |
| [HADOOP-19434](https://issues.apache.org/jira/browse/HADOOP-19434) | Upgrade JUnit from 4 to 5 in hadoop-federation-balance. |  Major | build, test | Shilun Fan | Shilun Fan |
| [HADOOP-19441](https://issues.apache.org/jira/browse/HADOOP-19441) | Upgrade JUnit from 4 to 5 in hadoop-streaming. |  Major | build, test | Shilun Fan | Shilun Fan |
| [HADOOP-19494](https://issues.apache.org/jira/browse/HADOOP-19494) | ABFS: Fix Case Sensitivity Issue for hdi\_isfolder metadata |  Major | fs/azure | Manish Bhatt | Manish Bhatt |
| [HADOOP-19436](https://issues.apache.org/jira/browse/HADOOP-19436) | Upgrade JUnit from 4 to 5 in hadoop-gridmix. |  Major | build, test | Shilun Fan | Shilun Fan |
| [HADOOP-19531](https://issues.apache.org/jira/browse/HADOOP-19531) | ABFS: [FnsOverBlob] Streaming List Path Result Should Happen Inside Retry Loop |  Major | fs/azure | Anuj Modi | Anuj Modi |
| [HADOOP-19474](https://issues.apache.org/jira/browse/HADOOP-19474) | ABFS: [FnsOverBlob] Listing Optimizations to avoid multiple iteration over list response. |  Major | fs/azure | Anuj Modi | Anuj Modi |
| [HADOOP-19444](https://issues.apache.org/jira/browse/HADOOP-19444) | ABFS: [FnsOverBlob][Tests] Add Tests For Negative Scenarios Identified for Ingress Operations |  Major | fs/azure | Anuj Modi | Anmol Asrani |
| [HADOOP-19515](https://issues.apache.org/jira/browse/HADOOP-19515) | ABFS: [FnsOverBlob] Updating Documentations of Hadoop Drivers for Azure |  Minor | fs/azure | Manika Joshi | Manika Joshi |
| [HADOOP-19256](https://issues.apache.org/jira/browse/HADOOP-19256) | S3A: Support S3 Conditional Writes |  Major | fs/s3 | Ahmar Suhail | Saikat Roy |
| [HADOOP-19428](https://issues.apache.org/jira/browse/HADOOP-19428) | Upgrade JUnit from 4 to 5 in hadoop-dynamometer-blockgen. |  Major | build, test | Shilun Fan | Shilun Fan |
| [HADOOP-19432](https://issues.apache.org/jira/browse/HADOOP-19432) | Upgrade JUnit from 4 to 5 in hadoop-dynamometer-workload. |  Major | build, test | Shilun Fan | Shilun Fan |
| [HADOOP-19429](https://issues.apache.org/jira/browse/HADOOP-19429) | Upgrade JUnit from 4 to 5 in hadoop-dynamometer-infra. |  Major | build, test | Shilun Fan | Shilun Fan |
| [HADOOP-19422](https://issues.apache.org/jira/browse/HADOOP-19422) | Upgrade JUnit from 4 to 5 in hadoop-archive-logs. |  Major | build, test | Shilun Fan | Shilun Fan |
| [HADOOP-19543](https://issues.apache.org/jira/browse/HADOOP-19543) | ABFS: [FnsOverBlob] Remove Duplicates from Blob Endpoint Listing Across Iterations |  Blocker | fs/azure | Anuj Modi | Anuj Modi |
| [HADOOP-19522](https://issues.apache.org/jira/browse/HADOOP-19522) | ABFS: [FnsOverBlob] Rename Recovery Should Succeed When Marker File Exists with Destination Directory |  Blocker | fs/azure | Manish Bhatt | Manish Bhatt |
| [HADOOP-19518](https://issues.apache.org/jira/browse/HADOOP-19518) | ABFS: [FnsOverBlob] WASB to ABFS Migration Config Support Script |  Major | fs/azure | Manika Joshi | Manika Joshi |
| [HADOOP-19497](https://issues.apache.org/jira/browse/HADOOP-19497) | [ABFS] Enable rename and create recovery from client transaction id over DFS endpoint |  Major | fs/azure | Manish Bhatt | Manish Bhatt |
| [HADOOP-19471](https://issues.apache.org/jira/browse/HADOOP-19471) | ABFS: [FnsOverBlob] Support Fixed SAS token at container level |  Major | fs/azure | Manika Joshi | Manika Joshi |
| [HADOOP-19234](https://issues.apache.org/jira/browse/HADOOP-19234) | ABFS: [FnsOverBlob] Adding Integration Tests for Special Scenarios in Blob Endpoint |  Major | fs/azure | Anuj Modi | Anuj Modi |
| [HADOOP-19416](https://issues.apache.org/jira/browse/HADOOP-19416) | Upgrade JUnit from 4 to 5 in hadoop-kms. |  Major | . | Shilun Fan | Shilun Fan |
| [HADOOP-19411](https://issues.apache.org/jira/browse/HADOOP-19411) | Upgrade JUnit from 4 to 5 in hadoop-cos. |  Major | cloud-storage, cos | Shilun Fan | Shilun Fan |
| [HADOOP-19412](https://issues.apache.org/jira/browse/HADOOP-19412) | Upgrade JUnit from 4 to 5 in hadoop-huaweicloud. |  Major | cloud-storage, huaweicloud | Shilun Fan | Shilun Fan |
| [HADOOP-19418](https://issues.apache.org/jira/browse/HADOOP-19418) | Upgrade JUnit from 4 to 5 in hadoop-nfs. |  Major | hadoop-nfs | Shilun Fan | Shilun Fan |
| [HDFS-12431](https://issues.apache.org/jira/browse/HDFS-12431) | Upgrade JUnit from 4 to 5 in hadoop-hdfs hdfs |  Major | test | Ajay Kumar | Hualong Zhang |
| [HADOOP-19430](https://issues.apache.org/jira/browse/HADOOP-19430) | Upgrade JUnit from 4 to 5 in hadoop-datajoin. |  Major | build, test | Shilun Fan | Shilun Fan |
| [HADOOP-19427](https://issues.apache.org/jira/browse/HADOOP-19427) | Upgrade JUnit from 4 to 5 in hadoop-compat-bench. |  Major | build, test | Shilun Fan | Shilun Fan |
| [HADOOP-19423](https://issues.apache.org/jira/browse/HADOOP-19423) | Upgrade JUnit from 4 to 5 in hadoop-archives. |  Major | build, test | Shilun Fan | Shilun Fan |
| [HADOOP-19419](https://issues.apache.org/jira/browse/HADOOP-19419) | Upgrade JUnit from 4 to 5 in hadoop-registry. |  Major | build, test | Shilun Fan | Shilun Fan |
| [HADOOP-19527](https://issues.apache.org/jira/browse/HADOOP-19527) | S3A: testVectoredReadAfterNormalRead() failing with 412 response from S3 |  Blocker | fs/s3 | Steve Loughran | Ahmar Suhail |
| [HADOOP-19485](https://issues.apache.org/jira/browse/HADOOP-19485) | S3A: Upgrade AWS V2 SDK to 2.29.52 |  Major | build, fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-19410](https://issues.apache.org/jira/browse/HADOOP-19410) | Upgrade JUnit from 4 to 5 in hadoop-client-integration-tests. |  Major | test | Shilun Fan | Shilun Fan |
| [HADOOP-19417](https://issues.apache.org/jira/browse/HADOOP-19417) | Upgrade JUnit from 4 to 5 in hadoop-minikdc. |  Major | minikdc, test | Shilun Fan | Shilun Fan |
| [HADOOP-19406](https://issues.apache.org/jira/browse/HADOOP-19406) | ABFS: [FNS Over Blob] Support User Delegation SAS for FNS Blob |  Major | fs/azure | Anmol Asrani | Manika Joshi |
| [HADOOP-19414](https://issues.apache.org/jira/browse/HADOOP-19414) | Upgrade JUnit from 4 to 5 in hadoop-auth. |  Major | hadoop-auth, test | Shilun Fan | Shilun Fan |
| [HADOOP-19557](https://issues.apache.org/jira/browse/HADOOP-19557) | S3A: S3ABlockOutputStream to never log/reject hflush(): calls |  Critical | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-19480](https://issues.apache.org/jira/browse/HADOOP-19480) | S3A Analytics-Accelerator: Upgrade AAL to 1.0.0 |  Major | fs/s3 | Ahmar Suhail |  |
| [HADOOP-19542](https://issues.apache.org/jira/browse/HADOOP-19542) | S3A Analytics-Accelerator: AAL stream factory not being closed |  Blocker | fs/s3 | Ahmar Suhail | Ahmar Suhail |
| [HADOOP-19421](https://issues.apache.org/jira/browse/HADOOP-19421) | Upgrade JUnit from 4 to 5 in hadoop-aliyun. |  Major | build, test | Shilun Fan | Shilun Fan |
| [HADOOP-19540](https://issues.apache.org/jira/browse/HADOOP-19540) | [JDK17] Add ubuntu:noble as a build platform with JDK-17 as default |  Major | hadoop-common, hdfs-client | Vinayakumar B | Vinayakumar B |
| [HADOOP-19433](https://issues.apache.org/jira/browse/HADOOP-19433) | Upgrade JUnit from 4 to 5 in hadoop-extras. |  Major | build, test | Shilun Fan | Shilun Fan |
| [HADOOP-19567](https://issues.apache.org/jira/browse/HADOOP-19567) | S3A: error stack traces printed on analytics stream factory close |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-19572](https://issues.apache.org/jira/browse/HADOOP-19572) | ABFS: [FnsOverBlob] Empty Page Issue on Subsequent ListBlob call |  Major | fs/azure | Anuj Modi | Anuj Modi |
| [HADOOP-19548](https://issues.apache.org/jira/browse/HADOOP-19548) | [ABFS] Fix logging in FSDataInputStream buffersize as that is not used and confusing the customer |  Major | fs/azure, hadoop-common | Mukund Thakur | Manika Joshi |
| [HDFS-17788](https://issues.apache.org/jira/browse/HDFS-17788) | [ARR] getFileInfo not handle exception rightly which may cause FileNotFoundException in DistributedFileSystem |  Major | rbf | Haobo Zhang | Haobo Zhang |
| [HADOOP-19580](https://issues.apache.org/jira/browse/HADOOP-19580) | ABFS: [FnsOverBlob][BugFix] IsNonEmptyDirectory Check should loop on listing using updated continuation token |  Critical | fs/azure | Anuj Modi | Anuj Modi |
| [HADOOP-18940](https://issues.apache.org/jira/browse/HADOOP-18940) | ABFS: Remove commons IOUtils.close() from AbfsOutputStream |  Critical | fs/azure | Steve Loughran | Mehakmeet Singh |
| [HADOOP-19295](https://issues.apache.org/jira/browse/HADOOP-19295) | S3A: fs.s3a.connection.request.timeout too low for large uploads over slow links |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-19415](https://issues.apache.org/jira/browse/HADOOP-19415) | Upgrade JUnit from 4 to 5 in hadoop-common. |  Major | common, test | Shilun Fan | Shilun Fan |
| [HADOOP-19424](https://issues.apache.org/jira/browse/HADOOP-19424) | [S3A] Upgrade JUnit from 4 to 5 in hadoop-aws. |  Major | build, fs/s3, test | Shilun Fan | Shilun Fan |
| [YARN-11267](https://issues.apache.org/jira/browse/YARN-11267) | Upgrade JUnit from 4 to 5 in hadoop-yarn-server-router |  Major | test, yarn | Ashutosh Gupta | Ashutosh Gupta |
| [HADOOP-19575](https://issues.apache.org/jira/browse/HADOOP-19575) | ABFS: [FNSOverBlob] Add Distinct String In User Agent to Get Telemetry for FNS-Blob |  Major | fs/azure | Manika Joshi | Manika Joshi |
| [HADOOP-19329](https://issues.apache.org/jira/browse/HADOOP-19329) | [JDK17] Remove usage of sun.misc.Signal |  Major | build | yanmin | Cheng Pan |
| [HADOOP-19615](https://issues.apache.org/jira/browse/HADOOP-19615) | Upgrade os-maven-plugin to 1.7.1 to support riscv64 |  Minor | build | Lei Wen | Lei Wen |
| [HADOOP-19616](https://issues.apache.org/jira/browse/HADOOP-19616) | Add bswap support for RISC-V |  Major | native | Lei Wen | Lei Wen |
| [HADOOP-19608](https://issues.apache.org/jira/browse/HADOOP-19608) | Upgrade to Junit 5.13.3 |  Major | build, test | Steve Loughran | Steve Loughran |
| [HADOOP-19627](https://issues.apache.org/jira/browse/HADOOP-19627) | S3A: testIfMatchOverwriteWithOutdatedEtag() fails when not using SSE-KMS |  Major | fs/s3 | Ahmar Suhail | Ahmar Suhail |
| [HADOOP-19610](https://issues.apache.org/jira/browse/HADOOP-19610) | S3A: ITests to run under JUnit5 |  Major | fs/s3, test | Steve Loughran | Steve Loughran |
| [HADOOP-18296](https://issues.apache.org/jira/browse/HADOOP-18296) | Memory fragmentation in ChecksumFileSystem Vectored IO implementation. |  Minor | common | Mukund Thakur | Steve Loughran |
| [HADOOP-19425](https://issues.apache.org/jira/browse/HADOOP-19425) | [ABFS] Upgrade JUnit from 4 to 5 in hadoop-azure. |  Major | build, fs/azure, test | Shilun Fan | Shilun Fan |
| [HADOOP-19426](https://issues.apache.org/jira/browse/HADOOP-19426) | Upgrade JUnit from 4 to 5 in hadoop-azure-datalake. |  Major | build, test | Shilun Fan | Shilun Fan |
| [YARN-11850](https://issues.apache.org/jira/browse/YARN-11850) | Remove JUnit4 Parameterized from TestRuncContainerRuntime |  Major | nodemanager | Shilun Fan | Shilun Fan |
| [HADOOP-19646](https://issues.apache.org/jira/browse/HADOOP-19646) | S3A: Migrate hadoop-aws module from JUnit4 Assume to JUnit5 Assumptions |  Major | fs/s3 | Shilun Fan | Shilun Fan |
| [HADOOP-19653](https://issues.apache.org/jira/browse/HADOOP-19653) | [JDK25] Bump ByteBuddy 1.17.6 and ASM 9.8 to support Java 25 bytecode |  Major | build | Cheng Pan | Cheng Pan |
| [HADOOP-19626](https://issues.apache.org/jira/browse/HADOOP-19626) | S3A: AAL - Update to version 1.2.1 |  Major | fs/s3 | Ahmar Suhail | Ahmar Suhail |
| [HADOOP-19636](https://issues.apache.org/jira/browse/HADOOP-19636) | [JDK17] Remove CentOS 7 Support and Clean Up Dockerfile. |  Major | build | Shilun Fan | Shilun Fan |
| [HADOOP-19649](https://issues.apache.org/jira/browse/HADOOP-19649) | ABFS: Fixing Test Failures and Wrong assumptions after Junit Upgrade |  Major | fs/azure | Anuj Modi | Anuj Modi |
| [HADOOP-19645](https://issues.apache.org/jira/browse/HADOOP-19645) | ABFS: [ReadAheadV2] Improve Metrics for Read Calls to identify type of read done. |  Major | fs/azure | Anuj Modi | Anuj Modi |
| [HADOOP-19677](https://issues.apache.org/jira/browse/HADOOP-19677) | [JDK17] Remove mockito-all 1.10.19 and powermock |  Major | test | Tsz-wo Sze | Tsz-wo Sze |
| [HADOOP-19679](https://issues.apache.org/jira/browse/HADOOP-19679) | Maven site task fails with Java 17 |  Major | site | Michael Smith | Michael Smith |
| [HADOOP-19678](https://issues.apache.org/jira/browse/HADOOP-19678) | [JDK17] Remove powermock dependency |  Major | build | Tsz-wo Sze | Tsz-wo Sze |
| [HADOOP-19674](https://issues.apache.org/jira/browse/HADOOP-19674) | [JDK 17] Implementation of JAXB-API has not been found on module path or classpath |  Major | hadoop-common | Bence Kosztolnik | Bence Kosztolnik |
| [HADOOP-19617](https://issues.apache.org/jira/browse/HADOOP-19617) | [JDK17] Remove JUnit4 Dependency |  Major | build | Shilun Fan | Shilun Fan |
| [HADOOP-19688](https://issues.apache.org/jira/browse/HADOOP-19688) | S3A: ITestS3ACommitterMRJob failing on Junit5 |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-19664](https://issues.apache.org/jira/browse/HADOOP-19664) | S3A Analytics-Accelerator: Move AAL to use Java sync client |  Major | fs/s3 | Ahmar Suhail | Ahmar Suhail |
| [HADOOP-19394](https://issues.apache.org/jira/browse/HADOOP-19394) | S3A Analytics Accelerator: vector IO support |  Major | fs/s3 | Steve Loughran |  |
| [HADOOP-19698](https://issues.apache.org/jira/browse/HADOOP-19698) | S3A Analytics-Accelerator: Update LICENSE-binary |  Major | fs/s3 | Ahmar Suhail | Ahmar Suhail |
| [HADOOP-19692](https://issues.apache.org/jira/browse/HADOOP-19692) | Exclude junit 4 transitive dependency |  Major | build | Tsz-wo Sze | Tsz-wo Sze |
| [HADOOP-19691](https://issues.apache.org/jira/browse/HADOOP-19691) | [JDK17] Disallow JUnit4 Imports After JUnit5 Migration |  Major | build | Shilun Fan | Shilun Fan |
| [HADOOP-19663](https://issues.apache.org/jira/browse/HADOOP-19663) | Add RISC-V build infrastructure and placeholder implementation for CRC32 acceleration |  Major | build, common | Ptroc | Ptroc |
| [HADOOP-19695](https://issues.apache.org/jira/browse/HADOOP-19695) | Add dual-stack/IPv6 Support to HttpServer2 |  Minor | hadoop-common | Ferenc Erdelyi | Ferenc Erdelyi |
| [HADOOP-19638](https://issues.apache.org/jira/browse/HADOOP-19638) | [JDK17] Set Up CI Support JDK17 & JDK21 |  Major | build | Shilun Fan | Shilun Fan |
| [HADOOP-19212](https://issues.apache.org/jira/browse/HADOOP-19212) | [JDK25] UserGroupInformation use of Subject needs to move to replacement APIs |  Major | security | Alan Bateman | Cheng Pan |
| [HADOOP-19037](https://issues.apache.org/jira/browse/HADOOP-19037) | S3A: S3A: ITestS3AConfiguration failing with region problems |  Major | fs/s3, test | Steve Loughran |  |
| [HADOOP-19593](https://issues.apache.org/jira/browse/HADOOP-19593) | S3A: tests against third party stores failing after latest update & conditional creation |  Minor | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-19743](https://issues.apache.org/jira/browse/HADOOP-19743) | S3A: ITestS3ACommitterMRJob MR process OOM on java17 |  Minor | fs/s3, test | Steve Loughran | Steve Loughran |
| [HADOOP-19742](https://issues.apache.org/jira/browse/HADOOP-19742) | S3A: AAL - Upgrade version to 1.3.1 |  Major | fs/s3 | Ahmar Suhail |  |
| [HADOOP-19556](https://issues.apache.org/jira/browse/HADOOP-19556) | S3A Analaytics-Accelerator: ITestS3AContractOpen.testInputStreamReadNegativePosition() failing |  Major | fs/s3 | Ahmar Suhail |  |
| [HADOOP-19365](https://issues.apache.org/jira/browse/HADOOP-19365) | S3A Analytics-Accelerator: Add audit header support |  Major | fs/s3 | Ahmar Suhail |  |
| [HADOOP-19587](https://issues.apache.org/jira/browse/HADOOP-19587) | S3A Analytics-Accelerator: Add support SSE-C |  Major | . | Ahmar Suhail |  |
| [HADOOP-19364](https://issues.apache.org/jira/browse/HADOOP-19364) | S3A Analytics-Accelerator: Add IoStatistics support |  Major | fs/s3 | Ahmar Suhail |  |
| [HADOOP-19696](https://issues.apache.org/jira/browse/HADOOP-19696) | hadoop binary distribution to move cloud connectors to hadoop common/lib |  Major | fs/azure, fs/gcs, fs/huawei, fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-11886](https://issues.apache.org/jira/browse/YARN-11886) | Introduce Capacity Scheduler UI |  Major | . | Benjamin Teke | Benjamin Teke |
| [HADOOP-19709](https://issues.apache.org/jira/browse/HADOOP-19709) | [JDK17] Add debian:12 and debian:13 as a build platform with JDK-17 as default |  Major | . | Vinayakumar B | Vinayakumar B |
| [YARN-11888](https://issues.apache.org/jira/browse/YARN-11888) | Serve the Capacity Scheduler UI |  Major | . | Benjamin Teke | Benjamin Teke |
| [HADOOP-19402](https://issues.apache.org/jira/browse/HADOOP-19402) | [JDK11] JDiff Support JDK11 |  Major | build, documentation | Shilun Fan | HuaLong Zhang |
| [HADOOP-19737](https://issues.apache.org/jira/browse/HADOOP-19737) | ABFS: Add metrics to identify improvements with read and write aggressiveness |  Major | fs/azure | Anmol Asrani | Anmol Asrani |
| [HADOOP-19658](https://issues.apache.org/jira/browse/HADOOP-19658) | ABFS: [FNS Over Blob] Support create and rename idempotency on FNS Blob from client side |  Major | fs/azure | Anmol Asrani | Anmol Asrani |
| [HADOOP-19635](https://issues.apache.org/jira/browse/HADOOP-19635) | ABFS: [FNS Over Blob] Marker creation fail exception should not be propagated |  Major | fs/azure | Anmol Asrani | Anmol Asrani |
| [HADOOP-19472](https://issues.apache.org/jira/browse/HADOOP-19472) | ABFS: Enhance performance of ABFS driver for write-heavy workloads |  Minor | fs/azure | Anmol Asrani | Anmol Asrani |
| [HADOOP-19604](https://issues.apache.org/jira/browse/HADOOP-19604) | ABFS: Fix WASB ABFS compatibility issues |  Major | . | Anmol Asrani | Anmol Asrani |
| [HADOOP-19773](https://issues.apache.org/jira/browse/HADOOP-19773) | [JDK17] Use JDK17 as default in precommit Docker image |  Major | build, common | Hualong Zhang | HuaLong Zhang |
| [HADOOP-19179](https://issues.apache.org/jira/browse/HADOOP-19179) | ABFS: Support FNS Accounts over BlobEndpoint |  Major | fs/azure | Sneha Vijayarajan | Sneha Vijayarajan |
| [HADOOP-19613](https://issues.apache.org/jira/browse/HADOOP-19613) | ABFS: [ReadAheadV2] Refactor ReadBufferManager to isolate new code with the current working code |  Major | fs/azure | Anuj Modi | Anuj Modi |
| [HADOOP-19622](https://issues.apache.org/jira/browse/HADOOP-19622) | ABFS: [ReadAheadV2] Implement Read Buffer Manager V2 with improved aggressiveness |  Major | fs/azure | Anuj Modi | Anuj Modi |
| [HADOOP-19729](https://issues.apache.org/jira/browse/HADOOP-19729) | ABFS: [Perf] Network Profiling of Tailing Requests and Killing Bad Connections Proactively |  Major | fs/azure | Anuj Modi | Anuj Modi |
| [HADOOP-19710](https://issues.apache.org/jira/browse/HADOOP-19710) | ABFS: Read Buffer Manager V2 should not be allowed untill implemented |  Major | fs/azure | Anuj Modi | Anuj Modi |
| [HADOOP-19660](https://issues.apache.org/jira/browse/HADOOP-19660) | ABFS: Proposed Enhancement in WorkloadIdentityTokenProvider |  Major | fs/azure | Anuj Modi |  |
| [HADOOP-19596](https://issues.apache.org/jira/browse/HADOOP-19596) | ABFS: [ReadAheadV2] Increase Prefetch Aggressiveness to improve sequential read performance |  Major | fs/azure | Anuj Modi | Anuj Modi |
| [HADOOP-19595](https://issues.apache.org/jira/browse/HADOOP-19595) | ABFS: AbfsConfiguration should store account type information (HNS or FNS) |  Major | fs/azure | Manish Bhatt | Manish Bhatt |
| [HADOOP-19672](https://issues.apache.org/jira/browse/HADOOP-19672) | ABFS: Network Error-Based Client Switchover: Apache to JDK (continuous failure)) |  Major | fs/azure | Manish Bhatt | Manish Bhatt |
| [HADOOP-19609](https://issues.apache.org/jira/browse/HADOOP-19609) | ABFS: Apache Client Connection Pool Relook |  Major | fs/azure | Manish Bhatt | Manish Bhatt |
| [HADOOP-19676](https://issues.apache.org/jira/browse/HADOOP-19676) | ABFS: Enhancing ABFS Driver Metrics for Analytical Usability |  Major | fs/azure | Manish Bhatt | Manish Bhatt |
| [HADOOP-19767](https://issues.apache.org/jira/browse/HADOOP-19767) | ABFS: [Read] Introduce Abfs Input Policy for detecting read patterns |  Major | fs/azure | Anuj Modi | Anuj Modi |
| [HADOOP-19785](https://issues.apache.org/jira/browse/HADOOP-19785) | mvn site fails in JDK17 |  Blocker | build, site | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-19181](https://issues.apache.org/jira/browse/HADOOP-19181) | S3A: IAMCredentialsProvider throttling results in AWS auth failures |  Blocker | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-19724](https://issues.apache.org/jira/browse/HADOOP-19724) | [RISC-V]  Add rv bulk CRC32 (non-CRC32C) optimized path |  Major | hadoop-common | Ptroc | Ptroc |
| [HADOOP-19805](https://issues.apache.org/jira/browse/HADOOP-19805) | When creating a signer, if it is Configurable, set its config |  Trivial | . | Steve Loughran | Steve Loughran |
| [HADOOP-19748](https://issues.apache.org/jira/browse/HADOOP-19748) | S3A: ITestAssumeRole tests failing now STS returns detailed error messages |  Minor | fs/s3, test | Steve Loughran | Steve Loughran |
| [YARN-11927](https://issues.apache.org/jira/browse/YARN-11927) | Follow-up: Cleanup tech debts in the Capacity Scheduler UI code |  Major | . | Benjamin Teke | Benjamin Teke |
| [YARN-11928](https://issues.apache.org/jira/browse/YARN-11928) | Capacity Scheduler UI improvements |  Major | . | Benjamin Teke | Benjamin Teke |
| [YARN-11929](https://issues.apache.org/jira/browse/YARN-11929) | Introduce Zustand's useShallow method to minimize re-renders |  Major | . | Benjamin Teke | Benjamin Teke |
| [HADOOP-19821](https://issues.apache.org/jira/browse/HADOOP-19821) | [JDK25] Token initialization fails with NoClassDefFoundError |  Major | common | Cheng Pan | Cheng Pan |
| [HADOOP-19778](https://issues.apache.org/jira/browse/HADOOP-19778) | Remove Deprecated WASB Code from Hadoop |  Major | fs/azure | Anuj Modi | Anuj Modi |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-11647](https://issues.apache.org/jira/browse/YARN-11647) | more places to use StandardCharsets |  Major | yarn | PJ Fanning | PJ Fanning |
| [HDFS-17362](https://issues.apache.org/jira/browse/HDFS-17362) | RBF: Implement RouterObserverReadConfiguredFailoverProxyProvider |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-19041](https://issues.apache.org/jira/browse/HADOOP-19041) | further use of StandardCharsets |  Major | . | PJ Fanning | PJ Fanning |
| [HADOOP-19024](https://issues.apache.org/jira/browse/HADOOP-19024) | Use bouncycastle jdk18 1.77 |  Major | . | PJ Fanning | PJ Fanning |
| [HDFS-17450](https://issues.apache.org/jira/browse/HDFS-17450) | Add explicit dependency on httpclient jar |  Major | . | PJ Fanning | PJ Fanning |
| [HADOOP-19077](https://issues.apache.org/jira/browse/HADOOP-19077) | Remove use of javax.ws.rs.core.HttpHeaders |  Major | io | PJ Fanning | PJ Fanning |
| [HADOOP-19123](https://issues.apache.org/jira/browse/HADOOP-19123) | Update commons-configuration2 to 2.10.1 due to CVE |  Major | build | PJ Fanning | PJ Fanning |
| [HADOOP-19079](https://issues.apache.org/jira/browse/HADOOP-19079) | HttpExceptionUtils to check that loaded class is really an exception before instantiation |  Major | common, security | PJ Fanning | PJ Fanning |
| [HADOOP-18851](https://issues.apache.org/jira/browse/HADOOP-18851) | Performance improvement for DelegationTokenSecretManager |  Major | common | Vikas Kumar | Vikas Kumar |
| [HADOOP-19216](https://issues.apache.org/jira/browse/HADOOP-19216) | Upgrade Guice from 4.0 to 5.1.0 to support Java 17 |  Major | . | Cheng Pan | Cheng Pan |
| [HDFS-17591](https://issues.apache.org/jira/browse/HDFS-17591) | RBF: Router should follow X-FRAME-OPTIONS protection setting |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-19237](https://issues.apache.org/jira/browse/HADOOP-19237) | upgrade dnsjava to 3.6.1 due to CVEs |  Major | . | PJ Fanning | PJ Fanning |
| [HADOOP-19269](https://issues.apache.org/jira/browse/HADOOP-19269) | Upgrade maven-shade-plugin to 3.6.0 |  Major | . | PJ Fanning | PJ Fanning |
| [HDFS-17040](https://issues.apache.org/jira/browse/HDFS-17040) | Namenode web UI should set content type to application/octet-stream when uploading a file |  Major | ui | Attila Magyar | Attila Magyar |
| [HADOOP-19279](https://issues.apache.org/jira/browse/HADOOP-19279) | ABFS: Disabling Apache Http Client as Default Http Client for ABFS Driver |  Minor | fs/azure | Manika Joshi | Manika Joshi |
| [HADOOP-19315](https://issues.apache.org/jira/browse/HADOOP-19315) | Bump avro from 1.9.2 to 1.11.4 |  Major | build | Dominik Diedrich | Dominik Diedrich |
| [HADOOP-19335](https://issues.apache.org/jira/browse/HADOOP-19335) | Bump netty to 4.1.116 due to CVE-2024-47535 |  Major | . | PJ Fanning | PJ Fanning |
| [HADOOP-19380](https://issues.apache.org/jira/browse/HADOOP-19380) | Update the year to 2025 |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-17080](https://issues.apache.org/jira/browse/HDFS-17080) | Fix ec connection leak (GitHub PR#5807) |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-19397](https://issues.apache.org/jira/browse/HADOOP-19397) | Update LICENSE-binary with jersey 2 details |  Major | . | PJ Fanning | PJ Fanning |
| [HADOOP-19456](https://issues.apache.org/jira/browse/HADOOP-19456) | Upgrade kafka to 3.9.0 to fix CVE-2024-31141 |  Major | build | Palakur Eshwitha Sai | Palakur Eshwitha Sai |
| [HADOOP-19465](https://issues.apache.org/jira/browse/HADOOP-19465) | upgrade to netty 4.1.118 due to CVE-2025-24970 |  Major | build | PJ Fanning | PJ Fanning |
| [HADOOP-19311](https://issues.apache.org/jira/browse/HADOOP-19311) | [ABFS] Implement Backoff and Read Footer metrics using IOStatistics Class |  Major | fs/azure | Manish Bhatt | Manish Bhatt |
| [HADOOP-19383](https://issues.apache.org/jira/browse/HADOOP-19383) | upgrade Mina 2.0.27 due to CVE-2024-52046 |  Major | . | PJ Fanning | PJ Fanning |
| [HADOOP-19450](https://issues.apache.org/jira/browse/HADOOP-19450) | [ABFS] Rename/Create path idempotency client-level resolution |  Major | fs/azure | Manish Bhatt | Manish Bhatt |
| [HADOOP-19259](https://issues.apache.org/jira/browse/HADOOP-19259) | upgrade to jackson 2.14.3 |  Major | common | PJ Fanning | PJ Fanning |
| [YARN-11822](https://issues.apache.org/jira/browse/YARN-11822) | Fix flaky unit test in TestFederationProtocolRecords |  Minor | federation, test | Peter Szucs | Peter Szucs |
| [HADOOP-19634](https://issues.apache.org/jira/browse/HADOOP-19634) | acknowledge Guava license on LimitInputStream |  Major | . | PJ Fanning | PJ Fanning |
| [HADOOP-19685](https://issues.apache.org/jira/browse/HADOOP-19685) | Clover breaks on double semicolon |  Major | fs/s3 | Michael Smith | Michael Smith |
| [HADOOP-19690](https://issues.apache.org/jira/browse/HADOOP-19690) | Bump commons-lang3 to 3.18.0 due to CVE-2025-48924 |  Major | common | PJ Fanning | PJ Fanning |
| [HADOOP-19689](https://issues.apache.org/jira/browse/HADOOP-19689) | Bump netty to 4.1.127 due to CVE-2025-58057 |  Major | common | PJ Fanning | PJ Fanning |
| [HADOOP-19730](https://issues.apache.org/jira/browse/HADOOP-19730) | upgrade bouncycastle to 1.82 due to CVE-2025-8916 |  Major | build, common | PJ Fanning | PJ Fanning |
| [HADOOP-19544](https://issues.apache.org/jira/browse/HADOOP-19544) | upgrade to jackson 2.18.5 |  Major | build | PJ Fanning | PJ Fanning |
| [HADOOP-19771](https://issues.apache.org/jira/browse/HADOOP-19771) | Update the year to 2026 |  Major | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-19765](https://issues.apache.org/jira/browse/HADOOP-19765) | upgrade log4j2.version to 2.25.3 due to CVE-2025-68161 |  Major | build | PJ Fanning | PJ Fanning |
| [HADOOP-19764](https://issues.apache.org/jira/browse/HADOOP-19764) | upgrade amazon-s3-encryption-client-java to 4.0.0+ due to Invisible Salamanders (CVE-2025-14763) |  Major | . | PJ Fanning | PJ Fanning |
| [HADOOP-19775](https://issues.apache.org/jira/browse/HADOOP-19775) | ARM Support for Ubuntu 24.04 build environment |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-19774](https://issues.apache.org/jira/browse/HADOOP-19774) | Bump default OS version to Ubuntu 24.04 |  Blocker | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-19831](https://issues.apache.org/jira/browse/HADOOP-19831) | upgrade to jackson 2.18.6 |  Major | build | PJ Fanning | PJ Fanning |


