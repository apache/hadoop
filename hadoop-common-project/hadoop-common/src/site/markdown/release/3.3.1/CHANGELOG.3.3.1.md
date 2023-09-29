
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

## Release 3.3.1 - 2021-06-13



### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-17338](https://issues.apache.org/jira/browse/HADOOP-17338) | Intermittent S3AInputStream failures: Premature end of Content-Length delimited message body etc |  Major | fs/s3 | Yongjun Zhang | Yongjun Zhang |
| [HDFS-15380](https://issues.apache.org/jira/browse/HDFS-15380) | RBF: Could not fetch real remote IP in RouterWebHdfsMethods |  Major | webhdfs | tomscut | tomscut |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-16916](https://issues.apache.org/jira/browse/HADOOP-16916) | ABFS: Delegation SAS generator for integration with Ranger |  Minor | fs/azure | Thomas Marqardt | Thomas Marqardt |
| [HDFS-13183](https://issues.apache.org/jira/browse/HDFS-13183) | Standby NameNode process getBlocks request to reduce Active load |  Major | balancer & mover, namenode | Xiaoqiao He | Xiaoqiao He |
| [HADOOP-17076](https://issues.apache.org/jira/browse/HADOOP-17076) | ABFS: Delegation SAS Generator Updates |  Minor | fs/azure | Thomas Marqardt | Thomas Marqardt |
| [HADOOP-15891](https://issues.apache.org/jira/browse/HADOOP-15891) | Provide Regex Based Mount Point In Inode Tree |  Major | viewfs | zhenzhao wang | zhenzhao wang |
| [HADOOP-17125](https://issues.apache.org/jira/browse/HADOOP-17125) | Using snappy-java in SnappyCodec |  Major | common | DB Tsai | L. C. Hsieh |
| [HADOOP-17292](https://issues.apache.org/jira/browse/HADOOP-17292) | Using lz4-java in Lz4Codec |  Major | common | L. C. Hsieh | L. C. Hsieh |
| [HDFS-15711](https://issues.apache.org/jira/browse/HDFS-15711) | Add Metrics to HttpFS Server |  Major | httpfs | Ahmed Hussein | Ahmed Hussein |
| [MAPREDUCE-7315](https://issues.apache.org/jira/browse/MAPREDUCE-7315) | LocatedFileStatusFetcher to collect/publish IOStatistics |  Minor | client | Steve Loughran | Steve Loughran |
| [HADOOP-16830](https://issues.apache.org/jira/browse/HADOOP-16830) | Add Public IOStatistics API |  Major | fs, fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-15759](https://issues.apache.org/jira/browse/HDFS-15759) | EC: Verify EC reconstruction correctness on DataNode |  Major | datanode, ec, erasure-coding | Toshihiko Uchida | Toshihiko Uchida |
| [HADOOP-16829](https://issues.apache.org/jira/browse/HADOOP-16829) | Über-jira: S3A Hadoop 3.3.1 features |  Major | fs/s3 | Steve Loughran | Steve Loughran |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-15245](https://issues.apache.org/jira/browse/HDFS-15245) | Improve JournalNode web UI |  Major | journal-node, ui | Jianfei Jiang | Jianfei Jiang |
| [HADOOP-16952](https://issues.apache.org/jira/browse/HADOOP-16952) | Add .diff to gitignore |  Minor | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-16954](https://issues.apache.org/jira/browse/HADOOP-16954) | Add -S option in "Count" command to show only Snapshot Counts |  Major | . | Hemanth Boyina | Hemanth Boyina |
| [HDFS-15247](https://issues.apache.org/jira/browse/HDFS-15247) | RBF: Provide Non DFS Used per DataNode in DataNode UI |  Major | . | Ayush Saxena | Lisheng Sun |
| [MAPREDUCE-7199](https://issues.apache.org/jira/browse/MAPREDUCE-7199) | HsJobsBlock reuse JobACLsManager for checkAccess |  Minor | . | Bibin Chundatt | Bilwa S T |
| [HDFS-15295](https://issues.apache.org/jira/browse/HDFS-15295) | AvailableSpaceBlockPlacementPolicy should use chooseRandomWithStorageTypeTwoTrial() for better performance. |  Minor | . | Jinglun | Jinglun |
| [HADOOP-16054](https://issues.apache.org/jira/browse/HADOOP-16054) | Update Dockerfile to use Bionic |  Major | build, test | Akira Ajisaka | Akira Ajisaka |
| [YARN-10237](https://issues.apache.org/jira/browse/YARN-10237) | Add isAbsoluteResource config for queue in scheduler response |  Minor | scheduler | Prabhu Joseph | Prabhu Joseph |
| [HADOOP-16886](https://issues.apache.org/jira/browse/HADOOP-16886) | Add hadoop.http.idle\_timeout.ms to core-default.xml |  Major | . | Wei-Chiu Chuang | Lisheng Sun |
| [HDFS-14283](https://issues.apache.org/jira/browse/HDFS-14283) | DFSInputStream to prefer cached replica |  Major | . | Wei-Chiu Chuang | Lisheng Sun |
| [HDFS-15338](https://issues.apache.org/jira/browse/HDFS-15338) | listOpenFiles() should throw InvalidPathException in case of invalid paths |  Minor | . | Jinglun | Jinglun |
| [YARN-10160](https://issues.apache.org/jira/browse/YARN-10160) | Add auto queue creation related configs to RMWebService#CapacitySchedulerQueueInfo |  Major | . | Prabhu Joseph | Prabhu Joseph |
| [HDFS-15255](https://issues.apache.org/jira/browse/HDFS-15255) | Consider StorageType when DatanodeManager#sortLocatedBlock() |  Major | . | Lisheng Sun | Lisheng Sun |
| [YARN-10260](https://issues.apache.org/jira/browse/YARN-10260) | Allow transitioning queue from DRAINING to RUNNING state |  Major | . | Jonathan Hung | Bilwa S T |
| [HADOOP-17036](https://issues.apache.org/jira/browse/HADOOP-17036) | TestFTPFileSystem failing as ftp server dir already exists |  Minor | fs, test | Steve Loughran | Mikhail Pryakhin |
| [HDFS-15356](https://issues.apache.org/jira/browse/HDFS-15356) | Unify configuration \`dfs.ha.allow.stale.reads\` to DFSConfigKeys |  Major | hdfs | Xiaoqiao He | Xiaoqiao He |
| [HDFS-15358](https://issues.apache.org/jira/browse/HDFS-15358) | RBF: Unify router datanode UI with namenode datanode UI |  Major | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-17042](https://issues.apache.org/jira/browse/HADOOP-17042) | Hadoop distcp throws "ERROR: Tools helper ///usr/lib/hadoop/libexec/tools/hadoop-distcp.sh was not found" |  Minor | tools/distcp | Aki Tanaka | Aki Tanaka |
| [HDFS-15202](https://issues.apache.org/jira/browse/HDFS-15202) | HDFS-client: boost ShortCircuit Cache |  Minor | dfsclient | Danil Lipovoy | Danil Lipovoy |
| [HDFS-15207](https://issues.apache.org/jira/browse/HDFS-15207) | VolumeScanner skip to scan blocks accessed during recent scan peroid |  Minor | datanode | Yang Yun | Yang Yun |
| [HDFS-14999](https://issues.apache.org/jira/browse/HDFS-14999) | Avoid Potential Infinite Loop in DFSNetworkTopology |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-13639](https://issues.apache.org/jira/browse/HDFS-13639) | SlotReleaser is not fast enough |  Major | hdfs-client | Gang Xie | Lisheng Sun |
| [HDFS-15369](https://issues.apache.org/jira/browse/HDFS-15369) | Refactor method VolumeScanner#runLoop() |  Minor | datanode | Yang Yun | Yang Yun |
| [HADOOP-14698](https://issues.apache.org/jira/browse/HADOOP-14698) | Make copyFromLocal's -t option available for put as well |  Major | . | Andras Bokor | Andras Bokor |
| [HDFS-10792](https://issues.apache.org/jira/browse/HDFS-10792) | RedundantEditLogInputStream should log caught exceptions |  Minor | namenode | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-6492](https://issues.apache.org/jira/browse/YARN-6492) | Generate queue metrics for each partition |  Major | capacity scheduler | Jonathan Hung | Manikandan R |
| [HADOOP-17016](https://issues.apache.org/jira/browse/HADOOP-17016) | Adding Common Counters in ABFS |  Major | fs/azure | Mehakmeet Singh | Mehakmeet Singh |
| [HADOOP-16828](https://issues.apache.org/jira/browse/HADOOP-16828) | Zookeeper Delegation Token Manager fetch sequence number by batch |  Major | . | Fengnan Li | Fengnan Li |
| [HADOOP-14566](https://issues.apache.org/jira/browse/HADOOP-14566) | Add seek support for SFTP FileSystem |  Minor | fs | Azhagu Selvan SP | Mikhail Pryakhin |
| [HADOOP-17047](https://issues.apache.org/jira/browse/HADOOP-17047) | TODO comments exist in trunk while the related issues are already fixed. |  Trivial | . | Rungroj Maipradit | Rungroj Maipradit |
| [HADOOP-17020](https://issues.apache.org/jira/browse/HADOOP-17020) | Improve RawFileSystem Performance |  Minor | fs | Rajesh Balamohan | Mehakmeet Singh |
| [HDFS-15406](https://issues.apache.org/jira/browse/HDFS-15406) | Improve the speed of Datanode Block Scan |  Major | . | Hemanth Boyina | Hemanth Boyina |
| [HADOOP-17090](https://issues.apache.org/jira/browse/HADOOP-17090) | Increase precommit job timeout from 5 hours to 20 hours |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17084](https://issues.apache.org/jira/browse/HADOOP-17084) | Update Dockerfile\_aarch64 to use Bionic |  Major | build, test | RuiChen | zhaorenhai |
| [YARN-8047](https://issues.apache.org/jira/browse/YARN-8047) | RMWebApp make external class pluggable |  Minor | . | Bibin Chundatt | Bilwa S T |
| [YARN-10297](https://issues.apache.org/jira/browse/YARN-10297) | TestContinuousScheduling#testFairSchedulerContinuousSchedulingInitTime fails intermittently |  Major | . | Jonathan Hung | Jim Brennan |
| [HADOOP-17127](https://issues.apache.org/jira/browse/HADOOP-17127) | Use RpcMetrics.TIMEUNIT to initialize rpc queueTime and processingTime |  Minor | common | Jim Brennan | Jim Brennan |
| [HDFS-15404](https://issues.apache.org/jira/browse/HDFS-15404) | ShellCommandFencer should expose info about source |  Major | . | Chen Liang | Chen Liang |
| [HADOOP-17147](https://issues.apache.org/jira/browse/HADOOP-17147) | Dead link in hadoop-kms/index.md.vm |  Minor | documentation, kms | Akira Ajisaka | Xieming Li |
| [HADOOP-17113](https://issues.apache.org/jira/browse/HADOOP-17113) | Adding ReadAhead Counters in ABFS |  Major | fs/azure | Mehakmeet Singh | Mehakmeet Singh |
| [YARN-10343](https://issues.apache.org/jira/browse/YARN-10343) | Legacy RM UI should include labeled metrics for allocated, total, and reserved resources. |  Major | . | Eric Payne | Eric Payne |
| [YARN-1529](https://issues.apache.org/jira/browse/YARN-1529) | Add Localization overhead metrics to NM |  Major | nodemanager | Gera Shegalov | Jim Brennan |
| [YARN-10361](https://issues.apache.org/jira/browse/YARN-10361) | Make custom DAO classes configurable into RMWebApp#JAXBContextResolver |  Major | . | Prabhu Joseph | Bilwa S T |
| [YARN-10251](https://issues.apache.org/jira/browse/YARN-10251) | Show extended resources on legacy RM UI. |  Major | . | Eric Payne | Eric Payne |
| [HDFS-15493](https://issues.apache.org/jira/browse/HDFS-15493) | Update block map and name cache in parallel while loading fsimage. |  Major | namenode | Chengwei Wang | Chengwei Wang |
| [HADOOP-17057](https://issues.apache.org/jira/browse/HADOOP-17057) | ABFS driver enhancement - Allow customizable translation from AAD SPNs and security groups to Linux user and group |  Major | fs/azure | Karthik Amarnath | Karthik Amarnath |
| [HADOOP-17194](https://issues.apache.org/jira/browse/HADOOP-17194) | Adding Context class for AbfsClient to pass AbfsConfigurations to limit number of parameters |  Major | fs/azure | Mehakmeet Singh | Mehakmeet Singh |
| [HADOOP-17065](https://issues.apache.org/jira/browse/HADOOP-17065) | Adding Network Counters in ABFS |  Major | fs/azure | Mehakmeet Singh | Mehakmeet Singh |
| [HADOOP-17159](https://issues.apache.org/jira/browse/HADOOP-17159) | Make UGI support forceful relogin from keytab ignoring the last login time |  Major | security | Sandeep Guggilam | Sandeep Guggilam |
| [YARN-10407](https://issues.apache.org/jira/browse/YARN-10407) | Add phantomjsdriver.log to gitignore |  Minor | . | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-10353](https://issues.apache.org/jira/browse/YARN-10353) | Log vcores used and cumulative cpu in containers monitor |  Minor | yarn | Jim Brennan | Jim Brennan |
| [YARN-10369](https://issues.apache.org/jira/browse/YARN-10369) | Make NMTokenSecretManagerInRM sending NMToken for nodeId DEBUG |  Minor | yarn | Jim Brennan | Jim Brennan |
| [YARN-10390](https://issues.apache.org/jira/browse/YARN-10390) | LeafQueue: retain user limits cache across assignContainers() calls |  Major | capacity scheduler, capacityscheduler | Muhammad Samir Khan | Muhammad Samir Khan |
| [HDFS-15574](https://issues.apache.org/jira/browse/HDFS-15574) | Remove unnecessary sort of block list in DirectoryScanner |  Major | . | Stephen O'Donnell | Stephen O'Donnell |
| [HADOOP-17270](https://issues.apache.org/jira/browse/HADOOP-17270) | Fix testCompressorDecompressorWithExeedBufferLimit to cover the intended scenario |  Minor | test | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-15581](https://issues.apache.org/jira/browse/HDFS-15581) | Access Controlled HTTPFS Proxy |  Minor | httpfs | Richard | Richard |
| [HADOOP-17283](https://issues.apache.org/jira/browse/HADOOP-17283) | Hadoop - Upgrade to JQuery 3.5.1 |  Major | . | Aryan Gupta | Aryan Gupta |
| [HADOOP-17267](https://issues.apache.org/jira/browse/HADOOP-17267) | Add debug-level logs in Filesystem#close |  Minor | fs | Karen Coppage | Karen Coppage |
| [HADOOP-17284](https://issues.apache.org/jira/browse/HADOOP-17284) | Support BCFKS keystores for Hadoop Credential Provider |  Major | . | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-15415](https://issues.apache.org/jira/browse/HDFS-15415) | Reduce locking in Datanode DirectoryScanner |  Major | datanode | Stephen O'Donnell | Stephen O'Donnell |
| [YARN-10451](https://issues.apache.org/jira/browse/YARN-10451) | RM (v1) UI NodesPage can NPE when yarn.io/gpu resource type is defined. |  Major | . | Eric Payne | Eric Payne |
| [HADOOP-17021](https://issues.apache.org/jira/browse/HADOOP-17021) | Add concat fs command |  Minor | fs | Jinglun | Jinglun |
| [MAPREDUCE-7301](https://issues.apache.org/jira/browse/MAPREDUCE-7301) | Expose Mini MR Cluster attribute for testing |  Minor | test | Swaroopa Kadam | Swaroopa Kadam |
| [HDFS-15567](https://issues.apache.org/jira/browse/HDFS-15567) | [SBN Read] HDFS should expose msync() API to allow downstream applications call it explicitly. |  Major | ha, hdfs-client | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-15633](https://issues.apache.org/jira/browse/HDFS-15633) | Avoid redundant RPC calls for getDiskStatus |  Major | dfsclient | Ayush Saxena | Ayush Saxena |
| [YARN-10450](https://issues.apache.org/jira/browse/YARN-10450) | Add cpu and memory utilization per node and cluster-wide metrics |  Minor | yarn | Jim Brennan | Jim Brennan |
| [HADOOP-17302](https://issues.apache.org/jira/browse/HADOOP-17302) | Upgrade to jQuery 3.5.1 in hadoop-sls |  Major | . | Aryan Gupta | Aryan Gupta |
| [HDFS-15652](https://issues.apache.org/jira/browse/HDFS-15652) | Make block size from NNThroughputBenchmark configurable |  Minor | benchmarks | Hui Fei | Hui Fei |
| [YARN-10475](https://issues.apache.org/jira/browse/YARN-10475) | Scale RM-NM heartbeat interval based on node utilization |  Minor | yarn | Jim Brennan | Jim Brennan |
| [HDFS-15665](https://issues.apache.org/jira/browse/HDFS-15665) | Balancer logging improvement |  Major | balancer & mover | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-17342](https://issues.apache.org/jira/browse/HADOOP-17342) | Creating a token identifier should not do kerberos name resolution |  Major | common | Jim Brennan | Jim Brennan |
| [YARN-10479](https://issues.apache.org/jira/browse/YARN-10479) | RMProxy should retry on SocketTimeout Exceptions |  Major | yarn | Jim Brennan | Jim Brennan |
| [HDFS-15623](https://issues.apache.org/jira/browse/HDFS-15623) | Respect configured values of rpc.engine |  Major | hdfs | Hector Sandoval Chaverri | Hector Sandoval Chaverri |
| [HADOOP-17369](https://issues.apache.org/jira/browse/HADOOP-17369) | Bump up snappy-java to 1.1.8.1 |  Minor | . | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-10480](https://issues.apache.org/jira/browse/YARN-10480) | replace href tags with ng-href |  Trivial | . | Gabriel Medeiros Coelho | Gabriel Medeiros Coelho |
| [HADOOP-17367](https://issues.apache.org/jira/browse/HADOOP-17367) | Add InetAddress api to ProxyUsers.authorize |  Major | performance, security | Ahmed Hussein | Ahmed Hussein |
| [MAPREDUCE-7304](https://issues.apache.org/jira/browse/MAPREDUCE-7304) | Enhance the map-reduce Job end notifier to be able to notify the given URL via a custom class |  Major | mrv2 | Daniel Fritsi | Zoltán Erdmann |
| [MAPREDUCE-7309](https://issues.apache.org/jira/browse/MAPREDUCE-7309) | Improve performance of reading resource request for mapper/reducers from config |  Major | applicationmaster | Wangda Tan | Peter Bacsko |
| [HDFS-15694](https://issues.apache.org/jira/browse/HDFS-15694) | Avoid calling UpdateHeartBeatState inside DataNodeDescriptor |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15703](https://issues.apache.org/jira/browse/HDFS-15703) | Don't generate edits for set operations that are no-op |  Major | namenode | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17392](https://issues.apache.org/jira/browse/HADOOP-17392) | Remote exception messages should not include the exception class |  Major | ipc | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15706](https://issues.apache.org/jira/browse/HDFS-15706) | HttpFS: Log more information on request failures |  Major | httpfs | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17389](https://issues.apache.org/jira/browse/HADOOP-17389) | KMS should log full UGI principal |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17425](https://issues.apache.org/jira/browse/HADOOP-17425) | Bump up snappy-java to 1.1.8.2 |  Minor | . | L. C. Hsieh | L. C. Hsieh |
| [HDFS-15720](https://issues.apache.org/jira/browse/HDFS-15720) | namenode audit async logger should add some log4j config |  Minor | hdfs | Max  Xie |  |
| [HDFS-15717](https://issues.apache.org/jira/browse/HDFS-15717) | Improve fsck logging |  Major | logging, namenode | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15704](https://issues.apache.org/jira/browse/HDFS-15704) | Mitigate lease monitor's rapid infinite loop |  Major | namenode | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15569](https://issues.apache.org/jira/browse/HDFS-15569) | Speed up the Storage#doRecover during datanode rolling upgrade |  Major | . | Hemanth Boyina | Hemanth Boyina |
| [HDFS-15751](https://issues.apache.org/jira/browse/HDFS-15751) | Add documentation for msync() API to filesystem.md |  Major | documentation | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-17454](https://issues.apache.org/jira/browse/HADOOP-17454) | [s3a] Disable bucket existence check - set fs.s3a.bucket.probe to 0 |  Major | . | Gabor Bota | Gabor Bota |
| [YARN-10538](https://issues.apache.org/jira/browse/YARN-10538) | Add recommissioning nodes to the list of updated nodes returned to the AM |  Major | . | Srinivas S T | Srinivas S T |
| [YARN-10541](https://issues.apache.org/jira/browse/YARN-10541) | capture the performance metrics of ZKRMStateStore |  Minor | resourcemanager | Max  Xie | Max  Xie |
| [HADOOP-17408](https://issues.apache.org/jira/browse/HADOOP-17408) | Optimize NetworkTopology while sorting of block locations |  Major | common, net | Ahmed Hussein | Ahmed Hussein |
| [YARN-4589](https://issues.apache.org/jira/browse/YARN-4589) | Diagnostics for localization timeouts is lacking |  Major | . | Chang Li | Chang Li |
| [YARN-10562](https://issues.apache.org/jira/browse/YARN-10562) | Follow up changes for YARN-9833 |  Major | yarn | Jim Brennan | Jim Brennan |
| [HDFS-15783](https://issues.apache.org/jira/browse/HDFS-15783) | Speed up BlockPlacementPolicyRackFaultTolerant#verifyBlockPlacement |  Major | block placement | Akira Ajisaka | Akira Ajisaka |
| [YARN-10519](https://issues.apache.org/jira/browse/YARN-10519) | Refactor QueueMetricsForCustomResources class to move to yarn-common package |  Major | . | Minni Mittal | Minni Mittal |
| [HADOOP-17484](https://issues.apache.org/jira/browse/HADOOP-17484) | Typo in hadop-aws index.md |  Trivial | documentation, fs/s3 | Maksim | Maksim |
| [HADOOP-17478](https://issues.apache.org/jira/browse/HADOOP-17478) | Improve the description of hadoop.http.authentication.signature.secret.file |  Minor | documentation | Akira Ajisaka | Akira Ajisaka |
| [MAPREDUCE-7317](https://issues.apache.org/jira/browse/MAPREDUCE-7317) | Add latency information in FileOutputCommitter.mergePaths |  Minor | client | Jungtaek Lim | Jungtaek Lim |
| [HDFS-15789](https://issues.apache.org/jira/browse/HDFS-15789) | Lease renewal does not require namesystem lock |  Major | hdfs | Jim Brennan | Jim Brennan |
| [HADOOP-17501](https://issues.apache.org/jira/browse/HADOOP-17501) | Fix logging typo in ShutdownHookManager |  Major | common | Konstantin Shvachko | Fengnan Li |
| [HADOOP-17354](https://issues.apache.org/jira/browse/HADOOP-17354) | Move Jenkinsfile outside of the root directory |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17508](https://issues.apache.org/jira/browse/HADOOP-17508) | Simplify dependency installation instructions |  Trivial | documentation | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-17509](https://issues.apache.org/jira/browse/HADOOP-17509) | Parallelize building of dependencies |  Minor | build | Gautham Banasandra | Gautham Banasandra |
| [HDFS-15799](https://issues.apache.org/jira/browse/HDFS-15799) | Make DisallowedDatanodeException terse |  Minor | hdfs | Richard | Richard |
| [HDFS-15813](https://issues.apache.org/jira/browse/HDFS-15813) | DataStreamer: keep sending heartbeat packets while streaming |  Major | hdfs | Jim Brennan | Jim Brennan |
| [MAPREDUCE-7319](https://issues.apache.org/jira/browse/MAPREDUCE-7319) | Log list of mappers at trace level in ShuffleHandler audit log |  Minor | yarn | Jim Brennan | Jim Brennan |
| [HADOOP-14402](https://issues.apache.org/jira/browse/HADOOP-14402) | roll out StreamCapabilities across output streams of all filesystems |  Major | fs, fs/adl, fs/azure, fs/oss, fs/s3, fs/swift | Steve Loughran | Steve Loughran |
| [HDFS-15821](https://issues.apache.org/jira/browse/HDFS-15821) | Add metrics for in-service datanodes |  Minor | . | Zehao Chen | Zehao Chen |
| [YARN-10626](https://issues.apache.org/jira/browse/YARN-10626) | Log resource allocation in NM log at container start time |  Major | . | Eric Badger | Eric Badger |
| [HDFS-15815](https://issues.apache.org/jira/browse/HDFS-15815) |  if required storageType are unavailable, log the failed reason during choosing Datanode |  Minor | block placement | Yang Yun | Yang Yun |
| [HDFS-15830](https://issues.apache.org/jira/browse/HDFS-15830) | Support to make dfs.image.parallel.load reconfigurable |  Major | namenode | Hui Fei | Hui Fei |
| [HDFS-15835](https://issues.apache.org/jira/browse/HDFS-15835) | Erasure coding: Add/remove logs for the better readability/debugging |  Minor | erasure-coding, hdfs | Bhavik Patel | Bhavik Patel |
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
| [HDFS-15907](https://issues.apache.org/jira/browse/HDFS-15907) | Reduce Memory Overhead of AclFeature by avoiding AtomicInteger |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-15911](https://issues.apache.org/jira/browse/HDFS-15911) | Provide blocks moved count in Balancer iteration result |  Major | balancer & mover | Viraj Jasani | Viraj Jasani |
| [HDFS-15919](https://issues.apache.org/jira/browse/HDFS-15919) | BlockPoolManager should log stack trace if unable to get Namenode addresses |  Major | datanode | Stephen O'Donnell | Stephen O'Donnell |
| [HADOOP-17531](https://issues.apache.org/jira/browse/HADOOP-17531) | DistCp: Reduce memory usage on copying huge directories |  Critical | . | Ayush Saxena | Ayush Saxena |
| [HDFS-15879](https://issues.apache.org/jira/browse/HDFS-15879) | Exclude slow nodes when choose targets for blocks |  Major | . | tomscut | tomscut |
| [HADOOP-16870](https://issues.apache.org/jira/browse/HADOOP-16870) | Use spotbugs-maven-plugin instead of findbugs-maven-plugin |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17222](https://issues.apache.org/jira/browse/HADOOP-17222) |  Create socket address leveraging URI cache |  Major | common, hdfs-client | fanrui | fanrui |
| [HDFS-15932](https://issues.apache.org/jira/browse/HDFS-15932) | Improve the balancer error message when process exits abnormally. |  Major | . | Renukaprasad C | Renukaprasad C |
| [HADOOP-16524](https://issues.apache.org/jira/browse/HADOOP-16524) | Automatic keystore reloading for HttpServer2 |  Major | . | Kihwal Lee | Borislav Iordanov |
| [HDFS-15931](https://issues.apache.org/jira/browse/HDFS-15931) | Fix non-static inner classes for better memory management |  Major | . | Viraj Jasani | Viraj Jasani |
| [HADOOP-17371](https://issues.apache.org/jira/browse/HADOOP-17371) | Bump Jetty to the latest version 9.4.35 |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-15942](https://issues.apache.org/jira/browse/HDFS-15942) | Increase Quota initialization threads |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [HADOOP-17613](https://issues.apache.org/jira/browse/HADOOP-17613) | Log not flushed fully when daemon shutdown |  Major | common | Renukaprasad C | Renukaprasad C |
| [HDFS-15937](https://issues.apache.org/jira/browse/HDFS-15937) | Reduce memory used during datanode layout upgrade |  Major | datanode | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-15160](https://issues.apache.org/jira/browse/HDFS-15160) | ReplicaMap, Disk Balancer, Directory Scanner and various FsDatasetImpl methods should use datanode readlock |  Major | datanode | Stephen O'Donnell | Stephen O'Donnell |
| [HADOOP-17569](https://issues.apache.org/jira/browse/HADOOP-17569) | Building native code fails on Fedora 33 |  Major | build, common | Kengo Seki | Masatake Iwasaki |
| [HADOOP-17633](https://issues.apache.org/jira/browse/HADOOP-17633) | Please upgrade json-smart dependency to the latest version |  Major | auth, build | helen huang | Viraj Jasani |
| [HADOOP-17620](https://issues.apache.org/jira/browse/HADOOP-17620) | DistCp: Use Iterator for listing target directory as well |  Major | . | Ayush Saxena | Ayush Saxena |
| [YARN-10743](https://issues.apache.org/jira/browse/YARN-10743) | Add a policy for not aggregating for containers which are killed because exceeding container log size limit. |  Major | . | Qi Zhu | Qi Zhu |
| [HDFS-15967](https://issues.apache.org/jira/browse/HDFS-15967) | Improve the log for Short Circuit Local Reads |  Minor | . | Bhavik Patel | Bhavik Patel |
| [HADOOP-17675](https://issues.apache.org/jira/browse/HADOOP-17675) | LdapGroupsMapping$LdapSslSocketFactory ClassNotFoundException |  Major | common | Tamas Mate | István Fajth |
| [HADOOP-11616](https://issues.apache.org/jira/browse/HADOOP-11616) | Remove workaround for Curator's ChildReaper requiring Guava 15+ |  Major | . | Robert Kanter | Viraj Jasani |
| [HDFS-16003](https://issues.apache.org/jira/browse/HDFS-16003) | ProcessReport print invalidatedBlocks should judge debug level at first |  Minor | namanode | lei w | lei w |
| [HDFS-16007](https://issues.apache.org/jira/browse/HDFS-16007) | Deserialization of ReplicaState should avoid throwing ArrayIndexOutOfBoundsException |  Major | . | junwen yang | Viraj Jasani |
| [HADOOP-17615](https://issues.apache.org/jira/browse/HADOOP-17615) | ADLFS: Update SDK version from 2.3.6 to 2.3.9 |  Minor | fs/adl | Bilahari T H | Bilahari T H |
| [HADOOP-16822](https://issues.apache.org/jira/browse/HADOOP-16822) | Provide source artifacts for hadoop-client-api |  Major | . | Karel Kolman | Karel Kolman |
| [HADOOP-17680](https://issues.apache.org/jira/browse/HADOOP-17680) | Allow ProtobufRpcEngine to be extensible |  Major | common | Hector Sandoval Chaverri | Hector Sandoval Chaverri |
| [YARN-10258](https://issues.apache.org/jira/browse/YARN-10258) | Add metrics for 'ApplicationsRunning' in NodeManager |  Minor | nodemanager | ANANDA G B | ANANDA G B |
| [HDFS-15790](https://issues.apache.org/jira/browse/HDFS-15790) | Make ProtobufRpcEngineProtos and ProtobufRpcEngineProtos2 Co-Exist |  Critical | . | David Mollitor | Vinayakumar B |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-15196](https://issues.apache.org/jira/browse/HDFS-15196) | RBF: RouterRpcServer getListing cannot list large dirs correctly |  Critical | . | Fengnan Li | Fengnan Li |
| [HDFS-15252](https://issues.apache.org/jira/browse/HDFS-15252) | HttpFS: setWorkingDirectory should not accept invalid paths |  Major | . | Hemanth Boyina | Hemanth Boyina |
| [HDFS-15249](https://issues.apache.org/jira/browse/HDFS-15249) | ThrottledAsyncChecker is not thread-safe. |  Major | federation | Toshihiro Suzuki | Toshihiro Suzuki |
| [HDFS-15266](https://issues.apache.org/jira/browse/HDFS-15266) | Add missing DFSOps Statistics in WebHDFS |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-15275](https://issues.apache.org/jira/browse/HDFS-15275) | HttpFS: Response of Create was not correct with noredirect and data are true |  Major | . | Hemanth Boyina | Hemanth Boyina |
| [HDFS-15281](https://issues.apache.org/jira/browse/HDFS-15281) | ZKFC ignores dfs.namenode.rpc-bind-host and uses dfs.namenode.rpc-address to bind to host address |  Major | ha, namenode | Dhiraj Hegde | Dhiraj Hegde |
| [HDFS-15297](https://issues.apache.org/jira/browse/HDFS-15297) | TestNNHandlesBlockReportPerStorage::blockReport\_02 fails intermittently in trunk |  Major | datanode, test | Mingliang Liu | Ayush Saxena |
| [HDFS-15210](https://issues.apache.org/jira/browse/HDFS-15210) | EC : File write hanged when DN is shutdown by admin command. |  Major | ec | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-15285](https://issues.apache.org/jira/browse/HDFS-15285) | The same distance and load nodes don't shuffle when consider DataNode load |  Major | . | Lisheng Sun | Lisheng Sun |
| [HDFS-15265](https://issues.apache.org/jira/browse/HDFS-15265) | HttpFS: validate content-type in HttpFSUtils |  Major | . | Hemanth Boyina | Hemanth Boyina |
| [HDFS-15320](https://issues.apache.org/jira/browse/HDFS-15320) | StringIndexOutOfBoundsException in HostRestrictingAuthorizationFilter |  Major | webhdfs | Akira Ajisaka | Akira Ajisaka |
| [YARN-10256](https://issues.apache.org/jira/browse/YARN-10256) | Refactor TestContainerSchedulerQueuing.testContainerUpdateExecTypeGuaranteedToOpportunistic |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15270](https://issues.apache.org/jira/browse/HDFS-15270) | Account for \*env == NULL in hdfsThreadDestructor |  Major | . | Babneet Singh | Babneet Singh |
| [HDFS-15331](https://issues.apache.org/jira/browse/HDFS-15331) | Remove invalid exclusions that minicluster dependency on HDFS |  Major | . | Wanqiang Ji | Wanqiang Ji |
| [YARN-8959](https://issues.apache.org/jira/browse/YARN-8959) | TestContainerResizing fails randomly |  Minor | . | Bibin Chundatt | Ahmed Hussein |
| [HDFS-15332](https://issues.apache.org/jira/browse/HDFS-15332) | Quota Space consumed was wrong in truncate with Snapshots |  Major | . | Hemanth Boyina | Hemanth Boyina |
| [YARN-9017](https://issues.apache.org/jira/browse/YARN-9017) | PlacementRule order is not maintained in CS |  Major | . | Bibin Chundatt | Bilwa S T |
| [HADOOP-17025](https://issues.apache.org/jira/browse/HADOOP-17025) | Fix invalid metastore configuration in S3GuardTool tests |  Minor | fs/s3, test | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-15339](https://issues.apache.org/jira/browse/HDFS-15339) | TestHDFSCLI fails for user names with the dot/dash character |  Major | test | Yan Xiaole | Yan Xiaole |
| [HDFS-15250](https://issues.apache.org/jira/browse/HDFS-15250) | Setting \`dfs.client.use.datanode.hostname\` to true can crash the system because of unhandled UnresolvedAddressException |  Major | . | Ctest | Ctest |
| [HADOOP-16768](https://issues.apache.org/jira/browse/HADOOP-16768) | SnappyCompressor test cases wrongly assume that the compressed data is always smaller than the input data |  Major | io, test | zhao bo | Akira Ajisaka |
| [HDFS-1820](https://issues.apache.org/jira/browse/HDFS-1820) | FTPFileSystem attempts to close the outputstream even when it is not initialised |  Major | hdfs-client | Sudharsan Sampath | Mikhail Pryakhin |
| [HDFS-15243](https://issues.apache.org/jira/browse/HDFS-15243) | Add an option to prevent sub-directories of protected directories from deletion |  Major | 3.1.1 | liuyanyu | liuyanyu |
| [HDFS-14367](https://issues.apache.org/jira/browse/HDFS-14367) | EC: Parameter maxPoolSize in striped reconstruct thread pool isn't affecting number of threads |  Major | ec | Guo Lei | Guo Lei |
| [YARN-9301](https://issues.apache.org/jira/browse/YARN-9301) | Too many InvalidStateTransitionException with SLS |  Major | . | Bibin Chundatt | Bilwa S T |
| [HDFS-15300](https://issues.apache.org/jira/browse/HDFS-15300) | RBF: updateActiveNamenode() is invalid when RPC address is IP |  Major | . | xuzq | xuzq |
| [HDFS-15316](https://issues.apache.org/jira/browse/HDFS-15316) | Deletion failure should not remove directory from snapshottables |  Major | . | Hemanth Boyina | Hemanth Boyina |
| [YARN-8942](https://issues.apache.org/jira/browse/YARN-8942) | PriorityBasedRouterPolicy throws exception if all sub-cluster weights have negative value |  Minor | . | Akshay Agarwal | Bilwa S T |
| [HADOOP-17044](https://issues.apache.org/jira/browse/HADOOP-17044) | Revert "HADOOP-8143. Change distcp to have -pb on by default" |  Major | tools/distcp | Steve Loughran | Steve Loughran |
| [HADOOP-17024](https://issues.apache.org/jira/browse/HADOOP-17024) | ListStatus on ViewFS root (ls "/") should list the linkFallBack root (configured target root). |  Major | fs, viewfs | Uma Maheswara Rao G | Abhishek Das |
| [MAPREDUCE-6826](https://issues.apache.org/jira/browse/MAPREDUCE-6826) | Job fails with InvalidStateTransitonException: Invalid event: JOB\_TASK\_COMPLETED at SUCCEEDED/COMMITTING |  Major | . | Varun Saxena | Bilwa S T |
| [HADOOP-16900](https://issues.apache.org/jira/browse/HADOOP-16900) | Very large files can be truncated when written through S3AFileSystem |  Major | fs/s3 | Andrew Olson | Mukund Thakur |
| [HADOOP-17049](https://issues.apache.org/jira/browse/HADOOP-17049) | javax.activation-api and jakarta.activation-api define overlapping classes |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17040](https://issues.apache.org/jira/browse/HADOOP-17040) | Fix intermittent failure of ITestBlockingThreadPoolExecutorService |  Minor | fs/s3, test | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-15363](https://issues.apache.org/jira/browse/HDFS-15363) | BlockPlacementPolicyWithNodeGroup should validate if it is initialized by NetworkTopologyWithNodeGroup |  Major | . | Hemanth Boyina | Hemanth Boyina |
| [HDFS-15093](https://issues.apache.org/jira/browse/HDFS-15093) | RENAME.TO\_TRASH is ignored When RENAME.OVERWRITE is specified |  Major | . | Harshakiran Reddy | Ayush Saxena |
| [HDFS-12288](https://issues.apache.org/jira/browse/HDFS-12288) | Fix DataNode's xceiver count calculation |  Major | datanode, hdfs | Lukas Majercak | Lisheng Sun |
| [HDFS-15362](https://issues.apache.org/jira/browse/HDFS-15362) | FileWithSnapshotFeature#updateQuotaAndCollectBlocks should collect all distinct blocks |  Major | . | Hemanth Boyina | Hemanth Boyina |
| [HADOOP-7002](https://issues.apache.org/jira/browse/HADOOP-7002) | Wrong description of copyFromLocal and copyToLocal in documentation |  Minor | . | Jingguo Yao | Andras Bokor |
| [HADOOP-17052](https://issues.apache.org/jira/browse/HADOOP-17052) | NetUtils.connect() throws unchecked exception (UnresolvedAddressException) causing clients to abort |  Major | net | Dhiraj Hegde | Dhiraj Hegde |
| [HADOOP-17018](https://issues.apache.org/jira/browse/HADOOP-17018) | Intermittent failing of ITestAbfsStreamStatistics in ABFS |  Minor | fs/azure, test | Mehakmeet Singh | Mehakmeet Singh |
| [YARN-10254](https://issues.apache.org/jira/browse/YARN-10254) | CapacityScheduler incorrect User Group Mapping after leaf queue change |  Major | . | Gergely Pollák | Gergely Pollák |
| [HADOOP-17062](https://issues.apache.org/jira/browse/HADOOP-17062) | Fix shelldocs path in Jenkinsfile |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17056](https://issues.apache.org/jira/browse/HADOOP-17056) | shelldoc fails in hadoop-common |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [YARN-10286](https://issues.apache.org/jira/browse/YARN-10286) | PendingContainers bugs in the scheduler outputs |  Critical | . | Adam Antal | Andras Gyori |
| [HDFS-15396](https://issues.apache.org/jira/browse/HDFS-15396) | Fix TestViewFileSystemOverloadSchemeHdfsFileSystemContract#testListStatusRootDir |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-15386](https://issues.apache.org/jira/browse/HDFS-15386) | ReplicaNotFoundException keeps happening in DN after removing multiple DN's data directories |  Major | . | Toshihiro Suzuki | Toshihiro Suzuki |
| [HDFS-15398](https://issues.apache.org/jira/browse/HDFS-15398) | EC: hdfs client hangs due to exception during addBlock |  Critical | ec, hdfs-client | Hongbing Wang | Hongbing Wang |
| [YARN-10300](https://issues.apache.org/jira/browse/YARN-10300) | appMasterHost not set in RM ApplicationSummary when AM fails before first heartbeat |  Major | . | Eric Badger | Eric Badger |
| [HADOOP-17059](https://issues.apache.org/jira/browse/HADOOP-17059) | ArrayIndexOfboundsException in ViewFileSystem#listStatus |  Major | viewfs | Hemanth Boyina | Hemanth Boyina |
| [YARN-10296](https://issues.apache.org/jira/browse/YARN-10296) | Make ContainerPBImpl#getId/setId synchronized |  Minor | . | Benjamin Teke | Benjamin Teke |
| [HADOOP-17060](https://issues.apache.org/jira/browse/HADOOP-17060) | listStatus and getFileStatus behave inconsistent in the case of ViewFs implementation for isDirectory |  Major | viewfs | Srinivasu Majeti | Uma Maheswara Rao G |
| [YARN-10312](https://issues.apache.org/jira/browse/YARN-10312) | Add support for yarn logs -logFile to retain backward compatibility |  Major | client | Jim Brennan | Jim Brennan |
| [HDFS-15351](https://issues.apache.org/jira/browse/HDFS-15351) | Blocks scheduled count was wrong on truncate |  Major | . | Hemanth Boyina | Hemanth Boyina |
| [HDFS-15403](https://issues.apache.org/jira/browse/HDFS-15403) | NPE in FileIoProvider#transferToSocketFully |  Major | . | Hemanth Boyina | Hemanth Boyina |
| [HDFS-15372](https://issues.apache.org/jira/browse/HDFS-15372) | Files in snapshots no longer see attribute provider permissions |  Major | . | Stephen O'Donnell | Stephen O'Donnell |
| [MAPREDUCE-7281](https://issues.apache.org/jira/browse/MAPREDUCE-7281) | Fix NoClassDefFoundError on 'mapred minicluster' |  Major | . | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-17029](https://issues.apache.org/jira/browse/HADOOP-17029) | ViewFS does not return correct user/group and ACL |  Major | fs, viewfs | Abhishek Das | Abhishek Das |
| [HDFS-14546](https://issues.apache.org/jira/browse/HDFS-14546) | Document block placement policies |  Major | . | Íñigo Goiri | Amithsha |
| [HADOOP-17068](https://issues.apache.org/jira/browse/HADOOP-17068) | client fails forever when namenode ipaddr changed |  Major | hdfs-client | Sean Chow | Sean Chow |
| [HADOOP-17089](https://issues.apache.org/jira/browse/HADOOP-17089) | WASB: Update azure-storage-java SDK |  Critical | fs/azure | Thomas Marqardt | Thomas Marqardt |
| [HDFS-15378](https://issues.apache.org/jira/browse/HDFS-15378) | TestReconstructStripedFile#testErasureCodingWorkerXmitsWeight is failing on trunk |  Major | . | Hemanth Boyina | Hemanth Boyina |
| [YARN-9903](https://issues.apache.org/jira/browse/YARN-9903) | Support reservations continue looking for Node Labels |  Major | . | Tarun Parimi | Jim Brennan |
| [YARN-10331](https://issues.apache.org/jira/browse/YARN-10331) | Upgrade node.js to 10.21.0 |  Critical | build, yarn-ui-v2 | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17032](https://issues.apache.org/jira/browse/HADOOP-17032) | Handle an internal dir in viewfs having multiple children mount points pointing to different filesystems |  Major | fs, viewfs | Abhishek Das | Abhishek Das |
| [YARN-10318](https://issues.apache.org/jira/browse/YARN-10318) | ApplicationHistory Web UI incorrect column indexing |  Minor | yarn | Andras Gyori | Andras Gyori |
| [YARN-10330](https://issues.apache.org/jira/browse/YARN-10330) | Add missing test scenarios to TestUserGroupMappingPlacementRule and TestAppNameMappingPlacementRule |  Major | capacity scheduler, capacityscheduler, test | Peter Bacsko | Peter Bacsko |
| [HDFS-15446](https://issues.apache.org/jira/browse/HDFS-15446) | CreateSnapshotOp fails during edit log loading for /.reserved/raw/path with error java.io.FileNotFoundException: Directory does not exist: /.reserved/raw/path |  Major | hdfs | Srinivasu Majeti | Stephen O'Donnell |
| [HADOOP-17081](https://issues.apache.org/jira/browse/HADOOP-17081) | MetricsSystem doesn't start the sink adapters on restart |  Minor | metrics | Madhusoodan | Madhusoodan |
| [HDFS-15451](https://issues.apache.org/jira/browse/HDFS-15451) | Restarting name node stuck in safe mode when using provided storage |  Major | namenode | shanyu zhao | shanyu zhao |
| [HADOOP-17117](https://issues.apache.org/jira/browse/HADOOP-17117) | Fix typos in hadoop-aws documentation |  Trivial | documentation, fs/s3 | Sebastian Nagel | Sebastian Nagel |
| [HADOOP-17120](https://issues.apache.org/jira/browse/HADOOP-17120) | Fix failure of docker image creation due to pip2 install error |  Major | . | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-10344](https://issues.apache.org/jira/browse/YARN-10344) | Sync netty versions in hadoop-yarn-csi |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [YARN-10341](https://issues.apache.org/jira/browse/YARN-10341) | Yarn Service Container Completed event doesn't get processed |  Critical | . | Bilwa S T | Bilwa S T |
| [HADOOP-16998](https://issues.apache.org/jira/browse/HADOOP-16998) | WASB : NativeAzureFsOutputStream#close() throwing IllegalArgumentException |  Major | fs/azure | Anoop Sam John | Anoop Sam John |
| [YARN-10348](https://issues.apache.org/jira/browse/YARN-10348) | Allow RM to always cancel tokens after app completes |  Major | yarn | Jim Brennan | Jim Brennan |
| [MAPREDUCE-7284](https://issues.apache.org/jira/browse/MAPREDUCE-7284) | TestCombineFileInputFormat#testMissingBlocks fails |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [HDFS-14498](https://issues.apache.org/jira/browse/HDFS-14498) | LeaseManager can loop forever on the file for which create has failed |  Major | namenode | Sergey Shelukhin | Stephen O'Donnell |
| [HADOOP-17130](https://issues.apache.org/jira/browse/HADOOP-17130) | Configuration.getValByRegex() shouldn't update the results while fetching. |  Major | common | Mukund Thakur | Mukund Thakur |
| [HDFS-15198](https://issues.apache.org/jira/browse/HDFS-15198) | RBF: Add test for MountTableRefresherService failed to refresh other router MountTableEntries in secure mode |  Major | rbf | zhengchenyu | zhengchenyu |
| [HADOOP-17119](https://issues.apache.org/jira/browse/HADOOP-17119) | Jetty upgrade to 9.4.x causes MR app fail with IOException |  Major | . | Bilwa S T | Bilwa S T |
| [HDFS-15246](https://issues.apache.org/jira/browse/HDFS-15246) | ArrayIndexOfboundsException in BlockManager CreateLocatedBlock |  Major | . | Hemanth Boyina | Hemanth Boyina |
| [HADOOP-17138](https://issues.apache.org/jira/browse/HADOOP-17138) | Fix spotbugs warnings surfaced after upgrade to 4.0.6 |  Minor | . | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-4771](https://issues.apache.org/jira/browse/YARN-4771) | Some containers can be skipped during log aggregation after NM restart |  Major | nodemanager | Jason Darrell Lowe | Jim Brennan |
| [YARN-10367](https://issues.apache.org/jira/browse/YARN-10367) | Failed to get nodejs 10.21.0 when building docker image |  Blocker | build, webapp | Akira Ajisaka | Akira Ajisaka |
| [MAPREDUCE-7051](https://issues.apache.org/jira/browse/MAPREDUCE-7051) | Fix typo in MultipleOutputFormat |  Trivial | . | ywheel | ywheel |
| [HDFS-15313](https://issues.apache.org/jira/browse/HDFS-15313) | Ensure inodes in active filesystem are not deleted during snapshot delete |  Major | snapshots | Shashikant Banerjee | Shashikant Banerjee |
| [HDFS-14950](https://issues.apache.org/jira/browse/HDFS-14950) | missing libhdfspp libs in dist-package |  Major | build, libhdfs++ | Yuan Zhou | Yuan Zhou |
| [YARN-10359](https://issues.apache.org/jira/browse/YARN-10359) | Log container report only if list is not empty |  Minor | . | Bilwa S T | Bilwa S T |
| [YARN-10229](https://issues.apache.org/jira/browse/YARN-10229) | [Federation] Client should be able to submit application to RM directly using normal client conf |  Major | amrmproxy, federation | JohnsonGuo | Bilwa S T |
| [HDFS-15503](https://issues.apache.org/jira/browse/HDFS-15503) | File and directory permissions are not able to be modified from WebUI |  Major | . | Hemanth Boyina | Hemanth Boyina |
| [HADOOP-17184](https://issues.apache.org/jira/browse/HADOOP-17184) | Add --mvn-custom-repos parameter to yetus calls |  Major | build | Mingliang Liu | Mingliang Liu |
| [HDFS-15499](https://issues.apache.org/jira/browse/HDFS-15499) | Clean up httpfs/pom.xml to remove aws-java-sdk-s3 exclusion |  Major | httpfs | Mingliang Liu | Mingliang Liu |
| [HADOOP-17186](https://issues.apache.org/jira/browse/HADOOP-17186) | Fixing javadoc in ListingOperationCallbacks |  Major | build, documentation | Akira Ajisaka | Mukund Thakur |
| [HADOOP-17164](https://issues.apache.org/jira/browse/HADOOP-17164) | UGI loginUserFromKeytab doesn't set the last login time |  Major | security | Sandeep Guggilam | Sandeep Guggilam |
| [YARN-4575](https://issues.apache.org/jira/browse/YARN-4575) | ApplicationResourceUsageReport should return ALL  reserved resource |  Major | . | Bibin Chundatt | Bibin Chundatt |
| [YARN-10388](https://issues.apache.org/jira/browse/YARN-10388) | RMNode updatedCapability flag not set while RecommissionNodeTransition |  Major | resourcemanager | Pranjal Protim Borah | Pranjal Protim Borah |
| [HDFS-15443](https://issues.apache.org/jira/browse/HDFS-15443) | Setting dfs.datanode.max.transfer.threads to a very small value can cause strange failure. |  Major | datanode | AMC-team | AMC-team |
| [HDFS-15508](https://issues.apache.org/jira/browse/HDFS-15508) | [JDK 11] Fix javadoc errors in hadoop-hdfs-rbf module |  Major | documentation | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15506](https://issues.apache.org/jira/browse/HDFS-15506) | [JDK 11] Fix javadoc errors in hadoop-hdfs module |  Major | documentation | Akira Ajisaka | Xieming Li |
| [HDFS-15507](https://issues.apache.org/jira/browse/HDFS-15507) | [JDK 11] Fix javadoc errors in hadoop-hdfs-client module |  Major | documentation | Akira Ajisaka | Xieming Li |
| [HADOOP-17196](https://issues.apache.org/jira/browse/HADOOP-17196) | Fix C/C++ standard warnings |  Major | build | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-17204](https://issues.apache.org/jira/browse/HADOOP-17204) | Fix typo in Hadoop KMS document |  Trivial | documentation, kms | Akira Ajisaka | Xieming Li |
| [HADOOP-17192](https://issues.apache.org/jira/browse/HADOOP-17192) | ITestS3AHugeFilesSSECDiskBlock failing because of bucket overrides |  Major | fs/s3 | Mukund Thakur | Mukund Thakur |
| [YARN-10336](https://issues.apache.org/jira/browse/YARN-10336) | RM page should throw exception when command injected in RM REST API to get applications |  Major | . | Rajshree Mishra | Bilwa S T |
| [HDFS-15439](https://issues.apache.org/jira/browse/HDFS-15439) | Setting dfs.mover.retry.max.attempts to negative value will retry forever. |  Major | balancer & mover | AMC-team | AMC-team |
| [YARN-10391](https://issues.apache.org/jira/browse/YARN-10391) | --module-gpu functionality is broken in container-executor |  Major | nodemanager | Eric Badger | Eric Badger |
| [HADOOP-17122](https://issues.apache.org/jira/browse/HADOOP-17122) | Bug in preserving Directory Attributes in DistCp with Atomic Copy |  Major | tools/distcp | Swaminathan Balachandran |  |
| [HDFS-14504](https://issues.apache.org/jira/browse/HDFS-14504) | Rename with Snapshots does not honor quota limit |  Major | . | Shashikant Banerjee | Hemanth Boyina |
| [HADOOP-17209](https://issues.apache.org/jira/browse/HADOOP-17209) | Erasure Coding: Native library memory leak |  Major | native | Sean Chow | Sean Chow |
| [HADOOP-16925](https://issues.apache.org/jira/browse/HADOOP-16925) | MetricsConfig incorrectly loads the configuration whose value is String list in the properties file |  Major | metrics | Jiayi Liu | Jiayi Liu |
| [HADOOP-17220](https://issues.apache.org/jira/browse/HADOOP-17220) | Upgrade slf4j to 1.7.30 ( To Address: CVE-2018-8088) |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-14852](https://issues.apache.org/jira/browse/HDFS-14852) | Removing from LowRedundancyBlocks does not remove the block from all queues |  Major | namenode | Hui Fei | Hui Fei |
| [HDFS-15536](https://issues.apache.org/jira/browse/HDFS-15536) | RBF:  Clear Quota in Router was not consistent |  Critical | . | Hemanth Boyina | Hemanth Boyina |
| [HDFS-15510](https://issues.apache.org/jira/browse/HDFS-15510) | RBF: Quota and Content Summary was not correct in Multiple Destinations |  Critical | . | Hemanth Boyina | Hemanth Boyina |
| [HDFS-15540](https://issues.apache.org/jira/browse/HDFS-15540) | Directories protected from delete can still be moved to the trash |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [HADOOP-17129](https://issues.apache.org/jira/browse/HADOOP-17129) | Validating storage keys in ABFS correctly |  Major | fs/azure | Mehakmeet Singh | Mehakmeet Singh |
| [HDFS-15471](https://issues.apache.org/jira/browse/HDFS-15471) | TestHDFSContractMultipartUploader fails on trunk |  Major | test | Ahmed Hussein | Steve Loughran |
| [HDFS-15290](https://issues.apache.org/jira/browse/HDFS-15290) | NPE in HttpServer during NameNode startup |  Major | namenode | Konstantin Shvachko | Simbarashe Dzinamarira |
| [HADOOP-17158](https://issues.apache.org/jira/browse/HADOOP-17158) | Test timeout for ITestAbfsInputStreamStatistics#testReadAheadCounters |  Major | fs/azure | Mehakmeet Singh | Mehakmeet Singh |
| [HADOOP-17229](https://issues.apache.org/jira/browse/HADOOP-17229) | Test failure as failed request body counted in byte received metric - ITestAbfsNetworkStatistics#testAbfsHttpResponseStatistics |  Major | fs/azure, test | Sneha Vijayarajan | Mehakmeet Singh |
| [YARN-10397](https://issues.apache.org/jira/browse/YARN-10397) | SchedulerRequest should be forwarded to scheduler if custom scheduler supports placement constraints |  Minor | . | Bilwa S T | Bilwa S T |
| [HDFS-15573](https://issues.apache.org/jira/browse/HDFS-15573) | Only log warning if considerLoad and considerStorageType are both true |  Major | . | Stephen O'Donnell | Stephen O'Donnell |
| [YARN-10430](https://issues.apache.org/jira/browse/YARN-10430) | Log improvements in NodeStatusUpdaterImpl |  Minor | nodemanager | Bilwa S T | Bilwa S T |
| [HADOOP-17246](https://issues.apache.org/jira/browse/HADOOP-17246) | Fix build the hadoop-build Docker image failed |  Major | build | Wanqiang Ji | Wanqiang Ji |
| [HDFS-15438](https://issues.apache.org/jira/browse/HDFS-15438) | Setting dfs.disk.balancer.max.disk.errors = 0 will fail the block copy |  Major | balancer & mover | AMC-team | AMC-team |
| [HADOOP-15136](https://issues.apache.org/jira/browse/HADOOP-15136) | Typo in rename spec pseudocode |  Major | documentation | Rae Marks |  |
| [HADOOP-17088](https://issues.apache.org/jira/browse/HADOOP-17088) | Failed to load XInclude files with relative path. |  Minor | conf | Yushi Hayasaka | Yushi Hayasaka |
| [MAPREDUCE-7294](https://issues.apache.org/jira/browse/MAPREDUCE-7294) | Only application master should upload resource to Yarn Shared Cache |  Major | mrv2 | zhenzhao wang | zhenzhao wang |
| [HADOOP-17277](https://issues.apache.org/jira/browse/HADOOP-17277) | Correct spelling errors for separator |  Trivial | common | Hui Fei | Hui Fei |
| [HADOOP-17286](https://issues.apache.org/jira/browse/HADOOP-17286) | Upgrade to jQuery 3.5.1 in hadoop-yarn-common |  Major | . | Wei-Chiu Chuang | Aryan Gupta |
| [HDFS-15591](https://issues.apache.org/jira/browse/HDFS-15591) | RBF: Fix webHdfs file display error |  Major | . | wangzhaohui | wangzhaohui |
| [MAPREDUCE-7289](https://issues.apache.org/jira/browse/MAPREDUCE-7289) | Fix wrong comment in LongLong.java |  Trivial | documentation, examples | Akira Ajisaka | Wanqiang Ji |
| [YARN-9809](https://issues.apache.org/jira/browse/YARN-9809) | NMs should supply a health status when registering with RM |  Major | . | Eric Badger | Eric Badger |
| [HADOOP-17300](https://issues.apache.org/jira/browse/HADOOP-17300) | FileSystem.DirListingIterator.next() call should return NoSuchElementException |  Major | common, fs | Mukund Thakur | Mukund Thakur |
| [YARN-10393](https://issues.apache.org/jira/browse/YARN-10393) | MR job live lock caused by completed state container leak in heartbeat between node manager and RM |  Major | nodemanager, yarn | zhenzhao wang | Jim Brennan |
| [HDFS-15253](https://issues.apache.org/jira/browse/HDFS-15253) | Set default throttle value on dfs.image.transfer.bandwidthPerSec |  Major | namenode | Karthik Palanisamy | Karthik Palanisamy |
| [HDFS-15610](https://issues.apache.org/jira/browse/HDFS-15610) | Reduce datanode upgrade/hardlink thread |  Major | datanode | Karthik Palanisamy | Karthik Palanisamy |
| [YARN-10455](https://issues.apache.org/jira/browse/YARN-10455) | TestNMProxy.testNMProxyRPCRetry is not consistent |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15456](https://issues.apache.org/jira/browse/HDFS-15456) | TestExternalStoragePolicySatisfier fails intermittently |  Major | . | Ahmed Hussein | Leon Gao |
| [HADOOP-17223](https://issues.apache.org/jira/browse/HADOOP-17223) | update  org.apache.httpcomponents:httpclient to 4.5.13 and httpcore to 4.4.13 |  Blocker | . | Pranav Bheda | Pranav Bheda |
| [HDFS-15628](https://issues.apache.org/jira/browse/HDFS-15628) | HttpFS server throws NPE if a file is a symlink |  Major | fs, httpfs | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15627](https://issues.apache.org/jira/browse/HDFS-15627) | Audit log deletes before collecting blocks |  Major | logging, namenode | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17309](https://issues.apache.org/jira/browse/HADOOP-17309) | Javadoc warnings and errors are ignored in the precommit jobs |  Major | build, documentation | Akira Ajisaka | Akira Ajisaka |
| [HDFS-14383](https://issues.apache.org/jira/browse/HDFS-14383) | Compute datanode load based on StoragePolicy |  Major | hdfs, namenode | Karthik Palanisamy | Ayush Saxena |
| [HADOOP-17310](https://issues.apache.org/jira/browse/HADOOP-17310) | Touch command with -c  option is broken |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-15626](https://issues.apache.org/jira/browse/HDFS-15626) | TestWebHDFS.testLargeDirectory failing |  Major | test, webhdfs | Mukund Thakur | Mukund Thakur |
| [HADOOP-17298](https://issues.apache.org/jira/browse/HADOOP-17298) | Backslash in username causes build failure in the environment started by start-build-env.sh. |  Minor | build | Takeru Kuramoto | Takeru Kuramoto |
| [HDFS-15639](https://issues.apache.org/jira/browse/HDFS-15639) | [JDK 11] Fix Javadoc errors in hadoop-hdfs-client |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-15622](https://issues.apache.org/jira/browse/HDFS-15622) | Deleted blocks linger in the replications queue |  Major | hdfs | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17308](https://issues.apache.org/jira/browse/HADOOP-17308) | WASB : PageBlobOutputStream succeeding hflush even when underlying flush to storage failed |  Critical | . | Anoop Sam John | Anoop Sam John |
| [HDFS-15641](https://issues.apache.org/jira/browse/HDFS-15641) | DataNode could meet deadlock if invoke refreshNameNode |  Critical | . | Hongbing Wang | Hongbing Wang |
| [HADOOP-17328](https://issues.apache.org/jira/browse/HADOOP-17328) | LazyPersist Overwrite fails in direct write mode |  Major | . | Ayush Saxena | Ayush Saxena |
| [MAPREDUCE-7302](https://issues.apache.org/jira/browse/MAPREDUCE-7302) | Upgrading to JUnit 4.13 causes testcase TestFetcher.testCorruptedIFile() to fail |  Major | test | Peter Bacsko | Peter Bacsko |
| [HDFS-15644](https://issues.apache.org/jira/browse/HDFS-15644) | Failed volumes can cause DNs to stop block reporting |  Major | block placement, datanode | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17236](https://issues.apache.org/jira/browse/HADOOP-17236) | Bump up snakeyaml to 1.26 to mitigate CVE-2017-18640 |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-10467](https://issues.apache.org/jira/browse/YARN-10467) | ContainerIdPBImpl objects can be leaked in RMNodeImpl.completedContainers |  Major | resourcemanager | Haibo Chen | Haibo Chen |
| [HADOOP-17329](https://issues.apache.org/jira/browse/HADOOP-17329) | mvn site commands fails due to MetricsSystemImpl changes |  Major | . | Xiaoqiao He | Xiaoqiao He |
| [HDFS-15651](https://issues.apache.org/jira/browse/HDFS-15651) | Client could not obtain block when DN CommandProcessingThread exit |  Major | . | Yiqun Lin | Aiphago |
| [HADOOP-17340](https://issues.apache.org/jira/browse/HADOOP-17340) | TestLdapGroupsMapping failing -string mismatch in exception validation |  Major | test | Steve Loughran | Steve Loughran |
| [HDFS-15667](https://issues.apache.org/jira/browse/HDFS-15667) | Audit log record the unexpected allowed result when delete called |  Major | hdfs | Baolong Mao | Baolong Mao |
| [HADOOP-17352](https://issues.apache.org/jira/browse/HADOOP-17352) | Update PATCH\_NAMING\_RULE in the personality file |  Minor | build | Akira Ajisaka | Akira Ajisaka |
| [YARN-10458](https://issues.apache.org/jira/browse/YARN-10458) | Hive On Tez queries fails upon submission to dynamically created pools |  Major | resourcemanager | Anand Srinivasan | Peter Bacsko |
| [HDFS-15485](https://issues.apache.org/jira/browse/HDFS-15485) | Fix outdated properties of JournalNode when performing rollback |  Minor | . | Deegue | Deegue |
| [HADOOP-17096](https://issues.apache.org/jira/browse/HADOOP-17096) | ZStandardCompressor throws java.lang.InternalError: Error (generic) |  Major | io | Stephen Jung (Stripe) | Stephen Jung (Stripe) |
| [HADOOP-17327](https://issues.apache.org/jira/browse/HADOOP-17327) | NPE when starting MiniYARNCluster from hadoop-client-minicluster |  Critical | . | Chao Sun |  |
| [HADOOP-17324](https://issues.apache.org/jira/browse/HADOOP-17324) | Don't relocate org.bouncycastle in shaded client jars |  Critical | . | Chao Sun | Chao Sun |
| [HADOOP-17373](https://issues.apache.org/jira/browse/HADOOP-17373) | hadoop-client-integration-tests doesn't work when building with skipShade |  Major | . | Chao Sun | Chao Sun |
| [HADOOP-17365](https://issues.apache.org/jira/browse/HADOOP-17365) | Contract test for renaming over existing file is too lenient |  Minor | test | Attila Doroszlai | Attila Doroszlai |
| [HADOOP-17358](https://issues.apache.org/jira/browse/HADOOP-17358) | Improve excessive reloading of Configurations |  Major | conf | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15538](https://issues.apache.org/jira/browse/HDFS-15538) | Fix the documentation for dfs.namenode.replication.max-streams in hdfs-default.xml |  Major | . | Xieming Li | Xieming Li |
| [HADOOP-17362](https://issues.apache.org/jira/browse/HADOOP-17362) | Doing hadoop ls on Har file triggers too many RPC calls |  Major | fs | Ahmed Hussein | Ahmed Hussein |
| [YARN-10485](https://issues.apache.org/jira/browse/YARN-10485) | TimelineConnector swallows InterruptedException |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17360](https://issues.apache.org/jira/browse/HADOOP-17360) | Log the remote address for authentication success |  Minor | ipc | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15685](https://issues.apache.org/jira/browse/HDFS-15685) | [JDK 14] TestConfiguredFailoverProxyProvider#testResolveDomainNameUsingDNS fails |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [MAPREDUCE-7305](https://issues.apache.org/jira/browse/MAPREDUCE-7305) | [JDK 11] TestMRJobsWithProfiler fails |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [YARN-10396](https://issues.apache.org/jira/browse/YARN-10396) | Max applications calculation per queue disregards queue level settings in absolute mode |  Major | capacity scheduler | Benjamin Teke | Benjamin Teke |
| [HADOOP-17390](https://issues.apache.org/jira/browse/HADOOP-17390) | Skip license check on lz4 code files |  Major | build | Zhihua Deng | Zhihua Deng |
| [MAPREDUCE-7307](https://issues.apache.org/jira/browse/MAPREDUCE-7307) | Potential thread leak in LocatedFileStatusFetcher |  Major | job submission | Zhihua Deng | Zhihua Deng |
| [HADOOP-17346](https://issues.apache.org/jira/browse/HADOOP-17346) | Fair call queue is defeated by abusive service principals |  Major | common, ipc | Ahmed Hussein | Ahmed Hussein |
| [YARN-10470](https://issues.apache.org/jira/browse/YARN-10470) | When building new web ui with root user, the bower install should support it. |  Major | build, yarn-ui-v2 | Qi Zhu | Qi Zhu |
| [HADOOP-17398](https://issues.apache.org/jira/browse/HADOOP-17398) | Skipping network I/O in S3A getFileStatus(/) breaks some tests |  Major | fs/s3, test | Mukund Thakur | Mukund Thakur |
| [HDFS-15698](https://issues.apache.org/jira/browse/HDFS-15698) | Fix the typo of dfshealth.html after HDFS-15358 |  Trivial | namenode | Hui Fei | Hui Fei |
| [YARN-10498](https://issues.apache.org/jira/browse/YARN-10498) | Fix Yarn CapacityScheduler Markdown document |  Trivial | documentation | zhaoshengjie | zhaoshengjie |
| [HADOOP-17399](https://issues.apache.org/jira/browse/HADOOP-17399) | lz4 sources missing for native Visual Studio project |  Major | native | Gautham Banasandra | Gautham Banasandra |
| [HDFS-15695](https://issues.apache.org/jira/browse/HDFS-15695) | NN should not let the balancer run in safemode |  Major | namenode | Ahmed Hussein | Ahmed Hussein |
| [YARN-10511](https://issues.apache.org/jira/browse/YARN-10511) | Update yarn.nodemanager.env-whitelist value in docs |  Minor | documentation | Andrea Scarpino | Andrea Scarpino |
| [HADOOP-16080](https://issues.apache.org/jira/browse/HADOOP-16080) | hadoop-aws does not work with hadoop-client-api |  Major | fs/s3 | Keith Turner | Chao Sun |
| [HDFS-15660](https://issues.apache.org/jira/browse/HDFS-15660) | StorageTypeProto is not compatiable between 3.x and 2.6 |  Major | . | Ryan Wu | Ryan Wu |
| [HDFS-15707](https://issues.apache.org/jira/browse/HDFS-15707) | NNTop counts don't add up as expected |  Major | hdfs, metrics, namenode | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15709](https://issues.apache.org/jira/browse/HDFS-15709) | EC: Socket file descriptor leak in StripedBlockChecksumReconstructor |  Major | datanode, ec, erasure-coding | Yushi Hayasaka | Yushi Hayasaka |
| [YARN-10495](https://issues.apache.org/jira/browse/YARN-10495) | make the rpath of container-executor configurable |  Major | yarn | angerszhu | angerszhu |
| [HDFS-15240](https://issues.apache.org/jira/browse/HDFS-15240) | Erasure Coding: dirty buffer causes reconstruction block error |  Blocker | datanode, erasure-coding | HuangTao | HuangTao |
| [YARN-10491](https://issues.apache.org/jira/browse/YARN-10491) | Fix deprecation warnings in SLSWebApp.java |  Minor | build | Akira Ajisaka | Ankit Kumar |
| [HADOOP-13571](https://issues.apache.org/jira/browse/HADOOP-13571) | ServerSocketUtil.getPort() should use loopback address, not 0.0.0.0 |  Major | . | Eric Badger | Eric Badger |
| [HDFS-15725](https://issues.apache.org/jira/browse/HDFS-15725) | Lease Recovery never completes for a committed block which the DNs never finalize |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-15170](https://issues.apache.org/jira/browse/HDFS-15170) | EC: Block gets marked as CORRUPT in case of failover and pipeline recovery |  Critical | erasure-coding | Ayush Saxena | Ayush Saxena |
| [YARN-10536](https://issues.apache.org/jira/browse/YARN-10536) | Client in distributedShell swallows interrupt exceptions |  Major | client, distributed-shell | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15743](https://issues.apache.org/jira/browse/HDFS-15743) | Fix -Pdist build failure of hadoop-hdfs-native-client |  Major | . | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-10334](https://issues.apache.org/jira/browse/YARN-10334) | TestDistributedShell leaks resources on timeout/failure |  Major | distributed-shell, test, yarn | Ahmed Hussein | Ahmed Hussein |
| [YARN-10558](https://issues.apache.org/jira/browse/YARN-10558) | Fix failure of TestDistributedShell#testDSShellWithOpportunisticContainers |  Minor | test | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-15719](https://issues.apache.org/jira/browse/HDFS-15719) | [Hadoop 3] Both NameNodes can crash simultaneously due to the short JN socket timeout |  Critical | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-10560](https://issues.apache.org/jira/browse/YARN-10560) | Upgrade node.js to 10.23.1 and yarn to 1.22.5 in Web UI v2 |  Major | webapp, yarn-ui-v2 | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17444](https://issues.apache.org/jira/browse/HADOOP-17444) | ADLFS: Update SDK version from 2.3.6 to 2.3.9 |  Minor | fs/adl | Bilahari T H | Bilahari T H |
| [YARN-10528](https://issues.apache.org/jira/browse/YARN-10528) | maxAMShare should only be accepted for leaf queues, not parent queues |  Major | . | Siddharth Ahuja | Siddharth Ahuja |
| [HADOOP-17438](https://issues.apache.org/jira/browse/HADOOP-17438) | Increase docker memory limit in Jenkins |  Major | build, scripts, test, yetus | Ahmed Hussein | Ahmed Hussein |
| [MAPREDUCE-7310](https://issues.apache.org/jira/browse/MAPREDUCE-7310) | Clear the fileMap in JHEventHandlerForSigtermTest |  Minor | test | Zhengxi Li | Zhengxi Li |
| [HADOOP-16947](https://issues.apache.org/jira/browse/HADOOP-16947) | Stale record should be remove when MutableRollingAverages generating aggregate data. |  Major | . | Haibin Huang | Haibin Huang |
| [YARN-10515](https://issues.apache.org/jira/browse/YARN-10515) | Fix flaky test TestCapacitySchedulerAutoQueueCreation.testDynamicAutoQueueCreationWithTags |  Major | test | Peter Bacsko | Peter Bacsko |
| [HADOOP-17224](https://issues.apache.org/jira/browse/HADOOP-17224) | Install Intel ISA-L library in Dockerfile |  Blocker | . | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-15632](https://issues.apache.org/jira/browse/HDFS-15632) | AbstractContractDeleteTest should set recursive parameter to true for recursive test cases. |  Major | . | Konstantin Shvachko | Anton Kutuzov |
| [HADOOP-17258](https://issues.apache.org/jira/browse/HADOOP-17258) | MagicS3GuardCommitter fails with \`pendingset\` already exists |  Major | fs/s3 | Dongjoon Hyun | Dongjoon Hyun |
| [HDFS-15661](https://issues.apache.org/jira/browse/HDFS-15661) | The DeadNodeDetector shouldn't be shared by different DFSClients. |  Major | . | Jinglun | Jinglun |
| [HDFS-10498](https://issues.apache.org/jira/browse/HDFS-10498) | Intermittent test failure org.apache.hadoop.hdfs.server.namenode.snapshot.TestSnapshotFileLength.testSnapshotfileLength |  Major | hdfs, snapshots | Hanisha Koneru | Jim Brennan |
| [HADOOP-17506](https://issues.apache.org/jira/browse/HADOOP-17506) | Fix typo in BUILDING.txt |  Trivial | documentation | Gautham Banasandra | Gautham Banasandra |
| [HDFS-15791](https://issues.apache.org/jira/browse/HDFS-15791) | Possible Resource Leak in FSImageFormatProtobuf |  Major | namenode | Narges Shadab | Narges Shadab |
| [HDFS-15795](https://issues.apache.org/jira/browse/HDFS-15795) | EC: Wrong checksum when reconstruction was failed by exception |  Major | datanode, ec, erasure-coding | Yushi Hayasaka | Yushi Hayasaka |
| [HDFS-15779](https://issues.apache.org/jira/browse/HDFS-15779) | EC: fix NPE caused by StripedWriter.clearBuffers during reconstruct block |  Major | . | Hongbing Wang | Hongbing Wang |
| [HADOOP-17217](https://issues.apache.org/jira/browse/HADOOP-17217) | S3A FileSystem does not correctly delete directories with fake entries |  Major | fs/s3 | Kaya Kupferschmidt |  |
| [HDFS-15798](https://issues.apache.org/jira/browse/HDFS-15798) | EC: Reconstruct task failed, and It would be XmitsInProgress of DN has negative number |  Major | . | Haiyang Hu | Haiyang Hu |
| [YARN-10607](https://issues.apache.org/jira/browse/YARN-10607) | User environment is unable to prepend PATH when mapreduce.admin.user.env also sets PATH |  Major | . | Eric Badger | Eric Badger |
| [HDFS-15792](https://issues.apache.org/jira/browse/HDFS-15792) | ClasscastException while loading FSImage |  Major | nn | Renukaprasad C | Renukaprasad C |
| [HADOOP-17516](https://issues.apache.org/jira/browse/HADOOP-17516) | Upgrade ant to 1.10.9 |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [YARN-10500](https://issues.apache.org/jira/browse/YARN-10500) | TestDelegationTokenRenewer fails intermittently |  Major | test | Akira Ajisaka | Masatake Iwasaki |
| [HDFS-15806](https://issues.apache.org/jira/browse/HDFS-15806) | DeadNodeDetector should close all the threads when it is closed. |  Major | . | Jinglun | Jinglun |
| [HADOOP-17534](https://issues.apache.org/jira/browse/HADOOP-17534) | Upgrade Jackson databind to 2.10.5.1 |  Major | build | Adam Roberts | Akira Ajisaka |
| [MAPREDUCE-7323](https://issues.apache.org/jira/browse/MAPREDUCE-7323) | Remove job\_history\_summary.py |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [YARN-10647](https://issues.apache.org/jira/browse/YARN-10647) | Fix TestRMNodeLabelsManager failed after YARN-10501. |  Major | . | Qi Zhu | Qi Zhu |
| [HADOOP-17528](https://issues.apache.org/jira/browse/HADOOP-17528) | Not closing an SFTP File System instance prevents JVM from exiting. |  Major | . | Mikhail Pryakhin | Mikhail Pryakhin |
| [HADOOP-17510](https://issues.apache.org/jira/browse/HADOOP-17510) | Hadoop prints sensitive Cookie information. |  Major | . | Renukaprasad C | Renukaprasad C |
| [HDFS-15422](https://issues.apache.org/jira/browse/HDFS-15422) | Reported IBR is partially replaced with stored info when queuing. |  Critical | namenode | Kihwal Lee | Stephen O'Donnell |
| [YARN-10651](https://issues.apache.org/jira/browse/YARN-10651) | CapacityScheduler crashed with NPE in AbstractYarnScheduler.updateNodeResource() |  Major | . | Haibo Chen | Haibo Chen |
| [MAPREDUCE-7320](https://issues.apache.org/jira/browse/MAPREDUCE-7320) | ClusterMapReduceTestCase does not clean directories |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [HDFS-14013](https://issues.apache.org/jira/browse/HDFS-14013) | Skip any credentials stored in HDFS when starting ZKFC |  Major | hdfs | Krzysztof Adamski | Stephen O'Donnell |
| [HDFS-15849](https://issues.apache.org/jira/browse/HDFS-15849) | ExpiredHeartbeats metric should be of Type.COUNTER |  Major | metrics | Konstantin Shvachko | Qi Zhu |
| [YARN-10649](https://issues.apache.org/jira/browse/YARN-10649) | Fix RMNodeImpl.updateExistContainers leak |  Major | resourcemanager | Max  Xie | Max  Xie |
| [YARN-10672](https://issues.apache.org/jira/browse/YARN-10672) | All testcases in TestReservations are flaky |  Major | . | Szilard Nemeth | Szilard Nemeth |
| [HADOOP-17557](https://issues.apache.org/jira/browse/HADOOP-17557) | skip-dir option is not processed by Yetus |  Major | build, precommit, yetus | Ahmed Hussein | Ahmed Hussein |
| [YARN-10671](https://issues.apache.org/jira/browse/YARN-10671) | Fix Typo in TestSchedulingRequestContainerAllocation |  Minor | . | D M Murali Krishna Reddy | D M Murali Krishna Reddy |
| [HDFS-15875](https://issues.apache.org/jira/browse/HDFS-15875) | Check whether file is being truncated before truncate |  Major | . | Hui Fei | Hui Fei |
| [HADOOP-17582](https://issues.apache.org/jira/browse/HADOOP-17582) | Replace GitHub App Token with GitHub OAuth token |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [YARN-10687](https://issues.apache.org/jira/browse/YARN-10687) | Add option to disable/enable free disk space checking and percentage checking for full and not-full disks |  Major | nodemanager | Qi Zhu | Qi Zhu |
| [HADOOP-17586](https://issues.apache.org/jira/browse/HADOOP-17586) | Upgrade org.codehaus.woodstox:stax2-api to 4.2.1 |  Major | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-17585](https://issues.apache.org/jira/browse/HADOOP-17585) | Correct timestamp format in the docs for the touch command |  Major | . | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-15809](https://issues.apache.org/jira/browse/HDFS-15809) | DeadNodeDetector doesn't remove live nodes from dead node set. |  Major | . | Jinglun | Jinglun |
| [HADOOP-17532](https://issues.apache.org/jira/browse/HADOOP-17532) | Yarn Job execution get failed when LZ4 Compression Codec is used |  Major | common | Bhavik Patel | Bhavik Patel |
| [YARN-10588](https://issues.apache.org/jira/browse/YARN-10588) | Percentage of queue and cluster is zero in WebUI |  Major | . | Bilwa S T | Bilwa S T |
| [MAPREDUCE-7322](https://issues.apache.org/jira/browse/MAPREDUCE-7322) | revisiting TestMRIntermediateDataEncryption |  Major | job submission, security, test | Ahmed Hussein | Ahmed Hussein |
| [YARN-10703](https://issues.apache.org/jira/browse/YARN-10703) | Fix potential null pointer error of gpuNodeResourceUpdateHandler in NodeResourceMonitorImpl. |  Major | . | Qi Zhu | Qi Zhu |
| [HDFS-15868](https://issues.apache.org/jira/browse/HDFS-15868) | Possible Resource Leak in EditLogFileOutputStream |  Major | . | Narges Shadab | Narges Shadab |
| [HADOOP-17592](https://issues.apache.org/jira/browse/HADOOP-17592) | Fix the wrong CIDR range example in Proxy User documentation |  Minor | documentation | Kwangsun Noh | Kwangsun Noh |
| [YARN-10706](https://issues.apache.org/jira/browse/YARN-10706) | Upgrade com.github.eirslett:frontend-maven-plugin to 1.11.2 |  Major | buid | Mingliang Liu | Mingliang Liu |
| [MAPREDUCE-7325](https://issues.apache.org/jira/browse/MAPREDUCE-7325) | Intermediate data encryption is broken in LocalJobRunner |  Major | job submission, security | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15908](https://issues.apache.org/jira/browse/HDFS-15908) | Possible Resource Leak in org.apache.hadoop.hdfs.qjournal.server.Journal |  Major | . | Narges Shadab | Narges Shadab |
| [HDFS-15910](https://issues.apache.org/jira/browse/HDFS-15910) | Replace bzero with explicit\_bzero for better safety |  Critical | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [YARN-10697](https://issues.apache.org/jira/browse/YARN-10697) | Resources are displayed in bytes in UI for schedulers other than capacity |  Major | . | Bilwa S T | Bilwa S T |
| [HADOOP-17602](https://issues.apache.org/jira/browse/HADOOP-17602) | Upgrade JUnit to 4.13.1 |  Major | build, security, test | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15900](https://issues.apache.org/jira/browse/HDFS-15900) | RBF: empty blockpool id on dfsrouter caused by UNAVAILABLE NameNode |  Major | rbf | Harunobu Daikoku | Harunobu Daikoku |
| [YARN-10501](https://issues.apache.org/jira/browse/YARN-10501) | Can't remove all node labels after add node label without nodemanager port |  Critical | yarn | caozhiqiang | caozhiqiang |
| [YARN-10437](https://issues.apache.org/jira/browse/YARN-10437) | Destroy yarn service if any YarnException occurs during submitApp |  Minor | yarn-native-services | D M Murali Krishna Reddy | D M Murali Krishna Reddy |
| [YARN-10439](https://issues.apache.org/jira/browse/YARN-10439) | Yarn Service AM listens on all IP's on the machine |  Minor | security, yarn-native-services | D M Murali Krishna Reddy | D M Murali Krishna Reddy |
| [YARN-10441](https://issues.apache.org/jira/browse/YARN-10441) | Add support for hadoop.http.rmwebapp.scheduler.page.class |  Major | scheduler | D M Murali Krishna Reddy | D M Murali Krishna Reddy |
| [YARN-10466](https://issues.apache.org/jira/browse/YARN-10466) | Fix NullPointerException in  yarn-services Component.java |  Minor | . | D M Murali Krishna Reddy | D M Murali Krishna Reddy |
| [YARN-10716](https://issues.apache.org/jira/browse/YARN-10716) | Fix typo in ContainerRuntime |  Trivial | documentation | Wanqiang Ji | xishuhai |
| [HDFS-15494](https://issues.apache.org/jira/browse/HDFS-15494) | TestReplicaCachingGetSpaceUsed#testReplicaCachingGetSpaceUsedByRBWReplica Fails on Windows |  Major | . | Ravuri Sushma sree | Ravuri Sushma sree |
| [HADOOP-17610](https://issues.apache.org/jira/browse/HADOOP-17610) | DelegationTokenAuthenticator prints token information |  Major | . | Ravuri Sushma sree | Ravuri Sushma sree |
| [HADOOP-17587](https://issues.apache.org/jira/browse/HADOOP-17587) | Kinit with keytab should not display the keytab file's full path in any logs |  Major | . | Ravuri Sushma sree | Ravuri Sushma sree |
| [HDFS-15950](https://issues.apache.org/jira/browse/HDFS-15950) | Remove unused hdfs.proto import |  Major | hdfs-client | Gautham Banasandra | Gautham Banasandra |
| [HDFS-15940](https://issues.apache.org/jira/browse/HDFS-15940) | Some tests in TestBlockRecovery are consistently failing |  Major | . | Viraj Jasani | Viraj Jasani |
| [HDFS-15949](https://issues.apache.org/jira/browse/HDFS-15949) | Fix integer overflow |  Major | libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-17621](https://issues.apache.org/jira/browse/HADOOP-17621) | hadoop-auth to remove jetty-server dependency |  Major | auth | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-15948](https://issues.apache.org/jira/browse/HDFS-15948) | Fix test4tests for libhdfspp |  Critical | build, libhdfs++ | Gautham Banasandra | Gautham Banasandra |
| [HADOOP-17617](https://issues.apache.org/jira/browse/HADOOP-17617) | Incorrect representation of RESPONSE for Get Key Version in KMS index.md.vm file |  Major | . | Ravuri Sushma sree | Ravuri Sushma sree |
| [MAPREDUCE-7329](https://issues.apache.org/jira/browse/MAPREDUCE-7329) | HadoopPipes task may fail when linux kernel version change from 3.x to 4.x |  Major | pipes | chaoli | chaoli |
| [HADOOP-17608](https://issues.apache.org/jira/browse/HADOOP-17608) | Fix TestKMS failure |  Major | kms | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15963](https://issues.apache.org/jira/browse/HDFS-15963) | Unreleased volume references cause an infinite loop |  Major | datanode | Shuyan Zhang | Shuyan Zhang |
| [YARN-10460](https://issues.apache.org/jira/browse/YARN-10460) | Upgrading to JUnit 4.13 causes tests in TestNodeStatusUpdater to fail |  Major | nodemanager, test | Peter Bacsko | Peter Bacsko |
| [HADOOP-17641](https://issues.apache.org/jira/browse/HADOOP-17641) | ITestWasbUriAndConfiguration.testCanonicalServiceName() failing now mockaccount exists |  Minor | fs/azure, test | Steve Loughran | Steve Loughran |
| [HDFS-15974](https://issues.apache.org/jira/browse/HDFS-15974) | RBF: Unable to display the datanode UI of the router |  Major | rbf, ui | Xiangyi Zhu | Xiangyi Zhu |
| [HADOOP-17655](https://issues.apache.org/jira/browse/HADOOP-17655) | Upgrade Jetty to 9.4.40 |  Blocker | . | Akira Ajisaka | Akira Ajisaka |
| [YARN-10749](https://issues.apache.org/jira/browse/YARN-10749) | Can't remove all node labels after add node label without nodemanager port, broken by YARN-10647 |  Major | . | D M Murali Krishna Reddy | D M Murali Krishna Reddy |
| [HDFS-15566](https://issues.apache.org/jira/browse/HDFS-15566) | NN restart fails after RollingUpgrade from  3.1.3/3.2.1 to 3.3.0 |  Blocker | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-15621](https://issues.apache.org/jira/browse/HDFS-15621) | Datanode DirectoryScanner uses excessive memory |  Major | datanode | Stephen O'Donnell | Stephen O'Donnell |
| [YARN-10752](https://issues.apache.org/jira/browse/YARN-10752) | Shaded guava not found when compiling with profile hbase2.0 |  Blocker | timelineserver | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-15865](https://issues.apache.org/jira/browse/HDFS-15865) | Interrupt DataStreamer thread |  Minor | datanode | Karthik Palanisamy |  |
| [HDFS-15810](https://issues.apache.org/jira/browse/HDFS-15810) | RBF: RBFMetrics's TotalCapacity out of bounds |  Major | . | Xiaoxing Wei | Fengnan Li |
| [HADOOP-17657](https://issues.apache.org/jira/browse/HADOOP-17657) | SequeneFile.Writer should implement StreamCapabilities |  Major | . | Kishen Das | Kishen Das |
| [YARN-10756](https://issues.apache.org/jira/browse/YARN-10756) | Remove additional junit 4.11 dependency from javadoc |  Major | build, test, timelineservice | ANANDA G B | Akira Ajisaka |
| [HADOOP-17375](https://issues.apache.org/jira/browse/HADOOP-17375) | Fix the error of TestDynamometerInfra |  Major | test | Akira Ajisaka | Takanobu Asanuma |
| [HDFS-16001](https://issues.apache.org/jira/browse/HDFS-16001) | TestOfflineEditsViewer.testStored() fails reading negative value of FSEditLogOpCodes |  Blocker | hdfs | Konstantin Shvachko | Akira Ajisaka |
| [HADOOP-17142](https://issues.apache.org/jira/browse/HADOOP-17142) | Fix outdated properties of journal node when perform rollback |  Minor | . | Deegue |  |
| [HADOOP-17107](https://issues.apache.org/jira/browse/HADOOP-17107) | hadoop-azure parallel tests not working on recent JDKs |  Major | build, fs/azure | Steve Loughran | Steve Loughran |
| [YARN-10555](https://issues.apache.org/jira/browse/YARN-10555) |  Missing access check before getAppAttempts |  Critical | webapp | lujie | lujie |
| [HADOOP-17703](https://issues.apache.org/jira/browse/HADOOP-17703) | checkcompatibility.py errors out when specifying annotations |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-16027](https://issues.apache.org/jira/browse/HDFS-16027) |  HDFS-15245 breaks source code compatibility between 3.3.0 and 3.3.1. |  Blocker | journal-node, ui | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-10725](https://issues.apache.org/jira/browse/YARN-10725) | Backport YARN-10120 to branch-3.3 |  Major | . | Bilwa S T | Bilwa S T |
| [YARN-10701](https://issues.apache.org/jira/browse/YARN-10701) | The yarn.resource-types should support multi types without trimmed. |  Major | . | Qi Zhu | Qi Zhu |
| [HADOOP-17718](https://issues.apache.org/jira/browse/HADOOP-17718) | Explicitly set locale in the Dockerfile |  Blocker | build | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [MAPREDUCE-7348](https://issues.apache.org/jira/browse/MAPREDUCE-7348) | TestFrameworkUploader#testNativeIO fails |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17723](https://issues.apache.org/jira/browse/HADOOP-17723) | [build] fix the Dockerfile for ARM |  Blocker | build | Wei-Chiu Chuang | Wei-Chiu Chuang |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-7288](https://issues.apache.org/jira/browse/MAPREDUCE-7288) | Fix TestLongLong#testRightShift |  Minor | . | Wanqiang Ji | Wanqiang Ji |
| [HDFS-15514](https://issues.apache.org/jira/browse/HDFS-15514) | Remove useless dfs.webhdfs.enabled |  Minor | test | Hui Fei | Hui Fei |
| [HADOOP-17205](https://issues.apache.org/jira/browse/HADOOP-17205) | Move personality file from Yetus to Hadoop repository |  Major | test, yetus | Chao Sun | Chao Sun |
| [HDFS-15564](https://issues.apache.org/jira/browse/HDFS-15564) | Add Test annotation for TestPersistBlocks#testRestartDfsWithSync |  Minor | hdfs | Hui Fei | Hui Fei |
| [YARN-9333](https://issues.apache.org/jira/browse/YARN-9333) | TestFairSchedulerPreemption.testRelaxLocalityPreemptionWithNoLessAMInRemainingNodes fails intermittently |  Major | yarn | Prabhu Joseph | Peter Bacsko |
| [HDFS-15690](https://issues.apache.org/jira/browse/HDFS-15690) | Add lz4-java as hadoop-hdfs test dependency |  Major | . | L. C. Hsieh | L. C. Hsieh |
| [HADOOP-17459](https://issues.apache.org/jira/browse/HADOOP-17459) | ADL Gen1: Fix the test case failures which are failing after the contract test update in hadoop-common |  Minor | fs/adl | Bilahari T H | Bilahari T H |
| [HDFS-15898](https://issues.apache.org/jira/browse/HDFS-15898) | Test case TestOfflineImageViewer fails |  Minor | . | Hui Fei | Hui Fei |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-16857](https://issues.apache.org/jira/browse/HADOOP-16857) | ABFS: Optimize HttpRequest retry triggers |  Major | fs/azure | Sneha Vijayarajan | Sneha Vijayarajan |
| [HADOOP-17002](https://issues.apache.org/jira/browse/HADOOP-17002) | ABFS: Avoid storage calls to check if the account is HNS enabled or not |  Minor | fs/azure | Bilahari T H | Bilahari T H |
| [YARN-10215](https://issues.apache.org/jira/browse/YARN-10215) | Endpoint for obtaining direct URL for the logs |  Major | yarn | Adam Antal | Andras Gyori |
| [HDFS-14353](https://issues.apache.org/jira/browse/HDFS-14353) | Erasure Coding: metrics xmitsInProgress become to negative. |  Major | datanode, erasure-coding | Baolong Mao | Baolong Mao |
| [HDFS-15305](https://issues.apache.org/jira/browse/HDFS-15305) | Extend ViewFS and provide ViewFSOverloadScheme implementation with scheme configurable. |  Major | fs, hadoop-client, hdfs-client, viewfs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [YARN-10259](https://issues.apache.org/jira/browse/YARN-10259) | Reserved Containers not allocated from available space of other nodes in CandidateNodeSet in MultiNodePlacement |  Major | capacityscheduler | Prabhu Joseph | Prabhu Joseph |
| [HDFS-15306](https://issues.apache.org/jira/browse/HDFS-15306) | Make mount-table to read from central place ( Let's say from HDFS) |  Major | configuration, hadoop-client | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-16756](https://issues.apache.org/jira/browse/HADOOP-16756) | distcp -update to S3A; abfs, etc always overwrites due to block size mismatch |  Major | fs/s3, tools/distcp | Daisuke Kobayashi | Steve Loughran |
| [HDFS-15322](https://issues.apache.org/jira/browse/HDFS-15322) | Make NflyFS to work when ViewFsOverloadScheme's scheme and target uris schemes are same. |  Major | fs, nflyFs, viewfs, viewfsOverloadScheme | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [YARN-10108](https://issues.apache.org/jira/browse/YARN-10108) | FS-CS converter: nestedUserQueue with default rule results in invalid queue mapping |  Major | . | Prabhu Joseph | Gergely Pollák |
| [HADOOP-16852](https://issues.apache.org/jira/browse/HADOOP-16852) | ABFS: Send error back to client for Read Ahead request failure |  Major | fs/azure | Sneha Vijayarajan | Sneha Vijayarajan |
| [HADOOP-17053](https://issues.apache.org/jira/browse/HADOOP-17053) | ABFS: FS initialize fails for incompatible account-agnostic Token Provider setting |  Major | fs/azure | Sneha Vijayarajan | Sneha Vijayarajan |
| [HDFS-15321](https://issues.apache.org/jira/browse/HDFS-15321) | Make DFSAdmin tool to work with ViewFSOverloadScheme |  Major | dfsadmin, fs, viewfs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-16568](https://issues.apache.org/jira/browse/HADOOP-16568) | S3A FullCredentialsTokenBinding fails if local credentials are unset |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-10284](https://issues.apache.org/jira/browse/YARN-10284) | Add lazy initialization of LogAggregationFileControllerFactory in LogServlet |  Major | log-aggregation, yarn | Adam Antal | Adam Antal |
| [HDFS-15330](https://issues.apache.org/jira/browse/HDFS-15330) | Document the ViewFSOverloadScheme details in ViewFS guide |  Major | viewfs, viewfsOverloadScheme | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-15389](https://issues.apache.org/jira/browse/HDFS-15389) | DFSAdmin should close filesystem and dfsadmin -setBalancerBandwidth should work with ViewFSOverloadScheme |  Major | dfsadmin, viewfsOverloadScheme | Ayush Saxena | Ayush Saxena |
| [HDFS-15394](https://issues.apache.org/jira/browse/HDFS-15394) | Add all available fs.viewfs.overload.scheme.target.\<scheme\>.impl classes in core-default.xml bydefault. |  Major | configuration, viewfs, viewfsOverloadScheme | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-15387](https://issues.apache.org/jira/browse/HDFS-15387) | FSUsage$DF should consider ViewFSOverloadScheme in processPath |  Minor | viewfs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [YARN-10292](https://issues.apache.org/jira/browse/YARN-10292) | FS-CS converter: add an option to enable asynchronous scheduling in CapacityScheduler |  Major | fairscheduler | Benjamin Teke | Benjamin Teke |
| [HADOOP-17004](https://issues.apache.org/jira/browse/HADOOP-17004) | ABFS: Improve the ABFS driver documentation |  Minor | fs/azure | Bilahari T H | Bilahari T H |
| [HDFS-15418](https://issues.apache.org/jira/browse/HDFS-15418) | ViewFileSystemOverloadScheme should represent mount links as non symlinks |  Major | . | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [YARN-9930](https://issues.apache.org/jira/browse/YARN-9930) | Support max running app logic for CapacityScheduler |  Major | capacity scheduler, capacityscheduler | zhoukang | Peter Bacsko |
| [HDFS-15427](https://issues.apache.org/jira/browse/HDFS-15427) | Merged ListStatus with Fallback target filesystem and InternalDirViewFS. |  Major | viewfs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [YARN-10316](https://issues.apache.org/jira/browse/YARN-10316) | FS-CS converter: convert maxAppsDefault, maxRunningApps settings |  Major | . | Peter Bacsko | Peter Bacsko |
| [HADOOP-17054](https://issues.apache.org/jira/browse/HADOOP-17054) | ABFS: Fix idempotency test failures when SharedKey is set as AuthType |  Major | fs/azure | Sneha Vijayarajan | Sneha Vijayarajan |
| [HADOOP-17050](https://issues.apache.org/jira/browse/HADOOP-17050) | S3A to support additional token issuers |  Minor | fs/s3 | Gabor Bota | Steve Loughran |
| [HADOOP-17015](https://issues.apache.org/jira/browse/HADOOP-17015) | ABFS: Make PUT and POST operations idempotent |  Major | fs/azure | Sneha Vijayarajan | Sneha Vijayarajan |
| [HDFS-15429](https://issues.apache.org/jira/browse/HDFS-15429) | mkdirs should work when parent dir is internalDir and fallback configured. |  Major | . | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-15436](https://issues.apache.org/jira/browse/HDFS-15436) | Default mount table name used by ViewFileSystem should be configurable |  Major | viewfs, viewfsOverloadScheme | Virajith Jalaparti | Virajith Jalaparti |
| [HADOOP-16798](https://issues.apache.org/jira/browse/HADOOP-16798) | job commit failure in S3A MR magic committer test |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-10325](https://issues.apache.org/jira/browse/YARN-10325) | Document max-parallel-apps for Capacity Scheduler |  Major | capacity scheduler, capacityscheduler | Peter Bacsko | Peter Bacsko |
| [HADOOP-16961](https://issues.apache.org/jira/browse/HADOOP-16961) | ABFS: Adding metrics to AbfsInputStream (AbfsInputStreamStatistics) |  Major | fs/azure | Gabor Bota | Mehakmeet Singh |
| [HADOOP-17086](https://issues.apache.org/jira/browse/HADOOP-17086) | ABFS: Fix the parsing errors in ABFS Driver with creation Time (being returned in ListPath) |  Major | fs/azure | Ishani | Bilahari T H |
| [HDFS-15430](https://issues.apache.org/jira/browse/HDFS-15430) | create should work when parent dir is internalDir and fallback configured. |  Major | . | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-15450](https://issues.apache.org/jira/browse/HDFS-15450) | Fix NN trash emptier to work if ViewFSOveroadScheme enabled |  Major | namenode, viewfsOverloadScheme | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-17111](https://issues.apache.org/jira/browse/HADOOP-17111) | Replace Guava Optional with Java8+ Optional |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15449](https://issues.apache.org/jira/browse/HDFS-15449) | Optionally ignore port number in mount-table name when picking from initialized uri |  Major | . | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-17058](https://issues.apache.org/jira/browse/HADOOP-17058) | Support for Appendblob in abfs driver |  Major | fs/azure | Ishani | Ishani |
| [HDFS-15462](https://issues.apache.org/jira/browse/HDFS-15462) | Add fs.viewfs.overload.scheme.target.ofs.impl to core-default.xml |  Major | configuration, viewfs, viewfsOverloadScheme | Siyao Meng | Siyao Meng |
| [HDFS-15464](https://issues.apache.org/jira/browse/HDFS-15464) | ViewFsOverloadScheme should work when -fs option pointing to remote cluster without mount links |  Major | viewfsOverloadScheme | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-17105](https://issues.apache.org/jira/browse/HADOOP-17105) | S3AFS globStatus attempts to resolve symlinks |  Minor | fs/s3 | Jimmy Zuber | Jimmy Zuber |
| [HADOOP-17022](https://issues.apache.org/jira/browse/HADOOP-17022) | Tune S3A listFiles() api. |  Major | fs/s3 | Mukund Thakur | Mukund Thakur |
| [HADOOP-17101](https://issues.apache.org/jira/browse/HADOOP-17101) | Replace Guava Function with Java8+ Function |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17099](https://issues.apache.org/jira/browse/HADOOP-17099) | Replace Guava Predicate with Java8+ Predicate |  Minor | . | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-16682](https://issues.apache.org/jira/browse/HADOOP-16682) | Remove unnecessary ABFS toString() invocations |  Minor | fs/azure | Jeetesh Mangwani | Bilahari T H |
| [HADOOP-17136](https://issues.apache.org/jira/browse/HADOOP-17136) | ITestS3ADirectoryPerformance.testListOperations failing |  Minor | fs/s3, test | Mukund Thakur | Mukund Thakur |
| [HDFS-15478](https://issues.apache.org/jira/browse/HDFS-15478) | When Empty mount points, we are assigning fallback link to self. But it should not use full URI for target fs. |  Major | . | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-17100](https://issues.apache.org/jira/browse/HADOOP-17100) | Replace Guava Supplier with Java8+ Supplier in Hadoop |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17132](https://issues.apache.org/jira/browse/HADOOP-17132) | ABFS: Fix For Idempotency code |  Major | fs/azure | Sneha Vijayarajan | Sneha Vijayarajan |
| [HADOOP-17092](https://issues.apache.org/jira/browse/HADOOP-17092) | ABFS: Long waits and unintended retries when multiple threads try to fetch token using ClientCreds |  Major | fs/azure | Sneha Vijayarajan | Bilahari T H |
| [HADOOP-17131](https://issues.apache.org/jira/browse/HADOOP-17131) | Refactor S3A Listing code for better isolation |  Major | fs/s3 | Mukund Thakur | Mukund Thakur |
| [HADOOP-17137](https://issues.apache.org/jira/browse/HADOOP-17137) | ABFS: Tests ITestAbfsNetworkStatistics need to be config setting agnostic |  Minor | fs/azure, test | Sneha Vijayarajan | Bilahari T H |
| [HADOOP-17149](https://issues.apache.org/jira/browse/HADOOP-17149) | ABFS: Test failure: testFailedRequestWhenCredentialsNotCorrect fails when run with SharedKey |  Minor | fs/azure | Sneha Vijayarajan | Bilahari T H |
| [HADOOP-17163](https://issues.apache.org/jira/browse/HADOOP-17163) | ABFS: Add debug log for rename failures |  Major | fs/azure | Bilahari T H | Bilahari T H |
| [HDFS-15515](https://issues.apache.org/jira/browse/HDFS-15515) | mkdirs on fallback should throw IOE out instead of suppressing and returning false |  Major | . | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-13230](https://issues.apache.org/jira/browse/HADOOP-13230) | S3A to optionally retain directory markers |  Major | fs/s3 | Aaron Fabbri | Steve Loughran |
| [HADOOP-14124](https://issues.apache.org/jira/browse/HADOOP-14124) | S3AFileSystem silently deletes "fake" directories when writing a file. |  Minor | fs, fs/s3 | Joel Baranick |  |
| [HADOOP-16966](https://issues.apache.org/jira/browse/HADOOP-16966) | ABFS: Upgrade Store REST API Version to 2019-12-12 |  Major | fs/azure | Ishani | Sneha Vijayarajan |
| [HDFS-15533](https://issues.apache.org/jira/browse/HDFS-15533) | Provide DFS API compatible class(ViewDistributedFileSystem), but use ViewFileSystemOverloadScheme inside |  Major | dfs, viewfs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-17074](https://issues.apache.org/jira/browse/HADOOP-17074) | Optimise s3a Listing to be fully asynchronous. |  Major | fs/s3 | Mukund Thakur | Mukund Thakur |
| [HDFS-15529](https://issues.apache.org/jira/browse/HDFS-15529) | getChildFilesystems should include fallback fs as well |  Critical | viewfs, viewfsOverloadScheme | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-17167](https://issues.apache.org/jira/browse/HADOOP-17167) | ITestS3AEncryptionWithDefaultS3Settings fails if default bucket encryption != KMS |  Minor | fs/s3 | Steve Loughran | Mukund Thakur |
| [HADOOP-17227](https://issues.apache.org/jira/browse/HADOOP-17227) | improve s3guard markers command line tool |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-10332](https://issues.apache.org/jira/browse/YARN-10332) | RESOURCE\_UPDATE event was repeatedly registered in DECOMMISSIONING state |  Minor | resourcemanager | yehuanhuan | yehuanhuan |
| [HDFS-15558](https://issues.apache.org/jira/browse/HDFS-15558) | ViewDistributedFileSystem#recoverLease should call super.recoverLease when there are no mounts configured |  Major | . | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-17181](https://issues.apache.org/jira/browse/HADOOP-17181) | Handle transient stream read failures in FileSystem contract tests |  Minor | fs/s3 | Steve Loughran |  |
| [HDFS-15551](https://issues.apache.org/jira/browse/HDFS-15551) | Tiny Improve for DeadNode detector |  Minor | hdfs-client | dark\_num | imbajin |
| [HDFS-15555](https://issues.apache.org/jira/browse/HDFS-15555) | RBF: Refresh cacheNS when SocketException occurs |  Major | rbf | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15532](https://issues.apache.org/jira/browse/HDFS-15532) | listFiles on root/InternalDir will fail if fallback root has file |  Major | . | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-15578](https://issues.apache.org/jira/browse/HDFS-15578) | Fix the rename issues with fallback fs enabled |  Major | viewfs, viewfsOverloadScheme | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-15585](https://issues.apache.org/jira/browse/HDFS-15585) | ViewDFS#getDelegationToken should not throw UnsupportedOperationException. |  Major | . | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-17215](https://issues.apache.org/jira/browse/HADOOP-17215) | ABFS: Support for conditional overwrite |  Major | fs/azure | Sneha Vijayarajan | Sneha Vijayarajan |
| [HDFS-14811](https://issues.apache.org/jira/browse/HDFS-14811) | RBF: TestRouterRpc#testErasureCoding is flaky |  Major | rbf | Chen Zhang | Chen Zhang |
| [HADOOP-17023](https://issues.apache.org/jira/browse/HADOOP-17023) | Tune listStatus() api of s3a. |  Major | fs/s3 | Mukund Thakur | Mukund Thakur |
| [HADOOP-17279](https://issues.apache.org/jira/browse/HADOOP-17279) | ABFS: Test testNegativeScenariosForCreateOverwriteDisabled fails for non-HNS account |  Major | fs/azure | Sneha Vijayarajan | Sneha Vijayarajan |
| [HADOOP-17183](https://issues.apache.org/jira/browse/HADOOP-17183) | ABFS: Enable checkaccess API |  Major | fs/azure | Bilahari T H | Bilahari T H |
| [HDFS-15613](https://issues.apache.org/jira/browse/HDFS-15613) | RBF: Router FSCK fails after HDFS-14442 |  Major | rbf | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17281](https://issues.apache.org/jira/browse/HADOOP-17281) | Implement FileSystem.listStatusIterator() in S3AFileSystem |  Major | fs/s3 | Mukund Thakur | Mukund Thakur |
| [HADOOP-17293](https://issues.apache.org/jira/browse/HADOOP-17293) |  S3A to always probe S3 in S3A getFileStatus on non-auth paths |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-15620](https://issues.apache.org/jira/browse/HDFS-15620) | RBF: Fix test failures after HADOOP-17281 |  Major | rbf, test | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17261](https://issues.apache.org/jira/browse/HADOOP-17261) | s3a rename() now requires s3:deleteObjectVersion permission |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-16915](https://issues.apache.org/jira/browse/HADOOP-16915) | ABFS: Test failure ITestAzureBlobFileSystemRandomRead.testRandomReadPerformance |  Major | . | Bilahari T H | Bilahari T H |
| [HADOOP-17166](https://issues.apache.org/jira/browse/HADOOP-17166) | ABFS: configure output stream thread pool |  Minor | fs/azure | Bilahari T H | Bilahari T H |
| [HADOOP-17301](https://issues.apache.org/jira/browse/HADOOP-17301) | ABFS: read-ahead error reporting breaks buffer management |  Critical | fs/azure | Sneha Vijayarajan | Sneha Vijayarajan |
| [HADOOP-17288](https://issues.apache.org/jira/browse/HADOOP-17288) | Use shaded guava from thirdparty |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-15459](https://issues.apache.org/jira/browse/HDFS-15459) | TestBlockTokenWithDFSStriped fails intermittently |  Major | hdfs | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15461](https://issues.apache.org/jira/browse/HDFS-15461) | TestDFSClientRetries#testGetFileChecksum fails intermittently |  Major | dfsclient, test | Ahmed Hussein | Ahmed Hussein |
| [HDFS-9776](https://issues.apache.org/jira/browse/HDFS-9776) | TestHAAppend#testMultipleAppendsDuringCatchupTailing is flaky |  Major | . | Vinayakumar B | Ahmed Hussein |
| [HDFS-15657](https://issues.apache.org/jira/browse/HDFS-15657) | RBF: TestRouter#testNamenodeHeartBeatEnableDefault fails by BindException |  Major | rbf, test | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17305](https://issues.apache.org/jira/browse/HADOOP-17305) | ITestCustomSigner fails with gcs s3 compatible endpoint. |  Major | fs/s3 | Mukund Thakur | Mukund Thakur |
| [HDFS-15643](https://issues.apache.org/jira/browse/HDFS-15643) | EC: Fix checksum computation in case of native encoders |  Blocker | . | Ahmed Hussein | Ayush Saxena |
| [HADOOP-17344](https://issues.apache.org/jira/browse/HADOOP-17344) | Harmonize guava version and shade guava in yarn-csi |  Major | . | Wei-Chiu Chuang | Akira Ajisaka |
| [HADOOP-17376](https://issues.apache.org/jira/browse/HADOOP-17376) | ITestS3AContractRename failing against stricter tests |  Major | fs/s3, test | Steve Loughran | Attila Doroszlai |
| [HADOOP-17379](https://issues.apache.org/jira/browse/HADOOP-17379) | AbstractS3ATokenIdentifier to set issue date == now |  Major | fs/s3 | Steve Loughran | Jungtaek Lim |
| [HADOOP-17244](https://issues.apache.org/jira/browse/HADOOP-17244) | HADOOP-17244. S3A directory delete tombstones dir markers prematurely. |  Blocker | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-17388](https://issues.apache.org/jira/browse/HADOOP-17388) | AbstractS3ATokenIdentifier to issue date in UTC |  Major | . | Steve Loughran | Jungtaek Lim |
| [HADOOP-17343](https://issues.apache.org/jira/browse/HADOOP-17343) | Upgrade aws-java-sdk to 1.11.901 |  Minor | build, fs/s3 | Dongjoon Hyun | Steve Loughran |
| [HADOOP-17325](https://issues.apache.org/jira/browse/HADOOP-17325) | WASB: Test failures |  Major | fs/azure, test | Sneha Vijayarajan | Steve Loughran |
| [HADOOP-17323](https://issues.apache.org/jira/browse/HADOOP-17323) | s3a getFileStatus("/") to skip IO |  Minor | fs/s3 | Steve Loughran | Mukund Thakur |
| [HADOOP-17311](https://issues.apache.org/jira/browse/HADOOP-17311) | ABFS: Logs should redact SAS signature |  Major | fs/azure, security | Sneha Vijayarajan | Bilahari T H |
| [HADOOP-17313](https://issues.apache.org/jira/browse/HADOOP-17313) | FileSystem.get to support slow-to-instantiate FS clients |  Major | fs, fs/azure, fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-17394](https://issues.apache.org/jira/browse/HADOOP-17394) | [JDK 11] mvn package -Pdocs fails |  Major | build, documentation | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17396](https://issues.apache.org/jira/browse/HADOOP-17396) | ABFS: testRenameFileOverExistingFile Fails after Contract test update |  Major | fs/azure, test | Sneha Vijayarajan | Sneha Vijayarajan |
| [HADOOP-17318](https://issues.apache.org/jira/browse/HADOOP-17318) | S3A committer to support concurrent jobs with same app attempt ID & dest dir |  Minor | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-17385](https://issues.apache.org/jira/browse/HADOOP-17385) | ITestS3ADeleteCost.testDirMarkersFileCreation failure |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-15844](https://issues.apache.org/jira/browse/HADOOP-15844) | tag S3GuardTool entry points as limitedPrivate("management-tools")/evolving |  Minor | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-17332](https://issues.apache.org/jira/browse/HADOOP-17332) | S3A marker tool mixes up -min and -max |  Trivial | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-17397](https://issues.apache.org/jira/browse/HADOOP-17397) | ABFS: SAS Test updates for version and permission update |  Major | fs/azure | Sneha Vijayarajan | Sneha Vijayarajan |
| [HDFS-15708](https://issues.apache.org/jira/browse/HDFS-15708) | TestURLConnectionFactory fails by NoClassDefFoundError in branch-3.3 and branch-3.2 |  Blocker | test | Akira Ajisaka | Chao Sun |
| [HDFS-15716](https://issues.apache.org/jira/browse/HDFS-15716) | TestUpgradeDomainBlockPlacementPolicy flaky |  Major | namenode, test | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17422](https://issues.apache.org/jira/browse/HADOOP-17422) | ABFS: Set default ListMaxResults to max server limit |  Major | fs/azure | Sumangala Patki | Thomas Marqardt |
| [HADOOP-17450](https://issues.apache.org/jira/browse/HADOOP-17450) | hadoop-common to add IOStatistics API |  Major | fs | Steve Loughran | Steve Loughran |
| [HADOOP-17271](https://issues.apache.org/jira/browse/HADOOP-17271) | S3A statistics to support IOStatistics |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-17347](https://issues.apache.org/jira/browse/HADOOP-17347) | ABFS: Optimise read for small files/tails of files |  Major | fs/azure | Bilahari T H | Bilahari T H |
| [HADOOP-17272](https://issues.apache.org/jira/browse/HADOOP-17272) | ABFS Streams to  support IOStatistics API |  Major | fs/azure | Steve Loughran | Mehakmeet Singh |
| [HADOOP-17451](https://issues.apache.org/jira/browse/HADOOP-17451) | Intermittent failure of S3A tests which make assertions on statistics/IOStatistics |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-15762](https://issues.apache.org/jira/browse/HDFS-15762) | TestMultipleNNPortQOP#testMultipleNNPortOverwriteDownStream fails intermittently |  Minor | . | Toshihiko Uchida | Toshihiko Uchida |
| [HDFS-15672](https://issues.apache.org/jira/browse/HDFS-15672) | TestBalancerWithMultipleNameNodes#testBalancingBlockpoolsWithBlockPoolPolicy fails on trunk |  Major | . | Ahmed Hussein | Masatake Iwasaki |
| [HADOOP-13845](https://issues.apache.org/jira/browse/HADOOP-13845) | s3a to instrument duration of HTTP calls |  Minor | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-17456](https://issues.apache.org/jira/browse/HADOOP-17456) | S3A ITestPartialRenamesDeletes.testPartialDirDelete[bulk-delete=true] failure |  Minor | fs/s3, test | Steve Loughran | Steve Loughran |
| [HADOOP-17455](https://issues.apache.org/jira/browse/HADOOP-17455) | [s3a] Intermittent failure of ITestS3ADeleteCost.testDeleteSingleFileInDir |  Major | fs/s3, test | Gabor Bota | Steve Loughran |
| [HADOOP-17433](https://issues.apache.org/jira/browse/HADOOP-17433) | Skipping network I/O in S3A getFileStatus(/) breaks ITestAssumeRole |  Minor | fs/s3, test | Steve Loughran | Steve Loughran |
| [HADOOP-17296](https://issues.apache.org/jira/browse/HADOOP-17296) | ABFS: Allow Random Reads to be of Buffer Size |  Major | fs/azure | Sneha Vijayarajan | Sneha Vijayarajan |
| [HADOOP-17413](https://issues.apache.org/jira/browse/HADOOP-17413) | ABFS: Release Elastic ByteBuffer pool memory at outputStream close |  Major | fs/azure | Sneha Vijayarajan | Sneha Vijayarajan |
| [HADOOP-17407](https://issues.apache.org/jira/browse/HADOOP-17407) | ABFS: Delete Idempotency handling can lead to NPE |  Major | fs/azure | Sneha Vijayarajan | Sneha Vijayarajan |
| [HADOOP-17404](https://issues.apache.org/jira/browse/HADOOP-17404) | ABFS: Piggyback flush on Append calls for short writes |  Major | fs/azure | Sneha Vijayarajan | Sneha Vijayarajan |
| [HADOOP-17480](https://issues.apache.org/jira/browse/HADOOP-17480) | S3A docs to state s3 is consistent, deprecate S3Guard |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-17414](https://issues.apache.org/jira/browse/HADOOP-17414) | Magic committer files don't have the count of bytes written collected by spark |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-17493](https://issues.apache.org/jira/browse/HADOOP-17493) | renaming S3A Statistic DELEGATION\_TOKENS\_ISSUED to DELEGATION\_TOKEN\_ISSUED broke tests downstream |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-17483](https://issues.apache.org/jira/browse/HADOOP-17483) | magic committer to be enabled for all S3 buckets |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-17337](https://issues.apache.org/jira/browse/HADOOP-17337) | S3A NetworkBinding has a runtime class dependency on a third-party shaded class |  Blocker | fs/s3 | Chris Wensel | Steve Loughran |
| [HADOOP-17475](https://issues.apache.org/jira/browse/HADOOP-17475) | ABFS : add high performance listStatusIterator |  Major | fs/azure | Bilahari T H | Bilahari T H |
| [HADOOP-17432](https://issues.apache.org/jira/browse/HADOOP-17432) | [JDK 16] KerberosUtil#getOidInstance is broken by JEP 396 |  Major | auth | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-13327](https://issues.apache.org/jira/browse/HADOOP-13327) | Add OutputStream + Syncable to the Filesystem Specification |  Major | fs | Steve Loughran | Steve Loughran |
| [HDFS-15836](https://issues.apache.org/jira/browse/HDFS-15836) | RBF: Fix contract tests after HADOOP-13327 |  Major | rbf | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17038](https://issues.apache.org/jira/browse/HADOOP-17038) | Support disabling buffered reads in ABFS positional reads |  Major | . | Anoop Sam John | Anoop Sam John |
| [HADOOP-15710](https://issues.apache.org/jira/browse/HADOOP-15710) | ABFS checkException to map 403 to AccessDeniedException |  Blocker | fs/azure | Steve Loughran | Steve Loughran |
| [HDFS-15847](https://issues.apache.org/jira/browse/HDFS-15847) | create client protocol: add ecPolicyName & storagePolicy param to debug statement string |  Minor | . | Bhavik Patel | Bhavik Patel |
| [HADOOP-16748](https://issues.apache.org/jira/browse/HADOOP-16748) | Migrate to Python 3 and upgrade Yetus to 0.13.0 |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-16906](https://issues.apache.org/jira/browse/HADOOP-16906) | Add some Abortable.abort() interface for streams etc which can be terminated |  Blocker | fs, fs/azure, fs/s3 | Steve Loughran | Jungtaek Lim |
| [HADOOP-17567](https://issues.apache.org/jira/browse/HADOOP-17567) | typo in MagicCommitTracker |  Trivial | fs/s3 | Pierrick HYMBERT | Pierrick HYMBERT |
| [HADOOP-17191](https://issues.apache.org/jira/browse/HADOOP-17191) | ABFS: Run the integration tests with various combinations of configurations and publish consolidated results |  Minor | fs/azure, test | Bilahari T H | Bilahari T H |
| [HADOOP-16721](https://issues.apache.org/jira/browse/HADOOP-16721) | Improve S3A rename resilience |  Blocker | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-17548](https://issues.apache.org/jira/browse/HADOOP-17548) | ABFS: Toggle Store Mkdirs request overwrite parameter |  Major | fs/azure | Sumangala Patki | Sumangala Patki |
| [HADOOP-17537](https://issues.apache.org/jira/browse/HADOOP-17537) | Correct abfs test assertion reversed in HADOOP-13327 |  Major | fs/azure, test | Sumangala Patki | Sumangala Patki |
| [HDFS-15890](https://issues.apache.org/jira/browse/HDFS-15890) | Improve the Logs for File Concat Operation |  Minor | namenode | Bhavik Patel | Bhavik Patel |
| [HDFS-13975](https://issues.apache.org/jira/browse/HDFS-13975) | TestBalancer#testMaxIterationTime fails sporadically |  Major | . | Jason Darrell Lowe | Toshihiko Uchida |
| [YARN-10688](https://issues.apache.org/jira/browse/YARN-10688) | ClusterMetrics should support GPU capacity related metrics. |  Major | metrics, resourcemanager | Qi Zhu | Qi Zhu |
| [YARN-10692](https://issues.apache.org/jira/browse/YARN-10692) | Add Node GPU Utilization and apply to NodeMetrics. |  Major | . | Qi Zhu | Qi Zhu |
| [HDFS-15902](https://issues.apache.org/jira/browse/HDFS-15902) | Improve the log for HTTPFS server operation |  Minor | httpfs | Bhavik Patel | Bhavik Patel |
| [HADOOP-17476](https://issues.apache.org/jira/browse/HADOOP-17476) | ITestAssumeRole.testAssumeRoleBadInnerAuth failure |  Major | fs/s3, test | Steve Loughran | Steve Loughran |
| [HADOOP-13551](https://issues.apache.org/jira/browse/HADOOP-13551) | Collect AwsSdkMetrics in S3A FileSystem IOStatistics |  Blocker | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-10713](https://issues.apache.org/jira/browse/YARN-10713) | ClusterMetrics should support custom resource capacity related metrics. |  Major | . | Qi Zhu | Qi Zhu |
| [HDFS-15921](https://issues.apache.org/jira/browse/HDFS-15921) | Improve the log for the Storage Policy Operations |  Minor | namenode | Bhavik Patel | Bhavik Patel |
| [YARN-10702](https://issues.apache.org/jira/browse/YARN-10702) | Add cluster metric for amount of CPU used by RM Event Processor |  Minor | yarn | Jim Brennan | Jim Brennan |
| [YARN-10503](https://issues.apache.org/jira/browse/YARN-10503) | Support queue capacity in terms of absolute resources with custom resourceType. |  Critical | . | Qi Zhu | Qi Zhu |
| [HADOOP-17630](https://issues.apache.org/jira/browse/HADOOP-17630) | [JDK 15] TestPrintableString fails due to Unicode 13.0 support |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17576](https://issues.apache.org/jira/browse/HADOOP-17576) | ABFS: Disable throttling update for auth failures |  Major | fs/azure | Sumangala Patki | Sumangala Patki |
| [YARN-10723](https://issues.apache.org/jira/browse/YARN-10723) | Change CS nodes page in UI to support custom resource. |  Major | . | Qi Zhu | Qi Zhu |
| [HADOOP-16948](https://issues.apache.org/jira/browse/HADOOP-16948) | ABFS: Support infinite lease dirs |  Minor | . | Billie Rinaldi | Billie Rinaldi |
| [HADOOP-17471](https://issues.apache.org/jira/browse/HADOOP-17471) | ABFS to collect IOStatistics |  Major | fs/azure | Steve Loughran | Mehakmeet Singh |
| [HADOOP-17535](https://issues.apache.org/jira/browse/HADOOP-17535) | ABFS: ITestAzureBlobFileSystemCheckAccess test failure if test doesn't have oauth keys |  Major | fs/azure | Steve Loughran | Steve Loughran |
| [HADOOP-17112](https://issues.apache.org/jira/browse/HADOOP-17112) | whitespace not allowed in paths when saving files to s3a via committer |  Blocker | fs/s3 | Krzysztof Adamski | Krzysztof Adamski |
| [HADOOP-17597](https://issues.apache.org/jira/browse/HADOOP-17597) | Add option to downgrade S3A rejection of Syncable to warning |  Minor | . | Steve Loughran | Steve Loughran |
| [HADOOP-17661](https://issues.apache.org/jira/browse/HADOOP-17661) | mvn versions:set fails to parse pom.xml |  Blocker | build | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-17536](https://issues.apache.org/jira/browse/HADOOP-17536) | ABFS: Suport for customer provided encryption key |  Minor | fs/azure | Bilahari T H | Bilahari T H |
| [YARN-10707](https://issues.apache.org/jira/browse/YARN-10707) | Support custom resources in ResourceUtilization, and update Node GPU Utilization to use. |  Major | yarn | Qi Zhu | Qi Zhu |
| [HADOOP-17653](https://issues.apache.org/jira/browse/HADOOP-17653) | Do not use guava's Files.createTempDir() |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-15952](https://issues.apache.org/jira/browse/HDFS-15952) | TestRouterRpcMultiDestination#testProxyGetTransactionID and testProxyVersionRequest are flaky |  Major | rbf | Harunobu Daikoku | Akira Ajisaka |
| [HADOOP-16742](https://issues.apache.org/jira/browse/HADOOP-16742) | Possible NPE in S3A MultiObjectDeleteSupport error handling |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-17644](https://issues.apache.org/jira/browse/HADOOP-17644) | Add back the exceptions removed by HADOOP-17432 for compatibility |  Blocker | bulid | Akira Ajisaka | Quan Li |
| [YARN-10642](https://issues.apache.org/jira/browse/YARN-10642) | Race condition: AsyncDispatcher can get stuck by the changes introduced in YARN-8995 |  Critical | resourcemanager | zhengchenyu | zhengchenyu |
| [YARN-9615](https://issues.apache.org/jira/browse/YARN-9615) | Add dispatcher metrics to RM |  Major | . | Jonathan Hung | Qi Zhu |
| [HDFS-13934](https://issues.apache.org/jira/browse/HDFS-13934) | Multipart uploaders to be created through API call to FileSystem/FileContext, not service loader |  Major | fs, fs/s3, hdfs | Steve Loughran | Steve Loughran |
| [HADOOP-17665](https://issues.apache.org/jira/browse/HADOOP-17665) | Ignore missing keystore configuration in reloading mechanism |  Major | . | Borislav Iordanov | Borislav Iordanov |
| [HADOOP-17663](https://issues.apache.org/jira/browse/HADOOP-17663) | Remove useless property hadoop.assemblies.version in pom file |  Trivial | build | Wei-Chiu Chuang | Akira Ajisaka |
| [HADOOP-17666](https://issues.apache.org/jira/browse/HADOOP-17666) | Update LICENSE for 3.3.1 |  Blocker | . | Wei-Chiu Chuang | Wei-Chiu Chuang |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-17430](https://issues.apache.org/jira/browse/HADOOP-17430) |  Restore ability to set Text to empty byte array |  Minor | common | gaozhan ding | gaozhan ding |
| [HDFS-15870](https://issues.apache.org/jira/browse/HDFS-15870) | Remove unused configuration dfs.namenode.stripe.min |  Minor | . | tomscut | tomscut |
| [HDFS-15808](https://issues.apache.org/jira/browse/HDFS-15808) | Add metrics for FSNamesystem read/write lock hold long time |  Major | hdfs | tomscut | tomscut |
| [HDFS-15873](https://issues.apache.org/jira/browse/HDFS-15873) | Add namenode address in logs for block report |  Minor | datanode, hdfs | tomscut | tomscut |
| [HDFS-15906](https://issues.apache.org/jira/browse/HDFS-15906) | Close FSImage and FSNamesystem after formatting is complete |  Minor | . | tomscut | tomscut |
| [HDFS-15892](https://issues.apache.org/jira/browse/HDFS-15892) | Add metric for editPendingQ in FSEditLogAsync |  Minor | . | tomscut | tomscut |
| [HDFS-15951](https://issues.apache.org/jira/browse/HDFS-15951) | Remove unused parameters in NameNodeProxiesClient |  Minor | . | tomscut | tomscut |
| [HDFS-15975](https://issues.apache.org/jira/browse/HDFS-15975) | Use LongAdder instead of AtomicLong |  Minor | . | tomscut | tomscut |
| [HDFS-15970](https://issues.apache.org/jira/browse/HDFS-15970) | Print network topology on the web |  Minor | . | tomscut | tomscut |
| [HDFS-15991](https://issues.apache.org/jira/browse/HDFS-15991) | Add location into datanode info for NameNodeMXBean |  Minor | . | tomscut | tomscut |
| [HADOOP-17055](https://issues.apache.org/jira/browse/HADOOP-17055) | Remove residual code of Ozone |  Major | . | Wanqiang Ji | Wanqiang Ji |
| [YARN-10274](https://issues.apache.org/jira/browse/YARN-10274) | Merge QueueMapping and QueueMappingEntity |  Major | yarn | Gergely Pollák | Gergely Pollák |
| [YARN-10281](https://issues.apache.org/jira/browse/YARN-10281) | Redundant QueuePath usage in UserGroupMappingPlacementRule and AppNameMappingPlacementRule |  Major | . | Gergely Pollák | Gergely Pollák |
| [YARN-10279](https://issues.apache.org/jira/browse/YARN-10279) | Avoid unnecessary QueueMappingEntity creations |  Minor | . | Gergely Pollák | Hudáky Márton Gyula |
| [YARN-10277](https://issues.apache.org/jira/browse/YARN-10277) | CapacityScheduler test TestUserGroupMappingPlacementRule should build proper hierarchy |  Major | . | Gergely Pollák | Szilard Nemeth |
| [HADOOP-16990](https://issues.apache.org/jira/browse/HADOOP-16990) | Update Mockserver |  Major | . | Wei-Chiu Chuang | Attila Doroszlai |
| [YARN-10278](https://issues.apache.org/jira/browse/YARN-10278) | CapacityScheduler test framework ProportionalCapacityPreemptionPolicyMockFramework need some review |  Major | . | Gergely Pollák | Szilard Nemeth |
| [YARN-10540](https://issues.apache.org/jira/browse/YARN-10540) | Node page is broken in YARN UI1 and UI2 including RMWebService api for nodes |  Critical | webapp | Sunil G | Jim Brennan |
| [HADOOP-17445](https://issues.apache.org/jira/browse/HADOOP-17445) | Update the year to 2021 |  Major | . | Xiaoqiao He | Xiaoqiao He |
| [HDFS-15731](https://issues.apache.org/jira/browse/HDFS-15731) | Reduce threadCount for unit tests to reduce the memory usage |  Major | build, test | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17571](https://issues.apache.org/jira/browse/HADOOP-17571) | Upgrade com.fasterxml.woodstox:woodstox-core for security reasons |  Major | . | Viraj Jasani | Viraj Jasani |
| [HDFS-15895](https://issues.apache.org/jira/browse/HDFS-15895) | DFSAdmin#printOpenFiles has redundant String#format usage |  Minor | . | Viraj Jasani | Viraj Jasani |
| [HDFS-15926](https://issues.apache.org/jira/browse/HDFS-15926) | Removed duplicate dependency of hadoop-annotations |  Minor | . | Viraj Jasani | Viraj Jasani |
| [HADOOP-17614](https://issues.apache.org/jira/browse/HADOOP-17614) | Bump netty to the latest 4.1.61 |  Blocker | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-17622](https://issues.apache.org/jira/browse/HADOOP-17622) | Avoid usage of deprecated IOUtils#cleanup API |  Minor | . | Viraj Jasani | Viraj Jasani |
| [HADOOP-17624](https://issues.apache.org/jira/browse/HADOOP-17624) | Remove any rocksdb exclusion code |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-17625](https://issues.apache.org/jira/browse/HADOOP-17625) | Update to Jetty 9.4.39 |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-15989](https://issues.apache.org/jira/browse/HDFS-15989) | Split TestBalancer into two classes |  Major | . | Viraj Jasani | Viraj Jasani |
| [HADOOP-17676](https://issues.apache.org/jira/browse/HADOOP-17676) | Restrict imports from org.apache.curator.shaded |  Major | . | Viraj Jasani | Viraj Jasani |
| [HADOOP-17683](https://issues.apache.org/jira/browse/HADOOP-17683) | Update commons-io to 2.8.0 |  Major | . | Wei-Chiu Chuang | Akira Ajisaka |
| [HADOOP-17426](https://issues.apache.org/jira/browse/HADOOP-17426) | Upgrade to hadoop-thirdparty-1.1.0 |  Major | . | Ayush Saxena | Wei-Chiu Chuang |
| [HADOOP-17739](https://issues.apache.org/jira/browse/HADOOP-17739) | Use hadoop-thirdparty 1.1.1 |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |


