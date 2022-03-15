
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

## Release 3.3.2 - 2022-02-21



### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-15814](https://issues.apache.org/jira/browse/HDFS-15814) | Make some parameters configurable for DataNodeDiskMetrics |  Major | hdfs | tomscut | tomscut |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-15288](https://issues.apache.org/jira/browse/HDFS-15288) | Add Available Space Rack Fault Tolerant BPP |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-16048](https://issues.apache.org/jira/browse/HDFS-16048) | RBF: Print network topology on the router web |  Minor | . | tomscut | tomscut |
| [HDFS-16337](https://issues.apache.org/jira/browse/HDFS-16337) | Show start time of Datanode on Web |  Minor | . | tomscut | tomscut |
| [HADOOP-17979](https://issues.apache.org/jira/browse/HADOOP-17979) | Interface EtagSource to allow FileStatus subclasses to provide etags |  Major | fs, fs/azure, fs/s3 | Steve Loughran | Steve Loughran |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-10123](https://issues.apache.org/jira/browse/YARN-10123) | Error message around yarn app -stop/start can be improved to highlight that an implementation at framework level is needed for the stop/start functionality to work |  Minor | client, documentation | Siddharth Ahuja | Siddharth Ahuja |
| [HADOOP-17756](https://issues.apache.org/jira/browse/HADOOP-17756) | Increase precommit job timeout from 20 hours to 24 hours. |  Major | build | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-16073](https://issues.apache.org/jira/browse/HDFS-16073) | Remove redundant RPC requests for getFileLinkInfo in ClientNamenodeProtocolTranslatorPB |  Minor | . | lei w | lei w |
| [HDFS-16074](https://issues.apache.org/jira/browse/HDFS-16074) | Remove an expensive debug string concatenation |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-16080](https://issues.apache.org/jira/browse/HDFS-16080) | RBF: Invoking method in all locations should break the loop after successful result |  Minor | . | Viraj Jasani | Viraj Jasani |
| [HDFS-16075](https://issues.apache.org/jira/browse/HDFS-16075) | Use empty array constants present in StorageType and DatanodeInfo to avoid creating redundant objects |  Major | . | Viraj Jasani | Viraj Jasani |
| [MAPREDUCE-7354](https://issues.apache.org/jira/browse/MAPREDUCE-7354) | Use empty array constants present in TaskCompletionEvent to avoid creating redundant objects |  Minor | . | Viraj Jasani | Viraj Jasani |
| [HDFS-16082](https://issues.apache.org/jira/browse/HDFS-16082) | Avoid non-atomic operations on exceptionsSinceLastBalance and failedTimesSinceLastSuccessfulBalance in Balancer |  Major | . | Viraj Jasani | Viraj Jasani |
| [HDFS-16076](https://issues.apache.org/jira/browse/HDFS-16076) | Avoid using slow DataNodes for reading by sorting locations |  Major | hdfs | tomscut | tomscut |
| [HDFS-16085](https://issues.apache.org/jira/browse/HDFS-16085) | Move the getPermissionChecker out of the read lock |  Minor | . | tomscut | tomscut |
| [YARN-10834](https://issues.apache.org/jira/browse/YARN-10834) | Intra-queue preemption: apps that don't use defined custom resource won't be preempted. |  Major | . | Eric Payne | Eric Payne |
| [HADOOP-17777](https://issues.apache.org/jira/browse/HADOOP-17777) | Update clover-maven-plugin version from 3.3.0 to 4.4.1 |  Major | . | Wanqiang Ji | Wanqiang Ji |
| [HDFS-16090](https://issues.apache.org/jira/browse/HDFS-16090) | Fine grained locking for datanodeNetworkCounts |  Major | . | Viraj Jasani | Viraj Jasani |
| [HADOOP-17749](https://issues.apache.org/jira/browse/HADOOP-17749) | Remove lock contention in SelectorPool of SocketIOWithTimeout |  Major | common | Xuesen Liang | Xuesen Liang |
| [HADOOP-17775](https://issues.apache.org/jira/browse/HADOOP-17775) | Remove JavaScript package from Docker environment |  Major | build | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-17402](https://issues.apache.org/jira/browse/HADOOP-17402) | Add GCS FS impl reference to core-default.xml |  Major | fs | Rafal Wojdyla | Rafal Wojdyla |
| [HADOOP-17794](https://issues.apache.org/jira/browse/HADOOP-17794) | Add a sample configuration to use ZKDelegationTokenSecretManager in Hadoop KMS |  Major | documentation, kms, security | Akira Ajisaka | Akira Ajisaka |
| [HDFS-16122](https://issues.apache.org/jira/browse/HDFS-16122) | Fix DistCpContext#toString() |  Minor | . | tomscut | tomscut |
| [HADOOP-12665](https://issues.apache.org/jira/browse/HADOOP-12665) | Document hadoop.security.token.service.use\_ip |  Major | documentation | Arpit Agarwal | Akira Ajisaka |
| [YARN-10456](https://issues.apache.org/jira/browse/YARN-10456) | RM PartitionQueueMetrics records are named QueueMetrics in Simon metrics registry |  Major | resourcemanager | Eric Payne | Eric Payne |
| [HDFS-15650](https://issues.apache.org/jira/browse/HDFS-15650) | Make the socket timeout for computing checksum of striped blocks configurable |  Minor | datanode, ec, erasure-coding | Yushi Hayasaka | Yushi Hayasaka |
| [YARN-10858](https://issues.apache.org/jira/browse/YARN-10858) | [UI2] YARN-10826 breaks Queue view |  Major | yarn-ui-v2 | Andras Gyori | Masatake Iwasaki |
| [HADOOP-16290](https://issues.apache.org/jira/browse/HADOOP-16290) | Enable RpcMetrics units to be configurable |  Major | ipc, metrics | Erik Krogen | Viraj Jasani |
| [YARN-10860](https://issues.apache.org/jira/browse/YARN-10860) | Make max container per heartbeat configs refreshable |  Major | . | Eric Badger | Eric Badger |
| [HADOOP-17813](https://issues.apache.org/jira/browse/HADOOP-17813) | Checkstyle - Allow line length: 100 |  Major | . | Akira Ajisaka | Viraj Jasani |
| [HADOOP-17811](https://issues.apache.org/jira/browse/HADOOP-17811) | ABFS ExponentialRetryPolicy doesn't pick up configuration values |  Minor | documentation, fs/azure | Brian Frank Loss | Brian Frank Loss |
| [HADOOP-17819](https://issues.apache.org/jira/browse/HADOOP-17819) | Add extensions to ProtobufRpcEngine RequestHeaderProto |  Major | common | Hector Sandoval Chaverri | Hector Sandoval Chaverri |
| [HDFS-15936](https://issues.apache.org/jira/browse/HDFS-15936) | Solve BlockSender#sendPacket() does not record SocketTimeout exception |  Minor | . | JiangHua Zhu | JiangHua Zhu |
| [HDFS-16153](https://issues.apache.org/jira/browse/HDFS-16153) | Avoid evaluation of LOG.debug statement in QuorumJournalManager |  Trivial | . | wangzhaohui | wangzhaohui |
| [HDFS-16154](https://issues.apache.org/jira/browse/HDFS-16154) | TestMiniJournalCluster failing intermittently because of not reseting UserGroupInformation completely |  Minor | . | wangzhaohui | wangzhaohui |
| [HADOOP-17837](https://issues.apache.org/jira/browse/HADOOP-17837) | Make it easier to debug UnknownHostExceptions from NetUtils.connect |  Minor | . | Bryan Beaudreault | Bryan Beaudreault |
| [HDFS-16175](https://issues.apache.org/jira/browse/HDFS-16175) | Improve the configurable value of Server #PURGE\_INTERVAL\_NANOS |  Major | ipc | JiangHua Zhu | JiangHua Zhu |
| [HDFS-16173](https://issues.apache.org/jira/browse/HDFS-16173) | Improve CopyCommands#Put#executor queue configurability |  Major | fs | JiangHua Zhu | JiangHua Zhu |
| [HADOOP-17897](https://issues.apache.org/jira/browse/HADOOP-17897) | Allow nested blocks in switch case in checkstyle settings |  Minor | build | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-17857](https://issues.apache.org/jira/browse/HADOOP-17857) | Check real user ACLs in addition to proxied user ACLs |  Major | . | Eric Payne | Eric Payne |
| [HDFS-16210](https://issues.apache.org/jira/browse/HDFS-16210) | RBF: Add the option of refreshCallQueue to RouterAdmin |  Major | . | Janus Chow | Janus Chow |
| [HDFS-16221](https://issues.apache.org/jira/browse/HDFS-16221) | RBF: Add usage of refreshCallQueue for Router |  Major | . | Janus Chow | Janus Chow |
| [HDFS-16223](https://issues.apache.org/jira/browse/HDFS-16223) | AvailableSpaceRackFaultTolerantBlockPlacementPolicy should use chooseRandomWithStorageTypeTwoTrial() for better performance. |  Major | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-17893](https://issues.apache.org/jira/browse/HADOOP-17893) | Improve PrometheusSink for Namenode TopMetrics |  Major | metrics | Max  Xie | Max  Xie |
| [HADOOP-17926](https://issues.apache.org/jira/browse/HADOOP-17926) | Maven-eclipse-plugin is no longer needed since Eclipse can import Maven projects by itself. |  Minor | documentation | Rintaro Ikeda | Rintaro Ikeda |
| [YARN-10935](https://issues.apache.org/jira/browse/YARN-10935) | AM Total Queue Limit goes below per-user AM Limit if parent is full. |  Major | capacity scheduler, capacityscheduler | Eric Payne | Eric Payne |
| [HADOOP-17939](https://issues.apache.org/jira/browse/HADOOP-17939) | Support building on Apple Silicon |  Major | build, common | Dongjoon Hyun | Dongjoon Hyun |
| [HADOOP-17941](https://issues.apache.org/jira/browse/HADOOP-17941) | Update xerces to 2.12.1 |  Minor | . | Zhongwei Zhu | Zhongwei Zhu |
| [HDFS-16246](https://issues.apache.org/jira/browse/HDFS-16246) | Print lockWarningThreshold in InstrumentedLock#logWarning and InstrumentedLock#logWaitWarning |  Minor | . | tomscut | tomscut |
| [HDFS-16252](https://issues.apache.org/jira/browse/HDFS-16252) | Correct docs for dfs.http.client.retry.policy.spec |  Major | . | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-16241](https://issues.apache.org/jira/browse/HDFS-16241) | Standby close reconstruction thread |  Major | . | zhanghuazong | zhanghuazong |
| [HADOOP-17974](https://issues.apache.org/jira/browse/HADOOP-17974) | Fix the import statements in hadoop-aws module |  Minor | build, fs/azure | Tamas Domok |  |
| [HDFS-16277](https://issues.apache.org/jira/browse/HDFS-16277) | Improve decision in AvailableSpaceBlockPlacementPolicy |  Major | block placement | guophilipse | guophilipse |
| [HADOOP-17770](https://issues.apache.org/jira/browse/HADOOP-17770) | WASB : Support disabling buffered reads in positional reads |  Major | . | Anoop Sam John | Anoop Sam John |
| [HDFS-16282](https://issues.apache.org/jira/browse/HDFS-16282) | Duplicate generic usage information to hdfs debug command |  Minor | tools | daimin | daimin |
| [YARN-1115](https://issues.apache.org/jira/browse/YARN-1115) | Provide optional means for a scheduler to check real user ACLs |  Major | capacity scheduler, scheduler | Eric Payne |  |
| [HDFS-16279](https://issues.apache.org/jira/browse/HDFS-16279) | Print detail datanode info when process first storage report |  Minor | . | tomscut | tomscut |
| [HDFS-16286](https://issues.apache.org/jira/browse/HDFS-16286) | Debug tool to verify the correctness of erasure coding on file |  Minor | erasure-coding, tools | daimin | daimin |
| [HDFS-16294](https://issues.apache.org/jira/browse/HDFS-16294) | Remove invalid DataNode#CONFIG\_PROPERTY\_SIMULATED |  Major | datanode | JiangHua Zhu | JiangHua Zhu |
| [HDFS-16299](https://issues.apache.org/jira/browse/HDFS-16299) | Fix bug for TestDataNodeVolumeMetrics#verifyDataNodeVolumeMetrics |  Minor | . | tomscut | tomscut |
| [HDFS-16301](https://issues.apache.org/jira/browse/HDFS-16301) | Improve BenchmarkThroughput#SIZE naming standardization |  Minor | benchmarks, test | JiangHua Zhu | JiangHua Zhu |
| [HDFS-16287](https://issues.apache.org/jira/browse/HDFS-16287) | Support to make dfs.namenode.avoid.read.slow.datanode  reconfigurable |  Major | . | Haiyang Hu | Haiyang Hu |
| [HDFS-16321](https://issues.apache.org/jira/browse/HDFS-16321) | Fix invalid config in TestAvailableSpaceRackFaultTolerantBPP |  Minor | test | guophilipse | guophilipse |
| [HDFS-16315](https://issues.apache.org/jira/browse/HDFS-16315) | Add metrics related to Transfer and NativeCopy for DataNode |  Major | . | tomscut | tomscut |
| [HADOOP-17998](https://issues.apache.org/jira/browse/HADOOP-17998) | Allow get command to run with multi threads. |  Major | fs | Chengwei Wang | Chengwei Wang |
| [HDFS-16344](https://issues.apache.org/jira/browse/HDFS-16344) | Improve DirectoryScanner.Stats#toString |  Major | . | tomscut | tomscut |
| [HADOOP-18023](https://issues.apache.org/jira/browse/HADOOP-18023) | Allow cp command to run with multi threads. |  Major | fs | Chengwei Wang | Chengwei Wang |
| [HDFS-16314](https://issues.apache.org/jira/browse/HDFS-16314) | Support to make dfs.namenode.block-placement-policy.exclude-slow-nodes.enabled reconfigurable |  Major | . | Haiyang Hu | Haiyang Hu |
| [HADOOP-18026](https://issues.apache.org/jira/browse/HADOOP-18026) | Fix default value of Magic committer |  Minor | common | guophilipse | guophilipse |
| [HDFS-16345](https://issues.apache.org/jira/browse/HDFS-16345) | Fix test cases fail in TestBlockStoragePolicy |  Major | build | guophilipse | guophilipse |
| [HADOOP-18040](https://issues.apache.org/jira/browse/HADOOP-18040) | Use maven.test.failure.ignore instead of ignoreTestFailure |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17643](https://issues.apache.org/jira/browse/HADOOP-17643) | WASB : Make metadata checks case insensitive |  Major | . | Anoop Sam John | Anoop Sam John |
| [HADOOP-18033](https://issues.apache.org/jira/browse/HADOOP-18033) | Upgrade fasterxml Jackson to 2.13.0 |  Major | build | Akira Ajisaka | Viraj Jasani |
| [HDFS-16327](https://issues.apache.org/jira/browse/HDFS-16327) | Make dfs.namenode.max.slowpeer.collect.nodes reconfigurable |  Major | . | tomscut | tomscut |
| [HDFS-16375](https://issues.apache.org/jira/browse/HDFS-16375) | The FBR lease ID should be exposed to the log |  Major | . | tomscut | tomscut |
| [HDFS-16386](https://issues.apache.org/jira/browse/HDFS-16386) | Reduce DataNode load when FsDatasetAsyncDiskService is working |  Major | datanode | JiangHua Zhu | JiangHua Zhu |
| [HDFS-16391](https://issues.apache.org/jira/browse/HDFS-16391) | Avoid evaluation of LOG.debug statement in NameNodeHeartbeatService |  Trivial | . | wangzhaohui | wangzhaohui |
| [YARN-8234](https://issues.apache.org/jira/browse/YARN-8234) | Improve RM system metrics publisher's performance by pushing events to timeline server in batch |  Critical | resourcemanager, timelineserver | Hu Ziqian | Ashutosh Gupta |
| [HADOOP-18052](https://issues.apache.org/jira/browse/HADOOP-18052) | Support Apple Silicon in start-build-env.sh |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-18056](https://issues.apache.org/jira/browse/HADOOP-18056) | DistCp: Filter duplicates in the source paths |  Major | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-18065](https://issues.apache.org/jira/browse/HADOOP-18065) | ExecutorHelper.logThrowableFromAfterExecute() is too noisy. |  Minor | . | Mukund Thakur | Mukund Thakur |
| [HDFS-16043](https://issues.apache.org/jira/browse/HDFS-16043) | Add markedDeleteBlockScrubberThread to delete blocks asynchronously |  Major | hdfs, namanode | Xiangyi Zhu | Xiangyi Zhu |
| [HADOOP-18094](https://issues.apache.org/jira/browse/HADOOP-18094) | Disable S3A auditing by default. |  Blocker | fs/s3 | Steve Loughran | Steve Loughran |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-10438](https://issues.apache.org/jira/browse/YARN-10438) | Handle null containerId in ClientRMService#getContainerReport() |  Major | resourcemanager | Raghvendra Singh | Shubham Gupta |
| [YARN-10428](https://issues.apache.org/jira/browse/YARN-10428) | Zombie applications in the YARN queue using FAIR + sizebasedweight |  Critical | capacityscheduler | Guang Yang | Andras Gyori |
| [HDFS-15916](https://issues.apache.org/jira/browse/HDFS-15916) | DistCp: Backward compatibility: Distcp fails from Hadoop 3 to Hadoop 2 for snapshotdiff |  Major | distcp | Srinivasu Majeti | Ayush Saxena |
| [HDFS-15977](https://issues.apache.org/jira/browse/HDFS-15977) | Call explicit\_bzero only if it is available |  Major | libhdfs++ | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-14922](https://issues.apache.org/jira/browse/HADOOP-14922) | Build of Mapreduce Native Task module fails with unknown opcode "bswap" |  Major | . | Anup Halarnkar | Anup Halarnkar |
| [HADOOP-17700](https://issues.apache.org/jira/browse/HADOOP-17700) | ExitUtil#halt info log should log HaltException |  Major | . | Viraj Jasani | Viraj Jasani |
| [YARN-10770](https://issues.apache.org/jira/browse/YARN-10770) | container-executor permission is wrong in SecureContainer.md |  Major | documentation | Akira Ajisaka | Siddharth Ahuja |
| [YARN-10691](https://issues.apache.org/jira/browse/YARN-10691) | DominantResourceCalculator isInvalidDivisor should consider only countable resource types |  Major | . | Bilwa S T | Bilwa S T |
| [HDFS-16031](https://issues.apache.org/jira/browse/HDFS-16031) | Possible Resource Leak in org.apache.hadoop.hdfs.server.aliasmap#InMemoryAliasMap |  Major | . | Narges Shadab | Narges Shadab |
| [MAPREDUCE-7348](https://issues.apache.org/jira/browse/MAPREDUCE-7348) | TestFrameworkUploader#testNativeIO fails |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15915](https://issues.apache.org/jira/browse/HDFS-15915) | Race condition with async edits logging due to updating txId outside of the namesystem log |  Major | hdfs, namenode | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-16040](https://issues.apache.org/jira/browse/HDFS-16040) | RpcQueueTime metric counts requeued calls as unique events. |  Major | hdfs | Simbarashe Dzinamarira | Simbarashe Dzinamarira |
| [MAPREDUCE-7287](https://issues.apache.org/jira/browse/MAPREDUCE-7287) | Distcp will delete existing file ,  If we use "-delete and -update" options and distcp file. |  Major | distcp | zhengchenyu | zhengchenyu |
| [HDFS-15998](https://issues.apache.org/jira/browse/HDFS-15998) | Fix NullPointException In listOpenFiles |  Major | . | Haiyang Hu | Haiyang Hu |
| [HDFS-16050](https://issues.apache.org/jira/browse/HDFS-16050) | Some dynamometer tests fail |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-17631](https://issues.apache.org/jira/browse/HADOOP-17631) | Configuration ${env.VAR:-FALLBACK} should eval FALLBACK when restrictSystemProps=true |  Minor | common | Steve Loughran | Steve Loughran |
| [YARN-10809](https://issues.apache.org/jira/browse/YARN-10809) | testWithHbaseConfAtHdfsFileSystem consistently failing |  Major | . | Viraj Jasani | Viraj Jasani |
| [YARN-10803](https://issues.apache.org/jira/browse/YARN-10803) | [JDK 11] TestRMFailoverProxyProvider and TestNoHaRMFailoverProxyProvider fails by ClassCastException |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [HDFS-16057](https://issues.apache.org/jira/browse/HDFS-16057) | Make sure the order for location in ENTERING\_MAINTENANCE state |  Minor | . | tomscut | tomscut |
| [HDFS-16055](https://issues.apache.org/jira/browse/HDFS-16055) | Quota is not preserved in snapshot INode |  Major | hdfs | Siyao Meng | Siyao Meng |
| [HDFS-16068](https://issues.apache.org/jira/browse/HDFS-16068) | WebHdfsFileSystem has a possible connection leak in connection with HttpFS |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-10767](https://issues.apache.org/jira/browse/YARN-10767) | Yarn Logs Command retrying on Standby RM for 30 times |  Major | . | D M Murali Krishna Reddy | D M Murali Krishna Reddy |
| [HADOOP-17760](https://issues.apache.org/jira/browse/HADOOP-17760) | Delete hadoop.ssl.enabled and dfs.https.enable from docs and core-default.xml |  Major | documentation | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-13671](https://issues.apache.org/jira/browse/HDFS-13671) | Namenode deletes large dir slowly caused by FoldedTreeSet#removeAndGet |  Major | . | Yiqun Lin | Haibin Huang |
| [HDFS-16061](https://issues.apache.org/jira/browse/HDFS-16061) | DFTestUtil.waitReplication can produce false positives |  Major | hdfs | Ahmed Hussein | Ahmed Hussein |
| [HDFS-14575](https://issues.apache.org/jira/browse/HDFS-14575) | LeaseRenewer#daemon threads leak in DFSClient |  Major | . | Tao Yang | Renukaprasad C |
| [YARN-10826](https://issues.apache.org/jira/browse/YARN-10826) | [UI2] Upgrade Node.js to at least v12.22.1 |  Major | yarn-ui-v2 | Akira Ajisaka | Masatake Iwasaki |
| [HADOOP-17769](https://issues.apache.org/jira/browse/HADOOP-17769) | Upgrade JUnit to 4.13.2 |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [YARN-10824](https://issues.apache.org/jira/browse/YARN-10824) | Title not set for JHS and NM webpages |  Major | . | Rajshree Mishra | Bilwa S T |
| [HDFS-16092](https://issues.apache.org/jira/browse/HDFS-16092) | Avoid creating LayoutFlags redundant objects |  Major | . | Viraj Jasani | Viraj Jasani |
| [HADOOP-17764](https://issues.apache.org/jira/browse/HADOOP-17764) | S3AInputStream read does not re-open the input stream on the second read retry attempt |  Major | fs/s3 | Zamil Majdy | Zamil Majdy |
| [HDFS-16109](https://issues.apache.org/jira/browse/HDFS-16109) | Fix flaky some unit tests since they offen timeout |  Minor | test | tomscut | tomscut |
| [HDFS-16108](https://issues.apache.org/jira/browse/HDFS-16108) | Incorrect log placeholders used in JournalNodeSyncer |  Minor | . | Viraj Jasani | Viraj Jasani |
| [MAPREDUCE-7353](https://issues.apache.org/jira/browse/MAPREDUCE-7353) | Mapreduce job fails when NM is stopped |  Major | . | Bilwa S T | Bilwa S T |
| [HDFS-16121](https://issues.apache.org/jira/browse/HDFS-16121) | Iterative snapshot diff report can generate duplicate records for creates, deletes and Renames |  Major | snapshots | Srinivasu Majeti | Shashikant Banerjee |
| [HDFS-15796](https://issues.apache.org/jira/browse/HDFS-15796) | ConcurrentModificationException error happens on NameNode occasionally |  Critical | hdfs | Daniel Ma | Daniel Ma |
| [HADOOP-17793](https://issues.apache.org/jira/browse/HADOOP-17793) | Better token validation |  Major | . | Artem Smotrakov | Artem Smotrakov |
| [HDFS-16042](https://issues.apache.org/jira/browse/HDFS-16042) | DatanodeAdminMonitor scan should be delay based |  Major | datanode | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17803](https://issues.apache.org/jira/browse/HADOOP-17803) | Remove WARN logging from LoggingAuditor when executing a request outside an audit span |  Major | fs/s3 | Mehakmeet Singh | Mehakmeet Singh |
| [HDFS-16127](https://issues.apache.org/jira/browse/HDFS-16127) | Improper pipeline close recovery causes a permanent write failure or data loss. |  Major | . | Kihwal Lee | Kihwal Lee |
| [HADOOP-17028](https://issues.apache.org/jira/browse/HADOOP-17028) | ViewFS should initialize target filesystems lazily |  Major | client-mounts, fs, viewfs | Uma Maheswara Rao G | Abhishek Das |
| [HADOOP-17801](https://issues.apache.org/jira/browse/HADOOP-17801) | No error message reported when bucket doesn't exist in S3AFS |  Major | fs/s3 | Mehakmeet Singh | Mehakmeet Singh |
| [HADOOP-17796](https://issues.apache.org/jira/browse/HADOOP-17796) | Upgrade jetty version to 9.4.43 |  Major | . | Wei-Chiu Chuang | Renukaprasad C |
| [HDFS-12920](https://issues.apache.org/jira/browse/HDFS-12920) | HDFS default value change (with adding time unit) breaks old version MR tarball work with Hadoop 3.x |  Critical | configuration, hdfs | Junping Du | Akira Ajisaka |
| [HDFS-16145](https://issues.apache.org/jira/browse/HDFS-16145) | CopyListing fails with FNF exception with snapshot diff |  Major | distcp | Shashikant Banerjee | Shashikant Banerjee |
| [YARN-10813](https://issues.apache.org/jira/browse/YARN-10813) | Set default capacity of root for node labels |  Major | . | Andras Gyori | Andras Gyori |
| [HDFS-16144](https://issues.apache.org/jira/browse/HDFS-16144) | Revert HDFS-15372 (Files in snapshots no longer see attribute provider permissions) |  Major | . | Stephen O'Donnell | Stephen O'Donnell |
| [HADOOP-17817](https://issues.apache.org/jira/browse/HADOOP-17817) | HADOOP-17817. S3A to raise IOE if both S3-CSE and S3Guard enabled |  Major | fs/s3 | Mehakmeet Singh | Mehakmeet Singh |
| [YARN-9551](https://issues.apache.org/jira/browse/YARN-9551) | TestTimelineClientV2Impl.testSyncCall fails intermittently |  Minor | ATSv2, test | Prabhu Joseph | Andras Gyori |
| [HDFS-15175](https://issues.apache.org/jira/browse/HDFS-15175) | Multiple CloseOp shared block instance causes the standby namenode to crash when rolling editlog |  Critical | . | Yicong Cai | Wan Chang |
| [YARN-10869](https://issues.apache.org/jira/browse/YARN-10869) | CS considers only the default maximum-allocation-mb/vcore property as a maximum when it creates dynamic queues |  Major | capacity scheduler | Benjamin Teke | Benjamin Teke |
| [YARN-10789](https://issues.apache.org/jira/browse/YARN-10789) | RM HA startup can fail due to race conditions in ZKConfigurationStore |  Major | . | Tarun Parimi | Tarun Parimi |
| [HADOOP-17812](https://issues.apache.org/jira/browse/HADOOP-17812) | NPE in S3AInputStream read() after failure to reconnect to store |  Major | fs/s3 | Bobby Wang | Bobby Wang |
| [YARN-6221](https://issues.apache.org/jira/browse/YARN-6221) | Entities missing from ATS when summary log file info got returned to the ATS before the domain log |  Critical | yarn | Sushmitha Sreenivasan | Xiaomin Zhang |
| [MAPREDUCE-7258](https://issues.apache.org/jira/browse/MAPREDUCE-7258) | HistoryServerRest.html#Task\_Counters\_API, modify the jobTaskCounters's itemName from "taskcounterGroup" to "taskCounterGroup". |  Minor | documentation | jenny | jenny |
| [HADOOP-17370](https://issues.apache.org/jira/browse/HADOOP-17370) | Upgrade commons-compress to 1.21 |  Major | common | Dongjoon Hyun | Akira Ajisaka |
| [HDFS-16151](https://issues.apache.org/jira/browse/HDFS-16151) | Improve the parameter comments related to ProtobufRpcEngine2#Server() |  Minor | documentation | JiangHua Zhu | JiangHua Zhu |
| [HADOOP-17844](https://issues.apache.org/jira/browse/HADOOP-17844) | Upgrade JSON smart to 2.4.7 |  Major | . | Renukaprasad C | Renukaprasad C |
| [HDFS-16177](https://issues.apache.org/jira/browse/HDFS-16177) | Bug fix for Util#receiveFile |  Minor | . | tomscut | tomscut |
| [YARN-10814](https://issues.apache.org/jira/browse/YARN-10814) | YARN shouldn't start with empty hadoop.http.authentication.signature.secret.file |  Major | . | Benjamin Teke | Tamas Domok |
| [HADOOP-17858](https://issues.apache.org/jira/browse/HADOOP-17858) | Avoid possible class loading deadlock with VerifierNone initialization |  Major | . | Viraj Jasani | Viraj Jasani |
| [HADOOP-17869](https://issues.apache.org/jira/browse/HADOOP-17869) | fs.s3a.connection.maximum should be bigger than fs.s3a.threads.max |  Major | common | Dongjoon Hyun | Dongjoon Hyun |
| [HADOOP-17886](https://issues.apache.org/jira/browse/HADOOP-17886) | Upgrade ant to 1.10.11 |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17874](https://issues.apache.org/jira/browse/HADOOP-17874) | ExceptionsHandler to add terse/suppressed Exceptions in thread-safe manner |  Major | . | Viraj Jasani | Viraj Jasani |
| [HADOOP-15129](https://issues.apache.org/jira/browse/HADOOP-15129) | Datanode caches namenode DNS lookup failure and cannot startup |  Minor | ipc | Karthik Palaniappan | Chris Nauroth |
| [HADOOP-17870](https://issues.apache.org/jira/browse/HADOOP-17870) | HTTP Filesystem to qualify paths in open()/getFileStatus() |  Minor | fs | VinothKumar Raman | VinothKumar Raman |
| [HADOOP-17899](https://issues.apache.org/jira/browse/HADOOP-17899) | Avoid using implicit dependency on junit-jupiter-api |  Major | test | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-10901](https://issues.apache.org/jira/browse/YARN-10901) | Permission checking error on an existing directory in LogAggregationFileController#verifyAndCreateRemoteLogDir |  Major | nodemanager | Tamas Domok | Tamas Domok |
| [HADOOP-17804](https://issues.apache.org/jira/browse/HADOOP-17804) | Prometheus metrics only include the last set of labels |  Major | common | Adam Binford | Adam Binford |
| [HDFS-16207](https://issues.apache.org/jira/browse/HDFS-16207) | Remove NN logs stack trace for non-existent xattr query |  Major | namenode | Ahmed Hussein | Ahmed Hussein |
| [HDFS-16187](https://issues.apache.org/jira/browse/HDFS-16187) | SnapshotDiff behaviour with Xattrs and Acls is not consistent across NN restarts with checkpointing |  Major | snapshots | Srinivasu Majeti | Shashikant Banerjee |
| [HDFS-16198](https://issues.apache.org/jira/browse/HDFS-16198) | Short circuit read leaks Slot objects when InvalidToken exception is thrown |  Major | . | Eungsop Yoo | Eungsop Yoo |
| [YARN-10870](https://issues.apache.org/jira/browse/YARN-10870) | Missing user filtering check -\> yarn.webapp.filter-entity-list-by-user for RM Scheduler page |  Major | yarn | Siddharth Ahuja | Gergely Pollák |
| [HADOOP-17891](https://issues.apache.org/jira/browse/HADOOP-17891) | lz4-java and snappy-java should be excluded from relocation in shaded Hadoop libraries |  Major | . | L. C. Hsieh | L. C. Hsieh |
| [HADOOP-17919](https://issues.apache.org/jira/browse/HADOOP-17919) | Fix command line example in Hadoop Cluster Setup documentation |  Minor | documentation | Rintaro Ikeda | Rintaro Ikeda |
| [YARN-9606](https://issues.apache.org/jira/browse/YARN-9606) | Set sslfactory for AuthenticatedURL() while creating LogsCLI#webServiceClient |  Major | . | Bilwa S T | Bilwa S T |
| [HDFS-16233](https://issues.apache.org/jira/browse/HDFS-16233) | Do not use exception handler to implement copy-on-write for EnumCounters |  Major | namenode | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-16235](https://issues.apache.org/jira/browse/HDFS-16235) | Deadlock in LeaseRenewer for static remove method |  Major | hdfs | angerszhu | angerszhu |
| [HADOOP-17940](https://issues.apache.org/jira/browse/HADOOP-17940) | Upgrade Kafka to 2.8.1 |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-10970](https://issues.apache.org/jira/browse/YARN-10970) | Standby RM should expose prom endpoint |  Major | resourcemanager | Max  Xie | Max  Xie |
| [HADOOP-17934](https://issues.apache.org/jira/browse/HADOOP-17934) | NullPointerException when no HTTP response set on AbfsRestOperation |  Major | fs/azure | Josh Elser | Josh Elser |
| [HDFS-16181](https://issues.apache.org/jira/browse/HDFS-16181) | [SBN Read] Fix metric of RpcRequestCacheMissAmount can't display when tailEditLog form JN |  Critical | . | wangzhaohui | wangzhaohui |
| [HADOOP-17922](https://issues.apache.org/jira/browse/HADOOP-17922) | Lookup old S3 encryption configs for JCEKS |  Major | fs/s3 | Mehakmeet Singh | Mehakmeet Singh |
| [HADOOP-17925](https://issues.apache.org/jira/browse/HADOOP-17925) | BUILDING.txt should not encourage to activate docs profile on building binary artifacts |  Minor | documentation | Rintaro Ikeda | Masatake Iwasaki |
| [HADOOP-16532](https://issues.apache.org/jira/browse/HADOOP-16532) | Fix TestViewFsTrash to use the correct homeDir. |  Minor | test, viewfs | Steve Loughran | Xing Lin |
| [HDFS-16268](https://issues.apache.org/jira/browse/HDFS-16268) | Balancer stuck when moving striped blocks due to NPE |  Major | balancer & mover, erasure-coding | Leon Gao | Leon Gao |
| [HDFS-16271](https://issues.apache.org/jira/browse/HDFS-16271) | RBF: NullPointerException when setQuota through routers with quota disabled |  Major | . | Chengwei Wang | Chengwei Wang |
| [YARN-10976](https://issues.apache.org/jira/browse/YARN-10976) | Fix resource leak due to Files.walk |  Minor | . | lujie | lujie |
| [HADOOP-17932](https://issues.apache.org/jira/browse/HADOOP-17932) | Distcp file length comparison have no effect |  Major | common, tools, tools/distcp | yinan zhan | yinan zhan |
| [HDFS-16272](https://issues.apache.org/jira/browse/HDFS-16272) | Int overflow in computing safe length during EC block recovery |  Critical | 3.1.1 | daimin | daimin |
| [HADOOP-17953](https://issues.apache.org/jira/browse/HADOOP-17953) | S3A: ITestS3AFileContextStatistics test to lookup global or per-bucket configuration for encryption algorithm |  Minor | fs/s3 | Mehakmeet Singh | Mehakmeet Singh |
| [HADOOP-17971](https://issues.apache.org/jira/browse/HADOOP-17971) | Exclude IBM Java security classes from being shaded/relocated |  Major | build | Nicholas Marion | Nicholas Marion |
| [HDFS-7612](https://issues.apache.org/jira/browse/HDFS-7612) | TestOfflineEditsViewer.testStored() uses incorrect default value for cacheDir |  Major | test | Konstantin Shvachko | Michael Kuchenbecker |
| [HDFS-16269](https://issues.apache.org/jira/browse/HDFS-16269) | [Fix] Improve NNThroughputBenchmark#blockReport operation |  Major | benchmarks, namenode | JiangHua Zhu | JiangHua Zhu |
| [HADOOP-17945](https://issues.apache.org/jira/browse/HADOOP-17945) | JsonSerialization raises EOFException reading JSON data stored on google GCS |  Major | fs | Steve Loughran | Steve Loughran |
| [HDFS-16259](https://issues.apache.org/jira/browse/HDFS-16259) | Catch and re-throw sub-classes of AccessControlException thrown by any permission provider plugins (eg Ranger) |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [HADOOP-17988](https://issues.apache.org/jira/browse/HADOOP-17988) | Disable JIRA plugin for YETUS on Hadoop |  Critical | build | Gautham Banasandra | Gautham Banasandra |
| [HDFS-16311](https://issues.apache.org/jira/browse/HDFS-16311) | Metric metadataOperationRate calculation error in DataNodeVolumeMetrics |  Major | . | tomscut | tomscut |
| [HADOOP-18002](https://issues.apache.org/jira/browse/HADOOP-18002) | abfs rename idempotency broken -remove recovery |  Major | fs/azure | Steve Loughran | Steve Loughran |
| [HDFS-16182](https://issues.apache.org/jira/browse/HDFS-16182) | numOfReplicas is given the wrong value in  BlockPlacementPolicyDefault$chooseTarget can cause DataStreamer to fail with Heterogeneous Storage |  Major | namanode | Max  Xie | Max  Xie |
| [HADOOP-17999](https://issues.apache.org/jira/browse/HADOOP-17999) | No-op implementation of setWriteChecksum and setVerifyChecksum in ViewFileSystem |  Major | . | Abhishek Das | Abhishek Das |
| [HDFS-16329](https://issues.apache.org/jira/browse/HDFS-16329) | Fix log format for BlockManager |  Minor | . | tomscut | tomscut |
| [HDFS-16330](https://issues.apache.org/jira/browse/HDFS-16330) | Fix incorrect placeholder for Exception logs in DiskBalancer |  Major | . | Viraj Jasani | Viraj Jasani |
| [HDFS-16328](https://issues.apache.org/jira/browse/HDFS-16328) | Correct disk balancer param desc |  Minor | documentation, hdfs | guophilipse | guophilipse |
| [HDFS-16334](https://issues.apache.org/jira/browse/HDFS-16334) | Correct NameNode ACL description |  Minor | documentation | guophilipse | guophilipse |
| [HDFS-16343](https://issues.apache.org/jira/browse/HDFS-16343) | Add some debug logs when the dfsUsed are not used during Datanode startup |  Major | datanode | Mukul Kumar Singh | Mukul Kumar Singh |
| [YARN-10991](https://issues.apache.org/jira/browse/YARN-10991) | Fix to ignore the grouping "[]" for resourcesStr in parseResourcesString method |  Minor | distributed-shell | Ashutosh Gupta | Ashutosh Gupta |
| [HADOOP-17975](https://issues.apache.org/jira/browse/HADOOP-17975) | Fallback to simple auth does not work for a secondary DistributedFileSystem instance |  Major | ipc | István Fajth | István Fajth |
| [HDFS-16350](https://issues.apache.org/jira/browse/HDFS-16350) | Datanode start time should be set after RPC server starts successfully |  Minor | . | Viraj Jasani | Viraj Jasani |
| [YARN-11007](https://issues.apache.org/jira/browse/YARN-11007) | Correct words in YARN documents |  Minor | documentation | guophilipse | guophilipse |
| [YARN-10975](https://issues.apache.org/jira/browse/YARN-10975) | EntityGroupFSTimelineStore#ActiveLogParser parses already processed files |  Major | timelineserver | Prabhu Joseph | Ravuri Sushma sree |
| [HDFS-16332](https://issues.apache.org/jira/browse/HDFS-16332) | Expired block token causes slow read due to missing handling in sasl handshake |  Major | datanode, dfs, dfsclient | Shinya Yoshida | Shinya Yoshida |
| [HDFS-16293](https://issues.apache.org/jira/browse/HDFS-16293) | Client sleeps and holds 'dataQueue' when DataNodes are congested |  Major | hdfs-client | Yuanxin Zhu | Yuanxin Zhu |
| [YARN-9063](https://issues.apache.org/jira/browse/YARN-9063) | ATS 1.5 fails to start if RollingLevelDb files are corrupt or missing |  Major | timelineserver, timelineservice | Tarun Parimi | Ashutosh Gupta |
| [HDFS-16333](https://issues.apache.org/jira/browse/HDFS-16333) | fix balancer bug when transfer an EC block |  Major | balancer & mover, erasure-coding | qinyuren | qinyuren |
| [YARN-11020](https://issues.apache.org/jira/browse/YARN-11020) | [UI2] No container is found for an application attempt with a single AM container |  Major | yarn-ui-v2 | Andras Gyori | Andras Gyori |
| [HDFS-16373](https://issues.apache.org/jira/browse/HDFS-16373) | Fix MiniDFSCluster restart in case of multiple namenodes |  Major | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-18048](https://issues.apache.org/jira/browse/HADOOP-18048) | [branch-3.3] Dockerfile\_aarch64 build fails with fatal error: Python.h: No such file or directory |  Major | . | Siyao Meng | Siyao Meng |
| [HDFS-16377](https://issues.apache.org/jira/browse/HDFS-16377) | Should CheckNotNull before access FsDatasetSpi |  Major | . | tomscut | tomscut |
| [YARN-6862](https://issues.apache.org/jira/browse/YARN-6862) | Nodemanager resource usage metrics sometimes are negative |  Major | nodemanager | YunFan Zhou | Benjamin Teke |
| [HADOOP-13500](https://issues.apache.org/jira/browse/HADOOP-13500) | Synchronizing iteration of Configuration properties object |  Major | conf | Jason Darrell Lowe | Dhananjay Badaya |
| [YARN-10178](https://issues.apache.org/jira/browse/YARN-10178) | Global Scheduler async thread crash caused by 'Comparison method violates its general contract |  Major | capacity scheduler | tuyu | Andras Gyori |
| [YARN-11053](https://issues.apache.org/jira/browse/YARN-11053) | AuxService should not use class name as default system classes |  Major | auxservices | Cheng Pan | Cheng Pan |
| [HDFS-16395](https://issues.apache.org/jira/browse/HDFS-16395) | Remove useless NNThroughputBenchmark#dummyActionNoSynch() |  Major | benchmarks, namenode | JiangHua Zhu | JiangHua Zhu |
| [HADOOP-18045](https://issues.apache.org/jira/browse/HADOOP-18045) | Disable TestDynamometerInfra |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [HDFS-14099](https://issues.apache.org/jira/browse/HDFS-14099) | Unknown frame descriptor when decompressing multiple frames in ZStandardDecompressor |  Major | . | xuzq | xuzq |
| [HADOOP-18063](https://issues.apache.org/jira/browse/HADOOP-18063) | Remove unused import AbstractJavaKeyStoreProvider in Shell class |  Minor | . | JiangHua Zhu | JiangHua Zhu |
| [HDFS-16409](https://issues.apache.org/jira/browse/HDFS-16409) | Fix typo: testHasExeceptionsReturnsCorrectValue -\> testHasExceptionsReturnsCorrectValue |  Trivial | . | Ashutosh Gupta | Ashutosh Gupta |
| [HDFS-16408](https://issues.apache.org/jira/browse/HDFS-16408) | Ensure LeaseRecheckIntervalMs is greater than zero |  Major | namenode | Jingxuan Fu | Jingxuan Fu |
| [HDFS-16410](https://issues.apache.org/jira/browse/HDFS-16410) | Insecure Xml parsing in OfflineEditsXmlLoader |  Minor | . | Ashutosh Gupta | Ashutosh Gupta |
| [HDFS-16420](https://issues.apache.org/jira/browse/HDFS-16420) | Avoid deleting unique data blocks when deleting redundancy striped blocks |  Critical | ec, erasure-coding | qinyuren | Jackson Wang |
| [YARN-10561](https://issues.apache.org/jira/browse/YARN-10561) | Upgrade node.js to 12.22.1 and yarn to 1.22.5 in YARN application catalog webapp |  Critical | webapp | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-18096](https://issues.apache.org/jira/browse/HADOOP-18096) | Distcp: Sync moves filtered file to home directory rather than deleting |  Critical | . | Ayush Saxena | Ayush Saxena |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-7342](https://issues.apache.org/jira/browse/MAPREDUCE-7342) | Stop RMService in TestClientRedirect.testRedirect() |  Minor | . | Zhengxi Li | Zhengxi Li |
| [MAPREDUCE-7311](https://issues.apache.org/jira/browse/MAPREDUCE-7311) | Fix non-idempotent test in TestTaskProgressReporter |  Minor | . | Zhengxi Li | Zhengxi Li |
| [HADOOP-17936](https://issues.apache.org/jira/browse/HADOOP-17936) | TestLocalFSCopyFromLocal.testDestinationFileIsToParentDirectory failure after reverting HADOOP-16878 |  Major | . | Chao Sun | Chao Sun |
| [HDFS-15862](https://issues.apache.org/jira/browse/HDFS-15862) | Make TestViewfsWithNfs3.testNfsRenameSingleNN() idempotent |  Minor | nfs | Zhengxi Li | Zhengxi Li |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-10337](https://issues.apache.org/jira/browse/YARN-10337) | TestRMHATimelineCollectors fails on hadoop trunk |  Major | test, yarn | Ahmed Hussein | Bilwa S T |
| [HDFS-15457](https://issues.apache.org/jira/browse/HDFS-15457) | TestFsDatasetImpl fails intermittently |  Major | hdfs | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17424](https://issues.apache.org/jira/browse/HADOOP-17424) | Replace HTrace with No-Op tracer |  Major | . | Siyao Meng | Siyao Meng |
| [HADOOP-17705](https://issues.apache.org/jira/browse/HADOOP-17705) | S3A to add option fs.s3a.endpoint.region to set AWS region |  Major | fs/s3 | Mehakmeet Singh | Mehakmeet Singh |
| [HADOOP-17670](https://issues.apache.org/jira/browse/HADOOP-17670) | S3AFS and ABFS to log IOStats at DEBUG mode or optionally at INFO level in close() |  Minor | fs/azure, fs/s3 | Mehakmeet Singh | Mehakmeet Singh |
| [HADOOP-17511](https://issues.apache.org/jira/browse/HADOOP-17511) | Add an Audit plugin point for S3A auditing/context |  Major | . | Steve Loughran | Steve Loughran |
| [HADOOP-17470](https://issues.apache.org/jira/browse/HADOOP-17470) | Collect more S3A IOStatistics |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-17735](https://issues.apache.org/jira/browse/HADOOP-17735) | Upgrade aws-java-sdk to 1.11.1026 |  Major | build, fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-17547](https://issues.apache.org/jira/browse/HADOOP-17547) | Magic committer to downgrade abort in cleanup if list uploads fails with access denied |  Major | fs/s3 | Steve Loughran | Bogdan Stolojan |
| [HADOOP-17771](https://issues.apache.org/jira/browse/HADOOP-17771) | S3AFS creation fails "Unable to find a region via the region provider chain." |  Blocker | fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-15659](https://issues.apache.org/jira/browse/HDFS-15659) | Set dfs.namenode.redundancy.considerLoad to false in MiniDFSCluster |  Major | test | Akira Ajisaka | Ahmed Hussein |
| [HADOOP-17774](https://issues.apache.org/jira/browse/HADOOP-17774) | bytesRead FS statistic showing twice the correct value in S3A |  Major | fs/s3 | Mehakmeet Singh | Mehakmeet Singh |
| [HADOOP-17290](https://issues.apache.org/jira/browse/HADOOP-17290) | ABFS: Add Identifiers to Client Request Header |  Major | fs/azure | Sumangala Patki | Sumangala Patki |
| [HADOOP-17250](https://issues.apache.org/jira/browse/HADOOP-17250) | ABFS: Random read perf improvement |  Major | fs/azure | Sneha Vijayarajan | Mukund Thakur |
| [HADOOP-17596](https://issues.apache.org/jira/browse/HADOOP-17596) | ABFS: Change default Readahead Queue Depth from num(processors) to const |  Major | fs/azure | Sumangala Patki | Sumangala Patki |
| [HADOOP-17715](https://issues.apache.org/jira/browse/HADOOP-17715) | ABFS: Append blob tests with non HNS accounts fail |  Minor | . | Sneha Varma | Sneha Varma |
| [HADOOP-17714](https://issues.apache.org/jira/browse/HADOOP-17714) | ABFS: testBlobBackCompatibility, testRandomRead & WasbAbfsCompatibility tests fail when triggered with default configs |  Minor | test | Sneha Varma | Sneha Varma |
| [HDFS-16140](https://issues.apache.org/jira/browse/HDFS-16140) | TestBootstrapAliasmap fails by BindException |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-13887](https://issues.apache.org/jira/browse/HADOOP-13887) | Encrypt S3A data client-side with AWS SDK (S3-CSE) |  Minor | fs/s3 | Jeeyoung Kim | Mehakmeet Singh |
| [HADOOP-17458](https://issues.apache.org/jira/browse/HADOOP-17458) | S3A to treat "SdkClientException: Data read has a different length than the expected" as EOFException |  Minor | fs/s3 | Steve Loughran | Bogdan Stolojan |
| [HADOOP-17628](https://issues.apache.org/jira/browse/HADOOP-17628) | Distcp contract test is really slow with ABFS and S3A; timing out |  Minor | fs/azure, fs/s3, test, tools/distcp | Bilahari T H | Steve Loughran |
| [HADOOP-17822](https://issues.apache.org/jira/browse/HADOOP-17822) | fs.s3a.acl.default not working after S3A Audit feature added |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-17139](https://issues.apache.org/jira/browse/HADOOP-17139) | Re-enable optimized copyFromLocal implementation in S3AFileSystem |  Minor | fs/s3 | Sahil Takiar | Bogdan Stolojan |
| [HADOOP-17823](https://issues.apache.org/jira/browse/HADOOP-17823) | S3A Tests to skip if S3Guard and S3-CSE are enabled. |  Major | build, fs/s3 | Mehakmeet Singh | Mehakmeet Singh |
| [HDFS-16184](https://issues.apache.org/jira/browse/HDFS-16184) | De-flake TestBlockScanner#testSkipRecentAccessFile |  Major | . | Viraj Jasani | Viraj Jasani |
| [HADOOP-17677](https://issues.apache.org/jira/browse/HADOOP-17677) | Distcp is unable to determine region with S3 PrivateLink endpoints |  Major | fs/s3, tools/distcp | KJ |  |
| [HDFS-16192](https://issues.apache.org/jira/browse/HDFS-16192) | ViewDistributedFileSystem#rename wrongly using src in the place of dst. |  Major | . | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-17156](https://issues.apache.org/jira/browse/HADOOP-17156) | Clear abfs readahead requests on stream close |  Major | fs/azure | Rajesh Balamohan | Mukund Thakur |
| [HADOOP-17618](https://issues.apache.org/jira/browse/HADOOP-17618) | ABFS: Partially obfuscate SAS object IDs in Logs |  Major | fs/azure | Sumangala Patki | Sumangala Patki |
| [HADOOP-17894](https://issues.apache.org/jira/browse/HADOOP-17894) | CredentialProviderFactory.getProviders() recursion loading JCEKS file from s3a |  Major | conf, fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-17126](https://issues.apache.org/jira/browse/HADOOP-17126) | implement non-guava Precondition checkNotNull |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17195](https://issues.apache.org/jira/browse/HADOOP-17195) | Intermittent OutOfMemory error while performing hdfs CopyFromLocal to abfs |  Major | fs/azure | Mehakmeet Singh | Mehakmeet Singh |
| [HADOOP-17929](https://issues.apache.org/jira/browse/HADOOP-17929) | implement non-guava Precondition checkArgument |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17198](https://issues.apache.org/jira/browse/HADOOP-17198) | Support S3 Access Points |  Major | fs/s3 | Steve Loughran | Bogdan Stolojan |
| [HADOOP-17871](https://issues.apache.org/jira/browse/HADOOP-17871) | S3A CSE: minor tuning |  Minor | fs/s3 | Steve Loughran | Mehakmeet Singh |
| [HADOOP-17947](https://issues.apache.org/jira/browse/HADOOP-17947) | Provide alternative to Guava VisibleForTesting |  Major | . | Viraj Jasani | Viraj Jasani |
| [HADOOP-17930](https://issues.apache.org/jira/browse/HADOOP-17930) | implement non-guava Precondition checkState |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17374](https://issues.apache.org/jira/browse/HADOOP-17374) | AliyunOSS: support ListObjectsV2 |  Major | fs/oss | wujinhu | wujinhu |
| [HADOOP-17863](https://issues.apache.org/jira/browse/HADOOP-17863) | ABFS: Fix compiler deprecation warning in TextFileBasedIdentityHandler |  Minor | fs/azure | Sumangala Patki | Sumangala Patki |
| [HADOOP-17928](https://issues.apache.org/jira/browse/HADOOP-17928) | s3a: set fs.s3a.downgrade.syncable.exceptions = true by default |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-16336](https://issues.apache.org/jira/browse/HDFS-16336) | De-flake TestRollingUpgrade#testRollback |  Minor | hdfs, test | Kevin Wikant | Viraj Jasani |
| [HDFS-16171](https://issues.apache.org/jira/browse/HDFS-16171) | De-flake testDecommissionStatus |  Major | . | Viraj Jasani | Viraj Jasani |
| [HADOOP-17226](https://issues.apache.org/jira/browse/HADOOP-17226) | Failure of ITestAssumeRole.testRestrictedCommitActions |  Minor | fs/s3, test | Steve Loughran | Steve Loughran |
| [HADOOP-14334](https://issues.apache.org/jira/browse/HADOOP-14334) | S3 SSEC  tests to downgrade when running against a mandatory encryption object store |  Minor | fs/s3, test | Steve Loughran | Monthon Klongklaew |
| [HADOOP-16223](https://issues.apache.org/jira/browse/HADOOP-16223) | remove misleading fs.s3a.delegation.tokens.enabled prompt |  Minor | fs/s3 | Steve Loughran |  |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-16078](https://issues.apache.org/jira/browse/HDFS-16078) | Remove unused parameters for DatanodeManager.handleLifeline() |  Minor | . | tomscut | tomscut |
| [HDFS-16079](https://issues.apache.org/jira/browse/HDFS-16079) | Improve the block state change log |  Minor | . | tomscut | tomscut |
| [HDFS-16089](https://issues.apache.org/jira/browse/HDFS-16089) | EC: Add metric EcReconstructionValidateTimeMillis for StripedBlockReconstructor |  Minor | . | tomscut | tomscut |
| [HDFS-16298](https://issues.apache.org/jira/browse/HDFS-16298) | Improve error msg for BlockMissingException |  Minor | . | tomscut | tomscut |
| [HDFS-16312](https://issues.apache.org/jira/browse/HDFS-16312) | Fix typo for DataNodeVolumeMetrics and ProfilingFileIoEvents |  Minor | . | tomscut | tomscut |
| [HADOOP-18005](https://issues.apache.org/jira/browse/HADOOP-18005) | Correct log format for LdapGroupsMapping |  Minor | . | tomscut | tomscut |
| [HDFS-16319](https://issues.apache.org/jira/browse/HDFS-16319) | Add metrics doc for ReadLockLongHoldCount and WriteLockLongHoldCount |  Minor | . | tomscut | tomscut |
| [HDFS-16326](https://issues.apache.org/jira/browse/HDFS-16326) | Simplify the code for DiskBalancer |  Minor | . | tomscut | tomscut |
| [HDFS-16335](https://issues.apache.org/jira/browse/HDFS-16335) | Fix HDFSCommands.md |  Minor | . | tomscut | tomscut |
| [HDFS-16339](https://issues.apache.org/jira/browse/HDFS-16339) | Show the threshold when mover threads quota is exceeded |  Minor | . | tomscut | tomscut |
| [YARN-10820](https://issues.apache.org/jira/browse/YARN-10820) | Make GetClusterNodesRequestPBImpl thread safe |  Major | client | Prabhu Joseph | SwathiChandrashekar |
| [HADOOP-17808](https://issues.apache.org/jira/browse/HADOOP-17808) | ipc.Client not setting interrupt flag after catching InterruptedException |  Minor | . | Viraj Jasani | Viraj Jasani |
| [HADOOP-17834](https://issues.apache.org/jira/browse/HADOOP-17834) | Bump aliyun-sdk-oss to 3.13.0 |  Major | . | Siyao Meng | Siyao Meng |
| [HADOOP-17950](https://issues.apache.org/jira/browse/HADOOP-17950) | Provide replacement for deprecated APIs of commons-io IOUtils |  Major | . | Viraj Jasani | Viraj Jasani |
| [HADOOP-17955](https://issues.apache.org/jira/browse/HADOOP-17955) | Bump netty to the latest 4.1.68 |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-17946](https://issues.apache.org/jira/browse/HADOOP-17946) | Update commons-lang to latest 3.x |  Minor | . | Sean Busbey | Renukaprasad C |
| [HDFS-16323](https://issues.apache.org/jira/browse/HDFS-16323) | DatanodeHttpServer doesn't require handler state map while retrieving filter handlers |  Minor | . | Viraj Jasani | Viraj Jasani |
| [HADOOP-13464](https://issues.apache.org/jira/browse/HADOOP-13464) | update GSON to 2.7+ |  Minor | build | Sean Busbey | Igor Dvorzhak |
| [HADOOP-18061](https://issues.apache.org/jira/browse/HADOOP-18061) | Update the year to 2022 |  Major | . | Ayush Saxena | Ayush Saxena |


