
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

## Release 2.7.3 - 2016-08-25

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-11252](https://issues.apache.org/jira/browse/HADOOP-11252) | RPC client does not time out by default |  Critical | ipc | Wilfred Spiegelenburg | Masatake Iwasaki |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-4785](https://issues.apache.org/jira/browse/YARN-4785) | inconsistent value type of the "type" field for LeafQueueInfo in response of RM REST API - cluster/scheduler |  Major | webapp | Jayesh | Varun Vasudev |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-2580](https://issues.apache.org/jira/browse/HDFS-2580) | NameNode#main(...) can make use of GenericOptionsParser. |  Minor | namenode | Harsh J | Harsh J |
| [HDFS-8101](https://issues.apache.org/jira/browse/HDFS-8101) | DFSClient use of non-constant DFSConfigKeys pulls in WebHDFS classes at runtime |  Minor | hdfs-client | Sean Busbey | Sean Busbey |
| [YARN-3404](https://issues.apache.org/jira/browse/YARN-3404) | View the queue name to YARN Application page |  Minor | . | Ryu Kobayashi | Ryu Kobayashi |
| [HDFS-8647](https://issues.apache.org/jira/browse/HDFS-8647) | Abstract BlockManager's rack policy into BlockPlacementPolicy |  Major | . | Ming Ma | Brahma Reddy Battula |
| [MAPREDUCE-5485](https://issues.apache.org/jira/browse/MAPREDUCE-5485) | Allow repeating job commit by extending OutputCommitter API |  Critical | . | Nemon Lou | Junping Du |
| [HDFS-9314](https://issues.apache.org/jira/browse/HDFS-9314) | Improve BlockPlacementPolicyDefault's picking of excess replicas |  Major | . | Ming Ma | Xiao Chen |
| [MAPREDUCE-6436](https://issues.apache.org/jira/browse/MAPREDUCE-6436) | JobHistory cache issue |  Blocker | . | Ryu Kobayashi | Kai Sasaki |
| [HDFS-9198](https://issues.apache.org/jira/browse/HDFS-9198) | Coalesce IBR processing in the NN |  Major | namenode | Daryn Sharp | Daryn Sharp |
| [HADOOP-12570](https://issues.apache.org/jira/browse/HADOOP-12570) | HDFS Secure Mode Documentation updates |  Major | documentation | Arpit Agarwal | Arpit Agarwal |
| [HDFS-9569](https://issues.apache.org/jira/browse/HDFS-9569) | Log the name of the fsimage being loaded for better supportability |  Trivial | namenode | Yongjun Zhang | Yongjun Zhang |
| [HDFS-9415](https://issues.apache.org/jira/browse/HDFS-9415) | Document dfs.cluster.administrators and dfs.permissions.superusergroup |  Major | documentation | Arpit Agarwal | Xiaobing Zhou |
| [YARN-4492](https://issues.apache.org/jira/browse/YARN-4492) | Add documentation for preemption supported in Capacity scheduler |  Minor | capacity scheduler | Naganarasimha G R | Naganarasimha G R |
| [HDFS-9654](https://issues.apache.org/jira/browse/HDFS-9654) | Code refactoring for HDFS-8578 |  Minor | datanode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-9669](https://issues.apache.org/jira/browse/HDFS-9669) | TcpPeerServer should respect ipc.server.listen.queue.size |  Major | . | Elliott Clark | Elliott Clark |
| [HDFS-9629](https://issues.apache.org/jira/browse/HDFS-9629) | Update the footer of Web UI to show year 2016 |  Major | . | Xiao Chen | Xiao Chen |
| [HADOOP-12772](https://issues.apache.org/jira/browse/HADOOP-12772) | NetworkTopologyWithNodeGroup.getNodeGroup() can loop infinitely for invalid 'loc' values |  Minor | . | Kuhu Shukla | Kuhu Shukla |
| [HADOOP-12805](https://issues.apache.org/jira/browse/HADOOP-12805) | Annotate CanUnbuffer with @InterfaceAudience.Public |  Major | . | Ted Yu | Ted Yu |
| [YARN-4690](https://issues.apache.org/jira/browse/YARN-4690) | Skip object allocation in FSAppAttempt#getResourceUsage when possible |  Major | . | Ming Ma | Ming Ma |
| [HDFS-4946](https://issues.apache.org/jira/browse/HDFS-4946) | Allow preferLocalNode in BlockPlacementPolicyDefault to be configurable |  Major | namenode | James Kinley | Nathan Roberts |
| [HADOOP-12794](https://issues.apache.org/jira/browse/HADOOP-12794) | Support additional compression levels for GzipCodec |  Major | io | Ravi Mutyala | Ravi Mutyala |
| [HADOOP-12800](https://issues.apache.org/jira/browse/HADOOP-12800) | Copy docker directory from 2.8 to 2.7/2.6 repos to enable pre-commit Jenkins runs |  Major | build, yetus | Zhe Zhang | Zhe Zhang |
| [HDFS-8578](https://issues.apache.org/jira/browse/HDFS-8578) | On upgrade, Datanode should process all storage/data dirs in parallel |  Critical | datanode | Raju Bairishetti | Vinayakumar B |
| [MAPREDUCE-6622](https://issues.apache.org/jira/browse/MAPREDUCE-6622) | Add capability to set JHS job cache to a task-based limit |  Critical | jobhistoryserver | Ray Chiang | Ray Chiang |
| [HDFS-9906](https://issues.apache.org/jira/browse/HDFS-9906) | Remove spammy log spew when a datanode is restarted |  Major | namenode | Elliott Clark | Brahma Reddy Battula |
| [HADOOP-12789](https://issues.apache.org/jira/browse/HADOOP-12789) | log classpath of ApplicationClassLoader at INFO level |  Minor | util | Sangjin Lee | Sangjin Lee |
| [HDFS-9860](https://issues.apache.org/jira/browse/HDFS-9860) | Backport HDFS-9638 to branch-2.7. |  Major | distcp, documentation | Gary Steelman | Wei-Chiu Chuang |
| [HDFS-10264](https://issues.apache.org/jira/browse/HDFS-10264) | Logging improvements in FSImageFormatProtobuf.Saver |  Major | namenode | Konstantin Shvachko | Xiaobing Zhou |
| [HADOOP-13039](https://issues.apache.org/jira/browse/HADOOP-13039) | Add documentation for configuration property ipc.maximum.data.length for controlling maximum RPC message size. |  Major | documentation | Chris Nauroth | Mingliang Liu |
| [HADOOP-13103](https://issues.apache.org/jira/browse/HADOOP-13103) | Group resolution from LDAP may fail on javax.naming.ServiceUnavailableException |  Minor | security | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-10488](https://issues.apache.org/jira/browse/HDFS-10488) | Update WebHDFS documentation regarding CREATE and MKDIR default permissions |  Minor | documentation, webhdfs | Wellington Chevreuil | Wellington Chevreuil |
| [HADOOP-13298](https://issues.apache.org/jira/browse/HADOOP-13298) | Fix the leftover L&N files in hadoop-build-tools/src/main/resources/META-INF/ |  Minor | . | Xiao Chen | Tsuyoshi Ozawa |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-8818](https://issues.apache.org/jira/browse/HADOOP-8818) | Should use equals() rather than == to compare String or Text in MD5MD5CRC32FileChecksum and TFileDumper |  Minor | fs, io | Brandon Li | Brandon Li |
| [HADOOP-9121](https://issues.apache.org/jira/browse/HADOOP-9121) | InodeTree.java has redundant check for vName while throwing exception |  Major | fs | Arup Malakar | Arup Malakar |
| [HADOOP-7817](https://issues.apache.org/jira/browse/HADOOP-7817) | RawLocalFileSystem.append() should give FSDataOutputStream with accurate .getPos() |  Major | fs | Kristofer Tomasette | Kanaka Kumar Avvaru |
| [MAPREDUCE-6413](https://issues.apache.org/jira/browse/MAPREDUCE-6413) | TestLocalJobSubmission is failing with unknown host |  Major | test | Jason Lowe | zhihai xu |
| [YARN-3695](https://issues.apache.org/jira/browse/YARN-3695) | ServerProxy (NMProxy, etc.) shouldn't retry forever for non network exception. |  Major | . | Junping Du | Raju Bairishetti |
| [HADOOP-12107](https://issues.apache.org/jira/browse/HADOOP-12107) | long running apps may have a huge number of StatisticsData instances under FileSystem |  Critical | fs | Sangjin Lee | Sangjin Lee |
| [YARN-3849](https://issues.apache.org/jira/browse/YARN-3849) | Too much of preemption activity causing continuos killing of containers across queues |  Critical | capacityscheduler | Sunil G | Sunil G |
| [HDFS-8772](https://issues.apache.org/jira/browse/HDFS-8772) | Fix TestStandbyIsHot#testDatanodeRestarts which occasionally fails |  Major | . | Walter Su | Walter Su |
| [MAPREDUCE-5817](https://issues.apache.org/jira/browse/MAPREDUCE-5817) | Mappers get rescheduled on node transition even after all reducers are completed |  Major | applicationmaster | Sangjin Lee | Sangjin Lee |
| [HDFS-8845](https://issues.apache.org/jira/browse/HDFS-8845) | DiskChecker should not traverse the entire tree |  Major | . | Chang Li | Chang Li |
| [YARN-4121](https://issues.apache.org/jira/browse/YARN-4121) | Typos in capacity scheduler documentation. |  Trivial | documentation | Kai Sasaki | Kai Sasaki |
| [HDFS-8581](https://issues.apache.org/jira/browse/HDFS-8581) | ContentSummary on / skips further counts on yielding lock |  Minor | namenode | tongshiquan | J.Andreina |
| [HADOOP-12348](https://issues.apache.org/jira/browse/HADOOP-12348) | MetricsSystemImpl creates MetricsSourceAdapter with wrong time unit parameter. |  Major | metrics | zhihai xu | zhihai xu |
| [HADOOP-12374](https://issues.apache.org/jira/browse/HADOOP-12374) | Description of hdfs expunge command is confusing |  Major | documentation, trash | Weiwei Yang | Weiwei Yang |
| [HDFS-9072](https://issues.apache.org/jira/browse/HDFS-9072) | Fix random failures in TestJMXGet |  Critical | test | J.Andreina | J.Andreina |
| [MAPREDUCE-6460](https://issues.apache.org/jira/browse/MAPREDUCE-6460) | TestRMContainerAllocator.testAttemptNotFoundCausesRMCommunicatorException fails |  Major | test | zhihai xu | zhihai xu |
| [MAPREDUCE-6302](https://issues.apache.org/jira/browse/MAPREDUCE-6302) | Preempt reducers after a configurable timeout irrespective of headroom |  Critical | . | mai shurong | Karthik Kambatla |
| [YARN-4288](https://issues.apache.org/jira/browse/YARN-4288) | NodeManager restart should keep retrying to register to RM while connection exception happens during RM failed over. |  Critical | nodemanager | Junping Du | Junping Du |
| [HDFS-9313](https://issues.apache.org/jira/browse/HDFS-9313) | Possible NullPointerException in BlockManager if no excess replica can be chosen |  Major | . | Ming Ma | Ming Ma |
| [HADOOP-12296](https://issues.apache.org/jira/browse/HADOOP-12296) | when setnetgrent returns 0 in linux, exception should be thrown |  Major | . | Chang Li | Chang Li |
| [HDFS-4937](https://issues.apache.org/jira/browse/HDFS-4937) | ReplicationMonitor can infinite-loop in BlockPlacementPolicyDefault#chooseRandom() |  Major | namenode | Kihwal Lee | Kihwal Lee |
| [HDFS-6481](https://issues.apache.org/jira/browse/HDFS-6481) | DatanodeManager#getDatanodeStorageInfos() should check the length of storageIDs |  Minor | namenode | Ted Yu | Tsz Wo Nicholas Sze |
| [HDFS-9383](https://issues.apache.org/jira/browse/HDFS-9383) | TestByteArrayManager#testByteArrayManager fails |  Major | . | Kihwal Lee | Tsz Wo Nicholas Sze |
| [HADOOP-12482](https://issues.apache.org/jira/browse/HADOOP-12482) | Race condition in JMX cache update |  Major | . | Tony Wu | Tony Wu |
| [YARN-4347](https://issues.apache.org/jira/browse/YARN-4347) | Resource manager fails with Null pointer exception |  Major | yarn | Yesha Vora | Jian He |
| [HADOOP-12545](https://issues.apache.org/jira/browse/HADOOP-12545) | Hadoop javadoc has broken links for AccessControlList, ImpersonationProvider, DefaultImpersonationProvider, and DistCp |  Major | documentation | Mohammad Arshad | Mohammad Arshad |
| [YARN-4374](https://issues.apache.org/jira/browse/YARN-4374) | RM capacity scheduler UI rounds user limit factor |  Major | capacityscheduler | Chang Li | Chang Li |
| [YARN-3769](https://issues.apache.org/jira/browse/YARN-3769) | Consider user limit when calculating total pending resource for preemption policy in Capacity Scheduler |  Major | capacityscheduler | Eric Payne | Eric Payne |
| [YARN-4380](https://issues.apache.org/jira/browse/YARN-4380) | TestResourceLocalizationService.testDownloadingResourcesOnContainerKill fails intermittently |  Major | test | Tsuyoshi Ozawa | Varun Saxena |
| [YARN-4398](https://issues.apache.org/jira/browse/YARN-4398) | Yarn recover functionality causes the cluster running slowly and the cluster usage rate is far below 100 |  Major | resourcemanager | NING DING | NING DING |
| [HADOOP-12565](https://issues.apache.org/jira/browse/HADOOP-12565) | Replace DSA with RSA for SSH key type in SingleCluster.md |  Minor | documentation | Alexander Veit | Mingliang Liu |
| [YARN-4422](https://issues.apache.org/jira/browse/YARN-4422) | Generic AHS sometimes doesn't show started, node, or logs on App page |  Major | . | Eric Payne | Eric Payne |
| [HADOOP-12602](https://issues.apache.org/jira/browse/HADOOP-12602) | TestMetricsSystemImpl#testQSize occasionally fail |  Major | test | Wei-Chiu Chuang | Masatake Iwasaki |
| [YARN-4439](https://issues.apache.org/jira/browse/YARN-4439) | Clarify NMContainerStatus#toString method. |  Major | . | Jian He | Jian He |
| [HDFS-9516](https://issues.apache.org/jira/browse/HDFS-9516) | truncate file fails with data dirs on multiple disks |  Major | datanode | Bogdan Raducanu | Plamen Jeliazkov |
| [YARN-4452](https://issues.apache.org/jira/browse/YARN-4452) | NPE when submit Unmanaged application |  Critical | . | Naganarasimha G R | Naganarasimha G R |
| [HDFS-9533](https://issues.apache.org/jira/browse/HDFS-9533) | seen\_txid in the shared edits directory is modified during bootstrapping |  Major | ha, namenode | Kihwal Lee | Kihwal Lee |
| [HDFS-9347](https://issues.apache.org/jira/browse/HDFS-9347) | Invariant assumption in TestQuorumJournalManager.shutdown() is wrong |  Major | test | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-12636](https://issues.apache.org/jira/browse/HADOOP-12636) | Prevent ServiceLoader failure init for unused FileSystems |  Major | fs | Íñigo Goiri | Íñigo Goiri |
| [MAPREDUCE-6583](https://issues.apache.org/jira/browse/MAPREDUCE-6583) | Clarify confusing sentence in MapReduce tutorial document |  Minor | documentation | chris snow | Kai Sasaki |
| [HDFS-9505](https://issues.apache.org/jira/browse/HDFS-9505) | HDFS Architecture documentation needs to be refreshed. |  Major | documentation | Chris Nauroth | Masatake Iwasaki |
| [HDFS-7163](https://issues.apache.org/jira/browse/HDFS-7163) | WebHdfsFileSystem should retry reads according to the configured retry policy. |  Major | webhdfs | Eric Payne | Eric Payne |
| [HADOOP-12559](https://issues.apache.org/jira/browse/HADOOP-12559) | KMS connection failures should trigger TGT renewal |  Major | security | Zhe Zhang | Zhe Zhang |
| [HADOOP-12682](https://issues.apache.org/jira/browse/HADOOP-12682) | Fix TestKMS#testKMSRestart\* failure |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [MAPREDUCE-6577](https://issues.apache.org/jira/browse/MAPREDUCE-6577) | MR AM unable to load native library without MR\_AM\_ADMIN\_USER\_ENV set |  Critical | mr-am | Sangjin Lee | Sangjin Lee |
| [YARN-4546](https://issues.apache.org/jira/browse/YARN-4546) | ResourceManager crash due to scheduling opportunity overflow |  Critical | resourcemanager | Jason Lowe | Jason Lowe |
| [HDFS-9600](https://issues.apache.org/jira/browse/HDFS-9600) | do not check replication if the block is under construction |  Critical | . | Phil Yang | Phil Yang |
| [HADOOP-12613](https://issues.apache.org/jira/browse/HADOOP-12613) | TestFind.processArguments occasionally fails |  Major | test | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-4414](https://issues.apache.org/jira/browse/YARN-4414) | Nodemanager connection errors are retried at multiple levels |  Major | nodemanager | Jason Lowe | Chang Li |
| [HDFS-9648](https://issues.apache.org/jira/browse/HDFS-9648) | TestStartup.testImageChecksum is broken by HDFS-9569's message change |  Trivial | namenode | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-12706](https://issues.apache.org/jira/browse/HADOOP-12706) | TestLocalFsFCStatistics#testStatisticsThreadLocalDataCleanUp times out occasionally |  Major | test | Jason Lowe | Sangjin Lee |
| [YARN-4581](https://issues.apache.org/jira/browse/YARN-4581) | AHS writer thread leak makes RM crash while RM is recovering |  Major | resourcemanager | sandflee | sandflee |
| [MAPREDUCE-6554](https://issues.apache.org/jira/browse/MAPREDUCE-6554) | MRAppMaster servicestart failing  with NPE in MRAppMaster#parsePreviousJobHistory |  Critical | . | Bibin A Chundatt | Bibin A Chundatt |
| [HDFS-9661](https://issues.apache.org/jira/browse/HDFS-9661) | Deadlock in DN.FsDatasetImpl between moveBlockAcrossStorage and createRbw |  Major | datanode | ade | ade |
| [HDFS-9625](https://issues.apache.org/jira/browse/HDFS-9625) | set replication for empty file  failed when set storage policy |  Major | namenode | DENG FEI | DENG FEI |
| [HDFS-9634](https://issues.apache.org/jira/browse/HDFS-9634) | webhdfs client side exceptions don't provide enough details |  Major | webhdfs | Eric Payne | Eric Payne |
| [YARN-4610](https://issues.apache.org/jira/browse/YARN-4610) | Reservations continue looking for one app causes other apps to starve |  Blocker | capacityscheduler | Jason Lowe | Jason Lowe |
| [YARN-4598](https://issues.apache.org/jira/browse/YARN-4598) | Invalid event: RESOURCE\_FAILED at CONTAINER\_CLEANEDUP\_AFTER\_KILL |  Major | nodemanager | tangshangwen | tangshangwen |
| [HDFS-9690](https://issues.apache.org/jira/browse/HDFS-9690) | ClientProtocol.addBlock is not idempotent after HDFS-8071 |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [MAPREDUCE-6619](https://issues.apache.org/jira/browse/MAPREDUCE-6619) | HADOOP\_CLASSPATH is overwritten in MR container |  Major | mrv2 | shanyu zhao | Junping Du |
| [YARN-4428](https://issues.apache.org/jira/browse/YARN-4428) | Redirect RM page to AHS page when AHS turned on and RM page is not available |  Major | . | Chang Li | Chang Li |
| [MAPREDUCE-6618](https://issues.apache.org/jira/browse/MAPREDUCE-6618) | YarnClientProtocolProvider leaking the YarnClient thread. |  Major | . | Xuan Gong | Xuan Gong |
| [YARN-3102](https://issues.apache.org/jira/browse/YARN-3102) | Decommisioned Nodes not listed in Web UI |  Minor | resourcemanager | Bibin A Chundatt | Kuhu Shukla |
| [HDFS-9406](https://issues.apache.org/jira/browse/HDFS-9406) | FSImage may get corrupted after deleting snapshot |  Major | namenode | Stanislav Antic | Yongjun Zhang |
| [MAPREDUCE-6621](https://issues.apache.org/jira/browse/MAPREDUCE-6621) | Memory Leak in JobClient#submitJobInternal() |  Major | . | Xuan Gong | Xuan Gong |
| [HDFS-9740](https://issues.apache.org/jira/browse/HDFS-9740) | Use a reasonable limit in DFSTestUtil.waitForMetric() |  Major | test | Kihwal Lee | Chang Li |
| [HADOOP-12761](https://issues.apache.org/jira/browse/HADOOP-12761) | incremental maven build is not really incremental |  Minor | build | Sangjin Lee | Sangjin Lee |
| [HDFS-9743](https://issues.apache.org/jira/browse/HDFS-9743) | Fix TestLazyPersistFiles#testFallbackToDiskFull in branch-2.7 |  Major | . | Kihwal Lee | Kihwal Lee |
| [HDFS-9730](https://issues.apache.org/jira/browse/HDFS-9730) | Storage ID update does not happen when there is a layout change |  Major | datanode | Kihwal Lee | Tsz Wo Nicholas Sze |
| [HDFS-9724](https://issues.apache.org/jira/browse/HDFS-9724) | Degraded performance in WebHDFS listing as it does not reuse ObjectMapper |  Critical | performance | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-12773](https://issues.apache.org/jira/browse/HADOOP-12773) | HBase classes fail to load with client/job classloader enabled |  Major | util | Sangjin Lee | Sangjin Lee |
| [HDFS-9784](https://issues.apache.org/jira/browse/HDFS-9784) | Example usage is not correct in Transparent Encryption document |  Major | documentation | Takashi Ohnishi | Takashi Ohnishi |
| [HDFS-9752](https://issues.apache.org/jira/browse/HDFS-9752) | Permanent write failures may happen to slow writers during datanode rolling upgrades |  Critical | . | Kihwal Lee | Walter Su |
| [HDFS-9779](https://issues.apache.org/jira/browse/HDFS-9779) | TestReplicationPolicyWithNodeGroup NODE variable picks wrong rack value |  Minor | test | Kuhu Shukla | Kuhu Shukla |
| [HADOOP-12792](https://issues.apache.org/jira/browse/HADOOP-12792) | TestUserGroupInformation#testGetServerSideGroups fails in chroot |  Minor | security, test | Eric Badger | Eric Badger |
| [HADOOP-12589](https://issues.apache.org/jira/browse/HADOOP-12589) | Fix intermittent test failure of TestCopyPreserveFlag |  Major | test | Tsuyoshi Ozawa | Masatake Iwasaki |
| [HADOOP-12786](https://issues.apache.org/jira/browse/HADOOP-12786) | "hadoop key" command usage is not documented |  Major | documentation | Akira Ajisaka | Xiao Chen |
| [HDFS-9765](https://issues.apache.org/jira/browse/HDFS-9765) | TestBlockScanner#testVolumeIteratorWithCaching fails intermittently |  Major | test | Mingliang Liu | Akira Ajisaka |
| [HADOOP-12810](https://issues.apache.org/jira/browse/HADOOP-12810) | FileSystem#listLocatedStatus causes unnecessary RPC calls |  Major | fs, fs/s3 | Ryan Blue | Ryan Blue |
| [MAPREDUCE-6637](https://issues.apache.org/jira/browse/MAPREDUCE-6637) | Testcase Failure : TestFileInputFormat.testSplitLocationInfo |  Major | test | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-4707](https://issues.apache.org/jira/browse/YARN-4707) | Remove the extra char (\>) from SecureContainer.md |  Major | documentation | Brahma Reddy Battula | Brahma Reddy Battula |
| [MAPREDUCE-6635](https://issues.apache.org/jira/browse/MAPREDUCE-6635) | Unsafe long to int conversion in UncompressedSplitLineReader and IndexOutOfBoundsException |  Critical | . | Sergey Shelukhin | Junping Du |
| [YARN-2046](https://issues.apache.org/jira/browse/YARN-2046) | Out of band heartbeats are sent only on container kill and possibly too early |  Major | nodemanager | Jason Lowe | Ming Ma |
| [YARN-4722](https://issues.apache.org/jira/browse/YARN-4722) | AsyncDispatcher logs redundant event queue sizes |  Major | . | Jason Lowe | Jason Lowe |
| [YARN-4723](https://issues.apache.org/jira/browse/YARN-4723) | NodesListManager$UnknownNodeId ClassCastException |  Critical | resourcemanager | Jason Lowe | Kuhu Shukla |
| [HDFS-9864](https://issues.apache.org/jira/browse/HDFS-9864) | Correct reference for RENEWDELEGATIONTOKEN and CANCELDELEGATIONTOKEN in webhdfs doc |  Major | documentation | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-9880](https://issues.apache.org/jira/browse/HDFS-9880) | TestDatanodeRegistration fails occasionally |  Major | test | Kihwal Lee | Kihwal Lee |
| [HDFS-9766](https://issues.apache.org/jira/browse/HDFS-9766) | TestDataNodeMetrics#testDataNodeTimeSpend fails intermittently |  Major | test | Mingliang Liu | Xiao Chen |
| [HDFS-9851](https://issues.apache.org/jira/browse/HDFS-9851) | Name node throws NPE when setPermission is called on a path that does not exist |  Critical | namenode | David Yan | Brahma Reddy Battula |
| [HADOOP-12870](https://issues.apache.org/jira/browse/HADOOP-12870) | Fix typo admininistration in CommandsManual.md |  Minor | documentation | Akira Ajisaka | John Zhuge |
| [HDFS-9048](https://issues.apache.org/jira/browse/HDFS-9048) | DistCp documentation is out-of-dated |  Major | . | Haohui Mai | Daisuke Kobayashi |
| [HADOOP-12871](https://issues.apache.org/jira/browse/HADOOP-12871) | Fix dead link to NativeLibraries.html in CommandsManual.md |  Minor | documentation | Akira Ajisaka | Brahma Reddy Battula |
| [HADOOP-12872](https://issues.apache.org/jira/browse/HADOOP-12872) | Fix formatting in ServiceLevelAuth.md |  Trivial | documentation | Akira Ajisaka | Brahma Reddy Battula |
| [MAPREDUCE-4785](https://issues.apache.org/jira/browse/MAPREDUCE-4785) | TestMRApp occasionally fails |  Major | mrv2, test | Jason Lowe | Haibo Chen |
| [YARN-4761](https://issues.apache.org/jira/browse/YARN-4761) | NMs reconnecting with changed capabilities can lead to wrong cluster resource calculations on fair scheduler |  Major | fairscheduler | Sangjin Lee | Sangjin Lee |
| [YARN-4760](https://issues.apache.org/jira/browse/YARN-4760) | proxy redirect to history server uses wrong URL |  Major | webapp | Jason Lowe | Eric Badger |
| [HDFS-9865](https://issues.apache.org/jira/browse/HDFS-9865) | TestBlockReplacement fails intermittently in trunk |  Major | test | Yiqun Lin | Yiqun Lin |
| [HDFS-9812](https://issues.apache.org/jira/browse/HDFS-9812) | Streamer threads leak if failure happens when closing DFSOutputStream |  Major | hdfs-client | Yiqun Lin | Yiqun Lin |
| [HADOOP-12688](https://issues.apache.org/jira/browse/HADOOP-12688) | Fix deadlinks in Compatibility.md |  Major | documentation | Akira Ajisaka | Gabor Liptak |
| [HDFS-9904](https://issues.apache.org/jira/browse/HDFS-9904) | testCheckpointCancellationDuringUpload occasionally fails |  Major | test | Kihwal Lee | Yiqun Lin |
| [MAPREDUCE-6645](https://issues.apache.org/jira/browse/MAPREDUCE-6645) | TestWordStats outputs logs under directories other than target/test-dir |  Major | test | Akira Ajisaka | Gabor Liptak |
| [HDFS-9874](https://issues.apache.org/jira/browse/HDFS-9874) | Long living DataXceiver threads cause volume shutdown to block. |  Critical | datanode | Rushabh S Shah | Rushabh S Shah |
| [YARN-4686](https://issues.apache.org/jira/browse/YARN-4686) | MiniYARNCluster.start() returns before cluster is completely started |  Major | test | Rohith Sharma K S | Eric Badger |
| [MAPREDUCE-6363](https://issues.apache.org/jira/browse/MAPREDUCE-6363) | [NNBench] Lease mismatch error when running with multiple mappers |  Critical | benchmarks | Brahma Reddy Battula | Bibin A Chundatt |
| [MAPREDUCE-6580](https://issues.apache.org/jira/browse/MAPREDUCE-6580) | Test failure : TestMRJobsWithProfiler |  Major | . | Rohith Sharma K S | Eric Badger |
| [MAPREDUCE-6656](https://issues.apache.org/jira/browse/MAPREDUCE-6656) | [NNBench] OP\_DELETE operation isn't working after MAPREDUCE-6363 |  Blocker | . | J.Andreina | J.Andreina |
| [YARN-4850](https://issues.apache.org/jira/browse/YARN-4850) | test-fair-scheduler.xml isn't valid xml |  Blocker | fairscheduler, test | Allen Wittenauer | Yufei Gu |
| [HADOOP-12958](https://issues.apache.org/jira/browse/HADOOP-12958) | PhantomReference for filesystem statistics can trigger OOM |  Major | . | Jason Lowe | Sangjin Lee |
| [HDFS-10182](https://issues.apache.org/jira/browse/HDFS-10182) | Hedged read might overwrite user's buf |  Major | . | zhouyingchao | zhouyingchao |
| [YARN-4773](https://issues.apache.org/jira/browse/YARN-4773) | Log aggregation performs extraneous filesystem operations when rolling log aggregation is disabled |  Minor | nodemanager | Jason Lowe | Jun Gong |
| [HDFS-9478](https://issues.apache.org/jira/browse/HDFS-9478) | Reason for failing ipc.FairCallQueue contruction should be thrown |  Minor | . | Archana T | Ajith S |
| [HADOOP-12902](https://issues.apache.org/jira/browse/HADOOP-12902) | JavaDocs for SignerSecretProvider are out-of-date in AuthenticationFilter |  Major | documentation | Robert Kanter | Gabor Liptak |
| [YARN-4183](https://issues.apache.org/jira/browse/YARN-4183) | Clarify the behavior of timeline service config properties |  Major | . | Mit Desai | Naganarasimha G R |
| [YARN-4706](https://issues.apache.org/jira/browse/YARN-4706) | UI Hosting Configuration in TimelineServer doc is broken |  Critical | documentation | Akira Ajisaka | Akira Ajisaka |
| [HDFS-10178](https://issues.apache.org/jira/browse/HDFS-10178) | Permanent write failures can happen if pipeline recoveries occur for the first packet |  Critical | . | Kihwal Lee | Kihwal Lee |
| [HDFS-9917](https://issues.apache.org/jira/browse/HDFS-9917) | IBR accumulate more objects when SNN was down for sometime. |  Critical | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-10239](https://issues.apache.org/jira/browse/HDFS-10239) | Fsshell mv fails if port usage doesn't match in src and destination paths |  Major | . | Kuhu Shukla | Kuhu Shukla |
| [YARN-4915](https://issues.apache.org/jira/browse/YARN-4915) | Fix typo in YARN Secure Containers documentation |  Trivial | documentation, yarn | Takashi Ohnishi | Takashi Ohnishi |
| [YARN-4917](https://issues.apache.org/jira/browse/YARN-4917) | Fix typos in documentation of Capacity Scheduler. |  Minor | documentation | Takashi Ohnishi | Takashi Ohnishi |
| [HDFS-10261](https://issues.apache.org/jira/browse/HDFS-10261) | TestBookKeeperHACheckpoints doesn't handle ephemeral HTTP ports |  Major | . | Eric Badger | Eric Badger |
| [MAPREDUCE-6670](https://issues.apache.org/jira/browse/MAPREDUCE-6670) | TestJobListCache#testEviction sometimes fails on Windows with timeout |  Minor | test | Gergely Novák | Gergely Novák |
| [MAPREDUCE-6633](https://issues.apache.org/jira/browse/MAPREDUCE-6633) | AM should retry map attempts if the reduce task encounters commpression related errors. |  Major | . | Rushabh S Shah | Rushabh S Shah |
| [YARN-4938](https://issues.apache.org/jira/browse/YARN-4938) | MiniYarnCluster should not request transitionToActive to RM on non-HA environment |  Major | test | Akira Ajisaka | Eric Badger |
| [HADOOP-12406](https://issues.apache.org/jira/browse/HADOOP-12406) | AbstractMapWritable.readFields throws ClassNotFoundException with custom writables |  Blocker | io | Nadeem Douba | Nadeem Douba |
| [HDFS-10271](https://issues.apache.org/jira/browse/HDFS-10271) | Extra bytes are getting released from reservedSpace for append |  Critical | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-4794](https://issues.apache.org/jira/browse/YARN-4794) | Deadlock in NMClientImpl |  Critical | . | Sumana Sathish | Jian He |
| [HDFS-9772](https://issues.apache.org/jira/browse/HDFS-9772) | TestBlockReplacement#testThrottler doesn't work as expected |  Minor | test | Yiqun Lin | Yiqun Lin |
| [YARN-4924](https://issues.apache.org/jira/browse/YARN-4924) | NM recovery race can lead to container not cleaned up |  Major | nodemanager | Nathan Roberts | sandflee |
| [HADOOP-12989](https://issues.apache.org/jira/browse/HADOOP-12989) | Some tests in org.apache.hadoop.fs.shell.find occasionally time out |  Major | test | Akira Ajisaka | Takashi Ohnishi |
| [YARN-4940](https://issues.apache.org/jira/browse/YARN-4940) | yarn node -list -all failed if RM start with decommissioned node |  Major | . | sandflee | sandflee |
| [HDFS-10275](https://issues.apache.org/jira/browse/HDFS-10275) | TestDataNodeMetrics failing intermittently due to TotalWriteTime counted incorrectly |  Major | test | Yiqun Lin | Yiqun Lin |
| [MAPREDUCE-6680](https://issues.apache.org/jira/browse/MAPREDUCE-6680) | JHS UserLogDir scan algorithm sometime could skip directory with update in CloudFS (Azure FileSystem, S3, etc.) |  Major | jobhistoryserver | Junping Du | Junping Du |
| [HADOOP-13042](https://issues.apache.org/jira/browse/HADOOP-13042) | Restore lost leveldbjni LICENSE and NOTICE changes |  Major | . | Andrew Wang | Andrew Wang |
| [HADOOP-13043](https://issues.apache.org/jira/browse/HADOOP-13043) | Add LICENSE.txt entries for bundled javascript dependencies |  Major | . | Andrew Wang | Andrew Wang |
| [HDFS-10319](https://issues.apache.org/jira/browse/HDFS-10319) | Balancer should not try to pair storages with different types |  Minor | balancer & mover | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-9555](https://issues.apache.org/jira/browse/HDFS-9555) | LazyPersistFileScrubber should still sleep if there are errors in the clear progress |  Major | . | Phil Yang | Phil Yang |
| [HADOOP-13052](https://issues.apache.org/jira/browse/HADOOP-13052) | ChecksumFileSystem mishandles crc file permissions |  Major | fs | Daryn Sharp | Daryn Sharp |
| [HDFS-9905](https://issues.apache.org/jira/browse/HDFS-9905) | WebHdfsFileSystem#runWithRetry should display original stack trace on error |  Major | test | Kihwal Lee | Wei-Chiu Chuang |
| [HDFS-10245](https://issues.apache.org/jira/browse/HDFS-10245) | Fix the findbug warnings in branch-2.7 |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-4556](https://issues.apache.org/jira/browse/YARN-4556) |  TestFifoScheduler.testResourceOverCommit fails |  Major | scheduler, test | Akihiro Suda | Akihiro Suda |
| [HDFS-9958](https://issues.apache.org/jira/browse/HDFS-9958) | BlockManager#createLocatedBlocks can throw NPE for corruptBlocks on failed storages. |  Major | . | Kuhu Shukla | Kuhu Shukla |
| [YARN-5008](https://issues.apache.org/jira/browse/YARN-5008) | LeveldbRMStateStore database can grow substantially leading to long recovery times |  Major | resourcemanager | Jason Lowe | Jason Lowe |
| [YARN-5009](https://issues.apache.org/jira/browse/YARN-5009) | NMLeveldbStateStoreService database can grow substantially leading to longer recovery times |  Major | nodemanager | Jason Lowe | Jason Lowe |
| [HDFS-10335](https://issues.apache.org/jira/browse/HDFS-10335) | Mover$Processor#chooseTarget() always chooses the first matching target storage group |  Critical | balancer & mover | Mingliang Liu | Mingliang Liu |
| [HDFS-10347](https://issues.apache.org/jira/browse/HDFS-10347) | Namenode report bad block method doesn't log the bad block or datanode. |  Minor | namenode | Rushabh S Shah | Rushabh S Shah |
| [YARN-4834](https://issues.apache.org/jira/browse/YARN-4834) | ProcfsBasedProcessTree doesn't track daemonized processes |  Major | nodemanager | Nathan Roberts | Nathan Roberts |
| [MAPREDUCE-6514](https://issues.apache.org/jira/browse/MAPREDUCE-6514) | Job hangs as ask is not updated after ramping down of all reducers |  Blocker | applicationmaster | Varun Saxena | Varun Saxena |
| [HDFS-2043](https://issues.apache.org/jira/browse/HDFS-2043) | TestHFlush failing intermittently |  Major | test | Aaron T. Myers | Yiqun Lin |
| [MAPREDUCE-6689](https://issues.apache.org/jira/browse/MAPREDUCE-6689) | MapReduce job can infinitely increase number of reducer resource requests |  Blocker | . | Wangda Tan | Wangda Tan |
| [YARN-4747](https://issues.apache.org/jira/browse/YARN-4747) | AHS error 500 due to NPE when container start event is missing |  Major | timelineserver | Jason Lowe | Varun Saxena |
| [HADOOP-13084](https://issues.apache.org/jira/browse/HADOOP-13084) | Fix ASF License warnings in branch-2.7 |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-10372](https://issues.apache.org/jira/browse/HDFS-10372) | Fix for failing TestFsDatasetImpl#testCleanShutdownOfVolume |  Major | test | Rushabh S Shah | Rushabh S Shah |
| [MAPREDUCE-6558](https://issues.apache.org/jira/browse/MAPREDUCE-6558) | multibyte delimiters with compressed input files generate duplicate records |  Major | mrv1, mrv2 | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [MAPREDUCE-6513](https://issues.apache.org/jira/browse/MAPREDUCE-6513) | MR job got hanged forever when one NM unstable for some time |  Critical | applicationmaster, resourcemanager | Bob.zhao | Varun Saxena |
| [YARN-3840](https://issues.apache.org/jira/browse/YARN-3840) | Resource Manager web ui issue when sorting application by id (with application having id \> 9999) |  Major | resourcemanager | LINTE | Varun Saxena |
| [YARN-5055](https://issues.apache.org/jira/browse/YARN-5055) | max apps per user can be larger than max per queue |  Minor | capacityscheduler, resourcemanager | Jason Lowe | Eric Badger |
| [YARN-4751](https://issues.apache.org/jira/browse/YARN-4751) | In 2.7, Labeled queue usage not shown properly in capacity scheduler UI |  Major | capacity scheduler, yarn | Eric Payne | Eric Payne |
| [HDFS-9365](https://issues.apache.org/jira/browse/HDFS-9365) | Balancer does not work with the HDFS-6376 HA setup |  Major | balancer & mover | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [YARN-4459](https://issues.apache.org/jira/browse/YARN-4459) | container-executor should only kill process groups |  Major | nodemanager | Jun Gong | Jun Gong |
| [HDFS-9476](https://issues.apache.org/jira/browse/HDFS-9476) | TestDFSUpgradeFromImage#testUpgradeFromRel1BBWImage occasionally fail |  Major | . | Wei-Chiu Chuang | Masatake Iwasaki |
| [HDFS-10458](https://issues.apache.org/jira/browse/HDFS-10458) | getFileEncryptionInfo should return quickly for non-encrypted cluster |  Major | encryption, namenode | Zhe Zhang | Zhe Zhang |
| [YARN-5206](https://issues.apache.org/jira/browse/YARN-5206) | RegistrySecurity includes id:pass in exception text if considered invalid |  Minor | client, security | Steve Loughran | Steve Loughran |
| [HADOOP-13270](https://issues.apache.org/jira/browse/HADOOP-13270) | BZip2CompressionInputStream finds the same compression marker twice in corner case, causing duplicate data blocks |  Critical | . | Haibo Chen | Kai Sasaki |
| [HADOOP-13255](https://issues.apache.org/jira/browse/HADOOP-13255) | KMSClientProvider should check and renew tgt when doing delegation token operations. |  Major | kms | Xiao Chen | Xiao Chen |
| [HDFS-10474](https://issues.apache.org/jira/browse/HDFS-10474) | hftp copy fails when file name with Chinese+special char in branch-2 |  Critical | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-13192](https://issues.apache.org/jira/browse/HADOOP-13192) | org.apache.hadoop.util.LineReader cannot handle multibyte delimiters correctly |  Critical | util | binde | binde |
| [HADOOP-13350](https://issues.apache.org/jira/browse/HADOOP-13350) | Additional fix to LICENSE and NOTICE |  Blocker | build | Xiao Chen | Xiao Chen |
| [HADOOP-12893](https://issues.apache.org/jira/browse/HADOOP-12893) | Verify LICENSE.txt and NOTICE.txt |  Blocker | build | Allen Wittenauer | Xiao Chen |
| [HADOOP-13297](https://issues.apache.org/jira/browse/HADOOP-13297) | Add missing dependency in setting maven-remote-resource-plugin to fix builds |  Major | build | Akira Ajisaka | Sean Busbey |
| [HDFS-10623](https://issues.apache.org/jira/browse/HDFS-10623) | Remove unused import of httpclient.HttpConnection from TestWebHdfsTokens. |  Major | hdfs | Jitendra Nath Pandey | Hanisha Koneru |
| [YARN-5309](https://issues.apache.org/jira/browse/YARN-5309) | Fix SSLFactory truststore reloader thread leak in TimelineClientImpl |  Blocker | timelineserver, yarn | Thomas Friedrich | Weiwei Yang |
| [HDFS-8914](https://issues.apache.org/jira/browse/HDFS-8914) | Document HA support in the HDFS HdfsDesign.md |  Major | documentation | Ravindra Babu | Lars Francke |
| [HADOOP-12588](https://issues.apache.org/jira/browse/HADOOP-12588) | Fix intermittent test failure of TestGangliaMetrics |  Major | . | Tsuyoshi Ozawa | Masatake Iwasaki |
| [HADOOP-13434](https://issues.apache.org/jira/browse/HADOOP-13434) | Add quoting to Shell class |  Major | . | Owen O'Malley | Owen O'Malley |
| [HDFS-9530](https://issues.apache.org/jira/browse/HDFS-9530) | ReservedSpace is not cleared for abandoned Blocks |  Critical | datanode | Fei Hui | Brahma Reddy Battula |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-6191](https://issues.apache.org/jira/browse/MAPREDUCE-6191) | TestJavaSerialization fails with getting incorrect MR job result |  Minor | test | sam liu | sam liu |
| [YARN-3602](https://issues.apache.org/jira/browse/YARN-3602) | TestResourceLocalizationService.testPublicResourceInitializesLocalDir fails Intermittently due to IOException from cleanup |  Minor | test | zhihai xu | zhihai xu |
| [HADOOP-12736](https://issues.apache.org/jira/browse/HADOOP-12736) | TestTimedOutTestsListener#testThreadDumpAndDeadlocks sometimes times out |  Major | . | Xiao Chen | Xiao Chen |
| [HADOOP-12715](https://issues.apache.org/jira/browse/HADOOP-12715) | TestValueQueue#testgetAtMostPolicyALL fails intermittently |  Major | . | Xiao Chen | Xiao Chen |
| [HDFS-9688](https://issues.apache.org/jira/browse/HDFS-9688) | Test the effect of nested encryption zones in HDFS downgrade |  Major | encryption, test | Zhe Zhang | Zhe Zhang |
| [YARN-5069](https://issues.apache.org/jira/browse/YARN-5069) | TestFifoScheduler.testResourceOverCommit race condition |  Major | test | Eric Badger | Eric Badger |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-4623](https://issues.apache.org/jira/browse/YARN-4623) | TestSystemMetricsPublisher#testPublishAppAttemptMetricsForUnmanagedAM fails with NPE |  Major | yarn | Jason Lowe | Naganarasimha G R |
| [HDFS-10186](https://issues.apache.org/jira/browse/HDFS-10186) | DirectoryScanner: Improve logs by adding full path of both actual and expected block directories |  Minor | datanode | Rakesh R | Rakesh R |
| [HADOOP-13154](https://issues.apache.org/jira/browse/HADOOP-13154) | S3AFileSystem printAmazonServiceException/printAmazonClientException appear copy & paste of AWS examples |  Blocker | fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-3362](https://issues.apache.org/jira/browse/YARN-3362) | Add node label usage in RM CapacityScheduler web UI |  Major | capacityscheduler, resourcemanager, webapp | Wangda Tan | Naganarasimha G R |
| [YARN-3426](https://issues.apache.org/jira/browse/YARN-3426) | Add jdiff support to YARN |  Blocker | . | Li Lu | Li Lu |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-11814](https://issues.apache.org/jira/browse/HADOOP-11814) | Reformat hadoop-annotations, o.a.h.classification.tools |  Minor | . | Li Lu | Li Lu |
| [YARN-4653](https://issues.apache.org/jira/browse/YARN-4653) | Document YARN security model from the perspective of Application Developers |  Major | site | Steve Loughran | Steve Loughran |
| [HADOOP-13312](https://issues.apache.org/jira/browse/HADOOP-13312) | Update CHANGES.txt to reflect all the changes in branch-2.7 |  Blocker | . | Akira Ajisaka | Akira Ajisaka |
