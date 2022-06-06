
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

## Release 2.5.0 - 2014-08-11

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-6168](https://issues.apache.org/jira/browse/HDFS-6168) | Remove deprecated methods in DistributedFileSystem |  Major | hdfs-client | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-6164](https://issues.apache.org/jira/browse/HDFS-6164) | Remove lsr in OfflineImageViewer |  Major | tools | Haohui Mai | Haohui Mai |
| [HDFS-6153](https://issues.apache.org/jira/browse/HDFS-6153) | Document "fileId" and "childrenNum" fields in the FileStatus Json schema |  Minor | documentation, webhdfs | Akira AJISAKA | Akira AJISAKA |
| [MAPREDUCE-5777](https://issues.apache.org/jira/browse/MAPREDUCE-5777) | Support utf-8 text with BOM (byte order marker) |  Major | . | bc Wong | zhihai xu |
| [YARN-2107](https://issues.apache.org/jira/browse/YARN-2107) | Refactor timeline classes into server.timeline package |  Major | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-10514](https://issues.apache.org/jira/browse/HADOOP-10514) | Common side changes to support  HDFS extended attributes (HDFS-2006) |  Major | fs | Uma Maheswara Rao G | Yi Liu |
| [HADOOP-10498](https://issues.apache.org/jira/browse/HADOOP-10498) | Add support for proxy server |  Major | util | Daryn Sharp | Daryn Sharp |
| [HADOOP-9704](https://issues.apache.org/jira/browse/HADOOP-9704) | Write metrics sink plugin for Hadoop/Graphite |  Major | . | Chu Tong |  |
| [HDFS-6435](https://issues.apache.org/jira/browse/HDFS-6435) | Add support for specifying a static uid/gid mapping for the NFS gateway |  Major | nfs | Aaron T. Myers | Aaron T. Myers |
| [HDFS-6406](https://issues.apache.org/jira/browse/HDFS-6406) | Add capability for NFS gateway to reject connections from unprivileged ports |  Major | nfs | Aaron T. Myers | Aaron T. Myers |
| [HDFS-6281](https://issues.apache.org/jira/browse/HDFS-6281) | Provide option to use the NFS Gateway without having to use the Hadoop portmapper |  Major | nfs | Aaron T. Myers | Aaron T. Myers |
| [YARN-1864](https://issues.apache.org/jira/browse/YARN-1864) | Fair Scheduler Dynamic Hierarchical User Queues |  Major | scheduler | Ashwin Shankar | Ashwin Shankar |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-10896](https://issues.apache.org/jira/browse/HADOOP-10896) | Update compatibility doc to capture visibility of un-annotated classes/ methods |  Blocker | documentation | Karthik Kambatla | Karthik Kambatla |
| [HADOOP-10782](https://issues.apache.org/jira/browse/HADOOP-10782) | Typo in DataChecksum classs |  Trivial | . | Jingguo Yao | Jingguo Yao |
| [HADOOP-10767](https://issues.apache.org/jira/browse/HADOOP-10767) | Clean up unused code in Ls shell command. |  Trivial | fs | Chris Nauroth | Chris Nauroth |
| [HADOOP-10747](https://issues.apache.org/jira/browse/HADOOP-10747) | Support configurable retries on SASL connection failures in RPC client. |  Minor | ipc | Chris Nauroth | Chris Nauroth |
| [HADOOP-10691](https://issues.apache.org/jira/browse/HADOOP-10691) | Improve the readability of 'hadoop fs -help' |  Minor | tools | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HADOOP-10688](https://issues.apache.org/jira/browse/HADOOP-10688) | Expose thread-level FileSystem StatisticsData |  Major | fs | Sandy Ryza | Sandy Ryza |
| [HADOOP-10674](https://issues.apache.org/jira/browse/HADOOP-10674) | Rewrite the PureJavaCrc32 loop for performance improvement |  Major | performance, util | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-10666](https://issues.apache.org/jira/browse/HADOOP-10666) | Remove Copyright /d/d/d/d Apache Software Foundation from the source files license header |  Minor | documentation | Henry Saputra | Henry Saputra |
| [HADOOP-10665](https://issues.apache.org/jira/browse/HADOOP-10665) | Make Hadoop Authentication Handler loads case in-sensitive |  Minor | security | Benoy Antony | Benoy Antony |
| [HADOOP-10618](https://issues.apache.org/jira/browse/HADOOP-10618) | Remove SingleNodeSetup.apt.vm |  Minor | documentation | Akira AJISAKA | Akira AJISAKA |
| [HADOOP-10614](https://issues.apache.org/jira/browse/HADOOP-10614) | CBZip2InputStream is not threadsafe |  Major | . | Xiangrui Meng | Xiangrui Meng |
| [HADOOP-10572](https://issues.apache.org/jira/browse/HADOOP-10572) | Example NFS mount command must pass noacl as it isn't supported by the server yet |  Trivial | nfs | Harsh J | Harsh J |
| [HADOOP-10561](https://issues.apache.org/jira/browse/HADOOP-10561) | Copy command with preserve option should handle Xattrs |  Major | fs | Uma Maheswara Rao G | Yi Liu |
| [HADOOP-10557](https://issues.apache.org/jira/browse/HADOOP-10557) | FsShell -cp -pa option for preserving extended ACLs |  Major | fs | Akira AJISAKA | Akira AJISAKA |
| [HADOOP-10556](https://issues.apache.org/jira/browse/HADOOP-10556) | Add toLowerCase support to auth\_to\_local rules for service name |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-10549](https://issues.apache.org/jira/browse/HADOOP-10549) | MAX\_SUBST and varPat should be final in Configuration.java |  Major | conf | Gera Shegalov | Gera Shegalov |
| [HADOOP-10539](https://issues.apache.org/jira/browse/HADOOP-10539) | Provide backward compatibility for ProxyUsers.authorize() call |  Minor | security | Benoy Antony | Benoy Antony |
| [HADOOP-10535](https://issues.apache.org/jira/browse/HADOOP-10535) | Make the retry numbers in ActiveStandbyElector configurable |  Minor | . | Jing Zhao | Jing Zhao |
| [HADOOP-10458](https://issues.apache.org/jira/browse/HADOOP-10458) | swifts should throw FileAlreadyExistsException on attempt to overwrite file |  Minor | fs | Steve Loughran | Steve Loughran |
| [HADOOP-10454](https://issues.apache.org/jira/browse/HADOOP-10454) | Provide FileContext version of har file system |  Major | . | Kihwal Lee | Kihwal Lee |
| [HADOOP-10451](https://issues.apache.org/jira/browse/HADOOP-10451) | Remove unused field and imports from SaslRpcServer |  Trivial | security | Benoy Antony | Benoy Antony |
| [HADOOP-10376](https://issues.apache.org/jira/browse/HADOOP-10376) | Refactor refresh\*Protocols into a single generic refreshConfigProtocol |  Minor | . | Chris Li | Chris Li |
| [HADOOP-10345](https://issues.apache.org/jira/browse/HADOOP-10345) | Sanitize the the inputs (groups and hosts) for the proxyuser configuration |  Minor | security | Benoy Antony | Benoy Antony |
| [HADOOP-10322](https://issues.apache.org/jira/browse/HADOOP-10322) | Add ability to read principal names from a keytab |  Major | security | Benoy Antony | Benoy Antony |
| [HADOOP-9968](https://issues.apache.org/jira/browse/HADOOP-9968) | ProxyUsers does not work with NetGroups |  Major | security | Benoy Antony | Benoy Antony |
| [HADOOP-9495](https://issues.apache.org/jira/browse/HADOOP-9495) | Define behaviour of Seekable.seek(), write tests, fix all hadoop implementations for compliance |  Major | fs | Steve Loughran | Steve Loughran |
| [HADOOP-9361](https://issues.apache.org/jira/browse/HADOOP-9361) | Strictly define the expected behavior of filesystem APIs and write tests to verify compliance |  Blocker | fs, test | Steve Loughran | Steve Loughran |
| [HADOOP-8943](https://issues.apache.org/jira/browse/HADOOP-8943) | Support multiple group mapping providers |  Major | security | Kai Zheng | Kai Zheng |
| [HADOOP-6350](https://issues.apache.org/jira/browse/HADOOP-6350) | Documenting Hadoop metrics |  Major | documentation, metrics | Hong Tang | Akira AJISAKA |
| [HDFS-6620](https://issues.apache.org/jira/browse/HDFS-6620) | Snapshot docs should specify about preserve options with cp command |  Major | namenode | Uma Maheswara Rao G | Stephen Chu |
| [HDFS-6603](https://issues.apache.org/jira/browse/HDFS-6603) | Add XAttr with ACL test |  Minor | test | Stephen Chu | Stephen Chu |
| [HDFS-6595](https://issues.apache.org/jira/browse/HDFS-6595) | Configure the maximum threads allowed for balancing on datanodes |  Minor | balancer & mover, datanode | Benoy Antony | Benoy Antony |
| [HDFS-6593](https://issues.apache.org/jira/browse/HDFS-6593) | Move SnapshotDiffInfo out of INodeDirectorySnapshottable |  Minor | namenode, snapshots | Jing Zhao | Jing Zhao |
| [HDFS-6580](https://issues.apache.org/jira/browse/HDFS-6580) | FSNamesystem.mkdirsInt should call the getAuditFileInfo() wrapper |  Major | namenode | Zhilei Xu | Zhilei Xu |
| [HDFS-6578](https://issues.apache.org/jira/browse/HDFS-6578) | add toString method to DatanodeStorage for easier debugging |  Major | . | Yongjun Zhang | Yongjun Zhang |
| [HDFS-6558](https://issues.apache.org/jira/browse/HDFS-6558) | Missing '\n' in the description of dfsadmin -rollingUpgrade |  Trivial | . | Akira AJISAKA | Chen He |
| [HDFS-6545](https://issues.apache.org/jira/browse/HDFS-6545) | Finalizing rolling upgrade can make NN unavailable for a long duration |  Critical | . | Kihwal Lee | Kihwal Lee |
| [HDFS-6529](https://issues.apache.org/jira/browse/HDFS-6529) | Trace logging for RemoteBlockReader2 to identify remote datanode and file being read |  Minor | hdfs-client | Anubhav Dhoot | Anubhav Dhoot |
| [HDFS-6528](https://issues.apache.org/jira/browse/HDFS-6528) | Add XAttrs to TestOfflineImageViewer |  Minor | test | Stephen Chu | Stephen Chu |
| [HDFS-6507](https://issues.apache.org/jira/browse/HDFS-6507) | Improve DFSAdmin to support HA cluster better |  Major | tools | Zesheng Wu | Zesheng Wu |
| [HDFS-6503](https://issues.apache.org/jira/browse/HDFS-6503) | Fix typo of DFSAdmin restoreFailedStorage |  Minor | tools | Zesheng Wu | Zesheng Wu |
| [HDFS-6499](https://issues.apache.org/jira/browse/HDFS-6499) | Use NativeIO#renameTo instead of File#renameTo in FileJournalManager |  Major | namenode | Yongjun Zhang | Yongjun Zhang |
| [HDFS-6492](https://issues.apache.org/jira/browse/HDFS-6492) | Support create-time xattrs and atomically setting multiple xattrs |  Major | namenode | Andrew Wang | Andrew Wang |
| [HDFS-6463](https://issues.apache.org/jira/browse/HDFS-6463) | Clarify behavior of AclStorage#createFsPermissionForExtendedAcl in comments. |  Trivial | namenode | Aaron T. Myers | Chris Nauroth |
| [HDFS-6460](https://issues.apache.org/jira/browse/HDFS-6460) | Ignore stale and decommissioned nodes in NetworkTopology#sortByDistance |  Minor | . | Yongjun Zhang | Yongjun Zhang |
| [HDFS-6453](https://issues.apache.org/jira/browse/HDFS-6453) | use Time#monotonicNow to avoid system clock reset |  Major | datanode, namenode | Liang Xie | Liang Xie |
| [HDFS-6448](https://issues.apache.org/jira/browse/HDFS-6448) | BlockReaderLocalLegacy should set socket timeout based on conf.socketTimeout |  Major | hdfs-client | Liang Xie | Liang Xie |
| [HDFS-6447](https://issues.apache.org/jira/browse/HDFS-6447) | balancer should timestamp the completion message |  Trivial | balancer & mover | Allen Wittenauer | Juan Yu |
| [HDFS-6442](https://issues.apache.org/jira/browse/HDFS-6442) | Fix TestEditLogAutoroll and TestStandbyCheckpoints failure caused by port conficts |  Minor | test | Zesheng Wu | Zesheng Wu |
| [HDFS-6433](https://issues.apache.org/jira/browse/HDFS-6433) | Replace BytesMoved class with AtomicLong |  Major | balancer & mover | Benoy Antony | Benoy Antony |
| [HDFS-6432](https://issues.apache.org/jira/browse/HDFS-6432) | Add snapshot related APIs to webhdfs |  Major | namenode, webhdfs | Suresh Srinivas | Jing Zhao |
| [HDFS-6416](https://issues.apache.org/jira/browse/HDFS-6416) | Use Time#monotonicNow in OpenFileCtx and OpenFileCtxCatch to avoid system clock bugs |  Minor | nfs | Brandon Li | Abhiraj Butala |
| [HDFS-6403](https://issues.apache.org/jira/browse/HDFS-6403) | Add metrics for log warnings reported by JVM pauses |  Major | datanode, namenode | Yongjun Zhang | Yongjun Zhang |
| [HDFS-6396](https://issues.apache.org/jira/browse/HDFS-6396) | Remove support for ACL feature from INodeSymlink |  Minor | . | Andrew Wang | Charles Lamb |
| [HDFS-6375](https://issues.apache.org/jira/browse/HDFS-6375) | Listing extended attributes with the search permission |  Major | namenode | Andrew Wang | Charles Lamb |
| [HDFS-6369](https://issues.apache.org/jira/browse/HDFS-6369) | Document that BlockReader#available() can return more bytes than are remaining in the block |  Trivial | . | Ted Yu | Ted Yu |
| [HDFS-6356](https://issues.apache.org/jira/browse/HDFS-6356) | Fix typo in DatanodeLayoutVersion |  Trivial | datanode | Tulasi G | Tulasi G |
| [HDFS-6334](https://issues.apache.org/jira/browse/HDFS-6334) | Client failover proxy provider for IP failover based NN HA |  Major | . | Kihwal Lee | Kihwal Lee |
| [HDFS-6304](https://issues.apache.org/jira/browse/HDFS-6304) | Consolidate the logic of path resolution in FSDirectory |  Major | namenode | Haohui Mai | Haohui Mai |
| [HDFS-6297](https://issues.apache.org/jira/browse/HDFS-6297) | Add CLI testcases to reflect new features of dfs and dfsadmin |  Major | test | Dasha Boudnik | Dasha Boudnik |
| [HDFS-6295](https://issues.apache.org/jira/browse/HDFS-6295) | Add "decommissioning" state and node state filtering to dfsadmin |  Major | . | Andrew Wang | Andrew Wang |
| [HDFS-6282](https://issues.apache.org/jira/browse/HDFS-6282) | re-add testIncludeByRegistrationName |  Minor | test | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-6279](https://issues.apache.org/jira/browse/HDFS-6279) | Create new index page for JN / DN |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-6278](https://issues.apache.org/jira/browse/HDFS-6278) | Create HTML5-based UI for SNN |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-6273](https://issues.apache.org/jira/browse/HDFS-6273) | Config options to allow wildcard endpoints for namenode HTTP and HTTPS servers |  Major | namenode | Arpit Agarwal | Arpit Agarwal |
| [HDFS-6269](https://issues.apache.org/jira/browse/HDFS-6269) | NameNode Audit Log should differentiate between webHDFS open and HDFS open. |  Major | namenode, webhdfs | Eric Payne | Eric Payne |
| [HDFS-6268](https://issues.apache.org/jira/browse/HDFS-6268) | Better sorting in NetworkTopology#pseudoSortByDistance when no local node is found |  Minor | . | Andrew Wang | Andrew Wang |
| [HDFS-6266](https://issues.apache.org/jira/browse/HDFS-6266) | Identify full path for a given INode |  Major | snapshots | Jing Zhao | Jing Zhao |
| [HDFS-6256](https://issues.apache.org/jira/browse/HDFS-6256) | Clean up ImageVisitor and SpotCheckImageVisitor |  Major | tools | Akira AJISAKA | Akira AJISAKA |
| [HDFS-6225](https://issues.apache.org/jira/browse/HDFS-6225) | Remove the o.a.h.hdfs.server.common.UpgradeStatusReport |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-6191](https://issues.apache.org/jira/browse/HDFS-6191) | Disable quota checks when replaying edit log. |  Major | namenode | Kihwal Lee | Kihwal Lee |
| [HDFS-6167](https://issues.apache.org/jira/browse/HDFS-6167) | Relocate the non-public API classes in the hdfs.client package |  Major | hdfs-client | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-6158](https://issues.apache.org/jira/browse/HDFS-6158) | Clean up dead code for OfflineImageViewer |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-6110](https://issues.apache.org/jira/browse/HDFS-6110) | adding more slow action log in critical write path |  Major | datanode | Liang Xie | Liang Xie |
| [HDFS-6109](https://issues.apache.org/jira/browse/HDFS-6109) | let sync\_file\_range() system call run in background |  Major | datanode | Liang Xie | Liang Xie |
| [HDFS-6007](https://issues.apache.org/jira/browse/HDFS-6007) | Update documentation about short-circuit local reads |  Minor | documentation | Masatake Iwasaki |  |
| [HDFS-5693](https://issues.apache.org/jira/browse/HDFS-5693) | Few NN metrics data points were collected via JMX when NN is under heavy load |  Major | namenode | Ming Ma | Ming Ma |
| [HDFS-5683](https://issues.apache.org/jira/browse/HDFS-5683) | Better audit log messages for caching operations |  Major | namenode | Andrew Wang | Abhiraj Butala |
| [HDFS-5381](https://issues.apache.org/jira/browse/HDFS-5381) | ExtendedBlock#hashCode should use both blockId and block pool ID |  Minor | federation | Colin Patrick McCabe | Benoy Antony |
| [HDFS-5196](https://issues.apache.org/jira/browse/HDFS-5196) | Provide more snapshot information in WebUI |  Minor | snapshots | Haohui Mai | Shinichi Yamashita |
| [HDFS-5168](https://issues.apache.org/jira/browse/HDFS-5168) | BlockPlacementPolicy does not work for cross node group dependencies |  Critical | namenode | Nikola Vujic | Nikola Vujic |
| [HDFS-2949](https://issues.apache.org/jira/browse/HDFS-2949) | HA: Add check to active state transition to prevent operator-induced split brain |  Major | ha, namenode | Todd Lipcon | Rushabh S Shah |
| [HDFS-2006](https://issues.apache.org/jira/browse/HDFS-2006) | ability to support storing extended attributes per file |  Major | namenode | dhruba borthakur | Yi Liu |
| [MAPREDUCE-5899](https://issues.apache.org/jira/browse/MAPREDUCE-5899) | Support incremental data copy in DistCp |  Major | distcp | Jing Zhao | Jing Zhao |
| [MAPREDUCE-5896](https://issues.apache.org/jira/browse/MAPREDUCE-5896) | InputSplits should indicate which locations have the block cached in memory |  Major | . | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-5886](https://issues.apache.org/jira/browse/MAPREDUCE-5886) | Allow wordcount example job to accept multiple input paths. |  Minor | examples | Chris Nauroth | Chris Nauroth |
| [MAPREDUCE-5861](https://issues.apache.org/jira/browse/MAPREDUCE-5861) | finishedSubMaps field in LocalContainerLauncher does not need to be volatile |  Minor | . | Ted Yu | Tsuyoshi Ozawa |
| [MAPREDUCE-5825](https://issues.apache.org/jira/browse/MAPREDUCE-5825) | Provide diagnostics for reducers killed during ramp down |  Major | mr-am | Gera Shegalov | Gera Shegalov |
| [MAPREDUCE-5812](https://issues.apache.org/jira/browse/MAPREDUCE-5812) |  Make job context available to OutputCommitter.isRecoverySupported() |  Major | mr-am | Mohammad Kamrul Islam | Mohammad Kamrul Islam |
| [MAPREDUCE-5809](https://issues.apache.org/jira/browse/MAPREDUCE-5809) | Enhance distcp to support preserving HDFS ACLs. |  Major | distcp | Chris Nauroth | Chris Nauroth |
| [MAPREDUCE-5774](https://issues.apache.org/jira/browse/MAPREDUCE-5774) | Job overview in History UI should list reducer phases in chronological order |  Trivial | jobhistoryserver | Gera Shegalov | Gera Shegalov |
| [MAPREDUCE-5402](https://issues.apache.org/jira/browse/MAPREDUCE-5402) | DynamicInputFormat should allow overriding of MAX\_CHUNKS\_TOLERABLE |  Major | distcp, mrv2 | David Rosenstrauch | Tsuyoshi Ozawa |
| [MAPREDUCE-5014](https://issues.apache.org/jira/browse/MAPREDUCE-5014) | Extending DistCp through a custom CopyListing is not possible |  Major | distcp | Srikanth Sundarrajan | Srikanth Sundarrajan |
| [YARN-2335](https://issues.apache.org/jira/browse/YARN-2335) | Annotate all hadoop-sls APIs as @Private |  Minor | . | Wei Yan | Wei Yan |
| [YARN-2300](https://issues.apache.org/jira/browse/YARN-2300) | Document better sample requests for RM web services for submitting apps |  Major | documentation | Varun Vasudev | Varun Vasudev |
| [YARN-2195](https://issues.apache.org/jira/browse/YARN-2195) | Clean a piece of code in ResourceRequest |  Trivial | . | Wei Yan | Wei Yan |
| [YARN-2159](https://issues.apache.org/jira/browse/YARN-2159) | Better logging in SchedulerNode#allocateContainer |  Trivial | resourcemanager | Ray Chiang | Ray Chiang |
| [YARN-2089](https://issues.apache.org/jira/browse/YARN-2089) | FairScheduler: QueuePlacementPolicy and QueuePlacementRule are missing audience annotations |  Major | scheduler | Anubhav Dhoot | zhihai xu |
| [YARN-2072](https://issues.apache.org/jira/browse/YARN-2072) | RM/NM UIs and webservices are missing vcore information |  Major | nodemanager, resourcemanager, webapp | Nathan Roberts | Nathan Roberts |
| [YARN-2061](https://issues.apache.org/jira/browse/YARN-2061) | Revisit logging levels in ZKRMStateStore |  Minor | resourcemanager | Karthik Kambatla | Ray Chiang |
| [YARN-2030](https://issues.apache.org/jira/browse/YARN-2030) | Use StateMachine to simplify handleStoreEvent() in RMStateStore |  Major | . | Junping Du | Binglin Chang |
| [YARN-2012](https://issues.apache.org/jira/browse/YARN-2012) | Fair Scheduler: allow default queue placement rule to take an arbitrary queue |  Major | scheduler | Ashwin Shankar | Ashwin Shankar |
| [YARN-1987](https://issues.apache.org/jira/browse/YARN-1987) | Wrapper for leveldb DBIterator to aid in handling database exceptions |  Major | . | Jason Lowe | Jason Lowe |
| [YARN-1923](https://issues.apache.org/jira/browse/YARN-1923) | Make FairScheduler resource ratio calculations terminate faster |  Major | fairscheduler | Anubhav Dhoot | Anubhav Dhoot |
| [YARN-1889](https://issues.apache.org/jira/browse/YARN-1889) | In Fair Scheduler, avoid creating objects on each call to AppSchedulable comparator |  Minor | scheduler | Hong Zhiguo | Hong Zhiguo |
| [YARN-1870](https://issues.apache.org/jira/browse/YARN-1870) | FileInputStream is not closed in ProcfsBasedProcessTree#constructProcessSMAPInfo() |  Minor | resourcemanager | Ted Yu | Fengdong Yu |
| [YARN-1845](https://issues.apache.org/jira/browse/YARN-1845) |  Elapsed time for failed tasks that never started is  wrong |  Major | . | Rushabh S Shah | Rushabh S Shah |
| [YARN-1751](https://issues.apache.org/jira/browse/YARN-1751) | Improve MiniYarnCluster for log aggregation testing |  Major | nodemanager | Ming Ma | Ming Ma |
| [YARN-1561](https://issues.apache.org/jira/browse/YARN-1561) | Fix a generic type warning in FairScheduler |  Minor | scheduler | Junping Du | Chen He |
| [YARN-1479](https://issues.apache.org/jira/browse/YARN-1479) | Invalid NaN values in Hadoop REST API JSON response |  Major | . | Kendall Thrapp | Chen He |
| [YARN-1424](https://issues.apache.org/jira/browse/YARN-1424) | RMAppAttemptImpl should return the DummyApplicationResourceUsageReport for all invalid accesses |  Minor | resourcemanager | Sandy Ryza | Ray Chiang |
| [YARN-614](https://issues.apache.org/jira/browse/YARN-614) | Separate AM failures from hardware failure or YARN error and do not count them to AM retry count |  Major | resourcemanager | Bikas Saha | Xuan Gong |
| [YARN-483](https://issues.apache.org/jira/browse/YARN-483) | Improve documentation on log aggregation in yarn-default.xml |  Major | documentation | Sandy Ryza | Akira AJISAKA |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-10910](https://issues.apache.org/jira/browse/HADOOP-10910) | Increase findbugs maxHeap size |  Blocker | . | Andrew Wang | Andrew Wang |
| [HADOOP-10890](https://issues.apache.org/jira/browse/HADOOP-10890) | TestDFVariations.testMount fails intermittently |  Major | . | Yongjun Zhang | Yongjun Zhang |
| [HADOOP-10872](https://issues.apache.org/jira/browse/HADOOP-10872) | TestPathData fails intermittently with "Mkdirs failed to create d1" |  Major | fs | Yongjun Zhang | Yongjun Zhang |
| [HADOOP-10801](https://issues.apache.org/jira/browse/HADOOP-10801) | Fix dead link in site.xml |  Major | documentation | Akira AJISAKA | Akira AJISAKA |
| [HADOOP-10746](https://issues.apache.org/jira/browse/HADOOP-10746) | TestSocketIOWithTimeout#testSocketIOWithTimeout fails on Power PC |  Major | test | Jinghui Wang | Jinghui Wang |
| [HADOOP-10739](https://issues.apache.org/jira/browse/HADOOP-10739) | Renaming a file into a directory containing the same filename results in a confusing I/O error |  Major | fs | Jason Lowe | Chang Li |
| [HADOOP-10737](https://issues.apache.org/jira/browse/HADOOP-10737) | S3n silent failure on copy, data loss on rename |  Major | fs/s3 | Gian Merlino | Steve Loughran |
| [HADOOP-10716](https://issues.apache.org/jira/browse/HADOOP-10716) | Cannot use more than 1 har filesystem |  Critical | conf, fs | Daryn Sharp | Rushabh S Shah |
| [HADOOP-10711](https://issues.apache.org/jira/browse/HADOOP-10711) | Cleanup some extra dependencies from hadoop-auth |  Major | security | Robert Kanter | Robert Kanter |
| [HADOOP-10710](https://issues.apache.org/jira/browse/HADOOP-10710) | hadoop.auth cookie is not properly constructed according to RFC2109 |  Major | security | Alejandro Abdelnur | Juan Yu |
| [HADOOP-10702](https://issues.apache.org/jira/browse/HADOOP-10702) | KerberosAuthenticationHandler does not log the principal names correctly |  Minor | security | Benoy Antony | Benoy Antony |
| [HADOOP-10701](https://issues.apache.org/jira/browse/HADOOP-10701) | NFS should not validate the access premission only based on the user's primary group |  Major | nfs | Premchandra Preetham Kukillaya | Harsh J |
| [HADOOP-10699](https://issues.apache.org/jira/browse/HADOOP-10699) | Fix build native library on mac osx |  Major | . | Kirill A. Korinskiy | Binglin Chang |
| [HADOOP-10686](https://issues.apache.org/jira/browse/HADOOP-10686) | Writables are not always configured |  Major | . | Abraham Elmahrek | Abraham Elmahrek |
| [HADOOP-10683](https://issues.apache.org/jira/browse/HADOOP-10683) | Users authenticated with KERBEROS are recorded as being authenticated with SIMPLE |  Major | security | Benoy Antony | Benoy Antony |
| [HADOOP-10678](https://issues.apache.org/jira/browse/HADOOP-10678) | SecurityUtil has unnecessary synchronization on collection used for only tests |  Minor | security | Benoy Antony | Benoy Antony |
| [HADOOP-10664](https://issues.apache.org/jira/browse/HADOOP-10664) | TestNetUtils.testNormalizeHostName fails |  Major | . | Chen He | Aaron T. Myers |
| [HADOOP-10660](https://issues.apache.org/jira/browse/HADOOP-10660) | GraphiteSink should implement Closeable |  Major | . | Ted Yu | Chen He |
| [HADOOP-10658](https://issues.apache.org/jira/browse/HADOOP-10658) | SSLFactory expects truststores being configured |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-10657](https://issues.apache.org/jira/browse/HADOOP-10657) | Have RetryInvocationHandler log failover attempt at INFO level |  Major | . | Ming Ma | Ming Ma |
| [HADOOP-10656](https://issues.apache.org/jira/browse/HADOOP-10656) | The password keystore file is not picked by LDAP group mapping |  Major | security | Brandon Li | Brandon Li |
| [HADOOP-10647](https://issues.apache.org/jira/browse/HADOOP-10647) | String Format Exception in SwiftNativeFileSystemStore.java |  Minor | fs/swift | Gene Kim | Gene Kim |
| [HADOOP-10639](https://issues.apache.org/jira/browse/HADOOP-10639) | FileBasedKeyStoresFactory initialization is not using default for SSL\_REQUIRE\_CLIENT\_CERT\_KEY |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-10638](https://issues.apache.org/jira/browse/HADOOP-10638) | Updating hadoop-daemon.sh to work as expected when nfs is started as a privileged user. |  Major | nfs | Manikandan Narayanaswamy | Manikandan Narayanaswamy |
| [HADOOP-10630](https://issues.apache.org/jira/browse/HADOOP-10630) | Possible race condition in RetryInvocationHandler |  Major | . | Jing Zhao | Jing Zhao |
| [HADOOP-10625](https://issues.apache.org/jira/browse/HADOOP-10625) | Configuration: names should be trimmed when putting/getting to properties |  Major | conf | Wangda Tan | Wangda Tan |
| [HADOOP-10622](https://issues.apache.org/jira/browse/HADOOP-10622) | Shell.runCommand can deadlock |  Critical | . | Jason Lowe | Gera Shegalov |
| [HADOOP-10609](https://issues.apache.org/jira/browse/HADOOP-10609) | .gitignore should ignore .orig and .rej files |  Major | . | Karthik Kambatla | Karthik Kambatla |
| [HADOOP-10602](https://issues.apache.org/jira/browse/HADOOP-10602) | Documentation has broken "Go Back" hyperlinks. |  Trivial | documentation | Chris Nauroth | Akira AJISAKA |
| [HADOOP-10590](https://issues.apache.org/jira/browse/HADOOP-10590) | ServiceAuthorizationManager  is not threadsafe |  Major | security | Benoy Antony | Benoy Antony |
| [HADOOP-10589](https://issues.apache.org/jira/browse/HADOOP-10589) | NativeS3FileSystem throw NullPointerException when the file is empty |  Major | fs/s3 | shuisheng wei | Steve Loughran |
| [HADOOP-10588](https://issues.apache.org/jira/browse/HADOOP-10588) | Workaround for jetty6 acceptor startup issue |  Major | . | Kihwal Lee | Kihwal Lee |
| [HADOOP-10585](https://issues.apache.org/jira/browse/HADOOP-10585) | Retry polices ignore interrupted exceptions |  Critical | ipc | Daryn Sharp | Daryn Sharp |
| [HADOOP-10581](https://issues.apache.org/jira/browse/HADOOP-10581) | TestUserGroupInformation#testGetServerSideGroups fails because groups stored in Set and ArrayList are compared |  Major | . | Mit Desai | Mit Desai |
| [HADOOP-10568](https://issues.apache.org/jira/browse/HADOOP-10568) | Add s3 server-side encryption |  Major | fs/s3 | David S. Wang | David S. Wang |
| [HADOOP-10547](https://issues.apache.org/jira/browse/HADOOP-10547) | Give SaslPropertiesResolver.getDefaultProperties() public scope |  Major | security | Jason Dere | Benoy Antony |
| [HADOOP-10543](https://issues.apache.org/jira/browse/HADOOP-10543) | RemoteException's unwrapRemoteException method failed for PathIOException |  Major | . | Yongjun Zhang | Yongjun Zhang |
| [HADOOP-10541](https://issues.apache.org/jira/browse/HADOOP-10541) | InputStream in MiniKdc#initKDCServer for minikdc.ldiff is not closed |  Minor | test | Ted Yu | Swarnim Kulkarni |
| [HADOOP-10540](https://issues.apache.org/jira/browse/HADOOP-10540) | Datanode upgrade in Windows fails with hardlink error. |  Major | tools | Huan Huang | Arpit Agarwal |
| [HADOOP-10533](https://issues.apache.org/jira/browse/HADOOP-10533) | S3 input stream NPEs in MapReduce job |  Minor | fs/s3 | Benjamin Kim | Steve Loughran |
| [HADOOP-10531](https://issues.apache.org/jira/browse/HADOOP-10531) | hadoop-config.sh - bug in --hosts argument |  Major | . | Sebastien Barrier | Sebastien Barrier |
| [HADOOP-10526](https://issues.apache.org/jira/browse/HADOOP-10526) | Chance for Stream leakage in CompressorStream |  Minor | . | SreeHari | Rushabh S Shah |
| [HADOOP-10517](https://issues.apache.org/jira/browse/HADOOP-10517) | InputStream is not closed in two methods of JarFinder |  Minor | test, util | Ted Yu | Ted Yu |
| [HADOOP-10508](https://issues.apache.org/jira/browse/HADOOP-10508) | RefreshCallQueue fails when authorization is enabled |  Major | ipc | Chris Li | Chris Li |
| [HADOOP-10500](https://issues.apache.org/jira/browse/HADOOP-10500) | TestDoAsEffectiveUser fails on JDK7 due to failure to reset proxy user configuration. |  Trivial | security, test | Chris Nauroth | Chris Nauroth |
| [HADOOP-10496](https://issues.apache.org/jira/browse/HADOOP-10496) | Metrics system FileSink can leak file descriptor. |  Major | metrics | Chris Nauroth | Chris Nauroth |
| [HADOOP-10495](https://issues.apache.org/jira/browse/HADOOP-10495) | TestFileUtil fails on Windows due to bad permission assertions. |  Trivial | fs, test | Chris Nauroth | Chris Nauroth |
| [HADOOP-10489](https://issues.apache.org/jira/browse/HADOOP-10489) | UserGroupInformation#getTokens and UserGroupInformation#addToken can lead to ConcurrentModificationException |  Major | . | Jing Zhao | Robert Kanter |
| [HADOOP-10475](https://issues.apache.org/jira/browse/HADOOP-10475) | ConcurrentModificationException in AbstractDelegationTokenSelector.selectToken() |  Major | security | Arpit Gupta | Jing Zhao |
| [HADOOP-10468](https://issues.apache.org/jira/browse/HADOOP-10468) | TestMetricsSystemImpl.testMultiThreadedPublish fails intermediately |  Blocker | . | Haohui Mai | Akira AJISAKA |
| [HADOOP-10462](https://issues.apache.org/jira/browse/HADOOP-10462) | DF#getFilesystem is not parsing the command output |  Major | . | Akira AJISAKA | Akira AJISAKA |
| [HADOOP-10459](https://issues.apache.org/jira/browse/HADOOP-10459) | distcp V2 doesn't preserve root dir's attributes when -p is specified |  Major | tools/distcp | Yongjun Zhang | Yongjun Zhang |
| [HADOOP-10419](https://issues.apache.org/jira/browse/HADOOP-10419) | BufferedFSInputStream NPEs on getPos() on a closed stream |  Minor | fs | Steve Loughran | Steve Loughran |
| [HADOOP-10418](https://issues.apache.org/jira/browse/HADOOP-10418) | SaslRpcClient should not assume that remote principals are in the default\_realm |  Major | security | Aaron T. Myers | Aaron T. Myers |
| [HADOOP-10414](https://issues.apache.org/jira/browse/HADOOP-10414) | Incorrect property name for RefreshUserMappingProtocol in hadoop-policy.xml |  Major | conf | Joey Echeverria | Joey Echeverria |
| [HADOOP-10401](https://issues.apache.org/jira/browse/HADOOP-10401) | ShellBasedUnixGroupsMapping#getGroups does not always return primary group first |  Major | util | Colin Patrick McCabe | Akira AJISAKA |
| [HADOOP-10378](https://issues.apache.org/jira/browse/HADOOP-10378) | Typo in help printed by hdfs dfs -help |  Major | . | Mit Desai | Mit Desai |
| [HADOOP-10350](https://issues.apache.org/jira/browse/HADOOP-10350) | BUILDING.txt should mention openssl dependency required for hadoop-pipes |  Major | . | Vinayakumar B | Vinayakumar B |
| [HADOOP-10342](https://issues.apache.org/jira/browse/HADOOP-10342) | Extend UserGroupInformation to return a UGI given a preauthenticated kerberos Subject |  Major | security | Larry McCay | Larry McCay |
| [HADOOP-10332](https://issues.apache.org/jira/browse/HADOOP-10332) | HttpServer's jetty audit log always logs 200 OK |  Major | . | Daryn Sharp | Jonathan Eagles |
| [HADOOP-10312](https://issues.apache.org/jira/browse/HADOOP-10312) | Shell.ExitCodeException to have more useful toString |  Minor | util | Steve Loughran | Steve Loughran |
| [HADOOP-10251](https://issues.apache.org/jira/browse/HADOOP-10251) | Both NameNodes could be in STANDBY State if SNN network is unstable |  Critical | ha | Vinayakumar B | Vinayakumar B |
| [HADOOP-10158](https://issues.apache.org/jira/browse/HADOOP-10158) | SPNEGO should work with multiple interfaces/SPNs. |  Critical | . | Kihwal Lee | Daryn Sharp |
| [HADOOP-9919](https://issues.apache.org/jira/browse/HADOOP-9919) | Update hadoop-metrics2.properties examples to Yarn |  Major | conf | Akira AJISAKA | Akira AJISAKA |
| [HADOOP-9705](https://issues.apache.org/jira/browse/HADOOP-9705) | FsShell cp -p does not preserve directory attibutes |  Major | fs | Stephen Chu | Akira AJISAKA |
| [HADOOP-9559](https://issues.apache.org/jira/browse/HADOOP-9559) | When metrics system is restarted MBean names get incorrectly flagged as dupes |  Major | metrics | Mostafa Elhemali | Mike Liddell |
| [HADOOP-9555](https://issues.apache.org/jira/browse/HADOOP-9555) | HA functionality that uses ZooKeeper may experience inadvertent TCP RST and miss session expiration event due to bug in client connection management |  Major | ha | Chris Nauroth | Chris Nauroth |
| [HADOOP-9099](https://issues.apache.org/jira/browse/HADOOP-9099) | NetUtils.normalizeHostName fails on domains where UnknownHost resolves to an IP address |  Minor | test | Ivan Mitic | Ivan Mitic |
| [HDFS-6793](https://issues.apache.org/jira/browse/HDFS-6793) | Missing changes in HftpFileSystem when Reintroduce dfs.http.port / dfs.https.port in branch-2 |  Blocker | . | Juan Yu | Juan Yu |
| [HDFS-6752](https://issues.apache.org/jira/browse/HDFS-6752) | Avoid Address bind errors in TestDatanodeConfig#testMemlockLimit |  Major | test | Vinayakumar B | Vinayakumar B |
| [HDFS-6723](https://issues.apache.org/jira/browse/HDFS-6723) | New NN webUI no longer displays decommissioned state for dead node |  Major | . | Ming Ma | Ming Ma |
| [HDFS-6712](https://issues.apache.org/jira/browse/HDFS-6712) | Document HDFS Multihoming Settings |  Major | documentation | Arpit Agarwal | Arpit Agarwal |
| [HDFS-6703](https://issues.apache.org/jira/browse/HDFS-6703) | NFS: Files can be deleted from a read-only mount |  Major | nfs | Abhiraj Butala | Srikanth Upputuri |
| [HDFS-6696](https://issues.apache.org/jira/browse/HDFS-6696) | Name node cannot start if the path of a file under construction contains ".snapshot" |  Blocker | . | Kihwal Lee | Andrew Wang |
| [HDFS-6680](https://issues.apache.org/jira/browse/HDFS-6680) | BlockPlacementPolicyDefault does not choose favored nodes correctly |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-6647](https://issues.apache.org/jira/browse/HDFS-6647) | Edit log corruption when pipeline recovery occurs for deleted file present in snapshot |  Blocker | namenode, snapshots | Aaron T. Myers | Kihwal Lee |
| [HDFS-6632](https://issues.apache.org/jira/browse/HDFS-6632) | Reintroduce dfs.http.port / dfs.https.port in branch-2 |  Major | . | Yongjun Zhang | Yongjun Zhang |
| [HDFS-6631](https://issues.apache.org/jira/browse/HDFS-6631) | TestPread#testHedgedReadLoopTooManyTimes fails intermittently. |  Major | hdfs-client, test | Chris Nauroth | Liang Xie |
| [HDFS-6622](https://issues.apache.org/jira/browse/HDFS-6622) | Rename and AddBlock may race and produce invalid edits |  Blocker | . | Kihwal Lee | Kihwal Lee |
| [HDFS-6618](https://issues.apache.org/jira/browse/HDFS-6618) | FSNamesystem#delete drops the FSN lock between removing INodes from the tree and deleting them from the inode map |  Blocker | . | Kihwal Lee | Kihwal Lee |
| [HDFS-6612](https://issues.apache.org/jira/browse/HDFS-6612) | MiniDFSNNTopology#simpleFederatedTopology(int) always hardcode nameservice ID |  Minor | . | Juan Yu | Juan Yu |
| [HDFS-6610](https://issues.apache.org/jira/browse/HDFS-6610) | TestShortCircuitLocalRead tests sometimes timeout on slow machines |  Minor | test | Charles Lamb | Charles Lamb |
| [HDFS-6604](https://issues.apache.org/jira/browse/HDFS-6604) | The short-circuit cache doesn't correctly time out replicas that haven't been used in a while |  Critical | hdfs-client | Giuseppe Reina | Colin Patrick McCabe |
| [HDFS-6601](https://issues.apache.org/jira/browse/HDFS-6601) | Issues in finalizing rolling upgrade when there is a layout version change |  Blocker | . | Kihwal Lee | Kihwal Lee |
| [HDFS-6599](https://issues.apache.org/jira/browse/HDFS-6599) | 2.4 addBlock is 10 to 20 times slower compared to 0.23 |  Blocker | . | Kihwal Lee | Daryn Sharp |
| [HDFS-6598](https://issues.apache.org/jira/browse/HDFS-6598) | Fix a typo in message issued from explorer.js |  Trivial | webhdfs | Yongjun Zhang | Yongjun Zhang |
| [HDFS-6591](https://issues.apache.org/jira/browse/HDFS-6591) | while loop is executed tens of thousands of times  in Hedged  Read |  Major | hdfs-client | LiuLei | Liang Xie |
| [HDFS-6587](https://issues.apache.org/jira/browse/HDFS-6587) | Bug in TestBPOfferService can cause test failure |  Major | test | Zhilei Xu | Zhilei Xu |
| [HDFS-6583](https://issues.apache.org/jira/browse/HDFS-6583) | Remove clientNode in FileUnderConstructionFeature |  Minor | namenode | Haohui Mai | Haohui Mai |
| [HDFS-6572](https://issues.apache.org/jira/browse/HDFS-6572) | Add an option to the NameNode that prints the software and on-disk image versions |  Minor | namenode | Charles Lamb | Charles Lamb |
| [HDFS-6563](https://issues.apache.org/jira/browse/HDFS-6563) | NameNode cannot save fsimage in certain circumstances when snapshots are in use |  Critical | namenode, snapshots | Aaron T. Myers | Aaron T. Myers |
| [HDFS-6559](https://issues.apache.org/jira/browse/HDFS-6559) | Fix wrong option "dfsadmin -rollingUpgrade start" in the document |  Minor | documentation | Akira AJISAKA | Akira AJISAKA |
| [HDFS-6556](https://issues.apache.org/jira/browse/HDFS-6556) | Refine XAttr permissions |  Major | namenode | Yi Liu | Uma Maheswara Rao G |
| [HDFS-6553](https://issues.apache.org/jira/browse/HDFS-6553) | Add missing DeprecationDeltas for NFS Kerberos configurations |  Major | nfs | Stephen Chu | Stephen Chu |
| [HDFS-6552](https://issues.apache.org/jira/browse/HDFS-6552) | add DN storage to a BlockInfo will not replace the different storage from same DN |  Trivial | namenode | Amir Langer | Amir Langer |
| [HDFS-6551](https://issues.apache.org/jira/browse/HDFS-6551) | Rename with OVERWRITE option may throw NPE when the target file/directory is a reference INode |  Major | namenode, snapshots | Jing Zhao | Jing Zhao |
| [HDFS-6549](https://issues.apache.org/jira/browse/HDFS-6549) | Add support for accessing the NFS gateway from the AIX NFS client |  Major | nfs | Aaron T. Myers | Aaron T. Myers |
| [HDFS-6539](https://issues.apache.org/jira/browse/HDFS-6539) | test\_native\_mini\_dfs is skipped in hadoop-hdfs/pom.xml |  Major | . | Binglin Chang | Binglin Chang |
| [HDFS-6535](https://issues.apache.org/jira/browse/HDFS-6535) | HDFS quota update is wrong when file is appended |  Major | namenode | George Wong | George Wong |
| [HDFS-6530](https://issues.apache.org/jira/browse/HDFS-6530) | Fix Balancer documentation |  Minor | documentation | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-6527](https://issues.apache.org/jira/browse/HDFS-6527) | Edit log corruption due to defered INode removal |  Blocker | . | Kihwal Lee | Kihwal Lee |
| [HDFS-6518](https://issues.apache.org/jira/browse/HDFS-6518) | TestCacheDirectives#testExceedsCapacity should take FSN read lock when accessing pendingCached list |  Major | . | Yongjun Zhang | Andrew Wang |
| [HDFS-6500](https://issues.apache.org/jira/browse/HDFS-6500) | Snapshot shouldn't be removed silently after renaming to an existing snapshot |  Blocker | snapshots | Junping Du | Tsz Wo Nicholas Sze |
| [HDFS-6497](https://issues.apache.org/jira/browse/HDFS-6497) | Make TestAvailableSpaceVolumeChoosingPolicy deterministic |  Minor | test | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-6493](https://issues.apache.org/jira/browse/HDFS-6493) | Change dfs.namenode.startup.delay.block.deletion to second instead of millisecond |  Trivial | . | Juan Yu | Juan Yu |
| [HDFS-6487](https://issues.apache.org/jira/browse/HDFS-6487) | TestStandbyCheckpoint#testSBNCheckpoints is racy |  Major | . | Mit Desai | Mit Desai |
| [HDFS-6475](https://issues.apache.org/jira/browse/HDFS-6475) | WebHdfs clients fail without retry because incorrect handling of StandbyException |  Major | ha, webhdfs | Yongjun Zhang | Yongjun Zhang |
| [HDFS-6472](https://issues.apache.org/jira/browse/HDFS-6472) | fix typo in webapps/hdfs/explorer.js |  Trivial | . | Juan Yu | Juan Yu |
| [HDFS-6471](https://issues.apache.org/jira/browse/HDFS-6471) | Make moveFromLocal CLI testcases to be non-disruptive |  Major | test | Dasha Boudnik | Dasha Boudnik |
| [HDFS-6470](https://issues.apache.org/jira/browse/HDFS-6470) | TestBPOfferService.testBPInitErrorHandling is flaky |  Major | . | Andrew Wang | Ming Ma |
| [HDFS-6464](https://issues.apache.org/jira/browse/HDFS-6464) | Support multiple xattr.name parameters for WebHDFS getXAttrs. |  Major | webhdfs | Yi Liu | Yi Liu |
| [HDFS-6462](https://issues.apache.org/jira/browse/HDFS-6462) | NFS: fsstat request fails with the secure hdfs |  Major | nfs | Yesha Vora | Brandon Li |
| [HDFS-6461](https://issues.apache.org/jira/browse/HDFS-6461) | Use Time#monotonicNow to compute duration in DataNode#shutDown |  Trivial | datanode | James Thomas | James Thomas |
| [HDFS-6443](https://issues.apache.org/jira/browse/HDFS-6443) | Fix MiniQJMHACluster related test failures |  Minor | test | Zesheng Wu | Zesheng Wu |
| [HDFS-6439](https://issues.apache.org/jira/browse/HDFS-6439) | NFS should not reject NFS requests to the NULL procedure whether port monitoring is enabled or not |  Major | nfs | Brandon Li | Aaron T. Myers |
| [HDFS-6438](https://issues.apache.org/jira/browse/HDFS-6438) | DeleteSnapshot should be a DELETE request in WebHdfs |  Major | webhdfs | Jing Zhao | Jing Zhao |
| [HDFS-6424](https://issues.apache.org/jira/browse/HDFS-6424) | blockReport doesn't need to invalidate blocks on SBN |  Major | . | Ming Ma | Ming Ma |
| [HDFS-6423](https://issues.apache.org/jira/browse/HDFS-6423) | Diskspace quota usage should be updated when appending data to partial block |  Major | namenode | Jing Zhao | Jing Zhao |
| [HDFS-6422](https://issues.apache.org/jira/browse/HDFS-6422) | getfattr in CLI doesn't throw exception or return non-0 return code when xattr doesn't exist |  Blocker | . | Charles Lamb | Charles Lamb |
| [HDFS-6421](https://issues.apache.org/jira/browse/HDFS-6421) | Fix vecsum.c compile on BSD and some other systems |  Major | libhdfs | Jason Lowe | Mit Desai |
| [HDFS-6418](https://issues.apache.org/jira/browse/HDFS-6418) | Regression: DFS\_NAMENODE\_USER\_NAME\_KEY missing in trunk |  Blocker | hdfs-client | Steve Loughran | Tsz Wo Nicholas Sze |
| [HDFS-6409](https://issues.apache.org/jira/browse/HDFS-6409) | Fix typo in log message about NameNode layout version upgrade. |  Trivial | namenode | Chris Nauroth | Chen He |
| [HDFS-6404](https://issues.apache.org/jira/browse/HDFS-6404) | HttpFS should use a 000 umask for mkdir and create operations |  Major | . | Alejandro Abdelnur | Mike Yoder |
| [HDFS-6400](https://issues.apache.org/jira/browse/HDFS-6400) | Cannot execute "hdfs oiv\_legacy" |  Critical | tools | Akira AJISAKA | Akira AJISAKA |
| [HDFS-6399](https://issues.apache.org/jira/browse/HDFS-6399) | Add note about setfacl in HDFS permissions guide |  Minor | documentation, namenode | Charles Lamb | Chris Nauroth |
| [HDFS-6395](https://issues.apache.org/jira/browse/HDFS-6395) | Skip checking xattr limits for non-user-visible namespaces |  Major | namenode | Andrew Wang | Yi Liu |
| [HDFS-6381](https://issues.apache.org/jira/browse/HDFS-6381) | Fix a typo in INodeReference.java |  Trivial | documentation | Binglin Chang | Binglin Chang |
| [HDFS-6379](https://issues.apache.org/jira/browse/HDFS-6379) | HTTPFS - Implement ACLs support |  Major | . | Alejandro Abdelnur | Mike Yoder |
| [HDFS-6378](https://issues.apache.org/jira/browse/HDFS-6378) | NFS registration should timeout instead of hanging when portmap/rpcbind is not available |  Major | nfs | Brandon Li | Abhiraj Butala |
| [HDFS-6370](https://issues.apache.org/jira/browse/HDFS-6370) | Web UI fails to display in intranet under IE |  Major | datanode, journal-node, namenode | Haohui Mai | Haohui Mai |
| [HDFS-6367](https://issues.apache.org/jira/browse/HDFS-6367) | EnumSetParam$Domain#parse fails for parameter containing more than one enum. |  Major | webhdfs | Yi Liu | Yi Liu |
| [HDFS-6364](https://issues.apache.org/jira/browse/HDFS-6364) | Incorrect check for unknown datanode in Balancer |  Major | balancer & mover | Benoy Antony | Benoy Antony |
| [HDFS-6355](https://issues.apache.org/jira/browse/HDFS-6355) | Fix divide-by-zero, improper use of wall-clock time in BlockPoolSliceScanner |  Major | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-6345](https://issues.apache.org/jira/browse/HDFS-6345) | DFS.listCacheDirectives() should allow filtering based on cache directive ID |  Major | caching | Lenni Kuff | Andrew Wang |
| [HDFS-6337](https://issues.apache.org/jira/browse/HDFS-6337) | Setfacl testcase is failing due to dash character in username in TestAclCLI |  Major | test | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-6312](https://issues.apache.org/jira/browse/HDFS-6312) | WebHdfs HA failover is broken on secure clusters |  Blocker | webhdfs | Daryn Sharp | Daryn Sharp |
| [HDFS-6305](https://issues.apache.org/jira/browse/HDFS-6305) | WebHdfs response decoding may throw RuntimeExceptions |  Critical | webhdfs | Daryn Sharp | Daryn Sharp |
| [HDFS-6294](https://issues.apache.org/jira/browse/HDFS-6294) | Use INode IDs to avoid conflicts when a file open for write is renamed |  Major | namenode | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-6293](https://issues.apache.org/jira/browse/HDFS-6293) | Issues with OIV processing PB-based fsimages |  Blocker | . | Kihwal Lee | Kihwal Lee |
| [HDFS-6289](https://issues.apache.org/jira/browse/HDFS-6289) | HA failover can fail if there are pending DN messages for DNs which no longer exist |  Critical | ha | Aaron T. Myers | Aaron T. Myers |
| [HDFS-6288](https://issues.apache.org/jira/browse/HDFS-6288) | DFSInputStream Pread doesn't update ReadStatistics |  Minor | . | Juan Yu | Juan Yu |
| [HDFS-6270](https://issues.apache.org/jira/browse/HDFS-6270) | Secondary namenode status page shows transaction count in bytes |  Minor | . | Benoy Antony | Benoy Antony |
| [HDFS-6250](https://issues.apache.org/jira/browse/HDFS-6250) | TestBalancerWithNodeGroup.testBalancerWithRackLocality fails |  Major | . | Kihwal Lee | Binglin Chang |
| [HDFS-6243](https://issues.apache.org/jira/browse/HDFS-6243) | HA NameNode transition to active or shutdown may leave lingering image transfer thread. |  Major | ha, namenode | Chris Nauroth | Chris Nauroth |
| [HDFS-6238](https://issues.apache.org/jira/browse/HDFS-6238) | TestDirectoryScanner leaks file descriptors. |  Minor | datanode, test | Chris Nauroth | Chris Nauroth |
| [HDFS-6230](https://issues.apache.org/jira/browse/HDFS-6230) | Expose upgrade status through NameNode web UI |  Major | namenode | Arpit Agarwal | Mit Desai |
| [HDFS-6227](https://issues.apache.org/jira/browse/HDFS-6227) | ShortCircuitCache#unref should purge ShortCircuitReplicas whose streams have been closed by java interrupts |  Major | . | Jing Zhao | Colin Patrick McCabe |
| [HDFS-6222](https://issues.apache.org/jira/browse/HDFS-6222) | Remove background token renewer from webhdfs |  Major | webhdfs | Daryn Sharp | Daryn Sharp |
| [HDFS-6216](https://issues.apache.org/jira/browse/HDFS-6216) | Issues with webhdfs and http proxies |  Major | webhdfs | Daryn Sharp | Daryn Sharp |
| [HDFS-6214](https://issues.apache.org/jira/browse/HDFS-6214) | Webhdfs has poor throughput for files \>2GB |  Major | webhdfs | Daryn Sharp | Daryn Sharp |
| [HDFS-6213](https://issues.apache.org/jira/browse/HDFS-6213) | TestDataNodeConfig failing on Jenkins runs due to DN web port in use |  Minor | test | Steve Loughran | Andrew Wang |
| [HDFS-6194](https://issues.apache.org/jira/browse/HDFS-6194) | Create new tests for ByteRangeInputStream |  Major | . | Haohui Mai | Akira AJISAKA |
| [HDFS-6190](https://issues.apache.org/jira/browse/HDFS-6190) | minor textual fixes in DFSClient |  Trivial | tools | Charles Lamb | Charles Lamb |
| [HDFS-6181](https://issues.apache.org/jira/browse/HDFS-6181) | Fix the wrong property names in NFS user guide |  Trivial | documentation, nfs | Brandon Li | Brandon Li |
| [HDFS-6180](https://issues.apache.org/jira/browse/HDFS-6180) | dead node count / listing is very broken in JMX and old GUI |  Blocker | . | Travis Thompson | Haohui Mai |
| [HDFS-6178](https://issues.apache.org/jira/browse/HDFS-6178) | Decommission on standby NN couldn't finish |  Major | namenode | Ming Ma | Ming Ma |
| [HDFS-6160](https://issues.apache.org/jira/browse/HDFS-6160) | TestSafeMode occasionally fails |  Major | test | Ted Yu | Arpit Agarwal |
| [HDFS-6159](https://issues.apache.org/jira/browse/HDFS-6159) | TestBalancerWithNodeGroup.testBalancerWithNodeGroup fails if there is block missing after balancer success |  Major | test | Chen He | Chen He |
| [HDFS-6156](https://issues.apache.org/jira/browse/HDFS-6156) | Simplify the JMX API that provides snapshot information |  Major | . | Haohui Mai | Shinichi Yamashita |
| [HDFS-6143](https://issues.apache.org/jira/browse/HDFS-6143) | WebHdfsFileSystem open should throw FileNotFoundException for non-existing paths |  Blocker | . | Gera Shegalov | Gera Shegalov |
| [HDFS-6112](https://issues.apache.org/jira/browse/HDFS-6112) | NFS Gateway docs are incorrect for allowed hosts configuration |  Minor | nfs | Aaron T. Myers | Aaron T. Myers |
| [HDFS-6056](https://issues.apache.org/jira/browse/HDFS-6056) | Clean up NFS config settings |  Major | nfs | Aaron T. Myers | Brandon Li |
| [HDFS-5669](https://issues.apache.org/jira/browse/HDFS-5669) | Storage#tryLock() should check for null before logging successfull message |  Major | datanode | Vinayakumar B | Vinayakumar B |
| [HDFS-5591](https://issues.apache.org/jira/browse/HDFS-5591) | Checkpointing should use monotonic time when calculating period |  Minor | namenode | Andrew Wang | Charles Lamb |
| [HDFS-5522](https://issues.apache.org/jira/browse/HDFS-5522) | Datanode disk error check may be incorrectly skipped |  Major | . | Kihwal Lee | Rushabh S Shah |
| [HDFS-4913](https://issues.apache.org/jira/browse/HDFS-4913) | Deleting file through fuse-dfs when using trash fails requiring root permissions |  Major | fuse-dfs | Stephen Chu | Colin Patrick McCabe |
| [HDFS-4909](https://issues.apache.org/jira/browse/HDFS-4909) | Avoid protocol buffer RPC namespace clashes |  Blocker | datanode, journal-node, namenode | Ralph Castain | Colin Patrick McCabe |
| [HDFS-3848](https://issues.apache.org/jira/browse/HDFS-3848) | A Bug in recoverLeaseInternal method of FSNameSystem class |  Major | namenode | Hooman Peiro Sajjad | Chen He |
| [HDFS-3828](https://issues.apache.org/jira/browse/HDFS-3828) | Block Scanner rescans blocks too frequently |  Major | . | Andy Isaacson | Andy Isaacson |
| [HDFS-3493](https://issues.apache.org/jira/browse/HDFS-3493) | Invalidate excess corrupted blocks as long as minimum replication is satisfied |  Major | namenode | J.Andreina | Juan Yu |
| [HDFS-3087](https://issues.apache.org/jira/browse/HDFS-3087) | Decomissioning on NN restart can complete without blocks being replicated |  Critical | namenode | Kihwal Lee | Rushabh S Shah |
| [MAPREDUCE-6002](https://issues.apache.org/jira/browse/MAPREDUCE-6002) | MR task should prevent report error to AM when process is shutting down |  Major | task | Wangda Tan | Wangda Tan |
| [MAPREDUCE-5952](https://issues.apache.org/jira/browse/MAPREDUCE-5952) | LocalContainerLauncher#renameMapOutputForReduce incorrectly assumes a single dir for mapOutIndex |  Blocker | mr-am, mrv2 | Gera Shegalov | Gera Shegalov |
| [MAPREDUCE-5939](https://issues.apache.org/jira/browse/MAPREDUCE-5939) | StartTime showing up as the epoch time in JHS UI after upgrade |  Major | . | Kihwal Lee | Chen He |
| [MAPREDUCE-5924](https://issues.apache.org/jira/browse/MAPREDUCE-5924) | Windows: Sort Job failed due to 'Invalid event: TA\_COMMIT\_PENDING at COMMIT\_PENDING' |  Major | . | Yesha Vora | Zhijie Shen |
| [MAPREDUCE-5920](https://issues.apache.org/jira/browse/MAPREDUCE-5920) | Add Xattr option in DistCp docs |  Minor | distcp, documentation | Uma Maheswara Rao G | Yi Liu |
| [MAPREDUCE-5898](https://issues.apache.org/jira/browse/MAPREDUCE-5898) | distcp to support preserving HDFS extended attributes(XAttrs) |  Major | distcp | Uma Maheswara Rao G | Yi Liu |
| [MAPREDUCE-5895](https://issues.apache.org/jira/browse/MAPREDUCE-5895) | FileAlreadyExistsException was thrown : Temporary Index File can not be cleaned up because OutputStream doesn't close properly |  Major | client | Kousuke Saruta | Kousuke Saruta |
| [MAPREDUCE-5888](https://issues.apache.org/jira/browse/MAPREDUCE-5888) | Failed job leaves hung AM after it unregisters |  Major | mr-am | Jason Lowe | Jason Lowe |
| [MAPREDUCE-5884](https://issues.apache.org/jira/browse/MAPREDUCE-5884) | History server uses short user name when canceling tokens |  Major | jobhistoryserver, security | Mohammad Kamrul Islam | Mohammad Kamrul Islam |
| [MAPREDUCE-5874](https://issues.apache.org/jira/browse/MAPREDUCE-5874) | Creating MapReduce REST API section |  Major | documentation | Ravi Prakash | Tsuyoshi Ozawa |
| [MAPREDUCE-5868](https://issues.apache.org/jira/browse/MAPREDUCE-5868) | TestPipeApplication causing nightly build to fail |  Major | test | Jason Lowe | Akira AJISAKA |
| [MAPREDUCE-5862](https://issues.apache.org/jira/browse/MAPREDUCE-5862) | Line records longer than 2x split size aren't handled correctly |  Critical | . | bc Wong | bc Wong |
| [MAPREDUCE-5846](https://issues.apache.org/jira/browse/MAPREDUCE-5846) | Rumen doesn't understand JobQueueChangedEvent |  Major | tools/rumen | Nathan Roberts | Nathan Roberts |
| [MAPREDUCE-5844](https://issues.apache.org/jira/browse/MAPREDUCE-5844) | Add a configurable delay to reducer-preemption |  Major | . | Maysam Yabandeh | Maysam Yabandeh |
| [MAPREDUCE-5837](https://issues.apache.org/jira/browse/MAPREDUCE-5837) | MRAppMaster fails when checking on uber mode |  Critical | . | Haohui Mai | Haohui Mai |
| [MAPREDUCE-5836](https://issues.apache.org/jira/browse/MAPREDUCE-5836) | Fix typo in RandomTextWriter |  Trivial | . | Akira AJISAKA | Akira AJISAKA |
| [MAPREDUCE-5834](https://issues.apache.org/jira/browse/MAPREDUCE-5834) | TestGridMixClasses tests timesout on branch-2 |  Major | . | Mit Desai | Mit Desai |
| [MAPREDUCE-5814](https://issues.apache.org/jira/browse/MAPREDUCE-5814) | fat jar with \*-default.xml may fail when mapreduce.job.classloader=true. |  Major | mrv2 | Gera Shegalov | Gera Shegalov |
| [MAPREDUCE-5790](https://issues.apache.org/jira/browse/MAPREDUCE-5790) | Default map hprof profile options do not work |  Blocker | . | Andrew Wang | Gera Shegalov |
| [MAPREDUCE-5775](https://issues.apache.org/jira/browse/MAPREDUCE-5775) | Remove unnecessary job.setNumReduceTasks in SleepJob.createJob |  Minor | . | Liyin Liang | jhanver chand sharma |
| [MAPREDUCE-5765](https://issues.apache.org/jira/browse/MAPREDUCE-5765) | Update hadoop-pipes examples README |  Minor | pipes | Jonathan Eagles | Mit Desai |
| [MAPREDUCE-5759](https://issues.apache.org/jira/browse/MAPREDUCE-5759) | Remove unnecessary conf load in Limits |  Major | . | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-5749](https://issues.apache.org/jira/browse/MAPREDUCE-5749) | TestRMContainerAllocator#testReportedAppProgress Failed |  Major | . | Hong Shen | Jason Lowe |
| [MAPREDUCE-5713](https://issues.apache.org/jira/browse/MAPREDUCE-5713) | InputFormat and JobConf JavaDoc Fixes |  Trivial | documentation | Ben Robie | Chen He |
| [MAPREDUCE-5671](https://issues.apache.org/jira/browse/MAPREDUCE-5671) | NaN can be created by client and assign to Progress |  Major | . | Chen He | Chen He |
| [MAPREDUCE-5665](https://issues.apache.org/jira/browse/MAPREDUCE-5665) | Add audience annotations to MiniMRYarnCluster and MiniMRCluster |  Major | test | Sandy Ryza | Anubhav Dhoot |
| [MAPREDUCE-5652](https://issues.apache.org/jira/browse/MAPREDUCE-5652) | NM Recovery. ShuffleHandler should handle NM restarts |  Major | . | Karthik Kambatla | Jason Lowe |
| [MAPREDUCE-5517](https://issues.apache.org/jira/browse/MAPREDUCE-5517) | enabling uber mode with 0 reducer still requires mapreduce.reduce.memory.mb to be less than yarn.app.mapreduce.am.resource.mb |  Minor | . | Siqi Li | Siqi Li |
| [MAPREDUCE-5456](https://issues.apache.org/jira/browse/MAPREDUCE-5456) | TestFetcher.testCopyFromHostExtraBytes is missing |  Minor | mrv2, test | Jason Lowe | Jason Lowe |
| [MAPREDUCE-5309](https://issues.apache.org/jira/browse/MAPREDUCE-5309) | 2.0.4 JobHistoryParser can't parse certain failed job history files generated by 2.0.3 history server |  Major | jobhistoryserver, mrv2 | Vrushali C | Rushabh S Shah |
| [MAPREDUCE-4937](https://issues.apache.org/jira/browse/MAPREDUCE-4937) | MR AM handles an oversized split metainfo file poorly |  Major | mr-am | Jason Lowe | Eric Payne |
| [YARN-2250](https://issues.apache.org/jira/browse/YARN-2250) | FairScheduler.findLowestCommonAncestorQueue returns null when queues not identical |  Major | scheduler | Krisztian Horvath | Krisztian Horvath |
| [YARN-2241](https://issues.apache.org/jira/browse/YARN-2241) | ZKRMStateStore: On startup, show nicer messages if znodes already exist |  Minor | resourcemanager | Robert Kanter | Robert Kanter |
| [YARN-2232](https://issues.apache.org/jira/browse/YARN-2232) | ClientRMService doesn't allow delegation token owner to cancel their own token in secure mode |  Major | . | Varun Vasudev | Varun Vasudev |
| [YARN-2204](https://issues.apache.org/jira/browse/YARN-2204) | TestAMRestart#testAMRestartWithExistingContainers assumes CapacityScheduler |  Trivial | resourcemanager | Robert Kanter | Robert Kanter |
| [YARN-2201](https://issues.apache.org/jira/browse/YARN-2201) | TestRMWebServicesAppsModification dependent on yarn-default.xml |  Major | . | Ray Chiang | Varun Vasudev |
| [YARN-2192](https://issues.apache.org/jira/browse/YARN-2192) | TestRMHA fails when run with a mix of Schedulers |  Major | . | Anubhav Dhoot | Anubhav Dhoot |
| [YARN-2191](https://issues.apache.org/jira/browse/YARN-2191) | Add a test to make sure NM will do application cleanup even if RM restarting happens before application completed |  Major | resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-2187](https://issues.apache.org/jira/browse/YARN-2187) | FairScheduler: Disable max-AM-share check by default |  Major | fairscheduler | Robert Kanter | Robert Kanter |
| [YARN-2171](https://issues.apache.org/jira/browse/YARN-2171) | AMs block on the CapacityScheduler lock during allocate() |  Critical | capacityscheduler | Jason Lowe | Jason Lowe |
| [YARN-2167](https://issues.apache.org/jira/browse/YARN-2167) | LeveldbIterator should get closed in NMLeveldbStateStoreService#loadLocalizationState() within finally block |  Major | nodemanager | Junping Du | Junping Du |
| [YARN-2163](https://issues.apache.org/jira/browse/YARN-2163) | WebUI: Order of AppId in apps table should be consistent with ApplicationId.compareTo(). |  Minor | resourcemanager, webapp | Wangda Tan | Wangda Tan |
| [YARN-2155](https://issues.apache.org/jira/browse/YARN-2155) | FairScheduler: Incorrect threshold check for preemption |  Major | . | Wei Yan | Wei Yan |
| [YARN-2148](https://issues.apache.org/jira/browse/YARN-2148) | TestNMClient failed due more exit code values added and passed to AM |  Major | client | Wangda Tan | Wangda Tan |
| [YARN-2132](https://issues.apache.org/jira/browse/YARN-2132) | ZKRMStateStore.ZKAction#runWithRetries doesn't log the exception it encounters |  Major | resourcemanager | Karthik Kambatla | Vamsee Yarlagadda |
| [YARN-2128](https://issues.apache.org/jira/browse/YARN-2128) | FairScheduler: Incorrect calculation of amResource usage |  Major | . | Wei Yan | Wei Yan |
| [YARN-2124](https://issues.apache.org/jira/browse/YARN-2124) | ProportionalCapacityPreemptionPolicy cannot work because it's initialized before scheduler initialized |  Critical | resourcemanager, scheduler | Wangda Tan | Wangda Tan |
| [YARN-2122](https://issues.apache.org/jira/browse/YARN-2122) | In AllocationFileLoaderService, the reloadThread should be created in init() and started in start() |  Major | scheduler | Karthik Kambatla | Robert Kanter |
| [YARN-2119](https://issues.apache.org/jira/browse/YARN-2119) | DEFAULT\_PROXY\_ADDRESS should use DEFAULT\_PROXY\_PORT |  Major | . | Anubhav Dhoot | Anubhav Dhoot |
| [YARN-2112](https://issues.apache.org/jira/browse/YARN-2112) | Hadoop-client is missing jackson libs due to inappropriate configs in pom.xml |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-2111](https://issues.apache.org/jira/browse/YARN-2111) | In FairScheduler.attemptScheduling, we don't count containers as assigned if they have 0 memory but non-zero cores |  Major | scheduler | Sandy Ryza | Sandy Ryza |
| [YARN-2109](https://issues.apache.org/jira/browse/YARN-2109) | Fix TestRM to work with both schedulers |  Major | scheduler | Anubhav Dhoot | Karthik Kambatla |
| [YARN-2104](https://issues.apache.org/jira/browse/YARN-2104) | Scheduler queue filter failed to work because index of queue column changed |  Major | resourcemanager, webapp | Wangda Tan | Wangda Tan |
| [YARN-2103](https://issues.apache.org/jira/browse/YARN-2103) | Inconsistency between viaProto flag and initial value of SerializedExceptionProto.Builder |  Major | . | Binglin Chang | Binglin Chang |
| [YARN-2096](https://issues.apache.org/jira/browse/YARN-2096) | Race in TestRMRestart#testQueueMetricsOnRMRestart |  Major | . | Anubhav Dhoot | Anubhav Dhoot |
| [YARN-2075](https://issues.apache.org/jira/browse/YARN-2075) | TestRMAdminCLI consistently fail on trunk and branch-2 |  Major | . | Zhijie Shen | Kenji Kikushima |
| [YARN-2073](https://issues.apache.org/jira/browse/YARN-2073) | Fair Scheduler: Add a utilization threshold to prevent preempting resources when cluster is free |  Critical | scheduler | Karthik Kambatla | Karthik Kambatla |
| [YARN-2065](https://issues.apache.org/jira/browse/YARN-2065) | AM cannot create new containers after restart-NM token from previous attempt used |  Major | . | Steve Loughran | Jian He |
| [YARN-2054](https://issues.apache.org/jira/browse/YARN-2054) | Better defaults for YARN ZK configs for retries and retry-inteval when HA is enabled |  Major | resourcemanager | Karthik Kambatla | Karthik Kambatla |
| [YARN-2050](https://issues.apache.org/jira/browse/YARN-2050) | Fix LogCLIHelpers to create the correct FileContext |  Major | . | Ming Ma | Ming Ma |
| [YARN-2036](https://issues.apache.org/jira/browse/YARN-2036) | Document yarn.resourcemanager.hostname in ClusterSetup |  Minor | documentation | Karthik Kambatla | Ray Chiang |
| [YARN-1981](https://issues.apache.org/jira/browse/YARN-1981) | Nodemanager version is not updated when a node reconnects |  Major | resourcemanager | Jason Lowe | Jason Lowe |
| [YARN-1940](https://issues.apache.org/jira/browse/YARN-1940) | deleteAsUser() terminates early without deleting more files on error |  Major | . | Kihwal Lee | Rushabh S Shah |
| [YARN-1913](https://issues.apache.org/jira/browse/YARN-1913) | With Fair Scheduler, cluster can logjam when all resources are consumed by AMs |  Major | scheduler | bc Wong | Wei Yan |
| [YARN-1885](https://issues.apache.org/jira/browse/YARN-1885) | RM may not send the app-finished signal after RM restart to some nodes where the application ran before RM restarts |  Major | . | Arpit Gupta | Wangda Tan |
| [YARN-1868](https://issues.apache.org/jira/browse/YARN-1868) | YARN status web ui does not show correctly in IE 11 |  Major | webapp | Chuan Liu | Chuan Liu |
| [YARN-1865](https://issues.apache.org/jira/browse/YARN-1865) | ShellScriptBuilder does not check for some error conditions |  Minor | nodemanager | Remus Rusanu | Remus Rusanu |
| [YARN-1790](https://issues.apache.org/jira/browse/YARN-1790) | Fair Scheduler UI not showing apps table |  Major | . | bc Wong | bc Wong |
| [YARN-1784](https://issues.apache.org/jira/browse/YARN-1784) | TestContainerAllocation assumes CapacityScheduler |  Minor | resourcemanager | Karthik Kambatla | Robert Kanter |
| [YARN-1736](https://issues.apache.org/jira/browse/YARN-1736) | FS: AppSchedulable.assignContainer's priority argument is redundant |  Minor | scheduler | Sandy Ryza | Naren Koneru |
| [YARN-1726](https://issues.apache.org/jira/browse/YARN-1726) | ResourceSchedulerWrapper broken due to AbstractYarnScheduler |  Blocker | . | Wei Yan | Wei Yan |
| [YARN-1718](https://issues.apache.org/jira/browse/YARN-1718) | Fix a couple isTerminals in Fair Scheduler queue placement rules |  Major | scheduler | Sandy Ryza | Sandy Ryza |
| [YARN-1678](https://issues.apache.org/jira/browse/YARN-1678) | Fair scheduler gabs incessantly about reservations |  Major | scheduler | Sandy Ryza | Sandy Ryza |
| [YARN-1550](https://issues.apache.org/jira/browse/YARN-1550) | NPE in FairSchedulerAppsBlock#render |  Critical | fairscheduler | caolong | Anubhav Dhoot |
| [YARN-1520](https://issues.apache.org/jira/browse/YARN-1520) | update capacity scheduler docs to include necessary parameters |  Major | . | Chen He | Chen He |
| [YARN-1429](https://issues.apache.org/jira/browse/YARN-1429) | \*nix: Allow a way for users to augment classpath of YARN daemons |  Trivial | client | Sandy Ryza | Jarek Jarcec Cecho |
| [YARN-1136](https://issues.apache.org/jira/browse/YARN-1136) | Replace junit.framework.Assert with org.junit.Assert |  Major | . | Karthik Kambatla | Chen He |
| [YARN-738](https://issues.apache.org/jira/browse/YARN-738) | TestClientRMTokens is failing irregularly while running all yarn tests |  Major | . | Omkar Vinit Joshi | Ming Ma |
| [YARN-596](https://issues.apache.org/jira/browse/YARN-596) | Use scheduling policies throughout the queue hierarchy to decide which containers to preempt |  Major | scheduler | Sandy Ryza | Wei Yan |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-10754](https://issues.apache.org/jira/browse/HADOOP-10754) | Reenable several HA ZooKeeper-related tests on Windows. |  Trivial | ha, test | Chris Nauroth | Chris Nauroth |
| [HADOOP-3679](https://issues.apache.org/jira/browse/HADOOP-3679) | calls to junit Assert::assertEquals invert arguments, causing misleading error messages, other minor improvements. |  Minor | test | Chris Douglas | jay vyas |
| [HDFS-6614](https://issues.apache.org/jira/browse/HDFS-6614) | shorten TestPread run time with a smaller retry timeout setting |  Minor | test | Liang Xie | Liang Xie |
| [HDFS-6419](https://issues.apache.org/jira/browse/HDFS-6419) | TestBookKeeperHACheckpoints#TestSBNCheckpoints fails on trunk |  Major | . | Akira AJISAKA | Akira AJISAKA |
| [HDFS-6287](https://issues.apache.org/jira/browse/HDFS-6287) | Add vecsum test of libhdfs read access times |  Minor | libhdfs, test | Colin Patrick McCabe | Colin Patrick McCabe |
| [HDFS-6265](https://issues.apache.org/jira/browse/HDFS-6265) | Prepare HDFS codebase for JUnit 4.11. |  Minor | test | Chris Nauroth | Chris Nauroth |
| [HDFS-6257](https://issues.apache.org/jira/browse/HDFS-6257) | TestCacheDirectives#testExceedsCapacity fails occasionally |  Minor | caching | Ted Yu | Colin Patrick McCabe |
| [HDFS-6224](https://issues.apache.org/jira/browse/HDFS-6224) | Add a unit test to TestAuditLogger for file permissions passed to logAuditEvent |  Minor | test | Charles Lamb | Charles Lamb |
| [HDFS-5892](https://issues.apache.org/jira/browse/HDFS-5892) | TestDeleteBlockPool fails in branch-2 |  Minor | . | Ted Yu | Ted Yu |
| [HDFS-5409](https://issues.apache.org/jira/browse/HDFS-5409) | TestOfflineEditsViewer#testStored fails on Windows due to CRLF line endings in editsStored.xml from git checkout |  Minor | test | Chris Nauroth | Chris Nauroth |
| [MAPREDUCE-5852](https://issues.apache.org/jira/browse/MAPREDUCE-5852) | Prepare MapReduce codebase for JUnit 4.11. |  Minor | test | Chris Nauroth | Chris Nauroth |
| [MAPREDUCE-5804](https://issues.apache.org/jira/browse/MAPREDUCE-5804) | TestMRJobsWithProfiler#testProfiler timesout |  Major | . | Mit Desai | Mit Desai |
| [MAPREDUCE-5642](https://issues.apache.org/jira/browse/MAPREDUCE-5642) | TestMiniMRChildTask fails on Windows |  Minor | test | Chuan Liu | Chuan Liu |
| [YARN-2319](https://issues.apache.org/jira/browse/YARN-2319) | Fix MiniKdc not close in TestRMWebServicesDelegationTokens.java |  Major | resourcemanager | Wenwu Peng | Wenwu Peng |
| [YARN-2270](https://issues.apache.org/jira/browse/YARN-2270) | TestFSDownload#testDownloadPublicWithStatCache fails in trunk |  Minor | . | Ted Yu | Akira AJISAKA |
| [YARN-2224](https://issues.apache.org/jira/browse/YARN-2224) | Explicitly enable vmem check in TestContainersMonitor#testContainerKillOnMemoryOverflow |  Trivial | nodemanager | Anubhav Dhoot | Anubhav Dhoot |
| [YARN-2216](https://issues.apache.org/jira/browse/YARN-2216) | TestRMApplicationHistoryWriter sometimes fails in trunk |  Minor | . | Ted Yu | Zhijie Shen |
| [YARN-2105](https://issues.apache.org/jira/browse/YARN-2105) | Fix TestFairScheduler after YARN-2012 |  Major | . | Ted Yu | Ashwin Shankar |
| [YARN-2011](https://issues.apache.org/jira/browse/YARN-2011) | Fix typo and warning in TestLeafQueue |  Trivial | . | Chen He | Chen He |
| [YARN-1977](https://issues.apache.org/jira/browse/YARN-1977) | Add tests on getApplicationRequest with filtering start time range |  Minor | . | Junping Du | Junping Du |
| [YARN-1970](https://issues.apache.org/jira/browse/YARN-1970) | Prepare YARN codebase for JUnit 4.11. |  Minor | . | Chris Nauroth | Chris Nauroth |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-10894](https://issues.apache.org/jira/browse/HADOOP-10894) | Fix dead link in ToolRunner documentation |  Minor | documentation | Akira AJISAKA | Akira AJISAKA |
| [HADOOP-10864](https://issues.apache.org/jira/browse/HADOOP-10864) | Tool documentenation is broken |  Minor | documentation | Allen Wittenauer | Akira AJISAKA |
| [HADOOP-10659](https://issues.apache.org/jira/browse/HADOOP-10659) | Refactor AccessControlList to reuse utility functions and to improve performance |  Minor | security | Benoy Antony | Benoy Antony |
| [HADOOP-10652](https://issues.apache.org/jira/browse/HADOOP-10652) | Refactor Proxyusers to use AccessControlList |  Major | security | Benoy Antony | Benoy Antony |
| [HADOOP-10649](https://issues.apache.org/jira/browse/HADOOP-10649) | Allow overriding the default ACL for service authorization |  Major | security | Benoy Antony | Benoy Antony |
| [HADOOP-10566](https://issues.apache.org/jira/browse/HADOOP-10566) | Refactor proxyservers out of ProxyUsers |  Major | security | Benoy Antony | Benoy Antony |
| [HADOOP-10565](https://issues.apache.org/jira/browse/HADOOP-10565) | Support IP ranges (CIDR) in  proxyuser.hosts |  Major | security | Benoy Antony | Benoy Antony |
| [HADOOP-10503](https://issues.apache.org/jira/browse/HADOOP-10503) | Move junit up to v 4.11 |  Minor | build | Steve Loughran | Chris Nauroth |
| [HADOOP-10499](https://issues.apache.org/jira/browse/HADOOP-10499) | Remove unused parameter from ProxyUsers.authorize() |  Minor | security | Benoy Antony | Benoy Antony |
| [HADOOP-10479](https://issues.apache.org/jira/browse/HADOOP-10479) | Fix new findbugs warnings in hadoop-minikdc |  Major | . | Haohui Mai | Swarnim Kulkarni |
| [HADOOP-10471](https://issues.apache.org/jira/browse/HADOOP-10471) | Reduce the visibility of constants in ProxyUsers |  Major | security | Benoy Antony | Benoy Antony |
| [HADOOP-10467](https://issues.apache.org/jira/browse/HADOOP-10467) | Enable proxyuser specification to support list of users in addition to list of groups. |  Major | security | Benoy Antony | Benoy Antony |
| [HADOOP-10448](https://issues.apache.org/jira/browse/HADOOP-10448) | Support pluggable mechanism to specify proxy user settings |  Major | security | Benoy Antony | Benoy Antony |
| [HADOOP-10439](https://issues.apache.org/jira/browse/HADOOP-10439) | Fix compilation error in branch-2 after HADOOP-10426 |  Major | build | Haohui Mai | Haohui Mai |
| [HADOOP-10426](https://issues.apache.org/jira/browse/HADOOP-10426) | CreateOpts.getOpt(..) should declare with generic type argument |  Minor | fs | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-10279](https://issues.apache.org/jira/browse/HADOOP-10279) | Create multiplexer, a requirement for the fair queue |  Major | . | Chris Li | Chris Li |
| [HADOOP-10104](https://issues.apache.org/jira/browse/HADOOP-10104) | Update jackson to 1.9.13 |  Minor | build | Steve Loughran | Akira AJISAKA |
| [HADOOP-9712](https://issues.apache.org/jira/browse/HADOOP-9712) | Write contract tests for FTP filesystem, fix places where it breaks |  Minor | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-9711](https://issues.apache.org/jira/browse/HADOOP-9711) | Write contract tests for S3Native; fix places where it breaks |  Minor | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-9371](https://issues.apache.org/jira/browse/HADOOP-9371) | Define Semantics of FileSystem more rigorously |  Major | fs | Steve Loughran | Steve Loughran |
| [HDFS-6562](https://issues.apache.org/jira/browse/HDFS-6562) | Refactor rename() in FSDirectory |  Minor | namenode | Haohui Mai | Haohui Mai |
| [HDFS-6557](https://issues.apache.org/jira/browse/HDFS-6557) | Move the reference of fsimage to FSNamesystem |  Major | namenode | Haohui Mai | Haohui Mai |
| [HDFS-6480](https://issues.apache.org/jira/browse/HDFS-6480) | Move waitForReady() from FSDirectory to FSNamesystem |  Major | namenode | Haohui Mai | Haohui Mai |
| [HDFS-6351](https://issues.apache.org/jira/browse/HDFS-6351) | Command "hdfs dfs -rm -r" can't remove empty directory |  Major | hdfs-client | Yongjun Zhang | Yongjun Zhang |
| [HDFS-6330](https://issues.apache.org/jira/browse/HDFS-6330) | Move mkdirs() to FSNamesystem |  Major | namenode | Haohui Mai | Haohui Mai |
| [HDFS-6328](https://issues.apache.org/jira/browse/HDFS-6328) | Clean up dead code in FSDirectory |  Major | namenode | Haohui Mai | Haohui Mai |
| [HDFS-6315](https://issues.apache.org/jira/browse/HDFS-6315) | Decouple recording edit logs from FSDirectory |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-6276](https://issues.apache.org/jira/browse/HDFS-6276) | Remove unnecessary conditions and null check |  Major | . | Suresh Srinivas | Suresh Srinivas |
| [HDFS-6275](https://issues.apache.org/jira/browse/HDFS-6275) | Fix warnings - type arguments can be inferred and redudant local variable |  Major | . | Suresh Srinivas | Suresh Srinivas |
| [HDFS-6274](https://issues.apache.org/jira/browse/HDFS-6274) | Cleanup javadoc warnings in HDFS code |  Major | . | Suresh Srinivas | Suresh Srinivas |
| [HDFS-6240](https://issues.apache.org/jira/browse/HDFS-6240) | WebImageViewer returns 404 if LISTSTATUS to an empty directory |  Major | tools | Akira AJISAKA | Akira AJISAKA |
| [HDFS-6219](https://issues.apache.org/jira/browse/HDFS-6219) | Proxy superuser configuration should use true client IP for address checks |  Major | namenode, webhdfs | Daryn Sharp | Daryn Sharp |
| [HDFS-6218](https://issues.apache.org/jira/browse/HDFS-6218) | Audit log should use true client IP for proxied webhdfs operations |  Major | namenode, webhdfs | Daryn Sharp | Daryn Sharp |
| [HDFS-6217](https://issues.apache.org/jira/browse/HDFS-6217) | Webhdfs PUT operations may not work via a http proxy |  Major | webhdfs | Daryn Sharp | Daryn Sharp |
| [HDFS-6210](https://issues.apache.org/jira/browse/HDFS-6210) | Support GETACLSTATUS operation in WebImageViewer |  Major | tools | Akira AJISAKA | Akira AJISAKA |
| [HDFS-6186](https://issues.apache.org/jira/browse/HDFS-6186) | Pause deletion of blocks when the namenode starts up |  Major | namenode | Suresh Srinivas | Jing Zhao |
| [HDFS-6173](https://issues.apache.org/jira/browse/HDFS-6173) | Move the default processor from Ls to Web in OfflineImageViewer |  Major | tools | Akira AJISAKA | Akira AJISAKA |
| [HDFS-6170](https://issues.apache.org/jira/browse/HDFS-6170) | Support GETFILESTATUS operation in WebImageViewer |  Major | tools | Akira AJISAKA | Akira AJISAKA |
| [HDFS-6169](https://issues.apache.org/jira/browse/HDFS-6169) | Move the address in WebImageViewer |  Major | tools | Akira AJISAKA | Akira AJISAKA |
| [HDFS-6162](https://issues.apache.org/jira/browse/HDFS-6162) | Format strings should use platform independent line separator |  Minor | . | Suresh Srinivas | Suresh Srinivas |
| [HDFS-6155](https://issues.apache.org/jira/browse/HDFS-6155) | Fix Boxing/unboxing to parse a primitive findbugs warnings |  Major | . | Suresh Srinivas | Suresh Srinivas |
| [HDFS-6125](https://issues.apache.org/jira/browse/HDFS-6125) | Cleanup unnecessary cast in HDFS code base |  Major | test | Suresh Srinivas | Suresh Srinivas |
| [HDFS-6119](https://issues.apache.org/jira/browse/HDFS-6119) | FSNamesystem code cleanup |  Minor | namenode | Suresh Srinivas | Suresh Srinivas |
| [HDFS-5978](https://issues.apache.org/jira/browse/HDFS-5978) | Create a tool to take fsimage and expose read-only WebHDFS API |  Major | tools | Akira AJISAKA | Akira AJISAKA |
| [HDFS-5865](https://issues.apache.org/jira/browse/HDFS-5865) | Update OfflineImageViewer document |  Minor | documentation | Akira AJISAKA | Akira AJISAKA |
| [HDFS-5411](https://issues.apache.org/jira/browse/HDFS-5411) | Update Bookkeeper dependency to 4.2.3 |  Minor | . | Robert Rati | Rakesh R |
| [HDFS-4667](https://issues.apache.org/jira/browse/HDFS-4667) | Capture renamed files/directories in snapshot diff report |  Major | namenode | Jing Zhao | Jing Zhao |
| [HDFS-4286](https://issues.apache.org/jira/browse/HDFS-4286) | Changes from BOOKKEEPER-203 broken capability of including bookkeeper-server jar in hidden package of BKJM |  Major | . | Vinayakumar B | Rakesh R |
| [HDFS-4221](https://issues.apache.org/jira/browse/HDFS-4221) | Remove the format limitation point from BKJM documentation as HDFS-3810 closed |  Major | ha | Uma Maheswara Rao G | Rakesh R |
| [MAPREDUCE-5900](https://issues.apache.org/jira/browse/MAPREDUCE-5900) | Container preemption interpreted as task failures and eventually job failures |  Major | applicationmaster, mr-am, mrv2 | Mayank Bansal | Mayank Bansal |
| [MAPREDUCE-5639](https://issues.apache.org/jira/browse/MAPREDUCE-5639) | Port DistCp2 document to trunk |  Major | documentation | Akira AJISAKA | Akira AJISAKA |
| [MAPREDUCE-5638](https://issues.apache.org/jira/browse/MAPREDUCE-5638) | Port Hadoop Archives document to trunk |  Major | documentation | Akira AJISAKA | Akira AJISAKA |
| [MAPREDUCE-5637](https://issues.apache.org/jira/browse/MAPREDUCE-5637) | Convert Hadoop Streaming document to APT |  Major | documentation | Akira AJISAKA | Akira AJISAKA |
| [MAPREDUCE-5636](https://issues.apache.org/jira/browse/MAPREDUCE-5636) | Convert MapReduce Tutorial document to APT |  Major | documentation | Akira AJISAKA | Akira AJISAKA |
| [YARN-2247](https://issues.apache.org/jira/browse/YARN-2247) | Allow RM web services users to authenticate using delegation tokens |  Blocker | . | Varun Vasudev | Varun Vasudev |
| [YARN-2233](https://issues.apache.org/jira/browse/YARN-2233) | Implement web services to create, renew and cancel delegation tokens |  Blocker | resourcemanager | Varun Vasudev | Varun Vasudev |
| [YARN-2152](https://issues.apache.org/jira/browse/YARN-2152) | Recover missing container information |  Major | resourcemanager | Jian He | Jian He |
| [YARN-2121](https://issues.apache.org/jira/browse/YARN-2121) | TimelineAuthenticator#hasDelegationToken may throw NPE |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-2118](https://issues.apache.org/jira/browse/YARN-2118) | Type mismatch in contains() check of TimelineWebServices#injectOwnerInfo() |  Major | . | Ted Yu | Ted Yu |
| [YARN-2117](https://issues.apache.org/jira/browse/YARN-2117) | Close of Reader in TimelineAuthenticationFilterInitializer#initFilter() should be enclosed in finally block |  Minor | . | Ted Yu | Chen He |
| [YARN-2115](https://issues.apache.org/jira/browse/YARN-2115) | Replace RegisterNodeManagerRequest#ContainerStatus with a new NMContainerStatus |  Major | . | Jian He | Jian He |
| [YARN-2074](https://issues.apache.org/jira/browse/YARN-2074) | Preemption of AM containers shouldn't count towards AM failures |  Major | resourcemanager | Vinod Kumar Vavilapalli | Jian He |
| [YARN-2071](https://issues.apache.org/jira/browse/YARN-2071) | Enforce more restricted permissions for the directory of Leveldb store |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-2059](https://issues.apache.org/jira/browse/YARN-2059) | Extend access control for admin acls |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-2052](https://issues.apache.org/jira/browse/YARN-2052) | ContainerId creation after work preserving restart is broken |  Major | resourcemanager | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [YARN-2049](https://issues.apache.org/jira/browse/YARN-2049) | Delegation token stuff for the timeline sever |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-2022](https://issues.apache.org/jira/browse/YARN-2022) | Preempting an Application Master container can be kept as least priority when multiple applications are marked for preemption by ProportionalCapacityPreemptionPolicy |  Major | resourcemanager | Sunil G | Sunil G |
| [YARN-2017](https://issues.apache.org/jira/browse/YARN-2017) | Merge some of the common lib code in schedulers |  Major | resourcemanager | Jian He | Jian He |
| [YARN-1982](https://issues.apache.org/jira/browse/YARN-1982) | Rename the daemon name to timelineserver |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-1938](https://issues.apache.org/jira/browse/YARN-1938) | Kerberos authentication for the timeline server |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-1937](https://issues.apache.org/jira/browse/YARN-1937) | Add entity-level access control of the timeline data for owners only |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-1936](https://issues.apache.org/jira/browse/YARN-1936) | Secured timeline client |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-1877](https://issues.apache.org/jira/browse/YARN-1877) | Document yarn.resourcemanager.zk-auth and its scope |  Critical | resourcemanager | Karthik Kambatla | Robert Kanter |
| [YARN-1757](https://issues.apache.org/jira/browse/YARN-1757) | NM Recovery. Auxiliary service support. |  Major | nodemanager | Jason Lowe | Jason Lowe |
| [YARN-1713](https://issues.apache.org/jira/browse/YARN-1713) | Implement getnewapplication and submitapp as part of RM web service |  Blocker | . | Varun Vasudev | Varun Vasudev |
| [YARN-1702](https://issues.apache.org/jira/browse/YARN-1702) | Expose kill app functionality as part of RM web services |  Major | . | Varun Vasudev | Varun Vasudev |
| [YARN-1474](https://issues.apache.org/jira/browse/YARN-1474) | Make schedulers services |  Major | scheduler | Sandy Ryza | Tsuyoshi Ozawa |
| [YARN-1408](https://issues.apache.org/jira/browse/YARN-1408) | Preemption caused Invalid State Event: ACQUIRED at KILLED and caused a task timeout for 30mins |  Major | resourcemanager | Sunil G | Sunil G |
| [YARN-1368](https://issues.apache.org/jira/browse/YARN-1368) | Common work to re-populate containers’ state into scheduler |  Major | . | Bikas Saha | Jian He |
| [YARN-1366](https://issues.apache.org/jira/browse/YARN-1366) | AM should implement Resync with the ApplicationMasterService instead of shutting down |  Major | resourcemanager | Bikas Saha | Rohith Sharma K S |
| [YARN-1365](https://issues.apache.org/jira/browse/YARN-1365) | ApplicationMasterService to allow Register of an app that was running before restart |  Major | resourcemanager | Bikas Saha | Anubhav Dhoot |
| [YARN-1362](https://issues.apache.org/jira/browse/YARN-1362) | Distinguish between nodemanager shutdown for decommission vs shutdown for restart |  Major | nodemanager | Jason Lowe | Jason Lowe |
| [YARN-1339](https://issues.apache.org/jira/browse/YARN-1339) | Recover DeletionService state upon nodemanager restart |  Major | nodemanager | Jason Lowe | Jason Lowe |
| [YARN-1338](https://issues.apache.org/jira/browse/YARN-1338) | Recover localized resource cache state upon nodemanager restart |  Major | nodemanager | Jason Lowe | Jason Lowe |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-10821](https://issues.apache.org/jira/browse/HADOOP-10821) | Prepare the release notes for Hadoop 2.5.0 |  Blocker | . | Akira AJISAKA | Andrew Wang |
| [HADOOP-10715](https://issues.apache.org/jira/browse/HADOOP-10715) | Remove public GraphiteSink#setWriter() |  Minor | . | Ted Yu |  |
| [HDFS-6486](https://issues.apache.org/jira/browse/HDFS-6486) | Add user doc for XAttrs via WebHDFS. |  Minor | webhdfs | Yi Liu | Yi Liu |
| [HDFS-6430](https://issues.apache.org/jira/browse/HDFS-6430) | HTTPFS - Implement XAttr support |  Major | . | Yi Liu | Yi Liu |
| [MAPREDUCE-4282](https://issues.apache.org/jira/browse/MAPREDUCE-4282) | Convert Forrest docs to APT |  Major | documentation | Eli Collins | Akira AJISAKA |
| [YARN-2125](https://issues.apache.org/jira/browse/YARN-2125) | ProportionalCapacityPreemptionPolicy should only log CSV when debug enabled |  Minor | resourcemanager, scheduler | Wangda Tan | Wangda Tan |
| [YARN-2091](https://issues.apache.org/jira/browse/YARN-2091) | Add more values to ContainerExitStatus and pass it from NM to RM and then to app masters |  Major | . | Bikas Saha | Tsuyoshi Ozawa |


