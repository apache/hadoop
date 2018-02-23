
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

## Release 2.7.0 - 2015-04-20

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-6252](https://issues.apache.org/jira/browse/HDFS-6252) | Phase out the old web UI in HDFS |  Minor | namenode | Fengdong Yu | Haohui Mai |
| [HADOOP-11311](https://issues.apache.org/jira/browse/HADOOP-11311) | Restrict uppercase key names from being created with JCEKS |  Major | security | Andrew Wang | Andrew Wang |
| [HDFS-7210](https://issues.apache.org/jira/browse/HDFS-7210) | Avoid two separate RPC's namenode.append() and namenode.getFileInfo() for an append call from DFSClient |  Major | hdfs-client, namenode | Vinayakumar B | Vinayakumar B |
| [HADOOP-10530](https://issues.apache.org/jira/browse/HADOOP-10530) | Make hadoop trunk build on Java7+ only |  Blocker | build | Steve Loughran | Steve Loughran |
| [HADOOP-11385](https://issues.apache.org/jira/browse/HADOOP-11385) | Prevent cross site scripting attack on JMXJSONServlet |  Critical | . | Haohui Mai | Haohui Mai |
| [HADOOP-11498](https://issues.apache.org/jira/browse/HADOOP-11498) | Bump the version of HTrace to 3.1.0-incubating |  Major | tracing | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-6651](https://issues.apache.org/jira/browse/HDFS-6651) | Deletion failure can leak inodes permanently |  Critical | . | Kihwal Lee | Jing Zhao |
| [HADOOP-11492](https://issues.apache.org/jira/browse/HADOOP-11492) | Bump up curator version to 2.7.1 |  Major | . | Karthik Kambatla | Arun Suresh |
| [YARN-3217](https://issues.apache.org/jira/browse/YARN-3217) | Remove httpclient dependency from hadoop-yarn-server-web-proxy |  Major | . | Akira Ajisaka | Brahma Reddy Battula |
| [YARN-3154](https://issues.apache.org/jira/browse/YARN-3154) | Should not upload partial logs for MR jobs or other "short-running' applications |  Blocker | nodemanager, resourcemanager | Xuan Gong | Xuan Gong |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-2360](https://issues.apache.org/jira/browse/YARN-2360) | Fair Scheduler: Display dynamic fair share for queues on the scheduler page |  Major | fairscheduler | Ashwin Shankar | Ashwin Shankar |
| [HDFS-7222](https://issues.apache.org/jira/browse/HDFS-7222) | Expose DataNode network errors as a metric |  Minor | datanode | Charles Lamb | Charles Lamb |
| [HDFS-6663](https://issues.apache.org/jira/browse/HDFS-6663) | Admin command to track file and locations from block id |  Major | . | Kihwal Lee | Chen He |
| [HDFS-1362](https://issues.apache.org/jira/browse/HDFS-1362) | Provide volume management functionality for DataNode |  Major | datanode | Wang Xu | Wang Xu |
| [HADOOP-7984](https://issues.apache.org/jira/browse/HADOOP-7984) | Add hadoop --loglevel option to change log level |  Minor | scripts | Eli Collins | Akira Ajisaka |
| [HADOOP-8989](https://issues.apache.org/jira/browse/HADOOP-8989) | hadoop fs -find feature |  Major | . | Marco Nicosia | Jonathan Allen |
| [HDFS-6982](https://issues.apache.org/jira/browse/HDFS-6982) | nntop: top­-like tool for name node users |  Major | . | Maysam Yabandeh | Maysam Yabandeh |
| [HADOOP-11341](https://issues.apache.org/jira/browse/HADOOP-11341) | KMS support for whitelist key ACLs |  Major | kms, security | Arun Suresh | Arun Suresh |
| [HDFS-7424](https://issues.apache.org/jira/browse/HDFS-7424) | Add web UI for NFS gateway |  Major | nfs | Brandon Li | Brandon Li |
| [HDFS-7449](https://issues.apache.org/jira/browse/HDFS-7449) | Add metrics to NFS gateway |  Major | nfs | Brandon Li | Brandon Li |
| [HADOOP-9629](https://issues.apache.org/jira/browse/HADOOP-9629) | Support Windows Azure Storage - Blob as a file system in Hadoop |  Major | tools | Mostafa Elhemali | Chris Nauroth |
| [HADOOP-10728](https://issues.apache.org/jira/browse/HADOOP-10728) | Metrics system for Windows Azure Storage Filesystem |  Major | tools | Mike Liddell | Mike Liddell |
| [YARN-2837](https://issues.apache.org/jira/browse/YARN-2837) | Timeline server needs to recover the timeline DT when restarting |  Blocker | timelineserver | Zhijie Shen | Zhijie Shen |
| [YARN-2574](https://issues.apache.org/jira/browse/YARN-2574) | Add support for FairScheduler to the ReservationSystem |  Major | fairscheduler | Subru Krishnan | Anubhav Dhoot |
| [YARN-2427](https://issues.apache.org/jira/browse/YARN-2427) | Add support for moving apps between queues in RM web services |  Major | resourcemanager | Varun Vasudev | Varun Vasudev |
| [HDFS-3107](https://issues.apache.org/jira/browse/HDFS-3107) | HDFS truncate |  Major | datanode, namenode | Lei Chang | Plamen Jeliazkov |
| [HADOOP-11490](https://issues.apache.org/jira/browse/HADOOP-11490) | Expose truncate API via FileSystem and shell command |  Major | fs | Konstantin Shvachko | Milan Desai |
| [HDFS-3689](https://issues.apache.org/jira/browse/HDFS-3689) | Add support for variable length block |  Major | datanode, hdfs-client, namenode | Suresh Srinivas | Jing Zhao |
| [MAPREDUCE-6227](https://issues.apache.org/jira/browse/MAPREDUCE-6227) | DFSIO for truncate |  Major | benchmarks, test | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-11510](https://issues.apache.org/jira/browse/HADOOP-11510) | Expose truncate API via FileContext |  Major | fs | Yi Liu | Yi Liu |
| [HDFS-7584](https://issues.apache.org/jira/browse/HDFS-7584) | Enable Quota Support for Storage Types |  Major | datanode, namenode | Xiaoyu Yao | Xiaoyu Yao |
| [MAPREDUCE-6228](https://issues.apache.org/jira/browse/MAPREDUCE-6228) | Add truncate operation to SLive |  Major | benchmarks, test | Konstantin Shvachko | Plamen Jeliazkov |
| [YARN-2190](https://issues.apache.org/jira/browse/YARN-2190) | Add CPU and memory limit options to the default container executor for Windows containers |  Major | nodemanager | Chuan Liu | Chuan Liu |
| [HDFS-6488](https://issues.apache.org/jira/browse/HDFS-6488) | Support HDFS superuser in NFSv3 gateway |  Major | nfs | Stephen Chu | Brandon Li |
| [HDFS-6826](https://issues.apache.org/jira/browse/HDFS-6826) | Plugin interface to enable delegation of HDFS authorization assertions |  Major | security | Alejandro Abdelnur | Arun Suresh |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-8059](https://issues.apache.org/jira/browse/HADOOP-8059) | Add javadoc to InterfaceAudience and InterfaceStability |  Major | documentation | Suresh Srinivas | Brandon Li |
| [HADOOP-10563](https://issues.apache.org/jira/browse/HADOOP-10563) | Remove the dependency of jsp in trunk |  Major | . | Haohui Mai | Haohui Mai |
| [YARN-2254](https://issues.apache.org/jira/browse/YARN-2254) | TestRMWebServicesAppsModification should run against both CS and FS |  Minor | . | zhihai xu | zhihai xu |
| [HDFS-7186](https://issues.apache.org/jira/browse/HDFS-7186) | Document the "hadoop trace" command. |  Minor | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-11172](https://issues.apache.org/jira/browse/HADOOP-11172) | Improve error message in Shell#runCommand on OutOfMemoryError |  Major | . | Yongjun Zhang | Yongjun Zhang |
| [HDFS-7026](https://issues.apache.org/jira/browse/HDFS-7026) | Introduce a string constant for "Failed to obtain user group info..." |  Trivial | . | Yongjun Zhang | Yongjun Zhang |
| [YARN-2641](https://issues.apache.org/jira/browse/YARN-2641) | Decommission nodes on -refreshNodes instead of next NM-RM heartbeat |  Major | resourcemanager | zhihai xu | zhihai xu |
| [HDFS-7190](https://issues.apache.org/jira/browse/HDFS-7190) | Bad use of Preconditions in startFileInternal() |  Major | namenode | Konstantin Shvachko | Dawson Choong |
| [HDFS-7242](https://issues.apache.org/jira/browse/HDFS-7242) | Code improvement for FSN#checkUnreadableBySuperuser |  Minor | namenode | Yi Liu | Yi Liu |
| [HDFS-7252](https://issues.apache.org/jira/browse/HDFS-7252) | small refinement to the use of isInAnEZ in FSNamesystem |  Trivial | . | Yi Liu | Yi Liu |
| [HDFS-7266](https://issues.apache.org/jira/browse/HDFS-7266) | HDFS Peercache enabled check should not lock on object |  Minor | hdfs-client | Gopal V | Andrew Wang |
| [HDFS-7257](https://issues.apache.org/jira/browse/HDFS-7257) | Add the time of last HA state transition to NN's /jmx page |  Minor | namenode | Charles Lamb | Charles Lamb |
| [HDFS-7223](https://issues.apache.org/jira/browse/HDFS-7223) | Tracing span description of IPC client is too long |  Minor | . | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-7283](https://issues.apache.org/jira/browse/HDFS-7283) | Bump DataNode OOM log from WARN to ERROR |  Trivial | datanode | Stephen Chu | Stephen Chu |
| [HADOOP-11231](https://issues.apache.org/jira/browse/HADOOP-11231) | Remove dead code in ServletUtil |  Minor | . | Haohui Mai | Li Lu |
| [HDFS-7278](https://issues.apache.org/jira/browse/HDFS-7278) | Add a command that allows sysadmins to manually trigger full block reports from a DN |  Major | datanode | Colin P. McCabe | Colin P. McCabe |
| [HDFS-6741](https://issues.apache.org/jira/browse/HDFS-6741) | Improve permission denied message when FSPermissionChecker#checkOwner fails |  Trivial | . | Stephen Chu | Harsh J |
| [HDFS-7280](https://issues.apache.org/jira/browse/HDFS-7280) | Use netty 4 in WebImageViewer |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-7165](https://issues.apache.org/jira/browse/HDFS-7165) | Separate block metrics for files with replication count 1 |  Major | namenode | Andrew Wang | Zhe Zhang |
| [HDFS-3342](https://issues.apache.org/jira/browse/HDFS-3342) | SocketTimeoutException in BlockSender.sendChunks could have a better error message |  Minor | datanode | Todd Lipcon | Yongjun Zhang |
| [HADOOP-10987](https://issues.apache.org/jira/browse/HADOOP-10987) | Provide an iterator-based listing API for FileSystem |  Major | . | Kihwal Lee | Kihwal Lee |
| [HADOOP-10847](https://issues.apache.org/jira/browse/HADOOP-10847) | Remove the usage of sun.security.x509.\* in testing code |  Minor | security | Kai Zheng | pascal oliva |
| [HDFS-7356](https://issues.apache.org/jira/browse/HDFS-7356) | Use DirectoryListing.hasMore() directly in nfs |  Minor | nfs | Haohui Mai | Li Lu |
| [HDFS-7357](https://issues.apache.org/jira/browse/HDFS-7357) | FSNamesystem.checkFileProgress should log file path |  Minor | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-7335](https://issues.apache.org/jira/browse/HDFS-7335) | Redundant checkOperation() in FSN.analyzeFileState() |  Major | namenode | Konstantin Shvachko | Milan Desai |
| [HDFS-7333](https://issues.apache.org/jira/browse/HDFS-7333) | Improve log message in Storage.tryLock() |  Major | datanode, namenode | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-7329](https://issues.apache.org/jira/browse/HDFS-7329) | MiniDFSCluster should log the exception when createNameNodesAndSetConf() fails. |  Major | test | Konstantin Shvachko | Byron Wong |
| [HDFS-7336](https://issues.apache.org/jira/browse/HDFS-7336) | Unused member DFSInputStream.buffersize |  Major | hdfs-client | Konstantin Shvachko | Milan Desai |
| [HDFS-7365](https://issues.apache.org/jira/browse/HDFS-7365) | Remove hdfs.server.blockmanagement.MutableBlockCollection |  Minor | . | Li Lu | Li Lu |
| [HDFS-7381](https://issues.apache.org/jira/browse/HDFS-7381) | Decouple the management of block id and gen stamps from FSNamesystem |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-7375](https://issues.apache.org/jira/browse/HDFS-7375) | Move FSClusterStats to o.a.h.h.hdfs.server.blockmanagement |  Major | . | Haohui Mai | Haohui Mai |
| [YARN-2780](https://issues.apache.org/jira/browse/YARN-2780) | Log aggregated resource allocation in rm-appsummary.log |  Minor | resourcemanager | Koji Noguchi | Eric Payne |
| [HADOOP-11291](https://issues.apache.org/jira/browse/HADOOP-11291) | Log the cause of SASL connection failures |  Minor | security | Stephen Chu | Stephen Chu |
| [HDFS-7386](https://issues.apache.org/jira/browse/HDFS-7386) | Replace check "port number \< 1024" with shared isPrivilegedPort method |  Trivial | datanode, security | Yongjun Zhang | Yongjun Zhang |
| [HDFS-7279](https://issues.apache.org/jira/browse/HDFS-7279) | Use netty to implement DatanodeWebHdfsMethods |  Major | datanode, webhdfs | Haohui Mai | Haohui Mai |
| [HDFS-7404](https://issues.apache.org/jira/browse/HDFS-7404) | Remove o.a.h.hdfs.server.datanode.web.resources |  Major | . | Haohui Mai | Li Lu |
| [YARN-2157](https://issues.apache.org/jira/browse/YARN-2157) | Document YARN metrics |  Major | documentation | Akira Ajisaka | Akira Ajisaka |
| [HDFS-7398](https://issues.apache.org/jira/browse/HDFS-7398) | Reset cached thread-local FSEditLogOp's on every FSEditLog#logEdit |  Major | namenode | Gera Shegalov | Gera Shegalov |
| [YARN-2878](https://issues.apache.org/jira/browse/YARN-2878) | Fix DockerContainerExecutor.apt.vm formatting |  Major | documentation | Abin Shahab | Abin Shahab |
| [HDFS-7409](https://issues.apache.org/jira/browse/HDFS-7409) | Allow dead nodes to finish decommissioning if all files are fully replicated |  Minor | . | Andrew Wang | Andrew Wang |
| [YARN-2802](https://issues.apache.org/jira/browse/YARN-2802) | ClusterMetrics to include AM launch and register delays |  Major | resourcemanager | zhihai xu | zhihai xu |
| [HADOOP-11323](https://issues.apache.org/jira/browse/HADOOP-11323) | WritableComparator#compare keeps reference to byte array |  Major | performance | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [MAPREDUCE-6169](https://issues.apache.org/jira/browse/MAPREDUCE-6169) | MergeQueue should release reference to the current item from key and value at the end of the iteration to save memory. |  Major | mrv2 | zhihai xu | zhihai xu |
| [YARN-2669](https://issues.apache.org/jira/browse/YARN-2669) | FairScheduler: queue names shouldn't allow periods |  Major | . | Wei Yan | Wei Yan |
| [HDFS-7331](https://issues.apache.org/jira/browse/HDFS-7331) | Add Datanode network counts to datanode jmx page |  Minor | datanode | Charles Lamb | Charles Lamb |
| [HDFS-7419](https://issues.apache.org/jira/browse/HDFS-7419) | Improve error messages for DataNode hot swap drive feature |  Major | datanode | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HADOOP-11173](https://issues.apache.org/jira/browse/HADOOP-11173) | Improve error messages for some KeyShell commands |  Minor | . | Andrew Wang | Andrew Wang |
| [HADOOP-11301](https://issues.apache.org/jira/browse/HADOOP-11301) | [optionally] update jmx cache to drop old metrics |  Major | . | Maysam Yabandeh | Maysam Yabandeh |
| [HDFS-6735](https://issues.apache.org/jira/browse/HDFS-6735) | A minor optimization to avoid pread() be blocked by read() inside the same DFSInputStream |  Major | hdfs-client | Liang Xie | Lars Hofhansl |
| [YARN-1156](https://issues.apache.org/jira/browse/YARN-1156) | Enhance NodeManager AllocatedGB and AvailableGB metrics for aggregation of decimal values |  Major | . | Akira Ajisaka | Tsuyoshi Ozawa |
| [MAPREDUCE-5932](https://issues.apache.org/jira/browse/MAPREDUCE-5932) | Provide an option to use a dedicated reduce-side shuffle log |  Major | mrv2 | Gera Shegalov | Gera Shegalov |
| [HDFS-7458](https://issues.apache.org/jira/browse/HDFS-7458) | Add description to the nfs ports in core-site.xml used by nfs test to avoid confusion |  Minor | nfs, test | Yongjun Zhang | Yongjun Zhang |
| [YARN-2891](https://issues.apache.org/jira/browse/YARN-2891) | Failed Container Executor does not provide a clear error message |  Minor | nodemanager | Dustin Cote | Dustin Cote |
| [YARN-2301](https://issues.apache.org/jira/browse/YARN-2301) | Improve yarn container command |  Major | . | Jian He | Naganarasimha G R |
| [HDFS-7454](https://issues.apache.org/jira/browse/HDFS-7454) | Reduce memory footprint for AclEntries in NameNode |  Major | namenode | Vinayakumar B | Vinayakumar B |
| [HDFS-7478](https://issues.apache.org/jira/browse/HDFS-7478) | Move org.apache.hadoop.hdfs.server.namenode.NNConf to FSNamesystem |  Major | . | Li Lu | Li Lu |
| [HADOOP-11313](https://issues.apache.org/jira/browse/HADOOP-11313) | Adding a document about NativeLibraryChecker |  Major | documentation | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [HDFS-7384](https://issues.apache.org/jira/browse/HDFS-7384) | 'getfacl' command and 'getAclStatus' output should be in sync |  Major | namenode | Vinayakumar B | Vinayakumar B |
| [YARN-2604](https://issues.apache.org/jira/browse/YARN-2604) | Scheduler should consider max-allocation-\* in conjunction with the largest node |  Major | scheduler | Karthik Kambatla | Robert Kanter |
| [HDFS-7463](https://issues.apache.org/jira/browse/HDFS-7463) | Simplify FSNamesystem#getBlockLocationsUpdateTimes |  Major | . | Haohui Mai | Haohui Mai |
| [MAPREDUCE-6046](https://issues.apache.org/jira/browse/MAPREDUCE-6046) | Change the class name for logs in RMCommunicator.java |  Minor | mr-am | Devaraj K | Sahil Takiar |
| [HDFS-7426](https://issues.apache.org/jira/browse/HDFS-7426) | Change nntop JMX format to be a JSON blob |  Major | namenode | Andrew Wang | Andrew Wang |
| [YARN-2950](https://issues.apache.org/jira/browse/YARN-2950) | Change message to mandate, not suggest JS requirement on UI |  Minor | webapp | Harsh J | Dustin Cote |
| [HADOOP-11396](https://issues.apache.org/jira/browse/HADOOP-11396) | Provide navigation in the site documentation linking to the Hadoop Compatible File Systems. |  Major | documentation | Chris Nauroth | Chris Nauroth |
| [MAPREDUCE-6194](https://issues.apache.org/jira/browse/MAPREDUCE-6194) | Bubble up final exception in failures during creation of output collectors |  Minor | task | Harsh J | Varun Saxena |
| [HDFS-7513](https://issues.apache.org/jira/browse/HDFS-7513) | HDFS inotify: add defaultBlockSize to CreateEvent |  Major | namenode | Colin P. McCabe | Colin P. McCabe |
| [HADOOP-11410](https://issues.apache.org/jira/browse/HADOOP-11410) | make the rpath of libhadoop.so configurable |  Major | . | Colin P. McCabe | Colin P. McCabe |
| [HADOOP-11416](https://issues.apache.org/jira/browse/HADOOP-11416) | Move ChunkedArrayList into hadoop-common |  Major | . | Colin P. McCabe | Colin P. McCabe |
| [HADOOP-11421](https://issues.apache.org/jira/browse/HADOOP-11421) | Add IOUtils#listDirectory |  Major | . | Colin P. McCabe | Colin P. McCabe |
| [HADOOP-10809](https://issues.apache.org/jira/browse/HADOOP-10809) | hadoop-azure: page blob support |  Major | tools | Mike Liddell | Eric Hanson |
| [HADOOP-11188](https://issues.apache.org/jira/browse/HADOOP-11188) | hadoop-azure: automatically expand page blobs when they become full |  Major | fs | Eric Hanson | Eric Hanson |
| [HDFS-7531](https://issues.apache.org/jira/browse/HDFS-7531) | Improve the concurrent access on FsVolumeList |  Major | datanode | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HADOOP-11427](https://issues.apache.org/jira/browse/HADOOP-11427) | ChunkedArrayList: fix removal via iterator and implement get |  Major | . | Colin P. McCabe | Colin P. McCabe |
| [HADOOP-11430](https://issues.apache.org/jira/browse/HADOOP-11430) | Add GenericTestUtils#disableLog, GenericTestUtils#setLogLevel |  Major | . | Colin P. McCabe | Colin P. McCabe |
| [HADOOP-11422](https://issues.apache.org/jira/browse/HADOOP-11422) | Check CryptoCodec is AES-CTR for Crypto input/output stream |  Minor | . | Yi Liu | Yi Liu |
| [HADOOP-11395](https://issues.apache.org/jira/browse/HADOOP-11395) | Add site documentation for Azure Storage FileSystem integration. |  Major | documentation | Chris Nauroth | Chris Nauroth |
| [HDFS-7555](https://issues.apache.org/jira/browse/HDFS-7555) | Remove the support of unmanaged connectors in HttpServer2 |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-7557](https://issues.apache.org/jira/browse/HDFS-7557) | Fix spacing for a few keys in DFSConfigKeys.java |  Minor | . | Colin P. McCabe | Colin P. McCabe |
| [YARN-2939](https://issues.apache.org/jira/browse/YARN-2939) | Fix new findbugs warnings in hadoop-yarn-common |  Major | . | Varun Saxena | Li Lu |
| [HDFS-7484](https://issues.apache.org/jira/browse/HDFS-7484) | Make FSDirectory#addINode take existing INodes as its parameter |  Major | . | Haohui Mai | Jing Zhao |
| [HADOOP-11399](https://issues.apache.org/jira/browse/HADOOP-11399) | Java Configuration file and .xml files should be automatically cross-compared |  Minor | . | Ray Chiang | Ray Chiang |
| [YARN-2940](https://issues.apache.org/jira/browse/YARN-2940) | Fix new findbugs warnings in rest of the hadoop-yarn components |  Major | . | Varun Saxena | Li Lu |
| [YARN-2937](https://issues.apache.org/jira/browse/YARN-2937) | Fix new findbugs warnings in hadoop-yarn-nodemanager |  Major | . | Varun Saxena | Varun Saxena |
| [HADOOP-11448](https://issues.apache.org/jira/browse/HADOOP-11448) | Fix findbugs warnings in FileBasedIPList |  Minor | . | Akira Ajisaka | Tsuyoshi Ozawa |
| [YARN-2938](https://issues.apache.org/jira/browse/YARN-2938) | Fix new findbugs warnings in hadoop-yarn-resourcemanager and hadoop-yarn-applicationhistoryservice |  Major | . | Varun Saxena | Varun Saxena |
| [MAPREDUCE-6149](https://issues.apache.org/jira/browse/MAPREDUCE-6149) | Document override log4j.properties in MR job |  Major | documentation | Junping Du | Junping Du |
| [HADOOP-11455](https://issues.apache.org/jira/browse/HADOOP-11455) | KMS and Credential CLI should request confirmation for deletion by default |  Minor | security | Charles Lamb | Charles Lamb |
| [HADOOP-11390](https://issues.apache.org/jira/browse/HADOOP-11390) | Metrics 2 ganglia provider to include hostname in unresolved address problems |  Minor | metrics | Steve Loughran | Varun Saxena |
| [HDFS-7564](https://issues.apache.org/jira/browse/HDFS-7564) | NFS gateway dynamically reload UID/GID mapping file /etc/nfs.map |  Minor | nfs | Hari Sekhon | Yongjun Zhang |
| [HADOOP-11032](https://issues.apache.org/jira/browse/HADOOP-11032) | Replace use of Guava's Stopwatch with Hadoop's StopWatch |  Major | . | Gary Steelman | Tsuyoshi Ozawa |
| [YARN-2996](https://issues.apache.org/jira/browse/YARN-2996) | Refine fs operations in FileSystemRMStateStore and few fixes |  Major | resourcemanager | Yi Liu | Yi Liu |
| [HDFS-7579](https://issues.apache.org/jira/browse/HDFS-7579) | Improve log reporting during block report rpc failure |  Minor | datanode | Charles Lamb | Charles Lamb |
| [HDFS-7446](https://issues.apache.org/jira/browse/HDFS-7446) | HDFS inotify should have the ability to determine what txid it has read up to |  Major | hdfs-client | Colin P. McCabe | Colin P. McCabe |
| [HADOOP-11464](https://issues.apache.org/jira/browse/HADOOP-11464) | Reinstate support for launching Hadoop processes on Windows using Cygwin. |  Major | bin | Chris Nauroth | Chris Nauroth |
| [HADOOP-9992](https://issues.apache.org/jira/browse/HADOOP-9992) | Modify the NN loadGenerator to optionally run as a MapReduce job |  Major | test | Akshay Radia | Akshay Radia |
| [HDFS-7182](https://issues.apache.org/jira/browse/HDFS-7182) | JMX metrics aren't accessible when NN is busy |  Major | . | Ming Ma | Ming Ma |
| [HDFS-7323](https://issues.apache.org/jira/browse/HDFS-7323) | Move the get/setStoragePolicy commands out from dfsadmin |  Major | hdfs-client | Tsz Wo Nicholas Sze | Jing Zhao |
| [YARN-2957](https://issues.apache.org/jira/browse/YARN-2957) | Create unit test to automatically compare YarnConfiguration and yarn-default.xml |  Minor | . | Ray Chiang | Ray Chiang |
| [HDFS-7326](https://issues.apache.org/jira/browse/HDFS-7326) | Add documentation for hdfs debug commands |  Minor | documentation | Colin P. McCabe | Vijay Bhat |
| [HDFS-7598](https://issues.apache.org/jira/browse/HDFS-7598) | Remove dependency on old version of Guava in TestDFSClientCache#testEviction |  Minor | test | Sangjin Lee | Sangjin Lee |
| [HDFS-7600](https://issues.apache.org/jira/browse/HDFS-7600) | Refine hdfs admin classes to reuse common code |  Major | tools | Yi Liu | Jing Zhao |
| [YARN-2643](https://issues.apache.org/jira/browse/YARN-2643) | Don't create a new DominantResourceCalculator on every FairScheduler.allocate call |  Trivial | . | Sandy Ryza | Karthik Kambatla |
| [YARN-2679](https://issues.apache.org/jira/browse/YARN-2679) | Add metric for container launch duration |  Major | nodemanager | zhihai xu | zhihai xu |
| [MAPREDUCE-6173](https://issues.apache.org/jira/browse/MAPREDUCE-6173) | Document the configuration of deploying MR over distributed cache with enabling wired encryption at the same time |  Major | distributed-cache, documentation | Junping Du | Junping Du |
| [HDFS-2219](https://issues.apache.org/jira/browse/HDFS-2219) | Fsck should work with fully qualified file paths. |  Minor | tools | Jitendra Nath Pandey | Tsz Wo Nicholas Sze |
| [HADOOP-11481](https://issues.apache.org/jira/browse/HADOOP-11481) | ClassCastException while using a key created by keytool to create encryption zone. |  Minor | . | Yi Yao | Charles Lamb |
| [HADOOP-11483](https://issues.apache.org/jira/browse/HADOOP-11483) | HardLink.java should use the jdk7 createLink method |  Major | . | Colin P. McCabe | Akira Ajisaka |
| [YARN-3005](https://issues.apache.org/jira/browse/YARN-3005) | [JDK7] Use switch statement for String instead of if-else statement in RegistrySecurity.java |  Trivial | . | Akira Ajisaka | Kengo Seki |
| [HADOOP-8757](https://issues.apache.org/jira/browse/HADOOP-8757) | Metrics should disallow names with invalid characters |  Minor | metrics | Todd Lipcon | Ray Chiang |
| [HADOOP-11261](https://issues.apache.org/jira/browse/HADOOP-11261) | Set custom endpoint for S3A |  Major | fs/s3 | Thomas Demoor | Thomas Demoor |
| [HDFS-7591](https://issues.apache.org/jira/browse/HDFS-7591) | hdfs classpath command should support same options as hadoop classpath. |  Minor | scripts | Chris Nauroth | Varun Saxena |
| [HADOOP-11171](https://issues.apache.org/jira/browse/HADOOP-11171) | Enable using a proxy server to connect to S3a. |  Major | fs/s3 | Thomas Demoor | Thomas Demoor |
| [HDFS-7640](https://issues.apache.org/jira/browse/HDFS-7640) | print NFS Client in the NFS log |  Trivial | nfs | Brandon Li | Brandon Li |
| [HDFS-7430](https://issues.apache.org/jira/browse/HDFS-7430) | Rewrite the BlockScanner to use O(1) memory and use multiple threads |  Major | . | Colin P. McCabe | Colin P. McCabe |
| [HADOOP-11419](https://issues.apache.org/jira/browse/HADOOP-11419) | improve hadoop-maven-plugins |  Minor | build | Hervé Boutemy | Hervé Boutemy |
| [MAPREDUCE-6141](https://issues.apache.org/jira/browse/MAPREDUCE-6141) | History server leveldb recovery store |  Major | jobhistoryserver | Jason Lowe | Jason Lowe |
| [YARN-3086](https://issues.apache.org/jira/browse/YARN-3086) | Make NodeManager memory configurable in MiniYARNCluster |  Minor | test | Robert Metzger | Robert Metzger |
| [HADOOP-4297](https://issues.apache.org/jira/browse/HADOOP-4297) | Enable Java assertions when running tests |  Major | build | Yoram Kulbak | Tsz Wo Nicholas Sze |
| [HDFS-7683](https://issues.apache.org/jira/browse/HDFS-7683) | Combine usages and percent stats in NameNode UI |  Minor | namenode | Vinayakumar B | Vinayakumar B |
| [HADOOP-10626](https://issues.apache.org/jira/browse/HADOOP-10626) | Limit Returning Attributes for LDAP search |  Major | security | Jason Hubbard | Jason Hubbard |
| [HDFS-7675](https://issues.apache.org/jira/browse/HDFS-7675) | Remove unused member DFSClient#spanReceiverHost |  Trivial | hdfs-client | Konstantin Shvachko | Colin P. McCabe |
| [HADOOP-10525](https://issues.apache.org/jira/browse/HADOOP-10525) | Remove DRFA.MaxBackupIndex config from log4j.properties |  Minor | . | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-11441](https://issues.apache.org/jira/browse/HADOOP-11441) | Hadoop-azure: Change few methods scope to public |  Minor | tools | shashank | shashank |
| [MAPREDUCE-6150](https://issues.apache.org/jira/browse/MAPREDUCE-6150) | Update document of Rumen |  Minor | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-3108](https://issues.apache.org/jira/browse/YARN-3108) | ApplicationHistoryServer doesn't process -D arguments |  Major | . | Chang Li | Chang Li |
| [MAPREDUCE-6151](https://issues.apache.org/jira/browse/MAPREDUCE-6151) | Update document of GridMix |  Minor | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-7706](https://issues.apache.org/jira/browse/HDFS-7706) | Switch BlockManager logging to use slf4j |  Minor | namenode | Andrew Wang | Andrew Wang |
| [YARN-3077](https://issues.apache.org/jira/browse/YARN-3077) | RM should create yarn.resourcemanager.zk-state-store.parent-path recursively |  Major | resourcemanager | Chun Chen | Chun Chen |
| [HADOOP-11442](https://issues.apache.org/jira/browse/HADOOP-11442) | hadoop-azure: Create test jar |  Major | tools | shashank | shashank |
| [MAPREDUCE-6143](https://issues.apache.org/jira/browse/MAPREDUCE-6143) | add configuration for  mapreduce speculative execution in MR2 |  Major | mrv2 | zhihai xu | zhihai xu |
| [YARN-3085](https://issues.apache.org/jira/browse/YARN-3085) | Application summary should include the application type |  Major | resourcemanager | Jason Lowe | Rohith Sharma K S |
| [HADOOP-11045](https://issues.apache.org/jira/browse/HADOOP-11045) | Introducing a tool to detect flaky tests of hadoop jenkins test job |  Major | build, tools | Yongjun Zhang | Yongjun Zhang |
| [YARN-3022](https://issues.apache.org/jira/browse/YARN-3022) | Expose Container resource information from NodeManager for monitoring |  Major | . | Anubhav Dhoot | Anubhav Dhoot |
| [YARN-1393](https://issues.apache.org/jira/browse/YARN-1393) | SLS: Add how-to-use instructions |  Major | . | Wei Yan | Wei Yan |
| [MAPREDUCE-5800](https://issues.apache.org/jira/browse/MAPREDUCE-5800) | Use Job#getInstance instead of deprecated constructors |  Minor | . | Akira Ajisaka | Akira Ajisaka |
| [YARN-3056](https://issues.apache.org/jira/browse/YARN-3056) | add verification for containerLaunchDuration in TestNodeManagerMetrics. |  Trivial | test | zhihai xu | zhihai xu |
| [HADOOP-11544](https://issues.apache.org/jira/browse/HADOOP-11544) | Remove unused configuration keys for tracing |  Trivial | . | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-3123](https://issues.apache.org/jira/browse/YARN-3123) | Make YARN CLI show a single completed container even if the app is running |  Major | client | Zhijie Shen | Naganarasimha G R |
| [MAPREDUCE-6059](https://issues.apache.org/jira/browse/MAPREDUCE-6059) | Speed up history server startup time |  Major | . | Siqi Li | Siqi Li |
| [HADOOP-10976](https://issues.apache.org/jira/browse/HADOOP-10976) | moving the source code of hadoop-tools docs to the directory under hadoop-tools |  Minor | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-7270](https://issues.apache.org/jira/browse/HDFS-7270) | Add congestion signaling capability to DataNode write protocol |  Major | datanode | Haohui Mai | Haohui Mai |
| [YARN-1582](https://issues.apache.org/jira/browse/YARN-1582) | Capacity Scheduler: add a maximum-allocation-mb setting per queue |  Major | capacityscheduler | Thomas Graves | Thomas Graves |
| [HADOOP-11506](https://issues.apache.org/jira/browse/HADOOP-11506) | Configuration variable expansion regex expensive for long values |  Major | conf | Dmitriy V. Ryaboy | Gera Shegalov |
| [HDFS-7732](https://issues.apache.org/jira/browse/HDFS-7732) | Fix the order of the parameters in DFSConfigKeys |  Trivial | . | Akira Ajisaka | Brahma Reddy Battula |
| [YARN-3144](https://issues.apache.org/jira/browse/YARN-3144) | Configuration for making delegation token failures to timeline server not-fatal |  Major | . | Jonathan Eagles | Jonathan Eagles |
| [HADOOP-11520](https://issues.apache.org/jira/browse/HADOOP-11520) | Clean incomplete multi-part uploads in S3A tests |  Minor | fs/s3 | Thomas Demoor | Thomas Demoor |
| [HDFS-7710](https://issues.apache.org/jira/browse/HDFS-7710) | Remove dead code in BackupImage.java |  Minor | . | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-7743](https://issues.apache.org/jira/browse/HDFS-7743) | Code cleanup of BlockInfo and rename BlockInfo to BlockInfoContiguous |  Minor | namenode | Jing Zhao | Jing Zhao |
| [YARN-3100](https://issues.apache.org/jira/browse/YARN-3100) | Make YARN authorization pluggable |  Major | . | Jian He | Jian He |
| [HADOOP-11495](https://issues.apache.org/jira/browse/HADOOP-11495) | Convert site documentation from apt to markdown |  Major | documentation | Allen Wittenauer | Masatake Iwasaki |
| [HDFS-316](https://issues.apache.org/jira/browse/HDFS-316) | Balancer should run for a configurable # of iterations |  Minor | balancer & mover | Brian Bockelman | Xiaoyu Yao |
| [HADOOP-11579](https://issues.apache.org/jira/browse/HADOOP-11579) | Documentation for truncate |  Major | documentation | Steve Loughran | Konstantin Shvachko |
| [HDFS-7771](https://issues.apache.org/jira/browse/HDFS-7771) | fuse\_dfs should permit FILE: on the front of KRB5CCNAME |  Major | . | Colin P. McCabe | Colin P. McCabe |
| [HDFS-6133](https://issues.apache.org/jira/browse/HDFS-6133) | Add a feature for replica pinning so that a pinned replica will not be moved by Balancer/Mover. |  Major | balancer & mover, datanode | yunjiong zhao | yunjiong zhao |
| [HADOOP-10140](https://issues.apache.org/jira/browse/HADOOP-10140) | Specification of HADOOP\_CONF\_DIR via the environment in hadoop\_config.cmd |  Minor | scripts | Ian Jackson | Kiran Kumar M R |
| [HDFS-7761](https://issues.apache.org/jira/browse/HDFS-7761) | cleanup unnecssary code logic in LocatedBlock |  Minor | . | Yi Liu | Yi Liu |
| [HDFS-7703](https://issues.apache.org/jira/browse/HDFS-7703) | Support favouredNodes for the append for new blocks |  Major | . | Vinayakumar B | Vinayakumar B |
| [MAPREDUCE-6253](https://issues.apache.org/jira/browse/MAPREDUCE-6253) | Update use of Iterator to Iterable |  Minor | . | Ray Chiang | Ray Chiang |
| [HADOOP-11543](https://issues.apache.org/jira/browse/HADOOP-11543) | Improve help message for hadoop/yarn command |  Trivial | scripts | Jagadesh Kiran N | Brahma Reddy Battula |
| [MAPREDUCE-5335](https://issues.apache.org/jira/browse/MAPREDUCE-5335) | Rename Job Tracker terminology in ShuffleSchedulerImpl |  Major | applicationmaster | Devaraj K | Devaraj K |
| [MAPREDUCE-4431](https://issues.apache.org/jira/browse/MAPREDUCE-4431) | mapred command should print the reason on killing already completed jobs |  Minor | mrv2 | Nishan Shetty | Devaraj K |
| [YARN-3157](https://issues.apache.org/jira/browse/YARN-3157) | Refactor the exception handling in ConverterUtils#to\*Id |  Minor | resourcemanager | Bibin A Chundatt | Bibin A Chundatt |
| [HDFS-7694](https://issues.apache.org/jira/browse/HDFS-7694) | FSDataInputStream should support "unbuffer" |  Major | . | Colin P. McCabe | Colin P. McCabe |
| [YARN-3147](https://issues.apache.org/jira/browse/YARN-3147) | Clean up RM web proxy code |  Major | webapp | Steve Loughran | Steve Loughran |
| [HADOOP-11586](https://issues.apache.org/jira/browse/HADOOP-11586) | Update use of Iterator to Iterable in AbstractMetricsContext.java |  Minor | metrics | Ray Chiang | Ray Chiang |
| [HADOOP-9869](https://issues.apache.org/jira/browse/HADOOP-9869) |  Configuration.getSocketAddr()/getEnum() should use getTrimmed() |  Minor | conf | Steve Loughran | Tsuyoshi Ozawa |
| [YARN-3158](https://issues.apache.org/jira/browse/YARN-3158) | Correct log messages in ResourceTrackerService |  Major | . | Devaraj K | Varun Saxena |
| [YARN-3179](https://issues.apache.org/jira/browse/YARN-3179) | Update use of Iterator to Iterable |  Minor | . | Ray Chiang | Ray Chiang |
| [HDFS-7684](https://issues.apache.org/jira/browse/HDFS-7684) | The host:port settings of the daemons should be trimmed before use |  Major | . | Tianyin Xu | Anu Engineer |
| [HDFS-7790](https://issues.apache.org/jira/browse/HDFS-7790) | Do not create optional fields in DFSInputStream unless they are needed |  Minor | hdfs-client | Colin P. McCabe | Colin P. McCabe |
| [HDFS-7668](https://issues.apache.org/jira/browse/HDFS-7668) | Convert site documentation from apt to markdown |  Major | documentation | Allen Wittenauer | Masatake Iwasaki |
| [YARN-3182](https://issues.apache.org/jira/browse/YARN-3182) | Cleanup switch statement in ApplicationMasterLauncher#handle() |  Minor | . | Ray Chiang | Ray Chiang |
| [MAPREDUCE-6255](https://issues.apache.org/jira/browse/MAPREDUCE-6255) | Fix JobCounter's format to use grouping separator |  Minor | client | Ryu Kobayashi | Ryu Kobayashi |
| [HADOOP-11589](https://issues.apache.org/jira/browse/HADOOP-11589) | NetUtils.createSocketAddr should trim the input URI |  Minor | net | Akira Ajisaka | Rakesh R |
| [MAPREDUCE-6256](https://issues.apache.org/jira/browse/MAPREDUCE-6256) | Removed unused private methods in o.a.h.mapreduce.Job.java |  Minor | . | Devaraj K | Naganarasimha G R |
| [YARN-3203](https://issues.apache.org/jira/browse/YARN-3203) | Correct a log message in AuxServices |  Minor | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-1299](https://issues.apache.org/jira/browse/YARN-1299) | Improve a log message in AppSchedulingInfo by adding application id |  Major | resourcemanager | Devaraj K |  |
| [HDFS-7604](https://issues.apache.org/jira/browse/HDFS-7604) | Track and display failed DataNode storage locations in NameNode. |  Major | datanode, namenode | Chris Nauroth | Chris Nauroth |
| [HDFS-7797](https://issues.apache.org/jira/browse/HDFS-7797) | Add audit log for setQuota operation |  Major | namenode | Rakesh R | Rakesh R |
| [HDFS-7795](https://issues.apache.org/jira/browse/HDFS-7795) | Show warning if not all favored nodes were chosen by namenode |  Minor | . | Kihwal Lee | Kihwal Lee |
| [HDFS-7780](https://issues.apache.org/jira/browse/HDFS-7780) | Update use of Iterator to Iterable in DataXceiverServer and SnapshotDiffInfo |  Minor | . | Ray Chiang | Ray Chiang |
| [HADOOP-11599](https://issues.apache.org/jira/browse/HADOOP-11599) | Client#getTimeout should use IPC\_CLIENT\_PING\_DEFAULT when IPC\_CLIENT\_PING\_KEY is not configured. |  Minor | ipc | zhihai xu | zhihai xu |
| [HDFS-7772](https://issues.apache.org/jira/browse/HDFS-7772) | Document hdfs balancer -exclude/-include option in HDFSCommands.html |  Trivial | documentation | Xiaoyu Yao | Xiaoyu Yao |
| [HADOOP-11440](https://issues.apache.org/jira/browse/HADOOP-11440) | Use "test.build.data" instead of "build.test.dir" for testing in ClientBaseWithFixes |  Minor | . | Akira Ajisaka | Kengo Seki |
| [HDFS-7752](https://issues.apache.org/jira/browse/HDFS-7752) | Improve description for "dfs.namenode.num.extra.edits.retained" and "dfs.namenode.num.checkpoints.retained" properties on hdfs-default.xml |  Minor | documentation | Wellington Chevreuil | Wellington Chevreuil |
| [YARN-2799](https://issues.apache.org/jira/browse/YARN-2799) | cleanup TestLogAggregationService based on the change in YARN-90 |  Minor | test | zhihai xu | zhihai xu |
| [YARN-3230](https://issues.apache.org/jira/browse/YARN-3230) | Clarify application states on the web UI |  Major | . | Jian He | Jian He |
| [HDFS-7773](https://issues.apache.org/jira/browse/HDFS-7773) | Additional metrics in HDFS to be accessed via jmx. |  Major | datanode, namenode | Anu Engineer | Anu Engineer |
| [HADOOP-11607](https://issues.apache.org/jira/browse/HADOOP-11607) | Reduce log spew in S3AFileSystem |  Trivial | fs/s3 | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [YARN-3236](https://issues.apache.org/jira/browse/YARN-3236) | cleanup RMAuthenticationFilter#AUTH\_HANDLER\_PROPERTY. |  Trivial | resourcemanager | zhihai xu | zhihai xu |
| [YARN-2797](https://issues.apache.org/jira/browse/YARN-2797) | TestWorkPreservingRMRestart should use ParametrizedSchedulerTestBase |  Minor | . | Karthik Kambatla | Karthik Kambatla |
| [YARN-3195](https://issues.apache.org/jira/browse/YARN-3195) | Add -help to yarn logs and nodes CLI command |  Minor | client | Jagadesh Kiran N | Jagadesh Kiran N |
| [HADOOP-11632](https://issues.apache.org/jira/browse/HADOOP-11632) | Cleanup Find.java to remove SupressWarnings annotations |  Minor | . | Akira Ajisaka | Akira Ajisaka |
| [HDFS-7537](https://issues.apache.org/jira/browse/HDFS-7537) | fsck is confusing when dfs.namenode.replication.min \> 1 && missing replicas && NN restart |  Major | namenode | Allen Wittenauer | Rui Gao |
| [HADOOP-11620](https://issues.apache.org/jira/browse/HADOOP-11620) | Add support for load balancing across a group of KMS for HA |  Major | kms | Arun Suresh | Arun Suresh |
| [HDFS-7832](https://issues.apache.org/jira/browse/HDFS-7832) | Show 'Last Modified' in Namenode's 'Browse Filesystem' |  Major | namenode | Vinayakumar B | Vinayakumar B |
| [MAPREDUCE-5612](https://issues.apache.org/jira/browse/MAPREDUCE-5612) | Add javadoc for TaskCompletionEvent.Status |  Minor | documentation | Sandy Ryza | Chris Palmer |
| [HADOOP-11569](https://issues.apache.org/jira/browse/HADOOP-11569) | Provide Merge API for MapFile to merge multiple similar MapFiles to one MapFile |  Major | . | Vinayakumar B | Vinayakumar B |
| [HDFS-7308](https://issues.apache.org/jira/browse/HDFS-7308) | DFSClient write packet size may \> 64kB |  Minor | hdfs-client | Tsz Wo Nicholas Sze | Takuya Fukudome |
| [YARN-2820](https://issues.apache.org/jira/browse/YARN-2820) |  Retry in FileSystemRMStateStore when FS's operations fail due to IOException. |  Major | resourcemanager | zhihai xu | zhihai xu |
| [HDFS-7685](https://issues.apache.org/jira/browse/HDFS-7685) | Document dfs.namenode.heartbeat.recheck-interval in hdfs-default.xml |  Minor | documentation | Frank Lanitz | Kai Sasaki |
| [YARN-3262](https://issues.apache.org/jira/browse/YARN-3262) |  Surface application outstanding resource requests table in RM web UI |  Major | yarn | Jian He | Jian He |
| [HDFS-5853](https://issues.apache.org/jira/browse/HDFS-5853) | Add "hadoop.user.group.metrics.percentiles.intervals" to hdfs-default.xml |  Minor | documentation, namenode | Akira Ajisaka | Akira Ajisaka |
| [HDFS-7439](https://issues.apache.org/jira/browse/HDFS-7439) | Add BlockOpResponseProto's message to DFSClient's exception message |  Minor | balancer & mover, datanode, hdfs-client | Ming Ma | Takanobu Asanuma |
| [HDFS-7789](https://issues.apache.org/jira/browse/HDFS-7789) | DFSck should resolve the path to support cross-FS symlinks |  Major | tools | Gera Shegalov | Gera Shegalov |
| [HADOOP-11658](https://issues.apache.org/jira/browse/HADOOP-11658) | Externalize io.compression.codecs property |  Minor | . | Kai Zheng | Kai Zheng |
| [MAPREDUCE-5583](https://issues.apache.org/jira/browse/MAPREDUCE-5583) | Ability to limit running map and reduce tasks |  Major | mr-am, mrv2 | Jason Lowe | Jason Lowe |
| [HDFS-7757](https://issues.apache.org/jira/browse/HDFS-7757) | Misleading error messages in FSImage.java |  Major | namenode | Arpit Agarwal | Brahma Reddy Battula |
| [YARN-3272](https://issues.apache.org/jira/browse/YARN-3272) | Surface container locality info in RM web UI |  Major | . | Jian He | Jian He |
| [MAPREDUCE-6248](https://issues.apache.org/jira/browse/MAPREDUCE-6248) | Allow users to get the MR job information for distcp |  Major | distcp | Jing Zhao | Jing Zhao |
| [YARN-3285](https://issues.apache.org/jira/browse/YARN-3285) | Convert branch-2 .apt.vm files of YARN to markdown |  Major | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-6565](https://issues.apache.org/jira/browse/HDFS-6565) | Use jackson instead jetty json in hdfs-client |  Major | . | Haohui Mai | Akira Ajisaka |
| [HDFS-7535](https://issues.apache.org/jira/browse/HDFS-7535) | Utilize Snapshot diff report for distcp |  Major | distcp, snapshots | Jing Zhao | Jing Zhao |
| [MAPREDUCE-6267](https://issues.apache.org/jira/browse/MAPREDUCE-6267) | Refactor JobSubmitter#copyAndConfigureFiles into it's own class |  Minor | . | Chris Trezzo | Chris Trezzo |
| [HDFS-7434](https://issues.apache.org/jira/browse/HDFS-7434) | DatanodeID hashCode should not be mutable |  Major | namenode | Daryn Sharp | Daryn Sharp |
| [HADOOP-11648](https://issues.apache.org/jira/browse/HADOOP-11648) | Set DomainSocketWatcher thread name explicitly |  Major | net | Liang Xie | Liang Xie |
| [YARN-3249](https://issues.apache.org/jira/browse/YARN-3249) | Add a "kill application" button to Resource Manager's Web UI |  Minor | resourcemanager | Ryu Kobayashi | Ryu Kobayashi |
| [HADOOP-11642](https://issues.apache.org/jira/browse/HADOOP-11642) | Upgrade azure sdk version from 0.6.0 to 2.0.0 |  Major | tools | shashank | shashank |
| [HDFS-7411](https://issues.apache.org/jira/browse/HDFS-7411) | Refactor and improve decommissioning logic into DecommissionManager |  Major | . | Andrew Wang | Andrew Wang |
| [HDFS-7898](https://issues.apache.org/jira/browse/HDFS-7898) | Change TestAppendSnapshotTruncate to fail-fast |  Minor | test | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-6806](https://issues.apache.org/jira/browse/HDFS-6806) | HDFS Rolling upgrade document should mention the versions available |  Minor | documentation | Akira Ajisaka | J.Andreina |
| [YARN-3187](https://issues.apache.org/jira/browse/YARN-3187) | Documentation of Capacity Scheduler Queue mapping based on user or group |  Major | capacityscheduler, documentation | Naganarasimha G R | Gururaj Shetty |
| [MAPREDUCE-4815](https://issues.apache.org/jira/browse/MAPREDUCE-4815) | Speed up FileOutputCommitter#commitJob for many output files |  Major | mrv2 | Jason Lowe | Siqi Li |
| [HDFS-7491](https://issues.apache.org/jira/browse/HDFS-7491) | Add incremental blockreport latency to DN metrics |  Minor | datanode | Ming Ma | Ming Ma |
| [HDFS-7435](https://issues.apache.org/jira/browse/HDFS-7435) | PB encoding of block reports is very inefficient |  Critical | datanode, namenode | Daryn Sharp | Daryn Sharp |
| [HADOOP-11633](https://issues.apache.org/jira/browse/HADOOP-11633) | Convert remaining branch-2 .apt.vm files to markdown |  Major | documentation | Colin P. McCabe | Masatake Iwasaki |
| [MAPREDUCE-6265](https://issues.apache.org/jira/browse/MAPREDUCE-6265) | Make ContainerLauncherImpl.INITIAL\_POOL\_SIZE configurable to better control to launch/kill containers |  Major | mrv2 | zhihai xu | zhihai xu |
| [YARN-2854](https://issues.apache.org/jira/browse/YARN-2854) | The document about timeline service and generic service needs to be updated |  Critical | timelineserver | Zhijie Shen | Naganarasimha G R |
| [HADOOP-11714](https://issues.apache.org/jira/browse/HADOOP-11714) | Add more trace log4j messages to SpanReceiverHost |  Minor | tracing | Colin P. McCabe | Colin P. McCabe |
| [YARN-3349](https://issues.apache.org/jira/browse/YARN-3349) | Treat all exceptions as failure in TestFSRMStateStore#testFSRMStateStoreClientRetry |  Minor | test | zhihai xu | zhihai xu |
| [YARN-3273](https://issues.apache.org/jira/browse/YARN-3273) | Improve web UI to facilitate scheduling analysis and debugging |  Major | . | Jian He | Rohith Sharma K S |
| [HDFS-7849](https://issues.apache.org/jira/browse/HDFS-7849) | Update documentation for enabling a new feature in rolling upgrade |  Minor | documentation | Tsz Wo Nicholas Sze | J.Andreina |
| [HDFS-7962](https://issues.apache.org/jira/browse/HDFS-7962) | Remove duplicated logs in BlockManager |  Minor | . | Yi Liu | Yi Liu |
| [YARN-2777](https://issues.apache.org/jira/browse/YARN-2777) | Mark the end of individual log in aggregated log |  Major | . | Ted Yu | Varun Saxena |
| [HDFS-7917](https://issues.apache.org/jira/browse/HDFS-7917) | Use file to replace data dirs in test to simulate a disk failure. |  Minor | test | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HDFS-7976](https://issues.apache.org/jira/browse/HDFS-7976) | Update NFS user guide for mount option "sync" to minimize or avoid reordered writes |  Major | documentation, nfs | Brandon Li | Brandon Li |
| [MAPREDUCE-6292](https://issues.apache.org/jira/browse/MAPREDUCE-6292) | Use org.junit package instead of junit.framework in TestCombineFileInputFormat |  Minor | test | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-10670](https://issues.apache.org/jira/browse/HADOOP-10670) | Allow AuthenticationFilters to load secret from signature secret files |  Minor | security | Kai Zheng | Kai Zheng |
| [HDFS-7410](https://issues.apache.org/jira/browse/HDFS-7410) | Support CreateFlags with append() to support hsync() for appending streams |  Major | hdfs-client | Vinayakumar B | Vinayakumar B |
| [HADOOP-11257](https://issues.apache.org/jira/browse/HADOOP-11257) | Update "hadoop jar" documentation to warn against using it for launching yarn jars |  Blocker | . | Allen Wittenauer | Masatake Iwasaki |
| [HDFS-8071](https://issues.apache.org/jira/browse/HDFS-8071) | Redundant checkFileProgress() in PART II of getAdditionalBlock() |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-11801](https://issues.apache.org/jira/browse/HADOOP-11801) | Update BUILDING.txt for Ubuntu |  Minor | documentation | Gabor Liptak | Gabor Liptak |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-10717](https://issues.apache.org/jira/browse/HADOOP-10717) | HttpServer2 should load jsp DTD from local jars instead of going remote |  Blocker | . | Dapeng Sun | Dapeng Sun |
| [HDFS-6657](https://issues.apache.org/jira/browse/HDFS-6657) | Remove link to 'Legacy UI' in Namenode UI |  Minor | . | Vinayakumar B | Vinayakumar B |
| [HDFS-6938](https://issues.apache.org/jira/browse/HDFS-6938) | Cleanup javac warnings in FSNamesystem |  Trivial | namenode | Charles Lamb | Charles Lamb |
| [HADOOP-10748](https://issues.apache.org/jira/browse/HADOOP-10748) | HttpServer2 should not load JspServlet |  Major | . | Haohui Mai | Haohui Mai |
| [HADOOP-11156](https://issues.apache.org/jira/browse/HADOOP-11156) | DelegateToFileSystem should implement getFsStatus(final Path f). |  Major | fs | zhihai xu | zhihai xu |
| [HDFS-7194](https://issues.apache.org/jira/browse/HDFS-7194) | Fix findbugs "inefficient new String constructor" warning in DFSClient#PATH |  Trivial | . | Yongjun Zhang | Yongjun Zhang |
| [HADOOP-11166](https://issues.apache.org/jira/browse/HADOOP-11166) | Remove ulimit from test-patch.sh |  Major | . | Andrew Wang | Andrew Wang |
| [HDFS-7202](https://issues.apache.org/jira/browse/HDFS-7202) | Should be able to omit package name of SpanReceiver on "hadoop trace -add" |  Minor | . | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-7198](https://issues.apache.org/jira/browse/HDFS-7198) | Fix findbugs "unchecked conversion" warning in DFSClient#getPathTraceScope |  Trivial | . | Colin P. McCabe | Colin P. McCabe |
| [HDFS-7201](https://issues.apache.org/jira/browse/HDFS-7201) | Fix typos in hdfs-default.xml |  Major | . | Konstantin Shvachko | Dawson Choong |
| [HDFS-7277](https://issues.apache.org/jira/browse/HDFS-7277) | Remove explicit dependency on netty 3.2 in BKJournal |  Minor | build | Haohui Mai | Haohui Mai |
| [HDFS-7227](https://issues.apache.org/jira/browse/HDFS-7227) | Fix findbugs warning about NP\_DEREFERENCE\_OF\_READLINE\_VALUE in SpanReceiverHost |  Minor | . | Colin P. McCabe | Colin P. McCabe |
| [HDFS-7232](https://issues.apache.org/jira/browse/HDFS-7232) | Populate hostname in httpfs audit log |  Trivial | . | Zoran Dimitrijevic | Zoran Dimitrijevic |
| [HDFS-7258](https://issues.apache.org/jira/browse/HDFS-7258) | CacheReplicationMonitor rescan schedule log should use DEBUG level instead of INFO level |  Minor | namenode | Xiaoyu Yao | Xiaoyu Yao |
| [YARN-2713](https://issues.apache.org/jira/browse/YARN-2713) | "RM Home" link in NM should point to one of the RMs in an HA setup |  Major | resourcemanager | Karthik Kambatla | Karthik Kambatla |
| [HADOOP-11236](https://issues.apache.org/jira/browse/HADOOP-11236) | NFS: Fix javadoc warning in RpcProgram.java |  Trivial | documentation | Abhiraj Butala | Abhiraj Butala |
| [HDFS-6538](https://issues.apache.org/jira/browse/HDFS-6538) | Comment format error in ShortCircuitRegistry javadoc |  Trivial | datanode | debugging | David Luo |
| [HDFS-7282](https://issues.apache.org/jira/browse/HDFS-7282) | Fix intermittent TestShortCircuitCache and TestBlockReaderFactory failures resulting from TemporarySocketDirectory GC |  Major | test | Jinghui Wang | Jinghui Wang |
| [HDFS-7213](https://issues.apache.org/jira/browse/HDFS-7213) | processIncrementalBlockReport performance degradation |  Critical | namenode | Daryn Sharp | Eric Payne |
| [HDFS-7301](https://issues.apache.org/jira/browse/HDFS-7301) | TestMissingBlocksAlert should use MXBeans instead of old web UI |  Minor | . | Zhe Zhang | Zhe Zhang |
| [HDFS-7235](https://issues.apache.org/jira/browse/HDFS-7235) | DataNode#transferBlock should report blocks that don't exist using reportBadBlock |  Major | datanode, namenode | Yongjun Zhang | Yongjun Zhang |
| [YARN-2742](https://issues.apache.org/jira/browse/YARN-2742) | FairSchedulerConfiguration should allow extra spaces between value and unit |  Minor | fairscheduler | Sangjin Lee | Wei Yan |
| [HADOOP-11186](https://issues.apache.org/jira/browse/HADOOP-11186) | documentation should talk about hadoop.htrace.spanreceiver.classes, not hadoop.trace.spanreceiver.classes |  Minor | . | Colin P. McCabe | Colin P. McCabe |
| [HADOOP-10840](https://issues.apache.org/jira/browse/HADOOP-10840) | Fix OutOfMemoryError caused by metrics system in Azure File System |  Major | metrics | shanyu zhao | shanyu zhao |
| [HDFS-7263](https://issues.apache.org/jira/browse/HDFS-7263) | Snapshot read can reveal future bytes for appended files. |  Major | hdfs-client | Konstantin Shvachko | Tao Luo |
| [HDFS-7315](https://issues.apache.org/jira/browse/HDFS-7315) | DFSTestUtil.readFileBuffer opens extra FSDataInputStream |  Trivial | . | Plamen Jeliazkov | Plamen Jeliazkov |
| [HDFS-6917](https://issues.apache.org/jira/browse/HDFS-6917) | Add an hdfs debug command to validate blocks, call recoverlease, etc. |  Major | hdfs-client | Colin P. McCabe | Colin P. McCabe |
| [HADOOP-11268](https://issues.apache.org/jira/browse/HADOOP-11268) | Update BUILDING.txt to remove the workaround for tools.jar |  Minor | . | Haohui Mai | Li Lu |
| [HADOOP-11230](https://issues.apache.org/jira/browse/HADOOP-11230) | Add missing dependency of bouncycastle for kms, httpfs, hdfs, MR and YARN |  Major | test | Robert Kanter | Robert Kanter |
| [HADOOP-11269](https://issues.apache.org/jira/browse/HADOOP-11269) | Add java 8 profile for hadoop-annotations |  Major | . | Haohui Mai | Li Lu |
| [HADOOP-11271](https://issues.apache.org/jira/browse/HADOOP-11271) | Use Time.monotonicNow() in Shell.java instead of Time.now() |  Minor | . | Vinayakumar B | Vinayakumar B |
| [HADOOP-11266](https://issues.apache.org/jira/browse/HADOOP-11266) | Remove no longer supported activation properties for packaging from pom |  Trivial | build | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-11267](https://issues.apache.org/jira/browse/HADOOP-11267) | TestSecurityUtil fails when run with JDK8 because of empty principal names |  Minor | security, test | Stephen Chu | Stephen Chu |
| [HADOOP-10714](https://issues.apache.org/jira/browse/HADOOP-10714) | AmazonS3Client.deleteObjects() need to be limited to 1000 entries per call |  Critical | fs/s3 | David S. Wang | Juan Yu |
| [HADOOP-11272](https://issues.apache.org/jira/browse/HADOOP-11272) | Allow ZKSignerSecretProvider and ZKDelegationTokenSecretManager to use the same curator client |  Major | . | Arun Suresh | Arun Suresh |
| [HDFS-7366](https://issues.apache.org/jira/browse/HDFS-7366) | BlockInfo should take replication as an short in the constructor |  Minor | . | Haohui Mai | Li Lu |
| [HADOOP-11187](https://issues.apache.org/jira/browse/HADOOP-11187) | NameNode - KMS communication fails after a long period of inactivity |  Major | . | Arun Suresh | Arun Suresh |
| [HDFS-7361](https://issues.apache.org/jira/browse/HDFS-7361) | TestCheckpoint#testStorageAlreadyLockedErrorMessage fails after change of log message related to locking violation. |  Minor | datanode, namenode, test | Chris Nauroth | Konstantin Shvachko |
| [HADOOP-10786](https://issues.apache.org/jira/browse/HADOOP-10786) | Fix UGI#reloginFromKeytab on Java 8 |  Major | security | Tobi Vollebregt | Stephen Chu |
| [HADOOP-11289](https://issues.apache.org/jira/browse/HADOOP-11289) | Fix typo in RpcUtil log message |  Trivial | net | Charles Lamb | Charles Lamb |
| [HADOOP-11294](https://issues.apache.org/jira/browse/HADOOP-11294) | Nfs3FileAttributes should not change the values of rdev, nlink and size in the constructor |  Minor | nfs | Brandon Li | Brandon Li |
| [YARN-2735](https://issues.apache.org/jira/browse/YARN-2735) | diskUtilizationPercentageCutoff and diskUtilizationSpaceCutoff are initialized twice in DirectoryCollection |  Trivial | nodemanager | zhihai xu | zhihai xu |
| [YARN-570](https://issues.apache.org/jira/browse/YARN-570) | Time strings are formated in different timezone |  Major | webapp | Peng Zhang | Akira Ajisaka |
| [HDFS-7389](https://issues.apache.org/jira/browse/HDFS-7389) | Named user ACL cannot stop the user from accessing the FS entity. |  Major | namenode | Chunjun Xiao | Vinayakumar B |
| [HDFS-7358](https://issues.apache.org/jira/browse/HDFS-7358) | Clients may get stuck waiting when using ByteArrayManager |  Major | hdfs-client | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-7395](https://issues.apache.org/jira/browse/HDFS-7395) | BlockIdManager#clear() bails out when resetting the GenerationStampV1Limit |  Major | namenode | Yongjun Zhang | Haohui Mai |
| [YARN-2856](https://issues.apache.org/jira/browse/YARN-2856) | Application recovery throw InvalidStateTransitonException: Invalid event: ATTEMPT\_KILLED at ACCEPTED |  Critical | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [MAPREDUCE-5918](https://issues.apache.org/jira/browse/MAPREDUCE-5918) | LineRecordReader can return the same decompressor to CodecPool multiple times |  Major | . | Sergey Murylev | Sergey Murylev |
| [YARN-2857](https://issues.apache.org/jira/browse/YARN-2857) | ConcurrentModificationException in ContainerLogAppender |  Critical | . | Mohammad Kamrul Islam | Mohammad Kamrul Islam |
| [YARN-2816](https://issues.apache.org/jira/browse/YARN-2816) | NM fail to start with NPE during container recovery |  Major | nodemanager | zhihai xu | zhihai xu |
| [YARN-2811](https://issues.apache.org/jira/browse/YARN-2811) | In Fair Scheduler, reservation fulfillments shouldn't ignore max share |  Major | . | Siqi Li | Siqi Li |
| [YARN-2432](https://issues.apache.org/jira/browse/YARN-2432) | RMStateStore should process the pending events before close |  Major | resourcemanager | Varun Saxena | Varun Saxena |
| [YARN-1703](https://issues.apache.org/jira/browse/YARN-1703) | Too many connections are opened for proxy server when applicationMaster UI is accessed. |  Critical | . | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-7394](https://issues.apache.org/jira/browse/HDFS-7394) | Log at INFO level, not WARN level, when InvalidToken is seen in ShortCircuitCache |  Minor | . | Kihwal Lee | Keith Pak |
| [HDFS-7399](https://issues.apache.org/jira/browse/HDFS-7399) | Lack of synchronization in DFSOutputStream#Packet#getLastByteOffsetBlock() |  Minor | . | Ted Yu | Vinayakumar B |
| [HADOOP-11157](https://issues.apache.org/jira/browse/HADOOP-11157) | ZKDelegationTokenSecretManager never shuts down listenerThreadPool |  Major | security | Gregory Chanan | Arun Suresh |
| [YARN-2414](https://issues.apache.org/jira/browse/YARN-2414) | RM web UI: app page will crash if app is failed before any attempt has been created |  Major | webapp | Zhijie Shen | Wangda Tan |
| [HDFS-7146](https://issues.apache.org/jira/browse/HDFS-7146) | NFS ID/Group lookup requires SSSD enumeration on the server |  Major | nfs | Yongjun Zhang | Yongjun Zhang |
| [MAPREDUCE-6162](https://issues.apache.org/jira/browse/MAPREDUCE-6162) | mapred hsadmin fails on a secure cluster |  Blocker | jobhistoryserver | Jason Lowe | Jason Lowe |
| [MAPREDUCE-6049](https://issues.apache.org/jira/browse/MAPREDUCE-6049) | AM JVM does not exit if MRClientService gracefull shutdown fails |  Major | applicationmaster, resourcemanager | Nishan Shetty | Rohith Sharma K S |
| [HADOOP-11309](https://issues.apache.org/jira/browse/HADOOP-11309) | System class pattern package.Foo should match package.Foo$Bar, too |  Blocker | . | Gera Shegalov | Gera Shegalov |
| [HADOOP-11312](https://issues.apache.org/jira/browse/HADOOP-11312) | Fix unit tests to not use uppercase key names |  Major | security | Andrew Wang | Andrew Wang |
| [HDFS-7406](https://issues.apache.org/jira/browse/HDFS-7406) | SimpleHttpProxyHandler puts incorrect "Connection: Close" header |  Major | . | Haohui Mai | Haohui Mai |
| [YARN-2870](https://issues.apache.org/jira/browse/YARN-2870) | Update examples in document of Timeline Server |  Trivial | documentation, timelineserver | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-11201](https://issues.apache.org/jira/browse/HADOOP-11201) | Hadoop Archives should support globs resolving to files |  Blocker | tools | Gera Shegalov | Gera Shegalov |
| [HDFS-7374](https://issues.apache.org/jira/browse/HDFS-7374) | Allow decommissioning of dead DataNodes |  Major | . | Zhe Zhang | Zhe Zhang |
| [HDFS-7225](https://issues.apache.org/jira/browse/HDFS-7225) | Remove stale block invalidation work when DN re-registers with different UUID |  Major | namenode | Zhe Zhang | Zhe Zhang |
| [YARN-2865](https://issues.apache.org/jira/browse/YARN-2865) | Application recovery continuously fails with "Application with id already present. Cannot duplicate" |  Critical | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-2315](https://issues.apache.org/jira/browse/YARN-2315) | FairScheduler: Set current capacity in addition to capacity |  Major | . | zhihai xu | zhihai xu |
| [HDFS-7403](https://issues.apache.org/jira/browse/HDFS-7403) | Inaccurate javadoc of  BlockUCState#COMPLETE state |  Trivial | namenode | Yongjun Zhang | Yongjun Zhang |
| [YARN-2697](https://issues.apache.org/jira/browse/YARN-2697) | RMAuthenticationHandler is no longer useful |  Major | resourcemanager | Zhijie Shen | haosdent |
| [HDFS-7303](https://issues.apache.org/jira/browse/HDFS-7303) | NN UI fails to distinguish datanodes on the same host |  Minor | . | Benoy Antony | Benoy Antony |
| [HADOOP-11322](https://issues.apache.org/jira/browse/HADOOP-11322) | key based ACL check in KMS always check KeyOpType.MANAGEMENT even actual KeyOpType is not MANAGEMENT |  Major | security | Dian Fu | Dian Fu |
| [MAPREDUCE-5568](https://issues.apache.org/jira/browse/MAPREDUCE-5568) | JHS returns invalid string for reducer completion percentage if AM restarts with 0 reducer. |  Major | . | Jian He | MinJi Kim |
| [HADOOP-11300](https://issues.apache.org/jira/browse/HADOOP-11300) | KMS startup scripts must not display the keystore / truststore passwords |  Major | kms | Arun Suresh | Arun Suresh |
| [YARN-2906](https://issues.apache.org/jira/browse/YARN-2906) | CapacitySchedulerPage shows HTML tags for a queue's Active Users |  Major | capacityscheduler | Jason Lowe | Jason Lowe |
| [HDFS-7097](https://issues.apache.org/jira/browse/HDFS-7097) | Allow block reports to be processed during checkpointing on standby name node |  Critical | . | Kihwal Lee | Kihwal Lee |
| [HDFS-7444](https://issues.apache.org/jira/browse/HDFS-7444) | convertToBlockUnderConstruction should preserve BlockCollection |  Major | . | Haohui Mai | Haohui Mai |
| [YARN-2907](https://issues.apache.org/jira/browse/YARN-2907) | SchedulerNode#toString should print all resource detail instead of only memory. |  Trivial | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [HADOOP-11333](https://issues.apache.org/jira/browse/HADOOP-11333) | Fix deadlock in DomainSocketWatcher when the notification pipe is full |  Major | . | yunjiong zhao | yunjiong zhao |
| [MAPREDUCE-6172](https://issues.apache.org/jira/browse/MAPREDUCE-6172) | TestDbClasses timeouts are too aggressive |  Minor | test | Jason Lowe | Varun Saxena |
| [YARN-2905](https://issues.apache.org/jira/browse/YARN-2905) | AggregatedLogsBlock page can infinitely loop if the aggregated log file is corrupted |  Blocker | . | Jason Lowe | Varun Saxena |
| [MAPREDUCE-6160](https://issues.apache.org/jira/browse/MAPREDUCE-6160) | Potential NullPointerException in MRClientProtocol interface implementation. |  Major | . | Rohith Sharma K S | Rohith Sharma K S |
| [HADOOP-11337](https://issues.apache.org/jira/browse/HADOOP-11337) | KeyAuthorizationKeyProvider access checks need to be done atomically |  Major | . | Dian Fu | Dian Fu |
| [YARN-2136](https://issues.apache.org/jira/browse/YARN-2136) | RMStateStore can explicitly handle store/update events when fenced |  Major | . | Jian He | Varun Saxena |
| [YARN-2894](https://issues.apache.org/jira/browse/YARN-2894) | When ACL's are enabled, if RM switches then application can not be viewed from web. |  Major | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [HADOOP-11344](https://issues.apache.org/jira/browse/HADOOP-11344) | KMS kms-config.sh sets a default value for the keystore password even in non-ssl setup |  Major | . | Arun Suresh | Arun Suresh |
| [HADOOP-11342](https://issues.apache.org/jira/browse/HADOOP-11342) | KMS key ACL should ignore ALL operation for default key ACL and whitelist key ACL |  Major | kms, security | Dian Fu | Dian Fu |
| [YARN-2874](https://issues.apache.org/jira/browse/YARN-2874) | Dead lock in "DelegationTokenRenewer" which blocks RM to execute any further apps |  Blocker | resourcemanager | Naganarasimha G R | Naganarasimha G R |
| [HADOOP-11332](https://issues.apache.org/jira/browse/HADOOP-11332) | KerberosAuthenticator#doSpnegoSequence should check if kerberos TGT is available in the subject |  Major | security | Dian Fu | Dian Fu |
| [HADOOP-11348](https://issues.apache.org/jira/browse/HADOOP-11348) | Remove unused variable from CMake error message for finding openssl |  Minor | . | Dian Fu | Dian Fu |
| [HDFS-7472](https://issues.apache.org/jira/browse/HDFS-7472) | Fix typo in message of ReplicaNotFoundException |  Trivial | . | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-11355](https://issues.apache.org/jira/browse/HADOOP-11355) | When accessing data in HDFS and the key has been deleted, a Null Pointer Exception is shown. |  Minor | . | Arun Suresh | Arun Suresh |
| [YARN-2461](https://issues.apache.org/jira/browse/YARN-2461) | Fix PROCFS\_USE\_SMAPS\_BASED\_RSS\_ENABLED property in YarnConfiguration |  Minor | . | Ray Chiang | Ray Chiang |
| [YARN-2869](https://issues.apache.org/jira/browse/YARN-2869) | CapacityScheduler should trim sub queue names when parse configuration |  Major | capacityscheduler, resourcemanager | Wangda Tan | Wangda Tan |
| [HADOOP-11343](https://issues.apache.org/jira/browse/HADOOP-11343) | Overflow is not properly handled in caclulating final iv for AES CTR |  Blocker | security | Jerry Chen | Jerry Chen |
| [MAPREDUCE-6177](https://issues.apache.org/jira/browse/MAPREDUCE-6177) | Minor typo in the EncryptedShuffle document about ssl-client.xml |  Trivial | documentation | wyp | wyp |
| [HDFS-7473](https://issues.apache.org/jira/browse/HDFS-7473) | Document setting dfs.namenode.fs-limits.max-directory-items to 0 is invalid |  Major | documentation | Jason Keller | Akira Ajisaka |
| [HADOOP-11354](https://issues.apache.org/jira/browse/HADOOP-11354) | ThrottledInputStream doesn't perform effective throttling |  Major | . | Ted Yu | Ted Yu |
| [HADOOP-11329](https://issues.apache.org/jira/browse/HADOOP-11329) | Add JAVA\_LIBRARY\_PATH to KMS startup options |  Major | kms, security | Dian Fu | Arun Suresh |
| [HADOOP-11287](https://issues.apache.org/jira/browse/HADOOP-11287) | Simplify UGI#reloginFromKeytab for Java 7+ |  Major | . | Haohui Mai | Li Lu |
| [YARN-2931](https://issues.apache.org/jira/browse/YARN-2931) | PublicLocalizer may fail until directory is initialized by LocalizeRunner |  Critical | nodemanager | Anubhav Dhoot | Anubhav Dhoot |
| [HADOOP-11363](https://issues.apache.org/jira/browse/HADOOP-11363) | Hadoop maven surefire-plugin uses must set heap size |  Major | build | Steve Loughran | Steve Loughran |
| [HADOOP-10134](https://issues.apache.org/jira/browse/HADOOP-10134) | [JDK8] Fix Javadoc errors caused by incorrect or illegal tags in doc comments |  Minor | . | Andrew Purtell | Andrew Purtell |
| [HDFS-7490](https://issues.apache.org/jira/browse/HDFS-7490) | HDFS tests OOM on Java7+ |  Major | build, test | Steve Loughran | Steve Loughran |
| [HADOOP-11368](https://issues.apache.org/jira/browse/HADOOP-11368) | Fix SSLFactory truststore reloader thread leak in KMSClientProvider |  Major | kms | Arun Suresh | Arun Suresh |
| [HADOOP-11273](https://issues.apache.org/jira/browse/HADOOP-11273) | TestMiniKdc failure: login options not compatible with IBM JDK |  Major | test | Gao Zhong Liang | Gao Zhong Liang |
| [YARN-2910](https://issues.apache.org/jira/browse/YARN-2910) | FSLeafQueue can throw ConcurrentModificationException |  Major | fairscheduler | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [HADOOP-11349](https://issues.apache.org/jira/browse/HADOOP-11349) | RawLocalFileSystem leaks file descriptor while creating a file if creat succeeds but chmod fails. |  Minor | fs | Chris Nauroth | Varun Saxena |
| [HDFS-7481](https://issues.apache.org/jira/browse/HDFS-7481) | Add ACL indicator to the "Permission Denied" exception. |  Minor | namenode | Vinayakumar B | Vinayakumar B |
| [HDFS-7502](https://issues.apache.org/jira/browse/HDFS-7502) | Fix findbugs warning in hdfs-nfs project |  Major | nfs | Brandon Li | Brandon Li |
| [HADOOP-11388](https://issues.apache.org/jira/browse/HADOOP-11388) | Remove deprecated o.a.h.metrics.file.FileContext |  Minor | . | Haohui Mai | Li Lu |
| [HADOOP-11386](https://issues.apache.org/jira/browse/HADOOP-11386) | Replace \\n by %n in format hadoop-common format strings |  Major | . | Li Lu | Li Lu |
| [HDFS-5578](https://issues.apache.org/jira/browse/HDFS-5578) | [JDK8] Fix Javadoc errors caused by incorrect or illegal tags in doc comments |  Minor | . | Andrew Purtell | Andrew Purtell |
| [YARN-2917](https://issues.apache.org/jira/browse/YARN-2917) | Potential deadlock in AsyncDispatcher when system.exit called in AsyncDispatcher#dispatch and AsyscDispatcher#serviceStop from shutdown hook |  Critical | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-7515](https://issues.apache.org/jira/browse/HDFS-7515) | Fix new findbugs warnings in hadoop-hdfs |  Major | . | Haohui Mai | Haohui Mai |
| [HADOOP-11211](https://issues.apache.org/jira/browse/HADOOP-11211) | mapreduce.job.classloader.system.classes semantics should be order-independent |  Major | . | Yitong Zhou | Yitong Zhou |
| [HDFS-7497](https://issues.apache.org/jira/browse/HDFS-7497) | Inconsistent report of decommissioning DataNodes between dfsadmin and NameNode webui |  Major | datanode, namenode | Yongjun Zhang | Yongjun Zhang |
| [YARN-2243](https://issues.apache.org/jira/browse/YARN-2243) | Order of arguments for Preconditions.checkNotNull() is wrong in SchedulerApplicationAttempt ctor |  Minor | . | Ted Yu | Devaraj K |
| [YARN-2912](https://issues.apache.org/jira/browse/YARN-2912) | Jersey Tests failing with port in use |  Major | test | Steve Loughran | Varun Saxena |
| [HDFS-7517](https://issues.apache.org/jira/browse/HDFS-7517) | Remove redundant non-null checks in FSNamesystem#getBlockLocations |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-7514](https://issues.apache.org/jira/browse/HDFS-7514) | TestTextCommand fails on Windows |  Major | test | Arpit Agarwal | Arpit Agarwal |
| [HADOOP-11238](https://issues.apache.org/jira/browse/HADOOP-11238) | Update the NameNode's Group Cache in the background when possible |  Minor | . | Chris Li | Chris Li |
| [HADOOP-11394](https://issues.apache.org/jira/browse/HADOOP-11394) | hadoop-aws documentation missing. |  Major | documentation | Chris Nauroth | Chris Nauroth |
| [MAPREDUCE-4879](https://issues.apache.org/jira/browse/MAPREDUCE-4879) | TeraOutputFormat may overwrite an existing output directory |  Major | examples | Gera Shegalov | Gera Shegalov |
| [YARN-2356](https://issues.apache.org/jira/browse/YARN-2356) | yarn status command for non-existent application/application attempt/container is too verbose |  Minor | client | Sunil G | Sunil G |
| [HDFS-7516](https://issues.apache.org/jira/browse/HDFS-7516) | Fix findbugs warnings in hadoop-nfs project |  Major | nfs | Brandon Li | Brandon Li |
| [HADOOP-10852](https://issues.apache.org/jira/browse/HADOOP-10852) | NetgroupCache is not thread-safe |  Major | security | Benoy Antony | Benoy Antony |
| [MAPREDUCE-6166](https://issues.apache.org/jira/browse/MAPREDUCE-6166) | Reducers do not validate checksum of map outputs when fetching directly to disk |  Major | mrv2 | Eric Payne | Eric Payne |
| [HADOOP-11412](https://issues.apache.org/jira/browse/HADOOP-11412) | POMs mention "The Apache Software License" rather than "Apache License" |  Trivial | . | Hervé Boutemy | Hervé Boutemy |
| [HDFS-6425](https://issues.apache.org/jira/browse/HDFS-6425) | Large postponedMisreplicatedBlocks has impact on blockReport latency |  Major | namenode | Ming Ma | Ming Ma |
| [HDFS-7494](https://issues.apache.org/jira/browse/HDFS-7494) | Checking of closed in DFSInputStream#pread() should be protected by synchronization |  Minor | . | Ted Yu | Ted Yu |
| [HDFS-7536](https://issues.apache.org/jira/browse/HDFS-7536) | Remove unused CryptoCodec in org.apache.hadoop.fs.Hdfs |  Minor | security | Yi Liu | Yi Liu |
| [HADOOP-11321](https://issues.apache.org/jira/browse/HADOOP-11321) | copyToLocal cannot save a file to an SMB share unless the user has Full Control permissions. |  Major | fs | Chris Nauroth | Chris Nauroth |
| [YARN-2945](https://issues.apache.org/jira/browse/YARN-2945) | FSLeafQueue#assignContainer - document the reason for using both write and read locks |  Major | . | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [HADOOP-11420](https://issues.apache.org/jira/browse/HADOOP-11420) | Use latest maven-site-plugin and replace link to svn with link to git |  Trivial | site | Hervé Boutemy | Hervé Boutemy |
| [HADOOP-10689](https://issues.apache.org/jira/browse/HADOOP-10689) | InputStream is not closed in AzureNativeFileSystemStore#retrieve() |  Minor | tools | Ted Yu | Chen He |
| [HADOOP-10690](https://issues.apache.org/jira/browse/HADOOP-10690) | Lack of synchronization on access to InputStream in NativeAzureFileSystem#NativeAzureFsInputStream#close() |  Minor | tools | Ted Yu | Chen He |
| [HADOOP-11248](https://issues.apache.org/jira/browse/HADOOP-11248) | Add hadoop configuration to disable Azure Filesystem metrics collection |  Major | fs | shanyu zhao | shanyu zhao |
| [YARN-2972](https://issues.apache.org/jira/browse/YARN-2972) | DelegationTokenRenewer thread pool never expands |  Major | resourcemanager | Jason Lowe | Jason Lowe |
| [HADOOP-11246](https://issues.apache.org/jira/browse/HADOOP-11246) | Move jenkins to Java 7 |  Major | . | Haohui Mai | Steve Loughran |
| [HDFS-7373](https://issues.apache.org/jira/browse/HDFS-7373) | Clean up temporary files after fsimage transfer failures |  Major | . | Kihwal Lee | Kihwal Lee |
| [HADOOP-11409](https://issues.apache.org/jira/browse/HADOOP-11409) | FileContext.getFileContext can stack overflow if default fs misconfigured |  Major | . | Jason Lowe | Gera Shegalov |
| [HADOOP-11428](https://issues.apache.org/jira/browse/HADOOP-11428) | Remove obsolete reference to Cygwin in BUILDING.txt |  Major | documentation | Arpit Agarwal | Arpit Agarwal |
| [HDFS-7530](https://issues.apache.org/jira/browse/HDFS-7530) | Allow renaming of encryption zone roots |  Minor | namenode | Charles Lamb | Charles Lamb |
| [HDFS-7431](https://issues.apache.org/jira/browse/HDFS-7431) | log message for InvalidMagicNumberException may be incorrect |  Major | security | Yi Liu | Yi Liu |
| [YARN-2964](https://issues.apache.org/jira/browse/YARN-2964) | RM prematurely cancels tokens for jobs that submit jobs (oozie) |  Blocker | resourcemanager | Daryn Sharp | Jian He |
| [HADOOP-11431](https://issues.apache.org/jira/browse/HADOOP-11431) | clean up redundant maven-site-plugin configuration |  Major | . | Hervé Boutemy | Hervé Boutemy |
| [HDFS-7552](https://issues.apache.org/jira/browse/HDFS-7552) | change FsVolumeList toString() to fix TestDataNodeVolumeFailureToleration |  Major | datanode, test | Liang Xie | Liang Xie |
| [MAPREDUCE-6045](https://issues.apache.org/jira/browse/MAPREDUCE-6045) | need close the DataInputStream after open it in TestMapReduce.java |  Minor | test | zhihai xu | zhihai xu |
| [HADOOP-11213](https://issues.apache.org/jira/browse/HADOOP-11213) | Typos in html pages: SecureMode and EncryptedShuffle |  Minor | . | Wei Yan | Wei Yan |
| [YARN-2675](https://issues.apache.org/jira/browse/YARN-2675) | containersKilled metrics is not updated when the container is killed during localization |  Major | nodemanager | zhihai xu | zhihai xu |
| [YARN-2952](https://issues.apache.org/jira/browse/YARN-2952) | Incorrect version check in RMStateStore |  Major | . | Jian He | Rohith Sharma K S |
| [YARN-2977](https://issues.apache.org/jira/browse/YARN-2977) | TestNMClient get failed intermittently |  Major | . | Junping Du | Junping Du |
| [YARN-2975](https://issues.apache.org/jira/browse/YARN-2975) | FSLeafQueue app lists are accessed without required locks |  Blocker | . | Karthik Kambatla | Karthik Kambatla |
| [HADOOP-11411](https://issues.apache.org/jira/browse/HADOOP-11411) | Hive build failure on hadoop-2.7 due to HADOOP-11356 |  Major | . | Jason Dere |  |
| [HADOOP-11414](https://issues.apache.org/jira/browse/HADOOP-11414) | FileBasedIPList#readLines() can leak file descriptors |  Minor | . | Ted Yu | Tsuyoshi Ozawa |
| [HDFS-7560](https://issues.apache.org/jira/browse/HDFS-7560) | ACLs removed by removeDefaultAcl() will be back after NameNode restart/failover |  Critical | namenode | Vinayakumar B | Vinayakumar B |
| [YARN-2340](https://issues.apache.org/jira/browse/YARN-2340) | NPE thrown when RM restart after queue is STOPPED. There after RM can not recovery application's and remain in standby |  Critical | resourcemanager, scheduler | Nishan Shetty | Rohith Sharma K S |
| [HDFS-7456](https://issues.apache.org/jira/browse/HDFS-7456) | De-duplicate AclFeature instances with same AclEntries do reduce memory footprint of NameNode |  Major | namenode | Vinayakumar B | Vinayakumar B |
| [YARN-2988](https://issues.apache.org/jira/browse/YARN-2988) | Graph#save() may leak file descriptors |  Minor | . | Ted Yu | Ted Yu |
| [HADOOP-11283](https://issues.apache.org/jira/browse/HADOOP-11283) | Potentially unclosed SequenceFile.Writer in DistCpV1#setup() |  Minor | . | Ted Yu | Varun Saxena |
| [YARN-2993](https://issues.apache.org/jira/browse/YARN-2993) | Several fixes (missing acl check, error log msg ...) and some refinement in AdminService |  Major | resourcemanager | Yi Liu | Yi Liu |
| [YARN-2992](https://issues.apache.org/jira/browse/YARN-2992) | ZKRMStateStore crashes due to session expiry |  Blocker | resourcemanager | Karthik Kambatla | Karthik Kambatla |
| [HADOOP-11039](https://issues.apache.org/jira/browse/HADOOP-11039) | ByteBufferReadable API doc is inconsistent with the implementations. |  Minor | documentation | Yi Liu | Yi Liu |
| [YARN-2987](https://issues.apache.org/jira/browse/YARN-2987) | ClientRMService#getQueueInfo doesn't check app ACLs |  Major | . | Jian He | Varun Saxena |
| [HDFS-7563](https://issues.apache.org/jira/browse/HDFS-7563) | NFS gateway parseStaticMap NumberFormatException |  Major | nfs | Hari Sekhon | Yongjun Zhang |
| [YARN-2991](https://issues.apache.org/jira/browse/YARN-2991) | TestRMRestart.testDecomissionedNMsMetricsOnRMRestart intermittently fails on trunk |  Blocker | . | Zhijie Shen | Rohith Sharma K S |
| [YARN-2983](https://issues.apache.org/jira/browse/YARN-2983) | NPE possible in ClientRMService#getQueueInfo |  Major | resourcemanager | Varun Saxena | Varun Saxena |
| [YARN-2922](https://issues.apache.org/jira/browse/YARN-2922) | ConcurrentModificationException in CapacityScheduler's LeafQueue |  Major | capacityscheduler, resourcemanager, scheduler | Jason Tufo | Rohith Sharma K S |
| [HADOOP-11446](https://issues.apache.org/jira/browse/HADOOP-11446) | S3AOutputStream should use shared thread pool to avoid OutOfMemoryError |  Major | fs/s3 | Ted Yu | Ted Yu |
| [HDFS-7572](https://issues.apache.org/jira/browse/HDFS-7572) | TestLazyPersistFiles#testDnRestartWithSavedReplicas is flaky on Windows |  Major | test | Arpit Agarwal | Arpit Agarwal |
| [YARN-2958](https://issues.apache.org/jira/browse/YARN-2958) | RMStateStore seems to unnecessarily and wrongly store sequence number separately |  Blocker | resourcemanager | Zhijie Shen | Varun Saxena |
| [HADOOP-11402](https://issues.apache.org/jira/browse/HADOOP-11402) | Negative user-to-group cache entries are never cleared for never-again-accessed users |  Major | . | Colin P. McCabe | Varun Saxena |
| [HADOOP-11459](https://issues.apache.org/jira/browse/HADOOP-11459) | Fix recent findbugs in ActiveStandbyElector, NetUtils and ShellBasedIdMapping |  Minor | . | Vinayakumar B | Vinayakumar B |
| [HDFS-7583](https://issues.apache.org/jira/browse/HDFS-7583) | Fix findbug in TransferFsImage.java |  Minor | namenode | Vinayakumar B | Vinayakumar B |
| [HADOOP-11445](https://issues.apache.org/jira/browse/HADOOP-11445) | Bzip2Codec: Data block is skipped when position of newly created stream is equal to start of split |  Critical | . | Ankit Kamboj | Ankit Kamboj |
| [YARN-2978](https://issues.apache.org/jira/browse/YARN-2978) | ResourceManager crashes with NPE while getting queue info |  Critical | . | Jason Tufo | Varun Saxena |
| [MAPREDUCE-6206](https://issues.apache.org/jira/browse/MAPREDUCE-6206) | TestAggregatedTransferRate fails on non-US systems |  Critical | . | Jens Rabe | Jens Rabe |
| [YARN-2230](https://issues.apache.org/jira/browse/YARN-2230) | Fix description of yarn.scheduler.maximum-allocation-vcores in yarn-default.xml (or code) |  Minor | client, documentation, scheduler | Adam Kawa | Vijay Bhat |
| [HADOOP-11462](https://issues.apache.org/jira/browse/HADOOP-11462) | TestSocketIOWithTimeout needs change for PowerPC platform |  Major | test | Ayappan | Ayappan |
| [YARN-3010](https://issues.apache.org/jira/browse/YARN-3010) | Fix recent findbug issue in AbstractYarnScheduler |  Minor | . | Yi Liu | Yi Liu |
| [HDFS-7561](https://issues.apache.org/jira/browse/HDFS-7561) | TestFetchImage should write fetched-image-dir under target. |  Major | . | Konstantin Shvachko | Liang Xie |
| [YARN-2936](https://issues.apache.org/jira/browse/YARN-2936) | YARNDelegationTokenIdentifier doesn't set proto.builder now |  Major | . | Zhijie Shen | Varun Saxena |
| [YARN-2997](https://issues.apache.org/jira/browse/YARN-2997) | NM keeps sending already-sent completed containers to RM until containers are removed from context |  Major | nodemanager | Chengbing Liu | Chengbing Liu |
| [HADOOP-11470](https://issues.apache.org/jira/browse/HADOOP-11470) | Remove some uses of obsolete guava APIs from the hadoop codebase |  Major | . | Sangjin Lee | Sangjin Lee |
| [YARN-2956](https://issues.apache.org/jira/browse/YARN-2956) | Some yarn-site index linked pages are difficult to discover because are not in the side bar |  Minor | documentation | Remus Rusanu | Masatake Iwasaki |
| [HADOOP-11400](https://issues.apache.org/jira/browse/HADOOP-11400) | GraphiteSink does not reconnect to Graphite after 'broken pipe' |  Major | metrics | Kamil Gorlo | Kamil Gorlo |
| [HDFS-7596](https://issues.apache.org/jira/browse/HDFS-7596) | NameNode should prune dead storages from storageMap |  Major | namenode | Arpit Agarwal | Arpit Agarwal |
| [YARN-3027](https://issues.apache.org/jira/browse/YARN-3027) | Scheduler should use totalAvailable resource from node instead of availableResource for maxAllocation |  Major | . | Anubhav Dhoot | Anubhav Dhoot |
| [HDFS-7533](https://issues.apache.org/jira/browse/HDFS-7533) | Datanode sometimes does not shutdown on receiving upgrade shutdown command |  Major | . | Kihwal Lee | Eric Payne |
| [HDFS-5445](https://issues.apache.org/jira/browse/HDFS-5445) | PacketReceiver populates the packetLen field in PacketHeader incorrectly |  Minor | datanode | Jonathan Mace | Jonathan Mace |
| [HDFS-7470](https://issues.apache.org/jira/browse/HDFS-7470) | SecondaryNameNode need twice memory when calling reloadFromImageFile |  Major | namenode | yunjiong zhao | yunjiong zhao |
| [YARN-2637](https://issues.apache.org/jira/browse/YARN-2637) | maximum-am-resource-percent could be respected for both LeafQueue/User when trying to activate applications. |  Critical | resourcemanager | Wangda Tan | Craig Welch |
| [MAPREDUCE-6210](https://issues.apache.org/jira/browse/MAPREDUCE-6210) | Use getApplicationAttemptId() instead of getApplicationID() for logging AttemptId in RMContainerAllocator.java |  Minor | applicationmaster | Leitao Guo | Leitao Guo |
| [HADOOP-11318](https://issues.apache.org/jira/browse/HADOOP-11318) | Update the document for hadoop fs -stat |  Major | documentation | Akira Ajisaka | Akira Ajisaka |
| [YARN-2861](https://issues.apache.org/jira/browse/YARN-2861) | Timeline DT secret manager should not reuse the RM's configs. |  Major | . | Zhijie Shen | Zhijie Shen |
| [HDFS-7615](https://issues.apache.org/jira/browse/HDFS-7615) | Remove longReadLock |  Major | . | Kihwal Lee | Kihwal Lee |
| [HDFS-7457](https://issues.apache.org/jira/browse/HDFS-7457) | DatanodeID generates excessive garbage |  Major | namenode | Daryn Sharp | Daryn Sharp |
| [HADOOP-11350](https://issues.apache.org/jira/browse/HADOOP-11350) | The size of header buffer of HttpServer is too small when HTTPS is enabled |  Major | security | Benoy Antony | Benoy Antony |
| [YARN-3064](https://issues.apache.org/jira/browse/YARN-3064) | TestRMRestart/TestContainerResourceUsage/TestNodeManagerResync failure with allocation timeout |  Critical | scheduler | Wangda Tan | Jian He |
| [HDFS-7635](https://issues.apache.org/jira/browse/HDFS-7635) | Remove TestCorruptFilesJsp from branch-2. |  Minor | test | Chris Nauroth | Chris Nauroth |
| [HDFS-7632](https://issues.apache.org/jira/browse/HDFS-7632) | MiniDFSCluster configures DataNode data directories incorrectly if using more than 1 DataNode and more than 2 storage locations per DataNode. |  Major | test | Chris Nauroth | Chris Nauroth |
| [HDFS-7606](https://issues.apache.org/jira/browse/HDFS-7606) | Missing null check in INodeFile#getBlocks() |  Minor | . | Ted Yu | Byron Wong |
| [YARN-2815](https://issues.apache.org/jira/browse/YARN-2815) | Remove jline from hadoop-yarn-server-common |  Major | . | Ferdinand Xu | Ferdinand Xu |
| [HADOOP-10542](https://issues.apache.org/jira/browse/HADOOP-10542) | Potential null pointer dereference in Jets3tFileSystemStore#retrieveBlock() |  Minor | fs/s3 | Ted Yu | Ted Yu |
| [YARN-3071](https://issues.apache.org/jira/browse/YARN-3071) | Remove invalid char from sample conf in doc of FairScheduler |  Trivial | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-7637](https://issues.apache.org/jira/browse/HDFS-7637) | Fix the check condition for reserved path |  Minor | . | Yi Liu | Yi Liu |
| [YARN-3015](https://issues.apache.org/jira/browse/YARN-3015) | yarn classpath command should support same options as hadoop classpath. |  Minor | scripts | Chris Nauroth | Varun Saxena |
| [YARN-2731](https://issues.apache.org/jira/browse/YARN-2731) | Fixed RegisterApplicationMasterResponsePBImpl to properly invoke maybeInitBuilder |  Major | . | Carlo Curino | Carlo Curino |
| [HDFS-7641](https://issues.apache.org/jira/browse/HDFS-7641) | Update archival storage user doc for list/set/get block storage policies |  Minor | documentation | Yi Liu | Yi Liu |
| [HDFS-7496](https://issues.apache.org/jira/browse/HDFS-7496) | Fix FsVolume removal race conditions on the DataNode by reference-counting the volume instances |  Major | . | Colin P. McCabe | Lei (Eddy) Xu |
| [HDFS-7610](https://issues.apache.org/jira/browse/HDFS-7610) | Fix removal of dynamically added DN volumes |  Major | datanode | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HADOOP-11256](https://issues.apache.org/jira/browse/HADOOP-11256) | Some site docs have inconsistent appearance |  Minor | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-11327](https://issues.apache.org/jira/browse/HADOOP-11327) | BloomFilter#not() omits the last bit, resulting in an incorrect filter |  Minor | util | Tim Luo | Eric Payne |
| [HDFS-7548](https://issues.apache.org/jira/browse/HDFS-7548) | Corrupt block reporting delayed until datablock scanner thread detects it |  Major | . | Rushabh S Shah | Rushabh S Shah |
| [MAPREDUCE-3283](https://issues.apache.org/jira/browse/MAPREDUCE-3283) | mapred classpath CLI does not display the complete classpath |  Minor | scripts | Ramya Sunil | Varun Saxena |
| [HADOOP-11209](https://issues.apache.org/jira/browse/HADOOP-11209) | Configuration#updatingResource/finalParameters are not thread-safe |  Major | conf | Josh Rosen | Varun Saxena |
| [YARN-3078](https://issues.apache.org/jira/browse/YARN-3078) | LogCLIHelpers lacks of a blank space before string 'does not exist' |  Minor | log-aggregation | sam liu |  |
| [HADOOP-11500](https://issues.apache.org/jira/browse/HADOOP-11500) | InputStream is left unclosed in ApplicationClassLoader |  Major | . | Ted Yu | Ted Yu |
| [HDFS-7575](https://issues.apache.org/jira/browse/HDFS-7575) | Upgrade should generate a unique storage ID for each volume |  Critical | . | Lars Francke | Arpit Agarwal |
| [HADOOP-11008](https://issues.apache.org/jira/browse/HADOOP-11008) | Remove duplicated description about proxy-user in site documents |  Minor | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-3519](https://issues.apache.org/jira/browse/HDFS-3519) | Checkpoint upload may interfere with a concurrent saveNamespace |  Critical | namenode | Todd Lipcon | Ming Ma |
| [HADOOP-11493](https://issues.apache.org/jira/browse/HADOOP-11493) | Fix some typos in kms-acls.xml description |  Trivial | kms | Charles Lamb | Charles Lamb |
| [HDFS-7660](https://issues.apache.org/jira/browse/HDFS-7660) | BlockReceiver#close() might be called multiple times, which causes the fsvolume reference being released incorrectly. |  Minor | . | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [YARN-3082](https://issues.apache.org/jira/browse/YARN-3082) | Non thread safe access to systemCredentials in NodeHeartbeatResponse processing |  Major | . | Anubhav Dhoot | Anubhav Dhoot |
| [HADOOP-11507](https://issues.apache.org/jira/browse/HADOOP-11507) | Hadoop RPC Authentication problem with different user locale |  Minor | . | Talat UYARER | Talat UYARER |
| [HADOOP-11482](https://issues.apache.org/jira/browse/HADOOP-11482) | Use correct UGI when KMSClientProvider is called by a proxy user |  Major | . | Arun Suresh | Arun Suresh |
| [HDFS-7644](https://issues.apache.org/jira/browse/HDFS-7644) | minor typo in HttpFS doc |  Trivial | documentation | Charles Lamb | Charles Lamb |
| [YARN-3024](https://issues.apache.org/jira/browse/YARN-3024) | LocalizerRunner should give DIE action when all resources are localized |  Major | nodemanager | Chengbing Liu | Chengbing Liu |
| [HADOOP-11450](https://issues.apache.org/jira/browse/HADOOP-11450) | Cleanup DistCpV1 not to use deprecated methods and fix javadocs |  Minor | tools/distcp | Tsuyoshi Ozawa | Varun Saxena |
| [HDFS-7224](https://issues.apache.org/jira/browse/HDFS-7224) | Allow reuse of NN connections via webhdfs |  Major | webhdfs | Eric Payne | Eric Payne |
| [YARN-3088](https://issues.apache.org/jira/browse/YARN-3088) | LinuxContainerExecutor.deleteAsUser can throw NPE if native executor returns an error |  Major | nodemanager | Jason Lowe | Eric Payne |
| [HADOOP-11499](https://issues.apache.org/jira/browse/HADOOP-11499) | Check of executorThreadsStarted in ValueQueue#submitRefillTask() evades lock acquisition |  Minor | . | Ted Yu | Ted Yu |
| [HADOOP-6221](https://issues.apache.org/jira/browse/HADOOP-6221) | RPC Client operations cannot be interrupted |  Minor | ipc | Steve Loughran | Steve Loughran |
| [HADOOP-11509](https://issues.apache.org/jira/browse/HADOOP-11509) | change parsing sequence in GenericOptionsParser to parse -D parameters first |  Major | . | Xuan Gong | Xuan Gong |
| [HDFS-49](https://issues.apache.org/jira/browse/HDFS-49) | MiniDFSCluster.stopDataNode will always shut down a node in the cluster if a matching name is not found |  Minor | test | Steve Loughran | Steve Loughran |
| [YARN-2897](https://issues.apache.org/jira/browse/YARN-2897) | CrossOriginFilter needs more log statements |  Major | . | Mit Desai | Mit Desai |
| [HDFS-7566](https://issues.apache.org/jira/browse/HDFS-7566) | Remove obsolete entries from hdfs-default.xml |  Major | . | Ray Chiang | Ray Chiang |
| [YARN-2932](https://issues.apache.org/jira/browse/YARN-2932) | Add entry for "preemptable" status (enabled/disabled) to scheduler web UI and queue initialize/refresh logging |  Major | . | Eric Payne | Eric Payne |
| [HADOOP-11469](https://issues.apache.org/jira/browse/HADOOP-11469) | KMS should skip default.key.acl and whitelist.key.acl when loading key acl |  Minor | kms | Dian Fu | Dian Fu |
| [HADOOP-11316](https://issues.apache.org/jira/browse/HADOOP-11316) | "mvn package -Pdist,docs -DskipTests -Dtar" fails because of non-ascii characters |  Blocker | . | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [YARN-3103](https://issues.apache.org/jira/browse/YARN-3103) | AMRMClientImpl does not update AMRM token properly |  Blocker | client | Jason Lowe | Jason Lowe |
| [HDFS-7611](https://issues.apache.org/jira/browse/HDFS-7611) | deleteSnapshot and delete of a file can leave orphaned blocks in the blocksMap on NameNode restart. |  Critical | namenode | Konstantin Shvachko | Jing Zhao |
| [MAPREDUCE-6230](https://issues.apache.org/jira/browse/MAPREDUCE-6230) | MR AM does not survive RM restart if RM activated a new AMRM secret key |  Blocker | mr-am | Jason Lowe | Jason Lowe |
| [HDFS-7423](https://issues.apache.org/jira/browse/HDFS-7423) | various typos and message formatting fixes in nfs daemon and doc |  Trivial | nfs | Charles Lamb | Charles Lamb |
| [YARN-3079](https://issues.apache.org/jira/browse/YARN-3079) | Scheduler should also update maximumAllocation when updateNodeResource. |  Major | . | zhihai xu | zhihai xu |
| [MAPREDUCE-6231](https://issues.apache.org/jira/browse/MAPREDUCE-6231) | Grep example job is not working on a fully-distributed cluster |  Major | examples | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-9907](https://issues.apache.org/jira/browse/HADOOP-9907) | Webapp http://hostname:port/metrics  link is not working |  Critical | . | Jian He | Akira Ajisaka |
| [HDFS-7603](https://issues.apache.org/jira/browse/HDFS-7603) | The background replication queue initialization may not let others run |  Critical | rolling upgrades | Kihwal Lee | Kihwal Lee |
| [YARN-3029](https://issues.apache.org/jira/browse/YARN-3029) | FSDownload.unpack() uses local locale for FS case conversion, may not work everywhere |  Major | nodemanager | Steve Loughran | Varun Saxena |
| [HADOOP-11403](https://issues.apache.org/jira/browse/HADOOP-11403) | Avoid using sys\_errlist on Solaris, which lacks support for it |  Major | . | Malcolm Kavalsky | Malcolm Kavalsky |
| [HADOOP-11523](https://issues.apache.org/jira/browse/HADOOP-11523) | StorageException complaining " no lease ID" when updating FolderLastModifiedTime in WASB |  Major | tools | Duo Xu | Duo Xu |
| [HADOOP-9137](https://issues.apache.org/jira/browse/HADOOP-9137) | Support connection limiting in IPC server |  Major | . | Sanjay Radia | Kihwal Lee |
| [HADOOP-11488](https://issues.apache.org/jira/browse/HADOOP-11488) | Difference in default connection timeout for S3A FS |  Minor | fs/s3 | Harsh J | Daisuke Kobayashi |
| [HADOOP-11494](https://issues.apache.org/jira/browse/HADOOP-11494) | Lock acquisition on WrappedInputStream#unwrappedRpcBuffer may race with another thread |  Minor | . | Ted Yu | Ted Yu |
| [YARN-3113](https://issues.apache.org/jira/browse/YARN-3113) | Release audit warning for "Sorting icons.psd" |  Major | . | Chang Li | Steve Loughran |
| [HADOOP-10181](https://issues.apache.org/jira/browse/HADOOP-10181) | GangliaContext does not work with multicast ganglia setup |  Minor | metrics | Andrew Otto | Andrew Johnson |
| [YARN-2808](https://issues.apache.org/jira/browse/YARN-2808) | yarn client tool can not list app\_attempt's container info correctly |  Major | client | Gordon Wang | Naganarasimha G R |
| [HDFS-7696](https://issues.apache.org/jira/browse/HDFS-7696) | FsDatasetImpl.getTmpInputStreams(..) may leak file descriptors |  Minor | datanode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-11529](https://issues.apache.org/jira/browse/HADOOP-11529) | Fix findbugs warnings in hadoop-archives |  Minor | tools | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-7721](https://issues.apache.org/jira/browse/HDFS-7721) | The HDFS BlockScanner may run fast during the first hour |  Major | datanode | Tsz Wo Nicholas Sze | Colin P. McCabe |
| [HDFS-7707](https://issues.apache.org/jira/browse/HDFS-7707) | Edit log corruption due to delayed block removal again |  Major | namenode | Yongjun Zhang | Yongjun Zhang |
| [YARN-3058](https://issues.apache.org/jira/browse/YARN-3058) | Fix error message of tokens' activation delay configuration |  Minor | . | Yi Liu | Yi Liu |
| [HADOOP-11546](https://issues.apache.org/jira/browse/HADOOP-11546) | Checkstyle failing: Unable to instantiate DoubleCheckedLockingCheck |  Major | build | Steve Loughran | Tsuyoshi Ozawa |
| [MAPREDUCE-6243](https://issues.apache.org/jira/browse/MAPREDUCE-6243) | Fix findbugs warnings in hadoop-rumen |  Minor | tools/rumen | Akira Ajisaka | Masatake Iwasaki |
| [HADOOP-11548](https://issues.apache.org/jira/browse/HADOOP-11548) | checknative should display a nicer error message when openssl support is not compiled in |  Major | build, native | Colin P. McCabe | Anu Engineer |
| [HDFS-7734](https://issues.apache.org/jira/browse/HDFS-7734) | Class cast exception in NameNode#main |  Blocker | namenode | Arpit Agarwal | Yi Liu |
| [HADOOP-11547](https://issues.apache.org/jira/browse/HADOOP-11547) | hadoop-common native compilation fails on Windows due to missing support for \_\_attribute\_\_ declaration. |  Major | native | Chris Nauroth | Chris Nauroth |
| [HDFS-7719](https://issues.apache.org/jira/browse/HDFS-7719) | BlockPoolSliceStorage#removeVolumes fails to remove some in-memory state associated with volumes |  Major | . | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HADOOP-11549](https://issues.apache.org/jira/browse/HADOOP-11549) | flaky test detection tool failed to handle special control characters in test result |  Major | tools | Yongjun Zhang | Yongjun Zhang |
| [MAPREDUCE-5988](https://issues.apache.org/jira/browse/MAPREDUCE-5988) | Fix dead links to the javadocs in mapreduce project |  Minor | documentation | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-10062](https://issues.apache.org/jira/browse/HADOOP-10062) | race condition in MetricsSystemImpl#publishMetricsNow that causes incorrect results |  Major | metrics | Shinichi Yamashita | Sangjin Lee |
| [HDFS-7709](https://issues.apache.org/jira/browse/HDFS-7709) | Fix findbug warnings in httpfs |  Major | . | Rakesh R | Rakesh R |
| [YARN-3101](https://issues.apache.org/jira/browse/YARN-3101) | In Fair Scheduler, fix canceling of reservations for exceeding max share |  Major | fairscheduler | Anubhav Dhoot | Anubhav Dhoot |
| [MAPREDUCE-6186](https://issues.apache.org/jira/browse/MAPREDUCE-6186) | Redundant call to requireJob() while displaying the conf page |  Minor | jobhistoryserver | Rohit Agarwal | Rohit Agarwal |
| [MAPREDUCE-6233](https://issues.apache.org/jira/browse/MAPREDUCE-6233) | org.apache.hadoop.mapreduce.TestLargeSort.testLargeSort failed in trunk |  Major | test | Yongjun Zhang | zhihai xu |
| [YARN-3149](https://issues.apache.org/jira/browse/YARN-3149) | Typo in message for invalid application id |  Trivial | resourcemanager | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-3145](https://issues.apache.org/jira/browse/YARN-3145) | ConcurrentModificationException on CapacityScheduler ParentQueue#getQueueUserAclInfo |  Major | . | Jian He | Tsuyoshi Ozawa |
| [HDFS-7698](https://issues.apache.org/jira/browse/HDFS-7698) | Fix locking on HDFS read statistics and add a method for clearing them. |  Major | . | Colin P. McCabe | Colin P. McCabe |
| [HADOOP-11526](https://issues.apache.org/jira/browse/HADOOP-11526) | Memory leak in Bzip2Compressor and Bzip2Decompressor |  Major | io, native | Ian Rogers | Anu Engineer |
| [HDFS-7741](https://issues.apache.org/jira/browse/HDFS-7741) | Remove unnecessary synchronized in FSDataInputStream and HdfsDataInputStream |  Minor | . | Yi Liu | Yi Liu |
| [YARN-3089](https://issues.apache.org/jira/browse/YARN-3089) | LinuxContainerExecutor does not handle file arguments to deleteAsUser |  Blocker | . | Jason Lowe | Eric Payne |
| [YARN-3143](https://issues.apache.org/jira/browse/YARN-3143) | RM Apps REST API can return NPE or entries missing id and other fields |  Major | webapp | Kendall Thrapp | Jason Lowe |
| [YARN-2990](https://issues.apache.org/jira/browse/YARN-2990) | FairScheduler's delay-scheduling always waits for node-local and rack-local delays, even for off-rack-only requests |  Major | fairscheduler | Karthik Kambatla | Karthik Kambatla |
| [HDFS-7647](https://issues.apache.org/jira/browse/HDFS-7647) | DatanodeManager.sortLocatedBlocks sorts DatanodeInfos but not StorageIDs |  Major | . | Milan Desai | Milan Desai |
| [YARN-3094](https://issues.apache.org/jira/browse/YARN-3094) | reset timer for liveness monitors after RM recovery |  Major | resourcemanager | Jun Gong | Jun Gong |
| [YARN-3155](https://issues.apache.org/jira/browse/YARN-3155) | Refactor the exception handling code for TimelineClientImpl's retryOn method |  Minor | . | Li Lu | Li Lu |
| [HDFS-7756](https://issues.apache.org/jira/browse/HDFS-7756) | Restore method signature for LocatedBlock#getLocations() |  Major | . | Ted Yu | Ted Yu |
| [HDFS-7744](https://issues.apache.org/jira/browse/HDFS-7744) | Fix potential NPE in DFSInputStream after setDropBehind or setReadahead is called |  Major | hdfs-client | Colin P. McCabe | Colin P. McCabe |
| [HDFS-7718](https://issues.apache.org/jira/browse/HDFS-7718) | Store KeyProvider in ClientContext to avoid leaking key provider threads when using FileContext |  Major | . | Arun Suresh | Arun Suresh |
| [HDFS-7714](https://issues.apache.org/jira/browse/HDFS-7714) | Simultaneous restart of HA NameNodes and DataNode can cause DataNode to register successfully with only one NameNode. |  Major | datanode | Chris Nauroth | Vinayakumar B |
| [HADOOP-11512](https://issues.apache.org/jira/browse/HADOOP-11512) | Use getTrimmedStrings when reading serialization keys |  Minor | io | Harsh J | Ryan P |
| [YARN-3090](https://issues.apache.org/jira/browse/YARN-3090) | DeletionService can silently ignore deletion task failures |  Major | nodemanager | Jason Lowe | Varun Saxena |
| [YARN-2809](https://issues.apache.org/jira/browse/YARN-2809) | Implement workaround for linux kernel panic when removing cgroup |  Major | nodemanager | Nathan Roberts | Nathan Roberts |
| [HADOOP-10953](https://issues.apache.org/jira/browse/HADOOP-10953) | NetworkTopology#add calls NetworkTopology#toString without holding the netlock |  Minor | net | Liang Xie | Liang Xie |
| [YARN-2246](https://issues.apache.org/jira/browse/YARN-2246) | Job History Link in RM UI is redirecting to the URL which contains Job Id twice |  Major | webapp | Devaraj K | Devaraj K |
| [HDFS-7753](https://issues.apache.org/jira/browse/HDFS-7753) | Fix Multithreaded correctness Warnings in BackupImage.java |  Major | . | Rakesh R | Konstantin Shvachko |
| [YARN-3160](https://issues.apache.org/jira/browse/YARN-3160) | Non-atomic operation on nodeUpdateQueue in RMNodeImpl |  Major | resourcemanager | Chengbing Liu | Chengbing Liu |
| [YARN-3074](https://issues.apache.org/jira/browse/YARN-3074) | Nodemanager dies when localizer runner tries to write to a full disk |  Major | nodemanager | Jason Lowe | Varun Saxena |
| [YARN-3151](https://issues.apache.org/jira/browse/YARN-3151) | On Failover tracking url wrong in application cli for KILLED application |  Minor | client, resourcemanager | Bibin A Chundatt | Rohith Sharma K S |
| [YARN-1237](https://issues.apache.org/jira/browse/YARN-1237) | Description for yarn.nodemanager.aux-services in yarn-default.xml is misleading |  Minor | documentation | Hitesh Shah | Brahma Reddy Battula |
| [YARN-1580](https://issues.apache.org/jira/browse/YARN-1580) | Documentation error regarding "container-allocation.expiry-interval-ms" |  Trivial | documentation | German Florez-Larrahondo | Brahma Reddy Battula |
| [HDFS-7704](https://issues.apache.org/jira/browse/HDFS-7704) | DN heartbeat to Active NN may be blocked and expire if connection to Standby NN continues to time out. |  Major | datanode, namenode | Rushabh S Shah | Rushabh S Shah |
| [MAPREDUCE-6221](https://issues.apache.org/jira/browse/MAPREDUCE-6221) | Stringifier is left unclosed in Chain#getChainElementConf() |  Minor | . | Ted Yu | Ted Yu |
| [HADOOP-11587](https://issues.apache.org/jira/browse/HADOOP-11587) | TestMapFile#testMainMethodMapFile creates test files in hadoop-common project root |  Trivial | test | Xiaoyu Yao | Xiaoyu Yao |
| [YARN-3104](https://issues.apache.org/jira/browse/YARN-3104) | RM generates new AMRM tokens every heartbeat between rolling and activation |  Critical | resourcemanager | Jason Lowe | Jason Lowe |
| [YARN-3191](https://issues.apache.org/jira/browse/YARN-3191) | Log object should be initialized with its own class |  Trivial | nodemanager | Rohith Sharma K S | Rohith Sharma K S |
| [HADOOP-11497](https://issues.apache.org/jira/browse/HADOOP-11497) | Fix typo in ClusterSetup.html#Hadoop\_Startup |  Major | documentation | Christian Winkler | Christian Winkler |
| [YARN-3164](https://issues.apache.org/jira/browse/YARN-3164) | rmadmin command usage prints incorrect command name |  Minor | resourcemanager | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-2847](https://issues.apache.org/jira/browse/YARN-2847) | Linux native container executor segfaults if default banned user detected |  Major | nodemanager | Jason Lowe | Olaf Flebbe |
| [HADOOP-11467](https://issues.apache.org/jira/browse/HADOOP-11467) | KerberosAuthenticator can connect to a non-secure cluster |  Critical | security | Robert Kanter | Yongjun Zhang |
| [HDFS-7686](https://issues.apache.org/jira/browse/HDFS-7686) | Re-add rapid rescan of possibly corrupt block feature to the block scanner |  Blocker | . | Rushabh S Shah | Colin P. McCabe |
| [HDFS-7778](https://issues.apache.org/jira/browse/HDFS-7778) | Rename FsVolumeListTest to TestFsVolumeList and commit it to branch-2 |  Major | datanode, test | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [YARN-2899](https://issues.apache.org/jira/browse/YARN-2899) | Run TestDockerContainerExecutorWithMocks on Linux only |  Minor | nodemanager, test | Ming Ma | Ming Ma |
| [YARN-2749](https://issues.apache.org/jira/browse/YARN-2749) | Some testcases from TestLogAggregationService fails in trunk |  Major | . | Xuan Gong | Xuan Gong |
| [MAPREDUCE-6225](https://issues.apache.org/jira/browse/MAPREDUCE-6225) | Fix new findbug warnings in hadoop-mapreduce-client-core |  Major | . | Jason Lowe | Varun Saxena |
| [HDFS-7798](https://issues.apache.org/jira/browse/HDFS-7798) | Checkpointing failure caused by shared KerberosAuthenticator |  Critical | security | Chengbing Liu | Chengbing Liu |
| [HADOOP-11000](https://issues.apache.org/jira/browse/HADOOP-11000) | HAServiceProtocol's health state is incorrectly transitioned to SERVICE\_NOT\_RESPONDING |  Major | . | Ming Ma | Ming Ma |
| [HDFS-6662](https://issues.apache.org/jira/browse/HDFS-6662) | WebHDFS cannot open a file if its path contains "%" |  Critical | namenode | Brahma Reddy Battula | Gerson Carlos |
| [HADOOP-11295](https://issues.apache.org/jira/browse/HADOOP-11295) | RPC Server Reader thread can't shutdown if RPCCallQueue is full |  Major | . | Ming Ma | Ming Ma |
| [MAPREDUCE-4286](https://issues.apache.org/jira/browse/MAPREDUCE-4286) | TestClientProtocolProviderImpls passes on failure conditions |  Major | . | Devaraj K | Devaraj K |
| [HADOOP-11545](https://issues.apache.org/jira/browse/HADOOP-11545) | ArrayIndexOutOfBoundsException is thrown with "hadoop credential list -provider" |  Minor | security | Brahma Reddy Battula | Brahma Reddy Battula |
| [MAPREDUCE-6261](https://issues.apache.org/jira/browse/MAPREDUCE-6261) | NullPointerException if MapOutputBuffer.flush invoked twice |  Major | mrv2 | Jason Lowe | Tsuyoshi Ozawa |
| [YARN-1615](https://issues.apache.org/jira/browse/YARN-1615) | Fix typos in description about delay scheduling |  Trivial | documentation, scheduler | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-11595](https://issues.apache.org/jira/browse/HADOOP-11595) | Add default implementation for AbstractFileSystem#truncate |  Major | fs | Yi Liu | Yi Liu |
| [HADOOP-9087](https://issues.apache.org/jira/browse/HADOOP-9087) | Queue size metric for metric sinks isn't actually maintained |  Minor | metrics | Mostafa Elhemali | Akira Ajisaka |
| [YARN-933](https://issues.apache.org/jira/browse/YARN-933) | Potential InvalidStateTransitonException: Invalid event: LAUNCHED at FINAL\_SAVING |  Major | resourcemanager | J.Andreina | Rohith Sharma K S |
| [HDFS-7788](https://issues.apache.org/jira/browse/HDFS-7788) | Post-2.6 namenode may not start up with an image containing inodes created with an old release. |  Blocker | . | Kihwal Lee | Rushabh S Shah |
| [YARN-3194](https://issues.apache.org/jira/browse/YARN-3194) | RM should handle NMContainerStatuses sent by NM while registering if NM is Reconnected node |  Blocker | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [HADOOP-11604](https://issues.apache.org/jira/browse/HADOOP-11604) | Prevent ConcurrentModificationException while closing domain sockets during shutdown of DomainSocketWatcher thread. |  Critical | net | Liang Xie | Chris Nauroth |
| [YARN-3237](https://issues.apache.org/jira/browse/YARN-3237) | AppLogAggregatorImpl fails to log error cause |  Major | . | Rushabh S Shah | Rushabh S Shah |
| [HDFS-7813](https://issues.apache.org/jira/browse/HDFS-7813) | TestDFSHAAdminMiniCluster#testFencer testcase is failing frequently |  Major | ha, test | Rakesh R | Rakesh R |
| [YARN-3238](https://issues.apache.org/jira/browse/YARN-3238) | Connection timeouts to nodemanagers are retried at multiple levels |  Blocker | . | Jason Lowe | Jason Lowe |
| [YARN-3207](https://issues.apache.org/jira/browse/YARN-3207) | secondary filter matches entites which do not have the key being filtered for. |  Major | timelineserver | Prakash Ramachandran | Zhijie Shen |
| [HDFS-7009](https://issues.apache.org/jira/browse/HDFS-7009) | Active NN and standby NN have different live nodes |  Major | datanode | Ming Ma | Ming Ma |
| [HADOOP-8642](https://issues.apache.org/jira/browse/HADOOP-8642) | Document that io.native.lib.available only controls native bz2 and zlib compression codecs |  Major | documentation, native | Eli Collins | Akira Ajisaka |
| [HDFS-7807](https://issues.apache.org/jira/browse/HDFS-7807) | libhdfs htable.c: fix htable resizing, add unit test |  Major | native | Colin P. McCabe | Colin P. McCabe |
| [HDFS-7805](https://issues.apache.org/jira/browse/HDFS-7805) | NameNode recovery prompt should be printed on console |  Major | namenode | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HADOOP-11619](https://issues.apache.org/jira/browse/HADOOP-11619) | FTPFileSystem should override getDefaultPort |  Major | fs | Gera Shegalov | Brahma Reddy Battula |
| [HDFS-7008](https://issues.apache.org/jira/browse/HDFS-7008) | xlator should be closed upon exit from DFSAdmin#genericRefresh() |  Minor | . | Ted Yu | Tsuyoshi Ozawa |
| [HDFS-7831](https://issues.apache.org/jira/browse/HDFS-7831) | Fix the starting index and end condition of the loop in FileDiffList.findEarlierSnapshotBlocks() |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [MAPREDUCE-2815](https://issues.apache.org/jira/browse/MAPREDUCE-2815) | JavaDoc does not generate correctly for MultithreadedMapRunner |  Minor | documentation | Shane Butler | Chris Palmer |
| [HDFS-7763](https://issues.apache.org/jira/browse/HDFS-7763) | fix zkfc hung issue due to not catching exception in a corner case |  Major | ha | Liang Xie | Liang Xie |
| [HADOOP-11480](https://issues.apache.org/jira/browse/HADOOP-11480) | Typo in hadoop-aws/index.md uses wrong scheme for test.fs.s3.name |  Minor | documentation | Ted Yu | Ted Yu |
| [YARN-3239](https://issues.apache.org/jira/browse/YARN-3239) | WebAppProxy does not support a final tracking url which has query fragments and params |  Major | . | Hitesh Shah | Jian He |
| [HADOOP-11629](https://issues.apache.org/jira/browse/HADOOP-11629) | WASB filesystem should not start BandwidthGaugeUpdater if fs.azure.skip.metrics set to true |  Major | tools | shanyu zhao | shanyu zhao |
| [HDFS-7495](https://issues.apache.org/jira/browse/HDFS-7495) | Remove updatePosition argument from DFSInputStream#getBlockAt() |  Minor | . | Ted Yu | Colin P. McCabe |
| [YARN-3256](https://issues.apache.org/jira/browse/YARN-3256) | TestClientToAMTokens#testClientTokenRace is not running against all Schedulers even when using ParameterizedSchedulerTestBase |  Major | . | Anubhav Dhoot | Anubhav Dhoot |
| [HADOOP-9922](https://issues.apache.org/jira/browse/HADOOP-9922) | hadoop windows native build will fail in 32 bit machine |  Major | build, native | Vinayakumar B | Kiran Kumar M R |
| [HDFS-7774](https://issues.apache.org/jira/browse/HDFS-7774) | Unresolved symbols error while compiling HDFS on Windows 7/32 bit |  Critical | build, native | Venkatasubramaniam Ramakrishnan | Kiran Kumar M R |
| [YARN-3255](https://issues.apache.org/jira/browse/YARN-3255) | RM, NM, JobHistoryServer, and WebAppProxyServer's main() should support generic options |  Major | nodemanager, resourcemanager | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-6753](https://issues.apache.org/jira/browse/HDFS-6753) | Initialize checkDisk when DirectoryScanner not able to get files list for scanning |  Major | . | J.Andreina | J.Andreina |
| [HDFS-7769](https://issues.apache.org/jira/browse/HDFS-7769) | TestHDFSCLI create files in hdfs project root dir |  Trivial | test | Tsz Wo Nicholas Sze |  |
| [HADOOP-11634](https://issues.apache.org/jira/browse/HADOOP-11634) | Description of webhdfs' principal/keytab should switch places each other |  Major | documentation | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-11615](https://issues.apache.org/jira/browse/HADOOP-11615) | Update ServiceLevelAuth.md for YARN |  Minor | documentation | Akira Ajisaka | Brahma Reddy Battula |
| [YARN-3281](https://issues.apache.org/jira/browse/YARN-3281) | Add RMStateStore to StateMachine visualization list |  Minor | scripts | Chengbing Liu | Chengbing Liu |
| [HDFS-7785](https://issues.apache.org/jira/browse/HDFS-7785) | Improve diagnostics information for HttpPutFailedException |  Major | namenode | Chengbing Liu | Chengbing Liu |
| [YARN-3270](https://issues.apache.org/jira/browse/YARN-3270) | node label expression not getting set in ApplicationSubmissionContext |  Minor | . | Rohit Agarwal | Rohit Agarwal |
| [HADOOP-11449](https://issues.apache.org/jira/browse/HADOOP-11449) | [JDK8] Cannot build on Windows: error: unexpected end tag: \</ul\> |  Major | build | Alec Taylor | Chris Nauroth |
| [HADOOP-11605](https://issues.apache.org/jira/browse/HADOOP-11605) | FilterFileSystem#create with ChecksumOpt should propagate it to wrapped FS |  Minor | fs | Gera Shegalov | Gera Shegalov |
| [HDFS-7871](https://issues.apache.org/jira/browse/HDFS-7871) | NameNodeEditLogRoller can keep printing "Swallowing exception" message |  Critical | . | Jing Zhao | Jing Zhao |
| [MAPREDUCE-6268](https://issues.apache.org/jira/browse/MAPREDUCE-6268) | Fix typo in Task Attempt API's URL |  Minor | . | Ryu Kobayashi | Ryu Kobayashi |
| [YARN-3222](https://issues.apache.org/jira/browse/YARN-3222) | RMNodeImpl#ReconnectNodeTransition should send scheduler events in sequential order |  Critical | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-7682](https://issues.apache.org/jira/browse/HDFS-7682) | {{DistributedFileSystem#getFileChecksum}} of a snapshotted file includes non-snapshotted content |  Major | . | Charles Lamb | Charles Lamb |
| [HADOOP-11666](https://issues.apache.org/jira/browse/HADOOP-11666) | Revert the format change of du output introduced by HADOOP-6857 |  Major | . | Akira Ajisaka | Byron Wong |
| [HDFS-7869](https://issues.apache.org/jira/browse/HDFS-7869) | Inconsistency in the return information while performing rolling upgrade |  Major | . | J.Andreina | J.Andreina |
| [HDFS-7879](https://issues.apache.org/jira/browse/HDFS-7879) | hdfs.dll does not export functions of the public libhdfs API |  Major | build, libhdfs | Chris Nauroth | Chris Nauroth |
| [YARN-3131](https://issues.apache.org/jira/browse/YARN-3131) | YarnClientImpl should check FAILED and KILLED state in submitApplication |  Major | . | Chang Li | Chang Li |
| [HDFS-1522](https://issues.apache.org/jira/browse/HDFS-1522) | Merge Block.BLOCK\_FILE\_PREFIX and DataStorage.BLOCK\_FILE\_PREFIX into one constant |  Major | datanode | Konstantin Shvachko | Dongming Liang |
| [YARN-3231](https://issues.apache.org/jira/browse/YARN-3231) | FairScheduler: Changing queueMaxRunningApps interferes with pending jobs |  Critical | . | Siqi Li | Siqi Li |
| [YARN-3242](https://issues.apache.org/jira/browse/YARN-3242) | Asynchrony in ZK-close can lead to ZKRMStateStore watcher receiving events for old client |  Critical | resourcemanager | zhihai xu | zhihai xu |
| [MAPREDUCE-6136](https://issues.apache.org/jira/browse/MAPREDUCE-6136) | MRAppMaster doesn't shutdown file systems |  Major | applicationmaster | Noah Watkins | Brahma Reddy Battula |
| [HADOOP-11674](https://issues.apache.org/jira/browse/HADOOP-11674) | oneByteBuf in CryptoInputStream and CryptoOutputStream should be non static |  Critical | io | Sean Busbey | Sean Busbey |
| [HDFS-7885](https://issues.apache.org/jira/browse/HDFS-7885) | Datanode should not trust the generation stamp provided by client |  Critical | datanode | vitthal (Suhas) Gogate | Tsz Wo Nicholas Sze |
| [YARN-3227](https://issues.apache.org/jira/browse/YARN-3227) | Timeline renew delegation token fails when RM user's TGT is expired |  Critical | . | Jonathan Eagles | Zhijie Shen |
| [HDFS-7818](https://issues.apache.org/jira/browse/HDFS-7818) | OffsetParam should return the default value instead of throwing NPE when the value is unspecified |  Blocker | webhdfs | Eric Payne | Eric Payne |
| [YARN-3275](https://issues.apache.org/jira/browse/YARN-3275) | CapacityScheduler: Preemption happening on non-preemptable queues |  Major | . | Eric Payne | Eric Payne |
| [YARN-3296](https://issues.apache.org/jira/browse/YARN-3296) | yarn.nodemanager.container-monitor.process-tree.class is configurable but ResourceCalculatorProcessTree class is marked Private |  Major | . | Hitesh Shah | Hitesh Shah |
| [MAPREDUCE-5657](https://issues.apache.org/jira/browse/MAPREDUCE-5657) | [JDK8] Fix Javadoc errors caused by incorrect or illegal tags in doc comments |  Minor | documentation | Andrew Purtell | Akira Ajisaka |
| [HADOOP-11602](https://issues.apache.org/jira/browse/HADOOP-11602) | Fix toUpperCase/toLowerCase to use Locale.ENGLISH |  Blocker | . | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [HADOOP-11686](https://issues.apache.org/jira/browse/HADOOP-11686) | MiniKDC cannot change ORG\_NAME or ORG\_DOMAIN |  Major | security, test | Duo Zhang | Duo Zhang |
| [YARN-3287](https://issues.apache.org/jira/browse/YARN-3287) | TimelineClient kerberos authentication failure uses wrong login context. |  Major | . | Jonathan Eagles | Daryn Sharp |
| [HADOOP-11571](https://issues.apache.org/jira/browse/HADOOP-11571) | Über-jira: S3a stabilisation phase I |  Blocker | fs/s3 | Steve Loughran | Steve Loughran |
| [MAPREDUCE-6263](https://issues.apache.org/jira/browse/MAPREDUCE-6263) | Configurable timeout between YARNRunner terminate the application and forcefully kill. |  Major | client | Jason Lowe | Eric Payne |
| [MAPREDUCE-4742](https://issues.apache.org/jira/browse/MAPREDUCE-4742) | Fix typo in nnbench#displayUsage |  Trivial | test | Liang Xie | Liang Xie |
| [HADOOP-11618](https://issues.apache.org/jira/browse/HADOOP-11618) | DelegateToFileSystem erroneously uses default FS's port in constructor |  Major | fs | Gera Shegalov | Brahma Reddy Battula |
| [HDFS-7830](https://issues.apache.org/jira/browse/HDFS-7830) | DataNode does not release the volume lock when adding a volume fails. |  Major | datanode | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [YARN-3295](https://issues.apache.org/jira/browse/YARN-3295) | Fix documentation nits found in markdown conversion |  Trivial | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-11693](https://issues.apache.org/jira/browse/HADOOP-11693) | Azure Storage FileSystem rename operations are throttled too aggressively to complete HBase WAL archiving. |  Major | tools | Duo Xu | Duo Xu |
| [HDFS-7880](https://issues.apache.org/jira/browse/HDFS-7880) | Remove the tests for legacy Web UI in branch-2 |  Blocker | test | Akira Ajisaka | Brahma Reddy Battula |
| [YARN-3338](https://issues.apache.org/jira/browse/YARN-3338) | Exclude jline dependency from YARN |  Blocker | build | Zhijie Shen | Zhijie Shen |
| [HDFS-6833](https://issues.apache.org/jira/browse/HDFS-6833) | DirectoryScanner should not register a deleting block with memory of DataNode |  Critical | datanode | Shinichi Yamashita | Shinichi Yamashita |
| [HDFS-7722](https://issues.apache.org/jira/browse/HDFS-7722) | DataNode#checkDiskError should also remove Storage when error is found. |  Major | datanode | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [YARN-3267](https://issues.apache.org/jira/browse/YARN-3267) | Timelineserver applies the ACL rules after applying the limit on the number of records |  Major | . | Prakash Ramachandran | Chang Li |
| [HDFS-7926](https://issues.apache.org/jira/browse/HDFS-7926) | NameNode implementation of ClientProtocol.truncate(..) is not idempotent |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-2605](https://issues.apache.org/jira/browse/HDFS-2605) | CHANGES.txt has two "Release 0.21.1" sections |  Major | documentation | Konstantin Shvachko | Allen Wittenauer |
| [HADOOP-11558](https://issues.apache.org/jira/browse/HADOOP-11558) | Fix dead links to doc of hadoop-tools |  Minor | documentation | Masatake Iwasaki | Jean-Pierre Matsumoto |
| [HDFS-7915](https://issues.apache.org/jira/browse/HDFS-7915) | The DataNode can sometimes allocate a ShortCircuitShm slot and fail to tell the DFSClient about it because of a network error |  Major | . | Colin P. McCabe | Colin P. McCabe |
| [YARN-1453](https://issues.apache.org/jira/browse/YARN-1453) | [JDK8] Fix Javadoc errors caused by incorrect or illegal tags in doc comments |  Minor | . | Andrew Purtell | Akira Ajisaka |
| [HDFS-7886](https://issues.apache.org/jira/browse/HDFS-7886) | TestFileTruncate#testTruncateWithDataNodesRestart runs timeout sometimes |  Minor | test | Yi Liu | Plamen Jeliazkov |
| [HADOOP-11638](https://issues.apache.org/jira/browse/HADOOP-11638) | OpensslSecureRandom.c pthreads\_thread\_id should support FreeBSD and Solaris in addition to Linux |  Major | native | Dmitry Sivachenko | Kiran Kumar M R |
| [HADOOP-11720](https://issues.apache.org/jira/browse/HADOOP-11720) | [JDK8] Fix javadoc errors caused by incorrect or illegal tags in hadoop-tools |  Major | documentation | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-11722](https://issues.apache.org/jira/browse/HADOOP-11722) | Some Instances of Services using ZKDelegationTokenSecretManager go down when old token cannot be deleted |  Major | . | Arun Suresh | Arun Suresh |
| [HDFS-7953](https://issues.apache.org/jira/browse/HDFS-7953) | NN Web UI fails to navigate to paths that contain # |  Minor | namenode | Kanaka Kumar Avvaru | Kanaka Kumar Avvaru |
| [MAPREDUCE-6277](https://issues.apache.org/jira/browse/MAPREDUCE-6277) | Job can post multiple history files if attempt loses connection to the RM |  Major | mr-am | Chang Li | Chang Li |
| [HDFS-7697](https://issues.apache.org/jira/browse/HDFS-7697) | Mark the PB OIV tool as experimental |  Major | . | Haohui Mai | Lei (Eddy) Xu |
| [HDFS-7945](https://issues.apache.org/jira/browse/HDFS-7945) | The WebHdfs system on DN does not honor the length parameter |  Blocker | . | Haohui Mai | Haohui Mai |
| [HDFS-7943](https://issues.apache.org/jira/browse/HDFS-7943) | Append cannot handle the last block with length greater than the preferred block size |  Blocker | . | Jing Zhao | Jing Zhao |
| [HDFS-7929](https://issues.apache.org/jira/browse/HDFS-7929) | inotify unable fetch pre-upgrade edit log segments once upgrade starts |  Major | . | Zhe Zhang | Zhe Zhang |
| [HDFS-7587](https://issues.apache.org/jira/browse/HDFS-7587) | Edit log corruption can happen if append fails with a quota violation |  Blocker | namenode | Kihwal Lee | Jing Zhao |
| [HADOOP-10703](https://issues.apache.org/jira/browse/HADOOP-10703) | HttpServer2 creates multiple authentication filters |  Major | security | Benoy Antony | Benoy Antony |
| [HDFS-7816](https://issues.apache.org/jira/browse/HDFS-7816) | Unable to open webhdfs paths with "+" |  Blocker | webhdfs | Jason Lowe | Haohui Mai |
| [HADOOP-11729](https://issues.apache.org/jira/browse/HADOOP-11729) | Fix link to cgroups doc in site.xml |  Minor | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-7932](https://issues.apache.org/jira/browse/HDFS-7932) | Speed up the shutdown of datanode during rolling upgrade |  Major | . | Kihwal Lee | Kihwal Lee |
| [MAPREDUCE-6275](https://issues.apache.org/jira/browse/MAPREDUCE-6275) | Race condition in FileOutputCommitter v2 for user-specified task output subdirs |  Critical | . | Siqi Li | Gera Shegalov |
| [HDFS-7930](https://issues.apache.org/jira/browse/HDFS-7930) | commitBlockSynchronization() does not remove locations |  Blocker | namenode | Konstantin Shvachko | Yi Liu |
| [YARN-3369](https://issues.apache.org/jira/browse/YARN-3369) | Missing NullPointer check in AppSchedulingInfo causes RM to die |  Blocker | resourcemanager | Giovanni Matteo Fumarola | Brahma Reddy Battula |
| [HDFS-7957](https://issues.apache.org/jira/browse/HDFS-7957) | Truncate should verify quota before making changes |  Critical | namenode | Jing Zhao | Jing Zhao |
| [HDFS-6841](https://issues.apache.org/jira/browse/HDFS-6841) | Use Time.monotonicNow() wherever applicable instead of Time.now() |  Major | . | Vinayakumar B | Vinayakumar B |
| [YARN-3384](https://issues.apache.org/jira/browse/YARN-3384) | TestLogAggregationService.verifyContainerLogs fails after YARN-2777 |  Minor | . | Naganarasimha G R | Naganarasimha G R |
| [HDFS-7942](https://issues.apache.org/jira/browse/HDFS-7942) | NFS: support regexp grouping in nfs.exports.allowed.hosts |  Major | nfs | Brandon Li | Brandon Li |
| [YARN-3336](https://issues.apache.org/jira/browse/YARN-3336) | FileSystem memory leak in DelegationTokenRenewer |  Critical | resourcemanager | zhihai xu | zhihai xu |
| [HDFS-7881](https://issues.apache.org/jira/browse/HDFS-7881) | TestHftpFileSystem#testSeek fails in branch-2 |  Blocker | . | Akira Ajisaka | Brahma Reddy Battula |
| [YARN-3393](https://issues.apache.org/jira/browse/YARN-3393) | Getting application(s) goes wrong when app finishes before starting the attempt |  Critical | timelineserver | Zhijie Shen | Zhijie Shen |
| [HDFS-7884](https://issues.apache.org/jira/browse/HDFS-7884) | NullPointerException in BlockSender |  Blocker | datanode | Tsz Wo Nicholas Sze | Brahma Reddy Battula |
| [HDFS-7960](https://issues.apache.org/jira/browse/HDFS-7960) | The full block report should prune zombie storages even if they're not empty |  Critical | . | Lei (Eddy) Xu | Colin P. McCabe |
| [HDFS-7956](https://issues.apache.org/jira/browse/HDFS-7956) | Improve logging for DatanodeRegistration. |  Major | namenode | Konstantin Shvachko | Plamen Jeliazkov |
| [HADOOP-11609](https://issues.apache.org/jira/browse/HADOOP-11609) | Correct credential commands info in CommandsManual.html#credential |  Major | documentation, security | Brahma Reddy Battula | Varun Saxena |
| [MAPREDUCE-6285](https://issues.apache.org/jira/browse/MAPREDUCE-6285) | ClientServiceDelegate should not retry upon AuthenticationException |  Major | . | Jonathan Eagles | Jonathan Eagles |
| [HDFS-7961](https://issues.apache.org/jira/browse/HDFS-7961) | Trigger full block report after hot swapping disk |  Major | . | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HDFS-7977](https://issues.apache.org/jira/browse/HDFS-7977) | NFS couldn't take percentile intervals |  Major | nfs | Brandon Li | Brandon Li |
| [HADOOP-11014](https://issues.apache.org/jira/browse/HADOOP-11014) | Potential resource leak in JavaKeyStoreProvider due to unclosed stream |  Minor | security | Ted Yu | Tsuyoshi Ozawa |
| [HADOOP-11738](https://issues.apache.org/jira/browse/HADOOP-11738) | Fix a link of Protocol Buffers 2.5 for download in BUILDING.txt |  Major | . | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [HDFS-7963](https://issues.apache.org/jira/browse/HDFS-7963) | Fix expected tracing spans in TestTracing along with HDFS-7054 |  Critical | test | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-11748](https://issues.apache.org/jira/browse/HADOOP-11748) | The secrets of auth cookies should not be specified in configuration in clear text |  Critical | . | Haohui Mai | Li Lu |
| [HADOOP-11691](https://issues.apache.org/jira/browse/HADOOP-11691) | X86 build of libwinutils is broken |  Critical | build, native | Remus Rusanu | Kiran Kumar M R |
| [HADOOP-11639](https://issues.apache.org/jira/browse/HADOOP-11639) | Clean up Windows native code compilation warnings related to Windows Secure Container Executor. |  Major | native | Chris Nauroth | Remus Rusanu |
| [HDFS-7742](https://issues.apache.org/jira/browse/HDFS-7742) | favoring decommissioning node for replication can cause a block to stay underreplicated for long periods |  Major | namenode | Nathan Roberts | Nathan Roberts |
| [HADOOP-11761](https://issues.apache.org/jira/browse/HADOOP-11761) | Fix findbugs warnings in org.apache.hadoop.security.authentication |  Minor | . | Li Lu | Li Lu |
| [HADOOP-11754](https://issues.apache.org/jira/browse/HADOOP-11754) | RM fails to start in non-secure mode due to authentication filter failure |  Blocker | . | Sangjin Lee | Haohui Mai |
| [HDFS-7748](https://issues.apache.org/jira/browse/HDFS-7748) | Separate ECN flags from the Status in the DataTransferPipelineAck |  Blocker | . | Haohui Mai | Anu Engineer |
| [YARN-3304](https://issues.apache.org/jira/browse/YARN-3304) | ResourceCalculatorProcessTree#getCpuUsagePercent default return value is inconsistent with other getters |  Blocker | nodemanager | Junping Du | Junping Du |
| [HADOOP-11787](https://issues.apache.org/jira/browse/HADOOP-11787) | OpensslSecureRandom.c pthread\_threadid\_np usage signature is wrong on 32-bit Mac |  Critical | native | Colin P. McCabe | Kiran Kumar M R |
| [HDFS-8036](https://issues.apache.org/jira/browse/HDFS-8036) | Use snapshot path as source when using snapshot diff report in DistCp |  Major | distcp | Sushmitha Sreenivasan | Jing Zhao |
| [HADOOP-11757](https://issues.apache.org/jira/browse/HADOOP-11757) | NFS gateway should shutdown when it can't start UDP or TCP server |  Major | nfs | Brandon Li | Brandon Li |
| [HDFS-8001](https://issues.apache.org/jira/browse/HDFS-8001) | RpcProgramNfs3 : wrong parsing of dfs.blocksize |  Trivial | nfs | Remi Catherinot | Remi Catherinot |
| [MAPREDUCE-6303](https://issues.apache.org/jira/browse/MAPREDUCE-6303) | Read timeout when retrying a fetch error can be fatal to a reducer |  Blocker | . | Jason Lowe | Jason Lowe |
| [HDFS-7996](https://issues.apache.org/jira/browse/HDFS-7996) | After swapping a volume, BlockReceiver reports ReplicaNotFoundException |  Critical | datanode | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HDFS-8051](https://issues.apache.org/jira/browse/HDFS-8051) | FsVolumeList#addVolume should release volume reference if not put it into BlockScanner. |  Major | datanode | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HADOOP-11776](https://issues.apache.org/jira/browse/HADOOP-11776) | jdiff is broken in Hadoop 2 |  Blocker | . | Li Lu | Li Lu |
| [HDFS-7999](https://issues.apache.org/jira/browse/HDFS-7999) | FsDatasetImpl#createTemporary sometimes holds the FSDatasetImpl lock for a very long time |  Major | . | zhouyingchao | zhouyingchao |
| [HDFS-8072](https://issues.apache.org/jira/browse/HDFS-8072) | Reserved RBW space is not released if client terminates while writing block |  Major | datanode | Arpit Agarwal | Arpit Agarwal |
| [HDFS-8025](https://issues.apache.org/jira/browse/HDFS-8025) | Addendum fix for HDFS-3087 Decomissioning on NN restart can complete without blocks being replicated |  Major | . | Ming Ma | Ming Ma |
| [HADOOP-11815](https://issues.apache.org/jira/browse/HADOOP-11815) | HttpServer2 should destroy SignerSecretProvider when it stops |  Blocker | . | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-8063](https://issues.apache.org/jira/browse/HDFS-8063) | Fix intermittent test failures in TestTracing |  Minor | test | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-3055](https://issues.apache.org/jira/browse/YARN-3055) | The token is not renewed properly if it's shared by jobs (oozie) in DelegationTokenRenewer |  Blocker | security | Yi Liu | Daryn Sharp |
| [HADOOP-11837](https://issues.apache.org/jira/browse/HADOOP-11837) | AuthenticationFilter should destroy SignerSecretProvider in Tomcat deployments |  Blocker | security | Venkat Ranganathan | Bowen Zhang |
| [HDFS-6672](https://issues.apache.org/jira/browse/HDFS-6672) | Regression with hdfs oiv tool |  Minor | . | Lei (Eddy) Xu | Lei (Eddy) Xu |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-1979](https://issues.apache.org/jira/browse/YARN-1979) | TestDirectoryCollection fails when the umask is unusual |  Major | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [HADOOP-11165](https://issues.apache.org/jira/browse/HADOOP-11165) | TestUTF8 fails when run against java 8 |  Minor | test | Ted Yu | Stephen Chu |
| [HDFS-7448](https://issues.apache.org/jira/browse/HDFS-7448) | TestBookKeeperHACheckpoints fails in trunk build |  Minor | . | Ted Yu | Akira Ajisaka |
| [YARN-2930](https://issues.apache.org/jira/browse/YARN-2930) | TestRMRestart#testRMRestartRecoveringNodeLabelManager sometimes fails against Java 7 & 8 |  Minor | . | Ted Yu | Wangda Tan |
| [HDFS-7475](https://issues.apache.org/jira/browse/HDFS-7475) | Make TestLazyPersistFiles#testLazyPersistBlocksAreSaved deterministic |  Major | test | Xiaoyu Yao | Xiaoyu Yao |
| [HADOOP-11358](https://issues.apache.org/jira/browse/HADOOP-11358) | Tests for encryption/decryption with IV calculation overflow |  Major | security, test | Yi Liu | Yi Liu |
| [HADOOP-11125](https://issues.apache.org/jira/browse/HADOOP-11125) | Remove redundant tests in TestOsSecureRandom |  Major | . | Ted Yu | Masanori Oyama |
| [HDFS-7585](https://issues.apache.org/jira/browse/HDFS-7585) | Get TestEnhancedByteBufferAccess working on CPU architectures with page sizes other than 4096 |  Major | test | sam liu | sam liu |
| [YARN-3070](https://issues.apache.org/jira/browse/YARN-3070) | TestRMAdminCLI#testHelp fails for transitionToActive command |  Minor | . | Ted Yu | Junping Du |
| [HADOOP-10668](https://issues.apache.org/jira/browse/HADOOP-10668) | TestZKFailoverControllerStress#testExpireBackAndForth occasionally fails |  Major | test | Ted Yu | Ming Ma |
| [HADOOP-11432](https://issues.apache.org/jira/browse/HADOOP-11432) | Fix SymlinkBaseTest#testCreateLinkUsingPartQualPath2 |  Major | fs | Liang Xie | Liang Xie |
| [YARN-1537](https://issues.apache.org/jira/browse/YARN-1537) | TestLocalResourcesTrackerImpl.testLocalResourceCache often failed |  Major | nodemanager | Hong Shen | Xuan Gong |
| [HDFS-7914](https://issues.apache.org/jira/browse/HDFS-7914) | TestJournalNode#testFailToStartWithBadConfig fails when the default dfs.journalnode.http-address port 8480 is in use |  Major | . | Xiaoyu Yao | Xiaoyu Yao |
| [HADOOP-11535](https://issues.apache.org/jira/browse/HADOOP-11535) | TableMapping related tests failed due to 'successful' resolving of invalid test hostname |  Minor | . | Kai Zheng | Kai Zheng |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-2179](https://issues.apache.org/jira/browse/YARN-2179) | Initial cache manager structure and context |  Major | . | Chris Trezzo | Chris Trezzo |
| [HDFS-7055](https://issues.apache.org/jira/browse/HDFS-7055) | Add tracing to DFSInputStream |  Major | datanode, namenode | Colin P. McCabe | Colin P. McCabe |
| [YARN-2180](https://issues.apache.org/jira/browse/YARN-2180) | In-memory backing store for cache manager |  Major | . | Chris Trezzo | Chris Trezzo |
| [HDFS-7209](https://issues.apache.org/jira/browse/HDFS-7209) | Populate EDEK cache when creating encryption zone |  Major | encryption, performance | Yi Liu | Yi Liu |
| [HDFS-7254](https://issues.apache.org/jira/browse/HDFS-7254) | Add documentation for hot swaping DataNode drives |  Major | datanode | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HDFS-6877](https://issues.apache.org/jira/browse/HDFS-6877) | Avoid calling checkDisk when an HDFS volume is removed during a write. |  Major | datanode | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HDFS-6824](https://issues.apache.org/jira/browse/HDFS-6824) | Additional user documentation for HDFS encryption. |  Minor | documentation | Andrew Wang | Andrew Wang |
| [YARN-2183](https://issues.apache.org/jira/browse/YARN-2183) | Cleaner service for cache manager |  Major | . | Chris Trezzo | Chris Trezzo |
| [HDFS-5928](https://issues.apache.org/jira/browse/HDFS-5928) | show namespace and namenode ID on NN dfshealth page |  Major | . | Siqi Li | Siqi Li |
| [YARN-2712](https://issues.apache.org/jira/browse/YARN-2712) | TestWorkPreservingRMRestart: Augment FS tests with queue and headroom checks |  Major | resourcemanager | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [HDFS-7035](https://issues.apache.org/jira/browse/HDFS-7035) | Make adding a new data directory to the DataNode an atomic operation and improve error handling |  Major | datanode | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [YARN-2186](https://issues.apache.org/jira/browse/YARN-2186) | Node Manager uploader service for cache manager |  Major | . | Chris Trezzo | Chris Trezzo |
| [YARN-2236](https://issues.apache.org/jira/browse/YARN-2236) | Shared Cache uploader service on the Node Manager |  Major | . | Chris Trezzo | Chris Trezzo |
| [YARN-2766](https://issues.apache.org/jira/browse/YARN-2766) |  ApplicationHistoryManager is expected to return a sorted list of apps/attempts/containers |  Major | timelineserver | Robert Kanter | Robert Kanter |
| [YARN-2690](https://issues.apache.org/jira/browse/YARN-2690) | Make ReservationSystem and its dependent classes independent of Scheduler type |  Major | fairscheduler | Anubhav Dhoot | Anubhav Dhoot |
| [HDFS-7413](https://issues.apache.org/jira/browse/HDFS-7413) | Some unit tests should use NameNodeProtocols instead of FSNameSystem |  Major | test | Haohui Mai | Haohui Mai |
| [HDFS-7415](https://issues.apache.org/jira/browse/HDFS-7415) | Move FSNameSystem.resolvePath() to FSDirectory |  Major | . | Haohui Mai | Haohui Mai |
| [YARN-2375](https://issues.apache.org/jira/browse/YARN-2375) | Allow enabling/disabling timeline server per framework |  Major | . | Jonathan Eagles | Mit Desai |
| [HDFS-7420](https://issues.apache.org/jira/browse/HDFS-7420) | Delegate permission checks to FSDirectory |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-7412](https://issues.apache.org/jira/browse/HDFS-7412) | Move RetryCache to NameNodeRpcServer |  Major | . | Haohui Mai | Haohui Mai |
| [YARN-1984](https://issues.apache.org/jira/browse/YARN-1984) | LeveldbTimelineStore does not handle db exceptions properly |  Major | . | Jason Lowe | Varun Saxena |
| [YARN-2404](https://issues.apache.org/jira/browse/YARN-2404) | Remove ApplicationAttemptState and ApplicationState class in RMStateStore class |  Major | . | Jian He | Tsuyoshi Ozawa |
| [YARN-2188](https://issues.apache.org/jira/browse/YARN-2188) | Client service for cache manager |  Major | . | Chris Trezzo | Chris Trezzo |
| [HDFS-7440](https://issues.apache.org/jira/browse/HDFS-7440) | Consolidate snapshot related operations in a single class |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-6803](https://issues.apache.org/jira/browse/HDFS-6803) | Documenting DFSClient#DFSInputStream expectations reading and preading in concurrent context |  Major | hdfs-client | stack | stack |
| [HDFS-7310](https://issues.apache.org/jira/browse/HDFS-7310) | Mover can give first priority to local DN if it has target storage type available in local DN |  Major | balancer & mover | Uma Maheswara Rao G | Vinayakumar B |
| [YARN-2165](https://issues.apache.org/jira/browse/YARN-2165) | Timeline server should validate the numeric configuration values |  Major | timelineserver | Karam Singh | Vasanth kumar RJ |
| [YARN-2765](https://issues.apache.org/jira/browse/YARN-2765) | Add leveldb-based implementation for RMStateStore |  Major | resourcemanager | Jason Lowe | Jason Lowe |
| [HDFS-7450](https://issues.apache.org/jira/browse/HDFS-7450) | Consolidate the implementation of GetFileInfo(), GetListings() and GetContentSummary() into a single class |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-7438](https://issues.apache.org/jira/browse/HDFS-7438) | Consolidate the implementation of rename() into a single class |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-7462](https://issues.apache.org/jira/browse/HDFS-7462) | Consolidate implementation of mkdirs() into a single class |  Major | . | Haohui Mai | Haohui Mai |
| [YARN-2880](https://issues.apache.org/jira/browse/YARN-2880) | Add a test in TestRMRestart to make sure node labels will be recovered if it is enabled |  Major | resourcemanager | Wangda Tan | Rohith Sharma K S |
| [HDFS-7468](https://issues.apache.org/jira/browse/HDFS-7468) | Moving verify\* functions to corresponding classes |  Major | . | Li Lu | Li Lu |
| [YARN-2056](https://issues.apache.org/jira/browse/YARN-2056) | Disable preemption at Queue level |  Major | resourcemanager | Mayank Bansal | Eric Payne |
| [HDFS-7474](https://issues.apache.org/jira/browse/HDFS-7474) | Avoid resolving path in FSPermissionChecker |  Major | namenode | Jing Zhao | Jing Zhao |
| [HDFS-7459](https://issues.apache.org/jira/browse/HDFS-7459) | Consolidate cache-related implementation in FSNamesystem into a single class |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-7476](https://issues.apache.org/jira/browse/HDFS-7476) | Consolidate ACL-related operations to a single class |  Major | namenode | Haohui Mai | Haohui Mai |
| [YARN-2927](https://issues.apache.org/jira/browse/YARN-2927) | InMemorySCMStore properties are inconsistent |  Major | . | Ray Chiang | Ray Chiang |
| [HDFS-7486](https://issues.apache.org/jira/browse/HDFS-7486) | Consolidate XAttr-related implementation into a single class |  Major | . | Haohui Mai | Haohui Mai |
| [HADOOP-10476](https://issues.apache.org/jira/browse/HADOOP-10476) | Bumping the findbugs version to 3.0.0 |  Major | build | Haohui Mai | Haohui Mai |
| [HADOOP-11367](https://issues.apache.org/jira/browse/HADOOP-11367) | Fix warnings from findbugs 3.0 in hadoop-streaming |  Major | . | Li Lu | Li Lu |
| [HADOOP-11369](https://issues.apache.org/jira/browse/HADOOP-11369) | Fix new findbugs warnings in hadoop-mapreduce-client, non-core directories |  Major | . | Li Lu | Li Lu |
| [HADOOP-11372](https://issues.apache.org/jira/browse/HADOOP-11372) | Fix new findbugs warnings in mapreduce-examples |  Major | . | Li Lu | Li Lu |
| [HDFS-7498](https://issues.apache.org/jira/browse/HDFS-7498) | Simplify the logic in INodesInPath |  Major | namenode | Jing Zhao | Jing Zhao |
| [HADOOP-11379](https://issues.apache.org/jira/browse/HADOOP-11379) | Fix new findbugs warnings in hadoop-auth\* |  Major | . | Li Lu | Li Lu |
| [HADOOP-11378](https://issues.apache.org/jira/browse/HADOOP-11378) | Fix new findbugs warnings in hadoop-kms |  Major | . | Li Lu | Li Lu |
| [YARN-2924](https://issues.apache.org/jira/browse/YARN-2924) | Node to labels mapping should not transfer to lowercase when adding from RMAdminCLI |  Major | client | Wangda Tan | Wangda Tan |
| [HADOOP-11381](https://issues.apache.org/jira/browse/HADOOP-11381) | Fix findbugs warnings in hadoop-distcp, hadoop-aws, hadoop-azure, and hadoop-openstack |  Major | . | Li Lu | Li Lu |
| [HADOOP-10482](https://issues.apache.org/jira/browse/HADOOP-10482) | Fix various findbugs warnings in hadoop-common |  Major | . | Haohui Mai | Haohui Mai |
| [HADOOP-11389](https://issues.apache.org/jira/browse/HADOOP-11389) | Clean up byte to string encoding issues in hadoop-common |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-7509](https://issues.apache.org/jira/browse/HDFS-7509) | Avoid resolving path multiple times |  Major | . | Jing Zhao | Jing Zhao |
| [HDFS-7506](https://issues.apache.org/jira/browse/HDFS-7506) | Consolidate implementation of setting inode attributes into a single class |  Major | . | Haohui Mai | Haohui Mai |
| [YARN-2914](https://issues.apache.org/jira/browse/YARN-2914) | Potential race condition in Singleton implementation of SharedCacheUploaderMetrics, CleanerMetrics, ClientSCMMetrics |  Minor | . | Ted Yu | Varun Saxena |
| [YARN-2762](https://issues.apache.org/jira/browse/YARN-2762) | RMAdminCLI node-labels-related args should be trimmed and checked before sending to RM |  Minor | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [YARN-2189](https://issues.apache.org/jira/browse/YARN-2189) | Admin service for cache manager |  Major | . | Chris Trezzo | Chris Trezzo |
| [HDFS-7528](https://issues.apache.org/jira/browse/HDFS-7528) | Consolidate symlink-related implementation into a single class |  Major | . | Haohui Mai | Haohui Mai |
| [YARN-2944](https://issues.apache.org/jira/browse/YARN-2944) | InMemorySCMStore can not be instantiated with ReflectionUtils#newInstance |  Minor | . | Chris Trezzo | Chris Trezzo |
| [YARN-2203](https://issues.apache.org/jira/browse/YARN-2203) | Web UI for cache manager |  Major | . | Chris Trezzo | Chris Trezzo |
| [HDFS-7543](https://issues.apache.org/jira/browse/HDFS-7543) | Avoid path resolution when getting FileStatus for audit logs |  Major | . | Haohui Mai | Haohui Mai |
| [YARN-2738](https://issues.apache.org/jira/browse/YARN-2738) | Add FairReservationSystem for FairScheduler |  Major | fairscheduler | Anubhav Dhoot | Anubhav Dhoot |
| [HADOOP-11429](https://issues.apache.org/jira/browse/HADOOP-11429) | Findbugs warnings in hadoop extras |  Major | . | Varun Saxena | Varun Saxena |
| [YARN-2920](https://issues.apache.org/jira/browse/YARN-2920) | CapacityScheduler should be notified when labels on nodes changed |  Major | . | Wangda Tan | Wangda Tan |
| [YARN-2970](https://issues.apache.org/jira/browse/YARN-2970) | NodeLabel operations in RMAdmin CLI get missing in help command. |  Minor | api, client, resourcemanager | Junping Du | Varun Saxena |
| [HADOOP-11370](https://issues.apache.org/jira/browse/HADOOP-11370) | Fix new findbug warnings hadoop-yarn |  Major | . | Zhijie Shen |  |
| [YARN-2943](https://issues.apache.org/jira/browse/YARN-2943) | Add a node-labels page in RM web UI |  Major | resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-2998](https://issues.apache.org/jira/browse/YARN-2998) | Abstract out scheduler independent PlanFollower components |  Major | fairscheduler | Anubhav Dhoot | Anubhav Dhoot |
| [YARN-2881](https://issues.apache.org/jira/browse/YARN-2881) | Implement PlanFollower for FairScheduler |  Major | fairscheduler | Anubhav Dhoot | Anubhav Dhoot |
| [HADOOP-10651](https://issues.apache.org/jira/browse/HADOOP-10651) | Add ability to restrict service access using IP addresses and hostnames |  Major | security | Benoy Antony | Benoy Antony |
| [HDFS-7589](https://issues.apache.org/jira/browse/HDFS-7589) | Break the dependency between libnative\_mini\_dfs and libhdfs |  Major | libhdfs | Zhanwei Wang | Zhanwei Wang |
| [YARN-3014](https://issues.apache.org/jira/browse/YARN-3014) | Replaces labels on a host should update all NM's labels on that host |  Major | . | Wangda Tan | Wangda Tan |
| [HADOOP-11465](https://issues.apache.org/jira/browse/HADOOP-11465) | Fix findbugs warnings in hadoop-gridmix |  Major | . | Varun Saxena | Varun Saxena |
| [HDFS-7056](https://issues.apache.org/jira/browse/HDFS-7056) | Snapshot support for truncate |  Major | namenode | Konstantin Shvachko | Plamen Jeliazkov |
| [YARN-3019](https://issues.apache.org/jira/browse/YARN-3019) | Make work-preserving-recovery the default mechanism for RM recovery |  Major | resourcemanager | Jian He | Jian He |
| [YARN-2807](https://issues.apache.org/jira/browse/YARN-2807) | Option "--forceactive" not works as described in usage of "yarn rmadmin -transitionToActive" |  Minor | documentation, resourcemanager | Wangda Tan | Masatake Iwasaki |
| [HDFS-5782](https://issues.apache.org/jira/browse/HDFS-5782) | BlockListAsLongs should take lists of Replicas rather than concrete classes |  Minor | datanode | David Powell | Joe Pallas |
| [YARN-2217](https://issues.apache.org/jira/browse/YARN-2217) | Shared cache client side changes |  Major | . | Chris Trezzo | Chris Trezzo |
| [HDFS-7189](https://issues.apache.org/jira/browse/HDFS-7189) | Add trace spans for DFSClient metadata operations |  Major | datanode, namenode | Colin P. McCabe | Colin P. McCabe |
| [YARN-2984](https://issues.apache.org/jira/browse/YARN-2984) | Metrics for container's actual memory usage |  Major | nodemanager | Karthik Kambatla | Karthik Kambatla |
| [HDFS-7573](https://issues.apache.org/jira/browse/HDFS-7573) | Consolidate the implementation of delete() into a single class |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-5631](https://issues.apache.org/jira/browse/HDFS-5631) | Expose interfaces required by FsDatasetSpi implementations |  Minor | datanode | David Powell | Joe Pallas |
| [YARN-2933](https://issues.apache.org/jira/browse/YARN-2933) | Capacity Scheduler preemption policy should only consider capacity without labels temporarily |  Major | capacityscheduler | Wangda Tan | Mayank Bansal |
| [HDFS-7638](https://issues.apache.org/jira/browse/HDFS-7638) | Small fix and few refinements for FSN#truncate |  Major | datanode, namenode | Yi Liu | Yi Liu |
| [HDFS-7634](https://issues.apache.org/jira/browse/HDFS-7634) | Disallow truncation of Lazy persist files |  Major | datanode, namenode | Yi Liu | Yi Liu |
| [HDFS-7623](https://issues.apache.org/jira/browse/HDFS-7623) | Add htrace configuration properties to core-default.xml and update user doc about how to enable htrace |  Major | . | Yi Liu | Yi Liu |
| [HDFS-7643](https://issues.apache.org/jira/browse/HDFS-7643) | Test case to ensure lazy persist files cannot be truncated |  Major | test | Arpit Agarwal | Yi Liu |
| [YARN-2800](https://issues.apache.org/jira/browse/YARN-2800) | Remove MemoryNodeLabelsStore and add a way to enable/disable node labels feature |  Major | client, resourcemanager | Wangda Tan | Wangda Tan |
| [HDFS-7659](https://issues.apache.org/jira/browse/HDFS-7659) | We should check the new length of truncate can't be a negative value. |  Major | datanode, namenode | Yi Liu | Yi Liu |
| [HDFS-7676](https://issues.apache.org/jira/browse/HDFS-7676) | Fix TestFileTruncate to avoid bug of HDFS-7611 |  Major | test | Konstantin Shvachko | Konstantin Shvachko |
| [YARN-3092](https://issues.apache.org/jira/browse/YARN-3092) | Create common ResourceUsage class to track labeled resource usages in Capacity Scheduler |  Major | api, client, resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-3011](https://issues.apache.org/jira/browse/YARN-3011) | NM dies because of the failure of resource localization |  Major | nodemanager | Wang Hao | Varun Saxena |
| [YARN-3028](https://issues.apache.org/jira/browse/YARN-3028) | Better syntax for replaceLabelsOnNode in RMAdmin CLI |  Major | api, client, resourcemanager | Jian He | Rohith Sharma K S |
| [HDFS-7677](https://issues.apache.org/jira/browse/HDFS-7677) | DistributedFileSystem#truncate should resolve symlinks |  Major | datanode, namenode | Yi Liu | Yi Liu |
| [HADOOP-11317](https://issues.apache.org/jira/browse/HADOOP-11317) | Increment SLF4J version to 1.7.10 |  Major | build | Steve Loughran | Tim Robertson |
| [HDFS-6673](https://issues.apache.org/jira/browse/HDFS-6673) | Add delimited format support to PB OIV tool |  Minor | . | Lei (Eddy) Xu | Lei (Eddy) Xu |
| [HDFS-7681](https://issues.apache.org/jira/browse/HDFS-7681) | Fix ReplicaInputStream constructor to take InputStreams |  Major | datanode | Joe Pallas | Joe Pallas |
| [HADOOP-10574](https://issues.apache.org/jira/browse/HADOOP-10574) | Bump the maven plugin versions too -moving the numbers into properties |  Major | build | Steve Loughran | Akira Ajisaka |
| [YARN-3099](https://issues.apache.org/jira/browse/YARN-3099) | Capacity Scheduler LeafQueue/ParentQueue should use ResourceUsage to track used-resources-by-label. |  Major | api, client, resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-3098](https://issues.apache.org/jira/browse/YARN-3098) | Create common QueueCapacities class in Capacity Scheduler to track capacities-by-labels of queues |  Major | capacityscheduler | Wangda Tan | Wangda Tan |
| [YARN-3075](https://issues.apache.org/jira/browse/YARN-3075) | NodeLabelsManager implementation to retrieve label to node mapping |  Major | resourcemanager | Varun Saxena | Varun Saxena |
| [YARN-2543](https://issues.apache.org/jira/browse/YARN-2543) | Resource usage should be published to the timeline server as well |  Major | timelineserver | Zhijie Shen | Naganarasimha G R |
| [YARN-1723](https://issues.apache.org/jira/browse/YARN-1723) | AMRMClientAsync missing blacklist addition and removal functionality |  Major | . | Bikas Saha | Bartosz Ługowski |
| [HDFS-7655](https://issues.apache.org/jira/browse/HDFS-7655) | Expose truncate API for Web HDFS |  Major | datanode, namenode | Yi Liu | Yi Liu |
| [YARN-1904](https://issues.apache.org/jira/browse/YARN-1904) | Uniform the XXXXNotFound messages from ClientRMService and ApplicationHistoryClientService |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-2694](https://issues.apache.org/jira/browse/YARN-2694) | Ensure only single node labels specified in resource request / host, and node label expression only specified when resourceName=ANY |  Major | capacityscheduler, resourcemanager | Wangda Tan | Wangda Tan |
| [HDFS-7720](https://issues.apache.org/jira/browse/HDFS-7720) | Quota by Storage Type API, tools and ClientNameNode Protocol changes |  Major | datanode, namenode | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-7738](https://issues.apache.org/jira/browse/HDFS-7738) | Add more tests for truncate |  Minor | test | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [YARN-2971](https://issues.apache.org/jira/browse/YARN-2971) | RM uses conf instead of token service address to renew timeline delegation tokens |  Major | timelineserver | Jonathan Eagles | Jonathan Eagles |
| [HDFS-7058](https://issues.apache.org/jira/browse/HDFS-7058) | Tests for truncate CLI |  Major | test | Konstantin Shvachko | Dasha Boudnik |
| [YARN-2616](https://issues.apache.org/jira/browse/YARN-2616) | Add CLI client to the registry to list, view and manipulate entries |  Major | client | Steve Loughran | Akshay Radia |
| [YARN-2683](https://issues.apache.org/jira/browse/YARN-2683) | registry config options: document and move to core-default |  Major | api, resourcemanager | Steve Loughran | Steve Loughran |
| [HDFS-7760](https://issues.apache.org/jira/browse/HDFS-7760) | Document truncate for WebHDFS. |  Minor | documentation | Yi Liu | Konstantin Shvachko |
| [HDFS-7723](https://issues.apache.org/jira/browse/HDFS-7723) | Quota By Storage Type namenode implemenation |  Major | namenode | Xiaoyu Yao | Xiaoyu Yao |
| [YARN-2079](https://issues.apache.org/jira/browse/YARN-2079) | Recover NonAggregatingLogHandler state upon nodemanager restart |  Major | nodemanager | Jason Lowe | Jason Lowe |
| [YARN-3124](https://issues.apache.org/jira/browse/YARN-3124) | Capacity Scheduler LeafQueue/ParentQueue should use QueueCapacities to track capacities-by-label |  Major | api, client, resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-2994](https://issues.apache.org/jira/browse/YARN-2994) | Document work-preserving RM restart |  Major | resourcemanager | Jian He | Jian He |
| [HDFS-7776](https://issues.apache.org/jira/browse/HDFS-7776) | Adding additional unit tests for Quota By Storage Type |  Major | test | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-4265](https://issues.apache.org/jira/browse/HDFS-4265) | BKJM doesn't take advantage of speculative reads |  Major | ha | Ivan Kelly | Rakesh R |
| [HDFS-7775](https://issues.apache.org/jira/browse/HDFS-7775) | Use consistent naming for NN-internal quota related types and functions |  Minor | datanode, namenode | Xiaoyu Yao | Xiaoyu Yao |
| [HADOOP-11600](https://issues.apache.org/jira/browse/HADOOP-11600) | Fix up source codes to be compiled with Guava 17.0 |  Major | . | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [HDFS-4266](https://issues.apache.org/jira/browse/HDFS-4266) | BKJM: Separate write and ack quorum |  Major | ha | Ivan Kelly | Rakesh R |
| [HADOOP-11570](https://issues.apache.org/jira/browse/HADOOP-11570) | S3AInputStream.close() downloads the remaining bytes of the object from S3 |  Major | fs/s3 | Dan Hecht | Dan Hecht |
| [HADOOP-11522](https://issues.apache.org/jira/browse/HADOOP-11522) | Update S3A Documentation |  Major | fs/s3 | Thomas Demoor | Thomas Demoor |
| [HADOOP-11521](https://issues.apache.org/jira/browse/HADOOP-11521) | Make connection timeout configurable in s3a |  Minor | fs/s3 | Thomas Demoor | Thomas Demoor |
| [YARN-3132](https://issues.apache.org/jira/browse/YARN-3132) | RMNodeLabelsManager should remove node from node-to-label mapping when node becomes deactivated |  Major | api, client, resourcemanager | Wangda Tan | Wangda Tan |
| [YARN-1514](https://issues.apache.org/jira/browse/YARN-1514) | Utility to benchmark ZKRMStateStore#loadState for ResourceManager-HA |  Major | . | Tsuyoshi Ozawa | Tsuyoshi Ozawa |
| [HDFS-7656](https://issues.apache.org/jira/browse/HDFS-7656) | Expose truncate API for HDFS httpfs |  Major | datanode, namenode | Yi Liu | Yi Liu |
| [YARN-3076](https://issues.apache.org/jira/browse/YARN-3076) | Add API/Implementation to YarnClient to retrieve label-to-node mapping |  Major | client | Varun Saxena | Varun Saxena |
| [HDFS-7814](https://issues.apache.org/jira/browse/HDFS-7814) | Fix usage string of storageType parameter for "dfsadmin -setSpaceQuota/clrSpaceQuota" |  Minor | datanode, namenode | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-7740](https://issues.apache.org/jira/browse/HDFS-7740) | Test truncate with DataNodes restarting |  Major | test | Konstantin Shvachko | Yi Liu |
| [HADOOP-11584](https://issues.apache.org/jira/browse/HADOOP-11584) | s3a file block size set to 0 in getFileStatus |  Blocker | fs/s3 | Dan Hecht | Brahma Reddy Battula |
| [HDFS-7806](https://issues.apache.org/jira/browse/HDFS-7806) | Refactor: move StorageType from hadoop-hdfs to hadoop-common |  Minor | datanode, namenode | Xiaoyu Yao | Xiaoyu Yao |
| [HADOOP-10478](https://issues.apache.org/jira/browse/HADOOP-10478) | Fix new findbugs warnings in hadoop-maven-plugins |  Major | . | Haohui Mai | Li Lu |
| [HDFS-7467](https://issues.apache.org/jira/browse/HDFS-7467) | Provide storage tier information for a directory via fsck |  Major | balancer & mover | Benoy Antony | Benoy Antony |
| [HDFS-7843](https://issues.apache.org/jira/browse/HDFS-7843) | A truncated file is corrupted after rollback from a rolling upgrade |  Blocker | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-7819](https://issues.apache.org/jira/browse/HDFS-7819) | Log WARN message for the blocks which are not in Block ID based layout |  Major | datanode | Rakesh R | Rakesh R |
| [YARN-3265](https://issues.apache.org/jira/browse/YARN-3265) | CapacityScheduler deadlock when computing absolute max avail capacity (fix for trunk/branch-2) |  Blocker | capacityscheduler, resourcemanager | Wangda Tan | Wangda Tan |
| [HADOOP-11183](https://issues.apache.org/jira/browse/HADOOP-11183) | Memory-based S3AOutputstream |  Major | fs/s3 | Thomas Demoor | Thomas Demoor |
| [YARN-3122](https://issues.apache.org/jira/browse/YARN-3122) | Metrics for container's actual CPU usage |  Major | nodemanager | Anubhav Dhoot | Anubhav Dhoot |
| [HDFS-7746](https://issues.apache.org/jira/browse/HDFS-7746) | Add a test randomly mixing append, truncate and snapshot |  Minor | test | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [YARN-2786](https://issues.apache.org/jira/browse/YARN-2786) | Create yarn cluster CLI to enable list node labels collection |  Major | api, client, resourcemanager | Wangda Tan | Wangda Tan |
| [HDFS-7855](https://issues.apache.org/jira/browse/HDFS-7855) | Separate class Packet from DFSOutputStream |  Major | hdfs-client | Li Bo | Li Bo |
| [YARN-1809](https://issues.apache.org/jira/browse/YARN-1809) | Synchronize RM and Generic History Service Web-UIs |  Major | . | Zhijie Shen | Xuan Gong |
| [HADOOP-11670](https://issues.apache.org/jira/browse/HADOOP-11670) | Regression: s3a auth setup broken |  Blocker | fs/s3 | Adam Budde | Adam Budde |
| [YARN-3300](https://issues.apache.org/jira/browse/YARN-3300) | outstanding\_resource\_requests table should not be shown in AHS |  Major | resourcemanager | Xuan Gong | Xuan Gong |
| [YARN-1884](https://issues.apache.org/jira/browse/YARN-1884) | ContainerReport should have nodeHttpAddress |  Major | . | Zhijie Shen | Xuan Gong |
| [HADOOP-11710](https://issues.apache.org/jira/browse/HADOOP-11710) | Make CryptoOutputStream behave like DFSOutputStream wrt synchronization |  Critical | fs | Sean Busbey | Sean Busbey |
| [HDFS-7903](https://issues.apache.org/jira/browse/HDFS-7903) | Cannot recover block after truncate and delete snapshot |  Blocker | datanode, namenode | Tsz Wo Nicholas Sze | Plamen Jeliazkov |
| [YARN-3171](https://issues.apache.org/jira/browse/YARN-3171) | Sort by Application id, AppAttempt & ContainerID doesn't work in ATS / RM web ui |  Minor | timelineserver | Jeff Zhang | Naganarasimha G R |
| [HDFS-7838](https://issues.apache.org/jira/browse/HDFS-7838) | Expose truncate API for libhdfs |  Major | datanode, namenode | Yi Liu | Yi Liu |
| [HDFS-7940](https://issues.apache.org/jira/browse/HDFS-7940) | Add tracing to DFSClient#setQuotaByStorageType |  Major | hdfs-client | Rakesh R | Rakesh R |
| [HDFS-7946](https://issues.apache.org/jira/browse/HDFS-7946) | TestDataNodeVolumeFailureReporting NPE on Windows |  Major | test | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-7948](https://issues.apache.org/jira/browse/HDFS-7948) | TestDataNodeHotSwapVolumes#testAddVolumeFailures failed on Windows |  Major | test | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-7950](https://issues.apache.org/jira/browse/HDFS-7950) | Fix TestFsDatasetImpl#testAddVolumes failure on Windows |  Major | test | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-7951](https://issues.apache.org/jira/browse/HDFS-7951) | Fix NPE for TestFsDatasetImpl#testAddVolumeFailureReleasesInUseLock on Linux |  Major | test | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-7054](https://issues.apache.org/jira/browse/HDFS-7054) | Make DFSOutputStream tracing more fine-grained |  Major | datanode, namenode | Colin P. McCabe | Colin P. McCabe |
| [YARN-3379](https://issues.apache.org/jira/browse/YARN-3379) | Missing data in localityTable and ResourceRequests table in RM WebUI |  Major | resourcemanager, webapp, yarn | Xuan Gong | Xuan Gong |
| [HDFS-7824](https://issues.apache.org/jira/browse/HDFS-7824) | GetContentSummary API and its namenode implementation for Storage Type Quota/Usage |  Major | datanode, namenode | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-7700](https://issues.apache.org/jira/browse/HDFS-7700) | Document quota support for storage types |  Major | documentation | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-7954](https://issues.apache.org/jira/browse/HDFS-7954) | TestBalancer#testBalancerWithPinnedBlocks failed on Windows |  Minor | test | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-7811](https://issues.apache.org/jira/browse/HDFS-7811) | Avoid recursive call getStoragePolicyID in INodeFile#computeQuotaUsage |  Minor | namenode | Xiaoyu Yao | Xiaoyu Yao |
| [HDFS-8039](https://issues.apache.org/jira/browse/HDFS-8039) | Fix TestDebugAdmin#testRecoverLease and testVerfiyBlockChecksumCommand on Windows |  Minor | test | Xiaoyu Yao | Xiaoyu Yao |
| [HADOOP-11377](https://issues.apache.org/jira/browse/HADOOP-11377) | jdiff failing on java 7 and java 8, "Null.java" not found |  Major | build | Steve Loughran | Tsuyoshi Ozawa |
| [YARN-3430](https://issues.apache.org/jira/browse/YARN-3430) | RMAppAttempt headroom data is missing in RM Web UI |  Blocker | resourcemanager, webapp, yarn | Xuan Gong | Xuan Gong |
| [HDFS-8038](https://issues.apache.org/jira/browse/HDFS-8038) | PBImageDelimitedTextWriter#getEntry output HDFS path in platform-specific format. |  Minor | tools | Xiaoyu Yao | Xiaoyu Yao |
| [HADOOP-11796](https://issues.apache.org/jira/browse/HADOOP-11796) | Skip TestShellBasedIdMapping.testStaticMapUpdate on Windows |  Minor | test | Xiaoyu Yao | Xiaoyu Yao |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-2486](https://issues.apache.org/jira/browse/HDFS-2486) | Review issues with UnderReplicatedBlocks |  Minor | namenode | Steve Loughran | Uma Maheswara Rao G |
| [MAPREDUCE-5420](https://issues.apache.org/jira/browse/MAPREDUCE-5420) | Remove mapreduce.task.tmp.dir from mapred-default.xml |  Major | . | Sandy Ryza | James Carman |
| [YARN-2949](https://issues.apache.org/jira/browse/YARN-2949) | Add documentation for CGroups |  Major | documentation, nodemanager | Varun Vasudev | Varun Vasudev |
| [HADOOP-11489](https://issues.apache.org/jira/browse/HADOOP-11489) | Dropping dependency on io.netty from hadoop-nfs' pom.xml |  Minor | nfs | Ted Yu | Ted Yu |
| [HADOOP-11463](https://issues.apache.org/jira/browse/HADOOP-11463) | Replace method-local TransferManager object with S3AFileSystem#transfers |  Major | fs/s3 | Ted Yu | Ted Yu |
| [HADOOP-11612](https://issues.apache.org/jira/browse/HADOOP-11612) | Workaround for Curator's ChildReaper requiring Guava 15+ |  Major | . | Robert Kanter | Robert Kanter |
| [MAPREDUCE-6264](https://issues.apache.org/jira/browse/MAPREDUCE-6264) | Remove httpclient dependency from hadoop-mapreduce-client |  Major | . | Akira Ajisaka | Brahma Reddy Battula |
| [HADOOP-9329](https://issues.apache.org/jira/browse/HADOOP-9329) | document native build dependencies in BUILDING.txt |  Trivial | documentation | Colin P. McCabe | Vijay Bhat |
| [YARN-2213](https://issues.apache.org/jira/browse/YARN-2213) | Change proxy-user cookie log in AmIpFilter to DEBUG |  Minor | . | Ted Yu | Varun Saxena |


