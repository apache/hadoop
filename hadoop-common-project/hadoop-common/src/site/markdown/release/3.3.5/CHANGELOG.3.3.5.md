
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

## Release 3.3.5 - 2023-03-14



### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-17956](https://issues.apache.org/jira/browse/HADOOP-17956) | Replace all default Charset usage with UTF-8 |  Major | . | Viraj Jasani | Viraj Jasani |
| [HADOOP-18621](https://issues.apache.org/jira/browse/HADOOP-18621) | CryptoOutputStream::close leak when encrypted zones + quota exceptions |  Critical | fs | Colm Dougan | Colm Dougan |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-18003](https://issues.apache.org/jira/browse/HADOOP-18003) | Add a method appendIfAbsent for CallerContext |  Minor | . | Tao Li | Tao Li |
| [HDFS-16331](https://issues.apache.org/jira/browse/HDFS-16331) | Make dfs.blockreport.intervalMsec reconfigurable |  Major | . | Tao Li | Tao Li |
| [HDFS-16371](https://issues.apache.org/jira/browse/HDFS-16371) | Exclude slow disks when choosing volume |  Major | . | Tao Li | Tao Li |
| [HDFS-16400](https://issues.apache.org/jira/browse/HDFS-16400) | Reconfig DataXceiver parameters for datanode |  Major | . | Tao Li | Tao Li |
| [HDFS-16399](https://issues.apache.org/jira/browse/HDFS-16399) | Reconfig cache report parameters for datanode |  Major | . | Tao Li | Tao Li |
| [HDFS-16398](https://issues.apache.org/jira/browse/HDFS-16398) | Reconfig block report parameters for datanode |  Major | . | Tao Li | Tao Li |
| [HDFS-16396](https://issues.apache.org/jira/browse/HDFS-16396) | Reconfig slow peer parameters for datanode |  Major | . | Tao Li | Tao Li |
| [HDFS-16397](https://issues.apache.org/jira/browse/HDFS-16397) | Reconfig slow disk parameters for datanode |  Major | . | Tao Li | Tao Li |
| [MAPREDUCE-7341](https://issues.apache.org/jira/browse/MAPREDUCE-7341) | Add a task-manifest output committer for Azure and GCS |  Major | client | Steve Loughran | Steve Loughran |
| [HADOOP-18163](https://issues.apache.org/jira/browse/HADOOP-18163) | hadoop-azure support for the Manifest Committer of MAPREDUCE-7341 |  Major | fs/azure | Steve Loughran | Steve Loughran |
| [HDFS-16413](https://issues.apache.org/jira/browse/HDFS-16413) | Reconfig dfs usage parameters for datanode |  Major | . | Tao Li | Tao Li |
| [HDFS-16521](https://issues.apache.org/jira/browse/HDFS-16521) | DFS API to retrieve slow datanodes |  Major | . | Viraj Jasani | Viraj Jasani |
| [HDFS-16568](https://issues.apache.org/jira/browse/HDFS-16568) | dfsadmin -reconfig option to start/query reconfig on all live datanodes |  Major | . | Viraj Jasani | Viraj Jasani |
| [HDFS-16582](https://issues.apache.org/jira/browse/HDFS-16582) | Expose aggregate latency of slow node as perceived by the reporting node |  Major | . | Viraj Jasani | Viraj Jasani |
| [HDFS-16595](https://issues.apache.org/jira/browse/HDFS-16595) | Slow peer metrics - add median, mad and upper latency limits |  Major | . | Viraj Jasani | Viraj Jasani |
| [YARN-11241](https://issues.apache.org/jira/browse/YARN-11241) | Add uncleaning option for local app log file with log-aggregation enabled |  Major | log-aggregation | Ashutosh Gupta | Ashutosh Gupta |
| [HADOOP-18103](https://issues.apache.org/jira/browse/HADOOP-18103) | High performance vectored read API in Hadoop |  Major | common, fs, fs/adl, fs/s3 | Mukund Thakur | Mukund Thakur |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-17276](https://issues.apache.org/jira/browse/HADOOP-17276) | Extend CallerContext to make it include many items |  Major | . | Hui Fei | Hui Fei |
| [HDFS-15745](https://issues.apache.org/jira/browse/HDFS-15745) | Make DataNodePeerMetrics#LOW\_THRESHOLD\_MS and MIN\_OUTLIER\_DETECTION\_NODES configurable |  Major | . | Haibin Huang | Haibin Huang |
| [HDFS-16266](https://issues.apache.org/jira/browse/HDFS-16266) | Add remote port information to HDFS audit log |  Major | . | Tao Li | Tao Li |
| [YARN-10997](https://issues.apache.org/jira/browse/YARN-10997) | Revisit allocation and reservation logging |  Major | . | Andras Gyori | Andras Gyori |
| [HDFS-16310](https://issues.apache.org/jira/browse/HDFS-16310) | RBF: Add client port to CallerContext for Router |  Major | . | Tao Li | Tao Li |
| [HDFS-16352](https://issues.apache.org/jira/browse/HDFS-16352) | return the real datanode numBlocks in #getDatanodeStorageReport |  Major | . | qinyuren | qinyuren |
| [HDFS-16426](https://issues.apache.org/jira/browse/HDFS-16426) | fix nextBlockReportTime when trigger full block report force |  Major | . | qinyuren | qinyuren |
| [HDFS-16430](https://issues.apache.org/jira/browse/HDFS-16430) | Validate maximum blocks in EC group when adding an EC policy |  Minor | ec, erasure-coding | daimin | daimin |
| [HDFS-16403](https://issues.apache.org/jira/browse/HDFS-16403) | Improve FUSE IO performance by supporting FUSE parameter max\_background |  Minor | fuse-dfs | daimin | daimin |
| [HDFS-16262](https://issues.apache.org/jira/browse/HDFS-16262) | Async refresh of cached locations in DFSInputStream |  Major | . | Bryan Beaudreault | Bryan Beaudreault |
| [HADOOP-18093](https://issues.apache.org/jira/browse/HADOOP-18093) | Better exception handling for testFileStatusOnMountLink() in ViewFsBaseTest.java |  Trivial | . | Xing Lin | Xing Lin |
| [HDFS-16423](https://issues.apache.org/jira/browse/HDFS-16423) | balancer should not get blocks on stale storages |  Major | balancer & mover | qinyuren | qinyuren |
| [HADOOP-18139](https://issues.apache.org/jira/browse/HADOOP-18139) | Allow configuration of zookeeper server principal |  Major | auth | Owen O'Malley | Owen O'Malley |
| [YARN-11076](https://issues.apache.org/jira/browse/YARN-11076) | Upgrade jQuery version in Yarn UI2 |  Major | yarn-ui-v2 | Tamas Domok | Tamas Domok |
| [HDFS-16495](https://issues.apache.org/jira/browse/HDFS-16495) | RBF should prepend the client ip rather than append it. |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-18144](https://issues.apache.org/jira/browse/HADOOP-18144) | getTrashRoot/s in ViewFileSystem should return viewFS path, not targetFS path |  Major | common | Xing Lin | Xing Lin |
| [HADOOP-18162](https://issues.apache.org/jira/browse/HADOOP-18162) | hadoop-common enhancements for the Manifest Committer of MAPREDUCE-7341 |  Major | fs | Steve Loughran | Steve Loughran |
| [HDFS-16529](https://issues.apache.org/jira/browse/HDFS-16529) | Remove unnecessary setObserverRead in TestConsistentReadsObserver |  Trivial | test | Zhaohui Wang | Zhaohui Wang |
| [HDFS-16530](https://issues.apache.org/jira/browse/HDFS-16530) | setReplication debug log creates a new string even if debug is disabled |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-16457](https://issues.apache.org/jira/browse/HDFS-16457) | Make fs.getspaceused.classname reconfigurable |  Major | namenode | yanbin.zhang | yanbin.zhang |
| [HDFS-16427](https://issues.apache.org/jira/browse/HDFS-16427) | Add debug log for BlockManager#chooseExcessRedundancyStriped |  Minor | erasure-coding | Tao Li | Tao Li |
| [HDFS-16497](https://issues.apache.org/jira/browse/HDFS-16497) | EC: Add param comment for liveBusyBlockIndices with HDFS-14768 |  Minor | erasure-coding, namanode | caozhiqiang | caozhiqiang |
| [HDFS-16389](https://issues.apache.org/jira/browse/HDFS-16389) | Improve NNThroughputBenchmark test mkdirs |  Major | benchmarks, namenode | JiangHua Zhu | JiangHua Zhu |
| [HADOOP-17551](https://issues.apache.org/jira/browse/HADOOP-17551) | Upgrade maven-site-plugin to 3.11.0 |  Major | . | Akira Ajisaka | Ashutosh Gupta |
| [HDFS-16519](https://issues.apache.org/jira/browse/HDFS-16519) | Add throttler to EC reconstruction |  Minor | datanode, ec | daimin | daimin |
| [HDFS-14478](https://issues.apache.org/jira/browse/HDFS-14478) | Add libhdfs APIs for openFile |  Major | hdfs-client, libhdfs, native | Sahil Takiar | Sahil Takiar |
| [HADOOP-16202](https://issues.apache.org/jira/browse/HADOOP-16202) | Enhance openFile() for better read performance against object stores |  Major | fs, fs/s3, tools/distcp | Steve Loughran | Steve Loughran |
| [YARN-11116](https://issues.apache.org/jira/browse/YARN-11116) | Migrate Times util from SimpleDateFormat to thread-safe DateTimeFormatter class |  Minor | . | Jonathan Turner Eagles | Jonathan Turner Eagles |
| [HDFS-16520](https://issues.apache.org/jira/browse/HDFS-16520) | Improve EC pread: avoid potential reading whole block |  Major | dfsclient, ec, erasure-coding | daimin | daimin |
| [HADOOP-18167](https://issues.apache.org/jira/browse/HADOOP-18167) | Add metrics to track delegation token secret manager operations |  Major | . | Hector Sandoval Chaverri | Hector Sandoval Chaverri |
| [YARN-10080](https://issues.apache.org/jira/browse/YARN-10080) | Support show app id on localizer thread pool |  Major | nodemanager | zhoukang | Ashutosh Gupta |
| [HADOOP-18172](https://issues.apache.org/jira/browse/HADOOP-18172) | Change scope of getRootFallbackLink for InodeTree to make them accessible from outside package |  Minor | . | Xing Lin | Xing Lin |
| [HDFS-16588](https://issues.apache.org/jira/browse/HDFS-16588) | Backport HDFS-16584 to branch-3.3. |  Major | balancer & mover, namenode | JiangHua Zhu | JiangHua Zhu |
| [HADOOP-18240](https://issues.apache.org/jira/browse/HADOOP-18240) | Upgrade Yetus to 0.14.0 |  Major | build | Akira Ajisaka | Ashutosh Gupta |
| [HDFS-16585](https://issues.apache.org/jira/browse/HDFS-16585) | Add @VisibleForTesting in Dispatcher.java after HDFS-16268 |  Trivial | . | Wei-Chiu Chuang | Ashutosh Gupta |
| [HADOOP-18244](https://issues.apache.org/jira/browse/HADOOP-18244) | Fix Hadoop-Common JavaDoc Error on branch-3.3 |  Major | common | Shilun Fan | Shilun Fan |
| [HADOOP-18269](https://issues.apache.org/jira/browse/HADOOP-18269) | Misleading method name in DistCpOptions |  Minor | tools/distcp | guophilipse | guophilipse |
| [HADOOP-18275](https://issues.apache.org/jira/browse/HADOOP-18275) | update os-maven-plugin to 1.7.0 |  Minor | build | Steve Loughran | Steve Loughran |
| [HDFS-16610](https://issues.apache.org/jira/browse/HDFS-16610) | Make fsck read timeout configurable |  Major | hdfs-client | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-16576](https://issues.apache.org/jira/browse/HDFS-16576) | Remove unused imports in HDFS project |  Minor | . | Ashutosh Gupta | Ashutosh Gupta |
| [HDFS-16629](https://issues.apache.org/jira/browse/HDFS-16629) | [JDK 11] Fix javadoc  warnings in hadoop-hdfs module |  Minor | hdfs | Shilun Fan | Shilun Fan |
| [YARN-11172](https://issues.apache.org/jira/browse/YARN-11172) | Fix testDelegationToken |  Major | test | zhengchenyu | zhengchenyu |
| [HADOOP-17833](https://issues.apache.org/jira/browse/HADOOP-17833) | Improve Magic Committer Performance |  Minor | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-18288](https://issues.apache.org/jira/browse/HADOOP-18288) | Total requests and total requests per sec served by RPC servers |  Major | . | Viraj Jasani | Viraj Jasani |
| [HADOOP-18336](https://issues.apache.org/jira/browse/HADOOP-18336) | tag FSDataInputStream.getWrappedStream() @Public/@Stable |  Minor | fs | Steve Loughran | Ashutosh Gupta |
| [HADOOP-13144](https://issues.apache.org/jira/browse/HADOOP-13144) | Enhancing IPC client throughput via multiple connections per user |  Minor | ipc | Jason Kace | Íñigo Goiri |
| [HDFS-16712](https://issues.apache.org/jira/browse/HDFS-16712) | Fix incorrect placeholder in DataNode.java |  Major | . | ZanderXu | ZanderXu |
| [HDFS-16702](https://issues.apache.org/jira/browse/HDFS-16702) | MiniDFSCluster should report cause of exception in assertion error |  Minor | hdfs | Steve Vaughan | Steve Vaughan |
| [HADOOP-18365](https://issues.apache.org/jira/browse/HADOOP-18365) | Updated addresses are still accessed using the old IP address |  Major | common | Steve Vaughan | Steve Vaughan |
| [HDFS-16687](https://issues.apache.org/jira/browse/HDFS-16687) | RouterFsckServlet replicates code from DfsServlet base class |  Major | federation | Steve Vaughan | Steve Vaughan |
| [HADOOP-18333](https://issues.apache.org/jira/browse/HADOOP-18333) | hadoop-client-runtime impact by CVE-2022-2047 CVE-2022-2048 due to shaded jetty |  Major | build | phoebe chen | Ashutosh Gupta |
| [HADOOP-18406](https://issues.apache.org/jira/browse/HADOOP-18406) | Adds alignment context to call path for creating RPC proxy with multiple connections per user. |  Major | ipc | Simbarashe Dzinamarira | Simbarashe Dzinamarira |
| [HDFS-16684](https://issues.apache.org/jira/browse/HDFS-16684) | Exclude self from JournalNodeSyncer when using a bind host |  Major | journal-node | Steve Vaughan | Steve Vaughan |
| [HDFS-16686](https://issues.apache.org/jira/browse/HDFS-16686) | GetJournalEditServlet fails to authorize valid Kerberos request |  Major | journal-node | Steve Vaughan | Steve Vaughan |
| [YARN-11303](https://issues.apache.org/jira/browse/YARN-11303) | Upgrade jquery ui to 1.13.2 |  Major | security | D M Murali Krishna Reddy | Ashutosh Gupta |
| [HADOOP-16769](https://issues.apache.org/jira/browse/HADOOP-16769) | LocalDirAllocator to provide diagnostics when file creation fails |  Minor | util | Ramesh Kumar Thangarajan | Ashutosh Gupta |
| [HADOOP-18341](https://issues.apache.org/jira/browse/HADOOP-18341) | upgrade commons-configuration2 to 2.8.0 and commons-text to 1.9 |  Major | . | PJ Fanning | PJ Fanning |
| [HDFS-16776](https://issues.apache.org/jira/browse/HDFS-16776) | Erasure Coding: The length of targets should be checked when DN gets a reconstruction task |  Major | . | Kidd5368 | Kidd5368 |
| [HADOOP-18469](https://issues.apache.org/jira/browse/HADOOP-18469) | Add XMLUtils methods to centralise code that creates secure XML parsers |  Major | . | PJ Fanning | PJ Fanning |
| [HADOOP-18442](https://issues.apache.org/jira/browse/HADOOP-18442) | Remove the hadoop-openstack module |  Major | build, fs, fs/swift | Steve Loughran | Steve Loughran |
| [HADOOP-18468](https://issues.apache.org/jira/browse/HADOOP-18468) | upgrade jettison json jar due to fix CVE-2022-40149 |  Major | build | PJ Fanning | PJ Fanning |
| [HADOOP-17779](https://issues.apache.org/jira/browse/HADOOP-17779) | Lock File System Creator Semaphore Uninterruptibly |  Minor | fs | David Mollitor | David Mollitor |
| [HADOOP-18360](https://issues.apache.org/jira/browse/HADOOP-18360) | Update commons-csv from 1.0 to 1.9.0. |  Minor | common | Shilun Fan | Shilun Fan |
| [HADOOP-18493](https://issues.apache.org/jira/browse/HADOOP-18493) | update jackson-databind 2.12.7.1 due to CVE fixes |  Major | . | PJ Fanning | PJ Fanning |
| [HADOOP-17563](https://issues.apache.org/jira/browse/HADOOP-17563) | Update Bouncy Castle to 1.68 or later |  Major | build | Takanobu Asanuma | PJ Fanning |
| [HADOOP-18497](https://issues.apache.org/jira/browse/HADOOP-18497) | Upgrade commons-text version to fix CVE-2022-42889 |  Major | build | Xiaoqiao He | PJ Fanning |
| [HDFS-16795](https://issues.apache.org/jira/browse/HDFS-16795) | Use secure XML parser utils in hdfs classes |  Major | . | PJ Fanning | PJ Fanning |
| [YARN-11330](https://issues.apache.org/jira/browse/YARN-11330) | Use secure XML parser utils in YARN |  Major | . | PJ Fanning | PJ Fanning |
| [MAPREDUCE-7411](https://issues.apache.org/jira/browse/MAPREDUCE-7411) | Use secure XML parser utils in MapReduce |  Major | . | PJ Fanning | PJ Fanning |
| [HADOOP-18512](https://issues.apache.org/jira/browse/HADOOP-18512) | upgrade woodstox-core to 5.4.0 for security fix |  Major | common | phoebe chen | PJ Fanning |
| [YARN-11363](https://issues.apache.org/jira/browse/YARN-11363) | Remove unused TimelineVersionWatcher and TimelineVersion from hadoop-yarn-server-tests |  Major | test, yarn | Ashutosh Gupta | Ashutosh Gupta |
| [YARN-11364](https://issues.apache.org/jira/browse/YARN-11364) | Docker Container to accept docker Image name with sha256 digest |  Major | yarn | Ashutosh Gupta | Ashutosh Gupta |
| [HADOOP-18517](https://issues.apache.org/jira/browse/HADOOP-18517) | ABFS: Add fs.azure.enable.readahead option to disable readahead |  Major | fs/azure | Steve Loughran | Steve Loughran |
| [HADOOP-18484](https://issues.apache.org/jira/browse/HADOOP-18484) | upgrade hsqldb to v2.7.1 due to CVE |  Major | . | PJ Fanning | Ashutosh Gupta |
| [HDFS-16844](https://issues.apache.org/jira/browse/HDFS-16844) | [RBF] The routers should be resiliant against exceptions from StateStore |  Major | rbf | Owen O'Malley | Owen O'Malley |
| [HADOOP-18573](https://issues.apache.org/jira/browse/HADOOP-18573) | Improve error reporting on non-standard kerberos names |  Blocker | security | Steve Loughran | Steve Loughran |
| [HADOOP-18561](https://issues.apache.org/jira/browse/HADOOP-18561) | CVE-2021-37533 on commons-net is included in hadoop common and hadoop-client-runtime |  Blocker | build | phoebe chen | Steve Loughran |
| [HADOOP-18067](https://issues.apache.org/jira/browse/HADOOP-18067) | Über-jira: S3A Hadoop 3.3.5 features |  Major | fs/s3 | Steve Loughran | Mukund Thakur |
| [YARN-10444](https://issues.apache.org/jira/browse/YARN-10444) | Node Manager to use openFile() with whole-file read policy for localizing files. |  Minor | nodemanager | Steve Loughran | Steve Loughran |
| [HADOOP-18661](https://issues.apache.org/jira/browse/HADOOP-18661) | Fix bin/hadoop usage script terminology |  Blocker | scripts | Steve Loughran | Steve Loughran |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-17116](https://issues.apache.org/jira/browse/HADOOP-17116) | Skip Retry INFO logging on first failover from a proxy |  Major | ha | Hanisha Koneru | Hanisha Koneru |
| [YARN-10553](https://issues.apache.org/jira/browse/YARN-10553) | Refactor TestDistributedShell |  Major | distributed-shell, test | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15839](https://issues.apache.org/jira/browse/HDFS-15839) | RBF: Cannot get method setBalancerBandwidth on Router Client |  Major | rbf | Yang Yun | Yang Yun |
| [HADOOP-17588](https://issues.apache.org/jira/browse/HADOOP-17588) | CryptoInputStream#close() should be synchronized |  Major | . | Renukaprasad C | Renukaprasad C |
| [HADOOP-17836](https://issues.apache.org/jira/browse/HADOOP-17836) | Improve logging on ABFS error reporting |  Minor | fs/azure | Steve Loughran | Steve Loughran |
| [HADOOP-17989](https://issues.apache.org/jira/browse/HADOOP-17989) | ITestAzureBlobFileSystemDelete failing "Operations has null HTTP response" |  Major | fs/azure, test | Steve Loughran | Steve Loughran |
| [YARN-11055](https://issues.apache.org/jira/browse/YARN-11055) | In cgroups-operations.c some fprintf format strings don't end with "\\n" |  Minor | nodemanager | Gera Shegalov | Gera Shegalov |
| [YARN-11065](https://issues.apache.org/jira/browse/YARN-11065) | Bump follow-redirects from 1.13.3 to 1.14.7 in hadoop-yarn-ui |  Major | yarn-ui-v2 | Akira Ajisaka |  |
| [HDFS-16303](https://issues.apache.org/jira/browse/HDFS-16303) | Losing over 100 datanodes in state decommissioning results in full blockage of all datanode decommissioning |  Major | . | Kevin Wikant | Kevin Wikant |
| [HDFS-16443](https://issues.apache.org/jira/browse/HDFS-16443) | Fix edge case where DatanodeAdminDefaultMonitor doubly enqueues a DatanodeDescriptor on exception |  Major | hdfs | Kevin Wikant | Kevin Wikant |
| [HDFS-16449](https://issues.apache.org/jira/browse/HDFS-16449) | Fix hadoop web site release notes and changelog not available |  Minor | documentation | guophilipse | guophilipse |
| [YARN-10788](https://issues.apache.org/jira/browse/YARN-10788) | TestCsiClient fails |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-18126](https://issues.apache.org/jira/browse/HADOOP-18126) | Update junit 5 version due to build issues |  Major | bulid | PJ Fanning | PJ Fanning |
| [YARN-11033](https://issues.apache.org/jira/browse/YARN-11033) | isAbsoluteResource is not correct for dynamically created queues |  Minor | yarn | Tamas Domok | Tamas Domok |
| [YARN-10894](https://issues.apache.org/jira/browse/YARN-10894) | Follow up YARN-10237: fix the new test case in TestRMWebServicesCapacitySched |  Major | . | Tamas Domok | Tamas Domok |
| [YARN-11022](https://issues.apache.org/jira/browse/YARN-11022) | Fix the documentation for max-parallel-apps in CS |  Major | capacity scheduler | Tamas Domok | Tamas Domok |
| [HADOOP-18150](https://issues.apache.org/jira/browse/HADOOP-18150) | Fix ITestAuditManagerDisabled after S3A audit logging was enabled in HADOOP-18091 |  Major | fs/s3 | Mehakmeet Singh | Mehakmeet Singh |
| [HADOOP-17976](https://issues.apache.org/jira/browse/HADOOP-17976) | abfs etag extraction inconsistent between LIST and HEAD calls |  Minor | fs/azure | Steve Loughran | Steve Loughran |
| [HADOOP-18129](https://issues.apache.org/jira/browse/HADOOP-18129) | Change URI[] in INodeLink to String[] to reduce memory footprint of ViewFileSystem |  Major | . | Abhishek Das | Abhishek Das |
| [HADOOP-18145](https://issues.apache.org/jira/browse/HADOOP-18145) | Fileutil's unzip method causes unzipped files to lose their original permissions |  Major | common | jingxiong zhong | jingxiong zhong |
| [HDFS-16518](https://issues.apache.org/jira/browse/HDFS-16518) | KeyProviderCache close cached KeyProvider with Hadoop ShutdownHookManager |  Major | hdfs | Lei Yang | Lei Yang |
| [HADOOP-18169](https://issues.apache.org/jira/browse/HADOOP-18169) | getDelegationTokens in ViewFs should also fetch the token from the fallback FS |  Major | . | Xing Lin | Xing Lin |
| [HDFS-16479](https://issues.apache.org/jira/browse/HDFS-16479) | EC: NameNode should not send a reconstruction work when the source datanodes are insufficient |  Critical | ec, erasure-coding | Yuanbo Liu | Takanobu Asanuma |
| [HDFS-16509](https://issues.apache.org/jira/browse/HDFS-16509) | Fix decommission UnsupportedOperationException: Remove unsupported |  Major | namenode | daimin | daimin |
| [HDFS-16456](https://issues.apache.org/jira/browse/HDFS-16456) | EC: Decommission a rack with only on dn will fail when the rack number is equal with replication |  Critical | ec, namenode | caozhiqiang | caozhiqiang |
| [HADOOP-18201](https://issues.apache.org/jira/browse/HADOOP-18201) | Remove base and bucket overrides for endpoint in ITestS3ARequesterPays.java |  Major | fs/s3 | Mehakmeet Singh | Daniel Carl Jones |
| [HDFS-16536](https://issues.apache.org/jira/browse/HDFS-16536) | TestOfflineImageViewer fails on branch-3.3 |  Major | test | Akira Ajisaka | Ashutosh Gupta |
| [HDFS-16538](https://issues.apache.org/jira/browse/HDFS-16538) |  EC decoding failed due to not enough valid inputs |  Major | erasure-coding | qinyuren | qinyuren |
| [HDFS-16544](https://issues.apache.org/jira/browse/HDFS-16544) | EC decoding failed due to invalid buffer |  Major | erasure-coding | qinyuren | qinyuren |
| [HADOOP-17564](https://issues.apache.org/jira/browse/HADOOP-17564) | Fix typo in UnixShellGuide.html |  Trivial | . | Takanobu Asanuma | Ashutosh Gupta |
| [HDFS-16552](https://issues.apache.org/jira/browse/HDFS-16552) | Fix NPE for TestBlockManager |  Major | . | Tao Li | Tao Li |
| [MAPREDUCE-7246](https://issues.apache.org/jira/browse/MAPREDUCE-7246) |  In MapredAppMasterRest#Mapreduce\_Application\_Master\_Info\_API, the datatype of appId should be "string". |  Major | documentation | jenny | Ashutosh Gupta |
| [YARN-10187](https://issues.apache.org/jira/browse/YARN-10187) | Removing hadoop-yarn-project/hadoop-yarn/README as it is no longer maintained. |  Minor | documentation | N Sanketh Reddy | Ashutosh Gupta |
| [HADOOP-16515](https://issues.apache.org/jira/browse/HADOOP-16515) | Update the link to compatibility guide |  Minor | documentation | Akira Ajisaka | Ashutosh Gupta |
| [HDFS-16185](https://issues.apache.org/jira/browse/HDFS-16185) | Fix comment in LowRedundancyBlocks.java |  Minor | documentation | Akira Ajisaka | Ashutosh Gupta |
| [HADOOP-17479](https://issues.apache.org/jira/browse/HADOOP-17479) | Fix the examples of hadoop config prefix |  Minor | documentation | Akira Ajisaka | Ashutosh Gupta |
| [HADOOP-18222](https://issues.apache.org/jira/browse/HADOOP-18222) | Prevent DelegationTokenSecretManagerMetrics from registering multiple times |  Major | . | Hector Sandoval Chaverri | Hector Sandoval Chaverri |
| [HDFS-16540](https://issues.apache.org/jira/browse/HDFS-16540) | Data locality is lost when DataNode pod restarts in kubernetes |  Major | namenode | Huaxiang Sun | Huaxiang Sun |
| [YARN-11133](https://issues.apache.org/jira/browse/YARN-11133) | YarnClient gets the wrong EffectiveMinCapacity value |  Major | api | Zilong Zhu | Zilong Zhu |
| [YARN-10850](https://issues.apache.org/jira/browse/YARN-10850) | TimelineService v2 lists containers for all attempts when filtering for one |  Major | timelinereader | Benjamin Teke | Benjamin Teke |
| [YARN-11141](https://issues.apache.org/jira/browse/YARN-11141) | Capacity Scheduler does not support ambiguous queue names when moving application across queues |  Major | capacity scheduler | András Győri | András Győri |
| [HDFS-16586](https://issues.apache.org/jira/browse/HDFS-16586) | Purge FsDatasetAsyncDiskService threadgroup; it causes BPServiceActor$CommandProcessingThread IllegalThreadStateException 'fatal exception and exit' |  Major | datanode | Michael Stack | Michael Stack |
| [HADOOP-18251](https://issues.apache.org/jira/browse/HADOOP-18251) | Fix failure of extracting JIRA id from commit message in git\_jira\_fix\_version\_check.py |  Minor | build | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-11128](https://issues.apache.org/jira/browse/YARN-11128) | Fix comments in TestProportionalCapacityPreemptionPolicy\* |  Minor | capacityscheduler, documentation | Ashutosh Gupta | Ashutosh Gupta |
| [HADOOP-18234](https://issues.apache.org/jira/browse/HADOOP-18234) | s3a access point xml examples are wrong |  Minor | documentation, fs/s3 | Steve Loughran | Ashutosh Gupta |
| [HADOOP-18238](https://issues.apache.org/jira/browse/HADOOP-18238) | Fix reentrancy check in SFTPFileSystem.close() |  Major | common | yi liu | Ashutosh Gupta |
| [HDFS-16583](https://issues.apache.org/jira/browse/HDFS-16583) | DatanodeAdminDefaultMonitor can get stuck in an infinite loop |  Major | . | Stephen O'Donnell | Stephen O'Donnell |
| [HDFS-16608](https://issues.apache.org/jira/browse/HDFS-16608) | Fix the link in TestClientProtocolForPipelineRecovery |  Minor | documentation | Samrat Deb | Samrat Deb |
| [HDFS-16563](https://issues.apache.org/jira/browse/HDFS-16563) | Namenode WebUI prints sensitive information on Token Expiry |  Major | namanode, security, webhdfs | Renukaprasad C | Renukaprasad C |
| [HDFS-16623](https://issues.apache.org/jira/browse/HDFS-16623) | IllegalArgumentException in LifelineSender |  Major | . | ZanderXu | ZanderXu |
| [HDFS-16064](https://issues.apache.org/jira/browse/HDFS-16064) | Determine when to invalidate corrupt replicas based on number of usable replicas |  Major | datanode, namenode | Kevin Wikant | Kevin Wikant |
| [HADOOP-18255](https://issues.apache.org/jira/browse/HADOOP-18255) | fsdatainputstreambuilder.md refers to hadoop 3.3.3, when it shouldn't |  Minor | documentation | Steve Loughran | Ashutosh Gupta |
| [MAPREDUCE-7387](https://issues.apache.org/jira/browse/MAPREDUCE-7387) | Fix TestJHSSecurity#testDelegationToken AssertionError due to HDFS-16563 |  Major | . | Shilun Fan | Shilun Fan |
| [MAPREDUCE-7369](https://issues.apache.org/jira/browse/MAPREDUCE-7369) | MapReduce tasks timing out when spends more time on MultipleOutputs#close |  Major | . | Prabhu Joseph | Ashutosh Gupta |
| [MAPREDUCE-7391](https://issues.apache.org/jira/browse/MAPREDUCE-7391) | TestLocalDistributedCacheManager failing after HADOOP-16202 |  Major | test | Steve Loughran | Steve Loughran |
| [HDFS-16591](https://issues.apache.org/jira/browse/HDFS-16591) | StateStoreZooKeeper fails to initialize |  Major | rbf | Hector Sandoval Chaverri | Hector Sandoval Chaverri |
| [HADOOP-18321](https://issues.apache.org/jira/browse/HADOOP-18321) | Fix when to read an additional record from a BZip2 text file split |  Critical | io | Ashutosh Gupta | Ashutosh Gupta |
| [HADOOP-18100](https://issues.apache.org/jira/browse/HADOOP-18100) | Change scope of inner classes in InodeTree to make them accessible outside package |  Major | . | Abhishek Das | Abhishek Das |
| [HADOOP-18217](https://issues.apache.org/jira/browse/HADOOP-18217) | shutdownhookmanager should not be multithreaded (deadlock possible) |  Minor | util | Catherinot Remi |  |
| [MAPREDUCE-7372](https://issues.apache.org/jira/browse/MAPREDUCE-7372) | MapReduce set permission too late in copyJar method |  Major | mrv2 | Zhang Dongsheng |  |
| [HADOOP-18330](https://issues.apache.org/jira/browse/HADOOP-18330) | S3AFileSystem removes Path when calling createS3Client |  Minor | fs/s3 | Ashutosh Pant | Ashutosh Pant |
| [HADOOP-18390](https://issues.apache.org/jira/browse/HADOOP-18390) | Fix out of sync import for HADOOP-18321 |  Minor | . | Ashutosh Gupta | Ashutosh Gupta |
| [HADOOP-18340](https://issues.apache.org/jira/browse/HADOOP-18340) | deleteOnExit does not work with S3AFileSystem |  Minor | fs/s3 | Huaxiang Sun | Huaxiang Sun |
| [HADOOP-18383](https://issues.apache.org/jira/browse/HADOOP-18383) | Codecs with @DoNotPool annotation are not closed causing memory leak |  Major | common | Kevin Sewell | Kevin Sewell |
| [HDFS-16729](https://issues.apache.org/jira/browse/HDFS-16729) | RBF: fix some unreasonably annotated docs |  Major | documentation, rbf | JiangHua Zhu | JiangHua Zhu |
| [HADOOP-18398](https://issues.apache.org/jira/browse/HADOOP-18398) | Prevent AvroRecord\*.class from being included non-test jar |  Major | common | YUBI LEE | YUBI LEE |
| [HDFS-4043](https://issues.apache.org/jira/browse/HDFS-4043) | Namenode Kerberos Login does not use proper hostname for host qualified hdfs principal name. |  Major | security | Ahad Rana | Steve Vaughan |
| [MAPREDUCE-7403](https://issues.apache.org/jira/browse/MAPREDUCE-7403) | Support spark dynamic partitioning in the Manifest Committer |  Major | mrv2 | Steve Loughran | Steve Loughran |
| [HDFS-16732](https://issues.apache.org/jira/browse/HDFS-16732) | [SBN READ] Avoid get location from observer when the block report is delayed. |  Critical | hdfs | zhengchenyu | zhengchenyu |
| [HADOOP-18375](https://issues.apache.org/jira/browse/HADOOP-18375) | Fix failure of shelltest for hadoop\_add\_ldlibpath |  Minor | test | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-16755](https://issues.apache.org/jira/browse/HDFS-16755) | TestQJMWithFaults.testUnresolvableHostName() can fail due to unexpected host resolution |  Minor | test | Steve Vaughan | Steve Vaughan |
| [HADOOP-18400](https://issues.apache.org/jira/browse/HADOOP-18400) |  Fix file split duplicating records from a succeeding split when reading BZip2 text files |  Critical | . | Ashutosh Gupta | Ashutosh Gupta |
| [HADOOP-18242](https://issues.apache.org/jira/browse/HADOOP-18242) | ABFS Rename Failure when tracking metadata is in incomplete state |  Major | fs/azure | Mehakmeet Singh | Mehakmeet Singh |
| [HADOOP-18456](https://issues.apache.org/jira/browse/HADOOP-18456) | NullPointerException in ObjectListingIterator's constructor |  Blocker | fs/s3 | Quanlong Huang | Steve Loughran |
| [HADOOP-18444](https://issues.apache.org/jira/browse/HADOOP-18444) | Add Support for localized trash for ViewFileSystem in Trash.moveToAppropriateTrash |  Major | . | Xing Lin | Xing Lin |
| [HADOOP-18443](https://issues.apache.org/jira/browse/HADOOP-18443) | Upgrade snakeyaml to 1.32 |  Major | security | Ashutosh Gupta | Ashutosh Gupta |
| [HDFS-16766](https://issues.apache.org/jira/browse/HDFS-16766) | hdfs ec command loads (administrator provided) erasure code policy files without disabling xml entity expansion |  Major | security | Jing | Ashutosh Gupta |
| [HDFS-13369](https://issues.apache.org/jira/browse/HDFS-13369) | FSCK Report broken with RequestHedgingProxyProvider |  Major | hdfs | Harshakiran Reddy | Ranith Sardar |
| [YARN-11039](https://issues.apache.org/jira/browse/YARN-11039) | LogAggregationFileControllerFactory::getFileControllerForRead can leak threads |  Blocker | log-aggregation | Rajesh Balamohan | Steve Loughran |
| [HADOOP-18499](https://issues.apache.org/jira/browse/HADOOP-18499) | S3A to support HTTPS web proxies |  Major | fs/s3 | Mehakmeet Singh | Mehakmeet Singh |
| [HADOOP-18233](https://issues.apache.org/jira/browse/HADOOP-18233) | Possible race condition with TemporaryAWSCredentialsProvider |  Major | auth, fs/s3 | Jason Sleight | Jimmy Wong |
| [MAPREDUCE-7425](https://issues.apache.org/jira/browse/MAPREDUCE-7425) | Document Fix for yarn.app.mapreduce.client-am.ipc.max-retries |  Major | yarn | teng wang | teng wang |
| [HADOOP-18528](https://issues.apache.org/jira/browse/HADOOP-18528) | Disable abfs prefetching by default |  Major | fs/azure | Mehakmeet Singh | Mehakmeet Singh |
| [HDFS-16836](https://issues.apache.org/jira/browse/HDFS-16836) | StandbyCheckpointer can still trigger rollback fs image after RU is finalized |  Major | hdfs | Lei Yang | Lei Yang |
| [HADOOP-18324](https://issues.apache.org/jira/browse/HADOOP-18324) | Interrupting RPC Client calls can lead to thread exhaustion |  Critical | ipc | Owen O'Malley | Owen O'Malley |
| [HDFS-16832](https://issues.apache.org/jira/browse/HDFS-16832) | [SBN READ] Fix NPE when check the block location of empty directory |  Major | . | zhengchenyu | zhengchenyu |
| [HADOOP-18498](https://issues.apache.org/jira/browse/HADOOP-18498) | [ABFS]: Error introduced when SAS Token containing '?' prefix is passed |  Minor | fs/azure | Sree Bhattacharyya | Sree Bhattacharyya |
| [HDFS-16847](https://issues.apache.org/jira/browse/HDFS-16847) | RBF: StateStore writer should not commit tmp fail if there was an error in writing the file. |  Critical | hdfs, rbf | Simbarashe Dzinamarira | Simbarashe Dzinamarira |
| [HADOOP-18401](https://issues.apache.org/jira/browse/HADOOP-18401) | No ARM binaries in branch-3.3.x releases |  Minor | build | Ling Xu |  |
| [HADOOP-18408](https://issues.apache.org/jira/browse/HADOOP-18408) | [ABFS]: ITestAbfsManifestCommitProtocol  fails on nonHNS configuration |  Minor | fs/azure, test | Pranav Saxena | Sree Bhattacharyya |
| [HADOOP-18402](https://issues.apache.org/jira/browse/HADOOP-18402) | S3A committer NPE in spark job abort |  Blocker | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-18569](https://issues.apache.org/jira/browse/HADOOP-18569) | NFS Gateway may release buffer too early |  Blocker | nfs | Attila Doroszlai | Attila Doroszlai |
| [HADOOP-18574](https://issues.apache.org/jira/browse/HADOOP-18574) | Changing log level of IOStatistics increment to make the DEBUG logs less noisy |  Major | fs/s3 | Mehakmeet Singh | Mehakmeet Singh |
| [HADOOP-18521](https://issues.apache.org/jira/browse/HADOOP-18521) | ABFS ReadBufferManager buffer sharing across concurrent HTTP requests |  Critical | fs/azure | Steve Loughran | Steve Loughran |
| [MAPREDUCE-7375](https://issues.apache.org/jira/browse/MAPREDUCE-7375) | JobSubmissionFiles don't set right permission after mkdirs |  Major | mrv2 | Zhang Dongsheng |  |
| [HADOOP-17717](https://issues.apache.org/jira/browse/HADOOP-17717) | Update wildfly openssl to 1.1.3.Final |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-18598](https://issues.apache.org/jira/browse/HADOOP-18598) | maven site generation doesn't include javadocs |  Blocker | site | Steve Loughran | Steve Loughran |
| [HDFS-16895](https://issues.apache.org/jira/browse/HDFS-16895) | NamenodeHeartbeatService should use credentials of logged in user |  Major | rbf | Hector Sandoval Chaverri | Hector Sandoval Chaverri |
| [HDFS-16853](https://issues.apache.org/jira/browse/HDFS-16853) | The UT TestLeaseRecovery2#testHardLeaseRecoveryAfterNameNodeRestart failed because HADOOP-18324 |  Blocker | . | ZanderXu | ZanderXu |
| [HADOOP-18641](https://issues.apache.org/jira/browse/HADOOP-18641) | cyclonedx maven plugin breaks builds on recent maven releases (3.9.0) |  Major | build | Steve Loughran | Steve Loughran |
| [HDFS-16923](https://issues.apache.org/jira/browse/HDFS-16923) | The getListing RPC will throw NPE if the path does not exist |  Critical | . | ZanderXu | ZanderXu |
| [HDFS-16896](https://issues.apache.org/jira/browse/HDFS-16896) | HDFS Client hedged read has increased failure rate than without hedged read |  Major | hdfs-client | Tom McCormick | Tom McCormick |
| [YARN-11383](https://issues.apache.org/jira/browse/YARN-11383) | Workflow priority mappings is case sensitive |  Major | yarn | Aparajita Choudhary | Aparajita Choudhary |
| [HDFS-16939](https://issues.apache.org/jira/browse/HDFS-16939) | Fix the thread safety bug in LowRedundancyBlocks |  Major | namanode | Shuyan Zhang | Shuyan Zhang |
| [HDFS-16934](https://issues.apache.org/jira/browse/HDFS-16934) | org.apache.hadoop.hdfs.tools.TestDFSAdmin#testAllDatanodesReconfig regression |  Minor | dfsadmin, test | Steve Loughran | Shilun Fan |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-16573](https://issues.apache.org/jira/browse/HDFS-16573) | Fix test TestDFSStripedInputStreamWithRandomECPolicy |  Minor | test | daimin | daimin |
| [HDFS-16637](https://issues.apache.org/jira/browse/HDFS-16637) | TestHDFSCLI#testAll consistently failing |  Major | . | Viraj Jasani | Viraj Jasani |
| [YARN-11248](https://issues.apache.org/jira/browse/YARN-11248) | Add unit test for FINISHED\_CONTAINERS\_PULLED\_BY\_AM event on DECOMMISSIONING |  Major | test | Ashutosh Gupta | Ashutosh Gupta |
| [HDFS-16625](https://issues.apache.org/jira/browse/HDFS-16625) | Unit tests aren't checking for PMDK availability |  Major | test | Steve Vaughan | Steve Vaughan |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-13293](https://issues.apache.org/jira/browse/HDFS-13293) | RBF: The RouterRPCServer should transfer client IP via CallerContext to NamenodeRpcServer |  Major | rbf | Baolong Mao | Hui Fei |
| [HDFS-15630](https://issues.apache.org/jira/browse/HDFS-15630) | RBF: Fix wrong client IP info in CallerContext when requests mount points with multi-destinations. |  Major | rbf | Chengwei Wang | Chengwei Wang |
| [HADOOP-17152](https://issues.apache.org/jira/browse/HADOOP-17152) | Implement wrapper for guava newArrayList and newLinkedList |  Major | common | Ahmed Hussein | Viraj Jasani |
| [HADOOP-17851](https://issues.apache.org/jira/browse/HADOOP-17851) | S3A to support user-specified content encoding |  Minor | fs/s3 | Holden Karau | Holden Karau |
| [HADOOP-17492](https://issues.apache.org/jira/browse/HADOOP-17492) | abfs listLocatedStatus to support incremental/async page fetching |  Major | fs/azure | Steve Loughran | Steve Loughran |
| [HADOOP-17409](https://issues.apache.org/jira/browse/HADOOP-17409) | Remove S3Guard - no longer needed |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-18084](https://issues.apache.org/jira/browse/HADOOP-18084) | ABFS: Add testfilePath while verifying test contents are read correctly |  Minor | fs/azure, test | Anmol Asrani | Anmol Asrani |
| [HDFS-16169](https://issues.apache.org/jira/browse/HDFS-16169) | Fix TestBlockTokenWithDFSStriped#testEnd2End failure |  Major | test | Hui Fei | secfree |
| [HADOOP-18091](https://issues.apache.org/jira/browse/HADOOP-18091) | S3A auditing leaks memory through ThreadLocal references |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-18071](https://issues.apache.org/jira/browse/HADOOP-18071) | ABFS: Set driver global timeout for ITestAzureBlobFileSystemBasics |  Major | fs/azure | Sumangala Patki | Sumangala Patki |
| [HADOOP-17765](https://issues.apache.org/jira/browse/HADOOP-17765) | ABFS: Use Unique File Paths in Tests |  Major | fs/azure | Sumangala Patki | Sumangala Patki |
| [HADOOP-17862](https://issues.apache.org/jira/browse/HADOOP-17862) | ABFS: Fix unchecked cast compiler warning for AbfsListStatusRemoteIterator |  Major | fs/azure | Sumangala Patki | Sumangala Patki |
| [HADOOP-18075](https://issues.apache.org/jira/browse/HADOOP-18075) | ABFS: Fix failure caused by listFiles() in ITestAbfsRestOperationException |  Major | fs/azure | Sumangala Patki | Sumangala Patki |
| [HADOOP-18112](https://issues.apache.org/jira/browse/HADOOP-18112) | Implement paging during S3 multi object delete. |  Critical | fs/s3 | Mukund Thakur | Mukund Thakur |
| [HADOOP-16204](https://issues.apache.org/jira/browse/HADOOP-16204) | ABFS tests to include terasort |  Minor | fs/azure, test | Steve Loughran | Steve Loughran |
| [HDFS-13248](https://issues.apache.org/jira/browse/HDFS-13248) | RBF: Namenode need to choose block location for the client |  Major | . | Wu Weiwei | Owen O'Malley |
| [HADOOP-13704](https://issues.apache.org/jira/browse/HADOOP-13704) | S3A getContentSummary() to move to listFiles(recursive) to count children; instrument use |  Minor | fs/s3 | Steve Loughran | Ahmar Suhail |
| [HADOOP-14661](https://issues.apache.org/jira/browse/HADOOP-14661) | S3A to support Requester Pays Buckets |  Minor | common, util | Mandus Momberg | Daniel Carl Jones |
| [HDFS-16484](https://issues.apache.org/jira/browse/HDFS-16484) | [SPS]: Fix an infinite loop bug in SPSPathIdProcessor thread |  Major | . | qinyuren | qinyuren |
| [HADOOP-17682](https://issues.apache.org/jira/browse/HADOOP-17682) | ABFS: Support FileStatus input to OpenFileWithOptions() via OpenFileParameters |  Major | fs/azure | Sumangala Patki | Sumangala Patki |
| [HADOOP-15983](https://issues.apache.org/jira/browse/HADOOP-15983) | Use jersey-json that is built to use jackson2 |  Major | build | Akira Ajisaka | PJ Fanning |
| [HADOOP-18104](https://issues.apache.org/jira/browse/HADOOP-18104) | Add configs to configure minSeekForVectorReads and maxReadSizeForVectorReads |  Major | common, fs | Mukund Thakur | Mukund Thakur |
| [HADOOP-18168](https://issues.apache.org/jira/browse/HADOOP-18168) | ITestMarkerTool.testRunLimitedLandsatAudit failing due to most of bucket content purged |  Minor | fs/s3, test | Steve Loughran | Daniel Carl Jones |
| [HADOOP-12020](https://issues.apache.org/jira/browse/HADOOP-12020) | Support configuration of different S3 storage classes |  Major | fs/s3 | Yann Landrin-Schweitzer | Monthon Klongklaew |
| [HADOOP-18105](https://issues.apache.org/jira/browse/HADOOP-18105) | Implement a variant of ElasticByteBufferPool which uses weak references for garbage collection. |  Major | common, fs | Mukund Thakur | Mukund Thakur |
| [HADOOP-18107](https://issues.apache.org/jira/browse/HADOOP-18107) | Vectored IO support for large S3 files. |  Major | fs/s3 | Mukund Thakur | Mukund Thakur |
| [HADOOP-18106](https://issues.apache.org/jira/browse/HADOOP-18106) | Handle memory fragmentation in S3 Vectored IO implementation. |  Major | fs/s3 | Mukund Thakur | Mukund Thakur |
| [HADOOP-17461](https://issues.apache.org/jira/browse/HADOOP-17461) | Add thread-level IOStatistics Context |  Major | fs, fs/azure, fs/s3 | Steve Loughran | Mehakmeet Singh |
| [HADOOP-18372](https://issues.apache.org/jira/browse/HADOOP-18372) | ILoadTestS3ABulkDeleteThrottling failing |  Minor | fs/s3, test | Steve Loughran | Ahmar Suhail |
| [HADOOP-18368](https://issues.apache.org/jira/browse/HADOOP-18368) | ITestCustomSigner fails when access point name has '-' |  Minor | . | Ahmar Suhail | Ahmar Suhail |
| [HADOOP-15964](https://issues.apache.org/jira/browse/HADOOP-15964) | Add S3A support for Async Scatter/Gather IO |  Major | fs/s3 | Steve Loughran | Mukund Thakur |
| [HADOOP-18366](https://issues.apache.org/jira/browse/HADOOP-18366) | ITestS3Select.testSelectSeekFullLandsat is timing out |  Minor | . | Ahmar Suhail | Ahmar Suhail |
| [HADOOP-18373](https://issues.apache.org/jira/browse/HADOOP-18373) | IOStatisticsContext tuning |  Minor | fs/s3, test | Steve Loughran | Viraj Jasani |
| [HADOOP-18227](https://issues.apache.org/jira/browse/HADOOP-18227) | Add input stream IOstats for vectored IO api in S3A. |  Major | fs/s3 | Mukund Thakur | Mukund Thakur |
| [HADOOP-18392](https://issues.apache.org/jira/browse/HADOOP-18392) | Propagate vectored s3a input stream stats to file system stats. |  Major | fs/s3 | Mukund Thakur | Mukund Thakur |
| [HADOOP-18355](https://issues.apache.org/jira/browse/HADOOP-18355) | Update previous index properly while validating overlapping ranges. |  Major | common, fs/s3 | Mukund Thakur | Mukund Thakur |
| [HADOOP-18371](https://issues.apache.org/jira/browse/HADOOP-18371) | s3a FS init logs at warn if fs.s3a.create.storage.class is unset |  Blocker | fs/s3 | Steve Loughran | Viraj Jasani |
| [HADOOP-18385](https://issues.apache.org/jira/browse/HADOOP-18385) | ITestS3ACannedACLs failure; not in a span |  Major | fs/s3, test | Steve Loughran | Ashutosh Gupta |
| [HADOOP-18403](https://issues.apache.org/jira/browse/HADOOP-18403) | Fix FileSystem leak in ITestS3AAWSCredentialsProvider |  Minor | fs/s3 | Viraj Jasani | Viraj Jasani |
| [HADOOP-17882](https://issues.apache.org/jira/browse/HADOOP-17882) | distcp to use openFile() with sequential IO; ranges of reads |  Major | tools/distcp | Steve Loughran | Steve Loughran |
| [HADOOP-18391](https://issues.apache.org/jira/browse/HADOOP-18391) | Improve VectoredReadUtils#readVectored() for direct buffers |  Major | fs | Steve Loughran | Mukund Thakur |
| [HADOOP-18407](https://issues.apache.org/jira/browse/HADOOP-18407) | Improve vectored IO api spec. |  Minor | fs, fs/s3 | Mukund Thakur | Mukund Thakur |
| [HADOOP-18339](https://issues.apache.org/jira/browse/HADOOP-18339) | S3A storage class option only picked up when buffering writes to disk |  Major | fs/s3 | Steve Loughran | Monthon Klongklaew |
| [HADOOP-18410](https://issues.apache.org/jira/browse/HADOOP-18410) | S3AInputStream.unbuffer() async drain not releasing http connections |  Blocker | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-18439](https://issues.apache.org/jira/browse/HADOOP-18439) | Fix VectoredIO for LocalFileSystem when checksum is enabled. |  Major | common | Mukund Thakur | Mukund Thakur |
| [HADOOP-18416](https://issues.apache.org/jira/browse/HADOOP-18416) | ITestS3AIOStatisticsContext failure |  Major | fs/s3, test | Steve Loughran | Mehakmeet Singh |
| [HADOOP-18347](https://issues.apache.org/jira/browse/HADOOP-18347) | Restrict vectoredIO threadpool to reduce memory pressure |  Major | common, fs, fs/adl, fs/s3 | Rajesh Balamohan | Mukund Thakur |
| [HADOOP-18463](https://issues.apache.org/jira/browse/HADOOP-18463) | Add an integration test to process data asynchronously during vectored read. |  Major | . | Mukund Thakur | Mukund Thakur |
| [HADOOP-15460](https://issues.apache.org/jira/browse/HADOOP-15460) | S3A FS to add  "fs.s3a.create.performance" to the builder file creation option set |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-18382](https://issues.apache.org/jira/browse/HADOOP-18382) | Upgrade AWS SDK to V2 - Prerequisites |  Minor | . | Ahmar Suhail | Ahmar Suhail |
| [HADOOP-18480](https://issues.apache.org/jira/browse/HADOOP-18480) | upgrade  AWS SDK to 1.12.316 |  Major | build, fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-18460](https://issues.apache.org/jira/browse/HADOOP-18460) | ITestS3AContractVectoredRead.testStopVectoredIoOperationsUnbuffer failing |  Minor | fs/s3, test | Steve Loughran | Mukund Thakur |
| [HADOOP-18488](https://issues.apache.org/jira/browse/HADOOP-18488) | Cherrypick HADOOP-11245 to branch-3.3 |  Major | . | Wei-Chiu Chuang | Ashutosh Gupta |
| [HADOOP-18481](https://issues.apache.org/jira/browse/HADOOP-18481) | AWS v2 SDK upgrade log to not warn of use standard AWS Credential Providers |  Major | fs/s3 | Steve Loughran | Ahmar Suhail |
| [HADOOP-18476](https://issues.apache.org/jira/browse/HADOOP-18476) | Abfs and S3A FileContext bindings to close wrapped filesystems in finalizer |  Blocker | fs/azure, fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-18304](https://issues.apache.org/jira/browse/HADOOP-18304) | Improve S3A committers documentation clarity |  Trivial | documentation | Daniel Carl Jones | Daniel Carl Jones |
| [HADOOP-18465](https://issues.apache.org/jira/browse/HADOOP-18465) | S3A server-side encryption tests fail before checking encryption tests should skip |  Minor | fs/s3, test | Daniel Carl Jones | Daniel Carl Jones |
| [HADOOP-18530](https://issues.apache.org/jira/browse/HADOOP-18530) | ChecksumFileSystem::readVectored might return byte buffers not positioned at 0 |  Blocker | fs | Harshit Gupta | Harshit Gupta |
| [HADOOP-18457](https://issues.apache.org/jira/browse/HADOOP-18457) | ABFS: Support for account level throttling |  Major | . | Anmol Asrani | Anmol Asrani |
| [HADOOP-18560](https://issues.apache.org/jira/browse/HADOOP-18560) | AvroFSInput opens a stream twice and discards the second one without closing |  Blocker | fs | Steve Loughran | Steve Loughran |
| [HADOOP-18526](https://issues.apache.org/jira/browse/HADOOP-18526) | Leak of S3AInstrumentation instances via hadoop Metrics references |  Blocker | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-18546](https://issues.apache.org/jira/browse/HADOOP-18546) | disable purging list of in progress reads in abfs stream closed |  Blocker | fs/azure | Steve Loughran | Pranav Saxena |
| [HADOOP-18577](https://issues.apache.org/jira/browse/HADOOP-18577) | ABFS: add probes of readahead fix |  Major | fs/azure | Steve Loughran | Steve Loughran |
| [HADOOP-11867](https://issues.apache.org/jira/browse/HADOOP-11867) | Add a high-performance vectored read API. |  Major | fs, fs/azure, fs/s3, hdfs-client | Gopal Vijayaraghavan | Mukund Thakur |
| [HADOOP-18507](https://issues.apache.org/jira/browse/HADOOP-18507) | VectorIO FileRange type to support a "reference" field |  Major | fs | Steve Loughran | Steve Loughran |
| [HADOOP-18627](https://issues.apache.org/jira/browse/HADOOP-18627) | site intro docs to make clear Kerberos is mandatory for secure clusters |  Major | site | Steve Loughran | Arnout Engelen |
| [HADOOP-17584](https://issues.apache.org/jira/browse/HADOOP-17584) | s3a magic committer may commit more data |  Major | fs/s3 | yinan zhan | Steve Loughran |
| [HADOOP-18642](https://issues.apache.org/jira/browse/HADOOP-18642) | Cut excess dependencies from hadoop-azure, hadoop-aliyun transitive imports; fix LICENSE-binary |  Blocker | build, fs/azure, fs/oss | Steve Loughran | Steve Loughran |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-15854](https://issues.apache.org/jira/browse/HDFS-15854) | Make some parameters configurable for SlowDiskTracker and SlowPeerTracker |  Major | . | Tao Li | Tao Li |
| [YARN-10747](https://issues.apache.org/jira/browse/YARN-10747) | Bump YARN CSI protobuf version to 3.7.1 |  Major | . | Siyao Meng | Siyao Meng |
| [HDFS-16139](https://issues.apache.org/jira/browse/HDFS-16139) | Update BPServiceActor Scheduler's nextBlockReportTime atomically |  Major | . | Viraj Jasani | Viraj Jasani |
| [HADOOP-18014](https://issues.apache.org/jira/browse/HADOOP-18014) | CallerContext should not include some characters |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [MAPREDUCE-7371](https://issues.apache.org/jira/browse/MAPREDUCE-7371) | DistributedCache alternative APIs should not use DistributedCache APIs internally |  Major | . | Viraj Jasani | Viraj Jasani |
| [HADOOP-18114](https://issues.apache.org/jira/browse/HADOOP-18114) | Documentation Syntax Error Fix \> AWS Assumed Roles |  Trivial | documentation, fs/s3 | Joey Krabacher | Joey Krabacher |
| [HDFS-16481](https://issues.apache.org/jira/browse/HDFS-16481) | Provide support to set Http and Rpc ports in MiniJournalCluster |  Major | . | Viraj Jasani | Viraj Jasani |
| [HDFS-16502](https://issues.apache.org/jira/browse/HDFS-16502) | Reconfigure Block Invalidate limit |  Major | . | Viraj Jasani | Viraj Jasani |
| [HDFS-16522](https://issues.apache.org/jira/browse/HDFS-16522) | Set Http and Ipc ports for Datanodes in MiniDFSCluster |  Major | . | Viraj Jasani | Viraj Jasani |
| [HADOOP-18191](https://issues.apache.org/jira/browse/HADOOP-18191) | Log retry count while handling exceptions in RetryInvocationHandler |  Minor | . | Viraj Jasani | Viraj Jasani |
| [HDFS-16551](https://issues.apache.org/jira/browse/HDFS-16551) | Backport HADOOP-17588 to 3.3 and other active old branches. |  Major | . | Renukaprasad C | Renukaprasad C |
| [HDFS-16618](https://issues.apache.org/jira/browse/HDFS-16618) | sync\_file\_range error should include more volume and file info |  Minor | . | Viraj Jasani | Viraj Jasani |
| [HADOOP-18300](https://issues.apache.org/jira/browse/HADOOP-18300) | Update google-gson to 2.9.0 |  Minor | build | Igor Dvorzhak | Igor Dvorzhak |
| [HADOOP-18397](https://issues.apache.org/jira/browse/HADOOP-18397) | Shutdown AWSSecurityTokenService when its resources are no longer in use |  Major | fs/s3 | Viraj Jasani | Viraj Jasani |
| [HADOOP-18575](https://issues.apache.org/jira/browse/HADOOP-18575) | Make XML transformer factory more lenient |  Major | common | PJ Fanning | PJ Fanning |
| [HADOOP-18586](https://issues.apache.org/jira/browse/HADOOP-18586) | Update the year to 2023 |  Major | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-18587](https://issues.apache.org/jira/browse/HADOOP-18587) | upgrade to jettison 1.5.3 to fix CVE-2022-40150 |  Major | common | PJ Fanning | PJ Fanning |


