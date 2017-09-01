
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

## Release 2.8.2 - Unreleased (as of 2017-08-28)

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-14174](https://issues.apache.org/jira/browse/HADOOP-14174) | Set default ADLS access token provider type to ClientCredential |  Major | fs/adl | John Zhuge | John Zhuge |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-7764](https://issues.apache.org/jira/browse/HDFS-7764) | DirectoryScanner shouldn't abort the scan if one directory had an error |  Major | datanode | Rakesh R | Rakesh R |
| [HDFS-7541](https://issues.apache.org/jira/browse/HDFS-7541) | Upgrade Domains in HDFS |  Major | . | Ming Ma | Ming Ma |
| [HDFS-10683](https://issues.apache.org/jira/browse/HDFS-10683) | Make class Token$PrivateToken private |  Minor | . | John Zhuge | John Zhuge |
| [HADOOP-13688](https://issues.apache.org/jira/browse/HADOOP-13688) | Stop bundling HTML source code in javadoc JARs |  Major | build | Andrew Wang | Andrew Wang |
| [HADOOP-13737](https://issues.apache.org/jira/browse/HADOOP-13737) | Cleanup DiskChecker interface |  Major | util | Arpit Agarwal | Arpit Agarwal |
| [HADOOP-13496](https://issues.apache.org/jira/browse/HADOOP-13496) | Include file lengths in Mismatch in length error for distcp |  Minor | . | Ted Yu | Ted Yu |
| [HDFS-10534](https://issues.apache.org/jira/browse/HDFS-10534) | NameNode WebUI should display DataNode usage histogram |  Major | namenode, ui | Zhe Zhang | Kai Sasaki |
| [HADOOP-14050](https://issues.apache.org/jira/browse/HADOOP-14050) | Add process name to kms process |  Minor | kms, scripts | Rushabh S Shah | Rushabh S Shah |
| [HDFS-11390](https://issues.apache.org/jira/browse/HDFS-11390) | Add process name to httpfs process |  Minor | httpfs, scripts | John Zhuge | Weiwei Yang |
| [HDFS-11333](https://issues.apache.org/jira/browse/HDFS-11333) | Print a user friendly error message when plugins are not found |  Minor | namenode | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-11466](https://issues.apache.org/jira/browse/HDFS-11466) | Change dfs.namenode.write-lock-reporting-threshold-ms default from 1000ms to 5000ms |  Major | namenode | Andrew Wang | Andrew Wang |
| [HDFS-11432](https://issues.apache.org/jira/browse/HDFS-11432) | Federation : Support fully qualified path for Quota/Snapshot/cacheadmin/cryptoadmin commands |  Major | federation | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-14169](https://issues.apache.org/jira/browse/HADOOP-14169) | Implement listStatusIterator, listLocatedStatus for ViewFs |  Minor | viewfs | Erik Krogen | Erik Krogen |
| [HADOOP-14233](https://issues.apache.org/jira/browse/HADOOP-14233) | Delay construction of PreCondition.check failure message in Configuration#set |  Major | . | Jonathan Eagles | Jonathan Eagles |
| [HADOOP-14240](https://issues.apache.org/jira/browse/HADOOP-14240) | Configuration#get return value optimization |  Major | . | Jonathan Eagles | Jonathan Eagles |
| [YARN-6339](https://issues.apache.org/jira/browse/YARN-6339) | Improve performance for createAndGetApplicationReport |  Major | . | yunjiong zhao | yunjiong zhao |
| [HDFS-9705](https://issues.apache.org/jira/browse/HDFS-9705) | Refine the behaviour of getFileChecksum when length = 0 |  Minor | . | Kai Zheng | SammiChen |
| [HDFS-11628](https://issues.apache.org/jira/browse/HDFS-11628) | Clarify the behavior of HDFS Mover in documentation |  Major | documentation | Xiaobing Zhou | Xiaobing Zhou |
| [HADOOP-14104](https://issues.apache.org/jira/browse/HADOOP-14104) | Client should always ask namenode for kms provider path. |  Major | kms | Rushabh S Shah | Rushabh S Shah |
| [HADOOP-14276](https://issues.apache.org/jira/browse/HADOOP-14276) | Add a nanosecond API to Time/Timer/FakeTimer |  Minor | util | Erik Krogen | Erik Krogen |
| [HDFS-11558](https://issues.apache.org/jira/browse/HDFS-11558) | BPServiceActor thread name is too long |  Minor | datanode | Tsz Wo Nicholas Sze | Xiaobing Zhou |
| [HDFS-11648](https://issues.apache.org/jira/browse/HDFS-11648) | Lazy construct the IIP pathname |  Major | . | Daryn Sharp | Daryn Sharp |
| [HDFS-11634](https://issues.apache.org/jira/browse/HDFS-11634) | Optimize BlockIterator when iterating starts in the middle. |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-11384](https://issues.apache.org/jira/browse/HDFS-11384) | Add option for balancer to disperse getBlocks calls to avoid NameNode's rpc.CallQueueLength spike |  Major | balancer & mover | yunjiong zhao | Konstantin Shvachko |
| [YARN-6457](https://issues.apache.org/jira/browse/YARN-6457) | Allow custom SSL configuration to be supplied in WebApps |  Major | webapp, yarn | Sanjay M Pujare | Sanjay M Pujare |
| [HDFS-11641](https://issues.apache.org/jira/browse/HDFS-11641) | Reduce cost of audit logging by using FileStatus instead of HdfsFileStatus |  Major | hdfs | Daryn Sharp | Daryn Sharp |
| [YARN-6493](https://issues.apache.org/jira/browse/YARN-6493) | Print requested node partition in assignContainer logs |  Major | . | Jonathan Hung | Jonathan Hung |
| [HDFS-11891](https://issues.apache.org/jira/browse/HDFS-11891) | DU#refresh should print the path of the directory when an exception is caught |  Minor | . | Chen Liang | Chen Liang |
| [HADOOP-14440](https://issues.apache.org/jira/browse/HADOOP-14440) | Add metrics for connections dropped |  Major | . | Eric Badger | Eric Badger |
| [HDFS-11861](https://issues.apache.org/jira/browse/HDFS-11861) | ipc.Client.Connection#sendRpcRequest should log request name |  Trivial | ipc | John Zhuge | John Zhuge |
| [HDFS-11345](https://issues.apache.org/jira/browse/HDFS-11345) | Document the configuration key for FSNamesystem lock fairness |  Minor | documentation, namenode | Zhe Zhang | Erik Krogen |
| [YARN-6738](https://issues.apache.org/jira/browse/YARN-6738) | LevelDBCacheTimelineStore should reuse ObjectMapper instances |  Major | timelineserver | Zoltan Haindrich | Zoltan Haindrich |
| [HADOOP-14515](https://issues.apache.org/jira/browse/HADOOP-14515) | Specifically configure zookeeper-related log levels in KMS log4j |  Major | kms | Xiao Chen | Xiao Chen |
| [HDFS-11881](https://issues.apache.org/jira/browse/HDFS-11881) | NameNode consumes a lot of memory for snapshot diff report generation |  Major | hdfs, snapshots | Manoj Govindassamy | Manoj Govindassamy |
| [HDFS-12042](https://issues.apache.org/jira/browse/HDFS-12042) | Lazy initialize AbstractINodeDiffList#diffs for snapshots to reduce memory consumption |  Major | . | Misha Dmitriev | Misha Dmitriev |
| [HDFS-12078](https://issues.apache.org/jira/browse/HDFS-12078) | Add time unit to the description of property dfs.namenode.stale.datanode.interval in hdfs-default.xml |  Minor | documentation, hdfs | Weiwei Yang | Weiwei Yang |
| [YARN-6764](https://issues.apache.org/jira/browse/YARN-6764) | Simplify the logic in FairScheduler#attemptScheduling |  Trivial | fairscheduler | Yufei Gu | Yufei Gu |
| [HADOOP-14629](https://issues.apache.org/jira/browse/HADOOP-14629) | Improve exception checking in FileContext related JUnit tests |  Major | fs, test | Andras Bokor | Andras Bokor |
| [HDFS-12137](https://issues.apache.org/jira/browse/HDFS-12137) | DN dataset lock should be fair |  Critical | datanode | Daryn Sharp | Daryn Sharp |
| [HADOOP-14521](https://issues.apache.org/jira/browse/HADOOP-14521) | KMS client needs retry logic |  Major | . | Rushabh S Shah | Rushabh S Shah |
| [HADOOP-14659](https://issues.apache.org/jira/browse/HADOOP-14659) | UGI getShortUserName does not need to search the Subject |  Major | common | Daryn Sharp | Daryn Sharp |
| [YARN-6768](https://issues.apache.org/jira/browse/YARN-6768) | Improve performance of yarn api record toString and fromString |  Major | . | Jonathan Eagles | Jonathan Eagles |
| [YARN-5892](https://issues.apache.org/jira/browse/YARN-5892) | Support user-specific minimum user limit percentage in Capacity Scheduler |  Major | capacityscheduler | Eric Payne | Eric Payne |
| [YARN-6917](https://issues.apache.org/jira/browse/YARN-6917) | Queue path is recomputed from scratch on every allocation |  Minor | capacityscheduler | Jason Lowe | Eric Payne |
| [HDFS-12301](https://issues.apache.org/jira/browse/HDFS-12301) | NN File Browser UI: Navigate to a path when enter is pressed |  Trivial | ui | Ravi Prakash | Ravi Prakash |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-524](https://issues.apache.org/jira/browse/YARN-524) | TestYarnVersionInfo failing if generated properties doesn't include an SVN URL |  Minor | api | Steve Loughran | Steve Loughran |
| [YARN-1471](https://issues.apache.org/jira/browse/YARN-1471) | The SLS simulator is not running the preemption policy for CapacityScheduler |  Minor | . | Carlo Curino | Carlo Curino |
| [HADOOP-11703](https://issues.apache.org/jira/browse/HADOOP-11703) | git should ignore .DS\_Store files on Mac OS X |  Major | . | Abin Shahab | Abin Shahab |
| [YARN-4612](https://issues.apache.org/jira/browse/YARN-4612) | Fix rumen and scheduler load simulator handle killed tasks properly |  Major | . | Ming Ma | Ming Ma |
| [YARN-4594](https://issues.apache.org/jira/browse/YARN-4594) | container-executor fails to remove directory tree when chmod required |  Major | nodemanager | Colin P. McCabe | Colin P. McCabe |
| [YARN-4731](https://issues.apache.org/jira/browse/YARN-4731) | container-executor should not follow symlinks in recursive\_unlink\_children |  Blocker | . | Bibin A Chundatt | Colin P. McCabe |
| [YARN-4812](https://issues.apache.org/jira/browse/YARN-4812) | TestFairScheduler#testContinuousScheduling fails intermittently |  Major | fairscheduler | Karthik Kambatla | Karthik Kambatla |
| [YARN-4927](https://issues.apache.org/jira/browse/YARN-4927) | TestRMHA#testTransitionedToActiveRefreshFail fails with FairScheduler |  Major | test | Karthik Kambatla | Bibin A Chundatt |
| [YARN-4562](https://issues.apache.org/jira/browse/YARN-4562) | YARN WebApp ignores the configuration passed to it for keystore settings |  Major | . | Sergey Shelukhin | Sergey Shelukhin |
| [HDFS-9276](https://issues.apache.org/jira/browse/HDFS-9276) | Failed to Update HDFS Delegation Token for long running application in HA mode |  Major | fs, ha, security | Liangliang Gu | Liangliang Gu |
| [YARN-5333](https://issues.apache.org/jira/browse/YARN-5333) | Some recovered apps are put into default queue when RM HA |  Major | . | Jun Gong | Jun Gong |
| [HADOOP-13437](https://issues.apache.org/jira/browse/HADOOP-13437) | KMS should reload whitelist and default key ACLs when hot-reloading |  Major | kms | Xiao Chen | Xiao Chen |
| [HDFS-8312](https://issues.apache.org/jira/browse/HDFS-8312) | Trash does not descent into child directories to check for permissions |  Critical | fs, security | Eric Yang | Weiwei Yang |
| [YARN-5920](https://issues.apache.org/jira/browse/YARN-5920) | Fix deadlock in TestRMHA.testTransitionedToStandbyShouldNotHang |  Major | test | Rohith Sharma K S | Varun Saxena |
| [HADOOP-13867](https://issues.apache.org/jira/browse/HADOOP-13867) | FilterFileSystem should override rename(.., options) to take effect of Rename options called via FilterFileSystem implementations |  Major | . | Vinayakumar B | Vinayakumar B |
| [HADOOP-13508](https://issues.apache.org/jira/browse/HADOOP-13508) | FsPermission string constructor does not recognize sticky bit |  Major | . | Atul Sikaria | Atul Sikaria |
| [YARN-5988](https://issues.apache.org/jira/browse/YARN-5988) | RM unable to start in secure setup |  Blocker | . | Ajith S | Ajith S |
| [YARN-6054](https://issues.apache.org/jira/browse/YARN-6054) | TimelineServer fails to start when some LevelDb state files are missing. |  Critical | . | Ravi Prakash | Ravi Prakash |
| [HADOOP-13928](https://issues.apache.org/jira/browse/HADOOP-13928) | TestAdlFileContextMainOperationsLive.testGetFileContext1 runtime error |  Major | fs/adl, test | John Zhuge | John Zhuge |
| [HADOOP-13976](https://issues.apache.org/jira/browse/HADOOP-13976) | Path globbing does not match newlines |  Major | . | Eric Badger | Eric Badger |
| [HDFS-11316](https://issues.apache.org/jira/browse/HDFS-11316) | TestDataNodeVolumeFailure#testUnderReplicationAfterVolFailure fails in trunk |  Minor | . | Yiqun Lin | Yiqun Lin |
| [HADOOP-14044](https://issues.apache.org/jira/browse/HADOOP-14044) | Synchronization issue in delegation token cancel functionality |  Major | . | Hrishikesh Gadre | Hrishikesh Gadre |
| [HDFS-11377](https://issues.apache.org/jira/browse/HDFS-11377) | Balancer hung due to no available mover threads |  Major | balancer & mover | yunjiong zhao | yunjiong zhao |
| [YARN-6031](https://issues.apache.org/jira/browse/YARN-6031) | Application recovery has failed when node label feature is turned off during RM recovery |  Minor | scheduler | Ying Zhang | Ying Zhang |
| [YARN-6137](https://issues.apache.org/jira/browse/YARN-6137) | Yarn client implicitly invoke ATS client which accesses HDFS |  Major | . | Yesha Vora | Li Lu |
| [HADOOP-13433](https://issues.apache.org/jira/browse/HADOOP-13433) | Race in UGI.reloginFromKeytab |  Major | security | Duo Zhang | Duo Zhang |
| [HADOOP-13119](https://issues.apache.org/jira/browse/HADOOP-13119) | Add ability to secure log servlet using proxy users |  Major | . | Jeffrey E  Rodriguez | Yuanbo Liu |
| [HADOOP-14058](https://issues.apache.org/jira/browse/HADOOP-14058) | Fix NativeS3FileSystemContractBaseTest#testDirWithDifferentMarkersWorks |  Major | fs/s3, test | Akira Ajisaka | Yiqun Lin |
| [HDFS-11084](https://issues.apache.org/jira/browse/HDFS-11084) | Add a regression test for sticky bit support of OIV ReverseXML processor |  Major | tools | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-11391](https://issues.apache.org/jira/browse/HDFS-11391) | Numeric usernames do no work with WebHDFS FS (write access) |  Major | webhdfs | Pierre Villard | Pierre Villard |
| [HDFS-11177](https://issues.apache.org/jira/browse/HDFS-11177) | 'storagepolicies -getStoragePolicy' command should accept URI based path. |  Major | shell | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-11404](https://issues.apache.org/jira/browse/HDFS-11404) | Increase timeout on TestShortCircuitLocalRead.testDeprecatedGetBlockLocalPathInfoRpc |  Major | . | Eric Badger | Eric Badger |
| [HADOOP-14114](https://issues.apache.org/jira/browse/HADOOP-14114) | S3A can no longer handle unencoded + in URIs |  Minor | fs/s3 | Sean Mackrory | Sean Mackrory |
| [MAPREDUCE-6841](https://issues.apache.org/jira/browse/MAPREDUCE-6841) | Fix dead link in MapReduce tutorial document |  Minor | documentation | Akira Ajisaka | Victor Nee |
| [MAPREDUCE-6753](https://issues.apache.org/jira/browse/MAPREDUCE-6753) | Variable in byte printed directly in mapreduce client |  Major | client | Nemo Chen | Kai Sasaki |
| [YARN-6263](https://issues.apache.org/jira/browse/YARN-6263) | NMTokenSecretManagerInRM.createAndGetNMToken is not thread safe |  Major | yarn | Haibo Chen | Haibo Chen |
| [HADOOP-14026](https://issues.apache.org/jira/browse/HADOOP-14026) | start-build-env.sh: invalid docker image name |  Major | build | Gergő Pásztor | Gergő Pásztor |
| [HDFS-11441](https://issues.apache.org/jira/browse/HDFS-11441) | Add escaping to error message in KMS web UI |  Minor | security | Aaron T. Myers | Aaron T. Myers |
| [MAPREDUCE-6855](https://issues.apache.org/jira/browse/MAPREDUCE-6855) | Specify charset when create String in CredentialsTestJob |  Minor | . | Akira Ajisaka | Kai Sasaki |
| [YARN-6165](https://issues.apache.org/jira/browse/YARN-6165) | Intra-queue preemption occurs even when preemption is turned off for a specific queue. |  Major | capacity scheduler, scheduler preemption | Eric Payne | Eric Payne |
| [YARN-6310](https://issues.apache.org/jira/browse/YARN-6310) | OutputStreams in AggregatedLogFormat.LogWriter can be left open upon exceptions |  Major | yarn | Haibo Chen | Haibo Chen |
| [YARN-6321](https://issues.apache.org/jira/browse/YARN-6321) | TestResources test timeouts are too aggressive |  Major | test | Jason Lowe | Eric Badger |
| [HDFS-11512](https://issues.apache.org/jira/browse/HDFS-11512) | Increase timeout on TestShortCircuitLocalRead#testSkipWithVerifyChecksum |  Minor | . | Eric Badger | Eric Badger |
| [HDFS-11499](https://issues.apache.org/jira/browse/HDFS-11499) | Decommissioning stuck because of failing recovery |  Major | hdfs, namenode | Lukas Majercak | Lukas Majercak |
| [HDFS-11395](https://issues.apache.org/jira/browse/HDFS-11395) | RequestHedgingProxyProvider#RequestHedgingInvocationHandler hides the Exception thrown from NameNode |  Major | ha | Nandakumar | Nandakumar |
| [YARN-4051](https://issues.apache.org/jira/browse/YARN-4051) | ContainerKillEvent lost when container is still recovering and application finishes |  Critical | nodemanager | sandflee | sandflee |
| [YARN-6217](https://issues.apache.org/jira/browse/YARN-6217) | TestLocalCacheDirectoryManager test timeout is too aggressive |  Major | test | Jason Lowe | Miklos Szegedi |
| [HDFS-11132](https://issues.apache.org/jira/browse/HDFS-11132) | Allow AccessControlException in contract tests when getFileStatus on subdirectory of existing files |  Major | fs/adl, test | Vishwajeet Dusane | Vishwajeet Dusane |
| [HADOOP-14204](https://issues.apache.org/jira/browse/HADOOP-14204) | S3A multipart commit failing, "UnsupportedOperationException at java.util.Collections$UnmodifiableList.sort" |  Critical | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-14205](https://issues.apache.org/jira/browse/HADOOP-14205) | No FileSystem for scheme: adl |  Major | fs/adl | John Zhuge | John Zhuge |
| [HDFS-11561](https://issues.apache.org/jira/browse/HDFS-11561) | HttpFS doc errors |  Trivial | documentation, httpfs, test | Yuanbo Liu | Yuanbo Liu |
| [HADOOP-9631](https://issues.apache.org/jira/browse/HADOOP-9631) | ViewFs should use underlying FileSystem's server side defaults |  Major | fs, viewfs | Lohit Vijayarenu | Erik Krogen |
| [HADOOP-14214](https://issues.apache.org/jira/browse/HADOOP-14214) | DomainSocketWatcher::add()/delete() should not self interrupt while looping await() |  Critical | hdfs-client | Mingliang Liu | Mingliang Liu |
| [HADOOP-14195](https://issues.apache.org/jira/browse/HADOOP-14195) | CredentialProviderFactory$getProviders is not thread-safe |  Major | security | Vihang Karajgaonkar | Vihang Karajgaonkar |
| [HADOOP-14211](https://issues.apache.org/jira/browse/HADOOP-14211) | FilterFs and ChRootedFs are too aggressive about enforcing "authorityNeeded" |  Major | viewfs | Erik Krogen | Erik Krogen |
| [MAPREDUCE-6866](https://issues.apache.org/jira/browse/MAPREDUCE-6866) | Fix getNumMapTasks() documentation in JobConf |  Minor | documentation | Joe Mészáros | Joe Mészáros |
| [MAPREDUCE-6868](https://issues.apache.org/jira/browse/MAPREDUCE-6868) | License check for jdiff output files should be ignored |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HDFS-10506](https://issues.apache.org/jira/browse/HDFS-10506) | OIV's ReverseXML processor cannot reconstruct some snapshot details |  Major | tools | Colin P. McCabe | Akira Ajisaka |
| [HDFS-11486](https://issues.apache.org/jira/browse/HDFS-11486) | Client close() should not fail fast if the last block is being decommissioned |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-6359](https://issues.apache.org/jira/browse/YARN-6359) | TestRM#testApplicationKillAtAcceptedState fails rarely due to race condition |  Major | test | Robert Kanter | Robert Kanter |
| [YARN-5368](https://issues.apache.org/jira/browse/YARN-5368) | Memory leak in timeline server |  Critical | timelineserver | Wataru Yukawa | Jonathan Eagles |
| [HADOOP-14247](https://issues.apache.org/jira/browse/HADOOP-14247) | FileContextMainOperationsBaseTest should clean up test root path |  Minor | fs, test | Mingliang Liu | Mingliang Liu |
| [YARN-6352](https://issues.apache.org/jira/browse/YARN-6352) | Header injections are possible in application proxy servlet |  Major | resourcemanager, security | Naganarasimha G R | Naganarasimha G R |
| [MAPREDUCE-6873](https://issues.apache.org/jira/browse/MAPREDUCE-6873) | MR Job Submission Fails if MR framework application path not on defaultFS |  Minor | mrv2 | Erik Krogen | Erik Krogen |
| [HADOOP-14256](https://issues.apache.org/jira/browse/HADOOP-14256) | [S3A DOC] Correct the format for "Seoul" example |  Minor | documentation, s3 | Brahma Reddy Battula | Brahma Reddy Battula |
| [MAPREDUCE-6850](https://issues.apache.org/jira/browse/MAPREDUCE-6850) | Shuffle Handler keep-alive connections are closed from the server side |  Major | . | Jonathan Eagles | Jonathan Eagles |
| [HDFS-11592](https://issues.apache.org/jira/browse/HDFS-11592) | Closing a file has a wasteful preconditions in NameNode |  Major | namenode | Eric Badger | Eric Badger |
| [YARN-6354](https://issues.apache.org/jira/browse/YARN-6354) | LeveldbRMStateStore can parse invalid keys when recovering reservations |  Major | resourcemanager | Jason Lowe | Jason Lowe |
| [YARN-5703](https://issues.apache.org/jira/browse/YARN-5703) | ReservationAgents are not correctly configured |  Major | capacity scheduler, resourcemanager | Sean Po | Manikandan R |
| [HADOOP-14268](https://issues.apache.org/jira/browse/HADOOP-14268) | Fix markdown itemization in hadoop-aws documents |  Minor | documentation, fs/s3 | Akira Ajisaka | Akira Ajisaka |
| [YARN-6436](https://issues.apache.org/jira/browse/YARN-6436) | TestSchedulingPolicy#testParseSchedulingPolicy timeout is too low |  Major | test | Jason Lowe | Eric Badger |
| [YARN-6420](https://issues.apache.org/jira/browse/YARN-6420) | RM startup failure due to wrong order in nodelabel editlog |  Critical | . | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-6403](https://issues.apache.org/jira/browse/YARN-6403) | Invalid local resource request can raise NPE and make NM exit |  Major | nodemanager | Tao Yang | Tao Yang |
| [HDFS-11538](https://issues.apache.org/jira/browse/HDFS-11538) | Move ClientProtocol HA proxies into hadoop-hdfs-client |  Blocker | hdfs-client | Andrew Wang | Huafeng Wang |
| [YARN-6437](https://issues.apache.org/jira/browse/YARN-6437) | TestSignalContainer#testSignalRequestDeliveryToNM fails intermittently |  Major | test | Jason Lowe | Jason Lowe |
| [HDFS-11629](https://issues.apache.org/jira/browse/HDFS-11629) | Revert HDFS-11431 hadoop-hdfs-client JAR does not include ConfiguredFailoverProxyProvider. |  Major | . | Andrew Wang | Andrew Wang |
| [HADOOP-13996](https://issues.apache.org/jira/browse/HADOOP-13996) | Fix some release build issues |  Blocker | build | Andrew Wang | Andrew Wang |
| [HDFS-11608](https://issues.apache.org/jira/browse/HDFS-11608) | HDFS write crashed with block size greater than 2 GB |  Critical | hdfs-client | Xiaobing Zhou | Xiaobing Zhou |
| [YARN-6288](https://issues.apache.org/jira/browse/YARN-6288) | Exceptions during aggregated log writes are mishandled |  Critical | log-aggregation | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-14066](https://issues.apache.org/jira/browse/HADOOP-14066) | VersionInfo should be marked as public API |  Critical | common | Thejas M Nair | Akira Ajisaka |
| [HADOOP-14293](https://issues.apache.org/jira/browse/HADOOP-14293) | Initialize FakeTimer with a less trivial value |  Major | test | Andrew Wang | Andrew Wang |
| [YARN-6461](https://issues.apache.org/jira/browse/YARN-6461) | TestRMAdminCLI has very low test timeouts |  Major | test | Jason Lowe | Eric Badger |
| [YARN-6463](https://issues.apache.org/jira/browse/YARN-6463) | correct spelling mistake in FileSystemRMStateStore |  Trivial | . | Yeliang Cang | Yeliang Cang |
| [HDFS-11163](https://issues.apache.org/jira/browse/HDFS-11163) | Mover should move the file blocks to default storage once policy is unset |  Major | balancer & mover | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [YARN-6450](https://issues.apache.org/jira/browse/YARN-6450) | TestContainerManagerWithLCE requires override for each new test added to ContainerManagerTest |  Major | test | Jason Lowe | Jason Lowe |
| [YARN-3760](https://issues.apache.org/jira/browse/YARN-3760) | FSDataOutputStream leak in AggregatedLogFormat.LogWriter.close() |  Critical | nodemanager | Daryn Sharp | Haibo Chen |
| [YARN-5994](https://issues.apache.org/jira/browse/YARN-5994) | TestCapacityScheduler.testAMLimitUsage fails intermittently |  Major | . | Eric Badger | Eric Badger |
| [YARN-6480](https://issues.apache.org/jira/browse/YARN-6480) | Timeout is too aggressive for TestAMRestart.testPreemptedAMRestartOnRMRestart |  Major | . | Eric Badger | Eric Badger |
| [HADOOP-14311](https://issues.apache.org/jira/browse/HADOOP-14311) | Add python2.7-dev to Dockerfile |  Major | . | Allen Wittenauer | Allen Wittenauer |
| [YARN-6304](https://issues.apache.org/jira/browse/YARN-6304) | Skip rm.transitionToActive call to RM if RM is already active. |  Major | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-11615](https://issues.apache.org/jira/browse/HDFS-11615) | FSNamesystemLock metrics can be inaccurate due to millisecond precision |  Major | hdfs | Erik Krogen | Erik Krogen |
| [HADOOP-14318](https://issues.apache.org/jira/browse/HADOOP-14318) | Remove non-existent setfattr command option from FileSystemShell.md |  Minor | documentation | Doris Gu | Doris Gu |
| [HADOOP-14315](https://issues.apache.org/jira/browse/HADOOP-14315) | Python example in the rack awareness document doesn't work due to bad indentation |  Minor | documentation | Kengo Seki | Kengo Seki |
| [HDFS-11689](https://issues.apache.org/jira/browse/HDFS-11689) | New exception thrown by DFSClient#isHDFSEncryptionEnabled broke hacky hive code |  Major | . | Yongjun Zhang | Yongjun Zhang |
| [HDFS-11691](https://issues.apache.org/jira/browse/HDFS-11691) | Add a proper scheme to the datanode links in NN web UI |  Major | . | Kihwal Lee | Kihwal Lee |
| [HADOOP-14341](https://issues.apache.org/jira/browse/HADOOP-14341) | Support multi-line value for ssl.server.exclude.cipher.list |  Major | . | John Zhuge | John Zhuge |
| [YARN-5617](https://issues.apache.org/jira/browse/YARN-5617) | AMs only intended to run one attempt can be run more than once |  Major | resourcemanager | Jason Lowe | Jason Lowe |
| [HADOOP-14346](https://issues.apache.org/jira/browse/HADOOP-14346) | CryptoOutputStream throws IOException on flush() if stream is closed |  Major | . | Pierre Lacave | Pierre Lacave |
| [HDFS-11709](https://issues.apache.org/jira/browse/HDFS-11709) | StandbyCheckpointer should handle an non-existing legacyOivImageDir gracefully |  Critical | ha, namenode | Zhe Zhang | Erik Krogen |
| [YARN-5894](https://issues.apache.org/jira/browse/YARN-5894) | fixed license warning caused by de.ruedigermoeller:fst:jar:2.24 |  Blocker | yarn | Haibo Chen | Haibo Chen |
| [HADOOP-14320](https://issues.apache.org/jira/browse/HADOOP-14320) | TestIPC.testIpcWithReaderQueuing fails intermittently |  Major | . | Eric Badger | Eric Badger |
| [YARN-6536](https://issues.apache.org/jira/browse/YARN-6536) | TestAMRMClient.testAMRMClientWithSaslEncryption fails intermittently |  Major | . | Eric Badger | Jason Lowe |
| [HDFS-11609](https://issues.apache.org/jira/browse/HDFS-11609) | Some blocks can be permanently lost if nodes are decommissioned while dead |  Blocker | namenode | Kihwal Lee | Kihwal Lee |
| [HDFS-8498](https://issues.apache.org/jira/browse/HDFS-8498) | Blocks can be committed with wrong size |  Critical | hdfs-client | Daryn Sharp | Jing Zhao |
| [HDFS-11714](https://issues.apache.org/jira/browse/HDFS-11714) | Newly added NN storage directory won't get initialized and cause space exhaustion |  Critical | . | Kihwal Lee | Kihwal Lee |
| [HADOOP-14371](https://issues.apache.org/jira/browse/HADOOP-14371) | License error in TestLoadBalancingKMSClientProvider.java |  Major | . | hu xiaodong | hu xiaodong |
| [HADOOP-14369](https://issues.apache.org/jira/browse/HADOOP-14369) | NetworkTopology calls expensive toString() when logging |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HADOOP-14306](https://issues.apache.org/jira/browse/HADOOP-14306) | TestLocalFileSystem tests have very low timeouts |  Major | . | Eric Badger | Eric Badger |
| [HADOOP-14372](https://issues.apache.org/jira/browse/HADOOP-14372) | TestSymlinkLocalFS timeouts are too low |  Major | . | Eric Badger | Eric Badger |
| [HADOOP-14207](https://issues.apache.org/jira/browse/HADOOP-14207) | "dfsadmin -refreshCallQueue" fails with DecayRpcScheduler |  Blocker | rpc-server | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-11702](https://issues.apache.org/jira/browse/HDFS-11702) | Remove indefinite caching of key provider uri in DFSClient |  Major | hdfs-client | Rushabh S Shah | Rushabh S Shah |
| [HADOOP-14374](https://issues.apache.org/jira/browse/HADOOP-14374) | License error in GridmixTestUtils.java |  Major | . | lixinglong | lixinglong |
| [HADOOP-14100](https://issues.apache.org/jira/browse/HADOOP-14100) | Upgrade Jsch jar to latest version to fix vulnerability in old versions |  Critical | . | Vinayakumar B | Vinayakumar B |
| [HADOOP-14377](https://issues.apache.org/jira/browse/HADOOP-14377) | Increase Common test timeouts from 1 second to 10 seconds |  Major | . | Eric Badger | Eric Badger |
| [HADOOP-14373](https://issues.apache.org/jira/browse/HADOOP-14373) | License error In org.apache.hadoop.metrics2.util.Servers |  Major | . | hu xiaodong | hu xiaodong |
| [YARN-6552](https://issues.apache.org/jira/browse/YARN-6552) | Increase YARN test timeouts from 1 second to 10 seconds |  Major | . | Eric Badger | Eric Badger |
| [MAPREDUCE-6882](https://issues.apache.org/jira/browse/MAPREDUCE-6882) | Increase MapReduce test timeouts from 1 second to 10 seconds |  Major | . | Eric Badger | Eric Badger |
| [HADOOP-14405](https://issues.apache.org/jira/browse/HADOOP-14405) | Fix performance regression due to incorrect use of DataChecksum |  Major | native, performance | LiXin Ge | LiXin Ge |
| [HDFS-11745](https://issues.apache.org/jira/browse/HDFS-11745) | Increase HDFS test timeouts from 1 second to 10 seconds |  Major | . | Eric Badger | Eric Badger |
| [HDFS-11755](https://issues.apache.org/jira/browse/HDFS-11755) | Underconstruction blocks can be considered missing |  Major | . | Nathan Roberts | Nathan Roberts |
| [YARN-5543](https://issues.apache.org/jira/browse/YARN-5543) | ResourceManager SchedulingMonitor could potentially terminate the preemption checker thread |  Major | capacityscheduler, resourcemanager | Min Shen | Min Shen |
| [HDFS-11674](https://issues.apache.org/jira/browse/HDFS-11674) | reserveSpaceForReplicas is not released if append request failed due to mirror down and replica recovered |  Critical | datanode | Vinayakumar B | Vinayakumar B |
| [HADOOP-14376](https://issues.apache.org/jira/browse/HADOOP-14376) | Memory leak when reading a compressed file using the native library |  Major | common, io | Eli Acherkan | Eli Acherkan |
| [HDFS-11818](https://issues.apache.org/jira/browse/HDFS-11818) | TestBlockManager.testSufficientlyReplBlocksUsesNewRack fails intermittently |  Major | . | Eric Badger | Nathan Roberts |
| [YARN-6598](https://issues.apache.org/jira/browse/YARN-6598) | History server getApplicationReport NPE when fetching report for pre-2.8 job |  Blocker | timelineserver | Jason Lowe | Jason Lowe |
| [YARN-6603](https://issues.apache.org/jira/browse/YARN-6603) | NPE in RMAppsBlock |  Major | resourcemanager | Jason Lowe | Jason Lowe |
| [HDFS-11833](https://issues.apache.org/jira/browse/HDFS-11833) | HDFS architecture documentation describes outdated placement policy |  Minor | . | dud | Chen Liang |
| [HADOOP-14412](https://issues.apache.org/jira/browse/HADOOP-14412) | HostsFileReader#getHostDetails is very expensive on large clusters |  Major | util | Jason Lowe | Jason Lowe |
| [YARN-6577](https://issues.apache.org/jira/browse/YARN-6577) | Remove unused ContainerLocalization classes |  Minor | nodemanager | ZhangBing Lin | ZhangBing Lin |
| [YARN-6618](https://issues.apache.org/jira/browse/YARN-6618) | TestNMLeveldbStateStoreService#testCompactionCycle can fail if compaction occurs more than once |  Minor | test | Jason Lowe | Jason Lowe |
| [HDFS-11849](https://issues.apache.org/jira/browse/HDFS-11849) | JournalNode startup failure exception should be logged in log file |  Major | journal-node | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-11864](https://issues.apache.org/jira/browse/HDFS-11864) | Document  Metrics to track usage of memory for writes |  Major | documentation | Brahma Reddy Battula | Yiqun Lin |
| [YARN-6615](https://issues.apache.org/jira/browse/YARN-6615) | AmIpFilter drops query parameters on redirect |  Major | . | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [HADOOP-14449](https://issues.apache.org/jira/browse/HADOOP-14449) | The ASF Header in ComparableVersion.java and SSLHostnameVerifier.java is not correct |  Minor | common, documentation | ZhangBing Lin | ZhangBing Lin |
| [HADOOP-14166](https://issues.apache.org/jira/browse/HADOOP-14166) | Reset the DecayRpcScheduler AvgResponseTime metric to zero when queue is not used |  Major | common | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-11661](https://issues.apache.org/jira/browse/HDFS-11661) | GetContentSummary uses excessive amounts of memory |  Blocker | namenode | Nathan Roberts | Wei-Chiu Chuang |
| [YARN-6141](https://issues.apache.org/jira/browse/YARN-6141) | ppc64le on Linux doesn't trigger \_\_linux get\_executable codepath |  Major | nodemanager | Sonia Garudi | Ayappan |
| [HDFS-11445](https://issues.apache.org/jira/browse/HDFS-11445) | FSCK shows overall health stauts as corrupt even one replica is corrupt |  Critical | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-6643](https://issues.apache.org/jira/browse/YARN-6643) | TestRMFailover fails rarely due to port conflict |  Major | test | Robert Kanter | Robert Kanter |
| [HDFS-11817](https://issues.apache.org/jira/browse/HDFS-11817) | A faulty node can cause a lease leak and NPE on accessing data |  Critical | . | Kihwal Lee | Kihwal Lee |
| [YARN-6641](https://issues.apache.org/jira/browse/YARN-6641) | Non-public resource localization on a bad disk causes subsequent containers failure |  Major | . | Kuhu Shukla | Kuhu Shukla |
| [HDFS-11078](https://issues.apache.org/jira/browse/HDFS-11078) | Fix NPE in LazyPersistFileScrubber |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HADOOP-14464](https://issues.apache.org/jira/browse/HADOOP-14464) | hadoop-aws doc header warning #5 line wrapped |  Trivial | documentation, fs/s3 | John Zhuge | John Zhuge |
| [HDFS-5042](https://issues.apache.org/jira/browse/HDFS-5042) | Completed files lost after power failure |  Critical | . | Dave Latham | Vinayakumar B |
| [YARN-6649](https://issues.apache.org/jira/browse/YARN-6649) | RollingLevelDBTimelineServer throws RuntimeException if object decoding ever fails runtime exception |  Critical | . | Jonathan Eagles | Jonathan Eagles |
| [HDFS-11893](https://issues.apache.org/jira/browse/HDFS-11893) | Fix TestDFSShell.testMoveWithTargetPortEmpty failure. |  Major | test | Konstantin Shvachko | Brahma Reddy Battula |
| [HDFS-11741](https://issues.apache.org/jira/browse/HDFS-11741) | Long running balancer may fail due to expired DataEncryptionKey |  Major | balancer & mover | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-11856](https://issues.apache.org/jira/browse/HDFS-11856) | Ability to re-add Upgrading Nodes (remote) to pipeline for future pipeline updates |  Major | hdfs-client, rolling upgrades | Vinayakumar B | Vinayakumar B |
| [HADOOP-14474](https://issues.apache.org/jira/browse/HADOOP-14474) | Use OpenJDK 7 instead of Oracle JDK 7 to avoid oracle-java7-installer failures |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HDFS-10816](https://issues.apache.org/jira/browse/HDFS-10816) | TestComputeInvalidateWork#testDatanodeReRegistration fails due to race between test and replication monitor |  Major | . | Eric Badger | Eric Badger |
| [HDFS-11932](https://issues.apache.org/jira/browse/HDFS-11932) | BPServiceActor thread name is not correctly set |  Major | hdfs | Chen Liang | Chen Liang |
| [HDFS-11708](https://issues.apache.org/jira/browse/HDFS-11708) | Positional read will fail if replicas moved to different DNs after stream is opened |  Critical | hdfs-client | Vinayakumar B | Vinayakumar B |
| [HDFS-11711](https://issues.apache.org/jira/browse/HDFS-11711) | DN should not delete the block On "Too many open files" Exception |  Critical | datanode | Brahma Reddy Battula | Brahma Reddy Battula |
| [MAPREDUCE-6676](https://issues.apache.org/jira/browse/MAPREDUCE-6676) | NNBench should Throw IOException when rename and delete fails |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-14208](https://issues.apache.org/jira/browse/HADOOP-14208) | Fix typo in the top page in branch-2.8 |  Trivial | common, documentation | Akira Ajisaka | Wenxin He |
| [HDFS-11945](https://issues.apache.org/jira/browse/HDFS-11945) | Internal lease recovery may not be retried for a long time |  Major | namenode | Kihwal Lee | Kihwal Lee |
| [HADOOP-14511](https://issues.apache.org/jira/browse/HADOOP-14511) | WritableRpcEngine.Invocation#toString NPE on null parameters |  Minor | ipc | John Zhuge | John Zhuge |
| [HADOOP-14512](https://issues.apache.org/jira/browse/HADOOP-14512) | WASB atomic rename should not throw exception if the file is neither in src nor in dst when doing the rename |  Major | fs/azure | Duo Xu | Duo Xu |
| [YARN-6585](https://issues.apache.org/jira/browse/YARN-6585) | RM fails to start when upgrading from 2.7 to 2.8 for clusters with node labels. |  Blocker | . | Eric Payne | Sunil G |
| [HDFS-11967](https://issues.apache.org/jira/browse/HDFS-11967) | TestJMXGet fails occasionally |  Major | . | Arpit Agarwal | Arpit Agarwal |
| [HDFS-11947](https://issues.apache.org/jira/browse/HDFS-11947) | When constructing a thread name, BPOfferService may print a bogus warning message |  Minor | datanode | Tsz Wo Nicholas Sze | Weiwei Yang |
| [YARN-6719](https://issues.apache.org/jira/browse/YARN-6719) | Fix findbugs warnings in SLSCapacityScheduler.java |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-14540](https://issues.apache.org/jira/browse/HADOOP-14540) | Replace MRv1 specific terms in HostsFileReader |  Minor | documentation | Akira Ajisaka | hu xiaodong |
| [HDFS-11995](https://issues.apache.org/jira/browse/HDFS-11995) | HDFS Architecture documentation incorrectly describes writing to a local temporary file. |  Minor | documentation | Chris Nauroth | Nandakumar |
| [HDFS-11736](https://issues.apache.org/jira/browse/HDFS-11736) | OIV tests should not write outside 'target' directory. |  Major | . | Konstantin Shvachko | Yiqun Lin |
| [YARN-6713](https://issues.apache.org/jira/browse/YARN-6713) | Fix dead link in the Javadoc of FairSchedulerEventLog.java |  Minor | documentation | Akira Ajisaka | Weiwei Yang |
| [HADOOP-14533](https://issues.apache.org/jira/browse/HADOOP-14533) | Size of args cannot be less than zero in TraceAdmin#run as its linkedlist |  Trivial | common, tracing | Weisen Han | Weisen Han |
| [HDFS-11960](https://issues.apache.org/jira/browse/HDFS-11960) | Successfully closed files can stay under-replicated. |  Critical | . | Kihwal Lee | Kihwal Lee |
| [HADOOP-14146](https://issues.apache.org/jira/browse/HADOOP-14146) | KerberosAuthenticationHandler should authenticate with SPN in AP-REQ |  Major | security | Daryn Sharp | Daryn Sharp |
| [YARN-5876](https://issues.apache.org/jira/browse/YARN-5876) | TestResourceTrackerService#testGracefulDecommissionWithApp fails intermittently on trunk |  Major | . | Varun Saxena | Robert Kanter |
| [YARN-6467](https://issues.apache.org/jira/browse/YARN-6467) | CSQueueMetrics needs to update the current metrics for default partition only |  Major | capacity scheduler | Naganarasimha G R | Manikandan R |
| [HADOOP-14024](https://issues.apache.org/jira/browse/HADOOP-14024) | KMS JMX endpoint throws ClassNotFoundException |  Critical | kms | Andrew Wang | John Zhuge |
| [YARN-6749](https://issues.apache.org/jira/browse/YARN-6749) | TestAppSchedulingInfo.testPriorityAccounting fails consistently |  Major | . | Eric Badger | Naganarasimha G R |
| [MAPREDUCE-6905](https://issues.apache.org/jira/browse/MAPREDUCE-6905) | Fix meaningless operations in TestDFSIO in some situation. |  Major | tools/rumen | LiXin Ge | LiXin Ge |
| [MAPREDUCE-6909](https://issues.apache.org/jira/browse/MAPREDUCE-6909) | LocalJobRunner fails when run on a node from multiple users |  Blocker | client | Jason Lowe | Jason Lowe |
| [HADOOP-13414](https://issues.apache.org/jira/browse/HADOOP-13414) | Hide Jetty Server version header in HTTP responses |  Major | security | Vinayakumar B | Surendra Singh Lilhore |
| [HDFS-12089](https://issues.apache.org/jira/browse/HDFS-12089) | Fix ambiguous NN retry log message |  Major | webhdfs | Eric Badger | Eric Badger |
| [MAPREDUCE-6911](https://issues.apache.org/jira/browse/MAPREDUCE-6911) | TestMapreduceConfigFields.testCompareXmlAgainstConfigurationClass fails consistently |  Major | . | Eric Badger | Eric Badger |
| [YARN-6708](https://issues.apache.org/jira/browse/YARN-6708) | Nodemanager container crash after ext3 folder limit |  Critical | . | Bibin A Chundatt | Bibin A Chundatt |
| [HADOOP-14563](https://issues.apache.org/jira/browse/HADOOP-14563) | LoadBalancingKMSClientProvider#warmUpEncryptedKeys swallows IOException |  Major | . | Rushabh S Shah | Rushabh S Shah |
| [MAPREDUCE-6246](https://issues.apache.org/jira/browse/MAPREDUCE-6246) | DBOutputFormat.java appending extra semicolon to query which is incompatible with DB2 |  Major | mrv1, mrv2 | ramtin | Gergely Novák |
| [YARN-6428](https://issues.apache.org/jira/browse/YARN-6428) | Queue AM limit is not honored  in CS always |  Major | . | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-6770](https://issues.apache.org/jira/browse/YARN-6770) | [Docs] A small mistake in the example of TimelineClient |  Trivial | docs | Jinjiang Ling | Jinjiang Ling |
| [YARN-6809](https://issues.apache.org/jira/browse/YARN-6809) | Fix typo in ResourceManagerHA.md |  Trivial | documentation | Akira Ajisaka | Yeliang Cang |
| [MAPREDUCE-5621](https://issues.apache.org/jira/browse/MAPREDUCE-5621) | mr-jobhistory-daemon.sh doesn't have to execute mkdir and chown all the time |  Minor | jobhistoryserver | Shinichi Yamashita | Shinichi Yamashita |
| [YARN-6797](https://issues.apache.org/jira/browse/YARN-6797) | TimelineWriter does not fully consume the POST response |  Major | timelineclient | Jason Lowe | Jason Lowe |
| [HDFS-11502](https://issues.apache.org/jira/browse/HDFS-11502) | Datanode UI should display hostname based on JMX bean instead of window.location.hostname |  Major | hdfs | Jeffrey E  Rodriguez | Jeffrey E  Rodriguez |
| [HADOOP-14646](https://issues.apache.org/jira/browse/HADOOP-14646) | FileContextMainOperationsBaseTest#testListStatusFilterWithSomeMatches never runs |  Minor | test | Andras Bokor | Andras Bokor |
| [MAPREDUCE-6697](https://issues.apache.org/jira/browse/MAPREDUCE-6697) | Concurrent task limits should only be applied when necessary |  Major | mrv2 | Jason Lowe | Nathan Roberts |
| [YARN-6654](https://issues.apache.org/jira/browse/YARN-6654) | RollingLevelDBTimelineStore backwards incompatible after fst upgrade |  Blocker | . | Jonathan Eagles | Jonathan Eagles |
| [YARN-6805](https://issues.apache.org/jira/browse/YARN-6805) | NPE in LinuxContainerExecutor due to null PrivilegedOperationException exit code |  Major | nodemanager | Jason Lowe | Jason Lowe |
| [YARN-3260](https://issues.apache.org/jira/browse/YARN-3260) | AM attempt fail to register before RM processes launch event |  Critical | resourcemanager | Jason Lowe | Bibin A Chundatt |
| [HDFS-12140](https://issues.apache.org/jira/browse/HDFS-12140) | Remove BPOfferService lock contention to get block pool id |  Critical | datanode | Daryn Sharp | Daryn Sharp |
| [HDFS-12112](https://issues.apache.org/jira/browse/HDFS-12112) | TestBlockManager#testBlockManagerMachinesArray sometimes fails with NPE |  Minor | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [MAPREDUCE-6910](https://issues.apache.org/jira/browse/MAPREDUCE-6910) | MapReduceTrackingUriPlugin can not return the right URI of history server with HTTPS |  Major | jobhistoryserver | Lantao Jin | Lantao Jin |
| [HDFS-12158](https://issues.apache.org/jira/browse/HDFS-12158) | Secondary Namenode's web interface lack configs for X-FRAME-OPTIONS protection |  Major | namenode | Mukul Kumar Singh | Mukul Kumar Singh |
| [YARN-6837](https://issues.apache.org/jira/browse/YARN-6837) | Null LocalResource visibility or resource type can crash the nodemanager |  Major | . | Jinjiang Ling | Jinjiang Ling |
| [HDFS-11472](https://issues.apache.org/jira/browse/HDFS-11472) | Fix inconsistent replica size after a data pipeline failure |  Critical | datanode | Wei-Chiu Chuang | Erik Krogen |
| [HDFS-12177](https://issues.apache.org/jira/browse/HDFS-12177) | NameNode exits due to  setting BlockPlacementPolicy loglevel to Debug |  Major | block placement | Jiandan Yang | Jiandan Yang |
| [HDFS-11742](https://issues.apache.org/jira/browse/HDFS-11742) | Improve balancer usability after HDFS-8818 |  Blocker | . | Kihwal Lee | Kihwal Lee |
| [HDFS-11896](https://issues.apache.org/jira/browse/HDFS-11896) | Non-dfsUsed will be doubled on dead node re-registration |  Blocker | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-5728](https://issues.apache.org/jira/browse/YARN-5728) | TestMiniYarnClusterNodeUtilization.testUpdateNodeUtilization timeout |  Major | test | Akira Ajisaka | Akira Ajisaka |
| [YARN-6628](https://issues.apache.org/jira/browse/YARN-6628) | Unexpected jackson-core-2.2.3 dependency introduced |  Blocker | timelineserver | Jason Lowe | Jonathan Eagles |
| [YARN-5731](https://issues.apache.org/jira/browse/YARN-5731) | Preemption calculation is not accurate when reserved containers are present in queue. |  Major | capacity scheduler | Sunil G | Wangda Tan |
| [HADOOP-14683](https://issues.apache.org/jira/browse/HADOOP-14683) | FileStatus.compareTo binary compatible issue |  Blocker | . | Sergey Shelukhin | Akira Ajisaka |
| [YARN-6872](https://issues.apache.org/jira/browse/YARN-6872) | Ensure apps could run given NodeLabels are disabled post RM switchover/restart |  Major | resourcemanager | Sunil G | Sunil G |
| [YARN-6846](https://issues.apache.org/jira/browse/YARN-6846) | Nodemanager can fail to fully delete application local directories when applications are killed |  Critical | nodemanager | Jason Lowe | Jason Lowe |
| [MAPREDUCE-6927](https://issues.apache.org/jira/browse/MAPREDUCE-6927) | MR job should only set tracking url if history was successfully written |  Major | . | Eric Badger | Eric Badger |
| [YARN-6890](https://issues.apache.org/jira/browse/YARN-6890) | If UI is not secured, we allow user to kill other users' job even yarn cluster is secured. |  Critical | . | Sumana Sathish | Junping Du |
| [HDFS-10326](https://issues.apache.org/jira/browse/HDFS-10326) | Disable setting tcp socket send/receive buffers for write pipelines |  Major | datanode, hdfs | Daryn Sharp | Daryn Sharp |
| [HDFS-12157](https://issues.apache.org/jira/browse/HDFS-12157) | Do fsyncDirectory(..) outside of FSDataset lock |  Critical | datanode | Vinayakumar B | Vinayakumar B |
| [HDFS-12278](https://issues.apache.org/jira/browse/HDFS-12278) | LeaseManager operations are inefficient in 2.8. |  Blocker | namenode | Rushabh S Shah | Rushabh S Shah |
| [YARN-6987](https://issues.apache.org/jira/browse/YARN-6987) | Log app attempt during InvalidStateTransition |  Major | . | Jonathan Eagles | Jonathan Eagles |
| [YARN-7020](https://issues.apache.org/jira/browse/YARN-7020) | TestAMRMProxy#testAMRMProxyTokenRenewal is flakey |  Major | . | Robert Kanter | Robert Kanter |
| [YARN-2416](https://issues.apache.org/jira/browse/YARN-2416) | InvalidStateTransitonException in ResourceManager if AMLauncher does not receive response for startContainers() call in time |  Critical | resourcemanager | Jian Fang | Jonathan Eagles |
| [YARN-7048](https://issues.apache.org/jira/browse/YARN-7048) | Fix tests faking kerberos to explicitly set ugi auth type |  Major | yarn | Daryn Sharp | Daryn Sharp |
| [HADOOP-14687](https://issues.apache.org/jira/browse/HADOOP-14687) | AuthenticatedURL will reuse bad/expired session cookies |  Critical | common | Daryn Sharp | Daryn Sharp |
| [YARN-6640](https://issues.apache.org/jira/browse/YARN-6640) |  AM heartbeat stuck when responseId overflows MAX\_INT |  Blocker | . | Botong Huang | Botong Huang |
| [HDFS-12319](https://issues.apache.org/jira/browse/HDFS-12319) | DirectoryScanner will throw IllegalStateException when Multiple BP's are present |  Blocker | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-12299](https://issues.apache.org/jira/browse/HDFS-12299) | Race Between update pipeline and DN Re-Registration |  Critical | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-7052](https://issues.apache.org/jira/browse/YARN-7052) | RM SchedulingMonitor gives no indication why the spawned thread crashed. |  Critical | yarn | Eric Payne | Eric Payne |
| [YARN-7087](https://issues.apache.org/jira/browse/YARN-7087) | NM failed to perform log aggregation due to absent container |  Blocker | log-aggregation | Jason Lowe | Jason Lowe |
| [YARN-7051](https://issues.apache.org/jira/browse/YARN-7051) | Avoid concurrent modification exception in FifoIntraQueuePreemptionPlugin |  Critical | capacity scheduler, scheduler preemption, yarn | Eric Payne | Eric Payne |
| [HDFS-12364](https://issues.apache.org/jira/browse/HDFS-12364) | [branch-2.8.2] Fix the Compile Error after HDFS-12299 |  Blocker | hdfs | Jiandan Yang | Jiandan Yang |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-9300](https://issues.apache.org/jira/browse/HDFS-9300) | TestDirectoryScanner.testThrottle() is still a little flakey |  Major | balancer & mover, test | Daniel Templeton | Daniel Templeton |
| [HADOOP-13178](https://issues.apache.org/jira/browse/HADOOP-13178) | TestShellBasedIdMapping.testStaticMapUpdate doesn't work on OS X |  Major | test | Allen Wittenauer | Kai Sasaki |
| [YARN-5343](https://issues.apache.org/jira/browse/YARN-5343) | TestContinuousScheduling#testSortedNodes fails intermittently |  Minor | . | sandflee | Yufei Gu |
| [MAPREDUCE-6831](https://issues.apache.org/jira/browse/MAPREDUCE-6831) | Flaky test TestJobImpl.testKilledDuringKillAbort |  Major | mrv2 | Peter Bacsko | Peter Bacsko |
| [HDFS-11290](https://issues.apache.org/jira/browse/HDFS-11290) | TestFSNameSystemMBean should wait until JMX cache is cleared |  Major | test | Akira Ajisaka | Erik Krogen |
| [YARN-5349](https://issues.apache.org/jira/browse/YARN-5349) | TestWorkPreservingRMRestart#testUAMRecoveryOnRMWorkPreservingRestart  fail intermittently |  Minor | . | sandflee | Jason Lowe |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-9754](https://issues.apache.org/jira/browse/HDFS-9754) | Avoid unnecessary getBlockCollection calls in BlockManager |  Major | namenode | Jing Zhao | Jing Zhao |
| [HADOOP-14032](https://issues.apache.org/jira/browse/HADOOP-14032) | Reduce fair call queue priority inversion |  Major | ipc | Daryn Sharp | Daryn Sharp |
| [HADOOP-14034](https://issues.apache.org/jira/browse/HADOOP-14034) | Allow ipc layer exceptions to selectively close connections |  Major | ipc | Daryn Sharp | Daryn Sharp |
| [HADOOP-14033](https://issues.apache.org/jira/browse/HADOOP-14033) | Reduce fair call queue lock contention |  Major | ipc | Daryn Sharp | Daryn Sharp |
| [HADOOP-14135](https://issues.apache.org/jira/browse/HADOOP-14135) | Remove URI parameter in AWSCredentialProvider constructors |  Major | fs/s3 | Mingliang Liu | Mingliang Liu |
| [HADOOP-14196](https://issues.apache.org/jira/browse/HADOOP-14196) | Azure Data Lake doc is missing required config entry |  Major | fs/adl | Atul Sikaria | Atul Sikaria |
| [HADOOP-14197](https://issues.apache.org/jira/browse/HADOOP-14197) | Fix ADLS doc for credential provider |  Major | documentation, fs/adl | John Zhuge | John Zhuge |
| [HADOOP-14230](https://issues.apache.org/jira/browse/HADOOP-14230) | TestAdlFileSystemContractLive fails to clean up |  Minor | fs/adl, test | John Zhuge | John Zhuge |
| [HADOOP-14038](https://issues.apache.org/jira/browse/HADOOP-14038) | Rename ADLS credential properties |  Minor | fs/adl | John Zhuge | John Zhuge |
| [HADOOP-14321](https://issues.apache.org/jira/browse/HADOOP-14321) | Explicitly exclude S3A root dir ITests from parallel runs |  Minor | fs/s3, test | Steve Loughran | Steve Loughran |
| [HADOOP-14241](https://issues.apache.org/jira/browse/HADOOP-14241) | Add ADLS sensitive config keys to default list |  Minor | fs, fs/adl, security | John Zhuge | John Zhuge |
| [HADOOP-14349](https://issues.apache.org/jira/browse/HADOOP-14349) | Rename ADLS CONTRACT\_ENABLE\_KEY |  Minor | fs/adl | Mingliang Liu | Mingliang Liu |
| [HDFS-9005](https://issues.apache.org/jira/browse/HDFS-9005) | Provide configuration support for upgrade domain |  Major | . | Ming Ma | Ming Ma |
| [HDFS-9016](https://issues.apache.org/jira/browse/HDFS-9016) | Display upgrade domain information in fsck |  Major | . | Ming Ma | Ming Ma |
| [HDFS-9922](https://issues.apache.org/jira/browse/HDFS-9922) | Upgrade Domain placement policy status marks a good block in violation when there are decommissioned nodes |  Minor | . | Chris Trezzo | Chris Trezzo |
| [HADOOP-14035](https://issues.apache.org/jira/browse/HADOOP-14035) | Reduce fair call queue backoff's impact on clients |  Major | ipc | Daryn Sharp | Daryn Sharp |
| [YARN-6682](https://issues.apache.org/jira/browse/YARN-6682) | Improve performance of AssignmentInformation datastructures |  Major | . | Daryn Sharp | Daryn Sharp |
| [YARN-6680](https://issues.apache.org/jira/browse/YARN-6680) | Avoid locking overhead for NO\_LABEL lookups |  Major | resourcemanager | Daryn Sharp | Daryn Sharp |
| [YARN-6681](https://issues.apache.org/jira/browse/YARN-6681) | Eliminate double-copy of child queues in canAssignToThisQueue |  Major | resourcemanager | Daryn Sharp | Daryn Sharp |
| [YARN-2113](https://issues.apache.org/jira/browse/YARN-2113) | Add cross-user preemption within CapacityScheduler's leaf-queue |  Major | capacity scheduler | Vinod Kumar Vavilapalli | Sunil G |
| [YARN-6775](https://issues.apache.org/jira/browse/YARN-6775) | CapacityScheduler: Improvements to assignContainers, avoid unnecessary canAssignToUser/Queue calls |  Major | capacityscheduler | Nathan Roberts | Nathan Roberts |
| [YARN-6988](https://issues.apache.org/jira/browse/YARN-6988) | container-executor fails for docker when command length \> 4096 B |  Major | yarn | Eric Badger | Eric Badger |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-14344](https://issues.apache.org/jira/browse/HADOOP-14344) | Revert HADOOP-13606 swift FS to add a service load metadata file |  Major | . | John Zhuge | John Zhuge |
| [HDFS-11717](https://issues.apache.org/jira/browse/HDFS-11717) | Add unit test for HDFS-11709 StandbyCheckpointer should handle non-existing legacyOivImageDir gracefully |  Minor | ha, namenode | Erik Krogen | Erik Krogen |
