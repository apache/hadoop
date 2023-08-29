
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

## Release 3.3.6 - 2023-06-19



### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-18671](https://issues.apache.org/jira/browse/HADOOP-18671) | Add recoverLease(), setSafeMode(), isFileClosed() APIs to FileSystem |  Major | fs | Wei-Chiu Chuang | Tak-Lon (Stephen) Wu |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-15368](https://issues.apache.org/jira/browse/HDFS-15368) | TestBalancerWithHANameNodes#testBalancerWithObserver failed occasionally |  Major | . | Xiaoqiao He | Xiaoqiao He |
| [HDFS-15383](https://issues.apache.org/jira/browse/HDFS-15383) | RBF: Disable watch in ZKDelegationSecretManager for performance |  Major | . | Fengnan Li | Fengnan Li |
| [HDFS-15803](https://issues.apache.org/jira/browse/HDFS-15803) | EC: Remove unnecessary method (getWeight) in StripedReconstructionInfo |  Trivial | . | Haiyang Hu | Haiyang Hu |
| [HDFS-16024](https://issues.apache.org/jira/browse/HDFS-16024) | RBF: Rename data to the Trash should be based on src locations |  Major | rbf | Xiangyi Zhu | Xiangyi Zhu |
| [HDFS-16016](https://issues.apache.org/jira/browse/HDFS-16016) | BPServiceActor add a new thread to handle IBR |  Minor | . | JiangHua Zhu | Viraj Jasani |
| [HADOOP-17749](https://issues.apache.org/jira/browse/HADOOP-17749) | Remove lock contention in SelectorPool of SocketIOWithTimeout |  Major | common | Xuesen Liang | Xuesen Liang |
| [HDFS-16480](https://issues.apache.org/jira/browse/HDFS-16480) | Fix typo: indicies -\> indices |  Minor | . | Jiale Qi | Jiale Qi |
| [HADOOP-18466](https://issues.apache.org/jira/browse/HADOOP-18466) | Limit the findbugs suppression IS2\_INCONSISTENT\_SYNC to S3AFileSystem field |  Minor | fs/s3 | Viraj Jasani | Viraj Jasani |
| [MAPREDUCE-7370](https://issues.apache.org/jira/browse/MAPREDUCE-7370) | Parallelize MultipleOutputs#close call |  Major | . | Prabhu Joseph | Ashutosh Gupta |
| [YARN-11360](https://issues.apache.org/jira/browse/YARN-11360) | Add number of decommissioning/shutdown nodes to YARN cluster metrics. |  Major | client, resourcemanager | Chris Nauroth | Chris Nauroth |
| [HADOOP-18472](https://issues.apache.org/jira/browse/HADOOP-18472) | Upgrade to snakeyaml 1.33 |  Major | . | PJ Fanning | PJ Fanning |
| [HDFS-16811](https://issues.apache.org/jira/browse/HDFS-16811) | Support DecommissionBackoffMonitor parameters reconfigurable |  Major | . | Haiyang Hu | Haiyang Hu |
| [HADOOP-18433](https://issues.apache.org/jira/browse/HADOOP-18433) | Fix main thread name. |  Major | common, ipc | zhengchenyu | zhengchenyu |
| [HDFS-16851](https://issues.apache.org/jira/browse/HDFS-16851) | RBF: Add a utility to dump the StateStore |  Major | rbf | Owen O'Malley | Owen O'Malley |
| [HDFS-16839](https://issues.apache.org/jira/browse/HDFS-16839) | It should consider EC reconstruction work when we determine if a node is busy |  Major | ec, erasure-coding | Kidd5368 | Kidd5368 |
| [HDFS-16887](https://issues.apache.org/jira/browse/HDFS-16887) | Log start and end of phase/step in startup progress |  Minor | namenode | Viraj Jasani | Viraj Jasani |
| [HDFS-16891](https://issues.apache.org/jira/browse/HDFS-16891) | Avoid the overhead of copy-on-write exception list while loading inodes sub sections in parallel |  Major | namenode | Viraj Jasani | Viraj Jasani |
| [HADOOP-18604](https://issues.apache.org/jira/browse/HADOOP-18604) | Add compile platform in the hadoop version output |  Major | . | Ayush Saxena | Ayush Saxena |
| [HDFS-16888](https://issues.apache.org/jira/browse/HDFS-16888) | BlockManager#maxReplicationStreams, replicationStreamsHardLimit, blocksReplWorkMultiplier and PendingReconstructionBlocks#timeout should be volatile |  Major | . | Haiyang Hu | Haiyang Hu |
| [HADOOP-18592](https://issues.apache.org/jira/browse/HADOOP-18592) | Sasl connection failure should log remote address |  Major | . | Viraj Jasani | Viraj Jasani |
| [HADOOP-18625](https://issues.apache.org/jira/browse/HADOOP-18625) | Fix method name  of RPC.Builder#setnumReaders |  Minor | ipc | Haiyang Hu | Haiyang Hu |
| [HDFS-16882](https://issues.apache.org/jira/browse/HDFS-16882) | RBF: Add cache hit rate metric in MountTableResolver#getDestinationForPath |  Minor | rbf | farmmamba | farmmamba |
| [HDFS-16907](https://issues.apache.org/jira/browse/HDFS-16907) | Add LastHeartbeatResponseTime for BP service actor |  Major | . | Viraj Jasani | Viraj Jasani |
| [HADOOP-18628](https://issues.apache.org/jira/browse/HADOOP-18628) | Server connection should log host name before returning VersionMismatch error |  Minor | ipc | Viraj Jasani | Viraj Jasani |
| [HADOOP-18596](https://issues.apache.org/jira/browse/HADOOP-18596) | Distcp -update between different cloud stores to use modification time while checking for file skip. |  Major | tools/distcp | Mehakmeet Singh | Mehakmeet Singh |
| [HADOOP-18215](https://issues.apache.org/jira/browse/HADOOP-18215) | Enhance WritableName to be able to return aliases for classes that use serializers |  Minor | . | Bryan Beaudreault | Bryan Beaudreault |
| [HADOOP-18622](https://issues.apache.org/jira/browse/HADOOP-18622) | Upgrade ant to 1.10.13 |  Major | . | Aleksandr Nikolaev | Aleksandr Nikolaev |
| [YARN-11394](https://issues.apache.org/jira/browse/YARN-11394) | Fix hadoop-yarn-server-resourcemanager module Java Doc Errors. |  Major | resourcemanager | Shilun Fan | Shilun Fan |
| [HADOOP-18535](https://issues.apache.org/jira/browse/HADOOP-18535) | Implement token storage solution based on MySQL |  Major | . | Hector Sandoval Chaverri | Hector Sandoval Chaverri |
| [HADOOP-18646](https://issues.apache.org/jira/browse/HADOOP-18646) | Upgrade Netty to 4.1.89.Final |  Major | build | Aleksandr Nikolaev | Aleksandr Nikolaev |
| [HADOOP-18684](https://issues.apache.org/jira/browse/HADOOP-18684) | S3A filesystem to support binding to other URI schemes |  Major | . | Harshit Gupta | Harshit Gupta |
| [HADOOP-18590](https://issues.apache.org/jira/browse/HADOOP-18590) | Publish SBOM artifacts |  Major | build | Dongjoon Hyun | Dongjoon Hyun |
| [HADOOP-18597](https://issues.apache.org/jira/browse/HADOOP-18597) | Simplify single node instructions for creating directories for Map Reduce |  Trivial | documentation | Nikita Eshkeev | Nikita Eshkeev |
| [HADOOP-18691](https://issues.apache.org/jira/browse/HADOOP-18691) | Add a CallerContext getter on the Schedulable interface |  Major | . | Christos Bisias | Christos Bisias |
| [HADOOP-18689](https://issues.apache.org/jira/browse/HADOOP-18689) | Bump jettison from 1.5.3 to 1.5.4 in /hadoop-project |  Major | common | Ayush Saxena |  |
| [HDFS-16988](https://issues.apache.org/jira/browse/HDFS-16988) | Improve NameServices info at JournalNode web UI |  Minor | . | Zhaohui Wang | Zhaohui Wang |
| [HADOOP-18637](https://issues.apache.org/jira/browse/HADOOP-18637) | S3A to support upload of files greater than 2 GB using DiskBlocks |  Major | fs/s3 | Harshit Gupta | Harshit Gupta |
| [HADOOP-18695](https://issues.apache.org/jira/browse/HADOOP-18695) | S3A: reject multipart copy requests when disabled |  Minor | fs/s3 | Steve Loughran | Steve Loughran |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-16628](https://issues.apache.org/jira/browse/HDFS-16628) | RBF: Correct target directory when move to trash for kerberos login user. |  Major | rbf | Xiping Zhang | Xiping Zhang |
| [HDFS-16633](https://issues.apache.org/jira/browse/HDFS-16633) | Reserved Space For Replicas is not released on some cases |  Major | hdfs | Prabhu Joseph | Ashutosh Gupta |
| [HDFS-16566](https://issues.apache.org/jira/browse/HDFS-16566) | Erasure Coding: Recovery may cause excess replicas when busy DN exsits |  Major | ec, erasure-coding | Kidd5368 | Kidd5368 |
| [HADOOP-18471](https://issues.apache.org/jira/browse/HADOOP-18471) | An unhandled ArrayIndexOutOfBoundsException in DefaultStringifier.storeArray() if provided with an empty input |  Minor | common, io | FuzzingTeam | FuzzingTeam |
| [HDFS-16809](https://issues.apache.org/jira/browse/HDFS-16809) | EC striped block is not sufficient when doing in maintenance |  Major | ec, erasure-coding | dingshun | dingshun |
| [YARN-11390](https://issues.apache.org/jira/browse/YARN-11390) | TestResourceTrackerService.testNodeRemovalNormally: Shutdown nodes should be 0 now expected: \<1\> but was: \<0\> |  Major | yarn | Bence Kosztolnik | Bence Kosztolnik |
| [HDFS-16852](https://issues.apache.org/jira/browse/HDFS-16852) | Register the shutdown hook only when not in shutdown for KeyProviderCache constructor |  Minor | hdfs | Xing Lin | Xing Lin |
| [HADOOP-18567](https://issues.apache.org/jira/browse/HADOOP-18567) | LogThrottlingHelper: the dependent recorder is not triggered correctly |  Major | . | Chengbing Liu |  |
| [YARN-11395](https://issues.apache.org/jira/browse/YARN-11395) | Resource Manager UI, cluster/appattempt/\*, can not present FINAL\_SAVING state |  Critical | yarn | Bence Kosztolnik | Bence Kosztolnik |
| [YARN-11392](https://issues.apache.org/jira/browse/YARN-11392) | ClientRMService implemented getCallerUgi and verifyUserAccessForRMApp methods but forget to use sometimes, caused audit log missing. |  Major | yarn | Beibei Zhao | Beibei Zhao |
| [HDFS-16872](https://issues.apache.org/jira/browse/HDFS-16872) | Fix log throttling by declaring LogThrottlingHelper as static members |  Major | . | Chengbing Liu |  |
| [HADOOP-18591](https://issues.apache.org/jira/browse/HADOOP-18591) | Fix a typo in Trash |  Minor | documentation | xiaoping.huang | xiaoping.huang |
| [HDFS-16764](https://issues.apache.org/jira/browse/HDFS-16764) | ObserverNamenode handles addBlock rpc and throws a FileNotFoundException |  Critical | . | ZanderXu | ZanderXu |
| [HADOOP-18584](https://issues.apache.org/jira/browse/HADOOP-18584) | [NFS GW] Fix regression after netty4 migration |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-18279](https://issues.apache.org/jira/browse/HADOOP-18279) | Cancel fileMonitoringTimer even if trustManager isn't defined |  Major | common, test | Steve Vaughan | Steve Vaughan |
| [HADOOP-18576](https://issues.apache.org/jira/browse/HADOOP-18576) | Java 11 JavaDoc fails due to missing package comments |  Major | build, common | Steve Loughran | Steve Vaughan |
| [HADOOP-18612](https://issues.apache.org/jira/browse/HADOOP-18612) | Avoid mixing canonical and non-canonical when performing comparisons |  Minor | common, test | Steve Vaughan | Steve Vaughan |
| [HDFS-16925](https://issues.apache.org/jira/browse/HDFS-16925) | Namenode audit log to only include IP address of client |  Major | . | Viraj Jasani | Viraj Jasani |
| [HADOOP-18633](https://issues.apache.org/jira/browse/HADOOP-18633) | fix test AbstractContractDistCpTest#testDistCpUpdateCheckFileSkip |  Major | tools/distcp | Mehakmeet Singh | Mehakmeet Singh |
| [HADOOP-18582](https://issues.apache.org/jira/browse/HADOOP-18582) | No need to clean tmp files in distcp direct mode |  Major | tools/distcp | 10000kang | 10000kang |
| [HADOOP-18636](https://issues.apache.org/jira/browse/HADOOP-18636) | LocalDirAllocator cannot recover from directory tree deletion during the life of a filesystem client |  Minor | fs, fs/azure, fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-16935](https://issues.apache.org/jira/browse/HDFS-16935) | TestFsDatasetImpl.testReportBadBlocks brittle |  Minor | test | Steve Loughran | Viraj Jasani |
| [HDFS-16942](https://issues.apache.org/jira/browse/HDFS-16942) | Send error to datanode if FBR is rejected due to bad lease |  Major | datanode, namenode | Stephen O'Donnell | Stephen O'Donnell |
| [HADOOP-18662](https://issues.apache.org/jira/browse/HADOOP-18662) | ListFiles with recursive fails with FNF |  Major | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-18680](https://issues.apache.org/jira/browse/HADOOP-18680) | Insufficient heap during full test runs in Docker container. |  Minor | build | Chris Nauroth | Chris Nauroth |
| [HADOOP-18714](https://issues.apache.org/jira/browse/HADOOP-18714) | Wrong StringUtils.join() called in AbstractContractRootDirectoryTest |  Trivial | test | Attila Doroszlai | Attila Doroszlai |
| [HADOOP-18705](https://issues.apache.org/jira/browse/HADOOP-18705) | ABFS should exclude incompatible credential providers |  Major | fs/azure | Tamas Domok | Tamas Domok |
| [HADOOP-18660](https://issues.apache.org/jira/browse/HADOOP-18660) | Filesystem Spelling Mistake |  Trivial | fs | Sebastian Baunsgaard | Sebastian Baunsgaard |
| [MAPREDUCE-7437](https://issues.apache.org/jira/browse/MAPREDUCE-7437) | MR Fetcher class to use an AtomicInteger to generate IDs. |  Major | build, client | Steve Loughran | Steve Loughran |
| [HDFS-16672](https://issues.apache.org/jira/browse/HDFS-16672) | Fix lease interval comparison in BlockReportLeaseManager |  Trivial | namenode | dzcxzl | dzcxzl |
| [YARN-11482](https://issues.apache.org/jira/browse/YARN-11482) | Fix bug of DRF comparison DominantResourceFairnessComparator2 in fair scheduler |  Major | fairscheduler | Xiaoqiao He | Xiaoqiao He |
| [HDFS-16897](https://issues.apache.org/jira/browse/HDFS-16897) | Fix abundant Broken pipe exception in BlockSender |  Minor | hdfs | fanluo | fanluo |
| [HADOOP-18715](https://issues.apache.org/jira/browse/HADOOP-18715) | Add debug log for getting details of tokenKindMap |  Minor | . | Pralabh Kumar | Pralabh Kumar |
| [YARN-11312](https://issues.apache.org/jira/browse/YARN-11312) | [UI2] Refresh buttons don't work after EmberJS upgrade |  Minor | yarn-ui-v2 | Brian Goerlitz | Susheel Gupta |
| [HADOOP-18724](https://issues.apache.org/jira/browse/HADOOP-18724) | Open file fails with NumberFormatException for S3AFileSystem |  Critical | fs, fs/azure, fs/s3 | Ayush Saxena | Steve Loughran |
| [HADOOP-18652](https://issues.apache.org/jira/browse/HADOOP-18652) | Path.suffix raises NullPointerException |  Minor | hdfs-client | Patrick Grandjean | Patrick Grandjean |
| [HDFS-17022](https://issues.apache.org/jira/browse/HDFS-17022) | Fix the exception message to print the Identifier pattern |  Minor | . | Nishtha Shah | Nishtha Shah |
| [HDFS-17017](https://issues.apache.org/jira/browse/HDFS-17017) | Fix the issue of arguments number limit in report command in DFSAdmin. |  Major | . | Haiyang Hu | Haiyang Hu |
| [HADOOP-18755](https://issues.apache.org/jira/browse/HADOOP-18755) | openFile builder new optLong() methods break hbase-filesystem |  Major | fs | Steve Loughran | Steve Loughran |
| [HDFS-17011](https://issues.apache.org/jira/browse/HDFS-17011) | Fix the metric of  "HttpPort" at DataNodeInfo |  Minor | . | Zhaohui Wang | Zhaohui Wang |
| [HDFS-17003](https://issues.apache.org/jira/browse/HDFS-17003) | Erasure Coding: invalidate wrong block after reporting bad blocks from datanode |  Critical | namenode | farmmamba | farmmamba |
| [HADOOP-18718](https://issues.apache.org/jira/browse/HADOOP-18718) | Fix several maven build warnings |  Minor | build | Dongjoon Hyun | Dongjoon Hyun |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-11388](https://issues.apache.org/jira/browse/YARN-11388) | Prevent resource leaks in TestClientRMService. |  Minor | test | Chris Nauroth | Chris Nauroth |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-15654](https://issues.apache.org/jira/browse/HDFS-15654) | TestBPOfferService#testMissBlocksWhenReregister fails intermittently |  Major | datanode | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15674](https://issues.apache.org/jira/browse/HDFS-15674) | TestBPOfferService#testMissBlocksWhenReregister fails on trunk |  Major | . | Ahmed Hussein | Masatake Iwasaki |
| [HADOOP-18380](https://issues.apache.org/jira/browse/HADOOP-18380) | fs.s3a.prefetch.block.size to be read through longBytesOption |  Major | fs/s3 | Steve Loughran | Viraj Jasani |
| [HADOOP-18186](https://issues.apache.org/jira/browse/HADOOP-18186) | s3a prefetching to use SemaphoredDelegatingExecutor for submitting work |  Major | fs/s3 | Steve Loughran | Viraj Jasani |
| [HADOOP-18377](https://issues.apache.org/jira/browse/HADOOP-18377) | hadoop-aws maven build to add a prefetch profile to run all tests with prefetching |  Major | fs/s3, test | Steve Loughran | Viraj Jasani |
| [HADOOP-18455](https://issues.apache.org/jira/browse/HADOOP-18455) | s3a prefetching Executor should be closed |  Major | fs/s3 | Viraj Jasani | Viraj Jasani |
| [HADOOP-18378](https://issues.apache.org/jira/browse/HADOOP-18378) | Implement readFully(long position, byte[] buffer, int offset, int length) |  Minor | fs/s3 | Ahmar Suhail | Alessandro Passaro |
| [HADOOP-18189](https://issues.apache.org/jira/browse/HADOOP-18189) | S3PrefetchingInputStream to support status probes when closed |  Minor | fs/s3 | Steve Loughran | Viraj Jasani |
| [HADOOP-18156](https://issues.apache.org/jira/browse/HADOOP-18156) | Address JavaDoc warnings in classes like MarkerTool, S3ObjectAttributes, etc. |  Minor | fs/s3 | Mukund Thakur | Ankit Saurabh |
| [HADOOP-18482](https://issues.apache.org/jira/browse/HADOOP-18482) | ITestS3APrefetchingInputStream does not skip if no CSV test file available |  Minor | fs/s3 | Daniel Carl Jones | Daniel Carl Jones |
| [HADOOP-18531](https://issues.apache.org/jira/browse/HADOOP-18531) | assertion failure in ITestS3APrefetchingInputStream |  Major | fs/s3, test | Steve Loughran | Ashutosh Gupta |
| [HADOOP-18620](https://issues.apache.org/jira/browse/HADOOP-18620) | Avoid using grizzly-http-\* APIs |  Major | . | Viraj Jasani | Viraj Jasani |
| [HADOOP-18351](https://issues.apache.org/jira/browse/HADOOP-18351) | S3A prefetching: Error logging during reads |  Minor | . | Ahmar Suhail | Ankit Saurabh |
| [HADOOP-17746](https://issues.apache.org/jira/browse/HADOOP-17746) | compatibility table in directory\_markers.md doesn't render right |  Minor | fs/s3, site | Steve Loughran | Masatake Iwasaki |
| [HADOOP-18606](https://issues.apache.org/jira/browse/HADOOP-18606) | Add reason in in x-ms-client-request-id on a retry API call. |  Minor | fs/azure | Pranav Saxena | Pranav Saxena |
| [HADOOP-18146](https://issues.apache.org/jira/browse/HADOOP-18146) | ABFS: Add changes for expect hundred continue header with append requests |  Major | fs/azure | Anmol Asrani | Anmol Asrani |
| [HADOOP-18647](https://issues.apache.org/jira/browse/HADOOP-18647) | x-ms-client-request-id to have some way that identifies retry of an API. |  Minor | fs/azure | Pranav Saxena | Pranav Saxena |
| [HADOOP-18012](https://issues.apache.org/jira/browse/HADOOP-18012) | ABFS: Enable config controlled ETag check for Rename idempotency |  Major | fs/azure | Sneha Vijayarajan | Sree Bhattacharyya |
| [HADOOP-18696](https://issues.apache.org/jira/browse/HADOOP-18696) | S3A ITestS3ABucketExistence access point test failure |  Major | fs/s3, test | Steve Loughran | Steve Loughran |
| [HADOOP-18399](https://issues.apache.org/jira/browse/HADOOP-18399) | S3A Prefetch - SingleFilePerBlockCache to use LocalDirAllocator |  Major | fs/s3 | Steve Loughran | Viraj Jasani |
| [HADOOP-18703](https://issues.apache.org/jira/browse/HADOOP-18703) | Backport S3A prefetching stream to branch-3.3 |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-18697](https://issues.apache.org/jira/browse/HADOOP-18697) | Fix transient failure of ITestS3APrefetchingInputStream#testRandomReadLargeFile |  Major | fs/s3, test | Steve Loughran | Viraj Jasani |
| [HADOOP-18688](https://issues.apache.org/jira/browse/HADOOP-18688) | S3A audit header to include count of items in delete ops |  Major | fs/s3 | Steve Loughran | Viraj Jasani |
| [HADOOP-18740](https://issues.apache.org/jira/browse/HADOOP-18740) | s3a prefetch cache blocks should be accessed by RW locks |  Major | . | Viraj Jasani | Viraj Jasani |
| [HADOOP-18763](https://issues.apache.org/jira/browse/HADOOP-18763) | Upgrade aws-java-sdk to 1.12.367+ |  Major | fs/s3 | Steve Loughran | Viraj Jasani |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-16822](https://issues.apache.org/jira/browse/HDFS-16822) | HostRestrictingAuthorizationFilter should pass through requests if they don't access WebHDFS API |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-16886](https://issues.apache.org/jira/browse/HDFS-16886) | Fix documentation for StateStoreRecordOperations#get(Class ..., Query ...) |  Trivial | . | Simbarashe Dzinamarira | Simbarashe Dzinamarira |
| [HDFS-16902](https://issues.apache.org/jira/browse/HDFS-16902) | Add Namenode status to BPServiceActor metrics and improve logging in offerservice |  Major | . | Viraj Jasani | Viraj Jasani |
| [HDFS-16901](https://issues.apache.org/jira/browse/HDFS-16901) | RBF: Routers should propagate the real user in the UGI via the caller context |  Major | . | Simbarashe Dzinamarira | Simbarashe Dzinamarira |
| [HADOOP-18658](https://issues.apache.org/jira/browse/HADOOP-18658) | snakeyaml dependency: upgrade to v2.0 |  Major | . | PJ Fanning | PJ Fanning |
| [HADOOP-18693](https://issues.apache.org/jira/browse/HADOOP-18693) | Upgrade Apache Derby from 10.10.2.0 to 10.14.2.0 due to CVEs |  Major | build, test | PJ Fanning |  |
| [HADOOP-18712](https://issues.apache.org/jira/browse/HADOOP-18712) | Upgrade to jetty 9.4.51 due to cve |  Major | common | PJ Fanning | PJ Fanning |
| [HADOOP-18727](https://issues.apache.org/jira/browse/HADOOP-18727) | Fix WriteOperations.listMultipartUploads function description |  Trivial | fs/s3 | Dongjoon Hyun | Dongjoon Hyun |
| [HADOOP-18761](https://issues.apache.org/jira/browse/HADOOP-18761) | Remove mysql-connector-java |  Blocker | . | Wei-Chiu Chuang | Wei-Chiu Chuang |


