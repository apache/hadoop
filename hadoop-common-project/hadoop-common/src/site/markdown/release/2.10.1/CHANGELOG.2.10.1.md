
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
# "Apache Hadoop" Changelog

## Release 2.10.1 - 2020-09-14

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-14597](https://issues.apache.org/jira/browse/HADOOP-14597) | Native compilation broken with OpenSSL-1.1.0 because EVP\_CIPHER\_CTX has been made opaque |  Major | . | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-7069](https://issues.apache.org/jira/browse/MAPREDUCE-7069) | Add ability to specify user environment variables individually |  Major | . | Jim Brennan | Jim Brennan |
| [HDFS-13511](https://issues.apache.org/jira/browse/HDFS-13511) | Provide specialized exception when block length cannot be obtained |  Major | . | Ted Yu | Gabor Bota |
| [HDFS-10659](https://issues.apache.org/jira/browse/HDFS-10659) | Namenode crashes after Journalnode re-installation in an HA cluster due to missing paxos directory |  Major | ha, journal-node | Amit Anand | star |
| [MAPREDUCE-7208](https://issues.apache.org/jira/browse/MAPREDUCE-7208) | Tuning TaskRuntimeEstimator |  Minor | . | Ahmed Hussein | Ahmed Hussein |
| [HDFS-14979](https://issues.apache.org/jira/browse/HDFS-14979) | [Observer Node] Balancer should submit getBlocks to Observer Node when possible |  Major | balancer & mover, hdfs | Erik Krogen | Erik Krogen |
| [HDFS-14991](https://issues.apache.org/jira/browse/HDFS-14991) | Backport better time precision of configuration#getTimeDuration to branch-2 to support SBN read |  Major | hdfs | Chen Liang | Chen Liang |
| [HDFS-14952](https://issues.apache.org/jira/browse/HDFS-14952) | Skip safemode if blockTotal is 0 in new NN |  Trivial | namenode | Rajesh Balamohan | Xiaoqiao He |
| [YARN-8842](https://issues.apache.org/jira/browse/YARN-8842) | Expose metrics for custom resource types in QueueMetrics |  Major | . | Szilard Nemeth | Szilard Nemeth |
| [HADOOP-16735](https://issues.apache.org/jira/browse/HADOOP-16735) | Make it clearer in config default that EnvironmentVariableCredentialsProvider supports AWS\_SESSION\_TOKEN |  Minor | documentation, fs/s3 | Mingliang Liu | Mingliang Liu |
| [YARN-10012](https://issues.apache.org/jira/browse/YARN-10012) | Guaranteed and max capacity queue metrics for custom resources |  Major | . | Jonathan Hung | Manikandan R |
| [HDFS-15050](https://issues.apache.org/jira/browse/HDFS-15050) | Optimize log information when DFSInputStream meet CannotObtainBlockLengthException |  Major | dfsclient | Xiaoqiao He | Xiaoqiao He |
| [YARN-10039](https://issues.apache.org/jira/browse/YARN-10039) | Allow disabling app submission from REST endpoints |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-9894](https://issues.apache.org/jira/browse/YARN-9894) | CapacitySchedulerPerf test for measuring hundreds of apps in a large number of queues. |  Major | capacity scheduler, test | Eric Payne | Eric Payne |
| [YARN-10009](https://issues.apache.org/jira/browse/YARN-10009) | In Capacity Scheduler, DRC can treat minimum user limit percent as a max when custom resource is defined |  Critical | capacity scheduler | Eric Payne | Eric Payne |
| [HADOOP-16753](https://issues.apache.org/jira/browse/HADOOP-16753) | Refactor HAAdmin |  Major | ha | Akira Ajisaka | Xieming Li |
| [HDFS-14968](https://issues.apache.org/jira/browse/HDFS-14968) | Add ability to know datanode staleness |  Minor | datanode, logging, namenode | Ahmed Hussein | Ahmed Hussein |
| [MAPREDUCE-7262](https://issues.apache.org/jira/browse/MAPREDUCE-7262) | MRApp helpers block for long intervals (500ms) |  Minor | mr-am | Ahmed Hussein | Ahmed Hussein |
| [YARN-10084](https://issues.apache.org/jira/browse/YARN-10084) | Allow inheritance of max app lifetime / default app lifetime |  Major | capacity scheduler | Eric Payne | Eric Payne |
| [HDFS-15046](https://issues.apache.org/jira/browse/HDFS-15046) | Backport HDFS-7060 to branch-2.10 |  Major | datanode | Wei-Chiu Chuang | Lisheng Sun |
| [HDFS-12491](https://issues.apache.org/jira/browse/HDFS-12491) | Support wildcard in CLASSPATH for libhdfs |  Major | libhdfs | John Zhuge | Muhammad Samir Khan |
| [YARN-10116](https://issues.apache.org/jira/browse/YARN-10116) | Expose diagnostics in RMAppManager summary |  Major | . | Jonathan Hung | Jonathan Hung |
| [HADOOP-16739](https://issues.apache.org/jira/browse/HADOOP-16739) | Fix native build failure of hadoop-pipes on CentOS 8 |  Major | tools/pipes | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-14758](https://issues.apache.org/jira/browse/HDFS-14758) | Decrease lease hard limit |  Minor | . | Eric Payne | Hemanth Boyina |
| [YARN-9018](https://issues.apache.org/jira/browse/YARN-9018) | Add functionality to AuxiliaryLocalPathHandler to return all locations to read for a given path |  Major | . | Kuhu Shukla | Kuhu Shukla |
| [HADOOP-16882](https://issues.apache.org/jira/browse/HADOOP-16882) | Update jackson-databind to 2.9.10.2 in branch-3.1, branch-2.10 |  Blocker | . | Wei-Chiu Chuang | Lisheng Sun |
| [HADOOP-16776](https://issues.apache.org/jira/browse/HADOOP-16776) | backport HADOOP-16775: distcp copies to s3 are randomly corrupted |  Blocker | tools/distcp | Amir Shenavandeh | Amir Shenavandeh |
| [HDFS-15197](https://issues.apache.org/jira/browse/HDFS-15197) | [SBN read] Change ObserverRetryOnActiveException log to debug |  Minor | hdfs | Chen Liang | Chen Liang |
| [YARN-10200](https://issues.apache.org/jira/browse/YARN-10200) | Add number of containers to RMAppManager summary |  Major | . | Jonathan Hung | Jonathan Hung |
| [YARN-8213](https://issues.apache.org/jira/browse/YARN-8213) | Add Capacity Scheduler performance metrics |  Critical | capacityscheduler, metrics | Weiwei Yang | Weiwei Yang |
| [HADOOP-16952](https://issues.apache.org/jira/browse/HADOOP-16952) | Add .diff to gitignore |  Minor | . | Ayush Saxena | Ayush Saxena |
| [YARN-10212](https://issues.apache.org/jira/browse/YARN-10212) | Create separate configuration for max global AM attempts |  Major | . | Jonathan Hung | Bilwa S T |
| [HDFS-14476](https://issues.apache.org/jira/browse/HDFS-14476) | lock too long when fix inconsistent blocks between disk and in-memory |  Major | datanode | Sean Chow | Sean Chow |
| [YARN-8680](https://issues.apache.org/jira/browse/YARN-8680) | YARN NM: Implement Iterable Abstraction for LocalResourceTracker state |  Critical | yarn | Pradeep Ambati | Pradeep Ambati |
| [YARN-9954](https://issues.apache.org/jira/browse/YARN-9954) | Configurable max application tags and max tag length |  Major | . | Jonathan Hung | Bilwa S T |
| [YARN-10260](https://issues.apache.org/jira/browse/YARN-10260) | Allow transitioning queue from DRAINING to RUNNING state |  Major | . | Jonathan Hung | Bilwa S T |
| [HDFS-15264](https://issues.apache.org/jira/browse/HDFS-15264) | Backport Datanode detection to branch-2.10 |  Major | . | Lisheng Sun | Lisheng Sun |
| [YARN-6492](https://issues.apache.org/jira/browse/YARN-6492) | Generate queue metrics for each partition |  Major | capacity scheduler | Jonathan Hung | Manikandan R |
| [HADOOP-17047](https://issues.apache.org/jira/browse/HADOOP-17047) | TODO comments exist in trunk while the related issues are already fixed. |  Trivial | . | Rungroj Maipradit | Rungroj Maipradit |
| [HADOOP-17090](https://issues.apache.org/jira/browse/HADOOP-17090) | Increase precommit job timeout from 5 hours to 20 hours |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [YARN-10297](https://issues.apache.org/jira/browse/YARN-10297) | TestContinuousScheduling#testFairSchedulerContinuousSchedulingInitTime fails intermittently |  Major | . | Jonathan Hung | Jim Brennan |
| [HADOOP-17127](https://issues.apache.org/jira/browse/HADOOP-17127) | Use RpcMetrics.TIMEUNIT to initialize rpc queueTime and processingTime |  Minor | common | Jim Brennan | Jim Brennan |
| [HDFS-15404](https://issues.apache.org/jira/browse/HDFS-15404) | ShellCommandFencer should expose info about source |  Major | . | Chen Liang | Chen Liang |
| [YARN-10343](https://issues.apache.org/jira/browse/YARN-10343) | Legacy RM UI should include labeled metrics for allocated, total, and reserved resources. |  Major | . | Eric Payne | Eric Payne |
| [YARN-1529](https://issues.apache.org/jira/browse/YARN-1529) | Add Localization overhead metrics to NM |  Major | nodemanager | Gera Shegalov | Jim Brennan |
| [YARN-10251](https://issues.apache.org/jira/browse/YARN-10251) | Show extended resources on legacy RM UI. |  Major | . | Eric Payne | Eric Payne |
| [HADOOP-17159](https://issues.apache.org/jira/browse/HADOOP-17159) | Make UGI support forceful relogin from keytab ignoring the last login time |  Major | security | Sandeep Guggilam | Sandeep Guggilam |
| [HADOOP-17251](https://issues.apache.org/jira/browse/HADOOP-17251) | Upgrade netty-all to 4.1.50.Final on branch-2.10 |  Major | . | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-17249](https://issues.apache.org/jira/browse/HADOOP-17249) | Upgrade jackson-databind to 2.9.10.6 on branch-2.10 |  Major | . | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-17253](https://issues.apache.org/jira/browse/HADOOP-17253) | Upgrade zookeeper to 3.4.14 on branch-2.10 |  Major | . | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-17254](https://issues.apache.org/jira/browse/HADOOP-17254) | Upgrade hbase to 1.4.13 on branch-2.10 |  Major | . | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-16918](https://issues.apache.org/jira/browse/HADOOP-16918) | Dependency update for Hadoop 2.10 |  Major | . | Wei-Chiu Chuang |  |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-9376](https://issues.apache.org/jira/browse/HDFS-9376) | TestSeveralNameNodes fails occasionally |  Major | test | Kihwal Lee | Masatake Iwasaki |
| [MAPREDUCE-6881](https://issues.apache.org/jira/browse/MAPREDUCE-6881) | Fix warnings from Spotbugs in hadoop-mapreduce |  Major | . | Weiwei Yang | Weiwei Yang |
| [HADOOP-14354](https://issues.apache.org/jira/browse/HADOOP-14354) | SysInfoWindows is not thread safe |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HADOOP-14400](https://issues.apache.org/jira/browse/HADOOP-14400) | Fix warnings from spotbugs in hadoop-tools |  Major | tools | Weiwei Yang | Weiwei Yang |
| [YARN-6515](https://issues.apache.org/jira/browse/YARN-6515) | Fix warnings from Spotbugs in hadoop-yarn-server-nodemanager |  Major | nodemanager | Naganarasimha G R | Naganarasimha G R |
| [HDFS-11696](https://issues.apache.org/jira/browse/HDFS-11696) | Fix warnings from Spotbugs in hadoop-hdfs |  Major | . | Yiqun Lin | Yiqun Lin |
| [HADOOP-14910](https://issues.apache.org/jira/browse/HADOOP-14910) | Upgrade netty-all jar to latest 4.0.x.Final |  Critical | . | Vinayakumar B | Vinayakumar B |
| [YARN-7589](https://issues.apache.org/jira/browse/YARN-7589) | TestPBImplRecords fails with NullPointerException |  Major | . | Jason Darrell Lowe | Daniel Templeton |
| [YARN-7739](https://issues.apache.org/jira/browse/YARN-7739) | DefaultAMSProcessor should properly check customized resource types against minimum/maximum allocation |  Blocker | . | Wangda Tan | Wangda Tan |
| [YARN-8011](https://issues.apache.org/jira/browse/YARN-8011) | TestOpportunisticContainerAllocatorAMService#testContainerPromoteAndDemoteBeforeContainerStart fails sometimes in trunk |  Minor | . | Tao Yang | Tao Yang |
| [HADOOP-15062](https://issues.apache.org/jira/browse/HADOOP-15062) | TestCryptoStreamsWithOpensslAesCtrCryptoCodec fails on Debian 9 |  Major | . | Miklos Szegedi | Miklos Szegedi |
| [YARN-8004](https://issues.apache.org/jira/browse/YARN-8004) | Add unit tests for inter queue preemption for dominant resource calculator |  Critical | yarn | Sumana Sathish | Zian Chen |
| [YARN-8210](https://issues.apache.org/jira/browse/YARN-8210) | AMRMClient logging on every heartbeat to track updation of AM RM token causes too many log lines to be generated in AM logs |  Major | yarn | Suma Shivaprasad | Suma Shivaprasad |
| [YARN-8202](https://issues.apache.org/jira/browse/YARN-8202) | DefaultAMSProcessor should properly check units of requested custom resource types against minimum/maximum allocation |  Blocker | . | Szilard Nemeth | Szilard Nemeth |
| [YARN-8179](https://issues.apache.org/jira/browse/YARN-8179) | Preemption does not happen due to natural\_termination\_factor when DRF is used |  Major | . | kyungwan nam | kyungwan nam |
| [YARN-8369](https://issues.apache.org/jira/browse/YARN-8369) | Javadoc build failed due to "bad use of '\>'" |  Critical | build, docs | Takanobu Asanuma | Takanobu Asanuma |
| [YARN-8382](https://issues.apache.org/jira/browse/YARN-8382) | cgroup file leak in NM |  Major | nodemanager | Hu Ziqian | Hu Ziqian |
| [HDFS-12459](https://issues.apache.org/jira/browse/HDFS-12459) | Fix revert: Add new op GETFILEBLOCKLOCATIONS to WebHDFS REST API |  Major | webhdfs | Weiwei Yang | Weiwei Yang |
| [HDFS-14054](https://issues.apache.org/jira/browse/HDFS-14054) | TestLeaseRecovery2: testHardLeaseRecoveryAfterNameNodeRestart2 and testHardLeaseRecoveryWithRenameAfterNameNodeRestart are flaky |  Major | . | Zsolt Venczel | Zsolt Venczel |
| [YARN-9205](https://issues.apache.org/jira/browse/YARN-9205) | When using custom resource type, application will fail to run due to the CapacityScheduler throws InvalidResourceRequestException(GREATER\_THEN\_MAX\_ALLOCATION) |  Critical | . | Zhankun Tang | Zhankun Tang |
| [YARN-4901](https://issues.apache.org/jira/browse/YARN-4901) | QueueMetrics needs to be cleared before MockRM is initialized |  Major | scheduler | Daniel Templeton | Peter Bacsko |
| [HDFS-14647](https://issues.apache.org/jira/browse/HDFS-14647) | NPE during secure namenode startup |  Major | hdfs | Fengnan Li | Fengnan Li |
| [HADOOP-16255](https://issues.apache.org/jira/browse/HADOOP-16255) | ChecksumFS.Make FileSystem.rename(path, path, options) doesn't rename checksum |  Major | fs | Steve Loughran | Jungtaek Lim |
| [HADOOP-16580](https://issues.apache.org/jira/browse/HADOOP-16580) | Disable retry of FailoverOnNetworkExceptionRetry in case of AccessControlException |  Major | common | Adam Antal | Adam Antal |
| [HADOOP-16662](https://issues.apache.org/jira/browse/HADOOP-16662) | Remove unnecessary InnerNode check in NetworkTopology#add() |  Minor | . | Lisheng Sun | Lisheng Sun |
| [YARN-9915](https://issues.apache.org/jira/browse/YARN-9915) | Fix FindBug issue in QueueMetrics |  Minor | . | Prabhu Joseph | Prabhu Joseph |
| [HDFS-12749](https://issues.apache.org/jira/browse/HDFS-12749) | DN may not send block report to NN after NN restart |  Major | datanode | TanYuxin | Xiaoqiao He |
| [HDFS-13901](https://issues.apache.org/jira/browse/HDFS-13901) | INode access time is ignored because of race between open and rename |  Major | . | Jinglun | Jinglun |
| [HDFS-14931](https://issues.apache.org/jira/browse/HDFS-14931) | hdfs crypto commands limit column width |  Major | . | Eric Badger | Eric Badger |
| [YARN-9945](https://issues.apache.org/jira/browse/YARN-9945) | Fix javadoc in FederationProxyProviderUtil in branch-2 |  Major | . | Jonathan Hung | Jonathan Hung |
| [HDFS-14806](https://issues.apache.org/jira/browse/HDFS-14806) | Bootstrap standby may fail if used in-progress tailing |  Major | namenode | Chen Liang | Chen Liang |
| [HDFS-14941](https://issues.apache.org/jira/browse/HDFS-14941) | Potential editlog race condition can cause corrupted file |  Major | namenode | Chen Liang | Chen Liang |
| [HDFS-14958](https://issues.apache.org/jira/browse/HDFS-14958) | TestBalancerWithNodeGroup is not using NetworkTopologyWithNodeGroup |  Minor | hdfs | Jim Brennan | Jim Brennan |
| [HDFS-14884](https://issues.apache.org/jira/browse/HDFS-14884) | Add sanity check that zone key equals feinfo key while setting Xattrs |  Major | encryption, hdfs | Mukul Kumar Singh | Mukul Kumar Singh |
| [HADOOP-15097](https://issues.apache.org/jira/browse/HADOOP-15097) | AbstractContractDeleteTest::testDeleteNonEmptyDirRecursive with misleading path |  Minor | fs, test | zhoutai.zt | Xieming Li |
| [HADOOP-16700](https://issues.apache.org/jira/browse/HADOOP-16700) | RpcQueueTime may be negative when the response has to be sent later |  Minor | . | xuzq | xuzq |
| [YARN-9838](https://issues.apache.org/jira/browse/YARN-9838) | Fix resource inconsistency for queues when moving app with reserved container to another queue |  Critical | capacity scheduler | jiulongzhu | jiulongzhu |
| [HDFS-14973](https://issues.apache.org/jira/browse/HDFS-14973) | Balancer getBlocks RPC dispersal does not function properly |  Major | balancer & mover | Erik Krogen | Erik Krogen |
| [MAPREDUCE-7240](https://issues.apache.org/jira/browse/MAPREDUCE-7240) | Exception ' Invalid event: TA\_TOO\_MANY\_FETCH\_FAILURE at SUCCESS\_FINISHING\_CONTAINER' cause job error |  Critical | . | luhuachao | luhuachao |
| [HDFS-14986](https://issues.apache.org/jira/browse/HDFS-14986) | ReplicaCachingGetSpaceUsed throws  ConcurrentModificationException |  Major | datanode, performance | Ryan Wu | Aiphago |
| [MAPREDUCE-7249](https://issues.apache.org/jira/browse/MAPREDUCE-7249) | Invalid event TA\_TOO\_MANY\_FETCH\_FAILURE at SUCCESS\_CONTAINER\_CLEANUP causes job failure |  Critical | applicationmaster, mrv2 | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [HDFS-15005](https://issues.apache.org/jira/browse/HDFS-15005) | Backport HDFS-12300 to branch-2 |  Major | hdfs | Chao Sun | Chao Sun |
| [HDFS-15017](https://issues.apache.org/jira/browse/HDFS-15017) | Remove redundant import of AtomicBoolean in NameNodeConnector. |  Major | balancer & mover, hdfs | Konstantin Shvachko | Chao Sun |
| [HADOOP-16754](https://issues.apache.org/jira/browse/HADOOP-16754) | Fix docker failed to build yetus/hadoop |  Blocker | build | Kevin Su | Kevin Su |
| [HDFS-15032](https://issues.apache.org/jira/browse/HDFS-15032) | Balancer crashes when it fails to contact an unavailable NN via ObserverReadProxyProvider |  Major | balancer & mover | Erik Krogen | Erik Krogen |
| [HDFS-15036](https://issues.apache.org/jira/browse/HDFS-15036) | Active NameNode should not silently fail the image transfer |  Major | namenode | Konstantin Shvachko | Chen Liang |
| [HDFS-15048](https://issues.apache.org/jira/browse/HDFS-15048) | Fix findbug in DirectoryScanner |  Major | . | Takanobu Asanuma | Masatake Iwasaki |
| [HDFS-14519](https://issues.apache.org/jira/browse/HDFS-14519) | NameQuota is not update after concat operation, so namequota is wrong |  Major | . | Ranith Sardar | Ranith Sardar |
| [HDFS-15076](https://issues.apache.org/jira/browse/HDFS-15076) | Fix tests that hold FSDirectory lock, without holding FSNamesystem lock. |  Major | test | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-14934](https://issues.apache.org/jira/browse/HDFS-14934) | [SBN Read] Standby NN throws many InterruptedExceptions when dfs.ha.tail-edits.period is 0 |  Major | . | Takanobu Asanuma | Ayush Saxena |
| [HADOOP-16789](https://issues.apache.org/jira/browse/HADOOP-16789) | In TestZKFailoverController, restore changes from HADOOP-11149 that were dropped by HDFS-6440 |  Minor | common | Jim Brennan | Jim Brennan |
| [HDFS-15077](https://issues.apache.org/jira/browse/HDFS-15077) | Fix intermittent failure of TestDFSClientRetries#testLeaseRenewSocketTimeout |  Minor | test | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-7387](https://issues.apache.org/jira/browse/YARN-7387) | org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestIncreaseAllocationExpirer fails intermittently |  Major | . | Miklos Szegedi | Jim Brennan |
| [YARN-8672](https://issues.apache.org/jira/browse/YARN-8672) | TestContainerManager#testLocalingResourceWhileContainerRunning occasionally times out |  Major | nodemanager | Jason Darrell Lowe | Chandni Singh |
| [MAPREDUCE-7252](https://issues.apache.org/jira/browse/MAPREDUCE-7252) | Handling 0 progress in SimpleExponential task runtime estimator |  Minor | . | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-16749](https://issues.apache.org/jira/browse/HADOOP-16749) | Configuration parsing of CDATA values are blank |  Major | conf | Jonathan Turner Eagles | Daryn Sharp |
| [HDFS-15099](https://issues.apache.org/jira/browse/HDFS-15099) | [SBN Read] checkOperation(WRITE) should throw ObserverRetryOnActiveException on ObserverNode |  Major | namenode | Konstantin Shvachko | Chen Liang |
| [HADOOP-16683](https://issues.apache.org/jira/browse/HADOOP-16683) | Disable retry of FailoverOnNetworkExceptionRetry in case of wrapped AccessControlException |  Major | common | Adam Antal | Adam Antal |
| [HDFS-13339](https://issues.apache.org/jira/browse/HDFS-13339) | Volume reference can't be released and may lead to deadlock when DataXceiver does a check volume |  Critical | datanode | liaoyuxiangqin | Zsolt Venczel |
| [MAPREDUCE-7256](https://issues.apache.org/jira/browse/MAPREDUCE-7256) | Fix javadoc error in SimpleExponentialSmoothing |  Minor | documentation | Masatake Iwasaki | Masatake Iwasaki |
| [MAPREDUCE-7247](https://issues.apache.org/jira/browse/MAPREDUCE-7247) | Modify HistoryServerRest.html content,change The job attempt id‘s datatype from string to int |  Major | documentation | zhaoshengjie | zhaoshengjie |
| [HADOOP-16808](https://issues.apache.org/jira/browse/HADOOP-16808) | Use forkCount and reuseForks parameters instead of forkMode in the config of maven surefire plugin |  Minor | build | Akira Ajisaka | Xieming Li |
| [HADOOP-16793](https://issues.apache.org/jira/browse/HADOOP-16793) |  Remove WARN log when ipc connection interrupted in Client#handleSaslConnectionFailure() |  Minor | . | Lisheng Sun | Lisheng Sun |
| [YARN-9790](https://issues.apache.org/jira/browse/YARN-9790) | Failed to set default-application-lifetime if maximum-application-lifetime is less than or equal to zero |  Major | . | kyungwan nam | kyungwan nam |
| [HDFS-13179](https://issues.apache.org/jira/browse/HDFS-13179) | TestLazyPersistReplicaRecovery#testDnRestartWithSavedReplicas fails intermittently |  Critical | fs | Gabor Bota | Ahmed Hussein |
| [MAPREDUCE-7259](https://issues.apache.org/jira/browse/MAPREDUCE-7259) | testSpeculateSuccessfulWithUpdateEvents fails Intermittently |  Minor | . | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15146](https://issues.apache.org/jira/browse/HDFS-15146) | TestBalancerRPCDelay.testBalancerRPCDelay fails intermittently |  Minor | . | Ahmed Hussein | Ahmed Hussein |
| [MAPREDUCE-7079](https://issues.apache.org/jira/browse/MAPREDUCE-7079) | JobHistory#ServiceStop implementation is incorrect |  Major | . | Jason Darrell Lowe | Ahmed Hussein |
| [HDFS-15118](https://issues.apache.org/jira/browse/HDFS-15118) | [SBN Read] Slow clients when Observer reads are enabled but there are no Observers on the cluster. |  Major | hdfs-client | Konstantin Shvachko | Chen Liang |
| [HDFS-15148](https://issues.apache.org/jira/browse/HDFS-15148) | dfs.namenode.send.qop.enabled should not apply to primary NN port |  Major | . | Chen Liang | Chen Liang |
| [YARN-8292](https://issues.apache.org/jira/browse/YARN-8292) | Fix the dominant resource preemption cannot happen when some of the resource vector becomes negative |  Critical | yarn | Sumana Sathish | Wangda Tan |
| [HDFS-15115](https://issues.apache.org/jira/browse/HDFS-15115) | Namenode crash caused by NPE in BlockPlacementPolicyDefault when dynamically change logger to debug |  Major | . | wangzhixiang | wangzhixiang |
| [HADOOP-16849](https://issues.apache.org/jira/browse/HADOOP-16849) | start-build-env.sh behaves incorrectly when username is numeric only |  Minor | build | Jihyun Cho | Jihyun Cho |
| [HDFS-15161](https://issues.apache.org/jira/browse/HDFS-15161) | When evictableMmapped or evictable size is zero, do not throw NoSuchElementException in ShortCircuitCache#close() |  Major | . | Lisheng Sun | Lisheng Sun |
| [HDFS-15164](https://issues.apache.org/jira/browse/HDFS-15164) | Fix TestDelegationTokensWithHA |  Major | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-16868](https://issues.apache.org/jira/browse/HADOOP-16868) | ipc.Server readAndProcess threw NullPointerException |  Major | rpc-server | Tsz-wo Sze | Tsz-wo Sze |
| [HADOOP-16869](https://issues.apache.org/jira/browse/HADOOP-16869) |  Upgrade findbugs-maven-plugin to 3.0.5 to fix mvn findbugs:findbugs failure |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15052](https://issues.apache.org/jira/browse/HDFS-15052) | WebHDFS getTrashRoot leads to OOM due to FileSystem object creation |  Major | webhdfs | Wei-Chiu Chuang | Masatake Iwasaki |
| [HDFS-15185](https://issues.apache.org/jira/browse/HDFS-15185) | StartupProgress reports edits segments until the entire startup completes |  Major | namenode | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-15166](https://issues.apache.org/jira/browse/HDFS-15166) | Remove redundant field fStream in ByteStringLog |  Major | . | Konstantin Shvachko | Xieming Li |
| [YARN-10140](https://issues.apache.org/jira/browse/YARN-10140) | TestTimelineAuthFilterForV2 fails due to login failures in branch-2.10 |  Major | timelineservice | Ahmed Hussein | Ahmed Hussein |
| [YARN-10156](https://issues.apache.org/jira/browse/YARN-10156) | Fix typo 'complaint' which means quite different in Federation.md |  Minor | documentation, federation | Sungpeo Kook | Sungpeo Kook |
| [HDFS-15147](https://issues.apache.org/jira/browse/HDFS-15147) | LazyPersistTestCase wait logic is error-prone |  Minor | . | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-16891](https://issues.apache.org/jira/browse/HADOOP-16891) | Upgrade jackson-databind to 2.9.10.3 |  Blocker | . | Siyao Meng | Siyao Meng |
| [HDFS-15204](https://issues.apache.org/jira/browse/HDFS-15204) | TestRetryCacheWithHA testRemoveCacheDescriptor fails intermittently |  Major | hdfs | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-16840](https://issues.apache.org/jira/browse/HADOOP-16840) | AliyunOSS: getFileStatus throws FileNotFoundException in versioning bucket |  Major | fs/oss | wujinhu | wujinhu |
| [YARN-9427](https://issues.apache.org/jira/browse/YARN-9427) | TestContainerSchedulerQueuing.testKillOnlyRequiredOpportunisticContainers fails sporadically |  Major | scheduler, test | Prabhu Joseph | Ahmed Hussein |
| [HDFS-11396](https://issues.apache.org/jira/browse/HDFS-11396) | TestNameNodeMetadataConsistency#testGenerationStampInFuture timed out |  Minor | namenode, test | John Zhuge | Ayush Saxena |
| [HADOOP-16949](https://issues.apache.org/jira/browse/HADOOP-16949) | pylint fails in the build environment |  Critical | build | Akira Ajisaka | Akira Ajisaka |
| [YARN-10227](https://issues.apache.org/jira/browse/YARN-10227) | Pull YARN-8242 back to branch-2.10 |  Major | yarn | Jim Brennan | Jim Brennan |
| [YARN-2710](https://issues.apache.org/jira/browse/YARN-2710) | RM HA tests failed intermittently on trunk |  Major | client | Wangda Tan | Ahmed Hussein |
| [MAPREDUCE-7272](https://issues.apache.org/jira/browse/MAPREDUCE-7272) | TaskAttemptListenerImpl excessive log messages |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15283](https://issues.apache.org/jira/browse/HDFS-15283) | Cache pool MAXTTL is not persisted and restored on cluster restart |  Major | namenode | Stephen O'Donnell | Stephen O'Donnell |
| [HADOOP-16944](https://issues.apache.org/jira/browse/HADOOP-16944) | Use Yetus 0.12.0 in GitHub PR |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15276](https://issues.apache.org/jira/browse/HDFS-15276) | Concat on INodeRefernce fails with illegal state exception |  Critical | . | Hemanth Boyina | Hemanth Boyina |
| [HADOOP-16361](https://issues.apache.org/jira/browse/HADOOP-16361) | TestSecureLogins#testValidKerberosName fails on branch-2 |  Major | security | Jim Brennan | Jim Brennan |
| [HDFS-15281](https://issues.apache.org/jira/browse/HDFS-15281) | ZKFC ignores dfs.namenode.rpc-bind-host and uses dfs.namenode.rpc-address to bind to host address |  Major | ha, namenode | Dhiraj Hegde | Dhiraj Hegde |
| [HDFS-15297](https://issues.apache.org/jira/browse/HDFS-15297) | TestNNHandlesBlockReportPerStorage::blockReport\_02 fails intermittently in trunk |  Major | datanode, test | Mingliang Liu | Ayush Saxena |
| [YARN-8193](https://issues.apache.org/jira/browse/YARN-8193) | YARN RM hangs abruptly (stops allocating resources) when running successive applications. |  Blocker | yarn | Zian Chen | Zian Chen |
| [YARN-10255](https://issues.apache.org/jira/browse/YARN-10255) | fix intermittent failure TestContainerSchedulerQueuing.testContainerUpdateExecTypeGuaranteedToOpportunistic in branch-2.10 |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [HDFS-15302](https://issues.apache.org/jira/browse/HDFS-15302) | Backport HDFS-15286 to branch-2.x |  Blocker | . | Akira Ajisaka | Hemanth Boyina |
| [YARN-8959](https://issues.apache.org/jira/browse/YARN-8959) | TestContainerResizing fails randomly |  Minor | . | Bibin Chundatt | Ahmed Hussein |
| [HDFS-15323](https://issues.apache.org/jira/browse/HDFS-15323) | StandbyNode fails transition to active due to insufficient transaction tailing |  Major | namenode, qjm | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-15565](https://issues.apache.org/jira/browse/HADOOP-15565) | ViewFileSystem.close doesn't close child filesystems and causes FileSystem objects leak. |  Major | . | Jinglun | Jinglun |
| [YARN-9444](https://issues.apache.org/jira/browse/YARN-9444) | YARN API ResourceUtils's getRequestedResourcesFromConfig doesn't recognize yarn.io/gpu as a valid resource |  Minor | api | Gergely Pollak | Gergely Pollak |
| [HDFS-15038](https://issues.apache.org/jira/browse/HDFS-15038) | TestFsck testFsckListCorruptSnapshotFiles is failing in trunk |  Major | . | Hemanth Boyina | Hemanth Boyina |
| [HADOOP-17045](https://issues.apache.org/jira/browse/HADOOP-17045) | `Configuration` javadoc describes supporting environment variables, but the feature is not available |  Minor | . | Nick Dimiduk | Masatake Iwasaki |
| [HDFS-15293](https://issues.apache.org/jira/browse/HDFS-15293) | Relax the condition for accepting a fsimage when receiving a checkpoint |  Critical | namenode | Chen Liang | Chen Liang |
| [HADOOP-17052](https://issues.apache.org/jira/browse/HADOOP-17052) | NetUtils.connect() throws unchecked exception (UnresolvedAddressException) causing clients to abort |  Major | net | Dhiraj Hegde | Dhiraj Hegde |
| [HADOOP-17062](https://issues.apache.org/jira/browse/HADOOP-17062) | Fix shelldocs path in Jenkinsfile |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15386](https://issues.apache.org/jira/browse/HDFS-15386) | ReplicaNotFoundException keeps happening in DN after removing multiple DN's data directories |  Major | . | Toshihiro Suzuki | Toshihiro Suzuki |
| [YARN-10300](https://issues.apache.org/jira/browse/YARN-10300) | appMasterHost not set in RM ApplicationSummary when AM fails before first heartbeat |  Major | . | Eric Badger | Eric Badger |
| [HADOOP-17059](https://issues.apache.org/jira/browse/HADOOP-17059) | ArrayIndexOfboundsException in ViewFileSystem#listStatus |  Major | viewfs | Hemanth Boyina | Hemanth Boyina |
| [YARN-10312](https://issues.apache.org/jira/browse/YARN-10312) | Add support for yarn logs -logFile to retain backward compatibility |  Major | client | Jim Brennan | Jim Brennan |
| [HDFS-12453](https://issues.apache.org/jira/browse/HDFS-12453) | TestDataNodeHotSwapVolumes fails in trunk Jenkins runs |  Critical | test | Arpit Agarwal | Lei (Eddy) Xu |
| [HADOOP-17089](https://issues.apache.org/jira/browse/HADOOP-17089) | WASB: Update azure-storage-java SDK |  Critical | fs/azure | Thomas Marqardt | Thomas Marqardt |
| [HDFS-15421](https://issues.apache.org/jira/browse/HDFS-15421) | IBR leak causes standby NN to be stuck in safe mode |  Blocker | namenode | Kihwal Lee | Akira Ajisaka |
| [YARN-9903](https://issues.apache.org/jira/browse/YARN-9903) | Support reservations continue looking for Node Labels |  Major | . | Tarun Parimi | Jim Brennan |
| [HADOOP-17094](https://issues.apache.org/jira/browse/HADOOP-17094) | vulnerabilities reported in jackson and jackson-databind in branch-2.10 |  Major | . | Ahmed Hussein | Ahmed Hussein |
| [HADOOP-17120](https://issues.apache.org/jira/browse/HADOOP-17120) | Fix failure of docker image creation due to pip2 install error |  Major | . | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-10348](https://issues.apache.org/jira/browse/YARN-10348) | Allow RM to always cancel tokens after app completes |  Major | yarn | Jim Brennan | Jim Brennan |
| [HDFS-14498](https://issues.apache.org/jira/browse/HDFS-14498) | LeaseManager can loop forever on the file for which create has failed |  Major | namenode | Sergey Shelukhin | Stephen O'Donnell |
| [YARN-4771](https://issues.apache.org/jira/browse/YARN-4771) | Some containers can be skipped during log aggregation after NM restart |  Major | nodemanager | Jason Darrell Lowe | Jim Brennan |
| [HDFS-15313](https://issues.apache.org/jira/browse/HDFS-15313) | Ensure inodes in active filesystem are not deleted during snapshot delete |  Major | snapshots | Shashikant Banerjee | Shashikant Banerjee |
| [YARN-10363](https://issues.apache.org/jira/browse/YARN-10363) | TestRMAdminCLI.testHelp is failing in branch-2.10 |  Major | yarn | Jim Brennan | Bilwa S T |
| [HADOOP-17184](https://issues.apache.org/jira/browse/HADOOP-17184) | Add --mvn-custom-repos parameter to yetus calls |  Major | build | Mingliang Liu | Mingliang Liu |
| [HADOOP-17185](https://issues.apache.org/jira/browse/HADOOP-17185) | Set MAVEN\_OPTS in Jenkinsfile |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HDFS-15499](https://issues.apache.org/jira/browse/HDFS-15499) | Clean up httpfs/pom.xml to remove aws-java-sdk-s3 exclusion |  Major | httpfs | Mingliang Liu | Mingliang Liu |
| [HADOOP-17164](https://issues.apache.org/jira/browse/HADOOP-17164) | UGI loginUserFromKeytab doesn't set the last login time |  Major | security | Sandeep Guggilam | Sandeep Guggilam |
| [YARN-4575](https://issues.apache.org/jira/browse/YARN-4575) | ApplicationResourceUsageReport should return ALL  reserved resource |  Major | . | Bibin Chundatt | Bibin Chundatt |
| [YARN-7677](https://issues.apache.org/jira/browse/YARN-7677) | Docker image cannot set HADOOP\_CONF\_DIR |  Major | . | Eric Badger | Jim Brennan |
| [YARN-8459](https://issues.apache.org/jira/browse/YARN-8459) | Improve Capacity Scheduler logs to debug invalid states |  Major | capacity scheduler | Wangda Tan | Wangda Tan |
| [MAPREDUCE-7286](https://issues.apache.org/jira/browse/MAPREDUCE-7286) | Fix findbugs warnings in hadoop-mapreduce-project on branch-2.10 |  Minor | . | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-17204](https://issues.apache.org/jira/browse/HADOOP-17204) | Fix typo in Hadoop KMS document |  Trivial | documentation, kms | Akira Ajisaka | Xieming Li |
| [HADOOP-17202](https://issues.apache.org/jira/browse/HADOOP-17202) | Fix findbugs warnings in hadoop-tools on branch-2.10 |  Minor | . | Masatake Iwasaki | Masatake Iwasaki |
| [HDFS-15290](https://issues.apache.org/jira/browse/HDFS-15290) | NPE in HttpServer during NameNode startup |  Major | namenode | Konstantin Shvachko | Simbarashe Dzinamarira |
| [YARN-10358](https://issues.apache.org/jira/browse/YARN-10358) | Fix findbugs warnings in hadoop-yarn-project on branch-2.10 |  Minor | . | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-10177](https://issues.apache.org/jira/browse/YARN-10177) | Backport YARN-7307 to branch-2.10 Allow client/AM update supported resource types via YARN APIs |  Blocker | . | Wei-Chiu Chuang | Wei-Chiu Chuang |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-9404](https://issues.apache.org/jira/browse/YARN-9404) | TestApplicationLifetimeMonitor#testApplicationLifetimeMonitor fails intermittent |  Major | resourcemanager | Prabhu Joseph | Prabhu Joseph |
| [YARN-10072](https://issues.apache.org/jira/browse/YARN-10072) | TestCSAllocateCustomResource failures |  Major | yarn | Jim Brennan | Jim Brennan |
| [HDFS-15125](https://issues.apache.org/jira/browse/HDFS-15125) | Pull back HDFS-11353, HDFS-13993, HDFS-13945, and HDFS-14324 to branch-2.10 |  Minor | hdfs | Jim Brennan | Jim Brennan |
| [YARN-10161](https://issues.apache.org/jira/browse/YARN-10161) | TestRouterWebServicesREST is corrupting STDOUT |  Minor | yarn | Jim Brennan | Jim Brennan |
| [HADOOP-14206](https://issues.apache.org/jira/browse/HADOOP-14206) | TestSFTPFileSystem#testFileExists failure: Invalid encoding for signature |  Major | fs, test | John Zhuge | Jim Brennan |
| [HADOOP-17205](https://issues.apache.org/jira/browse/HADOOP-17205) | Move personality file from Yetus to Hadoop repository |  Major | test, yetus | Chao Sun | Chao Sun |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-14056](https://issues.apache.org/jira/browse/HADOOP-14056) | Update maven-javadoc-plugin to 2.10.4 |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [HADOOP-12940](https://issues.apache.org/jira/browse/HADOOP-12940) | Fix warnings from Spotbugs in hadoop-common |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [YARN-7411](https://issues.apache.org/jira/browse/YARN-7411) | Inter-Queue preemption's computeFixpointAllocation need to handle absolute resources while computing normalizedGuarantee |  Major | resourcemanager | Sunil G | Sunil G |
| [YARN-7541](https://issues.apache.org/jira/browse/YARN-7541) | Node updates don't update the maximum cluster capability for resources other than CPU and memory |  Critical | resourcemanager | Daniel Templeton | Daniel Templeton |
| [HADOOP-15787](https://issues.apache.org/jira/browse/HADOOP-15787) | [JDK11] TestIPC.testRTEDuringConnectionSetup fails |  Major | . | Akira Ajisaka | Zsolt Venczel |
| [HDFS-13404](https://issues.apache.org/jira/browse/HDFS-13404) | RBF: TestRouterWebHDFSContractAppend.testRenameFileBeingAppended fails |  Major | test | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-14590](https://issues.apache.org/jira/browse/HDFS-14590) | [SBN Read] Add the document link to the top page |  Major | documentation | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-16652](https://issues.apache.org/jira/browse/HADOOP-16652) | Backport HADOOP-16587 - "Make AAD endpoint configurable on all Auth flows" to branch-2 |  Minor | fs/azure | Bilahari T H | Bilahari T H |
| [YARN-9773](https://issues.apache.org/jira/browse/YARN-9773) | Add QueueMetrics for Custom Resources |  Major | . | Manikandan R | Manikandan R |
| [HADOOP-16598](https://issues.apache.org/jira/browse/HADOOP-16598) | Backport "HADOOP-16558 [COMMON+HDFS] use protobuf-maven-plugin to generate protobuf classes" to all active branches |  Major | common | Duo Zhang | Duo Zhang |
| [HDFS-14792](https://issues.apache.org/jira/browse/HDFS-14792) | [SBN read] StanbyNode does not come out of safemode while adding new blocks. |  Major | namenode | Konstantin Shvachko |  |
| [HADOOP-16610](https://issues.apache.org/jira/browse/HADOOP-16610) | Upgrade to yetus 0.11.1 and use emoji vote on github pre commit |  Major | build | Duo Zhang | Duo Zhang |
| [HADOOP-16758](https://issues.apache.org/jira/browse/HADOOP-16758) | Refine testing.md to tell user better how to use auth-keys.xml |  Minor | fs/s3 | Mingliang Liu | Mingliang Liu |
| [HADOOP-16609](https://issues.apache.org/jira/browse/HADOOP-16609) | Add Jenkinsfile for all active branches |  Major | build | Duo Zhang | Akira Ajisaka |
| [HADOOP-16734](https://issues.apache.org/jira/browse/HADOOP-16734) | Backport HADOOP-16455- "ABFS: Implement FileSystem.access() method" to branch-2 |  Minor | fs/azure | Bilahari T H | Bilahari T H |
| [HADOOP-16778](https://issues.apache.org/jira/browse/HADOOP-16778) | ABFS: Backport HADOOP-16660  ABFS: Make RetryCount in ExponentialRetryPolicy Configurable to Branch-2 |  Major | fs/azure | Sneha Vijayarajan | Sneha Vijayarajan |
| [HADOOP-14918](https://issues.apache.org/jira/browse/HADOOP-14918) | Remove the Local Dynamo DB test option |  Major | fs/s3 | Steve Loughran | Gabor Bota |
| [HADOOP-16933](https://issues.apache.org/jira/browse/HADOOP-16933) | Backport HADOOP-16890- "ABFS: Change in expiry calculation for MSI token provider" & HADOOP-16825 "ITestAzureBlobFileSystemCheckAccess failing" to branch-2 |  Minor | fs/azure | Bilahari T H | Bilahari T H |
| [HDFS-10499](https://issues.apache.org/jira/browse/HDFS-10499) | TestNameNodeMetadataConsistency#testGenerationStampInFuture Fails Intermittently |  Major | namenode, test | Hanisha Koneru | Yiqun Lin |
| [HADOOP-17199](https://issues.apache.org/jira/browse/HADOOP-17199) | Backport HADOOP-13230 list/getFileStatus changes for preserved directory markers |  Major | fs/s3 | Steve Loughran | Steve Loughran |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-16784](https://issues.apache.org/jira/browse/HADOOP-16784) | Update the year to 2020 |  Major | . | Ayush Saxena | Ayush Saxena |
| [HADOOP-16647](https://issues.apache.org/jira/browse/HADOOP-16647) | Support OpenSSL 1.1.1 LTS |  Critical | security | Wei-Chiu Chuang | Rakesh Radhakrishnan |


