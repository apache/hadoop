
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

## Release 2.7.4 - 2017-08-04

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-7933](https://issues.apache.org/jira/browse/HDFS-7933) | fsck should also report decommissioning replicas. |  Major | namenode | Jitendra Nath Pandey | Xiaoyu Yao |
| [HADOOP-13812](https://issues.apache.org/jira/browse/HADOOP-13812) | Upgrade Tomcat to 6.0.48 |  Blocker | kms | John Zhuge | John Zhuge |
| [HADOOP-13119](https://issues.apache.org/jira/browse/HADOOP-13119) | Add ability to secure log servlet using proxy users |  Major | . | Jeffrey E  Rodriguez | Yuanbo Liu |
| [HADOOP-14138](https://issues.apache.org/jira/browse/HADOOP-14138) | Remove S3A ref from META-INF service discovery, rely on existing core-default entry |  Critical | fs/s3 | Steve Loughran | Steve Loughran |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-9804](https://issues.apache.org/jira/browse/HDFS-9804) | Allow long-running Balancer to login with keytab |  Major | balancer & mover, security | Xiao Chen | Xiao Chen |
| [MAPREDUCE-6304](https://issues.apache.org/jira/browse/MAPREDUCE-6304) | Specifying node labels when submitting MR jobs |  Major | job submission | Jian Fang | Naganarasimha G R |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-8200](https://issues.apache.org/jira/browse/HDFS-8200) | Refactor FSDirStatAndListingOp |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-8131](https://issues.apache.org/jira/browse/HDFS-8131) | Implement a space balanced block placement policy |  Minor | namenode | Liu Shaohui | Liu Shaohui |
| [HDFS-8549](https://issues.apache.org/jira/browse/HDFS-8549) | Abort the balancer if an upgrade is in progress |  Major | balancer & mover | Andrew Wang | Andrew Wang |
| [HDFS-8709](https://issues.apache.org/jira/browse/HDFS-8709) | Clarify automatic sync in FSEditLog#logEdit |  Minor | . | Andrew Wang | Andrew Wang |
| [HADOOP-12259](https://issues.apache.org/jira/browse/HADOOP-12259) | Utility to Dynamic port allocation |  Major | test, util | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-8883](https://issues.apache.org/jira/browse/HDFS-8883) | NameNode Metrics : Add FSNameSystem lock Queue Length |  Major | namenode | Anu Engineer | Anu Engineer |
| [HDFS-9019](https://issues.apache.org/jira/browse/HDFS-9019) | Adding informative message to sticky bit permission denied exception |  Minor | security | Thejas M Nair | Xiaoyu Yao |
| [HDFS-9145](https://issues.apache.org/jira/browse/HDFS-9145) | Tracking methods that hold FSNamesytemLock for too long |  Major | namenode | Jing Zhao | Mingliang Liu |
| [HDFS-9726](https://issues.apache.org/jira/browse/HDFS-9726) | Refactor IBR code to a new class |  Minor | datanode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-12668](https://issues.apache.org/jira/browse/HADOOP-12668) | Support excluding weak Ciphers in HttpServer2 through ssl-server.xml |  Critical | security | Vijay Singh | Vijay Singh |
| [HDFS-9710](https://issues.apache.org/jira/browse/HDFS-9710) | Change DN to send block receipt IBRs in batches |  Major | datanode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-9412](https://issues.apache.org/jira/browse/HDFS-9412) | getBlocks occupies FSLock and takes too long to complete |  Major | balancer & mover, namenode | He Tianyi | He Tianyi |
| [HDFS-9902](https://issues.apache.org/jira/browse/HDFS-9902) | Support different values of dfs.datanode.du.reserved per storage type |  Major | datanode | Pan Yuxuan | Brahma Reddy Battula |
| [HADOOP-13290](https://issues.apache.org/jira/browse/HADOOP-13290) | Appropriate use of generics in FairCallQueue |  Major | ipc | Konstantin Shvachko | Jonathan Hung |
| [YARN-5483](https://issues.apache.org/jira/browse/YARN-5483) | Optimize RMAppAttempt#pullJustFinishedContainers |  Major | . | sandflee | sandflee |
| [HDFS-10798](https://issues.apache.org/jira/browse/HDFS-10798) | Make the threshold of reporting FSNamesystem lock contention configurable |  Major | logging, namenode | Zhe Zhang | Erik Krogen |
| [HDFS-10807](https://issues.apache.org/jira/browse/HDFS-10807) | Doc about upgrading to a version of HDFS with snapshots may be confusing |  Minor | documentation | Mingliang Liu | Mingliang Liu |
| [HDFS-10625](https://issues.apache.org/jira/browse/HDFS-10625) |  VolumeScanner to report why a block is found bad |  Major | datanode, hdfs | Yongjun Zhang | Rushabh S Shah |
| [YARN-5550](https://issues.apache.org/jira/browse/YARN-5550) | TestYarnCLI#testGetContainers should format according to CONTAINER\_PATTERN |  Minor | client, test | Jonathan Hung | Jonathan Hung |
| [HDFS-10817](https://issues.apache.org/jira/browse/HDFS-10817) | Add Logging for Long-held NN Read Locks |  Major | logging, namenode | Erik Krogen | Erik Krogen |
| [YARN-5540](https://issues.apache.org/jira/browse/YARN-5540) | scheduler spends too much time looking at empty priorities |  Major | capacity scheduler, fairscheduler, resourcemanager | Nathan Roberts | Jason Lowe |
| [YARN-3877](https://issues.apache.org/jira/browse/YARN-3877) | YarnClientImpl.submitApplication swallows exceptions |  Minor | client | Steve Loughran | Varun Saxena |
| [MAPREDUCE-6741](https://issues.apache.org/jira/browse/MAPREDUCE-6741) | add MR support to redact job conf properties |  Major | mrv2 | Haibo Chen | Haibo Chen |
| [HDFS-11069](https://issues.apache.org/jira/browse/HDFS-11069) | Tighten the authorization of datanode RPC |  Major | datanode, security | Kihwal Lee | Kihwal Lee |
| [HADOOP-12325](https://issues.apache.org/jira/browse/HADOOP-12325) | RPC Metrics : Add the ability track and log slow RPCs |  Major | ipc, metrics | Anu Engineer | Anu Engineer |
| [HADOOP-13782](https://issues.apache.org/jira/browse/HADOOP-13782) | Make MutableRates metrics thread-local write, aggregate-on-read |  Major | metrics | Erik Krogen | Erik Krogen |
| [HDFS-10941](https://issues.apache.org/jira/browse/HDFS-10941) | Improve BlockManager#processMisReplicatesAsync log |  Major | namenode | Xiaoyu Yao | Chen Liang |
| [HADOOP-13742](https://issues.apache.org/jira/browse/HADOOP-13742) | Expose "NumOpenConnectionsPerUser" as a metric |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-10534](https://issues.apache.org/jira/browse/HDFS-10534) | NameNode WebUI should display DataNode usage histogram |  Major | namenode, ui | Zhe Zhang | Kai Sasaki |
| [HDFS-11333](https://issues.apache.org/jira/browse/HDFS-11333) | Print a user friendly error message when plugins are not found |  Minor | namenode | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-11466](https://issues.apache.org/jira/browse/HDFS-11466) | Change dfs.namenode.write-lock-reporting-threshold-ms default from 1000ms to 5000ms |  Major | namenode | Andrew Wang | Andrew Wang |
| [HADOOP-14169](https://issues.apache.org/jira/browse/HADOOP-14169) | Implement listStatusIterator, listLocatedStatus for ViewFs |  Minor | viewfs | Erik Krogen | Erik Krogen |
| [HDFS-11628](https://issues.apache.org/jira/browse/HDFS-11628) | Clarify the behavior of HDFS Mover in documentation |  Major | documentation | Xiaobing Zhou | Xiaobing Zhou |
| [HADOOP-14276](https://issues.apache.org/jira/browse/HADOOP-14276) | Add a nanosecond API to Time/Timer/FakeTimer |  Minor | util | Erik Krogen | Erik Krogen |
| [HDFS-11648](https://issues.apache.org/jira/browse/HDFS-11648) | Lazy construct the IIP pathname |  Major | . | Daryn Sharp | Daryn Sharp |
| [HDFS-11634](https://issues.apache.org/jira/browse/HDFS-11634) | Optimize BlockIterator when iterating starts in the middle. |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-11384](https://issues.apache.org/jira/browse/HDFS-11384) | Add option for balancer to disperse getBlocks calls to avoid NameNode's rpc.CallQueueLength spike |  Major | balancer & mover | yunjiong zhao | Konstantin Shvachko |
| [HDFS-8873](https://issues.apache.org/jira/browse/HDFS-8873) | Allow the directoryScanner to be rate-limited |  Major | datanode | Nathan Roberts | Daniel Templeton |
| [YARN-6457](https://issues.apache.org/jira/browse/YARN-6457) | Allow custom SSL configuration to be supplied in WebApps |  Major | webapp, yarn | Sanjay M Pujare | Sanjay M Pujare |
| [YARN-6493](https://issues.apache.org/jira/browse/YARN-6493) | Print requested node partition in assignContainer logs |  Major | . | Jonathan Hung | Jonathan Hung |
| [HADOOP-14440](https://issues.apache.org/jira/browse/HADOOP-14440) | Add metrics for connections dropped |  Major | . | Eric Badger | Eric Badger |
| [HDFS-11345](https://issues.apache.org/jira/browse/HDFS-11345) | Document the configuration key for FSNamesystem lock fairness |  Minor | documentation, namenode | Zhe Zhang | Erik Krogen |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-7851](https://issues.apache.org/jira/browse/HADOOP-7851) | Configuration.getClasses() never returns the default value. |  Major | conf | Amar Kamat | Uma Maheswara Rao G |
| [YARN-1471](https://issues.apache.org/jira/browse/YARN-1471) | The SLS simulator is not running the preemption policy for CapacityScheduler |  Minor | . | Carlo Curino | Carlo Curino |
| [HADOOP-11703](https://issues.apache.org/jira/browse/HADOOP-11703) | git should ignore .DS\_Store files on Mac OS X |  Major | . | Abin Shahab | Abin Shahab |
| [YARN-3269](https://issues.apache.org/jira/browse/YARN-3269) | Yarn.nodemanager.remote-app-log-dir could not be configured to fully qualified path |  Major | . | Xuan Gong | Xuan Gong |
| [HDFS-7990](https://issues.apache.org/jira/browse/HDFS-7990) | IBR delete ack should not be delayed |  Major | namenode | Daryn Sharp | Daryn Sharp |
| [HADOOP-11859](https://issues.apache.org/jira/browse/HADOOP-11859) | PseudoAuthenticationHandler fails with httpcomponents v4.4 |  Major | . | Eugene Koifman | Eugene Koifman |
| [HDFS-7847](https://issues.apache.org/jira/browse/HDFS-7847) | Modify NNThroughputBenchmark to be able to operate on a remote NameNode |  Major | . | Colin P. McCabe | Charles Lamb |
| [HDFS-6291](https://issues.apache.org/jira/browse/HDFS-6291) | FSImage may be left unclosed in BootstrapStandby#doRun() |  Minor | ha | Ted Yu | Sanghyun Yun |
| [YARN-3707](https://issues.apache.org/jira/browse/YARN-3707) | RM Web UI queue filter doesn't work |  Blocker | . | Wangda Tan | Wangda Tan |
| [HADOOP-12173](https://issues.apache.org/jira/browse/HADOOP-12173) | NetworkTopology#add calls NetworkTopology#toString always |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-8682](https://issues.apache.org/jira/browse/HDFS-8682) | Should not remove decommissioned node,while calculating the number of live/dead decommissioned node. |  Major | . | J.Andreina | J.Andreina |
| [HDFS-5802](https://issues.apache.org/jira/browse/HDFS-5802) | NameNode does not check for inode type before traversing down a path |  Trivial | namenode | Harsh J | Xiao Chen |
| [YARN-4017](https://issues.apache.org/jira/browse/YARN-4017) | container-executor overuses PATH\_MAX |  Major | nodemanager | Allen Wittenauer | Sidharta Seethana |
| [YARN-4250](https://issues.apache.org/jira/browse/YARN-4250) | NPE in AppSchedulingInfo#isRequestLabelChanged |  Major | resourcemanager, scheduler | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-12483](https://issues.apache.org/jira/browse/HADOOP-12483) | Maintain wrapped SASL ordering for postponed IPC responses |  Critical | ipc | Daryn Sharp | Daryn Sharp |
| [HADOOP-12418](https://issues.apache.org/jira/browse/HADOOP-12418) | TestRPC.testRPCInterruptedSimple fails intermittently |  Major | test | Steve Loughran | Kihwal Lee |
| [YARN-4302](https://issues.apache.org/jira/browse/YARN-4302) | SLS not able start due to NPE in SchedulerApplicationAttempt#getResourceUsageReport |  Major | . | Bibin A Chundatt | Bibin A Chundatt |
| [YARN-4367](https://issues.apache.org/jira/browse/YARN-4367) | SLS webapp doesn't load |  Major | scheduler-load-simulator | Karthik Kambatla | Karthik Kambatla |
| [HADOOP-11149](https://issues.apache.org/jira/browse/HADOOP-11149) | Increase the timeout of TestZKFailoverController |  Major | test | Rajat Jain | Steve Loughran |
| [HDFS-9467](https://issues.apache.org/jira/browse/HDFS-9467) | Fix data race accessing writeLockHeldTimeStamp in FSNamesystem |  Major | namenode | Mingliang Liu | Mingliang Liu |
| [YARN-4109](https://issues.apache.org/jira/browse/YARN-4109) | Exception on RM scheduler page loading with labels |  Minor | . | Bibin A Chundatt | Mohammad Shahid Khan |
| [YARN-4612](https://issues.apache.org/jira/browse/YARN-4612) | Fix rumen and scheduler load simulator handle killed tasks properly |  Major | . | Ming Ma | Ming Ma |
| [YARN-4927](https://issues.apache.org/jira/browse/YARN-4927) | TestRMHA#testTransitionedToActiveRefreshFail fails with FairScheduler |  Major | test | Karthik Kambatla | Bibin A Chundatt |
| [YARN-4562](https://issues.apache.org/jira/browse/YARN-4562) | YARN WebApp ignores the configuration passed to it for keystore settings |  Major | . | Sergey Shelukhin | Sergey Shelukhin |
| [HDFS-10270](https://issues.apache.org/jira/browse/HDFS-10270) | TestJMXGet:testNameNode() fails |  Minor | test | Andras Bokor | Gergely Novák |
| [HADOOP-13026](https://issues.apache.org/jira/browse/HADOOP-13026) | Should not wrap IOExceptions into a AuthenticationException in KerberosAuthenticator |  Critical | . | Xuan Gong | Xuan Gong |
| [HDFS-10276](https://issues.apache.org/jira/browse/HDFS-10276) | HDFS should not expose path info that user has no permission to see. |  Major | fs, security | Kevin Cox | Yuanbo Liu |
| [YARN-5197](https://issues.apache.org/jira/browse/YARN-5197) | RM leaks containers if running container disappears from node update |  Critical | resourcemanager | Jason Lowe | Jason Lowe |
| [YARN-5262](https://issues.apache.org/jira/browse/YARN-5262) | Optimize sending RMNodeFinishedContainersPulledByAMEvent for every AM heartbeat |  Major | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-10396](https://issues.apache.org/jira/browse/HDFS-10396) | Using -diff option with DistCp may get "Comparison method violates its general contract" exception |  Major | . | Yongjun Zhang | Yongjun Zhang |
| [HDFS-10336](https://issues.apache.org/jira/browse/HDFS-10336) | TestBalancer failing intermittently because of not reseting UserGroupInformation completely |  Major | test | Yiqun Lin | Yiqun Lin |
| [HDFS-10512](https://issues.apache.org/jira/browse/HDFS-10512) | VolumeScanner may terminate due to NPE in DataNode.reportBadBlocks |  Major | datanode | Wei-Chiu Chuang | Yiqun Lin |
| [YARN-5353](https://issues.apache.org/jira/browse/YARN-5353) | ResourceManager can leak delegation tokens when they are shared across apps |  Critical | resourcemanager | Jason Lowe | Jason Lowe |
| [HADOOP-11361](https://issues.apache.org/jira/browse/HADOOP-11361) | Fix a race condition in MetricsSourceAdapter.updateJmxCache |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-10544](https://issues.apache.org/jira/browse/HDFS-10544) | Balancer doesn't work with IPFailoverProxyProvider |  Major | balancer & mover, ha | Zhe Zhang | Zhe Zhang |
| [HADOOP-13202](https://issues.apache.org/jira/browse/HADOOP-13202) | Avoid possible overflow in org.apache.hadoop.util.bloom.BloomFilter#getNBytes |  Major | util | zhengbing li | Kai Sasaki |
| [HADOOP-12991](https://issues.apache.org/jira/browse/HADOOP-12991) | Conflicting default ports in DelegateToFileSystem |  Major | fs | Kevin Hogeland | Kai Sasaki |
| [MAPREDUCE-6744](https://issues.apache.org/jira/browse/MAPREDUCE-6744) | Increase timeout on TestDFSIO tests |  Major | . | Eric Badger | Eric Badger |
| [HDFS-10691](https://issues.apache.org/jira/browse/HDFS-10691) | FileDistribution fails in hdfs oiv command due to ArrayIndexOutOfBoundsException |  Major | . | Yiqun Lin | Yiqun Lin |
| [YARN-5121](https://issues.apache.org/jira/browse/YARN-5121) | fix some container-executor portability issues |  Blocker | nodemanager, security | Allen Wittenauer | Allen Wittenauer |
| [MAPREDUCE-6724](https://issues.apache.org/jira/browse/MAPREDUCE-6724) | Single shuffle to memory must not exceed Integer#MAX\_VALUE |  Major | mrv2 | Haibo Chen | Haibo Chen |
| [HDFS-8780](https://issues.apache.org/jira/browse/HDFS-8780) | Fetching live/dead datanode list with arg true for removeDecommissionNode,returns list with decom node. |  Major | . | J.Andreina | J.Andreina |
| [YARN-5462](https://issues.apache.org/jira/browse/YARN-5462) | TestNodeStatusUpdater.testNodeStatusUpdaterRetryAndNMShutdown fails intermittently |  Major | . | Eric Badger | Eric Badger |
| [YARN-5469](https://issues.apache.org/jira/browse/YARN-5469) | Increase timeout of TestAmFilter.testFilter |  Minor | . | Eric Badger | Eric Badger |
| [HDFS-10716](https://issues.apache.org/jira/browse/HDFS-10716) | In Balancer, the target task should be removed when its size \< 0. |  Minor | balancer & mover | Yiqun Lin | Yiqun Lin |
| [HDFS-10715](https://issues.apache.org/jira/browse/HDFS-10715) | NPE when applying AvailableSpaceBlockPlacementPolicy |  Major | namenode | Guangbin Zhu | Guangbin Zhu |
| [YARN-5333](https://issues.apache.org/jira/browse/YARN-5333) | Some recovered apps are put into default queue when RM HA |  Major | . | Jun Gong | Jun Gong |
| [HDFS-10693](https://issues.apache.org/jira/browse/HDFS-10693) | metaSave should print blocks, not LightWeightHashSet |  Major | namenode | Konstantin Shvachko | Yuanbo Liu |
| [HDFS-10694](https://issues.apache.org/jira/browse/HDFS-10694) | BlockManager.processReport() should print blockReportId in each log message. |  Major | logging, namenode | Konstantin Shvachko | Yuanbo Liu |
| [HDFS-8224](https://issues.apache.org/jira/browse/HDFS-8224) | Schedule a block for scanning if its metadata file is corrupt |  Major | datanode | Rushabh S Shah | Rushabh S Shah |
| [YARN-5382](https://issues.apache.org/jira/browse/YARN-5382) | RM does not audit log kill request for active applications |  Major | resourcemanager | Jason Lowe | Vrushali C |
| [HDFS-9696](https://issues.apache.org/jira/browse/HDFS-9696) | Garbage snapshot records lingering forever |  Critical | . | Kihwal Lee | Kihwal Lee |
| [HDFS-10747](https://issues.apache.org/jira/browse/HDFS-10747) | o.a.h.hdfs.tools.DebugAdmin usage message is misleading |  Minor | hdfs-client | Mingliang Liu | Mingliang Liu |
| [HADOOP-13494](https://issues.apache.org/jira/browse/HADOOP-13494) | ReconfigurableBase can log sensitive information |  Major | security | Sean Mackrory | Sean Mackrory |
| [HADOOP-13512](https://issues.apache.org/jira/browse/HADOOP-13512) | ReloadingX509TrustManager should keep reloading in case of exception |  Critical | security | Mingliang Liu | Mingliang Liu |
| [HDFS-10763](https://issues.apache.org/jira/browse/HDFS-10763) | Open files can leak permanently due to inconsistent lease update |  Critical | . | Kihwal Lee | Kihwal Lee |
| [MAPREDUCE-6763](https://issues.apache.org/jira/browse/MAPREDUCE-6763) | Shuffle server listen queue is too small |  Major | mrv2 | Jason Lowe | Jason Lowe |
| [HDFS-8312](https://issues.apache.org/jira/browse/HDFS-8312) | Trash does not descent into child directories to check for permissions |  Critical | fs, security | Eric Yang | Weiwei Yang |
| [HDFS-8915](https://issues.apache.org/jira/browse/HDFS-8915) | TestFSNamesystem.testFSLockGetWaiterCount fails intermittently in jenkins |  Minor | test | Anu Engineer | Masatake Iwasaki |
| [HADOOP-12765](https://issues.apache.org/jira/browse/HADOOP-12765) | HttpServer2 should switch to using the non-blocking SslSelectChannelConnector to prevent performance degradation when handling SSL connections |  Major | . | Min Shen | Min Shen |
| [MAPREDUCE-6768](https://issues.apache.org/jira/browse/MAPREDUCE-6768) | TestRecovery.testSpeculative failed with NPE |  Major | mrv2 | Haibo Chen | Haibo Chen |
| [MAPREDUCE-4784](https://issues.apache.org/jira/browse/MAPREDUCE-4784) | TestRecovery occasionally fails |  Major | mrv2, test | Jason Lowe | Haibo Chen |
| [HDFS-10809](https://issues.apache.org/jira/browse/HDFS-10809) | getNumEncryptionZones causes NPE in branch-2.7 |  Major | encryption, namenode | Zhe Zhang | Vinitha Reddy Gankidi |
| [HADOOP-13558](https://issues.apache.org/jira/browse/HADOOP-13558) | UserGroupInformation created from a Subject incorrectly tries to renew the Kerberos ticket |  Major | security | Alejandro Abdelnur | Xiao Chen |
| [HDFS-9038](https://issues.apache.org/jira/browse/HDFS-9038) | DFS reserved space is erroneously counted towards non-DFS used. |  Major | datanode | Chris Nauroth | Brahma Reddy Battula |
| [HADOOP-13579](https://issues.apache.org/jira/browse/HADOOP-13579) | Fix source-level compatibility after HADOOP-11252 |  Blocker | . | Akira Ajisaka | Tsuyoshi Ozawa |
| [HADOOP-13601](https://issues.apache.org/jira/browse/HADOOP-13601) | Fix typo in a log messages of AbstractDelegationTokenSecretManager |  Trivial | . | Mehran Hassani | Mehran Hassani |
| [HDFS-10879](https://issues.apache.org/jira/browse/HDFS-10879) | TestEncryptionZonesWithKMS#testReadWrite fails intermittently |  Major | . | Xiao Chen | Xiao Chen |
| [HDFS-10843](https://issues.apache.org/jira/browse/HDFS-10843) | Update space quota when a UC block is completed rather than committed. |  Major | hdfs, namenode | Erik Krogen | Erik Krogen |
| [HADOOP-13535](https://issues.apache.org/jira/browse/HADOOP-13535) | Add jetty6 acceptor startup issue workaround to branch-2 |  Major | . | Wei-Chiu Chuang | Min Shen |
| [HADOOP-12597](https://issues.apache.org/jira/browse/HADOOP-12597) | In kms-site.xml configuration "hadoop.security.keystore.JavaKeyStoreProvider.password" should be updated with new name |  Minor | security | huangyitian | Surendra Singh Lilhore |
| [HDFS-9885](https://issues.apache.org/jira/browse/HDFS-9885) | Correct the distcp counters name while displaying counters |  Minor | distcp | Archana T | Surendra Singh Lilhore |
| [HDFS-10889](https://issues.apache.org/jira/browse/HDFS-10889) | Remove outdated Fault Injection Framework documentaion |  Major | documentation | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-10713](https://issues.apache.org/jira/browse/HDFS-10713) | Throttle FsNameSystem lock warnings |  Major | logging, namenode | Arpit Agarwal | Hanisha Koneru |
| [HDFS-10915](https://issues.apache.org/jira/browse/HDFS-10915) | Fix time measurement bug in TestDatanodeRestart#testWaitForRegistrationOnRestart |  Minor | test | Xiaobing Zhou | Xiaobing Zhou |
| [HDFS-9444](https://issues.apache.org/jira/browse/HDFS-9444) | Add utility to find set of available ephemeral ports to ServerSocketUtil |  Major | . | Brahma Reddy Battula | Masatake Iwasaki |
| [HADOOP-11780](https://issues.apache.org/jira/browse/HADOOP-11780) | Prevent IPC reader thread death |  Critical | ipc | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-6771](https://issues.apache.org/jira/browse/MAPREDUCE-6771) | RMContainerAllocator sends container diagnostics event after corresponding completion event |  Major | mrv2 | Haibo Chen | Haibo Chen |
| [HDFS-10878](https://issues.apache.org/jira/browse/HDFS-10878) | TestDFSClientRetries#testIdempotentAllocateBlockAndClose throws ConcurrentModificationException |  Major | hdfs-client | Rushabh S Shah | Rushabh S Shah |
| [HDFS-10609](https://issues.apache.org/jira/browse/HDFS-10609) | Uncaught InvalidEncryptionKeyException during pipeline recovery may abort downstream applications |  Major | encryption | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-10991](https://issues.apache.org/jira/browse/HDFS-10991) | Export hdfsTruncateFile symbol in libhdfs |  Blocker | libhdfs | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-11002](https://issues.apache.org/jira/browse/HDFS-11002) | Fix broken attr/getfattr/setfattr links in ExtendedAttributes.md |  Major | documentation | Mingliang Liu | Mingliang Liu |
| [HDFS-10987](https://issues.apache.org/jira/browse/HDFS-10987) | Make Decommission less expensive when lot of blocks present. |  Critical | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-10301](https://issues.apache.org/jira/browse/HDFS-10301) | BlockReport retransmissions may lead to storages falsely being declared zombie if storage report processing happens out of order |  Critical | namenode | Konstantin Shvachko | Vinitha Reddy Gankidi |
| [HDFS-10712](https://issues.apache.org/jira/browse/HDFS-10712) | Fix TestDataNodeVolumeFailure on 2.\* branches. |  Major | . | Konstantin Shvachko | Vinitha Reddy Gankidi |
| [HDFS-10627](https://issues.apache.org/jira/browse/HDFS-10627) | Volume Scanner marks a block as "suspect" even if the exception is network-related |  Major | hdfs | Rushabh S Shah | Rushabh S Shah |
| [HADOOP-13236](https://issues.apache.org/jira/browse/HADOOP-13236) | truncate will fail when we use viewfilesystem |  Major | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-11015](https://issues.apache.org/jira/browse/HDFS-11015) | Enforce timeout in balancer |  Major | balancer & mover | Kihwal Lee | Kihwal Lee |
| [HDFS-11053](https://issues.apache.org/jira/browse/HDFS-11053) | Unnecessary superuser check in versionRequest() |  Major | namenode, security | Kihwal Lee | Kihwal Lee |
| [HDFS-10921](https://issues.apache.org/jira/browse/HDFS-10921) | TestDiskspaceQuotaUpdate doesn't wait for NN to get out of safe mode |  Major | . | Eric Badger | Eric Badger |
| [HADOOP-13201](https://issues.apache.org/jira/browse/HADOOP-13201) | Print the directory paths when ViewFs denies the rename operation on internal dirs |  Major | viewfs | Tianyin Xu | Rakesh R |
| [YARN-4328](https://issues.apache.org/jira/browse/YARN-4328) | Findbugs warning in resourcemanager in branch-2.7 and branch-2.6 |  Minor | resourcemanager | Varun Saxena | Akira Ajisaka |
| [YARN-3432](https://issues.apache.org/jira/browse/YARN-3432) | Cluster metrics have wrong Total Memory when there is reserved memory on CS |  Major | capacityscheduler, resourcemanager | Thomas Graves | Brahma Reddy Battula |
| [HDFS-9500](https://issues.apache.org/jira/browse/HDFS-9500) | datanodesSoftwareVersions map may counting wrong when rolling upgrade |  Major | . | Phil Yang | Erik Krogen |
| [HDFS-10455](https://issues.apache.org/jira/browse/HDFS-10455) | Logging the username when deny the setOwner operation |  Minor | namenode | Tianyin Xu | Rakesh R |
| [YARN-5001](https://issues.apache.org/jira/browse/YARN-5001) | Aggregated Logs root directory is created with wrong group if nonexistent |  Major | log-aggregation, nodemanager, security | Haibo Chen | Haibo Chen |
| [YARN-5837](https://issues.apache.org/jira/browse/YARN-5837) | NPE when getting node status of a decommissioned node after an RM restart |  Major | . | Robert Kanter | Robert Kanter |
| [HADOOP-13804](https://issues.apache.org/jira/browse/HADOOP-13804) | MutableStat mean loses accuracy if add(long, long) is used |  Minor | metrics | Erik Krogen | Erik Krogen |
| [HDFS-8307](https://issues.apache.org/jira/browse/HDFS-8307) | Spurious DNS Queries from hdfs shell |  Trivial | hdfs-client | Anu Engineer | Andres Perez |
| [HDFS-11087](https://issues.apache.org/jira/browse/HDFS-11087) | NamenodeFsck should check if the output writer is still writable. |  Major | namenode | Konstantin Shvachko | Erik Krogen |
| [HDFS-11056](https://issues.apache.org/jira/browse/HDFS-11056) | Concurrent append and read operations lead to checksum error |  Major | datanode, httpfs | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-4355](https://issues.apache.org/jira/browse/YARN-4355) | NPE while processing localizer heartbeat |  Major | nodemanager | Jason Lowe | Varun Saxena |
| [HDFS-10966](https://issues.apache.org/jira/browse/HDFS-10966) | Enhance Dispatcher logic on deciding when to give up a source DataNode |  Major | balancer & mover | Zhe Zhang | Mark Wagner |
| [HDFS-11174](https://issues.apache.org/jira/browse/HDFS-11174) | Wrong HttpFS test command in doc |  Minor | documentation, httpfs | John Zhuge | John Zhuge |
| [YARN-5694](https://issues.apache.org/jira/browse/YARN-5694) | ZKRMStateStore can prevent the transition to standby in branch-2.7 if the ZK node is unreachable |  Critical | resourcemanager | Daniel Templeton | Daniel Templeton |
| [HDFS-11180](https://issues.apache.org/jira/browse/HDFS-11180) | Intermittent deadlock in NameNode when failover happens. |  Blocker | namenode | Abhishek Modi | Akira Ajisaka |
| [HADOOP-13867](https://issues.apache.org/jira/browse/HADOOP-13867) | FilterFileSystem should override rename(.., options) to take effect of Rename options called via FilterFileSystem implementations |  Major | . | Vinayakumar B | Vinayakumar B |
| [HDFS-11229](https://issues.apache.org/jira/browse/HDFS-11229) | HDFS-11056 failed to close meta file |  Blocker | datanode | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-11160](https://issues.apache.org/jira/browse/HDFS-11160) | VolumeScanner reports write-in-progress replicas as corrupt incorrectly |  Major | datanode | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-11263](https://issues.apache.org/jira/browse/HDFS-11263) | ClassCastException when we use Bzipcodec for Fsimage compression |  Critical | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-6024](https://issues.apache.org/jira/browse/YARN-6024) | Capacity Scheduler 'continuous reservation looking' doesn't work when sum of queue's used and reserved resources is equal to max |  Major | . | Wangda Tan | Wangda Tan |
| [YARN-5988](https://issues.apache.org/jira/browse/YARN-5988) | RM unable to start in secure setup |  Blocker | . | Ajith S | Ajith S |
| [HADOOP-13839](https://issues.apache.org/jira/browse/HADOOP-13839) | Fix outdated tracing documentation |  Minor | documentation, tracing | Masatake Iwasaki | Elek, Marton |
| [HDFS-11280](https://issues.apache.org/jira/browse/HDFS-11280) | Allow WebHDFS to reuse HTTP connections to NN |  Major | hdfs | Zheng Shao | Zheng Shao |
| [MAPREDUCE-6711](https://issues.apache.org/jira/browse/MAPREDUCE-6711) | JobImpl fails to handle preemption events on state COMMITTING |  Major | . | Li Lu | Prabhu Joseph |
| [HADOOP-13958](https://issues.apache.org/jira/browse/HADOOP-13958) | Bump up release year to 2017 |  Blocker | . | Junping Du | Junping Du |
| [HDFS-10733](https://issues.apache.org/jira/browse/HDFS-10733) | NameNode terminated after full GC thinking QJM is unresponsive. |  Major | namenode, qjm | Konstantin Shvachko | Vinitha Reddy Gankidi |
| [HADOOP-14001](https://issues.apache.org/jira/browse/HADOOP-14001) | Improve delegation token validity checking |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [HDFS-11352](https://issues.apache.org/jira/browse/HDFS-11352) | Potential deadlock in NN when failing over |  Critical | namenode | Erik Krogen | Erik Krogen |
| [HADOOP-14044](https://issues.apache.org/jira/browse/HADOOP-14044) | Synchronization issue in delegation token cancel functionality |  Major | . | Hrishikesh Gadre | Hrishikesh Gadre |
| [HDFS-11377](https://issues.apache.org/jira/browse/HDFS-11377) | Balancer hung due to no available mover threads |  Major | balancer & mover | yunjiong zhao | yunjiong zhao |
| [YARN-6152](https://issues.apache.org/jira/browse/YARN-6152) | Used queue percentage not accurate in UI for 2.7 and below when using DominantResourceCalculator |  Major | . | Jonathan Hung | Jonathan Hung |
| [HADOOP-13433](https://issues.apache.org/jira/browse/HADOOP-13433) | Race in UGI.reloginFromKeytab |  Major | security | Duo Zhang | Duo Zhang |
| [HDFS-11379](https://issues.apache.org/jira/browse/HDFS-11379) | DFSInputStream may infinite loop requesting block locations |  Critical | hdfs-client | Daryn Sharp | Daryn Sharp |
| [YARN-1728](https://issues.apache.org/jira/browse/YARN-1728) | Workaround guice3x-undecoded pathInfo in YARN WebApp |  Major | . | Abraham Elmahrek | Yuanbo Liu |
| [YARN-6310](https://issues.apache.org/jira/browse/YARN-6310) | OutputStreams in AggregatedLogFormat.LogWriter can be left open upon exceptions |  Major | yarn | Haibo Chen | Haibo Chen |
| [HDFS-11499](https://issues.apache.org/jira/browse/HDFS-11499) | Decommissioning stuck because of failing recovery |  Major | hdfs, namenode | Lukas Majercak | Lukas Majercak |
| [HADOOP-9631](https://issues.apache.org/jira/browse/HADOOP-9631) | ViewFs should use underlying FileSystem's server side defaults |  Major | fs, viewfs | Lohit Vijayarenu | Erik Krogen |
| [HADOOP-14214](https://issues.apache.org/jira/browse/HADOOP-14214) | DomainSocketWatcher::add()/delete() should not self interrupt while looping await() |  Critical | hdfs-client | Mingliang Liu | Mingliang Liu |
| [HADOOP-14195](https://issues.apache.org/jira/browse/HADOOP-14195) | CredentialProviderFactory$getProviders is not thread-safe |  Major | security | Vihang Karajgaonkar | Vihang Karajgaonkar |
| [HADOOP-14211](https://issues.apache.org/jira/browse/HADOOP-14211) | FilterFs and ChRootedFs are too aggressive about enforcing "authorityNeeded" |  Major | viewfs | Erik Krogen | Erik Krogen |
| [HDFS-11486](https://issues.apache.org/jira/browse/HDFS-11486) | Client close() should not fail fast if the last block is being decommissioned |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HADOOP-13362](https://issues.apache.org/jira/browse/HADOOP-13362) | DefaultMetricsSystem leaks the source name when a source unregisters |  Blocker | metrics | Jason Lowe | Junping Du |
| [MAPREDUCE-6873](https://issues.apache.org/jira/browse/MAPREDUCE-6873) | MR Job Submission Fails if MR framework application path not on defaultFS |  Minor | mrv2 | Erik Krogen | Erik Krogen |
| [HDFS-11608](https://issues.apache.org/jira/browse/HDFS-11608) | HDFS write crashed with block size greater than 2 GB |  Critical | hdfs-client | Xiaobing Zhou | Xiaobing Zhou |
| [HADOOP-14066](https://issues.apache.org/jira/browse/HADOOP-14066) | VersionInfo should be marked as public API |  Critical | common | Thejas M Nair | Akira Ajisaka |
| [HADOOP-14293](https://issues.apache.org/jira/browse/HADOOP-14293) | Initialize FakeTimer with a less trivial value |  Major | test | Andrew Wang | Andrew Wang |
| [YARN-6304](https://issues.apache.org/jira/browse/YARN-6304) | Skip rm.transitionToActive call to RM if RM is already active. |  Major | resourcemanager | Rohith Sharma K S | Rohith Sharma K S |
| [HDFS-11615](https://issues.apache.org/jira/browse/HDFS-11615) | FSNamesystemLock metrics can be inaccurate due to millisecond precision |  Major | hdfs | Erik Krogen | Erik Krogen |
| [HDFS-11709](https://issues.apache.org/jira/browse/HDFS-11709) | StandbyCheckpointer should handle an non-existing legacyOivImageDir gracefully |  Critical | ha, namenode | Zhe Zhang | Erik Krogen |
| [HDFS-11609](https://issues.apache.org/jira/browse/HDFS-11609) | Some blocks can be permanently lost if nodes are decommissioned while dead |  Blocker | namenode | Kihwal Lee | Kihwal Lee |
| [HDFS-8498](https://issues.apache.org/jira/browse/HDFS-8498) | Blocks can be committed with wrong size |  Critical | hdfs-client | Daryn Sharp | Jing Zhao |
| [HDFS-11714](https://issues.apache.org/jira/browse/HDFS-11714) | Newly added NN storage directory won't get initialized and cause space exhaustion |  Critical | . | Kihwal Lee | Kihwal Lee |
| [HADOOP-14371](https://issues.apache.org/jira/browse/HADOOP-14371) | License error in TestLoadBalancingKMSClientProvider.java |  Major | . | hu xiaodong | hu xiaodong |
| [HADOOP-14374](https://issues.apache.org/jira/browse/HADOOP-14374) | License error in GridmixTestUtils.java |  Major | . | lixinglong | lixinglong |
| [HDFS-11766](https://issues.apache.org/jira/browse/HDFS-11766) | Fix findbugs warning in branch-2.7 |  Major | . | Akira Ajisaka | Chen Liang |
| [HADOOP-14100](https://issues.apache.org/jira/browse/HADOOP-14100) | Upgrade Jsch jar to latest version to fix vulnerability in old versions |  Critical | . | Vinayakumar B | Vinayakumar B |
| [HDFS-11373](https://issues.apache.org/jira/browse/HDFS-11373) | Backport HDFS-11258 and HDFS-11272 to branch-2.7 |  Critical | . | Akira Ajisaka | Akira Ajisaka |
| [HDFS-11795](https://issues.apache.org/jira/browse/HDFS-11795) | Fix ASF Licence warnings in branch-2.7 |  Major | . | Yiqun Lin | Yiqun Lin |
| [YARN-5543](https://issues.apache.org/jira/browse/YARN-5543) | ResourceManager SchedulingMonitor could potentially terminate the preemption checker thread |  Major | capacityscheduler, resourcemanager | Min Shen | Min Shen |
| [HDFS-11674](https://issues.apache.org/jira/browse/HDFS-11674) | reserveSpaceForReplicas is not released if append request failed due to mirror down and replica recovered |  Critical | datanode | Vinayakumar B | Vinayakumar B |
| [HADOOP-14376](https://issues.apache.org/jira/browse/HADOOP-14376) | Memory leak when reading a compressed file using the native library |  Major | common, io | Eli Acherkan | Eli Acherkan |
| [HADOOP-14434](https://issues.apache.org/jira/browse/HADOOP-14434) | Use MoveFileEx to allow renaming a file when the destination exists |  Major | native | Lukas Majercak | Lukas Majercak |
| [HDFS-11849](https://issues.apache.org/jira/browse/HDFS-11849) | JournalNode startup failure exception should be logged in log file |  Major | journal-node | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-11864](https://issues.apache.org/jira/browse/HDFS-11864) | Document  Metrics to track usage of memory for writes |  Major | documentation | Brahma Reddy Battula | Yiqun Lin |
| [YARN-6615](https://issues.apache.org/jira/browse/YARN-6615) | AmIpFilter drops query parameters on redirect |  Major | . | Wilfred Spiegelenburg | Wilfred Spiegelenburg |
| [HDFS-11445](https://issues.apache.org/jira/browse/HDFS-11445) | FSCK shows overall health status as corrupt even one replica is corrupt |  Critical | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-11078](https://issues.apache.org/jira/browse/HDFS-11078) | Fix NPE in LazyPersistFileScrubber |  Major | . | Íñigo Goiri | Íñigo Goiri |
| [HDFS-5042](https://issues.apache.org/jira/browse/HDFS-5042) | Completed files lost after power failure |  Critical | . | Dave Latham | Vinayakumar B |
| [HDFS-11893](https://issues.apache.org/jira/browse/HDFS-11893) | Fix TestDFSShell.testMoveWithTargetPortEmpty failure. |  Major | test | Konstantin Shvachko | Brahma Reddy Battula |
| [HDFS-11741](https://issues.apache.org/jira/browse/HDFS-11741) | Long running balancer may fail due to expired DataEncryptionKey |  Major | balancer & mover | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [HDFS-11856](https://issues.apache.org/jira/browse/HDFS-11856) | Ability to re-add Upgrading Nodes (remote) to pipeline for future pipeline updates |  Major | hdfs-client, rolling upgrades | Vinayakumar B | Vinayakumar B |
| [HADOOP-14474](https://issues.apache.org/jira/browse/HADOOP-14474) | Use OpenJDK 7 instead of Oracle JDK 7 to avoid oracle-java7-installer failures |  Major | build | Akira Ajisaka | Akira Ajisaka |
| [YARN-4925](https://issues.apache.org/jira/browse/YARN-4925) | ContainerRequest in AMRMClient, application should be able to specify nodes/racks together with nodeLabelExpression |  Major | . | Bibin A Chundatt | Bibin A Chundatt |
| [HDFS-11708](https://issues.apache.org/jira/browse/HDFS-11708) | Positional read will fail if replicas moved to different DNs after stream is opened |  Critical | hdfs-client | Vinayakumar B | Vinayakumar B |
| [HDFS-11743](https://issues.apache.org/jira/browse/HDFS-11743) | Revert the incompatible fsck reporting output in HDFS-7933 from branch-2.7 |  Blocker | namenode | Zhe Zhang | Zhe Zhang |
| [HDFS-11583](https://issues.apache.org/jira/browse/HDFS-11583) | Parent spans are not initialized to NullScope for every DFSPacket |  Major | tracing | Karan Mehta | Masatake Iwasaki |
| [YARN-6719](https://issues.apache.org/jira/browse/YARN-6719) | Fix findbugs warnings in SLSCapacityScheduler.java |  Major | . | Akira Ajisaka | Akira Ajisaka |
| [HDFS-11736](https://issues.apache.org/jira/browse/HDFS-11736) | OIV tests should not write outside 'target' directory. |  Major | . | Konstantin Shvachko | Yiqun Lin |
| [MAPREDUCE-6433](https://issues.apache.org/jira/browse/MAPREDUCE-6433) | launchTime may be negative |  Major | jobhistoryserver, mrv2 | Allen Wittenauer | zhihai xu |
| [HADOOP-10829](https://issues.apache.org/jira/browse/HADOOP-10829) | Iteration on CredentialProviderFactory.serviceLoader  is thread-unsafe |  Major | security | Benoy Antony | Benoy Antony |
| [HADOOP-14586](https://issues.apache.org/jira/browse/HADOOP-14586) | StringIndexOutOfBoundsException breaks org.apache.hadoop.util.Shell on 2.7.x with Java 9 |  Minor | common | Uwe Schindler | Akira Ajisaka |
| [MAPREDUCE-6697](https://issues.apache.org/jira/browse/MAPREDUCE-6697) | Concurrent task limits should only be applied when necessary |  Major | mrv2 | Jason Lowe | Nathan Roberts |
| [YARN-3260](https://issues.apache.org/jira/browse/YARN-3260) | AM attempt fail to register before RM processes launch event |  Critical | resourcemanager | Jason Lowe | Bibin A Chundatt |
| [YARN-6818](https://issues.apache.org/jira/browse/YARN-6818) | User limit per partition is not honored in branch-2.7 \>= |  Major | . | Jonathan Hung | Jonathan Hung |
| [HADOOP-14356](https://issues.apache.org/jira/browse/HADOOP-14356) | Update CHANGES.txt to reflect all the changes in branch-2.7 |  Blocker | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [HDFS-11472](https://issues.apache.org/jira/browse/HDFS-11472) | Fix inconsistent replica size after a data pipeline failure |  Critical | datanode | Wei-Chiu Chuang | Erik Krogen |
| [HDFS-12177](https://issues.apache.org/jira/browse/HDFS-12177) | NameNode exits due to  setting BlockPlacementPolicy loglevel to Debug |  Major | block placement | Jiandan Yang | Jiandan Yang |
| [HDFS-11742](https://issues.apache.org/jira/browse/HDFS-11742) | Improve balancer usability after HDFS-8818 |  Blocker | . | Kihwal Lee | Kihwal Lee |
| [HADOOP-14686](https://issues.apache.org/jira/browse/HADOOP-14686) | Branch-2.7 .gitignore is out of date |  Blocker | build, precommit | Sean Busbey | Sean Busbey |
| [HDFS-11896](https://issues.apache.org/jira/browse/HDFS-11896) | Non-dfsUsed will be doubled on dead node re-registration |  Blocker | . | Brahma Reddy Battula | Brahma Reddy Battula |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-9888](https://issues.apache.org/jira/browse/HDFS-9888) | Allow reseting KerberosName in unit tests |  Minor | . | Xiao Chen | Xiao Chen |
| [YARN-4717](https://issues.apache.org/jira/browse/YARN-4717) | TestResourceLocalizationService.testPublicResourceInitializesLocalDir fails Intermittently due to IllegalArgumentException from cleanup |  Minor | nodemanager | Daniel Templeton | Daniel Templeton |
| [YARN-5092](https://issues.apache.org/jira/browse/YARN-5092) | TestRMDelegationTokens fails intermittently |  Major | test | Rohith Sharma K S | Jason Lowe |
| [HADOOP-10980](https://issues.apache.org/jira/browse/HADOOP-10980) | TestActiveStandbyElector fails occasionally in trunk |  Minor | . | Ted Yu | Eric Badger |
| [HDFS-9745](https://issues.apache.org/jira/browse/HDFS-9745) | TestSecureNNWithQJM#testSecureMode sometimes fails with timeouts |  Minor | . | Xiao Chen | Xiao Chen |
| [HDFS-9333](https://issues.apache.org/jira/browse/HDFS-9333) | Some tests using MiniDFSCluster errored complaining port in use |  Minor | test | Kai Zheng | Masatake Iwasaki |
| [HDFS-11290](https://issues.apache.org/jira/browse/HDFS-11290) | TestFSNameSystemMBean should wait until JMX cache is cleared |  Major | test | Akira Ajisaka | Erik Krogen |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-8721](https://issues.apache.org/jira/browse/HDFS-8721) | Add a metric for number of encryption zones |  Major | encryption | Rakesh R | Rakesh R |
| [HDFS-8824](https://issues.apache.org/jira/browse/HDFS-8824) | Do not use small blocks for balancing the cluster |  Major | balancer & mover | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-9421](https://issues.apache.org/jira/browse/HDFS-9421) | NNThroughputBenchmark replication test NPE with -namenode option |  Major | benchmarks | Xiaoyu Yao | Mingliang Liu |
| [YARN-4393](https://issues.apache.org/jira/browse/YARN-4393) | TestResourceLocalizationService#testFailedDirsResourceRelease fails intermittently |  Major | test | Varun Saxena | Varun Saxena |
| [HDFS-9621](https://issues.apache.org/jira/browse/HDFS-9621) | getListing wrongly associates Erasure Coding policy to pre-existing replicated files under an EC directory |  Critical | erasure-coding | Sushmitha Sreenivasan | Jing Zhao |
| [HDFS-9601](https://issues.apache.org/jira/browse/HDFS-9601) | NNThroughputBenchmark.BlockReportStats should handle NotReplicatedYetException on adding block |  Major | test | Masatake Iwasaki | Masatake Iwasaki |
| [YARN-4573](https://issues.apache.org/jira/browse/YARN-4573) | TestRMAppTransitions.testAppRunningKill and testAppKilledKilled fail on trunk |  Major | resourcemanager, test | Takashi Ohnishi | Takashi Ohnishi |
| [HDFS-9503](https://issues.apache.org/jira/browse/HDFS-9503) | Replace -namenode option with -fs for NNThroughputBenchmark |  Major | test | Konstantin Shvachko | Mingliang Liu |
| [HADOOP-12975](https://issues.apache.org/jira/browse/HADOOP-12975) | Add jitter to CachingGetSpaceUsed's thread |  Major | . | Elliott Clark | Elliott Clark |
| [HDFS-10653](https://issues.apache.org/jira/browse/HDFS-10653) | Optimize conversion from path string to components |  Major | hdfs | Daryn Sharp | Daryn Sharp |
| [HDFS-10656](https://issues.apache.org/jira/browse/HDFS-10656) | Optimize conversion of byte arrays back to path string |  Major | hdfs | Daryn Sharp | Daryn Sharp |
| [HDFS-10674](https://issues.apache.org/jira/browse/HDFS-10674) | Optimize creating a full path from an inode |  Major | hdfs | Daryn Sharp | Daryn Sharp |
| [HDFS-10655](https://issues.apache.org/jira/browse/HDFS-10655) | Fix path related byte array conversion bugs |  Major | hdfs | Daryn Sharp | Daryn Sharp |
| [HDFS-10662](https://issues.apache.org/jira/browse/HDFS-10662) | Optimize UTF8 string/byte conversions |  Major | hdfs | Daryn Sharp | Daryn Sharp |
| [HDFS-10673](https://issues.apache.org/jira/browse/HDFS-10673) | Optimize FSPermissionChecker's internal path usage |  Major | hdfs | Daryn Sharp | Daryn Sharp |
| [HDFS-10744](https://issues.apache.org/jira/browse/HDFS-10744) | Internally optimize path component resolution |  Major | hdfs | Daryn Sharp | Daryn Sharp |
| [HDFS-10619](https://issues.apache.org/jira/browse/HDFS-10619) | Cache path in InodesInPath |  Major | hdfs | Daryn Sharp | Daryn Sharp |
| [HDFS-10896](https://issues.apache.org/jira/browse/HDFS-10896) | Move lock logging logic from FSNamesystem into FSNamesystemLock |  Major | namenode | Erik Krogen | Erik Krogen |
| [HDFS-10745](https://issues.apache.org/jira/browse/HDFS-10745) | Directly resolve paths into INodesInPath |  Major | hdfs | Daryn Sharp | Daryn Sharp |
| [HADOOP-10597](https://issues.apache.org/jira/browse/HADOOP-10597) | RPC Server signals backoff to clients when all request queues are full |  Major | . | Ming Ma | Ming Ma |
| [HADOOP-10300](https://issues.apache.org/jira/browse/HADOOP-10300) | Allowed deferred sending of call responses |  Major | ipc | Daryn Sharp | Daryn Sharp |
| [HDFS-10872](https://issues.apache.org/jira/browse/HDFS-10872) | Add MutableRate metrics for FSNamesystemLock operations |  Major | namenode | Erik Krogen | Erik Krogen |
| [HADOOP-13655](https://issues.apache.org/jira/browse/HADOOP-13655) | document object store use with fs shell and distcp |  Major | documentation, fs, fs/s3 | Steve Loughran | Steve Loughran |
| [HDFS-8818](https://issues.apache.org/jira/browse/HDFS-8818) | Allow Balancer to run faster |  Major | balancer & mover | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [YARN-4140](https://issues.apache.org/jira/browse/YARN-4140) | RM container allocation delayed incase of app submitted to Nodelabel partition |  Major | scheduler | Bibin A Chundatt | Bibin A Chundatt |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-13670](https://issues.apache.org/jira/browse/HADOOP-13670) | Update CHANGES.txt to reflect all the changes in branch-2.7 |  Blocker | . | Brahma Reddy Battula | Brahma Reddy Battula |
| [YARN-6274](https://issues.apache.org/jira/browse/YARN-6274) | Documentation refers to incorrect nodemanager health checker interval property |  Trivial | documentation | Charles Zhang | Weiwei Yang |
| [HDFS-11717](https://issues.apache.org/jira/browse/HDFS-11717) | Add unit test for HDFS-11709 StandbyCheckpointer should handle non-existing legacyOivImageDir gracefully |  Minor | ha, namenode | Erik Krogen | Erik Krogen |
