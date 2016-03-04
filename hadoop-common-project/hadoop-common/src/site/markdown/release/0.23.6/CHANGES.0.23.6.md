
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

## Release 0.23.6 - 2013-02-06

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
| [HADOOP-8992](https://issues.apache.org/jira/browse/HADOOP-8992) | Enhance unit-test coverage of class HarFileSystem |  Minor | . | Ivan A. Veselovsky | Ivan A. Veselovsky |
| [HADOOP-8931](https://issues.apache.org/jira/browse/HADOOP-8931) | Add Java version to startup message |  Trivial | . | Eli Collins | Eli Collins |
| [HADOOP-8561](https://issues.apache.org/jira/browse/HADOOP-8561) | Introduce HADOOP\_PROXY\_USER for secure impersonation in child hadoop client processes |  Major | security | Luke Lu | Yu Gao |
| [MAPREDUCE-4899](https://issues.apache.org/jira/browse/MAPREDUCE-4899) | Provide a plugin to the Yarn Web App Proxy to generate tracking links for M/R appllications given the ID |  Major | . | Derek Dagit | Derek Dagit |
| [MAPREDUCE-4845](https://issues.apache.org/jira/browse/MAPREDUCE-4845) | ClusterStatus.getMaxMemory() and getUsedMemory() exist in MR1 but not MR2 |  Major | client | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-4811](https://issues.apache.org/jira/browse/MAPREDUCE-4811) | JobHistoryServer should show when it was started in WebUI About page |  Minor | jobhistoryserver, mrv2 | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-4810](https://issues.apache.org/jira/browse/MAPREDUCE-4810) | Add admin command options for ApplicationMaster |  Minor | applicationmaster | Jason Lowe | Jerry Chen |
| [MAPREDUCE-4764](https://issues.apache.org/jira/browse/MAPREDUCE-4764) | repair test org.apache.hadoop.mapreduce.security.TestBinaryTokenFile |  Major | . | Ivan A. Veselovsky |  |
| [YARN-285](https://issues.apache.org/jira/browse/YARN-285) | RM should be able to provide a tracking link for apps that have already been purged |  Major | . | Derek Dagit | Derek Dagit |
| [YARN-80](https://issues.apache.org/jira/browse/YARN-80) | Support delay scheduling for node locality in MR2's capacity scheduler |  Major | capacityscheduler | Todd Lipcon | Arun C Murthy |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-9255](https://issues.apache.org/jira/browse/HADOOP-9255) | relnotes.py missing last jira |  Critical | scripts | Thomas Graves | Thomas Graves |
| [HADOOP-9242](https://issues.apache.org/jira/browse/HADOOP-9242) | Duplicate surefire plugin config in hadoop-common |  Major | test | Andrey Klochkov | Andrey Klochkov |
| [HADOOP-9181](https://issues.apache.org/jira/browse/HADOOP-9181) | Set daemon flag for HttpServer's QueuedThreadPool |  Major | . | Liang Xie | Liang Xie |
| [HADOOP-9169](https://issues.apache.org/jira/browse/HADOOP-9169) | Bring branch-0.23 ExitUtil up to same level as branch-2 |  Major | . | Robert Joseph Evans | Robert Joseph Evans |
| [HADOOP-9152](https://issues.apache.org/jira/browse/HADOOP-9152) | HDFS can report negative DFS Used on clusters with very small amounts of data |  Minor | fs | Brock Noland | Brock Noland |
| [HADOOP-9135](https://issues.apache.org/jira/browse/HADOOP-9135) | JniBasedUnixGroupsMappingWithFallback should log at debug rather than info during fallback |  Trivial | security | Colin Patrick McCabe | Colin Patrick McCabe |
| [HADOOP-9108](https://issues.apache.org/jira/browse/HADOOP-9108) | Add a method to clear terminateCalled to ExitUtil for test cases |  Major | util | Kihwal Lee | Kihwal Lee |
| [HADOOP-9105](https://issues.apache.org/jira/browse/HADOOP-9105) | FsShell -moveFromLocal erroneously fails |  Major | fs | Daryn Sharp | Daryn Sharp |
| [HADOOP-9097](https://issues.apache.org/jira/browse/HADOOP-9097) | Maven RAT plugin is not checking all source files |  Critical | build | Tom White | Thomas Graves |
| [HADOOP-9072](https://issues.apache.org/jira/browse/HADOOP-9072) | Hadoop-Common-0.23-Build Fails to build in Jenkins |  Major | . | Robert Parker | Robert Parker |
| [HADOOP-7868](https://issues.apache.org/jira/browse/HADOOP-7868) | Hadoop native fails to compile when default linker option is -Wl,--as-needed |  Major | native | James Page | Trevor Robinson |
| [HDFS-4426](https://issues.apache.org/jira/browse/HDFS-4426) | Secondary namenode shuts down immediately after startup |  Blocker | namenode | Jason Lowe | Arpit Agarwal |
| [HDFS-4385](https://issues.apache.org/jira/browse/HDFS-4385) | Maven RAT plugin is not checking all source files |  Critical | build | Thomas Graves | Thomas Graves |
| [HDFS-4315](https://issues.apache.org/jira/browse/HDFS-4315) | DNs with multiple BPs can have BPOfferServices fail to start due to unsynchronized map access |  Major | datanode | Aaron T. Myers | Aaron T. Myers |
| [HDFS-4270](https://issues.apache.org/jira/browse/HDFS-4270) | Replications of the highest priority should be allowed to choose a source datanode that has reached its max replication limit |  Minor | namenode | Derek Dagit | Derek Dagit |
| [HDFS-4254](https://issues.apache.org/jira/browse/HDFS-4254) | testAllEditsDirsFailOnFlush makes subsequent test cases fail (0.23.6 only) |  Major | test | Kihwal Lee | Kihwal Lee |
| [HDFS-4242](https://issues.apache.org/jira/browse/HDFS-4242) | Map.Entry is incorrectly used in LeaseManager |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-4233](https://issues.apache.org/jira/browse/HDFS-4233) | NN keeps serving even after no journals started while rolling edit |  Blocker | namenode | Kihwal Lee | Kihwal Lee |
| [HDFS-4232](https://issues.apache.org/jira/browse/HDFS-4232) | NN fails to write a fsimage with stale leases |  Blocker | namenode | Daryn Sharp | Daryn Sharp |
| [HDFS-4186](https://issues.apache.org/jira/browse/HDFS-4186) | logSync() is called with the write lock held while releasing lease |  Critical | namenode | Kihwal Lee | Kihwal Lee |
| [HDFS-2757](https://issues.apache.org/jira/browse/HDFS-2757) | Cannot read a local block that's being written to when using the local read short circuit |  Major | . | Jean-Daniel Cryans | Jean-Daniel Cryans |
| [MAPREDUCE-4934](https://issues.apache.org/jira/browse/MAPREDUCE-4934) | Maven RAT plugin is not checking all source files |  Critical | build | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4925](https://issues.apache.org/jira/browse/MAPREDUCE-4925) | The pentomino option parser may be buggy |  Major | examples | Karthik Kambatla | Karthik Kambatla |
| [MAPREDUCE-4921](https://issues.apache.org/jira/browse/MAPREDUCE-4921) | JobClient should acquire HS token with RM principal |  Blocker | client | Daryn Sharp | Daryn Sharp |
| [MAPREDUCE-4913](https://issues.apache.org/jira/browse/MAPREDUCE-4913) | TestMRAppMaster#testMRAppMasterMissingStaging occasionally exits |  Major | mr-am | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4902](https://issues.apache.org/jira/browse/MAPREDUCE-4902) | Fix typo "receievd" should be "received" in log output |  Trivial | . | Albert Chu | Albert Chu |
| [MAPREDUCE-4895](https://issues.apache.org/jira/browse/MAPREDUCE-4895) | Fix compilation failure of org.apache.hadoop.mapred.gridmix.TestResourceUsageEmulators |  Major | . | Dennis Y | Dennis Y |
| [MAPREDUCE-4894](https://issues.apache.org/jira/browse/MAPREDUCE-4894) | Renewal / cancellation of JobHistory tokens |  Blocker | jobhistoryserver, mrv2 | Siddharth Seth | Siddharth Seth |
| [MAPREDUCE-4890](https://issues.apache.org/jira/browse/MAPREDUCE-4890) | Invalid TaskImpl state transitions when task fails while speculating |  Critical | mr-am | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4861](https://issues.apache.org/jira/browse/MAPREDUCE-4861) | Cleanup: Remove unused mapreduce.security.token.DelegationTokenRenewal |  Major | . | Karthik Kambatla | Karthik Kambatla |
| [MAPREDUCE-4856](https://issues.apache.org/jira/browse/MAPREDUCE-4856) | TestJobOutputCommitter uses same directory as TestJobCleanup |  Major | test | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-4848](https://issues.apache.org/jira/browse/MAPREDUCE-4848) | TaskAttemptContext cast error during AM recovery |  Major | mr-am | Jason Lowe | Jerry Chen |
| [MAPREDUCE-4842](https://issues.apache.org/jira/browse/MAPREDUCE-4842) | Shuffle race can hang reducer |  Blocker | mrv2 | Jason Lowe | Mariappan Asokan |
| [MAPREDUCE-4836](https://issues.apache.org/jira/browse/MAPREDUCE-4836) | Elapsed time for running tasks on AM web UI tasks page is 0 |  Major | . | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-4833](https://issues.apache.org/jira/browse/MAPREDUCE-4833) | Task can get stuck in FAIL\_CONTAINER\_CLEANUP |  Critical | applicationmaster, mrv2 | Robert Joseph Evans | Robert Parker |
| [MAPREDUCE-4832](https://issues.apache.org/jira/browse/MAPREDUCE-4832) | MR AM can get in a split brain situation |  Critical | applicationmaster | Robert Joseph Evans | Jason Lowe |
| [MAPREDUCE-4825](https://issues.apache.org/jira/browse/MAPREDUCE-4825) | JobImpl.finished doesn't expect ERROR as a final job state |  Major | mr-am | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4819](https://issues.apache.org/jira/browse/MAPREDUCE-4819) | AM can rerun job after reporting final job status to the client |  Blocker | mr-am | Jason Lowe | Bikas Saha |
| [MAPREDUCE-4817](https://issues.apache.org/jira/browse/MAPREDUCE-4817) | Hardcoded task ping timeout kills tasks localizing large amounts of data |  Critical | applicationmaster, mr-am | Jason Lowe | Thomas Graves |
| [MAPREDUCE-4813](https://issues.apache.org/jira/browse/MAPREDUCE-4813) | AM timing out during job commit |  Critical | applicationmaster | Jason Lowe | Jason Lowe |
| [MAPREDUCE-4678](https://issues.apache.org/jira/browse/MAPREDUCE-4678) | Running the Pentomino example with defaults throws java.lang.NegativeArraySizeException |  Minor | examples | Chris McConnell | Chris McConnell |
| [MAPREDUCE-4279](https://issues.apache.org/jira/browse/MAPREDUCE-4279) | getClusterStatus() fails with null pointer exception when running jobs in local mode |  Major | jobtracker | Rahul Jain | Devaraj K |
| [YARN-354](https://issues.apache.org/jira/browse/YARN-354) | WebAppProxyServer exits immediately after startup |  Blocker | . | Liang Xie | Liang Xie |
| [YARN-334](https://issues.apache.org/jira/browse/YARN-334) | Maven RAT plugin is not checking all source files |  Critical | . | Thomas Graves | Thomas Graves |
| [YARN-325](https://issues.apache.org/jira/browse/YARN-325) | RM CapacityScheduler can deadlock when getQueueInfo() is called and a container is completing |  Blocker | capacityscheduler | Jason Lowe | Arun C Murthy |
| [YARN-320](https://issues.apache.org/jira/browse/YARN-320) | RM should always be able to renew its own tokens |  Blocker | resourcemanager | Daryn Sharp | Daryn Sharp |
| [YARN-293](https://issues.apache.org/jira/browse/YARN-293) | Node Manager leaks LocalizerRunner object for every Container |  Critical | nodemanager | Devaraj K | Robert Joseph Evans |
| [YARN-266](https://issues.apache.org/jira/browse/YARN-266) | RM and JHS Web UIs are blank because AppsBlock is not escaping string properly |  Critical | resourcemanager | Ravi Prakash | Ravi Prakash |
| [YARN-258](https://issues.apache.org/jira/browse/YARN-258) | RM web page UI shows Invalid Date for start and finish times |  Major | resourcemanager | Ravi Prakash | Ravi Prakash |
| [YARN-251](https://issues.apache.org/jira/browse/YARN-251) | Proxy URI generation fails for blank tracking URIs |  Major | resourcemanager | Tom White | Tom White |
| [YARN-225](https://issues.apache.org/jira/browse/YARN-225) | Proxy Link in RM UI thows NPE in Secure mode |  Critical | resourcemanager | Devaraj K | Devaraj K |
| [YARN-223](https://issues.apache.org/jira/browse/YARN-223) | Change processTree interface to work better with native code |  Critical | . | Radim Kolar | Radim Kolar |
| [YARN-170](https://issues.apache.org/jira/browse/YARN-170) | NodeManager stop() gets called twice on shutdown |  Major | nodemanager | Sandy Ryza | Sandy Ryza |
| [YARN-72](https://issues.apache.org/jira/browse/YARN-72) | NM should handle cleaning up containers when it shuts down |  Major | nodemanager | Hitesh Shah | Sandy Ryza |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-9217](https://issues.apache.org/jira/browse/HADOOP-9217) | Print thread dumps when hadoop-common tests fail |  Major | test | Andrey Klochkov | Andrey Klochkov |
| [HADOOP-9038](https://issues.apache.org/jira/browse/HADOOP-9038) | provide unit-test coverage of class org.apache.hadoop.fs.LocalDirAllocator.AllocatorPerContext.PathIterator |  Minor | . | Ivan A. Veselovsky | Ivan A. Veselovsky |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-9020](https://issues.apache.org/jira/browse/HADOOP-9020) | Add a SASL PLAIN server |  Major | ipc, security | Daryn Sharp | Daryn Sharp |
| [HDFS-4248](https://issues.apache.org/jira/browse/HDFS-4248) | Renames may remove file leases |  Blocker | namenode | Daryn Sharp | Daryn Sharp |
| [HDFS-4247](https://issues.apache.org/jira/browse/HDFS-4247) | saveNamespace should be tolerant of dangling lease |  Blocker | namenode | Daryn Sharp | Daryn Sharp |
| [YARN-280](https://issues.apache.org/jira/browse/YARN-280) | RM does not reject app submission with invalid tokens |  Major | resourcemanager | Daryn Sharp | Daryn Sharp |
| [YARN-204](https://issues.apache.org/jira/browse/YARN-204) | test coverage for org.apache.hadoop.tools |  Major | applications | Aleksey Gorshkov | Aleksey Gorshkov |
| [YARN-50](https://issues.apache.org/jira/browse/YARN-50) | Implement renewal / cancellation of Delegation Tokens |  Blocker | . | Siddharth Seth | Siddharth Seth |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-8427](https://issues.apache.org/jira/browse/HADOOP-8427) | Convert Forrest docs to APT, incremental |  Major | documentation | Eli Collins | Andy Isaacson |


