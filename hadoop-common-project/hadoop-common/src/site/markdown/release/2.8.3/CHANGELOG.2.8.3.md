
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

## Release 2.8.3 - 2017-12-12

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-13588](https://issues.apache.org/jira/browse/HADOOP-13588) | ConfServlet should respect Accept request header |  Major | conf | Weiwei Yang | Weiwei Yang |
| [HADOOP-14260](https://issues.apache.org/jira/browse/HADOOP-14260) | Configuration.dumpConfiguration should redact sensitive information |  Major | conf, security | Vihang Karajgaonkar | John Zhuge |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-13933](https://issues.apache.org/jira/browse/HADOOP-13933) | Add haadmin -getAllServiceState option to get the HA state of all the NameNodes/ResourceManagers |  Major | tools | Surendra Singh Lilhore | Surendra Singh Lilhore |
| [HDFS-10480](https://issues.apache.org/jira/browse/HDFS-10480) | Add an admin command to list currently open files |  Major | . | Kihwal Lee | Manoj Govindassamy |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-13628](https://issues.apache.org/jira/browse/HADOOP-13628) | Support to retrieve specific property from configuration via REST API |  Major | conf | Weiwei Yang | Weiwei Yang |
| [HDFS-12143](https://issues.apache.org/jira/browse/HDFS-12143) | Improve performance of getting and removing inode features |  Major | namenode | Daryn Sharp | Daryn Sharp |
| [HDFS-12171](https://issues.apache.org/jira/browse/HDFS-12171) | Reduce IIP object allocations for inode lookup |  Major | namenode | Daryn Sharp | Daryn Sharp |
| [HADOOP-14455](https://issues.apache.org/jira/browse/HADOOP-14455) | ViewFileSystem#rename should support be supported within same nameservice with different mountpoints |  Major | viewfs | Brahma Reddy Battula | Brahma Reddy Battula |
| [HADOOP-14627](https://issues.apache.org/jira/browse/HADOOP-14627) | Support MSI and DeviceCode token provider in ADLS |  Major | fs/adl | Atul Sikaria | Atul Sikaria |
| [YARN-7037](https://issues.apache.org/jira/browse/YARN-7037) | Optimize data transfer with zero-copy approach for containerlogs REST API in NMWebServices |  Major | nodemanager | Tao Yang | Tao Yang |
| [HADOOP-14827](https://issues.apache.org/jira/browse/HADOOP-14827) | Allow StopWatch to accept a Timer parameter for tests |  Minor | common, test | Erik Krogen | Erik Krogen |
| [HDFS-12131](https://issues.apache.org/jira/browse/HDFS-12131) | Add some of the FSNamesystem JMX values as metrics |  Minor | hdfs, namenode | Erik Krogen | Erik Krogen |
| [HADOOP-14844](https://issues.apache.org/jira/browse/HADOOP-14844) | Remove requirement to specify TenantGuid for MSI Token Provider |  Major | fs/adl | Atul Sikaria | Atul Sikaria |
| [HADOOP-14864](https://issues.apache.org/jira/browse/HADOOP-14864) | FSDataInputStream#unbuffer UOE should include stream class name |  Minor | fs | John Zhuge | Bharat Viswanadham |
| [HDFS-12441](https://issues.apache.org/jira/browse/HDFS-12441) | Suppress UnresolvedPathException in namenode log |  Minor | . | Kihwal Lee | Kihwal Lee |
| [MAPREDUCE-6958](https://issues.apache.org/jira/browse/MAPREDUCE-6958) | Shuffle audit logger should log size of shuffle transfer |  Minor | . | Jason Lowe | Jason Lowe |
| [HDFS-12420](https://issues.apache.org/jira/browse/HDFS-12420) | Add an option to disallow 'namenode format -force' |  Major | . | Ajay Kumar | Ajay Kumar |
| [HADOOP-14521](https://issues.apache.org/jira/browse/HADOOP-14521) | KMS client needs retry logic |  Major | . | Rushabh S Shah | Rushabh S Shah |
| [HDFS-12603](https://issues.apache.org/jira/browse/HDFS-12603) | Enable async edit logging by default |  Major | namenode | Andrew Wang | Andrew Wang |
| [HDFS-12642](https://issues.apache.org/jira/browse/HDFS-12642) | Log block and datanode details in BlockRecoveryWorker |  Major | datanode | Xiao Chen | Xiao Chen |
| [HADOOP-14880](https://issues.apache.org/jira/browse/HADOOP-14880) | [KMS] Document&test missing KMS client side configs |  Minor | . | Wei-Chiu Chuang | Gabor Bota |
| [HDFS-12619](https://issues.apache.org/jira/browse/HDFS-12619) | Do not catch and throw unchecked exceptions if IBRs fail to process |  Minor | namenode | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-4163](https://issues.apache.org/jira/browse/YARN-4163) | Audit getQueueInfo and getApplications calls |  Major | . | Chang Li | Chang Li |
| [MAPREDUCE-6975](https://issues.apache.org/jira/browse/MAPREDUCE-6975) | Logging task counters |  Major | task | Prabhu Joseph | Prabhu Joseph |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-5444](https://issues.apache.org/jira/browse/YARN-5444) | Fix failing unit tests in TestLinuxContainerExecutorWithMocks |  Major | nodemanager | Yufei Gu | Yufei Gu |
| [MAPREDUCE-6808](https://issues.apache.org/jira/browse/MAPREDUCE-6808) | Log map attempts as part of shuffle handler audit log |  Major | . | Jonathan Eagles | Gergő Pásztor |
| [HADOOP-14578](https://issues.apache.org/jira/browse/HADOOP-14578) | Bind IPC connections to kerberos UPN host for proxy users |  Major | ipc | Daryn Sharp | Daryn Sharp |
| [HADOOP-14677](https://issues.apache.org/jira/browse/HADOOP-14677) | mvn clean compile fails |  Major | build | Andras Bokor | Andras Bokor |
| [HADOOP-14702](https://issues.apache.org/jira/browse/HADOOP-14702) | Fix formatting issue and regression caused by conversion from APT to Markdown |  Minor | documentation | Doris Gu | Doris Gu |
| [YARN-6965](https://issues.apache.org/jira/browse/YARN-6965) | Duplicate instantiation in FairSchedulerQueueInfo |  Minor | fairscheduler | Masahiro Tanaka | Masahiro Tanaka |
| [HDFS-11738](https://issues.apache.org/jira/browse/HDFS-11738) | Hedged pread takes more time when block moved from initial locations |  Major | hdfs-client | Vinayakumar B | Vinayakumar B |
| [HDFS-12318](https://issues.apache.org/jira/browse/HDFS-12318) | Fix IOException condition for openInfo in DFSInputStream |  Major | . | legend | legend |
| [HDFS-12336](https://issues.apache.org/jira/browse/HDFS-12336) | Listing encryption zones still fails when deleted EZ is not a direct child of snapshottable directory |  Minor | encryption, hdfs | Wellington Chevreuil | Wellington Chevreuil |
| [MAPREDUCE-6931](https://issues.apache.org/jira/browse/MAPREDUCE-6931) | Remove TestDFSIO "Total Throughput" calculation |  Critical | benchmarks, test | Dennis Huo | Dennis Huo |
| [YARN-7116](https://issues.apache.org/jira/browse/YARN-7116) | CapacityScheduler Web UI: Queue's AM usage is always show on per-user's AM usage. |  Major | capacity scheduler, webapp | Wangda Tan | Wangda Tan |
| [HADOOP-14824](https://issues.apache.org/jira/browse/HADOOP-14824) | Update ADLS SDK to 2.2.2 for MSI fix |  Major | fs/adl | Atul Sikaria | Atul Sikaria |
| [YARN-7120](https://issues.apache.org/jira/browse/YARN-7120) | CapacitySchedulerPage NPE in "Aggregate scheduler counts" section |  Minor | . | Eric Payne | Eric Payne |
| [YARN-7164](https://issues.apache.org/jira/browse/YARN-7164) | TestAMRMClientOnRMRestart fails sporadically with bind address in use |  Major | test | Jason Lowe | Jason Lowe |
| [HDFS-12369](https://issues.apache.org/jira/browse/HDFS-12369) | Edit log corruption due to hard lease recovery of not-closed file which has snapshots |  Major | namenode | Xiao Chen | Xiao Chen |
| [HADOOP-14867](https://issues.apache.org/jira/browse/HADOOP-14867) | Update HDFS Federation setup document, for incorrect property name for secondary name node http address |  Major | . | Bharat Viswanadham | Bharat Viswanadham |
| [YARN-4727](https://issues.apache.org/jira/browse/YARN-4727) | Unable to override the $HADOOP\_CONF\_DIR env variable for container |  Major | nodemanager | Terence Yim | Jason Lowe |
| [MAPREDUCE-6957](https://issues.apache.org/jira/browse/MAPREDUCE-6957) | shuffle hangs after a node manager connection timeout |  Major | mrv2 | Jooseong Kim | Jooseong Kim |
| [HDFS-12424](https://issues.apache.org/jira/browse/HDFS-12424) | Datatable sorting on the Datanode Information page in the Namenode UI is broken |  Major | . | Shawna Martell | Shawna Martell |
| [HDFS-12323](https://issues.apache.org/jira/browse/HDFS-12323) | NameNode terminates after full GC thinking QJM unresponsive if full GC is much longer than timeout |  Major | namenode, qjm | Erik Krogen | Erik Krogen |
| [MAPREDUCE-6960](https://issues.apache.org/jira/browse/MAPREDUCE-6960) | Shuffle Handler prints disk error stack traces for every read failure. |  Major | . | Kuhu Shukla | Kuhu Shukla |
| [HDFS-11799](https://issues.apache.org/jira/browse/HDFS-11799) | Introduce a config to allow setting up write pipeline with fewer nodes than replication factor |  Major | . | Yongjun Zhang | Brahma Reddy Battula |
| [YARN-6771](https://issues.apache.org/jira/browse/YARN-6771) | Use classloader inside configuration class to make new classes |  Major | . | Jongyoul Lee | Jongyoul Lee |
| [HDFS-12526](https://issues.apache.org/jira/browse/HDFS-12526) | FSDirectory should use Time.monotonicNow for durations |  Minor | . | Chetna Chaudhari | Bharat Viswanadham |
| [HDFS-12371](https://issues.apache.org/jira/browse/HDFS-12371) | "BlockVerificationFailures" and "BlocksVerified" show up as 0 in Datanode JMX |  Major | metrics | Sai Nukavarapu | Hanisha Koneru |
| [MAPREDUCE-6966](https://issues.apache.org/jira/browse/MAPREDUCE-6966) | DistSum should use Time.monotonicNow for measuring durations |  Minor | . | Chetna Chaudhari | Chetna Chaudhari |
| [HDFS-12531](https://issues.apache.org/jira/browse/HDFS-12531) | Fix conflict in the javadoc of UnderReplicatedBlocks.java in branch-2 |  Minor | documentation | Akira Ajisaka | Bharat Viswanadham |
| [HDFS-12495](https://issues.apache.org/jira/browse/HDFS-12495) | TestPendingInvalidateBlock#testPendingDeleteUnknownBlocks fails intermittently |  Major | . | Eric Badger | Eric Badger |
| [HADOOP-14891](https://issues.apache.org/jira/browse/HADOOP-14891) | Remove references to Guava Objects.toStringHelper |  Major | . | Jonathan Eagles | Jonathan Eagles |
| [HADOOP-14902](https://issues.apache.org/jira/browse/HADOOP-14902) | LoadGenerator#genFile write close timing is incorrectly calculated |  Major | fs | Jason Lowe | Hanisha Koneru |
| [YARN-7084](https://issues.apache.org/jira/browse/YARN-7084) | TestSchedulingMonitor#testRMStarts fails sporadically |  Major | . | Jason Lowe | Jason Lowe |
| [YARN-7226](https://issues.apache.org/jira/browse/YARN-7226) | Whitelisted variables do not support delayed variable expansion |  Major | nodemanager | Jason Lowe | Jason Lowe |
| [YARN-7285](https://issues.apache.org/jira/browse/YARN-7285) | ContainerExecutor always launches with priorities due to yarn-default property |  Minor | nodemanager | Jason Lowe | Jason Lowe |
| [YARN-7245](https://issues.apache.org/jira/browse/YARN-7245) | Max AM Resource column in Active Users Info section of Capacity Scheduler UI page should be updated per-user |  Major | capacity scheduler, yarn | Eric Payne | Eric Payne |
| [HADOOP-14912](https://issues.apache.org/jira/browse/HADOOP-14912) | FairCallQueue may defer servicing calls |  Major | ipc | Daryn Sharp | Daryn Sharp |
| [HDFS-12659](https://issues.apache.org/jira/browse/HDFS-12659) | Update TestDeadDatanode#testNonDFSUsedONDeadNodeReReg to increase heartbeat recheck interval |  Minor | . | Ajay Kumar | Ajay Kumar |
| [HDFS-12485](https://issues.apache.org/jira/browse/HDFS-12485) | expunge may fail to remove trash from encryption zone |  Major | . | Wei-Chiu Chuang | Wei-Chiu Chuang |
| [YARN-7333](https://issues.apache.org/jira/browse/YARN-7333) | container-executor fails to remove entries from a directory that is not writable or executable |  Critical | . | Jason Lowe | Jason Lowe |
| [HADOOP-14966](https://issues.apache.org/jira/browse/HADOOP-14966) | Handle JDK-8071638 for hadoop-common |  Blocker | . | Bibin A Chundatt | Bibin A Chundatt |
| [HDFS-9914](https://issues.apache.org/jira/browse/HDFS-9914) | Fix configurable WebhDFS connect/read timeout |  Blocker | hdfs-client, webhdfs | Xiaoyu Yao | Xiaoyu Yao |
| [YARN-7244](https://issues.apache.org/jira/browse/YARN-7244) | ShuffleHandler is not aware of disks that are added |  Major | . | Kuhu Shukla | Kuhu Shukla |
| [HADOOP-14990](https://issues.apache.org/jira/browse/HADOOP-14990) | Clean up jdiff xml files added for 2.8.2 release |  Blocker | . | Subru Krishnan | Junping Du |
| [HADOOP-14919](https://issues.apache.org/jira/browse/HADOOP-14919) | BZip2 drops records when reading data in splits |  Critical | . | Aki Tanaka | Jason Lowe |
| [YARN-7370](https://issues.apache.org/jira/browse/YARN-7370) | Preemption properties should be refreshable |  Major | capacity scheduler, scheduler preemption | Eric Payne | Gergely Novák |
| [YARN-7361](https://issues.apache.org/jira/browse/YARN-7361) | Improve the docker container runtime documentation |  Major | . | Shane Kumpf | Shane Kumpf |
| [YARN-7469](https://issues.apache.org/jira/browse/YARN-7469) | Capacity Scheduler Intra-queue preemption: User can starve if newest app is exactly at user limit |  Major | capacity scheduler, yarn | Eric Payne | Eric Payne |
| [HADOOP-15047](https://issues.apache.org/jira/browse/HADOOP-15047) | Python is required for -Preleasedoc but not documented in branch-2.8 |  Major | build, documentation | Akira Ajisaka | Bharat Viswanadham |
| [YARN-7496](https://issues.apache.org/jira/browse/YARN-7496) | CS Intra-queue preemption user-limit calculations are not in line with LeafQueue user-limit calculations |  Major | . | Eric Payne | Eric Payne |
| [HDFS-12832](https://issues.apache.org/jira/browse/HDFS-12832) | INode.getFullPathName may throw ArrayIndexOutOfBoundsException lead to NameNode exit |  Critical | namenode | DENG FEI | Konstantin Shvachko |
| [HDFS-12638](https://issues.apache.org/jira/browse/HDFS-12638) | Delete copy-on-truncate block along with the original block, when deleting a file being truncated |  Blocker | hdfs | Jiandan Yang | Konstantin Shvachko |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-14678](https://issues.apache.org/jira/browse/HADOOP-14678) | AdlFilesystem#initialize swallows exception when getting user name |  Minor | fs/adl | John Zhuge | John Zhuge |
| [HADOOP-14892](https://issues.apache.org/jira/browse/HADOOP-14892) | MetricsSystemImpl should use Time.monotonicNow for measuring durations |  Minor | . | Chetna Chaudhari | Chetna Chaudhari |
| [HADOOP-14881](https://issues.apache.org/jira/browse/HADOOP-14881) | LoadGenerator should use Time.monotonicNow() to measure durations |  Major | . | Chetna Chaudhari | Bharat Viswanadham |
| [HADOOP-14893](https://issues.apache.org/jira/browse/HADOOP-14893) | WritableRpcEngine should use Time.monotonicNow |  Minor | . | Chetna Chaudhari | Chetna Chaudhari |
| [HDFS-12386](https://issues.apache.org/jira/browse/HDFS-12386) | Add fsserver defaults call to WebhdfsFileSystem. |  Minor | webhdfs | Rushabh S Shah | Rushabh S Shah |
