
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

## Release 2.2.0 - 2013-10-15

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-1229](https://issues.apache.org/jira/browse/YARN-1229) | Define constraints on Auxiliary Service names. Change ShuffleHandler service name from mapreduce.shuffle to mapreduce\_shuffle. |  Blocker | nodemanager | Tassapol Athiapinya | Xuan Gong |
| [YARN-1228](https://issues.apache.org/jira/browse/YARN-1228) | Clean up Fair Scheduler configuration loading |  Major | scheduler | Sandy Ryza | Sandy Ryza |
| [HADOOP-10020](https://issues.apache.org/jira/browse/HADOOP-10020) | disable symlinks temporarily |  Blocker | fs | Colin P. McCabe | Sanjay Radia |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-4817](https://issues.apache.org/jira/browse/HDFS-4817) | make HDFS advisory caching configurable on a per-file basis |  Minor | hdfs-client | Colin P. McCabe | Colin P. McCabe |
| [HADOOP-9758](https://issues.apache.org/jira/browse/HADOOP-9758) | Provide configuration option for FileSystem/FileContext symlink resolution |  Major | . | Andrew Wang | Andrew Wang |
| [HDFS-5139](https://issues.apache.org/jira/browse/HDFS-5139) | Remove redundant -R option from setrep |  Major | tools | Arpit Agarwal | Arpit Agarwal |
| [YARN-1246](https://issues.apache.org/jira/browse/YARN-1246) | Log application status in the rm log when app is done running |  Minor | . | Arpit Gupta | Arpit Gupta |
| [HDFS-5256](https://issues.apache.org/jira/browse/HDFS-5256) | Use guava LoadingCache to implement DFSClientCache |  Major | nfs | Haohui Mai | Haohui Mai |
| [HADOOP-8315](https://issues.apache.org/jira/browse/HADOOP-8315) | Support SASL-authenticated ZooKeeper in ActiveStandbyElector |  Major | auto-failover, ha | Todd Lipcon | Todd Lipcon |
| [YARN-1213](https://issues.apache.org/jira/browse/YARN-1213) | Restore config to ban submitting to undeclared pools in the Fair Scheduler |  Major | scheduler | Sandy Ryza | Sandy Ryza |
| [HDFS-5308](https://issues.apache.org/jira/browse/HDFS-5308) | Replace HttpConfig#getSchemePrefix with implicit schemes in HDFS JSP |  Major | . | Haohui Mai | Haohui Mai |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-5031](https://issues.apache.org/jira/browse/HDFS-5031) | BlockScanner scans the block multiple times and on restart scans everything |  Blocker | datanode | Vinayakumar B | Vinayakumar B |
| [MAPREDUCE-5488](https://issues.apache.org/jira/browse/MAPREDUCE-5488) | Job recovery fails after killing all the running containers for the app |  Major | . | Arpit Gupta | Jian He |
| [MAPREDUCE-5515](https://issues.apache.org/jira/browse/MAPREDUCE-5515) | Application Manager UI does not appear with Https enabled |  Major | . | Omkar Vinit Joshi | Omkar Vinit Joshi |
| [HDFS-5251](https://issues.apache.org/jira/browse/HDFS-5251) | Race between the initialization of NameNode and the http server |  Major | . | Haohui Mai | Haohui Mai |
| [HDFS-5228](https://issues.apache.org/jira/browse/HDFS-5228) | The RemoteIterator returned by DistributedFileSystem.listFiles(..) may throw NPE |  Blocker | hdfs-client | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [MAPREDUCE-5523](https://issues.apache.org/jira/browse/MAPREDUCE-5523) | Need to add https port related property in Job history server |  Major | . | Omkar Vinit Joshi | Omkar Vinit Joshi |
| [YARN-1128](https://issues.apache.org/jira/browse/YARN-1128) | FifoPolicy.computeShares throws NPE on empty list of Schedulables |  Major | scheduler | Sandy Ryza | Karthik Kambatla |
| [HADOOP-9776](https://issues.apache.org/jira/browse/HADOOP-9776) | HarFileSystem.listStatus() returns invalid authority if port number is empty |  Major | fs | shanyu zhao | shanyu zhao |
| [HADOOP-9761](https://issues.apache.org/jira/browse/HADOOP-9761) | ViewFileSystem#rename fails when using DistributedFileSystem |  Blocker | viewfs | Andrew Wang | Andrew Wang |
| [MAPREDUCE-5503](https://issues.apache.org/jira/browse/MAPREDUCE-5503) | TestMRJobClient.testJobClient is failing |  Blocker | mrv2 | Jason Lowe | Jian He |
| [YARN-1157](https://issues.apache.org/jira/browse/YARN-1157) | ResourceManager UI has invalid tracking URL link for distributed shell application |  Major | resourcemanager | Tassapol Athiapinya | Xuan Gong |
| [MAPREDUCE-5170](https://issues.apache.org/jira/browse/MAPREDUCE-5170) | incorrect exception message if min node size \> min rack size |  Trivial | mrv2 | Sangjin Lee | Sangjin Lee |
| [HDFS-5258](https://issues.apache.org/jira/browse/HDFS-5258) | Skip tests in TestHDFSCLI that are not applicable on Windows. |  Minor | test | Chris Nauroth | Chuan Liu |
| [HADOOP-9976](https://issues.apache.org/jira/browse/HADOOP-9976) | Different versions of avro and avro-maven-plugin |  Major | . | Karthik Kambatla | Karthik Kambatla |
| [MAPREDUCE-5513](https://issues.apache.org/jira/browse/MAPREDUCE-5513) | ConcurrentModificationException in JobControl |  Major | . | Jason Lowe | Robert Parker |
| [MAPREDUCE-5545](https://issues.apache.org/jira/browse/MAPREDUCE-5545) | org.apache.hadoop.mapred.TestTaskAttemptListenerImpl.testCommitWindow times out |  Major | . | Robert Kanter | Robert Kanter |
| [HDFS-5268](https://issues.apache.org/jira/browse/HDFS-5268) | NFS write commit verifier is not set in a few places |  Major | nfs | Brandon Li | Brandon Li |
| [HDFS-5265](https://issues.apache.org/jira/browse/HDFS-5265) | Namenode fails to start when dfs.https.port is unspecified |  Major | . | Haohui Mai | Haohui Mai |
| [YARN-1221](https://issues.apache.org/jira/browse/YARN-1221) | With Fair Scheduler, reserved MB reported in RM web UI increases indefinitely |  Major | resourcemanager, scheduler | Sandy Ryza | Siqi Li |
| [YARN-1247](https://issues.apache.org/jira/browse/YARN-1247) | test-container-executor has gotten out of sync with the changes to container-executor |  Major | nodemanager | Roman Shaposhnik | Roman Shaposhnik |
| [MAPREDUCE-5544](https://issues.apache.org/jira/browse/MAPREDUCE-5544) | JobClient#getJob loads job conf twice |  Major | . | Sandy Ryza | Sandy Ryza |
| [YARN-1215](https://issues.apache.org/jira/browse/YARN-1215) | Yarn URL should include userinfo |  Major | api | Chuan Liu | Chuan Liu |
| [YARN-1262](https://issues.apache.org/jira/browse/YARN-1262) | TestApplicationCleanup relies on all containers assigned in a single heartbeat |  Major | . | Sandy Ryza | Karthik Kambatla |
| [HADOOP-10003](https://issues.apache.org/jira/browse/HADOOP-10003) | HarFileSystem.listLocatedStatus() fails |  Major | fs | Jason Dere |  |
| [HDFS-5255](https://issues.apache.org/jira/browse/HDFS-5255) | Distcp job fails with hsftp when https is enabled in insecure cluster |  Major | . | Yesha Vora | Arpit Agarwal |
| [MAPREDUCE-5536](https://issues.apache.org/jira/browse/MAPREDUCE-5536) | mapreduce.jobhistory.webapp.https.address property is not respected |  Blocker | . | Yesha Vora | Omkar Vinit Joshi |
| [HADOOP-10012](https://issues.apache.org/jira/browse/HADOOP-10012) | Secure Oozie jobs fail with delegation token renewal exception in Namenode HA setup |  Blocker | ha | Arpit Gupta | Suresh Srinivas |
| [HDFS-5279](https://issues.apache.org/jira/browse/HDFS-5279) | Guard against NullPointerException in NameNode JSP pages before initialization of FSNamesystem. |  Major | namenode | Chris Nauroth | Chris Nauroth |
| [MAPREDUCE-5459](https://issues.apache.org/jira/browse/MAPREDUCE-5459) | Update the doc of running MRv1 examples jar on YARN |  Major | . | Zhijie Shen | Zhijie Shen |
| [YARN-1141](https://issues.apache.org/jira/browse/YARN-1141) | Updating resource requests should be decoupled with updating blacklist |  Major | . | Zhijie Shen | Zhijie Shen |
| [MAPREDUCE-5554](https://issues.apache.org/jira/browse/MAPREDUCE-5554) | hdfs-site.xml included in hadoop-mapreduce-client-jobclient tests jar is breaking tests for downstream components |  Minor | test | Robert Kanter | Robert Kanter |
| [YARN-876](https://issues.apache.org/jira/browse/YARN-876) | Node resource is added twice when node comes back from unhealthy to healthy |  Major | resourcemanager | Peng Zhang | Peng Zhang |
| [HDFS-5289](https://issues.apache.org/jira/browse/HDFS-5289) | Race condition in TestRetryCacheWithHA#testCreateSymlink causes spurious test failure |  Major | test | Aaron T. Myers | Aaron T. Myers |
| [MAPREDUCE-5489](https://issues.apache.org/jira/browse/MAPREDUCE-5489) | MR jobs hangs as it does not use the node-blacklisting feature in RM requests |  Critical | . | Yesha Vora | Zhijie Shen |
| [YARN-890](https://issues.apache.org/jira/browse/YARN-890) | The roundup for memory values on resource manager UI is misleading |  Major | resourcemanager | Trupti Dhavle | Xuan Gong |
| [YARN-1236](https://issues.apache.org/jira/browse/YARN-1236) | FairScheduler setting queue name in RMApp is not working |  Major | resourcemanager | Sandy Ryza | Sandy Ryza |
| [YARN-1271](https://issues.apache.org/jira/browse/YARN-1271) | "Text file busy" errors launching containers again |  Major | nodemanager | Sandy Ryza | Sandy Ryza |
| [YARN-1149](https://issues.apache.org/jira/browse/YARN-1149) | NM throws InvalidStateTransitonException: Invalid event: APPLICATION\_LOG\_HANDLING\_FINISHED at RUNNING |  Major | . | Ramya Sunil | Xuan Gong |
| [MAPREDUCE-5442](https://issues.apache.org/jira/browse/MAPREDUCE-5442) | $HADOOP\_MAPRED\_HOME/$HADOOP\_CONF\_DIR setting not working on Windows |  Major | client | Yingda Chen | Yingda Chen |
| [YARN-1219](https://issues.apache.org/jira/browse/YARN-1219) | FSDownload changes file suffix making FileUtil.unTar() throw exception |  Major | nodemanager | shanyu zhao | shanyu zhao |
| [MAPREDUCE-5533](https://issues.apache.org/jira/browse/MAPREDUCE-5533) | Speculative execution does not function for reduce |  Major | applicationmaster | Tassapol Athiapinya | Xuan Gong |
| [HDFS-5300](https://issues.apache.org/jira/browse/HDFS-5300) | FSNameSystem#deleteSnapshot() should not check owner in case of permissions disabled |  Major | namenode | Vinayakumar B | Vinayakumar B |
| [YARN-1251](https://issues.apache.org/jira/browse/YARN-1251) | TestDistributedShell#TestDSShell failed with timeout |  Major | applications/distributed-shell | Junping Du | Xuan Gong |
| [YARN-1167](https://issues.apache.org/jira/browse/YARN-1167) | Submitted distributed shell application shows appMasterHost = empty |  Major | applications/distributed-shell | Tassapol Athiapinya | Xuan Gong |
| [YARN-1273](https://issues.apache.org/jira/browse/YARN-1273) | Distributed shell does not account for start container failures reported asynchronously. |  Major | . | Hitesh Shah | Hitesh Shah |
| [YARN-1032](https://issues.apache.org/jira/browse/YARN-1032) | NPE in RackResolve |  Critical | . | Lohit Vijayarenu | Lohit Vijayarenu |
| [YARN-1090](https://issues.apache.org/jira/browse/YARN-1090) | Job does not get into Pending State |  Major | . | Yesha Vora | Jian He |
| [YARN-1274](https://issues.apache.org/jira/browse/YARN-1274) | LCE fails to run containers that don't have resources to localize |  Blocker | nodemanager | Alejandro Abdelnur | Siddharth Seth |
| [YARN-1278](https://issues.apache.org/jira/browse/YARN-1278) | New AM does not start after rm restart |  Blocker | . | Yesha Vora | Hitesh Shah |
| [HDFS-5299](https://issues.apache.org/jira/browse/HDFS-5299) | DFS client hangs in updatePipeline RPC when failover happened |  Blocker | namenode | Vinayakumar B | Vinayakumar B |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-9948](https://issues.apache.org/jira/browse/HADOOP-9948) | Add a config value to CLITestHelper to skip tests on Windows |  Minor | test | Chuan Liu | Chuan Liu |
| [HDFS-5186](https://issues.apache.org/jira/browse/HDFS-5186) | TestFileJournalManager fails on Windows due to file handle leaks |  Minor | namenode, test | Chuan Liu | Chuan Liu |
| [MAPREDUCE-5525](https://issues.apache.org/jira/browse/MAPREDUCE-5525) | Increase timeout of TestDFSIO.testAppend and TestMRJobsWithHistoryService.testJobHistoryData |  Minor | mrv2, test | Chuan Liu | Chuan Liu |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-1203](https://issues.apache.org/jira/browse/YARN-1203) | Application Manager UI does not appear with Https enabled |  Major | . | Yesha Vora | Omkar Vinit Joshi |
| [YARN-1204](https://issues.apache.org/jira/browse/YARN-1204) | Need to add https port related property in Yarn |  Major | . | Yesha Vora | Omkar Vinit Joshi |
| [MAPREDUCE-5505](https://issues.apache.org/jira/browse/MAPREDUCE-5505) | Clients should be notified job finished only after job successfully unregistered |  Critical | . | Jian He | Zhijie Shen |
| [YARN-1214](https://issues.apache.org/jira/browse/YARN-1214) | Register ClientToken MasterKey in SecretManager after it is saved |  Critical | resourcemanager | Jian He | Jian He |
| [HDFS-5246](https://issues.apache.org/jira/browse/HDFS-5246) | Make Hadoop nfs server port and mount daemon port configurable |  Major | nfs | Jinghui Wang | Jinghui Wang |
| [YARN-49](https://issues.apache.org/jira/browse/YARN-49) | Improve distributed shell application to work on a secure cluster |  Major | applications/distributed-shell | Hitesh Shah | Vinod Kumar Vavilapalli |
| [MAPREDUCE-5531](https://issues.apache.org/jira/browse/MAPREDUCE-5531) | Binary and source incompatibility in mapreduce.TaskID and mapreduce.TaskAttemptID between branch-1 and branch-2 |  Blocker | mrv1, mrv2 | Robert Kanter | Robert Kanter |
| [MAPREDUCE-5529](https://issues.apache.org/jira/browse/MAPREDUCE-5529) | Binary incompatibilities in mapred.lib.TotalOrderPartitioner between branch-1 and branch-2 |  Blocker | mrv1, mrv2 | Robert Kanter | Robert Kanter |
| [YARN-899](https://issues.apache.org/jira/browse/YARN-899) | Get queue administration ACLs working |  Major | scheduler | Sandy Ryza | Xuan Gong |
| [MAPREDUCE-5538](https://issues.apache.org/jira/browse/MAPREDUCE-5538) | MRAppMaster#shutDownJob shouldn't send job end notification before checking isLastRetry |  Blocker | . | Zhijie Shen | Zhijie Shen |
| [HDFS-5230](https://issues.apache.org/jira/browse/HDFS-5230) | Introduce RpcInfo to decouple XDR classes from the RPC API |  Major | nfs | Haohui Mai | Haohui Mai |
| [YARN-1070](https://issues.apache.org/jira/browse/YARN-1070) | ContainerImpl State Machine: Invalid event: CONTAINER\_KILLED\_ON\_REQUEST at CONTAINER\_CLEANEDUP\_AFTER\_KILL |  Major | nodemanager | Hitesh Shah | Zhijie Shen |
| [MAPREDUCE-5551](https://issues.apache.org/jira/browse/MAPREDUCE-5551) | Binary Incompatibility of O.A.H.U.mapred.SequenceFileAsBinaryOutputFormat.WritableValueBytes |  Blocker | . | Zhijie Shen | Zhijie Shen |
| [YARN-1260](https://issues.apache.org/jira/browse/YARN-1260) | RM\_HOME link breaks when webapp.https.address related properties are not specified |  Major | . | Yesha Vora | Omkar Vinit Joshi |
| [MAPREDUCE-5530](https://issues.apache.org/jira/browse/MAPREDUCE-5530) | Binary and source incompatibility in mapred.lib.CombineFileInputFormat between branch-1 and branch-2 |  Blocker | mrv1, mrv2 | Robert Kanter | Robert Kanter |
| [YARN-621](https://issues.apache.org/jira/browse/YARN-621) | RM triggers web auth failure before first job |  Critical | resourcemanager | Allen Wittenauer | Omkar Vinit Joshi |
| [YARN-1256](https://issues.apache.org/jira/browse/YARN-1256) | NM silently ignores non-existent service in StartContainerRequest |  Critical | . | Bikas Saha | Xuan Gong |
| [YARN-1131](https://issues.apache.org/jira/browse/YARN-1131) | $yarn logs command should return an appropriate error message if YARN application is still running |  Minor | client | Tassapol Athiapinya | Siddharth Seth |
| [YARN-1254](https://issues.apache.org/jira/browse/YARN-1254) | NM is polluting container's credentials |  Major | . | Vinod Kumar Vavilapalli | Omkar Vinit Joshi |
| [HDFS-5306](https://issues.apache.org/jira/browse/HDFS-5306) | Datanode https port is not available at the namenode |  Major | datanode, namenode | Suresh Srinivas | Suresh Srinivas |
| [HADOOP-10017](https://issues.apache.org/jira/browse/HADOOP-10017) | Fix NPE in DFSClient#getDelegationToken when doing Distcp from a secured cluster to an insecured cluster |  Major | . | Jing Zhao | Haohui Mai |
| [YARN-1277](https://issues.apache.org/jira/browse/YARN-1277) | Add http policy support for YARN daemons |  Major | . | Suresh Srinivas | Omkar Vinit Joshi |
| [MAPREDUCE-5562](https://issues.apache.org/jira/browse/MAPREDUCE-5562) | MR AM should exit when unregister() throws exception |  Major | . | Zhijie Shen | Zhijie Shen |
| [HDFS-5259](https://issues.apache.org/jira/browse/HDFS-5259) | Support client which combines appended data with old data before sends it to NFS server |  Major | nfs | Yesha Vora | Brandon Li |


