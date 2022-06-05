
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

## Release 1.3.0 - Unreleased (as of 2016-03-04)

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-5777](https://issues.apache.org/jira/browse/MAPREDUCE-5777) | Support utf-8 text with BOM (byte order marker) |  Major | . | bc Wong | zhihai xu |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-10614](https://issues.apache.org/jira/browse/HADOOP-10614) | CBZip2InputStream is not threadsafe |  Major | . | Xiangrui Meng | Xiangrui Meng |
| [HADOOP-9855](https://issues.apache.org/jira/browse/HADOOP-9855) | Backport HADOOP-6578 to branch-1 |  Major | . | James Kinley | James Kinley |
| [HADOOP-9450](https://issues.apache.org/jira/browse/HADOOP-9450) | HADOOP\_USER\_CLASSPATH\_FIRST is not honored; CLASSPATH is PREpended instead of APpended |  Major | scripts | Mitch Wyle | Harsh J |
| [HADOOP-8873](https://issues.apache.org/jira/browse/HADOOP-8873) | Port HADOOP-8175 (Add mkdir -p flag) to branch-1 |  Major | . | Eli Collins | Akira AJISAKA |
| [HDFS-7312](https://issues.apache.org/jira/browse/HDFS-7312) | Update DistCp v1 to optionally not use tmp location (branch-1 only) |  Minor | tools | Joseph Prosser | Joseph Prosser |
| [HDFS-5367](https://issues.apache.org/jira/browse/HDFS-5367) | Restoring namenode storage locks namenode due to unnecessary fsimage write |  Major | . | zhaoyunjiong | zhaoyunjiong |
| [HDFS-5038](https://issues.apache.org/jira/browse/HDFS-5038) | Backport several branch-2 APIs to branch-1 |  Minor | . | Jing Zhao | Jing Zhao |
| [HDFS-4963](https://issues.apache.org/jira/browse/HDFS-4963) | Improve multihoming support in namenode |  Major | namenode | Arpit Agarwal | Arpit Agarwal |
| [HDFS-4903](https://issues.apache.org/jira/browse/HDFS-4903) | Print trash configuration and trash emptier state in namenode log |  Minor | namenode | Suresh Srinivas | Arpit Agarwal |
| [HDFS-4521](https://issues.apache.org/jira/browse/HDFS-4521) | invalid network topologies should not be cached |  Minor | . | Colin Patrick McCabe | Colin Patrick McCabe |
| [MAPREDUCE-6088](https://issues.apache.org/jira/browse/MAPREDUCE-6088) | TestTokenCache tests should use their own JobConf instances |  Major | mrv1, test | zhihai xu | zhihai xu |
| [MAPREDUCE-5712](https://issues.apache.org/jira/browse/MAPREDUCE-5712) | Backport Fair Scheduler pool placement by secondary group |  Major | scheduler | Ted Malaska | Ted Malaska |
| [MAPREDUCE-5651](https://issues.apache.org/jira/browse/MAPREDUCE-5651) | Backport Fair Scheduler queue placement policies to branch-1 |  Major | scheduler | Sandy Ryza | Ted Malaska |
| [MAPREDUCE-5609](https://issues.apache.org/jira/browse/MAPREDUCE-5609) | Add debug log message when sending job end notification |  Major | . | Robert Kanter | Robert Kanter |
| [MAPREDUCE-5457](https://issues.apache.org/jira/browse/MAPREDUCE-5457) | Add a KeyOnlyTextOutputReader to enable streaming to write out text files without separators |  Major | . | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-5408](https://issues.apache.org/jira/browse/MAPREDUCE-5408) | CLONE - The logging level of the tasks should be configurable by the job |  Major | . | Owen O'Malley | Arun C Murthy |
| [MAPREDUCE-5406](https://issues.apache.org/jira/browse/MAPREDUCE-5406) | Improve logging around Task Tracker exiting with JVM manager inconsistent state |  Major | tasktracker | Chelsey Chang | Chelsey Chang |
| [MAPREDUCE-5367](https://issues.apache.org/jira/browse/MAPREDUCE-5367) | Local jobs all use same local working directory |  Major | . | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-3310](https://issues.apache.org/jira/browse/MAPREDUCE-3310) | Custom grouping comparator cannot be set for Combiners |  Major | client | Mathias Herberts | Alejandro Abdelnur |
| [MAPREDUCE-2351](https://issues.apache.org/jira/browse/MAPREDUCE-2351) | mapred.job.tracker.history.completed.location should support an arbitrary filesystem URI |  Major | . | Tom White | Tom White |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-11035](https://issues.apache.org/jira/browse/HADOOP-11035) | distcp on mr1(branch-1) fails with NPE using a short relative source path. |  Major | tools | zhihai xu | zhihai xu |
| [HADOOP-10562](https://issues.apache.org/jira/browse/HADOOP-10562) | Namenode exits on exception without printing stack trace in AbstractDelegationTokenSecretManager |  Critical | . | Suresh Srinivas | Suresh Srinivas |
| [HADOOP-10142](https://issues.apache.org/jira/browse/HADOOP-10142) | Avoid groups lookup for unprivileged users such as "dr.who" |  Major | . | Vinayakumar B | Vinayakumar B |
| [HADOOP-10090](https://issues.apache.org/jira/browse/HADOOP-10090) | Jobtracker metrics not updated properly after execution of a mapreduce job |  Major | metrics | Ivan Mitic | Ivan Mitic |
| [HADOOP-10009](https://issues.apache.org/jira/browse/HADOOP-10009) | Backport HADOOP-7808 to branch-1 |  Major | . | Haohui Mai | Haohui Mai |
| [HADOOP-9801](https://issues.apache.org/jira/browse/HADOOP-9801) | Configuration#writeXml uses platform defaulting encoding, which may mishandle multi-byte characters. |  Major | conf | Chris Nauroth | Chris Nauroth |
| [HADOOP-9768](https://issues.apache.org/jira/browse/HADOOP-9768) | chown and chgrp reject users and groups with spaces on platforms where spaces are otherwise acceptable |  Major | fs | Chris Nauroth | Chris Nauroth |
| [HADOOP-9678](https://issues.apache.org/jira/browse/HADOOP-9678) | TestRPC#testStopsAllThreads intermittently fails on Windows |  Major | . | Ivan Mitic | Ivan Mitic |
| [HADOOP-9507](https://issues.apache.org/jira/browse/HADOOP-9507) | LocalFileSystem rename() is broken in some cases when destination exists |  Minor | fs | Mostafa Elhemali | Chris Nauroth |
| [HADOOP-9307](https://issues.apache.org/jira/browse/HADOOP-9307) | BufferedFSInputStream.read returns wrong results after certain seeks |  Major | fs | Todd Lipcon | Todd Lipcon |
| [HADOOP-7140](https://issues.apache.org/jira/browse/HADOOP-7140) | IPC Reader threads do not stop when server stops |  Critical | . | Todd Lipcon | Todd Lipcon |
| [HDFS-7503](https://issues.apache.org/jira/browse/HDFS-7503) | Namenode restart after large deletions can cause slow processReport (due to logging) |  Major | namenode | Arpit Agarwal | Arpit Agarwal |
| [HDFS-6822](https://issues.apache.org/jira/browse/HDFS-6822) | In branch-1, Namenode and datanode fails to replace "\_HOST" to hostname for hadoop.http.authentication.kerberos.principal |  Major | security | Jing Zhao | Jing Zhao |
| [HDFS-6649](https://issues.apache.org/jira/browse/HDFS-6649) | Documentation for setrep is wrong |  Trivial | documentation | Alexander Fahlke | Akira AJISAKA |
| [HDFS-6141](https://issues.apache.org/jira/browse/HDFS-6141) | WebHdfsFileSystem#toUrl does not perform character escaping. |  Major | webhdfs | Chris Nauroth | Chris Nauroth |
| [HDFS-5944](https://issues.apache.org/jira/browse/HDFS-5944) | LeaseManager:findLeaseWithPrefixPath can't handle path like /a/b/ right and cause SecondaryNameNode failed do checkpoint |  Major | namenode | zhaoyunjiong | zhaoyunjiong |
| [HDFS-5685](https://issues.apache.org/jira/browse/HDFS-5685) | DistCp will fail to copy with -delete switch |  Major | hdfs-client | Yongjun Zhang | Yongjun Zhang |
| [HDFS-5516](https://issues.apache.org/jira/browse/HDFS-5516) | WebHDFS does not require user name when anonymous http requests are disallowed. |  Major | webhdfs | Chris Nauroth | Miodrag Radulovic |
| [HDFS-5245](https://issues.apache.org/jira/browse/HDFS-5245) | shouldRetry() in WebHDFSFileSystem generates excessive warnings |  Minor | webhdfs | Haohui Mai | Haohui Mai |
| [HDFS-5211](https://issues.apache.org/jira/browse/HDFS-5211) | Race condition between DistributedFileSystem#close and FileSystem#close can cause return of a closed DistributedFileSystem instance from the FileSystem cache. |  Major | hdfs-client | Chris Nauroth | Chris Nauroth |
| [HDFS-5003](https://issues.apache.org/jira/browse/HDFS-5003) | TestNNThroughputBenchmark failed caused by existing directories |  Minor | test | Xi Fang | Xi Fang |
| [HDFS-4944](https://issues.apache.org/jira/browse/HDFS-4944) | WebHDFS cannot create a file path containing characters that must be URI-encoded, such as space. |  Major | webhdfs | Chris Nauroth | Chris Nauroth |
| [HDFS-4898](https://issues.apache.org/jira/browse/HDFS-4898) | BlockPlacementPolicyWithNodeGroup.chooseRemoteRack() fails to properly fallback to local rack |  Minor | namenode | Eric Sirianni | Tsz Wo Nicholas Sze |
| [HDFS-4794](https://issues.apache.org/jira/browse/HDFS-4794) | Browsing filesystem via webui throws kerberos exception when NN service RPC is enabled in a secure cluster |  Major | security | Benoy Antony | Benoy Antony |
| [HDFS-4622](https://issues.apache.org/jira/browse/HDFS-4622) | Remove redundant synchronized from FSNamesystem#rollEditLog in branch-1 |  Trivial | . | Jing Zhao | Jing Zhao |
| [HDFS-2264](https://issues.apache.org/jira/browse/HDFS-2264) | NamenodeProtocol has the wrong value for clientPrincipal in KerberosInfo annotation |  Major | namenode | Aaron T. Myers | Aaron T. Myers |
| [MAPREDUCE-6198](https://issues.apache.org/jira/browse/MAPREDUCE-6198) | NPE from JobTracker#resolveAndAddToTopology in MR1 cause initJob and heartbeat failure. |  Major | mrv1 | zhihai xu | zhihai xu |
| [MAPREDUCE-6196](https://issues.apache.org/jira/browse/MAPREDUCE-6196) | Fix BigDecimal ArithmeticException in PiEstimator |  Minor | . | Ray Chiang | Ray Chiang |
| [MAPREDUCE-6170](https://issues.apache.org/jira/browse/MAPREDUCE-6170) | TestUlimit failure on JDK8 |  Major | contrib/streaming | bc Wong | bc Wong |
| [MAPREDUCE-6147](https://issues.apache.org/jira/browse/MAPREDUCE-6147) | Support mapreduce.input.fileinputformat.split.maxsize |  Minor | mrv1 | zhihai xu | zhihai xu |
| [MAPREDUCE-6076](https://issues.apache.org/jira/browse/MAPREDUCE-6076) | Zero map split input length combine with none zero  map split input length may cause MR1 job hung sometimes. |  Major | mrv1 | zhihai xu | zhihai xu |
| [MAPREDUCE-6012](https://issues.apache.org/jira/browse/MAPREDUCE-6012) | DBInputSplit creates invalid ranges on Oracle |  Major | . | Julien Serdaru | Wei Yan |
| [MAPREDUCE-6009](https://issues.apache.org/jira/browse/MAPREDUCE-6009) | Map-only job with new-api runs wrong OutputCommitter when cleanup scheduled in a reduce slot |  Blocker | client, job submission | Gera Shegalov | Gera Shegalov |
| [MAPREDUCE-5979](https://issues.apache.org/jira/browse/MAPREDUCE-5979) | FairScheduler: zero weight can cause sort failures |  Major | scheduler | Anubhav Dhoot | Anubhav Dhoot |
| [MAPREDUCE-5968](https://issues.apache.org/jira/browse/MAPREDUCE-5968) | Work directory is not deleted when downloadCacheObject throws IOException |  Major | mrv1 | zhihai xu | zhihai xu |
| [MAPREDUCE-5966](https://issues.apache.org/jira/browse/MAPREDUCE-5966) | MR1 FairScheduler use of custom weight adjuster is not thread safe for comparisons |  Major | scheduler | Anubhav Dhoot | Anubhav Dhoot |
| [MAPREDUCE-5877](https://issues.apache.org/jira/browse/MAPREDUCE-5877) | Inconsistency between JT/TT for tasks taking a long time to launch |  Critical | jobtracker, tasktracker | Karthik Kambatla | Karthik Kambatla |
| [MAPREDUCE-5822](https://issues.apache.org/jira/browse/MAPREDUCE-5822) | FairScheduler does not preempt due to fairshare-starvation when fairshare is 1 |  Major | scheduler | Anubhav Dhoot | Anubhav Dhoot |
| [MAPREDUCE-5808](https://issues.apache.org/jira/browse/MAPREDUCE-5808) | Port output replication factor configurable for terasort to Hadoop 1.x |  Minor | examples | Chuan Liu | Chuan Liu |
| [MAPREDUCE-5710](https://issues.apache.org/jira/browse/MAPREDUCE-5710) | Backport MAPREDUCE-1305 to branch-1 |  Major | . | Yongjun Zhang | Yongjun Zhang |
| [MAPREDUCE-5702](https://issues.apache.org/jira/browse/MAPREDUCE-5702) | TaskLogServlet#printTaskLog has spurious HTML closing tags |  Trivial | task | Karthik Kambatla | Robert Kanter |
| [MAPREDUCE-5698](https://issues.apache.org/jira/browse/MAPREDUCE-5698) | Backport MAPREDUCE-1285 to branch-1 |  Major | . | Yongjun Zhang | Yongjun Zhang |
| [MAPREDUCE-5660](https://issues.apache.org/jira/browse/MAPREDUCE-5660) | Log info about possible thrashing (when using memory-based scheduling in Capacity Scheduler) is not printed |  Trivial | capacity-sched, mrv1, tasktracker | Adam Kawa | Adam Kawa |
| [MAPREDUCE-5569](https://issues.apache.org/jira/browse/MAPREDUCE-5569) | FloatSplitter is not generating correct splits |  Major | . | Nathan Roberts | Nathan Roberts |
| [MAPREDUCE-5556](https://issues.apache.org/jira/browse/MAPREDUCE-5556) | mapred docs have incorrect classpath |  Trivial | . | Allen Wittenauer | Harsh J |
| [MAPREDUCE-5512](https://issues.apache.org/jira/browse/MAPREDUCE-5512) | TaskTracker hung after failed reconnect to the JobTracker |  Major | tasktracker | Ivan Mitic | Ivan Mitic |
| [MAPREDUCE-5508](https://issues.apache.org/jira/browse/MAPREDUCE-5508) | JobTracker memory leak caused by unreleased FileSystem objects in JobInProgress#cleanupJob |  Critical | jobtracker | Xi Fang | Xi Fang |
| [MAPREDUCE-5405](https://issues.apache.org/jira/browse/MAPREDUCE-5405) | Job recovery can fail if task log directory symlink from prior run still exists |  Major | mrv1 | Chris Nauroth | Chris Nauroth |
| [MAPREDUCE-5288](https://issues.apache.org/jira/browse/MAPREDUCE-5288) | ResourceEstimator#getEstimatedTotalMapOutputSize suffers from divide by zero issues |  Major | mrv1 | Harsh J | Karthik Kambatla |
| [MAPREDUCE-5272](https://issues.apache.org/jira/browse/MAPREDUCE-5272) | A Minor Error in Javadoc of TestMRWithDistributedCache in Branch-1 |  Trivial | test | Zhijie Shen | Zhijie Shen |
| [MAPREDUCE-5250](https://issues.apache.org/jira/browse/MAPREDUCE-5250) | Searching for ';' in JobTracker History throws ArrayOutOfBoundException |  Minor | . | Karthik Kambatla | Karthik Kambatla |
| [MAPREDUCE-5218](https://issues.apache.org/jira/browse/MAPREDUCE-5218) | Annotate (comment) internal classes as Private |  Minor | . | Karthik Kambatla | Karthik Kambatla |
| [MAPREDUCE-5217](https://issues.apache.org/jira/browse/MAPREDUCE-5217) | DistCp fails when launched by Oozie in a secure cluster |  Major | distcp, security | Venkat Ranganathan | Venkat Ranganathan |
| [MAPREDUCE-5183](https://issues.apache.org/jira/browse/MAPREDUCE-5183) | In, TaskTracker#reportProgress logging of 0.0-1.0 progress is followed by percent sign |  Minor | mrv1, tasktracker | Sandy Ryza | Niranjan Singh |
| [MAPREDUCE-5133](https://issues.apache.org/jira/browse/MAPREDUCE-5133) | TestSubmitJob.testSecureJobExecution is flaky due to job dir deletion race |  Major | test | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-5095](https://issues.apache.org/jira/browse/MAPREDUCE-5095) | TestShuffleExceptionCount#testCheckException fails occasionally with JDK7 |  Major | . | Arpit Agarwal | Arpit Agarwal |
| [MAPREDUCE-5073](https://issues.apache.org/jira/browse/MAPREDUCE-5073) | TestJobStatusPersistency.testPersistency fails on JDK7 |  Major | test | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-5072](https://issues.apache.org/jira/browse/MAPREDUCE-5072) | TestDelegationTokenRenewal.testDTRenewal fails in MR1 on jdk7 |  Major | test | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-5070](https://issues.apache.org/jira/browse/MAPREDUCE-5070) | TestClusterStatus.testClusterMetrics fails on JDK7 |  Major | test | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-5047](https://issues.apache.org/jira/browse/MAPREDUCE-5047) | keep.failed.task.files=true causes job failure on secure clusters |  Major | task, tasktracker | Sandy Ryza | Sandy Ryza |
| [MAPREDUCE-5046](https://issues.apache.org/jira/browse/MAPREDUCE-5046) | backport MAPREDUCE-1423 to mapred.lib.CombineFileInputFormat |  Major | client | Sangjin Lee |  |
| [MAPREDUCE-4366](https://issues.apache.org/jira/browse/MAPREDUCE-4366) | mapred metrics shows negative count of waiting maps and reduces |  Major | jobtracker | Thomas Graves | Sandy Ryza |
| [MAPREDUCE-2817](https://issues.apache.org/jira/browse/MAPREDUCE-2817) | MiniRMCluster hardcodes 'mapred.local.dir' configuration to 'build/test/mapred/local' |  Minor | test | Alejandro Abdelnur | Robert Kanter |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-9624](https://issues.apache.org/jira/browse/HADOOP-9624) | TestFSMainOperationsLocalFileSystem failed when the Hadoop test root path has "X" in its name |  Minor | test | Xi Fang | Xi Fang |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-9573](https://issues.apache.org/jira/browse/HADOOP-9573) | Fix test-patch script to work with the enhanced PreCommit-Admin script. |  Major | build | Giridharan Kesavan | Giridharan Kesavan |
| [MAPREDUCE-4049](https://issues.apache.org/jira/browse/MAPREDUCE-4049) | plugin for generic shuffle service |  Major | performance, task, tasktracker | Avner BenHanoch | Avner BenHanoch |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


