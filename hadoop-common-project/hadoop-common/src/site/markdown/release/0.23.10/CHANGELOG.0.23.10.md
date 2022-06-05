
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

## Release 0.23.10 - 2013-12-09

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [YARN-707](https://issues.apache.org/jira/browse/YARN-707) | Add user info in the YARN ClientToken |  Blocker | . | Bikas Saha | Jason Lowe |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-9686](https://issues.apache.org/jira/browse/HADOOP-9686) | Easy access to final parameters in Configuration |  Major | conf | Jason Lowe | Jason Lowe |
| [HADOOP-8704](https://issues.apache.org/jira/browse/HADOOP-8704) | add request logging to jetty/httpserver |  Major | . | Thomas Graves | Jonathan Eagles |
| [HDFS-5010](https://issues.apache.org/jira/browse/HDFS-5010) | Reduce the frequency of getCurrentUser() calls from namenode |  Major | namenode, performance | Kihwal Lee | Kihwal Lee |
| [MAPREDUCE-5640](https://issues.apache.org/jira/browse/MAPREDUCE-5640) | Rename TestLineRecordReader in jobclient module |  Trivial | test | Jason Lowe | Jason Lowe |
| [MAPREDUCE-1981](https://issues.apache.org/jira/browse/MAPREDUCE-1981) | Improve getSplits performance by using listLocatedStatus |  Major | job submission | Hairong Kuang | Hairong Kuang |
| [YARN-985](https://issues.apache.org/jira/browse/YARN-985) | Nodemanager should log where a resource was localized |  Major | nodemanager | Ravi Prakash | Ravi Prakash |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-10091](https://issues.apache.org/jira/browse/HADOOP-10091) | Job with a har archive as input fails on 0.23 |  Blocker | fs | Jason Lowe | Jason Lowe |
| [HADOOP-9757](https://issues.apache.org/jira/browse/HADOOP-9757) | Har metadata cache can grow without limit |  Major | fs | Jason Lowe | Cristina L. Abad |
| [HADOOP-9476](https://issues.apache.org/jira/browse/HADOOP-9476) | Some test cases in TestUserGroupInformation fail if ran after testSetLoginUser. |  Major | security, test | Kihwal Lee | Robert Parker |
| [HDFS-5558](https://issues.apache.org/jira/browse/HDFS-5558) | LeaseManager monitor thread can crash if the last block is complete but another block is not. |  Major | . | Kihwal Lee | Kihwal Lee |
| [HDFS-5557](https://issues.apache.org/jira/browse/HDFS-5557) | Write pipeline recovery for the last packet in the block may cause rejection of valid replicas |  Critical | . | Kihwal Lee | Kihwal Lee |
| [HDFS-5526](https://issues.apache.org/jira/browse/HDFS-5526) | Datanode cannot roll back to previous layout version |  Blocker | datanode | Tsz Wo Nicholas Sze | Kihwal Lee |
| [HDFS-5438](https://issues.apache.org/jira/browse/HDFS-5438) | Flaws in block report processing can cause data loss |  Critical | namenode | Kihwal Lee | Kihwal Lee |
| [HDFS-5346](https://issues.apache.org/jira/browse/HDFS-5346) | Avoid unnecessary call to getNumLiveDataNodes() for each block during IBR processing |  Major | namenode, performance | Kihwal Lee | Ravi Prakash |
| [HDFS-5047](https://issues.apache.org/jira/browse/HDFS-5047) | Supress logging of full stack trace of quota and lease exceptions |  Major | namenode | Kihwal Lee | Robert Parker |
| [HDFS-5028](https://issues.apache.org/jira/browse/HDFS-5028) | LeaseRenewer throw java.util.ConcurrentModificationException when timeout |  Major | . | zhaoyunjiong | zhaoyunjiong |
| [HDFS-4998](https://issues.apache.org/jira/browse/HDFS-4998) | TestUnderReplicatedBlocks fails intermittently |  Major | test | Kihwal Lee | Kihwal Lee |
| [HDFS-4995](https://issues.apache.org/jira/browse/HDFS-4995) | Make getContentSummary() less expensive |  Major | namenode | Kihwal Lee | Kihwal Lee |
| [HDFS-4994](https://issues.apache.org/jira/browse/HDFS-4994) | Audit log getContentSummary() calls |  Minor | namenode | Kihwal Lee | Robert Parker |
| [HDFS-4993](https://issues.apache.org/jira/browse/HDFS-4993) | fsck can fail if a file is renamed or deleted |  Major | . | Kihwal Lee | Robert Parker |
| [HDFS-4984](https://issues.apache.org/jira/browse/HDFS-4984) | Incorrect Quota counting in INodeFile |  Major | namenode | Kihwal Lee | Jing Zhao |
| [HDFS-4972](https://issues.apache.org/jira/browse/HDFS-4972) | [branch-0.23] permission check and operation are done in a separate lock for getBlockLocations() |  Major | namenode | Kihwal Lee | Kihwal Lee |
| [HDFS-4943](https://issues.apache.org/jira/browse/HDFS-4943) | WebHdfsFileSystem does not work when original file path has encoded chars |  Minor | webhdfs | Jerry He | Jerry He |
| [HDFS-4482](https://issues.apache.org/jira/browse/HDFS-4482) | ReplicationMonitor thread can exit with NPE due to the race between delete and replication of same file. |  Blocker | namenode | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-4329](https://issues.apache.org/jira/browse/HDFS-4329) | DFSShell issues with directories with spaces in name |  Major | hdfs-client | Andy Isaacson | Cristina L. Abad |
| [HDFS-3970](https://issues.apache.org/jira/browse/HDFS-3970) | BlockPoolSliceStorage#doRollback(..) should use BlockPoolSliceStorage instead of DataStorage to read prev version file. |  Major | datanode | Vinayakumar B | Vinayakumar B |
| [HDFS-3020](https://issues.apache.org/jira/browse/HDFS-3020) | Auto-logSync based on edit log buffer size broken |  Critical | namenode | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-5587](https://issues.apache.org/jira/browse/MAPREDUCE-5587) | TestTextOutputFormat fails on JDK7 |  Major | . | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-5586](https://issues.apache.org/jira/browse/MAPREDUCE-5586) | TestCopyMapper#testCopyFailOnBlockSizeDifference fails when run from hadoop-tools/hadoop-distcp directory |  Major | . | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-5585](https://issues.apache.org/jira/browse/MAPREDUCE-5585) | TestCopyCommitter#testNoCommitAction Fails on JDK7 |  Major | . | Jonathan Eagles | Jonathan Eagles |
| [MAPREDUCE-5569](https://issues.apache.org/jira/browse/MAPREDUCE-5569) | FloatSplitter is not generating correct splits |  Major | . | Nathan Roberts | Nathan Roberts |
| [MAPREDUCE-5561](https://issues.apache.org/jira/browse/MAPREDUCE-5561) | org.apache.hadoop.mapreduce.v2.app.job.impl.TestJobImpl testcase failing on trunk |  Critical | . | Cindy Li | Karthik Kambatla |
| [MAPREDUCE-5543](https://issues.apache.org/jira/browse/MAPREDUCE-5543) | In-memory map outputs can be leaked after shuffle completes in 0.23 |  Blocker | mrv2 | Jason Lowe | Jason Lowe |
| [MAPREDUCE-5513](https://issues.apache.org/jira/browse/MAPREDUCE-5513) | ConcurrentModificationException in JobControl |  Major | . | Jason Lowe | Robert Parker |
| [MAPREDUCE-5504](https://issues.apache.org/jira/browse/MAPREDUCE-5504) | mapred queue -info inconsistent with types |  Major | client | Thomas Graves | Kousuke Saruta |
| [MAPREDUCE-5475](https://issues.apache.org/jira/browse/MAPREDUCE-5475) | MRClientService does not verify ACLs properly |  Blocker | mr-am, mrv2 | Jason Lowe | Jason Lowe |
| [MAPREDUCE-5468](https://issues.apache.org/jira/browse/MAPREDUCE-5468) | AM recovery does not work for map only jobs |  Blocker | . | Yesha Vora | Vinod Kumar Vavilapalli |
| [MAPREDUCE-5440](https://issues.apache.org/jira/browse/MAPREDUCE-5440) | TestCopyCommitter Fails on JDK7 |  Major | mrv2 | Robert Parker | Robert Parker |
| [MAPREDUCE-5425](https://issues.apache.org/jira/browse/MAPREDUCE-5425) | Junit in TestJobHistoryServer failing in jdk 7 |  Major | jobhistoryserver | Ashwin Shankar | Robert Parker |
| [MAPREDUCE-5419](https://issues.apache.org/jira/browse/MAPREDUCE-5419) | TestSlive is getting FileNotFound Exception |  Major | mrv2 | Robert Parker | Robert Parker |
| [MAPREDUCE-5414](https://issues.apache.org/jira/browse/MAPREDUCE-5414) | TestTaskAttempt fails jdk7 with NullPointerException |  Major | test | Nemon Lou | Nemon Lou |
| [MAPREDUCE-5380](https://issues.apache.org/jira/browse/MAPREDUCE-5380) | Invalid mapred command should return non-zero exit code |  Major | . | Stephen Chu | Stephen Chu |
| [MAPREDUCE-5373](https://issues.apache.org/jira/browse/MAPREDUCE-5373) | TestFetchFailure.testFetchFailureMultipleReduces could fail intermittently |  Major | . | Chuan Liu | Jonathan Eagles |
| [MAPREDUCE-5317](https://issues.apache.org/jira/browse/MAPREDUCE-5317) | Stale files left behind for failed jobs |  Major | mrv2 | Ravi Prakash | Ravi Prakash |
| [MAPREDUCE-5251](https://issues.apache.org/jira/browse/MAPREDUCE-5251) | Reducer should not implicate map attempt if it has insufficient space to fetch map output |  Major | mrv2 | Jason Lowe | Ashwin Shankar |
| [MAPREDUCE-5001](https://issues.apache.org/jira/browse/MAPREDUCE-5001) | LocalJobRunner has race condition resulting in job failures |  Major | . | Brock Noland | Sandy Ryza |
| [MAPREDUCE-4716](https://issues.apache.org/jira/browse/MAPREDUCE-4716) | TestHsWebServicesJobsQuery.testJobsQueryStateInvalid fails with jdk7 |  Major | jobhistoryserver | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4579](https://issues.apache.org/jira/browse/MAPREDUCE-4579) | TestTaskAttempt fails jdk7 |  Major | . | Thomas Graves | Thomas Graves |
| [MAPREDUCE-4571](https://issues.apache.org/jira/browse/MAPREDUCE-4571) | TestHsWebServicesJobs fails on jdk7 |  Major | webapps | Thomas Graves | Thomas Graves |
| [MAPREDUCE-3193](https://issues.apache.org/jira/browse/MAPREDUCE-3193) | FileInputFormat doesn't read files recursively in the input path dir |  Major | mrv1, mrv2 | Ramgopal N | Devaraj K |
| [YARN-1419](https://issues.apache.org/jira/browse/YARN-1419) | TestFifoScheduler.testAppAttemptMetrics fails intermittently under jdk7 |  Minor | scheduler | Jonathan Eagles | Jonathan Eagles |
| [YARN-1386](https://issues.apache.org/jira/browse/YARN-1386) | NodeManager mistakenly loses resources and relocalizes them |  Critical | nodemanager | Jason Lowe | Jason Lowe |
| [YARN-1243](https://issues.apache.org/jira/browse/YARN-1243) | ResourceManager: Error in handling event type NODE\_UPDATE to the scheduler - NPE at SchedulerApp.java:411 |  Major | capacityscheduler | Sanjay Upadhyay | Jason Lowe |
| [YARN-1176](https://issues.apache.org/jira/browse/YARN-1176) | RM web services ClusterMetricsInfo total nodes doesn't include unhealthy nodes |  Critical | resourcemanager | Thomas Graves | Jonathan Eagles |
| [YARN-1101](https://issues.apache.org/jira/browse/YARN-1101) | Active nodes can be decremented below 0 |  Major | resourcemanager | Robert Parker | Robert Parker |
| [YARN-1036](https://issues.apache.org/jira/browse/YARN-1036) | Distributed Cache gives inconsistent result if cache files get deleted from task tracker |  Major | nodemanager | Ravi Prakash | Ravi Prakash |
| [YARN-1031](https://issues.apache.org/jira/browse/YARN-1031) | JQuery UI components reference external css in branch-23 |  Major | . | Jonathan Eagles | Jonathan Eagles |
| [YARN-949](https://issues.apache.org/jira/browse/YARN-949) | Failed log aggregation can leave a file open. |  Major | nodemanager | Kihwal Lee | Kihwal Lee |
| [YARN-337](https://issues.apache.org/jira/browse/YARN-337) | RM handles killed application tracking URL poorly |  Major | resourcemanager | Jason Lowe | Jason Lowe |
| [YARN-155](https://issues.apache.org/jira/browse/YARN-155) | TestAppManager intermittently fails with jdk7 |  Major | . | Thomas Graves | Thomas Graves |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [MAPREDUCE-5610](https://issues.apache.org/jira/browse/MAPREDUCE-5610) | TestSleepJob fails in jdk7 |  Major | . | Jonathan Eagles | Jonathan Eagles |
| [YARN-1119](https://issues.apache.org/jira/browse/YARN-1119) | Add ClusterMetrics checks to tho TestRMNodeTransitions tests |  Major | resourcemanager | Robert Parker | Mit Desai |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-9956](https://issues.apache.org/jira/browse/HADOOP-9956) | RPC listener inefficiently assigns connections to readers |  Major | ipc | Daryn Sharp | Daryn Sharp |
| [HADOOP-9955](https://issues.apache.org/jira/browse/HADOOP-9955) | RPC idle connection closing is extremely inefficient |  Major | ipc | Daryn Sharp | Daryn Sharp |
| [HADOOP-9748](https://issues.apache.org/jira/browse/HADOOP-9748) | Reduce blocking on UGI.ensureInitialized |  Critical | security | Daryn Sharp | Daryn Sharp |
| [HDFS-5239](https://issues.apache.org/jira/browse/HDFS-5239) | Allow FSNamesystem lock fairness to be configurable |  Major | namenode | Daryn Sharp | Daryn Sharp |
| [YARN-573](https://issues.apache.org/jira/browse/YARN-573) | Shared data structures in Public Localizer and Private Localizer are not Thread safe. |  Critical | . | Omkar Vinit Joshi | Omkar Vinit Joshi |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


