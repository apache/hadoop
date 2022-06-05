
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

## Release 0.19.1 - 2009-02-24

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-5225](https://issues.apache.org/jira/browse/HADOOP-5225) | workaround for tmp file handling on DataNodes in 0.19.1 (HADOOP-4663) |  Blocker | . | Nigel Daley | Raghu Angadi |
| [HADOOP-5224](https://issues.apache.org/jira/browse/HADOOP-5224) | Disable append |  Blocker | . | Nigel Daley |  |
| [HADOOP-4061](https://issues.apache.org/jira/browse/HADOOP-4061) | Large number of decommission freezes the Namenode |  Major | . | Koji Noguchi | Tsz Wo Nicholas Sze |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-5034](https://issues.apache.org/jira/browse/HADOOP-5034) | NameNode should send both replication and deletion requests to DataNode in one reply to a heartbeat |  Major | . | Hairong Kuang | Hairong Kuang |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-5127](https://issues.apache.org/jira/browse/HADOOP-5127) | FSDirectory should not have public methods. |  Major | . | Konstantin Shvachko | Jakob Homan |
| [HADOOP-5086](https://issues.apache.org/jira/browse/HADOOP-5086) | Trash URI semantics can be relaxed |  Minor | fs | Chris Douglas | Chris Douglas |
| [HADOOP-4739](https://issues.apache.org/jira/browse/HADOOP-4739) | Minor enhancements to some sections of the Map/Reduce tutorial |  Trivial | . | Vivek Ratan | Vivek Ratan |
| [HADOOP-3894](https://issues.apache.org/jira/browse/HADOOP-3894) | DFSClient chould log errors better, and provide better diagnostics |  Trivial | . | Steve Loughran | Steve Loughran |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-5665](https://issues.apache.org/jira/browse/HADOOP-5665) | Namenode could not be formatted because the "whoami" program could not be run. |  Major | . | Evelyn Sylvia |  |
| [HADOOP-5268](https://issues.apache.org/jira/browse/HADOOP-5268) | Using MultipleOutputFormat and setting reducers to 0 causes org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException and job to fail |  Major | . | Thibaut |  |
| [HADOOP-5193](https://issues.apache.org/jira/browse/HADOOP-5193) | SecondaryNameNode does not rollImage because of incorrect calculation of edits modification time. |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-5192](https://issues.apache.org/jira/browse/HADOOP-5192) | Block reciever should not remove a finalized block when block replication fails |  Blocker | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-5166](https://issues.apache.org/jira/browse/HADOOP-5166) | JobTracker fails to restart if recovery and ACLs are enabled |  Blocker | . | Karam Singh | Amar Kamat |
| [HADOOP-5161](https://issues.apache.org/jira/browse/HADOOP-5161) | Accepted sockets do not get placed in DataXceiverServer#childSockets |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-5156](https://issues.apache.org/jira/browse/HADOOP-5156) | TestHeartbeatHandling uses MiniDFSCluster.getNamesystem() which does not exist in branch 0.20 |  Major | test | Konstantin Shvachko | Hairong Kuang |
| [HADOOP-5134](https://issues.apache.org/jira/browse/HADOOP-5134) | FSNamesystem#commitBlockSynchronization adds under-construction block locations to blocksMap |  Blocker | . | Hairong Kuang | dhruba borthakur |
| [HADOOP-5067](https://issues.apache.org/jira/browse/HADOOP-5067) | Failed/Killed attempts column in jobdetails.jsp does not show the number of failed/killed attempts correctly |  Major | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-5009](https://issues.apache.org/jira/browse/HADOOP-5009) | DataNode#shutdown sometimes leaves data block scanner verification log unclosed |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-5008](https://issues.apache.org/jira/browse/HADOOP-5008) | TestReplication#testPendingReplicationRetry leaves an opened fd unclosed |  Major | test | Hairong Kuang | Hairong Kuang |
| [HADOOP-5002](https://issues.apache.org/jira/browse/HADOOP-5002) | 2 core tests TestFileOutputFormat and TestHarFileSystem are failing in branch 19 |  Blocker | . | Ravi Gummadi | Amareshwari Sriramadasu |
| [HADOOP-4992](https://issues.apache.org/jira/browse/HADOOP-4992) | TestCustomOutputCommitter fails on hadoop-0.19 |  Blocker | . | Amar Kamat | Amareshwari Sriramadasu |
| [HADOOP-4983](https://issues.apache.org/jira/browse/HADOOP-4983) | Job counters sometimes go down as tasks run without task failures |  Critical | . | Owen O'Malley | Amareshwari Sriramadasu |
| [HADOOP-4982](https://issues.apache.org/jira/browse/HADOOP-4982) | TestFsck does not run in Eclipse. |  Major | test | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-4967](https://issues.apache.org/jira/browse/HADOOP-4967) | Inconsistent state in JVM manager |  Major | . | Amareshwari Sriramadasu | Devaraj Das |
| [HADOOP-4966](https://issues.apache.org/jira/browse/HADOOP-4966) | Setup tasks are not removed from JobTracker's taskIdToTIPMap even after the job completes |  Major | . | Amar Kamat | Amareshwari Sriramadasu |
| [HADOOP-4965](https://issues.apache.org/jira/browse/HADOOP-4965) | DFSClient should log instead of printing into std err. |  Major | test | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-4955](https://issues.apache.org/jira/browse/HADOOP-4955) | Make DBOutputFormat us column names from setOutput(...) |  Major | . | Kevin Peterson | Kevin Peterson |
| [HADOOP-4943](https://issues.apache.org/jira/browse/HADOOP-4943) | fair share scheduler does not utilize all slots if the task trackers are configured heterogeneously |  Major | . | Zheng Shao | Zheng Shao |
| [HADOOP-4924](https://issues.apache.org/jira/browse/HADOOP-4924) | Race condition in re-init of TaskTracker |  Blocker | . | Devaraj Das | Devaraj Das |
| [HADOOP-4918](https://issues.apache.org/jira/browse/HADOOP-4918) | Fix bzip2 work with SequenceFile |  Major | io | Zheng Shao | Zheng Shao |
| [HADOOP-4906](https://issues.apache.org/jira/browse/HADOOP-4906) | TaskTracker running out of memory after running several tasks |  Blocker | . | Arun C Murthy | Sharad Agarwal |
| [HADOOP-4862](https://issues.apache.org/jira/browse/HADOOP-4862) | A spurious IOException log on DataNode is not completely removed |  Blocker | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-4847](https://issues.apache.org/jira/browse/HADOOP-4847) | OutputCommitter is loaded in the TaskTracker in localizeConfiguration |  Blocker | . | Owen O'Malley | Amareshwari Sriramadasu |
| [HADOOP-4836](https://issues.apache.org/jira/browse/HADOOP-4836) | Minor typos in documentation and comments |  Trivial | documentation | Jordà Polo | Jordà Polo |
| [HADOOP-4821](https://issues.apache.org/jira/browse/HADOOP-4821) | Usage description in the Quotas guide documentations are incorrect |  Minor | documentation | Boris Shkolnik | Boris Shkolnik |
| [HADOOP-4797](https://issues.apache.org/jira/browse/HADOOP-4797) | RPC Server can leave a lot of direct buffers |  Blocker | ipc | Raghu Angadi | Raghu Angadi |
| [HADOOP-4760](https://issues.apache.org/jira/browse/HADOOP-4760) | HDFS streams should not throw exceptions when closed twice |  Major | fs, fs/s3 | Alejandro Abdelnur | Enis Soztutar |
| [HADOOP-4759](https://issues.apache.org/jira/browse/HADOOP-4759) | HADOOP-4654 to be fixed for branches \>= 0.19 |  Major | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-4731](https://issues.apache.org/jira/browse/HADOOP-4731) | Job is not removed from the waiting jobs queue upon completion. |  Major | . | Hemanth Yamijala | Amar Kamat |
| [HADOOP-4727](https://issues.apache.org/jira/browse/HADOOP-4727) | Groups do not work for fuse-dfs out of the box on 0.19.0 |  Blocker | . | Brian Bockelman | Brian Bockelman |
| [HADOOP-4720](https://issues.apache.org/jira/browse/HADOOP-4720) | docs/api does not contain the hdfs directory after building |  Major | build | Ramya Sunil |  |
| [HADOOP-4697](https://issues.apache.org/jira/browse/HADOOP-4697) | KFS::getBlockLocations() fails with files having multiple blocks |  Major | fs | Lohit Vijayarenu | Sriram Rao |
| [HADOOP-4635](https://issues.apache.org/jira/browse/HADOOP-4635) | Memory leak ? |  Blocker | . | Marc-Olivier Fleury | Pete Wyckoff |
| [HADOOP-4632](https://issues.apache.org/jira/browse/HADOOP-4632) | TestJobHistoryVersion should not create directory in current dir. |  Major | . | Amareshwari Sriramadasu | Amar Kamat |
| [HADOOP-4616](https://issues.apache.org/jira/browse/HADOOP-4616) | assertion makes fuse-dfs exit when reading incomplete data |  Blocker | . | Marc-Olivier Fleury | Pete Wyckoff |
| [HADOOP-4508](https://issues.apache.org/jira/browse/HADOOP-4508) | FSDataOutputStream.getPos() == 0when appending to existing file and should be file length |  Major | fs | Pete Wyckoff | dhruba borthakur |
| [HADOOP-4494](https://issues.apache.org/jira/browse/HADOOP-4494) | libhdfs does not call FileSystem.append when O\_APPEND passed to hdfsOpenFile |  Major | . | Pete Wyckoff | Pete Wyckoff |
| [HADOOP-4420](https://issues.apache.org/jira/browse/HADOOP-4420) | JobTracker.killJob() doesn't check for the JobID being valid |  Minor | . | Steve Loughran | Aaron Kimball |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


