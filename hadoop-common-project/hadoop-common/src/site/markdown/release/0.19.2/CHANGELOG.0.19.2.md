
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

## Release 0.19.2 - 2009-07-23

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-5332](https://issues.apache.org/jira/browse/HADOOP-5332) | Make support for file append API configurable |  Blocker | . | Nigel Daley | dhruba borthakur |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-5379](https://issues.apache.org/jira/browse/HADOOP-5379) | Throw exception instead of writing to System.err when there is a CRC error on CBZip2InputStream |  Minor | io | Rodrigo Schmidt | Rodrigo Schmidt |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-5269](https://issues.apache.org/jira/browse/HADOOP-5269) | TaskTracker.runningTasks holding FAILED\_UNCLEAN and KILLED\_UNCLEAN taskStatuses forever in some cases. |  Blocker | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-5233](https://issues.apache.org/jira/browse/HADOOP-5233) | Reducer not Succeded after 100% |  Blocker | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-5247](https://issues.apache.org/jira/browse/HADOOP-5247) | NPEs in JobTracker and JobClient when mapred.jobtracker.completeuserjobs.maximum is set to zero. |  Blocker | . | Vinod Kumar Vavilapalli | Amar Kamat |
| [HADOOP-5285](https://issues.apache.org/jira/browse/HADOOP-5285) | JobTracker hangs for long periods of time |  Blocker | . | Vinod Kumar Vavilapalli | Devaraj Das |
| [HADOOP-5241](https://issues.apache.org/jira/browse/HADOOP-5241) | Reduce tasks get stuck because of over-estimated task size (regression from 0.18) |  Blocker | . | Andy Pavlo | Sharad Agarwal |
| [HADOOP-5280](https://issues.apache.org/jira/browse/HADOOP-5280) | When expiring a lost launched task, JT doesn't remove the attempt from the taskidToTIPMap. |  Blocker | . | Vinod Kumar Vavilapalli | Devaraj Das |
| [HADOOP-5154](https://issues.apache.org/jira/browse/HADOOP-5154) | 4-way deadlock in FairShare scheduler |  Blocker | . | Vinod Kumar Vavilapalli | Matei Zaharia |
| [HADOOP-5146](https://issues.apache.org/jira/browse/HADOOP-5146) | LocalDirAllocator misses files on the local filesystem |  Blocker | . | Arun C Murthy | Devaraj Das |
| [HADOOP-5326](https://issues.apache.org/jira/browse/HADOOP-5326) | bzip2 codec (CBZip2OutputStream) creates corrupted output file for some inputs |  Major | io | Rodrigo Schmidt | Rodrigo Schmidt |
| [HADOOP-4638](https://issues.apache.org/jira/browse/HADOOP-4638) | Exception thrown in/from RecoveryManager.recover() should be caught and handled |  Blocker | . | Amar Kamat | Amar Kamat |
| [HADOOP-5384](https://issues.apache.org/jira/browse/HADOOP-5384) | DataNodeCluster should not create blocks with generationStamp == 1 |  Blocker | test | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-5376](https://issues.apache.org/jira/browse/HADOOP-5376) | JobInProgress.obtainTaskCleanupTask() throws an ArrayIndexOutOfBoundsException |  Blocker | . | Vinod Kumar Vavilapalli | Amareshwari Sriramadasu |
| [HADOOP-5421](https://issues.apache.org/jira/browse/HADOOP-5421) | HADOOP-4638 has broken 0.19 compilation |  Blocker | . | Amar Kamat | Devaraj Das |
| [HADOOP-5392](https://issues.apache.org/jira/browse/HADOOP-5392) | JobTracker crashes during recovery if job files are garbled |  Blocker | . | Amar Kamat | Amar Kamat |
| [HADOOP-5333](https://issues.apache.org/jira/browse/HADOOP-5333) | The libhdfs append API is not coded correctly |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-3998](https://issues.apache.org/jira/browse/HADOOP-3998) | Got an exception from ClientFinalizer when the JT is terminated |  Blocker | . | Amar Kamat | dhruba borthakur |
| [HADOOP-5440](https://issues.apache.org/jira/browse/HADOOP-5440) | Successful taskid are not removed from TaskMemoryManager |  Blocker | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-5446](https://issues.apache.org/jira/browse/HADOOP-5446) | TaskTracker metrics are disabled |  Major | metrics | Chris Douglas | Chris Douglas |
| [HADOOP-5449](https://issues.apache.org/jira/browse/HADOOP-5449) | Verify if JobHistory.HistoryCleaner works as expected |  Blocker | . | Amar Kamat | Amareshwari Sriramadasu |
| [HADOOP-5465](https://issues.apache.org/jira/browse/HADOOP-5465) | Blocks remain under-replicated |  Blocker | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-5479](https://issues.apache.org/jira/browse/HADOOP-5479) | NameNode should not send empty block replication request to DataNode |  Critical | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-5522](https://issues.apache.org/jira/browse/HADOOP-5522) | Document job setup/cleaup tasks and task cleanup tasks in mapred tutorial |  Blocker | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-5549](https://issues.apache.org/jira/browse/HADOOP-5549) | ReplicationMonitor should schedule both replication and deletion work in one iteration |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-5554](https://issues.apache.org/jira/browse/HADOOP-5554) | DataNodeCluster should create blocks with the same generation stamp as the blocks created in CreateEditsLog |  Major | test | Hairong Kuang | Hairong Kuang |
| [HADOOP-5557](https://issues.apache.org/jira/browse/HADOOP-5557) | Two minor problems in TestOverReplicatedBlocks |  Minor | test | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-5231](https://issues.apache.org/jira/browse/HADOOP-5231) | Negative number of maps in cluster summary |  Major | . | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-4719](https://issues.apache.org/jira/browse/HADOOP-4719) | The ls shell command documentation is out-dated |  Major | documentation | Tsz Wo Nicholas Sze | Ravi Phulari |
| [HADOOP-5374](https://issues.apache.org/jira/browse/HADOOP-5374) | NPE in JobTracker.getTasksToSave() method |  Major | . | Vinod Kumar Vavilapalli | Amareshwari Sriramadasu |
| [HADOOP-4780](https://issues.apache.org/jira/browse/HADOOP-4780) | Task Tracker  burns a lot of cpu in calling getLocalCache |  Major | . | Runping Qi | He Yongqiang |
| [HADOOP-5551](https://issues.apache.org/jira/browse/HADOOP-5551) | Namenode permits directory destruction on overwrite |  Critical | . | Brian Bockelman | Brian Bockelman |
| [HADOOP-5644](https://issues.apache.org/jira/browse/HADOOP-5644) | Namnode is stuck in safe mode |  Major | . | Suresh Srinivas | Suresh Srinivas |
| [HADOOP-5671](https://issues.apache.org/jira/browse/HADOOP-5671) | DistCp.sameFile(..) should return true if src fs does not support checksum |  Major | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-5213](https://issues.apache.org/jira/browse/HADOOP-5213) | BZip2CompressionOutputStream NullPointerException |  Blocker | io | Zheng Shao | Zheng Shao |
| [HADOOP-5579](https://issues.apache.org/jira/browse/HADOOP-5579) | libhdfs does not set errno correctly |  Major | . | Brian Bockelman | Brian Bockelman |
| [HADOOP-5728](https://issues.apache.org/jira/browse/HADOOP-5728) | FSEditLog.printStatistics may cause IndexOutOfBoundsException |  Major | . | Wang Xu | Wang Xu |
| [HADOOP-5816](https://issues.apache.org/jira/browse/HADOOP-5816) | ArrayIndexOutOfBoundsException when using KeyFieldBasedComparator |  Minor | . | Min Zhou | He Yongqiang |
| [HADOOP-5951](https://issues.apache.org/jira/browse/HADOOP-5951) | StorageInfo needs Apache license header. |  Major | . | Suresh Srinivas | Suresh Srinivas |
| [HADOOP-6017](https://issues.apache.org/jira/browse/HADOOP-6017) | NameNode and SecondaryNameNode fail to restart because of abnormal filenames. |  Blocker | . | Raghu Angadi | Tsz Wo Nicholas Sze |


