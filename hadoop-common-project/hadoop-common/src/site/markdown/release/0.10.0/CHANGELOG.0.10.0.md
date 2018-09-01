
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

## Release 0.10.0 - 2007-01-05



### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-454](https://issues.apache.org/jira/browse/HADOOP-454) | hadoop du optionally behave like unix's du -s |  Trivial | . | Marco Nicosia | Hairong Kuang |
| [HADOOP-574](https://issues.apache.org/jira/browse/HADOOP-574) | want FileSystem implementation for Amazon S3 |  Major | fs | Doug Cutting | Tom White |
| [HADOOP-811](https://issues.apache.org/jira/browse/HADOOP-811) | Patch to support multi-threaded MapRunnable |  Major | . | Alejandro Abdelnur | Doug Cutting |
| [HADOOP-681](https://issues.apache.org/jira/browse/HADOOP-681) | Adminstrative hook to pull live nodes out of a HDFS cluster |  Major | . | dhruba borthakur | dhruba borthakur |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-763](https://issues.apache.org/jira/browse/HADOOP-763) | NameNode benchmark using mapred is insufficient |  Minor | test | Nigel Daley | Nigel Daley |
| [HADOOP-621](https://issues.apache.org/jira/browse/HADOOP-621) | When a dfs -cat command is killed by the user, the correspondig hadoop process does not get aborted |  Minor | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-676](https://issues.apache.org/jira/browse/HADOOP-676) | JobClient should print user friendly messages for standard errors |  Major | . | Owen O'Malley | Sanjay Dahiya |
| [HADOOP-756](https://issues.apache.org/jira/browse/HADOOP-756) | new dfsadmin command to wait until safe mode is exited |  Minor | . | Owen O'Malley | dhruba borthakur |
| [HADOOP-331](https://issues.apache.org/jira/browse/HADOOP-331) | map outputs should be written to a single output file with an index |  Major | . | eric baldeschwieler | Devaraj Das |
| [HADOOP-796](https://issues.apache.org/jira/browse/HADOOP-796) | Node failing tasks and failed tasks should be more easily accessible through jobtracker history. |  Minor | . | Sanjay Dahiya | Sanjay Dahiya |
| [HADOOP-806](https://issues.apache.org/jira/browse/HADOOP-806) | NameNode WebUI : Include link to each of datanodes |  Minor | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-618](https://issues.apache.org/jira/browse/HADOOP-618) | JobProfile and JobSubmissionProtocol should be public |  Major | . | Runping Qi | Arun C Murthy |
| [HADOOP-571](https://issues.apache.org/jira/browse/HADOOP-571) | Path should use URI syntax |  Major | fs | Doug Cutting | Doug Cutting |
| [HADOOP-720](https://issues.apache.org/jira/browse/HADOOP-720) | Write a white paper on Hadoop File System Architecture, Design and Features |  Major | documentation | dhruba borthakur | dhruba borthakur |
| [HADOOP-717](https://issues.apache.org/jira/browse/HADOOP-717) | When there are few reducers, sorting should be done by mappers |  Major | . | arkady borkovsky | Owen O'Malley |
| [HADOOP-451](https://issues.apache.org/jira/browse/HADOOP-451) | Add a Split interface |  Major | . | Doug Cutting | Owen O'Malley |
| [HADOOP-783](https://issues.apache.org/jira/browse/HADOOP-783) | Hadoop dfs -put and -get accept '-' to indicate stdin/stdout |  Minor | . | Marco Nicosia | Wendy Chien |
| [HADOOP-837](https://issues.apache.org/jira/browse/HADOOP-837) | RunJar should unpack jar files into hadoop.tmp.dir |  Major | util | Hairong Kuang | Hairong Kuang |
| [HADOOP-850](https://issues.apache.org/jira/browse/HADOOP-850) | Add Writable implementations for variable-length integer types. |  Minor | io | Andrzej Bialecki | Andrzej Bialecki |
| [HADOOP-525](https://issues.apache.org/jira/browse/HADOOP-525) | Need raw comparators for hadoop record types |  Major | record | Sameer Paranjpye | Milind Bhandarkar |
| [HADOOP-804](https://issues.apache.org/jira/browse/HADOOP-804) | Cut down on the "mumbling" in the Task process' stdout/stderr |  Major | . | Owen O'Malley | Sanjay Dahiya |
| [HADOOP-853](https://issues.apache.org/jira/browse/HADOOP-853) | Move site directories to docs directories |  Minor | documentation | Nigel Daley | Doug Cutting |
| [HADOOP-371](https://issues.apache.org/jira/browse/HADOOP-371) | ant tar should package contrib jars |  Major | build | Michel Tourn | Nigel Daley |
| [HADOOP-470](https://issues.apache.org/jira/browse/HADOOP-470) | Some improvements in the DFS content browsing UI |  Minor | . | Devaraj Das | Hairong Kuang |
| [HADOOP-619](https://issues.apache.org/jira/browse/HADOOP-619) | Unify Map-Reduce and Streaming to take the same globbed input specification |  Major | . | eric baldeschwieler | Sanjay Dahiya |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-777](https://issues.apache.org/jira/browse/HADOOP-777) | the tasktracker hostname is not fully qualified |  Minor | . | Mahadev konar | Mahadev konar |
| [HADOOP-752](https://issues.apache.org/jira/browse/HADOOP-752) | Possible locking issues in HDFS Namenode |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-629](https://issues.apache.org/jira/browse/HADOOP-629) | none of the rpc servers check the protcol name for validity |  Major | ipc | Owen O'Malley | Owen O'Malley |
| [HADOOP-774](https://issues.apache.org/jira/browse/HADOOP-774) | Datanodes fails to heartbeat when a directory with a large number of blocks is deleted |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-738](https://issues.apache.org/jira/browse/HADOOP-738) | dfs get or copyToLocal should not copy crc file |  Major | . | Milind Bhandarkar | Milind Bhandarkar |
| [HADOOP-770](https://issues.apache.org/jira/browse/HADOOP-770) | When JobTracker gets restarted, Job Tracker History doesn't show the jobs that were running. (incomplete jobs) |  Minor | . | Koji Noguchi | Sanjay Dahiya |
| [HADOOP-546](https://issues.apache.org/jira/browse/HADOOP-546) | Task tracker doesnt generate job.xml in jobcache for some tasks ( possibly for only rescheduled tasks) |  Critical | . | Sanjay Dahiya | Arun C Murthy |
| [HADOOP-737](https://issues.apache.org/jira/browse/HADOOP-737) | TaskTracker's job cleanup loop should check for finished job before deleting local directories |  Critical | . | Sanjay Dahiya | Arun C Murthy |
| [HADOOP-818](https://issues.apache.org/jira/browse/HADOOP-818) | ant clean test-contrib doesn't work |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-786](https://issues.apache.org/jira/browse/HADOOP-786) | PhasedFileSystem should use debug level log for ignored exception. |  Trivial | . | Sanjay Dahiya | Sanjay Dahiya |
| [HADOOP-764](https://issues.apache.org/jira/browse/HADOOP-764) | The memory consumption of processReport() in the namenode can be reduced |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-802](https://issues.apache.org/jira/browse/HADOOP-802) | mapred.speculative.execution description in hadoop-defauls.xml is not complete |  Trivial | conf | Nigel Daley | Nigel Daley |
| [HADOOP-782](https://issues.apache.org/jira/browse/HADOOP-782) | TaskTracker.java:killOverflowingTasks & TaskTracker.java:markUnresponsiveTasks only put the tip in tasksToCleanup queue, they don't update the runningJobs |  Major | . | Arun C Murthy | Arun C Murthy |
| [HADOOP-792](https://issues.apache.org/jira/browse/HADOOP-792) | Invalid dfs -mv can trash your entire dfs |  Major | . | Chris Schneider |  |
| [HADOOP-673](https://issues.apache.org/jira/browse/HADOOP-673) | the task execution environment should have a current working directory that is task specific |  Major | . | Owen O'Malley | Mahadev konar |
| [HADOOP-794](https://issues.apache.org/jira/browse/HADOOP-794) | JobTracker crashes with ArithmeticException |  Major | . | Nigel Daley | Owen O'Malley |
| [HADOOP-824](https://issues.apache.org/jira/browse/HADOOP-824) | DFSShell should become FSShell |  Major | . | Doug Cutting |  |
| [HADOOP-813](https://issues.apache.org/jira/browse/HADOOP-813) | map tasks lost during sort |  Major | . | Owen O'Malley | Devaraj Das |
| [HADOOP-825](https://issues.apache.org/jira/browse/HADOOP-825) | If the default file system is set using the new uri syntax, the namenode will not start |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-596](https://issues.apache.org/jira/browse/HADOOP-596) | TaskTracker taskstatus's phase doesnt get updated on phase transition causing wrong values displayed in WI |  Major | . | Sanjay Dahiya | Sanjay Dahiya |
| [HADOOP-829](https://issues.apache.org/jira/browse/HADOOP-829) | Separate the datanode contents that is written to the fsimage vs the contents used in over-the-wire communication |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-823](https://issues.apache.org/jira/browse/HADOOP-823) | DataNode will not start up if any directories from dfs.data.dir are missing |  Major | . | Bryan Pendleton | Sameer Paranjpye |
| [HADOOP-814](https://issues.apache.org/jira/browse/HADOOP-814) | Increase dfs scalability by optimizing locking on namenode. |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-835](https://issues.apache.org/jira/browse/HADOOP-835) | conf not set for the default Codec when initializing a Reader for a record-compressed sequence file |  Major | io | Hairong Kuang | Hairong Kuang |
| [HADOOP-836](https://issues.apache.org/jira/browse/HADOOP-836) | unit tests fail on windows (/C:/cygwin/... is invalid) |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-841](https://issues.apache.org/jira/browse/HADOOP-841) | native hadoop libraries don't build properly with 64-bit OS and a 32-bit jvm |  Major | build | Arun C Murthy | Arun C Murthy |
| [HADOOP-838](https://issues.apache.org/jira/browse/HADOOP-838) | TaskRunner.run() doesn't pass along the 'java.library.path' to the child (task) jvm |  Major | . | Arun C Murthy | Arun C Murthy |
| [HADOOP-844](https://issues.apache.org/jira/browse/HADOOP-844) | Metrics messages are sent on a fixed-delay schedule instead of a fixed-rate schedule |  Minor | metrics | David Bowen |  |
| [HADOOP-849](https://issues.apache.org/jira/browse/HADOOP-849) | randomwriter fails with 'java.lang.OutOfMemoryError: Java heap space' in the 'reduce' task |  Major | . | Arun C Murthy | Devaraj Das |
| [HADOOP-745](https://issues.apache.org/jira/browse/HADOOP-745) | NameNode throws FileNotFoundException: Parent path does not exist on startup |  Major | . | Nigel Daley | dhruba borthakur |
| [HADOOP-628](https://issues.apache.org/jira/browse/HADOOP-628) | hadoop hdfs -cat   replaces some characters with question marks. |  Major | . | arkady borkovsky | Wendy Chien |
| [HADOOP-846](https://issues.apache.org/jira/browse/HADOOP-846) | Progress report is not sent during the intermediate sorts in the map phase |  Major | . | Devaraj Das | Devaraj Das |
| [HADOOP-840](https://issues.apache.org/jira/browse/HADOOP-840) | the task tracker is getting blocked by long deletes of local files |  Major | . | Owen O'Malley | Mahadev konar |
| [HADOOP-700](https://issues.apache.org/jira/browse/HADOOP-700) | bin/hadoop includes in classpath all jar files in HADOOP\_HOME |  Major | scripts | Nigel Daley | Doug Cutting |


