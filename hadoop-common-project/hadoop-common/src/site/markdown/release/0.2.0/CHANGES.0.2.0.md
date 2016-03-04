
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

## Release 0.2.0 - 2006-05-05

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-191](https://issues.apache.org/jira/browse/HADOOP-191) | add hadoopStreaming to src/contrib |  Major | . | Michel Tourn | Doug Cutting |
| [HADOOP-189](https://issues.apache.org/jira/browse/HADOOP-189) | Add job jar lib, classes, etc. to CLASSPATH when in standalone mode |  Major | . | stack | Doug Cutting |
| [HADOOP-148](https://issues.apache.org/jira/browse/HADOOP-148) | add a failure count to task trackers |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-132](https://issues.apache.org/jira/browse/HADOOP-132) | An API for reporting performance metrics |  Major | . | David Bowen |  |
| [HADOOP-65](https://issues.apache.org/jira/browse/HADOOP-65) | add a record I/O framework to hadoop |  Minor | io, ipc | Sameer Paranjpye |  |
| [HADOOP-51](https://issues.apache.org/jira/browse/HADOOP-51) | per-file replication counts |  Major | . | Doug Cutting | Konstantin Shvachko |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-198](https://issues.apache.org/jira/browse/HADOOP-198) | adding owen's examples to exampledriver |  Minor | . | Mahadev konar | Mahadev konar |
| [HADOOP-178](https://issues.apache.org/jira/browse/HADOOP-178) | piggyback block work requests to heartbeats and move block replication/deletion startup delay from datanodes to namenode |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-177](https://issues.apache.org/jira/browse/HADOOP-177) | improvement to browse through the map/reduce tasks |  Minor | . | Mahadev konar | Mahadev konar |
| [HADOOP-173](https://issues.apache.org/jira/browse/HADOOP-173) | optimize allocation of tasks w/ local data |  Major | . | Doug Cutting | Doug Cutting |
| [HADOOP-170](https://issues.apache.org/jira/browse/HADOOP-170) | setReplication and related bug fixes |  Major | fs | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-167](https://issues.apache.org/jira/browse/HADOOP-167) | reducing the number of Configuration & JobConf objects created |  Major | conf | Owen O'Malley | Owen O'Malley |
| [HADOOP-166](https://issues.apache.org/jira/browse/HADOOP-166) | IPC is unable to invoke methods that use interfaces as parameter |  Minor | ipc | Stefan Groschupf | Doug Cutting |
| [HADOOP-150](https://issues.apache.org/jira/browse/HADOOP-150) | tip and task names should reflect the job name |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-144](https://issues.apache.org/jira/browse/HADOOP-144) | the dfs client id isn't relatable to the map/reduce task ids |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-142](https://issues.apache.org/jira/browse/HADOOP-142) | failed tasks should be rescheduled on different hosts after other jobs |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-138](https://issues.apache.org/jira/browse/HADOOP-138) | stop all tasks |  Trivial | . | Stefan Groschupf | Doug Cutting |
| [HADOOP-131](https://issues.apache.org/jira/browse/HADOOP-131) | Separate start/stop-dfs.sh and start/stop-mapred.sh scripts |  Minor | . | Chris A. Mattmann | Doug Cutting |
| [HADOOP-129](https://issues.apache.org/jira/browse/HADOOP-129) | FileSystem should not name files with java.io.File |  Major | fs | Doug Cutting | Doug Cutting |
| [HADOOP-116](https://issues.apache.org/jira/browse/HADOOP-116) | cleaning up /tmp/hadoop/mapred/system |  Major | . | raghavendra prabhu | Doug Cutting |
| [HADOOP-114](https://issues.apache.org/jira/browse/HADOOP-114) | Non-informative error message |  Trivial | . | Rod Taylor | Doug Cutting |
| [HADOOP-96](https://issues.apache.org/jira/browse/HADOOP-96) | name server should log decisions that affect data: block creation, removal, replication |  Critical | . | Yoram Arnon | Hairong Kuang |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-192](https://issues.apache.org/jira/browse/HADOOP-192) | Trivial JRE 1.5 versus 1.4 bug |  Blocker | . | David Bowen |  |
| [HADOOP-190](https://issues.apache.org/jira/browse/HADOOP-190) | Job fails though task succeeded if we fail to exit |  Major | . | stack |  |
| [HADOOP-188](https://issues.apache.org/jira/browse/HADOOP-188) | more unprotected RPC calls in JobClient.runJob allow loss of job due to timeout |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-186](https://issues.apache.org/jira/browse/HADOOP-186) | communication problems in the task tracker cause long latency |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-185](https://issues.apache.org/jira/browse/HADOOP-185) | tasks are lost during pollForNewTask |  Critical | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-183](https://issues.apache.org/jira/browse/HADOOP-183) | adjust file replication factor when loading image and edits according to replication.min and replication.max |  Minor | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-182](https://issues.apache.org/jira/browse/HADOOP-182) | lost task trackers should not update status of completed jobs |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-174](https://issues.apache.org/jira/browse/HADOOP-174) | jobclient kills job for one timeout |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-172](https://issues.apache.org/jira/browse/HADOOP-172) | rpc doesn't handle returning null for a String[] |  Blocker | ipc | Owen O'Malley | Owen O'Malley |
| [HADOOP-169](https://issues.apache.org/jira/browse/HADOOP-169) | a single failure from locateMapOutputs kills the entire job |  Critical | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-168](https://issues.apache.org/jira/browse/HADOOP-168) | JobSubmissionProtocol and InterTrackerProtocol don't include "throws IOException" on all methods |  Minor | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-162](https://issues.apache.org/jira/browse/HADOOP-162) | concurrent modification exception in FSNamesystem.Lease.releaseLocks |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-160](https://issues.apache.org/jira/browse/HADOOP-160) | sleeping with locks held |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-157](https://issues.apache.org/jira/browse/HADOOP-157) | job fails because pendingCreates is not cleaned up after a task fails |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-151](https://issues.apache.org/jira/browse/HADOOP-151) | RPC code has socket leak? |  Major | ipc | p sutter | Doug Cutting |
| [HADOOP-143](https://issues.apache.org/jira/browse/HADOOP-143) | exception call stacks are word wrapped in webapp |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-139](https://issues.apache.org/jira/browse/HADOOP-139) | Deadlock in LocalFileSystem lock/release |  Major | fs | Igor Bolotin | Doug Cutting |
| [HADOOP-137](https://issues.apache.org/jira/browse/HADOOP-137) | Different TaskTrackers may get the same task tracker id, thus cause many problems. |  Critical | . | Runping Qi | Owen O'Malley |
| [HADOOP-134](https://issues.apache.org/jira/browse/HADOOP-134) | JobTracker trapped in a loop if it fails to localize a task |  Major | . | Runping Qi | Owen O'Malley |
| [HADOOP-133](https://issues.apache.org/jira/browse/HADOOP-133) | the TaskTracker.Child.ping thread calls exit |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-128](https://issues.apache.org/jira/browse/HADOOP-128) | Failure to replicate dfs block kills client |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-126](https://issues.apache.org/jira/browse/HADOOP-126) | "hadoop dfs -cp" does not copy crc files |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-125](https://issues.apache.org/jira/browse/HADOOP-125) | LocalFileSystem.makeAbsolute bug on Windows |  Minor | fs | p sutter | Doug Cutting |
| [HADOOP-118](https://issues.apache.org/jira/browse/HADOOP-118) | Namenode does not always clean up pendingCreates |  Critical | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-117](https://issues.apache.org/jira/browse/HADOOP-117) | mapred temporary files not deleted |  Blocker | . | raghavendra prabhu | Doug Cutting |
| [HADOOP-92](https://issues.apache.org/jira/browse/HADOOP-92) | Error Reporting/logging in MapReduce |  Minor | . | Mahadev konar | Mahadev konar |
| [HADOOP-69](https://issues.apache.org/jira/browse/HADOOP-69) | Unchecked lookup value causes NPE in FSNamesystemgetDatanodeHints |  Major | . | Bryan Pendleton |  |
| [HADOOP-68](https://issues.apache.org/jira/browse/HADOOP-68) | "Cannot abandon block during write to \<file\>" and "Cannot obtain additional block for file \<file\>" errors during dfs write test |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-63](https://issues.apache.org/jira/browse/HADOOP-63) | problem with webapp when start a jobtracker |  Minor | . | Hairong Kuang | Hairong Kuang |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-194](https://issues.apache.org/jira/browse/HADOOP-194) | Distributed checkup of the file system consistency. |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-193](https://issues.apache.org/jira/browse/HADOOP-193) | DFS i/o benchmark. |  Major | fs | Konstantin Shvachko |  |
| [HADOOP-187](https://issues.apache.org/jira/browse/HADOOP-187) | simple distributed dfs random data writer & sort example applications |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-184](https://issues.apache.org/jira/browse/HADOOP-184) | hadoop nightly build and regression test on a cluster |  Minor | . | Mahadev konar | Mahadev konar |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


