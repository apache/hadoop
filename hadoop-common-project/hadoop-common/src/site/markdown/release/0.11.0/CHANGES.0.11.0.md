
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

## Release 0.11.0 - 2007-02-02

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-967](https://issues.apache.org/jira/browse/HADOOP-967) | flip boolean to have rpc clients send a header |  Major | ipc | Owen O'Malley | Owen O'Malley |
| [HADOOP-961](https://issues.apache.org/jira/browse/HADOOP-961) | a cli tool to get the event logs from a job |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-918](https://issues.apache.org/jira/browse/HADOOP-918) | Examples of Abacus using Python plugins |  Major | . | Runping Qi | Runping Qi |
| [HADOOP-908](https://issues.apache.org/jira/browse/HADOOP-908) | Hadoop Abacus, a package for performing simple counting/aggregation |  Major | . | Runping Qi | Runping Qi |
| [HADOOP-884](https://issues.apache.org/jira/browse/HADOOP-884) | Create scripts to run Hadoop on Amazon EC2 |  Major | scripts | Tom White | Tom White |
| [HADOOP-852](https://issues.apache.org/jira/browse/HADOOP-852) | want ant task for record definitions |  Major | record | Doug Cutting | Milind Bhandarkar |
| [HADOOP-732](https://issues.apache.org/jira/browse/HADOOP-732) | SequenceFile's header should allow to store metadata in the form of key/value pairs |  Major | io | Runping Qi | Runping Qi |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-963](https://issues.apache.org/jira/browse/HADOOP-963) | improve the stack trace returned by RPC client |  Major | ipc | Owen O'Malley | Owen O'Malley |
| [HADOOP-936](https://issues.apache.org/jira/browse/HADOOP-936) | More updates to metric names to conform to HADOOP-887 |  Minor | . | Nigel Daley |  |
| [HADOOP-897](https://issues.apache.org/jira/browse/HADOOP-897) | Need a simpler way to specify arbitrary options to java compiler while building Hadoop |  Minor | build | Milind Bhandarkar | Milind Bhandarkar |
| [HADOOP-890](https://issues.apache.org/jira/browse/HADOOP-890) | Update tag and metric names to conform to HADOOP-887 |  Minor | metrics | Nigel Daley | Nigel Daley |
| [HADOOP-862](https://issues.apache.org/jira/browse/HADOOP-862) | Add handling of s3 to CopyFile tool |  Minor | util | stack |  |
| [HADOOP-842](https://issues.apache.org/jira/browse/HADOOP-842) | change the open method in ClientProtocol to take an additional argument: clientMachine |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-833](https://issues.apache.org/jira/browse/HADOOP-833) | need documentation of native build requirements |  Major | documentation | Doug Cutting | Arun C Murthy |
| [HADOOP-830](https://issues.apache.org/jira/browse/HADOOP-830) | Improve the performance of the Merge phase |  Major | . | Devaraj Das | Devaraj Das |
| [HADOOP-805](https://issues.apache.org/jira/browse/HADOOP-805) | JobClient should print the Task's stdout and stderr to the clients console |  Major | . | Owen O'Malley | Sanjay Dahiya |
| [HADOOP-801](https://issues.apache.org/jira/browse/HADOOP-801) | job tracker should keep a log of task completion and failure |  Major | . | Owen O'Malley | Sanjay Dahiya |
| [HADOOP-788](https://issues.apache.org/jira/browse/HADOOP-788) | Streaming should use a subclass of TextInputFormat for reading text inputs. |  Major | . | Owen O'Malley | Sanjay Dahiya |
| [HADOOP-726](https://issues.apache.org/jira/browse/HADOOP-726) | HDFS locking mechanisms should be simplified or removed |  Minor | . | Sameer Paranjpye | Raghu Angadi |
| [HADOOP-692](https://issues.apache.org/jira/browse/HADOOP-692) | Rack-aware Replica Placement |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-659](https://issues.apache.org/jira/browse/HADOOP-659) | Boost the priority of re-replicating blocks that are far from their replication target |  Major | . | Konstantin Shvachko | Hairong Kuang |
| [HADOOP-602](https://issues.apache.org/jira/browse/HADOOP-602) | Remove Lucene dependency |  Major | . | Andrzej Bialecki | Milind Bhandarkar |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-969](https://issues.apache.org/jira/browse/HADOOP-969) | deadlock in job tracker RetireJobs |  Critical | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-965](https://issues.apache.org/jira/browse/HADOOP-965) | Isolation Runner looking for job.jar in wrong directory |  Major | . | Dennis Kubes |  |
| [HADOOP-964](https://issues.apache.org/jira/browse/HADOOP-964) | ClassNotFoundException in ReduceTaskRunner |  Blocker | . | Dennis Kubes |  |
| [HADOOP-962](https://issues.apache.org/jira/browse/HADOOP-962) | Hadoop EC2 scripts are not executable |  Major | scripts | Tom White | Tom White |
| [HADOOP-959](https://issues.apache.org/jira/browse/HADOOP-959) | TestCheckpoint fails on Windows |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-937](https://issues.apache.org/jira/browse/HADOOP-937) | data node re-registration |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-935](https://issues.apache.org/jira/browse/HADOOP-935) | Abacus should not delete the output dir |  Major | . | Runping Qi | Runping Qi |
| [HADOOP-934](https://issues.apache.org/jira/browse/HADOOP-934) | TaskTracker sends duplicate status when updating task metrics throws exception |  Major | . | Nigel Daley | Arun C Murthy |
| [HADOOP-929](https://issues.apache.org/jira/browse/HADOOP-929) | PhasedFileSystem should implement get/set configuration |  Major | . | Sanjay Dahiya | Sanjay Dahiya |
| [HADOOP-922](https://issues.apache.org/jira/browse/HADOOP-922) | Optimize small reads and seeks |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-920](https://issues.apache.org/jira/browse/HADOOP-920) | MapFileOutputFormat and SequenceFileOutputFormat use incorrect key/value classes in map/reduce tasks |  Major | . | Andrzej Bialecki | Andrzej Bialecki |
| [HADOOP-916](https://issues.apache.org/jira/browse/HADOOP-916) | HADOOP-908 patch causes javadoc warnings |  Trivial | . | Nigel Daley | Nigel Daley |
| [HADOOP-912](https://issues.apache.org/jira/browse/HADOOP-912) | TestMiniMRWithDFS fails sporadically |  Major | . | Nigel Daley | Arun C Murthy |
| [HADOOP-909](https://issues.apache.org/jira/browse/HADOOP-909) | dfs "du" shows that the size of a subdirectory is 0 |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-905](https://issues.apache.org/jira/browse/HADOOP-905) | Code to qualify inputDirs doesn't affect path validation |  Major | fs | Kenji Matsuoka |  |
| [HADOOP-902](https://issues.apache.org/jira/browse/HADOOP-902) | NPE in DFSOutputStream.closeBackupStream() |  Major | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-901](https://issues.apache.org/jira/browse/HADOOP-901) | Make S3FileSystem do recursive renames |  Major | fs | Tom White |  |
| [HADOOP-899](https://issues.apache.org/jira/browse/HADOOP-899) | Removal of deprecated code (in v0.10.0) from trunk breaks libhdfs |  Major | . | Sameer Paranjpye | Sameer Paranjpye |
| [HADOOP-898](https://issues.apache.org/jira/browse/HADOOP-898) | namenode generates infinite stream of null pointers |  Major | . | Owen O'Malley | Raghu Angadi |
| [HADOOP-886](https://issues.apache.org/jira/browse/HADOOP-886) | thousands of TimerThreads created by metrics API |  Major | metrics | Nigel Daley | Nigel Daley |
| [HADOOP-881](https://issues.apache.org/jira/browse/HADOOP-881) | job history web/ui does not count task failures correctly |  Major | . | Owen O'Malley | Sanjay Dahiya |
| [HADOOP-856](https://issues.apache.org/jira/browse/HADOOP-856) | fsck reports a non-existant DFS path as healthy |  Minor | . | Nigel Daley | Milind Bhandarkar |
| [HADOOP-855](https://issues.apache.org/jira/browse/HADOOP-855) | HDFS should repair corrupted files |  Major | . | Wendy Chien | Wendy Chien |
| [HADOOP-781](https://issues.apache.org/jira/browse/HADOOP-781) | Remove from trunk things deprecated in 0.10 branch. |  Major | . | Doug Cutting | Doug Cutting |
| [HADOOP-758](https://issues.apache.org/jira/browse/HADOOP-758) | FileNotFound on DFS block file |  Major | . | Owen O'Malley | Raghu Angadi |
| [HADOOP-757](https://issues.apache.org/jira/browse/HADOOP-757) | "Bad File Descriptor" in closing DFS file |  Major | . | Owen O'Malley | Raghu Angadi |
| [HADOOP-735](https://issues.apache.org/jira/browse/HADOOP-735) | The underlying data structure, ByteArrayOutputStream,  for buffer type of Hadoop record is inappropriate |  Major | record | Runping Qi | Milind Bhandarkar |
| [HADOOP-731](https://issues.apache.org/jira/browse/HADOOP-731) | Sometimes when a dfs file is accessed and one copy has a checksum error the I/O command fails, even if another copy is alright. |  Major | . | Dick King | Wendy Chien |
| [HADOOP-549](https://issues.apache.org/jira/browse/HADOOP-549) | NullPointerException in TaskReport's serialization code |  Major | . | Michel Tourn | Owen O'Malley |
| [HADOOP-405](https://issues.apache.org/jira/browse/HADOOP-405) | Duplicate browseDirectory.jsp |  Minor | . | Konstantin Shvachko | navychen |
| [HADOOP-309](https://issues.apache.org/jira/browse/HADOOP-309) | NullPointerException in StatusHttpServer |  Minor | . | Konstantin Shvachko | navychen |
| [HADOOP-227](https://issues.apache.org/jira/browse/HADOOP-227) | Namespace check pointing is not performed until the namenode restarts. |  Major | . | Konstantin Shvachko | dhruba borthakur |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


