
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

## Release 0.22.1 - Unreleased (as of 2016-03-04)

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-6453](https://issues.apache.org/jira/browse/HADOOP-6453) | Hadoop wrapper script shouldn't ignore an existing JAVA\_LIBRARY\_PATH |  Minor | scripts | Chad Metcalf |  |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-7937](https://issues.apache.org/jira/browse/HADOOP-7937) | Forward port SequenceFile#syncFs and friends from Hadoop 1.x |  Major | io | Eli Collins | Tom White |
| [HADOOP-7119](https://issues.apache.org/jira/browse/HADOOP-7119) | add Kerberos HTTP SPNEGO authentication support to Hadoop JT/NN/DN/TT web-consoles |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |
| [MAPREDUCE-3837](https://issues.apache.org/jira/browse/MAPREDUCE-3837) | Job tracker is not able to recover job in case of crash and after that no user can submit job. |  Major | . | Mayank Bansal | Mayank Bansal |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-7338](https://issues.apache.org/jira/browse/HADOOP-7338) | LocalDirAllocator improvements for MR-2178 |  Major | . | Todd Lipcon | Benoy Antony |
| [HADOOP-7272](https://issues.apache.org/jira/browse/HADOOP-7272) | Remove unnecessary security related info logs |  Major | ipc, security | Suresh Srinivas | Suresh Srinivas |
| [HADOOP-6995](https://issues.apache.org/jira/browse/HADOOP-6995) | Allow wildcards to be used in ProxyUsers configurations |  Minor | security | Todd Lipcon | Todd Lipcon |
| [HDFS-2246](https://issues.apache.org/jira/browse/HDFS-2246) | Shortcut a local client reads to a Datanodes files directly |  Major | . | Sanjay Radia | Jitendra Nath Pandey |
| [HDFS-1601](https://issues.apache.org/jira/browse/HDFS-1601) | Pipeline ACKs are sent as lots of tiny TCP packets |  Major | datanode | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-4405](https://issues.apache.org/jira/browse/MAPREDUCE-4405) | Adding test case for HierarchicalQueue in TestJobQueueClient |  Minor | client | Mayank Bansal | Mayank Bansal |
| [MAPREDUCE-4403](https://issues.apache.org/jira/browse/MAPREDUCE-4403) | Adding test case for resubmission of jobs in TestRecoveryManager |  Minor | jobtracker | Mayank Bansal | Mayank Bansal |
| [MAPREDUCE-4349](https://issues.apache.org/jira/browse/MAPREDUCE-4349) | Distributed Cache gives inconsistent result if cache Archive files get deleted from task tracker |  Minor | . | Mayank Bansal | Mayank Bansal |
| [MAPREDUCE-2353](https://issues.apache.org/jira/browse/MAPREDUCE-2353) | Make the MR changes to reflect the API changes in SecureIO library |  Major | security, task, tasktracker | Devaraj Das | Benoy Antony |
| [MAPREDUCE-1521](https://issues.apache.org/jira/browse/MAPREDUCE-1521) | Protection against incorrectly configured reduces |  Major | jobtracker | Arun C Murthy | Mahadev konar |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-7680](https://issues.apache.org/jira/browse/HADOOP-7680) | TestHardLink fails on Mac OS X, when gnu stat is in path |  Major | . | Milind Bhandarkar | Milind Bhandarkar |
| [HADOOP-7621](https://issues.apache.org/jira/browse/HADOOP-7621) | alfredo config should be in a file not readable by users |  Critical | security | Alejandro Abdelnur | Aaron T. Myers |
| [HADOOP-7115](https://issues.apache.org/jira/browse/HADOOP-7115) | Add a cache for getpwuid\_r and getpwgid\_r calls |  Major | . | Arun C Murthy | Alejandro Abdelnur |
| [HDFS-3402](https://issues.apache.org/jira/browse/HDFS-3402) | Fix hdfs scripts for secure datanodes |  Minor | scripts, security | Benoy Antony | Benoy Antony |
| [HDFS-3368](https://issues.apache.org/jira/browse/HDFS-3368) | Missing blocks due to bad DataNodes coming up and down. |  Major | namenode | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-2991](https://issues.apache.org/jira/browse/HDFS-2991) | failure to load edits: ClassCastException |  Blocker | namenode | Todd Lipcon | Todd Lipcon |
| [HDFS-2877](https://issues.apache.org/jira/browse/HDFS-2877) | If locking of a storage dir fails, it will remove the other NN's lock file on exit |  Major | namenode | Todd Lipcon | Todd Lipcon |
| [HDFS-2718](https://issues.apache.org/jira/browse/HDFS-2718) | Optimize OP\_ADD in edits loading |  Major | namenode | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-2698](https://issues.apache.org/jira/browse/HDFS-2698) | BackupNode is downloading image from NameNode for every checkpoint |  Major | namenode | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-1910](https://issues.apache.org/jira/browse/HDFS-1910) | when dfs.name.dir and dfs.name.edits.dir are same fsimage will be saved twice every time |  Minor | namenode | Gokul |  |
| [HDFS-1584](https://issues.apache.org/jira/browse/HDFS-1584) | Need to check TGT and renew if needed when fetching delegation tokens using HFTP |  Major | security | Kan Zhang | Benoy Antony |
| [MAPREDUCE-5706](https://issues.apache.org/jira/browse/MAPREDUCE-5706) | toBeDeleted parent directories aren't being cleaned up |  Major | security | Robert Kanter | Robert Kanter |
| [MAPREDUCE-4404](https://issues.apache.org/jira/browse/MAPREDUCE-4404) | Adding Test case for TestMRJobClient to verify the user name |  Minor | client | Mayank Bansal | Mayank Bansal |
| [MAPREDUCE-4360](https://issues.apache.org/jira/browse/MAPREDUCE-4360) | Capacity Scheduler Hierarchical leaf queue does not honor the max capacity of container queue |  Major | . | Mayank Bansal | Mayank Bansal |
| [MAPREDUCE-4318](https://issues.apache.org/jira/browse/MAPREDUCE-4318) | TestRecoveryManager should not use raw and deprecated configuration parameters. |  Major | test | Konstantin Shvachko | Benoy Antony |
| [MAPREDUCE-4314](https://issues.apache.org/jira/browse/MAPREDUCE-4314) | Synchronization in JvmManager for 0.22 branch |  Major | tasktracker | Konstantin Shvachko | Benoy Antony |
| [MAPREDUCE-4164](https://issues.apache.org/jira/browse/MAPREDUCE-4164) | Hadoop 22 Exception thrown after task completion causes its reexecution |  Major | tasktracker | Mayank Bansal | Mayank Bansal |
| [MAPREDUCE-3863](https://issues.apache.org/jira/browse/MAPREDUCE-3863) | 0.22 branch mvn deploy is not publishing hadoop-streaming JAR |  Critical | build | Alejandro Abdelnur | Benoy Antony |
| [MAPREDUCE-3725](https://issues.apache.org/jira/browse/MAPREDUCE-3725) | Hadoop 22 hadoop job -list returns user name as NULL |  Major | client | Mayank Bansal | Mayank Bansal |
| [MAPREDUCE-3593](https://issues.apache.org/jira/browse/MAPREDUCE-3593) | MAPREDUCE Impersonation is not working in 22 |  Major | job submission | Mayank Bansal | Mayank Bansal |
| [MAPREDUCE-2452](https://issues.apache.org/jira/browse/MAPREDUCE-2452) | Delegation token cancellation shouldn't hold global JobTracker lock |  Major | jobtracker | Devaraj Das | Devaraj Das |
| [MAPREDUCE-2420](https://issues.apache.org/jira/browse/MAPREDUCE-2420) | JobTracker should be able to renew delegation token over HTTP |  Major | . | Boris Shkolnik | Boris Shkolnik |
| [MAPREDUCE-2377](https://issues.apache.org/jira/browse/MAPREDUCE-2377) | task-controller fails to parse configuration if it doesn't end in \n |  Major | task-controller | Todd Lipcon | Benoy Antony |
| [MAPREDUCE-2178](https://issues.apache.org/jira/browse/MAPREDUCE-2178) | Race condition in LinuxTaskController permissions handling |  Major | security, task-controller | Todd Lipcon | Benoy Antony |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-8383](https://issues.apache.org/jira/browse/HADOOP-8383) | TestKerberosAuthenticator fails |  Minor | security | Benoy Antony | Benoy Antony |
| [HADOOP-8381](https://issues.apache.org/jira/browse/HADOOP-8381) | Substitute \_HOST with hostname  for HTTP principals |  Minor | security | Benoy Antony | Benoy Antony |
| [HDFS-2886](https://issues.apache.org/jira/browse/HDFS-2886) | CreateEditLogs should generate a realistic edit log. |  Major | namenode | Konstantin Shvachko | Konstantin Shvachko |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-8357](https://issues.apache.org/jira/browse/HADOOP-8357) | Restore security in Hadoop 0.22 branch |  Major | security | Konstantin Shvachko | Benoy Antony |
| [MAPREDUCE-4249](https://issues.apache.org/jira/browse/MAPREDUCE-4249) | Fix failures in streaming test TestFileArgs |  Minor | security | Benoy Antony | Benoy Antony |
| [MAPREDUCE-4248](https://issues.apache.org/jira/browse/MAPREDUCE-4248) | TestRecoveryManager fails |  Minor | security | Benoy Antony | Benoy Antony |
| [MAPREDUCE-4247](https://issues.apache.org/jira/browse/MAPREDUCE-4247) | TestTaskTrackerLocalization fails |  Minor | security | Benoy Antony | Benoy Antony |
| [MAPREDUCE-4246](https://issues.apache.org/jira/browse/MAPREDUCE-4246) | Failure in deleting user directories in Secure hadoop |  Major | security | Benoy Antony | Benoy Antony |
| [MAPREDUCE-4244](https://issues.apache.org/jira/browse/MAPREDUCE-4244) | Fix an issue related to do with setting of correct groups for tasks |  Minor | security | Benoy Antony | Benoy Antony |
| [MAPREDUCE-4243](https://issues.apache.org/jira/browse/MAPREDUCE-4243) | Modify mapreduce build to include task-controller |  Minor | build | Benoy Antony | Benoy Antony |
| [MAPREDUCE-4240](https://issues.apache.org/jira/browse/MAPREDUCE-4240) | Revert MAPREDUCE-2767 |  Minor | security | Benoy Antony | Benoy Antony |


