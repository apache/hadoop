
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

## Release 0.8.0 - 2006-11-03

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-636](https://issues.apache.org/jira/browse/HADOOP-636) | MapFile constructor should accept Progressible |  Major | io | Doug Cutting | Doug Cutting |
| [HADOOP-635](https://issues.apache.org/jira/browse/HADOOP-635) | hadoop dfs copy, move commands should accept multiple source files as arguments |  Major | . | dhruba borthakur |  |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-670](https://issues.apache.org/jira/browse/HADOOP-670) | Generic types for FSNamesystem |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-660](https://issues.apache.org/jira/browse/HADOOP-660) | Format of junit output should be configurable |  Minor | . | Nigel Daley | Nigel Daley |
| [HADOOP-647](https://issues.apache.org/jira/browse/HADOOP-647) | Map outputs can't have a different type of compression from the reduce outputs |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-626](https://issues.apache.org/jira/browse/HADOOP-626) | NNBench example comments are incorrect and code contains cut-and-paste error |  Trivial | . | Nigel Daley | Nigel Daley |
| [HADOOP-625](https://issues.apache.org/jira/browse/HADOOP-625) | add a little servlet to display the server's thread call stacks |  Minor | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-586](https://issues.apache.org/jira/browse/HADOOP-586) | Job Name should not ben empty, if its is not given bu user, Hadoop should use original Jar name as the job name. |  Trivial | . | Sanjay Dahiya | Sanjay Dahiya |
| [HADOOP-583](https://issues.apache.org/jira/browse/HADOOP-583) | DFSClient should use debug level to log missing data-node. |  Minor | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-578](https://issues.apache.org/jira/browse/HADOOP-578) | Failed tasks should not be put at the end of the job tracker's queue |  Major | . | Owen O'Malley | Sanjay Dahiya |
| [HADOOP-498](https://issues.apache.org/jira/browse/HADOOP-498) | fsck should execute faster |  Major | . | Yoram Arnon | Milind Bhandarkar |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-671](https://issues.apache.org/jira/browse/HADOOP-671) | Distributed cache creates unnecessary symlinks if asked for creating symlinks |  Major | . | Mahadev konar | Mahadev konar |
| [HADOOP-669](https://issues.apache.org/jira/browse/HADOOP-669) | Upgrade to trunk causes data loss. |  Blocker | . | Milind Bhandarkar | Milind Bhandarkar |
| [HADOOP-665](https://issues.apache.org/jira/browse/HADOOP-665) |  hadoop -rmr  does NOT process multiple arguments and does not complain |  Minor | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-664](https://issues.apache.org/jira/browse/HADOOP-664) | build doesn't fail if libhdfs test(s) fail |  Minor | . | Nigel Daley | Nigel Daley |
| [HADOOP-663](https://issues.apache.org/jira/browse/HADOOP-663) | ant test is failing |  Major | . | Mahadev konar | Mahadev konar |
| [HADOOP-658](https://issues.apache.org/jira/browse/HADOOP-658) | source headers must conform to new Apache guidelines |  Major | . | Doug Cutting | Doug Cutting |
| [HADOOP-651](https://issues.apache.org/jira/browse/HADOOP-651) | fsck does not handle arguments -blocks and -locations correctly |  Major | . | Milind Bhandarkar | Milind Bhandarkar |
| [HADOOP-642](https://issues.apache.org/jira/browse/HADOOP-642) | Explicit timeout for ipc.Client |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-641](https://issues.apache.org/jira/browse/HADOOP-641) | Name-node should demand a block report from resurrected data-nodes. |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-638](https://issues.apache.org/jira/browse/HADOOP-638) | TaskTracker missing synchronization around tasks variable access |  Major | . | Nigel Daley | Nigel Daley |
| [HADOOP-634](https://issues.apache.org/jira/browse/HADOOP-634) | Test files missing copyright headers |  Trivial | . | Nigel Daley | Nigel Daley |
| [HADOOP-633](https://issues.apache.org/jira/browse/HADOOP-633) | if the jobinit thread gets killed the jobtracker keeps running without doing anything. |  Minor | . | Mahadev konar | Owen O'Malley |
| [HADOOP-627](https://issues.apache.org/jira/browse/HADOOP-627) | MiniMRCluster missing synchronization |  Major | . | Nigel Daley | Nigel Daley |
| [HADOOP-624](https://issues.apache.org/jira/browse/HADOOP-624) | fix warning about pathSpec should start with '/' or '\*' : mapOutput |  Minor | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-610](https://issues.apache.org/jira/browse/HADOOP-610) | Task Tracker offerService does not adequately protect from exceptions |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-599](https://issues.apache.org/jira/browse/HADOOP-599) | DFS Always Reports 0 Bytes Used |  Minor | . | Albert Chern | Raghu Angadi |
| [HADOOP-588](https://issues.apache.org/jira/browse/HADOOP-588) | JobTracker History bug - kill() ed tasks are logged wrongly as finished. |  Major | . | Sanjay Dahiya | Sanjay Dahiya |
| [HADOOP-572](https://issues.apache.org/jira/browse/HADOOP-572) | Chain reaction in a big cluster caused by simultaneous failure of only a few data-nodes. |  Major | . | Konstantin Shvachko | Sameer Paranjpye |
| [HADOOP-563](https://issues.apache.org/jira/browse/HADOOP-563) | DFS client should try to re-new lease if it gets a lease expiration exception when it adds a block to a file |  Major | . | Runping Qi | dhruba borthakur |
| [HADOOP-561](https://issues.apache.org/jira/browse/HADOOP-561) | one replica of a file should be written locally if possible |  Major | . | Yoram Arnon | dhruba borthakur |
| [HADOOP-554](https://issues.apache.org/jira/browse/HADOOP-554) | hadoop dfs command line doesn't exit with status code on error |  Major | . | Marco Nicosia | dhruba borthakur |
| [HADOOP-553](https://issues.apache.org/jira/browse/HADOOP-553) | DataNode and NameNode main() should catch and report exceptions. |  Major | . | Konstantin Shvachko | Raghu Angadi |
| [HADOOP-514](https://issues.apache.org/jira/browse/HADOOP-514) | namenode heartbeat interval should be configurable |  Major | . | Wendy Chien | Milind Bhandarkar |
| [HADOOP-482](https://issues.apache.org/jira/browse/HADOOP-482) | unit test hangs when a cluster is running on the same machine |  Minor | . | Wendy Chien | Wendy Chien |
| [HADOOP-477](https://issues.apache.org/jira/browse/HADOOP-477) | Streaming should execute Unix commands and scripts in well known languages without user specifying the path |  Major | . | arkady borkovsky | dhruba borthakur |
| [HADOOP-462](https://issues.apache.org/jira/browse/HADOOP-462) | DFSShell throws out arrayoutofbounds exceptions if the number of arguments is not right |  Minor | . | Mahadev konar | dhruba borthakur |
| [HADOOP-399](https://issues.apache.org/jira/browse/HADOOP-399) | the javadoc currently generates lot of warnings about bad fields |  Trivial | . | Owen O'Malley | Nigel Daley |
| [HADOOP-373](https://issues.apache.org/jira/browse/HADOOP-373) | Some calls to mkdirs do not check return value |  Major | . | Wendy Chien | Wendy Chien |
| [HADOOP-90](https://issues.apache.org/jira/browse/HADOOP-90) | DFS is succeptible to data loss in case of name node failure |  Major | . | Yoram Arnon | Konstantin Shvachko |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


