
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

## Release 0.3.0 - 2006-06-02

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-256](https://issues.apache.org/jira/browse/HADOOP-256) | Implement a C api for hadoop dfs |  Major | . | Arun C Murthy | Arun C Murthy |
| [HADOOP-233](https://issues.apache.org/jira/browse/HADOOP-233) | add a http status server for the task trackers |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-222](https://issues.apache.org/jira/browse/HADOOP-222) | Set replication from dfsshell |  Trivial | . | Johan Oskarsson | Johan Oskarsson |
| [HADOOP-209](https://issues.apache.org/jira/browse/HADOOP-209) | Add a program to recursively copy directories across file systems |  Major | fs | Milind Bhandarkar |  |
| [HADOOP-115](https://issues.apache.org/jira/browse/HADOOP-115) | permit reduce input types to differ from reduce output types |  Major | . | Runping Qi | Runping Qi |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-254](https://issues.apache.org/jira/browse/HADOOP-254) | use http to shuffle data between the maps and the reduces |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-218](https://issues.apache.org/jira/browse/HADOOP-218) | Inefficient calls to get configuration values in TaskInprogress |  Minor | . | Mahadev konar | Mahadev konar |
| [HADOOP-216](https://issues.apache.org/jira/browse/HADOOP-216) | Task Detail web page missing progress |  Trivial | . | Bryan Pendleton | Doug Cutting |
| [HADOOP-212](https://issues.apache.org/jira/browse/HADOOP-212) | allow changes to dfs block size |  Critical | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-211](https://issues.apache.org/jira/browse/HADOOP-211) | logging improvements for Hadoop |  Minor | . | Sameer Paranjpye | Sameer Paranjpye |
| [HADOOP-208](https://issues.apache.org/jira/browse/HADOOP-208) | add failure page to webapp |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-204](https://issues.apache.org/jira/browse/HADOOP-204) | Need to tweak a few things in the metrics package to support the Simon plugin |  Major | metrics | David Bowen | David Bowen |
| [HADOOP-202](https://issues.apache.org/jira/browse/HADOOP-202) | sort should use a smaller number of reduces |  Trivial | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-195](https://issues.apache.org/jira/browse/HADOOP-195) | improve performance of map output transfers |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-75](https://issues.apache.org/jira/browse/HADOOP-75) | dfs should check full file availability only at close |  Minor | . | Doug Cutting | Milind Bhandarkar |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-270](https://issues.apache.org/jira/browse/HADOOP-270) | possible deadlock when shut down a datanode thread |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-265](https://issues.apache.org/jira/browse/HADOOP-265) | Abort tasktracker if it can not write to its local directories |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-264](https://issues.apache.org/jira/browse/HADOOP-264) | WritableFactory has no permissions to create DatanodeRegistration |  Blocker | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-259](https://issues.apache.org/jira/browse/HADOOP-259) | map output http client does not timeout |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-251](https://issues.apache.org/jira/browse/HADOOP-251) | progress report failures kill task |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-247](https://issues.apache.org/jira/browse/HADOOP-247) | The Reduce Task thread for reporting progress during the sort exits in case of any IOException |  Critical | . | Mahadev konar | Mahadev konar |
| [HADOOP-241](https://issues.apache.org/jira/browse/HADOOP-241) | TestCopyFiles fails under cygwin due to incorrect path |  Minor | fs | Konstantin Shvachko | Milind Bhandarkar |
| [HADOOP-238](https://issues.apache.org/jira/browse/HADOOP-238) | map outputs transfers fail with EOFException |  Blocker | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-235](https://issues.apache.org/jira/browse/HADOOP-235) | LocalFileSystem.openRaw() throws the wrong string for FileNotFoundException |  Major | . | Benjamin Reed |  |
| [HADOOP-229](https://issues.apache.org/jira/browse/HADOOP-229) | hadoop cp should generate a better number of map tasks |  Minor | fs | Yoram Arnon | Milind Bhandarkar |
| [HADOOP-228](https://issues.apache.org/jira/browse/HADOOP-228) | hadoop cp should have a -config option |  Minor | fs | Yoram Arnon | Milind Bhandarkar |
| [HADOOP-219](https://issues.apache.org/jira/browse/HADOOP-219) | SequenceFile#handleChecksumException NPE |  Trivial | io | stack | Doug Cutting |
| [HADOOP-217](https://issues.apache.org/jira/browse/HADOOP-217) | IllegalAcessException when creating a Block object via WritableFactories |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-205](https://issues.apache.org/jira/browse/HADOOP-205) | the job tracker does not schedule enough map on the cluster |  Major | . | Owen O'Malley | Mahadev konar |
| [HADOOP-200](https://issues.apache.org/jira/browse/HADOOP-200) | The map task names are sent to the reduces |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-180](https://issues.apache.org/jira/browse/HADOOP-180) | task tracker times out cleaning big job |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-179](https://issues.apache.org/jira/browse/HADOOP-179) | task tracker ghosts remain after 10 minutes |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-163](https://issues.apache.org/jira/browse/HADOOP-163) | If a DFS datanode cannot write onto its file system. it should tell the name node not to assign new blocks to it. |  Major | . | Runping Qi | Hairong Kuang |
| [HADOOP-161](https://issues.apache.org/jira/browse/HADOOP-161) | dfs blocks define equal, but not hashcode |  Major | . | Owen O'Malley | Milind Bhandarkar |
| [HADOOP-146](https://issues.apache.org/jira/browse/HADOOP-146) | potential conflict in block id's, leading to data corruption |  Major | . | Yoram Arnon | Konstantin Shvachko |
| [HADOOP-141](https://issues.apache.org/jira/browse/HADOOP-141) | Disk thrashing / task timeouts during map output copy phase |  Major | . | p sutter | Owen O'Malley |
| [HADOOP-124](https://issues.apache.org/jira/browse/HADOOP-124) | don't permit two datanodes to run from same dfs.data.dir |  Critical | . | Bryan Pendleton | Konstantin Shvachko |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-220](https://issues.apache.org/jira/browse/HADOOP-220) | Add -dfs and -jt command-line parameters to specify namenode and jobtracker. |  Major | fs | Milind Bhandarkar | Milind Bhandarkar |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


