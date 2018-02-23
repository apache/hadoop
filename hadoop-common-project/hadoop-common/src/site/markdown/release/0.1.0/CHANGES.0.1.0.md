
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

## Release 0.1.0 - 2006-04-02



### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-37](https://issues.apache.org/jira/browse/HADOOP-37) | A way to determine the size and overall activity of the cluster |  Major | . | Owen O'Malley |  |
| [HADOOP-80](https://issues.apache.org/jira/browse/HADOOP-80) | binary key |  Major | io | Owen O'Malley | Owen O'Malley |
| [HADOOP-44](https://issues.apache.org/jira/browse/HADOOP-44) | RPC exceptions should include remote stack trace |  Major | ipc | Doug Cutting | Doug Cutting |
| [HADOOP-46](https://issues.apache.org/jira/browse/HADOOP-46) | user-specified job names |  Major | . | Doug Cutting | Owen O'Malley |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-25](https://issues.apache.org/jira/browse/HADOOP-25) | a new map/reduce example and moving the examples from src/java to src/examples |  Minor | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-20](https://issues.apache.org/jira/browse/HADOOP-20) | Mapper, Reducer need an occasion to cleanup after the last record is processed. |  Major | . | Michel Tourn |  |
| [HADOOP-30](https://issues.apache.org/jira/browse/HADOOP-30) | DFS shell: support for ls -r and cat |  Major | . | Michel Tourn |  |
| [HADOOP-38](https://issues.apache.org/jira/browse/HADOOP-38) | default splitter should incorporate fs block size |  Major | . | Doug Cutting |  |
| [HADOOP-36](https://issues.apache.org/jira/browse/HADOOP-36) | Adding some uniformity/convenience to environment management |  Major | conf | Bryan Pendleton |  |
| [HADOOP-49](https://issues.apache.org/jira/browse/HADOOP-49) | JobClient cannot use a non-default server (unlike DFSShell) |  Major | . | Michel Tourn | Michel Tourn |
| [HADOOP-41](https://issues.apache.org/jira/browse/HADOOP-41) | JAVA\_OPTS for the TaskRunner Child |  Minor | conf | stack |  |
| [HADOOP-60](https://issues.apache.org/jira/browse/HADOOP-60) | Specification of alternate conf. directory |  Minor | . | stack |  |
| [HADOOP-79](https://issues.apache.org/jira/browse/HADOOP-79) | listFiles optimization |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-87](https://issues.apache.org/jira/browse/HADOOP-87) | SequenceFile performance degrades substantially compression is on and large values are encountered |  Major | io | Sameer Paranjpye | Doug Cutting |
| [HADOOP-45](https://issues.apache.org/jira/browse/HADOOP-45) | JobTracker should log task errors |  Major | . | Doug Cutting | Doug Cutting |
| [HADOOP-67](https://issues.apache.org/jira/browse/HADOOP-67) | Added statistic/reporting info to DFS |  Trivial | . | Barry Kaplan | Doug Cutting |
| [HADOOP-33](https://issues.apache.org/jira/browse/HADOOP-33) | DF enhancement: performance and win XP support |  Minor | fs | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-103](https://issues.apache.org/jira/browse/HADOOP-103) | introduce a common parent class for Mapper and Reducer |  Minor | . | Owen O'Malley | Owen O'Malley |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-6](https://issues.apache.org/jira/browse/HADOOP-6) | missing build directory in classpath |  Minor | . | Owen O'Malley |  |
| [HADOOP-7](https://issues.apache.org/jira/browse/HADOOP-7) | MapReduce has a series of problems concerning task-allocation to worker nodes |  Major | . | Mike Cafarella |  |
| [HADOOP-10](https://issues.apache.org/jira/browse/HADOOP-10) | ndfs.replication is not documented within the nutch-default.xml configuration file. |  Trivial | . | Rod Taylor |  |
| [HADOOP-5](https://issues.apache.org/jira/browse/HADOOP-5) | need commons-logging-api jar file |  Minor | . | Owen O'Malley |  |
| [HADOOP-21](https://issues.apache.org/jira/browse/HADOOP-21) | the webapps need to be updated for the move from nutch |  Minor | . | Owen O'Malley |  |
| [HADOOP-22](https://issues.apache.org/jira/browse/HADOOP-22) | remove unused imports |  Trivial | . | Sami Siren |  |
| [HADOOP-28](https://issues.apache.org/jira/browse/HADOOP-28) | webapps broken |  Major | . | Owen O'Malley |  |
| [HADOOP-12](https://issues.apache.org/jira/browse/HADOOP-12) | InputFormat used in job must be in JobTracker classpath (not loaded from job JAR) |  Minor | . | Bryan Pendleton |  |
| [HADOOP-34](https://issues.apache.org/jira/browse/HADOOP-34) | Build Paths Relative to PWD in build.xml |  Trivial | . | Jeremy Bensley |  |
| [HADOOP-42](https://issues.apache.org/jira/browse/HADOOP-42) | PositionCache decrements its position for reads at the end of file |  Major | fs | Konstantin Shvachko |  |
| [HADOOP-40](https://issues.apache.org/jira/browse/HADOOP-40) | bufferSize argument is ignored in FileSystem.create(File, boolean, int) |  Minor | fs | Konstantin Shvachko |  |
| [HADOOP-16](https://issues.apache.org/jira/browse/HADOOP-16) | RPC call times out while indexing map task is computing splits |  Major | . | Chris Schneider | Mike Cafarella |
| [HADOOP-57](https://issues.apache.org/jira/browse/HADOOP-57) | hadoop dfs -ls / does not show root of file system |  Minor | . | Yoram Arnon |  |
| [HADOOP-66](https://issues.apache.org/jira/browse/HADOOP-66) | dfs client writes all data for a chunk to /tmp |  Major | . | Sameer Paranjpye | Doug Cutting |
| [HADOOP-70](https://issues.apache.org/jira/browse/HADOOP-70) | the two file system tests TestDFS and TestFileSystem take too long |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-77](https://issues.apache.org/jira/browse/HADOOP-77) | hang / crash when input folder does not exists. |  Critical | . | Stefan Groschupf |  |
| [HADOOP-78](https://issues.apache.org/jira/browse/HADOOP-78) | rpc commands not buffered |  Major | ipc | Owen O'Malley | Owen O'Malley |
| [HADOOP-81](https://issues.apache.org/jira/browse/HADOOP-81) | speculative execution is only controllable from the default config |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-82](https://issues.apache.org/jira/browse/HADOOP-82) | JobTracker loses it: NoSuchElementException |  Minor | . | stack |  |
| [HADOOP-86](https://issues.apache.org/jira/browse/HADOOP-86) | If corrupted map outputs, reducers get stuck fetching forever |  Major | . | stack | Doug Cutting |
| [HADOOP-93](https://issues.apache.org/jira/browse/HADOOP-93) | allow minimum split size configurable |  Major | . | Hairong Kuang | Doug Cutting |
| [HADOOP-97](https://issues.apache.org/jira/browse/HADOOP-97) | DFSShell.cat returns NullPointerException if file does not exist |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-3](https://issues.apache.org/jira/browse/HADOOP-3) | Output directories are not cleaned up before the reduces run |  Minor | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-83](https://issues.apache.org/jira/browse/HADOOP-83) | infinite retries accessing a missing block |  Major | . | Yoram Arnon | Konstantin Shvachko |
| [HADOOP-52](https://issues.apache.org/jira/browse/HADOOP-52) | mapred input and output dirs must be absolute |  Major | . | Doug Cutting | Owen O'Malley |
| [HADOOP-98](https://issues.apache.org/jira/browse/HADOOP-98) | The JobTracker's count of the number of running maps and reduces is wrong |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-19](https://issues.apache.org/jira/browse/HADOOP-19) | Datanode corruption |  Critical | . | Rod Taylor | Doug Cutting |
| [HADOOP-84](https://issues.apache.org/jira/browse/HADOOP-84) | client should report file name in which IO exception occurs |  Minor | . | Yoram Arnon | Konstantin Shvachko |
| [HADOOP-2](https://issues.apache.org/jira/browse/HADOOP-2) | Reused Keys and Values fail with a Combiner |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-110](https://issues.apache.org/jira/browse/HADOOP-110) | new key and value instances are allocated before each map |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-107](https://issues.apache.org/jira/browse/HADOOP-107) | Namenode errors "Failed to complete filename.crc  because dir.getFile()==null and null" |  Major | . | Igor Bolotin | Doug Cutting |
| [HADOOP-100](https://issues.apache.org/jira/browse/HADOOP-100) | Inconsistent locking of the JobTracker.taskTrackers field |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-102](https://issues.apache.org/jira/browse/HADOOP-102) | Two identical consecutive loops in FSNamesystem.chooseTarget() |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-112](https://issues.apache.org/jira/browse/HADOOP-112) | copyFromLocal should exclude .crc files |  Minor | . | Monu Ogbe | Doug Cutting |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-1](https://issues.apache.org/jira/browse/HADOOP-1) | initial import of code from Nutch |  Major | . | Doug Cutting | Doug Cutting |


