
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

## Release 0.4.0 - 2006-06-28

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-296](https://issues.apache.org/jira/browse/HADOOP-296) | Do not assign blocks to a datanode with \< x mb free |  Major | . | Johan Oskarsson | Johan Oskarsson |
| [HADOOP-250](https://issues.apache.org/jira/browse/HADOOP-250) | HTTP Browsing interface for DFS Health/Status |  Major | . | Devaraj Das |  |
| [HADOOP-123](https://issues.apache.org/jira/browse/HADOOP-123) | mini map/reduce cluster for junit tests |  Major | . | Owen O'Malley | Milind Bhandarkar |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-328](https://issues.apache.org/jira/browse/HADOOP-328) | add a -i option to distcp to ignore read errors of the input files |  Major | util | Owen O'Malley | Owen O'Malley |
| [HADOOP-326](https://issues.apache.org/jira/browse/HADOOP-326) | cleanup of dead field (map ouput port) |  Minor | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-314](https://issues.apache.org/jira/browse/HADOOP-314) | remove the append phase in sorting the reduce inputs |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-305](https://issues.apache.org/jira/browse/HADOOP-305) | tasktracker waits for 10 seconds for asking for a task. |  Minor | . | Mahadev konar | Mahadev konar |
| [HADOOP-301](https://issues.apache.org/jira/browse/HADOOP-301) | the randomwriter example will clobber the output file |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-298](https://issues.apache.org/jira/browse/HADOOP-298) | nicer reports of progress for distcp |  Minor | util | Owen O'Malley | Owen O'Malley |
| [HADOOP-271](https://issues.apache.org/jira/browse/HADOOP-271) | add links to task tracker http server from task details and failure pages |  Minor | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-59](https://issues.apache.org/jira/browse/HADOOP-59) | support generic command-line options |  Minor | conf | Doug Cutting | Hairong Kuang |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-325](https://issues.apache.org/jira/browse/HADOOP-325) | ClassNotFoundException under jvm 1.6 |  Major | ipc | Owen O'Malley | Owen O'Malley |
| [HADOOP-319](https://issues.apache.org/jira/browse/HADOOP-319) | FileSystem "close" does not remove the closed fs from the fs map |  Major | fs | Hairong Kuang | Hairong Kuang |
| [HADOOP-318](https://issues.apache.org/jira/browse/HADOOP-318) | Progress in writing a DFS file does not count towards Job progress and can make the task timeout |  Major | . | Milind Bhandarkar | Milind Bhandarkar |
| [HADOOP-317](https://issues.apache.org/jira/browse/HADOOP-317) | "connection was forcibly closed" Exception in RPC on Windows |  Major | ipc | Konstantin Shvachko | Doug Cutting |
| [HADOOP-316](https://issues.apache.org/jira/browse/HADOOP-316) | job tracker has a deadlock |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-311](https://issues.apache.org/jira/browse/HADOOP-311) | dfs client timeout on read kills task |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-304](https://issues.apache.org/jira/browse/HADOOP-304) | UnregisteredDatanodeException message correction |  Trivial | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-299](https://issues.apache.org/jira/browse/HADOOP-299) | maps from second jobs will not run until the first job finishes completely |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-278](https://issues.apache.org/jira/browse/HADOOP-278) | a missing map/reduce input directory does not produce a user-visible error message |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-210](https://issues.apache.org/jira/browse/HADOOP-210) | Namenode not able to accept connections |  Major | . | Mahadev konar | Devaraj Das |
| [HADOOP-135](https://issues.apache.org/jira/browse/HADOOP-135) | Potential deadlock in JobTracker. |  Major | . | Konstantin Shvachko | Owen O'Malley |
| [HADOOP-99](https://issues.apache.org/jira/browse/HADOOP-99) | task trackers can only be assigned one task every heartbeat |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-27](https://issues.apache.org/jira/browse/HADOOP-27) | MapRed tries to allocate tasks to nodes that have no available disk space |  Major | . | Mike Cafarella |  |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


