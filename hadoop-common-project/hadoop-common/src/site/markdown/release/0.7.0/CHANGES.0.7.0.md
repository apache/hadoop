
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

## Release 0.7.0 - 2006-10-06

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-567](https://issues.apache.org/jira/browse/HADOOP-567) | The build script should record the Hadoop version into the build |  Major | util | Owen O'Malley | Owen O'Malley |
| [HADOOP-559](https://issues.apache.org/jira/browse/HADOOP-559) | Support file patterns in dfs commands |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-542](https://issues.apache.org/jira/browse/HADOOP-542) | on-the-fly merge sort, HADOOP-540, reformat |  Major | . | Michel Tourn |  |
| [HADOOP-522](https://issues.apache.org/jira/browse/HADOOP-522) | MapFile should support block compression |  Major | io | Doug Cutting | Doug Cutting |
| [HADOOP-519](https://issues.apache.org/jira/browse/HADOOP-519) | HDFS File API should be extended to include positional read |  Major | . | Milind Bhandarkar | Milind Bhandarkar |
| [HADOOP-306](https://issues.apache.org/jira/browse/HADOOP-306) | Safe mode and name node startup procedures |  Major | . | Konstantin Shvachko | Konstantin Shvachko |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-560](https://issues.apache.org/jira/browse/HADOOP-560) | tasks should have a "killed" state |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-556](https://issues.apache.org/jira/browse/HADOOP-556) | Streaming should send keep-alive signals to Reporter every 10 seconds, not every 100 records |  Major | . | Michel Tourn |  |
| [HADOOP-551](https://issues.apache.org/jira/browse/HADOOP-551) | reduce the number of lines printed to the console during execution |  Minor | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-527](https://issues.apache.org/jira/browse/HADOOP-527) | Allow to specify the bind address for all hadoop servers |  Major | . | Philippe Gassmann |  |
| [HADOOP-487](https://issues.apache.org/jira/browse/HADOOP-487) | misspelt DFS host name gives null pointer exception in getProtocolVersion |  Major | . | Dick King | Sameer Paranjpye |
| [HADOOP-431](https://issues.apache.org/jira/browse/HADOOP-431) | default behaviour of dfsShell -rm should resemble 'rm -i', not 'rm -rf' |  Major | . | Yoram Arnon | Sameer Paranjpye |
| [HADOOP-263](https://issues.apache.org/jira/browse/HADOOP-263) | task status should include timestamps for when a job transitions |  Major | . | Owen O'Malley | Sanjay Dahiya |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-581](https://issues.apache.org/jira/browse/HADOOP-581) | Datanode's offerService does not handle rpc timeouts |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-568](https://issues.apache.org/jira/browse/HADOOP-568) | small jobs benchmark fails: task is UNASSIGNED |  Major | . | Yoram Arnon | Owen O'Malley |
| [HADOOP-566](https://issues.apache.org/jira/browse/HADOOP-566) | hadoop-daemons.sh fails with "no such file or directory" when used from a relative symlinked path |  Major | scripts | Lee Faris | Doug Cutting |
| [HADOOP-552](https://issues.apache.org/jira/browse/HADOOP-552) | getMapOutput doesn't reliably detect errors and throw to the caller |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-550](https://issues.apache.org/jira/browse/HADOOP-550) | Text constructure can throw exception |  Major | . | Bryan Pendleton | Hairong Kuang |
| [HADOOP-547](https://issues.apache.org/jira/browse/HADOOP-547) | ReduceTaskRunner can miss sending hearbeats if no map output copy finishes within "mapred.task.timeout" |  Major | . | Sanjay Dahiya | Sanjay Dahiya |
| [HADOOP-545](https://issues.apache.org/jira/browse/HADOOP-545) | Unused parameter in hadoop-default.xml |  Trivial | conf | Philippe Gassmann |  |
| [HADOOP-540](https://issues.apache.org/jira/browse/HADOOP-540) | Streaming should send keep-alive signals to Reporter |  Major | . | Michel Tourn |  |
| [HADOOP-537](https://issues.apache.org/jira/browse/HADOOP-537) | clean-libhdfs target of build.xml does not work on windows |  Major | . | Konstantin Shvachko | Arun C Murthy |
| [HADOOP-536](https://issues.apache.org/jira/browse/HADOOP-536) | Broke ant test on windows |  Major | . | Mahadev konar |  |
| [HADOOP-533](https://issues.apache.org/jira/browse/HADOOP-533) | TestDFSShellGenericOptions creates a sub-directory in conf/ |  Minor | . | Doug Cutting | Hairong Kuang |
| [HADOOP-530](https://issues.apache.org/jira/browse/HADOOP-530) | Error message does not expose mismached key or value class name correctly in Sequence file |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-513](https://issues.apache.org/jira/browse/HADOOP-513) | IllegalStateException is thrown by TaskTracker |  Major | . | Konstantin Shvachko | Owen O'Malley |
| [HADOOP-508](https://issues.apache.org/jira/browse/HADOOP-508) | random seeks using FSDataInputStream can become invalid such that reads return invalid data |  Major | . | Christian Kunz | Milind Bhandarkar |
| [HADOOP-506](https://issues.apache.org/jira/browse/HADOOP-506) | job tracker hangs on to dead task trackers "forever" |  Minor | . | Yoram Arnon | Sanjay Dahiya |
| [HADOOP-444](https://issues.apache.org/jira/browse/HADOOP-444) | In streaming with a NONE reducer, you get duplicate files if a mapper fails, is restarted, and succeeds next time. |  Major | . | Dick King | Michel Tourn |
| [HADOOP-438](https://issues.apache.org/jira/browse/HADOOP-438) | DFS pathname limitation. |  Major | . | Konstantin Shvachko | Wendy Chien |
| [HADOOP-423](https://issues.apache.org/jira/browse/HADOOP-423) | file paths are not normalized |  Major | . | Christian Kunz | Wendy Chien |
| [HADOOP-343](https://issues.apache.org/jira/browse/HADOOP-343) | In case of dead task tracker, the copy mapouts try copying all mapoutputs from this tasktracker |  Major | . | Mahadev konar | Sameer Paranjpye |
| [HADOOP-293](https://issues.apache.org/jira/browse/HADOOP-293) | map reduce job fail without reporting a reason |  Major | . | Yoram Arnon | Owen O'Malley |
| [HADOOP-288](https://issues.apache.org/jira/browse/HADOOP-288) | RFC: Efficient file caching |  Major | . | Michel Tourn | Mahadev konar |
| [HADOOP-261](https://issues.apache.org/jira/browse/HADOOP-261) | when map outputs are lost, nothing is shown in the webapp about why the map task failed |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-255](https://issues.apache.org/jira/browse/HADOOP-255) | Client Calls are not cancelled after a call timeout |  Major | ipc | Naveen Nalam | Owen O'Malley |
| [HADOOP-243](https://issues.apache.org/jira/browse/HADOOP-243) | WI shows progress as 100.00% before actual completion (rounding error) |  Trivial | . | Yoram Arnon | Owen O'Malley |
| [HADOOP-239](https://issues.apache.org/jira/browse/HADOOP-239) | job tracker WI drops jobs after 24 hours |  Minor | . | Yoram Arnon | Sanjay Dahiya |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-548](https://issues.apache.org/jira/browse/HADOOP-548) | add a switch to allow unit tests to show output |  Minor | . | Owen O'Malley | Owen O'Malley |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


