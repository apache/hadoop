
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

## Release 0.5.0 - 2006-08-04

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-425](https://issues.apache.org/jira/browse/HADOOP-425) | a python word count example that runs under jython |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-412](https://issues.apache.org/jira/browse/HADOOP-412) | provide an input format that fetches a subset of sequence file records |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-386](https://issues.apache.org/jira/browse/HADOOP-386) | Periodically move blocks from full nodes to those with space |  Major | . | Johan Oskarsson | Johan Oskarsson |
| [HADOOP-381](https://issues.apache.org/jira/browse/HADOOP-381) | keeping files for tasks that match regex on task id |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-369](https://issues.apache.org/jira/browse/HADOOP-369) | Added ability to copy all part-files into one output file |  Trivial | . | Johan Oskarsson | Johan Oskarsson |
| [HADOOP-359](https://issues.apache.org/jira/browse/HADOOP-359) | add optional compression of map outputs |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-347](https://issues.apache.org/jira/browse/HADOOP-347) | Implement HDFS content browsing interface |  Major | . | Devaraj Das | Devaraj Das |
| [HADOOP-342](https://issues.apache.org/jira/browse/HADOOP-342) | Design/Implement a tool to support archival and analysis of logfiles. |  Major | . | Arun C Murthy |  |
| [HADOOP-339](https://issues.apache.org/jira/browse/HADOOP-339) | making improvements to the jobclients to get information on currenlyl running jobs and the jobqueue |  Minor | . | Mahadev konar | Mahadev konar |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-410](https://issues.apache.org/jira/browse/HADOOP-410) | Using HashMap instead of TreeMap for some maps in Namenode yields 17% performance improvement |  Major | . | Milind Bhandarkar | Milind Bhandarkar |
| [HADOOP-409](https://issues.apache.org/jira/browse/HADOOP-409) | expose JobConf properties as environment variables |  Major | . | Michel Tourn |  |
| [HADOOP-396](https://issues.apache.org/jira/browse/HADOOP-396) | Writable DatanodeID |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-395](https://issues.apache.org/jira/browse/HADOOP-395) | infoPort field should be a DatanodeID member |  Major | . | Konstantin Shvachko | Devaraj Das |
| [HADOOP-394](https://issues.apache.org/jira/browse/HADOOP-394) | MiniDFSCluster shudown order |  Minor | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-392](https://issues.apache.org/jira/browse/HADOOP-392) | Improve the UI for DFS content browsing |  Major | . | Devaraj Das |  |
| [HADOOP-361](https://issues.apache.org/jira/browse/HADOOP-361) | junit with pure-Java hadoopStreaming combiner; remove CRLF in some files |  Major | . | Michel Tourn |  |
| [HADOOP-356](https://issues.apache.org/jira/browse/HADOOP-356) | Build and test hadoopStreaming nightly |  Major | . | Michel Tourn |  |
| [HADOOP-355](https://issues.apache.org/jira/browse/HADOOP-355) | hadoopStreaming: fix APIs, -reduce NONE, StreamSequenceRecordReader |  Major | . | Michel Tourn |  |
| [HADOOP-345](https://issues.apache.org/jira/browse/HADOOP-345) | JobConf access to name-values |  Major | . | Michel Tourn | Michel Tourn |
| [HADOOP-341](https://issues.apache.org/jira/browse/HADOOP-341) | Enhance distcp to handle \*http\* as a 'source protocol'. |  Major | util | Arun C Murthy | Arun C Murthy |
| [HADOOP-340](https://issues.apache.org/jira/browse/HADOOP-340) | Using wildcards in config pathnames |  Minor | conf | Johan Oskarsson | Doug Cutting |
| [HADOOP-335](https://issues.apache.org/jira/browse/HADOOP-335) | factor out the namespace image/transaction log writing |  Major | . | Owen O'Malley | Konstantin Shvachko |
| [HADOOP-321](https://issues.apache.org/jira/browse/HADOOP-321) | DatanodeInfo refactoring |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-302](https://issues.apache.org/jira/browse/HADOOP-302) | class Text (replacement for class UTF8) was: HADOOP-136 |  Major | io | Michel Tourn | Hairong Kuang |
| [HADOOP-260](https://issues.apache.org/jira/browse/HADOOP-260) | the start up scripts should take a command line parameter --config making it easy to run multiple hadoop installation on same machines |  Minor | . | Mahadev konar | Milind Bhandarkar |
| [HADOOP-252](https://issues.apache.org/jira/browse/HADOOP-252) | add versioning to RPC |  Major | ipc | Yoram Arnon |  |
| [HADOOP-237](https://issues.apache.org/jira/browse/HADOOP-237) | Standard set of Performance Metrics for Hadoop |  Major | metrics | Milind Bhandarkar | Milind Bhandarkar |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-415](https://issues.apache.org/jira/browse/HADOOP-415) | DFSNodesStatus() should sort data nodes. |  Major | . | Konstantin Shvachko |  |
| [HADOOP-404](https://issues.apache.org/jira/browse/HADOOP-404) | Regression tests are not working. |  Major | . | Mahadev konar |  |
| [HADOOP-393](https://issues.apache.org/jira/browse/HADOOP-393) | The validateUTF function of class Text throws MalformedInputException for valid UTF8 code containing ascii chars |  Major | io | Hairong Kuang | Hairong Kuang |
| [HADOOP-391](https://issues.apache.org/jira/browse/HADOOP-391) | test-contrib with spaces in classpath (Windows) |  Major | . | Michel Tourn |  |
| [HADOOP-389](https://issues.apache.org/jira/browse/HADOOP-389) | MiniMapReduce tests get stuck because of some timing issues with initialization of tasktrackers. |  Major | . | Mahadev konar | Mahadev konar |
| [HADOOP-388](https://issues.apache.org/jira/browse/HADOOP-388) | the hadoop-daemons.sh fails with "no such file or directory" when used from a relative path |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-387](https://issues.apache.org/jira/browse/HADOOP-387) | LocalJobRunner assigns duplicate mapid's |  Major | . | Sami Siren |  |
| [HADOOP-385](https://issues.apache.org/jira/browse/HADOOP-385) | rcc does not generate correct Java code for the field of a record type |  Major | . | Hairong Kuang | Milind Bhandarkar |
| [HADOOP-384](https://issues.apache.org/jira/browse/HADOOP-384) | improved error messages for file checksum errors |  Minor | fs | Owen O'Malley | Owen O'Malley |
| [HADOOP-383](https://issues.apache.org/jira/browse/HADOOP-383) | unit tests fail on windows |  Major | . | Owen O'Malley | Michel Tourn |
| [HADOOP-380](https://issues.apache.org/jira/browse/HADOOP-380) | The reduce tasks poll for mapoutputs in a loop |  Major | . | Mahadev konar | Mahadev konar |
| [HADOOP-377](https://issues.apache.org/jira/browse/HADOOP-377) | Configuration does not handle URL |  Major | . | Jean-Baptiste Quenot |  |
| [HADOOP-376](https://issues.apache.org/jira/browse/HADOOP-376) | Datanode does not scan for an open http port |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-375](https://issues.apache.org/jira/browse/HADOOP-375) | Introduce a way for datanodes to register their HTTP info ports with the NameNode |  Major | . | Devaraj Das | Devaraj Das |
| [HADOOP-368](https://issues.apache.org/jira/browse/HADOOP-368) | DistributedFSCheck should cleanup, seek, and report missing files. |  Minor | fs | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-365](https://issues.apache.org/jira/browse/HADOOP-365) | datanode crashes on startup with ClassCastException |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-364](https://issues.apache.org/jira/browse/HADOOP-364) | rpc versioning broke out-of-order server launches |  Major | ipc | Owen O'Malley | Owen O'Malley |
| [HADOOP-362](https://issues.apache.org/jira/browse/HADOOP-362) | tasks can get lost when reporting task completion to the JobTracker has an error |  Major | . | Devaraj Das | Owen O'Malley |
| [HADOOP-360](https://issues.apache.org/jira/browse/HADOOP-360) | hadoop-daemon starts but does not stop servers under cygWin |  Major | . | Konstantin Shvachko |  |
| [HADOOP-358](https://issues.apache.org/jira/browse/HADOOP-358) | NPE in Path.equals |  Major | fs | Frédéric Bertin | Doug Cutting |
| [HADOOP-354](https://issues.apache.org/jira/browse/HADOOP-354) | All daemons should have public methods to start and stop them |  Major | . | Barry Kaplan |  |
| [HADOOP-352](https://issues.apache.org/jira/browse/HADOOP-352) | Portability of hadoop shell scripts for deployment |  Major | . | Jean-Baptiste Quenot |  |
| [HADOOP-350](https://issues.apache.org/jira/browse/HADOOP-350) | In standalone mode, 'org.apache.commons.cli cannot be resolved' |  Minor | . | stack |  |
| [HADOOP-344](https://issues.apache.org/jira/browse/HADOOP-344) | TaskTracker passes incorrect file path to DF under cygwin |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-327](https://issues.apache.org/jira/browse/HADOOP-327) | ToolBase calls System.exit |  Major | util | Owen O'Malley | Hairong Kuang |
| [HADOOP-313](https://issues.apache.org/jira/browse/HADOOP-313) | A stand alone driver for individual tasks |  Major | . | Michel Tourn | Michel Tourn |
| [HADOOP-226](https://issues.apache.org/jira/browse/HADOOP-226) | DFSShell problems. Incorrect block replication detection in fsck. |  Major | . | Konstantin Shvachko |  |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-418](https://issues.apache.org/jira/browse/HADOOP-418) | hadoopStreaming test jobconf -\> env.var. mapping |  Major | . | Michel Tourn |  |
| [HADOOP-411](https://issues.apache.org/jira/browse/HADOOP-411) | junit test for HADOOP-59: support generic command-line options |  Major | . | Hairong Kuang | Hairong Kuang |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-351](https://issues.apache.org/jira/browse/HADOOP-351) | Remove Jetty dependency |  Major | ipc | Barry Kaplan | Devaraj Das |
| [HADOOP-307](https://issues.apache.org/jira/browse/HADOOP-307) | Many small jobs benchmark for MapReduce |  Minor | . | Sanjay Dahiya | Sanjay Dahiya |


