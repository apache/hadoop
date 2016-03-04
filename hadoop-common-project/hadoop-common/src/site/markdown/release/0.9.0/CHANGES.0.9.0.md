
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

## Release 0.9.0 - 2006-12-01

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-611](https://issues.apache.org/jira/browse/HADOOP-611) | SequenceFile.Sorter should have a merge method that returns an iterator |  Major | io | Owen O'Malley | Devaraj Das |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-725](https://issues.apache.org/jira/browse/HADOOP-725) | chooseTargets method in FSNamesystem is very inefficient |  Major | . | Milind Bhandarkar | Milind Bhandarkar |
| [HADOOP-721](https://issues.apache.org/jira/browse/HADOOP-721) | jobconf.jsp shouldn't find the jobconf.xsl via http |  Major | . | Owen O'Malley | Arun C Murthy |
| [HADOOP-689](https://issues.apache.org/jira/browse/HADOOP-689) | hadoop should provide a common way to wrap instances with different types into one type |  Major | io | Feng Jiang |  |
| [HADOOP-688](https://issues.apache.org/jira/browse/HADOOP-688) | move dfs administrative interfaces to a separate command |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-677](https://issues.apache.org/jira/browse/HADOOP-677) | RPC should send a fixed header and version at the start of connection |  Major | ipc | Owen O'Malley | Owen O'Malley |
| [HADOOP-668](https://issues.apache.org/jira/browse/HADOOP-668) | improvement to DFS browsing WI |  Minor | . | Yoram Arnon | Hairong Kuang |
| [HADOOP-661](https://issues.apache.org/jira/browse/HADOOP-661) | JobConf for a job should be viewable from the web/ui |  Major | . | Owen O'Malley | Arun C Murthy |
| [HADOOP-655](https://issues.apache.org/jira/browse/HADOOP-655) | remove deprecations |  Minor | . | Doug Cutting | Doug Cutting |
| [HADOOP-613](https://issues.apache.org/jira/browse/HADOOP-613) | The final merge on the reduces should feed the reduce directly |  Major | . | Owen O'Malley | Devaraj Das |
| [HADOOP-565](https://issues.apache.org/jira/browse/HADOOP-565) | Upgrade Jetty to 6.x |  Major | . | Owen O'Malley | Sanjay Dahiya |
| [HADOOP-538](https://issues.apache.org/jira/browse/HADOOP-538) | Implement a nio's 'direct buffer' based wrapper over zlib to improve performance of java.util.zip.{De\|In}flater as a 'custom codec' |  Major | io | Arun C Murthy | Arun C Murthy |
| [HADOOP-489](https://issues.apache.org/jira/browse/HADOOP-489) | Seperating user logs from system logs in map reduce |  Minor | . | Mahadev konar | Arun C Murthy |
| [HADOOP-76](https://issues.apache.org/jira/browse/HADOOP-76) | Implement speculative re-execution of reduces |  Minor | . | Doug Cutting | Sanjay Dahiya |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-750](https://issues.apache.org/jira/browse/HADOOP-750) | race condition on stalled map output fetches |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-749](https://issues.apache.org/jira/browse/HADOOP-749) | The jobfailures.jsp gets a NullPointerException after a task tracker has been lost |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-747](https://issues.apache.org/jira/browse/HADOOP-747) | RecordIO compiler does not produce correct Java code when buffer is used as key or value in map |  Major | record | Milind Bhandarkar | Milind Bhandarkar |
| [HADOOP-741](https://issues.apache.org/jira/browse/HADOOP-741) | Fix average progress calculation in speculative execution of maps + fix pending issues in speculative execution of reduces |  Major | . | Sanjay Dahiya | Sanjay Dahiya |
| [HADOOP-739](https://issues.apache.org/jira/browse/HADOOP-739) | TestIPC occassionally fails with BindException |  Minor | test | Nigel Daley | Nigel Daley |
| [HADOOP-736](https://issues.apache.org/jira/browse/HADOOP-736) | Roll back Jetty6.0.1 to Jetty5.1.4 |  Major | . | Devaraj Das | Devaraj Das |
| [HADOOP-734](https://issues.apache.org/jira/browse/HADOOP-734) | Link to the FAQ in the "Documentation" section of the hadoop website |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-733](https://issues.apache.org/jira/browse/HADOOP-733) | dfs shell has inconsistent exit codes |  Major | . | Christian Kunz | dhruba borthakur |
| [HADOOP-729](https://issues.apache.org/jira/browse/HADOOP-729) | packageNativeHadoop.sh has non-standard sh code |  Critical | scripts | Arun C Murthy | Arun C Murthy |
| [HADOOP-728](https://issues.apache.org/jira/browse/HADOOP-728) | Map-reduce task does not produce correct results when -reducer NONE is specified through streaming |  Major | . | dhruba borthakur | Sanjay Dahiya |
| [HADOOP-724](https://issues.apache.org/jira/browse/HADOOP-724) | bin/hadoop:111 uses java directly, it should use JAVA\_HOME |  Minor | scripts | Sanjay Dahiya | Arun C Murthy |
| [HADOOP-723](https://issues.apache.org/jira/browse/HADOOP-723) | Race condition exists in the method MapOutputLocation.getFile |  Major | . | Devaraj Das | Owen O'Malley |
| [HADOOP-722](https://issues.apache.org/jira/browse/HADOOP-722) | native-hadoop deficiencies |  Critical | . | Arun C Murthy | Arun C Murthy |
| [HADOOP-716](https://issues.apache.org/jira/browse/HADOOP-716) | Javadoc warning in SequenceFile.java |  Minor | io | Devaraj Das | Devaraj Das |
| [HADOOP-715](https://issues.apache.org/jira/browse/HADOOP-715) | build.xml sets up wrong 'hadoop.log.dir' property for 'ant test' |  Minor | test | Arun C Murthy | Arun C Murthy |
| [HADOOP-712](https://issues.apache.org/jira/browse/HADOOP-712) | Record-IO XML serialization is broken for control characters |  Major | record | Milind Bhandarkar | Milind Bhandarkar |
| [HADOOP-710](https://issues.apache.org/jira/browse/HADOOP-710) | block size isn't honored |  Major | . | Nigel Daley | Milind Bhandarkar |
| [HADOOP-709](https://issues.apache.org/jira/browse/HADOOP-709) | streaming job with Control characters in the command causes runtime exception in the job tracker |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-708](https://issues.apache.org/jira/browse/HADOOP-708) | test-libhdfs.sh does not properly capture and return error status |  Major | test | Nigel Daley | Nigel Daley |
| [HADOOP-705](https://issues.apache.org/jira/browse/HADOOP-705) | IOException: job.xml already exists |  Major | . | Nigel Daley | Mahadev konar |
| [HADOOP-699](https://issues.apache.org/jira/browse/HADOOP-699) | "Browse the filesystem" link on Name Node redirects to wrong port on DataNode |  Major | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-698](https://issues.apache.org/jira/browse/HADOOP-698) | When DFS client fails to read from a datanode, the failed datanode is not excluded from target reselection |  Major | . | Hairong Kuang | Milind Bhandarkar |
| [HADOOP-696](https://issues.apache.org/jira/browse/HADOOP-696) | TestTextInputFormat fails on some platforms due to non-determinism in format.getSplits() |  Minor | test | Sameer Paranjpye | Sameer Paranjpye |
| [HADOOP-695](https://issues.apache.org/jira/browse/HADOOP-695) | Unexpected NPE from the next method of StreamLineRecordReader fails map/reduce jobs |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-694](https://issues.apache.org/jira/browse/HADOOP-694) | jobtracker expireluanching tasks throws out Nullpointer exceptions |  Minor | . | Mahadev konar | Mahadev konar |
| [HADOOP-687](https://issues.apache.org/jira/browse/HADOOP-687) | Upgrade to Jetty 6 does not patch bin/hadoop |  Critical | scripts | Sameer Paranjpye | Sameer Paranjpye |
| [HADOOP-683](https://issues.apache.org/jira/browse/HADOOP-683) | bin/hadoop.sh doesn't work for /bin/dash (eg ubuntu 6.10b) |  Trivial | scripts | James Todd |  |
| [HADOOP-682](https://issues.apache.org/jira/browse/HADOOP-682) | hadoop namenode -format doesnt work anymore if target directory doesnt exist |  Major | . | Sanjay Dahiya |  |
| [HADOOP-652](https://issues.apache.org/jira/browse/HADOOP-652) | Not all Datastructures are updated when a block is deleted |  Major | . | Raghu Angadi |  |
| [HADOOP-646](https://issues.apache.org/jira/browse/HADOOP-646) | name node server does not load large (\> 2^31 bytes) edits file |  Critical | . | Christian Kunz | Milind Bhandarkar |
| [HADOOP-645](https://issues.apache.org/jira/browse/HADOOP-645) | Map-reduce task does not finish correctly when -reducer NONE is specified |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-637](https://issues.apache.org/jira/browse/HADOOP-637) | ipc.Server has memory leak -- serious issue for namenode server |  Major | ipc | Christian Kunz | Raghu Angadi |
| [HADOOP-604](https://issues.apache.org/jira/browse/HADOOP-604) | DataNodes get NullPointerException and become unresponsive on 50010 |  Major | . | Christian Kunz | Raghu Angadi |
| [HADOOP-459](https://issues.apache.org/jira/browse/HADOOP-459) | libhdfs leaks memory when writing to files |  Major | . | Christian Kunz | Sameer Paranjpye |
| [HADOOP-447](https://issues.apache.org/jira/browse/HADOOP-447) | DistributedFileSystem.getBlockSize(Path) does not resolve absolute path |  Major | . | Benjamin Reed | Raghu Angadi |
| [HADOOP-430](https://issues.apache.org/jira/browse/HADOOP-430) | http server should die if the Datanode fails. |  Major | . | Konstantin Shvachko | Wendy Chien |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-382](https://issues.apache.org/jira/browse/HADOOP-382) | add a unit test for multiple datanodes in a machine |  Major | . | Yoram Arnon | Milind Bhandarkar |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


