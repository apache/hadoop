
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

## Release 0.12.1 - 2007-03-17



### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-1053](https://issues.apache.org/jira/browse/HADOOP-1053) | Make Record I/O functionally modular from the rest of Hadoop |  Major | record | Milind Bhandarkar | Milind Bhandarkar |
| [HADOOP-1096](https://issues.apache.org/jira/browse/HADOOP-1096) | Rename InputArchive and OutputArchive and make them public |  Major | record | Milind Bhandarkar | Milind Bhandarkar |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-1035](https://issues.apache.org/jira/browse/HADOOP-1035) | StackOverflowError in FSDataSet |  Blocker | . | Philippe Gassmann | Raghu Angadi |
| [HADOOP-1067](https://issues.apache.org/jira/browse/HADOOP-1067) | Compile fails if Checkstyle jar is present in lib directory |  Major | build | Tom White | Tom White |
| [HADOOP-1060](https://issues.apache.org/jira/browse/HADOOP-1060) | IndexOutOfBoundsException in JobInProgress.updateTaskStatus leads to hung jobs |  Blocker | . | Arun C Murthy | Arun C Murthy |
| [HADOOP-1077](https://issues.apache.org/jira/browse/HADOOP-1077) | Race condition in fetching map outputs (might lead to hung reduces) |  Blocker | . | Devaraj Das | Devaraj Das |
| [HADOOP-1083](https://issues.apache.org/jira/browse/HADOOP-1083) | Replication not occuring after cluster restart when datanodes missing |  Blocker | . | Nigel Daley | Hairong Kuang |
| [HADOOP-1082](https://issues.apache.org/jira/browse/HADOOP-1082) | NullpointerException in ChecksumFileSystem$FSInputChecker.seek |  Blocker | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-1088](https://issues.apache.org/jira/browse/HADOOP-1088) | Csv and Xml serialization for buffers do not work for byte value of -1 |  Blocker | record | Milind Bhandarkar | Milind Bhandarkar |
| [HADOOP-1080](https://issues.apache.org/jira/browse/HADOOP-1080) | Cygwin path translation should occur earlier in bin/hadoop |  Major | scripts | Andrzej Bialecki |  |
| [HADOOP-1091](https://issues.apache.org/jira/browse/HADOOP-1091) |   NPE from Simon in JT stdout |  Major | . | David Bowen | David Bowen |
| [HADOOP-1092](https://issues.apache.org/jira/browse/HADOOP-1092) | NullPointerException in HeartbeatMonitor thread |  Blocker | . | Nigel Daley | Hairong Kuang |
| [HADOOP-1112](https://issues.apache.org/jira/browse/HADOOP-1112) | Race condition in Hadoop metrics |  Major | . | David Bowen |  |
| [HADOOP-1108](https://issues.apache.org/jira/browse/HADOOP-1108) | Checksumed file system should  retry reading if a different replica is found when handle ChecksumException |  Blocker | . | dhruba borthakur | Hairong Kuang |
| [HADOOP-1070](https://issues.apache.org/jira/browse/HADOOP-1070) | Number of racks and datanode double temporarily when upgrading from 0.10.1 to 0.11.2 |  Blocker | . | Nigel Daley | Konstantin Shvachko |
| [HADOOP-1099](https://issues.apache.org/jira/browse/HADOOP-1099) | NullPointerException in JobInProgress.getTaskInProgress |  Major | . | Nigel Daley | Gautam Kowshik |
| [HADOOP-1115](https://issues.apache.org/jira/browse/HADOOP-1115) | copyToLocal doesn't copy directories |  Blocker | . | Nigel Daley |  |
| [HADOOP-1109](https://issues.apache.org/jira/browse/HADOOP-1109) | Streaming, NPE when reading sequencefile |  Major | . | Koji Noguchi |  |
| [HADOOP-1117](https://issues.apache.org/jira/browse/HADOOP-1117) | DFS Scalability: When the namenode is restarted it consumes 80% CPU |  Blocker | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-1089](https://issues.apache.org/jira/browse/HADOOP-1089) | The c++ version of write and read v-int don't agree with the java versions |  Major | record | Owen O'Malley | Milind Bhandarkar |
| [HADOOP-1128](https://issues.apache.org/jira/browse/HADOOP-1128) | Missing progress information in map tasks |  Major | . | Andrzej Bialecki | Andrzej Bialecki |
| [HADOOP-1129](https://issues.apache.org/jira/browse/HADOOP-1129) | The DFSClient hides IOExceptions in flush |  Major | . | Owen O'Malley | Hairong Kuang |
| [HADOOP-1126](https://issues.apache.org/jira/browse/HADOOP-1126) | Optimize CPU usage when cluster restarts |  Major | . | dhruba borthakur | Hairong Kuang |


