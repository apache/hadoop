
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

## Release 0.12.0 - 2007-03-02



### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-491](https://issues.apache.org/jira/browse/HADOOP-491) | streaming jobs should allow programs that don't do any IO for a long time |  Major | . | arkady borkovsky | Arun C Murthy |
| [HADOOP-492](https://issues.apache.org/jira/browse/HADOOP-492) | Global counters |  Major | . | arkady borkovsky | David Bowen |
| [HADOOP-1032](https://issues.apache.org/jira/browse/HADOOP-1032) | Support for caching Job JARs |  Minor | . | Gautam Kowshik | Gautam Kowshik |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-975](https://issues.apache.org/jira/browse/HADOOP-975) | Separation of user tasks' stdout and stderr streams |  Major | . | Arun C Murthy | Arun C Murthy |
| [HADOOP-982](https://issues.apache.org/jira/browse/HADOOP-982) | A couple setter functions and toString method for BytesWritable. |  Major | io | Owen O'Malley | Owen O'Malley |
| [HADOOP-858](https://issues.apache.org/jira/browse/HADOOP-858) | clean up smallJobsBenchmark and move to src/test/org/apache/hadoop/mapred |  Minor | build | Nigel Daley | Nigel Daley |
| [HADOOP-954](https://issues.apache.org/jira/browse/HADOOP-954) | Metrics should offer complete set of static report methods or none at all |  Minor | metrics | Nigel Daley | David Bowen |
| [HADOOP-882](https://issues.apache.org/jira/browse/HADOOP-882) | S3FileSystem should retry if there is a communication problem with S3 |  Major | fs | Tom White | Tom White |
| [HADOOP-977](https://issues.apache.org/jira/browse/HADOOP-977) | The output from the user's task should be tagged and sent to the resepective console streams. |  Major | . | Owen O'Malley | Arun C Murthy |
| [HADOOP-1007](https://issues.apache.org/jira/browse/HADOOP-1007) | Names used for map, reduce, and shuffle metrics should be unique |  Trivial | metrics | Nigel Daley | Nigel Daley |
| [HADOOP-889](https://issues.apache.org/jira/browse/HADOOP-889) | DFS unit tests have duplicate code |  Minor | test | Doug Cutting | Milind Bhandarkar |
| [HADOOP-943](https://issues.apache.org/jira/browse/HADOOP-943) | fsck to show the filename of the corrupted file |  Trivial | . | Koji Noguchi | dhruba borthakur |
| [HADOOP-333](https://issues.apache.org/jira/browse/HADOOP-333) | we should have some checks that the sort benchmark generates correct outputs |  Major | . | Owen O'Malley | Arun C Murthy |
| [HADOOP-1017](https://issues.apache.org/jira/browse/HADOOP-1017) | Optimization: Reduce Overhead from ReflectionUtils.newInstance |  Major | util | Ron Bodkin |  |
| [HADOOP-867](https://issues.apache.org/jira/browse/HADOOP-867) | job client should generate input fragments before the job is submitted |  Major | . | Owen O'Malley |  |
| [HADOOP-952](https://issues.apache.org/jira/browse/HADOOP-952) | Create a public (shared) Hadoop EC2 AMI |  Major | scripts | Tom White | Tom White |
| [HADOOP-1025](https://issues.apache.org/jira/browse/HADOOP-1025) | remove dead code in Server.java |  Minor | ipc | Doug Cutting | Doug Cutting |
| [HADOOP-997](https://issues.apache.org/jira/browse/HADOOP-997) | Implement S3 retry mechanism for failed block transfers |  Major | fs | Tom White | Tom White |
| [HADOOP-1030](https://issues.apache.org/jira/browse/HADOOP-1030) | in unit tests, set ipc timeout in one place |  Minor | test | Doug Cutting | Doug Cutting |
| [HADOOP-985](https://issues.apache.org/jira/browse/HADOOP-985) | Namenode should identify DataNodes as ip:port instead of hostname:port |  Major | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-248](https://issues.apache.org/jira/browse/HADOOP-248) | locating map outputs via random probing is inefficient |  Major | . | Owen O'Malley | Devaraj Das |
| [HADOOP-1040](https://issues.apache.org/jira/browse/HADOOP-1040) | Improvement of RandomWriter example to use custom InputFormat, OutputFormat, and Counters |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-928](https://issues.apache.org/jira/browse/HADOOP-928) | make checksums optional per FileSystem |  Major | fs | Doug Cutting | Hairong Kuang |
| [HADOOP-1042](https://issues.apache.org/jira/browse/HADOOP-1042) | Improve the handling of failed map output fetches |  Major | . | Devaraj Das | Devaraj Das |
| [HADOOP-972](https://issues.apache.org/jira/browse/HADOOP-972) | Improve the rack-aware replica placement performance |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-1043](https://issues.apache.org/jira/browse/HADOOP-1043) | Optimize the shuffle phase (increase the parallelism) |  Major | . | Devaraj Das | Devaraj Das |
| [HADOOP-940](https://issues.apache.org/jira/browse/HADOOP-940) | pendingReplications of FSNamesystem is not informative |  Major | . | Hairong Kuang | dhruba borthakur |
| [HADOOP-941](https://issues.apache.org/jira/browse/HADOOP-941) | Enhancements to Hadoop record I/O - Part 1 |  Major | record | Milind Bhandarkar | Milind Bhandarkar |
| [HADOOP-1041](https://issues.apache.org/jira/browse/HADOOP-1041) | Counter names are ugly |  Major | . | Owen O'Malley | David Bowen |
| [HADOOP-432](https://issues.apache.org/jira/browse/HADOOP-432) | support undelete, snapshots, or other mechanism to recover lost files |  Major | . | Yoram Arnon | Doug Cutting |
| [HADOOP-1033](https://issues.apache.org/jira/browse/HADOOP-1033) | Rewrite AmazonEC2 wiki page |  Minor | scripts | Tom White | Tom White |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-992](https://issues.apache.org/jira/browse/HADOOP-992) | The mini/mr cluster for testing always uses the local file system rather than the namenode that was passed in |  Major | test | Owen O'Malley | Owen O'Malley |
| [HADOOP-893](https://issues.apache.org/jira/browse/HADOOP-893) | dead datanode set should be maintained in the file handle or file system for hdfs |  Major | . | Owen O'Malley | Raghu Angadi |
| [HADOOP-761](https://issues.apache.org/jira/browse/HADOOP-761) | Unit tests should cleanup created files in /tmp. It causes tests to fail if more than one users run tests on same machine. |  Minor | test | Sanjay Dahiya | Nigel Daley |
| [HADOOP-1010](https://issues.apache.org/jira/browse/HADOOP-1010) | getReordReader methof of InputFormat class should handle null reporter argument |  Major | . | Runping Qi | Runping Qi |
| [HADOOP-923](https://issues.apache.org/jira/browse/HADOOP-923) | DFS Scalability: datanode heartbeat timeouts cause cascading timeouts of other datanodes |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-476](https://issues.apache.org/jira/browse/HADOOP-476) | Streaming should check for correctness of the task |  Major | . | arkady borkovsky | Arun C Murthy |
| [HADOOP-973](https://issues.apache.org/jira/browse/HADOOP-973) | NPE in FSDataset during heavy Namenode load |  Major | . | Nigel Daley | dhruba borthakur |
| [HADOOP-649](https://issues.apache.org/jira/browse/HADOOP-649) | Jobs without any map and reduce operations seems to be lost after their execution |  Major | . | Thomas Friol | Owen O'Malley |
| [HADOOP-803](https://issues.apache.org/jira/browse/HADOOP-803) | Reducing memory consumption on Namenode : Part 1 |  Major | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-1021](https://issues.apache.org/jira/browse/HADOOP-1021) | TestMiniMRLocalFS and TestMiniMRCaching broken on Windows |  Major | test | Nigel Daley |  |
| [HADOOP-947](https://issues.apache.org/jira/browse/HADOOP-947) | isReplicationInProgress() is very heavyweight |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-442](https://issues.apache.org/jira/browse/HADOOP-442) | slaves file should include an 'exclude' section, to prevent "bad" datanodes and tasktrackers from disrupting  a cluster |  Major | conf | Yoram Arnon | Wendy Chien |
| [HADOOP-933](https://issues.apache.org/jira/browse/HADOOP-933) | Application defined InputSplits do not work |  Major | . | Benjamin Reed | Owen O'Malley |
| [HADOOP-1006](https://issues.apache.org/jira/browse/HADOOP-1006) | The "-local" option does work properly with test programs |  Minor | test | Gautam Kowshik | Doug Cutting |
| [HADOOP-990](https://issues.apache.org/jira/browse/HADOOP-990) | Datanode doesn't retry when write to one (full)drive fail |  Major | . | Koji Noguchi | Raghu Angadi |
| [HADOOP-564](https://issues.apache.org/jira/browse/HADOOP-564) | we should use hdfs:// in all API URIs |  Major | . | eric baldeschwieler | Wendy Chien |
| [HADOOP-654](https://issues.apache.org/jira/browse/HADOOP-654) | jobs fail with some hardware/system failures on a small number of nodes |  Minor | . | Yoram Arnon | Arun C Murthy |
| [HADOOP-1029](https://issues.apache.org/jira/browse/HADOOP-1029) | streaming doesn't work with multiple maps |  Blocker | . | Arun C Murthy | Arun C Murthy |
| [HADOOP-1034](https://issues.apache.org/jira/browse/HADOOP-1034) | RuntimeException and Error not catched in DataNode.DataXceiver.run() |  Major | . | Philippe Gassmann |  |
| [HADOOP-878](https://issues.apache.org/jira/browse/HADOOP-878) | reducer NONE does not work with multiple maps |  Minor | . | Mahadev konar | Arun C Murthy |
| [HADOOP-1039](https://issues.apache.org/jira/browse/HADOOP-1039) | Reduce the time taken by TestCheckpoint |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-1027](https://issues.apache.org/jira/browse/HADOOP-1027) | Fix the RAM FileSystem/Merge problems (reported in HADOOP-1014) |  Major | . | Devaraj Das | Devaraj Das |
| [HADOOP-1036](https://issues.apache.org/jira/browse/HADOOP-1036) | task gets lost during assignment |  Critical | . | Owen O'Malley | Arun C Murthy |
| [HADOOP-1044](https://issues.apache.org/jira/browse/HADOOP-1044) | TestDecommission fails because it attempts to transfer block to a dead datanode |  Major | test | Wendy Chien | Wendy Chien |
| [HADOOP-109](https://issues.apache.org/jira/browse/HADOOP-109) | Blocks are not replicated when... |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-1020](https://issues.apache.org/jira/browse/HADOOP-1020) | Path class on Windows seems broken |  Major | . | Nigel Daley | Doug Cutting |
| [HADOOP-1000](https://issues.apache.org/jira/browse/HADOOP-1000) | Loggers in the Task framework should not write the the Tasks stderr |  Major | . | Owen O'Malley | Arun C Murthy |
| [HADOOP-1037](https://issues.apache.org/jira/browse/HADOOP-1037) | bin/slaves.sh not compatible with /bin/dash |  Major | . | Doug Cutting |  |
| [HADOOP-1046](https://issues.apache.org/jira/browse/HADOOP-1046) | Datanode should periodically clean up /tmp from partially received (and not completed) block files |  Major | . | Andrzej Bialecki | Andrzej Bialecki |
| [HADOOP-1049](https://issues.apache.org/jira/browse/HADOOP-1049) | race condition in setting up ipc connections |  Major | ipc | Owen O'Malley | Devaraj Das |
| [HADOOP-1056](https://issues.apache.org/jira/browse/HADOOP-1056) | Decommission only recognizes IP addesses in hosts and exclude files on refresh. |  Major | . | Wendy Chien | Wendy Chien |
| [HADOOP-994](https://issues.apache.org/jira/browse/HADOOP-994) | DFS Scalability : a BlockReport that returns large number of blocks-to-be-deleted cause datanode to lost connectivity to namenode |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-338](https://issues.apache.org/jira/browse/HADOOP-338) | the number of maps in the JobConf does not match reality |  Major | . | Owen O'Malley | Owen O'Malley |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-1051](https://issues.apache.org/jira/browse/HADOOP-1051) | Add checkstyle target to ant build file |  Major | build, test | Tom White | Tom White |


