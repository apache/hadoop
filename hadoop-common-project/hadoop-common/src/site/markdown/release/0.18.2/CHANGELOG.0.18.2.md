
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

## Release 0.18.2 - 2008-11-03

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-4228](https://issues.apache.org/jira/browse/HADOOP-4228) | dfs datanode metrics, bytes\_read, bytes\_written overflows due to incorrect type used. |  Blocker | metrics | Eric Yang | Eric Yang |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-2421](https://issues.apache.org/jira/browse/HADOOP-2421) | Release JDiff report of changes between different versions of Hadoop |  Minor | documentation | Nigel Daley | Doug Cutting |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-4277](https://issues.apache.org/jira/browse/HADOOP-4277) | Checksum verification is disabled for LocalFS |  Blocker | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-4271](https://issues.apache.org/jira/browse/HADOOP-4271) | Bug in FSInputChecker makes it possible to read from an invalid buffer |  Blocker | fs | Ning Li | Ning Li |
| [HADOOP-3614](https://issues.apache.org/jira/browse/HADOOP-3614) | TestLeaseRecovery fails when run with assertions enabled. |  Blocker | . | Konstantin Shvachko | Tsz Wo Nicholas Sze |
| [HADOOP-4314](https://issues.apache.org/jira/browse/HADOOP-4314) | TestReplication fails quite often |  Blocker | test | Raghu Angadi | Raghu Angadi |
| [HADOOP-4326](https://issues.apache.org/jira/browse/HADOOP-4326) | ChecksumFileSystem does not override all create(...) methods |  Blocker | fs | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-4395](https://issues.apache.org/jira/browse/HADOOP-4395) | Reloading FSImage and FSEditLog may erase user and group information |  Blocker | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-4351](https://issues.apache.org/jira/browse/HADOOP-4351) | ArrayIndexOutOfBoundsException during fsck |  Blocker | . | Brian Bockelman | Hairong Kuang |
| [HADOOP-4407](https://issues.apache.org/jira/browse/HADOOP-4407) | HADOOP-4395 should use a Java 1.5 API for 0.18 |  Blocker | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-4292](https://issues.apache.org/jira/browse/HADOOP-4292) | append() does not work for LocalFileSystem |  Blocker | fs | Raghu Angadi | Hairong Kuang |
| [HADOOP-3217](https://issues.apache.org/jira/browse/HADOOP-3217) | [HOD] Be less agressive when querying job status from resource manager. |  Blocker | contrib/hod | Hemanth Yamijala | Hemanth Yamijala |
| [HADOOP-3786](https://issues.apache.org/jira/browse/HADOOP-3786) | Changes in HOD documentation |  Blocker | documentation | Suman Sehgal | Vinod Kumar Vavilapalli |
| [HADOOP-4399](https://issues.apache.org/jira/browse/HADOOP-4399) |  fuse-dfs per FD context is not thread safe and can cause segfaults and corruptions |  Blocker | . | Pete Wyckoff | Pete Wyckoff |
| [HADOOP-4369](https://issues.apache.org/jira/browse/HADOOP-4369) | Metric Averages are not averages |  Blocker | metrics | Brian Bockelman | Brian Bockelman |
| [HADOOP-4469](https://issues.apache.org/jira/browse/HADOOP-4469) | ant jar file not being included in tar distribution |  Blocker | build | Nigel Daley | Nigel Daley |
| [HADOOP-3914](https://issues.apache.org/jira/browse/HADOOP-3914) | checksumOk implementation in DFSClient can break applications |  Blocker | . | Christian Kunz | Christian Kunz |
| [HADOOP-4467](https://issues.apache.org/jira/browse/HADOOP-4467) | SerializationFactory should use current context ClassLoader |  Blocker | . | Chris K Wensel | Chris K Wensel |
| [HADOOP-4517](https://issues.apache.org/jira/browse/HADOOP-4517) | unstable dfs when running jobs on 0.18.1 |  Blocker | . | Christian Kunz | Tsz Wo Nicholas Sze |
| [HADOOP-4526](https://issues.apache.org/jira/browse/HADOOP-4526) | fsck failing with NullPointerException  (return value 0) |  Major | . | Koji Noguchi | Hairong Kuang |
| [HADOOP-4533](https://issues.apache.org/jira/browse/HADOOP-4533) | HDFS client of hadoop 0.18.1 and HDFS server 0.18.2 (0.18 branch) not compatible |  Blocker | . | Runping Qi | Hairong Kuang |
| [HADOOP-4483](https://issues.apache.org/jira/browse/HADOOP-4483) | getBlockArray in DatanodeDescriptor does not honor passed in maxblocks value |  Critical | . | Ahad Rana | Ahad Rana |
| [HADOOP-4340](https://issues.apache.org/jira/browse/HADOOP-4340) | "hadoop jar" always returns exit code 0 (success) to the shell when jar throws a fatal exception |  Major | . | David Litster | Arun C Murthy |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-4403](https://issues.apache.org/jira/browse/HADOOP-4403) | TestLeaseRecovery.testBlockSynchronization failed on trunk |  Blocker | test | Hemanth Yamijala | Tsz Wo Nicholas Sze |


