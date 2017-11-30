
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

## Release 0.18.3 - 2009-01-29

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-4061](https://issues.apache.org/jira/browse/HADOOP-4061) | Large number of decommission freezes the Namenode |  Major | . | Koji Noguchi | Tsz Wo Nicholas Sze |
| [HADOOP-4659](https://issues.apache.org/jira/browse/HADOOP-4659) | Root cause of connection failure is being lost to code that uses it for delaying startup |  Blocker | ipc | Steve Loughran | Steve Loughran |
| [HADOOP-4997](https://issues.apache.org/jira/browse/HADOOP-4997) | workaround for tmp file handling on DataNodes in 0.18 (HADOOP-4663) |  Blocker | . | Raghu Angadi | Raghu Angadi |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-3780](https://issues.apache.org/jira/browse/HADOOP-3780) | JobTracker should synchronously resolve the tasktracker's network location when the tracker registers |  Major | . | Amar Kamat | Amar Kamat |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-4257](https://issues.apache.org/jira/browse/HADOOP-4257) | TestLeaseRecovery2.testBlockSynchronization failing. |  Blocker | test | Vinod Kumar Vavilapalli | Tsz Wo Nicholas Sze |
| [HADOOP-4499](https://issues.apache.org/jira/browse/HADOOP-4499) | DFSClient should invoke checksumOk only once. |  Blocker | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-4597](https://issues.apache.org/jira/browse/HADOOP-4597) | Under-replicated blocks are not calculated if the name-node is forced out of safe-mode. |  Blocker | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-4610](https://issues.apache.org/jira/browse/HADOOP-4610) | Always calculate mis-replicated blocks when safe-mode is turned off. |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-3121](https://issues.apache.org/jira/browse/HADOOP-3121) | dfs -lsr fail with "Could not get listing " |  Minor | fs | Koji Noguchi | Tsz Wo Nicholas Sze |
| [HADOOP-3883](https://issues.apache.org/jira/browse/HADOOP-3883) | TestFileCreation fails once in a while |  Blocker | test | Lohit Vijayarenu | Tsz Wo Nicholas Sze |
| [HADOOP-4556](https://issues.apache.org/jira/browse/HADOOP-4556) | Block went missing |  Major | . | Robert Chansler | Hairong Kuang |
| [HADOOP-4643](https://issues.apache.org/jira/browse/HADOOP-4643) | NameNode should exclude excessive replicas when counting live replicas for a block |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-4703](https://issues.apache.org/jira/browse/HADOOP-4703) | DataNode.createInterDataNodeProtocolProxy should not wait for proxy forever while recovering lease |  Major | . | Hairong Kuang | Tsz Wo Nicholas Sze |
| [HADOOP-4647](https://issues.apache.org/jira/browse/HADOOP-4647) | NamenodeFsck creates a new DFSClient but never closes it |  Major | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-4616](https://issues.apache.org/jira/browse/HADOOP-4616) | assertion makes fuse-dfs exit when reading incomplete data |  Blocker | . | Marc-Olivier Fleury | Pete Wyckoff |
| [HADOOP-4614](https://issues.apache.org/jira/browse/HADOOP-4614) | "Too many open files" error while processing a large gzip file |  Blocker | . | Abdul Qadeer | Yuri Pradkin |
| [HADOOP-4542](https://issues.apache.org/jira/browse/HADOOP-4542) | Fault in TestDistributedUpgrade |  Minor | test | Robert Chansler | Raghu Angadi |
| [HADOOP-4713](https://issues.apache.org/jira/browse/HADOOP-4713) | librecordio does not scale to large records |  Blocker | record | Christian Kunz | Christian Kunz |
| [HADOOP-4635](https://issues.apache.org/jira/browse/HADOOP-4635) | Memory leak ? |  Blocker | . | Marc-Olivier Fleury | Pete Wyckoff |
| [HADOOP-4714](https://issues.apache.org/jira/browse/HADOOP-4714) | map tasks timing out during merge phase |  Major | . | Christian Kunz | Jothi Padmanabhan |
| [HADOOP-4726](https://issues.apache.org/jira/browse/HADOOP-4726) | documentation typos: "the the" |  Minor | documentation | Tsz Wo Nicholas Sze | Edward J. Yoon |
| [HADOOP-4679](https://issues.apache.org/jira/browse/HADOOP-4679) | Datanode prints tons of log messages: Waiting for threadgroup to exit, active theads is XX |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-4734](https://issues.apache.org/jira/browse/HADOOP-4734) | Some lease recovery codes in 0.19 or trunk should also be committed in 0.18. |  Blocker | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-4746](https://issues.apache.org/jira/browse/HADOOP-4746) | Job output directory should be normalized |  Blocker | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-4654](https://issues.apache.org/jira/browse/HADOOP-4654) | remove temporary output directory of failed tasks |  Major | . | Christian Kunz | Amareshwari Sriramadasu |
| [HADOOP-4717](https://issues.apache.org/jira/browse/HADOOP-4717) | Removal of default port# in NameNode.getUri() cause a map/reduce job failed to prompt temporay output |  Blocker | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-4778](https://issues.apache.org/jira/browse/HADOOP-4778) | Check for zero size block meta file when updating a block. |  Blocker | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-4742](https://issues.apache.org/jira/browse/HADOOP-4742) | Mistake delete replica in hadoop 0.18.1 |  Blocker | . | Wang Xu | Wang Xu |
| [HADOOP-4702](https://issues.apache.org/jira/browse/HADOOP-4702) | Failed block replication leaves an incomplete block in receiver's tmp data directory |  Blocker | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-4613](https://issues.apache.org/jira/browse/HADOOP-4613) | browseBlock.jsp does not generate "genstamp" property. |  Major | . | Konstantin Shvachko | Johan Oskarsson |
| [HADOOP-4806](https://issues.apache.org/jira/browse/HADOOP-4806) | HDFS rename does not work correctly if src contains Java regular expression special characters |  Blocker | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-4795](https://issues.apache.org/jira/browse/HADOOP-4795) | Lease monitor may get into an infinite loop |  Blocker | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-4822](https://issues.apache.org/jira/browse/HADOOP-4822) | 0.18 cannot be compiled in Java 5. |  Blocker | util | Tsz Wo Nicholas Sze |  |
| [HADOOP-4620](https://issues.apache.org/jira/browse/HADOOP-4620) | Streaming mapper never completes if the mapper does not write to stdout |  Major | . | Runping Qi | Ravi Gummadi |
| [HADOOP-4810](https://issues.apache.org/jira/browse/HADOOP-4810) | Data lost at cluster startup time |  Blocker | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-4797](https://issues.apache.org/jira/browse/HADOOP-4797) | RPC Server can leave a lot of direct buffers |  Blocker | ipc | Raghu Angadi | Raghu Angadi |
| [HADOOP-4840](https://issues.apache.org/jira/browse/HADOOP-4840) | TestNodeCount sometimes fails with NullPointerException |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-4904](https://issues.apache.org/jira/browse/HADOOP-4904) | Deadlock while leaving safe mode. |  Blocker | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-4919](https://issues.apache.org/jira/browse/HADOOP-4919) | [HOD] Provide execute access to JT history directory path for group |  Major | contrib/hod | Hemanth Yamijala | Peeyush Bishnoi |
| [HADOOP-1980](https://issues.apache.org/jira/browse/HADOOP-1980) | 'dfsadmin -safemode enter' should prevent the namenode from leaving safemode automatically after startup |  Minor | . | Koji Noguchi | Konstantin Shvachko |
| [HADOOP-4924](https://issues.apache.org/jira/browse/HADOOP-4924) | Race condition in re-init of TaskTracker |  Blocker | . | Devaraj Das | Devaraj Das |
| [HADOOP-4935](https://issues.apache.org/jira/browse/HADOOP-4935) | Manual leaving of safe mode may lead to data lost |  Major | . | Hairong Kuang | Konstantin Shvachko |
| [HADOOP-4951](https://issues.apache.org/jira/browse/HADOOP-4951) | Lease monitor does not own the LeaseManager lock in changing leases. |  Blocker | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-4961](https://issues.apache.org/jira/browse/HADOOP-4961) | ConcurrentModificationException in lease recovery of empty files. |  Blocker | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-4971](https://issues.apache.org/jira/browse/HADOOP-4971) | Block report times from datanodes could converge to same time. |  Blocker | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-4910](https://issues.apache.org/jira/browse/HADOOP-4910) | NameNode should exclude corrupt replicas when choosing excessive replicas to delete |  Blocker | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-5077](https://issues.apache.org/jira/browse/HADOOP-5077) | JavaDoc errors in 0.18.3 |  Blocker | util | Raghu Angadi | Raghu Angadi |
| [HADOOP-4983](https://issues.apache.org/jira/browse/HADOOP-4983) | Job counters sometimes go down as tasks run without task failures |  Critical | . | Owen O'Malley | Amareshwari Sriramadasu |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-4823](https://issues.apache.org/jira/browse/HADOOP-4823) | Should not use java.util.NavigableMap in 0.18 |  Major | util | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-4824](https://issues.apache.org/jira/browse/HADOOP-4824) | Should not use File.setWritable(..) in 0.18 |  Major | . | Tsz Wo Nicholas Sze | Hairong Kuang |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-4150](https://issues.apache.org/jira/browse/HADOOP-4150) | Include librecordio as part of the release |  Blocker | build | Koji Noguchi | Giridharan Kesavan |


