
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
# Apache Hadoop  0.18.3 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-5077](https://issues.apache.org/jira/browse/HADOOP-5077) | *Blocker* | **JavaDoc errors in 0.18.3**

Fix couple of JavaDoc warnings.


---

* [HADOOP-4997](https://issues.apache.org/jira/browse/HADOOP-4997) | *Blocker* | **workaround for tmp file handling on DataNodes in 0.18 (HADOOP-4663)**

Revert tmp files handling on DataNodes back to 0.17. sync() introduced in 0.18 has less gaurantees.


---

* [HADOOP-4971](https://issues.apache.org/jira/browse/HADOOP-4971) | *Blocker* | **Block report times from datanodes could converge to same time.**

A long (unexpected) delay at datanodes could make subsequent block reports from many datanode at the same time.


---

* [HADOOP-4797](https://issues.apache.org/jira/browse/HADOOP-4797) | *Blocker* | **RPC Server can leave a lot of direct buffers**

Improve how RPC server reads and writes large buffers. Avoids soft-leak of direct buffers and excess copies in NIO layer.


---

* [HADOOP-4679](https://issues.apache.org/jira/browse/HADOOP-4679) | *Major* | **Datanode prints tons of log messages: Waiting for threadgroup to exit, active theads is XX**

1. Only datanode's offerService thread shutdown the datanode to avoid deadlock;
2. Datanode checks disk in case of failure on creating a block file.


---

* [HADOOP-4659](https://issues.apache.org/jira/browse/HADOOP-4659) | *Blocker* | **Root cause of connection failure is being lost to code that uses it for delaying startup**

**WARNING: No release note provided for this incompatible change.**


---

* [HADOOP-4635](https://issues.apache.org/jira/browse/HADOOP-4635) | *Blocker* | **Memory leak ?**

fix memory leak of user/group information in fuse-dfs


---

* [HADOOP-4620](https://issues.apache.org/jira/browse/HADOOP-4620) | *Major* | **Streaming mapper never completes if the mapper does not write to stdout**

This patch HADOOP-4620.patch
(1) solves the hanging problem on map side with empty input and nonempty output — this map task generates output properly to intermediate files similar to other map tasks.
(2) solves the problem of hanging reducer with empty input to reduce task and nonempty output — this reduce task doesn't generate output if input to reduce task is empty.


---

* [HADOOP-4542](https://issues.apache.org/jira/browse/HADOOP-4542) | *Minor* | **Fault in TestDistributedUpgrade**

TestDistributedUpgrade used succeed for wrong reasons.


---

* [HADOOP-4150](https://issues.apache.org/jira/browse/HADOOP-4150) | *Blocker* | **Include librecordio as part of the release**

Included librecordio in release for use by xerces-c  (ant  -Dlibrecordio=true -Dxercescroot=\<path to the xerces-c root\>)


---

* [HADOOP-4061](https://issues.apache.org/jira/browse/HADOOP-4061) | *Major* | **Large number of decommission freezes the Namenode**

Added a new conf property dfs.namenode.decommission.nodes.per.interval so that NameNode checks decommission status of x nodes for every y seconds, where x is the value of dfs.namenode.decommission.nodes.per.interval and y is the value of dfs.namenode.decommission.interval.



