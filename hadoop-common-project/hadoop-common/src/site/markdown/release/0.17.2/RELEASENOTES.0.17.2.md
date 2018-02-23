
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
# Apache Hadoop  0.17.2 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-3760](https://issues.apache.org/jira/browse/HADOOP-3760) | *Blocker* | **DFS operations fail because of Stream closed error**

Fix a bug with HDFS file close() mistakenly introduced by HADOOP-3681.


---

* [HADOOP-3707](https://issues.apache.org/jira/browse/HADOOP-3707) | *Blocker* | **Frequent DiskOutOfSpaceException on almost-full datanodes**

NameNode keeps a count of number of blocks scheduled to be written to a datanode and uses it to avoid allocating more blocks than a datanode can hold.


---

* [HADOOP-3678](https://issues.apache.org/jira/browse/HADOOP-3678) | *Blocker* | **Avoid spurious "DataXceiver: java.io.IOException: Connection reset by peer" errors in DataNode log**

Avoid spurious exceptions logged at DataNode when clients read from DFS.


---

* [HADOOP-3859](https://issues.apache.org/jira/browse/HADOOP-3859) | *Blocker* | **1000  concurrent read on a single file failing  the task/client**

Allows the user to change the maximum number of xceivers in the datanode.

