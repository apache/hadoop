
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
# Apache Hadoop  0.19.1 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-5225](https://issues.apache.org/jira/browse/HADOOP-5225) | *Blocker* | **workaround for tmp file handling on DataNodes in 0.19.1 (HADOOP-4663)**

Work around for tmp file handling. sync() does not work as a result.


---

* [HADOOP-5224](https://issues.apache.org/jira/browse/HADOOP-5224) | *Blocker* | **Disable append**

HDFS append() is disabled. It throws UnsupportedOperationException.


---

* [HADOOP-5034](https://issues.apache.org/jira/browse/HADOOP-5034) | *Major* | **NameNode should send both replication and deletion requests to DataNode in one reply to a heartbeat**

This patch changes the DatanodeProtocoal version number from 18 to 19. The patch allows NameNode to send both block replication and deletion request to a DataNode in response to a heartbeat.


---

* [HADOOP-5002](https://issues.apache.org/jira/browse/HADOOP-5002) | *Blocker* | **2 core tests TestFileOutputFormat and TestHarFileSystem are failing in branch 19**

This patch solves the null pointer exception issue in the 2 core tests TestFileOutputFormat and TestHarFileSystem in branch 19.


---

* [HADOOP-4943](https://issues.apache.org/jira/browse/HADOOP-4943) | *Major* | **fair share scheduler does not utilize all slots if the task trackers are configured heterogeneously**

HADOOP-4943: Fixed fair share scheduler to utilize all slots when the task trackers are configured heterogeneously.


---

* [HADOOP-4906](https://issues.apache.org/jira/browse/HADOOP-4906) | *Blocker* | **TaskTracker running out of memory after running several tasks**

Fix the tasktracker for OOM exception by sharing the jobconf properties across tasks of the same job. Earlier a new instance was held for each task. With this fix, the job level configuration properties are shared across tasks of the same job.


---

* [HADOOP-4862](https://issues.apache.org/jira/browse/HADOOP-4862) | *Blocker* | **A spurious IOException log on DataNode is not completely removed**

Minor : HADOOP-3678 did not remove all the cases of spurious IOExceptions logged by DataNode.


---

* [HADOOP-4797](https://issues.apache.org/jira/browse/HADOOP-4797) | *Blocker* | **RPC Server can leave a lot of direct buffers**

Improve how RPC server reads and writes large buffers. Avoids soft-leak of direct buffers and excess copies in NIO layer.


---

* [HADOOP-4635](https://issues.apache.org/jira/browse/HADOOP-4635) | *Blocker* | **Memory leak ?**

fix memory leak of user/group information in fuse-dfs


---

* [HADOOP-4494](https://issues.apache.org/jira/browse/HADOOP-4494) | *Major* | **libhdfs does not call FileSystem.append when O\_APPEND passed to hdfsOpenFile**

libhdfs supports O\_APPEND flag


---

* [HADOOP-4061](https://issues.apache.org/jira/browse/HADOOP-4061) | *Major* | **Large number of decommission freezes the Namenode**

Added a new conf property dfs.namenode.decommission.nodes.per.interval so that NameNode checks decommission status of x nodes for every y seconds, where x is the value of dfs.namenode.decommission.nodes.per.interval and y is the value of dfs.namenode.decommission.interval.



