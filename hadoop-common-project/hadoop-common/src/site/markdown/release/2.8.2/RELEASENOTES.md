
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
# "Apache Hadoop"  2.8.2 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HDFS-8312](https://issues.apache.org/jira/browse/HDFS-8312) | *Critical* | **Trash does not descent into child directories to check for permissions**

Permissions are now checked when moving a file to Trash.


---

* [HDFS-11499](https://issues.apache.org/jira/browse/HDFS-11499) | *Major* | **Decommissioning stuck because of failing recovery**

Allow a block to complete if the number of replicas on live nodes, decommissioning nodes and nodes in maintenance mode satisfies minimum replication factor.
The fix prevents block recovery failure if replica of last block is being decommissioned. Vice versa, the decommissioning will be stuck, waiting for the last block to be completed. In addition, file close() operation will not fail due to last block being decommissioned.


---

* [HADOOP-14038](https://issues.apache.org/jira/browse/HADOOP-14038) | *Minor* | **Rename ADLS credential properties**

<!-- markdown -->

* Properties {{dfs.adls.*}} are renamed {{fs.adl.*}}
* Property {{adl.dfs.enable.client.latency.tracker}} is renamed {{adl.enable.client.latency.tracker}}
* Old properties are still supported


---

* [HADOOP-14174](https://issues.apache.org/jira/browse/HADOOP-14174) | *Major* | **Set default ADLS access token provider type to ClientCredential**

Switch the default ADLS access token provider type from Custom to ClientCredential.


---

* [HDFS-9016](https://issues.apache.org/jira/browse/HDFS-9016) | *Major* | **Display upgrade domain information in fsck**

New fsck option "-upgradedomains" has been added to display upgrade domains of any block.


---

* [HDFS-11661](https://issues.apache.org/jira/browse/HDFS-11661) | *Blocker* | **GetContentSummary uses excessive amounts of memory**

Reverted HDFS-10797 to fix a scalability regression brought by the commit.


---

* [HDFS-10326](https://issues.apache.org/jira/browse/HDFS-10326) | *Major* | **Disable setting tcp socket send/receive buffers for write pipelines**

The size of the TCP socket buffers are no longer hardcoded by default. Instead the OS now will automatically tune the size for the buffer.



