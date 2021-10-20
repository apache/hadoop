
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
# "Apache Hadoop"  2.10.1 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [MAPREDUCE-7069](https://issues.apache.org/jira/browse/MAPREDUCE-7069) | *Major* | **Add ability to specify user environment variables individually**

Environment variables for MapReduce tasks can now be specified as separate properties, e.g.:
mapreduce.map.env.VARNAME=value
mapreduce.reduce.env.VARNAME=value
yarn.app.mapreduce.am.env.VARNAME=value
yarn.app.mapreduce.am.admin.user.env.VARNAME=value
This form of specifying environment variables is useful when the value of an environment variable contains commas.


---

* [HDFS-15281](https://issues.apache.org/jira/browse/HDFS-15281) | *Major* | **ZKFC ignores dfs.namenode.rpc-bind-host and uses dfs.namenode.rpc-address to bind to host address**

ZKFC binds host address to "dfs.namenode.servicerpc-bind-host", if configured. Otherwise, it binds to "dfs.namenode.rpc-bind-host". If neither of those is configured, ZKFC binds itself to NameNode RPC server address (effectively "dfs.namenode.rpc-address").


---

* [HDFS-12453](https://issues.apache.org/jira/browse/HDFS-12453) | *Critical* | **TestDataNodeHotSwapVolumes fails in trunk Jenkins runs**

Submitted version of patch for branch-2.10


---

* [HADOOP-17089](https://issues.apache.org/jira/browse/HADOOP-17089) | *Critical* | **WASB: Update azure-storage-java SDK**

Azure WASB bug fix that can cause list results to appear empty.


---

* [YARN-7677](https://issues.apache.org/jira/browse/YARN-7677) | *Major* | **Docker image cannot set HADOOP\_CONF\_DIR**

The HADOOP\_CONF\_DIR environment variable is no longer unconditionally inherited by containers even if it does not appear in the nodemanager whitelist variables specified by the yarn.nodemanager.env-whitelist property. If the whitelist property has been modified from the default to not include HADOOP\_CONF\_DIR yet containers need it to be inherited from the nodemanager's environment then the whitelist settings need to be updated to include HADOOP\_CONF\_DIR.



