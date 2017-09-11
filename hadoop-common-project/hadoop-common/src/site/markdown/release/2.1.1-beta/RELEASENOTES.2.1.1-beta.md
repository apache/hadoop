
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
# Apache Hadoop  2.1.1-beta Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [YARN-707](https://issues.apache.org/jira/browse/YARN-707) | *Blocker* | **Add user info in the YARN ClientToken**

**WARNING: No release note provided for this change.**


---

* [HDFS-5118](https://issues.apache.org/jira/browse/HDFS-5118) | *Major* | **Provide testing support for DFSClient to drop RPC responses**

Used for testing when NameNode HA is enabled. Users can use a new configuration property "dfs.client.test.drop.namenode.response.number" to specify the number of responses that DFSClient will drop in each RPC call. This feature can help testing functionalities such as NameNode retry cache.


---

* [YARN-1170](https://issues.apache.org/jira/browse/YARN-1170) | *Blocker* | **yarn proto definitions should specify package as 'hadoop.yarn'**

**WARNING: No release note provided for this change.**


---

* [HADOOP-9944](https://issues.apache.org/jira/browse/HADOOP-9944) | *Blocker* | **RpcRequestHeaderProto defines callId as uint32 while ipc.Client.CONNECTION\_CONTEXT\_CALL\_ID is signed (-3)**

**WARNING: No release note provided for this change.**



