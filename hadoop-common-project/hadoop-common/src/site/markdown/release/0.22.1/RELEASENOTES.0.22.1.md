
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
# Apache Hadoop  0.22.1 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-7119](https://issues.apache.org/jira/browse/HADOOP-7119) | *Major* | **add Kerberos HTTP SPNEGO authentication support to Hadoop JT/NN/DN/TT web-consoles**

Adding support for Kerberos HTTP SPNEGO authentication to the Hadoop web-consoles


---

* [HADOOP-6995](https://issues.apache.org/jira/browse/HADOOP-6995) | *Minor* | **Allow wildcards to be used in ProxyUsers configurations**

When configuring proxy users and hosts, the special wildcard value "\*" may be specified to match any host or any user.


---

* [HADOOP-6453](https://issues.apache.org/jira/browse/HADOOP-6453) | *Minor* | **Hadoop wrapper script shouldn't ignore an existing JAVA\_LIBRARY\_PATH**

**WARNING: No release note provided for this incompatible change.**


---

* [HDFS-2246](https://issues.apache.org/jira/browse/HDFS-2246) | *Major* | **Shortcut a local client reads to a Datanodes files directly**

1. New configurations
a. dfs.block.local-path-access.user is the key in datanode configuration to specify the user allowed to do short circuit read.
b. dfs.client.read.shortcircuit is the key to enable short circuit read at the client side configuration.
c. dfs.client.read.shortcircuit.skip.checksum is the key to bypass checksum check at the client side.
2. By default none of the above are enabled and short circuit read will not kick in.
3. If security is on, the feature can be used only for user that has kerberos credentials at the client, therefore map reduce tasks cannot benefit from it in general.


---

* [MAPREDUCE-3725](https://issues.apache.org/jira/browse/MAPREDUCE-3725) | *Major* | **Hadoop 22 hadoop job -list returns user name as NULL**

Submitting the patch after setting the user name at the client side



