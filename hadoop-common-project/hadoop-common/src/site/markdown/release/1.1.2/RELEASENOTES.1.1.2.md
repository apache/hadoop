
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
# Apache Hadoop  1.1.2 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-9115](https://issues.apache.org/jira/browse/HADOOP-9115) | *Blocker* | **Deadlock in configuration when writing configuration to hdfs**

This fixes a bug where Hive could trigger a deadlock condition in the Hadoop configuration management code.


---

* [HADOOP-8567](https://issues.apache.org/jira/browse/HADOOP-8567) | *Major* | **Port conf servlet to dump running configuration  to branch 1.x**

Users can use the conf servlet to get the server-side configuration. Users can

1) connect to http\_server\_url/conf or http\_server\_url/conf?format=xml and get XML-based configuration description;
2) connect to http\_server\_url/conf?format=json and get JSON-based configuration description.


---

* [HDFS-5996](https://issues.apache.org/jira/browse/HDFS-5996) | *Major* | **hadoop 1.1.2.  hdfs  write bug**

someone  has  discovered   this  bug  and  it  has  been  resolved


---

* [MAPREDUCE-4478](https://issues.apache.org/jira/browse/MAPREDUCE-4478) | *Major* | **TaskTracker's heartbeat is out of control**

Fixed a bug in TaskTracker's heartbeat to keep it under control.



