
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
# Apache Hadoop  3.0.3 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [YARN-7190](https://issues.apache.org/jira/browse/YARN-7190) | *Major* | **Ensure only NM classpath in 2.x gets TSv2 related hbase jars, not the user classpath**

Ensure only NM classpath in 2.x gets TSv2 related hbase jars, not the user classpath.


---

* [HDFS-13099](https://issues.apache.org/jira/browse/HDFS-13099) | *Minor* | **RBF: Use the ZooKeeper as the default State Store**

Change default State Store from local file to ZooKeeper. This will require additional zk address to be configured.



