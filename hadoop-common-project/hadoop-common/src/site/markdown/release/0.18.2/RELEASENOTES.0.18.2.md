
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
# Apache Hadoop  0.18.2 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-4277](https://issues.apache.org/jira/browse/HADOOP-4277) | *Blocker* | **Checksum verification is disabled for LocalFS**

Checksum verification was mistakenly disabled for LocalFileSystem.


---

* [HADOOP-4271](https://issues.apache.org/jira/browse/HADOOP-4271) | *Blocker* | **Bug in FSInputChecker makes it possible to read from an invalid buffer**

Checksum input stream can sometimes return invalid data to the user.


---

* [HADOOP-4314](https://issues.apache.org/jira/browse/HADOOP-4314) | *Blocker* | **TestReplication fails quite often**

Simulated Datanodes should not include blocks that are still being written in their block report.


---

* [HADOOP-4228](https://issues.apache.org/jira/browse/HADOOP-4228) | *Blocker* | **dfs datanode metrics, bytes\_read, bytes\_written overflows due to incorrect type used.**

Changed bytes\_read, bytes\_written to type long to prevent metrics overflow.


---

* [HADOOP-4467](https://issues.apache.org/jira/browse/HADOOP-4467) | *Blocker* | **SerializationFactory should use current context ClassLoader**

SerializationFactory now uses the current context ClassLoader allowing for user supplied Serialization instances.



