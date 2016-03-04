
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
# Apache Hadoop  0.17.1 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-3565](https://issues.apache.org/jira/browse/HADOOP-3565) | *Major* | **JavaSerialization can throw java.io.StreamCorruptedException**

Change the Java serialization framework, which is not enabled by default, to correctly make the objects independent of the previous objects.


---

* [HADOOP-1979](https://issues.apache.org/jira/browse/HADOOP-1979) | *Minor* | **fsck on namenode without datanodes takes too much time**

Improved performance of {{fsck}} by better management of the data stream on the client side.



