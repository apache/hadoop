
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
# Apache Hadoop  3.0.1 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-15027](https://issues.apache.org/jira/browse/HADOOP-15027) | *Major* | **AliyunOSS: Support multi-thread pre-read to improve sequential read from Hadoop to Aliyun OSS performance**

Support multi-thread pre-read in AliyunOSSInputStream to improve the sequential read performance from Hadoop to Aliyun OSS.


---

* [HDFS-12528](https://issues.apache.org/jira/browse/HDFS-12528) | *Major* | **Add an option to not disable short-circuit reads on failures**

Added an option to not disables short-circuit reads on failures, by setting dfs.domain.socket.disable.interval.seconds to 0.


---

* [HDFS-13083](https://issues.apache.org/jira/browse/HDFS-13083) | *Major* | **RBF: Fix doc error setting up client**

Fix the document error of setting up HFDS Router Federation


---

* [HDFS-12990](https://issues.apache.org/jira/browse/HDFS-12990) | *Critical* | **Change default NameNode RPC port back to 8020**

HDFS NameNode default RPC port is changed back to 8020. The only official release that has this differently is 3.0.0, which used 9820.

It is recommended for 2.x users to upgrade to 3.0.1+, to reduce the burden on changing default NN RPC port.



