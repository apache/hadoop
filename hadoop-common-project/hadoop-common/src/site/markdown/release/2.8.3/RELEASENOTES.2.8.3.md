
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
# Apache Hadoop  2.8.3 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-13588](https://issues.apache.org/jira/browse/HADOOP-13588) | *Major* | **ConfServlet should respect Accept request header**

Conf HTTP service should set response's content type according to the Accept header in the request.


---

* [HADOOP-14260](https://issues.apache.org/jira/browse/HADOOP-14260) | *Major* | **Configuration.dumpConfiguration should redact sensitive information**

<!-- markdown -->
Configuration.dumpConfiguration no longer prints out the clear text values for the sensitive keys listed in `hadoop.security.sensitive-config-keys`. Callers can override the default list of sensitive keys either to redact more keys or print the clear text values for a few extra keys for debugging purpose.


---

* [HDFS-11799](https://issues.apache.org/jira/browse/HDFS-11799) | *Major* | **Introduce a config to allow setting up write pipeline with fewer nodes than replication factor**

Added new configuration "dfs.client.block.write.replace-datanode-on-failure.min-replication".

    The minimum number of replications that are needed to not to fail
      the write pipeline if new datanodes can not be found to replace
      failed datanodes (could be due to network failure) in the write pipeline.
      If the number of the remaining datanodes in the write pipeline is greater
      than or equal to this property value, continue writing to the remaining nodes.
      Otherwise throw exception.

      If this is set to 0, an exception will be thrown, when a replacement
      can not be found.
