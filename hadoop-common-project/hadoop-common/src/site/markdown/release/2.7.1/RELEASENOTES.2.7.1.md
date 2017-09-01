
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
# Apache Hadoop  2.7.1 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HDFS-8226](https://issues.apache.org/jira/browse/HDFS-8226) | *Blocker* | **Non-HA rollback compatibility broken**

Non-HA rollback steps have been changed. Run the rollback command on the namenode (\`bin/hdfs namenode -rollback\`) before starting cluster with '-rollback' option using (sbin/start-dfs.sh -rollback).


---

* [HDFS-8486](https://issues.apache.org/jira/browse/HDFS-8486) | *Blocker* | **DN startup may cause severe data loss**

<!-- markdown -->
Public service notice:
* Every restart of a 2.6.x or 2.7.0 DN incurs a risk of unwanted block deletion.
* Apply this patch if you are running a pre-2.7.1 release.


---

* [HDFS-8270](https://issues.apache.org/jira/browse/HDFS-8270) | *Major* | **create() always retried with hardcoded timeout when file already exists with open lease**

Proxy level retries will not be done on AlreadyBeingCreatedExeption for create() op.


---

* [YARN-6959](https://issues.apache.org/jira/browse/YARN-6959) | *Major* | **RM may allocate wrong AM Container for new attempt**

ResourceManager will now record ResourceRequests from different attempts into different objects.



