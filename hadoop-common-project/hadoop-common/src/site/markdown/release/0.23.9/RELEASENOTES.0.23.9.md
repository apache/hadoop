
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
# Apache Hadoop  0.23.9 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HDFS-4832](https://issues.apache.org/jira/browse/HDFS-4832) | *Critical* | **Namenode doesn't change the number of missing blocks in safemode when DNs rejoin or leave**

This change makes name node keep its internal replication queues and data node state updated in manual safe mode. This allows metrics and UI to present up-to-date information while in safe mode. The behavior during start-up safe mode is unchanged.



