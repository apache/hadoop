
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
# Apache Hadoop  2.7.2 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-7139](https://issues.apache.org/jira/browse/HADOOP-7139) | *Major* | **Allow appending to existing SequenceFiles**

Existing sequence files can be appended.


---

* [YARN-4087](https://issues.apache.org/jira/browse/YARN-4087) | *Major* | **Followup fixes after YARN-2019 regarding RM behavior when state-store error occurs**

Set YARN\_FAIL\_FAST to be false by default. If HA is enabled and if there's any state-store error, after the retry operation failed, we always transition RM to standby state.



