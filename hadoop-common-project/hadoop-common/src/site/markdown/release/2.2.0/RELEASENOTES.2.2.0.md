
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
# Apache Hadoop  2.2.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-10020](https://issues.apache.org/jira/browse/HADOOP-10020) | *Blocker* | **disable symlinks temporarily**

During review of symbolic links, many issues were found related to the impact on semantics of existing APIs such FileSystem#listStatus, FileSystem#globStatus etc. There were also many issues brought up about symbolic links, and the impact on security and functionality of HDFS. All these issues will be addressed in the upcoming release 2.3. Until then the feature is temporarily disabled.


---

* [YARN-1229](https://issues.apache.org/jira/browse/YARN-1229) | *Blocker* | **Define constraints on Auxiliary Service names. Change ShuffleHandler service name from mapreduce.shuffle to mapreduce\_shuffle.**

**WARNING: No release note provided for this incompatible change.**


---

* [YARN-1228](https://issues.apache.org/jira/browse/YARN-1228) | *Major* | **Clean up Fair Scheduler configuration loading**

**WARNING: No release note provided for this incompatible change.**



