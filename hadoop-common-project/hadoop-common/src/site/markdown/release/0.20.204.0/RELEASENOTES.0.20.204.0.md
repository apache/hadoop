
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
# Apache Hadoop  0.20.204.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HDFS-1445](https://issues.apache.org/jira/browse/HDFS-1445) | *Major* | **Batch the calls in DataStorage to FileUtil.createHardLink(), so we call it once per directory instead of once per file**

Batch hardlinking during "upgrade" snapshots, cutting time from aprx 8 minutes per volume to aprx 8 seconds.  Validated in both Linux and Windows.  Depends on prior integration with patch for HADOOP-7133.


---

* [MAPREDUCE-2479](https://issues.apache.org/jira/browse/MAPREDUCE-2479) | *Major* | **Backport MAPREDUCE-1568 to hadoop security branch**

Added mapreduce.tasktracker.distributedcache.checkperiod to the task tracker that defined the period to wait while cleaning up the distributed cache.  The default is 1 min.


---

* [HADOOP-6255](https://issues.apache.org/jira/browse/HADOOP-6255) | *Major* | **Create an rpm integration project**

Added RPM/DEB packages to build system.


---

* [MAPREDUCE-2524](https://issues.apache.org/jira/browse/MAPREDUCE-2524) | *Minor* | **Backport trunk heuristics for failing maps when we get fetch failures retrieving map output during shuffle**

Added a new configuration option: mapreduce.reduce.shuffle.maxfetchfailures, and removed a no longer used option: mapred.reduce.copy.backoff.


---

* [MAPREDUCE-2529](https://issues.apache.org/jira/browse/MAPREDUCE-2529) | *Major* | **Recognize Jetty bug 1342 and handle it**

Added 2 new config parameters:

mapreduce.reduce.shuffle.catch.exception.stack.regex
mapreduce.reduce.shuffle.catch.exception.message.regex


---

* [HDFS-2218](https://issues.apache.org/jira/browse/HDFS-2218) | *Blocker* | **Disable TestHdfsProxy.testHdfsProxyInterface in 0.20-security and branch-1 until HDFS-2217 is fixed**

Test case TestHdfsProxy.testHdfsProxyInterface has been temporarily disabled for this release, due to failure in the Hudson automated test environment.


---

* [MAPREDUCE-2846](https://issues.apache.org/jira/browse/MAPREDUCE-2846) | *Blocker* | **a small % of all tasks fail with DefaultTaskController**

Fixed a race condition in writing the log index file that caused tasks to 'fail'.


---

* [MAPREDUCE-2804](https://issues.apache.org/jira/browse/MAPREDUCE-2804) | *Blocker* | **"Creation of symlink to attempt log dir failed." message is not useful**

Removed duplicate chmods of job log dir that were vulnerable to race conditions between tasks. Also improved the messages when the symlinks failed to be created.



