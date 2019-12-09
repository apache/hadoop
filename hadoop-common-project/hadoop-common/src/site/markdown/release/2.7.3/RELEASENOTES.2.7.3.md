
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
# Apache Hadoop  2.7.3 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [MAPREDUCE-5485](https://issues.apache.org/jira/browse/MAPREDUCE-5485) | *Critical* | **Allow repeating job commit by extending OutputCommitter API**

Previously, the MR job will get failed if AM get restarted for some reason (like node failure, etc.) during its doing commit job no matter if AM attempts reach to the maximum attempts.
In this improvement, we add a new API isCommitJobRepeatable() to OutputCommitter interface which to indicate if job's committer can do commitJob again if previous commit work is interrupted by NM/AM failures, etc. The instance of OutputCommitter, which support repeatable job commit (like FileOutputCommitter in algorithm 2), can allow AM to continue the commitJob() after AM restart as a new attempt.


---

* [HADOOP-11252](https://issues.apache.org/jira/browse/HADOOP-11252) | *Critical* | **RPC client does not time out by default**

This fix includes public method interface change.
A follow-up JIRA issue for this incompatibility for branch-2.7 is HADOOP-13579.


---

* [HADOOP-12805](https://issues.apache.org/jira/browse/HADOOP-12805) | *Major* | **Annotate CanUnbuffer with @InterfaceAudience.Public**

Made CanBuffer interface public for use in client applications.


---

* [HADOOP-12794](https://issues.apache.org/jira/browse/HADOOP-12794) | *Major* | **Support additional compression levels for GzipCodec**

Added New compression levels for GzipCodec that can be set in zlib.compress.level


---

* [MAPREDUCE-6622](https://issues.apache.org/jira/browse/MAPREDUCE-6622) | *Critical* | **Add capability to set JHS job cache to a task-based limit**

Two recommendations for the mapreduce.jobhistory.loadedtasks.cache.size property:
1) For every 100k of cache size, set the heap size of the Job History Server to 1.2GB.  For example, mapreduce.jobhistory.loadedtasks.cache.size=500000, heap size=6GB.
2) Make sure that the cache size is larger than the number of tasks required for the largest job run on the cluster.  It might be a good idea to set the value slightly higher (say, 20%) in order to allow for job size growth.


---

* [YARN-4785](https://issues.apache.org/jira/browse/YARN-4785) | *Major* | **inconsistent value type of the "type" field for LeafQueueInfo in response of RM REST API - cluster/scheduler**

Fix inconsistent value type ( String and Array ) of the "type" field for LeafQueueInfo in response of RM REST API


---

* [MAPREDUCE-6670](https://issues.apache.org/jira/browse/MAPREDUCE-6670) | *Minor* | **TestJobListCache#testEviction sometimes fails on Windows with timeout**

Backport the fix to 2.7 and 2.8
