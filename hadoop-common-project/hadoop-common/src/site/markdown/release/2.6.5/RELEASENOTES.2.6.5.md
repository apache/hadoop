
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
# Apache Hadoop  2.6.5 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-12805](https://issues.apache.org/jira/browse/HADOOP-12805) | *Major* | **Annotate CanUnbuffer with @InterfaceAudience.Public**

Made CanBuffer interface public for use in client applications.


---

* [MAPREDUCE-6622](https://issues.apache.org/jira/browse/MAPREDUCE-6622) | *Critical* | **Add capability to set JHS job cache to a task-based limit**

Two recommendations for the mapreduce.jobhistory.loadedtasks.cache.size property:
1) For every 100k of cache size, set the heap size of the Job History Server to 1.2GB.  For example, mapreduce.jobhistory.loadedtasks.cache.size=500000, heap size=6GB.
2) Make sure that the cache size is larger than the number of tasks required for the largest job run on the cluster.  It might be a good idea to set the value slightly higher (say, 20%) in order to allow for job size growth.


---

* [YARN-4785](https://issues.apache.org/jira/browse/YARN-4785) | *Major* | **inconsistent value type of the "type" field for LeafQueueInfo in response of RM REST API - cluster/scheduler**

Fix inconsistent value type ( String and Array ) of the "type" field for LeafQueueInfo in response of RM REST API
