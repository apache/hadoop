
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
# Apache Hadoop  0.23.5 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-8926](https://issues.apache.org/jira/browse/HADOOP-8926) | *Major* | **hadoop.util.PureJavaCrc32 cache hit-ratio is low for static data**

Speed up Crc32 by improving the cache hit-ratio of hadoop.util.PureJavaCrc32


---

* [HDFS-4080](https://issues.apache.org/jira/browse/HDFS-4080) | *Major* | **Add a separate logger for block state change logs to enable turning off those logs**

Add a separate logger "BlockStateChange" for block state change logs.



