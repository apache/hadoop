
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
# Apache Hadoop  1.3.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [MAPREDUCE-4049](https://issues.apache.org/jira/browse/MAPREDUCE-4049) | *Major* | **plugin for generic shuffle service**

Allow ReduceTask loading a third party plugin for shuffle (and merge) instead of the default shuffle.


---

* [HADOOP-8873](https://issues.apache.org/jira/browse/HADOOP-8873) | *Major* | **Port HADOOP-8175 (Add mkdir -p flag) to branch-1**

FsShell mkdir now accepts a -p flag. Like unix, mkdir -p will not fail if the directory already exists. Unlike unix, intermediate directories are always created, regardless of the flag, to avoid incompatibilities.


---

* [MAPREDUCE-5408](https://issues.apache.org/jira/browse/MAPREDUCE-5408) | *Major* | **CLONE - The logging level of the tasks should be configurable by the job**

Allow logging level of map/reduce tasks to be configurable.
Configuration changes:
  add mapred.map.child.log.level
  add mapred.reduce.child.log.level


---

* [HDFS-5685](https://issues.apache.org/jira/browse/HDFS-5685) | *Major* | **DistCp will fail to copy with -delete switch**

Has dependency on MAPREDUCE-1285/MAPREDUCE-5698


---

* [MAPREDUCE-5777](https://issues.apache.org/jira/browse/MAPREDUCE-5777) | *Major* | **Support utf-8 text with BOM (byte order marker)**

**WARNING: No release note provided for this change.**


---

* [HDFS-7312](https://issues.apache.org/jira/browse/HDFS-7312) | *Minor* | **Update DistCp v1 to optionally not use tmp location (branch-1 only)**

DistCp v1 currently copies files to a temporary location and then renames that to the specified destination. This can cause performance issues on file systems such as S3. A -skiptmp flag is added to bypass this step and copy directly to the destination.



