
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
# Apache Hadoop  2.9.1 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-12756](https://issues.apache.org/jira/browse/HADOOP-12756) | *Major* | **Incorporate Aliyun OSS file system implementation**

Aliyun OSS is widely used among China’s cloud users and this work implemented a new Hadoop compatible filesystem AliyunOSSFileSystem with oss scheme, similar to the s3a and azure support.


---

* [HADOOP-14964](https://issues.apache.org/jira/browse/HADOOP-14964) | *Major* | **AliyunOSS: backport Aliyun OSS module to branch-2**

Aliyun OSS is widely used among China’s cloud users and this work implemented a new Hadoop compatible filesystem AliyunOSSFileSystem with oss:// scheme, similar to the s3a and azure support.


---

* [HDFS-12883](https://issues.apache.org/jira/browse/HDFS-12883) | *Major* | **RBF: Document Router and State Store metrics**

This JIRA makes following change:
Change Router metrics context from 'router' to 'dfs'.


---

* [HDFS-12895](https://issues.apache.org/jira/browse/HDFS-12895) | *Major* | **RBF: Add ACL support for mount table**

Mount tables support ACL, The users won't be able to modify their own entries (we are assuming these old (no-permissions before) mount table with owner:superuser, group:supergroup, permission:755 as the default permissions).  The fix way is login as superuser to modify these mount table entries.


---

* [YARN-7190](https://issues.apache.org/jira/browse/YARN-7190) | *Major* | **Ensure only NM classpath in 2.x gets TSv2 related hbase jars, not the user classpath**

Ensure only NM classpath in 2.x gets TSv2 related hbase jars, not the user classpath.


---

* [HADOOP-15156](https://issues.apache.org/jira/browse/HADOOP-15156) | *Major* | **backport HADOOP-15086 rename fix to branch-2**

[WASB] Fix Azure implementation of Filesystem.rename to ensure that at most one operation succeeds when there are multiple, concurrent rename operations targeting the same destination file.


---

* [HADOOP-15027](https://issues.apache.org/jira/browse/HADOOP-15027) | *Major* | **AliyunOSS: Support multi-thread pre-read to improve sequential read from Hadoop to Aliyun OSS performance**

Support multi-thread pre-read in AliyunOSSInputStream to improve the sequential read performance from Hadoop to Aliyun OSS.


---

* [HDFS-13083](https://issues.apache.org/jira/browse/HDFS-13083) | *Major* | **RBF: Fix doc error setting up client**

Fix the document error of setting up HFDS Router Federation


---

* [HDFS-13099](https://issues.apache.org/jira/browse/HDFS-13099) | *Minor* | **RBF: Use the ZooKeeper as the default State Store**

Change default State Store from local file to ZooKeeper. This will require additional zk address to be configured.



