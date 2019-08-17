
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
# Apache Hadoop  2.10.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HDFS-12883](https://issues.apache.org/jira/browse/HDFS-12883) | *Major* | **RBF: Document Router and State Store metrics**

This JIRA makes following change:
Change Router metrics context from 'router' to 'dfs'.


---

* [HDFS-12895](https://issues.apache.org/jira/browse/HDFS-12895) | *Major* | **RBF: Add ACL support for mount table**

Mount tables support ACL, The users won't be able to modify their own entries (we are assuming these old (no-permissions before) mount table with owner:superuser, group:supergroup, permission:755 as the default permissions).  The fix way is login as superuser to modify these mount table entries.


---

* [HADOOP-15027](https://issues.apache.org/jira/browse/HADOOP-15027) | *Major* | **AliyunOSS: Support multi-thread pre-read to improve sequential read from Hadoop to Aliyun OSS performance**

Support multi-thread pre-read in AliyunOSSInputStream to improve the sequential read performance from Hadoop to Aliyun OSS.


---

* [MAPREDUCE-7029](https://issues.apache.org/jira/browse/MAPREDUCE-7029) | *Minor* | **FileOutputCommitter is slow on filesystems lacking recursive delete**

MapReduce jobs that output to filesystems without direct support for recursive delete can set mapreduce.fileoutputcommitter.task.cleanup.enabled=true to have each task delete their intermediate work directory rather than waiting for the ApplicationMaster to clean up at the end of the job. This can significantly speed up the cleanup phase for large jobs on such filesystems.


---

* [HDFS-12528](https://issues.apache.org/jira/browse/HDFS-12528) | *Major* | **Add an option to not disable short-circuit reads on failures**

Added an option to not disables short-circuit reads on failures, by setting dfs.domain.socket.disable.interval.seconds to 0.


---

* [HDFS-13083](https://issues.apache.org/jira/browse/HDFS-13083) | *Major* | **RBF: Fix doc error setting up client**

Fix the document error of setting up HFDS Router Federation


---

* [HDFS-13099](https://issues.apache.org/jira/browse/HDFS-13099) | *Minor* | **RBF: Use the ZooKeeper as the default State Store**

Change default State Store from local file to ZooKeeper. This will require additional zk address to be configured.


---

* [YARN-7919](https://issues.apache.org/jira/browse/YARN-7919) | *Major* | **Refactor timelineservice-hbase module into submodules**

HBase integration module was mixed up with for hbase-server and hbase-client dependencies. This JIRA split into sub modules such that hbase-client dependent modules and hbase-server dependent modules are separated. This allows to make conditional compilation with different version of Hbase.


---

* [HDFS-13492](https://issues.apache.org/jira/browse/HDFS-13492) | *Major* | **Limit httpfs binds to certain IP addresses in branch-2**

Use environment variable HTTPFS\_HTTP\_HOSTNAME to limit the IP addresses httpfs server binds to. Default: httpfs server binds to all IP addresses on the host.


---

* [HADOOP-15446](https://issues.apache.org/jira/browse/HADOOP-15446) | *Major* | **WASB: PageBlobInputStream.skip breaks HBASE replication**

WASB: Bug fix to support non-sequential page blob reads.  Required for HBASE replication.


---

* [HADOOP-15478](https://issues.apache.org/jira/browse/HADOOP-15478) | *Major* | **WASB: hflush() and hsync() regression**

WASB: Bug fix for recent regression in hflush() and hsync().


---

* [HADOOP-15506](https://issues.apache.org/jira/browse/HADOOP-15506) | *Minor* | **Upgrade Azure Storage Sdk version to 7.0.0 and update corresponding code blocks**

WASB: Fix Spark process hang at shutdown due to use of non-daemon threads by updating Azure Storage Java SDK to 7.0


---

* [HDFS-13553](https://issues.apache.org/jira/browse/HDFS-13553) | *Major* | **RBF: Support global quota**

Federation supports and controls global quota at mount table level.

In a federated environment, a folder can be spread across multiple subclusters. Router aggregates quota that queried from these subclusters  and uses that for the quota-verification.
