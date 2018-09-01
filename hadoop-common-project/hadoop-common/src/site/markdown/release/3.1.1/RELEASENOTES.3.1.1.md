
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
# Apache Hadoop  3.1.1 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-14667](https://issues.apache.org/jira/browse/HADOOP-14667) | *Major* | **Flexible Visual Studio support**

<!-- markdown -->

This change updates the Microsoft Windows build directions to be more flexible with regards to Visual Studio compiler versions:

* Any version of Visual Studio 2010 Pro or higher may be used.
* MSBuild Solution files are converted to the version of VS at build time
* Example command file to set command paths prior to using maven so that conversion works

Additionally, Snappy and ISA-L that use bin as the location of the DLL will now be recognized without having to set their respective lib paths if the prefix is set.

Note to contributors:

It is very important that solutions for any patches remain at the VS 2010-level.


---

* [HADOOP-15446](https://issues.apache.org/jira/browse/HADOOP-15446) | *Major* | **WASB: PageBlobInputStream.skip breaks HBASE replication**

WASB: Bug fix to support non-sequential page blob reads.  Required for HBASE replication.


---

* [HADOOP-15478](https://issues.apache.org/jira/browse/HADOOP-15478) | *Major* | **WASB: hflush() and hsync() regression**

WASB: Bug fix for recent regression in hflush() and hsync().


---

* [HDFS-13589](https://issues.apache.org/jira/browse/HDFS-13589) | *Major* | **Add dfsAdmin command to query if "upgrade" is finalized**

New command is added to dfsadmin.
hdfs dfsadmin [-upgrade [query \| finalize]
1. -upgrade query gives the upgradeStatus
2. -upgrade finalize is equivalent to -finalizeUpgrade.


---

* [HADOOP-15506](https://issues.apache.org/jira/browse/HADOOP-15506) | *Minor* | **Upgrade Azure Storage Sdk version to 7.0.0 and update corresponding code blocks**

WASB: Fix Spark process hang at shutdown due to use of non-daemon threads by updating Azure Storage Java SDK to 7.0


---

* [HDFS-13174](https://issues.apache.org/jira/browse/HDFS-13174) | *Major* | **hdfs mover -p /path times out after 20 min**

Mover could have fail after 20+ minutes if a block move was enqueued for this long, between two DataNodes due to an internal constant that was introduced for Balancer, but affected Mover as well.
The internal constant can be configured with the dfs.balancer.max-iteration-time parameter after the patch, and affects only the Balancer. Default is 20 minutes.


