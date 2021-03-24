
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
# Apache Hadoop  3.2.0 Release Notes

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

* [YARN-6257](https://issues.apache.org/jira/browse/YARN-6257) | *Minor* | **CapacityScheduler REST API produces incorrect JSON - JSON object operationsInfo contains deplicate key**

**WARNING: No release note provided for this change.**


---

* [HADOOP-15146](https://issues.apache.org/jira/browse/HADOOP-15146) | *Minor* | **Remove DataOutputByteBuffer**

**WARNING: No release note provided for this change.**


---

* [MAPREDUCE-7069](https://issues.apache.org/jira/browse/MAPREDUCE-7069) | *Major* | **Add ability to specify user environment variables individually**

Environment variables for MapReduce tasks can now be specified as separate properties, e.g.:
mapreduce.map.env.VARNAME=value
mapreduce.reduce.env.VARNAME=value
yarn.app.mapreduce.am.env.VARNAME=value
yarn.app.mapreduce.am.admin.user.env.VARNAME=value
This form of specifying environment variables is useful when the value of an environment variable contains commas.


---

* [HADOOP-15446](https://issues.apache.org/jira/browse/HADOOP-15446) | *Major* | **WASB: PageBlobInputStream.skip breaks HBASE replication**

WASB: Bug fix to support non-sequential page blob reads.  Required for HBASE replication.


---

* [HDFS-13589](https://issues.apache.org/jira/browse/HDFS-13589) | *Major* | **Add dfsAdmin command to query if "upgrade" is finalized**

New command is added to dfsadmin.
hdfs dfsadmin [-upgrade [query \| finalize]
1. -upgrade query gives the upgradeStatus
2. -upgrade finalize is equivalent to -finalizeUpgrade.


---

* [YARN-8191](https://issues.apache.org/jira/browse/YARN-8191) | *Major* | **Fair scheduler: queue deletion without RM restart**

**WARNING: No release note provided for this change.**


---

* [HADOOP-15477](https://issues.apache.org/jira/browse/HADOOP-15477) | *Trivial* | **Make unjar in RunJar overrideable**

<!-- markdown -->
If `HADOOP_CLIENT_SKIP_UNJAR` environment variable is set to true, Apache Hadoop RunJar skips unjar the provided jar.


---

* [HADOOP-15506](https://issues.apache.org/jira/browse/HADOOP-15506) | *Minor* | **Upgrade Azure Storage Sdk version to 7.0.0 and update corresponding code blocks**

WASB: Fix Spark process hang at shutdown due to use of non-daemon threads by updating Azure Storage Java SDK to 7.0


---

* [HDFS-13174](https://issues.apache.org/jira/browse/HDFS-13174) | *Major* | **hdfs mover -p /path times out after 20 min**

Mover could have fail after 20+ minutes if a block move was enqueued for this long, between two DataNodes due to an internal constant that was introduced for Balancer, but affected Mover as well.
The internal constant can be configured with the dfs.balancer.max-iteration-time parameter after the patch, and affects only the Balancer. Default is 20 minutes.


---

* [HADOOP-15495](https://issues.apache.org/jira/browse/HADOOP-15495) | *Major* | **Upgrade common-lang version to 3.7 in hadoop-common-project and hadoop-tools**

commons-lang version 2.6 was removed from Apache Hadoop. If you are using commons-lang 2.6 as transitive dependency of Hadoop, you need to add the dependency directly. Note: this also means it is absent from share/hadoop/common/lib/


---

* [HDFS-13322](https://issues.apache.org/jira/browse/HDFS-13322) | *Minor* | **fuse dfs - uid persists when switching between ticket caches**

FUSE lib now recognize the change of the Kerberos ticket cache path if it was changed between two file system access in the same local user session via the KRB5CCNAME environment variable.


---

* [HADOOP-15638](https://issues.apache.org/jira/browse/HADOOP-15638) | *Major* | **KMS Accept Queue Size default changed from 500 to 128 in Hadoop 3.x**

Restore the KMS accept queue size to 500 in Hadoop 3.x, making it the same as in Hadoop 2.x.
