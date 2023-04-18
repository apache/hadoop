
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
# Apache Hadoop  3.3.5 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-17956](https://issues.apache.org/jira/browse/HADOOP-17956) | *Major* | **Replace all default Charset usage with UTF-8**

All of the default charset usages have been replaced to UTF-8. If the default charset of your environment is not UTF-8, the behavior can be different.


---

* [HADOOP-15983](https://issues.apache.org/jira/browse/HADOOP-15983) | *Major* | **Use jersey-json that is built to use jackson2**

Use modified jersey-json 1.20 in https://github.com/pjfanning/jersey-1.x/tree/v1.20 that uses Jackson 2.x. By this change, Jackson 1.x dependency has been removed from Hadoop.
downstream applications which explicitly exclude jersey from transitive dependencies must now exclude com.github.pjfanning:jersey-json


---

* [HDFS-16595](https://issues.apache.org/jira/browse/HDFS-16595) | *Major* | **Slow peer metrics - add median, mad and upper latency limits**

Namenode metrics that represent Slownode Json now include three important factors (median, median absolute deviation, upper latency limit) that can help user determine how urgently a given slownode requires manual intervention.


---

* [HADOOP-17833](https://issues.apache.org/jira/browse/HADOOP-17833) | *Minor* | **Improve Magic Committer Performance**

S3A filesytem's createFile() operation supports an option to disable all safety checks when creating a file. Consult the documentation and use with care


---

* [HADOOP-18382](https://issues.apache.org/jira/browse/HADOOP-18382) | *Minor* | **Upgrade AWS SDK to V2 - Prerequisites**

In preparation for an (incompatible but necessary) move to the AWS SDK v2, some uses of internal/deprecated uses of AWS classes/interfaces are logged as warnings, though only once during the life of a JVM. Set the log "org.apache.hadoop.fs.s3a.SDKV2Upgrade" to only log at INFO to hide these.


---

* [HADOOP-18442](https://issues.apache.org/jira/browse/HADOOP-18442) | *Major* | **Remove the hadoop-openstack module**

The swift:// connector for openstack support has been removed. It had fundamental problems (swift's handling of files \> 4GB). A subset of the S3 protocol is now exported by almost all object store services -please use that through the s3a connector instead. The hadoop-openstack jar remains, only now it is empty of code. This is to ensure that projects which declare the JAR a dependency will still have successful builds.


---

* [HADOOP-17563](https://issues.apache.org/jira/browse/HADOOP-17563) | *Major* | **Update Bouncy Castle to 1.68 or later**

bouncy castle 1.68+ is a multirelease JAR containing java classes compiled for different target JREs. older versions of asm.jar and maven shade plugin may have problems with these. fix: upgrade the dependencies


---

* [HADOOP-18528](https://issues.apache.org/jira/browse/HADOOP-18528) | *Major* | **Disable abfs prefetching by default**

ABFS block prefetching has been disabled to avoid HADOOP-18521 and buffer sharing on multithreaded processes (Hive, Spark etc). This will have little/no performance impact on queries against Parquet or ORC data, but can slow down sequential stream processing, including CSV files -however, the read data will be correct.
It may slow down distcp downloads, where the race condition does not arise. For maximum distcp performance re-enable the readahead by setting fs.abfs.enable.readahead to true.


---

* [HADOOP-18621](https://issues.apache.org/jira/browse/HADOOP-18621) | *Critical* | **CryptoOutputStream::close leak when encrypted zones + quota exceptions**

**WARNING: No release note provided for this change.**



