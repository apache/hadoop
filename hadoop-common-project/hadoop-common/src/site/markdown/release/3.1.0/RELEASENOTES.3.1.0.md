
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
# Apache Hadoop  3.1.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HDFS-11799](https://issues.apache.org/jira/browse/HDFS-11799) | *Major* | **Introduce a config to allow setting up write pipeline with fewer nodes than replication factor**

Added new configuration "dfs.client.block.write.replace-datanode-on-failure.min-replication".
     
    The minimum number of replications that are needed to not to fail
      the write pipeline if new datanodes can not be found to replace
      failed datanodes (could be due to network failure) in the write pipeline.
      If the number of the remaining datanodes in the write pipeline is greater
      than or equal to this property value, continue writing to the remaining nodes.
      Otherwise throw exception.

      If this is set to 0, an exception will be thrown, when a replacement
      can not be found.


---

* [HDFS-12486](https://issues.apache.org/jira/browse/HDFS-12486) | *Major* | **GetConf to get journalnodeslist**

Adds a getconf command option to list the journal nodes.
Usage: hdfs getconf -journalnodes


---

* [HADOOP-14840](https://issues.apache.org/jira/browse/HADOOP-14840) | *Major* | **Tool to estimate resource requirements of an application pipeline based on prior executions**

The first version of Resource Estimator service, a tool that captures the historical resource usage of an app and predicts its future resource requirement.


---

* [YARN-5079](https://issues.apache.org/jira/browse/YARN-5079) | *Major* | **[Umbrella] Native YARN framework layer for services and beyond**

A framework is implemented to orchestrate containers on YARN


---

* [YARN-4757](https://issues.apache.org/jira/browse/YARN-4757) | *Major* | **[Umbrella] Simplified discovery of services via DNS mechanisms**

A DNS server backed by yarn service registry is implemented to enable service discovery on YARN using standard DNS lookup.


---

* [YARN-4793](https://issues.apache.org/jira/browse/YARN-4793) | *Major* | **[Umbrella] Simplified API layer for services and beyond**

A REST API service is implemented to enable users to launch and manage container based services on YARN via REST API


---

* [HADOOP-15008](https://issues.apache.org/jira/browse/HADOOP-15008) | *Minor* | **Metrics sinks may emit too frequently if multiple sink periods are configured**

Previously if multiple metrics sinks were configured with different periods, they may emit more frequently than configured, at a period as low as the GCD of the configured periods. This change makes all metrics sinks emit at their configured period.


---

* [HDFS-12825](https://issues.apache.org/jira/browse/HDFS-12825) | *Minor* | **Fsck report shows config key name for min replication issues**

**WARNING: No release note provided for this change.**


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

* [HDFS-9806](https://issues.apache.org/jira/browse/HDFS-9806) | *Major* | **Allow HDFS block replicas to be provided by an external storage system**

Provided storage allows data stored outside HDFS to be mapped to and addressed from HDFS. It builds on heterogeneous storage by introducing a new storage type, PROVIDED, to the set of media in a datanode. Clients accessing data in PROVIDED storages can cache replicas in local media, enforce HDFS invariants (e.g., security, quotas), and address more data than the cluster could persist in the storage attached to DataNodes.


---

* [HADOOP-13282](https://issues.apache.org/jira/browse/HADOOP-13282) | *Minor* | **S3 blob etags to be made visible in S3A status/getFileChecksum() calls**

now that S3A has a checksum, you need to explicitly disable checksums when uploading from HDFS : use -skipCrc

checksum verification does work between s3a buckets, provided the block size on uploads was identical


---

* [YARN-7688](https://issues.apache.org/jira/browse/YARN-7688) | *Minor* | **Miscellaneous Improvements To ProcfsBasedProcessTree**

Added new patch.  Fixes white spaces and some check-style items.


---

* [YARN-6486](https://issues.apache.org/jira/browse/YARN-6486) | *Major* | **FairScheduler: Deprecate continuous scheduling**

FairScheduler Continuous Scheduling is deprecated starting from 3.1.0.


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

* [HADOOP-15252](https://issues.apache.org/jira/browse/HADOOP-15252) | *Major* | **Checkstyle version is not compatible with IDEA's checkstyle plugin**

Updated checkstyle to 8.8 and updated maven-checkstyle-plugin to 3.0.0.


---

* [YARN-7919](https://issues.apache.org/jira/browse/YARN-7919) | *Major* | **Refactor timelineservice-hbase module into submodules**

HBase integration module was mixed up with for hbase-server and hbase-client dependencies. This JIRA split into sub modules such that hbase-client dependent modules and hbase-server dependent modules are separated. This allows to make conditional compilation with different version of Hbase.


---

* [YARN-7677](https://issues.apache.org/jira/browse/YARN-7677) | *Major* | **Docker image cannot set HADOOP\_CONF\_DIR**

The HADOOP\_CONF\_DIR environment variable is no longer unconditionally inherited by containers even if it does not appear in the nodemanager whitelist variables specified by the yarn.nodemanager.env-whitelist property. If the whitelist property has been modified from the default to not include HADOOP\_CONF\_DIR yet containers need it to be inherited from the nodemanager's environment then the whitelist settings need to be updated to include HADOOP\_CONF\_DIR.



