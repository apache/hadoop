
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
# "Apache Hadoop"  2.10.2 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HDFS-10650](https://issues.apache.org/jira/browse/HDFS-10650) | *Minor* | **DFSClient#mkdirs and DFSClient#primitiveMkdir should use default directory permission**

If the caller does not supply a permission, DFSClient#mkdirs and DFSClient#primitiveMkdir will create a new directory with the default directory permission 00777 now, instead of 00666.


---

* [HDFS-13174](https://issues.apache.org/jira/browse/HDFS-13174) | *Major* | **hdfs mover -p /path times out after 20 min**

Mover could have fail after 20+ minutes if a block move was enqueued for this long, between two DataNodes due to an internal constant that was introduced for Balancer, but affected Mover as well.
The internal constant can be configured with the dfs.balancer.max-iteration-time parameter after the patch, and affects only the Balancer. Default is 20 minutes.


---

* [YARN-10036](https://issues.apache.org/jira/browse/YARN-10036) | *Major* | **Install yarnpkg and upgrade nodejs in Dockerfile**

In the Dockerfile, nodejs is upgraded to 8.17.0 and yarn 1.12.1 is installed.


---

* [HADOOP-16054](https://issues.apache.org/jira/browse/HADOOP-16054) | *Major* | **Update Dockerfile to use Bionic**

The build image has been upgraded to Bionic.


---

* [HADOOP-17338](https://issues.apache.org/jira/browse/HADOOP-17338) | *Major* | **Intermittent S3AInputStream failures: Premature end of Content-Length delimited message body etc**

**WARNING: No release note provided for this change.**


---

* [HADOOP-16748](https://issues.apache.org/jira/browse/HADOOP-16748) | *Major* | **Migrate to Python 3 and upgrade Yetus to 0.13.0**

<!-- markdown -->
- Upgraded Yetus to 0.13.0.
- Removed determine-flaky-tests-hadoop.py.
- Temporarily disabled shelldocs check in the Jenkins jobs due to YETUS-1099.


---

* [HADOOP-16870](https://issues.apache.org/jira/browse/HADOOP-16870) | *Major* | **Use spotbugs-maven-plugin instead of findbugs-maven-plugin**

Removed findbugs from the hadoop build images and added spotbugs instead.
Upgraded SpotBugs to 4.2.2 and spotbugs-maven-plugin to 4.2.0.


---

* [YARN-8234](https://issues.apache.org/jira/browse/YARN-8234) | *Critical* | **Improve RM system metrics publisher's performance by pushing events to timeline server in batch**

When Timeline Service V1 or V1.5 is used, if "yarn.resourcemanager.system-metrics-publisher.timeline-server-v1.enable-batch" is set to true, ResourceManager sends timeline events in batch. The default value is false. If this functionality is enabled, the maximum number that events published in batch is configured by "yarn.resourcemanager.system-metrics-publisher.timeline-server-v1.batch-size". The default value is 1000. The interval of publishing events can be configured by "yarn.resourcemanager.system-metrics-publisher.timeline-server-v1.interval-seconds". By default, it is set to 60 seconds.


---

* [HADOOP-18088](https://issues.apache.org/jira/browse/HADOOP-18088) | *Major* | **Replace log4j 1.x with reload4j**

log4j 1 was replaced with reload4j which is fork of log4j 1.2.17 with the goal of fixing pressing security issues.

If you are depending on the hadoop artifacts in your build were explicitly excluding log4 artifacts, and now want to exclude the reload4j files, you will need to update your exclusion lists
\<exclusion\>
  \<groupId\>org.slf4j\</groupId\>
  \<artifactId\>slf4j-reload4j\</artifactId\>
\</exclusion\>
\<exclusion\>
  \<groupId\>ch.qos.reload4j\</groupId\>
  \<artifactId\>reload4j\</artifactId\>
\</exclusion\>



