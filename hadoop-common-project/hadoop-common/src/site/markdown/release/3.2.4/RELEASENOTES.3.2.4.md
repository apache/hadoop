
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
# Apache Hadoop  3.2.4 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [YARN-10820](https://issues.apache.org/jira/browse/YARN-10820) | *Major* | **Make GetClusterNodesRequestPBImpl thread safe**

Added syncronization so that the "yarn node list" command does not fail intermittently


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



