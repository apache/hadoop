
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
# Apache Hadoop  3.3.3 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-17650](https://issues.apache.org/jira/browse/HADOOP-17650) | *Major* | **Fails to build using Maven 3.8.1**

In order to resolve build issues with Maven 3.8.1, we have to bump SolrJ to latest version 8.8.2 as of now. Solr is used by YARN application catalog. Hence, we would recommend upgrading Solr cluster accordingly before upgrading entire Hadoop cluster to 3.4.0 if the YARN application catalog service is used.


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



