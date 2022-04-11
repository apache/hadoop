
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
# Apache Hadoop  3.2.3 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [YARN-10036](https://issues.apache.org/jira/browse/YARN-10036) | *Major* | **Install yarnpkg and upgrade nodejs in Dockerfile**

In the Dockerfile, nodejs is upgraded to 8.17.0 and yarn 1.12.1 is installed.


---

* [HADOOP-16054](https://issues.apache.org/jira/browse/HADOOP-16054) | *Major* | **Update Dockerfile to use Bionic**

The build image has been upgraded to Bionic.


---

* [HDFS-15719](https://issues.apache.org/jira/browse/HDFS-15719) | *Critical* | **[Hadoop 3] Both NameNodes can crash simultaneously due to the short JN socket timeout**

The default value of the configuration hadoop.http.idle\_timeout.ms (how long does Jetty disconnect an idle connection) is changed from 10000 to 60000. 
This property is inlined during compile time, so an application that references this property must be recompiled in order for it to take effect.


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

* [HDFS-15942](https://issues.apache.org/jira/browse/HDFS-15942) | *Major* | **Increase Quota initialization threads**

The default quota initialization thread count during the NameNode startup process (dfs.namenode.quota.init-threads) is increased from 4 to 12.



