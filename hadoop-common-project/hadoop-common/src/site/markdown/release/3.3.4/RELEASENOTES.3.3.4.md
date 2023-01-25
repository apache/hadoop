
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
# Apache Hadoop  3.3.4 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HDFS-16453](https://issues.apache.org/jira/browse/HDFS-16453) | *Major* | **Upgrade okhttp from 2.7.5 to 4.9.3**

okhttp has been updated to address CVE-2021-0341


---

* [HADOOP-18237](https://issues.apache.org/jira/browse/HADOOP-18237) | *Major* | **Upgrade Apache Xerces Java to 2.12.2**

Apache Xerces has been updated  to 2.12.2 to fix CVE-2022-23437


---

* [HADOOP-18307](https://issues.apache.org/jira/browse/HADOOP-18307) | *Major* | **remove hadoop-cos as a dependency of hadoop-cloud-storage**

We have recently become aware that libraries which include a shaded apache httpclient libraries (hadoop-client-runtime.jar, aws-java-sdk-bundle.jar, gcs-connector-shaded.jar, cos\_api-bundle-5.6.19.jar) all load and use the unshaded resource mozilla/public-suffix-list.txt. If an out of date version of this is found on the classpath first, attempts to negotiate TLS connections may fail with the error "Certificate doesn't match any of the subject alternative names". This release does not declare the hadoop-cos library to be a dependency of the hadoop-cloud-storage POM, so applications depending on that module are no longer exposed to this issue. If an application requires use of the hadoop-cos module, please declare an explicit dependency.


---

* [HADOOP-18332](https://issues.apache.org/jira/browse/HADOOP-18332) | *Major* | **Remove rs-api dependency by downgrading jackson to 2.12.7**

Downgrades Jackson from 2.13.2 to 2.12.7 to fix class conflicts in downstream projects. This version of jackson does contain the fix for CVE-2020-36518.


---

* [HADOOP-18079](https://issues.apache.org/jira/browse/HADOOP-18079) | *Major* | **Upgrade Netty to 4.1.77.Final**

Netty has been updated to address CVE-2019-20444, CVE-2019-20445 and CVE-2022-24823


---

* [HADOOP-18344](https://issues.apache.org/jira/browse/HADOOP-18344) | *Major* | **AWS SDK update to 1.12.262 to address jackson  CVE-2018-7489**

The AWS SDK has been updated to 1.12.262 to address jackson CVE-2018-7489



