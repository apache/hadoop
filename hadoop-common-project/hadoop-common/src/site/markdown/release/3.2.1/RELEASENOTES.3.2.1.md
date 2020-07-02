
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
# Apache Hadoop  3.2.1 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [YARN-8986](https://issues.apache.org/jira/browse/YARN-8986) | *Minor* | **publish all exposed ports to random ports when using bridge network**

support -p and -P for bridge type network;


---

* [YARN-9071](https://issues.apache.org/jira/browse/YARN-9071) | *Critical* | **NM and service AM don't have updated status for reinitialized containers**

In progress upgrade status may show READY state sooner than actual upgrade operations.  External caller to upgrade API is recommended to wait minimum 30 seconds before querying yarn app -status.


---

* [YARN-9084](https://issues.apache.org/jira/browse/YARN-9084) | *Major* | **Service Upgrade: With default readiness check, the status of upgrade is reported to be successful prematurely**

Improve transient container status accuracy for upgrade.


---

* [HADOOP-15922](https://issues.apache.org/jira/browse/HADOOP-15922) | *Major* | **DelegationTokenAuthenticationFilter get wrong doAsUser since it does not decode URL**

- Fix DelegationTokenAuthentication filter for incorrectly double encode doAs user parameter.


---

* [YARN-8761](https://issues.apache.org/jira/browse/YARN-8761) | *Major* | **Service AM support for decommissioning component instances**

- Component instance number is not linear increment when decommission feature is used.  Application with assumption of linear increment component instance number maybe impacted by introduction of this feature.


---

* [HDFS-14305](https://issues.apache.org/jira/browse/HDFS-14305) | *Major* | **Serial number in BlockTokenSecretManager could overlap between different namenodes**

NameNodes rely on independent block token key ranges to communicate block token identities to DataNodes and clients in a way that does not create conflicts between the tokens issued by multiple NameNodes. HDFS-6440 introduced the potential for overlaps in key ranges; this fixes the issue by creating 64 possible key ranges that NameNodes assign themselves to, allowing for up to 64 NameNodes to run safely. This limitation only applies within a single Namespace; there may be more than 64 NameNodes total spread among multiple federated Namespaces.


---

* [HDFS-14396](https://issues.apache.org/jira/browse/HDFS-14396) | *Blocker* | **Failed to load image from FSImageFile when downgrade from 3.x to 2.x**

During a rolling upgrade from Hadoop 2.x to 3.x, NameNode cannot persist erasure coding information, and therefore a user cannot start using erasure coding feature until finalize is done.


---

* [YARN-7055](https://issues.apache.org/jira/browse/YARN-7055) | *Major* | **YARN Timeline Service v.2: beta 1 / GA**

Application Timeline Server v2 is ready for production. It is GA from 3.2.1 release on wards.



