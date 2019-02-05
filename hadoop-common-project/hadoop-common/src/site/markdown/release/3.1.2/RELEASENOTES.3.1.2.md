
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
# Apache Hadoop  3.1.2 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-15638](https://issues.apache.org/jira/browse/HADOOP-15638) | *Major* | **KMS Accept Queue Size default changed from 500 to 128 in Hadoop 3.x**

Restore the KMS accept queue size to 500 in Hadoop 3.x, making it the same as in Hadoop 2.x.


---

* [HADOOP-15677](https://issues.apache.org/jira/browse/HADOOP-15677) | *Major* | **WASB: Add support for StreamCapabilities**

WASB: Support for StreamCapabilities.


---

* [HADOOP-15684](https://issues.apache.org/jira/browse/HADOOP-15684) | *Critical* | **triggerActiveLogRoll stuck on dead name node, when ConnectTimeoutException happens.**

When a namenode A sends request RollEditLog to a remote NN, either the remote NN is standby or IO Exception happens, A should continue to try next NN, instead of getting stuck on the problematic one.  This Patch is based on trunk.


---

* [HADOOP-14445](https://issues.apache.org/jira/browse/HADOOP-14445) | *Major* | **Use DelegationTokenIssuer to create KMS delegation tokens that can authenticate to all KMS instances**

<!-- markdown -->

This patch improves the KMS delegation token issuing and authentication logic, to enable tokens to authenticate with a set of KMS servers. The change is backport compatible, in that it keeps the existing authentication logic as a fall back.

Historically, KMS delegation tokens have ip:port as service, making KMS clients only able to use the token to authenticate with the KMS server specified as ip:port, even though the token is shared among all KMS servers at server-side. After this patch, newly created tokens will have the KMS URL as service.

A `DelegationTokenIssuer` interface is introduced for token creation.


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

* [HADOOP-15996](https://issues.apache.org/jira/browse/HADOOP-15996) | *Major* | **Plugin interface to support more complex usernames in Hadoop**

This patch enables "Hadoop" and "MIT" as options for "hadoop.security.auth\_to\_local.mechanism" and defaults to 'hadoop'. This should be backward compatible with pre-HADOOP-12751.

This is basically HADOOP-12751 plus configurable + extended tests.



