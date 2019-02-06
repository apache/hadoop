
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
# Apache Hadoop  2.7.6 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-9477](https://issues.apache.org/jira/browse/HADOOP-9477) | *Major* | **Add posixGroups support for LDAP groups mapping service**

Add posixGroups support for LDAP groups mapping service. The change in LDAPGroupMapping is compatible with previous scenario. In LDAP, the group mapping between {{posixAccount}} and {{posixGroup}} is different from the general LDAPGroupMapping, one of the differences is the {{"memberUid"}} will be used to mapping {{posixAccount}} and {{posixGroup}}. The feature will handle the mapping in internal when configuration {{hadoop.security.group.mapping.ldap.search.filter.user}} is set as "posixAccount" and {{hadoop.security.group.mapping.ldap.search.filter.group}} is "posixGroup".


---

* [HADOOP-13105](https://issues.apache.org/jira/browse/HADOOP-13105) | *Major* | **Support timeouts in LDAP queries in LdapGroupsMapping.**

This patch adds two new config keys for supporting timeouts in LDAP query operations. The property "hadoop.security.group.mapping.ldap.connection.timeout.ms" is the connection timeout (in milliseconds), within which period if the LDAP provider doesn't establish a connection, it will abort the connect attempt. The property "hadoop.security.group.mapping.ldap.read.timeout.ms" is the read timeout (in milliseconds), within which period if the LDAP provider doesn't get a LDAP response, it will abort the read attempt.


---

* [HADOOP-13263](https://issues.apache.org/jira/browse/HADOOP-13263) | *Major* | **Reload cached groups in background after expiry**

hadoop.security.groups.cache.background.reload can be set to true to enable background reload of expired groups cache entries. This setting can improve the performance of services that use Groups.java (e.g. the NameNode) when group lookups are slow. The setting is disabled by default.
