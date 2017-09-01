
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
# Apache Hadoop  0.24.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-7547](https://issues.apache.org/jira/browse/HADOOP-7547) | *Minor* | **Fix the warning in writable classes.[ WritableComparable is a raw type. References to generic type WritableComparable\<T\> should be parameterized  ]**

**WARNING: No release note provided for this change.**


---

* [HADOOP-7507](https://issues.apache.org/jira/browse/HADOOP-7507) | *Major* | **jvm metrics all use the same namespace**

JVM metrics published to Ganglia now include the process name as part of the gmetric name.


---

* [HADOOP-7668](https://issues.apache.org/jira/browse/HADOOP-7668) | *Minor* | **Add a NetUtils method that can tell if an InetAddress belongs to local host**

closing again


---

* [HADOOP-7728](https://issues.apache.org/jira/browse/HADOOP-7728) | *Major* | **hadoop-setup-conf.sh should be modified to enable task memory manager**

Enable task memory management to be configurable via hadoop config setup script.



