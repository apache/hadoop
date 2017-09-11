
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
# Apache Hadoop  0.20.3 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-6382](https://issues.apache.org/jira/browse/HADOOP-6382) | *Major* | **publish hadoop jars to apache mvn repo.**

The hadoop jars are renamed  from previous hadoop-\<version\>-\<name\>.jar to hadoop-\<name\>-\<version\>.jar. Applications and documentation need to be updated to use the new file naming scheme.


---

* [HDFS-132](https://issues.apache.org/jira/browse/HDFS-132) | *Minor* | **Namenode in Safemode reports to Simon non-zero number of deleted files during startup**

With this incompatible change, under metrics context "dfs", the record name "FSDirectory" is no longer available. The metrics "files\_deleted" from the deleted record "FSDirectory" is now available in metrics context "dfs", record name "namenode" with the metrics name "FilesDeleted".


---

* [HADOOP-6701](https://issues.apache.org/jira/browse/HADOOP-6701) | *Minor* | ** Incorrect exit codes for "dfs -chown", "dfs -chgrp"**

Commands chmod, chown and chgrp now returns non zero exit code and an error message on failure instead of returning zero.



