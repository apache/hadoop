
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
# Apache Hadoop  1.0.3 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-5528](https://issues.apache.org/jira/browse/HADOOP-5528) | *Major* | **Binary partitioner**

New BinaryPartitioner that partitions BinaryComparable keys by hashing a configurable part of the bytes array corresponding to the key.


---

* [HADOOP-8352](https://issues.apache.org/jira/browse/HADOOP-8352) | *Major* | **We should always generate a new configure script for the c++ code**

If you are compiling c++, the configure script will now be automatically regenerated as it should be.
This requires autoconf version 2.61 or greater.



