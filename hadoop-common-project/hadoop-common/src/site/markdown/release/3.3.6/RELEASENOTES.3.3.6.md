
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
# Apache Hadoop  3.3.6 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-18215](https://issues.apache.org/jira/browse/HADOOP-18215) | *Minor* | **Enhance WritableName to be able to return aliases for classes that use serializers**

If you have a SequenceFile with an old key or value class which has been renamed, you can use WritableName.addName to add an alias class. This functionality previously existed, but only worked for classes which extend Writable. It now works for any class, notably key or value classes which use io.serializations.



