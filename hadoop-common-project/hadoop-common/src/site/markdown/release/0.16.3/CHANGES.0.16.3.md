
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
# Apache Hadoop Changelog

## Release 0.16.3 - 2008-04-16



### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-3010](https://issues.apache.org/jira/browse/HADOOP-3010) | ConcurrentModificationException from org.apache.hadoop.ipc.Server$Responder in JobTracker |  Major | ipc | Amar Kamat | Raghu Angadi |
| [HADOOP-3159](https://issues.apache.org/jira/browse/HADOOP-3159) | FileSystem cache keep overwriting cached value |  Blocker | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-3154](https://issues.apache.org/jira/browse/HADOOP-3154) | Job successful but dropping records (when disk full) |  Blocker | . | Koji Noguchi | Devaraj Das |
| [HADOOP-3139](https://issues.apache.org/jira/browse/HADOOP-3139) | DistributedFileSystem.close() deadlock and FileSystem.closeAll() warning |  Blocker | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-3195](https://issues.apache.org/jira/browse/HADOOP-3195) | TestFileSystem fails randomly |  Minor | test | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-3069](https://issues.apache.org/jira/browse/HADOOP-3069) | A failure on SecondaryNameNode truncates the primary NameNode image. |  Blocker | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-3182](https://issues.apache.org/jira/browse/HADOOP-3182) | JobClient creates submitJobDir with SYSTEM\_DIR\_PERMISSION ( rwx-wx-wx) |  Blocker | . | Lohit Vijayarenu | Tsz Wo Nicholas Sze |


