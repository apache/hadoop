
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

## Release 0.24.0 - Unreleased (as of 2017-08-28)

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-7547](https://issues.apache.org/jira/browse/HADOOP-7547) | Fix the warning in writable classes.[ WritableComparable is a raw type. References to generic type WritableComparable\<T\> should be parameterized  ] |  Minor | io | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-7507](https://issues.apache.org/jira/browse/HADOOP-7507) | jvm metrics all use the same namespace |  Major | metrics | Jeff Bean | Alejandro Abdelnur |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-7594](https://issues.apache.org/jira/browse/HADOOP-7594) | Support HTTP REST in HttpServer |  Major | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-7705](https://issues.apache.org/jira/browse/HADOOP-7705) | Add a log4j back end that can push out JSON data, one per line |  Minor | util | Steve Loughran | Steve Loughran |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-7595](https://issues.apache.org/jira/browse/HADOOP-7595) | Upgrade dependency to Avro 1.5.3 |  Major | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-7668](https://issues.apache.org/jira/browse/HADOOP-7668) | Add a NetUtils method that can tell if an InetAddress belongs to local host |  Minor | util | Suresh Srinivas | Steve Loughran |
| [HADOOP-7642](https://issues.apache.org/jira/browse/HADOOP-7642) | create hadoop-dist module where TAR stitching would happen |  Major | build | Alejandro Abdelnur | Tom White |
| [HADOOP-7737](https://issues.apache.org/jira/browse/HADOOP-7737) | normalize hadoop-mapreduce & hadoop-dist dist/tar build with common/hdfs |  Major | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-7743](https://issues.apache.org/jira/browse/HADOOP-7743) | Add Maven profile to create a full source tarball |  Major | build | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-7509](https://issues.apache.org/jira/browse/HADOOP-7509) | Improve message when Authentication is required |  Trivial | . | Ravi Prakash | Ravi Prakash |
| [HADOOP-7772](https://issues.apache.org/jira/browse/HADOOP-7772) | javadoc the topology classes |  Trivial | . | Steve Loughran | Steve Loughran |
| [HADOOP-7360](https://issues.apache.org/jira/browse/HADOOP-7360) | FsShell does not preserve relative paths with globs |  Major | fs | Daryn Sharp | Kihwal Lee |
| [HADOOP-7763](https://issues.apache.org/jira/browse/HADOOP-7763) | Add top-level navigation to APT docs |  Major | documentation | Tom White | Tom White |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-7589](https://issues.apache.org/jira/browse/HADOOP-7589) | Prefer mvn test -DskipTests over mvn compile in test-patch.sh |  Major | build | Robert Joseph Evans | Robert Joseph Evans |
| [HADOOP-7576](https://issues.apache.org/jira/browse/HADOOP-7576) | Fix findbugs warnings in Hadoop Auth (Alfredo) |  Major | security | Tom White | Tsz Wo Nicholas Sze |
| [HADOOP-7593](https://issues.apache.org/jira/browse/HADOOP-7593) | AssertionError in TestHttpServer.testMaxThreads() |  Major | test | Tsz Wo Nicholas Sze | Uma Maheswara Rao G |
| [HADOOP-7606](https://issues.apache.org/jira/browse/HADOOP-7606) | Upgrade Jackson to version 1.7.1 to match the version required by Jersey |  Major | test | Aaron T. Myers | Alejandro Abdelnur |
| [HADOOP-7568](https://issues.apache.org/jira/browse/HADOOP-7568) | SequenceFile should not print into stdout |  Major | io | Konstantin Shvachko | Plamen Jeliazkov |
| [HADOOP-7599](https://issues.apache.org/jira/browse/HADOOP-7599) | Improve hadoop setup conf script to setup secure Hadoop cluster |  Major | scripts | Eric Yang | Eric Yang |
| [HADOOP-7608](https://issues.apache.org/jira/browse/HADOOP-7608) | SnappyCodec check for Hadoop native lib is wrong |  Major | io | Alejandro Abdelnur | Alejandro Abdelnur |
| [HADOOP-7663](https://issues.apache.org/jira/browse/HADOOP-7663) | TestHDFSTrash failing on 22 |  Major | test | Mayank Bansal | Mayank Bansal |
| [HADOOP-7721](https://issues.apache.org/jira/browse/HADOOP-7721) | dfs.web.authentication.kerberos.principal expects the full hostname and does not replace \_HOST with the hostname |  Major | . | Arpit Gupta | Jitendra Nath Pandey |
| [HADOOP-7745](https://issues.apache.org/jira/browse/HADOOP-7745) | I switched variable names in HADOOP-7509 |  Major | . | Ravi Prakash | Ravi Prakash |
| [HADOOP-7766](https://issues.apache.org/jira/browse/HADOOP-7766) | The auth to local mappings are not being respected, with webhdfs and security enabled. |  Major | . | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [HADOOP-7728](https://issues.apache.org/jira/browse/HADOOP-7728) | hadoop-setup-conf.sh should be modified to enable task memory manager |  Major | conf | Ramya Sunil | Ramya Sunil |
| [HADOOP-7778](https://issues.apache.org/jira/browse/HADOOP-7778) | FindBugs warning in Token.getKind() |  Major | . | Tom White | Tom White |
| [HADOOP-7771](https://issues.apache.org/jira/browse/HADOOP-7771) | NPE when running hdfs dfs -copyToLocal, -get etc |  Blocker | . | John George | John George |
| [HADOOP-7782](https://issues.apache.org/jira/browse/HADOOP-7782) | Aggregate project javadocs |  Critical | build | Arun C Murthy | Tom White |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-7469](https://issues.apache.org/jira/browse/HADOOP-7469) | add a standard handler for socket connection problems which improves diagnostics |  Minor | util | Steve Loughran | Steve Loughran |
| [HADOOP-7564](https://issues.apache.org/jira/browse/HADOOP-7564) | Remove test-patch SVN externals |  Major | . | Tom White | Tom White |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-7579](https://issues.apache.org/jira/browse/HADOOP-7579) | Rename package names from alfredo to auth |  Major | security | Alejandro Abdelnur | Alejandro Abdelnur |


