
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

## Release 0.15.0 - 2007-10-19

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-1963](https://issues.apache.org/jira/browse/HADOOP-1963) | Code contribution of Kosmos Filesystem implementation of Hadoop Filesystem interface |  Major | fs | Sriram Rao | Sriram Rao |
| [HADOOP-1914](https://issues.apache.org/jira/browse/HADOOP-1914) | HDFS should have a NamenodeProtocol to allow  secondary namenodes and rebalancing processes to communicate with a primary namenode |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-1894](https://issues.apache.org/jira/browse/HADOOP-1894) | Add fancy graphs for mapred task statuses |  Major | . | Enis Soztutar | Enis Soztutar |
| [HADOOP-1880](https://issues.apache.org/jira/browse/HADOOP-1880) | SleepJob |  Major | . | Enis Soztutar | Enis Soztutar |
| [HADOOP-1851](https://issues.apache.org/jira/browse/HADOOP-1851) | Map output compression codec cannot be set independently of job output compression codec |  Major | . | Riccardo Boscolo | Arun C Murthy |
| [HADOOP-1822](https://issues.apache.org/jira/browse/HADOOP-1822) | Allow SOCKS proxy configuration to remotely access the DFS and submit Jobs |  Minor | ipc | Christophe Taton | Christophe Taton |
| [HADOOP-1809](https://issues.apache.org/jira/browse/HADOOP-1809) | Add link to irc channel #hadoop |  Major | . | Enis Soztutar | Enis Soztutar |
| [HADOOP-1727](https://issues.apache.org/jira/browse/HADOOP-1727) | Make ...hbase.io.MapWritable more generic so that it can be included in ...hadoop.io |  Minor | io | Jim Kellerman | Jim Kellerman |
| [HADOOP-1351](https://issues.apache.org/jira/browse/HADOOP-1351) | Want to kill a particular task or attempt |  Major | . | Owen O'Malley | Enis Soztutar |
| [HADOOP-789](https://issues.apache.org/jira/browse/HADOOP-789) | DFS shell should return a list of nodes for a file saying that where the blocks for these files are located. |  Minor | . | Mahadev konar | Mahadev konar |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-2046](https://issues.apache.org/jira/browse/HADOOP-2046) | Documentation: improve mapred javadocs |  Blocker | documentation | Arun C Murthy | Arun C Murthy |
| [HADOOP-1971](https://issues.apache.org/jira/browse/HADOOP-1971) | Constructing a JobConf without a class leads to a very misleading error message. |  Minor | . | Ted Dunning | Enis Soztutar |
| [HADOOP-1968](https://issues.apache.org/jira/browse/HADOOP-1968) | Wildcard input syntax (glob) should support {} |  Major | fs | eric baldeschwieler | Hairong Kuang |
| [HADOOP-1942](https://issues.apache.org/jira/browse/HADOOP-1942) | Increase the concurrency of transaction logging to edits log |  Blocker | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-1933](https://issues.apache.org/jira/browse/HADOOP-1933) | Consider include/exclude files while listing datanodes. |  Major | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-1926](https://issues.apache.org/jira/browse/HADOOP-1926) | Design/implement a set of compression benchmarks for the map-reduce framework |  Major | . | Arun C Murthy | Arun C Murthy |
| [HADOOP-1921](https://issues.apache.org/jira/browse/HADOOP-1921) | Save the configuration of completed/failed jobs and make them available via the web-ui. |  Major | . | Arun C Murthy | Amar Kamat |
| [HADOOP-1908](https://issues.apache.org/jira/browse/HADOOP-1908) | Restructure data node code so that block sending/receiving is seperated from data transfer header handling |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-1906](https://issues.apache.org/jira/browse/HADOOP-1906) | JobConf should warn about the existance of obsolete mapred-default.xml. |  Major | conf | Owen O'Malley | Arun C Murthy |
| [HADOOP-1881](https://issues.apache.org/jira/browse/HADOOP-1881) | Update documentation for hadoop's configuration post HADOOP-785 |  Major | documentation | Arun C Murthy | Arun C Murthy |
| [HADOOP-1878](https://issues.apache.org/jira/browse/HADOOP-1878) | Change priority feature in the job details JSP page misses spaces between each priority link |  Trivial | . | Thomas Friol | Thomas Friol |
| [HADOOP-1803](https://issues.apache.org/jira/browse/HADOOP-1803) | Generalize making contrib bin content executable in ant package target |  Minor | build | stack | stack |
| [HADOOP-1779](https://issues.apache.org/jira/browse/HADOOP-1779) | Small INodeDirectory enhancement to get all existing INodes components on a path |  Trivial | . | Christophe Taton | Christophe Taton |
| [HADOOP-1777](https://issues.apache.org/jira/browse/HADOOP-1777) | Typo issue in the job details JSP page |  Trivial | . | Thomas Friol | Thomas Friol |
| [HADOOP-1774](https://issues.apache.org/jira/browse/HADOOP-1774) | Remove use of INode.parent in Block CRC upgrade |  Major | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-1767](https://issues.apache.org/jira/browse/HADOOP-1767) | JobClient CLI cleanup and improvement |  Minor | . | Christophe Taton | Christophe Taton |
| [HADOOP-1766](https://issues.apache.org/jira/browse/HADOOP-1766) | Merging Block and BlockInfo classes on name-node. |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-1762](https://issues.apache.org/jira/browse/HADOOP-1762) | Namenode does not need to store storageID and datanodeID persistently |  Major | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-1759](https://issues.apache.org/jira/browse/HADOOP-1759) | File name should be represented by a byte array instead of a String |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-1756](https://issues.apache.org/jira/browse/HADOOP-1756) | Add toString() methods to some Writable types |  Major | io | Andrzej Bialecki | Andrzej Bialecki |
| [HADOOP-1750](https://issues.apache.org/jira/browse/HADOOP-1750) | We should log better if something goes wrong with the process fork |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-1744](https://issues.apache.org/jira/browse/HADOOP-1744) | Small cleanup of DistributedFileSystem and DFSClient (next) |  Trivial | . | Christophe Taton | Christophe Taton |
| [HADOOP-1743](https://issues.apache.org/jira/browse/HADOOP-1743) | INode refactoring |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-1731](https://issues.apache.org/jira/browse/HADOOP-1731) | contrib jar file names should include hadoop version number |  Major | . | Doug Cutting | Doug Cutting |
| [HADOOP-1718](https://issues.apache.org/jira/browse/HADOOP-1718) | Test coverage target in build files using clover |  Major | build | Nigel Daley | Nigel Daley |
| [HADOOP-1703](https://issues.apache.org/jira/browse/HADOOP-1703) | Small cleanup of DistributedFileSystem and DFSClient |  Trivial | . | Christophe Taton | Christophe Taton |
| [HADOOP-1693](https://issues.apache.org/jira/browse/HADOOP-1693) | Remove LOG members from PendingReplicationBlocks and ReplicationTargetChooser. |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-1687](https://issues.apache.org/jira/browse/HADOOP-1687) | Name-node memory size estimates and optimization proposal. |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-1667](https://issues.apache.org/jira/browse/HADOOP-1667) | organize CHANGES.txt messages into sections for future releases |  Major | documentation | Doug Cutting | Doug Cutting |
| [HADOOP-1654](https://issues.apache.org/jira/browse/HADOOP-1654) | IOUtils class |  Major | io | Enis Soztutar | Enis Soztutar |
| [HADOOP-1626](https://issues.apache.org/jira/browse/HADOOP-1626) | DFSAdmin. Help messages are missing for -finalizeUpgrade and -metasave. |  Blocker | . | Konstantin Shvachko | Lohit Vijayarenu |
| [HADOOP-1621](https://issues.apache.org/jira/browse/HADOOP-1621) | Make FileStatus a concrete class |  Major | fs | Chris Douglas | Chris Douglas |
| [HADOOP-1610](https://issues.apache.org/jira/browse/HADOOP-1610) | Add metrics for failed tasks |  Major | . | Devaraj Das | Devaraj Das |
| [HADOOP-1595](https://issues.apache.org/jira/browse/HADOOP-1595) | Add an option to setReplication method to wait for completion of replication |  Major | . | Christian Kunz | Tsz Wo Nicholas Sze |
| [HADOOP-1592](https://issues.apache.org/jira/browse/HADOOP-1592) | Print the diagnostic error messages for FAILED task-attempts to the user console via TaskCompletionEvents |  Major | . | Arun C Murthy | Amar Kamat |
| [HADOOP-1500](https://issues.apache.org/jira/browse/HADOOP-1500) | typo's in dfs webui |  Trivial | . | Nigel Daley | Nigel Daley |
| [HADOOP-1436](https://issues.apache.org/jira/browse/HADOOP-1436) | Redesign Tool and ToolBase API and releted functionality |  Major | util | Enis Soztutar | Enis Soztutar |
| [HADOOP-1425](https://issues.apache.org/jira/browse/HADOOP-1425) | Rework the various programs in 'examples' to extend ToolBase |  Minor | . | Arun C Murthy | Enis Soztutar |
| [HADOOP-1266](https://issues.apache.org/jira/browse/HADOOP-1266) | Remove DatanodeDescriptor dependency from NetworkTopology |  Major | . | Konstantin Shvachko | Hairong Kuang |
| [HADOOP-1231](https://issues.apache.org/jira/browse/HADOOP-1231) | Add generics to Mapper and Reducer interfaces |  Major | . | Owen O'Malley | Tom White |
| [HADOOP-1158](https://issues.apache.org/jira/browse/HADOOP-1158) | JobTracker should collect statistics of failed map output fetches, and take decisions to reexecute map tasks and/or restart the (possibly faulty) Jetty server on the TaskTracker |  Major | . | Devaraj Das | Arun C Murthy |
| [HADOOP-785](https://issues.apache.org/jira/browse/HADOOP-785) | Divide the server and client configurations |  Major | conf | Owen O'Malley | Arun C Murthy |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-2103](https://issues.apache.org/jira/browse/HADOOP-2103) | HADOOP-2046 caused some javadoc anomalies |  Major | documentation | Nigel Daley | Nigel Daley |
| [HADOOP-2102](https://issues.apache.org/jira/browse/HADOOP-2102) | ToolBase doesn't keep configuration |  Blocker | util | Dennis Kubes | Dennis Kubes |
| [HADOOP-2080](https://issues.apache.org/jira/browse/HADOOP-2080) | ChecksumFileSystem checksum file size incorrect. |  Blocker | fs | Richard Lee | Owen O'Malley |
| [HADOOP-2073](https://issues.apache.org/jira/browse/HADOOP-2073) | Datanode corruption if machine dies while writing VERSION file |  Blocker | . | Michael Bieniosek | Konstantin Shvachko |
| [HADOOP-2072](https://issues.apache.org/jira/browse/HADOOP-2072) | RawLocalFileStatus is causing Path problems |  Major | fs | Dennis Kubes |  |
| [HADOOP-2070](https://issues.apache.org/jira/browse/HADOOP-2070) | Test org.apache.hadoop.mapred.pipes.TestPipes.unknown failed |  Blocker | . | Mukund Madhugiri | Owen O'Malley |
| [HADOOP-2051](https://issues.apache.org/jira/browse/HADOOP-2051) | JobTracker's TaskCommitQueue is vulnerable to non-IOExceptions |  Blocker | . | Arun C Murthy | Arun C Murthy |
| [HADOOP-2048](https://issues.apache.org/jira/browse/HADOOP-2048) | DISTCP mapper should report progress more often |  Blocker | . | Runping Qi | Chris Douglas |
| [HADOOP-2044](https://issues.apache.org/jira/browse/HADOOP-2044) | Namenode encounters ClassCastException exceptions for INodeFileUnderConstruction |  Blocker | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-2033](https://issues.apache.org/jira/browse/HADOOP-2033) | In SequenceFile sync doesn't work unless the file is compressed (block or record) |  Blocker | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-2031](https://issues.apache.org/jira/browse/HADOOP-2031) | Lost tasktracker not handled properly leading to tips wrongly being kept as completed, and hence not rescheduled |  Blocker | . | Devaraj Das | Devaraj Das |
| [HADOOP-2028](https://issues.apache.org/jira/browse/HADOOP-2028) | distcp fails if log dir not specified and destination not present |  Blocker | util | Chris Douglas | Chris Douglas |
| [HADOOP-2026](https://issues.apache.org/jira/browse/HADOOP-2026) | Namenode prints out too many log lines for "Number of transactions" |  Blocker | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-2023](https://issues.apache.org/jira/browse/HADOOP-2023) | TestLocalDirAllocator fails on Windows |  Blocker | fs | Mukund Madhugiri | Hairong Kuang |
| [HADOOP-2022](https://issues.apache.org/jira/browse/HADOOP-2022) | Task times are not saved correctly (bug in hadoop-1874) |  Blocker | . | Devaraj Das | Amar Kamat |
| [HADOOP-2018](https://issues.apache.org/jira/browse/HADOOP-2018) | Broken pipe SocketException in DataNode$DataXceiver |  Blocker | . | Konstantin Shvachko | Hairong Kuang |
| [HADOOP-2016](https://issues.apache.org/jira/browse/HADOOP-2016) | Race condition in removing a KILLED task from tasktracker |  Blocker | . | Devaraj Das | Arun C Murthy |
| [HADOOP-1997](https://issues.apache.org/jira/browse/HADOOP-1997) | TestCheckpoint fails on Windows |  Blocker | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-1992](https://issues.apache.org/jira/browse/HADOOP-1992) | Sort validation is taking considerably longer than before |  Blocker | . | Mukund Madhugiri | Arun C Murthy |
| [HADOOP-1983](https://issues.apache.org/jira/browse/HADOOP-1983) | jobs using pipes interface with tasks not using java output format have a good chance of not updating progress and timing out |  Major | . | Christian Kunz | Owen O'Malley |
| [HADOOP-1978](https://issues.apache.org/jira/browse/HADOOP-1978) | Name-node should remove edits.new during startup rather than renaming it to edits. |  Blocker | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-1973](https://issues.apache.org/jira/browse/HADOOP-1973) | NPE at JobTracker startup.. |  Blocker | . | Gautam Kowshik | Amareshwari Sriramadasu |
| [HADOOP-1961](https://issues.apache.org/jira/browse/HADOOP-1961) | -get, -copyToLocal fail when  single filename is passed |  Blocker | . | Koji Noguchi | Raghu Angadi |
| [HADOOP-1959](https://issues.apache.org/jira/browse/HADOOP-1959) | Use of File.separator in StatusHttpServer prevents running Junit tests inside eclipse on Windows |  Minor | . | Jim Kellerman | Jim Kellerman |
| [HADOOP-1955](https://issues.apache.org/jira/browse/HADOOP-1955) | Corrupted block replication retries for ever |  Blocker | . | Koji Noguchi | Raghu Angadi |
| [HADOOP-1953](https://issues.apache.org/jira/browse/HADOOP-1953) | the job tracker should wait beteween calls to try and delete the system directory |  Blocker | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-1948](https://issues.apache.org/jira/browse/HADOOP-1948) | Spurious error message during block crc upgrade. |  Blocker | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-1946](https://issues.apache.org/jira/browse/HADOOP-1946) | du should be not called on every heartbeat |  Blocker | . | Konstantin Shvachko | Hairong Kuang |
| [HADOOP-1944](https://issues.apache.org/jira/browse/HADOOP-1944) | Maps which ran on trackers declared 'lost' are being marked as FAILED rather than KILLED |  Blocker | . | Arun C Murthy | Devaraj Das |
| [HADOOP-1940](https://issues.apache.org/jira/browse/HADOOP-1940) | TestDFSUpgradeFromImage doesn't shut down its MiniDFSCluster |  Major | test | Chris Douglas | Chris Douglas |
| [HADOOP-1935](https://issues.apache.org/jira/browse/HADOOP-1935) | NullPointerException in internalReleaseCreate |  Blocker | . | Konstantin Shvachko | dhruba borthakur |
| [HADOOP-1934](https://issues.apache.org/jira/browse/HADOOP-1934) | the os.name string on Mac OS contains spaces, which causes the c++ compilation to fail |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-1932](https://issues.apache.org/jira/browse/HADOOP-1932) | Test dfs.TestFileCreation.testFileCreation failed on Windows |  Blocker | test | Mukund Madhugiri | dhruba borthakur |
| [HADOOP-1930](https://issues.apache.org/jira/browse/HADOOP-1930) | Too many fetch-failures issue |  Blocker | . | Christian Kunz | Arun C Murthy |
| [HADOOP-1925](https://issues.apache.org/jira/browse/HADOOP-1925) | Hadoop Pipes doesn't compile on solaris |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-1910](https://issues.apache.org/jira/browse/HADOOP-1910) | Extra checks in DFS.create() are not necessary. |  Minor | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-1907](https://issues.apache.org/jira/browse/HADOOP-1907) | JobClient.runJob kills the job for failed tasks with no diagnostics |  Major | . | Christian Kunz | Christian Kunz |
| [HADOOP-1904](https://issues.apache.org/jira/browse/HADOOP-1904) | ArrayIndexOutOfBoundException in BlocksMap |  Blocker | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-1897](https://issues.apache.org/jira/browse/HADOOP-1897) | about.html page is there but not linked. |  Major | . | Enis Soztutar | Enis Soztutar |
| [HADOOP-1892](https://issues.apache.org/jira/browse/HADOOP-1892) | In the Job UI, some links don't work |  Major | . | Devaraj Das | Amar Kamat |
| [HADOOP-1890](https://issues.apache.org/jira/browse/HADOOP-1890) | Revert a debug patch. |  Trivial | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-1889](https://issues.apache.org/jira/browse/HADOOP-1889) | Fix path in EC2 scripts for building your own AMI |  Major | contrib/cloud | Tom White | Tom White |
| [HADOOP-1887](https://issues.apache.org/jira/browse/HADOOP-1887) | ArrayIndexOutOfBoundsException with trunk |  Major | . | Raghu Angadi | dhruba borthakur |
| [HADOOP-1885](https://issues.apache.org/jira/browse/HADOOP-1885) | Race condition in MiniDFSCluster shutdown |  Major | test | Chris Douglas | Chris Douglas |
| [HADOOP-1882](https://issues.apache.org/jira/browse/HADOOP-1882) | Remove extra '\*'s from FsShell.limitDecimal() |  Minor | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-1875](https://issues.apache.org/jira/browse/HADOOP-1875) | multiple dfs.client.buffer.dir directories are not treated as alternatives |  Blocker | fs | Christian Kunz | Hairong Kuang |
| [HADOOP-1874](https://issues.apache.org/jira/browse/HADOOP-1874) | lost task trackers -- jobs hang |  Blocker | . | Christian Kunz | Devaraj Das |
| [HADOOP-1846](https://issues.apache.org/jira/browse/HADOOP-1846) | DatanodeReport should distinguish live datanodes from dead datanodes |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-1840](https://issues.apache.org/jira/browse/HADOOP-1840) | Task's diagnostic messages are lost sometimes |  Critical | . | Arun C Murthy | Arun C Murthy |
| [HADOOP-1838](https://issues.apache.org/jira/browse/HADOOP-1838) | Files created with an pre-0.15 gets blocksize as zero, causing performance degradation |  Blocker | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-1832](https://issues.apache.org/jira/browse/HADOOP-1832) | listTables() returns duplicate tables |  Major | . | Andrew Hitchcock | Jim Kellerman |
| [HADOOP-1825](https://issues.apache.org/jira/browse/HADOOP-1825) | hadoop-daemon.sh script fails if HADOOP\_PID\_DIR doesn't exist |  Minor | scripts | Michael Bieniosek | Michael Bieniosek |
| [HADOOP-1819](https://issues.apache.org/jira/browse/HADOOP-1819) | The JobTracker should ensure that it is running on the right host. |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-1818](https://issues.apache.org/jira/browse/HADOOP-1818) | MutliFileInputFormat returns "empty" MultiFileSplit when number of paths \< number of splits |  Major | . | Thomas Friol | Thomas Friol |
| [HADOOP-1817](https://issues.apache.org/jira/browse/HADOOP-1817) | MultiFileSplit does not write and read the total length |  Major | . | Thomas Friol | Thomas Friol |
| [HADOOP-1812](https://issues.apache.org/jira/browse/HADOOP-1812) | TestIPC and TestRPC should use dynamically allocated ports |  Major | ipc | Doug Cutting | Doug Cutting |
| [HADOOP-1810](https://issues.apache.org/jira/browse/HADOOP-1810) | Incorrect Value type in MRBench (SmallJobs) |  Blocker | . | Devaraj Das | Devaraj Das |
| [HADOOP-1806](https://issues.apache.org/jira/browse/HADOOP-1806) | DfsTask no longer compiles |  Major | build | Chris Douglas | Chris Douglas |
| [HADOOP-1795](https://issues.apache.org/jira/browse/HADOOP-1795) | Task.moveTaskOutputs is escaping special characters in output filenames |  Critical | . | Frédéric Bertin | Frédéric Bertin |
| [HADOOP-1792](https://issues.apache.org/jira/browse/HADOOP-1792) | df command doesn't exist under windows |  Major | fs | Benjamin Francisoud | Mahadev konar |
| [HADOOP-1788](https://issues.apache.org/jira/browse/HADOOP-1788) | Increase the buffer size of pipes from 1k to 128k |  Blocker | . | Owen O'Malley | Amareshwari Sriramadasu |
| [HADOOP-1783](https://issues.apache.org/jira/browse/HADOOP-1783) | keyToPath in Jets3tFileSystemStore needs to return absolute path |  Major | fs/s3 | Ahad Rana | Tom White |
| [HADOOP-1775](https://issues.apache.org/jira/browse/HADOOP-1775) | MapWritable and SortedMapWritable - Writable problems |  Major | io | Jim Kellerman | Jim Kellerman |
| [HADOOP-1772](https://issues.apache.org/jira/browse/HADOOP-1772) | Hadoop does not run in Cygwin in Windows |  Critical | scripts | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-1771](https://issues.apache.org/jira/browse/HADOOP-1771) | streaming hang when IOException in MROutputThread. (NPE) |  Blocker | . | Koji Noguchi | Lohit Vijayarenu |
| [HADOOP-1758](https://issues.apache.org/jira/browse/HADOOP-1758) | processing escapes in a jute record is quadratic |  Blocker | record | Dick King | Vivek Ratan |
| [HADOOP-1749](https://issues.apache.org/jira/browse/HADOOP-1749) | TestDFSUpgrade some times fails with an assert |  Major | . | Raghu Angadi | Enis Soztutar |
| [HADOOP-1748](https://issues.apache.org/jira/browse/HADOOP-1748) | Task Trackers fail to launch tasks when they have relative log directories configured |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-1739](https://issues.apache.org/jira/browse/HADOOP-1739) | ConnectException in TaskTracker Child |  Major | . | Srikanth Kakani | Doug Cutting |
| [HADOOP-1708](https://issues.apache.org/jira/browse/HADOOP-1708) | make files visible in the namespace as soon as they are created |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-1695](https://issues.apache.org/jira/browse/HADOOP-1695) | Secondary Namenode halt when SocketTimeoutException at startup |  Blocker | . | Koji Noguchi | dhruba borthakur |
| [HADOOP-1692](https://issues.apache.org/jira/browse/HADOOP-1692) | DfsTask cache interferes with operation |  Minor | util | Chris Douglas | Chris Douglas |
| [HADOOP-1689](https://issues.apache.org/jira/browse/HADOOP-1689) | .sh scripts do not work on Solaris |  Minor | scripts | David Biesack | Doug Cutting |
| [HADOOP-1656](https://issues.apache.org/jira/browse/HADOOP-1656) | HDFS does not record the blocksize for a file |  Major | . | Sameer Paranjpye | dhruba borthakur |
| [HADOOP-1651](https://issues.apache.org/jira/browse/HADOOP-1651) | Some improvements in progress reporting |  Major | . | Devaraj Das | Devaraj Das |
| [HADOOP-1636](https://issues.apache.org/jira/browse/HADOOP-1636) | constant should be user-configurable: MAX\_COMPLETE\_USER\_JOBS\_IN\_MEMORY |  Major | . | Michael Bieniosek | Michael Bieniosek |
| [HADOOP-1601](https://issues.apache.org/jira/browse/HADOOP-1601) | GenericWritable should use ReflectionUtils.newInstance to avoid problems with classloaders |  Major | io | Owen O'Malley | Enis Soztutar |
| [HADOOP-1573](https://issues.apache.org/jira/browse/HADOOP-1573) | Support for 0 reducers in PIPES |  Major | . | Christian Kunz | Owen O'Malley |
| [HADOOP-1569](https://issues.apache.org/jira/browse/HADOOP-1569) | distcp should use the Path -\> FileSystem interface like the rest of Hadoop |  Major | util | Owen O'Malley | Chris Douglas |
| [HADOOP-1565](https://issues.apache.org/jira/browse/HADOOP-1565) | DFSScalability: reduce memory usage of namenode |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-1463](https://issues.apache.org/jira/browse/HADOOP-1463) | dfs.datanode.du.reserved semantics being violated |  Blocker | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-1316](https://issues.apache.org/jira/browse/HADOOP-1316) | "Go to parent directory" does not work on windows. |  Minor | . | Konstantin Shvachko | Mahadev konar |
| [HADOOP-1076](https://issues.apache.org/jira/browse/HADOOP-1076) | Periodic checkpointing cannot resume if the secondary name-node fails. |  Major | . | Konstantin Shvachko | dhruba borthakur |
| [HADOOP-1018](https://issues.apache.org/jira/browse/HADOOP-1018) | Single lost heartbeat leads to a "Lost task tracker" |  Major | . | Andrzej Bialecki | Arun C Murthy |
| [HADOOP-999](https://issues.apache.org/jira/browse/HADOOP-999) | DFS Client should create file when the user creates the file |  Major | . | Owen O'Malley | Tsz Wo Nicholas Sze |
| [HADOOP-932](https://issues.apache.org/jira/browse/HADOOP-932) | File locking interface and implementation should be remvoed. |  Minor | fs | Raghu Angadi | Raghu Angadi |
| [HADOOP-795](https://issues.apache.org/jira/browse/HADOOP-795) | hdfs -cp /a/b/c  /x/y    acts like   hdfs -cp /a/b/c/\*   /x/y |  Minor | . | arkady borkovsky | Mahadev konar |
| [HADOOP-120](https://issues.apache.org/jira/browse/HADOOP-120) | Reading an ArrayWriter does not work because valueClass does not get initialized |  Major | io | Dick King | Cameron Pope |
| [HADOOP-89](https://issues.apache.org/jira/browse/HADOOP-89) | files are not visible until they are closed |  Critical | . | Yoram Arnon | dhruba borthakur |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-1879](https://issues.apache.org/jira/browse/HADOOP-1879) | Warnings With JDK1.6.0\_02 |  Minor | . | Nilay Vaish | Nilay Vaish |


