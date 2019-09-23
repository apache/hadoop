
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

## Release 0.14.0 - 2007-08-20



### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-234](https://issues.apache.org/jira/browse/HADOOP-234) | Hadoop Pipes for writing map/reduce jobs in C++ and python |  Major | . | Sanjay Dahiya | Owen O'Malley |
| [HADOOP-1379](https://issues.apache.org/jira/browse/HADOOP-1379) | Integrate Findbugs into nightly build process |  Major | test | Nigel Daley | Nigel Daley |
| [HADOOP-1447](https://issues.apache.org/jira/browse/HADOOP-1447) | Support for textInputFormat in contrib/data\_join |  Minor | . | Senthil Subramanian | Senthil Subramanian |
| [HADOOP-1469](https://issues.apache.org/jira/browse/HADOOP-1469) | Asynchronous table creation |  Minor | . | James Kennedy | stack |
| [HADOOP-1377](https://issues.apache.org/jira/browse/HADOOP-1377) | Creation time and modification time for hadoop files and directories |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-1515](https://issues.apache.org/jira/browse/HADOOP-1515) | MultiFileSplit, MultiFileInputFormat |  Major | . | Enis Soztutar | Enis Soztutar |
| [HADOOP-1508](https://issues.apache.org/jira/browse/HADOOP-1508) | ant Task for FsShell operations |  Minor | build, fs | Chris Douglas | Chris Douglas |
| [HADOOP-1570](https://issues.apache.org/jira/browse/HADOOP-1570) | Add a per-job configuration knob to control loading of native hadoop libraries |  Major | io | Arun C Murthy | Arun C Murthy |
| [HADOOP-1433](https://issues.apache.org/jira/browse/HADOOP-1433) | Add job priority |  Minor | . | Johan Oskarsson | Johan Oskarsson |
| [HADOOP-1597](https://issues.apache.org/jira/browse/HADOOP-1597) | Distributed upgrade status reporting and post upgrade features. |  Blocker | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-1562](https://issues.apache.org/jira/browse/HADOOP-1562) | Report Java VM metrics |  Major | metrics | David Bowen | David Bowen |
| [HADOOP-1134](https://issues.apache.org/jira/browse/HADOOP-1134) | Block level CRCs in HDFS |  Major | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-1568](https://issues.apache.org/jira/browse/HADOOP-1568) | NameNode Schema for HttpFileSystem |  Major | fs | Chris Douglas | Chris Douglas |
| [HADOOP-1437](https://issues.apache.org/jira/browse/HADOOP-1437) | Eclipse plugin for developing and executing MapReduce programs on Hadoop |  Major | . | Eugene Hung | Christophe Taton |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-1343](https://issues.apache.org/jira/browse/HADOOP-1343) | Deprecate the Configuration.set(String,Object) method and make Configuration Iterable |  Major | conf | Owen O'Malley | Owen O'Malley |
| [HADOOP-1342](https://issues.apache.org/jira/browse/HADOOP-1342) | A configurable limit on the number of unique values should be set on the UniqueValueCount and ValueHistogram aggregators |  Major | . | Runping Qi | Runping Qi |
| [HADOOP-1340](https://issues.apache.org/jira/browse/HADOOP-1340) | md5 file in filecache should inherit replication factor from the file it belongs to. |  Major | . | Christian Kunz | dhruba borthakur |
| [HADOOP-894](https://issues.apache.org/jira/browse/HADOOP-894) | dfs client protocol should allow asking for parts of the block map |  Major | . | Owen O'Malley | Konstantin Shvachko |
| [HADOOP-1413](https://issues.apache.org/jira/browse/HADOOP-1413) | A new example to do tile placements using distributed dancing links |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-800](https://issues.apache.org/jira/browse/HADOOP-800) | More improvements to DFS browsing WI |  Major | . | arkady borkovsky | Enis Soztutar |
| [HADOOP-1408](https://issues.apache.org/jira/browse/HADOOP-1408) | fix warning about cast of Map\<String, Map\<String, JobInfo\>\> in jobhistory.jsp |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-1376](https://issues.apache.org/jira/browse/HADOOP-1376) | RandomWriter should be tweaked to generate input data for terasort |  Major | . | Devaraj Das | Devaraj Das |
| [HADOOP-1429](https://issues.apache.org/jira/browse/HADOOP-1429) | RPC Server won't go quietly |  Minor | ipc | stack | stack |
| [HADOOP-1450](https://issues.apache.org/jira/browse/HADOOP-1450) | checksums should be closer to data generation and consumption |  Major | fs | Doug Cutting | Doug Cutting |
| [HADOOP-1467](https://issues.apache.org/jira/browse/HADOOP-1467) | Remove redundant counters from WordCount example |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-1438](https://issues.apache.org/jira/browse/HADOOP-1438) | Grammatical / wording / copy edits for Hadoop Distributed File System: Architecture and Design white paper |  Trivial | documentation | Luke Nezda |  |
| [HADOOP-1457](https://issues.apache.org/jira/browse/HADOOP-1457) | Counters for monitoring task assignments |  Minor | . | Devaraj Das | Arun C Murthy |
| [HADOOP-1417](https://issues.apache.org/jira/browse/HADOOP-1417) | Exclude some Findbugs detectors |  Minor | build | Nigel Daley | Nigel Daley |
| [HADOOP-1320](https://issues.apache.org/jira/browse/HADOOP-1320) | Rewrite 'random-writer' to use '-reducer NONE' |  Minor | . | Arun C Murthy | Arun C Murthy |
| [HADOOP-1484](https://issues.apache.org/jira/browse/HADOOP-1484) | Kill jobs from web interface |  Minor | . | Enis Soztutar | Enis Soztutar |
| [HADOOP-1003](https://issues.apache.org/jira/browse/HADOOP-1003) | Proposal to batch commits to edits log. |  Major | . | Raghu Angadi | dhruba borthakur |
| [HADOOP-1023](https://issues.apache.org/jira/browse/HADOOP-1023) | better links to mailing list archives |  Major | documentation | Daniel Naber | Tom White |
| [HADOOP-1462](https://issues.apache.org/jira/browse/HADOOP-1462) | Better progress reporting from a Task |  Major | . | Vivek Ratan | Vivek Ratan |
| [HADOOP-1440](https://issues.apache.org/jira/browse/HADOOP-1440) | JobClient should not sort input-splits |  Major | . | Milind Bhandarkar | Senthil Subramanian |
| [HADOOP-1455](https://issues.apache.org/jira/browse/HADOOP-1455) | Allow any key-value pair on the command line of 'hadoop pipes' to be added to the JobConf |  Major | . | Christian Kunz | Devaraj Das |
| [HADOOP-1147](https://issues.apache.org/jira/browse/HADOOP-1147) | remove all @author tags from source |  Minor | . | Doug Cutting | Doug Cutting |
| [HADOOP-1283](https://issues.apache.org/jira/browse/HADOOP-1283) | Eliminate internal UTF8 to String and vice versa conversions in the name-node. |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-1518](https://issues.apache.org/jira/browse/HADOOP-1518) | Add session id to metric data |  Major | . | David Bowen | David Bowen |
| [HADOOP-1292](https://issues.apache.org/jira/browse/HADOOP-1292) | dfs -copyToLocal should guarantee file is complete |  Major | . | eric baldeschwieler | Tsz Wo Nicholas Sze |
| [HADOOP-1028](https://issues.apache.org/jira/browse/HADOOP-1028) | Servers should log startup and shutdown messages |  Major | . | Nigel Daley | Tsz Wo Nicholas Sze |
| [HADOOP-1485](https://issues.apache.org/jira/browse/HADOOP-1485) | Metrics should be there for reporting shuffle failures/successes |  Major | . | Devaraj Das | Devaraj Das |
| [HADOOP-1286](https://issues.apache.org/jira/browse/HADOOP-1286) | Distributed cluster upgrade |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-1580](https://issues.apache.org/jira/browse/HADOOP-1580) | provide better error message when subprocesses fail in hadoop streaming |  Minor | . | John Heidemann | John Heidemann |
| [HADOOP-1473](https://issues.apache.org/jira/browse/HADOOP-1473) | Make jobids unique across jobtracker restarts |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-1582](https://issues.apache.org/jira/browse/HADOOP-1582) | hdfsRead and hdfsPread should return 0 instead of -1 at end-of-file. |  Minor | . | Christian Kunz | Christian Kunz |
| [HADOOP-1470](https://issues.apache.org/jira/browse/HADOOP-1470) | Rework FSInputChecker and FSOutputSummer to support checksum code sharing between ChecksumFileSystem and block level crc dfs |  Major | fs | Hairong Kuang | Hairong Kuang |
| [HADOOP-1585](https://issues.apache.org/jira/browse/HADOOP-1585) | GenericWritable should use generics |  Minor | io | Espen Amble Kolstad | Espen Amble Kolstad |
| [HADOOP-1547](https://issues.apache.org/jira/browse/HADOOP-1547) | Provide examples for aggregate library |  Major | . | Tom White | Runping Qi |
| [HADOOP-1620](https://issues.apache.org/jira/browse/HADOOP-1620) | FileSystem should have fewer abstract methods |  Major | . | Doug Cutting | Doug Cutting |
| [HADOOP-1478](https://issues.apache.org/jira/browse/HADOOP-1478) | The blockStream of DFSClient.FSInputStream should not be buffered |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-1653](https://issues.apache.org/jira/browse/HADOOP-1653) | FSDirectory class code cleanup |  Trivial | . | Christophe Taton | Christophe Taton |
| [HADOOP-1066](https://issues.apache.org/jira/browse/HADOOP-1066) | http://lucene.apache.org/hadoop/ front page is not user-friendly |  Minor | documentation | Marco Nicosia | Doug Cutting |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-1197](https://issues.apache.org/jira/browse/HADOOP-1197) | The misleading Configuration.set(String, Object) should be removed |  Minor | conf | Owen O'Malley | Owen O'Malley |
| [HADOOP-1344](https://issues.apache.org/jira/browse/HADOOP-1344) | getJobName not accessible from JobClient |  Minor | . | Michael Bieniosek |  |
| [HADOOP-1355](https://issues.apache.org/jira/browse/HADOOP-1355) | Possible null pointer dereference in TaskLogAppender.append(LoggingEvent) |  Minor | . | Nigel Daley | Arun C Murthy |
| [HADOOP-1357](https://issues.apache.org/jira/browse/HADOOP-1357) | Call to equals() comparing different types in CopyFiles.cleanup(Configuration, JobConf, String, String) |  Minor | util | Nigel Daley | Arun C Murthy |
| [HADOOP-1335](https://issues.apache.org/jira/browse/HADOOP-1335) | C++ reducers under hadoop-pipes are not started when there are no key-value pairs to be reduced |  Major | . | Christian Kunz | Owen O'Malley |
| [HADOOP-1359](https://issues.apache.org/jira/browse/HADOOP-1359) | Variable dereferenced then later checked for null |  Minor | . | Nigel Daley | Hairong Kuang |
| [HADOOP-1364](https://issues.apache.org/jira/browse/HADOOP-1364) | Inconsistent synchronization of SequenceFile$Reader.noBufferedValues; locked 66% of time |  Minor | io | Nigel Daley | Owen O'Malley |
| [HADOOP-1390](https://issues.apache.org/jira/browse/HADOOP-1390) | Inconsistent Synchronization cleanup for {Configuration, TaskLog, MapTask, Server}.java |  Minor | conf, ipc | Devaraj Das | Devaraj Das |
| [HADOOP-1393](https://issues.apache.org/jira/browse/HADOOP-1393) | using Math.abs(Random.getInt()) does not guarantee a positive number |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-1387](https://issues.apache.org/jira/browse/HADOOP-1387) | FindBugs -\> Performance |  Major | . | Arun C Murthy | Arun C Murthy |
| [HADOOP-1406](https://issues.apache.org/jira/browse/HADOOP-1406) | Metrics based on Map-Reduce Counters are not cleaned up |  Major | . | David Bowen | David Bowen |
| [HADOOP-1394](https://issues.apache.org/jira/browse/HADOOP-1394) | FindBugs : Performance : in dfs |  Major | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-1226](https://issues.apache.org/jira/browse/HADOOP-1226) | makeQualified should return an instance of a DfsPath when passed  a DfsPath |  Major | . | Koji Noguchi | dhruba borthakur |
| [HADOOP-1443](https://issues.apache.org/jira/browse/HADOOP-1443) | TestFileCorruption fails with ArrayIndexOutOfBoundsException |  Critical | . | Nigel Daley | Konstantin Shvachko |
| [HADOOP-1461](https://issues.apache.org/jira/browse/HADOOP-1461) | Corner-case deadlock in TaskTracker |  Blocker | . | Arun C Murthy | Arun C Murthy |
| [HADOOP-1446](https://issues.apache.org/jira/browse/HADOOP-1446) | Metrics from the TaskTracker are updated only when map/reduce tasks start/end/fail |  Major | . | Devaraj Das | Devaraj Das |
| [HADOOP-1414](https://issues.apache.org/jira/browse/HADOOP-1414) | Findbugs - Bad Practice |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-1392](https://issues.apache.org/jira/browse/HADOOP-1392) | FindBugs : Fix some correctness bugs reported in DFS, FS, etc. |  Major | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-1412](https://issues.apache.org/jira/browse/HADOOP-1412) | FindBugs: Dodgy bugs in fs, filecache, io, and util packages |  Major | . | Hairong Kuang | Hairong Kuang |
| [HADOOP-1479](https://issues.apache.org/jira/browse/HADOOP-1479) | NPE in HStore#get if StoreFile only has keys \< than passed key |  Minor | . | stack | stack |
| [HADOOP-1261](https://issues.apache.org/jira/browse/HADOOP-1261) | Restart of the same data-node should not generate edits log records. |  Minor | . | Konstantin Shvachko | Raghu Angadi |
| [HADOOP-1311](https://issues.apache.org/jira/browse/HADOOP-1311) | Bug in BytesWritable.set(byte[] newData, int offset, int length) |  Major | io | Srikanth Kakani | dhruba borthakur |
| [HADOOP-1456](https://issues.apache.org/jira/browse/HADOOP-1456) | TestDecommission fails with assertion  Number of replicas for block1 expected:\<3\> but was:\<2\> |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-1396](https://issues.apache.org/jira/browse/HADOOP-1396) | FileNotFound exception on DFS block |  Blocker | . | Devaraj Das | dhruba borthakur |
| [HADOOP-1139](https://issues.apache.org/jira/browse/HADOOP-1139) | All block trasitions should be logged at log level INFO |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-1269](https://issues.apache.org/jira/browse/HADOOP-1269) | DFS Scalability: namenode throughput impacted becuase of global FSNamesystem lock |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-1472](https://issues.apache.org/jira/browse/HADOOP-1472) | Timed-out tasks are marked as 'KILLED' rather than as 'FAILED' which means the framework doesn't fail a TIP with 4 or more timed-out attempts |  Blocker | . | Arun C Murthy | Arun C Murthy |
| [HADOOP-1234](https://issues.apache.org/jira/browse/HADOOP-1234) | map tasks fail because they do not find application in file cache |  Major | . | Christian Kunz | Arun C Murthy |
| [HADOOP-1482](https://issues.apache.org/jira/browse/HADOOP-1482) | SecondaryNameNode does not roll ports |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-1300](https://issues.apache.org/jira/browse/HADOOP-1300) | deletion of excess replicas does not take into account 'rack-locality' |  Major | . | Koji Noguchi | Hairong Kuang |
| [HADOOP-1459](https://issues.apache.org/jira/browse/HADOOP-1459) | FileSystem.getFileCacheHints returns IP addresses rather than hostnames, which breaks 'data-locality' in map-reduce |  Blocker | . | Arun C Murthy | dhruba borthakur |
| [HADOOP-1493](https://issues.apache.org/jira/browse/HADOOP-1493) | possible double setting of java.library.path introduced by HADOOP-838 |  Major | . | Enis Soztutar | Enis Soztutar |
| [HADOOP-1372](https://issues.apache.org/jira/browse/HADOOP-1372) | DFS Clients should start using the org.apache.hadoop.fs.LocalDirAllocator |  Major | . | Devaraj Das | dhruba borthakur |
| [HADOOP-1193](https://issues.apache.org/jira/browse/HADOOP-1193) | Map/reduce job gets OutOfMemoryException when set map out to be compressed |  Blocker | . | Hairong Kuang | Arun C Murthy |
| [HADOOP-1492](https://issues.apache.org/jira/browse/HADOOP-1492) | DataNode version mismatch during handshake() causes NullPointerException. |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-1442](https://issues.apache.org/jira/browse/HADOOP-1442) | Zero-byte input files are not included in InputSplit |  Major | . | Milind Bhandarkar | Senthil Subramanian |
| [HADOOP-1444](https://issues.apache.org/jira/browse/HADOOP-1444) | Block allocation method does not check pendingCreates for duplicate block ids |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-1207](https://issues.apache.org/jira/browse/HADOOP-1207) | hdfs -rm should NOT fail if one of the files to remove is missing |  Minor | . | arkady borkovsky | Tsz Wo Nicholas Sze |
| [HADOOP-1503](https://issues.apache.org/jira/browse/HADOOP-1503) | Fix for broken build by HADOOP-1498 |  Major | . | stack | stack |
| [HADOOP-1475](https://issues.apache.org/jira/browse/HADOOP-1475) | local filecache disappears |  Blocker | . | Christian Kunz | Owen O'Malley |
| [HADOOP-1504](https://issues.apache.org/jira/browse/HADOOP-1504) | terminate-hadoop-cluster may be overzealous |  Blocker | fs/s3 | Doug Cutting | Tom White |
| [HADOOP-1453](https://issues.apache.org/jira/browse/HADOOP-1453) | exists() not necessary before DFS.open |  Major | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-1489](https://issues.apache.org/jira/browse/HADOOP-1489) | Input file get truncated for text files with \\r\\n |  Major | io | Bwolen Yang |  |
| [HADOOP-1501](https://issues.apache.org/jira/browse/HADOOP-1501) | Block reports from all datanodes arrive at the namenode within a small band of time |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-1517](https://issues.apache.org/jira/browse/HADOOP-1517) | Three methods in FSNamesystem should not be synchronized. |  Critical | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-1512](https://issues.apache.org/jira/browse/HADOOP-1512) | TestTextInputFormat fails on Windows |  Major | . | Konstantin Shvachko |  |
| [HADOOP-1514](https://issues.apache.org/jira/browse/HADOOP-1514) | Progress reporting not handled for the case where a reducer currently doesn't have anything to fetch |  Blocker | . | Devaraj Das | Vivek Ratan |
| [HADOOP-1367](https://issues.apache.org/jira/browse/HADOOP-1367) | Inconsistent synchronization of NetworkTopology.distFrom; locked 50% of time |  Major | io | Nigel Daley | Hairong Kuang |
| [HADOOP-1536](https://issues.apache.org/jira/browse/HADOOP-1536) | libhdfs tests failing |  Blocker | . | Nigel Daley | dhruba borthakur |
| [HADOOP-1520](https://issues.apache.org/jira/browse/HADOOP-1520) | IndexOutOfBoundsException in FSEditLog.processIOError |  Blocker | . | Nigel Daley | dhruba borthakur |
| [HADOOP-1542](https://issues.apache.org/jira/browse/HADOOP-1542) | Incorrect task/tip being scheduled (looks like speculative execution) |  Blocker | . | Nigel Daley | Owen O'Malley |
| [HADOOP-1513](https://issues.apache.org/jira/browse/HADOOP-1513) | A likely race condition between the creation of a directory and checking for its existence in the DiskChecker class |  Critical | fs | Devaraj Das | Devaraj Das |
| [HADOOP-1546](https://issues.apache.org/jira/browse/HADOOP-1546) | The DFS WebUI shows an incorrect column for file Creatin Time |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-690](https://issues.apache.org/jira/browse/HADOOP-690) | NPE in jobcontrol |  Major | . | Johan Oskarsson | Owen O'Malley |
| [HADOOP-1556](https://issues.apache.org/jira/browse/HADOOP-1556) | 9 unit test failures: file.out.index already exists |  Major | . | Nigel Daley | Devaraj Das |
| [HADOOP-1554](https://issues.apache.org/jira/browse/HADOOP-1554) | Fix the JobHistory to display things like the number of nodes the job ran on, the number of killed/failed tasks |  Major | . | Devaraj Das | Devaraj Das |
| [HADOOP-1448](https://issues.apache.org/jira/browse/HADOOP-1448) | Setting the replication factor of a file too high causes namenode cpu overload |  Major | . | dhruba borthakur | Hairong Kuang |
| [HADOOP-1578](https://issues.apache.org/jira/browse/HADOOP-1578) | Data-nodes should send storage ID to the name-node during registration |  Blocker | . | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-1584](https://issues.apache.org/jira/browse/HADOOP-1584) | Bug in readFields of GenericWritable |  Minor | io | Espen Amble Kolstad | Espen Amble Kolstad |
| [HADOOP-911](https://issues.apache.org/jira/browse/HADOOP-911) | Multithreading issue with libhdfs library |  Blocker | . | Christian Kunz | Christian Kunz |
| [HADOOP-1486](https://issues.apache.org/jira/browse/HADOOP-1486) | ReplicationMonitor thread goes away |  Blocker | . | Koji Noguchi | dhruba borthakur |
| [HADOOP-1590](https://issues.apache.org/jira/browse/HADOOP-1590) | Jobtracker web interface contains several absolute href links instead of relative ones |  Major | . | Thomas Friol |  |
| [HADOOP-1596](https://issues.apache.org/jira/browse/HADOOP-1596) | TestSymLink is failing |  Blocker | . | Doug Cutting | Owen O'Malley |
| [HADOOP-1535](https://issues.apache.org/jira/browse/HADOOP-1535) | Wrong comparator used to merge files in Reduce phase |  Major | . | Vivek Ratan | Vivek Ratan |
| [HADOOP-1428](https://issues.apache.org/jira/browse/HADOOP-1428) | ChecksumFileSystem : some operations implicitly not supported. |  Major | fs | Raghu Angadi |  |
| [HADOOP-1124](https://issues.apache.org/jira/browse/HADOOP-1124) | ChecksumFileSystem does not handle ChecksumError correctly |  Major | fs | Hairong Kuang | Hairong Kuang |
| [HADOOP-1576](https://issues.apache.org/jira/browse/HADOOP-1576) | web interface inconsistencies when using speculative execution |  Blocker | . | Christian Kunz | Arun C Murthy |
| [HADOOP-1524](https://issues.apache.org/jira/browse/HADOOP-1524) | Task Logs userlogs don't show up for a while |  Major | . | Michael Bieniosek | Michael Bieniosek |
| [HADOOP-1599](https://issues.apache.org/jira/browse/HADOOP-1599) | TestCopyFiles with IllegalArgumentException on Windows |  Blocker | fs | Nigel Daley | Senthil Subramanian |
| [HADOOP-1613](https://issues.apache.org/jira/browse/HADOOP-1613) | The dfs webui (dfshealth) shows "Last Contact" as a negative number |  Minor | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-1400](https://issues.apache.org/jira/browse/HADOOP-1400) | JobClient rpc times out getting job status |  Blocker | . | Nigel Daley | Owen O'Malley |
| [HADOOP-1564](https://issues.apache.org/jira/browse/HADOOP-1564) | Write unit tests to detect CRC corruption |  Major | . | dhruba borthakur | dhruba borthakur |
| [HADOOP-1285](https://issues.apache.org/jira/browse/HADOOP-1285) | ChecksumFileSystem : Can't read when io.file.buffer.size \< bytePerChecksum |  Major | fs | Raghu Angadi |  |
| [HADOOP-1625](https://issues.apache.org/jira/browse/HADOOP-1625) | "could not move files" exception in DataXceiver |  Blocker | . | Konstantin Shvachko | Raghu Angadi |
| [HADOOP-1624](https://issues.apache.org/jira/browse/HADOOP-1624) | Unknown op code exception in DataXceiver. |  Blocker | . | Konstantin Shvachko | Raghu Angadi |
| [HADOOP-1084](https://issues.apache.org/jira/browse/HADOOP-1084) | updating a hdfs file, doesn't cause the distributed file cache to update itself |  Blocker | . | Owen O'Malley | Arun C Murthy |
| [HADOOP-1639](https://issues.apache.org/jira/browse/HADOOP-1639) | TestSymLink is failing fairly often and is blocking the regression |  Major | . | Owen O'Malley | Mahadev konar |
| [HADOOP-1623](https://issues.apache.org/jira/browse/HADOOP-1623) | dfs -cp infinite loop creating sub-directories |  Blocker | . | Koji Noguchi | dhruba borthakur |
| [HADOOP-1603](https://issues.apache.org/jira/browse/HADOOP-1603) | Replication gets set to 1 sometimes when Namenode restarted. |  Blocker | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-1635](https://issues.apache.org/jira/browse/HADOOP-1635) | Keypair Name Hardcoded |  Minor | contrib/cloud | Stu Hood |  |
| [HADOOP-1634](https://issues.apache.org/jira/browse/HADOOP-1634) | EC2 launch-hadoop-cluster awk Problem |  Minor | contrib/cloud | Stu Hood |  |
| [HADOOP-1638](https://issues.apache.org/jira/browse/HADOOP-1638) | Master node unable to bind to DNS hostname |  Minor | contrib/cloud | Stu Hood |  |
| [HADOOP-1632](https://issues.apache.org/jira/browse/HADOOP-1632) | IllegalArgumentException in fsck |  Blocker | . | Konstantin Shvachko | Hairong Kuang |
| [HADOOP-1619](https://issues.apache.org/jira/browse/HADOOP-1619) | FSInputChecker attempts to seek past EOF |  Blocker | fs | Nigel Daley | Hairong Kuang |
| [HADOOP-1640](https://issues.apache.org/jira/browse/HADOOP-1640) | TestDecommission fails on Windows |  Blocker | . | Nigel Daley | dhruba borthakur |
| [HADOOP-1587](https://issues.apache.org/jira/browse/HADOOP-1587) | Tasks run by MiniMRCluster don't get sysprops from TestCases |  Blocker | test | Alejandro Abdelnur | Devaraj Das |
| [HADOOP-1551](https://issues.apache.org/jira/browse/HADOOP-1551) | libhdfs API is out of sync with Filesystem API |  Blocker | . | Christian Kunz | Sameer Paranjpye |
| [HADOOP-1647](https://issues.apache.org/jira/browse/HADOOP-1647) | DistributedFileSystem.getFileStatus() fails for path "/" |  Blocker | . | Enis Soztutar | dhruba borthakur |
| [HADOOP-1657](https://issues.apache.org/jira/browse/HADOOP-1657) | NNBench benchmark hangs with trunk |  Blocker | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-1666](https://issues.apache.org/jira/browse/HADOOP-1666) | The FsShell Object cannot be used for multiple fs commands. |  Minor | fs | dhruba borthakur | dhruba borthakur |
| [HADOOP-1553](https://issues.apache.org/jira/browse/HADOOP-1553) | Extensive logging of C++ application can slow down task by an order of magnitude |  Blocker | . | Christian Kunz | Owen O'Malley |
| [HADOOP-1659](https://issues.apache.org/jira/browse/HADOOP-1659) | job id / job name mix-up |  Blocker | . | Christian Kunz | Arun C Murthy |
| [HADOOP-1665](https://issues.apache.org/jira/browse/HADOOP-1665) | DFS Trash feature bugs |  Blocker | . | Nigel Daley | dhruba borthakur |
| [HADOOP-1649](https://issues.apache.org/jira/browse/HADOOP-1649) | Performance regression with Block CRCs |  Blocker | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-1680](https://issues.apache.org/jira/browse/HADOOP-1680) | Improvements to Block CRC upgrade messages |  Blocker | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-71](https://issues.apache.org/jira/browse/HADOOP-71) | The SequenceFileRecordReader uses the default FileSystem rather than the supplied one |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-1668](https://issues.apache.org/jira/browse/HADOOP-1668) | add INCOMPATIBLE CHANGES section to CHANGES.txt for Hadoop 0.14 |  Blocker | documentation | Nigel Daley | Nigel Daley |
| [HADOOP-1698](https://issues.apache.org/jira/browse/HADOOP-1698) | 7500+ reducers/partitions causes job to hang |  Blocker | . | Srikanth Kakani | Devaraj Das |
| [HADOOP-1716](https://issues.apache.org/jira/browse/HADOOP-1716) | TestPipes.testPipes fails |  Blocker | . | Nigel Daley | Owen O'Malley |
| [HADOOP-1714](https://issues.apache.org/jira/browse/HADOOP-1714) | TestDFSUpgradeFromImage fails on Windows |  Blocker | test | Nigel Daley | Raghu Angadi |
| [HADOOP-1663](https://issues.apache.org/jira/browse/HADOOP-1663) | streaming returning 0 when submitJob fails with Exception |  Major | . | Koji Noguchi | Koji Noguchi |
| [HADOOP-1712](https://issues.apache.org/jira/browse/HADOOP-1712) | Unhandled exception in Block CRC upgrade on datanode. |  Blocker | . | Raghu Angadi | Raghu Angadi |
| [HADOOP-1717](https://issues.apache.org/jira/browse/HADOOP-1717) | TestDFSUpgradeFromImage fails on Solaris |  Blocker | test | Nigel Daley | Raghu Angadi |
| [HADOOP-1681](https://issues.apache.org/jira/browse/HADOOP-1681) | Re organize StreamJob::submitAndMonitorJob() Exception handling |  Minor | . | Lohit Vijayarenu |  |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-1628](https://issues.apache.org/jira/browse/HADOOP-1628) | Block CRC Unit Tests: protocol tests |  Blocker | . | Nigel Daley | Raghu Angadi |
| [HADOOP-1629](https://issues.apache.org/jira/browse/HADOOP-1629) | Block CRC Unit Tests: upgrade test |  Blocker | . | Nigel Daley | Raghu Angadi |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-1336](https://issues.apache.org/jira/browse/HADOOP-1336) | turn on speculative execution by defaul |  Major | . | Owen O'Malley | Owen O'Malley |
| [HADOOP-1449](https://issues.apache.org/jira/browse/HADOOP-1449) | Example for contrib/data\_join |  Minor | . | Senthil Subramanian | Senthil Subramanian |


