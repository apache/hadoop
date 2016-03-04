
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

## Release 0.22.0 - 2011-12-10

### INCOMPATIBLE CHANGES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-7229](https://issues.apache.org/jira/browse/HADOOP-7229) | Absolute path to kinit in auto-renewal thread |  Major | security | Aaron T. Myers | Aaron T. Myers |
| [HADOOP-7137](https://issues.apache.org/jira/browse/HADOOP-7137) | Remove hod contrib |  Major | . | Nigel Daley | Nigel Daley |
| [HADOOP-7013](https://issues.apache.org/jira/browse/HADOOP-7013) | Add boolean field isCorrupt to BlockLocation |  Major | . | Patrick Kling | Patrick Kling |
| [HADOOP-6949](https://issues.apache.org/jira/browse/HADOOP-6949) | Reduces RPC packet size for primitive arrays, especially long[], which is used at block reporting |  Major | io | Navis | Matt Foley |
| [HADOOP-6905](https://issues.apache.org/jira/browse/HADOOP-6905) | Better logging messages when a delegation token is invalid |  Major | security | Kan Zhang | Kan Zhang |
| [HADOOP-6835](https://issues.apache.org/jira/browse/HADOOP-6835) | Support concatenated gzip files |  Major | io | Tom White | Greg Roelofs |
| [HADOOP-6787](https://issues.apache.org/jira/browse/HADOOP-6787) | Factor out glob pattern code from FileContext and Filesystem |  Major | fs | Luke Lu | Luke Lu |
| [HADOOP-6730](https://issues.apache.org/jira/browse/HADOOP-6730) | Bug in FileContext#copy and provide base class for FileContext tests |  Major | fs, test | Eli Collins | Ravi Phulari |
| [HDFS-1825](https://issues.apache.org/jira/browse/HDFS-1825) | Remove thriftfs contrib |  Major | . | Nigel Daley | Nigel Daley |
| [HDFS-1560](https://issues.apache.org/jira/browse/HDFS-1560) | dfs.data.dir permissions should default to 700 |  Minor | datanode | Todd Lipcon | Todd Lipcon |
| [HDFS-1435](https://issues.apache.org/jira/browse/HDFS-1435) | Provide an option to store fsimage compressed |  Major | namenode | Hairong Kuang | Hairong Kuang |
| [HDFS-1315](https://issues.apache.org/jira/browse/HDFS-1315) | Add fsck event to audit log and remove other audit log events corresponding to FSCK listStatus and open calls |  Major | namenode, tools | Suresh Srinivas | Suresh Srinivas |
| [HDFS-1109](https://issues.apache.org/jira/browse/HDFS-1109) | HFTP and URL Encoding |  Major | contrib/hdfsproxy, datanode | Dmytro Molkov | Dmytro Molkov |
| [HDFS-1080](https://issues.apache.org/jira/browse/HDFS-1080) | SecondaryNameNode image transfer should use the defined http address rather than local ip address |  Major | namenode | Jakob Homan | Jakob Homan |
| [HDFS-1061](https://issues.apache.org/jira/browse/HDFS-1061) | Memory footprint optimization for INodeFile object. |  Minor | namenode | Bharath Mundlapudi | Bharath Mundlapudi |
| [HDFS-903](https://issues.apache.org/jira/browse/HDFS-903) | NN should verify images and edit logs on startup |  Critical | namenode | Eli Collins | Hairong Kuang |
| [HDFS-330](https://issues.apache.org/jira/browse/HDFS-330) | Datanode Web UIs should provide robots.txt |  Trivial | datanode | Allen Wittenauer | Allen Wittenauer |
| [HDFS-202](https://issues.apache.org/jira/browse/HDFS-202) | Add a bulk FIleSystem.getFileBlockLocations |  Major | hdfs-client, namenode | Arun C Murthy | Hairong Kuang |
| [MAPREDUCE-1905](https://issues.apache.org/jira/browse/MAPREDUCE-1905) | Context.setStatus() and progress() api are ignored |  Blocker | task | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [MAPREDUCE-1866](https://issues.apache.org/jira/browse/MAPREDUCE-1866) | Remove deprecated class org.apache.hadoop.streaming.UTF8ByteArrayUtils |  Minor | contrib/streaming | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [MAPREDUCE-1733](https://issues.apache.org/jira/browse/MAPREDUCE-1733) | Authentication between pipes processes and java counterparts. |  Major | . | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [MAPREDUCE-1683](https://issues.apache.org/jira/browse/MAPREDUCE-1683) | Remove JNI calls from ClusterStatus cstr |  Major | jobtracker | Chris Douglas | Luke Lu |
| [MAPREDUCE-1664](https://issues.apache.org/jira/browse/MAPREDUCE-1664) | Job Acls affect Queue Acls |  Major | security | Ravi Gummadi | Ravi Gummadi |


### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |


### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-6996](https://issues.apache.org/jira/browse/HADOOP-6996) | Allow CodecFactory to return a codec object given a codec' class name |  Major | io | Hairong Kuang | Hairong Kuang |
| [HADOOP-6978](https://issues.apache.org/jira/browse/HADOOP-6978) | Add JNI support for secure IO operations |  Blocker | io, native, security | Todd Lipcon | Todd Lipcon |
| [HADOOP-6892](https://issues.apache.org/jira/browse/HADOOP-6892) | Common component of HDFS-1150 (Verify datanodes' identities to clients in secure clusters) |  Major | security | Jakob Homan | Jakob Homan |
| [HADOOP-6889](https://issues.apache.org/jira/browse/HADOOP-6889) | Make RPC to have an option to timeout |  Major | ipc | Hairong Kuang | John George |
| [HADOOP-6870](https://issues.apache.org/jira/browse/HADOOP-6870) | Add FileSystem#listLocatedStatus to list a directory's content together with each file's block locations |  Major | fs | Hairong Kuang | Hairong Kuang |
| [HADOOP-6832](https://issues.apache.org/jira/browse/HADOOP-6832) | Provide a web server plugin that uses a static user for the web UI |  Major | security | Owen O'Malley | Owen O'Malley |
| [HADOOP-6600](https://issues.apache.org/jira/browse/HADOOP-6600) | mechanism for authorization check for inter-server protocols |  Major | . | Boris Shkolnik | Boris Shkolnik |
| [HADOOP-6586](https://issues.apache.org/jira/browse/HADOOP-6586) | Log authentication and authorization failures and successes |  Major | security | Boris Shkolnik | Boris Shkolnik |
| [HADOOP-6472](https://issues.apache.org/jira/browse/HADOOP-6472) | add tokenCache option to GenericOptionsParser for passing file with secret keys to a map reduce job |  Major | . | Boris Shkolnik | Boris Shkolnik |
| [HADOOP-4487](https://issues.apache.org/jira/browse/HADOOP-4487) | Security features for Hadoop |  Major | security | Kan Zhang | Kan Zhang |
| [HDFS-1330](https://issues.apache.org/jira/browse/HDFS-1330) | Make RPCs to DataNodes timeout |  Major | datanode | Hairong Kuang | John George |
| [HDFS-1318](https://issues.apache.org/jira/browse/HDFS-1318) | HDFS Namenode and Datanode WebUI information needs to be accessible programmatically for scripts |  Major | . | Suresh Srinivas | Tanping Wang |
| [HDFS-1150](https://issues.apache.org/jira/browse/HDFS-1150) | Verify datanodes' identities to clients in secure clusters |  Major | datanode | Jakob Homan | Jakob Homan |
| [HDFS-1111](https://issues.apache.org/jira/browse/HDFS-1111) | getCorruptFiles() should give some hint that the list is not complete |  Major | . | Rodrigo Schmidt | Sriram Rao |
| [HDFS-1096](https://issues.apache.org/jira/browse/HDFS-1096) | allow dfsadmin/mradmin refresh of superuser proxy group mappings |  Major | security | Boris Shkolnik | Boris Shkolnik |
| [HDFS-1079](https://issues.apache.org/jira/browse/HDFS-1079) | HDFS implementation should throw exceptions defined in AbstractFileSystem |  Major | namenode | Suresh Srinivas | Suresh Srinivas |
| [HDFS-1005](https://issues.apache.org/jira/browse/HDFS-1005) | Fsck security |  Major | . | Jitendra Nath Pandey | Boris Shkolnik |
| [HDFS-1004](https://issues.apache.org/jira/browse/HDFS-1004) | Update NN to support Kerberized SSL from HADOOP-6584 |  Major | namenode | Jakob Homan | Jakob Homan |
| [HDFS-1003](https://issues.apache.org/jira/browse/HDFS-1003) | authorization checks for inter-server protocol (based on HADOOP-6600) |  Major | . | Boris Shkolnik | Boris Shkolnik |
| [HDFS-992](https://issues.apache.org/jira/browse/HDFS-992) | Re-factor block access token implementation to conform to the generic Token interface in Common |  Major | security | Kan Zhang | Kan Zhang |
| [HDFS-811](https://issues.apache.org/jira/browse/HDFS-811) | Add metrics, failure reporting and additional tests for HDFS-457 |  Minor | test | Ravi Phulari | Eli Collins |
| [HDFS-752](https://issues.apache.org/jira/browse/HDFS-752) | Add interface classification stable & scope to HDFS |  Major | . | Suresh Srinivas | Suresh Srinivas |
| [HDFS-708](https://issues.apache.org/jira/browse/HDFS-708) | A stress-test tool for HDFS. |  Major | test, tools | Konstantin Shvachko | Joshua Harlow |
| [HDFS-528](https://issues.apache.org/jira/browse/HDFS-528) | Add ability for safemode to wait for a minimum number of live datanodes |  Major | scripts | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-1804](https://issues.apache.org/jira/browse/MAPREDUCE-1804) | Stress-test tool for HDFS introduced in HDFS-708 |  Major | benchmarks, test | Konstantin Shvachko | Konstantin Shvachko |
| [MAPREDUCE-1680](https://issues.apache.org/jira/browse/MAPREDUCE-1680) | Add a metrics to track the number of heartbeats processed |  Major | jobtracker | Hong Tang | Dick King |
| [MAPREDUCE-1594](https://issues.apache.org/jira/browse/MAPREDUCE-1594) | Support for Sleep Jobs in gridmix |  Major | contrib/gridmix | rahul k singh | rahul k singh |
| [MAPREDUCE-1517](https://issues.apache.org/jira/browse/MAPREDUCE-1517) | streaming should support running on background |  Major | contrib/streaming | Bochun Bai | Bochun Bai |
| [MAPREDUCE-1516](https://issues.apache.org/jira/browse/MAPREDUCE-1516) | JobTracker should issue a delegation token only for kerberos authenticated client |  Major | . | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [MAPREDUCE-220](https://issues.apache.org/jira/browse/MAPREDUCE-220) | Collecting cpu and memory usage for MapReduce tasks |  Major | task, tasktracker | Hong Tang | Scott Chen |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-7861](https://issues.apache.org/jira/browse/HADOOP-7861) | changes2html.pl should generate links to HADOOP, HDFS, and MAPREDUCE jiras |  Major | documentation | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-7786](https://issues.apache.org/jira/browse/HADOOP-7786) | Remove HDFS-specific configuration keys defined in FsConfig |  Major | . | Eli Collins | Eli Collins |
| [HADOOP-7457](https://issues.apache.org/jira/browse/HADOOP-7457) | Remove out-of-date Chinese language documentation |  Blocker | documentation | Jakob Homan | Jakob Homan |
| [HADOOP-7358](https://issues.apache.org/jira/browse/HADOOP-7358) | Improve log levels when exceptions caught in RPC handler |  Minor | ipc | Todd Lipcon | Todd Lipcon |
| [HADOOP-7355](https://issues.apache.org/jira/browse/HADOOP-7355) | Add audience and stability annotations to HttpServer class |  Major | . | stack | stack |
| [HADOOP-7346](https://issues.apache.org/jira/browse/HADOOP-7346) | Send back nicer error to clients using outdated IPC version |  Major | ipc | Todd Lipcon | Todd Lipcon |
| [HADOOP-7335](https://issues.apache.org/jira/browse/HADOOP-7335) | Force entropy to come from non-true random for tests |  Minor | build, test | Todd Lipcon | Todd Lipcon |
| [HADOOP-7325](https://issues.apache.org/jira/browse/HADOOP-7325) | hadoop command - do not accept class names starting with a hyphen |  Minor | scripts | Brock Noland | Brock Noland |
| [HADOOP-7244](https://issues.apache.org/jira/browse/HADOOP-7244) | Documentation change for updated configuration keys |  Blocker | documentation | Tom White | Tom White |
| [HADOOP-7241](https://issues.apache.org/jira/browse/HADOOP-7241) | fix typo of command 'hadoop fs -help tail' |  Minor | fs, test | Wei Yongjun | Wei Yongjun |
| [HADOOP-7193](https://issues.apache.org/jira/browse/HADOOP-7193) | Help message is wrong for touchz command. |  Minor | fs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-7192](https://issues.apache.org/jira/browse/HADOOP-7192) | fs -stat docs aren't updated to reflect the format features |  Trivial | documentation | Harsh J | Harsh J |
| [HADOOP-7189](https://issues.apache.org/jira/browse/HADOOP-7189) | Add ability to enable 'debug' property in JAAS configuration |  Minor | security | Todd Lipcon | Ted Yu |
| [HADOOP-7177](https://issues.apache.org/jira/browse/HADOOP-7177) | CodecPool should report which compressor it is using |  Trivial | native | Allen Wittenauer | Allen Wittenauer |
| [HADOOP-7154](https://issues.apache.org/jira/browse/HADOOP-7154) | Should set MALLOC\_ARENA\_MAX in hadoop-config.sh |  Minor | scripts | Todd Lipcon | Todd Lipcon |
| [HADOOP-7134](https://issues.apache.org/jira/browse/HADOOP-7134) | configure files that are generated as part of the released tarball need to have executable bit set |  Major | build | Roman Shaposhnik | Roman Shaposhnik |
| [HADOOP-7117](https://issues.apache.org/jira/browse/HADOOP-7117) | Move secondary namenode checkpoint configs from core-default.xml to hdfs-default.xml |  Major | conf | Patrick Angeles | Harsh J |
| [HADOOP-7110](https://issues.apache.org/jira/browse/HADOOP-7110) | Implement chmod with JNI |  Major | io, native | Todd Lipcon | Todd Lipcon |
| [HADOOP-7106](https://issues.apache.org/jira/browse/HADOOP-7106) | Re-organize hadoop subversion layout |  Blocker | build | Nigel Daley | Todd Lipcon |
| [HADOOP-7054](https://issues.apache.org/jira/browse/HADOOP-7054) | Change NN LoadGenerator to use the new FileContext api |  Major | . | Sanjay Radia | Sanjay Radia |
| [HADOOP-7032](https://issues.apache.org/jira/browse/HADOOP-7032) | Assert type constraints in the FileStatus constructor |  Major | fs | Eli Collins | Eli Collins |
| [HADOOP-7010](https://issues.apache.org/jira/browse/HADOOP-7010) | Typo in FileSystem.java |  Minor | fs | Jingguo Yao | Jingguo Yao |
| [HADOOP-7009](https://issues.apache.org/jira/browse/HADOOP-7009) | MD5Hash provides a public factory method that creates an instance of MessageDigest |  Major | io | Hairong Kuang | Hairong Kuang |
| [HADOOP-7008](https://issues.apache.org/jira/browse/HADOOP-7008) | Enable test-patch.sh to have a configured number of acceptable findbugs and javadoc warnings |  Major | test | Nigel Daley | Giridharan Kesavan |
| [HADOOP-7007](https://issues.apache.org/jira/browse/HADOOP-7007) | update the hudson-test-patch target to work with the latest test-patch script. |  Major | build | Giridharan Kesavan | Giridharan Kesavan |
| [HADOOP-7005](https://issues.apache.org/jira/browse/HADOOP-7005) | Update test-patch.sh to remove callback to Hudson master |  Major | test | Nigel Daley | Nigel Daley |
| [HADOOP-6987](https://issues.apache.org/jira/browse/HADOOP-6987) | Use JUnit Rule to optionally fail test cases that run more than 10 seconds |  Major | test | Jakob Homan | Jakob Homan |
| [HADOOP-6985](https://issues.apache.org/jira/browse/HADOOP-6985) | Suggest that HADOOP\_OPTS be preserved in hadoop-env.sh.template |  Minor | . | Ramkumar Vadali | Ramkumar Vadali |
| [HADOOP-6977](https://issues.apache.org/jira/browse/HADOOP-6977) | Herriot daemon clients should vend statistics |  Major | test | Konstantin Boudnik | Konstantin Boudnik |
| [HADOOP-6950](https://issues.apache.org/jira/browse/HADOOP-6950) | Suggest that HADOOP\_CLASSPATH should be preserved in hadoop-env.sh.template |  Trivial | scripts | Philip Zeyliger | Philip Zeyliger |
| [HADOOP-6943](https://issues.apache.org/jira/browse/HADOOP-6943) | The GroupMappingServiceProvider interface should be public |  Major | security | Aaron T. Myers | Aaron T. Myers |
| [HADOOP-6911](https://issues.apache.org/jira/browse/HADOOP-6911) | doc update for DelegationTokenFetcher (part of HDFS-1036) |  Minor | . | Boris Shkolnik |  |
| [HADOOP-6903](https://issues.apache.org/jira/browse/HADOOP-6903) | Make AbstractFileSystem's methods public to allow filter-Fs like implementions in a differnt package than fs |  Major | . | Sanjay Radia | Sanjay Radia |
| [HADOOP-6890](https://issues.apache.org/jira/browse/HADOOP-6890) | Improve listFiles API introduced by HADOOP-6870 |  Major | fs | Hairong Kuang | Hairong Kuang |
| [HADOOP-6884](https://issues.apache.org/jira/browse/HADOOP-6884) | Add LOG.isDebugEnabled() guard for each LOG.debug("...") |  Major | . | Erik Steffl | Erik Steffl |
| [HADOOP-6879](https://issues.apache.org/jira/browse/HADOOP-6879) | Provide SSH based (Jsch) remote execution API for system tests |  Major | build, test | Iyappan Srinivasan | Konstantin Boudnik |
| [HADOOP-6877](https://issues.apache.org/jira/browse/HADOOP-6877) | Common part of HDFS-1178 |  Major | ipc | Kan Zhang | Kan Zhang |
| [HADOOP-6862](https://issues.apache.org/jira/browse/HADOOP-6862) | Add api to add user/group to AccessControlList |  Major | security | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [HADOOP-6861](https://issues.apache.org/jira/browse/HADOOP-6861) | Method in Credentials to read and write a token storage file. |  Major | . | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [HADOOP-6859](https://issues.apache.org/jira/browse/HADOOP-6859) | Introduce additional statistics to FileSystem |  Major | . | Suresh Srinivas | Suresh Srinivas |
| [HADOOP-6856](https://issues.apache.org/jira/browse/HADOOP-6856) | SequenceFile and MapFile need cleanup to remove redundant constructors |  Major | io | Owen O'Malley | Owen O'Malley |
| [HADOOP-6845](https://issues.apache.org/jira/browse/HADOOP-6845) | TokenStorage renamed to Credentials. |  Major | . | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [HADOOP-6825](https://issues.apache.org/jira/browse/HADOOP-6825) | FileStatus needs unit tests |  Major | . | Rodrigo Schmidt | Rodrigo Schmidt |
| [HADOOP-6818](https://issues.apache.org/jira/browse/HADOOP-6818) | Provide a JNI-based implementation of GroupMappingServiceProvider |  Major | security | Devaraj Das | Devaraj Das |
| [HADOOP-6814](https://issues.apache.org/jira/browse/HADOOP-6814) | Method in UGI to get the authentication method of the real user. |  Major | . | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [HADOOP-6811](https://issues.apache.org/jira/browse/HADOOP-6811) | Remove EC2 bash scripts |  Blocker | . | Tom White | Tom White |
| [HADOOP-6805](https://issues.apache.org/jira/browse/HADOOP-6805) | add buildDTServiceName method to SecurityUtil (as part of MAPREDUCE-1718) |  Major | . | Boris Shkolnik | Boris Shkolnik |
| [HADOOP-6791](https://issues.apache.org/jira/browse/HADOOP-6791) | Refresh for proxy superuser config  (common part for HDFS-1096) |  Major | . | Boris Shkolnik | Boris Shkolnik |
| [HADOOP-6761](https://issues.apache.org/jira/browse/HADOOP-6761) | Improve Trash Emptier |  Major | . | Dmytro Molkov | Dmytro Molkov |
| [HADOOP-6745](https://issues.apache.org/jira/browse/HADOOP-6745) | adding some java doc to Server.RpcMetrics, UGI |  Minor | ipc | Boris Shkolnik | Boris Shkolnik |
| [HADOOP-6714](https://issues.apache.org/jira/browse/HADOOP-6714) | FsShell 'hadoop fs -text' does not support compression codecs |  Major | . | Patrick Angeles | Patrick Angeles |
| [HADOOP-6693](https://issues.apache.org/jira/browse/HADOOP-6693) | Add metrics to track kerberos login activity |  Major | . | Suresh Srinivas | Suresh Srinivas |
| [HADOOP-6674](https://issues.apache.org/jira/browse/HADOOP-6674) | Performance Improvement in Secure RPC |  Major | . | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [HADOOP-6632](https://issues.apache.org/jira/browse/HADOOP-6632) | Support for using different Kerberos keys for different instances of Hadoop services |  Major | . | Kan Zhang | Kan Zhang |
| [HADOOP-6623](https://issues.apache.org/jira/browse/HADOOP-6623) | Add StringUtils.split for non-escaped single-character separator |  Minor | util | Todd Lipcon | Todd Lipcon |
| [HADOOP-6605](https://issues.apache.org/jira/browse/HADOOP-6605) | Add JAVA\_HOME detection to hadoop-config |  Minor | . | Chad Metcalf | Eli Collins |
| [HADOOP-6599](https://issues.apache.org/jira/browse/HADOOP-6599) | Split RPC metrics into summary and detailed metrics |  Major | . | Suresh Srinivas | Suresh Srinivas |
| [HADOOP-6584](https://issues.apache.org/jira/browse/HADOOP-6584) | Provide Kerberized SSL encryption for webservices |  Major | . | Jakob Homan | Jakob Homan |
| [HADOOP-6578](https://issues.apache.org/jira/browse/HADOOP-6578) | Configuration should trim whitespace around a lot of value types |  Minor | conf | Todd Lipcon | Michele Catasta |
| [HADOOP-6562](https://issues.apache.org/jira/browse/HADOOP-6562) | FileContextSymlinkBaseTest should use FileContextTestHelper |  Minor | test | Eli Collins | Eli Collins |
| [HADOOP-6436](https://issues.apache.org/jira/browse/HADOOP-6436) | Remove auto-generated native build files |  Major | . | Eli Collins | Roman Shaposhnik |
| [HADOOP-6298](https://issues.apache.org/jira/browse/HADOOP-6298) | BytesWritable#getBytes is a bad name that leads to programming mistakes |  Major | . | Nathan Marz | Owen O'Malley |
| [HADOOP-6056](https://issues.apache.org/jira/browse/HADOOP-6056) | Use java.net.preferIPv4Stack to force IPv4 |  Major | scripts | Steve Loughran | Michele Catasta |
| [HADOOP-4675](https://issues.apache.org/jira/browse/HADOOP-4675) | Current Ganglia metrics implementation is incompatible with Ganglia 3.1 |  Major | metrics | Brian Bockelman | Brian Bockelman |
| [HDFS-2286](https://issues.apache.org/jira/browse/HDFS-2286) | DataXceiverServer logs AsynchronousCloseException at shutdown |  Trivial | datanode | Todd Lipcon | Todd Lipcon |
| [HDFS-2054](https://issues.apache.org/jira/browse/HDFS-2054) | BlockSender.sendChunk() prints ERROR for connection closures encountered  during transferToFully() |  Minor | datanode | Kihwal Lee | Kihwal Lee |
| [HDFS-2039](https://issues.apache.org/jira/browse/HDFS-2039) | TestNameNodeMetrics uses a bad test root path |  Major | . | Todd Lipcon | Todd Lipcon |
| [HDFS-1980](https://issues.apache.org/jira/browse/HDFS-1980) | Move build/webapps deeper in directory heirarchy to aid eclipse users |  Major | build | Todd Lipcon | Todd Lipcon |
| [HDFS-1957](https://issues.apache.org/jira/browse/HDFS-1957) | Documentation for HFTP |  Minor | documentation | Ari Rabkin | Ari Rabkin |
| [HDFS-1954](https://issues.apache.org/jira/browse/HDFS-1954) | Improve corrupt files warning message on NameNode web UI |  Major | namenode | philo vivero | Patrick Hunt |
| [HDFS-1947](https://issues.apache.org/jira/browse/HDFS-1947) | DFSClient should use mapreduce.task.attempt.id |  Trivial | hdfs-client | Eli Collins | Eli Collins |
| [HDFS-1935](https://issues.apache.org/jira/browse/HDFS-1935) | Build should not redownload ivy on every invocation |  Minor | build | Todd Lipcon | Joep Rottinghuis |
| [HDFS-1866](https://issues.apache.org/jira/browse/HDFS-1866) | Document dfs.datanode.max.transfer.threads in hdfs-default.xml |  Minor | datanode, documentation | Eli Collins | Harsh J |
| [HDFS-1861](https://issues.apache.org/jira/browse/HDFS-1861) | Rename dfs.datanode.max.xcievers and bump its default value |  Major | datanode | Eli Collins | Eli Collins |
| [HDFS-1736](https://issues.apache.org/jira/browse/HDFS-1736) | Break dependency between DatanodeJspHelper and FsShell |  Minor | datanode | Daryn Sharp | Daryn Sharp |
| [HDFS-1619](https://issues.apache.org/jira/browse/HDFS-1619) | Remove AC\_TYPE\* from the libhdfs |  Major | libhdfs | Roman Shaposhnik | Roman Shaposhnik |
| [HDFS-1618](https://issues.apache.org/jira/browse/HDFS-1618) | configure files that are generated as part of the released tarball need to have executable bit set |  Major | build | Roman Shaposhnik | Roman Shaposhnik |
| [HDFS-1596](https://issues.apache.org/jira/browse/HDFS-1596) | Move secondary namenode checkpoint configs from core-default.xml to hdfs-default.xml |  Major | documentation, namenode | Patrick Angeles | Harsh J |
| [HDFS-1582](https://issues.apache.org/jira/browse/HDFS-1582) | Remove auto-generated native build files |  Major | libhdfs | Roman Shaposhnik | Roman Shaposhnik |
| [HDFS-1513](https://issues.apache.org/jira/browse/HDFS-1513) | Fix a number of warnings |  Minor | . | Eli Collins | Eli Collins |
| [HDFS-1491](https://issues.apache.org/jira/browse/HDFS-1491) | Update  Hdfs to match the change of methods from protected to public  in AbstractFileSystem (Hadoop-6903) |  Major | . | Sanjay Radia | Sanjay Radia |
| [HDFS-1485](https://issues.apache.org/jira/browse/HDFS-1485) | Typo in BlockPlacementPolicy.java |  Minor | . | Jingguo Yao | Jingguo Yao |
| [HDFS-1472](https://issues.apache.org/jira/browse/HDFS-1472) | Refactor DFSck to allow programmatic access to output |  Major | tools | Ramkumar Vadali | Ramkumar Vadali |
| [HDFS-1457](https://issues.apache.org/jira/browse/HDFS-1457) | Limit transmission rate when transfering image between primary and secondary NNs |  Major | namenode | Hairong Kuang | Hairong Kuang |
| [HDFS-1456](https://issues.apache.org/jira/browse/HDFS-1456) | Provide builder for constructing instances of MiniDFSCluster |  Major | test | Jakob Homan | Jakob Homan |
| [HDFS-1454](https://issues.apache.org/jira/browse/HDFS-1454) | Update the documentation to reflect true client caching strategy |  Major | documentation, hdfs-client | Jeff Hammerbacher | Harsh J |
| [HDFS-1434](https://issues.apache.org/jira/browse/HDFS-1434) | Refactor Datanode#startDataNode method |  Major | datanode | Suresh Srinivas | Suresh Srinivas |
| [HDFS-1426](https://issues.apache.org/jira/browse/HDFS-1426) | Remove unused method BlockInfo#listCount |  Major | namenode | Hairong Kuang | Hairong Kuang |
| [HDFS-1417](https://issues.apache.org/jira/browse/HDFS-1417) | Add @Override annotation to SimulatedFSDataset methods that implement FSDatasetInterface |  Major | . | Suresh Srinivas | Suresh Srinivas |
| [HDFS-1408](https://issues.apache.org/jira/browse/HDFS-1408) | Herriot NN and DN clients should vend statistics |  Major | test | Al Thompson | Konstantin Boudnik |
| [HDFS-1407](https://issues.apache.org/jira/browse/HDFS-1407) | Use Block in DataTransferProtocol |  Major | . | Suresh Srinivas | Suresh Srinivas |
| [HDFS-1395](https://issues.apache.org/jira/browse/HDFS-1395) | Add @Override annotation to FSDataset methods that implement FSDatasetInterface |  Major | datanode | Suresh Srinivas | Suresh Srinivas |
| [HDFS-1387](https://issues.apache.org/jira/browse/HDFS-1387) | Update HDFS permissions guide for security |  Major | documentation, security | Todd Lipcon | Todd Lipcon |
| [HDFS-1383](https://issues.apache.org/jira/browse/HDFS-1383) | Better error messages on hftp |  Major | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-1369](https://issues.apache.org/jira/browse/HDFS-1369) | Invalid javadoc reference in FSDatasetMBean.java |  Trivial | datanode | Eli Collins | Eli Collins |
| [HDFS-1368](https://issues.apache.org/jira/browse/HDFS-1368) | Add a block counter to DatanodeDescriptor |  Major | namenode | Hairong Kuang | Hairong Kuang |
| [HDFS-1356](https://issues.apache.org/jira/browse/HDFS-1356) | Provide information as to whether or not security is enabled on web interface for NameNode (part of HADOOP-6822) |  Major | . | Boris Shkolnik | Boris Shkolnik |
| [HDFS-1353](https://issues.apache.org/jira/browse/HDFS-1353) | Remove most of getBlockLocation optimization |  Major | namenode | Jakob Homan | Jakob Homan |
| [HDFS-1320](https://issues.apache.org/jira/browse/HDFS-1320) | Add LOG.isDebugEnabled() guard for each LOG.debug("...") |  Major | . | Erik Steffl | Erik Steffl |
| [HDFS-1307](https://issues.apache.org/jira/browse/HDFS-1307) | Add start time, end time and total time taken for FSCK to FSCK report |  Major | namenode, tools | Suresh Srinivas | Suresh Srinivas |
| [HDFS-1304](https://issues.apache.org/jira/browse/HDFS-1304) | There is no unit test for HftpFileSystem.open(..) |  Major | test | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-1302](https://issues.apache.org/jira/browse/HDFS-1302) | Use writeTokenStorageToStream method in Credentials to store credentials to a file. |  Major | . | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [HDFS-1298](https://issues.apache.org/jira/browse/HDFS-1298) | Add support in HDFS to update statistics that tracks number of file system operations in FileSystem |  Major | . | Suresh Srinivas | Suresh Srinivas |
| [HDFS-1297](https://issues.apache.org/jira/browse/HDFS-1297) | Fix some comments |  Trivial | documentation | Jeff Ames | Jeff Ames |
| [HDFS-1272](https://issues.apache.org/jira/browse/HDFS-1272) | HDFS changes corresponding to rename of TokenStorage to Credentials |  Major | . | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [HDFS-1205](https://issues.apache.org/jira/browse/HDFS-1205) | FSDatasetAsyncDiskService should name its threads |  Major | datanode | Todd Lipcon | Todd Lipcon |
| [HDFS-1203](https://issues.apache.org/jira/browse/HDFS-1203) | DataNode should sleep before reentering service loop after an exception |  Major | datanode | Todd Lipcon | Todd Lipcon |
| [HDFS-1201](https://issues.apache.org/jira/browse/HDFS-1201) |  Support for using different Kerberos keys for Namenode and datanode. |  Major | . | Jitendra Nath Pandey | Kan Zhang |
| [HDFS-1190](https://issues.apache.org/jira/browse/HDFS-1190) | Remove unused getNamenode() method from DataNode. |  Minor | datanode | Jeff Ames | Jeff Ames |
| [HDFS-1187](https://issues.apache.org/jira/browse/HDFS-1187) | Modify fetchdt to allow renewing and canceling token |  Major | security | Owen O'Malley | Owen O'Malley |
| [HDFS-1185](https://issues.apache.org/jira/browse/HDFS-1185) | Remove duplicate now() functions in DataNode, FSNamesystem |  Minor | datanode, namenode | Jeff Ames | Jeff Ames |
| [HDFS-1184](https://issues.apache.org/jira/browse/HDFS-1184) | Replace tabs in code with spaces |  Minor | . | Jeff Ames | Jeff Ames |
| [HDFS-1183](https://issues.apache.org/jira/browse/HDFS-1183) | Remove some duplicate code in NamenodeJspHelper.java |  Minor | namenode | Jeff Ames | Jeff Ames |
| [HDFS-1178](https://issues.apache.org/jira/browse/HDFS-1178) | The NameNode servlets should not use RPC to connect to the NameNode |  Major | namenode | Owen O'Malley | Owen O'Malley |
| [HDFS-1160](https://issues.apache.org/jira/browse/HDFS-1160) | Improve some FSDataset warnings and comments |  Major | datanode | Eli Collins | Eli Collins |
| [HDFS-1140](https://issues.apache.org/jira/browse/HDFS-1140) | Speedup INode.getPathComponents |  Minor | namenode | Dmytro Molkov | Dmytro Molkov |
| [HDFS-1114](https://issues.apache.org/jira/browse/HDFS-1114) | Reducing NameNode memory usage by an alternate hash table |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-1110](https://issues.apache.org/jira/browse/HDFS-1110) | Namenode heap optimization - reuse objects for commonly used file names |  Major | . | Suresh Srinivas | Suresh Srinivas |
| [HDFS-1093](https://issues.apache.org/jira/browse/HDFS-1093) | Improve namenode scalability by splitting the FSNamesystem synchronized section in a read/write lock |  Major | namenode | dhruba borthakur | dhruba borthakur |
| [HDFS-1081](https://issues.apache.org/jira/browse/HDFS-1081) | Performance regression in DistributedFileSystem::getFileBlockLocations in secure systems |  Major | security | Jakob Homan | Jakob Homan |
| [HDFS-1055](https://issues.apache.org/jira/browse/HDFS-1055) | Improve thread naming for DataXceivers |  Major | datanode | Todd Lipcon | Todd Lipcon |
| [HDFS-1035](https://issues.apache.org/jira/browse/HDFS-1035) | Generate Eclipse's .classpath file from Ivy config |  Major | build | Tom White | Nigel Daley |
| [HDFS-1033](https://issues.apache.org/jira/browse/HDFS-1033) | In secure clusters, NN and SNN should verify that the remote principal during image and edits transfer |  Major | namenode, security | Jakob Homan | Jakob Homan |
| [HDFS-1028](https://issues.apache.org/jira/browse/HDFS-1028) | INode.getPathNames could split more efficiently |  Minor | namenode | Todd Lipcon | Dmytro Molkov |
| [HDFS-1023](https://issues.apache.org/jira/browse/HDFS-1023) | Allow http server to start as regular principal if https principal not defined. |  Major | namenode | Jakob Homan | Jakob Homan |
| [HDFS-947](https://issues.apache.org/jira/browse/HDFS-947) | The namenode should redirect a hftp request to read a file to the datanode that has the maximum number of local replicas |  Major | . | dhruba borthakur | Dmytro Molkov |
| [HDFS-941](https://issues.apache.org/jira/browse/HDFS-941) | Datanode xceiver protocol should allow reuse of a connection |  Major | datanode, hdfs-client, performance | Todd Lipcon | bc Wong |
| [HDFS-895](https://issues.apache.org/jira/browse/HDFS-895) | Allow hflush/sync to occur in parallel with new writes to the file |  Major | hdfs-client | dhruba borthakur | Todd Lipcon |
| [HDFS-884](https://issues.apache.org/jira/browse/HDFS-884) | DataNode makeInstance should report the directory list when failing to start up |  Minor | datanode | Steve Loughran | Steve Loughran |
| [HDFS-882](https://issues.apache.org/jira/browse/HDFS-882) | Datanode could log what hostname and ports its listening on on startup |  Minor | datanode | Steve Loughran | Steve Loughran |
| [HDFS-881](https://issues.apache.org/jira/browse/HDFS-881) | Refactor DataNode Packet header into DataTransferProtocol |  Major | . | Todd Lipcon | Todd Lipcon |
| [HDFS-853](https://issues.apache.org/jira/browse/HDFS-853) | The HDFS webUI should show a metric that summarizes whether the cluster is balanced regarding disk space usage |  Major | namenode | dhruba borthakur | Dmytro Molkov |
| [HDFS-718](https://issues.apache.org/jira/browse/HDFS-718) | configuration parameter to prevent accidental formatting of HDFS filesystem |  Minor | namenode | Andrew Ryan | Andrew Ryan |
| [HDFS-599](https://issues.apache.org/jira/browse/HDFS-599) | Improve Namenode robustness by prioritizing datanode heartbeats over client requests |  Major | namenode | dhruba borthakur | Dmytro Molkov |
| [HDFS-556](https://issues.apache.org/jira/browse/HDFS-556) | Provide info on failed volumes in the web ui |  Major | namenode | Jakob Homan | Eli Collins |
| [HDFS-455](https://issues.apache.org/jira/browse/HDFS-455) | Make NN and DN handle in a intuitive way comma-separated configuration strings |  Minor | datanode, namenode | Michele Catasta | Michele Catasta |
| [MAPREDUCE-2505](https://issues.apache.org/jira/browse/MAPREDUCE-2505) | Explain how to use ACLs in the fair scheduler |  Minor | contrib/fair-share, documentation | Matei Zaharia | Matei Zaharia |
| [MAPREDUCE-2502](https://issues.apache.org/jira/browse/MAPREDUCE-2502) | JobSubmitter should use mapreduce.job.maps |  Trivial | job submission | Eli Collins | Eli Collins |
| [MAPREDUCE-2410](https://issues.apache.org/jira/browse/MAPREDUCE-2410) | document multiple keys per reducer oddity in hadoop streaming FAQ |  Minor | contrib/streaming, documentation | Dieter Plaetinck | Harsh J |
| [MAPREDUCE-2372](https://issues.apache.org/jira/browse/MAPREDUCE-2372) | TaskLogAppender mechanism shouldn't be set in log4j.properties |  Major | task | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2337](https://issues.apache.org/jira/browse/MAPREDUCE-2337) | Remove dependence of public MapReduce API on classes in server package |  Major | . | Tom White | Tom White |
| [MAPREDUCE-2314](https://issues.apache.org/jira/browse/MAPREDUCE-2314) | configure files that are generated as part of the released tarball need to have executable bit set |  Major | build | Roman Shaposhnik | Roman Shaposhnik |
| [MAPREDUCE-2260](https://issues.apache.org/jira/browse/MAPREDUCE-2260) | Remove auto-generated native build files |  Major | build | Roman Shaposhnik | Roman Shaposhnik |
| [MAPREDUCE-2184](https://issues.apache.org/jira/browse/MAPREDUCE-2184) | Port DistRaid.java to new mapreduce API |  Major | contrib/raid | Ramkumar Vadali | Ramkumar Vadali |
| [MAPREDUCE-2167](https://issues.apache.org/jira/browse/MAPREDUCE-2167) | Faster directory traversal for raid node |  Major | contrib/raid | Ramkumar Vadali | Ramkumar Vadali |
| [MAPREDUCE-2150](https://issues.apache.org/jira/browse/MAPREDUCE-2150) | RaidNode should periodically fix corrupt blocks |  Major | contrib/raid | Ramkumar Vadali | Ramkumar Vadali |
| [MAPREDUCE-2147](https://issues.apache.org/jira/browse/MAPREDUCE-2147) | JobInProgress has some redundant lines in its ctor |  Trivial | jobtracker | Harsh J | Harsh J |
| [MAPREDUCE-2145](https://issues.apache.org/jira/browse/MAPREDUCE-2145) | upgrade clover on the builds servers to v 3.0.2 |  Major | . | Giridharan Kesavan | Giridharan Kesavan |
| [MAPREDUCE-2141](https://issues.apache.org/jira/browse/MAPREDUCE-2141) | Add an "extra data" field to Task for use by Mesos |  Minor | . | Matei Zaharia | Matei Zaharia |
| [MAPREDUCE-2140](https://issues.apache.org/jira/browse/MAPREDUCE-2140) | Re-generate fair scheduler design doc PDF |  Trivial | . | Matei Zaharia | Matei Zaharia |
| [MAPREDUCE-2132](https://issues.apache.org/jira/browse/MAPREDUCE-2132) | Need a command line option in RaidShell to fix blocks using raid |  Major | contrib/raid | Ramkumar Vadali | Ramkumar Vadali |
| [MAPREDUCE-2126](https://issues.apache.org/jira/browse/MAPREDUCE-2126) | JobQueueJobInProgressListener's javadoc is inconsistent with source code |  Minor | jobtracker | Jingguo Yao | Jingguo Yao |
| [MAPREDUCE-2103](https://issues.apache.org/jira/browse/MAPREDUCE-2103) | task-controller shouldn't require o-r permissions |  Trivial | task-controller | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2093](https://issues.apache.org/jira/browse/MAPREDUCE-2093) | Herriot JT and TT clients should vend statistics |  Major | test | Konstantin Boudnik | Konstantin Boudnik |
| [MAPREDUCE-2046](https://issues.apache.org/jira/browse/MAPREDUCE-2046) | A CombineFileInputSplit cannot be less than a dfs block |  Major | . | Namit Jain | dhruba borthakur |
| [MAPREDUCE-2026](https://issues.apache.org/jira/browse/MAPREDUCE-2026) | JobTracker.getJobCounters() should not hold JobTracker lock while calling JobInProgress.getCounters() |  Major | . | Scott Chen | Joydeep Sen Sarma |
| [MAPREDUCE-1945](https://issues.apache.org/jira/browse/MAPREDUCE-1945) | Support for using different Kerberos keys for Jobtracker and TaskTrackers |  Major | . | Kan Zhang | Kan Zhang |
| [MAPREDUCE-1936](https://issues.apache.org/jira/browse/MAPREDUCE-1936) | [gridmix3] Make Gridmix3 more customizable. |  Major | contrib/gridmix | Hong Tang | Hong Tang |
| [MAPREDUCE-1935](https://issues.apache.org/jira/browse/MAPREDUCE-1935) | HFTP needs to be updated to use delegation tokens (from HDFS-1007) |  Major | . | Boris Shkolnik | Boris Shkolnik |
| [MAPREDUCE-1931](https://issues.apache.org/jira/browse/MAPREDUCE-1931) | Gridmix forrest documentation |  Major | contrib/gridmix | rahul k singh | Ranjit Mathew |
| [MAPREDUCE-1918](https://issues.apache.org/jira/browse/MAPREDUCE-1918) | Add documentation to Rumen |  Major | tools/rumen | Amar Kamat | Amar Kamat |
| [MAPREDUCE-1893](https://issues.apache.org/jira/browse/MAPREDUCE-1893) | Multiple reducers for Slive |  Major | benchmarks, test | Konstantin Shvachko | Konstantin Shvachko |
| [MAPREDUCE-1892](https://issues.apache.org/jira/browse/MAPREDUCE-1892) | RaidNode can allow layered policies more efficiently |  Major | contrib/raid | Ramkumar Vadali | Ramkumar Vadali |
| [MAPREDUCE-1878](https://issues.apache.org/jira/browse/MAPREDUCE-1878) | Add MRUnit documentation |  Major | contrib/mrunit | Aaron Kimball | Aaron Kimball |
| [MAPREDUCE-1868](https://issues.apache.org/jira/browse/MAPREDUCE-1868) | Add read timeout on userlog pull |  Major | client | Krishna Ramachandran | Krishna Ramachandran |
| [MAPREDUCE-1851](https://issues.apache.org/jira/browse/MAPREDUCE-1851) | Document configuration parameters in streaming |  Major | contrib/streaming, documentation | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [MAPREDUCE-1850](https://issues.apache.org/jira/browse/MAPREDUCE-1850) | Include job submit host information (name and ip) in jobconf and jobdetails display |  Major | . | Krishna Ramachandran | Krishna Ramachandran |
| [MAPREDUCE-1848](https://issues.apache.org/jira/browse/MAPREDUCE-1848) | Put number of speculative, data local, rack local tasks in JobTracker metrics |  Major | jobtracker | Scott Chen | Scott Chen |
| [MAPREDUCE-1840](https://issues.apache.org/jira/browse/MAPREDUCE-1840) | [Gridmix] Exploit/Add security features in GridMix |  Major | contrib/gridmix | Amar Kamat | Amar Kamat |
| [MAPREDUCE-1838](https://issues.apache.org/jira/browse/MAPREDUCE-1838) | DistRaid map tasks have large variance in running times |  Minor | contrib/raid | Ramkumar Vadali | Ramkumar Vadali |
| [MAPREDUCE-1832](https://issues.apache.org/jira/browse/MAPREDUCE-1832) | Support for file sizes less than 1MB in DFSIO benchmark. |  Major | benchmarks | Konstantin Shvachko | Konstantin Shvachko |
| [MAPREDUCE-1829](https://issues.apache.org/jira/browse/MAPREDUCE-1829) | JobInProgress.findSpeculativeTask should use min() to find the candidate instead of sort() |  Major | jobtracker | Scott Chen | Scott Chen |
| [MAPREDUCE-1819](https://issues.apache.org/jira/browse/MAPREDUCE-1819) | RaidNode should be smarter in submitting Raid jobs |  Major | contrib/raid | Ramkumar Vadali | Ramkumar Vadali |
| [MAPREDUCE-1818](https://issues.apache.org/jira/browse/MAPREDUCE-1818) | RaidNode should specify a pool name incase the cluster is using FairScheduler |  Minor | contrib/raid | Ramkumar Vadali | Ramkumar Vadali |
| [MAPREDUCE-1816](https://issues.apache.org/jira/browse/MAPREDUCE-1816) | HAR files used for RAID parity need to have configurable partfile size |  Minor | contrib/raid | Ramkumar Vadali | Ramkumar Vadali |
| [MAPREDUCE-1798](https://issues.apache.org/jira/browse/MAPREDUCE-1798) | normalize property names for JT kerberos principal names in configuration (from HADOOP 6633) |  Major | . | Boris Shkolnik | Boris Shkolnik |
| [MAPREDUCE-1785](https://issues.apache.org/jira/browse/MAPREDUCE-1785) | Add streaming config option for not emitting the key |  Minor | contrib/streaming | Eli Collins | Eli Collins |
| [MAPREDUCE-1783](https://issues.apache.org/jira/browse/MAPREDUCE-1783) | Task Initialization should be delayed till when a job can be run |  Major | contrib/fair-share | Ramkumar Vadali | Ramkumar Vadali |
| [MAPREDUCE-1778](https://issues.apache.org/jira/browse/MAPREDUCE-1778) | CompletedJobStatusStore initialization should fail if {mapred.job.tracker.persist.jobstatus.dir} is unwritable |  Major | jobtracker | Amar Kamat | Krishna Ramachandran |
| [MAPREDUCE-1762](https://issues.apache.org/jira/browse/MAPREDUCE-1762) | Add a setValue() method in Counter |  Major | . | Scott Chen | Scott Chen |
| [MAPREDUCE-1761](https://issues.apache.org/jira/browse/MAPREDUCE-1761) | FairScheduler should allow separate configuration of node and rack locality wait time |  Major | . | Scott Chen | Scott Chen |
| [MAPREDUCE-1711](https://issues.apache.org/jira/browse/MAPREDUCE-1711) | Gridmix should provide an option to submit jobs to the same queues as specified in the trace. |  Major | contrib/gridmix | Hong Tang | rahul k singh |
| [MAPREDUCE-1626](https://issues.apache.org/jira/browse/MAPREDUCE-1626) | Publish Javadoc for all contrib packages with user-facing APIs |  Major | documentation | Tom White | Jolly Chen |
| [MAPREDUCE-1592](https://issues.apache.org/jira/browse/MAPREDUCE-1592) | Generate Eclipse's .classpath file from Ivy config |  Major | build | Tom White | Tom White |
| [MAPREDUCE-1548](https://issues.apache.org/jira/browse/MAPREDUCE-1548) | Hadoop archives should be able to preserve times and other properties from original files |  Major | harchive | Rodrigo Schmidt | Rodrigo Schmidt |
| [MAPREDUCE-1546](https://issues.apache.org/jira/browse/MAPREDUCE-1546) | Jobtracker JSP pages should automatically redirect to the corresponding history page if not in memory |  Minor | . | Scott Chen | Scott Chen |
| [MAPREDUCE-1545](https://issues.apache.org/jira/browse/MAPREDUCE-1545) | Add 'first-task-launched' to job-summary |  Major | jobtracker | Arun C Murthy | Luke Lu |
| [MAPREDUCE-1526](https://issues.apache.org/jira/browse/MAPREDUCE-1526) | Cache the job related information while submitting the job , this would avoid many RPC calls to JobTracker. |  Major | contrib/gridmix | rahul k singh | rahul k singh |
| [MAPREDUCE-1492](https://issues.apache.org/jira/browse/MAPREDUCE-1492) | Delete or recreate obsolete har files used on hdfs raid |  Major | contrib/raid | Rodrigo Schmidt | Rodrigo Schmidt |
| [MAPREDUCE-1382](https://issues.apache.org/jira/browse/MAPREDUCE-1382) | MRAsyncDiscService should tolerate missing local.dir |  Major | . | Scott Chen | Zheng Shao |
| [MAPREDUCE-1376](https://issues.apache.org/jira/browse/MAPREDUCE-1376) | Support for varied user submission in Gridmix |  Major | contrib/gridmix | Chris Douglas | Chris Douglas |
| [MAPREDUCE-1354](https://issues.apache.org/jira/browse/MAPREDUCE-1354) | Incremental enhancements to the JobTracker for better scalability |  Critical | jobtracker | Devaraj Das | Dick King |
| [MAPREDUCE-1253](https://issues.apache.org/jira/browse/MAPREDUCE-1253) | Making Mumak work with Capacity-Scheduler |  Major | contrib/mumak | Anirban Dasgupta | Anirban Dasgupta |
| [MAPREDUCE-1248](https://issues.apache.org/jira/browse/MAPREDUCE-1248) | Redundant memory copying in StreamKeyValUtil |  Minor | contrib/streaming | Ruibang He | Ruibang He |
| [MAPREDUCE-1159](https://issues.apache.org/jira/browse/MAPREDUCE-1159) | Limit Job name on jobtracker.jsp to be 80 char long |  Trivial | . | Zheng Shao | Harsh J |
| [MAPREDUCE-478](https://issues.apache.org/jira/browse/MAPREDUCE-478) | separate jvm param for mapper and reducer |  Minor | . | Koji Noguchi | Arun C Murthy |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-7697](https://issues.apache.org/jira/browse/HADOOP-7697) | Remove dependency on different version of slf4j in avro |  Major | build | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-7663](https://issues.apache.org/jira/browse/HADOOP-7663) | TestHDFSTrash failing on 22 |  Major | test | Mayank Bansal | Mayank Bansal |
| [HADOOP-7646](https://issues.apache.org/jira/browse/HADOOP-7646) | Make hadoop-common use same version of avro as HBase |  Major | io, ipc | Joep Rottinghuis | Joep Rottinghuis |
| [HADOOP-7577](https://issues.apache.org/jira/browse/HADOOP-7577) | TT does not start due to backwards compatibility wrt. EventCounter |  Minor | metrics | Joep Rottinghuis | Joep Rottinghuis |
| [HADOOP-7568](https://issues.apache.org/jira/browse/HADOOP-7568) | SequenceFile should not print into stdout |  Major | io | Konstantin Shvachko | Plamen Jeliazkov |
| [HADOOP-7514](https://issues.apache.org/jira/browse/HADOOP-7514) | Build fails with ClassCastException when running both mvn-install and mvn-deploy targets |  Minor | build | Joep Rottinghuis | Joep Rottinghuis |
| [HADOOP-7513](https://issues.apache.org/jira/browse/HADOOP-7513) | mvn-deploy target fails |  Major | build | Joep Rottinghuis | Joep Rottinghuis |
| [HADOOP-7450](https://issues.apache.org/jira/browse/HADOOP-7450) | Bump jetty to 6.1.26 |  Blocker | build | Jitendra Nath Pandey | Konstantin Boudnik |
| [HADOOP-7390](https://issues.apache.org/jira/browse/HADOOP-7390) | VersionInfo not generated properly in git after unsplit |  Minor | build | Thomas Graves | Todd Lipcon |
| [HADOOP-7351](https://issues.apache.org/jira/browse/HADOOP-7351) | Regression: HttpServer#getWebAppsPath used to be protected so subclasses could supply alternate webapps path but it was made private by HADOOP-6461 |  Major | . | stack | stack |
| [HADOOP-7349](https://issues.apache.org/jira/browse/HADOOP-7349) | HADOOP-7121 accidentally disabled some tests |  Major | ipc, test | Todd Lipcon | Todd Lipcon |
| [HADOOP-7318](https://issues.apache.org/jira/browse/HADOOP-7318) | MD5Hash factory should reset the digester it returns |  Critical | io | Todd Lipcon | Todd Lipcon |
| [HADOOP-7312](https://issues.apache.org/jira/browse/HADOOP-7312) | core-default.xml lists configuration version as 0.21 |  Minor | conf | Todd Lipcon | Harsh J |
| [HADOOP-7302](https://issues.apache.org/jira/browse/HADOOP-7302) | webinterface.private.actions should not be in common |  Major | documentation | Ari Rabkin | Ari Rabkin |
| [HADOOP-7300](https://issues.apache.org/jira/browse/HADOOP-7300) | Configuration methods that return collections are inconsistent about mutability |  Major | conf | Todd Lipcon | Todd Lipcon |
| [HADOOP-7296](https://issues.apache.org/jira/browse/HADOOP-7296) | The FsPermission(FsPermission) constructor does not use the sticky bit |  Minor | fs | Siddharth Seth | Siddharth Seth |
| [HADOOP-7287](https://issues.apache.org/jira/browse/HADOOP-7287) | Configuration deprecation mechanism doesn't work properly for GenericOptionsParser/Tools |  Blocker | conf | Todd Lipcon | Aaron T. Myers |
| [HADOOP-7252](https://issues.apache.org/jira/browse/HADOOP-7252) | JUnit shows up as a compile time dependency |  Minor | build, conf, test | Pony | Harsh J |
| [HADOOP-7245](https://issues.apache.org/jira/browse/HADOOP-7245) | FsConfig should use constants in CommonConfigurationKeys |  Major | . | Tom White | Tom White |
| [HADOOP-7194](https://issues.apache.org/jira/browse/HADOOP-7194) | Potential Resource leak in IOUtils.java |  Major | io | Devaraj K | Devaraj K |
| [HADOOP-7187](https://issues.apache.org/jira/browse/HADOOP-7187) | Socket Leak in org.apache.hadoop.metrics.ganglia.GangliaContext |  Major | metrics | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-7184](https://issues.apache.org/jira/browse/HADOOP-7184) | Remove deprecated local.cache.size from core-default.xml |  Major | documentation, filecache | Todd Lipcon | Todd Lipcon |
| [HADOOP-7183](https://issues.apache.org/jira/browse/HADOOP-7183) | WritableComparator.get should not cache comparator objects |  Blocker | . | Todd Lipcon | Tom White |
| [HADOOP-7174](https://issues.apache.org/jira/browse/HADOOP-7174) | null is displayed in the console,if the src path is invalid while doing copyToLocal operation from commandLine |  Minor | fs | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HADOOP-7172](https://issues.apache.org/jira/browse/HADOOP-7172) | SecureIO should not check owner on non-secure clusters that have no native support |  Critical | io, security | Todd Lipcon | Todd Lipcon |
| [HADOOP-7162](https://issues.apache.org/jira/browse/HADOOP-7162) | FsShell: call srcFs.listStatus(src) twice |  Minor | fs | Alexey Diomin | Alexey Diomin |
| [HADOOP-7156](https://issues.apache.org/jira/browse/HADOOP-7156) | getpwuid\_r is not thread-safe on RHEL6 |  Critical | . | Todd Lipcon | Todd Lipcon |
| [HADOOP-7146](https://issues.apache.org/jira/browse/HADOOP-7146) | RPC server leaks file descriptors |  Major | . | Todd Lipcon | Todd Lipcon |
| [HADOOP-7145](https://issues.apache.org/jira/browse/HADOOP-7145) | Configuration.getLocalPath should trim whitespace from the provided directories |  Major | . | Todd Lipcon | Todd Lipcon |
| [HADOOP-7140](https://issues.apache.org/jira/browse/HADOOP-7140) | IPC Reader threads do not stop when server stops |  Critical | . | Todd Lipcon | Todd Lipcon |
| [HADOOP-7126](https://issues.apache.org/jira/browse/HADOOP-7126) | TestDFSShell fails in trunk |  Major | . | Po Cheung | Po Cheung |
| [HADOOP-7122](https://issues.apache.org/jira/browse/HADOOP-7122) | Timed out shell commands leak Timer threads |  Critical | util | Todd Lipcon | Todd Lipcon |
| [HADOOP-7121](https://issues.apache.org/jira/browse/HADOOP-7121) | Exceptions while serializing IPC call response are not handled well |  Critical | ipc | Todd Lipcon | Todd Lipcon |
| [HADOOP-7120](https://issues.apache.org/jira/browse/HADOOP-7120) | 200 new Findbugs warnings |  Major | test | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HADOOP-7118](https://issues.apache.org/jira/browse/HADOOP-7118) | NPE in Configuration.writeXml |  Blocker | . | Todd Lipcon | Todd Lipcon |
| [HADOOP-7104](https://issues.apache.org/jira/browse/HADOOP-7104) | Remove unnecessary DNS reverse lookups from RPC layer |  Major | ipc, security | Kan Zhang | Kan Zhang |
| [HADOOP-7102](https://issues.apache.org/jira/browse/HADOOP-7102) | Remove "fs.ramfs.impl" field from core-deafult.xml |  Major | conf | Konstantin Shvachko | Konstantin Shvachko |
| [HADOOP-7100](https://issues.apache.org/jira/browse/HADOOP-7100) | Build broken by HADOOP-6811 |  Blocker | build, contrib/cloud | Todd Lipcon | Todd Lipcon |
| [HADOOP-7097](https://issues.apache.org/jira/browse/HADOOP-7097) | java.library.path missing basedir |  Blocker | build, native | Noah Watkins | Noah Watkins |
| [HADOOP-7094](https://issues.apache.org/jira/browse/HADOOP-7094) | hadoop.css got lost during project split |  Blocker | build | Todd Lipcon | Konstantin Boudnik |
| [HADOOP-7093](https://issues.apache.org/jira/browse/HADOOP-7093) | Servlets should default to text/plain |  Major | io | Todd Lipcon | Todd Lipcon |
| [HADOOP-7091](https://issues.apache.org/jira/browse/HADOOP-7091) | reloginFromKeytab() should happen even if TGT can't be found |  Major | security | Kan Zhang | Kan Zhang |
| [HADOOP-7089](https://issues.apache.org/jira/browse/HADOOP-7089) | Fix link resolution logic in hadoop-config.sh |  Minor | scripts | Eli Collins | Eli Collins |
| [HADOOP-7087](https://issues.apache.org/jira/browse/HADOOP-7087) | SequenceFile.createWriter ignores FileSystem parameter |  Blocker | io | Todd Lipcon | Todd Lipcon |
| [HADOOP-7082](https://issues.apache.org/jira/browse/HADOOP-7082) | Configuration.writeXML should not hold lock while outputting |  Critical | conf | Todd Lipcon | Todd Lipcon |
| [HADOOP-7070](https://issues.apache.org/jira/browse/HADOOP-7070) | JAAS configuration should delegate unknown application names to pre-existing configuration |  Critical | security | Todd Lipcon | Todd Lipcon |
| [HADOOP-7068](https://issues.apache.org/jira/browse/HADOOP-7068) | Ivy resolve force mode should be turned off by default |  Major | . | Luke Lu | Luke Lu |
| [HADOOP-7057](https://issues.apache.org/jira/browse/HADOOP-7057) | IOUtils.readFully and IOUtils.skipFully have typo in exception creation's message |  Minor | util | Konstantin Boudnik | Konstantin Boudnik |
| [HADOOP-7053](https://issues.apache.org/jira/browse/HADOOP-7053) | wrong FSNamesystem Audit logging setting in conf/log4j.properties |  Minor | conf | Jingguo Yao | Jingguo Yao |
| [HADOOP-7052](https://issues.apache.org/jira/browse/HADOOP-7052) | misspelling of threshold in conf/log4j.properties |  Major | conf | Jingguo Yao | Jingguo Yao |
| [HADOOP-7046](https://issues.apache.org/jira/browse/HADOOP-7046) | 1 Findbugs warning on trunk and branch-0.22 |  Blocker | security | Nigel Daley | Po Cheung |
| [HADOOP-7038](https://issues.apache.org/jira/browse/HADOOP-7038) | saveVersion script includes an additional \r while running whoami under windows |  Minor | build | Wang Xu | Wang Xu |
| [HADOOP-7028](https://issues.apache.org/jira/browse/HADOOP-7028) | ant eclipse does not include requisite ant.jar in the classpath |  Minor | build | Patrick Angeles | Patrick Angeles |
| [HADOOP-7011](https://issues.apache.org/jira/browse/HADOOP-7011) | KerberosName.main(...) throws NPE |  Major | . | Aaron T. Myers | Aaron T. Myers |
| [HADOOP-7006](https://issues.apache.org/jira/browse/HADOOP-7006) | hadoop fs -getmerge does not work using codebase from trunk. |  Major | fs | Chris Nauroth | Chris Nauroth |
| [HADOOP-6993](https://issues.apache.org/jira/browse/HADOOP-6993) | Broken link on cluster setup page of docs |  Major | documentation | Aaron T. Myers | Eli Collins |
| [HADOOP-6991](https://issues.apache.org/jira/browse/HADOOP-6991) | SequenceFile::Reader discards length for files, does not call openFile |  Minor | . | Chris Douglas | Chris Douglas |
| [HADOOP-6989](https://issues.apache.org/jira/browse/HADOOP-6989) | TestSetFile is failing on trunk |  Major | . | Jakob Homan | Chris Douglas |
| [HADOOP-6984](https://issues.apache.org/jira/browse/HADOOP-6984) | NPE from SequenceFile::Writer.CompressionCodecOption |  Minor | io | Chris Douglas | Chris Douglas |
| [HADOOP-6975](https://issues.apache.org/jira/browse/HADOOP-6975) | integer overflow in S3InputStream for blocks \> 2GB |  Major | . | Patrick Kling | Patrick Kling |
| [HADOOP-6970](https://issues.apache.org/jira/browse/HADOOP-6970) | SecurityAuth.audit should be generated under /build |  Major | build | Konstantin Shvachko | Boris Shkolnik |
| [HADOOP-6965](https://issues.apache.org/jira/browse/HADOOP-6965) | Method in UGI to get Kerberos ticket. |  Major | . | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [HADOOP-6951](https://issues.apache.org/jira/browse/HADOOP-6951) | Distinct minicluster services (e.g. NN and JT) overwrite each other's service policies |  Major | security | Aaron T. Myers | Aaron T. Myers |
| [HADOOP-6947](https://issues.apache.org/jira/browse/HADOOP-6947) | Kerberos relogin should set refreshKrb5Config to true |  Major | security | Todd Lipcon | Todd Lipcon |
| [HADOOP-6940](https://issues.apache.org/jira/browse/HADOOP-6940) | RawLocalFileSystem's markSupported method misnamed markSupport |  Minor | fs | Tom White | Tom White |
| [HADOOP-6938](https://issues.apache.org/jira/browse/HADOOP-6938) | ConnectionId.getRemotePrincipal() should check if security is enabled |  Major | ipc, security | Kan Zhang | Kan Zhang |
| [HADOOP-6933](https://issues.apache.org/jira/browse/HADOOP-6933) | TestListFiles is flaky |  Minor | test | Todd Lipcon | Todd Lipcon |
| [HADOOP-6932](https://issues.apache.org/jira/browse/HADOOP-6932) | Namenode start (init) fails because of invalid kerberos key, even when security set to "simple" |  Major | . | Boris Shkolnik | Boris Shkolnik |
| [HADOOP-6930](https://issues.apache.org/jira/browse/HADOOP-6930) | AvroRpcEngine doesn't work with generated Avro code |  Major | ipc | Sharad Agarwal | Sharad Agarwal |
| [HADOOP-6926](https://issues.apache.org/jira/browse/HADOOP-6926) | SocketInputStream incorrectly implements read() |  Minor | io | Todd Lipcon | Todd Lipcon |
| [HADOOP-6925](https://issues.apache.org/jira/browse/HADOOP-6925) | BZip2Codec incorrectly implements read() |  Critical | io | Todd Lipcon | Todd Lipcon |
| [HADOOP-6922](https://issues.apache.org/jira/browse/HADOOP-6922) | COMMON part of MAPREDUCE-1664 |  Major | documentation, security | Ravi Gummadi | Ravi Gummadi |
| [HADOOP-6913](https://issues.apache.org/jira/browse/HADOOP-6913) | Circular initialization between UserGroupInformation and KerberosName |  Major | security | Kan Zhang | Kan Zhang |
| [HADOOP-6907](https://issues.apache.org/jira/browse/HADOOP-6907) | Rpc client doesn't use the per-connection conf to figure out server's Kerberos principal |  Major | ipc, security | Kan Zhang | Kan Zhang |
| [HADOOP-6906](https://issues.apache.org/jira/browse/HADOOP-6906) | FileContext copy() utility doesn't work with recursive copying of directories. |  Major | fs | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [HADOOP-6900](https://issues.apache.org/jira/browse/HADOOP-6900) | FileSystem#listLocatedStatus should not throw generic RuntimeException to indicate error conditions |  Major | . | Suresh Srinivas | Hairong Kuang |
| [HADOOP-6899](https://issues.apache.org/jira/browse/HADOOP-6899) | RawLocalFileSystem#setWorkingDir() does not work for relative names |  Major | fs | Sanjay Radia | Sanjay Radia |
| [HADOOP-6898](https://issues.apache.org/jira/browse/HADOOP-6898) | FileSystem.copyToLocal creates files with 777 permissions |  Blocker | fs, security | Todd Lipcon | Aaron T. Myers |
| [HADOOP-6888](https://issues.apache.org/jira/browse/HADOOP-6888) | Being able to close all cached FileSystem objects for a given UGI |  Major | fs, security | Kan Zhang | Kan Zhang |
| [HADOOP-6885](https://issues.apache.org/jira/browse/HADOOP-6885) | Fix java doc warnings in Groups and RefreshUserMappingsProtocol |  Major | security | Eli Collins | Eli Collins |
| [HADOOP-6873](https://issues.apache.org/jira/browse/HADOOP-6873) | using delegation token over hftp for long running clients (part of hdfs 1296) |  Major | . | Boris Shkolnik | Boris Shkolnik |
| [HADOOP-6853](https://issues.apache.org/jira/browse/HADOOP-6853) | Common component of HDFS-1045 |  Major | . | Jakob Homan | Jakob Homan |
| [HADOOP-6834](https://issues.apache.org/jira/browse/HADOOP-6834) | TFile.append compares initial key against null lastKey |  Major | io | Ahad Rana | Hong Tang |
| [HADOOP-6833](https://issues.apache.org/jira/browse/HADOOP-6833) | IPC leaks call parameters when exceptions thrown |  Blocker | . | Todd Lipcon | Todd Lipcon |
| [HADOOP-6815](https://issues.apache.org/jira/browse/HADOOP-6815) | refreshSuperUserGroupsConfiguration should use server side configuration for the refresh |  Major | . | Boris Shkolnik | Boris Shkolnik |
| [HADOOP-6812](https://issues.apache.org/jira/browse/HADOOP-6812) | fs.inmemory.size.mb not listed in conf. Cluster setup page gives wrong advice. |  Major | documentation | Edward Capriolo | Chris Douglas |
| [HADOOP-6781](https://issues.apache.org/jira/browse/HADOOP-6781) | security audit log shouldn't have exception in it. |  Major | . | Boris Shkolnik | Boris Shkolnik |
| [HADOOP-6778](https://issues.apache.org/jira/browse/HADOOP-6778) | add isRunning() method to AbstractDelegationTokenSecretManager (for  HDFS-1044) |  Major | . | Boris Shkolnik | Boris Shkolnik |
| [HADOOP-6763](https://issues.apache.org/jira/browse/HADOOP-6763) | Remove verbose logging from the Groups class |  Major | . | Owen O'Malley | Boris Shkolnik |
| [HADOOP-6758](https://issues.apache.org/jira/browse/HADOOP-6758) | MapFile.fix does not allow index interval definition |  Major | . | Gianmarco De Francisci Morales | Gianmarco De Francisci Morales |
| [HADOOP-6756](https://issues.apache.org/jira/browse/HADOOP-6756) | Clean up and add documentation for configuration keys in CommonConfigurationKeys.java |  Major | fs | Erik Steffl | Erik Steffl |
| [HADOOP-6747](https://issues.apache.org/jira/browse/HADOOP-6747) | TestNetUtils fails on Mac OS X |  Major | . | Luke Lu | Todd Lipcon |
| [HADOOP-6724](https://issues.apache.org/jira/browse/HADOOP-6724) | IPC doesn't properly handle IOEs thrown by socket factory |  Major | ipc | Todd Lipcon | Todd Lipcon |
| [HADOOP-6715](https://issues.apache.org/jira/browse/HADOOP-6715) | AccessControlList.toString() returns empty string when we set acl to "\*" |  Major | security, util | Ravi Gummadi | Ravi Gummadi |
| [HADOOP-6706](https://issues.apache.org/jira/browse/HADOOP-6706) | Relogin behavior for RPC clients could be improved |  Major | security | Devaraj Das | Devaraj Das |
| [HADOOP-6702](https://issues.apache.org/jira/browse/HADOOP-6702) | Incorrect exit codes for "dfs -chown", "dfs -chgrp"  when input is given in wildcard format. |  Minor | fs | Ravi Phulari | Ravi Phulari |
| [HADOOP-6682](https://issues.apache.org/jira/browse/HADOOP-6682) | NetUtils:normalizeHostName does not process hostnames starting with [a-f] correctly |  Major | io | Jakob Homan | Jakob Homan |
| [HADOOP-6670](https://issues.apache.org/jira/browse/HADOOP-6670) | UserGroupInformation doesn't support use in hash tables |  Major | security | Owen O'Malley | Owen O'Malley |
| [HADOOP-6669](https://issues.apache.org/jira/browse/HADOOP-6669) | zlib.compress.level  ignored for DefaultCodec initialization |  Minor | io | Koji Noguchi | Koji Noguchi |
| [HADOOP-6663](https://issues.apache.org/jira/browse/HADOOP-6663) | BlockDecompressorStream get EOF exception when decompressing the file compressed from empty file |  Major | io | Kang Xiao | Kang Xiao |
| [HADOOP-6656](https://issues.apache.org/jira/browse/HADOOP-6656) | Security framework needs to renew Kerberos tickets while the process is running |  Major | . | Owen O'Malley | Devaraj Das |
| [HADOOP-6652](https://issues.apache.org/jira/browse/HADOOP-6652) | ShellBasedUnixGroupsMapping shouldn't have a cache |  Major | . | Devaraj Das | Devaraj Das |
| [HADOOP-6648](https://issues.apache.org/jira/browse/HADOOP-6648) | Credentials should ignore null tokens |  Major | security | Owen O'Malley | Devaraj Das |
| [HADOOP-6642](https://issues.apache.org/jira/browse/HADOOP-6642) | Fix javac, javadoc, findbugs warnings |  Major | . | Arun C Murthy | Po Cheung |
| [HADOOP-6620](https://issues.apache.org/jira/browse/HADOOP-6620) | NPE if renewer is passed as null in getDelegationToken |  Major | . | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [HADOOP-6613](https://issues.apache.org/jira/browse/HADOOP-6613) | RPC server should check for version mismatch first |  Major | ipc, security | Kan Zhang | Kan Zhang |
| [HADOOP-6612](https://issues.apache.org/jira/browse/HADOOP-6612) | Protocols RefreshUserToGroupMappingsProtocol and RefreshAuthorizationPolicyProtocol will fail with security enabled |  Major | security | Boris Shkolnik | Boris Shkolnik |
| [HADOOP-6536](https://issues.apache.org/jira/browse/HADOOP-6536) | FileUtil.fullyDelete(dir) behavior is not defined when we pass a symlink as the argument |  Major | fs | Amareshwari Sriramadasu | Ravi Gummadi |
| [HADOOP-6496](https://issues.apache.org/jira/browse/HADOOP-6496) | HttpServer sends wrong content-type for CSS files (and others) |  Minor | . | Lars Francke | Ivan Mitic |
| [HADOOP-6482](https://issues.apache.org/jira/browse/HADOOP-6482) | GenericOptionsParser constructor that takes Options and String[] ignores options |  Minor | util | Chris Wilkes | Eli Collins |
| [HADOOP-6404](https://issues.apache.org/jira/browse/HADOOP-6404) | Rename the generated artifacts to common instead of core |  Blocker | build | Owen O'Malley | Tom White |
| [HADOOP-6344](https://issues.apache.org/jira/browse/HADOOP-6344) | rm and rmr fail to correctly move the user's files to the trash prior to deleting when they are over quota. |  Major | fs | gary murry | Jakob Homan |
| [HDFS-2573](https://issues.apache.org/jira/browse/HDFS-2573) | TestFiDataXceiverServer is failing, not testing OOME |  Major | datanode, test | Konstantin Shvachko | Konstantin Boudnik |
| [HDFS-2514](https://issues.apache.org/jira/browse/HDFS-2514) | Link resolution bug for intermediate symlinks with relative targets |  Major | namenode | Eli Collins | Eli Collins |
| [HDFS-2491](https://issues.apache.org/jira/browse/HDFS-2491) | TestBalancer can fail when datanode utilization and avgUtilization is exactly same. |  Major | . | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-2452](https://issues.apache.org/jira/browse/HDFS-2452) | OutOfMemoryError in DataXceiverServer takes down the DataNode |  Major | datanode | Konstantin Shvachko | Uma Maheswara Rao G |
| [HDFS-2388](https://issues.apache.org/jira/browse/HDFS-2388) | Remove dependency on different version of slf4j in avro |  Major | build | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-2383](https://issues.apache.org/jira/browse/HDFS-2383) | TestDfsOverAvroRpc is failing on 0.22 |  Blocker | test | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-2346](https://issues.apache.org/jira/browse/HDFS-2346) | TestHost2NodesMap & TestReplicasMap will fail depending upon execution order of test methods |  Blocker | test | Uma Maheswara Rao G | Laxman |
| [HDFS-2343](https://issues.apache.org/jira/browse/HDFS-2343) | Make hdfs use same version of avro as HBase |  Blocker | hdfs-client | Joep Rottinghuis | Joep Rottinghuis |
| [HDFS-2341](https://issues.apache.org/jira/browse/HDFS-2341) | Contribs not building |  Blocker | build | Joep Rottinghuis | Joep Rottinghuis |
| [HDFS-2315](https://issues.apache.org/jira/browse/HDFS-2315) | Build fails with ant 1.7.0 but works with 1.8.0 |  Blocker | build | Joep Rottinghuis | Joep Rottinghuis |
| [HDFS-2297](https://issues.apache.org/jira/browse/HDFS-2297) | FindBugs OutOfMemoryError |  Blocker | build | Joep Rottinghuis | Joep Rottinghuis |
| [HDFS-2290](https://issues.apache.org/jira/browse/HDFS-2290) | Block with corrupt replica is not getting replicated |  Major | namenode | Konstantin Shvachko | Benoy Antony |
| [HDFS-2287](https://issues.apache.org/jira/browse/HDFS-2287) | TestParallelRead has a small off-by-one bug |  Trivial | test | Todd Lipcon | Todd Lipcon |
| [HDFS-2281](https://issues.apache.org/jira/browse/HDFS-2281) | NPE in checkpoint during processIOError() |  Major | namenode | Konstantin Shvachko | Uma Maheswara Rao G |
| [HDFS-2280](https://issues.apache.org/jira/browse/HDFS-2280) | BackupNode fails with MD5 checksum Exception during checkpoint if BN's image is outdated. |  Major | namenode | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-2271](https://issues.apache.org/jira/browse/HDFS-2271) | startJournalSpool should invoke ProcessIOError with failed storage directories if createEditLogFile throws any exception. |  Major | namenode | Uma Maheswara Rao G | Uma Maheswara Rao G |
| [HDFS-2258](https://issues.apache.org/jira/browse/HDFS-2258) | TestLeaseRecovery2 fails as lease hard limit is not reset to default |  Major | namenode, test | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-2232](https://issues.apache.org/jira/browse/HDFS-2232) | TestHDFSCLI fails on 0.22 branch |  Blocker | test | Konstantin Shvachko | Plamen Jeliazkov |
| [HDFS-2214](https://issues.apache.org/jira/browse/HDFS-2214) | Generated POMs hardcode dependency on hadoop-common version 0.22.0-SNAPSHOT |  Major | build | Joep Rottinghuis | Joep Rottinghuis |
| [HDFS-2211](https://issues.apache.org/jira/browse/HDFS-2211) | Build does not pass along properties to contrib builds |  Blocker | build | Joep Rottinghuis | Joep Rottinghuis |
| [HDFS-2189](https://issues.apache.org/jira/browse/HDFS-2189) | guava-r09 dependency missing from "ivy/hadoop-hdfs-template.xml" in HDFS. |  Blocker | . | Plamen Jeliazkov | Joep Rottinghuis |
| [HDFS-2071](https://issues.apache.org/jira/browse/HDFS-2071) | Use of isConnected() in DataXceiver is invalid |  Minor | datanode | Kihwal Lee | Kihwal Lee |
| [HDFS-2012](https://issues.apache.org/jira/browse/HDFS-2012) | Recurring failure of TestBalancer due to incorrect treatment of nodes whose utilization equals avgUtilization. |  Blocker | balancer & mover, test | Aaron T. Myers | Uma Maheswara Rao G |
| [HDFS-2002](https://issues.apache.org/jira/browse/HDFS-2002) | Incorrect computation of needed blocks in getTurnOffTip() |  Major | namenode | Konstantin Shvachko | Plamen Jeliazkov |
| [HDFS-2000](https://issues.apache.org/jira/browse/HDFS-2000) | Missing deprecation for io.bytes.per.checksum |  Major | . | Aaron T. Myers | Aaron T. Myers |
| [HDFS-1981](https://issues.apache.org/jira/browse/HDFS-1981) | When namenode goes down while checkpointing and if is started again subsequent Checkpointing is always failing |  Blocker | namenode | ramkrishna.s.vasudevan | Uma Maheswara Rao G |
| [HDFS-1978](https://issues.apache.org/jira/browse/HDFS-1978) | All but first option in LIBHDFS\_OPTS is ignored |  Major | libhdfs | Brock Noland | Eli Collins |
| [HDFS-1969](https://issues.apache.org/jira/browse/HDFS-1969) | Running rollback on new-version namenode destroys namespace |  Blocker | namenode | Todd Lipcon | Todd Lipcon |
| [HDFS-1965](https://issues.apache.org/jira/browse/HDFS-1965) | IPCs done using block token-based tickets can't reuse connections |  Critical | security | Todd Lipcon | Todd Lipcon |
| [HDFS-1964](https://issues.apache.org/jira/browse/HDFS-1964) | Incorrect HTML unescaping in DatanodeJspHelper.java |  Major | . | Aaron T. Myers | Aaron T. Myers |
| [HDFS-1952](https://issues.apache.org/jira/browse/HDFS-1952) | FSEditLog.open() appears to succeed even if all EDITS directories fail |  Major | . | Matt Foley | Andrew |
| [HDFS-1943](https://issues.apache.org/jira/browse/HDFS-1943) | fail to start datanode while start-dfs.sh is executed by root user |  Blocker | scripts | Wei Yongjun | Matt Foley |
| [HDFS-1936](https://issues.apache.org/jira/browse/HDFS-1936) | Updating the layout version from HDFS-1822 causes upgrade problems. |  Blocker | namenode | Suresh Srinivas | Suresh Srinivas |
| [HDFS-1932](https://issues.apache.org/jira/browse/HDFS-1932) | Ensure that HDFS configuration deprecations are set up in every spot that HDFS configurations are loaded. |  Critical | . | Todd Lipcon | Jolly Chen |
| [HDFS-1925](https://issues.apache.org/jira/browse/HDFS-1925) | SafeModeInfo should use DFS\_NAMENODE\_SAFEMODE\_THRESHOLD\_PCT\_DEFAULT instead of 0.95 |  Major | . | Konstantin Shvachko | Joey Echeverria |
| [HDFS-1921](https://issues.apache.org/jira/browse/HDFS-1921) | Save namespace can cause NN to be unable to come up on restart |  Blocker | . | Aaron T. Myers | Matt Foley |
| [HDFS-1909](https://issues.apache.org/jira/browse/HDFS-1909) | TestHDFSCLI fails due to typo in expected output |  Major | . | Tom White | Tom White |
| [HDFS-1897](https://issues.apache.org/jira/browse/HDFS-1897) | Documention refers to removed option dfs.network.script |  Minor | documentation | Ari Rabkin | Andrew Whang |
| [HDFS-1891](https://issues.apache.org/jira/browse/HDFS-1891) | TestBackupNode fails intermittently |  Major | test | Suresh Srinivas | Suresh Srinivas |
| [HDFS-1850](https://issues.apache.org/jira/browse/HDFS-1850) | DN should transmit absolute failed volume count rather than increments to the NN |  Major | datanode, namenode | Eli Collins | Eli Collins |
| [HDFS-1845](https://issues.apache.org/jira/browse/HDFS-1845) | symlink comes up as directory after namenode restart |  Major | . | John George | John George |
| [HDFS-1823](https://issues.apache.org/jira/browse/HDFS-1823) | start-dfs.sh script fails if HADOOP\_HOME is not set |  Blocker | scripts | Tom White | Tom White |
| [HDFS-1822](https://issues.apache.org/jira/browse/HDFS-1822) | Editlog opcodes overlap between 20 security and later releases |  Blocker | namenode | Suresh Srinivas | Suresh Srinivas |
| [HDFS-1821](https://issues.apache.org/jira/browse/HDFS-1821) | FileContext.createSymlink with kerberos enabled sets wrong owner |  Major | . | John George | John George |
| [HDFS-1806](https://issues.apache.org/jira/browse/HDFS-1806) | TestBlockReport.blockReport\_08() and \_09() are timing-dependent and likely to fail on fast servers |  Major | datanode, namenode | Matt Foley | Matt Foley |
| [HDFS-1786](https://issues.apache.org/jira/browse/HDFS-1786) | Some cli test cases expect a "null" message |  Minor | test | Tsz Wo Nicholas Sze | Uma Maheswara Rao G |
| [HDFS-1782](https://issues.apache.org/jira/browse/HDFS-1782) | FSNamesystem.startFileInternal(..) throws NullPointerException |  Major | namenode | John George | John George |
| [HDFS-1781](https://issues.apache.org/jira/browse/HDFS-1781) | jsvc executable delivered into wrong package... |  Major | scripts | John George | John George |
| [HDFS-1750](https://issues.apache.org/jira/browse/HDFS-1750) | fs -ls hftp://file not working |  Major | . | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-1728](https://issues.apache.org/jira/browse/HDFS-1728) | SecondaryNameNode.checkpointSize is in byte but not MB. |  Minor | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-1665](https://issues.apache.org/jira/browse/HDFS-1665) | Balancer sleeps inadequately |  Minor | balancer & mover | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-1625](https://issues.apache.org/jira/browse/HDFS-1625) | TestDataNodeMXBean fails if disk space usage changes during test run |  Minor | test | Todd Lipcon | Tsz Wo Nicholas Sze |
| [HDFS-1621](https://issues.apache.org/jira/browse/HDFS-1621) | Fix references to hadoop-common-${version} in build.xml |  Major | . | Todd Lipcon | Jolly Chen |
| [HDFS-1615](https://issues.apache.org/jira/browse/HDFS-1615) | seek() on closed DFS input stream throws NPE |  Major | . | Todd Lipcon | Scott Carey |
| [HDFS-1612](https://issues.apache.org/jira/browse/HDFS-1612) | HDFS Design Documentation is outdated |  Minor | documentation | Joe Crobak | Joe Crobak |
| [HDFS-1602](https://issues.apache.org/jira/browse/HDFS-1602) | NameNode storage failed replica restoration is broken |  Major | namenode | Konstantin Boudnik | Boris Shkolnik |
| [HDFS-1598](https://issues.apache.org/jira/browse/HDFS-1598) | ListPathsServlet excludes .\*.crc files |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-1597](https://issues.apache.org/jira/browse/HDFS-1597) | Batched edit log syncs can reset synctxid throw assertions |  Blocker | . | Todd Lipcon | Todd Lipcon |
| [HDFS-1591](https://issues.apache.org/jira/browse/HDFS-1591) | Fix javac, javadoc, findbugs warnings |  Major | . | Po Cheung | Po Cheung |
| [HDFS-1575](https://issues.apache.org/jira/browse/HDFS-1575) | viewing block from web UI broken |  Blocker | . | Todd Lipcon | Aaron T. Myers |
| [HDFS-1572](https://issues.apache.org/jira/browse/HDFS-1572) | Checkpointer should trigger checkpoint with specified period. |  Blocker | . | Liyin Liang | Jakob Homan |
| [HDFS-1561](https://issues.apache.org/jira/browse/HDFS-1561) | BackupNode listens on default host |  Blocker | namenode | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-1550](https://issues.apache.org/jira/browse/HDFS-1550) | NPE when listing a file with no location |  Blocker | hdfs-client | Hairong Kuang | Hairong Kuang |
| [HDFS-1544](https://issues.apache.org/jira/browse/HDFS-1544) | Ivy resolve force mode should be turned off by default |  Major | . | Luke Lu | Luke Lu |
| [HDFS-1542](https://issues.apache.org/jira/browse/HDFS-1542) | Deadlock in Configuration.writeXml when serialized form is larger than one DFS block |  Critical | hdfs-client | Todd Lipcon | Todd Lipcon |
| [HDFS-1532](https://issues.apache.org/jira/browse/HDFS-1532) | Exclude Findbugs warning in FSImageFormat$Saver |  Major | test | Todd Lipcon | Todd Lipcon |
| [HDFS-1531](https://issues.apache.org/jira/browse/HDFS-1531) | Clean up stack traces due to duplicate MXBean registration |  Trivial | datanode, namenode | Todd Lipcon | Todd Lipcon |
| [HDFS-1529](https://issues.apache.org/jira/browse/HDFS-1529) | Incorrect handling of interrupts in waitForAckedSeqno can cause deadlock |  Blocker | hdfs-client | Todd Lipcon | Todd Lipcon |
| [HDFS-1527](https://issues.apache.org/jira/browse/HDFS-1527) | SocketOutputStream.transferToFully fails for blocks \>= 2GB on 32 bit JVM |  Major | datanode | Patrick Kling | Patrick Kling |
| [HDFS-1524](https://issues.apache.org/jira/browse/HDFS-1524) | Image loader should make sure to read every byte in image file |  Blocker | namenode | Hairong Kuang | Hairong Kuang |
| [HDFS-1523](https://issues.apache.org/jira/browse/HDFS-1523) | TestLargeBlock is failing on trunk |  Major | test | Konstantin Boudnik | Konstantin Boudnik |
| [HDFS-1511](https://issues.apache.org/jira/browse/HDFS-1511) | 98 Release Audit warnings on trunk and branch-0.22 |  Blocker | . | Nigel Daley | Jakob Homan |
| [HDFS-1507](https://issues.apache.org/jira/browse/HDFS-1507) | TestAbandonBlock should abandon a block |  Minor | test | Eli Collins | Eli Collins |
| [HDFS-1505](https://issues.apache.org/jira/browse/HDFS-1505) | saveNamespace appears to succeed even if all directories fail to save |  Blocker | . | Todd Lipcon | Aaron T. Myers |
| [HDFS-1504](https://issues.apache.org/jira/browse/HDFS-1504) | FSImageSaver should catch all exceptions, not just IOE |  Minor | namenode | Todd Lipcon | Todd Lipcon |
| [HDFS-1503](https://issues.apache.org/jira/browse/HDFS-1503) | TestSaveNamespace fails |  Minor | test | Eli Collins | Todd Lipcon |
| [HDFS-1502](https://issues.apache.org/jira/browse/HDFS-1502) | TestBlockRecovery triggers NPE in assert |  Minor | . | Eli Collins | Hairong Kuang |
| [HDFS-1500](https://issues.apache.org/jira/browse/HDFS-1500) | TestOfflineImageViewer failing on trunk |  Major | test, tools | Todd Lipcon | Todd Lipcon |
| [HDFS-1498](https://issues.apache.org/jira/browse/HDFS-1498) | FSDirectory#unprotectedConcat calls setModificationTime on a file |  Minor | namenode | Eli Collins | Eli Collins |
| [HDFS-1487](https://issues.apache.org/jira/browse/HDFS-1487) | FSDirectory.removeBlock() should update diskspace count of the block owner node |  Major | namenode | Zhong Wang | Zhong Wang |
| [HDFS-1483](https://issues.apache.org/jira/browse/HDFS-1483) | DFSClient.getBlockLocations returns BlockLocations with no indication that the corresponding blocks are corrupt |  Major | hdfs-client | Patrick Kling | Patrick Kling |
| [HDFS-1467](https://issues.apache.org/jira/browse/HDFS-1467) | Append pipeline never succeeds with more than one replica |  Blocker | datanode | Todd Lipcon | Todd Lipcon |
| [HDFS-1466](https://issues.apache.org/jira/browse/HDFS-1466) | TestFcHdfsSymlink relies on /tmp/test not existing |  Minor | test | Todd Lipcon | Eli Collins |
| [HDFS-1440](https://issues.apache.org/jira/browse/HDFS-1440) | TestComputeInvalidateWork fails intermittently |  Major | test | Suresh Srinivas | Suresh Srinivas |
| [HDFS-1433](https://issues.apache.org/jira/browse/HDFS-1433) | Fix test failures - TestPread and TestFileLimit |  Major | test | Suresh Srinivas | Suresh Srinivas |
| [HDFS-1409](https://issues.apache.org/jira/browse/HDFS-1409) | The "register" method of the BackupNode class should be "UnsupportedActionException("register")" |  Trivial | namenode | Ching-Shen Chen | Ching-Shen Chen |
| [HDFS-1399](https://issues.apache.org/jira/browse/HDFS-1399) | Distinct minicluster services (e.g. NN and JT) overwrite each other's service policies |  Major | security | Aaron T. Myers | Aaron T. Myers |
| [HDFS-1377](https://issues.apache.org/jira/browse/HDFS-1377) | Quota bug for partial blocks allows quotas to be violated |  Blocker | namenode | Eli Collins | Eli Collins |
| [HDFS-1364](https://issues.apache.org/jira/browse/HDFS-1364) | HFTP client should support relogin from keytab |  Major | security | Kan Zhang | Jitendra Nath Pandey |
| [HDFS-1361](https://issues.apache.org/jira/browse/HDFS-1361) | Add -fileStatus operation to NNThroughputBenchmark |  Major | . | Konstantin Shvachko | Konstantin Shvachko |
| [HDFS-1357](https://issues.apache.org/jira/browse/HDFS-1357) | HFTP traffic served by DataNode shouldn't use service port on NameNode |  Major | datanode, security | Kan Zhang | Kan Zhang |
| [HDFS-1355](https://issues.apache.org/jira/browse/HDFS-1355) | ant veryclean (clean-cache) doesn't clean enough |  Major | build | Luke Lu | Luke Lu |
| [HDFS-1352](https://issues.apache.org/jira/browse/HDFS-1352) | Fix jsvc.location |  Major | build | Eli Collins | Eli Collins |
| [HDFS-1349](https://issues.apache.org/jira/browse/HDFS-1349) | Remove empty java files |  Major | . | Tom White | Eli Collins |
| [HDFS-1347](https://issues.apache.org/jira/browse/HDFS-1347) | TestDelegationToken uses mortbay.log for logging |  Major | test | Boris Shkolnik | Boris Shkolnik |
| [HDFS-1340](https://issues.apache.org/jira/browse/HDFS-1340) | A null delegation token is appended to the url if security is disabled when browsing filesystem. |  Major | . | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [HDFS-1334](https://issues.apache.org/jira/browse/HDFS-1334) | open in HftpFileSystem does not add delegation tokens to the url. |  Major | . | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [HDFS-1317](https://issues.apache.org/jira/browse/HDFS-1317) | HDFSProxy needs additional changes to work after changes to streamFile servlet in HDFS-1109 |  Major | contrib/hdfsproxy | Rohini Palaniswamy | Rohini Palaniswamy |
| [HDFS-1308](https://issues.apache.org/jira/browse/HDFS-1308) |  job conf key for the services name of DelegationToken for HFTP url is constructed incorrectly in HFTPFileSystem (part of MR-1718) |  Major | . | Boris Shkolnik | Boris Shkolnik |
| [HDFS-1301](https://issues.apache.org/jira/browse/HDFS-1301) | TestHDFSProxy need to use server side conf for ProxyUser stuff. |  Major | . | Boris Shkolnik | Boris Shkolnik |
| [HDFS-1296](https://issues.apache.org/jira/browse/HDFS-1296) | using delegation token over hftp for long running clients (spawn from hdfs-1007). |  Major | . | Boris Shkolnik | Boris Shkolnik |
| [HDFS-1289](https://issues.apache.org/jira/browse/HDFS-1289) | Datanode secure mode is broken |  Major | datanode | Kan Zhang | Kan Zhang |
| [HDFS-1284](https://issues.apache.org/jira/browse/HDFS-1284) | TestBlockToken fails |  Major | test | Konstantin Shvachko | Kan Zhang |
| [HDFS-1283](https://issues.apache.org/jira/browse/HDFS-1283) | ant eclipse-files has drifted again |  Major | build | Jakob Homan | Jakob Homan |
| [HDFS-1258](https://issues.apache.org/jira/browse/HDFS-1258) | Clearing namespace quota on "/" corrupts FS image |  Blocker | namenode | Aaron T. Myers | Aaron T. Myers |
| [HDFS-1250](https://issues.apache.org/jira/browse/HDFS-1250) | Namenode accepts block report from dead datanodes |  Major | namenode | Suresh Srinivas | Suresh Srinivas |
| [HDFS-1206](https://issues.apache.org/jira/browse/HDFS-1206) | TestFiHFlush fails intermittently |  Major | test | Tsz Wo Nicholas Sze | Konstantin Boudnik |
| [HDFS-1202](https://issues.apache.org/jira/browse/HDFS-1202) | DataBlockScanner throws NPE when updated before initialized |  Major | datanode | Todd Lipcon | Todd Lipcon |
| [HDFS-1198](https://issues.apache.org/jira/browse/HDFS-1198) | Resolving cross-realm principals |  Major | namenode | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [HDFS-1192](https://issues.apache.org/jira/browse/HDFS-1192) | refreshSuperUserGroupsConfiguration should use server side configuration for the refresh (for HADOOP-6815) |  Major | . | Boris Shkolnik | Boris Shkolnik |
| [HDFS-1189](https://issues.apache.org/jira/browse/HDFS-1189) | Quota counts missed between clear quota and set quota |  Major | namenode | Kang Xiao | John George |
| [HDFS-1164](https://issues.apache.org/jira/browse/HDFS-1164) | TestHdfsProxy is failing |  Major | contrib/hdfsproxy | Eli Collins | Todd Lipcon |
| [HDFS-1163](https://issues.apache.org/jira/browse/HDFS-1163) | normalize property names for JT/NN kerberos principal names in configuration (from HADOOP 6633) |  Major | . | Boris Shkolnik | Boris Shkolnik |
| [HDFS-1157](https://issues.apache.org/jira/browse/HDFS-1157) | Modifications introduced by HDFS-1150 are breaking aspect's bindings |  Major | test | Konstantin Boudnik | Konstantin Boudnik |
| [HDFS-1146](https://issues.apache.org/jira/browse/HDFS-1146) | Javadoc for getDelegationTokenSecretManager in FSNamesystem |  Major | . | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [HDFS-1145](https://issues.apache.org/jira/browse/HDFS-1145) | When NameNode is shutdown it tries to exit safemode |  Major | namenode | dhruba borthakur | dhruba borthakur |
| [HDFS-1141](https://issues.apache.org/jira/browse/HDFS-1141) | completeFile does not check lease ownership |  Blocker | namenode | Todd Lipcon | Todd Lipcon |
| [HDFS-1138](https://issues.apache.org/jira/browse/HDFS-1138) | Modification times are being overwritten when FSImage loads |  Major | . | Dmytro Molkov | Dmytro Molkov |
| [HDFS-1130](https://issues.apache.org/jira/browse/HDFS-1130) | Pass Administrator acl to HTTPServer for common servlet access. |  Major | security | Amareshwari Sriramadasu | Devaraj Das |
| [HDFS-1118](https://issues.apache.org/jira/browse/HDFS-1118) | DFSOutputStream socket leak when cannot connect to DataNode |  Major | . | Zheng Shao | Zheng Shao |
| [HDFS-1112](https://issues.apache.org/jira/browse/HDFS-1112) | Edit log buffer should not grow unboundedly |  Major | namenode | Hairong Kuang | Hairong Kuang |
| [HDFS-1085](https://issues.apache.org/jira/browse/HDFS-1085) | hftp read  failing silently |  Major | datanode | Koji Noguchi | Tsz Wo Nicholas Sze |
| [HDFS-1045](https://issues.apache.org/jira/browse/HDFS-1045) | In secure clusters, re-login is necessary for https clients before opening connections |  Major | security | Jakob Homan | Jakob Homan |
| [HDFS-1044](https://issues.apache.org/jira/browse/HDFS-1044) | Cannot submit mapreduce job from secure client to unsecure sever |  Major | . | Boris Shkolnik | Boris Shkolnik |
| [HDFS-1039](https://issues.apache.org/jira/browse/HDFS-1039) | Service should be set in the token in JspHelper.getUGI |  Major | . | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [HDFS-1038](https://issues.apache.org/jira/browse/HDFS-1038) | In nn\_browsedfscontent.jsp fetch delegation token only if security is enabled. |  Major | . | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [HDFS-1036](https://issues.apache.org/jira/browse/HDFS-1036) | in DelegationTokenFetch dfs.getURI returns no port |  Major | . | Boris Shkolnik | Boris Shkolnik |
| [HDFS-1027](https://issues.apache.org/jira/browse/HDFS-1027) | Update  year to 2010. |  Trivial | . | Ravi Phulari | Ravi Phulari |
| [HDFS-1021](https://issues.apache.org/jira/browse/HDFS-1021) | specify correct server principal for RefreshAuthorizationPolicyProtocol and RefreshUserToGroupMappingsProtocol protocols in DFSAdmin (for HADOOP-6612) |  Major | security | Boris Shkolnik | Boris Shkolnik |
| [HDFS-1019](https://issues.apache.org/jira/browse/HDFS-1019) | Incorrect default values for delegation tokens in hdfs-default.xml |  Major | . | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [HDFS-1017](https://issues.apache.org/jira/browse/HDFS-1017) | browsedfs jsp should call JspHelper.getUGI rather than using createRemoteUser() |  Major | security | Jakob Homan | Jakob Homan |
| [HDFS-1007](https://issues.apache.org/jira/browse/HDFS-1007) | HFTP needs to be updated to use delegation tokens |  Major | security | Devaraj Das | Devaraj Das |
| [HDFS-1006](https://issues.apache.org/jira/browse/HDFS-1006) | getImage/putImage http requests should be https for the case of security enabled. |  Major | . | Boris Shkolnik | Boris Shkolnik |
| [HDFS-1001](https://issues.apache.org/jira/browse/HDFS-1001) | DataXceiver and BlockReader disagree on when to send/recv CHECKSUM\_OK |  Minor | datanode | bc Wong | bc Wong |
| [HDFS-988](https://issues.apache.org/jira/browse/HDFS-988) | saveNamespace race can corrupt the edits log |  Blocker | namenode | dhruba borthakur | Eli Collins |
| [HDFS-977](https://issues.apache.org/jira/browse/HDFS-977) | DataNode.createInterDataNodeProtocolProxy() guards a log at the wrong level |  Trivial | datanode | Steve Loughran | Harsh J |
| [HDFS-970](https://issues.apache.org/jira/browse/HDFS-970) | FSImage writing should always fsync before close |  Critical | namenode | Todd Lipcon | Todd Lipcon |
| [HDFS-939](https://issues.apache.org/jira/browse/HDFS-939) | libhdfs test is broken |  Blocker | libhdfs | Eli Collins | Eli Collins |
| [HDFS-912](https://issues.apache.org/jira/browse/HDFS-912) |  sed in build.xml fails |  Minor | . | Allen Wittenauer | Allen Wittenauer |
| [HDFS-900](https://issues.apache.org/jira/browse/HDFS-900) | Corrupt replicas are not tracked correctly through block report from DN |  Blocker | . | Todd Lipcon | Konstantin Shvachko |
| [HDFS-874](https://issues.apache.org/jira/browse/HDFS-874) | TestHDFSFileContextMainOperations fails on weirdly configured DNS hosts |  Major | hdfs-client, test | Todd Lipcon | Todd Lipcon |
| [HDFS-829](https://issues.apache.org/jira/browse/HDFS-829) | hdfsJniHelper.c: #include \<error.h\> is not portable |  Major | . | Allen Wittenauer | Allen Wittenauer |
| [HDFS-727](https://issues.apache.org/jira/browse/HDFS-727) | bug setting block size hdfsOpenFile |  Blocker | libhdfs | Eli Collins | Eli Collins |
| [HDFS-671](https://issues.apache.org/jira/browse/HDFS-671) | Documentation change for updated configuration keys. |  Blocker | . | Jitendra Nath Pandey | Tom White |
| [HDFS-613](https://issues.apache.org/jira/browse/HDFS-613) | TestBalancer and TestBlockTokenWithDFS fail Balancer assert |  Major | test | Konstantin Shvachko | Todd Lipcon |
| [HDFS-96](https://issues.apache.org/jira/browse/HDFS-96) | HDFS does not support blocks greater than 2GB |  Major | . | dhruba borthakur | Patrick Kling |
| [MAPREDUCE-3438](https://issues.apache.org/jira/browse/MAPREDUCE-3438) | TestRaidNode fails because of "Too many open files" |  Major | contrib/raid | Konstantin Shvachko | Ramkumar Vadali |
| [MAPREDUCE-3429](https://issues.apache.org/jira/browse/MAPREDUCE-3429) | Few contrib tests are failing because of the missing commons-lang dependency |  Major | capacity-sched, contrib/gridmix | Konstantin Boudnik | Konstantin Boudnik |
| [MAPREDUCE-3151](https://issues.apache.org/jira/browse/MAPREDUCE-3151) | Contrib tests failing |  Major | contrib/vertica | Joep Rottinghuis | Joep Rottinghuis |
| [MAPREDUCE-3139](https://issues.apache.org/jira/browse/MAPREDUCE-3139) | SlivePartitioner generates negative partitions |  Major | test | Konstantin Shvachko | Jakob Homan |
| [MAPREDUCE-3138](https://issues.apache.org/jira/browse/MAPREDUCE-3138) | Allow for applications to deal with MAPREDUCE-954 |  Blocker | client, mrv2 | Arun C Murthy | Owen O'Malley |
| [MAPREDUCE-3088](https://issues.apache.org/jira/browse/MAPREDUCE-3088) | Clover 2.4.3 breaks build for 0.22 branch |  Major | build | Konstantin Shvachko | Konstantin Shvachko |
| [MAPREDUCE-3039](https://issues.apache.org/jira/browse/MAPREDUCE-3039) | Make mapreduce use same version of avro as HBase |  Major | capacity-sched, contrib/fair-share, contrib/gridmix, contrib/mrunit, contrib/mumak, contrib/raid, contrib/streaming, jobhistoryserver | Joep Rottinghuis | Joep Rottinghuis |
| [MAPREDUCE-3026](https://issues.apache.org/jira/browse/MAPREDUCE-3026) | When user adds hierarchical queues to the cluster, mapred queue -list returns NULL Pointer Exception |  Major | jobtracker | Mayank Bansal | Mayank Bansal |
| [MAPREDUCE-3025](https://issues.apache.org/jira/browse/MAPREDUCE-3025) | Contribs not building |  Blocker | build | Joep Rottinghuis | Joep Rottinghuis |
| [MAPREDUCE-2991](https://issues.apache.org/jira/browse/MAPREDUCE-2991) | queueinfo.jsp fails to show queue status if any Capacity scheduler queue name has dash/hiphen in it. |  Major | scheduler | Priyo Mustafi | Priyo Mustafi |
| [MAPREDUCE-2948](https://issues.apache.org/jira/browse/MAPREDUCE-2948) | Hadoop streaming test failure, post MR-2767 |  Major | contrib/streaming | Milind Bhandarkar | Mahadev konar |
| [MAPREDUCE-2940](https://issues.apache.org/jira/browse/MAPREDUCE-2940) | Build fails with ant 1.7.0 but works with 1.8.0 |  Major | build | Joep Rottinghuis | Joep Rottinghuis |
| [MAPREDUCE-2936](https://issues.apache.org/jira/browse/MAPREDUCE-2936) | Contrib Raid compilation broken after HDFS-1620 |  Major | . | Vinod Kumar Vavilapalli | Vinod Kumar Vavilapalli |
| [MAPREDUCE-2779](https://issues.apache.org/jira/browse/MAPREDUCE-2779) | JobSplitWriter.java can't handle large job.split file |  Major | job submission | Ming Ma | Ming Ma |
| [MAPREDUCE-2767](https://issues.apache.org/jira/browse/MAPREDUCE-2767) | Remove Linux task-controller from 0.22 branch |  Blocker | security | Milind Bhandarkar | Milind Bhandarkar |
| [MAPREDUCE-2753](https://issues.apache.org/jira/browse/MAPREDUCE-2753) | Generated POMs hardcode dependency on hadoop-common version 0.22.0-SNAPSHOT |  Major | build | Joep Rottinghuis | Joep Rottinghuis |
| [MAPREDUCE-2752](https://issues.apache.org/jira/browse/MAPREDUCE-2752) | Build does not pass along properties to contrib builds |  Minor | build | Joep Rottinghuis | Joep Rottinghuis |
| [MAPREDUCE-2571](https://issues.apache.org/jira/browse/MAPREDUCE-2571) | CombineFileInputFormat.getSplits throws a java.lang.ArrayStoreException |  Blocker | . | Bochun Bai | Bochun Bai |
| [MAPREDUCE-2531](https://issues.apache.org/jira/browse/MAPREDUCE-2531) | org.apache.hadoop.mapred.jobcontrol.getAssignedJobID throw class cast exception |  Blocker | client | Robert Joseph Evans | Robert Joseph Evans |
| [MAPREDUCE-2516](https://issues.apache.org/jira/browse/MAPREDUCE-2516) | option to control sensitive web actions |  Minor | . | Ari Rabkin | Ari Rabkin |
| [MAPREDUCE-2515](https://issues.apache.org/jira/browse/MAPREDUCE-2515) | MapReduce references obsolete options |  Major | jobtracker | Ari Rabkin | Ari Rabkin |
| [MAPREDUCE-2487](https://issues.apache.org/jira/browse/MAPREDUCE-2487) | ChainReducer uses MAPPER\_BY\_VALUE instead of REDUCER\_BY\_VALUE |  Minor | . | Forrest Vines | Devaraj K |
| [MAPREDUCE-2486](https://issues.apache.org/jira/browse/MAPREDUCE-2486) | 0.22 - snapshot incorrect dependency published in .pom files |  Blocker | . | Dmitriy V. Ryaboy | Todd Lipcon |
| [MAPREDUCE-2472](https://issues.apache.org/jira/browse/MAPREDUCE-2472) | Extra whitespace in mapred.child.java.opts breaks JVM initialization |  Major | task-controller | Todd Lipcon | Aaron T. Myers |
| [MAPREDUCE-2457](https://issues.apache.org/jira/browse/MAPREDUCE-2457) | job submission should inject group.name (on the JT side) |  Critical | jobtracker | Alejandro Abdelnur | Alejandro Abdelnur |
| [MAPREDUCE-2448](https://issues.apache.org/jira/browse/MAPREDUCE-2448) | NoSuchMethodError: org.apache.hadoop.hdfs.TestDatanodeBlockScanner.corruptReplica(..) |  Minor | contrib/raid, test | Tsz Wo Nicholas Sze | Eli Collins |
| [MAPREDUCE-2445](https://issues.apache.org/jira/browse/MAPREDUCE-2445) | TestMiniMRWithDFSWithDistinctUsers is very broken |  Major | security, test | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2437](https://issues.apache.org/jira/browse/MAPREDUCE-2437) | SLive should process only part\* files while generating the report. |  Blocker | test | Konstantin Shvachko | Konstantin Shvachko |
| [MAPREDUCE-2428](https://issues.apache.org/jira/browse/MAPREDUCE-2428) | start-mapred.sh script fails if HADOOP\_HOME is not set |  Blocker | . | Tom White | Tom White |
| [MAPREDUCE-2394](https://issues.apache.org/jira/browse/MAPREDUCE-2394) | JUnit output format doesn't propagate into some contrib builds |  Blocker | . | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2392](https://issues.apache.org/jira/browse/MAPREDUCE-2392) | TaskTracker shutdown in the tests sometimes take 60s |  Major | . | Tom White | Tom White |
| [MAPREDUCE-2336](https://issues.apache.org/jira/browse/MAPREDUCE-2336) | Tool-related packages should be in the Tool javadoc group |  Major | documentation | Tom White | Tom White |
| [MAPREDUCE-2327](https://issues.apache.org/jira/browse/MAPREDUCE-2327) | MapTask doesn't need to put username information in SpillRecord |  Blocker | . | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2317](https://issues.apache.org/jira/browse/MAPREDUCE-2317) | HadoopArchives throwing NullPointerException while creating hadoop archives (.har files) |  Minor | harchive | Devaraj K | Devaraj K |
| [MAPREDUCE-2315](https://issues.apache.org/jira/browse/MAPREDUCE-2315) | javadoc is failing in nightly |  Blocker | . | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2311](https://issues.apache.org/jira/browse/MAPREDUCE-2311) | TestFairScheduler failing on trunk |  Blocker | contrib/fair-share | Todd Lipcon | Scott Chen |
| [MAPREDUCE-2304](https://issues.apache.org/jira/browse/MAPREDUCE-2304) | TestMRCLI fails when hostname has a hyphen (-) |  Minor | test | Priyo Mustafi | Priyo Mustafi |
| [MAPREDUCE-2285](https://issues.apache.org/jira/browse/MAPREDUCE-2285) | MiniMRCluster does not start after ant test-patch |  Blocker | test | Ramkumar Vadali | Todd Lipcon |
| [MAPREDUCE-2284](https://issues.apache.org/jira/browse/MAPREDUCE-2284) | TestLocalRunner.testMultiMaps times out |  Critical | test | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2283](https://issues.apache.org/jira/browse/MAPREDUCE-2283) | TestBlockFixer hangs initializing MiniMRCluster |  Blocker | contrib/raid | Nigel Daley | Ramkumar Vadali |
| [MAPREDUCE-2282](https://issues.apache.org/jira/browse/MAPREDUCE-2282) | MapReduce tests don't compile following HDFS-1561 |  Blocker | test | Tom White | Konstantin Shvachko |
| [MAPREDUCE-2281](https://issues.apache.org/jira/browse/MAPREDUCE-2281) | Fix javac, javadoc, findbugs warnings |  Major | . | Po Cheung | Po Cheung |
| [MAPREDUCE-2277](https://issues.apache.org/jira/browse/MAPREDUCE-2277) | TestCapacitySchedulerWithJobTracker fails sometimes |  Minor | capacity-sched | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2272](https://issues.apache.org/jira/browse/MAPREDUCE-2272) | Job ACL file should not be executable |  Trivial | tasktracker | Todd Lipcon | Harsh J |
| [MAPREDUCE-2256](https://issues.apache.org/jira/browse/MAPREDUCE-2256) | FairScheduler fairshare preemption from multiple pools may preempt all tasks from one pool causing that pool to go below fairshare. |  Major | contrib/fair-share | Priyo Mustafi | Priyo Mustafi |
| [MAPREDUCE-2253](https://issues.apache.org/jira/browse/MAPREDUCE-2253) | Servlets should specify content type |  Critical | . | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2251](https://issues.apache.org/jira/browse/MAPREDUCE-2251) | Remove mapreduce.job.userhistorylocation config |  Major | . | Todd Lipcon | Harsh J |
| [MAPREDUCE-2238](https://issues.apache.org/jira/browse/MAPREDUCE-2238) | Undeletable build directories |  Critical | build, test | Eli Collins | Todd Lipcon |
| [MAPREDUCE-2224](https://issues.apache.org/jira/browse/MAPREDUCE-2224) | Synchronization bugs in JvmManager |  Critical | tasktracker | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2222](https://issues.apache.org/jira/browse/MAPREDUCE-2222) | Ivy resolve force mode should be turned off by default |  Major | . | Luke Lu | Luke Lu |
| [MAPREDUCE-2219](https://issues.apache.org/jira/browse/MAPREDUCE-2219) | JT should not try to remove mapred.system.dir during startup |  Major | jobtracker | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2200](https://issues.apache.org/jira/browse/MAPREDUCE-2200) | TestUmbilicalProtocolWithJobToken is failing without Krb evironment: needs to be conditional |  Major | test | Konstantin Boudnik | Konstantin Boudnik |
| [MAPREDUCE-2195](https://issues.apache.org/jira/browse/MAPREDUCE-2195) | New property for local conf directory in system-test-mapreduce.xml file. |  Major | test | Konstantin Boudnik | Konstantin Boudnik |
| [MAPREDUCE-2188](https://issues.apache.org/jira/browse/MAPREDUCE-2188) | The new API MultithreadedMapper doesn't call the initialize method of the RecordReader |  Major | . | Owen O'Malley | Owen O'Malley |
| [MAPREDUCE-2179](https://issues.apache.org/jira/browse/MAPREDUCE-2179) | RaidBlockSender.java compilation fails |  Blocker | contrib/raid | Giridharan Kesavan | Ramkumar Vadali |
| [MAPREDUCE-2173](https://issues.apache.org/jira/browse/MAPREDUCE-2173) | Race condition in TestBlockFixer causes intermittent failure |  Major | . | Patrick Kling | Patrick Kling |
| [MAPREDUCE-2146](https://issues.apache.org/jira/browse/MAPREDUCE-2146) | Raid should not affect access time of a source file |  Minor | contrib/raid | Ramkumar Vadali | Ramkumar Vadali |
| [MAPREDUCE-2143](https://issues.apache.org/jira/browse/MAPREDUCE-2143) | HarFileSystem is not able to handle spaces in its path |  Major | harchive | Ramkumar Vadali | Ramkumar Vadali |
| [MAPREDUCE-2127](https://issues.apache.org/jira/browse/MAPREDUCE-2127) | mapreduce trunk builds are failing on hudson |  Major | build, pipes | Giridharan Kesavan | Bruno Mahé |
| [MAPREDUCE-2099](https://issues.apache.org/jira/browse/MAPREDUCE-2099) | RaidNode should recreate outdated parity HARs |  Major | contrib/raid | Ramkumar Vadali | Ramkumar Vadali |
| [MAPREDUCE-2096](https://issues.apache.org/jira/browse/MAPREDUCE-2096) | Secure local filesystem IO from symlink vulnerabilities |  Blocker | jobtracker, security, tasktracker | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2095](https://issues.apache.org/jira/browse/MAPREDUCE-2095) | Gridmix unable to run for compressed traces(.gz format). |  Major | contrib/gridmix | Vinay Kumar Thota | Ranjit Mathew |
| [MAPREDUCE-2084](https://issues.apache.org/jira/browse/MAPREDUCE-2084) | Deprecated org.apache.hadoop.util package in MapReduce produces deprecations in Common classes in Eclipse |  Blocker | documentation | Tom White | Tom White |
| [MAPREDUCE-2082](https://issues.apache.org/jira/browse/MAPREDUCE-2082) | Race condition in writing the jobtoken password file when launching pipes jobs |  Major | . | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [MAPREDUCE-2078](https://issues.apache.org/jira/browse/MAPREDUCE-2078) | TraceBuilder unable to generate the traces while giving the job history path by globing. |  Major | tools/rumen | Vinay Kumar Thota | Amar Kamat |
| [MAPREDUCE-2077](https://issues.apache.org/jira/browse/MAPREDUCE-2077) | Name clash in the deprecated o.a.h.util.MemoryCalculatorPlugin |  Major | . | Luke Lu | Luke Lu |
| [MAPREDUCE-2067](https://issues.apache.org/jira/browse/MAPREDUCE-2067) | Distinct minicluster services (e.g. NN and JT) overwrite each other's service policies |  Major | security | Aaron T. Myers | Aaron T. Myers |
| [MAPREDUCE-2059](https://issues.apache.org/jira/browse/MAPREDUCE-2059) | RecoveryManager attempts to add jobtracker.info |  Major | jobtracker | Dan Adkins | Subroto Sanyal |
| [MAPREDUCE-2054](https://issues.apache.org/jira/browse/MAPREDUCE-2054) | Hierarchical queue implementation broke dynamic queue addition in Dynamic Scheduler |  Major | contrib/dynamic-scheduler | Thomas Sandholm | Thomas Sandholm |
| [MAPREDUCE-2032](https://issues.apache.org/jira/browse/MAPREDUCE-2032) | TestJobOutputCommitter fails in ant test run |  Major | task | Amareshwari Sriramadasu | Dick King |
| [MAPREDUCE-2031](https://issues.apache.org/jira/browse/MAPREDUCE-2031) | TestTaskLauncher and TestTaskTrackerLocalization fail with NPE in trunk. |  Major | tasktracker, test | Amareshwari Sriramadasu | Ravi Gummadi |
| [MAPREDUCE-2029](https://issues.apache.org/jira/browse/MAPREDUCE-2029) | DistributedRaidFileSystem not removed from cache on close() |  Major | contrib/raid | Paul Yang | Ramkumar Vadali |
| [MAPREDUCE-2023](https://issues.apache.org/jira/browse/MAPREDUCE-2023) | TestDFSIO read test may not read specified bytes. |  Major | benchmarks | Hong Tang | Hong Tang |
| [MAPREDUCE-2022](https://issues.apache.org/jira/browse/MAPREDUCE-2022) | trunk build broken |  Blocker | test | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [MAPREDUCE-2021](https://issues.apache.org/jira/browse/MAPREDUCE-2021) | CombineFileInputFormat returns duplicate  hostnames in split locations |  Major | client | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [MAPREDUCE-2000](https://issues.apache.org/jira/browse/MAPREDUCE-2000) | Rumen is not able to extract counters for Job history logs from Hadoop 0.20 |  Major | tools/rumen | Hong Tang | Hong Tang |
| [MAPREDUCE-1999](https://issues.apache.org/jira/browse/MAPREDUCE-1999) | ClientProtocol incorrectly uses hdfs DelegationTokenSelector |  Major | . | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [MAPREDUCE-1996](https://issues.apache.org/jira/browse/MAPREDUCE-1996) | API: Reducer.reduce() method detail misstatement |  Trivial | documentation | Glynn Durham | Harsh J |
| [MAPREDUCE-1993](https://issues.apache.org/jira/browse/MAPREDUCE-1993) | TestTrackerDistributedCacheManagerWithLinuxTaskController fails on trunk |  Major | . | Devaraj Das | Devaraj Das |
| [MAPREDUCE-1992](https://issues.apache.org/jira/browse/MAPREDUCE-1992) | NPE in JobTracker's constructor |  Major | jobtracker | Ravi Gummadi | Kan Zhang |
| [MAPREDUCE-1989](https://issues.apache.org/jira/browse/MAPREDUCE-1989) | Gridmix3 doesn't emit out proper mesage when user resolver is set and no user list is given |  Major | contrib/gridmix | Ravi Gummadi | Ravi Gummadi |
| [MAPREDUCE-1982](https://issues.apache.org/jira/browse/MAPREDUCE-1982) | [Rumen] TraceBuilder's output shows jobname as NULL for jobhistory files with valid jobnames |  Major | tools/rumen | Amar Kamat | Ravi Gummadi |
| [MAPREDUCE-1979](https://issues.apache.org/jira/browse/MAPREDUCE-1979) | "Output directory already exists" error in gridmix when gridmix.output.directory is not defined |  Major | contrib/gridmix | Ravi Gummadi | Ravi Gummadi |
| [MAPREDUCE-1975](https://issues.apache.org/jira/browse/MAPREDUCE-1975) | gridmix shows unnecessary InterruptedException |  Major | contrib/gridmix | Ravi Gummadi | Ravi Gummadi |
| [MAPREDUCE-1974](https://issues.apache.org/jira/browse/MAPREDUCE-1974) | FairScheduler can preempt the same task many times |  Major | contrib/fair-share | Scott Chen | Scott Chen |
| [MAPREDUCE-1961](https://issues.apache.org/jira/browse/MAPREDUCE-1961) | [gridmix3] ConcurrentModificationException when shutting down Gridmix |  Major | contrib/gridmix | Hong Tang | Hong Tang |
| [MAPREDUCE-1958](https://issues.apache.org/jira/browse/MAPREDUCE-1958) | using delegation token over hftp for long running clients (part of hdfs 1296) |  Major | . | Boris Shkolnik | Boris Shkolnik |
| [MAPREDUCE-1925](https://issues.apache.org/jira/browse/MAPREDUCE-1925) | TestRumenJobTraces fails in trunk |  Major | tools/rumen | Amareshwari Sriramadasu | Ravi Gummadi |
| [MAPREDUCE-1915](https://issues.apache.org/jira/browse/MAPREDUCE-1915) | IndexCache - getIndexInformation - check reduce index Out Of Bounds |  Trivial | tasktracker | Rares Vernica | Priyo Mustafi |
| [MAPREDUCE-1911](https://issues.apache.org/jira/browse/MAPREDUCE-1911) | Fix errors in -info option in streaming |  Major | contrib/streaming | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [MAPREDUCE-1908](https://issues.apache.org/jira/browse/MAPREDUCE-1908) | DistributedRaidFileSystem does not handle ChecksumException correctly |  Major | contrib/raid | Ramkumar Vadali | Ramkumar Vadali |
| [MAPREDUCE-1900](https://issues.apache.org/jira/browse/MAPREDUCE-1900) | MapReduce daemons should close FileSystems that are not needed anymore |  Major | jobtracker, tasktracker | Devaraj Das | Kan Zhang |
| [MAPREDUCE-1894](https://issues.apache.org/jira/browse/MAPREDUCE-1894) | DistributedRaidFileSystem.readFully() does not return |  Major | contrib/raid | Ramkumar Vadali | Ramkumar Vadali |
| [MAPREDUCE-1888](https://issues.apache.org/jira/browse/MAPREDUCE-1888) | Streaming overrides user given output key and value types. |  Major | contrib/streaming | Amareshwari Sriramadasu | Ravi Gummadi |
| [MAPREDUCE-1887](https://issues.apache.org/jira/browse/MAPREDUCE-1887) | MRAsyncDiskService does not properly absolutize volume root paths |  Major | . | Aaron Kimball | Aaron Kimball |
| [MAPREDUCE-1880](https://issues.apache.org/jira/browse/MAPREDUCE-1880) | "java.lang.ArithmeticException: Non-terminating decimal expansion; no exact representable decimal result." while running "hadoop jar hadoop-0.20.1+169.89-examples.jar pi 4 30" |  Minor | examples | Victor Pakhomov | Tsz Wo Nicholas Sze |
| [MAPREDUCE-1867](https://issues.apache.org/jira/browse/MAPREDUCE-1867) | Remove unused methods in org.apache.hadoop.streaming.StreamUtil |  Minor | contrib/streaming | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [MAPREDUCE-1865](https://issues.apache.org/jira/browse/MAPREDUCE-1865) | [Rumen] Rumen should also support jobhistory files generated using trunk |  Major | tools/rumen | Amar Kamat | Amar Kamat |
| [MAPREDUCE-1864](https://issues.apache.org/jira/browse/MAPREDUCE-1864) | PipeMapRed.java has uninitialized members log\_ and LOGNAME |  Major | contrib/streaming | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [MAPREDUCE-1863](https://issues.apache.org/jira/browse/MAPREDUCE-1863) | [Rumen] Null failedMapAttemptCDFs in job traces generated by Rumen |  Major | tools/rumen | Amar Kamat | Amar Kamat |
| [MAPREDUCE-1857](https://issues.apache.org/jira/browse/MAPREDUCE-1857) | Remove unused streaming configuration from src |  Trivial | contrib/streaming | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [MAPREDUCE-1853](https://issues.apache.org/jira/browse/MAPREDUCE-1853) | MultipleOutputs does not cache TaskAttemptContext |  Critical | task | Torsten Curdt | Torsten Curdt |
| [MAPREDUCE-1845](https://issues.apache.org/jira/browse/MAPREDUCE-1845) | FairScheduler.tasksToPeempt() can return negative number |  Major | contrib/fair-share | Scott Chen | Scott Chen |
| [MAPREDUCE-1836](https://issues.apache.org/jira/browse/MAPREDUCE-1836) | Refresh for proxy superuser config (mr part for HDFS-1096) |  Major | . | Boris Shkolnik | Boris Shkolnik |
| [MAPREDUCE-1834](https://issues.apache.org/jira/browse/MAPREDUCE-1834) | TestSimulatorDeterministicReplay timesout on trunk |  Major | contrib/mumak | Amareshwari Sriramadasu | Hong Tang |
| [MAPREDUCE-1825](https://issues.apache.org/jira/browse/MAPREDUCE-1825) | jobqueue\_details.jsp and FairSchedulerServelet should not call finishedMaps and finishedReduces when job is not initialized |  Major | jobtracker | Amareshwari Sriramadasu | Scott Chen |
| [MAPREDUCE-1820](https://issues.apache.org/jira/browse/MAPREDUCE-1820) | InputSampler does not create a deep copy of the key object when creating a sample, which causes problems with some formats like SequenceFile\<Text,Text\> |  Major | . | Alex Kozlov | Alex Kozlov |
| [MAPREDUCE-1813](https://issues.apache.org/jira/browse/MAPREDUCE-1813) | NPE in PipeMapred.MRErrorThread |  Major | contrib/streaming | Amareshwari Sriramadasu | Ravi Gummadi |
| [MAPREDUCE-1784](https://issues.apache.org/jira/browse/MAPREDUCE-1784) | IFile should check for null compressor |  Minor | . | Eli Collins | Eli Collins |
| [MAPREDUCE-1780](https://issues.apache.org/jira/browse/MAPREDUCE-1780) | AccessControlList.toString() is used for serialization of ACL in JobStatus.java |  Major | jobtracker | Ravi Gummadi | Ravi Gummadi |
| [MAPREDUCE-1773](https://issues.apache.org/jira/browse/MAPREDUCE-1773) | streaming doesn't support jobclient.output.filter |  Major | contrib/streaming | Alok Singh | Amareshwari Sriramadasu |
| [MAPREDUCE-1772](https://issues.apache.org/jira/browse/MAPREDUCE-1772) | Hadoop streaming doc should not use IdentityMapper as an example |  Minor | contrib/streaming, documentation | Marco Nicosia | Amareshwari Sriramadasu |
| [MAPREDUCE-1754](https://issues.apache.org/jira/browse/MAPREDUCE-1754) | Replace mapred.persmissions.supergroup with an acl : mapreduce.cluster.administrators |  Major | jobtracker | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [MAPREDUCE-1718](https://issues.apache.org/jira/browse/MAPREDUCE-1718) | job conf key for the services name of DelegationToken for HFTP url is constructed incorrectly in HFTPFileSystem |  Major | . | Boris Shkolnik | Boris Shkolnik |
| [MAPREDUCE-1707](https://issues.apache.org/jira/browse/MAPREDUCE-1707) | TaskRunner can get NPE in getting ugi from TaskTracker |  Major | tasktracker | Amareshwari Sriramadasu | Vinod Kumar Vavilapalli |
| [MAPREDUCE-1701](https://issues.apache.org/jira/browse/MAPREDUCE-1701) | AccessControlException while renewing a delegation token in not correctly handled in the JobTracker |  Major | jobtracker | Devaraj Das | Boris Shkolnik |
| [MAPREDUCE-1686](https://issues.apache.org/jira/browse/MAPREDUCE-1686) | ClassNotFoundException for custom format classes provided in libjars |  Minor | contrib/streaming, test | Paul Burkhardt | Paul Burkhardt |
| [MAPREDUCE-1670](https://issues.apache.org/jira/browse/MAPREDUCE-1670) | RAID should avoid policies that scan their own destination path |  Major | contrib/raid | Rodrigo Schmidt | Ramkumar Vadali |
| [MAPREDUCE-1668](https://issues.apache.org/jira/browse/MAPREDUCE-1668) | RaidNode should only Har a directory if all its parity files have been created |  Major | contrib/raid | Rodrigo Schmidt | Ramkumar Vadali |
| [MAPREDUCE-1662](https://issues.apache.org/jira/browse/MAPREDUCE-1662) | TaskRunner.prepare() and close() can be removed |  Major | tasktracker | Amareshwari Sriramadasu | Amareshwari Sriramadasu |
| [MAPREDUCE-1621](https://issues.apache.org/jira/browse/MAPREDUCE-1621) | Streaming's TextOutputReader.getLastOutput throws NPE if it has never read any output |  Major | contrib/streaming | Todd Lipcon | Amareshwari Sriramadasu |
| [MAPREDUCE-1617](https://issues.apache.org/jira/browse/MAPREDUCE-1617) | TestBadRecords failed once in our test runs |  Major | test | Amareshwari Sriramadasu | Luke Lu |
| [MAPREDUCE-1599](https://issues.apache.org/jira/browse/MAPREDUCE-1599) | MRBench reuses jobConf and credentials there in. |  Major | . | Jitendra Nath Pandey | Jitendra Nath Pandey |
| [MAPREDUCE-1597](https://issues.apache.org/jira/browse/MAPREDUCE-1597) | combinefileinputformat does not work with non-splittable files |  Major | . | Namit Jain | Amareshwari Sriramadasu |
| [MAPREDUCE-1566](https://issues.apache.org/jira/browse/MAPREDUCE-1566) | Need to add a mechanism to import tokens and secrets into a submitted job. |  Major | security | Owen O'Malley | Jitendra Nath Pandey |
| [MAPREDUCE-1559](https://issues.apache.org/jira/browse/MAPREDUCE-1559) | The DelegationTokenRenewal timer task should use the jobtracker's credentials to create the filesystem |  Major | jobtracker | Devaraj Das | Devaraj Das |
| [MAPREDUCE-1558](https://issues.apache.org/jira/browse/MAPREDUCE-1558) | specify correct server principal for RefreshAuthorizationPolicyProtocol and RefreshUserToGroupMappingsProtocol protocols in MRAdmin (for HADOOP-6612) |  Major | security | Boris Shkolnik | Boris Shkolnik |
| [MAPREDUCE-1543](https://issues.apache.org/jira/browse/MAPREDUCE-1543) | Log messages of JobACLsManager should use security logging of HADOOP-6586 |  Major | security | Vinod Kumar Vavilapalli | Luke Lu |
| [MAPREDUCE-1533](https://issues.apache.org/jira/browse/MAPREDUCE-1533) | Reduce or remove usage of String.format() usage in CapacityTaskScheduler.updateQSIObjects and Counters.makeEscapedString() |  Major | jobtracker | Rajesh Balamohan | Dick King |
| [MAPREDUCE-1532](https://issues.apache.org/jira/browse/MAPREDUCE-1532) | Delegation token is obtained as the superuser |  Major | job submission, security | Devaraj Das | Devaraj Das |
| [MAPREDUCE-1528](https://issues.apache.org/jira/browse/MAPREDUCE-1528) | TokenStorage should not be static |  Major | . | Owen O'Malley | Jitendra Nath Pandey |
| [MAPREDUCE-1505](https://issues.apache.org/jira/browse/MAPREDUCE-1505) | Cluster class should create the rpc client only when needed |  Major | client | Devaraj Das | Dick King |
| [MAPREDUCE-1375](https://issues.apache.org/jira/browse/MAPREDUCE-1375) | TestFileArgs fails intermittently |  Major | contrib/streaming, test | Amar Kamat | Todd Lipcon |
| [MAPREDUCE-1288](https://issues.apache.org/jira/browse/MAPREDUCE-1288) | DistributedCache localizes only once per cache URI |  Critical | distributed-cache, security, tasktracker | Devaraj Das | Devaraj Das |
| [MAPREDUCE-1280](https://issues.apache.org/jira/browse/MAPREDUCE-1280) | Eclipse Plugin does not work with Eclipse Ganymede (3.4) |  Major | . | Aaron Kimball | Alex Kozlov |
| [MAPREDUCE-1225](https://issues.apache.org/jira/browse/MAPREDUCE-1225) | TT successfully localizes a task even though the corresponding cache-file has already changed on DFS. |  Major | tasktracker | Vinod Kumar Vavilapalli | Zhong Wang |
| [MAPREDUCE-1118](https://issues.apache.org/jira/browse/MAPREDUCE-1118) | Capacity Scheduler scheduling information is hard to read / should be tabular format |  Major | capacity-sched | Allen Wittenauer | Krishna Ramachandran |
| [MAPREDUCE-1085](https://issues.apache.org/jira/browse/MAPREDUCE-1085) | For tasks, "ulimit -v -1" is being run when user doesn't specify mapred.child.ulimit |  Minor | tasktracker | Ravi Gummadi | Todd Lipcon |
| [MAPREDUCE-869](https://issues.apache.org/jira/browse/MAPREDUCE-869) | Documentation for config to set map/reduce task environment |  Major | documentation, task | Arun C Murthy | Alejandro Abdelnur |
| [MAPREDUCE-714](https://issues.apache.org/jira/browse/MAPREDUCE-714) | JobConf.findContainingJar unescapes unnecessarily on Linux |  Major | . | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-647](https://issues.apache.org/jira/browse/MAPREDUCE-647) | Update the DistCp forrest doc to make it consistent with the latest changes (5472, 5620, 5762, 5826) |  Major | documentation | Rodrigo Schmidt | Rodrigo Schmidt |
| [MAPREDUCE-577](https://issues.apache.org/jira/browse/MAPREDUCE-577) | Duplicate Mapper input when using StreamXmlRecordReader |  Major | contrib/streaming | David Campbell | Ravi Gummadi |
| [MAPREDUCE-572](https://issues.apache.org/jira/browse/MAPREDUCE-572) | If #link is missing from uri format of -cacheArchive then streaming does not throw error. |  Minor | contrib/streaming | Karam Singh | Amareshwari Sriramadasu |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-7783](https://issues.apache.org/jira/browse/HADOOP-7783) | Add more symlink tests that cover intermediate links |  Major | fs | Eli Collins | Eli Collins |
| [HADOOP-7298](https://issues.apache.org/jira/browse/HADOOP-7298) | Add test utility for writing multi-threaded tests |  Major | test | Todd Lipcon | Todd Lipcon |
| [HADOOP-7034](https://issues.apache.org/jira/browse/HADOOP-7034) | Add TestPath tests to cover dot, dot dot, and slash normalization |  Major | fs | Eli Collins | Eli Collins |
| [HADOOP-7024](https://issues.apache.org/jira/browse/HADOOP-7024) | Create a test method for adding file systems during tests. |  Major | test | Kan Zhang | Kan Zhang |
| [HADOOP-6934](https://issues.apache.org/jira/browse/HADOOP-6934) | test for ByteWritable comparator |  Major | record | Johannes Zillmann | Johannes Zillmann |
| [HADOOP-6803](https://issues.apache.org/jira/browse/HADOOP-6803) | Add native gzip read/write coverage to TestCodec |  Major | io | Eli Collins | Eli Collins |
| [HDFS-1855](https://issues.apache.org/jira/browse/HDFS-1855) | TestDatanodeBlockScanner.testBlockCorruptionRecoveryPolicy() part 2 fails in two different ways |  Major | test | Matt Foley | Matt Foley |
| [HDFS-1762](https://issues.apache.org/jira/browse/HDFS-1762) | Allow TestHDFSCLI to be run against a cluster |  Major | build, test | Tom White | Konstantin Boudnik |
| [HDFS-1562](https://issues.apache.org/jira/browse/HDFS-1562) | Add rack policy tests |  Major | namenode, test | Eli Collins | Eli Collins |
| [HDFS-1310](https://issues.apache.org/jira/browse/HDFS-1310) | TestFileConcurrentReader fails |  Major | hdfs-client | Suresh Srinivas | sam rash |
| [HDFS-1132](https://issues.apache.org/jira/browse/HDFS-1132) | Refactor TestFileStatus |  Minor | test | Eli Collins | Eli Collins |
| [HDFS-982](https://issues.apache.org/jira/browse/HDFS-982) | TestDelegationToken#testDelegationTokenWithRealUser is failing |  Blocker | contrib/hdfsproxy, security | Eli Collins | Po Cheung |
| [HDFS-981](https://issues.apache.org/jira/browse/HDFS-981) | test-contrib fails due to test-cactus failure |  Blocker | contrib/hdfsproxy | Eli Collins | Konstantin Boudnik |
| [HDFS-697](https://issues.apache.org/jira/browse/HDFS-697) | Enable asserts for tests by default |  Major | . | Eli Collins | Eli Collins |
| [HDFS-696](https://issues.apache.org/jira/browse/HDFS-696) | Java assertion failures triggered by tests |  Major | test | Eli Collins | Eli Collins |
| [MAPREDUCE-3156](https://issues.apache.org/jira/browse/MAPREDUCE-3156) | Allow TestMRCLI to be run against a cluster |  Major | test | Konstantin Boudnik | Konstantin Boudnik |
| [MAPREDUCE-2241](https://issues.apache.org/jira/browse/MAPREDUCE-2241) | ClusterWithLinuxTaskController should accept relative path on the command line |  Trivial | task-controller, test | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2180](https://issues.apache.org/jira/browse/MAPREDUCE-2180) | Add coverage of fair scheduler servlet to system test |  Minor | contrib/fair-share | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2073](https://issues.apache.org/jira/browse/MAPREDUCE-2073) | TestTrackerDistributedCacheManager should be up-front about requirements on build environment |  Trivial | distributed-cache, test | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2051](https://issues.apache.org/jira/browse/MAPREDUCE-2051) | Contribute a fair scheduler preemption system test |  Major | contrib/fair-share | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2034](https://issues.apache.org/jira/browse/MAPREDUCE-2034) | TestSubmitJob triggers NPE instead of permissions error |  Trivial | test | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-1092](https://issues.apache.org/jira/browse/MAPREDUCE-1092) | Enable asserts for tests by default |  Major | test | Eli Collins | Eli Collins |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-6683](https://issues.apache.org/jira/browse/HADOOP-6683) | the first optimization: ZlibCompressor does not fully utilize the buffer |  Minor | io | Kang Xiao | Kang Xiao |
| [HDFS-1997](https://issues.apache.org/jira/browse/HDFS-1997) | Image transfer process misreports client side exceptions |  Major | namenode | Todd Lipcon | Todd Lipcon |
| [HDFS-1473](https://issues.apache.org/jira/browse/HDFS-1473) | Refactor storage management into separate classes than fsimage file reading/writing |  Major | namenode | Todd Lipcon | Todd Lipcon |
| [HDFS-1462](https://issues.apache.org/jira/browse/HDFS-1462) | Refactor edit log loading to a separate class from edit log writing |  Major | namenode | Todd Lipcon | Todd Lipcon |
| [HDFS-1319](https://issues.apache.org/jira/browse/HDFS-1319) | Fix location of re-login for secondary namenode from HDFS-999 |  Major | namenode | Jakob Homan | Jakob Homan |
| [HDFS-1119](https://issues.apache.org/jira/browse/HDFS-1119) | Refactor BlocksMap with GettableSet |  Major | namenode | Tsz Wo Nicholas Sze | Tsz Wo Nicholas Sze |
| [HDFS-1071](https://issues.apache.org/jira/browse/HDFS-1071) | savenamespace should write the fsimage to all configured fs.name.dir in parallel |  Major | namenode | dhruba borthakur | Dmytro Molkov |
| [HDFS-1057](https://issues.apache.org/jira/browse/HDFS-1057) | Concurrent readers hit ChecksumExceptions if following a writer to very end of file |  Blocker | datanode | Todd Lipcon | sam rash |
| [HDFS-259](https://issues.apache.org/jira/browse/HDFS-259) | Remove intentionally corrupt 0.13 directory layout creation |  Major | . | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-2234](https://issues.apache.org/jira/browse/MAPREDUCE-2234) | If Localizer can't create task log directory, it should fail on the spot |  Major | tasktracker | Todd Lipcon | Todd Lipcon |
| [MAPREDUCE-1970](https://issues.apache.org/jira/browse/MAPREDUCE-1970) | Reed-Solomon code implementation to be used in raid |  Major | contrib/raid | Scott Chen | Scott Chen |
| [MAPREDUCE-927](https://issues.apache.org/jira/browse/MAPREDUCE-927) | Cleanup of task-logs should happen in TaskTracker instead of the Child |  Major | security, tasktracker | Vinod Kumar Vavilapalli | Amareshwari Sriramadasu |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-7291](https://issues.apache.org/jira/browse/HADOOP-7291) | Update Hudson job not to run test-contrib |  Major | . | Eli Collins | Eli Collins |
| [HADOOP-7283](https://issues.apache.org/jira/browse/HADOOP-7283) | Include 32-bit and 64-bit native libraries in Jenkins tarball builds |  Blocker | build | Tom White | Tom White |
| [HADOOP-6846](https://issues.apache.org/jira/browse/HADOOP-6846) | Scripts for building Hadoop 0.22.0 release |  Major | build | Tom White | Tom White |
| [HDFS-2513](https://issues.apache.org/jira/browse/HDFS-2513) | Bump jetty to 6.1.26 |  Major | build | Konstantin Boudnik | Konstantin Boudnik |
| [HDFS-1948](https://issues.apache.org/jira/browse/HDFS-1948) | Forward port 'hdfs-1520 lightweight namenode operation to trigger lease reccovery' |  Major | . | stack | stack |
| [HDFS-1946](https://issues.apache.org/jira/browse/HDFS-1946) | HDFS part of HADOOP-7291 |  Major | . | Eli Collins | Eli Collins |
| [HDFS-1167](https://issues.apache.org/jira/browse/HDFS-1167) | New property for local conf directory in system-test-hdfs.xml file. |  Major | test | Vinay Kumar Thota | Vinay Kumar Thota |
| [HDFS-712](https://issues.apache.org/jira/browse/HDFS-712) | Move libhdfs from mr to hdfs |  Major | libhdfs | Eli Collins | Eli Collins |
| [MAPREDUCE-3311](https://issues.apache.org/jira/browse/MAPREDUCE-3311) | Bump jetty to 6.1.26 |  Major | build | Konstantin Boudnik | Konstantin Boudnik |
| [MAPREDUCE-2939](https://issues.apache.org/jira/browse/MAPREDUCE-2939) | Ant setup on hadoop7 jenkins host |  Major | build | Joep Rottinghuis | Joep Rottinghuis |
| [MAPREDUCE-2383](https://issues.apache.org/jira/browse/MAPREDUCE-2383) | Improve documentation of DistributedCache methods |  Major | distributed-cache, documentation | Todd Lipcon | Harsh J |
| [MAPREDUCE-2169](https://issues.apache.org/jira/browse/MAPREDUCE-2169) | Integrated Reed-Solomon code with RaidNode |  Major | contrib/raid | Ramkumar Vadali | Ramkumar Vadali |
| [MAPREDUCE-2142](https://issues.apache.org/jira/browse/MAPREDUCE-2142) | Refactor RaidNode to remove dependence on map reduce |  Major | . | Patrick Kling | Patrick Kling |
| [MAPREDUCE-231](https://issues.apache.org/jira/browse/MAPREDUCE-231) | Split map/reduce into sub-project |  Major | . | Owen O'Malley | Owen O'Malley |


