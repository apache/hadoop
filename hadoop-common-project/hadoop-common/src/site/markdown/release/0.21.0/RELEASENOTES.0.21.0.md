
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
# Apache Hadoop  0.21.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-6813](https://issues.apache.org/jira/browse/HADOOP-6813) | *Blocker* | **Add a new newInstance method in FileSystem that takes a "user" as argument**

I've just committed this to 0.21.


---

* [HADOOP-6748](https://issues.apache.org/jira/browse/HADOOP-6748) | *Major* | **Remove hadoop.cluster.administrators**

Removed configuration property "hadoop.cluster.administrators". Added constructor public HttpServer(String name, String bindAddress, int port, boolean findPort, Configuration conf, AccessControlList adminsAcl) in HttpServer, which takes cluster administrators acl as a parameter.


---

* [HADOOP-6701](https://issues.apache.org/jira/browse/HADOOP-6701) | *Minor* | ** Incorrect exit codes for "dfs -chown", "dfs -chgrp"**

Commands chmod, chown and chgrp now returns non zero exit code and an error message on failure instead of returning zero.


---

* [HADOOP-6692](https://issues.apache.org/jira/browse/HADOOP-6692) | *Major* | **Add FileContext#listStatus that returns an iterator**

This issue adds Iterator\<FileStatus\> listStatus(Path) to FileContext, moves FileStatus[] listStatus(Path) to FileContext#Util, and adds Iterator\<FileStatus\> listStatusItor(Path) to AbstractFileSystem which provides a default implementation by using FileStatus[] listStatus(Path).


---

* [HADOOP-6686](https://issues.apache.org/jira/browse/HADOOP-6686) | *Major* | **Remove redundant exception class name in unwrapped exceptions thrown at the RPC client**

The exceptions thrown by the RPC client no longer carries a redundant exception class name in exception message.


---

* [HADOOP-6577](https://issues.apache.org/jira/browse/HADOOP-6577) | *Major* | **IPC server response buffer reset threshold should be configurable**

Add hidden configuration option "ipc.server.max.response.size" to change the default 1 MB, the maximum size when large IPC handler response buffer is reset.


---

* [HADOOP-6569](https://issues.apache.org/jira/browse/HADOOP-6569) | *Major* | **FsShell#cat should avoid calling unecessary getFileStatus before opening a file to read**

**WARNING: No release note provided for this incompatible change.**


---

* [HADOOP-6568](https://issues.apache.org/jira/browse/HADOOP-6568) | *Major* | **Authorization for default servlets**

Added web-authorization for the default servlets - /logs, /stacks, /logLevel, /metrics, /conf, so that only cluster administrators can access these servlets. hadoop.cluster.administrators is the new configuration in core-default.xml that can be used to specify the ACL against which an authenticated user should be verified if he/she is an administrator.


---

* [HADOOP-6537](https://issues.apache.org/jira/browse/HADOOP-6537) | *Major* | **Proposal for exceptions thrown by FileContext and Abstract File System**

Detailed exceptions declared in FileContext and AbstractFileSystem


---

* [HADOOP-6531](https://issues.apache.org/jira/browse/HADOOP-6531) | *Minor* | **add FileUtil.fullyDeleteContents(dir) api to delete contents of a directory**

Added an api FileUtil.fullyDeleteContents(String dir) to delete contents of the directory.


---

* [HADOOP-6515](https://issues.apache.org/jira/browse/HADOOP-6515) | *Major* | **Make maximum number of http threads configurable**

HADOOP-6515. Make maximum number of http threads configurable (Scott Chen via zshao)


---

* [HADOOP-6489](https://issues.apache.org/jira/browse/HADOOP-6489) | *Major* | **Findbug report: LI\_LAZY\_INIT\_STATIC, OBL\_UNSATISFIED\_OBLIGATION**

Fix 3 findsbugs warnings.


---

* [HADOOP-6441](https://issues.apache.org/jira/browse/HADOOP-6441) | *Major* | **Prevent remote CSS attacks in Hostname and UTF-7.**

Quotes the characters coming out of getRequestUrl and getServerName in HttpServer.java as per the specification in HADOOP-6151.


---

* [HADOOP-6433](https://issues.apache.org/jira/browse/HADOOP-6433) | *Major* | **Add AsyncDiskService that is used in both hdfs and mapreduce**

HADOOP-6433. Add AsyncDiskService for asynchronous disk services.


---

* [HADOOP-6386](https://issues.apache.org/jira/browse/HADOOP-6386) | *Blocker* | **NameNode's HttpServer can't instantiate InetSocketAddress: IllegalArgumentException is thrown**

Improved initialization sequence so that Port Out of Range error when starting web server will less likely interrupt testing.


---

* [HADOOP-6367](https://issues.apache.org/jira/browse/HADOOP-6367) | *Major* | **Move Access Token implementation from Common to HDFS**

**WARNING: No release note provided for this incompatible change.**


---

* [HADOOP-6344](https://issues.apache.org/jira/browse/HADOOP-6344) | *Major* | **rm and rmr fail to correctly move the user's files to the trash prior to deleting when they are over quota.**

Trash feature notifies user of over-quota condition rather than silently deleting files/directories; deletion can be compelled with "rm -skiptrash".


---

* [HADOOP-6343](https://issues.apache.org/jira/browse/HADOOP-6343) | *Major* | **Stack trace of any runtime exceptions should be recorded in the server logs.**

Record runtime exceptions in server log to facilitate fault analysis.


---

* [HADOOP-6313](https://issues.apache.org/jira/browse/HADOOP-6313) | *Major* | **Expose flush APIs to application users**

FSOutputDataStream implement Syncable interface to provide hflush and hsync APIs to the application users.


---

* [HADOOP-6299](https://issues.apache.org/jira/browse/HADOOP-6299) | *Major* | **Use JAAS LoginContext for our login**

**WARNING: No release note provided for this incompatible change.**


---

* [HADOOP-6281](https://issues.apache.org/jira/browse/HADOOP-6281) | *Major* | **HtmlQuoting throws NullPointerException**

Fixed null pointer error when quoting HTML in the case JSP has no parameters.


---

* [HADOOP-6235](https://issues.apache.org/jira/browse/HADOOP-6235) | *Major* | **Adding a new method for getting server default values from a FileSystem**

New FileSystem method reports default parameters that would be used by server. See also HDFS-578.


---

* [HADOOP-6234](https://issues.apache.org/jira/browse/HADOOP-6234) | *Major* | **Permission configuration files should use octal and symbolic**

New configuration option dfs.umaskmode sets umask with octal or symbolic value.


---

* [HADOOP-6230](https://issues.apache.org/jira/browse/HADOOP-6230) | *Major* | **Move process tree, and memory calculator classes out of Common into Map/Reduce.**

Moved process tree, and memory calculator classes out of Common project into the Map/Reduce project.


---

* [HADOOP-6226](https://issues.apache.org/jira/browse/HADOOP-6226) | *Major* | **Create a LimitedByteArrayOutputStream that does not expand its buffer on write**

New LimitedByteArrayOutputStream does not expand buffer on writes.


---

* [HADOOP-6223](https://issues.apache.org/jira/browse/HADOOP-6223) | *Major* | **New improved FileSystem interface for those implementing new files systems.**

Add new file system interface AbstractFileSystem with implementation of some file systems that delegate to old FileSystem.


---

* [HADOOP-6203](https://issues.apache.org/jira/browse/HADOOP-6203) | *Major* | **Improve error message when moving to trash fails due to quota issue**

Improved error message suggests using -skpTrash option when hdfs -rm fails to move to trash because of quota.


---

* [HADOOP-6201](https://issues.apache.org/jira/browse/HADOOP-6201) | *Major* | **FileSystem::ListStatus should throw FileNotFoundException**

FileSystem listStatus method throws FileNotFoundException for all implementations. Application code should catch or propagate FileNotFoundException.


---

* [HADOOP-6184](https://issues.apache.org/jira/browse/HADOOP-6184) | *Major* | **Provide a configuration dump in json format.**

New Configuration.dumpConfiguration(Configuration, Writer) writes configuration parameters in the JSON format.


---

* [HADOOP-6170](https://issues.apache.org/jira/browse/HADOOP-6170) | *Major* | **add Avro-based RPC serialization**

RPC can use Avro serialization.


---

* [HADOOP-6161](https://issues.apache.org/jira/browse/HADOOP-6161) | *Minor* | **Add get/setEnum to Configuration**

Added following APIs to Configuration:
- public \<T extends Enum\<T\>\> T getEnum(String name, T defaultValue)
- public \<T extends Enum\<T\>\> void setEnum(String name, T value)


---

* [HADOOP-6151](https://issues.apache.org/jira/browse/HADOOP-6151) | *Critical* | **The servlets should quote html characters**

The input parameters for all of the servlets will have the 5 html meta characters quoted. The characters are '&', '\<', '\>', '"' and the apostrophe. The goal is to ensure that our web ui servlets can't be used for cross site scripting (XSS) attacks. In particular, it blocks the frequent (especially for errors) case where the servlet echos back the parameters to the user.


---

* [HADOOP-6120](https://issues.apache.org/jira/browse/HADOOP-6120) | *Major* | **Add support for Avro types in hadoop**

New Avro serialization in .../io/serializer/avro.


---

* [HADOOP-5976](https://issues.apache.org/jira/browse/HADOOP-5976) | *Major* | **create script to provide classpath for external tools**

New Hadoop script command classpath prints the path to the Hadoop jar and libraries.


---

* [HADOOP-5913](https://issues.apache.org/jira/browse/HADOOP-5913) | *Major* | **Allow administrators to be able to start and stop queues**

New mradmin command -refreshQueues  reads new configuration of ACLs and queue states from mapred-queues.xml. If the new queue state is not "running," jobs in progress will continue, but no other jobs from that queue will be started.


---

* [HADOOP-5887](https://issues.apache.org/jira/browse/HADOOP-5887) | *Major* | **Sqoop should create tables in Hive metastore after importing to HDFS**

New Sqoop argument --hive-import facilitates loading data into Hive.


---

* [HADOOP-5879](https://issues.apache.org/jira/browse/HADOOP-5879) | *Major* | **GzipCodec should read compression level etc from configuration**

Provide an ability to configure the compression level and strategy for codecs. Compressors need to be 'reinited' with new characteristics such as compression level etc. and hence an incompatible addition to the api.


---

* [HADOOP-5861](https://issues.apache.org/jira/browse/HADOOP-5861) | *Major* | **s3n files are not getting split by default**

Files stored on the native S3 filesystem (s3n:// URIs) now report a block size determined by the fs.s3n.block.size property (default 64MB).


---

* [HADOOP-5815](https://issues.apache.org/jira/browse/HADOOP-5815) | *Major* | **Sqoop: A database import tool for Hadoop**

New contribution Sqoop is a JDBC-based database import tool for Hadoop.


---

* [HADOOP-5784](https://issues.apache.org/jira/browse/HADOOP-5784) | *Major* | **The length of the heartbeat cycle should be configurable.**

Introduced a configuration parameter, mapred.heartbeats.in.second, as an expert option, that defines how many heartbeats a jobtracker can process in a second. Administrators can set this to an appropriate value based on cluster size and expected processing time on the jobtracker to achieve a balance between jobtracker scalability and latency of jobs.


---

* [HADOOP-5771](https://issues.apache.org/jira/browse/HADOOP-5771) | *Major* | **Create unit test for LinuxTaskController**

Added unit tests for verifying LinuxTaskController functionality.


---

* [HADOOP-5752](https://issues.apache.org/jira/browse/HADOOP-5752) | *Major* | **Provide examples of using offline image viewer (oiv) to analyze hadoop file systems**

Additional examples and documentation for HDFS Offline Image Viewer Tool show how to generate Pig-friendly data and to do analysis with Pig.


---

* [HADOOP-5745](https://issues.apache.org/jira/browse/HADOOP-5745) | *Major* | **Allow setting the default value of maxRunningJobs for all pools**

New Fair Scheduler configuration parameter sets a default limit on number of running jobs for all pools.


---

* [HADOOP-5738](https://issues.apache.org/jira/browse/HADOOP-5738) | *Major* | **Split waiting tasks field in JobTracker metrics to individual tasks**

**WARNING: No release note provided for this incompatible change.**


---

* [HADOOP-5737](https://issues.apache.org/jira/browse/HADOOP-5737) | *Major* | **UGI checks in testcases are broken**

Fixed JobTracker to use it's own credentials instead of the job's credentials for accessing mapred.system.dir. Also added APIs in the JobTracker to get the FileSystem objects as per the JobTracker's configuration.


---

* [HADOOP-5679](https://issues.apache.org/jira/browse/HADOOP-5679) | *Major* | **Resolve findbugs warnings in core/streaming/pipes/examples**

**WARNING: No release note provided for this incompatible change.**


---

* [HADOOP-5675](https://issues.apache.org/jira/browse/HADOOP-5675) | *Minor* | **DistCp should not launch a job if it is not necessary**

Distcp will no longer start jobs that move no data.


---

* [HADOOP-5643](https://issues.apache.org/jira/browse/HADOOP-5643) | *Major* | **Ability to blacklist tasktracker**

New mradmin command -refreshNodes updates the job tracker's node lists.


---

* [HADOOP-5620](https://issues.apache.org/jira/browse/HADOOP-5620) | *Major* | **discp can preserve modification times of files**

New DistCp option -pt preserves last modification and last access times of copied files.


---

* [HADOOP-5592](https://issues.apache.org/jira/browse/HADOOP-5592) | *Minor* | **Hadoop Streaming - GzipCodec**

Updates streaming documentation to correct the name used for the GZipCodec.


---

* [HADOOP-5582](https://issues.apache.org/jira/browse/HADOOP-5582) | *Major* | **Hadoop Vaidya throws number format exception due to changes in the job history counters string format (escaped compact representation).**

Fixed error parsing job history counters after change of counter format.


---

* [HADOOP-5528](https://issues.apache.org/jira/browse/HADOOP-5528) | *Major* | **Binary partitioner**

New BinaryPartitioner that partitions BinaryComparable keys by hashing a configurable part of the bytes array corresponding to the key.


---

* [HADOOP-5518](https://issues.apache.org/jira/browse/HADOOP-5518) | *Major* | **MRUnit unit test library**

New contribution MRUnit helps authors of map-reduce programs write unit tests with JUnit.


---

* [HADOOP-5485](https://issues.apache.org/jira/browse/HADOOP-5485) | *Major* | **Authorisation machanism required for acceesing jobtracker url :- jobtracker.com:port/scheduler**

New Fair Scheduler configuration parameter webinterface.private.actions controls whether changes to pools and priorities are permitted from the web interface. Changes are not permitted by default.


---

* [HADOOP-5469](https://issues.apache.org/jira/browse/HADOOP-5469) | *Major* | **Exposing Hadoop metrics via HTTP**

New server web page .../metrics allows convenient access to metrics data via JSON and text.


---

* [HADOOP-5467](https://issues.apache.org/jira/browse/HADOOP-5467) | *Major* | **Create an offline fsimage image viewer**

New Offline Image Viewer (oiv) tool reads an fsimage file and writes the data in a variety of user-friendly formats, including XML.


---

* [HADOOP-5464](https://issues.apache.org/jira/browse/HADOOP-5464) | *Major* | **DFSClient does not treat write timeout of 0 properly**

Zero values for dfs.socket.timeout and dfs.datanode.socket.write.timeout are now respected. Previously zero values for these parameters resulted in a 5 second timeout.


---

* [HADOOP-5457](https://issues.apache.org/jira/browse/HADOOP-5457) | *Major* | **Failing contrib tests should not stop the build**

Fixed the build to make sure that all the unit tests in contrib are run, regardless of the success/failure status of the previous projects' tests.


---

* [HADOOP-5438](https://issues.apache.org/jira/browse/HADOOP-5438) | *Major* | **Merge FileSystem.create and FileSystem.append**

**WARNING: No release note provided for this incompatible change.**


---

* [HADOOP-5396](https://issues.apache.org/jira/browse/HADOOP-5396) | *Major* | **Queue ACLs should be refreshed without requiring a restart of the job tracker**

Job Tracker queue ACLs can be changed without restarting Job Tracker.


---

* [HADOOP-5363](https://issues.apache.org/jira/browse/HADOOP-5363) | *Major* | **Proxying for multiple HDFS clusters of different versions**

New HDFS proxy server (Tomcat based) allows clients controlled access to clusters with different versions. See Hadoop-5366 for information on using curl and wget.


---

* [HADOOP-5258](https://issues.apache.org/jira/browse/HADOOP-5258) | *Major* | **Provide dfsadmin functionality to report on namenode's view of network topology**

New dfsAdmin command -printTopology shows topology as understood by the namenode.


---

* [HADOOP-5257](https://issues.apache.org/jira/browse/HADOOP-5257) | *Minor* | **Export namenode/datanode functionality through a pluggable RPC layer**

New plugin facility for namenode and datanode instantiates classes named in new configuration properties dfs.datanode.plugins and dfs.namenode.plugins.


---

* [HADOOP-5222](https://issues.apache.org/jira/browse/HADOOP-5222) | *Minor* | **Add offset in client trace**

Include IO offset to client trace logging output.


---

* [HADOOP-5219](https://issues.apache.org/jira/browse/HADOOP-5219) | *Major* | **SequenceFile is using mapred property**

New configuration parameter io.seqfile.local.dir for use by SequenceFile replaces mapred.local.dir.


---

* [HADOOP-5191](https://issues.apache.org/jira/browse/HADOOP-5191) | *Minor* | **After creation and startup of the hadoop namenode on AIX or Solaris, you will only be allowed to connect to the namenode via hostname but not IP.**

Accessing HDFS with any ip, hostname, or proxy should work as long as it points to the interface NameNode is listening on.


---

* [HADOOP-5176](https://issues.apache.org/jira/browse/HADOOP-5176) | *Trivial* | **TestDFSIO reports itself as TestFDSIO**

**WARNING: No release note provided for this incompatible change.**


---

* [HADOOP-5175](https://issues.apache.org/jira/browse/HADOOP-5175) | *Major* | **Option to prohibit jars unpacking**

Jars passed to the -libjars option of hadoop jars are no longer unpacked inside mapred.local.dir.


---

* [HADOOP-5144](https://issues.apache.org/jira/browse/HADOOP-5144) | *Major* | **manual way of turning on restore of failed storage replicas for namenode**

New DFSAdmin command -restoreFailedStorage true\|false\|check sets policy for restoring failed fsimage/editslog volumes.


---

* [HADOOP-5094](https://issues.apache.org/jira/browse/HADOOP-5094) | *Minor* | **Show dead nodes information in dfsadmin -report**

Changed df dfsadmin -report to list live and dead nodes, and attempt to resolve the hostname of datanode ip addresses.


---

* [HADOOP-5073](https://issues.apache.org/jira/browse/HADOOP-5073) | *Major* | **Hadoop 1.0 Interface Classification - scope (visibility - public/private) and stability**

Annotation mechanism enables interface classification.


---

* [HADOOP-5052](https://issues.apache.org/jira/browse/HADOOP-5052) | *Major* | **Add an example for computing exact digits of Pi**

New example BaileyBorweinPlouffe computes digits of pi. (World record!)


---

* [HADOOP-5042](https://issues.apache.org/jira/browse/HADOOP-5042) | *Major* | ** Add expiration handling to the chukwa log4j appender**

Chukwwa Log4J appender options allow a retention policy to limit number of files.


---

* [HADOOP-5022](https://issues.apache.org/jira/browse/HADOOP-5022) | *Blocker* | **[HOD] logcondense should delete all hod logs for a user, including jobtracker logs**

New logcondense option retain-master-logs indicates whether the script should delete master logs as part of its cleanup process. By default this option is false; master logs are deleted. Earlier versions of logcondense did not delete master logs.


---

* [HADOOP-5018](https://issues.apache.org/jira/browse/HADOOP-5018) | *Major* | **Chukwa should support pipelined writers**

Chukwa supports pipelined writers for improved extensibility.


---

* [HADOOP-4952](https://issues.apache.org/jira/browse/HADOOP-4952) | *Major* | **Improved files system interface for the application writer.**

New FileContext API introduced to replace FileSystem API. FileContext will be the version-compatible API for future releases. FileSystem API will be deprecated in the next release.


---

* [HADOOP-4942](https://issues.apache.org/jira/browse/HADOOP-4942) | *Major* | **Remove getName() and getNamed(String name, Configuration conf)**

Removed deprecated methods getName() and getNamed(String, Configuration) from FileSystem and descendant classes.


---

* [HADOOP-4941](https://issues.apache.org/jira/browse/HADOOP-4941) | *Major* | **Remove getBlockSize(Path f), getLength(Path f) and getReplication(Path src)**

Removed deprecated FileSystem methods getBlockSize(Path f), getLength(Path f), and getReplication(Path src).


---

* [HADOOP-4940](https://issues.apache.org/jira/browse/HADOOP-4940) | *Major* | **Remove delete(Path f)**

Removed deprecated method FileSystem.delete(Path).


---

* [HADOOP-4933](https://issues.apache.org/jira/browse/HADOOP-4933) | *Blocker* | **ConcurrentModificationException in JobHistory.java**

Fixed a synchronization bug in job history content parsing that could result in garbled history data or a ConcurrentModificationException.


---

* [HADOOP-4927](https://issues.apache.org/jira/browse/HADOOP-4927) | *Major* | **Part files on the output filesystem are created irrespective of whether the corresponding task has anything to write there**

All output part files are created regardless of whether the corresponding task has output.


---

* [HADOOP-4895](https://issues.apache.org/jira/browse/HADOOP-4895) | *Major* | **Remove deprecated methods in DFSClient**

Removed deprecated methods DFSClient.getHints() and DFSClient.isDirectory().


---

* [HADOOP-4885](https://issues.apache.org/jira/browse/HADOOP-4885) | *Major* | **Try to restore failed replicas of Name Node storage (at checkpoint time)**

Patch introduces new configuration switch dfs.name.dir.restore (boolean) enabling this functionality. Documentation needs to be updated.

UPDATE: Config key is now "dfs.namenode.name.dir.restore" for 1.x and 2.x+ versions of HDFS


---

* [HADOOP-4861](https://issues.apache.org/jira/browse/HADOOP-4861) | *Trivial* | **Add disk usage with human-readable size (-duh)**

Output of hadoop fs -dus changed to be consistent with hadoop fs -du and with Linux du. Users who previously parsed this output should update their scripts. New feature hadoop fs -du -h may be used for human readable output.


---

* [HADOOP-4842](https://issues.apache.org/jira/browse/HADOOP-4842) | *Major* | **Streaming combiner should allow command, not just JavaClass**

Streaming option -combiner allows any streaming command (not just Java class) to be a combiner.


---

* [HADOOP-4829](https://issues.apache.org/jira/browse/HADOOP-4829) | *Minor* | **Allow FileSystem shutdown hook to be disabled**

New configuration parameter fs.automatic.close can be set false to disable the JVM shutdown hook that automatically closes FileSystems.


---

* [HADOOP-4779](https://issues.apache.org/jira/browse/HADOOP-4779) | *Major* | **Remove deprecated FileSystem methods**

Removed deprecated FileSystem methods .


---

* [HADOOP-4768](https://issues.apache.org/jira/browse/HADOOP-4768) | *Major* | **Dynamic Priority Scheduler that allows queue shares to be controlled dynamically by a currency**

New contribution Dynamic Scheduler implements dynamic priorities with a currency model. Usage instructions are in the Jira item.


---

* [HADOOP-4756](https://issues.apache.org/jira/browse/HADOOP-4756) | *Major* | **Create a command line tool to access JMX exported properties from a NameNode server**

New HDFS tool JMXGet facilitates command line access to statistics via JMX.


---

* [HADOOP-4655](https://issues.apache.org/jira/browse/HADOOP-4655) | *Major* | **FileSystem.CACHE should be ref-counted**

Every invocation of FileSystem.newInstance() returns a newly allocated FileSystem object. This may be an incompatible change for applications that relied on FileSystem object identity.


---

* [HADOOP-4648](https://issues.apache.org/jira/browse/HADOOP-4648) | *Major* | **Remove ChecksumDistriubtedFileSystem and InMemoryFileSystem**

Removed obsolete, deprecated subclasses of ChecksumFileSystem (InMemoryFileSystem, ChecksumDistributedFileSystem).


---

* [HADOOP-4539](https://issues.apache.org/jira/browse/HADOOP-4539) | *Major* | **Streaming Edits to a Backup Node.**

Introduced backup node which maintains the up-to-date state of the namespace by receiving edits from the namenode, and checkpoint node, which creates checkpoints of the name space. These facilities replace the secondary namenode.


---

* [HADOOP-4368](https://issues.apache.org/jira/browse/HADOOP-4368) | *Minor* | **Superuser privileges required to do "df"**

New filesystem shell command -df reports capacity, space used and space free. Any user may execute this command without special privileges.


---

* [HADOOP-4359](https://issues.apache.org/jira/browse/HADOOP-4359) | *Major* | **Access Token: Support for data access authorization checking on DataNodes**

Introduced access tokens as capabilities for accessing datanodes. This change to internal protocols does not affect client applications.


---

* [HADOOP-4268](https://issues.apache.org/jira/browse/HADOOP-4268) | *Major* | **Permission checking in fsck**

Fsck now checks permissions as directories are traversed. Any user can now use fsck, but information is provided only for directories the user has permission to read.


---

* [HADOOP-4041](https://issues.apache.org/jira/browse/HADOOP-4041) | *Major* | **IsolationRunner does not work as documented**

Fixed a bug in IsolationRunner to make it work for map tasks.


---

* [HADOOP-4012](https://issues.apache.org/jira/browse/HADOOP-4012) | *Major* | **Providing splitting support for bzip2 compressed files**

BZip2 files can now be split.


---

* [HADOOP-3953](https://issues.apache.org/jira/browse/HADOOP-3953) | *Major* | **Sticky bit for directories**

UNIX-style sticky bit implemented for HDFS directories. When  the  sticky  bit  is set on a directory, files in that directory may be deleted or renamed only by a superuser or the file's owner.


---

* [HADOOP-3741](https://issues.apache.org/jira/browse/HADOOP-3741) | *Major* | **SecondaryNameNode has http server on dfs.secondary.http.address but without any contents**

Backup namenode's web UI default page now has some useful content.


---

* [HADOOP-2827](https://issues.apache.org/jira/browse/HADOOP-2827) | *Major* | **Remove deprecated NetUtils.getServerAddress**

Removed deprecated NetUtils.getServerAddress.


---

* [HADOOP-1722](https://issues.apache.org/jira/browse/HADOOP-1722) | *Major* | **Make streaming to handle non-utf8 byte array**

Streaming allows binary (or other non-UTF8) streams.


---

* [HDFS-1024](https://issues.apache.org/jira/browse/HDFS-1024) | *Blocker* | **SecondaryNamenode fails to checkpoint because namenode fails with CancelledKeyException**

**WARNING: No release note provided for this incompatible change.**


---

* [HDFS-1016](https://issues.apache.org/jira/browse/HDFS-1016) | *Major* | **HDFS side change for HADOOP-6569**

When cat a directory or a non-existent file from the command line, the error message gets printed becomes
cat: io.java.FileNotFoundException: File does not exist: \<absolute path name\>


---

* [HDFS-1012](https://issues.apache.org/jira/browse/HDFS-1012) | *Major* | **documentLocation attribute in LdapEntry for HDFSProxy isn't specific to a cluster**

Support for fully qualified HDFS path in addition to simple unqualified path. 
The qualified path indicates that the path is accessible on the specific HDFS. Non qualified path is qualified in all clusters.


---

* [HDFS-998](https://issues.apache.org/jira/browse/HDFS-998) | *Major* | **The servlets should quote server generated strings sent in the response**

The servlets should quote server generated strings sent in the response.


---

* [HDFS-985](https://issues.apache.org/jira/browse/HDFS-985) | *Major* | **HDFS should issue multiple RPCs for listing a large directory**

**WARNING: No release note provided for this incompatible change.**


---

* [HDFS-984](https://issues.apache.org/jira/browse/HDFS-984) | *Major* | **Delegation Tokens should be persisted in Namenode**

Layout version is set to -24 reflecting changes in edits log and fsimage format related to persisting delegation tokens.


---

* [HDFS-946](https://issues.apache.org/jira/browse/HDFS-946) | *Major* | **NameNode should not return full path name when lisitng a diretory or getting the status of a file**

**WARNING: No release note provided for this incompatible change.**


---

* [HDFS-913](https://issues.apache.org/jira/browse/HDFS-913) | *Major* | **TestRename won't run automatically from 'run-test-hdfs-faul-inject' target**

HDFS-913. Rename fault injection test TestRename.java to TestFiRename.java to include it in tests run by ant target run-test-hdfs-fault-inject.


---

* [HDFS-897](https://issues.apache.org/jira/browse/HDFS-897) | *Major* | **ReplicasMap remove has a bug in generation stamp comparison**

Fixed a bug in ReplicasMap.remove method, which compares the generation stamp of the replica removed to  itself instead of the the block passed to the method to identify the replica to be removed.


---

* [HDFS-892](https://issues.apache.org/jira/browse/HDFS-892) | *Major* | **optionally use Avro for namenode RPC**

**WARNING: No release note provided for this incompatible change.**


---

* [HDFS-873](https://issues.apache.org/jira/browse/HDFS-873) | *Major* | **DataNode directories as URIs**

**WARNING: No release note provided for this incompatible change.**


---

* [HDFS-850](https://issues.apache.org/jira/browse/HDFS-850) | *Minor* | **Display more memory details on the web ui**

Changes the format of the message with Heap usage on the NameNode web page.


---

* [HDFS-814](https://issues.apache.org/jira/browse/HDFS-814) | *Major* | **Add an api to get the visible length of a DFSDataInputStream.**

Add an api to get the visible length of a DFSDataInputStream.


---

* [HDFS-793](https://issues.apache.org/jira/browse/HDFS-793) | *Blocker* | **DataNode should first receive the whole packet ack message before it constructs and sends its own ack message for the packet**

**WARNING: No release note provided for this incompatible change.**


---

* [HDFS-785](https://issues.apache.org/jira/browse/HDFS-785) | *Minor* | **Missing license header in java source files.**

Add the Apache license header to several files that are missing it.


---

* [HDFS-781](https://issues.apache.org/jira/browse/HDFS-781) | *Blocker* | **Metrics PendingDeletionBlocks is not decremented**

Correct PendingDeletionBlocks metric to properly decrement counts.


---

* [HDFS-764](https://issues.apache.org/jira/browse/HDFS-764) | *Major* | **Moving Access Token implementation from Common to HDFS**

**WARNING: No release note provided for this incompatible change.**


---

* [HDFS-761](https://issues.apache.org/jira/browse/HDFS-761) | *Major* | **Failure to process rename operation from edits log due to quota verification**

Corrected an error when checking quota policy that resulted in a failure to read the edits log, stopping the primary/secondary name node.


---

* [HDFS-758](https://issues.apache.org/jira/browse/HDFS-758) | *Major* | **Improve reporting of progress of decommissioning**

New name node web UI page displays details of decommissioning progress. (dfsnodelist.jsp?whatNodes=DECOMMISSIONING)


---

* [HDFS-737](https://issues.apache.org/jira/browse/HDFS-737) | *Major* | **Improvement in metasave output**

Add full path name of the file to the under replicated block information and summary of total number of files, blocks, live and dead datanodes to metasave output.


---

* [HDFS-702](https://issues.apache.org/jira/browse/HDFS-702) | *Major* | **Add Hdfs Impl for the new file system interface**

Add HDFS implementation of AbstractFileSystem.


---

* [HDFS-677](https://issues.apache.org/jira/browse/HDFS-677) | *Blocker* | **Rename failure due to quota results in deletion of src directory**

Rename properly considers the case where both source and destination are over quota; operation will fail with error indication.


---

* [HDFS-660](https://issues.apache.org/jira/browse/HDFS-660) | *Major* | **Remove deprecated methods from InterDatanodeProtocol.**

**WARNING: No release note provided for this incompatible change.**


---

* [HDFS-631](https://issues.apache.org/jira/browse/HDFS-631) | *Major* | **Changes in HDFS to rename the config keys as detailed in HDFS-531.**

File system configuration keys renamed as a step toward API standardization and backward compatibility.


---

* [HDFS-630](https://issues.apache.org/jira/browse/HDFS-630) | *Major* | **In DFSOutputStream.nextBlockOutputStream(), the client can exclude specific datanodes when locating the next block.**

**WARNING: No release note provided for this incompatible change.**


---

* [HDFS-625](https://issues.apache.org/jira/browse/HDFS-625) | *Major* | **ListPathsServlet throws NullPointerException**

Corrected error where listing path no longer in name space could stop ListPathsServlet until system restarted.


---

* [HDFS-618](https://issues.apache.org/jira/browse/HDFS-618) | *Major* | **Support for non-recursive mkdir in HDFS**

New DFSClient.mkdir(...) allows option of not creating missing parent(s).


---

* [HDFS-617](https://issues.apache.org/jira/browse/HDFS-617) | *Major* | **Support for non-recursive create() in HDFS**

New DFSClient.create(...) allows option of not creating missing parent(s).


---

* [HDFS-602](https://issues.apache.org/jira/browse/HDFS-602) | *Major* | **Atempt to make a directory under an existing file on DistributedFileSystem should throw an FileAlreadyExistsException instead of FileNotFoundException**

**WARNING: No release note provided for this incompatible change.**


---

* [HDFS-596](https://issues.apache.org/jira/browse/HDFS-596) | *Blocker* | **Memory leak in libhdfs: hdfsFreeFileInfo() in libhdfs does not free memory for mOwner and mGroup**

Memory leak in function hdfsFreeFileInfo in libhdfs. This bug affects fuse-dfs severely.


---

* [HDFS-595](https://issues.apache.org/jira/browse/HDFS-595) | *Major* | **FsPermission tests need to be updated for new octal configuration parameter from HADOOP-6234**

Unit tests updated to match syntax of new configuration parameters.


---

* [HDFS-578](https://issues.apache.org/jira/browse/HDFS-578) | *Major* | **Support for using server default values for blockSize and replication when creating a file**

New FileSystem.getServerDefaults() reports the server's default file creation parameters.


---

* [HDFS-567](https://issues.apache.org/jira/browse/HDFS-567) | *Major* | **Two contrib tools to facilitate searching for block history information**

New contribution Block Forensics aids investigation of missing blocks.


---

* [HDFS-538](https://issues.apache.org/jira/browse/HDFS-538) | *Major* | **DistributedFileSystem::listStatus incorrectly returns null for empty result sets**

FileSystem.listStatus() previously returned null for empty or nonexistent directories; will now return empty array for empty directories and throw FileNotFoundException for non-existent directory. Client code should be updated for new semantics.


---

* [HDFS-514](https://issues.apache.org/jira/browse/HDFS-514) | *Major* | **DFSClient.namenode is a public field. Should be private.**

**WARNING: No release note provided for this incompatible change.**


---

* [HDFS-512](https://issues.apache.org/jira/browse/HDFS-512) | *Major* | **Set block id as the key to Block**

**WARNING: No release note provided for this incompatible change.**


---

* [HDFS-503](https://issues.apache.org/jira/browse/HDFS-503) | *Major* | **Implement erasure coding as a layer on HDFS**

This patch implements an optional layer over HDFS that implements offline erasure-coding.  It can be used to reduce the total storage requirements of DFS.


---

* [HDFS-492](https://issues.apache.org/jira/browse/HDFS-492) | *Major* | **Expose corrupt replica/block information**

New server web pages provide block information: corrupt\_replicas\_xml and block\_info\_xml.


---

* [HDFS-457](https://issues.apache.org/jira/browse/HDFS-457) | *Major* | **better handling of volume failure in Data Node storage**

Datanode can continue if a volume for replica storage fails. Previously a datanode resigned if any volume failed.


---

* [HDFS-385](https://issues.apache.org/jira/browse/HDFS-385) | *Major* | **Design a pluggable interface to place replicas of blocks in HDFS**

New experimental API BlockPlacementPolicy allows investigating alternate rules for locating block replicas.


---

* [HDFS-288](https://issues.apache.org/jira/browse/HDFS-288) | *Major* | **Redundant computation in hashCode() implemenation**

**WARNING: No release note provided for this incompatible change.**


---

* [HDFS-245](https://issues.apache.org/jira/browse/HDFS-245) | *Major* | **Create symbolic links in HDFS**

HDFS-245. Adds a symlink implementation to HDFS. This complements the new symlink feature added in HADOOP-6421


---

* [HDFS-235](https://issues.apache.org/jira/browse/HDFS-235) | *Major* | **Add support for byte-ranges to hftp**

HFTP can now serve a specific byte range from a file


---

* [MAPREDUCE-1747](https://issues.apache.org/jira/browse/MAPREDUCE-1747) | *Blocker* | **Remove documentation for the 'unstable' job-acls feature**

Removed the documentation for the 'unstable' job-acls feature from branch 0.21.


---

* [MAPREDUCE-1727](https://issues.apache.org/jira/browse/MAPREDUCE-1727) | *Major* | **TestJobACLs fails after HADOOP-6686**

Fixed a testcase problem in TestJobACLs.


---

* [MAPREDUCE-1697](https://issues.apache.org/jira/browse/MAPREDUCE-1697) | *Major* | **Document the behavior of -file option in streaming and deprecate it in favour of generic -files option.**

Documented the behavior of -file option in streaming and deprecated it in favor of generic -files option.


---

* [MAPREDUCE-1692](https://issues.apache.org/jira/browse/MAPREDUCE-1692) | *Minor* | **Remove TestStreamedMerge from the streaming tests**

Removed streaming testcase which tested non-existent functionality in Streaming.


---

* [MAPREDUCE-1657](https://issues.apache.org/jira/browse/MAPREDUCE-1657) | *Major* | **After task logs directory is deleted, tasklog servlet displays wrong error message about job ACLs**

Fixed a bug in tasklog servlet which displayed wrong error message about job ACLs - an access control error instead of the expected log files gone error - after task logs directory is deleted.


---

* [MAPREDUCE-1635](https://issues.apache.org/jira/browse/MAPREDUCE-1635) | *Major* | **ResourceEstimator does not work after MAPREDUCE-842**

Fixed a bug related to resource estimation for disk-based scheduling by modifying TaskTracker to return correct map output size for the completed maps and -1 for other tasks or failures.


---

* [MAPREDUCE-1612](https://issues.apache.org/jira/browse/MAPREDUCE-1612) | *Major* | **job conf file is not accessible from job history web page**

Fixed a bug related to access of job\_conf.xml from the history web page of a job.


---

* [MAPREDUCE-1611](https://issues.apache.org/jira/browse/MAPREDUCE-1611) | *Blocker* | **Refresh nodes and refresh queues doesnt work with service authorization enabled**

Fixed a bug that caused all the AdminOperationsProtocol operations to fail when service-level authorization is enabled. The problem is solved by registering AdminOperationsProtocol also with MapReducePolicyProvider.


---

* [MAPREDUCE-1610](https://issues.apache.org/jira/browse/MAPREDUCE-1610) | *Major* | **Forrest documentation should be updated to reflect the changes in MAPREDUCE-856**

Updated forrest documentation to reflect the changes to make localized files from DistributedCache have right access-control on TaskTrackers(MAPREDUCE-856).


---

* [MAPREDUCE-1609](https://issues.apache.org/jira/browse/MAPREDUCE-1609) | *Major* | **TaskTracker.localizeJob should not set permissions on job log directory recursively**

Fixed TaskTracker so that it does not set permissions on job-log directory recursively. This fix both improves the performance of job localization as well as avoids a bug related to launching of task-cleanup attempts after TaskTracker's restart.


---

* [MAPREDUCE-1607](https://issues.apache.org/jira/browse/MAPREDUCE-1607) | *Major* | **Task controller may not set permissions for a task cleanup attempt's log directory**

Fixed initialization of a task-cleanup attempt's log directory by setting correct permissions via task-controller. Added new log4j properties hadoop.tasklog.iscleanup and log4j.appender.TLA.isCleanup to conf/log4j.properties. Changed the userlogs for a task-cleanup attempt to go into its own directory instead of the original attempt directory. This is an incompatible change as old userlogs of cleanup attempt-dirs before this release will no longer be visible.


---

* [MAPREDUCE-1606](https://issues.apache.org/jira/browse/MAPREDUCE-1606) | *Major* | **TestJobACLs may timeout as there are no slots for launching JOB\_CLEANUP task**

Fixed TestJobACLs test timeout failure because of no slots for launching JOB\_CLEANUP task.


---

* [MAPREDUCE-1568](https://issues.apache.org/jira/browse/MAPREDUCE-1568) | *Major* | **TrackerDistributedCacheManager should clean up cache in a background thread**

MAPREDUCE-1568. TrackerDistributedCacheManager should clean up cache in a background thread. (Scott Chen via zshao)


---

* [MAPREDUCE-1493](https://issues.apache.org/jira/browse/MAPREDUCE-1493) | *Major* | **Authorization for job-history pages**

Added web-authorization for job-history pages. This is an incompatible change - it changes the JobHistory format by adding job-acls to job-history files and JobHistory currently does not have the support to read older versions of history files.


---

* [MAPREDUCE-1482](https://issues.apache.org/jira/browse/MAPREDUCE-1482) | *Major* | **Better handling of task diagnostic information stored in the TaskInProgress**

Limit the size of diagnostics-string and state-string shipped as part of task status. This will help keep the JobTracker's memory usage under control. Diagnostic string and state string are capped to 1024 chars.


---

* [MAPREDUCE-1476](https://issues.apache.org/jira/browse/MAPREDUCE-1476) | *Major* | **committer.needsTaskCommit should not be called for a task cleanup attempt**

Fixed Map/Reduce framework to not call commit task for special tasks like job setup/cleanup and task cleanup.


---

* [MAPREDUCE-1466](https://issues.apache.org/jira/browse/MAPREDUCE-1466) | *Minor* | **FileInputFormat should save #input-files in JobConf**

Added a private configuration variable mapreduce.input.num.files, to store number of input files being processed by M/R job.


---

* [MAPREDUCE-1455](https://issues.apache.org/jira/browse/MAPREDUCE-1455) | *Major* | **Authorization for servlets**

Adds job-level authorization to servlets(other than history related servlets) for accessing job related info. Deprecates mapreduce.jobtracker.permissions.supergroup and adds the config mapreduce.cluster.permissions.supergroup at cluster level sothat it will be used by TaskTracker also. Authorization checks are done if authentication is succeeded and mapreduce.cluster.job-authorization-enabled is set to true.


---

* [MAPREDUCE-1454](https://issues.apache.org/jira/browse/MAPREDUCE-1454) | *Major* | **The servlets should quote server generated strings sent in the response**

Servlets should quote server generated strings sent in the response


---

* [MAPREDUCE-1435](https://issues.apache.org/jira/browse/MAPREDUCE-1435) | *Major* | **symlinks in cwd of the task are not handled properly after MAPREDUCE-896**

Fixes bugs in linux task controller and TaskRunner.setupWorkDir() related to handling of symlinks.


---

* [MAPREDUCE-1430](https://issues.apache.org/jira/browse/MAPREDUCE-1430) | *Major* | **JobTracker should be able to renew delegation tokens for the jobs**

mapreduce.job.complete.cancel.delegation.tokens - if false - don't cancel delegation token renewal when the job is complete, because it may be used by some other job.


---

* [MAPREDUCE-1423](https://issues.apache.org/jira/browse/MAPREDUCE-1423) | *Major* | **Improve performance of CombineFileInputFormat when multiple pools are configured**

MAPREDUCE-1423. Improve performance of CombineFileInputFormat when multiple pools are configured. (Dhruba Borthakur via zshao)


---

* [MAPREDUCE-1422](https://issues.apache.org/jira/browse/MAPREDUCE-1422) | *Major* | **Changing permissions of files/dirs under job-work-dir may be needed sothat cleaning up of job-dir in all mapred-local-directories succeeds always**

Introduced enableJobForCleanup() api in TaskController. This api enables deletion of stray files (with no write permissions for task-tracker) from job's work dir.  Note that the behavior is similar to TaskController#enableTaskForCleanup() except the path on which the 'chmod' is done is the job's work dir.


---

* [MAPREDUCE-1420](https://issues.apache.org/jira/browse/MAPREDUCE-1420) | *Major* | **TestTTResourceReporting failing in trunk**

Fixed a bug in the testcase TestTTResourceReporting.


---

* [MAPREDUCE-1417](https://issues.apache.org/jira/browse/MAPREDUCE-1417) | *Major* | **Forrest documentation should be updated to reflect the changes in MAPREDUCE-744**

Updated forrest documentation to reflect the changes w.r.t public and private distributed cache files.


---

* [MAPREDUCE-1403](https://issues.apache.org/jira/browse/MAPREDUCE-1403) | *Major* | **Save file-sizes of each of the artifacts in DistributedCache in the JobConf**

Added private configuration variables: mapred.cache.files.filesizes and mapred.cache.archives.filesizes to store sizes of distributed cache artifacts per job. This can be used by tools like Gridmix in simulation runs.


---

* [MAPREDUCE-1398](https://issues.apache.org/jira/browse/MAPREDUCE-1398) | *Major* | **TaskLauncher remains stuck on tasks waiting for free nodes even if task is killed.**

Fixed TaskLauncher to stop waiting for blocking slots, for a TIP that is killed / failed while it is in queue.


---

* [MAPREDUCE-1397](https://issues.apache.org/jira/browse/MAPREDUCE-1397) | *Minor* | **NullPointerException observed during task failures**

Fixed a race condition involving JvmRunner.kill() and KillTaskAction, which was leading to an NullPointerException causing a transient inconsistent state in JvmManager and failure of tasks.


---

* [MAPREDUCE-1385](https://issues.apache.org/jira/browse/MAPREDUCE-1385) | *Major* | **Make changes to MapReduce for the new UserGroupInformation APIs (HADOOP-6299)**

**WARNING: No release note provided for this incompatible change.**


---

* [MAPREDUCE-1383](https://issues.apache.org/jira/browse/MAPREDUCE-1383) | *Major* | **Allow storage and caching of delegation token.**

mapreduce.job.hdfs-servers - declares hdfs servers to be used by the job, so client can pre-fetch delegation tokens for thsese servers (comma separated list of NameNodes).


---

* [MAPREDUCE-1342](https://issues.apache.org/jira/browse/MAPREDUCE-1342) | *Major* | **Potential JT deadlock in faulty TT tracking**

Fix for a potential deadlock in the global blacklist of tasktrackers feature.


---

* [MAPREDUCE-1338](https://issues.apache.org/jira/browse/MAPREDUCE-1338) | *Major* | **need security keys storage solution**

new command line argument:
 tokensFile - path to the file with clients secret keys in JSON format


---

* [MAPREDUCE-1316](https://issues.apache.org/jira/browse/MAPREDUCE-1316) | *Blocker* | **JobTracker holds stale references to retired jobs via unreported tasks**

JobTracker holds stale references to TaskInProgress objects and hence indirectly holds reference to retired jobs resulting into memory leak. Only task-attempts which are yet to report their status are left behind in the memory. All the task-attempts are now removed from the JobTracker by iterating over the scheduled task-attempt ids instead of iterating over available task statuses.


---

* [MAPREDUCE-1307](https://issues.apache.org/jira/browse/MAPREDUCE-1307) | *Major* | **Introduce the concept of Job Permissions**

Added job-level authorization to MapReduce. JobTracker will now use the cluster configuration "mapreduce.cluster.job-authorization-enabled" to enable the checks to verify the authority of access of jobs where ever needed. Introduced two job-configuration properties to specify ACLs: "mapreduce.job.acl-view-job" and "mapreduce.job.acl-modify-job". For now, RPCs related to job-level counters, task-level counters and tasks' diagnostic information are protected by "mapreduce.job.acl-view-job" ACL. "mapreduce.job.acl-modify-job" protects killing of a job, killing a task of a job, failing a task of a job and setting the priority of a job. Irrespective of the above two ACLs, job-owner, superuser and members of supergroup configured on JobTracker via mapred.permissions.supergroup, can do all the view and modification operations.


---

* [MAPREDUCE-1306](https://issues.apache.org/jira/browse/MAPREDUCE-1306) | *Major* | **[MUMAK] Randomize the arrival of heartbeat responses**

[MUMAK] Randomize the arrival of heartbeat responses


---

* [MAPREDUCE-1287](https://issues.apache.org/jira/browse/MAPREDUCE-1287) | *Minor* | **Avoid calling Partitioner with only 1 reducer**

For jobs with only one reducer, the Partitioner will no longer be called. Applications depending on Partitioners modifying records for single reducer jobs will need to move this functionality elsewhere.


---

* [MAPREDUCE-1284](https://issues.apache.org/jira/browse/MAPREDUCE-1284) | *Major* | **TestLocalizationWithLinuxTaskController fails**

Fixes a bug in linux task controller by making the paths array passed to fts\_open() as null-terminated as per the man page.


---

* [MAPREDUCE-1230](https://issues.apache.org/jira/browse/MAPREDUCE-1230) | *Major* | **Vertica streaming adapter doesn't handle nulls in all cases**

Fixes null handling in records returned from VerticaInputFormat


---

* [MAPREDUCE-1218](https://issues.apache.org/jira/browse/MAPREDUCE-1218) | *Major* | **Collecting cpu and memory usage for TaskTrackers**

This patch allows TaskTracker reports it's current available memory and CPU usage to JobTracker through heartbeat. The information can be used for scheduling and monitoring in the JobTracker. This patch changes the version of InterTrackerProtocal.


---

* [MAPREDUCE-1213](https://issues.apache.org/jira/browse/MAPREDUCE-1213) | *Major* | **TaskTrackers restart is very slow because it deletes distributed cache directory synchronously**

Directories specified in mapred.local.dir that can not be created now cause the TaskTracker to fail to start.


---

* [MAPREDUCE-1185](https://issues.apache.org/jira/browse/MAPREDUCE-1185) | *Major* | **URL to JT webconsole for running job and job history should be the same**

If the running job is retired, then Job url is redirected to the history page. To construct the history url, JobTracker maintains the mapping of job id to history file names. The entries from mapping is purged for jobs older than mapreduce.jobtracker.jobhistory.maxage configured value.


---

* [MAPREDUCE-1171](https://issues.apache.org/jira/browse/MAPREDUCE-1171) | *Blocker* | **Lots of fetch failures**

Added two expert level configuration properties.
1. "mapreduce.reduce.shuffle.notify.readerror"  to know whether to send notification to JobTracker after every read error or not. If the configuration is false, read errors are treated similar to connection errors.
2. "mapreduce.reduce.shuffle.maxfetchfailures" to specify the maximum number of the fetch failures after which the failure will be notified to JobTracker.


---

* [MAPREDUCE-1160](https://issues.apache.org/jira/browse/MAPREDUCE-1160) | *Major* | **Two log statements at INFO level fill up jobtracker logs**

Changed some log statements that were filling up jobtracker logs to debug level.


---

* [MAPREDUCE-1158](https://issues.apache.org/jira/browse/MAPREDUCE-1158) | *Major* | **running\_maps is not decremented when the tasks of a job is killed/failed**

Fix Jobtracker running maps/reduces metrics.


---

* [MAPREDUCE-1153](https://issues.apache.org/jira/browse/MAPREDUCE-1153) | *Major* | **Metrics counting tasktrackers and blacklisted tasktrackers are not updated when trackers are decommissioned.**

Update the number of trackers and blacklisted trackers metrics when trackers are decommissioned.


---

* [MAPREDUCE-1143](https://issues.apache.org/jira/browse/MAPREDUCE-1143) | *Blocker* | **runningMapTasks counter is not properly decremented in case of failed Tasks.**

Corrects the behaviour of tasks counters in case of failed tasks.Incorrect counter values can lead to bad scheduling decisions .This jira rectifies the problem by making sure decrement properly happens incase of failed tasks.


---

* [MAPREDUCE-1140](https://issues.apache.org/jira/browse/MAPREDUCE-1140) | *Major* | **Per cache-file refcount can become negative when tasks release distributed-cache files**

Fixed a bug in DistributedCache, to not decrement reference counts for unreferenced files in error conditions.


---

* [MAPREDUCE-1124](https://issues.apache.org/jira/browse/MAPREDUCE-1124) | *Major* | **TestGridmixSubmission fails sometimes**

Fixed errors that caused occasional failure of TestGridmixSubmission, and additionally refactored some Gridmix code.


---

* [MAPREDUCE-1105](https://issues.apache.org/jira/browse/MAPREDUCE-1105) | *Blocker* | **CapacityScheduler: It should be possible to set queue hard-limit beyond it's actual capacity**

Replaced the existing max task limits variables "mapred.capacity-scheduler.queue.\<queue-name\>.max.map.slots" and "mapred.capacity-scheduler.queue.\<queue-name\>.max.reduce.slots"  with  "mapred.capacity-scheduler.queue.\<queue-name\>.maximum-capacity" . 

max task limit variables were used to throttle the queue, i.e, these were the hard limit and not allowing queue to grow further.
maximum-capacity variable defines a limit beyond which a queue cannot use the capacity of the cluster. This provides a means to limit how much excess capacity a queue can use.
 
maximum-capacity variable  behavior is different from max task limit variables, as maximum-capacity is a percentage and it grows and shrinks in absolute terms based on total cluster capacity.Also same maximum-capacity percentage is applied to both map and reduce.


---

* [MAPREDUCE-1103](https://issues.apache.org/jira/browse/MAPREDUCE-1103) | *Major* | **Additional JobTracker metrics**

Add following additional job tracker metrics: 
Reserved{Map, Reduce}Slots
Occupied{Map, Reduce}Slots
Running{Map, Reduce}Tasks
Killed{Map, Reduce}Tasks

FailedJobs
KilledJobs
PrepJobs
RunningJobs

TotalTrackers
BlacklistedTrackers
DecommissionedTrackers


---

* [MAPREDUCE-1098](https://issues.apache.org/jira/browse/MAPREDUCE-1098) | *Major* | **Incorrect synchronization in DistributedCache causes TaskTrackers to freeze up during localization of Cache for tasks.**

Fixed the distributed cache's localizeCache to lock only the uri it is localizing.


---

* [MAPREDUCE-1097](https://issues.apache.org/jira/browse/MAPREDUCE-1097) | *Minor* | **Changes/fixes to support Vertica 3.5**

Adds support for Vertica 3.5 truncate table, deploy\_design and numeric types.


---

* [MAPREDUCE-1090](https://issues.apache.org/jira/browse/MAPREDUCE-1090) | *Major* | **Modify log statement in Tasktracker log related to memory monitoring to include attempt id.**

Modified log statement in task memory monitoring thread to include task attempt id.


---

* [MAPREDUCE-1086](https://issues.apache.org/jira/browse/MAPREDUCE-1086) | *Major* | **hadoop commands in streaming tasks are trying to write to tasktracker's log**

This patch makes TT to set HADOOP\_ROOT\_LOGGER to INFO,TLA by default in the environment of taskjvm and its children.


---

* [MAPREDUCE-1063](https://issues.apache.org/jira/browse/MAPREDUCE-1063) | *Minor* | **Document Gridmix benchmark**

Created new Forrest documentation for Gridmix.


---

* [MAPREDUCE-1062](https://issues.apache.org/jira/browse/MAPREDUCE-1062) | *Major* | **MRReliability test does not work with retired jobs**

Ensure that MRReliability works with retired-jobs feature turned on.


---

* [MAPREDUCE-1048](https://issues.apache.org/jira/browse/MAPREDUCE-1048) | *Major* | **Show total slot usage in cluster summary on jobtracker webui**

Added occupied map/reduce slots and reserved map/reduce slots to the "Cluster Summary" table on jobtracker web ui.


---

* [MAPREDUCE-1030](https://issues.apache.org/jira/browse/MAPREDUCE-1030) | *Blocker* | **Reduce tasks are getting starved in capacity scheduler**

Modified the scheduling logic in capacity scheduler to return a map and a reduce task per heartbeat.


---

* [MAPREDUCE-1028](https://issues.apache.org/jira/browse/MAPREDUCE-1028) | *Blocker* | **Cleanup tasks are scheduled using high memory configuration, leaving tasks in unassigned state.**

Makes taskCleanup tasks to use 1 slot even for high memory jobs.


---

* [MAPREDUCE-1018](https://issues.apache.org/jira/browse/MAPREDUCE-1018) | *Blocker* | **Document changes to the memory management and scheduling model**

Updated documentation related to (1) the changes in the configuration for memory management of tasks and (2) the modifications in scheduling model of CapacityTaskScheduler to include memory requirement by tasks.


---

* [MAPREDUCE-1003](https://issues.apache.org/jira/browse/MAPREDUCE-1003) | *Major* | **trunk build fails when -Declipse.home is set**

Minor changes to HadoopJob.java in eclipse-plugin contrib project to accommodate changes in JobStatus (MAPREDUCE-777)


---

* [MAPREDUCE-975](https://issues.apache.org/jira/browse/MAPREDUCE-975) | *Major* | **Add an API in job client to get the history file url for a given job id**

Adds an API Cluster#getJobHistoryUrl(JobID jobId) to get the history url for a given job id. The API does not check for the validity of job id or existence of the history file. It just constructs the url based on history folder, job  id and the current user.


---

* [MAPREDUCE-967](https://issues.apache.org/jira/browse/MAPREDUCE-967) | *Major* | **TaskTracker does not need to fully unjar job jars**

For efficiency, TaskTrackers no longer unjar the job jar into the job cache directory. Users who previously depended on this functionality for shipping non-code dependencies can use the undocumented configuration parameter "mapreduce.job.jar.unpack.pattern" to cause specific jar contents to be unpacked.


---

* [MAPREDUCE-964](https://issues.apache.org/jira/browse/MAPREDUCE-964) | *Critical* | **Inaccurate values in jobSummary logs**

Updates to task timing information were fixed in some code paths that led to inconsistent metering of task run times. Also, checks were added to guard against inconsistencies and record information about state if they should happen to occur.


---

* [MAPREDUCE-963](https://issues.apache.org/jira/browse/MAPREDUCE-963) | *Major* | **mapred's FileAlreadyExistsException should be deprecated in favor of hadoop-common's one.**

Deprecate o.a.h.mapred.FileAlreadyExistsException and replace it with o.a.h.fs.FileAlreadyExistsException.


---

* [MAPREDUCE-962](https://issues.apache.org/jira/browse/MAPREDUCE-962) | *Major* | **NPE in ProcfsBasedProcessTree.destroy()**

Fixes an issue of NPE in ProcfsBasedProcessTree in a corner case.


---

* [MAPREDUCE-954](https://issues.apache.org/jira/browse/MAPREDUCE-954) | *Major* | **The new interface's Context objects should be interfaces**

Changed Map-Reduce context objects to be interfaces.


---

* [MAPREDUCE-947](https://issues.apache.org/jira/browse/MAPREDUCE-947) | *Major* | **OutputCommitter should have an abortJob method**

Introduced abortJob() method in OutputCommitter which will be invoked when the job fails or is killed. By default it invokes OutputCommitter.cleanupJob(). Deprecated OutputCommitter.cleanupJob() and introduced OutputCommitter.commitJob() method which will be invoked for successful jobs. Also a \_SUCCESS file is created in the output folder for successful jobs. A configuration parameter mapreduce.fileoutputcommitter.marksuccessfuljobs can be set to false to disable creation of \_SUCCESS file, or to true to enable creation of the \_SUCCESS file.


---

* [MAPREDUCE-943](https://issues.apache.org/jira/browse/MAPREDUCE-943) | *Major* | **TestNodeRefresh timesout occasionally**

TestNodeRefresh timed out as the code to do with node refresh got removed. This patch removes the testcase.


---

* [MAPREDUCE-913](https://issues.apache.org/jira/browse/MAPREDUCE-913) | *Blocker* | **TaskRunner crashes with NPE resulting in held up slots, UNINITIALIZED tasks and hung TaskTracker**

Fixed TaskTracker to avoid hung and unusable slots when TaskRunner crashes with NPE and leaves tasks in UNINITIALIZED state for ever.


---

* [MAPREDUCE-899](https://issues.apache.org/jira/browse/MAPREDUCE-899) | *Major* | **When using LinuxTaskController, localized files may become accessible to unintended users if permissions are misconfigured.**

Added configuration "mapreduce.tasktracker.group", a group name to which TaskTracker belongs. When LinuxTaskController is used, task-controller binary's group owner should be this group. The same should be specified in task-controller.cfg also.


---

* [MAPREDUCE-895](https://issues.apache.org/jira/browse/MAPREDUCE-895) | *Major* | **FileSystem::ListStatus will now throw FileNotFoundException, MapRed needs updated**

The semantics for dealing with non-existent paths passed to FileSystem::listStatus() were updated and solidified in HADOOP-6201 and HDFS-538.  Existing code within MapReduce that relied on the previous behavior of some FileSystem implementations of returning null has been updated to catch or propagate a FileNotFoundException, per the method's contract.


---

* [MAPREDUCE-893](https://issues.apache.org/jira/browse/MAPREDUCE-893) | *Major* | **Provide an ability to refresh queue configuration without restart.**

Extended the framework's refresh-queue mechanism to support refresh of scheduler specific queue properties and implemented this refresh operation for some of the capacity scheduler properties. With this feature, one can refresh some of the capacity-scheduler's queue related properties - queue capacities, user-limits per queue, max map/reduce capacity and max-jobs per user to initialize while the system is running and without restarting JT. Even after this, some features like changing enable/disable priorities, adding/removing queues are not supported in capacity-scheduler.


---

* [MAPREDUCE-890](https://issues.apache.org/jira/browse/MAPREDUCE-890) | *Blocker* | **After HADOOP-4491, the user who started mapred system is not able to run job.**

Fixed a bug that failed jobs that are run by the same user who started the mapreduce system(cluster).


---

* [MAPREDUCE-873](https://issues.apache.org/jira/browse/MAPREDUCE-873) | *Major* | **Simplify Job Recovery**

Simplifies job recovery. On jobtracker restart, incomplete jobs are resubmitted and all tasks reexecute.
This JIRA removes a public constructor in JobInProgress.


---

* [MAPREDUCE-871](https://issues.apache.org/jira/browse/MAPREDUCE-871) | *Major* | **Job/Task local files have incorrect group ownership set by LinuxTaskController binary**

Fixed LinuxTaskController binary so that permissions of local files on TT are set correctly: user owned by the job-owner and group-owned by the group owner of the binary and \_not\_ the primary group of the TaskTracker.


---

* [MAPREDUCE-870](https://issues.apache.org/jira/browse/MAPREDUCE-870) | *Major* | **Clean up the job Retire code**

Removed the Job Retire thread and the associated configuration parameters. Job is purged from memory as soon as the history file is copied to HDFS. Only JobStatus object is retained in the retired jobs cache.


---

* [MAPREDUCE-862](https://issues.apache.org/jira/browse/MAPREDUCE-862) | *Major* | **Modify UI to support a hierarchy of queues**

- The command line of hadoop queue -list and -info was changed to support hierarchical queues. So, they would now print information about child queues, wherever relevant.
- The Web UI of the JobTracker was changed to list queues and queue information in a separate page.


---

* [MAPREDUCE-861](https://issues.apache.org/jira/browse/MAPREDUCE-861) | *Major* | **Modify queue configuration format and parsing to support a hierarchy of queues.**

Added support for hierarchical queues in the Map/Reduce framework with the following changes:
- mapred-queues.xml is modified to a new XML template as mentioned in the JIRA.
- Modified JobQueueInfo to contain a handle to child queues.
- Added new APIs in the client to get 'root' queues, so that the entire hierarchy of queues can be iterated.
-Added new APIs to get the child queues for a given queue .


---

* [MAPREDUCE-856](https://issues.apache.org/jira/browse/MAPREDUCE-856) | *Major* | **Localized files from DistributedCache should have right access-control**

Fixed TaskTracker and related classes so as to set correct and most restrictive access control for DistributedCache files/archives.
 - To do this, it changed the directory structure of per-job local files on a TaskTracker to the following:
$mapred.local.dir
   `-- taskTracker
        `-- $user
               \|- distcache
               `-- jobcache
 - Distributed cache files/archives are now user-owned by the job-owner and the group-owned by the special group-owner of the task-controller binary. The files/archives are set most private permissions possible, and as soon as possible, immediately after the files/dirs are first localized on the TT.
 - As depicted by the new directory structure, a directory corresponding to each user is created on each TT when that particular user's first task are assigned to the corresponding TT. These user directories remain on the TT forever are not cleaned when unused, which is targeted to be fixed via MAPREDUCE-1019.
 - The distributed cache files are now accessible \_only\_ by the user who first localized them. Sharing of these files across users is no longer possible, but is targeted for future versions via MAPREDUCE-744.


---

* [MAPREDUCE-852](https://issues.apache.org/jira/browse/MAPREDUCE-852) | *Major* | **ExampleDriver is incorrectly set as a Main-Class in tools in build.xml**

Changed the target name from "tools-jar" to "tools" in build.xml.


---

* [MAPREDUCE-849](https://issues.apache.org/jira/browse/MAPREDUCE-849) | *Major* | **Renaming of configuration property names in mapreduce**

Rename and categorize configuration keys into - cluster, jobtracker, tasktracker, job, client. Constants are defined for all keys in java and code is changed to use constants instead of direct strings. All old keys are deprecated except of examples and tests. The change is incompatible because support for old keys is not provided for config keys in examples.


---

* [MAPREDUCE-848](https://issues.apache.org/jira/browse/MAPREDUCE-848) | *Major* | **TestCapacityScheduler is failing**

MAPREDUCE-805 changed the way the job was initialized. Capacity schedulers testcases were not modified as part of MAPREDUCE-805. This patch fixes this bug.


---

* [MAPREDUCE-845](https://issues.apache.org/jira/browse/MAPREDUCE-845) | *Minor* | **build.xml hard codes findbugs heap size, in some configurations 512M is insufficient to successfully build**

Changes the heapsize for findbugs to a parameter which can be changed on the build command line.


---

* [MAPREDUCE-842](https://issues.apache.org/jira/browse/MAPREDUCE-842) | *Major* | **Per-job local data on the TaskTracker node should have right access-control**

Modified TaskTracker and related classes so that per-job local data on the TaskTracker node has right access-control. Important changes:
 - All files/directories of the job on the TaskTracker are now user-owned by the job-owner and group-owner by a special TaskTracker's group.
 - The permissions of the file/directories are set to the most restrictive permissions possible.
 - Files/dirs shareable by all tasks of the job on this TT are set proper access control as soon as possible, i.e immediately after job-localization and those that are private to a single task are set access control after the corresponding task's localization.
 - Also fixes MAPREDUCE-131 which is related to a bug because of which tasks hang when the taskcontroller.cfg has multiple entries for mapred.local.dir
 - A new configuration entry hadoop.log.dir corresponding to the hadoop.log.dir in TT's configuration is now needed in task-controller.cfg so as to support restricted access control for userlogs of the tasks on the TaskTracker.


---

* [MAPREDUCE-830](https://issues.apache.org/jira/browse/MAPREDUCE-830) | *Major* | **Providing BZip2 splitting support for Text data**

Splitting support for BZip2 Text data


---

* [MAPREDUCE-824](https://issues.apache.org/jira/browse/MAPREDUCE-824) | *Major* | **Support a hierarchy of queues in the capacity scheduler**

Support hierarchical queues in the CapacityScheduler to allow for more predictable sharing of cluster resources.


---

* [MAPREDUCE-817](https://issues.apache.org/jira/browse/MAPREDUCE-817) | *Major* | **Add a cache for retired jobs with minimal job info and provide a way to access history file url**

Provides a way to configure the cache of JobStatus objects for the retired jobs. 
Adds an API in RunningJob to access history file url. 
Adds a LRU based cache for job history files loaded in memory when accessed via JobTracker web UI.
Adds Retired Jobs table on the Jobtracker UI. The job move from Running to Completed/Failed table. Then job move to Retired table when it is purged from memory. The Retired table shows last 100 retired jobs. The Completed/Failed jobs table are only shown if there are non-zero jobs in the table.


---

* [MAPREDUCE-814](https://issues.apache.org/jira/browse/MAPREDUCE-814) | *Major* | **Move completed Job history files to HDFS**

Provides an ability to move completed job history files to a HDFS location via  configuring "mapred.job.tracker.history.completed.location". If the directory location does not already exist, it would be created by jobtracker.


---

* [MAPREDUCE-809](https://issues.apache.org/jira/browse/MAPREDUCE-809) | *Major* | **Job summary logs show status of completed jobs as RUNNING**

Fix job-summary logs to correctly record final status of FAILED and KILLED jobs.


---

* [MAPREDUCE-800](https://issues.apache.org/jira/browse/MAPREDUCE-800) | *Major* | **MRUnit should support the new API**

Support new API in unit tests developed with MRUnit.


---

* [MAPREDUCE-798](https://issues.apache.org/jira/browse/MAPREDUCE-798) | *Major* | **MRUnit should be able to test a succession of MapReduce passes**

Add PipelineMapReduceDriver to MRUnit to support testing a pipeline of MapReduce passes


---

* [MAPREDUCE-797](https://issues.apache.org/jira/browse/MAPREDUCE-797) | *Major* | **MRUnit MapReduceDriver should support combiners**

Add Combiner support to MapReduceDriver in MRUnit


---

* [MAPREDUCE-793](https://issues.apache.org/jira/browse/MAPREDUCE-793) | *Major* | **Create a new test that consolidates a few tests to be included in the commit-test list**

Creates a new test to test several miscellaneous functionality at one shot instead of running a job for each, to be used as a fast test for the ant commit-tests target.


---

* [MAPREDUCE-788](https://issues.apache.org/jira/browse/MAPREDUCE-788) | *Major* | **Modify gridmix2 to use new api.**

Modifies Gridmix2 to use the new Map/Reduce API


---

* [MAPREDUCE-787](https://issues.apache.org/jira/browse/MAPREDUCE-787) | *Major* | **-files, -archives should honor user given symlink path**

Fix JobSubmitter to honor user given symlink in the path.


---

* [MAPREDUCE-785](https://issues.apache.org/jira/browse/MAPREDUCE-785) | *Major* | **Refactor TestReduceFetchFromPartialMem into a separate test**

Moves TestReduceFetchFromPartialMem out of TestReduceFetch into a separate test to enable it to be included in the commit-tests target.


---

* [MAPREDUCE-784](https://issues.apache.org/jira/browse/MAPREDUCE-784) | *Major* | **Modify TestUserDefinedCounters to use LocalJobRunner instead of MiniMR**

Modifies TestUserDefinedCounters to use LocalJobRunner instead of using MiniMR cluster


---

* [MAPREDUCE-777](https://issues.apache.org/jira/browse/MAPREDUCE-777) | *Major* | **A method for finding and tracking jobs from the new API**

Enhance the Context Objects api to add features to find and track jobs.


---

* [MAPREDUCE-775](https://issues.apache.org/jira/browse/MAPREDUCE-775) | *Major* | **Add input/output formatters for Vertica clustered ADBMS.**

Add native and streaming support for Vertica as an input or output format taking advantage of parallel read and write properties of the DBMS.


---

* [MAPREDUCE-773](https://issues.apache.org/jira/browse/MAPREDUCE-773) | *Major* | **LineRecordReader can report non-zero progress while it is processing a compressed stream**

Modifies LineRecordReader to report an approximate progress, instead of just returning 0, when using compressed streams.


---

* [MAPREDUCE-772](https://issues.apache.org/jira/browse/MAPREDUCE-772) | *Major* | **Chaging LineRecordReader algo so that it does not need to skip backwards in the stream**

**WARNING: No release note provided for this incompatible change.**


---

* [MAPREDUCE-768](https://issues.apache.org/jira/browse/MAPREDUCE-768) | *Major* | **Configuration information should generate dump in a standard format.**

Provides an ability to dump jobtracker configuration in JSON format to standard output and exits.
To dump, use hadoop jobtracker -dumpConfiguration
The format of the dump is {"properties":[{"key":\<key\>,"value":\<value\>,"isFinal":\<true/false\>,"resource" : \<resource\>}] }


---

* [MAPREDUCE-766](https://issues.apache.org/jira/browse/MAPREDUCE-766) | *Major* | **Enhance -list-blacklisted-trackers to display host name, blacklisted reason and blacklist report.**

Enhanced -list-blacklisted-trackers to include the reason for blacklisting a node. Modified JobSubmissionProtocol's version as the ClusterStatus is changed to have a new class. The format of the -list-blacklisted-trackers command line interface has also changed to show the reason.


---

* [MAPREDUCE-760](https://issues.apache.org/jira/browse/MAPREDUCE-760) | *Major* | **TestNodeRefresh might not work as expected**

TestNodeRefresh waits for the newly added tracker to join before starting the testing.


---

* [MAPREDUCE-754](https://issues.apache.org/jira/browse/MAPREDUCE-754) | *Minor* | **NPE in expiry thread when a TT is lost**

Fixes the NPE in 'refreshNodes', ExpiryTracker thread and heartbeat. NPE occurred in the following cases
- a blacklisted tracker is either decommissioned or expires.
- a lost tracker gets blacklisted


---

* [MAPREDUCE-744](https://issues.apache.org/jira/browse/MAPREDUCE-744) | *Major* | **Support in DistributedCache to share cache files with other users after HADOOP-4493**

Fixed DistributedCache to support sharing of the local cache files with other users on the same TaskTracker. The cache files are checked at the client side for public/private access on the file system, and that information is passed in the configuration. The TaskTrackers look at the configuration for each file during task localization, and, if the file was public on the filesystem, they are localized to a common space for sharing by all users' tasks on the TaskTracker. Else the file is localized to the user's private directory on the local filesystem.


---

* [MAPREDUCE-740](https://issues.apache.org/jira/browse/MAPREDUCE-740) | *Major* | **Provide summary information per job once a job is finished.**

Log a job-summary at the end of a job, while allowing it to be configured to use a custom appender if desired.


---

* [MAPREDUCE-739](https://issues.apache.org/jira/browse/MAPREDUCE-739) | *Major* | **Allow relative paths to be created inside archives.**

Allow creating archives with relative paths  with a -p option on the command line.


---

* [MAPREDUCE-732](https://issues.apache.org/jira/browse/MAPREDUCE-732) | *Minor* | **node health check script should not log "UNHEALTHY" status for every heartbeat in INFO mode**

Changed log level of addition of blacklisted reason in the JobTracker log to debug instead of INFO


---

* [MAPREDUCE-717](https://issues.apache.org/jira/browse/MAPREDUCE-717) | *Major* | **Fix some corner case issues in speculative execution (post hadoop-2141)**

Fixes some edge cases while using speculative execution


---

* [MAPREDUCE-711](https://issues.apache.org/jira/browse/MAPREDUCE-711) | *Major* | **Move Distributed Cache from Common to Map/Reduce**

- Removed distributed cache classes and package from the Common project. 
- Added the same to the mapreduce project. 
- This will mean that users using Distributed Cache will now necessarily need the mapreduce jar in Hadoop 0.21.
- Modified the package name to o.a.h.mapreduce.filecache from o.a.h.filecache and deprecated the old package name.


---

* [MAPREDUCE-707](https://issues.apache.org/jira/browse/MAPREDUCE-707) | *Trivial* | **Provide a jobconf property for explicitly assigning a job to a pool**

add mapred.fairscheduler.pool property to define which pool a job belongs to.


---

* [MAPREDUCE-706](https://issues.apache.org/jira/browse/MAPREDUCE-706) | *Major* | **Support for FIFO pools in the fair scheduler**

Support for FIFO pools added to the Fair Scheduler.


---

* [MAPREDUCE-701](https://issues.apache.org/jira/browse/MAPREDUCE-701) | *Minor* | **Make TestRackAwareTaskPlacement a unit test**

Modifies TestRackAwareTaskPlacement to not use MiniMR/DFS Cluster for testing, thereby making it a unit test


---

* [MAPREDUCE-698](https://issues.apache.org/jira/browse/MAPREDUCE-698) | *Major* | **Per-pool task limits for the fair scheduler**

Per-pool map and reduce caps for Fair Scheduler.


---

* [MAPREDUCE-686](https://issues.apache.org/jira/browse/MAPREDUCE-686) | *Major* | **Move TestSpeculativeExecution.Fake\* into a separate class so that it can be used by other tests also**

Consolidate the Mock Objects used for testing in a separate class(FakeObjectUtiltities) to ease re-usability


---

* [MAPREDUCE-683](https://issues.apache.org/jira/browse/MAPREDUCE-683) | *Major* | **TestJobTrackerRestart fails with Map task completion events ordering mismatch**

TestJobTrackerRestart failed because of stale filemanager cache (which was created once per jvm). This patch makes sure that the filemanager is inited upon every JobHistory.init() and hence upon every restart. Note that this wont happen in production as upon a restart the new jobtracker will start in a new jvm and hence a new cache will be created.


---

* [MAPREDUCE-682](https://issues.apache.org/jira/browse/MAPREDUCE-682) | *Major* | **Reserved tasktrackers should be removed when a node is globally blacklisted**

Jobtracker was modified to cleanup reservations created on tasktracker nodes to support high RAM jobs, when the nodes are blacklisted.


---

* [MAPREDUCE-679](https://issues.apache.org/jira/browse/MAPREDUCE-679) | *Major* | **XML-based metrics as JSP servlet for JobTracker**

Added XML-based JobTracker status JSP page for metrics reporting


---

* [MAPREDUCE-677](https://issues.apache.org/jira/browse/MAPREDUCE-677) | *Major* | **TestNodeRefresh timesout**

TestNodeRefresh sometimes timed out. This happened because the test started a MR cluster with 2 trackers and ran a half-waiting-mapper job. Tasks that have id \> total-maps/2 wait for a signal. Because of 2 trackers, the tasks got scheduled out of order (locality) and hence the job got stuck. The fix is to start only one tracker and then add a new tracker later.


---

* [MAPREDUCE-676](https://issues.apache.org/jira/browse/MAPREDUCE-676) | *Major* | **Existing diagnostic rules fail for MAP ONLY jobs**

hadoop vaidya counter names LOCAL\_BYTES\_READ and LOCAL\_BYTES\_WRITTEN  are changed to respectively FILE\_BYTES\_READ, FILE\_BYTES\_WRITTEN as per current hadoop counter names.


---

* [MAPREDUCE-670](https://issues.apache.org/jira/browse/MAPREDUCE-670) | *Major* | ** Create target for 10 minute patch test build for mapreduce**

Added a new target 'test-commit' to the build.xml file which runs tests specified in the file src/test/commit-tests. The tests specified in src/test/commit-tests should provide maximum coverage and all the tests should run within 10mins.


---

* [MAPREDUCE-656](https://issues.apache.org/jira/browse/MAPREDUCE-656) | *Major* | **Change org.apache.hadoop.mapred.SequenceFile\* classes to use new api**

Ports the SequenceFile\* classes to the new Map/Reduce API


---

* [MAPREDUCE-655](https://issues.apache.org/jira/browse/MAPREDUCE-655) | *Major* | **Change KeyValueLineRecordReader and KeyValueTextInputFormat to use new api.**

Ports KeyValueLineRecordReader and KeyValueTextInputFormat the new Map/Reduce API


---

* [MAPREDUCE-646](https://issues.apache.org/jira/browse/MAPREDUCE-646) | *Major* | **distcp should place the file distcp\_src\_files in distributed cache**

Patch increases the replication factor of \_distcp\_src\_files to sqrt(min(maxMapsOnCluster, totalMapsInThisJob)) sothat many maps won't access the same replica of the file \_distcp\_src\_files at the same time.


---

* [MAPREDUCE-642](https://issues.apache.org/jira/browse/MAPREDUCE-642) | *Major* | **distcp could have an option to preserve the full source path**

DistCp now has a "-basedir" option that allows you to set the sufix of the source path that will be copied to the destination.


---

* [MAPREDUCE-632](https://issues.apache.org/jira/browse/MAPREDUCE-632) | *Major* | **Merge TestCustomOutputCommitter with TestCommandLineJobSubmission**

Modifies TestCommandLineJobSubmission to add a test for testing custom output committer and removes TestCustomOutputCommitter


---

* [MAPREDUCE-630](https://issues.apache.org/jira/browse/MAPREDUCE-630) | *Minor* | **TestKillCompletedJob can be modified to improve execution times**

Modifies TestKillCompletedJob to rid of its dependence on MiniMR clusters and makes it a unit test


---

* [MAPREDUCE-627](https://issues.apache.org/jira/browse/MAPREDUCE-627) | *Minor* | **Modify TestTrackerBlacklistAcrossJobs to improve execution time**

Modifies TestTrackerBlacklistAcrossJobs to use mock objects for testing instead of running a full-fledged job using MiniMR clusters.


---

* [MAPREDUCE-626](https://issues.apache.org/jira/browse/MAPREDUCE-626) | *Minor* | **Modify TestLostTracker to improve execution time**

Modifies TestLostTracker to use Mock objects instead of running full-fledged jobs using the MiniMR clusters.


---

* [MAPREDUCE-625](https://issues.apache.org/jira/browse/MAPREDUCE-625) | *Minor* | **Modify TestTaskLimits to improve execution time**

Modifies TestTaskLimits to do unit testing instead of running jobs using MR clusters


---

* [MAPREDUCE-551](https://issues.apache.org/jira/browse/MAPREDUCE-551) | *Major* | **Add preemption to the fair scheduler**

Added support for preemption in the fair scheduler. The new configuration options for enabling this are described in the fair scheduler documentation.


---

* [MAPREDUCE-532](https://issues.apache.org/jira/browse/MAPREDUCE-532) | *Major* | **Allow admins of the Capacity Scheduler to set a hard-limit on the capacity of a queue**

Provided ability in the capacity scheduler to limit the number of slots that can be concurrently used per queue at any given time.


---

* [MAPREDUCE-516](https://issues.apache.org/jira/browse/MAPREDUCE-516) | *Major* | **Fix the 'cluster drain' problem in the Capacity Scheduler wrt High RAM Jobs**

**WARNING: No release note provided for this incompatible change.**


---

* [MAPREDUCE-502](https://issues.apache.org/jira/browse/MAPREDUCE-502) | *Major* | **Allow jobtracker to be configured with zero completed jobs in memory**

If the number of jobs per user exceeded mapred.jobtracker.completeuserjobs.maximum then the job was flushed out of the jobtracker's memory after the job finishes min-time (hardcoded to 1 min). This caused jobclient's fail with NPE. In this patch the min-time to retain a job is made configurable (mapred.jobtracker.retirejob.interval.min).


---

* [MAPREDUCE-479](https://issues.apache.org/jira/browse/MAPREDUCE-479) | *Minor* | **Add reduce ID to shuffle clienttrace**

Adds Reduce Attempt ID to ClientTrace log messages, and adds Reduce Attempt ID to HTTP query string sent to mapOutputServlet. Extracts partition number from attempt ID.


---

* [MAPREDUCE-476](https://issues.apache.org/jira/browse/MAPREDUCE-476) | *Minor* | **extend DistributedCache to work locally (LocalJobRunner)**

Extended DistributedCache to work with LocalJobRunner.


---

* [MAPREDUCE-467](https://issues.apache.org/jira/browse/MAPREDUCE-467) | *Major* | **Collect information about number of tasks succeeded / total per time unit for a tasktracker.**

Provide ability to collect statistics about tasks completed and succeeded for each tracker in time windows. The statistics is available on the jobtrackers' nodes UI page.


---

* [MAPREDUCE-463](https://issues.apache.org/jira/browse/MAPREDUCE-463) | *Major* | **The job setup and cleanup tasks should be optional**

Added Configuration property "mapred.committer.job.setup.cleanup.needed" to specify whether job-setup and job-cleanup is needed for the job output committer. The default value is true. 
Added Job.setJobSetupCleanupNeeded and JobContext.getJobSetupCleanupNeeded api for setting/getting the configuration.
If the configuration is set to false, no setup or cleanup will be done.


---

* [MAPREDUCE-416](https://issues.apache.org/jira/browse/MAPREDUCE-416) | *Major* | **Move the completed jobs' history files to a DONE subdirectory inside the configured history directory**

Once the job is done, the history file and associated conf file is moved to history.folder/done folder. This is done to avoid garbling the running jobs'  folder and the framework no longer gets affected with the files in the done folder. This helps in 2 was
1) ls on running folder (recovery) is faster with less files
2) changes in running folder results into FileNotFoundException. 


So with existing code, the best way to keep the running folder clean is to note the id's of running job and then move files that are not in this list to the done folder. Note that on an avg there will be 2 files in the history folder namely
1) job history file
2) conf file. 

With restart, there might be more than 2 files, mostly the extra conf files. In such a case keep the oldest conf file (based on timestamp) and delete the rest. Note that this its better to do this when the jobtracker is down.


---

* [MAPREDUCE-408](https://issues.apache.org/jira/browse/MAPREDUCE-408) | *Major* | **TestKillSubProcesses fails with assertion failure sometimes**

Fixed a bug in the testcase TestKillSubProcesses.


---

* [MAPREDUCE-375](https://issues.apache.org/jira/browse/MAPREDUCE-375) | *Major* | ** Change org.apache.hadoop.mapred.lib.NLineInputFormat and org.apache.hadoop.mapred.MapFileOutputFormat to use new api.**

Ports NLineInputFormat and MapFileOutputFormat to the new Map/Reduce API


---

* [MAPREDUCE-373](https://issues.apache.org/jira/browse/MAPREDUCE-373) | *Major* | **Change org.apache.hadoop.mapred.lib. FieldSelectionMapReduce to use new api.**

Ports FieldSelectionMapReduce to the new Map/Reduce API


---

* [MAPREDUCE-371](https://issues.apache.org/jira/browse/MAPREDUCE-371) | *Major* | **Change org.apache.hadoop.mapred.lib.KeyFieldBasedComparator and org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner to use new api**

Ports KeyFieldBasedComparator and KeyFieldBasedPartitioner to the new Map/Reduce API


---

* [MAPREDUCE-370](https://issues.apache.org/jira/browse/MAPREDUCE-370) | *Major* | **Change org.apache.hadoop.mapred.lib.MultipleOutputs to use new api.**

Ports MultipleOutputs to the new Map/Reduce API


---

* [MAPREDUCE-369](https://issues.apache.org/jira/browse/MAPREDUCE-369) | *Major* | **Change org.apache.hadoop.mapred.lib.MultipleInputs to use new api.**

Patch that ports MultipleInputs, DelegatingInputFormat, DelegatingMapper and TaggedInputSplit to the new Map/Reduce API


---

* [MAPREDUCE-358](https://issues.apache.org/jira/browse/MAPREDUCE-358) | *Major* | **Change org.apache.hadoop.examples. AggregateWordCount and  org.apache.hadoop.examples.AggregateWordHistogram to use new mapreduce api.**

Modifies AggregateWordCount and AggregateWordHistogram examples to use the new Map/Reduce API


---

* [MAPREDUCE-355](https://issues.apache.org/jira/browse/MAPREDUCE-355) | *Major* | **Change org.apache.hadoop.mapred.join to use new api**

Ports the mapred.join library to the new Map/Reduce API


---

* [MAPREDUCE-353](https://issues.apache.org/jira/browse/MAPREDUCE-353) | *Major* | **Allow shuffle read and connection timeouts to be configurable**

Expert level config properties mapred.shuffle.connect.timeout and mapred.shuffle.read.timeout that are to be used at cluster level are added by this patch.


---

* [MAPREDUCE-336](https://issues.apache.org/jira/browse/MAPREDUCE-336) | *Major* | **The logging level of the tasks should be configurable by the job**

Allow logging level of map/reduce tasks to be configurable.
Configuration changes:
  add mapred.map.child.log.level
  add mapred.reduce.child.log.level


---

* [MAPREDUCE-318](https://issues.apache.org/jira/browse/MAPREDUCE-318) | *Major* | **Refactor reduce shuffle code**

Refactors shuffle code out of ReduceTask into separate classes in a new package(org.apache.hadoop.mapreduce.task.reduce)
Incorporates MAPREDUCE-240, batches up several map output files from a TT to a reducer in a single transfer
Introduces new Shuffle counters to keep track of shuffle errors


---

* [MAPREDUCE-284](https://issues.apache.org/jira/browse/MAPREDUCE-284) | *Major* | **Improvements to RPC between Child and TaskTracker**

Enables tcp.nodelay for RPC between Child and TaskTracker.


---

* [MAPREDUCE-277](https://issues.apache.org/jira/browse/MAPREDUCE-277) | *Blocker* | **Job history counters should be avaible on the UI.**

Modifies job history parser and Web UI to display counters


---

* [MAPREDUCE-270](https://issues.apache.org/jira/browse/MAPREDUCE-270) | *Major* | **TaskTracker could send an out-of-band heartbeat when the last running map/reduce completes**

Introduced an option to allow tasktrackers to send an out of band heartbeat on task-completion to improve job latency. A new configuration option mapreduce.tasktracker.outofband.heartbeat is defined, which can be enabled to send this heartbeat.


---

* [MAPREDUCE-245](https://issues.apache.org/jira/browse/MAPREDUCE-245) | *Major* | **Job and JobControl classes should return interfaces rather than implementations**

**WARNING: No release note provided for this incompatible change.**


---

* [MAPREDUCE-211](https://issues.apache.org/jira/browse/MAPREDUCE-211) | *Major* | **Provide a node health check script and run it periodically to check the node health status**

Provides ability to run a health check script on the tasktracker nodes and blacklist nodes if they are unhealthy.


---

* [MAPREDUCE-157](https://issues.apache.org/jira/browse/MAPREDUCE-157) | *Major* | **Job History log file format is not friendly for external tools.**

Changes the Job History file format to use JSON.  
Simplifies the Job History Parsing logic
Removes duplication of code between HistoryViewer and the JSP files
History Files are now named as JobID\_user
Introduces a new cluster level configuration "mapreduce.cluster.jobhistory.maxage" for configuring the amount of time history files are kept before getting cleaned up
The configuration "hadoop.job.history.user.location" is no longer supported.


---

* [MAPREDUCE-153](https://issues.apache.org/jira/browse/MAPREDUCE-153) | *Major* | **TestJobInProgressListener sometimes timesout**

Only one MR cluster is brought up and hence there is no scope of jobid clashing.


---

* [MAPREDUCE-144](https://issues.apache.org/jira/browse/MAPREDUCE-144) | *Major* | **TaskMemoryManager should log process-tree's status while killing tasks.**

Modified TaskMemoryManager so that it logs a map/reduce task's process-tree's status just before it is killed when it grows out of its configured memory limits. The log dump is in the format " \|- PID PPID PGRPID SESSID CMD\_NAME VMEM\_USAGE(BYTES) FULL\_CMD\_LINE".

This is useful for debugging the cause for a map/reduce task and it's corresponding process-tree to be killed by the TaskMemoryManager.



