
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
# Apache Hadoop  3.0.0-alpha1 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HDFS-46](https://issues.apache.org/jira/browse/HDFS-46) | *Major* | **The namespace quota of root directory should not be Integer.MAX\_VALUE**

Change default namespace quota of root directory from Integer.MAX\_VALUE to Long.MAX\_VALUE.


---

* [HADOOP-8124](https://issues.apache.org/jira/browse/HADOOP-8124) | *Major* | **Remove the deprecated Syncable.sync() method**

Remove the deprecated FSDataOutputStream constructor, FSDataOutputStream.sync() and Syncable.sync().


---

* [HDFS-3034](https://issues.apache.org/jira/browse/HDFS-3034) | *Major* | **Remove the deprecated Syncable.sync() method**

Remove the deprecated DFSOutputStream.sync() method.


---

* [HADOOP-7659](https://issues.apache.org/jira/browse/HADOOP-7659) | *Minor* | **fs -getmerge isn't guaranteed to work well over non-HDFS filesystems**

Documented that the "fs -getmerge" shell command may not work properly over non HDFS-filesystem implementations due to platform-varying file list ordering.


---

* [HADOOP-8776](https://issues.apache.org/jira/browse/HADOOP-8776) | *Minor* | **Provide an option in test-patch that can enable / disable compiling native code**

test-patch.sh adds a new option "--build-native". When set to false native
components are not built. When set to true native components are built. The
default value is true.


---

* [HDFS-5079](https://issues.apache.org/jira/browse/HDFS-5079) | *Major* | **Cleaning up NNHAStatusHeartbeat.State DatanodeProtocolProtos.**

This change affects wire-compatibility of the NameNode/DataNode heartbeat protocol.  Only present in 3.0.0-alpha1. It has been reverted before 3.0.0-alpha2


---

* [HDFS-5570](https://issues.apache.org/jira/browse/HDFS-5570) | *Major* | **Deprecate hftp / hsftp and replace them with webhdfs / swebhdfs**

Support for hftp and hsftp has been removed.  They have superseded by webhdfs  and swebhdfs.


---

* [HADOOP-10485](https://issues.apache.org/jira/browse/HADOOP-10485) | *Major* | **Remove dead classes in hadoop-streaming**

Deprecated and unused classes in the org.apache.hadoop.record package have been removed from hadoop-streaming.


---

* [HDFS-6246](https://issues.apache.org/jira/browse/HDFS-6246) | *Minor* | **Remove 'dfs.support.append' flag from trunk code**

Appends in HDFS can no longer be disabled.


---

* [HADOOP-10474](https://issues.apache.org/jira/browse/HADOOP-10474) | *Major* | **Move o.a.h.record to hadoop-streaming**

The classes in org.apache.hadoop.record are moved from hadoop-common to a new hadoop-streaming artifact within the hadoop-tools module.


---

* [HADOOP-9902](https://issues.apache.org/jira/browse/HADOOP-9902) | *Major* | **Shell script rewrite**

<!-- markdown -->
The Hadoop shell scripts have been rewritten to fix many long standing bugs and include some new features.  While an eye has been kept towards compatibility, some changes may break existing installations.

INCOMPATIBLE CHANGES:

* The pid and out files for secure daemons have been renamed to include the appropriate ${HADOOP\_IDENT\_STR}.  This should allow, with proper configurations in place, for multiple versions of the same secure daemon to run on a host. Additionally, pid files are now created when daemons are run in interactive mode.  This will also prevent the accidental starting of two daemons with the same configuration prior to launching java (i.e., "fast fail" without having to wait for socket opening).
* All Hadoop shell script subsystems now execute hadoop-env.sh, which allows for all of the environment variables to be in one location.  This was not the case previously.
* The default content of *-env.sh has been significantly altered, with the majority of defaults moved into more protected areas inside the code. Additionally, these files do not auto-append anymore; setting a variable on the command line prior to calling a shell command must contain the entire content, not just any extra settings.  This brings Hadoop more in-line with the vast majority of other software packages.
* All HDFS\_\*, YARN\_\*, and MAPRED\_\* environment variables act as overrides to their equivalent HADOOP\_\* environment variables when 'hdfs', 'yarn', 'mapred', and related commands are executed. Previously, these were separated out which meant a significant amount of duplication of common settings.  
* hdfs-config.sh and hdfs-config.cmd were inadvertently duplicated into libexec and sbin.  The sbin versions have been removed.
* The log4j settings forcibly set by some *-daemon.sh commands have been removed.  These settings are now configurable in the \*-env.sh files via \*\_OPT. 
* Support for various undocumented YARN log4j.properties files has been removed.
* Support for ${HADOOP\_MASTER} and the related rsync code have been removed.
* The undocumented and unused yarn.id.str Java property has been removed.
* The unused yarn.policy.file Java property has been removed.
* We now require bash v3 (released July 27, 2004) or better in order to take advantage of better regex handling and ${BASH\_SOURCE}.  POSIX sh will not work.
* Support for --script has been removed. We now use ${HADOOP\_\*\_PATH} or ${HADOOP\_PREFIX} to find the necessary binaries.  (See other note regarding ${HADOOP\_PREFIX} auto discovery.)
* Non-existent classpaths, ld.so library paths, JNI library paths, etc, will be ignored and stripped from their respective environment settings.

NEW FEATURES:

* Daemonization has been moved from *-daemon.sh to the bin commands via the --daemon option. Simply use --daemon start to start a daemon, --daemon stop to stop a daemon, and --daemon status to set $? to the daemon's status.  The return code for status is LSB-compatible.  For example, 'hdfs --daemon start namenode'.
* It is now possible to override some of the shell code capabilities to provide site specific functionality without replacing the shipped versions.  Replacement functions should go into the new hadoop-user-functions.sh file.
* A new option called --buildpaths will attempt to add developer build directories to the classpath to allow for in source tree testing.
* Operations which trigger ssh connections can now use pdsh if installed.  ${HADOOP\_SSH\_OPTS} still gets applied. 
* Added distch and jnipath subcommands to the hadoop command.
* Shell scripts now support a --debug option which will report basic information on the construction of various environment variables, java options, classpath, etc. to help in configuration debugging.

BUG FIXES:

* ${HADOOP\_CONF\_DIR} is now properly honored everywhere, without requiring symlinking and other such tricks.
* ${HADOOP\_CONF\_DIR}/hadoop-layout.sh is now documented with a provided hadoop-layout.sh.example file.
* Shell commands should now work properly when called as a relative path, without ${HADOOP\_PREFIX} being defined, and as the target of bash -x for debugging. If ${HADOOP\_PREFIX} is not set, it will be automatically determined based upon the current location of the shell library.  Note that other parts of the extended Hadoop ecosystem may still require this environment variable to be configured.
* Operations which trigger ssh will now limit the number of connections to run in parallel to ${HADOOP\_SSH\_PARALLEL} to prevent memory and network exhaustion.  By default, this is set to 10.
* ${HADOOP\_CLIENT\_OPTS} support has been added to a few more commands.
* Some subcommands were not listed in the usage.
* Various options on hadoop command lines were supported inconsistently.  These have been unified into hadoop-config.sh. --config is still required to be first, however.
* ulimit logging for secure daemons no longer assumes /bin/bash but does assume bash is on the command line path.
* Removed references to some Yahoo! specific paths.
* Removed unused slaves.sh from YARN build tree.
* Many exit states have been changed to reflect reality.
* Shell level errors now go to STDERR.  Before, many of them went incorrectly to STDOUT.
* CDPATH with a period (.) should no longer break the scripts.
* The scripts no longer try to chown directories.
* If ${JAVA\_HOME} is not set on OS X, it now properly detects it instead of throwing an error.

IMPROVEMENTS:

* The *.out files are now appended instead of overwritten to allow for external log rotation.
* The style and layout of the scripts is much more consistent across subprojects.  
* More of the shell code is now commented.
* Significant amounts of redundant code have been moved into a new file called hadoop-functions.sh.
* The various *-env.sh have been massively changed to include documentation and examples on what can be set, ramifications of setting, etc.  for all variables that are expected to be set by a user.  
* There is now some trivial de-duplication and sanitization of the classpath and JVM options.  This allows, amongst other things, for custom settings in \*\_OPTS for Hadoop daemons to override defaults and other generic settings (i.e., ${HADOOP\_OPTS}).  This is particularly relevant for Xmx settings, as one can now set them in _OPTS and ignore the heap specific options for daemons which force the size in megabytes.
* Subcommands have been alphabetized in both usage and in the code.
* All/most of the functionality provided by the sbin/* commands has been moved to either their bin/ equivalents or made into functions.  The rewritten versions of these commands are now wrappers to maintain backward compatibility.
* Usage information is given with the following options/subcommands for all scripts using the common framework: --? -? ? --help -help -h help 
* Several generic environment variables have been added to provide a common configuration for pids, logs, and their security equivalents.  The older versions still act as overrides to these generic versions.
* Groundwork has been laid to allow for custom secure daemon setup using something other than jsvc (e.g., pfexec on Solaris).
* Scripts now test and report better error messages for various states of the log and pid dirs on daemon startup.  Before, unprotected shell errors would be displayed to the user.


---

* [HADOOP-11041](https://issues.apache.org/jira/browse/HADOOP-11041) | *Minor* | **VersionInfo output specifies subversion**

This changes the output of the 'hadoop version' command to generically say 'Source code repository' rather than specify which type of repo.


---

* [MAPREDUCE-5972](https://issues.apache.org/jira/browse/MAPREDUCE-5972) | *Trivial* | **Fix typo 'programatically' in job.xml (and a few other places)**

Fix a typo. If a configuration is set through program, the source of the configuration is set to 'programmatically' instead of 'programatically' now.


---

* [MAPREDUCE-2841](https://issues.apache.org/jira/browse/MAPREDUCE-2841) | *Major* | **Task level native optimization**

Adds a native implementation of the map output collector. The native library will build automatically with -Pnative. Users may choose the new collector on a job-by-job basis by setting mapreduce.job.map.output.collector.class=org.apache.hadoop.mapred.
nativetask.NativeMapOutputCollectorDelegator in their job configuration. For shuffle-intensive jobs this may provide speed-ups of 30% or more.


---

* [HADOOP-11356](https://issues.apache.org/jira/browse/HADOOP-11356) | *Major* | **Removed deprecated o.a.h.fs.permission.AccessControlException**

org.apache.hadoop.fs.permission.AccessControlException was deprecated in the last major release, and has been removed in favor of org.apache.hadoop.security.AccessControlException


---

* [HADOOP-10950](https://issues.apache.org/jira/browse/HADOOP-10950) | *Major* | **rework  heap management  vars**

<!-- markdown -->

* HADOOP\_HEAPSIZE variable has been deprecated  (It will still be honored if set, but expect it to go away in the future).    In its place, HADOOP\_HEAPSIZE\_MAX and HADOOP\_HEAPSIZE\_MIN have been introduced to set Xmx and Xms, respectively. 

* The internal variable JAVA\_HEAP\_MAX has been removed.

* Default heap sizes have been removed. This will allow for the JVM to use auto-tuning based upon the memory size of the host. To re-enable the old default, configure HADOOP\_HEAPSIZE_MAX="1g" in hadoop-env.sh. 

* All global and daemon-specific heap size variables now support units.  If the variable is only a number, the size is assumed to be in megabytes.


---

* [HADOOP-11353](https://issues.apache.org/jira/browse/HADOOP-11353) | *Major* | **Add support for .hadooprc**

.hadooprc allows users a convenient way to set and/or override the shell level settings.


---

* [MAPREDUCE-5785](https://issues.apache.org/jira/browse/MAPREDUCE-5785) | *Major* | **Derive heap size or mapreduce.\*.memory.mb automatically**

The memory values for mapreduce.map/reduce.memory.mb keys, if left to their default values of -1, will now be automatically inferred from the heap size value system property (-Xmx) specified for mapreduce.map/reduce.java.opts keys.

The converse is also done, i.e. if mapreduce.map/reduce.memory.mb values are specified, but no -Xmx is supplied for mapreduce.map/reduce.java.opts keys, then the -Xmx value will be derived from the former's value.

If neither is specified, then a default value of 1024 MB gets used.

For both these conversions, a scaling factor specified by property mapreduce.job.heap.memory-mb.ratio is used, to account for overheads between heap usage vs. actual physical memory usage.

Existing configs or job code that already specify both the set of properties explicitly would not be affected by this inferring change.


---

* [YARN-2428](https://issues.apache.org/jira/browse/YARN-2428) | *Trivial* | **LCE default banned user list should have yarn**

The user 'yarn' is no longer allowed to run tasks for security reasons.


---

* [HADOOP-11460](https://issues.apache.org/jira/browse/HADOOP-11460) | *Major* | **Deprecate shell vars**

<!-- markdown -->

| Old | New |
|:---- |:---- |
| HADOOP\_HDFS\_LOG\_DIR | HADOOP\_LOG\_DIR |
| HADOOP\_HDFS\_LOGFILE | HADOOP\_LOGFILE |
| HADOOP\_HDFS\_NICENESS | HADOOP\_NICENESS |
| HADOOP\_HDFS\_STOP\_TIMEOUT | HADOOP\_STOP\_TIMEOUT |
| HADOOP\_HDFS\_PID\_DIR | HADOOP\_PID\_DIR |
| HADOOP\_HDFS\_ROOT\_LOGGER | HADOOP\_ROOT\_LOGGER |
| HADOOP\_HDFS\_IDENT\_STRING | HADOOP\_IDENT\_STRING |
| HADOOP\_MAPRED\_LOG\_DIR | HADOOP\_LOG\_DIR |
| HADOOP\_MAPRED\_LOGFILE | HADOOP\_LOGFILE |
| HADOOP\_MAPRED\_NICENESS | HADOOP\_NICENESS |
| HADOOP\_MAPRED\_STOP\_TIMEOUT | HADOOP\_STOP\_TIMEOUT |
| HADOOP\_MAPRED\_PID\_DIR | HADOOP\_PID\_DIR |
| HADOOP\_MAPRED\_ROOT\_LOGGER | HADOOP\_ROOT\_LOGGER |
| HADOOP\_MAPRED\_IDENT\_STRING | HADOOP\_IDENT\_STRING |
| YARN\_CONF\_DIR | HADOOP\_CONF\_DIR |
| YARN\_LOG\_DIR | HADOOP\_LOG\_DIR |
| YARN\_LOGFILE | HADOOP\_LOGFILE |
| YARN\_NICENESS | HADOOP\_NICENESS |
| YARN\_STOP\_TIMEOUT | HADOOP\_STOP\_TIMEOUT |
| YARN\_PID\_DIR | HADOOP\_PID\_DIR |
| YARN\_ROOT\_LOGGER | HADOOP\_ROOT\_LOGGER |
| YARN\_IDENT\_STRING | HADOOP\_IDENT\_STRING |
| YARN\_OPTS | HADOOP\_OPTS |
| YARN\_SLAVES | HADOOP\_SLAVES |
| YARN\_USER\_CLASSPATH | HADOOP\_CLASSPATH |
| YARN\_USER\_CLASSPATH\_FIRST | HADOOP\_USER\_CLASSPATH\_FIRST |
| KMS\_CONFIG | HADOOP\_CONF\_DIR |
| KMS\_LOG | HADOOP\_LOG\_DIR |


---

* [HADOOP-7713](https://issues.apache.org/jira/browse/HADOOP-7713) | *Trivial* | **dfs -count -q should label output column**

Added -v option to fs -count command to display a header record in the report.


---

* [HADOOP-11485](https://issues.apache.org/jira/browse/HADOOP-11485) | *Major* | **Pluggable shell integration**

Support for shell profiles has been added.  They allow for easy integration of additional functionality, classpaths, and more from inside the bash scripts rather than relying upon modifying hadoop-env.sh, etc.  See the Unix Shell Guide for more information.


---

* [HADOOP-8934](https://issues.apache.org/jira/browse/HADOOP-8934) | *Minor* | **Shell command ls should include sort options**

Options to sort output of fs -ls comment: -t (mtime), -S (size), -u (atime), -r (reverse)


---

* [HADOOP-11554](https://issues.apache.org/jira/browse/HADOOP-11554) | *Major* | **Expose HadoopKerberosName as a hadoop subcommand**

The hadoop kerbname subcommand has been added to ease operational pain in determining the output of auth\_to\_local rules.


---

* [HDFS-7460](https://issues.apache.org/jira/browse/HDFS-7460) | *Major* | **Rewrite httpfs to use new shell framework**

<!-- markdown -->
This deprecates the following environment variables:

| Old | New |
|:---- |:---- |
| HTTPFS_LOG | HADOOP_LOG_DIR|
| HTTPFS_CONFG | HADOOP_CONF_DIR |


---

* [MAPREDUCE-5653](https://issues.apache.org/jira/browse/MAPREDUCE-5653) | *Major* | **DistCp does not honour config-overrides for mapreduce.[map,reduce].memory.mb**

Prior to this change, distcp had hard-coded values for memory usage.  Now distcp will honor memory settings in a way compatible with the rest of MapReduce.


---

* [HADOOP-11657](https://issues.apache.org/jira/browse/HADOOP-11657) | *Minor* | **Align the output of \`hadoop fs -du\` to be more Unix-like**

The output of du has now been made more Unix-like, with aligned output.


---

* [HDFS-7302](https://issues.apache.org/jira/browse/HDFS-7302) | *Major* | **namenode -rollingUpgrade downgrade may finalize a rolling upgrade**

Remove "downgrade" from "namenode -rollingUpgrade" startup option since it may incorrectly finalize an ongoing rolling upgrade.


---

* [HADOOP-6857](https://issues.apache.org/jira/browse/HADOOP-6857) | *Major* | **FsShell should report raw disk usage including replication factor**

The output format of hadoop fs -du has been changed. It shows not only the file size but also the raw disk usage including the replication factor.


---

* [HADOOP-11226](https://issues.apache.org/jira/browse/HADOOP-11226) | *Major* | **Add a configuration to set ipc.Client's traffic class with IPTOS\_LOWDELAY\|IPTOS\_RELIABILITY**

Use low latency TCP connections for hadoop IPC


---

* [HADOOP-10115](https://issues.apache.org/jira/browse/HADOOP-10115) | *Major* | **Exclude duplicate jars in hadoop package under different component's lib**

Jars in the various subproject lib directories are now de-duplicated against Hadoop common.  Users who interact directly with those directories must be sure to pull in common's dependencies as well.


---

* [YARN-3154](https://issues.apache.org/jira/browse/YARN-3154) | *Blocker* | **Should not upload partial logs for MR jobs or other "short-running' applications**

Applications which made use of the LogAggregationContext in their application will need to revisit this code in order to make sure that their logs continue to get rolled out.


---

* [HADOOP-9477](https://issues.apache.org/jira/browse/HADOOP-9477) | *Major* | **Add posixGroups support for LDAP groups mapping service**

Add posixGroups support for LDAP groups mapping service. The change in LDAPGroupMapping is compatible with previous scenario. In LDAP, the group mapping between {{posixAccount}} and {{posixGroup}} is different from the general LDAPGroupMapping, one of the differences is the {{"memberUid"}} will be used to mapping {{posixAccount}} and {{posixGroup}}. The feature will handle the mapping in internal when configuration {{hadoop.security.group.mapping.ldap.search.filter.user}} is set as "posixAccount" and {{hadoop.security.group.mapping.ldap.search.filter.group}} is "posixGroup".


---

* [MAPREDUCE-4424](https://issues.apache.org/jira/browse/MAPREDUCE-4424) | *Minor* | **'mapred job -list' command should show the job name as well**

Now "mapred job -list" command displays the Job Name as well.


---

* [YARN-3241](https://issues.apache.org/jira/browse/YARN-3241) | *Major* | **FairScheduler handles "invalid" queue names inconsistently**

FairScheduler does not allow queue names with leading or tailing spaces or empty sub-queue names anymore.


---

* [HDFS-7985](https://issues.apache.org/jira/browse/HDFS-7985) | *Major* | **WebHDFS should be always enabled**

WebHDFS is mandatory and cannot be disabled.


---

* [HDFS-6353](https://issues.apache.org/jira/browse/HDFS-6353) | *Major* | **Check and make checkpoint before stopping the NameNode**

Stopping the namenode on secure systems now requires the user be authenticated.


---

* [HADOOP-11553](https://issues.apache.org/jira/browse/HADOOP-11553) | *Blocker* | **Formalize the shell API**

Python is now required to build the documentation.


---

* [HDFS-7501](https://issues.apache.org/jira/browse/HDFS-7501) | *Major* | **TransactionsSinceLastCheckpoint can be negative on SBNs**

Fixed a bug where the StandbyNameNode's TransactionsSinceLastCheckpoint metric may slide into a negative number after every subsequent checkpoint.


---

* [HADOOP-11660](https://issues.apache.org/jira/browse/HADOOP-11660) | *Minor* | **Add support for hardware crc of HDFS checksums on ARM aarch64 architecture**

Add support for aarch64 CRC instructions


---

* [HADOOP-11731](https://issues.apache.org/jira/browse/HADOOP-11731) | *Major* | **Rework the changelog and releasenotes**

<!-- markdown -->
* The release notes now only contains JIRA issues with incompatible changes and actual release notes.  The generated format has been changed from HTML to markdown.

* The changelog is now automatically generated from data stored in JIRA rather than manually maintained. The format has been changed from pure text to markdown as well as containing more of the information that was previously stored in the release notes.

* In order to generate the changes file, python must be installed.

* New -Preleasedocs profile added to maven in order to trigger this functionality.


---

* [YARN-3365](https://issues.apache.org/jira/browse/YARN-3365) | *Major* | **Add support for using the 'tc' tool via container-executor**

Adding support for using the 'tc' tool in batch mode via container-executor. This is a prerequisite for traffic-shaping functionality that is necessary to support outbound bandwidth as a resource in YARN.


---

* [HADOOP-11781](https://issues.apache.org/jira/browse/HADOOP-11781) | *Major* | **fix race conditions and add URL support to smart-apply-patch.sh**

Now auto-downloads patch from issue-id; fixed race conditions; fixed bug affecting some patches.


---

* [YARN-3443](https://issues.apache.org/jira/browse/YARN-3443) | *Major* | **Create a 'ResourceHandler' subsystem to ease addition of support for new resource types on the NM**

The current cgroups implementation is closely tied to supporting CPU as a resource . This patch separates out CGroups implementation into a reusable class as well as provides a simple ResourceHandler subsystem that will enable us to add support for new resource types on the NM - e.g Network, Disk etc.


---

* [HDFS-6666](https://issues.apache.org/jira/browse/HDFS-6666) | *Minor* | **Abort NameNode and DataNode startup if security is enabled but block access token is not enabled.**

NameNode and DataNode now abort during startup if attempting to run in secure mode, but block access tokens are not enabled by setting configuration property dfs.block.access.token.enable to true in hdfs-site.xml.  Previously, this case logged a warning, because this would be an insecure configuration.


---

* [YARN-3021](https://issues.apache.org/jira/browse/YARN-3021) | *Major* | **YARN's delegation-token handling disallows certain trust setups to operate properly over DistCp**

ResourceManager renews delegation tokens for applications. This behavior has been changed to renew tokens only if the token's renewer is a non-empty string. MapReduce jobs can instruct ResourceManager to skip renewal of tokens obtained from certain hosts by specifying the hosts with configuration mapreduce.job.hdfs-servers.token-renewal.exclude=\<host1\>,\<host2\>,..,\<hostN\>.


---

* [HADOOP-11746](https://issues.apache.org/jira/browse/HADOOP-11746) | *Major* | **rewrite test-patch.sh**

<!-- markdown -->
* test-patch.sh now has new output that is different than the previous versions
* test-patch.sh is now pluggable via the test-patch.d directory, with checkstyle and shellcheck tests included
* JIRA comments now use much more markup to improve readability
* test-patch.sh now supports either a file name, a URL, or a JIRA issue as input in developer mode
* If part of the patch testing code is changed, test-patch.sh will now attempt to re-executing itself using the new version.
* Some logic to try and reduce the amount of unnecessary tests.  For example, patches that only modify markdown should not run the Java compilation tests.
* Plugins for checkstyle, shellcheck, and whitespace now execute as necessary.
* New test code for mvn site
* A breakdown of the times needed to execute certain blocks as well as a total runtime is now reported to assist in fixing long running tests and optimize the entire process.
* Several new options
  * --resetrepo will put test-patch.sh in destructive mode, similar to a normal Jenkins run
  * --testlist allows one to provide a comma delimited list of test subsystems to forcibly execute
  * --modulelist to provide a comma delimited list of module tests to execute in addition to the ones that are automatically detected
  * --offline mode to attempt to stop connecting to the Internet for certain operations
* test-patch.sh now defaults to the POSIX equivalents on Solaris and Illumos-based operating systems
* shelldocs.py may be used to generate test-patch.sh API information
* FindBugs output is now listed on the JIRA comment
* lots of general code cleanup, including attempts to remove any local state files to reduce potential race conditions
* Some logic to determine if a patch is for a given major branch using several strategies as well as a particular git ref (using git+ref as part of the name).
* Some logic to determine if a patch references a particular JIRA issue.
* Unit tests are only flagged as necessary with native or Java code, since Hadoop has no framework in place yet for other types of unit tests.
* test-patch now exits with a failure status if problems arise trying to do git checkouts.  Previously the exit code was success.


---

* [YARN-3366](https://issues.apache.org/jira/browse/YARN-3366) | *Major* | **Outbound network bandwidth : classify/shape traffic originating from YARN containers**

1) A TrafficController class that provides an implementation for traffic shaping using tc. 
2) A ResourceHandler implementation for OutboundBandwidth as a resource - isolation/enforcement using cgroups and tc.


---

* [HADOOP-11861](https://issues.apache.org/jira/browse/HADOOP-11861) | *Major* | **test-patch.sh rewrite addendum patch**

<!-- markdown -->
* --build-native=false should work now
* --branch option lets one specify a branch to test against on the command line
* On certain Jenkins machines, the artifact directory sometimes gets deleted from outside the test-patch script.  There is now some code to try to detect, alert, and quick exit if that happens.
* Various semi-critical output and bug fixes


---

* [HADOOP-11627](https://issues.apache.org/jira/browse/HADOOP-11627) | *Major* | **Remove io.native.lib.available**

io.native.lib.available was removed. Always use native libraries if they exist.


---

* [HADOOP-11843](https://issues.apache.org/jira/browse/HADOOP-11843) | *Major* | **Make setting up the build environment easier**

Includes a docker based solution for setting up a build environment with minimal effort.


---

* [HDFS-7281](https://issues.apache.org/jira/browse/HDFS-7281) | *Major* | **Missing block is marked as corrupted block**

The patch improves the reporting around missing blocks and corrupted blocks.
 
1. A block is missing if and only if all DNs of its expected replicas are dead. 
2. A block is corrupted if and only if all its available replicas are corrupted. So if a block has 3 replicas; one of the DN is dead, the other two replicas are corrupted; it will be marked as corrupted.
3. A new line is added to fsck output to display the corrupt block size per file.
4. A new line is added to fsck output to display the number of missing blocks in the summary section.


---

* [HADOOP-11813](https://issues.apache.org/jira/browse/HADOOP-11813) | *Minor* | **releasedocmaker.py should use today's date instead of unreleased**

Use today instead of 'Unreleased' in releasedocmaker.py when --usetoday is given as an option.


---

* [HDFS-8226](https://issues.apache.org/jira/browse/HDFS-8226) | *Blocker* | **Non-HA rollback compatibility broken**

Non-HA rollback steps have been changed. Run the rollback command on the namenode (\`bin/hdfs namenode -rollback\`) before starting cluster with '-rollback' option using (sbin/start-dfs.sh -rollback).


---

* [MAPREDUCE-2632](https://issues.apache.org/jira/browse/MAPREDUCE-2632) | *Major* | **Avoid calling the partitioner when the numReduceTasks is 1.**

A partitioner is now only created if there are multiple reducers.


---

* [HDFS-8241](https://issues.apache.org/jira/browse/HDFS-8241) | *Minor* | **Remove unused NameNode startup option -finalize**

Remove -finalize option from hdfs namenode command.


---

* [HDFS-6888](https://issues.apache.org/jira/browse/HDFS-6888) | *Major* | **Allow selectively audit logging ops**

Specific HDFS ops can be selectively excluded from audit logging via 'dfs.namenode.audit.log.debug.cmdlist' configuration.


---

* [HDFS-8157](https://issues.apache.org/jira/browse/HDFS-8157) | *Major* | **Writes to RAM DISK reserve locked memory for block files**

This change requires setting the dfs.datanode.max.locked.memory configuration key to use the HDFS Lazy Persist feature. Its value limits the combined off-heap memory for blocks in RAM via caching and lazy persist writes.


---

* [HDFS-8332](https://issues.apache.org/jira/browse/HDFS-8332) | *Major* | **DFS client API calls should check filesystem closed**

Users may need special attention for this change while upgrading to this version. Previously user could call some APIs(example: setReplication) wrongly even after closing the fs object. With this change DFS client will not allow any operations to call on closed fs objects.  As calling fs operations on closed fs is not right thing to do, users need to correct the usage if any.


---

* [HADOOP-11698](https://issues.apache.org/jira/browse/HADOOP-11698) | *Major* | **Remove DistCpV1 and Logalyzer**

Removed DistCpV1 and Logalyzer.


---

* [HADOOP-11772](https://issues.apache.org/jira/browse/HADOOP-11772) | *Major* | **RPC Invoker relies on static ClientCache which has synchronized(this) blocks**

The Client#call() methods that are deprecated since 0.23 have been removed.


---

* [YARN-3684](https://issues.apache.org/jira/browse/YARN-3684) | *Major* | **Change ContainerExecutor's primary lifecycle methods to use a more extensible mechanism for passing information.**

Modifying key methods in ContainerExecutor to use context objects instead of an argument list. This is more extensible and less brittle.


---

* [YARN-2336](https://issues.apache.org/jira/browse/YARN-2336) | *Major* | **Fair scheduler REST api returns a missing '[' bracket JSON for deep queue tree**

Fix FairScheduler's REST api returns a missing '[' blacket JSON for childQueues.


---

* [HDFS-8135](https://issues.apache.org/jira/browse/HDFS-8135) | *Major* | **Remove the deprecated FSConstants class**

The FSConstants class has been deprecated since 0.23 and it is removed in the release.


---

* [MAPREDUCE-6336](https://issues.apache.org/jira/browse/MAPREDUCE-6336) | *Major* | **Enable v2 FileOutputCommitter by default**

mapreduce.fileoutputcommitter.algorithm.version now defaults to 2.
  
In algorithm version 1:

  1. commitTask renames directory
  $joboutput/\_temporary/$appAttemptID/\_temporary/$taskAttemptID/
  to
  $joboutput/\_temporary/$appAttemptID/$taskID/

  2. recoverTask renames
  $joboutput/\_temporary/$appAttemptID/$taskID/
  to
  $joboutput/\_temporary/($appAttemptID + 1)/$taskID/

  3. commitJob merges every task output file in
  $joboutput/\_temporary/$appAttemptID/$taskID/
  to
  $joboutput/, then it will delete $joboutput/\_temporary/
  and write $joboutput/\_SUCCESS

commitJob's run time, number of RPC, is O(n) in terms of output files, which is discussed in MAPREDUCE-4815, and can take minutes. 

Algorithm version 2 changes the behavior of commitTask, recoverTask, and commitJob.

  1. commitTask renames all files in
  $joboutput/\_temporary/$appAttemptID/\_temporary/$taskAttemptID/
  to $joboutput/

  2. recoverTask is a nop strictly speaking, but for
  upgrade from version 1 to version 2 case, it checks if there
  are any files in
  $joboutput/\_temporary/($appAttemptID - 1)/$taskID/
  and renames them to $joboutput/

  3. commitJob deletes $joboutput/\_temporary and writes
  $joboutput/\_SUCCESS

Algorithm 2 takes advantage of task parallelism and makes commitJob itself O(1). However, the window of vulnerability for having incomplete output in $jobOutput directory is much larger. Therefore, pipeline logic for consuming job outputs should be built on checking for existence of \_SUCCESS marker.


---

* [YARN-2355](https://issues.apache.org/jira/browse/YARN-2355) | *Major* | **MAX\_APP\_ATTEMPTS\_ENV may no longer be a useful env var for a container**

Removed consumption of the MAX\_APP\_ATTEMPTS\_ENV environment variable


---

* [HDFS-5033](https://issues.apache.org/jira/browse/HDFS-5033) | *Minor* | **Bad error message for fs -put/copyFromLocal if user doesn't have permissions to read the source**

"Permission denied" error message when unable to read local file for -put/copyFromLocal


---

* [HADOOP-9905](https://issues.apache.org/jira/browse/HADOOP-9905) | *Major* | **remove dependency of zookeeper for hadoop-client**

Zookeeper jar removed from hadoop-client dependency tree.


---

* [HDFS-8486](https://issues.apache.org/jira/browse/HDFS-8486) | *Blocker* | **DN startup may cause severe data loss**

<!-- markdown -->
Public service notice:
* Every restart of a 2.6.x or 2.7.0 DN incurs a risk of unwanted block deletion.
* Apply this patch if you are running a pre-2.7.1 release.


---

* [HDFS-8270](https://issues.apache.org/jira/browse/HDFS-8270) | *Major* | **create() always retried with hardcoded timeout when file already exists with open lease**

Proxy level retries will not be done on AlreadyBeingCreatedExeption for create() op.


---

* [YARN-41](https://issues.apache.org/jira/browse/YARN-41) | *Major* | **The RM should handle the graceful shutdown of the NM.**

The behavior of shutdown a NM could be different (if NM work preserving is not enabled): NM will unregister to RM immediately rather than waiting for timeout to be LOST. A new status of NodeStatus - SHUTDOWN is involved which could affect UI, CLI and ClusterMetrics for node's status.


---

* [HADOOP-11347](https://issues.apache.org/jira/browse/HADOOP-11347) | *Major* | **RawLocalFileSystem#mkdir and create should honor umask**

**WARNING: No release note provided for this change.**


---

* [HDFS-8591](https://issues.apache.org/jira/browse/HDFS-8591) | *Minor* | **Remove support for deprecated configuration key dfs.namenode.decommission.nodes.per.interval**

Related to the decommission enhancements in HDFS-7411, this change removes the deprecated configuration key "dfs.namenode.decommission.nodes.per.interval" which has been subsumed by the configuration key "dfs.namenode.decommission.blocks.per.interval".


---

* [HADOOP-7139](https://issues.apache.org/jira/browse/HADOOP-7139) | *Major* | **Allow appending to existing SequenceFiles**

Existing sequence files can be appended.


---

* [HDFS-8582](https://issues.apache.org/jira/browse/HDFS-8582) | *Minor* | **Support getting a list of reconfigurable config properties and do not generate spurious reconfig warnings**

Add a new option "properties" to the "dfsadmin -reconfig" command to get a list of reconfigurable properties.


---

* [HDFS-6564](https://issues.apache.org/jira/browse/HDFS-6564) | *Major* | **Use slf4j instead of common-logging in hdfs-client**

Users may need special attention for this change while upgrading to this version. Previously hdfs client was using commons-logging as the logging framework. With this change it will use slf4j framework. For more details about slf4j, please see: http://www.slf4j.org/manual.html. Also, org.apache.hadoop.hdfs.protocol.CachePoolInfo#LOG public static member variable has been removed as it is not used anywhere. Users need to correct their code if any one has a reference to this variable. One can retrieve the named logger via the logging framework of their choice directly like, org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(org.apache.hadoop.hdfs.protocol.CachePoolInfo.class);


---

* [HDFS-6440](https://issues.apache.org/jira/browse/HDFS-6440) | *Major* | **Support more than 2 NameNodes**

This feature adds support for running additional standby NameNodes, which provides additional fault-tolerance. It is designed for a total of 3-5 NameNodes.


---

* [YARN-3823](https://issues.apache.org/jira/browse/YARN-3823) | *Minor* | **Fix mismatch in default values for yarn.scheduler.maximum-allocation-vcores property**

Default value for 'yarn.scheduler.maximum-allocation-vcores' changed from 32 to 4.


---

* [HADOOP-5732](https://issues.apache.org/jira/browse/HADOOP-5732) | *Minor* | **Add SFTP FileSystem**

Added SFTP filesystem by using the JSch library.


---

* [YARN-3069](https://issues.apache.org/jira/browse/YARN-3069) | *Major* | **Document missing properties in yarn-default.xml**

Documented missing properties and added the regression test to verify that there are no missing properties in yarn-default.xml.


---

* [MAPREDUCE-6427](https://issues.apache.org/jira/browse/MAPREDUCE-6427) | *Minor* | **Fix typo in JobHistoryEventHandler**

There is a typo in the event string "WORKFLOW\_ID" (as "WORKLFOW\_ID").  The branch-2 change will publish both event strings for compatibility with consumers, but the misspelled metric will be removed in trunk.


---

* [HADOOP-12209](https://issues.apache.org/jira/browse/HADOOP-12209) | *Minor* | **Comparable type should be in FileStatus**

**WARNING: No release note provided for this change.**


---

* [HDFS-7582](https://issues.apache.org/jira/browse/HDFS-7582) | *Major* | **Enforce maximum number of ACL entries separately per access and default.**

Limit on Maximum number of ACL entries(32) will be enforced separately on access and default ACLs. So in total, max. 64 ACL entries can be present in a ACL spec.


---

* [HADOOP-12269](https://issues.apache.org/jira/browse/HADOOP-12269) | *Major* | **Update aws-sdk dependency to 1.10.6; move to aws-sdk-s3**

The Maven dependency on aws-sdk has been changed to aws-sdk-s3 and the version bumped. Applications depending on transitive dependencies pulled in by aws-sdk and not aws-sdk-s3 might not work.


---

* [HDFS-8895](https://issues.apache.org/jira/browse/HDFS-8895) | *Major* | **Remove deprecated BlockStorageLocation APIs**

This removes the deprecated DistributedFileSystem#getFileBlockStorageLocations API used for getting VolumeIds of block replicas. Applications interested in the volume of a replica can instead consult BlockLocation#getStorageIds to obtain equivalent information.


---

* [HADOOP-12352](https://issues.apache.org/jira/browse/HADOOP-12352) | *Trivial* | **Delay in checkpointing Trash can leave trash for 2 intervals before deleting**

Fixes an Trash related issue wherein a delay in the periodic checkpointing of one user's directory causes the subsequent user directory checkpoints to carry a newer timestamp, thereby delaying their eventual deletion.


---

* [HDFS-8900](https://issues.apache.org/jira/browse/HDFS-8900) | *Major* | **Compact XAttrs to optimize memory footprint.**

The config key "dfs.namenode.fs-limits.max-xattr-size" can no longer be set to a value of 0 (previously used to indicate unlimited) or a value greater than 32KB. This is a constraint on xattr size similar to many local filesystems.


---

* [HDFS-8890](https://issues.apache.org/jira/browse/HDFS-8890) | *Major* | **Allow admin to specify which blockpools the balancer should run on**

Adds a new blockpools flag to the balancer. This allows admins to specify which blockpools the balancer will run on.
Usage:
-blockpools \<comma-separated list of blockpool ids\>
The balancer will only run on blockpools included in this list.


---

* [HDFS-8981](https://issues.apache.org/jira/browse/HDFS-8981) | *Minor* | **Adding revision to data node jmx getVersion() method**

getSoftwareVersion method would replace original getVersion method, which returns the version string.

The new getVersion method would return both version string and revision string.


---

* [YARN-4087](https://issues.apache.org/jira/browse/YARN-4087) | *Major* | **Followup fixes after YARN-2019 regarding RM behavior when state-store error occurs**

Set YARN\_FAIL\_FAST to be false by default. If HA is enabled and if there's any state-store error, after the retry operation failed, we always transition RM to standby state.


---

* [HADOOP-12384](https://issues.apache.org/jira/browse/HADOOP-12384) | *Major* | **Add "-direct" flag option for fs copy so that user can choose not to create ".\_COPYING\_" file**

An option '-d' added for all command-line copy commands to skip intermediate '.COPYING' file creation.


---

* [HDFS-8929](https://issues.apache.org/jira/browse/HDFS-8929) | *Major* | **Add a metric to expose the timestamp of the last journal**

Exposed a metric 'LastJournalTimestamp' for JournalNode


---

* [HDFS-7116](https://issues.apache.org/jira/browse/HDFS-7116) | *Major* | **Add a command to get the balancer bandwidth**

Exposed command "-getBalancerBandwidth" in dfsadmin to get the bandwidth of balancer.


---

* [YARN-4126](https://issues.apache.org/jira/browse/YARN-4126) | *Major* | **RM should not issue delegation tokens in unsecure mode**

Yarn now only issues and allows delegation tokens in secure clusters.  Clients should no longer request delegation tokens in a non-secure cluster, or they'll receive an IOException.


---

* [HDFS-8829](https://issues.apache.org/jira/browse/HDFS-8829) | *Major* | **Make SO\_RCVBUF and SO\_SNDBUF size configurable for DataTransferProtocol sockets and allow configuring auto-tuning**

HDFS-8829 introduces two new configuration settings: dfs.datanode.transfer.socket.send.buffer.size and dfs.datanode.transfer.socket.recv.buffer.size. These settings can be used to control the socket send buffer and receive buffer sizes respectively on the DataNode for client-DataNode and DataNode-DataNode connections. The default values of both settings are 128KB for backwards compatibility. For optimum performance it is recommended to set these values to zero to enable the OS networking stack to auto-tune buffer sizes.


---

* [YARN-313](https://issues.apache.org/jira/browse/YARN-313) | *Critical* | **Add Admin API for supporting node resource configuration in command line**

After this patch, the feature to support NM resource dynamically configuration is completed, so that user can configure NM with new resource without bring NM down or decommissioned.
Two CLIs are provided to support update resources on individual node or a batch of nodes:
1. Update resource on single node: yarn rmadmin -updateNodeResource [NodeID] [MemSize] [vCores] 
2. Update resource on a batch of nodes: yarn rmadmin -refreshNodesResources, that reflect nodes' resource configuration defined in dynamic-resources.xml which is loaded by RM dynamically (like capacity-scheduler.xml or fair-scheduler.xml). 
The first version of configuration format is:
\<configuration\>
  \<property\>
    \<name\>yarn.resource.dynamic.nodes\</name\>
    \<value\>h1:1234\</value\>
  \</property\>
  \<property\>
    \<name\>yarn.resource.dynamic.h1:1234.vcores\</name\>
    \<value\>16\</value\>
  \</property\>
  \<property\>
    \<name\>yarn.resource.dynamic.h1:1234.memory\</name\>
    \<value\>1024\</value\>
  \</property\>
\</configuration\>


---

* [HADOOP-12416](https://issues.apache.org/jira/browse/HADOOP-12416) | *Major* | **Trash messages should be handled by Logger instead of being delivered on System.out**

Now trash message is not printed to System.out. It is handled by Logger instead.


---

* [HDFS-9063](https://issues.apache.org/jira/browse/HDFS-9063) | *Major* | **Correctly handle snapshot path for getContentSummary**

The jira made the following changes:
1. Fix a bug to exclude newly-created files from quota usage calculation for a snapshot path.
2. Number of snapshots is no longer counted as directory number in getContentSummary result.


---

* [HADOOP-12360](https://issues.apache.org/jira/browse/HADOOP-12360) | *Minor* | **Create StatsD metrics2 sink**

Added StatsD metrics2 sink


---

* [HDFS-9013](https://issues.apache.org/jira/browse/HDFS-9013) | *Major* | **Deprecate NameNodeMXBean#getNNStarted in branch2 and remove from trunk**

NameNodeMXBean#getNNStarted()  metric is deprecated in branch-2 and removed from trunk.


---

* [HADOOP-12437](https://issues.apache.org/jira/browse/HADOOP-12437) | *Major* | **Allow SecurityUtil to lookup alternate hostnames**

HADOOP-12437 introduces two new configuration settings: hadoop.security.dns.interface and hadoop.security.dns.nameserver. These settings can be used to control how Hadoop service instances look up their own hostname and may be required in some multi-homed environments where hosts are configured with multiple hostnames in DNS or hosts files. They supersede the existing settings dfs.datanode.dns.interface and dfs.datanode.dns.nameserver.


---

* [HADOOP-12446](https://issues.apache.org/jira/browse/HADOOP-12446) | *Major* | **Undeprecate createNonRecursive()**

FileSystem#createNonRecursive() is undeprecated.


---

* [HDFS-8696](https://issues.apache.org/jira/browse/HDFS-8696) | *Major* | **Make the lower and higher watermark in the DN Netty server configurable**

Introduced two new configuration dfs.webhdfs.netty.low.watermark and dfs.webhdfs.netty.high.watermark to enable tuning the size of the buffers of the Netty server inside Datanodes.


---

* [HDFS-7285](https://issues.apache.org/jira/browse/HDFS-7285) | *Major* | **Erasure Coding Support inside HDFS**

<!-- markdown -->
HDFS now provides native support for erasure coding (EC) to store data more efficiently. Each individual directory can be configured with an EC policy with command `hdfs erasurecode -setPolicy`. When a file is created, it will inherit the EC policy from its nearest ancestor directory to determine how its blocks are stored. Compared to 3-way replication, the default EC policy saves 50% of storage space while also tolerating more storage failures.

To support small files, the currently phase of HDFS-EC stores blocks in _striped_ layout, where a logical file block is divided into small units (64KB by default) and distributed to a set of DataNodes. This enables parallel I/O but also decreases data locality. Therefore, the cluster environment and I/O workloads should be considered before configuring EC policies.


---

* [HDFS-9085](https://issues.apache.org/jira/browse/HDFS-9085) | *Trivial* | **Show renewer information in DelegationTokenIdentifier#toString**

The output of the "hdfs fetchdt --print" command now includes the token renewer appended to the end of the existing token information.  This change may be incompatible with tools that parse the output of the command.


---

* [HADOOP-12493](https://issues.apache.org/jira/browse/HADOOP-12493) | *Major* | **bash unit tests are failing**

In the extremely rare event that HADOOP\_USER\_IDENT and USER environment variables are not defined, we now fall back to use 'hadoop' as the identification string.


---

* [HADOOP-12495](https://issues.apache.org/jira/browse/HADOOP-12495) | *Major* | **Fix posix\_spawn error on OS X**

When Hadoop JVMs create other processes on OS X, it will always use posix\_spawn.


---

* [HDFS-9070](https://issues.apache.org/jira/browse/HDFS-9070) | *Major* | **Allow fsck display pending replica location information for being-written blocks**

The output of fsck command for being written hdfs files had been changed. When using fsck against being written hdfs files with {{-openforwrite}} and {{-files -blocks -locations}}, the fsck output will include the being written block for replication files or being written block group for erasure code files.


---

* [HDFS-9278](https://issues.apache.org/jira/browse/HDFS-9278) | *Trivial* | **Fix preferredBlockSize typo in OIV XML output**

The preferred block size XML element has been corrected from "\\\<perferredBlockSize\>" to "\\\<preferredBlockSize\>".


---

* [HDFS-9184](https://issues.apache.org/jira/browse/HDFS-9184) | *Major* | **Logging HDFS operation's caller context into audit logs**

The feature needs to enabled by setting "hadoop.caller.context.enabled" to true. When the feature is used, additional fields are written into namenode audit log records.


---

* [HDFS-9259](https://issues.apache.org/jira/browse/HDFS-9259) | *Major* | **Make SO\_SNDBUF size configurable at DFSClient side for hdfs write scenario**

Introduces a new configuration setting dfs.client.socket.send.buffer.size to control the socket send buffer size for writes. Setting it to zero enables TCP auto-tuning on systems that support it.


---

* [HDFS-9311](https://issues.apache.org/jira/browse/HDFS-9311) | *Major* | **Support optional offload of NameNode HA service health checks to a separate RPC server.**

There is now support for offloading HA health check RPC activity to a separate RPC server endpoint running within the NameNode process.  This may improve reliability of HA health checks and prevent spurious failovers in highly overloaded conditions.  For more details, please refer to the hdfs-default.xml documentation for properties dfs.namenode.lifeline.rpc-address, dfs.namenode.lifeline.rpc-bind-host and dfs.namenode.lifeline.handler.count.


---

* [HDFS-6200](https://issues.apache.org/jira/browse/HDFS-6200) | *Major* | **Create a separate jar for hdfs-client**

Projects that access HDFS can depend on the hadoop-hdfs-client module instead of the hadoop-hdfs module to avoid pulling in unnecessary dependency.
Please note that hadoop-hdfs-client module could miss class like ConfiguredFailoverProxyProvider. So if a cluster is in HA deployment, we should still use hadoop-hdfs instead.


---

* [HADOOP-10787](https://issues.apache.org/jira/browse/HADOOP-10787) | *Blocker* | **Rename/remove non-HADOOP\_\*, etc from the shell scripts**

<!-- markdown -->
The following shell environment variables have been deprecated:

| Old | New |
|:---- |:---- |
| DEFAULT\_LIBEXEC\_DIR | HADOOP\_DEFAULT\_LIBEXEC\_DIR |
| SLAVE\_NAMES | HADOOP\_SLAVE\_NAMES |
| TOOL\_PATH | HADOOP\_TOOLS\_PATH |

In addition:

* DEFAULT\_LIBEXEC\_DIR will NOT be automatically transitioned to HADOOP\_DEFAULT\_LIBEXEC\_DIR and will require changes to any scripts setting that value.  A warning will be printed to the screen if DEFAULT\_LIBEXEC\_DIR has been configured.
* HADOOP\_TOOLS\_PATH is now properly handled as a multi-valued, Java classpath-style variable.  Prior, multiple values assigned to TOOL\_PATH would not work a predictable manner.


---

* [HDFS-9057](https://issues.apache.org/jira/browse/HDFS-9057) | *Major* | **allow/disallow snapshots via webhdfs**

Snapshots can be allowed/disallowed on a directory via WebHdfs from users with superuser privilege.


---

* [MAPREDUCE-5485](https://issues.apache.org/jira/browse/MAPREDUCE-5485) | *Critical* | **Allow repeating job commit by extending OutputCommitter API**

Previously, the MR job will get failed if AM get restarted for some reason (like node failure, etc.) during its doing commit job no matter if AM attempts reach to the maximum attempts. 
In this improvement, we add a new API isCommitJobRepeatable() to OutputCommitter interface which to indicate if job's committer can do commitJob again if previous commit work is interrupted by NM/AM failures, etc. The instance of OutputCommitter, which support repeatable job commit (like FileOutputCommitter in algorithm 2), can allow AM to continue the commitJob() after AM restart as a new attempt.


---

* [HADOOP-12294](https://issues.apache.org/jira/browse/HADOOP-12294) | *Major* | **Throw an Exception when fs.permissions.umask-mode is misconfigured**

The support of the deprecated dfs.umask key is removed in Hadoop 3.0.


---

* [HADOOP-10465](https://issues.apache.org/jira/browse/HADOOP-10465) | *Minor* | **Fix use of generics within SortedMapWritable**

SortedMapWritable has changed to SortedMapWritable\<K extends WritableComparable\<? super K\>\>. That way user can declare the class by such as SortedMapWritable\<Text\>.


---

* [HADOOP-12313](https://issues.apache.org/jira/browse/HADOOP-12313) | *Critical* | **NPE in JvmPauseMonitor when calling stop() before start()**

Allow stop() before start() completed in JvmPauseMonitor


---

* [HDFS-9433](https://issues.apache.org/jira/browse/HDFS-9433) | *Major* | **DFS getEZForPath API on a non-existent file should throw FileNotFoundException**

Unify the behavior of dfs.getEZForPath() API when getting a non-existent normal file and non-existent ezone file by throwing FileNotFoundException


---

* [HDFS-5165](https://issues.apache.org/jira/browse/HDFS-5165) | *Minor* | **Remove the TotalFiles metrics**

Now TotalFiles metric is removed from FSNameSystem. Use FilesTotal instead.


---

* [HDFS-8335](https://issues.apache.org/jira/browse/HDFS-8335) | *Major* | **FSNamesystem should construct FSPermissionChecker only if permission is enabled**

Only check permissions when permissions enabled in FSDirStatAndListingOp.getFileInfo() and getListingInt()


---

* [HDFS-8831](https://issues.apache.org/jira/browse/HDFS-8831) | *Major* | **Trash Support for deletion in HDFS encryption zone**

Add Trash support for deleting files within encryption zones. Deleted files will remain encrypted and they will be moved to a “.Trash” subdirectory under the root of the encryption zone, prefixed by $USER/current. Checkpoint and expunge continue to work like the existing Trash.


---

* [HDFS-9214](https://issues.apache.org/jira/browse/HDFS-9214) | *Major* | **Support reconfiguring dfs.datanode.balance.max.concurrent.moves without DN restart**

Steps to reconfigure:
1. change value of the parameter in corresponding xml configuration file
2. to reconfigure, run
    hdfs dfsadmin -reconfig datanode \<dn\_addr\>:\<ipc\_port\> start
3. repeat step 2 until all DNs are reconfigured
4. to check status of the most recent reconfigure operation, run
    hdfs dfsadmin -reconfig datanode \<dn\_addr\>:\<ipc\_port\> status
5. to query a list reconfigurable properties on DN, run
    hdfs dfsadmin -reconfig datanode \<dn\_addr\>:\<ipc\_port\> properties


---

* [YARN-3623](https://issues.apache.org/jira/browse/YARN-3623) | *Major* | **We should have a config to indicate the Timeline Service version**

Add a new configuration "yarn.timeline-service.version" to indicate what is the current version of the running timeline service. For example, if "yarn.timeline-service.version" is 1.5, and "yarn.timeline-service.enabled" is true, it means the cluster will and should bring up the timeline service v.1.5. On the client side, if the client uses the same version of timeline service, it should succeed. If the client chooses to use a smaller version in spite of this, then depending on how robust the compatibility story is between versions, the results may vary.


---

* [YARN-4207](https://issues.apache.org/jira/browse/YARN-4207) | *Major* | **Add a non-judgemental YARN app completion status**

Adds the ENDED attribute to o.a.h.yarn.api.records.FinalApplicationStatus


---

* [HADOOP-12657](https://issues.apache.org/jira/browse/HADOOP-12657) | *Minor* | **Add a option to skip newline on empty files with getMerge -nl**

Added -skip-empty-file option to hadoop fs -getmerge command. With the option, delimiter (LF) is not printed for empty files even if -nl option is used.


---

* [HADOOP-11252](https://issues.apache.org/jira/browse/HADOOP-11252) | *Critical* | **RPC client does not time out by default**

This fix includes public method interface change.
A follow-up JIRA issue for this incompatibility for branch-2.7 is HADOOP-13579.


---

* [HDFS-9047](https://issues.apache.org/jira/browse/HDFS-9047) | *Major* | **Retire libwebhdfs**

libwebhdfs has been retired in 2.8.0 due to the lack of maintenance.


---

* [HADOOP-11262](https://issues.apache.org/jira/browse/HADOOP-11262) | *Major* | **Enable YARN to use S3A**

S3A has been made accessible through the FileContext API.


---

* [HDFS-9624](https://issues.apache.org/jira/browse/HDFS-9624) | *Major* | **DataNode start slowly due to the initial DU command operations**

Make it configurable how long the cached du file is valid. Useful for rolling upgrade.


---

* [HADOOP-12635](https://issues.apache.org/jira/browse/HADOOP-12635) | *Major* | **Adding Append API support for WASB**

The Azure Blob Storage file system (WASB) now includes optional support for use of the append API by a single writer on a path.  Please note that the implementation differs from the semantics of HDFS append.  HDFS append internally guarantees that only a single writer may append to a path at a given time.  WASB does not enforce this guarantee internally.  Instead, the application must enforce access by a single writer, such as by running single-threaded or relying on some external locking mechanism to coordinate concurrent processes.  Refer to the Azure Blob Storage documentation page for more details on enabling append in configuration.


---

* [HADOOP-12651](https://issues.apache.org/jira/browse/HADOOP-12651) | *Major* | **Replace dev-support with wrappers to Yetus**

<!-- markdown -->

* Major portions of dev-support have been replaced with wrappers to Apache Yetus:
  * releasedocmaker.py is now dev-support/bin/releasedocmaker
  * shelldocs.py is now dev-support/bin/shelldocs
  * smart-apply-patch.sh is now dev-support/bin/smart-apply-patch
  * test-patch.sh is now dev-support/bin/test-patch
* See the dev-support/README.md file for more details on how to control the wrappers to various degrees.


---

* [HDFS-9525](https://issues.apache.org/jira/browse/HDFS-9525) | *Blocker* | **hadoop utilities need to support provided delegation tokens**

If hadoop.token.files property is defined and configured to one or more comma-delimited delegation token files, Hadoop will use those token files to connect to the services as named in the token.


---

* [HDFS-9503](https://issues.apache.org/jira/browse/HDFS-9503) | *Major* | **Replace -namenode option with -fs for NNThroughputBenchmark**

The patch replaces -namenode option with -fs for specifying the remote name node against which the benchmark is running. Before this patch, if '-namenode' was not given, the benchmark would run in standalone mode, ignoring the 'fs.defaultFS' in config file even if it's remote. With this patch, the benchmark, as other tools, will rely on the 'fs.defaultFS' config, which is overridable by -fs command option, to run standalone mode or remote mode.


---

* [HADOOP-12426](https://issues.apache.org/jira/browse/HADOOP-12426) | *Minor* | **Add Entry point for Kerberos health check**

Hadoop now includes a shell command named KDiag that helps with diagnosis of Kerberos misconfiguration problems.  Please refer to the Secure Mode documentation for full details on usage of the command.


---

* [HADOOP-12805](https://issues.apache.org/jira/browse/HADOOP-12805) | *Major* | **Annotate CanUnbuffer with @InterfaceAudience.Public**

Made CanBuffer interface public for use in client applications.


---

* [HADOOP-12548](https://issues.apache.org/jira/browse/HADOOP-12548) | *Major* | **Read s3a creds from a Credential Provider**

The S3A Hadoop-compatible file system now support reading its S3 credentials from the Hadoop Credential Provider API in addition to XML configuration files.


---

* [HDFS-9711](https://issues.apache.org/jira/browse/HDFS-9711) | *Major* | **Integrate CSRF prevention filter in WebHDFS.**

WebHDFS now supports options to enforce cross-site request forgery (CSRF) prevention for HTTP requests to both the NameNode and the DataNode.  Please refer to the updated WebHDFS documentation for a description of this feature and further details on how to configure it.


---

* [HADOOP-12794](https://issues.apache.org/jira/browse/HADOOP-12794) | *Major* | **Support additional compression levels for GzipCodec**

Added New compression levels for GzipCodec that can be set in zlib.compress.level


---

* [MAPREDUCE-6613](https://issues.apache.org/jira/browse/MAPREDUCE-6613) | *Minor* | **Change mapreduce.jobhistory.jhist.format default from json to binary**

Default of 'mapreduce.jobhistory.jhist.format' property changed from 'json' to 'binary'.  Creates smaller, binary Avro .jhist files for faster JHS performance.


---

* [HDFS-9425](https://issues.apache.org/jira/browse/HDFS-9425) | *Major* | **Expose number of blocks per volume as a metric**

Number of blocks per volume is made available as a metric.


---

* [HADOOP-12668](https://issues.apache.org/jira/browse/HADOOP-12668) | *Critical* | **Support excluding weak Ciphers in HttpServer2 through ssl-server.xml**

The Code Changes include following:
- Modified DFSUtil.java in Apache HDFS project for supplying new parameter ssl.server.exclude.cipher.list
- Modified HttpServer2.java in Apache Hadoop-common project to work with new parameter and exclude ciphers using jetty setExcludeCihers method.
- Modfied associated test classes to owrk with existing code and also cover the newfunctionality in junit


---

* [HADOOP-12555](https://issues.apache.org/jira/browse/HADOOP-12555) | *Minor* | **WASB to read credentials from a credential provider**

The hadoop-azure file system now supports configuration of the Azure Storage account credentials using the standard Hadoop Credential Provider API.  For details, please refer to the documentation on hadoop-azure and the Credential Provider API.


---

* [HDFS-9395](https://issues.apache.org/jira/browse/HDFS-9395) | *Major* | **Make HDFS audit logging consistant**

<!-- markdown -->

Audit logs will now only be generated in the following two cases:
* When an operation results in an `AccessControlException`
* When an operation is successful

Notably, this means audit log events will not be generated for exceptions besides AccessControlException.


---

* [MAPREDUCE-6622](https://issues.apache.org/jira/browse/MAPREDUCE-6622) | *Critical* | **Add capability to set JHS job cache to a task-based limit**

Two recommendations for the mapreduce.jobhistory.loadedtasks.cache.size property:
1) For every 100k of cache size, set the heap size of the Job History Server to 1.2GB.  For example, mapreduce.jobhistory.loadedtasks.cache.size=500000, heap size=6GB.
2) Make sure that the cache size is larger than the number of tasks required for the largest job run on the cluster.  It might be a good idea to set the value slightly higher (say, 20%) in order to allow for job size growth.


---

* [HADOOP-12552](https://issues.apache.org/jira/browse/HADOOP-12552) | *Minor* | **Fix undeclared/unused dependency to httpclient**

Dependency on commons-httpclient::commons-httpclient was removed from hadoop-common. Downstream projects using commons-httpclient transitively provided by hadoop-common need to add explicit dependency to their pom. Since commons-httpclient is EOL, it is recommended to migrate to org.apache.httpcomponents:httpclient which is the successor.


---

* [HADOOP-12850](https://issues.apache.org/jira/browse/HADOOP-12850) | *Major* | **pull shell code out of hadoop-dist**

This change contains the content of HADOOP-10115 which is an incompatible change.


---

* [HDFS-8791](https://issues.apache.org/jira/browse/HDFS-8791) | *Blocker* | **block ID-based DN storage layout can be very slow for datanode on ext4**

HDFS-8791 introduces a new datanode layout format. This layout is identical to the previous block id based layout except it has a smaller 32x32 sub-directory structure in each data storage. On startup, the datanode will automatically upgrade it's storages to this new layout. Currently, datanode layout changes support rolling upgrades, on the other hand downgrading is not supported between datanode layout changes and a rollback would be required.


---

* [HDFS-9887](https://issues.apache.org/jira/browse/HDFS-9887) | *Major* | **WebHdfs socket timeouts should be configurable**

Added new configuration options: dfs.webhdfs.socket.connect-timeout and dfs.webhdfs.socket.read-timeout both defaulting to 60s.


---

* [HADOOP-11792](https://issues.apache.org/jira/browse/HADOOP-11792) | *Major* | **Remove all of the CHANGES.txt files**

With the introduction of the markdown-formatted and automatically built changes file, the CHANGES.txt files have been eliminated.


---

* [HDFS-9239](https://issues.apache.org/jira/browse/HDFS-9239) | *Major* | **DataNode Lifeline Protocol: an alternative protocol for reporting DataNode liveness**

This release adds a new feature called the DataNode Lifeline Protocol.  If configured, then DataNodes can report that they are still alive to the NameNode via a fallback protocol, separate from the existing heartbeat messages.  This can prevent the NameNode from incorrectly marking DataNodes as stale or dead in highly overloaded clusters where heartbeat processing is suffering delays.  For more information, please refer to the hdfs-default.xml documentation for several new configuration properties: dfs.namenode.lifeline.rpc-address, dfs.namenode.lifeline.rpc-bind-host, dfs.datanode.lifeline.interval.seconds, dfs.namenode.lifeline.handler.ratio and dfs.namenode.lifeline.handler.count.


---

* [YARN-4762](https://issues.apache.org/jira/browse/YARN-4762) | *Blocker* | **NMs failing on DelegatingLinuxContainerRuntime init with LCE on**

Fixed CgroupHandler's creation and usage to avoid NodeManagers crashing when LinuxContainerExecutor is enabled.


---

* [HDFS-1477](https://issues.apache.org/jira/browse/HDFS-1477) | *Major* | **Support reconfiguring dfs.heartbeat.interval and dfs.namenode.heartbeat.recheck-interval without NN restart**

Steps to reconfigure:
1. change value of the parameter in corresponding xml configuration file
2. to reconfigure, run
    hdfs dfsadmin -reconfig namenode \<nn\_addr\>:\<ipc\_port\> start
3. to check status of the most recent reconfigure operation, run
    hdfs dfsadmin -reconfig namenode \<nn\_addr\>:\<ipc\_port\> status
4. to query a list reconfigurable properties on NN, run
    hdfs dfsadmin -reconfig namenode \<nn\_addr\>:\<ipc\_port\> properties


---

* [YARN-4785](https://issues.apache.org/jira/browse/YARN-4785) | *Major* | **inconsistent value type of the "type" field for LeafQueueInfo in response of RM REST API - cluster/scheduler**

Fix inconsistent value type ( String and Array ) of the "type" field for LeafQueueInfo in response of RM REST API


---

* [YARN-4732](https://issues.apache.org/jira/browse/YARN-4732) | *Trivial* | **\*ProcessTree classes have too many whitespace issues**




---

* [HADOOP-12857](https://issues.apache.org/jira/browse/HADOOP-12857) | *Major* | **Rework hadoop-tools**

<!-- markdown -->
* Turning on optional things from the tools directory such as S3 support can now be done in hadoop-env.sh with the HADOOP\_OPTIONAL\_TOOLS environment variable without impacting the various user-facing CLASSPATH variables.
* The tools directory is no longer pulled in blindly for any utilities that pull it in.  
* TOOL\_PATH / HADOOP\_TOOLS\_PATH has been broken apart and replaced with HADOOP\_TOOLS\_HOME, HADOOP\_TOOLS\_DIR and HADOOP\_TOOLS\_LIB\_JARS\_DIR to be consistent with the rest of Hadoop.


---

* [HDFS-9694](https://issues.apache.org/jira/browse/HDFS-9694) | *Major* | **Make existing DFSClient#getFileChecksum() work for striped blocks**

Makes the getFileChecksum API works with striped layout EC files. Checksum computation done by block level in the distributed fashion. The current API does not support to compare the checksum generated with normal file and the checksum generated for the same file but in striped layout.


---

* [HDFS-9640](https://issues.apache.org/jira/browse/HDFS-9640) | *Major* | **Remove hsftp from DistCp in trunk**

DistCp in Hadoop 3.0 no longer supports -mapredSSLConf option. Use global ssl-client.xml configuration file for swebhdfs file systems instead.


---

* [HDFS-9349](https://issues.apache.org/jira/browse/HDFS-9349) | *Major* | **Support reconfiguring fs.protected.directories without NN restart**

Steps to reconfigure:
1. change value of the parameter in corresponding xml configuration file
2. to reconfigure, run
    hdfs dfsadmin -reconfig namenode \<nn\_addr\>:\<ipc\_port\> start
3. to check status of the most recent reconfigure operation, run
    hdfs dfsadmin -reconfig namenode \<nn\_addr\>:\<ipc\_port\> status
4. to query a list reconfigurable properties on NN, run
    hdfs dfsadmin -reconfig namenode \<nn\_addr\>:\<ipc\_port\> properties


---

* [HADOOP-11393](https://issues.apache.org/jira/browse/HADOOP-11393) | *Major* | **Revert HADOOP\_PREFIX, go back to HADOOP\_HOME**

On Unix platforms, HADOOP\_PREFIX has been deprecated in favor of returning to HADOOP\_HOME as in prior Apache Hadoop releases.


---

* [HADOOP-12967](https://issues.apache.org/jira/browse/HADOOP-12967) | *Major* | **Remove FileUtil#copyMerge**

Removed FileUtil.copyMerge.


---

* [MAPREDUCE-6670](https://issues.apache.org/jira/browse/MAPREDUCE-6670) | *Minor* | **TestJobListCache#testEviction sometimes fails on Windows with timeout**

Backport the fix to 2.7 and 2.8


---

* [HDFS-9945](https://issues.apache.org/jira/browse/HDFS-9945) | *Major* | **Datanode command for evicting writers**

This new dfsadmin command, evictWriters, stops active block writing activities on a data node. The affected writes will continue without the node after a write pipeline recovery. This is useful when data node decommissioning is blocked by slow writers. If issued against a non-decommissioing data node, all current writers will be stopped, but new write requests will continue to be served.


---

* [HADOOP-12963](https://issues.apache.org/jira/browse/HADOOP-12963) | *Minor* | **Allow using path style addressing for accessing the s3 endpoint**

Add new flag to allow supporting path style addressing for s3a


---

* [HADOOP-12811](https://issues.apache.org/jira/browse/HADOOP-12811) | *Critical* | **Change kms server port number which conflicts with HMaster port number**

The default port for KMS service is now 9600. This is to avoid conflicts on the previous port 16000, which is also used by HMaster as the default port.


---

* [YARN-4784](https://issues.apache.org/jira/browse/YARN-4784) | *Major* | **Fairscheduler: defaultQueueSchedulingPolicy should not accept FIFO**

Clusters cannot use FIFO policy as the defaultQueueSchedulingPolicy. Clusters with a single level of queues will have to explicitly set the policy to FIFO if that is desired.


---

* [HDFS-9427](https://issues.apache.org/jira/browse/HDFS-9427) | *Critical* | **HDFS should not default to ephemeral ports**

The patch updates the HDFS default HTTP/RPC ports to non-ephemeral ports. The changes are listed below:
Namenode ports: 50470 --\> 9871, 50070 --\> 9870, 8020 --\> 9820
Secondary NN ports: 50091 --\> 9869, 50090 --\> 9868
Datanode ports: 50020 --\> 9867, 50010 --\> 9866, 50475 --\> 9865, 50075 --\> 9864


---

* [HDFS-3702](https://issues.apache.org/jira/browse/HDFS-3702) | *Minor* | **Add an option for NOT writing the blocks locally if there is a datanode on the same box as the client**

This patch will attempt to allocate all replicas to remote DataNodes, by adding local DataNode to the excluded DataNodes. If no sufficient replicas can be obtained, it will fall back to default block placement policy, which writes one replica to local DataNode.


---

* [HADOOP-12563](https://issues.apache.org/jira/browse/HADOOP-12563) | *Major* | **Updated utility to create/modify token files**

This feature introduces a new command called "hadoop dtutil" which lets users request and download delegation tokens with certain attributes.


---

* [HADOOP-13045](https://issues.apache.org/jira/browse/HADOOP-13045) | *Major* | **hadoop\_add\_classpath is not working in .hadooprc**

<!-- markdown -->
With this change, the `.hadooprc` file is now processed after Apache Hadoop has been fully bootstrapped.  This allows for usage of the Apache Hadoop Shell API.  A new file, `.hadoop-env`, now provides the ability for end users to override `hadoop-env.sh`.


---

* [MAPREDUCE-6526](https://issues.apache.org/jira/browse/MAPREDUCE-6526) | *Blocker* | **Remove usage of metrics v1 from hadoop-mapreduce**

LocalJobRunnerMetrics and ShuffleClientMetrics were updated to use Hadoop Metrics V2 framework.


---

* [HDFS-9902](https://issues.apache.org/jira/browse/HDFS-9902) | *Major* | **Support different values of dfs.datanode.du.reserved per storage type**

Reserved space can be configured independently for different storage types for clusters with heterogeneous storage. The 'dfs.datanode.du.reserved' property name can be suffixed with a storage types (i.e. one of ssd, disk, archival or ram\_disk). e.g. reserved space for RAM\_DISK storage can be configured using the property 'dfs.datanode.du.reserved.ram\_disk'. If specific storage type reservation is not configured then the value specified by 'dfs.datanode.du.reserved' will be used for all volumes.


---

* [HADOOP-12504](https://issues.apache.org/jira/browse/HADOOP-12504) | *Blocker* | **Remove metrics v1**

<!-- markdown -->
* org.apache.hadoop.metrics package was removed. Use org.apache.hadoop.metrics2 package instead.
* "/metrics" endpoint was removed. Use "/jmx" instead to see the metrics.


---

* [HDFS-10324](https://issues.apache.org/jira/browse/HDFS-10324) | *Major* | **Trash directory in an encryption zone should be pre-created with correct permissions**

HDFS will create a ".Trash" subdirectory when creating a new encryption zone to support soft delete for files deleted within the encryption zone. A new "crypto -provisionTrash" command has been introduced to provision trash directories for encryption zones created with Apache Hadoop minor releases prior to 2.8.0.


---

* [HDFS-10337](https://issues.apache.org/jira/browse/HDFS-10337) | *Minor* | **OfflineEditsViewer stats option should print 0 instead of null for the count of operations**

The output of "hdfs oev -p stats" has changed. The option prints 0 instead of null for the count of the operations that have never been executed.


---

* [HADOOP-10694](https://issues.apache.org/jira/browse/HADOOP-10694) | *Major* | **Remove synchronized input streams from Writable deserialization**

Remove invisible synchronization primitives from DataInputBuffer


---

* [HADOOP-13122](https://issues.apache.org/jira/browse/HADOOP-13122) | *Minor* | **Customize User-Agent header sent in HTTP requests by S3A.**

S3A now includes the current Hadoop version in the User-Agent string passed through the AWS SDK to the S3 service.  Users also may include optional additional information to identify their application.  See the documentation of configuration property fs.s3a.user.agent.prefix for further details.


---

* [HADOOP-11858](https://issues.apache.org/jira/browse/HADOOP-11858) | *Blocker* | **[JDK8] Set minimum version of Hadoop 3 to JDK 8**

The minimum required JDK version for Hadoop has been increased from JDK7 to JDK8.


---

* [HADOOP-12930](https://issues.apache.org/jira/browse/HADOOP-12930) | *Critical* | **[Umbrella] Dynamic subcommands for hadoop shell scripts**

It is now possible to add or modify the behavior of existing subcommands in the hadoop, hdfs, mapred, and yarn scripts. See the Unix Shell Guide for more information.


---

* [HADOOP-12782](https://issues.apache.org/jira/browse/HADOOP-12782) | *Major* | **Faster LDAP group name resolution with ActiveDirectory**

If the user object returned by LDAP server has the user's group object DN (supported by Active Directory), Hadoop can reduce LDAP group mapping latency by setting hadoop.security.group.mapping.ldap.search.attr.memberof to memberOf.


---

* [HADOOP-12723](https://issues.apache.org/jira/browse/HADOOP-12723) | *Major* | **S3A: Add ability to plug in any AWSCredentialsProvider**

Users can integrate a custom credential provider with S3A.  See documentation of configuration property fs.s3a.aws.credentials.provider for further details.


---

* [MAPREDUCE-6607](https://issues.apache.org/jira/browse/MAPREDUCE-6607) | *Minor* | **Enable regex pattern matching when mapreduce.task.files.preserve.filepattern is set**

Before this fix, the files in .staging directory are always preserved when mapreduce.task.files.preserve.filepattern is set. After this fix, the files in .staging directory are preserved if the name of the directory matches the regex pattern specified by mapreduce.task.files.preserve.filepattern.


---

* [YARN-5035](https://issues.apache.org/jira/browse/YARN-5035) | *Major* | **FairScheduler: Adjust maxAssign dynamically when assignMultiple is turned on**

Introducing a new configuration "yarn.scheduler.fair.dynamic.max.assign" to dynamically determine the resources to assign per heartbeat when assignmultiple is turned on. When turned on, the scheduler allocates roughly half of the remaining resources overriding any max.assign settings configured. This is turned ON by default.


---

* [YARN-5132](https://issues.apache.org/jira/browse/YARN-5132) | *Critical* | **Exclude generated protobuf sources from YARN Javadoc build**

Exclude javadocs for proto-generated java classes.


---

* [HADOOP-13105](https://issues.apache.org/jira/browse/HADOOP-13105) | *Major* | **Support timeouts in LDAP queries in LdapGroupsMapping.**

This patch adds two new config keys for supporting timeouts in LDAP query operations. The property "hadoop.security.group.mapping.ldap.connection.timeout.ms" is the connection timeout (in milliseconds), within which period if the LDAP provider doesn't establish a connection, it will abort the connect attempt. The property "hadoop.security.group.mapping.ldap.read.timeout.ms" is the read timeout (in milliseconds), within which period if the LDAP provider doesn't get a LDAP response, it will abort the read attempt.


---

* [HADOOP-13155](https://issues.apache.org/jira/browse/HADOOP-13155) | *Major* | **Implement TokenRenewer to renew and cancel delegation tokens in KMS**

Enables renewal and cancellation of KMS delegation tokens. hadoop.security.key.provider.path needs to be configured to reach the key provider.


---

* [HADOOP-12807](https://issues.apache.org/jira/browse/HADOOP-12807) | *Minor* | **S3AFileSystem should read AWS credentials from environment variables**

Adds support to S3AFileSystem for reading AWS credentials from environment variables.


---

* [HDFS-10375](https://issues.apache.org/jira/browse/HDFS-10375) | *Trivial* | **Remove redundant TestMiniDFSCluster.testDualClusters**

Remove redundent TestMiniDFSCluster.testDualClusters to save time.


---

* [HDFS-10220](https://issues.apache.org/jira/browse/HDFS-10220) | *Major* | **A large number of expired leases can make namenode unresponsive and cause failover**

Two new configuration have been added "dfs.namenode.lease-recheck-interval-ms" and "dfs.namenode.max-lock-hold-to-release-lease-ms" to fine tune the duty cycle with which the Namenode recovers old leases.


---

* [HADOOP-13237](https://issues.apache.org/jira/browse/HADOOP-13237) | *Minor* | **s3a initialization against public bucket fails if caller lacks any credentials**

S3A now supports read access to a public S3 bucket even if the client does not configure any AWS credentials.  See the documentation of configuration property fs.s3a.aws.credentials.provider for further details.


---

* [HADOOP-12537](https://issues.apache.org/jira/browse/HADOOP-12537) | *Minor* | **S3A to support Amazon STS temporary credentials**

S3A now supports use of AWS Security Token Service temporary credentials for authentication to S3.  Refer to the documentation of configuration property fs.s3a.session.token for further details.


---

* [HADOOP-13175](https://issues.apache.org/jira/browse/HADOOP-13175) | *Major* | **Remove hadoop-ant from hadoop-tools**

The hadoop-ant module in hadoop-tools has been removed.


---

* [HADOOP-12666](https://issues.apache.org/jira/browse/HADOOP-12666) | *Major* | **Support Microsoft Azure Data Lake - as a file system in Hadoop**

Hadoop now supports integration with Azure Data Lake as an alternative Hadoop-compatible file system.  Please refer to the Hadoop site documentation of Azure Data Lake for details on usage and configuration.


---

* [HADOOP-12892](https://issues.apache.org/jira/browse/HADOOP-12892) | *Blocker* | **fix/rewrite create-release**

This rewrites the release process with a new dev-support/bin/create-release script.  See http://wiki.apache.org/hadoop/HowToRelease for updated instructions on how to use it.


---

* [HADOOP-3733](https://issues.apache.org/jira/browse/HADOOP-3733) | *Minor* | **"s3:" URLs break when Secret Key contains a slash, even if encoded**

Allows userinfo component of URI authority to contain a slash (escaped as %2F).  Especially useful for accessing AWS S3 with distcp or hadoop fs.


---

* [HADOOP-13242](https://issues.apache.org/jira/browse/HADOOP-13242) | *Major* | **Authenticate to Azure Data Lake using client ID and keys**

Adds support for Azure ActiveDirectory tokens using client ID and keys


---

* [HADOOP-9613](https://issues.apache.org/jira/browse/HADOOP-9613) | *Major* | **[JDK8] Update jersey version to latest 1.x release**

Upgrading Jersey and its related libraries:

1. Upgrading jersey from 1.9 to 1.19
2. Adding jersey-servlet 1.19
3. Upgrading grizzly-http-servlet from 2.1.2 to 2.2.21
4. Adding grizzly-http 2.2.21
5. Adding grizzly-http-server 2.2.21

After upgrading Jersey from 1.12 to 1.13, the root element whose content is empty collection is changed from null to empty object({}).


---

* [HDFS-10328](https://issues.apache.org/jira/browse/HDFS-10328) | *Minor* | **Add per-cache-pool default replication num configuration**

Add per-cache-pool default replication num configuration


---

* [HADOOP-13203](https://issues.apache.org/jira/browse/HADOOP-13203) | *Major* | **S3A: Support fadvise "random" mode for high performance readPositioned() reads**

S3A has added support for configurable input policies.  Similar to fadvise, this configuration provides applications with a way to specify their expected access pattern (sequential or random) while reading a file.  S3A then performs optimizations tailored to that access pattern.  See site documentation of the fs.s3a.experimental.input.fadvise configuration property for more details.  Please be advised that this feature is experimental and subject to backward-incompatible changes in future releases.


---

* [HDFS-1312](https://issues.apache.org/jira/browse/HDFS-1312) | *Major* | **Re-balance disks within a Datanode**

The Disk Balancer lets administrators rebalance data across multiple disks of a DataNode. It is useful to correct skewed data distribution often seen after adding or replacing disks. Disk Balancer can be enabled by setting dfs.disk.balancer.enabled to true in hdfs-site.xml. It can be invoked by running "hdfs diskbalancer". See the "HDFS Diskbalancer"  section in the HDFS Commands guide for detailed usage.


---

* [HADOOP-13263](https://issues.apache.org/jira/browse/HADOOP-13263) | *Major* | **Reload cached groups in background after expiry**

hadoop.security.groups.cache.background.reload can be set to true to enable background reload of expired groups cache entries. This setting can improve the performance of services that use Groups.java (e.g. the NameNode) when group lookups are slow. The setting is disabled by default.


---

* [HDFS-10440](https://issues.apache.org/jira/browse/HDFS-10440) | *Major* | **Improve DataNode web UI**

DataNode Web UI has been improved with new HTML5 page, showing useful information.


---

* [HADOOP-13209](https://issues.apache.org/jira/browse/HADOOP-13209) | *Major* | **replace slaves with workers**

The 'slaves' file has been deprecated in favor of the 'workers' file and, other than the deprecation warnings, all references to slavery have been removed from the source tree.


---

* [HDFS-6434](https://issues.apache.org/jira/browse/HDFS-6434) | *Minor* | **Default permission for creating file should be 644 for WebHdfs/HttpFS**

The default permissions of files and directories created via WebHDFS and HttpFS are now 644 and 755 respectively. See HDFS-10488 for related discussion.


---

* [HADOOP-12864](https://issues.apache.org/jira/browse/HADOOP-12864) | *Blocker* | **Remove bin/rcc script**

The rcc command has been removed. See HADOOP-12485 where unused Hadoop Streaming classes were removed.


---

* [HADOOP-12709](https://issues.apache.org/jira/browse/HADOOP-12709) | *Major* | **Cut s3:// from trunk**

The s3 file system has been removed. The s3a file system should be used instead.


---

* [HADOOP-12064](https://issues.apache.org/jira/browse/HADOOP-12064) | *Blocker* | **[JDK8] Update guice version to 4.0**

Upgrading following dependences:
\* Guice from 3.0 to 4.0
\* cglib from 2.2 to 3.2.0
\* asm from 3.2 to 5.0.4


---

* [HDFS-10548](https://issues.apache.org/jira/browse/HDFS-10548) | *Major* | **Remove the long deprecated BlockReaderRemote**

This removes the configuration property {{dfs.client.use.legacy.blockreader}}, since the legacy remote block reader class has been removed from the codebase.


---

* [YARN-2928](https://issues.apache.org/jira/browse/YARN-2928) | *Critical* | **YARN Timeline Service v.2: alpha 1**

We are introducing an early preview (alpha 1) of a major revision of YARN Timeline Service: v.2. YARN Timeline Service v.2 addresses two major challenges: improving scalability and reliability of Timeline Service, and enhancing usability by introducing flows and aggregation.

YARN Timeline Service v.2 alpha 1 is provided so that users and developers can test it and provide feedback and suggestions for making it a ready replacement for Timeline Service v.1.x. It should be used only in a test capacity. Most importantly, security is not enabled. Do not set up or use Timeline Service v.2 until security is implemented if security is a critical requirement.

More details are available in the [YARN Timeline Service v.2](./hadoop-yarn/hadoop-yarn-site/TimelineServiceV2.html) documentation.


---

* [HADOOP-13301](https://issues.apache.org/jira/browse/HADOOP-13301) | *Minor* | **Millisecond timestamp for FsShell console log and MapReduce jobsummary log**

The time format of console logger and MapReduce job summary logger is ISO8601 by default to print milliseconds.


---

* [HADOOP-13382](https://issues.apache.org/jira/browse/HADOOP-13382) | *Major* | **remove unneeded commons-httpclient dependencies from POM files in Hadoop and sub-projects**

Dependencies on commons-httpclient have been removed. Projects with undeclared transitive dependencies on commons-httpclient, previously provided via hadoop-common or hadoop-client, may find this to be an incompatible change. Such project are also potentially exposed to the commons-httpclient CVE, and should be fixed for that reason as well.


---

* [HADOOP-13354](https://issues.apache.org/jira/browse/HADOOP-13354) | *Major* | **Update WASB driver to use the latest version (4.2.0) of SDK for Microsoft Azure Storage Clients**

The WASB FileSystem now uses version 4.2.0 of the Azure Storage SDK.


---

* [HDFS-10519](https://issues.apache.org/jira/browse/HDFS-10519) | *Minor* | **Add a configuration option to enable in-progress edit log tailing**

Add a configuration option to enable in-progress edit tailing and a related unit test


---

* [HDFS-10650](https://issues.apache.org/jira/browse/HDFS-10650) | *Minor* | **DFSClient#mkdirs and DFSClient#primitiveMkdir should use default directory permission**

If the caller does not supply a permission, DFSClient#mkdirs and DFSClient#primitiveMkdir will create a new directory with the default directory permission 00777 now, instead of 00666.


---

* [HDFS-10689](https://issues.apache.org/jira/browse/HDFS-10689) | *Minor* | **Hdfs dfs chmod should reset sticky bit permission when the bit is omitted in the octal mode**

Hdfs dfs chmod command will reset sticky bit permission on a file/directory when the leading sticky bit is omitted in the octal mode (like 644). So when a file/directory permission is applied using octal mode and sticky bit permission needs to be preserved, then it has to be explicitly mentioned in the permission bits (like 1644). This behavior is similar to many other filesystems on Linux/BSD.


---

* [HADOOP-13403](https://issues.apache.org/jira/browse/HADOOP-13403) | *Major* | **AzureNativeFileSystem rename/delete performance improvements**

WASB has added an optional capability to execute certain FileSystem operations in parallel on multiple threads for improved performance.  Please refer to the Azure Blob Storage documentation page for more information on how to enable and control the feature.


---

* [HADOOP-12747](https://issues.apache.org/jira/browse/HADOOP-12747) | *Major* | **support wildcard in libjars argument**

It is now possible to specify multiple jar files for the libjars argument using a wildcard. For example, you can specify "-libjars 'libs/\*'" as a shorthand for all jars in the libs directory.


---

* [YARN-5137](https://issues.apache.org/jira/browse/YARN-5137) | *Major* | **Make DiskChecker pluggable in NodeManager**

Added new plugin property yarn.nodemanager.disk-validator to allow the NodeManager to use an alternate class for checking whether a disk is good or not.


---

* [HDFS-10725](https://issues.apache.org/jira/browse/HDFS-10725) | *Minor* | **Caller context should always be constructed by a builder**

Previously, CallerContext was constructed by a builder. In this new pattern, the constructor is private so that caller context will always be constructed by a builder.


---

* [HDFS-7933](https://issues.apache.org/jira/browse/HDFS-7933) | *Major* | **fsck should also report decommissioning replicas.**

The output of hdfs fsck now also contains information about decommissioning replicas.


---

* [HADOOP-13208](https://issues.apache.org/jira/browse/HADOOP-13208) | *Minor* | **S3A listFiles(recursive=true) to do a bulk listObjects instead of walking the pseudo-tree of directories**

S3A has optimized the listFiles method by doing a bulk listing of all entries under a path in a single S3 operation instead of recursively walking the directory tree.  The listLocatedStatus method has been optimized by fetching results from S3 lazily as the caller traverses the returned iterator instead of doing an eager fetch of all possible results.


---

* [HADOOP-13252](https://issues.apache.org/jira/browse/HADOOP-13252) | *Minor* | **Tune S3A provider plugin mechanism**

S3A now supports configuration of multiple credential provider classes for authenticating to S3.  These are loaded and queried in sequence for a valid set of credentials.  For more details, refer to the description of the fs.s3a.aws.credentials.provider configuration property or the S3A documentation page.


---

* [HDFS-8312](https://issues.apache.org/jira/browse/HDFS-8312) | *Critical* | **Trash does not descent into child directories to check for permissions**

Permissions are now checked when moving a file to Trash.


---

* [HADOOP-12726](https://issues.apache.org/jira/browse/HADOOP-12726) | *Major* | **Unsupported FS operations should throw UnsupportedOperationException**

Unsupported FileSystem operations now throw an UnsupportedOperationException rather than an IOException.


---

* [HDFS-8986](https://issues.apache.org/jira/browse/HDFS-8986) | *Major* | **Add option to -du to calculate directory space usage excluding snapshots**

Add a -x option for "hdfs -du" and "hdfs -count" commands to exclude snapshots from being calculated.


---

* [HADOOP-13534](https://issues.apache.org/jira/browse/HADOOP-13534) | *Minor* | **Remove unused TrashPolicy#getInstance and initialize code**

TrashPolicy#getInstance and initialize with Path were removed. Use the method without Path instead.


---

* [YARN-5567](https://issues.apache.org/jira/browse/YARN-5567) | *Major* | **Fix script exit code checking in NodeHealthScriptRunner#reportHealthStatus**

Prior to this fix, the NodeManager will ignore any non-zero exit code for any script in the yarn.nodemanager.health-checker.script.path property.  With this change, any syntax errors in the health checking script will get flagged as an error in the same fashion (likely exit code 1) that the script detecting a health issue.


---

* [HADOOP-10597](https://issues.apache.org/jira/browse/HADOOP-10597) | *Major* | **RPC Server signals backoff to clients when all request queues are full**

This change introduces a new configuration key used by RPC server to decide whether to send backoff signal to RPC Client when RPC call queue is full. When the feature is enabled, RPC server will no longer block on the processing of RPC requests when RPC call queue is full. It helps to improve quality of service when the service is under heavy load. The configuration key is in the format of "ipc.#port#.backoff.enable" where #port# is the port number that RPC server listens on. For example, if you want to enable the feature for the RPC server that listens on 8020, set ipc.8020.backoff.enable to true.


---

* [HDFS-9016](https://issues.apache.org/jira/browse/HDFS-9016) | *Major* | **Display upgrade domain information in fsck**

New fsck option "-upgradedomains" has been added to display upgrade domains of any block.


---

* [HDFS-8818](https://issues.apache.org/jira/browse/HDFS-8818) | *Major* | **Allow Balancer to run faster**

Add a new conf "dfs.balancer.max-size-to-move" so that Balancer.MAX\_SIZE\_TO\_MOVE becomes configurable.


---

* [HDFS-2538](https://issues.apache.org/jira/browse/HDFS-2538) | *Minor* | **option to disable fsck dots**

fsck does not print out dots for progress reporting by default. To print out dots, you should specify '-showprogress' option.


---

* [YARN-5049](https://issues.apache.org/jira/browse/YARN-5049) | *Major* | **Extend NMStateStore to save queued container information**

This breaks rolling upgrades because it changes the major version of the NM state store schema. Therefore when a new NM comes up on an old state store it crashes.

The state store versions for this change have been updated in YARN-6798.

