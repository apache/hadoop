
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
# Apache Hadoop  0.23.1 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [MAPREDUCE-2784](https://issues.apache.org/jira/browse/MAPREDUCE-2784) | *Major* | **[Gridmix] TestGridmixSummary fails with NPE when run in DEBUG mode.**

Fixed bugs in ExecutionSummarizer and ResourceUsageMatcher.


---

* [MAPREDUCE-2950](https://issues.apache.org/jira/browse/MAPREDUCE-2950) | *Major* | **[Gridmix] TestUserResolve fails in trunk**

Fixes bug in TestUserResolve.


---

* [HDFS-2130](https://issues.apache.org/jira/browse/HDFS-2130) | *Major* | **Switch default checksum to CRC32C**

The default checksum algorithm used on HDFS is now CRC32C. Data from previous versions of Hadoop can still be read backwards-compatibly.


---

* [HDFS-2129](https://issues.apache.org/jira/browse/HDFS-2129) | *Major* | **Simplify BlockReader to not inherit from FSInputChecker**

BlockReader has been reimplemented to use direct byte buffers. If you use a custom socket factory, it must generate sockets that have associated Channels.


---

* [MAPREDUCE-3297](https://issues.apache.org/jira/browse/MAPREDUCE-3297) | *Major* | **Move Log Related components from yarn-server-nodemanager to yarn-common**

Moved log related components into yarn-common so that HistoryServer and clients can use them without depending on the yarn-server-nodemanager module.


---

* [MAPREDUCE-3221](https://issues.apache.org/jira/browse/MAPREDUCE-3221) | *Minor* | **ant test TestSubmitJob failing on trunk**

Fixed a bug in TestSubmitJob.


---

* [MAPREDUCE-3215](https://issues.apache.org/jira/browse/MAPREDUCE-3215) | *Minor* | **org.apache.hadoop.mapreduce.TestNoJobSetupCleanup failing on trunk**

Reneabled and fixed bugs in the failing test TestNoJobSetupCleanup.


---

* [MAPREDUCE-3219](https://issues.apache.org/jira/browse/MAPREDUCE-3219) | *Minor* | **ant test TestDelegationToken failing on trunk**

Reenabled and fixed bugs in the failing test TestDelegationToken.


---

* [MAPREDUCE-3217](https://issues.apache.org/jira/browse/MAPREDUCE-3217) | *Minor* | **ant test TestAuditLogger fails on trunk**

Reenabled and fixed bugs in the failing ant test TestAuditLogger.


---

* [MAPREDUCE-3342](https://issues.apache.org/jira/browse/MAPREDUCE-3342) | *Critical* | **JobHistoryServer doesn't show job queue**

Fixed JobHistoryServer to also show the job's queue name.


---

* [MAPREDUCE-3345](https://issues.apache.org/jira/browse/MAPREDUCE-3345) | *Major* | **Race condition in ResourceManager causing TestContainerManagerSecurity to fail sometimes**

Fixed a race condition in ResourceManager that was causing TestContainerManagerSecurity to fail sometimes.


---

* [MAPREDUCE-3368](https://issues.apache.org/jira/browse/MAPREDUCE-3368) | *Critical* | **compile-mapred-test fails**

Fixed ant test compilation.


---

* [MAPREDUCE-2733](https://issues.apache.org/jira/browse/MAPREDUCE-2733) | *Major* | **Gridmix v3 cpu emulation system tests.**

Adds system tests for the CPU emulation feature in Gridmix3.


---

* [MAPREDUCE-3333](https://issues.apache.org/jira/browse/MAPREDUCE-3333) | *Blocker* | **MR AM for sort-job going out of memory**

Fixed bugs in ContainerLauncher of MR AppMaster due to which per-container connections to NodeManager were lingering long enough to hit the ulimits on number of processes.


---

* [MAPREDUCE-3280](https://issues.apache.org/jira/browse/MAPREDUCE-3280) | *Major* | **MR AM should not read the username from configuration**

Removed the unnecessary job user-name configuration in mapred-site.xml.


---

* [MAPREDUCE-3392](https://issues.apache.org/jira/browse/MAPREDUCE-3392) | *Blocker* | **Cluster.getDelegationToken() throws NPE if client.getDelegationToken() returns null.**

Fixed Cluster's getDelegationToken's API to return null when there isn't a supported token.


---

* [MAPREDUCE-3379](https://issues.apache.org/jira/browse/MAPREDUCE-3379) | *Major* | **LocalResourceTracker should not tracking deleted cache entries**

Fixed LocalResourceTracker in NodeManager to remove deleted cache entries correctly.


---

* [MAPREDUCE-3325](https://issues.apache.org/jira/browse/MAPREDUCE-3325) | *Major* | **Improvements to CapacityScheduler doc**

document changes only.


---

* [MAPREDUCE-3375](https://issues.apache.org/jira/browse/MAPREDUCE-3375) | *Major* | **Memory Emulation system tests.**

Added system tests to test the memory emulation feature in Gridmix.


---

* [MAPREDUCE-3102](https://issues.apache.org/jira/browse/MAPREDUCE-3102) | *Major* | **NodeManager should fail fast with wrong configuration or permissions for LinuxContainerExecutor**

Changed NodeManager to fail fast when LinuxContainerExecutor has wrong configuration or permissions.


---

* [MAPREDUCE-3355](https://issues.apache.org/jira/browse/MAPREDUCE-3355) | *Blocker* | **AM scheduling hangs frequently with sort job on 350 nodes**

Fixed MR AM's ContainerLauncher to handle node-command timeouts correctly.


---

* [MAPREDUCE-3407](https://issues.apache.org/jira/browse/MAPREDUCE-3407) | *Minor* | **Wrong jar getting used in TestMR\*Jobs\* for MiniMRYarnCluster**

Fixed pom files to refer to the correct MR app-jar needed by the integration tests.


---

* [HADOOP-7802](https://issues.apache.org/jira/browse/HADOOP-7802) | *Major* | **Hadoop scripts unconditionally source "$bin"/../libexec/hadoop-config.sh.**

Here is a patch to enable this behavior


---

* [MAPREDUCE-3412](https://issues.apache.org/jira/browse/MAPREDUCE-3412) | *Major* | **'ant docs' is broken**

Fixes 'ant docs' by removing stale references to capacity-scheduler docs.


---

* [HDFS-2246](https://issues.apache.org/jira/browse/HDFS-2246) | *Major* | **Shortcut a local client reads to a Datanodes files directly**

1. New configurations
a. dfs.block.local-path-access.user is the key in datanode configuration to specify the user allowed to do short circuit read.
b. dfs.client.read.shortcircuit is the key to enable short circuit read at the client side configuration.
c. dfs.client.read.shortcircuit.skip.checksum is the key to bypass checksum check at the client side.
2. By default none of the above are enabled and short circuit read will not kick in.
3. If security is on, the feature can be used only for user that has kerberos credentials at the client, therefore map reduce tasks cannot benefit from it in general.


---

* [HADOOP-7851](https://issues.apache.org/jira/browse/HADOOP-7851) | *Major* | **Configuration.getClasses() never returns the default value.**

Fixed Configuration.getClasses() API to return the default value if the key is not set.


---

* [MAPREDUCE-3519](https://issues.apache.org/jira/browse/MAPREDUCE-3519) | *Blocker* | **Deadlock in LocalDirsHandlerService and ShuffleHandler**

Fixed a deadlock in NodeManager LocalDirectories's handling service.


---

* [MAPREDUCE-2863](https://issues.apache.org/jira/browse/MAPREDUCE-2863) | *Blocker* | **Support web-services for RM & NM**

Support for web-services in YARN and MR components.


---

* [MAPREDUCE-3426](https://issues.apache.org/jira/browse/MAPREDUCE-3426) | *Blocker* | **uber-jobs tried to write outputs into wrong dir**

Fixed MR AM in uber mode to write map intermediate outputs in the correct directory to work properly in secure mode.


---

* [MAPREDUCE-3398](https://issues.apache.org/jira/browse/MAPREDUCE-3398) | *Blocker* | **Log Aggregation broken in Secure Mode**

Fixed log aggregation to work correctly in secure mode. Contributed by Siddharth Seth.


---

* [MAPREDUCE-3530](https://issues.apache.org/jira/browse/MAPREDUCE-3530) | *Blocker* | **Sometimes NODE\_UPDATE to the scheduler throws an NPE causing the scheduling to stop**

Fixed an NPE occuring during scheduling in the ResourceManager.


---

* [MAPREDUCE-3484](https://issues.apache.org/jira/browse/MAPREDUCE-3484) | *Major* | **JobEndNotifier is getting interrupted before completing all its retries.**

Fixed JobEndNotifier to not get interrupted before completing all its retries.


---

* [MAPREDUCE-3487](https://issues.apache.org/jira/browse/MAPREDUCE-3487) | *Critical* | **jobhistory web ui task counters no longer links to singletakecounter page**

Fixed JobHistory web-UI to display links to single task's counters' page.


---

* [MAPREDUCE-3564](https://issues.apache.org/jira/browse/MAPREDUCE-3564) | *Blocker* | **TestStagingCleanup and TestJobEndNotifier are failing on trunk.**

Fixed failures in TestStagingCleanup and TestJobEndNotifier tests.


---

* [MAPREDUCE-778](https://issues.apache.org/jira/browse/MAPREDUCE-778) | *Major* | **[Rumen] Need a standalone JobHistory log anonymizer**

Added an anonymizer tool to Rumen. Anonymizer takes a Rumen trace file and/or topology as input. It supports persistence and plugins to override the default behavior.


---

* [MAPREDUCE-3387](https://issues.apache.org/jira/browse/MAPREDUCE-3387) | *Critical* | **A tracking URL of N/A before the app master is launched breaks oozie**

Fixed AM's tracking URL to always go through the proxy, even before the job started, so that it works properly with oozie throughout the job execution.


---

* [MAPREDUCE-3339](https://issues.apache.org/jira/browse/MAPREDUCE-3339) | *Blocker* | **Job is getting hanged indefinitely,if the child processes are killed on the NM.  KILL\_CONTAINER eventtype is continuosly sent to the containers that are not existing**

Fixed MR AM to stop considering node blacklisting after the number of nodes blacklisted crosses a threshold.


---

* [MAPREDUCE-3349](https://issues.apache.org/jira/browse/MAPREDUCE-3349) | *Blocker* | **No rack-name logged in JobHistory for unsuccessful tasks**

Unsuccessful tasks now log hostname and rackname to job history.


---

* [MAPREDUCE-3586](https://issues.apache.org/jira/browse/MAPREDUCE-3586) | *Blocker* | **Lots of AMs hanging around in PIG testing**

Modified CompositeService to avoid duplicate stop operations thereby solving race conditions in MR AM shutdown.


---

* [MAPREDUCE-3597](https://issues.apache.org/jira/browse/MAPREDUCE-3597) | *Major* | **Provide a way to access other info of history file from Rumentool**

Rumen now provides {{Parsed\*}} objects. These objects provide extra information that are not provided by {{Logged\*}} objects.


---

* [MAPREDUCE-3399](https://issues.apache.org/jira/browse/MAPREDUCE-3399) | *Blocker* | **ContainerLocalizer should request new resources after completing the current one**

Modified ContainerLocalizer to send a heartbeat to NM immediately after downloading a resource instead of always waiting for a second.


---

* [MAPREDUCE-3568](https://issues.apache.org/jira/browse/MAPREDUCE-3568) | *Critical* | **Optimize Job's progress calculations in MR AM**

Optimized Job's progress calculations in MR AM.


---

* [HADOOP-7348](https://issues.apache.org/jira/browse/HADOOP-7348) | *Major* | **Modify the option of FsShell getmerge from [addnl] to [-nl] for consistency**

The 'fs -getmerge' tool now uses a -nl flag to determine if adding a newline at end of each file is required, in favor of the 'addnl' boolean flag that was used earlier.


---

* [MAPREDUCE-3462](https://issues.apache.org/jira/browse/MAPREDUCE-3462) | *Blocker* | **Job submission failing in JUnit tests**

Fixed failing JUnit tests in Gridmix.


---

* [HDFS-1314](https://issues.apache.org/jira/browse/HDFS-1314) | *Minor* | **dfs.blocksize accepts only absolute value**

The default blocksize property 'dfs.blocksize' now accepts unit symbols to be used instead of byte length. Values such as "10k", "128m", "1g" are now OK to provide instead of just no. of bytes as was before.


---

* [MAPREDUCE-3490](https://issues.apache.org/jira/browse/MAPREDUCE-3490) | *Blocker* | **RMContainerAllocator counts failed maps towards Reduce ramp up**

Fixed MapReduce AM to count failed maps also towards Reduce ramp up.


---

* [MAPREDUCE-3511](https://issues.apache.org/jira/browse/MAPREDUCE-3511) | *Blocker* | **Counters occupy a good part of AM heap**

Removed a multitude of cloned/duplicate counters in the AM thereby reducing the AM heap size and preventing full GCs.


---

* [HADOOP-7963](https://issues.apache.org/jira/browse/HADOOP-7963) | *Blocker* | **test failures: TestViewFileSystemWithAuthorityLocalFileSystem and TestViewFileSystemLocalFileSystem**

Fix ViewFS to catch a null canonical service-name and pass tests TestViewFileSystem\*


---

* [MAPREDUCE-3528](https://issues.apache.org/jira/browse/MAPREDUCE-3528) | *Major* | **The task timeout check interval should be configurable independent of mapreduce.task.timeout**

Fixed TaskHeartBeatHandler to use a new configuration for the thread loop interval separate from task-timeout configuration property.


---

* [MAPREDUCE-3639](https://issues.apache.org/jira/browse/MAPREDUCE-3639) | *Blocker* | **TokenCache likely broken for FileSystems which don't issue delegation tokens**

Fixed TokenCache to work with absent FileSystem canonical service-names.


---

* [MAPREDUCE-3312](https://issues.apache.org/jira/browse/MAPREDUCE-3312) | *Major* | **Make MR AM not send a stopContainer w/o corresponding start container**

Modified MR AM to not send a stop-container request for a container that isn't launched at all.


---

* [MAPREDUCE-3382](https://issues.apache.org/jira/browse/MAPREDUCE-3382) | *Critical* | **Network ACLs can prevent AMs to ping the Job-end notification URL**

Enhanced MR AM to use a proxy to ping the job-end notification URL.


---

* [MAPREDUCE-3299](https://issues.apache.org/jira/browse/MAPREDUCE-3299) | *Minor* | **Add AMInfo table to the AM job page**

Added AMInfo table to the MR AM job pages to list all the job-attempts when AM restarts and recovers.


---

* [MAPREDUCE-3618](https://issues.apache.org/jira/browse/MAPREDUCE-3618) | *Major* | **TaskHeartbeatHandler holds a global lock for all task-updates**

Fixed TaskHeartbeatHandler to not hold a global lock for all task-updates.


---

* [MAPREDUCE-3512](https://issues.apache.org/jira/browse/MAPREDUCE-3512) | *Blocker* | **Batch jobHistory disk flushes**

Batching JobHistory flushing to DFS so that we don't flush for every event slowing down AM.


---

* [MAPREDUCE-3656](https://issues.apache.org/jira/browse/MAPREDUCE-3656) | *Blocker* | **Sort job on 350 scale is consistently failing with latest MRV2 code**

Fixed a race condition in MR AM which is failing the sort benchmark consistently.


---

* [MAPREDUCE-3532](https://issues.apache.org/jira/browse/MAPREDUCE-3532) | *Critical* | **When 0 is provided as port number in yarn.nodemanager.webapp.address, NMs webserver component picks up random port, NM keeps on Reporting 0 port to RM**

Modified NM to report correct http address when an ephemeral web port is configured.


---

* [MAPREDUCE-3404](https://issues.apache.org/jira/browse/MAPREDUCE-3404) | *Critical* | **Speculative Execution: speculative map tasks launched even if -Dmapreduce.map.speculative=false**

Corrected MR AM to honor speculative configuration and enable speculating either maps or reduces.


---

* [MAPREDUCE-3641](https://issues.apache.org/jira/browse/MAPREDUCE-3641) | *Blocker* | **CapacityScheduler should be more conservative assigning off-switch requests**

Making CapacityScheduler more conservative so as to assign only one off-switch container in a single scheduling iteration.


---

* [HADOOP-7986](https://issues.apache.org/jira/browse/HADOOP-7986) | *Major* | **Add config for History Server protocol in hadoop-policy for service level authorization.**

Adding config for MapReduce History Server protocol in hadoop-policy.xml for service level authorization.


---

* [MAPREDUCE-3549](https://issues.apache.org/jira/browse/MAPREDUCE-3549) | *Blocker* | **write api documentation for web service apis for RM, NM, mapreduce app master, and job history server**

new files added: A      hadoop-mapreduce-project/hadoop-yarn/hadoop-yarn-site/src/site/apt/WebServicesIntro.apt.vm
A      hadoop-mapreduce-project/hadoop-yarn/hadoop-yarn-site/src/site/apt/NodeManagerRest.apt.vm
A      hadoop-mapreduce-project/hadoop-yarn/hadoop-yarn-site/src/site/apt/ResourceManagerRest.apt.vm
A      hadoop-mapreduce-project/hadoop-yarn/hadoop-yarn-site/src/site/apt/MapredAppMasterRest.apt.vm
A      hadoop-mapreduce-project/hadoop-yarn/hadoop-yarn-site/src/site/apt/HistoryServerRest.apt.vm

The hadoop-project/src/site/site.xml is split into separate patch.


---

* [MAPREDUCE-3710](https://issues.apache.org/jira/browse/MAPREDUCE-3710) | *Major* | **last split generated by FileInputFormat.getSplits may not have the best locality**

Improved FileInputFormat to return better locality for the last split.


---

* [MAPREDUCE-3714](https://issues.apache.org/jira/browse/MAPREDUCE-3714) | *Blocker* | **Reduce hangs in a corner case**

Fixed EventFetcher and Fetcher threads to shut-down properly so that reducers don't hang in corner cases.


---

* [MAPREDUCE-3630](https://issues.apache.org/jira/browse/MAPREDUCE-3630) | *Critical* | **NullPointerException running teragen**

Committed to trunk and branch-0.23. Thanks Mahadev.


---

* [MAPREDUCE-3713](https://issues.apache.org/jira/browse/MAPREDUCE-3713) | *Blocker* | **Incorrect headroom reported to jobs**

Fixed the way head-room is allocated to applications by CapacityScheduler so that it deducts current-usage per user and not per-application.


---

* [MAPREDUCE-2765](https://issues.apache.org/jira/browse/MAPREDUCE-2765) | *Major* | **DistCp Rewrite**

DistCpV2 added to hadoop-tools.


---

* [MAPREDUCE-3699](https://issues.apache.org/jira/browse/MAPREDUCE-3699) | *Major* | **Default RPC handlers are very low for YARN servers**

Increased RPC handlers for all YARN servers to reasonable values for working at scale.


---

* [MAPREDUCE-3360](https://issues.apache.org/jira/browse/MAPREDUCE-3360) | *Critical* | **Provide information about lost nodes in the UI.**

Added information about lost/rebooted/decommissioned nodes on the webapps.


---

* [MAPREDUCE-3720](https://issues.apache.org/jira/browse/MAPREDUCE-3720) | *Major* | **Command line listJobs should not visit each AM**

Changed bin/mapred job -list to not print job-specific information not available at RM.

Very minor incompatibility in cmd-line output, inevitable due to MRv2 architecture.


---

* [MAPREDUCE-3732](https://issues.apache.org/jira/browse/MAPREDUCE-3732) | *Blocker* | **CS should only use 'activeUsers with pending requests' for computing user-limits**

Modified CapacityScheduler to use only users with pending requests for computing user-limits.


---

* [MAPREDUCE-3481](https://issues.apache.org/jira/browse/MAPREDUCE-3481) | *Major* | **[Gridmix] Improve STRESS mode locking**

Modified Gridmix STRESS mode locking structure. The submitted thread and the polling thread now run simultaneously without blocking each other.


---

* [MAPREDUCE-3703](https://issues.apache.org/jira/browse/MAPREDUCE-3703) | *Critical* | **ResourceManager should provide node lists in JMX output**

New JMX Bean in ResourceManager to provide list of live node managers:

Hadoop:service=ResourceManager,name=RMNMInfo LiveNodeManagers


---

* [MAPREDUCE-3716](https://issues.apache.org/jira/browse/MAPREDUCE-3716) | *Blocker* | **java.io.File.createTempFile fails in map/reduce tasks**

Fixing YARN+MR to allow MR jobs to be able to use java.io.File.createTempFile to create temporary files as part of their tasks.


---

* [MAPREDUCE-3754](https://issues.apache.org/jira/browse/MAPREDUCE-3754) | *Major* | **RM webapp should have pages filtered based on App-state**

Modified RM UI to filter applications based on state of the applications.


---

* [MAPREDUCE-3774](https://issues.apache.org/jira/browse/MAPREDUCE-3774) | *Major* | **yarn-default.xml should be moved to hadoop-yarn-common.**

MAPREDUCE-3774. Moved yarn-default.xml to hadoop-yarn-common from hadoop-server-common.


---

* [HADOOP-7470](https://issues.apache.org/jira/browse/HADOOP-7470) | *Minor* | **move up to Jackson 1.8.8**

**WARNING: No release note provided for this change.**


---

* [MAPREDUCE-3752](https://issues.apache.org/jira/browse/MAPREDUCE-3752) | *Blocker* | **Headroom should be capped by queue max-cap**

Modified application limits to include queue max-capacities besides the usual user limits.


---

* [MAPREDUCE-3784](https://issues.apache.org/jira/browse/MAPREDUCE-3784) | *Major* | **maxActiveApplications(\|PerUser) per queue is too low for small clusters**

Fixed CapacityScheduler so that maxActiveApplication and maxActiveApplicationsPerUser per queue are not too low for small clusters.


---

* [HADOOP-8013](https://issues.apache.org/jira/browse/HADOOP-8013) | *Major* | **ViewFileSystem does not honor setVerifyChecksum**

**WARNING: No release note provided for this change.**


---

* [MAPREDUCE-3711](https://issues.apache.org/jira/browse/MAPREDUCE-3711) | *Blocker* | **AppMaster recovery for Medium to large jobs take long time**

Fixed MR AM recovery so that only single selected task output is recovered and thus reduce the unnecessarily bloated recovery time.


---

* [MAPREDUCE-3760](https://issues.apache.org/jira/browse/MAPREDUCE-3760) | *Major* | **Blacklisted NMs should not appear in Active nodes list**

Changed active nodes list to not contain unhealthy nodes on the webUI and metrics.


---

* [MAPREDUCE-3417](https://issues.apache.org/jira/browse/MAPREDUCE-3417) | *Blocker* | **job access controls not working app master and job history UI's**

Fixed job-access-controls to work with MR AM and JobHistoryServer web-apps.


---

* [MAPREDUCE-3808](https://issues.apache.org/jira/browse/MAPREDUCE-3808) | *Blocker* | **NPE in FileOutputCommitter when running a 0 reduce job**

Fixed an NPE in FileOutputCommitter for jobs with maps but no reduces.


---

* [MAPREDUCE-3804](https://issues.apache.org/jira/browse/MAPREDUCE-3804) | *Major* | **yarn webapp interface vulnerable to cross scripting attacks**

fix cross scripting attacks vulnerability through webapp interface.


---

* [MAPREDUCE-3815](https://issues.apache.org/jira/browse/MAPREDUCE-3815) | *Critical* | **Data Locality suffers if the AM asks for containers using IPs instead of hostnames**

Fixed MR AM to always use hostnames and never IPs when requesting containers so that scheduler can give off data local containers correctly.


---

* [MAPREDUCE-3834](https://issues.apache.org/jira/browse/MAPREDUCE-3834) | *Critical* | **If multiple hosts for a split belong to the same rack, the rack is added multiple times in the AM request table**

Changed MR AM to not add the same rack entry multiple times into the container request table when multiple hosts for a split happen to be on the same rack


---

* [MAPREDUCE-3846](https://issues.apache.org/jira/browse/MAPREDUCE-3846) | *Critical* | **Restarted+Recovered AM hangs in some corner cases**

Addressed MR AM hanging issues during AM restart and then the recovery.


---

* [HADOOP-8009](https://issues.apache.org/jira/browse/HADOOP-8009) | *Critical* | **Create hadoop-client and hadoop-minicluster artifacts for downstream projects**

Generate integration artifacts "org.apache.hadoop:hadoop-client" and "org.apache.hadoop:hadoop-minicluster" containing all the jars needed to use Hadoop client APIs, and to run Hadoop MiniClusters, respectively.  Push these artifacts to the maven repository when mvn-deploy, along with existing artifacts.


---

* [MAPREDUCE-3802](https://issues.apache.org/jira/browse/MAPREDUCE-3802) | *Critical* | **If an MR AM dies twice  it looks like the process freezes**

Added test to validate that AM can crash multiple times and still can recover successfully after MAPREDUCE-3846.


---

* [MAPREDUCE-3854](https://issues.apache.org/jira/browse/MAPREDUCE-3854) | *Major* | **Reinstate environment variable tests in TestMiniMRChildTask**

Fixed and reenabled tests related to MR child JVM's environmental variables in TestMiniMRChildTask.


---

* [HDFS-2316](https://issues.apache.org/jira/browse/HDFS-2316) | *Major* | **[umbrella] WebHDFS: a complete FileSystem implementation for accessing HDFS over HTTP**

Provide WebHDFS as a complete FileSystem implementation for accessing HDFS over HTTP.
Previous hftp feature was a read-only FileSystem and does not provide "write" accesses.



