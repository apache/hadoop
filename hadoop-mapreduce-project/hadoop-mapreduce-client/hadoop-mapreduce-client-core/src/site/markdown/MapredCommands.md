<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

MapReduce Commands Guide
========================

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

Overview
--------

All mapreduce commands are invoked by the `bin/mapred` script. Running the mapred script without any arguments prints the description for all commands.

Usage: `mapred [SHELL_OPTIONS] COMMAND [GENERIC_OPTIONS] [COMMAND_OPTIONS]`

Hadoop has an option parsing framework that employs parsing generic options as well as running classes.

| COMMAND\_OPTIONS | Description |
|:---- |:---- |
| SHELL\_OPTIONS | The common set of shell options. These are documented on the [Hadoop Commands Reference](../../hadoop-project-dist/hadoop-common/CommandsManual.html#Shell_Options) page. |
| GENERIC\_OPTIONS | The common set of options supported by multiple commands. See the [Hadoop Commands Reference](../../hadoop-project-dist/hadoop-common/CommandsManual.html#Generic_Options) for more information. |
| COMMAND COMMAND\_OPTIONS | Various commands with their options are described in the following sections. The commands have been grouped into [User Commands](#User_Commands) and [Administration Commands](#Administration_Commands). |

User Commands
-------------

Commands useful for users of a hadoop cluster.

### `archive`

Creates a hadoop archive. More information can be found at
[Hadoop Archives Guide](../../hadoop-archives/HadoopArchives.html).

### `archive-logs`

A tool to combine YARN aggregated logs into Hadoop archives to reduce the number
of files in HDFS. More information can be found at
[Hadoop Archive Logs Guide](../../hadoop-archive-logs/HadoopArchiveLogs.html).

### `classpath`

Usage: `yarn classpath [--glob |--jar <path> |-h |--help]`

| COMMAND\_OPTION | Description |
|:---- |:---- |
| `--glob` | expand wildcards |
| `--jar` *path* | write classpath as manifest in jar named *path* |
| `-h`, `--help` | print help |

Prints the class path needed to get the Hadoop jar and the required libraries. If called without arguments, then prints the classpath set up by the command scripts, which is likely to contain wildcards in the classpath entries. Additional options print the classpath after wildcard expansion or write the classpath into the manifest of a jar file. The latter is useful in environments where wildcards cannot be used and the expanded classpath exceeds the maximum supported command line length.

### `distcp`

Copy file or directories recursively. More information can be found at
[Hadoop DistCp Guide](../../hadoop-distcp/DistCp.html).

### `job`

Command to interact with Map Reduce Jobs.

Usage: `mapred job | [GENERIC_OPTIONS] | [-submit <job-file>] | [-status <job-id>] | [-counter <job-id> <group-name> <counter-name>] | [-kill <job-id>] | [-events <job-id> <from-event-#> <#-of-events>] | [-history [all] <jobHistoryFile|jobId> [-outfile <file>] [-format <human|json>]] | [-list [all]] | [-kill-task <task-id>] | [-fail-task <task-id>] | [-set-priority <job-id> <priority>] | [-list-active-trackers] | [-list-blacklisted-trackers] | [-list-attempt-ids <job-id> <task-type> <task-state>] [-logs <job-id> <task-attempt-id>] [-config <job-id> <file>]`

| COMMAND\_OPTION | Description |
|:---- |:---- |
| -submit *job-file* | Submits the job. |
| -status *job-id* | Prints the map and reduce completion percentage and all job counters. |
| -counter *job-id* *group-name* *counter-name* | Prints the counter value. |
| -kill *job-id* | Kills the job. |
| -events *job-id* *from-event-\#* *\#-of-events* | Prints the events' details received by jobtracker for the given range. |
| -history [all] *jobHistoryFile|jobId* [-outfile *file*] [-format *human|json*] | Prints job details, failed and killed task details. More details about the job such as successful tasks, task attempts made for each task, task counters, etc can be viewed by specifying the [all] option. An optional file output path (instead of stdout) can be specified. The format defaults to human-readable but can also be changed to JSON with the [-format] option. |
| -list [all] | Displays jobs which are yet to complete. `-list all` displays all jobs. |
| -kill-task *task-id* | Kills the task. Killed tasks are NOT counted against failed attempts. |
| -fail-task *task-id* | Fails the task. Failed tasks are counted against failed attempts. |
| -set-priority *job-id* *priority* | Changes the priority of the job. Allowed priority values are VERY\_HIGH, HIGH, NORMAL, LOW, VERY\_LOW |
| -list-active-trackers | List all the active NodeManagers in the cluster. |
| -list-blacklisted-trackers | List the black listed task trackers in the cluster. This command is not supported in MRv2 based cluster. |
| -list-attempt-ids *job-id* *task-type* *task-state* | List the attempt-ids based on the task type and the status given. Valid values for task-type are REDUCE, MAP. Valid values for task-state are running, pending, completed, failed, killed. |
| -logs *job-id* *task-attempt-id* | Dump the container log for a job if taskAttemptId is not specified, otherwise dump the log for the task with the specified taskAttemptId. The logs will be dumped in system out. |
| -config *job-id* *file* | Download the job configuration file. |

### `pipes`

Runs a pipes job.

Usage: `mapred pipes [-conf <path>] [-jobconf <key=value>, <key=value>, ...] [-input <path>] [-output <path>] [-jar <jar file>] [-inputformat <class>] [-map <class>] [-partitioner <class>] [-reduce <class>] [-writer <class>] [-program <executable>] [-reduces <num>]`

| COMMAND\_OPTION | Description |
|:---- |:---- |
| -conf *path* | Configuration for job |
| -jobconf *key=value*, *key=value*, ... | Add/override configuration for job |
| -input *path* | Input directory |
| -output *path* | Output directory |
| -jar *jar file* | Jar filename |
| -inputformat *class* | InputFormat class |
| -map *class* | Java Map class |
| -partitioner *class* | Java Partitioner |
| -reduce *class* | Java Reduce class |
| -writer *class* | Java RecordWriter |
| -program *executable* | Executable URI |
| -reduces *num* | Number of reduces |

### `queue`

command to interact and view Job Queue information

Usage: `mapred queue [-list] | [-info <job-queue-name> [-showJobs]] | [-showacls]`

| COMMAND\_OPTION | Description |
|:---- |:---- |
| -list | Gets list of Job Queues configured in the system. Along with scheduling information associated with the job queues. |
| -info *job-queue-name* [-showJobs] | Displays the job queue information and associated scheduling information of particular job queue. If `-showJobs` options is present a list of jobs submitted to the particular job queue is displayed. |
| -showacls | Displays the queue name and associated queue operations allowed for the current user. The list consists of only those queues to which the user has access. |

### `version`

Prints the version.

Usage: `mapred version`

### `envvars`

Usage: `mapred envvars`

Display computed Hadoop environment variables.

Administration Commands
-----------------------

Commands useful for administrators of a hadoop cluster.

### `historyserver`

Start JobHistoryServer.

Usage: `mapred historyserver`

### `hsadmin`

Runs a MapReduce hsadmin client for execute JobHistoryServer administrative commands.

Usage: `mapred hsadmin [-refreshUserToGroupsMappings] | [-refreshSuperUserGroupsConfiguration] | [-refreshAdminAcls] | [-refreshLoadedJobCache] | [-refreshLogRetentionSettings] | [-refreshJobRetentionSettings] | [-getGroups [username]] | [-help [cmd]]`

| COMMAND\_OPTION | Description |
|:---- |:---- |
| -refreshUserToGroupsMappings | Refresh user-to-groups mappings |
| -refreshSuperUserGroupsConfiguration | Refresh superuser proxy groups mappings |
| -refreshAdminAcls | Refresh acls for administration of Job history server |
| -refreshLoadedJobCache | Refresh loaded job cache of Job history server |
| -refreshJobRetentionSettings | Refresh job history period, job cleaner settings |
| -refreshLogRetentionSettings | Refresh log retention period and log retention check interval |
| -getGroups [username] | Get the groups which given user belongs to |
| -help [cmd] | Displays help for the given command or all commands if none is specified. |

### `frameworkuploader`

Collects framework jars and uploads them to HDFS as a tarball.

Usage: `mapred frameworkuploader -target <target> [-fs <filesystem>] [-input <classpath>] [-blacklist <list>] [-whitelist <list>] [-initialReplication <num>] [-acceptableReplication <num>] [-finalReplication <num>] [-timeout <seconds>] [-nosymlink]`

| COMMAND\_OPTION | Description |
|:---- |:---- |
| -input *classpath* | This is the input classpath that is searched for jar files to be included in the tarball. |
| -fs *filesystem* | The target file system. Defaults to the default filesystem set by fs.defaultFS. |
| -target *target* | This is the target location of the framework tarball, optionally followed by a # with the localized alias. An example would be /usr/lib/framework.tar#framework. Make sure the target directory is readable by all users but it is not writable by others than administrators to protect cluster security.
| -blacklist *list* | This is a comma separated regex array to filter the jar file names to exclude from the class path. It can be used for example to exclude test jars or Hadoop services that are not necessary to localize. |
| -whitelist *list* | This is a comma separated regex array to include certain jar files. This can be used to provide additional security, so that no external source can include malicious code in the classpath when the tool runs. |
| -nosymlink | This flag can be used to exclude symlinks that point to the same directory. This is not widely used. For example, `/a/foo.jar` and a symlink `/a/bar.jar` that points to `/a/foo.jar` would normally add `foo.jar` and `bar.jar` to the tarball as separate files despite them actually being the same file. This flag would make the tool exclude `/a/bar.jar` so only one copy of the file is added. |
| -initialReplication *num* | This is the replication count that the framework tarball is created with. It is safe to leave this value at the default 3. This is the tested scenario. |
| -finalReplication *num* | The uploader tool sets the replication once all blocks are collected and uploaded. If quick initial startup is required, then it is advised to set this to the commissioned node count divided by two but not more than 512. |
| -acceptableReplication *num* | The tool will wait until the tarball has been replicated this number of times before exiting. This should be a replication count less than or equal to the value in `finalReplication`. This is typically a 90% of the value in `finalReplication` to accomodate failing nodes. |
| -timeout *seconds* | A timeout in seconds to wait to reach `acceptableReplication` before the tool exits. The tool logs an error otherwise and returns.


