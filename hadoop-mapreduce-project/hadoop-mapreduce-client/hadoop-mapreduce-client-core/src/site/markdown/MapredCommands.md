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

* [Overview](#Overview)
* [User Commands](#User_Commands)
    * [archive](#archive)
    * [classpath](#classpath)
    * [distcp](#distcp)
    * [job](#job)
    * [pipes](#pipes)
    * [queue](#queue)
    * [version](#version)
* [Administration Commands](#Administration_Commands)
    * [historyserver](#historyserver)
    * [hsadmin](#hsadmin)

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

### `classpath`

Prints the class path needed to get the Hadoop jar and the required libraries.

Usage: `mapred classpath`

### `distcp`

Copy file or directories recursively. More information can be found at
[Hadoop DistCp Guide](../../hadoop-distcp/DistCp.html).

### `job`

Command to interact with Map Reduce Jobs.

Usage: `mapred job | [GENERIC_OPTIONS] | [-submit <job-file>] | [-status <job-id>] | [-counter <job-id> <group-name> <counter-name>] | [-kill <job-id>] | [-events <job-id> <from-event-#> <#-of-events>] | [-history [all] <jobOutputDir>] | [-list [all]] | [-kill-task <task-id>] | [-fail-task <task-id>] | [-set-priority <job-id> <priority>]`

| COMMAND\_OPTION | Description |
|:---- |:---- |
| -submit *job-file* | Submits the job. |
| -status *job-id* | Prints the map and reduce completion percentage and all job counters. |
| -counter *job-id* *group-name* *counter-name* | Prints the counter value. |
| -kill *job-id* | Kills the job. |
| -events *job-id* *from-event-\#* *\#-of-events* | Prints the events' details received by jobtracker for the given range. |
| -history [all]*jobOutputDir* | Prints job details, failed and killed tip details. More details about the job such as successful tasks and task attempts made for each task can be viewed by specifying the [all] option. |
| -list [all] | Displays jobs which are yet to complete. `-list all` displays all jobs. |
| -kill-task *task-id* | Kills the task. Killed tasks are NOT counted against failed attempts. |
| -fail-task *task-id* | Fails the task. Failed tasks are counted against failed attempts. |
| -set-priority *job-id* *priority* | Changes the priority of the job. Allowed priority values are VERY\_HIGH, HIGH, NORMAL, LOW, VERY\_LOW |

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
