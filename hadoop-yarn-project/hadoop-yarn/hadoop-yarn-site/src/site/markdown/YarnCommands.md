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

YARN Commands
=============

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

Overview
--------

YARN commands are invoked by the bin/yarn script. Running the yarn script without any arguments prints the description for all commands.

Usage: `yarn [SHELL_OPTIONS] COMMAND [GENERIC_OPTIONS] [COMMAND_OPTIONS]`

YARN has an option parsing framework that employs parsing generic options as well as running classes.

| COMMAND\_OPTIONS | Description |
|:---- |:---- |
| SHELL\_OPTIONS | The common set of shell options. These are documented on the [Commands Manual](../../hadoop-project-dist/hadoop-common/CommandsManual.html#Shell_Options) page. |
| GENERIC\_OPTIONS | The common set of options supported by multiple commands. See the Hadoop [Commands Manual](../../hadoop-project-dist/hadoop-common/CommandsManual.html#Generic_Options) for more information. |
| COMMAND COMMAND\_OPTIONS | Various commands with their options are described in the following sections. The commands have been grouped into [User Commands](#User_Commands) and [Administration Commands](#Administration_Commands). |

User Commands
-------------

Commands useful for users of a Hadoop cluster.

### `application`

Usage: `yarn application [options] `

| COMMAND\_OPTIONS | Description |
|:---- |:---- |
| -appId \<ApplicationId\> | Specify Application Id to be operated |
| -appStates \<States\> | Works with -list to filter applications based on input comma-separated list of application states. The valid application state can be one of the following:  ALL, NEW, NEW\_SAVING, SUBMITTED, ACCEPTED, RUNNING, FINISHED, FAILED, KILLED |
| -appTypes \<Types\> | Works with -list to filter applications based on input comma-separated list of application types. |
| -help | Help |
| -list | Lists applications from the RM. Supports optional use of -appTypes to filter applications based on application type, and -appStates to filter applications based on application state. |
| -kill \<ApplicationId\> | Kills the application. |
| -movetoqueue \<Application Id\> | Moves the application to a different queue. |
| -queue \<Queue Name\> | Works with the movetoqueue command to specify which queue to move an application to. |
| -status \<ApplicationId\> | Prints the status of the application. |
| -updateLifetime \<Timeout\> | Update application timeout (from the time of request) in seconds. ApplicationId can be specified using 'appId' option. |
| -updatePriority \<Priority\> | Update priority of an application. ApplicationId can be passed using 'appId' option. |

Prints application(s) report/kill application

### `applicationattempt`

Usage: `yarn applicationattempt [options] `

| COMMAND\_OPTIONS | Description |
|:---- |:---- |
| -help | Help |
| -list \<ApplicationId\> | Lists applications attempts for the given application. |
| -status \<Application Attempt Id\> | Prints the status of the application attempt. |

prints applicationattempt(s) report

### `classpath`

Usage: `yarn classpath [--glob |--jar <path> |-h |--help]`

| COMMAND\_OPTION | Description |
|:---- |:---- |
| `--glob` | expand wildcards |
| `--jar` *path* | write classpath as manifest in jar named *path* |
| `-h`, `--help` | print help |

Prints the class path needed to get the Hadoop jar and the required libraries. If called without arguments, then prints the classpath set up by the command scripts, which is likely to contain wildcards in the classpath entries. Additional options print the classpath after wildcard expansion or write the classpath into the manifest of a jar file. The latter is useful in environments where wildcards cannot be used and the expanded classpath exceeds the maximum supported command line length.

### `container`

Usage: `yarn container [options] `

| COMMAND\_OPTIONS | Description |
|:---- |:---- |
| -help | Help |
| -list \<Application Attempt Id\> | Lists containers for the application attempt. |
| -status \<ContainerId\> | Prints the status of the container. |

prints container(s) report

### `jar`

Usage: `yarn jar <jar> [mainClass] args... `

Runs a jar file. Users can bundle their YARN code in a jar file and execute it using this command.

### `logs`

Usage: `yarn logs -applicationId <application ID> [options] `

| COMMAND\_OPTIONS | Description |
|:---- |:---- |
| -applicationId \<application ID\> | Specifies an application id |
| -appOwner \<AppOwner\> | AppOwner (assumed to be current user if not specified) |
| -containerId \<ContainerId\> | ContainerId (must be specified if node address is specified) |
| -help | Help |
| -nodeAddress \<NodeAddress\> | NodeAddress in the format nodename:port (must be specified if container id is specified) |

Dump the container logs

### `node`

Usage: `yarn node [options] `

| COMMAND\_OPTIONS | Description |
|:---- |:---- |
| -all | Works with -list to list all nodes. |
| -list | Lists all running nodes. Supports optional use of -states to filter nodes based on node state, and -all to list all nodes. |
| -states \<States\> | Works with -list to filter nodes based on input comma-separated list of node states. |
| -status \<NodeId\> | Prints the status report of the node. |

Prints node report(s)

### `queue`

Usage: `yarn queue [options] `

| COMMAND\_OPTIONS | Description |
|:---- |:---- |
| -help | Help |
| -status \<QueueName\> | Prints the status of the queue. |

Prints queue information

### `version`

Usage: `yarn version`

Prints the Hadoop version.

### `envvars`

Usage: `yarn envvars`

Display computed Hadoop environment variables.

Administration Commands
-----------------------

Commands useful for administrators of a Hadoop cluster.

### `daemonlog`

Get/Set the log level for a Log identified by a qualified class name in the daemon dynamically.
See the Hadoop [Commands Manual](../../hadoop-project-dist/hadoop-common/CommandsManual.html#daemonlog) for more information.

### `nodemanager`

Usage: `yarn nodemanager`

Start the NodeManager

### `proxyserver`

Usage: `yarn proxyserver`

Start the web proxy server

### `resourcemanager`

Usage: `yarn resourcemanager [-format-state-store]`

| COMMAND\_OPTIONS | Description |
|:---- |:---- |
| -format-state-store | Formats the RMStateStore. This will clear the RMStateStore and is useful if past applications are no longer needed. This should be run only when the ResourceManager is not running. |
| -remove-application-from-state-store \<appId\> | Remove the application from RMStateStore. This should be run only when the ResourceManager is not running. |

Start the ResourceManager

### `rmadmin`

Usage:

```
  Usage: yarn rmadmin
     -refreshQueues
     -refreshNodes [-g|graceful [timeout in seconds] -client|server]
     -refreshNodesResources
     -refreshSuperUserGroupsConfiguration
     -refreshUserToGroupsMappings
     -refreshAdminAcls
     -refreshServiceAcl
     -getGroups [username]
     -addToClusterNodeLabels <"label1(exclusive=true),label2(exclusive=false),label3">
     -removeFromClusterNodeLabels <label1,label2,label3> (label splitted by ",")
     -replaceLabelsOnNode <"node1[:port]=label1,label2 node2[:port]=label1,label2"> [-failOnUnknownNodes]
     -directlyAccessNodeLabelStore
     -refreshClusterMaxPriority
     -updateNodeResource [NodeID] [MemSize] [vCores] ([OvercommitTimeout])
     -transitionToActive [--forceactive] <serviceId>
     -transitionToStandby <serviceId>
     -failover [--forcefence] [--forceactive] <serviceId> <serviceId>
     -getServiceState <serviceId>
     -getAllServiceState
     -checkHealth <serviceId>
     -help [cmd]
```

| COMMAND\_OPTIONS | Description |
|:---- |:---- |
| -refreshQueues | Reload the queues' acls, states and scheduler specific properties. ResourceManager will reload the mapred-queues configuration file. |
| -refreshNodes [-g&#124;graceful [timeout in seconds] -client&#124;server] | Refresh the hosts information at the ResourceManager. Here [-g&#124;graceful [timeout in seconds] -client&#124;server] is optional, if we specify the timeout then ResourceManager will wait for timeout before marking the NodeManager as decommissioned. The -client&#124;server indicates if the timeout tracking should be handled by the client or the ResourceManager. The client-side tracking is blocking, while the server-side tracking is not. Omitting the timeout, or a timeout of -1, indicates an infinite timeout. Known Issue: the server-side tracking will immediately decommission if an RM HA failover occurs. |
| -refreshNodesResources | Refresh resources of NodeManagers at the ResourceManager. |
| -refreshSuperUserGroupsConfiguration | Refresh superuser proxy groups mappings. |
| -refreshUserToGroupsMappings | Refresh user-to-groups mappings. |
| -refreshAdminAcls | Refresh acls for administration of ResourceManager |
| -refreshServiceAcl | Reload the service-level authorization policy file ResourceManager will reload the authorization policy file. |
| -getGroups [username] | Get groups the specified user belongs to. |
| -addToClusterNodeLabels <"label1(exclusive=true),label2(exclusive=false),label3"> | Add to cluster node labels. Default exclusivity is true. |
| -removeFromClusterNodeLabels <label1,label2,label3> (label splitted by ",") | Remove from cluster node labels. |
| -replaceLabelsOnNode <"node1[:port]=label1,label2 node2[:port]=label1,label2"> [-failOnUnknownNodes]| Replace labels on nodes (please note that we do not support specifying multiple labels on a single host for now.) -failOnUnknownNodes is optional, when we set this option, it will fail if specified nodes are unknown.|
| -directlyAccessNodeLabelStore | This is DEPRECATED, will be removed in future releases. Directly access node label store, with this option, all node label related operations will not connect RM. Instead, they will access/modify stored node labels directly. By default, it is false (access via RM). AND PLEASE NOTE: if you configured yarn.node-labels.fs-store.root-dir to a local directory (instead of NFS or HDFS), this option will only work when the command run on the machine where RM is running. |
| -refreshClusterMaxPriority | Refresh cluster max priority |
| -updateNodeResource [NodeID] [MemSize] [vCores] \([OvercommitTimeout]\) | Update resource on specific node. |
| -transitionToActive [--forceactive] [--forcemanual] \<serviceId\> | Transitions the service into Active state. Try to make the target active without checking that there is no active node if the --forceactive option is used. This command can not be used if automatic failover is enabled. Though you can override this by --forcemanual option, you need caution. This command can not be used if automatic failover is enabled.|
| -transitionToStandby [--forcemanual] \<serviceId\> | Transitions the service into Standby state. This command can not be used if automatic failover is enabled. Though you can override this by --forcemanual option, you need caution. |
| -failover [--forceactive] \<serviceId1\> \<serviceId2\> | Initiate a failover from serviceId1 to serviceId2. Try to failover to the target service even if it is not ready if the --forceactive option is used. This command can not be used if automatic failover is enabled. |
| -getServiceState \<serviceId\> | Returns the state of the service. |
| -getAllServiceState | Returns the state of all the services. |
| -checkHealth \<serviceId\> | Requests that the service perform a health check. The RMAdmin tool will exit with a non-zero exit code if the check fails. |
| -help [cmd] | Displays help for the given command or all commands if none is specified. |

Runs ResourceManager admin client

### scmadmin

Usage: `yarn scmadmin [options] `

| COMMAND\_OPTIONS | Description |
|:---- |:---- |
| -help | Help |
| -runCleanerTask | Runs the cleaner task |

Runs Shared Cache Manager admin client

### sharedcachemanager

Usage: `yarn sharedcachemanager`

Start the Shared Cache Manager

### timelineserver

Usage: `yarn timelineserver`

Start the TimeLineServer

Files
-----

| File | Description |
|:---- |:---- |
| etc/hadoop/hadoop-env.sh | This file stores the global settings used by all Hadoop shell commands. |
| etc/hadoop/yarn-env.sh | This file stores overrides used by all YARN shell commands. |
| etc/hadoop/hadoop-user-functions.sh | This file allows for advanced users to override some shell functionality. |
| ~/.hadooprc | This stores the personal environment for an individual user. It is processed after the `hadoop-env.sh`, `hadoop-user-functions.sh`, and `yarn-env.sh` files and can contain the same settings. |
