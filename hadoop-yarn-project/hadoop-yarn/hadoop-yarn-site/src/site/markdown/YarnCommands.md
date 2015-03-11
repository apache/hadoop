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

* [Overview](#Overview)
* [User Commands](#User_Commands)
    * [application](#application)
    * [applicationattempt](#applicationattempt)
    * [classpath](#classpath)
    * [container](#container)
    * [jar](#jar)
    * [logs](#logs)
    * [node](#node)
    * [queue](#queue)
    * [version](#version)
* [Administration Commands](#Administration_Commands)
    * [daemonlog](#daemonlog)
    * [nodemanager](#nodemanager)
    * [proxyserver](#proxyserver)
    * [resourcemanager](#resourcemanager)
    * [rmadmin](#rmadmin)
    * [scmadmin](#scmadmin)
    * [sharedcachemanager](#sharedcachemanager)
    * [timelineserver](#timelineserver)

Overview
--------

YARN commands are invoked by the bin/yarn script. Running the yarn script without any arguments prints the description for all commands.

Usage: `yarn [--config confdir] COMMAND [--loglevel loglevel] [GENERIC_OPTIONS] [COMMAND_OPTIONS]`

YARN has an option parsing framework that employs parsing generic options as well as running classes.

| COMMAND\_OPTIONS | Description |
|:---- |:---- |
| `--config confdir` | Overwrites the default Configuration directory. Default is `${HADOOP_PREFIX}/conf`. |
| `--loglevel loglevel` | Overwrites the log level. Valid log levels are FATAL, ERROR, WARN, INFO, DEBUG, and TRACE. Default is INFO. |
| GENERIC\_OPTIONS | The common set of options supported by multiple commands. See the Hadoop [Commands Manual](../../hadoop-project-dist/hadoop-common/CommandsManual.html#Generic_Options) for more information. |
| COMMAND COMMAND\_OPTIONS | Various commands with their options are described in the following sections. The commands have been grouped into [User Commands](#User_Commands) and [Administration Commands](#Administration_Commands). |

User Commands
-------------

Commands useful for users of a Hadoop cluster.

### `application`

Usage: `yarn application [options] `

| COMMAND\_OPTIONS | Description |
|:---- |:---- |
| -appStates \<States\> | Works with -list to filter applications based on input comma-separated list of application states. The valid application state can be one of the following:  ALL, NEW, NEW\_SAVING, SUBMITTED, ACCEPTED, RUNNING, FINISHED, FAILED, KILLED |
| -appTypes \<Types\> | Works with -list to filter applications based on input comma-separated list of application types. |
| -list | Lists applications from the RM. Supports optional use of -appTypes to filter applications based on application type, and -appStates to filter applications based on application state. |
| -kill \<ApplicationId\> | Kills the application. |
| -status \<ApplicationId\> | Prints the status of the application. |

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

Usage: `yarn classpath`

Prints the class path needed to get the Hadoop jar and the required libraries

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

Administration Commands
-----------------------

Commands useful for administrators of a Hadoop cluster.

### `daemonlog`

Usage:

```
   yarn daemonlog -getlevel <host:httpport> <classname> 
   yarn daemonlog -setlevel <host:httpport> <classname> <level>
```

| COMMAND\_OPTIONS | Description |
|:---- |:---- |
| -getlevel `<host:httpport>` `<classname>` | Prints the log level of the log identified by a qualified `<classname>`, in the daemon running at `<host:httpport>`. This command internally connects to `http://<host:httpport>/logLevel?log=<classname>` |
| -setlevel `<host:httpport> <classname> <level>` | Sets the log level of the log identified by a qualified `<classname>` in the daemon running at `<host:httpport>`. This command internally connects to `http://<host:httpport>/logLevel?log=<classname>&level=<level>` |

Get/Set the log level for a Log identified by a qualified class name in the daemon.

Example: `$ bin/yarn daemonlog -setlevel 127.0.0.1:8088 org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl DEBUG`

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

Start the ResourceManager

### `rmadmin`

Usage:

```
  yarn rmadmin [-refreshQueues]
               [-refreshNodes]
               [-refreshUserToGroupsMapping] 
               [-refreshSuperUserGroupsConfiguration]
               [-refreshAdminAcls] 
               [-refreshServiceAcl]
               [-getGroups [username]]
               [-transitionToActive [--forceactive] [--forcemanual] <serviceId>]
               [-transitionToStandby [--forcemanual] <serviceId>]
               [-failover [--forcefence] [--forceactive] <serviceId1> <serviceId2>]
               [-getServiceState <serviceId>]
               [-checkHealth <serviceId>]
               [-help [cmd]]
```

| COMMAND\_OPTIONS | Description |
|:---- |:---- |
| -refreshQueues | Reload the queues' acls, states and scheduler specific properties. ResourceManager will reload the mapred-queues configuration file. |
| -refreshNodes | Refresh the hosts information at the ResourceManager. |
| -refreshUserToGroupsMappings | Refresh user-to-groups mappings. |
| -refreshSuperUserGroupsConfiguration | Refresh superuser proxy groups mappings. |
| -refreshAdminAcls | Refresh acls for administration of ResourceManager |
| -refreshServiceAcl | Reload the service-level authorization policy file ResourceManager will reload the authorization policy file. |
| -getGroups [username] | Get groups the specified user belongs to. |
| -transitionToActive [--forceactive] [--forcemanual] \<serviceId\> | Transitions the service into Active state. Try to make the target active without checking that there is no active node if the --forceactive option is used. This command can not be used if automatic failover is enabled. Though you can override this by --forcemanual option, you need caution. |
| -transitionToStandby [--forcemanual] \<serviceId\> | Transitions the service into Standby state. This command can not be used if automatic failover is enabled. Though you can override this by --forcemanual option, you need caution. |
| -failover [--forceactive] \<serviceId1\> \<serviceId2\> | Initiate a failover from serviceId1 to serviceId2. Try to failover to the target service even if it is not ready if the --forceactive option is used. This command can not be used if automatic failover is enabled. |
| -getServiceState \<serviceId\> | Returns the state of the service. |
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
