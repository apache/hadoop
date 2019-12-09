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


Graceful Decommission of YARN Nodes
===============

* [Overview](#overview)
* [Features](#features)
  * [NodesListManager detects and handles include and exclude list changes](#nodeslistmanager-detects-and-handles-include-and-exclude-list-changes)
  * [RMNode handles decommission events](#rmnode-handles-decommission-events)
  * [Automatic and asynchronous tracking of decommissioning nodes status](#automatic-and-asynchronous-tracking-of-decommissioning-nodes-status)
  * [Per-Node decommission timeout support](#per-node-decommission-timeout-support)
* [Configuration](#configuration)



Overview
--------

YARN is scalable very easily: any new NodeManager could join to the configured ResourceManager and start to execute jobs. But to achieve full elasticity we need a decommissioning process which helps to remove existing nodes and down-scale the cluster.

YARN Nodes could be decommissioned NORMAL or GRACEFUL.

Normal Decommission of YARN Nodes means an immediate shutdown.

Graceful Decommission of YARN Nodes is the mechanism to decommission NMs while minimize the impact to running applications. Once a node is in DECOMMISSIONING state, RM won't schedule new containers on it and will wait for running containers and applications to complete (or until decommissioning timeout exceeded) before transition the node into DECOMMISSIONED.

## Quick start

To do a normal decommissioning:

1. Start a YARN cluster (with NodeManageres and ResourceManager)
2. Start a yarn job (for example with `yarn jar...` )
3. Add `yarn.resourcemanager.nodes.exclude-path` property to your `yarn-site.xml` (Note: you don't need to restart the ResourceManager)
4. Create a text file (the location is defined in the previous step) with one line which contains the name of a selected NodeManager 
5. Call `./bin/yarn rmadmin  -refreshNodes`
6. Result: The nodemanager is decommissioned *immediately*

In the next sections we will cover some more detailed usage (for example: using graceful decommissioning with timeout).

Features
--------

###Trigger decommission/recommission based on exclude/include lists

`yarn rmadmin -refreshNodes [-g [timeout in seconds] -client|server]` notifies NodesListManager to detect and handle include and exclude hosts changes. NodesListManager loads excluded hosts from the exclude file as specified through the `yarn.resourcemanager.nodes.exclude-path` configuration in yarn-site.xml. (Note:  It is unnecessary to restart RM in case of changing the exclude-path 
as this config will be read again for every `refreshNodes` command)

The format of the file could be plain text or XML depending the extension of the file. Only the XML format supports per node timout for graceful decommissioning.

NodesListManager inspects and compares status of RMNodes in resource manager and the exclude list, and apply necessary actions based on following rules:

* Recommission DECOMMISSIONED or DECOMMISSIONING nodes that are no longer excluded;
* Gracefully decommission excluded nodes that are not already in DECOMMISSIONED nor
  DECOMMISSIONING state;
* _Immediately_ decommission excluded nodes that are not already in DECOMMISSIONED state if `-g` flag is not specified.

Accordingly, RECOMMISSION, GRACEFUL_DECOMMISSION or DECOMMISSION RMNodeEvent will be sent to the RMNode.

###Per-Node decommission timeout support

To support flexible graceful decommission of nodes using different timeout through
single or multiple refreshNodes requests, HostsFileReader supports optional timeout value
after each hostname (or ip) in the exclude host file. 

The effective decommissioning timeout to use for a particular host is based on following priorities:

In case of server side timeout:

1. Use the timeout for the particular host if specified in exclude host file;
2. Use the timeout in `yarn rmadmin -refreshNodes -g [timeout in seconds] -server|client` if specified;
3. Use the default timeout specified through *"yarn.resourcemanager.nodemanager-graceful-decommission-timeout-secs"* configuration.

In case of client side timout (see bellow):

1. Only the command line parameter defined by the `-g` flag will be used. 

NodesListManager decides the effective timeout to use and set it on individual RMNode.
The timeout could also be dynamically adjusted through `yarn rmadmin -refreshNodes -g [timeout in seconds]` command. NodesListManager will resolve the effective timeout to use and update RMNode as necessary of the new timeout. Change of timeout does not reset the ongoing decommissioning but only affect the evaluation of whether the node has reached decommissioning timeout.

Here is a sample excludes file in xml format.

```xml
<?xml version="1.0"?>
<hosts>
  <host><name>host1</name></host>
  <host><name>host2</name><timeout>123</timeout></host>
  <host><name>host3</name><timeout>-1</timeout></host>
  <host><name>host4, host5,host6</name><timeout>1800</timeout></host>
</hosts>
```

If the file extension of the exclude file is not xml, standard one-line-per-host format is used without timeout support.

```
host1
host2
host3
```

Note: In the future more file formats are planned with timeout support. Follow the [YARN-5536](https://issues.apache.org/jira/browse/YARN-5536) if you are interested. 

Important to mention, that the timeout is not persited. In case of a RM restart/failover the node will be immediatelly decommission. (Follow the [YARN-5464](https://issues.apache.org/jira/browse/YARN-5464) for changes in this behavior).

### Client or server side timeout

Timeout of Graceful decommissioning could be tracked on server or client side. The `-client|server` indicates if the timeout tracking should be handled by the client or the ResourceManager. The client side tracking is blocking, while the server-side tracking is not.

###RMNode handles decommission events

Upon receiving GRACEFUL_DECOMMISSION event, the RMNode will save the decommissioning timeout if specified, update metrics for graceful decommission and preserve its original total capacity, and transition into DECOMMISSIONING state.

Resources will be dynamically and periodically updated on DECOMMISSIONING RMNode so that scheduler won't be scheduling new containers on them due to no available resources.

###Automatic and asynchronous tracking of decommissioning nodes status

**DecommissioningNodeWatcher** is the YARN component that tracks DECOMMISSIONING nodes
status automatically and asynchronously after client/admin made the graceful decommission
request. NM periodically send RM heart beat with it latest container status.
DecommissioningNodeWatcher tracks heartbeat updates on all DECOMMISSIONING nodes to decide when,
after all running containers on the node have completed, will be transitioned into DECOMMISSIONED state
after which NodeManager will be told to shutdown.

Under MR application, a node, after completes all its containers, may still serve it map output data
during the duration of the application for reducers. The YARN graceful decommission
mechanism keeps such DECOMMISSIONING nodes until all involved applications complete.
It could be however undesirable under long-running applications scenario where a bunch of
"idle" nodes might stay around for long period of time. DecommissioningNodeWatcher
balances such concern with a timeout --- a DECOMMISSIONING node will be DECOMMISSIONED
no later than decommissioning timeout regardless of running containers or applications.
If running containers finished earlier, it continues waiting for applications to finish
until the decommissioning timeout. When decommissioning timeout reaches, the node
will be decommissioned regardless. The node will be deactivated and owning tasks will
be rescheduled as necessary.

Status of all decommissioning node are logged periodically (every 20 seconds) in resource manager logs.
Following are the sub-status of a decommissioning node:

* NONE --- Node is not in DECOMMISSIONING state.
* WAIT_CONTAINER --- Wait for running containers to complete.
* WAIT_APP --- Wait for running application to complete (after all containers complete)
* TIMEOUT --- Timeout waiting for either containers or applications to complete
* READY --- Nothing to wait, ready to be decommissioned
* DECOMMISSIONED --- The node has already been decommissioned



Configuration
--------

| Property                                 | Value                                    |
| ---------------------------------------- | ---------------------------------------- |
| yarn.resourcemanager.nodemanager-graceful-decommission-timeout-secs | Timeout in seconds for YARN node graceful decommission. This is the maximal time to wait for running containers and applications to complete before transition a DECOMMISSIONING node into DECOMMISSIONED. The default value is 3600 seconds. Negative value (like -1) is handled as infinite timeout. |
| yarn.resourcemanager.decommissioning-nodes-watcher.poll-interval-secs | Period in seconds of the poll timer task inside DecommissioningNodesWatcher to identify and take care of DECOMMISSIONING nodes missing regular heart beat. The default value is 20 seconds. |
| yarn.resourcemanager.nodes.exclude-path  | Path to file with nodes to exclude.      |
| yarn.resourcemanager.nodes.include-path  | Path to file with nodes to include.      |
