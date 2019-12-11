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

HDFS DataNode Admin Guide
=================

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

Overview
--------

The Hadoop Distributed File System (HDFS) namenode maintains states of all datanodes.
There are two types of states. The fist type describes the liveness of a datanode indicating if
the node is live, dead or stale. The second type describes the admin state indicating if the node
is in service, decommissioned or under maintenance.

When an administrator decommission a datanode, the datanode will first be transitioned into
`DECOMMISSION_INPROGRESS` state. After all blocks belonging to that datanode have been fully replicated elsewhere
based on each block's replication factor. the datanode will be transitioned to `DECOMMISSIONED` state. After that,
the administrator can shutdown the node to perform long-term repair and maintenance that could take days or weeks.
After the machine has been repaired, the machine can be recommissioned back to the cluster.

Sometimes administrators only need to take datanodes down for minutes/hours to perform short-term repair/maintenance.
In such scenario, the HDFS block replication overhead incurred by decommission might not be necessary and a light-weight process is desirable.
And that is what maintenance state is used for. When an administrator put a datanode in maintenance state, the datanode will first be transitioned
to `ENTERING_MAINTENANCE` state. As long as all blocks belonging to that datanode is minimally replicated elsewhere, the datanode
will immediately be transitioned to `IN_MAINTENANCE` state. After the maintenance has completed, the administrator can take the datanode
out of the maintenance state. In addition, maintenance state supports timeout that allows administrators to config the maximum duration in
which a datanode is allowed to stay in maintenance state. After the timeout, the datanode will be transitioned out of maintenance state
automatically by HDFS without human intervention.

In summary, datanode admin operations include the followings:

* Decommission
* Recommission
* Putting nodes in maintenance state
* Taking nodes out of maintenance state

And datanode admin states include the followings:

* `NORMAL` The node is in service.
* `DECOMMISSIONED` The node has been decommissioned.
* `DECOMMISSION_INPROGRESS` The node is being transitioned to DECOMMISSIONED state.
* `IN_MAINTENANCE` The node in in maintenance state.
* `ENTERING_MAINTENANCE` The node is being transitioned to maintenance state.


Host-level settings
-----------

To perform any of datanode admin operations, there are two steps.

* Update host-level configuration files to indicate the desired admin states of targeted datanodes. There are two supported formats for configuration files.
    * Hostname-only configuration. Each line includes the hostname/ip address for a datanode. That is the default format.
    * JSON-based configuration. The configuration is in JSON format. Each element maps to one datanode and each datanode can have multiple properties. This format is required to put datanodes to maintenance states.

* Run the following command to have namenode reload the host-level configuration files.
`hdfs dfsadmin [-refreshNodes]`

### Hostname-only configuration
This is the default configuration used by the namenode. It only supports node decommission and recommission; it doesn't support admin operations related to maintenance state. Use `dfs.hosts` and `dfs.hosts.exclude` as explained in [hdfs-default.xml](./hdfs-default.xml).

In the following example, `host1` and `host2` need to be in service.
`host3` and `host4` need to be in decommissioned state.

dfs.hosts file
```text
host1
host2
host3
host4
```
dfs.hosts.exclude file
```text
host3
host4
```

### JSON-based configuration

JSON-based format is the new configuration format that supports generic properties on datanodes. Set the following
configurations to enable JSON-based format as explained in [hdfs-default.xml](./hdfs-default.xml).


| Setting | Value |
|:---- |:---- |
|`dfs.namenode.hosts.provider.classname`| `org.apache.hadoop.hdfs.server.blockmanagement.CombinedHostFileManager`|
|`dfs.hosts`| the path of the json hosts file |

Here is the list of currently supported properties by HDFS.


| Property | Description |
|:---- |:---- |
|`hostName`| Required. The host name of the datanode. |
|`upgradeDomain`| Optional. The upgrade domain id of the datanode. |
|`adminState`| Optional. The expected admin state. The default value is `NORMAL`; `DECOMMISSIONED` for decommission; `IN_MAINTENANCE` for maintenance state. |
|`port`| Optional. the port number of the datanode |
|`maintenanceExpireTimeInMS`| Optional. The epoch time in milliseconds until which the datanode will remain in maintenance state. The default value is forever. |

In the following example, `host1` and `host2` need to in service. `host3` need to be in decommissioned state. `host4` need to be in in maintenance state.

dfs.hosts file
```json
[
  {
    "hostName": "host1"
  },
  {
    "hostName": "host2",
    "upgradeDomain": "ud0"
  },
  {
    "hostName": "host3",
    "adminState": "DECOMMISSIONED"
  },
  {
    "hostName": "host4",
    "upgradeDomain": "ud2",
    "adminState": "IN_MAINTENANCE"
  }
]
```


Cluster-level settings
-----------

There are several cluster-level settings related to datanode administration.
For common use cases, you should rely on the default values. Please refer to
[hdfs-default.xml](./hdfs-default.xml) for descriptions and default values.

```text
dfs.namenode.maintenance.replication.min
dfs.namenode.decommission.interval
dfs.namenode.decommission.blocks.per.interval
dfs.namenode.decommission.max.concurrent.tracked.nodes
```


Backing-off Decommission Monitor (experimental)
------------

The original decommissioning algorithm has issues when DataNodes having lots of
blocks are decommissioned such as

* Write lock in the NameNode could be held for a long time for queueing re-replication.
* Re-replication work progresses node by node if there are multiple decommissioning DataNodes.

[HDFS-14854](https://issues.apache.org/jira/browse/HDFS-14854) introduced
new decommission monitor in order to mitigate those issues.
This feature is currently marked as experimental and disabled by default.
You can enable this by setting the value of
`dfs.namenode.decommission.monitor.class` to
`org.apache.hadoop.hdfs.server.blockmanagement.DatanodeAdminBackoffMonitor`
in hdfs-site.xml.

The relevant configuration properties are listed in the table below.
Please refer to [hdfs-default.xml](./hdfs-default.xml)
for descriptions and default values.

| Property |
|:-------- |
| `dfs.namenode.decommission.monitor.class` |
| `dfs.namenode.decommission.backoff.monitor.pending.limit` |
| `dfs.namenode.decommission.backoff.monitor.pending.blocks.per.lock` |


Metrics
-----------

Admin states are part of the namenode's webUI and JMX. As explained in [HDFSCommands.html](./HDFSCommands.html), you can also verify admin states using the following commands.

Use `dfsadmin` to check admin states at the cluster level.

`hdfs dfsadmin -report`

Use `fsck` to check admin states of datanodes storing data at a specific path. For backward compatibility, a special flag is required to return maintenance states.

```text
hdfs fsck <path> // only show decommission state
hdfs fsck <path> -maintenance // include maintenance state
```
