<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

HDFS Upgrade Domain
====================

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->


Introduction
------------

The current default HDFS block placement policy guarantees that a block’s 3 replicas will be placed
on at least 2 racks. Specifically one replica is placed on one rack and the other two replicas
are placed on another rack during write pipeline. This is a good compromise between rack diversity and write-pipeline efficiency. Note that
subsequent load balancing or machine membership change might cause 3 replicas of a block to be distributed
across 3 different racks. Thus any 3 datanodes in different racks could store 3 replicas of a block.


However, the default placement policy impacts how we should perform datanode rolling upgrade.
[HDFS Rolling Upgrade document](./HdfsRollingUpgrade.html) explains how the datanodes can be upgraded in a rolling
fashion without downtime. Because any 3 datanodes in different racks could store all the replicas of a block, it is
important to perform sequential restart of datanodes one at a time in order to minimize the impact on data availability
and read/write operations. Upgrading one rack at a time is another option; but that will increase the chance of
data unavailability if there is machine failure at another rack during the upgrade.

The side effect of this sequential datanode rolling upgrade strategy is longer
upgrade duration for larger clusters.


Architecture
-------

To address the limitation of block placement policy on rolling upgrade, the concept of upgrade domain
has been added to HDFS via a new block placement policy. The idea is to group datanodes in a new
dimension called upgrade domain, in addition to the existing rack-based grouping.
For example, we can assign all datanodes in the first position of any rack to upgrade domain ud_01,
nodes in the second position to upgrade domain ud_02 and so on.

The namenode provides BlockPlacementPolicy interface to support any custom block placement besides
the default block placement policy. A new upgrade domain block placement policy based on this interface
is available in HDFS. It will make sure replicas of any given block are distributed across machines from different upgrade domains.
By default, 3 replicas of any given block are placed on 3 different upgrade domains. This means all datanodes belonging to
a specific upgrade domain collectively won't store more than one replica of any block.

With upgrade domain block placement policy in place, we can upgrade all datanodes belonging to one upgrade domain at the
same time without impacting data availability. Only after finishing upgrading one upgrade domain we move to the next
upgrade domain until all upgrade domains have been upgraded. Such procedure will ensure no two replicas of any given
block will be upgraded at the same time. This means we can upgrade many machines at the same time for a large cluster.
And as the cluster continues to scale, new machines will be added to the existing upgrade domains without impact the
parallelism of the upgrade.

For an existing cluster with the default block placement policy, after switching to the new upgrade domain block
placement policy, any newly created blocks will conform the new policy. The old blocks allocated based on the old policy
need to migrated the new policy. There is a migrator tool you can use. See HDFS-8789 for details.


Settings
-------

To enable upgrade domain on your clusters, please follow these steps:

* Assign datanodes to individual upgrade domain groups.
* Enable upgrade domain block placement policy.
* Migrate blocks allocated based on old block placement policy to the new upgrade domain policy.

### Upgrade domain id assignment

How a datanode maps to an upgrade domain id is defined by administrators and specific to the cluster layout.
A common way to use the rack position of the machine as its upgrade domain id.

To configure mapping from host name to its upgrade domain id, we need to use json-based host configuration file.
by setting the following property as explained in [hdfs-default.xml](./hdfs-default.xml).

| Setting | Value |
|:---- |:---- |
|`dfs.namenode.hosts.provider.classname` | `org.apache.hadoop.hdfs.server.blockmanagement.CombinedHostFileManager`|
|`dfs.hosts`| the path of the json hosts file |

The json hosts file defines the property for all hosts. In the following example,
there are 4 datanodes in 2 racks; the machines at rack position 01 belong to upgrade domain 01;
the machines at rack position 02 belong to upgrade domain 02.

```json
[
  {
    "hostName": "dcA­rackA­01",
    "upgradeDomain": "01"
  },
  {
    "hostName": "dcA­rackA­02",
    "upgradeDomain": "02"
  },
  {
    "hostName": "dcA­rackB­01",
    "upgradeDomain": "01"
  },
  {
    "hostName": "dcA­rackB­02",
    "upgradeDomain": "02"
  }
]
```


### Enable upgrade domain block placement policy

After each datanode has been assigned an upgrade domain id, the next step is to enable
upgrade domain block placement policy with the following configuration as explained in [hdfs-default.xml](./hdfs-default.xml).

| Setting | Value |
|:---- |:---- |
|`dfs.block.replicator.classname`| `org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyWithUpgradeDomain` |

After restarting of namenode, the new policy will be used for any new block allocation.


### Migration

If you change the block placement policy of an existing cluster, you will need to make sure the
blocks allocated prior to the block placement policy change conform the new block placement policy.

HDFS-8789 provides the initial draft patch of a client-side migration tool. After the tool is committed,
we will be able to describe how to use the tool.


Rolling restart based on upgrade domains
-------

During cluster administration, we might need to restart datanodes to pick up new configuration, new hadoop release
or JVM version and so on. With upgrade domains enabled and all blocks on the cluster conform to the new policy, we can now
restart datanodes in batches, one upgrade domain at a time. Whether it is manual process or via automation, the steps are

* Group datanodes by upgrade domains based on dfsadmin or JMX's datanode information.
* For each upgrade domain
    * (Optional) put all the nodes in that upgrade domain to maintenance state (refer to [HdfsDataNodeAdminGuide.html](./HdfsDataNodeAdminGuide.html)).
    * Restart all those nodes.
    * Check if all datanodes are healthy after restart. Unhealthy nodes should be decommissioned.
    * (Optional) Take all those nodes out of maintenance state.


Metrics
-----------

Upgrade domains are part of namenode's JMX. As explained in [HDFSCommands.html](./HDFSCommands.html), you can also verify upgrade domains using the following commands.

Use `dfsadmin` to check upgrade domains at the cluster level.

`hdfs dfsadmin -report`

Use `fsck` to check upgrade domains of datanodes storing data at a specific path.

`hdfs fsck <path> -files -blocks -upgradedomains`
