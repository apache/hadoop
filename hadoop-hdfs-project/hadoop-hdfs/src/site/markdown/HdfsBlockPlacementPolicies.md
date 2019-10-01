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

BlockPlacementPolicies
======================
* [Block Placement Policies](#Block_Placement_Policies)
	* [Introduction](#Introduction)
	* [Additional Types of Policies](#Policy_Types)
		* [BlockPlacementPolicyRackFaultTolerant](#Block_PlacementPolicy_RackFaultTolerant)
		* [BlockPlacementPolicyWithNodeGroup](#BlockPlacementPolicy_With_NodeGroup)
		* [BlockPlacementPolicyWithUpgradeDomain](#BlockPlacementPolicy_With_UpgradeDomain)

##Introduction
By default HDFS supports BlockPlacementPolicyDefault. Where one block on local and copy on 2 different nodes of same remote rack. Additional to this HDFS supports 3 different pluggable block placement policies. Users can choose the policy based on their infrastructre and use case. This document describes the detailed information about the type of policies with its use cases and configuration.


### BlockPlacementPolicyRackFaultTolerant

BlockPlacementPolicyRackFaultTolerant can be used to split the placement of blocks across multiple rack.By default with replication of 3 BlockPlacementPolicyDefault will  put one replica on the local machine if the writer is on a datanode, otherwise on a random datanode in the same rack as that of the writer, another replica on a node in a different (remote) rack, and the last on a different node in the same remote rack. So totally 2 racks will be used, in sceneraio like 2 racks going down at the same time will cause data inavailability where using BlockPlacementPolicyRackFaultTolerant will helop in placing 3 blocks on 3 different racks.
https://issues.apache.org/jira/browse/HDFS-7891

![Rack Fault Tolerant Policy](images/RackFaultTolerant.jpg)

Configration

hdfs-site.xml

```xml
<property>
  <name>dfs.block.replicator.classname</name>
  <value>org.apache.hadoop.hdfs.server.namenode.BlockPlacementPolicyRackFaultTolerant</value>
</property>
```


### BlockPlacementPolicyWithNodeGroup

With new 3 layer hierarchical topology, a node group level got introduced, which maps well onto a infrastructure that is based on a virtulized environment. In Virtualized environment multiple vm's will be hosted on same physical machine. Vm's on the same physical host are affected by the same hardware failure. So mapping the physical host a node groups this block placement gurantees that it will never place more than one replica on the same node group (physical host), in case of node group failure, only one replica will be lost at the maximum. 
https://issues.apache.org/jira/browse/HADOOP-8468


- core-site.xml

```xml
<property>
  <name>net.topology.impl</name>
  <value>org.apache.hadoop.net.NetworkTopologyWithNodeGroup</value>
</property>
<property>
  <name>net.topology.nodegroup.aware</name>
  <value>true</value>
</property>
```

- hdfs-site.xml

```xml
<property>
  <name>dfs.block.replicator.classname</name>
  <value>
    org.apache.hadoop.hdfs.server.namenode.BlockPlacementPolicyWithNodeGroup
  </value>
</property>
```

-    Topology script

Topology script is the same as the examples above, the only difference is,
instead of returning only **/{rack}**, the script should return
**/{rack}/{nodegroup}**. Following is an example topology mapping table:

```
192.168.0.1 /rack1/nodegroup1
192.168.0.2 /rack1/nodegroup1
192.168.0.3 /rack1/nodegroup2
192.168.0.4 /rack1/nodegroup2
192.168.0.5 /rack2/nodegroup3
192.168.0.6 /rack2/nodegroup3
```


### BlockPlacementPolicyWithUpgradeDomain

To address the limitation of block placement policy on rolling upgrade, the concept of upgrade domain has been added to HDFS via a new block placement policy. The idea is to group datanodes in a new dimension called upgrade domain, in addition to the existing rack-based grouping. For example, we can assign all datanodes in the first position of any rack to upgrade domain ud_01, nodes in the second position to upgrade domain ud_02 and so on.
It will make sure replicas of any given block are distributed across machines from different upgrade domains. By default, 3 replicas of any given block are placed on 3 different upgrade domains. This means all datanodes belonging to a specific upgrade domain collectively won’t store more than one replica of any block.
https://issues.apache.org/jira/browse/HDFS-9006
Detailed info about configuration https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsUpgradeDomain.html
