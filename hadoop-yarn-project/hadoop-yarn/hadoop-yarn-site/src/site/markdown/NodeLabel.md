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

YARN Node Labels
===============

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

Overview
--------

Node label is a way to group nodes with similar characteristics and applications can specify where to run.

Now we only support node partition, which is:

* One node can have only one node partition, so a cluster is partitioned to several disjoint sub-clusters by node partitions. By default, nodes belong to DEFAULT partition (partition="")
* User need to configure how much resources of each partition can be used by different queues. For more detail, please refer next section.
* There are two kinds of node partitions:
    * Exclusive: containers will be allocated to nodes with exactly match node partition. (e.g. asking partition=“x” will be allocated to node with partition=“x”, asking DEFAULT partition will be allocated to DEFAULT partition nodes).
    * Non-exclusive: if a partition is non-exclusive, it shares idle resource to container requesting DEFAULT partition.

User can specify set of node labels which can be accessed by each queue, one application can only use subset of node labels that can be accessed by the queue which contains the application.

Features
--------

The ```Node Labels``` supports the following features for now:

* Partition cluster - each node can be assigned one label, so the cluster will be divided to several smaller disjoint partitions.
* ACL of node-labels on queues - user can set accessible node labels on each queue so only some nodes can only be accessed by specific queues.
* Specify percentage of resource of a partition which can be accessed by a queue - user can set percentage like: queue A can access 30% of resources on nodes with label=hbase. Such percentage setting will be consistent with existing resource manager
* Specify required node label in resource request, it will only be allocated when node has the same label. If no node label requirement specified, such Resource Request will only be allocated on nodes belong to DEFAULT partition.
* Operability
    * Node labels and node labels mapping can be recovered across RM restart
    * Update node labels - admin can update labels on nodes and labels on queues
      when RM is running
* Mapping of NM to node labels can be done in three ways, but in all of the approaches Partition Label should be one among the valid node labels list configured in the RM.
    * **Centralized :** Node to labels mapping can be done through RM exposed CLI, REST or RPC.
    * **Distributed :** Node to labels mapping will be set by a configured Node Labels Provider in NM. We have two different providers in YARN: *Script* based provider and *Configuration* based provider. In case of script, NM can be configured with a script path and the script can emit the labels of the node. In case of config, node Labels can be directly configured in the NM's yarn-site.xml. In both of these options dynamic refresh of the label mapping is supported.
    * **Delegated-Centralized :** Node to labels mapping will be set by a configured Node Labels Provider in RM. This would be helpful when label mapping cannot be provided by each node due to security concerns and to avoid interaction through RM Interfaces for each node in a large cluster. Labels will be fetched from this interface during NM registration and periodical refresh is also supported.

Configuration
-------------

###Setting up ResourceManager to enable Node Labels

Setup following properties in ```yarn-site.xml```

Property  | Value
--- | ----
yarn.node-labels.fs-store.root-dir  | hdfs://namenode:port/path/to/store/node-labels/
yarn.node-labels.enabled | true
yarn.node-labels.configuration-type | Set configuration type for node labels. Administrators can specify “centralized”, “delegated-centralized” or “distributed”. Default value is “centralized”.

Notes:

* Make sure ```yarn.node-labels.fs-store.root-dir``` is created and ```ResourceManager``` has permission to access it. (Typically from “yarn” user)
* If user want to store node label to local file system of RM (instead of HDFS), paths like `file:///home/yarn/node-label` can be used

###Add/modify node labels list to YARN

* Add cluster node labels list:
    * Executing ```yarn rmadmin -addToClusterNodeLabels "label_1(exclusive=true/false),label_2(exclusive=true/false)"``` to add node label.
    * If user don’t specify “(exclusive=…)”, exclusive will be ```true``` by default.
    * Run ```yarn cluster --list-node-labels``` to check added node labels are visible in the cluster.

###Remove node labels from YARN

* Remove cluster node labels:
    * To remove one or more node labels, execute the following command: ```yarn rmadmin -removeFromClusterNodeLabels "<label>[,<label>,...]"```. The command argument should be a comma-separated list of node labels to remove.
    * It is not allowed to remove a label which has been associated with queues, i.e., one or more queues have access to this label.
    * To verify if specified node labels have been successfully removed, run ```yarn cluster --list-node-labels```.

###Add/modify node-to-labels mapping to YARN

* Configuring nodes to labels mapping in **Centralized** NodeLabel setup
    * Executing ```yarn rmadmin -replaceLabelsOnNode “node1[:port]=label1 node2=label2” [-failOnUnknownNodes]```. Added label1 to node1, label2 to node2. If user don’t specify port, it adds the label to all ```NodeManagers``` running on the node. If option ```-failOnUnknownNodes``` is set, this command will fail if specified nodes are unknown.

* Configuring nodes to labels mapping in **Distributed** NodeLabel setup

Property  | Value
----- | ------
yarn.node-labels.configuration-type | Needs to be set as *"distributed"* in RM, to fetch node to labels mapping from a configured Node Labels Provider in NM.
yarn.nodemanager.node-labels.provider | When *"yarn.node-labels.configuration-type"* is configured with *"distributed"* in RM, Administrators can configure the provider for the node labels by configuring this parameter in NM. Administrators can configure *"config"*, *"script"* or the *class name* of the provider. Configured  class needs to extend *org.apache.hadoop.yarn.server.nodemanager.nodelabels.NodeLabelsProvider*. If *"config"* is configured, then *"ConfigurationNodeLabelsProvider"* and if *"script"* is configured, then *"ScriptNodeLabelsProvider"* will be used.
yarn.nodemanager.node-labels.resync-interval-ms | Interval at which NM syncs its node labels with RM. NM will send its loaded labels every x intervals configured, along with heartbeat to RM. This resync is required even when the labels are not modified because admin might have removed the cluster label which was provided by NM. Default is 2 mins.
yarn.nodemanager.node-labels.provider.fetch-interval-ms | When *"yarn.nodemanager.node-labels.provider"* is configured with *"config"*, *"script"* or the *configured class* extends AbstractNodeLabelsProvider, then periodically node labels are retrieved from the node labels provider. This configuration is to define the interval period. If -1 is configured, then node labels are retrieved from provider only during initialization. Defaults to 10 mins.
yarn.nodemanager.node-labels.provider.fetch-timeout-ms | When *"yarn.nodemanager.node-labels.provider"* is configured with *"script"*, then this configuration provides the timeout period after which it will interrupt the script which queries the node labels. Defaults to 20 mins.
yarn.nodemanager.node-labels.provider.script.path | The node label script to run. Script output Line starting with *"NODE_PARTITION:"* will be considered as node label Partition. In case multiple lines of script output have this pattern, then the last one will be considered.
yarn.nodemanager.node-labels.provider.script.opts | The arguments to pass to the node label script.
yarn.nodemanager.node-labels.provider.configured-node-partition | When *"yarn.nodemanager.node-labels.provider"* is configured with *"config"*, then ConfigurationNodeLabelsProvider fetches the partition label from this parameter.

* Configuring nodes to labels mapping in **Delegated-Centralized** NodeLabel setup

Property  | Value
----- | ------
yarn.node-labels.configuration-type | Needs to be set as *"delegated-centralized"* to fetch node to labels mapping from a configured Node Labels Provider in RM.
yarn.resourcemanager.node-labels.provider | When *"yarn.node-labels.configuration-type"* is configured with *"delegated-centralized"*, then administrators should configure the class for fetching node labels by ResourceManager. Configured class needs to extend *org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsMappingProvider*.
yarn.resourcemanager.node-labels.provider.fetch-interval-ms | When *"yarn.node-labels.configuration-type"* is configured with *"delegated-centralized"*, then periodically node labels are retrieved from the node labels provider. This configuration is to define the interval. If -1 is configured, then node labels are retrieved from provider only once for each node after it registers. Defaults to 30 mins.

###Configuration of Schedulers for node labels

* Capacity Scheduler Configuration

Property  | Value
----- | ------
yarn.scheduler.capacity.`<queue-path>`.capacity | Set the percentage of the queue can access to nodes belong to DEFAULT partition. The sum of DEFAULT capacities for direct children under each parent, must be equal to 100.
yarn.scheduler.capacity.`<queue-path>`.accessible-node-labels | Admin need specify labels can be accessible by each queue, split by comma, like “hbase,storm” means queue can access label hbase and storm. All queues can access to nodes without label, user don’t have to specify that. If user don’t specify this field, it will inherit from its parent. If user want to explicitly specify a queue can only access nodes without labels, just put a space as the value.
yarn.scheduler.capacity.`<queue-path>`.accessible-node-labels.`<label>`.capacity | Set the percentage of the queue can access to nodes belong to `<label>` partition . The sum of `<label>` capacities for direct children under each parent, must be equal to 100. By default, it's 0.
yarn.scheduler.capacity.`<queue-path>`.accessible-node-labels.`<label>`.maximum-capacity | Similar to yarn.scheduler.capacity.`<queue-path>`.maximum-capacity, it is for maximum-capacity for labels of each queue. By default, it's 100.
yarn.scheduler.capacity.`<queue-path>`.default-node-label-expression | Value like “hbase”, which means: if applications submitted to the queue without specifying node label in their resource requests, it will use "hbase" as default-node-label-expression. By default, this is empty, so application will get containers from nodes without label.

**An example of node label configuration**:

Assume we have a queue structure

```
                root
            /     |    \
     engineer    sales  marketing
```

We have 5 nodes (hostname=h1..h5) in the cluster, each of them has 24G memory, 24 vcores. 1 among the 5 nodes has GPU (assume it’s h5). So admin added GPU label to h5.

Assume user have a Capacity Scheduler configuration like: (key=value is used here for readability)

```
yarn.scheduler.capacity.root.queues=engineering,marketing,sales
yarn.scheduler.capacity.root.engineering.capacity=33
yarn.scheduler.capacity.root.marketing.capacity=34
yarn.scheduler.capacity.root.sales.capacity=33

yarn.scheduler.capacity.root.engineering.accessible-node-labels=GPU
yarn.scheduler.capacity.root.marketing.accessible-node-labels=GPU

yarn.scheduler.capacity.root.engineering.accessible-node-labels.GPU.capacity=50
yarn.scheduler.capacity.root.marketing.accessible-node-labels.GPU.capacity=50

yarn.scheduler.capacity.root.engineering.default-node-label-expression=GPU
```

You can see root.engineering/marketing/sales.capacity=33, so each of them can has guaranteed resource equals to 1/3 of resource **without partition**. So each of them can use 1/3 resource of h1..h4, which is 24 * 4 * (1/3) = (32G mem, 32 v-cores).

And only engineering/marketing queue has permission to access GPU partition (see root.`<queue-name>`.accessible-node-labels).

Each of engineering/marketing queue has guaranteed resource equals to 1/2 of resource **with partition=GPU**. So each of them can use 1/2 resource of h5, which is 24 * 0.5 = (12G mem, 12 v-cores).

Notes:

* After finishing configuration of CapacityScheduler, execute ```yarn rmadmin -refreshQueues``` to apply changes
* Go to scheduler page of RM Web UI to check if you have successfully set configuration.

Specifying node label for application
-------------------------------------

Applications can use following Java APIs to specify node label to request

* `ApplicationSubmissionContext.setNodeLabelExpression(..)` to set node label expression for all containers of the application.
* `ResourceRequest.setNodeLabelExpression(..)` to set node label expression for individual resource requests. This can overwrite node label expression set in ApplicationSubmissionContext
* Specify `setAMContainerResourceRequest.setNodeLabelExpression` in `ApplicationSubmissionContext` to indicate expected node label for application master container.

Monitoring
----------

###Monitoring through web UI

Following label-related fields can be seen on web UI:

* Nodes page: http://RM-Address:port/cluster/nodes, you can get labels on each node
* Node labels page: http://RM-Address:port/cluster/nodelabels, you can get type (exclusive/non-exclusive), number of active node managers, total resource of each partition
* Scheduler page: http://RM-Address:port/cluster/scheduler, you can get label-related settings of each queue, and resource usage of queue partitions.

###Monitoring through commandline

* Use `yarn cluster --list-node-labels` to get labels in the cluster
* Use `yarn node -status <NodeId>` to get node status including labels on a given node

Useful links
------------

* [YARN Capacity Scheduler](./CapacityScheduler.html), if you need more understanding about how to configure Capacity Scheduler
* Write YARN application using node labels, you can see following two links as examples: [YARN distributed shell](https://issues.apache.org/jira/browse/YARN-2502), [Hadoop MapReduce](https://issues.apache.org/jira/browse/MAPREDUCE-6304)
