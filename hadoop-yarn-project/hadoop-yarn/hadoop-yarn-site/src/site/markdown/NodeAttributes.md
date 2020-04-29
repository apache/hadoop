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

YARN Node Attributes
===============

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

Overview
--------

Node Attribute is a way to describe the attributes of a Node without resource guarantees. This could be used by applications to pick up the right nodes for their container to be placed based on expression of multitude of these attributes.

Features
--------

The salient features of ```Node Attributes``` is as follows:

* A Node can be associated with multiple attributes.
* Value can be associated with a attribute tagged to a node. String type values are only supported currently.
* Unlike Node Labels, Node Attributes need not be specified explicitly at the cluster level, but there are API's to list the attributes available at the cluster level.
* As its non tangible resource, its not associated with any queue and thus queue resource planning and authorisation is not required for attributes.
* Similar to the allocation tags, Applications will be able to request containers using expressions containing one or more of these attributes using *Placement Constraints*.
* Equals (=) and Not Equals (!=) are the only supported operators in the expression. AND & OR can also be used as part of attribute expression.
* Node attribute constraints are hard limits, that says the allocation can only be made if the node satisfies the node attribute constraint. In another word, the request keeps pending until it finds a valid node satisfying the constraint. There is no relax policy at present.
* Operability
    * Node Attributes and its mapping to nodes can be recovered across RM restart
    * Update node attributes - admin can add, remove and replace attributes on nodes when RM is running
* Mapping of NM to node attributes can be done in two ways,
    * **Centralised :** Node to attributes mapping can be done through RM exposed CLI or RPC (REST is yet to be supported).
    * **Distributed :** Node to attributes mapping will be set by a configured Node Attributes Provider in NM. We have two different providers in YARN: *Script* based provider and *Configuration* based provider. In case of script, NM can be configured with a script path and the script can emit the attribute(s) of the node. In case of config, node Attributes can be directly configured in the NM's yarn-site.xml. In both of these options dynamic refresh of the attribute mapping is supported.

* Unlike labels, attributes can be mapped to a node from both Centralised and Distributed modes at the same time. There will be no clashes as attributes are identified with different prefix in different modes. In case of **Centralized** attributes are identified by prefix *"rm.yarn.io"* and in case of **Distributed** attributes are identified by prefix *"nm.yarn.io"*. This implies attributes are uniquely identified by *prefix* and *name*.

Configuration
-------------

###Setting up ResourceManager for Node Attributes

Unlike Node Labels, Node Attributes need not be explicitly enabled as it will always exist and would have no impact in terms of performance or compatibility even if feature is not used.

Setup following properties in ```yarn-site.xml```

Property  | Value | Default Value
--- | ---- | ----
yarn.node-attribute.fs-store.root-dir  | path where centralized attribute mappings are stored | file:///tmp/hadoop-yarn-${user}/node-attribute/
yarn.node-attribute.fs-store.impl.class | Configured class needs to extend org.apache.hadoop.yarn.nodelabels.NodeAttributeStore | FileSystemNodeAttributeStore


Notes:

* Make sure ```yarn.node-attribute.fs-store.root-dir``` is created with resource manager process user and ```ResourceManager``` has permission to access it. (Typically from “yarn” user)
* If user want to store node attributes to local file system of RM, paths like `file:///home/yarn/node-attributes` can be used else if in hdfs  paths like `hdfs://namenode:port/path/to/store/node-attributes/` can be used.

###Centralised Node Attributes mapping.

Three options are supported to map attributes to node in **Centralised** approach:

* **add**
    Executing ```yarn nodeattributes -add “node1:attribute[(type)][=value],attribute2  node2:attribute2[=value],attribute3``` adds attributes to the nodes without impacting already existing mapping on the node(s).

* **remove**
    Executing ```yarn nodeattributes -remove “node1:attribute,attribute1 node2:attribute2"``` removes attributes to the nodes without impacting already existing mapping on the node(s).

* **replace**
    Executing ```yarn nodeattributes -replace “node1:attribute[(type)][=value],attribute1[=value],attribute2  node2:attribute2[=value],attribute3""``` replaces the existing attributes to the nodes with the one configured as part of this command.

Notes:

* Ports need **not** be mentioned, attributes are mapped to all the NM instances in the node.
* *Space* is the delimiter for multiple node-Attribute mapping pair
* *","* is used as delimiter for multiple attributes of a node.
* *"type"* defaults to string if not specified which is the only type currently supported.
* All the above 3 operations can be performed only by admin user.

###Distributed Node Attributes mapping.

Configuring attributes to nodes in **Distributed** mode

Property  | Value
----- | ------
yarn.nodemanager.node-attributes.provider | Administrators can configure the provider for the node attributes by configuring this parameter in NM. Administrators can configure *"config"*, *"script"* or the *class name* of the provider. Configured  class needs to extend *org.apache.hadoop.yarn.server.nodemanager.nodelabels.NodeAttributesProvider*. If *"config"* is configured, then *"ConfigurationNodeAttributesProvider"* and if *"script"* is configured, then *"ScriptBasedNodeAttributesProvider"* will be used.
yarn.nodemanager.node-attributes.provider.fetch-interval-ms  | When *"yarn.nodemanager.node-attributes.provider"* is configured with *"config"*, *"script"* or the *configured class* extends NodeAttributesProvider, then periodically node attributes are retrieved from the node attributes provider. This configuration is to define the interval period. If -1 is configured, then node attributes are retrieved from provider only during initialisation. Defaults to 10 mins.
yarn.nodemanager.node-attributes.provider.fetch-timeout-ms | When *"yarn.nodemanager.node-attributes.provider"* is configured with *"script"*, then this configuration provides the timeout period after which it will interrupt the script which queries the node attributes. Defaults to 20 mins.
yarn.nodemanager.node-attributes.provider.script.path | The node attribute script NM runs to collect node attributes. Lines in the script output starting with "NODE_ATTRIBUTE:" will be considered as a record of node attribute, attribute name, type and value should be delimited by comma. Each of such lines will be parsed to a node attribute.
yarn.nodemanager.node-attributes.provider.script.opts | The arguments to pass to the node attribute script.
yarn.nodemanager.node-attributes.provider.configured-node-attributes |  When "yarn.nodemanager.node-attributes.provider" is configured with "config" then ConfigurationNodeAttributesProvider fetches node attributes from this parameter.

Specifying node attributes for application
-------------------------------------

Applications can use Placement Constraint APIs to specify node attribute request as mentioned in [Placement Constraint documentation](./PlacementConstraints.html).

Here is an example for creating a Scheduling Request object with NodeAttribute expression:


    //expression : AND(python!=3:java=1.8)
    SchedulingRequest schedulingRequest =
        SchedulingRequest.newBuilder().executionType(
            ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED))
            .allocationRequestId(10L).priority(Priority.newInstance(1))
            .placementConstraintExpression(
                PlacementConstraints.and(
                    PlacementConstraints
                        .targetNodeAttribute(PlacementConstraints.NODE,
                            NodeAttributeOpCode.NE,
                            PlacementConstraints.PlacementTargets
                                .nodeAttribute("python", "3")),
                    PlacementConstraints
                        .targetNodeAttribute(PlacementConstraints.NODE,
                            NodeAttributeOpCode.EQ,
                            PlacementConstraints.PlacementTargets
                                .nodeAttribute("java", "1.8")))
                    .build()).resourceSizing(
            ResourceSizing.newInstance(1, Resource.newInstance(1024, 1)))
            .build();

The above SchedulingRequest requests for 1 container on nodes that must satisfy following constraints:

1. Node attribute *`rm.yarn.io/python`* doesn't exist on the node or it exist but its value is not equal to 3

2. Node attribute *`rm.yarn.io/java`* must exist on the node and its value is equal to 1.8


Monitoring
----------

###Monitoring through REST

As part of *`http://rm-http-address:port/ws/v1/cluster/nodes/{nodeid}`* REST output attributes and its values mapped to the given node can be got.

###Monitoring through web UI

Yet to be supported

###Monitoring through commandline

* Use `yarn cluster --list-node-attributes` to get all the attributes in the cluster
* Use `yarn nodeattributes -list` to get attributes in the cluster
* Use `yarn nodeattributes -attributestonodes  -attributes <Attributes>` to list for each attribute, all the mapped nodes and the attribute value configured for each node. Optionally we can specify for the specified attributes using *-attributes*.
* Use `yarn nodeattributes -nodestoattributes -nodes <Host Names>` to list all the attributes and its value mapped to a node. Optionally we can specify for the specified node using *-nodes*.
* Node status/detail got from `yarn node -status` will list all the attributes and its value associated with the node.

Useful links
------------

*  [Placement Constraint documentation](./PlacementConstraints.html), if you need more understanding about how to configure Placement Constraints.
