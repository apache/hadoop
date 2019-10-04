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

Hadoop: YARN Resource Types
===========================

Overview
--------
Resource types support in YARN helps to extend the YARN resource model to a more flexible model which makes it easier to add new countable resource­types. This solution also helps the users to submit jobs with ease to specify the resources they need.

Resource model of YARN
-----------------------
Resource Manager will load a new configuration file named `resource-types.xml` to determine the set of resource ­types for which scheduling is enabled. Sample XML will look like below.

```xml
<configuration>
  <property>
    <name>yarn.resource-types</name>
    <value>resource1, resource2</value>
  </property>

  <property>
    <name>yarn.resource-types.resource1.units</name>
    <value>G</value>
  </property>
</configuration>
```

Similarly, a new configuration file `node­-resources.xml` will also be loaded by Node Manager where the resource capabilities of a node can be specified.

```xml
<configuration>
 <property>
   <name>yarn.nodemanager.resource-type.resource1</name>
   <value>5G</value>
 </property>

 <property>
   <name>yarn.nodemanager.resource-type.resource2</name>
   <value>2m</value>
 </property>

</configuration>
```

Node Manager will use these custom resource types and will register it's capability to Resource Manager.

Configurations
-------------

Please note that, `resource-types.xml` and `node­-resources.xml` file also need to be placed in conf directory if new resources are to be added to YARN.

*In `resource-types.xml`*

| Configuration Property | Value | Description |
|:---- |:---- |:---- |
| `yarn.resource-types` | resource1 | Custom resource  |
| `yarn.resource-types.resource1.units` | G | Default unit for resource1 type  |

*In `node­-resources.xml`*

| Configuration Property | Value | Description |
|:---- |:---- |:---- |
| `yarn.nodemanager.resource-type.resource1` | 5G | Resource capability for resource named 'resource1'. |

