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

Hadoop: YARN Resource Configuration
===================================

Overview
--------
YARN supports an extensible resource model. By default YARN tracks CPU and
memory for all nodes, applications, and queues, but the resource definition
can be extended to include arbitrary "countable" resources. A countable
resource is a resource that is consumed while a container is running, but is
released afterwards. CPU and memory are both countable resources. Other examples
include GPU resources and software licenses.

In addition, YARN also supports the use of "resource profiles", which allow a
user to specify multiple resource requests through a single profile, similar to
Amazon Web Services Elastic Compute Cluster instance types. For example,
"large" might mean 8 virtual cores and 16GB RAM.

Configuration
-------------

The following configuration properties are supported. See below for details.

`yarn-site.xml`

| Configuration Property | Description |
|:---- |:---- |
| `yarn.resourcemanager.resource-profiles.enabled` | Indicates whether resource profiles support is enabled. Defaults to `false`. |

`resource-types.xml`

| Configuration Property | Value | Description |
|:---- |:---- |:---- |
| `yarn.resource-types` | Comma-separated list of additional resources. May not include `memory`, `memory-mb`, or `vcores` |
| `yarn.resource-types.<resource>.units` | Default unit for the specified resource type |
| `yarn.resource-types.<resource>.minimum` | The minimum request for the specified resource type |
| `yarn.resource-types.<resource>.maximum` | The maximum request for the specified resource type |

`node­-resources.xml`

| Configuration Property | Value | Description |
|:---- |:---- |:---- |
| `yarn.nodemanager.resource-type.<resource>` | The count of the specified resource available from the node manager |

Please note that the `resource-types.xml` and `node­-resources.xml` files
also need to be placed in the same configuration directory as `yarn-site.xml` if
they are used. Alternatively, the properties may be placed into the
`yarn-site.xml` file instead.

YARN Resource Model
-------------------

### Resource Manager
The resource manager is the final arbiter of what resources in the cluster are
tracked. The resource manager loads its resource definition from XML
configuration files. For example, to define a new resource in addition to
CPU and memory, the following property should be configured:

```xml
<configuration>
  <property>
    <name>yarn.resource-types</name>
    <value>resource1,resource2</value>
    <description>
    The resources to be used for scheduling. Use resource-types.xml
    to specify details about the individual resource types.
    </description>
  </property>
</configuration>
```

A valid resource name must begin with a letter and contain only letters, numbers,
and any of: '.', '_', or '-'. A valid resource name may also be optionally
preceded by a name space followed by a slash. A valid name space consists of
period-separated groups of letters, numbers, and dashes. For example, the
following are valid resource names:

* myresource
* my_resource
* My-Resource01
* com.acme/myresource

The following are examples of invalid resource names:

* 10myresource
* my resource
* com/acme/myresource
* $NS/myresource
* -none-/myresource

For each new resource type defined an optional unit property can be added to
set the default unit for the resource type. Valid values are:

|Unit Name | Meaning |
|:---- |:---- |
| p | pico |
| n | nano |
| u | micro |
| m | milli |
| | default, i.e. no unit |
| k | kilo |
| M | mega |
| G | giga |
| T | tera |
| P | peta |
| Ki | binary kilo, i.e. 1024 |
| Mi | binary mega, i.e. 1024^2 |
| Gi | binary giga, i.e. 1024^3 |
| Ti | binary tera, i.e. 1024^4 |
| Pi | binary peta, i.e. 1024^5 |

The property must be named `yarn.resource-types.<resource>.units`. Each defined
resource may also have optional minimum and maximum properties. The properties
must be named `yarn.resource-types.<resource>.minimum` and
`yarn.resource-types.<resource>.maximum`.

The `yarn.resource-types` property and any unit, mimimum, or maximum properties
may be defined in either the usual `yarn-site.xml` file or in a file named
`resource-types.xml`. For example, the following could appear in either file:

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

  <property>
    <name>yarn.resource-types.resource2.minimum</name>
    <value>1</value>
  </property>

  <property>
    <name>yarn.resource-types.resource2.maximum</name>
    <value>1024</value>
  </property>
</configuration>
```

### Node Manager

Each node manager independently defines the resources that are available from
that node. The resource definition is done through setting a property for each
available resource. The property must be named
`yarn.nodemanager.resource-type.<resource>` and may be placed in the usual
`yarn-site.xml` file or in a file named `node­resources.xml`. The value of the
property should be the amount of that resource offered by the node. For
example:

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

Note that the units used for these resources need not match the definition
held by the resource manager. If the units do not match, the resource
manager will automatically do a conversion.

### Using Resources With MapReduce

MapReduce requests three different kinds of containers from YARN: the
application master container, map containers, and reduce containers. For each
container type, there is a corresponding set of properties that can be used to
set the resources requested.

The properties for setting resource requests in MapReduce are:

| Property | Description |
|:---- |:---- |
| `yarn.app.mapreduce.am.resource.mb` | Sets the memory requested for the application master container to the value in MB. No longer preferred. Use `yarn.app.mapreduce.am.resource.memory-mb` instead. Defaults to 1536. |
| `yarn.app.mapreduce.am.resource.memory` | Sets the memory requested for the application master container to the value in MB. No longer preferred. Use `yarn.app.mapreduce.am.resource.memory-mb` instead. Defaults to 1536. |
| `yarn.app.mapreduce.am.resource.memory-mb` | Sets the memory requested for the application master container to the value in MB. Defaults to 1536. |
| `yarn.app.mapreduce.am.resource.cpu-vcores` | Sets the CPU requested for the application master container to the value. No longer preferred. Use `yarn.app.mapreduce.am.resource.vcores` instead. Defaults to 1. |
| `yarn.app.mapreduce.am.resource.vcores` | Sets the CPU requested for the application master container to the value. Defaults to 1. |
| `yarn.app.mapreduce.am.resource.<resource>` | Sets the quantity requested of `<resource>` for the application master container to the value. If no unit is specified, the default unit for the resource is assumed. See the section on units above. |
| `mapreduce.map.memory.mb` | Sets the memory requested for the all map task containers to the value in MB. No longer preferred. Use `mapreduce.map.resource.memory-mb` instead. Defaults to 1024. |
| `mapreduce.map.resource.memory` | Sets the memory requested for the all map task containers to the value in MB. No longer preferred. Use `mapreduce.map.resource.memory-mb` instead. Defaults to 1024. |
| `mapreduce.map.resource.memory-mb` | Sets the memory requested for the all map task containers to the value in MB. Defaults to 1024. |
| `mapreduce.map.cpu.vcores` | Sets the CPU requested for the all map task containers to the value. No longer preferred. Use `mapreduce.map.resource.vcores` instead. Defaults to 1. |
| `mapreduce.map.resource.vcores` | Sets the CPU requested for the all map task containers to the value. Defaults to 1. |
| `mapreduce.map.resource.<resource>` | Sets the quantity requested of `<resource>` for the all map task containers to the value. If no unit is specified, the default unit for the resource is assumed. See the section on units above. |
| `mapreduce.reduce.memory.mb` | Sets the memory requested for the all reduce task containers to the value in MB. No longer preferred. Use `mapreduce.reduce.resource.memory-mb` instead. Defaults to 1024. |
| `mapreduce.reduce.resource.memory` | Sets the memory requested for the all reduce task containers to the value in MB. No longer preferred. Use `mapreduce.reduce.resource.memory-mb` instead. Defaults to 1024. |
| `mapreduce.reduce.resource.memory-mb` | Sets the memory requested for the all reduce task containers to the value in MB. Defaults to 1024. |
| `mapreduce.reduce.cpu.vcores` | Sets the CPU requested for the all reduce task containers to the value. No longer preferred. Use `mapreduce.reduce.resource.vcores` instead. Defaults to 1. |
| `mapreduce.reduce.resource.vcores` | Sets the CPU requested for the all reduce task containers to the value. Defaults to 1. |
| `mapreduce.reduce.resource.<resource>` | Sets the quantity requested of `<resource>` for the all reduce task containers to the value. If no unit is specified, the default unit for the resource is assumed. See the section on units above. |

Note that these resource requests may be modified by YARN to meet the configured
minimum and maximum resource values or to be a multiple of the configured
increment. See the `yarn.scheduler.maximum-allocation-mb`,
`yarn.scheduler.minimum-allocation-mb`,
`yarn.scheduler.increment-allocation-mb`,
`yarn.scheduler.maximum-allocation-vcores`,
`yarn.scheduler.minimum-allocation-vcores`, and
`yarn.scheduler.increment-allocation-vcores` properties in the YARN scheduler
configuration.

Resource Profiles
-----------------
Resource profiles provides an easy way for users to request a set of resources
with a single profile and a means for administrators to regulate how resources
are consumed.

To configure resource types, the administrator must set
`yarn.resourcemanager.resource-profiles.enabled` ot `true` in the resource
manager's `yarn-site.xml` file. This file defines the supported profiles.
For example:

```json
{
    "small": {
        "memory-mb" : 1024,
        "vcores" : 1
    },
    "default" : {
        "memory-mb" : 2048,
        "vcores" : 2
    },
    "large" : {
        "memory-mb": 4096,
        "vcores" : 4
    },
    "compute" : {
        "memory-mb" : 2048,
        "vcores" : 2,
        "gpu" : 1
    }
}
```
In this example, users have access to four profiles with different resource
settings. Note that in the `compute` profile, the administrator has configured
an additional resource as described above.

### Requesting Profiles

The distributed shell is currently the only client that supports resource
profiles. Using the distributed shell, the user can specify a resource profile
name which will automatically be translated into an appropriate set of resource
requests.

For example:

```hadoop job $DISTSHELL -jar $DISTSHELL -shell_command run.sh -container_resource_profile small```
