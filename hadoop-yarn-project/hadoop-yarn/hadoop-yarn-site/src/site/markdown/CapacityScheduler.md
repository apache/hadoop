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

Hadoop: Capacity Scheduler
==========================

* [Purpose](#Purpose)
* [Overview](#Overview)
* [Features](#Features)
* [Configuration](#Configuration)
    * [Setting up `ResourceManager` to use `CapacityScheduler`](#Setting_up_ResourceManager_to_use_CapacityScheduler`)
    * [Setting up queues](#Setting_up_queues)
    * [Queue Properties](#Queue_Properties)
    * [Other Properties](#Other_Properties)
    * [Reviewing the configuration of the CapacityScheduler](#Reviewing_the_configuration_of_the_CapacityScheduler)
* [Changing Queue Configuration](#Changing_Queue_Configuration)

Purpose
-------

This document describes the `CapacityScheduler`, a pluggable scheduler for Hadoop which allows for multiple-tenants to securely share a large cluster such that their applications are allocated resources in a timely manner under constraints of allocated capacities.

Overview
--------

The `CapacityScheduler` is designed to run Hadoop applications as a shared, multi-tenant cluster in an operator-friendly manner while maximizing the throughput and the utilization of the cluster.

Traditionally each organization has it own private set of compute resources that have sufficient capacity to meet the organization's SLA under peak or near peak conditions. This generally leads to poor average utilization and overhead of managing multiple independent clusters, one per each organization. Sharing clusters between organizations is a cost-effective manner of running large Hadoop installations since this allows them to reap benefits of economies of scale without creating private clusters. However, organizations are concerned about sharing a cluster because they are worried about others using the resources that are critical for their SLAs.

The `CapacityScheduler` is designed to allow sharing a large cluster while giving each organization capacity guarantees. The central idea is that the available resources in the Hadoop cluster are shared among multiple organizations who collectively fund the cluster based on their computing needs. There is an added benefit that an organization can access any excess capacity not being used by others. This provides elasticity for the organizations in a cost-effective manner.

Sharing clusters across organizations necessitates strong support for multi-tenancy since each organization must be guaranteed capacity and safe-guards to ensure the shared cluster is impervious to single rouge application or user or sets thereof. The `CapacityScheduler` provides a stringent set of limits to ensure that a single application or user or queue cannot consume disproportionate amount of resources in the cluster. Also, the `CapacityScheduler` provides limits on initialized/pending applications from a single user and queue to ensure fairness and stability of the cluster.

The primary abstraction provided by the `CapacityScheduler` is the concept of *queues*. These queues are typically setup by administrators to reflect the economics of the shared cluster.

To provide further control and predictability on sharing of resources, the `CapacityScheduler` supports *hierarchical queues* to ensure resources are shared among the sub-queues of an organization before other queues are allowed to use free resources, there-by providing *affinity* for sharing free resources among applications of a given organization.

Features
--------

The `CapacityScheduler` supports the following features:

* **Hierarchical Queues** - Hierarchy of queues is supported to ensure resources are shared among the sub-queues of an organization before other queues are allowed to use free resources, there-by providing more control and predictability.

* **Capacity Guarantees** - Queues are allocated a fraction of the capacity of the grid in the sense that a certain capacity of resources will be at their disposal. All applications submitted to a queue will have access to the capacity allocated to the queue. Adminstrators can configure soft limits and optional hard limits on the capacity allocated to each queue.

* **Security** - Each queue has strict ACLs which controls which users can submit applications to individual queues. Also, there are safe-guards to ensure that users cannot view and/or modify applications from other users. Also, per-queue and system administrator roles are supported.

* **Elasticity** - Free resources can be allocated to any queue beyond it's capacity. When there is demand for these resources from queues running below capacity at a future point in time, as tasks scheduled on these resources complete, they will be assigned to applications on queues running below the capacity (pre-emption is not supported). This ensures that resources are available in a predictable and elastic manner to queues, thus preventing artifical silos of resources in the cluster which helps utilization.

* **Multi-tenancy** - Comprehensive set of limits are provided to prevent a single application, user and queue from monopolizing resources of the queue or the cluster as a whole to ensure that the cluster isn't overwhelmed.

* **Operability**

    * Runtime Configuration - The queue definitions and properties such as capacity, ACLs can be changed, at runtime, by administrators in a secure manner to minimize disruption to users. Also, a console is provided for users and administrators to view current allocation of resources to various queues in the system. Administrators can *add additional queues* at runtime, but queues cannot be *deleted* at runtime.

    * Drain applications - Administrators can *stop* queues at runtime to ensure that while existing applications run to completion, no new applications can be submitted. If a queue is in `STOPPED` state, new applications cannot be submitted to *itself* or *any of its child queueus*. Existing applications continue to completion, thus the queue can be *drained* gracefully. Administrators can also *start* the stopped queues.

* **Resource-based Scheduling** - Support for resource-intensive applications, where-in a application can optionally specify higher resource-requirements than the default, there-by accomodating applications with differing resource requirements. Currently, *memory* is the the resource requirement supported.

* **Queue Mapping based on User or Group** - This feature allows users to map a job to a specific queue based on the user or group.

Configuration
-------------

###Setting up `ResourceManager` to use `CapacityScheduler`

  To configure the `ResourceManager` to use the `CapacityScheduler`, set the following property in the **conf/yarn-site.xml**:

| Property | Value |
|:---- |:---- |
| `yarn.resourcemanager.scheduler.class` | `org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler` |

###Setting up queues

  `etc/hadoop/capacity-scheduler.xml` is the configuration file for the `CapacityScheduler`.

  The `CapacityScheduler` has a pre-defined queue called *root*. All queueus in the system are children of the root queue.

  Further queues can be setup by configuring `yarn.scheduler.capacity.root.queues` with a list of comma-separated child queues.

  The configuration for `CapacityScheduler` uses a concept called *queue path* to configure the hierarchy of queues. The *queue path* is the full path of the queue's hierarchy, starting at *root*, with . (dot) as the delimiter.

  A given queue's children can be defined with the configuration knob: `yarn.scheduler.capacity.<queue-path>.queues`. Children do not inherit properties directly from the parent unless otherwise noted.

  Here is an example with three top-level child-queues `a`, `b` and `c` and some sub-queues for `a` and `b`:
    
```xml
<property>
  <name>yarn.scheduler.capacity.root.queues</name>
  <value>a,b,c</value>
  <description>The queues at the this level (root is the root queue).
  </description>
</property>

<property>
  <name>yarn.scheduler.capacity.root.a.queues</name>
  <value>a1,a2</value>
  <description>The queues at the this level (root is the root queue).
  </description>
</property>

<property>
  <name>yarn.scheduler.capacity.root.b.queues</name>
  <value>b1,b2,b3</value>
  <description>The queues at the this level (root is the root queue).
  </description>
</property>
```

###Queue Properties

  * Resource Allocation

| Property | Description |
|:---- |:---- |
| `yarn.scheduler.capacity.<queue-path>.capacity` | Queue *capacity* in percentage (%) as a float (e.g. 12.5). The sum of capacities for all queues, at each level, must be equal to 100. Applications in the queue may consume more resources than the queue's capacity if there are free resources, providing elasticity. |
| `yarn.scheduler.capacity.<queue-path>.maximum-capacity` | Maximum queue capacity in percentage (%) as a float. This limits the *elasticity* for applications in the queue. Defaults to -1 which disables it. |
| `yarn.scheduler.capacity.<queue-path>.minimum-user-limit-percent` | Each queue enforces a limit on the percentage of resources allocated to a user at any given time, if there is demand for resources. The user limit can vary between a minimum and maximum value. The the former (the minimum value) is set to this property value and the latter (the maximum value) depends on the number of users who have submitted applications. For e.g., suppose the value of this property is 25. If two users have submitted applications to a queue, no single user can use more than 50% of the queue resources. If a third user submits an application, no single user can use more than 33% of the queue resources. With 4 or more users, no user can use more than 25% of the queues resources. A value of 100 implies no user limits are imposed. The default is 100. Value is specified as a integer. |
| `yarn.scheduler.capacity.<queue-path>.user-limit-factor` | The multiple of the queue capacity which can be configured to allow a single user to acquire more resources. By default this is set to 1 which ensures that a single user can never take more than the queue's configured capacity irrespective of how idle th cluster is. Value is specified as a float. |
| `yarn.scheduler.capacity.<queue-path>.maximum-allocation-mb` | The per queue maximum limit of memory to allocate to each container request at the Resource Manager. This setting overrides the cluster configuration `yarn.scheduler.maximum-allocation-mb`. This value must be smaller than or equal to the cluster maximum. |
| `yarn.scheduler.capacity.<queue-path>.maximum-allocation-vcores` | The per queue maximum limit of virtual cores to allocate to each container request at the Resource Manager. This setting overrides the cluster configuration `yarn.scheduler.maximum-allocation-vcores`. This value must be smaller than or equal to the cluster maximum. |

  * Running and Pending Application Limits
  
  The `CapacityScheduler` supports the following parameters to control the running and pending applications:

| Property | Description |
|:---- |:---- |
| `yarn.scheduler.capacity.maximum-applications` / `yarn.scheduler.capacity.<queue-path>.maximum-applications` | Maximum number of applications in the system which can be concurrently active both running and pending. Limits on each queue are directly proportional to their queue capacities and user limits. This is a hard limit and any applications submitted when this limit is reached will be rejected. Default is 10000. This can be set for all queues with `yarn.scheduler.capacity.maximum-applications` and can also be overridden on a per queue basis by setting `yarn.scheduler.capacity.<queue-path>.maximum-applications`. Integer value expected. |
| `yarn.scheduler.capacity.maximum-am-resource-percent` / `yarn.scheduler.capacity.<queue-path>.maximum-am-resource-percent` | Maximum percent of resources in the cluster which can be used to run application masters - controls number of concurrent active applications. Limits on each queue are directly proportional to their queue capacities and user limits. Specified as a float - ie 0.5 = 50%. Default is 10%. This can be set for all queues with `yarn.scheduler.capacity.maximum-am-resource-percent` and can also be overridden on a per queue basis by setting `yarn.scheduler.capacity.<queue-path>.maximum-am-resource-percent` |

  * Queue Administration & Permissions
  
  The `CapacityScheduler` supports the following parameters to the administer the queues:

| Property | Description |
|:---- |:---- |
| `yarn.scheduler.capacity.<queue-path>.state` | The *state* of the queue. Can be one of `RUNNING` or `STOPPED`. If a queue is in `STOPPED` state, new applications cannot be submitted to *itself* or *any of its child queues*. Thus, if the *root* queue is `STOPPED` no applications can be submitted to the entire cluster. Existing applications continue to completion, thus the queue can be *drained* gracefully. Value is specified as Enumeration. |
| `yarn.scheduler.capacity.root.<queue-path>.acl_submit_applications` | The *ACL* which controls who can *submit* applications to the given queue. If the given user/group has necessary ACLs on the given queue or *one of the parent queues in the hierarchy* they can submit applications. *ACLs* for this property *are* inherited from the parent queue if not specified. |
| `yarn.scheduler.capacity.root.<queue-path>.acl_administer_queue` | The *ACL* which controls who can *administer* applications on the given queue. If the given user/group has necessary ACLs on the given queue or *one of the parent queues in the hierarchy* they can administer applications. *ACLs* for this property *are* inherited from the parent queue if not specified. |

**Note:** An *ACL* is of the form *user1*, *user2spacegroup1*, *group2*. The special value of * implies *anyone*. The special value of *space* implies *no one*. The default is * for the root queue if not specified.

  * Queue Mapping based on User or Group

  The `CapacityScheduler` supports the following parameters to configure the queue mapping based on user or group:

| Property | Description |
|:---- |:---- |
| `yarn.scheduler.capacity.queue-mappings` | This configuration specifies the mapping of user or group to aspecific queue. You can map a single user or a list of users to queues. Syntax: `[u or g]:[name]:[queue_name][,next_mapping]*`. Here, *u or g* indicates whether the mapping is for a user or group. The value is *u* for user and *g* for group. *name* indicates the user name or group name. To specify the user who has submitted the application, %user can be used. *queue_name* indicates the queue name for which the application has to be mapped. To specify queue name same as user name, *%user* can be used. To specify queue name same as the name of the primary group for which the user belongs to, *%primary_group* can be used.|
| `yarn.scheduler.capacity.queue-mappings-override.enable` | This function is used to specify whether the user specified queues can be overridden. This is a Boolean value and the default value is *false*. |

Example:

```
 <property>
   <name>yarn.scheduler.capacity.queue-mappings</name>
   <value>u:user1:queue1,g:group1:queue2,u:%user:%user,u:user2:%primary_group</value>
   <description>
     Here, <user1> is mapped to <queue1>, <group1> is mapped to <queue2>, 
     maps users to queues with the same name as user, <user2> is mapped 
     to queue name same as <primary group> respectively. The mappings will be 
     evaluated from left to right, and the first valid mapping will be used.
   </description>
 </property>
```

###Other Properties

  * Resource Calculator

| Property | Description |
|:---- |:---- |
| `yarn.scheduler.capacity.resource-calculator` | The ResourceCalculator implementation to be used to compare Resources in the scheduler. The default i.e. org.apache.hadoop.yarn.util.resource.DefaultResourseCalculator only uses Memory while DominantResourceCalculator uses Dominant-resource to compare multi-dimensional resources such as Memory, CPU etc. A Java ResourceCalculator class name is expected. |

  * Data Locality

| Property | Description |
|:---- |:---- |
| `yarn.scheduler.capacity.node-locality-delay` | Number of missed scheduling opportunities after which the CapacityScheduler attempts to schedule rack-local containers. Typically, this should be set to number of nodes in the cluster. By default is setting approximately number of nodes in one rack which is 40. Positive integer value is expected. |

###Reviewing the configuration of the CapacityScheduler

  Once the installation and configuration is completed, you can review it after starting the YARN cluster from the web-ui.

  * Start the YARN cluster in the normal manner.

  * Open the `ResourceManager` web UI.

  * The */scheduler* web-page should show the resource usages of individual queues.

Changing Queue Configuration
----------------------------

Changing queue properties and adding new queues is very simple. You need to edit **conf/capacity-scheduler.xml** and run *yarn rmadmin -refreshQueues*.

    $ vi $HADOOP_CONF_DIR/capacity-scheduler.xml
    $ $HADOOP_YARN_HOME/bin/yarn rmadmin -refreshQueues

**Note:** Queues cannot be *deleted*, only addition of new queues is supported - the updated queue configuration should be a valid one i.e. queue-capacity at each *level* should be equal to 100%.
