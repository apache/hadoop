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

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

Purpose
-------

This document describes the `CapacityScheduler`, a pluggable scheduler for Hadoop which allows for multiple-tenants to securely share a large cluster such that their applications are allocated resources in a timely manner under constraints of allocated capacities.

Overview
--------

The `CapacityScheduler` is designed to run Hadoop applications as a shared, multi-tenant cluster in an operator-friendly manner while maximizing the throughput and the utilization of the cluster.

Traditionally each organization has it own private set of compute resources that have sufficient capacity to meet the organization's SLA under peak or near-peak conditions. This generally leads to poor average utilization and overhead of managing multiple independent clusters, one per each organization. Sharing clusters between organizations is a cost-effective manner of running large Hadoop installations since this allows them to reap benefits of economies of scale without creating private clusters. However, organizations are concerned about sharing a cluster because they are worried about others using the resources that are critical for their SLAs.

The `CapacityScheduler` is designed to allow sharing a large cluster while giving each organization capacity guarantees. The central idea is that the available resources in the Hadoop cluster are shared among multiple organizations who collectively fund the cluster based on their computing needs. There is an added benefit that an organization can access any excess capacity not being used by others. This provides elasticity for the organizations in a cost-effective manner.

Sharing clusters across organizations necessitates strong support for multi-tenancy since each organization must be guaranteed capacity and safe-guards to ensure the shared cluster is impervious to single rogue application or user or sets thereof. The `CapacityScheduler` provides a stringent set of limits to ensure that a single application or user or queue cannot consume disproportionate amount of resources in the cluster. Also, the `CapacityScheduler` provides limits on initialized and pending applications from a single user and queue to ensure fairness and stability of the cluster.

The primary abstraction provided by the `CapacityScheduler` is the concept of *queues*. These queues are typically setup by administrators to reflect the economics of the shared cluster.

To provide further control and predictability on sharing of resources, the `CapacityScheduler` supports *hierarchical queues* to ensure resources are shared among the sub-queues of an organization before other queues are allowed to use free resources, thereby providing *affinity* for sharing free resources among applications of a given organization.

Features
--------

The `CapacityScheduler` supports the following features:

* **Hierarchical Queues** - Hierarchy of queues is supported to ensure resources are shared among the sub-queues of an organization before other queues are allowed to use free resources, thereby providing more control and predictability.

* **Capacity Guarantees** - Queues are allocated a fraction of the capacity of the grid in the sense that a certain capacity of resources will be at their disposal. All applications submitted to a queue will have access to the capacity allocated to the queue. Administrators can configure soft limits and optional hard limits on the capacity allocated to each queue.

* **Security** - Each queue has strict ACLs which controls which users can submit applications to individual queues. Also, there are safe-guards to ensure that users cannot view and/or modify applications from other users. Also, per-queue and system administrator roles are supported.

* **Elasticity** - Free resources can be allocated to any queue beyond its capacity. When there is demand for these resources from queues running below capacity at a future point in time, as tasks scheduled on these resources complete, they will be assigned to applications on queues running below the capacity (preemption is also supported). This ensures that resources are available in a predictable and elastic manner to queues, thus preventing artificial silos of resources in the cluster which helps utilization.

* **Multi-tenancy** - Comprehensive set of limits are provided to prevent a single application, user and queue from monopolizing resources of the queue or the cluster as a whole to ensure that the cluster isn't overwhelmed.

* **Operability**

    * Runtime Configuration - The queue definitions and properties such as capacity, ACLs can be changed, at runtime, by administrators in a secure manner to minimize disruption to users. Also, a console is provided for users and administrators to view current allocation of resources to various queues in the system. Administrators can *add additional queues* at runtime, but queues cannot be *deleted* at runtime unless the queue is STOPPED and nhas no pending/running apps.

    * Drain applications - Administrators can *stop* queues at runtime to ensure that while existing applications run to completion, no new applications can be submitted. If a queue is in `STOPPED` state, new applications cannot be submitted to *itself* or *any of its child queues*. Existing applications continue to completion, thus the queue can be *drained* gracefully. Administrators can also *start* the stopped queues.

* **Resource-based Scheduling** - Support for resource-intensive applications, where-in a application can optionally specify higher resource-requirements than the default, thereby accommodating applications with differing resource requirements. Currently, *memory* is the resource requirement supported.

* **Queue Mapping based on User or Group** - This feature allows users to map a job to a specific queue based on the user or group.

* **Priority Scheduling** - This feature allows applications to be submitted and scheduled with different priorities. Higher integer value indicates higher priority for an application. Currently Application priority is supported only for FIFO ordering policy.

* **Absolute Resource Configuration** - Administrators could specify absolute resources to a queue instead of providing percentage based values. This provides better control for admins to configure required amount of resources for a given queue.

* **Dynamic Auto-Creation and Management of Leaf Queues** - This feature supports auto-creation of **leaf queues** in conjunction with **queue-mapping** which currently supports **user-group** based queue mappings for application placement to a queue. The scheduler also supports capacity management for these queues based on a policy configured on the parent queue.

Configuration
-------------

###Setting up `ResourceManager` to use `CapacityScheduler`

  To configure the `ResourceManager` to use the `CapacityScheduler`, set the following property in the **conf/yarn-site.xml**:

| Property | Value |
|:---- |:---- |
| `yarn.resourcemanager.scheduler.class` | `org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler` |

###Setting up queues

  `etc/hadoop/capacity-scheduler.xml` is the configuration file for the `CapacityScheduler`.

  The `CapacityScheduler` has a predefined queue called *root*. All queues in the system are children of the root queue.

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
| `yarn.scheduler.capacity.<queue-path>.capacity` | Queue *capacity* in percentage (%) as a float (e.g. 12.5) OR as absolute resource queue minimum capacity. The sum of capacities for all queues, at each level, must be equal to 100. However if absolute resource is configured, sum of absolute resources of child queues could be less than it's parent absolute resource capacity. Applications in the queue may consume more resources than the queue's capacity if there are free resources, providing elasticity. |
| `yarn.scheduler.capacity.<queue-path>.maximum-capacity` | Maximum queue capacity in percentage (%) as a float OR as absolute resource queue maximum capacity. This limits the *elasticity* for applications in the queue. 1) Value is between 0 and 100. 2) Admin needs to make sure absolute maximum capacity >= absolute capacity for each queue. Also, setting this value to -1 sets maximum capacity to 100%. |
| `yarn.scheduler.capacity.<queue-path>.minimum-user-limit-percent` | Each queue enforces a limit on the percentage of resources allocated to a user at any given time, if there is demand for resources. The user limit can vary between a minimum and maximum value. The former (the minimum value) is set to this property value and the latter (the maximum value) depends on the number of users who have submitted applications. For e.g., suppose the value of this property is 25. If two users have submitted applications to a queue, no single user can use more than 50% of the queue resources. If a third user submits an application, no single user can use more than 33% of the queue resources. With 4 or more users, no user can use more than 25% of the queues resources. A value of 100 implies no user limits are imposed. The default is 100. Value is specified as a integer. |
| `yarn.scheduler.capacity.<queue-path>.user-limit-factor` | The multiple of the queue capacity which can be configured to allow a single user to acquire more resources. By default this is set to 1 which ensures that a single user can never take more than the queue's configured capacity irrespective of how idle the cluster is. Value is specified as a float. |
| `yarn.scheduler.capacity.<queue-path>.maximum-allocation-mb` | The per queue maximum limit of memory to allocate to each container request at the Resource Manager. This setting overrides the cluster configuration `yarn.scheduler.maximum-allocation-mb`. This value must be smaller than or equal to the cluster maximum. |
| `yarn.scheduler.capacity.<queue-path>.maximum-allocation-vcores` | The per queue maximum limit of virtual cores to allocate to each container request at the Resource Manager. This setting overrides the cluster configuration `yarn.scheduler.maximum-allocation-vcores`. This value must be smaller than or equal to the cluster maximum. |
| `yarn.scheduler.capacity.<queue-path>.user-settings.<user-name>.weight` | This floating point value is used when calculating the user limit resource values for users in a queue. This value will weight each user more or less than the other users in the queue. For example, if user A should receive 50% more resources in a queue than users B and C, this property will be set to 1.5 for user A.  Users B and C will default to 1.0. |

  * Resource Allocation using Absolute Resources configuration

 `CapacityScheduler` supports configuration of absolute resources instead of providing Queue *capacity* in percentage. As mentioned in above configuration section for `yarn.scheduler.capacity.<queue-path>.capacity` and `yarn.scheduler.capacity.<queue-path>.max-capacity`, administrator could specify an absolute resource value like `[memory=10240,vcores=12]`. This is a valid configuration which indicates 10GB Memory and 12 VCores.

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

**Note:** An *ACL* is of the form *user1*,*user2* *space* *group1*,*group2*. The special value of * implies *anyone*. The special value of *space* implies *no one*. The default is * for the root queue if not specified.

  * Queue Mapping based on User or Group

  The `CapacityScheduler` supports the following parameters to configure the queue mapping based on user or group:

| Property | Description |
|:---- |:---- |
| `yarn.scheduler.capacity.queue-mappings` | This configuration specifies the mapping of user or group to a specific queue. You can map a single user or a list of users to queues. Syntax: `[u or g]:[name]:[queue_name][,next_mapping]*`. Here, *u or g* indicates whether the mapping is for a user or group. The value is *u* for user and *g* for group. *name* indicates the user name or group name. To specify the user who has submitted the application, %user can be used. *queue_name* indicates the queue name for which the application has to be mapped. To specify queue name same as user name, *%user* can be used. To specify queue name same as the name of the primary group for which the user belongs to, *%primary_group* can be used.|
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

  * Queue lifetime for applications

    The `CapacityScheduler` supports the following parameters to lifetime of an application:

| Property | Description |
|:---- |:---- |
| `yarn.scheduler.capacity.<queue-path>.maximum-application-lifetime` | Maximum lifetime of an application which is submitted to a queue in seconds. Any value less than or equal to zero will be considered as disabled. This will be a hard time limit for all applications in this queue. If positive value is configured then any application submitted to this queue will be killed after exceeds the configured lifetime. User can also specify lifetime per application basis in application submission context. But user lifetime will be overridden if it exceeds queue maximum lifetime. It is point-in-time configuration. Note : Configuring too low value will result in killing application sooner. This feature is applicable only for leaf queue. |
| `yarn.scheduler.capacity.root.<queue-path>.default-application-lifetime` | Default lifetime of an application which is submitted to a queue in seconds. Any value less than or equal to zero will be considered as disabled. If the user has not submitted application with lifetime value then this value will be taken. It is point-in-time configuration. Note : Default lifetime can't exceed maximum lifetime. This feature is applicable only for leaf queue.|


###Setup for application priority.

  Application priority works only along with FIFO ordering policy. Default ordering policy is FIFO.

  Default priority for an application can be at cluster level and queue level.

  * Cluster-level priority : Any application submitted with a priority greater than the cluster-max priority will have its priority reset to the cluster-max priority.
          $HADOOP_HOME/etc/hadoop/yarn-site.xml is the configuration file for cluster-max priority.

| Property | Description |
|:---- |:---- |
| `yarn.cluster.max-application-priority` | Defines maximum application priority in a cluster. |

  * Leaf Queue-level priority : Each leaf queue provides default priority by the administrator. The queue's default priority will be used for any application submitted without a specified priority.
         $HADOOP_HOME/etc/hadoop/capacity-scheduler.xml is the configuration file for queue-level priority.

| Property | Description |
|:---- |:---- |
| `yarn.scheduler.capacity.root.<leaf-queue-path>.default-application-priority` | Defines default application priority in a leaf queue. |

**Note:** Priority of an application will not be changed when application is moved to different queue.

### Capacity Scheduler container preemption

 The `CapacityScheduler` supports preemption of container from the queues whose resource usage is more than their guaranteed capacity. The following configuration parameters need to be enabled in yarn-site.xml for supporting preemption of application containers.

| Property | Description |
|:---- |:---- |
| `yarn.resourcemanager.scheduler.monitor.enable` | Enable a set of periodic monitors (specified in yarn.resourcemanager.scheduler.monitor.policies) that affect the scheduler. Default value is false. |
| `yarn.resourcemanager.scheduler.monitor.policies` | The list of SchedulingEditPolicy classes that interact with the scheduler. Configured policies need to be compatible with the scheduler. Default value is `org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy` which is compatible with `CapacityScheduler` |

The following configuration parameters can be configured in yarn-site.xml to control the preemption of containers when `ProportionalCapacityPreemptionPolicy` class is configured for `yarn.resourcemanager.scheduler.monitor.policies`

| Property | Description |
|:---- |:---- |
| `yarn.resourcemanager.monitor.capacity.preemption.observe_only` | If true, run the policy but do not affect the cluster with preemption and kill events. Default value is false |
| `yarn.resourcemanager.monitor.capacity.preemption.monitoring_interval` | Time in milliseconds between invocations of this ProportionalCapacityPreemptionPolicy policy. Default value is 3000 |
| `yarn.resourcemanager.monitor.capacity.preemption.max_wait_before_kill` | Time in milliseconds between requesting a preemption from an application and killing the container. Default value is 15000 |
| `yarn.resourcemanager.monitor.capacity.preemption.total_preemption_per_round` | Maximum percentage of resources preempted in a single round. By controlling this value one can throttle the pace at which containers are reclaimed from the cluster. After computing the total desired preemption, the policy scales it back within this limit. Default value is `0.1` |
| `yarn.resourcemanager.monitor.capacity.preemption.max_ignored_over_capacity` | Maximum amount of resources above the target capacity ignored for preemption. This defines a deadzone around the target capacity that helps prevent thrashing and oscillations around the computed target balance. High values would slow the time to capacity and (absent natural.completions) it might prevent convergence to guaranteed capacity. Default value is  `0.1` |
| `yarn.resourcemanager.monitor.capacity.preemption.natural_termination_factor` | Given a computed preemption target, account for containers naturally expiring and preempt only this percentage of the delta. This determines the rate of geometric convergence into the deadzone (`MAX_IGNORED_OVER_CAPACITY`). For example, a termination factor of 0.5 will reclaim almost 95% of resources within 5 * #`WAIT_TIME_BEFORE_KILL`, even absent natural termination. Default value is `0.2` |

 The `CapacityScheduler` supports the following configurations in capacity-scheduler.xml to control the preemption of application containers submitted to a queue.

| Property | Description |
|:---- |:---- |
| `yarn.scheduler.capacity.<queue-path>.disable_preemption` | This configuration can be set to `true` to selectively disable preemption of application containers submitted to a given queue. This property applies only when system wide preemption is enabled by configuring `yarn.resourcemanager.scheduler.monitor.enable` to *true* and `yarn.resourcemanager.scheduler.monitor.policies` to *ProportionalCapacityPreemptionPolicy*. If this property is not set for a queue, then the property value is inherited from the queue's parent. Default value is false.
| `yarn.scheduler.capacity.<queue-path>.intra-queue-preemption.disable_preemption` | This configuration can be set to *true* to selectively disable intra-queue preemption of application containers submitted to a given queue. This property applies only when system wide preemption is enabled by configuring `yarn.resourcemanager.scheduler.monitor.enable` to *true*, `yarn.resourcemanager.scheduler.monitor.policies` to *ProportionalCapacityPreemptionPolicy*, and `yarn.resourcemanager.monitor.capacity.preemption.intra-queue-preemption.enabled` to *true*. If this property is not set for a queue, then the property value is inherited from the queue's parent. Default value is *false*.

###Reservation Properties

  * Reservation Administration & Permissions

  The `CapacityScheduler` supports the following parameters to control the creation, deletion, update, and listing of reservations. Note that any user can update, delete, or list their own reservations. If reservation ACLs are enabled but not defined, everyone will have access. In the examples below, \<queue\> is the queue name. For example, to set the reservation ACL to administer reservations on the default queue, use the property `yarn.scheduler.capacity.root.default.acl_administer_reservations`

| Property | Description |
|:---- |:---- |
| `yarn.scheduler.capacity.root.<queue>.acl_administer_reservations` | The ACL which controls who can *administer* reservations to the given queue. If the given user/group has necessary ACLs on the given queue or they can submit, delete, update and list all reservations. ACLs for this property *are not* inherited from the parent queue if not specified. |
| `yarn.scheduler.capacity.root.<queue>.acl_list_reservations` | The ACL which controls who can *list* reservations to the given queue. If the given user/group has necessary ACLs on the given queue they can list all applications. ACLs for this property *are not* inherited from the parent queue if not specified. |
| `yarn.scheduler.capacity.root.<queue>.acl_submit_reservations` | The ACL which controls who can *submit* reservations to the given queue. If the given user/group has necessary ACLs on the given queue they can submit reservations. ACLs for this property *are not* inherited from the parent queue if not specified. |

### Configuring `ReservationSystem` with `CapacityScheduler`

 The `CapacityScheduler` supports the **ReservationSystem** which allows users to reserve resources ahead of time. The application can request the reserved resources at runtime by specifying the `reservationId` during submission. The following configuration parameters can be configured in yarn-site.xml for `ReservationSystem`.

| Property | Description |
|:---- |:---- |
| `yarn.resourcemanager.reservation-system.enable` | *Mandatory* parameter: to enable the `ReservationSystem` in the **ResourceManager**. Boolean value expected. The default value is *false*, i.e. `ReservationSystem` is not enabled by default. |
| `yarn.resourcemanager.reservation-system.class` | *Optional* parameter: the class name of the `ReservationSystem`. The default value is picked based on the configured Scheduler, i.e. if `CapacityScheduler` is configured, then it is `CapacityReservationSystem`. |
| `yarn.resourcemanager.reservation-system.plan.follower` | *Optional* parameter: the class name of the `PlanFollower` that runs on a timer, and synchronizes the `CapacityScheduler` with the `Plan` and viceversa. The default value is picked based on the configured Scheduler, i.e. if `CapacityScheduler` is configured, then it is `CapacitySchedulerPlanFollower`. |
| `yarn.resourcemanager.reservation-system.planfollower.time-step` | *Optional* parameter: the frequency in milliseconds of the `PlanFollower` timer. Long value expected. The default value is *1000*. |


The `ReservationSystem` is integrated with the `CapacityScheduler` queue hierachy and can be configured for any **LeafQueue** currently. The `CapacityScheduler` supports the following parameters to tune the `ReservationSystem`:

| Property | Description |
|:---- |:---- |
| `yarn.scheduler.capacity.<queue-path>.reservable` | *Mandatory* parameter: indicates to the `ReservationSystem` that the queue's resources is available for users to reserve. Boolean value expected. The default value is *false*, i.e. reservations are not enabled in *LeafQueues* by default. |
| `yarn.scheduler.capacity.<queue-path>.reservation-agent` | *Optional* parameter: the class name that will be used to determine the implementation of the `ReservationAgent`  which will attempt to place the user's reservation request in the `Plan`. The default value is *org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.AlignedPlannerWithGreedy*. |
| `yarn.scheduler.capacity.<queue-path>.reservation-move-on-expiry` | *Optional* parameter to specify to the `ReservationSystem` whether the applications should be moved or killed to the parent reservable queue (configured above) when the associated reservation expires. Boolean value expected. The default value is *true* indicating that the application will be moved to the reservable queue. |
| `yarn.scheduler.capacity.<queue-path>.show-reservations-as-queues` | *Optional* parameter to show or hide the reservation queues in the Scheduler UI. Boolean value expected. The default value is *false*, i.e. reservation queues will be hidden. |
| `yarn.scheduler.capacity.<queue-path>.reservation-policy` | *Optional* parameter: the class name that will be used to determine the implementation of the `SharingPolicy`  which will validate if the new reservation doesn't violate any invariants.. The default value is *org.apache.hadoop.yarn.server.resourcemanager.reservation.CapacityOverTimePolicy*. |
| `yarn.scheduler.capacity.<queue-path>.reservation-window` | *Optional* parameter representing the time in milliseconds for which the `SharingPolicy` will validate if the constraints in the Plan are satisfied. Long value expected. The default value is one day. |
| `yarn.scheduler.capacity.<queue-path>.instantaneous-max-capacity` | *Optional* parameter: maximum capacity at any time in percentage (%) as a float that the `SharingPolicy` allows a single user to reserve. The default value is 1, i.e. 100%. |
| `yarn.scheduler.capacity.<queue-path>.average-capacity` | *Optional* parameter: the average allowed capacity which will aggregated over the *ReservationWindow* in percentage (%) as a float that the `SharingPolicy` allows a single user to reserve. The default value is 1, i.e. 100%. |
| `yarn.scheduler.capacity.<queue-path>.reservation-planner` | *Optional* parameter: the class name that will be used to determine the implementation of the *Planner*  which will be invoked if the `Plan` capacity fall below (due to scheduled maintenance or node failuers) the user reserved resources. The default value is *org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.SimpleCapacityReplanner* which scans the `Plan` and greedily removes reservations in reversed order of acceptance (LIFO) till the reserved resources are within the `Plan` capacity |
| `yarn.scheduler.capacity.<queue-path>.reservation-enforcement-window` | *Optional* parameter representing the time in milliseconds for which the `Planner` will validate if the constraints in the Plan are satisfied. Long value expected. The default value is one hour. |

###Dynamic Auto-Creation and Management of Leaf Queues

The `CapacityScheduler` supports auto-creation of **leaf queues** under parent queues which have been configured to enable this feature.

  * Setup for dynamic auto-created leaf queues through queue mapping

  **user-group queue mapping(s)** listed in `yarn.scheduler.capacity.queue-mappings` need to specify an additional parent queue parameter to
  identify which parent queue the auto-created leaf queues need to be created
   under. Refer above `Queue Mapping based on User or Group` section for more
    details. Please note that such parent queues also need to enable
    auto-creation of child queues as mentioned in `Parent queue configuration
     for dynamic leaf queue creation and management` section below

Example:

```
 <property>
   <name>yarn.scheduler.capacity.queue-mappings</name>
   <value>u:user1:queue1,g:group1:queue2,u:user2:%primary_group,u:%user:parent1.%user</value>
   <description>
     Here, u:%user:parent1.%user mapping allows any <user> other than user1,
     user2 to be mapped to its own user specific leaf queue which
     will be auto-created under <parent1>.
   </description>
 </property>
```

 * Parent queue configuration for dynamic leaf queue auto-creation and management

The `Dynamic Queue Auto-Creation and Management` feature is integrated with the
`CapacityScheduler` queue hierarchy and can be configured for a **ParentQueue** currently to auto-create leaf queues. Such parent queues do not
support other pre-configured queues to co-exist along with auto-created queues. The `CapacityScheduler` supports the following parameters to enable auto-creation of queues

| Property | Description |
|:---- |:---- |
| `yarn.scheduler.capacity.<queue-path>.auto-create-child-queue.enabled` | *Mandatory* parameter: Indicates to the `CapacityScheduler` that auto leaf queue creation needs to be enabled for the specified parent queue.  Boolean value expected. The default value is *false*, i.e. auto leaf queue creation is not enabled in *ParentQueue* by default. |
| `yarn.scheduler.capacity.<queue-path>.auto-create-child-queue.management-policy` | *Optional* parameter: the class name that will be used to determine the implementation of the `AutoCreatedQueueManagementPolicy`  which will manage leaf queues and their capacities dynamically under this parent queue. The default value is *org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.queuemanagement.GuaranteedOrZeroCapacityOverTimePolicy*. Users or groups might submit applications to the auto-created leaf queues for a limited time and stop using them. Hence there could be more number of leaf queues auto-created under the parent queue than its guaranteed capacity. The current policy implementation allots either configured or zero capacity on a **best-effort** basis based on availability of capacity on the parent queue and the application submission order across leaf queues. |


* Configuring `Auto-Created Leaf Queues` with `CapacityScheduler`

The parent queue which has been enabled for auto leaf queue creation,supports
 the configuration of template parameters for automatic configuration of the auto-created leaf queues. The auto-created queues support all of the
 leaf queue configuration parameters except for **Queue ACL**, **Absolute
 Resource** configurations. Queue ACLs are
 currently inherited from the parent queue i.e they are not configurable on the leaf queue template

| Property | Description |
|:---- |:---- |
| `yarn.scheduler.capacity.<queue-path>.leaf-queue-template.capacity` | *Mandatory* parameter: Specifies the minimum guaranteed capacity for the  auto-created leaf queues. Currently *Absolute Resource* configurations are not supported on auto-created leaf queues |
| `yarn.scheduler.capacity.<queue-path>.leaf-queue-template.<leaf-queue-property>` |  *Optional* parameter: For other queue parameters that can be configured on auto-created leaf queues like maximum-capacity, user-limit-factor, maximum-am-resource-percent ...  - Refer **Queue Properties** section |

Example:

```
 <property>
   <name>yarn.scheduler.capacity.root.parent1.auto-create-child-queue.enabled</name>
   <value>true</value>
 </property>
 <property>
    <name>yarn.scheduler.capacity.root.parent1.leaf-queue-template.capacity</name>
    <value>5</value>
 </property>
 <property>
    <name>yarn.scheduler.capacity.root.parent1.leaf-queue-template.maximum-capacity</name>
    <value>100</value>
 </property>
 <property>
    <name>yarn.scheduler.capacity.root.parent1.leaf-queue-template.user-limit-factor</name>
    <value>3.0</value>
 </property>
 <property>
    <name>yarn.scheduler.capacity.root.parent1.leaf-queue-template.ordering-policy</name>
    <value>fair</value>
 </property>
 <property>
    <name>yarn.scheduler.capacity.root.parent1.GPU.capacity</name>
    <value>50</value>
 </property>
 <property>
     <name>yarn.scheduler.capacity.root.parent1.accessible-node-labels</name>
     <value>GPU,SSD</value>
   </property>
 <property>
     <name>yarn.scheduler.capacity.root.parent1.leaf-queue-template.accessible-node-labels</name>
     <value>GPU</value>
  </property>
 <property>
    <name>yarn.scheduler.capacity.root.parent1.leaf-queue-template.accessible-node-labels.GPU.capacity</name>
    <value>5</value>
 </property>
```

* Scheduling Edit Policy configuration for auto-created queue management

Admins need to specify an additional `org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueManagementDynamicEditPolicy` scheduling edit policy to the
list of current scheduling edit policies as a comma separated string in `yarn.resourcemanager.scheduler.monitor.policies` configuration. For more details, refer `Capacity Scheduler container preemption` section above

| Property | Description |
|:---- |:---- |
| `yarn.resourcemanager.monitor.capacity.queue-management.monitoring-interval` | Time in milliseconds between invocations of this QueueManagementDynamicEditPolicy policy. Default value is 1500 |

###Other Properties

  * Resource Calculator

| Property | Description |
|:---- |:---- |
| `yarn.scheduler.capacity.resource-calculator` | The ResourceCalculator implementation to be used to compare Resources in the scheduler. The default i.e. org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator only uses Memory while DominantResourceCalculator uses Dominant-resource to compare multi-dimensional resources such as Memory, CPU etc. A Java ResourceCalculator class name is expected. |

  * Data Locality

Capacity Scheduler leverages `Delay Scheduling` to honor task locality constraints. There are 3 levels of locality constraint: node-local, rack-local and off-switch. The scheduler counts the number of missed opportunities when the locality cannot be satisfied, and waits this count to reach a threshold before relaxing the locality constraint to next level. The threshold can be configured in following properties:

| Property | Description |
|:---- |:---- |
| `yarn.scheduler.capacity.node-locality-delay` | Number of missed scheduling opportunities after which the CapacityScheduler attempts to schedule rack-local containers. Typically, this should be set to number of nodes in the cluster. By default is setting approximately number of nodes in one rack which is 40. Positive integer value is expected. |
| `yarn.scheduler.capacity.rack-locality-additional-delay` |  Number of additional missed scheduling opportunities over the node-locality-delay ones, after which the CapacityScheduler attempts to schedule off-switch containers. By default this value is set to -1, in this case, the number of missed opportunities for assigning off-switch containers is calculated based on the formula `L * C / N`, where `L` is number of locations (nodes or racks) specified in the resource request, `C` is the number of requested containers, and `N` is the size of the cluster. |

Note, this feature should be disabled if YARN is deployed separately with the file system, as locality is meaningless. This can be done by setting `yarn.scheduler.capacity.node-locality-delay` to `-1`, in this case, request's locality constraint is ignored.

  * Container Allocation per NodeManager Heartbeat

  The `CapacityScheduler` supports the following parameters to control how many containers can be allocated in each NodeManager heartbeat.

| Property | Description |
|:---- |:---- |
| `yarn.scheduler.capacity.per-node-heartbeat.multiple-assignments-enabled` | Whether to allow multiple container assignments in one NodeManager heartbeat. Defaults to true. |
| `yarn.scheduler.capacity.per-node-heartbeat.maximum-container-assignments` | If `multiple-assignments-enabled` is true, the maximum amount of containers that can be assigned in one NodeManager heartbeat. Default value is 100, which limits the maximum number of container assignments per heartbeat to 100. Set this value to -1 will disable this limit. |
| `yarn.scheduler.capacity.per-node-heartbeat.maximum-offswitch-assignments` | If `multiple-assignments-enabled` is true, the maximum amount of off-switch containers that can be assigned in one NodeManager heartbeat. Defaults to 1, which represents only one off-switch allocation allowed in one heartbeat. |

###Reviewing the configuration of the CapacityScheduler

  Once the installation and configuration is completed, you can review it after starting the YARN cluster from the web-ui.

  * Start the YARN cluster in the normal manner.

  * Open the `ResourceManager` web UI.

  * The */scheduler* web-page should show the resource usages of individual queues.

Changing Queue Configuration
----------------------------

Changing queue/scheduler properties and adding/removing queues can be done in two ways, via file or via API. This behavior can be changed via `yarn.scheduler.configuration.store.class` in yarn-site.xml. Possible values are *file*, which allows modifying properties via file; *memory*, which allows modifying properties via API, but does not persist changes across restart; *leveldb*, which allows modifying properties via API and stores changes in leveldb backing store; and *zk*, which allows modifying properties via API and stores changes in zookeeper backing store. The default value is *file*.

### Changing queue configuration via file

  To edit by file, you need to edit **conf/capacity-scheduler.xml** and run *yarn rmadmin -refreshQueues*.

    $ vi $HADOOP_CONF_DIR/capacity-scheduler.xml
    $ $HADOOP_YARN_HOME/bin/yarn rmadmin -refreshQueues

#### Deleting queue via file

  Step 1: Stop the queue

  Before deleting a leaf queue, the leaf queue should not have any running/pending apps and has to BE STOPPED by changing `yarn.scheduler.capacity.<queue-path>.state`. See the
  [Queue Administration & Permissions](CapacityScheduler.html#Queue Properties) section. 
  Before deleting a parent queue, all its child queues should not have any running/pending apps and have to BE STOPPED. The parent queue also needs to be STOPPED

  Step 2: Delete the queue

  Remove the queue configurations from the file and run refresh as described above

### Changing queue configuration via API

  Editing by API uses a backing store for the scheduler configuration. To enable this, the following parameters can be configured in yarn-site.xml.

  **Note:** This feature is in alpha phase and is subject to change.

| Property | Description |
|:---- |:---- |
| `yarn.scheduler.configuration.store.class` | The type of backing store to use, as described [above](CapacityScheduler.html#Changing_Queue_Configuration). |
| `yarn.scheduler.configuration.mutation.acl-policy.class` | An ACL policy can be configured to restrict which users can modify which queues. Default value is *org.apache.hadoop.yarn.server.resourcemanager.scheduler.DefaultConfigurationMutationACLPolicy*, which only allows YARN admins to make any configuration modifications. Another value is *org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.QueueAdminConfigurationMutationACLPolicy*, which only allows queue modifications if the caller is an admin of the queue. |
| `yarn.scheduler.configuration.store.max-logs` | Configuration changes are audit logged in the backing store, if using leveldb or zookeeper. This configuration controls the maximum number of audit logs to store, dropping the oldest logs when exceeded. Default is 1000. |
| `yarn.scheduler.configuration.leveldb-store.path` | The storage path of the configuration store when using leveldb. Default value is *${hadoop.tmp.dir}/yarn/system/confstore*. |
| `yarn.scheduler.configuration.leveldb-store.compaction-interval-secs` | The interval for compacting the configuration store in seconds, when using leveldb. Default value is 86400, or one day. |
| `yarn.scheduler.configuration.zk-store.parent-path` | The zookeeper root node path for configuration store related information, when using zookeeper. Default value is */confstore*. |

  **Note:** When enabling scheduler configuration mutations via `yarn.scheduler.configuration.store.class`, *yarn rmadmin -refreshQueues* will be disabled, i.e. it will no longer be possible to update configuration via file.

  See the [YARN Resource Manager REST API](ResourceManagerRest.html#Scheduler_Configuration_Mutation_API) for examples on how to change scheduler configuration via REST, and [YARN Commands Reference](YarnCommands.html#schedulerconf) for examples on how to change scheduler configuration via command line.

Updating a Container (Experimental - API may change in the future)
--------------------

  Once an Application Master has received a Container from the Resource Manager, it may request the Resource Manager to update certain attributes of the container.

  Currently only two types of container updates are supported:

  * **Resource Update** : Where the AM can request the RM to update the resource size of the container. For eg: Change the container from a 2GB, 2 vcore container to a 4GB, 2 vcore container.
  * **ExecutionType Update** : Where the AM can request the RM to update the ExecutionType of the container. For eg: Change the execution type from *GUARANTEED* to *OPPORTUNISTIC* or vice versa.
  
  This is facilitated by the AM populating the **updated_containers** field, which is a list of type **UpdateContainerRequestProto**, in **AllocateRequestProto.** The AM can make multiple container update requests in the same allocate call.
  
  The schema of the **UpdateContainerRequestProto** is as follows:
  
    message UpdateContainerRequestProto {
      required int32 container_version = 1;
      required ContainerIdProto container_id = 2;
      required ContainerUpdateTypeProto update_type = 3;
      optional ResourceProto capability = 4;
      optional ExecutionTypeProto execution_type = 5;
    }

  The **ContainerUpdateTypeProto** is an enum:
  
    enum ContainerUpdateTypeProto {
      INCREASE_RESOURCE = 0;
      DECREASE_RESOURCE = 1;
      PROMOTE_EXECUTION_TYPE = 2;
      DEMOTE_EXECUTION_TYPE = 3;
    }

  As constrained by the above enum, the scheduler currently supports changing either the resource update OR executionType of a container in one update request.
  
  The AM must also provide the latest **ContainerProto** it received from the RM. This is the container which the RM will attempt to update.

  If the RM is able to update the requested container, the updated container will be returned, in the **updated_containers** list field of type **UpdatedContainerProto** in the **AllocateResponseProto** return value of either the same allocate call or in one of the subsequent calls.
  
  The schema of the **UpdatedContainerProto** is as follows:
  
    message UpdatedContainerProto {
      required ContainerUpdateTypeProto update_type = 1;
      required ContainerProto container = 2;
    }
  
  It specifies the type of container update that was performed on the Container and the updated Container object which container an updated token.

  The container token can then be used by the AM to ask the corresponding NM to either start the container, if the container has not already been started or update the container using the updated token.
  
  The **DECREASE_RESOURCE** and **DEMOTE_EXECUTION_TYPE** container updates are automatic - the AM does not explicitly have to ask the NM to decrease the resources of the container. The other update types require the AM to explicitly ask the NM to update the container.
  
  If the **yarn.resourcemanager.auto-update.containers** configuration parameter is set to **true** (false by default), The RM will ensure that all container updates are automatic.
