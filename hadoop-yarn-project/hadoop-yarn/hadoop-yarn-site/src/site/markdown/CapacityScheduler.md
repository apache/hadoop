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

    * Runtime Configuration - The queue definitions and properties such as capacity, ACLs can be changed, at runtime, by administrators in a secure manner to minimize disruption to users. Also, a console is provided for users and administrators to view current allocation of resources to various queues in the system. Administrators can *add additional queues* at runtime, but queues cannot be *deleted* at runtime unless the queue is STOPPED and has no pending/running apps.

    * Drain applications - Administrators can *stop* queues at runtime to ensure that while existing applications run to completion, no new applications can be submitted. If a queue is in `STOPPED` state, new applications cannot be submitted to *itself* or *any of its child queues*. Existing applications continue to completion, thus the queue can be *drained* gracefully. Administrators can also *start* the stopped queues.

* **Resource-based Scheduling** - Support for resource-intensive applications, where-in a application can optionally specify higher resource-requirements than the default, thereby accommodating applications with differing resource requirements. Currently, *memory* is the resource requirement supported.

* **Queue Mapping Interface based on Default or User Defined Placement Rules** - This feature allows users to map a job to a specific queue based on some default placement rule. For instance based on user & group, or application name. User can also define their own placement rule.

* **Priority Scheduling** - This feature allows applications to be submitted and scheduled with different priorities. Higher integer value indicates higher priority for an application. Currently Application priority is supported only for FIFO ordering policy.

* **Percentage Resource Configuration** - Administrators could specify percentages of resources to a queue.

* **Absolute Resource Configuration** - Administrators could specify absolute resources to a queue instead of providing percentage based values. This provides better control for admins to configure required amount of resources for a given queue.

* **Weight Resource Configuration** - Administrators could specify weights to a queue instead of providing percentage based values. This provides better control for admins to configure resources for the queue in a dynamically changing queue hierarchy.

* **Universal Capacity Vector Resource Configuration** - Administrators could specify resources in a mixed manner to a queue using absolute, weight or percentage modes for each defined resource types. This provides the most flexible way to configure the required amount of resources for a given queue.

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
| `yarn.scheduler.capacity.legacy-queue-mode.enabled` | Disabling the legacy-queue mode opens up the possibility to mix different capacity modes and the usage of the Universal Capacity Vector format to allocate resources flexibly for the queues. Default is *true*. |
| `yarn.scheduler.capacity.<queue-path>.capacity` | Queue *capacity* in percentage (%) as a float (e.g. 12.5), weight as a float with the postfix *w* (e.g. 2.0w) or as absolute resource queue minimum capacity. When using percentage values the sum of capacities for all queues, at each level, must be equal to 100. If absolute resource is configured, sum of absolute resources of child queues could be less than its parent absolute resource capacity. Applications in the queue may consume more resources than the queue's capacity if there are free resources, providing elasticity. When the legacy-queue-mode is disabled the Universal Capacity Vector format can be used to configure the queue capacities, e.g. `[memory=50%,vcores=2w,gpu=1]`. |
| `yarn.scheduler.capacity.<queue-path>.maximum-capacity` | Maximum queue capacity in percentage (%) as a float (when the *capacity* property is defined with either percentages or weights) or as absolute resource queue maximum capacity. This limits the *elasticity* for applications in the queue. 1) Value is between 0 and 100. 2) Admin needs to make sure absolute maximum capacity >= absolute capacity for each queue. Also, setting this value to -1 sets maximum capacity to 100%. When the legacy-queue-mode is disabled the Universal Capacity Vector format can be used to configure the queue capacities, e.g. `[memory=50%,vcores=2w,gpu=1]`. |
| `yarn.scheduler.capacity.minimum-user-limit-percent` / `yarn.scheduler.capacity.<queue-path>.minimum-user-limit-percent` | Each queue enforces a limit on the percentage of resources allocated to a user at any given time, if there is demand for resources. The user limit can vary between a minimum and maximum value. The former (the minimum value) is set to this property value and the latter (the maximum value) depends on the number of users who have submitted applications. For e.g., suppose the value of this property is 25. If two users have submitted applications to a queue, no single user can use more than 50% of the queue resources. If a third user submits an application, no single user can use more than 33% of the queue resources. With 4 or more users, no user can use more than 25% of the queues resources. A value of 100 implies no user limits are imposed. The default is 100. Value is specified as an integer. This can be set for all queues with `yarn.scheduler.capacity.minimum-user-limit-percent` and can also be overridden on a per queue basis by setting `yarn.scheduler.capacity.<queue-path>.minimum-user-limit-percent`. |
| `yarn.scheduler.capacity.user-limit-factor` / `yarn.scheduler.capacity.<queue-path>.user-limit-factor` | User limit factor provides a way to control the max amount of resources that a single user can consume. It is the multiple of the queue's capacity. By default this is set to 1 which ensures that a single user can never take more than the queue's configured capacity irrespective of how idle the cluster is. Increasing it means a single user can use more than the minimum capacity of the cluster, while decreasing it results in lower maximum resources. Setting this to -1 will disable the feature. Value is specified as a float. Note: using the flexible auto queue creation (yarn.scheduler.capacity.\<queue-path\>.auto-queue-creation-v2) with weights will automatically set this property to -1, as the dynamic queues will be created with the hardcoded weight of 1 and in idle cluster scenarios they should be able to use more resources than calculated. This can be set for all queues with `yarn.scheduler.capacity.user-limit-factor` and can also be overridden on a per queue basis by setting `yarn.scheduler.capacity.<queue-path>.user-limit-factor`. |
| `yarn.scheduler.capacity.<queue-path>.maximum-allocation-mb` | The per queue maximum limit of memory to allocate to each container request at the Resource Manager. This setting overrides the cluster configuration `yarn.scheduler.maximum-allocation-mb`. This value must be smaller than or equal to the cluster maximum. |
| `yarn.scheduler.capacity.<queue-path>.maximum-allocation-vcores` | The per queue maximum limit of virtual cores to allocate to each container request at the Resource Manager. This setting overrides the cluster configuration `yarn.scheduler.maximum-allocation-vcores`. This value must be smaller than or equal to the cluster maximum. |
| `yarn.scheduler.capacity.<queue-path>.user-settings.<user-name>.weight` | This floating point value is used when calculating the user limit resource values for users in a queue. This value will weight each user more or less than the other users in the queue. For example, if user A should receive 50% more resources in a queue than users B and C, this property will be set to 1.5 for user A.  Users B and C will default to 1.0. |

  * Configuring Resource Allocation (legacy-queue-mode)

  `CapacityScheduler` supports three different resource allocation configuration modes: percentage values (*relative mode*), weights and absolute resources.

  Relative mode provides a way to describe queue's resources as a fraction of its parent's resources. For example if *capacity* is set as 50.0 for the queue `root.users`, users queue has 50% of root's resources set as minimum capacity.

  In weight mode the resources are divided based on how the queue's weight relates to the sum of configured weights under the same parent. For example if there are three queues under a parent with weights *3w*, *2w*, *5w*, the sum is 10, so the calculated minimum *capacity* will be 30%, 20% and 50% respectively. The benefit of using this mode is flexibility. When using percentages every time a new queue gets added the percentage values need to be manually recalculated, as the sum under a parent must be 100%, but with weights this is performed automatically. Using the previous example when a new queue gets added under the same parent as the previous three with weight *10w* the new sum will be 20, so the new calculated *capacities* will be: 15%, 10%, 25%, 50%. Note: `yarn.scheduler.capacity.<queue-path>.max-capacity` must be configured with percentages, as there is no weight mode for *maximum-capacity*.

  To use absolute resources mode both `yarn.scheduler.capacity.<queue-path>.capacity` and `yarn.scheduler.capacity.<queue-path>.max-capacity` should have absolute resource values like `[memory=10240,vcores=12]`. This configuration indicates 10GB Memory and 12 VCores.

  It is possible to mix weights and percentages in a queue structure, but child queues under one parent must use the same *capacity* mode.

  * Configuring Resource Allocation (non-legacy-queue-mode)

  The capacity and maximum-capacity can be set for the queues using the Universal Capacity Vector format, e.g. `[memory=50%,vcores=2w,gpu=1]`.
  In this example the Memory is set in percentage mode, the Vcores is set in weight mode and the GPU is set in absolute units.
  It is also possible to use to old capacity format, e.g.: `50.0` for percentage, `[memory=1024,vcores=1]` for absolute and `1w` for weight mode.
  Different modes can be mixed freely in the queue hierarchy.

  The hierarchy between the resources is calculated based on the queues' capacity configuration and the available cluster resources.
  Calculating the hierarchy of resources is done in the following way, for each defined resource type:
  1. the configured absolute capacities are allocated to the queues from the available cluster resources
  2. the remaining cluster resources are allocated to the queues with percentage resource configurations
  3. the rest of the resources are allocated to the queues with weighted resource configurations

  Example for mixed queue resource allocation using the Universal Capacity Vector format:
```
    # Configuration
    yarn.scheduler.capacity.legacy-queue-mode.enabled = false
    yarn.scheduler.capacity.root.queues = default, test_1, test_2
    yarn.scheduler.capacity.root.test_1.queues = test_1_1, test_1_2, test_1_3
    yarn.scheduler.capacity.root.default.capacity =         [memory=1w, vcores=1w]
    yarn.scheduler.capacity.root.test_1.capacity =          [memory=16384, vcores=16]
    yarn.scheduler.capacity.root.test_1.test_1_1.capacity = [memory=50%, vcores=50%]
    yarn.scheduler.capacity.root.test_1.test_1_2.capacity = [memory=1w, vcores=1w]
    yarn.scheduler.capacity.root.test_1.test_1_3.capacity = [memory=12288, vcores=12]
    yarn.scheduler.capacity.root.test_2.capacity =          [memory=75%, vcores=75%]

    # ClusterResources=[32GB 32VCores]  EffectiveMin                     AbsoluteCapacity
    root.default              4/32      [memory=4096,    vcores=4]       12.5%
    root.test_1              16/32      [memory=16384,   vcores=16]
    root.test_1.test_1_1        2/16      [memory=2048,  vcores=2]       6.25%
    root.test_1.test_1_2        2/16      [memory=2048,  vcores=2]       6.25%
    root.test_1.test_1_3       12/16      [memory=12288, vcores=12]      37.5%
    root.test_2              12/32      [memory=12288,   vcores=12]      37.5%
```
  Further examples can be found at `TestRMWebServicesCapacitySchedulerMixedMode.java`.

  The capacity for the root queue cannot be configured, it is fixed to 100% (percentage mode).

  The actual capacity, absoluteCapacity and derived properties like maximumApplications are calculated from the hierarchy between the resources.
  If there is no cluster resource, the maximumApplications defaults to the configured values.

  * Running and Pending Application Limits

  The `CapacityScheduler` supports the following parameters to control the running and pending applications:

| Property | Description |
|:---- |:---- |
| `yarn.scheduler.capacity.maximum-applications` / `yarn.scheduler.capacity.<queue-path>.maximum-applications` | Maximum number of applications in the system which can be concurrently active both running and pending. Limits on each queue are directly proportional to their queue absolute capacities and user limits. This is a hard limit and any applications submitted when this limit is reached will be rejected. Default is 10000. This can be set for all queues with `yarn.scheduler.capacity.maximum-applications` and can also be overridden on a per queue basis by setting `yarn.scheduler.capacity.<queue-path>.maximum-applications`. When this property is not set for a specific queue path, the maximum application number is calculated by taking all configured node labels into consideration, and choosing the highest possible value. When the legacy-queue-mode is disabled and no cluster resource is available, it defaults to the configured values. Integer value expected. |
| `yarn.scheduler.capacity.maximum-am-resource-percent` / `yarn.scheduler.capacity.<queue-path>.maximum-am-resource-percent` | Maximum percent of resources in the cluster which can be used to run application masters - controls number of concurrent active applications. Limits on each queue are directly proportional to their queue capacities and user limits. Specified as a float - ie 0.5 = 50%. Default is 10%. This can be set for all queues with `yarn.scheduler.capacity.maximum-am-resource-percent` and can also be overridden on a per queue basis by setting `yarn.scheduler.capacity.<queue-path>.maximum-am-resource-percent` |
| `yarn.scheduler.capacity.max-parallel-apps` / `yarn.scheduler.capacity.<queue-path>.max-parallel-apps` | Maximum number of applications that can run at the same time. Unlike to `maximum-applications`, application submissions are *not* rejected when this limit is reached. Instead they stay in `ACCEPTED` state until they are eligible to run. This can be set for all queues with `yarn.scheduler.capacity.max-parallel-apps` and can also be overridden on a per queue basis by setting `yarn.scheduler.capacity.<queue-path>.max-parallel-apps`. Integer value is expected. By default, there is no limit. The maximum parallel application limit is an inherited property in the queue hierarchy, meaning that the lowest value will be selected as the enforced limit in every branch of the hierarchy. |

  You can also limit the number of parallel applications on a per user basis.

| Property | Description |
|:---- |:---- |
| `yarn.scheduler.capacity.user.max-parallel-apps` | Maximum number of applications that can run at the same time for all users. Default value is unlimited. |
| `yarn.scheduler.capacity.user.<username>.max-parallel-apps` | Maximum number of applications that can run at the same for a specific user. This overrides the global setting. |


 The evaluation of these limits happens in the following order:

1. `maximum-applications` check - if the limit is exceeded, the submission is rejected immediately.

2. `max-parallel-apps` check - the submission is accepted, but the application will not transition to `RUNNING` state. It stays in `ACCEPTED` until the queue / user limits are satisfied.

3. `maximum-am-resource-percent` check - if there are too many Application Masters running, the application stays in `ACCEPTED` state until there is enough room for it.

  * Queue Administration & Permissions
  
  The `CapacityScheduler` supports the following parameters to the administer the queues:

| Property | Description |
|:---- |:---- |
| `yarn.scheduler.capacity.<queue-path>.state` | The *state* of the queue. Can be one of `RUNNING` or `STOPPED`. If a queue is in `STOPPED` state, new applications cannot be submitted to *itself* or *any of its child queues*. Thus, if the *root* queue is `STOPPED` no applications can be submitted to the entire cluster. Existing applications continue to completion, thus the queue can be *drained* gracefully. Value is specified as Enumeration. |
| `yarn.scheduler.capacity.root.<queue-path>.acl_submit_applications` | The *ACL* which controls who can *submit* applications to the given queue. If the given user/group has necessary ACLs on the given queue or *one of the parent queues in the hierarchy* they can submit applications. *ACLs* for this property *are* inherited from the parent queue if not specified. If a tilde (~) is prepended to a user name in this list, the real user's ACLs will allow the proxied user to submit to the queue. |
| `yarn.scheduler.capacity.root.<queue-path>.acl_administer_queue` | The *ACL* which controls who can *administer* applications on the given queue. If the given user/group has necessary ACLs on the given queue or *one of the parent queues in the hierarchy* they can administer applications. *ACLs* for this property *are* inherited from the parent queue if not specified. If a tilde (~) is prepended to a user name in this list, the real user's ACLs will allow the proxied user to administer apps the queue. |

**Note:** An *ACL* is of the form *user1*,*user2* *space* *group1*,*group2*. The special value of * implies *anyone*. The special value of *space* implies *no one*. The default is * for the root queue if not specified.

  * Queue lifetime for applications

    The `CapacityScheduler` supports the following parameters to lifetime of an application:

| Property | Description |
|:---- |:---- |
| `yarn.scheduler.capacity.<queue-path>.maximum-application-lifetime` | Maximum lifetime (in seconds) of an application which is submitted to a queue. Any value less than or equal to zero will be considered as disabled. The default is -1. If positive value is configured then any application submitted to this queue will be killed after it exceeds the configured lifetime. User can also specify lifetime per application in application submission context. However, user lifetime will be overridden if it exceeds queue maximum lifetime. It is point-in-time configuration. Note: This feature can be set at any level in the queue hierarchy. Child queues will inherit their parent's value unless overridden at the child level. A value of 0 means no max lifetime and will override a parent's max lifetime. If this property is not set or is set to a negative number, then this queue's max lifetime value will be inherited from it's parent.|
| `yarn.scheduler.capacity.root.<queue-path>.default-application-lifetime` | Default lifetime (in seconds) of an application which is submitted to a queue. Any value less than or equal to zero will be considered as disabled. If the user has not submitted application with lifetime value then this value will be taken. It is point-in-time configuration. This feature can be set at any level in the queue hierarchy. Child queues will inherit their parent's value unless overridden at the child level. If set to less than or equal to 0, the queue's max value must also be unlimited. Default lifetime can't exceed maximum lifetime. |

  * Queue Mapping based on User or Group, Application Name or user defined placement rules

  The `CapacityScheduler` supports the following parameters to configure the queue mapping based on user or group, user & group, or application name. User can also define their own placement rule:

| Property | Description |
|:---- |:---- |
| `yarn.scheduler.capacity.queue-mappings` | This configuration specifies the mapping of user or group to a specific queue. You can map a single user or a list of users to queues. Syntax: `[u or g]:[name]:[queue_name][,next_mapping]*`. Here, *u or g* indicates whether the mapping is for a user or group. The value is *u* for user and *g* for group. *name* indicates the user name or group name. To specify the user who has submitted the application, %user can be used. *queue_name* indicates the queue name for which the application has to be mapped. To specify queue name same as user name, *%user* can be used. To specify queue name same as the name of the primary group for which the user belongs to, *%primary_group* can be used. Secondary group can be referenced as *%secondary_group* |
| `yarn.scheduler.queue-placement-rules.app-name` | This configuration specifies the mapping of application_name to a specific queue. You can map a single application or a list of applications to queues. Syntax: `[app_name]:[queue_name][,next_mapping]*`. Here, *app_name* indicates the application name you want to do the mapping. *queue_name* indicates the queue name for which the application has to be mapped. To specify the current application's name as the app_name, %application can be used.|
| `yarn.scheduler.capacity.queue-mappings-override.enable` | This function is used to specify whether the user specified queues can be overridden. This is a Boolean value and the default value is *false*. |

Example:

Below example covers single mapping separately. In case of multiple mappings with comma separated values, evaluation would be from left to right, and the first valid mapping will be used. Below example order has been documented based on actual order of execution at runtime in case of multiple mappings.
``` 
 <property>
    <name>yarn.scheduler.capacity.queue-mappings</name>
    <value>u:%user:%primary_group.%user</value>
    <description>Maps users to queue with the same name as user but
    parent queue name should be same as primary group of the user</description>
 </property>
 ...
 <property>
    <name>yarn.scheduler.capacity.queue-mappings</name>
    <value>u:%user:%secondary_group.%user</value>
    <description>Maps users to queue with the same name as user but
    parent queue name should be same as any secondary group of the user</description>
 </property>
 ...
 <property>
    <name>yarn.scheduler.capacity.queue-mappings</name>
    <value>u:%user:%user</value>
    <description>Maps users to queues with the same name as user</description>
 </property>
 ...
 <property>
    <name>yarn.scheduler.capacity.queue-mappings</name>
    <value>u:user2:%primary_group</value>
    <description>user2 is mapped to queue name same as primary group</description>
 </property>
 ...
 <property>
    <name>yarn.scheduler.capacity.queue-mappings</name>
    <value>u:user3:%secondary_group</value>
    <description>user3 is mapped to queue name same as secondary group</description>
 </property>
 ...
 <property>
    <name>yarn.scheduler.capacity.queue-mappings</name>
    <value>u:user1:queue1</value>
    <description>user1 is mapped to queue1</description>
 </property>
 ...
 <property>
    <name>yarn.scheduler.capacity.queue-mappings</name>
    <value>g:group1:queue2</value>
    <description>group1 is mapped to queue2</description>
 </property>
 ...
 <property>
    <name>yarn.scheduler.capacity.queue-mappings</name>
    <value>u:user1:queue1,u:user2:queue2</value>
    <description>Here, <user1> is mapped to <queue1>, <user2> is mapped to <queue2> respectively</description>
 </property>

  <property>
    <name>yarn.scheduler.queue-placement-rules.app-name</name>
    <value>appName1:queue1,%application:%application</value>
    <description>
      Here, <appName1> is mapped to <queue1>, maps applications to queues with
      the same name as application respectively. The mappings will be
      evaluated from left to right, and the first valid mapping will be used.
    </description>
  </property>
```

###JSON-based queue mapping configuration

  In order to make the queue mapping feature more versatile, a new format and evaluation engine has been added to Capacity Scheduler. The new engine is fully backwards compatible with the old one and adds several new features. Note that it can also parse the old format, but the new features are only available if you specify the mappings in JSON.

####Syntax

  Based on the current JSON schema, users can define mapping rules the following way:

```
{
  "rules": [
    {
      "type": "...",
      "matches": "...",
      "policy": "...",
      "parentQueue": "...",
      "customPlacement": "...",
      "fallbackResult":"...",
      "create": true/false,
      "value": "...",
      "customPlacement": "..."
    },
    {
       ... next rule ...
    }
  ]
}
```

Rules are evaluated from top to bottom. Compared to the legacy mapping rule evaluator, it can be adjusted more flexibly what happens when the evaluation stops and a given rule does not match.

####How to enable JSON-based queue mapping

The following properties control how the new placement engine expects rules.

| Setting | Description |
|:---- |:---- |
| `yarn.scheduler.capacity.mapping-rule-format` | Allowed values are `legacy` or `json`. If it is not set, then the engine assumes that the old format might be in use so it also checks the value of `yarn.scheduler.capacity.queue-mappings`. Therefore, this must be set to `json` and cannot be left empty. |
| `yarn.scheduler.capacity.mapping-rule-json` | The value of this property should contain the entire chain of rules inline. This is the preferred way of configuring Capacity Scheduler if you use the Mutation API, ie. modify configuration real-time via the REST interface. |
| `yarn.scheduler.capacity.mapping-rule-json-file` | Defines an absolute path to a JSON file which contains the rules. For example, `/opt/hadoop/config/mapping-rules.json`. |

The property `yarn.scheduler.capacity.mapping-rule-json` takes precedence over `yarn.scheduler.capacity.mapping-rule-json-file`. If the format is set to `json` but you don't define either of these, then you'll get a warning but the initialization of Capacity Scheduler will not fail.

####Differences between legacy and flexible queue auto-creation modes

To use the flexible Queue Auto-Creation under a parent the queue capacities must be configured with weights in legacy-queue-mode. The flexible mode gives the user much more freedom to automatically create new leaf queues or entire queue hierarchies based on mapping rules. "Legacy" mode refers to either percentage-based configuration or where capacities are defined with absolute resources.
If legacy-queue-mode is disabled the capacities can be configured using the Universal Capacity Vector format or using weights `1w`, percentages `75` or absolute units `[memory=1024,vcores=1]`.

In flexible Queue Auto-Creation mode, every parent queue can have dynamically created parent or leaf queues (if the `yarn.scheduler.capacity.<queue-path>.auto-queue-creation-v2.enabled` property is set to true), even if it already has static child queues. This also means that certain settings influence the outcome of the queue placement depending on how the scheduler is configured.

When the mode is relevant, the document explains how certain settings or flags affect the overall logic.

####Rules

  Each mapping rule can have the following settings:

| Setting | Description |
|:---- |:---- |
| `type` | Possible values: `user`, `group`, `application`. It tells the engine what the current rule should be matched against. |
| `matches` | The string to match, or an asterisk "&ast;" which means "all". For example, if the type is `user` and this string is "hadoop" then the rule will only be evaluated if the submitter user is "hadoop". The "&ast;" does not work with groups. |
| `policy` | Selects a list of pre-defined policies which defines where the application should be placed. This will be explained later in the "Policies" section. |
| `parentQueue` | In case of `user`, `primaryGroup`, `primaryGroupUser`, `secondaryGroup`, `secondaryGroupUser` policies, this tells the engine where the matching queue should be looked for. For example, if the policy is `primaryGroup`, parent is `root.groups` and the submitter's group is "admins", then the resulting queue will be "root.groups.admin" |
| `fallbackResult` | If the target queue does not exist or it cannot be created, it defines a fallback action. Valid values are `skip`, `reject` and `placeDefault`. |
| `create` | If set to "false", then the queue will not be created if it does not exist. This flag works differently in flexible and in legacy mode (see below). |
| `value` | If the policy is `setDefaultQueue`, then the default queue will change to this setting from "root.default". Otherwise ignored. |
| `customPlacement` | Only works with `custom` placement policy. The value of this field will be evaluated directly by the engine, which means that various placeholders such as `%application` or `%primary_group` will be replaced with their respective values. |


  `type` is the equivalent of the first column in the old format. It is either "g" or "u" and there is a separate property for application mappings. `matches` is the second column. The only difference is that `%user` means to match all users, but it's not expressive enough. So in the new format, it's been changed to `*`.
  The `fallbackResult` setting is checked what to do when the target queue cannot be created or does not exist. The three settings work the following way:

* `skip`: ignore the current rule and proceed to the next. This is how Fair Scheduler evaluates placement rules.

* `placeDefault`: place the application to the default queue `root.default` (unless it's overridden to something else). This is how Capacity Scheduler works with the old mapping rules.

* `reject`: rejects the submission.

The `create` flag is affected by the mode:

* **Legacy** mode: applies to all parent queues that have the `yarn.scheduler.capacity.<queue-path>.auto-create-child-queue.enabled` set to true.

* **Flexible** mode: applies to all parent queues that have the `yarn.scheduler.capacity.<queue-path>.auto-queue-creation-v2.enabled` set to true.

####Policies

  There are a number of pre-defined placement policies which are similar to those in Fair Scheduler. Many of them can be expressed as a "custom" placement policy as you will see soon, but in many cases, it's safer and more straightforward to use them directly.

| Policy | Description |
|:---- |:---- |
| `specified` | Places the application to the queue that was defined during submission. |
| `reject` | Rejects the submission. |
| `defaultQueue` | Places the application into the default queue `root.default` or to its overwritten value set by `setDefaultQueue`. |
| `user` | Places the application into a queue which matches the username of the submitter. |
| `applicationName` | Places the application into a queue which matches the name of the application. Important: it is case-sensitive, white spaces are not removed. |
| `primaryGroup` | Places the application into a queue which matches the primary group of the submitter. |
| `primaryGroupUser` | Places the application into the queue hierarchy `root.[parentQueue].<primaryGroup>.<userName>`. Note that `parentQueue` is optional. |
| `secondaryGroup` | Places the application into a queue which matches the secondary group of the submitter. |
| `secondaryGroupUser` | Places the application into the queue hierarchy `root.[parentQueue].<secondaryGroup>.<userName>`. Note that `parentQueue` is optional. |
| `setDefaultQueue` | Changes the default queue from `root.default`. The change is permanent in a sense that it is not restored in the next rule. You can change the default queue at any point and as many times as necessary. |
| `custom` | Enables the user to use custom placement strings. See explanation below. |

Notes:

1. The `setDefaultQueue` rule only changes the default queue. If you want to restore the default queue back to `root.default`, then it has to be added to the rule chain again.

2. The nested rules `primaryGroupUser` and `secondaryGroupUser` also work differently in legacy and flexible mode:
    * **Legacy** mode: they expect the parent queues to exist, ie. they cannot be created automatically. More specifically: when you use `primaryGroupUser`, it will result in a queue path like `root.<primaryGroup>.<userName>` and `root.<primaryGroup>` must exist. It can be a managed parent in order to have `userName` leaf created automatically, but the parent still has to be created by hand.
    * **Flexible** mode: as long as the parent allows dynamic queues to be created, there are no limitations. The requested queues will be created.

3. The `custom` placement policy can describe other policies with the appropriate variable placeholders (see below). For example, `primaryGroupUser` with the parent queue `root.groups` can be expressed as `root.groups.%primary_group.%user`. The primary reason for the rules to exist is that its easier to understand for user who have background in configuring Fair Scheduler and it is more natural to configure the mapping rules this way. It is also more robust because it's less likely that the user makes a mistake. The "Variables" section describes what variables are available if you intend to use the `custom` policy.


####Variables

  Internally, the tool populates certain variables with appropriate values. These can be used if `custom` mapping policy is selected. Note that the engine does only minimal verification when it comes to replacing them - therefore it is your responsibility to provide the correct string.

| Variable | Meaning |
|:---- |:---- |
| `%application` | The name of the submitted application. |
| `%user` | The user who submitted the application. |
| `%primary_group` | Primary group of the submitter. |
| `%secondary_group` | Secondary (supplementary) group of the submitter. |
| `%default` | The default queue of the scheduler. |
| `%specified` | Contains the queue what the submitter defined. |

Example: let's say we submit a MapReduce application to a queue `root.users.mrjobs`. In this case, the value of `%specified` will be set to `root.users.mrjobs`.

As explained in the "Policies" section, quite a few policies can be achieved with `custom`. So, instead of using the `specified` policy, you can use `custom` with setting the `customPlacement` field to `%specified`. However, you have much greater control over it, because you can also append or prepend an extra string to these variables. So the following setting is possible: `%specified.%user.largejobs`. Keep in mind that the string must be resolved to a valid queue path in order to have a proper placement.


####Converting the old mapping rule format to the new one

  In this table, you can see how to rewrite the old, colon-separated rules to the new format.

| Old mapping rule | JSON-based mapping rule |
|:---- |:---- |
| `u:username:root.user.queue` | <tt>{ &quot;type&quot;: &quot;user&quot;,<br/>&quot;matches&quot;: &quot;username&quot;,<br/>&quot;policy&quot;: &quot;custom&quot;,<br/>&quot;customPlacement&quot;: &quot;root.user.queue&quot;,<br/>&quot;fallbackResult&quot;:&quot;placeDefault&quot; }</tt> |
| `u:%user:%user` | <tt>{ &quot;type&quot;: &quot;user&quot;,<br/>&quot;matches&quot;: &quot;*&quot;,<br/> &quot;policy&quot;: &quot;user&quot;,<br/> &quot;fallbackResult&quot;:&quot;placeDefault&quot; }</tt> |
| `u:%user:root.parent.%user` | <tt>{ &quot;type&quot;: &quot;user&quot;,<br/>&quot;matches&quot;: &quot;*&quot;,<br/>&quot;policy&quot;: &quot;user&quot;,<br/> &quot;parentQueue&quot;: &quot;root.parent&quot;,<br/> &quot;fallbackResult&quot;:&quot;placeDefault&quot; }</tt> |
| `u:%user:%primary_group` | <tt>{ &quot;type&quot;: &quot;user&quot;,<br/>&quot;matches&quot;: &quot;*&quot;,<br/> &quot;policy&quot;: &quot;primaryGroup&quot;,<br/> &quot;fallbackResult&quot;:&quot;placeDefault&quot; }</tt> |
| `u:%user:%primary_group.%user` | <tt>{ &quot;type&quot;: &quot;user&quot;,<br/>&quot;matches&quot;: &quot;*&quot;,<br/> &quot;policy&quot;: &quot;primaryGroupUser&quot;,<br/> &quot;fallbackResult&quot;:&quot;placeDefault&quot; }</tt> |
| `u:%user:root.groups.%primary_group.%user` | <tt>{ &quot;type&quot;: &quot;user&quot;,<br/>&quot;matches&quot;: &quot;*&quot;,<br/> &quot;policy&quot;: &quot;primaryGroupUser&quot;, <br/>&quot;parentQueue&quot;: &quot;root.groups&quot;,<br/> &quot;fallbackResult&quot;:&quot;placeDefault&quot; }</tt> |
| `u:%user:%secondary_group` | <tt>{ &quot;type&quot;: &quot;user&quot;,<br/>&quot;matches&quot;: &quot;*&quot;,<br/> &quot;policy&quot;: &quot;secondaryGroup&quot;,<br/> &quot;fallbackResult&quot;:&quot;placeDefault&quot; }</tt> |
| `u:%user:%secondary_group.%user` | <tt>{ &quot;type&quot;: &quot;user&quot;,<br/>&quot;matches&quot;: &quot;*&quot;,<br/> &quot;policy&quot;: &quot;secondaryGroupUser&quot;,<br/> &quot;fallbackResult&quot;:&quot;placeDefault&quot; }</tt> |
| `u:%user:root.groups.%secondary_group.%user` | <tt>{ &quot;type&quot;: &quot;user&quot;,<br/>&quot;matches&quot;: &quot;*&quot;,<br/> &quot;policy&quot;: &quot;secondaryGroupUser&quot;,<br/> &quot;parentQueue&quot;: &quot;root.groups&quot;,<br/> &quot;fallbackResult&quot;:&quot;placeDefault&quot; }</tt> |
| `g:hadoop:root.groups.hadoop` | <tt>{ &quot;type&quot;: &quot;group&quot;,<br/>&quot;matches&quot;: &quot;hadoop&quot;,<br/> &quot;policy&quot;: &quot;custom&quot;,<br/> &quot;customPlacement&quot;: &quot;root.groups.hadoop&quot;,<br/> &quot;fallbackResult&quot;:&quot;placeDefault&quot; }</tt> |
| `%application:%application` (application mapping) | <tt>{ &quot;type&quot;: &quot;user&quot;,<br/>&quot;matches&quot;: &quot;*&quot;,<br/> &quot;policy&quot;: &quot;applicationName&quot;,<br/> &quot;fallbackResult&quot;:&quot;placeDefault&quot; }</tt> |
| `hive_query:root.query.hive` (application mapping) | <tt>{ &quot;type&quot;: &quot;application&quot;,<br/>&quot;matches&quot;: &quot;hive_query&quot;,<br/> &quot;policy&quot;: &quot;custom&quot;,<br/>&quot;customPlacement&quot;: &quot;root.query.hive&quot;,<br/>&quot;fallbackResult&quot;:&quot;placeDefault&quot; }</tt> |

  It's worth noting that `%application:%application` requires a `user` type matcher. It is because internally, the "&ast;" is interpreted only for users. If you set the `type` to `application`, then the "&ast;" means to match an application which is named "&ast;".

####Example

  We have a cluster which is shared among developers, QA engineers and test developers.

  We'd like to achieve the following placement logic:

1. If the user belongs to the `devs` primary group, it should be placed to `root.users.devs`. This is reserved for developers.

2. If the user belongs to the `qa` primary group, then the application should go to `root.users.lowpriogroups.<primaryGroup>`. These queues have lower capacities and are intended for testers.

3. If the user belongs to the `qa-dev` primary group, then the application should go to `root.users.highpriogroups.<primaryGroup>`. These queues have higher capacities and are intended for test developers.

4. Put the application into the queue which matches the user name.

5. If there is no such queue, take the queue from the application submission context, but the queue should not be created if it does not exist and the parent is managed.

6. If none of the above matches, then the application should be placed to `root.default`.

7. If the default placement fails for whatever reason, we change the default queue to `root.users.default`.

8. Try a placement to the default queue again.

9. If that fails, reject the submission altogether.

  This means a chain of 9 rules:

 ```json
 {
  "rules":[
    {
      "type": "group",
      "matches": "devs",
      "policy": "custom",
      "customPlacement": "root.users.devs",
      "fallbackResult":"skip"
    },
    {
      "type": "group",
      "matches": "qa",
      "policy": "primaryGroup",
      "parentQueue": "root.users.lowpriogroups",
      "fallbackResult":"skip"
    },
    {
      "type": "group",
      "matches": "qa-dev",
      "policy": "primaryGroup",
      "parentQueue": "root.users.highpriogroups",
      "fallbackResult":"skip"
    },
    {
      "type": "user",
      "matches": "*",
      "policy": "user",
      "fallbackResult":"skip"
    },
    {
      "type": "user",
      "matches": "*",
      "policy": "specified",
      "create": false,
      "fallbackResult":"skip"
    },
    {
      "type": "user",
      "matches": "*",
      "policy": "defaultQueue",
      "fallbackResult":"skip"
    },
    {
      "type": "user",
      "matches": "*",
      "policy": "setDefaultQueue",
      "value": "root.users.default",
      "fallbackResult": "skip"
    },
    {
      "type": "user",
      "matches": "*",
      "policy": "defaultQueue",
      "fallbackResult":"skip"
    },
    {
      "type":"user",
      "matches":"*",
      "policy":"reject"
    }
  ]
}
```

  Note: it's actually possible to set the `fallbackResult` to `reject` on the 8th rule, so you don't need the final `reject`. But using `reject` on its own has its merits: since the `type` and `matches` fields are mandatory, you can reject submissions from certain groups, applications or users.




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
| `yarn.scheduler.capacity.<queue-path>.reservation-planner` | *Optional* parameter: the class name that will be used to determine the implementation of the *Planner*  which will be invoked if the `Plan` capacity fall below (due to scheduled maintenance or node failures) the user reserved resources. The default value is *org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.SimpleCapacityReplanner* which scans the `Plan` and greedily removes reservations in reversed order of acceptance (LIFO) till the reserved resources are within the `Plan` capacity |
| `yarn.scheduler.capacity.<queue-path>.reservation-enforcement-window` | *Optional* parameter representing the time in milliseconds for which the `Planner` will validate if the constraints in the Plan are satisfied. Long value expected. The default value is one hour. |

###Dynamic Auto-Creation and Management of Leaf Queues

The `CapacityScheduler` supports two types of queue auto-creation modes: legacy and flexible. Legacy mode allows the creation of **leaf queues** under parent queues which have been configured to use this feature. Flexible mode allows the creation of both **parent queues** and **leaf queues**. Note: The created queues will be and can only be configured with weights as *capacity*.

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

 * Parent queue configuration for **legacy** dynamic leaf queue auto-creation and management

The `Dynamic Queue Auto-Creation and Management` feature is integrated with the
`CapacityScheduler` queue hierarchy and can be configured for a **ParentQueue** currently to auto-create leaf queues. Such parent queues do not
support other pre-configured queues to co-exist along with auto-created queues. The `CapacityScheduler` supports the following parameters to enable auto-creation of queues

| Property | Description |
|:---- |:---- |
| `yarn.scheduler.capacity.<queue-path>.auto-create-child-queue.enabled` | *Mandatory* parameter: Indicates to the `CapacityScheduler` that auto leaf queue creation needs to be enabled for the specified parent queue.  Boolean value expected. The default value is *false*, i.e. auto leaf queue creation is not enabled in *ParentQueue* by default. |
| `yarn.scheduler.capacity.<queue-path>.auto-create-child-queue.management-policy` | *Optional* parameter: the class name that will be used to determine the implementation of the `AutoCreatedQueueManagementPolicy`  which will manage leaf queues and their capacities dynamically under this parent queue. The default value is *org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.queuemanagement.GuaranteedOrZeroCapacityOverTimePolicy*. Users or groups might submit applications to the auto-created leaf queues for a limited time and stop using them. Hence there could be more number of leaf queues auto-created under the parent queue than its guaranteed capacity. The current policy implementation allots either configured or zero capacity on a **best-effort** basis based on availability of capacity on the parent queue and the application submission order across leaf queues. |


* Configuring **legacy** `Auto-Created Leaf Queues` with `CapacityScheduler`

The parent queue which has been enabled for auto leaf queue creation,supports
 the configuration of template parameters for automatic configuration of the auto-created leaf queues. The auto-created queues support all of the
 leaf queue configuration parameters except for **Absolute Resource** configurations.

| Property | Description |
|:---- |:---- |
| `yarn.scheduler.capacity.<queue-path>.leaf-queue-template.capacity` | *Mandatory* parameter: Specifies the minimum guaranteed capacity for the  auto-created leaf queues. |
| `yarn.scheduler.capacity.<queue-path>.leaf-queue-template.maximum-capacity` | *Optional* parameter: Specifies the maximum capacity for the  auto-created leaf queues. This value must be smaller than or equal to the cluster maximum. |
| `yarn.scheduler.capacity.<queue-path>.leaf-queue-template.<leaf-queue-property>` |  *Optional* parameter: For other queue parameters that can be configured on auto-created leaf queues like maximum-capacity, user-limit-factor, maximum-am-resource-percent ...  - Refer **Queue Properties** section |

Example 1:

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

Example 2:

```
 <property>
   <name>yarn.scheduler.capacity.root.parent2.auto-create-child-queue.enabled</name>
   <value>true</value>
 </property>
 <property>
    <name>yarn.scheduler.capacity.root.parent2.leaf-queue-template.capacity</name>
    <value>[memory=1024,vcores=1]</value>
 </property>
 <property>
    <name>yarn.scheduler.capacity.root.parent2.leaf-queue-template.maximum-capacity</name>
    <value>[memory=10240,vcores=10]</value>
 </property>
```
* Scheduling Edit Policy configuration for auto-created queue management

Admins need to specify an additional `org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueManagementDynamicEditPolicy` scheduling edit policy to the
list of current scheduling edit policies as a comma separated string in `yarn.resourcemanager.scheduler.monitor.policies` configuration. For more details, refer `Capacity Scheduler container preemption` section above

| Property | Description |
|:---- |:---- |
| `yarn.resourcemanager.monitor.capacity.queue-management.monitoring-interval` | Time in milliseconds between invocations of this QueueManagementDynamicEditPolicy policy. Default value is 1500 |

* Parent queue configuration for **flexible** dynamic leaf queue auto-creation and management

The `Flexible Dynamic Queue Auto-Creation and Management` feature allows a **ParentQueue** to auto-create both parent and leaf queues. Such parent queues can have other pre-configured queues co-existing with the auto-created queues. The auto-created queues will have weights as *capacity* so the pre-configured queues under the parent must be configured the same way. The `CapacityScheduler` supports the following parameters to enable auto-creation of queues

| Property | Description |
|:---- |:---- |
| `yarn.scheduler.capacity.<queue-path>.auto-queue-creation-v2.enabled` | *Mandatory* parameter: Indicates to the `CapacityScheduler` that flexible auto queue creation needs to be enabled for the specified parent queue.  Boolean value expected. The default value is *false*, i.e. auto leaf queue creation is not enabled in *ParentQueue* by default. |
| `yarn.scheduler.capacity.<queue-path>.auto-queue-creation-v2.max-queues` | *Optional* parameter: Limits the number of dynamic queues created under a parent queue.  Integer value expected. The default value is *1000*. |


* Configuring **flexible** `Auto-Created Leaf Queues` with `CapacityScheduler`

The parent queue which has the flexible auto queue creation enabled supports the configuration of dynamically created leaf and parent queues through template parameters. The auto-created queues support all of the leaf queue configuration parameters except for **Absolute Resource** configurations.

| Property | Description |
|:---- |:---- |
| `yarn.scheduler.capacity.<queue-path>.auto-queue-creation-v2.template.<queue-property>` | *Optional* parameter: Specifies a queue property (like capacity, maximum-capacity, user-limit-factor, maximum-am-resource-percent ...  - Refer **Queue Properties** section) inherited by the auto-created **parent** and **leaf** queues. Dynamic Queue ACLs set here can be overwritten by the parent-template for dynamic parent queues and with the leaf-template for dynamic leaf queues.  |
| `yarn.scheduler.capacity.<queue-path>.auto-queue-creation-v2.leaf-template.<queue-property>` | *Optional* parameter: Specifies a queue property inherited by auto-created **leaf** queues. |
| `yarn.scheduler.capacity.<queue-path>.auto-queue-creation-v2.parent-template.<queue-property>` |  *Optional* parameter: Specifies a queue property inherited by auto-created **parent** queues. |

Using the following example configuration snippet will instruct the `CapacityScheduler` to:
* enable the flexible auto queue creation for root.parent
* create all of the dynamic queues **two levels below** `root.parent` (for example `root.parent.parent-auto.leaf-auto`) with 80% as the maximum capacity, because of the wildcard queue path (root.parent.*)
* create the dynamic parent queues **directly** under `root.parent` with weight 2
* add the GPU label to every leaf queue created **directly** under `root.parent`
* set the GPU label related weight of every queue **directly** under `root.parent`

```
 <property>
   <name>yarn.scheduler.capacity.root.parent.auto-queue-creation-v2.enabled</name>
   <value>true</value>
 </property>
 <property>
    <name>yarn.scheduler.capacity.root.parent.*.auto-queue-creation-v2.template.maximum-capacity</name>
    <value>80</value>
 </property>
 <property>
    <name>yarn.scheduler.capacity.root.parent.auto-queue-creation-v2.parent-template.capacity</name>
    <value>2w</value>
 </property>
 <property>
    <name>yarn.scheduler.capacity.root.parent.auto-queue-creation-v2.leaf-template.accessible-node-labels</name>
    <value>GPU</value>
 </property>
  <property>
    <name>yarn.scheduler.capacity.root.parent.auto-queue-creation-v2.leaf-template.accessible-node-labels.GPU.capacity</name>
    <value>5w</value>
 </property>
```

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

  The `CapacityScheduler` supports the following parameters to control how many containers can be allocated in each NodeManager heartbeat. These parameters are refreshable via *yarn rmadmin -refreshQueues*.

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

### Enabling periodic configuration refresh
Enabling queue configuration periodic refresh allows reloading and applying the configuration by editing the *conf/capacity-scheduler.xml* without the necessicity of calling yarn rmadmin -refreshQueues.

| Property | Description |
|:---- |:---- |
| `yarn.resourcemanager.scheduler.monitor.enable` | Enabling monitoring is necessary for the periodic refresh. Default value is false. |
| `yarn.resourcemanager.scheduler.monitor.policies` | This is a configuration property that holds a list of classes. Adding more classes means more monitor tasks will be launched, Add `org.apache.hadoop.yarn.server.resourcemanager.capacity.QueueConfigurationAutoRefreshPolicy` to the policies list to enable the periodic refresh. Default value of this property is `org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy`, it means the preemption feature is enabled by default. If the ProportionalCapacityPreemptionPolicy class is removed from the list, it disables the preemption feature. |
| `yarn.resourcemanager.queue.auto.refresh.monitoring-interval` | Adjusting the auto-refresh monitoring interval is possible with this configuration property. The value is in milliseconds. The default value is 5000 (5 seconds). |
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

Activities
--------------------

  Scheduling activities are activity messages used for debugging on some critical scheduling path, they can be recorded and exposed via RESTful API with minor impact on the scheduler performance.
  Currently, there are two types of activities supported: **scheduler activities** and **application activities**.

### Scheduler Activities

  Scheduler activities include useful scheduling info in a scheduling cycle, which illustrate how the scheduler allocates a container.
  Scheduler activities REST API (`http://rm-http-address:port/ws/v1/cluster/scheduler/activities`) provides a way to enable recording scheduler activities and fetch them from cache.
  To eliminate the performance impact, scheduler automatically disables recording activities at the end of a scheduling cycle, you can query the RESTful API again to get the latest scheduler activities.

  See the [YARN Resource Manager REST API](ResourceManagerRest.html#Scheduler_Activities_API) for query parameters, output structure and examples about scheduler activities.

### Application Activities

  Application activities include useful scheduling info for a specified application, which illustrate how the requirements are satisfied or just skipped.
  Application activities REST API (`http://rm-http-address:port/ws/v1/cluster/scheduler/app-activities/{appid}`) provides a way to enable recording application activities for a specified application within a few seconds or fetch historical application activities from cache, available actions which include "refresh" and "get" can be specified by the "actions" parameter:

  * Query with parameter "actions=refresh" will enable recording application activities for the specified application for a certain time (defaults to 3 seconds) and get a simple response like: {"appActivities":{"applicationId":"application_1562308866454_0001","diagnostic":"Successfully received action: refresh","timestamp":1562308869253,"dateTime":"Fri Jul 05 14:41:09 CST 2019"}}.
  * Query with parameter "actions=get" will not enable recording but directly get historical application activities from cache.
  * If no actions parameter is specified, default actions are "refresh,get", which means both "refresh" and "get" will be performed.

  See the [YARN Resource Manager REST API](ResourceManagerRest.html#Application_Activities_API) for query parameters, output structure and examples about application activities.

### Configuration

  The CapacityScheduler supports the following parameters to control the cache size and the expiration of scheduler/application activities.

| Property | Description |
|:---- |:---- |
| `yarn.resourcemanager.activities-manager.cleanup-interval-ms` | The cleanup interval for activities in milliseconds. Defaults to 5000. |
| `yarn.resourcemanager.activities-manager.scheduler-activities.ttl-ms` | Time to live for scheduler activities in milliseconds. Defaults to 600000. |
| `yarn.resourcemanager.activities-manager.app-activities.ttl-ms` | Time to live for application activities in milliseconds. Defaults to 600000. |
| `yarn.resourcemanager.activities-manager.app-activities.max-queue-length` | Max queue length for app activities. Defaults to 100. |

### Web UI

   Activities info is available in the application attempt page on RM Web UI, where outstanding requests are aggregated and displayed.
   Simply click the refresh button to get the latest activities info.
