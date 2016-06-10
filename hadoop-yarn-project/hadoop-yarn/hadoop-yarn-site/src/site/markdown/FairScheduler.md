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

Hadoop: Fair Scheduler
======================

* [Purpose](#Purpose)
* [Introduction](#Introduction)
* [Hierarchical queues with pluggable policies](#Hierarchical_queues_with_pluggable_policies)
* [Automatically placing applications in queues](#Automatically_placing_applications_in_queues)
* [Installation](#Installation)
* [Configuration](#Configuration)
    * [Properties that can be placed in yarn-site.xml](#Properties_that_can_be_placed_in_yarn-site.xml)
    * [Allocation file format](#Allocation_file_format)
    * [Queue Access Control Lists](#Queue_Access_Control_Lists)
* [Administration](#Administration)
    * [Modifying configuration at runtime](#Modifying_configuration_at_runtime)
    * [Monitoring through web UI](#Monitoring_through_web_UI)
    * [Moving applications between queues](#Moving_applications_between_queues)

##Purpose

This document describes the `FairScheduler`, a pluggable scheduler for Hadoop that allows YARN applications to share resources in large clusters fairly.

##Introduction

Fair scheduling is a method of assigning resources to applications such that all apps get, on average, an equal share of resources over time. Hadoop NextGen is capable of scheduling multiple resource types. By default, the Fair Scheduler bases scheduling fairness decisions only on memory. It can be configured to schedule with both memory and CPU, using the notion of Dominant Resource Fairness developed by Ghodsi et al. When there is a single app running, that app uses the entire cluster. When other apps are submitted, resources that free up are assigned to the new apps, so that each app eventually on gets roughly the same amount of resources. Unlike the default Hadoop scheduler, which forms a queue of apps, this lets short apps finish in reasonable time while not starving long-lived apps. It is also a reasonable way to share a cluster between a number of users. Finally, fair sharing can also work with app priorities - the priorities are used as weights to determine the fraction of total resources that each app should get.

The scheduler organizes apps further into "queues", and shares resources fairly between these queues. By default, all users share a single queue, named "default". If an app specifically lists a queue in a container resource request, the request is submitted to that queue. It is also possible to assign queues based on the user name included with the request through configuration. Within each queue, a scheduling policy is used to share resources between the running apps. The default is memory-based fair sharing, but FIFO and multi-resource with Dominant Resource Fairness can also be configured. Queues can be arranged in a hierarchy to divide resources and configured with weights to share the cluster in specific proportions.

In addition to providing fair sharing, the Fair Scheduler allows assigning guaranteed minimum shares to queues, which is useful for ensuring that certain users, groups or production applications always get sufficient resources. When a queue contains apps, it gets at least its minimum share, but when the queue does not need its full guaranteed share, the excess is split between other running apps. This lets the scheduler guarantee capacity for queues while utilizing resources efficiently when these queues don't contain applications.

The Fair Scheduler lets all apps run by default, but it is also possible to limit the number of running apps per user and per queue through the config file. This can be useful when a user must submit hundreds of apps at once, or in general to improve performance if running too many apps at once would cause too much intermediate data to be created or too much context-switching. Limiting the apps does not cause any subsequently submitted apps to fail, only to wait in the scheduler's queue until some of the user's earlier apps finish.

##Hierarchical queues with pluggable policies

The fair scheduler supports hierarchical queues. All queues descend from a queue named "root". Available resources are distributed among the children of the root queue in the typical fair scheduling fashion. Then, the children distribute the resources assigned to them to their children in the same fashion. Applications may only be scheduled on leaf queues. Queues can be specified as children of other queues by placing them as sub-elements of their parents in the fair scheduler allocation file.

A queue's name starts with the names of its parents, with periods as separators. So a queue named "queue1" under the root queue, would be referred to as "root.queue1", and a queue named "queue2" under a queue named "parent1" would be referred to as "root.parent1.queue2". When referring to queues, the root part of the name is optional, so queue1 could be referred to as just "queue1", and a queue2 could be referred to as just "parent1.queue2".

Additionally, the fair scheduler allows setting a different custom policy for each queue to allow sharing the queue's resources in any which way the user wants. A custom policy can be built by extending `org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.SchedulingPolicy`. FifoPolicy, FairSharePolicy (default), and DominantResourceFairnessPolicy are built-in and can be readily used.

Certain add-ons are not yet supported which existed in the original (MR1) Fair Scheduler. Among them, is the use of a custom policies governing priority "boosting" over certain apps.

##Automatically placing applications in queues

The Fair Scheduler allows administrators to configure policies that automatically place submitted applications into appropriate queues. Placement can depend on the user and groups of the submitter and the requested queue passed by the application. A policy consists of a set of rules that are applied sequentially to classify an incoming application. Each rule either places the app into a queue, rejects it, or continues on to the next rule. Refer to the allocation file format below for how to configure these policies.

##Installation

To use the Fair Scheduler first assign the appropriate scheduler class in yarn-site.xml:

    <property>
      <name>yarn.resourcemanager.scheduler.class</name>
      <value>org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler</value>
    </property>

##Configuration

Customizing the Fair Scheduler typically involves altering two files. First, scheduler-wide options can be set by adding configuration properties in the yarn-site.xml file in your existing configuration directory. Second, in most cases users will want to create an allocation file listing which queues exist and their respective weights and capacities. The allocation file is reloaded every 10 seconds, allowing changes to be made on the fly.

###Properties that can be placed in yarn-site.xml

| Property | Description |
|:---- |:---- |
| `yarn.scheduler.fair.allocation.file` | Path to allocation file. An allocation file is an XML manifest describing queues and their properties, in addition to certain policy defaults. This file must be in the XML format described in the next section. If a relative path is given, the file is searched for on the classpath (which typically includes the Hadoop conf directory). Defaults to fair-scheduler.xml. |
| `yarn.scheduler.fair.user-as-default-queue` | Whether to use the username associated with the allocation as the default queue name, in the event that a queue name is not specified. If this is set to "false" or unset, all jobs have a shared default queue, named "default". Defaults to true. If a queue placement policy is given in the allocations file, this property is ignored. |
| `yarn.scheduler.fair.preemption` | Whether to use preemption. Defaults to false. |
| `yarn.scheduler.fair.preemption.cluster-utilization-threshold` | The utilization threshold after which preemption kicks in. The utilization is computed as the maximum ratio of usage to capacity among all resources. Defaults to 0.8f. |
| `yarn.scheduler.fair.sizebasedweight` | Whether to assign shares to individual apps based on their size, rather than providing an equal share to all apps regardless of size. When set to true, apps are weighted by the natural logarithm of one plus the app's total requested memory, divided by the natural logarithm of 2. Defaults to false. |
| `yarn.scheduler.fair.assignmultiple` | Whether to allow multiple container assignments in one heartbeat. Defaults to false. |
| `yarn.scheduler.fair.dynamic.max.assign` | If assignmultiple is true, whether to dynamically determine the amount of resources that can be assigned in one heartbeat. When turned on, about half of the un-allocated resources on the node are allocated to containers in a single heartbeat. Defaults to true. |
| `yarn.scheduler.fair.max.assign` | If assignmultiple is true and dynamic.max.assign is false, the maximum amount of containers that can be assigned in one heartbeat. Defaults to -1, which sets no limit. |
| `yarn.scheduler.fair.locality.threshold.node` | For applications that request containers on particular nodes, the number of scheduling opportunities since the last container assignment to wait before accepting a placement on another node. Expressed as a float between 0 and 1, which, as a fraction of the cluster size, is the number of scheduling opportunities to pass up. The default value of -1.0 means don't pass up any scheduling opportunities. |
| `yarn.scheduler.fair.locality.threshold.rack` | For applications that request containers on particular racks, the number of scheduling opportunities since the last container assignment to wait before accepting a placement on another rack. Expressed as a float between 0 and 1, which, as a fraction of the cluster size, is the number of scheduling opportunities to pass up. The default value of -1.0 means don't pass up any scheduling opportunities. |
| `yarn.scheduler.fair.allow-undeclared-pools` | If this is true, new queues can be created at application submission time, whether because they are specified as the application's queue by the submitter or because they are placed there by the user-as-default-queue property. If this is false, any time an app would be placed in a queue that is not specified in the allocations file, it is placed in the "default" queue instead. Defaults to true. If a queue placement policy is given in the allocations file, this property is ignored. |
| `yarn.scheduler.fair.update-interval-ms` | The interval at which to lock the scheduler and recalculate fair shares, recalculate demand, and check whether anything is due for preemption. Defaults to 500 ms. |
| `yarn.scheduler.increment-allocation-mb` | The fairscheduler grants memory in increments of this value. If you submit a task with resource request that is not a multiple of increment-allocation-mb, the request will be rounded up to the nearest increment. Defaults to 1024 MB. |
| `yarn.scheduler.increment-allocation-vcores` | The fairscheduler grants vcores in increments of this value. If you submit a task with resource request that is not a multiple of increment-allocation-vcores, the request will be rounded up to the nearest increment. Defaults to 1. |

###Allocation file format

The allocation file must be in XML format. The format contains five types of elements:

* **Queue elements**: which represent queues. Queue elements can take an optional attribute 'type', which when set to 'parent' makes it a parent queue. This is useful when we want to create a parent queue without configuring any leaf queues. Each queue element may contain the following properties:

    * minResources: minimum resources the queue is entitled to, in the form "X mb, Y vcores". For the single-resource fairness policy, the vcores value is ignored. If a queue's minimum share is not satisfied, it will be offered available resources before any other queue under the same parent. Under the single-resource fairness policy, a queue is considered unsatisfied if its memory usage is below its minimum memory share. Under dominant resource fairness, a queue is considered unsatisfied if its usage for its dominant resource with respect to the cluster capacity is below its minimum share for that resource. If multiple queues are unsatisfied in this situation, resources go to the queue with the smallest ratio between relevant resource usage and minimum. Note that it is possible that a queue that is below its minimum may not immediately get up to its minimum when it submits an application, because already-running jobs may be using those resources.

    * maxResources: maximum resources a queue is allowed, in the form "X mb, Y vcores". A queue will never be assigned a container that would put its aggregate usage over this limit.

    * maxRunningApps: limit the number of apps from the queue to run at once

    * maxAMShare: limit the fraction of the queue's fair share that can be used to run application masters. This property can only be used for leaf queues. For example, if set to 1.0f, then AMs in the leaf queue can take up to 100% of both the memory and CPU fair share. The value of -1.0f will disable this feature and the amShare will not be checked. The default value is 0.5f.

    * weight: to share the cluster non-proportionally with other queues. Weights default to 1, and a queue with weight 2 should receive approximately twice as many resources as a queue with the default weight.

    * schedulingPolicy: to set the scheduling policy of any queue. The allowed values are "fifo"/"fair"/"drf" or any class that extends `org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.SchedulingPolicy`. Defaults to "fair". If "fifo", apps with earlier submit times are given preference for containers, but apps submitted later may run concurrently if there is leftover space on the cluster after satisfying the earlier app's requests.

    * aclSubmitApps: a list of users and/or groups that can submit apps to the queue. Refer to the ACLs section below for more info on the format of this list and how queue ACLs work.

    * aclAdministerApps: a list of users and/or groups that can administer a queue. Currently the only administrative action is killing an application. Refer to the ACLs section below for more info on the format of this list and how queue ACLs work.

    * minSharePreemptionTimeout: number of seconds the queue is under its minimum share before it will try to preempt containers to take resources from other queues. If not set, the queue will inherit the value from its parent queue.

    * fairSharePreemptionTimeout: number of seconds the queue is under its fair share threshold before it will try to preempt containers to take resources from other queues. If not set, the queue will inherit the value from its parent queue.

    * fairSharePreemptionThreshold: the fair share preemption threshold for the queue. If the queue waits fairSharePreemptionTimeout without receiving fairSharePreemptionThreshold\*fairShare resources, it is allowed to preempt containers to take resources from other queues. If not set, the queue will inherit the value from its parent queue.

* **User elements**: which represent settings governing the behavior of individual users. They can contain a single property: maxRunningApps, a limit on the number of running apps for a particular user.

* **A userMaxAppsDefault element**: which sets the default running app limit for any users whose limit is not otherwise specified.

* **A defaultFairSharePreemptionTimeout element**: which sets the fair share preemption timeout for the root queue; overridden by fairSharePreemptionTimeout element in root queue.

* **A defaultMinSharePreemptionTimeout element**: which sets the min share preemption timeout for the root queue; overridden by minSharePreemptionTimeout element in root queue.

* **A defaultFairSharePreemptionThreshold element**: which sets the fair share preemption threshold for the root queue; overridden by fairSharePreemptionThreshold element in root queue.

* **A queueMaxAppsDefault element**: which sets the default running app limit for queues; overriden by maxRunningApps element in each queue.

* **A queueMaxResourcesDefault element**: which sets the default max resource limit for queue; overriden by maxResources element in each queue.

* **A queueMaxAMShareDefault element**: which sets the default AM resource limit for queue; overriden by maxAMShare element in each queue.

* **A defaultQueueSchedulingPolicy element**: which sets the default scheduling policy for queues; overriden by the schedulingPolicy element in each queue if specified. Defaults to "fair".

* **A queuePlacementPolicy element**: which contains a list of rule elements that tell the scheduler how to place incoming apps into queues. Rules are applied in the order that they are listed. Rules may take arguments. All rules accept the "create" argument, which indicates whether the rule can create a new queue. "Create" defaults to true; if set to false and the rule would place the app in a queue that is not configured in the allocations file, we continue on to the next rule. The last rule must be one that can never issue a continue. Valid rules are:

    * specified: the app is placed into the queue it requested. If the app requested no queue, i.e. it specified "default", we continue. If the app requested a queue name starting or ending with period, i.e. names like ".q1" or "q1." will be rejected.

    * user: the app is placed into a queue with the name of the user who submitted it. Periods in the username will be replace with "\_dot\_", i.e. the queue name for user "first.last" is "first\_dot\_last".

    * primaryGroup: the app is placed into a queue with the name of the primary group of the user who submitted it. Periods in the group name will be replaced with "\_dot\_", i.e. the queue name for group "one.two" is "one\_dot\_two".

    * secondaryGroupExistingQueue: the app is placed into a queue with a name that matches a secondary group of the user who submitted it. The first secondary group that matches a configured queue will be selected. Periods in group names will be replaced with "\_dot\_", i.e. a user with "one.two" as one of their secondary groups would be placed into the "one\_dot\_two" queue, if such a queue exists.

    * nestedUserQueue : the app is placed into a queue with the name of the user under the queue suggested by the nested rule. This is similar to âuserâ rule,the difference being in 'nestedUserQueue' rule,user queues can be created under any parent queue, while 'user' rule creates user queues only under root queue. Note that nestedUserQueue rule would be applied only if the nested rule returns a parent queue.One can configure a parent queue either by setting 'type' attribute of queue to 'parent' or by configuring at least one leaf under that queue which makes it a parent. See example allocation for a sample use case.

    * default: the app is placed into the queue specified in the 'queue' attribute of the default rule. If 'queue' attribute is not specified, the app is placed into 'root.default' queue.

    * reject: the app is rejected.

    An example allocation file is given here:

```xml
<?xml version="1.0"?>
<allocations>
  <queue name="sample_queue">
    <minResources>10000 mb,0vcores</minResources>
    <maxResources>90000 mb,0vcores</maxResources>
    <maxRunningApps>50</maxRunningApps>
    <maxAMShare>0.1</maxAMShare>
    <weight>2.0</weight>
    <schedulingPolicy>fair</schedulingPolicy>
    <queue name="sample_sub_queue">
      <aclSubmitApps>charlie</aclSubmitApps>
      <minResources>5000 mb,0vcores</minResources>
    </queue>
  </queue>

  <queueMaxAMShareDefault>0.5</queueMaxAMShareDefault>
  <queueMaxResourcesDefault>40000 mb,0vcores</queueMaxResourcesDefault>

  <!-- Queue 'secondary_group_queue' is a parent queue and may have
       user queues under it -->
  <queue name="secondary_group_queue" type="parent">
  <weight>3.0</weight>
  </queue>

  <user name="sample_user">
    <maxRunningApps>30</maxRunningApps>
  </user>
  <userMaxAppsDefault>5</userMaxAppsDefault>

  <queuePlacementPolicy>
    <rule name="specified" />
    <rule name="primaryGroup" create="false" />
    <rule name="nestedUserQueue">
        <rule name="secondaryGroupExistingQueue" create="false" />
    </rule>
    <rule name="default" queue="sample_queue"/>
  </queuePlacementPolicy>
</allocations>
```

  Note that for backwards compatibility with the original FairScheduler, "queue" elements can instead be named as "pool" elements.

###Queue Access Control Lists

Queue Access Control Lists (ACLs) allow administrators to control who may take actions on particular queues. They are configured with the aclSubmitApps and aclAdministerApps properties, which can be set per queue. Currently the only supported administrative action is killing an application. An administrator may also submit applications to it. These properties take values in a format like "user1,user2 group1,group2" or " group1,group2". Actions on a queue are permitted if the user/group is a member of the queue ACL or a member of the queue ACL of any of that queue's ancestors. So if queue2 is inside queue1, and user1 is in queue1's ACL, and user2 is in queue2's ACL, then both users may submit to queue2.

**Note:** The delimiter is a space character. To specify only ACL groups, begin the value with a space character.

The root queue's ACLs are "\*" by default which, because ACLs are passed down, means that everybody may submit to and kill applications from every queue. To start restricting access, change the root queue's ACLs to something other than "\*".

###Reservation Access Control Lists

Reservation Access Control Lists (ACLs) allow administrators to control who may take reservation actions on particular queues. They are configured with the aclAdministerReservations, aclListReservations, and the aclSubmitReservations properties, which can be set per queue. Currently the supported administrative actions are updating and deleting reservations. An administrator may also submit and list *all* reservations on the queue. These properties take values in a format like "user1,user2 group1,group2" or " group1,group2". Actions on a queue are permitted if the user/group is a member of the reservation ACL. Note that any user can update, delete, or list their own reservations. If reservation ACLs are enabled but not defined, everyone will have access.

##Administration

The fair scheduler provides support for administration at runtime through a few mechanisms:

###Modifying configuration at runtime

It is possible to modify minimum shares, limits, weights, preemption timeouts and queue scheduling policies at runtime by editing the allocation file. The scheduler will reload this file 10-15 seconds after it sees that it was modified.

###Monitoring through web UI

Current applications, queues, and fair shares can be examined through the ResourceManager's web interface, at `http://*ResourceManager URL*/cluster/scheduler`.

The following fields can be seen for each queue on the web interface:

* Used Resources - The sum of resources allocated to containers within the queue.

* Num Active Applications - The number of applications in the queue that have received at least one container.

* Num Pending Applications - The number of applications in the queue that have not yet received any containers.

* Min Resources - The configured minimum resources that are guaranteed to the queue.

* Max Resources - The configured maximum resources that are allowed to the queue.

* Instantaneous Fair Share - The queue's instantaneous fair share of resources. These shares consider only actives queues (those with running applications), and are used for scheduling decisions. Queues may be allocated resources beyond their shares when other queues aren't using them. A queue whose resource consumption lies at or below its instantaneous fair share will never have its containers preempted.

* Steady Fair Share - The queue's steady fair share of resources. These shares consider all the queues irrespective of whether they are active (have running applications) or not. These are computed less frequently and change only when the configuration or capacity changes.They are meant to provide visibility into resources the user can expect, and hence displayed in the Web UI.

###Moving applications between queues

The Fair Scheduler supports moving a running application to a different queue. This can be useful for moving an important application to a higher priority queue, or for moving an unimportant application to a lower priority queue. Apps can be moved by running `yarn application -movetoqueue appID -queue targetQueueName`.

When an application is moved to a queue, its existing allocations become counted with the new queue's allocations instead of the old for purposes of determining fairness. An attempt to move an application to a queue will fail if the addition of the app's resources to that queue would violate the its maxRunningApps or maxResources constraints.
