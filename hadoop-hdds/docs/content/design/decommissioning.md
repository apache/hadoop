<!--
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

---
title: Decommissioning in Ozone
summary: Formal process to shut down machines in a safe way after the required replications.
date: 2019-07-31
jira: HDDS-1881
status: current
author: Anu Engineer, Marton Elek, Stephen O'Donnell
---

# Abstract

The goal of decommissioning is to turn off a selected set of machines without data loss. It may or may not require to move the existing replicas of the containers to other nodes.

There are two main classes of the decommissioning:

 * __Maintenance mode__: where the node is expected to be back after a while. It may not require replication of containers if enough replicas are available from other nodes (as we expect to have the current replicas after the restart.)

 * __Decommissioning__: where the node won't be started again. All the data should be replicated according to the current replication rules.

Goals:

 * Decommissioning can be canceled any time
 * The progress of the decommissioning should be trackable
 * The nodes under decommissioning / maintenance mode should not been used for new pipelines / containers
 * The state of the datanodes should be persisted / replicated by the SCM (in HDFS the decommissioning info exclude/include lists are replicated manually by the admin). If datanode is marked for decommissioning this state be available after SCM and/or Datanode restarts.
 * We need to support validations before decommissioing (but the violations can be ignored by the admin).
 * The administrator should be notified when a node can be turned off.
 * The maintenance mode can be time constrained: if the node marked for maintenance for 1 week and the node is not up after one week, the containers should be considered as lost (DEAD node) and should be replicated.

# Introduction

Ozone is a highly available file system that relies on commodity hardware. In other words, Ozone is designed to handle failures of these nodes all the time.

The Storage Container Manager(SCM) is designed to monitor the node health and replicate blocks and containers as needed.

At times, Operators of the cluster can help the SCM by giving it hints. When removing a datanode, the operator can provide a hint. That is, a planned failure of the node is coming up, and SCM can make sure it reaches a safe state to handle this planned failure.

Some times, this failure is transient; that is, the operator is taking down this node temporarily. In that case, we can live with lower replica counts by being optimistic.

Both of these operations, __Maintenance__, and __Decommissioning__ are similar from the Replication point of view. In both cases, and the user instructs us on how to handle an upcoming failure.

Today, SCM (*Replication Manager* component inside SCM) understands only one form of failure handling. This paper extends Replica Manager failure modes to allow users to request which failure handling model to be adopted(Optimistic or Pessimistic).

Based on physical realities, there are two responses to any perceived failure, to heal the system by taking corrective actions or ignore the failure since the actions in the future will heal the system automatically.

## User Experiences (Decommissioning vs Maintenance mode)

From the user's point of view, there are two kinds of planned failures that the user would like to communicate to Ozone.

The first kind is when a 'real' failure is going to happen in the future. This 'real' failure is the act of decommissioning. We denote this as "decommission" throughout this paper. The response that the user wants is SCM/Ozone to make replicas to deal with the planned failure.

The second kind is when the failure is 'transient.' The user knows that this failure is temporary and cluster in most cases can safely ignore this issue. However, if the transient failures are going to cause a failure of availability; then the user would like the Ozone to take appropriate actions to address it.  An example of this case, is if the user put 3 data nodes into maintenance mode and switched them off.

The transient failure can violate the availability guarantees of Ozone; Since the user is telling us not to take corrective actions. Many times, the user does not understand the impact on availability while asking Ozone to ignore the failure.

So this paper proposes the following definitions for Decommission and Maintenance of data nodes.

__Decommission__ *of a data node is deemed to be complete when SCM/Ozone completes the replica of all containers on decommissioned data node to other data nodes.That is, the expected count matches the healthy count of containers in the cluster*.

__Maintenance mode__ of a data node is complete if Ozone can guarantee at *least one copy of every container* is available in other healthy data nodes.

## Examples

Here are some illustrative examples:

1.  Let us say we have a container, which has only one copy and resides on Machine A. If the user wants to put machine A into maintenance mode; Ozone will make a replica before entering the maintenance mode.

2. Suppose a container has two copies, and the user wants to put Machine A to maintenance mode. In this case; the Ozone understands that availability of the container is not affected and hence can decide to forgo replication.

3. Suppose a container has two copies, and the user wants to put Machine A into maintenance mode. However, the user wants to put the machine into maintenance mode for one month. As the period of maintenance mode increases, the probability of data loss increases; hence, Ozone might choose to make a replica of the container even if we are entering maintenance mode.

4. The semantics of decommissioning means that as long as we can find copies of containers in other machines, we can technically get away with calling decommission complete. Hence this clarification node; in the ordinary course of action; each decommission will create a replication flow for each container we have; however, it is possible to complete a decommission of a data node, even if we get a failure of the  data node being decommissioned. As long as we can find the other datanodes to replicate from and get the number of replicas needed backup to expected count we are good.

5. Let us say we have a copy of a container replica on Machine A, B, and C. It is possible to decommission all three machines at the same time, as decommissioning is just a status indicator of the data node and until we finish the decommissioning process.


The user-visible features for both of these  are very similar:

Both Decommission and Maintenance mode can be canceled any time before the operation is marked as completed by SCM.

Decommissioned nodes, if and when added back, shall be treated as new data nodes; if they have blocks or containers on them, they can be used to reconstruct data.


## Maintenance mode in HDFS

HDFS supports decommissioning and maintenance mode similar to Ozone. This is a quick description of the HDFS approach.

The usage of HDFS maintenance mode:

  * First, you set a minimum replica count on the cluster, which can be zero, but defaults to 1.
  * Then you can set a number of nodes into maintenance, with an expiry time or have them remain in maintenance forever, until they are manually removed. Nodes are put into maintenance in much the same way as nodes are decommissioned.
 * When a set of nodes go into maintenance, all blocks hosted on them are scanned and if the node going into maintenance would cause the number of replicas to fall below the minimum replica count, the relevant nodes go into a decommissioning like state while new replicas are made for the blocks.
  * Once the node goes into maintenance, it can be stopped etc and HDFS will not be concerned about the under-replicated state of the blocks.
  * When the expiry time passes, the node is put back to normal state (if it is online and heartbeating) or marked as dead, at which time new replicas will start to be made.

This is very similar to decommissioning, and the code to track maintenance mode and ensure the blocks are replicated etc, is effectively the same code as with decommissioning. The one area that differs is probably in the replication monitor as it must understand that the node is expected to be offline.

The ideal way to use maintenance mode, is when you know there are a set of nodes you can stop without having to do any replications. In HDFS, the rack awareness states that all blocks should be on two racks, so that means a rack can be put into maintenance safely.

There is another feature in HDFS called "upgrade Domain" which allows each datanode to be assigned a group. By default there should be at least 3 groups (domains) and then each of the 3 replicas will be stored on different group, allowing one full group to be put into maintenance at once. That is not yet supported in CDH, but is something we are targeting for CDPD I believe.

One other difference with maintenance mode and decommissioning, is that you must have some sort of monitor thread checking for when maintenance is scheduled to end. HDFS solves this by having a class called the DatanodeAdminManager, and it tracks all nodes transitioning state, the under-replicated block count on them etc.


# Implementation


## Datanode state machine

`NodeStateManager` maintains the state of the connected datanodes. The possible states:

  state                | description
  ---------------------|------------
  HEALTHY              | The node is up and running.
  STALE                | Some heartbeats were missing for an already missing nodes.
  DEAD                 | The stale node has not been recovered.
  ENTERING_MAINTENANCE | The in-progress state, scheduling is disabled but the node can't not been turned off due to in-progress replication.
  IN_MAINTENANCE       | Node can be turned off but we expecteed to get it back and have all the replicas.
  DECOMMISSIONING      | The in-progress state, scheduling is disabled, all the containers should be replicated to other nodes.
  DECOMMISSIONED       | The node can be turned off, all the containers are replicated to other machine



## High level algorithm

The Algorithm is pretty simple from the Decommission or Maintenance point of view;

 1. Mark a data node as DECOMMISSIONING or ENTERING_MAINTENANCE. This implies that node is NOT healthy anymore; we assume the use of a single flag and law of excluded middle.

 2. Pipelines should be shut down and wait for confirmation that all pipelines are shutdown. So no new I/O or container creation can happen on a Datanode that is part of decomm/maint.

 3. Once the Node has been marked as DECOMMISSIONING or ENTERING_MAINTENANCE; the Node will generate a list of containers that need replication. This list is generated by the Replica Count decisions for each container; the Replica Count will be computed by Replica Manager;

 4. Replica Manager will check the stop condition for each node. The following should be true for all the containers to go from DECOMMISSIONING to DECOMMISSIONED or from ENTERING_MAINTENANCE to IN_MAINTENANCE.

   * Container is closed.
   * We have at least one HEALTHY copy at all times.
   * For entering DECOMMISSIONED mode `maintenance + healthy` must equal to `expectedeCount`

 5. We will update the node state to DECOMMISSIONED or IN_MAINTENANCE reached state.

_Replica count_ is a calculated number which represents the number of _missing_ replicas. The number can be negative in case of an over-replicated container.

## Calculation of the _Replica count_ (required replicas)

### Counters / Variables

We have 7 different datanode state, but some of them can be combined. At high level we can group the existing state to three groups:

 * healthy state (when the container is available)
 * maintenance (including IN_MAINTENANCE and ENTERING_MAINTENANCE)
 * all the others.

To calculate the required steps (required replication + stop condition) we need counter about the first two categories.

Each counters should be calculated per container bases.

   Node state                            | Variable (# of containers)      |
   --------------------------------------|---------------------------------|
   HEALTHY	                             | `healthy`                       |
   STALE + DEAD + DECOMMISSIONED	     |                                 |
   DECOMMISSIONING                       |                                 |
   ENTERING_MAINTENANCE + IN_MAINTENANCE | `maintenance`                   |


### The current replication model

The current replication model in SCM/Ozone is very simplistic. We compute the replication count or the number of replications that we need to do as:

```
Replica count = expectedCount - currentCount
```

In case the _Replica count_ is positive, it means that we need to make more replicas. If the number is negative, it means that we are over replicated and we need to remove some replicas of this container. If the Replica count for a container is zero; it means that we have the expected number of containers in the cluster.

To support idempontent placement strategies we should substract the in-fligt replications from the result: If there are one in-flight replication process and two replicas we won't start a new replication command unless the original command is timed out. The timeout is configured with `hdds.scm.replication.event.timeout` and the default value is 10 minutes.

More preciously the current algorithm is the following:

```java
replicaCount = expectedCount - healthy;

if (replicaCount - inflight copies + inflight deletes) > 0 {
   // container is over replicated
}.else if (replicaCount - inflight copies + inflight deletes) <0 {
  // handle under replicated containers
}
```

The handling of inflight copies and deletes are independent from the decommissioning problem therefore here we focus only on the core mode:

```
replicaCount = expectedCount - healthy;
```

### The proposed solution

To support the notion that a user can provide hints to the replication model, we propose to add two variables to the current model.

In the new model, we propose to break the `currentCount` into the two separate groups. That is _Healthy nodes_ and _Maintenance nodes_. The new model replaces the currentCount with these two separate counts. The following function captures the code that drives the logic of computing Replica counts in the new model. The later section discusses the input and output of this model very extensively.

```java
/**
 * Calculate the number of the missing replicas.
 *
 * @return the number of the missing replicas.
     If it's less than zero, the container is over replicated.
 */
int getReplicationCount(int expectedCount, int healthy, int maintenance) {

   //for over replication, count only with the healthy replicas
   if (expectedCount <= healthy) {
      return expectedCount - healthy;
   }

   replicaCount = expectedCount - (healthy + maintenance);

   //at least one HEALTHY replicas should be guaranteed!
   if (replicaCount == 0 && healthy < 1) {
      replicaCount ++;
   }

   //over replication is already handled. Always return with
   // positive as over replication is already handled
   return Math.max(0, replicaCount);
}

```

To understand the reasing behind the two special `if` condition check the examples below.

We also need to specify two end condition when the DECOMMISSIONING node can be moved to the DECOMMISSIONED state or the ENTERING_MAINTENANCE mode can be moved to the IN_MAINTENANCE state.

The following conditions should be true for all the containers and all the containers on the specific node should be closed.

**From DECOMMISSIONING to DECOMMISSIONED**:

 * There are at least one healthy replica
 * We have three replicas (both helthy and maintenance)

Which means that our stop condition can be formalized as:

```
(healthy >= 1) && (healthy + maintenance >= 3)
```

Both the numbers can be configurable:

  * 1 is the minimum number of healthy replicas (`decommissioning.minimum.healthy-replicas`)
  * 3 is the minimum number of existing replicas (`decommissioning.minimum.replicas`)

For example `decommissioning.minimum.healthy-replicas` can be set to two if administrator would like to survive an additional node failure during the maintenance period.

**From ENTERING_MAINTENANCE to IN_MAINTENANCE:**

 * There are at least one healthy replicas

This is the weaker version of the previous condition:

```
(healthy >= 1)
```

### Examples (normal cases)

In this section we show example use cases together with the output of the proposed algorithm

#### All healthy

  Node A  | Node B  | Node C | expctd | healthy | mainten | repCount
  --------|---------|--------|--------|---------|---------|----------
  HEALTHY | HEALTHY | HEALTHY|  3     | 3       | 0       | 0

The container C1 exists on machines A, B , and C. All the container reports tell us that the container is healthy.  Running the above algorithm, we get:

`expected - healthy + maint. = 3 - (3 + 0) = 0`

It means, _"we don’t need no replication"._

#### One failure


  Node A  | Node B  | Node C | expctd | healthy | mainten | repCount
  --------|---------|--------|--------|---------|---------|----------
  HEALTHY | HEALTHY | DEAD   |  3     | 2       | 0       | 1


The machine C has failed, and as a result, the healthy count has gone down from `3` to `2`. This means that we need to start one replication flow.

`ReplicaCount = expected - healthy + maint. =  3 - (2 + 0) = 1.`

This means that the new model will handle failure cases just like the current model.

#### One decommissioning

  Node A  | Node B  | Node C | expctd | healthy | mainten | repCount
  --------|---------|--------|--------|---------|---------|----------
  HEALTHY | HEALTHY | DECOMM |  3     | 2       | 0       | 1


In this case, machine C is being decommissioned. Therefore the healthy count has gone down to `2` , and decommission count is `1`. Since the

```ReplicaCount = expected - healthy + maint`. we have `1 = 3 - (2 + 0)```,

this gives us the decommission count implicitly. The trick here is to realize that incrementing decommission automatically causes a decrement in the healthy count, which allows us not to have _decommission_ in the equation explicitly.

**Stop condition**: Not that if this containers is the only one on node C, node C can be moved to the DECOMMISSIONED state.

#### Failure + decommissioning

  Node A  | Node B  | Node C | expctd | healthy | mainten | repCount
  --------|---------|--------|--------|---------|---------|----------
  HEALTHY | DEAD    | DECOMM |  3     | 1       | 0       | 2


Here is a case where we have a failure of a data node and a decommission of another data node. In this case, the container C1 needs two replica flows to heal itself. The equation is the same and we get

`ReplicaCount(2) = ExpectecCount(3) - healthy(1)`

The maintenance is still zero so ignored in this equation.

#### 1 failure + 2 decommissioning

  Node A  | Node B  | Node C | expctd | healthy | mainten | repCount
  --------|---------|--------|--------|---------|---------|----------
  HEALTHY | DECOMM  | DECOMM |  3     | 0       | 0       | 3

In this case, we have one failed data node and two data nodes being decommissioned. We need to get three replica flows in the system. This is achieved by:

```
ReplicaCount(3) = ExpectedCount(3) - (healthy(0) + maintenance(0))
```

#### Maintenance mode

  Node A  | Node B  | Node C | expctd | healthy | mainten | repCount
  --------|---------|--------|--------|---------|---------|----------
  HEALTHY | DEAD    | MAINT  |  3     | 2       | 1       | 0

This represents the normal maintenance mode, where a single machine is marked as in maintenance mode. This means the following:

```
ReplicaCount(0) = ExpectedCount(3) - (healthy(2) + maintenance(1)
```

There are no replica flows since the user has asked us to move a single node into maintenance mode, and asked us explicitly not to worry about the single missing node.

**Stop condition**: Not that if this containers is the only one on node C, node C can be moved to the IN_MAINTENANCE state.

#### Maintenance + decommissioning

  Node A  | Node B  | Node C | expctd | healthy | mainten | repCount
  --------|---------|--------|--------|---------|---------|----------
  HEALTHY | DECOMM  | MAINT  |  3     | 1       | 1       | 1

*This is a fascinating case*; We have one good node; one decommissioned node and one node in maintenance mode. The expected result is that the replica manager will launch one replication flow to compensate for the node that is being decommissioned, and we also expect that there will be no replication for the node in maintenance mode.

```
Replica Count (1) = expectedCount(3) - (healthy(1) + maintenance(1))
```
So as expected we have one replication flow in the system.

**Stop condition**: Not that if this containers is the only one in the system:

 * node C can be moved to the IN_MAINTENANCE state
 * node B can not be decommissioned (we need the three replicas first)

#### Decommissioning all the replicas

  Node A  | Node B  | Node C | expctd | healthy | mainten | repCount
  --------|---------|--------|--------|---------|---------|----------
  DECOMM  | DECOMM  | DECOMM |  3     | 0       | 0       | 3

In this case, we deal with all the data nodes being decommissioned. The number of healthy replicas for this container is 0, and hence:

```
replicaCount (3) = expectedCount (3)- (healthy(0) + maintenance(0)).
```

This provides us with all 3 independent replica flows in the system.

#### Decommissioning the one remaining replicas

  Node A  | Node B  | Node C | expctd | healthy | mainten | repCount
  --------|---------|--------|--------|---------|---------|----------
  DEAD    | DEAD    | DECOMM |  3     | 0       | 0       | 3

We have two failed nodes and one node in Decomm. It is the opposite of case Line 5, where we have one failed node and 2 nodes in Decomm. The expected results are the same, we get 3 flows.

#### Total failure

  Node A  | Node B  | Node C | expctd | healthy | mainten | repCount
  --------|---------|--------|--------|---------|---------|----------
  DEAD    | DEAD    | DEAD   |  3     | 0       | 0       | 3

This is really an error condition. We have lost all 3 data nodes. The Replica Manager will compute that we need to rebuild 3 replicas, but we might not have a source to rebuild from.

### Last replica is on ENTERING_MAINTENANCE

  Node A  | Node B  | Node C | expctd | healthy | mainten | repCount
  --------|---------|--------|--------|---------|---------|----------
  DEAD    | MAINT   | DEAD   |  3     | 0       | 1       | 2

Is also an interesting case; we have lost 2 data nodes; and one node is being marked as Maint. Since we have 2 failed nodes, we need 2 replica flows in the system. However, the maintenance mode cannot be entered, since we will lose lone replica if we do that.

### All maintenance

  Node A  | Node B  | Node C | expctd | healthy | mainten | repCount
  --------|---------|--------|--------|---------|---------|----------
  MAINT   | MAINT   | MAINT  |  3     | 0       | 3       | *1*

This is also a very special case; this is the case where the user is telling us to ignore the peril for all 3 replicas being offline. This means that the system will not be able to get to that container and would lead to potential I/O errors. Ozone will strive to avoid that case; this means that Ozone will hit the “if condition” and discover that we our ReplicCount is 0; since the user asked for it; but we are also going to lose all Replicas. At this point of time, we make a conscious decision to replicate one copy instead of obeying the user command and get to the situation where I/O can fail.

**This brings us back to the semantics of Maintenance mode in Ozone**. If going into maintenance mode will not lead to a potential I/O failure, we will enter into the maintenance mode; Otherwise, we will replicate and enter into the maintenance mode after the replication is done. This is just the core replication algorithm, not the complete Decommission or Maintenance mode algorithms, just how the replica manager would behave.  Once we define the behavior of Replica Manager, rest of the algorithm is easy to construct.

Note: this is the case why we need the seconf `if` in the model (numbers in the brackets shows the actual value):

```
   replicaCount(0) = expectedCount(3) - ( healthy(0) + maintenance(0) );


   //at least one HEALTHY replicas should be guaranteed!
   if (replicaCount(0) == 0 && healthy(0) < 1) {
      replicaCount ++;
   }
```

### Over replication

For over-replicated containers Ozone prefers to keep the replicas on the healthy nodes. We delete containers only if we have enough replicas on *healthy* nodes.

```
int getReplicationCount(int expectedCount, int healthy, int maintenance) {

   //for over replication, count only with the healthy replicas
   if (expectedCount <= healthy) {
      return expectedCount - healthy;
   }

   replicaCount = ... //calculate missing replicas

   //over replication is already handled. Always return with
   // positive as over replication is already handled
   return Math.max(0, replicaCount);
}
```

Please note that we always assume that the the in-flight deletion are applied and the container is already deleted.

There is a very rare case where the in-flight deletion is timed out (and as a result replication manager would assume the container is not deleted) BUT in the mean-time the container finally deleted. It can be survivied with including the creation timestamp in the ContainerDeleteCommand.

### Over replication examples

#### 4 replicas


  Node A  | Node B  | Node C  | Node D  | expctd | healthy | mainten | repCount
  --------|---------|---------|---------|--------|---------|---------|----------
  HEALTHY | HEALTHY | HEALTHY | HEALTHY |  3     | 4       | 0       | 0

This is an easy case as we have too many replicas we can safely remove on.

```
if (expectedCount <= healthy) {
   return expectedCount -  healthy
}
```

#### over replicated with IN_MAINTENANCE


  Node A  | Node B  | Node C  | Node D  | expctd | healthy | mainten | repCount
  --------|---------|---------|---------|--------|---------|---------|----------
  HEALTHY | HEALTHY | HEALTHY | MAINT   |  3     | 3       | 1       | 0


In this case we will delete the forth replica only after node D is restored and healthy again. (expectedCount is not less than healthy). As the `expectedCount (3) <= healthy (3)` the replicaCount will be calculated as `0`.

#### over replicated with IN_MAINTENANCE


  Node A  | Node B  | Node C  | Node D  | expctd | healthy | mainten | repCount
  --------|---------|---------|---------|--------|---------|---------|----------
  HEALTHY | HEALTHY | MAINT   | MAINT   |  3     | 2       | 2       | 0

Here we are not over-repliacated as we don't have any healthy nodes. We will calculate the under-replication number as defined in the previous section:

```
replicaCount(-1) = expectedCount(3) - ( healthy(2) + maintenance(2) );
```

The main algorithm would return with `replicaCount = -1` but as we return `Math.max(0,replicaCount)` the real response will be 0. Waiting for healthy nodes.

### Handling in-flight replications

In-flight replication requests and deletes are handled by the Replica Manager and the problem is orthogonal to the replication problem, but this section shows that the proposed model is not conflicted with the existing approach.

Let's say we have an under-replicated container and we already selected a new datanode to copy a new replica to that specific node.


  Node A  | Node B  | Node C  | expctd | healthy | mainten | repCount
  --------|---------|---------|--------|---------|---------|----------
  HEALTHY | HEALTHY | (copy)  |  3     | 2       | 0       | 1


Here the Replication Manager detects that one replica is missing but the real copy shouldn't been requested as it's alread inprogress. ReplicaManager must not select a new datanode based on the ContainerPlacementPolicy implementation as the policy may or may not be idempotent.

For example if the placement policy would select a datanode randomly with each loop we would select a new datanode to replicate to.

To avoid such a situation Replica Manager maintains a list of the in-flight copies (in-memory) on the SCM side. In this list we have all the sent replication requests but they are removed after a given amount of time (10 minutes) by default.

With calculating the in-flight copy as a possible replication the Replication Manger doesn't need to request new replication.

When a datanode is marked to be decommissioned there could be any in-flight replication copy process in that time.

 * At datanode we should stop all of the in-flight copy (datanodes should be notified about the DECOMMISSIONING/IN_MAINTENANCE state)
 * We never ask any non-healthy nodes to replicate containers.
 * In SCM, we don't need to do any special action
     * In `ReplicationManager` we already have a map about the inflight replications (`Map<ContainerID, List<InflightAction>>`).
     * During a normal replication the number of in-flight replications are counted as real replication (2 real replicas + 1 inflight replication = replica count 3). During this calculation we need to check the current state of the datanodes and ignore the inflight replication if they are assigned to a node which is in decommissioning state. (Or we should update the inflight map, in case of node state change)

### In-flight examples

#### Maintenance + inflight

  Node A  | Node B  | Node C  | expctd | healthy | mainten | repCount | copy |
  --------|---------|---------|--------|---------|---------|----------|------|
  HEALTHY | MAINT   | copying |  3     | 1       | 1       | 1        | 1    |

Here one have one node ENTERING_MAINTENANCE state, and one replica is missing and already started to be replicated. We don't need to start a new copy and node B can be moved to the IN_MAINTENANCE mode.

```
Replica Count (1) = expectedCount(3) - (healthy(1) + maintenance(1))
Containers to copy (0) = Replica Count (1) - inflight copies (1)
```

#### Maintenance + inflight

  Node A  | Node B  | Node C  | expctd | healthy | mainten | repCount | copy |
  --------|---------|---------|--------|---------|---------|----------|------|
  DECOMM  | copying | copying |  3     | 0       | 0       | 3        | 1    |


Node A can not be DECOMMISSIONED as we have no HEALTHY replicas at all.


## Statefulness

SCM stores all the node state in-memory. After a restart on the SCM side the datanode state can be lost.

**Ozone doesn't guarantee that decommissioning/maintenance mode state survives the SCM restarts!!!**

 * If SCM restarts DECOMMISSIONED nodes will not report any more container reports and the nodes won't be registered.
 * ENTERING_MAINTENANCE and DECOMMISSIONING nodes will became HEALTHY again and the decommissioning CLI command should be repeated.
 *  IN_MAINTENANCE nodes will become DEAD and all the containers will be replicated.

 *Ozone assumes that the maintenance mode is used short-term and SCM is not restarted during this specific period.*

*Reasoning*:

Neither of the node state nor the container state are persisted in SCM side. The decommissioned state can be stored on the SCM side (or on the SCM side and the datanode side) which can provide better user experience (and may be implemented).

But to support maintenance mode after restart all the container information is required to be persisted (which is a too big architectural change).

To make a replication decision replication manager needs the number of healthy replicas (they are reported via heartbeats) AND the number of containers on the node which is in maintenance mode. The later one is not available if the SCM is restarted as the container map exists only in the memory and the node which is turned off can't report any more container reports. Therefore the information about the existing containers on the node which is in the maintenance mode **can't be available'**.

## Throttling

SCM should avoid to request too many replication to live enough network bandwidth for the requests.

Replication Manager can easily throttle the replication requests based on `inflightReplication` map, but this problem is independent from the handling of the decommissioning / maintenance mode because it should be solved for any kind of replication not just for this.

## User interface

The decommissioning and maintenance mode can be administered with a CLI interface.

Required feature:

 * Set the state of a datanode (to DECOMMISSIONING or ENTERING_MAINTENANCE)
 * Undo the decommissioning process
 * check the current progress:
   * This can be a table with the nodes, status of the nodes, number of containers, containers under replication and containers which doesn't much the stop condition yet (required replications)
 * All the commands can support topology related filters (eg. display the nodes only for a specific rack or show the status of the nodes of s specific rack)
 * Check current replication information of one specific container (required to debug why the decommissioning is stuck)

## Checks before the decommissioning

Decommissioning is requested via a new RPC call with the help of a new CLI tool. The server should check the current state of the cluster and deny the decommissioning if it's not possible. Possible violations:

 * Not enough space to store the new replicas.
 * Not enough node to create all kind of pipelines

 In case of any violation, the request will fail, but any of theses rules can be turned off with a next request and the decommissioning can be forced.

## Maintain progress

We need to show the progress of the decommissioning process per node and cluster-wide. We already have the information about the under replicated containers, but we don't know the numbers of the containers before decommissioning.

Instead of saving the original number of the required replications before (which is very fragile) we don't provide an absolute progress just the numbers of the remaining replication:


```
 Node            | Status                 | # containers |  in-progress replications| required replication
 ----------------|------------------------|--------------|--------------------------|------------------------
 Node A          | ENTERING_MAINTENANCE   | 2837         | 12                       | 402
 Node B          | HEALTHY                | 1239         | 0                        | 0
 Node C          | IN_MAINTENANCE         | 2348         | 0                        | 0
```

`# containers` means the total number of the containers on the specific datanodes. To get the original number of the planned copies we can save the original 'container-to-node' map in memory and show some progress and provide more information for the users.
