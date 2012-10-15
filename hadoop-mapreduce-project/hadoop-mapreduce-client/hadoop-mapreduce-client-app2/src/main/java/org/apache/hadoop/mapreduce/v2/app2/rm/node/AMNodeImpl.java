/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.mapreduce.v2.app2.rm.node;

import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.v2.app2.AppContext;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerEventType;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;

public class AMNodeImpl implements AMNode {

  private static final Log LOG = LogFactory.getLog(AMNodeImpl.class);

  private final ReadLock readLock;
  private final WriteLock writeLock;
  private final NodeId nodeId;
  private final AppContext appContext;
  private final int maxTaskFailuresPerNode;
  private int numFailedTAs = 0;
  private int numSuccessfulTAs = 0;

  @SuppressWarnings("rawtypes")
  protected EventHandler eventHandler;

  private final List<ContainerId> containers = new LinkedList<ContainerId>();
  
  //Book-keeping only. In case of Health status change.
  private final List<ContainerId> pastContainers = new LinkedList<ContainerId>();

  private static boolean stateMachineInited = false;
  private static StateMachineFactory
      <AMNodeImpl, AMNodeState, AMNodeEventType, AMNodeEvent> 
      stateMachineFactory = 
      new StateMachineFactory<AMNodeImpl, AMNodeState, AMNodeEventType, AMNodeEvent>(
      AMNodeState.ACTIVE);

  private final StateMachine<AMNodeState, AMNodeEventType, AMNodeEvent> stateMachine;

  private void initStateMachineFactory() {
    stateMachineFactory = 
    stateMachineFactory
        // Transitions from ACTIVE state.
        .addTransition(AMNodeState.ACTIVE, AMNodeState.ACTIVE,
            AMNodeEventType.N_CONTAINER_ALLOCATED,
            createContainerAllocatedTransition())
        .addTransition(AMNodeState.ACTIVE, AMNodeState.ACTIVE,
            AMNodeEventType.N_TA_SUCCEEDED,
            createTaskAttemptSucceededTransition())
        .addTransition(AMNodeState.ACTIVE,
            EnumSet.of(AMNodeState.ACTIVE, AMNodeState.BLACKLISTED),
            AMNodeEventType.N_TA_ENDED,
            createTaskAttemptFailedTransition())
        .addTransition(AMNodeState.ACTIVE, AMNodeState.UNHEALTHY,
            AMNodeEventType.N_TURNED_UNHEALTHY,
            createNodeTurnedUnhealthyTransition())
        .addTransition(AMNodeState.ACTIVE,
            EnumSet.of(AMNodeState.ACTIVE, AMNodeState.BLACKLISTED),
            AMNodeEventType.N_BLACKLISTING_ENABLED,
            createBlacklistingEnabledTransition())
        .addTransition(
            AMNodeState.ACTIVE,
            AMNodeState.ACTIVE,
            EnumSet.of(AMNodeEventType.N_TURNED_HEALTHY,
                AMNodeEventType.N_BLACKLISTING_DISABLED))

        // Transitions from BLACKLISTED state.
        .addTransition(AMNodeState.BLACKLISTED, AMNodeState.BLACKLISTED,
            AMNodeEventType.N_CONTAINER_ALLOCATED,
            createContainerAllocatedWhileBlacklistedTransition())
        .addTransition(AMNodeState.BLACKLISTED,
            EnumSet.of(AMNodeState.BLACKLISTED, AMNodeState.ACTIVE),
            AMNodeEventType.N_TA_SUCCEEDED,
            createTaskAttemptSucceededWhileBlacklistedTransition())
        .addTransition(AMNodeState.BLACKLISTED, AMNodeState.BLACKLISTED,
            AMNodeEventType.N_TA_ENDED,
            createTaskAttemptFailedWhileBlacklistedTransition())
        .addTransition(AMNodeState.BLACKLISTED, AMNodeState.UNHEALTHY,
            AMNodeEventType.N_TURNED_UNHEALTHY,
            createNodeTurnedUnhealthyTransition())
        .addTransition(AMNodeState.BLACKLISTED, AMNodeState.FORCED_ACTIVE, AMNodeEventType.N_BLACKLISTING_DISABLED)
        .addTransition(AMNodeState.BLACKLISTED, AMNodeState.BLACKLISTED, EnumSet.of(AMNodeEventType.N_TURNED_HEALTHY, AMNodeEventType.N_BLACKLISTING_ENABLED), createGenericErrorTransition())

        //Transitions from FORCED_ACTIVE state.
        .addTransition(AMNodeState.FORCED_ACTIVE, AMNodeState.FORCED_ACTIVE,
            AMNodeEventType.N_CONTAINER_ALLOCATED,
            createContainerAllocatedTransition())
        .addTransition(AMNodeState.FORCED_ACTIVE, AMNodeState.FORCED_ACTIVE,
            AMNodeEventType.N_TA_SUCCEEDED,
            createTaskAttemptSucceededTransition())
        .addTransition(AMNodeState.FORCED_ACTIVE, AMNodeState.FORCED_ACTIVE,
            AMNodeEventType.N_TA_ENDED,
            createTaskAttemptFailedWhileBlacklistedTransition())
        .addTransition(AMNodeState.FORCED_ACTIVE, AMNodeState.UNHEALTHY,
            AMNodeEventType.N_TURNED_UNHEALTHY,
            createNodeTurnedUnhealthyTransition())
        // For now, always to blacklisted - since there's no way to unblacklist
        // a node.
        .addTransition(AMNodeState.FORCED_ACTIVE, EnumSet.of(AMNodeState.BLACKLISTED, AMNodeState.ACTIVE), AMNodeEventType.N_BLACKLISTING_ENABLED, createBlacklistingEnabledTransition())
        .addTransition(AMNodeState.FORCED_ACTIVE, AMNodeState.FORCED_ACTIVE, EnumSet.of(AMNodeEventType.N_TURNED_HEALTHY, AMNodeEventType.N_BLACKLISTING_DISABLED), createGenericErrorTransition())
            
        // Transitions from UNHEALTHY state.
        .addTransition(AMNodeState.UNHEALTHY, AMNodeState.UNHEALTHY, AMNodeEventType.N_CONTAINER_ALLOCATED, createContainerAllocatedWhileUnhealthyTransition())
        .addTransition(AMNodeState.UNHEALTHY, AMNodeState.UNHEALTHY, EnumSet.of(AMNodeEventType.N_TA_SUCCEEDED, AMNodeEventType.N_TA_ENDED, AMNodeEventType.N_BLACKLISTING_DISABLED, AMNodeEventType.N_BLACKLISTING_ENABLED))
        .addTransition(AMNodeState.UNHEALTHY, AMNodeState.ACTIVE, AMNodeEventType.N_TURNED_HEALTHY, createNodeTurnedHealthyTransition())
        .addTransition(AMNodeState.UNHEALTHY, AMNodeState.UNHEALTHY, AMNodeEventType.N_TURNED_UNHEALTHY, createGenericErrorTransition())

        .installTopology();
  }

  @SuppressWarnings("rawtypes")
  public AMNodeImpl(NodeId nodeId, int maxTaskFailuresPerNode,
      EventHandler eventHandler, AppContext appContext) {
    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    this.readLock = rwLock.readLock();
    this.writeLock = rwLock.writeLock();
    this.nodeId = nodeId;
    this.appContext = appContext;
    this.eventHandler = eventHandler;
    this.maxTaskFailuresPerNode = maxTaskFailuresPerNode;
    // TODO Better way to kill the job. Node really should not care about the
    // JobId.

    synchronized (stateMachineFactory) {
      if (!stateMachineInited) {
        initStateMachineFactory();
        stateMachineInited = true;
      }
    }
    this.stateMachine = stateMachineFactory.make(this);
    // TODO Handle the case where a node is created due to the RM reporting it's
    // state as UNHEALTHY
  }

  @Override
  public NodeId getNodeId() {
    return this.nodeId;
  }

  @Override
  public AMNodeState getState() {
    this.readLock.lock();
    try {
      return this.stateMachine.getCurrentState();
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public List<ContainerId> getContainers() {
    this.readLock.lock();
    try {
      List<ContainerId> cIds = new LinkedList<ContainerId>(this.containers);
      return cIds;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public void handle(AMNodeEvent event) {
    this.writeLock.lock();
    try {
      final AMNodeState oldState = getState();
      try {
        stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle event " + event.getType()
            + " at current state " + oldState + " for NodeId " + this.nodeId, e);
        // TODO Should this fail the job ?
      }
      if (oldState != getState()) {
        LOG.info("AMNode " + this.nodeId + " transitioned from " + oldState
            + " to " + getState());
      }
    } finally {
      writeLock.unlock();
    }
  }
  
  @SuppressWarnings("unchecked")
  private void sendEvent(Event<?> event) {
    this.eventHandler.handle(event);
  }

  protected SingleArcTransition<AMNodeImpl, AMNodeEvent> 
      createContainerAllocatedTransition() {
    return new ContainerAllocated();
  }

  protected static class ContainerAllocated implements
      SingleArcTransition<AMNodeImpl, AMNodeEvent> {
    @Override
    public void transition(AMNodeImpl node, AMNodeEvent nEvent) {
      AMNodeEventContainerAllocated event = (AMNodeEventContainerAllocated) nEvent;
      node.containers.add(event.getContainerId());
    }
  }

  protected SingleArcTransition<AMNodeImpl, AMNodeEvent> 
      createTaskAttemptSucceededTransition() {
    return new TaskAttemptSucceededTransition();
  }
  protected static class TaskAttemptSucceededTransition implements
      SingleArcTransition<AMNodeImpl, AMNodeEvent> {
    @Override
    public void transition(AMNodeImpl node, AMNodeEvent nEvent) {
      node.numSuccessfulTAs++;
    }
  }

  protected MultipleArcTransition<AMNodeImpl, AMNodeEvent, AMNodeState> 
      createTaskAttemptFailedTransition() {
    return new TaskAttemptFailed();
  }
  protected static class TaskAttemptFailed implements
      MultipleArcTransition<AMNodeImpl, AMNodeEvent, AMNodeState> {
    @Override
    public AMNodeState transition(AMNodeImpl node, AMNodeEvent nEvent) {
      AMNodeEventTaskAttemptEnded event = (AMNodeEventTaskAttemptEnded) nEvent;
      if (event.failed()) {
        node.numFailedTAs++;
        boolean shouldBlacklist = node.shouldBlacklistNode();
        if (shouldBlacklist) {
          node.sendEvent(new AMNodeEvent(node.getNodeId(),
              AMNodeEventType.N_NODE_WAS_BLACKLISTED));
          return AMNodeState.BLACKLISTED;
          // TODO XXX: An event likely needs to go out to the scheduler.
          // XXX Someone needs to update the scheduler tables - send a ZEROd request to the scheduler. Who's doing that ?
        }
      }
      return AMNodeState.ACTIVE;
    }
  }

  protected SingleArcTransition<AMNodeImpl, AMNodeEvent> 
      createNodeTurnedUnhealthyTransition() {
    return new NodeTurnedUnhealthy();
  }

  // Forgetting about past errors. Will go back to ACTIVE, not FORCED_ACTIVE
  protected static class NodeTurnedUnhealthy implements
      SingleArcTransition<AMNodeImpl, AMNodeEvent> {
    @Override
    public void transition(AMNodeImpl node, AMNodeEvent nEvent) {
      for (ContainerId c : node.containers) {
        node.sendEvent(new AMContainerEvent(c,
            AMContainerEventType.C_NODE_FAILED));
      }
      // Resetting counters.
      node.numFailedTAs = 0;
      node.numSuccessfulTAs = 0;
    }
  }

  protected SingleArcTransition<AMNodeImpl, AMNodeEvent> createGenericErrorTransition() {
    return new GenericError();
  }

  protected static class GenericError implements
      SingleArcTransition<AMNodeImpl, AMNodeEvent> {
    @Override
    public void transition(AMNodeImpl node, AMNodeEvent nEvent) {
      LOG.warn("Invalid event: " + nEvent.getType() + " while in state: "
          + node.getState() + ". Ignoring." + " Event: " + nEvent);

    }
  }
  
  protected MultipleArcTransition<AMNodeImpl, AMNodeEvent, AMNodeState>
      createBlacklistingEnabledTransition() {
    return new BlacklistingEnabled();
  }

  protected static class BlacklistingEnabled implements
      MultipleArcTransition<AMNodeImpl, AMNodeEvent, AMNodeState> {

    @Override
    public AMNodeState transition(AMNodeImpl node, AMNodeEvent nEvent) {
      boolean shouldBlacklist = node.shouldBlacklistNode();
      if (shouldBlacklist) {
        node.sendEvent(new AMNodeEvent(node.getNodeId(),
            AMNodeEventType.N_NODE_WAS_BLACKLISTED));
        return AMNodeState.BLACKLISTED;
        // TODO XXX: An event likely needs to go out to the scheduler.
      }
      return AMNodeState.ACTIVE;
    }
  }

  protected boolean shouldBlacklistNode() {
    return (numFailedTAs >= maxTaskFailuresPerNode);
  }

  protected SingleArcTransition<AMNodeImpl, AMNodeEvent> 
      createContainerAllocatedWhileBlacklistedTransition() {
    return new ContainerAllocatedWhileBlacklisted();
  }

  protected static class ContainerAllocatedWhileBlacklisted implements
      SingleArcTransition<AMNodeImpl, AMNodeEvent> {
    @Override
    public void transition(AMNodeImpl node, AMNodeEvent nEvent) {
      AMNodeEventContainerAllocated event = (AMNodeEventContainerAllocated) nEvent;
      // Maybe send out a StopContainer message to the container.
      node.sendEvent(new AMContainerEvent(event.getContainerId(),
          AMContainerEventType.C_STOP_REQUEST));
    }
  }

  protected MultipleArcTransition<AMNodeImpl, AMNodeEvent, AMNodeState> createTaskAttemptSucceededWhileBlacklistedTransition() {
    return new TaskAttemptSucceededWhileBlacklisted();
  }
  protected static class TaskAttemptSucceededWhileBlacklisted implements
      MultipleArcTransition<AMNodeImpl, AMNodeEvent, AMNodeState> {
    @Override
    public AMNodeState transition(AMNodeImpl node, AMNodeEvent nEvent) {
      node.numSuccessfulTAs++;
      return AMNodeState.BLACKLISTED;
      // For now, always blacklisted. May change it over to re-enable the node.
    }
  }

  protected SingleArcTransition<AMNodeImpl, AMNodeEvent> createTaskAttemptFailedWhileBlacklistedTransition() {
    return new TaskAttemptFailedWhileBlacklisted();
  }
  protected static class TaskAttemptFailedWhileBlacklisted implements
      SingleArcTransition<AMNodeImpl, AMNodeEvent> {
    @Override
    public void transition(AMNodeImpl node, AMNodeEvent nEvent) {
      AMNodeEventTaskAttemptEnded event = (AMNodeEventTaskAttemptEnded) nEvent;
      if (event.failed())
        node.numFailedTAs++;
    }
  }


  protected SingleArcTransition<AMNodeImpl, AMNodeEvent> createContainerAllocatedWhileUnhealthyTransition() {
    return new ContainerAllocatedWhileUnhealthy();
  }
  protected static class ContainerAllocatedWhileUnhealthy implements
      SingleArcTransition<AMNodeImpl, AMNodeEvent> {
    @Override
    public void transition(AMNodeImpl node, AMNodeEvent nEvent) {
      AMNodeEventContainerAllocated event = (AMNodeEventContainerAllocated) nEvent;
      LOG.info("Node: " + node.getNodeId()
          + " got allocated a contaienr with id: " + event.getContainerId()
          + " while in UNHEALTHY state. Releasing it.");
      // TODO XXX: Maybe consider including some diagnostics with this event. (RM reported NODE as unhealthy maybe). Which would then be included in diagnostics from the Container.
      node.sendEvent(new AMContainerEvent(event.getContainerId(),
          AMContainerEventType.C_NODE_FAILED));
    }
  }

  protected SingleArcTransition<AMNodeImpl, AMNodeEvent> createNodeTurnedHealthyTransition() {
    return new NodeTurnedHealthyTransition();
  }
  protected static class NodeTurnedHealthyTransition implements
      SingleArcTransition<AMNodeImpl, AMNodeEvent> {
    @Override
    public void transition(AMNodeImpl node, AMNodeEvent nEvent) {
      node.pastContainers.addAll(node.containers);
      node.containers.clear();
    }
  }

  @Override
  public boolean isUnhealthy() {
    this.readLock.lock();
    try {
      return getState() == AMNodeState.UNHEALTHY;
    } finally {
      this.readLock.unlock();
    }
  }

  public boolean isBlacklisted() {
    this.readLock.lock();
    try {
      return getState() == AMNodeState.BLACKLISTED;
    } finally {
      this.readLock.unlock();
    }
  }
}
