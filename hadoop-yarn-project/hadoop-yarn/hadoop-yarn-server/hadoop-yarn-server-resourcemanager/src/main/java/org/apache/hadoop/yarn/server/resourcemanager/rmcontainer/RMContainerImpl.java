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

package org.apache.hadoop.yarn.server.resourcemanager.rmcontainer;

import java.util.EnumSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerAcquiredEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerAllocatedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanContainerEvent;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;

@SuppressWarnings({"unchecked", "rawtypes"})
public class RMContainerImpl implements RMContainer {

  private static final Log LOG = LogFactory.getLog(RMContainerImpl.class);

  private static final StateMachineFactory<RMContainerImpl, RMContainerState, 
                                           RMContainerEventType, RMContainerEvent> 
   stateMachineFactory = new StateMachineFactory<RMContainerImpl, 
       RMContainerState, RMContainerEventType, RMContainerEvent>(
      RMContainerState.NEW)

    // Transitions from NEW state
    .addTransition(RMContainerState.NEW, RMContainerState.ALLOCATED,
        RMContainerEventType.START, new ContainerStartedTransition())
    .addTransition(RMContainerState.NEW, RMContainerState.KILLED,
        RMContainerEventType.KILL)
    .addTransition(RMContainerState.NEW, RMContainerState.RESERVED,
        RMContainerEventType.RESERVED, new ContainerReservedTransition())

    // Transitions from RESERVED state
    .addTransition(RMContainerState.RESERVED, RMContainerState.RESERVED, 
        RMContainerEventType.RESERVED, new ContainerReservedTransition())
    .addTransition(RMContainerState.RESERVED, RMContainerState.ALLOCATED, 
        RMContainerEventType.START, new ContainerStartedTransition())
    .addTransition(RMContainerState.RESERVED, RMContainerState.KILLED, 
        RMContainerEventType.KILL) // nothing to do
    .addTransition(RMContainerState.RESERVED, RMContainerState.RELEASED, 
        RMContainerEventType.RELEASED) // nothing to do
       

    // Transitions from ALLOCATED state
    .addTransition(RMContainerState.ALLOCATED, RMContainerState.ACQUIRED,
        RMContainerEventType.ACQUIRED, new AcquiredTransition())
    .addTransition(RMContainerState.ALLOCATED, RMContainerState.EXPIRED,
        RMContainerEventType.EXPIRE, new FinishedTransition())
    .addTransition(RMContainerState.ALLOCATED, RMContainerState.KILLED,
        RMContainerEventType.KILL, new FinishedTransition())

    // Transitions from ACQUIRED state
    .addTransition(RMContainerState.ACQUIRED, RMContainerState.RUNNING,
        RMContainerEventType.LAUNCHED, new LaunchedTransition())
    .addTransition(RMContainerState.ACQUIRED, RMContainerState.COMPLETED,
        RMContainerEventType.FINISHED, new ContainerFinishedAtAcquiredState())        
    .addTransition(RMContainerState.ACQUIRED, RMContainerState.RELEASED,
        RMContainerEventType.RELEASED, new KillTransition())
    .addTransition(RMContainerState.ACQUIRED, RMContainerState.EXPIRED,
        RMContainerEventType.EXPIRE, new KillTransition())
    .addTransition(RMContainerState.ACQUIRED, RMContainerState.KILLED,
        RMContainerEventType.KILL, new KillTransition())

    // Transitions from RUNNING state
    .addTransition(RMContainerState.RUNNING, RMContainerState.COMPLETED,
        RMContainerEventType.FINISHED, new FinishedTransition())
    .addTransition(RMContainerState.RUNNING, RMContainerState.KILLED,
        RMContainerEventType.KILL, new KillTransition())
    .addTransition(RMContainerState.RUNNING, RMContainerState.RELEASED,
        RMContainerEventType.RELEASED, new KillTransition())
    .addTransition(RMContainerState.RUNNING, RMContainerState.RUNNING,
        RMContainerEventType.EXPIRE)

    // Transitions from COMPLETED state
    .addTransition(RMContainerState.COMPLETED, RMContainerState.COMPLETED,
        EnumSet.of(RMContainerEventType.EXPIRE, RMContainerEventType.RELEASED,
            RMContainerEventType.KILL))

    // Transitions from EXPIRED state
    .addTransition(RMContainerState.EXPIRED, RMContainerState.EXPIRED,
        EnumSet.of(RMContainerEventType.RELEASED, RMContainerEventType.KILL))

    // Transitions from RELEASED state
    .addTransition(RMContainerState.RELEASED, RMContainerState.RELEASED,
        EnumSet.of(RMContainerEventType.EXPIRE, RMContainerEventType.RELEASED,
            RMContainerEventType.KILL, RMContainerEventType.FINISHED))

    // Transitions from KILLED state
    .addTransition(RMContainerState.KILLED, RMContainerState.KILLED,
        EnumSet.of(RMContainerEventType.EXPIRE, RMContainerEventType.RELEASED,
            RMContainerEventType.KILL, RMContainerEventType.FINISHED))

    // create the topology tables
    .installTopology(); 
                        
                        

  private final StateMachine<RMContainerState, RMContainerEventType,
                                                 RMContainerEvent> stateMachine;
  private final ReadLock readLock;
  private final WriteLock writeLock;
  private final ContainerId containerId;
  private final ApplicationAttemptId appAttemptId;
  private final NodeId nodeId;
  private final Container container;
  private final EventHandler eventHandler;
  private final ContainerAllocationExpirer containerAllocationExpirer;

  private Resource reservedResource;
  private NodeId reservedNode;
  private Priority reservedPriority;

  public RMContainerImpl(Container container,
      ApplicationAttemptId appAttemptId, NodeId nodeId,
      EventHandler handler,
      ContainerAllocationExpirer containerAllocationExpirer) {
    this.stateMachine = stateMachineFactory.make(this);
    this.containerId = container.getId();
    this.nodeId = nodeId;
    this.container = container;
    this.appAttemptId = appAttemptId;
    this.eventHandler = handler;
    this.containerAllocationExpirer = containerAllocationExpirer;
    
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();
  }

  @Override
  public ContainerId getContainerId() {
    return this.containerId;
  }

  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    return this.appAttemptId;
  }

  @Override
  public Container getContainer() {
    return this.container;
  }

  @Override
  public RMContainerState getState() {
    this.readLock.lock();

    try {
      return this.stateMachine.getCurrentState();
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public Resource getReservedResource() {
    return reservedResource;
  }

  @Override
  public NodeId getReservedNode() {
    return reservedNode;
  }

  @Override
  public Priority getReservedPriority() {
    return reservedPriority;
  }
  
  @Override
  public String toString() {
    return containerId.toString();
  }
  
  @Override
  public void handle(RMContainerEvent event) {
    LOG.debug("Processing " + event.getContainerId() + " of type " + event.getType());
    try {
      writeLock.lock();
      RMContainerState oldState = getState();
      try {
         stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state", e);
        LOG.error("Invalid event " + event.getType() + 
            " on container " + this.containerId);
      }
      if (oldState != getState()) {
        LOG.info(event.getContainerId() + " Container Transitioned from "
            + oldState + " to " + getState());
      }
    }
    
    finally {
      writeLock.unlock();
    }
  }
  
  private static class BaseTransition implements
      SingleArcTransition<RMContainerImpl, RMContainerEvent> {

    @Override
    public void transition(RMContainerImpl cont, RMContainerEvent event) {

    }
  }

  private static final class ContainerReservedTransition extends
  BaseTransition {

    @Override
    public void transition(RMContainerImpl container, RMContainerEvent event) {
      RMContainerReservedEvent e = (RMContainerReservedEvent)event;
      container.reservedResource = e.getReservedResource();
      container.reservedNode = e.getReservedNode();
      container.reservedPriority = e.getReservedPriority();
    }
  }


  private static final class ContainerStartedTransition extends
      BaseTransition {

    @Override
    public void transition(RMContainerImpl container, RMContainerEvent event) {
      container.eventHandler.handle(new RMAppAttemptContainerAllocatedEvent(
          container.appAttemptId, container.container));
    }
  }

  private static final class AcquiredTransition extends BaseTransition {

    @Override
    public void transition(RMContainerImpl container, RMContainerEvent event) {
      // Register with containerAllocationExpirer.
      container.containerAllocationExpirer.register(container.getContainerId());

      // Tell the appAttempt
      container.eventHandler.handle(new RMAppAttemptContainerAcquiredEvent(
          container.getApplicationAttemptId(), container.getContainer()));
    }
  }

  private static final class LaunchedTransition extends BaseTransition {

    @Override
    public void transition(RMContainerImpl container, RMContainerEvent event) {
      // Unregister from containerAllocationExpirer.
      container.containerAllocationExpirer.unregister(container
          .getContainerId());
    }
  }

  private static class FinishedTransition extends BaseTransition {

    @Override
    public void transition(RMContainerImpl container, RMContainerEvent event) {
      RMContainerFinishedEvent finishedEvent = (RMContainerFinishedEvent) event;

      // Update container-status for diagnostics. Today we completely
      // replace it on finish. We may just need to update diagnostics.
      container.container.setContainerStatus(finishedEvent
          .getRemoteContainerStatus());

      // Inform AppAttempt
      container.eventHandler.handle(new RMAppAttemptContainerFinishedEvent(
          container.appAttemptId, container.container.getContainerStatus()));
    }
  }

  private static final class ContainerFinishedAtAcquiredState extends
      FinishedTransition {
    @Override
    public void transition(RMContainerImpl container, RMContainerEvent event) {

      // Unregister from containerAllocationExpirer.
      container.containerAllocationExpirer.unregister(container
          .getContainerId());

      // Inform AppAttempt
      super.transition(container, event);
    }
  }

  private static final class KillTransition extends FinishedTransition {

    @Override
    public void transition(RMContainerImpl container, RMContainerEvent event) {

      // Unregister from containerAllocationExpirer.
      container.containerAllocationExpirer.unregister(container
          .getContainerId());

      // Inform node
      container.eventHandler.handle(new RMNodeCleanContainerEvent(
          container.nodeId, container.containerId));

      // Inform appAttempt
      super.transition(container, event);
    }
  }

}
