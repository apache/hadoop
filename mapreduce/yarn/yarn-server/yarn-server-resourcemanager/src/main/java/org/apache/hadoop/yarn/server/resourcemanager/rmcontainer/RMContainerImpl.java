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
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerAllocatedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerFinishedSchedulerEvent;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;

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

    // Transitions from COMPLETED state
    .addTransition(RMContainerState.COMPLETED, RMContainerState.COMPLETED,
        EnumSet.of(RMContainerEventType.RELEASED, RMContainerEventType.KILL))

    // Transitions from EXPIRED state
    .addTransition(RMContainerState.EXPIRED, RMContainerState.EXPIRED,
        EnumSet.of(RMContainerEventType.RELEASED, RMContainerEventType.KILL))

    // Transitions from KILLED state
    .addTransition(RMContainerState.KILLED, RMContainerState.KILLED,
        EnumSet.of(RMContainerEventType.RELEASED, RMContainerEventType.KILL))

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

  public RMContainerImpl(ContainerId containerId,
      ApplicationAttemptId appAttemptId, NodeId nodeId, Container container,
      EventHandler handler,
      ContainerAllocationExpirer containerAllocationExpirer) {
    this.stateMachine = stateMachineFactory.make(this);
    this.containerId = containerId;
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
  public void handle(RMContainerEvent event) {
    LOG.info("Processing " + event.getContainerId() + " of type " + event.getType());
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
        LOG.info(nodeId + " Container Transitioned from " + oldState + " to "
                 + getState());
      }
    }
    
    finally {
      writeLock.unlock();
    }
  }

  private static class RMContainerTransition implements
      SingleArcTransition<RMContainerImpl, RMContainerEvent> {

    @Override
    public void transition(RMContainerImpl cont, RMContainerEvent event) {

    }
  }

  private static final class ContainerStartedTransition extends
      RMContainerTransition {

    @Override
    public void transition(RMContainerImpl container, RMContainerEvent event) {
      container.eventHandler.handle(new RMAppAttemptContainerAllocatedEvent(
          container.appAttemptId, container.container));
    }
}

  private static final class AcquiredTransition extends RMContainerTransition {

    @Override
    public void transition(RMContainerImpl container, RMContainerEvent event) {
      // Register with containerAllocationExpirer.
      container.containerAllocationExpirer.register(container.containerId);
    }
  }

  private static final class LaunchedTransition extends RMContainerTransition {

    @Override
    public void transition(RMContainerImpl container, RMContainerEvent event) {
      // Unregister from containerAllocationExpirer.
      container.containerAllocationExpirer.unregister(container.containerId);
    }
  }

  private static class FinishedTransition extends RMContainerTransition {

    @Override
    public void transition(RMContainerImpl container, RMContainerEvent event) {

      RMContainerFinishedEvent finishedEvent = (RMContainerFinishedEvent) event;

      // Update container-status for diagnostics. Today we completely
      // replace it on finish. We may just need to update diagnostics.
      // ^TODO
      container.container.setContainerStatus(finishedEvent
          .getRemoteContainerStatus());

      // Inform AppAttempt
      container.eventHandler.handle(new RMAppAttemptContainerFinishedEvent(
          container.appAttemptId, container.container));

      // Inform Scheduler
      container.eventHandler.handle(new ContainerFinishedSchedulerEvent(
          container.container));
    }
  }

  private static final class ContainerFinishedAtAcquiredState extends
      FinishedTransition {
    @Override
    public void transition(RMContainerImpl container, RMContainerEvent event) {

      // Unregister from containerAllocationExpirer.
      container.containerAllocationExpirer.unregister(container.containerId);

      // Inform AppAttempt, scheduler etc.
      super.transition(container, event);
    }
  }

  private static final class KillTransition extends FinishedTransition {

    @Override
    public void transition(RMContainerImpl container, RMContainerEvent event) {

      // Unregister from containerAllocationExpirer.
      container.containerAllocationExpirer.unregister(container.containerId);

      // Inform node
      container.eventHandler.handle(new RMNodeCleanContainerEvent(
          container.nodeId, container.containerId));

      // Inform appAttempt and scheduler
      super.transition(container, event);
    }
  }

  

}
