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

package org.apache.hadoop.mapreduce.v2.app2.rm.container;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.WrappedJvmID;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.ContainerHeartbeatHandler;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app2.AppContext;
import org.apache.hadoop.mapreduce.v2.app2.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEventContainerTerminated;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEventContainerTerminating;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEventNodeFailed;
import org.apache.hadoop.mapreduce.v2.app2.rm.AMSchedulerEventContainerCompleted;
import org.apache.hadoop.mapreduce.v2.app2.rm.NMCommunicatorLaunchRequestEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.NMCommunicatorStopRequestEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.RMCommunicatorContainerDeAllocateRequestEvent;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;

@SuppressWarnings("rawtypes")
public class AMContainerImpl implements AMContainer {

  private static final Log LOG = LogFactory.getLog(AMContainerImpl.class);
  
  private final ReadLock readLock;
  private final WriteLock writeLock;
  private final ContainerId containerId;
  // Container to be used for getters on capability, locality etc.
  private final Container container;
  private final AppContext appContext;
  private final ContainerHeartbeatHandler containerHeartbeatHandler;
  private final TaskAttemptListener taskAttemptListener;
  protected final EventHandler eventHandler;

  private final List<TaskAttemptId> completedAttempts = new LinkedList<TaskAttemptId>();

  // TODO Maybe this should be pulled from the TaskAttempt.s
  private final Map<TaskAttemptId, org.apache.hadoop.mapred.Task> remoteTaskMap 
      = new HashMap<TaskAttemptId, org.apache.hadoop.mapred.Task>();
  
  // TODO ?? Convert to list and hash.
  
  private int shufflePort; 
  private long idleTimeBetweenTasks = 0;
  private long lastTaskFinishTime;
  
  
  private TaskAttemptId pendingAttempt;
  private TaskAttemptId runningAttempt;
  private List<TaskAttemptId> failedAssignments;
  private TaskAttemptId pullAttempt;
  
  private boolean inError = false;
  
  private ContainerLaunchContext clc;
  private WrappedJvmID jvmId;
  

  private static boolean stateMachineInited = false;
  private static StateMachineFactory
      <AMContainerImpl, AMContainerState, AMContainerEventType, AMContainerEvent> 
      stateMachineFactory = 
      new StateMachineFactory<AMContainerImpl, AMContainerState, AMContainerEventType, AMContainerEvent>(
      AMContainerState.ALLOCATED);
  
  private final StateMachine<AMContainerState, AMContainerEventType, AMContainerEvent> stateMachine;

  private void initStateMachineFactory() {
    stateMachineFactory = 
    stateMachineFactory
        .addTransition(AMContainerState.ALLOCATED, AMContainerState.LAUNCHING, AMContainerEventType.C_LAUNCH_REQUEST, createLaunchRequestTransition())
        .addTransition(AMContainerState.ALLOCATED, AMContainerState.COMPLETED, AMContainerEventType.C_ASSIGN_TA, createAssignTaskAttemptAtAllocatedTransition())
        .addTransition(AMContainerState.ALLOCATED, AMContainerState.COMPLETED, AMContainerEventType.C_COMPLETED, createCompletedAtAllocatedTransition())
        .addTransition(AMContainerState.ALLOCATED, AMContainerState.COMPLETED, AMContainerEventType.C_STOP_REQUEST, createStopRequestTransition())
        .addTransition(AMContainerState.ALLOCATED, AMContainerState.COMPLETED, AMContainerEventType.C_NODE_FAILED, createNodeFailedAtAllocatedTransition())
        .addTransition(AMContainerState.ALLOCATED, AMContainerState.COMPLETED, EnumSet.of(AMContainerEventType.C_LAUNCHED, AMContainerEventType.C_LAUNCH_FAILED, AMContainerEventType.C_PULL_TA, AMContainerEventType.C_TA_SUCCEEDED, AMContainerEventType.C_NM_STOP_SENT, AMContainerEventType.C_NM_STOP_FAILED, AMContainerEventType.C_TIMED_OUT), createGenericErrorTransition())
        
        
        .addTransition(AMContainerState.LAUNCHING, EnumSet.of(AMContainerState.LAUNCHING, AMContainerState.STOP_REQUESTED), AMContainerEventType.C_ASSIGN_TA, createAssignTaskAttemptTransition())
        .addTransition(AMContainerState.LAUNCHING, AMContainerState.IDLE, AMContainerEventType.C_LAUNCHED, createLaunchedTransition())
        .addTransition(AMContainerState.LAUNCHING, AMContainerState.STOPPING, AMContainerEventType.C_LAUNCH_FAILED, createLaunchFailedTransition())
        .addTransition(AMContainerState.LAUNCHING, AMContainerState.LAUNCHING, AMContainerEventType.C_PULL_TA) // Is assuming the pullAttempt will be null.
        .addTransition(AMContainerState.LAUNCHING, AMContainerState.COMPLETED, AMContainerEventType.C_COMPLETED, createCompletedAtLaunchingTransition())
        .addTransition(AMContainerState.LAUNCHING, AMContainerState.STOP_REQUESTED, AMContainerEventType.C_STOP_REQUEST, createStopRequestAtLaunchingTransition())
        .addTransition(AMContainerState.LAUNCHING, AMContainerState.STOPPING, AMContainerEventType.C_NODE_FAILED, createNodeFailedAtLaunchingTransition())
        .addTransition(AMContainerState.LAUNCHING, AMContainerState.STOP_REQUESTED, EnumSet.of(AMContainerEventType.C_LAUNCH_REQUEST, AMContainerEventType.C_TA_SUCCEEDED, AMContainerEventType.C_NM_STOP_SENT, AMContainerEventType.C_NM_STOP_FAILED, AMContainerEventType.C_TIMED_OUT), createGenericErrorAtLaunchingTransition())

        .addTransition(AMContainerState.IDLE, EnumSet.of(AMContainerState.IDLE, AMContainerState.STOP_REQUESTED), AMContainerEventType.C_ASSIGN_TA, createAssignTaskAttemptAtIdleTransition())
        .addTransition(AMContainerState.IDLE, EnumSet.of(AMContainerState.RUNNING, AMContainerState.IDLE), AMContainerEventType.C_PULL_TA, createPullTAAtIdleTransition())
        .addTransition(AMContainerState.IDLE, AMContainerState.COMPLETED, AMContainerEventType.C_COMPLETED, createCompletedAtIdleTransition())
        .addTransition(AMContainerState.IDLE, AMContainerState.STOP_REQUESTED, AMContainerEventType.C_STOP_REQUEST, createStopRequestAtIdleTransition())
        .addTransition(AMContainerState.IDLE, AMContainerState.STOP_REQUESTED, AMContainerEventType.C_TIMED_OUT, createTimedOutAtIdleTransition())
        .addTransition(AMContainerState.IDLE, AMContainerState.STOPPING, AMContainerEventType.C_NODE_FAILED, createNodeFailedAtIdleTransition())
        .addTransition(AMContainerState.IDLE, AMContainerState.STOP_REQUESTED, EnumSet.of(AMContainerEventType.C_LAUNCH_REQUEST, AMContainerEventType.C_LAUNCHED, AMContainerEventType.C_LAUNCH_FAILED, AMContainerEventType.C_TA_SUCCEEDED, AMContainerEventType.C_NM_STOP_SENT, AMContainerEventType.C_NM_STOP_FAILED), createGenericErrorAtIdleTransition())
        
        .addTransition(AMContainerState.RUNNING, AMContainerState.STOP_REQUESTED, AMContainerEventType.C_ASSIGN_TA, createAssignTaskAttemptAtRunningTransition())
        .addTransition(AMContainerState.RUNNING, AMContainerState.RUNNING, AMContainerEventType.C_PULL_TA)
        .addTransition(AMContainerState.RUNNING, AMContainerState.IDLE, AMContainerEventType.C_TA_SUCCEEDED, createTASucceededAtRunningTransition())
        .addTransition(AMContainerState.RUNNING, AMContainerState.COMPLETED, AMContainerEventType.C_COMPLETED, createCompletedAtRunningTransition())
        .addTransition(AMContainerState.RUNNING, AMContainerState.STOP_REQUESTED, AMContainerEventType.C_STOP_REQUEST, createStopRequestAtRunningTransition())
        .addTransition(AMContainerState.RUNNING, AMContainerState.STOP_REQUESTED, AMContainerEventType.C_TIMED_OUT, createTimedOutAtRunningTransition())
        .addTransition(AMContainerState.RUNNING, AMContainerState.STOPPING, AMContainerEventType.C_NODE_FAILED, createNodeFailedAtRunningTransition())
        .addTransition(AMContainerState.RUNNING, AMContainerState.STOP_REQUESTED, EnumSet.of(AMContainerEventType.C_LAUNCH_REQUEST, AMContainerEventType.C_LAUNCHED, AMContainerEventType.C_LAUNCH_FAILED, AMContainerEventType.C_NM_STOP_SENT, AMContainerEventType.C_NM_STOP_FAILED), createGenericErrorAtRunningTransition())
        
        .addTransition(AMContainerState.STOP_REQUESTED, AMContainerState.STOP_REQUESTED, AMContainerEventType.C_ASSIGN_TA, createAssignTAAtStoppingTransition())
        .addTransition(AMContainerState.STOP_REQUESTED, AMContainerState.COMPLETED, AMContainerEventType.C_COMPLETED, createCompletedAtStoppingTransition())
        .addTransition(AMContainerState.STOP_REQUESTED, AMContainerState.STOPPING, AMContainerEventType.C_NM_STOP_SENT)
        .addTransition(AMContainerState.STOP_REQUESTED, AMContainerState.STOPPING, AMContainerEventType.C_NM_STOP_FAILED, createStopFailedAtNMStopRequested()) // TODO XXX: Rename these.
        .addTransition(AMContainerState.STOP_REQUESTED, AMContainerState.STOPPING, AMContainerEventType.C_NODE_FAILED, createNodeFailedAtNMStopRequestedTransition())
        .addTransition(AMContainerState.STOP_REQUESTED, AMContainerState.STOP_REQUESTED, EnumSet.of(AMContainerEventType.C_LAUNCHED, AMContainerEventType.C_LAUNCH_FAILED, AMContainerEventType.C_PULL_TA, AMContainerEventType.C_TA_SUCCEEDED, AMContainerEventType.C_STOP_REQUEST, AMContainerEventType.C_TIMED_OUT))
        .addTransition(AMContainerState.STOP_REQUESTED, AMContainerState.STOP_REQUESTED, AMContainerEventType.C_LAUNCH_REQUEST, createGenericErrorAtStoppingTransition())
        
        .addTransition(AMContainerState.STOPPING, AMContainerState.STOPPING, AMContainerEventType.C_ASSIGN_TA, createAssignTAAtStoppingTransition())
        .addTransition(AMContainerState.STOPPING, AMContainerState.COMPLETED, AMContainerEventType.C_COMPLETED, createCompletedAtStoppingTransition())
        .addTransition(AMContainerState.STOPPING, AMContainerState.STOPPING, AMContainerEventType.C_NODE_FAILED, createNodeFailedAtStoppingTransition())
        .addTransition(AMContainerState.STOPPING, AMContainerState.STOPPING, EnumSet.of(AMContainerEventType.C_LAUNCHED, AMContainerEventType.C_LAUNCH_FAILED, AMContainerEventType.C_PULL_TA, AMContainerEventType.C_TA_SUCCEEDED, AMContainerEventType.C_STOP_REQUEST, AMContainerEventType.C_NM_STOP_SENT, AMContainerEventType.C_NM_STOP_FAILED, AMContainerEventType.C_TIMED_OUT))
        .addTransition(AMContainerState.STOP_REQUESTED, AMContainerState.STOP_REQUESTED, AMContainerEventType.C_LAUNCH_REQUEST, createGenericErrorAtStoppingTransition())
        
        .addTransition(AMContainerState.COMPLETED, AMContainerState.COMPLETED, AMContainerEventType.C_ASSIGN_TA, createAssignTAAtCompletedTransition())
        .addTransition(AMContainerState.COMPLETED, AMContainerState.COMPLETED, AMContainerEventType.C_NODE_FAILED, createNodeFailedAtCompletedTransition())
        .addTransition(AMContainerState.COMPLETED, AMContainerState.COMPLETED, EnumSet.of(AMContainerEventType.C_LAUNCH_REQUEST, AMContainerEventType.C_LAUNCH_FAILED, AMContainerEventType.C_PULL_TA, AMContainerEventType.C_TA_SUCCEEDED, AMContainerEventType.C_STOP_REQUEST, AMContainerEventType.C_NM_STOP_SENT, AMContainerEventType.C_NM_STOP_FAILED, AMContainerEventType.C_TIMED_OUT))
        .addTransition(AMContainerState.COMPLETED, AMContainerState.COMPLETED, EnumSet.of(AMContainerEventType.C_LAUNCH_REQUEST), createGenericErrorAtStoppingTransition())

        .installTopology();
  }

  // Attempting to use a container based purely on reosurces required, etc needs
  // additional change - JvmID, YarnChild, etc depend on TaskType.
  public AMContainerImpl(Container container, ContainerHeartbeatHandler chh,
      TaskAttemptListener tal, AppContext appContext) {
    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    this.readLock = rwLock.readLock();
    this.writeLock = rwLock.writeLock();
    this.container = container;
    this.containerId = container.getId();
    this.eventHandler = appContext.getEventHandler();
    this.appContext = appContext;
    this.containerHeartbeatHandler = chh;
    this.taskAttemptListener = tal;

    synchronized (stateMachineFactory) {
      if (!stateMachineInited) {
        initStateMachineFactory();
        stateMachineInited = true;
      }
    }
    this.stateMachine = stateMachineFactory.make(this);
  }
  
  @Override
  public AMContainerState getState() {
    readLock.lock();
    try {
      return stateMachine.getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public ContainerId getContainerId() {
    return this.containerId;
  }
  
  @Override
  public Container getContainer() {
    return this.container;
  }

  @Override
  public List<TaskAttemptId> getCompletedTaskAttempts() {
    readLock.lock();
    try {
      return new ArrayList<TaskAttemptId>(this.completedAttempts);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public List<TaskAttemptId> getQueuedTaskAttempts() {
    readLock.lock();
    try {
      return Collections.singletonList(this.pendingAttempt);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public TaskAttemptId getRunningTaskAttempt() {
    readLock.lock();
    try {
      return this.runningAttempt;
    } finally {
      readLock.unlock();
    }
  }
  
  @Override
  public int getShufflePort() {
    readLock.lock();
    try {
      return this.shufflePort;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void handle(AMContainerEvent event) {
    this.writeLock.lock();
    LOG.info("XXX: Processing ContainerEvent: " + event.getContainerId() + " of type "
        + event.getType() + " while in state: " + getState());
    try {
      final AMContainerState oldState = getState();
      try {
        stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle event " + event.getType()
            + " at current state " + oldState + " for ContainerId "
            + this.containerId, e);
        inError = true;
        // TODO XXX: Can't set state to COMPLETED. Add a default error state.
      }
      if (oldState != getState()) {
        LOG.info("AMContainer " + this.containerId + " transitioned from "
            + oldState + " to " + getState());
      }
    } finally {
      writeLock.unlock();
    }
  }
  
  @SuppressWarnings("unchecked")
  private void sendEvent(Event<?> event) {
    this.eventHandler.handle(event);
  }

  // Push the TaskAttempt to the TAL, instead of the TAL pulling when a JVM asks for a TaskAttempt.
  public org.apache.hadoop.mapred.Task pullTaskAttempt() {
    this.writeLock.lock();
    try {
      // Force through the state machine.
      this.handle(new AMContainerEvent(containerId,
          AMContainerEventType.C_PULL_TA));
      return pullAttempt == null ? null : remoteTaskMap.get(pullAttempt);
      // TODO Clear the remoteTask at this point.
    } finally {
      this.pullAttempt = null;
      this.writeLock.unlock();
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createLaunchRequestTransition() {
    return new LaunchRequest();
  }

  protected static class LaunchRequest implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      AMContainerLaunchRequestEvent event = (AMContainerLaunchRequestEvent) cEvent;

      JobID oldJobID = TypeConverter.fromYarn(event.getJobId());
      container.jvmId = new WrappedJvmID(oldJobID,
          event.getTaskTypeForContainer() == TaskType.MAP,
          container.containerId.getId());
      
      container.clc = AMContainerHelpers.createContainerLaunchContext(
          container.appContext.getApplicationACLs(),
          container.getContainerId(), event.getJobConf(),
          event.getTaskTypeForContainer(), event.getJobToken(),
          TypeConverter.fromYarn(event.getJobId()),
          container.getContainer().getResource(), container.jvmId,
          container.taskAttemptListener, event.getCredentials(),
          event.shouldProfile());
      
      container.sendEvent(new NMCommunicatorLaunchRequestEvent(container.clc,
          container.container));
      LOG.info("Sending Launch Request for Container with id: "
          + container.clc.getContainerId());
      // Forget about the clc to save resources. At some point, part of the clc
      // info may need to be exposed to the scheduler to figure out whether a 
      // container can be used for a specific TaskAttempt.
      container.clc = null;
    }
  }
  
  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createAssignTaskAttemptAtAllocatedTransition() {
    return new AssignTaskAttemptAtAllocated();
  }

  protected static class AssignTaskAttemptAtAllocated implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      AMContainerAssignTAEvent event = (AMContainerAssignTAEvent) cEvent;
      container.inError = true;
      container.sendTerminatedToTaskAttempt(event.getTaskAttemptId(),
          "AMScheduler Error: TaskAttempt allocated to unlaunched container: "
              + container.getContainerId());
      container.sendCompletedToScheduler();
      container.deAllocate();
      LOG.warn("Unexpected TA Assignment: TAId: " + event.getTaskAttemptId()
          + "  for ContainerId: " + container.getContainerId()
          + " while in state: " + container.getState());
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createCompletedAtAllocatedTransition() {
    return new CompltedAtAllocated();
  }

  protected static class CompltedAtAllocated implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      container.sendCompletedToScheduler();
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createStopRequestTransition() {
    return new StopRequest();
  }

  // TODO Rename to de-allocate container transition. subclass
  protected static class StopRequest implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      container.sendCompletedToScheduler();
      container.deAllocate();
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createNodeFailedAtAllocatedTransition() {
    return new NodeFailedAtAllocated();
  }

  protected static class NodeFailedAtAllocated implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      container.sendCompletedToScheduler();
      container.deAllocate();
    }
  }
  
  protected void registerFailedTAAssignment(TaskAttemptId taId) {
    failedAssignments.add(taId);
  }
  
  protected void deAllocate() {
    sendEvent(new RMCommunicatorContainerDeAllocateRequestEvent(containerId));
  }
  
  protected void sendCompletedToScheduler() {
    sendEvent(new AMSchedulerEventContainerCompleted(containerId));
  }

  protected void sendTerminatedToTaskAttempt(TaskAttemptId taId, String message) {
    sendEvent(new TaskAttemptEventContainerTerminated(taId, message));
  }

  protected void sendTerminatingToTA(TaskAttemptId taId, String message) {
    sendEvent(new TaskAttemptEventContainerTerminating(taId, message));
  }
  
  protected void sendNodeFailureToTA(AMContainerEvent event,
      TaskAttemptId taId, String message) {
    sendEvent(new TaskAttemptEventNodeFailed(taId, message));
    // TODO XXX: Diag message from the node. Otherwise include the nodeId
  }

  protected void sendStopRequestToNM() {
    sendEvent(new NMCommunicatorStopRequestEvent(containerId,
        container.getNodeId(), container.getContainerToken()));
  }
  
  protected void unregisterAttemptFromListener(TaskAttemptId attemptId) {
    taskAttemptListener.unregisterTaskAttempt(attemptId);
  }
  
  protected void unregisterJvmFromListener(WrappedJvmID jvmId) {
    taskAttemptListener.unregisterRunningJvm(jvmId);
  }

  protected MultipleArcTransition<AMContainerImpl, AMContainerEvent, AMContainerState>
      createAssignTaskAttemptTransition() {
    return new AssignTaskAttempt();
  }

  protected static class AssignTaskAttempt
      implements
      MultipleArcTransition<AMContainerImpl, AMContainerEvent, AMContainerState> {

    @Override
    // Instead of a MultpleArcTransition - this could just be an exception in
    // case of the errror.
    public AMContainerState transition(AMContainerImpl container,
        AMContainerEvent cEvent) {
      AMContainerAssignTAEvent event = (AMContainerAssignTAEvent) cEvent;
      if (container.pendingAttempt != null) {
        container.inError = true;
        String errorMessage = "AMScheduler Error: Multiple simultaneous " +
        		"taskAttempt allocations to: " + container.getContainerId();
        container.sendTerminatingToTA(event.getTaskAttemptId(), errorMessage);
        container.registerFailedTAAssignment(event.getTaskAttemptId());
        // TODO XXX: Verify that it's ok to send in a NM_STOP_REQUEST. The
        // NMCommunicator should be able to handle this. The STOP_REQUEST would
        // only go out after the START_REQUEST.
        LOG.warn(errorMessage);
        container.sendStopRequestToNM();
        return AMContainerState.STOP_REQUESTED;
      }
      container.pendingAttempt = event.getTaskAttemptId();
      container.remoteTaskMap.put(event.getTaskAttemptId(),
          event.getRemoteTask());
      // TODO Consider registering with the TAL, instead of the TAL pulling.
      // Possibly after splitting TAL and ContainerListener.
      return container.getState();
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createLaunchedTransition() {
    return new Launched();
  }

  protected static class Launched implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      AMContainerEventLaunched event = (AMContainerEventLaunched) cEvent;
      container.shufflePort = event.getShufflePort();
      container.taskAttemptListener.registerRunningJvm(container.jvmId, container.containerId);
      container.containerHeartbeatHandler.register(container.containerId);
    }
  }

  protected MultipleArcTransition<AMContainerImpl, AMContainerEvent, 
      AMContainerState> createPullTAAtIdleTransition() {
    return new PullTAAtIdle();
  }

  protected static class PullTAAtIdle implements MultipleArcTransition<
      AMContainerImpl, AMContainerEvent, AMContainerState> {

    @Override
    public AMContainerState transition(AMContainerImpl container,
        AMContainerEvent cEvent) {
      if (container.pendingAttempt != null) {
        // This will be invoked as part of the PULL_REQUEST - so pullAttempt pullAttempt
        // should ideally only end up being populated during the duration of this call,
        // which is in a write lock. pullRequest() should move this to the running state.
        container.pullAttempt = container.pendingAttempt;
        container.runningAttempt = container.pendingAttempt;
        container.pendingAttempt = null;
        if (container.lastTaskFinishTime != 0) {
          long idleTimeDiff = System.currentTimeMillis() - container.lastTaskFinishTime;
          LOG.info("XXX: Computing idle time for container: " + container.getContainerId() + ", lastFinishTime: " + container.lastTaskFinishTime + ", Incremented by: " + idleTimeDiff);
          container.idleTimeBetweenTasks += System.currentTimeMillis() - container.lastTaskFinishTime;
        }
        LOG.info("XXX: Assigned task + [" + container.runningAttempt + "] to container: [" + container.getContainerId() + "]");
        return AMContainerState.RUNNING;
      } else {
        return AMContainerState.IDLE;
      }
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createLaunchFailedTransition() {
    return new LaunchFailed();
  }

  protected static class LaunchFailed implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      if (container.pendingAttempt != null) {
        AMContainerEventLaunchFailed event = (AMContainerEventLaunchFailed) cEvent;
        container.sendTerminatingToTA(container.pendingAttempt,
            event.getMessage());
      }
      container.deAllocate();
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createCompletedAtLaunchingTransition() {
    return new CompletedAtLaunching();
  }

  protected static class CompletedAtLaunching implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      if (container.pendingAttempt != null) {
        String errorMessage = "Container" + container.getContainerId()
            + " failed. Received COMPLETED event while trying to launch";
        container.sendTerminatedToTaskAttempt(container.pendingAttempt,
            errorMessage);
        LOG.warn(errorMessage);    
        // TODO XXX Maybe nullify pendingAttempt.
      }
      container.sendCompletedToScheduler();
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createStopRequestAtLaunchingTransition() {
    return new StopRequestAtLaunching();
  }

  protected static class StopRequestAtLaunching implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      if (container.pendingAttempt != null) {
        container.sendTerminatingToTA(container.pendingAttempt,
            " Container" + container.getContainerId() + " received a STOP_REQUEST");
      }
      container.sendStopRequestToNM();
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createNodeFailedAtLaunchingTransition() {
    return new NodeFailedAtLaunching();
  }

  protected static class NodeFailedAtLaunching implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      if (container.pendingAttempt != null) {
        container.sendNodeFailureToTA(cEvent, container.pendingAttempt, null);
        // TODO XXX: Maybe include a diagnostic message along with the incoming
        // Node failure event.
        container.sendTerminatingToTA(container.pendingAttempt, "Node failure");
      }
      container.sendStopRequestToNM();
      container.deAllocate();
    }
  }

  protected MultipleArcTransition<AMContainerImpl, AMContainerEvent, AMContainerState>
      createAssignTaskAttemptAtIdleTransition() {
    return new AssignTaskAttemptAtIdle();
  }

  // TODO XXX Make this the base for all assignRequests. Some more error checking in
  // that case.
  protected static class AssignTaskAttemptAtIdle
      implements
      MultipleArcTransition<AMContainerImpl, AMContainerEvent, AMContainerState> {
    @Override
    public AMContainerState transition(AMContainerImpl container,
        AMContainerEvent cEvent) {
      AMContainerAssignTAEvent event = (AMContainerAssignTAEvent) cEvent;
      if (container.pendingAttempt != null) {
        container.inError = true;
        String errorMessage = "AMScheduler Error: Multiple simultaneous "
            + "taskAttempt allocations to: " + container.getContainerId();
        container.sendTerminatingToTA(event.getTaskAttemptId(), errorMessage);
        container.registerFailedTAAssignment(event.getTaskAttemptId());
        LOG.warn(errorMessage);
        container.sendStopRequestToNM();
        container.containerHeartbeatHandler.unregister(container.containerId);
        
        return AMContainerState.STOP_REQUESTED;
      }
      container.pendingAttempt = event.getTaskAttemptId();
      // TODO XXX LATER. Cleanup the remoteTaskMap.
      container.remoteTaskMap.put(event.getTaskAttemptId(),
          event.getRemoteTask());
      return AMContainerState.IDLE;
    }
  }
  
  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createCompletedAtIdleTransition() {
    return new CompletedAtIdle();
  }
  
  // TODO Create a base CompletedTransition - at least contains some kind of logging. Informing the scheduler etc.
  protected static class CompletedAtIdle implements SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      LOG.info("Cotnainer with id: " + container.getContainerId()
          + " Completed." + " Previous state was: " + container.getState());
      if (container.pendingAttempt != null) {
        container.sendTerminatedToTaskAttempt(container.pendingAttempt,
            "Container " + container.getContainerId() + " FINISHED.");
      }
      container.sendCompletedToScheduler();
      container.containerHeartbeatHandler.unregister(container.containerId);
      container.unregisterJvmFromListener(container.jvmId);
    }
  }
  
  protected SingleArcTransition<AMContainerImpl, AMContainerEvent> 
      createStopRequestAtIdleTransition() {
    return new StopRequestAtIdle();
  }
  
  protected static class StopRequestAtIdle extends StopRequestAtLaunching {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      super.transition(container, cEvent);
      LOG.info("XXX: IdleTimeBetweenTasks: " + container.idleTimeBetweenTasks);
      container.containerHeartbeatHandler.unregister(container.containerId);
      container.unregisterJvmFromListener(container.jvmId);
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createTimedOutAtIdleTransition() {
    return new TimedOutAtIdle();
  }

  protected static class TimedOutAtIdle extends StopRequestAtIdle {
    // TODO XXX: Override to change the diagnostic message that goes to the TaskAttempt. Functionality is the same.
  }
  
  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createTASucceededAtRunningTransition() {
    return new TASucceededAtRunning();
  }

  protected static class TASucceededAtRunning implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      container.lastTaskFinishTime = System.currentTimeMillis();
      container.completedAttempts.add(container.runningAttempt);
      container.unregisterAttemptFromListener(container.runningAttempt);
      container.runningAttempt = null;
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createCompletedAtRunningTransition() {
    return new CompletedAtRunning();
  }

  protected static class CompletedAtRunning implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      container.sendTerminatedToTaskAttempt(container.runningAttempt,
          "Container " + container.getContainerId()
              + " FINISHED while task was running");
      container.sendCompletedToScheduler();
      container.containerHeartbeatHandler.unregister(container.containerId);
      container.unregisterAttemptFromListener(container.runningAttempt);
      container.unregisterJvmFromListener(container.jvmId);
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent> 
      createStopRequestAtRunningTransition() {
    return new StopRequestAtRunning();
  }
  
  protected static class StopRequestAtRunning extends StopRequestAtIdle {
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      super.transition(container, cEvent);
      container.unregisterAttemptFromListener(container.runningAttempt);
      container.sendTerminatingToTA(container.runningAttempt,
          " Container" + container.getContainerId() + " received a STOP_REQUEST");
      // TODO XXX: All running transition. verify whether runningAttempt should be null.
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createTimedOutAtRunningTransition() {
    return new TimedOutAtRunning();
  }

  protected static class TimedOutAtRunning extends StopRequestAtRunning {
    // TODO XXX: Change the error message.
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createNodeFailedAtRunningTransition() {
    return new NodeFailedAtRunning();
  }

  protected static class NodeFailedAtRunning extends NodeFailedAtIdle {

    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      super.transition(container, cEvent);
      container.sendNodeFailureToTA(cEvent, container.runningAttempt, null);
      container.sendTerminatingToTA(container.runningAttempt, "Node failure");

      container.unregisterAttemptFromListener(container.runningAttempt);
    }
  }
 
  // TODO Rename - is re-used in COMPLETED states.
  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createAssignTAAtStoppingTransition() {
    return new AssignTAAtStopping();
  }

  protected static class AssignTAAtStopping implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      AMContainerAssignTAEvent event = (AMContainerAssignTAEvent) cEvent;
      container.inError = true;
      String errorMessage = "AttemptId: " + event.getTaskAttemptId()
          + " cannot be allocated to container: " + container.getContainerId()
          + " in " + container.getState() + " state";
      container.sendTerminatingToTA(event.getTaskAttemptId(), errorMessage);
      container.registerFailedTAAssignment(event.getTaskAttemptId());
    }
  }

  // TODO XXX Rename all createGenericError*s ... not really generic.
  protected SingleArcTransition<AMContainerImpl, AMContainerEvent> createGenericErrorAtStoppingTransition() {
    return new GenericErrorAtStopping();
  }

  protected static class GenericErrorAtStopping implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {

    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      container.inError = true;
      // TODO XXX: Anything else required in the error transitions ?
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent> createAssignTAAtCompletedTransition() {
    return new AssignTAAtCompleted();
  }

  protected static class AssignTAAtCompleted implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {

    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      AMContainerAssignTAEvent event = (AMContainerAssignTAEvent) cEvent;
      String errorMessage = "AttemptId: " + event.getTaskAttemptId()
          + " cannot be allocated to container: " + container.getContainerId()
          + " in COMPLETED state";
      container.sendTerminatedToTaskAttempt(event.getTaskAttemptId(),
          errorMessage);
    }
  }
  
  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createCompletedAtStoppingTransition() {
    return new CompletedAtStopping();
  }
  
  protected static class CompletedAtStopping implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      // TODO XXX: Set everything to null after sending these out.
      if (container.pendingAttempt != null) {
        container.sendTerminatedToTaskAttempt(container.pendingAttempt, null);
      }
      if (container.runningAttempt != null) {
        container.sendTerminatedToTaskAttempt(container.runningAttempt, null);
      }
      container.sendCompletedToScheduler();
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createStopFailedAtNMStopRequested() {
    return new StopFailedAtNMStopRequested();
  }

  protected static class StopFailedAtNMStopRequested implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      container.deAllocate();
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent> 
      createNodeFailedBaseTransition() {
    return new NodeFailedBase();
  }
  
  protected static class NodeFailedBase implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      // TODO Make sure runningAttempts etc are set to null before entering this
      // state.
      // Alternately some way to track if an event has gone out for a task. or
      // let multiple events go out and the TA should be able to handle them.
      // Kill_TA going out in this case.
      if (container.runningAttempt != null) {
        container.sendNodeFailureToTA(cEvent, container.runningAttempt, null);
        container.sendTerminatingToTA(container.runningAttempt, "Node Failure");
      }
      if (container.pendingAttempt != null) {
        container.sendNodeFailureToTA(cEvent, container.pendingAttempt, null);
      }
      for (TaskAttemptId attemptId : container.completedAttempts) {
        // TODO XXX: Make sure TaskAttempt knows how to handle kills to REDUCEs.
        container.sendNodeFailureToTA(cEvent, attemptId, null);
      }
    }
  }
  
  protected SingleArcTransition<AMContainerImpl, AMContainerEvent> 
      createNodeFailedAtStoppingTransition() {
    return new NodeFailedAtSopping();
  }
  
  protected static class NodeFailedAtSopping extends NodeFailedBase {
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      super.transition(container, cEvent);
      if (container.runningAttempt != null) { 
        container.sendTerminatingToTA(container.runningAttempt, "Node Failure");
      }
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createNodeFailedAtCompletedTransition() {
    return new NodeFailedAtCompleted();
  }

  protected static class NodeFailedAtCompleted extends NodeFailedBase {
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      super.transition(container, cEvent);
      if (container.runningAttempt != null) {
        container.sendTerminatedToTaskAttempt(container.runningAttempt,
            "Node Failure");
      }
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent> createNodeFailedAtNMStopRequestedTransition() {
    return new NodeFailedAtNMStopRequested();
  }

  protected static class NodeFailedAtNMStopRequested implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      if (container.runningAttempt != null) {
        container.sendNodeFailureToTA(cEvent, container.runningAttempt,
            null);
        container.sendTerminatingToTA(container.runningAttempt, "Node Failure");
      }
      if (container.pendingAttempt != null) {
        container.sendNodeFailureToTA(cEvent, container.pendingAttempt,
            null);
      }
      for (TaskAttemptId attemptId : container.completedAttempts) {
        // TODO XXX: Make sure TaskAttempt knows how to handle kills to REDUCEs.
        container.sendNodeFailureToTA(cEvent, attemptId, null);
      }
      for (TaskAttemptId attemptId : container.failedAssignments) {
        container.sendNodeFailureToTA(cEvent, attemptId, null);
      }
      container.deAllocate();
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createNodeFailedAtIdleTransition() {
    return new NodeFailedAtIdle();
  }

  protected static class NodeFailedAtIdle implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {

    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      container.sendStopRequestToNM();
      container.deAllocate();
      if (container.pendingAttempt != null) {
        container.sendNodeFailureToTA(cEvent, container.pendingAttempt, null);
        container.sendTerminatingToTA(container.pendingAttempt, "Node Failure");
      }
      for (TaskAttemptId taId : container.completedAttempts) {
        container.sendNodeFailureToTA(cEvent, taId, null);
      }
      container.containerHeartbeatHandler.unregister(container.containerId);
      container.unregisterJvmFromListener(container.jvmId);
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createAssignTaskAttemptAtRunningTransition() {
    return new AssignTaskAttemptAtRunning();
  }

  protected static class AssignTaskAttemptAtRunning implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      AMContainerAssignTAEvent event = (AMContainerAssignTAEvent) cEvent;
      container.inError = true;
      String errorMessage = "AttemptId: " + event.getTaskAttemptId()
          + " cannot be allocated to container: " + container.getContainerId()
          + " in RUNNING state. Already executing TaskAttempt: "
          + container.runningAttempt;
      container.sendTerminatingToTA(event.getTaskAttemptId(), errorMessage);
      container.registerFailedTAAssignment(event.getTaskAttemptId());
      
      container.sendTerminatingToTA(container.runningAttempt, errorMessage);
      
      container.sendStopRequestToNM();
      container.unregisterAttemptFromListener(container.runningAttempt);
      container.unregisterJvmFromListener(container.jvmId);
      container.containerHeartbeatHandler.unregister(container.containerId);

    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createGenericErrorTransition() {
    return new GenericError();
  }

  protected static class GenericError implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      container.inError = true;
      container.deAllocate();
      LOG.info("Unexpected event type: " + cEvent.getType()
          + " while in state: " + container.getState() + ". Event: " + cEvent);

    }
  }
  
  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createGenericErrorAtLaunchingTransition() {
    return new GenericErrorAtLaunching();
  }

  protected static class GenericErrorAtLaunching extends GenericError {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      container.sendStopRequestToNM();
      super.transition(container, cEvent);
    }
  }

  protected SingleArcTransition<AMContainerImpl, AMContainerEvent>
      createGenericErrorAtIdleTransition() {
    return new GenericErrorAtIdle();
  }

  protected static class GenericErrorAtIdle extends GenericErrorAtLaunching {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      super.transition(container, cEvent);
      container.containerHeartbeatHandler.unregister(container.containerId);
      container.unregisterJvmFromListener(container.jvmId);
    }
  }
  
  protected SingleArcTransition<AMContainerImpl, AMContainerEvent> createGenericErrorAtRunningTransition() {
    return new GenericErrorAtRunning();
  }

  protected static class GenericErrorAtRunning extends GenericErrorAtIdle {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      super.transition(container, cEvent);
      container.unregisterAttemptFromListener(container.runningAttempt);
      container.unregisterJvmFromListener(container.jvmId);
    }
  }

  // TODO Create a generic ERROR state. Container tries informing relevant components in this case.
  // TODO XXX: Rename all generic error transitions.
  

}
