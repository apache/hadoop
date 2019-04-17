/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.container.common.statemachine;

import com.google.common.base.Preconditions;
import com.google.protobuf.GeneratedMessage;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.PipelineAction;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerAction;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.CommandStatus.Status;
import org.apache.hadoop.ozone.container.common.states.DatanodeState;
import org.apache.hadoop.ozone.container.common.states.datanode
    .InitDatanodeState;
import org.apache.hadoop.ozone.container.common.states.datanode
    .RunningDatanodeState;
import org.apache.hadoop.ozone.protocol.commands.CommandStatus;
import org.apache.hadoop.ozone.protocol.commands.CommandStatus
    .CommandStatusBuilder;
import org.apache.hadoop.ozone.protocol.commands
    .DeleteBlockCommandStatus.DeleteBlockCommandStatusBuilder;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;

import static java.lang.Math.min;
import static org.apache.hadoop.hdds.scm.HddsServerUtil.getScmHeartbeatInterval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * Current Context of State Machine.
 */
public class StateContext {
  static final Logger LOG =
      LoggerFactory.getLogger(StateContext.class);
  private final Queue<SCMCommand> commandQueue;
  private final Map<Long, CommandStatus> cmdStatusMap;
  private final Lock lock;
  private final DatanodeStateMachine parent;
  private final AtomicLong stateExecutionCount;
  private final Configuration conf;
  private final List<GeneratedMessage> reports;
  private final Queue<ContainerAction> containerActions;
  private final Queue<PipelineAction> pipelineActions;
  private DatanodeStateMachine.DatanodeStates state;

  /**
   * Starting with a 2 sec heartbeat frequency which will be updated to the
   * real HB frequency after scm registration. With this method the
   * initial registration could be significant faster.
   */
  private AtomicLong heartbeatFrequency = new AtomicLong(2000);

  /**
   * Constructs a StateContext.
   *
   * @param conf   - Configration
   * @param state  - State
   * @param parent Parent State Machine
   */
  public StateContext(Configuration conf, DatanodeStateMachine.DatanodeStates
      state, DatanodeStateMachine parent) {
    this.conf = conf;
    this.state = state;
    this.parent = parent;
    commandQueue = new LinkedList<>();
    cmdStatusMap = new ConcurrentHashMap<>();
    reports = new LinkedList<>();
    containerActions = new LinkedList<>();
    pipelineActions = new LinkedList<>();
    lock = new ReentrantLock();
    stateExecutionCount = new AtomicLong(0);
  }

  /**
   * Returns the ContainerStateMachine class that holds this state.
   *
   * @return ContainerStateMachine.
   */
  public DatanodeStateMachine getParent() {
    return parent;
  }

  /**
   * Returns true if we are entering a new state.
   *
   * @return boolean
   */
  boolean isEntering() {
    return stateExecutionCount.get() == 0;
  }

  /**
   * Returns true if we are exiting from the current state.
   *
   * @param newState - newState.
   * @return boolean
   */
  boolean isExiting(DatanodeStateMachine.DatanodeStates newState) {
    boolean isExiting = state != newState && stateExecutionCount.get() > 0;
    if(isExiting) {
      stateExecutionCount.set(0);
    }
    return isExiting;
  }

  /**
   * Returns the current state the machine is in.
   *
   * @return state.
   */
  public DatanodeStateMachine.DatanodeStates getState() {
    return state;
  }

  /**
   * Sets the current state of the machine.
   *
   * @param state state.
   */
  public void setState(DatanodeStateMachine.DatanodeStates state) {
    this.state = state;
  }

  /**
   * Adds the report to report queue.
   *
   * @param report report to be added
   */
  public void addReport(GeneratedMessage report) {
    if (report != null) {
      synchronized (reports) {
        reports.add(report);
      }
    }
  }

  /**
   * Adds the reports which could not be sent by heartbeat back to the
   * reports list.
   *
   * @param reportsToPutBack list of reports which failed to be sent by
   *                         heartbeat.
   */
  public void putBackReports(List<GeneratedMessage> reportsToPutBack) {
    synchronized (reports) {
      reports.addAll(0, reportsToPutBack);
    }
  }

  /**
   * Returns all the available reports from the report queue, or empty list if
   * the queue is empty.
   *
   * @return List of reports
   */
  public List<GeneratedMessage> getAllAvailableReports() {
    return getReports(Integer.MAX_VALUE);
  }

  /**
   * Returns available reports from the report queue with a max limit on
   * list size, or empty list if the queue is empty.
   *
   * @return List of reports
   */
  public List<GeneratedMessage> getReports(int maxLimit) {
    List<GeneratedMessage> reportsToReturn = new LinkedList<>();
    synchronized (reports) {
      List<GeneratedMessage> tempList = reports.subList(
          0, min(reports.size(), maxLimit));
      reportsToReturn.addAll(tempList);
      tempList.clear();
    }
    return reportsToReturn;
  }


  /**
   * Adds the ContainerAction to ContainerAction queue.
   *
   * @param containerAction ContainerAction to be added
   */
  public void addContainerAction(ContainerAction containerAction) {
    synchronized (containerActions) {
      containerActions.add(containerAction);
    }
  }

  /**
   * Add ContainerAction to ContainerAction queue if it's not present.
   *
   * @param containerAction ContainerAction to be added
   */
  public void addContainerActionIfAbsent(ContainerAction containerAction) {
    synchronized (containerActions) {
      if (!containerActions.contains(containerAction)) {
        containerActions.add(containerAction);
      }
    }
  }

  /**
   * Returns all the pending ContainerActions from the ContainerAction queue,
   * or empty list if the queue is empty.
   *
   * @return {@literal List<ContainerAction>}
   */
  public List<ContainerAction> getAllPendingContainerActions() {
    return getPendingContainerAction(Integer.MAX_VALUE);
  }

  /**
   * Returns pending ContainerActions from the ContainerAction queue with a
   * max limit on list size, or empty list if the queue is empty.
   *
   * @return {@literal List<ContainerAction>}
   */
  public List<ContainerAction> getPendingContainerAction(int maxLimit) {
    List<ContainerAction> containerActionList = new ArrayList<>();
    synchronized (containerActions) {
      if (!containerActions.isEmpty()) {
        int size = containerActions.size();
        int limit = size > maxLimit ? maxLimit : size;
        for (int count = 0; count < limit; count++) {
          // we need to remove the action from the containerAction queue
          // as well
          ContainerAction action = containerActions.poll();
          Preconditions.checkNotNull(action);
          containerActionList.add(action);
        }
      }
      return containerActionList;
    }
  }

  /**
   * Add PipelineAction to PipelineAction queue if it's not present.
   *
   * @param pipelineAction PipelineAction to be added
   */
  public void addPipelineActionIfAbsent(PipelineAction pipelineAction) {
    synchronized (pipelineActions) {
      /**
       * If pipelineAction queue already contains entry for the pipeline id
       * with same action, we should just return.
       * Note: We should not use pipelineActions.contains(pipelineAction) here
       * as, pipelineAction has a msg string. So even if two msgs differ though
       * action remains same on the given pipeline, it will end up adding it
       * multiple times here.
       */
      for (PipelineAction pipelineActionIter : pipelineActions) {
        if (pipelineActionIter.getAction() == pipelineAction.getAction()
            && pipelineActionIter.hasClosePipeline() && pipelineAction
            .hasClosePipeline()
            && pipelineActionIter.getClosePipeline().getPipelineID()
            .equals(pipelineAction.getClosePipeline().getPipelineID())) {
          return;
        }
      }
      pipelineActions.add(pipelineAction);
    }
  }

  /**
   * Returns pending PipelineActions from the PipelineAction queue with a
   * max limit on list size, or empty list if the queue is empty.
   *
   * @return {@literal List<ContainerAction>}
   */
  public List<PipelineAction> getPendingPipelineAction(int maxLimit) {
    List<PipelineAction> pipelineActionList = new ArrayList<>();
    synchronized (pipelineActions) {
      if (!pipelineActions.isEmpty()) {
        int size = pipelineActions.size();
        int limit = size > maxLimit ? maxLimit : size;
        for (int count = 0; count < limit; count++) {
          pipelineActionList.add(pipelineActions.poll());
        }
      }
      return pipelineActionList;
    }
  }

  /**
   * Returns the next task to get executed by the datanode state machine.
   * @return A callable that will be executed by the
   * {@link DatanodeStateMachine}
   */
  @SuppressWarnings("unchecked")
  public DatanodeState<DatanodeStateMachine.DatanodeStates> getTask() {
    switch (this.state) {
    case INIT:
      return new InitDatanodeState(this.conf, parent.getConnectionManager(),
          this);
    case RUNNING:
      return new RunningDatanodeState(this.conf, parent.getConnectionManager(),
          this);
    case SHUTDOWN:
      return null;
    default:
      throw new IllegalArgumentException("Not Implemented yet.");
    }
  }

  /**
   * Executes the required state function.
   *
   * @param service - Executor Service
   * @param time    - seconds to wait
   * @param unit    - Seconds.
   * @throws InterruptedException
   * @throws ExecutionException
   * @throws TimeoutException
   */
  public void execute(ExecutorService service, long time, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    stateExecutionCount.incrementAndGet();
    DatanodeState<DatanodeStateMachine.DatanodeStates> task = getTask();

    // Adding not null check, in a case where datanode is still starting up, but
    // we called stop DatanodeStateMachine, this sets state to SHUTDOWN, and
    // there is a chance of getting task as null.
    if (task != null) {
      if (this.isEntering()) {
        task.onEnter();
      }
      task.execute(service);
      DatanodeStateMachine.DatanodeStates newState = task.await(time, unit);
      if (this.state != newState) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Task {} executed, state transited from {} to {}",
              task.getClass().getSimpleName(), this.state, newState);
        }
        if (isExiting(newState)) {
          task.onExit();
        }
        this.setState(newState);
      }
    }
  }

  /**
   * Returns the next command or null if it is empty.
   *
   * @return SCMCommand or Null.
   */
  public SCMCommand getNextCommand() {
    lock.lock();
    try {
      return commandQueue.poll();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Adds a command to the State Machine queue.
   *
   * @param command - SCMCommand.
   */
  public void addCommand(SCMCommand command) {
    lock.lock();
    try {
      commandQueue.add(command);
    } finally {
      lock.unlock();
    }
    this.addCmdStatus(command);
  }

  /**
   * Returns the count of the Execution.
   * @return long
   */
  public long getExecutionCount() {
    return stateExecutionCount.get();
  }

  /**
   * Returns the next {@link CommandStatus} or null if it is empty.
   *
   * @return {@link CommandStatus} or Null.
   */
  public CommandStatus getCmdStatus(Long key) {
    return cmdStatusMap.get(key);
  }

  /**
   * Adds a {@link CommandStatus} to the State Machine.
   *
   * @param status - {@link CommandStatus}.
   */
  public void addCmdStatus(Long key, CommandStatus status) {
    cmdStatusMap.put(key, status);
  }

  /**
   * Adds a {@link CommandStatus} to the State Machine for given SCMCommand.
   *
   * @param cmd - {@link SCMCommand}.
   */
  public void addCmdStatus(SCMCommand cmd) {
    final Optional<CommandStatusBuilder> cmdStatusBuilder;
    switch (cmd.getType()) {
    case replicateContainerCommand:
      cmdStatusBuilder = Optional.of(CommandStatusBuilder.newBuilder());
      break;
    case deleteBlocksCommand:
      cmdStatusBuilder = Optional.of(
          DeleteBlockCommandStatusBuilder.newBuilder());
      break;
    case deleteContainerCommand:
      cmdStatusBuilder = Optional.of(CommandStatusBuilder.newBuilder());
      break;
    default:
      cmdStatusBuilder = Optional.empty();
    }
    cmdStatusBuilder.ifPresent(statusBuilder ->
        addCmdStatus(cmd.getId(), statusBuilder
            .setCmdId(cmd.getId())
            .setStatus(Status.PENDING)
            .setType(cmd.getType())
            .build()));
  }

  /**
   * Get map holding all {@link CommandStatus} objects.
   *
   */
  public Map<Long, CommandStatus> getCommandStatusMap() {
    return cmdStatusMap;
  }

  /**
   * Updates status of a pending status command.
   * @param cmdId       command id
   * @param cmdStatusUpdater Consumer to update command status.
   * @return true if command status updated successfully else false.
   */
  public boolean updateCommandStatus(Long cmdId,
      Consumer<CommandStatus> cmdStatusUpdater) {
    if(cmdStatusMap.containsKey(cmdId)) {
      cmdStatusUpdater.accept(cmdStatusMap.get(cmdId));
      return true;
    }
    return false;
  }

  public void configureHeartbeatFrequency(){
    heartbeatFrequency.set(getScmHeartbeatInterval(conf));
  }

  /**
   * Return current heartbeat frequency in ms.
   */
  public long getHeartbeatFrequency() {
    return heartbeatFrequency.get();
  }
}
