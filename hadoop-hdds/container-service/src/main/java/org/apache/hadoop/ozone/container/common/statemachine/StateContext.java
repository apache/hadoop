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

import com.google.protobuf.GeneratedMessage;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.OzoneConsts.INVALID_PORT;

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
  private final Queue<GeneratedMessage> reports;
  private final Queue<ContainerAction> containerActions;
  private DatanodeStateMachine.DatanodeStates state;

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
   * Get the container server port.
   * @return The container server port if available, return -1 if otherwise
   */
  public int getContainerPort() {
    return parent == null ?
        INVALID_PORT : parent.getContainer().getContainerServerPort();
  }

  /**
   * Gets the Ratis Port.
   * @return int , return -1 if not valid.
   */
  public int getRatisPort() {
    return parent == null ?
        INVALID_PORT : parent.getContainer().getRatisContainerServerPort();
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
    synchronized (reports) {
      reports.add(report);
    }
  }

  /**
   * Returns the next report, or null if the report queue is empty.
   *
   * @return report
   */
  public GeneratedMessage getNextReport() {
    synchronized (reports) {
      return reports.poll();
    }
  }

  /**
   * Returns all the available reports from the report queue, or empty list if
   * the queue is empty.
   *
   * @return List<reports>
   */
  public List<GeneratedMessage> getAllAvailableReports() {
    return getReports(Integer.MAX_VALUE);
  }

  /**
   * Returns available reports from the report queue with a max limit on
   * list size, or empty list if the queue is empty.
   *
   * @return List<reports>
   */
  public List<GeneratedMessage> getReports(int maxLimit) {
    synchronized (reports) {
      return reports.parallelStream().limit(maxLimit)
          .collect(Collectors.toList());
    }
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
   * @return List<ContainerAction>
   */
  public List<ContainerAction> getAllPendingContainerActions() {
    return getPendingContainerAction(Integer.MAX_VALUE);
  }

  /**
   * Returns pending ContainerActions from the ContainerAction queue with a
   * max limit on list size, or empty list if the queue is empty.
   *
   * @return List<ContainerAction>
   */
  public List<ContainerAction> getPendingContainerAction(int maxLimit) {
    synchronized (containerActions) {
      return containerActions.parallelStream().limit(maxLimit)
          .collect(Collectors.toList());
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
    this.addCmdStatus(cmd.getId(),
        CommandStatusBuilder.newBuilder()
            .setCmdId(cmd.getId())
            .setStatus(Status.PENDING)
            .setType(cmd.getType())
            .build());
  }

  /**
   * Get map holding all {@link CommandStatus} objects.
   *
   */
  public Map<Long, CommandStatus> getCommandStatusMap() {
    return cmdStatusMap;
  }

  /**
   * Remove object from cache in StateContext#cmdStatusMap.
   *
   */
  public void removeCommandStatus(Long cmdId) {
    cmdStatusMap.remove(cmdId);
  }

  /**
   * Updates status of a pending status command.
   * @param cmdId       command id
   * @param cmdExecuted SCMCommand
   * @return true if command status updated successfully else false.
   */
  public boolean updateCommandStatus(Long cmdId, boolean cmdExecuted) {
    if(cmdStatusMap.containsKey(cmdId)) {
      cmdStatusMap.get(cmdId)
          .setStatus(cmdExecuted ? Status.EXECUTED : Status.FAILED);
      return true;
    }
    return false;
  }
}
