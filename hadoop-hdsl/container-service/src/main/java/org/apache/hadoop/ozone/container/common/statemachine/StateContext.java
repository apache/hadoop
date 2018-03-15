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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.container.common.states.datanode.InitDatanodeState;
import org.apache.hadoop.ozone.container.common.states.DatanodeState;
import org.apache.hadoop.ozone.container.common.states.datanode.RunningDatanodeState;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.hdsl.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMNodeReport;
import org.apache.hadoop.hdsl.protocol.proto.StorageContainerDatanodeProtocolProtos.ReportState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.hadoop.ozone.OzoneConsts.INVALID_PORT;
import static org.apache.hadoop.hdsl.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ReportState.states
    .noContainerReports;

/**
 * Current Context of State Machine.
 */
public class StateContext {
  static final Logger LOG =
      LoggerFactory.getLogger(StateContext.class);
  private final Queue<SCMCommand> commandQueue;
  private final Lock lock;
  private final DatanodeStateMachine parent;
  private final AtomicLong stateExecutionCount;
  private final Configuration conf;
  private DatanodeStateMachine.DatanodeStates state;
  private SCMNodeReport nrState;
  private ReportState  reportState;
  private static final ReportState DEFAULT_REPORT_STATE =
      ReportState.newBuilder().setState(noContainerReports).setCount(0).build();

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
    lock = new ReentrantLock();
    stateExecutionCount = new AtomicLong(0);
    nrState = SCMNodeReport.getDefaultInstance();
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
   * Returns the node report of the datanode state context.
   * @return the node report.
   */
  public SCMNodeReport getNodeReport() {
    return nrState;
  }

  /**
   * Sets the storage location report of the datanode state context.
   * @param nrReport - node report
   */
  public void setReportState(SCMNodeReport nrReport) {
    this.nrState = nrReport;
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
      this.clearReportState();
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
  }

  /**
   * Returns the count of the Execution.
   * @return long
   */
  public long getExecutionCount() {
    return stateExecutionCount.get();
  }


  /**
   * Gets the ReportState.
   * @return ReportState.
   */
  public synchronized  ReportState getContainerReportState() {
    if (reportState == null) {
      return DEFAULT_REPORT_STATE;
    }
    return reportState;
  }

  /**
   * Sets the ReportState.
   * @param rState - ReportState.
   */
  public synchronized  void setContainerReportState(ReportState rState) {
    this.reportState = rState;
  }

  /**
   * Clears report state after it has been communicated.
   */
  public synchronized void clearReportState() {
    if(reportState != null) {
      setContainerReportState(null);
    }
  }

}
