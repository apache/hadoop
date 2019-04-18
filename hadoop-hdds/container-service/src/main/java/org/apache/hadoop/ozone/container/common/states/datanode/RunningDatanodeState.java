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
package org.apache.hadoop.ozone.container.common.states.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.states.DatanodeState;
import org.apache.hadoop.ozone.container.common.states.endpoint.HeartbeatEndpointTask;
import org.apache.hadoop.ozone.container.common.states.endpoint.RegisterEndpointTask;
import org.apache.hadoop.ozone.container.common.states.endpoint.VersionEndpointTask;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Class that implements handshake with SCM.
 */
public class RunningDatanodeState implements DatanodeState {
  static final Logger
      LOG = LoggerFactory.getLogger(RunningDatanodeState.class);
  private final SCMConnectionManager connectionManager;
  private final Configuration conf;
  private final StateContext context;
  private CompletionService<EndpointStateMachine.EndPointStates> ecs;

  public RunningDatanodeState(Configuration conf,
      SCMConnectionManager connectionManager,
      StateContext context) {
    this.connectionManager = connectionManager;
    this.conf = conf;
    this.context = context;
  }

  /**
   * Called before entering this state.
   */
  @Override
  public void onEnter() {
    LOG.trace("Entering handshake task.");
  }

  /**
   * Called After exiting this state.
   */
  @Override
  public void onExit() {
    LOG.trace("Exiting handshake task.");
  }

  /**
   * Executes one or more tasks that is needed by this state.
   *
   * @param executor -  ExecutorService
   */
  @Override
  public void execute(ExecutorService executor) {
    ecs = new ExecutorCompletionService<>(executor);
    for (EndpointStateMachine endpoint : connectionManager.getValues()) {
      Callable<EndpointStateMachine.EndPointStates> endpointTask
          = getEndPointTask(endpoint);
      if (endpointTask != null) {
        ecs.submit(endpointTask);
      } else {
        // This can happen if a task is taking more time than the timeOut
        // specified for the task in await, and when it is completed the task
        // has set the state to Shutdown, we may see the state as shutdown
        // here. So, we need to Shutdown DatanodeStateMachine.
        LOG.error("State is Shutdown in RunningDatanodeState");
        context.setState(DatanodeStateMachine.DatanodeStates.SHUTDOWN);
      }
    }
  }
  //TODO : Cache some of these tasks instead of creating them
  //all the time.
  private Callable<EndpointStateMachine.EndPointStates>
      getEndPointTask(EndpointStateMachine endpoint) {
    switch (endpoint.getState()) {
    case GETVERSION:
      return new VersionEndpointTask(endpoint, conf, context.getParent()
          .getContainer());
    case REGISTER:
      return  RegisterEndpointTask.newBuilder()
          .setConfig(conf)
          .setEndpointStateMachine(endpoint)
          .setContext(context)
          .setDatanodeDetails(context.getParent().getDatanodeDetails())
          .setOzoneContainer(context.getParent().getContainer())
          .build();
    case HEARTBEAT:
      return HeartbeatEndpointTask.newBuilder()
          .setConfig(conf)
          .setEndpointStateMachine(endpoint)
          .setDatanodeDetails(context.getParent().getDatanodeDetails())
          .setContext(context)
          .build();
    case SHUTDOWN:
      break;
    default:
      throw new IllegalArgumentException("Illegal Argument.");
    }
    return null;
  }

  /**
   * Computes the next state the container state machine must move to by looking
   * at all the state of endpoints.
   * <p>
   * if any endpoint state has moved to Shutdown, either we have an
   * unrecoverable error or we have been told to shutdown. Either case the
   * datanode state machine should move to Shutdown state, otherwise we
   * remain in the Running state.
   *
   * @return next container state.
   */
  private DatanodeStateMachine.DatanodeStates
      computeNextContainerState(
      List<Future<EndpointStateMachine.EndPointStates>> results) {
    for (Future<EndpointStateMachine.EndPointStates> state : results) {
      try {
        if (state.get() == EndpointStateMachine.EndPointStates.SHUTDOWN) {
          // if any endpoint tells us to shutdown we move to shutdown state.
          return DatanodeStateMachine.DatanodeStates.SHUTDOWN;
        }
      } catch (InterruptedException | ExecutionException e) {
        LOG.error("Error in executing end point task.", e);
      }
    }
    return DatanodeStateMachine.DatanodeStates.RUNNING;
  }

  /**
   * Wait for execute to finish.
   *
   * @param duration - Time
   * @param timeUnit - Unit of duration.
   */
  @Override
  public DatanodeStateMachine.DatanodeStates
      await(long duration, TimeUnit timeUnit)
      throws InterruptedException, ExecutionException, TimeoutException {
    int count = connectionManager.getValues().size();
    int returned = 0;
    long timeLeft = timeUnit.toMillis(duration);
    long startTime = Time.monotonicNow();
    List<Future<EndpointStateMachine.EndPointStates>> results = new
        LinkedList<>();

    while (returned < count && timeLeft > 0) {
      Future<EndpointStateMachine.EndPointStates> result =
          ecs.poll(timeLeft, TimeUnit.MILLISECONDS);
      if (result != null) {
        results.add(result);
        returned++;
      }
      timeLeft = timeLeft - (Time.monotonicNow() - startTime);
    }
    return computeNextContainerState(results);
  }
}
