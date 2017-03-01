/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.container.common.statemachine;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.OzoneClientUtils;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * State Machine Class.
 */
public class DatanodeStateMachine implements Closeable {
  @VisibleForTesting
  static final Logger LOG =
      LoggerFactory.getLogger(DatanodeStateMachine.class);
  private final ExecutorService executorService;
  private final Configuration conf;
  private final SCMConnectionManager connectionManager;
  private final long heartbeatFrequency;
  private StateContext context;
  private final OzoneContainer container;

  /**
   * Constructs a a datanode state machine.
   *
   * @param conf - Configration.
   */
  public DatanodeStateMachine(Configuration conf) throws IOException {
    this.conf = conf;
    executorService = HadoopExecutors.newCachedThreadPool(
                new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("Datanode State Machine Thread - %d").build());
    connectionManager = new SCMConnectionManager(conf);
    context = new StateContext(this.conf, DatanodeStates.getInitState(), this);
    heartbeatFrequency = TimeUnit.SECONDS.toMillis(
        OzoneClientUtils.getScmHeartbeatInterval(conf));
    container = new OzoneContainer(conf);
  }

  /**
   * Returns the Connection manager for this state machine.
   *
   * @return - SCMConnectionManager.
   */
  public SCMConnectionManager getConnectionManager() {
    return connectionManager;
  }

  /**
   * Runs the state machine at a fixed frequency.
   */
  public void start() throws IOException {
    long now = 0;
    long nextHB = 0;
    container.start();
    while (context.getState() != DatanodeStates.SHUTDOWN) {
      try {
        LOG.debug("Executing cycle Number : {}", context.getExecutionCount());
        nextHB = Time.monotonicNow() + heartbeatFrequency;
        context.setReportState(container.getNodeReport());
        context.execute(executorService, heartbeatFrequency,
            TimeUnit.MILLISECONDS);
        now = Time.monotonicNow();
        if (now < nextHB) {
          Thread.sleep(nextHB - now);
        }
      } catch (Exception e) {
        LOG.error("Unable to finish the execution.", e);
      }
    }
  }

  /**
   * Gets the current context.
   *
   * @return StateContext
   */
  public StateContext getContext() {
    return context;
  }

  /**
   * Sets the current context.
   *
   * @param context - Context
   */
  public void setContext(StateContext context) {
    this.context = context;
  }

  /**
   * Closes this stream and releases any system resources associated with it. If
   * the stream is already closed then invoking this method has no effect.
   * <p>
   * <p> As noted in {@link AutoCloseable#close()}, cases where the close may
   * fail require careful attention. It is strongly advised to relinquish the
   * underlying resources and to internally <em>mark</em> the {@code Closeable}
   * as closed, prior to throwing the {@code IOException}.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void close() throws IOException {
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }

      if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
        LOG.error("Unable to shutdown statemachine properly.");
      }
    } catch (InterruptedException e) {
      LOG.error("Error attempting to shutdown.", e);
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }

    for (EndpointStateMachine endPoint : connectionManager.getValues()) {
      endPoint.close();
    }

    if(container != null) {
      container.stop();
    }
  }

  /**
   * States that a datanode  can be in. GetNextState will move this enum from
   * getInitState to getLastState.
   */
  public enum DatanodeStates {
    INIT(1),
    RUNNING(2),
    SHUTDOWN(3);
    private final int value;

    /**
     * Constructs ContainerStates.
     *
     * @param value  Enum Value
     */
    DatanodeStates(int value) {
      this.value = value;
    }

    /**
     * Returns the first State.
     *
     * @return First State.
     */
    public static DatanodeStates getInitState() {
      return INIT;
    }

    /**
     * The last state of endpoint states.
     *
     * @return last state.
     */
    public static DatanodeStates getLastState() {
      return SHUTDOWN;
    }

    /**
     * returns the numeric value associated with the endPoint.
     *
     * @return int.
     */
    public int getValue() {
      return value;
    }

    /**
     * Returns the next logical state that endPoint should move to. This
     * function assumes the States are sequentially numbered.
     *
     * @return NextState.
     */
    public DatanodeStates getNextState() {
      if (this.value < getLastState().getValue()) {
        int stateValue = this.getValue() + 1;
        for (DatanodeStates iter : values()) {
          if (stateValue == iter.getValue()) {
            return iter;
          }
        }
      }
      return getLastState();
    }
  }

  public static DatanodeStateMachine initStateMachine(Configuration conf)
      throws IOException {
    DatanodeStateMachine stateMachine = new DatanodeStateMachine(conf);
    Runnable startStateMachineTask = () -> {
      try {
        stateMachine.start();
      } catch (Exception ex) {
        LOG.error("Unable to start the DatanodeState Machine", ex);
      }
    };
    Thread thread =  new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("Datanode State Machine Thread - %d")
        .build().newThread(startStateMachineTask);
    thread.start();
    return stateMachine;
  }
}
