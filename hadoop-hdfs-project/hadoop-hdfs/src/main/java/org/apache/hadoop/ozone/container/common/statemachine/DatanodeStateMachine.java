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
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneClientUtils;
import org.apache.hadoop.ozone.container.common.statemachine.commandhandler.CommandDispatcher;
import org.apache.hadoop.ozone.container.common.statemachine.commandhandler.ContainerReportHandler;
import org.apache.hadoop.ozone.container.common.statemachine.commandhandler.DeleteBlocksCommandHandler;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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
  private DatanodeID datanodeID = null;
  private final CommandDispatcher commandDispatcher;
  private long commandsHandled;
  private AtomicLong nextHB;
  private Thread stateMachineThread = null;
  private Thread cmdProcessThread = null;

  /**
   * Constructs a a datanode state machine.
   *
   * @param datanodeID - DatanodeID used to identify a datanode
   * @param conf - Configuration.
   */
  public DatanodeStateMachine(DatanodeID datanodeID,
      Configuration conf) throws IOException {
    this.conf = conf;
    executorService = HadoopExecutors.newCachedThreadPool(
                new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("Datanode State Machine Thread - %d").build());
    connectionManager = new SCMConnectionManager(conf);
    context = new StateContext(this.conf, DatanodeStates.getInitState(), this);
    heartbeatFrequency = TimeUnit.SECONDS.toMillis(
        OzoneClientUtils.getScmHeartbeatInterval(conf));
    container = new OzoneContainer(datanodeID, new OzoneConfiguration(conf));
    this.datanodeID = datanodeID;
    nextHB = new AtomicLong(Time.monotonicNow());

     // When we add new handlers just adding a new handler here should do the
     // trick.
    commandDispatcher = CommandDispatcher.newBuilder()
        .addHandler(new ContainerReportHandler())
        .addHandler(new DeleteBlocksCommandHandler(
            container.getContainerManager(), conf))
        .setConnectionManager(connectionManager)
        .setContainer(container)
        .setContext(context)
        .build();
  }

  public void setDatanodeID(DatanodeID datanodeID) {
    this.datanodeID = datanodeID;
  }

  /**
   *
   * Return DatanodeID if set, return null otherwise.
   *
   * @return datanodeID
   */
  public DatanodeID getDatanodeID() {
    return this.datanodeID;
  }

  /**
   * Returns the Connection manager for this state machine.
   *
   * @return - SCMConnectionManager.
   */
  public SCMConnectionManager getConnectionManager() {
    return connectionManager;
  }

  public OzoneContainer getContainer() {
    return this.container;
  }

  /**
   * Runs the state machine at a fixed frequency.
   */
  private void start() throws IOException {
    long now = 0;

    container.start();
    initCommandHandlerThread(conf);
    while (context.getState() != DatanodeStates.SHUTDOWN) {
      try {
        LOG.debug("Executing cycle Number : {}", context.getExecutionCount());
        nextHB.set(Time.monotonicNow() + heartbeatFrequency);
        context.setReportState(container.getNodeReport());
        context.setContainerReportState(container.getContainerReportState());
        context.execute(executorService, heartbeatFrequency,
            TimeUnit.MILLISECONDS);
        now = Time.monotonicNow();
        if (now < nextHB.get()) {
          Thread.sleep(nextHB.get() - now);
        }
      } catch (InterruptedException e) {
        // Ignore this exception.
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
    if (stateMachineThread != null) {
      stateMachineThread.interrupt();
    }
    if (cmdProcessThread != null) {
      cmdProcessThread.interrupt();
    }
    context.setState(DatanodeStates.getLastState());
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }

      if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
        LOG.error("Unable to shutdown state machine properly.");
      }
    } catch (InterruptedException e) {
      LOG.error("Error attempting to shutdown.", e);
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }

    if (connectionManager != null) {
      connectionManager.close();
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

  /**
   * Start datanode state machine as a single thread daemon.
   */
  public void startDaemon() {
    Runnable startStateMachineTask = () -> {
      try {
        start();
        LOG.info("Ozone container server started.");
      } catch (Exception ex) {
        LOG.error("Unable to start the DatanodeState Machine", ex);
      }
    };
    stateMachineThread =  new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("Datanode State Machine Thread - %d")
        .build().newThread(startStateMachineTask);
    stateMachineThread.start();
  }

  /**
   * Stop the daemon thread of the datanode state machine.
   */
  public synchronized void stopDaemon() {
    try {
      context.setState(DatanodeStates.SHUTDOWN);
      this.close();
      LOG.info("Ozone container server stopped.");
    } catch (IOException e) {
      LOG.error("Stop ozone container server failed.", e);
    }
  }

  /**
   *
   * Check if the datanode state machine daemon is stopped.
   *
   * @return True if datanode state machine daemon is stopped
   * and false otherwise.
   */
  @VisibleForTesting
  public boolean isDaemonStopped() {
    return this.executorService.isShutdown()
        && this.getContext().getExecutionCount() == 0
        && this.getContext().getState() == DatanodeStates.SHUTDOWN;
  }

  /**
   * Create a command handler thread.
   *
   * @param config
   */
  private void initCommandHandlerThread(Configuration config) {

    /**
     * Task that periodically checks if we have any outstanding commands.
     * It is assumed that commands can be processed slowly and in order.
     * This assumption might change in future. Right now due to this assumption
     * we have single command  queue process thread.
     */
    Runnable processCommandQueue = () -> {
      long now;
      while (getContext().getState() != DatanodeStates.SHUTDOWN) {
        SCMCommand command = getContext().getNextCommand();
        if (command != null) {
          commandDispatcher.handle(command);
          commandsHandled++;
        } else {
          try {
            // Sleep till the next HB + 1 second.
            now = Time.monotonicNow();
            if (nextHB.get() > now) {
              Thread.sleep((nextHB.get() - now) + 1000L);
            }
          } catch (InterruptedException e) {
            // Ignore this exception.
          }
        }
      }
    };

    // We will have only one thread for command processing in a datanode.
    cmdProcessThread = new Thread(processCommandQueue);
    cmdProcessThread.setDaemon(true);
    cmdProcessThread.setName("Command processor thread");
    cmdProcessThread.setUncaughtExceptionHandler((Thread t, Throwable e) -> {
      // Let us just restart this thread after logging a critical error.
      // if this thread is not running we cannot handle commands from SCM.
      LOG.error("Critical Error : Command processor thread encountered an " +
          "error. Thread: {}", t.toString(), e);
      cmdProcessThread.start();
    });
    cmdProcessThread.start();
  }

  /**
   * Returns the number of commands handled  by the datanode.
   * @return  count
   */
  @VisibleForTesting
  public long getCommandHandled() {
    return commandsHandled;
  }
}
