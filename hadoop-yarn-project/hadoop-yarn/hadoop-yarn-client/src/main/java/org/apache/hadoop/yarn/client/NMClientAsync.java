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

package org.apache.hadoop.yarn.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * <code>NMClientAsync</code> handles communication with all the NodeManagers
 * and provides asynchronous updates on getting responses from them. It
 * maintains a thread pool to communicate with individual NMs where a number of
 * worker threads process requests to NMs by using {@link NMClientImpl}. The max
 * size of the thread pool is configurable through
 * {@link YarnConfiguration#NM_CLIENT_ASYNC_THREAD_POOL_MAX_SIZE}.
 *
 * It should be used in conjunction with a CallbackHandler. For example
 *
 * <pre>
 * {@code
 * class MyCallbackHandler implements NMClientAsync.CallbackHandler {
 *   public void onContainerStarted(ContainerId containerId,
 *       Map<String, ByteBuffer> allServiceResponse) {
 *     [post process after the container is started, process the response]
 *   }
 *
 *   public void onContainerStatusReceived(ContainerId containerId,
 *       ContainerStatus containerStatus) {
 *     [make use of the status of the container]
 *   }
 *
 *   public void onContainerStopped(ContainerId containerId) {
 *     [post process after the container is stopped]
 *   }
 *
 *   public void onStartContainerError(
 *       ContainerId containerId, Throwable t) {
 *     [handle the raised exception]
 *   }
 *
 *   public void onGetContainerStatusError(
 *       ContainerId containerId, Throwable t) {
 *     [handle the raised exception]
 *   }
 *
 *   public void onStopContainerError(
 *       ContainerId containerId, Throwable t) {
 *     [handle the raised exception]
 *   }
 * }
 * }
 * </pre>
 *
 * The client's life-cycle should be managed like the following:
 *
 * <pre>
 * {@code
 * NMClientAsync asyncClient = new NMClientAsync(new MyCallbackhandler());
 * asyncClient.init(conf);
 * asyncClient.start();
 * asyncClient.startContainer(container, containerLaunchContext);
 * [... wait for container being started]
 * asyncClient.getContainerStatus(container.getId(), container.getNodeId(),
 *     container.getContainerToken());
 * [... handle the status in the callback instance]
 * asyncClient.stopContainer(container.getId(), container.getNodeId(),
 *     container.getContainerToken());
 * [... wait for container being stopped]
 * asyncClient.stop();
 * }
 * </pre>
 */
@Unstable
@Evolving
public class NMClientAsync extends AbstractService {

  private static final Log LOG = LogFactory.getLog(NMClientAsync.class);

  protected static final int INITIAL_THREAD_POOL_SIZE = 10;

  protected ThreadPoolExecutor threadPool;
  protected int maxThreadPoolSize;
  protected Thread eventDispatcherThread;
  protected AtomicBoolean stopped = new AtomicBoolean(false);
  protected BlockingQueue<ContainerEvent> events =
      new LinkedBlockingQueue<ContainerEvent>();

  protected NMClient client;
  protected CallbackHandler callbackHandler;

  protected ConcurrentMap<ContainerId, StatefulContainer> containers =
      new ConcurrentHashMap<ContainerId, StatefulContainer>();

  public NMClientAsync(CallbackHandler callbackHandler) {
    this (NMClientAsync.class.getName(), callbackHandler);
  }

  public NMClientAsync(String name, CallbackHandler callbackHandler) {
    this (name, new NMClientImpl(), callbackHandler);
  }

  @Private
  @VisibleForTesting
  protected NMClientAsync(String name, NMClient client,
      CallbackHandler callbackHandler) {
    super(name);
    this.client = client;
    this.callbackHandler = callbackHandler;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    this.maxThreadPoolSize = conf.getInt(
        YarnConfiguration.NM_CLIENT_ASYNC_THREAD_POOL_MAX_SIZE,
        YarnConfiguration.DEFAULT_NM_CLIENT_ASYNC_THREAD_POOL_MAX_SIZE);
    LOG.info("Upper bound of the thread pool size is " + maxThreadPoolSize);

    client.init(conf);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    client.start();

    ThreadFactory tf = new ThreadFactoryBuilder().setNameFormat(
        this.getClass().getName() + " #%d").setDaemon(true).build();

    // Start with a default core-pool size and change it dynamically.
    int initSize = Math.min(INITIAL_THREAD_POOL_SIZE, maxThreadPoolSize);
    threadPool = new ThreadPoolExecutor(initSize, Integer.MAX_VALUE, 1,
        TimeUnit.HOURS, new LinkedBlockingQueue<Runnable>(), tf);

    eventDispatcherThread = new Thread() {
      @Override
      public void run() {
        ContainerEvent event = null;
        Set<String> allNodes = new HashSet<String>();

        while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
          try {
            event = events.take();
          } catch (InterruptedException e) {
            if (!stopped.get()) {
              LOG.error("Returning, thread interrupted", e);
            }
            return;
          }

          allNodes.add(event.getNodeId().toString());

          int threadPoolSize = threadPool.getCorePoolSize();

          // We can increase the pool size only if haven't reached the maximum
          // limit yet.
          if (threadPoolSize != maxThreadPoolSize) {

            // nodes where containers will run at *this* point of time. This is
            // *not* the cluster size and doesn't need to be.
            int nodeNum = allNodes.size();
            int idealThreadPoolSize = Math.min(maxThreadPoolSize, nodeNum);

            if (threadPoolSize < idealThreadPoolSize) {
              // Bump up the pool size to idealThreadPoolSize +
              // INITIAL_POOL_SIZE, the later is just a buffer so we are not
              // always increasing the pool-size
              int newThreadPoolSize = Math.min(maxThreadPoolSize,
                  idealThreadPoolSize + INITIAL_THREAD_POOL_SIZE);
              LOG.info("Set NMClientAsync thread pool size to " +
                  newThreadPoolSize + " as the number of nodes to talk to is "
                  + nodeNum);
              threadPool.setCorePoolSize(newThreadPoolSize);
            }
          }

          // the events from the queue are handled in parallel with a thread
          // pool
          threadPool.execute(getContainerEventProcessor(event));

          // TODO: Group launching of multiple containers to a single
          // NodeManager into a single connection
        }
      }
    };
    eventDispatcherThread.setName("Container  Event Dispatcher");
    eventDispatcherThread.setDaemon(false);
    eventDispatcherThread.start();

    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (stopped.getAndSet(true)) {
      // return if already stopped
      return;
    }
    if (eventDispatcherThread != null) {
      eventDispatcherThread.interrupt();
      try {
        eventDispatcherThread.join();
      } catch (InterruptedException e) {
        LOG.error("The thread of " + eventDispatcherThread.getName() +
                  " didn't finish normally.", e);
      }
    }
    if (threadPool != null) {
      threadPool.shutdownNow();
    }
    if (client != null) {
      // If NMClientImpl doesn't stop running containers, the states doesn't
      // need to be cleared.
      if (!(client instanceof NMClientImpl) ||
          ((NMClientImpl) client).cleanupRunningContainers.get()) {
        if (containers != null) {
          containers.clear();
        }
      }
      client.stop();
    }
    super.serviceStop();
  }

  public void startContainer(
      Container container, ContainerLaunchContext containerLaunchContext) {
    if (containers.putIfAbsent(container.getId(),
        new StatefulContainer(this, container.getId())) != null) {
      callbackHandler.onStartContainerError(container.getId(),
          RPCUtil.getRemoteException("Container " + container.getId() +
              " is already started or scheduled to start"));
    }
    try {
      events.put(new StartContainerEvent(container, containerLaunchContext));
    } catch (InterruptedException e) {
      LOG.warn("Exception when scheduling the event of starting Container " +
          container.getId());
      callbackHandler.onStartContainerError(container.getId(), e);
    }
  }

  public void stopContainer(ContainerId containerId, NodeId nodeId,
      Token containerToken) {
    if (containers.get(containerId) == null) {
      callbackHandler.onStopContainerError(containerId,
          RPCUtil.getRemoteException("Container " + containerId +
              " is neither started nor scheduled to start"));
    }
    try {
      events.put(new ContainerEvent(containerId, nodeId, containerToken,
          ContainerEventType.STOP_CONTAINER));
    } catch (InterruptedException e) {
      LOG.warn("Exception when scheduling the event of stopping Container " +
          containerId);
      callbackHandler.onStopContainerError(containerId, e);
    }
  }

  public void getContainerStatus(ContainerId containerId, NodeId nodeId,
      Token containerToken) {
    try {
      events.put(new ContainerEvent(containerId, nodeId, containerToken,
          ContainerEventType.QUERY_CONTAINER));
    } catch (InterruptedException e) {
      LOG.warn("Exception when scheduling the event of querying the status" +
          " of Container " + containerId);
      callbackHandler.onGetContainerStatusError(containerId, e);
    }
  }

  protected static enum ContainerState {
    PREP, FAILED, RUNNING, DONE,
  }

  protected boolean isCompletelyDone(StatefulContainer container) {
    return container.getState() == ContainerState.DONE ||
        container.getState() == ContainerState.FAILED;
  }

  protected ContainerEventProcessor getContainerEventProcessor(
      ContainerEvent event) {
    return new ContainerEventProcessor(event);
  }

  /**
   * The type of the event of interacting with a container
   */
  protected static enum ContainerEventType {
    START_CONTAINER,
    STOP_CONTAINER,
    QUERY_CONTAINER
  }

  protected static class ContainerEvent
      extends AbstractEvent<ContainerEventType>{
    private ContainerId containerId;
    private NodeId nodeId;
    private Token containerToken;

    public ContainerEvent(ContainerId containerId, NodeId nodeId,
        Token containerToken, ContainerEventType type) {
      super(type);
      this.containerId = containerId;
      this.nodeId = nodeId;
      this.containerToken = containerToken;
    }

    public ContainerId getContainerId() {
      return containerId;
    }

    public NodeId getNodeId() {
      return nodeId;
    }

    public Token getContainerToken() {
      return containerToken;
    }
  }

  protected static class StartContainerEvent extends ContainerEvent {
    private Container container;
    private ContainerLaunchContext containerLaunchContext;

    public StartContainerEvent(Container container,
        ContainerLaunchContext containerLaunchContext) {
      super(container.getId(), container.getNodeId(),
          container.getContainerToken(), ContainerEventType.START_CONTAINER);
      this.container = container;
      this.containerLaunchContext = containerLaunchContext;
    }

    public Container getContainer() {
      return container;
    }

    public ContainerLaunchContext getContainerLaunchContext() {
      return containerLaunchContext;
    }
  }

  protected static class StatefulContainer implements
      EventHandler<ContainerEvent> {

    protected final static StateMachineFactory<StatefulContainer,
        ContainerState, ContainerEventType, ContainerEvent> stateMachineFactory
            = new StateMachineFactory<StatefulContainer, ContainerState,
                ContainerEventType, ContainerEvent>(ContainerState.PREP)

            // Transitions from PREP state
            .addTransition(ContainerState.PREP,
                EnumSet.of(ContainerState.RUNNING, ContainerState.FAILED),
                ContainerEventType.START_CONTAINER,
                new StartContainerTransition())
            .addTransition(ContainerState.PREP, ContainerState.DONE,
                ContainerEventType.STOP_CONTAINER, new OutOfOrderTransition())

            // Transitions from RUNNING state
            // RUNNING -> RUNNING should be the invalid transition
            .addTransition(ContainerState.RUNNING,
                EnumSet.of(ContainerState.DONE, ContainerState.FAILED),
                ContainerEventType.STOP_CONTAINER,
                new StopContainerTransition())

            // Transition from DONE state
            .addTransition(ContainerState.DONE, ContainerState.DONE,
                EnumSet.of(ContainerEventType.START_CONTAINER,
                    ContainerEventType.STOP_CONTAINER))

            // Transition from FAILED state
            .addTransition(ContainerState.FAILED, ContainerState.FAILED,
                EnumSet.of(ContainerEventType.START_CONTAINER,
                    ContainerEventType.STOP_CONTAINER));

    protected static class StartContainerTransition implements
        MultipleArcTransition<StatefulContainer, ContainerEvent,
        ContainerState> {

      @Override
      public ContainerState transition(
          StatefulContainer container, ContainerEvent event) {
        ContainerId containerId = event.getContainerId();
        try {
          StartContainerEvent scEvent = null;
          if (event instanceof StartContainerEvent) {
            scEvent = (StartContainerEvent) event;
          }
          assert scEvent != null;
          Map<String, ByteBuffer> allServiceResponse =
              container.nmClientAsync.client.startContainer(
                  scEvent.getContainer(), scEvent.getContainerLaunchContext());
          try {
            container.nmClientAsync.callbackHandler.onContainerStarted(
                containerId, allServiceResponse);
          } catch (Throwable thr) {
            // Don't process user created unchecked exception
            LOG.info("Unchecked exception is thrown from onContainerStarted for "
                + "Container " + containerId, thr);
          }
          return ContainerState.RUNNING;
        } catch (YarnException e) {
          return onExceptionRaised(container, event, e);
        } catch (IOException e) {
          return onExceptionRaised(container, event, e);
        } catch (Throwable t) {
          return onExceptionRaised(container, event, t);
        }
      }

      private ContainerState onExceptionRaised(StatefulContainer container,
          ContainerEvent event, Throwable t) {
        try {
          container.nmClientAsync.callbackHandler.onStartContainerError(
              event.getContainerId(), t);
        } catch (Throwable thr) {
          // Don't process user created unchecked exception
          LOG.info(
              "Unchecked exception is thrown from onStartContainerError for " +
                  "Container " + event.getContainerId(), thr);
        }
        return ContainerState.FAILED;
      }
    }

    protected static class StopContainerTransition implements
        MultipleArcTransition<StatefulContainer, ContainerEvent,
        ContainerState> {

      @Override
      public ContainerState transition(
          StatefulContainer container, ContainerEvent event) {
        ContainerId containerId = event.getContainerId();
        try {
          container.nmClientAsync.client.stopContainer(
              containerId, event.getNodeId(), event.getContainerToken());
          try {
            container.nmClientAsync.callbackHandler.onContainerStopped(
                event.getContainerId());
          } catch (Throwable thr) {
            // Don't process user created unchecked exception
            LOG.info("Unchecked exception is thrown from onContainerStopped for "
                + "Container " + event.getContainerId(), thr);
          }
          return ContainerState.DONE;
        } catch (YarnException e) {
          return onExceptionRaised(container, event, e);
        } catch (IOException e) {
          return onExceptionRaised(container, event, e);
        } catch (Throwable t) {
          return onExceptionRaised(container, event, t);
        }
      }

      private ContainerState onExceptionRaised(StatefulContainer container,
          ContainerEvent event, Throwable t) {
        try {
          container.nmClientAsync.callbackHandler.onStopContainerError(
              event.getContainerId(), t);
        } catch (Throwable thr) {
          // Don't process user created unchecked exception
          LOG.info("Unchecked exception is thrown from onStopContainerError for "
              + "Container " + event.getContainerId(), thr);
        }
        return ContainerState.FAILED;
      }
    }

    protected static class OutOfOrderTransition implements
        SingleArcTransition<StatefulContainer, ContainerEvent> {

      protected static final String STOP_BEFORE_START_ERROR_MSG =
          "Container was killed before it was launched";

      @Override
      public void transition(StatefulContainer container, ContainerEvent event) {
        try {
          container.nmClientAsync.callbackHandler.onStartContainerError(
              event.getContainerId(),
              RPCUtil.getRemoteException(STOP_BEFORE_START_ERROR_MSG));
        } catch (Throwable thr) {
          // Don't process user created unchecked exception
          LOG.info(
              "Unchecked exception is thrown from onStartContainerError for " +
                  "Container " + event.getContainerId(), thr);
        }
      }
    }

    private final NMClientAsync nmClientAsync;
    private final ContainerId containerId;
    private final StateMachine<ContainerState, ContainerEventType,
        ContainerEvent> stateMachine;
    private final ReadLock readLock;
    private final WriteLock writeLock;

    public StatefulContainer(NMClientAsync client, ContainerId containerId) {
      this.nmClientAsync = client;
      this.containerId = containerId;
      stateMachine = stateMachineFactory.make(this);
      ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
      readLock = lock.readLock();
      writeLock = lock.writeLock();
    }

    @Override
    public void handle(ContainerEvent event) {
      writeLock.lock();
      try {
        try {
          this.stateMachine.doTransition(event.getType(), event);
        } catch (InvalidStateTransitonException e) {
          LOG.error("Can't handle this event at current state", e);
        }
      } finally {
        writeLock.unlock();
      }
    }

    public ContainerId getContainerId() {
      return containerId;
    }

    public ContainerState getState() {
      readLock.lock();
      try {
        return stateMachine.getCurrentState();
      } finally {
        readLock.unlock();
      }
    }
  }

  protected class ContainerEventProcessor implements Runnable {
    protected ContainerEvent event;

    public ContainerEventProcessor(ContainerEvent event) {
      this.event = event;
    }

    @Override
    public void run() {
      ContainerId containerId = event.getContainerId();
      LOG.info("Processing Event " + event + " for Container " + containerId);
      if (event.getType() == ContainerEventType.QUERY_CONTAINER) {
        try {
          ContainerStatus containerStatus = client.getContainerStatus(
              containerId, event.getNodeId(), event.getContainerToken());
          try {
            callbackHandler.onContainerStatusReceived(
                containerId, containerStatus);
          } catch (Throwable thr) {
            // Don't process user created unchecked exception
            LOG.info(
                "Unchecked exception is thrown from onContainerStatusReceived" +
                    " for Container " + event.getContainerId(), thr);
          }
        } catch (YarnException e) {
          onExceptionRaised(containerId, e);
        } catch (IOException e) {
          onExceptionRaised(containerId, e);
        } catch (Throwable t) {
          onExceptionRaised(containerId, t);
        }
      } else {
        StatefulContainer container = containers.get(containerId);
        if (container == null) {
          LOG.info("Container " + containerId + " is already stopped or failed");
        } else {
          container.handle(event);
          if (isCompletelyDone(container)) {
            containers.remove(containerId);
          }
        }
      }
    }

    private void onExceptionRaised(ContainerId containerId, Throwable t) {
      try {
        callbackHandler.onGetContainerStatusError(containerId, t);
      } catch (Throwable thr) {
        // Don't process user created unchecked exception
        LOG.info("Unchecked exception is thrown from onGetContainerStatusError" +
            " for Container " + containerId, thr);
      }
    }
  }

  /**
   * <p>
   * The callback interface needs to be implemented by {@link NMClientAsync}
   * users. The APIs are called when responses from <code>NodeManager</code> are
   * available.
   * </p>
   *
   * <p>
   * Once a callback happens, the users can chose to act on it in blocking or
   * non-blocking manner. If the action on callback is done in a blocking
   * manner, some of the threads performing requests on NodeManagers may get
   * blocked depending on how many threads in the pool are busy.
   * </p>
   *
   * <p>
   * The implementation of the callback function should not throw the
   * unexpected exception. Otherwise, {@link NMClientAsync} will just
   * catch, log and then ignore it.
   * </p>
   */
  public static interface CallbackHandler {
    /**
     * The API is called when <code>NodeManager</code> responds to indicate its
     * acceptance of the starting container request
     * @param containerId the Id of the container
     * @param allServiceResponse a Map between the auxiliary service names and
     *                           their outputs
     */
    void onContainerStarted(ContainerId containerId,
        Map<String, ByteBuffer> allServiceResponse);

    /**
     * The API is called when <code>NodeManager</code> responds with the status
     * of the container
     * @param containerId the Id of the container
     * @param containerStatus the status of the container
     */
    void onContainerStatusReceived(ContainerId containerId,
        ContainerStatus containerStatus);

    /**
     * The API is called when <code>NodeManager</code> responds to indicate the
     * container is stopped.
     * @param containerId the Id of the container
     */
    void onContainerStopped(ContainerId containerId);

    /**
     * The API is called when an exception is raised in the process of
     * starting a container
     *
     * @param containerId the Id of the container
     * @param t the raised exception
     */
    void onStartContainerError(ContainerId containerId, Throwable t);

    /**
     * The API is called when an exception is raised in the process of
     * querying the status of a container
     *
     * @param containerId the Id of the container
     * @param t the raised exception
     */
    void onGetContainerStatusError(ContainerId containerId, Throwable t);

    /**
     * The API is called when an exception is raised in the process of
     * stopping a container
     *
     * @param containerId the Id of the container
     * @param t the raised exception
     */
    void onStopContainerError(ContainerId containerId, Throwable t);

  }

}
