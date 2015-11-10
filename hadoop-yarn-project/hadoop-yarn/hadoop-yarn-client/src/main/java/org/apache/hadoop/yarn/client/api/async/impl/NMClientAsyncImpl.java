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

package org.apache.hadoop.yarn.client.api.async.impl;

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
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.impl.NMClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

@Private
@Unstable
public class NMClientAsyncImpl extends NMClientAsync {

  private static final Log LOG = LogFactory.getLog(NMClientAsyncImpl.class);

  protected static final int INITIAL_THREAD_POOL_SIZE = 10;

  protected ThreadPoolExecutor threadPool;
  protected int maxThreadPoolSize;
  protected Thread eventDispatcherThread;
  protected AtomicBoolean stopped = new AtomicBoolean(false);
  protected BlockingQueue<ContainerEvent> events =
      new LinkedBlockingQueue<ContainerEvent>();

  protected ConcurrentMap<ContainerId, StatefulContainer> containers =
      new ConcurrentHashMap<ContainerId, StatefulContainer>();

  public NMClientAsyncImpl(AbstractCallbackHandler callbackHandler) {
    this(NMClientAsync.class.getName(), callbackHandler);
  }

  public NMClientAsyncImpl(
      String name, AbstractCallbackHandler callbackHandler) {
    this(name, new NMClientImpl(), callbackHandler);
  }

  @Private
  @VisibleForTesting
  protected NMClientAsyncImpl(String name, NMClient client,
      AbstractCallbackHandler callbackHandler) {
    super(name, client, callbackHandler);
    this.client = client;
    this.callbackHandler = callbackHandler;
  }

  /**
   * @deprecated Use {@link
   *             #NMClientAsyncImpl(NMClientAsync.AbstractCallbackHandler)}
   *             instead.
   */
  @Deprecated
  public NMClientAsyncImpl(CallbackHandler callbackHandler) {
    this(NMClientAsync.class.getName(), callbackHandler);
  }

  /**
   * @deprecated Use {@link #NMClientAsyncImpl(String,
   *             NMClientAsync.AbstractCallbackHandler)} instead.
   */
  @Deprecated
  public NMClientAsyncImpl(String name, CallbackHandler callbackHandler) {
    this(name, new NMClientImpl(), callbackHandler);
  }

  @Private
  @VisibleForTesting
  @Deprecated
  protected NMClientAsyncImpl(String name, NMClient client,
      CallbackHandler callbackHandler) {
    super(name, client, callbackHandler);
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
          ((NMClientImpl) client).getCleanupRunningContainers().get()) {
        if (containers != null) {
          containers.clear();
        }
      }
      client.stop();
    }
    super.serviceStop();
  }

  public void startContainerAsync(
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

  public void increaseContainerResourceAsync(Container container) {
    if (!(callbackHandler instanceof AbstractCallbackHandler)) {
      LOG.error("Callback handler does not implement container resource "
              + "increase callback methods");
      return;
    }
    AbstractCallbackHandler handler = (AbstractCallbackHandler) callbackHandler;
    if (containers.get(container.getId()) == null) {
      handler.onIncreaseContainerResourceError(
          container.getId(),
          RPCUtil.getRemoteException(
              "Container " + container.getId() +
                  " is neither started nor scheduled to start"));
    }
    try {
      events.put(new IncreaseContainerResourceEvent(container));
    } catch (InterruptedException e) {
      LOG.warn("Exception when scheduling the event of increasing resource of "
          + "Container " + container.getId());
      handler.onIncreaseContainerResourceError(container.getId(), e);
    }
  }

  public void stopContainerAsync(ContainerId containerId, NodeId nodeId) {
    if (containers.get(containerId) == null) {
      callbackHandler.onStopContainerError(containerId,
          RPCUtil.getRemoteException("Container " + containerId +
              " is neither started nor scheduled to start"));
    }
    try {
      events.put(new ContainerEvent(containerId, nodeId, null,
          ContainerEventType.STOP_CONTAINER));
    } catch (InterruptedException e) {
      LOG.warn("Exception when scheduling the event of stopping Container " +
          containerId);
      callbackHandler.onStopContainerError(containerId, e);
    }
  }

 public void getContainerStatusAsync(ContainerId containerId, NodeId nodeId) {
    try {
      events.put(new ContainerEvent(containerId, nodeId, null,
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
    QUERY_CONTAINER,
    INCREASE_CONTAINER_RESOURCE
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

  protected static class IncreaseContainerResourceEvent extends ContainerEvent {
    private Container container;

    public IncreaseContainerResourceEvent(Container container) {
      super(container.getId(), container.getNodeId(),
          container.getContainerToken(),
              ContainerEventType.INCREASE_CONTAINER_RESOURCE);
      this.container = container;
    }

    public Container getContainer() {
      return container;
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
            .addTransition(ContainerState.RUNNING, ContainerState.RUNNING,
                ContainerEventType.INCREASE_CONTAINER_RESOURCE,
                new IncreaseContainerResourceTransition())
            .addTransition(ContainerState.RUNNING,
                EnumSet.of(ContainerState.DONE, ContainerState.FAILED),
                ContainerEventType.STOP_CONTAINER,
                new StopContainerTransition())

            // Transition from DONE state
            .addTransition(ContainerState.DONE, ContainerState.DONE,
                EnumSet.of(ContainerEventType.START_CONTAINER,
                    ContainerEventType.STOP_CONTAINER,
                    ContainerEventType.INCREASE_CONTAINER_RESOURCE))

            // Transition from FAILED state
            .addTransition(ContainerState.FAILED, ContainerState.FAILED,
                EnumSet.of(ContainerEventType.START_CONTAINER,
                    ContainerEventType.STOP_CONTAINER,
                    ContainerEventType.INCREASE_CONTAINER_RESOURCE));

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
              container.nmClientAsync.getClient().startContainer(
                  scEvent.getContainer(), scEvent.getContainerLaunchContext());
          try {
            container.nmClientAsync.getCallbackHandler().onContainerStarted(
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
          container.nmClientAsync.getCallbackHandler().onStartContainerError(
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

    protected static class IncreaseContainerResourceTransition implements
        SingleArcTransition<StatefulContainer, ContainerEvent> {
      @Override
      public void transition(
          StatefulContainer container, ContainerEvent event) {
        if (!(container.nmClientAsync.getCallbackHandler()
            instanceof AbstractCallbackHandler)) {
          LOG.error("Callback handler does not implement container resource "
              + "increase callback methods");
          return;
        }
        AbstractCallbackHandler handler =
            (AbstractCallbackHandler) container.nmClientAsync
                .getCallbackHandler();
        try {
          if (!(event instanceof IncreaseContainerResourceEvent)) {
            throw new AssertionError("Unexpected event type. Expecting:"
                + "IncreaseContainerResourceEvent. Got:" + event);
          }
          IncreaseContainerResourceEvent increaseEvent =
              (IncreaseContainerResourceEvent) event;
          container.nmClientAsync.getClient().increaseContainerResource(
              increaseEvent.getContainer());
          try {
            handler.onContainerResourceIncreased(
                increaseEvent.getContainerId(), increaseEvent.getContainer()
                    .getResource());
          } catch (Throwable thr) {
            // Don't process user created unchecked exception
            LOG.info("Unchecked exception is thrown from "
                + "onContainerResourceIncreased for Container "
                + event.getContainerId(), thr);
          }
        } catch (Exception e) {
          try {
            handler.onIncreaseContainerResourceError(event.getContainerId(), e);
          } catch (Throwable thr) {
            // Don't process user created unchecked exception
            LOG.info("Unchecked exception is thrown from "
                + "onIncreaseContainerResourceError for Container "
                + event.getContainerId(), thr);
          }
        }
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
         container.nmClientAsync.getClient().stopContainer(
              containerId, event.getNodeId());
         try {
            container.nmClientAsync.getCallbackHandler().onContainerStopped(
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
          container.nmClientAsync.getCallbackHandler().onStopContainerError(
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
          container.nmClientAsync.getCallbackHandler().onStartContainerError(
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
        } catch (InvalidStateTransitionException e) {
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
              containerId, event.getNodeId());
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

}
