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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.junit.After;
import org.junit.Test;


public class TestNMClientAsync {

  private final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);

  private NMClientAsyncImpl asyncClient;
  private NodeId nodeId;
  private Token containerToken;

  @After
  public void teardown() {
    ServiceOperations.stop(asyncClient);
  }

  @Test (timeout = 10000)
  public void testNMClientAsync() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(YarnConfiguration.NM_CLIENT_ASYNC_THREAD_POOL_MAX_SIZE, 10);

    // Threads to run are more than the max size of the thread pool
    int expectedSuccess = 40;
    int expectedFailure = 40;

    asyncClient = new MockNMClientAsync1(expectedSuccess, expectedFailure);
    asyncClient.init(conf);
    Assert.assertEquals("The max thread pool size is not correctly set",
        10, asyncClient.maxThreadPoolSize);
    asyncClient.start();


    for (int i = 0; i < expectedSuccess + expectedFailure; ++i) {
      if (i == expectedSuccess) {
        while (!((TestCallbackHandler1) asyncClient.getCallbackHandler())
            .isAllSuccessCallsExecuted()) {
          Thread.sleep(10);
        }
        asyncClient.setClient(mockNMClient(1));
      }
      Container container = mockContainer(i);
      ContainerLaunchContext clc =
          recordFactory.newRecordInstance(ContainerLaunchContext.class);
      asyncClient.startContainerAsync(container, clc);
    }
    while (!((TestCallbackHandler1) asyncClient.getCallbackHandler())
        .isStartAndQueryFailureCallsExecuted()) {
      Thread.sleep(10);
    }
    asyncClient.setClient(mockNMClient(2));
    ((TestCallbackHandler1) asyncClient.getCallbackHandler()).path = false;
    for (int i = 0; i < expectedFailure; ++i) {
      Container container = mockContainer(
          expectedSuccess + expectedFailure + i);
      ContainerLaunchContext clc =
          recordFactory.newRecordInstance(ContainerLaunchContext.class);
      asyncClient.startContainerAsync(container, clc);
    }
    while (!((TestCallbackHandler1) asyncClient.getCallbackHandler())
        .isStopFailureCallsExecuted()) {
      Thread.sleep(10);
    }
    for (String errorMsg :
        ((TestCallbackHandler1) asyncClient.getCallbackHandler())
            .errorMsgs) {
      System.out.println(errorMsg);
    }
    Assert.assertEquals("Error occurs in CallbackHandler", 0,
        ((TestCallbackHandler1) asyncClient.getCallbackHandler())
            .errorMsgs.size());
    for (String errorMsg : ((MockNMClientAsync1) asyncClient).errorMsgs) {
      System.out.println(errorMsg);
    }
    Assert.assertEquals("Error occurs in ContainerEventProcessor", 0,
        ((MockNMClientAsync1) asyncClient).errorMsgs.size());
    // When the callback functions are all executed, the event processor threads
    // may still not terminate and the containers may still not removed.
    while (asyncClient.containers.size() > 0) {
      Thread.sleep(10);
    }
    asyncClient.stop();
    Assert.assertFalse(
        "The thread of Container Management Event Dispatcher is still alive",
        asyncClient.eventDispatcherThread.isAlive());
    Assert.assertTrue("The thread pool is not shut down",
        asyncClient.threadPool.isShutdown());
  }

  private class MockNMClientAsync1 extends NMClientAsyncImpl {
    private Set<String> errorMsgs =
        Collections.synchronizedSet(new HashSet<String>());

    protected MockNMClientAsync1(int expectedSuccess, int expectedFailure)
        throws YarnException, IOException {
      super(MockNMClientAsync1.class.getName(), mockNMClient(0),
          new TestCallbackHandler1(expectedSuccess, expectedFailure));
    }

    private class MockContainerEventProcessor extends ContainerEventProcessor {
      public MockContainerEventProcessor(ContainerEvent event) {
        super(event);
      }

      @Override
      public void run() {
        try {
          super.run();
        } catch (RuntimeException e) {
          // If the unexpected throwable comes from error callback functions, it
          // will break ContainerEventProcessor.run(). Therefore, monitor
          // the exception here
          errorMsgs.add("Unexpected throwable from callback functions should" +
              " be ignored by Container " + event.getContainerId());
        }
      }
    }

    @Override
    protected ContainerEventProcessor getContainerEventProcessor(
        ContainerEvent event) {
      return new MockContainerEventProcessor(event);
    }
  }

  private class TestCallbackHandler1
      implements NMClientAsync.CallbackHandler {

    private boolean path = true;

    private int expectedSuccess;
    private int expectedFailure;

    private AtomicInteger actualStartSuccess = new AtomicInteger(0);
    private AtomicInteger actualStartFailure = new AtomicInteger(0);
    private AtomicInteger actualQuerySuccess = new AtomicInteger(0);
    private AtomicInteger actualQueryFailure = new AtomicInteger(0);
    private AtomicInteger actualStopSuccess = new AtomicInteger(0);
    private AtomicInteger actualStopFailure = new AtomicInteger(0);

    private AtomicIntegerArray actualStartSuccessArray;
    private AtomicIntegerArray actualStartFailureArray;
    private AtomicIntegerArray actualQuerySuccessArray;
    private AtomicIntegerArray actualQueryFailureArray;
    private AtomicIntegerArray actualStopSuccessArray;
    private AtomicIntegerArray actualStopFailureArray;

    private Set<String> errorMsgs =
        Collections.synchronizedSet(new HashSet<String>());

    public TestCallbackHandler1(int expectedSuccess, int expectedFailure) {
      this.expectedSuccess = expectedSuccess;
      this.expectedFailure = expectedFailure;

      actualStartSuccessArray = new AtomicIntegerArray(expectedSuccess);
      actualStartFailureArray = new AtomicIntegerArray(expectedFailure);
      actualQuerySuccessArray = new AtomicIntegerArray(expectedSuccess);
      actualQueryFailureArray = new AtomicIntegerArray(expectedFailure);
      actualStopSuccessArray = new AtomicIntegerArray(expectedSuccess);
      actualStopFailureArray = new AtomicIntegerArray(expectedFailure);
    }

    @Override
    public void onContainerStarted(ContainerId containerId,
        Map<String, ByteBuffer> allServiceResponse) {
      if (path) {
        if (containerId.getId() >= expectedSuccess) {
          errorMsgs.add("Container " + containerId +
              " should throw the exception onContainerStarted");
          return;
        }
        actualStartSuccess.addAndGet(1);
        actualStartSuccessArray.set(containerId.getId(), 1);

        // move on to the following success tests
        asyncClient.getContainerStatusAsync(containerId, nodeId);
      } else {
        // move on to the following failure tests
        asyncClient.stopContainerAsync(containerId, nodeId);
      }

      // Shouldn't crash the test thread
      throw new RuntimeException("Ignorable Exception");
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId,
        ContainerStatus containerStatus) {
      if (containerId.getId() >= expectedSuccess) {
        errorMsgs.add("Container " + containerId +
            " should throw the exception onContainerStatusReceived");
        return;
      }
      actualQuerySuccess.addAndGet(1);
      actualQuerySuccessArray.set(containerId.getId(), 1);
      // move on to the following success tests
      asyncClient.stopContainerAsync(containerId, nodeId);

      // Shouldn't crash the test thread
      throw new RuntimeException("Ignorable Exception");
    }

    @Override
    public void onContainerStopped(ContainerId containerId) {
      if (containerId.getId() >= expectedSuccess) {
        errorMsgs.add("Container " + containerId +
            " should throw the exception onContainerStopped");
        return;
      }
      actualStopSuccess.addAndGet(1);
      actualStopSuccessArray.set(containerId.getId(), 1);

      // Shouldn't crash the test thread
      throw new RuntimeException("Ignorable Exception");
    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
      // If the unexpected throwable comes from success callback functions, it
      // will be handled by the error callback functions. Therefore, monitor
      // the exception here
      if (t instanceof RuntimeException) {
        errorMsgs.add("Unexpected throwable from callback functions should be" +
            " ignored by Container " + containerId);
      }
      if (containerId.getId() < expectedSuccess) {
        errorMsgs.add("Container " + containerId +
            " shouldn't throw the exception onStartContainerError");
        return;
      }
      actualStartFailure.addAndGet(1);
      actualStartFailureArray.set(containerId.getId() - expectedSuccess, 1);
      // move on to the following failure tests
      asyncClient.getContainerStatusAsync(containerId, nodeId);

      // Shouldn't crash the test thread
      throw new RuntimeException("Ignorable Exception");
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {
      if (t instanceof RuntimeException) {
        errorMsgs.add("Unexpected throwable from callback functions should be" +
            " ignored by Container " + containerId);
      }
      if (containerId.getId() < expectedSuccess + expectedFailure) {
        errorMsgs.add("Container " + containerId +
            " shouldn't throw the exception onStopContainerError");
        return;
      }

      actualStopFailure.addAndGet(1);
      actualStopFailureArray.set(
          containerId.getId() - expectedSuccess - expectedFailure, 1);

      // Shouldn't crash the test thread
      throw new RuntimeException("Ignorable Exception");
    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId,
        Throwable t) {
      if (t instanceof RuntimeException) {
        errorMsgs.add("Unexpected throwable from callback functions should be"
            + " ignored by Container " + containerId);
      }
      if (containerId.getId() < expectedSuccess) {
        errorMsgs.add("Container " + containerId +
            " shouldn't throw the exception onGetContainerStatusError");
        return;
      }
      actualQueryFailure.addAndGet(1);
      actualQueryFailureArray.set(containerId.getId() - expectedSuccess, 1);

      // Shouldn't crash the test thread
      throw new RuntimeException("Ignorable Exception");
    }

    public boolean isAllSuccessCallsExecuted() {
      boolean isAllSuccessCallsExecuted =
          actualStartSuccess.get() == expectedSuccess &&
          actualQuerySuccess.get() == expectedSuccess &&
          actualStopSuccess.get() == expectedSuccess;
      if (isAllSuccessCallsExecuted) {
        assertAtomicIntegerArray(actualStartSuccessArray);
        assertAtomicIntegerArray(actualQuerySuccessArray);
        assertAtomicIntegerArray(actualStopSuccessArray);
      }
      return isAllSuccessCallsExecuted;
    }

    public boolean isStartAndQueryFailureCallsExecuted() {
      boolean isStartAndQueryFailureCallsExecuted =
          actualStartFailure.get() == expectedFailure &&
          actualQueryFailure.get() == expectedFailure;
      if (isStartAndQueryFailureCallsExecuted) {
        assertAtomicIntegerArray(actualStartFailureArray);
        assertAtomicIntegerArray(actualQueryFailureArray);
      }
      return isStartAndQueryFailureCallsExecuted;
    }

    public boolean isStopFailureCallsExecuted() {
      boolean isStopFailureCallsExecuted =
          actualStopFailure.get() == expectedFailure;
      if (isStopFailureCallsExecuted) {
        assertAtomicIntegerArray(actualStopFailureArray);
      }
      return isStopFailureCallsExecuted;
    }

    private void assertAtomicIntegerArray(AtomicIntegerArray array) {
      for (int i = 0; i < array.length(); ++i) {
        Assert.assertEquals(1, array.get(i));
      }
    }
  }

  private NMClient mockNMClient(int mode)
      throws YarnException, IOException {
    NMClient client = mock(NMClient.class);
    switch (mode) {
      case 0:
        when(client.startContainer(any(Container.class),
            any(ContainerLaunchContext.class))).thenReturn(
                Collections.<String, ByteBuffer>emptyMap());
        when(client.getContainerStatus(any(ContainerId.class),
            any(NodeId.class))).thenReturn(
                recordFactory.newRecordInstance(ContainerStatus.class));
        doNothing().when(client).stopContainer(any(ContainerId.class),
            any(NodeId.class));
        break;
      case 1:
        doThrow(RPCUtil.getRemoteException("Start Exception")).when(client)
            .startContainer(any(Container.class),
                any(ContainerLaunchContext.class));
        doThrow(RPCUtil.getRemoteException("Query Exception")).when(client)
            .getContainerStatus(any(ContainerId.class), any(NodeId.class));
        doThrow(RPCUtil.getRemoteException("Stop Exception")).when(client)
            .stopContainer(any(ContainerId.class), any(NodeId.class));
        break;
      case 2:
        when(client.startContainer(any(Container.class),
            any(ContainerLaunchContext.class))).thenReturn(
                Collections.<String, ByteBuffer>emptyMap());
        when(client.getContainerStatus(any(ContainerId.class),
            any(NodeId.class))).thenReturn(
                recordFactory.newRecordInstance(ContainerStatus.class));
        doThrow(RPCUtil.getRemoteException("Stop Exception")).when(client)
            .stopContainer(any(ContainerId.class), any(NodeId.class));
    }
    return client;
  }

  @Test (timeout = 10000)
  public void testOutOfOrder() throws Exception {
    CyclicBarrier barrierA = new CyclicBarrier(2);
    CyclicBarrier barrierB = new CyclicBarrier(2);
    CyclicBarrier barrierC = new CyclicBarrier(2);
    asyncClient = new MockNMClientAsync2(barrierA, barrierB, barrierC);
    asyncClient.init(new Configuration());
    asyncClient.start();

    final Container container = mockContainer(1);
    final ContainerLaunchContext clc =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);

    // start container from another thread
    Thread t = new Thread() {
      @Override
      public void run() {
        asyncClient.startContainerAsync(container, clc);
      }
    };
    t.start();

    barrierA.await();
    asyncClient.stopContainerAsync(container.getId(), container.getNodeId());
    barrierC.await();

    Assert.assertFalse("Starting and stopping should be out of order",
        ((TestCallbackHandler2) asyncClient.getCallbackHandler())
            .exceptionOccurred.get());
  }

  private class MockNMClientAsync2 extends NMClientAsyncImpl {
    private CyclicBarrier barrierA;
    private CyclicBarrier barrierB;

    protected MockNMClientAsync2(CyclicBarrier barrierA, CyclicBarrier barrierB,
        CyclicBarrier barrierC) throws YarnException, IOException {
      super(MockNMClientAsync2.class.getName(), mockNMClient(0),
          new TestCallbackHandler2(barrierC));
      this.barrierA = barrierA;
      this.barrierB = barrierB;
    }

    private class MockContainerEventProcessor extends ContainerEventProcessor {

      public MockContainerEventProcessor(ContainerEvent event) {
        super(event);
      }

      @Override
      public void run() {
        try {
          if (event.getType() == ContainerEventType.START_CONTAINER) {
            barrierA.await();
            barrierB.await();
          }
          super.run();
          if (event.getType() == ContainerEventType.STOP_CONTAINER) {
            barrierB.await();
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (BrokenBarrierException e) {
          e.printStackTrace();
        }
      }
    }

    @Override
    protected ContainerEventProcessor getContainerEventProcessor(
        ContainerEvent event) {
      return new MockContainerEventProcessor(event);
    }
  }

  private class TestCallbackHandler2
      implements NMClientAsync.CallbackHandler {
    private CyclicBarrier barrierC;
    private AtomicBoolean exceptionOccurred = new AtomicBoolean(false);

    public TestCallbackHandler2(CyclicBarrier barrierC) {
      this.barrierC = barrierC;
    }

    @Override
    public void onContainerStarted(ContainerId containerId,
        Map<String, ByteBuffer> allServiceResponse) {
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId,
        ContainerStatus containerStatus) {
    }

    @Override
    public void onContainerStopped(ContainerId containerId) {
    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
      if (!t.getMessage().equals(NMClientAsyncImpl.StatefulContainer
          .OutOfOrderTransition.STOP_BEFORE_START_ERROR_MSG)) {
        exceptionOccurred.set(true);
        return;
      }
      try {
        barrierC.await();
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (BrokenBarrierException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId,
        Throwable t) {
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {
    }

  }

  private Container mockContainer(int i) {
    ApplicationId appId =
        ApplicationId.newInstance(System.currentTimeMillis(), 1);
    ApplicationAttemptId attemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId containerId = ContainerId.newInstance(attemptId, i);
    nodeId = NodeId.newInstance("localhost", 0);
    // Create an empty record
    containerToken = recordFactory.newRecordInstance(Token.class);
    return Container.newInstance(containerId, nodeId, null, null, null,
      containerToken);
  }
}
