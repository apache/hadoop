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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;
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

  enum OpsToTest {
    START, QUERY, STOP, INCR, REINIT, RESTART, ROLLBACK, COMMIT
  }

  final static class TestData {
    AtomicInteger success = new AtomicInteger(0);
    AtomicInteger failure = new AtomicInteger(0);
    final AtomicIntegerArray successArray;
    final AtomicIntegerArray failureArray;

    private TestData(int expectedSuccess, int expectedFailure) {
      this.successArray = new AtomicIntegerArray(expectedSuccess);
      this.failureArray = new AtomicIntegerArray(expectedFailure);
    }
  }

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
        .isIncreaseResourceFailureCallsExecuted()) {
      Thread.sleep(10);
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
      extends NMClientAsync.AbstractCallbackHandler {

    private boolean path = true;

    private int expectedSuccess;
    private int expectedFailure;

    private final Map<OpsToTest, TestData> testMap = new HashMap<>();

    private Set<String> errorMsgs =
        Collections.synchronizedSet(new HashSet<String>());

    public TestCallbackHandler1(int expectedSuccess, int expectedFailure) {
      this.expectedSuccess = expectedSuccess;
      this.expectedFailure = expectedFailure;

      for (OpsToTest op : OpsToTest.values()) {
        testMap.put(op, new TestData(expectedSuccess, expectedFailure));
      }
    }

    @SuppressWarnings("deprecation")
    @Override
    public void onContainerStarted(ContainerId containerId,
        Map<String, ByteBuffer> allServiceResponse) {
      if (path) {
        if (containerId.getId() >= expectedSuccess) {
          errorMsgs.add("Container " + containerId +
              " should throw the exception onContainerStarted");
          return;
        }
        TestData td = testMap.get(OpsToTest.START);
        td.success.addAndGet(1);
        td.successArray.set(containerId.getId(), 1);

        // move on to the following success tests
        asyncClient.getContainerStatusAsync(containerId, nodeId);
      } else {
        // move on to the following failure tests
        // make sure we pass in the container with the same
        // containerId
        Container container = Container.newInstance(
            containerId, nodeId, null, null, null, containerToken);
        int t = containerId.getId() % 5;
        switch (t) {
        case 0:
          asyncClient.updateContainerResourceAsync(container);
          break;
        case 1:
          asyncClient.reInitializeContainerAsync(containerId,
              recordFactory.newRecordInstance(ContainerLaunchContext.class),
              true);
          break;
        case 2:
          asyncClient.restartContainerAsync(containerId);
          break;
        case 3:
          asyncClient.rollbackLastReInitializationAsync(containerId);
          break;
        case 4:
          asyncClient.commitLastReInitializationAsync(containerId);
          break;
        default:
          break;
        }
      }

      // Shouldn't crash the test thread
      throw new RuntimeException("Ignorable Exception");
    }

    @SuppressWarnings("deprecation")
    @Override
    public void onContainerStatusReceived(ContainerId containerId,
        ContainerStatus containerStatus) {
      if (containerId.getId() >= expectedSuccess) {
        errorMsgs.add("Container " + containerId +
            " should throw the exception onContainerStatusReceived");
        return;
      }
      TestData td = testMap.get(OpsToTest.QUERY);
      td.success.addAndGet(1);
      td.successArray.set(containerId.getId(), 1);
      // move on to the following success tests
      // make sure we pass in the container with the same
      // containerId
      Container container = Container.newInstance(
          containerId, nodeId, null, null, null, containerToken);
      asyncClient.updateContainerResourceAsync(container);

      // Shouldn't crash the test thread
      throw new RuntimeException("Ignorable Exception");
    }

    @SuppressWarnings("deprecation")
    @Override
    public void onContainerResourceIncreased(
        ContainerId containerId, Resource resource) {
      if (containerId.getId() >= expectedSuccess) {
        errorMsgs.add("Container " + containerId +
            " should throw the exception onContainerResourceIncreased");
        return;
      }
      TestData td = testMap.get(OpsToTest.INCR);
      td.success.addAndGet(1);
      td.successArray.set(containerId.getId(), 1);
      // move on to the following success tests
      asyncClient.reInitializeContainerAsync(containerId,
          Records.newRecord(ContainerLaunchContext.class), true);
      // throw a fake user exception, and shouldn't crash the test
      throw new RuntimeException("Ignorable Exception");
    }

    @SuppressWarnings("deprecation")
    @Override
    public void onContainerResourceUpdated(ContainerId containerId,
        Resource resource) {
      if (containerId.getId() >= expectedSuccess) {
        errorMsgs.add("Container " + containerId +
            " should throw the exception onContainerResourceUpdated");
        return;
      }
      TestData td = testMap.get(OpsToTest.INCR);
      td.success.addAndGet(1);
      td.successArray.set(containerId.getId(), 1);
      // move on to the following success tests
      asyncClient.reInitializeContainerAsync(containerId,
          Records.newRecord(ContainerLaunchContext.class), true);
      // throw a fake user exception, and shouldn't crash the test
      throw new RuntimeException("Ignorable Exception");
    }

    @SuppressWarnings("deprecation")
    @Override
    public void onContainerReInitialize(ContainerId containerId) {
      if (containerId.getId() >= expectedSuccess) {
        errorMsgs.add("Container " + containerId +
            " should throw the exception onContainerReInitialize");
        return;
      }
      TestData td = testMap.get(OpsToTest.REINIT);
      td.success.addAndGet(1);
      td.successArray.set(containerId.getId(), 1);
      // move on to the following success tests
      asyncClient.restartContainerAsync(containerId);
      // throw a fake user exception, and shouldn't crash the test
      throw new RuntimeException("Ignorable Exception");
    }

    @SuppressWarnings("deprecation")
    @Override
    public void onContainerRestart(ContainerId containerId) {
      if (containerId.getId() >= expectedSuccess) {
        errorMsgs.add("Container " + containerId +
            " should throw the exception onContainerReInitialize");
        return;
      }
      TestData td = testMap.get(OpsToTest.RESTART);
      td.success.addAndGet(1);
      td.successArray.set(containerId.getId(), 1);
      // move on to the following success tests
      asyncClient.rollbackLastReInitializationAsync(containerId);
      // throw a fake user exception, and shouldn't crash the test
      throw new RuntimeException("Ignorable Exception");
    }

    @SuppressWarnings("deprecation")
    @Override
    public void onRollbackLastReInitialization(ContainerId containerId) {
      if (containerId.getId() >= expectedSuccess) {
        errorMsgs.add("Container " + containerId +
            " should throw the exception onContainerReInitialize");
        return;
      }
      TestData td = testMap.get(OpsToTest.ROLLBACK);
      td.success.addAndGet(1);
      td.successArray.set(containerId.getId(), 1);
      // move on to the following success tests
      asyncClient.commitLastReInitializationAsync(containerId);
      // throw a fake user exception, and shouldn't crash the test
      throw new RuntimeException("Ignorable Exception");
    }

    @SuppressWarnings("deprecation")
    @Override
    public void onCommitLastReInitialization(ContainerId containerId) {
      if (containerId.getId() >= expectedSuccess) {
        errorMsgs.add("Container " + containerId +
            " should throw the exception onContainerReInitialize");
        return;
      }
      TestData td = testMap.get(OpsToTest.COMMIT);
      td.success.addAndGet(1);
      td.successArray.set(containerId.getId(), 1);
      // move on to the following success tests
      asyncClient.stopContainerAsync(containerId, nodeId);
      // throw a fake user exception, and shouldn't crash the test
      throw new RuntimeException("Ignorable Exception");
    }

    @SuppressWarnings("deprecation")
    @Override
    public void onContainerStopped(ContainerId containerId) {
      if (containerId.getId() >= expectedSuccess) {
        errorMsgs.add("Container " + containerId +
            " should throw the exception onContainerStopped");
        return;
      }
      TestData td = testMap.get(OpsToTest.STOP);
      td.success.addAndGet(1);
      td.successArray.set(containerId.getId(), 1);

      // Shouldn't crash the test thread
      throw new RuntimeException("Ignorable Exception");
    }

    @SuppressWarnings("deprecation")
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
      TestData td = testMap.get(OpsToTest.START);
      td.failure.addAndGet(1);
      td.failureArray.set(containerId.getId() - expectedSuccess, 1);
      // move on to the following failure tests
      asyncClient.getContainerStatusAsync(containerId, nodeId);

      // Shouldn't crash the test thread
      throw new RuntimeException("Ignorable Exception");
    }

    @SuppressWarnings("deprecation")
    @Override
    public void onIncreaseContainerResourceError(
        ContainerId containerId, Throwable t) {
      if (containerId.getId() < expectedSuccess + expectedFailure) {
        errorMsgs.add("Container " + containerId +
            " shouldn't throw the exception onIncreaseContainerResourceError");
        return;
      }
      TestData td = testMap.get(OpsToTest.INCR);
      td.failure.addAndGet(1);
      td.failureArray.set(
          containerId.getId() - expectedSuccess - expectedFailure, 1);
      // increase container resource error should NOT change the
      // the container status to FAILED
      // move on to the following failure tests
      asyncClient.stopContainerAsync(containerId, nodeId);
      // Shouldn't crash the test thread
      throw new RuntimeException("Ignorable Exception");
    }

    @SuppressWarnings("deprecation")
    @Override
    public void onUpdateContainerResourceError(ContainerId containerId,
        Throwable t) {
      if (containerId.getId() < expectedSuccess + expectedFailure) {
        errorMsgs.add("Container " + containerId +
            " shouldn't throw the exception onUpdatedContainerResourceError");
        return;
      }
      TestData td = testMap.get(OpsToTest.INCR);
      td.failure.addAndGet(1);
      td.failureArray.set(
          containerId.getId() - expectedSuccess - expectedFailure, 1);
      // increase container resource error should NOT change the
      // the container status to FAILED
      // move on to the following failure tests
      asyncClient.stopContainerAsync(containerId, nodeId);
      // Shouldn't crash the test thread
      throw new RuntimeException("Ignorable Exception");
    }

    @SuppressWarnings("deprecation")
    @Override
    public void onContainerReInitializeError(ContainerId containerId,
        Throwable t) {
      if (containerId.getId() < expectedSuccess + expectedFailure) {
        errorMsgs.add("Container " + containerId +
            " shouldn't throw the exception onContainerReInitializeError");
        return;
      }
      TestData td = testMap.get(OpsToTest.REINIT);
      td.failure.addAndGet(1);
      td.failureArray.set(
          containerId.getId() - expectedSuccess - expectedFailure, 1);

      // increment the stop counters here.. since the container will fail
      td = testMap.get(OpsToTest.STOP);
      td.failure.addAndGet(1);
      td.failureArray.set(
          containerId.getId() - expectedSuccess - expectedFailure, 1);
      // reInit container changes the container status to FAILED
      // Shouldn't crash the test thread
      throw new RuntimeException("Ignorable Exception");
    }

    @SuppressWarnings("deprecation")
    @Override
    public void onContainerRestartError(ContainerId containerId, Throwable t) {
      if (containerId.getId() < expectedSuccess + expectedFailure) {
        errorMsgs.add("Container " + containerId +
            " shouldn't throw the exception onContainerRestartError");
        return;
      }
      TestData td = testMap.get(OpsToTest.RESTART);
      td.failure.addAndGet(1);
      td.failureArray.set(
          containerId.getId() - expectedSuccess - expectedFailure, 1);

      // increment the stop counters here.. since the container will fail
      td = testMap.get(OpsToTest.STOP);
      td.failure.addAndGet(1);
      td.failureArray.set(
          containerId.getId() - expectedSuccess - expectedFailure, 1);
      // restart container changes the container status to FAILED
      // Shouldn't crash the test thread
      throw new RuntimeException("Ignorable Exception");
    }

    @SuppressWarnings("deprecation")
    @Override
    public void onRollbackLastReInitializationError(ContainerId containerId,
        Throwable t) {
      if (containerId.getId() < expectedSuccess + expectedFailure) {
        errorMsgs.add("Container " + containerId +
            " shouldn't throw the exception" +
            " onRollbackLastReInitializationError");
        return;
      }
      TestData td = testMap.get(OpsToTest.ROLLBACK);
      td.failure.addAndGet(1);
      td.failureArray.set(
          containerId.getId() - expectedSuccess - expectedFailure, 1);

      // increment the stop counters here.. since the container will fail
      td = testMap.get(OpsToTest.STOP);
      td.failure.addAndGet(1);
      td.failureArray.set(
          containerId.getId() - expectedSuccess - expectedFailure, 1);
      // rollback container changes the container status to FAILED
      // Shouldn't crash the test thread
      throw new RuntimeException("Ignorable Exception");
    }

    @SuppressWarnings("deprecation")
    @Override
    public void onCommitLastReInitializationError(ContainerId containerId,
        Throwable t) {
      if (containerId.getId() < expectedSuccess + expectedFailure) {
        errorMsgs.add("Container " + containerId +
            " shouldn't throw the exception onCommitLastReInitializationError");
        return;
      }
      TestData td = testMap.get(OpsToTest.COMMIT);
      td.failure.addAndGet(1);
      td.failureArray.set(
          containerId.getId() - expectedSuccess - expectedFailure, 1);

      // increment the stop counters here.. since the container will fail
      td = testMap.get(OpsToTest.STOP);
      td.failure.addAndGet(1);
      td.failureArray.set(
          containerId.getId() - expectedSuccess - expectedFailure, 1);
      // commit container changes the container status to FAILED
      // Shouldn't crash the test thread
      throw new RuntimeException("Ignorable Exception");
    }

    @SuppressWarnings("deprecation")
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
      TestData td = testMap.get(OpsToTest.STOP);
      td.failure.addAndGet(1);
      td.failureArray.set(
          containerId.getId() - expectedSuccess - expectedFailure, 1);

      // Shouldn't crash the test thread
      throw new RuntimeException("Ignorable Exception");
    }

    @SuppressWarnings("deprecation")
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
      TestData td = testMap.get(OpsToTest.QUERY);
      td.failure.addAndGet(1);
      td.failureArray.set(containerId.getId() - expectedSuccess, 1);

      // Shouldn't crash the test thread
      throw new RuntimeException("Ignorable Exception");
    }

    public boolean isAllSuccessCallsExecuted() {
      boolean isAllSuccessCallsExecuted =
          testMap.get(OpsToTest.START).success.get() == expectedSuccess &&
              testMap.get(OpsToTest.QUERY).success.get() == expectedSuccess &&
              testMap.get(OpsToTest.INCR).success.get() == expectedSuccess &&
              testMap.get(OpsToTest.REINIT).success.get() == expectedSuccess &&
              testMap.get(OpsToTest.RESTART).success.get() == expectedSuccess &&
              testMap.get(OpsToTest.ROLLBACK).success.get() ==
                  expectedSuccess &&
              testMap.get(OpsToTest.COMMIT).success.get() == expectedSuccess &&
              testMap.get(OpsToTest.STOP).success.get() == expectedSuccess;
      if (isAllSuccessCallsExecuted) {
        assertAtomicIntegerArray(testMap.get(OpsToTest.START).successArray);
        assertAtomicIntegerArray(testMap.get(OpsToTest.QUERY).successArray);
        assertAtomicIntegerArray(testMap.get(OpsToTest.INCR).successArray);
        assertAtomicIntegerArray(testMap.get(OpsToTest.REINIT).successArray);
        assertAtomicIntegerArray(testMap.get(OpsToTest.RESTART).successArray);
        assertAtomicIntegerArray(testMap.get(OpsToTest.ROLLBACK).successArray);
        assertAtomicIntegerArray(testMap.get(OpsToTest.COMMIT).successArray);
        assertAtomicIntegerArray(testMap.get(OpsToTest.STOP).successArray);
      }
      return isAllSuccessCallsExecuted;
    }

    public boolean isStartAndQueryFailureCallsExecuted() {
      boolean isStartAndQueryFailureCallsExecuted =
          testMap.get(OpsToTest.START).failure.get() == expectedFailure &&
              testMap.get(OpsToTest.QUERY).failure.get() == expectedFailure;
      if (isStartAndQueryFailureCallsExecuted) {
        assertAtomicIntegerArray(testMap.get(OpsToTest.START).failureArray);
        assertAtomicIntegerArray(testMap.get(OpsToTest.QUERY).failureArray);
      }
      return isStartAndQueryFailureCallsExecuted;
    }

    public boolean isIncreaseResourceFailureCallsExecuted() {
      boolean isIncreaseResourceFailureCallsExecuted =
          testMap.get(OpsToTest.INCR).failure.get()
              + testMap.get(OpsToTest.REINIT).failure.get()
              + testMap.get(OpsToTest.RESTART).failure.get()
              + testMap.get(OpsToTest.ROLLBACK).failure.get()
              + testMap.get(OpsToTest.COMMIT).failure.get()
              == expectedFailure;
      if (isIncreaseResourceFailureCallsExecuted) {
        AtomicIntegerArray testArray =
            new AtomicIntegerArray(
                testMap.get(OpsToTest.INCR).failureArray.length());
        for (int i = 0; i < testArray.length(); i++) {
          for (OpsToTest op : EnumSet.of(OpsToTest.REINIT, OpsToTest.RESTART,
              OpsToTest.ROLLBACK, OpsToTest.COMMIT, OpsToTest.INCR)) {
            testArray.addAndGet(i, testMap.get(op).failureArray.get(i));
          }
        }
        assertAtomicIntegerArray(testArray);
      }
      return isIncreaseResourceFailureCallsExecuted;
    }

    public boolean isStopFailureCallsExecuted() {
      boolean isStopFailureCallsExecuted =
          testMap.get(OpsToTest.STOP).failure.get() == expectedFailure;
      if (isStopFailureCallsExecuted) {
        assertAtomicIntegerArray(testMap.get(OpsToTest.STOP).failureArray);
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
        doNothing().when(client).updateContainerResource(
            any(Container.class));
        doNothing().when(client).reInitializeContainer(
            any(ContainerId.class), any(ContainerLaunchContext.class),
            anyBoolean());
        doNothing().when(client).restartContainer(any(ContainerId.class));
        doNothing().when(client).rollbackLastReInitialization(
            any(ContainerId.class));
        doNothing().when(client).commitLastReInitialization(
            any(ContainerId.class));
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
        doThrow(RPCUtil.getRemoteException("Increase Resource Exception"))
            .when(client).updateContainerResource(any(Container.class));
        doThrow(RPCUtil.getRemoteException("ReInitialize Exception"))
            .when(client).reInitializeContainer(
            any(ContainerId.class), any(ContainerLaunchContext.class),
            anyBoolean());
        doThrow(RPCUtil.getRemoteException("Restart Exception"))
            .when(client).restartContainer(any(ContainerId.class));
        doThrow(RPCUtil.getRemoteException("Rollback upgrade Exception"))
            .when(client).rollbackLastReInitialization(
            any(ContainerId.class));
        doThrow(RPCUtil.getRemoteException("Commit upgrade Exception"))
            .when(client).commitLastReInitialization(
            any(ContainerId.class));
        doThrow(RPCUtil.getRemoteException("Stop Exception")).when(client)
            .stopContainer(any(ContainerId.class), any(NodeId.class));
    }
    when(client.getNodeIdOfStartedContainer(any(ContainerId.class)))
        .thenReturn(NodeId.newInstance("localhost", 0));
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
      extends NMClientAsync.AbstractCallbackHandler {
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

    @Deprecated
    @Override
    public void onContainerResourceIncreased(
        ContainerId containerId, Resource resource) {}

    @Override
    public void onContainerResourceUpdated(ContainerId containerId,
        Resource resource) {
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

    @Deprecated
    @Override
    public void onIncreaseContainerResourceError(
        ContainerId containerId, Throwable t) {}

    @Override
    public void onUpdateContainerResourceError(ContainerId containerId,
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
    ContainerId containerId = ContainerId.newContainerId(attemptId, i);
    nodeId = NodeId.newInstance("localhost", 0);
    // Create an empty record
    containerToken = recordFactory.newRecordInstance(Token.class);
    return Container.newInstance(containerId, nodeId, null, null, null,
      containerToken);
  }
}
