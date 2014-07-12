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

import static org.mockito.Matchers.anyFloat;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.AMCommand;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestAMRMClientAsync {

  private static final Log LOG = LogFactory.getLog(TestAMRMClientAsync.class);
  
  @SuppressWarnings("unchecked")
  @Test(timeout=10000)
  public void testAMRMClientAsync() throws Exception {
    Configuration conf = new Configuration();
    final AtomicBoolean heartbeatBlock = new AtomicBoolean(true);
    List<ContainerStatus> completed1 = Arrays.asList(
        ContainerStatus.newInstance(newContainerId(0, 0, 0, 0),
            ContainerState.COMPLETE, "", 0));
    List<Container> allocated1 = Arrays.asList(
        Container.newInstance(null, null, null, null, null, null));
    final AllocateResponse response1 = createAllocateResponse(
        new ArrayList<ContainerStatus>(), allocated1, null);
    final AllocateResponse response2 = createAllocateResponse(completed1,
        new ArrayList<Container>(), null);
    final AllocateResponse emptyResponse = createAllocateResponse(
        new ArrayList<ContainerStatus>(), new ArrayList<Container>(), null);

    TestCallbackHandler callbackHandler = new TestCallbackHandler();
    final AMRMClient<ContainerRequest> client = mock(AMRMClientImpl.class);
    final AtomicInteger secondHeartbeatSync = new AtomicInteger(0);
    when(client.allocate(anyFloat())).thenReturn(response1).thenAnswer(new Answer<AllocateResponse>() {
      @Override
      public AllocateResponse answer(InvocationOnMock invocation)
          throws Throwable {
        secondHeartbeatSync.incrementAndGet();
        while(heartbeatBlock.get()) {
          synchronized(heartbeatBlock) {
            heartbeatBlock.wait();
          }
        }
        secondHeartbeatSync.incrementAndGet();
        return response2;
      }
    }).thenReturn(emptyResponse);
    when(client.registerApplicationMaster(anyString(), anyInt(), anyString()))
      .thenReturn(null);
    when(client.getAvailableResources()).thenAnswer(new Answer<Resource>() {
      @Override
      public Resource answer(InvocationOnMock invocation)
          throws Throwable {
        // take client lock to simulate behavior of real impl
        synchronized (client) { 
          Thread.sleep(10);
        }
        return null;
      }
    });
    
    AMRMClientAsync<ContainerRequest> asyncClient = 
        AMRMClientAsync.createAMRMClientAsync(client, 20, callbackHandler);
    asyncClient.init(conf);
    asyncClient.start();
    asyncClient.registerApplicationMaster("localhost", 1234, null);
    
    // while the CallbackHandler will still only be processing the first response,
    // heartbeater thread should still be sending heartbeats.
    // To test this, wait for the second heartbeat to be received. 
    while (secondHeartbeatSync.get() < 1) {
      Thread.sleep(10);
    }
    
    // heartbeat will be blocked. make sure we can call client methods at this
    // time. Checks that heartbeat is not holding onto client lock
    assert(secondHeartbeatSync.get() < 2);
    asyncClient.getAvailableResources();
    // method returned. now unblock heartbeat
    assert(secondHeartbeatSync.get() < 2);
    synchronized (heartbeatBlock) {
      heartbeatBlock.set(false);
      heartbeatBlock.notifyAll();
    }
    
    // allocated containers should come before completed containers
    Assert.assertEquals(null, callbackHandler.takeCompletedContainers());
    
    // wait for the allocated containers from the first heartbeat's response
    while (callbackHandler.takeAllocatedContainers() == null) {
      Assert.assertEquals(null, callbackHandler.takeCompletedContainers());
      Thread.sleep(10);
    }
    
    // wait for the completed containers from the second heartbeat's response
    while (callbackHandler.takeCompletedContainers() == null) {
      Thread.sleep(10);
    }
    
    asyncClient.stop();
    
    Assert.assertEquals(null, callbackHandler.takeAllocatedContainers());
    Assert.assertEquals(null, callbackHandler.takeCompletedContainers());
  }

  @Test(timeout=10000)
  public void testAMRMClientAsyncException() throws Exception {
    String exStr = "TestException";
    YarnException mockException = mock(YarnException.class);
    when(mockException.getMessage()).thenReturn(exStr);
    runHeartBeatThrowOutException(mockException);
  }

  @Test(timeout=10000)
  public void testAMRMClientAsyncRunTimeException() throws Exception {
    String exStr = "TestRunTimeException";
    RuntimeException mockRunTimeException = mock(RuntimeException.class);
    when(mockRunTimeException.getMessage()).thenReturn(exStr);
    runHeartBeatThrowOutException(mockRunTimeException);
  }

  private void runHeartBeatThrowOutException(Exception ex) throws Exception{
    Configuration conf = new Configuration();
    TestCallbackHandler callbackHandler = new TestCallbackHandler();
    @SuppressWarnings("unchecked")
    AMRMClient<ContainerRequest> client = mock(AMRMClientImpl.class);
    when(client.allocate(anyFloat())).thenThrow(ex);

    AMRMClientAsync<ContainerRequest> asyncClient = 
        AMRMClientAsync.createAMRMClientAsync(client, 20, callbackHandler);
    asyncClient.init(conf);
    asyncClient.start();
    
    synchronized (callbackHandler.notifier) {
      asyncClient.registerApplicationMaster("localhost", 1234, null);
      while(callbackHandler.savedException == null) {
        try {
          callbackHandler.notifier.wait();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
    Assert.assertTrue(callbackHandler.savedException.getMessage().contains(
        ex.getMessage()));
    
    asyncClient.stop();
    // stopping should have joined all threads and completed all callbacks
    Assert.assertTrue(callbackHandler.callbackCount == 0);
  }

  @Test (timeout = 10000)
  public void testAMRMClientAsyncShutDown() throws Exception {
    Configuration conf = new Configuration();
    TestCallbackHandler callbackHandler = new TestCallbackHandler();
    @SuppressWarnings("unchecked")
    AMRMClient<ContainerRequest> client = mock(AMRMClientImpl.class);

    final AllocateResponse shutDownResponse = createAllocateResponse(
        new ArrayList<ContainerStatus>(), new ArrayList<Container>(), null);
    shutDownResponse.setAMCommand(AMCommand.AM_SHUTDOWN);
    when(client.allocate(anyFloat())).thenReturn(shutDownResponse);

    AMRMClientAsync<ContainerRequest> asyncClient =
        AMRMClientAsync.createAMRMClientAsync(client, 10, callbackHandler);
    asyncClient.init(conf);
    asyncClient.start();

    asyncClient.registerApplicationMaster("localhost", 1234, null);

    Thread.sleep(50);

    verify(client, times(1)).allocate(anyFloat());
    asyncClient.stop();
  }

  @Test (timeout = 5000)
  public void testCallAMRMClientAsyncStopFromCallbackHandler()
      throws YarnException, IOException, InterruptedException {
    Configuration conf = new Configuration();
    TestCallbackHandler2 callbackHandler = new TestCallbackHandler2();
    @SuppressWarnings("unchecked")
    AMRMClient<ContainerRequest> client = mock(AMRMClientImpl.class);

    List<ContainerStatus> completed = Arrays.asList(
        ContainerStatus.newInstance(newContainerId(0, 0, 0, 0),
            ContainerState.COMPLETE, "", 0));
    final AllocateResponse response = createAllocateResponse(completed,
        new ArrayList<Container>(), null);

    when(client.allocate(anyFloat())).thenReturn(response);

    AMRMClientAsync<ContainerRequest> asyncClient =
        AMRMClientAsync.createAMRMClientAsync(client, 20, callbackHandler);
    callbackHandler.asynClient = asyncClient;
    asyncClient.init(conf);
    asyncClient.start();

    synchronized (callbackHandler.notifier) {
      asyncClient.registerApplicationMaster("localhost", 1234, null);
      while(callbackHandler.notify == false) {
        try {
          callbackHandler.notifier.wait();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  void runCallBackThrowOutException(TestCallbackHandler2 callbackHandler) throws
        InterruptedException, YarnException, IOException {
    Configuration conf = new Configuration();
    @SuppressWarnings("unchecked")
    AMRMClient<ContainerRequest> client = mock(AMRMClientImpl.class);

    List<ContainerStatus> completed = Arrays.asList(
        ContainerStatus.newInstance(newContainerId(0, 0, 0, 0),
            ContainerState.COMPLETE, "", 0));
    final AllocateResponse response = createAllocateResponse(completed,
        new ArrayList<Container>(), null);

    when(client.allocate(anyFloat())).thenReturn(response);
    AMRMClientAsync<ContainerRequest> asyncClient =
        AMRMClientAsync.createAMRMClientAsync(client, 20, callbackHandler);
    callbackHandler.asynClient = asyncClient;
    callbackHandler.throwOutException = true;
    asyncClient.init(conf);
    asyncClient.start();

    // call register and wait for error callback and stop
    synchronized (callbackHandler.notifier) {
      asyncClient.registerApplicationMaster("localhost", 1234, null);
      while(callbackHandler.notify == false) {
        try {
          callbackHandler.notifier.wait();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
    // verify error invoked
    verify(callbackHandler, times(0)).getProgress();
    verify(callbackHandler, times(1)).onError(any(Exception.class));
    // sleep to wait for a few heartbeat calls that can trigger callbacks
    Thread.sleep(50);
    // verify no more invocations after the first one.
    // ie. callback thread has stopped
    verify(callbackHandler, times(0)).getProgress();
    verify(callbackHandler, times(1)).onError(any(Exception.class));
  }

  @Test (timeout = 5000)
  public void testCallBackThrowOutException() throws YarnException,
      IOException, InterruptedException {
    // test exception in callback with app calling stop() on app.onError()
    TestCallbackHandler2 callbackHandler = spy(new TestCallbackHandler2());
    runCallBackThrowOutException(callbackHandler);
  }

  @Test (timeout = 5000)
  public void testCallBackThrowOutExceptionNoStop() throws YarnException,
      IOException, InterruptedException {
    // test exception in callback with app not calling stop() on app.onError()
    TestCallbackHandler2 callbackHandler = spy(new TestCallbackHandler2());
    callbackHandler.stop = false;
    runCallBackThrowOutException(callbackHandler);
  }

  private AllocateResponse createAllocateResponse(
      List<ContainerStatus> completed, List<Container> allocated,
      List<NMToken> nmTokens) {
    AllocateResponse response =
        AllocateResponse.newInstance(0, completed, allocated,
            new ArrayList<NodeReport>(), null, null, 1, null, nmTokens);
    return response;
  }

  public static ContainerId newContainerId(int appId, int appAttemptId,
      long timestamp, int containerId) {
    ApplicationId applicationId = ApplicationId.newInstance(timestamp, appId);
    ApplicationAttemptId applicationAttemptId =
        ApplicationAttemptId.newInstance(applicationId, appAttemptId);
    return ContainerId.newInstance(applicationAttemptId, containerId);
  }

  private class TestCallbackHandler implements AMRMClientAsync.CallbackHandler {
    private volatile List<ContainerStatus> completedContainers;
    private volatile List<Container> allocatedContainers;
    Exception savedException = null;
    boolean reboot = false;
    Object notifier = new Object();
    
    int callbackCount = 0;
    
    public List<ContainerStatus> takeCompletedContainers() {
      List<ContainerStatus> ret = completedContainers;
      if (ret == null) {
        return null;
      }
      completedContainers = null;
      synchronized (ret) {
        ret.notify();
      }
      return ret;
    }
    
    public List<Container> takeAllocatedContainers() {
      List<Container> ret = allocatedContainers;
      if (ret == null) {
        return null;
      }
      allocatedContainers = null;
      synchronized (ret) {
        ret.notify();
      }
      return ret;
    }
    
    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {
      completedContainers = statuses;
      // wait for containers to be taken before returning
      synchronized (completedContainers) {
        while (completedContainers != null) {
          try {
            completedContainers.wait();
          } catch (InterruptedException ex) {
            LOG.error("Interrupted during wait", ex);
          }
        }
      }
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
      allocatedContainers = containers;
      // wait for containers to be taken before returning
      synchronized (allocatedContainers) {
        while (allocatedContainers != null) {
          try {
            allocatedContainers.wait();
          } catch (InterruptedException ex) {
            LOG.error("Interrupted during wait", ex);
          }
        }
      }
    }

    @Override
    public void onShutdownRequest() {
      reboot = true;
      synchronized (notifier) {
        notifier.notifyAll();        
      }
    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {}

    @Override
    public float getProgress() {
      callbackCount++;
      return 0.5f;
    }

    @Override
    public void onError(Throwable e) {
      savedException = new Exception(e.getMessage());
      synchronized (notifier) {
        notifier.notifyAll();        
      }
    }
  }

  private class TestCallbackHandler2 implements AMRMClientAsync.CallbackHandler {
    Object notifier = new Object();
    @SuppressWarnings("rawtypes")
    AMRMClientAsync asynClient;
    boolean stop = true;
    boolean notify = false;
    boolean throwOutException = false;

    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {
      if (throwOutException) {
        throw new YarnRuntimeException("Exception from callback handler");
      }
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {}

    @Override
    public void onShutdownRequest() {}

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {}

    @Override
    public float getProgress() {
      callStopAndNotify();
      return 0;
    }

    @Override
    public void onError(Throwable e) {
      Assert.assertEquals(e.getMessage(), "Exception from callback handler");
      callStopAndNotify();
    }

    void callStopAndNotify() {
      if(stop) {
        asynClient.stop();
      }
      notify = true;
      synchronized (notifier) {
        notifier.notifyAll();
      }
    }
  }
}
