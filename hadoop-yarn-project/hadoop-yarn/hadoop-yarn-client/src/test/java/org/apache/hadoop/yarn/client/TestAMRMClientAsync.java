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

import static org.mockito.Mockito.anyFloat;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestAMRMClientAsync {

  private static final Log LOG = LogFactory.getLog(TestAMRMClientAsync.class);
  
  @Test(timeout=10000)
  public void testAMRMClientAsync() throws Exception {
    Configuration conf = new Configuration();
    List<ContainerStatus> completed1 = Arrays.asList(
        BuilderUtils.newContainerStatus(
            BuilderUtils.newContainerId(0, 0, 0, 0),
            ContainerState.COMPLETE, "", 0));
    List<Container> allocated1 = Arrays.asList(
        BuilderUtils.newContainer(null, null, null, null, null, null, 0));
    final AllocateResponse response1 = createAllocateResponse(
        new ArrayList<ContainerStatus>(), allocated1);
    final AllocateResponse response2 = createAllocateResponse(completed1,
        new ArrayList<Container>());
    final AllocateResponse emptyResponse = createAllocateResponse(
        new ArrayList<ContainerStatus>(), new ArrayList<Container>());

    TestCallbackHandler callbackHandler = new TestCallbackHandler();
    AMRMClient client = mock(AMRMClient.class);
    final AtomicBoolean secondHeartbeatReceived = new AtomicBoolean(false);
    when(client.allocate(anyFloat())).thenReturn(response1).thenAnswer(new Answer<AllocateResponse>() {
      @Override
      public AllocateResponse answer(InvocationOnMock invocation)
          throws Throwable {
        secondHeartbeatReceived.set(true);
        return response2;
      }
    }).thenReturn(emptyResponse);
    when(client.registerApplicationMaster(anyString(), anyInt(), anyString()))
      .thenReturn(null);
    
    AMRMClientAsync asyncClient = new AMRMClientAsync(client, 20, callbackHandler);
    asyncClient.init(conf);
    asyncClient.start();
    asyncClient.registerApplicationMaster("localhost", 1234, null);
    
    // while the CallbackHandler will still only be processing the first response,
    // heartbeater thread should still be sending heartbeats.
    // To test this, wait for the second heartbeat to be received. 
    while (!secondHeartbeatReceived.get()) {
      Thread.sleep(10);
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
  
  private AllocateResponse createAllocateResponse(
      List<ContainerStatus> completed, List<Container> allocated) {
    AllocateResponse response = BuilderUtils.newAllocateResponse(0, completed, allocated,
        new ArrayList<NodeReport>(), null, false, 1, null);
    return response;
  }
  
  private class TestCallbackHandler implements AMRMClientAsync.CallbackHandler {
    private volatile List<ContainerStatus> completedContainers;
    private volatile List<Container> allocatedContainers;
    
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
    public void onRebootRequest() {}

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {}
  }
}
