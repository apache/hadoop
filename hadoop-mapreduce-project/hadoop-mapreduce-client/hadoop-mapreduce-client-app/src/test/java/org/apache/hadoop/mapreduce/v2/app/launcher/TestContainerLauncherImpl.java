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
package org.apache.hadoop.mapreduce.v2.app.launcher;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ShuffleHandler;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.MRApp;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncher.EventType;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.CommitResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerUpdateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetLocalizationStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetLocalizationStatusesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.IncreaseContainersResourceRequest;
import org.apache.hadoop.yarn.api.protocolrecords.IncreaseContainersResourceResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReInitializeContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReInitializeContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceLocalizationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceLocalizationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RestartContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RollbackResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.client.api.impl.ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestContainerLauncherImpl {
  static final Logger LOG =
      LoggerFactory.getLogger(TestContainerLauncherImpl.class);
  private static final RecordFactory recordFactory =
    RecordFactoryProvider.getRecordFactory(null);

  private Map<String, ByteBuffer> serviceResponse =
      new HashMap<String, ByteBuffer>();

  @Before
  public void setup() throws IOException {
    serviceResponse.clear();
    serviceResponse.put(ShuffleHandler.MAPREDUCE_SHUFFLE_SERVICEID,
        ShuffleHandler.serializeMetaData(80));
  }

  // tests here mock ContainerManagementProtocol which does not have close
  // method. creating an interface that implements ContainerManagementProtocol
  // and Closeable so the tests does not fail with NoSuchMethodException
  private static interface ContainerManagementProtocolClient extends
    ContainerManagementProtocol, Closeable {
  }
  
  private static class ContainerLauncherImplUnderTest extends 
    ContainerLauncherImpl {

    private ContainerManagementProtocol containerManager;

    public ContainerLauncherImplUnderTest(AppContext context,
        ContainerManagementProtocol containerManager) {
      super(context);
      this.containerManager = containerManager;
    }

    @Override
    public ContainerManagementProtocolProxyData getCMProxy(
        String containerMgrBindAddr, ContainerId containerId)
        throws IOException {
      ContainerManagementProtocolProxyData protocolProxy =
          mock(ContainerManagementProtocolProxyData.class);
      when(protocolProxy.getContainerManagementProtocol()).thenReturn(
        containerManager);
      return protocolProxy;
    }

    public void waitForPoolToIdle() throws InterruptedException {
      //I wish that we did not need the sleep, but it is here so that we are sure
      // That the other thread had time to insert the event into the queue and
      // start processing it.  For some reason we were getting interrupted
      // exceptions within eventQueue without this sleep.
      Thread.sleep(100l);
      LOG.debug("POOL SIZE 1: "+this.eventQueue.size()+
          " POOL SIZE 2: "+this.launcherPool.getQueue().size()+
          " ACTIVE COUNT: "+ this.launcherPool.getActiveCount());
      while(!this.eventQueue.isEmpty() || 
          !this.launcherPool.getQueue().isEmpty() || 
          this.launcherPool.getActiveCount() > 0) {
        Thread.sleep(100l);
        LOG.debug("POOL SIZE 1: "+this.eventQueue.size()+
            " POOL SIZE 2: "+this.launcherPool.getQueue().size()+
            " ACTIVE COUNT: "+ this.launcherPool.getActiveCount());
      }
      LOG.debug("POOL SIZE 1: "+this.eventQueue.size()+
          " POOL SIZE 2: "+this.launcherPool.getQueue().size()+
          " ACTIVE COUNT: "+ this.launcherPool.getActiveCount());
    }
  }
  
  public static ContainerId makeContainerId(long ts, int appId, int attemptId,
      int id) {
    return ContainerId.newContainerId(
      ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(ts, appId), attemptId), id);
  }

  public static TaskAttemptId makeTaskAttemptId(long ts, int appId, int taskId, 
      TaskType taskType, int id) {
    ApplicationId aID = ApplicationId.newInstance(ts, appId);
    JobId jID = MRBuilderUtils.newJobId(aID, id);
    TaskId tID = MRBuilderUtils.newTaskId(jID, taskId, taskType);
    return MRBuilderUtils.newTaskAttemptId(tID, id);
  }
  
  @Test(timeout = 5000)
  public void testHandle() throws Exception {
    LOG.info("STARTING testHandle");
    AppContext mockContext = mock(AppContext.class);
    @SuppressWarnings("unchecked")
    EventHandler<Event> mockEventHandler = mock(EventHandler.class);
    when(mockContext.getEventHandler()).thenReturn(mockEventHandler);
    String cmAddress = "127.0.0.1:8000";
    ContainerManagementProtocolClient mockCM =
        mock(ContainerManagementProtocolClient.class);
    ContainerLauncherImplUnderTest ut =
        new ContainerLauncherImplUnderTest(mockContext, mockCM);
    
    Configuration conf = new Configuration();
    ut.init(conf);
    ut.start();
    try {
      ContainerId contId = makeContainerId(0l, 0, 0, 1);
      TaskAttemptId taskAttemptId = makeTaskAttemptId(0l, 0, 0, TaskType.MAP, 0);
      StartContainersResponse startResp =
        recordFactory.newRecordInstance(StartContainersResponse.class);
      startResp.setAllServicesMetaData(serviceResponse);
      

      LOG.info("inserting launch event");
      ContainerRemoteLaunchEvent mockLaunchEvent = 
        mock(ContainerRemoteLaunchEvent.class);
      when(mockLaunchEvent.getType())
        .thenReturn(EventType.CONTAINER_REMOTE_LAUNCH);
      when(mockLaunchEvent.getContainerID())
        .thenReturn(contId);
      when(mockLaunchEvent.getTaskAttemptID()).thenReturn(taskAttemptId);
      when(mockLaunchEvent.getContainerMgrAddress()).thenReturn(cmAddress);
      when(mockCM.startContainers(any(StartContainersRequest.class))).thenReturn(startResp);
      when(mockLaunchEvent.getContainerToken()).thenReturn(
          createNewContainerToken(contId, cmAddress));
      ut.handle(mockLaunchEvent);
      
      ut.waitForPoolToIdle();
      
      verify(mockCM).startContainers(any(StartContainersRequest.class));
      
      LOG.info("inserting cleanup event");
      ContainerLauncherEvent mockCleanupEvent = 
        mock(ContainerLauncherEvent.class);
      when(mockCleanupEvent.getType())
        .thenReturn(EventType.CONTAINER_REMOTE_CLEANUP);
      when(mockCleanupEvent.getContainerID())
        .thenReturn(contId);
      when(mockCleanupEvent.getTaskAttemptID()).thenReturn(taskAttemptId);
      when(mockCleanupEvent.getContainerMgrAddress()).thenReturn(cmAddress);
      ut.handle(mockCleanupEvent);
      
      ut.waitForPoolToIdle();
      
      verify(mockCM).stopContainers(any(StopContainersRequest.class));
    } finally {
      ut.stop();
    }
  }
  
  @Test(timeout = 5000)
  public void testOutOfOrder() throws Exception {
    LOG.info("STARTING testOutOfOrder");
    AppContext mockContext = mock(AppContext.class);
    @SuppressWarnings("unchecked")
    EventHandler<Event> mockEventHandler = mock(EventHandler.class);
    when(mockContext.getEventHandler()).thenReturn(mockEventHandler);

    ContainerManagementProtocolClient mockCM =
        mock(ContainerManagementProtocolClient.class);
    ContainerLauncherImplUnderTest ut =
        new ContainerLauncherImplUnderTest(mockContext, mockCM);
    
    Configuration conf = new Configuration();
    ut.init(conf);
    ut.start();
    try {
      ContainerId contId = makeContainerId(0l, 0, 0, 1);
      TaskAttemptId taskAttemptId = makeTaskAttemptId(0l, 0, 0, TaskType.MAP, 0);
      String cmAddress = "127.0.0.1:8000";
      StartContainersResponse startResp =
        recordFactory.newRecordInstance(StartContainersResponse.class);
      startResp.setAllServicesMetaData(serviceResponse);

      LOG.info("inserting cleanup event");
      ContainerLauncherEvent mockCleanupEvent = 
        mock(ContainerLauncherEvent.class);
      when(mockCleanupEvent.getType())
        .thenReturn(EventType.CONTAINER_REMOTE_CLEANUP);
      when(mockCleanupEvent.getContainerID())
        .thenReturn(contId);
      when(mockCleanupEvent.getTaskAttemptID()).thenReturn(taskAttemptId);
      when(mockCleanupEvent.getContainerMgrAddress()).thenReturn(cmAddress);
      ut.handle(mockCleanupEvent);
      
      ut.waitForPoolToIdle();
      
      verify(mockCM, never()).stopContainers(any(StopContainersRequest.class));

      LOG.info("inserting launch event");
      ContainerRemoteLaunchEvent mockLaunchEvent = 
        mock(ContainerRemoteLaunchEvent.class);
      when(mockLaunchEvent.getType())
        .thenReturn(EventType.CONTAINER_REMOTE_LAUNCH);
      when(mockLaunchEvent.getContainerID())
        .thenReturn(contId);
      when(mockLaunchEvent.getTaskAttemptID()).thenReturn(taskAttemptId);
      when(mockLaunchEvent.getContainerMgrAddress()).thenReturn(cmAddress);
      when(mockCM.startContainers(any(StartContainersRequest.class))).thenReturn(startResp);
      when(mockLaunchEvent.getContainerToken()).thenReturn(
          createNewContainerToken(contId, cmAddress));
      ut.handle(mockLaunchEvent);
      
      ut.waitForPoolToIdle();
      
      verify(mockCM, never()).startContainers(any(StartContainersRequest.class));
    } finally {
      ut.stop();
    }
  }

  @Test(timeout = 5000)
  public void testMyShutdown() throws Exception {
    LOG.info("in test Shutdown");

    AppContext mockContext = mock(AppContext.class);
    @SuppressWarnings("unchecked")
    EventHandler<Event> mockEventHandler = mock(EventHandler.class);
    when(mockContext.getEventHandler()).thenReturn(mockEventHandler);

    ContainerManagementProtocolClient mockCM =
        mock(ContainerManagementProtocolClient.class);
    ContainerLauncherImplUnderTest ut =
        new ContainerLauncherImplUnderTest(mockContext, mockCM);

    Configuration conf = new Configuration();
    ut.init(conf);
    ut.start();
    try {
      ContainerId contId = makeContainerId(0l, 0, 0, 1);
      TaskAttemptId taskAttemptId = makeTaskAttemptId(0l, 0, 0, TaskType.MAP, 0);
      String cmAddress = "127.0.0.1:8000";
      StartContainersResponse startResp =
        recordFactory.newRecordInstance(StartContainersResponse.class);
      startResp.setAllServicesMetaData(serviceResponse);

      LOG.info("inserting launch event");
      ContainerRemoteLaunchEvent mockLaunchEvent =
        mock(ContainerRemoteLaunchEvent.class);
      when(mockLaunchEvent.getType())
        .thenReturn(EventType.CONTAINER_REMOTE_LAUNCH);
      when(mockLaunchEvent.getContainerID())
        .thenReturn(contId);
      when(mockLaunchEvent.getTaskAttemptID()).thenReturn(taskAttemptId);
      when(mockLaunchEvent.getContainerMgrAddress()).thenReturn(cmAddress);
      when(mockCM.startContainers(any(StartContainersRequest.class))).thenReturn(startResp);
      when(mockLaunchEvent.getContainerToken()).thenReturn(
          createNewContainerToken(contId, cmAddress));
      ut.handle(mockLaunchEvent);

      ut.waitForPoolToIdle();

      verify(mockCM).startContainers(any(StartContainersRequest.class));

      // skip cleanup and make sure stop kills the container

    } finally {
      ut.stop();
      verify(mockCM).stopContainers(any(StopContainersRequest.class));
    }
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test(timeout = 5000)
  public void testContainerCleaned() throws Exception {
    LOG.info("STARTING testContainerCleaned");
    
    CyclicBarrier startLaunchBarrier = new CyclicBarrier(2);
    CyclicBarrier completeLaunchBarrier = new CyclicBarrier(2);

    AppContext mockContext = mock(AppContext.class);
    
    EventHandler mockEventHandler = mock(EventHandler.class);
    when(mockContext.getEventHandler()).thenReturn(mockEventHandler);

    ContainerManagementProtocolClient mockCM =
        new ContainerManagerForTest(startLaunchBarrier, completeLaunchBarrier);
    ContainerLauncherImplUnderTest ut =
        new ContainerLauncherImplUnderTest(mockContext, mockCM);
    
    Configuration conf = new Configuration();
    ut.init(conf);
    ut.start();
    try {
      ContainerId contId = makeContainerId(0l, 0, 0, 1);
      TaskAttemptId taskAttemptId = makeTaskAttemptId(0l, 0, 0, TaskType.MAP, 0);
      String cmAddress = "127.0.0.1:8000";
      StartContainersResponse startResp =
        recordFactory.newRecordInstance(StartContainersResponse.class);
      startResp.setAllServicesMetaData(serviceResponse);
      
     
      LOG.info("inserting launch event");
      ContainerRemoteLaunchEvent mockLaunchEvent = 
        mock(ContainerRemoteLaunchEvent.class);
      when(mockLaunchEvent.getType())
        .thenReturn(EventType.CONTAINER_REMOTE_LAUNCH);
      when(mockLaunchEvent.getContainerID())
        .thenReturn(contId);
      when(mockLaunchEvent.getTaskAttemptID()).thenReturn(taskAttemptId);
      when(mockLaunchEvent.getContainerMgrAddress()).thenReturn(cmAddress);
      when(mockLaunchEvent.getContainerToken()).thenReturn(
          createNewContainerToken(contId, cmAddress));
      ut.handle(mockLaunchEvent);
      
      startLaunchBarrier.await();
      
           
      LOG.info("inserting cleanup event");
      ContainerLauncherEvent mockCleanupEvent = 
        mock(ContainerLauncherEvent.class);
      when(mockCleanupEvent.getType())
        .thenReturn(EventType.CONTAINER_REMOTE_CLEANUP);
      when(mockCleanupEvent.getContainerID())
        .thenReturn(contId);
      when(mockCleanupEvent.getTaskAttemptID()).thenReturn(taskAttemptId);
      when(mockCleanupEvent.getContainerMgrAddress()).thenReturn(cmAddress);
      ut.handle(mockCleanupEvent);

      completeLaunchBarrier.await();
     
      ut.waitForPoolToIdle();
      
      ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);
      verify(mockEventHandler, atLeast(2)).handle(arg.capture());
      boolean containerCleaned = false;
      
      for (int i =0; i < arg.getAllValues().size(); i++) {
        LOG.info(arg.getAllValues().get(i).toString());
        Event currentEvent = arg.getAllValues().get(i);
        if (currentEvent.getType() == TaskAttemptEventType.TA_CONTAINER_CLEANED) {
          containerCleaned = true;
        }
      }
      assert(containerCleaned);
      
    } finally {
      ut.stop();
    }
  }
  
  private Token createNewContainerToken(ContainerId contId,
      String containerManagerAddr) {
    long currentTime = System.currentTimeMillis();
    return MRApp.newContainerToken(NodeId.newInstance("127.0.0.1",
        1234), "password".getBytes(), new ContainerTokenIdentifier(
        contId, containerManagerAddr, "user",
        Resource.newInstance(1024, 1),
        currentTime + 10000L, 123, currentTime, Priority.newInstance(0), 0));
  }

  private static class ContainerManagerForTest implements ContainerManagementProtocolClient {

    private CyclicBarrier startLaunchBarrier;
    private CyclicBarrier completeLaunchBarrier;

    ContainerManagerForTest (CyclicBarrier startLaunchBarrier, CyclicBarrier completeLaunchBarrier) {
      this.startLaunchBarrier = startLaunchBarrier;
      this.completeLaunchBarrier = completeLaunchBarrier;
    }
    @Override
    public StartContainersResponse startContainers(StartContainersRequest request)
        throws IOException {
      try {
        startLaunchBarrier.await();
        completeLaunchBarrier.await();
        //To ensure the kill is started before the launch
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (BrokenBarrierException e) {
        e.printStackTrace();
      } 
      
      throw new IOException(new ContainerException("Force fail CM"));
      
    }

    @Override
    public StopContainersResponse stopContainers(StopContainersRequest request)
        throws IOException {
      return null;
    }

    @Override
    public GetContainerStatusesResponse getContainerStatuses(
        GetContainerStatusesRequest request) throws IOException {
      return null;
    }

    @Override
    @Deprecated
    public IncreaseContainersResourceResponse increaseContainersResource(
        IncreaseContainersResourceRequest request) throws YarnException,
        IOException {
      return null;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public SignalContainerResponse signalToContainer(
        SignalContainerRequest request) throws YarnException, IOException {
      return null;
    }

    @Override
    public ResourceLocalizationResponse localize(
        ResourceLocalizationRequest request) throws YarnException, IOException {
      return null;
    }

    @Override
    public ReInitializeContainerResponse reInitializeContainer(
        ReInitializeContainerRequest request) throws YarnException,
        IOException {
      return null;
    }

    @Override
    public RestartContainerResponse restartContainer(ContainerId containerId)
        throws YarnException, IOException {
      return null;
    }

    @Override
    public RollbackResponse rollbackLastReInitialization(
        ContainerId containerId) throws YarnException, IOException {
      return null;
    }

    @Override
    public CommitResponse commitLastReInitialization(ContainerId containerId)
        throws YarnException, IOException {
      return null;
    }

    @Override
    public ContainerUpdateResponse updateContainer(ContainerUpdateRequest
        request) throws YarnException, IOException {
      return null;
    }

    @Override
    public GetLocalizationStatusesResponse getLocalizationStatuses(
        GetLocalizationStatusesRequest request) throws YarnException,
        IOException {
      return null;
    }
  }
  
  @SuppressWarnings("serial")
  private static class ContainerException extends YarnException {

    public ContainerException(String message) {
      super(message);
    }

    @Override
    public YarnException getCause() {
      return null;
    }
    
  }
  
}
