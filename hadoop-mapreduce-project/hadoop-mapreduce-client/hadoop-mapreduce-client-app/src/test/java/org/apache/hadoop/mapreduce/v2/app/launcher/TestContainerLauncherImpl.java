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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.atLeast;
import org.mockito.ArgumentCaptor;

import java.net.InetSocketAddress;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ShuffleHandler;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncher.EventType;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.junit.Test;

public class TestContainerLauncherImpl {
  static final Log LOG = LogFactory.getLog(TestContainerLauncherImpl.class);
  private static final RecordFactory recordFactory =
    RecordFactoryProvider.getRecordFactory(null);

  
  private static class ContainerLauncherImplUnderTest extends 
    ContainerLauncherImpl {

    private YarnRPC rpc;
    
    public ContainerLauncherImplUnderTest(AppContext context, YarnRPC rpc) {
      super(context);
      this.rpc = rpc;
    }
    
    @Override
    protected YarnRPC createYarnRPC(Configuration conf) {
      return rpc;
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
    return BuilderUtils.newContainerId(
      BuilderUtils.newApplicationAttemptId(
        BuilderUtils.newApplicationId(ts, appId), attemptId), id);
  }

  public static TaskAttemptId makeTaskAttemptId(long ts, int appId, int taskId, 
      TaskType taskType, int id) {
    ApplicationId aID = BuilderUtils.newApplicationId(ts, appId);
    JobId jID = MRBuilderUtils.newJobId(aID, id);
    TaskId tID = MRBuilderUtils.newTaskId(jID, taskId, taskType);
    return MRBuilderUtils.newTaskAttemptId(tID, id);
  }
  
  @Test
  public void testHandle() throws Exception {
    LOG.info("STARTING testHandle");
    YarnRPC mockRpc = mock(YarnRPC.class);
    AppContext mockContext = mock(AppContext.class);
    @SuppressWarnings("rawtypes")
    EventHandler mockEventHandler = mock(EventHandler.class);
    when(mockContext.getEventHandler()).thenReturn(mockEventHandler);

    ContainerManager mockCM = mock(ContainerManager.class);
    when(mockRpc.getProxy(eq(ContainerManager.class), 
        any(InetSocketAddress.class), any(Configuration.class)))
        .thenReturn(mockCM);
    
    ContainerLauncherImplUnderTest ut = 
      new ContainerLauncherImplUnderTest(mockContext, mockRpc);
    
    Configuration conf = new Configuration();
    ut.init(conf);
    ut.start();
    try {
      ContainerId contId = makeContainerId(0l, 0, 0, 1);
      TaskAttemptId taskAttemptId = makeTaskAttemptId(0l, 0, 0, TaskType.MAP, 0);
      String cmAddress = "127.0.0.1:8000";
      StartContainerResponse startResp = 
        recordFactory.newRecordInstance(StartContainerResponse.class);
      startResp.setServiceResponse(ShuffleHandler.MAPREDUCE_SHUFFLE_SERVICEID, 
          ShuffleHandler.serializeMetaData(80));
      

      LOG.info("inserting launch event");
      ContainerRemoteLaunchEvent mockLaunchEvent = 
        mock(ContainerRemoteLaunchEvent.class);
      when(mockLaunchEvent.getType())
        .thenReturn(EventType.CONTAINER_REMOTE_LAUNCH);
      when(mockLaunchEvent.getContainerID())
        .thenReturn(contId);
      when(mockLaunchEvent.getTaskAttemptID()).thenReturn(taskAttemptId);
      when(mockLaunchEvent.getContainerMgrAddress()).thenReturn(cmAddress);
      when(mockCM.startContainer(any(StartContainerRequest.class))).thenReturn(startResp);
      ut.handle(mockLaunchEvent);
      
      ut.waitForPoolToIdle();
      
      verify(mockCM).startContainer(any(StartContainerRequest.class));
      
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
      
      verify(mockCM).stopContainer(any(StopContainerRequest.class));
    } finally {
      ut.stop();
    }
  }
  
  @Test
  public void testOutOfOrder() throws Exception {
    LOG.info("STARTING testOutOfOrder");
    YarnRPC mockRpc = mock(YarnRPC.class);
    AppContext mockContext = mock(AppContext.class);
    @SuppressWarnings("rawtypes")
    EventHandler mockEventHandler = mock(EventHandler.class);
    when(mockContext.getEventHandler()).thenReturn(mockEventHandler);

    ContainerManager mockCM = mock(ContainerManager.class);
    when(mockRpc.getProxy(eq(ContainerManager.class), 
        any(InetSocketAddress.class), any(Configuration.class)))
        .thenReturn(mockCM);
    
    ContainerLauncherImplUnderTest ut = 
      new ContainerLauncherImplUnderTest(mockContext, mockRpc);
    
    Configuration conf = new Configuration();
    ut.init(conf);
    ut.start();
    try {
      ContainerId contId = makeContainerId(0l, 0, 0, 1);
      TaskAttemptId taskAttemptId = makeTaskAttemptId(0l, 0, 0, TaskType.MAP, 0);
      String cmAddress = "127.0.0.1:8000";
      StartContainerResponse startResp = 
        recordFactory.newRecordInstance(StartContainerResponse.class);
      startResp.setServiceResponse(ShuffleHandler.MAPREDUCE_SHUFFLE_SERVICEID, 
          ShuffleHandler.serializeMetaData(80));

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
      
      verify(mockCM, never()).stopContainer(any(StopContainerRequest.class));

      LOG.info("inserting launch event");
      ContainerRemoteLaunchEvent mockLaunchEvent = 
        mock(ContainerRemoteLaunchEvent.class);
      when(mockLaunchEvent.getType())
        .thenReturn(EventType.CONTAINER_REMOTE_LAUNCH);
      when(mockLaunchEvent.getContainerID())
        .thenReturn(contId);
      when(mockLaunchEvent.getTaskAttemptID()).thenReturn(taskAttemptId);
      when(mockLaunchEvent.getContainerMgrAddress()).thenReturn(cmAddress);
      when(mockCM.startContainer(any(StartContainerRequest.class))).thenReturn(startResp);
      ut.handle(mockLaunchEvent);
      
      ut.waitForPoolToIdle();
      
      verify(mockCM, never()).startContainer(any(StartContainerRequest.class));
    } finally {
      ut.stop();
    }
  }

  @Test
  public void testMyShutdown() throws Exception {
    LOG.info("in test Shutdown");

    YarnRPC mockRpc = mock(YarnRPC.class);
    AppContext mockContext = mock(AppContext.class);
    @SuppressWarnings("rawtypes")
    EventHandler mockEventHandler = mock(EventHandler.class);
    when(mockContext.getEventHandler()).thenReturn(mockEventHandler);

    ContainerManager mockCM = mock(ContainerManager.class);
    when(mockRpc.getProxy(eq(ContainerManager.class),
        any(InetSocketAddress.class), any(Configuration.class)))
        .thenReturn(mockCM);

    ContainerLauncherImplUnderTest ut =
      new ContainerLauncherImplUnderTest(mockContext, mockRpc);

    Configuration conf = new Configuration();
    ut.init(conf);
    ut.start();
    try {
      ContainerId contId = makeContainerId(0l, 0, 0, 1);
      TaskAttemptId taskAttemptId = makeTaskAttemptId(0l, 0, 0, TaskType.MAP, 0);
      String cmAddress = "127.0.0.1:8000";
      StartContainerResponse startResp =
        recordFactory.newRecordInstance(StartContainerResponse.class);
      startResp.setServiceResponse(ShuffleHandler.MAPREDUCE_SHUFFLE_SERVICEID,
          ShuffleHandler.serializeMetaData(80));

      LOG.info("inserting launch event");
      ContainerRemoteLaunchEvent mockLaunchEvent =
        mock(ContainerRemoteLaunchEvent.class);
      when(mockLaunchEvent.getType())
        .thenReturn(EventType.CONTAINER_REMOTE_LAUNCH);
      when(mockLaunchEvent.getContainerID())
        .thenReturn(contId);
      when(mockLaunchEvent.getTaskAttemptID()).thenReturn(taskAttemptId);
      when(mockLaunchEvent.getContainerMgrAddress()).thenReturn(cmAddress);
      when(mockCM.startContainer(any(StartContainerRequest.class))).thenReturn(startResp);
      ut.handle(mockLaunchEvent);

      ut.waitForPoolToIdle();

      verify(mockCM).startContainer(any(StartContainerRequest.class));

      // skip cleanup and make sure stop kills the container

    } finally {
      ut.stop();
      verify(mockCM).stopContainer(any(StopContainerRequest.class));
    }
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testContainerCleaned() throws Exception {
    LOG.info("STARTING testContainerCleaned");
    
    CyclicBarrier startLaunchBarrier = new CyclicBarrier(2);
    CyclicBarrier completeLaunchBarrier = new CyclicBarrier(2);

    YarnRPC mockRpc = mock(YarnRPC.class);
    AppContext mockContext = mock(AppContext.class);
    
    EventHandler mockEventHandler = mock(EventHandler.class);
    when(mockContext.getEventHandler()).thenReturn(mockEventHandler);

    ContainerManager mockCM = new ContainerManagerForTest(startLaunchBarrier, completeLaunchBarrier);
    when(mockRpc.getProxy(eq(ContainerManager.class), 
        any(InetSocketAddress.class), any(Configuration.class)))
        .thenReturn(mockCM);
    
    ContainerLauncherImplUnderTest ut = 
      new ContainerLauncherImplUnderTest(mockContext, mockRpc);
    
    Configuration conf = new Configuration();
    ut.init(conf);
    ut.start();
    try {
      ContainerId contId = makeContainerId(0l, 0, 0, 1);
      TaskAttemptId taskAttemptId = makeTaskAttemptId(0l, 0, 0, TaskType.MAP, 0);
      String cmAddress = "127.0.0.1:8000";
      StartContainerResponse startResp = 
        recordFactory.newRecordInstance(StartContainerResponse.class);
      startResp.setServiceResponse(ShuffleHandler.MAPREDUCE_SHUFFLE_SERVICEID, 
          ShuffleHandler.serializeMetaData(80));
      
     
      LOG.info("inserting launch event");
      ContainerRemoteLaunchEvent mockLaunchEvent = 
        mock(ContainerRemoteLaunchEvent.class);
      when(mockLaunchEvent.getType())
        .thenReturn(EventType.CONTAINER_REMOTE_LAUNCH);
      when(mockLaunchEvent.getContainerID())
        .thenReturn(contId);
      when(mockLaunchEvent.getTaskAttemptID()).thenReturn(taskAttemptId);
      when(mockLaunchEvent.getContainerMgrAddress()).thenReturn(cmAddress);
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
  
  private static class ContainerManagerForTest implements ContainerManager {

    private CyclicBarrier startLaunchBarrier;
    private CyclicBarrier completeLaunchBarrier;

    ContainerManagerForTest (CyclicBarrier startLaunchBarrier, CyclicBarrier completeLaunchBarrier) {
      this.startLaunchBarrier = startLaunchBarrier;
      this.completeLaunchBarrier = completeLaunchBarrier;
    }
    @Override
    public StartContainerResponse startContainer(StartContainerRequest request)
        throws YarnRemoteException {
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
      
      throw new ContainerException("Force fail CM");
      
    }

    @Override
    public StopContainerResponse stopContainer(StopContainerRequest request)
        throws YarnRemoteException {
    
      return null;
    }

    @Override
    public GetContainerStatusResponse getContainerStatus(
        GetContainerStatusRequest request) throws YarnRemoteException {
    
      return null;
    }
  }
  
  @SuppressWarnings("serial")
  private static class ContainerException extends YarnRemoteException {

    public ContainerException(String message) {
      super(message);
    }

    @Override
    public String getRemoteTrace() {
      return null;
    }

    @Override
    public YarnRemoteException getCause() {
      return null;
    }
    
  }
  
}
