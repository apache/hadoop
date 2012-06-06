package org.apache.hadoop.mapreduce.v2.app.launcher;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ShuffleHandler;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncher.EventType;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.EventHandler;
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
}
