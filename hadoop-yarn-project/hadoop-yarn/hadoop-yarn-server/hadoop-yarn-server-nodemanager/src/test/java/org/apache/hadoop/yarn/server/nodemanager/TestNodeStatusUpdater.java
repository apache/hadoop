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

package org.apache.hadoop.yarn.server.nodemanager;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager.NMContext;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.security.NMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.utils.YarnServerBuilderUtils;
import org.apache.hadoop.yarn.service.Service;
import org.apache.hadoop.yarn.service.Service.STATE;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestNodeStatusUpdater {

  // temp fix until metrics system can auto-detect itself running in unit test:
  static {
    DefaultMetricsSystem.setMiniClusterMode(true);
  }

  static final Log LOG = LogFactory.getLog(TestNodeStatusUpdater.class);
  static final Path basedir =
      new Path("target", TestNodeStatusUpdater.class.getName());
  private static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  int heartBeatID = 0;
  volatile Throwable nmStartError = null;
  private final List<NodeId> registeredNodes = new ArrayList<NodeId>();
  private final Configuration conf = createNMConfig();
  private NodeManager nm;
  private boolean containerStatusBackupSuccessfully = true;
  private List<ContainerStatus> completedContainerStatusList = new ArrayList<ContainerStatus>();

  @After
  public void tearDown() {
    this.registeredNodes.clear();
    heartBeatID = 0;
    if (nm != null && nm.getServiceState() == STATE.STARTED) {
      nm.stop();
    }
    DefaultMetricsSystem.shutdown();
  }

  private class MyResourceTracker implements ResourceTracker {

    private final Context context;

    public MyResourceTracker(Context context) {
      this.context = context;
    }

    @Override
    public RegisterNodeManagerResponse registerNodeManager(
        RegisterNodeManagerRequest request) throws YarnRemoteException {
      NodeId nodeId = request.getNodeId();
      Resource resource = request.getResource();
      LOG.info("Registering " + nodeId.toString());
      // NOTE: this really should be checking against the config value
      InetSocketAddress expected = NetUtils.getConnectAddress(
          conf.getSocketAddr(YarnConfiguration.NM_ADDRESS, null, -1));
      Assert.assertEquals(NetUtils.getHostPortString(expected), nodeId.toString());
      Assert.assertEquals(5 * 1024, resource.getMemory());
      registeredNodes.add(nodeId);

      RegisterNodeManagerResponse response = recordFactory
          .newRecordInstance(RegisterNodeManagerResponse.class);
      return response;
    }

    ApplicationId applicationID = recordFactory
        .newRecordInstance(ApplicationId.class);
    ApplicationAttemptId appAttemptID = recordFactory
        .newRecordInstance(ApplicationAttemptId.class);
    ContainerId firstContainerID = recordFactory
        .newRecordInstance(ContainerId.class);
    ContainerId secondContainerID = recordFactory
        .newRecordInstance(ContainerId.class);

    private Map<ApplicationId, List<ContainerStatus>> getAppToContainerStatusMap(
        List<ContainerStatus> containers) {
      Map<ApplicationId, List<ContainerStatus>> map =
          new HashMap<ApplicationId, List<ContainerStatus>>();
      for (ContainerStatus cs : containers) {
        ApplicationId applicationId = 
            cs.getContainerId().getApplicationAttemptId().getApplicationId();
        List<ContainerStatus> appContainers = map.get(applicationId);
        if (appContainers == null) {
          appContainers = new ArrayList<ContainerStatus>();
          map.put(applicationId, appContainers);
        }
        appContainers.add(cs);
      }
      return map;
    }

    @Override
    public NodeHeartbeatResponse nodeHeartbeat(NodeHeartbeatRequest request)
        throws YarnRemoteException {
      NodeStatus nodeStatus = request.getNodeStatus();
      LOG.info("Got heartbeat number " + heartBeatID);
      NodeManagerMetrics mockMetrics = mock(NodeManagerMetrics.class);
      Dispatcher mockDispatcher = mock(Dispatcher.class);
      EventHandler mockEventHandler = mock(EventHandler.class);
      when(mockDispatcher.getEventHandler()).thenReturn(mockEventHandler);
      nodeStatus.setResponseId(heartBeatID++);
      Map<ApplicationId, List<ContainerStatus>> appToContainers =
          getAppToContainerStatusMap(nodeStatus.getContainersStatuses());
      org.apache.hadoop.yarn.api.records.Container mockContainer =
          mock(org.apache.hadoop.yarn.api.records.Container.class);
      if (heartBeatID == 1) {
        Assert.assertEquals(0, nodeStatus.getContainersStatuses().size());

        // Give a container to the NM.
        applicationID.setId(heartBeatID);
        appAttemptID.setApplicationId(applicationID);
        firstContainerID.setApplicationAttemptId(appAttemptID);
        firstContainerID.setId(heartBeatID);
        ContainerLaunchContext launchContext = recordFactory
            .newRecordInstance(ContainerLaunchContext.class);
        when(mockContainer.getId()).thenReturn(firstContainerID);
        Resource resource = BuilderUtils.newResource(2, 1);
        when(mockContainer.getResource()).thenReturn(resource);
        Container container =
            new ContainerImpl(conf, mockDispatcher, launchContext,
                mockContainer, null, mockMetrics);
        this.context.getContainers().put(firstContainerID, container);
      } else if (heartBeatID == 2) {
        // Checks on the RM end
        Assert.assertEquals("Number of applications should only be one!", 1,
            nodeStatus.getContainersStatuses().size());
        Assert.assertEquals("Number of container for the app should be one!",
            1, appToContainers.get(applicationID).size());

        // Checks on the NM end
        ConcurrentMap<ContainerId, Container> activeContainers =
            this.context.getContainers();
        Assert.assertEquals(1, activeContainers.size());

        // Give another container to the NM.
        applicationID.setId(heartBeatID);
        appAttemptID.setApplicationId(applicationID);
        secondContainerID.setApplicationAttemptId(appAttemptID);
        secondContainerID.setId(heartBeatID);
        ContainerLaunchContext launchContext = recordFactory
            .newRecordInstance(ContainerLaunchContext.class);
        when(mockContainer.getId()).thenReturn(secondContainerID);
        Resource resource = BuilderUtils.newResource(3, 1);
        when(mockContainer.getResource()).thenReturn(resource);
        Container container =
            new ContainerImpl(conf, mockDispatcher, launchContext,
                mockContainer, null, mockMetrics);
        this.context.getContainers().put(secondContainerID, container);
      } else if (heartBeatID == 3) {
        // Checks on the RM end
        Assert.assertEquals("Number of applications should only be one!", 1,
            appToContainers.size());
        Assert.assertEquals("Number of container for the app should be two!",
            2, appToContainers.get(applicationID).size());

        // Checks on the NM end
        ConcurrentMap<ContainerId, Container> activeContainers =
            this.context.getContainers();
        Assert.assertEquals(2, activeContainers.size());
      }

      NodeHeartbeatResponse nhResponse = YarnServerBuilderUtils.
          newNodeHeartbeatResponse(heartBeatID, null, null, null, null, 1000L);
      return nhResponse;
    }
  }

  private class MyNodeStatusUpdater extends NodeStatusUpdaterImpl {
    public ResourceTracker resourceTracker;
    private Context context;

    public MyNodeStatusUpdater(Context context, Dispatcher dispatcher,
        NodeHealthCheckerService healthChecker, NodeManagerMetrics metrics) {
      super(context, dispatcher, healthChecker, metrics);
      this.context = context;
      resourceTracker = new MyResourceTracker(this.context);
    }

    @Override
    protected ResourceTracker getRMClient() {
      return resourceTracker;
    }
  }

  private class MyNodeStatusUpdater2 extends NodeStatusUpdaterImpl {
    public ResourceTracker resourceTracker;

    public MyNodeStatusUpdater2(Context context, Dispatcher dispatcher,
        NodeHealthCheckerService healthChecker, NodeManagerMetrics metrics) {
      super(context, dispatcher, healthChecker, metrics);
      resourceTracker = new MyResourceTracker4(context);
    }

    @Override
    protected ResourceTracker getRMClient() {
      return resourceTracker;
    }

  }

  private class MyNodeStatusUpdater3 extends NodeStatusUpdaterImpl {
    public ResourceTracker resourceTracker;
    private Context context;

    public MyNodeStatusUpdater3(Context context, Dispatcher dispatcher,
        NodeHealthCheckerService healthChecker, NodeManagerMetrics metrics) {
      super(context, dispatcher, healthChecker, metrics);
      this.context = context;
      this.resourceTracker = new MyResourceTracker3(this.context);
    }

    @Override
    protected ResourceTracker getRMClient() {
      return resourceTracker;
    }
    
    @Override
    protected boolean isTokenKeepAliveEnabled(Configuration conf) {
      return true;
    }
  }

  private class MyNodeStatusUpdater4 extends NodeStatusUpdaterImpl {
    public ResourceTracker resourceTracker =
        new MyResourceTracker(this.context);
    private Context context;
    private final long waitStartTime;
    private final long rmStartIntervalMS;
    private final boolean rmNeverStart;

    public MyNodeStatusUpdater4(Context context, Dispatcher dispatcher,
        NodeHealthCheckerService healthChecker, NodeManagerMetrics metrics,
        long rmStartIntervalMS, boolean rmNeverStart) {
      super(context, dispatcher, healthChecker, metrics);
      this.context = context;
      this.waitStartTime = System.currentTimeMillis();
      this.rmStartIntervalMS = rmStartIntervalMS;
      this.rmNeverStart = rmNeverStart;
    }

    @Override
    protected ResourceTracker getRMClient() {
      if(System.currentTimeMillis() - waitStartTime <= rmStartIntervalMS
          || rmNeverStart) {
        throw new YarnException("Faking RM start failure as start " +
            "delay timer has not expired.");
      } else {
        return resourceTracker;
      }
    }
  }

  private class MyNodeStatusUpdater5 extends NodeStatusUpdaterImpl {
    private ResourceTracker resourceTracker;

    public MyNodeStatusUpdater5(Context context, Dispatcher dispatcher,
        NodeHealthCheckerService healthChecker, NodeManagerMetrics metrics) {
      super(context, dispatcher, healthChecker, metrics);
      resourceTracker = new MyResourceTracker5();
    }

    @Override
    protected ResourceTracker getRMClient() {
      return resourceTracker;
    }
  }

  private class MyNodeManager extends NodeManager {
    
    private MyNodeStatusUpdater3 nodeStatusUpdater;
    @Override
    protected NodeStatusUpdater createNodeStatusUpdater(Context context,
        Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
      this.nodeStatusUpdater =
          new MyNodeStatusUpdater3(context, dispatcher, healthChecker, metrics);
      return this.nodeStatusUpdater;
    }

    protected MyNodeStatusUpdater3 getNodeStatusUpdater() {
      return this.nodeStatusUpdater;
    }
  }
  
  private class MyNodeManager2 extends NodeManager {
    public boolean isStopped = false;
    private NodeStatusUpdater nodeStatusUpdater;
    private CyclicBarrier syncBarrier;
    public MyNodeManager2 (CyclicBarrier syncBarrier) {
      this.syncBarrier = syncBarrier;
    }
    @Override
    protected NodeStatusUpdater createNodeStatusUpdater(Context context,
        Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
      nodeStatusUpdater =
          new MyNodeStatusUpdater5(context, dispatcher, healthChecker,
                                     metrics);
      return nodeStatusUpdater;
    }

    @Override
    public void stop() {
      super.stop();
      isStopped = true;
      try {
        syncBarrier.await();
      } catch (Exception e) {
      }
    }
  }
  // 
  private class MyResourceTracker2 implements ResourceTracker {
    public NodeAction heartBeatNodeAction = NodeAction.NORMAL;
    public NodeAction registerNodeAction = NodeAction.NORMAL;

    @Override
    public RegisterNodeManagerResponse registerNodeManager(
        RegisterNodeManagerRequest request) throws YarnRemoteException {
      
      RegisterNodeManagerResponse response = recordFactory
          .newRecordInstance(RegisterNodeManagerResponse.class);
      response.setNodeAction(registerNodeAction );
      return response;
    }
    @Override
    public NodeHeartbeatResponse nodeHeartbeat(NodeHeartbeatRequest request)
        throws YarnRemoteException {
      NodeStatus nodeStatus = request.getNodeStatus();
      nodeStatus.setResponseId(heartBeatID++);
      
      NodeHeartbeatResponse nhResponse = YarnServerBuilderUtils.
          newNodeHeartbeatResponse(heartBeatID, heartBeatNodeAction, null,
              null, null, 1000L);
      return nhResponse;
    }
  }
  
  private class MyResourceTracker3 implements ResourceTracker {
    public NodeAction heartBeatNodeAction = NodeAction.NORMAL;
    public NodeAction registerNodeAction = NodeAction.NORMAL;
    private Map<ApplicationId, List<Long>> keepAliveRequests =
        new HashMap<ApplicationId, List<Long>>();
    private ApplicationId appId = BuilderUtils.newApplicationId(1, 1);
    private final Context context;

    MyResourceTracker3(Context context) {
      this.context = context;
    }
    
    @Override
    public RegisterNodeManagerResponse registerNodeManager(
        RegisterNodeManagerRequest request) throws YarnRemoteException {

      RegisterNodeManagerResponse response =
          recordFactory.newRecordInstance(RegisterNodeManagerResponse.class);
      response.setNodeAction(registerNodeAction);
      return response;
    }

    @Override
    public NodeHeartbeatResponse nodeHeartbeat(NodeHeartbeatRequest request)
        throws YarnRemoteException {
      LOG.info("Got heartBeatId: [" + heartBeatID +"]");
      NodeStatus nodeStatus = request.getNodeStatus();
      nodeStatus.setResponseId(heartBeatID++);
      NodeHeartbeatResponse nhResponse = YarnServerBuilderUtils.
          newNodeHeartbeatResponse(heartBeatID, heartBeatNodeAction, null,
              null, null, 1000L);

      if (nodeStatus.getKeepAliveApplications() != null
          && nodeStatus.getKeepAliveApplications().size() > 0) {
        for (ApplicationId appId : nodeStatus.getKeepAliveApplications()) {
          List<Long> list = keepAliveRequests.get(appId);
          if (list == null) {
            list = new LinkedList<Long>();
            keepAliveRequests.put(appId, list);
          }
          list.add(System.currentTimeMillis());
        }
      }
      if (heartBeatID == 2) {
        LOG.info("Sending FINISH_APP for application: [" + appId + "]");
        this.context.getApplications().put(appId, mock(Application.class));
        nhResponse.addAllApplicationsToCleanup(Collections.singletonList(appId));
      }
      return nhResponse;
    }
  }

  private class MyResourceTracker4 implements ResourceTracker {

    public NodeAction registerNodeAction = NodeAction.NORMAL;
    public NodeAction heartBeatNodeAction = NodeAction.NORMAL;
    private Context context;

    public MyResourceTracker4(Context context) {
      this.context = context;
    }

    @Override
    public RegisterNodeManagerResponse registerNodeManager(
        RegisterNodeManagerRequest request) throws YarnRemoteException {
      RegisterNodeManagerResponse response = recordFactory
          .newRecordInstance(RegisterNodeManagerResponse.class);
      response.setNodeAction(registerNodeAction);
      return response;
    }

    @Override
    public NodeHeartbeatResponse nodeHeartbeat(NodeHeartbeatRequest request)
        throws YarnRemoteException {
      try {
        if (heartBeatID == 0) {
          Assert.assertEquals(request.getNodeStatus().getContainersStatuses()
              .size(), 0);
          Assert.assertEquals(context.getContainers().size(), 0);
        } else if (heartBeatID == 1) {
          Assert.assertEquals(request.getNodeStatus().getContainersStatuses()
              .size(), 5);
          Assert.assertTrue(request.getNodeStatus().getContainersStatuses()
              .get(0).getState() == ContainerState.RUNNING
              && request.getNodeStatus().getContainersStatuses().get(0)
                  .getContainerId().getId() == 1);
          Assert.assertTrue(request.getNodeStatus().getContainersStatuses()
              .get(1).getState() == ContainerState.RUNNING
              && request.getNodeStatus().getContainersStatuses().get(1)
                  .getContainerId().getId() == 2);
          Assert.assertTrue(request.getNodeStatus().getContainersStatuses()
              .get(2).getState() == ContainerState.COMPLETE
              && request.getNodeStatus().getContainersStatuses().get(2)
                  .getContainerId().getId() == 3);
          Assert.assertTrue(request.getNodeStatus().getContainersStatuses()
              .get(3).getState() == ContainerState.COMPLETE
              && request.getNodeStatus().getContainersStatuses().get(3)
                  .getContainerId().getId() == 4);
          Assert.assertTrue(request.getNodeStatus().getContainersStatuses()
              .get(4).getState() == ContainerState.RUNNING
              && request.getNodeStatus().getContainersStatuses().get(4)
                  .getContainerId().getId() == 5);
          throw new YarnException("Lost the heartbeat response");
        } else if (heartBeatID == 2) {
          Assert.assertEquals(request.getNodeStatus().getContainersStatuses()
              .size(), 7);
          Assert.assertTrue(request.getNodeStatus().getContainersStatuses()
              .get(0).getState() == ContainerState.COMPLETE
              && request.getNodeStatus().getContainersStatuses().get(0)
                  .getContainerId().getId() == 3);
          Assert.assertTrue(request.getNodeStatus().getContainersStatuses()
              .get(1).getState() == ContainerState.COMPLETE
              && request.getNodeStatus().getContainersStatuses().get(1)
                  .getContainerId().getId() == 4);
          Assert.assertTrue(request.getNodeStatus().getContainersStatuses()
              .get(2).getState() == ContainerState.RUNNING
              && request.getNodeStatus().getContainersStatuses().get(2)
                  .getContainerId().getId() == 1);
          Assert.assertTrue(request.getNodeStatus().getContainersStatuses()
              .get(3).getState() == ContainerState.RUNNING
              && request.getNodeStatus().getContainersStatuses().get(3)
                  .getContainerId().getId() == 2);
          Assert.assertTrue(request.getNodeStatus().getContainersStatuses()
              .get(4).getState() == ContainerState.RUNNING
              && request.getNodeStatus().getContainersStatuses().get(4)
                  .getContainerId().getId() == 5);
          Assert.assertTrue(request.getNodeStatus().getContainersStatuses()
              .get(5).getState() == ContainerState.RUNNING
              && request.getNodeStatus().getContainersStatuses().get(5)
                  .getContainerId().getId() == 6);
          Assert.assertTrue(request.getNodeStatus().getContainersStatuses()
              .get(6).getState() == ContainerState.COMPLETE
              && request.getNodeStatus().getContainersStatuses().get(6)
                  .getContainerId().getId() == 7);
        }
      } catch (AssertionError error) {
        LOG.info(error);
        containerStatusBackupSuccessfully = false;
      } finally {
        heartBeatID++;
      }
      NodeStatus nodeStatus = request.getNodeStatus();
      nodeStatus.setResponseId(heartBeatID);
      NodeHeartbeatResponse nhResponse =
          YarnServerBuilderUtils.newNodeHeartbeatResponse(heartBeatID,
              heartBeatNodeAction, null, null, null, 1000L);
      return nhResponse;
    }
  }

  private class MyResourceTracker5 implements ResourceTracker {
    public NodeAction registerNodeAction = NodeAction.NORMAL;
    @Override
    public RegisterNodeManagerResponse registerNodeManager(
        RegisterNodeManagerRequest request) throws YarnRemoteException {
      
      RegisterNodeManagerResponse response = recordFactory
          .newRecordInstance(RegisterNodeManagerResponse.class);
      response.setNodeAction(registerNodeAction );
      return response;
    }
    
    @Override
    public NodeHeartbeatResponse nodeHeartbeat(NodeHeartbeatRequest request)
        throws YarnRemoteException {
      heartBeatID++;
      throw RPCUtil.getRemoteException("NodeHeartbeat exception");
    }
  }

  @Before
  public void clearError() {
    nmStartError = null;
  }

  @After
  public void deleteBaseDir() throws IOException {
    FileContext lfs = FileContext.getLocalFSFileContext();
    lfs.delete(basedir, true);
  }

  @Test
  public void testNMRegistration() throws InterruptedException {
    nm = new NodeManager() {
      @Override
      protected NodeStatusUpdater createNodeStatusUpdater(Context context,
          Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
        return new MyNodeStatusUpdater(context, dispatcher, healthChecker,
                                       metrics);
      }
    };

    YarnConfiguration conf = createNMConfig();
    nm.init(conf);

    // verify that the last service is the nodeStatusUpdater (ie registration
    // with RM)
    Object[] services  = nm.getServices().toArray();
    Object lastService = services[services.length-1];
    Assert.assertTrue("last service is NOT the node status updater",
        lastService instanceof NodeStatusUpdater);

    new Thread() {
      public void run() {
        try {
          nm.start();
        } catch (Throwable e) {
          TestNodeStatusUpdater.this.nmStartError = e;
          throw new YarnException(e);
        }
      }
    }.start();

    System.out.println(" ----- thread already started.."
        + nm.getServiceState());

    int waitCount = 0;
    while (nm.getServiceState() == STATE.INITED && waitCount++ != 20) {
      LOG.info("Waiting for NM to start..");
      if (nmStartError != null) {
        LOG.error("Error during startup. ", nmStartError);
        Assert.fail(nmStartError.getCause().getMessage());
      }
      Thread.sleep(1000);
    }
    if (nm.getServiceState() != STATE.STARTED) {
      // NM could have failed.
      Assert.fail("NodeManager failed to start");
    }

    waitCount = 0;
    while (heartBeatID <= 3 && waitCount++ != 20) {
      Thread.sleep(500);
    }
    Assert.assertFalse(heartBeatID <= 3);
    Assert.assertEquals("Number of registered NMs is wrong!!", 1,
        this.registeredNodes.size());

    nm.stop();
  }
  
  @Test
  public void testStopReentrant() throws Exception {
    final AtomicInteger numCleanups = new AtomicInteger(0);
    nm = new NodeManager() {
      @Override
      protected NodeStatusUpdater createNodeStatusUpdater(Context context,
          Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
        MyNodeStatusUpdater myNodeStatusUpdater = new MyNodeStatusUpdater(
            context, dispatcher, healthChecker, metrics);
        MyResourceTracker2 myResourceTracker2 = new MyResourceTracker2();
        myResourceTracker2.heartBeatNodeAction = NodeAction.SHUTDOWN;
        myNodeStatusUpdater.resourceTracker = myResourceTracker2;
        return myNodeStatusUpdater;
      }
      
      @Override
      protected void cleanupContainers(NodeManagerEventType eventType) {
        super.cleanupContainers(NodeManagerEventType.SHUTDOWN);
        numCleanups.incrementAndGet();
      }
    };

    YarnConfiguration conf = createNMConfig();
    nm.init(conf);
    nm.start();
    
    int waitCount = 0;
    while (heartBeatID < 1 && waitCount++ != 20) {
      Thread.sleep(500);
    }
    Assert.assertFalse(heartBeatID < 1);

    // Meanwhile call stop directly as the shutdown hook would
    nm.stop();
    
    // NM takes a while to reach the STOPPED state.
    waitCount = 0;
    while (nm.getServiceState() != STATE.STOPPED && waitCount++ != 20) {
      LOG.info("Waiting for NM to stop..");
      Thread.sleep(1000);
    }

    Assert.assertEquals(STATE.STOPPED, nm.getServiceState());
    Assert.assertEquals(numCleanups.get(), 1);
  }
  
  @Test
  public void testNodeDecommision() throws Exception {
    nm = getNodeManager(NodeAction.SHUTDOWN);
    YarnConfiguration conf = createNMConfig();
    nm.init(conf);
    Assert.assertEquals(STATE.INITED, nm.getServiceState());
    nm.start();

    int waitCount = 0;
    while (heartBeatID < 1 && waitCount++ != 20) {
      Thread.sleep(500);
    }
    Assert.assertFalse(heartBeatID < 1);

    // NM takes a while to reach the STOPPED state.
    waitCount = 0;
    while (nm.getServiceState() != STATE.STOPPED && waitCount++ != 20) {
      LOG.info("Waiting for NM to stop..");
      Thread.sleep(1000);
    }

    Assert.assertEquals(STATE.STOPPED, nm.getServiceState());
  }

  @Test
  public void testNMShutdownForRegistrationFailure() {

    nm = new NodeManager() {
      @Override
      protected NodeStatusUpdater createNodeStatusUpdater(Context context,
          Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
        MyNodeStatusUpdater nodeStatusUpdater = new MyNodeStatusUpdater(
            context, dispatcher, healthChecker, metrics);
        MyResourceTracker2 myResourceTracker2 = new MyResourceTracker2();
        myResourceTracker2.registerNodeAction = NodeAction.SHUTDOWN;
        nodeStatusUpdater.resourceTracker = myResourceTracker2;
        return nodeStatusUpdater;
      }
    };
    verifyNodeStartFailure("org.apache.hadoop.yarn.YarnException: "
        + "Recieved SHUTDOWN signal from Resourcemanager ,Registration of NodeManager failed");
  }

  @Test (timeout = 15000)
  public void testNMConnectionToRM() {
    final long delta = 1500;
    final long connectionWaitSecs = 5;
    final long connectionRetryIntervalSecs = 1;
    //Waiting for rmStartIntervalMS, RM will be started
    final long rmStartIntervalMS = 2*1000;
    YarnConfiguration conf = createNMConfig();
    conf.setLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_WAIT_SECS,
        connectionWaitSecs);
    conf.setLong(YarnConfiguration
        .RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_SECS,
        connectionRetryIntervalSecs);

    //Test NM try to connect to RM Several times, but finally fail
    nm = new NodeManager() {
      @Override
      protected NodeStatusUpdater createNodeStatusUpdater(Context context,
          Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
        NodeStatusUpdater nodeStatusUpdater = new MyNodeStatusUpdater4(
            context, dispatcher, healthChecker, metrics,
            rmStartIntervalMS, true);
        return nodeStatusUpdater;
      }
    };
    nm.init(conf);
    long waitStartTime = System.currentTimeMillis();
    try {
      nm.start();
      Assert.fail("NM should have failed to start due to RM connect failure");
    } catch(Exception e) {
      Assert.assertTrue("NM should have tried re-connecting to RM during " +
          "period of at least " + connectionWaitSecs + " seconds, but " +
          "stopped retrying within " + (connectionWaitSecs + delta/1000) +
          " seconds", (System.currentTimeMillis() - waitStartTime
              >= connectionWaitSecs*1000) && (System.currentTimeMillis()
              - waitStartTime < (connectionWaitSecs*1000+delta)));
    }

    //Test NM connect to RM, fail at first several attempts,
    //but finally success.
    nm = new NodeManager() {
      @Override
      protected NodeStatusUpdater createNodeStatusUpdater(Context context,
          Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
        NodeStatusUpdater nodeStatusUpdater = new MyNodeStatusUpdater4(
            context, dispatcher, healthChecker, metrics, rmStartIntervalMS,
            false);
        return nodeStatusUpdater;
      }
    };

    nm.init(conf);
    waitStartTime = System.currentTimeMillis();
    try {
      nm.start();
    } catch (Exception ex){
      Assert.fail("NM should have started successfully " +
          "after connecting to RM.");
    }
    Assert.assertTrue("NM should have connected to RM within " + delta/1000
        +" seconds of RM starting up.",
        (System.currentTimeMillis() - waitStartTime >= rmStartIntervalMS)
        && (System.currentTimeMillis() - waitStartTime
        < (rmStartIntervalMS+delta)));
  }

  /**
   * Verifies that if for some reason NM fails to start ContainerManager RPC
   * server, RM is oblivious to NM's presence. The behaviour is like this
   * because otherwise, NM will report to RM even if all its servers are not
   * started properly, RM will think that the NM is alive and will retire the NM
   * only after NM_EXPIRY interval. See MAPREDUCE-2749.
   */
  @Test
  public void testNoRegistrationWhenNMServicesFail() {

    nm = new NodeManager() {
      @Override
      protected NodeStatusUpdater createNodeStatusUpdater(Context context,
          Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
        return new MyNodeStatusUpdater(context, dispatcher, healthChecker,
                                       metrics);
      }

      @Override
      protected ContainerManagerImpl createContainerManager(Context context,
          ContainerExecutor exec, DeletionService del,
          NodeStatusUpdater nodeStatusUpdater,
          ApplicationACLsManager aclsManager,
          LocalDirsHandlerService diskhandler) {
        return new ContainerManagerImpl(context, exec, del, nodeStatusUpdater,
          metrics, aclsManager, diskhandler) {
          @Override
          public void start() {
            // Simulating failure of starting RPC server
            throw new YarnException("Starting of RPC Server failed");
          }
        };
      }
    };

    verifyNodeStartFailure("Starting of RPC Server failed");
  }

  @Test
  public void testApplicationKeepAlive() throws Exception {
    MyNodeManager nm = new MyNodeManager();
    try {
      YarnConfiguration conf = createNMConfig();
      conf.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, true);
      conf.setLong(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS,
          4000l);
      nm.init(conf);
      nm.start();
      // HB 2 -> app cancelled by RM.
      while (heartBeatID < 12) {
        Thread.sleep(1000l);
      }
      MyResourceTracker3 rt =
          (MyResourceTracker3) nm.getNodeStatusUpdater().getRMClient();
      rt.context.getApplications().remove(rt.appId);
      Assert.assertEquals(1, rt.keepAliveRequests.size());
      int numKeepAliveRequests = rt.keepAliveRequests.get(rt.appId).size();
      LOG.info("Number of Keep Alive Requests: [" + numKeepAliveRequests + "]");
      Assert.assertTrue(numKeepAliveRequests == 2 || numKeepAliveRequests == 3);
      while (heartBeatID < 20) {
        Thread.sleep(1000l);
      }
      int numKeepAliveRequests2 = rt.keepAliveRequests.get(rt.appId).size();
      Assert.assertEquals(numKeepAliveRequests, numKeepAliveRequests2);
    } finally {
      if (nm.getServiceState() == STATE.STARTED)
        nm.stop();
    }
  }

  /**
   * Test completed containerStatus get back up when heart beat lost
   */
  @Test(timeout = 20000)
  public void testCompletedContainerStatusBackup() throws Exception {
    nm = new NodeManager() {
      @Override
      protected NodeStatusUpdater createNodeStatusUpdater(Context context,
          Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
        MyNodeStatusUpdater2 myNodeStatusUpdater =
            new MyNodeStatusUpdater2(context, dispatcher, healthChecker,
                metrics);
        return myNodeStatusUpdater;
      }

      @Override
      protected NMContext createNMContext(
          NMContainerTokenSecretManager containerTokenSecretManager) {
        return new MyNMContext(containerTokenSecretManager);
      }

    };

    YarnConfiguration conf = createNMConfig();
    nm.init(conf);
    nm.start();

    int waitCount = 0;
    while (heartBeatID <= 3 && waitCount++ != 20) {
      Thread.sleep(500);
    }
    if(!containerStatusBackupSuccessfully) {
      Assert.fail("ContainerStatus Backup failed");
    }
    nm.stop();
  }

  @Test(timeout = 20000)
  public void testNodeStatusUpdaterRetryAndNMShutdown() 
      throws InterruptedException {
    final long connectionWaitSecs = 1;
    final long connectionRetryIntervalSecs = 1;
    YarnConfiguration conf = createNMConfig();
    conf.setLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_WAIT_SECS,
        connectionWaitSecs);
    conf.setLong(YarnConfiguration
        .RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_SECS,
        connectionRetryIntervalSecs);
    CyclicBarrier syncBarrier = new CyclicBarrier(2);
    nm = new MyNodeManager2(syncBarrier);
    nm.init(conf);
    nm.start();
    try {
      syncBarrier.await();
    } catch (Exception e) {
    }
    Assert.assertTrue(((MyNodeManager2) nm).isStopped);
    Assert.assertTrue("calculate heartBeatCount based on" +
        " connectionWaitSecs and RetryIntervalSecs", heartBeatID == 2);
  }

  private class MyNMContext extends NMContext {
    ConcurrentMap<ContainerId, Container> containers =
        new ConcurrentSkipListMap<ContainerId, Container>();

    public MyNMContext(NMContainerTokenSecretManager
        containerTokenSecretManager) {
      super(containerTokenSecretManager);
    }

    @Override
    public ConcurrentMap<ContainerId, Container> getContainers() {
      if (heartBeatID == 0) {
        return containers;
      } else if (heartBeatID == 1) {
        ContainerStatus containerStatus1 =
            createContainerStatus(1, ContainerState.RUNNING);
        Container container1 = getMockContainer(containerStatus1);
        containers.put(containerStatus1.getContainerId(), container1);

        ContainerStatus containerStatus2 =
            createContainerStatus(2, ContainerState.RUNNING);
        Container container2 = getMockContainer(containerStatus2);
        containers.put(containerStatus2.getContainerId(), container2);

        ContainerStatus containerStatus3 =
            createContainerStatus(3, ContainerState.COMPLETE);
        Container container3 = getMockContainer(containerStatus3);
        containers.put(containerStatus3.getContainerId(), container3);
        completedContainerStatusList.add(containerStatus3);

        ContainerStatus containerStatus4 =
            createContainerStatus(4, ContainerState.COMPLETE);
        Container container4 = getMockContainer(containerStatus4);
        containers.put(containerStatus4.getContainerId(), container4);
        completedContainerStatusList.add(containerStatus4);

        ContainerStatus containerStatus5 =
            createContainerStatus(5, ContainerState.RUNNING);
        Container container5 = getMockContainer(containerStatus5);
        containers.put(containerStatus5.getContainerId(), container5);

        return containers;
      } else if (heartBeatID == 2) {
        ContainerStatus containerStatus6 =
            createContainerStatus(6, ContainerState.RUNNING);
        Container container6 = getMockContainer(containerStatus6);
        containers.put(containerStatus6.getContainerId(), container6);

        ContainerStatus containerStatus7 =
            createContainerStatus(7, ContainerState.COMPLETE);
        Container container7 = getMockContainer(containerStatus7);
        containers.put(containerStatus7.getContainerId(), container7);
        completedContainerStatusList.add(containerStatus7);

        return containers;
      } else {
        containers.clear();

        return containers;
      }
    }

    private ContainerStatus createContainerStatus(int id,
        ContainerState containerState) {
      ApplicationId applicationId =
          BuilderUtils.newApplicationId(System.currentTimeMillis(), id);
      ApplicationAttemptId applicationAttemptId =
          BuilderUtils.newApplicationAttemptId(applicationId, id);
      ContainerId contaierId =
          BuilderUtils.newContainerId(applicationAttemptId, id);
      ContainerStatus containerStatus =
          BuilderUtils.newContainerStatus(contaierId, containerState,
              "test_containerStatus: id=" + id + ", containerState: "
                  + containerState, 0);
      return containerStatus;
    }

    private Container getMockContainer(ContainerStatus containerStatus) {
      Container container = mock(Container.class);
      when(container.cloneAndGetContainerStatus()).thenReturn(containerStatus);
      return container;
    }
  }

  private void verifyNodeStartFailure(String errMessage) {
    YarnConfiguration conf = createNMConfig();
    nm.init(conf);
    try {
      nm.start();
      Assert.fail("NM should have failed to start. Didn't get exception!!");
    } catch (Exception e) {
      Assert.assertEquals(errMessage, e.getCause()
          .getMessage());
    }
    
    // the state change to stopped occurs only if the startup is success, else
    // state change doesn't occur
    Assert.assertEquals("NM state is wrong!", Service.STATE.INITED, nm
        .getServiceState());

    Assert.assertEquals("Number of registered nodes is wrong!", 0,
        this.registeredNodes.size());
  }

  private YarnConfiguration createNMConfig() {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.NM_PMEM_MB, 5*1024); // 5GB
    conf.set(YarnConfiguration.NM_ADDRESS, "127.0.0.1:12345");
    conf.set(YarnConfiguration.NM_LOCALIZER_ADDRESS, "127.0.0.1:12346");
    conf.set(YarnConfiguration.NM_LOG_DIRS, new Path(basedir, "logs").toUri()
        .getPath());
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR, new Path(basedir,
        "remotelogs").toUri().getPath());
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, new Path(basedir, "nm0")
        .toUri().getPath());
    return conf;
  }
  
  private NodeManager getNodeManager(final NodeAction nodeHeartBeatAction) {
    return new NodeManager() {
      @Override
      protected NodeStatusUpdater createNodeStatusUpdater(Context context,
          Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
        MyNodeStatusUpdater myNodeStatusUpdater = new MyNodeStatusUpdater(
            context, dispatcher, healthChecker, metrics);
        MyResourceTracker2 myResourceTracker2 = new MyResourceTracker2();
        myResourceTracker2.heartBeatNodeAction = nodeHeartBeatAction;
        myNodeStatusUpdater.resourceTracker = myResourceTracker2;
        return myNodeStatusUpdater;
      }
    };
  }
}
