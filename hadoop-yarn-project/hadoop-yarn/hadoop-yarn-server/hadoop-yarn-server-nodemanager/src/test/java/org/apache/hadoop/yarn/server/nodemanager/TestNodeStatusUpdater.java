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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.RMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.api.records.impl.pb.MasterKeyPBImpl;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager.NMContext;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.security.NMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.nodemanager.security.NMTokenSecretManagerInNM;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.utils.YarnServerBuilderUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("rawtypes")
public class TestNodeStatusUpdater {

  // temp fix until metrics system can auto-detect itself running in unit test:
  static {
    DefaultMetricsSystem.setMiniClusterMode(true);
  }

  static final Log LOG = LogFactory.getLog(TestNodeStatusUpdater.class);
  static final File basedir =
      new File("target", TestNodeStatusUpdater.class.getName());
  static final File nmLocalDir = new File(basedir, "nm0");
  static final File tmpDir = new File(basedir, "tmpDir");
  static final File remoteLogsDir = new File(basedir, "remotelogs");
  static final File logsDir = new File(basedir, "logs");
  private static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  volatile int heartBeatID = 0;
  volatile Throwable nmStartError = null;
  private final List<NodeId> registeredNodes = new ArrayList<NodeId>();
  private boolean triggered = false;
  private Configuration conf;
  private NodeManager nm;
  private AtomicBoolean assertionFailedInThread = new AtomicBoolean(false);

  @Before
  public void setUp() {
    nmLocalDir.mkdirs();
    tmpDir.mkdirs();
    logsDir.mkdirs();
    remoteLogsDir.mkdirs();
    conf = createNMConfig();
  }

  @After
  public void tearDown() {
    this.registeredNodes.clear();
    heartBeatID = 0;
    ServiceOperations.stop(nm);
    assertionFailedInThread.set(false);
    DefaultMetricsSystem.shutdown();
  }

  public static MasterKey createMasterKey() {
    MasterKey masterKey = new MasterKeyPBImpl();
    masterKey.setKeyId(123);
    masterKey.setBytes(ByteBuffer.wrap(new byte[] { new Integer(123)
      .byteValue() }));
    return masterKey;
  }

  private class MyResourceTracker implements ResourceTracker {

    private final Context context;

    public MyResourceTracker(Context context) {
      this.context = context;
    }

    @Override
    public RegisterNodeManagerResponse registerNodeManager(
        RegisterNodeManagerRequest request) throws YarnException,
        IOException {
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
      response.setContainerTokenMasterKey(createMasterKey());
      response.setNMTokenMasterKey(createMasterKey());
      return response;
    }

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
        throws YarnException, IOException {
      NodeStatus nodeStatus = request.getNodeStatus();
      LOG.info("Got heartbeat number " + heartBeatID);
      NodeManagerMetrics mockMetrics = mock(NodeManagerMetrics.class);
      Dispatcher mockDispatcher = mock(Dispatcher.class);
      EventHandler mockEventHandler = mock(EventHandler.class);
      when(mockDispatcher.getEventHandler()).thenReturn(mockEventHandler);
      nodeStatus.setResponseId(heartBeatID++);
      Map<ApplicationId, List<ContainerStatus>> appToContainers =
          getAppToContainerStatusMap(nodeStatus.getContainersStatuses());
      
      ApplicationId appId1 = ApplicationId.newInstance(0, 1);
      ApplicationId appId2 = ApplicationId.newInstance(0, 2);
      
      if (heartBeatID == 1) {
        Assert.assertEquals(0, nodeStatus.getContainersStatuses().size());

        // Give a container to the NM.
        ApplicationAttemptId appAttemptID =
            ApplicationAttemptId.newInstance(appId1, 0);
        ContainerId firstContainerID =
            ContainerId.newInstance(appAttemptID, heartBeatID);
        ContainerLaunchContext launchContext = recordFactory
            .newRecordInstance(ContainerLaunchContext.class);
        Resource resource = BuilderUtils.newResource(2, 1);
        long currentTime = System.currentTimeMillis();
        String user = "testUser";
        ContainerTokenIdentifier containerToken = BuilderUtils
            .newContainerTokenIdentifier(BuilderUtils.newContainerToken(
                firstContainerID, InetAddress.getByName("localhost")
                    .getCanonicalHostName(), 1234, user, resource,
                currentTime + 10000, 123, "password".getBytes(), currentTime));
        Container container =
            new ContainerImpl(conf, mockDispatcher, launchContext, null,
              mockMetrics, containerToken);
        this.context.getContainers().put(firstContainerID, container);
      } else if (heartBeatID == 2) {
        // Checks on the RM end
        Assert.assertEquals("Number of applications should only be one!", 1,
            nodeStatus.getContainersStatuses().size());
        Assert.assertEquals("Number of container for the app should be one!",
            1, appToContainers.get(appId1).size());

        // Checks on the NM end
        ConcurrentMap<ContainerId, Container> activeContainers =
            this.context.getContainers();
        Assert.assertEquals(1, activeContainers.size());

        // Give another container to the NM.
        ApplicationAttemptId appAttemptID =
            ApplicationAttemptId.newInstance(appId2, 0);
        ContainerId secondContainerID =
            ContainerId.newInstance(appAttemptID, heartBeatID);
        ContainerLaunchContext launchContext = recordFactory
            .newRecordInstance(ContainerLaunchContext.class);
        long currentTime = System.currentTimeMillis();
        String user = "testUser";
        Resource resource = BuilderUtils.newResource(3, 1);
        ContainerTokenIdentifier containerToken = BuilderUtils
            .newContainerTokenIdentifier(BuilderUtils.newContainerToken(
                secondContainerID, InetAddress.getByName("localhost")
                    .getCanonicalHostName(), 1234, user, resource,
                currentTime + 10000, 123, "password".getBytes(), currentTime));
        Container container =
            new ContainerImpl(conf, mockDispatcher, launchContext, null,
              mockMetrics, containerToken);
        this.context.getContainers().put(secondContainerID, container);
      } else if (heartBeatID == 3) {
        // Checks on the RM end
        Assert.assertEquals("Number of applications should have two!", 2,
            appToContainers.size());
        Assert.assertEquals("Number of container for the app-1 should be only one!",
            1, appToContainers.get(appId1).size());
        Assert.assertEquals("Number of container for the app-2 should be only one!",
            1, appToContainers.get(appId2).size());

        // Checks on the NM end
        ConcurrentMap<ContainerId, Container> activeContainers =
            this.context.getContainers();
        Assert.assertEquals(2, activeContainers.size());
      }

      NodeHeartbeatResponse nhResponse = YarnServerBuilderUtils.
          newNodeHeartbeatResponse(heartBeatID, null, null, null, null, null,
            1000L);
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

    @Override
    protected void stopRMProxy() {
      return;
    }
  }

  // Test NodeStatusUpdater sends the right container statuses each time it
  // heart beats.
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

    @Override
    protected void stopRMProxy() {
      return;
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
    protected void stopRMProxy() {
      return;
    }

    @Override
    protected boolean isTokenKeepAliveEnabled(Configuration conf) {
      return true;
    }
  }

  private class MyNodeStatusUpdater4 extends NodeStatusUpdaterImpl {

    private final long rmStartIntervalMS;
    private final boolean rmNeverStart;
    public ResourceTracker resourceTracker;
    public MyNodeStatusUpdater4(Context context, Dispatcher dispatcher,
        NodeHealthCheckerService healthChecker, NodeManagerMetrics metrics,
        long rmStartIntervalMS, boolean rmNeverStart) {
      super(context, dispatcher, healthChecker, metrics);
      this.rmStartIntervalMS = rmStartIntervalMS;
      this.rmNeverStart = rmNeverStart;
    }

    @Override
    protected void serviceStart() throws Exception {
      //record the startup time
      super.serviceStart();
    }

    @Override
    protected ResourceTracker getRMClient() throws IOException {
      RetryPolicy retryPolicy = RMProxy.createRetryPolicy(conf);
      resourceTracker =
          (ResourceTracker) RetryProxy.create(ResourceTracker.class,
            new MyResourceTracker6(rmStartIntervalMS, rmNeverStart),
            retryPolicy);
      return resourceTracker;
    }

    private boolean isTriggered() {
      return triggered;
    }

    @Override
    protected void stopRMProxy() {
      return;
    }
  }



  private class MyNodeStatusUpdater5 extends NodeStatusUpdaterImpl {
    private ResourceTracker resourceTracker;
    private Configuration conf;

    public MyNodeStatusUpdater5(Context context, Dispatcher dispatcher,
        NodeHealthCheckerService healthChecker, NodeManagerMetrics metrics, Configuration conf) {
      super(context, dispatcher, healthChecker, metrics);
      resourceTracker = new MyResourceTracker5();
      this.conf = conf;
    }

    @Override
    protected ResourceTracker getRMClient() {
      RetryPolicy retryPolicy = RMProxy.createRetryPolicy(conf);
      return (ResourceTracker) RetryProxy.create(ResourceTracker.class,
        resourceTracker, retryPolicy);
    }

    @Override
    protected void stopRMProxy() {
      return;
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

    public MyNodeStatusUpdater3 getNodeStatusUpdater() {
      return this.nodeStatusUpdater;
    }
  }
  
  private class MyNodeManager2 extends NodeManager {
    public boolean isStopped = false;
    private NodeStatusUpdater nodeStatusUpdater;
    private CyclicBarrier syncBarrier;
    private Configuration conf;

    public MyNodeManager2 (CyclicBarrier syncBarrier, Configuration conf) {
      this.syncBarrier = syncBarrier;
      this.conf = conf;
    }
    @Override
    protected NodeStatusUpdater createNodeStatusUpdater(Context context,
        Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
      nodeStatusUpdater =
          new MyNodeStatusUpdater5(context, dispatcher, healthChecker,
                                     metrics, conf);
      return nodeStatusUpdater;
    }

    @Override
    protected void serviceStop() throws Exception {
      System.out.println("Called stooppppp");
      super.serviceStop();
      isStopped = true;
      ConcurrentMap<ApplicationId, Application> applications =
          getNMContext().getApplications();
      // ensure that applications are empty
      if(!applications.isEmpty()) {
        assertionFailedInThread.set(true);
      }
      syncBarrier.await(10000, TimeUnit.MILLISECONDS);
    }
  }
  // 
  private class MyResourceTracker2 implements ResourceTracker {
    public NodeAction heartBeatNodeAction = NodeAction.NORMAL;
    public NodeAction registerNodeAction = NodeAction.NORMAL;
    public String shutDownMessage = "";
    public String rmVersion = "3.0.1";

    @Override
    public RegisterNodeManagerResponse registerNodeManager(
        RegisterNodeManagerRequest request) throws YarnException,
        IOException {
      
      RegisterNodeManagerResponse response = recordFactory
          .newRecordInstance(RegisterNodeManagerResponse.class);
      response.setNodeAction(registerNodeAction );
      response.setContainerTokenMasterKey(createMasterKey());
      response.setNMTokenMasterKey(createMasterKey());
      response.setDiagnosticsMessage(shutDownMessage);
      response.setRMVersion(rmVersion);
      return response;
    }
    @Override
    public NodeHeartbeatResponse nodeHeartbeat(NodeHeartbeatRequest request)
        throws YarnException, IOException {
      NodeStatus nodeStatus = request.getNodeStatus();
      nodeStatus.setResponseId(heartBeatID++);
      
      NodeHeartbeatResponse nhResponse = YarnServerBuilderUtils.
          newNodeHeartbeatResponse(heartBeatID, heartBeatNodeAction, null,
              null, null, null, 1000L);
      nhResponse.setDiagnosticsMessage(shutDownMessage);
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
        RegisterNodeManagerRequest request) throws YarnException,
        IOException {

      RegisterNodeManagerResponse response =
          recordFactory.newRecordInstance(RegisterNodeManagerResponse.class);
      response.setNodeAction(registerNodeAction);
      response.setContainerTokenMasterKey(createMasterKey());
      response.setNMTokenMasterKey(createMasterKey());
      return response;
    }

    @Override
    public NodeHeartbeatResponse nodeHeartbeat(NodeHeartbeatRequest request)
        throws YarnException, IOException {
      LOG.info("Got heartBeatId: [" + heartBeatID +"]");
      NodeStatus nodeStatus = request.getNodeStatus();
      nodeStatus.setResponseId(heartBeatID++);
      NodeHeartbeatResponse nhResponse = YarnServerBuilderUtils.
          newNodeHeartbeatResponse(heartBeatID, heartBeatNodeAction, null,
              null, null, null, 1000L);

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

  // Test NodeStatusUpdater sends the right container statuses each time it
  // heart beats.
  private class MyResourceTracker4 implements ResourceTracker {

    public NodeAction registerNodeAction = NodeAction.NORMAL;
    public NodeAction heartBeatNodeAction = NodeAction.NORMAL;
    private Context context;

    public MyResourceTracker4(Context context) {
      this.context = context;
    }

    @Override
    public RegisterNodeManagerResponse registerNodeManager(
        RegisterNodeManagerRequest request) throws YarnException, IOException {
      RegisterNodeManagerResponse response =
          recordFactory.newRecordInstance(RegisterNodeManagerResponse.class);
      response.setNodeAction(registerNodeAction);
      response.setContainerTokenMasterKey(createMasterKey());
      response.setNMTokenMasterKey(createMasterKey());
      return response;
    }

    @Override
    public NodeHeartbeatResponse nodeHeartbeat(NodeHeartbeatRequest request)
        throws YarnException, IOException {
      try {
        if (heartBeatID == 0) {
          Assert.assertEquals(request.getNodeStatus().getContainersStatuses()
            .size(), 0);
          Assert.assertEquals(context.getContainers().size(), 0);
        } else if (heartBeatID == 1) {
          List<ContainerStatus> statuses =
              request.getNodeStatus().getContainersStatuses();
          Assert.assertEquals(statuses.size(), 2);
          Assert.assertEquals(context.getContainers().size(), 2);

          ContainerStatus containerStatus2 =
              createContainerStatus(2, ContainerState.RUNNING);
          ContainerStatus containerStatus3 =
              createContainerStatus(3, ContainerState.COMPLETE);
          boolean container2Exist = false, container3Exist = false;
          for (ContainerStatus status : statuses) {
            if (status.getContainerId().equals(
              containerStatus2.getContainerId())) {
              Assert.assertTrue(status.getState().equals(
                containerStatus2.getState()));
              container2Exist = true;
            }
            if (status.getContainerId().equals(
              containerStatus3.getContainerId())) {
              Assert.assertTrue(status.getState().equals(
                containerStatus3.getState()));
              container3Exist = true;
            }
          }
          Assert.assertTrue(container2Exist && container3Exist);

          // should throw exception that can be retried by the
          // nodeStatusUpdaterRunnable, otherwise nm just shuts down and the
          // test passes.
          throw new YarnRuntimeException("Lost the heartbeat response");
        } else if (heartBeatID == 2) {
          List<ContainerStatus> statuses =
              request.getNodeStatus().getContainersStatuses();
          Assert.assertEquals(statuses.size(), 4);
          Assert.assertEquals(context.getContainers().size(), 4);

          ContainerStatus containerStatus2 =
              createContainerStatus(2, ContainerState.RUNNING);
          ContainerStatus containerStatus3 =
              createContainerStatus(3, ContainerState.COMPLETE);
          ContainerStatus containerStatus4 =
              createContainerStatus(4, ContainerState.RUNNING);
          ContainerStatus containerStatus5 =
              createContainerStatus(5, ContainerState.COMPLETE);

          boolean container2Exist = false, container3Exist = false, container4Exist =
              false, container5Exist = false;
          for (ContainerStatus status : statuses) {
            if (status.getContainerId().equals(
              containerStatus2.getContainerId())) {
              Assert.assertTrue(status.getState().equals(
                containerStatus2.getState()));
              container2Exist = true;
            }
            if (status.getContainerId().equals(
              containerStatus3.getContainerId())) {
              Assert.assertTrue(status.getState().equals(
                containerStatus3.getState()));
              container3Exist = true;
            }
            if (status.getContainerId().equals(
              containerStatus4.getContainerId())) {
              Assert.assertTrue(status.getState().equals(
                containerStatus4.getState()));
              container4Exist = true;
            }
            if (status.getContainerId().equals(
              containerStatus5.getContainerId())) {
              Assert.assertTrue(status.getState().equals(
                containerStatus5.getState()));
              container5Exist = true;
            }
          }
          Assert.assertTrue(container2Exist && container3Exist
              && container4Exist && container5Exist);
        }
      } catch (AssertionError error) {
        error.printStackTrace();
        assertionFailedInThread.set(true);
      } finally {
        heartBeatID++;
      }
      NodeStatus nodeStatus = request.getNodeStatus();
      nodeStatus.setResponseId(heartBeatID);
      NodeHeartbeatResponse nhResponse =
          YarnServerBuilderUtils.newNodeHeartbeatResponse(heartBeatID,
            heartBeatNodeAction, null, null, null, null, 1000L);
      return nhResponse;
    }
  }

  private class MyResourceTracker5 implements ResourceTracker {
    public NodeAction registerNodeAction = NodeAction.NORMAL;
    @Override
    public RegisterNodeManagerResponse registerNodeManager(
        RegisterNodeManagerRequest request) throws YarnException,
        IOException {
      
      RegisterNodeManagerResponse response = recordFactory
          .newRecordInstance(RegisterNodeManagerResponse.class);
      response.setNodeAction(registerNodeAction );
      response.setContainerTokenMasterKey(createMasterKey());
      response.setNMTokenMasterKey(createMasterKey());
      return response;
    }
    
    @Override
    public NodeHeartbeatResponse nodeHeartbeat(NodeHeartbeatRequest request)
        throws YarnException, IOException {
      heartBeatID++;
      throw new java.net.ConnectException(
          "NodeHeartbeat exception");
    }
  }

  private class MyResourceTracker6 implements ResourceTracker {

    private long rmStartIntervalMS;
    private boolean rmNeverStart;
    private final long waitStartTime;

    public MyResourceTracker6(long rmStartIntervalMS, boolean rmNeverStart) {
      this.rmStartIntervalMS = rmStartIntervalMS;
      this.rmNeverStart = rmNeverStart;
      this.waitStartTime = System.currentTimeMillis();
    }

    @Override
    public RegisterNodeManagerResponse registerNodeManager(
        RegisterNodeManagerRequest request) throws YarnException, IOException,
        IOException {
      if (System.currentTimeMillis() - waitStartTime <= rmStartIntervalMS
          || rmNeverStart) {
        throw new java.net.ConnectException("Faking RM start failure as start "
            + "delay timer has not expired.");
      } else {
        NodeId nodeId = request.getNodeId();
        Resource resource = request.getResource();
        LOG.info("Registering " + nodeId.toString());
        // NOTE: this really should be checking against the config value
        InetSocketAddress expected = NetUtils.getConnectAddress(
            conf.getSocketAddr(YarnConfiguration.NM_ADDRESS, null, -1));
        Assert.assertEquals(NetUtils.getHostPortString(expected),
            nodeId.toString());
        Assert.assertEquals(5 * 1024, resource.getMemory());
        registeredNodes.add(nodeId);

        RegisterNodeManagerResponse response = recordFactory
            .newRecordInstance(RegisterNodeManagerResponse.class);
        triggered = true;
        return response;
      }
    }

    @Override
    public NodeHeartbeatResponse nodeHeartbeat(NodeHeartbeatRequest request)
        throws YarnException, IOException {
      NodeStatus nodeStatus = request.getNodeStatus();
      nodeStatus.setResponseId(heartBeatID++);

      NodeHeartbeatResponse nhResponse = YarnServerBuilderUtils.
          newNodeHeartbeatResponse(heartBeatID, NodeAction.NORMAL, null,
              null, null, null, 1000L);
      return nhResponse;
    }
  }

  @Before
  public void clearError() {
    nmStartError = null;
  }

  @After
  public void deleteBaseDir() throws IOException {
    FileContext lfs = FileContext.getLocalFSFileContext();
    lfs.delete(new Path(basedir.getPath()), true);
  }

  @Test(timeout = 90000)                                                      
  public void testRecentlyFinishedContainers() throws Exception {             
    NodeManager nm = new NodeManager();                                       
    YarnConfiguration conf = new YarnConfiguration();                         
    conf.set(                                                                 
        NodeStatusUpdaterImpl.YARN_NODEMANAGER_DURATION_TO_TRACK_STOPPED_CONTAINERS,
        "10000");                                                             
    nm.init(conf);                                                            
    NodeStatusUpdaterImpl nodeStatusUpdater =                                 
        (NodeStatusUpdaterImpl) nm.getNodeStatusUpdater();                    
    ApplicationId appId = ApplicationId.newInstance(0, 0);                    
    ApplicationAttemptId appAttemptId =                                       
        ApplicationAttemptId.newInstance(appId, 0);                           
    ContainerId cId = ContainerId.newInstance(appAttemptId, 0);               
                                                                              
                                                                              
    nodeStatusUpdater.updateStoppedContainersInCache(cId);
    Assert.assertTrue(nodeStatusUpdater.isContainerRecentlyStopped(cId));     
                                                                              
    long time1 = System.currentTimeMillis();                                  
    int waitInterval = 15;                                                    
    while (waitInterval-- > 0                                                 
        && nodeStatusUpdater.isContainerRecentlyStopped(cId)) {               
      nodeStatusUpdater.removeVeryOldStoppedContainersFromCache();          
      Thread.sleep(1000);                                                     
    }                                                                         
    long time2 = System.currentTimeMillis();                                  
    // By this time the container will be removed from cache. need to verify.
    Assert.assertFalse(nodeStatusUpdater.isContainerRecentlyStopped(cId));    
    Assert.assertTrue((time2 - time1) >= 10000 && (time2 -time1) <= 250000);  
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
          throw new YarnRuntimeException(e);
        }
      }
    }.start();

    System.out.println(" ----- thread already started.."
        + nm.getServiceState());

    int waitCount = 0;
    while (nm.getServiceState() == STATE.INITED && waitCount++ != 50) {
      LOG.info("Waiting for NM to start..");
      if (nmStartError != null) {
        LOG.error("Error during startup. ", nmStartError);
        Assert.fail(nmStartError.getCause().getMessage());
      }
      Thread.sleep(2000);
    }
    if (nm.getServiceState() != STATE.STARTED) {
      // NM could have failed.
      Assert.fail("NodeManager failed to start");
    }

    waitCount = 0;
    while (heartBeatID <= 3 && waitCount++ != 200) {
      Thread.sleep(1000);
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
      protected ContainerManagerImpl createContainerManager(Context context,
          ContainerExecutor exec, DeletionService del,
          NodeStatusUpdater nodeStatusUpdater,
          ApplicationACLsManager aclsManager,
          LocalDirsHandlerService dirsHandler) {
        return new ContainerManagerImpl(context, exec, del, nodeStatusUpdater,
            metrics, aclsManager, dirsHandler) {

          @Override
          public void cleanUpApplicationsOnNMShutDown() {
            super.cleanUpApplicationsOnNMShutDown();
            numCleanups.incrementAndGet();
          }
        };
      }
    };

    YarnConfiguration conf = createNMConfig();
    nm.init(conf);
    nm.start();
    
    int waitCount = 0;
    while (heartBeatID < 1 && waitCount++ != 200) {
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
    while (heartBeatID < 1 && waitCount++ != 200) {
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

  private abstract class NodeManagerWithCustomNodeStatusUpdater extends NodeManager {
    private NodeStatusUpdater updater;

    private NodeManagerWithCustomNodeStatusUpdater() {
    }

    @Override
    protected NodeStatusUpdater createNodeStatusUpdater(Context context,
                                                        Dispatcher dispatcher,
                                                        NodeHealthCheckerService healthChecker) {
      updater = createUpdater(context, dispatcher, healthChecker);
      return updater;
    }

    public NodeStatusUpdater getUpdater() {
      return updater;
    }

    abstract NodeStatusUpdater createUpdater(Context context,
                                                       Dispatcher dispatcher,
                                                       NodeHealthCheckerService healthChecker);
  }

  @Test
  public void testNMShutdownForRegistrationFailure() throws Exception {

    nm = new NodeManagerWithCustomNodeStatusUpdater() {
      @Override
      protected NodeStatusUpdater createUpdater(Context context,
          Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
        MyNodeStatusUpdater nodeStatusUpdater = new MyNodeStatusUpdater(
            context, dispatcher, healthChecker, metrics);
        MyResourceTracker2 myResourceTracker2 = new MyResourceTracker2();
        myResourceTracker2.registerNodeAction = NodeAction.SHUTDOWN;
        myResourceTracker2.shutDownMessage = "RM Shutting Down Node";
        nodeStatusUpdater.resourceTracker = myResourceTracker2;
        return nodeStatusUpdater;
      }
    };
    verifyNodeStartFailure(
          "Recieved SHUTDOWN signal from Resourcemanager ,"
        + "Registration of NodeManager failed, "
        + "Message from ResourceManager: RM Shutting Down Node");
  }

  @Test (timeout = 150000)
  public void testNMConnectionToRM() throws Exception {
    final long delta = 50000;
    final long connectionWaitMs = 5000;
    final long connectionRetryIntervalMs = 1000;
    //Waiting for rmStartIntervalMS, RM will be started
    final long rmStartIntervalMS = 2*1000;
    conf.setLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS,
        connectionWaitMs);
    conf.setLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS,
        connectionRetryIntervalMs);

    //Test NM try to connect to RM Several times, but finally fail
    NodeManagerWithCustomNodeStatusUpdater nmWithUpdater;
    nm = nmWithUpdater = new NodeManagerWithCustomNodeStatusUpdater() {
      @Override
      protected NodeStatusUpdater createUpdater(Context context,
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
      long t = System.currentTimeMillis();
      long duration = t - waitStartTime;
      boolean waitTimeValid = (duration >= connectionWaitMs)
              && (duration < (connectionWaitMs + delta));
      if(!waitTimeValid) {
        //either the exception was too early, or it had a different cause.
        //reject with the inner stack trace
        throw new Exception("NM should have tried re-connecting to RM during " +
          "period of at least " + connectionWaitMs + " ms, but " +
          "stopped retrying within " + (connectionWaitMs + delta) +
          " ms: " + e, e);
      }
    }

    //Test NM connect to RM, fail at first several attempts,
    //but finally success.
    nm = nmWithUpdater = new NodeManagerWithCustomNodeStatusUpdater() {
      @Override
      protected NodeStatusUpdater createUpdater(Context context,
          Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
        NodeStatusUpdater nodeStatusUpdater = new MyNodeStatusUpdater4(
            context, dispatcher, healthChecker, metrics, rmStartIntervalMS,
            false);
        return nodeStatusUpdater;
      }
    };
    nm.init(conf);
    NodeStatusUpdater updater = nmWithUpdater.getUpdater();
    Assert.assertNotNull("Updater not yet created ", updater);
    waitStartTime = System.currentTimeMillis();
    try {
      nm.start();
    } catch (Exception ex){
      LOG.error("NM should have started successfully " +
                "after connecting to RM.", ex);
      throw ex;
    }
    long duration = System.currentTimeMillis() - waitStartTime;
    MyNodeStatusUpdater4 myUpdater = (MyNodeStatusUpdater4) updater;
    Assert.assertTrue("NM started before updater triggered",
                      myUpdater.isTriggered());
    Assert.assertTrue("NM should have connected to RM after "
        +"the start interval of " + rmStartIntervalMS
        +": actual " + duration
        + " " + myUpdater,
        (duration >= rmStartIntervalMS));
    Assert.assertTrue("NM should have connected to RM less than "
        + (rmStartIntervalMS + delta)
        +" milliseconds of RM starting up: actual " + duration
        + " " + myUpdater,
        (duration < (rmStartIntervalMS + delta)));
  }

  /**
   * Verifies that if for some reason NM fails to start ContainerManager RPC
   * server, RM is oblivious to NM's presence. The behaviour is like this
   * because otherwise, NM will report to RM even if all its servers are not
   * started properly, RM will think that the NM is alive and will retire the NM
   * only after NM_EXPIRY interval. See MAPREDUCE-2749.
   */
  @Test
  public void testNoRegistrationWhenNMServicesFail() throws Exception {

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
          protected void serviceStart() {
            // Simulating failure of starting RPC server
            throw new YarnRuntimeException("Starting of RPC Server failed");
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
   * Test completed containerStatus get back up when heart beat lost, and will
   * be sent via next heart beat.
   */
  @Test(timeout = 200000)
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
          NMContainerTokenSecretManager containerTokenSecretManager,
          NMTokenSecretManagerInNM nmTokenSecretManager) {
        return new MyNMContext(containerTokenSecretManager,
          nmTokenSecretManager);
      }
    };

    YarnConfiguration conf = createNMConfig();
    nm.init(conf);
    nm.start();

    int waitCount = 0;
    while (heartBeatID <= 3 && waitCount++ != 20) {
      Thread.sleep(500);
    }
    if(assertionFailedInThread.get()) {
      Assert.fail("ContainerStatus Backup failed");
    }
    nm.stop();
  }

  @Test(timeout = 200000)
  public void testNodeStatusUpdaterRetryAndNMShutdown() 
      throws Exception {
    final long connectionWaitSecs = 1000;
    final long connectionRetryIntervalMs = 1000;
    YarnConfiguration conf = createNMConfig();
    conf.setLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS,
        connectionWaitSecs);
    conf.setLong(YarnConfiguration
        .RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS,
        connectionRetryIntervalMs);
    conf.setLong(YarnConfiguration.NM_SLEEP_DELAY_BEFORE_SIGKILL_MS, 5000);
    conf.setLong(YarnConfiguration.NM_LOG_RETAIN_SECONDS, 1);
    CyclicBarrier syncBarrier = new CyclicBarrier(2);
    nm = new MyNodeManager2(syncBarrier, conf);
    nm.init(conf);
    nm.start();
    // start a container
    ContainerId cId = TestNodeManagerShutdown.createContainerId();
    FileContext localFS = FileContext.getLocalFSFileContext();
    TestNodeManagerShutdown.startContainer(nm, cId, localFS, nmLocalDir,
      new File("start_file.txt"));

    try {
      syncBarrier.await(10000, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
    }
    Assert.assertFalse("Containers not cleaned up when NM stopped",
      assertionFailedInThread.get());
    Assert.assertTrue(((MyNodeManager2) nm).isStopped);
    Assert.assertTrue("calculate heartBeatCount based on" +
        " connectionWaitSecs and RetryIntervalSecs", heartBeatID == 2);
  }

  @Test
  public void testRMVersionLessThanMinimum() throws InterruptedException {
    final AtomicInteger numCleanups = new AtomicInteger(0);
    YarnConfiguration conf = createNMConfig();
    conf.set(YarnConfiguration.NM_RESOURCEMANAGER_MINIMUM_VERSION, "3.0.0");
    nm = new NodeManager() {
      @Override
      protected NodeStatusUpdater createNodeStatusUpdater(Context context,
                                                          Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
        MyNodeStatusUpdater myNodeStatusUpdater = new MyNodeStatusUpdater(
            context, dispatcher, healthChecker, metrics);
        MyResourceTracker2 myResourceTracker2 = new MyResourceTracker2();
        myResourceTracker2.heartBeatNodeAction = NodeAction.NORMAL;
        myResourceTracker2.rmVersion = "3.0.0";
        myNodeStatusUpdater.resourceTracker = myResourceTracker2;
        return myNodeStatusUpdater;
      }

      @Override
      protected ContainerManagerImpl createContainerManager(Context context,
          ContainerExecutor exec, DeletionService del,
          NodeStatusUpdater nodeStatusUpdater,
          ApplicationACLsManager aclsManager,
          LocalDirsHandlerService dirsHandler) {
        return new ContainerManagerImpl(context, exec, del, nodeStatusUpdater,
            metrics, aclsManager, dirsHandler) {

          @Override
          public void cleanUpApplicationsOnNMShutDown() {
            super.cleanUpApplicationsOnNMShutDown();
            numCleanups.incrementAndGet();
          }
        };
      }
    };

    nm.init(conf);
    nm.start();

    // NM takes a while to reach the STARTED state.
    int waitCount = 0;
    while (nm.getServiceState() != STATE.STARTED && waitCount++ != 20) {
      LOG.info("Waiting for NM to stop..");
      Thread.sleep(1000);
    }
    Assert.assertTrue(nm.getServiceState() == STATE.STARTED);
    nm.stop();
  }

  // Add new containers info into NM context each time node heart beats.
  private class MyNMContext extends NMContext {

    public MyNMContext(
        NMContainerTokenSecretManager containerTokenSecretManager,
        NMTokenSecretManagerInNM nmTokenSecretManager) {
      super(containerTokenSecretManager, nmTokenSecretManager, null, null);
    }

    @Override
    public ConcurrentMap<ContainerId, Container> getContainers() {
      if (heartBeatID == 0) {
        return containers;
      } else if (heartBeatID == 1) {
        ContainerStatus containerStatus2 =
            createContainerStatus(2, ContainerState.RUNNING);
        Container container2 = getMockContainer(containerStatus2);
        containers.put(containerStatus2.getContainerId(), container2);

        ContainerStatus containerStatus3 =
            createContainerStatus(3, ContainerState.COMPLETE);
        Container container3 = getMockContainer(containerStatus3);
        containers.put(containerStatus3.getContainerId(), container3);
        return containers;
      } else if (heartBeatID == 2) {
        ContainerStatus containerStatus4 =
            createContainerStatus(4, ContainerState.RUNNING);
        Container container4 = getMockContainer(containerStatus4);
        containers.put(containerStatus4.getContainerId(), container4);

        ContainerStatus containerStatus5 =
            createContainerStatus(5, ContainerState.COMPLETE);
        Container container5 = getMockContainer(containerStatus5);
        containers.put(containerStatus5.getContainerId(), container5);
        return containers;
      } else {
        containers.clear();
        return containers;
      }
    }
  }

  public static ContainerStatus createContainerStatus(int id,
      ContainerState containerState) {
    ApplicationId applicationId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId applicationAttemptId =
        ApplicationAttemptId.newInstance(applicationId, 1);
    ContainerId contaierId = ContainerId.newInstance(applicationAttemptId, id);
    ContainerStatus containerStatus =
        BuilderUtils.newContainerStatus(contaierId, containerState,
          "test_containerStatus: id=" + id + ", containerState: "
              + containerState, 0);
    return containerStatus;
  }

  public static Container getMockContainer(ContainerStatus containerStatus) {
    ContainerImpl container = mock(ContainerImpl.class);
    when(container.cloneAndGetContainerStatus()).thenReturn(containerStatus);
    when(container.getCurrentState()).thenReturn(containerStatus.getState());
    when(container.getContainerId()).thenReturn(
      containerStatus.getContainerId());
    return container;
  }

  private void verifyNodeStartFailure(String errMessage) throws Exception {
    Assert.assertNotNull("nm is null", nm);
    YarnConfiguration conf = createNMConfig();
    nm.init(conf);
    try {
      nm.start();
      Assert.fail("NM should have failed to start. Didn't get exception!!");
    } catch (Exception e) {
      //the version in trunk looked in the cause for equality
      // and assumed failures were nested.
      //this version assumes that error strings propagate to the base and
      //use a contains() test only. It should be less brittle
      if(!e.getMessage().contains(errMessage)) {
        throw e;
      }
    }
    
    // the service should be stopped
    Assert.assertEquals("NM state is wrong!", STATE.STOPPED, nm
        .getServiceState());

    Assert.assertEquals("Number of registered nodes is wrong!", 0,
        this.registeredNodes.size());
  }

  private YarnConfiguration createNMConfig() {
    YarnConfiguration conf = new YarnConfiguration();
    String localhostAddress = null;
    try {
      localhostAddress = InetAddress.getByName("localhost").getCanonicalHostName();
    } catch (UnknownHostException e) {
      Assert.fail("Unable to get localhost address: " + e.getMessage());
    }
    conf.setInt(YarnConfiguration.NM_PMEM_MB, 5 * 1024); // 5GB
    conf.set(YarnConfiguration.NM_ADDRESS, localhostAddress + ":12345");
    conf.set(YarnConfiguration.NM_LOCALIZER_ADDRESS, localhostAddress + ":12346");  
    conf.set(YarnConfiguration.NM_LOG_DIRS, logsDir.getAbsolutePath());
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
      remoteLogsDir.getAbsolutePath());
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, nmLocalDir.getAbsolutePath());
    conf.setLong(YarnConfiguration.NM_LOG_RETAIN_SECONDS, 1);
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
