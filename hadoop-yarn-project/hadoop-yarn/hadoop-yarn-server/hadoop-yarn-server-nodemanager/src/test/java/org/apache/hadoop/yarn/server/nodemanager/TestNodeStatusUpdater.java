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

import static org.apache.hadoop.yarn.server.utils.YarnServerBuilderUtils.newNodeHeartbeatResponse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.ServerSocketUtil;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.SignalContainerCommand;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.client.RMProxy;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.impl.pb.RpcClientFactoryPBImpl;
import org.apache.hadoop.yarn.factories.impl.pb.RpcServerFactoryPBImpl;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeHeartbeatResponseProto;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NodeHeartbeatResponsePBImpl;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.api.records.impl.pb.MasterKeyPBImpl;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager.NMContext;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitor;
import org.apache.hadoop.yarn.server.nodemanager.health.NodeHealthCheckerService;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMNullStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.security.NMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.nodemanager.security.NMTokenSecretManagerInNM;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.utils.YarnServerBuilderUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

@SuppressWarnings("rawtypes")
public class TestNodeStatusUpdater extends NodeManagerTestBase {

  /** Bytes in a GigaByte. */
  private static final long GB = 1024L * 1024L * 1024L;

  private volatile Throwable nmStartError = null;
  private AtomicInteger heartBeatID = new AtomicInteger(0);
  private final List<NodeId> registeredNodes = new ArrayList<NodeId>();
  private boolean triggered = false;
  private NodeManager nm;
  private AtomicBoolean assertionFailedInThread = new AtomicBoolean(false);

  @Before
  public void before() {
    // to avoid threading issues with JUnit 4.13+
    ProtobufRpcEngine2.clearClientCache();
  }

  @After
  public void tearDown() {
    this.registeredNodes.clear();
    heartBeatID.set(0);
    if (nm != null) {
      ServiceOperations.stop(nm);
      nm.waitForServiceToStop(10000);
    }

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
    private boolean signalContainer;

    public MyResourceTracker(Context context, boolean signalContainer) {
      this.context = context;
      this.signalContainer = signalContainer;
    }

    @Override
    public RegisterNodeManagerResponse registerNodeManager(
        RegisterNodeManagerRequest request) throws YarnException,
        IOException {
      NodeId nodeId = request.getNodeId();
      Resource resource = request.getResource();
      LOG.info("Registering {}.", nodeId.toString());
      // NOTE: this really should be checking against the config value
      InetSocketAddress expected = NetUtils.getConnectAddress(
          conf.getSocketAddr(YarnConfiguration.NM_ADDRESS, null, -1));
      Assert.assertEquals(NetUtils.getHostPortString(expected), nodeId.toString());
      Assert.assertEquals(5 * 1024, resource.getMemorySize());
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
      LOG.info("Got heartbeat number {}.", heartBeatID);
      NodeManagerMetrics mockMetrics = mock(NodeManagerMetrics.class);
      Dispatcher mockDispatcher = mock(Dispatcher.class);
      @SuppressWarnings("unchecked")
      EventHandler<Event> mockEventHandler = mock(EventHandler.class);
      when(mockDispatcher.getEventHandler()).thenReturn(mockEventHandler);
      NMStateStoreService stateStore = new NMNullStateStoreService();
      nodeStatus.setResponseId(heartBeatID.getAndIncrement());
      Map<ApplicationId, List<ContainerStatus>> appToContainers =
          getAppToContainerStatusMap(nodeStatus.getContainersStatuses());
      List<SignalContainerRequest> containersToSignal = null;

      ApplicationId appId1 = ApplicationId.newInstance(0, 1);
      ApplicationId appId2 = ApplicationId.newInstance(0, 2);

      ContainerId firstContainerID = null;
      if (heartBeatID.get() == 1) {
        Assert.assertEquals(0, nodeStatus.getContainersStatuses().size());

        // Give a container to the NM.
        ApplicationAttemptId appAttemptID =
            ApplicationAttemptId.newInstance(appId1, 0);
        firstContainerID =
            ContainerId.newContainerId(appAttemptID, heartBeatID.get());
        ContainerLaunchContext launchContext = recordFactory
            .newRecordInstance(ContainerLaunchContext.class);
        Resource resource = Resources.createResource(2, 1);
        long currentTime = System.currentTimeMillis();
        String user = "testUser";
        ContainerTokenIdentifier containerToken = BuilderUtils
            .newContainerTokenIdentifier(BuilderUtils.newContainerToken(
                firstContainerID, 0, InetAddress.getByName("localhost")
                    .getCanonicalHostName(), 1234, user, resource,
                currentTime + 10000, 123, "password".getBytes(), currentTime));
        Context context = mock(Context.class);
        when(context.getNMStateStore()).thenReturn(stateStore);
        Container container = new ContainerImpl(conf, mockDispatcher,
            launchContext, null, mockMetrics, containerToken, context);
        this.context.getContainers().put(firstContainerID, container);
      } else if (heartBeatID.get() == 2) {
        // Checks on the RM end
        Assert.assertEquals("Number of applications should only be one!", 1,
            nodeStatus.getContainersStatuses().size());
        Assert.assertEquals("Number of container for the app should be one!",
            1, appToContainers.get(appId1).size());

        // Checks on the NM end
        ConcurrentMap<ContainerId, Container> activeContainers =
            this.context.getContainers();
        Assert.assertEquals(1, activeContainers.size());

        if (this.signalContainer) {
          containersToSignal = new ArrayList<SignalContainerRequest>();
          SignalContainerRequest signalReq = recordFactory
              .newRecordInstance(SignalContainerRequest.class);
          signalReq.setContainerId(firstContainerID);
          signalReq.setCommand(SignalContainerCommand.OUTPUT_THREAD_DUMP);
          containersToSignal.add(signalReq);
        }

        // Give another container to the NM.
        ApplicationAttemptId appAttemptID =
            ApplicationAttemptId.newInstance(appId2, 0);
        ContainerId secondContainerID =
            ContainerId.newContainerId(appAttemptID, heartBeatID.get());
        ContainerLaunchContext launchContext = recordFactory
            .newRecordInstance(ContainerLaunchContext.class);
        long currentTime = System.currentTimeMillis();
        String user = "testUser";
        Resource resource = Resources.createResource(3, 1);
        ContainerTokenIdentifier containerToken = BuilderUtils
            .newContainerTokenIdentifier(BuilderUtils.newContainerToken(
                secondContainerID, 0, InetAddress.getByName("localhost")
                    .getCanonicalHostName(), 1234, user, resource,
                currentTime + 10000, 123, "password".getBytes(), currentTime));
        Context context = mock(Context.class);
        when(context.getNMStateStore()).thenReturn(stateStore);
        Container container = new ContainerImpl(conf, mockDispatcher,
            launchContext, null, mockMetrics, containerToken, context);
        this.context.getContainers().put(secondContainerID, container);
      } else if (heartBeatID.get() == 3) {
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
          newNodeHeartbeatResponse(heartBeatID.get(), null, null, null, null,
              null, 1000L);
      if (containersToSignal != null) {
        nhResponse.addAllContainersToSignal(containersToSignal);
      }
      return nhResponse;
    }

    @Override
    public UnRegisterNodeManagerResponse unRegisterNodeManager(
        UnRegisterNodeManagerRequest request) throws YarnException, IOException {
      return recordFactory
          .newRecordInstance(UnRegisterNodeManagerResponse.class);
    }
  }

  private class MyNodeStatusUpdater extends BaseNodeStatusUpdaterForTest {
    public MyNodeStatusUpdater(Context context, Dispatcher dispatcher,
        NodeHealthCheckerService healthChecker, NodeManagerMetrics metrics) {
      this(context, dispatcher, healthChecker, metrics, false);
    }

    public MyNodeStatusUpdater(Context context, Dispatcher dispatcher,
        NodeHealthCheckerService healthChecker, NodeManagerMetrics metrics,
        boolean signalContainer) {
      super(context, dispatcher, healthChecker, metrics,
          new MyResourceTracker(context, signalContainer));
    }
  }

  // Test NodeStatusUpdater sends the right container statuses each time it
  // heart beats.
  private class MyNodeStatusUpdater2 extends NodeStatusUpdaterImpl {
    public ResourceTracker resourceTracker;

    public MyNodeStatusUpdater2(Context context, Dispatcher dispatcher,
        NodeHealthCheckerService healthChecker, NodeManagerMetrics metrics) {
      super(context, dispatcher, healthChecker, metrics);
      InetSocketAddress address = new InetSocketAddress(0);
      Configuration configuration = new Configuration();
      Server server = RpcServerFactoryPBImpl.get().getServer(
          ResourceTracker.class, new MyResourceTracker4(context), address,
          configuration, null, 1);
      server.start();
      this.resourceTracker = (ResourceTracker) RpcClientFactoryPBImpl.get()
          .getClient(
          ResourceTracker.class, 1, NetUtils.getConnectAddress(server),
              configuration);
    }

    @Override
    protected ResourceTracker getRMClient() throws IOException {
      return resourceTracker;
    }

    @Override
    protected void stopRMProxy() {
      if (this.resourceTracker != null) {
        RPC.stopProxy(this.resourceTracker);
      }
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
    private final boolean useSocketTimeoutEx;
    public MyNodeStatusUpdater4(Context context, Dispatcher dispatcher,
        NodeHealthCheckerService healthChecker, NodeManagerMetrics metrics,
        long rmStartIntervalMS, boolean rmNeverStart,
        boolean useSocketTimeoutEx) {
      super(context, dispatcher, healthChecker, metrics);
      this.rmStartIntervalMS = rmStartIntervalMS;
      this.rmNeverStart = rmNeverStart;
      this.useSocketTimeoutEx = useSocketTimeoutEx;
    }

    @Override
    protected void serviceStart() throws Exception {
      //record the startup time
      super.serviceStart();
    }

    @Override
    protected ResourceTracker getRMClient() throws IOException {
      RetryPolicy retryPolicy = RMProxy.createRetryPolicy(conf,
          HAUtil.isHAEnabled(conf));
      resourceTracker =
          (ResourceTracker) RetryProxy.create(ResourceTracker.class,
            new MyResourceTracker6(rmStartIntervalMS, rmNeverStart,
                useSocketTimeoutEx),
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
      RetryPolicy retryPolicy = RMProxy.createRetryPolicy(conf,
          HAUtil.isHAEnabled(conf));
      return (ResourceTracker) RetryProxy.create(ResourceTracker.class,
        resourceTracker, retryPolicy);
    }

    @Override
    protected void stopRMProxy() {
      return;
    }
  }

  private class MyNodeStatusUpdater6 extends NodeStatusUpdaterImpl {

    private final long rmStartIntervalMS;
    private final boolean rmNeverStart;
    public ResourceTracker resourceTracker;
    public MyNodeStatusUpdater6(Context context, Dispatcher dispatcher,
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

    private boolean isTriggered() {
      return triggered;
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
      // Make sure that all containers are started before starting shutdown
      syncBarrier.await(10000, TimeUnit.MILLISECONDS);
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
      nodeStatus.setResponseId(heartBeatID.getAndIncrement());

      NodeHeartbeatResponse nhResponse = YarnServerBuilderUtils.
          newNodeHeartbeatResponse(heartBeatID.get(), heartBeatNodeAction, null,
              null, null, null, 1000L);
      nhResponse.setDiagnosticsMessage(shutDownMessage);
      return nhResponse;
    }

    @Override
    public UnRegisterNodeManagerResponse unRegisterNodeManager(
        UnRegisterNodeManagerRequest request) throws YarnException, IOException {
      return recordFactory
          .newRecordInstance(UnRegisterNodeManagerResponse.class);
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
      LOG.info("Got heartBeatId: [{}]", heartBeatID);
      NodeStatus nodeStatus = request.getNodeStatus();
      nodeStatus.setResponseId(heartBeatID.getAndIncrement());
      NodeHeartbeatResponse nhResponse = YarnServerBuilderUtils.
          newNodeHeartbeatResponse(heartBeatID.get(), heartBeatNodeAction, null,
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
      if (heartBeatID.get() == 2) {
        LOG.info("Sending FINISH_APP for application: [{}]", appId);
        this.context.getApplications().put(appId, mock(Application.class));
        nhResponse.addAllApplicationsToCleanup(Collections.singletonList(appId));
      }
      return nhResponse;
    }

    @Override
    public UnRegisterNodeManagerResponse unRegisterNodeManager(
        UnRegisterNodeManagerRequest request) throws YarnException, IOException {
      return recordFactory
          .newRecordInstance(UnRegisterNodeManagerResponse.class);
    }
  }

  // Test NodeStatusUpdater sends the right container statuses each time it
  // heart beats.
  private Credentials expectedCredentials = new Credentials();
  private class MyResourceTracker4 implements ResourceTracker {

    public NodeAction registerNodeAction = NodeAction.NORMAL;
    public NodeAction heartBeatNodeAction = NodeAction.NORMAL;
    private Context context;
    private final ContainerStatus containerStatus2 =
        createContainerStatus(2, ContainerState.RUNNING);
    private final ContainerStatus containerStatus3 =
        createContainerStatus(3, ContainerState.COMPLETE);
    private final ContainerStatus containerStatus4 =
        createContainerStatus(4, ContainerState.RUNNING);
    private final ContainerStatus containerStatus5 =
        createContainerStatus(5, ContainerState.COMPLETE);

    public MyResourceTracker4(Context context) {
      // create app Credentials
      org.apache.hadoop.security.token.Token<DelegationTokenIdentifier> token1 =
          new org.apache.hadoop.security.token.Token<DelegationTokenIdentifier>();
      token1.setKind(new Text("kind1"));
      expectedCredentials.addToken(new Text("token1"), token1);
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
      List<ContainerId> finishedContainersPulledByAM = new ArrayList
          <ContainerId>();
      try {
        if (heartBeatID.get() == 0) {
          Assert.assertEquals(0, request.getNodeStatus().getContainersStatuses()
            .size());
          Assert.assertEquals(0, context.getContainers().size());
        } else if (heartBeatID.get() == 1) {
          List<ContainerStatus> statuses =
              request.getNodeStatus().getContainersStatuses();
          Assert.assertEquals(2, statuses.size());
          Assert.assertEquals(2, context.getContainers().size());

          boolean container2Exist = false, container3Exist = false;
          for (ContainerStatus status : statuses) {
            if (status.getContainerId().equals(
              containerStatus2.getContainerId())) {
              Assert.assertEquals(containerStatus2.getState(),
                  status.getState());
              container2Exist = true;
            }
            if (status.getContainerId().equals(
              containerStatus3.getContainerId())) {
              Assert.assertEquals(containerStatus3.getState(),
                  status.getState());
              container3Exist = true;
            }
          }
          Assert.assertTrue(container2Exist && container3Exist);

          // should throw exception that can be retried by the
          // nodeStatusUpdaterRunnable, otherwise nm just shuts down and the
          // test passes.
          throw new YarnRuntimeException("Lost the heartbeat response");
        } else if (heartBeatID.get() == 2 || heartBeatID.get() == 3) {
          List<ContainerStatus> statuses =
              request.getNodeStatus().getContainersStatuses();
          // NM should send completed containers on heartbeat 2,
          // since heartbeat 1 was lost.  It will send them again on
          // heartbeat 3, because it does not clear them if the previous
          // heartbeat was lost in case the RM treated it as a duplicate.
          Assert.assertEquals(4, statuses.size());
          Assert.assertEquals(4, context.getContainers().size());

          boolean container2Exist = false, container3Exist = false,
              container4Exist = false, container5Exist = false;
          for (ContainerStatus status : statuses) {
            if (status.getContainerId().equals(
              containerStatus2.getContainerId())) {
              Assert.assertEquals(containerStatus2.getState(),
                  status.getState());
              container2Exist = true;
            }
            if (status.getContainerId().equals(
              containerStatus3.getContainerId())) {
              Assert.assertEquals(containerStatus3.getState(),
                  status.getState());
              container3Exist = true;
            }
            if (status.getContainerId().equals(
              containerStatus4.getContainerId())) {
              Assert.assertEquals(containerStatus4.getState(),
                  status.getState());
              container4Exist = true;
            }
            if (status.getContainerId().equals(
              containerStatus5.getContainerId())) {
              Assert.assertEquals(containerStatus5.getState(),
                  status.getState());
              container5Exist = true;
            }
          }
          Assert.assertTrue(container2Exist && container3Exist
              && container4Exist && container5Exist);

          if (heartBeatID.get() == 3) {
            finishedContainersPulledByAM.add(containerStatus3.getContainerId());
          }
        } else if (heartBeatID.get() == 4) {
          List<ContainerStatus> statuses =
              request.getNodeStatus().getContainersStatuses();
          Assert.assertEquals(2, statuses.size());
          // Container 3 is acked by AM, hence removed from context
          Assert.assertEquals(3, context.getContainers().size());

          boolean container3Exist = false;
          for (ContainerStatus status : statuses) {
            if (status.getContainerId().equals(
                containerStatus3.getContainerId())) {
              container3Exist = true;
            }
          }
          Assert.assertFalse(container3Exist);
        }
      } catch (AssertionError error) {
        error.printStackTrace();
        assertionFailedInThread.set(true);
      } finally {
        heartBeatID.incrementAndGet();
      }
      NodeStatus nodeStatus = request.getNodeStatus();
      nodeStatus.setResponseId(heartBeatID.get());
      NodeHeartbeatResponse nhResponse =
          YarnServerBuilderUtils.newNodeHeartbeatResponse(heartBeatID.get(),
            heartBeatNodeAction, null, null, null, null, 1000L);
      nhResponse.addContainersToBeRemovedFromNM(finishedContainersPulledByAM);
      Map<ApplicationId, ByteBuffer> appCredentials =
          new HashMap<ApplicationId, ByteBuffer>();
      DataOutputBuffer dob = new DataOutputBuffer();
      expectedCredentials.writeTokenStorageToStream(dob);
      ByteBuffer byteBuffer1 =
          ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
      appCredentials.put(ApplicationId.newInstance(1234, 1), byteBuffer1);
      nhResponse.setSystemCredentialsForApps(
          YarnServerBuilderUtils.convertToProtoFormat(appCredentials));
      return nhResponse;
    }

    @Override
    public UnRegisterNodeManagerResponse unRegisterNodeManager(
        UnRegisterNodeManagerRequest request) throws YarnException, IOException {
      return recordFactory
          .newRecordInstance(UnRegisterNodeManagerResponse.class);
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
      if (heartBeatID.incrementAndGet() == 1) {
        // EOFException should be retried as well.
        throw new EOFException("NodeHeartbeat exception");
      }
      else {
      throw new java.net.ConnectException(
          "NodeHeartbeat exception");
      }
    }

    @Override
    public UnRegisterNodeManagerResponse unRegisterNodeManager(
        UnRegisterNodeManagerRequest request) throws YarnException, IOException {
      return recordFactory
          .newRecordInstance(UnRegisterNodeManagerResponse.class);
    }
  }

  private class MyResourceTracker6 implements ResourceTracker {

    private long rmStartIntervalMS;
    private boolean rmNeverStart;
    private final long waitStartTime;
    private final boolean useSocketTimeoutEx;

    MyResourceTracker6(long rmStartIntervalMS, boolean rmNeverStart,
                       boolean useSocketTimeoutEx) {
      this.rmStartIntervalMS = rmStartIntervalMS;
      this.rmNeverStart = rmNeverStart;
      this.waitStartTime = System.currentTimeMillis();
      this.useSocketTimeoutEx = useSocketTimeoutEx;
    }

    @Override
    public RegisterNodeManagerResponse registerNodeManager(
        RegisterNodeManagerRequest request) throws YarnException, IOException,
        IOException {
      if (System.currentTimeMillis() - waitStartTime <= rmStartIntervalMS
          || rmNeverStart) {
        if (useSocketTimeoutEx) {
          throw new java.net.SocketTimeoutException(
              "Faking RM start failure as start delay timer has not expired.");
        } else {
          throw new java.net.ConnectException(
              "Faking RM start failure as start delay timer has not expired.");
        }
      } else {
        NodeId nodeId = request.getNodeId();
        Resource resource = request.getResource();
        LOG.info("Registering " + nodeId.toString());
        // NOTE: this really should be checking against the config value
        InetSocketAddress expected = NetUtils.getConnectAddress(
            conf.getSocketAddr(YarnConfiguration.NM_ADDRESS, null, -1));
        Assert.assertEquals(NetUtils.getHostPortString(expected),
            nodeId.toString());
        Assert.assertEquals(5 * 1024, resource.getMemorySize());
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
      nodeStatus.setResponseId(heartBeatID.getAndIncrement());

      NodeHeartbeatResponse nhResponse = YarnServerBuilderUtils.
          newNodeHeartbeatResponse(heartBeatID.get(), NodeAction.NORMAL, null,
              null, null, null, 1000L);
      return nhResponse;
    }

    @Override
    public UnRegisterNodeManagerResponse unRegisterNodeManager(
        UnRegisterNodeManagerRequest request) throws YarnException, IOException {
      return recordFactory
          .newRecordInstance(UnRegisterNodeManagerResponse.class);
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
    conf.setInt(NodeStatusUpdaterImpl.
        YARN_NODEMANAGER_DURATION_TO_TRACK_STOPPED_CONTAINERS, 1);
    nm.init(conf);                                                            
    NodeStatusUpdaterImpl nodeStatusUpdater =                                 
        (NodeStatusUpdaterImpl) nm.getNodeStatusUpdater();                    
    ApplicationId appId = ApplicationId.newInstance(0, 0);                    
    ApplicationAttemptId appAttemptId =                                       
        ApplicationAttemptId.newInstance(appId, 0);                           
    ContainerId cId = ContainerId.newContainerId(appAttemptId, 0);
    nm.getNMContext().getApplications().putIfAbsent(appId,
        mock(Application.class));
    nm.getNMContext().getContainers().putIfAbsent(cId, mock(Container.class));

    nodeStatusUpdater.addCompletedContainer(cId);
    Assert.assertTrue(nodeStatusUpdater.isContainerRecentlyStopped(cId));     
  }

  @Test(timeout = 90000)
  public void testRemovePreviousCompletedContainersFromContext() throws Exception {
    NodeManager nm = new NodeManager();
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(
        NodeStatusUpdaterImpl
            .YARN_NODEMANAGER_DURATION_TO_TRACK_STOPPED_CONTAINERS,
        "10000");
    nm.init(conf);
    NodeStatusUpdaterImpl nodeStatusUpdater =
        (NodeStatusUpdaterImpl) nm.getNodeStatusUpdater();
    ApplicationId appId = ApplicationId.newInstance(0, 0);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 0);
  
    ContainerId cId = ContainerId.newContainerId(appAttemptId, 1);
    Token containerToken =
        BuilderUtils.newContainerToken(cId, 0, "anyHost", 1234, "anyUser",
            Resources.createResource(1024), 0, 123,
            "password".getBytes(), 0);
    Container anyCompletedContainer = new ContainerImpl(conf, null,
        null, null, null,
        BuilderUtils.newContainerTokenIdentifier(containerToken),
        nm.getNMContext()) {

      @Override
      public ContainerState getCurrentState() {
        return ContainerState.COMPLETE;
      }

      @Override
      public org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState getContainerState() {
        return org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState.DONE;
      }
    };

    ContainerId runningContainerId =
        ContainerId.newContainerId(appAttemptId, 3);
    Token runningContainerToken =
        BuilderUtils.newContainerToken(runningContainerId, 0, "anyHost",
          1234, "anyUser", Resources.createResource(1024), 0, 123,
          "password".getBytes(), 0);
    Container runningContainer =
        new ContainerImpl(conf, null, null, null, null,
          BuilderUtils.newContainerTokenIdentifier(runningContainerToken),
          nm.getNMContext()) {
          @Override
          public ContainerState getCurrentState() {
            return ContainerState.RUNNING;
          }

          @Override
          public org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState getContainerState() {
            return org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState.RUNNING;
          }
        };

    nm.getNMContext().getApplications().putIfAbsent(appId,
        mock(Application.class));
    nm.getNMContext().getContainers().put(cId, anyCompletedContainer);
    nm.getNMContext().getContainers()
      .put(runningContainerId, runningContainer);

    Assert.assertEquals(2, nodeStatusUpdater.getContainerStatuses().size());

    List<ContainerId> ackedContainers = new ArrayList<ContainerId>();
    ackedContainers.add(cId);
    ackedContainers.add(runningContainerId);

    nodeStatusUpdater.removeOrTrackCompletedContainersFromContext(ackedContainers);

    Set<ContainerId> containerIdSet = new HashSet<ContainerId>();
    List<ContainerStatus> containerStatuses = nodeStatusUpdater.getContainerStatuses();
    for (ContainerStatus status : containerStatuses) {
      containerIdSet.add(status.getContainerId());
    }

    Assert.assertEquals(1, containerStatuses.size());
    // completed container is removed;
    Assert.assertFalse(containerIdSet.contains(cId));
    // running container is not removed;
    Assert.assertTrue(containerIdSet.contains(runningContainerId));
  }

  @Test(timeout = 10000)
  public void testCompletedContainersIsRecentlyStopped() throws Exception {
    NodeManager nm = new NodeManager();
    nm.init(conf);
    NodeStatusUpdaterImpl nodeStatusUpdater =
        (NodeStatusUpdaterImpl) nm.getNodeStatusUpdater();
    ApplicationId appId = ApplicationId.newInstance(0, 0);
    Application completedApp = mock(Application.class);
    when(completedApp.getApplicationState()).thenReturn(
        ApplicationState.FINISHED);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 0);
    ContainerId containerId = ContainerId.newContainerId(appAttemptId, 1);
    Token containerToken =
        BuilderUtils.newContainerToken(containerId, 0, "host", 1234, "user",
            Resources.createResource(1024), 0, 123,
            "password".getBytes(), 0);

    Container completedContainer = new ContainerImpl(conf, null,
        null, null, null,
        BuilderUtils.newContainerTokenIdentifier(containerToken),
        nm.getNMContext()) {
      @Override
      public ContainerState getCurrentState() {
        return ContainerState.COMPLETE;
      }
    };

    nm.getNMContext().getApplications().putIfAbsent(appId, completedApp);
    nm.getNMContext().getContainers().put(containerId, completedContainer);

    Assert.assertEquals(1, nodeStatusUpdater.getContainerStatuses().size());
    Assert.assertTrue(nodeStatusUpdater.isContainerRecentlyStopped(
        containerId));
  }

  @Test
  public void testCleanedupApplicationContainerCleanup() throws IOException {
    NodeManager nm = new NodeManager();
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(NodeStatusUpdaterImpl
            .YARN_NODEMANAGER_DURATION_TO_TRACK_STOPPED_CONTAINERS,
        "1000000");
    nm.init(conf);

    NodeStatusUpdaterImpl nodeStatusUpdater =
        (NodeStatusUpdaterImpl) nm.getNodeStatusUpdater();
    ApplicationId appId = ApplicationId.newInstance(0, 0);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 0);

    ContainerId cId = ContainerId.newContainerId(appAttemptId, 1);
    Token containerToken =
        BuilderUtils.newContainerToken(cId, 0, "anyHost", 1234, "anyUser",
            Resources.createResource(1024), 0, 123,
            "password".getBytes(), 0);
    Container anyCompletedContainer = new ContainerImpl(conf, null,
        null, null, null,
        BuilderUtils.newContainerTokenIdentifier(containerToken),
        nm.getNMContext()) {

      @Override
      public ContainerState getCurrentState() {
        return ContainerState.COMPLETE;
      }
    };

    Application application = mock(Application.class);
    when(application.getApplicationState()).thenReturn(ApplicationState.RUNNING);
    nm.getNMContext().getApplications().putIfAbsent(appId, application);
    nm.getNMContext().getContainers().put(cId, anyCompletedContainer);

    Assert.assertEquals(1, nodeStatusUpdater.getContainerStatuses().size());

    when(application.getApplicationState()).thenReturn(
        ApplicationState.FINISHING_CONTAINERS_WAIT);
    // The completed container will be saved in case of lost heartbeat.
    Assert.assertEquals(1, nodeStatusUpdater.getContainerStatuses().size());
    Assert.assertEquals(1, nodeStatusUpdater.getContainerStatuses().size());

    nm.getNMContext().getContainers().put(cId, anyCompletedContainer);
    nm.getNMContext().getApplications().remove(appId);
    // The completed container will be saved in case of lost heartbeat.
    Assert.assertEquals(1, nodeStatusUpdater.getContainerStatuses().size());
    Assert.assertEquals(1, nodeStatusUpdater.getContainerStatuses().size());
  }

  @Test
  public void testNMRegistration() throws Exception {
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

    Thread starterThread = new Thread(() -> {
      try {
        nm.start();
      } catch (Throwable e) {
        TestNodeStatusUpdater.this.nmStartError = e;
        throw new YarnRuntimeException(e);
      }
    });
    starterThread.start();

    LOG.info(" ----- thread already started..{}", nm.getServiceState());

    starterThread.join(100000);

    if (nmStartError != null) {
      LOG.error("Error during startup. ", nmStartError);
      Assert.fail(nmStartError.getCause().getMessage());
    }

    GenericTestUtils.waitFor(
        () -> nm.getServiceState() != STATE.STARTED || heartBeatID.get() > 3,
        50, 20000);

    Assert.assertTrue(heartBeatID.get() > 3);
    Assert.assertEquals("Number of registered NMs is wrong!!",
        1, this.registeredNodes.size());
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
            metrics, dirsHandler) {

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
    GenericTestUtils.waitFor(() -> nm.getServiceState() == STATE.STARTED,
        20, 10000);
    GenericTestUtils.waitFor(
        () -> nm.getServiceState() != STATE.STARTED || heartBeatID.get() >= 1,
        50, 20000);
    Assert.assertTrue(heartBeatID.get() >= 1);

    // Meanwhile call stop directly as the shutdown hook would
    nm.stop();

    // NM takes a while to reach the STOPPED state.
    nm.waitForServiceToStop(20000);

    Assert.assertEquals(STATE.STOPPED, nm.getServiceState());

    // It further takes a while after NM reached the STOPPED state.
    GenericTestUtils.waitFor(() -> numCleanups.get() > 0, 20, 20000);
    Assert.assertEquals(1, numCleanups.get());
  }

  @Test
  public void testNodeDecommision() throws Exception {
    nm = getNodeManager(NodeAction.SHUTDOWN);
    YarnConfiguration conf = createNMConfig();
    nm.init(conf);
    Assert.assertEquals(STATE.INITED, nm.getServiceState());
    nm.start();
    GenericTestUtils.waitFor(() -> nm.getServiceState() == STATE.STARTED,
        20, 10000);
    GenericTestUtils.waitFor(
        () -> {
          if (nm.getServiceState() == STATE.STARTED) {
            return (heartBeatID.get() >= 1
                && nm.getNMContext().getDecommissioned());
          }
          return true;
        },
        50, 200000);
    Assert.assertTrue(heartBeatID.get() >= 1);
    Assert.assertTrue(nm.getNMContext().getDecommissioned());

    // NM takes a while to reach the STOPPED state.
    nm.waitForServiceToStop(20000);

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
          "Received SHUTDOWN signal from Resourcemanager, "
        + "Registration of NodeManager failed, "
        + "Message from ResourceManager: RM Shutting Down Node");
  }

  @Test (timeout = 100000)
  public void testNMRMConnectionConf() throws Exception {
    final long delta = 50000;
    final long nmRmConnectionWaitMs = 100;
    final long nmRmRetryInterval = 100;
    final long connectionWaitMs = -1;
    final long connectionRetryIntervalMs = 1000;
    //Waiting for rmStartIntervalMS, RM will be started
    final long rmStartIntervalMS = 2*1000;
    conf.setLong(YarnConfiguration.NM_RESOURCEMANAGER_CONNECT_MAX_WAIT_MS,
        nmRmConnectionWaitMs);
    conf.setLong(
        YarnConfiguration.NM_RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS,
        nmRmRetryInterval);
    conf.setLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS,
        connectionWaitMs);
    conf.setLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS,
        connectionRetryIntervalMs);
    conf.setInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
            1);
    //Test NM try to connect to RM Several times, but finally fail
    NodeManagerWithCustomNodeStatusUpdater nmWithUpdater;
    nm = nmWithUpdater = new NodeManagerWithCustomNodeStatusUpdater() {
      @Override
      protected NodeStatusUpdater createUpdater(Context context,
          Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
        NodeStatusUpdater nodeStatusUpdater = new MyNodeStatusUpdater6(
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
      boolean waitTimeValid = (duration >= nmRmConnectionWaitMs) &&
          (duration < (nmRmConnectionWaitMs + delta));

      if(!waitTimeValid) {
        // throw exception if NM doesn't retry long enough
        throw new Exception("NM should have tried re-connecting to RM during " +
          "period of at least " + nmRmConnectionWaitMs + " ms, but " +
          "stopped retrying within " + (nmRmConnectionWaitMs + delta) +
          " ms: " + e, e);
      }
    }
  }

  private void testNMConnectionToRMInternal(boolean useSocketTimeoutEx)
      throws Exception {
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
            rmStartIntervalMS, true, useSocketTimeoutEx);
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
            false, useSocketTimeoutEx);
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

  @Test (timeout = 150000)
  public void testNMConnectionToRM() throws Exception {
    testNMConnectionToRMInternal(false);
  }

  @Test (timeout = 150000)
  public void testNMConnectionToRMwithSocketTimeout() throws Exception {
    testNMConnectionToRMInternal(true);
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
          metrics, diskhandler) {
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
      GenericTestUtils.waitFor(() -> nm.getServiceState() == STATE.STARTED, 20,
          10000);
      GenericTestUtils.waitFor(
          () -> nm.getServiceState() != STATE.STARTED
              || heartBeatID.get() >= 12,
          100L, 60000000);

      Assert.assertTrue(heartBeatID.get() >= 12);
      MyResourceTracker3 rt =
          (MyResourceTracker3) nm.getNodeStatusUpdater().getRMClient();
      rt.context.getApplications().remove(rt.appId);
      Assert.assertEquals(1, rt.keepAliveRequests.size());
      int numKeepAliveRequests = rt.keepAliveRequests.get(rt.appId).size();
      LOG.info("Number of Keep Alive Requests: [{}]", numKeepAliveRequests);
      Assert.assertTrue(numKeepAliveRequests == 2 || numKeepAliveRequests == 3);
      GenericTestUtils.waitFor(
          () -> nm.getServiceState() != STATE.STARTED
              || heartBeatID.get() >= 20,
          100L, 60000000);
      Assert.assertTrue(heartBeatID.get() >= 20);
      int numKeepAliveRequests2 = rt.keepAliveRequests.get(rt.appId).size();
      Assert.assertEquals(numKeepAliveRequests, numKeepAliveRequests2);
    } finally {
      if (nm != null) {
        nm.stop();
        nm.waitForServiceToStop(10000);
      }
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
          NMTokenSecretManagerInNM nmTokenSecretManager,
          NMStateStoreService store, boolean isDistributedSchedulingEnabled,
          Configuration config) {
        return new MyNMContext(containerTokenSecretManager,
          nmTokenSecretManager, config);
      }
    };

    YarnConfiguration conf = createNMConfig();
    nm.init(conf);
    nm.start();

    GenericTestUtils.waitFor(() -> nm.getServiceState() == STATE.STARTED,
        20, 10000);

    GenericTestUtils.waitFor(
        () -> nm.getServiceState() != STATE.STARTED || heartBeatID.get() > 4,
        50, 20000);
    int hbID = heartBeatID.get();
    Assert.assertFalse("Failed to get all heartbeats in time, "
        + "heartbeatID:" + hbID, hbID <= 4);
    Assert.assertFalse("ContainerStatus Backup failed",
        assertionFailedInThread.get());
    Assert.assertNotNull(nm.getNMContext().getSystemCredentialsForApps()
      .get(ApplicationId.newInstance(1234, 1)).getToken(new Text("token1")));
  }

  @Test(timeout = 200000)
  public void testNodeStatusUpdaterRetryAndNMShutdown()
      throws Exception {
    final long connectionWaitSecs = 1000;
    final long connectionRetryIntervalMs = 1000;
    int port = ServerSocketUtil.getPort(49156, 10);
    YarnConfiguration conf = createNMConfig(port);
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
        new File("start_file.txt"), port);

    try {
      // Wait until we start stopping
      syncBarrier.await(10000, TimeUnit.MILLISECONDS);
      // Wait until we finish stopping
      syncBarrier.await(10000, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
    }
    Assert.assertFalse("Containers not cleaned up when NM stopped",
      assertionFailedInThread.get());
    Assert.assertTrue(((MyNodeManager2) nm).isStopped);
    Assert.assertEquals("calculate heartBeatCount based on" +
        " connectionWaitSecs and RetryIntervalSecs", 2, heartBeatID.get());
  }

  @Test
  public void testRMVersionLessThanMinimum() throws Exception {
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
            metrics, dirsHandler) {

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
    GenericTestUtils.waitFor(() -> nm.getServiceState() == STATE.STARTED,
        20, 200000);
  }


  //Verify that signalContainer request can be dispatched from
  //NodeStatusUpdaterImpl to ContainerManagerImpl.
  @Test
  public void testSignalContainerToContainerManager() throws Exception {
    nm = new NodeManager() {
      @Override
      protected NodeStatusUpdater createNodeStatusUpdater(Context context,
          Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
        return new MyNodeStatusUpdater(
            context, dispatcher, healthChecker, metrics, true);
      }

      @Override
      protected ContainerManagerImpl createContainerManager(Context context,
          ContainerExecutor exec, DeletionService del,
          NodeStatusUpdater nodeStatusUpdater,
          ApplicationACLsManager aclsManager,
          LocalDirsHandlerService diskhandler) {
        return new MyContainerManager(context, exec, del, nodeStatusUpdater,
            metrics, diskhandler);
      }
    };

    YarnConfiguration conf = createNMConfig();
    nm.init(conf);
    nm.start();
    GenericTestUtils.waitFor(() -> nm.getServiceState() == STATE.STARTED,
        20, 20000);

    GenericTestUtils.waitFor(
        () -> nm.getServiceState() != STATE.STARTED
            || heartBeatID.get() > 3,
        50, 20000);
    Assert.assertTrue(heartBeatID.get() > 3);
    Assert.assertEquals("Number of registered NMs is wrong!!", 1,
        this.registeredNodes.size());

    MyContainerManager containerManager =
        (MyContainerManager)nm.getContainerManager();
    Assert.assertTrue(containerManager.signaled);
  }

  @Test
  public void testConcurrentAccessToSystemCredentials(){
    final Map<ApplicationId, ByteBuffer> testCredentials =
        new HashMap<>();
    ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[300]);
    ApplicationId applicationId = ApplicationId.newInstance(123456, 120);
    testCredentials.put(applicationId, byteBuffer);

    final List<Throwable> exceptions = Collections.synchronizedList(new
        ArrayList<Throwable>());

    final int NUM_THREADS = 10;
    final CountDownLatch allDone = new CountDownLatch(NUM_THREADS);
    final ExecutorService threadPool = HadoopExecutors.newFixedThreadPool(
        NUM_THREADS);

    final AtomicBoolean stop = new AtomicBoolean(false);

    try {
      for (int i = 0; i < NUM_THREADS; i++) {
        threadPool.submit(new Runnable() {
          @Override
          public void run() {
            try {
              for (int i = 0; i < 100 && !stop.get(); i++) {
                NodeHeartbeatResponse nodeHeartBeatResponse =
                    newNodeHeartbeatResponse(0, NodeAction.NORMAL,
                        null, null, null, null, 0);
                nodeHeartBeatResponse
                    .setSystemCredentialsForApps(YarnServerBuilderUtils
                        .convertToProtoFormat(testCredentials));
                NodeHeartbeatResponseProto proto =
                    ((NodeHeartbeatResponsePBImpl)nodeHeartBeatResponse)
                        .getProto();
                Assert.assertNotNull(proto);
              }
            } catch (Throwable t) {
              exceptions.add(t);
              stop.set(true);
            } finally {
              allDone.countDown();
            }
          }
        });
      }

      int testTimeout = 2;
      Assert.assertTrue("Timeout waiting for more than " + testTimeout + " " +
              "seconds",
          allDone.await(testTimeout, TimeUnit.SECONDS));
    } catch (InterruptedException ie) {
      exceptions.add(ie);
    } finally {
      threadPool.shutdownNow();
    }
    Assert.assertTrue("Test failed with exception(s)" + exceptions,
        exceptions.isEmpty());
  }

  /**
   * Test if the {@link NodeManager} updates the resources in the
   * {@link ContainersMonitor} when the {@link ResourceManager} triggers the
   * change.
   * @throws Exception If the test cannot run.
   */
  @Test
  public void testUpdateNMResources() throws Exception {

    // The resource set for the Node Manager from the Resource Tracker
    final Resource resource = Resource.newInstance(8 * 1024, 1);

    LOG.info("Start the Resource Tracker to mock heartbeats");
    Server resourceTracker = getMockResourceTracker(resource);
    resourceTracker.start();

    LOG.info("Start the Node Manager");
    NodeManager nodeManager = new NodeManager();
    YarnConfiguration nmConf = new YarnConfiguration();
    try {
      nmConf.setSocketAddr(YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS,
          resourceTracker.getListenerAddress());
      nmConf.set(YarnConfiguration.NM_LOCALIZER_ADDRESS, "0.0.0.0:0");
      nodeManager.init(nmConf);
      nodeManager.start();

      LOG.info("Initially the Node Manager should have the default resources");
      ContainerManager containerManager = nodeManager.getContainerManager();
      ContainersMonitor containerMonitor =
          containerManager.getContainersMonitor();
      Assert.assertEquals(8,
          containerMonitor.getVCoresAllocatedForContainers());
      Assert.assertEquals(8 * GB,
          containerMonitor.getPmemAllocatedForContainers());

      LOG.info("The first heartbeat should trigger a resource change to {}",
          resource);
      GenericTestUtils.waitFor(
          () -> containerMonitor.getVCoresAllocatedForContainers() == 1,
          100, 2 * 1000);
      Assert.assertEquals(8 * GB,
          containerMonitor.getPmemAllocatedForContainers());

      resource.setVirtualCores(5);
      resource.setMemorySize(4 * 1024);
      LOG.info("Change the resources to {}", resource);
      GenericTestUtils.waitFor(
          () -> containerMonitor.getVCoresAllocatedForContainers() == 5,
          100, 2 * 1000);
      Assert.assertEquals(4 * GB,
          containerMonitor.getPmemAllocatedForContainers());
    } finally {
      LOG.info("Cleanup");
      nodeManager.stop();
      try {
        nodeManager.close();
      } catch (IOException ex) {
        LOG.error("Could not close the node manager", ex);
      }
      resourceTracker.stop();
    }
  }

  /**
   * Create a mock Resource Tracker server that returns the resources we want
   * in the heartbeat.
   * @param resource Resource to reply in the heartbeat.
   * @return RPC server for the Resource Tracker.
   * @throws Exception If it cannot create the Resource Tracker.
   */
  private static Server getMockResourceTracker(final Resource resource)
      throws Exception {

    // Setup the mock Resource Tracker
    final ResourceTracker rt = mock(ResourceTracker.class);
    when(rt.registerNodeManager(any())).thenAnswer(invocation -> {
      RegisterNodeManagerResponse response = recordFactory.newRecordInstance(
          RegisterNodeManagerResponse.class);
      response.setContainerTokenMasterKey(createMasterKey());
      response.setNMTokenMasterKey(createMasterKey());
      return response;
    });
    when(rt.nodeHeartbeat(any())).thenAnswer(invocation -> {
      NodeHeartbeatResponse response = recordFactory.newRecordInstance(
          NodeHeartbeatResponse.class);
      response.setResource(resource);
      return response;
    });
    when(rt.unRegisterNodeManager(any())).thenAnswer(invocaiton -> {
      UnRegisterNodeManagerResponse response = recordFactory.newRecordInstance(
          UnRegisterNodeManagerResponse.class);
      return response;
    });

    // Get the RPC server
    YarnConfiguration conf = new YarnConfiguration();
    YarnRPC rpc = YarnRPC.create(conf);
    Server server = rpc.getServer(ResourceTracker.class, rt,
        new InetSocketAddress("0.0.0.0", 0), conf, null, 1);
    return server;
  }

  // Add new containers info into NM context each time node heart beats.
  private class MyNMContext extends NMContext {

    public MyNMContext(
        NMContainerTokenSecretManager containerTokenSecretManager,
        NMTokenSecretManagerInNM nmTokenSecretManager, Configuration conf) {
      super(containerTokenSecretManager, nmTokenSecretManager, null, null,
          new NMNullStateStoreService(), false, conf);
    }

    @Override
    public ConcurrentMap<ContainerId, Container> getContainers() {
      if (heartBeatID.get() == 0) {
        return containers;
      } else if (heartBeatID.get() == 1) {
        ContainerStatus containerStatus2 =
            createContainerStatus(2, ContainerState.RUNNING);
        putMockContainer(containerStatus2);

        ContainerStatus containerStatus3 =
            createContainerStatus(3, ContainerState.COMPLETE);
        putMockContainer(containerStatus3);
        return containers;
      } else if (heartBeatID.get() == 2) {
        ContainerStatus containerStatus4 =
            createContainerStatus(4, ContainerState.RUNNING);
        putMockContainer(containerStatus4);

        ContainerStatus containerStatus5 =
            createContainerStatus(5, ContainerState.COMPLETE);
        putMockContainer(containerStatus5);
        return containers;
      } else if (heartBeatID.get() == 3 || heartBeatID.get() == 4) {
        return containers;
      } else {
        containers.clear();
        return containers;
      }
    }

    private void putMockContainer(ContainerStatus containerStatus) {
      Container container = getMockContainer(containerStatus);
      containers.put(containerStatus.getContainerId(), container);
      applications.putIfAbsent(containerStatus.getContainerId()
          .getApplicationAttemptId().getApplicationId(),
          mock(Application.class));
    }
  }

  public static ContainerStatus createContainerStatus(int id,
      ContainerState containerState) {
    ApplicationId applicationId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId applicationAttemptId =
        ApplicationAttemptId.newInstance(applicationId, 1);
    ContainerId contaierId = ContainerId.newContainerId(applicationAttemptId, id);
    ContainerStatus containerStatus =
        BuilderUtils.newContainerStatus(contaierId, containerState,
          "test_containerStatus: id=" + id + ", containerState: "
              + containerState, 0, Resource.newInstance(1024, 1));
    return containerStatus;
  }

  public static Container getMockContainer(ContainerStatus containerStatus) {
    ContainerImpl container = mock(ContainerImpl.class);
    when(container.cloneAndGetContainerStatus()).thenReturn(containerStatus);
    when(container.getCurrentState()).thenReturn(containerStatus.getState());
    when(container.getContainerId()).thenReturn(
      containerStatus.getContainerId());
    if (containerStatus.getState().equals(ContainerState.COMPLETE)) {
      when(container.getContainerState())
        .thenReturn(org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState.DONE);
    } else if (containerStatus.getState().equals(ContainerState.RUNNING)) {
      when(container.getContainerState())
      .thenReturn(org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState.RUNNING);
    }
    return container;
  }

  private void verifyNodeStartFailure(String errMessage) throws Exception {
    Assert.assertNotNull("nm is null", nm);
    YarnConfiguration conf = createNMConfig();
    nm.init(conf);

    //the version in trunk looked in the cause for equality
    // and assumed failures were nested.
    //this version assumes that error strings propagate to the base and
    //use a contains() test only. It should be less brittle
    LambdaTestUtils.intercept(Exception.class, errMessage, () -> nm.start());

    // the service should be stopped
    Assert.assertEquals("NM state is wrong!", STATE.STOPPED,
        nm.getServiceState());

    Assert.assertEquals("Number of registered nodes is wrong!", 0,
        this.registeredNodes.size());
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

  @Test
  public void testExceptionReported() {
    nm = new NodeManager();
    YarnConfiguration conf = new YarnConfiguration();
    nm.init(conf);
    NodeStatusUpdater nodeStatusUpdater = nm.getNodeStatusUpdater();
    NodeHealthCheckerService nodeHealthChecker = nm.getNodeHealthChecker();

    assertThat(nodeHealthChecker.isHealthy()).isTrue();

    String message = "exception message";
    Exception e = new Exception(message);
    nodeStatusUpdater.reportException(e);
    assertThat(nodeHealthChecker.isHealthy()).isFalse();
    assertThat(nodeHealthChecker.getHealthReport()).isEqualTo(message);
  }
}
