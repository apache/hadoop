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

package org.apache.hadoop.yarn.server.resourcemanager;

import static org.apache.hadoop.yarn.server.resourcemanager.MockNM.createMockNodeStatus;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.FailApplicationAttemptRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FailApplicationAttemptResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.SignalContainerCommand;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsResponse;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEvent;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.ApplicationMasterLauncher;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.NullRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystem;
import org.apache.hadoop.yarn.server.resourcemanager.resource.TestResourceProfiles;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeDecommissioningEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeStartedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.YarnVersionInfo;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


@SuppressWarnings("unchecked")
public class MockRM extends ResourceManager {

  static final Logger LOG = LoggerFactory.getLogger(MockRM.class);
  static final String ENABLE_WEBAPP = "mockrm.webapp.enabled";
  private static final int SECOND = 1000;
  private static final int TIMEOUT_MS_FOR_ATTEMPT = 40 * SECOND;
  private static final int TIMEOUT_MS_FOR_APP_REMOVED = 40 * SECOND;
  private static final int TIMEOUT_MS_FOR_CONTAINER_AND_NODE = 20 * SECOND;
  private static final int WAIT_MS_PER_LOOP = 10;

  private final boolean useNullRMNodeLabelsManager;
  private boolean disableDrainEventsImplicitly;

  private boolean useRealElector = false;

  public MockRM() {
    this(new YarnConfiguration());
  }

  public MockRM(Configuration conf) {
    this(conf, null);    
  }
  
  public MockRM(Configuration conf, RMStateStore store) {
    this(conf, store, true, false);
  }

  public MockRM(Configuration conf, boolean useRealElector) {
    this(conf, null, true, useRealElector);
  }

  public MockRM(Configuration conf, RMStateStore store,
      boolean useRealElector) {
    this(conf, store, true, useRealElector);
  }

  public MockRM(Configuration conf, RMStateStore store,
      boolean useNullRMNodeLabelsManager, boolean useRealElector) {
    super();
    // Clear metrics to avoid possible interference between tests
    DefaultMetricsSystem.shutdown();
    QueueMetrics.clearQueueMetrics();
    if (conf.getBoolean(TestResourceProfiles.TEST_CONF_RESET_RESOURCE_TYPES,
        true)) {
      ResourceUtils.resetResourceTypes(conf);
    }
    this.useNullRMNodeLabelsManager = useNullRMNodeLabelsManager;
    this.useRealElector = useRealElector;
    init(conf instanceof YarnConfiguration ? conf : new YarnConfiguration(conf));
    if (store != null) {
      setRMStateStore(store);
    } else {
      Class storeClass = getRMContext().getStateStore().getClass();
      if (storeClass.equals(MemoryRMStateStore.class)) {
        MockMemoryRMStateStore mockStateStore = new MockMemoryRMStateStore();
        mockStateStore.init(conf);
        setRMStateStore(mockStateStore);
      } else if (storeClass.equals(NullRMStateStore.class)) {
        MockRMNullStateStore mockStateStore = new MockRMNullStateStore();
        mockStateStore.init(conf);
        setRMStateStore(mockStateStore);
      }
    }
    GenericTestUtils.setRootLogLevel(Level.DEBUG);
    disableDrainEventsImplicitly = false;
  }

  public class MockRMNullStateStore extends NullRMStateStore {
    @SuppressWarnings("rawtypes")
    @Override
    protected EventHandler getRMStateStoreEventHandler() {
      return rmStateStoreEventHandler;
    }
  }

  @Override
  protected RMNodeLabelsManager createNodeLabelManager()
      throws InstantiationException, IllegalAccessException {
    if (useNullRMNodeLabelsManager) {
      RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
      mgr.init(getConfig());
      return mgr;
    } else {
      return super.createNodeLabelManager();
    }
  }

  @Override
  protected Dispatcher createDispatcher() {
    return new DrainDispatcher();
  }

  @Override
  protected EmbeddedElector createEmbeddedElector() throws IOException {
    if (useRealElector) {
      return super.createEmbeddedElector();
    } else {
      return null;
    }
  }

  @Override
  protected EventHandler<SchedulerEvent> createSchedulerEventDispatcher() {
    return new EventHandler<SchedulerEvent>() {
      @Override
      public void handle(SchedulerEvent event) {
        scheduler.handle(event);
      }
    };
  }

  public void drainEvents() {
    Dispatcher rmDispatcher = getRmDispatcher();
    if (rmDispatcher instanceof DrainDispatcher) {
      ((DrainDispatcher) rmDispatcher).await();
    } else {
      throw new UnsupportedOperationException("Not a Drain Dispatcher!");
    }
  }

  private void waitForState(ApplicationId appId, EnumSet<RMAppState> finalStates)
      throws InterruptedException {
    drainEventsImplicitly();
    RMApp app = getRMContext().getRMApps().get(appId);
    Assert.assertNotNull("app shouldn't be null", app);
    final int timeoutMsecs = 80 * SECOND;
    int timeWaiting = 0;
    while (!finalStates.contains(app.getState())) {
      if (timeWaiting >= timeoutMsecs) {
        break;
      }

      LOG.info("App : " + appId + " State is : " + app.getState() +
          " Waiting for state : " + finalStates);
      Thread.sleep(WAIT_MS_PER_LOOP);
      timeWaiting += WAIT_MS_PER_LOOP;
    }

    LOG.info("App State is : " + app.getState());
    Assert.assertTrue("App State is not correct (timeout).",
        finalStates.contains(app.getState()));
  }

  /**
   * Wait until an application has reached a specified state.
   * The timeout is 80 seconds.
   * @param appId the id of an application
   * @param finalState the application state waited
   * @throws InterruptedException
   *         if interrupted while waiting for the state transition
   */
  public void waitForState(ApplicationId appId, RMAppState finalState)
      throws InterruptedException {
    drainEventsImplicitly();
    RMApp app = getRMContext().getRMApps().get(appId);
    Assert.assertNotNull("app shouldn't be null", app);
    final int timeoutMsecs = 80 * SECOND;
    int timeWaiting = 0;
    while (!finalState.equals(app.getState())) {
      if (timeWaiting >= timeoutMsecs) {
        break;
      }

      LOG.info("App : " + appId + " State is : " + app.getState() +
              " Waiting for state : " + finalState);
      Thread.sleep(WAIT_MS_PER_LOOP);
      timeWaiting += WAIT_MS_PER_LOOP;
    }

    LOG.info("App State is : " + app.getState());
    Assert.assertEquals("App State is not correct (timeout).", finalState,
      app.getState());
  }

  /**
   * Wait until an attempt has reached a specified state.
   * The timeout is 40 seconds.
   * @param attemptId the id of an attempt
   * @param finalState the attempt state waited
   * @throws InterruptedException
   *         if interrupted while waiting for the state transition
   */
  public void waitForState(ApplicationAttemptId attemptId,
      RMAppAttemptState finalState) throws InterruptedException {
    waitForState(attemptId, finalState, TIMEOUT_MS_FOR_ATTEMPT);
  }

  /**
   * Wait until an attempt has reached a specified state.
   * The timeout can be specified by the parameter.
   * @param attemptId the id of an attempt
   * @param finalState the attempt state waited
   * @param timeoutMsecs the length of timeout in milliseconds
   * @throws InterruptedException
   *         if interrupted while waiting for the state transition
   */
  public void waitForState(ApplicationAttemptId attemptId,
      RMAppAttemptState finalState, int timeoutMsecs)
      throws InterruptedException {
    drainEventsImplicitly();
    RMApp app = getRMContext().getRMApps().get(attemptId.getApplicationId());
    Assert.assertNotNull("app shouldn't be null", app);
    RMAppAttempt attempt = app.getRMAppAttempt(attemptId);
    MockRM.waitForState(attempt, finalState, timeoutMsecs);
  }

  /**
   * Wait until an attempt has reached a specified state.
   * The timeout is 40 seconds.
   * @param attempt an attempt
   * @param finalState the attempt state waited
   * @throws InterruptedException
   *         if interrupted while waiting for the state transition
   */
  public static void waitForState(RMAppAttempt attempt,
      RMAppAttemptState finalState) throws InterruptedException {
    waitForState(attempt, finalState, TIMEOUT_MS_FOR_ATTEMPT);
  }

  /**
   * Wait until an attempt has reached a specified state.
   * The timeout can be specified by the parameter.
   * @param attempt an attempt
   * @param finalState the attempt state waited
   * @param timeoutMsecs the length of timeout in milliseconds
   * @throws InterruptedException
   *         if interrupted while waiting for the state transition
   */
  public static void waitForState(RMAppAttempt attempt,
      RMAppAttemptState finalState, int timeoutMsecs)
      throws InterruptedException {
    int timeWaiting = 0;
    while (finalState != attempt.getAppAttemptState()) {
      if (timeWaiting >= timeoutMsecs) {
        break;
      }

      LOG.info("AppAttempt : " + attempt.getAppAttemptId() + " State is : " +
          attempt.getAppAttemptState() + " Waiting for state : " + finalState);
      Thread.sleep(WAIT_MS_PER_LOOP);
      timeWaiting += WAIT_MS_PER_LOOP;
    }

    LOG.info("Attempt State is : " + attempt.getAppAttemptState());
    Assert.assertEquals("Attempt state is not correct (timeout).", finalState,
        attempt.getState());
  }

  public void waitForContainerToComplete(RMAppAttempt attempt,
      NMContainerStatus completedContainer) throws InterruptedException {
    drainEventsImplicitly();
    int timeWaiting = 0;
    while (timeWaiting < TIMEOUT_MS_FOR_CONTAINER_AND_NODE) {
      List<ContainerStatus> containers = attempt.getJustFinishedContainers();
      LOG.info("Received completed containers " + containers);
      for (ContainerStatus container : containers) {
        if (container.getContainerId().equals(
          completedContainer.getContainerId())) {
          return;
        }
      }
      Thread.sleep(WAIT_MS_PER_LOOP);
      timeWaiting += WAIT_MS_PER_LOOP;
    }
  }

  public MockAM waitForNewAMToLaunchAndRegister(ApplicationId appId, int attemptSize,
      MockNM nm) throws Exception {
    RMApp app = getRMContext().getRMApps().get(appId);
    Assert.assertNotNull(app);
    int timeWaiting = 0;
    while (app.getAppAttempts().size() != attemptSize) {
      if (timeWaiting >= TIMEOUT_MS_FOR_ATTEMPT) {
        break;
      }
      LOG.info("Application " + appId
          + " is waiting for AM to restart. Current has "
          + app.getAppAttempts().size() + " attempts.");
      Thread.sleep(WAIT_MS_PER_LOOP);
      timeWaiting += WAIT_MS_PER_LOOP;
    }
    return launchAndRegisterAM(app, this, nm);
  }

  /**
   * Wait until a container has reached a specified state.
   * The timeout is 10 seconds.
   * @param nm A mock nodemanager
   * @param containerId the id of a container
   * @param containerState the container state waited
   * @return if reach the state before timeout; false otherwise.
   * @throws Exception
   *         if interrupted while waiting for the state transition
   *         or an unexpected error while MockNM is hearbeating.
   */
  public boolean waitForState(MockNM nm, ContainerId containerId,
      RMContainerState containerState) throws Exception {
    return waitForState(nm, containerId, containerState,
      TIMEOUT_MS_FOR_CONTAINER_AND_NODE);
  }

  /**
   * Wait until a container has reached a specified state.
   * The timeout is specified by the parameter.
   * @param nm A mock nodemanager
   * @param containerId the id of a container
   * @param containerState the container state waited
   * @param timeoutMsecs the length of timeout in milliseconds
   * @return if reach the state before timeout; false otherwise.
   * @throws Exception
   *         if interrupted while waiting for the state transition
   *         or an unexpected error while MockNM is hearbeating.
   */
  public boolean waitForState(MockNM nm, ContainerId containerId,
      RMContainerState containerState, int timeoutMsecs) throws Exception {
    return waitForState(Arrays.asList(nm), containerId, containerState,
      timeoutMsecs);
  }

  /**
   * Wait until a container has reached a specified state.
   * The timeout is 10 seconds.
   * @param nms array of mock nodemanagers
   * @param containerId the id of a container
   * @param containerState the container state waited
   * @return if reach the state before timeout; false otherwise.
   * @throws Exception
   *         if interrupted while waiting for the state transition
   *         or an unexpected error while MockNM is hearbeating.
   */
  public boolean waitForState(Collection<MockNM> nms, ContainerId containerId,
      RMContainerState containerState) throws Exception {
    return waitForState(nms, containerId, containerState,
      TIMEOUT_MS_FOR_CONTAINER_AND_NODE);
  }

  /**
   * Wait until a container has reached a specified state.
   * The timeout is specified by the parameter.
   * @param nms array of mock nodemanagers
   * @param containerId the id of a container
   * @param containerState the container state waited
   * @param timeoutMsecs the length of timeout in milliseconds
   * @return if reach the state before timeout; false otherwise.
   * @throws Exception
   *         if interrupted while waiting for the state transition
   *         or an unexpected error while MockNM is hearbeating.
   */
  public boolean waitForState(Collection<MockNM> nms, ContainerId containerId,
      RMContainerState containerState, int timeoutMsecs) throws Exception {
    drainEventsImplicitly();
    RMContainer container = getResourceScheduler().getRMContainer(containerId);
    int timeWaiting = 0;
    while (container == null) {
      if (timeWaiting >= timeoutMsecs) {
        return false;
      }

      for (MockNM nm : nms) {
        nm.nodeHeartbeat(true);
      }
      drainEventsImplicitly();
      container = getResourceScheduler().getRMContainer(containerId);
      LOG.info("Waiting for container " + containerId + " to be "
          + containerState + ", container is null right now.");
      Thread.sleep(WAIT_MS_PER_LOOP);
      timeWaiting += WAIT_MS_PER_LOOP;
    }

    while (!containerState.equals(container.getState())) {
      if (timeWaiting >= timeoutMsecs) {
        return false;
      }

      LOG.info("Container : " + containerId + " State is : "
          + container.getState() + " Waiting for state : " + containerState);
      for (MockNM nm : nms) {
        nm.nodeHeartbeat(true);
      }
      drainEventsImplicitly();
      Thread.sleep(WAIT_MS_PER_LOOP);
      timeWaiting += WAIT_MS_PER_LOOP;
    }

    LOG.info("Container State is : " + container.getState());
    return true;
  }

  // get new application id
  public GetNewApplicationResponse getNewAppId() throws Exception {
    ApplicationClientProtocol client = getClientRMService();
    return client.getNewApplication(Records
        .newRecord(GetNewApplicationRequest.class));
  }

  public MockNM unRegisterNode(MockNM nm) throws Exception {
    nm.unRegisterNode();
    drainEventsImplicitly();
    return nm;
  }

  public MockNM registerNode(String nodeIdStr, int memory) throws Exception {
    MockNM nm = new MockNM(nodeIdStr, memory, getResourceTrackerService());
    nm.registerNode();
    drainEventsImplicitly();
    return nm;
  }

  public MockNM registerNode(String nodeIdStr, int memory, int vCores)
      throws Exception {
    MockNM nm =
        new MockNM(nodeIdStr, memory, vCores, getResourceTrackerService());
    nm.registerNode();
    drainEventsImplicitly();
    return nm;
  }
  
  public MockNM registerNode(String nodeIdStr, int memory, int vCores,
      List<ApplicationId> runningApplications) throws Exception {
    MockNM nm =
        new MockNM(nodeIdStr, memory, vCores, getResourceTrackerService(),
            YarnVersionInfo.getVersion());
    nm.registerNode(runningApplications);
    drainEventsImplicitly();
    return nm;
  }

  public MockNM registerNode(String nodeIdStr, int memory, int vCores,
      List<ApplicationId> runningApplications,
      List<NMContainerStatus> containerStatuses) throws Exception {
    MockNM nm =
        new MockNM(nodeIdStr, memory, vCores, getResourceTrackerService(),
            YarnVersionInfo.getVersion());
    nm.registerNode(containerStatuses, runningApplications);
    drainEventsImplicitly();
    return nm;
  }

  public MockNM registerNode(String nodeIdStr, Resource nodeCapability)
      throws Exception {
    MockNM nm = new MockNM(nodeIdStr, nodeCapability,
        getResourceTrackerService());
    nm.registerNode();
    drainEventsImplicitly();
    return nm;
  }

  public void sendNodeStarted(MockNM nm) throws Exception {
    RMNodeImpl node = (RMNodeImpl) getRMContext().getRMNodes().get(
        nm.getNodeId());
    NodeStatus mockNodeStatus = createMockNodeStatus();
    node.handle(new RMNodeStartedEvent(nm.getNodeId(), null, null,
        mockNodeStatus));
    drainEventsImplicitly();
  }
  
  public void sendNodeLost(MockNM nm) throws Exception {
    RMNodeImpl node = (RMNodeImpl) getRMContext().getRMNodes().get(
        nm.getNodeId());
    node.handle(new RMNodeEvent(nm.getNodeId(), RMNodeEventType.EXPIRE));
    drainEventsImplicitly();
  }

  private RMNode getRMNode(NodeId nodeId) {
    RMNode node = getRMContext().getRMNodes().get(nodeId);
    if (node == null) {
      node = getRMContext().getInactiveRMNodes().get(nodeId);
    }
    return node;
  }

  /**
   * Wait until a node has reached a specified state.
   * The timeout is 20 seconds.
   * @param nodeId the id of a node
   * @param finalState the node state waited
   * @throws InterruptedException
   *         if interrupted while waiting for the state transition
   */
  public void waitForState(NodeId nodeId, NodeState finalState)
      throws InterruptedException {
    drainEventsImplicitly();
    int timeWaiting = 0;
    RMNode node = getRMNode(nodeId);
    while (node == null) {
      if (timeWaiting >= TIMEOUT_MS_FOR_CONTAINER_AND_NODE) {
        break;
      }
      node = getRMNode(nodeId);
      Thread.sleep(WAIT_MS_PER_LOOP);
      timeWaiting += WAIT_MS_PER_LOOP;
    }
    Assert.assertNotNull("node shouldn't be null (timedout)", node);
    while (!finalState.equals(node.getState())) {
      if (timeWaiting >= TIMEOUT_MS_FOR_CONTAINER_AND_NODE) {
        break;
      }

      LOG.info("Node State is : " + node.getState()
          + " Waiting for state : " + finalState);
      Thread.sleep(WAIT_MS_PER_LOOP);
      timeWaiting += WAIT_MS_PER_LOOP;
    }

    LOG.info("Node " + nodeId + " State is : " + node.getState());
    Assert.assertEquals("Node state is not correct (timedout)", finalState,
        node.getState());
  }

  public void sendNodeGracefulDecommission(
      MockNM nm, int timeout) throws Exception {
    RMNodeImpl node = (RMNodeImpl)
        getRMContext().getRMNodes().get(nm.getNodeId());
    Assert.assertNotNull("node shouldn't be null", node);
    node.handle(new RMNodeDecommissioningEvent(nm.getNodeId(), timeout));
  }

  public void sendNodeEvent(MockNM nm, RMNodeEventType event) throws Exception {
    RMNodeImpl node = (RMNodeImpl)
        getRMContext().getRMNodes().get(nm.getNodeId());
    Assert.assertNotNull("node shouldn't be null", node);
    node.handle(new RMNodeEvent(nm.getNodeId(), event));
  }

  public Integer getDecommissioningTimeout(NodeId nodeid) {
    return this.getRMContext().getRMNodes()
        .get(nodeid).getDecommissioningTimeout();
  }

  public KillApplicationResponse killApp(ApplicationId appId) throws Exception {
    ApplicationClientProtocol client = getClientRMService();
    KillApplicationRequest req = KillApplicationRequest.newInstance(appId);
    KillApplicationResponse response = client.forceKillApplication(req);
    drainEventsImplicitly();
    return response;
  }

  public FailApplicationAttemptResponse failApplicationAttempt(
      ApplicationAttemptId attemptId) throws Exception {
    ApplicationClientProtocol client = getClientRMService();
    FailApplicationAttemptRequest req =
        FailApplicationAttemptRequest.newInstance(attemptId);
    FailApplicationAttemptResponse response =
        client.failApplicationAttempt(req);
    drainEventsImplicitly();
    return response;
  }

  /**
   * recommend to use launchAM, or use sendAMLaunched like:
   * 1, wait RMAppAttempt scheduled
   * 2, send node heartbeat
   * 3, sendAMLaunched
   */
  public MockAM sendAMLaunched(ApplicationAttemptId appAttemptId)
      throws Exception {
    MockAM am = new MockAM(getRMContext(), masterService, appAttemptId);
    ((AbstractYarnScheduler)scheduler).update();
    waitForState(appAttemptId, RMAppAttemptState.ALLOCATED);
    //create and set AMRMToken
    Token<AMRMTokenIdentifier> amrmToken =
        this.rmContext.getAMRMTokenSecretManager().createAndGetAMRMToken(
          appAttemptId);
    ((RMAppAttemptImpl) this.rmContext.getRMApps()
      .get(appAttemptId.getApplicationId()).getRMAppAttempt(appAttemptId))
      .setAMRMToken(amrmToken);
    getRMContext()
        .getDispatcher()
        .getEventHandler()
        .handle(
            new RMAppAttemptEvent(appAttemptId, RMAppAttemptEventType.LAUNCHED));
    drainEventsImplicitly();
    return am;
  }

  public void sendAMLaunchFailed(ApplicationAttemptId appAttemptId)
      throws Exception {
    MockAM am = new MockAM(getRMContext(), masterService, appAttemptId);
    waitForState(am.getApplicationAttemptId(), RMAppAttemptState.ALLOCATED);
    getRMContext().getDispatcher().getEventHandler()
        .handle(new RMAppAttemptEvent(appAttemptId,
            RMAppAttemptEventType.LAUNCH_FAILED, "Failed"));
    drainEventsImplicitly();
  }

  @Override
  protected ClientRMService createClientRMService() {
    return new ClientRMService(getRMContext(), getResourceScheduler(),
        rmAppManager, applicationACLsManager, queueACLsManager,
        getRMContext().getRMDelegationTokenSecretManager()) {
      @Override
      protected void serviceStart() {
        // override to not start rpc handler
      }

      @Override
      protected void serviceStop() {
        // don't do anything
      }
    };
  }

  @Override
  protected ResourceTrackerService createResourceTrackerService() {

    RMContainerTokenSecretManager containerTokenSecretManager =
        getRMContext().getContainerTokenSecretManager();
    containerTokenSecretManager.rollMasterKey();
    NMTokenSecretManagerInRM nmTokenSecretManager =
        getRMContext().getNMTokenSecretManager();
    nmTokenSecretManager.rollMasterKey();
    return new ResourceTrackerService(getRMContext(), nodesListManager,
        this.nmLivelinessMonitor, containerTokenSecretManager,
        nmTokenSecretManager) {

      @Override
      protected void serviceStart() {
        // override to not start rpc handler
      }

      @Override
      protected void serviceStop() {
        // don't do anything
      }
    };
  }

  @Override
  protected ApplicationMasterService createApplicationMasterService() {
    if (this.rmContext.getYarnConfiguration().getBoolean(
        YarnConfiguration.OPPORTUNISTIC_CONTAINER_ALLOCATION_ENABLED,
        YarnConfiguration.DEFAULT_OPPORTUNISTIC_CONTAINER_ALLOCATION_ENABLED)) {
      return new OpportunisticContainerAllocatorAMService(getRMContext(),
          scheduler) {
        @Override
        protected void serviceStart() {
          // override to not start rpc handler
        }

        @Override
        protected void serviceStop() {
          // don't do anything
        }
      };
    }
    return new ApplicationMasterService(getRMContext(), scheduler) {
      @Override
      protected void serviceStart() {
        // override to not start rpc handler
      }

      @Override
      protected void serviceStop() {
        // don't do anything
      }
    };
  }

  @Override
  protected ApplicationMasterLauncher createAMLauncher() {
    return new ApplicationMasterLauncher(getRMContext()) {
      @Override
      protected void serviceStart() {
        // override to not start rpc handler
      }

      @Override
      public void handle(AMLauncherEvent appEvent) {
        // don't do anything
      }

      @Override
      protected void serviceStop() {
        // don't do anything
      }
    };
  }

  @Override
  protected AdminService createAdminService() {
    RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
    return new AdminService(this) {
      @Override
      protected void startServer() {
        // override to not start rpc handler
      }

      @Override
      protected void stopServer() {
        // don't do anything
      }

      @Override
      public RefreshServiceAclsResponse refreshServiceAcls(RefreshServiceAclsRequest request)
          throws YarnException, IOException {
        Configuration config = this.getConfig();
        boolean authorization =
            config.getBoolean(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, false);
        if (!authorization) {
          throw RPCUtil.getRemoteException(new IOException("Service Authorization (" +
              CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION + ") not enabled."));
        }
        return recordFactory.newRecordInstance(RefreshServiceAclsResponse.class);
      }

      public String[] getGroupsForUser(String user) throws IOException {
        if ("admin".equals(user)) {
          return new String[]{"admin"};
        }
        return new String[]{};
      }
    };
  }

  public NodesListManager getNodesListManager() {
    return this.nodesListManager;
  }

  public ClientToAMTokenSecretManagerInRM getClientToAMTokenSecretManager() {
    return this.getRMContext().getClientToAMTokenSecretManager();
  }

  public RMAppManager getRMAppManager() {
    return this.rmAppManager;
  }
  
  public AdminService getAdminService() {
    return this.adminService;
  }

  @Override
  protected void startWepApp() {
    if (getConfig().getBoolean(ENABLE_WEBAPP, false)) {
      super.startWepApp();
      return;
    }

    // Disable webapp
  }

  public static void finishAMAndVerifyAppState(RMApp rmApp, MockRM rm, MockNM nm,
      MockAM am) throws Exception {
    FinishApplicationMasterRequest req =
        FinishApplicationMasterRequest.newInstance(
          FinalApplicationStatus.SUCCEEDED, "", "");
    am.unregisterAppAttempt(req,true);
    rm.waitForState(am.getApplicationAttemptId(), RMAppAttemptState.FINISHING);
    nm.nodeHeartbeat(am.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
    rm.drainEventsImplicitly();
    rm.waitForState(am.getApplicationAttemptId(), RMAppAttemptState.FINISHED);
    rm.waitForState(rmApp.getApplicationId(), RMAppState.FINISHED);
  }

  @SuppressWarnings("rawtypes")
  private static void waitForSchedulerAppAttemptAdded(
      ApplicationAttemptId attemptId, MockRM rm) throws InterruptedException {
    int tick = 0;
    rm.drainEventsImplicitly();
    // Wait for at most 5 sec
    while (null == ((AbstractYarnScheduler) rm.getResourceScheduler())
        .getApplicationAttempt(attemptId) && tick < 50) {
      Thread.sleep(100);
      if (tick % 10 == 0) {
        LOG.info("waiting for SchedulerApplicationAttempt="
            + attemptId + " added.");
      }
      tick++;
    }
    Assert.assertNotNull("Timed out waiting for SchedulerApplicationAttempt=" +
      attemptId + " to be added.", ((AbstractYarnScheduler)
        rm.getResourceScheduler()).getApplicationAttempt(attemptId));
  }

  public static MockAM launchAMWhenAsyncSchedulingEnabled(RMApp app, MockRM rm)
      throws Exception {
    int i = 0;
    while (app.getCurrentAppAttempt() == null) {
      if (i < 100) {
        i++;
      }
      Thread.sleep(50);
    }

    RMAppAttempt attempt = app.getCurrentAppAttempt();

    rm.waitForState(attempt.getAppAttemptId(),
        RMAppAttemptState.ALLOCATED);
    MockAM am = rm.sendAMLaunched(attempt.getAppAttemptId());
    rm.waitForState(attempt.getAppAttemptId(), RMAppAttemptState.LAUNCHED);

    return am;
  }

  /**
   * NOTE: nm.nodeHeartbeat is explicitly invoked,
   * don't invoke it before calling launchAM
   */
  public static MockAM launchAM(RMApp app, MockRM rm, MockNM nm)
      throws Exception {
    rm.drainEventsImplicitly();
    RMAppAttempt attempt = waitForAttemptScheduled(app, rm);
    LOG.info("Launch AM " + attempt.getAppAttemptId());
    nm.nodeHeartbeat(true);
    ((AbstractYarnScheduler)rm.getResourceScheduler()).update();
    rm.drainEventsImplicitly();
    nm.nodeHeartbeat(true);
    MockAM am = rm.sendAMLaunched(attempt.getAppAttemptId());
    rm.waitForState(attempt.getAppAttemptId(), RMAppAttemptState.LAUNCHED);
    return am;
  }

  public static MockAM launchUAM(RMApp app, MockRM rm, MockNM nm)
      throws Exception {
    rm.drainEventsImplicitly();
    // UAMs go directly to LAUNCHED state
    rm.waitForState(app.getApplicationId(), RMAppState.ACCEPTED);
    RMAppAttempt attempt = app.getCurrentAppAttempt();
    waitForSchedulerAppAttemptAdded(attempt.getAppAttemptId(), rm);
    LOG.info("Launch AM " + attempt.getAppAttemptId());
    nm.nodeHeartbeat(true);
    ((AbstractYarnScheduler)rm.getResourceScheduler()).update();
    rm.drainEventsImplicitly();
    nm.nodeHeartbeat(true);
    MockAM am = new MockAM(rm.getRMContext(), rm.masterService,
        attempt.getAppAttemptId());
    rm.waitForState(attempt.getAppAttemptId(), RMAppAttemptState.LAUNCHED);
    return am;
  }

  public static RMAppAttempt waitForAttemptScheduled(RMApp app, MockRM rm)
      throws Exception {
    rm.waitForState(app.getApplicationId(), RMAppState.ACCEPTED);
    RMAppAttempt attempt = app.getCurrentAppAttempt();
    waitForSchedulerAppAttemptAdded(attempt.getAppAttemptId(), rm);
    rm.waitForState(attempt.getAppAttemptId(), RMAppAttemptState.SCHEDULED);
    return attempt;
  }

  public static MockAM launchAndRegisterAM(RMApp app, MockRM rm, MockNM nm)
      throws Exception {
    MockAM am = launchAM(app, rm, nm);
    am.registerAppAttempt();
    rm.waitForState(app.getApplicationId(), RMAppState.RUNNING);
    return am;
  }

  public static MockAM launchAndRegisterAM(RMApp app, MockRM rm, MockNM nm,
      Map<Set<String>, PlacementConstraint> constraints) throws Exception {
    MockAM am = launchAM(app, rm, nm);
    for (Map.Entry<Set<String>, PlacementConstraint> e :
        constraints.entrySet()) {
      am.addPlacementConstraint(e.getKey(), e.getValue());
    }
    am.registerAppAttempt();
    rm.waitForState(app.getApplicationId(), RMAppState.RUNNING);
    return am;
  }

  public ApplicationReport getApplicationReport(ApplicationId appId)
      throws YarnException, IOException {
    ApplicationClientProtocol client = getClientRMService();
    GetApplicationReportResponse response =
        client.getApplicationReport(GetApplicationReportRequest
            .newInstance(appId));
    return response.getApplicationReport();
  }

  public void updateReservationState(ReservationUpdateRequest request)
      throws IOException, YarnException {
    ApplicationClientProtocol client = getClientRMService();
    client.updateReservation(request);
    drainEventsImplicitly();
  }

  // Explicitly reset queue metrics for testing.
  @SuppressWarnings("static-access")
  public void clearQueueMetrics(RMApp app) {
    ((AbstractYarnScheduler<SchedulerApplicationAttempt, SchedulerNode>) getResourceScheduler())
      .getSchedulerApplications().get(app.getApplicationId()).getQueue()
      .getMetrics().clearQueueMetrics();
  }
  
  public RMActiveServices getRMActiveService() {
    return activeServices;
  }

  public void signalToContainer(ContainerId containerId,
      SignalContainerCommand command) throws Exception {
    ApplicationClientProtocol client = getClientRMService();
    SignalContainerRequest req =
        SignalContainerRequest.newInstance(containerId, command);
    client.signalToContainer(req);
    drainEventsImplicitly();
  }

  /**
   * Wait until an app removed from scheduler.
   * The timeout is 40 seconds.
   * @param appId the id of an app
   * @throws InterruptedException
   *         if interrupted while waiting for app removed
   */
  public void waitForAppRemovedFromScheduler(ApplicationId appId)
      throws InterruptedException {
    int timeWaiting = 0;
    drainEventsImplicitly();

    Map<ApplicationId, SchedulerApplication> apps  =
        ((AbstractYarnScheduler) getResourceScheduler())
            .getSchedulerApplications();
    while (apps.containsKey(appId)) {
      if (timeWaiting >= TIMEOUT_MS_FOR_APP_REMOVED) {
        break;
      }
      LOG.info("wait for app removed, " + appId);
      Thread.sleep(WAIT_MS_PER_LOOP);
      timeWaiting += WAIT_MS_PER_LOOP;
    }
    Assert.assertTrue("app is not removed from scheduler (timeout).",
        !apps.containsKey(appId));
    LOG.info("app is removed from scheduler, " + appId);
  }

  /**
   * Wait until a container has reached a completion state.
   * The timeout is 20 seconds.
   * @param nm A mock nodemanager
   * @param rm A mock resourcemanager
   * @param amContainerId The id of an am container
   * @param container A container
   * @throws Exception
   *         if interrupted while waiting for the completion transition
   *         or an unexpected error while MockNM is hearbeating.
   */
  public static void waitForContainerCompletion(MockRM rm, MockNM nm,
    ContainerId amContainerId, RMContainer container) throws Exception {
    ContainerId containerId = container.getContainerId();
    if (null != rm.scheduler.getRMContainer(containerId)) {
      if (containerId.equals(amContainerId)) {
        rm.waitForState(nm, containerId, RMContainerState.COMPLETED);
      } else {
        rm.waitForState(nm, containerId, RMContainerState.KILLED);
      }
    } else {
      rm.drainEvents();
    }
  }

  private void drainEventsImplicitly() {
    if (!disableDrainEventsImplicitly) {
      drainEvents();
    }
  }

  public void disableDrainEventsImplicitly() {
    disableDrainEventsImplicitly = true;
  }

  public void enableDrainEventsImplicityly() {
    disableDrainEventsImplicitly = false;
  }


  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    if (getRmDispatcher() instanceof AsyncDispatcher) {
      ((AsyncDispatcher) getRmDispatcher()).disableExitOnDispatchException();
    }
  }

  public RMStateStore getRMStateStore() {
    return getRMContext().getStateStore();
  }

  @VisibleForTesting
  public ReservationSystem getReservationSystem(){
    return this.reservationSystem;
  }
}
