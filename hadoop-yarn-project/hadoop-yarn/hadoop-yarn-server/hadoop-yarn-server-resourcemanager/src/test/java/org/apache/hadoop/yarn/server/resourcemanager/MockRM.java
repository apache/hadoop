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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEvent;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.ApplicationMasterLauncher;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
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
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeStartedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.YarnVersionInfo;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;

@SuppressWarnings("unchecked")
public class MockRM extends ResourceManager {

  static final String ENABLE_WEBAPP = "mockrm.webapp.enabled";
  
  final private boolean useNullRMNodeLabelsManager;

  public MockRM() {
    this(new YarnConfiguration());
  }

  public MockRM(Configuration conf) {
    this(conf, null);    
  }
  
  public MockRM(Configuration conf, RMStateStore store) {
    this(conf, store, true);
  }
  
  public MockRM(Configuration conf, RMStateStore store,
      boolean useNullRMNodeLabelsManager) {
    super();
    this.useNullRMNodeLabelsManager = useNullRMNodeLabelsManager;
    init(conf instanceof YarnConfiguration ? conf : new YarnConfiguration(conf));
    if(store != null) {
      setRMStateStore(store);
    }
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
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

  public void waitForState(ApplicationId appId, RMAppState finalState)
      throws Exception {
    RMApp app = getRMContext().getRMApps().get(appId);
    Assert.assertNotNull("app shouldn't be null", app);
    int timeoutSecs = 0;
    while (!finalState.equals(app.getState()) && timeoutSecs++ < 40) {
      System.out.println("App : " + appId + " State is : " + app.getState()
          + " Waiting for state : " + finalState);
      Thread.sleep(2000);
    }
    System.out.println("App State is : " + app.getState());
    Assert.assertEquals("App state is not correct (timedout)", finalState,
        app.getState());
  }

  public void waitForState(ApplicationAttemptId attemptId,
      RMAppAttemptState finalState)
      throws Exception {
    waitForState(attemptId, finalState, 40000);
  }

  public void waitForState(ApplicationAttemptId attemptId,
      RMAppAttemptState finalState, int timeoutMsecs) throws Exception {
    RMApp app = getRMContext().getRMApps().get(attemptId.getApplicationId());
    Assert.assertNotNull("app shouldn't be null", app);
    RMAppAttempt attempt = app.getRMAppAttempt(attemptId);
    int timeoutSecs = 0;
    while (!finalState.equals(attempt.getAppAttemptState()) && timeoutSecs++ < timeoutMsecs) {
      System.out.println("AppAttempt : " + attemptId 
          + " State is : " + attempt.getAppAttemptState()
          + " Waiting for state : " + finalState);
      Thread.sleep(1000);
    }
    System.out.println("Attempt State is : " + attempt.getAppAttemptState());
    Assert.assertEquals("Attempt state is not correct (timedout)", finalState,
        attempt.getAppAttemptState());
  }

  public void waitForContainerAllocated(MockNM nm, ContainerId containerId)
      throws Exception {
    int timeoutSecs = 0;
    while (getResourceScheduler().getRMContainer(containerId) == null
        && timeoutSecs++ < 40) {
      System.out.println("Waiting for" + containerId + " to be allocated.");
      nm.nodeHeartbeat(true);
      Thread.sleep(200);
    }
  }

  public void waitForContainerToComplete(RMAppAttempt attempt,
      NMContainerStatus completedContainer) throws InterruptedException {
    while (true) {
      List<ContainerStatus> containers = attempt.getJustFinishedContainers();
      System.out.println("Received completed containers " + containers);
      for (ContainerStatus container : containers) {
        if (container.getContainerId().equals(
          completedContainer.getContainerId())) {
          return;
        }
      }
      Thread.sleep(200);
    }
  }

  public MockAM waitForNewAMToLaunchAndRegister(ApplicationId appId, int attemptSize,
      MockNM nm) throws Exception {
    RMApp app = getRMContext().getRMApps().get(appId);
    Assert.assertNotNull(app);
    while (app.getAppAttempts().size() != attemptSize) {
      System.out.println("Application " + appId
          + " is waiting for AM to restart. Current has "
          + app.getAppAttempts().size() + " attempts.");
      Thread.sleep(200);
    }
    return launchAndRegisterAM(app, this, nm);
  }

  public boolean waitForState(MockNM nm, ContainerId containerId,
      RMContainerState containerState) throws Exception {
    // default is wait for 30,000 ms
    return waitForState(nm, containerId, containerState, 30 * 1000);
  }
  
  public boolean waitForState(MockNM nm, ContainerId containerId,
      RMContainerState containerState, int timeoutMillisecs) throws Exception {
    RMContainer container = getResourceScheduler().getRMContainer(containerId);
    int timeoutSecs = 0;
    while(container == null && timeoutSecs++ < timeoutMillisecs / 100) {
      nm.nodeHeartbeat(true);
      container = getResourceScheduler().getRMContainer(containerId);
      System.out.println("Waiting for container " + containerId + " to be allocated.");
      Thread.sleep(100);
      
      if (timeoutMillisecs <= timeoutSecs * 100) {
        return false;
      }
    }
    Assert.assertNotNull("Container shouldn't be null", container);
    while (!containerState.equals(container.getState())
        && timeoutSecs++ < timeoutMillisecs / 100) {
      System.out.println("Container : " + containerId + " State is : "
          + container.getState() + " Waiting for state : " + containerState);
      nm.nodeHeartbeat(true);
      Thread.sleep(100);
      
      if (timeoutMillisecs <= timeoutSecs * 100) {
        return false;
      }
    }
    
    System.out.println("Container State is : " + container.getState());
    Assert.assertEquals("Container state is not correct (timedout)",
      containerState, container.getState());
    return true;
  }

  // get new application id
  public GetNewApplicationResponse getNewAppId() throws Exception {
    ApplicationClientProtocol client = getClientRMService();
    return client.getNewApplication(Records
        .newRecord(GetNewApplicationRequest.class));
  }

  public RMApp submitApp(int masterMemory) throws Exception {
    return submitApp(masterMemory, false);
  }

  public RMApp submitApp(int masterMemory, boolean unmanaged)
      throws Exception {
    return submitApp(masterMemory, "", UserGroupInformation.getCurrentUser()
        .getShortUserName(), unmanaged);
  }

  // client
  public RMApp submitApp(int masterMemory, String name, String user) throws Exception {
    return submitApp(masterMemory, name, user, false);
  }

  public RMApp submitApp(int masterMemory, String name, String user,
      boolean unmanaged)
      throws Exception {
    return submitApp(masterMemory, name, user, null, unmanaged, null,
        super.getConfig().getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
            YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS), null);
  }

  public RMApp submitApp(int masterMemory, String name, String user,
      Map<ApplicationAccessType, String> acls) throws Exception {
    return submitApp(masterMemory, name, user, acls, false, null,
      super.getConfig().getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS), null);
  }  

  public RMApp submitApp(int masterMemory, String name, String user,
      Map<ApplicationAccessType, String> acls, String queue) throws Exception {
    return submitApp(masterMemory, name, user, acls, false, queue,
      super.getConfig().getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS), null);
  }
  
  public RMApp submitApp(int masterMemory, String name, String user,
      Map<ApplicationAccessType, String> acls, String queue, 
      boolean waitForAccepted) throws Exception {
    return submitApp(masterMemory, name, user, acls, false, queue,
      super.getConfig().getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS), null, null,
        waitForAccepted);
  }

  public RMApp submitApp(int masterMemory, String name, String user,
      Map<ApplicationAccessType, String> acls, boolean unmanaged, String queue,
      int maxAppAttempts, Credentials ts) throws Exception {
    return submitApp(masterMemory, name, user, acls, unmanaged, queue,
      maxAppAttempts, ts, null);
  }

  public RMApp submitApp(int masterMemory, String name, String user,
      Map<ApplicationAccessType, String> acls, boolean unmanaged, String queue,
      int maxAppAttempts, Credentials ts, String appType) throws Exception {
    return submitApp(masterMemory, name, user, acls, unmanaged, queue,
      maxAppAttempts, ts, appType, true);
  }

  public RMApp submitApp(int masterMemory, String name, String user,
      Map<ApplicationAccessType, String> acls, boolean unmanaged, String queue,
      int maxAppAttempts, Credentials ts, String appType,
      boolean waitForAccepted)
      throws Exception {
    return submitApp(masterMemory, name, user, acls, unmanaged, queue,
      maxAppAttempts, ts, appType, waitForAccepted, false);
  }

  public RMApp submitApp(int masterMemory, String name, String user,
      Map<ApplicationAccessType, String> acls, boolean unmanaged, String queue,
      int maxAppAttempts, Credentials ts, String appType,
      boolean waitForAccepted, boolean keepContainers) throws Exception {
    return submitApp(masterMemory, name, user, acls, unmanaged, queue,
        maxAppAttempts, ts, appType, waitForAccepted, keepContainers,
        false, null, 0, null, true);
  }

  public RMApp submitApp(int masterMemory, long attemptFailuresValidityInterval)
      throws Exception {
    return submitApp(masterMemory, "", UserGroupInformation.getCurrentUser()
      .getShortUserName(), null, false, null,
      super.getConfig().getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
      YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS), null, null, true, false,
      false, null, attemptFailuresValidityInterval, null, true);
  }

  public RMApp submitApp(int masterMemory, String name, String user,
      Map<ApplicationAccessType, String> acls, boolean unmanaged, String queue,
      int maxAppAttempts, Credentials ts, String appType,
      boolean waitForAccepted, boolean keepContainers, boolean isAppIdProvided,
      ApplicationId applicationId) throws Exception {
    return submitApp(masterMemory, name, user, acls, unmanaged, queue,
      maxAppAttempts, ts, appType, waitForAccepted, keepContainers,
      isAppIdProvided, applicationId, 0, null, true);
  }

  public RMApp submitApp(int masterMemory,
      LogAggregationContext logAggregationContext) throws Exception {
    return submitApp(masterMemory, "", UserGroupInformation.getCurrentUser()
      .getShortUserName(), null, false, null,
      super.getConfig().getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
      YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS), null, null, true, false,
      false, null, 0, logAggregationContext, true);
   }

  public RMApp submitApp(int masterMemory, String name, String user,
      Map<ApplicationAccessType, String> acls, boolean unmanaged, String queue,
      int maxAppAttempts, Credentials ts, String appType,
      boolean waitForAccepted, boolean keepContainers, boolean isAppIdProvided,
      ApplicationId applicationId, long attemptFailuresValidityInterval,
      LogAggregationContext logAggregationContext, boolean cancelTokensWhenComplete)
      throws Exception {
    ApplicationId appId = isAppIdProvided ? applicationId : null;
    ApplicationClientProtocol client = getClientRMService();
    if (! isAppIdProvided) {
      GetNewApplicationResponse resp = client.getNewApplication(Records
          .newRecord(GetNewApplicationRequest.class));
      appId = resp.getApplicationId();
    }
    SubmitApplicationRequest req = Records
        .newRecord(SubmitApplicationRequest.class);
    ApplicationSubmissionContext sub = Records
        .newRecord(ApplicationSubmissionContext.class);
    sub.setKeepContainersAcrossApplicationAttempts(keepContainers);
    sub.setApplicationId(appId);
    sub.setApplicationName(name);
    sub.setMaxAppAttempts(maxAppAttempts);
    if(unmanaged) {
      sub.setUnmanagedAM(true);
    }
    if (queue != null) {
      sub.setQueue(queue);
    }
    sub.setApplicationType(appType);
    ContainerLaunchContext clc = Records
        .newRecord(ContainerLaunchContext.class);
    final Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(masterMemory);
    sub.setResource(capability);
    clc.setApplicationACLs(acls);
    if (ts != null && UserGroupInformation.isSecurityEnabled()) {
      DataOutputBuffer dob = new DataOutputBuffer();
      ts.writeTokenStorageToStream(dob);
      ByteBuffer securityTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
      clc.setTokens(securityTokens);
    }
    sub.setAMContainerSpec(clc);
    sub.setAttemptFailuresValidityInterval(attemptFailuresValidityInterval);
    if (logAggregationContext != null) {
      sub.setLogAggregationContext(logAggregationContext);
    }
    sub.setCancelTokensWhenComplete(cancelTokensWhenComplete);
    req.setApplicationSubmissionContext(sub);
    UserGroupInformation fakeUser =
      UserGroupInformation.createUserForTesting(user, new String[] {"someGroup"});
    PrivilegedAction<SubmitApplicationResponse> action =
      new PrivilegedAction<SubmitApplicationResponse>() {
      ApplicationClientProtocol client;
      SubmitApplicationRequest req;
      @Override
      public SubmitApplicationResponse run() {
        try {
          return client.submitApplication(req);
        } catch (YarnException e) {
          e.printStackTrace();
        } catch (IOException e) {
          e.printStackTrace();
        }
        return null;
      }
      PrivilegedAction<SubmitApplicationResponse> setClientReq(
        ApplicationClientProtocol client, SubmitApplicationRequest req) {
        this.client = client;
        this.req = req;
        return this;
      }
    }.setClientReq(client, req);
    fakeUser.doAs(action);
    // make sure app is immediately available after submit
    if (waitForAccepted) {
      waitForState(appId, RMAppState.ACCEPTED);
    }
    RMApp rmApp = getRMContext().getRMApps().get(appId);

    // unmanaged AM won't go to RMAppAttemptState.SCHEDULED.
    if (waitForAccepted && !unmanaged) {
      waitForState(rmApp.getCurrentAppAttempt().getAppAttemptId(),
          RMAppAttemptState.SCHEDULED);
    }

    return rmApp;
  }

  public MockNM registerNode(String nodeIdStr, int memory) throws Exception {
    MockNM nm = new MockNM(nodeIdStr, memory, getResourceTrackerService());
    nm.registerNode();
    return nm;
  }

  public MockNM registerNode(String nodeIdStr, int memory, int vCores)
      throws Exception {
    MockNM nm =
        new MockNM(nodeIdStr, memory, vCores, getResourceTrackerService());
    nm.registerNode();
    return nm;
  }
  
  public MockNM registerNode(String nodeIdStr, int memory, int vCores,
      List<ApplicationId> runningApplications) throws Exception {
    MockNM nm =
        new MockNM(nodeIdStr, memory, vCores, getResourceTrackerService(),
            YarnVersionInfo.getVersion());
    nm.registerNode(runningApplications);
    return nm;
  }

  public void sendNodeStarted(MockNM nm) throws Exception {
    RMNodeImpl node = (RMNodeImpl) getRMContext().getRMNodes().get(
        nm.getNodeId());
    node.handle(new RMNodeStartedEvent(nm.getNodeId(), null, null));
  }
  
  public void sendNodeLost(MockNM nm) throws Exception {
    RMNodeImpl node = (RMNodeImpl) getRMContext().getRMNodes().get(
        nm.getNodeId());
    node.handle(new RMNodeEvent(nm.getNodeId(), RMNodeEventType.EXPIRE));
  }

  public void NMwaitForState(NodeId nodeid, NodeState finalState)
      throws Exception {
    RMNode node = getRMContext().getRMNodes().get(nodeid);
    Assert.assertNotNull("node shouldn't be null", node);
    int timeoutSecs = 0;
    while (!finalState.equals(node.getState()) && timeoutSecs++ < 20) {
      System.out.println("Node State is : " + node.getState()
          + " Waiting for state : " + finalState);
      Thread.sleep(500);
    }
    System.out.println("Node State is : " + node.getState());
    Assert.assertEquals("Node state is not correct (timedout)", finalState,
        node.getState());
  }

  public KillApplicationResponse killApp(ApplicationId appId) throws Exception {
    ApplicationClientProtocol client = getClientRMService();
    KillApplicationRequest req = KillApplicationRequest.newInstance(appId);
    return client.forceKillApplication(req);
  }

  // from AMLauncher
  public MockAM sendAMLaunched(ApplicationAttemptId appAttemptId)
      throws Exception {
    MockAM am = new MockAM(getRMContext(), masterService, appAttemptId);
    am.waitForState(RMAppAttemptState.ALLOCATED);
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
    return am;
  }

  public void sendAMLaunchFailed(ApplicationAttemptId appAttemptId)
      throws Exception {
    MockAM am = new MockAM(getRMContext(), masterService, appAttemptId);
    am.waitForState(RMAppAttemptState.ALLOCATED);
    getRMContext().getDispatcher().getEventHandler()
        .handle(new RMAppAttemptEvent(appAttemptId,
            RMAppAttemptEventType.LAUNCH_FAILED, "Failed"));
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
    return new AdminService(this, getRMContext()) {
      @Override
      protected void startServer() {
        // override to not start rpc handler
      }

      @Override
      protected void stopServer() {
        // don't do anything
      }

      @Override
      protected EmbeddedElectorService createEmbeddedElectorService() {
        return null;
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
    am.waitForState(RMAppAttemptState.FINISHING);
    nm.nodeHeartbeat(am.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
    am.waitForState(RMAppAttemptState.FINISHED);
    rm.waitForState(rmApp.getApplicationId(), RMAppState.FINISHED);
  }

  public static MockAM launchAM(RMApp app, MockRM rm, MockNM nm)
      throws Exception {
    rm.waitForState(app.getApplicationId(), RMAppState.ACCEPTED);
    RMAppAttempt attempt = app.getCurrentAppAttempt();
    System.out.println("Launch AM " + attempt.getAppAttemptId());
    nm.nodeHeartbeat(true);
    MockAM am = rm.sendAMLaunched(attempt.getAppAttemptId());
    rm.waitForState(attempt.getAppAttemptId(), RMAppAttemptState.LAUNCHED);
    return am;
  }

  public static MockAM launchAndRegisterAM(RMApp app, MockRM rm, MockNM nm)
      throws Exception {
    MockAM am = launchAM(app, rm, nm);
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
}
