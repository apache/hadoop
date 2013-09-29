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
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEvent;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.ApplicationMasterLauncher;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptLaunchFailedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMDelegationTokenSecretManager;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

@SuppressWarnings("unchecked")
public class MockRM extends ResourceManager {

  public MockRM() {
    this(new YarnConfiguration());
  }

  public MockRM(Configuration conf) {
    this(conf, null);    
  }
  
  public MockRM(Configuration conf, RMStateStore store) {
    super();    
    init(conf instanceof YarnConfiguration ? conf : new YarnConfiguration(conf));
    if(store != null) {
      setRMStateStore(store);
    }
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);    
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
    RMApp app = getRMContext().getRMApps().get(attemptId.getApplicationId());
    Assert.assertNotNull("app shouldn't be null", app);
    RMAppAttempt attempt = app.getCurrentAppAttempt();
    int timeoutSecs = 0;
    while (!finalState.equals(attempt.getAppAttemptState()) && timeoutSecs++ < 40) {
      System.out.println("AppAttempt : " + attemptId 
          + " State is : " + attempt.getAppAttemptState()
          + " Waiting for state : " + finalState);
      Thread.sleep(1000);
    }
    System.out.println("Attempt State is : " + attempt.getAppAttemptState());
    Assert.assertEquals("Attempt state is not correct (timedout)", finalState,
        attempt.getAppAttemptState());
  }

  // get new application id
  public GetNewApplicationResponse getNewAppId() throws Exception {
    ApplicationClientProtocol client = getClientRMService();
    return client.getNewApplication(Records
        .newRecord(GetNewApplicationRequest.class));
  }

  public RMApp submitApp(int masterMemory) throws Exception {
    return submitApp(masterMemory, "", UserGroupInformation.getCurrentUser()
      .getShortUserName());
  }

  // client
  public RMApp submitApp(int masterMemory, String name, String user) throws Exception {
    return submitApp(masterMemory, name, user, null, false, null,
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
      Map<ApplicationAccessType, String> acls, boolean unmanaged, String queue,
      int maxAppAttempts, Credentials ts) throws Exception {
    return submitApp(masterMemory, name, user, acls, unmanaged, queue,
      maxAppAttempts, ts, null);
  }

  public RMApp submitApp(int masterMemory, String name, String user,
      Map<ApplicationAccessType, String> acls, boolean unmanaged, String queue,
      int maxAppAttempts, Credentials ts, String appType) throws Exception {
    ApplicationClientProtocol client = getClientRMService();
    GetNewApplicationResponse resp = client.getNewApplication(Records
        .newRecord(GetNewApplicationRequest.class));
    ApplicationId appId = resp.getApplicationId();

    SubmitApplicationRequest req = Records
        .newRecord(SubmitApplicationRequest.class);
    ApplicationSubmissionContext sub = Records
        .newRecord(ApplicationSubmissionContext.class);
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
    waitForState(appId, RMAppState.ACCEPTED);
    return getRMContext().getRMApps().get(appId);
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

  public void sendNodeStarted(MockNM nm) throws Exception {
    RMNodeImpl node = (RMNodeImpl) getRMContext().getRMNodes().get(
        nm.getNodeId());
    node.handle(new RMNodeEvent(nm.getNodeId(), RMNodeEventType.STARTED));
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

  public void killApp(ApplicationId appId) throws Exception {
    ApplicationClientProtocol client = getClientRMService();
    KillApplicationRequest req = Records
        .newRecord(KillApplicationRequest.class);
    req.setApplicationId(appId);
    client.forceKillApplication(req);
  }

  // from AMLauncher
  public MockAM sendAMLaunched(ApplicationAttemptId appAttemptId)
      throws Exception {
    MockAM am = new MockAM(getRMContext(), masterService, appAttemptId);
    am.waitForState(RMAppAttemptState.ALLOCATED);
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
        .handle(new RMAppAttemptLaunchFailedEvent(appAttemptId, "Failed"));
  }

  @Override
  protected ClientRMService createClientRMService() {
    return new ClientRMService(getRMContext(), getResourceScheduler(),
        rmAppManager, applicationACLsManager, queueACLsManager,
        rmDTSecretManager) {
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
    Configuration conf = new Configuration();
    
    containerTokenSecretManager.rollMasterKey();
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
  protected AdminService createAdminService(ClientRMService clientRMService,
      ApplicationMasterService applicationMasterService,
      ResourceTrackerService resourceTrackerService) {
    return new AdminService(getConfig(), scheduler, getRMContext(),
        this.nodesListManager, clientRMService, applicationMasterService,
        resourceTrackerService) {
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

  public NodesListManager getNodesListManager() {
    return this.nodesListManager;
  }

  public RMDelegationTokenSecretManager getRMDTSecretManager() {
    return this.rmDTSecretManager;
  }

  public ClientToAMTokenSecretManagerInRM getClientToAMTokenSecretManager() {
    return this.clientToAMSecretManager;
  }

  @Override
  protected void startWepApp() {
    // override to disable webapp
  }

}
