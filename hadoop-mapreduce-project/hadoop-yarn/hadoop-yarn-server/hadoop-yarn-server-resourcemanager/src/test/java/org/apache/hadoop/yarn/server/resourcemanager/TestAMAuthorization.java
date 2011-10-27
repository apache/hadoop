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
import java.security.PrivilegedAction;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.resourcemanager.TestApplicationMasterLauncher.MockRMWithCustomAMLauncher;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Assert;
import org.junit.Test;

public class TestAMAuthorization {

  private static final Log LOG = LogFactory.getLog(TestAMAuthorization.class);

  private static final class MyContainerManager implements ContainerManager {

    Map<String, String> containerEnv;

    public MyContainerManager() {
    }

    @Override
    public StartContainerResponse
        startContainer(StartContainerRequest request)
            throws YarnRemoteException {
      containerEnv = request.getContainerLaunchContext().getEnvironment();
      return null;
    }

    @Override
    public StopContainerResponse stopContainer(StopContainerRequest request)
        throws YarnRemoteException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public GetContainerStatusResponse getContainerStatus(
        GetContainerStatusRequest request) throws YarnRemoteException {
      // TODO Auto-generated method stub
      return null;
    }
  }

  private static class MockRMWithAMS extends MockRMWithCustomAMLauncher {

    private static final Configuration conf = new Configuration();
    static {
      conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
          "kerberos");
      UserGroupInformation.setConfiguration(conf);
    }

    public MockRMWithAMS(ContainerManager containerManager) {
      super(conf, containerManager);
    }

    @Override
    protected void doSecureLogin() throws IOException {
      // Skip the login.
    }

    @Override
    protected ApplicationMasterService createApplicationMasterService() {

      return new ApplicationMasterService(getRMContext(),
          this.appTokenSecretManager, this.scheduler);
    }
  }

  @Test
  public void testAuthorizedAccess() throws Exception {
    MyContainerManager containerManager = new MyContainerManager();
    MockRM rm = new MockRMWithAMS(containerManager);
    rm.start();

    MockNM nm1 = rm.registerNode("localhost:1234", 5120);

    RMApp app = rm.submitApp(1024);

    nm1.nodeHeartbeat(true);

    int waitCount = 0;
    while (containerManager.containerEnv == null && waitCount++ < 20) {
      LOG.info("Waiting for AM Launch to happen..");
      Thread.sleep(1000);
    }
    Assert.assertNotNull(containerManager.containerEnv);

    RMAppAttempt attempt = app.getCurrentAppAttempt();
    ApplicationAttemptId applicationAttemptId = attempt.getAppAttemptId();
    waitForLaunchedState(attempt);

    // Create a client to the RM.
    final Configuration conf = rm.getConfig();
    final YarnRPC rpc = YarnRPC.create(conf);
    final String serviceAddr = conf.get(
        YarnConfiguration.RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS);

    UserGroupInformation currentUser = UserGroupInformation
        .createRemoteUser(applicationAttemptId.toString());
    String tokenURLEncodedStr = containerManager.containerEnv
        .get(ApplicationConstants.APPLICATION_MASTER_TOKEN_ENV_NAME);
    LOG.info("AppMasterToken is " + tokenURLEncodedStr);
    Token<? extends TokenIdentifier> token = new Token<TokenIdentifier>();
    token.decodeFromUrlString(tokenURLEncodedStr);
    currentUser.addToken(token);

    AMRMProtocol client = currentUser
        .doAs(new PrivilegedAction<AMRMProtocol>() {
          @Override
          public AMRMProtocol run() {
            return (AMRMProtocol) rpc.getProxy(AMRMProtocol.class, NetUtils
                .createSocketAddr(serviceAddr), conf);
          }
        });

    RegisterApplicationMasterRequest request = Records
        .newRecord(RegisterApplicationMasterRequest.class);
    request.setApplicationAttemptId(applicationAttemptId);
    client.registerApplicationMaster(request);

    rm.stop();
  }

  @Test
  public void testUnauthorizedAccess() throws Exception {
    MyContainerManager containerManager = new MyContainerManager();
    MockRM rm = new MockRMWithAMS(containerManager);
    rm.start();

    MockNM nm1 = rm.registerNode("localhost:1234", 5120);

    RMApp app = rm.submitApp(1024);

    nm1.nodeHeartbeat(true);

    int waitCount = 0;
    while (containerManager.containerEnv == null && waitCount++ < 20) {
      LOG.info("Waiting for AM Launch to happen..");
      Thread.sleep(1000);
    }
    Assert.assertNotNull(containerManager.containerEnv);

    RMAppAttempt attempt = app.getCurrentAppAttempt();
    ApplicationAttemptId applicationAttemptId = attempt.getAppAttemptId();
    waitForLaunchedState(attempt);

    // Create a client to the RM.
    final Configuration conf = rm.getConfig();
    final YarnRPC rpc = YarnRPC.create(conf);
    final String serviceAddr = conf.get(
        YarnConfiguration.RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS);

    UserGroupInformation currentUser = UserGroupInformation
        .createRemoteUser(applicationAttemptId.toString());
    String tokenURLEncodedStr = containerManager.containerEnv
        .get(ApplicationConstants.APPLICATION_MASTER_TOKEN_ENV_NAME);
    LOG.info("AppMasterToken is " + tokenURLEncodedStr);
    Token<? extends TokenIdentifier> token = new Token<TokenIdentifier>();
    token.decodeFromUrlString(tokenURLEncodedStr);
    currentUser.addToken(token);

    AMRMProtocol client = currentUser
        .doAs(new PrivilegedAction<AMRMProtocol>() {
          @Override
          public AMRMProtocol run() {
            return (AMRMProtocol) rpc.getProxy(AMRMProtocol.class, NetUtils
                .createSocketAddr(serviceAddr), conf);
          }
        });

    RegisterApplicationMasterRequest request = Records
        .newRecord(RegisterApplicationMasterRequest.class);
    ApplicationAttemptId otherAppAttemptId = BuilderUtils
        .newApplicationAttemptId(applicationAttemptId.getApplicationId(), 42);
    request.setApplicationAttemptId(otherAppAttemptId);
    try {
      client.registerApplicationMaster(request);
      Assert.fail("Should fail with authorization error");
    } catch (YarnRemoteException e) {
      Assert.assertEquals("Unauthorized request from ApplicationMaster. "
          + "Expected ApplicationAttemptID: "
          + applicationAttemptId.toString() + " Found: "
          + otherAppAttemptId.toString(), e.getMessage());
    } finally {
      rm.stop();
    }
  }

  private void waitForLaunchedState(RMAppAttempt attempt)
      throws InterruptedException {
    int waitCount = 0;
    while (attempt.getAppAttemptState() != RMAppAttemptState.LAUNCHED
        && waitCount++ < 20) {
      LOG.info("Waiting for AppAttempt to reach LAUNCHED state. "
          + "Current state is " + attempt.getAppAttemptState());
      Thread.sleep(1000);
    }
    Assert.assertEquals(attempt.getAppAttemptState(),
        RMAppAttemptState.LAUNCHED);
  }
}
