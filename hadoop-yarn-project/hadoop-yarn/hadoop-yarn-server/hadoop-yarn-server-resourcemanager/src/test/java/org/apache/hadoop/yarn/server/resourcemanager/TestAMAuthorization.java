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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestAMAuthorization {

  private static final Log LOG = LogFactory.getLog(TestAMAuthorization.class);

  private final Configuration conf;
  private MockRM rm;

  @Parameters
  public static Collection<Object[]> configs() {
    Configuration conf = new Configuration();
    Configuration confWithSecurity = new Configuration();
    confWithSecurity.set(
      CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
      UserGroupInformation.AuthenticationMethod.KERBEROS.toString());
    return Arrays.asList(new Object[][] {{ conf }, { confWithSecurity} });
  }

  public TestAMAuthorization(Configuration conf) {
    this.conf = conf;
    UserGroupInformation.setConfiguration(conf);
  }

  @After
  public void tearDown() {
    if (rm != null) {
      rm.stop();
    }
  }

  public static final class MyContainerManager implements ContainerManagementProtocol {

    public ByteBuffer containerTokens;

    public MyContainerManager() {
    }

    @Override
    public StartContainerResponse
        startContainer(StartContainerRequest request)
            throws YarnException {
      containerTokens = request.getContainerLaunchContext().getTokens();
      return null;
    }

    @Override
    public StopContainerResponse stopContainer(StopContainerRequest request)
        throws YarnException {
      return null;
    }

    @Override
    public GetContainerStatusResponse getContainerStatus(
        GetContainerStatusRequest request) throws YarnException {
      return null;
    }

    public Credentials getContainerCredentials() throws IOException {
      Credentials credentials = new Credentials();
      DataInputByteBuffer buf = new DataInputByteBuffer();
      containerTokens.rewind();
      buf.reset(containerTokens);
      credentials.readTokenStorageStream(buf);
      return credentials;
    }
  }

  public static class MockRMWithAMS extends MockRMWithCustomAMLauncher {

    public MockRMWithAMS(Configuration conf, ContainerManagementProtocol containerManager) {
      super(conf, containerManager);
    }

    @Override
    protected void doSecureLogin() throws IOException {
      // Skip the login.
    }

    @Override
    protected ApplicationMasterService createApplicationMasterService() {
      return new ApplicationMasterService(getRMContext(), this.scheduler);
    }
  }

  @Test
  public void testAuthorizedAccess() throws Exception {
    MyContainerManager containerManager = new MyContainerManager();
    rm =
        new MockRMWithAMS(conf, containerManager);
    rm.start();

    MockNM nm1 = rm.registerNode("localhost:1234", 5120);

    Map<ApplicationAccessType, String> acls =
        new HashMap<ApplicationAccessType, String>(2);
    acls.put(ApplicationAccessType.VIEW_APP, "*");
    RMApp app = rm.submitApp(1024, "appname", "appuser", acls);

    nm1.nodeHeartbeat(true);

    int waitCount = 0;
    while (containerManager.containerTokens == null && waitCount++ < 20) {
      LOG.info("Waiting for AM Launch to happen..");
      Thread.sleep(1000);
    }
    Assert.assertNotNull(containerManager.containerTokens);

    RMAppAttempt attempt = app.getCurrentAppAttempt();
    ApplicationAttemptId applicationAttemptId = attempt.getAppAttemptId();
    waitForLaunchedState(attempt);

    // Create a client to the RM.
    final Configuration conf = rm.getConfig();
    final YarnRPC rpc = YarnRPC.create(conf);

    UserGroupInformation currentUser = UserGroupInformation
        .createRemoteUser(applicationAttemptId.toString());
    Credentials credentials = containerManager.getContainerCredentials();
    currentUser.addCredentials(credentials);

    ApplicationMasterProtocol client = currentUser
        .doAs(new PrivilegedAction<ApplicationMasterProtocol>() {
          @Override
          public ApplicationMasterProtocol run() {
            return (ApplicationMasterProtocol) rpc.getProxy(ApplicationMasterProtocol.class, rm
              .getApplicationMasterService().getBindAddress(), conf);
          }
        });

    RegisterApplicationMasterRequest request = Records
        .newRecord(RegisterApplicationMasterRequest.class);
    RegisterApplicationMasterResponse response =
        client.registerApplicationMaster(request);
    Assert.assertNotNull(response.getClientToAMTokenMasterKey());
    if (UserGroupInformation.isSecurityEnabled()) {
      Assert
        .assertTrue(response.getClientToAMTokenMasterKey().array().length > 0);
    }
    Assert.assertEquals("Register response has bad ACLs", "*",
        response.getApplicationACLs().get(ApplicationAccessType.VIEW_APP));
  }

  @Test
  public void testUnauthorizedAccess() throws Exception {
    MyContainerManager containerManager = new MyContainerManager();
    rm = new MockRMWithAMS(conf, containerManager);
    rm.start();

    MockNM nm1 = rm.registerNode("localhost:1234", 5120);

    RMApp app = rm.submitApp(1024);

    nm1.nodeHeartbeat(true);

    int waitCount = 0;
    while (containerManager.containerTokens == null && waitCount++ < 40) {
      LOG.info("Waiting for AM Launch to happen..");
      Thread.sleep(1000);
    }
    Assert.assertNotNull(containerManager.containerTokens);

    RMAppAttempt attempt = app.getCurrentAppAttempt();
    ApplicationAttemptId applicationAttemptId = attempt.getAppAttemptId();
    waitForLaunchedState(attempt);

    final Configuration conf = rm.getConfig();
    final YarnRPC rpc = YarnRPC.create(conf);
    final InetSocketAddress serviceAddr = conf.getSocketAddr(
        YarnConfiguration.RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);

    UserGroupInformation currentUser = UserGroupInformation
        .createRemoteUser(applicationAttemptId.toString());

    // First try contacting NM without tokens
    ApplicationMasterProtocol client = currentUser
        .doAs(new PrivilegedAction<ApplicationMasterProtocol>() {
          @Override
          public ApplicationMasterProtocol run() {
            return (ApplicationMasterProtocol) rpc.getProxy(ApplicationMasterProtocol.class,
                serviceAddr, conf);
          }
        });
    
    RegisterApplicationMasterRequest request = Records
        .newRecord(RegisterApplicationMasterRequest.class);
    try {
      client.registerApplicationMaster(request);
      Assert.fail("Should fail with authorization error");
    } catch (Exception e) {
      // Because there are no tokens, the request should be rejected as the
      // server side will assume we are trying simple auth.
      String availableAuthMethods;
      if (UserGroupInformation.isSecurityEnabled()) {
        availableAuthMethods = "[TOKEN, KERBEROS]";
      } else {
        availableAuthMethods = "[TOKEN]";
      }
      Assert.assertTrue(e.getCause().getMessage().contains(
        "SIMPLE authentication is not enabled.  "
            + "Available:" + availableAuthMethods));
    }

    // TODO: Add validation of invalid authorization when there's more data in
    // the AMRMToken
  }

  private void waitForLaunchedState(RMAppAttempt attempt)
      throws InterruptedException {
    int waitCount = 0;
    while (attempt.getAppAttemptState() != RMAppAttemptState.LAUNCHED
        && waitCount++ < 40) {
      LOG.info("Waiting for AppAttempt to reach LAUNCHED state. "
          + "Current state is " + attempt.getAppAttemptState());
      Thread.sleep(1000);
    }
    Assert.assertEquals(attempt.getAppAttemptState(),
        RMAppAttemptState.LAUNCHED);
  }
}
