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
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ContainerManager;
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
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Assert;
import org.junit.Test;

public class TestAMAuthorization {

  private static final Log LOG = LogFactory.getLog(TestAMAuthorization.class);

  private static final Configuration confWithSecurityEnabled =
      new Configuration();
  static {
    confWithSecurityEnabled.set(
      CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(confWithSecurityEnabled);
  }

  public static final class MyContainerManager implements ContainerManager {

    public ByteBuffer amTokens;

    public MyContainerManager() {
    }

    @Override
    public StartContainerResponse
        startContainer(StartContainerRequest request)
            throws YarnRemoteException {
      amTokens = request.getContainerLaunchContext().getContainerTokens();
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

  public static class MockRMWithAMS extends MockRMWithCustomAMLauncher {

    public MockRMWithAMS(Configuration conf, ContainerManager containerManager) {
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
    final MockRM rm =
        new MockRMWithAMS(confWithSecurityEnabled, containerManager);
    rm.start();

    MockNM nm1 = rm.registerNode("localhost:1234", 5120);

    Map<ApplicationAccessType, String> acls =
        new HashMap<ApplicationAccessType, String>(2);
    acls.put(ApplicationAccessType.VIEW_APP, "*");
    RMApp app = rm.submitApp(1024, "appname", "appuser", acls);

    nm1.nodeHeartbeat(true);

    int waitCount = 0;
    while (containerManager.amTokens == null && waitCount++ < 20) {
      LOG.info("Waiting for AM Launch to happen..");
      Thread.sleep(1000);
    }
    Assert.assertNotNull(containerManager.amTokens);

    RMAppAttempt attempt = app.getCurrentAppAttempt();
    ApplicationAttemptId applicationAttemptId = attempt.getAppAttemptId();
    waitForLaunchedState(attempt);

    // Create a client to the RM.
    final Configuration conf = rm.getConfig();
    final YarnRPC rpc = YarnRPC.create(conf);

    UserGroupInformation currentUser = UserGroupInformation
        .createRemoteUser(applicationAttemptId.toString());
    Credentials credentials = new Credentials();
    DataInputByteBuffer buf = new DataInputByteBuffer();
    containerManager.amTokens.rewind();
    buf.reset(containerManager.amTokens);
    credentials.readTokenStorageStream(buf);
    currentUser.addCredentials(credentials);

    AMRMProtocol client = currentUser
        .doAs(new PrivilegedAction<AMRMProtocol>() {
          @Override
          public AMRMProtocol run() {
            return (AMRMProtocol) rpc.getProxy(AMRMProtocol.class, rm
              .getApplicationMasterService().getBindAddress(), conf);
          }
        });

    RegisterApplicationMasterRequest request = Records
        .newRecord(RegisterApplicationMasterRequest.class);
    request.setApplicationAttemptId(applicationAttemptId);
    RegisterApplicationMasterResponse response =
        client.registerApplicationMaster(request);
    Assert.assertEquals("Register response has bad ACLs", "*",
        response.getApplicationACLs().get(ApplicationAccessType.VIEW_APP));

    rm.stop();
  }

  @Test
  public void testUnauthorizedAccess() throws Exception {
    MyContainerManager containerManager = new MyContainerManager();
    MockRM rm = new MockRMWithAMS(confWithSecurityEnabled, containerManager);
    rm.start();

    MockNM nm1 = rm.registerNode("localhost:1234", 5120);

    RMApp app = rm.submitApp(1024);

    nm1.nodeHeartbeat(true);

    int waitCount = 0;
    while (containerManager.amTokens == null && waitCount++ < 20) {
      LOG.info("Waiting for AM Launch to happen..");
      Thread.sleep(1000);
    }
    Assert.assertNotNull(containerManager.amTokens);

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
    AMRMProtocol client = currentUser
        .doAs(new PrivilegedAction<AMRMProtocol>() {
          @Override
          public AMRMProtocol run() {
            return (AMRMProtocol) rpc.getProxy(AMRMProtocol.class,
                serviceAddr, conf);
          }
        });
    RegisterApplicationMasterRequest request = Records
        .newRecord(RegisterApplicationMasterRequest.class);
    request.setApplicationAttemptId(applicationAttemptId);
    try {
      client.registerApplicationMaster(request);
      Assert.fail("Should fail with authorization error");
    } catch (Exception e) {
      // Because there are no tokens, the request should be rejected as the
      // server side will assume we are trying simple auth.
      Assert.assertTrue(e.getCause().getMessage().contains(
        "SIMPLE authentication is not enabled.  "
            + "Available:[KERBEROS, DIGEST]"));
    }

    // Now try to validate invalid authorization.
    Credentials credentials = new Credentials();
    DataInputByteBuffer buf = new DataInputByteBuffer();
    containerManager.amTokens.rewind();
    buf.reset(containerManager.amTokens);
    credentials.readTokenStorageStream(buf);
    currentUser.addCredentials(credentials);

    // Create a client to the RM.
    client = currentUser
        .doAs(new PrivilegedAction<AMRMProtocol>() {
          @Override
          public AMRMProtocol run() {
            return (AMRMProtocol) rpc.getProxy(AMRMProtocol.class,
                serviceAddr, conf);
          }
        });

    request = Records.newRecord(RegisterApplicationMasterRequest.class);
    ApplicationAttemptId otherAppAttemptId = BuilderUtils
        .newApplicationAttemptId(applicationAttemptId.getApplicationId(), 42);
    request.setApplicationAttemptId(otherAppAttemptId);
    try {
      client.registerApplicationMaster(request);
      Assert.fail("Should fail with authorization error");
    } catch (YarnRemoteException e) {
      Assert.assertTrue(e.getMessage().contains(
          "Unauthorized request from ApplicationMaster. "
              + "Expected ApplicationAttemptID: "
              + applicationAttemptId.toString() + " Found: "
              + otherAppAttemptId.toString()));
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
