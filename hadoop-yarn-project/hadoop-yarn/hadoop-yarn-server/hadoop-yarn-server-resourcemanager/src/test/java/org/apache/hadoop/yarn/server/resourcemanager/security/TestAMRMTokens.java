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

package org.apache.hadoop.yarn.server.resourcemanager.security;

import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collection;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.TestAMAuthorization.MockRMWithAMS;
import org.apache.hadoop.yarn.server.resourcemanager.TestAMAuthorization.MyContainerManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerFinishedEvent;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestAMRMTokens {

  private static final Log LOG = LogFactory.getLog(TestAMRMTokens.class);

  private final Configuration conf;
  private static final int maxWaitAttempts = 50;

  @Parameters
  public static Collection<Object[]> configs() {
    Configuration conf = new Configuration();
    Configuration confWithSecurity = new Configuration();
    confWithSecurity.set(
      CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    return Arrays.asList(new Object[][] {{ conf }, { confWithSecurity } });
  }

  public TestAMRMTokens(Configuration conf) {
    this.conf = conf;
    UserGroupInformation.setConfiguration(conf);
  }

  /**
   * Validate that application tokens are unusable after the
   * application-finishes.
   * 
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testTokenExpiry() throws Exception {

    MyContainerManager containerManager = new MyContainerManager();
    final MockRMWithAMS rm =
        new MockRMWithAMS(conf, containerManager);
    rm.start();

    final Configuration conf = rm.getConfig();
    final YarnRPC rpc = YarnRPC.create(conf);
    ApplicationMasterProtocol rmClient = null;

    try {
      MockNM nm1 = rm.registerNode("localhost:1234", 5120);

      RMApp app = rm.submitApp(1024);

      nm1.nodeHeartbeat(true);

      int waitCount = 0;
      while (containerManager.containerTokens == null && waitCount++ < 20) {
        LOG.info("Waiting for AM Launch to happen..");
        Thread.sleep(1000);
      }
      Assert.assertNotNull(containerManager.containerTokens);

      RMAppAttempt attempt = app.getCurrentAppAttempt();
      ApplicationAttemptId applicationAttemptId = attempt.getAppAttemptId();

      // Create a client to the RM.
      UserGroupInformation currentUser =
          UserGroupInformation
            .createRemoteUser(applicationAttemptId.toString());
      Credentials credentials = containerManager.getContainerCredentials();
      final InetSocketAddress rmBindAddress =
          rm.getApplicationMasterService().getBindAddress();
      Token<? extends TokenIdentifier> amRMToken =
          MockRMWithAMS.setupAndReturnAMRMToken(rmBindAddress,
            credentials.getAllTokens());
      currentUser.addToken(amRMToken);
      rmClient = createRMClient(rm, conf, rpc, currentUser);

      RegisterApplicationMasterRequest request =
          Records.newRecord(RegisterApplicationMasterRequest.class);
      rmClient.registerApplicationMaster(request);

      FinishApplicationMasterRequest finishAMRequest =
          Records.newRecord(FinishApplicationMasterRequest.class);
      finishAMRequest
        .setFinalApplicationStatus(FinalApplicationStatus.SUCCEEDED);
      finishAMRequest.setDiagnostics("diagnostics");
      finishAMRequest.setTrackingUrl("url");
      rmClient.finishApplicationMaster(finishAMRequest);

      // Send RMAppAttemptEventType.CONTAINER_FINISHED to transit RMAppAttempt
      // from Finishing state to Finished State. Both AMRMToken and
      // ClientToAMToken will be removed.
      ContainerStatus containerStatus =
          BuilderUtils.newContainerStatus(attempt.getMasterContainer().getId(),
              ContainerState.COMPLETE,
              "AM Container Finished", 0);
      rm.getRMContext()
          .getDispatcher()
          .getEventHandler()
          .handle(
              new RMAppAttemptContainerFinishedEvent(applicationAttemptId,
                  containerStatus));

      // Make sure the RMAppAttempt is at Finished State.
      // Both AMRMToken and ClientToAMToken have been removed.
      int count = 0;
      while (attempt.getState() != RMAppAttemptState.FINISHED
          && count < maxWaitAttempts) {
        Thread.sleep(100);
        count++;
      }
      Assert.assertTrue(attempt.getState() == RMAppAttemptState.FINISHED);

      // Now simulate trying to allocate. RPC call itself should throw auth
      // exception.
      rpc.stopProxy(rmClient, conf); // To avoid using cached client
      rmClient = createRMClient(rm, conf, rpc, currentUser);
      AllocateRequest allocateRequest =
          Records.newRecord(AllocateRequest.class);
      try {
        rmClient.allocate(allocateRequest);
        Assert.fail("You got to be kidding me! "
            + "Using App tokens after app-finish should fail!");
      } catch (Throwable t) {
        LOG.info("Exception found is ", t);
        // The exception will still have the earlier appAttemptId as it picks it
        // up from the token.
        Assert.assertTrue(t.getCause().getMessage().contains(
            "Password not found for ApplicationAttempt " +
            applicationAttemptId.toString()));
      }

    } finally {
      rm.stop();
      if (rmClient != null) {
        rpc.stopProxy(rmClient, conf); // To avoid using cached client
      }
    }
  }

  /**
   * Validate master-key-roll-over and that tokens are usable even after
   * master-key-roll-over.
   * 
   * @throws Exception
   */
  @Test
  public void testMasterKeyRollOver() throws Exception {

    MyContainerManager containerManager = new MyContainerManager();
    final MockRMWithAMS rm =
        new MockRMWithAMS(conf, containerManager);
    rm.start();

    final Configuration conf = rm.getConfig();
    final YarnRPC rpc = YarnRPC.create(conf);
    ApplicationMasterProtocol rmClient = null;

    try {
      MockNM nm1 = rm.registerNode("localhost:1234", 5120);

      RMApp app = rm.submitApp(1024);

      nm1.nodeHeartbeat(true);

      int waitCount = 0;
      while (containerManager.containerTokens == null && waitCount++ < 20) {
        LOG.info("Waiting for AM Launch to happen..");
        Thread.sleep(1000);
      }
      Assert.assertNotNull(containerManager.containerTokens);

      RMAppAttempt attempt = app.getCurrentAppAttempt();
      ApplicationAttemptId applicationAttemptId = attempt.getAppAttemptId();

      // Create a client to the RM.
      UserGroupInformation currentUser =
          UserGroupInformation
            .createRemoteUser(applicationAttemptId.toString());
      Credentials credentials = containerManager.getContainerCredentials();
      final InetSocketAddress rmBindAddress =
          rm.getApplicationMasterService().getBindAddress();
      Token<? extends TokenIdentifier> amRMToken =
          MockRMWithAMS.setupAndReturnAMRMToken(rmBindAddress,
            credentials.getAllTokens());
      currentUser.addToken(amRMToken);
      rmClient = createRMClient(rm, conf, rpc, currentUser);

      RegisterApplicationMasterRequest request =
          Records.newRecord(RegisterApplicationMasterRequest.class);
      rmClient.registerApplicationMaster(request);

      // One allocate call.
      AllocateRequest allocateRequest =
          Records.newRecord(AllocateRequest.class);
      Assert.assertTrue(
          rmClient.allocate(allocateRequest).getAMCommand() == null);

      // Simulate a master-key-roll-over
      AMRMTokenSecretManager appTokenSecretManager =
          rm.getRMContext().getAMRMTokenSecretManager();
      SecretKey oldKey = appTokenSecretManager.getMasterKey();
      appTokenSecretManager.rollMasterKey();
      SecretKey newKey = appTokenSecretManager.getMasterKey();
      Assert.assertFalse("Master key should have changed!",
        oldKey.equals(newKey));

      // Another allocate call. Should continue to work.
      rpc.stopProxy(rmClient, conf); // To avoid using cached client
      rmClient = createRMClient(rm, conf, rpc, currentUser);
      allocateRequest = Records.newRecord(AllocateRequest.class);
      Assert.assertTrue(
          rmClient.allocate(allocateRequest).getAMCommand() == null);
    } finally {
      rm.stop();
      if (rmClient != null) {
        rpc.stopProxy(rmClient, conf); // To avoid using cached client
      }
    }
  }

  private ApplicationMasterProtocol createRMClient(final MockRM rm,
      final Configuration conf, final YarnRPC rpc,
      UserGroupInformation currentUser) {
    return currentUser.doAs(new PrivilegedAction<ApplicationMasterProtocol>() {
      @Override
      public ApplicationMasterProtocol run() {
        return (ApplicationMasterProtocol) rpc.getProxy(ApplicationMasterProtocol.class, rm
          .getApplicationMasterService().getBindAddress(), conf);
      }
    });
  }
}
