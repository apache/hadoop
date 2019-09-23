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

import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMSecretManagerService;
import org.apache.hadoop.yarn.server.resourcemanager.TestAMAuthorization.MockRMWithAMS;
import org.apache.hadoop.yarn.server.resourcemanager.TestAMAuthorization.MyContainerManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerFinishedEvent;
import org.apache.hadoop.yarn.server.security.MasterKeyData;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestAMRMTokens {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestAMRMTokens.class);

  private final Configuration conf;
  private static final int maxWaitAttempts = 50;
  private static final int rolling_interval_sec = 13;
  private static final long am_expire_ms = 4000;

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
    conf.setLong(
        YarnConfiguration.RM_AMRM_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS,
        YarnConfiguration.
            DEFAULT_RM_AMRM_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS);
    conf.setLong(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS,
        YarnConfiguration.DEFAULT_RM_AM_EXPIRY_INTERVAL_MS);
    conf.set(YarnConfiguration.RM_SCHEDULER_ADDRESS,
        "0.0.0.0:0");

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
              "AM Container Finished", 0,
              attempt.getMasterContainer().getResource());
      rm.getRMContext()
          .getDispatcher()
          .getEventHandler()
          .handle(
              new RMAppAttemptContainerFinishedEvent(applicationAttemptId,
                  containerStatus, nm1.getNodeId()));

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
          applicationAttemptId.toString()
          + " not found in AMRMTokenSecretManager."));
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

    conf.setLong(
      YarnConfiguration.RM_AMRM_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS,
      rolling_interval_sec);
    conf.setLong(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, am_expire_ms);
    conf.set(YarnConfiguration.RM_SCHEDULER_ADDRESS,
        "0.0.0.0:0");
    MyContainerManager containerManager = new MyContainerManager();
    final MockRMWithAMS rm =
        new MockRMWithAMS(conf, containerManager);
    rm.start();
    Long startTime = System.currentTimeMillis();
    final Configuration conf = rm.getConfig();
    final YarnRPC rpc = YarnRPC.create(conf);
    ApplicationMasterProtocol rmClient = null;
    AMRMTokenSecretManager appTokenSecretManager =
        rm.getRMContext().getAMRMTokenSecretManager();
    MasterKeyData oldKey = appTokenSecretManager.getMasterKey();
    Assert.assertNotNull(oldKey);
    try {
      MockNM nm1 = rm.registerNode("localhost:1234", 5120);

      RMApp app = rm.submitApp(1024);

      nm1.nodeHeartbeat(true);

      int waitCount = 0;
      while (containerManager.containerTokens == null && waitCount++ < maxWaitAttempts) {
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

      // Wait for enough time and make sure the roll_over happens
      // At mean time, the old AMRMToken should continue to work
      while(System.currentTimeMillis() - startTime < rolling_interval_sec*1000) {
        rmClient.allocate(allocateRequest);
        Thread.sleep(500);
      }

      MasterKeyData newKey = appTokenSecretManager.getMasterKey();
      Assert.assertNotNull(newKey);
      Assert.assertFalse("Master key should have changed!",
        oldKey.equals(newKey));

      // Another allocate call with old AMRMToken. Should continue to work.
      rpc.stopProxy(rmClient, conf); // To avoid using cached client
      rmClient = createRMClient(rm, conf, rpc, currentUser);
      Assert
        .assertTrue(rmClient.allocate(allocateRequest).getAMCommand() == null);

      waitCount = 0;
      while(waitCount++ <= maxWaitAttempts) {
        if (appTokenSecretManager.getCurrnetMasterKeyData() != oldKey) {
          break;
        }
        try {
          rmClient.allocate(allocateRequest);
        } catch (Exception ex) {
          break;
        }
        Thread.sleep(200);
      }
      // active the nextMasterKey, and replace the currentMasterKey
      Assert.assertTrue(appTokenSecretManager.getCurrnetMasterKeyData().equals(newKey));
      Assert.assertTrue(appTokenSecretManager.getMasterKey().equals(newKey));
      Assert.assertTrue(appTokenSecretManager.getNextMasterKeyData() == null);

      // Create a new Token
      Token<AMRMTokenIdentifier> newToken =
          appTokenSecretManager.createAndGetAMRMToken(applicationAttemptId);
      SecurityUtil.setTokenService(newToken, rmBindAddress);
      currentUser.addToken(newToken);
      // Another allocate call. Should continue to work.
      rpc.stopProxy(rmClient, conf); // To avoid using cached client
      rmClient = createRMClient(rm, conf, rpc, currentUser);
      allocateRequest = Records.newRecord(AllocateRequest.class);
      Assert
        .assertTrue(rmClient.allocate(allocateRequest).getAMCommand() == null);

      // Should not work by using the old AMRMToken.
      rpc.stopProxy(rmClient, conf); // To avoid using cached client
      try {
        currentUser.addToken(amRMToken);
        rmClient = createRMClient(rm, conf, rpc, currentUser);
        allocateRequest = Records.newRecord(AllocateRequest.class);
        Assert
          .assertTrue(rmClient.allocate(allocateRequest).getAMCommand() == null);
        Assert.fail("The old Token should not work");
      } catch (Exception ex) {
        // expect exception
      }
    } finally {
      rm.stop();
      if (rmClient != null) {
        rpc.stopProxy(rmClient, conf); // To avoid using cached client
      }
    }
  }

  @Test (timeout = 20000)
  public void testAMRMMasterKeysUpdate() throws Exception {
    final AtomicReference<AMRMTokenSecretManager> spySecretMgrRef =
        new AtomicReference<AMRMTokenSecretManager>();
    MockRM rm = new MockRM(conf) {
      @Override
      protected void doSecureLogin() throws IOException {
        // Skip the login.
      }

      @Override
      protected RMSecretManagerService createRMSecretManagerService() {
        return new RMSecretManagerService(conf, rmContext) {
          @Override
          protected AMRMTokenSecretManager createAMRMTokenSecretManager(
              Configuration conf, RMContext rmContext) {
            AMRMTokenSecretManager spySecretMgr = spy(
                super.createAMRMTokenSecretManager(conf, rmContext));
            spySecretMgrRef.set(spySecretMgr);
            return spySecretMgr;
          }
        };
      }
    };
    rm.start();
    MockNM nm = rm.registerNode("127.0.0.1:1234", 8000);
    RMApp app = rm.submitApp(200);
    MockAM am = MockRM.launchAndRegisterAM(app, rm, nm);
    AMRMTokenSecretManager spySecretMgr = spySecretMgrRef.get();
    // Do allocate. Should not update AMRMToken
    AllocateResponse response =
        am.allocate(Records.newRecord(AllocateRequest.class));
    Assert.assertNull(response.getAMRMToken());
    Token<AMRMTokenIdentifier> oldToken = rm.getRMContext().getRMApps()
        .get(app.getApplicationId())
        .getRMAppAttempt(am.getApplicationAttemptId()).getAMRMToken();

    // roll over the master key
    // Do allocate again. the AM should get the latest AMRMToken
    rm.getRMContext().getAMRMTokenSecretManager().rollMasterKey();
    response = am.allocate(Records.newRecord(AllocateRequest.class));
    Assert.assertNotNull(response.getAMRMToken());

    Token<AMRMTokenIdentifier> amrmToken =
        ConverterUtils.convertFromYarn(response.getAMRMToken(), new Text(
          response.getAMRMToken().getService()));

    Assert.assertEquals(amrmToken.decodeIdentifier().getKeyId(), rm
      .getRMContext().getAMRMTokenSecretManager().getMasterKey().getMasterKey()
      .getKeyId());

    // Do allocate again with the same old token and verify the RM sends
    // back the last generated token instead of generating it again.
    reset(spySecretMgr);
    UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
        am.getApplicationAttemptId().toString(), new String[0]);
    ugi.addTokenIdentifier(oldToken.decodeIdentifier());
    response = am.doAllocateAs(ugi, Records.newRecord(AllocateRequest.class));
    Assert.assertNotNull(response.getAMRMToken());
    verify(spySecretMgr, never()).createAndGetAMRMToken(isA(ApplicationAttemptId.class));

    // Do allocate again with the updated token and verify we do not
    // receive a new token to use.
    response = am.allocate(Records.newRecord(AllocateRequest.class));
    Assert.assertNull(response.getAMRMToken());

    // Activate the next master key. Since there is new master key generated
    // in AMRMTokenSecretManager. The AMRMToken will not get updated for AM
    rm.getRMContext().getAMRMTokenSecretManager().activateNextMasterKey();
    response = am.allocate(Records.newRecord(AllocateRequest.class));
    Assert.assertNull(response.getAMRMToken());
    rm.stop();
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
