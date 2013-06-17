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

package org.apache.hadoop.yarn.server;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.security.AMRMTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Test;

public class TestContainerManagerSecurity {

  static Log LOG = LogFactory.getLog(TestContainerManagerSecurity.class);
  static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);
  private static MiniYARNCluster yarnCluster;

  static final Configuration conf = new Configuration();

  @Test (timeout = 1000000)
  public void testContainerManagerWithSecurityEnabled() throws Exception {
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    testContainerManager();
  }
  
  @Test (timeout=1000000)
  public void testContainerManagerWithSecurityDisabled() throws Exception {
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "simple");
    testContainerManager();
  }
  
  private void testContainerManager() throws Exception {
    try {
      yarnCluster = new MiniYARNCluster(TestContainerManagerSecurity.class
          .getName(), 1, 1, 1);
      conf.setLong(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, 100000L);
      UserGroupInformation.setConfiguration(conf);
      yarnCluster.init(conf);
      yarnCluster.start();
      
      // Testing for authenticated user
      testAuthenticatedUser();
      
      // Testing for malicious user
      testMaliceUser();
      
      // Testing for usage of expired tokens
      testExpiredTokens();
      
    } finally {
      if (yarnCluster != null) {
        yarnCluster.stop();
        yarnCluster = null;
      }
    }
  }
  
  private void testAuthenticatedUser() throws IOException,
      InterruptedException, YarnException {

    LOG.info("Running test for authenticated user");

    ResourceManager resourceManager = yarnCluster.getResourceManager();

    final YarnRPC yarnRPC = YarnRPC.create(conf);

    // Submit an application
    ApplicationId appID = resourceManager.getClientRMService()
        .getNewApplication(Records.newRecord(GetNewApplicationRequest.class))
        .getApplicationId();
    ApplicationMasterProtocol scheduler = submitAndRegisterApplication(resourceManager,
        yarnRPC, appID);

    // Now request a container.
    final Container allocatedContainer = requestAndGetContainer(scheduler,
        appID);

    // Now talk to the NM for launching the container.
    final ContainerId containerID = allocatedContainer.getId();
    UserGroupInformation authenticatedUser = UserGroupInformation
        .createRemoteUser(containerID.toString());
    org.apache.hadoop.yarn.api.records.Token containerToken =
        allocatedContainer.getContainerToken();
    Token<ContainerTokenIdentifier> token = new Token<ContainerTokenIdentifier>(
        containerToken.getIdentifier().array(), containerToken.getPassword()
            .array(), new Text(containerToken.getKind()), new Text(
            containerToken.getService()));
    authenticatedUser.addToken(token);
    authenticatedUser.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        ContainerManagementProtocol client = (ContainerManagementProtocol) yarnRPC.getProxy(
            ContainerManagementProtocol.class, NetUtils
                .createSocketAddr(allocatedContainer.getNodeId().toString()),
            conf);
        LOG.info("Going to make a legal stopContainer() request");
        StopContainerRequest request = recordFactory
            .newRecordInstance(StopContainerRequest.class);
        request.setContainerId(containerID);
        client.stopContainer(request);
        return null;
      }
    });

    KillApplicationRequest request = Records
        .newRecord(KillApplicationRequest.class);
    request.setApplicationId(appID);
    resourceManager.getClientRMService().forceKillApplication(request);
  }

  /**
   * This tests a malice user getting a proper token but then messing with it by
   * tampering with containerID/Resource etc.. His/her containers should be
   * rejected.
   * 
   * @throws IOException
   * @throws InterruptedException
   * @throws YarnException
   */
  private void testMaliceUser() throws IOException, InterruptedException,
      YarnException {

    LOG.info("Running test for malice user");

    ResourceManager resourceManager = yarnCluster.getResourceManager();

    final YarnRPC yarnRPC = YarnRPC.create(conf);

    // Submit an application
    ApplicationId appID = resourceManager.getClientRMService()
        .getNewApplication(Records.newRecord(GetNewApplicationRequest.class))
        .getApplicationId();
    ApplicationMasterProtocol scheduler = submitAndRegisterApplication(resourceManager,
        yarnRPC, appID);

    // Now request a container.
    final Container allocatedContainer = requestAndGetContainer(scheduler,
        appID);

    // Now talk to the NM for launching the container with modified resource

    org.apache.hadoop.yarn.api.records.Token containerToken =
        allocatedContainer.getContainerToken();
    ContainerTokenIdentifier originalContainerTokenId =
        BuilderUtils.newContainerTokenIdentifier(containerToken);

    // Malice user modifies the resource amount
    Resource modifiedResource = BuilderUtils.newResource(2048, 1);
    ContainerTokenIdentifier modifiedIdentifier =
        new ContainerTokenIdentifier(originalContainerTokenId.getContainerID(),
          originalContainerTokenId.getNmHostAddress(), "testUser",
          modifiedResource, Long.MAX_VALUE,
          originalContainerTokenId.getMasterKeyId(),
          ResourceManager.clusterTimeStamp);
    Token<ContainerTokenIdentifier> modifiedToken =
        new Token<ContainerTokenIdentifier>(modifiedIdentifier.getBytes(),
          containerToken.getPassword().array(), new Text(
            containerToken.getKind()), new Text(containerToken.getService()));
    makeTamperedStartContainerCall(yarnRPC, allocatedContainer,
      modifiedIdentifier, modifiedToken);

    // Malice user modifies the container-Id
    ContainerId newContainerId =
        BuilderUtils.newContainerId(
          BuilderUtils.newApplicationAttemptId(originalContainerTokenId
            .getContainerID().getApplicationAttemptId().getApplicationId(), 1),
          originalContainerTokenId.getContainerID().getId() + 42);
    modifiedIdentifier =
        new ContainerTokenIdentifier(newContainerId,
          originalContainerTokenId.getNmHostAddress(), "testUser",
          originalContainerTokenId.getResource(), Long.MAX_VALUE,
          originalContainerTokenId.getMasterKeyId(),
          ResourceManager.clusterTimeStamp);
    modifiedToken =
        new Token<ContainerTokenIdentifier>(modifiedIdentifier.getBytes(),
          containerToken.getPassword().array(), new Text(
            containerToken.getKind()), new Text(containerToken.getService()));
    makeTamperedStartContainerCall(yarnRPC, allocatedContainer,
      modifiedIdentifier, modifiedToken);

    // Similarly messing with anything else will fail.

    KillApplicationRequest request = Records
        .newRecord(KillApplicationRequest.class);
    request.setApplicationId(appID);
    resourceManager.getClientRMService().forceKillApplication(request);
  }

  private void makeTamperedStartContainerCall(final YarnRPC yarnRPC,
      final Container allocatedContainer,
      final ContainerTokenIdentifier modifiedIdentifier,
      Token<ContainerTokenIdentifier> modifiedToken) {
    final ContainerId containerID = allocatedContainer.getId();
    UserGroupInformation maliceUser = UserGroupInformation
        .createRemoteUser(containerID.toString());
    maliceUser.addToken(modifiedToken);
    maliceUser.doAs(new PrivilegedAction<Void>() {
      @Override
      public Void run() {
        ContainerManagementProtocol client = (ContainerManagementProtocol) yarnRPC.getProxy(
            ContainerManagementProtocol.class, NetUtils
                .createSocketAddr(allocatedContainer.getNodeId().toString()),
            conf);

        LOG.info("Going to contact NM:  ilLegal request");
        StartContainerRequest request =
            Records.newRecord(StartContainerRequest.class);
        try {
          request.setContainerToken(allocatedContainer.getContainerToken());
          ContainerLaunchContext context =
              createContainerLaunchContextForTest(modifiedIdentifier);
          request.setContainerLaunchContext(context);
          client.startContainer(request);
          fail("Connection initiation with illegally modified "
              + "tokens is expected to fail.");
        } catch (YarnException e) {
          LOG.error("Got exception", e);
          fail("Cannot get a YARN remote exception as "
              + "it will indicate RPC success");
        } catch (Exception e) {
          Assert.assertEquals(
              javax.security.sasl.SaslException.class
              .getCanonicalName(), e.getClass().getCanonicalName());
          Assert.assertTrue(e
            .getMessage()
            .contains(
              "DIGEST-MD5: digest response format violation. "
                  + "Mismatched response."));
        }
        return null;
      }
    });
  }

  private void testExpiredTokens() throws IOException, InterruptedException,
      YarnException {

    LOG.info("\n\nRunning test for malice user");

    ResourceManager resourceManager = yarnCluster.getResourceManager();

    final YarnRPC yarnRPC = YarnRPC.create(conf);

    // Submit an application
    final ApplicationId appID = resourceManager.getClientRMService()
        .getNewApplication(Records.newRecord(GetNewApplicationRequest.class))
        .getApplicationId();
    ApplicationMasterProtocol scheduler = submitAndRegisterApplication(resourceManager,
        yarnRPC, appID);

    // Now request a container.
    final Container allocatedContainer = requestAndGetContainer(scheduler,
        appID);

    // Now talk to the NM for launching the container with modified containerID
    final ContainerId containerID = allocatedContainer.getId();

    org.apache.hadoop.yarn.api.records.Token containerToken =
        allocatedContainer.getContainerToken();
    final ContainerTokenIdentifier tokenId =
        BuilderUtils.newContainerTokenIdentifier(containerToken);

    /////////// Test calls with expired tokens
    UserGroupInformation unauthorizedUser = UserGroupInformation
        .createRemoteUser(containerID.toString());

    RMContainerTokenSecretManager containerTokenSecreteManager = 
      resourceManager.getRMContainerTokenSecretManager(); 
    final ContainerTokenIdentifier newTokenId =
        new ContainerTokenIdentifier(tokenId.getContainerID(),
          tokenId.getNmHostAddress(), tokenId.getApplicationSubmitter(),
          tokenId.getResource(), System.currentTimeMillis() - 1,
          containerTokenSecreteManager.getCurrentKey().getKeyId(),
          ResourceManager.clusterTimeStamp);
    final byte[] passowrd =
        containerTokenSecreteManager.createPassword(
            newTokenId);
    // Create a valid token by using the key from the RM.
    Token<ContainerTokenIdentifier> token =
        new Token<ContainerTokenIdentifier>(newTokenId.getBytes(), passowrd,
          new Text(containerToken.getKind()), new Text(
            containerToken.getService()));

    unauthorizedUser.addToken(token);
    unauthorizedUser.doAs(new PrivilegedAction<Void>() {
      @Override
      public Void run() {
        ContainerManagementProtocol client = (ContainerManagementProtocol) yarnRPC.getProxy(
            ContainerManagementProtocol.class, NetUtils
                .createSocketAddr(allocatedContainer.getNodeId().toString()),
            conf);

        LOG.info("Going to contact NM with expired token");
        ContainerLaunchContext context = createContainerLaunchContextForTest(newTokenId);
        StartContainerRequest request =
            Records.newRecord(StartContainerRequest.class);
        request.setContainerLaunchContext(context);
        allocatedContainer.setContainerToken(BuilderUtils.newContainerToken(
            allocatedContainer.getNodeId(), passowrd, newTokenId));
        request.setContainerToken(allocatedContainer.getContainerToken());

        //Calling startContainer with an expired token.
        try {
          client.startContainer(request);
          fail("Connection initiation with expired "
              + "token is expected to fail.");
        } catch (Throwable t) {
          LOG.info("Got exception : ", t);
          Assert.assertTrue(t.getMessage().contains(
                  "This token is expired. current time is"));
        }

        // Try stopping a container - should not get an expiry error.
        StopContainerRequest stopRequest = Records.newRecord(StopContainerRequest.class);
        stopRequest.setContainerId(newTokenId.getContainerID());
        try {
          client.stopContainer(stopRequest);
        } catch (Throwable t) {
          fail("Stop Container call should have succeeded");
        }
        
        return null;
      }
    });
    /////////// End of testing calls with expired tokens

    KillApplicationRequest request = Records
        .newRecord(KillApplicationRequest.class);
    request.setApplicationId(appID);
    resourceManager.getClientRMService().forceKillApplication(request);
  }
  
  private ApplicationMasterProtocol submitAndRegisterApplication(
      ResourceManager resourceManager, final YarnRPC yarnRPC,
      ApplicationId appID) throws IOException,
      UnsupportedFileSystemException, YarnException,
      InterruptedException {

    // Use ping to simulate sleep on Windows.
    List<String> cmd = Shell.WINDOWS ?
      Arrays.asList("ping", "-n", "100", "127.0.0.1", ">nul") :
      Arrays.asList("sleep", "100");

    ContainerLaunchContext amContainer =
        BuilderUtils.newContainerLaunchContext(
            Collections.<String, LocalResource> emptyMap(),
            new HashMap<String, String>(), cmd,
            new HashMap<String, ByteBuffer>(), null,
            new HashMap<ApplicationAccessType, String>());

    ApplicationSubmissionContext appSubmissionContext = recordFactory
        .newRecordInstance(ApplicationSubmissionContext.class);
    appSubmissionContext.setApplicationId(appID);
    appSubmissionContext.setAMContainerSpec(amContainer);
    appSubmissionContext.setResource(BuilderUtils.newResource(1024, 1));

    SubmitApplicationRequest submitRequest = recordFactory
        .newRecordInstance(SubmitApplicationRequest.class);
    submitRequest.setApplicationSubmissionContext(appSubmissionContext);
    resourceManager.getClientRMService().submitApplication(submitRequest);

    // Wait till container gets allocated for AM
    int waitCounter = 0;
    RMApp app = resourceManager.getRMContext().getRMApps().get(appID);
    RMAppAttempt appAttempt = app == null ? null : app.getCurrentAppAttempt();
    RMAppAttemptState state = appAttempt == null ? null : appAttempt
        .getAppAttemptState();
    while ((app == null || appAttempt == null || state == null || !state
        .equals(RMAppAttemptState.LAUNCHED))
        && waitCounter++ != 20) {
      LOG.info("Waiting for applicationAttempt to be created.. ");
      Thread.sleep(1000);
      app = resourceManager.getRMContext().getRMApps().get(appID);
      appAttempt = app == null ? null : app.getCurrentAppAttempt();
      state = appAttempt == null ? null : appAttempt.getAppAttemptState();
    }
    Assert.assertNotNull(app);
    Assert.assertNotNull(appAttempt);
    Assert.assertNotNull(state);
    Assert.assertEquals(RMAppAttemptState.LAUNCHED, state);

    UserGroupInformation currentUser = UserGroupInformation.createRemoteUser(
                                       appAttempt.getAppAttemptId().toString());

    // Ask for a container from the RM
    final InetSocketAddress schedulerAddr =
        resourceManager.getApplicationMasterService().getBindAddress();
    if (UserGroupInformation.isSecurityEnabled()) {
      AMRMTokenIdentifier appTokenIdentifier = new AMRMTokenIdentifier(
          appAttempt.getAppAttemptId());
      AMRMTokenSecretManager appTokenSecretManager =
          new AMRMTokenSecretManager(conf);
      appTokenSecretManager.setMasterKey(resourceManager
        .getAMRMTokenSecretManager().getMasterKey());
      Token<AMRMTokenIdentifier> appToken =
          new Token<AMRMTokenIdentifier>(appTokenIdentifier,
            appTokenSecretManager);
      SecurityUtil.setTokenService(appToken, schedulerAddr);
      currentUser.addToken(appToken);
    }
    
    ApplicationMasterProtocol scheduler = currentUser
        .doAs(new PrivilegedAction<ApplicationMasterProtocol>() {
          @Override
          public ApplicationMasterProtocol run() {
            return (ApplicationMasterProtocol) yarnRPC.getProxy(ApplicationMasterProtocol.class,
                schedulerAddr, conf);
          }
        });

    // Register the appMaster
    RegisterApplicationMasterRequest request = recordFactory
        .newRecordInstance(RegisterApplicationMasterRequest.class);
    request.setApplicationAttemptId(resourceManager.getRMContext()
        .getRMApps().get(appID).getCurrentAppAttempt().getAppAttemptId());
    scheduler.registerApplicationMaster(request);
    return scheduler;
  }

  private Container requestAndGetContainer(ApplicationMasterProtocol scheduler,
      ApplicationId appID) throws YarnException, InterruptedException,
      IOException {

    // Request a container allocation.
    List<ResourceRequest> ask = new ArrayList<ResourceRequest>();
    ask.add(BuilderUtils.newResourceRequest(BuilderUtils.newPriority(0),
        ResourceRequest.ANY, BuilderUtils.newResource(1024, 1), 1));

    AllocateRequest allocateRequest = AllocateRequest.newInstance(
        BuilderUtils.newApplicationAttemptId(appID, 1), 0, 0F, ask,
        new ArrayList<ContainerId>(), null);
    List<Container> allocatedContainers = scheduler.allocate(allocateRequest)
        .getAllocatedContainers();

    // Modify ask to request no more.
    allocateRequest.setAskList(new ArrayList<ResourceRequest>());

    int waitCounter = 0;
    while ((allocatedContainers == null || allocatedContainers.size() == 0)
        && waitCounter++ != 20) {
      LOG.info("Waiting for container to be allocated..");
      Thread.sleep(1000);
      allocateRequest.setResponseId(allocateRequest.getResponseId() + 1);
      allocatedContainers = scheduler.allocate(allocateRequest)
          .getAllocatedContainers();
    }

    Assert.assertNotNull("Container is not allocted!", allocatedContainers);
    Assert.assertEquals("Didn't get one container!", 1, allocatedContainers
        .size());

    return allocatedContainers.get(0);
  }

  private ContainerLaunchContext createContainerLaunchContextForTest(
      ContainerTokenIdentifier tokenId) {
    ContainerLaunchContext context =
        BuilderUtils.newContainerLaunchContext(
            new HashMap<String, LocalResource>(),
            new HashMap<String, String>(), new ArrayList<String>(),
            new HashMap<String, ByteBuffer>(), null,
            new HashMap<ApplicationAccessType, String>());
    return context;
  }
}
