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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
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
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest;
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
import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ApplicationTokenIdentifier;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.security.ApplicationTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestContainerManagerSecurity {

  static Log LOG = LogFactory.getLog(TestContainerManagerSecurity.class);
  static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);
  private static FileContext localFS = null;
  private static final File localDir = new File("target",
      TestContainerManagerSecurity.class.getName() + "-localDir")
      .getAbsoluteFile();
  private static MiniYARNCluster yarnCluster;

  static final Configuration conf = new Configuration();

  @BeforeClass
  public static void setup() throws AccessControlException,
      FileNotFoundException, UnsupportedFileSystemException, IOException {
    localFS = FileContext.getLocalFSFileContext();
    localFS.delete(new Path(localDir.getAbsolutePath()), true);
    localDir.mkdir();

    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    // Set AM expiry interval to be very long.
    conf.setLong(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, 100000L);
    UserGroupInformation.setConfiguration(conf);
    yarnCluster = new MiniYARNCluster(TestContainerManagerSecurity.class
        .getName(), 1, 1, 1);
    yarnCluster.init(conf);
    yarnCluster.start();
  }

  @AfterClass
  public static void teardown() {
    yarnCluster.stop();
  }

  @Test
  public void testAuthenticatedUser() throws IOException,
      InterruptedException {

    LOG.info("Running test for authenticated user");

    ResourceManager resourceManager = yarnCluster.getResourceManager();

    final YarnRPC yarnRPC = YarnRPC.create(conf);

    // Submit an application
    ApplicationId appID = resourceManager.getClientRMService()
        .getNewApplication(Records.newRecord(GetNewApplicationRequest.class))
        .getApplicationId();
    AMRMProtocol scheduler = submitAndRegisterApplication(resourceManager,
        yarnRPC, appID);

    // Now request a container.
    final Container allocatedContainer = requestAndGetContainer(scheduler,
        appID);

    // Now talk to the NM for launching the container.
    final ContainerId containerID = allocatedContainer.getId();
    UserGroupInformation authenticatedUser = UserGroupInformation
        .createRemoteUser(containerID.toString());
    ContainerToken containerToken = allocatedContainer.getContainerToken();
    Token<ContainerTokenIdentifier> token = new Token<ContainerTokenIdentifier>(
        containerToken.getIdentifier().array(), containerToken.getPassword()
            .array(), new Text(containerToken.getKind()), new Text(
            containerToken.getService()));
    authenticatedUser.addToken(token);
    authenticatedUser.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        ContainerManager client = (ContainerManager) yarnRPC.getProxy(
            ContainerManager.class, NetUtils
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

  @Test
  public void testMaliceUser() throws IOException, InterruptedException {

    LOG.info("Running test for malice user");

    ResourceManager resourceManager = yarnCluster.getResourceManager();

    final YarnRPC yarnRPC = YarnRPC.create(conf);

    // Submit an application
    ApplicationId appID = resourceManager.getClientRMService()
        .getNewApplication(Records.newRecord(GetNewApplicationRequest.class))
        .getApplicationId();
    AMRMProtocol scheduler = submitAndRegisterApplication(resourceManager,
        yarnRPC, appID);

    // Now request a container.
    final Container allocatedContainer = requestAndGetContainer(scheduler,
        appID);

    // Now talk to the NM for launching the container with modified resource
    final ContainerId containerID = allocatedContainer.getId();
    UserGroupInformation maliceUser = UserGroupInformation
        .createRemoteUser(containerID.toString());

    ContainerToken containerToken = allocatedContainer.getContainerToken();
    byte[] identifierBytes = containerToken.getIdentifier().array();

    DataInputBuffer di = new DataInputBuffer();
    di.reset(identifierBytes, identifierBytes.length);

    ContainerTokenIdentifier dummyIdentifier = new ContainerTokenIdentifier();
    dummyIdentifier.readFields(di);

    // Malice user modifies the resource amount
    Resource modifiedResource = BuilderUtils.newResource(2048, 1);
    ContainerTokenIdentifier modifiedIdentifier =
        new ContainerTokenIdentifier(dummyIdentifier.getContainerID(),
          dummyIdentifier.getNmHostAddress(), "testUser", modifiedResource,
          Long.MAX_VALUE, dummyIdentifier.getMasterKeyId());
    Token<ContainerTokenIdentifier> modifiedToken = new Token<ContainerTokenIdentifier>(
        modifiedIdentifier.getBytes(), containerToken.getPassword().array(),
        new Text(containerToken.getKind()), new Text(containerToken
            .getService()));
    maliceUser.addToken(modifiedToken);
    maliceUser.doAs(new PrivilegedAction<Void>() {
      @Override
      public Void run() {
        ContainerManager client = (ContainerManager) yarnRPC.getProxy(
            ContainerManager.class, NetUtils
                .createSocketAddr(allocatedContainer.getNodeId().toString()),
            conf);

        LOG.info("Going to contact NM:  ilLegal request");
        GetContainerStatusRequest request = recordFactory
            .newRecordInstance(GetContainerStatusRequest.class);
        request.setContainerId(containerID);
        try {
          client.getContainerStatus(request);
          fail("Connection initiation with illegally modified "
              + "tokens is expected to fail.");
        } catch (YarnRemoteException e) {
          LOG.error("Got exception", e);
          fail("Cannot get a YARN remote exception as "
              + "it will indicate RPC success");
        } catch (Exception e) {
          Assert.assertEquals(
            java.lang.reflect.UndeclaredThrowableException.class
              .getCanonicalName(), e.getClass().getCanonicalName());
          Assert.assertTrue(e
            .getCause()
            .getMessage()
            .contains(
              "DIGEST-MD5: digest response format violation. "
                  + "Mismatched response."));
        }
        return null;
      }
    });

    KillApplicationRequest request = Records
        .newRecord(KillApplicationRequest.class);
    request.setApplicationId(appID);
    resourceManager.getClientRMService().forceKillApplication(request);
  }

  @Test
  public void testUnauthorizedUser() throws IOException, InterruptedException {

    LOG.info("\n\nRunning test for malice user");

    ResourceManager resourceManager = yarnCluster.getResourceManager();

    final YarnRPC yarnRPC = YarnRPC.create(conf);

    // Submit an application
    final ApplicationId appID = resourceManager.getClientRMService()
        .getNewApplication(Records.newRecord(GetNewApplicationRequest.class))
        .getApplicationId();
    AMRMProtocol scheduler = submitAndRegisterApplication(resourceManager,
        yarnRPC, appID);

    // Now request a container.
    final Container allocatedContainer = requestAndGetContainer(scheduler,
        appID);

    // Now talk to the NM for launching the container with modified containerID
    final ContainerId containerID = allocatedContainer.getId();

    /////////// Test calls with illegal containerIDs and illegal Resources
    UserGroupInformation unauthorizedUser = UserGroupInformation
        .createRemoteUser(containerID.toString());
    ContainerToken containerToken = allocatedContainer.getContainerToken();

    byte[] identifierBytes = containerToken.getIdentifier().array();
    DataInputBuffer di = new DataInputBuffer();
    di.reset(identifierBytes, identifierBytes.length);
    final ContainerTokenIdentifier tokenId = new ContainerTokenIdentifier();
    tokenId.readFields(di);

    Token<ContainerTokenIdentifier> token = new Token<ContainerTokenIdentifier>(
        identifierBytes, containerToken.getPassword().array(), new Text(
            containerToken.getKind()), new Text(containerToken.getService()));

    unauthorizedUser.addToken(token);
    ContainerManager client =
        unauthorizedUser.doAs(new PrivilegedAction<ContainerManager>() {
      @Override
      public ContainerManager run() {
        ContainerManager client = (ContainerManager) yarnRPC.getProxy(
            ContainerManager.class, NetUtils
                .createSocketAddr(allocatedContainer.getNodeId().toString()),
            conf);

        LOG.info("Going to contact NM:  unauthorized request");

        callWithIllegalContainerID(client, tokenId);
        callWithIllegalResource(client, tokenId);
        callWithIllegalUserName(client, tokenId);

        return client;
      }
    });
    
    // ///////// End of testing for illegal containerIDs, illegal Resources and
    // illegal users

    /////////// Test calls with expired tokens
    RPC.stopProxy(client);
    unauthorizedUser = UserGroupInformation
        .createRemoteUser(containerID.toString());

    RMContainerTokenSecretManager containerTokenSecreteManager = 
      resourceManager.getRMContainerTokenSecretManager(); 
    final ContainerTokenIdentifier newTokenId =
        new ContainerTokenIdentifier(tokenId.getContainerID(),
          tokenId.getNmHostAddress(), "testUser", tokenId.getResource(),
          System.currentTimeMillis() - 1, 
          containerTokenSecreteManager.getCurrentKey().getKeyId());
    byte[] passowrd =
        containerTokenSecreteManager.createPassword(
            newTokenId);
    // Create a valid token by using the key from the RM.
    token = new Token<ContainerTokenIdentifier>(
        newTokenId.getBytes(), passowrd, new Text(
            containerToken.getKind()), new Text(containerToken.getService()));

    unauthorizedUser.addToken(token);
    unauthorizedUser.doAs(new PrivilegedAction<Void>() {
      @Override
      public Void run() {
        ContainerManager client = (ContainerManager) yarnRPC.getProxy(
            ContainerManager.class, NetUtils
                .createSocketAddr(allocatedContainer.getNodeId().toString()),
            conf);

        LOG.info("Going to contact NM with expired token");
        ContainerLaunchContext context = createContainerLaunchContextForTest(newTokenId);
        StartContainerRequest request = Records.newRecord(StartContainerRequest.class);
        request.setContainerLaunchContext(context);

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
  
  private AMRMProtocol submitAndRegisterApplication(
      ResourceManager resourceManager, final YarnRPC yarnRPC,
      ApplicationId appID) throws IOException,
      UnsupportedFileSystemException, YarnRemoteException,
      InterruptedException {

    ContainerLaunchContext amContainer = BuilderUtils
        .newContainerLaunchContext(null, "testUser", BuilderUtils
            .newResource(1024, 1), Collections.<String, LocalResource>emptyMap(),
            new HashMap<String, String>(), Arrays.asList("sleep", "100"),
            new HashMap<String, ByteBuffer>(), null,
            new HashMap<ApplicationAccessType, String>());

    ApplicationSubmissionContext appSubmissionContext = recordFactory
        .newRecordInstance(ApplicationSubmissionContext.class);
    appSubmissionContext.setApplicationId(appID);
    appSubmissionContext.setUser("testUser");
    appSubmissionContext.setAMContainerSpec(amContainer);

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
    ApplicationTokenIdentifier appTokenIdentifier = new ApplicationTokenIdentifier(
        appAttempt.getAppAttemptId());
    ApplicationTokenSecretManager appTokenSecretManager =
        new ApplicationTokenSecretManager(conf);
    appTokenSecretManager.setMasterKey(resourceManager
      .getApplicationTokenSecretManager().getMasterKey());
    Token<ApplicationTokenIdentifier> appToken =
        new Token<ApplicationTokenIdentifier>(appTokenIdentifier,
          appTokenSecretManager);
    SecurityUtil.setTokenService(appToken, schedulerAddr);
    currentUser.addToken(appToken);
    
    AMRMProtocol scheduler = currentUser
        .doAs(new PrivilegedAction<AMRMProtocol>() {
          @Override
          public AMRMProtocol run() {
            return (AMRMProtocol) yarnRPC.getProxy(AMRMProtocol.class,
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

  private Container requestAndGetContainer(AMRMProtocol scheduler,
      ApplicationId appID) throws YarnRemoteException, InterruptedException {

    // Request a container allocation.
    List<ResourceRequest> ask = new ArrayList<ResourceRequest>();
    ask.add(BuilderUtils.newResourceRequest(BuilderUtils.newPriority(0), "*",
        BuilderUtils.newResource(1024, 1), 1));

    AllocateRequest allocateRequest = BuilderUtils.newAllocateRequest(
        BuilderUtils.newApplicationAttemptId(appID, 1), 0, 0F, ask,
        new ArrayList<ContainerId>());
    List<Container> allocatedContainers = scheduler.allocate(allocateRequest)
        .getAMResponse().getAllocatedContainers();

    // Modify ask to request no more.
    allocateRequest.clearAsks();

    int waitCounter = 0;
    while ((allocatedContainers == null || allocatedContainers.size() == 0)
        && waitCounter++ != 20) {
      LOG.info("Waiting for container to be allocated..");
      Thread.sleep(1000);
      allocateRequest.setResponseId(allocateRequest.getResponseId() + 1);
      allocatedContainers = scheduler.allocate(allocateRequest)
          .getAMResponse().getAllocatedContainers();
    }

    Assert.assertNotNull("Container is not allocted!", allocatedContainers);
    Assert.assertEquals("Didn't get one container!", 1, allocatedContainers
        .size());

    return allocatedContainers.get(0);
  }

  void callWithIllegalContainerID(ContainerManager client,
      ContainerTokenIdentifier tokenId) {
    GetContainerStatusRequest request = recordFactory
        .newRecordInstance(GetContainerStatusRequest.class);
    ContainerId newContainerId = BuilderUtils.newContainerId(BuilderUtils
        .newApplicationAttemptId(tokenId.getContainerID()
            .getApplicationAttemptId().getApplicationId(), 1), 42);
    request.setContainerId(newContainerId); // Authenticated but
                                            // unauthorized.
    try {
      client.getContainerStatus(request);
      fail("Connection initiation with unauthorized "
          + "access is expected to fail.");
    } catch (YarnRemoteException e) {
      LOG.info("Got exception : ", e);
      Assert.assertTrue(e.getMessage().contains(
          "Unauthorized request to start container. "
              + "\nExpected containerId: " + tokenId.getContainerID()
              + " Found: " + newContainerId.toString()));
    }
  }

  void callWithIllegalResource(ContainerManager client,
      ContainerTokenIdentifier tokenId) {
    StartContainerRequest request = recordFactory
        .newRecordInstance(StartContainerRequest.class);
    // Authenticated but unauthorized, due to wrong resource
    ContainerLaunchContext context =
        createContainerLaunchContextForTest(tokenId);
    context.getResource().setMemory(2048); // Set a different resource size.
    request.setContainerLaunchContext(context);
    try {
      client.startContainer(request);
      fail("Connection initiation with unauthorized "
          + "access is expected to fail.");
    } catch (YarnRemoteException e) {
      LOG.info("Got exception : ", e);
      Assert.assertTrue(e.getMessage().contains(
          "Unauthorized request to start container. "));
      Assert.assertTrue(e.getMessage().contains(
          "\nExpected resource " + tokenId.getResource().toString()
              + " but found " + context.getResource().toString()));
    }
  }

  void callWithIllegalUserName(ContainerManager client,
      ContainerTokenIdentifier tokenId) {
    StartContainerRequest request = recordFactory
        .newRecordInstance(StartContainerRequest.class);
    // Authenticated but unauthorized, due to wrong resource
    ContainerLaunchContext context =
        createContainerLaunchContextForTest(tokenId);
    context.setUser("Saruman"); // Set a different user-name.
    request.setContainerLaunchContext(context);
    try {
      client.startContainer(request);
      fail("Connection initiation with unauthorized "
          + "access is expected to fail.");
    } catch (YarnRemoteException e) {
      LOG.info("Got exception : ", e);
      Assert.assertTrue(e.getMessage().contains(
          "Unauthorized request to start container. "));
      Assert.assertTrue(e.getMessage().contains(
        "Expected user-name " + tokenId.getApplicationSubmitter()
            + " but found " + context.getUser()));
    }
  }

  private ContainerLaunchContext createContainerLaunchContextForTest(
      ContainerTokenIdentifier tokenId) {
    ContainerLaunchContext context =
        BuilderUtils.newContainerLaunchContext(tokenId.getContainerID(),
            "testUser",
            BuilderUtils.newResource(
                tokenId.getResource().getMemory(), 
                tokenId.getResource().getVirtualCores()),
            new HashMap<String, LocalResource>(),
            new HashMap<String, String>(), new ArrayList<String>(),
            new HashMap<String, ByteBuffer>(), null,
            new HashMap<ApplicationAccessType, String>());
    return context;
  }
}
