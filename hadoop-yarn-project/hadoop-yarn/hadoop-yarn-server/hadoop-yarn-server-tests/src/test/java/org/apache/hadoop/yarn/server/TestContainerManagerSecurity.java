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
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.minikdc.KerberosSecurityTestcase;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.SerializedException;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.client.NMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.security.NMTokenSecretManagerInNM;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.MockRMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.security.BaseNMTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@RunWith(Parameterized.class)
public class TestContainerManagerSecurity extends KerberosSecurityTestcase {

  static Logger LOG = LoggerFactory.getLogger(TestContainerManagerSecurity.class);
  static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);
  private static MiniYARNCluster yarnCluster;
  private static final File testRootDir = new File("target",
    TestContainerManagerSecurity.class.getName() + "-root");
  private static File httpSpnegoKeytabFile = new File(testRootDir,
    "httpSpnegoKeytabFile.keytab");
  private static String httpSpnegoPrincipal = "HTTP/localhost@EXAMPLE.COM";

  private Configuration conf;

  @Before
  public void setUp() throws Exception {
    testRootDir.mkdirs();
    httpSpnegoKeytabFile.deleteOnExit();
    getKdc().createPrincipal(httpSpnegoKeytabFile, httpSpnegoPrincipal);
    UserGroupInformation.setConfiguration(conf);

    yarnCluster =
        new MiniYARNCluster(TestContainerManagerSecurity.class.getName(), 1, 1,
            1);
    yarnCluster.init(conf);
    yarnCluster.start();
  }
 
  @After
  public void tearDown() {
    if (yarnCluster != null) {
      yarnCluster.stop();
      yarnCluster = null;
    }
    testRootDir.delete();
  }

  /*
   * Run two tests: one with no security ("simple") and one with "Secure"
   * The first parameter is just the test name to make it easier to debug
   * and to give details in say an IDE.  The second is the configuraiton
   * object to use.
   */
  @Parameters(name = "{0}")
  public static Collection<Object[]> configs() {
    Configuration configurationWithoutSecurity = new Configuration();
    configurationWithoutSecurity.set(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "simple");
    
    Configuration configurationWithSecurity = new Configuration();
    configurationWithSecurity.set(
      CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    configurationWithSecurity.set(
      YarnConfiguration.RM_WEBAPP_SPNEGO_USER_NAME_KEY, httpSpnegoPrincipal);
    configurationWithSecurity.set(
      YarnConfiguration.RM_WEBAPP_SPNEGO_KEYTAB_FILE_KEY,
      httpSpnegoKeytabFile.getAbsolutePath());
    configurationWithSecurity.set(
      YarnConfiguration.NM_WEBAPP_SPNEGO_USER_NAME_KEY, httpSpnegoPrincipal);
    configurationWithSecurity.set(
      YarnConfiguration.NM_WEBAPP_SPNEGO_KEYTAB_FILE_KEY,
      httpSpnegoKeytabFile.getAbsolutePath());

    return Arrays.asList(new Object[][] {
        {"Simple", configurationWithoutSecurity},
        {"Secure", configurationWithSecurity}});
  }
  
  public TestContainerManagerSecurity(String name, Configuration conf) {
    LOG.info("RUNNING TEST " + name);
    conf.setLong(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, 100000L);
    this.conf = conf;
  }
  
  @Test
  public void testContainerManager() throws Exception {
      
      // TestNMTokens.
      testNMTokens(conf);
      
      // Testing for container token tampering
      testContainerToken(conf);
      
      // Testing for container token tampering with epoch
      testContainerTokenWithEpoch(conf);

  }

  /**
   * Run a series of tests using different NMTokens.  A configuration is
   * provided for managing creating of the tokens and rpc.
   */
  private void testNMTokens(Configuration testConf) throws Exception {
    NMTokenSecretManagerInRM nmTokenSecretManagerRM =
        yarnCluster.getResourceManager().getRMContext()
          .getNMTokenSecretManager();
    NMTokenSecretManagerInNM nmTokenSecretManagerNM =
        yarnCluster.getNodeManager(0).getNMContext().getNMTokenSecretManager();
    RMContainerTokenSecretManager containerTokenSecretManager =
        yarnCluster.getResourceManager().getRMContext().
            getContainerTokenSecretManager();
    
    NodeManager nm = yarnCluster.getNodeManager(0);
    
    waitForNMToReceiveNMTokenKey(nmTokenSecretManagerNM);
    
    // Both id should be equal.
    Assert.assertEquals(nmTokenSecretManagerNM.getCurrentKey().getKeyId(),
        nmTokenSecretManagerRM.getCurrentKey().getKeyId());
    
    /*
     * Below cases should be tested.
     * 1) If Invalid NMToken is used then it should be rejected.
     * 2) If valid NMToken but belonging to another Node is used then that
     * too should be rejected.
     * 3) NMToken for say appAttempt-1 is used for starting/stopping/retrieving
     * status for container with containerId for say appAttempt-2 should
     * be rejected.
     * 4) After start container call is successful nmtoken should have been
     * saved in NMTokenSecretManagerInNM.
     * 5) If start container call was successful (no matter if container is
     * still running or not), appAttempt->NMToken should be present in
     * NMTokenSecretManagerInNM's cache. Any future getContainerStatus call
     * for containerId belonging to that application attempt using
     * applicationAttempt's older nmToken should not get any invalid
     * nmToken error. (This can be best tested if we roll over NMToken
     * master key twice).
     */
    YarnRPC rpc = YarnRPC.create(testConf);
    String user = "test";
    Resource r = Resource.newInstance(1024, 1);

    ApplicationId appId = ApplicationId.newInstance(1, 1);
    MockRMApp m = new MockRMApp(appId.getId(), appId.getClusterTimestamp(),
        RMAppState.NEW);
    yarnCluster.getResourceManager().getRMContext().getRMApps().put(appId, m);
    ApplicationAttemptId validAppAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    
    ContainerId validContainerId =
        ContainerId.newContainerId(validAppAttemptId, 0);
    
    NodeId validNode = yarnCluster.getNodeManager(0).getNMContext().getNodeId();
    NodeId invalidNode = NodeId.newInstance("InvalidHost", 1234);

    
    org.apache.hadoop.yarn.api.records.Token validNMToken =
        nmTokenSecretManagerRM.createNMToken(validAppAttemptId, validNode, user);
    
    org.apache.hadoop.yarn.api.records.Token validContainerToken =
        containerTokenSecretManager.createContainerToken(validContainerId,
            0, validNode, user, r, Priority.newInstance(10), 1234);
    ContainerTokenIdentifier identifier =
        BuilderUtils.newContainerTokenIdentifier(validContainerToken);
    Assert.assertEquals(Priority.newInstance(10), identifier.getPriority());
    Assert.assertEquals(1234, identifier.getCreationTime());
    
    StringBuilder sb;
    // testInvalidNMToken ... creating NMToken using different secret manager.
    
    NMTokenSecretManagerInRM tempManager = new NMTokenSecretManagerInRM(testConf);
    tempManager.rollMasterKey();
    do {
      tempManager.rollMasterKey();
      tempManager.activateNextMasterKey();
      // Making sure key id is different.
    } while (tempManager.getCurrentKey().getKeyId() == nmTokenSecretManagerRM
        .getCurrentKey().getKeyId());
    
    // Testing that NM rejects the requests when we don't send any token.
    if (UserGroupInformation.isSecurityEnabled()) {
      sb = new StringBuilder("Client cannot authenticate via:[TOKEN]");
    } else {
      sb =
          new StringBuilder(
              "SIMPLE authentication is not enabled.  Available:[TOKEN]");
    }
    String errorMsg = testStartContainer(rpc, validAppAttemptId, validNode,
        validContainerToken, null, true);
    Assert.assertTrue("In calling " + validNode + " exception was '"
        + errorMsg + "' but doesn't contain '"
        + sb.toString() + "'", errorMsg.contains(sb.toString()));
    
    org.apache.hadoop.yarn.api.records.Token invalidNMToken =
        tempManager.createNMToken(validAppAttemptId, validNode, user);
    sb = new StringBuilder("Given NMToken for application : ");
    sb.append(validAppAttemptId.toString())
      .append(" seems to have been generated illegally.");
    Assert.assertTrue(sb.toString().contains(
        testStartContainer(rpc, validAppAttemptId, validNode,
            validContainerToken, invalidNMToken, true)));
    
    // valid NMToken but belonging to other node
    invalidNMToken =
        nmTokenSecretManagerRM.createNMToken(validAppAttemptId, invalidNode,
            user);
    sb = new StringBuilder("Given NMToken for application : ");
    sb.append(validAppAttemptId)
      .append(" is not valid for current node manager.expected : ")
      .append(validNode.toString())
      .append(" found : ").append(invalidNode.toString());
    Assert.assertTrue(sb.toString().contains(
        testStartContainer(rpc, validAppAttemptId, validNode,
            validContainerToken, invalidNMToken, true)));
    
    // using correct tokens. nmtoken for app attempt should get saved.
    testConf.setInt(YarnConfiguration.RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS,
        4 * 60 * 1000);
    validContainerToken =
        containerTokenSecretManager.createContainerToken(validContainerId,
            0, validNode, user, r, Priority.newInstance(0), 0);
    Assert.assertTrue(testStartContainer(rpc, validAppAttemptId, validNode,
      validContainerToken, validNMToken, false).isEmpty());
    Assert.assertTrue(nmTokenSecretManagerNM
        .isAppAttemptNMTokenKeyPresent(validAppAttemptId));
    
    // using a new compatible version nmtoken, expect container can be started 
    // successfully.
    ApplicationAttemptId validAppAttemptId2 =
        ApplicationAttemptId.newInstance(appId, 2);
        
    ContainerId validContainerId2 =
        ContainerId.newContainerId(validAppAttemptId2, 0);

    org.apache.hadoop.yarn.api.records.Token validContainerToken2 =
        containerTokenSecretManager.createContainerToken(validContainerId2,
            0, validNode, user, r, Priority.newInstance(0), 0);
    
    org.apache.hadoop.yarn.api.records.Token validNMToken2 =
        nmTokenSecretManagerRM.createNMToken(validAppAttemptId2, validNode, user);
    // First, get a new NMTokenIdentifier.
    NMTokenIdentifier newIdentifier = new NMTokenIdentifier();
    byte[] tokenIdentifierContent = validNMToken2.getIdentifier().array();
    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(tokenIdentifierContent, tokenIdentifierContent.length);
    newIdentifier.readFields(dib);
    
    // Then, generate a new version NMTokenIdentifier (NMTokenIdentifierNewForTest)
    // with additional field of message.
    NMTokenIdentifierNewForTest newVersionIdentifier = 
        new NMTokenIdentifierNewForTest(newIdentifier, "message");
    
    // check new version NMTokenIdentifier has correct info.
    Assert.assertEquals("The ApplicationAttemptId is changed after set to " +
        "newVersionIdentifier", validAppAttemptId2.getAttemptId(), 
        newVersionIdentifier.getApplicationAttemptId().getAttemptId()
    );
    
    Assert.assertEquals("The message is changed after set to newVersionIdentifier",
        "message", newVersionIdentifier.getMessage());
    
    Assert.assertEquals("The NodeId is changed after set to newVersionIdentifier", 
        validNode, newVersionIdentifier.getNodeId());
    
    // create new Token based on new version NMTokenIdentifier.
    org.apache.hadoop.yarn.api.records.Token newVersionedNMToken =
        BaseNMTokenSecretManager.newInstance(
            nmTokenSecretManagerRM.retrievePassword(newVersionIdentifier), 
            newVersionIdentifier);
    
    // Verify startContainer is successful and no exception is thrown.
    Assert.assertTrue(testStartContainer(rpc, validAppAttemptId2, validNode,
        validContainerToken2, newVersionedNMToken, false).isEmpty());
    Assert.assertTrue(nmTokenSecretManagerNM
        .isAppAttemptNMTokenKeyPresent(validAppAttemptId2));
    
    //Now lets wait till container finishes and is removed from node manager.
    waitForContainerToFinishOnNM(validContainerId);
    sb = new StringBuilder("Attempt to relaunch the same container with id ");
    sb.append(validContainerId);
    Assert.assertTrue(testStartContainer(rpc, validAppAttemptId, validNode,
        validContainerToken, validNMToken, true).contains(sb.toString()));
    
    // Container is removed from node manager's memory by this time.
    // trying to stop the container. It should not throw any exception.
    testStopContainer(rpc, validAppAttemptId, validNode, validContainerId,
        validNMToken, false);

    // Rolling over master key twice so that we can check whether older keys
    // are used for authentication.
    rollNMTokenMasterKey(nmTokenSecretManagerRM, nmTokenSecretManagerNM);
    // Key rolled over once.. rolling over again
    rollNMTokenMasterKey(nmTokenSecretManagerRM, nmTokenSecretManagerNM);
    
    // trying get container status. Now saved nmToken should be used for
    // authentication... It should complain saying container was recently
    // stopped.
    sb = new StringBuilder("Container ");
    sb.append(validContainerId)
        .append(" was recently stopped on node manager");
    Assert.assertTrue(testGetContainer(rpc, validAppAttemptId, validNode,
        validContainerId, validNMToken, true).contains(sb.toString()));

    // Now lets remove the container from nm-memory
    nm.getNodeStatusUpdater().clearFinishedContainersFromCache();
    
    // This should fail as container is removed from recently tracked finished
    // containers.
    sb = new StringBuilder("Container ")
        .append(validContainerId.toString())
        .append(" is not handled by this NodeManager");
    Assert.assertTrue(testGetContainer(rpc, validAppAttemptId, validNode,
        validContainerId, validNMToken, false).contains(sb.toString()));

    // using appAttempt-1 NMtoken for launching container for appAttempt-2
    // should succeed.
    ApplicationAttemptId attempt2 = ApplicationAttemptId.newInstance(appId, 2);
    Token attempt1NMToken =
        nmTokenSecretManagerRM
          .createNMToken(validAppAttemptId, validNode, user);
    org.apache.hadoop.yarn.api.records.Token newContainerToken =
        containerTokenSecretManager.createContainerToken(
          ContainerId.newContainerId(attempt2, 1), 0, validNode, user, r,
            Priority.newInstance(0), 0);
    Assert.assertTrue(testStartContainer(rpc, attempt2, validNode,
      newContainerToken, attempt1NMToken, false).isEmpty());
  }

  private void waitForContainerToFinishOnNM(ContainerId containerId)
      throws InterruptedException {
    Context nmContext = yarnCluster.getNodeManager(0).getNMContext();
    // Max time for container token to expire.
    final int timeout = 4 * 60 * 1000;

    // If the container is null, then it has already completed and been removed
    // from the Context by asynchronous calls.
    Container waitContainer = nmContext.getContainers().get(containerId);
    if (waitContainer != null) {
      try {
        LOG.info("Waiting for " + containerId + " to get to state " +
            ContainerState.COMPLETE);
        GenericTestUtils.waitFor(() -> ContainerState.COMPLETE.equals(
            waitContainer.cloneAndGetContainerStatus().getState()),
            500, timeout);
      } catch (TimeoutException te) {
        LOG.error("TimeoutException", te);
        fail("Was waiting for " + containerId + " to get to state " +
            ContainerState.COMPLETE + " but was in state " +
            waitContainer.cloneAndGetContainerStatus().getState() +
            " after the timeout");
      }
    }

    // Normally, Containers will be removed from NM context after they are
    // explicitly acked by RM. Now, manually remove it for testing.
    yarnCluster.getNodeManager(0).getNodeStatusUpdater()
      .addCompletedContainer(containerId);
    LOG.info("Removing container from NMContext, containerID = " + containerId);
    nmContext.getContainers().remove(containerId);
  }

  protected void waitForNMToReceiveNMTokenKey(
      NMTokenSecretManagerInNM nmTokenSecretManagerNM)
      throws InterruptedException {
    int attempt = 60;
    while (nmTokenSecretManagerNM.getNodeId() == null && attempt-- > 0) {
      Thread.sleep(2000);
    }
  }

  protected void rollNMTokenMasterKey(
      NMTokenSecretManagerInRM nmTokenSecretManagerRM,
      NMTokenSecretManagerInNM nmTokenSecretManagerNM) throws Exception {
    int oldKeyId = nmTokenSecretManagerRM.getCurrentKey().getKeyId();
    nmTokenSecretManagerRM.rollMasterKey();
    int interval = 40;
    while (nmTokenSecretManagerNM.getCurrentKey().getKeyId() == oldKeyId
        && interval-- > 0) {
      Thread.sleep(1000);
    }
    nmTokenSecretManagerRM.activateNextMasterKey();
    Assert.assertTrue((nmTokenSecretManagerNM.getCurrentKey().getKeyId()
        == nmTokenSecretManagerRM.getCurrentKey().getKeyId()));
  }
  
  private String testStopContainer(YarnRPC rpc,
      ApplicationAttemptId appAttemptId, NodeId nodeId,
      ContainerId containerId, Token nmToken, boolean isExceptionExpected) {
    try {
      stopContainer(rpc, nmToken,
          Arrays.asList(new ContainerId[] {containerId}), appAttemptId,
          nodeId);
      if (isExceptionExpected) {
        fail("Exception was expected!!");
      }
      return "";
    } catch (Exception e) {
      e.printStackTrace();
      return e.getMessage();
    }
  }

  private String testGetContainer(YarnRPC rpc,
      ApplicationAttemptId appAttemptId, NodeId nodeId,
      ContainerId containerId,
      org.apache.hadoop.yarn.api.records.Token nmToken,
      boolean isExceptionExpected) {
    try {
      getContainerStatus(rpc, nmToken, containerId, appAttemptId, nodeId,
          isExceptionExpected);
      if (isExceptionExpected) {
        fail("Exception was expected!!");
      }
      return "";
    } catch (Exception e) {
      e.printStackTrace();
      return e.getMessage();
    }
  }

  private String testStartContainer(YarnRPC rpc,
      ApplicationAttemptId appAttemptId, NodeId nodeId,
      org.apache.hadoop.yarn.api.records.Token containerToken,
      org.apache.hadoop.yarn.api.records.Token nmToken,
      boolean isExceptionExpected) {
    try {
      startContainer(rpc, nmToken, containerToken, nodeId,
          appAttemptId.toString());
      if (isExceptionExpected){
        fail("Exception was expected!!");        
      }
      return "";
    } catch (Exception e) {
      e.printStackTrace();
      return e.getMessage();
    }
  }
  
  private void stopContainer(YarnRPC rpc, Token nmToken,
      List<ContainerId> containerId, ApplicationAttemptId appAttemptId,
      NodeId nodeId) throws Exception {
    StopContainersRequest request =
        StopContainersRequest.newInstance(containerId);
    ContainerManagementProtocol proxy = null;
    try {
      proxy =
          getContainerManagementProtocolProxy(rpc, nmToken, nodeId,
              appAttemptId.toString());
      StopContainersResponse response = proxy.stopContainers(request);
      if (response.getFailedRequests() != null &&
          response.getFailedRequests().containsKey(containerId)) {
        parseAndThrowException(response.getFailedRequests().get(containerId)
            .deSerialize());
      }
    } catch (Exception e) {
      if (proxy != null) {
        rpc.stopProxy(proxy, conf);
      }
    }
  }
  
  private void
      getContainerStatus(YarnRPC rpc,
          org.apache.hadoop.yarn.api.records.Token nmToken,
          ContainerId containerId,
          ApplicationAttemptId appAttemptId, NodeId nodeId,
          boolean isExceptionExpected) throws Exception {
    List<ContainerId> containerIds = new ArrayList<ContainerId>();
    containerIds.add(containerId);
    GetContainerStatusesRequest request =
        GetContainerStatusesRequest.newInstance(containerIds);
    ContainerManagementProtocol proxy = null;
    try {
      proxy =
          getContainerManagementProtocolProxy(rpc, nmToken, nodeId,
              appAttemptId.toString());
      GetContainerStatusesResponse statuses
          = proxy.getContainerStatuses(request);
      if (statuses.getFailedRequests() != null
          && statuses.getFailedRequests().containsKey(containerId)) {
        parseAndThrowException(statuses.getFailedRequests().get(containerId)
          .deSerialize());
      }
    } finally {
      if (proxy != null) {
        rpc.stopProxy(proxy, conf);
      }
    }
  }
  
  private void startContainer(final YarnRPC rpc,
      org.apache.hadoop.yarn.api.records.Token nmToken,
      org.apache.hadoop.yarn.api.records.Token containerToken,
      NodeId nodeId, String user) throws Exception {

    ContainerLaunchContext context =
        Records.newRecord(ContainerLaunchContext.class);
    StartContainerRequest scRequest =
        StartContainerRequest.newInstance(context, containerToken);
    List<StartContainerRequest> list = new ArrayList<StartContainerRequest>();
    list.add(scRequest);
    StartContainersRequest allRequests =
        StartContainersRequest.newInstance(list);
    ContainerManagementProtocol proxy = null;
    try {
      proxy = getContainerManagementProtocolProxy(rpc, nmToken, nodeId, user);
      StartContainersResponse response = proxy.startContainers(allRequests);
      for(SerializedException ex : response.getFailedRequests().values()){
        parseAndThrowException(ex.deSerialize());
      }
    } finally {
      if (proxy != null) {
        rpc.stopProxy(proxy, conf);
      }
    }
  }

  private void parseAndThrowException(Throwable t) throws YarnException,
      IOException {
    if (t instanceof YarnException) {
      throw (YarnException) t;
    } else if (t instanceof InvalidToken) {
      throw (InvalidToken) t;
    } else {
      throw (IOException) t;
    }
  }

  protected ContainerManagementProtocol getContainerManagementProtocolProxy(
      final YarnRPC rpc, org.apache.hadoop.yarn.api.records.Token nmToken,
      NodeId nodeId, String user) {
    ContainerManagementProtocol proxy;
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
    final InetSocketAddress addr =
        new InetSocketAddress(nodeId.getHost(), nodeId.getPort());
    if (nmToken != null) {
      ugi.addToken(ConverterUtils.convertFromYarn(nmToken, addr));      
    }
    proxy =
        NMProxy.createNMProxy(conf, ContainerManagementProtocol.class, ugi,
          rpc, addr);
    return proxy;
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
  private void testContainerToken(Configuration conf) throws IOException,
      InterruptedException, YarnException {

    LOG.info("Running test for malice user");
    /*
     * We need to check for containerToken (authorization).
     * Here we will be assuming that we have valid NMToken  
     * 1) ContainerToken used is expired.
     * 2) ContainerToken is tampered (resource is modified).
     */
    NMTokenSecretManagerInRM nmTokenSecretManagerInRM =
        yarnCluster.getResourceManager().getRMContext()
          .getNMTokenSecretManager();
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 0);
    ContainerId cId = ContainerId.newContainerId(appAttemptId, 0);
    NodeManager nm = yarnCluster.getNodeManager(0);
    NMTokenSecretManagerInNM nmTokenSecretManagerInNM =
        nm.getNMContext().getNMTokenSecretManager();
    String user = "test";
    
    waitForNMToReceiveNMTokenKey(nmTokenSecretManagerInNM);

    NodeId nodeId = nm.getNMContext().getNodeId();
    
    // Both id should be equal.
    Assert.assertEquals(nmTokenSecretManagerInNM.getCurrentKey().getKeyId(),
        nmTokenSecretManagerInRM.getCurrentKey().getKeyId());
    
    
    RMContainerTokenSecretManager containerTokenSecretManager =
        yarnCluster.getResourceManager().getRMContext().
            getContainerTokenSecretManager();
    
    Resource r = Resource.newInstance(1230, 2);
    
    Token containerToken = 
        containerTokenSecretManager.createContainerToken(
            cId, 0, nodeId, user, r, Priority.newInstance(0), 0);
    
    ContainerTokenIdentifier containerTokenIdentifier = 
        getContainerTokenIdentifierFromToken(containerToken);
    
    // Verify new compatible version ContainerTokenIdentifier
    // can work successfully.
    ContainerTokenIdentifierForTest newVersionTokenIdentifier = 
        new ContainerTokenIdentifierForTest(containerTokenIdentifier,
            "message");
    byte[] password = 
        containerTokenSecretManager.createPassword(newVersionTokenIdentifier);
    
    Token newContainerToken = BuilderUtils.newContainerToken(
        nodeId, password, newVersionTokenIdentifier);
    
    Token nmToken =
            nmTokenSecretManagerInRM.createNMToken(appAttemptId, nodeId, user);
    YarnRPC rpc = YarnRPC.create(conf);
    Assert.assertTrue(testStartContainer(rpc, appAttemptId, nodeId,
        newContainerToken, nmToken, false).isEmpty());
    
    // Creating a tampered Container Token
    RMContainerTokenSecretManager tamperedContainerTokenSecretManager =
        new RMContainerTokenSecretManager(conf);
    tamperedContainerTokenSecretManager.rollMasterKey();
    do {
      tamperedContainerTokenSecretManager.rollMasterKey();
      tamperedContainerTokenSecretManager.activateNextMasterKey();
    } while (containerTokenSecretManager.getCurrentKey().getKeyId()
        == tamperedContainerTokenSecretManager.getCurrentKey().getKeyId());
    
    ContainerId cId2 = ContainerId.newContainerId(appAttemptId, 1);
    // Creating modified containerToken
    Token containerToken2 =
        tamperedContainerTokenSecretManager.createContainerToken(cId2, 0,
            nodeId, user, r, Priority.newInstance(0), 0);
    
    StringBuilder sb = new StringBuilder("Given Container ");
    sb.append(cId2)
        .append(" seems to have an illegally generated token.");
    Assert.assertTrue(testStartContainer(rpc, appAttemptId, nodeId,
        containerToken2, nmToken, true).contains(sb.toString()));
  }

  private ContainerTokenIdentifier getContainerTokenIdentifierFromToken(
      Token containerToken) throws IOException {
    ContainerTokenIdentifier containerTokenIdentifier;
    containerTokenIdentifier = new ContainerTokenIdentifier();
    byte[] tokenIdentifierContent = containerToken.getIdentifier().array();
    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(tokenIdentifierContent, tokenIdentifierContent.length);
    containerTokenIdentifier.readFields(dib);
    return containerTokenIdentifier;
  }

  /**
   * This tests whether a containerId is serialized/deserialized with epoch.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws YarnException
   */
  private void testContainerTokenWithEpoch(Configuration conf)
      throws IOException, InterruptedException, YarnException {

    LOG.info("Running test for serializing/deserializing containerIds");

    NMTokenSecretManagerInRM nmTokenSecretManagerInRM =
        yarnCluster.getResourceManager().getRMContext()
            .getNMTokenSecretManager();
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 0);
    ContainerId cId = ContainerId.newContainerId(appAttemptId, (5L << 40) | 3L);
    NodeManager nm = yarnCluster.getNodeManager(0);
    NMTokenSecretManagerInNM nmTokenSecretManagerInNM =
        nm.getNMContext().getNMTokenSecretManager();
    String user = "test";

    waitForNMToReceiveNMTokenKey(nmTokenSecretManagerInNM);

    NodeId nodeId = nm.getNMContext().getNodeId();

    // Both id should be equal.
    Assert.assertEquals(nmTokenSecretManagerInNM.getCurrentKey().getKeyId(),
        nmTokenSecretManagerInRM.getCurrentKey().getKeyId());

    // Creating a normal Container Token
    RMContainerTokenSecretManager containerTokenSecretManager =
        yarnCluster.getResourceManager().getRMContext().
            getContainerTokenSecretManager();
    Resource r = Resource.newInstance(1230, 2);
    Token containerToken =
        containerTokenSecretManager.createContainerToken(cId, 0, nodeId, user,
            r, Priority.newInstance(0), 0);
    
    ContainerTokenIdentifier containerTokenIdentifier =
        new ContainerTokenIdentifier();
    byte[] tokenIdentifierContent = containerToken.getIdentifier().array();
    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(tokenIdentifierContent, tokenIdentifierContent.length);
    containerTokenIdentifier.readFields(dib);
    
    
    Assert.assertEquals(cId, containerTokenIdentifier.getContainerID());
    Assert.assertEquals(
        cId.toString(), containerTokenIdentifier.getContainerID().toString());

    Token nmToken =
        nmTokenSecretManagerInRM.createNMToken(appAttemptId, nodeId, user);

    YarnRPC rpc = YarnRPC.create(conf);
    testStartContainer(rpc, appAttemptId, nodeId, containerToken, nmToken,
        false);

    List<ContainerId> containerIds = new LinkedList<ContainerId>();
    containerIds.add(cId);
    ContainerManagementProtocol proxy
        = getContainerManagementProtocolProxy(rpc, nmToken, nodeId, user);
    GetContainerStatusesResponse res = proxy.getContainerStatuses(
        GetContainerStatusesRequest.newInstance(containerIds));
    Assert.assertNotNull(res.getContainerStatuses().get(0));
    Assert.assertEquals(
        cId, res.getContainerStatuses().get(0).getContainerId());
    Assert.assertEquals(cId.toString(),
        res.getContainerStatuses().get(0).getContainerId().toString());
  }
}
