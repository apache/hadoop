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
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.minikdc.KerberosSecurityTestcase;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
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
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.SerializedException;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.security.NMTokenSecretManagerInNM;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestContainerManagerSecurity extends KerberosSecurityTestcase {

  static Log LOG = LogFactory.getLog(TestContainerManagerSecurity.class);
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
  }
 
  @After
  public void tearDown() {
    testRootDir.delete();
  }

  @Parameters
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

    return Arrays.asList(new Object[][] { { configurationWithoutSecurity },
        { configurationWithSecurity } });
  }
  
  public TestContainerManagerSecurity(Configuration conf) {
    conf.setLong(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, 100000L);
    UserGroupInformation.setConfiguration(conf);
    this.conf = conf;
  }
  
  @Test (timeout = 1000000)
  public void testContainerManager() throws Exception {
    try {
      yarnCluster = new MiniYARNCluster(TestContainerManagerSecurity.class
          .getName(), 1, 1, 1);
      yarnCluster.init(conf);
      yarnCluster.start();
      
      // TestNMTokens.
      testNMTokens(conf);
      
      // Testing for container token tampering
      testContainerToken(conf);
      
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    } finally {
      if (yarnCluster != null) {
        yarnCluster.stop();
        yarnCluster = null;
      }
    }
  }
  
  private void testNMTokens(Configuration conf) throws Exception {
    NMTokenSecretManagerInRM nmTokenSecretManagerRM =
        yarnCluster.getResourceManager().getRMContext()
          .getNMTokenSecretManager();
    NMTokenSecretManagerInNM nmTokenSecretManagerNM =
        yarnCluster.getNodeManager(0).getNMContext().getNMTokenSecretManager();
    RMContainerTokenSecretManager containerTokenSecretManager =
        yarnCluster.getResourceManager().getRMContext().
            getContainerTokenSecretManager();
    
    NodeManager nm = yarnCluster.getNodeManager(0);
    
    waitForNMToReceiveNMTokenKey(nmTokenSecretManagerNM, nm);
    
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
    YarnRPC rpc = YarnRPC.create(conf);
    String user = "test";
    Resource r = Resource.newInstance(1024, 1);

    ApplicationId appId = ApplicationId.newInstance(1, 1);
    ApplicationAttemptId validAppAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ApplicationAttemptId invalidAppAttemptId =
        ApplicationAttemptId.newInstance(appId, 2);
    
    ContainerId validContainerId =
        ContainerId.newInstance(validAppAttemptId, 0);
    
    NodeId validNode = yarnCluster.getNodeManager(0).getNMContext().getNodeId();
    NodeId invalidNode = NodeId.newInstance("InvalidHost", 1234);

    
    org.apache.hadoop.yarn.api.records.Token validNMToken =
        nmTokenSecretManagerRM.createNMToken(validAppAttemptId, validNode, user);
    
    org.apache.hadoop.yarn.api.records.Token validContainerToken =
        containerTokenSecretManager.createContainerToken(validContainerId,
            validNode, user, r);
    
    StringBuilder sb;
    // testInvalidNMToken ... creating NMToken using different secret manager.
    
    NMTokenSecretManagerInRM tempManager = new NMTokenSecretManagerInRM(conf);
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
    Assert.assertTrue(errorMsg.contains(sb.toString()));
    
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
    
    // using appAttempt-2 token for launching container for appAttempt-1.
    invalidNMToken =
        nmTokenSecretManagerRM.createNMToken(invalidAppAttemptId, validNode,
            user);
    sb = new StringBuilder("\nNMToken for application attempt : ");
    sb.append(invalidAppAttemptId.toString())
      .append(" was used for starting container with container token")
      .append(" issued for application attempt : ")
      .append(validAppAttemptId.toString());
    Assert.assertTrue(testStartContainer(rpc, validAppAttemptId, validNode,
        validContainerToken, invalidNMToken, true).contains(sb.toString()));
    
    // using correct tokens. nmtoken for app attempt should get saved.
    conf.setInt(YarnConfiguration.RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS,
        4 * 60 * 1000);
    validContainerToken =
        containerTokenSecretManager.createContainerToken(validContainerId,
            validNode, user, r);
    
    testStartContainer(rpc, validAppAttemptId, validNode, validContainerToken,
        validNMToken, false);
    Assert.assertTrue(nmTokenSecretManagerNM
        .isAppAttemptNMTokenKeyPresent(validAppAttemptId));
    
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
    sb.append(validContainerId);
    sb.append(" was recently stopped on node manager");
    Assert.assertTrue(testGetContainer(rpc, validAppAttemptId, validNode,
        validContainerId, validNMToken, true).contains(sb.toString()));
    
    // Now lets remove the container from nm-memory
    nm.getNodeStatusUpdater().clearFinishedContainersFromCache();
    
    // This should fail as container is removed from recently tracked finished
    // containers.
    sb = new StringBuilder("Container ");
    sb.append(validContainerId.toString());
    sb.append(" is not handled by this NodeManager");
    Assert.assertTrue(testGetContainer(rpc, validAppAttemptId, validNode,
        validContainerId, validNMToken, false).contains(sb.toString()));

  }

  private void waitForContainerToFinishOnNM(ContainerId containerId) {
    Context nmContet = yarnCluster.getNodeManager(0).getNMContext();
    int interval = 4 * 60; // Max time for container token to expire.
    while ((interval-- > 0)
        && nmContet.getContainers().containsKey(containerId)) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
    }
    Assert.assertFalse(nmContet.getContainers().containsKey(containerId));
  }

  protected void waitForNMToReceiveNMTokenKey(
      NMTokenSecretManagerInNM nmTokenSecretManagerNM, NodeManager nm)
      throws InterruptedException {
    int attempt = 60;
    ContainerManagerImpl cm =
        ((ContainerManagerImpl) nm.getNMContext().getContainerManager());
    while ((cm.getBlockNewContainerRequestsStatus() || nmTokenSecretManagerNM
        .getNodeId() == null) && attempt-- > 0) {
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
          Arrays.asList(new ContainerId[] { containerId }), appAttemptId,
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
      GetContainerStatusesResponse statuses = proxy.getContainerStatuses(request);
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
        StartContainerRequest.newInstance(context,containerToken);
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
        NetUtils.createSocketAddr(nodeId.getHost(), nodeId.getPort());
    if (nmToken != null) {
      ugi.addToken(ConverterUtils.convertFromYarn(nmToken, addr));      
    }

    proxy = ugi
        .doAs(new PrivilegedAction<ContainerManagementProtocol>() {

          @Override
          public ContainerManagementProtocol run() {
            return (ContainerManagementProtocol) rpc.getProxy(
                ContainerManagementProtocol.class,
                addr, conf);
          }
        });
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
    ContainerId cId = ContainerId.newInstance(appAttemptId, 0);
    NodeManager nm = yarnCluster.getNodeManager(0);
    NMTokenSecretManagerInNM nmTokenSecretManagerInNM =
        nm.getNMContext().getNMTokenSecretManager();
    String user = "test";
    
    waitForNMToReceiveNMTokenKey(nmTokenSecretManagerInNM, nm);

    NodeId nodeId = nm.getNMContext().getNodeId();
    
    // Both id should be equal.
    Assert.assertEquals(nmTokenSecretManagerInNM.getCurrentKey().getKeyId(),
        nmTokenSecretManagerInRM.getCurrentKey().getKeyId());
    
    // Creating a tampered Container Token
    RMContainerTokenSecretManager containerTokenSecretManager =
        yarnCluster.getResourceManager().getRMContext().
            getContainerTokenSecretManager();
    
    RMContainerTokenSecretManager tamperedContainerTokenSecretManager =
        new RMContainerTokenSecretManager(conf);
    tamperedContainerTokenSecretManager.rollMasterKey();
    do {
      tamperedContainerTokenSecretManager.rollMasterKey();
      tamperedContainerTokenSecretManager.activateNextMasterKey();
    } while (containerTokenSecretManager.getCurrentKey().getKeyId()
        == tamperedContainerTokenSecretManager.getCurrentKey().getKeyId());
    
    Resource r = Resource.newInstance(1230, 2);
    // Creating modified containerToken
    Token containerToken =
        tamperedContainerTokenSecretManager.createContainerToken(cId, nodeId,
            user, r);
    Token nmToken =
        nmTokenSecretManagerInRM.createNMToken(appAttemptId, nodeId, user);
    YarnRPC rpc = YarnRPC.create(conf);
    StringBuilder sb = new StringBuilder("Given Container ");
    sb.append(cId);
    sb.append(" seems to have an illegally generated token.");
    Assert.assertTrue(testStartContainer(rpc, appAttemptId, nodeId,
        containerToken, nmToken, true).contains(sb.toString()));
  }
}
