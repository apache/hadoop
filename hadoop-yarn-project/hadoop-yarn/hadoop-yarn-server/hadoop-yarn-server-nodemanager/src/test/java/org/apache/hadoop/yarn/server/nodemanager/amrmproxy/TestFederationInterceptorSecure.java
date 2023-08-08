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

package org.apache.hadoop.yarn.server.nodemanager.amrmproxy;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.impl.FSRegistryOperationsService;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.authorize.Service;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationClientProtocolPB;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationMasterNotRegisteredException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.MockResourceManagerFacade;
import org.apache.hadoop.yarn.server.federation.policies.manager.UniformBroadcastPolicyManager;
import org.apache.hadoop.yarn.server.federation.store.impl.MemoryFederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager.NMContext;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMMemoryStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.util.Records;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestFederationInterceptorSecure extends BaseAMRMProxyTest {

  private static final Logger LOG = LoggerFactory.getLogger(TestFederationInterceptor.class);

  public static final String HOME_SC_ID = "SC-home";
  public static final String SC_ID1 = "SC-1";
  private static final String HOME_RM_ADDRESS = "127.0.0.1:28032";

  private YarnConfiguration conf;
  private TestableFederationInterceptor interceptor;
  private MemoryFederationStateStore stateStore;
  private NMStateStoreService nmStateStore;
  private RegistryOperations registry;

  private Context nmContext;
  private int testAppId;
  private ApplicationAttemptId attemptId;
  private volatile int lastResponseId;

  private MockResourceManagerFacade mockHomeRm = null;
  private Server homeClientRMRpcServer;

  private static File workDir;
  private static final File TEST_ROOT_DIR =
      new File(System.getProperty("test.build.dir", "target/test-dir"),
          TestFederationInterceptorSecure.class.getName() + "-root");
  private static MiniKdc miniKDC;
  private static String serverUserName = "yarn";
  private static String serverPrincipal;
  private static File serverKeytab;

  @ClassRule
  public final static TemporaryFolder FOLDER = new TemporaryFolder();

  @BeforeClass
  public static void setUpCluster() throws Exception {
    miniKDC = new MiniKdc(MiniKdc.createConf(), TEST_ROOT_DIR);
    miniKDC.start();

    workDir = FOLDER.getRoot();
    serverKeytab = new File(workDir, "yarn.keytab");
    serverPrincipal =
        serverUserName + "/" + (Path.WINDOWS ? "127.0.0.1" : "localhost") + "@EXAMPLE.COM";
    miniKDC.createPrincipal(serverKeytab, serverPrincipal);
  }

  @AfterClass
  public static void tearDownCluster() {
    if (miniKDC != null) {
      miniKDC.stop();
    }
    if (FOLDER != null) {
      FOLDER.delete();
    }
  }

  @Override
  public void setUp() throws IOException {
    super.setUp();

    stateStore = new MemoryFederationStateStore();
    stateStore.init(conf);
    FederationStateStoreFacade.getInstance().reinitialize(stateStore, conf);

    nmStateStore = new NMMemoryStateStoreService();
    nmStateStore.init(conf);
    nmStateStore.start();

    registry = new FSRegistryOperationsService();
    registry.init(conf);
    registry.start();

    testAppId = 1;
    attemptId = getApplicationAttemptId(testAppId);
    nmContext = new NMContext(null, null, null, null, nmStateStore, false, conf);
    lastResponseId = 0;
    this.mockHomeRm = new MockResourceManagerFacade(new YarnConfiguration(conf), 0);

    // Login as NodeManager
    conf.set(YarnConfiguration.NM_KEYTAB, this.serverKeytab.getAbsolutePath());
    conf.set(YarnConfiguration.NM_PRINCIPAL, this.serverPrincipal);
    SecurityUtil.login(conf, YarnConfiguration.NM_KEYTAB, YarnConfiguration.NM_PRINCIPAL);

    // start rpc server
    startRpcServer();
  }

  private void startRpcServer() {
    YarnRPC rpc = YarnRPC.create(conf);
    this.homeClientRMRpcServer = rpc.getServer(ApplicationClientProtocol.class, mockHomeRm,
        NetUtils.createSocketAddr(HOME_RM_ADDRESS), conf, null, 2);
    this.homeClientRMRpcServer.start();
    this.homeClientRMRpcServer.refreshServiceAcl(conf, new MockRMPolicyProvider());
  }

  private void stopRpcServer() {
    if (homeClientRMRpcServer != null) {
      homeClientRMRpcServer.stop();
    }
  }

  @Override
  public void tearDown() {
    interceptor.cleanupRegistry();
    interceptor.shutdown();
    registry.stop();
    stopRpcServer();
    super.tearDown();
  }

  @Override
  protected YarnConfiguration createConfiguration() {
    conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.AMRM_PROXY_ENABLED, true);
    conf.setBoolean(YarnConfiguration.FEDERATION_ENABLED, true);
    String mockPassThroughInterceptorClass =
        PassThroughRequestInterceptor.class.getName();

    // Create a request interceptor pipeline for testing. The last one in the
    // chain is the federation interceptor that calls the mock resource manager.
    // The others in the chain will simply forward it to the next one in the
    // chain
    conf.set(YarnConfiguration.AMRM_PROXY_INTERCEPTOR_CLASS_PIPELINE,
        mockPassThroughInterceptorClass + "," + mockPassThroughInterceptorClass
            + "," + TestableFederationInterceptor.class.getName());

    conf.set(YarnConfiguration.FEDERATION_POLICY_MANAGER,
        UniformBroadcastPolicyManager.class.getName());

    conf.set(YarnConfiguration.RM_CLUSTER_ID, HOME_SC_ID);

    // Disable StateStoreFacade cache
    conf.setInt(YarnConfiguration.FEDERATION_CACHE_TIME_TO_LIVE_SECS, 0);

    // Set sub-cluster timeout to 500ms
    conf.setLong(YarnConfiguration.FEDERATION_AMRMPROXY_SUBCLUSTER_TIMEOUT, 500);

    // Enable security mode
    conf.set(HADOOP_SECURITY_AUTHENTICATION, AuthMethod.KERBEROS.toString());
    conf.setBoolean(HADOOP_SECURITY_AUTHORIZATION, true);
    UserGroupInformation.setConfiguration(conf);
    conf.set("hadoop.proxyuser.yarn.hosts", "*");
    conf.set("hadoop.proxyuser.yarn.groups", "*");
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf, ProxyUsers.CONF_HADOOP_PROXYUSER);
    conf.set(YarnConfiguration.RM_PRINCIPAL, serverPrincipal);

    return conf;
  }

  private void registerSubCluster(SubClusterId subClusterId)
      throws YarnException {
    if (subClusterId.getId().equals(HOME_SC_ID)) {
      stateStore.registerSubCluster(SubClusterRegisterRequest.newInstance(SubClusterInfo
          .newInstance(subClusterId, "1.2.3.4:1", HOME_RM_ADDRESS, "1.2.3.4:3", "1.2.3.4:4",
              SubClusterState.SC_RUNNING, 0, "capacity")));
    } else {
      stateStore.registerSubCluster(SubClusterRegisterRequest.newInstance(SubClusterInfo
          .newInstance(subClusterId, "1.2.3.4:1", "1.2.3.4:2", "1.2.3.4:3", "1.2.3.4:4",
              SubClusterState.SC_RUNNING, 0, "capacity")));
    }
  }

  private List<Container> getContainersAndAssert(int numberOfResourceRequests,
      int numberOfAllocationExcepted) throws Exception {
    AllocateRequest allocateRequest = Records.newRecord(AllocateRequest.class);
    List<Container> containers = new ArrayList<>(numberOfResourceRequests);
    List<ResourceRequest> askList = new ArrayList<>(numberOfResourceRequests);
    for (int id = 0; id < numberOfResourceRequests; id++) {
      askList.add(createResourceRequest("test-node-" + id, 6000, 2, id % 5, 1));
    }

    allocateRequest.setAskList(askList);

    allocateRequest.setResponseId(lastResponseId);
    AllocateResponse allocateResponse = interceptor.allocate(allocateRequest);
    Assert.assertNotNull("allocate() returned null response", allocateResponse);
    checkAMRMToken(allocateResponse.getAMRMToken());
    lastResponseId = allocateResponse.getResponseId();

    containers.addAll(allocateResponse.getAllocatedContainers());
    LOG.info("Number of allocated containers in the original request: {}",
        allocateResponse.getAllocatedContainers().size());

    // Send max 10 heart beats to receive all the containers. If not, we will
    // fail the test
    int numHeartbeat = 0;
    while (containers.size() < numberOfAllocationExcepted && numHeartbeat++ < 10) {
      allocateRequest = Records.newRecord(AllocateRequest.class);
      allocateRequest.setResponseId(lastResponseId);
      allocateResponse = interceptor.allocate(allocateRequest);
      Assert.assertNotNull("allocate() returned null response", allocateResponse);
      checkAMRMToken(allocateResponse.getAMRMToken());
      lastResponseId = allocateResponse.getResponseId();

      // Make sure this request is picked up by all async heartbeat handlers
      interceptor.drainAllAsyncQueue(false);

      containers.addAll(allocateResponse.getAllocatedContainers());
      LOG.info("Number of allocated containers in this request: {}.",
          allocateResponse.getAllocatedContainers().size());
      LOG.info("Total number of allocated containers: {}.", containers.size());
      Thread.sleep(10);
    }
    Assert.assertEquals(numberOfAllocationExcepted, containers.size());
    return containers;
  }

  private void releaseContainersAndAssert(List<Container> containers)
      throws Exception {
    Assert.assertTrue(containers.size() > 0);
    AllocateRequest allocateRequest = Records.newRecord(AllocateRequest.class);
    List<ContainerId> relList = new ArrayList<>(containers.size());
    for (Container container : containers) {
      relList.add(container.getId());
    }

    allocateRequest.setReleaseList(relList);

    allocateRequest.setResponseId(lastResponseId);
    AllocateResponse allocateResponse = interceptor.allocate(allocateRequest);
    Assert.assertNotNull(allocateResponse);
    checkAMRMToken(allocateResponse.getAMRMToken());
    lastResponseId = allocateResponse.getResponseId();

    // The release request will be split and handled by the corresponding UAM.
    // The release containers returned by the mock resource managers will be
    // aggregated and returned back to us, and we can check if total request size
    // and returned size are the same
    List<ContainerId> containersForReleasedContainerIds = new ArrayList<>();
    List<ContainerId> newlyFinished = getCompletedContainerIds(
        allocateResponse.getCompletedContainersStatuses());
    containersForReleasedContainerIds.addAll(newlyFinished);
    LOG.info("Number of containers received in the original request: {}", newlyFinished.size());

    // Send max 10 heart beats to receive all the containers. If not, we will fail the test
    int numHeartbeat = 0;
    while (containersForReleasedContainerIds.size() < relList.size() && numHeartbeat++ < 10) {
      allocateRequest = Records.newRecord(AllocateRequest.class);
      allocateRequest.setResponseId(lastResponseId);
      allocateResponse = interceptor.allocate(allocateRequest);
      Assert.assertNotNull(allocateResponse);
      checkAMRMToken(allocateResponse.getAMRMToken());
      lastResponseId = allocateResponse.getResponseId();

      // Make sure this request is picked up by all async heartbeat handlers
      interceptor.drainAllAsyncQueue(false);

      newlyFinished = getCompletedContainerIds(allocateResponse.getCompletedContainersStatuses());
      containersForReleasedContainerIds.addAll(newlyFinished);
      LOG.info("Number of containers received in this request: {}.", newlyFinished.size());
      LOG.info("Total number of containers received: {}.",
          containersForReleasedContainerIds.size());
      Thread.sleep(10);
    }

    Assert.assertEquals(relList.size(), containersForReleasedContainerIds.size());
  }

  private void checkAMRMToken(Token amrmToken) {
    if (amrmToken != null) {
      // The token should be the one issued by home MockRM
      Assert.assertEquals(Integer.toString(0), amrmToken.getKind());
    }
  }

  @Test
  public void testRecoverWithAMRMProxyHA() throws Exception {
    testRecoverWithRPCClientRM(registry);
  }

  @Test
  public void testRecoverWithoutAMRMProxyHA() throws Exception {
    testRecoverWithRPCClientRM(null);
  }

  protected void testRecoverWithRPCClientRM(
      final RegistryOperations registryObj)
      throws Exception {
    UserGroupInformation ugi = this.getUGIWithToken(attemptId);
    ugi.doAs((PrivilegedExceptionAction<Object>) () -> {
      interceptor = new TestableFederationInterceptor(mockHomeRm);
      interceptor.init(new AMRMProxyApplicationContextImpl(nmContext, conf, attemptId,
          "test-user", null, null, null, registryObj));
      interceptor.cleanupRegistry();

      // Register the application
      RegisterApplicationMasterRequest registerReq =
          Records.newRecord(RegisterApplicationMasterRequest.class);
      registerReq.setHost(Integer.toString(testAppId));
      registerReq.setRpcPort(testAppId);
      registerReq.setTrackingUrl("");

      RegisterApplicationMasterResponse registerResponse =
          interceptor.registerApplicationMaster(registerReq);
      Assert.assertNotNull(registerResponse);
      lastResponseId = 0;

      Assert.assertEquals(0, interceptor.getUnmanagedAMPoolSize());

      // Allocate one batch of containers
      registerSubCluster(SubClusterId.newInstance(SC_ID1));
      registerSubCluster(SubClusterId.newInstance(HOME_SC_ID));

      int numberOfContainers = 3;
      List<Container> containers =
          getContainersAndAssert(numberOfContainers, numberOfContainers * 2);
      Assert.assertEquals(1, interceptor.getUnmanagedAMPoolSize());

      // Make sure all async hb threads are done
      interceptor.drainAllAsyncQueue(true);

      // Prepare for Federation Interceptor restart and recover
      Map<String, byte[]> recoveredDataMap = recoverDataMapForAppAttempt(nmStateStore, attemptId);
      String scEntry = FederationInterceptor.NMSS_SECONDARY_SC_PREFIX + "SC-1";
      if (registryObj == null) {
        Assert.assertTrue(recoveredDataMap.containsKey(scEntry));
      } else {
        // When AMRMPRoxy HA is enabled, NMSS should not have the UAM token,
        // it should be in Registry
        Assert.assertFalse(recoveredDataMap.containsKey(scEntry));
      }

      // Preserve the mock RM instances
      MockResourceManagerFacade homeRM = interceptor.getHomeRM();
      ConcurrentHashMap<String, MockResourceManagerFacade> secondaries =
          interceptor.getSecondaryRMs();

      // Create a new interceptor instance and recover
      interceptor = new TestableFederationInterceptor(homeRM, secondaries);
      interceptor.init(new AMRMProxyApplicationContextImpl(nmContext, conf, attemptId,
          "test-user", null, null, null, registryObj));
      interceptor.setClientRPC(true);
      interceptor.recover(recoveredDataMap);
      interceptor.setClientRPC(false);

      Assert.assertEquals(1, interceptor.getUnmanagedAMPoolSize());
      // SC-1 should be initialized to be timed out
      Assert.assertEquals(1, interceptor.getTimedOutSCs(true).size());

      // The first allocate call expects a fail-over exception and re-register
      try {
        AllocateRequest allocateRequest = Records.newRecord(AllocateRequest.class);
        allocateRequest.setResponseId(lastResponseId);
        AllocateResponse allocateResponse = interceptor.allocate(allocateRequest);
        lastResponseId = allocateResponse.getResponseId();
        Assert.fail("Expecting an ApplicationMasterNotRegisteredException  "
            + " after FederationInterceptor restarts and recovers");
      } catch (ApplicationMasterNotRegisteredException e) {
      }
      interceptor.registerApplicationMaster(registerReq);
      lastResponseId = 0;

      // Release all containers
      releaseContainersAndAssert(containers);

      // Finish the application
      FinishApplicationMasterRequest finishReq =
          Records.newRecord(FinishApplicationMasterRequest.class);
      finishReq.setDiagnostics("");
      finishReq.setTrackingUrl("");
      finishReq.setFinalApplicationStatus(FinalApplicationStatus.SUCCEEDED);

      FinishApplicationMasterResponse finishResponse =
          interceptor.finishApplicationMaster(finishReq);
      Assert.assertNotNull(finishResponse);
      Assert.assertTrue(finishResponse.getIsUnregistered());

      // After the application succeeds, the registry/NMSS entry should be
      // cleaned up
      if (registryObj != null) {
        Assert.assertEquals(0, interceptor.getRegistryClient().getAllApplications().size());
      } else {
        recoveredDataMap = recoverDataMapForAppAttempt(nmStateStore, attemptId);
        Assert.assertFalse(recoveredDataMap.containsKey(scEntry));
      }
      return null;
    });
  }

  private UserGroupInformation getUGIWithToken(
      ApplicationAttemptId appAttemptId) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    AMRMTokenIdentifier token = new AMRMTokenIdentifier(appAttemptId, 1);
    ugi.addTokenIdentifier(token);
    return ugi;
  }

  private class MockRMPolicyProvider extends PolicyProvider {

    private MockRMPolicyProvider() {
    }

    private final Service[] resourceManagerServices =
        new Service[]{
            new Service(
                YarnConfiguration.YARN_SECURITY_SERVICE_AUTHORIZATION_APPLICATIONCLIENT_PROTOCOL,
                ApplicationClientProtocolPB.class),
        };

    @Override
    public Service[] getServices() {
      return resourceManagerServices;
    }
  }
}
