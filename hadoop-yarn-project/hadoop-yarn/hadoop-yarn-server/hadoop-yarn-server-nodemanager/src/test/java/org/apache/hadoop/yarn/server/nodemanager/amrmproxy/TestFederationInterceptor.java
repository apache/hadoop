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

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.impl.FSRegistryOperationsService;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.PreemptionContainer;
import org.apache.hadoop.yarn.api.records.PreemptionContract;
import org.apache.hadoop.yarn.api.records.PreemptionMessage;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.UpdateContainerError;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationMasterNotRegisteredException;
import org.apache.hadoop.yarn.exceptions.InvalidApplicationMasterRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.MockResourceManagerFacade;
import org.apache.hadoop.yarn.server.federation.policies.manager.UniformBroadcastPolicyManager;
import org.apache.hadoop.yarn.server.federation.store.impl.MemoryFederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.utils.FederationRegistryClient;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager.NMContext;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMMemoryStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.uam.UnmanagedAMPoolManager;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extends the TestAMRMProxyService and overrides methods in order to use the
 * AMRMProxyService's pipeline test cases for testing the FederationInterceptor
 * class. The tests for AMRMProxyService has been written cleverly so that it
 * can be reused to validate different request interceptor chains.
 */
public class TestFederationInterceptor extends BaseAMRMProxyTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestFederationInterceptor.class);

  public static final String HOME_SC_ID = "SC-home";

  private TestableFederationInterceptor interceptor;
  private MemoryFederationStateStore stateStore;
  private NMStateStoreService nmStateStore;
  private RegistryOperations registry;

  private Context nmContext;
  private int testAppId;
  private ApplicationAttemptId attemptId;

  private volatile int lastResponseId;

  @Override
  public void setUp() throws IOException {
    super.setUp();
    interceptor = new TestableFederationInterceptor();

    stateStore = new MemoryFederationStateStore();
    stateStore.init(getConf());
    FederationStateStoreFacade.getInstance().reinitialize(stateStore,
        getConf());

    nmStateStore = new NMMemoryStateStoreService();
    nmStateStore.init(getConf());
    nmStateStore.start();

    registry = new FSRegistryOperationsService();
    registry.init(getConf());
    registry.start();

    testAppId = 1;
    attemptId = getApplicationAttemptId(testAppId);
    stateStore.setApplicationContext(HOME_SC_ID, attemptId.getApplicationId(), Time.now());

    nmContext =
        new NMContext(null, null, null, null, nmStateStore, false, getConf());
    interceptor.init(new AMRMProxyApplicationContextImpl(nmContext, getConf(),
        attemptId, "test-user", null, null, null, registry));
    interceptor.cleanupRegistry();

    lastResponseId = 0;
  }

  @Override
  public void tearDown() {
    interceptor.cleanupRegistry();
    interceptor.shutdown();
    registry.stop();
    super.tearDown();
  }

  @Override
  protected YarnConfiguration createConfiguration() {
    YarnConfiguration conf = new YarnConfiguration();
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
    conf.setLong(YarnConfiguration.FEDERATION_AMRMPROXY_SUBCLUSTER_TIMEOUT,
        500);

    // Register UAM Retry Interval 1ms
    conf.setLong(YarnConfiguration.FEDERATION_AMRMPROXY_REGISTER_UAM_RETRY_INTERVAL, 1);
    return conf;
  }

  private void registerSubCluster(SubClusterId subClusterId)
      throws YarnException {
    stateStore
        .registerSubCluster(SubClusterRegisterRequest.newInstance(SubClusterInfo
            .newInstance(subClusterId, "1.2.3.4:1", "1.2.3.4:2", "1.2.3.4:3",
                "1.2.3.4:4", SubClusterState.SC_RUNNING, 0, "capacity")));
  }

  private void deRegisterSubCluster(SubClusterId subClusterId)
      throws YarnException {
    stateStore.deregisterSubCluster(SubClusterDeregisterRequest
        .newInstance(subClusterId, SubClusterState.SC_UNREGISTERED));
  }

  private List<Container> getContainersAndAssert(int numberOfResourceRequests,
      int numberOfAllocationExcepted) throws Exception {
    AllocateRequest allocateRequest = Records.newRecord(AllocateRequest.class);
    List<Container> containers = new ArrayList<>(numberOfResourceRequests);
    List<ResourceRequest> askList = new ArrayList<>(numberOfResourceRequests);
    for (int id = 0; id < numberOfResourceRequests; id++) {
      askList.add(createResourceRequest("test-node-" + id,
          6000, 2, id % 5, 1));
    }

    allocateRequest.setAskList(askList);

    allocateRequest.setResponseId(lastResponseId);
    AllocateResponse allocateResponse = interceptor.allocate(allocateRequest);
    Assert.assertNotNull("allocate() returned null response", allocateResponse);
    checkAMRMToken(allocateResponse.getAMRMToken());
    lastResponseId = allocateResponse.getResponseId();

    containers.addAll(allocateResponse.getAllocatedContainers());
    LOG.info("Number of allocated containers in the original request: "
        + allocateResponse.getAllocatedContainers().size());

    // Send max 10 heart beats to receive all the containers. If not, we will
    // fail the test
    int numHeartbeat = 0;
    while (containers.size() < numberOfAllocationExcepted
        && numHeartbeat++ < 10) {
      allocateRequest = Records.newRecord(AllocateRequest.class);
      allocateRequest.setResponseId(lastResponseId);
      allocateResponse = interceptor.allocate(allocateRequest);
      Assert.assertNotNull("allocate() returned null response",
          allocateResponse);
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
    LOG.info("Number of containers received in the original request: {}",
        newlyFinished.size());

    // Send max 10 heart beats to receive all the containers. If not, we will
    // fail the test
    int numHeartbeat = 0;
    while (containersForReleasedContainerIds.size() < relList.size()
        && numHeartbeat++ < 10) {
      allocateRequest = Records.newRecord(AllocateRequest.class);
      allocateRequest.setResponseId(lastResponseId);
      allocateResponse = interceptor.allocate(allocateRequest);
      Assert.assertNotNull(allocateResponse);
      checkAMRMToken(allocateResponse.getAMRMToken());
      lastResponseId = allocateResponse.getResponseId();

      // Make sure this request is picked up by all async heartbeat handlers
      interceptor.drainAllAsyncQueue(false);

      newlyFinished = getCompletedContainerIds(
          allocateResponse.getCompletedContainersStatuses());
      containersForReleasedContainerIds.addAll(newlyFinished);
      LOG.info("Number of containers received in this request: {}.", newlyFinished.size());
      LOG.info("Total number of containers received: {}.",
          containersForReleasedContainerIds.size());
      Thread.sleep(10);
    }

    Assert.assertEquals(relList.size(),
        containersForReleasedContainerIds.size());
  }

  private void checkAMRMToken(Token amrmToken) {
    if (amrmToken != null) {
      // The token should be the one issued by home MockRM
      Assert.assertEquals(Integer.toString(0), amrmToken.getKind());
    }
  }

  @Test
  public void testMultipleSubClusters() throws Exception {
    UserGroupInformation ugi =
        interceptor.getUGIWithToken(interceptor.getAttemptId());
    ugi.doAs((PrivilegedExceptionAction<Object>) () -> {
      // Register the application
      RegisterApplicationMasterRequest registerReq =
          Records.newRecord(RegisterApplicationMasterRequest.class);
      registerReq.setHost(Integer.toString(testAppId));
      registerReq.setRpcPort(0);
      registerReq.setTrackingUrl("");

      RegisterApplicationMasterResponse registerResponse =
          interceptor.registerApplicationMaster(registerReq);
      Assert.assertNotNull(registerResponse);
      lastResponseId = 0;

      Assert.assertEquals(0, interceptor.getUnmanagedAMPoolSize());

      // Allocate the first batch of containers, with sc1 and sc2 active
      registerSubCluster(SubClusterId.newInstance("SC-1"));
      registerSubCluster(SubClusterId.newInstance("SC-2"));

      int numberOfContainers = 3;
      List<Container> containers =
          getContainersAndAssert(numberOfContainers, numberOfContainers * 2);
      Assert.assertEquals(2, interceptor.getUnmanagedAMPoolSize());

      // Allocate the second batch of containers, with sc1 and sc3 active
      deRegisterSubCluster(SubClusterId.newInstance("SC-2"));
      registerSubCluster(SubClusterId.newInstance("SC-3"));

      numberOfContainers = 1;
      containers.addAll(
          getContainersAndAssert(numberOfContainers, numberOfContainers * 2));
      Assert.assertEquals(3, interceptor.getUnmanagedAMPoolSize());

      // Allocate the third batch of containers with only in home sub-cluster
      // active
      deRegisterSubCluster(SubClusterId.newInstance("SC-1"));
      deRegisterSubCluster(SubClusterId.newInstance("SC-3"));
      registerSubCluster(SubClusterId.newInstance(HOME_SC_ID));

      numberOfContainers = 2;
      containers.addAll(
          getContainersAndAssert(numberOfContainers, numberOfContainers));
      Assert.assertEquals(3, interceptor.getUnmanagedAMPoolSize());

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

      return null;
    });
  }

  /*
   * Test re-register when RM fails over.
   */
  @Test
  public void testReregister() throws Exception {
    UserGroupInformation ugi =
        interceptor.getUGIWithToken(interceptor.getAttemptId());
    ugi.doAs((PrivilegedExceptionAction<Object>) () -> {

      // Register the application
      RegisterApplicationMasterRequest registerReq =
          Records.newRecord(RegisterApplicationMasterRequest.class);
      registerReq.setHost(Integer.toString(testAppId));
      registerReq.setRpcPort(0);
      registerReq.setTrackingUrl("");

      RegisterApplicationMasterResponse registerResponse =
          interceptor.registerApplicationMaster(registerReq);
      Assert.assertNotNull(registerResponse);
      lastResponseId = 0;

      Assert.assertEquals(0, interceptor.getUnmanagedAMPoolSize());

      // Allocate the first batch of containers
      registerSubCluster(SubClusterId.newInstance("SC-1"));
      registerSubCluster(SubClusterId.newInstance(HOME_SC_ID));

      interceptor.setShouldReRegisterNext();

      int numberOfContainers = 3;
      List<Container> containers =
          getContainersAndAssert(numberOfContainers, numberOfContainers * 2);
      Assert.assertEquals(1, interceptor.getUnmanagedAMPoolSize());

      interceptor.setShouldReRegisterNext();

      // Release all containers
      releaseContainersAndAssert(containers);

      interceptor.setShouldReRegisterNext();

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
      return null;
    });
  }

  /*
   * Test concurrent register threads. This is possible because the timeout
   * between AM and AMRMProxy is shorter than the timeout + failOver between
   * FederationInterceptor (AMRMProxy) and RM. When first call is blocked due to
   * RM failover and AM timeout, it will call us resulting in a second register
   * thread.
   */
  @Test(timeout = 5000)
  public void testConcurrentRegister()
      throws InterruptedException, ExecutionException {
    ExecutorService threadpool = Executors.newCachedThreadPool();
    ExecutorCompletionService<RegisterApplicationMasterResponse> compSvc =
        new ExecutorCompletionService<>(threadpool);

    Object syncObj = MockResourceManagerFacade.getRegisterSyncObj();

    // Two register threads
    synchronized (syncObj) {
      // Make sure first thread will block within RM, before the second thread
      // starts
      LOG.info("Starting first register thread");
      compSvc.submit(new ConcurrentRegisterAMCallable());

      try {
        LOG.info("Test main starts waiting for the first thread to block");
        syncObj.wait();
        LOG.info("Test main wait finished");
      } catch (Exception e) {
        LOG.info("Test main wait interrupted", e);
      }
    }

    // The second thread will get already registered exception from RM.
    LOG.info("Starting second register thread");
    compSvc.submit(new ConcurrentRegisterAMCallable());

    // Notify the first register thread to return
    LOG.info("Let first blocked register thread move on");
    synchronized (syncObj) {
      syncObj.notifyAll();
    }

    // Both thread should return without exception
    RegisterApplicationMasterResponse response = compSvc.take().get();
    Assert.assertNotNull(response);

    response = compSvc.take().get();
    Assert.assertNotNull(response);

    threadpool.shutdown();
  }

  /**
   * A callable that calls registerAM to RM with blocking.
   */
  public class ConcurrentRegisterAMCallable
      implements Callable<RegisterApplicationMasterResponse> {
    @Override
    public RegisterApplicationMasterResponse call() throws Exception {
      RegisterApplicationMasterResponse response;
      try {
        // Use port number 1001 to let mock RM block in the register call
        response = interceptor.registerApplicationMaster(
            RegisterApplicationMasterRequest.newInstance(null, 1001, null));
        lastResponseId = 0;
      } catch (Exception e) {
        LOG.info("Register thread exception", e);
        response = null;
      }
      return response;
    }
  }

  @Test
  public void testRecoverWithAMRMProxyHA() throws Exception {
    testRecover(registry);
  }

  @Test
  public void testRecoverWithoutAMRMProxyHA() throws Exception {
    testRecover(null);
  }

  @Test
  public void testRecoverBadSCWithAMRMProxyHA() throws Exception {
    testRecoverWithBadSubCluster(registry);
  }

  @Test
  public void testRecoverBadSCWithoutAMRMProxyHA() throws Exception {
    testRecoverWithBadSubCluster(null);
  }

  protected void testRecover(final RegistryOperations registryObj)
      throws Exception {
    UserGroupInformation ugi =
        interceptor.getUGIWithToken(interceptor.getAttemptId());
    ugi.doAs((PrivilegedExceptionAction<Object>) () -> {
      interceptor = new TestableFederationInterceptor();
      interceptor.init(new AMRMProxyApplicationContextImpl(nmContext,
          getConf(), attemptId, "test-user", null, null, null, registryObj));
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
      registerSubCluster(SubClusterId.newInstance("SC-1"));
      registerSubCluster(SubClusterId.newInstance(HOME_SC_ID));

      int numberOfContainers = 3;
      List<Container> containers =
          getContainersAndAssert(numberOfContainers, numberOfContainers * 2);
      Assert.assertEquals(1, interceptor.getUnmanagedAMPoolSize());

      // Make sure all async hb threads are done
      interceptor.drainAllAsyncQueue(true);

      // Prepare for Federation Interceptor restart and recover
      Map<String, byte[]> recoveredDataMap =
          recoverDataMapForAppAttempt(nmStateStore, attemptId);
      String scEntry =
          FederationInterceptor.NMSS_SECONDARY_SC_PREFIX + "SC-1";
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
      interceptor.init(new AMRMProxyApplicationContextImpl(nmContext,
          getConf(), attemptId, "test-user", null, null, null, registryObj));
      interceptor.recover(recoveredDataMap);

      Assert.assertEquals(1, interceptor.getUnmanagedAMPoolSize());
      // SC1 should be initialized to be timed out
      Assert.assertEquals(1, interceptor.getTimedOutSCs(true).size());

      // The first allocate call expects a fail-over exception and re-register
      try {
        AllocateRequest allocateRequest =
            Records.newRecord(AllocateRequest.class);
        allocateRequest.setResponseId(lastResponseId);
        AllocateResponse allocateResponse =
            interceptor.allocate(allocateRequest);
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
        Assert.assertEquals(0,
            interceptor.getRegistryClient().getAllApplications().size());
      } else {
        recoveredDataMap =
            recoverDataMapForAppAttempt(nmStateStore, attemptId);
        Assert.assertFalse(recoveredDataMap.containsKey(scEntry));
      }
      return null;
    });
  }

  @Test
  public void testRequestInterceptorChainCreation() throws Exception {
    RequestInterceptor root =
        super.getAMRMProxyService().createRequestInterceptorChain();
    int index = 0;
    while (root != null) {
      switch (index) {
      case 0:
      case 1:
        Assert.assertEquals(PassThroughRequestInterceptor.class.getName(),
            root.getClass().getName());
        break;
      case 2:
        Assert.assertEquals(TestableFederationInterceptor.class.getName(),
            root.getClass().getName());
        break;
      default:
        Assert.fail();
      }
      root = root.getNextInterceptor();
      index++;
    }
    Assert.assertEquals("The number of interceptors in chain does not match",
        Integer.toString(3), Integer.toString(index));
  }

  /**
   * Between AM and AMRMProxy, FederationInterceptor modifies the RM behavior,
   * so that when AM registers more than once, it returns the same register
   * success response instead of throwing
   * {@link InvalidApplicationMasterRequestException}
   *
   * We did this because FederationInterceptor can receive concurrent register
   * requests from AM because of timeout between AM and AMRMProxy. This can
   * possible since the timeout between FederationInterceptor and RM longer
   * because of performFailover + timeout.
   */
  @Test
  public void testTwoIdenticalRegisterRequest() throws Exception {
    // Register the application twice
    RegisterApplicationMasterRequest registerReq =
        Records.newRecord(RegisterApplicationMasterRequest.class);
    registerReq.setHost(Integer.toString(testAppId));
    registerReq.setRpcPort(0);
    registerReq.setTrackingUrl("");

    for (int i = 0; i < 2; i++) {
      RegisterApplicationMasterResponse registerResponse =
          interceptor.registerApplicationMaster(registerReq);
      Assert.assertNotNull(registerResponse);
      lastResponseId = 0;
    }
  }

  @Test
  public void testTwoDifferentRegisterRequest() throws Exception {
    // Register the application first time
    RegisterApplicationMasterRequest registerReq =
        Records.newRecord(RegisterApplicationMasterRequest.class);
    registerReq.setHost(Integer.toString(testAppId));
    registerReq.setRpcPort(0);
    registerReq.setTrackingUrl("");

    RegisterApplicationMasterResponse registerResponse =
        interceptor.registerApplicationMaster(registerReq);
    Assert.assertNotNull(registerResponse);
    lastResponseId = 0;

    // Register the application second time with a different request obj
    registerReq = Records.newRecord(RegisterApplicationMasterRequest.class);
    registerReq.setHost(Integer.toString(testAppId));
    registerReq.setRpcPort(0);
    registerReq.setTrackingUrl("different");
    try {
      registerResponse = interceptor.registerApplicationMaster(registerReq);
      lastResponseId = 0;
      Assert.fail("Should throw if a different request obj is used");
    } catch (YarnException e) {
    }
  }

  @Test
  public void testAllocateResponse() throws Exception {
    interceptor.registerApplicationMaster(
        RegisterApplicationMasterRequest.newInstance(null, 0, null));
    AllocateRequest allocateRequest = Records.newRecord(AllocateRequest.class);

    Map<SubClusterId, List<AllocateResponse>> asyncResponseSink =
        interceptor.getAsyncResponseSink();

    ContainerId cid = ContainerId.newContainerId(attemptId, 0);
    ContainerStatus cStatus = Records.newRecord(ContainerStatus.class);
    cStatus.setContainerId(cid);
    Container container =
        Container.newInstance(cid, null, null, null, null, null);

    AllocateResponse response = Records.newRecord(AllocateResponse.class);
    response.setAllocatedContainers(Collections.singletonList(container));
    response.setCompletedContainersStatuses(Collections.singletonList(cStatus));
    response.setUpdatedNodes(
        Collections.singletonList(Records.newRecord(NodeReport.class)));
    response.setNMTokens(
        Collections.singletonList(Records.newRecord(NMToken.class)));
    response.setUpdatedContainers(
        Collections.singletonList(Records.newRecord(UpdatedContainer.class)));
    response.setUpdateErrors(Collections
        .singletonList(Records.newRecord(UpdateContainerError.class)));
    response.setAvailableResources(Records.newRecord(Resource.class));
    response.setPreemptionMessage(Records.newRecord(PreemptionMessage.class));

    List<AllocateResponse> list = new ArrayList<>();
    list.add(response);
    asyncResponseSink.put(SubClusterId.newInstance("SC-1"), list);

    response = interceptor.allocate(allocateRequest);

    Assert.assertEquals(1, response.getAllocatedContainers().size());
    Assert.assertNotNull(response.getAvailableResources());
    Assert.assertEquals(1, response.getCompletedContainersStatuses().size());
    Assert.assertEquals(1, response.getUpdatedNodes().size());
    Assert.assertNotNull(response.getPreemptionMessage());
    Assert.assertEquals(1, response.getNMTokens().size());
    Assert.assertEquals(1, response.getUpdatedContainers().size());
    Assert.assertEquals(1, response.getUpdateErrors().size());
    Assert.assertNotNull(response.getApplicationPriority());
  }

  @Test
  public void testSubClusterTimeOut() throws Exception {
    UserGroupInformation ugi =
        interceptor.getUGIWithToken(interceptor.getAttemptId());
    ugi.doAs((PrivilegedExceptionAction<Object>) () -> {
      // Register the application first time
      RegisterApplicationMasterRequest registerReq =
          Records.newRecord(RegisterApplicationMasterRequest.class);
      registerReq.setHost(Integer.toString(testAppId));
      registerReq.setRpcPort(0);
      registerReq.setTrackingUrl("");
      RegisterApplicationMasterResponse registerResponse =
          interceptor.registerApplicationMaster(registerReq);
      Assert.assertNotNull(registerResponse);
      lastResponseId = 0;

      registerSubCluster(SubClusterId.newInstance("SC-1"));

      getContainersAndAssert(1, 1);

      AllocateResponse allocateResponse = interceptor.generateBaseAllocationResponse();
      Assert.assertEquals(2, allocateResponse.getNumClusterNodes());
      Assert.assertEquals(0, interceptor.getTimedOutSCs(true).size());

      // Let all SC timeout (home and SC-1), without an allocate from AM
      Thread.sleep(800);

      // Should not be considered timeout, because there's no recent AM
      // heartbeat
      allocateResponse = interceptor.generateBaseAllocationResponse();
      Assert.assertEquals(2, allocateResponse.getNumClusterNodes());
      Assert.assertEquals(0, interceptor.getTimedOutSCs(true).size());

      // Generate a duplicate heartbeat from AM, so that it won't really
      // trigger a heartbeat to all SC
      AllocateRequest allocateRequest = Records.newRecord(AllocateRequest.class);
      // Set to lastResponseId - 1 so that it will be considered a duplicate
      // heartbeat and thus not forwarded to all SCs
      allocateRequest.setResponseId(lastResponseId - 1);
      interceptor.allocate(allocateRequest);

      // Should be considered timeout
      allocateResponse = interceptor.generateBaseAllocationResponse();
      Assert.assertEquals(0, allocateResponse.getNumClusterNodes());
      Assert.assertEquals(2, interceptor.getTimedOutSCs(true).size());
      return null;
    });
  }

  @Test
  public void testSecondAttempt() throws Exception {
    final RegisterApplicationMasterRequest registerReq =
        Records.newRecord(RegisterApplicationMasterRequest.class);
    registerReq.setHost(Integer.toString(testAppId));
    registerReq.setRpcPort(testAppId);
    registerReq.setTrackingUrl("");

    UserGroupInformation ugi =
        interceptor.getUGIWithToken(interceptor.getAttemptId());
    ugi.doAs((PrivilegedExceptionAction<Object>) () -> {
      // Register the application
      RegisterApplicationMasterResponse registerResponse =
          interceptor.registerApplicationMaster(registerReq);
      Assert.assertNotNull(registerResponse);
      lastResponseId = 0;

      Assert.assertEquals(0, interceptor.getUnmanagedAMPoolSize());

      // Allocate one batch of containers
      registerSubCluster(SubClusterId.newInstance("SC-1"));
      registerSubCluster(SubClusterId.newInstance(HOME_SC_ID));

      int numberOfContainers = 3;
      List<Container> containers =
          getContainersAndAssert(numberOfContainers, numberOfContainers * 2);
      for (Container c : containers) {
        LOG.info("Allocated container {}.", c.getId());
      }
      Assert.assertEquals(1, interceptor.getUnmanagedAMPoolSize());

      // Make sure all async hb threads are done
      interceptor.drainAllAsyncQueue(true);

      // Preserve the mock RM instances for secondaries
      ConcurrentHashMap<String, MockResourceManagerFacade> secondaries =
          interceptor.getSecondaryRMs();

      // Increase the attemptId and create a new interceptor instance for it
      attemptId = ApplicationAttemptId.newInstance(
          attemptId.getApplicationId(), attemptId.getAttemptId() + 1);

      interceptor = new TestableFederationInterceptor(null, secondaries);
      interceptor.init(new AMRMProxyApplicationContextImpl(nmContext,
          getConf(), attemptId, "test-user", null, null, null, registry));
      return null;
    });

    // Update the ugi with new attemptId
    ugi = interceptor.getUGIWithToken(interceptor.getAttemptId());
    ugi.doAs((PrivilegedExceptionAction<Object>) () -> {
      RegisterApplicationMasterResponse registerResponse =
          interceptor.registerApplicationMaster(registerReq);
      lastResponseId = 0;

      int numberOfContainers = 3;
      // Should re-attach secondaries and get the three running containers
      Assert.assertEquals(1, interceptor.getUnmanagedAMPoolSize());
      // SC1 should be initialized to be timed out
      Assert.assertEquals(1, interceptor.getTimedOutSCs(true).size());
      Assert.assertEquals(numberOfContainers,
          registerResponse.getContainersFromPreviousAttempts().size());

      // Release all containers
      releaseContainersAndAssert(
          registerResponse.getContainersFromPreviousAttempts());

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

      // After the application succeeds, the registry entry should be deleted
      if (interceptor.getRegistryClient() != null) {
        Assert.assertEquals(0,
            interceptor.getRegistryClient().getAllApplications().size());
      }
      return null;
    });
  }

  @Test
  public void testMergeAllocateResponse() {
    ContainerId cid = ContainerId.newContainerId(attemptId, 0);
    ContainerStatus cStatus = Records.newRecord(ContainerStatus.class);
    cStatus.setContainerId(cid);
    Container container =
        Container.newInstance(cid, null, null, null, null, null);

    AllocateResponse homeResponse = Records.newRecord(AllocateResponse.class);
    homeResponse.setAllocatedContainers(Collections.singletonList(container));
    homeResponse.setCompletedContainersStatuses(Collections.singletonList(cStatus));
    homeResponse.setUpdatedNodes(Collections.singletonList(Records.newRecord(NodeReport.class)));
    homeResponse.setNMTokens(Collections.singletonList(Records.newRecord(NMToken.class)));
    homeResponse.setUpdatedContainers(Collections.singletonList(
        Records.newRecord(UpdatedContainer.class)));
    homeResponse.setUpdateErrors(Collections.singletonList(
        Records.newRecord(UpdateContainerError.class)));
    homeResponse.setAvailableResources(Records.newRecord(Resource.class));
    homeResponse.setPreemptionMessage(createDummyPreemptionMessage(
        ContainerId.newContainerId(attemptId, 0)));

    AllocateResponse response = Records.newRecord(AllocateResponse.class);
    response.setAllocatedContainers(Collections.singletonList(container));
    response.setCompletedContainersStatuses(Collections.singletonList(cStatus));
    response.setUpdatedNodes(Collections.singletonList(Records.newRecord(NodeReport.class)));
    response.setNMTokens(Collections.singletonList(Records.newRecord(NMToken.class)));
    response.setUpdatedContainers(Collections.singletonList(
        Records.newRecord(UpdatedContainer.class)));
    response.setUpdateErrors(Collections.singletonList(
        Records.newRecord(UpdateContainerError.class)));
    response.setAvailableResources(Records.newRecord(Resource.class));
    response.setPreemptionMessage(createDummyPreemptionMessage(
        ContainerId.newContainerId(attemptId, 1)));

    interceptor.mergeAllocateResponse(homeResponse,
        response, SubClusterId.newInstance("SC-1"));

    Assert.assertEquals(2,
        homeResponse.getPreemptionMessage().getContract().getContainers().size());
    Assert.assertEquals(2, homeResponse.getAllocatedContainers().size());
    Assert.assertEquals(2, homeResponse.getUpdatedNodes().size());
    Assert.assertEquals(2, homeResponse.getCompletedContainersStatuses().size());
  }

  private PreemptionMessage createDummyPreemptionMessage(
      ContainerId containerId) {
    PreemptionMessage preemptionMessage = Records.newRecord(
        PreemptionMessage.class);
    PreemptionContainer container = Records.newRecord(
        PreemptionContainer.class);
    container.setId(containerId);
    Set<PreemptionContainer> preemptionContainers = new HashSet<>();
    preemptionContainers.add(container);
    PreemptionContract contract = Records.newRecord(PreemptionContract.class);
    contract.setContainers(preemptionContainers);
    preemptionMessage.setContract(contract);
    return preemptionMessage;
  }

  @Test
  public void testSameContainerFromDiffRM() throws IOException, InterruptedException {

    UserGroupInformation ugi =
        interceptor.getUGIWithToken(interceptor.getAttemptId());

    ugi.doAs((PrivilegedExceptionAction<Object>) () -> {

      // Register the application
      RegisterApplicationMasterRequest registerReq =
          Records.newRecord(RegisterApplicationMasterRequest.class);
      registerReq.setHost(Integer.toString(testAppId));
      registerReq.setRpcPort(0);
      registerReq.setTrackingUrl("");

      RegisterApplicationMasterResponse registerResponse =
          interceptor.registerApplicationMaster(registerReq);
      Assert.assertNotNull(registerResponse);
      lastResponseId = 0;

      Assert.assertEquals(0, interceptor.getUnmanagedAMPoolSize());

      // Allocate the first batch of containers, with sc1 active
      SubClusterId subClusterId1 = SubClusterId.newInstance("SC-1");
      registerSubCluster(subClusterId1);

      int numberOfContainers = 3;
      List<Container> containers =
          getContainersAndAssert(numberOfContainers, numberOfContainers);
      Assert.assertNotNull(containers);
      Assert.assertEquals(3, containers.size());

      // with sc2 active
      SubClusterId subClusterId2 = SubClusterId.newInstance("SC-2");
      registerSubCluster(subClusterId2);

      // 1.Container has been registered to SubCluster1, try to register the same Container
      // to SubCluster2.
      // Because SubCluster1 is in normal state at this time,
      // So the SubCluster corresponding to Container should be SubCluster1
      interceptor.cacheAllocatedContainersForSubClusterId(containers, subClusterId2);
      Map<ContainerId, SubClusterId> cIdToSCMap = interceptor.getContainerIdToSubClusterIdMap();
      for (SubClusterId subClusterId : cIdToSCMap.values()) {
        Assert.assertNotNull(subClusterId);
        Assert.assertEquals(subClusterId1, subClusterId);
      }

      // 2.Deregister SubCluster1, Register the same Containers to SubCluster2
      // So the SubCluster corresponding to Container should be SubCluster2
      deRegisterSubCluster(subClusterId1);
      interceptor.cacheAllocatedContainersForSubClusterId(containers, subClusterId2);
      Map<ContainerId, SubClusterId> cIdToSCMap2 = interceptor.getContainerIdToSubClusterIdMap();
      for (SubClusterId subClusterId : cIdToSCMap2.values()) {
        Assert.assertNotNull(subClusterId);
        Assert.assertEquals(subClusterId2, subClusterId);
      }

      // 3.Deregister subClusterId2, Register the same Containers to SubCluster1
      // Because both SubCluster1 and SubCluster2 are abnormal at this time,
      // an exception will be thrown when registering the first Container.
      deRegisterSubCluster(subClusterId2);
      Container container1 = containers.get(0);
      Assert.assertNotNull(container1);
      String errMsg =
          " Can't use any subCluster because an exception occurred" +
          " ContainerId: " + container1.getId() +
          " ApplicationId: " + interceptor.getAttemptId() +
          " From RM: " + subClusterId1 + ". " +
          " Previous Container was From subCluster: " + subClusterId2;

      LambdaTestUtils.intercept(YarnRuntimeException.class, errMsg,
          () -> interceptor.cacheAllocatedContainersForSubClusterId(containers, subClusterId1));

      // 4. register SubCluster1, re-register the Container,
      // and try to finish application
      registerSubCluster(subClusterId1);
      interceptor.cacheAllocatedContainersForSubClusterId(containers, subClusterId1);
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

      return null;
    });
  }

  @Test
  public void testBatchFinishApplicationMaster() throws IOException, InterruptedException {

    final RegisterApplicationMasterRequest registerReq =
        Records.newRecord(RegisterApplicationMasterRequest.class);
    registerReq.setHost(Integer.toString(testAppId));
    registerReq.setRpcPort(testAppId);
    registerReq.setTrackingUrl("");

    UserGroupInformation ugi = interceptor.getUGIWithToken(interceptor.getAttemptId());

    ugi.doAs((PrivilegedExceptionAction<Object>) () -> {

      // Register the application
      RegisterApplicationMasterRequest registerReq1 =
          Records.newRecord(RegisterApplicationMasterRequest.class);
      registerReq1.setHost(Integer.toString(testAppId));
      registerReq1.setRpcPort(0);
      registerReq1.setTrackingUrl("");

      // Register ApplicationMaster
      RegisterApplicationMasterResponse registerResponse =
          interceptor.registerApplicationMaster(registerReq1);
      Assert.assertNotNull(registerResponse);
      lastResponseId = 0;

      Assert.assertEquals(0, interceptor.getUnmanagedAMPoolSize());

      // Allocate the first batch of containers, with sc1 and sc2 active
      registerSubCluster(SubClusterId.newInstance("SC-1"));
      registerSubCluster(SubClusterId.newInstance("SC-2"));

      int numberOfContainers = 3;
      List<Container> containers =
          getContainersAndAssert(numberOfContainers, numberOfContainers * 2);
      Assert.assertEquals(2, interceptor.getUnmanagedAMPoolSize());
      Assert.assertEquals(numberOfContainers * 2, containers.size());

      // Finish the application
      FinishApplicationMasterRequest finishReq =
          Records.newRecord(FinishApplicationMasterRequest.class);
      finishReq.setDiagnostics("");
      finishReq.setTrackingUrl("");
      finishReq.setFinalApplicationStatus(FinalApplicationStatus.SUCCEEDED);

      FinishApplicationMasterResponse finishResp = interceptor.finishApplicationMaster(finishReq);
      Assert.assertNotNull(finishResp);
      Assert.assertTrue(finishResp.getIsUnregistered());

      return null;
    });
  }

  @Test
  public void testRemoveAppFromRegistryApplicationSuccess()
      throws IOException, InterruptedException {

    final RegisterApplicationMasterRequest registerReq =
        Records.newRecord(RegisterApplicationMasterRequest.class);
    registerReq.setHost(Integer.toString(testAppId));
    registerReq.setRpcPort(testAppId);
    registerReq.setTrackingUrl("");

    UserGroupInformation ugi = interceptor.getUGIWithToken(interceptor.getAttemptId());

    ugi.doAs((PrivilegedExceptionAction<Object>) () -> {

      // Register the application
      RegisterApplicationMasterRequest registerReq1 =
          Records.newRecord(RegisterApplicationMasterRequest.class);
      registerReq1.setHost(Integer.toString(testAppId));
      registerReq1.setRpcPort(0);
      registerReq1.setTrackingUrl("");

      // Register ApplicationMaster
      RegisterApplicationMasterResponse registerResponse =
          interceptor.registerApplicationMaster(registerReq1);
      Assert.assertNotNull(registerResponse);
      lastResponseId = 0;

      Assert.assertEquals(0, interceptor.getUnmanagedAMPoolSize());

      // Allocate the first batch of containers, with sc1 active
      registerSubCluster(SubClusterId.newInstance("SC-1"));

      int numberOfContainers = 3;
      List<Container> containers =
          getContainersAndAssert(numberOfContainers, numberOfContainers);
      Assert.assertEquals(1, interceptor.getUnmanagedAMPoolSize());
      Assert.assertEquals(numberOfContainers, containers.size());

      // Finish the application
      FinishApplicationMasterRequest finishReq =
          Records.newRecord(FinishApplicationMasterRequest.class);
      finishReq.setDiagnostics("");
      finishReq.setTrackingUrl("");
      finishReq.setFinalApplicationStatus(FinalApplicationStatus.SUCCEEDED);

      FinishApplicationMasterResponse finishResp = interceptor.finishApplicationMaster(finishReq);
      Assert.assertNotNull(finishResp);
      Assert.assertTrue(finishResp.getIsUnregistered());

      FederationRegistryClient client = interceptor.getRegistryClient();
      List<String> applications = client.getAllApplications();
      Assert.assertNotNull(finishResp);
      Assert.assertEquals(0, applications.size());
      return null;
    });
  }

  @Test
  public void testRemoveAppFromRegistryApplicationFailed()
      throws IOException, InterruptedException {

    final RegisterApplicationMasterRequest registerReq =
        Records.newRecord(RegisterApplicationMasterRequest.class);

    registerReq.setHost(Integer.toString(testAppId));
    registerReq.setRpcPort(testAppId);
    registerReq.setTrackingUrl("");

    UserGroupInformation ugi = interceptor.getUGIWithToken(interceptor.getAttemptId());

    ugi.doAs((PrivilegedExceptionAction<Object>) () -> {

      // Register the application
      RegisterApplicationMasterRequest registerReq1 =
          Records.newRecord(RegisterApplicationMasterRequest.class);
      registerReq1.setHost(Integer.toString(testAppId));
      registerReq1.setRpcPort(0);
      registerReq1.setTrackingUrl("");

      // Register ApplicationMaster
      RegisterApplicationMasterResponse registerResponse =
          interceptor.registerApplicationMaster(registerReq1);
      Assert.assertNotNull(registerResponse);
      lastResponseId = 0;

      Assert.assertEquals(0, interceptor.getUnmanagedAMPoolSize());

      // Allocate the first batch of containers, with sc1 active
      registerSubCluster(SubClusterId.newInstance("SC-1"));

      int numberOfContainers = 3;
      List<Container> containers =
          getContainersAndAssert(numberOfContainers, numberOfContainers);
      Assert.assertEquals(1, interceptor.getUnmanagedAMPoolSize());
      Assert.assertEquals(numberOfContainers, containers.size());

      // Finish the application
      FinishApplicationMasterRequest finishReq =
          Records.newRecord(FinishApplicationMasterRequest.class);
      finishReq.setDiagnostics("");
      finishReq.setTrackingUrl("");
      finishReq.setFinalApplicationStatus(FinalApplicationStatus.FAILED);

      // Check Registry Applications
      // At this time, the Application should not be cleaned up because the state is not SUCCESS.
      FederationRegistryClient client = interceptor.getRegistryClient();
      List<String> applications = client.getAllApplications();
      Assert.assertNotNull(applications);
      Assert.assertEquals(1, applications.size());

      // interceptor cleanupRegistry
      ApplicationId applicationId = interceptor.getAttemptId().getApplicationId();
      client.removeAppFromRegistry(applicationId);
      applications = client.getAllApplications();
      Assert.assertNotNull(applications);
      Assert.assertEquals(0, applications.size());

      return null;
    });
  }

  public void testRecoverWithBadSubCluster(final RegistryOperations registryObj)
      throws IOException, InterruptedException {

    UserGroupInformation ugi =
        interceptor.getUGIWithToken(interceptor.getAttemptId());

    // Prepare a list of subclusters
    List<SubClusterId> subClusterIds = new ArrayList<>();
    SubClusterId sc1 = SubClusterId.newInstance("SC-1");
    SubClusterId sc2 = SubClusterId.newInstance("SC-2");
    SubClusterId homeSC = SubClusterId.newInstance(HOME_SC_ID);
    subClusterIds.add(sc1);
    subClusterIds.add(sc2);
    subClusterIds.add(homeSC);

    // Prepare AMRMProxy Context
    AMRMProxyApplicationContext appContext = new AMRMProxyApplicationContextImpl(nmContext,
        getConf(), attemptId, "test-user", null, null, null, registryObj);

    // Prepare RegisterApplicationMasterRequest
    RegisterApplicationMasterRequest registerReq =
        Records.newRecord(RegisterApplicationMasterRequest.class);
    registerReq.setHost(Integer.toString(testAppId));
    registerReq.setRpcPort(testAppId);
    registerReq.setTrackingUrl("");

    ugi.doAs((PrivilegedExceptionAction<Object>) () -> {

      // Step1. Prepare subClusters SC-1, SC-2, HomeSC and Interceptor
      initSubClusterAndInterceptor(subClusterIds, registryObj);

      // Step2. Register Application And Assign Containers
      List<Container> containers = registerApplicationAndAssignContainers(registerReq);

      // Step3. Offline SC-1 cluster
      offlineSubClusterSC1(sc1);

      // Step4. Recover ApplicationMaster
      recoverApplicationMaster(appContext);

      // Step5. We recovered ApplicationMaster.
      // SC-1 was offline, SC-2 was recovered at this time, UnmanagedAMPool.size=1 and only SC-2
      UnmanagedAMPoolManager unmanagedAMPoolManager = interceptor.getUnmanagedAMPool();
      Set<String> allUAMIds = unmanagedAMPoolManager.getAllUAMIds();
      Assert.assertNotNull(allUAMIds);
      Assert.assertEquals(1, allUAMIds.size());
      Assert.assertTrue(allUAMIds.contains(sc2.getId()));

      // Step6. The first allocate call expects a fail-over exception and re-register.
      AllocateRequest allocateRequest = Records.newRecord(AllocateRequest.class);
      allocateRequest.setResponseId(0);
      LambdaTestUtils.intercept(ApplicationMasterNotRegisteredException.class,
          "AMRMProxy just restarted and recovered for " + this.attemptId +
          ". AM should re-register and full re-send pending requests.",
          () -> interceptor.allocate(allocateRequest));
      interceptor.registerApplicationMaster(registerReq);

      // Step7. release Containers
      releaseContainers(containers, sc1);

      // Step8. finish application
      finishApplication();

      return null;
    });
  }

  private void initSubClusterAndInterceptor(List<SubClusterId> subClusterIds,
      RegistryOperations registryObj) throws YarnException {
    // Prepare subClusters SC-1, SC-2, HomeSC
    for (SubClusterId subClusterId : subClusterIds) {
      registerSubCluster(subClusterId);
    }

    // Prepare Interceptor
    interceptor = new TestableFederationInterceptor();
    AMRMProxyApplicationContext appContext = new AMRMProxyApplicationContextImpl(nmContext,
        getConf(), attemptId, "test-user", null, null, null, registryObj);
    interceptor.init(appContext);
    interceptor.cleanupRegistry();
  }

  private List<Container> registerApplicationAndAssignContainers(
      RegisterApplicationMasterRequest registerReq) throws Exception {

    // Register HomeSC
    RegisterApplicationMasterResponse registerResponse =
        interceptor.registerApplicationMaster(registerReq);
    Assert.assertNotNull(registerResponse);

    // We only registered HomeSC, so UnmanagedAMPoolSize should be empty
    Assert.assertEquals(0, interceptor.getUnmanagedAMPoolSize());

    // We assign 3 Containers to each cluster
    int numberOfContainers = 3;
    List<Container> containers =
        getContainersAndAssert(numberOfContainers, numberOfContainers * 3);

    // At this point, UnmanagedAMPoolSize should be equal to 2 and should contain SC-1, SC-2
    Assert.assertEquals(2, interceptor.getUnmanagedAMPoolSize());
    UnmanagedAMPoolManager unmanagedAMPoolManager = interceptor.getUnmanagedAMPool();
    Set<String> allUAMIds = unmanagedAMPoolManager.getAllUAMIds();
    Assert.assertNotNull(allUAMIds);
    Assert.assertEquals(2, allUAMIds.size());
    Assert.assertTrue(allUAMIds.contains("SC-1"));
    Assert.assertTrue(allUAMIds.contains("SC-2"));

    // Make sure all async hb threads are done
    interceptor.drainAllAsyncQueue(true);

    return containers;
  }

  private void offlineSubClusterSC1(SubClusterId subClusterId) throws YarnException {

    ConcurrentHashMap<String, MockResourceManagerFacade> secondaries =
        interceptor.getSecondaryRMs();

    // SC-1 out of service
    deRegisterSubCluster(subClusterId);
    secondaries.get(subClusterId.getId()).setRunningMode(false);
  }

  private void recoverApplicationMaster(AMRMProxyApplicationContext appContext)
      throws IOException {
    // Prepare for Federation Interceptor restart and recover
    Map<String, byte[]> recoveredDataMap =
        recoverDataMapForAppAttempt(nmStateStore, attemptId);

    // Preserve the mock RM instances
    MockResourceManagerFacade homeRM = interceptor.getHomeRM();

    // Create a new interceptor instance and recover
    interceptor = new TestableFederationInterceptor(homeRM,
        interceptor.getSecondaryRMs());
    interceptor.init(appContext);
    interceptor.recover(recoveredDataMap);
  }

  private void releaseContainers(List<Container> containers, SubClusterId subClusterId)
      throws Exception {

    ConcurrentHashMap<String, MockResourceManagerFacade> secondaries =
        interceptor.getSecondaryRMs();
    lastResponseId = 0;

    // Get the Container list of SC-1
    MockResourceManagerFacade sc1Facade = secondaries.get("SC-1");
    HashMap<ApplicationId, List<ContainerId>> appContainerMap =
        sc1Facade.getApplicationContainerIdMap();
    Assert.assertNotNull(appContainerMap);
    ApplicationId applicationId = attemptId.getApplicationId();
    Assert.assertNotNull(applicationId);
    List<ContainerId> sc1ContainerList = appContainerMap.get(applicationId);

    // Release all containers,
    // Because SC-1 is offline, it is necessary to clean up the Containers allocated by SC-1
    containers = containers.stream()
        .filter(container -> !sc1ContainerList.contains(container.getId()))
        .collect(Collectors.toList());
    releaseContainersAndAssert(containers);
  }

  private void finishApplication() throws IOException, YarnException {
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
  }

  @Test
  public void testLaunchUAMAndRegisterApplicationMasterRetry() throws Exception {

    UserGroupInformation ugi = interceptor.getUGIWithToken(interceptor.getAttemptId());
    interceptor.setRetryCount(2);

    ugi.doAs((PrivilegedExceptionAction<Object>) () -> {
      // Register the application
      RegisterApplicationMasterRequest registerReq =
          Records.newRecord(RegisterApplicationMasterRequest.class);
      registerReq.setHost(Integer.toString(testAppId));
      registerReq.setRpcPort(0);
      registerReq.setTrackingUrl("");

      RegisterApplicationMasterResponse registerResponse =
          interceptor.registerApplicationMaster(registerReq);
      Assert.assertNotNull(registerResponse);
      lastResponseId = 0;

      Assert.assertEquals(0, interceptor.getUnmanagedAMPoolSize());

      // Allocate the first batch of containers, with sc1 active
      registerSubCluster(SubClusterId.newInstance("SC-1"));

      int numberOfContainers = 3;
      List<Container> containers = getContainersAndAssert(numberOfContainers, numberOfContainers);
      Assert.assertEquals(1, interceptor.getUnmanagedAMPoolSize());

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

      return null;
    });

    Assert.assertEquals(0, interceptor.getRetryCount());
  }
}
