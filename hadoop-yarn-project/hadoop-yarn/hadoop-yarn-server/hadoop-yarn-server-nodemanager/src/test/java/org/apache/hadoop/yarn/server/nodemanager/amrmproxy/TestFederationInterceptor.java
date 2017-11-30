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
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.impl.FSRegistryOperationsService;
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
import org.apache.hadoop.yarn.api.records.PreemptionMessage;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.UpdateContainerError;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.InvalidApplicationMasterRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MockResourceManagerFacade;
import org.apache.hadoop.yarn.server.federation.policies.manager.UniformBroadcastPolicyManager;
import org.apache.hadoop.yarn.server.federation.store.impl.MemoryFederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterRequest;
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
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extends the TestAMRMProxyService and overrides methods in order to use the
 * AMRMProxyService's pipeline test cases for testing the FederationInterceptor
 * class. The tests for AMRMProxyService has been written cleverly so that it
 * can be reused to validate different request intercepter chains.
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
    nmContext =
        new NMContext(null, null, null, null, nmStateStore, false, getConf());
    interceptor.init(new AMRMProxyApplicationContextImpl(nmContext, getConf(),
        attemptId, "test-user", null, null, null, registry));
    interceptor.cleanupRegistry();
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

    // Create a request intercepter pipeline for testing. The last one in the
    // chain is the federation intercepter that calls the mock resource manager.
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
    allocateRequest.setResponseId(1);

    List<Container> containers =
        new ArrayList<Container>(numberOfResourceRequests);
    List<ResourceRequest> askList =
        new ArrayList<ResourceRequest>(numberOfResourceRequests);
    for (int id = 0; id < numberOfResourceRequests; id++) {
      askList.add(createResourceRequest("test-node-" + Integer.toString(id),
          6000, 2, id % 5, 1));
    }

    allocateRequest.setAskList(askList);

    AllocateResponse allocateResponse = interceptor.allocate(allocateRequest);
    Assert.assertNotNull("allocate() returned null response", allocateResponse);

    containers.addAll(allocateResponse.getAllocatedContainers());
    LOG.info("Number of allocated containers in the original request: "
        + Integer.toString(allocateResponse.getAllocatedContainers().size()));

    // Send max 10 heart beats to receive all the containers. If not, we will
    // fail the test
    int numHeartbeat = 0;
    while (containers.size() < numberOfAllocationExcepted
        && numHeartbeat++ < 10) {
      allocateResponse =
          interceptor.allocate(Records.newRecord(AllocateRequest.class));
      Assert.assertNotNull("allocate() returned null response",
          allocateResponse);

      containers.addAll(allocateResponse.getAllocatedContainers());

      LOG.info("Number of allocated containers in this request: "
          + Integer.toString(allocateResponse.getAllocatedContainers().size()));
      LOG.info("Total number of allocated containers: "
          + Integer.toString(containers.size()));
      Thread.sleep(10);
    }
    Assert.assertEquals(numberOfAllocationExcepted, containers.size());
    return containers;
  }

  private void releaseContainersAndAssert(List<Container> containers)
      throws Exception {
    Assert.assertTrue(containers.size() > 0);
    AllocateRequest allocateRequest = Records.newRecord(AllocateRequest.class);
    allocateRequest.setResponseId(1);

    List<ContainerId> relList = new ArrayList<ContainerId>(containers.size());
    for (Container container : containers) {
      relList.add(container.getId());
    }

    allocateRequest.setReleaseList(relList);

    AllocateResponse allocateResponse = interceptor.allocate(allocateRequest);
    Assert.assertNotNull(allocateResponse);

    // The release request will be split and handled by the corresponding UAM.
    // The release containers returned by the mock resource managers will be
    // aggregated and returned back to us and we can check if total request size
    // and returned size are the same
    List<ContainerId> containersForReleasedContainerIds =
        new ArrayList<ContainerId>();
    List<ContainerId> newlyFinished = getCompletedContainerIds(
        allocateResponse.getCompletedContainersStatuses());
    containersForReleasedContainerIds.addAll(newlyFinished);
    LOG.info("Number of containers received in the original request: "
        + Integer.toString(newlyFinished.size()));

    // Send max 10 heart beats to receive all the containers. If not, we will
    // fail the test
    int numHeartbeat = 0;
    while (containersForReleasedContainerIds.size() < relList.size()
        && numHeartbeat++ < 10) {
      allocateResponse =
          interceptor.allocate(Records.newRecord(AllocateRequest.class));
      Assert.assertNotNull(allocateResponse);
      newlyFinished = getCompletedContainerIds(
          allocateResponse.getCompletedContainersStatuses());
      containersForReleasedContainerIds.addAll(newlyFinished);

      LOG.info("Number of containers received in this request: "
          + Integer.toString(newlyFinished.size()));
      LOG.info("Total number of containers received: "
          + Integer.toString(containersForReleasedContainerIds.size()));
      Thread.sleep(10);
    }

    Assert.assertEquals(relList.size(),
        containersForReleasedContainerIds.size());
  }

  @Test
  public void testMultipleSubClusters() throws Exception {

    // Register the application
    RegisterApplicationMasterRequest registerReq =
        Records.newRecord(RegisterApplicationMasterRequest.class);
    registerReq.setHost(Integer.toString(testAppId));
    registerReq.setRpcPort(0);
    registerReq.setTrackingUrl("");

    RegisterApplicationMasterResponse registerResponse =
        interceptor.registerApplicationMaster(registerReq);
    Assert.assertNotNull(registerResponse);

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
        getContainersAndAssert(numberOfContainers, numberOfContainers * 1));
    Assert.assertEquals(3, interceptor.getUnmanagedAMPoolSize());

    // Release all containers
    releaseContainersAndAssert(containers);

    // Finish the application
    FinishApplicationMasterRequest finishReq =
        Records.newRecord(FinishApplicationMasterRequest.class);
    finishReq.setDiagnostics("");
    finishReq.setTrackingUrl("");
    finishReq.setFinalApplicationStatus(FinalApplicationStatus.SUCCEEDED);

    FinishApplicationMasterResponse finshResponse =
        interceptor.finishApplicationMaster(finishReq);
    Assert.assertNotNull(finshResponse);
    Assert.assertEquals(true, finshResponse.getIsUnregistered());
  }

  /*
   * Test re-register when RM fails over.
   */
  @Test
  public void testReregister() throws Exception {

    // Register the application
    RegisterApplicationMasterRequest registerReq =
        Records.newRecord(RegisterApplicationMasterRequest.class);
    registerReq.setHost(Integer.toString(testAppId));
    registerReq.setRpcPort(0);
    registerReq.setTrackingUrl("");

    RegisterApplicationMasterResponse registerResponse =
        interceptor.registerApplicationMaster(registerReq);
    Assert.assertNotNull(registerResponse);

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

    FinishApplicationMasterResponse finshResponse =
        interceptor.finishApplicationMaster(finishReq);
    Assert.assertNotNull(finshResponse);
    Assert.assertEquals(true, finshResponse.getIsUnregistered());
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

    Object syncObj = MockResourceManagerFacade.getSyncObj();

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
      RegisterApplicationMasterResponse response = null;
      try {
        // Use port number 1001 to let mock RM block in the register call
        response = interceptor.registerApplicationMaster(
            RegisterApplicationMasterRequest.newInstance(null, 1001, null));
      } catch (Exception e) {
        LOG.info("Register thread exception", e);
        response = null;
      }
      return response;
    }
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

    // Register the application second time with a different request obj
    registerReq = Records.newRecord(RegisterApplicationMasterRequest.class);
    registerReq.setHost(Integer.toString(testAppId));
    registerReq.setRpcPort(0);
    registerReq.setTrackingUrl("different");
    try {
      registerResponse = interceptor.registerApplicationMaster(registerReq);
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
  }

  @Test
  public void testSecondAttempt() throws Exception {
    ApplicationUserInfo userInfo = getApplicationUserInfo(testAppId);
    userInfo.getUser().doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        // Register the application
        RegisterApplicationMasterRequest registerReq =
            Records.newRecord(RegisterApplicationMasterRequest.class);
        registerReq.setHost(Integer.toString(testAppId));
        registerReq.setRpcPort(testAppId);
        registerReq.setTrackingUrl("");

        RegisterApplicationMasterResponse registerResponse =
            interceptor.registerApplicationMaster(registerReq);
        Assert.assertNotNull(registerResponse);

        Assert.assertEquals(0, interceptor.getUnmanagedAMPoolSize());

        // Allocate one batch of containers
        registerSubCluster(SubClusterId.newInstance("SC-1"));
        registerSubCluster(SubClusterId.newInstance(HOME_SC_ID));

        int numberOfContainers = 3;
        List<Container> containers =
            getContainersAndAssert(numberOfContainers, numberOfContainers * 2);
        for (Container c : containers) {
          System.out.println(c.getId() + " ha");
        }
        Assert.assertEquals(1, interceptor.getUnmanagedAMPoolSize());

        // Preserve the mock RM instances for secondaries
        ConcurrentHashMap<String, MockResourceManagerFacade> secondaries =
            interceptor.getSecondaryRMs();

        // Increase the attemptId and create a new intercepter instance for it
        attemptId = ApplicationAttemptId.newInstance(
            attemptId.getApplicationId(), attemptId.getAttemptId() + 1);

        interceptor = new TestableFederationInterceptor(null, secondaries);
        interceptor.init(new AMRMProxyApplicationContextImpl(nmContext,
            getConf(), attemptId, "test-user", null, null, null, registry));
        registerResponse = interceptor.registerApplicationMaster(registerReq);

        // Should re-attach secondaries and get the three running containers
        Assert.assertEquals(1, interceptor.getUnmanagedAMPoolSize());
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

        FinishApplicationMasterResponse finshResponse =
            interceptor.finishApplicationMaster(finishReq);
        Assert.assertNotNull(finshResponse);
        Assert.assertEquals(true, finshResponse.getIsUnregistered());
        return null;
      }
    });
  }

}
