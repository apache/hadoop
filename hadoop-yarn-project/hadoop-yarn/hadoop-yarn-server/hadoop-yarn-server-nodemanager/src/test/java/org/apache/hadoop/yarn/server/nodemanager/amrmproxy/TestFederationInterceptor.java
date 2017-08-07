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
import java.util.ArrayList;
import java.util.List;

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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.InvalidApplicationMasterRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.manager.UniformBroadcastPolicyManager;
import org.apache.hadoop.yarn.server.federation.store.impl.MemoryFederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
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

    testAppId = 1;
    attemptId = getApplicationAttemptId(testAppId);
    interceptor.init(new AMRMProxyApplicationContextImpl(null, getConf(),
        attemptId, "test-user", null, null));
  }

  @Override
  public void tearDown() {
    interceptor.shutdown();
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

    // The way the mock resource manager is setup, it will return the containers
    // that were released in the allocated containers. The release request will
    // be split and handled by the corresponding UAM. The release containers
    // returned by the mock resource managers will be aggregated and returned
    // back to us and we can check if total request size and returned size are
    // the same
    List<Container> containersForReleasedContainerIds =
        new ArrayList<Container>();
    containersForReleasedContainerIds
        .addAll(allocateResponse.getAllocatedContainers());
    LOG.info("Number of containers received in the original request: "
        + Integer.toString(allocateResponse.getAllocatedContainers().size()));

    // Send max 10 heart beats to receive all the containers. If not, we will
    // fail the test
    int numHeartbeat = 0;
    while (containersForReleasedContainerIds.size() < relList.size()
        && numHeartbeat++ < 10) {
      allocateResponse =
          interceptor.allocate(Records.newRecord(AllocateRequest.class));
      Assert.assertNotNull(allocateResponse);
      containersForReleasedContainerIds
          .addAll(allocateResponse.getAllocatedContainers());

      LOG.info("Number of containers received in this request: "
          + Integer.toString(allocateResponse.getAllocatedContainers().size()));
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
    registerReq.setRpcPort(testAppId);
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
    registerReq.setRpcPort(testAppId);
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
    registerReq.setRpcPort(testAppId);
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
    registerReq.setRpcPort(testAppId);
    registerReq.setTrackingUrl("");

    RegisterApplicationMasterResponse registerResponse =
        interceptor.registerApplicationMaster(registerReq);
    Assert.assertNotNull(registerResponse);

    // Register the application second time with a different request obj
    registerReq = Records.newRecord(RegisterApplicationMasterRequest.class);
    registerReq.setHost(Integer.toString(testAppId));
    registerReq.setRpcPort(testAppId);
    registerReq.setTrackingUrl("different");
    try {
      registerResponse = interceptor.registerApplicationMaster(registerReq);
      Assert.fail("Should throw if a different request obj is used");
    } catch (YarnException e) {
    }
  }
}
