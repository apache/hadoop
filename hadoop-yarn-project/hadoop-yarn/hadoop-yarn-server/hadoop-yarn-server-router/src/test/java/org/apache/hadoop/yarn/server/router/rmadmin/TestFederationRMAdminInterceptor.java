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

package org.apache.hadoop.yarn.server.router.rmadmin;

import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.yarn.api.records.DecommissionType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesRequest;
import org.apache.hadoop.yarn.server.federation.store.impl.MemoryFederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreTestUtil;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Extends the FederationRMAdminInterceptor and overrides methods to provide a
 * testable implementation of FederationRMAdminInterceptor.
 */
public class TestFederationRMAdminInterceptor extends BaseRouterRMAdminTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestFederationRMAdminInterceptor.class);

  ////////////////////////////////
  // constant information
  ////////////////////////////////
  private final static String USER_NAME = "test-user";
  private final static int NUM_SUBCLUSTER = 4;

  private TestableFederationRMAdminInterceptor interceptor;
  private FederationStateStoreFacade facade;
  private MemoryFederationStateStore stateStore;
  private FederationStateStoreTestUtil stateStoreUtil;
  private List<SubClusterId> subClusters;

  @Override
  public void setUp() {

    super.setUpConfig();

    // Initialize facade & stateSore
    stateStore = new MemoryFederationStateStore();
    stateStore.init(this.getConf());
    facade = FederationStateStoreFacade.getInstance();
    facade.reinitialize(stateStore, getConf());
    stateStoreUtil = new FederationStateStoreTestUtil(stateStore);

    // Initialize interceptor
    interceptor = new TestableFederationRMAdminInterceptor();
    interceptor.setConf(this.getConf());
    interceptor.init(USER_NAME);

    // Storage SubClusters
    subClusters = new ArrayList<>();
    try {
      for (int i = 0; i < NUM_SUBCLUSTER; i++) {
        SubClusterId sc = SubClusterId.newInstance("SC-" + i);
        stateStoreUtil.registerSubCluster(sc);
        subClusters.add(sc);
      }
    } catch (YarnException e) {
      LOG.error(e.getMessage());
      Assert.fail();
    }

    DefaultMetricsSystem.setMiniClusterMode(true);
  }

  @Override
  protected YarnConfiguration createConfiguration() {
    // Set Enable YarnFederation
    YarnConfiguration config = new YarnConfiguration();
    config.setBoolean(YarnConfiguration.FEDERATION_ENABLED, true);

    String mockPassThroughInterceptorClass =
        PassThroughRMAdminRequestInterceptor.class.getName();

    // Create a request interceptor pipeline for testing. The last one in the
    // chain will call the mock resource manager. The others in the chain will
    // simply forward it to the next one in the chain
    config.set(YarnConfiguration.ROUTER_RMADMIN_INTERCEPTOR_CLASS_PIPELINE,
        mockPassThroughInterceptorClass + "," + mockPassThroughInterceptorClass + "," +
        TestFederationRMAdminInterceptor.class.getName());
    return config;
  }

  @Override
  public void tearDown() {
    interceptor.shutdown();
    super.tearDown();
  }

  @Test
  public void testRefreshQueues() throws Exception {
    // We will test 2 cases:
    // case 1, request is null.
    // case 2, normal request.
    // If the request is null, a Missing RefreshQueues request exception will be thrown.

    // null request.
    LambdaTestUtils.intercept(YarnException.class, "Missing RefreshQueues request.",
        () -> interceptor.refreshQueues(null));

    // normal request.
    RefreshQueuesRequest request = RefreshQueuesRequest.newInstance();
    interceptor.refreshQueues(request);
  }

  @Test
  public void testSC1RefreshQueues() throws Exception {
    // We will test 2 cases:
    // case 1, test the existing subCluster (SC-1).
    // case 2, test the non-exist subCluster.

    String existSubCluster = "SC-1";
    RefreshQueuesRequest request = RefreshQueuesRequest.newInstance(existSubCluster);
    interceptor.refreshQueues(request);

    String notExistsSubCluster = "SC-NON";
    RefreshQueuesRequest request1 = RefreshQueuesRequest.newInstance(notExistsSubCluster);
    LambdaTestUtils.intercept(YarnException.class,
        "subClusterId = SC-NON is not an active subCluster.",
        () -> interceptor.refreshQueues(request1));
  }

  @Test
  public void testRefreshNodes() throws Exception {
    // We will test 2 cases:
    // case 1, request is null.
    // case 2, normal request.
    // If the request is null, a Missing RefreshNodes request exception will be thrown.

    // null request.
    LambdaTestUtils.intercept(YarnException.class,
        "Missing RefreshNodes request.", () -> interceptor.refreshNodes(null));

    // normal request.
    RefreshNodesRequest request = RefreshNodesRequest.newInstance(DecommissionType.NORMAL);
    interceptor.refreshNodes(request);
  }

  @Test
  public void testSC1RefreshNodes() throws Exception {

    // We will test 2 cases:
    // case 1, test the existing subCluster (SC-1).
    // case 2, test the non-exist subCluster.

    RefreshNodesRequest request =
        RefreshNodesRequest.newInstance(DecommissionType.NORMAL, 10, "SC-1");
    interceptor.refreshNodes(request);

    String notExistsSubCluster = "SC-NON";
    RefreshNodesRequest request1 = RefreshNodesRequest.newInstance(
        DecommissionType.NORMAL, 10, notExistsSubCluster);
    LambdaTestUtils.intercept(YarnException.class,
        "subClusterId = SC-NON is not an active subCluster.",
        () -> interceptor.refreshNodes(request1));
  }
}
