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
package org.apache.hadoop.yarn.server.router.secure;

import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResponse;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreTestUtil;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.server.router.clientrm.FederationClientInterceptor;
import org.apache.hadoop.yarn.server.router.clientrm.RouterClientRMService;
import org.apache.hadoop.yarn.server.router.rmadmin.DefaultRMAdminRequestInterceptor;
import org.apache.hadoop.yarn.server.router.rmadmin.RouterRMAdminService;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class TestSecureLogins extends AbstractSecureRouterTest {

  private static final Logger LOG = LoggerFactory.getLogger(TestSecureLogins.class);

  @Test
  public void testHasRealm() throws Throwable {
    Assert.assertNotNull(getRealm());
    LOG.info("Router principal = {}", getPrincipalAndRealm(ROUTER_LOCALHOST));
  }

  @Test
  public void testRouterSecureLogin() throws Exception {
    startSecureRouter();

    List<Service> services = this.getRouter().getServices();
    Assert.assertNotNull(services);
    Assert.assertEquals(3, services.size());

    stopSecureRouter();
  }

  @Test
  public void testRouterClientRMService() throws Exception {
    // Start the Router in Secure Mode
    startSecureRouter();

    // Start RM and RouterClientRMService in Secure mode
    setupSecureMockRM();
    initRouterClientRMService();

    // Test the simple rpc call of the Router in the Secure environment
    RouterClientRMService routerClientRMService = this.getRouter().getClientRMProxyService();
    GetClusterMetricsRequest metricsRequest = GetClusterMetricsRequest.newInstance();
    GetClusterMetricsResponse metricsResponse =
        routerClientRMService.getClusterMetrics(metricsRequest);
    Assert.assertNotNull(metricsResponse);
    YarnClusterMetrics clusterMetrics = metricsResponse.getClusterMetrics();
    Assert.assertEquals(4, clusterMetrics.getNumNodeManagers());
    Assert.assertEquals(0, clusterMetrics.getNumLostNodeManagers());

    // Stop the Router in Secure Mode
    stopSecureRouter();
  }

  @Test
  public void testRouterRMAdminService() throws Exception {
    // Start the Router in Secure Mode
    startSecureRouter();

    // Start RM and RouterClientRMService in Secure mode
    setupSecureMockRM();
    initRouterRMAdminService();

    // Test the simple rpc call of the Router in the Secure environment
    RouterRMAdminService routerRMAdminService = this.getRouter().getRmAdminProxyService();
    RefreshNodesRequest refreshNodesRequest = RefreshNodesRequest.newInstance();
    RefreshNodesResponse refreshNodesResponse =
        routerRMAdminService.refreshNodes(refreshNodesRequest);
    Assert.assertNotNull(refreshNodesResponse);

    // Stop the Router in Secure Mode
    stopSecureRouter();
  }

  public static String getPrincipalAndRealm(String principal) {
    return principal + "@" + getRealm();
  }

  protected static String getRealm() {
    return getKdc().getRealm();
  }

  private void initRouterClientRMService() throws Exception {
    Router router = this.getRouter();
    Map<SubClusterId, MockRM> mockRMs = getMockRMs();

    RouterClientRMService rmService = router.getClientRMProxyService();
    RouterClientRMService.RequestInterceptorChainWrapper wrapper = rmService.getInterceptorChain();
    FederationClientInterceptor interceptor =
        (FederationClientInterceptor) wrapper.getRootInterceptor();
    FederationStateStoreFacade stateStoreFacade = interceptor.getFederationFacade();
    FederationStateStore stateStore = stateStoreFacade.getStateStore();
    FederationStateStoreTestUtil stateStoreUtil = new FederationStateStoreTestUtil(stateStore);
    Map<SubClusterId, ApplicationClientProtocol> clientRMProxies = interceptor.getClientRMProxies();

    if (MapUtils.isNotEmpty(mockRMs)) {
      for (Map.Entry<SubClusterId, MockRM> entry : mockRMs.entrySet()) {
        SubClusterId sc = entry.getKey();
        MockRM mockRM = entry.getValue();
        stateStoreUtil.registerSubCluster(sc);
        if (clientRMProxies.containsKey(sc)) {
          continue;
        }
        clientRMProxies.put(sc, mockRM.getClientRMService());
      }
    }
  }

  private void initRouterRMAdminService() throws Exception {
    Router router = this.getRouter();
    Map<SubClusterId, MockRM> mockRMs = getMockRMs();
    SubClusterId sc = SubClusterId.newInstance(0);
    MockRM mockRM = mockRMs.get(sc);
    RouterRMAdminService routerRMAdminService = router.getRmAdminProxyService();
    RouterRMAdminService.RequestInterceptorChainWrapper rmAdminChainWrapper =
        routerRMAdminService.getInterceptorChain();
    DefaultRMAdminRequestInterceptor rmInterceptor =
        (DefaultRMAdminRequestInterceptor) rmAdminChainWrapper.getRootInterceptor();
    rmInterceptor.setRMAdmin(mockRM.getAdminService());
  }
}
