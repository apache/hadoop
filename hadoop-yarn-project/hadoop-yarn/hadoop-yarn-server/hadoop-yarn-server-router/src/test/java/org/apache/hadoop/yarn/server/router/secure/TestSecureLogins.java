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

import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResponse;
import org.apache.hadoop.yarn.server.router.clientrm.RouterClientRMService;
import org.apache.hadoop.yarn.server.router.rmadmin.RouterRMAdminService;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class TestSecureLogins extends AbstractSecureRouterTest {

  private static final Logger LOG = LoggerFactory.getLogger(TestSecureLogins.class);

  @Test
  public void testHasRealm() throws Throwable {
    Assert.assertNotNull(getRealm());
    LOG.info("Router principal = {}", getPrincipalAndRealm(ROUTER_LOCALHOST));
  }

  @Test
  public void testRouterSecureLogin() throws Exception {
    startSecureRouter(false);

    List<Service> services = this.getRouter().getServices();
    Assert.assertNotNull(services);
    Assert.assertEquals(3, services.size());

    stopSecureRouter();
  }

  @Test
  public void testRouterClientRMProxyService() throws Exception {
    startSecureRouter(true);

    // Test the simple rpc call of the Router in the Secure environment
    RouterClientRMService routerClientRMService = this.getRouter().getClientRMProxyService();
    GetClusterMetricsRequest metricsRequest = GetClusterMetricsRequest.newInstance();
    GetClusterMetricsResponse metricsResponse = routerClientRMService.getClusterMetrics(metricsRequest);
    Assert.assertNotNull(metricsResponse);
    YarnClusterMetrics clusterMetrics = metricsResponse.getClusterMetrics();
    Assert.assertEquals(4, clusterMetrics.getNumNodeManagers());
    Assert.assertEquals(0, clusterMetrics.getNumLostNodeManagers());

    stopSecureRouter();
  }

  @Test
  public void testRouterRMAdminService() throws Exception {
    startSecureRouter(true);

    RouterRMAdminService routerRMAdminService = this.getRouter().getRmAdminProxyService();
    RefreshNodesRequest refreshNodesRequest = RefreshNodesRequest.newInstance();
    RefreshNodesResponse refreshNodesResponse =
        routerRMAdminService.refreshNodes(refreshNodesRequest);
    Assert.assertNotNull(refreshNodesResponse);

    stopSecureRouter();
  }

  public static String getPrincipalAndRealm(String principal) {
    return principal + "@" + getRealm();
  }

  protected static String getRealm() {
    return getKdc().getRealm();
  }
}
