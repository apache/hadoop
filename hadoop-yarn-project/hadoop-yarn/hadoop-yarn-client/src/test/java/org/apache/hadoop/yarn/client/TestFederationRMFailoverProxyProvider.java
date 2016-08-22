/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.client;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.federation.failover.FederationProxyProviderUtil;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.impl.MemoryFederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.resourcemanager.HATestUtil;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for FederationRMFailoverProxyProvider.
 */
public class TestFederationRMFailoverProxyProvider {

  private Configuration conf;
  private FederationStateStore stateStore;
  private final String dummyCapability = "cap";

  @Before
  public void setUp() throws IOException, YarnException {
    conf = new YarnConfiguration();
    stateStore = new MemoryFederationStateStore();
    stateStore.init(conf);
    FederationStateStoreFacade.getInstance().reinitialize(stateStore, conf);
  }

  @After
  public void tearDown() throws Exception {
    stateStore.close();
    stateStore = null;
  }

  @Test
  public void testFederationRMFailoverProxyProvider() throws Exception {
    final SubClusterId subClusterId = SubClusterId.newInstance("SC-1");
    final MiniYARNCluster cluster = new MiniYARNCluster(
        "testFederationRMFailoverProxyProvider", 3, 0, 1, 1);

    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, false);
    conf.set(YarnConfiguration.RM_CLUSTER_ID, "cluster1");
    conf.set(YarnConfiguration.RM_HA_IDS, "rm1,rm2,rm3");

    conf.setLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS,
        2000);

    HATestUtil.setRpcAddressForRM("rm1", 10000, conf);
    HATestUtil.setRpcAddressForRM("rm2", 20000, conf);
    HATestUtil.setRpcAddressForRM("rm3", 30000, conf);
    conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);

    cluster.init(conf);
    cluster.start();

    // Transition rm3 to active;
    makeRMActive(subClusterId, cluster, 2);

    ApplicationClientProtocol client = FederationProxyProviderUtil
        .createRMProxy(conf, ApplicationClientProtocol.class, subClusterId,
            UserGroupInformation.getCurrentUser());

    // client will retry until the rm becomes active.
    GetClusterMetricsResponse response =
        client.getClusterMetrics(GetClusterMetricsRequest.newInstance());

    // validate response
    checkResponse(response);

    // transition rm3 to standby
    cluster.getResourceManager(2).getRMContext().getRMAdminService()
        .transitionToStandby(new HAServiceProtocol.StateChangeRequestInfo(
            HAServiceProtocol.RequestSource.REQUEST_BY_USER));

    // Transition rm2 to active;
    makeRMActive(subClusterId, cluster, 1);
    response = client.getClusterMetrics(GetClusterMetricsRequest.newInstance());

    // validate response
    checkResponse(response);

    cluster.stop();
  }

  private void checkResponse(GetClusterMetricsResponse response) {
    Assert.assertNotNull(response.getClusterMetrics());
    Assert.assertEquals(0,
        response.getClusterMetrics().getNumActiveNodeManagers());
  }

  private void makeRMActive(final SubClusterId subClusterId,
      final MiniYARNCluster cluster, final int index) {
    try {
      System.out.println("Transition rm" + (index + 1) + " to active");
      String dummyAddress = "host:" + index;
      cluster.getResourceManager(index).getRMContext().getRMAdminService()
          .transitionToActive(new HAServiceProtocol.StateChangeRequestInfo(
              HAServiceProtocol.RequestSource.REQUEST_BY_USER));
      ResourceManager rm = cluster.getResourceManager(index);
      InetSocketAddress amRMAddress =
          rm.getApplicationMasterService().getBindAddress();
      InetSocketAddress clientRMAddress =
          rm.getClientRMService().getBindAddress();
      SubClusterRegisterRequest request = SubClusterRegisterRequest
          .newInstance(SubClusterInfo.newInstance(subClusterId,
              amRMAddress.getAddress().getHostAddress() + ":"
                  + amRMAddress.getPort(),
              clientRMAddress.getAddress().getHostAddress() + ":"
                  + clientRMAddress.getPort(),
              dummyAddress, dummyAddress, SubClusterState.SC_NEW, 1,
              dummyCapability));
      stateStore.registerSubCluster(request);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
