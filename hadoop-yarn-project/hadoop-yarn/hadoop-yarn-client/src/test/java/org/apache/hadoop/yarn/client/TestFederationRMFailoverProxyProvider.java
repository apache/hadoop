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
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.io.retry.FailoverProxyProvider.ProxyInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.federation.failover.FederationProxyProviderUtil;
import org.apache.hadoop.yarn.server.federation.failover.FederationRMFailoverProxyProvider;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.impl.MemoryFederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoRequest;
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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for FederationRMFailoverProxyProvider.
 */
public class TestFederationRMFailoverProxyProvider {

  private Configuration conf;
  private FederationStateStore stateStore;
  private final String dummyCapability = "cap";

  private GetClusterMetricsResponse threadResponse;

  @Before
  public void setUp() throws IOException, YarnException {
    conf = new YarnConfiguration();

    // Configure Facade cache to use a very long ttl
    conf.setInt(YarnConfiguration.FEDERATION_CACHE_TIME_TO_LIVE_SECS, 60 * 60);

    stateStore = spy(new MemoryFederationStateStore());
    stateStore.init(conf);
    FederationStateStoreFacade.getInstance().reinitialize(stateStore, conf);
    verify(stateStore, times(0))
        .getSubClusters(any(GetSubClustersInfoRequest.class));
  }

  @After
  public void tearDown() throws Exception {
    stateStore.close();
    stateStore = null;
  }

  @Test(timeout = 60000)
  public void testFederationRMFailoverProxyProvider() throws Exception {
    testProxyProvider(true);
  }

  @Test (timeout=60000)
  public void testFederationRMFailoverProxyProviderWithoutFlushFacadeCache()
      throws Exception {
    testProxyProvider(false);
  }

  private void testProxyProvider(boolean facadeFlushCache) throws Exception {
    final SubClusterId subClusterId = SubClusterId.newInstance("SC-1");
    final MiniYARNCluster cluster = new MiniYARNCluster(
        "testFederationRMFailoverProxyProvider", 3, 0, 1, 1);

    conf.setBoolean(YarnConfiguration.FEDERATION_FLUSH_CACHE_FOR_RM_ADDR,
        facadeFlushCache);

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

    verify(stateStore, times(1))
        .getSubClusters(any(GetSubClustersInfoRequest.class));

    // client will retry until the rm becomes active.
    GetClusterMetricsResponse response =
        client.getClusterMetrics(GetClusterMetricsRequest.newInstance());

    verify(stateStore, times(1))
        .getSubClusters(any(GetSubClustersInfoRequest.class));

    // validate response
    checkResponse(response);

    // transition rm3 to standby
    cluster.getResourceManager(2).getRMContext().getRMAdminService()
        .transitionToStandby(new HAServiceProtocol.StateChangeRequestInfo(
            HAServiceProtocol.RequestSource.REQUEST_BY_USER));

    // Transition rm2 to active;
    makeRMActive(subClusterId, cluster, 1);

    verify(stateStore, times(1))
        .getSubClusters(any(GetSubClustersInfoRequest.class));

    threadResponse = null;
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          // In non flush cache case, we will be hitting the cache with old RM
          // address and keep failing before the cache is flushed
          threadResponse =
              client.getClusterMetrics(GetClusterMetricsRequest.newInstance());
        } catch (YarnException | IOException e) {
          e.printStackTrace();
        }
      }
    });
    thread.start();

    if (!facadeFlushCache) {
      // Add a wait so that hopefully the thread has started hitting old cached
      Thread.sleep(500);

      // Should still be hitting cache
      verify(stateStore, times(1))
          .getSubClusters(any(GetSubClustersInfoRequest.class));

      // Force flush cache, so that it will pick up the new RM address
      FederationStateStoreFacade.getInstance().getSubCluster(subClusterId,
          true);
    }

    // Wait for the thread to finish and grab result
    thread.join();
    response = threadResponse;

    if (facadeFlushCache) {
      verify(stateStore, atLeast(2))
          .getSubClusters(any(GetSubClustersInfoRequest.class));
    } else {
      verify(stateStore, times(2))
          .getSubClusters(any(GetSubClustersInfoRequest.class));
    }

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

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testUGIForProxyCreation()
      throws IOException, InterruptedException {
    conf.set(YarnConfiguration.RM_CLUSTER_ID, "cluster1");

    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    UserGroupInformation user1 =
        UserGroupInformation.createProxyUser("user1", currentUser);
    UserGroupInformation user2 =
        UserGroupInformation.createProxyUser("user2", currentUser);

    final TestableFederationRMFailoverProxyProvider provider =
        new TestableFederationRMFailoverProxyProvider();

    InetSocketAddress addr =
        conf.getSocketAddr(YarnConfiguration.RM_SCHEDULER_ADDRESS,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);
    final ClientRMProxy rmProxy = mock(ClientRMProxy.class);
    when(rmProxy.getRMAddress(any(YarnConfiguration.class), any(Class.class)))
        .thenReturn(addr);

    user1.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() {
        provider.init(conf, rmProxy, ApplicationMasterProtocol.class);
        return null;
      }
    });

    final ProxyInfo currentProxy = provider.getProxy();
    Assert.assertEquals("user1", provider.getLastProxyUGI().getUserName());

    user2.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() {
        provider.performFailover(currentProxy.proxy);
        return null;
      }
    });
    Assert.assertEquals("user1", provider.getLastProxyUGI().getUserName());

    provider.close();
  }

  protected static class TestableFederationRMFailoverProxyProvider<T>
      extends FederationRMFailoverProxyProvider<T> {

    private UserGroupInformation lastProxyUGI = null;

    @Override
    protected T createRMProxy(InetSocketAddress rmAddress) throws IOException {
      lastProxyUGI = UserGroupInformation.getCurrentUser();
      return super.createRMProxy(rmAddress);
    }

    public UserGroupInformation getLastProxyUGI() {
      return lastProxyUGI;
    }
  }
}
