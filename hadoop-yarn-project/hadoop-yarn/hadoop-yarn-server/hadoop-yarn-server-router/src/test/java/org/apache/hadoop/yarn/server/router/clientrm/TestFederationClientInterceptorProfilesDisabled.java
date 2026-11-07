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

package org.apache.hadoop.yarn.server.router.clientrm;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceProfilesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetResourceProfileRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YARNFeatureNotEnabledException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.manager.UniformBroadcastPolicyManager;
import org.apache.hadoop.yarn.server.federation.store.impl.MemoryFederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreTestUtil;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests the {@link FederationClientInterceptor} resource profile APIs against
 * sub-clusters with resource profiles disabled, the default.
 */
public class TestFederationClientInterceptorProfilesDisabled extends BaseRouterClientRMTest {

  private ProfilesDisabledInterceptor interceptor;

  /**
   * Sub-clusters are MockRMs with resource profiles disabled.
   * {@link TestableFederationClientInterceptor}'s MockRMs read the module's test
   * yarn-site.xml in their constructor, which enables resource profiles.
   */
  static class ProfilesDisabledInterceptor extends TestableFederationClientInterceptor {
    private final Map<SubClusterId, MockRM> rms = new ConcurrentHashMap<>();

    // Synchronized like the parent: MockRMs starting concurrently share the node attribute store.
    @Override
    protected synchronized ApplicationClientProtocol getClientRMProxyForSubCluster(
        SubClusterId id) {
      return rms.computeIfAbsent(id, k -> {
        YarnConfiguration conf = new YarnConfiguration(getConf());
        conf.setBoolean(YarnConfiguration.RM_RESOURCE_PROFILES_ENABLED, false);
        MockRM rm = new MockRM(conf);
        rm.start();
        return rm;
      }).getClientRMService();
    }

    @Override
    public void shutdown() {
      rms.values().forEach(MockRM::stop);
      super.shutdown();
    }
  }

  @BeforeEach
  @Override
  public void setUp() throws IOException {
    super.setUpConfig();
    interceptor = new ProfilesDisabledInterceptor();
    MemoryFederationStateStore stateStore = new MemoryFederationStateStore();
    stateStore.init(getConf());
    FederationStateStoreFacade.getInstance(getConf()).reinitialize(stateStore, getConf());
    FederationStateStoreTestUtil util = new FederationStateStoreTestUtil(stateStore);
    interceptor.setConf(getConf());
    interceptor.init("test-user");
    try {
      for (int i = 0; i < 2; i++) {
        util.registerSubCluster(SubClusterId.newInstance(Integer.toString(i)));
      }
    } catch (YarnException e) {
      throw new IOException(e);
    }
    DefaultMetricsSystem.setMiniClusterMode(true);
  }

  @AfterEach
  @Override
  public void tearDown() {
    interceptor.shutdown();
    super.tearDown();
  }

  @Override
  protected YarnConfiguration createConfiguration() {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.FEDERATION_ENABLED, true);
    conf.set(YarnConfiguration.ROUTER_CLIENTRM_INTERCEPTOR_CLASS_PIPELINE,
        TestableFederationClientInterceptor.class.getName());
    conf.set(YarnConfiguration.FEDERATION_POLICY_MANAGER,
        UniformBroadcastPolicyManager.class.getName());
    conf.setInt(YarnConfiguration.FEDERATION_CACHE_TIME_TO_LIVE_SECS, 0);
    conf.setBoolean(YarnConfiguration.RM_RESOURCE_PROFILES_ENABLED, false);
    return conf;
  }

  /** What a client asking a sub-cluster's RM directly gets. */
  @Test
  public void testResourceManagerReportsFeatureNotEnabled() {
    assertThrows(YARNFeatureNotEnabledException.class, () -> interceptor
        .getClientRMProxyForSubCluster(SubClusterId.newInstance("0"))
        .getResourceProfiles(GetAllResourceProfilesRequest.newInstance()));
  }

  /**
   * The Router must report the same exception, which YarnClient callers such as
   * the distributed shell Client rely on to carry on without profiles.
   */
  @Test
  public void testRouterReportsFeatureNotEnabled() {
    assertThrows(YARNFeatureNotEnabledException.class,
        () -> interceptor.getResourceProfiles(GetAllResourceProfilesRequest.newInstance()));
    assertThrows(YARNFeatureNotEnabledException.class,
        () -> interceptor.getResourceProfile(GetResourceProfileRequest.newInstance("default")));
  }
}
