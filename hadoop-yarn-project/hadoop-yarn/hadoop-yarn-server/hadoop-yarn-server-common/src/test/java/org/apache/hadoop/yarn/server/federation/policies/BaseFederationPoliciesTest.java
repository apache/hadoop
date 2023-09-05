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

package org.apache.hadoop.yarn.server.federation.policies;

import static org.mockito.Mockito.mock;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.amrmproxy.FederationAMRMProxyPolicy;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyException;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.policies.router.FederationRouterPolicy;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.impl.MemoryFederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.utils.FederationPoliciesTestUtil;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.junit.Test;

/**
 * Base class for policies tests, tests for common reinitialization cases.
 */
public abstract class BaseFederationPoliciesTest {

  private ConfigurableFederationPolicy policy;
  private WeightedPolicyInfo policyInfo = mock(WeightedPolicyInfo.class);
  private Map<SubClusterId, SubClusterInfo> activeSubclusters = new HashMap<>();
  private FederationPolicyInitializationContext federationPolicyContext;
  private ApplicationSubmissionContext applicationSubmissionContext =
      mock(ApplicationSubmissionContext.class);
  private Random rand = new Random();
  private SubClusterId homeSubCluster;

  private ReservationSubmissionRequest reservationSubmissionRequest =
      mock(ReservationSubmissionRequest.class);

  @Test
  public void testReinitilialize() throws YarnException {
    FederationPolicyInitializationContext fpc =
        new FederationPolicyInitializationContext();
    ByteBuffer buf = getPolicyInfo().toByteBuffer();
    fpc.setSubClusterPolicyConfiguration(SubClusterPolicyConfiguration
        .newInstance("queue1", getPolicy().getClass().getCanonicalName(), buf));
    fpc.setFederationSubclusterResolver(
        FederationPoliciesTestUtil.initResolver());
    Configuration conf = new Configuration();
    fpc.setFederationStateStoreFacade(FederationPoliciesTestUtil.initFacade(conf));
    getPolicy().reinitialize(fpc);
  }

  @Test(expected = FederationPolicyInitializationException.class)
  public void testReinitilializeBad1() throws YarnException {
    getPolicy().reinitialize(null);
  }

  @Test(expected = FederationPolicyInitializationException.class)
  public void testReinitilializeBad2() throws YarnException {
    FederationPolicyInitializationContext fpc =
        new FederationPolicyInitializationContext();
    getPolicy().reinitialize(fpc);
  }

  @Test(expected = FederationPolicyInitializationException.class)
  public void testReinitilializeBad3() throws YarnException {
    FederationPolicyInitializationContext fpc =
        new FederationPolicyInitializationContext();
    ByteBuffer buf = mock(ByteBuffer.class);
    fpc.setSubClusterPolicyConfiguration(SubClusterPolicyConfiguration
        .newInstance("queue1", "WrongPolicyName", buf));
    fpc.setFederationSubclusterResolver(
        FederationPoliciesTestUtil.initResolver());
    Configuration conf = new Configuration();
    fpc.setFederationStateStoreFacade(FederationPoliciesTestUtil.initFacade(conf));
    getPolicy().reinitialize(fpc);
  }

  @Test(expected = FederationPolicyException.class)
  public void testNoSubclusters() throws YarnException {
    // empty the activeSubclusters map
    FederationPoliciesTestUtil.initializePolicyContext(getPolicy(),
        getPolicyInfo(), new HashMap<>());

    ConfigurableFederationPolicy localPolicy = getPolicy();
    if (localPolicy instanceof FederationRouterPolicy) {
      ((FederationRouterPolicy) localPolicy)
          .getHomeSubcluster(getApplicationSubmissionContext(), null);
    } else {
      String[] hosts = new String[] {"host1", "host2"};
      List<ResourceRequest> resourceRequests = FederationPoliciesTestUtil
          .createResourceRequests(hosts, 2 * 1024, 2, 1, 3, null, false);
      ((FederationAMRMProxyPolicy) localPolicy).splitResourceRequests(
          resourceRequests, new HashSet<SubClusterId>());
    }
  }

  public ConfigurableFederationPolicy getPolicy() {
    return policy;
  }

  public void setPolicy(ConfigurableFederationPolicy policy) {
    this.policy = policy;
  }

  public WeightedPolicyInfo getPolicyInfo() {
    return policyInfo;
  }

  public void setPolicyInfo(WeightedPolicyInfo policyInfo) {
    this.policyInfo = policyInfo;
  }

  public Map<SubClusterId, SubClusterInfo> getActiveSubclusters() {
    return activeSubclusters;
  }

  public void setActiveSubclusters(
      Map<SubClusterId, SubClusterInfo> activeSubclusters) {
    this.activeSubclusters = activeSubclusters;
  }

  public FederationPolicyInitializationContext getFederationPolicyContext() {
    return federationPolicyContext;
  }

  public void setFederationPolicyContext(
      FederationPolicyInitializationContext federationPolicyContext) {
    this.federationPolicyContext = federationPolicyContext;
  }

  public ApplicationSubmissionContext getApplicationSubmissionContext() {
    return applicationSubmissionContext;
  }

  public void setApplicationSubmissionContext(
      ApplicationSubmissionContext applicationSubmissionContext) {
    this.applicationSubmissionContext = applicationSubmissionContext;
  }

  public Random getRand() {
    return rand;
  }

  public void setRand(long seed) {
    this.rand.setSeed(seed);
  }

  public SubClusterId getHomeSubCluster() {
    return homeSubCluster;
  }

  public void setHomeSubCluster(SubClusterId homeSubCluster) {
    this.homeSubCluster = homeSubCluster;
  }

  public void setMockActiveSubclusters(int numSubclusters) {
    for (int i = 1; i <= numSubclusters; i++) {
      SubClusterIdInfo sc = new SubClusterIdInfo("sc" + i);
      SubClusterInfo sci = SubClusterInfo.newInstance(
          sc.toId(), "dns1:80", "dns1:81", "dns1:82", "dns1:83", SubClusterState.SC_RUNNING,
          System.currentTimeMillis(), "something");
      getActiveSubclusters().put(sc.toId(), sci);
    }
  }

  public String generateClusterMetricsInfo(int id) {
    long mem = 1024 * getRand().nextInt(277 * 100 - 1);
    // plant a best cluster
    if (id == 5) {
      mem = 1024 * 277 * 100;
    }
    String clusterMetrics =
        "{\"clusterMetrics\":{\"appsSubmitted\":65, \"appsCompleted\":64,\"appsPending\":0,"
        + "\"appsRunning\":0, \"appsFailed\":0, \"appsKilled\":1,\"reservedMB\":0,\"availableMB\":"
        + mem + ", \"allocatedMB\":0,\"reservedVirtualCores\":0, \"availableVirtualCores\":2216,"
        + "\"allocatedVirtualCores\":0, \"containersAllocated\":0,\"containersReserved\":0,"
        + "\"containersPending\":0,\"totalMB\":28364800, \"totalVirtualCores\":2216,"
        + "\"totalNodes\":278, \"lostNodes\":1,\"unhealthyNodes\":0,\"decommissionedNodes\":0, "
        + "\"rebootedNodes\":0, \"activeNodes\":277}}";
    return clusterMetrics;
  }

  public FederationStateStoreFacade getMemoryFacade() throws YarnException {

    // setting up a store and its facade (with caching off)
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.FEDERATION_CACHE_TIME_TO_LIVE_SECS, 0);
    FederationStateStoreFacade fedFacade = FederationStateStoreFacade.getInstance(conf);
    FederationStateStore store = new MemoryFederationStateStore();
    store.init(conf);
    fedFacade.reinitialize(store, conf);

    for (SubClusterInfo sinfo : getActiveSubclusters().values()) {
      store.registerSubCluster(SubClusterRegisterRequest.newInstance(sinfo));
    }

    return fedFacade;
  }

  public ReservationSubmissionRequest getReservationSubmissionRequest() {
    return reservationSubmissionRequest;
  }

  public void setReservationSubmissionRequest(
      ReservationSubmissionRequest reservationSubmissionRequest) {
    this.reservationSubmissionRequest = reservationSubmissionRequest;
  }

  public void setupContext() throws YarnException {
    FederationPolicyInitializationContext context =
        FederationPoliciesTestUtil.initializePolicyContext2(getPolicy(),
        getPolicyInfo(), getActiveSubclusters(), getMemoryFacade());
    this.setFederationPolicyContext(context);
  }
}
