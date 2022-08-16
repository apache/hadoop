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

package org.apache.hadoop.yarn.server.federation.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.HashSet;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.impl.MemoryFederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMDTSecretManagerState;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Unit tests for FederationStateStoreFacade.
 */
@RunWith(Parameterized.class)
public class TestFederationStateStoreFacade {

  @Parameters
  @SuppressWarnings({"NoWhitespaceAfter"})
  public static Collection<Boolean[]> getParameters() {
    return Arrays
        .asList(new Boolean[][] { { Boolean.FALSE }, { Boolean.TRUE } });
  }

  private final long clusterTs = System.currentTimeMillis();
  private final int numSubClusters = 3;
  private final int numApps = 5;
  private final int numQueues = 2;

  private Configuration conf;
  private FederationStateStore stateStore;
  private FederationStateStoreTestUtil stateStoreTestUtil;
  private FederationStateStoreFacade facade =
      FederationStateStoreFacade.getInstance();

  public TestFederationStateStoreFacade(Boolean isCachingEnabled) {
    conf = new Configuration();
    if (!(isCachingEnabled.booleanValue())) {
      conf.setInt(YarnConfiguration.FEDERATION_CACHE_TIME_TO_LIVE_SECS, 0);
    }
  }

  @Before
  public void setUp() throws IOException, YarnException {
    stateStore = new MemoryFederationStateStore();
    stateStore.init(conf);
    facade.reinitialize(stateStore, conf);
    // hydrate the store
    stateStoreTestUtil = new FederationStateStoreTestUtil(stateStore);
    stateStoreTestUtil.registerSubClusters(numSubClusters);
    stateStoreTestUtil.addAppsHomeSC(clusterTs, numApps);
    stateStoreTestUtil.addPolicyConfigs(numQueues);
  }

  @After
  public void tearDown() throws Exception {
    stateStore.close();
    stateStore = null;
  }

  @Test
  public void testGetSubCluster() throws YarnException {
    for (int i = 0; i < numSubClusters; i++) {
      SubClusterId subClusterId =
          SubClusterId.newInstance(FederationStateStoreTestUtil.SC_PREFIX + i);
      Assert.assertEquals(stateStoreTestUtil.querySubClusterInfo(subClusterId),
          facade.getSubCluster(subClusterId));
    }
  }

  @Test
  public void testInvalidGetSubCluster() throws YarnException {
    SubClusterId subClusterId =
        SubClusterId.newInstance(FederationStateStoreTestUtil.INVALID);
    Assert.assertNull(facade.getSubCluster(subClusterId));
  }

  @Test
  public void testGetSubClusterFlushCache() throws YarnException {
    for (int i = 0; i < numSubClusters; i++) {
      SubClusterId subClusterId =
          SubClusterId.newInstance(FederationStateStoreTestUtil.SC_PREFIX + i);
      Assert.assertEquals(stateStoreTestUtil.querySubClusterInfo(subClusterId),
          facade.getSubCluster(subClusterId, true));
    }
  }

  @Test
  public void testGetSubClusters() throws YarnException {
    Map<SubClusterId, SubClusterInfo> subClusters =
        facade.getSubClusters(false);
    for (SubClusterId subClusterId : subClusters.keySet()) {
      Assert.assertEquals(stateStoreTestUtil.querySubClusterInfo(subClusterId),
          subClusters.get(subClusterId));
    }
  }

  @Test
  public void testGetPolicyConfiguration() throws YarnException {
    for (int i = 0; i < numQueues; i++) {
      String queue = FederationStateStoreTestUtil.Q_PREFIX + i;
      Assert.assertEquals(stateStoreTestUtil.queryPolicyConfiguration(queue),
          facade.getPolicyConfiguration(queue));
    }
  }

  @Test
  public void testSubClustersCache() throws YarnException {
    Map<SubClusterId, SubClusterInfo> allClusters =
        facade.getSubClusters(false);
    Assert.assertEquals(numSubClusters, allClusters.size());
    SubClusterId clusterId = new ArrayList<>(allClusters.keySet()).get(0);
    // make  one subcluster down unregister
    stateStoreTestUtil.deRegisterSubCluster(clusterId);
    Map<SubClusterId, SubClusterInfo> activeClusters =
        facade.getSubClusters(true);
    Assert.assertEquals(numSubClusters - 1, activeClusters.size());
    // Recheck false case.
    allClusters = facade.getSubClusters(false);
    Assert.assertEquals(numSubClusters, allClusters.size());
  }

  @Test
  public void testInvalidGetPolicyConfiguration() throws YarnException {
    Assert.assertNull(
        facade.getPolicyConfiguration(FederationStateStoreTestUtil.INVALID));
  }

  @Test
  public void testGetPoliciesConfigurations() throws YarnException {
    Map<String, SubClusterPolicyConfiguration> queuePolicies =
        facade.getPoliciesConfigurations();
    for (String queue : queuePolicies.keySet()) {
      Assert.assertEquals(stateStoreTestUtil.queryPolicyConfiguration(queue),
          queuePolicies.get(queue));
    }
  }

  @Test
  public void testGetHomeSubClusterForApp() throws YarnException {
    for (int i = 0; i < numApps; i++) {
      ApplicationId appId = ApplicationId.newInstance(clusterTs, i);
      Assert.assertEquals(stateStoreTestUtil.queryApplicationHomeSC(appId),
          facade.getApplicationHomeSubCluster(appId));
    }
  }

  @Test
  public void testAddApplicationHomeSubCluster() throws YarnException {

    // Inserting <AppId, Home1> into FederationStateStore
    ApplicationId appId = ApplicationId.newInstance(clusterTs, numApps + 1);
    SubClusterId subClusterId1 = SubClusterId.newInstance("Home1");

    ApplicationHomeSubCluster appHomeSubCluster =
        ApplicationHomeSubCluster.newInstance(appId, subClusterId1);

    SubClusterId result =
        facade.addApplicationHomeSubCluster(appHomeSubCluster);

    Assert.assertEquals(facade.getApplicationHomeSubCluster(appId), result);
    Assert.assertEquals(subClusterId1, result);

    // Inserting <AppId, Home2> into FederationStateStore.
    // The application is already present.
    // FederationFacade will return Home1 as SubClusterId.
    SubClusterId subClusterId2 = SubClusterId.newInstance("Home2");
    appHomeSubCluster =
        ApplicationHomeSubCluster.newInstance(appId, subClusterId2);

    result = facade.addApplicationHomeSubCluster(appHomeSubCluster);

    Assert.assertEquals(facade.getApplicationHomeSubCluster(appId), result);
    Assert.assertEquals(subClusterId1, result);
  }

  @Test
  public void testStoreNewMasterKey() throws YarnException, IOException {
    // store delegation key;
    DelegationKey key = new DelegationKey(1234, 4321, "keyBytes".getBytes());
    HashSet<DelegationKey> keySet = new HashSet<DelegationKey>();
    keySet.add(key);
    facade.storeNewMasterKey(key);

    MemoryFederationStateStore federationStateStore = (MemoryFederationStateStore) facade.getStateStore();
    RouterRMDTSecretManagerState secretManagerState = federationStateStore.getRouterRMSecretManagerState();
    Assert.assertEquals(keySet, secretManagerState.getMasterKeyState());
  }

  @Test
  public void testRemoveStoredMasterKey() throws YarnException, IOException {
    // store delegation key;
    DelegationKey key = new DelegationKey(4567, 7654, "keyBytes".getBytes());
    HashSet<DelegationKey> keySet = new HashSet<DelegationKey>();
    keySet.add(key);
    facade.storeNewMasterKey(key);

    // check to delete delegationKey
    facade.removeStoredMasterKey(key);
    keySet.clear();

    MemoryFederationStateStore federationStateStore = (MemoryFederationStateStore) facade.getStateStore();
    RouterRMDTSecretManagerState secretManagerState = federationStateStore.getRouterRMSecretManagerState();
    Assert.assertEquals(keySet, secretManagerState.getMasterKeyState());
  }

  @Test
  public void testStoreNewToken() throws YarnException, IOException {
    // store new rm-token
    RMDelegationTokenIdentifier dtId1 = new RMDelegationTokenIdentifier(
        new Text("owner1"), new Text("renewer1"), new Text("realuser1"));
    int sequenceNumber = 1;
    dtId1.setSequenceNumber(sequenceNumber);
    Long renewDate1 = Time.now();
    facade.storeNewToken(dtId1, renewDate1);

    Map<RMDelegationTokenIdentifier, Long> token1 = new HashMap<RMDelegationTokenIdentifier, Long>();
    token1.put(dtId1, renewDate1);

    MemoryFederationStateStore stateStore = (MemoryFederationStateStore) facade.getStateStore();
    RouterRMDTSecretManagerState storeSecretManagerState =
        stateStore.getRouterRMSecretManagerState();
    Assert.assertEquals(token1, storeSecretManagerState.getTokenState());
  }

  @Test
  public void testUpdateNewToken() throws YarnException, IOException {
    // store new rm-token
    RMDelegationTokenIdentifier dtId1 = new RMDelegationTokenIdentifier(
        new Text("owner2"), new Text("renewer2"), new Text("realuser2"));
    int sequenceNumber = 2;
    dtId1.setSequenceNumber(sequenceNumber);
    Long renewDate1 = Time.now();
    facade.storeNewToken(dtId1, renewDate1);

    Map<RMDelegationTokenIdentifier, Long> token1 = new HashMap<RMDelegationTokenIdentifier, Long>();
    token1.put(dtId1, renewDate1);

    renewDate1 = Time.now();
    facade.updateStoredToken(dtId1, renewDate1);
    token1.put(dtId1, renewDate1);

    MemoryFederationStateStore stateStore = (MemoryFederationStateStore) facade.getStateStore();
    RouterRMDTSecretManagerState updateSecretManagerState =
        stateStore.getRouterRMSecretManagerState();
    Assert.assertEquals(token1, updateSecretManagerState.getTokenState());
    Assert.assertEquals(sequenceNumber, updateSecretManagerState.getDTSequenceNumber());
  }

  @Test
  public void testRemoveStoredToken() throws YarnException, IOException {
    // store new rm-token
    RMDelegationTokenIdentifier dtId1 = new RMDelegationTokenIdentifier(
        new Text("owner3"), new Text("renewer3"), new Text("realuser3"));
    int sequenceNumber = 3;
    dtId1.setSequenceNumber(sequenceNumber);
    Long renewDate1 = Time.now();
    facade.storeNewToken(dtId1, renewDate1);

    Map<RMDelegationTokenIdentifier, Long> token1 = new HashMap<RMDelegationTokenIdentifier, Long>();
    token1.put(dtId1, renewDate1);

    // remove rm-token
    facade.removeStoredToken(dtId1);
    token1.clear();

    MemoryFederationStateStore stateStore = (MemoryFederationStateStore) facade.getStateStore();
    RouterRMDTSecretManagerState deleteSecretManagerState =
        stateStore.getRouterRMSecretManagerState();

    Assert.assertEquals(token1, deleteSecretManagerState.getTokenState());
    Assert.assertEquals(sequenceNumber, deleteSecretManagerState.getDTSequenceNumber());
  }
}
