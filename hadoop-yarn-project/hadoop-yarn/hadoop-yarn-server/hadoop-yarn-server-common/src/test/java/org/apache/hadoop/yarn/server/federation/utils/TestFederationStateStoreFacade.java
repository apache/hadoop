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
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.security.client.YARNDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.federation.cache.FederationCache;
import org.apache.hadoop.yarn.server.federation.cache.FederationCache.ApplicationHomeSubClusterCacheResponse;
import org.apache.hadoop.yarn.server.federation.cache.FederationCache.CacheRequest;
import org.apache.hadoop.yarn.server.federation.cache.FederationJCache;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.impl.MemoryFederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMDTSecretManagerState;
import org.apache.hadoop.yarn.server.federation.store.records.RouterStoreToken;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.ehcache.Cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Unit tests for FederationStateStoreFacade.
 */
public class TestFederationStateStoreFacade {

  @SuppressWarnings({"NoWhitespaceAfter"})
  public static Collection<Boolean[]> getParameters() {
    return Arrays.asList(new Boolean[][]{{Boolean.FALSE}, {Boolean.TRUE}});
  }

  private final long clusterTs = System.currentTimeMillis();
  private final int numSubClusters = 3;
  private final int numApps = 5;
  private final int numQueues = 2;

  private Configuration conf;
  private FederationStateStore stateStore;
  private FederationStateStoreTestUtil stateStoreTestUtil;
  private FederationStateStoreFacade facade;

  private Boolean isCachingEnabled;

  private void initTestFederationStateStoreFacade(Boolean pIsCachingEnabled)
      throws IOException, YarnException {
    conf = new Configuration();
    if (!(pIsCachingEnabled.booleanValue())) {
      conf.setInt(YarnConfiguration.FEDERATION_CACHE_TIME_TO_LIVE_SECS, 0);
    }
    this.isCachingEnabled = pIsCachingEnabled;
    facade = FederationStateStoreFacade.getInstance(conf);
    setUp();
  }

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

  @AfterEach
  public void tearDown() throws Exception {
    stateStore.close();
    stateStore = null;
  }

  @ParameterizedTest
  @MethodSource("getParameters")
  public void testGetSubCluster(Boolean pIsCachingEnabled)
      throws YarnException, IOException {
    initTestFederationStateStoreFacade(pIsCachingEnabled);
    for (int i = 0; i < numSubClusters; i++) {
      SubClusterId subClusterId =
          SubClusterId.newInstance(FederationStateStoreTestUtil.SC_PREFIX + i);
      assertEquals(stateStoreTestUtil.querySubClusterInfo(subClusterId),
          facade.getSubCluster(subClusterId));
    }
  }

  @ParameterizedTest
  @MethodSource("getParameters")
  public void testInvalidGetSubCluster(Boolean pIsCachingEnabled)
      throws YarnException, IOException {
    initTestFederationStateStoreFacade(pIsCachingEnabled);
    SubClusterId subClusterId =
        SubClusterId.newInstance(FederationStateStoreTestUtil.INVALID);
    assertNull(facade.getSubCluster(subClusterId));
  }

  @ParameterizedTest
  @MethodSource("getParameters")
  public void testGetSubClusterFlushCache(Boolean pIsCachingEnabled)
      throws YarnException, IOException {
    initTestFederationStateStoreFacade(pIsCachingEnabled);
    for (int i = 0; i < numSubClusters; i++) {
      SubClusterId subClusterId =
          SubClusterId.newInstance(FederationStateStoreTestUtil.SC_PREFIX + i);
      assertEquals(stateStoreTestUtil.querySubClusterInfo(subClusterId),
          facade.getSubCluster(subClusterId, true));
    }
  }

  @ParameterizedTest
  @MethodSource("getParameters")
  public void testGetSubClusters(Boolean pIsCachingEnabled)
      throws YarnException, IOException {
    initTestFederationStateStoreFacade(pIsCachingEnabled);
    Map<SubClusterId, SubClusterInfo> subClusters =
        facade.getSubClusters(false);
    for (SubClusterId subClusterId : subClusters.keySet()) {
      assertEquals(stateStoreTestUtil.querySubClusterInfo(subClusterId),
          subClusters.get(subClusterId));
    }
  }

  @ParameterizedTest
  @MethodSource("getParameters")
  public void testGetPolicyConfiguration(Boolean pIsCachingEnabled)
      throws YarnException, IOException {
    initTestFederationStateStoreFacade(pIsCachingEnabled);
    for (int i = 0; i < numQueues; i++) {
      String queue = FederationStateStoreTestUtil.Q_PREFIX + i;
      assertEquals(stateStoreTestUtil.queryPolicyConfiguration(queue),
          facade.getPolicyConfiguration(queue));
    }
  }

  @ParameterizedTest
  @MethodSource("getParameters")
  public void testSubClustersCache(Boolean pIsCachingEnabled)
      throws YarnException, IOException {
    initTestFederationStateStoreFacade(pIsCachingEnabled);
    Map<SubClusterId, SubClusterInfo> allClusters =
        facade.getSubClusters(false);
    assertEquals(numSubClusters, allClusters.size());
    SubClusterId clusterId = new ArrayList<>(allClusters.keySet()).get(0);
    // make  one subcluster down unregister
    stateStoreTestUtil.deRegisterSubCluster(clusterId);
    Map<SubClusterId, SubClusterInfo> activeClusters =
        facade.getSubClusters(true);
    assertEquals(numSubClusters - 1, activeClusters.size());
    // Recheck false case.
    allClusters = facade.getSubClusters(false);
    assertEquals(numSubClusters, allClusters.size());
  }

  @ParameterizedTest
  @MethodSource("getParameters")
  public void testInvalidGetPolicyConfiguration(Boolean pIsCachingEnabled)
      throws YarnException, IOException {
    initTestFederationStateStoreFacade(pIsCachingEnabled);
    assertNull(facade.getPolicyConfiguration(FederationStateStoreTestUtil.INVALID));
  }

  @ParameterizedTest
  @MethodSource("getParameters")
  public void testGetPoliciesConfigurations(Boolean pIsCachingEnabled)
      throws YarnException, IOException {
    initTestFederationStateStoreFacade(pIsCachingEnabled);
    Map<String, SubClusterPolicyConfiguration> queuePolicies =
        facade.getPoliciesConfigurations();
    for (String queue : queuePolicies.keySet()) {
      assertEquals(stateStoreTestUtil.queryPolicyConfiguration(queue),
          queuePolicies.get(queue));
    }
  }

  @ParameterizedTest
  @MethodSource("getParameters")
  public void testGetHomeSubClusterForApp(Boolean pIsCachingEnabled)
      throws YarnException, IOException {
    initTestFederationStateStoreFacade(pIsCachingEnabled);
    for (int i = 0; i < numApps; i++) {
      ApplicationId appId = ApplicationId.newInstance(clusterTs, i);
      assertEquals(stateStoreTestUtil.queryApplicationHomeSC(appId),
          facade.getApplicationHomeSubCluster(appId));
    }
  }

  @ParameterizedTest
  @MethodSource("getParameters")
  public void testAddApplicationHomeSubCluster(Boolean pIsCachingEnabled)
      throws YarnException, IOException {
    initTestFederationStateStoreFacade(pIsCachingEnabled);
    // Inserting <AppId, Home1> into FederationStateStore
    ApplicationId appId = ApplicationId.newInstance(clusterTs, numApps + 1);
    SubClusterId subClusterId1 = SubClusterId.newInstance("Home1");

    ApplicationHomeSubCluster appHomeSubCluster =
        ApplicationHomeSubCluster.newInstance(appId, subClusterId1);

    SubClusterId result =
        facade.addApplicationHomeSubCluster(appHomeSubCluster);

    assertEquals(facade.getApplicationHomeSubCluster(appId), result);
    assertEquals(subClusterId1, result);

    // Inserting <AppId, Home2> into FederationStateStore.
    // The application is already present.
    // FederationFacade will return Home1 as SubClusterId.
    SubClusterId subClusterId2 = SubClusterId.newInstance("Home2");
    appHomeSubCluster =
        ApplicationHomeSubCluster.newInstance(appId, subClusterId2);

    result = facade.addApplicationHomeSubCluster(appHomeSubCluster);

    assertEquals(facade.getApplicationHomeSubCluster(appId), result);
    assertEquals(subClusterId1, result);
  }

  @ParameterizedTest
  @MethodSource("getParameters")
  public void testGetApplicationHomeSubClusterCache(Boolean pIsCachingEnabled)
      throws Exception {
    initTestFederationStateStoreFacade(pIsCachingEnabled);
    ApplicationId appId = ApplicationId.newInstance(clusterTs, numApps + 1);
    SubClusterId subClusterId1 = SubClusterId.newInstance("Home1");

    ApplicationHomeSubCluster appHomeSubCluster =
        ApplicationHomeSubCluster.newInstance(appId, subClusterId1);
    SubClusterId subClusterIdAdd = facade.addApplicationHomeSubCluster(appHomeSubCluster);

    SubClusterId subClusterIdByFacade = facade.getApplicationHomeSubCluster(appId);
    assertEquals(subClusterIdByFacade, subClusterIdAdd);
    assertEquals(subClusterId1, subClusterIdAdd);

    if (isCachingEnabled.booleanValue()) {
      FederationCache fedCache = facade.getFederationCache();
      assert fedCache instanceof FederationJCache;
      FederationJCache jCache = (FederationJCache) fedCache;
      String cacheKey = jCache.getAppHomeSubClusterCacheKey(appId);
      Cache<String, CacheRequest> cache = jCache.getCache();
      CacheRequest<String, ?> cacheRequest = cache.get(cacheKey);
      ApplicationHomeSubClusterCacheResponse response =
          ApplicationHomeSubClusterCacheResponse.class.cast(cacheRequest.getValue());
      SubClusterId subClusterIdByCache = response.getItem();
      assertEquals(subClusterIdByFacade, subClusterIdByCache);
      assertEquals(subClusterId1, subClusterIdByCache);
    }
  }

  @ParameterizedTest
  @MethodSource("getParameters")
  public void testStoreNewMasterKey(Boolean pIsCachingEnabled)
      throws YarnException, IOException {
    initTestFederationStateStoreFacade(pIsCachingEnabled);
    // store delegation key;
    DelegationKey key = new DelegationKey(1234, 4321, "keyBytes".getBytes());
    Set<DelegationKey> keySet = new HashSet<>();
    keySet.add(key);
    facade.storeNewMasterKey(key);

    MemoryFederationStateStore federationStateStore =
        (MemoryFederationStateStore) facade.getStateStore();
    RouterRMDTSecretManagerState secretManagerState =
        federationStateStore.getRouterRMSecretManagerState();
    assertEquals(keySet, secretManagerState.getMasterKeyState());
  }

  @ParameterizedTest
  @MethodSource("getParameters")
  public void testRemoveStoredMasterKey(Boolean pIsCachingEnabled)
      throws YarnException, IOException {
    initTestFederationStateStoreFacade(pIsCachingEnabled);
    // store delegation key;
    DelegationKey key = new DelegationKey(4567, 7654, "keyBytes".getBytes());
    Set<DelegationKey> keySet = new HashSet<>();
    keySet.add(key);
    facade.storeNewMasterKey(key);

    // check to delete delegationKey
    facade.removeStoredMasterKey(key);
    keySet.clear();

    MemoryFederationStateStore federationStateStore =
        (MemoryFederationStateStore) facade.getStateStore();
    RouterRMDTSecretManagerState secretManagerState =
        federationStateStore.getRouterRMSecretManagerState();
    assertEquals(keySet, secretManagerState.getMasterKeyState());
  }

  @ParameterizedTest
  @MethodSource("getParameters")
  public void testStoreNewToken(Boolean pIsCachingEnabled) throws YarnException, IOException {
    initTestFederationStateStoreFacade(pIsCachingEnabled);
    // store new rm-token
    RMDelegationTokenIdentifier dtId1 = new RMDelegationTokenIdentifier(
        new Text("owner1"), new Text("renewer1"), new Text("realuser1"));
    int sequenceNumber = 1;
    dtId1.setSequenceNumber(sequenceNumber);
    Long renewDate1 = Time.now();
    facade.storeNewToken(dtId1, renewDate1);

    // get RouterStoreToken from StateStore
    RouterStoreToken routerStoreToken = RouterStoreToken.newInstance(dtId1, renewDate1);
    RouterRMTokenRequest rmTokenRequest = RouterRMTokenRequest.newInstance(routerStoreToken);
    RouterRMTokenResponse rmTokenResponse = stateStore.getTokenByRouterStoreToken(rmTokenRequest);
    assertNotNull(rmTokenResponse);

    RouterStoreToken resultStoreToken = rmTokenResponse.getRouterStoreToken();
    YARNDelegationTokenIdentifier resultTokenIdentifier = resultStoreToken.getTokenIdentifier();
    assertNotNull(resultStoreToken);
    assertNotNull(resultTokenIdentifier);
    assertNotNull(resultStoreToken.getRenewDate());

    assertEquals(dtId1, resultTokenIdentifier);
    assertEquals(renewDate1, resultStoreToken.getRenewDate());
    assertEquals(sequenceNumber, resultTokenIdentifier.getSequenceNumber());
  }

  @ParameterizedTest
  @MethodSource("getParameters")
  public void testUpdateNewToken(Boolean pIsCachingEnabled) throws YarnException, IOException {
    initTestFederationStateStoreFacade(pIsCachingEnabled);
    // store new rm-token
    RMDelegationTokenIdentifier dtId1 = new RMDelegationTokenIdentifier(
        new Text("owner2"), new Text("renewer2"), new Text("realuser2"));
    int sequenceNumber = 2;
    dtId1.setSequenceNumber(sequenceNumber);
    Long renewDate1 = Time.now();
    facade.storeNewToken(dtId1, renewDate1);

    Long renewDate2 = Time.now();
    int sequenceNumber2 = 3;
    dtId1.setSequenceNumber(sequenceNumber2);
    facade.updateStoredToken(dtId1, renewDate2);

    // get RouterStoreToken from StateStore
    RouterStoreToken routerStoreToken = RouterStoreToken.newInstance(dtId1, renewDate1);
    RouterRMTokenRequest rmTokenRequest = RouterRMTokenRequest.newInstance(routerStoreToken);
    RouterRMTokenResponse rmTokenResponse = stateStore.getTokenByRouterStoreToken(rmTokenRequest);
    assertNotNull(rmTokenResponse);

    RouterStoreToken resultStoreToken = rmTokenResponse.getRouterStoreToken();
    YARNDelegationTokenIdentifier resultTokenIdentifier = resultStoreToken.getTokenIdentifier();
    assertNotNull(resultStoreToken);
    assertNotNull(resultTokenIdentifier);
    assertNotNull(resultStoreToken.getRenewDate());

    assertEquals(dtId1, resultTokenIdentifier);
    assertEquals(renewDate2, resultStoreToken.getRenewDate());
    assertEquals(sequenceNumber2, resultTokenIdentifier.getSequenceNumber());
  }

  @ParameterizedTest
  @MethodSource("getParameters")
  public void testRemoveStoredToken(Boolean pIsCachingEnabled) throws Exception {
    initTestFederationStateStoreFacade(pIsCachingEnabled);
    // store new rm-token
    RMDelegationTokenIdentifier dtId1 = new RMDelegationTokenIdentifier(
        new Text("owner3"), new Text("renewer3"), new Text("realuser3"));
    int sequenceNumber = 3;
    dtId1.setSequenceNumber(sequenceNumber);
    Long renewDate1 = Time.now();
    facade.storeNewToken(dtId1, renewDate1);

    // get RouterStoreToken from StateStore
    RouterStoreToken routerStoreToken = RouterStoreToken.newInstance(dtId1, renewDate1);
    RouterRMTokenRequest rmTokenRequest = RouterRMTokenRequest.newInstance(routerStoreToken);
    RouterRMTokenResponse rmTokenResponse = stateStore.getTokenByRouterStoreToken(rmTokenRequest);
    assertNotNull(rmTokenResponse);

    RouterStoreToken resultStoreToken = rmTokenResponse.getRouterStoreToken();
    YARNDelegationTokenIdentifier resultTokenIdentifier = resultStoreToken.getTokenIdentifier();
    assertNotNull(resultStoreToken);
    assertNotNull(resultTokenIdentifier);
    assertNotNull(resultStoreToken.getRenewDate());

    assertEquals(dtId1, resultTokenIdentifier);
    assertEquals(renewDate1, resultStoreToken.getRenewDate());
    assertEquals(sequenceNumber, resultTokenIdentifier.getSequenceNumber());

    // remove rm-token
    facade.removeStoredToken(dtId1);

    // Call again(getTokenByRouterStoreToken) after remove will throw IOException(not exist)
    LambdaTestUtils.intercept(IOException.class, "RMDelegationToken: " + dtId1 + " does not exist.",
        () -> stateStore.getTokenByRouterStoreToken(rmTokenRequest));
  }
}
