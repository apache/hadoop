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
package org.apache.hadoop.yarn.server.router.cleaner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.impl.MemoryFederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterHeartbeatRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterHeartbeatResponse;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.TimeoutException;

public class TestSubClusterCleaner {

  ////////////////////////////////
  // Router Constants
  ////////////////////////////////
  private Configuration conf;
  private MemoryFederationStateStore stateStore;
  private FederationStateStoreFacade facade;
  private SubClusterCleaner cleaner;
  private final static int NUM_SUBCLUSTERS = 4;
  private final static long EXPIRATION_TIME = Time.now() - 5000;

  @Before
  public void setup() throws YarnException {
    conf = new YarnConfiguration();
    conf.setLong(YarnConfiguration.ROUTER_SUBCLUSTER_EXPIRATION_TIME, 1000);
    conf.setInt(YarnConfiguration.FEDERATION_CACHE_TIME_TO_LIVE_SECS, 0);

    stateStore = new MemoryFederationStateStore();
    stateStore.init(conf);

    facade = FederationStateStoreFacade.getInstance();
    facade.reinitialize(stateStore, conf);

    cleaner = new SubClusterCleaner(conf);
    for (int i = 0; i < NUM_SUBCLUSTERS; i++){
      // Create sub cluster id and info
      SubClusterId subClusterId = SubClusterId.newInstance("SC-" + i);
      SubClusterInfo subClusterInfo = SubClusterInfo.newInstance(subClusterId,
          "127.0.0.1:1", "127.0.0.1:2", "127.0.0.1:3", "127.0.0.1:4",
           SubClusterState.SC_RUNNING, Time.now(), "");
      // Register the subCluster
      stateStore.registerSubCluster(
          SubClusterRegisterRequest.newInstance(subClusterInfo));
    }
  }

  @Test
  public void testSubClustersWithOutHeartBeat()
      throws InterruptedException, TimeoutException, YarnException {

    // We set up such a unit test, We set the status of all subClusters to RUNNING,
    // and Manually set subCluster heartbeat expiration.
    // At this time, the size of the Active SubCluster is 0.
    Map<SubClusterId, SubClusterInfo> subClustersMap = facade.getSubClusters(false);

    // Step1. Manually set subCluster heartbeat expiration.
    // subCluster has no heartbeat, and all subClusters will expire.
    subClustersMap.keySet().forEach(subClusterId ->
        stateStore.setExpiredHeartbeat(subClusterId, EXPIRATION_TIME));

    // Step2. Run the Cleaner to change the status of the expired SubCluster to SC_LOST.
    cleaner.run();

    // Step3. All clusters have expired,
    // so the current Federation has no active subClusters.
    int count = facade.getActiveSubClustersCount();
    Assert.assertEquals(0, count);

    // Step4. Check Active SubCluster Status.
    // We want all subClusters to be SC_LOST.
    subClustersMap.values().forEach(subClusterInfo -> {
      SubClusterState subClusterState = subClusterInfo.getState();
      Assert.assertEquals(SubClusterState.SC_LOST, subClusterState);
    });
  }

  @Test
  public void testSubClustersPartWithHeartBeat() throws YarnException, InterruptedException {

    // Step1. Manually set subCluster heartbeat expiration.
    for (int i = 0; i < NUM_SUBCLUSTERS; i++) {
      // Create subCluster id and info.
      expiredSubcluster("SC-" + i);
    }

    // Step2. Run the Cleaner to change the status of the expired SubCluster to SC_LOST.
    cleaner.run();

    // Step3. Let SC-0, SC-1 resume heartbeat.
    resumeSubClusterHeartbeat("SC-0");
    resumeSubClusterHeartbeat("SC-1");

    // Step4. At this point we should have 2 subClusters that are surviving clusters.
    int count = facade.getActiveSubClustersCount();
    Assert.assertEquals(2, count);

    // Step5. The result we expect is that SC-0 and SC-1 are in the RUNNING state,
    // and SC-2 and SC-3 are in the SC_LOST state.
    checkSubClusterState("SC-0", SubClusterState.SC_RUNNING);
    checkSubClusterState("SC-1", SubClusterState.SC_RUNNING);
    checkSubClusterState("SC-2", SubClusterState.SC_LOST);
    checkSubClusterState("SC-3", SubClusterState.SC_LOST);
  }

  private void resumeSubClusterHeartbeat(String pSubClusterId)
      throws YarnException {
    SubClusterId subClusterId = SubClusterId.newInstance(pSubClusterId);
    SubClusterHeartbeatRequest request = SubClusterHeartbeatRequest.newInstance(
        subClusterId, Time.now(), SubClusterState.SC_RUNNING, "test");
    SubClusterHeartbeatResponse response = stateStore.subClusterHeartbeat(request);
    Assert.assertNotNull(response);
  }

  private void expiredSubcluster(String pSubClusterId) {
    SubClusterId subClusterId = SubClusterId.newInstance(pSubClusterId);
    stateStore.setExpiredHeartbeat(subClusterId, EXPIRATION_TIME);
  }

  private void checkSubClusterState(String pSubClusterId, SubClusterState expectState)
      throws YarnException {
    Map<SubClusterId, SubClusterInfo> subClustersMap = facade.getSubClusters(false);
    SubClusterId subClusterId = SubClusterId.newInstance(pSubClusterId);
    SubClusterInfo subClusterInfo = subClustersMap.get(subClusterId);
    if (subClusterInfo == null) {
      throw new YarnException("subClusterId=" + pSubClusterId + " does not exist.");
    }
    Assert.assertEquals(expectState, subClusterInfo.getState());
  }
}
