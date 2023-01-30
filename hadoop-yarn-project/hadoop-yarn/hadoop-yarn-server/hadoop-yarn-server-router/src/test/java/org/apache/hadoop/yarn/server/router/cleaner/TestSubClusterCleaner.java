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
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.impl.MemoryFederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
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
    for (int i = 0; i < 4; i++){
      // Create sub cluster id and info
      SubClusterId subClusterId = SubClusterId.newInstance("SC-" + i);
      SubClusterInfo subClusterInfo = SubClusterInfo.newInstance(subClusterId,
          "127.0.0.1:1", "127.0.0.1:2", "127.0.0.1:3", "127.0.0.1:4",
           SubClusterState.SC_RUNNING, System.currentTimeMillis(), "");
      // Register the subCluster
      stateStore.registerSubCluster(
          SubClusterRegisterRequest.newInstance(subClusterInfo));
    }
  }

  @Test
  public void testSubClusterRegisterHeartBeatTime()
      throws InterruptedException, TimeoutException, YarnException {
    // We set up such a unit test, We set the status of all subClusters to RUNNING,
    // and set the SubClusterCleaner Check Heartbeat timeout to 1s.
    // After 1s, the status of all subClusters should be SC_LOST.
    // At this time, the size of the Active SubCluster is 0.
    GenericTestUtils.waitFor(() -> {
      cleaner.run();
      try {
        int count = facade.getActiveSubClustersCount();
        if(count == 0) {
          return true;
        }
      } catch (YarnException e) {
      }
      return false;
    }, 1000, 1 * 1000);

    // Check Active SubCluster Status.
    Map<SubClusterId, SubClusterInfo> subClustersMap = facade.getSubClusters(false);
    subClustersMap.values().forEach(subClusterInfo -> {
      SubClusterState subClusterState = subClusterInfo.getState();
      Assert.assertEquals(SubClusterState.SC_LOST, subClusterState.SC_LOST);
    });
  }
}
