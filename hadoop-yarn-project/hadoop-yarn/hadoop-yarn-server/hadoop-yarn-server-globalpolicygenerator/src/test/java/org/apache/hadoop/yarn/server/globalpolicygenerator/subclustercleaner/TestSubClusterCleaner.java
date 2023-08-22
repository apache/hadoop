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

package org.apache.hadoop.yarn.server.globalpolicygenerator.subclustercleaner;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.impl.MemoryFederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterHeartbeatRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GPGContext;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GPGContextImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for Sub-cluster Cleaner in GPG.
 */
public class TestSubClusterCleaner {

  private Configuration conf;
  private MemoryFederationStateStore stateStore;
  private FederationStateStoreFacade facade;
  private SubClusterCleaner cleaner;
  private GPGContext gpgContext;

  private static final long TWO_SECONDS = TimeUnit.SECONDS.toMillis(2);

  private ArrayList<SubClusterId> subClusterIds;

  @Before
  public void setup() throws YarnException {
    conf = new YarnConfiguration();

    // subcluster expires in one second
    conf.setLong(YarnConfiguration.GPG_SUBCLUSTER_EXPIRATION_MS, 1000);

    stateStore = new MemoryFederationStateStore();
    stateStore.init(conf);

    facade = FederationStateStoreFacade.getInstance();
    facade.reinitialize(stateStore, conf);

    gpgContext = new GPGContextImpl();
    gpgContext.setStateStoreFacade(facade);

    cleaner = new SubClusterCleaner(conf, gpgContext);

    // Create and register six sub clusters
    subClusterIds = new ArrayList<SubClusterId>();
    for (int i = 0; i < 3; i++) {
      // Create sub cluster id and info
      SubClusterId subClusterId =
          SubClusterId.newInstance("SUBCLUSTER-" + Integer.toString(i));

      SubClusterInfo subClusterInfo = SubClusterInfo.newInstance(subClusterId,
          "1.2.3.4:1", "1.2.3.4:2", "1.2.3.4:3", "1.2.3.4:4",
          SubClusterState.SC_RUNNING, System.currentTimeMillis(), "");
      // Register the sub cluster
      stateStore.registerSubCluster(
          SubClusterRegisterRequest.newInstance(subClusterInfo));
      // Append the id to a local list
      subClusterIds.add(subClusterId);
    }
  }

  @After
  public void breakDown() throws Exception {
    stateStore.close();
  }

  @Test
  public void testSubClusterRegisterHeartBeatTime() throws YarnException {
    cleaner.run();
    Assert.assertEquals(3, facade.getSubClusters(true, true).size());
  }

  /**
   * Test the base use case.
   */
  @Test
  public void testSubClusterHeartBeat() throws YarnException {
    // The first subcluster reports as Unhealthy
    SubClusterId subClusterId = subClusterIds.get(0);
    stateStore.subClusterHeartbeat(SubClusterHeartbeatRequest
        .newInstance(subClusterId, SubClusterState.SC_UNHEALTHY, "capacity"));

    // The second subcluster didn't heartbeat for two seconds, should mark lost
    subClusterId = subClusterIds.get(1);
    stateStore.setSubClusterLastHeartbeat(subClusterId,
        System.currentTimeMillis() - TWO_SECONDS);

    cleaner.run();
    Assert.assertEquals(1, facade.getSubClusters(true, true).size());
  }
}