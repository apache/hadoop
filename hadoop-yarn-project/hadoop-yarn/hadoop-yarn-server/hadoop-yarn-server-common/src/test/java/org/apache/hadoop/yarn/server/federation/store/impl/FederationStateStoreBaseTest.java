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

package org.apache.hadoop.yarn.server.federation.store.impl;

import java.io.IOException;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.FederationMembershipStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterHeartbeatRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Base class for FederationMembershipStateStore implementations.
 */
public abstract class FederationStateStoreBaseTest {

  private static final MonotonicClock CLOCK = new MonotonicClock();

  private FederationMembershipStateStore stateStore;

  @Before
  public void before() throws IOException {
    stateStore = getCleanStateStore();
  }

  @After
  public void after() {
    stateStore = null;
  }

  protected abstract FederationMembershipStateStore getCleanStateStore();

  @Test
  public void testRegisterSubCluster() throws Exception {
    SubClusterId subClusterId = SubClusterId.newInstance("SC");
    SubClusterInfo subClusterInfo = createSubClusterInfo(subClusterId);

    SubClusterRegisterResponse result = stateStore.registerSubCluster(
        SubClusterRegisterRequest.newInstance(subClusterInfo));

    Assert.assertNotNull(result);
    Assert.assertEquals(subClusterInfo, querySubClusterInfo(subClusterId));
  }

  @Test
  public void testDeregisterSubCluster() throws Exception {
    SubClusterId subClusterId = SubClusterId.newInstance("SC");
    SubClusterInfo subClusterInfo = createSubClusterInfo(subClusterId);

    stateStore.registerSubCluster(
        SubClusterRegisterRequest.newInstance(subClusterInfo));

    SubClusterDeregisterRequest deregisterRequest = SubClusterDeregisterRequest
        .newInstance(subClusterId, SubClusterState.SC_UNREGISTERED);

    stateStore.deregisterSubCluster(deregisterRequest);

    Assert.assertEquals(SubClusterState.SC_UNREGISTERED,
        querySubClusterInfo(subClusterId).getState());
  }

  @Test
  public void testDeregisterSubClusterUnknownSubCluster() throws Exception {
    SubClusterId subClusterId = SubClusterId.newInstance("SC");

    SubClusterDeregisterRequest deregisterRequest = SubClusterDeregisterRequest
        .newInstance(subClusterId, SubClusterState.SC_UNREGISTERED);
    try {
      stateStore.deregisterSubCluster(deregisterRequest);
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertTrue(e.getMessage().startsWith("SubCluster SC not found"));
    }
  }

  @Test
  public void testGetSubClusterInfo() throws Exception {

    SubClusterId subClusterId = SubClusterId.newInstance("SC");
    SubClusterInfo subClusterInfo = createSubClusterInfo(subClusterId);

    stateStore.registerSubCluster(
        SubClusterRegisterRequest.newInstance(subClusterInfo));

    GetSubClusterInfoRequest request =
        GetSubClusterInfoRequest.newInstance(subClusterId);
    Assert.assertEquals(subClusterInfo,
        stateStore.getSubCluster(request).getSubClusterInfo());
  }

  @Test
  public void testGetSubClusterInfoUnknownSubCluster() throws Exception {
    SubClusterId subClusterId = SubClusterId.newInstance("SC");
    GetSubClusterInfoRequest request =
        GetSubClusterInfoRequest.newInstance(subClusterId);

    try {
      stateStore.getSubCluster(request).getSubClusterInfo();
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertTrue(
          e.getMessage().startsWith("Subcluster SC does not exist"));
    }
  }

  @Test
  public void testGetAllSubClustersInfo() throws Exception {

    SubClusterId subClusterId1 = SubClusterId.newInstance("SC1");
    SubClusterInfo subClusterInfo1 = createSubClusterInfo(subClusterId1);

    SubClusterId subClusterId2 = SubClusterId.newInstance("SC2");
    SubClusterInfo subClusterInfo2 = createSubClusterInfo(subClusterId2);

    stateStore.registerSubCluster(
        SubClusterRegisterRequest.newInstance(subClusterInfo1));
    stateStore.registerSubCluster(
        SubClusterRegisterRequest.newInstance(subClusterInfo2));

    stateStore.subClusterHeartbeat(SubClusterHeartbeatRequest
        .newInstance(subClusterId1, SubClusterState.SC_RUNNING, ""));
    stateStore.subClusterHeartbeat(SubClusterHeartbeatRequest
        .newInstance(subClusterId2, SubClusterState.SC_UNHEALTHY, ""));

    Assert.assertTrue(
        stateStore.getSubClusters(GetSubClustersInfoRequest.newInstance(true))
            .getSubClusters().contains(subClusterInfo1));
    Assert.assertFalse(
        stateStore.getSubClusters(GetSubClustersInfoRequest.newInstance(true))
            .getSubClusters().contains(subClusterInfo2));

    Assert.assertTrue(
        stateStore.getSubClusters(GetSubClustersInfoRequest.newInstance(false))
            .getSubClusters().contains(subClusterInfo1));
    Assert.assertTrue(
        stateStore.getSubClusters(GetSubClustersInfoRequest.newInstance(false))
            .getSubClusters().contains(subClusterInfo2));
  }

  @Test
  public void testSubClusterHeartbeat() throws Exception {
    SubClusterId subClusterId = SubClusterId.newInstance("SC");
    SubClusterInfo subClusterInfo = createSubClusterInfo(subClusterId);

    stateStore.registerSubCluster(
        SubClusterRegisterRequest.newInstance(subClusterInfo));

    SubClusterHeartbeatRequest heartbeatRequest = SubClusterHeartbeatRequest
        .newInstance(subClusterId, SubClusterState.SC_RUNNING, "cabability");
    stateStore.subClusterHeartbeat(heartbeatRequest);

    Assert.assertEquals(SubClusterState.SC_RUNNING,
        querySubClusterInfo(subClusterId).getState());
    Assert.assertNotNull(querySubClusterInfo(subClusterId).getLastHeartBeat());
  }

  @Test
  public void testSubClusterHeartbeatUnknownSubCluster() throws Exception {
    SubClusterId subClusterId = SubClusterId.newInstance("SC");
    SubClusterHeartbeatRequest heartbeatRequest = SubClusterHeartbeatRequest
        .newInstance(subClusterId, SubClusterState.SC_RUNNING, "cabability");

    try {
      stateStore.subClusterHeartbeat(heartbeatRequest);
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertTrue(e.getMessage()
          .startsWith("Subcluster SC does not exist; cannot heartbeat"));
    }
  }

  private SubClusterInfo createSubClusterInfo(SubClusterId subClusterId) {

    String amRMAddress = "1.2.3.4:1";
    String clientRMAddress = "1.2.3.4:2";
    String rmAdminAddress = "1.2.3.4:3";
    String webAppAddress = "1.2.3.4:4";

    return SubClusterInfo.newInstance(subClusterId, amRMAddress,
        clientRMAddress, rmAdminAddress, webAppAddress, SubClusterState.SC_NEW,
        CLOCK.getTime(), "cabability");
  }

  private SubClusterInfo querySubClusterInfo(SubClusterId subClusterId)
      throws YarnException {
    GetSubClusterInfoRequest request =
        GetSubClusterInfoRequest.newInstance(subClusterId);
    return stateStore.getSubCluster(request).getSubClusterInfo();
  }

}
