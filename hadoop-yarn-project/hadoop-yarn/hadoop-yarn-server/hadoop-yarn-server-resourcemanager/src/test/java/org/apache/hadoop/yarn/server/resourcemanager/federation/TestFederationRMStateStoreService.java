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

package org.apache.hadoop.yarn.server.resourcemanager.federation;

import java.io.IOException;
import java.io.StringReader;
import java.net.UnknownHostException;

import javax.xml.bind.JAXBException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.api.json.JSONJAXBContext;
import com.sun.jersey.api.json.JSONUnmarshaller;

/**
 * Unit tests for FederationStateStoreService.
 */
public class TestFederationRMStateStoreService {

  private final HAServiceProtocol.StateChangeRequestInfo requestInfo =
      new HAServiceProtocol.StateChangeRequestInfo(
          HAServiceProtocol.RequestSource.REQUEST_BY_USER);
  private final SubClusterId subClusterId = SubClusterId.newInstance("SC-1");
  private final GetSubClusterInfoRequest request =
      GetSubClusterInfoRequest.newInstance(subClusterId);

  private Configuration conf;
  private FederationStateStore stateStore;
  private long lastHearbeatTS = 0;
  private JSONJAXBContext jc;
  private JSONUnmarshaller unmarshaller;

  @Before
  public void setUp() throws IOException, YarnException, JAXBException {
    conf = new YarnConfiguration();
    jc = new JSONJAXBContext(
        JSONConfiguration.mapped().rootUnwrapping(false).build(),
        ClusterMetricsInfo.class);
    unmarshaller = jc.createJSONUnmarshaller();
  }

  @After
  public void tearDown() throws Exception {
    unmarshaller = null;
    jc = null;
  }

  @Test
  public void testFederationStateStoreService() throws Exception {
    conf.setBoolean(YarnConfiguration.FEDERATION_ENABLED, true);
    conf.set(YarnConfiguration.RM_CLUSTER_ID, subClusterId.getId());
    final MockRM rm = new MockRM(conf);

    // Initially there should be no entry for the sub-cluster
    rm.init(conf);
    stateStore = rm.getFederationStateStoreService().getStateStoreClient();
    GetSubClusterInfoResponse response = stateStore.getSubCluster(request);
    Assert.assertNull(response);

    // Validate if sub-cluster is registered
    rm.start();
    String capability = checkSubClusterInfo(SubClusterState.SC_NEW);
    Assert.assertTrue(capability.isEmpty());

    // Heartbeat to see if sub-cluster transitions to running
    FederationStateStoreHeartbeat storeHeartbeat =
        rm.getFederationStateStoreService().getStateStoreHeartbeatThread();
    storeHeartbeat.run();
    capability = checkSubClusterInfo(SubClusterState.SC_RUNNING);
    checkClusterMetricsInfo(capability, 0);

    // heartbeat again after adding a node.
    rm.registerNode("127.0.0.1:1234", 4 * 1024);
    storeHeartbeat.run();
    capability = checkSubClusterInfo(SubClusterState.SC_RUNNING);
    checkClusterMetricsInfo(capability, 1);

    // Validate sub-cluster deregistration
    rm.getFederationStateStoreService()
        .deregisterSubCluster(SubClusterDeregisterRequest
            .newInstance(subClusterId, SubClusterState.SC_UNREGISTERED));
    checkSubClusterInfo(SubClusterState.SC_UNREGISTERED);

    // check after failover
    explicitFailover(rm);

    capability = checkSubClusterInfo(SubClusterState.SC_NEW);
    Assert.assertTrue(capability.isEmpty());

    // Heartbeat to see if sub-cluster transitions to running
    storeHeartbeat =
        rm.getFederationStateStoreService().getStateStoreHeartbeatThread();
    storeHeartbeat.run();
    capability = checkSubClusterInfo(SubClusterState.SC_RUNNING);
    checkClusterMetricsInfo(capability, 0);

    // heartbeat again after adding a node.
    rm.registerNode("127.0.0.1:1234", 4 * 1024);
    storeHeartbeat.run();
    capability = checkSubClusterInfo(SubClusterState.SC_RUNNING);
    checkClusterMetricsInfo(capability, 1);

    rm.stop();
  }

  private void explicitFailover(MockRM rm) throws IOException {
    rm.getAdminService().transitionToStandby(requestInfo);
    Assert.assertTrue(rm.getRMContext()
        .getHAServiceState() == HAServiceProtocol.HAServiceState.STANDBY);
    rm.getAdminService().transitionToActive(requestInfo);
    Assert.assertTrue(rm.getRMContext()
        .getHAServiceState() == HAServiceProtocol.HAServiceState.ACTIVE);
    lastHearbeatTS = 0;
    stateStore = rm.getFederationStateStoreService().getStateStoreClient();
  }

  private void checkClusterMetricsInfo(String capability, int numNodes)
      throws JAXBException {
    ClusterMetricsInfo clusterMetricsInfo = unmarshaller.unmarshalFromJSON(
        new StringReader(capability), ClusterMetricsInfo.class);
    Assert.assertEquals(numNodes, clusterMetricsInfo.getTotalNodes());
  }

  private String checkSubClusterInfo(SubClusterState state)
      throws YarnException, UnknownHostException {
    Assert.assertNotNull(stateStore.getSubCluster(request));
    SubClusterInfo response =
        stateStore.getSubCluster(request).getSubClusterInfo();
    Assert.assertEquals(state, response.getState());
    Assert.assertTrue(response.getLastHeartBeat() >= lastHearbeatTS);
    String expectedAddress =
        (response.getClientRMServiceAddress().split(":"))[0];
    Assert.assertEquals(expectedAddress,
        (response.getAMRMServiceAddress().split(":"))[0]);
    Assert.assertEquals(expectedAddress,
        (response.getRMAdminServiceAddress().split(":"))[0]);
    Assert.assertEquals(expectedAddress,
        (response.getRMWebServiceAddress().split(":"))[0]);
    lastHearbeatTS = response.getLastHeartBeat();
    return response.getCapability();
  }

}
