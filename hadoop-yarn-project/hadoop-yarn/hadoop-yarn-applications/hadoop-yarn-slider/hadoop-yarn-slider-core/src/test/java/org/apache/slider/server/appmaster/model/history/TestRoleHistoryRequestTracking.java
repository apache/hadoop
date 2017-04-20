/*
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

package org.apache.slider.server.appmaster.model.history;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.providers.PlacementPolicy;
import org.apache.slider.providers.ProviderRole;
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest;
import org.apache.slider.server.appmaster.model.mock.MockContainer;
import org.apache.slider.server.appmaster.model.mock.MockFactory;
import org.apache.slider.server.appmaster.model.mock.MockRoleHistory;
import org.apache.slider.server.appmaster.state.ContainerAllocationOutcome;
import org.apache.slider.server.appmaster.state.NodeEntry;
import org.apache.slider.server.appmaster.state.NodeInstance;
import org.apache.slider.server.appmaster.state.OutstandingRequest;
import org.apache.slider.server.appmaster.state.RoleHistory;
import org.apache.slider.server.appmaster.state.RoleStatus;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Test the RH availability list and request tracking: that hosts
 * get removed and added.
 */
public class TestRoleHistoryRequestTracking extends BaseMockAppStateTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestRoleHistoryRequestTracking.class);

  private String roleName = "test";

  private NodeInstance age1Active4;
  private NodeInstance age2Active2;
  private NodeInstance age2Active0;
  private NodeInstance age3Active0;
  private NodeInstance age4Active1;

  private RoleHistory roleHistory = new MockRoleHistory(MockFactory.ROLES);
  // 1MB, 1 vcore
  private Resource resource = Resource.newInstance(1, 1);

  private RoleStatus roleStatus;

  public TestRoleHistoryRequestTracking() throws BadConfigException {
  }

  AMRMClient.ContainerRequest requestContainer(RoleStatus rs) {
    return roleHistory.requestContainerForRole(rs).getIssuedRequest();
  }

  @Override
  public String getTestName() {
    return "TestRoleHistoryAvailableList";
  }

  @Override
  public void setup() throws Exception {
    super.setup();

    age1Active4 = nodeInstance(1, 4, 0, 0);
    age2Active2 = nodeInstance(2, 2, 0, 1);
    age2Active0 = nodeInstance(2, 0, 0, 0);
    age3Active0 = nodeInstance(3, 0, 0, 0);
    age4Active1 = nodeInstance(4, 1, 0, 0);

    roleHistory.insert(Arrays.asList(age2Active2, age2Active0, age4Active1,
        age1Active4, age3Active0));
    roleHistory.buildRecentNodeLists();
    roleStatus = getRole0Status();
    roleStatus.setResourceRequirements(Resource.newInstance(1, 1));
  }

  @Test
  public void testAvailableListBuiltForRoles() throws Throwable {
    List<NodeInstance> available0 = roleHistory.cloneRecentNodeList(
        roleStatus.getKey());
    assertListEquals(Arrays.asList(age3Active0, age2Active0), available0);
  }

  @Test
  public void testRequestedNodeOffList() throws Throwable {
    NodeInstance ni = roleHistory.findRecentNodeForNewInstance(roleStatus);
    assertEquals(age3Active0, ni);
    assertListEquals(Arrays.asList(age2Active0),
        roleHistory.cloneRecentNodeList(roleStatus.getKey()));
    roleHistory.requestInstanceOnNode(ni,
        roleStatus,
        resource
    );
  }

  @Test
  public void testRequestedNodeOffListWithFailures() throws Throwable {
    assertFalse(roleHistory.cloneRecentNodeList(roleStatus.getKey()).isEmpty());

    NodeEntry age3role0 = recordAsFailed(age3Active0, roleStatus.getKey(), 4);
    assertTrue(age3Active0.isConsideredUnreliable(roleStatus.getKey(),
        roleStatus.getNodeFailureThreshold()));
    recordAsFailed(age2Active0, roleStatus.getKey(), 4);
    assertTrue(age2Active0.isConsideredUnreliable(roleStatus.getKey(),
        roleStatus.getNodeFailureThreshold()));
    // expect to get a null node back
    NodeInstance ni = roleHistory.findRecentNodeForNewInstance(roleStatus);
    assertNull(ni);

    // which is translated to a no-location request
    AMRMClient.ContainerRequest req = roleHistory.requestInstanceOnNode(ni,
        roleStatus,
        resource).getIssuedRequest();

    assertNull(req.getNodes());

    LOG.info("resetting failure count");
    age3role0.resetFailedRecently();
    roleHistory.dump();
    assertEquals(0, age3role0.getFailedRecently());
    assertFalse(age3Active0.isConsideredUnreliable(roleStatus.getKey(),
        roleStatus.getNodeFailureThreshold()));
    assertFalse(roleHistory.cloneRecentNodeList(roleStatus.getKey()).isEmpty());
    // looking for a node should now find one
    ni = roleHistory.findRecentNodeForNewInstance(roleStatus);
    assertEquals(ni, age3Active0);
    req = roleHistory.requestInstanceOnNode(ni, roleStatus, resource)
        .getIssuedRequest();
    assertEquals(1, req.getNodes().size());
  }

  /**
   * Verify that strict placement policies generate requests for nodes
   * irrespective of their failed status.
   * @throws Throwable
   */
  @Test
  public void testStrictPlacementIgnoresFailures() throws Throwable {

    RoleStatus targetRole = getRole1Status();
    final ProviderRole providerRole1 = targetRole.getProviderRole();
    assertEquals(providerRole1.placementPolicy, PlacementPolicy.STRICT);
    int key1 = targetRole.getKey();
    int key0 = getRole0Status().getKey();

    List<NodeInstance> nodes0 = Arrays.asList(age1Active4, age2Active0,
        age2Active2, age3Active0, age4Active1);
    recordAllFailed(key0, 4, nodes0);
    recordAllFailed(key1, 4, nodes0);

    // trigger a list rebuild
    roleHistory.buildRecentNodeLists();
    List<NodeInstance> recentRole0 = roleHistory.cloneRecentNodeList(key0);
    assertTrue(recentRole0.indexOf(age3Active0) < recentRole0
        .indexOf(age2Active0));

    // the non-strict role has no suitable nodes
    assertNull(roleHistory.findRecentNodeForNewInstance(getRole0Status()));


    NodeInstance ni = roleHistory.findRecentNodeForNewInstance(targetRole);
    assertNotNull(ni);

    NodeInstance ni2 = roleHistory.findRecentNodeForNewInstance(targetRole);
    assertNotNull(ni2);
    assertNotEquals(ni, ni2);
  }

  @Test
  public void testFindAndRequestNode() throws Throwable {
    AMRMClient.ContainerRequest req = requestContainer(roleStatus);

    assertEquals(age3Active0.hostname, req.getNodes().get(0));
    List<NodeInstance> a2 = roleHistory.cloneRecentNodeList(roleStatus
        .getKey());
    assertListEquals(Arrays.asList(age2Active0), a2);
  }

  @Test
  public void testRequestedNodeIntoReqList() throws Throwable {
    requestContainer(roleStatus);
    List<OutstandingRequest> requests = roleHistory.listPlacedRequests();
    assertEquals(1, requests.size());
    assertEquals(age3Active0.hostname, requests.get(0).hostname);
  }

  @Test
  public void testCompletedRequestDropsNode() throws Throwable {
    AMRMClient.ContainerRequest req = requestContainer(roleStatus);
    List<OutstandingRequest> requests = roleHistory.listPlacedRequests();
    assertEquals(1, requests.size());
    String hostname = requests.get(0).hostname;
    assertEquals(age3Active0.hostname, hostname);
    assertEquals(hostname, req.getNodes().get(0));
    MockContainer container = factory.newContainer(req, hostname);
    assertOnContainerAllocated(container, 2, 1);
    assertNoOutstandingPlacedRequests();
  }

  public void assertOnContainerAllocated(Container c1, int p1, int p2) {
    assertNotEquals(ContainerAllocationOutcome.Open, roleHistory
        .onContainerAllocated(c1, p1, p2).outcome);
  }

  public void assertOnContainerAllocationOpen(Container c1, int p1, int p2) {
    assertEquals(ContainerAllocationOutcome.Open, roleHistory
        .onContainerAllocated(c1, p1, p2).outcome);
  }

  void assertNoOutstandingPlacedRequests() {
    assertTrue(roleHistory.listPlacedRequests().isEmpty());
  }

  public void assertOutstandingPlacedRequests(int i) {
    assertEquals(i, roleHistory.listPlacedRequests().size());
  }

  @Test
  public void testTwoRequests() throws Throwable {
    AMRMClient.ContainerRequest req = requestContainer(roleStatus);
    AMRMClient.ContainerRequest req2 = requestContainer(roleStatus);
    List<OutstandingRequest> requests = roleHistory.listPlacedRequests();
    assertEquals(2, requests.size());
    MockContainer container = factory.newContainer(req, req.getNodes().get(0));
    assertOnContainerAllocated(container, 2, 1);
    assertOutstandingPlacedRequests(1);
    container = factory.newContainer(req2, req2.getNodes().get(0));
    assertOnContainerAllocated(container, 2, 2);
    assertNoOutstandingPlacedRequests();
  }

  @Test
  public void testThreeRequestsOneUnsatisified() throws Throwable {
    AMRMClient.ContainerRequest req = requestContainer(roleStatus);
    AMRMClient.ContainerRequest req2 = requestContainer(roleStatus);
    AMRMClient.ContainerRequest req3 = requestContainer(roleStatus);
    List<OutstandingRequest> requests = roleHistory.listPlacedRequests();
    assertEquals(2, requests.size());
    MockContainer container = factory.newContainer(req, req.getNodes().get(0));
    assertOnContainerAllocated(container, 2, 1);
    assertOutstandingPlacedRequests(1);

    container = factory.newContainer(req3, "three");
    assertOnContainerAllocationOpen(container, 3, 2);
    assertOutstandingPlacedRequests(1);

    // the final allocation will trigger a cleanup
    container = factory.newContainer(req2, "four");
    // no node dropped
    assertEquals(ContainerAllocationOutcome.Unallocated,
           roleHistory.onContainerAllocated(container, 3, 3).outcome);
    // yet the list is now empty
    assertNoOutstandingPlacedRequests();
    roleHistory.listOpenRequests().isEmpty();

    // and the remainder goes onto the available list
    List<NodeInstance> a2 = roleHistory.cloneRecentNodeList(roleStatus
        .getKey());
    assertListEquals(Arrays.asList(age2Active0), a2);
  }

  @Test
  public void testThreeRequests() throws Throwable {
    AMRMClient.ContainerRequest req = requestContainer(roleStatus);
    AMRMClient.ContainerRequest req2 = requestContainer(roleStatus);
    AMRMClient.ContainerRequest req3 = requestContainer(roleStatus);
    assertOutstandingPlacedRequests(2);
    assertNull(req3.getNodes());
    MockContainer container = factory.newContainer(req, req.getNodes().get(0));
    assertOnContainerAllocated(container, 3, 1);
    assertOutstandingPlacedRequests(1);
    container = factory.newContainer(req2, req2.getNodes().get(0));
    assertOnContainerAllocated(container, 3, 2);
    assertNoOutstandingPlacedRequests();
    container = factory.newContainer(req3, "three");
    assertOnContainerAllocationOpen(container, 3, 3);
    assertNoOutstandingPlacedRequests();
  }

}
