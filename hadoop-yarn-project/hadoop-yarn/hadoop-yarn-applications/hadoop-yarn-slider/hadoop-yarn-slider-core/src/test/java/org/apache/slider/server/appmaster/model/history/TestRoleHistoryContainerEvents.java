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
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.slider.api.ResourceKeys;
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest;
import org.apache.slider.server.appmaster.model.mock.MockContainer;
import org.apache.slider.server.appmaster.model.mock.MockNodeId;
import org.apache.slider.server.appmaster.state.ContainerOutcome;
import org.apache.slider.server.appmaster.state.ContainerPriority;
import org.apache.slider.server.appmaster.state.NodeEntry;
import org.apache.slider.server.appmaster.state.NodeInstance;
import org.apache.slider.server.appmaster.state.NodeMap;
import org.apache.slider.server.appmaster.state.RoleHistory;
import org.apache.slider.server.appmaster.state.RoleInstance;
import org.apache.slider.server.appmaster.state.RoleStatus;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Test container events at the role history level -one below
 * the App State.
 */
public class TestRoleHistoryContainerEvents extends BaseMockAppStateTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestRoleHistoryContainerEvents.class);

  @Override
  public String getTestName() {
    return "TestRoleHistoryContainerEvents";
  }

  private NodeInstance age1Active4;
  private NodeInstance age2Active2;
  private NodeInstance age3Active0;
  private NodeInstance age4Active1;
  private NodeInstance age2Active0;

  private RoleHistory roleHistory;

  private Resource resource;

  AMRMClient.ContainerRequest requestContainer(RoleStatus roleStatus) {
    return roleHistory.requestContainerForRole(roleStatus).getIssuedRequest();
  }

  @Override
  public void setup() throws Exception {
    super.setup();

    age1Active4 = nodeInstance(1, 4, 0, 0);
    age2Active2 = nodeInstance(2, 2, 0, 1);
    age3Active0 = nodeInstance(3, 0, 0, 0);
    age4Active1 = nodeInstance(4, 1, 0, 0);
    age2Active0 = nodeInstance(2, 0, 0, 0);

    roleHistory = appState.getRoleHistory();
    roleHistory.insert(Arrays.asList(age2Active2, age2Active0,
        age4Active1, age1Active4, age3Active0));
    roleHistory.buildRecentNodeLists();
    resource = Resource.newInstance(ResourceKeys.DEF_YARN_CORES,
                                    ResourceKeys.DEF_YARN_MEMORY);
  }

  //@Test
  public void testFindAndCreate() throws Throwable {
    RoleStatus roleStatus = getRole0Status();

    AMRMClient.ContainerRequest request =
        requestContainer(roleStatus);

    List<String> requestNodes = request.getNodes();
    assertNotNull(requestNodes);
    assertEquals(1, requestNodes.size());
    String hostname = requestNodes.get(0);
    assertEquals(hostname, age3Active0.hostname);

    //build a container
    MockContainer container = factory.newContainer();
    container.setNodeId(new MockNodeId(hostname, 0));
    container.setPriority(request.getPriority());
    roleHistory.onContainerAssigned(container);

    NodeMap nodemap = roleHistory.cloneNodemap();
    NodeInstance allocated = nodemap.get(hostname);
    NodeEntry roleEntry = allocated.get(roleStatus.getKey());
    assertEquals(1, roleEntry.getStarting());
    assertFalse(roleEntry.isAvailable());
    RoleInstance ri = new RoleInstance(container);
    //start it
    roleHistory.onContainerStartSubmitted(container, ri);
    //later, declare that it started
    roleHistory.onContainerStarted(container);
    assertEquals(0, roleEntry.getStarting());
    assertFalse(roleEntry.isAvailable());
    assertEquals(1, roleEntry.getActive());
    assertEquals(1, roleEntry.getLive());
  }

  //@Test
  public void testCreateAndRelease() throws Throwable {
    RoleStatus roleStatus = getRole1Status();

    //verify it is empty
    assertTrue(roleHistory.listActiveNodes(roleStatus.getKey()).isEmpty());

    AMRMClient.ContainerRequest request =
        requestContainer(roleStatus);

    assertNull(request.getNodes());

    //pick an idle host
    String hostname = age3Active0.hostname;

    //build a container
    MockContainer container = factory.newContainer(new MockNodeId(hostname,
        0), request.getPriority());
    roleHistory.onContainerAssigned(container);

    NodeMap nodemap = roleHistory.cloneNodemap();
    NodeInstance allocated = nodemap.get(hostname);
    NodeEntry roleEntry = allocated.get(roleStatus.getKey());
    assertEquals(1, roleEntry.getStarting());
    assertFalse(roleEntry.isAvailable());
    RoleInstance ri = new RoleInstance(container);
    //start it
    roleHistory.onContainerStartSubmitted(container, ri);
    //later, declare that it started
    roleHistory.onContainerStarted(container);
    assertEquals(0, roleEntry.getStarting());
    assertFalse(roleEntry.isAvailable());
    assertEquals(1, roleEntry.getActive());
    assertEquals(1, roleEntry.getLive());

    // now pick that instance to destroy
    List<NodeInstance> activeNodes = roleHistory.listActiveNodes(roleStatus
        .getKey());


    assertEquals(1, activeNodes.size());
    NodeInstance target = activeNodes.get(0);
    assertEquals(target, allocated);
    roleHistory.onContainerReleaseSubmitted(container);
    assertEquals(1, roleEntry.getReleasing());
    assertEquals(1, roleEntry.getLive());
    assertEquals(0, roleEntry.getActive());

    // release completed
    roleHistory.onReleaseCompleted(container);
    assertEquals(0, roleEntry.getReleasing());
    assertEquals(0, roleEntry.getLive());
    assertEquals(0, roleEntry.getActive());

    // verify it is empty
    assertTrue(roleHistory.listActiveNodes(roleStatus.getKey()).isEmpty());

    // ask for a container and expect to get the recently released one
    AMRMClient.ContainerRequest request2 =
        requestContainer(roleStatus);

    List<String> nodes2 = request2.getNodes();
    assertNotNull(nodes2);
    String hostname2 = nodes2.get(0);

    //pick an idle host
    assertEquals(hostname2, age3Active0.hostname);
  }


  //@Test
  public void testStartWithoutWarning() throws Throwable {
    //pick an idle host
    String hostname = age3Active0.hostname;
    //build a container
    MockContainer container = factory.newContainer(
        new MockNodeId(hostname, 0),
        ContainerPriority.createPriority(getRole0Status().getKey(), false));

    NodeMap nodemap = roleHistory.cloneNodemap();
    NodeInstance allocated = nodemap.get(hostname);
    NodeEntry roleEntry = allocated.get(getRole0Status().getKey());

    //tell RH that it started
    roleHistory.onContainerStarted(container);
    assertEquals(0, roleEntry.getStarting());
    assertFalse(roleEntry.isAvailable());
    assertEquals(1, roleEntry.getActive());
    assertEquals(1, roleEntry.getLive());
  }

  //@Test
  public void testStartFailed() throws Throwable {
    RoleStatus roleStatus = getRole0Status();

    AMRMClient.ContainerRequest request =
        requestContainer(roleStatus);

    LOG.info("req {}", request);
    LOG.info("{}", request.getNodes());
    String hostname = request.getNodes().get(0);
    assertEquals(hostname, age3Active0.hostname);

    //build a container
    MockContainer container = factory.newContainer(new MockNodeId(hostname,
        0), request.getPriority());
    roleHistory.onContainerAssigned(container);

    NodeMap nodemap = roleHistory.cloneNodemap();
    NodeInstance allocated = nodemap.get(hostname);
    NodeEntry roleEntry = allocated.get(roleStatus.getKey());
    assertEquals(1, roleEntry.getStarting());
    assertFalse(roleEntry.isAvailable());
    RoleInstance ri = new RoleInstance(container);
    //start it
    roleHistory.onContainerStartSubmitted(container, ri);
    //later, declare that it failed on startup
    assertFalse(roleHistory.onNodeManagerContainerStartFailed(container));
    assertEquals(0, roleEntry.getStarting());
    assertEquals(1, roleEntry.getStartFailed());
    assertEquals(1, roleEntry.getFailed());
    assertTrue(roleEntry.isAvailable());
    assertEquals(0, roleEntry.getActive());
    assertEquals(0, roleEntry.getLive());
  }

  //@Test
  public void testStartFailedWithoutWarning() throws Throwable {
    RoleStatus roleStatus = getRole0Status();

    AMRMClient.ContainerRequest request =
        requestContainer(roleStatus);

    String hostname = request.getNodes().get(0);
    assertEquals(hostname, age3Active0.hostname);

    //build a container
    MockContainer container = factory.newContainer();
    container.setNodeId(new MockNodeId(hostname, 0));
    container.setPriority(request.getPriority());

    NodeMap nodemap = roleHistory.cloneNodemap();
    NodeInstance allocated = nodemap.get(hostname);
    NodeEntry roleEntry = allocated.get(roleStatus.getKey());

    assertFalse(roleHistory.onNodeManagerContainerStartFailed(container));
    assertEquals(0, roleEntry.getStarting());
    assertEquals(1, roleEntry.getStartFailed());
    assertEquals(1, roleEntry.getFailed());
    assertTrue(roleEntry.isAvailable());
    assertEquals(0, roleEntry.getActive());
    assertEquals(0, roleEntry.getLive());
  }

  //@Test
  public void testContainerFailed() throws Throwable {
    describe("fail a container without declaring it as starting");

    RoleStatus roleStatus = getRole0Status();

    AMRMClient.ContainerRequest request =
        requestContainer(roleStatus);

    String hostname = request.getNodes().get(0);
    assertEquals(hostname, age3Active0.hostname);

    //build a container
    MockContainer container = factory.newContainer();
    container.setNodeId(new MockNodeId(hostname, 0));
    container.setPriority(request.getPriority());
    roleHistory.onContainerAssigned(container);

    NodeMap nodemap = roleHistory.cloneNodemap();
    NodeInstance allocated = nodemap.get(hostname);
    NodeEntry roleEntry = allocated.get(roleStatus.getKey());
    assertEquals(1, roleEntry.getStarting());
    assertFalse(roleEntry.isAvailable());
    RoleInstance ri = new RoleInstance(container);
    //start it
    roleHistory.onContainerStartSubmitted(container, ri);
    roleHistory.onContainerStarted(container);

    //later, declare that it failed
    roleHistory.onFailedContainer(
        container,
        false,
        ContainerOutcome.Failed);
    assertEquals(0, roleEntry.getStarting());
    assertTrue(roleEntry.isAvailable());
    assertEquals(0, roleEntry.getActive());
    assertEquals(0, roleEntry.getLive());
  }

  //@Test
  public void testContainerFailedWithoutWarning() throws Throwable {
    describe("fail a container without declaring it as starting");
    RoleStatus roleStatus = getRole0Status();

    AMRMClient.ContainerRequest request =
        requestContainer(roleStatus);

    String hostname = request.getNodes().get(0);
    assertEquals(hostname, age3Active0.hostname);

    //build a container
    MockContainer container = factory.newContainer();
    container.setNodeId(new MockNodeId(hostname, 0));
    container.setPriority(request.getPriority());


    NodeMap nodemap = roleHistory.cloneNodemap();
    NodeInstance allocated = nodemap.get(hostname);
    NodeEntry roleEntry = allocated.get(roleStatus.getKey());
    assertTrue(roleEntry.isAvailable());
    roleHistory.onFailedContainer(
        container,
        false,
        ContainerOutcome.Failed);
    assertEquals(0, roleEntry.getStarting());
    assertEquals(1, roleEntry.getFailed());
    assertTrue(roleEntry.isAvailable());
    assertEquals(0, roleEntry.getActive());
    assertEquals(0, roleEntry.getLive());
  }

  //@Test
  public void testAllocationListPrep() throws Throwable {
    describe("test prepareAllocationList");
    RoleStatus roleStatus = getRole0Status();

    AMRMClient.ContainerRequest request =
        requestContainer(roleStatus);

    String hostname = request.getNodes().get(0);
    assertEquals(hostname, age3Active0.hostname);

    MockContainer container1 = factory.newContainer();
    container1.setNodeId(new MockNodeId(hostname, 0));
    container1.setPriority(Priority.newInstance(getRole0Status().getKey()));

    MockContainer container2 = factory.newContainer();
    container2.setNodeId(new MockNodeId(hostname, 0));
    container2.setPriority(Priority.newInstance(getRole1Status().getKey()));

    // put containers in List with role == 1 first
    List<Container> containers = Arrays.asList((Container) container2,
        (Container) container1);
    List<Container> sortedContainers = roleHistory.prepareAllocationList(
        containers);

    // verify that the first container has role == 0 after sorting
    MockContainer c1 = (MockContainer) sortedContainers.get(0);
    assertEquals(getRole0Status().getKey(), c1.getPriority().getPriority());
    MockContainer c2 = (MockContainer) sortedContainers.get(1);
    assertEquals(getRole1Status().getKey(), c2.getPriority().getPriority());
  }

  //@Test
  public void testNodeUpdated() throws Throwable {
    describe("fail a node");

    RoleStatus roleStatus = getRole0Status();

    AMRMClient.ContainerRequest request =
        roleHistory.requestContainerForRole(roleStatus).getIssuedRequest();

    String hostname = request.getNodes().get(0);
    assertEquals(age3Active0.hostname, hostname);

    // build a container
    MockContainer container = factory.newContainer(new MockNodeId(hostname,
        0), request.getPriority());

    roleHistory.onContainerAssigned(container);

    NodeMap nodemap = roleHistory.cloneNodemap();
    NodeInstance allocated = nodemap.get(hostname);
    NodeEntry roleEntry = allocated.get(roleStatus.getKey());
    assertEquals(1, roleEntry.getStarting());
    assertFalse(roleEntry.isAvailable());
    RoleInstance ri = new RoleInstance(container);
    // start it
    roleHistory.onContainerStartSubmitted(container, ri);
    roleHistory.onContainerStarted(container);

    int startSize = nodemap.size();

    // now send a list of updated (failed) nodes event
    List<NodeReport> nodesUpdated = new ArrayList<>();
    NodeReport nodeReport = NodeReport.newInstance(
        NodeId.newInstance(hostname, 0),
        NodeState.LOST,
        null, null, null, null, 1, null, 0);
    nodesUpdated.add(nodeReport);
    roleHistory.onNodesUpdated(nodesUpdated);

    nodemap = roleHistory.cloneNodemap();
    int endSize = nodemap.size();
    // as even unused nodes are added to the list, we expect the map size to
    // be >1
    assertTrue(startSize <= endSize);
    assertNotNull(nodemap.get(hostname));
    assertFalse(nodemap.get(hostname).isOnline());

    // add a failure of a node we've never head of
    String newhost = "newhost";
    nodesUpdated = Arrays.asList(
        NodeReport.newInstance(
            NodeId.newInstance(newhost, 0),
            NodeState.LOST,
            null, null, null, null, 1, null, 0)
    );
    roleHistory.onNodesUpdated(nodesUpdated);

    NodeMap nodemap2 = roleHistory.cloneNodemap();
    assertNotNull(nodemap2.get(newhost));
    assertFalse(nodemap2.get(newhost).isOnline());

  }
}
