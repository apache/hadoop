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

import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.slider.api.proto.Messages;
import org.apache.slider.api.types.NodeInformation;
import org.apache.slider.api.types.NodeInformationList;
import org.apache.slider.api.types.RestTypeMarshalling;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.server.appmaster.model.mock.MockFactory;
import org.apache.slider.server.appmaster.model.mock.MockRoleHistory;
import org.apache.slider.server.appmaster.state.NodeEntry;
import org.apache.slider.server.appmaster.state.NodeInstance;
import org.apache.slider.server.appmaster.state.NodeMap;
import org.apache.slider.server.appmaster.state.RoleHistory;
import org.apache.slider.utils.SliderTestBase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Test anti-affine placement.
 */
public class TestRoleHistoryAA extends SliderTestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestRoleHistoryAA.class);

  private List<String> hostnames = Arrays.asList("1", "2", "3");
  private NodeMap nodeMap, gpuNodeMap;
  private RoleHistory roleHistory = new MockRoleHistory(MockFactory.ROLES);

  public TestRoleHistoryAA() throws BadConfigException {
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    nodeMap = createNodeMap(hostnames, NodeState.RUNNING, "");
    gpuNodeMap = createNodeMap(hostnames, NodeState.RUNNING, "GPU");
  }

  //@Test
  public void testFindNodesInFullCluster() throws Throwable {
    // all three will surface at first
    verifyResultSize(3, nodeMap.findAllNodesForRole(1, ""));
  }

  //@Test
  public void testFindNodesInUnhealthyCluster() throws Throwable {
    // all three will surface at first
    markNodeOneUnhealthy();
    verifyResultSize(2, nodeMap.findAllNodesForRole(1, ""));
  }

  public boolean markNodeOneUnhealthy() {
    return setNodeState(nodeMap.get("1"), NodeState.UNHEALTHY);
  }

  protected boolean setNodeState(NodeInstance node, NodeState state) {
    return node.updateNode(MockFactory.INSTANCE.newNodeReport(node.hostname,
        state, ""));
  }

  //@Test
  public void testFindNoNodesWrongLabel() throws Throwable {
    // all three will surface at first
    verifyResultSize(0, nodeMap.findAllNodesForRole(1, "GPU"));
  }

  //@Test
  public void testFindSomeNodesSomeLabel() throws Throwable {
    // all three will surface at first
    update(nodeMap,
        Arrays.asList(MockFactory.INSTANCE.newNodeReport("1", NodeState
            .RUNNING, "GPU")));
    List<NodeInstance> gpuNodes = nodeMap.findAllNodesForRole(1, "GPU");
    verifyResultSize(1, gpuNodes);
    NodeInstance instance = gpuNodes.get(0);
    instance.getOrCreate(1).onStarting();
    assertFalse(instance.canHost(1, "GPU"));
    assertFalse(instance.canHost(1, ""));
    verifyResultSize(0, nodeMap.findAllNodesForRole(1, "GPU"));

  }

  //@Test
  public void testFindNoNodesRightLabel() throws Throwable {
    // all three will surface at first
    verifyResultSize(3, gpuNodeMap.findAllNodesForRole(1, "GPU"));
  }

  //@Test
  public void testFindNoNodesNoLabel() throws Throwable {
    // all three will surface at first
    verifyResultSize(3, gpuNodeMap.findAllNodesForRole(1, ""));
  }

  //@Test
  public void testFindNoNodesClusterRequested() throws Throwable {
    // all three will surface at first
    for (NodeInstance ni : nodeMap.values()) {
      ni.getOrCreate(1).request();
    }
    assertNoAvailableNodes(1);
  }

  //@Test
  public void testFindNoNodesClusterBusy() throws Throwable {
    // all three will surface at first
    for (NodeInstance ni : nodeMap.values()) {
      ni.getOrCreate(1).request();
    }
    assertNoAvailableNodes(1);
  }

  /**
   * Tag all nodes as starting, then walk one through a bit
   * more of its lifecycle.
   */
  //@Test
  public void testFindNoNodesLifecycle() throws Throwable {
    // all three will surface at first
    for (NodeInstance ni : nodeMap.values()) {
      ni.getOrCreate(1).onStarting();
    }
    assertNoAvailableNodes(1);

    // walk one of the nodes through the lifecycle
    NodeInstance node1 = nodeMap.get("1");
    assertFalse(node1.canHost(1, ""));
    node1.get(1).onStartCompleted();
    assertFalse(node1.canHost(1, ""));
    assertNoAvailableNodes(1);
    node1.get(1).release();
    assertTrue(node1.canHost(1, ""));
    List<NodeInstance> list2 =
        verifyResultSize(1, nodeMap.findAllNodesForRole(1, ""));
    assertEquals(list2.get(0).hostname, "1");

    // now tag that node as unhealthy and expect it to go away
    markNodeOneUnhealthy();
    assertNoAvailableNodes(1);
  }

  //@Test
  public void testRolesIndependent() throws Throwable {
    NodeInstance node1 = nodeMap.get("1");
    NodeEntry role1 = node1.getOrCreate(1);
    NodeEntry role2 = node1.getOrCreate(2);
    for (NodeInstance ni : nodeMap.values()) {
      ni.updateNode(MockFactory.INSTANCE.newNodeReport("0", NodeState
          .UNHEALTHY, ""));
    }
    assertNoAvailableNodes(1);
    assertNoAvailableNodes(2);
    assertTrue(setNodeState(node1, NodeState.RUNNING));
    // tag role 1 as busy
    role1.onStarting();
    assertNoAvailableNodes(1);

    verifyResultSize(1, nodeMap.findAllNodesForRole(2, ""));
    assertTrue(node1.canHost(2, ""));
  }

  //@Test
  public void testNodeEntryAvailablity() throws Throwable {
    NodeEntry entry = new NodeEntry(1);
    assertTrue(entry.isAvailable());
    entry.onStarting();
    assertFalse(entry.isAvailable());
    entry.onStartCompleted();
    assertFalse(entry.isAvailable());
    entry.release();
    assertTrue(entry.isAvailable());
    entry.onStarting();
    assertFalse(entry.isAvailable());
    entry.onStartFailed();
    assertTrue(entry.isAvailable());
  }

  //@Test
  public void testNodeInstanceSerialization() throws Throwable {
    MockRoleHistory rh2 = new MockRoleHistory(new ArrayList<>());
    rh2.getOrCreateNodeInstance("localhost");
    NodeInstance instance = rh2.getOrCreateNodeInstance("localhost");
    instance.getOrCreate(1).onStartCompleted();
    Map<Integer, String> naming = Collections.singletonMap(1, "manager");
    NodeInformation ni = instance.serialize(naming);
    assertEquals(1, ni.entries.get("manager").live);
    NodeInformation ni2 = rh2.getNodeInformation("localhost", naming);
    assertEquals(1, ni2.entries.get("manager").live);
    Map<String, NodeInformation> info = rh2.getNodeInformationSnapshot(naming);
    assertEquals(1, info.get("localhost").entries.get("manager").live);
    NodeInformationList nil = new NodeInformationList(info.values());
    assertEquals(1, nil.get(0).entries.get("manager").live);

    Messages.NodeInformationProto nodeInformationProto =
        RestTypeMarshalling.marshall(ni);
    Messages.NodeEntryInformationProto entryProto = nodeInformationProto
        .getEntries(0);
    assertNotNull(entryProto);
    assertEquals(1, entryProto.getPriority());
    NodeInformation unmarshalled =
        RestTypeMarshalling.unmarshall(nodeInformationProto);
    assertEquals(unmarshalled.hostname, ni.hostname);
    assertTrue(unmarshalled.entries.keySet().containsAll(ni.entries.keySet()));

  }

  //@Test
  public void testBuildRolenames() throws Throwable {

  }
  public List<NodeInstance> assertNoAvailableNodes(int role) {
    String label = "";
    return verifyResultSize(0, nodeMap.findAllNodesForRole(role, label));
  }

  List<NodeInstance> verifyResultSize(int size, List<NodeInstance> list) {
    if (list.size() != size) {
      for (NodeInstance ni : list) {
        LOG.error(ni.toFullString());
      }
    }
    assertEquals(size, list.size());
    return list;
  }

  NodeMap createNodeMap(List<NodeReport> nodeReports)
      throws BadConfigException {
    NodeMap newNodeMap = new NodeMap(1);
    update(newNodeMap, nodeReports);
    return newNodeMap;
  }

  protected boolean update(NodeMap nm, List<NodeReport> nodeReports) {
    return nm.buildOrUpdate(nodeReports);
  }

  NodeMap createNodeMap(List<String> hosts, NodeState state,
      String label) throws BadConfigException {
    return createNodeMap(MockFactory.INSTANCE.createNodeReports(hosts, state,
        label));
  }
}
