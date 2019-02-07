/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestRMServerUtils {
  @Test
  public void testGetApplicableNodeCountForAMLocality() throws Exception {
    List<NodeId> rack1Nodes = new ArrayList<>();
    for (int i = 0; i < 29; i++) {
      rack1Nodes.add(NodeId.newInstance("host" + i, 1234));
    }
    NodeId node1 = NodeId.newInstance("node1", 1234);
    NodeId node2 = NodeId.newInstance("node2", 1234);
    rack1Nodes.add(node2);

    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, false);
    ResourceScheduler scheduler = Mockito.mock(ResourceScheduler.class);
    Mockito.when(scheduler.getNumClusterNodes()).thenReturn(100);
    Mockito.when(scheduler.getNodeIds("/rack1")).thenReturn(rack1Nodes);
    Mockito.when(scheduler.getNodeIds("node1"))
        .thenReturn(Collections.singletonList(node1));
    Mockito.when(scheduler.getNodeIds("node2"))
        .thenReturn(Collections.singletonList(node2));
    RMContext rmContext = Mockito.mock(RMContext.class);
    Mockito.when(rmContext.getScheduler()).thenReturn(scheduler);

    ResourceRequest anyReq = createResourceRequest(ResourceRequest.ANY,
        true, null);
    List<ResourceRequest> reqs = new ArrayList<>();
    reqs.add(anyReq);
    Assert.assertEquals(100,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));

    ResourceRequest rackReq = createResourceRequest("/rack1", true, null);
    reqs.add(rackReq);
    Assert.assertEquals(30,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    anyReq.setRelaxLocality(false);
    Assert.assertEquals(30,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    rackReq.setRelaxLocality(false);
    Assert.assertEquals(100,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));

    ResourceRequest node1Req = createResourceRequest("node1", false, null);
    reqs.add(node1Req);
    Assert.assertEquals(100,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    node1Req.setRelaxLocality(true);
    Assert.assertEquals(1,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    rackReq.setRelaxLocality(true);
    Assert.assertEquals(31,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));

    ResourceRequest node2Req = createResourceRequest("node2", false, null);
    reqs.add(node2Req);
    Assert.assertEquals(31,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    node2Req.setRelaxLocality(true);
    Assert.assertEquals(31,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    rackReq.setRelaxLocality(false);
    Assert.assertEquals(2,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    node1Req.setRelaxLocality(false);
    Assert.assertEquals(1,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    node2Req.setRelaxLocality(false);
    Assert.assertEquals(100,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
  }

  @Test
  public void testQueryRMNodes() throws Exception {
    RMContext rmContext = Mockito.mock(RMContext.class);
    NodeId node1 = NodeId.newInstance("node1", 1234);
    RMNode rmNode1 = Mockito.mock(RMNode.class);
    ConcurrentMap<NodeId, RMNode> inactiveList =
        new ConcurrentHashMap<NodeId, RMNode>();
    Mockito.when(rmNode1.getState()).thenReturn(NodeState.SHUTDOWN);
    inactiveList.put(node1, rmNode1);
    Mockito.when(rmContext.getInactiveRMNodes()).thenReturn(inactiveList);
    List<RMNode> result = RMServerUtils.queryRMNodes(rmContext,
        EnumSet.of(NodeState.SHUTDOWN));
    Assert.assertTrue(result.size() != 0);
    Assert.assertEquals(result.get(0), rmNode1);
    Mockito.when(rmNode1.getState()).thenReturn(NodeState.DECOMMISSIONED);
    result = RMServerUtils.queryRMNodes(rmContext,
        EnumSet.of(NodeState.DECOMMISSIONED));
    Assert.assertTrue(result.size() != 0);
    Assert.assertEquals(result.get(0), rmNode1);
    Mockito.when(rmNode1.getState()).thenReturn(NodeState.LOST);
    result = RMServerUtils.queryRMNodes(rmContext,
        EnumSet.of(NodeState.LOST));
    Assert.assertTrue(result.size() != 0);
    Assert.assertEquals(result.get(0), rmNode1);
    Mockito.when(rmNode1.getState()).thenReturn(NodeState.REBOOTED);
    result = RMServerUtils.queryRMNodes(rmContext,
        EnumSet.of(NodeState.REBOOTED));
    Assert.assertTrue(result.size() != 0);
    Assert.assertEquals(result.get(0), rmNode1);
  }

  @Test
  public void testGetApplicableNodeCountForAMLabels() throws Exception {
    Set<NodeId> noLabelNodes = new HashSet<>();
    for (int i = 0; i < 80; i++) {
      noLabelNodes.add(NodeId.newInstance("host" + i, 1234));
    }
    Set<NodeId> label1Nodes = new HashSet<>();
    for (int i = 80; i < 90; i++) {
      label1Nodes.add(NodeId.newInstance("host" + i, 1234));
    }
    label1Nodes.add(NodeId.newInstance("host101", 0));
    label1Nodes.add(NodeId.newInstance("host102", 0));
    Map<String, Set<NodeId>> label1NodesMap = new HashMap<>();
    label1NodesMap.put("label1", label1Nodes);

    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, true);
    ResourceScheduler scheduler = Mockito.mock(ResourceScheduler.class);
    Mockito.when(scheduler.getNumClusterNodes()).thenReturn(100);
    RMContext rmContext = Mockito.mock(RMContext.class);
    Mockito.when(rmContext.getScheduler()).thenReturn(scheduler);
    RMNodeLabelsManager labMan = Mockito.mock(RMNodeLabelsManager.class);
    Mockito.when(labMan.getNodesWithoutALabel()).thenReturn(noLabelNodes);
    Mockito.when(labMan.getLabelsToNodes(Collections.singleton("label1")))
        .thenReturn(label1NodesMap);
    Mockito.when(rmContext.getNodeLabelManager()).thenReturn(labMan);

    ResourceRequest anyReq = createResourceRequest(ResourceRequest.ANY,
        true, null);
    List<ResourceRequest> reqs = new ArrayList<>();
    reqs.add(anyReq);
    Assert.assertEquals(80,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    anyReq.setNodeLabelExpression("label1");
    Assert.assertEquals(10,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
  }

  @Test
  public void testGetApplicableNodeCountForAMLocalityAndLabels()
      throws Exception {
    List<NodeId> rack1Nodes = new ArrayList<>();
    for (int i = 0; i < 29; i++) {
      rack1Nodes.add(NodeId.newInstance("host" + i, 1234));
    }
    NodeId node1 = NodeId.newInstance("node1", 1234);
    NodeId node2 = NodeId.newInstance("node2", 1234);
    rack1Nodes.add(node2);
    Set<NodeId> noLabelNodes = new HashSet<>();
    for (int i = 0; i < 19; i++) {
      noLabelNodes.add(rack1Nodes.get(i));
    }
    noLabelNodes.add(node2);
    for (int i = 29; i < 89; i++) {
      noLabelNodes.add(NodeId.newInstance("host" + i, 1234));
    }
    Set<NodeId> label1Nodes = new HashSet<>();
    label1Nodes.add(node1);
    for (int i = 89; i < 93; i++) {
      label1Nodes.add(NodeId.newInstance("host" + i, 1234));
    }
    for (int i = 19; i < 29; i++) {
      label1Nodes.add(rack1Nodes.get(i));
    }
    label1Nodes.add(NodeId.newInstance("host101", 0));
    label1Nodes.add(NodeId.newInstance("host102", 0));
    Map<String, Set<NodeId>> label1NodesMap = new HashMap<>();
    label1NodesMap.put("label1", label1Nodes);

    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, true);
    ResourceScheduler scheduler = Mockito.mock(ResourceScheduler.class);
    Mockito.when(scheduler.getNumClusterNodes()).thenReturn(100);
    Mockito.when(scheduler.getNodeIds("/rack1")).thenReturn(rack1Nodes);
    Mockito.when(scheduler.getNodeIds("node1"))
        .thenReturn(Collections.singletonList(node1));
    Mockito.when(scheduler.getNodeIds("node2"))
        .thenReturn(Collections.singletonList(node2));
    RMContext rmContext = Mockito.mock(RMContext.class);
    Mockito.when(rmContext.getScheduler()).thenReturn(scheduler);
    RMNodeLabelsManager labMan = Mockito.mock(RMNodeLabelsManager.class);
    Mockito.when(labMan.getNodesWithoutALabel()).thenReturn(noLabelNodes);
    Mockito.when(labMan.getLabelsToNodes(Collections.singleton("label1")))
        .thenReturn(label1NodesMap);
    Mockito.when(rmContext.getNodeLabelManager()).thenReturn(labMan);

    ResourceRequest anyReq = createResourceRequest(ResourceRequest.ANY,
        true, null);
    List<ResourceRequest> reqs = new ArrayList<>();
    reqs.add(anyReq);
    Assert.assertEquals(80,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));

    ResourceRequest rackReq = createResourceRequest("/rack1", true, null);
    reqs.add(rackReq);
    Assert.assertEquals(20,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    anyReq.setRelaxLocality(false);
    Assert.assertEquals(20,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    rackReq.setRelaxLocality(false);
    Assert.assertEquals(80,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));

    ResourceRequest node1Req = createResourceRequest("node1", false, null);
    reqs.add(node1Req);
    Assert.assertEquals(80,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    node1Req.setRelaxLocality(true);
    Assert.assertEquals(0,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    rackReq.setRelaxLocality(true);
    Assert.assertEquals(20,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));

    ResourceRequest node2Req = createResourceRequest("node2", false, null);
    reqs.add(node2Req);
    Assert.assertEquals(20,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    node2Req.setRelaxLocality(true);
    Assert.assertEquals(20,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    rackReq.setRelaxLocality(false);
    Assert.assertEquals(1,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    node1Req.setRelaxLocality(false);
    Assert.assertEquals(1,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    node2Req.setRelaxLocality(false);
    Assert.assertEquals(80,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));

    anyReq.setNodeLabelExpression("label1");
    rackReq.setNodeLabelExpression("label1");
    node1Req.setNodeLabelExpression("label1");
    node2Req.setNodeLabelExpression("label1");
    anyReq.setRelaxLocality(true);
    reqs = new ArrayList<>();
    reqs.add(anyReq);
    Assert.assertEquals(15,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));

    rackReq.setRelaxLocality(true);
    reqs.add(rackReq);
    Assert.assertEquals(10,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    anyReq.setRelaxLocality(false);
    Assert.assertEquals(10,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    rackReq.setRelaxLocality(false);
    Assert.assertEquals(15,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));

    node1Req.setRelaxLocality(false);
    reqs.add(node1Req);
    Assert.assertEquals(15,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    node1Req.setRelaxLocality(true);
    Assert.assertEquals(1,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    rackReq.setRelaxLocality(true);
    Assert.assertEquals(11,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));

    node2Req.setRelaxLocality(false);
    reqs.add(node2Req);
    Assert.assertEquals(11,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    node2Req.setRelaxLocality(true);
    Assert.assertEquals(11,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    rackReq.setRelaxLocality(false);
    Assert.assertEquals(1,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    node1Req.setRelaxLocality(false);
    Assert.assertEquals(0,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
    node2Req.setRelaxLocality(false);
    Assert.assertEquals(15,
        RMServerUtils.getApplicableNodeCountForAM(rmContext, conf, reqs));
  }

  private ResourceRequest createResourceRequest(String resource,
      boolean relaxLocality, String nodeLabel) {
    return ResourceRequest.newInstance(Priority.newInstance(0),
        resource, Resource.newInstance(1, 1), 1, relaxLocality, nodeLabel);
  }
}
