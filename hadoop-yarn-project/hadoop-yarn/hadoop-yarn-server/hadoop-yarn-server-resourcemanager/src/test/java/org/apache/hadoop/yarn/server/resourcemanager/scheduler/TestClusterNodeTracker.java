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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.ClusterMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSSchedulerNode;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test class to verify ClusterNodeTracker. Using FSSchedulerNode without
 * loss of generality.
 */
public class TestClusterNodeTracker {
  private ClusterNodeTracker<FSSchedulerNode> nodeTracker;
  private ClusterMetrics metrics;

  @Before
  public void setup() {
    metrics = ClusterMetrics.getMetrics();
    nodeTracker = new ClusterNodeTracker<>();
  }

  @After
  public void teardown() {
    ClusterMetrics.destroy();
  }

  private void addEight4x4Nodes() {
    MockNodes.resetHostIds();
    List<RMNode> rmNodes =
        MockNodes.newNodes(2, 4, Resource.newInstance(4096, 4));
    for (RMNode rmNode : rmNodes) {
      nodeTracker.addNode(new FSSchedulerNode(rmNode, false));
    }
  }

  @Test
  public void testGetNodeCount() {
    addEight4x4Nodes();
    assertEquals("Incorrect number of nodes in the cluster",
        8, nodeTracker.nodeCount());

    assertEquals("Incorrect number of nodes in each rack",
        4, nodeTracker.nodeCount("rack0"));
  }

  @Test
  public void testIncrCapability() {
    addEight4x4Nodes();
    assertEquals("Cluster Capability Memory incorrect",
        metrics.getCapabilityMB(), (4096 * 8));
    assertEquals("Cluster Capability Vcores incorrect",
        metrics.getCapabilityVirtualCores(), 4 * 8);
  }

  @Test
  public void testGetNodesForResourceName() throws Exception {
    addEight4x4Nodes();
    assertEquals("Incorrect number of nodes matching ANY",
        8, nodeTracker.getNodesByResourceName(ResourceRequest.ANY).size());

    assertEquals("Incorrect number of nodes matching rack",
        4, nodeTracker.getNodesByResourceName("rack0").size());

    assertEquals("Incorrect number of nodes matching node",
        1, nodeTracker.getNodesByResourceName("host0").size());
  }

  @Test
  public void testMaxAllowedAllocation() {
    // Add a third resource
    Configuration conf = new Configuration();

    conf.set(YarnConfiguration.RESOURCE_TYPES, "test1");

    ResourceUtils.resetResourceTypes(conf);
    setup();

    Resource maximum = Resource.newInstance(10240, 10,
        Collections.singletonMap("test1", 10L));

    nodeTracker.setConfiguredMaxAllocation(maximum);

    Resource result = nodeTracker.getMaxAllowedAllocation();

    assertEquals("With no nodes added, the ClusterNodeTracker did not return "
        + "the configured max allocation", maximum, result);

    List<RMNode> smallNodes =
        MockNodes.newNodes(1, 1, Resource.newInstance(1024, 2,
            Collections.singletonMap("test1", 4L)));
    FSSchedulerNode smallNode = new FSSchedulerNode(smallNodes.get(0), false);
    List<RMNode> mediumNodes =
        MockNodes.newNodes(1, 1, Resource.newInstance(4096, 2,
            Collections.singletonMap("test1", 2L)));
    FSSchedulerNode mediumNode = new FSSchedulerNode(mediumNodes.get(0), false);
    List<RMNode> largeNodes =
        MockNodes.newNodes(1, 1, Resource.newInstance(16384, 4,
            Collections.singletonMap("test1", 1L)));
    FSSchedulerNode largeNode = new FSSchedulerNode(largeNodes.get(0), false);

    nodeTracker.addNode(mediumNode);

    result = nodeTracker.getMaxAllowedAllocation();

    assertEquals("With a single node added, the ClusterNodeTracker did not "
        + "return that node's resources as the maximum allocation",
        mediumNodes.get(0).getTotalCapability(), result);

    nodeTracker.addNode(smallNode);

    result = nodeTracker.getMaxAllowedAllocation();

    assertEquals("With two nodes added, the ClusterNodeTracker did not "
        + "return a the maximum allocation that was the max of their aggregate "
        + "resources",
        Resource.newInstance(4096, 2, Collections.singletonMap("test1", 4L)),
        result);

    nodeTracker.removeNode(smallNode.getNodeID());

    result = nodeTracker.getMaxAllowedAllocation();

    assertEquals("After removing a node, the ClusterNodeTracker did not "
        + "recalculate the adjusted maximum allocation correctly",
        mediumNodes.get(0).getTotalCapability(), result);

    nodeTracker.addNode(largeNode);

    result = nodeTracker.getMaxAllowedAllocation();

    assertEquals("With two nodes added, the ClusterNodeTracker did not "
        + "return a the maximum allocation that was the max of their aggregate "
        + "resources",
        Resource.newInstance(10240, 4, Collections.singletonMap("test1", 2L)),
        result);

    nodeTracker.removeNode(largeNode.getNodeID());

    result = nodeTracker.getMaxAllowedAllocation();

    assertEquals("After removing a node, the ClusterNodeTracker did not "
        + "recalculate the adjusted maximum allocation correctly",
        mediumNodes.get(0).getTotalCapability(), result);

    nodeTracker.removeNode(mediumNode.getNodeID());

    result = nodeTracker.getMaxAllowedAllocation();

    assertEquals("After removing all nodes, the ClusterNodeTracker did not "
        + "return the configured maximum allocation", maximum, result);

    nodeTracker.addNode(smallNode);
    nodeTracker.addNode(mediumNode);
    nodeTracker.addNode(largeNode);

    result = nodeTracker.getMaxAllowedAllocation();

    assertEquals("With three nodes added, the ClusterNodeTracker did not "
        + "return a the maximum allocation that was the max of their aggregate "
        + "resources",
        Resource.newInstance(10240, 4, Collections.singletonMap("test1", 4L)),
        result);

    nodeTracker.removeNode(smallNode.getNodeID());
    nodeTracker.removeNode(mediumNode.getNodeID());
    nodeTracker.removeNode(largeNode.getNodeID());

    result = nodeTracker.getMaxAllowedAllocation();

    assertEquals("After removing all nodes, the ClusterNodeTracker did not "
        + "return the configured maximum allocation", maximum, result);
  }
}