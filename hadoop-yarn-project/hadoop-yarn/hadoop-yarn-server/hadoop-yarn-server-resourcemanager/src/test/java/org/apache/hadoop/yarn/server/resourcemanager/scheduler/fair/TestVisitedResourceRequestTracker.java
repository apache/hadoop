/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at*
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ClusterNodeTracker;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

import java.util.List;

public class TestVisitedResourceRequestTracker {
  private final ClusterNodeTracker<FSSchedulerNode>
      nodeTracker = new ClusterNodeTracker<>();
  private final ResourceRequest
      anyRequest, rackRequest, node1Request, node2Request;

  private final String NODE_VISITED = "The node is already visited. ";
  private final String RACK_VISITED = "The rack is already visited. ";
  private final String ANY_VISITED = "ANY is already visited. ";
  private final String NODE_FAILURE = "The node is visited again.";
  private final String RACK_FAILURE = "The rack is visited again.";
  private final String ANY_FAILURE = "ANY is visited again.";
  private final String FIRST_CALL_FAILURE = "First call to visit failed.";

  public TestVisitedResourceRequestTracker() {
    List<RMNode> rmNodes =
        MockNodes.newNodes(1, 2, Resources.createResource(8192, 8));

    FSSchedulerNode node1 = new FSSchedulerNode(rmNodes.get(0), false);
    nodeTracker.addNode(node1);
    node1Request = createRR(node1.getNodeName(), 1);

    FSSchedulerNode node2 = new FSSchedulerNode(rmNodes.get(1), false);
    node2Request = createRR(node2.getNodeName(), 1);
    nodeTracker.addNode(node2);

    anyRequest = createRR(ResourceRequest.ANY, 2);
    rackRequest = createRR(node1.getRackName(), 2);
  }

  private ResourceRequest createRR(String resourceName, int count) {
    return ResourceRequest.newInstance(
        Priority.UNDEFINED, resourceName, Resources.none(), count);
  }

  @Test
  public void testVisitAnyRequestFirst() {
    VisitedResourceRequestTracker tracker =
        new VisitedResourceRequestTracker(nodeTracker);

    // Visit ANY request first
    assertTrue(FIRST_CALL_FAILURE, tracker.visit(anyRequest));

    // All other requests should return false
    assertFalse(ANY_VISITED + RACK_FAILURE, tracker.visit(rackRequest));
    assertFalse(ANY_VISITED + NODE_FAILURE, tracker.visit(node1Request));
    assertFalse(ANY_VISITED + NODE_FAILURE, tracker.visit(node2Request));
  }

  @Test
  public void testVisitRackRequestFirst() {
    VisitedResourceRequestTracker tracker =
        new VisitedResourceRequestTracker(nodeTracker);

    // Visit rack request first
    assertTrue(FIRST_CALL_FAILURE, tracker.visit(rackRequest));

    // All other requests should return false
    assertFalse(RACK_VISITED + ANY_FAILURE, tracker.visit(anyRequest));
    assertFalse(RACK_VISITED + NODE_FAILURE, tracker.visit(node1Request));
    assertFalse(RACK_VISITED + NODE_FAILURE, tracker.visit(node2Request));
  }

  @Test
  public void testVisitNodeRequestFirst() {
    VisitedResourceRequestTracker tracker =
        new VisitedResourceRequestTracker(nodeTracker);

    // Visit node1 first
    assertTrue(FIRST_CALL_FAILURE, tracker.visit(node1Request));

    // Rack and ANY should return false
    assertFalse(NODE_VISITED + ANY_FAILURE, tracker.visit(anyRequest));
    assertFalse(NODE_VISITED + RACK_FAILURE, tracker.visit(rackRequest));

    // The other node should return true
    assertTrue(NODE_VISITED + "Different node visit failed",
        tracker.visit(node2Request));
  }
}
