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

package org.apache.hadoop.yarn.server.resourcemanager.nodelabels;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.nodelabels.NodeLabel;
import org.apache.hadoop.yarn.nodelabels.NodeLabelTestBase;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class TestRMNodeLabelsManager extends NodeLabelTestBase {
  private final Resource EMPTY_RESOURCE = Resource.newInstance(0, 0);
  private final Resource SMALL_RESOURCE = Resource.newInstance(100, 0);
  private final Resource LARGE_NODE = Resource.newInstance(1000, 0);
  
  NullRMNodeLabelsManager mgr = null;

  @Before
  public void before() {
    mgr = new NullRMNodeLabelsManager();
    Configuration conf = new Configuration();
    conf.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, true);
    mgr.init(conf);
    mgr.start();
  }

  @After
  public void after() {
    mgr.stop();
  }
  
  @Test(timeout = 5000)
  public void testGetLabelResourceWhenNodeActiveDeactive() throws Exception {
    mgr.addToCluserNodeLabels(toSet("p1", "p2", "p3"));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1"), toSet("p1"),
        toNodeId("n2"), toSet("p2"), toNodeId("n3"), toSet("p3")));

    Assert.assertEquals(mgr.getResourceByLabel("p1", null), EMPTY_RESOURCE);
    Assert.assertEquals(mgr.getResourceByLabel("p2", null), EMPTY_RESOURCE);
    Assert.assertEquals(mgr.getResourceByLabel("p3", null), EMPTY_RESOURCE);
    Assert.assertEquals(mgr.getResourceByLabel(RMNodeLabelsManager.NO_LABEL, null),
        EMPTY_RESOURCE);

    // active two NM to n1, one large and one small
    mgr.activateNode(NodeId.newInstance("n1", 1), SMALL_RESOURCE);
    mgr.activateNode(NodeId.newInstance("n1", 2), LARGE_NODE);
    Assert.assertEquals(mgr.getResourceByLabel("p1", null),
        Resources.add(SMALL_RESOURCE, LARGE_NODE));

    // check add labels multiple times shouldn't overwrite
    // original attributes on labels like resource
    mgr.addToCluserNodeLabels(toSet("p1", "p4"));
    Assert.assertEquals(mgr.getResourceByLabel("p1", null),
        Resources.add(SMALL_RESOURCE, LARGE_NODE));
    Assert.assertEquals(mgr.getResourceByLabel("p4", null), EMPTY_RESOURCE);

    // change the large NM to small, check if resource updated
    mgr.updateNodeResource(NodeId.newInstance("n1", 2), SMALL_RESOURCE);
    Assert.assertEquals(mgr.getResourceByLabel("p1", null),
        Resources.multiply(SMALL_RESOURCE, 2));

    // deactive one NM, and check if resource updated
    mgr.deactivateNode(NodeId.newInstance("n1", 1));
    Assert.assertEquals(mgr.getResourceByLabel("p1", null), SMALL_RESOURCE);

    // continus deactive, check if resource updated
    mgr.deactivateNode(NodeId.newInstance("n1", 2));
    Assert.assertEquals(mgr.getResourceByLabel("p1", null), EMPTY_RESOURCE);

    // Add two NM to n1 back
    mgr.activateNode(NodeId.newInstance("n1", 1), SMALL_RESOURCE);
    mgr.activateNode(NodeId.newInstance("n1", 2), LARGE_NODE);

    // And remove p1, now the two NM should come to default label,
    mgr.removeFromClusterNodeLabels(ImmutableSet.of("p1"));
    Assert.assertEquals(mgr.getResourceByLabel(RMNodeLabelsManager.NO_LABEL, null),
        Resources.add(SMALL_RESOURCE, LARGE_NODE));
  }
  
  @Test(timeout = 5000)
  public void testActivateNodeManagerWithZeroPort() throws Exception {
    // active two NM, one is zero port , another is non-zero port. no exception
    // should be raised
    mgr.activateNode(NodeId.newInstance("n1", 0), SMALL_RESOURCE);
    mgr.activateNode(NodeId.newInstance("n1", 2), LARGE_NODE);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test(timeout = 5000)
  public void testGetLabelResource() throws Exception {
    mgr.addToCluserNodeLabels(toSet("p1", "p2", "p3"));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1"), toSet("p1"),
        toNodeId("n2"), toSet("p2"), toNodeId("n3"), toSet("p3")));

    // active two NM to n1, one large and one small
    mgr.activateNode(NodeId.newInstance("n1", 1), SMALL_RESOURCE);
    mgr.activateNode(NodeId.newInstance("n2", 1), SMALL_RESOURCE);
    mgr.activateNode(NodeId.newInstance("n3", 1), SMALL_RESOURCE);

    // change label of n1 to p2
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1"), toSet("p2")));
    Assert.assertEquals(mgr.getResourceByLabel("p1", null), EMPTY_RESOURCE);
    Assert.assertEquals(mgr.getResourceByLabel("p2", null),
        Resources.multiply(SMALL_RESOURCE, 2));
    Assert.assertEquals(mgr.getResourceByLabel("p3", null), SMALL_RESOURCE);

    // add more labels
    mgr.addToCluserNodeLabels(toSet("p4", "p5", "p6"));
    mgr.replaceLabelsOnNode((Map) ImmutableMap.of(toNodeId("n4"), toSet("p1"),
        toNodeId("n5"), toSet("p2"), toNodeId("n6"), toSet("p3"),
        toNodeId("n7"), toSet("p4"), toNodeId("n8"), toSet("p5")));

    // now node -> label is,
    // p1 : n4
    // p2 : n1, n2, n5
    // p3 : n3, n6
    // p4 : n7
    // p5 : n8
    // no-label : n9

    // active these nodes
    mgr.activateNode(NodeId.newInstance("n4", 1), SMALL_RESOURCE);
    mgr.activateNode(NodeId.newInstance("n5", 1), SMALL_RESOURCE);
    mgr.activateNode(NodeId.newInstance("n6", 1), SMALL_RESOURCE);
    mgr.activateNode(NodeId.newInstance("n7", 1), SMALL_RESOURCE);
    mgr.activateNode(NodeId.newInstance("n8", 1), SMALL_RESOURCE);
    mgr.activateNode(NodeId.newInstance("n9", 1), SMALL_RESOURCE);

    // check varibles
    Assert.assertEquals(mgr.getResourceByLabel("p1", null), SMALL_RESOURCE);
    Assert.assertEquals(mgr.getResourceByLabel("p2", null),
        Resources.multiply(SMALL_RESOURCE, 3));
    Assert.assertEquals(mgr.getResourceByLabel("p3", null),
        Resources.multiply(SMALL_RESOURCE, 2));
    Assert.assertEquals(mgr.getResourceByLabel("p4", null),
        Resources.multiply(SMALL_RESOURCE, 1));
    Assert.assertEquals(mgr.getResourceByLabel("p5", null),
        Resources.multiply(SMALL_RESOURCE, 1));
    Assert.assertEquals(mgr.getResourceByLabel(RMNodeLabelsManager.NO_LABEL, null),
        Resources.multiply(SMALL_RESOURCE, 1));

    // change a bunch of nodes -> labels
    // n4 -> p2
    // n7 -> empty
    // n5 -> p1
    // n8 -> empty
    // n9 -> p1
    //
    // now become:
    // p1 : n5, n9
    // p2 : n1, n2, n4
    // p3 : n3, n6
    // p4 : [ ]
    // p5 : [ ]
    // no label: n8, n7
    mgr.replaceLabelsOnNode((Map) ImmutableMap.of(toNodeId("n4"), toSet("p2"),
        toNodeId("n7"), RMNodeLabelsManager.EMPTY_STRING_SET, toNodeId("n5"),
        toSet("p1"), toNodeId("n8"), RMNodeLabelsManager.EMPTY_STRING_SET,
        toNodeId("n9"), toSet("p1")));

    // check varibles
    Assert.assertEquals(mgr.getResourceByLabel("p1", null),
        Resources.multiply(SMALL_RESOURCE, 2));
    Assert.assertEquals(mgr.getResourceByLabel("p2", null),
        Resources.multiply(SMALL_RESOURCE, 3));
    Assert.assertEquals(mgr.getResourceByLabel("p3", null),
        Resources.multiply(SMALL_RESOURCE, 2));
    Assert.assertEquals(mgr.getResourceByLabel("p4", null),
        Resources.multiply(SMALL_RESOURCE, 0));
    Assert.assertEquals(mgr.getResourceByLabel("p5", null),
        Resources.multiply(SMALL_RESOURCE, 0));
    Assert.assertEquals(mgr.getResourceByLabel("", null),
        Resources.multiply(SMALL_RESOURCE, 2));
  }
  
  @Test(timeout=5000)
  public void testGetQueueResource() throws Exception {
    Resource clusterResource = Resource.newInstance(9999, 1);
    
    /*
     * Node->Labels:
     *   host1 : red
     *   host2 : blue
     *   host3 : yellow
     *   host4 :
     */
    mgr.addToCluserNodeLabels(toSet("red", "blue", "yellow"));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("host1"), toSet("red")));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("host2"), toSet("blue")));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("host3"), toSet("yellow")));
    
    // active two NM to n1, one large and one small
    mgr.activateNode(NodeId.newInstance("host1", 1), SMALL_RESOURCE);
    mgr.activateNode(NodeId.newInstance("host2", 1), SMALL_RESOURCE);
    mgr.activateNode(NodeId.newInstance("host3", 1), SMALL_RESOURCE);
    mgr.activateNode(NodeId.newInstance("host4", 1), SMALL_RESOURCE);
    
    // reinitialize queue
    Set<String> q1Label = toSet("red", "blue");
    Set<String> q2Label = toSet("blue", "yellow");
    Set<String> q3Label = toSet("yellow");
    Set<String> q4Label = RMNodeLabelsManager.EMPTY_STRING_SET;
    Set<String> q5Label = toSet(RMNodeLabelsManager.ANY);
    
    Map<String, Set<String>> queueToLabels = new HashMap<String, Set<String>>();
    queueToLabels.put("Q1", q1Label);
    queueToLabels.put("Q2", q2Label);
    queueToLabels.put("Q3", q3Label);
    queueToLabels.put("Q4", q4Label);
    queueToLabels.put("Q5", q5Label);

    mgr.reinitializeQueueLabels(queueToLabels);
    
    // check resource
    Assert.assertEquals(Resources.multiply(SMALL_RESOURCE, 3),
        mgr.getQueueResource("Q1", q1Label, clusterResource));
    Assert.assertEquals(Resources.multiply(SMALL_RESOURCE, 3),
        mgr.getQueueResource("Q2", q2Label, clusterResource));
    Assert.assertEquals(Resources.multiply(SMALL_RESOURCE, 2),
        mgr.getQueueResource("Q3", q3Label, clusterResource));
    Assert.assertEquals(Resources.multiply(SMALL_RESOURCE, 1),
        mgr.getQueueResource("Q4", q4Label, clusterResource));
    Assert.assertEquals(clusterResource,
        mgr.getQueueResource("Q5", q5Label, clusterResource));
    
    mgr.removeLabelsFromNode(ImmutableMap.of(toNodeId("host2"), toSet("blue")));
    /*
     * Check resource after changes some labels
     * Node->Labels:
     *   host1 : red
     *   host2 : (was: blue)
     *   host3 : yellow
     *   host4 :
     */
    
    // check resource
    Assert.assertEquals(Resources.multiply(SMALL_RESOURCE, 3),
        mgr.getQueueResource("Q1", q1Label, clusterResource));
    Assert.assertEquals(Resources.multiply(SMALL_RESOURCE, 3),
        mgr.getQueueResource("Q2", q2Label, clusterResource));
    Assert.assertEquals(Resources.multiply(SMALL_RESOURCE, 3),
        mgr.getQueueResource("Q3", q3Label, clusterResource));
    Assert.assertEquals(Resources.multiply(SMALL_RESOURCE, 2),
        mgr.getQueueResource("Q4", q4Label, clusterResource));
    Assert.assertEquals(clusterResource,
        mgr.getQueueResource("Q5", q5Label, clusterResource));
    
    /*
     * Check resource after deactive/active some nodes 
     * Node->Labels:
     *   (deactived) host1 : red
     *   host2 :
     *   (deactived and then actived) host3 : yellow
     *   host4 :
     */
    mgr.deactivateNode(NodeId.newInstance("host1", 1));
    mgr.deactivateNode(NodeId.newInstance("host3", 1));
    mgr.activateNode(NodeId.newInstance("host3", 1), SMALL_RESOURCE);
    
    // check resource
    Assert.assertEquals(Resources.multiply(SMALL_RESOURCE, 2),
        mgr.getQueueResource("Q1", q1Label, clusterResource));
    Assert.assertEquals(Resources.multiply(SMALL_RESOURCE, 3),
        mgr.getQueueResource("Q2", q2Label, clusterResource));
    Assert.assertEquals(Resources.multiply(SMALL_RESOURCE, 3),
        mgr.getQueueResource("Q3", q3Label, clusterResource));
    Assert.assertEquals(Resources.multiply(SMALL_RESOURCE, 2),
        mgr.getQueueResource("Q4", q4Label, clusterResource));
    Assert.assertEquals(clusterResource,
        mgr.getQueueResource("Q5", q5Label, clusterResource));
    
    /*
     * Check resource after refresh queue:
     *    Q1: blue
     *    Q2: red, blue
     *    Q3: red
     *    Q4:
     *    Q5: ANY
     */
    q1Label = toSet("blue");
    q2Label = toSet("blue", "red");
    q3Label = toSet("red");
    q4Label = RMNodeLabelsManager.EMPTY_STRING_SET;
    q5Label = toSet(RMNodeLabelsManager.ANY);
    
    queueToLabels.clear();
    queueToLabels.put("Q1", q1Label);
    queueToLabels.put("Q2", q2Label);
    queueToLabels.put("Q3", q3Label);
    queueToLabels.put("Q4", q4Label);
    queueToLabels.put("Q5", q5Label);

    mgr.reinitializeQueueLabels(queueToLabels);
    
    // check resource
    Assert.assertEquals(Resources.multiply(SMALL_RESOURCE, 2),
        mgr.getQueueResource("Q1", q1Label, clusterResource));
    Assert.assertEquals(Resources.multiply(SMALL_RESOURCE, 2),
        mgr.getQueueResource("Q2", q2Label, clusterResource));
    Assert.assertEquals(Resources.multiply(SMALL_RESOURCE, 2),
        mgr.getQueueResource("Q3", q3Label, clusterResource));
    Assert.assertEquals(Resources.multiply(SMALL_RESOURCE, 2),
        mgr.getQueueResource("Q4", q4Label, clusterResource));
    Assert.assertEquals(clusterResource,
        mgr.getQueueResource("Q5", q5Label, clusterResource));
    
    /*
     * Active NMs in nodes already have NM
     * Node->Labels:
     *   host2 :
     *   host3 : yellow (3 NMs)
     *   host4 : (2 NMs)
     */
    mgr.activateNode(NodeId.newInstance("host3", 2), SMALL_RESOURCE);
    mgr.activateNode(NodeId.newInstance("host3", 3), SMALL_RESOURCE);
    mgr.activateNode(NodeId.newInstance("host4", 2), SMALL_RESOURCE);
    
    // check resource
    Assert.assertEquals(Resources.multiply(SMALL_RESOURCE, 3),
        mgr.getQueueResource("Q1", q1Label, clusterResource));
    Assert.assertEquals(Resources.multiply(SMALL_RESOURCE, 3),
        mgr.getQueueResource("Q2", q2Label, clusterResource));
    Assert.assertEquals(Resources.multiply(SMALL_RESOURCE, 3),
        mgr.getQueueResource("Q3", q3Label, clusterResource));
    Assert.assertEquals(Resources.multiply(SMALL_RESOURCE, 3),
        mgr.getQueueResource("Q4", q4Label, clusterResource));
    Assert.assertEquals(clusterResource,
        mgr.getQueueResource("Q5", q5Label, clusterResource));
    
    /*
     * Deactive NMs in nodes already have NMs
     * Node->Labels:
     *   host2 :
     *   host3 : yellow (2 NMs)
     *   host4 : (0 NMs)
     */
    mgr.deactivateNode(NodeId.newInstance("host3", 3));
    mgr.deactivateNode(NodeId.newInstance("host4", 2));
    mgr.deactivateNode(NodeId.newInstance("host4", 1));
    
    // check resource
    Assert.assertEquals(Resources.multiply(SMALL_RESOURCE, 1),
        mgr.getQueueResource("Q1", q1Label, clusterResource));
    Assert.assertEquals(Resources.multiply(SMALL_RESOURCE, 1),
        mgr.getQueueResource("Q2", q2Label, clusterResource));
    Assert.assertEquals(Resources.multiply(SMALL_RESOURCE, 1),
        mgr.getQueueResource("Q3", q3Label, clusterResource));
    Assert.assertEquals(Resources.multiply(SMALL_RESOURCE, 1),
        mgr.getQueueResource("Q4", q4Label, clusterResource));
    Assert.assertEquals(clusterResource,
        mgr.getQueueResource("Q5", q5Label, clusterResource));
  }

  @Test(timeout=5000)
  public void testGetLabelResourceWhenMultipleNMsExistingInSameHost() throws IOException {
    // active two NM to n1, one large and one small
    mgr.activateNode(NodeId.newInstance("n1", 1), SMALL_RESOURCE);
    mgr.activateNode(NodeId.newInstance("n1", 2), SMALL_RESOURCE);
    mgr.activateNode(NodeId.newInstance("n1", 3), SMALL_RESOURCE);
    mgr.activateNode(NodeId.newInstance("n1", 4), SMALL_RESOURCE);
    
    // check resource of no label, it should be small * 4
    Assert.assertEquals(
        mgr.getResourceByLabel(CommonNodeLabelsManager.NO_LABEL, null),
        Resources.multiply(SMALL_RESOURCE, 4));
    
    // change two of these nodes to p1, check resource of no_label and P1
    mgr.addToCluserNodeLabels(toSet("p1"));
    mgr.addLabelsToNode(ImmutableMap.of(toNodeId("n1:1"), toSet("p1"),
        toNodeId("n1:2"), toSet("p1")));
    
    // check resource
    Assert.assertEquals(
        mgr.getResourceByLabel(CommonNodeLabelsManager.NO_LABEL, null),
        Resources.multiply(SMALL_RESOURCE, 2));    
    Assert.assertEquals(
            mgr.getResourceByLabel("p1", null),
            Resources.multiply(SMALL_RESOURCE, 2));
  }

  @Test(timeout = 5000)
  public void testRemoveLabelsFromNode() throws Exception {
    mgr.addToCluserNodeLabels(toSet("p1", "p2", "p3"));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1"), toSet("p1"),
            toNodeId("n2"), toSet("p2"), toNodeId("n3"), toSet("p3")));
    // active one NM to n1:1
    mgr.activateNode(NodeId.newInstance("n1", 1), SMALL_RESOURCE);
    try {
      mgr.removeLabelsFromNode(ImmutableMap.of(toNodeId("n1:1"), toSet("p1")));
      Assert.fail("removeLabelsFromNode should trigger IOException");
    } catch (IOException e) {
    }
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1:1"), toSet("p1")));
    try {
      mgr.removeLabelsFromNode(ImmutableMap.of(toNodeId("n1:1"), toSet("p1")));
    } catch (IOException e) {
      Assert.fail("IOException from removeLabelsFromNode " + e);
    }
  }
  
  @Test(timeout = 5000)
  public void testGetLabelsOnNodesWhenNodeActiveDeactive() throws Exception {
    mgr.addToCluserNodeLabels(toSet("p1", "p2", "p3"));
    mgr.replaceLabelsOnNode(ImmutableMap.of(
        toNodeId("n1"), toSet("p2")));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1:1"), toSet("p1")));
    
    // Active/Deactive a node directly assigned label, should not remove from
    // node->label map
    mgr.activateNode(toNodeId("n1:1"), SMALL_RESOURCE);
    assertCollectionEquals(mgr.getNodeLabels().get(toNodeId("n1:1")),
        toSet("p1"));
    mgr.deactivateNode(toNodeId("n1:1"));
    assertCollectionEquals(mgr.getNodeLabels().get(toNodeId("n1:1")),
        toSet("p1"));
    // Host will not affected
    assertCollectionEquals(mgr.getNodeLabels().get(toNodeId("n1")),
        toSet("p2"));
    
    // Active/Deactive a node doesn't directly assigned label, should remove
    // from node->label map
    mgr.activateNode(toNodeId("n1:2"), SMALL_RESOURCE);
    assertCollectionEquals(mgr.getNodeLabels().get(toNodeId("n1:2")),
        toSet("p2"));
    mgr.deactivateNode(toNodeId("n1:2"));
    Assert.assertNull(mgr.getNodeLabels().get(toNodeId("n1:2")));
    // Host will not affected too
    assertCollectionEquals(mgr.getNodeLabels().get(toNodeId("n1")),
        toSet("p2"));
    
    // When we change label on the host after active a node without directly
    // assigned label, such node will still be removed after deactive
    // Active/Deactive a node doesn't directly assigned label, should remove
    // from node->label map
    mgr.activateNode(toNodeId("n1:2"), SMALL_RESOURCE);
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1"), toSet("p3")));
    assertCollectionEquals(mgr.getNodeLabels().get(toNodeId("n1:2")),
        toSet("p3"));
    mgr.deactivateNode(toNodeId("n1:2"));
    Assert.assertNull(mgr.getNodeLabels().get(toNodeId("n1:2")));
    // Host will not affected too
    assertCollectionEquals(mgr.getNodeLabels().get(toNodeId("n1")),
        toSet("p3"));
    
  }
  
  private void checkNodeLabelInfo(List<NodeLabel> infos, String labelName, int activeNMs, int memory) {
    for (NodeLabel info : infos) {
      if (info.getLabelName().equals(labelName)) {
        Assert.assertEquals(activeNMs, info.getNumActiveNMs());
        Assert.assertEquals(memory, info.getResource().getMemory());
        return;
      }
    }
    Assert.fail("Failed to find info has label=" + labelName);
  }
  
  @Test(timeout = 5000)
  public void testPullRMNodeLabelsInfo() throws IOException {
    mgr.addToCluserNodeLabels(toSet("x", "y", "z"));
    mgr.activateNode(NodeId.newInstance("n1", 1), Resource.newInstance(10, 0));
    mgr.activateNode(NodeId.newInstance("n2", 1), Resource.newInstance(10, 0));
    mgr.activateNode(NodeId.newInstance("n3", 1), Resource.newInstance(10, 0));
    mgr.activateNode(NodeId.newInstance("n4", 1), Resource.newInstance(10, 0));
    mgr.activateNode(NodeId.newInstance("n5", 1), Resource.newInstance(10, 0));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1"), toSet("x"),
        toNodeId("n2"), toSet("x"), toNodeId("n3"), toSet("y")));
    
    // x, y, z and ""
    List<NodeLabel> infos = mgr.pullRMNodeLabelsInfo();
    Assert.assertEquals(4, infos.size());
    checkNodeLabelInfo(infos, RMNodeLabelsManager.NO_LABEL, 2, 20);
    checkNodeLabelInfo(infos, "x", 2, 20);
    checkNodeLabelInfo(infos, "y", 1, 10);
    checkNodeLabelInfo(infos, "z", 0, 0);
  }
  
  @Test(timeout = 5000)
  public void testLabelsToNodesOnNodeActiveDeactive() throws Exception {
    // Activate a node without assigning any labels
    mgr.activateNode(NodeId.newInstance("n1", 1), Resource.newInstance(10, 0));
    Assert.assertTrue(mgr.getLabelsToNodes().isEmpty());
    assertLabelsToNodesEquals(
        mgr.getLabelsToNodes(), transposeNodeToLabels(mgr.getNodeLabels()));

    // Add labels and replace labels on node
    mgr.addToCluserNodeLabels(toSet("p1"));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1"), toSet("p1")));
    // p1 -> n1, n1:1
    Assert.assertEquals(2, mgr.getLabelsToNodes().get("p1").size());
    assertLabelsToNodesEquals(
        mgr.getLabelsToNodes(), transposeNodeToLabels(mgr.getNodeLabels()));

    // Activate a node for which host to label mapping exists
    mgr.activateNode(NodeId.newInstance("n1", 2), Resource.newInstance(10, 0));
    // p1 -> n1, n1:1, n1:2
    Assert.assertEquals(3, mgr.getLabelsToNodes().get("p1").size());
    assertLabelsToNodesEquals(
        mgr.getLabelsToNodes(), transposeNodeToLabels(mgr.getNodeLabels()));

    // Deactivate a node. n1:1 will be removed from the map
    mgr.deactivateNode(NodeId.newInstance("n1", 1));
    // p1 -> n1, n1:2
    Assert.assertEquals(2, mgr.getLabelsToNodes().get("p1").size());
    assertLabelsToNodesEquals(
        mgr.getLabelsToNodes(), transposeNodeToLabels(mgr.getNodeLabels()));
  }

}
