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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.event.InlineDispatcher;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.nodelabels.NodeLabelTestBase;
import org.apache.hadoop.yarn.nodelabels.RMNodeLabel;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeLabelsUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
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
  RMNodeLabelsManager lmgr = null;
  boolean checkQueueCall = false;
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
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p1", "p2", "p3"));
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
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p1", "p4"));
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
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p1", "p2", "p3"));
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
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p4", "p5", "p6"));
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
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("red", "blue", "yellow"));
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
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p1"));
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
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p1", "p2", "p3"));
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

  private static class SchedulerEventHandler
      implements EventHandler<SchedulerEvent> {
    Map<NodeId, Set<String>> updatedNodeToLabels = new HashMap<>();
    boolean receivedEvent;

    @Override
    public void handle(SchedulerEvent event) {
      switch (event.getType()) {
      case NODE_LABELS_UPDATE:
        receivedEvent = true;
        updatedNodeToLabels =
            ((NodeLabelsUpdateSchedulerEvent) event).getUpdatedNodeToLabels();
        break;
      default:
        break;
      }
    }
  }

  @Test
  public void testReplaceLabelsFromNode() throws Exception {
    RMContext rmContext = mock(RMContext.class);
    Dispatcher syncDispatcher = new InlineDispatcher();
    SchedulerEventHandler schedEventsHandler = new SchedulerEventHandler();
    syncDispatcher.register(SchedulerEventType.class, schedEventsHandler);
    when(rmContext.getDispatcher()).thenReturn(syncDispatcher);
    mgr.setRMContext(rmContext);

    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p1", "p2", "p3"));
    mgr.activateNode(NodeId.newInstance("n1", 1), SMALL_RESOURCE);
    mgr.activateNode(NodeId.newInstance("n2", 1), SMALL_RESOURCE);
    mgr.activateNode(NodeId.newInstance("n3", 1), SMALL_RESOURCE);

    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1:1"), toSet("p1"),
        toNodeId("n2:1"), toSet("p2"), toNodeId("n3"), toSet("p3")));
    assertTrue("Event should be sent when there is change in labels",
        schedEventsHandler.receivedEvent);
    assertEquals("3 node label mapping modified", 3,
        schedEventsHandler.updatedNodeToLabels.size());
    ImmutableMap<NodeId, Set<String>> modifiedMap =
        ImmutableMap.of(toNodeId("n1:1"), toSet("p1"), toNodeId("n2:1"),
            toSet("p2"), toNodeId("n3:1"), toSet("p3"));
    assertEquals("Node label mapping is not matching", modifiedMap,
        schedEventsHandler.updatedNodeToLabels);
    schedEventsHandler.receivedEvent = false;

    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1:1"), toSet("p1")));
    assertFalse("No event should be sent when there is no change in labels",
        schedEventsHandler.receivedEvent);
    schedEventsHandler.receivedEvent = false;

    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n2:1"), toSet("p1"),
        toNodeId("n3"), toSet("p3")));
    assertTrue("Event should be sent when there is change in labels",
        schedEventsHandler.receivedEvent);
    assertEquals("Single node label mapping modified", 1,
        schedEventsHandler.updatedNodeToLabels.size());
    assertCollectionEquals(toSet("p1"),
        schedEventsHandler.updatedNodeToLabels.get(toNodeId("n2:1")));
    schedEventsHandler.receivedEvent = false;

    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n3"), toSet("p2")));
    assertTrue("Event should be sent when there is change in labels @ HOST",
        schedEventsHandler.receivedEvent);
    assertEquals("Single node label mapping modified", 1,
        schedEventsHandler.updatedNodeToLabels.size());
    assertCollectionEquals(toSet("p2"),
        schedEventsHandler.updatedNodeToLabels.get(toNodeId("n3:1")));
    schedEventsHandler.receivedEvent = false;

    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1"), toSet("p2")));
    assertTrue(
        "Event should be sent when labels are modified at host though labels were set @ NM level",
        schedEventsHandler.receivedEvent);
    assertEquals("Single node label mapping modified", 1,
        schedEventsHandler.updatedNodeToLabels.size());
    assertCollectionEquals(toSet("p2"),
        schedEventsHandler.updatedNodeToLabels.get(toNodeId("n1:1")));
    schedEventsHandler.receivedEvent = false;
  }

  @Test(timeout = 5000)
  public void testGetLabelsOnNodesWhenNodeActiveDeactive() throws Exception {
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p1", "p2", "p3"));
    mgr.replaceLabelsOnNode(ImmutableMap.of(
        toNodeId("n1"), toSet("p2")));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1:1"), toSet("p1")));
    
    // Active/Deactive a node directly assigned label, should not remove from
    // node->label map
    mgr.activateNode(toNodeId("n1:1"), SMALL_RESOURCE);
    assertCollectionEquals(toSet("p1"),
        mgr.getNodeLabels().get(toNodeId("n1:1")));
    mgr.deactivateNode(toNodeId("n1:1"));
    assertCollectionEquals(toSet("p1"),
        mgr.getNodeLabels().get(toNodeId("n1:1")));
    // Host will not affected
    assertCollectionEquals(toSet("p2"),
        mgr.getNodeLabels().get(toNodeId("n1")));

    // Active/Deactive a node doesn't directly assigned label, should remove
    // from node->label map
    mgr.activateNode(toNodeId("n1:2"), SMALL_RESOURCE);
    assertCollectionEquals(toSet("p2"),
        mgr.getNodeLabels().get(toNodeId("n1:2")));
    mgr.deactivateNode(toNodeId("n1:2"));
    Assert.assertNull(mgr.getNodeLabels().get(toNodeId("n1:2")));
    // Host will not affected too
    assertCollectionEquals(toSet("p2"),
        mgr.getNodeLabels().get(toNodeId("n1")));

    // When we change label on the host after active a node without directly
    // assigned label, such node will still be removed after deactive
    // Active/Deactive a node doesn't directly assigned label, should remove
    // from node->label map
    mgr.activateNode(toNodeId("n1:2"), SMALL_RESOURCE);
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1"), toSet("p3")));
    assertCollectionEquals(toSet("p3"),
        mgr.getNodeLabels().get(toNodeId("n1:2")));
    mgr.deactivateNode(toNodeId("n1:2"));
    Assert.assertNull(mgr.getNodeLabels().get(toNodeId("n1:2")));
    // Host will not affected too
    assertCollectionEquals(toSet("p3"),
        mgr.getNodeLabels().get(toNodeId("n1")));
  }
  
  private void checkNodeLabelInfo(List<RMNodeLabel> infos, String labelName, int activeNMs, int memory) {
    for (RMNodeLabel info : infos) {
      if (info.getLabelName().equals(labelName)) {
        Assert.assertEquals(activeNMs, info.getNumActiveNMs());
        Assert.assertEquals(memory, info.getResource().getMemorySize());
        return;
      }
    }
    Assert.fail("Failed to find info has label=" + labelName);
  }
  
  @Test(timeout = 5000)
  public void testPullRMNodeLabelsInfo() throws IOException {
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("x", "y", "z"));
    mgr.activateNode(NodeId.newInstance("n1", 1), Resource.newInstance(10, 0));
    mgr.activateNode(NodeId.newInstance("n2", 1), Resource.newInstance(10, 0));
    mgr.activateNode(NodeId.newInstance("n3", 1), Resource.newInstance(10, 0));
    mgr.activateNode(NodeId.newInstance("n4", 1), Resource.newInstance(10, 0));
    mgr.activateNode(NodeId.newInstance("n5", 1), Resource.newInstance(10, 0));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1"), toSet("x"),
        toNodeId("n2"), toSet("x"), toNodeId("n3"), toSet("y")));
    
    // x, y, z and ""
    List<RMNodeLabel> infos = mgr.pullRMNodeLabelsInfo();
    Assert.assertEquals(4, infos.size());
    checkNodeLabelInfo(infos, RMNodeLabelsManager.NO_LABEL, 2, 20);
    checkNodeLabelInfo(infos, "x", 2, 20);
    checkNodeLabelInfo(infos, "y", 1, 10);
    checkNodeLabelInfo(infos, "z", 0, 0);
  }

  @Test(timeout = 60000)
  public void testcheckRemoveFromClusterNodeLabelsOfQueue() throws Exception {
    lmgr = new RMNodeLabelsManager();
    Configuration conf = new Configuration();
    File tempDir = File.createTempFile("nlb", ".tmp");
    tempDir.delete();
    tempDir.mkdirs();
    tempDir.deleteOnExit();
    conf.set(YarnConfiguration.FS_NODE_LABELS_STORE_ROOT_DIR,
        tempDir.getAbsolutePath());
    conf.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, true);
    conf.set(YarnConfiguration.RM_SCHEDULER,
        "org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler");
    Configuration withQueueLabels = getConfigurationWithQueueLabels(conf);
    MockRM rm = initRM(conf);
    lmgr.addToCluserNodeLabels(toSet(NodeLabel.newInstance("x", false)));
    lmgr.removeFromClusterNodeLabels(Arrays.asList(new String[] { "x" }));
    lmgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("x"));
    rm.stop();
    class TestRMLabelManger extends RMNodeLabelsManager {
      @Override
      protected void checkRemoveFromClusterNodeLabelsOfQueue(
          Collection<String> labelsToRemove) throws IOException {
        checkQueueCall = true;
        // Do nothing
      }
    }
    lmgr = new TestRMLabelManger();
    MockRM rm2 = initRM(withQueueLabels);
    Assert.assertFalse(
        "checkRemoveFromClusterNodeLabelsOfQueue should not be called"
            + "on recovery",
        checkQueueCall);
    lmgr.removeFromClusterNodeLabels(Arrays.asList(new String[] { "x" }));
    Assert
        .assertTrue("checkRemoveFromClusterNodeLabelsOfQueue should be called "
            + "since its not recovery", checkQueueCall);
    rm2.stop();
  }

  private MockRM initRM(Configuration conf) {
    MockRM rm = new MockRM(conf) {
      @Override
      public RMNodeLabelsManager createNodeLabelManager() {
        return lmgr;
      }
    };
    rm.getRMContext().setNodeLabelManager(lmgr);
    rm.start();
    Assert.assertEquals(Service.STATE.STARTED, rm.getServiceState());
    return rm;
  }

  private Configuration getConfigurationWithQueueLabels(Configuration config) {
    CapacitySchedulerConfiguration conf =
        new CapacitySchedulerConfiguration(config);
    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] { "a" });
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "x", 100);

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    conf.setCapacity(A, 100);
    conf.setAccessibleNodeLabels(A, ImmutableSet.of("x"));
    conf.setCapacityByLabel(A, "x", 100);
    return conf;
  }

  @Test(timeout = 5000)
  public void testLabelsToNodesOnNodeActiveDeactive() throws Exception {
    // Activate a node without assigning any labels
    mgr.activateNode(NodeId.newInstance("n1", 1), Resource.newInstance(10, 0));
    Assert.assertTrue(mgr.getLabelsToNodes().isEmpty());
    assertLabelsToNodesEquals(
        mgr.getLabelsToNodes(), transposeNodeToLabels(mgr.getNodeLabels()));

    // Add labels and replace labels on node
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p1"));
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

  @Test(timeout = 60000)
  public void testBackwardsCompatableMirror() throws Exception {
    lmgr = new RMNodeLabelsManager();
    Configuration conf = new Configuration();
    File tempDir = File.createTempFile("nlb", ".tmp");
    tempDir.delete();
    tempDir.mkdirs();
    tempDir.deleteOnExit();
    String tempDirName = tempDir.getAbsolutePath();
    conf.set(YarnConfiguration.FS_NODE_LABELS_STORE_ROOT_DIR, tempDirName);

    // The following are the contents of a 2.7-formatted levelDB file to be
    // placed in nodelabel.mirror. There are 3 labels: 'a', 'b', and 'c'.
    // host1 is labeled with 'a', host2 is labeled with 'b', and c is not
    // associated with a node.
    byte[] contents =
      {
          0x09, 0x0A, 0x01, 0x61, 0x0A, 0x01, 0x62, 0x0A, 0x01, 0x63, 0x20,
          0x0A, 0x0E, 0x0A, 0x09, 0x0A, 0x05, 0x68, 0x6F, 0x73, 0x74, 0x32,
          0x10, 0x00, 0x12, 0x01, 0x62, 0x0A, 0x0E, 0x0A, 0x09, 0x0A, 0x05,
          0x68, 0x6F, 0x73, 0x74, 0x31, 0x10, 0x00, 0x12, 0x01, 0x61
      };
    File file = new File(tempDirName + "/nodelabel.mirror");
    file.createNewFile();
    FileOutputStream stream = new FileOutputStream(file);
    stream.write(contents);
    stream.close();

    conf.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, true);
    conf.set(YarnConfiguration.RM_SCHEDULER,
        "org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler");
    Configuration withQueueLabels = getConfigurationWithQueueLabels(conf);

    MockRM rm = initRM(withQueueLabels);
    Set<String> labelNames = lmgr.getClusterNodeLabelNames();
    Map<String, Set<NodeId>> labeledNodes = lmgr.getLabelsToNodes();

    Assert.assertTrue(labelNames.contains("a"));
    Assert.assertTrue(labelNames.contains("b"));
    Assert.assertTrue(labelNames.contains("c"));
    Assert.assertTrue(labeledNodes.get("a")
        .contains(NodeId.newInstance("host1", 0)));
    Assert.assertTrue(labeledNodes.get("b")
        .contains(NodeId.newInstance("host2", 0)));

    rm.stop();
  }
}
