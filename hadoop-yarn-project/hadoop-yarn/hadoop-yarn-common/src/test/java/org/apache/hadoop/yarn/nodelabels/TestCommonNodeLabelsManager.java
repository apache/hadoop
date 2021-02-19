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

package org.apache.hadoop.yarn.nodelabels;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.thirdparty.com.google.common.collect.Sets;

public class TestCommonNodeLabelsManager extends NodeLabelTestBase {
  DummyCommonNodeLabelsManager mgr = null;

  @Before
  public void before() {
    mgr = new DummyCommonNodeLabelsManager();
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, true);
    mgr.init(conf);
    mgr.start();
  }

  @After
  public void after() {
    mgr.stop();
  }

  @Test(timeout = 5000)
  public void testAddRemovelabel() throws Exception {
    // Add some label
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("hello"));
    verifyNodeLabelAdded(Sets.newHashSet("hello"), mgr.lastAddedlabels);

    mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("world"));
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("hello1", "world1"));
    verifyNodeLabelAdded(Sets.newHashSet("hello1", "world1"), mgr.lastAddedlabels);

    Assert.assertTrue(mgr.getClusterNodeLabelNames().containsAll(
        Sets.newHashSet("hello", "world", "hello1", "world1")));
    try {
      mgr.addToCluserNodeLabels(Arrays.asList(NodeLabel.newInstance("hello1",
          false)));
      Assert.fail("IOException not thrown on exclusivity change of labels");
    } catch (Exception e) {
      Assert.assertTrue("IOException is expected when exclusivity is modified",
          e instanceof IOException);
    }
    try {
      mgr.addToCluserNodeLabels(Arrays.asList(NodeLabel.newInstance("hello1",
          true)));
    } catch (Exception e) {
      Assert.assertFalse(
          "IOException not expected when no change in exclusivity",
          e instanceof IOException);
    }
    // try to remove null, empty and non-existed label, should fail
    for (String p : Arrays.asList(null, CommonNodeLabelsManager.NO_LABEL, "xx")) {
      boolean caught = false;
      try {
        mgr.removeFromClusterNodeLabels(Arrays.asList(p));
      } catch (IOException e) {
        caught = true;
      }
      Assert.assertTrue("remove label should fail "
          + "when label is null/empty/non-existed", caught);
    }

    // Remove some label
    mgr.removeFromClusterNodeLabels(Arrays.asList("hello"));
    assertCollectionEquals(Sets.newHashSet("hello"), mgr.lastRemovedlabels);
    Assert.assertTrue(mgr.getClusterNodeLabelNames().containsAll(
        Arrays.asList("world", "hello1", "world1")));

    mgr.removeFromClusterNodeLabels(Arrays
        .asList("hello1", "world1", "world"));
    Assert.assertTrue(mgr.lastRemovedlabels.containsAll(Sets.newHashSet(
        "hello1", "world1", "world")));
    Assert.assertTrue(mgr.getClusterNodeLabelNames().isEmpty());
  }

  @Test(timeout = 5000)
  public void testAddlabelWithCase() throws Exception {
    // Add some label, case will not ignore here
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("HeLlO"));
    verifyNodeLabelAdded(Sets.newHashSet("HeLlO"), mgr.lastAddedlabels);
    Assert.assertFalse(mgr.getClusterNodeLabelNames().containsAll(
        Arrays.asList("hello")));
  }

  @Test(timeout = 5000)
  public void testAddlabelWithExclusivity() throws Exception {
    // Add some label, case will not ignore here
    mgr.addToCluserNodeLabels(Arrays.asList(NodeLabel.newInstance("a", false), NodeLabel.newInstance("b", true)));
    Assert.assertFalse(mgr.isExclusiveNodeLabel("a"));
    Assert.assertTrue(mgr.isExclusiveNodeLabel("b"));
  }

  @Test(timeout = 5000)
  public void testAddInvalidlabel() throws IOException {
    boolean caught = false;
    try {
      Set<String> set = new HashSet<String>();
      set.add(null);
      mgr.addToCluserNodeLabelsWithDefaultExclusivity(set);
    } catch (IOException e) {
      caught = true;
    }
    Assert.assertTrue("null label should not add to repo", caught);

    caught = false;
    try {
      mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of(CommonNodeLabelsManager.NO_LABEL));
    } catch (IOException e) {
      caught = true;
    }

    Assert.assertTrue("empty label should not add to repo", caught);

    caught = false;
    try {
      mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("-?"));
    } catch (IOException e) {
      caught = true;
    }
    Assert.assertTrue("invalid label character should not add to repo", caught);

    caught = false;
    try {
      mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of(StringUtils.repeat("c", 257)));
    } catch (IOException e) {
      caught = true;
    }
    Assert.assertTrue("too long label should not add to repo", caught);

    caught = false;
    try {
      mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("-aaabbb"));
    } catch (IOException e) {
      caught = true;
    }
    Assert.assertTrue("label cannot start with \"-\"", caught);

    caught = false;
    try {
      mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("_aaabbb"));
    } catch (IOException e) {
      caught = true;
    }
    Assert.assertTrue("label cannot start with \"_\"", caught);
    
    caught = false;
    try {
      mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("a^aabbb"));
    } catch (IOException e) {
      caught = true;
    }
    Assert.assertTrue("label cannot contains other chars like ^[] ...", caught);
    
    caught = false;
    try {
      mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("aa[a]bbb"));
    } catch (IOException e) {
      caught = true;
    }
    Assert.assertTrue("label cannot contains other chars like ^[] ...", caught);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test(timeout = 5000)
  public void testAddReplaceRemoveLabelsOnNodes() throws Exception {
    // set a label on a node, but label doesn't exist
    boolean caught = false;
    try {
      mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("node"), toSet("label")));
    } catch (IOException e) {
      caught = true;
    }
    Assert.assertTrue("trying to set a label to a node but "
        + "label doesn't exist in repository should fail", caught);

    // set a label on a node, but node is null or empty
    try {
      mgr.replaceLabelsOnNode(ImmutableMap.of(
          toNodeId(CommonNodeLabelsManager.NO_LABEL), toSet("label")));
    } catch (IOException e) {
      caught = true;
    }
    Assert.assertTrue("trying to add a empty node but succeeded", caught);

    // set node->label one by one
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p1", "p2", "p3"));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1"), toSet("p1")));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1"), toSet("p2")));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n2"), toSet("p3")));
    assertMapEquals(mgr.getNodeLabels(), ImmutableMap.of(toNodeId("n1"),
        toSet("p2"), toNodeId("n2"), toSet("p3")));
    assertMapEquals(mgr.lastNodeToLabels,
        ImmutableMap.of(toNodeId("n2"), toSet("p3")));

    // set bunch of node->label
    mgr.replaceLabelsOnNode((Map) ImmutableMap.of(toNodeId("n3"), toSet("p3"),
        toNodeId("n1"), toSet("p1")));
    assertMapEquals(mgr.getNodeLabels(), ImmutableMap.of(toNodeId("n1"),
        toSet("p1"), toNodeId("n2"), toSet("p3"), toNodeId("n3"), toSet("p3")));
    assertMapEquals(mgr.lastNodeToLabels, ImmutableMap.of(toNodeId("n3"),
        toSet("p3"), toNodeId("n1"), toSet("p1")));

    /*
     * n1: p1 
     * n2: p3 
     * n3: p3
     */

    // remove label on node
    mgr.removeLabelsFromNode(ImmutableMap.of(toNodeId("n1"), toSet("p1")));
    assertMapEquals(mgr.getNodeLabels(), ImmutableMap.of(toNodeId("n2"),
        toSet("p3"), toNodeId("n3"), toSet("p3")));
    assertMapEquals(mgr.lastNodeToLabels,
        ImmutableMap.of(toNodeId("n1"), CommonNodeLabelsManager.EMPTY_STRING_SET));

    // add label on node
    mgr.addLabelsToNode(ImmutableMap.of(toNodeId("n1"), toSet("p1")));
    assertMapEquals(
        mgr.getNodeLabels(),
        ImmutableMap.of(toNodeId("n1"), toSet("p1"), toNodeId("n2"),
            toSet("p3"), toNodeId("n3"), toSet("p3")));
    assertMapEquals(mgr.lastNodeToLabels,
        ImmutableMap.of(toNodeId("n1"), toSet("p1")));

    // remove labels on node
    mgr.removeLabelsFromNode(ImmutableMap.of(toNodeId("n1"), toSet("p1"),
        toNodeId("n2"), toSet("p3"), toNodeId("n3"), toSet("p3")));
    Assert.assertEquals(0, mgr.getNodeLabels().size());
    assertMapEquals(mgr.lastNodeToLabels, ImmutableMap.of(toNodeId("n1"),
        CommonNodeLabelsManager.EMPTY_STRING_SET, toNodeId("n2"),
        CommonNodeLabelsManager.EMPTY_STRING_SET, toNodeId("n3"),
        CommonNodeLabelsManager.EMPTY_STRING_SET));
  }

  @Test(timeout = 5000)
  public void testRemovelabelWithNodes() throws Exception {
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p1", "p2", "p3"));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1"), toSet("p1")));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n2"), toSet("p2")));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n3"), toSet("p3")));

    mgr.removeFromClusterNodeLabels(ImmutableSet.of("p1"));
    assertMapEquals(mgr.getNodeLabels(),
        ImmutableMap.of(toNodeId("n2"), toSet("p2"), toNodeId("n3"), toSet("p3")));
    assertCollectionEquals(Arrays.asList("p1"), mgr.lastRemovedlabels);

    mgr.removeFromClusterNodeLabels(ImmutableSet.of("p2", "p3"));
    Assert.assertTrue(mgr.getNodeLabels().isEmpty());
    Assert.assertTrue(mgr.getClusterNodeLabelNames().isEmpty());
    assertCollectionEquals(Arrays.asList("p2", "p3"), mgr.lastRemovedlabels);
  }
  
  @Test(timeout = 5000) 
  public void testTrimLabelsWhenAddRemoveNodeLabels() throws IOException {
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet(" p1"));
    assertCollectionEquals(toSet("p1"), mgr.getClusterNodeLabelNames());
    mgr.removeFromClusterNodeLabels(toSet("p1 "));
    Assert.assertTrue(mgr.getClusterNodeLabelNames().isEmpty());
  }
  
  @Test(timeout = 5000) 
  public void testTrimLabelsWhenModifyLabelsOnNodes() throws IOException {
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet(" p1", "p2"));
    mgr.addLabelsToNode(ImmutableMap.of(toNodeId("n1"), toSet("p1 ")));
    assertMapEquals(
        mgr.getNodeLabels(),
        ImmutableMap.of(toNodeId("n1"), toSet("p1")));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1"), toSet(" p2")));
    assertMapEquals(
        mgr.getNodeLabels(),
        ImmutableMap.of(toNodeId("n1"), toSet("p2")));
    mgr.removeLabelsFromNode(ImmutableMap.of(toNodeId("n1"), toSet("  p2 ")));
    Assert.assertTrue(mgr.getNodeLabels().isEmpty());
  }
  
  @Test(timeout = 5000)
  public void testReplaceLabelsOnHostsShouldUpdateNodesBelongTo()
      throws IOException {
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p1", "p2", "p3"));
    mgr.addLabelsToNode(ImmutableMap.of(toNodeId("n1"), toSet("p1")));
    assertMapEquals(
        mgr.getNodeLabels(),
        ImmutableMap.of(toNodeId("n1"), toSet("p1")));
    
    // Replace labels on n1:1 to P2
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1:1"), toSet("p2"),
        toNodeId("n1:2"), toSet("p2")));
    assertMapEquals(mgr.getNodeLabels(), ImmutableMap.of(toNodeId("n1"),
        toSet("p1"), toNodeId("n1:1"), toSet("p2"), toNodeId("n1:2"),
        toSet("p2")));
    
    // Replace labels on n1 to P1, both n1:1/n1 will be P1 now
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1"), toSet("p1")));
    assertMapEquals(mgr.getNodeLabels(), ImmutableMap.of(toNodeId("n1"),
        toSet("p1"), toNodeId("n1:1"), toSet("p1"), toNodeId("n1:2"),
        toSet("p1")));
    
    // Set labels on n1:1 to P2 again to verify if add/remove works
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1:1"), toSet("p2")));
  }

  private void assertNodeLabelsDisabledErrorMessage(IOException e) {
    Assert.assertEquals(CommonNodeLabelsManager.NODE_LABELS_NOT_ENABLED_ERR,
        e.getMessage());
  }
  
  @Test(timeout = 5000)
  public void testNodeLabelsDisabled() throws IOException {
    DummyCommonNodeLabelsManager mgr = new DummyCommonNodeLabelsManager();
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, false);
    mgr.init(conf);
    mgr.start();
    boolean caught = false;
    
    // add labels
    try {
      mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x"));
    } catch (IOException e) {
      assertNodeLabelsDisabledErrorMessage(e);
      caught = true;
    }
    // check exception caught
    Assert.assertTrue(caught);
    caught = false;
    
    // remove labels
    try {
      mgr.removeFromClusterNodeLabels(ImmutableSet.of("x"));
    } catch (IOException e) {
      assertNodeLabelsDisabledErrorMessage(e);
      caught = true;
    }
    // check exception caught
    Assert.assertTrue(caught);
    caught = false;
    
    // add labels to node
    try {
      mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("host", 0),
          CommonNodeLabelsManager.EMPTY_STRING_SET));
    } catch (IOException e) {
      assertNodeLabelsDisabledErrorMessage(e);
      caught = true;
    }
    // check exception caught
    Assert.assertTrue(caught);
    caught = false;
    
    // remove labels from node
    try {
      mgr.removeLabelsFromNode(ImmutableMap.of(NodeId.newInstance("host", 0),
          CommonNodeLabelsManager.EMPTY_STRING_SET));
    } catch (IOException e) {
      assertNodeLabelsDisabledErrorMessage(e);
      caught = true;
    }
    // check exception caught
    Assert.assertTrue(caught);
    caught = false;
    
    // replace labels on node
    try {
      mgr.replaceLabelsOnNode(ImmutableMap.of(NodeId.newInstance("host", 0),
          CommonNodeLabelsManager.EMPTY_STRING_SET));
    } catch (IOException e) {
      assertNodeLabelsDisabledErrorMessage(e);
      caught = true;
    }
    // check exception caught
    Assert.assertTrue(caught);
    caught = false;
    
    mgr.close();
  }  

  @Test(timeout = 5000)
  public void testLabelsToNodes()
      throws IOException {
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p1", "p2", "p3"));
    mgr.addLabelsToNode(ImmutableMap.of(toNodeId("n1"), toSet("p1")));
    Map<String, Set<NodeId>> labelsToNodes = mgr.getLabelsToNodes();
    assertLabelsToNodesEquals(
        labelsToNodes,
        ImmutableMap.of(
        "p1", toSet(toNodeId("n1"))));
    assertLabelsToNodesEquals(
        labelsToNodes, transposeNodeToLabels(mgr.getNodeLabels()));

    // Replace labels on n1:1 to P2
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1:1"), toSet("p2"),
        toNodeId("n1:2"), toSet("p2")));
    labelsToNodes = mgr.getLabelsToNodes();
    assertLabelsToNodesEquals(
        labelsToNodes,
        ImmutableMap.of(
        "p1", toSet(toNodeId("n1")),
        "p2", toSet(toNodeId("n1:1"),toNodeId("n1:2"))));
    assertLabelsToNodesEquals(
        labelsToNodes, transposeNodeToLabels(mgr.getNodeLabels()));

    // Replace labels on n1 to P1, both n1:1/n1 will be P1 now
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1"), toSet("p1")));
    labelsToNodes = mgr.getLabelsToNodes();
    assertLabelsToNodesEquals(
        labelsToNodes,
        ImmutableMap.of(
        "p1", toSet(toNodeId("n1"),toNodeId("n1:1"),toNodeId("n1:2"))));
    assertLabelsToNodesEquals(
        labelsToNodes, transposeNodeToLabels(mgr.getNodeLabels()));

    // Set labels on n1:1 to P2 again to verify if add/remove works
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1:1"), toSet("p2")));
    // Add p3 to n1, should makes n1:1 to be p2/p3, and n1:2 to be p1/p3
    mgr.addLabelsToNode(ImmutableMap.of(toNodeId("n2"), toSet("p3")));
    labelsToNodes = mgr.getLabelsToNodes();
    assertLabelsToNodesEquals(
        labelsToNodes,
        ImmutableMap.of(
        "p1", toSet(toNodeId("n1"),toNodeId("n1:2")),
        "p2", toSet(toNodeId("n1:1")),
        "p3", toSet(toNodeId("n2"))));
    assertLabelsToNodesEquals(
        labelsToNodes, transposeNodeToLabels(mgr.getNodeLabels()));

    // Remove P3 from n1, should makes n1:1 to be p2, and n1:2 to be p1
    mgr.removeLabelsFromNode(ImmutableMap.of(toNodeId("n2"), toSet("p3")));
    labelsToNodes = mgr.getLabelsToNodes();
    assertLabelsToNodesEquals(
        labelsToNodes,
        ImmutableMap.of(
        "p1", toSet(toNodeId("n1"),toNodeId("n1:2")),
        "p2", toSet(toNodeId("n1:1"))));
    assertLabelsToNodesEquals(
        labelsToNodes, transposeNodeToLabels(mgr.getNodeLabels()));
  }

  @Test(timeout = 5000)
  public void testLabelsToNodesForSelectedLabels()
      throws IOException {
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p1", "p2", "p3"));
    mgr.addLabelsToNode(
        ImmutableMap.of(
        toNodeId("n1:1"), toSet("p1"),
        toNodeId("n1:2"), toSet("p2")));
    Set<String> setlabels =
        new HashSet<String>(Arrays.asList(new String[]{"p1"}));
    assertLabelsToNodesEquals(mgr.getLabelsToNodes(setlabels),
        ImmutableMap.of("p1", toSet(toNodeId("n1:1"))));

    // Replace labels on n1:1 to P3
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1"), toSet("p3")));
    assertTrue(mgr.getLabelsToNodes(setlabels).isEmpty());
    setlabels = new HashSet<String>(Arrays.asList(new String[]{"p2", "p3"}));
    assertLabelsToNodesEquals(
        mgr.getLabelsToNodes(setlabels),
        ImmutableMap.of(
        "p3", toSet(toNodeId("n1"), toNodeId("n1:1"),toNodeId("n1:2"))));

    mgr.addLabelsToNode(ImmutableMap.of(toNodeId("n2"), toSet("p2")));
    assertLabelsToNodesEquals(
        mgr.getLabelsToNodes(setlabels),
        ImmutableMap.of(
        "p2", toSet(toNodeId("n2")),
        "p3", toSet(toNodeId("n1"), toNodeId("n1:1"),toNodeId("n1:2"))));

    mgr.removeLabelsFromNode(ImmutableMap.of(toNodeId("n1"), toSet("p3")));
    setlabels =
        new HashSet<String>(Arrays.asList(new String[]{"p1", "p2", "p3"}));
    assertLabelsToNodesEquals(
        mgr.getLabelsToNodes(setlabels),
        ImmutableMap.of(
        "p2", toSet(toNodeId("n2"))));

    mgr.addLabelsToNode(ImmutableMap.of(toNodeId("n3"), toSet("p1")));
    assertLabelsToNodesEquals(
        mgr.getLabelsToNodes(setlabels),
        ImmutableMap.of(
        "p1", toSet(toNodeId("n3")),
        "p2", toSet(toNodeId("n2"))));

    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n2:2"), toSet("p3")));
    assertLabelsToNodesEquals(
        mgr.getLabelsToNodes(setlabels),
        ImmutableMap.of(
        "p1", toSet(toNodeId("n3")),
        "p2", toSet(toNodeId("n2")),
        "p3", toSet(toNodeId("n2:2"))));
    setlabels = new HashSet<String>(Arrays.asList(new String[]{"p1"}));
    assertLabelsToNodesEquals(mgr.getLabelsToNodes(setlabels),
        ImmutableMap.of("p1", toSet(toNodeId("n3"))));
  }

  @Test(timeout = 5000)
  public void testNoMoreThanOneLabelExistedInOneHost() throws IOException {
    boolean failed = false;
    // As in YARN-2694, we temporarily disable no more than one label existed in
    // one host
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p1", "p2", "p3"));
    try {
      mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1"), toSet("p1", "p2")));
    } catch (IOException e) {
      failed = true;
    }
    Assert.assertTrue("Should failed when set > 1 labels on a host", failed);

    try {
      mgr.addLabelsToNode(ImmutableMap.of(toNodeId("n1"), toSet("p1", "p2")));
    } catch (IOException e) {
      failed = true;
    }
    Assert.assertTrue("Should failed when add > 1 labels on a host", failed);

    mgr.addLabelsToNode(ImmutableMap.of(toNodeId("n1"), toSet("p1")));
    // add a same label to a node, #labels in this node is still 1, shouldn't
    // fail
    mgr.addLabelsToNode(ImmutableMap.of(toNodeId("n1"), toSet("p1")));
    try {
      mgr.addLabelsToNode(ImmutableMap.of(toNodeId("n1"), toSet("p2")));
    } catch (IOException e) {
      failed = true;
    }
    Assert.assertTrue("Should failed when #labels > 1 on a host after add",
        failed);
  }

  private void verifyNodeLabelAdded(Set<String> expectedAddedLabelNames,
      Collection<NodeLabel> addedNodeLabels) {
    Assert.assertEquals(expectedAddedLabelNames.size(), addedNodeLabels.size());
    for (NodeLabel label : addedNodeLabels) {
      Assert.assertTrue(expectedAddedLabelNames.contains(label.getName()));
    }
  }

  @Test(timeout = 5000)
  public void testReplaceLabelsOnNodeInDistributedMode() throws Exception {
    //create new DummyCommonNodeLabelsManager than the one got from @before
    mgr.stop();
    mgr = new DummyCommonNodeLabelsManager();
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, true);
    conf.set(YarnConfiguration.NODELABEL_CONFIGURATION_TYPE,
        YarnConfiguration.DISTRIBUTED_NODELABEL_CONFIGURATION_TYPE);

    mgr.init(conf);
    mgr.start();

    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p1", "p2", "p3"));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1"), toSet("p1")));
    Set<String> labelsByNode = mgr.getLabelsByNode(toNodeId("n1"));

    Assert.assertNull(
        "Labels are not expected to be written to the NodeLabelStore",
        mgr.lastNodeToLabels);
    Assert.assertNotNull("Updated labels should be available from the Mgr",
        labelsByNode);
    Assert.assertTrue(labelsByNode.contains("p1"));
  }

  @Test(timeout = 5000)
  public void testLabelsInfoToNodes() throws IOException {
    mgr.addToCluserNodeLabels(Arrays.asList(NodeLabel.newInstance("p1", false),
        NodeLabel.newInstance("p2", true), NodeLabel.newInstance("p3", true)));
    mgr.addLabelsToNode(ImmutableMap.of(toNodeId("n1"), toSet("p1")));
    Map<NodeLabel, Set<NodeId>> labelsToNodes = mgr.getLabelsInfoToNodes();
    assertLabelsInfoToNodesEquals(labelsToNodes, ImmutableMap.of(
        NodeLabel.newInstance("p1", false), toSet(toNodeId("n1"))));
  }

  @Test(timeout = 5000)
  public void testGetNodeLabelsInfo() throws IOException {
    mgr.addToCluserNodeLabels(Arrays.asList(NodeLabel.newInstance("p1", false),
        NodeLabel.newInstance("p2", true), NodeLabel.newInstance("p3", false)));
    mgr.addLabelsToNode(ImmutableMap.of(toNodeId("n1"), toSet("p2")));
    mgr.addLabelsToNode(ImmutableMap.of(toNodeId("n2"), toSet("p3")));

    assertLabelInfoMapEquals(mgr.getNodeLabelsInfo(), ImmutableMap.of(
        toNodeId("n1"), toSet(NodeLabel.newInstance("p2", true)),
        toNodeId("n2"), toSet(NodeLabel.newInstance("p3", false))));
  }

  @Test(timeout = 5000)
  public void testRemoveNodeLabelsInfo() throws IOException {
    mgr.addToCluserNodeLabels(Arrays.asList(NodeLabel.newInstance("p1", true)));
    mgr.addToCluserNodeLabels(Arrays.asList(NodeLabel.newInstance("p2", true)));
    mgr.addLabelsToNode(ImmutableMap.of(toNodeId("n1:1"), toSet("p1")));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1"), toSet("p2")));

    Map<String, Set<NodeId>> labelsToNodes = mgr.getLabelsToNodes();
    assertLabelsToNodesEquals(
        labelsToNodes,
        ImmutableMap.of(
        "p2", toSet(toNodeId("n1:1"), toNodeId("n1:0"))));

    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1"), new HashSet()));
    Map<String, Set<NodeId>> labelsToNodes2 = mgr.getLabelsToNodes();
    Assert.assertEquals(labelsToNodes2.get("p2"), null);
  }
}
