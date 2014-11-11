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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class TestCommonNodeLabelsManager extends NodeLabelTestBase {
  DummyCommonNodeLabelsManager mgr = null;

  @Before
  public void before() {
    mgr = new DummyCommonNodeLabelsManager();
    mgr.init(new Configuration());
    mgr.start();
  }

  @After
  public void after() {
    mgr.stop();
  }

  @Test(timeout = 5000)
  public void testAddRemovelabel() throws Exception {
    // Add some label
    mgr.addToCluserNodeLabels(ImmutableSet.of("hello"));
    assertCollectionEquals(mgr.lastAddedlabels, Arrays.asList("hello"));

    mgr.addToCluserNodeLabels(ImmutableSet.of("world"));
    mgr.addToCluserNodeLabels(toSet("hello1", "world1"));
    assertCollectionEquals(mgr.lastAddedlabels,
        Sets.newHashSet("hello1", "world1"));

    Assert.assertTrue(mgr.getClusterNodeLabels().containsAll(
        Sets.newHashSet("hello", "world", "hello1", "world1")));

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
    assertCollectionEquals(mgr.lastRemovedlabels, Arrays.asList("hello"));
    Assert.assertTrue(mgr.getClusterNodeLabels().containsAll(
        Arrays.asList("world", "hello1", "world1")));

    mgr.removeFromClusterNodeLabels(Arrays
        .asList("hello1", "world1", "world"));
    Assert.assertTrue(mgr.lastRemovedlabels.containsAll(Sets.newHashSet(
        "hello1", "world1", "world")));
    Assert.assertTrue(mgr.getClusterNodeLabels().isEmpty());
  }

  @Test(timeout = 5000)
  public void testAddlabelWithCase() throws Exception {
    // Add some label, case will not ignore here
    mgr.addToCluserNodeLabels(ImmutableSet.of("HeLlO"));
    assertCollectionEquals(mgr.lastAddedlabels, Arrays.asList("HeLlO"));
    Assert.assertFalse(mgr.getClusterNodeLabels().containsAll(Arrays.asList("hello")));
  }

  @Test(timeout = 5000)
  public void testAddInvalidlabel() throws IOException {
    boolean caught = false;
    try {
      Set<String> set = new HashSet<String>();
      set.add(null);
      mgr.addToCluserNodeLabels(set);
    } catch (IOException e) {
      caught = true;
    }
    Assert.assertTrue("null label should not add to repo", caught);

    caught = false;
    try {
      mgr.addToCluserNodeLabels(ImmutableSet.of(CommonNodeLabelsManager.NO_LABEL));
    } catch (IOException e) {
      caught = true;
    }

    Assert.assertTrue("empty label should not add to repo", caught);

    caught = false;
    try {
      mgr.addToCluserNodeLabels(ImmutableSet.of("-?"));
    } catch (IOException e) {
      caught = true;
    }
    Assert.assertTrue("invalid label charactor should not add to repo", caught);

    caught = false;
    try {
      mgr.addToCluserNodeLabels(ImmutableSet.of(StringUtils.repeat("c", 257)));
    } catch (IOException e) {
      caught = true;
    }
    Assert.assertTrue("too long label should not add to repo", caught);

    caught = false;
    try {
      mgr.addToCluserNodeLabels(ImmutableSet.of("-aaabbb"));
    } catch (IOException e) {
      caught = true;
    }
    Assert.assertTrue("label cannot start with \"-\"", caught);

    caught = false;
    try {
      mgr.addToCluserNodeLabels(ImmutableSet.of("_aaabbb"));
    } catch (IOException e) {
      caught = true;
    }
    Assert.assertTrue("label cannot start with \"_\"", caught);
    
    caught = false;
    try {
      mgr.addToCluserNodeLabels(ImmutableSet.of("a^aabbb"));
    } catch (IOException e) {
      caught = true;
    }
    Assert.assertTrue("label cannot contains other chars like ^[] ...", caught);
    
    caught = false;
    try {
      mgr.addToCluserNodeLabels(ImmutableSet.of("aa[a]bbb"));
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
    mgr.addToCluserNodeLabels(toSet("p1", "p2", "p3"));
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
    mgr.addLabelsToNode(ImmutableMap.of(toNodeId("n1"), toSet("p1"),
        toNodeId("n2"), toSet("p2")));
    assertMapEquals(
        mgr.getNodeLabels(),
        ImmutableMap.of(toNodeId("n1"), toSet("p1"), toNodeId("n2"),
            toSet("p2", "p3"), toNodeId("n3"), toSet("p3")));
    assertMapEquals(mgr.lastNodeToLabels,
        ImmutableMap.of(toNodeId("n1"), toSet("p1"), toNodeId("n2"),
            toSet("p2", "p3")));

    // remove labels on node
    mgr.removeLabelsFromNode(ImmutableMap.of(toNodeId("n1"), toSet("p1"),
        toNodeId("n2"), toSet("p2", "p3"), toNodeId("n3"), toSet("p3")));
    Assert.assertEquals(0, mgr.getNodeLabels().size());
    assertMapEquals(mgr.lastNodeToLabels, ImmutableMap.of(toNodeId("n1"),
        CommonNodeLabelsManager.EMPTY_STRING_SET, toNodeId("n2"),
        CommonNodeLabelsManager.EMPTY_STRING_SET, toNodeId("n3"),
        CommonNodeLabelsManager.EMPTY_STRING_SET));
  }

  @Test(timeout = 5000)
  public void testRemovelabelWithNodes() throws Exception {
    mgr.addToCluserNodeLabels(toSet("p1", "p2", "p3"));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1"), toSet("p1")));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n2"), toSet("p2")));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n3"), toSet("p3")));

    mgr.removeFromClusterNodeLabels(ImmutableSet.of("p1"));
    assertMapEquals(mgr.getNodeLabels(),
        ImmutableMap.of(toNodeId("n2"), toSet("p2"), toNodeId("n3"), toSet("p3")));
    assertCollectionEquals(mgr.lastRemovedlabels, Arrays.asList("p1"));

    mgr.removeFromClusterNodeLabels(ImmutableSet.of("p2", "p3"));
    Assert.assertTrue(mgr.getNodeLabels().isEmpty());
    Assert.assertTrue(mgr.getClusterNodeLabels().isEmpty());
    assertCollectionEquals(mgr.lastRemovedlabels, Arrays.asList("p2", "p3"));
  }
  
  @Test(timeout = 5000) 
  public void testTrimLabelsWhenAddRemoveNodeLabels() throws IOException {
    mgr.addToCluserNodeLabels(toSet(" p1"));
    assertCollectionEquals(mgr.getClusterNodeLabels(), toSet("p1"));
    mgr.removeFromClusterNodeLabels(toSet("p1 "));
    Assert.assertTrue(mgr.getClusterNodeLabels().isEmpty());
  }
  
  @Test(timeout = 5000) 
  public void testTrimLabelsWhenModifyLabelsOnNodes() throws IOException {
    mgr.addToCluserNodeLabels(toSet(" p1", "p2"));
    mgr.addLabelsToNode(ImmutableMap.of(toNodeId("n1"), toSet("p1 ", "p2")));
    assertMapEquals(
        mgr.getNodeLabels(),
        ImmutableMap.of(toNodeId("n1"), toSet("p1", "p2")));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1"), toSet(" p2")));
    assertMapEquals(
        mgr.getNodeLabels(),
        ImmutableMap.of(toNodeId("n1"), toSet("p2")));
    mgr.removeLabelsFromNode(ImmutableMap.of(toNodeId("n1"), toSet("  p2 ")));
    Assert.assertTrue(mgr.getNodeLabels().isEmpty());
  }
}