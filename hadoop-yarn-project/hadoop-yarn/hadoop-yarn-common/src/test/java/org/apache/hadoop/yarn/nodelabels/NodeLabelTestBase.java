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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.junit.Assert;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.thirdparty.com.google.common.collect.Sets;

public class NodeLabelTestBase {
  public static void assertMapEquals(Map<NodeId, Set<String>> expected,
      ImmutableMap<NodeId, Set<String>> actual) {
    Assert.assertEquals(expected.size(), actual.size());
    for (NodeId k : expected.keySet()) {
      Assert.assertTrue(actual.containsKey(k));
      assertCollectionEquals(expected.get(k), actual.get(k));
    }
  }

  public static void assertLabelInfoMapEquals(
      Map<NodeId, Set<NodeLabel>> expected,
      ImmutableMap<NodeId, Set<NodeLabel>> actual) {
    Assert.assertEquals(expected.size(), actual.size());
    for (NodeId k : expected.keySet()) {
      Assert.assertTrue(actual.containsKey(k));
      assertNLCollectionEquals(expected.get(k), actual.get(k));
    }
  }

  public static void assertLabelsToNodesEquals(
      Map<String, Set<NodeId>> expected,
      ImmutableMap<String, Set<NodeId>> actual) {
    Assert.assertEquals(expected.size(), actual.size());
    for (String k : expected.keySet()) {
      Assert.assertTrue(actual.containsKey(k));
      Set<NodeId> expectedS1 = new HashSet<>(expected.get(k));
      Set<NodeId> actualS2 = new HashSet<>(actual.get(k));
      Assert.assertEquals(expectedS1, actualS2);
      Assert.assertTrue(expectedS1.containsAll(actualS2));
    }
  }

  public static ImmutableMap<String, Set<NodeId>> transposeNodeToLabels(
      Map<NodeId, Set<String>> mapNodeToLabels) {
    Map<String, Set<NodeId>> mapLabelsToNodes = new HashMap<>();
    for(Entry<NodeId, Set<String>> entry : mapNodeToLabels.entrySet()) {
      NodeId node = entry.getKey();
      Set<String> setLabels = entry.getValue();
      for(String label : setLabels) {
        Set<NodeId> setNode = mapLabelsToNodes.get(label);
        if (setNode == null) {
          setNode = new HashSet<>();
        }
        setNode.add(NodeId.newInstance(node.getHost(), node.getPort()));
        mapLabelsToNodes.put(label, setNode);
      }
    }
    return ImmutableMap.copyOf(mapLabelsToNodes);
  }

  public static void assertMapContains(Map<NodeId, Set<String>> expected,
      ImmutableMap<NodeId, Set<String>> actual) {
    for (NodeId k : actual.keySet()) {
      Assert.assertTrue(expected.containsKey(k));
      assertCollectionEquals(expected.get(k), actual.get(k));
    }
  }

  public static void assertCollectionEquals(Collection<String> expected,
      Collection<String> actual) {
    if (expected == null) {
      Assert.assertNull(actual);
    } else {
      Assert.assertNotNull(actual);
    }
    Set<String> expectedSet = new HashSet<>(expected);
    Set<String> actualSet = new HashSet<>(actual);
    Assert.assertEquals(expectedSet, actualSet);
    Assert.assertTrue(expectedSet.containsAll(actualSet));
  }

  public static void assertNLCollectionEquals(Collection<NodeLabel> expected,
      Collection<NodeLabel> actual) {
    if (expected == null) {
      Assert.assertNull(actual);
    } else {
      Assert.assertNotNull(actual);
    }

    Set<NodeLabel> expectedSet = new HashSet<>(expected);
    Set<NodeLabel> actualSet = new HashSet<>(actual);
    Assert.assertEquals(expectedSet, actualSet);
    Assert.assertTrue(expectedSet.containsAll(actualSet));
  }

  @SuppressWarnings("unchecked")
  public static <E> Set<E> toSet(E... elements) {
    Set<E> set = Sets.newHashSet(elements);
    return set;
  }
  
  public static Set<NodeLabel> toNodeLabelSet(String... nodeLabelsStr) {
    if (null == nodeLabelsStr) {
      return null;
    }
    Set<NodeLabel> labels = new HashSet<>();
    for (String label : nodeLabelsStr) {
      labels.add(NodeLabel.newInstance(label));
    }
    return labels;
  }

  public NodeId toNodeId(String str) {
    if (str.contains(":")) {
      int idx = str.indexOf(':');
      NodeId id =
          NodeId.newInstance(str.substring(0, idx),
              Integer.parseInt(str.substring(idx + 1)));
      return id;
    } else {
      return NodeId.newInstance(str, CommonNodeLabelsManager.WILDCARD_PORT);
    }
  }

  public static void assertLabelsInfoToNodesEquals(
      Map<NodeLabel, Set<NodeId>> expected,
      ImmutableMap<NodeLabel, Set<NodeId>> actual) {
    Assert.assertEquals(expected.size(), actual.size());
    for (NodeLabel k : expected.keySet()) {
      Assert.assertTrue(actual.containsKey(k));
      Set<NodeId> expectedS1 = new HashSet<>(expected.get(k));
      Set<NodeId> actualS2 = new HashSet<>(actual.get(k));
      Assert.assertEquals(expectedS1, actualS2);
      Assert.assertTrue(expectedS1.containsAll(actualS2));
    }
  }
}
