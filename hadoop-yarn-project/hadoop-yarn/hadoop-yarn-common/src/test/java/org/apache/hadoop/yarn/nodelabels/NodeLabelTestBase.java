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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.junit.Assert;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

public class NodeLabelTestBase {
  public static void assertMapEquals(Map<NodeId, Set<String>> m1,
      ImmutableMap<NodeId, Set<String>> m2) {
    Assert.assertEquals(m1.size(), m2.size());
    for (NodeId k : m1.keySet()) {
      Assert.assertTrue(m2.containsKey(k));
      assertCollectionEquals(m1.get(k), m2.get(k));
    }
  }

  public static void assertMapContains(Map<NodeId, Set<String>> m1,
      ImmutableMap<NodeId, Set<String>> m2) {
    for (NodeId k : m2.keySet()) {
      Assert.assertTrue(m1.containsKey(k));
      assertCollectionEquals(m1.get(k), m2.get(k));
    }
  }

  public static void assertCollectionEquals(Collection<String> c1,
      Collection<String> c2) {
    Set<String> s1 = new HashSet<String>(c1);
    Set<String> s2 = new HashSet<String>(c2);
    Assert.assertEquals(s1, s2);
    Assert.assertTrue(s1.containsAll(s2));
  }

  public static <E> Set<E> toSet(E... elements) {
    Set<E> set = Sets.newHashSet(elements);
    return set;
  }
  
  public NodeId toNodeId(String str) {
    if (str.contains(":")) {
      int idx = str.indexOf(':');
      NodeId id =
          NodeId.newInstance(str.substring(0, idx),
              Integer.valueOf(str.substring(idx + 1)));
      return id;
    } else {
      return NodeId.newInstance(str, CommonNodeLabelsManager.WILDCARD_PORT);
    }
  }
}
