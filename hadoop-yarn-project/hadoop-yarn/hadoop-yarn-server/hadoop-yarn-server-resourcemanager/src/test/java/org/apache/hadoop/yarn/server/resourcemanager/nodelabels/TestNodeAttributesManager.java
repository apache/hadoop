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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeAttributeType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.nodelabels.AttributeValue;
import org.apache.hadoop.yarn.nodelabels.NodeAttributeStore;
import org.apache.hadoop.yarn.nodelabels.NodeAttributesManager;
import org.apache.hadoop.yarn.nodelabels.NodeLabelUtil;
import org.apache.hadoop.yarn.server.resourcemanager.NodeAttributeTestUtils;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.junit.Assert;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Unit tests for node attribute manager.
 */
public class TestNodeAttributesManager {

  private NodeAttributesManager attributesManager;
  private final static String[] PREFIXES =
      new String[] {"yarn.test1.io", "yarn.test2.io", "yarn.test3.io"};
  private final static String[] HOSTNAMES =
      new String[] {"host1", "host2", "host3"};

  @Before
  public void init() throws IOException {
    Configuration conf = new Configuration();
    attributesManager = new NodeAttributesManagerImpl();
    conf.setClass(YarnConfiguration.FS_NODE_ATTRIBUTE_STORE_IMPL_CLASS,
        FileSystemNodeAttributeStore.class, NodeAttributeStore.class);
    conf = NodeAttributeTestUtils.getRandomDirConf(conf);
    attributesManager.init(conf);
    attributesManager.start();
  }

  @After
  public void cleanUp() {
    if (attributesManager != null) {
      attributesManager.stop();
    }
  }

  private Set<NodeAttribute> createAttributesForTest(String attributePrefix,
      int numOfAttributes, String attributeNamePrefix,
      String attributeValuePrefix) {
    Set<NodeAttribute> attributes = new HashSet<>();
    for (int i = 0; i< numOfAttributes; i++) {
      NodeAttribute attribute = NodeAttribute.newInstance(
          attributePrefix, attributeNamePrefix + "_" + i,
          NodeAttributeType.STRING, attributeValuePrefix + "_" + i);
      attributes.add(attribute);
    }
    return attributes;
  }

  private boolean sameAttributeSet(Set<NodeAttribute> set1,
      Set<NodeAttribute> set2) {
    return Sets.difference(set1, set2).isEmpty();
  }

  @Test
  public void testAddNodeAttributes() throws IOException {
    Map<String, Set<NodeAttribute>> toAddAttributes = new HashMap<>();
    Map<NodeAttribute, AttributeValue> nodeAttributes;

    // Add 3 attributes to host1
    //  yarn.test1.io/A1=host1_v1_1
    //  yarn.test1.io/A2=host1_v1_2
    //  yarn.test1.io/A3=host1_v1_3
    toAddAttributes.put(HOSTNAMES[0],
        createAttributesForTest(PREFIXES[0], 3, "A", "host1_v1"));

    attributesManager.addNodeAttributes(toAddAttributes);
    nodeAttributes = attributesManager.getAttributesForNode(HOSTNAMES[0]);

    Assert.assertEquals(3, nodeAttributes.size());
    Assert.assertTrue(sameAttributeSet(toAddAttributes.get(HOSTNAMES[0]),
        nodeAttributes.keySet()));

    // Add 2 attributes to host2
    //  yarn.test1.io/A1=host2_v1_1
    //  yarn.test1.io/A2=host2_v1_2
    toAddAttributes.clear();
    toAddAttributes.put(HOSTNAMES[1],
        createAttributesForTest(PREFIXES[0], 2, "A", "host2_v1"));
    attributesManager.addNodeAttributes(toAddAttributes);

    // Verify host1 attributes are still valid.
    nodeAttributes = attributesManager.getAttributesForNode(HOSTNAMES[0]);
    Assert.assertEquals(3, nodeAttributes.size());

    // Verify new added host2 attributes are correctly updated.
    nodeAttributes = attributesManager.getAttributesForNode(HOSTNAMES[1]);
    Assert.assertEquals(2, nodeAttributes.size());
    Assert.assertTrue(sameAttributeSet(toAddAttributes.get(HOSTNAMES[1]),
        nodeAttributes.keySet()));

    // Cluster wide, it only has 3 attributes.
    //  yarn.test1.io/A1
    //  yarn.test1.io/A2
    //  yarn.test1.io/A3
    Set<NodeAttribute> clusterAttributes = attributesManager
        .getClusterNodeAttributes(Sets.newHashSet(PREFIXES[0]));
    Assert.assertEquals(3, clusterAttributes.size());

    // Query for attributes under a non-exist prefix,
    // ensure it returns an empty set.
    clusterAttributes = attributesManager
        .getClusterNodeAttributes(Sets.newHashSet("non_exist_prefix"));
    Assert.assertEquals(0, clusterAttributes.size());

    // Not provide any prefix, ensure it returns all attributes.
    clusterAttributes = attributesManager.getClusterNodeAttributes(null);
    Assert.assertEquals(3, clusterAttributes.size());

    // Add some other attributes with different prefixes on host1 and host2.
    toAddAttributes.clear();

    // Host1
    //  yarn.test2.io/A_1=host1_v2_1
    //  ...
    //  yarn.test2.io/A_10=host1_v2_10
    toAddAttributes.put(HOSTNAMES[0],
        createAttributesForTest(PREFIXES[1], 10, "C", "host1_v2"));
    // Host2
    //  yarn.test2.io/C_1=host1_v2_1
    //  ...
    //  yarn.test2.io/C_20=host1_v2_20
    toAddAttributes.put(HOSTNAMES[1],
        createAttributesForTest(PREFIXES[1], 20, "C", "host1_v2"));
    attributesManager.addNodeAttributes(toAddAttributes);

    nodeAttributes = attributesManager.getAttributesForNode(HOSTNAMES[0]);
    Assert.assertEquals(13, nodeAttributes.size());

    nodeAttributes = attributesManager.getAttributesForNode(HOSTNAMES[1]);
    Assert.assertEquals(22, nodeAttributes.size());
  }

  @Test
  public void testRemoveNodeAttributes() throws IOException {
    Map<String, Set<NodeAttribute>> toAddAttributes = new HashMap<>();
    Map<String, Set<NodeAttribute>> toRemoveAttributes = new HashMap<>();
    Set<NodeAttribute> allAttributesPerPrefix = new HashSet<>();
    Map<NodeAttribute, AttributeValue> nodeAttributes;

    // Host1 -----------------------
    //  yarn.test1.io
    //    A1=host1_v1_1
    //    A2=host1_v1_2
    //    A3=host1_v1_3
    //  yarn.test2.io
    //    B1=host1_v2_1
    //    ...
    //    B5=host5_v2_5
    // Host2 -----------------------
    //  yarn.test1.io
    //    A1=host2_v1_1
    //    A2=host2_v1_2
    //  yarn.test3.io
    //    C1=host2_v3_1
    //    c2=host2_v3_2
    Set<NodeAttribute> host1set = new HashSet<>();
    Set<NodeAttribute> host1set1 =
        createAttributesForTest(PREFIXES[0], 3, "A", "host1_v1");
    Set<NodeAttribute> host1set2 =
        createAttributesForTest(PREFIXES[1], 5, "B", "host1_v1");
    host1set.addAll(host1set1);
    host1set.addAll(host1set2);

    Set<NodeAttribute> host2set = new HashSet<>();
    Set<NodeAttribute> host2set1 =
        createAttributesForTest(PREFIXES[0], 2, "A", "host2_v1");
    Set<NodeAttribute> host2set2 =
        createAttributesForTest(PREFIXES[2], 2, "C", "host2_v3");
    host2set.addAll(host2set1);
    host2set.addAll(host2set2);

    toAddAttributes.put(HOSTNAMES[0], host1set);
    toAddAttributes.put(HOSTNAMES[1], host2set);
    attributesManager.addNodeAttributes(toAddAttributes);

    nodeAttributes = attributesManager.getAttributesForNode(HOSTNAMES[0]);
    Assert.assertEquals(8, nodeAttributes.size());

    nodeAttributes = attributesManager.getAttributesForNode(HOSTNAMES[1]);
    Assert.assertEquals(4, nodeAttributes.size());

    allAttributesPerPrefix = attributesManager
        .getClusterNodeAttributes(Sets.newHashSet(PREFIXES[0]));
    Assert.assertEquals(3, allAttributesPerPrefix.size());
    allAttributesPerPrefix = attributesManager
        .getClusterNodeAttributes(Sets.newHashSet(PREFIXES[1]));
    Assert.assertEquals(5, allAttributesPerPrefix.size());
    allAttributesPerPrefix = attributesManager
        .getClusterNodeAttributes(Sets.newHashSet(PREFIXES[2]));
    Assert.assertEquals(2, allAttributesPerPrefix.size());

    // Remove "yarn.test1.io/A_2" from host1
    Set<NodeAttribute> attributes2rm1 = new HashSet<>();
    attributes2rm1.add(NodeAttribute.newInstance(PREFIXES[0], "A_2",
        NodeAttributeType.STRING, "anyValue"));
    toRemoveAttributes.put(HOSTNAMES[0], attributes2rm1);
    attributesManager.removeNodeAttributes(toRemoveAttributes);

    nodeAttributes = attributesManager.getAttributesForNode(HOSTNAMES[0]);
    Assert.assertEquals(7, nodeAttributes.size());

    // Remove again, but give a non-exist attribute name
    attributes2rm1.clear();
    toRemoveAttributes.clear();
    attributes2rm1.add(NodeAttribute.newInstance(PREFIXES[0], "non_exist_name",
        NodeAttributeType.STRING, "anyValue"));
    toRemoveAttributes.put(HOSTNAMES[0], attributes2rm1);
    attributesManager.removeNodeAttributes(toRemoveAttributes);

    nodeAttributes = attributesManager.getAttributesForNode(HOSTNAMES[0]);
    Assert.assertEquals(7, nodeAttributes.size());

    // Remove "yarn.test1.io/A_2" from host2 too,
    // by then there will be no such attribute exist in the cluster.
    Set<NodeAttribute> attributes2rm2 = new HashSet<>();
    attributes2rm2.add(NodeAttribute.newInstance(PREFIXES[0], "A_2",
        NodeAttributeType.STRING, "anyValue"));
    toRemoveAttributes.clear();
    toRemoveAttributes.put(HOSTNAMES[1], attributes2rm2);
    attributesManager.removeNodeAttributes(toRemoveAttributes);

    // Make sure cluster wide attributes are still consistent.
    // Since both host1 and host2 doesn't have "yarn.test1.io/A_2",
    // get all attributes under prefix "yarn.test1.io" should only return
    // us A_1 and A_3.
    allAttributesPerPrefix = attributesManager
        .getClusterNodeAttributes(Sets.newHashSet(PREFIXES[0]));
    Assert.assertEquals(2, allAttributesPerPrefix.size());
  }

  @Test
  public void testReplaceNodeAttributes() throws IOException {
    Map<String, Set<NodeAttribute>> toAddAttributes = new HashMap<>();
    Map<String, Set<NodeAttribute>> toReplaceMap = new HashMap<>();
    Map<NodeAttribute, AttributeValue> nodeAttributes;
    Set<NodeAttribute> filteredAttributes;
    Set<NodeAttribute> clusterAttributes;

    // Add 3 attributes to host1
    //  yarn.test1.io/A1=host1_v1_1
    //  yarn.test1.io/A2=host1_v1_2
    //  yarn.test1.io/A3=host1_v1_3
    toAddAttributes.put(HOSTNAMES[0],
        createAttributesForTest(PREFIXES[0], 3, "A", "host1_v1"));

    attributesManager.addNodeAttributes(toAddAttributes);
    nodeAttributes = attributesManager.getAttributesForNode(HOSTNAMES[0]);
    Assert.assertEquals(3, nodeAttributes.size());

    // Add 10 distributed node attributes to host1
    //  nn.yarn.io/dist-node-attribute1=dist_v1_1
    //  nn.yarn.io/dist-node-attribute2=dist_v1_2
    //  ...
    //  nn.yarn.io/dist-node-attribute10=dist_v1_10
    toAddAttributes.clear();
    toAddAttributes.put(HOSTNAMES[0],
        createAttributesForTest(NodeAttribute.PREFIX_DISTRIBUTED,
            10, "dist-node-attribute", "dist_v1"));
    attributesManager.addNodeAttributes(toAddAttributes);
    nodeAttributes = attributesManager.getAttributesForNode(HOSTNAMES[0]);
    Assert.assertEquals(13, nodeAttributes.size());
    clusterAttributes = attributesManager.getClusterNodeAttributes(
        Sets.newHashSet(NodeAttribute.PREFIX_DISTRIBUTED, PREFIXES[0]));
    Assert.assertEquals(13, clusterAttributes.size());

    // Replace by prefix
    // Same distributed attributes names, but different values.
    Set<NodeAttribute> toReplaceAttributes =
        createAttributesForTest(NodeAttribute.PREFIX_DISTRIBUTED, 5,
            "dist-node-attribute", "dist_v2");

    attributesManager.replaceNodeAttributes(NodeAttribute.PREFIX_DISTRIBUTED,
        ImmutableMap.of(HOSTNAMES[0], toReplaceAttributes));
    nodeAttributes = attributesManager.getAttributesForNode(HOSTNAMES[0]);
    Assert.assertEquals(8, nodeAttributes.size());
    clusterAttributes = attributesManager.getClusterNodeAttributes(
        Sets.newHashSet(NodeAttribute.PREFIX_DISTRIBUTED, PREFIXES[0]));
    Assert.assertEquals(8, clusterAttributes.size());

    // Now we have 5 distributed attributes
    filteredAttributes = NodeLabelUtil.filterAttributesByPrefix(
        nodeAttributes.keySet(), NodeAttribute.PREFIX_DISTRIBUTED);
    Assert.assertEquals(5, filteredAttributes.size());
    // Values are updated to have prefix dist_v2
    Assert.assertTrue(filteredAttributes.stream().allMatch(
        nodeAttribute ->
            nodeAttribute.getAttributeValue().startsWith("dist_v2")));

    // We still have 3 yarn.test1.io attributes
    filteredAttributes = NodeLabelUtil.filterAttributesByPrefix(
        nodeAttributes.keySet(), PREFIXES[0]);
    Assert.assertEquals(3, filteredAttributes.size());

    // Replace with prefix
    // Different attribute names
    toReplaceAttributes =
        createAttributesForTest(NodeAttribute.PREFIX_DISTRIBUTED, 1,
            "dist-node-attribute-v2", "dist_v3");
    attributesManager.replaceNodeAttributes(NodeAttribute.PREFIX_DISTRIBUTED,
        ImmutableMap.of(HOSTNAMES[0], toReplaceAttributes));
    nodeAttributes = attributesManager.getAttributesForNode(HOSTNAMES[0]);
    Assert.assertEquals(4, nodeAttributes.size());
    clusterAttributes = attributesManager.getClusterNodeAttributes(
        Sets.newHashSet(NodeAttribute.PREFIX_DISTRIBUTED));
    Assert.assertEquals(1, clusterAttributes.size());
    NodeAttribute attr = clusterAttributes.iterator().next();
    Assert.assertEquals("dist-node-attribute-v2_0",
        attr.getAttributeKey().getAttributeName());
    Assert.assertEquals(NodeAttribute.PREFIX_DISTRIBUTED,
        attr.getAttributeKey().getAttributePrefix());
    Assert.assertEquals("dist_v3_0", attr.getAttributeValue());

    // Replace all attributes
    toReplaceMap.put(HOSTNAMES[0],
        createAttributesForTest(PREFIXES[1], 2, "B", "B_v1"));
    attributesManager.replaceNodeAttributes(null, toReplaceMap);

    nodeAttributes = attributesManager.getAttributesForNode(HOSTNAMES[0]);
    Assert.assertEquals(2, nodeAttributes.size());
    clusterAttributes = attributesManager
        .getClusterNodeAttributes(Sets.newHashSet(PREFIXES[1]));
    Assert.assertEquals(2, clusterAttributes.size());
    clusterAttributes = attributesManager
        .getClusterNodeAttributes(Sets.newHashSet(
            NodeAttribute.PREFIX_DISTRIBUTED));
    Assert.assertEquals(0, clusterAttributes.size());
  }
}
