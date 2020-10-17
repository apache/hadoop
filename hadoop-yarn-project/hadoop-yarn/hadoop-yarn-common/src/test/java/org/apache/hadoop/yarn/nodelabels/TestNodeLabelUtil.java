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

import static org.junit.Assert.fail;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeAttributeType;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test class to verify node label util ops.
 */
public class TestNodeLabelUtil {

  @Test
  public void testAttributeValueAddition() {
    String[] values =
        new String[]{"1_8", "1.8", "ABZ", "ABZ", "az", "a-z", "a_z",
            "123456789"};
    for (String val : values) {
      try {
        NodeLabelUtil.checkAndThrowAttributeValue(val);
      } catch (Exception e) {
        fail("Valid values for NodeAttributeValue :" + val);
      }
    }

    String[] invalidVals = new String[]{"_18", "1,8", "1/5", ".15", "1\\5"};
    for (String val : invalidVals) {
      try {
        NodeLabelUtil.checkAndThrowAttributeValue(val);
        fail("Valid values for NodeAttributeValue :" + val);
      } catch (Exception e) {
        // IGNORE
      }
    }
  }

  @Test
  public void testIsNodeAttributesEquals() {
    NodeAttribute nodeAttributeCK1V1 = NodeAttribute
        .newInstance(NodeAttribute.PREFIX_CENTRALIZED, "K1",
            NodeAttributeType.STRING, "V1");
    NodeAttribute nodeAttributeCK1V1Copy = NodeAttribute
        .newInstance(NodeAttribute.PREFIX_CENTRALIZED, "K1",
            NodeAttributeType.STRING, "V1");
    NodeAttribute nodeAttributeDK1V1 = NodeAttribute
        .newInstance(NodeAttribute.PREFIX_DISTRIBUTED, "K1",
            NodeAttributeType.STRING, "V1");
    NodeAttribute nodeAttributeDK1V1Copy = NodeAttribute
        .newInstance(NodeAttribute.PREFIX_DISTRIBUTED, "K1",
            NodeAttributeType.STRING, "V1");
    NodeAttribute nodeAttributeDK2V1 = NodeAttribute
        .newInstance(NodeAttribute.PREFIX_DISTRIBUTED, "K2",
            NodeAttributeType.STRING, "V1");
    NodeAttribute nodeAttributeDK2V2 = NodeAttribute
        .newInstance(NodeAttribute.PREFIX_DISTRIBUTED, "K2",
            NodeAttributeType.STRING, "V2");
    /*
     * equals if set size equals and items are all the same
     */
    Assert.assertTrue(NodeLabelUtil.isNodeAttributesEquals(null, null));
    Assert.assertTrue(NodeLabelUtil
        .isNodeAttributesEquals(ImmutableSet.of(), ImmutableSet.of()));
    Assert.assertTrue(NodeLabelUtil
        .isNodeAttributesEquals(ImmutableSet.of(nodeAttributeCK1V1),
            ImmutableSet.of(nodeAttributeCK1V1Copy)));
    Assert.assertTrue(NodeLabelUtil
        .isNodeAttributesEquals(ImmutableSet.of(nodeAttributeDK1V1),
            ImmutableSet.of(nodeAttributeDK1V1Copy)));
    Assert.assertTrue(NodeLabelUtil.isNodeAttributesEquals(
        ImmutableSet.of(nodeAttributeCK1V1, nodeAttributeDK1V1),
        ImmutableSet.of(nodeAttributeCK1V1Copy, nodeAttributeDK1V1Copy)));
    /*
     * not equals if set size not equals or items are different
     */
    Assert.assertFalse(
        NodeLabelUtil.isNodeAttributesEquals(null, ImmutableSet.of()));
    Assert.assertFalse(
        NodeLabelUtil.isNodeAttributesEquals(ImmutableSet.of(), null));
    // different attribute prefix
    Assert.assertFalse(NodeLabelUtil
        .isNodeAttributesEquals(ImmutableSet.of(nodeAttributeCK1V1),
            ImmutableSet.of(nodeAttributeDK1V1)));
    // different attribute name
    Assert.assertFalse(NodeLabelUtil
        .isNodeAttributesEquals(ImmutableSet.of(nodeAttributeDK1V1),
            ImmutableSet.of(nodeAttributeDK2V1)));
    // different attribute value
    Assert.assertFalse(NodeLabelUtil
        .isNodeAttributesEquals(ImmutableSet.of(nodeAttributeDK2V1),
            ImmutableSet.of(nodeAttributeDK2V2)));
    // different set
    Assert.assertFalse(NodeLabelUtil
        .isNodeAttributesEquals(ImmutableSet.of(nodeAttributeCK1V1),
            ImmutableSet.of()));
    Assert.assertFalse(NodeLabelUtil
        .isNodeAttributesEquals(ImmutableSet.of(nodeAttributeCK1V1),
            ImmutableSet.of(nodeAttributeCK1V1, nodeAttributeDK1V1)));
    Assert.assertFalse(NodeLabelUtil.isNodeAttributesEquals(
        ImmutableSet.of(nodeAttributeCK1V1, nodeAttributeDK1V1),
        ImmutableSet.of(nodeAttributeDK1V1)));
  }
}
