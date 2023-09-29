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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeAttributeType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.InlineDispatcher;
import org.apache.hadoop.yarn.nodelabels.AttributeValue;
import org.apache.hadoop.yarn.nodelabels.NodeAttributeStore;
import org.apache.hadoop.yarn.server.resourcemanager.NodeAttributeTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Test class for FileSystemNodeAttributeStore.
 */
public class TestFileSystemNodeAttributeStore {

  private MockNodeAttrbuteManager mgr = null;
  private Configuration conf = null;

  private static class MockNodeAttrbuteManager
      extends NodeAttributesManagerImpl {
    @Override
    protected void initDispatcher(Configuration conf) {
      super.dispatcher = new InlineDispatcher();
    }

    @Override
    protected void startDispatcher() {
      //Do nothing
    }

    @Override
    protected void stopDispatcher() {
      //Do nothing
    }
  }

  @Before
  public void before() throws IOException {
    mgr = new MockNodeAttrbuteManager();
    conf = new Configuration();
    conf.setClass(YarnConfiguration.FS_NODE_ATTRIBUTE_STORE_IMPL_CLASS,
        FileSystemNodeAttributeStore.class, NodeAttributeStore.class);
    conf = NodeAttributeTestUtils.getRandomDirConf(conf);
    mgr.init(conf);
    mgr.start();
  }

  @After
  public void after() throws IOException {
    FileSystemNodeAttributeStore fsStore =
        ((FileSystemNodeAttributeStore) mgr.store);
    fsStore.getFs().delete(fsStore.getFsWorkingPath(), true);
    mgr.stop();
  }

  @Test(timeout = 10000)
  public void testEmptyRecoverSkipInternalUdpate() throws Exception {
    // Stop manager
    mgr.stop();

    // Start new attribute manager with same path
    mgr = spy(new MockNodeAttrbuteManager());
    mgr.init(conf);
    mgr.start();

    verify(mgr, times(0))
        .internalUpdateAttributesOnNodes(any(), any(), any(), any());
  }

  @Test(timeout = 10000)
  public void testRecoverWithMirror() throws Exception {

    //------host0----
    // add       -GPU & FPGA
    // remove    -GPU
    // replace   -Docker
    //------host1----
    // add--GPU
    NodeAttribute docker = NodeAttribute
        .newInstance(NodeAttribute.PREFIX_CENTRALIZED, "DOCKER",
            NodeAttributeType.STRING, "docker-0");
    NodeAttribute gpu = NodeAttribute
        .newInstance(NodeAttribute.PREFIX_CENTRALIZED, "GPU",
            NodeAttributeType.STRING, "nvidia");
    NodeAttribute fpga = NodeAttribute
        .newInstance(NodeAttribute.PREFIX_CENTRALIZED, "FPGA",
            NodeAttributeType.STRING, "asus");

    Map<String, Set<NodeAttribute>> toAddAttributes = new HashMap<>();
    toAddAttributes.put("host0", ImmutableSet.of(gpu, fpga));
    toAddAttributes.put("host1", ImmutableSet.of(gpu));
    // Add node attribute
    mgr.addNodeAttributes(toAddAttributes);

    Assert.assertEquals("host0 size", 2,
        mgr.getAttributesForNode("host0").size());
    // Add test to remove
    toAddAttributes.clear();
    toAddAttributes.put("host0", ImmutableSet.of(gpu));
    mgr.removeNodeAttributes(toAddAttributes);

    // replace nodeattribute
    toAddAttributes.clear();
    toAddAttributes.put("host0", ImmutableSet.of(docker));
    mgr.replaceNodeAttributes(NodeAttribute.PREFIX_CENTRALIZED,
        toAddAttributes);
    Map<NodeAttribute, AttributeValue> attrs =
        mgr.getAttributesForNode("host0");
    assertThat(attrs).hasSize(1);
    assertThat(attrs.keySet().toArray()[0]).isEqualTo(docker);
    mgr.stop();

    // Start new attribute manager with same path
    mgr = new MockNodeAttrbuteManager();
    mgr.init(conf);
    mgr.start();

    mgr.getAttributesForNode("host0");
    Assert.assertEquals("host0 size", 1,
        mgr.getAttributesForNode("host0").size());
    Assert.assertEquals("host1 size", 1,
        mgr.getAttributesForNode("host1").size());
    attrs = mgr.getAttributesForNode("host0");
    assertThat(attrs).hasSize(1);
    assertThat(attrs.keySet().toArray()[0]).isEqualTo(docker);
    //------host0----
    // current       - docker
    // replace       - gpu
    //----- host1----
    // current       - gpu
    // add           - docker
    toAddAttributes.clear();
    toAddAttributes.put("host0", ImmutableSet.of(gpu));
    mgr.replaceNodeAttributes(NodeAttribute.PREFIX_CENTRALIZED,
        toAddAttributes);

    toAddAttributes.clear();
    toAddAttributes.put("host1", ImmutableSet.of(docker));
    mgr.addNodeAttributes(toAddAttributes);
    // Recover from mirror and edit log
    mgr.stop();

    mgr = new MockNodeAttrbuteManager();
    mgr.init(conf);
    mgr.start();
    Assert.assertEquals("host0 size", 1,
        mgr.getAttributesForNode("host0").size());
    Assert.assertEquals("host1 size", 2,
        mgr.getAttributesForNode("host1").size());
    attrs = mgr.getAttributesForNode("host0");
    assertThat(attrs).hasSize(1);
    assertThat(attrs.keySet().toArray()[0]).isEqualTo(gpu);
    attrs = mgr.getAttributesForNode("host1");
    Assert.assertTrue(attrs.keySet().contains(docker));
    Assert.assertTrue(attrs.keySet().contains(gpu));
  }

  @Test(timeout = 10000)
  public void testRecoverFromEditLog() throws Exception {
    NodeAttribute docker = NodeAttribute
        .newInstance(NodeAttribute.PREFIX_CENTRALIZED, "DOCKER",
            NodeAttributeType.STRING, "docker-0");
    NodeAttribute gpu = NodeAttribute
        .newInstance(NodeAttribute.PREFIX_CENTRALIZED, "GPU",
            NodeAttributeType.STRING, "nvidia");
    NodeAttribute fpga = NodeAttribute
        .newInstance(NodeAttribute.PREFIX_CENTRALIZED, "FPGA",
            NodeAttributeType.STRING, "asus");

    Map<String, Set<NodeAttribute>> toAddAttributes = new HashMap<>();
    toAddAttributes.put("host0", ImmutableSet.of(gpu, fpga));
    toAddAttributes.put("host1", ImmutableSet.of(docker));

    // Add node attribute
    mgr.addNodeAttributes(toAddAttributes);

    Assert.assertEquals("host0 size", 2,
        mgr.getAttributesForNode("host0").size());

    //  Increase editlog operation
    for (int i = 0; i < 5; i++) {
      // Add gpu host1
      toAddAttributes.clear();
      toAddAttributes.put("host0", ImmutableSet.of(gpu));
      mgr.removeNodeAttributes(toAddAttributes);

      // Add gpu host1
      toAddAttributes.clear();
      toAddAttributes.put("host1", ImmutableSet.of(docker));
      mgr.addNodeAttributes(toAddAttributes);

      // Remove GPU replace
      toAddAttributes.clear();
      toAddAttributes.put("host0", ImmutableSet.of(gpu));
      mgr.replaceNodeAttributes(NodeAttribute.PREFIX_CENTRALIZED,
          toAddAttributes);

      // Add fgpa host1
      toAddAttributes.clear();
      toAddAttributes.put("host1", ImmutableSet.of(gpu));
      mgr.addNodeAttributes(toAddAttributes);
    }
    mgr.stop();

    // Start new attribute manager with same path
    mgr = new MockNodeAttrbuteManager();
    mgr.init(conf);
    mgr.start();

    Assert.assertEquals("host0 size", 1,
        mgr.getAttributesForNode("host0").size());
    Assert.assertEquals("host1 size", 2,
        mgr.getAttributesForNode("host1").size());

    toAddAttributes.clear();
    NodeAttribute replaced =
        NodeAttribute.newInstance("GPU2", NodeAttributeType.STRING, "nvidia2");
    toAddAttributes.put("host0", ImmutableSet.of(replaced));
    mgr.replaceNodeAttributes(NodeAttribute.PREFIX_CENTRALIZED,
        toAddAttributes);
    mgr.stop();

    mgr = new MockNodeAttrbuteManager();
    mgr.init(conf);
    mgr.start();
    Map<NodeAttribute, AttributeValue> valueMap =
        mgr.getAttributesForNode("host0");
    Map.Entry<NodeAttribute, AttributeValue> entry =
        valueMap.entrySet().iterator().next();
    NodeAttribute attribute = entry.getKey();
    Assert.assertEquals("host0 size", 1,
        mgr.getAttributesForNode("host0").size());
    Assert.assertEquals("host1 size", 2,
        mgr.getAttributesForNode("host1").size());
    checkNodeAttributeEqual(replaced, attribute);
  }

  public void checkNodeAttributeEqual(NodeAttribute atr1, NodeAttribute atr2) {
    Assert.assertEquals(atr1.getAttributeType(), atr2.getAttributeType());
    Assert.assertEquals(atr1.getAttributeKey().getAttributeName(),
        atr2.getAttributeKey().getAttributeName());
    Assert.assertEquals(atr1.getAttributeKey().getAttributePrefix(),
        atr2.getAttributeKey().getAttributePrefix());
    Assert.assertEquals(atr1.getAttributeValue(), atr2.getAttributeValue());
  }
}
