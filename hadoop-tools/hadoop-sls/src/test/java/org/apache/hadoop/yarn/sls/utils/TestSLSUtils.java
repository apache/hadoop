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

package org.apache.hadoop.yarn.sls.utils;

import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.sls.SLSRunner.NodeDetails;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

public class TestSLSUtils {

  @Test
  public void testGetRackHostname() {
    String str = "/rack1/node1";
    String rackHostname[] = SLSUtils.getRackHostName(str);
    Assert.assertEquals(rackHostname[0], "rack1");
    Assert.assertEquals(rackHostname[1], "node1");

    str = "/rackA/rackB/node1";
    rackHostname = SLSUtils.getRackHostName(str);
    Assert.assertEquals(rackHostname[0], "rackA/rackB");
    Assert.assertEquals(rackHostname[1], "node1");
  }

  @Test
  public void testParseNodesFromNodeFile() throws Exception {
    String nodeFile = "src/test/resources/nodes.json";
    Set<NodeDetails> nodeDetails = SLSUtils.parseNodesFromNodeFile(
        nodeFile, Resources.createResource(1024, 2));
    Assert.assertEquals(20, nodeDetails.size());

    nodeFile = "src/test/resources/nodes-with-resources.json";
    nodeDetails = SLSUtils.parseNodesFromNodeFile(
        nodeFile, Resources.createResource(1024, 2));
    Assert.assertEquals(4, nodeDetails.size());
    for (NodeDetails nodeDetail : nodeDetails) {
      if (nodeDetail.getHostname().equals("/rack1/node1")) {
        Assert.assertEquals(2048,
            nodeDetail.getNodeResource().getMemorySize());
        Assert.assertEquals(6,
            nodeDetail.getNodeResource().getVirtualCores());
      } else if (nodeDetail.getHostname().equals("/rack1/node2")) {
        Assert.assertEquals(1024,
            nodeDetail.getNodeResource().getMemorySize());
        Assert.assertEquals(2,
            nodeDetail.getNodeResource().getVirtualCores());
        Assert.assertNull(nodeDetail.getLabels());
      } else if (nodeDetail.getHostname().equals("/rack1/node3")) {
        Assert.assertEquals(1024,
            nodeDetail.getNodeResource().getMemorySize());
        Assert.assertEquals(2,
            nodeDetail.getNodeResource().getVirtualCores());
        Assert.assertEquals(2, nodeDetail.getLabels().size());
        for (NodeLabel nodeLabel : nodeDetail.getLabels()) {
          if (nodeLabel.getName().equals("label1")) {
            Assert.assertTrue(nodeLabel.isExclusive());
          } else if(nodeLabel.getName().equals("label2")) {
            Assert.assertFalse(nodeLabel.isExclusive());
          } else {
            Assert.assertTrue("Unexepected label", false);
          }
        }
      } else if (nodeDetail.getHostname().equals("/rack1/node4")) {
        Assert.assertEquals(6144,
            nodeDetail.getNodeResource().getMemorySize());
        Assert.assertEquals(12,
            nodeDetail.getNodeResource().getVirtualCores());
        Assert.assertEquals(2, nodeDetail.getLabels().size());
      }
    }
  }

  @Test
  public void testGenerateNodes() {
    Set<NodeDetails> nodes = SLSUtils.generateNodes(3, 3);
    Assert.assertEquals("Number of nodes is wrong.", 3, nodes.size());
    Assert.assertEquals("Number of racks is wrong.", 3, getNumRack(nodes));

    nodes = SLSUtils.generateNodes(3, 1);
    Assert.assertEquals("Number of nodes is wrong.", 3, nodes.size());
    Assert.assertEquals("Number of racks is wrong.", 1, getNumRack(nodes));

    nodes = SLSUtils.generateNodes(3, 4);
    Assert.assertEquals("Number of nodes is wrong.", 3, nodes.size());
    Assert.assertEquals("Number of racks is wrong.", 3, getNumRack(nodes));

    nodes = SLSUtils.generateNodes(3, 0);
    Assert.assertEquals("Number of nodes is wrong.", 3, nodes.size());
    Assert.assertEquals("Number of racks is wrong.", 1, getNumRack(nodes));
  }

  private int getNumRack(Set<NodeDetails> nodes) {
    Set<String> racks = new HashSet<>();
    for (NodeDetails node : nodes) {
      String[] rackHostname = SLSUtils.getRackHostName(node.getHostname());
      racks.add(rackHostname[0]);
    }
    return racks.size();
  }
}
