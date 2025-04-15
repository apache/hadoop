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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TestSLSUtils {

  @Test
  public void testGetRackHostname() {
    String str = "/rack1/node1";
    String[] rackHostname = SLSUtils.getRackHostName(str);
    Assertions.assertEquals("rack1", rackHostname[0]);
    Assertions.assertEquals("node1", rackHostname[1]);

    str = "/rackA/rackB/node1";
    rackHostname = SLSUtils.getRackHostName(str);
    Assertions.assertEquals("rackA/rackB", rackHostname[0]);
    Assertions.assertEquals("node1", rackHostname[1]);
  }

  @Test
  public void testParseNodesFromNodeFile() throws Exception {
    String nodeFile = "src/test/resources/nodes.json";
    Set<NodeDetails> nodeDetails = SLSUtils.parseNodesFromNodeFile(
        nodeFile, Resources.createResource(1024, 2));
    Assertions.assertEquals(20, nodeDetails.size());

    nodeFile = "src/test/resources/nodes-with-resources.json";
    nodeDetails = SLSUtils.parseNodesFromNodeFile(
        nodeFile, Resources.createResource(1024, 2));
    Assertions.assertEquals(4, nodeDetails.size());
    for (NodeDetails nodeDetail : nodeDetails) {
      if (nodeDetail.getHostname().equals("/rack1/node1")) {
        Assertions.assertEquals(2048,
            nodeDetail.getNodeResource().getMemorySize());
        Assertions.assertEquals(6,
            nodeDetail.getNodeResource().getVirtualCores());
      } else if (nodeDetail.getHostname().equals("/rack1/node2")) {
        Assertions.assertEquals(1024,
            nodeDetail.getNodeResource().getMemorySize());
        Assertions.assertEquals(2,
            nodeDetail.getNodeResource().getVirtualCores());
        Assertions.assertNull(nodeDetail.getLabels());
      } else if (nodeDetail.getHostname().equals("/rack1/node3")) {
        Assertions.assertEquals(1024,
            nodeDetail.getNodeResource().getMemorySize());
        Assertions.assertEquals(2,
            nodeDetail.getNodeResource().getVirtualCores());
        Assertions.assertEquals(2, nodeDetail.getLabels().size());
        for (NodeLabel nodeLabel : nodeDetail.getLabels()) {
          if (nodeLabel.getName().equals("label1")) {
            Assertions.assertTrue(nodeLabel.isExclusive());
          } else if(nodeLabel.getName().equals("label2")) {
            Assertions.assertFalse(nodeLabel.isExclusive());
          } else {
            Assertions.fail("Unexpected label");
          }
        }
      } else if (nodeDetail.getHostname().equals("/rack1/node4")) {
        Assertions.assertEquals(6144,
            nodeDetail.getNodeResource().getMemorySize());
        Assertions.assertEquals(12,
            nodeDetail.getNodeResource().getVirtualCores());
        Assertions.assertEquals(2, nodeDetail.getLabels().size());
      }
    }
  }

  @Test
  public void testGenerateNodes() {
    Set<NodeDetails> nodes = SLSUtils.generateNodes(3, 3);
    Assertions.assertEquals(3, nodes.size(), "Number of nodes is wrong.");
    Assertions.assertEquals(3, getNumRack(nodes), "Number of racks is wrong.");

    nodes = SLSUtils.generateNodes(3, 1);
    Assertions.assertEquals(3, nodes.size(), "Number of nodes is wrong.");
    Assertions.assertEquals(1, getNumRack(nodes), "Number of racks is wrong.");

    nodes = SLSUtils.generateNodes(3, 4);
    Assertions.assertEquals(3, nodes.size(), "Number of nodes is wrong.");
    Assertions.assertEquals(3, getNumRack(nodes), "Number of racks is wrong.");

    nodes = SLSUtils.generateNodes(3, 0);
    Assertions.assertEquals(3, nodes.size(), "Number of nodes is wrong.");
    Assertions.assertEquals(1, getNumRack(nodes), "Number of racks is wrong.");
  }

  /**
   * Tests creation of table mapping based on given node details.
   * @throws Exception
   */
  @Test
  public void testGenerateNodeTableMapping() throws Exception {
    Set<NodeDetails> nodes = SLSUtils.generateNodes(3, 3);
    File tempFile = File.createTempFile("testslsutils", ".tmp");
    tempFile.deleteOnExit();
    String fileName = tempFile.getAbsolutePath();
    SLSUtils.generateNodeTableMapping(nodes, fileName);

    List<String> lines = Files.readAllLines(Paths.get(fileName));
    Assertions.assertEquals(3, lines.size());
    for (String line : lines) {
      Assertions.assertTrue(line.contains("node"));
      Assertions.assertTrue(line.contains("/rack"));
    }
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
