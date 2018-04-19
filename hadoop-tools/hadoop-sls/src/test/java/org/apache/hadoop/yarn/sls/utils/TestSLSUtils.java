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

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Map;
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
    Map<String, Resource> nodeResourceMap = SLSUtils.parseNodesFromNodeFile(
        nodeFile, Resources.createResource(1024, 2));
    Assert.assertEquals(20, nodeResourceMap.size());

    nodeFile = "src/test/resources/nodes-with-resources.json";
    nodeResourceMap = SLSUtils.parseNodesFromNodeFile(
        nodeFile, Resources.createResource(1024, 2));
    Assert.assertEquals(4,
        nodeResourceMap.size());
    Assert.assertEquals(2048,
        nodeResourceMap.get("/rack1/node1").getMemorySize());
    Assert.assertEquals(6,
        nodeResourceMap.get("/rack1/node1").getVirtualCores());
    Assert.assertEquals(1024,
        nodeResourceMap.get("/rack1/node2").getMemorySize());
    Assert.assertEquals(2,
        nodeResourceMap.get("/rack1/node2").getVirtualCores());
  }

  @Test
  public void testGenerateNodes() {
    Set<? extends String> nodes = SLSUtils.generateNodes(3, 3);
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

  private int getNumRack(Set<? extends String> nodes) {
    Set<String> racks = new HashSet<>();
    for (String node : nodes) {
      String[] rackHostname = SLSUtils.getRackHostName(node);
      racks.add(rackHostname[0]);
    }
    return racks.size();
  }
}
