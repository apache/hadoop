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

package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.MiniDFSNNTopology.NNConf;
import org.apache.hadoop.hdfs.MiniDFSNNTopology.NSConf;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.net.StaticMapping;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestRefreshTopology {
  private MiniDFSCluster cluster = null;
  private final String[] locations =
      {"/r1/d1", "/r1/d2", "/r1/d3", "/r2/d4", "/r2/d5" };
  private final Configuration conf = new Configuration();
  private StaticMapping mapping;

  @Before
  public void setUp() throws IOException {
    MiniDFSNNTopology topology = new MiniDFSNNTopology().addNameservice(
        new NSConf("ns").addNN(new NNConf(null)));
    mapping = newInstance();
    conf.set(StaticMapping.KEY_HADOOP_CONFIGURED_NODE_MAPPING,
        "dn1=/r1/d1,dn2=/r1/d2,dn3=/r1/d3,dn4=/r2/d4,dn5=/r2/d5");
    mapping.setconf(conf);
    final String[] hosts = {"dn1", "dn2", "dn3", "dn4", "dn5"};
    cluster = new MiniDFSCluster.Builder(conf).nnTopology(topology)
        .numDataNodes(5).hosts(hosts).build();
  }

  @Test
  public void testOriginalTopology() {
    int matchCount = 0;
    DatanodeManager dnManager = cluster.getNameNode().getNamesystem()
        .getBlockManager().getDatanodeManager();
    for (DatanodeDescriptor descriptor : dnManager.getDatanodes()) {
      if (descriptor.getNetworkLocation().equals(locations[0])
          || descriptor.getNetworkLocation().equals(locations[1])
          || descriptor.getNetworkLocation().equals(locations[2])
          || descriptor.getNetworkLocation().equals(locations[3])
          || descriptor.getNetworkLocation().equals(locations[4])) {
        matchCount++;
      }
    }
    Assert.assertEquals(matchCount, locations.length);
  }

  @Test
  public void testRefreshTopology() throws IOException {
    Map<String, String> oldMapping = mapping.getSwitchMap();
    StaticMapping.resetMap();
    StaticMapping.addNodeToRack("dn1", "/r2/d1");
    StaticMapping.addNodeToRack("dn2", "/r2/d2");
    StaticMapping.addNodeToRack("dn3", "/r1/d3"); // dn3's topology remains.
    StaticMapping.addNodeToRack("dn4", "/r1/d4");
    StaticMapping.addNodeToRack("dn5", "/r3/d5");

    DatanodeManager dnManager = cluster.getNameNode().getNamesystem()
        .getBlockManager().getDatanodeManager();
    // refresh topology
    Assert.assertTrue(dnManager.refreshTopology(conf, "dn1"));
    Assert.assertTrue(dnManager.refreshTopology(conf, "dn2"));
    Assert.assertTrue(dnManager.refreshTopology(conf, "dn3"));
    Assert.assertTrue(dnManager.refreshTopology(conf, "dn4"));
    Assert.assertTrue(dnManager.refreshTopology(conf, "dn5"));

    int match = 0;
    for (DatanodeDescriptor descriptor : dnManager.getDatanodes()) {
      if (descriptor.getNetworkLocation().equals("/r2/d1")
          || descriptor.getNetworkLocation().equals("/r2/d2")
          || descriptor.getNetworkLocation().equals("/r1/d3")
          || descriptor.getNetworkLocation().equals("/r1/d4")
          || descriptor.getNetworkLocation().equals("/r3/d5")) {
        match++;
      }
    }
    Assert.assertEquals(match, 5);

    int changed = 0;
    for (DatanodeDescriptor descriptor : dnManager.getDatanodes()) {
      if (!oldMapping.containsValue(descriptor.getNetworkLocation())) {
        changed++;
      }
    }
    Assert.assertEquals(changed, 4);
  }

  private StaticMapping newInstance() {
    StaticMapping.resetMap();
    return new StaticMapping();
  }
}
