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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.TestBlockStoragePolicy;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.net.Node;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class TestReplicationPolicyExcludeSlowNodes
    extends BaseReplicationPolicyTest {

  public TestReplicationPolicyExcludeSlowNodes(String blockPlacementPolicy) {
    this.blockPlacementPolicy = blockPlacementPolicy;
  }

  @Parameterized.Parameters
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {BlockPlacementPolicyDefault.class.getName()},
        {BlockPlacementPolicyWithUpgradeDomain.class.getName()},
        {AvailableSpaceBlockPlacementPolicy.class.getName()},
        {BlockPlacementPolicyRackFaultTolerant.class.getName()}
    });
  }

  @Override
  DatanodeDescriptor[] getDatanodeDescriptors(Configuration conf) {
    conf.setBoolean(DFSConfigKeys
        .DFS_DATANODE_PEER_STATS_ENABLED_KEY,
        true);
    conf.setStrings(DFSConfigKeys
        .DFS_NAMENODE_SLOWPEER_COLLECT_INTERVAL_KEY,
        "1s");
    conf.setBoolean(DFSConfigKeys
        .DFS_NAMENODE_BLOCKPLACEMENTPOLICY_EXCLUDE_SLOW_NODES_ENABLED_KEY,
        true);
    final String[] racks = {
        "/rack1",
        "/rack2",
        "/rack3",
        "/rack4",
        "/rack5",
        "/rack6"};
    storages = DFSTestUtil.createDatanodeStorageInfos(racks);
    return DFSTestUtil.toDatanodeDescriptor(storages);
  }

  /**
   * Tests that chooseTarget when excludeSlowNodesEnabled set to true.
   */
  @Test
  public void testChooseTargetExcludeSlowNodes() throws Exception {
    namenode.getNamesystem().writeLock();
    try {
      // add nodes
      for (int i = 0; i < dataNodes.length; i++) {
        dnManager.addDatanode(dataNodes[i]);
      }

      // mock slow nodes
      SlowPeerTracker tracker = dnManager.getSlowPeerTracker();
      tracker.addReport(dataNodes[0].getInfoAddr(), dataNodes[3].getInfoAddr());
      tracker.addReport(dataNodes[0].getInfoAddr(), dataNodes[4].getInfoAddr());
      tracker.addReport(dataNodes[1].getInfoAddr(), dataNodes[4].getInfoAddr());
      tracker.addReport(dataNodes[1].getInfoAddr(), dataNodes[5].getInfoAddr());
      tracker.addReport(dataNodes[2].getInfoAddr(), dataNodes[3].getInfoAddr());
      tracker.addReport(dataNodes[2].getInfoAddr(), dataNodes[5].getInfoAddr());

      // waiting for slow nodes collector run
      Thread.sleep(3000);

      // fetch slow nodes
      Set<Node> slowPeers = dnManager.getSlowPeers();

      // assert slow nodes
      assertEquals(3, slowPeers.size());
      for (int i = 0; i < slowPeers.size(); i++) {
        assertTrue(slowPeers.contains(dataNodes[i]));
      }

      // mock writer
      DatanodeDescriptor writerDn = dataNodes[0];

      // call chooseTarget()
      DatanodeStorageInfo[] targets = namenode.getNamesystem().getBlockManager()
          .getBlockPlacementPolicy().chooseTarget("testFile.txt", 3,
              writerDn, new ArrayList<DatanodeStorageInfo>(), false, null,
              1024, TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY, null);

      // assert targets
      assertEquals(3, targets.length);
      for (int i = 0; i < targets.length; i++) {
        assertTrue(!slowPeers.contains(targets[i].getDatanodeDescriptor()));
      }
    } finally {
      namenode.getNamesystem().writeUnlock();
    }
    NameNode.LOG.info("Done working on it");
  }

}
