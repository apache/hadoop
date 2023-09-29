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
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Verify that chooseTarget can exclude nodes with high volume average load.
 */
public class TestReplicationPolicyRatioConsiderLoadWithStorage
    extends BaseReplicationPolicyTest {

  public TestReplicationPolicyRatioConsiderLoadWithStorage() {
    this.blockPlacementPolicy = BlockPlacementPolicyDefault.class.getName();
  }

  @Override
  DatanodeDescriptor[] getDatanodeDescriptors(Configuration conf) {
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY,
        true);
    conf.setDouble(DFSConfigKeys
        .DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_FACTOR, 2);
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOADBYVOLUME_KEY, true);

    final String[] racks = {
        "/rack1",
        "/rack2",
        "/rack3",
        "/rack4",
        "/rack5"};
    storages = DFSTestUtil.createDatanodeStorageInfos(racks);
    DatanodeDescriptor[] descriptors =
        DFSTestUtil.toDatanodeDescriptor(storages);
    long storageCapacity =
        2 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE;
    // Each datanode has 6 storages, but the number of available storages
    // varies.
    for (int i = 0; i < descriptors.length; i++) {
      for (int j = 0; j < 5; j++) {
        DatanodeStorage s =
            new DatanodeStorage("s" + i + j);
        descriptors[i].updateStorage(s);

      }
      for (int j = 0; j < descriptors[i].getStorageInfos().length; j++) {
        DatanodeStorageInfo dsInfo = descriptors[i].getStorageInfos()[j];
        if (j > i + 1) {
          dsInfo.setUtilizationForTesting(storageCapacity, storageCapacity, 0,
              storageCapacity);
        } else {
          dsInfo.setUtilizationForTesting(storageCapacity, 0, storageCapacity,
              0);
        }
      }
    }
    return descriptors;
  }

  /**
   * Tests that chooseTarget with considerLoad and consider volume load set to
   * true and correctly calculates load.
   */
  @Test
  public void testChooseTargetWithRatioConsiderLoad() {
    namenode.getNamesystem().writeLock();
    try {
      // After heartbeat has been processed, the total load should be 200.
      // And average load per node should be 40. The max load should be 2 * 40;
      // And average load per storage should be 10. Considering available
      // storages, the max load should be:
      // 2*10*2, 3*10*2, 4*10*2, 5*10*2, 6*10*2.
      // Considering the load of every node and number of storages:
      // Index:             0,   1,   2,   3,   4
      // Available Storage: 2,   3,   4,   5,   6
      // Load:             50, 110,  28,   2,  10
      // So, dataNodes[1] should be never chosen because over-load of node.
      // And dataNodes[0] should be never chosen because over-load of per
      // storage.
      dnManager.getHeartbeatManager().updateHeartbeat(dataNodes[0],
          BlockManagerTestUtil.getStorageReportsForDatanode(dataNodes[0]),
          dataNodes[0].getCacheCapacity(),
          dataNodes[0].getCacheUsed(),
          50, 0, null);
      dnManager.getHeartbeatManager().updateHeartbeat(dataNodes[1],
          BlockManagerTestUtil.getStorageReportsForDatanode(dataNodes[1]),
          dataNodes[0].getCacheCapacity(),
          dataNodes[0].getCacheUsed(),
          110, 0, null);
      dnManager.getHeartbeatManager().updateHeartbeat(dataNodes[2],
          BlockManagerTestUtil.getStorageReportsForDatanode(dataNodes[2]),
          dataNodes[0].getCacheCapacity(),
          dataNodes[0].getCacheUsed(),
          28, 0, null);
      dnManager.getHeartbeatManager().updateHeartbeat(dataNodes[3],
          BlockManagerTestUtil.getStorageReportsForDatanode(dataNodes[3]),
          dataNodes[0].getCacheCapacity(),
          dataNodes[0].getCacheUsed(),
          2, 0, null);
      dnManager.getHeartbeatManager().updateHeartbeat(dataNodes[4],
          BlockManagerTestUtil.getStorageReportsForDatanode(dataNodes[4]),
          dataNodes[0].getCacheCapacity(),
          dataNodes[0].getCacheUsed(),
          10, 0, null);

      Set<DatanodeDescriptor> targetSet = new HashSet<>();

      // Try to choose 3 datanode targets.
      DatanodeDescriptor writerDn = dataNodes[2];
      DatanodeStorageInfo[] targets = namenode.getNamesystem().getBlockManager()
          .getBlockPlacementPolicy()
          .chooseTarget("testFile.txt", 3, writerDn, new ArrayList<>(), false,
              null, 1024, TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY, null);
      // The result contains 3 nodes(dataNodes[2],dataNodes[3],dataNodes[4]).
      assertEquals(3, targets.length);
      for (DatanodeStorageInfo dsi : targets) {
        targetSet.add(dsi.getDatanodeDescriptor());
      }
      assertTrue(targetSet.contains(dataNodes[2]));
      assertTrue(targetSet.contains(dataNodes[3]));
      assertTrue(targetSet.contains(dataNodes[4]));

      // Try to choose 4 datanode targets.
      targets = namenode.getNamesystem().getBlockManager()
          .getBlockPlacementPolicy()
          .chooseTarget("testFile.txt", 4, writerDn, new ArrayList<>(), false,
              null, 1024, TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY, null);
      // The result contains 3 nodes(dataNodes[2],dataNodes[3],dataNodes[4]).
      assertEquals(3, targets.length);
      targetSet.clear();
      for (DatanodeStorageInfo dsi : targets) {
        targetSet.add(dsi.getDatanodeDescriptor());
      }
      assertTrue(targetSet.contains(dataNodes[2]));
      assertTrue(targetSet.contains(dataNodes[3]));
      assertTrue(targetSet.contains(dataNodes[4]));
    } finally {
      namenode.getNamesystem().writeUnlock();
    }
  }
}