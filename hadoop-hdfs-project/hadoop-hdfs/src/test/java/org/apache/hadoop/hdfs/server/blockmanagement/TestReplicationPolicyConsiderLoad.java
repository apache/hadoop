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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.TestBlockStoragePolicy;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestReplicationPolicyConsiderLoad
    extends BaseReplicationPolicyTest {

  public TestReplicationPolicyConsiderLoad(String blockPlacementPolicy) {
    this.blockPlacementPolicy = blockPlacementPolicy;
  }

  @Parameterized.Parameters
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][] {
        { BlockPlacementPolicyDefault.class.getName() },
        { BlockPlacementPolicyWithUpgradeDomain.class.getName() } });
  }

  @Override
  DatanodeDescriptor[] getDatanodeDescriptors(Configuration conf) {
    conf.setDouble(DFSConfigKeys
        .DFS_NAMENODE_REPLICATION_CONSIDERLOAD_FACTOR, 1.2);
    final String[] racks = {
        "/rack1",
        "/rack1",
        "/rack2",
        "/rack2",
        "/rack3",
        "/rack3"};
    storages = DFSTestUtil.createDatanodeStorageInfos(racks);
    return DFSTestUtil.toDatanodeDescriptor(storages);
  }

  private final double EPSILON = 0.0001;
  /**
   * Tests that chooseTarget with considerLoad set to true correctly calculates
   * load with decommissioned nodes.
   */
  @Test
  public void testChooseTargetWithDecomNodes() throws IOException {
    namenode.getNamesystem().writeLock();
    try {
      dnManager.getHeartbeatManager().updateHeartbeat(dataNodes[3],
          BlockManagerTestUtil.getStorageReportsForDatanode(dataNodes[3]),
          dataNodes[3].getCacheCapacity(),
          dataNodes[3].getCacheUsed(),
          2, 0, null);
      dnManager.getHeartbeatManager().updateHeartbeat(dataNodes[4],
          BlockManagerTestUtil.getStorageReportsForDatanode(dataNodes[4]),
          dataNodes[4].getCacheCapacity(),
          dataNodes[4].getCacheUsed(),
          4, 0, null);
      dnManager.getHeartbeatManager().updateHeartbeat(dataNodes[5],
          BlockManagerTestUtil.getStorageReportsForDatanode(dataNodes[5]),
          dataNodes[5].getCacheCapacity(),
          dataNodes[5].getCacheUsed(),
          4, 0, null);

      // value in the above heartbeats
      final int load = 2 + 4 + 4;
      
      assertEquals((double)load/6, dnManager.getFSClusterStats()
        .getInServiceXceiverAverage(), EPSILON);
      
      // Decommission DNs so BlockPlacementPolicyDefault.isGoodTarget()
      // returns false
      for (int i = 0; i < 3; i++) {
        DatanodeDescriptor d = dataNodes[i];
        dnManager.getDecomManager().startDecommission(d);
        d.setDecommissioned();
      }
      assertEquals((double)load/3, dnManager.getFSClusterStats()
        .getInServiceXceiverAverage(), EPSILON);

      DatanodeDescriptor writerDn = dataNodes[0];

      // Call chooseTarget()
      DatanodeStorageInfo[] targets = namenode.getNamesystem().getBlockManager()
          .getBlockPlacementPolicy().chooseTarget("testFile.txt", 3,
              writerDn, new ArrayList<DatanodeStorageInfo>(), false, null,
              1024, TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY);

      assertEquals(3, targets.length);
      Set<DatanodeStorageInfo> targetSet = new HashSet<>(
          Arrays.asList(targets));
      for (int i = 3; i < storages.length; i++) {
        assertTrue(targetSet.contains(storages[i]));
      }
    } finally {
      dataNodes[0].stopDecommission();
      dataNodes[1].stopDecommission();
      dataNodes[2].stopDecommission();
      namenode.getNamesystem().writeUnlock();
    }
    NameNode.LOG.info("Done working on it");
  }

  @Test
  public void testConsiderLoadFactor() throws IOException {
    namenode.getNamesystem().writeLock();
    try {
      dnManager.getHeartbeatManager().updateHeartbeat(dataNodes[0],
          BlockManagerTestUtil.getStorageReportsForDatanode(dataNodes[0]),
          dataNodes[0].getCacheCapacity(),
          dataNodes[0].getCacheUsed(),
          5, 0, null);
      dnManager.getHeartbeatManager().updateHeartbeat(dataNodes[1],
          BlockManagerTestUtil.getStorageReportsForDatanode(dataNodes[1]),
          dataNodes[1].getCacheCapacity(),
          dataNodes[1].getCacheUsed(),
          10, 0, null);
      dnManager.getHeartbeatManager().updateHeartbeat(dataNodes[2],
          BlockManagerTestUtil.getStorageReportsForDatanode(dataNodes[2]),
          dataNodes[2].getCacheCapacity(),
          dataNodes[2].getCacheUsed(),
          5, 0, null);

      dnManager.getHeartbeatManager().updateHeartbeat(dataNodes[3],
          BlockManagerTestUtil.getStorageReportsForDatanode(dataNodes[3]),
          dataNodes[3].getCacheCapacity(),
          dataNodes[3].getCacheUsed(),
          10, 0, null);
      dnManager.getHeartbeatManager().updateHeartbeat(dataNodes[4],
          BlockManagerTestUtil.getStorageReportsForDatanode(dataNodes[4]),
          dataNodes[4].getCacheCapacity(),
          dataNodes[4].getCacheUsed(),
          15, 0, null);
      dnManager.getHeartbeatManager().updateHeartbeat(dataNodes[5],
          BlockManagerTestUtil.getStorageReportsForDatanode(dataNodes[5]),
          dataNodes[5].getCacheCapacity(),
          dataNodes[5].getCacheUsed(),
          15, 0, null);
      //Add values in above heartbeats
      double load = 5 + 10 + 15 + 10 + 15 + 5;
      // Call chooseTarget()
      DatanodeDescriptor writerDn = dataNodes[0];
      DatanodeStorageInfo[] targets = namenode.getNamesystem().getBlockManager()
          .getBlockPlacementPolicy().chooseTarget("testFile.txt", 3, writerDn,
              new ArrayList<DatanodeStorageInfo>(), false, null,
              1024, TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY);
      for(DatanodeStorageInfo info : targets) {
        assertTrue("The node "+info.getDatanodeDescriptor().getName()+
                " has higher load and should not have been picked!",
            info.getDatanodeDescriptor().getXceiverCount() <= (load/6)*1.2);
      }
    } finally {
      namenode.getNamesystem().writeUnlock();
    }
  }
}
