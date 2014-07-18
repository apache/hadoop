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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.util.VersionInfo;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestReplicationPolicyConsiderLoad {

  private static NameNode namenode;
  private static DatanodeManager dnManager;
  private static List<DatanodeRegistration> dnrList;
  private static DatanodeDescriptor[] dataNodes;
  private static DatanodeStorageInfo[] storages;

  @BeforeClass
  public static void setupCluster() throws IOException {
    Configuration conf = new HdfsConfiguration();
    final String[] racks = {
        "/rack1",
        "/rack1",
        "/rack1",
        "/rack2",
        "/rack2",
        "/rack2"};
    storages = DFSTestUtil.createDatanodeStorageInfos(racks);
    dataNodes = DFSTestUtil.toDatanodeDescriptor(storages);
    FileSystem.setDefaultUri(conf, "hdfs://localhost:0");
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    File baseDir = PathUtils.getTestDir(TestReplicationPolicy.class);
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        new File(baseDir, "name").getPath());
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_KEY, true);
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_KEY, true);
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_CONSIDERLOAD_KEY, true);
    DFSTestUtil.formatNameNode(conf);
    namenode = new NameNode(conf);
    int blockSize = 1024;

    dnrList = new ArrayList<DatanodeRegistration>();
    dnManager = namenode.getNamesystem().getBlockManager().getDatanodeManager();

    // Register DNs
    for (int i=0; i < 6; i++) {
      DatanodeRegistration dnr = new DatanodeRegistration(dataNodes[i],
          new StorageInfo(NodeType.DATA_NODE), new ExportedBlockKeys(),
          VersionInfo.getVersion());
      dnrList.add(dnr);
      dnManager.registerDatanode(dnr);
      dataNodes[i].getStorageInfos()[0].setUtilizationForTesting(
          2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*blockSize, 0L,
          2*HdfsConstants.MIN_BLOCKS_FOR_WRITE*blockSize, 0L);
      dataNodes[i].updateHeartbeat(
          BlockManagerTestUtil.getStorageReportsForDatanode(dataNodes[i]),
          0L, 0L, 0, 0);
    }
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
      String blockPoolId = namenode.getNamesystem().getBlockPoolId();
      dnManager.handleHeartbeat(dnrList.get(3),
          BlockManagerTestUtil.getStorageReportsForDatanode(dataNodes[3]),
          blockPoolId, dataNodes[3].getCacheCapacity(),
          dataNodes[3].getCacheRemaining(),
          2, 0, 0);
      dnManager.handleHeartbeat(dnrList.get(4),
          BlockManagerTestUtil.getStorageReportsForDatanode(dataNodes[4]),
          blockPoolId, dataNodes[4].getCacheCapacity(),
          dataNodes[4].getCacheRemaining(),
          4, 0, 0);
      dnManager.handleHeartbeat(dnrList.get(5),
          BlockManagerTestUtil.getStorageReportsForDatanode(dataNodes[5]),
          blockPoolId, dataNodes[5].getCacheCapacity(),
          dataNodes[5].getCacheRemaining(),
          4, 0, 0);
      // value in the above heartbeats
      final int load = 2 + 4 + 4;
      
      FSNamesystem fsn = namenode.getNamesystem();
      assertEquals((double)load/6, fsn.getInServiceXceiverAverage(), EPSILON);
      
      // Decommission DNs so BlockPlacementPolicyDefault.isGoodTarget()
      // returns false
      for (int i = 0; i < 3; i++) {
        DatanodeDescriptor d = dnManager.getDatanode(dnrList.get(i));
        dnManager.startDecommission(d);
        d.setDecommissioned();
      }
      assertEquals((double)load/3, fsn.getInServiceXceiverAverage(), EPSILON);

      // Call chooseTarget()
      DatanodeStorageInfo[] targets = namenode.getNamesystem().getBlockManager()
          .getBlockPlacementPolicy().chooseTarget("testFile.txt", 3,
              dataNodes[0], new ArrayList<DatanodeStorageInfo>(), false, null,
              1024, StorageType.DEFAULT);

      assertEquals(3, targets.length);
      Set<DatanodeStorageInfo> targetSet = new HashSet<DatanodeStorageInfo>(
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
  }

  @AfterClass
  public static void teardownCluster() {
    if (namenode != null) namenode.stop();
  }

}
