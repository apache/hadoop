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

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.TestBlockStoragePolicy;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.test.PathUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestAvailableSpaceBlockPlacementPolicy {
  private final static int numRacks = 4;
  private final static int nodesPerRack = 5;
  private final static int blockSize = 1024;
  private final static int chooseTimes = 10000;
  private final static String file = "/tobers/test";
  private final static int replica = 3;

  private static DatanodeStorageInfo[] storages;
  private static DatanodeDescriptor[] dataNodes;
  private static Configuration conf;
  private static NameNode namenode;
  private static BlockPlacementPolicy placementPolicy;
  private static NetworkTopology cluster;

  @BeforeClass
  public static void setupCluster() throws Exception {
    conf = new HdfsConfiguration();
    conf.setFloat(DFSConfigKeys.
        DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY,
            0.6f);
    conf.setInt(DFSConfigKeys.
        DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_LIMIT_KEY,
            93);
    String[] racks = new String[numRacks];
    for (int i = 0; i < numRacks; i++) {
      racks[i] = "/rack" + i;
    }

    String[] owerRackOfNodes = new String[numRacks * nodesPerRack];
    for (int i = 0; i < nodesPerRack; i++) {
      for (int j = 0; j < numRacks; j++) {
        owerRackOfNodes[i * numRacks + j] = racks[j];
      }
    }

    storages = DFSTestUtil.createDatanodeStorageInfos(owerRackOfNodes);
    dataNodes = DFSTestUtil.toDatanodeDescriptor(storages);

    FileSystem.setDefaultUri(conf, "hdfs://localhost:0");
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    File baseDir = PathUtils.getTestDir(AvailableSpaceBlockPlacementPolicy.class);
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, new File(baseDir, "name").getPath());
    conf.set(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
      AvailableSpaceBlockPlacementPolicy.class.getName());

    DFSTestUtil.formatNameNode(conf);
    namenode = new NameNode(conf);

    final BlockManager bm = namenode.getNamesystem().getBlockManager();
    placementPolicy = bm.getBlockPlacementPolicy();
    cluster = bm.getDatanodeManager().getNetworkTopology();
    for (int i = 0; i < nodesPerRack * numRacks; i++) {
      cluster.add(dataNodes[i]);
    }

    setupDataNodeCapacity();
  }

  private static void updateHeartbeatWithUsage(DatanodeDescriptor dn,
      long capacity, long dfsUsed, long remaining, long blockPoolUsed,
      long dnCacheCapacity, long dnCacheUsed, int xceiverCount,
      int volFailures) {
    dn.getStorageInfos()[0].setUtilizationForTesting(
        capacity, dfsUsed, remaining, blockPoolUsed);
    dn.updateHeartbeat(
        BlockManagerTestUtil.getStorageReportsForDatanode(dn),
        dnCacheCapacity, dnCacheUsed, xceiverCount, volFailures, null);
  }

  private static void setupDataNodeCapacity() {
    for (int i = 0; i < nodesPerRack * numRacks; i++) {
      if ((i % 2) == 0) {
        // remaining 100%
        updateHeartbeatWithUsage(dataNodes[i], 2 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * blockSize,
          0L, 2 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * blockSize, 0L, 0L, 0L, 0, 0);
      } else {
        // remaining 50%
        updateHeartbeatWithUsage(dataNodes[i], 2 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * blockSize,
          HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * blockSize, HdfsServerConstants.MIN_BLOCKS_FOR_WRITE
              * blockSize, 0L, 0L, 0L, 0, 0);
      }
    }
  }

  /*
   * To verify that the BlockPlacementPolicy can be replaced by AvailableSpaceBlockPlacementPolicy via
   * changing the configuration.
   */
  @Test
  public void testPolicyReplacement() {
    assertTrue((placementPolicy instanceof AvailableSpaceBlockPlacementPolicy));
  }

  /*
   * Call choose target many times and verify that nodes with more remaining percent will be chosen
   * with high possibility.
   */
  @Test
  public void testChooseTarget() {
    int total = 0;
    int moreRemainingNode = 0;
    for (int i = 0; i < chooseTimes; i++) {
      DatanodeStorageInfo[] targets =
          namenode
              .getNamesystem()
              .getBlockManager()
              .getBlockPlacementPolicy()
              .chooseTarget(file, replica, null, new ArrayList<DatanodeStorageInfo>(), false, null,
                blockSize, TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY, null);

      assertTrue(targets.length == replica);
      for (int j = 0; j < replica; j++) {
        total++;
        if (targets[j].getDatanodeDescriptor().getRemainingPercent() > 60) {
          moreRemainingNode++;
        }
      }
    }
    assertTrue(total == replica * chooseTimes);
    double possibility = 1.0 * moreRemainingNode / total;
    assertTrue(possibility > 0.52);
    assertTrue(possibility < 0.55);
  }

  @Test
  public void testChooseDataNode() {
    Collection<Node> allNodes = new ArrayList<>(dataNodes.length);
    Collections.addAll(allNodes, dataNodes);
    if (placementPolicy instanceof AvailableSpaceBlockPlacementPolicy) {
      // exclude all datanodes when chooseDataNode, no NPE should be thrown
      ((AvailableSpaceBlockPlacementPolicy) placementPolicy)
          .chooseDataNode("~", allNodes);
    }
  }

  @Test
  public void testChooseSimilarDataNode() {
    DatanodeDescriptor[] tolerateDataNodes;
    DatanodeStorageInfo[] tolerateStorages;
    int capacity  = 3;
    Collection<Node> allTolerateNodes = new ArrayList<>(capacity);
    String[] ownerRackOfTolerateNodes = new String[capacity];
    for (int i = 0; i < capacity; i++) {
      ownerRackOfTolerateNodes[i] = "rack"+i;
    }
    tolerateStorages = DFSTestUtil.createDatanodeStorageInfos(ownerRackOfTolerateNodes);
    tolerateDataNodes = DFSTestUtil.toDatanodeDescriptor(tolerateStorages);

    Collections.addAll(allTolerateNodes, tolerateDataNodes);
    final BlockManager bm = namenode.getNamesystem().getBlockManager();
    AvailableSpaceBlockPlacementPolicy toleratePlacementPolicy =
            (AvailableSpaceBlockPlacementPolicy)bm.getBlockPlacementPolicy();

    updateHeartbeatWithUsage(tolerateDataNodes[0],
            20 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * blockSize,
            1 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * blockSize,
            HdfsServerConstants.MIN_BLOCKS_FOR_WRITE
                    * blockSize, 0L, 0L, 0L, 0, 0);

    updateHeartbeatWithUsage(tolerateDataNodes[1],
            11 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * blockSize,
            1 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * blockSize,
            HdfsServerConstants.MIN_BLOCKS_FOR_WRITE
                    * blockSize, 0L, 0L, 0L, 0, 0);

    updateHeartbeatWithUsage(tolerateDataNodes[2],
            10 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * blockSize,
            1 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * blockSize,
            HdfsServerConstants.MIN_BLOCKS_FOR_WRITE
                    * blockSize, 0L, 0L, 0L, 0, 0);

    assertTrue(toleratePlacementPolicy.compareDataNode(tolerateDataNodes[0],
            tolerateDataNodes[1], false) == 0);
    assertTrue(toleratePlacementPolicy.compareDataNode(tolerateDataNodes[1],
            tolerateDataNodes[0], false) == 0);
    assertTrue(toleratePlacementPolicy.compareDataNode(tolerateDataNodes[0],
            tolerateDataNodes[2], false) == -1);
    assertTrue(toleratePlacementPolicy.compareDataNode(tolerateDataNodes[2],
            tolerateDataNodes[0], false) == 1);
  }


  @Test
  public void testCompareDataNode() {
    DatanodeDescriptor[] tolerateDataNodes;
    DatanodeStorageInfo[] tolerateStorages;
    int capacity  = 5;
    Collection<Node> allTolerateNodes = new ArrayList<>(capacity);
    String[] ownerRackOfTolerateNodes = new String[capacity];
    for (int i = 0; i < capacity; i++) {
      ownerRackOfTolerateNodes[i] = "rack"+i;
    }
    tolerateStorages = DFSTestUtil.createDatanodeStorageInfos(ownerRackOfTolerateNodes);
    tolerateDataNodes = DFSTestUtil.toDatanodeDescriptor(tolerateStorages);

    Collections.addAll(allTolerateNodes, tolerateDataNodes);
    final BlockManager bm = namenode.getNamesystem().getBlockManager();
    AvailableSpaceBlockPlacementPolicy toleratePlacementPolicy =
        (AvailableSpaceBlockPlacementPolicy)bm.getBlockPlacementPolicy();

    //96.6%
    updateHeartbeatWithUsage(tolerateDataNodes[0],
        30 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * blockSize,
        29 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * blockSize,
        HdfsServerConstants.MIN_BLOCKS_FOR_WRITE
        * blockSize, 0L, 0L, 0L, 0, 0);

    //93.3%
    updateHeartbeatWithUsage(tolerateDataNodes[1],
        30 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * blockSize,
        28 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * blockSize,
        HdfsServerConstants.MIN_BLOCKS_FOR_WRITE
        * blockSize, 0L, 0L, 0L, 0, 0);

    //90.0%
    updateHeartbeatWithUsage(tolerateDataNodes[2],
        30 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * blockSize,
        27 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * blockSize,
        HdfsServerConstants.MIN_BLOCKS_FOR_WRITE
        * blockSize, 0L, 0L, 0L, 0, 0);

    //86.6%
    updateHeartbeatWithUsage(tolerateDataNodes[3],
        30 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * blockSize,
        26 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * blockSize,
        HdfsServerConstants.MIN_BLOCKS_FOR_WRITE
        * blockSize, 0L, 0L, 0L, 0, 0);

    //83.3%
    updateHeartbeatWithUsage(tolerateDataNodes[4],
        30 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * blockSize,
        25 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * blockSize,
        HdfsServerConstants.MIN_BLOCKS_FOR_WRITE
        * blockSize, 0L, 0L, 0L, 0, 0);

    assertTrue(toleratePlacementPolicy.compareDataNode(tolerateDataNodes[0],
        tolerateDataNodes[1], false) == 1);
    assertTrue(toleratePlacementPolicy.compareDataNode(tolerateDataNodes[1],
        tolerateDataNodes[0], false) == -1);
    assertTrue(toleratePlacementPolicy.compareDataNode(tolerateDataNodes[1],
        tolerateDataNodes[2], false) == 1);
    assertTrue(toleratePlacementPolicy.compareDataNode(tolerateDataNodes[2],
        tolerateDataNodes[1], false) == -1);
    assertTrue(toleratePlacementPolicy.compareDataNode(tolerateDataNodes[2],
        tolerateDataNodes[3], false) == 0);
    assertTrue(toleratePlacementPolicy.compareDataNode(tolerateDataNodes[3],
        tolerateDataNodes[2], false) == 0);
    assertTrue(toleratePlacementPolicy.compareDataNode(tolerateDataNodes[2],
        tolerateDataNodes[4], false) == 1);
    assertTrue(toleratePlacementPolicy.compareDataNode(tolerateDataNodes[4],
        tolerateDataNodes[2], false) == -1);
  }

  @AfterClass
  public static void teardownCluster() {
    if (namenode != null) {
      namenode.stop();
    }
  }
}
