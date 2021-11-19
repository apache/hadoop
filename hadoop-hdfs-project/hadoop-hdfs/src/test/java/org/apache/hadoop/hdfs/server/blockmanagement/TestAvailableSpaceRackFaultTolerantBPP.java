/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.blockmanagement;

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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests AvailableSpaceRackFaultTolerant block placement policy.
 */
public class TestAvailableSpaceRackFaultTolerantBPP {
  private final static int NUM_RACKS = 4;
  private final static int NODES_PER_RACK = 5;
  private final static int BLOCK_SIZE = 1024;
  private final static int CHOOSE_TIMES = 10000;
  private final static String FILE = "/tobers/test";
  private final static int REPLICA = 3;

  private static DatanodeStorageInfo[] storages;
  private static DatanodeDescriptor[] dataNodes;
  private static Configuration conf;
  private static NameNode namenode;
  private static BlockPlacementPolicy placementPolicy;
  private static NetworkTopology cluster;

  @BeforeClass
  public static void setupCluster() throws Exception {
    conf = new HdfsConfiguration();
    conf.setFloat(
        DFSConfigKeys.DFS_NAMENODE_AVAILABLE_SPACE_RACK_FAULT_TOLERANT_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY,
        0.6f);
    String[] racks = new String[NUM_RACKS];
    for (int i = 0; i < NUM_RACKS; i++) {
      racks[i] = "/rack" + i;
    }

    String[] owerRackOfNodes = new String[NUM_RACKS * NODES_PER_RACK];
    for (int i = 0; i < NODES_PER_RACK; i++) {
      for (int j = 0; j < NUM_RACKS; j++) {
        owerRackOfNodes[i * NUM_RACKS + j] = racks[j];
      }
    }

    storages = DFSTestUtil.createDatanodeStorageInfos(owerRackOfNodes);
    dataNodes = DFSTestUtil.toDatanodeDescriptor(storages);

    FileSystem.setDefaultUri(conf, "hdfs://localhost:0");
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    File baseDir = PathUtils
        .getTestDir(AvailableSpaceRackFaultTolerantBlockPlacementPolicy.class);
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        new File(baseDir, "name").getPath());
    conf.set(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
        AvailableSpaceRackFaultTolerantBlockPlacementPolicy.class.getName());

    DFSTestUtil.formatNameNode(conf);
    namenode = new NameNode(conf);

    final BlockManager bm = namenode.getNamesystem().getBlockManager();
    placementPolicy = bm.getBlockPlacementPolicy();
    cluster = bm.getDatanodeManager().getNetworkTopology();
    for (int i = 0; i < NODES_PER_RACK * NUM_RACKS; i++) {
      cluster.add(dataNodes[i]);
    }

    setupDataNodeCapacity();
  }

  private static void updateHeartbeatWithUsage(DatanodeDescriptor dn,
      long capacity, long dfsUsed, long remaining, long blockPoolUsed,
      long dnCacheCapacity, long dnCacheUsed, int xceiverCount,
      int volFailures) {
    dn.getStorageInfos()[0]
        .setUtilizationForTesting(capacity, dfsUsed, remaining, blockPoolUsed);
    dn.updateHeartbeat(BlockManagerTestUtil.getStorageReportsForDatanode(dn),
        dnCacheCapacity, dnCacheUsed, xceiverCount, volFailures, null);
  }

  private static void setupDataNodeCapacity() {
    for (int i = 0; i < NODES_PER_RACK * NUM_RACKS; i++) {
      if ((i % 2) == 0) {
        // remaining 100%
        updateHeartbeatWithUsage(dataNodes[i],
            2 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE, 0L,
            2 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE, 0L, 0L,
            0L, 0, 0);
      } else {
        // remaining 50%
        updateHeartbeatWithUsage(dataNodes[i],
            2 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE,
            HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE,
            HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE, 0L, 0L, 0L,
            0, 0);
      }
    }
  }

  /*
   * To verify that the BlockPlacementPolicy can be replaced by
   * AvailableSpaceRackFaultTolerantBlockPlacementPolicy via
   * changing the configuration.
   */
  @Test
  public void testPolicyReplacement() {
    Assert.assertTrue(
        (placementPolicy instanceof
            AvailableSpaceRackFaultTolerantBlockPlacementPolicy));
  }

  /*
   * Call choose target many times and verify that nodes with more remaining
   * percent will be chosen with high possibility.
   */
  @Test
  public void testChooseTarget() {
    int total = 0;
    int moreRemainingNode = 0;
    for (int i = 0; i < CHOOSE_TIMES; i++) {
      DatanodeStorageInfo[] targets =
          namenode.getNamesystem().getBlockManager().getBlockPlacementPolicy()
              .chooseTarget(FILE, REPLICA, null,
                  new ArrayList<DatanodeStorageInfo>(), false, null, BLOCK_SIZE,
                  TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY, null);

      Assert.assertTrue(targets.length == REPLICA);
      for (int j = 0; j < REPLICA; j++) {
        total++;
        if (targets[j].getDatanodeDescriptor().getRemainingPercent() > 60) {
          moreRemainingNode++;
        }
      }
    }
    Assert.assertTrue(total == REPLICA * CHOOSE_TIMES);
    double possibility = 1.0 * moreRemainingNode / total;
    Assert.assertTrue(possibility > 0.52);
    Assert.assertTrue(possibility < 0.55);
  }

  @Test
  public void testChooseDataNode() {
    try {
      Collection<Node> allNodes = new ArrayList<>(dataNodes.length);
      Collections.addAll(allNodes, dataNodes);
      if (placementPolicy instanceof AvailableSpaceBlockPlacementPolicy) {
        // exclude all datanodes when chooseDataNode, no NPE should be thrown
        ((AvailableSpaceRackFaultTolerantBlockPlacementPolicy) placementPolicy)
            .chooseDataNode("~", allNodes);
      }
    } catch (NullPointerException npe) {
      Assert.fail("NPE should not be thrown");
    }
  }

  /**
   * Test if the nodes are all spread across all racks.
   */
  @Test
  public void testMaxRackAllocation() {
    DatanodeStorageInfo[] targets =
        namenode.getNamesystem().getBlockManager().getBlockPlacementPolicy()
            .chooseTarget(FILE, REPLICA, null,
                new ArrayList<DatanodeStorageInfo>(), false, null, BLOCK_SIZE,
                TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY, null);
    HashSet<String> racks = new HashSet<String>();
    for (int i = 0; i < targets.length; i++) {
      racks.add(targets[i].getDatanodeDescriptor().getNetworkLocation());

    }
    assertEquals(REPLICA, racks.size());
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
    AvailableSpaceRackFaultTolerantBlockPlacementPolicy toleratePlacementPolicy =
            (AvailableSpaceRackFaultTolerantBlockPlacementPolicy)bm.getBlockPlacementPolicy();

    updateHeartbeatWithUsage(tolerateDataNodes[0],
            20 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE,
            1 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE,
            HdfsServerConstants.MIN_BLOCKS_FOR_WRITE
                    * BLOCK_SIZE, 0L, 0L, 0L, 0, 0);

    updateHeartbeatWithUsage(tolerateDataNodes[1],
            11 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE,
            1 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE,
            HdfsServerConstants.MIN_BLOCKS_FOR_WRITE
                    * BLOCK_SIZE, 0L, 0L, 0L, 0, 0);

    updateHeartbeatWithUsage(tolerateDataNodes[2],
            10 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE,
            1 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE,
            HdfsServerConstants.MIN_BLOCKS_FOR_WRITE
                    * BLOCK_SIZE, 0L, 0L, 0L, 0, 0);

    assertTrue(toleratePlacementPolicy.compareDataNode(tolerateDataNodes[0],
            tolerateDataNodes[1]) == 0);
    assertTrue(toleratePlacementPolicy.compareDataNode(tolerateDataNodes[1],
            tolerateDataNodes[0]) == 0);
    assertTrue(toleratePlacementPolicy.compareDataNode(tolerateDataNodes[0],
            tolerateDataNodes[2]) == -1);
    assertTrue(toleratePlacementPolicy.compareDataNode(tolerateDataNodes[2],
            tolerateDataNodes[0]) == 1);
  }

  @AfterClass
  public static void teardownCluster() {
    if (namenode != null) {
      namenode.stop();
    }
  }
}
