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
import org.apache.hadoop.test.PathUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCE_LOCAL_NODE_KEY;

/**
 * Tests AvailableSpaceBlockPlacementPolicy with balance local.
 */
public class TestAvailableSpaceBPPBalanceLocal {
  private final static int NUM_RACKS = 2;
  private final static int NODES_PER_RACK = 3;
  final static int BLOCK_SIZE = 1024;
  final static int CHOOSE_TIMES = 10000;
  final static String FILE = "/tobers/test";

  private static DatanodeStorageInfo[] storages;
  private static DatanodeDescriptor[] dataNodes;
  private static Configuration conf;
  private static NameNode namenode;
  private static NetworkTopology cluster;

  @BeforeClass
  public static void setupCluster() throws Exception {
    conf = new HdfsConfiguration();
    conf.setFloat(
        DFSConfigKeys.DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY,
        0.6f);
    conf.setBoolean(
        DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCE_LOCAL_NODE_KEY,
        true);
    String[] racks = new String[NUM_RACKS];
    for (int i = 0; i < NUM_RACKS; i++) {
      racks[i] = "/rack" + i;
    }

    String[] ownerRackOfNodes = new String[NUM_RACKS * NODES_PER_RACK];
    for (int i = 0; i < NUM_RACKS; i++) {
      for (int j = 0; j < NODES_PER_RACK; j++) {
        ownerRackOfNodes[i * NODES_PER_RACK + j] = racks[i];
      }
    }

    storages = DFSTestUtil.createDatanodeStorageInfos(ownerRackOfNodes);
    dataNodes = DFSTestUtil.toDatanodeDescriptor(storages);

    FileSystem.setDefaultUri(conf, "hdfs://localhost:0");
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    File baseDir =
        PathUtils.getTestDir(AvailableSpaceBlockPlacementPolicy.class);
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        new File(baseDir, "name").getPath());
    conf.set(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
        AvailableSpaceBlockPlacementPolicy.class.getName());

    DFSTestUtil.formatNameNode(conf);
    namenode = new NameNode(conf);

    final BlockManager bm = namenode.getNamesystem().getBlockManager();
    cluster = bm.getDatanodeManager().getNetworkTopology();
    for (int i = 0; i < NODES_PER_RACK * NUM_RACKS; i++) {
      cluster.add(dataNodes[i]);
    }

    setupDataNodeCapacity();
  }

  protected static void updateHeartbeatWithUsage(DatanodeDescriptor dn,
      long capacity, long dfsUsed, long remaining, long blockPoolUsed,
      long dnCacheCapacity, long dnCacheUsed, int xceiverCount,
      int volFailures) {
    dn.getStorageInfos()[0]
        .setUtilizationForTesting(capacity, dfsUsed, remaining, blockPoolUsed);
    dn.updateHeartbeat(BlockManagerTestUtil.getStorageReportsForDatanode(dn),
        dnCacheCapacity, dnCacheUsed, xceiverCount, volFailures, null);
  }

  protected static void setupDataNodeCapacity() {
    for (int i = 0; i < NODES_PER_RACK * NUM_RACKS; i++) {
      if ((i % 2) == 0) {
        // remaining 100%
        updateHeartbeatWithUsage(dataNodes[i],
            4 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE, 0L,
            4 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE, 0L, 0L,
            0L, 0, 0);
      } else {
        // remaining 25%
        updateHeartbeatWithUsage(dataNodes[i],
            4 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE,
            3 * HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE,
            HdfsServerConstants.MIN_BLOCKS_FOR_WRITE * BLOCK_SIZE, 0L, 0L, 0L,
            0, 0);
      }
    }
  }

  @Test
  public void testChooseLocalNode() {
    // Choosing datanode with zero usage.
    DatanodeDescriptor localNode = dataNodes[0];
    for (int i = 0; i < CHOOSE_TIMES; i++) {
      DatanodeStorageInfo[] targets =
          namenode.getNamesystem().getBlockManager().getBlockPlacementPolicy()
              .chooseTarget(FILE, 1, localNode,
                  new ArrayList<DatanodeStorageInfo>(), false, null, BLOCK_SIZE,
                  TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY, null);
      Assert.assertEquals(1, targets.length);
      Assert.assertEquals(localNode, targets[0].getDatanodeDescriptor());
    }
  }

  @Test
  public void testChooseLocalNodeWithLocalNodeLoaded() {
    // Choosing datanode with 75 percent usage.
    DatanodeDescriptor localNode = dataNodes[1];
    int numLocalChosen = 0;
    for (int i = 0; i < CHOOSE_TIMES; i++) {
      DatanodeStorageInfo[] targets =
          namenode.getNamesystem().getBlockManager().getBlockPlacementPolicy()
              .chooseTarget(FILE, 1, localNode,
                  new ArrayList<DatanodeStorageInfo>(), false, null, BLOCK_SIZE,
                  TestBlockStoragePolicy.DEFAULT_STORAGE_POLICY, null);

      Assert.assertEquals(1, targets.length);
      if (localNode == targets[0].getDatanodeDescriptor()) {
        numLocalChosen++;
      }
    }
    Assert.assertTrue(numLocalChosen < (CHOOSE_TIMES - numLocalChosen));
  }
}