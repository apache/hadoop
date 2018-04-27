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

import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.test.Whitebox;
import org.apache.hadoop.util.VersionInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test if FSNamesystem handles heartbeat right
 */
public class TestComputeInvalidateWork {

  private Configuration conf;
  private final int NUM_OF_DATANODES = 3;
  private MiniDFSCluster cluster;
  private FSNamesystem namesystem;
  private BlockManager bm;
  private DatanodeDescriptor[] nodes;
  private ErasureCodingPolicy ecPolicy;
  private DistributedFileSystem fs;
  private Path ecFile;
  private int totalBlockGroups, blockGroupSize, stripesPerBlock, cellSize;
  private LocatedStripedBlock locatedStripedBlock;

  @Before
  public void setup() throws Exception {
    ecPolicy = SystemErasureCodingPolicies.getByID(
        SystemErasureCodingPolicies.XOR_2_1_POLICY_ID);
    conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_OF_DATANODES)
        .build();
    cluster.waitActive();
    namesystem = cluster.getNamesystem();
    bm = namesystem.getBlockManager();
    nodes = bm.getDatanodeManager().getHeartbeatManager().getDatanodes();
    BlockManagerTestUtil.stopRedundancyThread(bm);
    assertEquals(nodes.length, NUM_OF_DATANODES);

    // Create a striped file
    Path ecDir = new Path("/ec");
    fs = cluster.getFileSystem();
    fs.enableErasureCodingPolicy(ecPolicy.getName());
    fs.mkdirs(ecDir);
    fs.getClient().setErasureCodingPolicy(ecDir.toString(), ecPolicy.getName());
    ecFile = new Path(ecDir, "ec-file");
    stripesPerBlock = 2;
    cellSize = ecPolicy.getCellSize();
    int blockSize = stripesPerBlock * cellSize;
    blockGroupSize =  ecPolicy.getNumDataUnits() * blockSize;
    totalBlockGroups = 4;
    DFSTestUtil.createStripedFile(cluster, ecFile, ecDir, totalBlockGroups,
        stripesPerBlock, false, ecPolicy);
    LocatedBlocks lbs = cluster.getFileSystem().getClient().
        getNamenode().getBlockLocations(
        ecFile.toString(), 0, blockGroupSize);
    assert lbs.get(0) instanceof LocatedStripedBlock;
    locatedStripedBlock = (LocatedStripedBlock)(lbs.get(0));
  }

  @After
  public void teardown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  private void verifyInvalidationWorkCounts(int blockInvalidateLimit) {
    assertEquals(blockInvalidateLimit * NUM_OF_DATANODES,
        bm.computeInvalidateWork(NUM_OF_DATANODES + 1));
    assertEquals(blockInvalidateLimit * NUM_OF_DATANODES,
        bm.computeInvalidateWork(NUM_OF_DATANODES));
    assertEquals(blockInvalidateLimit * (NUM_OF_DATANODES - 1),
        bm.computeInvalidateWork(NUM_OF_DATANODES - 1));
    int workCount = bm.computeInvalidateWork(1);
    if (workCount == 1) {
      assertEquals(blockInvalidateLimit + 1, bm.computeInvalidateWork(2));
    } else {
      assertEquals(workCount, blockInvalidateLimit);
      assertEquals(2, bm.computeInvalidateWork(2));
    }
  }

  /**
   * Test if {@link BlockManager#computeInvalidateWork(int)}
   * can schedule invalidate work correctly for the replicas.
   */
  @Test(timeout=120000)
  public void testComputeInvalidateReplicas() throws Exception {
    final int blockInvalidateLimit = bm.getDatanodeManager()
        .getBlockInvalidateLimit();
    namesystem.writeLock();
    try {
      for (int i=0; i<nodes.length; i++) {
        for(int j=0; j<3*blockInvalidateLimit+1; j++) {
          Block block = new Block(i*(blockInvalidateLimit+1)+j, 0,
              GenerationStamp.LAST_RESERVED_STAMP);
          bm.addToInvalidates(block, nodes[i]);
        }
      }
      verifyInvalidationWorkCounts(blockInvalidateLimit);
    } finally {
      namesystem.writeUnlock();
    }
  }

  /**
   * Test if {@link BlockManager#computeInvalidateWork(int)}
   * can schedule invalidate work correctly for the striped block groups.
   */
  @Test(timeout=120000)
  public void testComputeInvalidateStripedBlockGroups() throws Exception {
    final int blockInvalidateLimit =
        bm.getDatanodeManager().getBlockInvalidateLimit();
    namesystem.writeLock();
    try {
      int nodeCount = ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits();
      for (int i = 0; i < nodeCount; i++) {
        for(int j = 0; j < 3 * blockInvalidateLimit + 1; j++) {
          Block blk = new Block(locatedStripedBlock.getBlock().getBlockId() +
              (i * 10 + j), stripesPerBlock * cellSize,
              locatedStripedBlock.getBlock().getGenerationStamp());
          bm.addToInvalidates(blk, nodes[i]);
        }
      }
      verifyInvalidationWorkCounts(blockInvalidateLimit);
    } finally {
      namesystem.writeUnlock();
    }
  }

  /**
   * Test if {@link BlockManager#computeInvalidateWork(int)}
   * can schedule invalidate work correctly for both replicas and striped
   * block groups, combined.
   */
  @Test(timeout=120000)
  public void testComputeInvalidate() throws Exception {
    final int blockInvalidateLimit =
        bm.getDatanodeManager().getBlockInvalidateLimit();
    final Random random = new Random(System.currentTimeMillis());
    namesystem.writeLock();
    try {
      int nodeCount = ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits();
      for (int i = 0; i < nodeCount; i++) {
        for(int j = 0; j < 3 * blockInvalidateLimit + 1; j++) {
          if (random.nextBoolean()) {
            Block stripedBlock = new Block(
                locatedStripedBlock.getBlock().getBlockId() + (i * 10 + j),
                stripesPerBlock * cellSize,
                locatedStripedBlock.getBlock().getGenerationStamp());
            bm.addToInvalidates(stripedBlock, nodes[i]);
          } else {
            Block replica = new Block(i * (blockInvalidateLimit + 1) + j, 0,
                GenerationStamp.LAST_RESERVED_STAMP);
            bm.addToInvalidates(replica, nodes[i]);
          }
        }
      }
      verifyInvalidationWorkCounts(blockInvalidateLimit);
    } finally {
      namesystem.writeUnlock();
    }
  }

  /**
   * Reformatted DataNodes will replace the original UUID in the
   * {@link DatanodeManager#datanodeMap}. This tests if block
   * invalidation work on the original DataNode can be skipped.
   */
  @Test(timeout=120000)
  public void testDatanodeReformat() throws Exception {
    namesystem.writeLock();
    try {
      // Change the datanode UUID to emulate a reformat
      String poolId = cluster.getNamesystem().getBlockPoolId();
      DatanodeRegistration dnr = cluster.getDataNode(nodes[0].getIpcPort())
                                        .getDNRegistrationForBP(poolId);
      dnr = new DatanodeRegistration(UUID.randomUUID().toString(), dnr);
      cluster.stopDataNode(nodes[0].getXferAddr());

      Block block = new Block(0, 0, GenerationStamp.LAST_RESERVED_STAMP);
      bm.addToInvalidates(block, nodes[0]);
      Block stripedBlock = new Block(
          locatedStripedBlock.getBlock().getBlockId() + 100,
          stripesPerBlock * cellSize,
          locatedStripedBlock.getBlock().getGenerationStamp());
      bm.addToInvalidates(stripedBlock, nodes[0]);
      bm.getDatanodeManager().registerDatanode(dnr);

      // Since UUID has changed, the invalidation work should be skipped
      assertEquals(0, bm.computeInvalidateWork(1));
      assertEquals(0, bm.getPendingDeletionBlocksCount());
    } finally {
      namesystem.writeUnlock();
    }
  }

  @Test(timeout=12000)
  public void testDatanodeReRegistration() throws Exception {
    // Create a test file
    final DistributedFileSystem dfs = cluster.getFileSystem();
    final Path path = new Path("/testRR");
    // Create a file and shutdown the DNs, which populates InvalidateBlocks
    short totalReplicas = NUM_OF_DATANODES;
    DFSTestUtil.createFile(dfs, path, dfs.getDefaultBlockSize(),
        totalReplicas, 0xED0ED0);
    DFSTestUtil.waitForReplication(dfs, path, (short) NUM_OF_DATANODES, 12000);
    for (DataNode dn : cluster.getDataNodes()) {
      dn.shutdown();
    }
    dfs.delete(path, false);
    dfs.delete(ecFile, false);
    namesystem.writeLock();
    InvalidateBlocks invalidateBlocks;
    int totalStripedDataBlocks = totalBlockGroups * (ecPolicy.getNumDataUnits()
        + ecPolicy.getNumParityUnits());
    int expected = totalReplicas + totalStripedDataBlocks;
    try {
      invalidateBlocks = (InvalidateBlocks) Whitebox
          .getInternalState(cluster.getNamesystem().getBlockManager(),
              "invalidateBlocks");
      assertEquals("Invalidate blocks should include both Replicas and " +
          "Striped BlockGroups!",
          (long) expected, invalidateBlocks.numBlocks());
      assertEquals("Unexpected invalidate count for replicas!",
          totalReplicas, invalidateBlocks.getBlocks());
      assertEquals("Unexpected invalidate count for striped block groups!",
          totalStripedDataBlocks, invalidateBlocks.getECBlocks());
    } finally {
      namesystem.writeUnlock();
    }
    // Re-register each DN and see that it wipes the invalidation work
    int totalBlockGroupsPerDataNode = totalBlockGroups;
    int totalReplicasPerDataNode = totalReplicas / NUM_OF_DATANODES;
    for (DataNode dn : cluster.getDataNodes()) {
      DatanodeID did = dn.getDatanodeId();
      DatanodeRegistration reg = new DatanodeRegistration(
          new DatanodeID(UUID.randomUUID().toString(), did),
          new StorageInfo(HdfsServerConstants.NodeType.DATA_NODE),
          new ExportedBlockKeys(),
          VersionInfo.getVersion());
      namesystem.writeLock();
      try {
        bm.getDatanodeManager().registerDatanode(reg);
        expected -= (totalReplicasPerDataNode + totalBlockGroupsPerDataNode);
        assertEquals("Expected number of invalidate blocks to decrease",
            (long) expected, invalidateBlocks.numBlocks());
      } finally {
          namesystem.writeUnlock();
      }
    }
  }
}
