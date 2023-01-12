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
package org.apache.hadoop.hdfs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.hadoop.hdfs.ReadStripedFileWithDecodingHelper.BLOCK_SIZE;
import static org.apache.hadoop.hdfs.ReadStripedFileWithDecodingHelper.CELL_SIZE;
import static org.apache.hadoop.hdfs.ReadStripedFileWithDecodingHelper.NUM_DATA_UNITS;
import static org.apache.hadoop.hdfs.ReadStripedFileWithDecodingHelper.NUM_PARITY_UNITS;
import static org.apache.hadoop.hdfs.ReadStripedFileWithDecodingHelper.findFirstDataNode;
import static org.apache.hadoop.hdfs.ReadStripedFileWithDecodingHelper.initializeCluster;
import static org.apache.hadoop.hdfs.ReadStripedFileWithDecodingHelper.tearDownCluster;

public class TestReadStripedFileWithDecoding {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestReadStripedFileWithDecoding.class);

  private MiniDFSCluster cluster;
  private DistributedFileSystem dfs;

  @Rule
  public Timeout globalTimeout = new Timeout(300000);

  @Before
  public void setup() throws IOException {
    cluster = initializeCluster();
    dfs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws IOException {
    tearDownCluster(cluster);
  }

  /**
   * After reading a corrupted block, make sure the client can correctly report
   * the corruption to the NameNode.
   */
  @Test
  public void testReportBadBlock() throws IOException {
    // create file
    final Path file = new Path("/corrupted");
    final int length = 10; // length of "corruption"
    final byte[] bytes = StripedFileTestUtil.generateBytes(length);
    DFSTestUtil.writeFile(dfs, file, bytes);

    // corrupt the first data block
    int dnIndex = ReadStripedFileWithDecodingHelper.findFirstDataNode(
        cluster, dfs, file, CELL_SIZE * NUM_DATA_UNITS);
    Assert.assertNotEquals(-1, dnIndex);
    LocatedStripedBlock slb = (LocatedStripedBlock) dfs.getClient()
        .getLocatedBlocks(file.toString(), 0, CELL_SIZE * NUM_DATA_UNITS)
        .get(0);
    final LocatedBlock[] blks = StripedBlockUtil.parseStripedBlockGroup(slb,
        CELL_SIZE, NUM_DATA_UNITS, NUM_PARITY_UNITS);
    // find the first block file
    File storageDir = cluster.getInstanceStorageDir(dnIndex, 0);
    File blkFile = MiniDFSCluster.getBlockFile(storageDir, blks[0].getBlock());
    Assert.assertTrue("Block file does not exist", blkFile.exists());
    // corrupt the block file
    LOG.info("Deliberately corrupting file " + blkFile.getName());
    try (FileOutputStream out = new FileOutputStream(blkFile)) {
      out.write("corruption".getBytes());
    }

    // disable the heartbeat from DN so that the corrupted block record is kept
    // in NameNode
    for (DataNode dn : cluster.getDataNodes()) {
      DataNodeTestUtils.setHeartbeatsDisabledForTests(dn, true);
    }

    try {
      // do stateful read
      StripedFileTestUtil.verifyStatefulRead(dfs, file, length, bytes,
          ByteBuffer.allocate(1024));

      // check whether the corruption has been reported to the NameNode
      final FSNamesystem ns = cluster.getNamesystem();
      final BlockManager bm = ns.getBlockManager();
      BlockInfo blockInfo = (ns.getFSDirectory().getINode4Write(file.toString())
          .asFile().getBlocks())[0];
      Assert.assertEquals(1, bm.getCorruptReplicas(blockInfo).size());
    } finally {
      for (DataNode dn : cluster.getDataNodes()) {
        DataNodeTestUtils.setHeartbeatsDisabledForTests(dn, false);
      }
    }
  }

  @Test
  public void testInvalidateBlock() throws IOException, InterruptedException {
    final Path file = new Path("/invalidate");
    final int length = 10;
    final byte[] bytes = StripedFileTestUtil.generateBytes(length);
    DFSTestUtil.writeFile(dfs, file, bytes);

    int dnIndex = findFirstDataNode(cluster, dfs, file,
        CELL_SIZE * NUM_DATA_UNITS);
    Assert.assertNotEquals(-1, dnIndex);
    LocatedStripedBlock slb = (LocatedStripedBlock) dfs.getClient()
        .getLocatedBlocks(file.toString(), 0, CELL_SIZE * NUM_DATA_UNITS)
        .get(0);
    final LocatedBlock[] blks = StripedBlockUtil.parseStripedBlockGroup(slb,
        CELL_SIZE, NUM_DATA_UNITS, NUM_PARITY_UNITS);
    final Block b = blks[0].getBlock().getLocalBlock();

    DataNode dn = cluster.getDataNodes().get(dnIndex);
    // disable the heartbeat from DN so that the invalidated block record is
    // kept in NameNode until heartbeat expires and NN mark the dn as dead
    DataNodeTestUtils.setHeartbeatsDisabledForTests(dn, true);

    try {
      // delete the file
      dfs.delete(file, true);
      BlockManagerTestUtil.waitForMarkedDeleteQueueIsEmpty(
          cluster.getNamesystem().getBlockManager());
      // check the block is added to invalidateBlocks
      final FSNamesystem fsn = cluster.getNamesystem();
      final BlockManager bm = fsn.getBlockManager();
      DatanodeDescriptor dnd =
          NameNodeAdapter.getDatanode(fsn, dn.getDatanodeId());
      Assert.assertTrue(bm.containsInvalidateBlock(
          blks[0].getLocations()[0], b) || dnd.containsInvalidateBlock(b));
    } finally {
      DataNodeTestUtils.setHeartbeatsDisabledForTests(dn, false);
    }
  }

  @Test
  public void testMoreThanOneCorruptedBlock() throws IOException {
    final Path file = new Path("/corrupted");
    final int length = BLOCK_SIZE * NUM_DATA_UNITS;
    final byte[] bytes = StripedFileTestUtil.generateBytes(length);
    DFSTestUtil.writeFile(dfs, file, bytes);

    // read the file with more than one corrupted data block
    byte[] buffer = new byte[length + 100];
    for (int count = 2; count < NUM_PARITY_UNITS; ++count) {
      ReadStripedFileWithDecodingHelper.corruptBlocks(cluster, dfs, file, count, 0,
          false);
      StripedFileTestUtil.verifyStatefulRead(dfs, file, length, bytes,
          buffer);
    }
  }

  @Test
  public void testReadWithCorruptedDataBlockAndParityBlock() throws IOException {
    final Path file = new Path("/corruptedDataBlockAndParityBlock");
    final int length = BLOCK_SIZE * NUM_DATA_UNITS;
    final byte[] bytes = StripedFileTestUtil.generateBytes(length);
    DFSTestUtil.writeFile(dfs, file, bytes);

    // set one dataBlock and the first parityBlock corrupted
    int dataBlkDelNum = 1;
    int parityBlkDelNum = 1;
    int recoverBlkNum = dataBlkDelNum + parityBlkDelNum;
    int[] dataBlkIndices = {0};
    int[] parityBlkIndices = {6};

    LocatedBlocks locatedBlocks = ReadStripedFileWithDecodingHelper.getLocatedBlocks(dfs, file);
    LocatedStripedBlock lastBlock =
        (LocatedStripedBlock)locatedBlocks.getLastLocatedBlock();

    int[] delBlkIndices = new int[recoverBlkNum];
    System.arraycopy(dataBlkIndices, 0,
        delBlkIndices, 0, dataBlkIndices.length);
    System.arraycopy(parityBlkIndices, 0,
        delBlkIndices, dataBlkIndices.length, parityBlkIndices.length);
    ExtendedBlock[] delBlocks = new ExtendedBlock[recoverBlkNum];
    for (int i = 0; i < recoverBlkNum; i++) {
      delBlocks[i] = StripedBlockUtil
          .constructInternalBlock(lastBlock.getBlock(),
              CELL_SIZE, NUM_DATA_UNITS, delBlkIndices[i]);
      cluster.corruptBlockOnDataNodes(delBlocks[i]);
    }

    byte[] buffer = new byte[length + 100];
    StripedFileTestUtil.verifyStatefulRead(dfs, file, length, bytes,
        buffer);
  }
}
