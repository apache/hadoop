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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.hadoop.hdfs.StripedFileTestUtil.blockSize;
import static org.apache.hadoop.hdfs.StripedFileTestUtil.numDNs;

public class TestReadStripedFileWithDecoding {
  static final Log LOG = LogFactory.getLog(TestReadStripedFileWithDecoding.class);

  static {
    ((Log4JLogger)LogFactory.getLog(BlockPlacementPolicy.class))
        .getLogger().setLevel(Level.ALL);
    GenericTestUtils.setLogLevel(BlockManager.LOG, Level.ALL);
    GenericTestUtils.setLogLevel(BlockManager.blockLog, Level.ALL);
    GenericTestUtils.setLogLevel(NameNode.stateChangeLog, Level.ALL);
  }

  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private static final short dataBlocks = StripedFileTestUtil.NUM_DATA_BLOCKS;
  private static final short parityBlocks = StripedFileTestUtil.NUM_PARITY_BLOCKS;
  private final int cellSize = StripedFileTestUtil.BLOCK_STRIPED_CELL_SIZE;
  private final int smallFileLength = blockSize * dataBlocks - 123;
  private final int largeFileLength = blockSize * dataBlocks + 123;
  private final int[] fileLengths = {smallFileLength, largeFileLength};
  private static final int[] dnFailureNums = getDnFailureNums();

  private static int[] getDnFailureNums() {
    int[] dnFailureNums = new int[parityBlocks];
    for (int i = 0; i < dnFailureNums.length; i++) {
      dnFailureNums[i] = i + 1;
    }
    return dnFailureNums;
  }

  @Rule
  public Timeout globalTimeout = new Timeout(300000);

  @Before
  public void setup() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 0);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_KEY, 0);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REPLICATION_CONSIDERLOAD_KEY, false);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDNs).build();
    cluster.getFileSystem().getClient().setErasureCodingPolicy("/", null);
    fs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * Shutdown tolerable number of Datanode before reading.
   * Verify the decoding works correctly.
   */
  @Test(timeout=300000)
  public void testReadWithDNFailure() throws Exception {
    for (int fileLength : fileLengths) {
      for (int dnFailureNum : dnFailureNums) {
        try {
          // setup a new cluster with no dead datanode
          setup();
          testReadWithDNFailure(fileLength, dnFailureNum);
        } catch (IOException ioe) {
          String fileType = fileLength < (blockSize * dataBlocks) ?
              "smallFile" : "largeFile";
          LOG.error("Failed to read file with DN failure:"
              + " fileType = "+ fileType
              + ", dnFailureNum = " + dnFailureNum);
        } finally {
          // tear down the cluster
          tearDown();
        }
      }
    }
  }

  /**
   * Corrupt tolerable number of block before reading.
   * Verify the decoding works correctly.
   */
  @Test(timeout=300000)
  public void testReadCorruptedData() throws IOException {
    for (int fileLength : fileLengths) {
      for (int dataDelNum = 1; dataDelNum <= parityBlocks; dataDelNum++) {
        for (int parityDelNum = 0; (dataDelNum + parityDelNum) <= parityBlocks;
             parityDelNum++) {
          String src = "/corrupted_" + dataDelNum + "_" + parityDelNum;
          testReadWithBlockCorrupted(src, fileLength,
              dataDelNum, parityDelNum, false);
        }
      }
    }
  }

  /**
   * Delete tolerable number of block before reading.
   * Verify the decoding works correctly.
   */
  @Test(timeout=300000)
  public void testReadCorruptedDataByDeleting() throws IOException {
    for (int fileLength : fileLengths) {
      for (int dataDelNum = 1; dataDelNum <= parityBlocks; dataDelNum++) {
        for (int parityDelNum = 0; (dataDelNum + parityDelNum) <= parityBlocks;
             parityDelNum++) {
          String src = "/deleted_" + dataDelNum + "_" + parityDelNum;
          testReadWithBlockCorrupted(src, fileLength,
              dataDelNum, parityDelNum, true);
        }
      }
    }
  }

  private int findFirstDataNode(Path file, long length) throws IOException {
    BlockLocation[] locs = fs.getFileBlockLocations(file, 0, length);
    String name = (locs[0].getNames())[0];
    int dnIndex = 0;
    for (DataNode dn : cluster.getDataNodes()) {
      int port = dn.getXferPort();
      if (name.contains(Integer.toString(port))) {
        return dnIndex;
      }
      dnIndex++;
    }
    return -1;
  }

  private void verifyRead(Path testPath, int length, byte[] expected)
      throws IOException {
    byte[] buffer = new byte[length + 100];
    StripedFileTestUtil.verifyLength(fs, testPath, length);
    StripedFileTestUtil.verifyPread(fs, testPath, length, expected, buffer);
    StripedFileTestUtil.verifyStatefulRead(fs, testPath, length, expected, buffer);
    StripedFileTestUtil.verifyStatefulRead(fs, testPath, length, expected,
        ByteBuffer.allocate(length + 100));
    StripedFileTestUtil.verifySeek(fs, testPath, length);
  }

  private void testReadWithDNFailure(int fileLength, int dnFailureNum)
      throws Exception {
    String fileType = fileLength < (blockSize * dataBlocks) ?
        "smallFile" : "largeFile";
    String src = "/dnFailure_" + dnFailureNum + "_" + fileType;
    LOG.info("testReadWithDNFailure: file = " + src
        + ", fileSize = " + fileLength
        + ", dnFailureNum = " + dnFailureNum);

    Path testPath = new Path(src);
    final byte[] bytes = StripedFileTestUtil.generateBytes(fileLength);
    DFSTestUtil.writeFile(fs, testPath, bytes);
    StripedFileTestUtil.waitBlockGroupsReported(fs, src);

    // shut down the DN that holds an internal data block
    BlockLocation[] locs = fs.getFileBlockLocations(testPath, cellSize * 5,
        cellSize);
    for (int failedDnIdx = 0; failedDnIdx < dnFailureNum; failedDnIdx++) {
      String name = (locs[0].getNames())[failedDnIdx];
      for (DataNode dn : cluster.getDataNodes()) {
        int port = dn.getXferPort();
        if (name.contains(Integer.toString(port))) {
          dn.shutdown();
        }
      }
    }

    // check file length, pread, stateful read and seek
    verifyRead(testPath, fileLength, bytes);
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
    DFSTestUtil.writeFile(fs, file, bytes);

    // corrupt the first data block
    int dnIndex = findFirstDataNode(file, cellSize * dataBlocks);
    Assert.assertNotEquals(-1, dnIndex);
    LocatedStripedBlock slb = (LocatedStripedBlock)fs.getClient()
        .getLocatedBlocks(file.toString(), 0, cellSize * dataBlocks).get(0);
    final LocatedBlock[] blks = StripedBlockUtil.parseStripedBlockGroup(slb,
        cellSize, dataBlocks, parityBlocks);
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
      StripedFileTestUtil.verifyStatefulRead(fs, file, length, bytes,
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
  public void testInvalidateBlock() throws IOException {
    final Path file = new Path("/invalidate");
    final int length = 10;
    final byte[] bytes = StripedFileTestUtil.generateBytes(length);
    DFSTestUtil.writeFile(fs, file, bytes);

    int dnIndex = findFirstDataNode(file, cellSize * dataBlocks);
    Assert.assertNotEquals(-1, dnIndex);
    LocatedStripedBlock slb = (LocatedStripedBlock)fs.getClient()
        .getLocatedBlocks(file.toString(), 0, cellSize * dataBlocks).get(0);
    final LocatedBlock[] blks = StripedBlockUtil.parseStripedBlockGroup(slb,
        cellSize, dataBlocks, parityBlocks);
    final Block b = blks[0].getBlock().getLocalBlock();

    DataNode dn = cluster.getDataNodes().get(dnIndex);
    // disable the heartbeat from DN so that the invalidated block record is kept
    // in NameNode until heartbeat expires and NN mark the dn as dead
    DataNodeTestUtils.setHeartbeatsDisabledForTests(dn, true);

    try {
      // delete the file
      fs.delete(file, true);
      // check the block is added to invalidateBlocks
      final FSNamesystem fsn = cluster.getNamesystem();
      final BlockManager bm = fsn.getBlockManager();
      DatanodeDescriptor dnd = NameNodeAdapter.getDatanode(fsn, dn.getDatanodeId());
      Assert.assertTrue(bm.containsInvalidateBlock(
          blks[0].getLocations()[0], b) || dnd.containsInvalidateBlock(b));
    } finally {
      DataNodeTestUtils.setHeartbeatsDisabledForTests(dn, false);
    }
  }

  /**
   * Test reading a file with some blocks(data blocks or parity blocks or both)
   * deleted or corrupted.
   * @param src file path
   * @param fileLength file length
   * @param dataBlkDelNum the deleted or corrupted number of data blocks.
   * @param parityBlkDelNum the deleted or corrupted number of parity blocks.
   * @param deleteBlockFile whether block file is deleted or corrupted.
   *                        true is to delete the block file.
   *                        false is to corrupt the content of the block file.
   * @throws IOException
   */
  private void testReadWithBlockCorrupted(String src, int fileLength,
      int dataBlkDelNum, int parityBlkDelNum, boolean deleteBlockFile)
      throws IOException {
    LOG.info("testReadWithBlockCorrupted: file = " + src
        + ", dataBlkDelNum = " + dataBlkDelNum
        + ", parityBlkDelNum = " + parityBlkDelNum
        + ", deleteBlockFile? " + deleteBlockFile);
    int recoverBlkNum = dataBlkDelNum + parityBlkDelNum;
    Assert.assertTrue("dataBlkDelNum and parityBlkDelNum should be positive",
        dataBlkDelNum >= 0 && parityBlkDelNum >= 0);
    Assert.assertTrue("The sum of dataBlkDelNum and parityBlkDelNum " +
        "should be between 1 ~ " + parityBlocks, recoverBlkNum <= parityBlocks);

    // write a file with the length of writeLen
    Path srcPath = new Path(src);
    final byte[] bytes = StripedFileTestUtil.generateBytes(fileLength);
    DFSTestUtil.writeFile(fs, srcPath, bytes);

    // delete or corrupt some blocks
    corruptBlocks(srcPath, dataBlkDelNum, parityBlkDelNum, deleteBlockFile);

    // check the file can be read after some blocks were deleted
    verifyRead(srcPath, fileLength, bytes);
  }

  private void corruptBlocks(Path srcPath, int dataBlkDelNum,
      int parityBlkDelNum, boolean deleteBlockFile) throws IOException {
    int recoverBlkNum = dataBlkDelNum + parityBlkDelNum;

    LocatedBlocks locatedBlocks = getLocatedBlocks(srcPath);
    LocatedStripedBlock lastBlock =
        (LocatedStripedBlock)locatedBlocks.getLastLocatedBlock();

    int[] delDataBlkIndices = StripedFileTestUtil.randomArray(0, dataBlocks,
        dataBlkDelNum);
    Assert.assertNotNull(delDataBlkIndices);
    int[] delParityBlkIndices = StripedFileTestUtil.randomArray(dataBlocks,
        dataBlocks + parityBlocks, parityBlkDelNum);
    Assert.assertNotNull(delParityBlkIndices);

    int[] delBlkIndices = new int[recoverBlkNum];
    System.arraycopy(delDataBlkIndices, 0,
        delBlkIndices, 0, delDataBlkIndices.length);
    System.arraycopy(delParityBlkIndices, 0,
        delBlkIndices, delDataBlkIndices.length, delParityBlkIndices.length);

    ExtendedBlock[] delBlocks = new ExtendedBlock[recoverBlkNum];
    for (int i = 0; i < recoverBlkNum; i++) {
      delBlocks[i] = StripedBlockUtil
          .constructInternalBlock(lastBlock.getBlock(),
              cellSize, dataBlocks, delBlkIndices[i]);
      if (deleteBlockFile) {
        // delete the block file
        cluster.corruptBlockOnDataNodesByDeletingBlockFile(delBlocks[i]);
      } else {
        // corrupt the block file
        cluster.corruptBlockOnDataNodes(delBlocks[i]);
      }
    }
  }

  private LocatedBlocks getLocatedBlocks(Path filePath) throws IOException {
    return fs.getClient().getLocatedBlocks(filePath.toString(),
        0, Long.MAX_VALUE);
  }
}
