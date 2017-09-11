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

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Utility class for testing online recovery of striped files.
 */
abstract public class ReadStripedFileWithDecodingHelper {
  static final Logger LOG =
      LoggerFactory.getLogger(ReadStripedFileWithDecodingHelper.class);

  static {
    ((Log4JLogger)LogFactory.getLog(BlockPlacementPolicy.class))
        .getLogger().setLevel(org.apache.log4j.Level.ALL);
    GenericTestUtils.setLogLevel(BlockManager.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(BlockManager.blockLog, Level.DEBUG);
    GenericTestUtils.setLogLevel(NameNode.stateChangeLog, Level.DEBUG);
  }

  protected static final ErasureCodingPolicy EC_POLICY =
      StripedFileTestUtil.getDefaultECPolicy();
  protected static final short NUM_DATA_UNITS =
      (short) EC_POLICY.getNumDataUnits();
  protected static final short NUM_PARITY_UNITS =
      (short) EC_POLICY.getNumParityUnits();
  protected static final int CELL_SIZE = EC_POLICY.getCellSize();
  private static final int STRIPES_PER_BLOCK = 4;
  protected static final int BLOCK_SIZE = CELL_SIZE * STRIPES_PER_BLOCK;
  private static final int BLOCK_GROUP_SIZE = BLOCK_SIZE * NUM_DATA_UNITS;

  private static final int NUM_DATANODES = NUM_DATA_UNITS + NUM_PARITY_UNITS;

  protected static final int[] FILE_LENGTHS =
      {BLOCK_GROUP_SIZE - 123, BLOCK_GROUP_SIZE + 123};

  public static MiniDFSCluster initializeCluster() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 0);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_KEY,
        0);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY,
        false);
    conf.set(DFSConfigKeys.DFS_NAMENODE_EC_POLICIES_ENABLED_KEY,
        StripedFileTestUtil.getDefaultECPolicy().getName());
    MiniDFSCluster myCluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(NUM_DATANODES)
        .build();
    myCluster.getFileSystem().getClient().setErasureCodingPolicy("/",
        StripedFileTestUtil.getDefaultECPolicy().getName());
    return myCluster;
  }

  public static void tearDownCluster(MiniDFSCluster cluster)
      throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  public static int findFirstDataNode(MiniDFSCluster cluster,
      DistributedFileSystem dfs, Path file, long length) throws IOException {
    BlockLocation[] locs = dfs.getFileBlockLocations(file, 0, length);
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

  /**
   * Cross product of FILE_LENGTHS, NUM_PARITY_UNITS+1, NUM_PARITY_UNITS.
   * Input for parameterized tests classes.
   *
   * @return Test parameters.
   */
  public static Collection<Object[]> getParameters() {
    ArrayList<Object[]> params = new ArrayList<>();
    for (int fileLength : FILE_LENGTHS) {
      for (int dataDelNum = 1; dataDelNum <= NUM_PARITY_UNITS; dataDelNum++) {
        for (int parityDelNum = 0;
             (dataDelNum + parityDelNum) <= NUM_PARITY_UNITS; parityDelNum++) {
          params.add(new Object[] {fileLength, dataDelNum, parityDelNum});
        }
      }
    }
    return params;
  }

  public static void verifyRead(DistributedFileSystem dfs, Path testPath,
      int length, byte[] expected) throws IOException {
    LOG.info("verifyRead on path {}", testPath);
    byte[] buffer = new byte[length + 100];
    LOG.info("verifyRead verifyLength on path {}", testPath);
    StripedFileTestUtil.verifyLength(dfs, testPath, length);
    LOG.info("verifyRead verifyPread on path {}", testPath);
    StripedFileTestUtil.verifyPread(dfs, testPath, length, expected, buffer);
    LOG.info("verifyRead verifyStatefulRead on path {}", testPath);
    StripedFileTestUtil.verifyStatefulRead(dfs, testPath, length, expected,
        buffer);
    LOG.info("verifyRead verifyStatefulRead2 on path {}", testPath);
    StripedFileTestUtil.verifyStatefulRead(dfs, testPath, length, expected,
        ByteBuffer.allocate(length + 100));
    LOG.info("verifyRead verifySeek on path {}", testPath);
    StripedFileTestUtil.verifySeek(dfs, testPath, length, EC_POLICY,
        BLOCK_GROUP_SIZE);
  }

  public static void testReadWithDNFailure(MiniDFSCluster cluster,
      DistributedFileSystem dfs, int fileLength, int dnFailureNum)
      throws Exception {
    String fileType = fileLength < (BLOCK_SIZE * NUM_DATA_UNITS) ?
        "smallFile" : "largeFile";
    String src = "/dnFailure_" + dnFailureNum + "_" + fileType;
    LOG.info("testReadWithDNFailure: file = " + src
        + ", fileSize = " + fileLength
        + ", dnFailureNum = " + dnFailureNum);

    Path testPath = new Path(src);
    final byte[] bytes = StripedFileTestUtil.generateBytes(fileLength);
    DFSTestUtil.writeFile(dfs, testPath, bytes);
    StripedFileTestUtil.waitBlockGroupsReported(dfs, src);

    // shut down the DN that holds an internal data block
    BlockLocation[] locs = dfs.getFileBlockLocations(testPath, CELL_SIZE * 5,
        CELL_SIZE);
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
    verifyRead(dfs, testPath, fileLength, bytes);
  }


  /**
   * Test reading a file with some blocks(data blocks or parity blocks or both)
   * deleted or corrupted.
   * @param src file path
   * @param fileNumBytes file length
   * @param dataBlkDelNum the deleted or corrupted number of data blocks.
   * @param parityBlkDelNum the deleted or corrupted number of parity blocks.
   * @param deleteBlockFile whether block file is deleted or corrupted.
   *                        true is to delete the block file.
   *                        false is to corrupt the content of the block file.
   * @throws IOException
   */
  public static void testReadWithBlockCorrupted(MiniDFSCluster cluster,
      DistributedFileSystem dfs, String src, int fileNumBytes,
      int dataBlkDelNum, int parityBlkDelNum,
      boolean deleteBlockFile) throws IOException {
    LOG.info("testReadWithBlockCorrupted: file = " + src
        + ", dataBlkDelNum = " + dataBlkDelNum
        + ", parityBlkDelNum = " + parityBlkDelNum
        + ", deleteBlockFile? " + deleteBlockFile);
    int recoverBlkNum = dataBlkDelNum + parityBlkDelNum;
    Assert.assertTrue("dataBlkDelNum and parityBlkDelNum should be positive",
        dataBlkDelNum >= 0 && parityBlkDelNum >= 0);
    Assert.assertTrue("The sum of dataBlkDelNum and parityBlkDelNum " +
        "should be between 1 ~ " + NUM_PARITY_UNITS, recoverBlkNum <=
        NUM_PARITY_UNITS);

    // write a file with the length of writeLen
    Path srcPath = new Path(src);
    final byte[] bytes = StripedFileTestUtil.generateBytes(fileNumBytes);
    DFSTestUtil.writeFile(dfs, srcPath, bytes);

    // delete or corrupt some blocks
    corruptBlocks(cluster, dfs, srcPath, dataBlkDelNum, parityBlkDelNum,
        deleteBlockFile);

    // check the file can be read after some blocks were deleted
    verifyRead(dfs, srcPath, fileNumBytes, bytes);
  }

  public static void corruptBlocks(MiniDFSCluster cluster,
      DistributedFileSystem dfs, Path srcPath,
      int dataBlkDelNum, int parityBlkDelNum, boolean deleteBlockFile)
      throws IOException {
    LOG.info("corruptBlocks on path {}", srcPath);
    int recoverBlkNum = dataBlkDelNum + parityBlkDelNum;

    LocatedBlocks locatedBlocks = getLocatedBlocks(dfs, srcPath);
    LocatedStripedBlock lastBlock =
        (LocatedStripedBlock)locatedBlocks.getLastLocatedBlock();

    int[] delDataBlkIndices = StripedFileTestUtil.randomArray(0, NUM_DATA_UNITS,
        dataBlkDelNum);
    Assert.assertNotNull(delDataBlkIndices);
    int[] delParityBlkIndices = StripedFileTestUtil.randomArray(NUM_DATA_UNITS,
        NUM_DATA_UNITS + NUM_PARITY_UNITS, parityBlkDelNum);
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
              CELL_SIZE, NUM_DATA_UNITS, delBlkIndices[i]);
      if (deleteBlockFile) {
        // delete the block file
        LOG.info("Deleting block file {}", delBlocks[i]);
        cluster.corruptBlockOnDataNodesByDeletingBlockFile(delBlocks[i]);
      } else {
        // corrupt the block file
        LOG.info("Corrupting block file {}", delBlocks[i]);
        cluster.corruptBlockOnDataNodes(delBlocks[i]);
      }
    }
  }

  public static LocatedBlocks getLocatedBlocks(DistributedFileSystem dfs,
      Path filePath) throws IOException {
    return dfs.getClient().getLocatedBlocks(filePath.toString(),
        0, Long.MAX_VALUE);
  }
}
