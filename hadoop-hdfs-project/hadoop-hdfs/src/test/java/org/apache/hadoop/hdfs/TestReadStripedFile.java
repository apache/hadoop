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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import static org.apache.hadoop.hdfs.DFSStripedInputStream.ReadPortion;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hdfs.server.blockmanagement.BlockIdManager;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class TestReadStripedFile {

  public static final Log LOG = LogFactory.getLog(TestReadStripedFile.class);

  private MiniDFSCluster cluster;
  private Configuration conf = new Configuration();
  private DistributedFileSystem fs;
  private final Path dirPath = new Path("/striped");
  private Path filePath = new Path(dirPath, "file");
  private final short GROUP_SIZE = HdfsConstants.NUM_DATA_BLOCKS;
  private final short TOTAL_SIZE = HdfsConstants.NUM_DATA_BLOCKS + HdfsConstants.NUM_PARITY_BLOCKS;
  private final int CELLSIZE = HdfsConstants.BLOCK_STRIPED_CELL_SIZE;
  private final int NUM_STRIPE_PER_BLOCK = 2;
  private final int BLOCKSIZE = 2 * GROUP_SIZE * CELLSIZE;

  @Before
  public void setup() throws IOException {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCKSIZE);
    SimulatedFSDataset.setFactory(conf);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(TOTAL_SIZE)
        .build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private void testPlanReadPortions(int startInBlk, int length,
      int bufferOffset, int[] readLengths, int[] offsetsInBlock,
      int[][] bufferOffsets, int[][] bufferLengths) {
    ReadPortion[] results = DFSStripedInputStream.planReadPortions(GROUP_SIZE,
        CELLSIZE, startInBlk, length, bufferOffset);
    assertEquals(GROUP_SIZE, results.length);

    for (int i = 0; i < GROUP_SIZE; i++) {
      assertEquals(readLengths[i], results[i].getReadLength());
      assertEquals(offsetsInBlock[i], results[i].getStartOffsetInBlock());
      final int[] bOffsets = results[i].getOffsets();
      assertArrayEquals(bufferOffsets[i], bOffsets);
      final int[] bLengths = results[i].getLengths();
      assertArrayEquals(bufferLengths[i], bLengths);
    }
  }

  /**
   * Test {@link DFSStripedInputStream#planReadPortions}
   */
  @Test
  public void testPlanReadPortions() {
    /**
     * start block offset is 0, read cellSize - 10
     */
    testPlanReadPortions(0, CELLSIZE - 10, 0,
        new int[]{CELLSIZE - 10, 0, 0}, new int[]{0, 0, 0},
        new int[][]{new int[]{0}, new int[]{}, new int[]{}},
        new int[][]{new int[]{CELLSIZE - 10}, new int[]{}, new int[]{}});

    /**
     * start block offset is 0, read 3 * cellSize
     */
    testPlanReadPortions(0, GROUP_SIZE * CELLSIZE, 0,
        new int[]{CELLSIZE, CELLSIZE, CELLSIZE}, new int[]{0, 0, 0},
        new int[][]{new int[]{0}, new int[]{CELLSIZE}, new int[]{CELLSIZE * 2}},
        new int[][]{new int[]{CELLSIZE}, new int[]{CELLSIZE}, new int[]{CELLSIZE}});

    /**
     * start block offset is 0, read cellSize + 10
     */
    testPlanReadPortions(0, CELLSIZE + 10, 0,
        new int[]{CELLSIZE, 10, 0}, new int[]{0, 0, 0},
        new int[][]{new int[]{0}, new int[]{CELLSIZE}, new int[]{}},
        new int[][]{new int[]{CELLSIZE}, new int[]{10}, new int[]{}});

    /**
     * start block offset is 0, read 5 * cellSize + 10, buffer start offset is 100
     */
    testPlanReadPortions(0, 5 * CELLSIZE + 10, 100,
        new int[]{CELLSIZE * 2, CELLSIZE * 2, CELLSIZE + 10}, new int[]{0, 0, 0},
        new int[][]{new int[]{100, 100 + CELLSIZE * GROUP_SIZE},
            new int[]{100 + CELLSIZE, 100 + CELLSIZE * 4},
            new int[]{100 + CELLSIZE * 2, 100 + CELLSIZE * 5}},
        new int[][]{new int[]{CELLSIZE, CELLSIZE},
            new int[]{CELLSIZE, CELLSIZE},
            new int[]{CELLSIZE, 10}});

    /**
     * start block offset is 2, read 3 * cellSize
     */
    testPlanReadPortions(2, GROUP_SIZE * CELLSIZE, 100,
        new int[]{CELLSIZE, CELLSIZE, CELLSIZE},
        new int[]{2, 0, 0},
        new int[][]{new int[]{100, 100 + GROUP_SIZE * CELLSIZE - 2},
            new int[]{100 + CELLSIZE - 2},
            new int[]{100 + CELLSIZE * 2 - 2}},
        new int[][]{new int[]{CELLSIZE - 2, 2},
            new int[]{CELLSIZE},
            new int[]{CELLSIZE}});

    /**
     * start block offset is 2, read 3 * cellSize + 10
     */
    testPlanReadPortions(2, GROUP_SIZE * CELLSIZE + 10, 0,
        new int[]{CELLSIZE + 10, CELLSIZE, CELLSIZE},
        new int[]{2, 0, 0},
        new int[][]{new int[]{0, GROUP_SIZE * CELLSIZE - 2},
            new int[]{CELLSIZE - 2},
            new int[]{CELLSIZE * 2 - 2}},
        new int[][]{new int[]{CELLSIZE - 2, 12},
            new int[]{CELLSIZE},
            new int[]{CELLSIZE}});

    /**
     * start block offset is cellSize * 2 - 1, read 5 * cellSize + 10
     */
    testPlanReadPortions(CELLSIZE * 2 - 1, 5 * CELLSIZE + 10, 0,
        new int[]{CELLSIZE * 2, CELLSIZE + 10, CELLSIZE * 2},
        new int[]{CELLSIZE, CELLSIZE - 1, 0},
        new int[][]{new int[]{CELLSIZE + 1, 4 * CELLSIZE + 1},
            new int[]{0, 2 * CELLSIZE + 1, 5 * CELLSIZE + 1},
            new int[]{1, 3 * CELLSIZE + 1}},
        new int[][]{new int[]{CELLSIZE, CELLSIZE},
            new int[]{1, CELLSIZE, 9},
            new int[]{CELLSIZE, CELLSIZE}});

    /**
     * start block offset is cellSize * 6 - 1, read 7 * cellSize + 10
     */
    testPlanReadPortions(CELLSIZE * 6 - 1, 7 * CELLSIZE + 10, 0,
        new int[]{CELLSIZE * 3, CELLSIZE * 2 + 9, CELLSIZE * 2 + 1},
        new int[]{CELLSIZE * 2, CELLSIZE * 2, CELLSIZE * 2 - 1},
        new int[][]{new int[]{1, 3 * CELLSIZE + 1, 6 * CELLSIZE + 1},
            new int[]{CELLSIZE + 1, 4 * CELLSIZE + 1, 7 * CELLSIZE + 1},
            new int[]{0, 2 * CELLSIZE + 1, 5 * CELLSIZE + 1}},
        new int[][]{new int[]{CELLSIZE, CELLSIZE, CELLSIZE},
            new int[]{CELLSIZE, CELLSIZE, 9},
            new int[]{1, CELLSIZE, CELLSIZE}});
  }

  private LocatedStripedBlock createDummyLocatedBlock() {
    final long blockGroupID = -1048576;
    DatanodeInfo[] locs = new DatanodeInfo[TOTAL_SIZE];
    String[] storageIDs = new String[TOTAL_SIZE];
    StorageType[] storageTypes = new StorageType[TOTAL_SIZE];
    int[] indices = new int[TOTAL_SIZE];
    for (int i = 0; i < TOTAL_SIZE; i++) {
      locs[i] = new DatanodeInfo(cluster.getDataNodes().get(i).getDatanodeId());
      storageIDs[i] = cluster.getDataNodes().get(i).getDatanodeUuid();
      storageTypes[i] = StorageType.DISK;
      indices[i] = (i + 2) % GROUP_SIZE;
    }
    return new LocatedStripedBlock(new ExtendedBlock("pool", blockGroupID),
        locs, storageIDs, storageTypes, indices, 0, false, null);
  }

  @Test
  public void testParseDummyStripedBlock() {
    LocatedStripedBlock lsb = createDummyLocatedBlock();
    LocatedBlock[] blocks = DFSStripedInputStream.parseStripedBlockGroup(
        lsb, GROUP_SIZE, CELLSIZE);
    assertEquals(GROUP_SIZE, blocks.length);
    for (int j = 0; j < GROUP_SIZE; j++) {
      assertFalse(blocks[j].isStriped());
      assertEquals(j,
          BlockIdManager.getBlockIndex(blocks[j].getBlock().getLocalBlock()));
      assertEquals(j * CELLSIZE, blocks[j].getStartOffset());
    }
  }

  @Test
  public void testParseStripedBlock() throws Exception {
    final int numBlocks = 4;
    DFSTestUtil.createECFile(cluster, filePath, dirPath, numBlocks,
        NUM_STRIPE_PER_BLOCK);
    LocatedBlocks lbs = fs.getClient().namenode.getBlockLocations(
        filePath.toString(), 0, BLOCKSIZE * numBlocks);

    assertEquals(4, lbs.locatedBlockCount());
    List<LocatedBlock> lbList = lbs.getLocatedBlocks();
    for (LocatedBlock lb : lbList) {
      assertTrue(lb.isStriped());
    }

    for (int i = 0; i < numBlocks; i++) {
      LocatedStripedBlock lsb = (LocatedStripedBlock) (lbs.get(i));
      LocatedBlock[] blks = DFSStripedInputStream.parseStripedBlockGroup(lsb,
          GROUP_SIZE, CELLSIZE);
      assertEquals(GROUP_SIZE, blks.length);
      for (int j = 0; j < GROUP_SIZE; j++) {
        assertFalse(blks[j].isStriped());
        assertEquals(j,
            BlockIdManager.getBlockIndex(blks[j].getBlock().getLocalBlock()));
        assertEquals(i * BLOCKSIZE + j * CELLSIZE, blks[j].getStartOffset());
      }
    }
  }

  /**
   * Test {@link DFSStripedInputStream#getBlockAt(long)}
   */
  @Test
  public void testGetBlock() throws Exception {
    final int numBlocks = 4;
    DFSTestUtil.createECFile(cluster, filePath, dirPath, numBlocks,
        NUM_STRIPE_PER_BLOCK);
    LocatedBlocks lbs = fs.getClient().namenode.getBlockLocations(
        filePath.toString(), 0, BLOCKSIZE * numBlocks);
    final DFSStripedInputStream in =
        new DFSStripedInputStream(fs.getClient(), filePath.toString(), false);

    List<LocatedBlock> lbList = lbs.getLocatedBlocks();
    for (LocatedBlock aLbList : lbList) {
      LocatedStripedBlock lsb = (LocatedStripedBlock) aLbList;
      LocatedBlock[] blks = DFSStripedInputStream.parseStripedBlockGroup(lsb,
          GROUP_SIZE, CELLSIZE);
      for (int j = 0; j < GROUP_SIZE; j++) {
        LocatedBlock refreshed = in.getBlockAt(blks[j].getStartOffset());
        assertEquals(blks[j].getBlock(), refreshed.getBlock());
        assertEquals(blks[j].getStartOffset(), refreshed.getStartOffset());
        assertArrayEquals(blks[j].getLocations(), refreshed.getLocations());
      }
    }
  }

  @Test
  public void testPread() throws Exception {
    final int numBlocks = 4;
    DFSTestUtil.createECFile(cluster, filePath, dirPath, numBlocks,
        NUM_STRIPE_PER_BLOCK);
    LocatedBlocks lbs = fs.getClient().namenode.getBlockLocations(
        filePath.toString(), 0, BLOCKSIZE);

    assert lbs.get(0) instanceof LocatedStripedBlock;
    LocatedStripedBlock bg = (LocatedStripedBlock)(lbs.get(0));
    for (int i = 0; i < GROUP_SIZE; i++) {
      Block blk = new Block(bg.getBlock().getBlockId() + i, BLOCKSIZE,
          bg.getBlock().getGenerationStamp());
      blk.setGenerationStamp(bg.getBlock().getGenerationStamp());
      cluster.injectBlocks(i, Arrays.asList(blk),
          bg.getBlock().getBlockPoolId());
    }
    DFSStripedInputStream in =
        new DFSStripedInputStream(fs.getClient(), filePath.toString(), false);
    in.setCellSize(CELLSIZE);
    int readSize = BLOCKSIZE;
    byte[] readBuffer = new byte[readSize];
    int ret = in.read(0, readBuffer, 0, readSize);

    assertEquals(readSize, ret);
    // TODO: verify read results with patterned data from HDFS-8117
  }
}
