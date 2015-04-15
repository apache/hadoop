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
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.junit.After;
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
  private final short DATA_BLK_NUM = HdfsConstants.NUM_DATA_BLOCKS;
  private final short PARITY_BLK_NUM = HdfsConstants.NUM_PARITY_BLOCKS;
  private final short BLK_GROUP_SIZE = DATA_BLK_NUM + PARITY_BLK_NUM;
  private final int CELLSIZE = HdfsConstants.BLOCK_STRIPED_CELL_SIZE;
  private final int NUM_STRIPE_PER_BLOCK = 2;
  private final int BLOCKSIZE = NUM_STRIPE_PER_BLOCK * DATA_BLK_NUM * CELLSIZE;

  @Before
  public void setup() throws IOException {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCKSIZE);
    SimulatedFSDataset.setFactory(conf);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(BLK_GROUP_SIZE)
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

  /**
   * Test {@link DFSStripedInputStream#getBlockAt(long)}
   */
  @Test
  public void testGetBlock() throws Exception {
    final int numBlocks = 4;
    DFSTestUtil.createStripedFile(cluster, filePath, dirPath, numBlocks,
        NUM_STRIPE_PER_BLOCK, true);
    LocatedBlocks lbs = fs.getClient().namenode.getBlockLocations(
        filePath.toString(), 0, BLOCKSIZE * numBlocks);
    final DFSStripedInputStream in =
        new DFSStripedInputStream(fs.getClient(), filePath.toString(), false);

    List<LocatedBlock> lbList = lbs.getLocatedBlocks();
    for (LocatedBlock aLbList : lbList) {
      LocatedStripedBlock lsb = (LocatedStripedBlock) aLbList;
      LocatedBlock[] blks = StripedBlockUtil.parseStripedBlockGroup(lsb,
          CELLSIZE, DATA_BLK_NUM, PARITY_BLK_NUM);
      for (int j = 0; j < DATA_BLK_NUM; j++) {
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
    DFSTestUtil.createStripedFile(cluster, filePath, dirPath, numBlocks,
        NUM_STRIPE_PER_BLOCK, true);
    LocatedBlocks lbs = fs.getClient().namenode.getBlockLocations(
        filePath.toString(), 0, BLOCKSIZE);

    assert lbs.get(0) instanceof LocatedStripedBlock;
    LocatedStripedBlock bg = (LocatedStripedBlock)(lbs.get(0));
    for (int i = 0; i < DATA_BLK_NUM; i++) {
      Block blk = new Block(bg.getBlock().getBlockId() + i,
          NUM_STRIPE_PER_BLOCK * CELLSIZE,
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
