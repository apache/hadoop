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
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ErasureCodingInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.namenode.ErasureCodingSchemaManager;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class TestDFSStripedInputStream {

  public static final Log LOG = LogFactory.getLog(TestDFSStripedInputStream.class);

  private MiniDFSCluster cluster;
  private Configuration conf = new Configuration();
  private DistributedFileSystem fs;
  private final Path dirPath = new Path("/striped");
  private Path filePath = new Path(dirPath, "file");
  private ErasureCodingInfo info = new ErasureCodingInfo(filePath.toString(),
      ErasureCodingSchemaManager.getSystemDefaultSchema());
  private final short DATA_BLK_NUM = HdfsConstants.NUM_DATA_BLOCKS;
  private final short PARITY_BLK_NUM = HdfsConstants.NUM_PARITY_BLOCKS;
  private final int CELLSIZE = HdfsConstants.BLOCK_STRIPED_CELL_SIZE;
  private final int NUM_STRIPE_PER_BLOCK = 2;
  private final int INTERNAL_BLOCK_SIZE = NUM_STRIPE_PER_BLOCK * CELLSIZE;
  private final int BLOCK_GROUP_SIZE =  DATA_BLK_NUM * INTERNAL_BLOCK_SIZE;

  @Before
  public void setup() throws IOException {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, INTERNAL_BLOCK_SIZE);
    SimulatedFSDataset.setFactory(conf);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(
        DATA_BLK_NUM + PARITY_BLK_NUM).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    fs.mkdirs(dirPath);
    fs.getClient().createErasureCodingZone(dirPath.toString(), null);
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
    DFSTestUtil.createStripedFile(cluster, filePath, null, numBlocks,
        NUM_STRIPE_PER_BLOCK, false);
    LocatedBlocks lbs = fs.getClient().namenode.getBlockLocations(
        filePath.toString(), 0, BLOCK_GROUP_SIZE * numBlocks);
    final DFSStripedInputStream in =
        new DFSStripedInputStream(fs.getClient(), filePath.toString(), false, info);

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
    final int numBlocks = 2;
    DFSTestUtil.createStripedFile(cluster, filePath, null, numBlocks,
        NUM_STRIPE_PER_BLOCK, false);
    LocatedBlocks lbs = fs.getClient().namenode.getBlockLocations(
        filePath.toString(), 0, BLOCK_GROUP_SIZE);

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
        new DFSStripedInputStream(fs.getClient(),
            filePath.toString(), false, info);
    int readSize = BLOCK_GROUP_SIZE;
    byte[] readBuffer = new byte[readSize];
    int ret = in.read(0, readBuffer, 0, readSize);

    assertEquals(readSize, ret);
    // TODO: verify read results with patterned data from HDFS-8117
  }

  @Test
  public void testStatefulRead() throws Exception {
    testStatefulRead(false, false);
    testStatefulRead(true, false);
    testStatefulRead(true, true);
  }

  private void testStatefulRead(boolean useByteBuffer, boolean cellMisalignPacket)
      throws Exception {
    final int numBlocks = 2;
    final int fileSize = numBlocks * BLOCK_GROUP_SIZE;
    if (cellMisalignPacket) {
      conf.setInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT + 1);
      tearDown();
      setup();
    }
    DFSTestUtil.createStripedFile(cluster, filePath, null, numBlocks,
        NUM_STRIPE_PER_BLOCK, false);
    LocatedBlocks lbs = fs.getClient().namenode.getBlockLocations(
        filePath.toString(), 0, fileSize);

    assert lbs.getLocatedBlocks().size() == numBlocks;
    for (LocatedBlock lb : lbs.getLocatedBlocks()) {
      assert lb instanceof LocatedStripedBlock;
      LocatedStripedBlock bg = (LocatedStripedBlock)(lb);
      for (int i = 0; i < DATA_BLK_NUM; i++) {
        Block blk = new Block(bg.getBlock().getBlockId() + i,
            NUM_STRIPE_PER_BLOCK * CELLSIZE,
            bg.getBlock().getGenerationStamp());
        blk.setGenerationStamp(bg.getBlock().getGenerationStamp());
        cluster.injectBlocks(i, Arrays.asList(blk),
            bg.getBlock().getBlockPoolId());
      }
    }

    DFSStripedInputStream in =
        new DFSStripedInputStream(fs.getClient(), filePath.toString(),
            false, info);

    byte[] expected = new byte[fileSize];

    for (LocatedBlock bg : lbs.getLocatedBlocks()) {
      /** A variation of {@link DFSTestUtil#fillExpectedBuf} for striped blocks */
      for (int i = 0; i < NUM_STRIPE_PER_BLOCK; i++) {
        for (int j = 0; j < DATA_BLK_NUM; j++) {
          for (int k = 0; k < CELLSIZE; k++) {
            int posInBlk = i * CELLSIZE + k;
            int posInFile = (int) bg.getStartOffset() +
                i * CELLSIZE * DATA_BLK_NUM + j * CELLSIZE + k;
            expected[posInFile] = SimulatedFSDataset.simulatedByte(
                new Block(bg.getBlock().getBlockId() + j), posInBlk);
          }
        }
      }
    }

    if (useByteBuffer) {
      ByteBuffer readBuffer = ByteBuffer.allocate(fileSize);
      int done = 0;
      while (done < fileSize) {
        int ret = in.read(readBuffer);
        assertTrue(ret > 0);
        done += ret;
      }
      assertArrayEquals(expected, readBuffer.array());
    } else {
      byte[] readBuffer = new byte[fileSize];
      int done = 0;
      while (done < fileSize) {
        int ret = in.read(readBuffer, done, fileSize - done);
        assertTrue(ret > 0);
        done += ret;
      }
      assertArrayEquals(expected, readBuffer);
    }
    fs.delete(filePath, true);
  }
}
