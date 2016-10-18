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
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.namenode.ErasureCodingPolicyManager;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ErasureCodeNative;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.NativeRSRawErasureCoderFactory;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestDFSStripedInputStream {

  public static final Log LOG =
      LogFactory.getLog(TestDFSStripedInputStream.class);

  private MiniDFSCluster cluster;
  private Configuration conf = new Configuration();
  private DistributedFileSystem fs;
  private final Path dirPath = new Path("/striped");
  private Path filePath = new Path(dirPath, "file");
  private final ErasureCodingPolicy ecPolicy =
      ErasureCodingPolicyManager.getSystemDefaultPolicy();
  private final short DATA_BLK_NUM = StripedFileTestUtil.NUM_DATA_BLOCKS;
  private final short PARITY_BLK_NUM = StripedFileTestUtil.NUM_PARITY_BLOCKS;
  private final int CELLSIZE = StripedFileTestUtil.BLOCK_STRIPED_CELL_SIZE;
  private final int NUM_STRIPE_PER_BLOCK = 2;
  private final int INTERNAL_BLOCK_SIZE = NUM_STRIPE_PER_BLOCK * CELLSIZE;
  private final int BLOCK_GROUP_SIZE =  DATA_BLK_NUM * INTERNAL_BLOCK_SIZE;

  @Rule
  public Timeout globalTimeout = new Timeout(300000);

  @Before
  public void setup() throws IOException {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, INTERNAL_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 0);
    if (ErasureCodeNative.isNativeCodeLoaded()) {
      conf.set(
          CodecUtil.IO_ERASURECODE_CODEC_RS_DEFAULT_RAWCODER_KEY,
          NativeRSRawErasureCoderFactory.class.getCanonicalName());
    }
    SimulatedFSDataset.setFactory(conf);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(
        DATA_BLK_NUM + PARITY_BLK_NUM).build();
    cluster.waitActive();
    for (DataNode dn : cluster.getDataNodes()) {
      DataNodeTestUtils.setHeartbeatsDisabledForTests(dn, true);
    }
    fs = cluster.getFileSystem();
    fs.mkdirs(dirPath);
    fs.getClient().setErasureCodingPolicy(dirPath.toString(), null);
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * Test {@link DFSStripedInputStream#getBlockAt(long)}
   */
  @Test
  public void testRefreshBlock() throws Exception {
    final int numBlocks = 4;
    DFSTestUtil.createStripedFile(cluster, filePath, null, numBlocks,
        NUM_STRIPE_PER_BLOCK, false);
    LocatedBlocks lbs = fs.getClient().namenode.getBlockLocations(
        filePath.toString(), 0, BLOCK_GROUP_SIZE * numBlocks);
    final DFSStripedInputStream in = new DFSStripedInputStream(fs.getClient(),
        filePath.toString(), false, ecPolicy, null);

    List<LocatedBlock> lbList = lbs.getLocatedBlocks();
    for (LocatedBlock aLbList : lbList) {
      LocatedStripedBlock lsb = (LocatedStripedBlock) aLbList;
      LocatedBlock[] blks = StripedBlockUtil.parseStripedBlockGroup(lsb,
          CELLSIZE, DATA_BLK_NUM, PARITY_BLK_NUM);
      for (int j = 0; j < DATA_BLK_NUM; j++) {
        LocatedBlock refreshed = in.refreshLocatedBlock(blks[j]);
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
        filePath.toString(), 0, BLOCK_GROUP_SIZE * numBlocks);
    int fileLen = BLOCK_GROUP_SIZE * numBlocks;

    byte[] expected = new byte[fileLen];
    assertEquals(numBlocks, lbs.getLocatedBlocks().size());
    for (int bgIdx = 0; bgIdx < numBlocks; bgIdx++) {
      LocatedStripedBlock bg = (LocatedStripedBlock) (lbs.get(bgIdx));
      for (int i = 0; i < DATA_BLK_NUM; i++) {
        Block blk = new Block(bg.getBlock().getBlockId() + i,
            NUM_STRIPE_PER_BLOCK * CELLSIZE,
            bg.getBlock().getGenerationStamp());
        blk.setGenerationStamp(bg.getBlock().getGenerationStamp());
        cluster.injectBlocks(i, Arrays.asList(blk),
            bg.getBlock().getBlockPoolId());
      }

      /** A variation of {@link DFSTestUtil#fillExpectedBuf} for striped blocks */
      for (int i = 0; i < NUM_STRIPE_PER_BLOCK; i++) {
        for (int j = 0; j < DATA_BLK_NUM; j++) {
          for (int k = 0; k < CELLSIZE; k++) {
            int posInBlk = i * CELLSIZE + k;
            int posInFile = i * CELLSIZE * DATA_BLK_NUM + j * CELLSIZE + k;
            expected[bgIdx*BLOCK_GROUP_SIZE + posInFile] =
                SimulatedFSDataset.simulatedByte(
                    new Block(bg.getBlock().getBlockId() + j), posInBlk);
          }
        }
      }
    }
    DFSStripedInputStream in = new DFSStripedInputStream(fs.getClient(),
        filePath.toString(), false, ecPolicy, null);

    int[] startOffsets = {0, 1, CELLSIZE - 102, CELLSIZE, CELLSIZE + 102,
        CELLSIZE*DATA_BLK_NUM, CELLSIZE*DATA_BLK_NUM + 102,
        BLOCK_GROUP_SIZE - 102, BLOCK_GROUP_SIZE, BLOCK_GROUP_SIZE + 102,
        fileLen - 1};
    for (int startOffset : startOffsets) {
      startOffset = Math.max(0, Math.min(startOffset, fileLen - 1));
      int remaining = fileLen - startOffset;
      byte[] buf = new byte[fileLen];
      int ret = in.read(startOffset, buf, 0, fileLen);
      assertEquals(remaining, ret);
      for (int i = 0; i < remaining; i++) {
        Assert.assertEquals("Byte at " + (startOffset + i) + " should be the " +
                "same",
            expected[startOffset + i], buf[i]);
      }
    }
    in.close();
  }

  @Test
  public void testPreadWithDNFailure() throws Exception {
    final int numBlocks = 4;
    final int failedDNIdx = DATA_BLK_NUM - 1;
    DFSTestUtil.createStripedFile(cluster, filePath, null, numBlocks,
        NUM_STRIPE_PER_BLOCK, false);
    LocatedBlocks lbs = fs.getClient().namenode.getBlockLocations(
        filePath.toString(), 0, BLOCK_GROUP_SIZE);

    assert lbs.get(0) instanceof LocatedStripedBlock;
    LocatedStripedBlock bg = (LocatedStripedBlock)(lbs.get(0));
    for (int i = 0; i < DATA_BLK_NUM + PARITY_BLK_NUM; i++) {
      Block blk = new Block(bg.getBlock().getBlockId() + i,
          NUM_STRIPE_PER_BLOCK * CELLSIZE,
          bg.getBlock().getGenerationStamp());
      blk.setGenerationStamp(bg.getBlock().getGenerationStamp());
      cluster.injectBlocks(i, Arrays.asList(blk),
          bg.getBlock().getBlockPoolId());
    }
    DFSStripedInputStream in =
        new DFSStripedInputStream(fs.getClient(), filePath.toString(), false,
            ecPolicy, null);
    int readSize = BLOCK_GROUP_SIZE;
    byte[] readBuffer = new byte[readSize];
    byte[] expected = new byte[readSize];
    /** A variation of {@link DFSTestUtil#fillExpectedBuf} for striped blocks */
    for (int i = 0; i < NUM_STRIPE_PER_BLOCK; i++) {
      for (int j = 0; j < DATA_BLK_NUM; j++) {
        for (int k = 0; k < CELLSIZE; k++) {
          int posInBlk = i * CELLSIZE + k;
          int posInFile = i * CELLSIZE * DATA_BLK_NUM + j * CELLSIZE + k;
          expected[posInFile] = SimulatedFSDataset.simulatedByte(
              new Block(bg.getBlock().getBlockId() + j), posInBlk);
        }
      }
    }

    ErasureCoderOptions coderOptions = new ErasureCoderOptions(
        DATA_BLK_NUM, PARITY_BLK_NUM);
    RawErasureDecoder rawDecoder = CodecUtil.createRawDecoder(conf,
        ecPolicy.getCodecName(), coderOptions);

    // Update the expected content for decoded data
    int[] missingBlkIdx = new int[PARITY_BLK_NUM];
    for (int i = 0; i < missingBlkIdx.length; i++) {
      if (i == 0) {
        missingBlkIdx[i] = failedDNIdx;
      } else {
        missingBlkIdx[i] = DATA_BLK_NUM + i;
      }
    }
    cluster.stopDataNode(failedDNIdx);
    for (int i = 0; i < NUM_STRIPE_PER_BLOCK; i++) {
      byte[][] decodeInputs = new byte[DATA_BLK_NUM + PARITY_BLK_NUM][CELLSIZE];
      byte[][] decodeOutputs = new byte[missingBlkIdx.length][CELLSIZE];
      for (int j = 0; j < DATA_BLK_NUM; j++) {
        int posInBuf = i * CELLSIZE * DATA_BLK_NUM + j * CELLSIZE;
        if (j != failedDNIdx) {
          System.arraycopy(expected, posInBuf, decodeInputs[j], 0, CELLSIZE);
        }
      }
      for (int j = DATA_BLK_NUM; j < DATA_BLK_NUM + PARITY_BLK_NUM; j++) {
        for (int k = 0; k < CELLSIZE; k++) {
          int posInBlk = i * CELLSIZE + k;
          decodeInputs[j][k] = SimulatedFSDataset.simulatedByte(
              new Block(bg.getBlock().getBlockId() + j), posInBlk);
        }
      }
      for (int m : missingBlkIdx) {
        decodeInputs[m] = null;
      }
      rawDecoder.decode(decodeInputs, missingBlkIdx, decodeOutputs);
      int posInBuf = i * CELLSIZE * DATA_BLK_NUM + failedDNIdx * CELLSIZE;
      System.arraycopy(decodeOutputs[0], 0, expected, posInBuf, CELLSIZE);
    }

    int delta = 10;
    int done = 0;
    // read a small delta, shouldn't trigger decode
    // |cell_0 |
    // |10     |
    done += in.read(0, readBuffer, 0, delta);
    assertEquals(delta, done);
    assertArrayEquals(Arrays.copyOf(expected, done),
        Arrays.copyOf(readBuffer, done));
    // both head and trail cells are partial
    // |c_0      |c_1    |c_2 |c_3 |c_4      |c_5         |
    // |256K - 10|missing|256K|256K|256K - 10|not in range|
    done += in.read(delta, readBuffer, delta,
        CELLSIZE * (DATA_BLK_NUM - 1) - 2 * delta);
    assertEquals(CELLSIZE * (DATA_BLK_NUM - 1) - delta, done);
    assertArrayEquals(Arrays.copyOf(expected, done),
        Arrays.copyOf(readBuffer, done));
    // read the rest
    done += in.read(done, readBuffer, done, readSize - done);
    assertEquals(readSize, done);
    assertArrayEquals(expected, readBuffer);
  }

  @Test
  public void testStatefulRead() throws Exception {
    testStatefulRead(false, false);
    testStatefulRead(true, false);
    testStatefulRead(true, true);
  }

  private void testStatefulRead(boolean useByteBuffer,
      boolean cellMisalignPacket) throws Exception {
    final int numBlocks = 2;
    final int fileSize = numBlocks * BLOCK_GROUP_SIZE;
    if (cellMisalignPacket) {
      conf.setInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT + 1);
      tearDown();
      setup();
    }
    DFSTestUtil.createStripedFile(cluster, filePath, null, numBlocks,
        NUM_STRIPE_PER_BLOCK, false);
    LocatedBlocks lbs = fs.getClient().namenode.
        getBlockLocations(filePath.toString(), 0, fileSize);

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
            false, ecPolicy, null);

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

  @Test
  public void testStatefulReadWithDNFailure() throws Exception {
    final int numBlocks = 4;
    final int failedDNIdx = DATA_BLK_NUM - 1;
    DFSTestUtil.createStripedFile(cluster, filePath, null, numBlocks,
        NUM_STRIPE_PER_BLOCK, false);
    LocatedBlocks lbs = fs.getClient().namenode.getBlockLocations(
        filePath.toString(), 0, BLOCK_GROUP_SIZE);

    assert lbs.get(0) instanceof LocatedStripedBlock;
    LocatedStripedBlock bg = (LocatedStripedBlock) (lbs.get(0));
    for (int i = 0; i < DATA_BLK_NUM + PARITY_BLK_NUM; i++) {
      Block blk = new Block(bg.getBlock().getBlockId() + i,
          NUM_STRIPE_PER_BLOCK * CELLSIZE,
          bg.getBlock().getGenerationStamp());
      blk.setGenerationStamp(bg.getBlock().getGenerationStamp());
      cluster.injectBlocks(i, Arrays.asList(blk),
          bg.getBlock().getBlockPoolId());
    }
    DFSStripedInputStream in =
        new DFSStripedInputStream(fs.getClient(), filePath.toString(), false,
            ecPolicy, null);
    int readSize = BLOCK_GROUP_SIZE;
    byte[] readBuffer = new byte[readSize];
    byte[] expected = new byte[readSize];
    /** A variation of {@link DFSTestUtil#fillExpectedBuf} for striped blocks */
    for (int i = 0; i < NUM_STRIPE_PER_BLOCK; i++) {
      for (int j = 0; j < DATA_BLK_NUM; j++) {
        for (int k = 0; k < CELLSIZE; k++) {
          int posInBlk = i * CELLSIZE + k;
          int posInFile = i * CELLSIZE * DATA_BLK_NUM + j * CELLSIZE + k;
          expected[posInFile] = SimulatedFSDataset.simulatedByte(
              new Block(bg.getBlock().getBlockId() + j), posInBlk);
        }
      }
    }

    ErasureCoderOptions coderOptions = new ErasureCoderOptions(
        DATA_BLK_NUM, PARITY_BLK_NUM);
    RawErasureDecoder rawDecoder = CodecUtil.createRawDecoder(conf,
        ecPolicy.getCodecName(), coderOptions);

    // Update the expected content for decoded data
    int[] missingBlkIdx = new int[PARITY_BLK_NUM];
    for (int i = 0; i < missingBlkIdx.length; i++) {
      if (i == 0) {
        missingBlkIdx[i] = failedDNIdx;
      } else {
        missingBlkIdx[i] = DATA_BLK_NUM + i;
      }
    }
    cluster.stopDataNode(failedDNIdx);
    for (int i = 0; i < NUM_STRIPE_PER_BLOCK; i++) {
      byte[][] decodeInputs = new byte[DATA_BLK_NUM + PARITY_BLK_NUM][CELLSIZE];
      byte[][] decodeOutputs = new byte[missingBlkIdx.length][CELLSIZE];
      for (int j = 0; j < DATA_BLK_NUM; j++) {
        int posInBuf = i * CELLSIZE * DATA_BLK_NUM + j * CELLSIZE;
        if (j != failedDNIdx) {
          System.arraycopy(expected, posInBuf, decodeInputs[j], 0, CELLSIZE);
        }
      }
      for (int j = DATA_BLK_NUM; j < DATA_BLK_NUM + PARITY_BLK_NUM; j++) {
        for (int k = 0; k < CELLSIZE; k++) {
          int posInBlk = i * CELLSIZE + k;
          decodeInputs[j][k] = SimulatedFSDataset.simulatedByte(
              new Block(bg.getBlock().getBlockId() + j), posInBlk);
        }
      }
      for (int m : missingBlkIdx) {
        decodeInputs[m] = null;
      }
      rawDecoder.decode(decodeInputs, missingBlkIdx, decodeOutputs);
      int posInBuf = i * CELLSIZE * DATA_BLK_NUM + failedDNIdx * CELLSIZE;
      System.arraycopy(decodeOutputs[0], 0, expected, posInBuf, CELLSIZE);
    }

    int delta = 10;
    int done = 0;
    // read a small delta, shouldn't trigger decode
    // |cell_0 |
    // |10     |
    done += in.read(readBuffer, 0, delta);
    assertEquals(delta, done);
    // both head and trail cells are partial
    // |c_0      |c_1    |c_2 |c_3 |c_4      |c_5         |
    // |256K - 10|missing|256K|256K|256K - 10|not in range|
    while (done < (CELLSIZE * (DATA_BLK_NUM - 1) - 2 * delta)) {
      int ret = in.read(readBuffer, delta,
          CELLSIZE * (DATA_BLK_NUM - 1) - 2 * delta);
      assertTrue(ret > 0);
      done += ret;
    }
    assertEquals(CELLSIZE * (DATA_BLK_NUM - 1) - delta, done);
    // read the rest

    int restSize;
    restSize = readSize - done;
    while (done < restSize) {
      int ret = in.read(readBuffer, done, restSize);
      assertTrue(ret > 0);
      done += ret;
    }

    assertEquals(readSize, done);
    assertArrayEquals(expected, readBuffer);
  }
}
