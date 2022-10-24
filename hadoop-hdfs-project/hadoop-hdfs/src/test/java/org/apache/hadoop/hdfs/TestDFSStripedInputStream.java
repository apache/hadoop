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

import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ErasureCodeNative;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.NativeRSRawErasureCoderFactory;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class TestDFSStripedInputStream {

  public static final Logger LOG =
      LoggerFactory.getLogger(TestDFSStripedInputStream.class);

  private MiniDFSCluster cluster;
  private Configuration conf = new Configuration();
  private DistributedFileSystem fs;
  private final Path dirPath = new Path("/striped");
  private Path filePath = new Path(dirPath, "file");
  private ErasureCodingPolicy ecPolicy;
  private short dataBlocks;
  private short parityBlocks;
  private int cellSize;
  private final int stripesPerBlock = 2;
  private int blockSize;
  private int blockGroupSize;

  @Rule
  public Timeout globalTimeout = new Timeout(300000);

  @Rule
  public TemporaryFolder baseDir = new TemporaryFolder();

  public ErasureCodingPolicy getEcPolicy() {
    return StripedFileTestUtil.getDefaultECPolicy();
  }

  @Before
  public void setup() throws IOException {
    /*
     * Initialize erasure coding policy.
     */
    ecPolicy = getEcPolicy();
    dataBlocks = (short) ecPolicy.getNumDataUnits();
    parityBlocks = (short) ecPolicy.getNumParityUnits();
    cellSize = ecPolicy.getCellSize();
    blockSize = stripesPerBlock * cellSize;
    blockGroupSize =  dataBlocks * blockSize;
    System.out.println("EC policy = " + ecPolicy);

    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 0);
    if (ErasureCodeNative.isNativeCodeLoaded()) {
      conf.set(
          CodecUtil.IO_ERASURECODE_CODEC_RS_RAWCODERS_KEY,
          NativeRSRawErasureCoderFactory.CODER_NAME);
    }
    SimulatedFSDataset.setFactory(conf);
    startUp();
  }

  private void startUp() throws IOException {
    cluster = new MiniDFSCluster.Builder(conf, baseDir.getRoot()).numDataNodes(
        dataBlocks + parityBlocks).build();
    cluster.waitActive();
    for (DataNode dn : cluster.getDataNodes()) {
      DataNodeTestUtils.setHeartbeatsDisabledForTests(dn, true);
    }
    fs = cluster.getFileSystem();
    fs.enableErasureCodingPolicy(getEcPolicy().getName());
    fs.mkdirs(dirPath);
    fs.getClient()
        .setErasureCodingPolicy(dirPath.toString(), ecPolicy.getName());
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * Test {@link DFSStripedInputStream#getBlockAt(long)}.
   */
  @Test
  public void testRefreshBlock() throws Exception {
    final int numBlocks = 4;
    DFSTestUtil.createStripedFile(cluster, filePath, null, numBlocks,
        stripesPerBlock, false, ecPolicy);
    LocatedBlocks lbs = fs.getClient().namenode.getBlockLocations(
        filePath.toString(), 0, blockGroupSize * numBlocks);
    final DFSStripedInputStream in = new DFSStripedInputStream(fs.getClient(),
        filePath.toString(), false, ecPolicy, null);

    List<LocatedBlock> lbList = lbs.getLocatedBlocks();
    for (LocatedBlock aLbList : lbList) {
      LocatedStripedBlock lsb = (LocatedStripedBlock) aLbList;
      LocatedBlock[] blks = StripedBlockUtil.parseStripedBlockGroup(lsb,
          cellSize, dataBlocks, parityBlocks);
      for (int j = 0; j < dataBlocks; j++) {
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
        stripesPerBlock, false, ecPolicy);
    LocatedBlocks lbs = fs.getClient().namenode.getBlockLocations(
        filePath.toString(), 0, blockGroupSize * numBlocks);
    int fileLen = blockGroupSize * numBlocks;

    byte[] expected = new byte[fileLen];
    assertEquals(numBlocks, lbs.getLocatedBlocks().size());
    for (int bgIdx = 0; bgIdx < numBlocks; bgIdx++) {
      LocatedStripedBlock bg = (LocatedStripedBlock) (lbs.get(bgIdx));
      for (int i = 0; i < dataBlocks; i++) {
        Block blk = new Block(bg.getBlock().getBlockId() + i,
            stripesPerBlock * cellSize,
            bg.getBlock().getGenerationStamp());
        blk.setGenerationStamp(bg.getBlock().getGenerationStamp());
        cluster.injectBlocks(i, Arrays.asList(blk),
            bg.getBlock().getBlockPoolId());
      }

      /**
       * A variation of {@link DFSTestUtil#fillExpectedBuf} for striped blocks
       */
      for (int i = 0; i < stripesPerBlock; i++) {
        for (int j = 0; j < dataBlocks; j++) {
          for (int k = 0; k < cellSize; k++) {
            int posInBlk = i * cellSize + k;
            int posInFile = i * cellSize * dataBlocks + j * cellSize + k;
            expected[bgIdx * blockGroupSize + posInFile] =
                SimulatedFSDataset.simulatedByte(
                    new Block(bg.getBlock().getBlockId() + j), posInBlk);
          }
        }
      }
    }
    DFSStripedInputStream in = new DFSStripedInputStream(fs.getClient(),
        filePath.toString(), false, ecPolicy, null);

    int[] startOffsets = {0, 1, cellSize - 102, cellSize, cellSize + 102,
        cellSize * dataBlocks, cellSize * dataBlocks + 102,
        blockGroupSize - 102, blockGroupSize, blockGroupSize + 102,
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
    final int failedDNIdx = dataBlocks - 1;
    DFSTestUtil.createStripedFile(cluster, filePath, null, numBlocks,
        stripesPerBlock, false, ecPolicy);
    LocatedBlocks lbs = fs.getClient().namenode.getBlockLocations(
        filePath.toString(), 0, blockGroupSize);

    assert lbs.get(0) instanceof LocatedStripedBlock;
    LocatedStripedBlock bg = (LocatedStripedBlock)(lbs.get(0));
    for (int i = 0; i < dataBlocks + parityBlocks; i++) {
      Block blk = new Block(bg.getBlock().getBlockId() + i,
          stripesPerBlock * cellSize,
          bg.getBlock().getGenerationStamp());
      blk.setGenerationStamp(bg.getBlock().getGenerationStamp());
      cluster.injectBlocks(i, Arrays.asList(blk),
          bg.getBlock().getBlockPoolId());
    }
    DFSStripedInputStream in =
        new DFSStripedInputStream(fs.getClient(), filePath.toString(), false,
            ecPolicy, null);
    int readSize = blockGroupSize;
    byte[] readBuffer = new byte[readSize];
    byte[] expected = new byte[readSize];
    /** A variation of {@link DFSTestUtil#fillExpectedBuf} for striped blocks */
    for (int i = 0; i < stripesPerBlock; i++) {
      for (int j = 0; j < dataBlocks; j++) {
        for (int k = 0; k < cellSize; k++) {
          int posInBlk = i * cellSize + k;
          int posInFile = i * cellSize * dataBlocks + j * cellSize + k;
          expected[posInFile] = SimulatedFSDataset.simulatedByte(
              new Block(bg.getBlock().getBlockId() + j), posInBlk);
        }
      }
    }

    ErasureCoderOptions coderOptions = new ErasureCoderOptions(
        dataBlocks, parityBlocks);
    RawErasureDecoder rawDecoder = CodecUtil.createRawDecoder(conf,
        ecPolicy.getCodecName(), coderOptions);

    // Update the expected content for decoded data
    int[] missingBlkIdx = new int[parityBlocks];
    for (int i = 0; i < missingBlkIdx.length; i++) {
      if (i == 0) {
        missingBlkIdx[i] = failedDNIdx;
      } else {
        missingBlkIdx[i] = dataBlocks + i;
      }
    }
    cluster.stopDataNode(failedDNIdx);
    for (int i = 0; i < stripesPerBlock; i++) {
      byte[][] decodeInputs = new byte[dataBlocks + parityBlocks][cellSize];
      byte[][] decodeOutputs = new byte[missingBlkIdx.length][cellSize];
      for (int j = 0; j < dataBlocks; j++) {
        int posInBuf = i * cellSize * dataBlocks + j * cellSize;
        if (j != failedDNIdx) {
          System.arraycopy(expected, posInBuf, decodeInputs[j], 0, cellSize);
        }
      }
      for (int j = dataBlocks; j < dataBlocks + parityBlocks; j++) {
        for (int k = 0; k < cellSize; k++) {
          int posInBlk = i * cellSize + k;
          decodeInputs[j][k] = SimulatedFSDataset.simulatedByte(
              new Block(bg.getBlock().getBlockId() + j), posInBlk);
        }
      }
      for (int m : missingBlkIdx) {
        decodeInputs[m] = null;
      }
      rawDecoder.decode(decodeInputs, missingBlkIdx, decodeOutputs);
      int posInBuf = i * cellSize * dataBlocks + failedDNIdx * cellSize;
      System.arraycopy(decodeOutputs[0], 0, expected, posInBuf, cellSize);
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
        cellSize * (dataBlocks - 1) - 2 * delta);
    assertEquals(cellSize * (dataBlocks - 1) - delta, done);
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
    final int fileSize = numBlocks * blockGroupSize;
    if (cellMisalignPacket) {
      conf.setInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT + 1);
      tearDown();
      startUp();
    }
    DFSTestUtil.createStripedFile(cluster, filePath, null, numBlocks,
        stripesPerBlock, false, ecPolicy);
    LocatedBlocks lbs = fs.getClient().namenode.
        getBlockLocations(filePath.toString(), 0, fileSize);

    assert lbs.getLocatedBlocks().size() == numBlocks;
    for (LocatedBlock lb : lbs.getLocatedBlocks()) {
      assert lb instanceof LocatedStripedBlock;
      LocatedStripedBlock bg = (LocatedStripedBlock)(lb);
      for (int i = 0; i < dataBlocks; i++) {
        Block blk = new Block(bg.getBlock().getBlockId() + i,
            stripesPerBlock * cellSize,
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
      /**
       * A variation of {@link DFSTestUtil#fillExpectedBuf} for striped blocks
       */
      for (int i = 0; i < stripesPerBlock; i++) {
        for (int j = 0; j < dataBlocks; j++) {
          for (int k = 0; k < cellSize; k++) {
            int posInBlk = i * cellSize + k;
            int posInFile = (int) bg.getStartOffset() +
                i * cellSize * dataBlocks + j * cellSize + k;
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
    final int failedDNIdx = dataBlocks - 1;
    DFSTestUtil.createStripedFile(cluster, filePath, null, numBlocks,
        stripesPerBlock, false, ecPolicy);
    LocatedBlocks lbs = fs.getClient().namenode.getBlockLocations(
        filePath.toString(), 0, blockGroupSize);

    assert lbs.get(0) instanceof LocatedStripedBlock;
    LocatedStripedBlock bg = (LocatedStripedBlock) (lbs.get(0));
    for (int i = 0; i < dataBlocks + parityBlocks; i++) {
      Block blk = new Block(bg.getBlock().getBlockId() + i,
          stripesPerBlock * cellSize,
          bg.getBlock().getGenerationStamp());
      blk.setGenerationStamp(bg.getBlock().getGenerationStamp());
      cluster.injectBlocks(i, Arrays.asList(blk),
          bg.getBlock().getBlockPoolId());
    }
    DFSStripedInputStream in =
        new DFSStripedInputStream(fs.getClient(), filePath.toString(), false,
            ecPolicy, null);
    int readSize = blockGroupSize;
    byte[] readBuffer = new byte[readSize];
    byte[] expected = new byte[readSize];
    /** A variation of {@link DFSTestUtil#fillExpectedBuf} for striped blocks */
    for (int i = 0; i < stripesPerBlock; i++) {
      for (int j = 0; j < dataBlocks; j++) {
        for (int k = 0; k < cellSize; k++) {
          int posInBlk = i * cellSize + k;
          int posInFile = i * cellSize * dataBlocks + j * cellSize + k;
          expected[posInFile] = SimulatedFSDataset.simulatedByte(
              new Block(bg.getBlock().getBlockId() + j), posInBlk);
        }
      }
    }

    ErasureCoderOptions coderOptions = new ErasureCoderOptions(
        dataBlocks, parityBlocks);
    RawErasureDecoder rawDecoder = CodecUtil.createRawDecoder(conf,
        ecPolicy.getCodecName(), coderOptions);

    // Update the expected content for decoded data
    int[] missingBlkIdx = new int[parityBlocks];
    for (int i = 0; i < missingBlkIdx.length; i++) {
      if (i == 0) {
        missingBlkIdx[i] = failedDNIdx;
      } else {
        missingBlkIdx[i] = dataBlocks + i;
      }
    }
    cluster.stopDataNode(failedDNIdx);
    for (int i = 0; i < stripesPerBlock; i++) {
      byte[][] decodeInputs = new byte[dataBlocks + parityBlocks][cellSize];
      byte[][] decodeOutputs = new byte[missingBlkIdx.length][cellSize];
      for (int j = 0; j < dataBlocks; j++) {
        int posInBuf = i * cellSize * dataBlocks + j * cellSize;
        if (j != failedDNIdx) {
          System.arraycopy(expected, posInBuf, decodeInputs[j], 0, cellSize);
        }
      }
      for (int j = dataBlocks; j < dataBlocks + parityBlocks; j++) {
        for (int k = 0; k < cellSize; k++) {
          int posInBlk = i * cellSize + k;
          decodeInputs[j][k] = SimulatedFSDataset.simulatedByte(
              new Block(bg.getBlock().getBlockId() + j), posInBlk);
        }
      }
      for (int m : missingBlkIdx) {
        decodeInputs[m] = null;
      }
      rawDecoder.decode(decodeInputs, missingBlkIdx, decodeOutputs);
      int posInBuf = i * cellSize * dataBlocks + failedDNIdx * cellSize;
      System.arraycopy(decodeOutputs[0], 0, expected, posInBuf, cellSize);
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
    while (done < (cellSize * (dataBlocks - 1) - 2 * delta)) {
      int ret = in.read(readBuffer, delta,
          cellSize * (dataBlocks - 1) - 2 * delta);
      assertTrue(ret > 0);
      done += ret;
    }
    assertEquals(cellSize * (dataBlocks - 1) - delta, done);
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

  @Test
  public void testIdempotentClose() throws Exception {
    final int numBlocks = 2;
    DFSTestUtil.createStripedFile(cluster, filePath, null, numBlocks,
        stripesPerBlock, false, ecPolicy);

    try (DFSInputStream in = fs.getClient().open(filePath.toString())) {
      assertTrue(in instanceof DFSStripedInputStream);
      // Close twice
      in.close();
    }
  }

  @Test
  public void testReadFailToGetCurrentBlock() throws Exception {
    DFSTestUtil.writeFile(cluster.getFileSystem(), filePath, "test");
    try (DFSStripedInputStream in = (DFSStripedInputStream) fs.getClient()
        .open(filePath.toString())) {
      final DFSStripedInputStream spy = spy(in);
      final String msg = "Injected exception for testReadNPE";
      doThrow(new IOException(msg)).when(spy).blockSeekTo(anyLong());
      assertNull(in.getCurrentBlock());
      try {
        spy.read();
        fail("read should have failed");
      } catch (IOException expected) {
        LOG.info("Exception caught", expected);
        GenericTestUtils.assertExceptionContains(msg, expected);
      }
    }
  }

  @Test
  public void testCloseDoesNotAllocateNewBuffer() throws Exception {
    final int numBlocks = 2;
    DFSTestUtil.createStripedFile(cluster, filePath, null, numBlocks,
        stripesPerBlock, false, ecPolicy);
    try (DFSInputStream in = fs.getClient().open(filePath.toString())) {
      assertTrue(in instanceof DFSStripedInputStream);
      final DFSStripedInputStream stream = (DFSStripedInputStream) in;
      final ElasticByteBufferPool ebbp =
          (ElasticByteBufferPool) stream.getBufferPool();
      // first clear existing pool
      LOG.info("Current pool size: direct: " + ebbp.size(true) + ", indirect: "
          + ebbp.size(false));
      emptyBufferPoolForCurrentPolicy(ebbp, true);
      emptyBufferPoolForCurrentPolicy(ebbp, false);
      final int startSizeDirect = ebbp.size(true);
      final int startSizeIndirect = ebbp.size(false);
      // close should not allocate new buffers in the pool.
      stream.close();
      assertEquals(startSizeDirect, ebbp.size(true));
      assertEquals(startSizeIndirect, ebbp.size(false));
    }
  }

  @Test
  public void testReadWhenLastIncompleteCellComeInToDecodeAlignedStripe()
      throws IOException {
    DataNodeProperties stopDataNode = null;
    try {
      cluster.waitActive();
      ErasureCodingPolicy policy = getEcPolicy();
      DistributedFileSystem filesystem = cluster.getFileSystem();
      filesystem.enableErasureCodingPolicy(policy.getName());
      Path dir = new Path("/tmp");
      filesystem.mkdirs(dir);
      filesystem.getClient().setErasureCodingPolicy(dir.toString(),
          policy.getName());
      Path f = new Path(dir, "file");

      //1. File with one stripe, last data cell should be half filed.
      long fileLength = (policy.getCellSize() * policy.getNumDataUnits())
          - (policy.getCellSize() / 2);
      DFSTestUtil.createFile(filesystem, f, fileLength, (short) 1, 0);

      //2. Stop first DN from stripe.
      LocatedBlocks lbs = cluster.getNameNodeRpc().getBlockLocations(
          f.toString(), 0, fileLength);
      LocatedStripedBlock bg = (LocatedStripedBlock) (lbs.get(0));
      final LocatedBlock[] blocks = StripedBlockUtil.parseStripedBlockGroup(bg,
          cellSize, dataBlocks, parityBlocks);
      cluster.stopDataNode(blocks[0].getLocations()[0].getName());

      //3. Do pread for fist cell, reconstruction should happen
      try (FSDataInputStream in = filesystem.open(f)) {
        DFSStripedInputStream stripedIn = (DFSStripedInputStream) in
            .getWrappedStream();
        byte[] b = new byte[policy.getCellSize()];
        stripedIn.read(0, b, 0, policy.getCellSize());
      }
    } catch (HadoopIllegalArgumentException e) {
      fail(e.getMessage());
    } finally {
      if (stopDataNode != null) {
        cluster.restartDataNode(stopDataNode, true);
      }
    }
  }

  /**
   * Empties the pool for the specified buffer type, for the current ecPolicy.
   * <p>
   * Note that {@link #ecPolicy} may change for difference test cases in
   * {@link TestDFSStripedInputStreamWithRandomECPolicy}.
   */
  private void emptyBufferPoolForCurrentPolicy(ElasticByteBufferPool ebbp,
      boolean direct) {
    int size;
    while ((size = ebbp.size(direct)) != 0) {
      ebbp.getBuffer(direct,
          ecPolicy.getCellSize() * ecPolicy.getNumDataUnits());
      if (size == ebbp.size(direct)) {
        // if getBuffer didn't decrease size, it means the pool for the buffer
        // corresponding to current ecPolicy is empty
        break;
      }
    }
  }

  @Test
  public void testUnbuffer() throws Exception {
    final int numBlocks = 2;
    final int fileSize = numBlocks * blockGroupSize;
    DFSTestUtil.createStripedFile(cluster, filePath, null, numBlocks,
        stripesPerBlock, false, ecPolicy);
    LocatedBlocks lbs = fs.getClient().namenode.
        getBlockLocations(filePath.toString(), 0, fileSize);

    for (LocatedBlock lb : lbs.getLocatedBlocks()) {
      assert lb instanceof LocatedStripedBlock;
      LocatedStripedBlock bg = (LocatedStripedBlock)(lb);
      for (int i = 0; i < dataBlocks; i++) {
        Block blk = new Block(bg.getBlock().getBlockId() + i,
            stripesPerBlock * cellSize,
            bg.getBlock().getGenerationStamp());
        blk.setGenerationStamp(bg.getBlock().getGenerationStamp());
        cluster.injectBlocks(i, Arrays.asList(blk),
            bg.getBlock().getBlockPoolId());
      }
    }
      DFSStripedInputStream in = new DFSStripedInputStream(fs.getClient(),
          filePath.toString(), false, ecPolicy, null);
      ByteBuffer readBuffer = ByteBuffer.allocate(fileSize);
      int done = 0;
      while (done < fileSize) {
        int ret = in.read(readBuffer);
        assertTrue(ret > 0);
        done += ret;
      }
      in.unbuffer();
      ByteBuffer curStripeBuf = (in.getCurStripeBuf());
      assertNull(curStripeBuf);
      assertNull(in.parityBuf);
      in.close();
  }

  @Test
  public void testBlockReader() throws Exception {
    ErasureCodingPolicy targetPolicy = StripedFileTestUtil.getDefaultECPolicy(); // RS-6-3-1024k
    if (!ecPolicy.equals(targetPolicy)) {
      // Be sure not affected by random EC policy from
      // TestDFSStripedInputStreamWithRandomECPolicy.
      return;
    }
    int fileSize = 19 * cellSize + 100;
    long stripeSize = (long) dataBlocks * cellSize;
    byte[] bytes = StripedFileTestUtil.generateBytes(fileSize);
    DFSTestUtil.writeFile(fs, filePath, new String(bytes));

    try (DFSStripedInputStream in =
             (DFSStripedInputStream) fs.getClient().open(filePath.toString())) {
      // Verify pread:
      verifyPreadRanges(in, 0, 2 * cellSize,
          2 * cellSize, Arrays.asList("0_0_1048576", "1_0_1048576"));
      verifyPreadRanges(in, 0, 5 * cellSize + 9527,
          5 * cellSize + 9527, Arrays.asList("0_0_1048576", "1_0_1048576",
              "2_0_1048576", "3_0_1048576", "4_0_1048576", "5_0_1048576"));
      verifyPreadRanges(in, 100, 5 * cellSize + 9527,
          5 * cellSize + 9527, Arrays.asList("0_100_1048476", "1_0_1048576",
              "2_0_1048576", "3_0_1048576", "4_0_1048576", "5_0_1048576"));
      verifyPreadRanges(in, stripeSize * 3, 2 * cellSize,
          cellSize + 100, Arrays.asList("0_1048576_1048576", "1_1048576_100"));

      // Verify sread:
      verifySreadRanges(in, 0, Arrays.asList("0_0_2097152", "1_0_2097152",
          "2_0_2097152", "3_0_2097152", "4_0_2097152", "5_0_2097152"));
      verifySreadRanges(in, stripeSize * 2, Arrays.asList("0_0_2097152", "1_0_1048676",
          "2_0_1048576", "3_0_1048576", "4_0_1048576", "5_0_1048576"));
    }
  }

  private void verifyPreadRanges(DFSStripedInputStream in, long position,
                                 int length, int lengthExpected,
                                 List<String> rangesExpected) throws Exception {
    List<String> ranges = new ArrayList<>(); // range format: chunkIndex_offset_len
    DFSClientFaultInjector.set(new DFSClientFaultInjector() {
      @Override
      public void onCreateBlockReader(LocatedBlock block, int chunkIndex,
                                      long offset, long length) {
        ranges.add(String.format("%s_%s_%s", chunkIndex, offset, length));
      }
    });
    assertEquals(lengthExpected, in.read(position, new byte[length], 0, length));
    Collections.sort(ranges);
    Collections.sort(rangesExpected);
    assertEquals(rangesExpected, ranges);
  }

  private void verifySreadRanges(DFSStripedInputStream in, long position,
                                 List<String> rangesExpected) throws Exception {
    List<String> ranges = new ArrayList<>(); // range format: chunkIndex_offset_len
    DFSClientFaultInjector.set(new DFSClientFaultInjector() {
      @Override
      public void onCreateBlockReader(LocatedBlock block, int chunkIndex,
                                      long offset, long length) {
        ranges.add(String.format("%s_%s_%s", chunkIndex, offset, length));
      }
    });
    in.seek(position);
    int length = in.read(new byte[1024]);
    assertEquals(1024, length);
    Collections.sort(ranges);
    Collections.sort(rangesExpected);
    assertEquals(rangesExpected, ranges);
  }

}
