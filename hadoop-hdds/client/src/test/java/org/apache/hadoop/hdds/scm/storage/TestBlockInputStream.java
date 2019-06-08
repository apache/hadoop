/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdds.scm.storage;

import com.google.common.primitives.Bytes;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.security.token.Token;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.apache.hadoop.hdds.scm.storage.TestChunkInputStream.generateRandomData;

/**
 * Tests for {@link BlockInputStream}'s functionality.
 */
public class TestBlockInputStream {

  private static final int CHUNK_SIZE = 100;
  private static Checksum checksum;

  private BlockInputStream blockStream;
  private byte[] blockData;
  private int blockSize;
  private List<ChunkInfo> chunks;
  private Map<String, byte[]> chunkDataMap;

  @Before
  public void setup() throws Exception {
    BlockID blockID = new BlockID(new ContainerBlockID(1, 1));
    checksum = new Checksum(ChecksumType.NONE, CHUNK_SIZE);
    createChunkList(5);

    blockStream = new DummyBlockInputStream(blockID, blockSize, null, null,
        false, null);
  }

  /**
   * Create a mock list of chunks. The first n-1 chunks of length CHUNK_SIZE
   * and the last chunk with length CHUNK_SIZE/2.
   */
  private void createChunkList(int numChunks)
      throws Exception {

    chunks = new ArrayList<>(numChunks);
    chunkDataMap = new HashMap<>();
    blockData = new byte[0];
    int i, chunkLen;
    byte[] byteData;
    String chunkName;

    for (i = 0; i < numChunks; i++) {
      chunkName = "chunk-" + i;
      chunkLen = CHUNK_SIZE;
      if (i == numChunks - 1) {
        chunkLen = CHUNK_SIZE / 2;
      }
      byteData = generateRandomData(chunkLen);
      ChunkInfo chunkInfo = ChunkInfo.newBuilder()
          .setChunkName(chunkName)
          .setOffset(0)
          .setLen(chunkLen)
          .setChecksumData(checksum.computeChecksum(
              byteData, 0, chunkLen).getProtoBufMessage())
          .build();

      chunkDataMap.put(chunkName, byteData);
      chunks.add(chunkInfo);

      blockSize += chunkLen;
      blockData = Bytes.concat(blockData, byteData);
    }
  }

  /**
   * A dummy BlockInputStream to mock read block call to DN.
   */
  private class DummyBlockInputStream extends BlockInputStream {

    DummyBlockInputStream(BlockID blockId,
        long blockLen,
        Pipeline pipeline,
        Token<OzoneBlockTokenIdentifier> token,
        boolean verifyChecksum,
        XceiverClientManager xceiverClientManager) {
      super(blockId, blockLen, pipeline, token, verifyChecksum,
          xceiverClientManager);
    }

    @Override
    protected List<ChunkInfo> getChunkInfos() {
      return chunks;
    }

    @Override
    protected void addStream(ChunkInfo chunkInfo) {
      TestChunkInputStream testChunkInputStream = new TestChunkInputStream();
      getChunkStreams().add(testChunkInputStream.new DummyChunkInputStream(
          chunkInfo, null, null, false,
          chunkDataMap.get(chunkInfo.getChunkName()).clone()));
    }

    @Override
    protected synchronized void checkOpen() throws IOException {
      // No action needed
    }
  }

  private void seekAndVerify(int pos) throws Exception {
    blockStream.seek(pos);
    Assert.assertEquals("Current position of buffer does not match with the " +
        "seeked position", pos, blockStream.getPos());
  }

  /**
   * Match readData with the chunkData byte-wise.
   * @param readData Data read through ChunkInputStream
   * @param inputDataStartIndex first index (inclusive) in chunkData to compare
   *                            with read data
   * @param length the number of bytes of data to match starting from
   *               inputDataStartIndex
   */
  private void matchWithInputData(byte[] readData, int inputDataStartIndex,
      int length) {
    for (int i = inputDataStartIndex; i < inputDataStartIndex + length; i++) {
      Assert.assertEquals(blockData[i], readData[i - inputDataStartIndex]);
    }
  }

  @Test
  public void testSeek() throws Exception {
    // Seek to position 0
    int pos = 0;
    seekAndVerify(pos);
    Assert.assertEquals("ChunkIndex is incorrect", 0,
        blockStream.getChunkIndex());

    // Before BlockInputStream is initialized (initialization happens during
    // read operation), seek should update the BlockInputStream#blockPosition
    pos = CHUNK_SIZE;
    seekAndVerify(pos);
    Assert.assertEquals("ChunkIndex is incorrect", 0,
        blockStream.getChunkIndex());
    Assert.assertEquals(pos, blockStream.getBlockPosition());

    // Initialize the BlockInputStream. After initializtion, the chunkIndex
    // should be updated to correspond to the seeked position.
    blockStream.initialize();
    Assert.assertEquals("ChunkIndex is incorrect", 1,
        blockStream.getChunkIndex());

    pos = (CHUNK_SIZE * 4) + 5;
    seekAndVerify(pos);
    Assert.assertEquals("ChunkIndex is incorrect", 4,
        blockStream.getChunkIndex());

    try {
      // Try seeking beyond the blockSize.
      pos = blockSize + 10;
      seekAndVerify(pos);
      Assert.fail("Seek to position beyond block size should fail.");
    } catch (EOFException e) {
      System.out.println(e);
    }

    // Seek to random positions between 0 and the block size.
    Random random = new Random();
    for (int i = 0; i < 10; i++) {
      pos = random.nextInt(blockSize);
      seekAndVerify(pos);
    }
  }

  @Test
  public void testRead() throws Exception {
    // read 200 bytes of data starting from position 50. Chunk0 contains
    // indices 0 to 99, chunk1 from 100 to 199 and chunk3 from 200 to 299. So
    // the read should result in 3 ChunkInputStream reads
    seekAndVerify(50);
    byte[] b = new byte[200];
    blockStream.read(b, 0, 200);
    matchWithInputData(b, 50, 200);

    // The new position of the blockInputStream should be the last index read
    // + 1.
    Assert.assertEquals(250, blockStream.getPos());
    Assert.assertEquals(2, blockStream.getChunkIndex());
  }

  @Test
  public void testSeekAndRead() throws Exception {
    // Seek to a position and read data
    seekAndVerify(50);
    byte[] b1 = new byte[100];
    blockStream.read(b1, 0, 100);
    matchWithInputData(b1, 50, 100);

    // Next read should start from the position of the last read + 1 i.e. 100
    byte[] b2 = new byte[100];
    blockStream.read(b2, 0, 100);
    matchWithInputData(b2, 150, 100);
  }
}
