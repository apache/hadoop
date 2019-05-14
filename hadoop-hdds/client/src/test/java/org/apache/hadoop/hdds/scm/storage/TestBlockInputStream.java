/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.storage;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ChecksumData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ChecksumType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Tests {@link BlockInputStream}.
 */
public class TestBlockInputStream {

  private static BlockInputStream blockInputStream;
  private static List<ChunkInfo> chunks;
  private static int blockSize;

  private static final int CHUNK_SIZE = 20;

  @Before
  public void setup() throws Exception {
    BlockID blockID = new BlockID(new ContainerBlockID(1, 1));
    chunks = createChunkList(10);
    String traceID = UUID.randomUUID().toString();
    blockInputStream = new DummyBlockInputStream(blockID, null, null, chunks,
        traceID, false, 0);

    blockSize = 0;
    for (ChunkInfo chunk : chunks) {
      blockSize += chunk.getLen();
    }
  }

  /**
   * Create a mock list of chunks. The first n-1 chunks of length CHUNK_SIZE
   * and the last chunk with length CHUNK_SIZE/2.
   * @param numChunks
   * @return
   */
  private static List<ChunkInfo> createChunkList(int numChunks) {
    ChecksumData dummyChecksumData = ChecksumData.newBuilder()
        .setType(ChecksumType.NONE)
        .setBytesPerChecksum(100)
        .build();
    List<ChunkInfo> chunkList = new ArrayList<>(numChunks);
    int i;
    for (i = 0; i < numChunks - 1; i++) {
      String chunkName = "chunk-" + i;
      ChunkInfo chunkInfo = ChunkInfo.newBuilder()
          .setChunkName(chunkName)
          .setOffset(0)
          .setLen(CHUNK_SIZE)
          .setChecksumData(dummyChecksumData)
          .build();
      chunkList.add(chunkInfo);
    }
    ChunkInfo chunkInfo = ChunkInfo.newBuilder()
        .setChunkName("chunk-" + i)
        .setOffset(0)
        .setLen(CHUNK_SIZE/2)
        .setChecksumData(dummyChecksumData)
        .build();
    chunkList.add(chunkInfo);

    return chunkList;
  }

  /**
   * A dummy BlockInputStream to test the functionality of BlockInputStream.
   */
  private static class DummyBlockInputStream extends BlockInputStream {

    DummyBlockInputStream(BlockID blockID,
        XceiverClientManager xceiverClientManager,
        XceiverClientSpi xceiverClient,
        List<ChunkInfo> chunks,
        String traceID,
        boolean verifyChecksum,
        long initialPosition) throws IOException {
      super(blockID, xceiverClientManager, xceiverClient, chunks, traceID,
          verifyChecksum, initialPosition);
    }

    @Override
    protected ByteString readChunk(final ChunkInfo chunkInfo,
        List<DatanodeDetails> excludeDns, List<DatanodeDetails> dnListFromReply)
        throws IOException {
      return getByteString(chunkInfo.getChunkName(), (int) chunkInfo.getLen());
    }

    @Override
    protected List<DatanodeDetails> getDatanodeList() {
      // return an empty dummy list of size 10
      return new ArrayList<>(10);
    }

    /**
     * Create ByteString with the input data to return when a readChunk call is
     * placed.
     */
    private static ByteString getByteString(String data, int length) {
      while (data.length() < length) {
        data = data + "0";
      }
      return ByteString.copyFrom(data.getBytes(), 0, length);
    }
  }

  @Test
  public void testSeek() throws Exception {
    // Seek to position 0
    int pos = 0;
    seekAndVerify(pos);
    Assert.assertEquals("ChunkIndex is incorrect", 0,
        blockInputStream.getChunkIndex());

    pos = CHUNK_SIZE;
    seekAndVerify(pos);
    Assert.assertEquals("ChunkIndex is incorrect", 1,
        blockInputStream.getChunkIndex());

    pos = (CHUNK_SIZE * 5) + 5;
    seekAndVerify(pos);
    Assert.assertEquals("ChunkIndex is incorrect", 5,
        blockInputStream.getChunkIndex());

    try {
      // Try seeking beyond the blockSize.
      pos = blockSize + 10;
      seekAndVerify(pos);
      Assert.fail("Seek to position beyond block size should fail.");
    } catch (EOFException e) {
      // Expected
    }

    // Seek to random positions between 0 and the block size.
    Random random = new Random();
    for (int i = 0; i < 10; i++) {
      pos = random.nextInt(blockSize);
      seekAndVerify(pos);
    }
  }

  @Test
  public void testBlockEOF() throws Exception {
    // Seek to some position < blockSize and verify EOF is not reached.
    seekAndVerify(CHUNK_SIZE);
    Assert.assertFalse(blockInputStream.blockStreamEOF());

    // Seek to blockSize-1 and verify that EOF is not reached as the chunk
    // has not been read from container yet.
    seekAndVerify(blockSize-1);
    Assert.assertFalse(blockInputStream.blockStreamEOF());
  }

  private void seekAndVerify(int pos) throws Exception {
    blockInputStream.seek(pos);
    Assert.assertEquals("Current position of buffer does not match with the " +
            "seeked position", pos, blockInputStream.getPos());
  }
}
