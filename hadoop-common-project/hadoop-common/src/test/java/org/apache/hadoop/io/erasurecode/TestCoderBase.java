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
package org.apache.hadoop.io.erasurecode;

import org.apache.hadoop.conf.Configuration;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertTrue;

/**
 * Test base of common utilities for tests not only raw coders but also block
 * coders.
 */
public abstract class TestCoderBase {
  protected static Random RAND = new Random();

  private Configuration conf;
  protected int numDataUnits;
  protected int numParityUnits;
  protected int baseChunkSize = 16 * 1024;
  private int chunkSize = baseChunkSize;

  private byte[] zeroChunkBytes;

  private boolean startBufferWithZero = true;

  // Indexes of erased data units.
  protected int[] erasedDataIndexes = new int[] {0};

  // Indexes of erased parity units.
  protected int[] erasedParityIndexes = new int[] {0};

  // Data buffers are either direct or on-heap, for performance the two cases
  // may go to different coding implementations.
  protected boolean usingDirectBuffer = true;

  protected int getChunkSize() {
    return chunkSize;
  }

  protected void setChunkSize(int chunkSize) {
    this.chunkSize = chunkSize;
    this.zeroChunkBytes = new byte[chunkSize]; // With ZERO by default
  }

  /**
   * Prepare before running the case.
   * @param numDataUnits
   * @param numParityUnits
   * @param erasedDataIndexes
   */
  protected void prepare(Configuration conf, int numDataUnits,
                         int numParityUnits, int[] erasedDataIndexes,
                         int[] erasedParityIndexes) {
    this.conf = conf;
    this.numDataUnits = numDataUnits;
    this.numParityUnits = numParityUnits;
    this.erasedDataIndexes = erasedDataIndexes != null ?
        erasedDataIndexes : new int[] {0};
    this.erasedParityIndexes = erasedParityIndexes != null ?
        erasedParityIndexes : new int[] {0};
  }

  /**
   * Get the conf the test.
   * @return configuration
   */
  protected Configuration getConf() {
    return this.conf;
  }

  /**
   * Compare and verify if erased chunks are equal to recovered chunks
   * @param erasedChunks
   * @param recoveredChunks
   */
  protected void compareAndVerify(ECChunk[] erasedChunks,
                                  ECChunk[] recoveredChunks) {
    byte[][] erased = toArrays(erasedChunks);
    byte[][] recovered = toArrays(recoveredChunks);
    boolean result = Arrays.deepEquals(erased, recovered);
    assertTrue("Decoding and comparing failed.", result);
  }

  /**
   * Adjust and return erased indexes altogether, including erased data indexes
   * and parity indexes.
   * @return erased indexes altogether
   */
  protected int[] getErasedIndexesForDecoding() {
    int[] erasedIndexesForDecoding =
        new int[erasedParityIndexes.length + erasedDataIndexes.length];

    int idx = 0;

    for (int i = 0; i < erasedParityIndexes.length; i++) {
      erasedIndexesForDecoding[idx ++] = erasedParityIndexes[i];
    }

    for (int i = 0; i < erasedDataIndexes.length; i++) {
      erasedIndexesForDecoding[idx ++] = erasedDataIndexes[i] + numParityUnits;
    }

    return erasedIndexesForDecoding;
  }

  /**
   * Return input chunks for decoding, which is parityChunks + dataChunks.
   * @param dataChunks
   * @param parityChunks
   * @return
   */
  protected ECChunk[] prepareInputChunksForDecoding(ECChunk[] dataChunks,
                                                  ECChunk[] parityChunks) {
    ECChunk[] inputChunks = new ECChunk[numParityUnits + numDataUnits];
    
    int idx = 0;
    for (int i = 0; i < numParityUnits; i++) {
      inputChunks[idx ++] = parityChunks[i];
    }
    for (int i = 0; i < numDataUnits; i++) {
      inputChunks[idx ++] = dataChunks[i];
    }
    
    return inputChunks;
  }

  /**
   * Erase chunks to test the recovering of them. Before erasure clone them
   * first so could return them.
   * @param dataChunks
   * @param parityChunks
   * @return clone of erased chunks
   */
  protected ECChunk[] backupAndEraseChunks(ECChunk[] dataChunks,
                                      ECChunk[] parityChunks) {
    ECChunk[] toEraseChunks = new ECChunk[erasedParityIndexes.length +
        erasedDataIndexes.length];

    int idx = 0;
    ECChunk chunk;

    for (int i = 0; i < erasedParityIndexes.length; i++) {
      chunk = parityChunks[erasedParityIndexes[i]];
      toEraseChunks[idx ++] = cloneChunkWithData(chunk);
      eraseDataFromChunk(chunk);
    }

    for (int i = 0; i < erasedDataIndexes.length; i++) {
      chunk = dataChunks[erasedDataIndexes[i]];
      toEraseChunks[idx ++] = cloneChunkWithData(chunk);
      eraseDataFromChunk(chunk);
    }

    return toEraseChunks;
  }

  /**
   * Erase data from the specified chunks, putting ZERO bytes to the buffers.
   * @param chunks
   */
  protected void eraseDataFromChunks(ECChunk[] chunks) {
    for (int i = 0; i < chunks.length; i++) {
      eraseDataFromChunk(chunks[i]);
    }
  }

  /**
   * Erase data from the specified chunk, putting ZERO bytes to the buffer.
   * @param chunk with a buffer ready to read at the current position
   */
  protected void eraseDataFromChunk(ECChunk chunk) {
    ByteBuffer chunkBuffer = chunk.getBuffer();
    // Erase the data at the position, and restore the buffer ready for reading
    // same many bytes but all ZERO.
    int pos = chunkBuffer.position();
    int len = chunkBuffer.remaining();
    chunkBuffer.put(zeroChunkBytes, 0, len);
    // Back to readable again after data erased
    chunkBuffer.flip();
    chunkBuffer.position(pos);
    chunkBuffer.limit(pos + len);
  }

  /**
   * Clone chunks along with copying the associated data. It respects how the
   * chunk buffer is allocated, direct or non-direct. It avoids affecting the
   * original chunk buffers.
   * @param chunks
   * @return
   */
  protected ECChunk[] cloneChunksWithData(ECChunk[] chunks) {
    ECChunk[] results = new ECChunk[chunks.length];
    for (int i = 0; i < chunks.length; i++) {
      results[i] = cloneChunkWithData(chunks[i]);
    }

    return results;
  }

  /**
   * Clone chunk along with copying the associated data. It respects how the
   * chunk buffer is allocated, direct or non-direct. It avoids affecting the
   * original chunk.
   * @param chunk
   * @return a new chunk
   */
  protected ECChunk cloneChunkWithData(ECChunk chunk) {
    ByteBuffer srcBuffer = chunk.getBuffer();

    byte[] bytesArr = new byte[srcBuffer.remaining()];
    srcBuffer.mark();
    srcBuffer.get(bytesArr, 0, bytesArr.length);
    srcBuffer.reset();

    ByteBuffer destBuffer = allocateOutputBuffer(bytesArr.length);
    int pos = destBuffer.position();
    destBuffer.put(bytesArr);
    destBuffer.flip();
    destBuffer.position(pos);

    return new ECChunk(destBuffer);
  }

  /**
   * Allocate a chunk for output or writing.
   * @return
   */
  protected ECChunk allocateOutputChunk() {
    ByteBuffer buffer = allocateOutputBuffer(chunkSize);

    return new ECChunk(buffer);
  }

  /**
   * Allocate a buffer for output or writing. It can prepare for two kinds of
   * data buffers: one with position as 0, the other with position > 0
   * @return a buffer ready to write chunkSize bytes from current position
   */
  protected ByteBuffer allocateOutputBuffer(int bufferLen) {
    /**
     * When startBufferWithZero, will prepare a buffer as:---------------
     * otherwise, the buffer will be like:             ___TO--BE--WRITTEN___,
     * and in the beginning, dummy data are prefixed, to simulate a buffer of
     * position > 0.
     */
    int startOffset = startBufferWithZero ? 0 : 11; // 11 is arbitrary
    int allocLen = startOffset + bufferLen + startOffset;
    ByteBuffer buffer = usingDirectBuffer ?
        ByteBuffer.allocateDirect(allocLen) : ByteBuffer.allocate(allocLen);
    buffer.limit(startOffset + bufferLen);
    fillDummyData(buffer, startOffset);
    startBufferWithZero = ! startBufferWithZero;

    return buffer;
  }

  /**
   * Prepare data chunks for each data unit, by generating random data.
   * @return
   */
  protected ECChunk[] prepareDataChunksForEncoding() {
    ECChunk[] chunks = new ECChunk[numDataUnits];
    for (int i = 0; i < chunks.length; i++) {
      chunks[i] = generateDataChunk();
    }

    return chunks;
  }

  /**
   * Generate data chunk by making random data.
   * @return
   */
  protected ECChunk generateDataChunk() {
    ByteBuffer buffer = allocateOutputBuffer(chunkSize);
    int pos = buffer.position();
    buffer.put(generateData(chunkSize));
    buffer.flip();
    buffer.position(pos);

    return new ECChunk(buffer);
  }

  /**
   * Fill len of dummy data in the buffer at the current position.
   * @param buffer
   * @param len
   */
  protected void fillDummyData(ByteBuffer buffer, int len) {
    byte[] dummy = new byte[len];
    RAND.nextBytes(dummy);
    buffer.put(dummy);
  }

  protected byte[] generateData(int len) {
    byte[] buffer = new byte[len];
    for (int i = 0; i < buffer.length; i++) {
      buffer[i] = (byte) RAND.nextInt(256);
    }
    return buffer;
  }

  /**
   * Prepare parity chunks for encoding, each chunk for each parity unit.
   * @return
   */
  protected ECChunk[] prepareParityChunksForEncoding() {
    ECChunk[] chunks = new ECChunk[numParityUnits];
    for (int i = 0; i < chunks.length; i++) {
      chunks[i] = allocateOutputChunk();
    }

    return chunks;
  }

  /**
   * Prepare output chunks for decoding, each output chunk for each erased
   * chunk.
   * @return
   */
  protected ECChunk[] prepareOutputChunksForDecoding() {
    ECChunk[] chunks = new ECChunk[erasedDataIndexes.length +
        erasedParityIndexes.length];

    for (int i = 0; i < chunks.length; i++) {
      chunks[i] = allocateOutputChunk();
    }

    return chunks;
  }

  /**
   * Convert an array of this chunks to an array of byte array.
   * Note the chunk buffers are not affected.
   * @param chunks
   * @return an array of byte array
   */
  protected byte[][] toArrays(ECChunk[] chunks) {
    byte[][] bytesArr = new byte[chunks.length][];

    for (int i = 0; i < chunks.length; i++) {
      bytesArr[i] = chunks[i].toBytesArray();
    }

    return bytesArr;
  }


  /**
   * Make some chunk messy or not correct any more
   * @param chunks
   */
  protected void corruptSomeChunk(ECChunk[] chunks) {
    int idx = new Random().nextInt(chunks.length);
    ByteBuffer buffer = chunks[idx].getBuffer();
    if (buffer.hasRemaining()) {
      buffer.position(buffer.position() + 1);
    }
  }
}
