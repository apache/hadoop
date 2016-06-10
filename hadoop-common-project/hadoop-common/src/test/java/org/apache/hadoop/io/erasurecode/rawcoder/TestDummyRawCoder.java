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
package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.io.erasurecode.ECChunk;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * Test dummy raw coder.
 */
public class TestDummyRawCoder extends TestRawCoderBase {
  @Before
  public void setup() {
    encoderClass = DummyRawEncoder.class;
    decoderClass = DummyRawDecoder.class;
    setAllowDump(false);
    setChunkSize(baseChunkSize);
  }

  @Test
  public void testCoding_6x3_erasing_d0_d2() {
    prepare(null, 6, 3, new int[]{0, 2}, new int[0], false);
    testCodingDoMixed();
  }

  @Test
  public void testCoding_6x3_erasing_d0_p0() {
    prepare(null, 6, 3, new int[]{0}, new int[]{0}, false);
    testCodingDoMixed();
  }

  @Override
  protected void testCoding(boolean usingDirectBuffer) {
    this.usingDirectBuffer = usingDirectBuffer;
    prepareCoders(true);

    prepareBufferAllocator(true);
    setAllowChangeInputs(false);

    // Generate data and encode
    ECChunk[] dataChunks = prepareDataChunksForEncoding();
    markChunks(dataChunks);
    ECChunk[] parityChunks = prepareParityChunksForEncoding();
    encoder.encode(dataChunks, parityChunks);
    compareAndVerify(parityChunks, getEmptyChunks(parityChunks.length));

    // Decode
    restoreChunksFromMark(dataChunks);
    backupAndEraseChunks(dataChunks, parityChunks);
    ECChunk[] inputChunks = prepareInputChunksForDecoding(
        dataChunks, parityChunks);
    ensureOnlyLeastRequiredChunks(inputChunks);
    ECChunk[] recoveredChunks = prepareOutputChunksForDecoding();
    decoder.decode(inputChunks, getErasedIndexesForDecoding(), recoveredChunks);
    compareAndVerify(recoveredChunks, getEmptyChunks(recoveredChunks.length));
  }

  private ECChunk[] getEmptyChunks(int num) {
    ECChunk[] chunks = new ECChunk[num];
    for (int i = 0; i < chunks.length; i++) {
      chunks[i] = new ECChunk(ByteBuffer.wrap(getZeroChunkBytes()));
    }
    return chunks;
  }
}
