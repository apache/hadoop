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
import org.apache.hadoop.io.erasurecode.TestCoderBase;

/**
 * Raw coder test base with utilities.
 */
public abstract class TestRawCoderBase extends TestCoderBase {
  protected Class<? extends RawErasureEncoder> encoderClass;
  protected Class<? extends RawErasureDecoder> decoderClass;
  private RawErasureEncoder encoder;
  private RawErasureDecoder decoder;

  /**
   * Generating source data, encoding, recovering and then verifying.
   * RawErasureCoder mainly uses ECChunk to pass input and output data buffers,
   * it supports two kinds of ByteBuffers, one is array backed, the other is
   * direct ByteBuffer. Use usingDirectBuffer indicate which case to test.
   *
   * @param usingDirectBuffer
   */
  protected void testCoding(boolean usingDirectBuffer) {
    this.usingDirectBuffer = usingDirectBuffer;
    prepareCoders();

    // Generate data and encode
    ECChunk[] dataChunks = prepareDataChunksForEncoding();
    ECChunk[] parityChunks = prepareParityChunksForEncoding();

    // Backup all the source chunks for later recovering because some coders
    // may affect the source data.
    ECChunk[] clonedDataChunks = cloneChunksWithData(dataChunks);

    encoder.encode(dataChunks, parityChunks);

    // Backup and erase some chunks
    ECChunk[] backupChunks = backupAndEraseChunks(clonedDataChunks);

    // Decode
    ECChunk[] inputChunks = prepareInputChunksForDecoding(
        clonedDataChunks, parityChunks);

    ECChunk[] recoveredChunks = prepareOutputChunksForDecoding();

    decoder.decode(inputChunks, getErasedIndexesForDecoding(), recoveredChunks);

    // Compare
    compareAndVerify(backupChunks, recoveredChunks);
  }

  private void prepareCoders() {
    if (encoder == null) {
      encoder = createEncoder();
    }

    if (decoder == null) {
      decoder = createDecoder();
    }
  }

  /**
   * Create the raw erasure encoder to test
   * @return
   */
  protected RawErasureEncoder createEncoder() {
    RawErasureEncoder encoder;
    try {
      encoder = encoderClass.newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Failed to create encoder", e);
    }

    encoder.initialize(numDataUnits, numParityUnits, chunkSize);
    encoder.setConf(getConf());
    return encoder;
  }

  /**
   * create the raw erasure decoder to test
   * @return
   */
  protected RawErasureDecoder createDecoder() {
    RawErasureDecoder decoder;
    try {
      decoder = decoderClass.newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Failed to create decoder", e);
    }

    decoder.initialize(numDataUnits, numParityUnits, chunkSize);
    decoder.setConf(getConf());
    return decoder;
  }

}
