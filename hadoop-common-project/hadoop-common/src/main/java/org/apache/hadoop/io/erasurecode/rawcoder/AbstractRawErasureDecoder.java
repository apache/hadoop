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

import java.nio.ByteBuffer;

/**
 * An abstract raw erasure decoder that's to be inherited by new decoders.
 *
 * It implements the {@link RawErasureDecoder} interface.
 */
public abstract class AbstractRawErasureDecoder extends AbstractRawErasureCoder
    implements RawErasureDecoder {

  @Override
  public void decode(ByteBuffer[] inputs, int[] erasedIndexes,
                     ByteBuffer[] outputs) {
    if (erasedIndexes.length == 0) {
      return;
    }

    doDecode(inputs, erasedIndexes, outputs);
  }

  /**
   * Perform the real decoding using ByteBuffer
   * @param inputs
   * @param erasedIndexes
   * @param outputs
   */
  protected abstract void doDecode(ByteBuffer[] inputs, int[] erasedIndexes,
                                   ByteBuffer[] outputs);

  @Override
  public void decode(byte[][] inputs, int[] erasedIndexes, byte[][] outputs) {
    if (erasedIndexes.length == 0) {
      return;
    }

    doDecode(inputs, erasedIndexes, outputs);
  }

  /**
   * Perform the real decoding using bytes array
   * @param inputs
   * @param erasedIndexes
   * @param outputs
   */
  protected abstract void doDecode(byte[][] inputs, int[] erasedIndexes,
                                   byte[][] outputs);

  @Override
  public void decode(ECChunk[] inputs, int[] erasedIndexes,
                     ECChunk[] outputs) {
    doDecode(inputs, erasedIndexes, outputs);
  }

  /**
   * Perform the real decoding using chunks
   * @param inputs
   * @param erasedIndexes
   * @param outputs
   */
  protected void doDecode(ECChunk[] inputs, int[] erasedIndexes,
                          ECChunk[] outputs) {
    if (inputs[0].getBuffer().hasArray()) {
      byte[][] inputBytesArr = ECChunk.toArray(inputs);
      byte[][] outputBytesArr = ECChunk.toArray(outputs);
      doDecode(inputBytesArr, erasedIndexes, outputBytesArr);
    } else {
      ByteBuffer[] inputBuffers = ECChunk.toBuffers(inputs);
      ByteBuffer[] outputBuffers = ECChunk.toBuffers(outputs);
      doDecode(inputBuffers, erasedIndexes, outputBuffers);
    }
  }
}
