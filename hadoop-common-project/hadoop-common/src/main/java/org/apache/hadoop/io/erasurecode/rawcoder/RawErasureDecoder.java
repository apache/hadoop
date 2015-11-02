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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ECChunk;

import java.nio.ByteBuffer;

/**
 * RawErasureDecoder performs decoding given chunks of input data and generates
 * missing data that corresponds to an erasure code scheme, like XOR and
 * Reed-Solomon.
 *
 * It extends the {@link RawErasureCoder} interface.
 */
@InterfaceAudience.Private
public interface RawErasureDecoder extends RawErasureCoder {

  /**
   * Decode with inputs and erasedIndexes, generates outputs.
   * How to prepare for inputs:
   * 1. Create an array containing data units + parity units. Please note the
   *    data units should be first or before the parity units.
   * 2. Set null in the array locations specified via erasedIndexes to indicate
   *    they're erased and no data are to read from;
   * 3. Set null in the array locations for extra redundant items, as they're
   *    not necessary to read when decoding. For example in RS-6-3, if only 1
   *    unit is really erased, then we have 2 extra items as redundant. They can
   *    be set as null to indicate no data will be used from them.
   *
   * For an example using RS (6, 3), assuming sources (d0, d1, d2, d3, d4, d5)
   * and parities (p0, p1, p2), d2 being erased. We can and may want to use only
   * 6 units like (d1, d3, d4, d5, p0, p2) to recover d2. We will have:
   *     inputs = [null(d0), d1, null(d2), d3, d4, d5, p0, null(p1), p2]
   *     erasedIndexes = [2] // index of d2 into inputs array
   *     outputs = [a-writable-buffer]
   *
   * Note, for both inputs and outputs, no mixing of on-heap buffers and direct
   * buffers are allowed.
   *
   * If the coder option ALLOW_CHANGE_INPUTS is set true (false by default), the
   * content of input buffers may change after the call, subject to concrete
   * implementation. Anyway the positions of input buffers will move forward.
   *
   * @param inputs input buffers to read data from
   * @param erasedIndexes indexes of erased units in the inputs array
   * @param outputs output buffers to put decoded data into according to
   *                erasedIndexes, ready for read after the call
   */
  void decode(ByteBuffer[] inputs, int[] erasedIndexes,
                     ByteBuffer[] outputs);

  /**
   * Decode with inputs and erasedIndexes, generates outputs. More see above.
   *
   * @param inputs input buffers to read data from
   * @param erasedIndexes indexes of erased units in the inputs array
   * @param outputs output buffers to put decoded data into according to
   *                erasedIndexes, ready for read after the call
   */
  void decode(byte[][] inputs, int[] erasedIndexes, byte[][] outputs);

  /**
   * Decode with inputs and erasedIndexes, generates outputs. More see above.
   *
   * Note, for both input and output ECChunks, no mixing of on-heap buffers and
   * direct buffers are allowed.
   *
   * @param inputs input buffers to read data from
   * @param erasedIndexes indexes of erased units in the inputs array
   * @param outputs output buffers to put decoded data into according to
   *                erasedIndexes, ready for read after the call
   */
  void decode(ECChunk[] inputs, int[] erasedIndexes, ECChunk[] outputs);

}
