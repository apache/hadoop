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

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * A utility class that maintains decoding state during a decode call.
 */
@InterfaceAudience.Private
class DecodingState {
  RawErasureDecoder decoder;
  int decodeLength;

  /**
   * Check and validate decoding parameters, throw exception accordingly. The
   * checking assumes it's a MDS code. Other code  can override this.
   * @param inputs input buffers to check
   * @param erasedIndexes indexes of erased units in the inputs array
   * @param outputs output buffers to check
   */
  <T> void checkParameters(T[] inputs, int[] erasedIndexes,
                           T[] outputs) {
    if (inputs.length != decoder.getNumParityUnits() +
        decoder.getNumDataUnits()) {
      throw new IllegalArgumentException("Invalid inputs length");
    }

    if (erasedIndexes.length != outputs.length) {
      throw new HadoopIllegalArgumentException(
          "erasedIndexes and outputs mismatch in length");
    }

    if (erasedIndexes.length > decoder.getNumParityUnits()) {
      throw new HadoopIllegalArgumentException(
          "Too many erased, not recoverable");
    }
  }
}
