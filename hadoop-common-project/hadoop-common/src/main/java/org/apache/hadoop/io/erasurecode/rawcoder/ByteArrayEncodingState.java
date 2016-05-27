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
 * A utility class that maintains encoding state during an encode call using
 * byte array inputs.
 */
@InterfaceAudience.Private
class ByteArrayEncodingState extends EncodingState {
  byte[][] inputs;
  byte[][] outputs;
  int[] inputOffsets;
  int[] outputOffsets;

  ByteArrayEncodingState(RawErasureEncoder encoder,
                         byte[][] inputs, byte[][] outputs) {
    this.encoder = encoder;
    byte[] validInput = CoderUtil.findFirstValidInput(inputs);
    this.encodeLength = validInput.length;
    this.inputs = inputs;
    this.outputs = outputs;

    checkParameters(inputs, outputs);
    checkBuffers(inputs);
    checkBuffers(outputs);

    this.inputOffsets = new int[inputs.length]; // ALL ZERO
    this.outputOffsets = new int[outputs.length]; // ALL ZERO
  }

  ByteArrayEncodingState(RawErasureEncoder encoder,
                         int encodeLength,
                         byte[][] inputs,
                         int[] inputOffsets,
                         byte[][] outputs,
                         int[] outputOffsets) {
    this.encoder = encoder;
    this.encodeLength = encodeLength;
    this.inputs = inputs;
    this.outputs = outputs;
    this.inputOffsets = inputOffsets;
    this.outputOffsets = outputOffsets;
  }

  /**
   * Check and ensure the buffers are of the desired length.
   * @param buffers the buffers to check
   */
  void checkBuffers(byte[][] buffers) {
    for (byte[] buffer : buffers) {
      if (buffer == null) {
        throw new HadoopIllegalArgumentException(
            "Invalid buffer found, not allowing null");
      }

      if (buffer.length != encodeLength) {
        throw new HadoopIllegalArgumentException(
            "Invalid buffer not of length " + encodeLength);
      }
    }
  }
}
