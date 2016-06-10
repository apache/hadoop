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

import java.nio.ByteBuffer;

/**
 * A utility class that maintains encoding state during an encode call using
 * ByteBuffer inputs.
 */
@InterfaceAudience.Private
class ByteBufferEncodingState extends EncodingState {
  ByteBuffer[] inputs;
  ByteBuffer[] outputs;
  boolean usingDirectBuffer;

  ByteBufferEncodingState(RawErasureEncoder encoder,
                          ByteBuffer[] inputs, ByteBuffer[] outputs) {
    this.encoder = encoder;
    ByteBuffer validInput = CoderUtil.findFirstValidInput(inputs);
    this.encodeLength = validInput.remaining();
    this.usingDirectBuffer = validInput.isDirect();
    this.inputs = inputs;
    this.outputs = outputs;

    checkParameters(inputs, outputs);
    checkBuffers(inputs);
    checkBuffers(outputs);
  }

  /**
   * Convert to a ByteArrayEncodingState when it's backed by on-heap arrays.
   */
  ByteArrayEncodingState convertToByteArrayState() {
    int[] inputOffsets = new int[inputs.length];
    int[] outputOffsets = new int[outputs.length];
    byte[][] newInputs = new byte[inputs.length][];
    byte[][] newOutputs = new byte[outputs.length][];

    ByteBuffer buffer;
    for (int i = 0; i < inputs.length; ++i) {
      buffer = inputs[i];
      inputOffsets[i] = buffer.arrayOffset() + buffer.position();
      newInputs[i] = buffer.array();
    }

    for (int i = 0; i < outputs.length; ++i) {
      buffer = outputs[i];
      outputOffsets[i] = buffer.arrayOffset() + buffer.position();
      newOutputs[i] = buffer.array();
    }

    ByteArrayEncodingState baeState = new ByteArrayEncodingState(encoder,
        encodeLength, newInputs, inputOffsets, newOutputs, outputOffsets);
    return baeState;
  }

  /**
   * Check and ensure the buffers are of the desired length and type, direct
   * buffers or not.
   * @param buffers the buffers to check
   */
  void checkBuffers(ByteBuffer[] buffers) {
    for (ByteBuffer buffer : buffers) {
      if (buffer == null) {
        throw new HadoopIllegalArgumentException(
            "Invalid buffer found, not allowing null");
      }

      if (buffer.remaining() != encodeLength) {
        throw new HadoopIllegalArgumentException(
            "Invalid buffer, not of length " + encodeLength);
      }
      if (buffer.isDirect() != usingDirectBuffer) {
        throw new HadoopIllegalArgumentException(
            "Invalid buffer, isDirect should be " + usingDirectBuffer);
      }
    }
  }
}
