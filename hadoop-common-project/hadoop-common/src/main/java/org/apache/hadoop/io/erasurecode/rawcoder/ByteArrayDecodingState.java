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
 * A utility class that maintains decoding state during a decode call using
 * byte array inputs.
 */
@InterfaceAudience.Private
class ByteArrayDecodingState extends DecodingState {
  byte[][] inputs;
  int[] inputOffsets;
  int[] erasedIndexes;
  byte[][] outputs;
  int[] outputOffsets;

  ByteArrayDecodingState(RawErasureDecoder decoder, byte[][] inputs,
                         int[] erasedIndexes, byte[][] outputs) {
    this.decoder = decoder;
    this.inputs = inputs;
    this.outputs = outputs;
    this.erasedIndexes = erasedIndexes;
    byte[] validInput = CoderUtil.findFirstValidInput(inputs);
    this.decodeLength = validInput.length;

    checkParameters(inputs, erasedIndexes, outputs);
    checkInputBuffers(inputs);
    checkOutputBuffers(outputs);

    this.inputOffsets = new int[inputs.length]; // ALL ZERO
    this.outputOffsets = new int[outputs.length]; // ALL ZERO
  }

  ByteArrayDecodingState(RawErasureDecoder decoder,
                         int decodeLength,
                         int[] erasedIndexes,
                         byte[][] inputs,
                         int[] inputOffsets,
                         byte[][] outputs,
                         int[] outputOffsets) {
    this.decoder = decoder;
    this.decodeLength = decodeLength;
    this.erasedIndexes = erasedIndexes;
    this.inputs = inputs;
    this.outputs = outputs;
    this.inputOffsets = inputOffsets;
    this.outputOffsets = outputOffsets;
  }

  /**
   * Convert to a ByteBufferDecodingState when it's backed by on-heap arrays.
   */
  ByteBufferDecodingState convertToByteBufferState() {
    ByteBuffer[] newInputs = new ByteBuffer[inputs.length];
    ByteBuffer[] newOutputs = new ByteBuffer[outputs.length];

    for (int i = 0; i < inputs.length; i++) {
      newInputs[i] = CoderUtil.cloneAsDirectByteBuffer(inputs[i],
          inputOffsets[i], decodeLength);
    }

    for (int i = 0; i < outputs.length; i++) {
      newOutputs[i] = ByteBuffer.allocateDirect(decodeLength);
    }

    ByteBufferDecodingState bbdState = new ByteBufferDecodingState(decoder,
        decodeLength, erasedIndexes, newInputs, newOutputs);
    return bbdState;
  }

  /**
   * Check and ensure the buffers are of the desired length.
   * @param buffers the buffers to check
   */
  void checkInputBuffers(byte[][] buffers) {
    int validInputs = 0;

    for (byte[] buffer : buffers) {
      if (buffer == null) {
        continue;
      }

      if (buffer.length != decodeLength) {
        throw new HadoopIllegalArgumentException(
            "Invalid buffer, not of length " + decodeLength);
      }

      validInputs++;
    }

    if (validInputs < decoder.getNumDataUnits()) {
      throw new HadoopIllegalArgumentException(
          "No enough valid inputs are provided, not recoverable");
    }
  }

  /**
   * Check and ensure the buffers are of the desired length.
   * @param buffers the buffers to check
   */
  void checkOutputBuffers(byte[][] buffers) {
    for (byte[] buffer : buffers) {
      if (buffer == null) {
        throw new HadoopIllegalArgumentException(
            "Invalid buffer found, not allowing null");
      }

      if (buffer.length != decodeLength) {
        throw new HadoopIllegalArgumentException(
            "Invalid buffer not of length " + decodeLength);
      }
    }
  }
}
