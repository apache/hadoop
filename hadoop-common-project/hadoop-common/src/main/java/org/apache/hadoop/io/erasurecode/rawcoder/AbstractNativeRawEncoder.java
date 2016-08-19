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
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Abstract native raw encoder for all native coders to extend with.
 */
@InterfaceAudience.Private
abstract class AbstractNativeRawEncoder extends RawErasureEncoder {
  public static Logger LOG =
      LoggerFactory.getLogger(AbstractNativeRawEncoder.class);

  public AbstractNativeRawEncoder(ErasureCoderOptions coderOptions) {
    super(coderOptions);
  }

  @Override
  protected void doEncode(ByteBufferEncodingState encodingState) {
    int[] inputOffsets = new int[encodingState.inputs.length];
    int[] outputOffsets = new int[encodingState.outputs.length];
    int dataLen = encodingState.inputs[0].remaining();

    ByteBuffer buffer;
    for (int i = 0; i < encodingState.inputs.length; ++i) {
      buffer = encodingState.inputs[i];
      inputOffsets[i] = buffer.position();
    }

    for (int i = 0; i < encodingState.outputs.length; ++i) {
      buffer = encodingState.outputs[i];
      outputOffsets[i] = buffer.position();
    }

    performEncodeImpl(encodingState.inputs, inputOffsets, dataLen,
        encodingState.outputs, outputOffsets);
  }

  protected abstract void performEncodeImpl(
          ByteBuffer[] inputs, int[] inputOffsets,
          int dataLen, ByteBuffer[] outputs, int[] outputOffsets);

  @Override
  protected void doEncode(ByteArrayEncodingState encodingState) {
    LOG.warn("convertToByteBufferState is invoked, " +
        "not efficiently. Please use direct ByteBuffer inputs/outputs");

    ByteBufferEncodingState bbeState = encodingState.convertToByteBufferState();
    doEncode(bbeState);

    for (int i = 0; i < encodingState.outputs.length; i++) {
      bbeState.outputs[i].get(encodingState.outputs[i],
          encodingState.outputOffsets[i], encodingState.encodeLength);
    }
  }

  // To link with the underlying data structure in the native layer.
  // No get/set as only used by native codes.
  private long nativeCoder;
}
