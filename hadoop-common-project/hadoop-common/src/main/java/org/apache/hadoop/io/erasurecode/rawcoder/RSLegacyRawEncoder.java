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
import org.apache.hadoop.io.erasurecode.rawcoder.util.RSUtil;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * A raw erasure encoder in RS code scheme in pure Java in case native one
 * isn't available in some environment. Please always use native implementations
 * when possible.
 */
@InterfaceAudience.Private
public class RSLegacyRawEncoder extends RawErasureEncoder {
  private int[] generatingPolynomial;

  public RSLegacyRawEncoder(ErasureCoderOptions coderOptions) {
    super(coderOptions);

    assert (getNumDataUnits() + getNumParityUnits() < RSUtil.GF.getFieldSize());

    int[] primitivePower = RSUtil.getPrimitivePower(getNumDataUnits(),
        getNumParityUnits());
    // compute generating polynomial
    int[] gen = {1};
    int[] poly = new int[2];
    for (int i = 0; i < getNumParityUnits(); i++) {
      poly[0] = primitivePower[i];
      poly[1] = 1;
      gen = RSUtil.GF.multiply(gen, poly);
    }
    // generating polynomial has all generating roots
    generatingPolynomial = gen;
  }

  @Override
  protected void doEncode(ByteBufferEncodingState encodingState) {
    CoderUtil.resetOutputBuffers(encodingState.outputs,
        encodingState.encodeLength);
    // parity units + data units
    ByteBuffer[] all = new ByteBuffer[encodingState.outputs.length +
        encodingState.inputs.length];

    if (allowChangeInputs()) {
      System.arraycopy(encodingState.outputs, 0, all, 0,
          encodingState.outputs.length);
      System.arraycopy(encodingState.inputs, 0, all,
          encodingState.outputs.length, encodingState.inputs.length);
    } else {
      System.arraycopy(encodingState.outputs, 0, all, 0,
          encodingState.outputs.length);

      /**
       * Note when this coder would be really (rarely) used in a production
       * system, this can  be optimized to cache and reuse the new allocated
       * buffers avoiding reallocating.
       */
      ByteBuffer tmp;
      for (int i = 0; i < encodingState.inputs.length; i++) {
        tmp = ByteBuffer.allocate(encodingState.inputs[i].remaining());
        tmp.put(encodingState.inputs[i]);
        tmp.flip();
        all[encodingState.outputs.length + i] = tmp;
      }
    }

    // Compute the remainder
    RSUtil.GF.remainder(all, generatingPolynomial);
  }

  @Override
  protected void doEncode(ByteArrayEncodingState encodingState) {
    int dataLen = encodingState.encodeLength;
    CoderUtil.resetOutputBuffers(encodingState.outputs,
        encodingState.outputOffsets, dataLen);
    // parity units + data units
    byte[][] all = new byte[encodingState.outputs.length +
        encodingState.inputs.length][];
    int[] allOffsets = new int[encodingState.outputOffsets.length +
        encodingState.inputOffsets.length];

    if (allowChangeInputs()) {
      System.arraycopy(encodingState.outputs, 0, all, 0,
          encodingState.outputs.length);
      System.arraycopy(encodingState.inputs, 0, all,
          encodingState.outputs.length, encodingState.inputs.length);

      System.arraycopy(encodingState.outputOffsets, 0, allOffsets, 0,
          encodingState.outputOffsets.length);
      System.arraycopy(encodingState.inputOffsets, 0, allOffsets,
          encodingState.outputOffsets.length,
          encodingState.inputOffsets.length);
    } else {
      System.arraycopy(encodingState.outputs, 0, all, 0,
          encodingState.outputs.length);
      System.arraycopy(encodingState.outputOffsets, 0, allOffsets, 0,
          encodingState.outputOffsets.length);

      for (int i = 0; i < encodingState.inputs.length; i++) {
        all[encodingState.outputs.length + i] =
            Arrays.copyOfRange(encodingState.inputs[i],
            encodingState.inputOffsets[i],
                encodingState.inputOffsets[i] + dataLen);
      }
    }

    // Compute the remainder
    RSUtil.GF.remainder(all, allOffsets, dataLen, generatingPolynomial);
  }
}
