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
import org.apache.hadoop.io.erasurecode.rawcoder.util.RSUtil;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * A raw erasure encoder in RS code scheme in pure Java in case native one
 * isn't available in some environment. Please always use native implementations
 * when possible.
 */
@InterfaceAudience.Private
public class RSRawEncoder extends AbstractRawErasureEncoder {
  private int[] generatingPolynomial;

  public RSRawEncoder(int numDataUnits, int numParityUnits) {
    super(numDataUnits, numParityUnits);

    assert (getNumDataUnits() + getNumParityUnits() < RSUtil.GF.getFieldSize());

    int[] primitivePower = RSUtil.getPrimitivePower(numDataUnits,
        numParityUnits);
    // compute generating polynomial
    int[] gen = {1};
    int[] poly = new int[2];
    for (int i = 0; i < numParityUnits; i++) {
      poly[0] = primitivePower[i];
      poly[1] = 1;
      gen = RSUtil.GF.multiply(gen, poly);
    }
    // generating polynomial has all generating roots
    generatingPolynomial = gen;
  }

  @Override
  protected void doEncode(ByteBuffer[] inputs, ByteBuffer[] outputs) {
    // parity units + data units
    ByteBuffer[] all = new ByteBuffer[outputs.length + inputs.length];

    if (isAllowingChangeInputs()) {
      System.arraycopy(outputs, 0, all, 0, outputs.length);
      System.arraycopy(inputs, 0, all, outputs.length, inputs.length);
    } else {
      System.arraycopy(outputs, 0, all, 0, outputs.length);

      /**
       * Note when this coder would be really (rarely) used in a production
       * system, this can  be optimized to cache and reuse the new allocated
       * buffers avoiding reallocating.
       */
      ByteBuffer tmp;
      for (int i = 0; i < inputs.length; i++) {
        tmp = ByteBuffer.allocate(inputs[i].remaining());
        tmp.put(inputs[i]);
        tmp.flip();
        all[outputs.length + i] = tmp;
      }
    }

    // Compute the remainder
    RSUtil.GF.remainder(all, generatingPolynomial);
  }

  @Override
  protected void doEncode(byte[][] inputs, int[] inputOffsets,
                          int dataLen, byte[][] outputs,
                          int[] outputOffsets) {
    // parity units + data units
    byte[][] all = new byte[outputs.length + inputs.length][];
    int[] allOffsets = new int[outputOffsets.length + inputOffsets.length];

    if (isAllowingChangeInputs()) {
      System.arraycopy(outputs, 0, all, 0, outputs.length);
      System.arraycopy(inputs, 0, all, outputs.length, inputs.length);

      System.arraycopy(outputOffsets, 0, allOffsets, 0, outputOffsets.length);
      System.arraycopy(inputOffsets, 0, allOffsets,
          outputOffsets.length, inputOffsets.length);
    } else {
      System.arraycopy(outputs, 0, all, 0, outputs.length);
      System.arraycopy(outputOffsets, 0, allOffsets, 0, outputOffsets.length);

      for (int i = 0; i < inputs.length; i++) {
        all[outputs.length + i] = Arrays.copyOfRange(inputs[i],
            inputOffsets[i], inputOffsets[i] + dataLen);
      }
    }

    // Compute the remainder
    RSUtil.GF.remainder(all, allOffsets, dataLen, generatingPolynomial);
  }
}
