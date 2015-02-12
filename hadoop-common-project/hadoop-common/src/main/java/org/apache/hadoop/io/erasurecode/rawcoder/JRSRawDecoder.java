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

import org.apache.hadoop.io.erasurecode.rawcoder.util.RSUtil;

import java.nio.ByteBuffer;

/**
 * A raw erasure decoder in RS code scheme in pure Java in case native one
 * isn't available in some environment. Please always use native implementations
 * when possible.
 */
public class JRSRawDecoder extends AbstractRawErasureDecoder {
  // To describe and calculate the needed Vandermonde matrix
  private int[] errSignature;
  private int[] primitivePower;

  @Override
  public void initialize(int numDataUnits, int numParityUnits, int chunkSize) {
    super.initialize(numDataUnits, numParityUnits, chunkSize);
    assert (getNumDataUnits() + getNumParityUnits() < RSUtil.GF.getFieldSize());

    this.errSignature = new int[getNumParityUnits()];
    this.primitivePower = RSUtil.getPrimitivePower(getNumDataUnits(),
        getNumParityUnits());
  }

  @Override
  protected void doDecode(ByteBuffer[] inputs, int[] erasedIndexes,
                          ByteBuffer[] outputs) {
    for (int i = 0; i < erasedIndexes.length; i++) {
      errSignature[i] = primitivePower[erasedIndexes[i]];
      RSUtil.GF.substitute(inputs, outputs[i], primitivePower[i]);
    }

    int dataLen = inputs[0].remaining();
    RSUtil.GF.solveVandermondeSystem(errSignature, outputs,
        erasedIndexes.length, dataLen);
  }

  @Override
  protected void doDecode(byte[][] inputs, int[] erasedIndexes,
                          byte[][] outputs) {
    for (int i = 0; i < erasedIndexes.length; i++) {
      errSignature[i] = primitivePower[erasedIndexes[i]];
      RSUtil.GF.substitute(inputs, outputs[i], primitivePower[i]);
    }

    int dataLen = inputs[0].length;
    RSUtil.GF.solveVandermondeSystem(errSignature, outputs,
        erasedIndexes.length, dataLen);
  }
}
