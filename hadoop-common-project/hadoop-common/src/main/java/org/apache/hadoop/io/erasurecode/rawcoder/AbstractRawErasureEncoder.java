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
import org.apache.hadoop.io.erasurecode.ECChunk;

import java.nio.ByteBuffer;

/**
 * An abstract raw erasure encoder that's to be inherited by new encoders.
 *
 * It implements the {@link RawErasureEncoder} interface.
 */
public abstract class AbstractRawErasureEncoder extends AbstractRawErasureCoder
    implements RawErasureEncoder {

  @Override
  public void encode(ByteBuffer[] inputs, ByteBuffer[] outputs) {
    checkParameters(inputs, outputs);
    int dataLen = inputs[0].remaining();
    if (dataLen == 0) {
      return;
    }
    ensureLength(inputs, dataLen);
    ensureLength(outputs, dataLen);

    boolean usingDirectBuffer = inputs[0].isDirect();
    if (usingDirectBuffer) {
      doEncode(inputs, outputs);
      return;
    }

    int[] inputOffsets = new int[inputs.length];
    int[] outputOffsets = new int[outputs.length];
    byte[][] newInputs = new byte[inputs.length][];
    byte[][] newOutputs = new byte[outputs.length][];

    ByteBuffer buffer;
    for (int i = 0; i < inputs.length; ++i) {
      buffer = inputs[i];
      inputOffsets[i] = buffer.position();
      newInputs[i] = buffer.array();
    }

    for (int i = 0; i < outputs.length; ++i) {
      buffer = outputs[i];
      outputOffsets[i] = buffer.position();
      newOutputs[i] = buffer.array();
    }

    doEncode(newInputs, inputOffsets, dataLen, newOutputs, outputOffsets);

    for (int i = 0; i < inputs.length; ++i) {
      buffer = inputs[i];
      buffer.position(buffer.position() + dataLen); // dataLen bytes consumed
    }
  }

  /**
   * Perform the real encoding work using direct ByteBuffer
   * @param inputs Direct ByteBuffers expected
   * @param outputs Direct ByteBuffers expected
   */
  protected abstract void doEncode(ByteBuffer[] inputs, ByteBuffer[] outputs);

  @Override
  public void encode(byte[][] inputs, byte[][] outputs) {
    checkParameters(inputs, outputs);
    int dataLen = inputs[0].length;
    if (dataLen == 0) {
      return;
    }
    ensureLength(inputs, dataLen);
    ensureLength(outputs, dataLen);

    int[] inputOffsets = new int[inputs.length]; // ALL ZERO
    int[] outputOffsets = new int[outputs.length]; // ALL ZERO

    doEncode(inputs, inputOffsets, dataLen, outputs, outputOffsets);
  }

  /**
   * Perform the real encoding work using bytes array, supporting offsets
   * and lengths.
   * @param inputs
   * @param inputOffsets
   * @param dataLen
   * @param outputs
   * @param outputOffsets
   */
  protected abstract void doEncode(byte[][] inputs, int[] inputOffsets,
                                   int dataLen, byte[][] outputs,
                                   int[] outputOffsets);

  @Override
  public void encode(ECChunk[] inputs, ECChunk[] outputs) {
    ByteBuffer[] newInputs = ECChunk.toBuffers(inputs);
    ByteBuffer[] newOutputs = ECChunk.toBuffers(outputs);
    encode(newInputs, newOutputs);
  }

  /**
   * Check and validate decoding parameters, throw exception accordingly.
   * @param inputs
   * @param outputs
   */
  protected void checkParameters(Object[] inputs, Object[] outputs) {
    if (inputs.length != getNumDataUnits()) {
      throw new HadoopIllegalArgumentException("Invalid inputs length");
    }
    if (outputs.length != getNumParityUnits()) {
      throw new HadoopIllegalArgumentException("Invalid outputs length");
    }
  }
}
