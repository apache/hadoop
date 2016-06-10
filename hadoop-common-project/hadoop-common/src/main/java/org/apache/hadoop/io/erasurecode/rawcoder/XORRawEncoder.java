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

import java.nio.ByteBuffer;

/**
 * A raw encoder in XOR code scheme in pure Java, adapted from HDFS-RAID.
 *
 * XOR code is an important primitive code scheme in erasure coding and often
 * used in advanced codes, like HitchHiker and LRC, though itself is rarely
 * deployed independently.
 */
@InterfaceAudience.Private
public class XORRawEncoder extends RawErasureEncoder {

  public XORRawEncoder(ErasureCoderOptions coderOptions) {
    super(coderOptions);
  }

  protected void doEncode(ByteBufferEncodingState encodingState) {
    CoderUtil.resetOutputBuffers(encodingState.outputs,
        encodingState.encodeLength);
    ByteBuffer output = encodingState.outputs[0];

    // Get the first buffer's data.
    int iIdx, oIdx;
    for (iIdx = encodingState.inputs[0].position(), oIdx = output.position();
         iIdx < encodingState.inputs[0].limit(); iIdx++, oIdx++) {
      output.put(oIdx, encodingState.inputs[0].get(iIdx));
    }

    // XOR with everything else.
    for (int i = 1; i < encodingState.inputs.length; i++) {
      for (iIdx = encodingState.inputs[i].position(), oIdx = output.position();
           iIdx < encodingState.inputs[i].limit();
           iIdx++, oIdx++) {
        output.put(oIdx, (byte) (output.get(oIdx) ^
            encodingState.inputs[i].get(iIdx)));
      }
    }
  }

  @Override
  protected void doEncode(ByteArrayEncodingState encodingState) {
    int dataLen = encodingState.encodeLength;
    CoderUtil.resetOutputBuffers(encodingState.outputs,
        encodingState.outputOffsets, dataLen);
    byte[] output = encodingState.outputs[0];

    // Get the first buffer's data.
    int iIdx, oIdx;
    for (iIdx = encodingState.inputOffsets[0],
             oIdx = encodingState.outputOffsets[0];
         iIdx < encodingState.inputOffsets[0] + dataLen; iIdx++, oIdx++) {
      output[oIdx] = encodingState.inputs[0][iIdx];
    }

    // XOR with everything else.
    for (int i = 1; i < encodingState.inputs.length; i++) {
      for (iIdx = encodingState.inputOffsets[i],
               oIdx = encodingState.outputOffsets[0];
           iIdx < encodingState.inputOffsets[i] + dataLen; iIdx++, oIdx++) {
        output[oIdx] ^= encodingState.inputs[i][iIdx];
      }
    }
  }
}
