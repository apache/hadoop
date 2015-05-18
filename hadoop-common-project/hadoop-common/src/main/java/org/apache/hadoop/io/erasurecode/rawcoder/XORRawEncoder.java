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

import java.nio.ByteBuffer;

/**
 * A raw encoder in XOR code scheme in pure Java, adapted from HDFS-RAID.
 */
public class XORRawEncoder extends AbstractRawErasureEncoder {

  @Override
  protected void doEncode(ByteBuffer[] inputs, ByteBuffer[] outputs) {
    resetBuffer(outputs[0]);

    int bufSize = getChunkSize();
    // Get the first buffer's data.
    for (int j = 0; j < bufSize; j++) {
      outputs[0].put(j, inputs[0].get(j));
    }

    // XOR with everything else.
    for (int i = 1; i < inputs.length; i++) {
      for (int j = 0; j < bufSize; j++) {
        outputs[0].put(j, (byte) (outputs[0].get(j) ^ inputs[i].get(j)));
      }
    }
  }

  @Override
  protected void doEncode(byte[][] inputs, byte[][] outputs) {
    resetBuffer(outputs[0]);

    int bufSize = getChunkSize();
    // Get the first buffer's data.
    for (int j = 0; j < bufSize; j++) {
      outputs[0][j] = inputs[0][j];
    }

    // XOR with everything else.
    for (int i = 1; i < inputs.length; i++) {
      for (int j = 0; j < bufSize; j++) {
        outputs[0][j] ^= inputs[i][j];
      }
    }
  }

}
