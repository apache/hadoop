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
package org.apache.hadoop.io.erasurecode.coder.util;

import java.nio.ByteBuffer;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;
import org.apache.hadoop.io.erasurecode.rawcoder.util.RSUtil;

/**
 * Some utilities for Hitchhiker coding.
 */
@InterfaceAudience.Private
public final class HHUtil {
  private HHUtil() {
    // No called
  }

  public static int[] initPiggyBackIndexWithoutPBVec(int numDataUnits,
                                                     int numParityUnits) {
    final int piggyBackSize = numDataUnits / (numParityUnits - 1);
    int[] piggyBackIndex = new int[numParityUnits];

    for (int i = 0; i < numDataUnits; ++i) {
      if ((i % piggyBackSize) == 0) {
        piggyBackIndex[i / piggyBackSize] = i;
      }
    }

    piggyBackIndex[numParityUnits - 1] = numDataUnits;
    return piggyBackIndex;
  }

  public static int[] initPiggyBackFullIndexVec(int numDataUnits,
                                                int[] piggyBackIndex) {
    int[] piggyBackFullIndex = new int[numDataUnits];

    for (int i = 1; i < piggyBackIndex.length; ++i) {
      for (int j = piggyBackIndex[i - 1]; j < piggyBackIndex[i]; ++j) {
        piggyBackFullIndex[j] = i;
      }
    }

    return piggyBackFullIndex;
  }

  public static ByteBuffer[] getPiggyBacksFromInput(ByteBuffer[] inputs,
                                                    int[] piggyBackIndex,
                                                    int numParityUnits,
                                                    int pgIndex,
                                                    RawErasureEncoder encoder) {
    ByteBuffer[] emptyInput = new ByteBuffer[inputs.length];
    ByteBuffer[] tempInput = new ByteBuffer[inputs.length];
    int[] inputPositions = new int[inputs.length];

    for (int m = 0; m < inputs.length; ++m) {
      if (inputs[m] != null) {
        emptyInput[m] = allocateByteBuffer(inputs[m].isDirect(),
                inputs[m].remaining());
      }
    }

    ByteBuffer[] tempOutput = new ByteBuffer[numParityUnits];
    for (int m = 0; m < numParityUnits; ++m) {
      tempOutput[m] = allocateByteBuffer(inputs[m].isDirect(),
              inputs[0].remaining());
    }

    ByteBuffer[] piggyBacks = new ByteBuffer[numParityUnits - 1];
    assert (piggyBackIndex.length >= numParityUnits);

    // using underlying RS code to create piggybacks
    for (int i = 0; i < numParityUnits - 1; ++i) {
      for (int k = piggyBackIndex[i]; k < piggyBackIndex[i + 1]; ++k) {
        tempInput[k] = inputs[k];
        inputPositions[k] = inputs[k].position();
      }
      for (int n = 0; n < emptyInput.length; ++n) {
        if (tempInput[n] == null) {
          tempInput[n] = emptyInput[n];
          inputPositions[n] = emptyInput[n].position();
        }
      }

      encoder.encode(tempInput, tempOutput);

      piggyBacks[i] = cloneBufferData(tempOutput[pgIndex]);

      for (int j = 0; j < tempInput.length; j++) {
        if (tempInput[j] != null) {
          tempInput[j].position(inputPositions[j]);
          tempInput[j] = null;
        }
      }

      for (int j = 0; j < tempOutput.length; j++) {
        tempOutput[j].clear();
      }
    }

    return piggyBacks;
  }

  private static ByteBuffer cloneBufferData(ByteBuffer srcBuffer) {
    ByteBuffer destBuffer;
    byte[] bytesArr = new byte[srcBuffer.remaining()];

    srcBuffer.mark();
    srcBuffer.get(bytesArr);
    srcBuffer.reset();

    if (!srcBuffer.isDirect()) {
      destBuffer = ByteBuffer.wrap(bytesArr);
    } else {
      destBuffer = ByteBuffer.allocateDirect(srcBuffer.remaining());
      destBuffer.put(bytesArr);
      destBuffer.flip();
    }

    return destBuffer;
  }

  public static ByteBuffer allocateByteBuffer(boolean useDirectBuffer,
                                              int bufSize) {
    if (useDirectBuffer) {
      return ByteBuffer.allocateDirect(bufSize);
    } else {
      return ByteBuffer.allocate(bufSize);
    }
  }

  public static ByteBuffer getPiggyBackForDecode(ByteBuffer[][] inputs,
                                                 ByteBuffer[][] outputs,
                                                 int pbParityIndex,
                                                 int numDataUnits,
                                                 int numParityUnits,
                                                 int pbIndex) {
    ByteBuffer fisrtValidInput = HHUtil.findFirstValidInput(inputs[0]);
    int bufSize = fisrtValidInput.remaining();

    ByteBuffer piggybacks = allocateByteBuffer(fisrtValidInput.isDirect(),
            bufSize);

    // Use piggyBackParityIndex to figure out which parity location has the
    // associated piggyBack
    // Obtain the piggyback by subtracting the decoded (second sub-packet
    // only ) parity value from the actually read parity value
    if (pbParityIndex < numParityUnits) {
      // not the last piggybackSet
      int inputIdx = numDataUnits + pbParityIndex;
      int inputPos = inputs[1][inputIdx].position();
      int outputPos = outputs[1][pbParityIndex].position();

      for (int m = 0, k = inputPos, n = outputPos; m < bufSize; k++, m++, n++) {
        int valueWithPb = 0xFF & inputs[1][inputIdx].get(k);
        int valueWithoutPb = 0xFF & outputs[1][pbParityIndex].get(n);
        piggybacks.put(m, (byte) RSUtil.GF.add(valueWithPb, valueWithoutPb));
      }
    } else {
      // last piggybackSet
      int sum = 0;
      for (int k = 0; k < bufSize; k++) {
        sum = 0;
        for (int i = 1; i < numParityUnits; i++) {
          int inIdx = numDataUnits + i;
          int inPos = inputs[1][numDataUnits + i].position();
          int outPos = outputs[1][i].position();

          sum = RSUtil.GF.add(sum, (0xFF & inputs[1][inIdx].get(inPos + k)));
          sum = RSUtil.GF.add(sum, (0xFF & outputs[1][i].get(outPos + k)));
        }

        sum = RSUtil.GF.add(sum,
                (0xFF & inputs[0][numDataUnits + pbIndex].get(
                        inputs[0][numDataUnits + pbIndex].position() + k)));

        piggybacks.put(k, (byte) sum);
      }

    }

    return piggybacks;
  }

  /**
   * Find the valid input from all the inputs.
   * @param inputs input buffers to look for valid input
   * @return the first valid input
   */
  public static <T> T findFirstValidInput(T[] inputs) {
    for (T input : inputs) {
      if (input != null) {
        return input;
      }
    }

    throw new HadoopIllegalArgumentException(
            "Invalid inputs are found, all being null");
  }
}
