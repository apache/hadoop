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
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.io.erasurecode.ECChunk;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A utility class to validate decoding.
 */
@InterfaceAudience.Private
public class DecodingValidator {

  private final RawErasureDecoder decoder;
  private ByteBuffer buffer;
  private int[] newValidIndexes;
  private int newErasedIndex;

  public DecodingValidator(RawErasureDecoder decoder) {
    this.decoder = decoder;
  }

  /**
   * Validate outputs decoded from inputs, by decoding an input back from
   * the outputs and comparing it with the original one.
   *
   * For instance, in RS (6, 3), let (d0, d1, d2, d3, d4, d5) be sources
   * and (p0, p1, p2) be parities, and assume
   *  inputs = [d0, null (d1), d2, d3, d4, d5, null (p0), p1, null (p2)];
   *  erasedIndexes = [1, 6];
   *  outputs = [d1, p0].
   * Then
   *  1. Create new inputs, erasedIndexes and outputs for validation so that
   *     the inputs could contain the decoded outputs, and decode them:
   *      newInputs = [d1, d2, d3, d4, d5, p0]
   *      newErasedIndexes = [0]
   *      newOutputs = [d0']
   *  2. Compare d0 and d0'. The comparison will fail with high probability
   *     when the initial outputs are wrong.
   *
   * Note that the input buffers' positions must be the ones where data are
   * read: If the input buffers have been processed by a decoder, the buffers'
   * positions must be reset before being passed into this method.
   *
   * This method does not change outputs and erasedIndexes.
   *
   * @param inputs input buffers used for decoding. The buffers' position
   *               are moved to the end after this method.
   * @param erasedIndexes indexes of erased units used for decoding
   * @param outputs decoded output buffers, which are ready to be read after
   *                the call
   * @throws IOException
   */
  public void validate(ByteBuffer[] inputs, int[] erasedIndexes,
      ByteBuffer[] outputs) throws IOException {
    markBuffers(outputs);

    try {
      ByteBuffer validInput = CoderUtil.findFirstValidInput(inputs);
      boolean isDirect = validInput.isDirect();
      int capacity = validInput.capacity();
      int remaining = validInput.remaining();

      // Init buffer
      if (buffer == null || buffer.isDirect() != isDirect
          || buffer.capacity() < remaining) {
        buffer = allocateBuffer(isDirect, capacity);
      }
      buffer.clear().limit(remaining);

      // Create newInputs and newErasedIndex for validation
      ByteBuffer[] newInputs = new ByteBuffer[inputs.length];
      int count = 0;
      for (int i = 0; i < erasedIndexes.length; i++) {
        newInputs[erasedIndexes[i]] = outputs[i];
        count++;
      }
      newErasedIndex = -1;
      boolean selected = false;
      int numValidIndexes = CoderUtil.getValidIndexes(inputs).length;
      for (int i = 0; i < newInputs.length; i++) {
        if (count == numValidIndexes) {
          break;
        } else if (!selected && inputs[i] != null) {
          newErasedIndex = i;
          newInputs[i] = null;
          selected = true;
        } else if (newInputs[i] == null) {
          newInputs[i] = inputs[i];
          if (inputs[i] != null) {
            count++;
          }
        }
      }

      // Keep it for testing
      newValidIndexes = CoderUtil.getValidIndexes(newInputs);

      decoder.decode(newInputs, new int[]{newErasedIndex},
          new ByteBuffer[]{buffer});

      if (!buffer.equals(inputs[newErasedIndex])) {
        throw new InvalidDecodingException("Failed to validate decoding");
      }
    } finally {
      toLimits(inputs);
      resetBuffers(outputs);
    }
  }

  /**
   *  Validate outputs decoded from inputs, by decoding an input back from
   *  those outputs and comparing it with the original one.
   * @param inputs input buffers used for decoding
   * @param erasedIndexes indexes of erased units used for decoding
   * @param outputs decoded output buffers
   * @throws IOException
   */
  public void validate(ECChunk[] inputs, int[] erasedIndexes, ECChunk[] outputs)
      throws IOException {
    ByteBuffer[] newInputs = CoderUtil.toBuffers(inputs);
    ByteBuffer[] newOutputs = CoderUtil.toBuffers(outputs);
    validate(newInputs, erasedIndexes, newOutputs);
  }

  private ByteBuffer allocateBuffer(boolean direct, int capacity) {
    if (direct) {
      buffer = ByteBuffer.allocateDirect(capacity);
    } else {
      buffer = ByteBuffer.allocate(capacity);
    }
    return buffer;
  }

  private static void markBuffers(ByteBuffer[] buffers) {
    for (ByteBuffer buffer: buffers) {
      if (buffer != null) {
        buffer.mark();
      }
    }
  }

  private static void resetBuffers(ByteBuffer[] buffers) {
    for (ByteBuffer buffer: buffers) {
      if (buffer != null) {
        buffer.reset();
      }
    }
  }

  private static void toLimits(ByteBuffer[] buffers) {
    for (ByteBuffer buffer: buffers) {
      if (buffer != null) {
        buffer.position(buffer.limit());
      }
    }
  }

  @VisibleForTesting
  protected int[] getNewValidIndexes() {
    return newValidIndexes;
  }

  @VisibleForTesting
  protected int getNewErasedIndex() {
    return newErasedIndex;
  }
}