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
import org.apache.hadoop.io.erasurecode.ECChunk;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Helpful utilities for implementing some raw erasure coders.
 */
@InterfaceAudience.Private
public final class CoderUtil {

  private CoderUtil() {
    // No called
  }

  private static byte[] emptyChunk = new byte[4096];

  /**
   * Make sure to return an empty chunk buffer for the desired length.
   * @param leastLength
   * @return empty chunk of zero bytes
   */
  static byte[] getEmptyChunk(int leastLength) {
    if (emptyChunk.length >= leastLength) {
      return emptyChunk; // In most time
    }

    synchronized (CoderUtil.class) {
      emptyChunk = new byte[leastLength];
    }

    return emptyChunk;
  }

  /**
   * Ensure a buffer filled with ZERO bytes from current readable/writable
   * position.
   * @param buffer a buffer ready to read / write certain size bytes
   * @return the buffer itself, with ZERO bytes written, the position and limit
   *         are not changed after the call
   */
  static ByteBuffer resetBuffer(ByteBuffer buffer, int len) {
    int pos = buffer.position();
    buffer.put(getEmptyChunk(len), 0, len);
    buffer.position(pos);

    return buffer;
  }

  /**
   * Ensure the buffer (either input or output) ready to read or write with ZERO
   * bytes fully in specified length of len.
   * @param buffer bytes array buffer
   * @return the buffer itself
   */
  static byte[] resetBuffer(byte[] buffer, int offset, int len) {
    byte[] empty = getEmptyChunk(len);
    System.arraycopy(empty, 0, buffer, offset, len);

    return buffer;
  }

  /**
   * Initialize the output buffers with ZERO bytes.
   */
  static void resetOutputBuffers(ByteBuffer[] buffers, int dataLen) {
    for (ByteBuffer buffer : buffers) {
      resetBuffer(buffer, dataLen);
    }
  }

  /**
   * Initialize the output buffers with ZERO bytes.
   */
  static void resetOutputBuffers(byte[][] buffers, int[] offsets,
                                 int dataLen) {
    for (int i = 0; i < buffers.length; i++) {
      resetBuffer(buffers[i], offsets[i], dataLen);
    }
  }

  /**
   * Convert an array of this chunks to an array of ByteBuffers
   * @param chunks chunks to convertToByteArrayState into buffers
   * @return an array of ByteBuffers
   */
  static ByteBuffer[] toBuffers(ECChunk[] chunks) {
    ByteBuffer[] buffers = new ByteBuffer[chunks.length];

    ECChunk chunk;
    for (int i = 0; i < chunks.length; i++) {
      chunk = chunks[i];
      if (chunk == null) {
        buffers[i] = null;
      } else {
        buffers[i] = chunk.getBuffer();
        if (chunk.isAllZero()) {
          CoderUtil.resetBuffer(buffers[i], buffers[i].remaining());
        }
      }
    }

    return buffers;
  }

  /**
   * Clone an input bytes array as direct ByteBuffer.
   */
  static ByteBuffer cloneAsDirectByteBuffer(byte[] input, int offset, int len) {
    if (input == null) { // an input can be null, if erased or not to read
      return null;
    }

    ByteBuffer directBuffer = ByteBuffer.allocateDirect(len);
    directBuffer.put(input, offset, len);
    directBuffer.flip();
    return directBuffer;
  }

  /**
   * Get indexes array for items marked as null, either erased or
   * not to read.
   * @return indexes array
   */
  static <T> int[] getNullIndexes(T[] inputs) {
    int[] nullIndexes = new int[inputs.length];
    int idx = 0;
    for (int i = 0; i < inputs.length; i++) {
      if (inputs[i] == null) {
        nullIndexes[idx++] = i;
      }
    }

    return Arrays.copyOf(nullIndexes, idx);
  }

  /**
   * Find the valid input from all the inputs.
   * @param inputs input buffers to look for valid input
   * @return the first valid input
   */
  static <T> T findFirstValidInput(T[] inputs) {
    for (T input : inputs) {
      if (input != null) {
        return input;
      }
    }

    throw new HadoopIllegalArgumentException(
        "Invalid inputs are found, all being null");
  }

  /**
   * Picking up indexes of valid inputs.
   * @param inputs decoding input buffers
   * @param <T>
   */
  static <T> int[] getValidIndexes(T[] inputs) {
    int[] validIndexes = new int[inputs.length];
    int idx = 0;
    for (int i = 0; i < inputs.length; i++) {
      if (inputs[i] != null) {
        validIndexes[idx++] = i;
      }
    }

    return Arrays.copyOf(validIndexes, idx);
  }
}
