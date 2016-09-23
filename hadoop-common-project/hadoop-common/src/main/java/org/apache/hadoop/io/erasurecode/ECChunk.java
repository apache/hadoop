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
package org.apache.hadoop.io.erasurecode;

import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * A wrapper for ByteBuffer or bytes array for an erasure code chunk.
 */
@InterfaceAudience.Private
public class ECChunk {

  private ByteBuffer chunkBuffer;

  // TODO: should be in a more general flags
  private boolean allZero = false;

  /**
   * Wrapping a ByteBuffer
   * @param buffer buffer to be wrapped by the chunk
   */
  public ECChunk(ByteBuffer buffer) {
    this.chunkBuffer = buffer;
  }

  public ECChunk(ByteBuffer buffer, int offset, int len) {
    ByteBuffer tmp = buffer.duplicate();
    tmp.position(offset);
    tmp.limit(offset + len);
    this.chunkBuffer = tmp.slice();
  }

  /**
   * Wrapping a bytes array
   * @param buffer buffer to be wrapped by the chunk
   */
  public ECChunk(byte[] buffer) {
    this.chunkBuffer = ByteBuffer.wrap(buffer);
  }

  public ECChunk(byte[] buffer, int offset, int len) {
    this.chunkBuffer = ByteBuffer.wrap(buffer, offset, len);
  }

  public boolean isAllZero() {
    return allZero;
  }

  public void setAllZero(boolean allZero) {
    this.allZero = allZero;
  }

  /**
   * Convert to ByteBuffer
   * @return ByteBuffer
   */
  public ByteBuffer getBuffer() {
    return chunkBuffer;
  }

  /**
   * Convert an array of this chunks to an array of ByteBuffers
   * @param chunks chunks to convert into buffers
   * @return an array of ByteBuffers
   */
  public static ByteBuffer[] toBuffers(ECChunk[] chunks) {
    ByteBuffer[] buffers = new ByteBuffer[chunks.length];

    ECChunk chunk;
    for (int i = 0; i < chunks.length; i++) {
      chunk = chunks[i];
      if (chunk == null) {
        buffers[i] = null;
      } else {
        buffers[i] = chunk.getBuffer();
      }
    }

    return buffers;
  }

  /**
   * Convert to a bytes array, just for test usage.
   * @return bytes array
   */
  public byte[] toBytesArray() {
    byte[] bytesArr = new byte[chunkBuffer.remaining()];
    // Avoid affecting the original one
    chunkBuffer.mark();
    chunkBuffer.get(bytesArr);
    chunkBuffer.reset();

    return bytesArr;
  }
}
