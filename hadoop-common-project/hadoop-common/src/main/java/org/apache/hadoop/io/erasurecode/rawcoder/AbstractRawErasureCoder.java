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
import org.apache.hadoop.conf.Configured;

import java.nio.ByteBuffer;

/**
 * A common class of basic facilities to be shared by encoder and decoder
 *
 * It implements the {@link RawErasureCoder} interface.
 */
@InterfaceAudience.Private
public abstract class AbstractRawErasureCoder
    extends Configured implements RawErasureCoder {

  private final int numDataUnits;
  private final int numParityUnits;
  private final int numAllUnits;

  public AbstractRawErasureCoder(int numDataUnits, int numParityUnits) {
    this.numDataUnits = numDataUnits;
    this.numParityUnits = numParityUnits;
    this.numAllUnits = numDataUnits + numParityUnits;
  }

  @Override
  public int getNumDataUnits() {
    return numDataUnits;
  }

  @Override
  public int getNumParityUnits() {
    return numParityUnits;
  }

  protected int getNumAllUnits() {
    return numAllUnits;
  }

  @Override
  public boolean preferDirectBuffer() {
    return false;
  }

  @Override
  public void release() {
    // Nothing to do by default
  }

  /**
   * Ensure a buffer filled with ZERO bytes from current readable/writable
   * position.
   * @param buffer a buffer ready to read / write certain size bytes
   * @return the buffer itself, with ZERO bytes written, the position and limit
   *         are not changed after the call
   */
  protected ByteBuffer resetBuffer(ByteBuffer buffer) {
    int pos = buffer.position();
    for (int i = pos; i < buffer.limit(); ++i) {
      buffer.put((byte) 0);
    }
    buffer.position(pos);

    return buffer;
  }

  /**
   * Ensure the buffer (either input or output) ready to read or write with ZERO
   * bytes fully in specified length of len.
   * @param buffer bytes array buffer
   * @return the buffer itself
   */
  protected byte[] resetBuffer(byte[] buffer, int offset, int len) {
    for (int i = offset; i < len; ++i) {
      buffer[i] = (byte) 0;
    }

    return buffer;
  }

  /**
   * Check and ensure the buffers are of the length specified by dataLen, also
   * ensure the buffers are direct buffers or not according to isDirectBuffer.
   * @param buffers the buffers to check
   * @param allowNull whether to allow any element to be null or not
   * @param dataLen the length of data available in the buffer to ensure with
   * @param isDirectBuffer is direct buffer or not to ensure with
   */
  protected void ensureLengthAndType(ByteBuffer[] buffers, boolean allowNull,
                                     int dataLen, boolean isDirectBuffer) {
    for (ByteBuffer buffer : buffers) {
      if (buffer == null && !allowNull) {
        throw new HadoopIllegalArgumentException(
            "Invalid buffer found, not allowing null");
      } else if (buffer != null) {
        if (buffer.remaining() != dataLen) {
          throw new HadoopIllegalArgumentException(
              "Invalid buffer, not of length " + dataLen);
        }
        if (buffer.isDirect() != isDirectBuffer) {
          throw new HadoopIllegalArgumentException(
              "Invalid buffer, isDirect should be " + isDirectBuffer);
        }
      }
    }
  }

  /**
   * Check and ensure the buffers are of the length specified by dataLen.
   * @param buffers the buffers to check
   * @param allowNull whether to allow any element to be null or not
   * @param dataLen the length of data available in the buffer to ensure with
   */
  protected void ensureLength(byte[][] buffers,
                              boolean allowNull, int dataLen) {
    for (byte[] buffer : buffers) {
      if (buffer == null && !allowNull) {
        throw new HadoopIllegalArgumentException(
            "Invalid buffer found, not allowing null");
      } else if (buffer != null && buffer.length != dataLen) {
        throw new HadoopIllegalArgumentException(
            "Invalid buffer not of length " + dataLen);
      }
    }
  }
}
