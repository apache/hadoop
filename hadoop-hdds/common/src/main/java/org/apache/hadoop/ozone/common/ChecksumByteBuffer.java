/*
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
 *
 * Some portions of this file Copyright (c) 2004-2006 Intel Corportation
 * and licensed under the BSD license.
 */
package org.apache.hadoop.ozone.common;

import org.apache.ratis.util.Preconditions;

import java.nio.ByteBuffer;
import java.util.zip.Checksum;

/**
 * A sub-interface of {@link Checksum}
 * with a method to update checksum from a {@link ByteBuffer}.
 */
public interface ChecksumByteBuffer extends Checksum {
  /**
   * Updates the current checksum with the specified bytes in the buffer.
   * Upon return, the buffer's position will be equal to its limit.
   *
   * @param buffer the bytes to update the checksum with
   */
  void update(ByteBuffer buffer);

  @Override
  default void update(byte[] b, int off, int len) {
    update(ByteBuffer.wrap(b, off, len).asReadOnlyBuffer());
  }

  /**
   * An abstract class implementing {@link ChecksumByteBuffer}
   * with a 32-bit checksum and a lookup table.
   */
  @SuppressWarnings("innerassignment")
  abstract class CrcIntTable implements ChecksumByteBuffer {
    /** Current CRC value with bit-flipped. */
    private int crc;

    CrcIntTable() {
      reset();
      Preconditions.assertTrue(getTable().length == 8 * (1 << 8));
    }

    abstract int[] getTable();

    @Override
    public final long getValue() {
      return (~crc) & 0xffffffffL;
    }

    @Override
    public final void reset() {
      crc = 0xffffffff;
    }

    @Override
    public final void update(int b) {
      crc = (crc >>> 8) ^ getTable()[(((crc ^ b) << 24) >>> 24)];
    }

    @Override
    public final void update(ByteBuffer b) {
      crc = update(crc, b, getTable());
    }

    private static int update(int crc, ByteBuffer b, int[] table) {
      for(; b.remaining() > 7;) {
        final int c0 = (b.get() ^ crc) & 0xff;
        final int c1 = (b.get() ^ (crc >>>= 8)) & 0xff;
        final int c2 = (b.get() ^ (crc >>>= 8)) & 0xff;
        final int c3 = (b.get() ^ (crc >>> 8)) & 0xff;
        crc = (table[0x700 + c0] ^ table[0x600 + c1])
            ^ (table[0x500 + c2] ^ table[0x400 + c3]);

        final int c4 = b.get() & 0xff;
        final int c5 = b.get() & 0xff;
        final int c6 = b.get() & 0xff;
        final int c7 = b.get() & 0xff;

        crc ^= (table[0x300 + c4] ^ table[0x200 + c5])
            ^ (table[0x100 + c6] ^ table[c7]);
      }

      // loop unroll - duff's device style
      switch (b.remaining()) {
      case 7:
        crc = (crc >>> 8) ^ table[((crc ^ b.get()) & 0xff)];
      case 6:
        crc = (crc >>> 8) ^ table[((crc ^ b.get()) & 0xff)];
      case 5:
        crc = (crc >>> 8) ^ table[((crc ^ b.get()) & 0xff)];
      case 4:
        crc = (crc >>> 8) ^ table[((crc ^ b.get()) & 0xff)];
      case 3:
        crc = (crc >>> 8) ^ table[((crc ^ b.get()) & 0xff)];
      case 2:
        crc = (crc >>> 8) ^ table[((crc ^ b.get()) & 0xff)];
      case 1:
        crc = (crc >>> 8) ^ table[((crc ^ b.get()) & 0xff)];
      default: // noop
      }

      return crc;
    }
  }
}
