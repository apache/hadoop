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

package org.apache.hadoop.yarn.server.timeline.util;


import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.WritableComparator;

import java.io.IOException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.yarn.server.timeline.GenericObjectMapper.readReverseOrderedLong;

public class LeveldbUtils {

  /** A string builder utility for building timeline server leveldb keys. */
  public static class KeyBuilder {
    /** Maximum subkeys that can be added to construct a key. */
    private static final int MAX_NUMBER_OF_KEY_ELEMENTS = 10;
    private byte[][] b;
    private boolean[] useSeparator;
    private int index;
    private int length;

    public KeyBuilder(int size) {
      b = new byte[size][];
      useSeparator = new boolean[size];
      index = 0;
      length = 0;
    }

    public static KeyBuilder newInstance() {
      return new KeyBuilder(MAX_NUMBER_OF_KEY_ELEMENTS);
    }

    /** Instantiate a new key build with the given maximum subkes.
     * @param size maximum subkeys that can be added to this key builder
     * @return a newly constructed key builder */
    public static KeyBuilder newInstance(final int size) {
      return new KeyBuilder(size);
    }

    public KeyBuilder add(String s) {
      return add(s.getBytes(UTF_8), true);
    }

    public KeyBuilder add(byte[] t) {
      return add(t, false);
    }

    public KeyBuilder add(byte[] t, boolean sep) {
      b[index] = t;
      useSeparator[index] = sep;
      length += t.length;
      if (sep) {
        length++;
      }
      index++;
      return this;
    }

    /** Builds a byte array without the final string delimiter. */
    public byte[] getBytes() {
      // check the last valid entry to see the final length
      int bytesLength = length;
      if (useSeparator[index - 1]) {
        bytesLength = length - 1;
      }
      byte[] bytes = new byte[bytesLength];
      int curPos = 0;
      for (int i = 0; i < index; i++) {
        System.arraycopy(b[i], 0, bytes, curPos, b[i].length);
        curPos += b[i].length;
        if (i < index - 1 && useSeparator[i]) {
          bytes[curPos++] = 0x0;
        }
      }
      return bytes;
    }

    /** Builds a byte array including the final string delimiter. */
    public byte[] getBytesForLookup() {
      byte[] bytes = new byte[length];
      int curPos = 0;
      for (int i = 0; i < index; i++) {
        System.arraycopy(b[i], 0, bytes, curPos, b[i].length);
        curPos += b[i].length;
        if (useSeparator[i]) {
          bytes[curPos++] = 0x0;
        }
      }
      return bytes;
    }
  }

  public static class KeyParser {
    private final byte[] b;
    private int offset;

    public KeyParser(final byte[] b, final int offset) {
      this.b = b;
      this.offset = offset;
    }

    /** Returns a string from the offset until the next string delimiter. */
    public String getNextString() throws IOException {
      if (offset >= b.length) {
        throw new IOException(
            "tried to read nonexistent string from byte array");
      }
      int i = 0;
      while (offset + i < b.length && b[offset + i] != 0x0) {
        i++;
      }
      String s = new String(b, offset, i, UTF_8);
      offset = offset + i + 1;
      return s;
    }

    /** Moves current position until after the next end of string marker. */
    public void skipNextString() throws IOException {
      if (offset >= b.length) {
        throw new IOException("tried to read nonexistent string from byte array");
      }
      while (offset < b.length && b[offset] != 0x0) {
        ++offset;
      }
      ++offset;
    }

    /** Read the next 8 bytes in the byte buffer as a long. */
    public long getNextLong() throws IOException {
      if (offset + 8 >= b.length) {
        throw new IOException("byte array ran out when trying to read long");
      }
      long value = readReverseOrderedLong(b, offset);
      offset += 8;
      return value;
    }

    public int getOffset() {
      return offset;
    }

    /** Returns a copy of the remaining bytes. */
    public byte[] getRemainingBytes() {
      byte[] bytes = new byte[b.length - offset];
      System.arraycopy(b, offset, bytes, 0, b.length - offset);
      return bytes;
    }
  }

  /**
   * Returns true if the byte array begins with the specified prefix.
   */
  public static boolean prefixMatches(byte[] prefix, int prefixlen,
      byte[] b) {
    if (b.length < prefixlen) {
      return false;
    }
    return WritableComparator.compareBytes(prefix, 0, prefixlen, b, 0,
        prefixlen) == 0;
  }

  /**
   * Default permission mask for the level db dir
   */
  public static final FsPermission LEVELDB_DIR_UMASK = FsPermission
      .createImmutable((short) 0700);

}
