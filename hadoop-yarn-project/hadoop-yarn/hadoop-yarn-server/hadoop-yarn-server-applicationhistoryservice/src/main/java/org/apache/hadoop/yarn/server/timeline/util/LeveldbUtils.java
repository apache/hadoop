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


import org.apache.hadoop.io.WritableComparator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;

import static org.apache.hadoop.yarn.server.timeline.GenericObjectMapper.readReverseOrderedLong;

public class LeveldbUtils {

  public static class KeyBuilder {
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

    public KeyBuilder add(String s) {
      return add(s.getBytes(Charset.forName("UTF-8")), true);
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

    public byte[] getBytes() throws IOException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(length);
      for (int i = 0; i < index; i++) {
        baos.write(b[i]);
        if (i < index - 1 && useSeparator[i]) {
          baos.write(0x0);
        }
      }
      return baos.toByteArray();
    }

    public byte[] getBytesForLookup() throws IOException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(length);
      for (int i = 0; i < index; i++) {
        baos.write(b[i]);
        if (useSeparator[i]) {
          baos.write(0x0);
        }
      }
      return baos.toByteArray();
    }
  }

  public static class KeyParser {
    private final byte[] b;
    private int offset;

    public KeyParser(byte[] b, int offset) {
      this.b = b;
      this.offset = offset;
    }

    public String getNextString() throws IOException {
      if (offset >= b.length) {
        throw new IOException(
            "tried to read nonexistent string from byte array");
      }
      int i = 0;
      while (offset + i < b.length && b[offset + i] != 0x0) {
        i++;
      }
      String s = new String(b, offset, i, Charset.forName("UTF-8"));
      offset = offset + i + 1;
      return s;
    }

    public long getNextLong() throws IOException {
      if (offset + 8 >= b.length) {
        throw new IOException("byte array ran out when trying to read long");
      }
      long l = readReverseOrderedLong(b, offset);
      offset += 8;
      return l;
    }

    public int getOffset() {
      return offset;
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

}
