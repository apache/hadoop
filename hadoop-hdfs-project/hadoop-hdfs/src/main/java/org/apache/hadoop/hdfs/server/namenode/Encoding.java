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
package org.apache.hadoop.hdfs.server.namenode;

import com.google.protobuf.ByteString;

import java.nio.ByteBuffer;

/**
 * The encoding class defines the schema that maps the filesystem objects to
 * the underlying key-value (KV) store.
 *
 * The KV store consists of multiple tables, each of which has different
 * schemes. The first byte of the key specifies the table of the KV pair that
 * belongs to.
 *
 */
class Encoding {
  // The key of the root inode: I || INode.ROOT_ID || ''
  static final ByteString INODE_ROOT = ByteString.EMPTY;

  static final byte[] EMPTY_BYTES = new byte[0];
  static final String EMPTY_STRING = "";
  static final int SIZEOF_LONG = 8;
  static final int SIZEOF_INT = 4;

  private static void writeRawVarint64(long value, ByteBuffer buf) {
    while (true) {
      if ((value & ~0x7FL) == 0) {
        buf.put((byte) value);
        return;
      } else {
        buf.put((byte) (((int) value & 0x7F) | 0x80));
        value >>>= 7;
      }
    }
  }

  static int computeRawVarint32Size(final int value) {
    if ((value & (0xffffffff <<  7)) == 0) return 1;
    if ((value & (0xffffffff << 14)) == 0) return 2;
    if ((value & (0xffffffff << 21)) == 0) return 3;
    if ((value & (0xffffffff << 28)) == 0) return 4;
    return 5;
  }

  private static int computeRawVarint64Size(final long value) {
    if ((value & (0xffffffffffffffffL <<  7)) == 0) return 1;
    if ((value & (0xffffffffffffffffL << 14)) == 0) return 2;
    if ((value & (0xffffffffffffffffL << 21)) == 0) return 3;
    if ((value & (0xffffffffffffffffL << 28)) == 0) return 4;
    if ((value & (0xffffffffffffffffL << 35)) == 0) return 5;
    if ((value & (0xffffffffffffffffL << 42)) == 0) return 6;
    if ((value & (0xffffffffffffffffL << 49)) == 0) return 7;
    if ((value & (0xffffffffffffffffL << 56)) == 0) return 8;
    if ((value & (0xffffffffffffffffL << 63)) == 0) return 9;
    return 10;
  }

  static String readString(ByteBuffer buf) {
    int size = readRawVarint32(buf, buf.position());
    byte[] r  = new byte[size];
    ByteBuffer b = ((ByteBuffer) buf.slice().position(computeRawVarint32Size
      (size)));
    b.get(r);
    return new String(r);
  }

  static int readRawVarint32(ByteBuffer buf, int off) {
    int r = 0;
    byte b = (byte) 0x80;
    for (int i = 0; i < 5 && (b & 0x80) != 0; ++i) {
      b = buf.get(off + i);
      r = (r << 7) | (b & 0x7f);
    }
    return r;
  }

  static int computeArraySize(int length) {
    return computeRawVarint32Size(length) + length;
  }
}
