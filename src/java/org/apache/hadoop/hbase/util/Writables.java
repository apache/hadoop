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
package org.apache.hadoop.hbase.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Writable;


public class Writables {
  /**
   * @param w
   * @return The bytes of <code>w</code> gotten by running its 
   * {@link Writable#write(java.io.DataOutput)} method.
   * @throws IOException
   * @see #getWritable(byte[], Writable)
   */
  public static byte [] getBytes(final Writable w) throws IOException {
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(byteStream);
    try {
      w.write(out);
      out.close();
      out = null;
      return byteStream.toByteArray();
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }

  /**
   * Set bytes into the passed Writable by calling its
   * {@link Writable#readFields(java.io.DataInput)}.
   * @param bytes
   * @param w An empty Writable (usually made by calling the null-arg
   * constructor).
   * @return The passed Writable after its readFields has been called fed
   * by the passed <code>bytes</code> array or null if passed null or
   * empty <code>bytes</code>.
   * @throws IOException
   */
  public static Writable getWritable(final byte [] bytes, final Writable w)
  throws IOException {
    if (bytes == null || bytes.length == 0) {
      throw new IOException("Con't build a writable with empty bytes array");
    }
    DataInputBuffer in = new DataInputBuffer();
    try {
      in.reset(bytes, bytes.length);
      w.readFields(in);
      return w;
    } finally {
      in.close();
    }
  }

  /**
   * Copy one Writable to another.  Copies bytes using data streams.
   * @param src Source Writable
   * @param tgt Target Writable
   * @return The target Writable.
   * @throws IOException
   */
  public static Writable copyWritable(final Writable src, final Writable tgt)
  throws IOException {
    byte [] bytes = getBytes(src);
    DataInputStream dis = null;
    try {
      dis = new DataInputStream(new ByteArrayInputStream(bytes));
      tgt.readFields(dis);
    } finally {
      dis.close();
    }
    return tgt;
  }
}
