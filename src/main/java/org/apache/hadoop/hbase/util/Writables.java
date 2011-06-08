/**
 * Copyright 2010 The Apache Software Foundation
 *
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

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.migration.HRegionInfo090x;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Writable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class with methods for manipulating Writable objects
 */
public class Writables {
  /**
   * @param w writable
   * @return The bytes of <code>w</code> gotten by running its
   * {@link Writable#write(java.io.DataOutput)} method.
   * @throws IOException e
   * @see #getWritable(byte[], Writable)
   */
  public static byte [] getBytes(final Writable w) throws IOException {
    if (w == null) {
      throw new IllegalArgumentException("Writable cannot be null");
    }
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
   * Put a bunch of Writables as bytes all into the one byte array.
   * @param ws writable
   * @return The bytes of <code>w</code> gotten by running its
   * {@link Writable#write(java.io.DataOutput)} method.
   * @throws IOException e
   * @see #getHRegionInfos(byte[], int, int)
   */
  public static byte [] getBytes(final Writable... ws) throws IOException {
    List<byte []> bytes = new ArrayList<byte []>();
    int size = 0;
    for (Writable w: ws) {
      byte [] b = getBytes(w);
      size += b.length;
      bytes.add(b);
    }
    byte [] result = new byte[size];
    int offset = 0;
    for (byte [] b: bytes) {
      System.arraycopy(b, 0, result, offset, b.length);
      offset += b.length;
    }
    return result;
  }

  /**
   * Set bytes into the passed Writable by calling its
   * {@link Writable#readFields(java.io.DataInput)}.
   * @param bytes serialized bytes
   * @param w An empty Writable (usually made by calling the null-arg
   * constructor).
   * @return The passed Writable after its readFields has been called fed
   * by the passed <code>bytes</code> array or IllegalArgumentException
   * if passed null or an empty <code>bytes</code> array.
   * @throws IOException e
   * @throws IllegalArgumentException
   */
  public static Writable getWritable(final byte [] bytes, final Writable w)
  throws IOException {
    return getWritable(bytes, 0, bytes.length, w);
  }

  /**
   * Set bytes into the passed Writable by calling its
   * {@link Writable#readFields(java.io.DataInput)}.
   * @param bytes serialized bytes
   * @param offset offset into array
   * @param length length of data
   * @param w An empty Writable (usually made by calling the null-arg
   * constructor).
   * @return The passed Writable after its readFields has been called fed
   * by the passed <code>bytes</code> array or IllegalArgumentException
   * if passed null or an empty <code>bytes</code> array.
   * @throws IOException e
   * @throws IllegalArgumentException
   */
  public static Writable getWritable(final byte [] bytes, final int offset,
    final int length, final Writable w)
  throws IOException {
    if (bytes == null || length <=0) {
      throw new IllegalArgumentException("Can't build a writable with empty " +
        "bytes array");
    }
    if (w == null) {
      throw new IllegalArgumentException("Writable cannot be null");
    }
    DataInputBuffer in = new DataInputBuffer();
    try {
      in.reset(bytes, offset, length);
      w.readFields(in);
      return w;
    } finally {
      in.close();
    }
  }

  /**
   * @param bytes serialized bytes
   * @return A HRegionInfo instance built out of passed <code>bytes</code>.
   * @throws IOException e
   */
  public static HRegionInfo getHRegionInfo(final byte [] bytes)
  throws IOException {
    return (HRegionInfo)getWritable(bytes, new HRegionInfo());
  }

  /**
   * @param bytes serialized bytes
   * @return All the hregioninfos that are in the byte array.  Keeps reading
   * till we hit the end.
   * @throws IOException e
   */
  public static List<HRegionInfo> getHRegionInfos(final byte [] bytes,
      final int offset, final int length)
  throws IOException {
    if (bytes == null) {
      throw new IllegalArgumentException("Can't build a writable with empty " +
        "bytes array");
    }
    DataInputBuffer in = new DataInputBuffer();
    List<HRegionInfo> hris = new ArrayList<HRegionInfo>();
    try {
      in.reset(bytes, offset, length);
      while (in.available() > 0) {
        HRegionInfo hri = new HRegionInfo();
        hri.readFields(in);
        hris.add(hri);
      }
    } finally {
      in.close();
    }
    return hris;
  }

  /**
   * @param bytes serialized bytes
   * @return A HRegionInfo instance built out of passed <code>bytes</code>
   * or <code>null</code> if passed bytes are null or an empty array.
   * @throws IOException e
   */
  public static HRegionInfo getHRegionInfoOrNull(final byte [] bytes)
  throws IOException {
    return (bytes == null || bytes.length <= 0)?
        null : getHRegionInfo(bytes);
  }

  /**
   * Copy one Writable to another.  Copies bytes using data streams.
   * @param src Source Writable
   * @param tgt Target Writable
   * @return The target Writable.
   * @throws IOException e
   */
  public static Writable copyWritable(final Writable src, final Writable tgt)
  throws IOException {
    return copyWritable(getBytes(src), tgt);
  }

  /**
   * Copy one Writable to another.  Copies bytes using data streams.
   * @param bytes Source Writable
   * @param tgt Target Writable
   * @return The target Writable.
   * @throws IOException e
   */
  public static Writable copyWritable(final byte [] bytes, final Writable tgt)
  throws IOException {
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
    try {
      tgt.readFields(dis);
    } finally {
      dis.close();
    }
    return tgt;
  }

  /**
   * Get HREgionInfoForMigration serialized from bytes.
   * @param bytes serialized bytes
   * @return HRegionInfoForMigration
   * @throws IOException
   */
  public static HRegionInfo090x getHRegionInfoForMigration(final byte [] bytes)
  throws IOException {
    return (HRegionInfo090x)getWritable(bytes, new HRegionInfo090x());
  }

}