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
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

/**
 * Utility class with methods for manipulating Writable objects
 */
public class Writables {
  /**
   * @param w
   * @return The bytes of <code>w</code> gotten by running its 
   * {@link Writable#write(java.io.DataOutput)} method.
   * @throws IOException
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
   * Set bytes into the passed Writable by calling its
   * {@link Writable#readFields(java.io.DataInput)}.
   * @param bytes
   * @param w An empty Writable (usually made by calling the null-arg
   * constructor).
   * @return The passed Writable after its readFields has been called fed
   * by the passed <code>bytes</code> array or IllegalArgumentException
   * if passed null or an empty <code>bytes</code> array.
   * @throws IOException
   * @throws IllegalArgumentException
   */
  public static Writable getWritable(final byte [] bytes, final Writable w)
  throws IOException {
    if (bytes == null || bytes.length == 0) {
      throw new IllegalArgumentException("Can't build a writable with empty " +
        "bytes array");
    }
    if (w == null) {
      throw new IllegalArgumentException("Writable cannot be null");
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
   * @param bytes
   * @return A HRegionInfo instance built out of passed <code>bytes</code>.
   * @throws IOException
   */
  public static HRegionInfo getHRegionInfo(final byte [] bytes)
  throws IOException {
    return (HRegionInfo)getWritable(bytes, new HRegionInfo());
  }
 
  /**
   * @param bytes
   * @return A HRegionInfo instance built out of passed <code>bytes</code>
   * or <code>null</code> if passed bytes are null or an empty array.
   * @throws IOException
   */
  public static HRegionInfo getHRegionInfoOrNull(final byte [] bytes)
  throws IOException {
    return (bytes == null || bytes.length <= 0)?
      (HRegionInfo)null: getHRegionInfo(bytes);
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
    if (src == null || tgt == null) {
      throw new IllegalArgumentException("Writables cannot be null");
    }
    byte [] bytes = getBytes(src);
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
    try {
      tgt.readFields(dis);
    } finally {
      dis.close();
    }
    return tgt;
  }
  
  /**
   * Convert a long value to a byte array
   * @param val
   * @return the byte array
   * @throws IOException
   */
  public static byte[] longToBytes(long val) throws IOException {
    return getBytes(new LongWritable(val));
  }
  
  /**
   * Converts a byte array to a long value
   * @param bytes
   * @return the long value
   * @throws IOException
   */
  public static long bytesToLong(byte[] bytes) throws IOException {
    if (bytes == null || bytes.length == 0) {
      return -1L;
    }
    return ((LongWritable) getWritable(bytes, new LongWritable())).get();
  }
  
  /**
   * Converts a string to a byte array in a consistent manner.
   * @param s
   * @return the byte array
   * @throws UnsupportedEncodingException
   */
  public static byte[] stringToBytes(String s)
  throws UnsupportedEncodingException {
    if (s == null) {
      throw new IllegalArgumentException("string cannot be null");
    }
    return s.getBytes(HConstants.UTF8_ENCODING);
  }
  
  /**
   * Converts a byte array to a string in a consistent manner.
   * @param bytes
   * @return the string
   * @throws UnsupportedEncodingException
   */
  public static String bytesToString(byte[] bytes)
  throws UnsupportedEncodingException {
    if (bytes == null || bytes.length == 0) {
      return "";
    }
    return new String(bytes, HConstants.UTF8_ENCODING);
  }
}
