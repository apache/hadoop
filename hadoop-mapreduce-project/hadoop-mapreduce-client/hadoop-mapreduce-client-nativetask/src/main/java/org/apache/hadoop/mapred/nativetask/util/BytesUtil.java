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

package org.apache.hadoop.mapred.nativetask.util;

import java.io.UnsupportedEncodingException;

public class BytesUtil {

  public static final int SIZEOF_INT = Integer.SIZE / Byte.SIZE;
  public static final int SIZEOF_LONG = Long.SIZE / Byte.SIZE;

  public static byte[] toBytes(String str) {
    if (str == null) {
      return null;
    }
    try {
      return str.getBytes("utf-8");
    } catch (final UnsupportedEncodingException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  public static String fromBytes(byte[] data) {
    if (data == null) {
      return null;
    }
    try {
      return new String(data, "utf-8");
    } catch (final UnsupportedEncodingException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  /**
   * Converts a byte array to an int value
   * @param bytes byte array
   * @return the int value
   */
  public static int toInt(byte[] bytes) {
    return toInt(bytes, 0, SIZEOF_INT);
  }

  /**
   * Converts a byte array to an int value
   * @param bytes byte array
   * @param offset offset into array
   * @param length length of int (has to be {@link #SIZEOF_INT})
   * @return the int value
   * @throws RuntimeException if length is not {@link #SIZEOF_INT} or
   * if there's not enough room in the array at the offset indicated.
   */
  public static int toInt(byte[] bytes, int offset, final int length) {
    if (length != SIZEOF_INT || offset + length > bytes.length) {
      throw new RuntimeException(
        "toInt exception. length not equals to SIZE of Int or buffer overflow");
    }
    int n = 0;
    for (int i = offset; i< offset + length; i++) {
      n <<= 4;
      n ^= bytes[i] & 0xff;
    }
    return n;
  }

  /**
   * Converts a byte array to a long value.
   * @param bytes array
   * @return the long value
   */
  public static long toLong(byte[] bytes) {
    return toLong(bytes, 0, SIZEOF_LONG);
  }

   /**
   * Converts a byte array to a long value.
   *
   * @param bytes array of bytes
   * @param offset offset into array
   * @return the long value
   */
  public static long toLong(byte[] bytes, int offset) {
    return toLong(bytes, offset, SIZEOF_LONG);
  }

  /**
   * Converts a byte array to a long value.
   *
   * @param bytes array of bytes
   * @param offset offset into array
   * @param length length of data (must be {@link #SIZEOF_LONG})
   * @return the long value
   * @throws RuntimeException if length is not {@link #SIZEOF_LONG} or
   * if there's not enough room in the array at the offset indicated.
   */
  public static long toLong(byte[] bytes, int offset, final int length) {
    if (length != SIZEOF_LONG || offset + length > bytes.length) {
      throw new RuntimeException(
        "toLong exception. length not equals to SIZE of Long or buffer overflow");
    }
    long l = 0;
    for (int i = offset; i < offset + length; i++) {
      l <<= 8;
      l ^= bytes[i] & 0xff;
    }
    return l;
  }

  /**
   * Presumes float encoded as IEEE 754 floating-point "single format"
   * @param bytes byte array
   * @return Float made from passed byte array.
   */
  public static float toFloat(byte [] bytes) {
    return toFloat(bytes, 0);
  }

  /**
   * Presumes float encoded as IEEE 754 floating-point "single format"
   * @param bytes array to convert
   * @param offset offset into array
   * @return Float made from passed byte array.
   */
  public static float toFloat(byte [] bytes, int offset) {
    return Float.intBitsToFloat(toInt(bytes, offset, SIZEOF_INT));
  }

  /**
   * @param bytes byte array
   * @return Return double made from passed bytes.
   */
  public static double toDouble(final byte [] bytes) {
    return toDouble(bytes, 0);
  }

  /**
   * @param bytes byte array
   * @param offset offset where double is
   * @return Return double made from passed bytes.
   */
  public static double toDouble(final byte [] bytes, final int offset) {
    return Double.longBitsToDouble(toLong(bytes, offset, SIZEOF_LONG));
  }

  /**
   * Write a printable representation of a byte array.
   *
   * @param b byte array
   * @return string
   * @see #toStringBinary(byte[], int, int)
   */
  public static String toStringBinary(final byte [] b) {
    if (b == null)
      return "null";
    return toStringBinary(b, 0, b.length);
  }

  /**
   * Write a printable representation of a byte array. Non-printable
   * characters are hex escaped in the format \\x%02X, eg:
   * \x00 \x05 etc
   *
   * @param b array to write out
   * @param off offset to start at
   * @param len length to write
   * @return string output
   */
  public static String toStringBinary(final byte [] b, int off, int len) {
    StringBuilder result = new StringBuilder();
    // Just in case we are passed a 'len' that is > buffer length...
    if (off >= b.length) return result.toString();
    if (off + len > b.length) len = b.length - off;
    for (int i = off; i < off + len ; ++i ) {
      int ch = b[i] & 0xFF;
      if ( (ch >= '0' && ch <= '9')
        || (ch >= 'A' && ch <= 'Z')
        || (ch >= 'a' && ch <= 'z')
        || " `~!@#$%^&*()-_=+[]{}|;:'\",.<>/?".indexOf(ch) >= 0 ) {
        result.append((char)ch);
      } else {
        result.append(String.format("\\x%02X", ch));
      }
    }
    return result.toString();
  }

  /**
   * Convert a boolean to a byte array. True becomes -1
   * and false becomes 0.
   *
   * @param b value
   * @return <code>b</code> encoded in a byte array.
   */
  public static byte [] toBytes(final boolean b) {
    return new byte[] { b ? (byte) -1 : (byte) 0 };
  }

  /**
   * Convert an int value to a byte array.  Big-endian.  Same as what DataOutputStream.writeInt
   * does.
   *
   * @param val value
   * @return the byte array
   */
  public static byte[] toBytes(int val) {
    byte [] b = new byte[4];
    for(int i = 3; i > 0; i--) {
      b[i] = (byte) val;
      val >>>= 8;
    }
    b[0] = (byte) val;
    return b;
  }

  /**
   * Convert a long value to a byte array using big-endian.
   *
   * @param val value to convert
   * @return the byte array
   */
  public static byte[] toBytes(long val) {
    byte [] b = new byte[8];
    for (int i = 7; i > 0; i--) {
      b[i] = (byte) val;
      val >>>= 8;
    }
    b[0] = (byte) val;
    return b;
  }

  /**
   * @param f float value
   * @return the float represented as byte []
   */
  public static byte [] toBytes(final float f) {
    // Encode it as int
    return toBytes(Float.floatToRawIntBits(f));
  }

  /**
   * Serialize a double as the IEEE 754 double format output. The resultant
   * array will be 8 bytes long.
   *
   * @param d value
   * @return the double represented as byte []
   */
  public static byte [] toBytes(final double d) {
    // Encode it as a long
    return toBytes(Double.doubleToRawLongBits(d));
  }

}
