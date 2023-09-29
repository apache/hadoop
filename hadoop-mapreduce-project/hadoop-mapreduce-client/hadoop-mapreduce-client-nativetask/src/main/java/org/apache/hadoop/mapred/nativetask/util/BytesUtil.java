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

import org.apache.hadoop.thirdparty.com.google.common.primitives.Ints;
import org.apache.hadoop.thirdparty.com.google.common.primitives.Longs;
import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
public class BytesUtil {

  private static final char[] HEX_CHARS =
      "0123456789abcdef".toCharArray();

  /**
   * Converts a big-endian byte array to a long value.
   *
   * @param bytes array of bytes
   * @param offset offset into array
   */
  public static long toLong(byte[] bytes, int offset) {
    return Longs.fromBytes(bytes[offset],
      bytes[offset + 1],
      bytes[offset + 2],
      bytes[offset + 3],
      bytes[offset + 4],
      bytes[offset + 5],
      bytes[offset + 6],
      bytes[offset + 7]);
  }

  /**
   * Convert a big-endian integer from a byte array to a primitive value.
   * @param bytes the array to parse from
   * @param offset the offset in the array
   */
  public static int toInt(byte[] bytes, int offset) {
    return Ints.fromBytes(bytes[offset],
      bytes[offset + 1],
      bytes[offset + 2],
      bytes[offset + 3]);
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
    return Float.intBitsToFloat(toInt(bytes, offset));
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
    return Double.longBitsToDouble(toLong(bytes, offset));
  }

  /**
   * Write a printable representation of a byte array.
   *
   * @param b byte array
   * @return the printable presentation
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
        result.append("\\x");
        result.append(HEX_CHARS[(ch >> 4) & 0x0F]);
        result.append(HEX_CHARS[ch & 0x0F]);
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
   * @param f float value
   * @return the float represented as byte []
   */
  public static byte [] toBytes(final float f) {
    // Encode it as int
    return Ints.toByteArray(Float.floatToRawIntBits(f));
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
    return Longs.toByteArray(Double.doubleToRawLongBits(d));
  }

}
