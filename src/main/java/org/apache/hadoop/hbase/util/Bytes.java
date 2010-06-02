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

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;

/**
 * Utility class that handles byte arrays, conversions to/from other types,
 * comparisons, hash code generation, manufacturing keys for HashMaps or
 * HashSets, etc.
 */
public class Bytes {

  private static final Log LOG = LogFactory.getLog(Bytes.class);

  /**
   * Size of boolean in bytes
   */
  public static final int SIZEOF_BOOLEAN = Byte.SIZE / Byte.SIZE;

  /**
   * Size of byte in bytes
   */
  public static final int SIZEOF_BYTE = SIZEOF_BOOLEAN;

  /**
   * Size of char in bytes
   */
  public static final int SIZEOF_CHAR = Character.SIZE / Byte.SIZE;

  /**
   * Size of double in bytes
   */
  public static final int SIZEOF_DOUBLE = Double.SIZE / Byte.SIZE;

  /**
   * Size of float in bytes
   */
  public static final int SIZEOF_FLOAT = Float.SIZE / Byte.SIZE;

  /**
   * Size of int in bytes
   */
  public static final int SIZEOF_INT = Integer.SIZE / Byte.SIZE;

  /**
   * Size of long in bytes
   */
  public static final int SIZEOF_LONG = Long.SIZE / Byte.SIZE;

  /**
   * Size of short in bytes
   */
  public static final int SIZEOF_SHORT = Short.SIZE / Byte.SIZE;


  /**
   * Estimate of size cost to pay beyond payload in jvm for instance of byte [].
   * Estimate based on study of jhat and jprofiler numbers.
   */
  // JHat says BU is 56 bytes.
  // SizeOf which uses java.lang.instrument says 24 bytes. (3 longs?)
  public static final int ESTIMATED_HEAP_TAX = 16;

  /**
   * Byte array comparator class.
   */
  public static class ByteArrayComparator implements RawComparator<byte []> {
    /**
     * Constructor
     */
    public ByteArrayComparator() {
      super();
    }
    public int compare(byte [] left, byte [] right) {
      return compareTo(left, right);
    }
    public int compare(byte [] b1, int s1, int l1, byte [] b2, int s2, int l2) {
      return compareTo(b1, s1, l1, b2, s2, l2);
    }
  }

  /**
   * Pass this to TreeMaps where byte [] are keys.
   */
  public static Comparator<byte []> BYTES_COMPARATOR =
    new ByteArrayComparator();

  /**
   * Use comparing byte arrays, byte-by-byte
   */
  public static RawComparator<byte []> BYTES_RAWCOMPARATOR =
    new ByteArrayComparator();

  /**
   * Read byte-array written with a WritableableUtils.vint prefix.
   * @param in Input to read from.
   * @return byte array read off <code>in</code>
   * @throws IOException e
   */
  public static byte [] readByteArray(final DataInput in)
  throws IOException {
    int len = WritableUtils.readVInt(in);
    if (len < 0) {
      throw new NegativeArraySizeException(Integer.toString(len));
    }
    byte [] result = new byte[len];
    in.readFully(result, 0, len);
    return result;
  }

  /**
   * Read byte-array written with a WritableableUtils.vint prefix.
   * IOException is converted to a RuntimeException.
   * @param in Input to read from.
   * @return byte array read off <code>in</code>
   */
  public static byte [] readByteArrayThrowsRuntime(final DataInput in) {
    try {
      return readByteArray(in);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Write byte-array with a WritableableUtils.vint prefix.
   * @param out output stream to be written to
   * @param b array to write
   * @throws IOException e
   */
  public static void writeByteArray(final DataOutput out, final byte [] b)
  throws IOException {
    if(b == null) {
      WritableUtils.writeVInt(out, 0);
    } else {
      writeByteArray(out, b, 0, b.length);
    }
  }

  /**
   * Write byte-array to out with a vint length prefix.
   * @param out output stream
   * @param b array
   * @param offset offset into array
   * @param length length past offset
   * @throws IOException e
   */
  public static void writeByteArray(final DataOutput out, final byte [] b,
      final int offset, final int length)
  throws IOException {
    WritableUtils.writeVInt(out, length);
    out.write(b, offset, length);
  }

  /**
   * Write byte-array from src to tgt with a vint length prefix.
   * @param tgt target array
   * @param tgtOffset offset into target array
   * @param src source array
   * @param srcOffset source offset
   * @param srcLength source length
   * @return New offset in src array.
   */
  public static int writeByteArray(final byte [] tgt, final int tgtOffset,
      final byte [] src, final int srcOffset, final int srcLength) {
    byte [] vint = vintToBytes(srcLength);
    System.arraycopy(vint, 0, tgt, tgtOffset, vint.length);
    int offset = tgtOffset + vint.length;
    System.arraycopy(src, srcOffset, tgt, offset, srcLength);
    return offset + srcLength;
  }

  /**
   * Put bytes at the specified byte array position.
   * @param tgtBytes the byte array
   * @param tgtOffset position in the array
   * @param srcBytes array to write out
   * @param srcOffset source offset
   * @param srcLength source length
   * @return incremented offset
   */
  public static int putBytes(byte[] tgtBytes, int tgtOffset, byte[] srcBytes,
      int srcOffset, int srcLength) {
    System.arraycopy(srcBytes, srcOffset, tgtBytes, tgtOffset, srcLength);
    return tgtOffset + srcLength;
  }

  /**
   * Write a single byte out to the specified byte array position.
   * @param bytes the byte array
   * @param offset position in the array
   * @param b byte to write out
   * @return incremented offset
   */
  public static int putByte(byte[] bytes, int offset, byte b) {
    bytes[offset] = b;
    return offset + 1;
  }

  /**
   * Returns a new byte array, copied from the passed ByteBuffer.
   * @param bb A ByteBuffer
   * @return the byte array
   */
  public static byte[] toBytes(ByteBuffer bb) {
    int length = bb.limit();
    byte [] result = new byte[length];
    System.arraycopy(bb.array(), bb.arrayOffset(), result, 0, length);
    return result;
  }

  /**
   * @param b Presumed UTF-8 encoded byte array.
   * @return String made from <code>b</code>
   */
  public static String toString(final byte [] b) {
    if (b == null) {
      return null;
    }
    return toString(b, 0, b.length);
  }

  /**
   * Joins two byte arrays together using a separator.
   * @param b1 The first byte array.
   * @param sep The separator to use.
   * @param b2 The second byte array.
   */
  public static String toString(final byte [] b1,
                                String sep,
                                final byte [] b2) {
    return toString(b1, 0, b1.length) + sep + toString(b2, 0, b2.length);
  }

  /**
   * This method will convert utf8 encoded bytes into a string. If
   * an UnsupportedEncodingException occurs, this method will eat it
   * and return null instead.
   *
   * @param b Presumed UTF-8 encoded byte array.
   * @param off offset into array
   * @param len length of utf-8 sequence
   * @return String made from <code>b</code> or null
   */
  public static String toString(final byte [] b, int off, int len) {
    if (b == null) {
      return null;
    }
    if (len == 0) {
      return "";
    }
    try {
      return new String(b, off, len, HConstants.UTF8_ENCODING);
    } catch (UnsupportedEncodingException e) {
      LOG.error("UTF-8 not supported?", e);
      return null;
    }
  }

  /**
   * Write a printable representation of a byte array.
   *
   * @param b byte array
   * @return string
   * @see #toStringBinary(byte[], int, int)
   */
  public static String toStringBinary(final byte [] b) {
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
    try {
      String first = new String(b, off, len, "ISO-8859-1");
      for (int i = 0; i < first.length() ; ++i ) {
        int ch = first.charAt(i) & 0xFF;
        if ( (ch >= '0' && ch <= '9')
            || (ch >= 'A' && ch <= 'Z')
            || (ch >= 'a' && ch <= 'z')
            || ch == ','
            || ch == '_'
            || ch == '-'
            || ch == ':'
            || ch == ' '
            || ch == '<'
            || ch == '>'
            || ch == '='
            || ch == '/'
            || ch == '.') {
          result.append(first.charAt(i));
        } else {
          result.append(String.format("\\x%02X", ch));
        }
      }
    } catch (UnsupportedEncodingException e) {
      LOG.error("ISO-8859-1 not supported?", e);
    }
    return result.toString();
  }

  private static boolean isHexDigit(char c) {
    return
        (c >= 'A' && c <= 'F') ||
        (c >= '0' && c <= '9');
  }

  /**
   * Takes a ASCII digit in the range A-F0-9 and returns
   * the corresponding integer/ordinal value.
   * @param ch  The hex digit.
   * @return The converted hex value as a byte.
   */
  public static byte toBinaryFromHex(byte ch) {
    if ( ch >= 'A' && ch <= 'F' )
      return (byte) ((byte)10 + (byte) (ch - 'A'));
    // else
    return (byte) (ch - '0');
  }

  public static byte [] toBytesBinary(String in) {
    // this may be bigger than we need, but lets be safe.
    byte [] b = new byte[in.length()];
    int size = 0;
    for (int i = 0; i < in.length(); ++i) {
      char ch = in.charAt(i);
      if (ch == '\\') {
        // begin hex escape:
        char next = in.charAt(i+1);
        if (next != 'x') {
          // invalid escape sequence, ignore this one.
          b[size++] = (byte)ch;
          continue;
        }
        // ok, take next 2 hex digits.
        char hd1 = in.charAt(i+2);
        char hd2 = in.charAt(i+3);

        // they need to be A-F0-9:
        if (!isHexDigit(hd1) ||
            !isHexDigit(hd2)) {
          // bogus escape code, ignore:
          continue;
        }
        // turn hex ASCII digit -> number
        byte d = (byte) ((toBinaryFromHex((byte)hd1) << 4) + toBinaryFromHex((byte)hd2));

        b[size++] = d;
        i += 3; // skip 3
      } else {
        b[size++] = (byte) ch;
      }
    }
    // resize:
    byte [] b2 = new byte[size];
    System.arraycopy(b, 0, b2, 0, size);
    return b2;
  }

  /**
   * Converts a string to a UTF-8 byte array.
   * @param s string
   * @return the byte array
   */
  public static byte[] toBytes(String s) {
    try {
      return s.getBytes(HConstants.UTF8_ENCODING);
    } catch (UnsupportedEncodingException e) {
      LOG.error("UTF-8 not supported?", e);
      return null;
    }
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
   * Reverses {@link #toBytes(boolean)}
   * @param b array
   * @return True or false.
   */
  public static boolean toBoolean(final byte [] b) {
    if (b.length != 1) {
      throw new IllegalArgumentException("Array has wrong size: " + b.length);
    }
    return b[0] != (byte) 0;
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
   * Converts a byte array to a long value. Reverses
   * {@link #toBytes(long)}
   * @param bytes array
   * @return the long value
   */
  public static long toLong(byte[] bytes) {
    return toLong(bytes, 0, SIZEOF_LONG);
  }

  /**
   * Converts a byte array to a long value. Assumes there will be
   * {@link #SIZEOF_LONG} bytes available.
   *
   * @param bytes bytes
   * @param offset offset
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
   * @throws IllegalArgumentException if length is not {@link #SIZEOF_LONG} or
   * if there's not enough room in the array at the offset indicated.
   */
  public static long toLong(byte[] bytes, int offset, final int length) {
    if (length != SIZEOF_LONG || offset + length > bytes.length) {
      throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_LONG);
    }
    long l = 0;
    for(int i = offset; i < offset + length; i++) {
      l <<= 8;
      l ^= bytes[i] & 0xFF;
    }
    return l;
  }

  private static IllegalArgumentException
    explainWrongLengthOrOffset(final byte[] bytes,
                               final int offset,
                               final int length,
                               final int expectedLength) {
    String reason;
    if (length != expectedLength) {
      reason = "Wrong length: " + length + ", expected " + expectedLength;
    } else {
     reason = "offset (" + offset + ") + length (" + length + ") exceed the"
        + " capacity of the array: " + bytes.length;
    }
    return new IllegalArgumentException(reason);
  }

  /**
   * Put a long value out to the specified byte array position.
   * @param bytes the byte array
   * @param offset position in the array
   * @param val long to write out
   * @return incremented offset
   * @throws IllegalArgumentException if the byte array given doesn't have
   * enough room at the offset specified.
   */
  public static int putLong(byte[] bytes, int offset, long val) {
    if (bytes.length - offset < SIZEOF_LONG) {
      throw new IllegalArgumentException("Not enough room to put a long at"
          + " offset " + offset + " in a " + bytes.length + " byte array");
    }
    for(int i = offset + 7; i > offset; i--) {
      bytes[i] = (byte) val;
      val >>>= 8;
    }
    bytes[offset] = (byte) val;
    return offset + SIZEOF_LONG;
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
   * @param offset offset to write to
   * @param f float value
   * @return New offset in <code>bytes</code>
   */
  public static int putFloat(byte [] bytes, int offset, float f) {
    return putInt(bytes, offset, Float.floatToRawIntBits(f));
  }

  /**
   * @param f float value
   * @return the float represented as byte []
   */
  public static byte [] toBytes(final float f) {
    // Encode it as int
    return Bytes.toBytes(Float.floatToRawIntBits(f));
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
   * @param bytes byte array
   * @param offset offset to write to
   * @param d value
   * @return New offset into array <code>bytes</code>
   */
  public static int putDouble(byte [] bytes, int offset, double d) {
    return putLong(bytes, offset, Double.doubleToLongBits(d));
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
    return Bytes.toBytes(Double.doubleToRawLongBits(d));
  }

  /**
   * Convert an int value to a byte array
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
   * @return the int value
   */
  public static int toInt(byte[] bytes, int offset) {
    return toInt(bytes, offset, SIZEOF_INT);
  }

  /**
   * Converts a byte array to an int value
   * @param bytes byte array
   * @param offset offset into array
   * @param length length of int (has to be {@link #SIZEOF_INT})
   * @return the int value
   * @throws IllegalArgumentException if length is not {@link #SIZEOF_INT} or
   * if there's not enough room in the array at the offset indicated.
   */
  public static int toInt(byte[] bytes, int offset, final int length) {
    if (length != SIZEOF_INT || offset + length > bytes.length) {
      throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_INT);
    }
    int n = 0;
    for(int i = offset; i < (offset + length); i++) {
      n <<= 8;
      n ^= bytes[i] & 0xFF;
    }
    return n;
  }

  /**
   * Put an int value out to the specified byte array position.
   * @param bytes the byte array
   * @param offset position in the array
   * @param val int to write out
   * @return incremented offset
   * @throws IllegalArgumentException if the byte array given doesn't have
   * enough room at the offset specified.
   */
  public static int putInt(byte[] bytes, int offset, int val) {
    if (bytes.length - offset < SIZEOF_INT) {
      throw new IllegalArgumentException("Not enough room to put an int at"
          + " offset " + offset + " in a " + bytes.length + " byte array");
    }
    for(int i= offset + 3; i > offset; i--) {
      bytes[i] = (byte) val;
      val >>>= 8;
    }
    bytes[offset] = (byte) val;
    return offset + SIZEOF_INT;
  }

  /**
   * Convert a short value to a byte array of {@link #SIZEOF_SHORT} bytes long.
   * @param val value
   * @return the byte array
   */
  public static byte[] toBytes(short val) {
    byte[] b = new byte[SIZEOF_SHORT];
    b[1] = (byte) val;
    val >>= 8;
    b[0] = (byte) val;
    return b;
  }

  /**
   * Converts a byte array to a short value
   * @param bytes byte array
   * @return the short value
   */
  public static short toShort(byte[] bytes) {
    return toShort(bytes, 0, SIZEOF_SHORT);
  }

  /**
   * Converts a byte array to a short value
   * @param bytes byte array
   * @param offset offset into array
   * @return the short value
   */
  public static short toShort(byte[] bytes, int offset) {
    return toShort(bytes, offset, SIZEOF_SHORT);
  }

  /**
   * Converts a byte array to a short value
   * @param bytes byte array
   * @param offset offset into array
   * @param length length, has to be {@link #SIZEOF_SHORT}
   * @return the short value
   * @throws IllegalArgumentException if length is not {@link #SIZEOF_SHORT}
   * or if there's not enough room in the array at the offset indicated.
   */
  public static short toShort(byte[] bytes, int offset, final int length) {
    if (length != SIZEOF_SHORT || offset + length > bytes.length) {
      throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_SHORT);
    }
    short n = 0;
    n ^= bytes[offset] & 0xFF;
    n <<= 8;
    n ^= bytes[offset+1] & 0xFF;
    return n;
  }

  /**
   * Put a short value out to the specified byte array position.
   * @param bytes the byte array
   * @param offset position in the array
   * @param val short to write out
   * @return incremented offset
   * @throws IllegalArgumentException if the byte array given doesn't have
   * enough room at the offset specified.
   */
  public static int putShort(byte[] bytes, int offset, short val) {
    if (bytes.length - offset < SIZEOF_SHORT) {
      throw new IllegalArgumentException("Not enough room to put a short at"
          + " offset " + offset + " in a " + bytes.length + " byte array");
    }
    bytes[offset+1] = (byte) val;
    val >>= 8;
    bytes[offset] = (byte) val;
    return offset + SIZEOF_SHORT;
  }

  /**
   * @param vint Integer to make a vint of.
   * @return Vint as bytes array.
   */
  public static byte [] vintToBytes(final long vint) {
    long i = vint;
    int size = WritableUtils.getVIntSize(i);
    byte [] result = new byte[size];
    int offset = 0;
    if (i >= -112 && i <= 127) {
      result[offset] = (byte) i;
      return result;
    }

    int len = -112;
    if (i < 0) {
      i ^= -1L; // take one's complement'
      len = -120;
    }

    long tmp = i;
    while (tmp != 0) {
      tmp = tmp >> 8;
      len--;
    }

    result[offset++] = (byte) len;

    len = (len < -120) ? -(len + 120) : -(len + 112);

    for (int idx = len; idx != 0; idx--) {
      int shiftbits = (idx - 1) * 8;
      long mask = 0xFFL << shiftbits;
      result[offset++] = (byte)((i & mask) >> shiftbits);
    }
    return result;
  }

  /**
   * @param buffer buffer to convert
   * @return vint bytes as an integer.
   */
  public static long bytesToVint(final byte [] buffer) {
    int offset = 0;
    byte firstByte = buffer[offset++];
    int len = WritableUtils.decodeVIntSize(firstByte);
    if (len == 1) {
      return firstByte;
    }
    long i = 0;
    for (int idx = 0; idx < len-1; idx++) {
      byte b = buffer[offset++];
      i = i << 8;
      i = i | (b & 0xFF);
    }
    return (WritableUtils.isNegativeVInt(firstByte) ? ~i : i);
  }

  /**
   * Reads a zero-compressed encoded long from input stream and returns it.
   * @param buffer Binary array
   * @param offset Offset into array at which vint begins.
   * @throws java.io.IOException e
   * @return deserialized long from stream.
   */
  public static long readVLong(final byte [] buffer, final int offset)
  throws IOException {
    byte firstByte = buffer[offset];
    int len = WritableUtils.decodeVIntSize(firstByte);
    if (len == 1) {
      return firstByte;
    }
    long i = 0;
    for (int idx = 0; idx < len-1; idx++) {
      byte b = buffer[offset + 1 + idx];
      i = i << 8;
      i = i | (b & 0xFF);
    }
    return (WritableUtils.isNegativeVInt(firstByte) ? ~i : i);
  }

  /**
   * @param left left operand
   * @param right right operand
   * @return 0 if equal, < 0 if left is less than right, etc.
   */
  public static int compareTo(final byte [] left, final byte [] right) {
    return compareTo(left, 0, left.length, right, 0, right.length);
  }

  /**
   * Lexographically compare two arrays.
   *
   * @param b1 left operand
   * @param b2 right operand
   * @param s1 Where to start comparing in the left buffer
   * @param s2 Where to start comparing in the right buffer
   * @param l1 How much to compare from the left buffer
   * @param l2 How much to compare from the right buffer
   * @return 0 if equal, < 0 if left is less than right, etc.
   */
  public static int compareTo(byte[] b1, int s1, int l1,
      byte[] b2, int s2, int l2) {
    // Bring WritableComparator code local
    int end1 = s1 + l1;
    int end2 = s2 + l2;
    for (int i = s1, j = s2; i < end1 && j < end2; i++, j++) {
      int a = (b1[i] & 0xff);
      int b = (b2[j] & 0xff);
      if (a != b) {
        return a - b;
      }
    }
    return l1 - l2;
  }

  /**
   * @param left left operand
   * @param right right operand
   * @return True if equal
   */
  public static boolean equals(final byte [] left, final byte [] right) {
    // Could use Arrays.equals?
    //noinspection SimplifiableConditionalExpression
    if (left == null && right == null) {
      return true;
    }
    return (left == null || right == null || (left.length != right.length)
            ? false : compareTo(left, right) == 0);
  }

  /**
   * Return true if the byte array on the right is a prefix of the byte
   * array on the left.
   */
  public static boolean startsWith(byte[] bytes, byte[] prefix) {
    return bytes != null && prefix != null &&
      bytes.length >= prefix.length &&
      compareTo(bytes, 0, prefix.length, prefix, 0, prefix.length) == 0;      
  }

  /**
   * @param b bytes to hash
   * @return Runs {@link WritableComparator#hashBytes(byte[], int)} on the
   * passed in array.  This method is what {@link org.apache.hadoop.io.Text} and
   * {@link ImmutableBytesWritable} use calculating hash code.
   */
  public static int hashCode(final byte [] b) {
    return hashCode(b, b.length);
  }

  /**
   * @param b value
   * @param length length of the value
   * @return Runs {@link WritableComparator#hashBytes(byte[], int)} on the
   * passed in array.  This method is what {@link org.apache.hadoop.io.Text} and
   * {@link ImmutableBytesWritable} use calculating hash code.
   */
  public static int hashCode(final byte [] b, final int length) {
    return WritableComparator.hashBytes(b, length);
  }

  /**
   * @param b bytes to hash
   * @return A hash of <code>b</code> as an Integer that can be used as key in
   * Maps.
   */
  public static Integer mapKey(final byte [] b) {
    return hashCode(b);
  }

  /**
   * @param b bytes to hash
   * @param length length to hash
   * @return A hash of <code>b</code> as an Integer that can be used as key in
   * Maps.
   */
  public static Integer mapKey(final byte [] b, final int length) {
    return hashCode(b, length);
  }

  /**
   * @param a lower half
   * @param b upper half
   * @return New array that has a in lower half and b in upper half.
   */
  public static byte [] add(final byte [] a, final byte [] b) {
    return add(a, b, HConstants.EMPTY_BYTE_ARRAY);
  }

  /**
   * @param a first third
   * @param b second third
   * @param c third third
   * @return New array made from a, b and c
   */
  public static byte [] add(final byte [] a, final byte [] b, final byte [] c) {
    byte [] result = new byte[a.length + b.length + c.length];
    System.arraycopy(a, 0, result, 0, a.length);
    System.arraycopy(b, 0, result, a.length, b.length);
    System.arraycopy(c, 0, result, a.length + b.length, c.length);
    return result;
  }

  /**
   * @param a array
   * @param length amount of bytes to grab
   * @return First <code>length</code> bytes from <code>a</code>
   */
  public static byte [] head(final byte [] a, final int length) {
    if (a.length < length) {
      return null;
    }
    byte [] result = new byte[length];
    System.arraycopy(a, 0, result, 0, length);
    return result;
  }

  /**
   * @param a array
   * @param length amount of bytes to snarf
   * @return Last <code>length</code> bytes from <code>a</code>
   */
  public static byte [] tail(final byte [] a, final int length) {
    if (a.length < length) {
      return null;
    }
    byte [] result = new byte[length];
    System.arraycopy(a, a.length - length, result, 0, length);
    return result;
  }

  /**
   * @param a array
   * @param length new array size
   * @return Value in <code>a</code> plus <code>length</code> prepended 0 bytes
   */
  public static byte [] padHead(final byte [] a, final int length) {
    byte [] padding = new byte[length];
    for (int i = 0; i < length; i++) {
      padding[i] = 0;
    }
    return add(padding,a);
  }

  /**
   * @param a array
   * @param length new array size
   * @return Value in <code>a</code> plus <code>length</code> appended 0 bytes
   */
  public static byte [] padTail(final byte [] a, final int length) {
    byte [] padding = new byte[length];
    for (int i = 0; i < length; i++) {
      padding[i] = 0;
    }
    return add(a,padding);
  }

  /**
   * Split passed range.  Expensive operation relatively.  Uses BigInteger math.
   * Useful splitting ranges for MapReduce jobs.
   * @param a Beginning of range
   * @param b End of range
   * @param num Number of times to split range.  Pass 1 if you want to split
   * the range in two; i.e. one split.
   * @return Array of dividing values
   */
  public static byte [][] split(final byte [] a, final byte [] b, final int num) {
    byte[][] ret = new byte[num+2][];
    int i = 0;
    Iterable<byte[]> iter = iterateOnSplits(a, b, num);
    if (iter == null) return null;
    for (byte[] elem : iter) {
      ret[i++] = elem;
    }
    return ret;
  }
  
  /**
   * Iterate over keys within the passed inclusive range.
   */
  public static Iterable<byte[]> iterateOnSplits(
      final byte[] a, final byte[]b, final int num)
  {  
    byte [] aPadded;
    byte [] bPadded;
    if (a.length < b.length) {
      aPadded = padTail(a, b.length - a.length);
      bPadded = b;
    } else if (b.length < a.length) {
      aPadded = a;
      bPadded = padTail(b, a.length - b.length);
    } else {
      aPadded = a;
      bPadded = b;
    }
    if (compareTo(aPadded,bPadded) >= 0) {
      throw new IllegalArgumentException("b <= a");
    }
    if (num <= 0) {
      throw new IllegalArgumentException("num cannot be < 0");
    }
    byte [] prependHeader = {1, 0};
    final BigInteger startBI = new BigInteger(add(prependHeader, aPadded));
    final BigInteger stopBI = new BigInteger(add(prependHeader, bPadded));
    final BigInteger diffBI = stopBI.subtract(startBI);
    final BigInteger splitsBI = BigInteger.valueOf(num + 1);
    if(diffBI.compareTo(splitsBI) < 0) {
      return null;
    }
    final BigInteger intervalBI;
    try {
      intervalBI = diffBI.divide(splitsBI);
    } catch(Exception e) {
      LOG.error("Exception caught during division", e);
      return null;
    }

    final Iterator<byte[]> iterator = new Iterator<byte[]>() {
      private int i = -1;
      
      @Override
      public boolean hasNext() {
        return i < num+1;
      }

      @Override
      public byte[] next() {
        i++;
        if (i == 0) return a;
        if (i == num + 1) return b;
        
        BigInteger curBI = startBI.add(intervalBI.multiply(BigInteger.valueOf(i)));
        byte [] padded = curBI.toByteArray();
        if (padded[1] == 0)
          padded = tail(padded, padded.length - 2);
        else
          padded = tail(padded, padded.length - 1);
        return padded;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
      
    };
    
    return new Iterable<byte[]>() {
      @Override
      public Iterator<byte[]> iterator() {
        return iterator;
      }
    };
  }

  /**
   * @param t operands
   * @return Array of byte arrays made from passed array of Text
   */
  public static byte [][] toByteArrays(final String [] t) {
    byte [][] result = new byte[t.length][];
    for (int i = 0; i < t.length; i++) {
      result[i] = Bytes.toBytes(t[i]);
    }
    return result;
  }

  /**
   * @param column operand
   * @return A byte array of a byte array where first and only entry is
   * <code>column</code>
   */
  public static byte [][] toByteArrays(final String column) {
    return toByteArrays(toBytes(column));
  }

  /**
   * @param column operand
   * @return A byte array of a byte array where first and only entry is
   * <code>column</code>
   */
  public static byte [][] toByteArrays(final byte [] column) {
    byte [][] result = new byte[1][];
    result[0] = column;
    return result;
  }

  /**
   * Binary search for keys in indexes.
   * @param arr array of byte arrays to search for
   * @param key the key you want to find
   * @param offset the offset in the key you want to find
   * @param length the length of the key
   * @param comparator a comparator to compare.
   * @return index of key
   */
  public static int binarySearch(byte [][]arr, byte []key, int offset,
      int length, RawComparator<byte []> comparator) {
    int low = 0;
    int high = arr.length - 1;

    while (low <= high) {
      int mid = (low+high) >>> 1;
      // we have to compare in this order, because the comparator order
      // has special logic when the 'left side' is a special key.
      int cmp = comparator.compare(key, offset, length,
          arr[mid], 0, arr[mid].length);
      // key lives above the midpoint
      if (cmp > 0)
        low = mid + 1;
      // key lives below the midpoint
      else if (cmp < 0)
        high = mid - 1;
      // BAM. how often does this really happen?
      else
        return mid;
    }
    return - (low+1);
  }

  /**
   * Bytewise binary increment/deincrement of long contained in byte array
   * on given amount.
   *
   * @param value - array of bytes containing long (length <= SIZEOF_LONG)
   * @param amount value will be incremented on (deincremented if negative)
   * @return array of bytes containing incremented long (length == SIZEOF_LONG)
   * @throws IOException - if value.length > SIZEOF_LONG
   */
  public static byte [] incrementBytes(byte[] value, long amount)
  throws IOException {
    byte[] val = value;
    if (val.length < SIZEOF_LONG) {
      // Hopefully this doesn't happen too often.
      byte [] newvalue;
      if (val[0] < 0) {
        newvalue = new byte[]{-1, -1, -1, -1, -1, -1, -1, -1};
      } else {
        newvalue = new byte[SIZEOF_LONG];
      }
      System.arraycopy(val, 0, newvalue, newvalue.length - val.length,
        val.length);
      val = newvalue;
    } else if (val.length > SIZEOF_LONG) {
      throw new IllegalArgumentException("Increment Bytes - value too big: " +
        val.length);
    }
    if(amount == 0) return val;
    if(val[0] < 0){
      return binaryIncrementNeg(val, amount);
    }
    return binaryIncrementPos(val, amount);
  }

  /* increment/deincrement for positive value */
  private static byte [] binaryIncrementPos(byte [] value, long amount) {
    long amo = amount;
    int sign = 1;
    if (amount < 0) {
      amo = -amount;
      sign = -1;
    }
    for(int i=0;i<value.length;i++) {
      int cur = ((int)amo % 256) * sign;
      amo = (amo >> 8);
      int val = value[value.length-i-1] & 0x0ff;
      int total = val + cur;
      if(total > 255) {
        amo += sign;
        total %= 256;
      } else if (total < 0) {
        amo -= sign;
      }
      value[value.length-i-1] = (byte)total;
      if (amo == 0) return value;
    }
    return value;
  }

  /* increment/deincrement for negative value */
  private static byte [] binaryIncrementNeg(byte [] value, long amount) {
    long amo = amount;
    int sign = 1;
    if (amount < 0) {
      amo = -amount;
      sign = -1;
    }
    for(int i=0;i<value.length;i++) {
      int cur = ((int)amo % 256) * sign;
      amo = (amo >> 8);
      int val = ((~value[value.length-i-1]) & 0x0ff) + 1;
      int total = cur - val;
      if(total >= 0) {
        amo += sign;
      } else if (total < -256) {
        amo -= sign;
        total %= 256;
      }
      value[value.length-i-1] = (byte)total;
      if (amo == 0) return value;
    }
    return value;
  }

}
