/**
 * Copyright 2009 The Apache Software Foundation
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Comparator;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 * Utility class that handles byte arrays, conversions to/from other types,
 * comparisons, hash code generation, manufacturing keys for HashMaps or
 * HashSets, etc.
 */
public class Bytes {
  /**
   * Size of long in bytes
   */
  public static final int SIZEOF_LONG = Long.SIZE/Byte.SIZE;

  /**
   * Size of int in bytes
   */
  public static final int SIZEOF_INT = Integer.SIZE/Byte.SIZE;
  
  /**
   * Size of int in bytes
   */
  public static final int SIZEOF_SHORT = Short.SIZE/Byte.SIZE;

  /**
   * Size of int in bytes
   */
  public static final int SIZEOF_FLOAT = Float.SIZE/Byte.SIZE;

  /**
   * Size of byte in bytes
   */
  public static final int SIZEOF_BYTE = 1;
  
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
    public ByteArrayComparator() {
      super();
    }
    @Override
    public int compare(byte [] left, byte [] right) {
      return compareTo(left, right);
    }
    @Override
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
   * Pass this to TreeMaps where byte [] are keys.
   */
  public static RawComparator<byte []> BYTES_RAWCOMPARATOR =
    new ByteArrayComparator();
  
  /**
   * @param in Input to read from.
   * @return byte array read off <code>in</code>
   * @throws IOException 
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
   * @param out
   * @param b
   * @throws IOException
   */
  public static void writeByteArray(final DataOutput out, final byte [] b)
  throws IOException {
    writeByteArray(out, b, 0, b.length);
  }

  /**
   * @param out
   * @param b
   * @throws IOException
   */
  public static void writeByteArray(final DataOutput out, final byte [] b,
      final int offset, final int length)
  throws IOException {
    WritableUtils.writeVInt(out, length);
    out.write(b, offset, length);
  }

  public static int writeByteArray(final byte [] tgt, final int tgtOffset,
      final byte [] src, final int srcOffset, final int srcLength) {
    byte [] vint = vintToBytes(srcLength);
    System.arraycopy(vint, 0, tgt, tgtOffset, vint.length);
    int offset = tgtOffset + vint.length;
    System.arraycopy(src, srcOffset, tgt, offset, srcLength);
    return offset + srcLength;
  }

  /**
   * Write a long value out to the specified byte array position.
   * @param tgtBytes the byte array
   * @param tgtOffset position in the array
   * @param srcBytes byte to write out
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
   * Returns a new byte array, copied from the specified ByteBuffer.
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
    return toString(b, 0, b.length);
  }
  
  public static String toString(final byte [] b, int off, int len) {
    String result = null;
    try {
      result = new String(b, off, len, HConstants.UTF8_ENCODING);
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    return result;
  }

  /**
   * Converts a string to a UTF-8 byte array.
   * @param s
   * @return the byte array
   */
  public static byte[] toBytes(String s) {
    if (s == null) {
      throw new IllegalArgumentException("string cannot be null");
    }
    byte [] result = null;
    try {
      result = s.getBytes(HConstants.UTF8_ENCODING);
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    return result;
  }
  
  /**
   * @param b
   * @return <code>b</code> encoded in a byte array.
   */
  public static byte [] toBytes(final boolean b) {
    byte [] bb = new byte[1];
    bb[0] = b? (byte)-1: (byte)0;
    return bb;
  }

  /**
   * @param b
   * @return True or false.
   */
  public static boolean toBoolean(final byte [] b) {
    if (b == null || b.length > 1) {
      throw new IllegalArgumentException("Array is wrong size");
    }
    return b[0] != (byte)0;
  }

  /**
   * Convert a long value to a byte array
   * @param val
   * @return the byte array
   */
  public static byte[] toBytes(long val) {
    byte [] b = new byte[8];
    for(int i=7;i>0;i--) {
      b[i] = (byte)(val);
      val >>>= 8;
    }
    b[0] = (byte)(val);
    return b;
  }

  /**
   * Converts a byte array to a long value
   * @param bytes
   * @return the long value
   */
  public static long toLong(byte[] bytes) {
    return toLong(bytes, 0);
  }

  /**
   * Converts a byte array to a long value
   * @param bytes
   * @return the long value
   */
  public static long toLong(byte[] bytes, int offset) {
    return toLong(bytes, offset, SIZEOF_LONG);
  }

  /**
   * Converts a byte array to a long value
   * @param bytes
   * @return the long value
   */
  public static long toLong(byte[] bytes, int offset, final int length) {
    if (bytes == null || length != SIZEOF_LONG ||
        (offset + length > bytes.length)) {
      return -1L;
    }
    long l = 0;
    for(int i = offset; i < (offset + length); i++) {
      l <<= 8;
      l ^= (long)bytes[i] & 0xFF;
    }
    return l;
  }
  
  /**
   * Write a long value out to the specified byte array position.
   * @param bytes the byte array
   * @param offset position in the array
   * @param val long to write out
   * @return incremented offset
   */
  public static int putLong(byte[] bytes, int offset, long val) {
    if (bytes == null || (bytes.length - offset < SIZEOF_LONG)) {
      return offset;
    }
    for(int i=offset+7;i>offset;i--) {
      bytes[i] = (byte)(val);
      val >>>= 8;
    }
    bytes[offset] = (byte)(val);
    return offset + SIZEOF_LONG;
  }
  
  /**
   * Convert an int value to a byte array
   * @param val
   * @return the byte array
   */
  public static byte[] toBytes(int val) {
    byte [] b = new byte[4];
    for(int i=3;i>0;i--) {
      b[i] = (byte)(val);
      val >>>= 8;
    }
    b[0] = (byte)(val);
    return b;
  }
  
  /**
   * Converts a byte array to an int value
   * @param bytes
   * @return the int value
   */
  public static int toInt(byte[] bytes) {
    return toInt(bytes, 0);
  }

  /**
   * Converts a byte array to an int value
   * @param bytes
   * @return the int value
   */
  public static int toInt(byte[] bytes, int offset) {
    return toInt(bytes, offset, SIZEOF_INT);
  }

  /**
   * Converts a byte array to an int value
   * @param bytes
   * @return the int value
   */
  public static int toInt(byte[] bytes, int offset, final int length) {
    if (bytes == null || length != SIZEOF_INT ||
        (offset + length > bytes.length)) {
      return -1;
    }
    int n = 0;
    for(int i = offset; i < (offset + length); i++) {
      n <<= 8;
      n ^= bytes[i] & 0xFF;
    }
    return n;
  }
  
  /**
   * Write an int value out to the specified byte array position.
   * @param bytes the byte array
   * @param offset position in the array
   * @param val int to write out
   * @return incremented offset
   */
  public static int putInt(byte[] bytes, int offset, int val) {
    if (bytes == null || (bytes.length - offset < SIZEOF_INT)) {
      return offset;
    }
    for(int i=offset+3;i>offset;i--) {
      bytes[i] = (byte)(val);
      val >>>= 8;
    }
    bytes[offset] = (byte)(val);
    return offset + SIZEOF_INT;
  }
  
  /**
   * Convert a short value to a byte array
   * @param val
   * @return the byte array
   */
  public static byte[] toBytes(short val) {
    byte[] b = new byte[2];
    b[1] = (byte)(val);
    val >>>= 8;
    b[0] = (byte)(val);
    return b;
  }

  /**
   * Converts a byte array to a short value
   * @param bytes
   * @return the short value
   */
  public static short toShort(byte[] bytes) {
    return toShort(bytes, 0);
  }

  /**
   * Converts a byte array to a short value
   * @param bytes
   * @return the short value
   */
  public static short toShort(byte[] bytes, int offset) {
    return toShort(bytes, offset, SIZEOF_SHORT);
  }

  /**
   * Converts a byte array to a short value
   * @param bytes
   * @return the short value
   */
  public static short toShort(byte[] bytes, int offset, final int length) {
    if (bytes == null || length != SIZEOF_SHORT ||
        (offset + length > bytes.length)) {
      return -1;
    }
    short n = 0;
    n ^= bytes[offset] & 0xFF;
    n <<= 8;
    n ^= bytes[offset+1] & 0xFF;
    return n;
  }
  
  /**
   * Write a short value out to the specified byte array position.
   * @param bytes the byte array
   * @param offset position in the array
   * @param val short to write out
   * @return incremented offset
   */
  public static int putShort(byte[] bytes, int offset, short val) {
    if (bytes == null || (bytes.length - offset < SIZEOF_SHORT)) {
      return offset;
    }
    bytes[offset+1] = (byte)(val);
    val >>>= 8;
    bytes[offset] = (byte)(val);
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
      result[offset] = ((byte)i);
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

    result[offset++] = (byte)len;

    len = (len < -120) ? -(len + 120) : -(len + 112);

    for (int idx = len; idx != 0; idx--) {
      int shiftbits = (idx - 1) * 8;
      long mask = 0xFFL << shiftbits;
      result[offset++] = (byte)((i & mask) >> shiftbits);
    }
    return result;
  }

  /**
   * @param buffer
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
    return (WritableUtils.isNegativeVInt(firstByte) ? (i ^ -1L) : i);
  }

  /**
   * Reads a zero-compressed encoded long from input stream and returns it.
   * @param buffer Binary array
   * @param offset Offset into array at which vint begins.
   * @throws java.io.IOException 
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
    return (WritableUtils.isNegativeVInt(firstByte) ? (i ^ -1L) : i);
  }

  /**
   * @param left
   * @param right
   * @return 0 if equal, < 0 if left is less than right, etc.
   */
  public static int compareTo(final byte [] left, final byte [] right) {
    return compareTo(left, 0, left.length, right, 0, right.length);
  }

  /**
   * @param b1
   * @param b2
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
   * @param left
   * @param right
   * @return True if equal
   */
  public static boolean equals(final byte [] left, final byte [] right) {
    // Could use Arrays.equals?
    return left == null && right == null? true:
      (left == null || right == null || (left.length != right.length))? false:
        compareTo(left, right) == 0;
  }
  
  /**
   * @param b
   * @return Runs {@link WritableComparator#hashBytes(byte[], int)} on the
   * passed in array.  This method is what {@link org.apache.hadoop.io.Text} and
   * {@link ImmutableBytesWritable} use calculating hash code.
   */
  public static int hashCode(final byte [] b) {
    return hashCode(b, b.length);
  }

  /**
   * @param b
   * @param length
   * @return Runs {@link WritableComparator#hashBytes(byte[], int)} on the
   * passed in array.  This method is what {@link org.apache.hadoop.io.Text} and
   * {@link ImmutableBytesWritable} use calculating hash code.
   */
  public static int hashCode(final byte [] b, final int length) {
    return WritableComparator.hashBytes(b, length);
  }

  /**
   * @param b
   * @return A hash of <code>b</code> as an Integer that can be used as key in
   * Maps.
   */
  public static Integer mapKey(final byte [] b) {
    return Integer.valueOf(hashCode(b));
  }

  /**
   * @param b
   * @param length
   * @return A hash of <code>b</code> as an Integer that can be used as key in
   * Maps.
   */
  public static Integer mapKey(final byte [] b, final int length) {
    return Integer.valueOf(hashCode(b, length));
  }

  /**
   * @param a
   * @param b
   * @return New array that has a in lower half and b in upper half.
   */
  public static byte [] add(final byte [] a, final byte [] b) {
    return add(a, b, HConstants.EMPTY_BYTE_ARRAY);
  }

  /**
   * @param a
   * @param b
   * @param c
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
   * @param t
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
   * @param column
   * @return A byte array of a byte array where first and only entry is
   * <code>column</code>
   */
  public static byte [][] toByteArrays(final String column) {
    return toByteArrays(toBytes(column));
  }
  
  /**
   * @param column
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
  public static int binarySearch(byte [][]arr, byte []key, int offset, int length,
      RawComparator<byte []> comparator) {
    int low = 0;
    int high = arr.length - 1;
    
    while (low <= high) {
      int mid = (low+high) >>> 1;
      int cmp = comparator.compare(arr[mid], 0, arr[mid].length, key, offset, length);
      if (cmp < 0) 
        low = mid + 1;
      else if (cmp > 0)
        high = mid - 1;
      else 
        return mid;
    }
    return - (low+1);
  }      
}
