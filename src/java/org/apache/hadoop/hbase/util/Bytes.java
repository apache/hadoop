package org.apache.hadoop.hbase.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Comparator;

import org.apache.hadoop.hbase.HConstants;
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
   * Size of float in bytes
   */
  public static final int SIZEOF_FLOAT = Float.SIZE/Byte.SIZE;
  
  /**
   * Size of double in bytes
   */
  public static final int SIZEOF_DOUBLE = Double.SIZE/Byte.SIZE;
  
  /**
   * Estimate of size cost to pay beyond payload in jvm for instance of byte [].
   * Estimate based on study of jhat and jprofiler numbers.
   */
  // JHat says BU is 56 bytes.
  public static final int ESTIMATED_HEAP_TAX = 16;

  /**
   * Pass this to TreeMaps where byte [] are keys.
   */
  public static Comparator<byte []> BYTES_COMPARATOR =
      new Comparator<byte []>() {
    public int compare(byte [] left, byte [] right) {
      return compareTo(left, right);
    }
  };
  
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
   * @param out
   * @param b
   * @throws IOException
   */
  public static void writeByteArray(final DataOutput out, final byte [] b)
  throws IOException {
    WritableUtils.writeVInt(out, b.length);
    out.write(b, 0, b.length);
  }
  
  /**
   * @param b Presumed UTF-8 encoded byte array.
   * @return String made from <code>b</code>
   */
  public static String toString(final byte [] b) {
    String result = null;
    try {
      result = new String(b, HConstants.UTF8_ENCODING);
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
   * Convert a long value to a byte array
   * @param val
   * @return the byte array
   */
  public static byte[] toBytes(final long val) {
    ByteBuffer bb = ByteBuffer.allocate(SIZEOF_LONG);
    bb.putLong(val);
    return bb.array();
  }

  /**
   * Converts a byte array to a long value
   * @param bytes
   * @return the long value
   */
  public static long toLong(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return -1L;
    }
    return ByteBuffer.wrap(bytes).getLong();
  }
  
  /**
   * Convert an int value to a byte array
   * @param val
   * @return the byte array
   */
  public static byte[] toBytes(final int val) {
    ByteBuffer bb = ByteBuffer.allocate(SIZEOF_INT);
    bb.putInt(val);
    return bb.array();
  }

  /**
   * Converts a byte array to a long value
   * @param bytes
   * @return the long value
   */
  public static int toInt(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return -1;
    }
    return ByteBuffer.wrap(bytes).getInt();
  }
  
  /**
   * Convert an float value to a byte array
   * @param val
   * @return the byte array
   */
  public static byte[] toBytes(final float val) {
    ByteBuffer bb = ByteBuffer.allocate(SIZEOF_FLOAT);
    bb.putFloat(val);
    return bb.array();
  }

  /**
   * Converts a byte array to a float value
   * @param bytes
   * @return the float value
   */
  public static float toFloat(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return -1;
    }
    return ByteBuffer.wrap(bytes).getFloat();
  }

  /**
   * Convert an double value to a byte array
   * @param val
   * @return the byte array
   */
  public static byte[] toBytes(final double val) {
    ByteBuffer bb = ByteBuffer.allocate(SIZEOF_DOUBLE);
    bb.putDouble(val);
    return bb.array();
  }

  /**
   * Converts a byte array to a double value
   * @param bytes
   * @return the double value
   */
  public static double toDouble(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return -1;
    }
    return ByteBuffer.wrap(bytes).getDouble();
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
   * @param left
   * @param right
   * @param leftOffset Where to start comparing in the left buffer
   * @param rightOffset Where to start comparing in the right buffer
   * @param leftLength How much to compare from the left buffer
   * @param rightLength How much to compare from the right buffer
   * @return 0 if equal, < 0 if left is less than right, etc.
   */
  public static int compareTo(final byte [] left, final int leftOffset,
      final int leftLength, final byte [] right, final int rightOffset,
      final int rightLength) {
    return WritableComparator.compareBytes(left,leftOffset, leftLength,
        right, rightOffset, rightLength);
  }
  
  /**
   * @param left
   * @param right
   * @return True if equal
   */
  public static boolean equals(final byte [] left, final byte [] right) {
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
}
