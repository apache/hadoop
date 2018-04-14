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

package org.apache.hadoop.util;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.io.IOException;
import java.util.Arrays;

/**
 * This class provides utilities for working with CRCs.
 */
@InterfaceAudience.LimitedPrivate({"Common", "HDFS", "MapReduce", "Yarn"})
@InterfaceStability.Unstable
public final class CrcUtil {
  public static final int MULTIPLICATIVE_IDENTITY = 0x80000000;
  public static final int GZIP_POLYNOMIAL = 0xEDB88320;
  public static final int CASTAGNOLI_POLYNOMIAL = 0x82F63B78;

  /**
   * Hide default constructor for a static utils class.
   */
  private CrcUtil() {
  }

  /**
   * Compute x^({@code lengthBytes} * 8) mod {@code mod}, where {@code mod} is
   * in "reversed" (little-endian) format such that {@code mod & 1} represents
   * x^31 and has an implicit term x^32.
   */
  public static int getMonomial(long lengthBytes, int mod) {
    if (lengthBytes == 0) {
      return MULTIPLICATIVE_IDENTITY;
    } else if (lengthBytes < 0) {
      throw new IllegalArgumentException(
          "lengthBytes must be positive, got " + lengthBytes);
    }

    // Decompose into
    // x^degree == x ^ SUM(bit[i] * 2^i) == PRODUCT(x ^ (bit[i] * 2^i))
    // Generate each x^(2^i) by squaring.
    // Since 'degree' is in 'bits', but we only need to support byte
    // granularity we can begin with x^8.
    int multiplier = MULTIPLICATIVE_IDENTITY >>> 8;
    int product = MULTIPLICATIVE_IDENTITY;
    long degree = lengthBytes;
    while (degree > 0) {
      if ((degree & 1) != 0) {
        product = (product == MULTIPLICATIVE_IDENTITY) ? multiplier :
            galoisFieldMultiply(product, multiplier, mod);
      }
      multiplier = galoisFieldMultiply(multiplier, multiplier, mod);
      degree >>= 1;
    }
    return product;
  }

  /**
   * @param monomial Precomputed x^(lengthBInBytes * 8) mod {@code mod}
   */
  public static int composeWithMonomial(
      int crcA, int crcB, int monomial, int mod) {
    return galoisFieldMultiply(crcA, monomial, mod) ^ crcB;
  }

  /**
   * @param lengthB length of content corresponding to {@code crcB}, in bytes.
   */
  public static int compose(int crcA, int crcB, long lengthB, int mod) {
    int monomial = getMonomial(lengthB, mod);
    return composeWithMonomial(crcA, crcB, monomial, mod);
  }

  /**
   * @return 4-byte array holding the big-endian representation of
   *     {@code value}.
   */
  public static byte[] intToBytes(int value) {
    byte[] buf = new byte[4];
    try {
      writeInt(buf, 0, value);
    } catch (IOException ioe) {
      // Since this should only be able to occur from code bugs within this
      // class rather than user input, we throw as a RuntimeException
      // rather than requiring this method to declare throwing IOException
      // for something the caller can't control.
      throw new RuntimeException(ioe);
    }
    return buf;
  }

  /**
   * Writes big-endian representation of {@code value} into {@code buf}
   * starting at {@code offset}. buf.length must be greater than or
   * equal to offset + 4.
   */
  public static void writeInt(byte[] buf, int offset, int value)
      throws IOException {
    if (offset + 4  > buf.length) {
      throw new IOException(String.format(
          "writeInt out of bounds: buf.length=%d, offset=%d",
          buf.length, offset));
    }
    buf[offset + 0] = (byte)((value >>> 24) & 0xff);
    buf[offset + 1] = (byte)((value >>> 16) & 0xff);
    buf[offset + 2] = (byte)((value >>> 8) & 0xff);
    buf[offset + 3] = (byte)(value & 0xff);
  }

  /**
   * Reads 4-byte big-endian int value from {@code buf} starting at
   * {@code offset}. buf.length must be greater than or equal to offset + 4.
   */
  public static int readInt(byte[] buf, int offset)
      throws IOException {
    if (offset + 4  > buf.length) {
      throw new IOException(String.format(
          "readInt out of bounds: buf.length=%d, offset=%d",
          buf.length, offset));
    }
    int value = ((buf[offset + 0] & 0xff) << 24) |
                ((buf[offset + 1] & 0xff) << 16) |
                ((buf[offset + 2] & 0xff) << 8)  |
                ((buf[offset + 3] & 0xff));
    return value;
  }

  /**
   * For use with debug statements; verifies bytes.length on creation,
   * expecting it to represent exactly one CRC, and returns a hex
   * formatted value.
   */
  public static String toSingleCrcString(final byte[] bytes)
      throws IOException {
    if (bytes.length != 4) {
      throw new IOException((String.format(
          "Unexpected byte[] length '%d' for single CRC. Contents: %s",
          bytes.length, Arrays.toString(bytes))));
    }
    return String.format("0x%08x", readInt(bytes, 0));
  }

  /**
   * For use with debug statements; verifies bytes.length on creation,
   * expecting it to be divisible by CRC byte size, and returns a list of
   * hex formatted values.
   */
  public static String toMultiCrcString(final byte[] bytes)
      throws IOException {
    if (bytes.length % 4 != 0) {
      throw new IOException((String.format(
          "Unexpected byte[] length '%d' not divisible by 4. Contents: %s",
          bytes.length, Arrays.toString(bytes))));
    }
    StringBuilder sb = new StringBuilder();
    sb.append('[');
    for (int i = 0; i < bytes.length; i += 4) {
      sb.append(String.format("0x%08x", readInt(bytes, i)));
      if (i != bytes.length - 4) {
        sb.append(", ");
      }
    }
    sb.append(']');
    return sb.toString();
  }

  /**
   * Galois field multiplication of {@code p} and {@code q} with the
   * generator polynomial {@code m} as the modulus.
   *
   * @param m The little-endian polynomial to use as the modulus when
   *     multiplying p and q, with implicit "1" bit beyond the bottom bit.
   */
  private static int galoisFieldMultiply(int p, int q, int m) {
    int summation = 0;

    // Top bit is the x^0 place; each right-shift increments the degree of the
    // current term.
    int curTerm = MULTIPLICATIVE_IDENTITY;

    // Iteratively multiply p by x mod m as we go to represent the q[i] term
    // (of degree x^i) times p.
    int px = p;

    while (curTerm != 0) {
      if ((q & curTerm) != 0) {
        summation ^= px;
      }

      // Bottom bit represents highest degree since we're little-endian; before
      // we multiply by "x" for the next term, check bottom bit to know whether
      // the resulting px will thus have a term matching the implicit "1" term
      // of "m" and thus will need to subtract "m" after mutiplying by "x".
      boolean hasMaxDegree = ((px & 1) != 0);
      px >>>= 1;
      if (hasMaxDegree) {
        px ^= m;
      }
      curTerm >>>= 1;
    }
    return summation;
  }
}
