/*
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

import java.util.Arrays;
import java.util.function.ToIntFunction;

/**
 * This class provides utilities for working with CRCs.
 */
@InterfaceAudience.LimitedPrivate({"Common", "HDFS", "MapReduce", "Yarn"})
@InterfaceStability.Unstable
public final class CrcUtil {
  public static final int MULTIPLICATIVE_IDENTITY = 0x80000000;
  public static final int GZIP_POLYNOMIAL = 0xEDB88320;
  public static final int CASTAGNOLI_POLYNOMIAL = 0x82F63B78;
  private static final long UNIT = 0x8000_0000_0000_0000L;

  /**
   * @return a * b (mod p),
   *         where mod p is computed by the given mod function.
   */
  static int multiplyMod(int a, int b, ToIntFunction<Long> mod) {
    final long left  = ((long)a) << 32;
    final long right = ((long)b) << 32;

    final long product
        = ((((((left & (UNIT /*  */)) == 0L? 0L : right)
        ^     ((left & (UNIT >>>  1)) == 0L? 0L : right >>>  1))
        ^    (((left & (UNIT >>>  2)) == 0L? 0L : right >>>  2)
        ^     ((left & (UNIT >>>  3)) == 0L? 0L : right >>>  3)))
        ^   ((((left & (UNIT >>>  4)) == 0L? 0L : right >>>  4)
        ^     ((left & (UNIT >>>  5)) == 0L? 0L : right >>>  5))
        ^    (((left & (UNIT >>>  6)) == 0L? 0L : right >>>  6)
        ^     ((left & (UNIT >>>  7)) == 0L? 0L : right >>>  7))))

        ^  (((((left & (UNIT >>>  8)) == 0L? 0L : right >>>  8)
        ^     ((left & (UNIT >>>  9)) == 0L? 0L : right >>>  9))
        ^    (((left & (UNIT >>> 10)) == 0L? 0L : right >>> 10)
        ^     ((left & (UNIT >>> 11)) == 0L? 0L : right >>> 11)))
        ^   ((((left & (UNIT >>> 12)) == 0L? 0L : right >>> 12)
        ^     ((left & (UNIT >>> 13)) == 0L? 0L : right >>> 13))
        ^    (((left & (UNIT >>> 14)) == 0L? 0L : right >>> 14)
        ^     ((left & (UNIT >>> 15)) == 0L? 0L : right >>> 15)))))

        ^ ((((((left & (UNIT >>> 16)) == 0L? 0L : right >>> 16)
        ^     ((left & (UNIT >>> 17)) == 0L? 0L : right >>> 17))
        ^    (((left & (UNIT >>> 18)) == 0L? 0L : right >>> 18)
        ^     ((left & (UNIT >>> 19)) == 0L? 0L : right >>> 19)))
        ^   ((((left & (UNIT >>> 20)) == 0L? 0L : right >>> 20)
        ^     ((left & (UNIT >>> 21)) == 0L? 0L : right >>> 21))
        ^    (((left & (UNIT >>> 22)) == 0L? 0L : right >>> 22)
        ^     ((left & (UNIT >>> 23)) == 0L? 0L : right >>> 23))))

        ^  (((((left & (UNIT >>> 24)) == 0L? 0L : right >>> 24)
        ^     ((left & (UNIT >>> 25)) == 0L? 0L : right >>> 25))
        ^    (((left & (UNIT >>> 26)) == 0L? 0L : right >>> 26)
        ^     ((left & (UNIT >>> 27)) == 0L? 0L : right >>> 27)))
        ^   ((((left & (UNIT >>> 28)) == 0L? 0L : right >>> 28)
        ^     ((left & (UNIT >>> 29)) == 0L? 0L : right >>> 29))
        ^    (((left & (UNIT >>> 30)) == 0L? 0L : right >>> 30)
        ^     ((left & (UNIT >>> 31)) == 0L? 0L : right >>> 31)))));

    return mod.applyAsInt(product);
  }

  /**
   * Hide default constructor for a static utils class.
   */
  private CrcUtil() {
  }

  /**
   * Compute x^({@code lengthBytes} * 8) mod {@code mod}, where {@code mod} is
   * in "reversed" (little-endian) format such that {@code mod & 1} represents
   * x^31 and has an implicit term x^32.
   *
   * @param lengthBytes lengthBytes.
   * @param mod mod.
   * @return monomial.
   */
  public static int getMonomial(long lengthBytes, ToIntFunction<Long> mod) {
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
            multiplyMod(product, multiplier, mod);
      }
      multiplier = multiplyMod(multiplier, multiplier, mod);
      degree >>= 1;
    }
    return product;
  }

  /**
   * composeWithMonomial.
   *
   * @param crcA crcA.
   * @param crcB crcB.
   * @param monomial Precomputed x^(lengthBInBytes * 8) mod {@code mod}
   * @param mod mod.
   * @return compose with monomial.
   */
  public static int composeWithMonomial(
      int crcA, int crcB, int monomial, ToIntFunction<Long> mod) {
    return multiplyMod(crcA, monomial, mod) ^ crcB;
  }

  /**
   * compose.
   *
   * @param crcA crcA.
   * @param crcB crcB.
   * @param lengthB length of content corresponding to {@code crcB}, in bytes.
   * @param mod mod.
   * @return compose result.
   */
  public static int compose(int crcA, int crcB, long lengthB, ToIntFunction<Long> mod) {
    int monomial = getMonomial(lengthB, mod);
    return composeWithMonomial(crcA, crcB, monomial, mod);
  }

  /**
   * @return 4-byte array holding the big-endian representation of
   *     {@code value}.
   *
   * @param value value.
   */
  public static byte[] intToBytes(int value) {
    byte[] buf = new byte[4];
    writeInt(buf, 0, value);
    return buf;
  }

  /**
   * Writes big-endian representation of {@code value} into {@code buf}
   * starting at {@code offset}. buf.length must be greater than or
   * equal to offset + 4.
   *
   * @param buf buf size.
   * @param offset offset.
   * @param value value.
   */
  public static void writeInt(byte[] buf, int offset, int value) {
    if (offset + 4  > buf.length) {
      throw new ArrayIndexOutOfBoundsException(String.format(
          "writeInt out of bounds: buf.length=%d, offset=%d",
          buf.length, offset));
    }
    buf[offset    ] = (byte)((value >>> 24) & 0xff);
    buf[offset + 1] = (byte)((value >>> 16) & 0xff);
    buf[offset + 2] = (byte)((value >>> 8) & 0xff);
    buf[offset + 3] = (byte)(value & 0xff);
  }

  /**
   * Reads 4-byte big-endian int value from {@code buf} starting at
   * {@code offset}. buf.length must be greater than or equal to offset + 4.
   *
   * @param offset offset.
   * @param buf buf.
   * @return int.
   */
  public static int readInt(byte[] buf, int offset) {
    if (offset + 4  > buf.length) {
      throw new ArrayIndexOutOfBoundsException(String.format(
          "readInt out of bounds: buf.length=%d, offset=%d",
          buf.length, offset));
    }
    return      ((buf[offset    ] & 0xff) << 24) |
                ((buf[offset + 1] & 0xff) << 16) |
                ((buf[offset + 2] & 0xff) << 8)  |
                ((buf[offset + 3] & 0xff));
  }

  /**
   * For use with debug statements; verifies bytes.length on creation,
   * expecting it to represent exactly one CRC, and returns a hex
   * formatted value.
   *
   * @param bytes bytes.
   * @return a list of hex formatted values.
   */
  public static String toSingleCrcString(final byte[] bytes) {
    if (bytes.length != 4) {
      throw new IllegalArgumentException((String.format(
          "Unexpected byte[] length '%d' for single CRC. Contents: %s",
          bytes.length, Arrays.toString(bytes))));
    }
    return String.format("0x%08x", readInt(bytes, 0));
  }

  /**
   * For use with debug statements; verifies bytes.length on creation,
   * expecting it to be divisible by CRC byte size, and returns a list of
   * hex formatted values.
   *
   * @param bytes bytes.
   * @return a list of hex formatted values.
   */
  public static String toMultiCrcString(final byte[] bytes) {
    if (bytes.length % 4 != 0) {
      throw new IllegalArgumentException((String.format(
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


}
