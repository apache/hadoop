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

import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.ToIntFunction;

import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.*;

/**
 * Unittests for CrcUtil.
 */
public class TestCrcUtil {
  @Rule
  public Timeout globalTimeout = new Timeout(10000, TimeUnit.MILLISECONDS);

  private static final Random RANDOM = new Random();

  @Test
  public void testComposeCrc32() {
    byte[] data = new byte[64 * 1024];
    RANDOM.nextBytes(data);
    doTestComposeCrc(data, DataChecksum.Type.CRC32, 512, false);
    doTestComposeCrc(data, DataChecksum.Type.CRC32, 511, false);
    doTestComposeCrc(data, DataChecksum.Type.CRC32, 32 * 1024, false);
    doTestComposeCrc(data, DataChecksum.Type.CRC32, 32 * 1024 - 1, false);
  }

  @Test
  public void testComposeCrc32c() {
    byte[] data = new byte[64 * 1024];
    RANDOM.nextBytes(data);
    doTestComposeCrc(data, DataChecksum.Type.CRC32C, 512, false);
    doTestComposeCrc(data, DataChecksum.Type.CRC32C, 511, false);
    doTestComposeCrc(data, DataChecksum.Type.CRC32C, 32 * 1024, false);
    doTestComposeCrc(data, DataChecksum.Type.CRC32C, 32 * 1024 - 1, false);
  }

  @Test
  public void testComposeCrc32WithMonomial() {
    byte[] data = new byte[64 * 1024];
    RANDOM.nextBytes(data);
    doTestComposeCrc(data, DataChecksum.Type.CRC32, 512, true);
    doTestComposeCrc(data, DataChecksum.Type.CRC32, 511, true);
    doTestComposeCrc(data, DataChecksum.Type.CRC32, 32 * 1024, true);
    doTestComposeCrc(data, DataChecksum.Type.CRC32, 32 * 1024 - 1, true);
  }

  @Test
  public void testComposeCrc32cWithMonomial() {
    byte[] data = new byte[64 * 1024];
    RANDOM.nextBytes(data);
    doTestComposeCrc(data, DataChecksum.Type.CRC32C, 512, true);
    doTestComposeCrc(data, DataChecksum.Type.CRC32C, 511, true);
    doTestComposeCrc(data, DataChecksum.Type.CRC32C, 32 * 1024, true);
    doTestComposeCrc(data, DataChecksum.Type.CRC32C, 32 * 1024 - 1, true);
  }

  @Test
  public void testComposeCrc32ZeroLength() {
    doTestComposeCrcZerolength(DataChecksum.Type.CRC32);
  }

  @Test
  public void testComposeCrc32CZeroLength() {
    doTestComposeCrcZerolength(DataChecksum.Type.CRC32C);
  }

  /**
   * Helper method to compare a DataChecksum-computed end-to-end CRC against
   * a piecewise-computed CRC that uses CrcUtil.compose on "chunk CRCs"
   * corresponding to ever {@code chunkSize} bytes.
   */
  private static void doTestComposeCrc(
      byte[] data, DataChecksum.Type type, int chunkSize, boolean useMonomial) {
    final ToIntFunction<Long> mod = DataChecksum.getModFunction(type);

    // Get full end-to-end CRC in a single shot first.
    DataChecksum checksum = DataChecksum.newDataChecksum(
        type, Integer.MAX_VALUE);
    Objects.requireNonNull(checksum, "checksum");
    checksum.update(data, 0, data.length);
    int fullCrc = (int) checksum.getValue();

    // Now compute CRCs of each chunk individually first, and compose them in a
    // second pass to compare to the end-to-end CRC.
    int compositeCrc = 0;
    int crcMonomial =
        useMonomial ? CrcUtil.getMonomial(chunkSize, mod) : 0;
    for (int offset = 0;
        offset + chunkSize <= data.length;
        offset += chunkSize) {
      checksum.reset();
      checksum.update(data, offset, chunkSize);
      int partialCrc = (int) checksum.getValue();
      if (useMonomial) {
        compositeCrc = CrcUtil.composeWithMonomial(
            compositeCrc, partialCrc, crcMonomial, mod);
      } else {
        compositeCrc = CrcUtil.compose(
            compositeCrc, partialCrc, chunkSize, mod);
      }
    }

    // There may be a final partial chunk smaller than chunkSize.
    int partialChunkSize = data.length % chunkSize;
    if (partialChunkSize > 0) {
      checksum.reset();
      checksum.update(data, data.length - partialChunkSize, partialChunkSize);
      int partialCrc = (int) checksum.getValue();
      compositeCrc = CrcUtil.compose(
          compositeCrc, partialCrc, partialChunkSize, mod);
    }
    assertEquals(String.format(
            "Using CRC type '%s' and chunkSize '%d', expected '0x%08x', got '0x%08x'",
            type, chunkSize, fullCrc, compositeCrc),
        fullCrc,
        compositeCrc);
  }

  /**
   * Helper method for testing the behavior of composing a CRC with a
   * zero-length second CRC.
   */
  private static void doTestComposeCrcZerolength(DataChecksum.Type type) {
    // Without loss of generality, we can pick any integer as our fake crcA
    // even if we don't happen to know the preimage.
    int crcA = 0xCAFEBEEF;
    final ToIntFunction<Long> mod = DataChecksum.getModFunction(type);
    DataChecksum checksum = DataChecksum.newDataChecksum(
        type, Integer.MAX_VALUE);
    Objects.requireNonNull(checksum, "checksum");
    int crcB = (int) checksum.getValue();
    assertEquals(crcA, CrcUtil.compose(crcA, crcB, 0, mod));

    int monomial = CrcUtil.getMonomial(0, mod);
    assertEquals(
        crcA, CrcUtil.composeWithMonomial(crcA, crcB, monomial, mod));
  }

  @Test
  public void testIntSerialization() {
    byte[] bytes = CrcUtil.intToBytes(0xCAFEBEEF);
    assertEquals(0xCAFEBEEF, CrcUtil.readInt(bytes, 0));

    bytes = new byte[8];
    CrcUtil.writeInt(bytes, 0, 0xCAFEBEEF);
    assertEquals(0xCAFEBEEF, CrcUtil.readInt(bytes, 0));
    CrcUtil.writeInt(bytes, 4, 0xABCDABCD);
    assertEquals(0xABCDABCD, CrcUtil.readInt(bytes, 4));

    // Assert big-endian format for general Java consistency.
    assertEquals(0xBEEFABCD, CrcUtil.readInt(bytes, 2));
  }

  @Test
  public void testToSingleCrcStringBadLength()
      throws Exception {
    LambdaTestUtils.intercept(
        IllegalArgumentException.class,
        "length",
        () -> CrcUtil.toSingleCrcString(new byte[8]));
  }

  @Test
  public void testToSingleCrcString() {
    byte[] buf = CrcUtil.intToBytes(0xcafebeef);
    assertEquals(
        "0xcafebeef", CrcUtil.toSingleCrcString(buf));
  }

  @Test
  public void testToMultiCrcStringBadLength()
      throws Exception {
    LambdaTestUtils.intercept(
        IllegalArgumentException.class,
        "length",
        () -> CrcUtil.toMultiCrcString(new byte[6]));
  }

  @Test
  public void testToMultiCrcStringMultipleElements() {
    byte[] buf = new byte[12];
    CrcUtil.writeInt(buf, 0, 0xcafebeef);
    CrcUtil.writeInt(buf, 4, 0xababcccc);
    CrcUtil.writeInt(buf, 8, 0xddddefef);
    assertEquals(
        "[0xcafebeef, 0xababcccc, 0xddddefef]",
        CrcUtil.toMultiCrcString(buf));
  }

  @Test
  public void testToMultiCrcStringSingleElement() {
    byte[] buf = new byte[4];
    CrcUtil.writeInt(buf, 0, 0xcafebeef);
    assertEquals(
        "[0xcafebeef]",
        CrcUtil.toMultiCrcString(buf));
  }

  @Test
  public void testToMultiCrcStringNoElements() {
    assertEquals(
        "[]",
        CrcUtil.toMultiCrcString(new byte[0]));
  }

  @Test
  public void testMultiplyMod() {
    runTestMultiplyMod(10_000_000, DataChecksum.Type.CRC32);
    runTestMultiplyMod(10_000_000, DataChecksum.Type.CRC32C);
  }

  private static long[] runTestMultiplyMod(int n, DataChecksum.Type type) {
    System.out.printf("Run %s with %d computations%n", type, n);
    final int polynomial = getCrcPolynomialForType(type);
    final ToIntFunction<Long> mod = DataChecksum.getModFunction(type);

    final int[] p = new int[n];
    final int[] q = new int[n];
    for (int i = 0; i < n; i++) {
      p[i] = RANDOM.nextInt();
      q[i] = RANDOM.nextInt();
    }

    final int[] expected = new int[n];
    final long[] times = new long[2];
    final long t0 = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {
      expected[i] = galoisFieldMultiply(p[i], q[i], polynomial);
    }
    times[0] = System.currentTimeMillis() - t0;
    final double ops0 = n * 1000.0 / times[0];
    System.out.printf("galoisFieldMultiply: %.3fs (%.2f ops)%n", times[0] / 1000.0, ops0);

    final int[] computed = new int[n];
    final long t1 = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {
      computed[i] = CrcUtil.multiplyMod(p[i], q[i], mod);
    }
    times[1] = System.currentTimeMillis() - t1;
    final double ops1 = n * 1000.0 / times[1];
    System.out.printf("multiplyCrc32      : %.3fs (%.2f ops)%n", times[1] / 1000.0, ops1);
    System.out.printf("multiplyCrc32 is %.2f%% faster%n", (ops1 - ops0) * 100.0 / ops0);

    for (int i = 0; i < n; i++) {
      if (expected[i] != computed[i]) {
        System.out.printf("expected %08X%n", expected[i]);
        System.out.printf("computed %08X%n", computed[i]);
        throw new IllegalStateException();
      }
    }
    return times;
  }

  /**
   * getCrcPolynomialForType.
   *
   * @param type type.
   * @return the int representation of the polynomial associated with the
   * CRC {@code type}, suitable for use with further CRC arithmetic.
   */
  private static int getCrcPolynomialForType(DataChecksum.Type type) {
    switch (type) {
    case CRC32:
      return CrcUtil.GZIP_POLYNOMIAL;
    case CRC32C:
      return CrcUtil.CASTAGNOLI_POLYNOMIAL;
    default:
      throw new IllegalArgumentException("Unexpected type: " + type);
    }
  }

  /**
   * Galois field multiplication of {@code p} and {@code q} with the
   * generator polynomial {@code m} as the modulus.
   *
   * @param m The little-endian polynomial to use as the modulus when
   *          multiplying p and q, with implicit "1" bit beyond the bottom bit.
   */
  private static int galoisFieldMultiply(int p, int q, int m) {
    int summation = 0;

    // Top bit is the x^0 place; each right-shift increments the degree of the
    // current term.
    int curTerm = CrcUtil.MULTIPLICATIVE_IDENTITY;

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

  /** For running benchmarks. */
  public static class Benchmark {
    /**
     * Usages: java {@link Benchmark} [m] [n] [type]
     *      m: the number of iterations
     *      n: the number of multiplication
     *   type: the CRC type, either CRC32 or CRC32C.
     */
    public static void main(String[] args) throws Exception {
      final int m = args.length >= 1? Integer.parseInt(args[0]) : 10;
      final int n = args.length >= 2? Integer.parseInt(args[1]) : 100_000_000;
      final DataChecksum.Type type = args.length >= 3? DataChecksum.Type.valueOf(args[2])
          : DataChecksum.Type.CRC32;

      final int warmUpIterations = 2;
      System.out.printf("%nStart warming up with %d iterations ...%n", warmUpIterations);
      for (int i = 0; i < 2; i++) {
        runTestMultiplyMod(n, type);
      }

      System.out.printf("%nStart benchmark with %d iterations ...%n", m);
      final long[] times = new long[2];
      for (int i = 0; i < m; i++) {
        System.out.printf("%d) ", i);
        final long[] t = runTestMultiplyMod(n, type);
        times[0] += t[0];
        times[1] += t[1];
      }

      System.out.printf("%nResult) %d x %d computations:%n", m, n);
      final double ops0 = n * 1000.0 / times[0];
      System.out.printf("galoisFieldMultiply: %.3fs (%.2f ops)%n", times[0] / 1000.0, ops0);
      final double ops1 = n * 1000.0 / times[1];
      System.out.printf("multiplyCrc32      : %.3fs (%.2f ops)%n", times[1] / 1000.0, ops1);
      System.out.printf("multiplyCrc32 is %.2f%% faster%n", (ops1 - ops0) * 100.0 / ops0);
    }
  }
}
