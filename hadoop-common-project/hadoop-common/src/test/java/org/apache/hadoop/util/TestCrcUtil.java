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

import java.io.IOException;
import java.util.Random;

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
  public Timeout globalTimeout = new Timeout(10000);

  private Random rand = new Random(1234);

  @Test
  public void testComposeCrc32() throws IOException {
    byte[] data = new byte[64 * 1024];
    rand.nextBytes(data);
    doTestComposeCrc(data, DataChecksum.Type.CRC32, 512, false);
    doTestComposeCrc(data, DataChecksum.Type.CRC32, 511, false);
    doTestComposeCrc(data, DataChecksum.Type.CRC32, 32 * 1024, false);
    doTestComposeCrc(data, DataChecksum.Type.CRC32, 32 * 1024 - 1, false);
  }

  @Test
  public void testComposeCrc32c() throws IOException {
    byte[] data = new byte[64 * 1024];
    rand.nextBytes(data);
    doTestComposeCrc(data, DataChecksum.Type.CRC32C, 512, false);
    doTestComposeCrc(data, DataChecksum.Type.CRC32C, 511, false);
    doTestComposeCrc(data, DataChecksum.Type.CRC32C, 32 * 1024, false);
    doTestComposeCrc(data, DataChecksum.Type.CRC32C, 32 * 1024 - 1, false);
  }

  @Test
  public void testComposeCrc32WithMonomial() throws IOException {
    byte[] data = new byte[64 * 1024];
    rand.nextBytes(data);
    doTestComposeCrc(data, DataChecksum.Type.CRC32, 512, true);
    doTestComposeCrc(data, DataChecksum.Type.CRC32, 511, true);
    doTestComposeCrc(data, DataChecksum.Type.CRC32, 32 * 1024, true);
    doTestComposeCrc(data, DataChecksum.Type.CRC32, 32 * 1024 - 1, true);
  }

  @Test
  public void testComposeCrc32cWithMonomial() throws IOException {
    byte[] data = new byte[64 * 1024];
    rand.nextBytes(data);
    doTestComposeCrc(data, DataChecksum.Type.CRC32C, 512, true);
    doTestComposeCrc(data, DataChecksum.Type.CRC32C, 511, true);
    doTestComposeCrc(data, DataChecksum.Type.CRC32C, 32 * 1024, true);
    doTestComposeCrc(data, DataChecksum.Type.CRC32C, 32 * 1024 - 1, true);
  }

  @Test
  public void testComposeCrc32ZeroLength() throws IOException {
    doTestComposeCrcZerolength(DataChecksum.Type.CRC32);
  }

  @Test
  public void testComposeCrc32CZeroLength() throws IOException {
    doTestComposeCrcZerolength(DataChecksum.Type.CRC32C);
  }

  /**
   * Helper method to compare a DataChecksum-computed end-to-end CRC against
   * a piecewise-computed CRC that uses CrcUtil.compose on "chunk CRCs"
   * corresponding to ever {@code chunkSize} bytes.
   */
  private static void doTestComposeCrc(
      byte[] data, DataChecksum.Type type, int chunkSize, boolean useMonomial)
      throws IOException {
    int crcPolynomial = DataChecksum.getCrcPolynomialForType(type);

    // Get full end-to-end CRC in a single shot first.
    DataChecksum checksum = DataChecksum.newDataChecksum(
        type, Integer.MAX_VALUE);
    checksum.update(data, 0, data.length);
    int fullCrc = (int) checksum.getValue();

    // Now compute CRCs of each chunk individually first, and compose them in a
    // second pass to compare to the end-to-end CRC.
    int compositeCrc = 0;
    int crcMonomial =
        useMonomial ? CrcUtil.getMonomial(chunkSize, crcPolynomial) : 0;
    for (int offset = 0;
        offset + chunkSize <= data.length;
        offset += chunkSize) {
      checksum.reset();
      checksum.update(data, offset, chunkSize);
      int partialCrc = (int) checksum.getValue();
      if (useMonomial) {
        compositeCrc = CrcUtil.composeWithMonomial(
            compositeCrc, partialCrc, crcMonomial, crcPolynomial);
      } else {
        compositeCrc = CrcUtil.compose(
            compositeCrc, partialCrc, chunkSize, crcPolynomial);
      }
    }

    // There may be a final partial chunk smaller than chunkSize.
    int partialChunkSize = data.length % chunkSize;
    if (partialChunkSize > 0) {
      checksum.reset();
      checksum.update(data, data.length - partialChunkSize, partialChunkSize);
      int partialCrc = (int) checksum.getValue();
      compositeCrc = CrcUtil.compose(
          compositeCrc, partialCrc, partialChunkSize, crcPolynomial);
    }
    assertEquals(
        String.format(
            "Using CRC type '%s' with crcPolynomial '0x%08x' and chunkSize '%d'"
            + ", expected '0x%08x', got '0x%08x'",
            type, crcPolynomial, chunkSize, fullCrc, compositeCrc),
        fullCrc,
        compositeCrc);
  }

  /**
   * Helper method for testing the behavior of composing a CRC with a
   * zero-length second CRC.
   */
  private static void doTestComposeCrcZerolength(DataChecksum.Type type)
      throws IOException {
    // Without loss of generality, we can pick any integer as our fake crcA
    // even if we don't happen to know the preimage.
    int crcA = 0xCAFEBEEF;
    int crcPolynomial = DataChecksum.getCrcPolynomialForType(type);
    DataChecksum checksum = DataChecksum.newDataChecksum(
        type, Integer.MAX_VALUE);
    int crcB = (int) checksum.getValue();
    assertEquals(crcA, CrcUtil.compose(crcA, crcB, 0, crcPolynomial));

    int monomial = CrcUtil.getMonomial(0, crcPolynomial);
    assertEquals(
        crcA, CrcUtil.composeWithMonomial(crcA, crcB, monomial, crcPolynomial));
  }

  @Test
  public void testIntSerialization() throws IOException {
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
        IOException.class,
        "length",
        () -> CrcUtil.toSingleCrcString(new byte[8]));
  }

  @Test
  public void testToSingleCrcString() throws IOException {
    byte[] buf = CrcUtil.intToBytes(0xcafebeef);
    assertEquals(
        "0xcafebeef", CrcUtil.toSingleCrcString(buf));
  }

  @Test
  public void testToMultiCrcStringBadLength()
      throws Exception {
    LambdaTestUtils.intercept(
        IOException.class,
        "length",
        () -> CrcUtil.toMultiCrcString(new byte[6]));
  }

  @Test
  public void testToMultiCrcStringMultipleElements()
      throws IOException {
    byte[] buf = new byte[12];
    CrcUtil.writeInt(buf, 0, 0xcafebeef);
    CrcUtil.writeInt(buf, 4, 0xababcccc);
    CrcUtil.writeInt(buf, 8, 0xddddefef);
    assertEquals(
        "[0xcafebeef, 0xababcccc, 0xddddefef]",
        CrcUtil.toMultiCrcString(buf));
  }

  @Test
  public void testToMultiCrcStringSingleElement()
      throws IOException {
    byte[] buf = new byte[4];
    CrcUtil.writeInt(buf, 0, 0xcafebeef);
    assertEquals(
        "[0xcafebeef]",
        CrcUtil.toMultiCrcString(buf));
  }

  @Test
  public void testToMultiCrcStringNoElements()
      throws IOException {
    assertEquals(
        "[]",
        CrcUtil.toMultiCrcString(new byte[0]));
  }
}
