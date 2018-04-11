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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.*;

/**
 * Unittests for CrcComposer.
 */
public class TestCrcComposer {
  @Rule
  public Timeout globalTimeout = new Timeout(10000);

  private Random rand = new Random(1234);

  private DataChecksum.Type type = DataChecksum.Type.CRC32C;
  private DataChecksum checksum = DataChecksum.newDataChecksum(
      type, Integer.MAX_VALUE);
  private int dataSize = 75;
  private byte[] data = new byte[dataSize];
  private int chunkSize = 10;
  private int cellSize = 20;

  private int fullCrc;
  private int[] crcsByChunk;
  private int[] crcsByCell;

  private byte[] crcBytesByChunk;
  private byte[] crcBytesByCell;

  @Before
  public void setup() throws IOException {
    rand.nextBytes(data);
    fullCrc = getRangeChecksum(data, 0, dataSize);

    // 7 chunks of size chunkSize, 1 chunk of size (dataSize % chunkSize).
    crcsByChunk = new int[8];
    for (int i = 0; i < 7; ++i) {
      crcsByChunk[i] = getRangeChecksum(data, i * chunkSize, chunkSize);
    }
    crcsByChunk[7] = getRangeChecksum(
        data, (crcsByChunk.length - 1) * chunkSize, dataSize % chunkSize);

    // 3 cells of size cellSize, 1 cell of size (dataSize % cellSize).
    crcsByCell = new int[4];
    for (int i = 0; i < 3; ++i) {
      crcsByCell[i] = getRangeChecksum(data, i * cellSize, cellSize);
    }
    crcsByCell[3] = getRangeChecksum(
        data, (crcsByCell.length - 1) * cellSize, dataSize % cellSize);

    crcBytesByChunk = intArrayToByteArray(crcsByChunk);
    crcBytesByCell = intArrayToByteArray(crcsByCell);
  }

  private int getRangeChecksum(byte[] buf, int offset, int length) {
    checksum.reset();
    checksum.update(buf, offset, length);
    return (int) checksum.getValue();
  }

  private byte[] intArrayToByteArray(int[] values) throws IOException {
    byte[] bytes = new byte[values.length * 4];
    for (int i = 0; i < values.length; ++i) {
      CrcUtil.writeInt(bytes, i * 4, values[i]);
    }
    return bytes;
  }

  @Test
  public void testUnstripedIncorrectChunkSize() throws IOException {
    CrcComposer digester = CrcComposer.newCrcComposer(type, chunkSize);

    // If we incorrectly specify that all CRCs ingested correspond to chunkSize
    // when the last CRC in the array actually corresponds to
    // dataSize % chunkSize then we expect the resulting CRC to not be equal to
    // the fullCrc.
    digester.update(crcBytesByChunk, 0, crcBytesByChunk.length, chunkSize);
    byte[] digest = digester.digest();
    assertEquals(4, digest.length);
    int calculatedCrc = CrcUtil.readInt(digest, 0);
    assertNotEquals(fullCrc, calculatedCrc);
  }

  @Test
  public void testUnstripedByteArray() throws IOException {
    CrcComposer digester = CrcComposer.newCrcComposer(type, chunkSize);
    digester.update(crcBytesByChunk, 0, crcBytesByChunk.length - 4, chunkSize);
    digester.update(
        crcBytesByChunk, crcBytesByChunk.length - 4, 4, dataSize % chunkSize);

    byte[] digest = digester.digest();
    assertEquals(4, digest.length);
    int calculatedCrc = CrcUtil.readInt(digest, 0);
    assertEquals(fullCrc, calculatedCrc);
  }

  @Test
  public void testUnstripedDataInputStream() throws IOException {
    CrcComposer digester = CrcComposer.newCrcComposer(type, chunkSize);
    DataInputStream input =
        new DataInputStream(new ByteArrayInputStream(crcBytesByChunk));
    digester.update(input, crcsByChunk.length - 1, chunkSize);
    digester.update(input, 1, dataSize % chunkSize);

    byte[] digest = digester.digest();
    assertEquals(4, digest.length);
    int calculatedCrc = CrcUtil.readInt(digest, 0);
    assertEquals(fullCrc, calculatedCrc);
  }

  @Test
  public void testUnstripedSingleCrcs() throws IOException {
    CrcComposer digester = CrcComposer.newCrcComposer(type, chunkSize);
    for (int i = 0; i < crcsByChunk.length - 1; ++i) {
      digester.update(crcsByChunk[i], chunkSize);
    }
    digester.update(crcsByChunk[crcsByChunk.length - 1], dataSize % chunkSize);

    byte[] digest = digester.digest();
    assertEquals(4, digest.length);
    int calculatedCrc = CrcUtil.readInt(digest, 0);
    assertEquals(fullCrc, calculatedCrc);
  }

  @Test
  public void testStripedByteArray() throws IOException {
    CrcComposer digester =
        CrcComposer.newStripedCrcComposer(type, chunkSize, cellSize);
    digester.update(crcBytesByChunk, 0, crcBytesByChunk.length - 4, chunkSize);
    digester.update(
        crcBytesByChunk, crcBytesByChunk.length - 4, 4, dataSize % chunkSize);

    byte[] digest = digester.digest();
    assertArrayEquals(crcBytesByCell, digest);
  }

  @Test
  public void testStripedDataInputStream() throws IOException {
    CrcComposer digester =
        CrcComposer.newStripedCrcComposer(type, chunkSize, cellSize);
    DataInputStream input =
        new DataInputStream(new ByteArrayInputStream(crcBytesByChunk));
    digester.update(input, crcsByChunk.length - 1, chunkSize);
    digester.update(input, 1, dataSize % chunkSize);

    byte[] digest = digester.digest();
    assertArrayEquals(crcBytesByCell, digest);
  }

  @Test
  public void testStripedSingleCrcs() throws IOException {
    CrcComposer digester =
        CrcComposer.newStripedCrcComposer(type, chunkSize, cellSize);
    for (int i = 0; i < crcsByChunk.length - 1; ++i) {
      digester.update(crcsByChunk[i], chunkSize);
    }
    digester.update(crcsByChunk[crcsByChunk.length - 1], dataSize % chunkSize);

    byte[] digest = digester.digest();
    assertArrayEquals(crcBytesByCell, digest);
  }

  @Test
  public void testMultiStageMixed() throws IOException {
    CrcComposer digester =
        CrcComposer.newStripedCrcComposer(type, chunkSize, cellSize);

    // First combine chunks into cells.
    DataInputStream input =
        new DataInputStream(new ByteArrayInputStream(crcBytesByChunk));
    digester.update(input, crcsByChunk.length - 1, chunkSize);
    digester.update(input, 1, dataSize % chunkSize);
    byte[] digest = digester.digest();

    // Second, individually combine cells into full crc.
    digester =
        CrcComposer.newCrcComposer(type, cellSize);
    for (int i = 0; i < digest.length - 4; i += 4) {
      int cellCrc = CrcUtil.readInt(digest, i);
      digester.update(cellCrc, cellSize);
    }
    digester.update(digest, digest.length - 4, 4, dataSize % cellSize);
    digest = digester.digest();
    assertEquals(4, digest.length);
    int calculatedCrc = CrcUtil.readInt(digest, 0);
    assertEquals(fullCrc, calculatedCrc);
  }

  @Test
  public void testUpdateMismatchesStripe() throws Exception {
    CrcComposer digester =
        CrcComposer.newStripedCrcComposer(type, chunkSize, cellSize);

    digester.update(crcsByChunk[0], chunkSize);

    // Going from chunkSize to chunkSize + cellSize will cross a cellSize
    // boundary in a single CRC, which is not allowed, since we'd lack a
    // CRC corresponding to the actual cellSize boundary.
    LambdaTestUtils.intercept(
        IOException.class,
        "stripe",
        () -> digester.update(crcsByChunk[1], cellSize));
  }

  @Test
  public void testUpdateByteArrayLengthUnalignedWithCrcSize()
      throws Exception {
    CrcComposer digester = CrcComposer.newCrcComposer(type, chunkSize);

    LambdaTestUtils.intercept(
        IOException.class,
        "length",
        () -> digester.update(crcBytesByChunk, 0, 6, chunkSize));
  }
}
