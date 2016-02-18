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

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.ChecksumException;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestDataChecksum {

  // Set up buffers that have some header and trailer before the
  // actual data or checksums, to make sure the code handles
  // buffer.position(), limit, etc correctly.
  private static final int SUMS_OFFSET_IN_BUFFER = 3;
  private static final int DATA_OFFSET_IN_BUFFER = 3;
  private static final int DATA_TRAILER_IN_BUFFER = 3;
  
  private static final int BYTES_PER_CHUNK = 512;
  private static final DataChecksum.Type CHECKSUM_TYPES[] = {
    DataChecksum.Type.CRC32, DataChecksum.Type.CRC32C
  };
  
  @Test
  public void testBulkOps() throws Exception {
    for (DataChecksum.Type type : CHECKSUM_TYPES) {
      System.err.println(
          "---- beginning tests with checksum type " + type  + "----");
      DataChecksum checksum = DataChecksum.newDataChecksum(
          type, BYTES_PER_CHUNK);
      for (boolean useDirect : new boolean[]{false, true}) {
        doBulkTest(checksum, 1023, useDirect);
        doBulkTest(checksum, 1024, useDirect);
        doBulkTest(checksum, 1025, useDirect);
      }
    }
  }

  private static class Harness {
    final DataChecksum checksum;
    final int dataLength, sumsLength, numSums;
    ByteBuffer dataBuf, checksumBuf;

    Harness(DataChecksum checksum, int dataLength, boolean useDirect) {
      this.checksum = checksum;
      this.dataLength = dataLength;

      numSums = (dataLength - 1)/checksum.getBytesPerChecksum() + 1;
      sumsLength = numSums * checksum.getChecksumSize();

      byte data[] = new byte[dataLength +
                             DATA_OFFSET_IN_BUFFER +
                             DATA_TRAILER_IN_BUFFER];
      new Random().nextBytes(data);
      dataBuf = ByteBuffer.wrap(
        data, DATA_OFFSET_IN_BUFFER, dataLength);

      byte checksums[] = new byte[SUMS_OFFSET_IN_BUFFER + sumsLength];
      checksumBuf = ByteBuffer.wrap(
        checksums, SUMS_OFFSET_IN_BUFFER, sumsLength);

      // Swap out for direct buffers if requested.
      if (useDirect) {
        dataBuf = directify(dataBuf);
        checksumBuf = directify(checksumBuf);
      }
    }

    void testCorrectness() throws ChecksumException {
      // calculate real checksum, make sure it passes
      checksum.calculateChunkedSums(dataBuf, checksumBuf);
      checksum.verifyChunkedSums(dataBuf, checksumBuf, "fake file", 0);

      // Change a byte in the header and in the trailer, make sure
      // it doesn't affect checksum result
      corruptBufferOffset(checksumBuf, 0);
      checksum.verifyChunkedSums(dataBuf, checksumBuf, "fake file", 0);
      corruptBufferOffset(dataBuf, 0);
      dataBuf.limit(dataBuf.limit() + 1);
      corruptBufferOffset(dataBuf, dataLength + DATA_OFFSET_IN_BUFFER);
      dataBuf.limit(dataBuf.limit() - 1);
      checksum.verifyChunkedSums(dataBuf, checksumBuf, "fake file", 0);

      // Make sure bad checksums fail - error at beginning of array
      corruptBufferOffset(checksumBuf, SUMS_OFFSET_IN_BUFFER);
      try {
        checksum.verifyChunkedSums(dataBuf, checksumBuf, "fake file", 0);
        fail("Did not throw on bad checksums");
      } catch (ChecksumException ce) {
        assertEquals(0, ce.getPos());
      }

      // Make sure bad checksums fail - error at end of array
      uncorruptBufferOffset(checksumBuf, SUMS_OFFSET_IN_BUFFER);
      corruptBufferOffset(checksumBuf, SUMS_OFFSET_IN_BUFFER + sumsLength - 1);
      try {
        checksum.verifyChunkedSums(dataBuf, checksumBuf, "fake file", 0);
        fail("Did not throw on bad checksums");
      } catch (ChecksumException ce) {
        int expectedPos = checksum.getBytesPerChecksum() * (numSums - 1);
        assertEquals(expectedPos, ce.getPos());
        assertTrue(ce.getMessage().contains("fake file"));
      }
    }
  }

  private void doBulkTest(DataChecksum checksum, int dataLength,
      boolean useDirect) throws Exception {
    System.err.println("Testing bulk checksums of length " +
        dataLength + " with " +
        (useDirect ? "direct" : "array-backed") + " buffers");

    new Harness(checksum, dataLength, useDirect).testCorrectness();
  }

  /**
   * Simple performance test for the "common case" checksum usage in HDFS:
   * computing and verifying CRC32C with 512 byte chunking on native
   * buffers.
   */
  @Test
  public void commonUsagePerfTest() throws Exception {
    final int NUM_RUNS = 5;
    final DataChecksum checksum = DataChecksum.newDataChecksum(DataChecksum.Type.CRC32C, 512);
    final int dataLength = 512 * 1024 * 1024;
    Harness h = new Harness(checksum, dataLength, true);

    for (int i = 0; i < NUM_RUNS; i++) {
      StopWatch s = new StopWatch().start();
      // calculate real checksum, make sure it passes
      checksum.calculateChunkedSums(h.dataBuf, h.checksumBuf);
      s.stop();
      System.err.println("Calculate run #" + i + ": " +
                         s.now(TimeUnit.MICROSECONDS) + "us");

      s = new StopWatch().start();
      // calculate real checksum, make sure it passes
      checksum.verifyChunkedSums(h.dataBuf, h.checksumBuf, "fake file", 0);
      s.stop();
      System.err.println("Verify run #" + i + ": " +
                         s.now(TimeUnit.MICROSECONDS) + "us");
    }
  }

  @Test
  public void testEquality() {
    assertEquals(
        DataChecksum.newDataChecksum(DataChecksum.Type.CRC32, 512),
        DataChecksum.newDataChecksum(DataChecksum.Type.CRC32, 512));
    assertFalse(
        DataChecksum.newDataChecksum(DataChecksum.Type.CRC32, 512).equals(
        DataChecksum.newDataChecksum(DataChecksum.Type.CRC32, 1024)));
    assertFalse(
        DataChecksum.newDataChecksum(DataChecksum.Type.CRC32, 512).equals(
        DataChecksum.newDataChecksum(DataChecksum.Type.CRC32C, 512)));        
  }
  
  @Test
  public void testToString() {
    assertEquals("DataChecksum(type=CRC32, chunkSize=512)",
        DataChecksum.newDataChecksum(DataChecksum.Type.CRC32, 512).toString());
  }

  private static void corruptBufferOffset(ByteBuffer buf, int offset) {
    buf.put(offset, (byte)(buf.get(offset) + 1));    
  }

  private static void uncorruptBufferOffset(ByteBuffer buf, int offset) {
    buf.put(offset, (byte)(buf.get(offset) - 1));    
  }

  private static ByteBuffer directify(ByteBuffer dataBuf) {
    ByteBuffer newBuf = ByteBuffer.allocateDirect(dataBuf.capacity());
    newBuf.position(dataBuf.position());
    newBuf.mark();
    newBuf.put(dataBuf);
    newBuf.reset();
    newBuf.limit(dataBuf.limit());
    return newBuf;
  }

  @Test
  public void testCrc32() throws Exception {
    new Crc32PerformanceTest(8, 3, true).run();
    new Crc32PerformanceTest(8, 3, false).run();
  }
}
