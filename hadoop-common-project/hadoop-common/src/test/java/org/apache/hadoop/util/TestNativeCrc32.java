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

import static org.junit.Assert.*;
import static org.junit.Assume.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestNativeCrc32 {

  private static final long BASE_POSITION = 0;
  private static final int IO_BYTES_PER_CHECKSUM_DEFAULT = 512;
  private static final String IO_BYTES_PER_CHECKSUM_KEY =
    "io.bytes.per.checksum";
  private static final int NUM_CHUNKS = 3;

  private final DataChecksum.Type checksumType;

  private int bytesPerChecksum;
  private String fileName;
  private ByteBuffer data, checksums;
  private DataChecksum checksum;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Parameters
  public static Collection<Object[]> data() {
    Collection<Object[]> params = new ArrayList<Object[]>(2);
    params.add(new Object[] { DataChecksum.Type.CRC32 });
    params.add(new Object[] { DataChecksum.Type.CRC32C });
    return params;
  }

  public TestNativeCrc32(DataChecksum.Type checksumType) {
    this.checksumType = checksumType;
  }

  @Before
  public void setup() {
    assumeTrue(NativeCrc32.isAvailable());
    assertEquals(
      "These tests assume they can write a checksum value as a 4-byte int.", 4,
      checksumType.size);
    Configuration conf = new Configuration();
    bytesPerChecksum = conf.getInt(IO_BYTES_PER_CHECKSUM_KEY,
      IO_BYTES_PER_CHECKSUM_DEFAULT);
    fileName = this.getClass().getSimpleName();
    checksum = DataChecksum.newDataChecksum(checksumType, bytesPerChecksum);
  }

  @Test
  public void testVerifyChunkedSumsSuccess() throws ChecksumException {
    allocateDirectByteBuffers();
    fillDataAndValidChecksums();
    NativeCrc32.verifyChunkedSums(bytesPerChecksum, checksumType.id,
      checksums, data, fileName, BASE_POSITION);
  }

  @Test
  public void testVerifyChunkedSumsFail() throws ChecksumException {
    allocateDirectByteBuffers();
    fillDataAndInvalidChecksums();
    exception.expect(ChecksumException.class);
    NativeCrc32.verifyChunkedSums(bytesPerChecksum, checksumType.id,
      checksums, data, fileName, BASE_POSITION);
  }

  @Test
  public void testVerifyChunkedSumsByteArraySuccess() throws ChecksumException {
    allocateArrayByteBuffers();
    fillDataAndValidChecksums();
    NativeCrc32.verifyChunkedSumsByteArray(bytesPerChecksum, checksumType.id,
      checksums.array(), checksums.position(), data.array(), data.position(),
      data.remaining(), fileName, BASE_POSITION);
  }

  @Test
  public void testVerifyChunkedSumsByteArrayFail() throws ChecksumException {
    allocateArrayByteBuffers();
    fillDataAndInvalidChecksums();
    exception.expect(ChecksumException.class);
    NativeCrc32.verifyChunkedSumsByteArray(bytesPerChecksum, checksumType.id,
      checksums.array(), checksums.position(), data.array(), data.position(),
      data.remaining(), fileName, BASE_POSITION);
  }

  @Test
  public void testCalculateChunkedSumsSuccess() throws ChecksumException {
    allocateDirectByteBuffers();
    fillDataAndValidChecksums();
    NativeCrc32.calculateChunkedSums(bytesPerChecksum, checksumType.id,
      checksums, data);
  }

  @Test
  public void testCalculateChunkedSumsFail() throws ChecksumException {
    allocateDirectByteBuffers();
    fillDataAndInvalidChecksums();
    NativeCrc32.calculateChunkedSums(bytesPerChecksum, checksumType.id,
      checksums, data);
  }

  @Test
  public void testCalculateChunkedSumsByteArraySuccess() throws ChecksumException {
    allocateArrayByteBuffers();
    fillDataAndValidChecksums();
    NativeCrc32.calculateChunkedSumsByteArray(bytesPerChecksum, checksumType.id,
      checksums.array(), checksums.position(), data.array(), data.position(),
      data.remaining());
  }

  @Test
  public void testCalculateChunkedSumsByteArrayFail() throws ChecksumException {
    allocateArrayByteBuffers();
    fillDataAndInvalidChecksums();
    NativeCrc32.calculateChunkedSumsByteArray(bytesPerChecksum, checksumType.id,
      checksums.array(), checksums.position(), data.array(), data.position(),
      data.remaining());
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testNativeVerifyChunkedSumsSuccess() throws ChecksumException {
    allocateDirectByteBuffers();
    fillDataAndValidChecksums();
    NativeCrc32.nativeVerifyChunkedSums(bytesPerChecksum, checksumType.id,
      checksums, checksums.position(), data, data.position(), data.remaining(),
      fileName, BASE_POSITION);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testNativeVerifyChunkedSumsFail() throws ChecksumException {
    allocateDirectByteBuffers();
    fillDataAndInvalidChecksums();
    exception.expect(ChecksumException.class);
    NativeCrc32.nativeVerifyChunkedSums(bytesPerChecksum, checksumType.id,
      checksums, checksums.position(), data, data.position(), data.remaining(),
      fileName, BASE_POSITION);
  }

  /**
   * Allocates data buffer and checksums buffer as arrays on the heap.
   */
  private void allocateArrayByteBuffers() {
    data = ByteBuffer.wrap(new byte[bytesPerChecksum * NUM_CHUNKS]);
    checksums = ByteBuffer.wrap(new byte[NUM_CHUNKS * checksumType.size]);
  }

  /**
   * Allocates data buffer and checksums buffer as direct byte buffers.
   */
  private void allocateDirectByteBuffers() {
    data = ByteBuffer.allocateDirect(bytesPerChecksum * NUM_CHUNKS);
    checksums = ByteBuffer.allocateDirect(NUM_CHUNKS * checksumType.size);
  }

  /**
   * Fill data buffer with monotonically increasing byte values.  Overflow is
   * fine, because it's just test data.  Update the checksum with the same byte
   * values.  After every chunk, write the checksum to the checksums buffer.
   * After finished writing, flip the buffers to prepare them for reading.
   */
  private void fillDataAndValidChecksums() {
    for (int i = 0; i < NUM_CHUNKS; ++i) {
      for (int j = 0; j < bytesPerChecksum; ++j) {
        byte b = (byte)((i * bytesPerChecksum + j) & 0xFF);
        data.put(b);
        checksum.update(b);
      }
      checksums.putInt((int)checksum.getValue());
      checksum.reset();
    }
    data.flip();
    checksums.flip();
  }

  /**
   * Fill data buffer with monotonically increasing byte values.  Overflow is
   * fine, because it's just test data.  Update the checksum with different byte
   * byte values, so that the checksums are incorrect intentionally.  After every
   * chunk, write the checksum to the checksums buffer.  After finished writing,
   * flip the buffers to prepare them for reading.
   */
  private void fillDataAndInvalidChecksums() {
    for (int i = 0; i < NUM_CHUNKS; ++i) {
      for (int j = 0; j < bytesPerChecksum; ++j) {
        byte b = (byte)((i * bytesPerChecksum + j) & 0xFF);
        data.put(b);
        checksum.update((byte)(b + 1));
      }
      checksums.putInt((int)checksum.getValue());
      checksum.reset();
    }
    data.flip();
    checksums.flip();
  }
}
