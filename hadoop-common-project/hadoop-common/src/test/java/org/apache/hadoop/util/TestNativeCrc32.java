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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestNativeCrc32 {

  private static final long BASE_POSITION = 0;
  private static final int IO_BYTES_PER_CHECKSUM_DEFAULT = 512;
  private static final String IO_BYTES_PER_CHECKSUM_KEY =
    "io.bytes.per.checksum";
  private static final int NUM_CHUNKS = 3;

  private DataChecksum.Type checksumType;

  private int bytesPerChecksum;
  private String fileName;
  private ByteBuffer data, checksums;
  private DataChecksum checksum;

  public static Collection<Object[]> data() {
    Collection<Object[]> params = new ArrayList<Object[]>(2);
    params.add(new Object[] { DataChecksum.Type.CRC32 });
    params.add(new Object[] { DataChecksum.Type.CRC32C });
    return params;
  }

  public void initTestNativeCrc32(DataChecksum.Type pChecksumType) {
    this.checksumType = pChecksumType;
    setup();
  }

  public void setup() {
    assumeTrue(NativeCrc32.isAvailable());
    assertEquals(4, checksumType.size,
        "These tests assume they can write a checksum value as a 4-byte int.");
    Configuration conf = new Configuration();
    bytesPerChecksum = conf.getInt(IO_BYTES_PER_CHECKSUM_KEY,
      IO_BYTES_PER_CHECKSUM_DEFAULT);
    fileName = this.getClass().getSimpleName();
    checksum = DataChecksum.newDataChecksum(checksumType, bytesPerChecksum);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testVerifyChunkedSumsSuccess(DataChecksum.Type pChecksumType)
      throws ChecksumException {
    initTestNativeCrc32(pChecksumType);
    allocateDirectByteBuffers();
    fillDataAndValidChecksums();
    NativeCrc32.verifyChunkedSums(bytesPerChecksum, checksumType.id,
      checksums, data, fileName, BASE_POSITION);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testVerifyChunkedSumsFail(DataChecksum.Type pChecksumType) {
    initTestNativeCrc32(pChecksumType);
    allocateDirectByteBuffers();
    fillDataAndInvalidChecksums();
    assertThrows(ChecksumException.class,
        () -> NativeCrc32.verifyChunkedSums(bytesPerChecksum, checksumType.id,
        checksums, data, fileName, BASE_POSITION));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testVerifyChunkedSumsSuccessOddSize(DataChecksum.Type pChecksumType)
      throws ChecksumException {
    initTestNativeCrc32(pChecksumType);
    // Test checksum with an odd number of bytes. This is a corner case that
    // is often broken in checksum calculation, because there is an loop which
    // handles an even multiple or 4 or 8 bytes and then some additional code
    // to finish the few odd bytes at the end. This code can often be broken
    // but is never tested because we are always calling it with an even value
    // such as 512.
    bytesPerChecksum--;
    allocateDirectByteBuffers();
    fillDataAndValidChecksums();
    NativeCrc32.verifyChunkedSums(bytesPerChecksum, checksumType.id,
      checksums, data, fileName, BASE_POSITION);
    bytesPerChecksum++;
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testVerifyChunkedSumsByteArraySuccess(DataChecksum.Type pChecksumType)
      throws ChecksumException {
    initTestNativeCrc32(pChecksumType);
    allocateArrayByteBuffers();
    fillDataAndValidChecksums();
    NativeCrc32.verifyChunkedSumsByteArray(bytesPerChecksum, checksumType.id,
      checksums.array(), checksums.position(), data.array(), data.position(),
      data.remaining(), fileName, BASE_POSITION);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testVerifyChunkedSumsByteArrayFail(DataChecksum.Type pChecksumType) {
    initTestNativeCrc32(pChecksumType);
    allocateArrayByteBuffers();
    fillDataAndInvalidChecksums();
    assertThrows(ChecksumException.class,
        () -> NativeCrc32.verifyChunkedSumsByteArray(bytesPerChecksum,
            checksumType.id, checksums.array(), checksums.position(),
            data.array(), data.position(), data.remaining(), fileName,
            BASE_POSITION));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testCalculateChunkedSumsSuccess(DataChecksum.Type pChecksumType)
      throws ChecksumException {
    initTestNativeCrc32(pChecksumType);
    allocateDirectByteBuffers();
    fillDataAndValidChecksums();
    NativeCrc32.calculateChunkedSums(bytesPerChecksum, checksumType.id,
      checksums, data);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testCalculateChunkedSumsFail(DataChecksum.Type pChecksumType)
      throws ChecksumException {
    initTestNativeCrc32(pChecksumType);
    allocateDirectByteBuffers();
    fillDataAndInvalidChecksums();
    NativeCrc32.calculateChunkedSums(bytesPerChecksum, checksumType.id,
      checksums, data);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testCalculateChunkedSumsByteArraySuccess(DataChecksum.Type pChecksumType)
      throws ChecksumException {
    initTestNativeCrc32(pChecksumType);
    allocateArrayByteBuffers();
    fillDataAndValidChecksums();
    NativeCrc32.calculateChunkedSumsByteArray(bytesPerChecksum, checksumType.id,
      checksums.array(), checksums.position(), data.array(), data.position(),
      data.remaining());
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testCalculateChunkedSumsByteArrayFail(DataChecksum.Type pChecksumType)
      throws ChecksumException {
    initTestNativeCrc32(pChecksumType);
    allocateArrayByteBuffers();
    fillDataAndInvalidChecksums();
    NativeCrc32.calculateChunkedSumsByteArray(bytesPerChecksum, checksumType.id,
      checksums.array(), checksums.position(), data.array(), data.position(),
      data.remaining());
  }

  @ParameterizedTest
  @MethodSource("data")
  @SuppressWarnings("deprecation")
  public void testNativeVerifyChunkedSumsSuccess(DataChecksum.Type pChecksumType)
      throws ChecksumException {
    initTestNativeCrc32(pChecksumType);
    allocateDirectByteBuffers();
    fillDataAndValidChecksums();
    NativeCrc32.nativeVerifyChunkedSums(bytesPerChecksum, checksumType.id,
      checksums, checksums.position(), data, data.position(), data.remaining(),
      fileName, BASE_POSITION);
  }

  @ParameterizedTest
  @MethodSource("data")
  @SuppressWarnings("deprecation")
  public void testNativeVerifyChunkedSumsFail(DataChecksum.Type pChecksumType) {
    initTestNativeCrc32(pChecksumType);
    allocateDirectByteBuffers();
    fillDataAndInvalidChecksums();
    assertThrows(ChecksumException.class,
        () -> NativeCrc32.nativeVerifyChunkedSums(bytesPerChecksum,
            checksumType.id, checksums, checksums.position(), data,
            data.position(), data.remaining(), fileName, BASE_POSITION));
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
