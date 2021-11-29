/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.common;

import static org.junit.Assert.*;

import org.junit.Test;

import java.nio.ByteBuffer;

public class FilePositionTest {

  @Test
  public void testArgChecks() {
    ByteBuffer buffer = ByteBuffer.allocate(10);
    BufferData data = new BufferData(0, buffer);

    // Should not throw.
    new FilePosition(0, 0);
    new FilePosition(0, 5);
    new FilePosition(10, 5);
    new FilePosition(5, 10);
    new FilePosition(10, 5).setData(data, 3, 4);

    // Verify it throws correctly.
    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'fileSize' must not be negative",
        () -> new FilePosition(-1, 2));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'blockSize' must be a positive integer",
        () -> new FilePosition(1, 0));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'blockSize' must be a positive integer",
        () -> new FilePosition(1, -1));

    FilePosition pos = new FilePosition(10, 3);

    // Verify that we cannot obtain buffer properties without setting buffer.
    ExceptionAsserts.assertThrows(
        IllegalStateException.class,
        "'buffer' must not be null",
        () -> pos.buffer());

    ExceptionAsserts.assertThrows(
        IllegalStateException.class,
        "'buffer' must not be null",
        () -> pos.absolute());

    ExceptionAsserts.assertThrows(
        IllegalStateException.class,
        "'buffer' must not be null",
        () -> pos.isWithinCurrentBuffer(2));

    ExceptionAsserts.assertThrows(
        IllegalStateException.class,
        "'buffer' must not be null",
        () -> pos.blockNumber());

    ExceptionAsserts.assertThrows(
        IllegalStateException.class,
        "'buffer' must not be null",
        () -> pos.isLastBlock());

    ExceptionAsserts.assertThrows(
        IllegalStateException.class,
        "'buffer' must not be null",
        () -> pos.bufferFullyRead());

    // Verify that we cannot set invalid buffer parameters.
    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'data' must not be null",
        () -> pos.setData(null, 4, 4));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'bufferStartOffset' must not be negative",
        () -> pos.setData(data, -4, 4));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'readStartOffset' must not be negative",
        () -> pos.setData(data, 4, -4));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'readStartOffset' must not be negative",
        () -> pos.setData(data, 4, -4));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'readStartOffset' (15) must be within the range [4, 13]",
        () -> pos.setData(data, 4, 15));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'readStartOffset' (3) must be within the range [4, 13]",
        () -> pos.setData(data, 4, 3));
  }

  @Test
  public void testValidity() {
    int bufferSize = 8;
    long fileSize = 100;
    long bufferStartOffset = 7;
    long readStartOffset = 9;

    ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
    BufferData data = new BufferData(0, buffer);
    FilePosition pos = new FilePosition(fileSize, bufferSize);

    assertFalse(pos.isValid());
    pos.setData(data, bufferStartOffset, readStartOffset);
    assertTrue(pos.isValid());

    pos.invalidate();
    assertFalse(pos.isValid());
  }

  @Test
  public void testOffsets() {
    int bufferSize = 8;
    long fileSize = 100;
    long bufferStartOffset = 7;
    long readStartOffset = 9;

    ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
    BufferData data = new BufferData(0, buffer);
    FilePosition pos = new FilePosition(fileSize, bufferSize);
    pos.setData(data, bufferStartOffset, readStartOffset);
    assertTrue(pos.isValid());

    assertEquals(readStartOffset, pos.absolute());
    assertEquals(readStartOffset - bufferStartOffset, pos.relative());
    assertTrue(pos.isWithinCurrentBuffer(8));
    assertFalse(pos.isWithinCurrentBuffer(6));
    assertFalse(pos.isWithinCurrentBuffer(1));

    int expectedBlockNumber = (int) (bufferStartOffset / bufferSize);
    assertEquals(expectedBlockNumber, pos.blockNumber());
    assertFalse(pos.isLastBlock());

    pos.setData(data, fileSize - 3, fileSize - 2);
    assertTrue(pos.isLastBlock());
  }

  @Test
  public void testBufferStats() {
    int bufferSize = 8;
    long fileSize = 100;
    long bufferStartOffset = 7;
    long readStartOffset = 9;

    ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
    BufferData data = new BufferData(0, buffer);
    FilePosition pos = new FilePosition(fileSize, bufferSize);
    pos.setData(data, bufferStartOffset, readStartOffset);
    assertTrue(pos.isValid());
    assertEquals(bufferStartOffset, pos.bufferStartOffset());

    assertEquals(0, pos.numBytesRead());
    assertEquals(0, pos.numSingleByteReads());
    assertEquals(0, pos.numBufferReads());

    pos.incrementBytesRead(1);
    pos.incrementBytesRead(1);
    pos.incrementBytesRead(1);
    pos.incrementBytesRead(5);
    pos.incrementBytesRead(51);

    assertEquals(59, pos.numBytesRead());
    assertEquals(3, pos.numSingleByteReads());
    assertEquals(2, pos.numBufferReads());

    assertFalse(pos.bufferFullyRead());

    pos.setData(data, bufferStartOffset, bufferStartOffset);
    assertTrue(pos.isValid());

    assertEquals(0, pos.numBytesRead());
    assertEquals(0, pos.numSingleByteReads());
    assertEquals(0, pos.numBufferReads());

    for (int i = 0; i < bufferSize; i++) {
      pos.buffer().get();
      pos.incrementBytesRead(1);
    }
    assertTrue(pos.bufferFullyRead());
  }
}
