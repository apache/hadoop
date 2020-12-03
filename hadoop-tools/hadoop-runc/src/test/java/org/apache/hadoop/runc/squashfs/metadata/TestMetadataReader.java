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

package org.apache.hadoop.runc.squashfs.metadata;

import org.apache.hadoop.runc.squashfs.superblock.SuperBlock;
import org.apache.hadoop.runc.squashfs.test.MetadataBlockReaderMock;
import org.apache.hadoop.runc.squashfs.test.MetadataTestUtils;
import org.junit.Test;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestMetadataReader {

  @Test
  public void isEofShouldReturnTrueOnEmptyStream() throws Exception {
    MetadataReader r = reader(new byte[0], ref(10101, 0L, (short) 0));
    assertTrue(r.isEof());
  }

  @Test
  public void isEofShouldReturnFalseOnSingleByteStream() throws Exception {
    MetadataReader r = reader(new byte[1], ref(10101, 0L, (short) 0));
    assertFalse(r.isEof());
  }

  @Test
  public void isEofShouldReturnTrueIfReadPastMaxLength() throws Exception {
    MetadataReader r = reader(new byte[1], ref(10101, 0L, (short) 0, 1));
    r.skipBytes(1);
    assertTrue(r.isEof());
  }

  @Test
  public void isEofShouldReturnFalseIfPartwayThroughBlock() throws Exception {
    MetadataReader r = reader(new byte[10], ref(10101, 0L, (short) 0, 10));
    r.skipBytes(5);
    assertFalse(r.isEof());
  }

  @Test
  public void positionShouldReturnZeroOnEmptyStream() throws Exception {
    MetadataReader r = reader(new byte[0], ref(10101, 0L, (short) 0));
    assertEquals(0, r.position());
  }

  @Test
  public void availableShouldReturnZeroOnEmptyStream() throws Exception {
    MetadataReader r = reader(new byte[0], ref(10101, 0L, (short) 0));
    assertEquals(0, r.available());
  }

  @Test
  public void availableShouldInitiallyReturnZeroOnSingleByteStream()
      throws Exception {
    MetadataReader r = reader(new byte[1], ref(10101, 0L, (short) 0));
    assertEquals(0, r.available());
  }

  @Test
  public void availableShouldReturnOneOnSingleByteStreamAfterCheckingEof()
      throws Exception {
    MetadataReader r = reader(new byte[1], ref(10101, 0L, (short) 0));
    r.isEof();
    assertEquals(1, r.available());
  }

  @Test
  public void availableShouldReturneZeroOnSingleByteStreamAfterConsumingByte()
      throws Exception {
    MetadataReader r = reader(new byte[1], ref(10101, 0L, (short) 0));
    r.readByte();
    assertEquals(0, r.available());
  }

  @Test
  public void isEofShouldReturnTrueAfterConsumingSingleByteStream()
      throws Exception {
    MetadataReader r = reader(new byte[1], ref(10101, 0L, (short) 0));
    r.readByte();
    assertTrue(r.isEof());
  }

  @Test
  public void readFullyShouldWorkIfNotEof() throws Exception {
    byte[] data = new byte[1024];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) (i & 0xff);
    }
    MetadataReader r = reader(data, ref(10101, 0L, (short) 0));
    byte[] copy = new byte[1024];
    r.readFully(copy);
    assertArrayEquals(data, copy);
  }

  @Test(expected = EOFException.class)
  public void readFullyShouldThrowExceptionIfTooLong() throws Exception {
    byte[] data = new byte[1024];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) (i & 0xff);
    }
    MetadataReader r = reader(data, ref(10101, 0L, (short) 0, 1024));
    byte[] copy = new byte[1025];
    r.readFully(copy);
  }

  @Test
  public void readFullyShouldWorkWithPartials() throws Exception {
    byte[] data = new byte[1024];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) (i & 0xff);
    }
    MetadataReader r = reader(data, ref(10101, 0L, (short) 0));
    byte[] copy = new byte[1024];
    r.readFully(copy, 0, 512);
    r.readFully(copy, 512, 512);
    assertArrayEquals(data, copy);
  }

  @Test
  public void skipBytesShouldDoSo() throws Exception {
    byte[] data = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    MetadataReader r = reader(data, ref(10101, 0L, (short) 0));
    assertEquals("wrong bytes skipped", 5, r.skipBytes(5));
    assertEquals("wrong next byte", (byte) 5, r.readByte());
  }

  @Test
  public void skipBytesShouldDoPartialSkipIfEof() throws Exception {
    byte[] data = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    MetadataReader r = reader(data, ref(10101, 0L, (short) 0, 10));
    assertEquals("wrong bytes skipped", 10, r.skipBytes(15));
  }

  @Test
  public void readBooleanShouldWork() throws Exception {
    byte[] data = new byte[] {0, 1};
    MetadataReader r = reader(data, ref(10101, 0L, (short) 0, 2));
    assertFalse("first value true", r.readBoolean());
    assertTrue("second value false", r.readBoolean());
  }

  @Test
  public void readByteShouldWork() throws Exception {
    byte[] data = new byte[] {(byte) 0xff};
    MetadataReader r = reader(data, ref(10101, 0L, (short) 0, 1));
    assertEquals((byte) 0xff, r.readByte());
  }

  @Test(expected = EOFException.class)
  public void readByteShouldThrowEOFExceptionIfEndOfStream() throws Exception {
    byte[] data = new byte[] {(byte) 0xff};
    MetadataReader r = reader(data, ref(10101, 0L, (short) 0, 1));
    assertEquals((byte) 0xff, r.readByte());
    r.readByte();
  }

  @Test
  public void readUnsignedByteShouldWork() throws Exception {
    byte[] data = new byte[] {(byte) 0xff};
    MetadataReader r = reader(data, ref(10101, 0L, (short) 0, 1));
    assertEquals(0xff, r.readUnsignedByte());
  }

  @Test
  public void readShortShouldSwapBytes() throws Exception {
    byte[] data = new byte[2];
    ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer()
        .put((short) 0x1234);
    MetadataReader r = reader(data, ref(10101, 0L, (short) 0, 2));
    assertEquals((short) 0x1234, r.readShort());
  }

  @Test
  public void readUnsignedShortShouldSwapBytes() throws Exception {
    byte[] data = new byte[2];
    ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer()
        .put((short) 0xfedc);
    MetadataReader r = reader(data, ref(10101, 0L, (short) 0, 2));
    assertEquals(0xfedc, r.readUnsignedShort());
  }

  @Test
  public void readCharShouldSwapBytes() throws Exception {
    byte[] data = new byte[2];
    ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).asCharBuffer()
        .put((char) 0x1234);
    MetadataReader r = reader(data, ref(10101, 0L, (short) 0, 2));
    assertEquals((char) 0x1234, r.readChar());
  }

  @Test
  public void readIntShouldSwapBytes() throws Exception {
    byte[] data = new byte[4];
    ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer()
        .put(0x12345678);
    MetadataReader r = reader(data, ref(10101, 0L, (short) 0, 4));
    assertEquals(0x12345678, r.readInt());
  }

  @Test
  public void readFloatShouldSwapBytes() throws Exception {
    float value = new Random(0L).nextFloat();

    byte[] data = new byte[4];
    ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer()
        .put(value);
    MetadataReader r = reader(data, ref(10101, 0L, (short) 0, 4));
    assertEquals(value, r.readFloat(), 0.0000001f);
  }

  @Test
  public void readLongShouldSwapBytes() throws Exception {
    byte[] data = new byte[8];
    ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).asLongBuffer()
        .put(0x12345678_90abcdefL);
    MetadataReader r = reader(data, ref(10101, 0L, (short) 0, 8));
    assertEquals(0x12345678_90abcdefL, r.readLong());
  }

  @Test
  public void readDoubleShouldSwapBytes() throws Exception {
    double value = new Random(0L).nextDouble();

    byte[] data = new byte[8];
    ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer()
        .put(value);
    MetadataReader r = reader(data, ref(10101, 0L, (short) 0, 8));
    assertEquals(value, r.readDouble(), 0.0000001d);
  }

  @Test
  public void readLineShouldReturnNullOnEof() throws Exception {
    String value = "";
    byte[] data = value.getBytes(StandardCharsets.ISO_8859_1);
    MetadataReader r = reader(data, ref(10101, 0L, (short) 0, data.length));
    assertNull(r.readLine());
  }

  @Test
  public void readLineShouldWorkWhenStringHitsEof() throws Exception {
    String value = "test";
    byte[] data = value.getBytes(StandardCharsets.ISO_8859_1);
    MetadataReader r = reader(data, ref(10101, 0L, (short) 0, data.length));
    assertEquals("test", r.readLine());
  }

  @Test
  public void readLineShouldWorkWhenStringEndsWithLF() throws Exception {
    String value = "test\ntest2";
    byte[] data = value.getBytes(StandardCharsets.ISO_8859_1);
    MetadataReader r = reader(data, ref(10101, 0L, (short) 0, data.length));
    assertEquals("test", r.readLine());
  }

  @Test
  public void readLineShouldWorkWhenStringEndsWithLFAndEof() throws Exception {
    String value = "test\n";
    byte[] data = value.getBytes(StandardCharsets.ISO_8859_1);
    MetadataReader r = reader(data, ref(10101, 0L, (short) 0, data.length));
    assertEquals("test", r.readLine());
  }

  @Test
  public void readLineShouldWorkWhenStringEndsWithCR() throws Exception {
    String value = "test\rtest2";
    byte[] data = value.getBytes(StandardCharsets.ISO_8859_1);
    MetadataReader r = reader(data, ref(10101, 0L, (short) 0, data.length));
    assertEquals("test", r.readLine());
  }

  @Test
  public void readLineShouldWorkWhenStringEndsWithCRAndEof() throws Exception {
    String value = "test\r";
    byte[] data = value.getBytes(StandardCharsets.ISO_8859_1);
    MetadataReader r = reader(data, ref(10101, 0L, (short) 0, data.length));
    assertEquals("test", r.readLine());
  }

  @Test
  public void readLineShouldWorkWhenStringEndsWithCRLF() throws Exception {
    String value = "test\r\ntest2";
    byte[] data = value.getBytes(StandardCharsets.ISO_8859_1);
    MetadataReader r = reader(data, ref(10101, 0L, (short) 0, data.length));
    assertEquals("test", r.readLine());
  }

  @Test
  public void readLineShouldWorkWhenStringEndsWithCRLFAndEof()
      throws Exception {
    String value = "test\r\n";
    byte[] data = value.getBytes(StandardCharsets.ISO_8859_1);
    MetadataReader r = reader(data, ref(10101, 0L, (short) 0, data.length));
    assertEquals("test", r.readLine());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void readUTFShouldThrowUnsupportedOperationException()
      throws Exception {
    byte[] data = new byte[1];
    MetadataReader r = reader(data, ref(10101, 0L, (short) 0, data.length));
    r.readUTF();
  }

  MetadataReference ref(int tag, long blockLocation, short offset) {
    return ref(tag, blockLocation, offset, Integer.MAX_VALUE);
  }

  MetadataReference ref(int tag, long blockLocation, short offset,
      int maxLength) {
    return new MetadataReference(tag, blockLocation, offset, maxLength);
  }

  MetadataReader reader(byte[] data, MetadataReference ref) throws IOException {
    return reader(new SuperBlock(), data, ref);
  }

  MetadataReader reader(SuperBlock sb, byte[] data, MetadataReference ref)
      throws IOException {
    MetadataBlock block = MetadataTestUtils.block(data);
    MetadataBlockReaderMock mbr =
        new MetadataBlockReaderMock(ref.getTag(), sb, ref.getBlockLocation(),
            block);
    return new MetadataReader(mbr, ref);
  }

}
