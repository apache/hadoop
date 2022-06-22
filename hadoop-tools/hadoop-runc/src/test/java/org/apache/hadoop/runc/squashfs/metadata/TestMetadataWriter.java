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

import org.apache.hadoop.runc.squashfs.test.MetadataTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestMetadataWriter {

  private MetadataWriter writer;

  @Before
  public void setUp() {
    writer = new MetadataWriter();
  }

  @Test
  public void getCurrentReferenceShouldReturnZeroBeforeBlocksWritten() {
    MetadataBlockRef ref = writer.getCurrentReference();
    assertEquals("wrong location", 0, ref.getLocation());
    assertEquals("wrong offset", (short) 0, ref.getOffset());
  }

  byte[] save() throws IOException {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
      try (DataOutputStream dos = new DataOutputStream(bos)) {
        writer.save(dos);
      }
      return bos.toByteArray();
    }
  }

  @Test
  public void writeByteArrayShouldWriteAsIs() throws Exception {
    byte[] buf = new byte[] {0, 1, 2, 3, 4};
    writer.write(buf);
    byte[] out = MetadataTestUtils.saveMetadataBlock(writer);
    byte[] buf2 = MetadataTestUtils.decodeMetadataBlock(out);
    assertArrayEquals(buf, buf2);
  }

  @Test
  public void writeLongByteArrayShouldSucceed() throws Exception {
    byte[] buf = new byte[16384];
    for (int i = 0; i < buf.length; i++) {
      buf[i] = (byte) (i % 32);
    }
    writer.write(buf);
    byte[] out = MetadataTestUtils.saveMetadataBlock(writer);
    byte[] buf2 = MetadataTestUtils.decodeMetadataBlocks(out, 0);
    assertArrayEquals(buf, buf2);
  }

  @Test
  public void writeEmptyByteArrayShouldDoNothing() throws Exception {
    byte[] buf = new byte[0];
    writer.write(buf);
    byte[] out = MetadataTestUtils.saveMetadataBlock(writer);
    assertEquals("wrong length", 0, out.length);
  }

  @Test
  public void flushShouldDoNothingIfNoData() throws Exception {
    writer.flush();
    byte[] out = MetadataTestUtils.saveMetadataBlock(writer);
    assertEquals("wrong length", 0, out.length);
  }

  @Test
  public void flushShouldWriteMultipleBlocksIfNecessary() throws Exception {
    byte[] buf0 = new byte[] {0, 1, 2, 3, 4};
    byte[] buf1 = new byte[] {5, 6, 7, 8, 9};

    MetadataBlockRef ref1 = writer.getCurrentReference();
    System.out.println(ref1);
    writer.write(buf0);
    writer.flush();

    MetadataBlockRef ref2 = writer.getCurrentReference();
    System.out.println(ref2);
    writer.write(buf1);
    writer.flush();

    byte[] out = MetadataTestUtils.saveMetadataBlock(writer);
    byte[] buf2 =
        MetadataTestUtils.decodeMetadataBlock(out, ref1.getLocation());
    byte[] buf3 =
        MetadataTestUtils.decodeMetadataBlock(out, ref2.getLocation());
    assertArrayEquals(buf0, buf2);
    assertArrayEquals(buf1, buf3);
  }

  @Test
  public void flushShouldHandleWritingCompressedData() throws Exception {
    byte[] buf = new byte[8192];
    for (int i = 0; i < buf.length; i++) {
      buf[i] = (byte) 0xff;
    }

    MetadataBlockRef ref = writer.getCurrentReference();
    System.out.println(ref);
    writer.write(buf);
    writer.flush();

    byte[] out = MetadataTestUtils.saveMetadataBlock(writer);
    byte[] buf2 = MetadataTestUtils.decodeMetadataBlock(out, ref.getLocation());
    assertArrayEquals(buf, buf2);
  }

  @Test
  public void writeByteArrayWithOffsetAndLengthShouldSucceed()
      throws Exception {
    byte[] buf = new byte[1024];
    for (int i = 0; i < buf.length; i++) {
      buf[i] = (byte) (i % 256);
    }
    writer.write(buf, 10, 100);
    byte[] out = MetadataTestUtils.saveMetadataBlock(writer);
    byte[] buf2 = MetadataTestUtils.decodeMetadataBlocks(out, 0);
    byte[] bufPrime = Arrays.copyOfRange(buf, 10, 110);
    assertArrayEquals(bufPrime, buf2);
  }

  @Test
  public void writeIntAsByteShouldStripHighBits() throws Exception {
    for (int i = 0; i < 512; i++) {
      writer.write(i);
    }
    byte[] out = MetadataTestUtils.saveMetadataBlock(writer);
    byte[] buf2 = MetadataTestUtils.decodeMetadataBlocks(out, 0);
    assertEquals("wrong length", 512, buf2.length);
    for (int i = 0; i < buf2.length; i++) {
      assertEquals(String.format("wrong value at index %d", i),
          (byte) (i % 256), buf2[i]);
    }
  }

  @Test
  public void writeByteShouldStripHighBits() throws Exception {
    for (int i = 0; i < 512; i++) {
      writer.writeByte(i);
    }
    byte[] out = MetadataTestUtils.saveMetadataBlock(writer);
    byte[] buf2 = MetadataTestUtils.decodeMetadataBlocks(out, 0);
    assertEquals("wrong length", 512, buf2.length);
    for (int i = 0; i < buf2.length; i++) {
      assertEquals(String.format("wrong value at index %d", i),
          (byte) (i % 256), buf2[i]);
    }
  }

  @Test
  public void writeBooleanShouldWorkAsExpected() throws Exception {
    writer.writeBoolean(false);
    writer.writeBoolean(true);
    byte[] out = MetadataTestUtils.saveMetadataBlock(writer);
    byte[] buf2 = MetadataTestUtils.decodeMetadataBlocks(out, 0);
    assertEquals("wrong length", 2, buf2.length);
    assertEquals("wrong value for false", (byte) 0, buf2[0]);
    assertEquals("wrong value for true", (byte) 1, buf2[1]);
  }

  @Test
  public void writeShortShouldByteSwapOutput() throws Exception {
    for (int i = 0; i <= 0xffff; i++) {
      writer.writeShort(i);
    }

    byte[] out = MetadataTestUtils.saveMetadataBlock(writer);
    byte[] buf2 = MetadataTestUtils.decodeMetadataBlocks(out, 0);
    assertEquals("wrong length", 0x20000, buf2.length);
    ShortBuffer sb =
        ByteBuffer.wrap(buf2).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer();
    for (int i = 0; i <= 0xffff; i++) {
      assertEquals(String.format("wrong value at index %d", i),
          (short) (i & 0xffff), sb.get(i));
    }
  }

  @Test
  public void writeCharShouldByteSwapOutput() throws Exception {
    for (int i = 0; i <= 0xffff; i++) {
      writer.writeChar(i);
    }

    byte[] out = MetadataTestUtils.saveMetadataBlock(writer);
    byte[] buf2 = MetadataTestUtils.decodeMetadataBlocks(out, 0);
    assertEquals("wrong length", 0x20000, buf2.length);
    CharBuffer cb =
        ByteBuffer.wrap(buf2).order(ByteOrder.LITTLE_ENDIAN).asCharBuffer();
    for (int i = 0; i <= 0xffff; i++) {
      assertEquals(String.format("wrong value at index %d", i),
          (char) (i & 0xffff), cb.get(i));
    }
  }

  @Test
  public void writIntShouldByteSwapOutput() throws Exception {
    writer.writeInt(0x000000ff);
    writer.writeInt(0x0000ff00);
    writer.writeInt(0x00ff0000);
    writer.writeInt(0xff000000);

    byte[] out = MetadataTestUtils.saveMetadataBlock(writer);
    byte[] buf2 = MetadataTestUtils.decodeMetadataBlocks(out, 0);
    assertEquals("wrong length", 16, buf2.length);
    IntBuffer ib =
        ByteBuffer.wrap(buf2).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
    assertEquals(0x000000ff, ib.get(0));
    assertEquals(0x0000ff00, ib.get(1));
    assertEquals(0x00ff0000, ib.get(2));
    assertEquals(0xff000000, ib.get(3));
  }

  @Test
  public void writLongShouldByteSwapOutput() throws Exception {
    writer.writeLong(0x0000_0000_0000_00ffL);
    writer.writeLong(0x0000_0000_0000_ff00L);
    writer.writeLong(0x0000_0000_00ff_0000L);
    writer.writeLong(0x0000_0000_ff00_0000L);
    writer.writeLong(0x0000_00ff_0000_0000L);
    writer.writeLong(0x0000_ff00_0000_0000L);
    writer.writeLong(0x00ff_0000_0000_0000L);
    writer.writeLong(0xff00_0000_0000_0000L);

    byte[] out = MetadataTestUtils.saveMetadataBlock(writer);
    byte[] buf2 = MetadataTestUtils.decodeMetadataBlocks(out, 0);
    assertEquals("wrong length", 64, buf2.length);

    LongBuffer lb =
        ByteBuffer.wrap(buf2).order(ByteOrder.LITTLE_ENDIAN).asLongBuffer();
    assertEquals(0x0000_0000_0000_00ffL, lb.get(0));
    assertEquals(0x0000_0000_0000_ff00L, lb.get(1));
    assertEquals(0x0000_0000_00ff_0000L, lb.get(2));
    assertEquals(0x0000_0000_ff00_0000L, lb.get(3));
    assertEquals(0x0000_00ff_0000_0000L, lb.get(4));
    assertEquals(0x0000_ff00_0000_0000L, lb.get(5));
    assertEquals(0x00ff_0000_0000_0000L, lb.get(6));
    assertEquals(0xff00_0000_0000_0000L, lb.get(7));
  }

  @Test
  public void writeFloatShouldByteSwapOutput() throws Exception {
    Random r = new Random(0L);

    float[] data = new float[1024];
    for (int i = 0; i < data.length; i++) {
      data[i] = r.nextFloat();
      writer.writeFloat(data[i]);
    }

    byte[] out = MetadataTestUtils.saveMetadataBlock(writer);
    byte[] buf2 = MetadataTestUtils.decodeMetadataBlocks(out, 0);
    assertEquals("wrong length", 4096, buf2.length);

    FloatBuffer fb =
        ByteBuffer.wrap(buf2).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer();
    for (int i = 0; i < data.length; i++) {
      assertEquals(String.format("wrong value at index %d", i), data[i],
          fb.get(i), 0.0000001f);
    }
  }

  @Test
  public void writeDoubleShouldByteSwapOutput() throws Exception {
    Random r = new Random(0L);

    double[] data = new double[1024];
    for (int i = 0; i < data.length; i++) {
      data[i] = r.nextDouble();
      writer.writeDouble(data[i]);
    }

    byte[] out = MetadataTestUtils.saveMetadataBlock(writer);
    byte[] buf2 = MetadataTestUtils.decodeMetadataBlocks(out, 0);
    assertEquals("wrong length", 8192, buf2.length);

    DoubleBuffer db =
        ByteBuffer.wrap(buf2).order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer();
    for (int i = 0; i < data.length; i++) {
      assertEquals(String.format("wrong value at index %d", i), data[i],
          db.get(i), 0.0000001d);
    }
  }

  @Test
  public void writeCharsStringShouldByteSwapOutput() throws Exception {
    StringBuilder buf = new StringBuilder();
    for (int i = 0; i <= 0xffff; i++) {
      buf.append((char) i);
    }
    String str = buf.toString();
    writer.writeChars(str);

    byte[] out = MetadataTestUtils.saveMetadataBlock(writer);
    byte[] buf2 = MetadataTestUtils.decodeMetadataBlocks(out, 0);
    assertEquals("wrong length", 0x20000, buf2.length);
    CharBuffer cb =
        ByteBuffer.wrap(buf2).order(ByteOrder.LITTLE_ENDIAN).asCharBuffer();
    for (int i = 0; i <= 0xffff; i++) {
      assertEquals(String.format("wrong value at index %d", i), str.charAt(i),
          cb.get(i));
    }
  }

  @Test
  public void writeBytesStringShouldWorkAsExpected() throws Exception {
    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < 512; i++) {
      buf.append((char) i);
    }
    String str = buf.toString();
    writer.writeBytes(str);

    byte[] out = MetadataTestUtils.saveMetadataBlock(writer);
    byte[] buf2 = MetadataTestUtils.decodeMetadataBlocks(out, 0);
    assertEquals("wrong length", 512, buf2.length);
    ByteBuffer bb = ByteBuffer.wrap(buf2).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < 512; i++) {
      assertEquals(String.format("wrong value at index %d", i),
          (byte) (i & 0xff), bb.get(i));
    }
  }

  @Test(expected = UnsupportedOperationException.class)
  public void writeUTFShouldThrowsExcpetion() throws Exception {
    writer.writeUTF("test");
  }

}
