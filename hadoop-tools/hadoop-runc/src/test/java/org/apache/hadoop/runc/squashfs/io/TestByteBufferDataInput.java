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

package org.apache.hadoop.runc.squashfs.io;

import org.junit.Test;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestByteBufferDataInput {

  @Test
  public void readFullyShouldWorkIfNotEof() throws Exception {
    byte[] data = new byte[1024];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) (i & 0xff);
    }
    ByteBufferDataInput r = input(data);
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
    ByteBufferDataInput r = input(data);
    byte[] copy = new byte[1025];
    r.readFully(copy);
  }

  @Test
  public void readFullyShouldWorkWithPartials() throws Exception {
    byte[] data = new byte[1024];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) (i & 0xff);
    }
    ByteBufferDataInput r = input(data);
    byte[] copy = new byte[1024];
    r.readFully(copy, 0, 512);
    r.readFully(copy, 512, 512);
    assertArrayEquals(data, copy);
  }

  @Test
  public void skipBytesShouldDoSo() throws Exception {
    byte[] data = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    ByteBufferDataInput r = input(data);
    assertEquals("wrong bytes skipped", 5, r.skipBytes(5));
    assertEquals("wrong next byte", (byte) 5, r.readByte());
  }

  @Test
  public void skipBytesShouldDoPartialSkipIfEof() throws Exception {
    byte[] data = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    ByteBufferDataInput r = input(data);
    assertEquals("wrong bytes skipped", 10, r.skipBytes(15));
  }

  @Test
  public void readBooleanShouldWork() throws Exception {
    byte[] data = new byte[] {0, 1};
    ByteBufferDataInput r = input(data);
    assertFalse("first value true", r.readBoolean());
    assertTrue("second value false", r.readBoolean());
  }

  @Test
  public void readByteShouldWork() throws Exception {
    byte[] data = new byte[] {(byte) 0xff};
    ByteBufferDataInput r = input(data);
    assertEquals((byte) 0xff, r.readByte());
  }

  @Test(expected = EOFException.class)
  public void readByteShouldThrowEOFExceptionIfEndOfStream() throws Exception {
    byte[] data = new byte[] {(byte) 0xff};
    ByteBufferDataInput r = input(data);
    assertEquals((byte) 0xff, r.readByte());
    r.readByte();
  }

  @Test
  public void readUnsignedByteShouldWork() throws Exception {
    byte[] data = new byte[] {(byte) 0xff};
    ByteBufferDataInput r = input(data);
    assertEquals(0xff, r.readUnsignedByte());
  }

  @Test
  public void readShortShouldWork() throws Exception {
    byte[] data = new byte[2];
    ByteBuffer.wrap(data).asShortBuffer().put((short) 0x1234);
    ByteBufferDataInput r = input(data);
    assertEquals((short) 0x1234, r.readShort());
  }

  @Test
  public void readUnsignedShortShouldWork() throws Exception {
    byte[] data = new byte[2];
    ByteBuffer.wrap(data).asShortBuffer().put((short) 0xfedc);
    ByteBufferDataInput r = input(data);
    assertEquals(0xfedc, r.readUnsignedShort());
  }

  @Test
  public void readCharShouldWork() throws Exception {
    byte[] data = new byte[2];
    ByteBuffer.wrap(data).asCharBuffer().put((char) 0x1234);
    ByteBufferDataInput r = input(data);
    assertEquals((char) 0x1234, r.readChar());
  }

  @Test
  public void readIntShouldWork() throws Exception {
    byte[] data = new byte[4];
    ByteBuffer.wrap(data).asIntBuffer().put(0x12345678);
    ByteBufferDataInput r = input(data);
    assertEquals(0x12345678, r.readInt());
  }

  @Test
  public void readFloatShouldWork() throws Exception {
    float value = new Random(0L).nextFloat();

    byte[] data = new byte[4];
    ByteBuffer.wrap(data).asFloatBuffer().put(value);
    ByteBufferDataInput r = input(data);
    assertEquals(value, r.readFloat(), 0.0000001f);
  }

  @Test
  public void readLongShouldWork() throws Exception {
    byte[] data = new byte[8];
    ByteBuffer.wrap(data).asLongBuffer().put(0x12345678_90abcdefL);
    ByteBufferDataInput r = input(data);
    assertEquals(0x12345678_90abcdefL, r.readLong());
  }

  @Test
  public void readDoubleShouldWork() throws Exception {
    double value = new Random(0L).nextDouble();

    byte[] data = new byte[8];
    ByteBuffer.wrap(data).asDoubleBuffer().put(value);
    ByteBufferDataInput r = input(data);
    assertEquals(value, r.readDouble(), 0.0000001d);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void readLineShouldThrowUnsupportedOperationException()
      throws Exception {
    String value = "test\r\n";
    byte[] data = value.getBytes(StandardCharsets.ISO_8859_1);
    ByteBufferDataInput r = input(data);
    r.readLine();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void readUTFShouldThrowUnsupportedOperationException()
      throws Exception {
    byte[] data = new byte[1];
    ByteBufferDataInput r = input(data);
    r.readUTF();
  }

  ByteBufferDataInput input(byte[] data) throws IOException {
    return new ByteBufferDataInput(ByteBuffer.wrap(data));
  }

}
