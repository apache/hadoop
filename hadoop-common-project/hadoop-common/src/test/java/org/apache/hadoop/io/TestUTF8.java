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

package org.apache.hadoop.io;

import junit.framework.TestCase;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.StringUtils;

/** Unit tests for UTF8. */
@SuppressWarnings("deprecation")
public class TestUTF8 extends TestCase {
  public TestUTF8(String name) { super(name); }

  private static final Random RANDOM = new Random();

  public static String getTestString() throws Exception {
    StringBuilder buffer = new StringBuilder();
    int length = RANDOM.nextInt(100);
    for (int i = 0; i < length; i++) {
      buffer.append((char)(RANDOM.nextInt(Character.MAX_VALUE)));
    }
    return buffer.toString();
  }

  public void testWritable() throws Exception {
    for (int i = 0; i < 10000; i++) {
      TestWritable.testWritable(new UTF8(getTestString()));
    }
  }

  public void testGetBytes() throws Exception {
    for (int i = 0; i < 10000; i++) {

      // generate a random string
      String before = getTestString();

      // Check that the bytes are stored correctly in Modified-UTF8 format.
      // Note that the DataInput and DataOutput interfaces convert between
      // bytes and Strings using the Modified-UTF8 format.
      assertEquals(before, readModifiedUTF(UTF8.getBytes(before)));
    }
  }

  private String readModifiedUTF(byte[] bytes) throws IOException {
    final short lengthBytes = (short)2;
    ByteBuffer bb = ByteBuffer.allocate(bytes.length + lengthBytes);
    bb.putShort((short)bytes.length).put(bytes);
    ByteArrayInputStream bis = new ByteArrayInputStream(bb.array());
    DataInputStream dis = new DataInputStream(bis);
    return dis.readUTF();
  }

  public void testIO() throws Exception {
    DataOutputBuffer out = new DataOutputBuffer();
    DataInputBuffer in = new DataInputBuffer();

    for (int i = 0; i < 10000; i++) {
      // generate a random string
      String before = getTestString();

      // write it
      out.reset();
      UTF8.writeString(out, before);

      // test that it reads correctly
      in.reset(out.getData(), out.getLength());
      String after = UTF8.readString(in);
      assertEquals(before, after);

      // test that it reads correctly with DataInput
      in.reset(out.getData(), out.getLength());
      String after2 = in.readUTF();
      assertEquals(before, after2);
    }

  }

  public void testNullEncoding() throws Exception {
    String s = new String(new char[] { 0 });

    DataOutputBuffer dob = new DataOutputBuffer();
    new UTF8(s).write(dob);

    assertEquals(s, new String(dob.getData(), 2, dob.getLength()-2, "UTF-8"));
  }

  /**
   * Test encoding and decoding of UTF8 outside the basic multilingual plane.
   *
   * This is a regression test for HADOOP-9103.
   */
  public void testNonBasicMultilingualPlane() throws Exception {
    // Test using the "CAT FACE" character (U+1F431)
    // See http://www.fileformat.info/info/unicode/char/1f431/index.htm
    String catFace = "\uD83D\uDC31";

    // This encodes to 4 bytes in UTF-8:
    byte[] encoded = catFace.getBytes("UTF-8");
    assertEquals(4, encoded.length);
    assertEquals("f09f90b1", StringUtils.byteToHexString(encoded));

    // Decode back to String using our own decoder
    String roundTrip = UTF8.fromBytes(encoded);
    assertEquals(catFace, roundTrip);
  }

  /**
   * Test that decoding invalid UTF8 throws an appropriate error message.
   */
  public void testInvalidUTF8() throws Exception {
    byte[] invalid = new byte[] {
        0x01, 0x02, (byte)0xff, (byte)0xff, 0x01, 0x02, 0x03, 0x04, 0x05 };
    try {
      UTF8.fromBytes(invalid);
      fail("did not throw an exception");
    } catch (UTFDataFormatException utfde) {
      GenericTestUtils.assertExceptionContains(
          "Invalid UTF8 at ffff01020304", utfde);
    }
  }

  /**
   * Test for a 5-byte UTF8 sequence, which is now considered illegal.
   */
  public void test5ByteUtf8Sequence() throws Exception {
    byte[] invalid = new byte[] {
        0x01, 0x02, (byte)0xf8, (byte)0x88, (byte)0x80,
        (byte)0x80, (byte)0x80, 0x04, 0x05 };
    try {
      UTF8.fromBytes(invalid);
      fail("did not throw an exception");
    } catch (UTFDataFormatException utfde) {
      GenericTestUtils.assertExceptionContains(
          "Invalid UTF8 at f88880808004", utfde);
    }
  }
  
  /**
   * Test that decoding invalid UTF8 due to truncation yields the correct
   * exception type.
   */
  public void testInvalidUTF8Truncated() throws Exception {
    // Truncated CAT FACE character -- this is a 4-byte sequence, but we
    // only have the first three bytes.
    byte[] truncated = new byte[] {
        (byte)0xF0, (byte)0x9F, (byte)0x90 };
    try {
      UTF8.fromBytes(truncated);
      fail("did not throw an exception");
    } catch (UTFDataFormatException utfde) {
      GenericTestUtils.assertExceptionContains(
          "Truncated UTF8 at f09f90", utfde);
    }
  }
}
