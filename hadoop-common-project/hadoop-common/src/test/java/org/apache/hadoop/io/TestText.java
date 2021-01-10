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

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Random;
import org.apache.hadoop.thirdparty.com.google.common.base.Charsets;
import org.apache.hadoop.thirdparty.com.google.common.primitives.Bytes;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Unit tests for LargeUTF8. */
public class TestText {
  private static final int NUM_ITERATIONS = 100;

  private static final Random RANDOM = new Random(1);

  private static final int RAND_LEN = -1;
  
  // generate a valid java String
  private static String getTestString(int len) throws Exception {
    StringBuilder buffer = new StringBuilder();    
    int length = (len==RAND_LEN) ? RANDOM.nextInt(1000) : len;
    while (buffer.length()<length) {
      int codePoint = RANDOM.nextInt(Character.MAX_CODE_POINT);
      char tmpStr[] = new char[2];
      if (Character.isDefined(codePoint)) {
        //unpaired surrogate
        if (codePoint < Character.MIN_SUPPLEMENTARY_CODE_POINT &&
            !Character.isHighSurrogate((char)codePoint) &&
            !Character.isLowSurrogate((char)codePoint)) {
          Character.toChars(codePoint, tmpStr, 0);
          buffer.append(tmpStr);
        }
      }
    }
    return buffer.toString();
  }
  
  public static String getTestString() throws Exception {
    return getTestString(RAND_LEN);
  }
  
  public static String getLongString() throws Exception {
    String str = getTestString();
    int length = Short.MAX_VALUE+str.length();
    StringBuilder buffer = new StringBuilder();
    while(buffer.length()<length)
      buffer.append(str);
      
    return buffer.toString();
  }

  @Test
  public void testWritable() throws Exception {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      String str;
      if (i == 0)
        str = getLongString();
      else
        str = getTestString();
      TestWritable.testWritable(new Text(str));
    }
  }


  @Test
  public void testCoding() throws Exception {
    String before = "Bad \t encoding \t testcase";
    Text text = new Text(before);
    String after = text.toString();
    assertTrue(before.equals(after));

    for (int i = 0; i < NUM_ITERATIONS; i++) {
      // generate a random string
      if (i == 0)
        before = getLongString();
      else
        before = getTestString();
    
      // test string to utf8
      ByteBuffer bb = Text.encode(before);
          
      byte[] utf8Text = bb.array();
      byte[] utf8Java = before.getBytes("UTF-8");
      assertEquals(0, WritableComparator.compareBytes(
              utf8Text, 0, bb.limit(),
              utf8Java, 0, utf8Java.length));
      // test utf8 to string
      after = Text.decode(utf8Java);
      assertTrue(before.equals(after));
    }
  }

  @Test
  public void testIO() throws Exception {
    DataOutputBuffer out = new DataOutputBuffer();
    DataInputBuffer in = new DataInputBuffer();

    for (int i = 0; i < NUM_ITERATIONS; i++) {
      // generate a random string
      String before;          
      if (i == 0)
        before = getLongString();
      else
        before = getTestString();
        
      // write it
      out.reset();
      Text.writeString(out, before);
        
      // test that it reads correctly
      in.reset(out.getData(), out.getLength());
      String after = Text.readString(in);
      assertTrue(before.equals(after));
        
      // Test compatibility with Java's other decoder 
      int strLenSize = WritableUtils.getVIntSize(Text.utf8Length(before));
      String after2 = new String(out.getData(), strLenSize, 
                                 out.getLength()-strLenSize, "UTF-8");
      assertTrue(before.equals(after2));
    }
  }
  
  public void doTestLimitedIO(String str, int len) throws IOException {
    DataOutputBuffer out = new DataOutputBuffer();
    DataInputBuffer in = new DataInputBuffer();

    out.reset();
    try {
      Text.writeString(out, str, len);
      fail("expected writeString to fail when told to write a string " +
          "that was too long!  The string was '" + str + "'");
    } catch (IOException e) {
    }
    Text.writeString(out, str, len + 1);

    // test that it reads correctly
    in.reset(out.getData(), out.getLength());
    in.mark(len);
    String after;
    try {
      after = Text.readString(in, len);
      fail("expected readString to fail when told to read a string " +
          "that was too long!  The string was '" + str + "'");
    } catch (IOException e) {
    }
    in.reset();
    after = Text.readString(in, len + 1);
    assertTrue(str.equals(after));
  }

  @Test
  public void testLimitedIO() throws Exception {
    doTestLimitedIO("abcd", 3);
    doTestLimitedIO("foo bar baz", 10);
    doTestLimitedIO("1", 0);
  }

  @Test
  public void testCompare() throws Exception {
    DataOutputBuffer out1 = new DataOutputBuffer();
    DataOutputBuffer out2 = new DataOutputBuffer();
    DataOutputBuffer out3 = new DataOutputBuffer();
    Text.Comparator comparator = new Text.Comparator();
    for (int i=0; i<NUM_ITERATIONS; i++) {
      // reset output buffer
      out1.reset();
      out2.reset();
      out3.reset();

      // generate two random strings
      String str1 = getTestString();
      String str2 = getTestString();
      if (i == 0) {
        str1 = getLongString();
        str2 = getLongString();
      } else {
        str1 = getTestString();
        str2 = getTestString();
      }
          
      // convert to texts
      Text txt1 = new Text(str1);
      Text txt2 = new Text(str2);
      Text txt3 = new Text(str1);
          
      // serialize them
      txt1.write(out1);
      txt2.write(out2);
      txt3.write(out3);
          
      // compare two strings by looking at their binary formats
      int ret1 = comparator.compare(out1.getData(), 0, out1.getLength(),
                                    out2.getData(), 0, out2.getLength());
      // compare two strings
      int ret2 = txt1.compareTo(txt2);
          
      assertEquals(ret1, ret2);
          
      assertEquals("Equivalence of different txt objects, same content" ,
              0,
              txt1.compareTo(txt3));
      assertEquals("Equvalence of data output buffers",
              0,
              comparator.compare(out1.getData(), 0, out3.getLength(),
                      out3.getData(), 0, out3.getLength()));
    }
  }

  @Test
  public void testFind() throws Exception {
    Text text = new Text("abcd\u20acbdcd\u20ac");
    assertThat(text.find("abd")).isEqualTo(-1);
    assertThat(text.find("ac")).isEqualTo(-1);
    assertThat(text.find("\u20ac")).isEqualTo(4);
    assertThat(text.find("\u20ac", 5)).isEqualTo(11);
  }

  @Test
  public void testFindAfterUpdatingContents() throws Exception {
    Text text = new Text("abcd");
    text.set("a".getBytes());
    assertEquals(text.getLength(),1);
    assertEquals(text.find("a"), 0);
    assertEquals(text.find("b"), -1);
  }

  @Test
  public void testValidate() throws Exception {
    Text text = new Text("abcd\u20acbdcd\u20ac");
    byte [] utf8 = text.getBytes();
    int length = text.getLength();
    Text.validateUTF8(utf8, 0, length);
  }

  @Test
  public void testClear() throws Exception {
    // Test lengths on an empty text object
    Text text = new Text();
    assertEquals(
            "Actual string on an empty text object must be an empty string",
        "", text.toString());
    assertEquals("Underlying byte array length must be zero",
            0, text.getBytes().length);
    assertEquals("String's length must be zero",
        0, text.getLength());

    // Test if clear works as intended
    text = new Text("abcd\u20acbdcd\u20ac");
    int len = text.getLength();
    text.clear();
    assertEquals("String must be empty after clear()",
            "", text.toString());
    assertTrue(
            "Length of the byte array must not decrease after clear()",
        text.getBytes().length >= len);
    assertEquals("Length of the string must be reset to 0 after clear()",
        0, text.getLength());
  }

  @Test
  public void testTextText() throws CharacterCodingException {
    Text a=new Text("abc");
    Text b=new Text("a");
    b.set(a);
    assertEquals("abc", b.toString());
    a.append("xdefgxxx".getBytes(), 1, 4);
    assertEquals("modified aliased string", "abc", b.toString());
    assertEquals("appended string incorrectly", "abcdefg", a.toString());
    // add an extra byte so that capacity = 14 and length = 8
    a.append(new byte[]{'d'}, 0, 1);
    assertEquals(14, a.getBytes().length);
    assertEquals(8, a.copyBytes().length);
  }
  
  private class ConcurrentEncodeDecodeThread extends Thread {
    public ConcurrentEncodeDecodeThread(String name) {
      super(name);
    }

    @Override
    public void run() {
      final String name = this.getName();
      DataOutputBuffer out = new DataOutputBuffer();
      DataInputBuffer in = new DataInputBuffer();
      for (int i=0; i < 1000; ++i) {
        try {
          out.reset();
          WritableUtils.writeString(out, name);
          
          in.reset(out.getData(), out.getLength());
          String s = WritableUtils.readString(in);
          
          assertEquals("input buffer reset contents = " + name, name, s);
        } catch (Exception ioe) {
          throw new RuntimeException(ioe);
        }
      }
    }
  }

  @Test
  public void testConcurrentEncodeDecode() throws Exception{
    Thread thread1 = new ConcurrentEncodeDecodeThread("apache");
    Thread thread2 = new ConcurrentEncodeDecodeThread("hadoop");
    
    thread1.start();
    thread2.start();
    
    thread2.join();
    thread2.join();
  }

  @Test
  public void testAvroReflect() throws Exception {
    AvroTestUtil.testReflect
            (new Text("foo"),
                    "{\"type\":\"string\",\"java-class\":\"org.apache.hadoop.io.Text\"}");
  }
  
  /**
   * 
   */
  @Test
  public void testCharAt() {
    String line = "adsawseeeeegqewgasddga";
    Text text = new Text(line);
    for (int i = 0; i < line.length(); i++) {
      assertTrue("testCharAt error1 !!!", text.charAt(i) == line.charAt(i));
    }    
    assertEquals("testCharAt error2 !!!", -1, text.charAt(-1));    
    assertEquals("testCharAt error3 !!!", -1, text.charAt(100));
  }    
  
  /**
   * test {@code Text} readFields/write operations
   */
  @Test
  public void testReadWriteOperations() {
    String line = "adsawseeeeegqewgasddga";
    byte[] inputBytes = line.getBytes();       
    inputBytes = Bytes.concat(new byte[] {(byte)22}, inputBytes);        
    
    DataInputBuffer in = new DataInputBuffer();
    DataOutputBuffer out = new DataOutputBuffer();
    Text text = new Text(line);
    try {      
      in.reset(inputBytes, inputBytes.length);
      text.readFields(in);      
    } catch(Exception ex) {
      fail("testReadFields error !!!");
    }    
    try {
      text.write(out);
    } catch(IOException ex) {      
    } catch(Exception ex) {
      fail("testReadWriteOperations error !!!");
    }        
  }

  @Test
  public void testReadWithKnownLength() throws IOException {
    String line = "hello world";
    byte[] inputBytes = line.getBytes(Charsets.UTF_8);
    DataInputBuffer in = new DataInputBuffer();
    Text text = new Text();

    in.reset(inputBytes, inputBytes.length);
    text.readWithKnownLength(in, 5);
    assertEquals("hello", text.toString());

    // Read longer length, make sure it lengthens
    in.reset(inputBytes, inputBytes.length);
    text.readWithKnownLength(in, 7);
    assertEquals("hello w", text.toString());

    // Read shorter length, make sure it shortens
    in.reset(inputBytes, inputBytes.length);
    text.readWithKnownLength(in, 2);
    assertEquals("he", text.toString());
  }
  
  /**
   * test {@code Text.bytesToCodePoint(bytes) } 
   * with {@code BufferUnderflowException}
   * 
   */
  @Test
  public void testBytesToCodePoint() {
    try {
      ByteBuffer bytes = ByteBuffer.wrap(new byte[] {-2, 45, 23, 12, 76, 89});                                      
      Text.bytesToCodePoint(bytes);
      assertTrue("testBytesToCodePoint error !!!", bytes.position() == 6 );
    } catch (BufferUnderflowException ex) {
      fail("testBytesToCodePoint unexp exception");
    } catch (Exception e) {
      fail("testBytesToCodePoint unexp exception");
    }    
  }

  @Test
  public void testbytesToCodePointWithInvalidUTF() {
    try {                 
      Text.bytesToCodePoint(ByteBuffer.wrap(new byte[] {-2}));
      fail("testbytesToCodePointWithInvalidUTF error unexp exception !!!");
    } catch (BufferUnderflowException ex) {      
    } catch(Exception e) {
      fail("testbytesToCodePointWithInvalidUTF error unexp exception !!!");
    }
  }

  @Test
  public void testUtf8Length() {
    assertEquals("testUtf8Length1 error   !!!",
            1, Text.utf8Length(new String(new char[]{(char) 1})));
    assertEquals("testUtf8Length127 error !!!",
            1, Text.utf8Length(new String(new char[]{(char) 127})));
    assertEquals("testUtf8Length128 error !!!",
            2, Text.utf8Length(new String(new char[]{(char) 128})));
    assertEquals("testUtf8Length193 error !!!",
            2, Text.utf8Length(new String(new char[]{(char) 193})));
    assertEquals("testUtf8Length225 error !!!",
            2, Text.utf8Length(new String(new char[]{(char) 225})));
    assertEquals("testUtf8Length254 error !!!",
            2, Text.utf8Length(new String(new char[]{(char)254})));
  }

}
