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

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * This is the unit test for BytesWritable.
 */
public class TestBytesWritable {

  @Test
  public void testSizeChange() throws Exception {
    byte[] hadoop = "hadoop".getBytes();
    BytesWritable buf = new BytesWritable(hadoop);
    int size = buf.getLength();
    int orig_capacity = buf.getCapacity();
    buf.setSize(size*2);
    int new_capacity = buf.getCapacity();
    System.arraycopy(buf.getBytes(), 0, buf.getBytes(), size, size);
    assertTrue(new_capacity >= size * 2);
    assertEquals(size * 2, buf.getLength());
    assertTrue(new_capacity != orig_capacity);
    buf.setSize(size*4);
    assertTrue(new_capacity != buf.getCapacity());
    for(int i=0; i < size*2; ++i) {
      assertEquals(hadoop[i%size], buf.getBytes()[i]);
    }
    // ensure that copyBytes is exactly the right length
    assertEquals(size*4, buf.copyBytes().length);
    // shrink the buffer
    buf.setCapacity(1);
    // make sure the size has been cut down too
    assertEquals(1, buf.getLength());
    // but that the data is still there
    assertEquals(hadoop[0], buf.getBytes()[0]);
  }
  
  @Test
  public void testHash() throws Exception {
    byte[] owen = "owen".getBytes();
    BytesWritable buf = new BytesWritable(owen);
    assertEquals(4347922, buf.hashCode());
    buf.setCapacity(10000);
    assertEquals(4347922, buf.hashCode());
    buf.setSize(0);
    assertEquals(1, buf.hashCode());
  }
  
  @Test
  public void testCompare() throws Exception {
    byte[][] values = new byte[][]{"abc".getBytes(), 
                                   "ad".getBytes(),
                                   "abcd".getBytes(),
                                   "".getBytes(),
                                   "b".getBytes()};
    BytesWritable[] buf = new BytesWritable[values.length];
    for(int i=0; i < values.length; ++i) {
      buf[i] = new BytesWritable(values[i]);
    }
    // check to make sure the compare function is symetric and reflexive
    for(int i=0; i < values.length; ++i) {
      for(int j=0; j < values.length; ++j) {
        assertTrue(buf[i].compareTo(buf[j]) == -buf[j].compareTo(buf[i]));
        assertTrue((i == j) == (buf[i].compareTo(buf[j]) == 0));
      }
    }
    assertTrue(buf[0].compareTo(buf[1]) < 0);
    assertTrue(buf[1].compareTo(buf[2]) > 0);
    assertTrue(buf[2].compareTo(buf[3]) > 0);
    assertTrue(buf[3].compareTo(buf[4]) < 0);
  }
  
  private void checkToString(byte[] input, String expected) {
    String actual = new BytesWritable(input).toString();
    assertEquals(expected, actual);
  }

  @Test
  public void testToString() {
    checkToString(new byte[]{0,1,2,0x10}, "00 01 02 10");
    checkToString(new byte[]{-0x80, -0x7f, -0x1, -0x2, 1, 0}, 
                  "80 81 ff fe 01 00");
  }
  /**
   * This test was written as result of adding the new zero
   * copy constructor and set method to BytesWritable. These
   * methods allow users to specify the backing buffer of the
   * BytesWritable instance and a length. 
   */
  @Test
  public void testZeroCopy() {
    byte[] bytes = "brock".getBytes();
    BytesWritable zeroBuf = new BytesWritable(bytes, bytes.length); // new
    BytesWritable copyBuf = new BytesWritable(bytes); // old
    // using zero copy constructor shouldn't result in a copy
    assertTrue("copy took place, backing array != array passed to constructor",
      bytes == zeroBuf.getBytes());
    assertTrue("length of BW should backing byte array", zeroBuf.getLength() == bytes.length);
    assertEquals("objects with same backing array should be equal", zeroBuf, copyBuf);
    assertEquals("string repr of objects with same backing array should be equal", 
        zeroBuf.toString(), copyBuf.toString());
    assertTrue("compare order objects with same backing array should be equal", 
        zeroBuf.compareTo(copyBuf) == 0);
    assertTrue("hash of objects with same backing array should be equal",
        zeroBuf.hashCode() == copyBuf.hashCode());
    
    // ensure expanding buffer is handled correctly
    // for buffers created with zero copy api
    byte[] buffer = new byte[bytes.length * 5];
    zeroBuf.set(buffer, 0, buffer.length); // expand internal buffer
    zeroBuf.set(bytes, 0, bytes.length); // set back to normal contents
    assertEquals("buffer created with (array, len) has bad contents", 
        zeroBuf, copyBuf);
    assertTrue("buffer created with (array, len) has bad length",
        zeroBuf.getLength() == copyBuf.getLength());
  }
    
  /**
   * test {@link ByteWritable} 
   * methods compareTo(), toString(), equals()
   */
  @Test
  public void testObjectCommonMethods() {    
    byte b = 0x9;
    ByteWritable bw = new ByteWritable();
    bw.set(b);
    assertTrue("testSetByteWritable error", bw.get() == b);
    assertTrue("testSetByteWritable error < 0", bw.compareTo(new ByteWritable((byte)0xA)) < 0);
    assertTrue("testSetByteWritable error > 0", bw.compareTo(new ByteWritable((byte)0x8)) > 0);
    assertTrue("testSetByteWritable error == 0", bw.compareTo(new ByteWritable((byte)0x9)) == 0);
    assertTrue("testSetByteWritable equals error !!!", bw.equals(new ByteWritable((byte)0x9)));
    assertTrue("testSetByteWritable equals error !!!", ! bw.equals(new ByteWritable((byte)0xA)));
    assertTrue("testSetByteWritable equals error !!!", ! bw.equals(new IntWritable(1)));
    assertEquals("testSetByteWritable error ", "9", bw.toString());    
  }
  
}
