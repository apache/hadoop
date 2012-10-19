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
package org.apache.hadoop.hdfs.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;

public class TestExactSizeInputStream {
  @Test
  public void testBasicsReadSingle() throws IOException {
    ExactSizeInputStream s = new ExactSizeInputStream(byteStream("hello"), 3);
    assertEquals(3, s.available());
    
    assertEquals((int)'h', s.read());
    assertEquals((int)'e', s.read());
    assertEquals((int)'l', s.read());
    assertEquals(-1, s.read());
    assertEquals(0, s.available());
  }
  
  @Test
  public void testBasicsReadArray() throws IOException {
    ExactSizeInputStream s = new ExactSizeInputStream(byteStream("hello"), 3);
    assertEquals(3, s.available());
    
    byte[] buf = new byte[10];
    
    assertEquals(2, s.read(buf, 0, 2));
    assertEquals('h', buf[0]);
    assertEquals('e', buf[1]);
    
    assertEquals(1, s.read(buf, 0, 2));
    assertEquals('l', buf[0]);
    
    assertEquals(-1, s.read(buf, 0, 2));
  }
  
  @Test
  public void testBasicsSkip() throws IOException {
    ExactSizeInputStream s = new ExactSizeInputStream(byteStream("hello"), 3);
    assertEquals(3, s.available());
    
    assertEquals(2, s.skip(2));
    assertEquals(1, s.skip(2));
    assertEquals(0, s.skip(2));
  }
  
  @Test
  public void testReadNotEnough() throws IOException {
    // Ask for 5 bytes, only has 2
    ExactSizeInputStream s = new ExactSizeInputStream(byteStream("he"), 5);
    assertEquals(2, s.available());
    
    assertEquals((int)'h', s.read());
    assertEquals((int)'e', s.read());
    try {
      s.read();
      fail("Read when should be out of data");
    } catch (EOFException e) {
      // expected
    }
  }
  
  @Test
  public void testSkipNotEnough() throws IOException {
    // Ask for 5 bytes, only has 2
    ExactSizeInputStream s = new ExactSizeInputStream(byteStream("he"), 5);
    assertEquals(2, s.skip(3));
    try {
      s.skip(1);
      fail("Skip when should be out of data");
    } catch (EOFException e) {
      // expected
    }
  }
  
  @Test
  public void testReadArrayNotEnough() throws IOException {
    // Ask for 5 bytes, only has 2
    ExactSizeInputStream s = new ExactSizeInputStream(byteStream("he"), 5);
    byte[] buf = new byte[10];
    assertEquals(2, s.read(buf, 0, 5));
    try {
      s.read(buf, 2, 3);
      fail("Read buf when should be out of data");
    } catch (EOFException e) {
      // expected
    }
  }

  @Test
  public void testMark() throws IOException {
    ExactSizeInputStream s = new ExactSizeInputStream(byteStream("he"), 5);
    assertFalse(s.markSupported());
    try {
      s.mark(1);
      fail("Mark should not succeed");
    } catch (UnsupportedOperationException uoe) {
      // expected
    }
  }
  
  private static InputStream byteStream(String data) {
    return new ByteArrayInputStream(data.getBytes());
  }
}
