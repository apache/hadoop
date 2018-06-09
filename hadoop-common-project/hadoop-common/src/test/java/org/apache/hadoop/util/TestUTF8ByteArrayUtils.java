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

import org.junit.Test;

import org.apache.hadoop.test.HadoopTestBase;

import static org.junit.Assert.assertEquals;

public class TestUTF8ByteArrayUtils extends HadoopTestBase {
  @Test
  public void testFindByte() {
    byte[] data = "Hello, world!".getBytes();
    assertEquals("Character 'a' does not exist in string", -1,
        UTF8ByteArrayUtils.findByte(data, 0, data.length, (byte) 'a'));
    assertEquals("Did not find first occurrence of character 'o'", 4,
        UTF8ByteArrayUtils.findByte(data, 0, data.length, (byte) 'o'));
  }

  @Test
  public void testFindBytes() {
    byte[] data = "Hello, world!".getBytes();
    assertEquals("Did not find first occurrence of pattern 'ello'", 1,
        UTF8ByteArrayUtils.findBytes(data, 0, data.length, "ello".getBytes()));
    assertEquals(
        "Substring starting at position 2 does not contain pattern 'ello'", -1,
        UTF8ByteArrayUtils.findBytes(data, 2, data.length, "ello".getBytes()));
  }

  @Test
  public void testFindNthByte() {
    byte[] data = "Hello, world!".getBytes();
    assertEquals("Did not find 2nd occurrence of character 'l'", 3,
        UTF8ByteArrayUtils.findNthByte(data, 0, data.length, (byte) 'l', 2));
    assertEquals("4th occurrence of character 'l' does not exist", -1,
        UTF8ByteArrayUtils.findNthByte(data, 0, data.length, (byte) 'l', 4));
    assertEquals("Did not find 3rd occurrence of character 'l'", 10,
        UTF8ByteArrayUtils.findNthByte(data, (byte) 'l', 3));
  }
}
