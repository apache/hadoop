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

import org.junit.jupiter.api.Test;

import org.apache.hadoop.test.HadoopTestBase;

public class TestUTF8ByteArrayUtils extends HadoopTestBase {
  @Test
  public void testFindByte() {
    byte[] data = "Hello, world!".getBytes();
    assertEquals(-1, UTF8ByteArrayUtils.findByte(data, 0, data.length, (byte) 'a'),
        "Character 'a' does not exist in string");
    assertEquals(4, UTF8ByteArrayUtils.findByte(data, 0, data.length, (byte) 'o'),
        "Did not find first occurrence of character 'o'");
  }

  @Test
  public void testFindBytes() {
    byte[] data = "Hello, world!".getBytes();
    assertEquals(1, UTF8ByteArrayUtils.findBytes(data, 0, data.length, "ello".getBytes()),
        "Did not find first occurrence of pattern 'ello'");
    assertEquals(-1, UTF8ByteArrayUtils.findBytes(data, 2, data.length, "ello".getBytes()),
        "Substring starting at position 2 does not contain pattern 'ello'");
  }

  @Test
  public void testFindNthByte() {
    byte[] data = "Hello, world!".getBytes();
    assertEquals(3, UTF8ByteArrayUtils.findNthByte(data, 0, data.length, (byte) 'l', 2),
        "Did not find 2nd occurrence of character 'l'");
    assertEquals(-1, UTF8ByteArrayUtils.findNthByte(data, 0, data.length, (byte) 'l', 4),
        "4th occurrence of character 'l' does not exist");
    assertEquals(10, UTF8ByteArrayUtils.findNthByte(data, (byte) 'l', 3),
        "Did not find 3rd occurrence of character 'l'");
  }
}
