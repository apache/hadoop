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
package org.apache.hadoop.hdfs.protocol;

import org.junit.Test;

import static org.apache.hadoop.hdfs.protocol.BlockType.CONTIGUOUS;
import static org.apache.hadoop.hdfs.protocol.BlockType.STRIPED;
import static org.junit.Assert.*;

/**
 * Test the BlockType class.
 */
public class TestBlockType {
  @Test
  public void testGetBlockType() throws Exception {
    assertEquals(BlockType.fromBlockId(0x0000000000000000L), CONTIGUOUS);
    assertEquals(BlockType.fromBlockId(0x1000000000000000L), CONTIGUOUS);
    assertEquals(BlockType.fromBlockId(0x2000000000000000L), CONTIGUOUS);
    assertEquals(BlockType.fromBlockId(0x4000000000000000L), CONTIGUOUS);
    assertEquals(BlockType.fromBlockId(0x7000000000000000L), CONTIGUOUS);
    assertEquals(BlockType.fromBlockId(0x00000000ffffffffL), CONTIGUOUS);
    assertEquals(BlockType.fromBlockId(0x10000000ffffffffL), CONTIGUOUS);
    assertEquals(BlockType.fromBlockId(0x20000000ffffffffL), CONTIGUOUS);
    assertEquals(BlockType.fromBlockId(0x40000000ffffffffL), CONTIGUOUS);
    assertEquals(BlockType.fromBlockId(0x70000000ffffffffL), CONTIGUOUS);
    assertEquals(BlockType.fromBlockId(0x70000000ffffffffL), CONTIGUOUS);
    assertEquals(BlockType.fromBlockId(0x0fffffffffffffffL), CONTIGUOUS);
    assertEquals(BlockType.fromBlockId(0x1fffffffffffffffL), CONTIGUOUS);
    assertEquals(BlockType.fromBlockId(0x2fffffffffffffffL), CONTIGUOUS);
    assertEquals(BlockType.fromBlockId(0x4fffffffffffffffL), CONTIGUOUS);
    assertEquals(BlockType.fromBlockId(0x7fffffffffffffffL), CONTIGUOUS);
    assertEquals(BlockType.fromBlockId(0x8000000000000000L), STRIPED);
    assertEquals(BlockType.fromBlockId(0x9000000000000000L), STRIPED);
    assertEquals(BlockType.fromBlockId(0xa000000000000000L), STRIPED);
    assertEquals(BlockType.fromBlockId(0xf000000000000000L), STRIPED);
    assertEquals(BlockType.fromBlockId(0x80000000ffffffffL), STRIPED);
    assertEquals(BlockType.fromBlockId(0x90000000ffffffffL), STRIPED);
    assertEquals(BlockType.fromBlockId(0xa0000000ffffffffL), STRIPED);
    assertEquals(BlockType.fromBlockId(0xf0000000ffffffffL), STRIPED);
    assertEquals(BlockType.fromBlockId(0x8fffffffffffffffL), STRIPED);
    assertEquals(BlockType.fromBlockId(0x9fffffffffffffffL), STRIPED);
    assertEquals(BlockType.fromBlockId(0xafffffffffffffffL), STRIPED);
    assertEquals(BlockType.fromBlockId(0xffffffffffffffffL), STRIPED);
  }
}
