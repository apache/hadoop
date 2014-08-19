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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Test;


public class TestExtendedBlock {
  static final String POOL_A = "blockpool-a";
  static final String POOL_B = "blockpool-b";
  static final Block BLOCK_1_GS1 = new Block(1L, 100L, 1L);
  static final Block BLOCK_1_GS2 = new Block(1L, 100L, 2L);
  static final Block BLOCK_2_GS1 = new Block(2L, 100L, 1L);
  
  @Test
  public void testEquals() {
    // Same block -> equal
    assertEquals(
        new ExtendedBlock(POOL_A, BLOCK_1_GS1),
        new ExtendedBlock(POOL_A, BLOCK_1_GS1));
    // Different pools, same block id -> not equal
    assertNotEquals(
        new ExtendedBlock(POOL_A, BLOCK_1_GS1),
        new ExtendedBlock(POOL_B, BLOCK_1_GS1));
    // Same pool, different block id -> not equal
    assertNotEquals(
        new ExtendedBlock(POOL_A, BLOCK_1_GS1),
        new ExtendedBlock(POOL_A, BLOCK_2_GS1));
    // Same block, different genstamps -> equal
    assertEquals(
        new ExtendedBlock(POOL_A, BLOCK_1_GS1),
        new ExtendedBlock(POOL_A, BLOCK_1_GS2));
  }
  
  @Test
  public void testHashcode() {
    
    // Different pools, same block id -> different hashcode
    assertNotEquals(
        new ExtendedBlock(POOL_A, BLOCK_1_GS1).hashCode(),
        new ExtendedBlock(POOL_B, BLOCK_1_GS1).hashCode());
    
    // Same pool, different block id -> different hashcode
    assertNotEquals(
        new ExtendedBlock(POOL_A, BLOCK_1_GS1).hashCode(),
        new ExtendedBlock(POOL_A, BLOCK_2_GS1).hashCode());
    
    // Same block -> same hashcode
    assertEquals(
        new ExtendedBlock(POOL_A, BLOCK_1_GS1).hashCode(),
        new ExtendedBlock(POOL_A, BLOCK_1_GS1).hashCode());

  }

  private static void assertNotEquals(Object a, Object b) {
    assertFalse("expected not equal: '" + a + "' and '" + b + "'",
        a.equals(b));
  }
}
