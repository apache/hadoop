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
package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.hdfs.protocol.Block;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit test for ReplicasMap class
 */
public class TestReplicasMap {
  private static final ReplicasMap map = new ReplicasMap();
  private static final  Block block = new Block(1234, 1234, 1234);
  
  @BeforeClass
  public static void setup() {
    map.add(new FinalizedReplica(block, null, null));
  }
  
  /**
   * Test for ReplicasMap.get(Block) and ReplicasMap.get(long) tests
   */
  @Test
  public void testGet() {
    // Test 1: null argument throws invalid argument exception
    try {
      map.get(null);
      fail("Expected exception not thrown");
    } catch (IllegalArgumentException expected) { }
    
    // Test 2: successful lookup based on block
    assertNotNull(map.get(block));
    
    // Test 3: Lookup failure - generation stamp mismatch 
    Block b = new Block(block);
    b.setGenerationStamp(0);
    assertNull(map.get(b));
    
    // Test 4: Lookup failure - blockID mismatch
    b.setGenerationStamp(block.getGenerationStamp());
    b.setBlockId(0);
    assertNull(map.get(b));
    
    // Test 5: successful lookup based on block ID
    assertNotNull(map.get(block.getBlockId()));
    
    // Test 6: failed lookup for invalid block ID
    assertNull(map.get(0));
  }
  
  @Test
  public void testAdd() {
    // Test 1: null argument throws invalid argument exception
    try {
      map.add(null);
      fail("Expected exception not thrown");
    } catch (IllegalArgumentException expected) { }
  }
  
  @Test
  public void testRemove() {
    // Test 1: null argument throws invalid argument exception
    try {
      map.remove(null);
      fail("Expected exception not thrown");
    } catch (IllegalArgumentException expected) { }
    
    // Test 2: remove failure - generation stamp mismatch 
    Block b = new Block(block);
    b.setGenerationStamp(0);
    assertNull(map.remove(b));
    
    // Test 3: remove failure - blockID mismatch
    b.setGenerationStamp(block.getGenerationStamp());
    b.setBlockId(0);
    assertNull(map.remove(b));
    
    // Test 4: remove success
    assertNotNull(map.remove(block));
    
    // Test 5: remove failure - invalid blockID
    assertNull(map.remove(0));
    
    // Test 6: remove success
    map.add(new FinalizedReplica(block, null, null));
    assertNotNull(map.remove(block.getBlockId()));
  }
}
