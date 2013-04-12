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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import org.junit.Test;

/**
 * Tests SortedMapWritable
 */
public class TestSortedMapWritable {
  /** the test */
  @Test
  @SuppressWarnings("unchecked")
  public void testSortedMapWritable() {
    Text[] keys = {
        new Text("key1"),
        new Text("key2"),
        new Text("key3"),
    };
    
    BytesWritable[] values = {
        new BytesWritable("value1".getBytes()),
        new BytesWritable("value2".getBytes()),
        new BytesWritable("value3".getBytes())
    };

    SortedMapWritable inMap = new SortedMapWritable();
    for (int i = 0; i < keys.length; i++) {
      inMap.put(keys[i], values[i]);
    }
    
    assertEquals(0, inMap.firstKey().compareTo(keys[0]));
    assertEquals(0, inMap.lastKey().compareTo(keys[2]));

    SortedMapWritable outMap = new SortedMapWritable(inMap);
    assertEquals(inMap.size(), outMap.size());
    
    for (Map.Entry<WritableComparable, Writable> e: inMap.entrySet()) {
      assertTrue(outMap.containsKey(e.getKey()));
      assertEquals(0, ((WritableComparable) outMap.get(e.getKey())).compareTo(
          e.getValue()));
    }
    
    // Now for something a little harder...
    
    Text[] maps = {
        new Text("map1"),
        new Text("map2")
    };
    
    SortedMapWritable mapOfMaps = new SortedMapWritable();
    mapOfMaps.put(maps[0], inMap);
    mapOfMaps.put(maps[1], outMap);
    
    SortedMapWritable copyOfMapOfMaps = new SortedMapWritable(mapOfMaps);
    for (int i = 0; i < maps.length; i++) {
      assertTrue(copyOfMapOfMaps.containsKey(maps[i]));

      SortedMapWritable a = (SortedMapWritable) mapOfMaps.get(maps[i]);
      SortedMapWritable b = (SortedMapWritable) copyOfMapOfMaps.get(maps[i]);
      assertEquals(a.size(), b.size());
      for (Writable key: a.keySet()) {
        assertTrue(b.containsKey(key));
        
        // This will work because we know what we put into each set
        
        WritableComparable aValue = (WritableComparable) a.get(key);
        WritableComparable bValue = (WritableComparable) b.get(key);
        assertEquals(0, aValue.compareTo(bValue));
      }
    }
  }
  
  /**
   * Test that number of "unknown" classes is propagated across multiple copies.
   */
  @Test
  @SuppressWarnings("deprecation")
  public void testForeignClass() {
    SortedMapWritable inMap = new SortedMapWritable();
    inMap.put(new Text("key"), new UTF8("value"));
    inMap.put(new Text("key2"), new UTF8("value2"));
    SortedMapWritable outMap = new SortedMapWritable(inMap);
    SortedMapWritable copyOfCopy = new SortedMapWritable(outMap);
    assertEquals(1, copyOfCopy.getNewClasses());
  }
  
  /**
   * Tests if equal and hashCode method still hold the contract.
   */
  @Test
  public void testEqualsAndHashCode() {
    String failureReason;
    SortedMapWritable mapA = new SortedMapWritable();
    SortedMapWritable mapB = new SortedMapWritable();
    
    // Sanity checks
    failureReason = "SortedMapWritable couldn't be initialized. Got null reference";
    assertNotNull(failureReason, mapA);
    assertNotNull(failureReason, mapB);
    
    // Basic null check
    assertFalse("equals method returns true when passed null", mapA.equals(null));
    
    // When entry set is empty, they should be equal
    assertTrue("Two empty SortedMapWritables are no longer equal", mapA.equals(mapB));
    
    // Setup
    Text[] keys = {
        new Text("key1"),
        new Text("key2")
    };
    
    BytesWritable[] values = {
        new BytesWritable("value1".getBytes()),
        new BytesWritable("value2".getBytes())
    };
    
    mapA.put(keys[0], values[0]);
    mapB.put(keys[1], values[1]);
    
    // entrySets are different
    failureReason = "Two SortedMapWritables with different data are now equal";
    assertTrue(failureReason, mapA.hashCode() != mapB.hashCode());
    assertTrue(failureReason, !mapA.equals(mapB));
    assertTrue(failureReason, !mapB.equals(mapA));
    
    mapA.put(keys[1], values[1]);
    mapB.put(keys[0], values[0]);
    
    // entrySets are now same
    failureReason = "Two SortedMapWritables with same entry sets formed in different order are now different";
    assertEquals(failureReason, mapA.hashCode(), mapB.hashCode());
    assertTrue(failureReason, mapA.equals(mapB));
    assertTrue(failureReason, mapB.equals(mapA));
    
    // Let's check if entry sets of same keys but different values
    mapA.put(keys[0], values[1]);
    mapA.put(keys[1], values[0]);
    
    failureReason = "Two SortedMapWritables with different content are now equal";
    assertTrue(failureReason, mapA.hashCode() != mapB.hashCode());
    assertTrue(failureReason, !mapA.equals(mapB));
    assertTrue(failureReason, !mapB.equals(mapA));
  }

  @Test(timeout = 1000)
  public void testPutAll() {
    SortedMapWritable map1 = new SortedMapWritable();
    SortedMapWritable map2 = new SortedMapWritable();
    map1.put(new Text("key"), new Text("value"));
    map2.putAll(map1);

    assertEquals("map1 entries don't match map2 entries", map1, map2);
    assertTrue(
        "map2 doesn't have class information from map1",
        map2.classToIdMap.containsKey(Text.class)
            && map2.idToClassMap.containsValue(Text.class));
  }
}
