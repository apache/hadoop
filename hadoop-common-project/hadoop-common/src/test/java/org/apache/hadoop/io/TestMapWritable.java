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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

/**
 * Tests MapWritable
 */
public class TestMapWritable {
  /** the test */
  @SuppressWarnings("unchecked")
  @Test
  public void testMapWritable() {
    Text[] keys = {
        new Text("key1"),
        new Text("key2"),
        new Text("Key3"),
    };
    
    BytesWritable[] values = {
        new BytesWritable("value1".getBytes()),
        new BytesWritable("value2".getBytes()),
        new BytesWritable("value3".getBytes())
    };

    MapWritable inMap = new MapWritable();
    for (int i = 0; i < keys.length; i++) {
      inMap.put(keys[i], values[i]);
    }

    MapWritable outMap = new MapWritable(inMap);
    assertEquals(inMap.size(), outMap.size());
    
    for (Map.Entry<Writable, Writable> e: inMap.entrySet()) {
      assertTrue(outMap.containsKey(e.getKey()));
      assertEquals(0, ((WritableComparable) outMap.get(e.getKey())).compareTo(
          e.getValue()));
    }
    
    // Now for something a little harder...
    
    Text[] maps = {
        new Text("map1"),
        new Text("map2")
    };
    
    MapWritable mapOfMaps = new MapWritable();
    mapOfMaps.put(maps[0], inMap);
    mapOfMaps.put(maps[1], outMap);
    
    MapWritable copyOfMapOfMaps = new MapWritable(mapOfMaps);
    for (int i = 0; i < maps.length; i++) {
      assertTrue(copyOfMapOfMaps.containsKey(maps[i]));
      MapWritable a = (MapWritable) mapOfMaps.get(maps[i]);
      MapWritable b = (MapWritable) copyOfMapOfMaps.get(maps[i]);
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
  @SuppressWarnings("deprecation")
  @Test
  public void testForeignClass() {
    MapWritable inMap = new MapWritable();
    inMap.put(new Text("key"), new UTF8("value"));
    inMap.put(new Text("key2"), new UTF8("value2"));
    MapWritable outMap = new MapWritable(inMap);
    MapWritable copyOfCopy = new MapWritable(outMap);
    assertEquals(1, copyOfCopy.getNewClasses());
  }
  
  /**
   * Assert MapWritable does not grow across calls to readFields.
   * @throws Exception
   * @see <a href="https://issues.apache.org/jira/browse/HADOOP-2244">HADOOP-2244</a>
   */
  @Test
  public void testMultipleCallsToReadFieldsAreSafe() throws Exception {
    // Create an instance and add a key/value.
    MapWritable m = new MapWritable();
    final Text t = new Text("testMultipleCallsToReadFieldsAreSafe");
    m.put(t, t);
    // Get current size of map.  Key values are 't'.
    int count = m.size();
    // Now serialize... save off the bytes.
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    m.write(dos);
    dos.close();
    // Now add new values to the MapWritable.
    m.put(new Text("key1"), new Text("value1"));
    m.put(new Text("key2"), new Text("value2"));
    // Now deserialize the original MapWritable.  Ensure count and key values
    // match original state.
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);
    m.readFields(dis);
    assertEquals(count, m.size());
    assertTrue(m.get(t).equals(t));
    dis.close();
  }

  @Test
  public void testEquality() {
    MapWritable map1 = new MapWritable();
    MapWritable map2 = new MapWritable();
    MapWritable map3 = new MapWritable();
    final IntWritable k1 = new IntWritable(5);
    final IntWritable k2 = new IntWritable(10);
    final Text value = new Text("value");

    map1.put(k1, value); // equal
    map2.put(k1, value); // equal
    map3.put(k2, value); // not equal

    assertTrue(map1.equals(map2));
    assertTrue(map2.equals(map1));
    assertFalse(map1.equals(map3));

    assertEquals(map1.hashCode(), map2.hashCode());
    assertFalse(map1.hashCode() == map3.hashCode());
  }

  /** Verify text command outputs a useful representation for MapWritable. */
  @Test
  public void testToString() {
    MapWritable map = new MapWritable();
    final IntWritable key = new IntWritable(5);
    final Text value = new Text("value");
    map.put(key, value);
    assertEquals("{5=value}", map.toString());
  }
}
