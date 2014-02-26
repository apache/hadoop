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
package org.apache.hadoop.fs;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestVolumeId {

  @Test
  public void testEquality() {
    final VolumeId id1 = new HdfsVolumeId(new byte[] { (byte)0, (byte)0 });
    testEq(true, id1, id1);

    final VolumeId id2 = new HdfsVolumeId(new byte[] { (byte)0, (byte)1 });
    testEq(true, id2, id2);
    testEq(false, id1, id2);

    final VolumeId id3 = new HdfsVolumeId(new byte[] { (byte)1, (byte)0 });
    testEq(true, id3, id3);
    testEq(false, id1, id3);
    
    // same as 2, but "invalid":
    final VolumeId id2copy1 = new HdfsVolumeId(new byte[] { (byte)0, (byte)1 });
    
    testEq(true, id2, id2copy1);

    // same as 2copy1: 
    final VolumeId id2copy2 = new HdfsVolumeId(new byte[] { (byte)0, (byte)1 });
    
    testEq(true, id2, id2copy2);
    
    testEqMany(true, new VolumeId[] { id2, id2copy1, id2copy2 });
    
    testEqMany(false, new VolumeId[] { id1, id2, id3 });
  }
  
  @SuppressWarnings("unchecked")
  private <T> void testEq(final boolean eq, Comparable<T> id1, Comparable<T> id2) {
    final int h1 = id1.hashCode();
    final int h2 = id2.hashCode();
    
    // eq reflectivity:
    assertTrue(id1.equals(id1));
    assertTrue(id2.equals(id2));
    assertEquals(0, id1.compareTo((T)id1));
    assertEquals(0, id2.compareTo((T)id2));

    // eq symmetry:
    assertEquals(eq, id1.equals(id2));
    assertEquals(eq, id2.equals(id1));
    
    // null comparison:
    assertFalse(id1.equals(null));
    assertFalse(id2.equals(null));
    
    // compareTo:
    assertEquals(eq, 0 == id1.compareTo((T)id2));
    assertEquals(eq, 0 == id2.compareTo((T)id1));
    // compareTo must be antisymmetric:
    assertEquals(sign(id1.compareTo((T)id2)), -sign(id2.compareTo((T)id1)));
    
    // compare with null should never return 0 to be consistent with #equals(): 
    assertTrue(id1.compareTo(null) != 0);
    assertTrue(id2.compareTo(null) != 0);
    
    // check that hash codes did not change:
    assertEquals(h1, id1.hashCode());
    assertEquals(h2, id2.hashCode());
    if (eq) {
      // in this case the hash codes must be the same:
      assertEquals(h1, h2);
    }
  }
  
  private static int sign(int x) {
    if (x == 0) {
      return 0;
    } else if (x > 0) {
      return 1;
    } else {
      return -1;
    }
  }
  
  @SuppressWarnings("unchecked")
  private <T> void testEqMany(final boolean eq, Comparable<T>... volumeIds) {
    Comparable<T> vidNext;
    int sum = 0;
    for (int i=0; i<volumeIds.length; i++) {
      if (i == volumeIds.length - 1) {
        vidNext = volumeIds[0];
      } else {
        vidNext = volumeIds[i + 1];
      }
      testEq(eq, volumeIds[i], vidNext);
      sum += sign(volumeIds[i].compareTo((T)vidNext));
    }
    // the comparison relationship must always be acyclic:
    assertTrue(sum < volumeIds.length);
  }

  /*
   * Test HdfsVolumeId(new byte[0]) instances: show that we permit such
   * objects, they are still valid, and obey the same equality
   * rules other objects do. 
   */
  @Test
  public void testIdEmptyBytes() {
    final VolumeId idEmpty1   = new HdfsVolumeId(new byte[0]);
    final VolumeId idEmpty2   = new HdfsVolumeId(new byte[0]);
    final VolumeId idNotEmpty = new HdfsVolumeId(new byte[] { (byte)1 });
    
    testEq(true, idEmpty1, idEmpty2);
    testEq(false, idEmpty1, idNotEmpty);
    testEq(false, idEmpty2, idNotEmpty);
  }

  /*
   * test #toString() for typical VolumeId equality classes
   */
  @Test
  public void testToString() {
    String strEmpty = new HdfsVolumeId(new byte[] {}).toString();
    assertNotNull(strEmpty);
    
    String strNotEmpty = new HdfsVolumeId(new byte[] { (byte)1 }).toString();
    assertNotNull(strNotEmpty);
  } 
  
}
