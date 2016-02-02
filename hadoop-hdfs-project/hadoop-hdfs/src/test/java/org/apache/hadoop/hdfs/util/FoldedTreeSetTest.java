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

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Random;

/**
 * Test of TreeSet
 */
public class FoldedTreeSetTest {

  private static Random srand;

  public FoldedTreeSetTest() {
  }

  @BeforeClass
  public static void setUpClass() {
    long seed = System.nanoTime();
    System.out.println("This run uses the random seed " + seed);
    srand = new Random(seed);
  }

  @AfterClass
  public static void tearDownClass() {
  }

  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {
  }

  /**
   * Test of comparator method, of class TreeSet.
   */
  @Test
  public void testComparator() {
    Comparator<String> comparator = new Comparator<String>() {

      @Override
      public int compare(String o1, String o2) {
        return o1.compareTo(o2);
      }
    };
    assertEquals(null, new FoldedTreeSet<>().comparator());
    assertEquals(comparator, new FoldedTreeSet<>(comparator).comparator());

    FoldedTreeSet<String> set = new FoldedTreeSet<>(comparator);
    set.add("apa3");
    set.add("apa2");
    set.add("apa");
    set.add("apa5");
    set.add("apa4");
    assertEquals(5, set.size());
    assertEquals("apa", set.get("apa"));
  }

  /**
   * Test of first method, of class TreeSet.
   */
  @Test
  public void testFirst() {
    FoldedTreeSet<Integer> tree = new FoldedTreeSet<>();
    for (int i = 0; i < 256; i++) {
      tree.add(1024 + i);
      assertEquals(1024, tree.first().intValue());
    }
    for (int i = 1; i < 256; i++) {
      tree.remove(1024 + i);
      assertEquals(1024, tree.first().intValue());
    }
  }

  /**
   * Test of last method, of class TreeSet.
   */
  @Test
  public void testLast() {
    FoldedTreeSet<Integer> tree = new FoldedTreeSet<>();
    for (int i = 0; i < 256; i++) {
      tree.add(1024 + i);
      assertEquals(1024 + i, tree.last().intValue());
    }
    for (int i = 0; i < 255; i++) {
      tree.remove(1024 + i);
      assertEquals(1279, tree.last().intValue());
    }
  }

  /**
   * Test of size method, of class TreeSet.
   */
  @Test
  public void testSize() {
    FoldedTreeSet<String> instance = new FoldedTreeSet<>();
    String entry = "apa";
    assertEquals(0, instance.size());
    instance.add(entry);
    assertEquals(1, instance.size());
    instance.remove(entry);
    assertEquals(0, instance.size());
  }

  /**
   * Test of isEmpty method, of class TreeSet.
   */
  @Test
  public void testIsEmpty() {
    FoldedTreeSet<String> instance = new FoldedTreeSet<>();
    boolean expResult = true;
    boolean result = instance.isEmpty();
    assertEquals(expResult, result);
    instance.add("apa");
    instance.remove("apa");
    assertEquals(expResult, result);
  }

  /**
   * Test of contains method, of class TreeSet.
   */
  @Test
  public void testContains() {
    FoldedTreeSet<String> instance = new FoldedTreeSet<>();
    String entry = "apa";
    assertEquals(false, instance.contains(entry));
    instance.add(entry);
    assertEquals(true, instance.contains(entry));
    assertEquals(false, instance.contains(entry + entry));
  }

  /**
   * Test of iterator method, of class TreeSet.
   */
  @Test
  public void testIterator() {

    for (int iter = 0; iter < 10; iter++) {
      FoldedTreeSet<Holder> set = new FoldedTreeSet<>();
      long[] longs = new long[64723];
      for (int i = 0; i < longs.length; i++) {
        Holder val = new Holder(srand.nextLong());
        while (set.contains(val)) {
          val = new Holder(srand.nextLong());
        }
        longs[i] = val.getId();
        set.add(val);
      }
      assertEquals(longs.length, set.size());
      Arrays.sort(longs);

      Iterator<Holder> it = set.iterator();
      for (int i = 0; i < longs.length; i++) {
        assertTrue(it.hasNext());
        Holder val = it.next();
        assertEquals(longs[i], val.getId());
        // remove randomly to force non linear removes
        if (srand.nextBoolean()) {
          it.remove();
        }
      }
    }
  }

  /**
   * Test of toArray method, of class TreeSet.
   */
  @Test
  public void testToArray() {
    FoldedTreeSet<Integer> tree = new FoldedTreeSet<>();
    ArrayList<Integer> list = new ArrayList<>(256);
    for (int i = 0; i < 256; i++) {
      list.add(1024 + i);
    }
    tree.addAll(list);
    assertArrayEquals(list.toArray(), tree.toArray());
  }

  /**
   * Test of toArray method, of class TreeSet.
   */
  @Test
  public void testToArray_GenericType() {
    FoldedTreeSet<Integer> tree = new FoldedTreeSet<>();
    ArrayList<Integer> list = new ArrayList<>(256);
    for (int i = 0; i < 256; i++) {
      list.add(1024 + i);
    }
    tree.addAll(list);
    assertArrayEquals(list.toArray(new Integer[tree.size()]), tree.toArray(new Integer[tree.size()]));
    assertArrayEquals(list.toArray(new Integer[tree.size() + 100]), tree.toArray(new Integer[tree.size() + 100]));
  }

  /**
   * Test of add method, of class TreeSet.
   */
  @Test
  public void testAdd() {
    FoldedTreeSet<String> simpleSet = new FoldedTreeSet<>();
    String entry = "apa";
    assertTrue(simpleSet.add(entry));
    assertFalse(simpleSet.add(entry));

    FoldedTreeSet<Integer> intSet = new FoldedTreeSet<>();
    for (int i = 512; i < 1024; i++) {
      assertTrue(intSet.add(i));
    }
    for (int i = -1024; i < -512; i++) {
      assertTrue(intSet.add(i));
    }
    for (int i = 0; i < 512; i++) {
      assertTrue(intSet.add(i));
    }
    for (int i = -512; i < 0; i++) {
      assertTrue(intSet.add(i));
    }
    assertEquals(2048, intSet.size());

    FoldedTreeSet<Holder> set = new FoldedTreeSet<>();
    long[] longs = new long[23432];
    for (int i = 0; i < longs.length; i++) {
      Holder val = new Holder(srand.nextLong());
      while (set.contains(val)) {
        val = new Holder(srand.nextLong());
      }
      longs[i] = val.getId();
      assertTrue(set.add(val));
    }
    assertEquals(longs.length, set.size());
    Arrays.sort(longs);

    Iterator<Holder> it = set.iterator();
    for (int i = 0; i < longs.length; i++) {
      assertTrue(it.hasNext());
      Holder val = it.next();
      assertEquals(longs[i], val.getId());
    }

    // Specially constructed adds to exercise all code paths
    FoldedTreeSet<Integer> specialAdds = new FoldedTreeSet<>();
    // Fill node with even numbers
    for (int i = 0; i < 128; i += 2) {
      assertTrue(specialAdds.add(i));
    }
    // Remove left and add left
    assertTrue(specialAdds.remove(0));
    assertTrue(specialAdds.add(-1));
    assertTrue(specialAdds.remove(-1));
    // Add right and shift everything left
    assertTrue(specialAdds.add(127));
    assertTrue(specialAdds.remove(127));

    // Empty at both ends
    assertTrue(specialAdds.add(0));
    assertTrue(specialAdds.remove(0));
    assertTrue(specialAdds.remove(126));
    // Add in the middle left to slide entries left
    assertTrue(specialAdds.add(11));
    assertTrue(specialAdds.remove(11));
    // Add in the middle right to slide entries right
    assertTrue(specialAdds.add(99));
    assertTrue(specialAdds.remove(99));
    // Add existing entry in the middle of a node
    assertFalse(specialAdds.add(64));
  }

  @Test
  public void testAddOrReplace() {
    FoldedTreeSet<String> simpleSet = new FoldedTreeSet<>();
    String entry = "apa";
    assertNull(simpleSet.addOrReplace(entry));
    assertEquals(entry, simpleSet.addOrReplace(entry));

    FoldedTreeSet<Integer> intSet = new FoldedTreeSet<>();
    for (int i = 0; i < 1024; i++) {
      assertNull(intSet.addOrReplace(i));
    }
    for (int i = 0; i < 1024; i++) {
      assertEquals(i, intSet.addOrReplace(i).intValue());
    }
  }

  private static class Holder implements Comparable<Holder> {

    private final long id;

    public Holder(long id) {
      this.id = id;
    }

    public long getId() {
      return id;
    }

    @Override
    public int compareTo(Holder o) {
      return id < o.getId() ? -1
             : id > o.getId() ? 1 : 0;
    }
  }

  @Test
  public void testRemoveWithComparator() {
    FoldedTreeSet<Holder> set = new FoldedTreeSet<>();
    long[] longs = new long[98327];
    for (int i = 0; i < longs.length; i++) {
      Holder val = new Holder(srand.nextLong());
      while (set.contains(val)) {
        val = new Holder(srand.nextLong());
      }
      longs[i] = val.getId();
      set.add(val);
    }
    assertEquals(longs.length, set.size());
    Comparator<Object> cmp = new Comparator<Object>() {
      @Override
      public int compare(Object o1, Object o2) {
        long lookup = (long) o1;
        long stored = ((Holder) o2).getId();
        return lookup < stored ? -1
               : lookup > stored ? 1 : 0;
      }
    };

    for (long val : longs) {
      set.remove(val, cmp);
    }
    assertEquals(0, set.size());
    assertTrue(set.isEmpty());
  }

  @Test
  public void testGetWithComparator() {
    FoldedTreeSet<Holder> set = new FoldedTreeSet<>();
    long[] longs = new long[32147];
    for (int i = 0; i < longs.length; i++) {
      Holder val = new Holder(srand.nextLong());
      while (set.contains(val)) {
        val = new Holder(srand.nextLong());
      }
      longs[i] = val.getId();
      set.add(val);
    }
    assertEquals(longs.length, set.size());
    Comparator<Object> cmp = new Comparator<Object>() {
      @Override
      public int compare(Object o1, Object o2) {
        long lookup = (long) o1;
        long stored = ((Holder) o2).getId();
        return lookup < stored ? -1
               : lookup > stored ? 1 : 0;
      }
    };

    for (long val : longs) {
      assertEquals(val, set.get(val, cmp).getId());
    }
  }

  @Test
  public void testGet() {
    FoldedTreeSet<Holder> set = new FoldedTreeSet<>();
    long[] longs = new long[43277];
    for (int i = 0; i < longs.length; i++) {
      Holder val = new Holder(srand.nextLong());
      while (set.contains(val)) {
        val = new Holder(srand.nextLong());
      }
      longs[i] = val.getId();
      set.add(val);
    }
    assertEquals(longs.length, set.size());

    for (long val : longs) {
      assertEquals(val, set.get(new Holder(val)).getId());
    }
  }

  /**
   * Test of remove method, of class TreeSet.
   */
  @Test
  public void testRemove() {
    FoldedTreeSet<String> instance = new FoldedTreeSet<>();
    assertEquals(false, instance.remove("apa"));
    instance.add("apa");
    assertEquals(true, instance.remove("apa"));

    removeLeft();
    removeRight();
    removeAt();
    removeRandom();
  }

  public void removeLeft() {
    FoldedTreeSet<Integer> set = new FoldedTreeSet<>();
    for (int i = 1; i <= 320; i++) {
      set.add(i);
    }
    for (int i = 193; i < 225; i++) {
      assertEquals(true, set.remove(i));
      assertEquals(false, set.remove(i));
    }
    for (int i = 129; i < 161; i++) {
      assertEquals(true, set.remove(i));
      assertEquals(false, set.remove(i));
    }
    for (int i = 256; i > 224; i--) {
      assertEquals(true, set.remove(i));
      assertEquals(false, set.remove(i));
    }
    for (int i = 257; i < 289; i++) {
      assertEquals(true, set.remove(i));
      assertEquals(false, set.remove(i));
    }
    while (!set.isEmpty()) {
      assertTrue(set.remove(set.first()));
    }
  }

  public void removeRight() {
    FoldedTreeSet<Integer> set = new FoldedTreeSet<>();
    for (int i = 1; i <= 320; i++) {
      set.add(i);
    }
    for (int i = 193; i < 225; i++) {
      assertEquals(true, set.remove(i));
      assertEquals(false, set.remove(i));
    }
    for (int i = 192; i > 160; i--) {
      assertEquals(true, set.remove(i));
      assertEquals(false, set.remove(i));
    }
    for (int i = 256; i > 224; i--) {
      assertEquals(true, set.remove(i));
      assertEquals(false, set.remove(i));
    }
    for (int i = 320; i > 288; i--) {
      assertEquals(true, set.remove(i));
      assertEquals(false, set.remove(i));
    }
    while (!set.isEmpty()) {
      assertTrue(set.remove(set.last()));
    }
  }

  public void removeAt() {
    FoldedTreeSet<Integer> set = new FoldedTreeSet<>();
    for (int i = 1; i <= 320; i++) {
      set.add(i);
    }
    for (int i = 193; i < 225; i++) {
      assertEquals(true, set.remove(i));
      assertEquals(false, set.remove(i));
    }
    for (int i = 160; i < 192; i++) {
      assertEquals(true, set.remove(i));
      assertEquals(false, set.remove(i));
    }
    for (int i = 225; i < 257; i++) {
      assertEquals(true, set.remove(i));
      assertEquals(false, set.remove(i));
    }
    for (int i = 288; i < 320; i++) {
      assertEquals(true, set.remove(i));
      assertEquals(false, set.remove(i));
    }
  }

  public void removeRandom() {
    FoldedTreeSet<Integer> set = new FoldedTreeSet<>();
    int[] integers = new int[2048];
    for (int i = 0; i < 2048; i++) {
      int val = srand.nextInt();
      while (set.contains(val)) {
        val = srand.nextInt();
      }
      integers[i] = val;
      set.add(val);
    }
    assertEquals(2048, set.size());

    for (int val : integers) {
      assertEquals(true, set.remove(val));
      assertEquals(false, set.remove(val));
    }
    assertEquals(true, set.isEmpty());
  }

  /**
   * Test of containsAll method, of class TreeSet.
   */
  @Test
  public void testContainsAll() {
    Collection<String> list = Arrays.asList(new String[]{"apa", "apa2", "apa"});
    FoldedTreeSet<String> instance = new FoldedTreeSet<>();
    assertEquals(false, instance.containsAll(list));
    instance.addAll(list);
    assertEquals(true, instance.containsAll(list));
  }

  /**
   * Test of addAll method, of class TreeSet.
   */
  @Test
  public void testAddAll() {
    Collection<String> list = Arrays.asList(new String[]{"apa", "apa2", "apa"});
    FoldedTreeSet<String> instance = new FoldedTreeSet<>();
    assertEquals(true, instance.addAll(list));
    assertEquals(false, instance.addAll(list)); // add same entries again
  }

  /**
   * Test of retainAll method, of class TreeSet.
   */
  @Test
  public void testRetainAll() {
    Collection<String> list = Arrays.asList(new String[]{"apa", "apa2", "apa"});
    FoldedTreeSet<String> instance = new FoldedTreeSet<>();
    instance.addAll(list);
    assertEquals(false, instance.retainAll(list));
    assertEquals(2, instance.size());
    Collection<String> list2 = Arrays.asList(new String[]{"apa"});
    assertEquals(true, instance.retainAll(list2));
    assertEquals(1, instance.size());
  }

  /**
   * Test of removeAll method, of class TreeSet.
   */
  @Test
  public void testRemoveAll() {
    Collection<String> list = Arrays.asList(new String[]{"apa", "apa2", "apa"});
    FoldedTreeSet<String> instance = new FoldedTreeSet<>();
    assertEquals(false, instance.removeAll(list));
    instance.addAll(list);
    assertEquals(true, instance.removeAll(list));
    assertEquals(true, instance.isEmpty());
  }

  /**
   * Test of clear method, of class TreeSet.
   */
  @Test
  public void testClear() {
    FoldedTreeSet<String> instance = new FoldedTreeSet<>();
    instance.clear();
    assertEquals(true, instance.isEmpty());
    instance.add("apa");
    assertEquals(false, instance.isEmpty());
    instance.clear();
    assertEquals(true, instance.isEmpty());
  }

  @Test
  public void testFillRatio() {
    FoldedTreeSet<Integer> set = new FoldedTreeSet<>();
    final int size = 1024;
    for (int i = 1; i <= size; i++) {
      set.add(i);
      assertEquals("Iteration: " + i, 1.0, set.fillRatio(), 0.0);
    }

    for (int i = 1; i <= size / 2; i++) {
      set.remove(i * 2);
      // Need the max since all the removes from the last node doesn't
      // affect the fill ratio
      assertEquals("Iteration: " + i,
                   Math.max((size - i) / (double) size, 0.53125),
                   set.fillRatio(), 0.0);
    }
  }

  @Test
  public void testCompact() {
    FoldedTreeSet<Holder> set = new FoldedTreeSet<>();
    long[] longs = new long[24553];
    for (int i = 0; i < longs.length; i++) {
      Holder val = new Holder(srand.nextLong());
      while (set.contains(val)) {
        val = new Holder(srand.nextLong());
      }
      longs[i] = val.getId();
      set.add(val);
    }
    assertEquals(longs.length, set.size());

    long[] longs2 = new long[longs.length];
    for (int i = 0; i < longs2.length; i++) {
      Holder val = new Holder(srand.nextLong());
      while (set.contains(val)) {
        val = new Holder(srand.nextLong());
      }
      longs2[i] = val.getId();
      set.add(val);
    }
    assertEquals(longs.length + longs2.length, set.size());

    // Create fragementation
    for (long val : longs) {
      assertTrue(set.remove(new Holder(val)));
    }
    assertEquals(longs2.length, set.size());

    assertFalse(set.compact(0));
    assertTrue(set.compact(Long.MAX_VALUE));
    assertEquals(longs2.length, set.size());
    for (long val : longs) {
      assertFalse(set.remove(new Holder(val)));
    }
    for (long val : longs2) {
      assertEquals(val, set.get(new Holder(val)).getId());
    }
  }
}
