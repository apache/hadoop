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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.util.Time;
import org.junit.Before;
import org.junit.Test;

public class TestLightWeightHashSet{

  private static final Logger LOG = LoggerFactory
      .getLogger("org.apache.hadoop.hdfs.TestLightWeightHashSet");
  private final ArrayList<Integer> list = new ArrayList<Integer>();
  private final int NUM = 100;
  private LightWeightHashSet<Integer> set;
  private Random rand;

  @Before
  public void setUp() {
    float maxF = LightWeightHashSet.DEFAULT_MAX_LOAD_FACTOR;
    float minF = LightWeightHashSet.DEFAUT_MIN_LOAD_FACTOR;
    int initCapacity = LightWeightHashSet.MINIMUM_CAPACITY;
    rand = new Random(Time.now());
    list.clear();
    for (int i = 0; i < NUM; i++) {
      list.add(rand.nextInt());
    }
    set = new LightWeightHashSet<Integer>(initCapacity, maxF, minF);
  }

  @Test
  public void testEmptyBasic() {
    LOG.info("Test empty basic");
    Iterator<Integer> iter = set.iterator();
    // iterator should not have next
    assertFalse(iter.hasNext());
    assertEquals(0, set.size());
    assertTrue(set.isEmpty());
    LOG.info("Test empty - DONE");
  }

  @Test
  public void testOneElementBasic() {
    LOG.info("Test one element basic");
    set.add(list.get(0));
    // set should be non-empty
    assertEquals(1, set.size());
    assertFalse(set.isEmpty());

    // iterator should have next
    Iterator<Integer> iter = set.iterator();
    assertTrue(iter.hasNext());

    // iterator should not have next
    assertEquals(list.get(0), iter.next());
    assertFalse(iter.hasNext());
    LOG.info("Test one element basic - DONE");
  }

  @Test
  public void testMultiBasic() {
    LOG.info("Test multi element basic");
    // add once
    for (Integer i : list) {
      assertTrue(set.add(i));
    }
    assertEquals(list.size(), set.size());

    // check if the elements are in the set
    for (Integer i : list) {
      assertTrue(set.contains(i));
    }

    // add again - should return false each time
    for (Integer i : list) {
      assertFalse(set.add(i));
    }

    // check again if the elements are there
    for (Integer i : list) {
      assertTrue(set.contains(i));
    }

    Iterator<Integer> iter = set.iterator();
    int num = 0;
    while (iter.hasNext()) {
      Integer next = iter.next();
      assertNotNull(next);
      assertTrue(list.contains(next));
      num++;
    }
    // check the number of element from the iterator
    assertEquals(list.size(), num);
    LOG.info("Test multi element basic - DONE");
  }

  @Test
  public void testRemoveOne() {
    LOG.info("Test remove one");
    assertTrue(set.add(list.get(0)));
    assertEquals(1, set.size());

    // remove from the head/tail
    assertTrue(set.remove(list.get(0)));
    assertEquals(0, set.size());

    // check the iterator
    Iterator<Integer> iter = set.iterator();
    assertFalse(iter.hasNext());

    // add the element back to the set
    assertTrue(set.add(list.get(0)));
    assertEquals(1, set.size());

    iter = set.iterator();
    assertTrue(iter.hasNext());
    LOG.info("Test remove one - DONE");
  }

  @Test
  public void testRemoveMulti() {
    LOG.info("Test remove multi");
    for (Integer i : list) {
      assertTrue(set.add(i));
    }
    for (int i = 0; i < NUM / 2; i++) {
      assertTrue(set.remove(list.get(i)));
    }

    // the deleted elements should not be there
    for (int i = 0; i < NUM / 2; i++) {
      assertFalse(set.contains(list.get(i)));
    }

    // the rest should be there
    for (int i = NUM / 2; i < NUM; i++) {
      assertTrue(set.contains(list.get(i)));
    }
    LOG.info("Test remove multi - DONE");
  }

  @Test
  public void testRemoveAll() {
    LOG.info("Test remove all");
    for (Integer i : list) {
      assertTrue(set.add(i));
    }
    for (int i = 0; i < NUM; i++) {
      assertTrue(set.remove(list.get(i)));
    }
    // the deleted elements should not be there
    for (int i = 0; i < NUM; i++) {
      assertFalse(set.contains(list.get(i)));
    }

    // iterator should not have next
    Iterator<Integer> iter = set.iterator();
    assertFalse(iter.hasNext());
    assertTrue(set.isEmpty());
    LOG.info("Test remove all - DONE");
  }

  @Test
  public void testRemoveAllViaIterator() {
    LOG.info("Test remove all via iterator");
    for (Integer i : list) {
      assertTrue(set.add(i));
    }
    for (Iterator<Integer> iter = set.iterator(); iter.hasNext(); ) {
      int e = iter.next();
      // element should be there before removing
      assertTrue(set.contains(e));
      iter.remove();
      // element should not be there now
      assertFalse(set.contains(e));
    }

    // the deleted elements should not be there
    for (int i = 0; i < NUM; i++) {
      assertFalse(set.contains(list.get(i)));
    }

    // iterator should not have next
    Iterator<Integer> iter = set.iterator();
    assertFalse(iter.hasNext());
    assertTrue(set.isEmpty());
    LOG.info("Test remove all via iterator - DONE");
  }

  @Test
  public void testPollAll() {
    LOG.info("Test poll all");
    for (Integer i : list) {
      assertTrue(set.add(i));
    }
    // remove all elements by polling
    List<Integer> poll = set.pollAll();
    assertEquals(0, set.size());
    assertTrue(set.isEmpty());

    // the deleted elements should not be there
    for (int i = 0; i < NUM; i++) {
      assertFalse(set.contains(list.get(i)));
    }

    // we should get all original items
    for (Integer i : poll) {
      assertTrue(list.contains(i));
    }

    Iterator<Integer> iter = set.iterator();
    assertFalse(iter.hasNext());
    LOG.info("Test poll all - DONE");
  }

  @Test
  public void testPollNMulti() {
    LOG.info("Test pollN multi");

    // use addAll
    set.addAll(list);

    // poll zero
    List<Integer> poll = set.pollN(0);
    assertEquals(0, poll.size());
    for (Integer i : list) {
      assertTrue(set.contains(i));
    }

    // poll existing elements (less than size)
    poll = set.pollN(10);
    assertEquals(10, poll.size());

    for (Integer i : poll) {
      // should be in original items
      assertTrue(list.contains(i));
      // should not be in the set anymore
      assertFalse(set.contains(i));
    }

    // poll more elements than present
    poll = set.pollN(1000);
    assertEquals(NUM - 10, poll.size());

    for (Integer i : poll) {
      // should be in original items
      assertTrue(list.contains(i));
    }

    // set is empty
    assertTrue(set.isEmpty());
    assertEquals(0, set.size());

    LOG.info("Test pollN multi - DONE");
  }

  @Test
  public void testPollNMultiArray() {
    LOG.info("Test pollN multi array");

    // use addAll
    set.addAll(list);

    // poll existing elements (less than size)
    Integer[] poll = new Integer[10];
    poll = set.pollToArray(poll);
    assertEquals(10, poll.length);

    for (Integer i : poll) {
      // should be in original items
      assertTrue(list.contains(i));
      // should not be in the set anymore
      assertFalse(set.contains(i));
    }

    // poll other elements (more than size)
    poll = new Integer[NUM];
    poll = set.pollToArray(poll);
    assertEquals(NUM - 10, poll.length);

    for (int i = 0; i < NUM - 10; i++) {
      assertTrue(list.contains(poll[i]));
    }

    // set is empty
    assertTrue(set.isEmpty());
    assertEquals(0, set.size());

    // //////
    set.addAll(list);
    // poll existing elements (exactly the size)
    poll = new Integer[NUM];
    poll = set.pollToArray(poll);
    assertTrue(set.isEmpty());
    assertEquals(0, set.size());
    assertEquals(NUM, poll.length);
    for (int i = 0; i < NUM; i++) {
      assertTrue(list.contains(poll[i]));
    }
    // //////

    // //////
    set.addAll(list);
    // poll existing elements (exactly the size)
    poll = new Integer[0];
    poll = set.pollToArray(poll);
    for (int i = 0; i < NUM; i++) {
      assertTrue(set.contains(list.get(i)));
    }
    assertEquals(0, poll.length);
    // //////

    LOG.info("Test pollN multi array- DONE");
  }

  @Test
  public void testClear() {
    LOG.info("Test clear");
    // use addAll
    set.addAll(list);
    assertEquals(NUM, set.size());
    assertFalse(set.isEmpty());

    // clear the set
    set.clear();
    assertEquals(0, set.size());
    assertTrue(set.isEmpty());

    // iterator should be empty
    Iterator<Integer> iter = set.iterator();
    assertFalse(iter.hasNext());

    LOG.info("Test clear - DONE");
  }

  @Test
  public void testCapacity() {
    LOG.info("Test capacity");
    float maxF = LightWeightHashSet.DEFAULT_MAX_LOAD_FACTOR;
    float minF = LightWeightHashSet.DEFAUT_MIN_LOAD_FACTOR;

    // capacity lower than min_capacity
    set = new LightWeightHashSet<Integer>(1, maxF, minF);
    assertEquals(LightWeightHashSet.MINIMUM_CAPACITY, set.getCapacity());

    // capacity not a power of two
    set = new LightWeightHashSet<Integer>(30, maxF, minF);
    assertEquals(Math.max(LightWeightHashSet.MINIMUM_CAPACITY, 32),
        set.getCapacity());

    // capacity valid
    set = new LightWeightHashSet<Integer>(64, maxF, minF);
    assertEquals(Math.max(LightWeightHashSet.MINIMUM_CAPACITY, 64),
        set.getCapacity());

    // add NUM elements
    set.addAll(list);
    int expCap = LightWeightHashSet.MINIMUM_CAPACITY;
    while (expCap < NUM && maxF * expCap < NUM)
      expCap <<= 1;
    assertEquals(expCap, set.getCapacity());

    // see if the set shrinks if we remove elements by removing
    set.clear();
    set.addAll(list);
    int toRemove = set.size() - (int) (set.getCapacity() * minF) + 1;
    for (int i = 0; i < toRemove; i++) {
      set.remove(list.get(i));
    }
    assertEquals(Math.max(LightWeightHashSet.MINIMUM_CAPACITY, expCap / 2),
        set.getCapacity());

    LOG.info("Test capacity - DONE");
  }

  @Test
  public void testOther() {
    LOG.info("Test other");

    // remove all
    assertTrue(set.addAll(list));
    assertTrue(set.removeAll(list));
    assertTrue(set.isEmpty());

    // remove sublist
    List<Integer> sub = new LinkedList<Integer>();
    for (int i = 0; i < 10; i++) {
      sub.add(list.get(i));
    }
    assertTrue(set.addAll(list));
    assertTrue(set.removeAll(sub));
    assertFalse(set.isEmpty());
    assertEquals(NUM - 10, set.size());

    for (Integer i : sub) {
      assertFalse(set.contains(i));
    }

    assertFalse(set.containsAll(sub));

    // the rest of the elements should be there
    List<Integer> sub2 = new LinkedList<Integer>();
    for (int i = 10; i < NUM; i++) {
      sub2.add(list.get(i));
    }
    assertTrue(set.containsAll(sub2));

    // to array
    Integer[] array = set.toArray(new Integer[0]);
    assertEquals(NUM - 10, array.length);
    for (int i = 0; i < array.length; i++) {
      assertTrue(sub2.contains(array[i]));
    }
    assertEquals(NUM - 10, set.size());

    // to array
    Object[] array2 = set.toArray();
    assertEquals(NUM - 10, array2.length);

    for (int i = 0; i < array2.length; i++) {
      assertTrue(sub2.contains(array2[i]));
    }

    LOG.info("Test other - DONE");
  }
  
  @Test
  public void testGetElement() {
    LightWeightHashSet<TestObject> objSet = new LightWeightHashSet<TestObject>();
    TestObject objA = new TestObject("object A");
    TestObject equalToObjA = new TestObject("object A");
    TestObject objB = new TestObject("object B");
    objSet.add(objA);
    objSet.add(objB);
    
    assertSame(objA, objSet.getElement(objA));
    assertSame(objA, objSet.getElement(equalToObjA));
    assertSame(objB, objSet.getElement(objB));
    assertNull(objSet.getElement(new TestObject("not in set")));
  }
  
  /**
   * Wrapper class which is used in
   * {@link TestLightWeightHashSet#testGetElement()}
   */
  private static class TestObject {
    private final String value;

    public TestObject(String value) {
      super();
      this.value = value;
    }

    @Override
    public int hashCode() {
      return value.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass())
        return false;
      TestObject other = (TestObject) obj;
      return this.value.equals(other.value);
    }
  }

}
