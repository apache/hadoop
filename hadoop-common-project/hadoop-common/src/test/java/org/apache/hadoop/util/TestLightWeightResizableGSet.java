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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import static org.junit.Assert.*;

/** Testing {@link LightWeightResizableGSet} */
public class TestLightWeightResizableGSet {
  public static final Log LOG = LogFactory.getLog(TestLightWeightResizableGSet.class);
  private Random random = new Random();

  private TestElement[] generateElements(int length) {
    TestElement[] elements = new TestElement[length];
    Set<Long> keys = new HashSet<>();
    long k = 0;
    for (int i = 0; i < length; i++) {
      while (keys.contains(k = random.nextLong()));
      elements[i] = new TestElement(k, random.nextLong());
      keys.add(k);
    }
    return elements;
  }

  private TestKey[] getKeys(TestElement[] elements) {
    TestKey[] keys = new TestKey[elements.length];
    for (int i = 0; i < elements.length; i++) {
      keys[i] = new TestKey(elements[i].getKey());
    }
    return keys;
  }

  private TestElement[] generateElements(TestKey[] keys) {
    TestElement[] elements = new TestElement[keys.length];
    for (int i = 0; i < keys.length; i++) {
      elements[i] = new TestElement(keys[i], random.nextLong());
    }
    return elements;
  }

  private static class TestKey {
    private final long key;

    TestKey(long key) {
      this.key = key;
    }

    TestKey(TestKey other) {
      this.key = other.key;
    }

    long getKey() {
      return key;
    }

    @Override
    public int hashCode() {
      return (int)(key^(key>>>32));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof TestKey)) {
        return false;
      }
      TestKey other = (TestKey)o;
      return key == other.key;
    }
  }

  private static class TestElement extends TestKey
      implements LightWeightResizableGSet.LinkedElement {
    private final long data;
    private LightWeightResizableGSet.LinkedElement next;

    TestElement(long key, long data) {
      super(key);
      this.data = data;
    }

    TestElement(TestKey key, long data) {
      super(key);
      this.data = data;
    }

    long getData() {
      return data;
    }

    @Override
    public void setNext(LightWeightResizableGSet.LinkedElement next) {
      this.next = next;
    }

    @Override
    public LightWeightResizableGSet.LinkedElement getNext() {
      return next;
    }
  }

  @Test(timeout = 60000)
  public void testBasicOperations() {
    TestElement[] elements = generateElements(1 << 16);
    final LightWeightResizableGSet<TestKey, TestElement> set =
        new LightWeightResizableGSet<TestKey, TestElement>();

    assertEquals(set.size(), 0);

    // put all elements
    for (int i = 0; i < elements.length; i++) {
      TestElement element = set.put(elements[i]);
      assertTrue(element == null);
    }

    // check the set size
    assertEquals(set.size(), elements.length);

    // check all elements exist in the set and the data is correct
    for (int i = 0; i < elements.length; i++) {
      assertTrue(set.contains(elements[i]));

      TestElement element = set.get(elements[i]);
      assertEquals(elements[i].getData(), element.getData());
    }

    TestKey[] keys = getKeys(elements);
    // generate new elements with same key, but new data
    TestElement[] newElements = generateElements(keys);
    // update the set
    for (int i = 0; i < newElements.length; i++) {
      TestElement element = set.put(newElements[i]);
      assertTrue(element != null);
    }

    // check the set size
    assertEquals(set.size(), elements.length);

    // check all elements exist in the set and the data is updated to new value
    for (int i = 0; i < keys.length; i++) {
      assertTrue(set.contains(keys[i]));

      TestElement element = set.get(keys[i]);
      assertEquals(newElements[i].getData(), element.getData());
    }

    // test LightWeightHashGSet#values
    Collection<TestElement> cElements = set.values();
    assertEquals(cElements.size(), elements.length);
    for (TestElement element : cElements) {
      assertTrue(set.contains(element));
    }

    // remove elements
    for (int i = 0; i < keys.length; i++) {
      TestElement element = set.remove(keys[i]);

      assertTrue(element != null);

      // the element should not exist after remove
      assertFalse(set.contains(keys[i]));
    }

    // check the set size
    assertEquals(set.size(), 0);
  }

  @Test(timeout = 60000)
  public void testRemoveAll() {
    TestElement[] elements = generateElements(1 << 16);
    final LightWeightResizableGSet<TestKey, TestElement> set =
        new LightWeightResizableGSet<TestKey, TestElement>();

    assertEquals(set.size(), 0);

    // put all elements
    for (int i = 0; i < elements.length; i++) {
      TestElement element = set.put(elements[i]);
      assertTrue(element == null);
    }

    // check the set size
    assertEquals(set.size(), elements.length);

    // remove all through clear
    {
      set.clear();
      assertEquals(set.size(), 0);

      // check all elements removed
      for (int i = 0; i < elements.length; i++) {
        assertFalse(set.contains(elements[i]));
      }
      assertFalse(set.iterator().hasNext());
    }

    // put all elements back
    for (int i = 0; i < elements.length; i++) {
      TestElement element = set.put(elements[i]);
      assertTrue(element == null);
    }

    // remove all through iterator
    {
      for (Iterator<TestElement> iter = set.iterator(); iter.hasNext(); ) {
        TestElement element = iter.next();
        // element should be there before removing
        assertTrue(set.contains(element));
        iter.remove();
        // element should not be there now
        assertFalse(set.contains(element));
      }

      // the deleted elements should not be there
      for (int i = 0; i < elements.length; i++) {
        assertFalse(set.contains(elements[i]));
      }

      // iterator should not have next
      assertFalse(set.iterator().hasNext());

      // check the set size
      assertEquals(set.size(), 0);
    }
  }
}
