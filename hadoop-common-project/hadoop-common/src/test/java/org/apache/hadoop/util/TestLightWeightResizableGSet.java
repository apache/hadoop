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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

/** Testing {@link LightWeightResizableGSet} */
public class TestLightWeightResizableGSet {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestLightWeightResizableGSet.class);
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

    assertThat(set.size()).isZero();

    // put all elements
    for (int i = 0; i < elements.length; i++) {
      TestElement element = set.put(elements[i]);
      assertThat(element).isNull();
    }

    // check the set size
    assertThat(set.size()).isEqualTo(elements.length);

    // check all elements exist in the set and the data is correct
    for (int i = 0; i < elements.length; i++) {
      assertThat(set.contains(elements[i])).isTrue();

      TestElement element = set.get(elements[i]);
      assertThat(elements[i].getData()).isEqualTo(element.getData());
    }

    TestKey[] keys = getKeys(elements);
    // generate new elements with same key, but new data
    TestElement[] newElements = generateElements(keys);
    // update the set
    for (int i = 0; i < newElements.length; i++) {
      TestElement element = set.put(newElements[i]);
      assertThat(element).isNotNull();
    }

    // check the set size
    assertThat(set.size()).isEqualTo(elements.length);

    // check all elements exist in the set and the data is updated to new value
    for (int i = 0; i < keys.length; i++) {
      assertThat(set.contains(keys[i])).isTrue();

      TestElement element = set.get(keys[i]);
      assertThat(newElements[i].getData()).isEqualTo(element.getData());
    }

    // test LightWeightHashGSet#values
    Collection<TestElement> cElements = set.values();
    assertThat(cElements.size()).isEqualTo(elements.length);
    for (TestElement element : cElements) {
      assertThat(set.contains(element)).isTrue();
    }

    // remove elements
    for (int i = 0; i < keys.length; i++) {
      TestElement element = set.remove(keys[i]);
      assertThat(element).isNotNull();

      // the element should not exist after remove
      assertThat(set.contains(keys[i])).isFalse();
    }

    // check the set size
    assertThat(set.size()).isZero();
  }

  @Test(timeout = 60000)
  public void testRemoveAll() {
    TestElement[] elements = generateElements(1 << 16);
    final LightWeightResizableGSet<TestKey, TestElement> set =
        new LightWeightResizableGSet<TestKey, TestElement>();

    assertThat(set.size()).isZero();

    // put all elements
    for (int i = 0; i < elements.length; i++) {
      TestElement element = set.put(elements[i]);
      assertThat(element).isNull();
    }

    // check the set size
    assertThat(set.size()).isEqualTo(elements.length);

    // remove all through clear
    {
      set.clear();
      assertThat(set.size()).isZero();

      // check all elements removed
      for (int i = 0; i < elements.length; i++) {
        assertThat(set.contains(elements[i])).isFalse();
      }
      assertThat(set.iterator().hasNext()).isFalse();
    }

    // put all elements back
    for (int i = 0; i < elements.length; i++) {
      TestElement element = set.put(elements[i]);
      assertThat(element).isNull();
    }

    // remove all through iterator
    {
      for (Iterator<TestElement> iter = set.iterator(); iter.hasNext(); ) {
        TestElement element = iter.next();
        // element should be there before removing
        assertThat(set.contains(element)).isTrue();
        iter.remove();
        // element should not be there now
        assertThat(set.contains(element)).isFalse();
      }

      // the deleted elements should not be there
      for (int i = 0; i < elements.length; i++) {
        assertThat(set.contains(elements[i])).isFalse();
      }

      // iterator should not have next
      assertThat(set.iterator().hasNext()).isFalse();

      // check the set size
      assertThat(set.size()).isZero();
    }
  }
}
