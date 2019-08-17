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

/**
 * <pre>
 * Story 1
 * As a software developer,
 *  I want to use the IntrusiveCollection class;
 * So that I can save on memory usage during execution.
 * </pre>
 */
package org.apache.hadoop.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.junit.Test;

import org.apache.hadoop.test.HadoopTestBase;
import org.apache.hadoop.util.IntrusiveCollection.Element;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestIntrusiveCollection extends HadoopTestBase {
  static class SimpleElement implements IntrusiveCollection.Element {
    private Map<IntrusiveCollection<? extends Element>, Element>
        prevMap, nextMap;
    private Map<IntrusiveCollection<? extends Element>, Boolean> isMemberMap;

    public SimpleElement() {
      prevMap = new HashMap<>();
      nextMap = new HashMap<>();
      isMemberMap = new HashMap<>();
    }

    @Override
    public void insertInternal(IntrusiveCollection<? extends Element> list,
        Element prev, Element next) {
      isMemberMap.put(list, true);
      prevMap.put(list, prev);
      nextMap.put(list, next);
    }

    @Override
    public void setPrev(IntrusiveCollection<? extends Element> list,
        Element prev) {
      prevMap.put(list, prev);
    }

    @Override
    public void setNext(IntrusiveCollection<? extends Element> list,
        Element next) {
      nextMap.put(list, next);
    }

    @Override
    public void removeInternal(IntrusiveCollection<? extends Element> list) {
      prevMap.remove(list);
      nextMap.remove(list);
      isMemberMap.remove(list);
    }

    @Override
    public Element getPrev(IntrusiveCollection<? extends Element> list) {
      return prevMap.getOrDefault(list, null);
    }

    @Override
    public Element getNext(IntrusiveCollection<? extends Element> list) {
      return nextMap.getOrDefault(list, null);
    }

    @Override
    public boolean isInList(IntrusiveCollection<? extends Element> list) {
      return isMemberMap.getOrDefault(list, false);
    }
  }

  /**
   * <pre>
   * Scenario S1.1: Adding an element
   * Given  an IntrusiveCollection has been created
   *  and    the IntrusiveCollection is empty
   * When    I insert an element
   * Then    the IntrusiveCollection contains the newly added element.
   * </pre>
   */
  @Test
  public void testShouldAddElement() {
    IntrusiveCollection<SimpleElement> intrusiveCollection =
      new IntrusiveCollection<>();

    SimpleElement element = new SimpleElement();
    intrusiveCollection.add(element);

    assertFalse("Collection should not be empty",
        intrusiveCollection.isEmpty());
    assertTrue("Collection should contain added element",
        intrusiveCollection.contains(element));
  }

  /**
   * <pre>
   * Scenario S1.2: Removing an element
   * Given  an IntrusiveCollection has been created
   *  and    the InstrusiveCollection contains a single element
   * When    I remove the element
   * Then    the IntrusiveCollection is empty.
   * </pre>
   */
  @Test
  public void testShouldRemoveElement() {
    IntrusiveCollection<SimpleElement> intrusiveCollection =
      new IntrusiveCollection<>();
    SimpleElement element = new SimpleElement();
    intrusiveCollection.add(element);

    intrusiveCollection.remove(element);

    assertTrue("Collection should be empty", intrusiveCollection.isEmpty());
    assertFalse("Collection should not contain removed element",
        intrusiveCollection.contains(element));
  }

  /**
   * <pre>
   * Scenario S1.3: Removing all elements
   * Given  an IntrusiveCollection has been created
   *  and    the IntrusiveCollection contains multiple elements
   * When    I remove all elements
   * Then    the IntrusiveCollection is empty.
   * </pre>
   */
  @Test
  public void testShouldRemoveAllElements() {
    IntrusiveCollection<SimpleElement> intrusiveCollection =
      new IntrusiveCollection<>();
    intrusiveCollection.add(new SimpleElement());
    intrusiveCollection.add(new SimpleElement());
    intrusiveCollection.add(new SimpleElement());

    intrusiveCollection.clear();

    assertTrue("Collection should be empty", intrusiveCollection.isEmpty());
  }

  /**
   * <pre>
   * Scenario S1.4: Iterating through elements
   * Given  an IntrusiveCollection has been created
   *  and    the IntrusiveCollection contains multiple elements
   * When    I iterate through the IntrusiveCollection
   * Then    I get each element in the collection, successively.
   * </pre>
   */
  @Test
  public void testIterateShouldReturnAllElements() {
    IntrusiveCollection<SimpleElement> intrusiveCollection =
      new IntrusiveCollection<>();
    SimpleElement elem1 = new SimpleElement();
    SimpleElement elem2 = new SimpleElement();
    SimpleElement elem3 = new SimpleElement();
    intrusiveCollection.add(elem1);
    intrusiveCollection.add(elem2);
    intrusiveCollection.add(elem3);

    Iterator<SimpleElement> iterator = intrusiveCollection.iterator();

    assertEquals("First element returned is incorrect", elem1, iterator.next());
    assertEquals("Second element returned is incorrect", elem2,
        iterator.next());
    assertEquals("Third element returned is incorrect", elem3, iterator.next());
    assertFalse("Iterator should not have next element", iterator.hasNext());
  }
}
