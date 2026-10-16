/*
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

package org.apache.hadoop.fs.tosfs.util;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestIterables {

  @Test
  public void testTransform() {
    List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
    Function<Integer, Integer> transform = i -> i + 10;
    Iterator<Integer> iter = Iterables.transform(list, transform).iterator();

    for (int i = 0; i < 5; i++) {
      assertTrue(iter.hasNext());
      int value = iter.next();
      assertEquals(10 + i + 1, value);
    }
    assertFalse(iter.hasNext());
  }

  @Test
  public void testTransformEmptyIterable() {
    List<Integer> list = Arrays.asList();
    Function<Integer, Integer> transform = i -> i + 10;
    Iterator<Integer> iter = Iterables.transform(list, transform).iterator();

    assertFalse(iter.hasNext());
  }

  @Test
  public void testFilter() {
    // Filter odd elements.
    List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
    Predicate<Integer> filter = i -> (i % 2) == 0;
    Iterator<Integer> iter = Iterables.filter(list, filter).iterator();

    for (int i = 0; i < 2; i++) {
      assertTrue(iter.hasNext());
      int value = iter.next();
      assertEquals((i + 1) * 2, value);
    }
    assertFalse(iter.hasNext());

    // Ignore all elements.
    filter = i -> false;
    iter = Iterables.filter(list, filter).iterator();
    assertFalse(iter.hasNext());
  }

  @Test
  public void testFilterEmptyIterable() {
    List<Integer> list = Arrays.asList();
    Predicate<Integer> filter = i -> (i % 2) == 0;
    Iterator<Integer> iter = Iterables.filter(list, filter).iterator();

    assertFalse(iter.hasNext());
  }

  // Full iterators.
  @Test
  public void testConcatFullIterators() {
    List<Integer> expectedList = new ArrayList<>();
    List<Iterable<Integer>> iterList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      List<Integer> list = new ArrayList<>();
      for (int j = 0; j < 10; j++) {
        list.add(i * 10 + j);
        expectedList.add(i * 10 + j);
      }
      iterList.add(list);
    }

    verifyConcat(expectedList.iterator(), iterList);
  }

  // Empty iterators.
  @Test
  public void testConcatEmptyIterators() {
    List<Integer> expectedList = new ArrayList<>();
    List<Iterable<Integer>> iterList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      iterList.add(Collections.emptyList());
    }

    verifyConcat(expectedList.iterator(), iterList);
  }

  // Mix full and empty iterators.
  @Test
  public void testConcatMixFullAndEmptyIterators() {
    List<Integer> expectedList = new ArrayList<>();
    List<Iterable<Integer>> iterList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      List<Integer> list = new ArrayList<>();
      for (int j = 0; j < 10; j++) {
        list.add(i * 10 + j);
        expectedList.add(i * 10 + j);
      }
      iterList.add(list);
      iterList.add(Collections.emptyList());
      iterList.add(Collections.emptyList());
    }

    verifyConcat(expectedList.iterator(), iterList);
  }

  // Invalid iterators.
  @Test
  public void testConcatNullMetaIterator() {
    assertThrows(NullPointerException.class, () -> verifyConcat(Collections.emptyIterator(), null),
        "Expect null verification error.");
  }

  // Concat null iterators.
  @Test
  public void testConcatNullElementIterators() {
    List<Iterable<Integer>> list = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      list.add(() -> null);
    }
    verifyConcat(Collections.emptyIterator(), list);
  }

  private <T> void verifyConcat(Iterator<T> expectedValues, Iterable<Iterable<T>> metaIter) {
    Iterator<T> iter = Iterables.concat(metaIter).iterator();
    while (expectedValues.hasNext()) {
      assertTrue(iter.hasNext());
      T v1 = expectedValues.next();
      T v2 = iter.next();
      assertEquals(v1, v2);
    }
    assertFalse(iter.hasNext());
  }
}
