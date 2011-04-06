/*
 * Copyright 2011 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.util;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Iterator;

import com.google.common.collect.Lists;
import org.junit.Test;

public class TestSortedCopyOnWriteSet {

  @Test
  public void testSorting() throws Exception {
    SortedCopyOnWriteSet<String> set = new SortedCopyOnWriteSet<String>();
    set.add("c");
    set.add("d");
    set.add("a");
    set.add("b");

    String[] expected = new String[]{"a", "b", "c", "d"};
    String[] stored = set.toArray(new String[4]);
    assertArrayEquals(expected, stored);

    set.add("c");
    assertEquals(4, set.size());
    stored = set.toArray(new String[4]);
    assertArrayEquals(expected, stored);
  }

  @Test
  public void testIteratorIsolation() throws Exception {
    SortedCopyOnWriteSet<String> set = new SortedCopyOnWriteSet<String>(
        Lists.newArrayList("a", "b", "c", "d", "e"));

    // isolation of remove()
    Iterator<String> iter = set.iterator();
    set.remove("c");
    boolean found = false;
    while (iter.hasNext() && !found) {
      found = "c".equals(iter.next());
    }
    assertTrue(found);

    iter = set.iterator();
    found = false;
    while (iter.hasNext() && !found) {
      found = "c".equals(iter.next());
    }
    assertFalse(found);

    // isolation of add()
    iter = set.iterator();
    set.add("f");
    found = false;
    while (iter.hasNext() && !found) {
      String next = iter.next();
      found = "f".equals(next);
    }
    assertFalse(found);

    // isolation of addAll()
    iter = set.iterator();
    set.addAll(Lists.newArrayList("g", "h", "i"));
    found = false;
    while (iter.hasNext() && !found) {
      String next = iter.next();
      found = "g".equals(next) || "h".equals(next) || "i".equals(next);
    }
    assertFalse(found);

    // isolation of clear()
    iter = set.iterator();
    set.clear();
    assertEquals(0, set.size());
    int size = 0;
    while (iter.hasNext()) {
      iter.next();
      size++;
    }
    assertTrue(size > 0);
  }
}
