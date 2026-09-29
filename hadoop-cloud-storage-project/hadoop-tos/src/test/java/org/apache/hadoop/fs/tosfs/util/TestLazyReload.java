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

import java.util.Arrays;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestLazyReload {
  @Test
  public void testLoadWithFilterCondition() {
    LazyReload<Integer> integers = new LazyReload<>(() -> {
      Iterator<Integer> source = Arrays.asList(1, 3, 5, 2, 4, 6).iterator();
      return buf -> {
        if (!source.hasNext()) {
          return true;
        }

        int pollCnt = 2;
        while (source.hasNext() && pollCnt-- > 0) {
          Integer item = source.next();
          if (item % 2 == 0) {
            buf.add(item);
          }
        }

        return !source.hasNext();
      };
    });

    Iterator<Integer> iterator = integers.iterator();
    assertTrue(iterator.hasNext());
    assertEquals(2, (int) iterator.next());
    assertEquals(4, (int) iterator.next());
    assertEquals(6, (int) iterator.next());
    assertFalse(iterator.hasNext());
  }

  @Test
  public void testLoadResultIsIdempotent() {
    LazyReload<Integer> integers = new LazyReload<>(() -> {
      Iterator<Integer> source = Arrays.asList(1, 3, 5, 2, 4, 6).iterator();
      return buf -> {
        if (!source.hasNext()) {
          return true;
        }

        int pollCnt = 2;
        while (source.hasNext() && pollCnt-- > 0) {
          Integer item = source.next();
          buf.add(item);
        }

        return !source.hasNext();
      };
    });
    Iterator<Integer> iterator1 = integers.iterator();
    Iterator<Integer> iterator2 = integers.iterator();

    assertEquals(1, (int) iterator1.next());
    assertEquals(1, (int) iterator2.next());
    assertEquals(3, (int) iterator1.next());
    assertEquals(3, (int) iterator2.next());
    assertEquals(5, (int) iterator1.next());
    assertEquals(5, (int) iterator2.next());

    assertEquals(2, (int) iterator1.next());
    assertEquals(4, (int) iterator1.next());
    assertEquals(6, (int) iterator1.next());
    assertEquals(2, (int) iterator2.next());
    assertEquals(4, (int) iterator2.next());
    assertEquals(6, (int) iterator2.next());
  }
}
