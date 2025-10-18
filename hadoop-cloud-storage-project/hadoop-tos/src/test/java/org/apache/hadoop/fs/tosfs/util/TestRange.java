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

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestRange {

  @Test
  public void testInclude() {
    Object[][] inputs = new Object[][]{
        new Object[]{Range.of(0, 0), 0L, false},
        new Object[]{Range.of(0, 1), 0L, true},
        new Object[]{Range.of(1, 1), 0L, false},
        new Object[]{Range.of(1, 1), 1L, true},
        new Object[]{Range.of(1, 1), 2L, false},
        new Object[]{Range.of(1, 99), 0L, false},
        new Object[]{Range.of(1, 99), 1L, true},
        new Object[]{Range.of(1, 99), 99L, true},
        new Object[]{Range.of(1, 99), 100L, false}
    };

    for (Object[] input : inputs) {
      Range r = (Range) input[0];
      long pos = (long) input[1];
      boolean expected = (boolean) input[2];

      assertEquals(expected, r.include(pos));
    }
  }

  @Test
  public void testOverlap() {
    Object[][] inputs = new Object[][]{
        new Object[]{Range.of(0, 0), Range.of(0, 0), false},
        new Object[]{Range.of(0, 1), Range.of(0, 1), true},
        new Object[]{Range.of(0, 1), Range.of(1, 0), false},
        new Object[]{Range.of(0, 1), Range.of(1, 1), false},
        new Object[]{Range.of(0, 2), Range.of(1, 1), true},
        new Object[]{Range.of(0, 2), Range.of(0, 1), true},
        new Object[]{Range.of(0, 2), Range.of(1, 2), true},
        new Object[]{Range.of(0, 2), Range.of(2, 0), false},
        new Object[]{Range.of(0, 2), Range.of(2, 1), false},
        new Object[]{Range.of(5, 9), Range.of(0, 5), false},
        new Object[]{Range.of(5, 9), Range.of(0, 6), true}
    };

    for (Object[] input : inputs) {
      Range l = (Range) input[0];
      Range r = (Range) input[1];
      boolean expect = (boolean) input[2];

      assertEquals(expect, l.overlap(r));
    }
  }

  @Test
  public void testSplit() {
    assertEquals(Range.split(10, 3),
        ImmutableList.of(Range.of(0, 3), Range.of(3, 3), Range.of(6, 3), Range.of(9, 1)));
    assertEquals(Range.split(10, 5),
        ImmutableList.of(Range.of(0, 5), Range.of(5, 5)));
    assertEquals(Range.split(10, 12),
        ImmutableList.of(Range.of(0, 10)));
    assertEquals(Range.split(2, 1),
        ImmutableList.of(Range.of(0, 1), Range.of(1, 1)));
  }
}
