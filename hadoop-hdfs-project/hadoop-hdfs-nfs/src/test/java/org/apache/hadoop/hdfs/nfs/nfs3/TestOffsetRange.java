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
package org.apache.hadoop.hdfs.nfs.nfs3;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

public class TestOffsetRange {
  @Test(expected = IllegalArgumentException.class)
  public void testConstructor1() throws IOException {
    new OffsetRange(0, 0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor2() throws IOException {
    new OffsetRange(-1, 0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor3() throws IOException {
    new OffsetRange(-3, -1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructor4() throws IOException {
    new OffsetRange(-3, 100);
  }

  @Test
  public void testCompare() throws IOException {
    OffsetRange r1 = new OffsetRange(0, 1);
    OffsetRange r2 = new OffsetRange(1, 3);
    OffsetRange r3 = new OffsetRange(1, 3);
    OffsetRange r4 = new OffsetRange(3, 4);

    assertEquals(0, OffsetRange.ReverseComparatorOnMin.compare(r2, r3));
    assertEquals(0, OffsetRange.ReverseComparatorOnMin.compare(r2, r2));
    assertTrue(OffsetRange.ReverseComparatorOnMin.compare(r2, r1) < 0);
    assertTrue(OffsetRange.ReverseComparatorOnMin.compare(r2, r4) > 0);
  }
}
