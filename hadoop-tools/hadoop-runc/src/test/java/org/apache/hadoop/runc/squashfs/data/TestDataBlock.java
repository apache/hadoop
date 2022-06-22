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

package org.apache.hadoop.runc.squashfs.data;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestDataBlock {

  private DataBlock full;
  private DataBlock sparse;
  private DataBlock partial;
  private DataBlock empty;

  @Before
  public void setUp() {
    full = new DataBlock(new byte[1024], 1024, 1024);
    full.getData()[0] = (byte) 0xff;
    sparse = new DataBlock(new byte[1024], 1024, 0);
    partial = new DataBlock(new byte[1024], 512, 1024);
    empty = new DataBlock(new byte[1024], 0, 0);
  }

  @Test
  public void getDataShouldReturnCorrectValues() {
    assertEquals((byte) 0xff, full.getData()[0]);
  }

  @Test
  public void getLogicalSizeShouldReturnCorrectValuesForAllCases() {
    assertEquals("wrong size for full", 1024, full.getLogicalSize());
    assertEquals("wrong size for sparse", 1024, sparse.getLogicalSize());
    assertEquals("wrong size for partial", 512, partial.getLogicalSize());
    assertEquals("wrong size for empty", 0, empty.getLogicalSize());
  }

  @Test
  public void getPhysicalSizeShouldReturnCorrectValuesForAllCases() {
    assertEquals("wrong size for full", 1024, full.getPhysicalSize());
    assertEquals("wrong size for sparse", 0, sparse.getPhysicalSize());
    assertEquals("wrong size for partial", 1024, partial.getPhysicalSize());
    assertEquals("wrong size for empty", 0, empty.getPhysicalSize());
  }

  @Test
  public void isSparseShouldReturnTrueOnlyForSparseBlock() {
    assertFalse("full is sparse", full.isSparse());
    assertFalse("empty is sparse", empty.isSparse());
    assertFalse("partial is sparse", partial.isSparse());
    assertTrue("sparse is not sparse", sparse.isSparse());
  }

}
