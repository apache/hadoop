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

public class TestDataBlockRef {

  private DataBlockRef ref;

  @Before
  public void setUp() {
    ref = new DataBlockRef(1L, 2, 3, false, false);
  }

  @Test
  public void locationPropertyShouldWorkAsExpected() {
    assertEquals(1L, ref.getLocation());
  }

  @Test
  public void logicalSizePropertyShouldWorkAsExpected() {
    assertEquals(2, ref.getLogicalSize());
  }

  @Test
  public void physicalSizePropertyShouldWorkAsExpected() {
    assertEquals(3, ref.getPhysicalSize());
  }

  @Test
  public void compressedPropertyShouldWorkAsExpected() {
    assertFalse(ref.isCompressed());
    assertTrue(new DataBlockRef(1L, 2, 3, true, false).isCompressed());
  }

  @Test
  public void getInodeSizeShouldReturnSizeAndCompressedValues() {
    assertEquals(0x1_000_003, ref.getInodeSize());
    ref = new DataBlockRef(1L, 2, 3, true, false);
    assertEquals(3, ref.getInodeSize());
  }

  @Test
  public void sparsePropertyShouldWorkAsExpected() {
    assertFalse(ref.isSparse());
    assertTrue(new DataBlockRef(1L, 2, 3, false, true).isSparse());
  }

  @Test
  public void toStringShouldNotFail() {
    System.out.println(ref.toString());
  }

}
