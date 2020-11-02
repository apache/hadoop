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

package org.apache.hadoop.runc.squashfs.table;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestFragmentTableEntry {

  @Test
  public void isCompressedShouldReturnTrueIfEntryIsCompressed() {
    FragmentTableEntry entry = new FragmentTableEntry(1L, 2, true);
    assertTrue(entry.isCompressed());
  }

  @Test
  public void isCompressedShouldReturnFalseIfEntryIsNotCompressed() {
    FragmentTableEntry entry = new FragmentTableEntry(1L, 2, false);
    assertFalse(entry.isCompressed());
  }

  @Test
  public void getStartShouldReturnStartValue() {
    FragmentTableEntry entry = new FragmentTableEntry(1L, 2, true);
    assertEquals(1L, entry.getStart());
  }

  @Test
  public void getSizeShouldReturnMaskedSizeIfNotCompressed() {
    FragmentTableEntry entry = new FragmentTableEntry(1L, 2, false);
    assertEquals(0x1000002, entry.getSize());
  }

  @Test
  public void getSizeShouldReturnSizeIfCompressed() {
    FragmentTableEntry entry = new FragmentTableEntry(1L, 2, true);
    assertEquals(2, entry.getSize());
  }

  @Test
  public void getDiskSizeShouldReturnSizeIfNotCompressed() {
    FragmentTableEntry entry = new FragmentTableEntry(1L, 2, false);
    assertEquals(2, entry.getDiskSize());
  }

  @Test
  public void getDiskSizeShouldReturnSizeIfCompressed() {
    FragmentTableEntry entry = new FragmentTableEntry(1L, 2, true);
    assertEquals(2, entry.getDiskSize());
  }

  @Test
  public void toStringShouldNotFailWhenCompressed() {
    System.out.println(new FragmentTableEntry(1L, 2, true).toString());
  }

  @Test
  public void toStringShouldNotFailWhenNotCompressed() {
    System.out.println(new FragmentTableEntry(1L, 2, false).toString());
  }

}
