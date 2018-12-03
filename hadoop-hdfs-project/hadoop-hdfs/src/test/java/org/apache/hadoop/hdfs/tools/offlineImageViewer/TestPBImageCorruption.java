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
package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for PBImageCorruptionType, CorruptionEntryBuilder and
 * PBImageCorruption classes.
 */
public class TestPBImageCorruption {
  @Test
  public void testProperCorruptionTypeCreation() {
    PBImageCorruption ct = new PBImageCorruption(209, false, true, 1);
    assertEquals("CorruptNode", ct.getType());
    ct.addMissingChildCorruption();
    assertEquals("CorruptNodeWithMissingChild", ct.getType());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testImproperCorruptionTypeCreation() {
    PBImageCorruption ct = new PBImageCorruption(210, false, false, 2);
  }

  @Test
  public void testCorruptionClass() {
    PBImageCorruption c = new PBImageCorruption(211, true, false, 3);
    String expected = "MissingChild";
    assertEquals(211, c.getId());
    assertEquals(expected, c.getType());
    assertEquals(3, c.getNumOfCorruptChildren());
    c.addCorruptNodeCorruption();
    expected = "CorruptNodeWithMissingChild";
    c.setNumberOfCorruption(34);
    assertEquals(expected, c.getType());
    assertEquals(34, c.getNumOfCorruptChildren());
  }
}
