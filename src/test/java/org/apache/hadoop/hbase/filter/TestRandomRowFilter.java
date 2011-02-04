/**
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

package org.apache.hadoop.hbase.filter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import junit.framework.TestCase;

import org.apache.hadoop.hbase.util.Bytes;

public class TestRandomRowFilter extends TestCase {
  protected RandomRowFilter halfChanceFilter;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    halfChanceFilter = new RandomRowFilter(0.25f);
  }

  /**
   * Tests basics
   * 
   * @throws Exception
   */
  public void testBasics() throws Exception {
    int included = 0;
    int max = 1000000;
    for (int i = 0; i < max; i++) {
      if (!halfChanceFilter.filterRowKey(Bytes.toBytes("row"), 0, Bytes
          .toBytes("row").length)) {
        included++;
      }
    }
    // Now let's check if the filter included the right number of rows;
    // since we're dealing with randomness, we must have a include an epsilon
    // tolerance.
    int epsilon = max / 100;
    assertTrue("Roughly 25% should pass the filter", Math.abs(included - max
        / 4) < epsilon);
  }

  /**
   * Tests serialization
   * 
   * @throws Exception
   */
  public void testSerialization() throws Exception {
    RandomRowFilter newFilter = serializationTest(halfChanceFilter);
    // use epsilon float comparison
    assertTrue("float should be equal", Math.abs(newFilter.getChance()
        - halfChanceFilter.getChance()) < 0.000001f);
  }

  private RandomRowFilter serializationTest(RandomRowFilter filter)
      throws Exception {
    // Decompose filter to bytes.
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(stream);
    filter.write(out);
    out.close();
    byte[] buffer = stream.toByteArray();

    // Recompose filter.
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(buffer));
    RandomRowFilter newFilter = new RandomRowFilter();
    newFilter.readFields(in);

    return newFilter;
  }
}