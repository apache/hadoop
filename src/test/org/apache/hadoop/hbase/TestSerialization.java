/**
 * Copyright 2007 The Apache Software Foundation
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
package org.apache.hadoop.hbase;

import junit.framework.TestCase;

import org.apache.hadoop.hbase.io.BatchOperation;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Text;

/**
 * Test HBase Writables serializations
 */
public class TestSerialization extends TestCase {

  protected void setUp() throws Exception {
    super.setUp();
  }

  protected void tearDown() throws Exception {
    super.tearDown();
  }

  /**
   * Test ServerInfo serialization
   * @throws Exception
   */
  public void testServerInfo() throws Exception {
    HServerInfo hsi = new HServerInfo(new HServerAddress("0.0.0.0:123"), -1,
      1245);
    byte [] b = Writables.getBytes(hsi);
    HServerInfo deserializedHsi =
      (HServerInfo)Writables.getWritable(b, new HServerInfo());
    assertTrue(hsi.equals(deserializedHsi));
  }
  
  /**
   * Test BatchUpdate serialization
   * @throws Exception
   */
  public void testBatchUpdate() throws Exception {
    final Text testName = new Text(getName());
    // Add row named 'testName'.
    BatchUpdate bu = new BatchUpdate(testName);
    // Add a column named same as row.
    bu.put(testName, testName.getBytes());
    byte [] b = Writables.getBytes(bu);
    BatchUpdate bubu =
      (BatchUpdate)Writables.getWritable(b, new BatchUpdate());
    // Assert rows are same.
    assertTrue(bu.getRow().equals(bubu.getRow()));
    // Assert has same number of BatchOperations.
    int firstCount = 0;
    for (BatchOperation bo: bubu) {
      firstCount++;
    }
    // Now deserialize again into same instance to ensure we're not
    // accumulating BatchOperations on each deserialization.
    BatchUpdate bububu = (BatchUpdate)Writables.getWritable(b, bubu);
    // Assert rows are same again.
    assertTrue(bu.getRow().equals(bububu.getRow()));
    int secondCount = 0;
    for (BatchOperation bo: bububu) {
      secondCount++;
    }
    assertEquals(firstCount, secondCount);
  }
}