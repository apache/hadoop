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
package org.apache.hadoop.hbase.filter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.apache.hadoop.io.Text;

import junit.framework.TestCase;

/**
 * Tests the stop row filter
 */
public class TestStopRowFilter extends TestCase {
  private final Text STOP_ROW = new Text("stop_row");
  private final Text GOOD_ROW = new Text("good_row");
  private final Text PAST_STOP_ROW = new Text("zzzzzz");
  
  RowFilterInterface mainFilter;

  /** {@inheritDoc} */
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    mainFilter = new StopRowFilter(STOP_ROW);
  }
  
  /**
   * Tests identification of the stop row
   * @throws Exception
   */
  public void testStopRowIdentification() throws Exception {
    stopRowTests(mainFilter);
  }
  
  /**
   * Tests serialization
   * @throws Exception
   */
  public void testSerialization() throws Exception {
    // Decompose mainFilter to bytes.
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(stream);
    mainFilter.write(out);
    out.close();
    byte[] buffer = stream.toByteArray();
    
    // Recompose mainFilter.
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(buffer));
    RowFilterInterface newFilter = new StopRowFilter();
    newFilter.readFields(in);
    
    // Ensure the serialization preserved the filter by running a full test.
    stopRowTests(newFilter);
  }
  
  private void stopRowTests(RowFilterInterface filter) throws Exception {
    assertFalse("Filtering on " + GOOD_ROW, filter.filter(GOOD_ROW));
    assertTrue("Filtering on " + STOP_ROW, filter.filter(STOP_ROW));
    assertTrue("Filtering on " + PAST_STOP_ROW, filter.filter(PAST_STOP_ROW));
    
    assertFalse("Filtering on " + GOOD_ROW, filter.filter(GOOD_ROW, null, 
      null));
    assertTrue("Filtering on " + STOP_ROW, filter.filter(STOP_ROW, null, null));
    assertTrue("Filtering on " + PAST_STOP_ROW, filter.filter(PAST_STOP_ROW, 
      null, null));

    assertFalse("FilterAllRemaining", filter.filterAllRemaining());
    assertFalse("FilterNotNull", filter.filterNotNull(null));
    
    assertFalse("Filter a null", filter.filter(null));
  }
}
