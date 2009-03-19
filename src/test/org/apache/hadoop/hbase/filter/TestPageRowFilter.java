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

import org.apache.hadoop.hbase.util.Bytes;


import junit.framework.TestCase;

/**
 * Tests for the page row filter
 */
public class TestPageRowFilter extends TestCase {
  
  RowFilterInterface mainFilter;
  static final int ROW_LIMIT = 3;
  
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    mainFilter = new PageRowFilter(ROW_LIMIT);
  }
  
  /**
   * test page size filter
   * @throws Exception
   */
  public void testPageSize() throws Exception {
    pageSizeTests(mainFilter);
  }
  
  /**
   * Test filter serialization
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
    RowFilterInterface newFilter = new PageRowFilter();
    newFilter.readFields(in);
    
    // Ensure the serialization preserved the filter by running a full test.
    pageSizeTests(newFilter);
  }
  
  private void pageSizeTests(RowFilterInterface filter) throws Exception {
    testFiltersBeyondPageSize(filter, ROW_LIMIT);
    // Test reset works by going in again.
    filter.reset();
    testFiltersBeyondPageSize(filter, ROW_LIMIT);
  }
  
  private void testFiltersBeyondPageSize(final RowFilterInterface filter,
    final int pageSize) {
    for (int i = 0; i < (pageSize * 2); i++) {
      byte [] row = Bytes.toBytes(Integer.toString(i));
      boolean filterOut = filter.filterRowKey(row);
      if (!filterOut) {
        assertFalse("Disagrees with 'filter'", filter.filterAllRemaining());
      } else {
        // Once we have all for a page, calls to filterAllRemaining should
        // stay true.
        assertTrue("Disagrees with 'filter'", filter.filterAllRemaining());
        assertTrue(i >= pageSize);
      }
      filter.rowProcessed(filterOut, row);
    }
  }
}
