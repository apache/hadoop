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

import junit.framework.TestCase;

/**
 * Tests for the page filter
 */
public class TestPageFilter extends TestCase {
  static final int ROW_LIMIT = 3;

  /**
   * test page size filter
   * @throws Exception
   */
  public void testPageSize() throws Exception {
    Filter f = new PageFilter(ROW_LIMIT);
    pageSizeTests(f);
  }

  /**
   * Test filter serialization
   * @throws Exception
   */
  public void testSerialization() throws Exception {
    Filter f = new PageFilter(ROW_LIMIT);
    // Decompose mainFilter to bytes.
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(stream);
    f.write(out);
    out.close();
    byte[] buffer = stream.toByteArray();
    // Recompose mainFilter.
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(buffer));
    Filter newFilter = new PageFilter();
    newFilter.readFields(in);

    // Ensure the serialization preserved the filter by running a full test.
    pageSizeTests(newFilter);
  }

  private void pageSizeTests(Filter f) throws Exception {
    testFiltersBeyondPageSize(f, ROW_LIMIT);
  }

  private void testFiltersBeyondPageSize(final Filter f, final int pageSize) {
    int count = 0;
    for (int i = 0; i < (pageSize * 2); i++) {
      boolean filterOut = f.filterRow();

      if(filterOut) {
        break;
      } else {
        count++;
      }

      // If at last row, should tell us to skip all remaining
      if(count == pageSize) {
        assertTrue(f.filterAllRemaining());
      } else {
        assertFalse(f.filterAllRemaining());
      }

    }
    assertEquals(pageSize, count);
  }
}
