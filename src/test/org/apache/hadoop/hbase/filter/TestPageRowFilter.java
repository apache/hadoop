/**
 * Copyright 2007 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.hadoop.io.Text;

import junit.framework.TestCase;

public class TestPageRowFilter extends TestCase {
  public void testPageSize() throws Exception {
    final int pageSize = 3;
    RowFilterInterface filter = new PageRowFilter(pageSize);
    testFiltersBeyondPageSize(filter, pageSize);
    // Test reset works by going in again.
    filter.reset();
    testFiltersBeyondPageSize(filter, pageSize);
  }
  
  private void testFiltersBeyondPageSize(final RowFilterInterface filter,
      final int pageSize) {
    for (int i = 0; i < (pageSize * 2); i++) {
      Text row = new Text(Integer.toString(i));
      boolean filterOut = filter.filter(row);
      if (!filterOut) {
        assertFalse("Disagrees with 'filter'", filter.filterAllRemaining());
        filter.acceptedRow(row);
      } else {
        // Once we have all for a page, calls to filterAllRemaining should
        // stay true.
        assertTrue("Disagrees with 'filter'", filter.filterAllRemaining());
        assertTrue(i >= pageSize);
      }
    }
  }
}
