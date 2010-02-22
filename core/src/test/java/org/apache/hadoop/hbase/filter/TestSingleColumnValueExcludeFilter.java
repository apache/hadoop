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

import junit.framework.TestCase;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Tests for {@link SingleColumnValueExcludeFilter}. Because this filter 
 * extends {@link SingleColumnValueFilter}, only the added functionality is
 * tested. That is, method filterKeyValue(KeyValue).
 * 
 * @author ferdy
 *
 */
public class TestSingleColumnValueExcludeFilter extends TestCase {
  private static final byte[] ROW = Bytes.toBytes("test");
  private static final byte[] COLUMN_FAMILY = Bytes.toBytes("test");
  private static final byte[] COLUMN_QUALIFIER = Bytes.toBytes("foo");
  private static final byte[] COLUMN_QUALIFIER_2 = Bytes.toBytes("foo_2");
  private static final byte[] VAL_1 = Bytes.toBytes("a");
  private static final byte[] VAL_2 = Bytes.toBytes("ab");

  /**
   * Test the overridden functionality of filterKeyValue(KeyValue)
   * @throws Exception
   */
  public void testFilterKeyValue() throws Exception {
    Filter filter = new SingleColumnValueExcludeFilter(COLUMN_FAMILY, COLUMN_QUALIFIER,
        CompareOp.EQUAL, VAL_1);
    
    // A 'match' situation
    KeyValue kv;
    kv = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER_2, VAL_1);
    // INCLUDE expected because test column has not yet passed
    assertTrue("otherColumn", filter.filterKeyValue(kv) == Filter.ReturnCode.INCLUDE);
    kv = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER, VAL_1);
    // Test column will pass (will match), will SKIP because test columns are excluded
    assertTrue("testedMatch", filter.filterKeyValue(kv) == Filter.ReturnCode.SKIP);
    // Test column has already passed and matched, all subsequent columns are INCLUDE
    kv = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER_2, VAL_1);
    assertTrue("otherColumn", filter.filterKeyValue(kv) == Filter.ReturnCode.INCLUDE);
    assertFalse("allRemainingWhenMatch", filter.filterAllRemaining());
    
    // A 'mismatch' situation
    filter.reset();
    // INCLUDE expected because test column has not yet passed
    kv = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER_2, VAL_1);
    assertTrue("otherColumn", filter.filterKeyValue(kv) == Filter.ReturnCode.INCLUDE);
    // Test column will pass (wont match), expect NEXT_ROW
    kv = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER, VAL_2);
    assertTrue("testedMismatch", filter.filterKeyValue(kv) == Filter.ReturnCode.NEXT_ROW);
    // After a mismatch (at least with LatestVersionOnly), subsequent columns are EXCLUDE
    kv = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER_2, VAL_1);
    assertTrue("otherColumn", filter.filterKeyValue(kv) == Filter.ReturnCode.NEXT_ROW);
  } 
              
}
