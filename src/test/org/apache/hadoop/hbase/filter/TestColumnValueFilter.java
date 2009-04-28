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
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import junit.framework.TestCase;

/**
 * Tests the stop row filter
 */
public class TestColumnValueFilter extends TestCase {

  private static final byte[] ROW = Bytes.toBytes("test");
  private static final byte[] COLUMN = Bytes.toBytes("test:foo");
  private static final byte[] VAL_1 = Bytes.toBytes("a");
  private static final byte[] VAL_2 = Bytes.toBytes("ab");
  private static final byte[] VAL_3 = Bytes.toBytes("abc");
  private static final byte[] VAL_4 = Bytes.toBytes("abcd");
  private static final byte[] FULLSTRING_1 = 
    Bytes.toBytes("The quick brown fox jumps over the lazy dog.");
  private static final byte[] FULLSTRING_2 = 
    Bytes.toBytes("The slow grey fox trips over the lazy dog.");
  private static final String QUICK_SUBSTR = "quick";
  private static final String QUICK_REGEX = "[q][u][i][c][k]";

  private RowFilterInterface basicFilterNew() {
    return new ColumnValueFilter(COLUMN,
        ColumnValueFilter.CompareOp.GREATER_OR_EQUAL, VAL_2);
  }

  private RowFilterInterface substrFilterNew() {
    return new ColumnValueFilter(COLUMN, ColumnValueFilter.CompareOp.EQUAL,
      new SubstringComparator(QUICK_SUBSTR));
  }

  private RowFilterInterface regexFilterNew() {
    return new ColumnValueFilter(COLUMN, ColumnValueFilter.CompareOp.EQUAL,
      new RegexStringComparator(QUICK_REGEX));
  }

  private void basicFilterTests(RowFilterInterface filter)
      throws Exception {
    assertTrue("basicFilter1", filter.filterColumn(ROW, 0, ROW.length,
      COLUMN, 0, COLUMN.length, VAL_1, 0, VAL_1.length));
    assertFalse("basicFilter2", filter.filterColumn(ROW, 0, ROW.length,
      COLUMN, 0, COLUMN.length, VAL_2, 0, VAL_2.length));
    assertFalse("basicFilter3", filter.filterColumn(ROW, 0, ROW.length,
      COLUMN, 0, COLUMN.length, VAL_3, 0, VAL_3.length));
    assertFalse("basicFilter4", filter.filterColumn(ROW, 0, ROW.length,
      COLUMN, 0, COLUMN.length, VAL_4, 0, VAL_4.length));
    assertFalse("basicFilterAllRemaining", filter.filterAllRemaining());
    assertFalse("basicFilterNotNull", filter.filterRow((List<KeyValue>)null));
  }

  private void substrFilterTests(RowFilterInterface filter) 
      throws Exception {
    assertTrue("substrTrue", filter.filterColumn(ROW, 0, ROW.length,
        COLUMN, 0, COLUMN.length, FULLSTRING_1, 0, FULLSTRING_1.length));
    assertFalse("substrFalse", filter.filterColumn(ROW, 0, ROW.length,
        COLUMN, 0, COLUMN.length, FULLSTRING_2, 0, FULLSTRING_2.length));
    assertFalse("substrFilterAllRemaining", filter.filterAllRemaining());
    assertFalse("substrFilterNotNull", filter.filterRow((List<KeyValue>)null));
  }

  private void regexFilterTests(RowFilterInterface filter) 
      throws Exception {
    assertTrue("regexTrue", filter.filterColumn(ROW, 0, ROW.length,
        COLUMN, 0, COLUMN.length, FULLSTRING_1, 0, FULLSTRING_1.length));
    assertFalse("regexFalse", filter.filterColumn(ROW, 0, ROW.length,
        COLUMN, 0, COLUMN.length, FULLSTRING_2, 0, FULLSTRING_2.length));
    assertFalse("regexFilterAllRemaining", filter.filterAllRemaining());
    assertFalse("regexFilterNotNull", filter.filterRow((List<KeyValue>)null));
  }

  private RowFilterInterface serializationTest(RowFilterInterface filter)
      throws Exception {
    // Decompose filter to bytes.
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(stream);
    filter.write(out);
    out.close();
    byte[] buffer = stream.toByteArray();
  
    // Recompose filter.
    DataInputStream in =
      new DataInputStream(new ByteArrayInputStream(buffer));
    RowFilterInterface newFilter = new ColumnValueFilter();
    newFilter.readFields(in);
  
    return newFilter;
  }

  RowFilterInterface basicFilter;
  RowFilterInterface substrFilter;
  RowFilterInterface regexFilter;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    basicFilter = basicFilterNew();
    substrFilter = substrFilterNew();
    regexFilter = regexFilterNew();
  }

  /**
   * Tests identification of the stop row
   * @throws Exception
   */
  public void testStop() throws Exception {
    basicFilterTests(basicFilter);
    substrFilterTests(substrFilter);
    regexFilterTests(regexFilter);
  }

  /**
   * Tests serialization
   * @throws Exception
   */
  public void testSerialization() throws Exception {
    RowFilterInterface newFilter = serializationTest(basicFilter);
    basicFilterTests(newFilter);
    newFilter = serializationTest(substrFilter);
    substrFilterTests(newFilter);
    newFilter = serializationTest(regexFilter);
    regexFilterTests(newFilter);
  }

}
