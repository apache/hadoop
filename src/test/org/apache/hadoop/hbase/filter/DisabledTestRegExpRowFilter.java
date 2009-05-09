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
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.TreeMap;

import junit.framework.TestCase;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.regionserver.HLogEdit;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Tests for regular expression row filter
 */
public class DisabledTestRegExpRowFilter extends TestCase {
  TreeMap<byte [], Cell> colvalues;
  RowFilterInterface mainFilter;
  static final char FIRST_CHAR = 'a';
  static final char LAST_CHAR = 'e';
  static final String HOST_PREFIX = "org.apache.site-";
  static byte [] GOOD_BYTES = null;

  static {
    try {
      GOOD_BYTES = "abc".getBytes(HConstants.UTF8_ENCODING);
    } catch (UnsupportedEncodingException e) {
      fail();
    }
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    this.colvalues = new TreeMap<byte [], Cell>(Bytes.BYTES_COMPARATOR);
    for (char c = FIRST_CHAR; c < LAST_CHAR; c++) {
      colvalues.put(Bytes.toBytes(new String(new char [] {c})),
          new Cell(GOOD_BYTES, HConstants.LATEST_TIMESTAMP));
    }
    this.mainFilter = new RegExpRowFilter(HOST_PREFIX + ".*", colvalues);
  }
  
  /**
   * Tests filtering using a regex on the row key
   * @throws Exception
   */
  public void testRegexOnRow() throws Exception {
    regexRowTests(mainFilter);
  }

  /**
   * Tests filtering using a regex on row and colum
   * @throws Exception
   */
  public void testRegexOnRowAndColumn() throws Exception {
    regexRowColumnTests(mainFilter);
  }
  
  /**
   * Only return values that are not null
   * @throws Exception
   */
  public void testFilterNotNull() throws Exception {
    filterNotNullTests(mainFilter);
  }
  
  /**
   * Test serialization
   * @throws Exception
   */
  public void testSerialization() throws Exception {
    // Decompose mainFilter to bytes.
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(stream);
    mainFilter.write(out);
    out.close();
    byte[] buffer = stream.toByteArray();
    
    // Recompose filter.
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(buffer));
    RowFilterInterface newFilter = new RegExpRowFilter();
    newFilter.readFields(in);
    
    // Ensure the serialization preserved the filter by running all test.
    regexRowTests(newFilter);
    newFilter.reset();
    regexRowColumnTests(newFilter);
    newFilter.reset();
    filterNotNullTests(newFilter);
  }
 
  private void regexRowTests(RowFilterInterface filter) throws Exception {
    for (char c = FIRST_CHAR; c <= LAST_CHAR; c++) {
      byte [] t = createRow(c);
      assertFalse("Failed with characer " + c, filter.filterRowKey(t));
    }
    String yahooSite = "com.yahoo.www";
    assertTrue("Failed with character " +
      yahooSite, filter.filterRowKey(Bytes.toBytes(yahooSite)));
  }
  
  private void regexRowColumnTests(RowFilterInterface filter)
    throws UnsupportedEncodingException {
    
    for (char c = FIRST_CHAR; c <= LAST_CHAR; c++) {
      byte [] t = createRow(c);
      for (Map.Entry<byte [], Cell> e: this.colvalues.entrySet()) {
        assertFalse("Failed on " + c,
          filter.filterColumn(t, e.getKey(), e.getValue().getValue()));
      }
    }
    // Try a row and column I know will pass.
    char c = 'c';
    byte [] r = createRow(c);
    byte [] col = Bytes.toBytes(Character.toString(c));
    assertFalse("Failed with character " + c,
      filter.filterColumn(r, col, GOOD_BYTES));
    
    // Do same but with bad bytes.
    assertTrue("Failed with character " + c,
      filter.filterColumn(r, col, "badbytes".getBytes(HConstants.UTF8_ENCODING)));
    
    // Do with good bytes but bad column name.  Should not filter out.
    assertFalse("Failed with character " + c,
      filter.filterColumn(r, Bytes.toBytes("badcolumn"), GOOD_BYTES));
    
    // Good column, good bytes but bad row.
    assertTrue("Failed with character " + c,
      filter.filterColumn(Bytes.toBytes("bad row"),
        Bytes.toBytes("badcolumn"), GOOD_BYTES));
  }
 
  private void filterNotNullTests(RowFilterInterface filter) throws Exception {
    // Modify the filter to expect certain columns to be null:
    // Expecting a row WITH columnKeys: a-d, WITHOUT columnKey: e
    ((RegExpRowFilter)filter).setColumnFilter(new byte [] {LAST_CHAR}, null);
    
    char secondToLast = (char)(LAST_CHAR - 1);
    char thirdToLast = (char)(LAST_CHAR - 2);
    
    // Modify the row to be missing an expected columnKey (d)
    colvalues.remove(new byte [] {(byte)secondToLast});

    // Try a row that is missing an expected columnKey.
    // Testing row with columnKeys: a-c
    assertTrue("Failed with last columnKey " + thirdToLast, filter.
      filterRow(colvalues));

    // Try a row that has all expected columnKeys, and NO null-expected
    // columnKeys.
    // Testing row with columnKeys: a-d
    colvalues.put(new byte [] {(byte)secondToLast},
        new Cell(GOOD_BYTES, HConstants.LATEST_TIMESTAMP));
    assertFalse("Failed with last columnKey " + secondToLast, filter.
      filterRow(colvalues));

    // Try a row that has all expected columnKeys AND a null-expected columnKey.
    // Testing row with columnKeys: a-e
    colvalues.put(new byte [] {LAST_CHAR},
        new Cell(GOOD_BYTES, HConstants.LATEST_TIMESTAMP));
    assertTrue("Failed with last columnKey " + LAST_CHAR, filter.
      filterRow(colvalues));
    
    // Try a row that has all expected columnKeys and a null-expected columnKey 
    // that maps to a null value.
    // Testing row with columnKeys: a-e, e maps to null
//    colvalues.put(new byte [] {LAST_CHAR}, 
//      new Cell(HLogEdit.DELETED_BYTES, HConstants.LATEST_TIMESTAMP));
//    assertFalse("Failed with last columnKey " + LAST_CHAR + " mapping to null.", 
//      filter.filterRow(colvalues));
  }

  private byte [] createRow(final char c) {
    return Bytes.toBytes(HOST_PREFIX + Character.toString(c));
  }
}
