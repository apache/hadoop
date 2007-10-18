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
import org.apache.hadoop.hbase.HLogEdit;
import org.apache.hadoop.io.Text;

/**
 * Tests for regular expression row filter
 */
public class TestRegExpRowFilter extends TestCase {
  TreeMap<Text, byte []> colvalues;
  RowFilterInterface mainFilter;
  final char FIRST_CHAR = 'a';
  final char LAST_CHAR = 'e';
  final String HOST_PREFIX = "org.apache.site-";
  static byte [] GOOD_BYTES = null;

  static {
    try {
      GOOD_BYTES = "abc".getBytes(HConstants.UTF8_ENCODING);
    } catch (UnsupportedEncodingException e) {
      fail();
    }
  }
  /** {@inheritDoc} */
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    this.colvalues = new TreeMap<Text, byte[]>();
    for (char c = FIRST_CHAR; c < LAST_CHAR; c++) {
      colvalues.put(new Text(new String(new char [] {c})), GOOD_BYTES);
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
      Text t = createRow(c);
      assertFalse("Failed with characer " + c, filter.filter(t));
    }
    String yahooSite = "com.yahoo.www";
    assertTrue("Failed with character " +
      yahooSite, filter.filter(new Text(yahooSite)));
  }
  
  private void regexRowColumnTests(RowFilterInterface filter)
    throws UnsupportedEncodingException {
    
    for (char c = FIRST_CHAR; c <= LAST_CHAR; c++) {
      Text t = createRow(c);
      for (Map.Entry<Text, byte []> e: this.colvalues.entrySet()) {
        assertFalse("Failed on " + c,
          filter.filter(t, e.getKey(), e.getValue()));
      }
    }
    // Try a row and column I know will pass.
    char c = 'c';
    Text r = createRow(c);
    Text col = new Text(Character.toString(c));
    assertFalse("Failed with character " + c,
      filter.filter(r, col, GOOD_BYTES));
    
    // Do same but with bad bytes.
    assertTrue("Failed with character " + c,
      filter.filter(r, col, "badbytes".getBytes(HConstants.UTF8_ENCODING)));
    
    // Do with good bytes but bad column name.  Should not filter out.
    assertFalse("Failed with character " + c,
      filter.filter(r, new Text("badcolumn"), GOOD_BYTES));
    
    // Good column, good bytes but bad row.
    assertTrue("Failed with character " + c,
      filter.filter(new Text("bad row"), new Text("badcolumn"), GOOD_BYTES));
  }
 
  private void filterNotNullTests(RowFilterInterface filter) throws Exception {
    // Modify the filter to expect certain columns to be null:
    // Expecting a row WITH columnKeys: a-d, WITHOUT columnKey: e
    ((RegExpRowFilter)filter).setColumnFilter(new Text(new String(new char[] { 
      LAST_CHAR })), null);
    
    char secondToLast = (char)(LAST_CHAR - 1);
    char thirdToLast = (char)(LAST_CHAR - 2);
    
    // Modify the row to be missing an expected columnKey (d)
    colvalues.remove(new Text(new String(new char[] { secondToLast })));

    // Try a row that is missing an expected columnKey.
    // Testing row with columnKeys: a-c
    assertTrue("Failed with last columnKey " + thirdToLast, filter.
      filterNotNull(colvalues));

    // Try a row that has all expected columnKeys, and NO null-expected
    // columnKeys.
    // Testing row with columnKeys: a-d
    colvalues.put(new Text(new String(new char[] { secondToLast })),
      GOOD_BYTES);
    assertFalse("Failed with last columnKey " + secondToLast, filter.
      filterNotNull(colvalues));

    // Try a row that has all expected columnKeys AND a null-expected columnKey.
    // Testing row with columnKeys: a-e
    colvalues.put(new Text(new String(new char[] { LAST_CHAR })), GOOD_BYTES);
    assertTrue("Failed with last columnKey " + LAST_CHAR, filter.
      filterNotNull(colvalues));
    
    // Try a row that has all expected columnKeys and a null-expected columnKey 
    // that maps to a null value.
    // Testing row with columnKeys: a-e, e maps to null
    colvalues.put(new Text(new String(new char[] { LAST_CHAR })), 
      HLogEdit.deleteBytes.get());
    assertFalse("Failed with last columnKey " + LAST_CHAR + " mapping to null.", 
      filter.filterNotNull(colvalues));
  }

  private Text createRow(final char c) {
    return new Text(HOST_PREFIX + Character.toString(c));
  }
}
