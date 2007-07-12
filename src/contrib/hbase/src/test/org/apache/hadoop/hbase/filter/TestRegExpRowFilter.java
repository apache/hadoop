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

import java.util.Map;
import java.util.TreeMap;

import junit.framework.TestCase;

import org.apache.hadoop.io.Text;

public class TestRegExpRowFilter extends TestCase {
  TreeMap<Text, byte []> colvalues;
  RowFilterInterface filter;
  final char FIRST_CHAR = 'a';
  final char LAST_CHAR = 'e';
  byte [] GOOD_BYTES = "abc".getBytes();
  final String HOST_PREFIX = "org.apache.site-";
  
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    this.colvalues = new TreeMap<Text, byte[]>();
    for (char c = FIRST_CHAR; c < LAST_CHAR; c++) {
      colvalues.put(new Text(new String(new char [] {c})), GOOD_BYTES);
    }
    this.filter = new RegExpRowFilter(HOST_PREFIX + ".*", colvalues);
  }
  
  public void testRegexOnRow() throws Exception {
    for (char c = FIRST_CHAR; c <= LAST_CHAR; c++) {
      Text t = createRow(c);
      assertFalse("Failed with characer " + c, filter.filter(t));
    }
    String yahooSite = "com.yahoo.www";
    assertTrue("Failed with character " +
      yahooSite, filter.filter(new Text(yahooSite)));
  }
  
  public void testRegexOnRowAndColumn() throws Exception {
    for (char c = FIRST_CHAR; c <= LAST_CHAR; c++) {
      Text t = createRow(c);
      for (Map.Entry<Text, byte []> e: this.colvalues.entrySet()) {
        assertFalse("Failed on " + c,
          this.filter.filter(t, e.getKey(), e.getValue()));
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
      filter.filter(r, col, "badbytes".getBytes()));
    // Do with good bytes but bad column name.  Should not filter out.
    assertFalse("Failed with character " + c,
      filter.filter(r, new Text("badcolumn"), GOOD_BYTES));
    // Good column, good bytes but bad row.
    assertTrue("Failed with character " + c,
      filter.filter(new Text("bad row"), new Text("badcolumn"), GOOD_BYTES));
  }
  
  private Text createRow(final char c) {
    return new Text(HOST_PREFIX + Character.toString(c));
  }
}
