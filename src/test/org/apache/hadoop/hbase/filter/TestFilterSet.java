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
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.util.Bytes;


import junit.framework.TestCase;

/**
 * Tests filter sets
 *
 */
public class TestFilterSet extends TestCase {
  static final int MAX_PAGES = 5;
  static final char FIRST_CHAR = 'a';
  static final char LAST_CHAR = 'e';
  static byte[] GOOD_BYTES = Bytes.toBytes("abc");
  static byte[] BAD_BYTES = Bytes.toBytes("def");

  /**
   * Test "must pass one"
   * @throws Exception
   */
  public void testMPONE() throws Exception {
    Set<Filter> filters = new HashSet<Filter>();
    filters.add(new PageFilter(MAX_PAGES));
    filters.add(new WhileMatchFilter(new PrefixFilter(Bytes.toBytes("yyy"))));
    Filter filterMPONE =
        new FilterSet(FilterSet.Operator.MUST_PASS_ONE, filters);
    /* Filter must do all below steps:
     * <ul>
     * <li>{@link #reset()}</li>
     * <li>{@link #filterAllRemaining()} -> true indicates scan is over, false, keep going on.</li>
     * <li>{@link #filterRowKey(byte[],int,int)} -> true to drop this row,
     * if false, we will also call</li>
     * <li>{@link #filterKeyValue(org.apache.hadoop.hbase.KeyValue)} -> true to drop this key/value</li>
     * <li>{@link #filterRow()} -> last chance to drop entire row based on the sequence of
     * filterValue() calls. Eg: filter a row if it doesn't contain a specified column.
     * </li>
     * </ul>
    */
    filterMPONE.reset();
    assertFalse(filterMPONE.filterAllRemaining());
    byte [] rowkey = Bytes.toBytes("yyyyyyyyy");
    assertFalse(filterMPONE.filterRowKey(rowkey, 0, rowkey.length));
    
  }

  /**
   * Test "must pass all"
   * @throws Exception
   */
  public void testMPALL() throws Exception {
    Set<Filter> filters = new HashSet<Filter>();
    filters.add(new PageFilter(MAX_PAGES));
    filters.add(new WhileMatchFilter(new PrefixFilter(Bytes.toBytes("yyy"))));
    Filter filterMPALL =
      new FilterSet(FilterSet.Operator.MUST_PASS_ALL, filters);
  }

  /**
   * Test serialization
   * @throws Exception
   */
  public void testSerialization() throws Exception {
    Set<Filter> filters = new HashSet<Filter>();
    filters.add(new PageFilter(MAX_PAGES));
    filters.add(new WhileMatchFilter(new PrefixFilter(Bytes.toBytes("yyy"))));
    Filter filterMPALL =
      new FilterSet(FilterSet.Operator.MUST_PASS_ALL, filters);

    // Decompose filterMPALL to bytes.
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(stream);
    filterMPALL.write(out);
    out.close();
    byte[] buffer = stream.toByteArray();

    // Recompose filterMPALL.
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(buffer));
    FilterSet newFilter = new FilterSet();
    newFilter.readFields(in);

    // TODO: Run TESTS!!!
  }
}