/**
 * Copyright 2009 The Apache Software Foundation
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
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Tests filter sets
 *
 */
public class TestFilterList extends TestCase {
  static final int MAX_PAGES = 2;
  static final char FIRST_CHAR = 'a';
  static final char LAST_CHAR = 'e';
  static byte[] GOOD_BYTES = Bytes.toBytes("abc");
  static byte[] BAD_BYTES = Bytes.toBytes("def");

  /**
   * Test "must pass one"
   * @throws Exception
   */
  public void testMPONE() throws Exception {
    List<Filter> filters = new ArrayList<Filter>();
    filters.add(new PageFilter(MAX_PAGES));
    filters.add(new WhileMatchFilter(new PrefixFilter(Bytes.toBytes("yyy"))));
    Filter filterMPONE =
        new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);
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

    /* Will pass both */
    byte [] rowkey = Bytes.toBytes("yyyyyyyyy");
    for (int i = 0; i < MAX_PAGES - 1; i++) {
      assertFalse(filterMPONE.filterRowKey(rowkey, 0, rowkey.length));
      assertFalse(filterMPONE.filterRow());
      KeyValue kv = new KeyValue(rowkey, rowkey, Bytes.toBytes(i),
        Bytes.toBytes(i));
      assertTrue(Filter.ReturnCode.INCLUDE == filterMPONE.filterKeyValue(kv));
    }

    /* Only pass PageFilter */
    rowkey = Bytes.toBytes("z");
    assertFalse(filterMPONE.filterRowKey(rowkey, 0, rowkey.length));
    assertFalse(filterMPONE.filterRow());
    KeyValue kv = new KeyValue(rowkey, rowkey, Bytes.toBytes(0),
        Bytes.toBytes(0));
    assertTrue(Filter.ReturnCode.INCLUDE == filterMPONE.filterKeyValue(kv));

    /* PageFilter will fail now, but should pass because we match yyy */
    rowkey = Bytes.toBytes("yyy");
    assertFalse(filterMPONE.filterRowKey(rowkey, 0, rowkey.length));
    assertFalse(filterMPONE.filterRow());
    kv = new KeyValue(rowkey, rowkey, Bytes.toBytes(0),
        Bytes.toBytes(0));
    assertTrue(Filter.ReturnCode.INCLUDE == filterMPONE.filterKeyValue(kv));

    /* We should filter any row */
    rowkey = Bytes.toBytes("z");
    assertTrue(filterMPONE.filterRowKey(rowkey, 0, rowkey.length));
    assertTrue(filterMPONE.filterRow());
    assertTrue(filterMPONE.filterAllRemaining());

  }

  /**
   * Test "must pass all"
   * @throws Exception
   */
  public void testMPALL() throws Exception {
    List<Filter> filters = new ArrayList<Filter>();
    filters.add(new PageFilter(MAX_PAGES));
    filters.add(new WhileMatchFilter(new PrefixFilter(Bytes.toBytes("yyy"))));
    Filter filterMPALL =
      new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
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
    filterMPALL.reset();
    assertFalse(filterMPALL.filterAllRemaining());
    byte [] rowkey = Bytes.toBytes("yyyyyyyyy");
    for (int i = 0; i < MAX_PAGES - 1; i++) {
      assertFalse(filterMPALL.filterRowKey(rowkey, 0, rowkey.length));
      KeyValue kv = new KeyValue(rowkey, rowkey, Bytes.toBytes(i),
        Bytes.toBytes(i));
      assertTrue(Filter.ReturnCode.INCLUDE == filterMPALL.filterKeyValue(kv));
    }
    filterMPALL.reset();
    rowkey = Bytes.toBytes("z");
    assertTrue(filterMPALL.filterRowKey(rowkey, 0, rowkey.length));
    // Should fail here; row should be filtered out.
    KeyValue kv = new KeyValue(rowkey, rowkey, rowkey, rowkey);
    assertTrue(Filter.ReturnCode.NEXT_ROW == filterMPALL.filterKeyValue(kv));

    // Both filters in Set should be satisfied by now
    assertTrue(filterMPALL.filterRow());
  }

  /**
   * Test list ordering
   * @throws Exception
   */
  public void testOrdering() throws Exception {
    List<Filter> filters = new ArrayList<Filter>();
    filters.add(new PrefixFilter(Bytes.toBytes("yyy")));
    filters.add(new PageFilter(MAX_PAGES));
    Filter filterMPONE =
        new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);
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

    /* We should be able to fill MAX_PAGES without incrementing page counter */
    byte [] rowkey = Bytes.toBytes("yyyyyyyy");
    for (int i = 0; i < MAX_PAGES; i++) {
      assertFalse(filterMPONE.filterRowKey(rowkey, 0, rowkey.length));
      KeyValue kv = new KeyValue(rowkey, rowkey, Bytes.toBytes(i),
          Bytes.toBytes(i));
        assertTrue(Filter.ReturnCode.INCLUDE == filterMPONE.filterKeyValue(kv));
      assertFalse(filterMPONE.filterRow());
    }

    /* Now let's fill the page filter */
    rowkey = Bytes.toBytes("xxxxxxx");
    for (int i = 0; i < MAX_PAGES; i++) {
      assertFalse(filterMPONE.filterRowKey(rowkey, 0, rowkey.length));
      KeyValue kv = new KeyValue(rowkey, rowkey, Bytes.toBytes(i),
          Bytes.toBytes(i));
        assertTrue(Filter.ReturnCode.INCLUDE == filterMPONE.filterKeyValue(kv));
      assertFalse(filterMPONE.filterRow());
    }

    /* We should still be able to include even though page filter is at max */
    rowkey = Bytes.toBytes("yyy");
    for (int i = 0; i < MAX_PAGES; i++) {
      assertFalse(filterMPONE.filterRowKey(rowkey, 0, rowkey.length));
      KeyValue kv = new KeyValue(rowkey, rowkey, Bytes.toBytes(i),
          Bytes.toBytes(i));
        assertTrue(Filter.ReturnCode.INCLUDE == filterMPONE.filterKeyValue(kv));
      assertFalse(filterMPONE.filterRow());
    }
  }

  /**
   * Test serialization
   * @throws Exception
   */
  public void testSerialization() throws Exception {
    List<Filter> filters = new ArrayList<Filter>();
    filters.add(new PageFilter(MAX_PAGES));
    filters.add(new WhileMatchFilter(new PrefixFilter(Bytes.toBytes("yyy"))));
    Filter filterMPALL =
      new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);

    // Decompose filterMPALL to bytes.
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(stream);
    filterMPALL.write(out);
    out.close();
    byte[] buffer = stream.toByteArray();

    // Recompose filterMPALL.
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(buffer));
    FilterList newFilter = new FilterList();
    newFilter.readFields(in);

    // TODO: Run TESTS!!!
  }

  /**
   * Test pass-thru of hints.
   */
  public void testHintPassThru() throws Exception {

    final KeyValue minKeyValue = new KeyValue(Bytes.toBytes(0L), null, null);
    final KeyValue maxKeyValue = new KeyValue(Bytes.toBytes(Long.MAX_VALUE),
        null, null);

    Filter filterNoHint = new FilterBase() {
      @Override
      public void readFields(DataInput arg0) throws IOException {}

      @Override
      public void write(DataOutput arg0) throws IOException {}
    };

    Filter filterMinHint = new FilterBase() {
      @Override
      public KeyValue getNextKeyHint(KeyValue currentKV) {
        return minKeyValue;
      }

      @Override
      public void readFields(DataInput arg0) throws IOException {}

      @Override
      public void write(DataOutput arg0) throws IOException {}
    };

    Filter filterMaxHint = new FilterBase() {
      @Override
      public KeyValue getNextKeyHint(KeyValue currentKV) {
        return new KeyValue(Bytes.toBytes(Long.MAX_VALUE), null, null);
      }

      @Override
      public void readFields(DataInput arg0) throws IOException {}

      @Override
      public void write(DataOutput arg0) throws IOException {}
    };

    // MUST PASS ONE

    // Should take the min if given two hints
    FilterList filterList = new FilterList(Operator.MUST_PASS_ONE,
        Arrays.asList(new Filter [] { filterMinHint, filterMaxHint } ));
    assertEquals(0, KeyValue.COMPARATOR.compare(filterList.getNextKeyHint(null),
        minKeyValue));

    // Should have no hint if any filter has no hint
    filterList = new FilterList(Operator.MUST_PASS_ONE,
        Arrays.asList(
            new Filter [] { filterMinHint, filterMaxHint, filterNoHint } ));
    assertNull(filterList.getNextKeyHint(null));
    filterList = new FilterList(Operator.MUST_PASS_ONE,
        Arrays.asList(new Filter [] { filterNoHint, filterMaxHint } ));
    assertNull(filterList.getNextKeyHint(null));

    // Should give max hint if its the only one
    filterList = new FilterList(Operator.MUST_PASS_ONE,
        Arrays.asList(new Filter [] { filterMaxHint, filterMaxHint } ));
    assertEquals(0, KeyValue.COMPARATOR.compare(filterList.getNextKeyHint(null),
        maxKeyValue));

    // MUST PASS ALL

    // Should take the max if given two hints
    filterList = new FilterList(Operator.MUST_PASS_ALL,
        Arrays.asList(new Filter [] { filterMinHint, filterMaxHint } ));
    assertEquals(0, KeyValue.COMPARATOR.compare(filterList.getNextKeyHint(null),
        maxKeyValue));

    // Should have max hint even if a filter has no hint
    filterList = new FilterList(Operator.MUST_PASS_ALL,
        Arrays.asList(
            new Filter [] { filterMinHint, filterMaxHint, filterNoHint } ));
    assertEquals(0, KeyValue.COMPARATOR.compare(filterList.getNextKeyHint(null),
        maxKeyValue));
    filterList = new FilterList(Operator.MUST_PASS_ALL,
        Arrays.asList(new Filter [] { filterNoHint, filterMaxHint } ));
    assertEquals(0, KeyValue.COMPARATOR.compare(filterList.getNextKeyHint(null),
        maxKeyValue));
    filterList = new FilterList(Operator.MUST_PASS_ALL,
        Arrays.asList(new Filter [] { filterNoHint, filterMinHint } ));
    assertEquals(0, KeyValue.COMPARATOR.compare(filterList.getNextKeyHint(null),
        minKeyValue));

    // Should give min hint if its the only one
    filterList = new FilterList(Operator.MUST_PASS_ALL,
        Arrays.asList(new Filter [] { filterNoHint, filterMinHint } ));
    assertEquals(0, KeyValue.COMPARATOR.compare(filterList.getNextKeyHint(null),
        minKeyValue));
  }
}
