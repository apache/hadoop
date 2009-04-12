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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.util.Bytes;

public class TestKeyValue extends TestCase {
  private final Log LOG = LogFactory.getLog(this.getClass().getName());
  
  public void testBasics() throws Exception {
    LOG.info("LOWKEY: " + KeyValue.LOWESTKEY.toString());
    check(Bytes.toBytes(getName()),
      Bytes.toBytes(getName() + ":" + getName()), 1,
      Bytes.toBytes(getName()));
    // Test empty value and empty column -- both should work.
    check(Bytes.toBytes(getName()), null, 1, null);
    check(HConstants.EMPTY_BYTE_ARRAY, null, 1, null);
  }
  
  private void check(final byte [] row, final byte [] column,
    final long timestamp, final byte [] value) {
    KeyValue kv = new KeyValue(row, column, timestamp, value);
    assertTrue(Bytes.compareTo(kv.getRow(), row) == 0);
    if (column != null && column.length > 0) {
      int index = KeyValue.getFamilyDelimiterIndex(column, 0, column.length);
      byte [] family = new byte [index];
      System.arraycopy(column, 0, family, 0, family.length);
      assertTrue(kv.matchingFamily(family));
    }
    // Call toString to make sure it works.
    LOG.info(kv.toString());
  }
  
  public void testPlainCompare() throws Exception {
    final byte [] a = Bytes.toBytes("aaa");
    final byte [] b = Bytes.toBytes("bbb");
    final byte [] column = Bytes.toBytes("col:umn");
    KeyValue aaa = new KeyValue(a, column, a);
    KeyValue bbb = new KeyValue(b, column, b);
    byte [] keyabb = aaa.getKey();
    byte [] keybbb = bbb.getKey();
    assertTrue(KeyValue.COMPARATOR.compare(aaa, bbb) < 0);
    assertTrue(KeyValue.KEY_COMPARATOR.compare(keyabb, 0, keyabb.length, keybbb,
      0, keybbb.length) < 0);
    assertTrue(KeyValue.COMPARATOR.compare(bbb, aaa) > 0);
    assertTrue(KeyValue.KEY_COMPARATOR.compare(keybbb, 0, keybbb.length, keyabb,
      0, keyabb.length) > 0);
    // Compare breaks if passed same ByteBuffer as both left and right arguments.
    assertTrue(KeyValue.COMPARATOR.compare(bbb, bbb) == 0);
    assertTrue(KeyValue.KEY_COMPARATOR.compare(keybbb, 0, keybbb.length, keybbb,
      0, keybbb.length) == 0);
    assertTrue(KeyValue.COMPARATOR.compare(aaa, aaa) == 0);
    assertTrue(KeyValue.KEY_COMPARATOR.compare(keyabb, 0, keyabb.length, keyabb,
      0, keyabb.length) == 0);
    // Do compare with different timestamps.
    aaa = new KeyValue(a, column, 1, a);
    bbb = new KeyValue(a, column, 2, a);
    assertTrue(KeyValue.COMPARATOR.compare(aaa, bbb) > 0);
    assertTrue(KeyValue.COMPARATOR.compare(bbb, aaa) < 0);
    assertTrue(KeyValue.COMPARATOR.compare(aaa, aaa) == 0);
    // Do compare with different types.  Higher numbered types -- Delete
    // should sort ahead of lower numbers; i.e. Put
    aaa = new KeyValue(a, column, 1, KeyValue.Type.Delete, a);
    bbb = new KeyValue(a, column, 1, a);
    assertTrue(KeyValue.COMPARATOR.compare(aaa, bbb) < 0);
    assertTrue(KeyValue.COMPARATOR.compare(bbb, aaa) > 0);
    assertTrue(KeyValue.COMPARATOR.compare(aaa, aaa) == 0);
  }

  public void testMoreComparisons() throws Exception {
    // Root compares
    long now = System.currentTimeMillis();
    KeyValue a = new KeyValue(".META.,,99999999999999", now);
    KeyValue b = new KeyValue(".META.,,1", now);
    KVComparator c = new KeyValue.RootComparator();
    assertTrue(c.compare(b, a) < 0);
    KeyValue aa = new KeyValue(".META.,,1", now);
    KeyValue bb = new KeyValue(".META.,,1", "info:regioninfo",
      1235943454602L);
    assertTrue(c.compare(aa, bb) < 0);
    
    // Meta compares
    KeyValue aaa =
      new KeyValue("TestScanMultipleVersions,row_0500,1236020145502", now);
    KeyValue bbb = new KeyValue("TestScanMultipleVersions,,99999999999999",
      now);
    c = new KeyValue.MetaComparator();
    assertTrue(c.compare(bbb, aaa) < 0);
    
    KeyValue aaaa = new KeyValue("TestScanMultipleVersions,,1236023996656",
      "info:regioninfo", 1236024396271L);
    assertTrue(c.compare(aaaa, bbb) < 0);
    
    KeyValue x = new KeyValue("TestScanMultipleVersions,row_0500,1236034574162",
      "", 9223372036854775807L);
    KeyValue y = new KeyValue("TestScanMultipleVersions,row_0500,1236034574162",
      "info:regioninfo", 1236034574912L);
    assertTrue(c.compare(x, y) < 0);
    comparisons(new KeyValue.MetaComparator());
    comparisons(new KeyValue.KVComparator());
    metacomparisons(new KeyValue.RootComparator());
    metacomparisons(new KeyValue.MetaComparator());
  }

  /**
   * Tests cases where rows keys have characters below the ','.
   * See HBASE-832
   * @throws IOException 
   */
  public void testKeyValueBorderCases() throws IOException {
    // % sorts before , so if we don't do special comparator, rowB would
    // come before rowA.
    KeyValue rowA = new KeyValue("testtable,www.hbase.org/,1234",
      "", Long.MAX_VALUE);
    KeyValue rowB = new KeyValue("testtable,www.hbase.org/%20,99999",
      "", Long.MAX_VALUE);
    assertTrue(KeyValue.META_COMPARATOR.compare(rowA, rowB) < 0);

    rowA = new KeyValue("testtable,,1234", "", Long.MAX_VALUE);
    rowB = new KeyValue("testtable,$www.hbase.org/,99999", "", Long.MAX_VALUE);
    assertTrue(KeyValue.META_COMPARATOR.compare(rowA, rowB) < 0);

    rowA = new KeyValue(".META.,testtable,www.hbase.org/,1234,4321", "",
      Long.MAX_VALUE);
    rowB = new KeyValue(".META.,testtable,www.hbase.org/%20,99999,99999", "",
      Long.MAX_VALUE);
    assertTrue(KeyValue.ROOT_COMPARATOR.compare(rowA, rowB) < 0);
  }

  private void metacomparisons(final KeyValue.MetaComparator c) {
    long now = System.currentTimeMillis();
    assertTrue(c.compare(new KeyValue(".META.,a,,0,1", now),
      new KeyValue(".META.,a,,0,1", now)) == 0);
    KeyValue a = new KeyValue(".META.,a,,0,1", now);
    KeyValue b = new KeyValue(".META.,a,,0,2", now);
    assertTrue(c.compare(a, b) < 0);
    assertTrue(c.compare(new KeyValue(".META.,a,,0,2", now),
      new KeyValue(".META.,a,,0,1", now)) > 0);
  }

  private void comparisons(final KeyValue.KVComparator c) {
    long now = System.currentTimeMillis();
    assertTrue(c.compare(new KeyValue(".META.,,1", now),
      new KeyValue(".META.,,1", now)) == 0);
    assertTrue(c.compare(new KeyValue(".META.,,1", now),
      new KeyValue(".META.,,2", now)) < 0);
    assertTrue(c.compare(new KeyValue(".META.,,2", now),
      new KeyValue(".META.,,1", now)) > 0);
  }

  public void testBinaryKeys() throws Exception {
    Set<KeyValue> set = new TreeSet<KeyValue>(KeyValue.COMPARATOR);
    String column = "col:umn";
    KeyValue [] keys = {new KeyValue("aaaaa,\u0000\u0000,2", column, 2),
      new KeyValue("aaaaa,\u0001,3", column, 3),
      new KeyValue("aaaaa,,1", column, 1),
      new KeyValue("aaaaa,\u1000,5", column, 5),
      new KeyValue("aaaaa,a,4", column, 4),
      new KeyValue("a,a,0", column, 0),
    };
    // Add to set with bad comparator
    for (int i = 0; i < keys.length; i++) {
      set.add(keys[i]);
    }
    // This will output the keys incorrectly.
    boolean assertion = false;
    int count = 0;
    try {
      for (KeyValue k: set) {
        assertTrue(count++ == k.getTimestamp());
      }
    } catch (junit.framework.AssertionFailedError e) {
      // Expected
      assertion = true;
    }
    assertTrue(assertion);
    // Make set with good comparator
    set = new TreeSet<KeyValue>(new KeyValue.MetaComparator());
    for (int i = 0; i < keys.length; i++) {
      set.add(keys[i]);
    }
    count = 0;
    for (KeyValue k: set) {
      assertTrue(count++ == k.getTimestamp());
    }
    // Make up -ROOT- table keys.
    KeyValue [] rootKeys = {
        new KeyValue(".META.,aaaaa,\u0000\u0000,0,2", column, 2),
        new KeyValue(".META.,aaaaa,\u0001,0,3", column, 3),
        new KeyValue(".META.,aaaaa,,0,1", column, 1),
        new KeyValue(".META.,aaaaa,\u1000,0,5", column, 5),
        new KeyValue(".META.,aaaaa,a,0,4", column, 4),
        new KeyValue(".META.,,0", column, 0),
      };
    // This will output the keys incorrectly.
    set = new TreeSet<KeyValue>(new KeyValue.MetaComparator());
    // Add to set with bad comparator
    for (int i = 0; i < keys.length; i++) {
      set.add(rootKeys[i]);
    }
    assertion = false;
    count = 0;
    try {
      for (KeyValue k: set) {
        assertTrue(count++ == k.getTimestamp());
      }
    } catch (junit.framework.AssertionFailedError e) {
      // Expected
      assertion = true;
    }
    // Now with right comparator
    set = new TreeSet<KeyValue>(new KeyValue.RootComparator());
    // Add to set with bad comparator
    for (int i = 0; i < keys.length; i++) {
      set.add(rootKeys[i]);
    }
    count = 0;
    for (KeyValue k: set) {
      assertTrue(count++ == k.getTimestamp());
    }
  }
}